package chain

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/dialer"
	httpconnector "github.com/go-gost/x/connector/http"
	tlsdialer "github.com/go-gost/x/dialer/tls"
	xlogger "github.com/go-gost/x/logger"
)

type connectRequest struct {
	host string
	auth string
}

type delayedTLSProxy struct {
	listener    net.Listener
	delay       time.Duration
	handshakes  atomic.Int64
	requests    chan connectRequest
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	wg          sync.WaitGroup
}

func newDelayedTLSProxy(t *testing.T, delay time.Duration) *delayedTLSProxy {
	t.Helper()
	cert := newTestCertificate(t)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &delayedTLSProxy{
		listener: tls.NewListener(raw, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}),
		delay:       delay,
		requests:    make(chan connectRequest, 8),
		connections: make(map[net.Conn]struct{}),
	}
	p.wg.Add(1)
	go p.serve()
	return p
}

func newTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (p *delayedTLSProxy) serve() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.connections[conn] = struct{}{}
		p.mu.Unlock()
		p.wg.Add(1)
		go p.serveConn(conn)
	}
}

func (p *delayedTLSProxy) serveConn(conn net.Conn) {
	defer p.wg.Done()
	defer func() {
		conn.Close()
		p.mu.Lock()
		delete(p.connections, conn)
		p.mu.Unlock()
	}()

	time.Sleep(p.delay)
	tlsConn, ok := conn.(*tls.Conn)
	if !ok || tlsConn.Handshake() != nil {
		return
	}
	p.handshakes.Add(1)
	req, err := http.ReadRequest(bufio.NewReader(tlsConn))
	if err != nil {
		return
	}
	p.requests <- connectRequest{
		host: req.Host,
		auth: req.Header.Get("Proxy-Authorization"),
	}
	if _, err := io.WriteString(tlsConn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, tlsConn)
}

func (p *delayedTLSProxy) close() {
	p.listener.Close()
	p.mu.Lock()
	for conn := range p.connections {
		conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func TestHTTPSPrewarmIntegration(t *testing.T) {
	const handshakeDelay = 75 * time.Millisecond
	proxy := newDelayedTLSProxy(t, handshakeDelay)
	defer proxy.close()

	d := tlsdialer.NewDialer(
		dialer.TLSConfigOption(&tls.Config{
			InsecureSkipVerify: true, // test-only certificate
			MinVersion:         tls.VersionTLS12,
		}),
		dialer.LoggerOption(xlogger.Nop()),
	)
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	c := httpconnector.NewConnector(
		connector.AuthOption(url.UserPassword("pool-user", "pool-pass")),
		connector.LoggerOption(xlogger.Nop()),
	)
	if err := c.Init(nil); err != nil {
		t.Fatal(err)
	}

	tr := NewPrewarmTransport(NewTransport(d, c), PrewarmConfig{
		Size:           1,
		MaxConcurrent:  1,
		Lifetime:       time.Minute,
		IdleTimeout:    time.Minute,
		RefillInterval: 10 * time.Millisecond,
		DialTimeout:    time.Second,
		CheckTimeout:   2 * time.Millisecond,
	}, xlogger.Nop(), "integration-hop", "integration-node")
	defer tr.Close()
	tr.StartPrewarm(proxy.listener.Addr().String())
	waitForPrewarmAvailable(t, tr, 1)

	deadline := time.Now().Add(time.Second)
	for proxy.handshakes.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := proxy.handshakes.Load(); got != 1 {
		t.Fatalf("TLS handshakes before checkout = %d, want 1", got)
	}
	select {
	case req := <-proxy.requests:
		t.Fatalf("CONNECT was sent during prewarming: %+v", req)
	default:
	}

	started := time.Now()
	conn, err := tr.Dial(context.Background(), proxy.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err = tr.Handshake(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	conn, err = tr.Connect(context.Background(), conn, "tcp", "destination.example:443")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	conn.Close()
	if elapsed >= handshakeDelay/2 {
		t.Fatalf("CONNECT took %v; the %v TLS delay remained on the critical path", elapsed, handshakeDelay)
	}

	select {
	case req := <-proxy.requests:
		if req.host != "destination.example:443" {
			t.Fatalf("CONNECT host = %q", req.host)
		}
		wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("pool-user:pool-pass"))
		if req.auth != wantAuth {
			t.Fatalf("Proxy-Authorization = %q, want %q", req.auth, wantAuth)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy did not receive CONNECT")
	}
}
