package chain

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/dialer"
	"github.com/go-gost/core/logger"
	xlogger "github.com/go-gost/x/logger"
)

type predictiveTestFixture struct {
	transport      *PredictiveTransport
	dials          atomic.Int64
	handshakes     atomic.Int64
	connects       atomic.Int64
	connectFailure atomic.Int64
	delay          time.Duration
	mu             sync.Mutex
	peers          []net.Conn
}

func newPredictiveTestFixture(size, minHits int, delay time.Duration) *predictiveTestFixture {
	f := &predictiveTestFixture{delay: delay}
	d := &testDialer{
		dialFn: func(ctx context.Context, _ string, _ ...dialer.DialOption) (net.Conn, error) {
			f.dials.Add(1)
			if err := waitPrewarmDelay(ctx, f.delay); err != nil {
				return nil, err
			}
			conn, peer := net.Pipe()
			f.mu.Lock()
			f.peers = append(f.peers, peer)
			f.mu.Unlock()
			return conn, nil
		},
		handshakeFn: func(ctx context.Context, conn net.Conn, _ ...dialer.HandshakeOption) (net.Conn, error) {
			f.handshakes.Add(1)
			if err := waitPrewarmDelay(ctx, f.delay); err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
	c := &testConnector{
		connectFn: func(ctx context.Context, conn net.Conn, _, _ string, _ ...connector.ConnectOption) (net.Conn, error) {
			f.connects.Add(1)
			if err := waitPrewarmDelay(ctx, f.delay); err != nil {
				return nil, err
			}
			for {
				remaining := f.connectFailure.Load()
				if remaining <= 0 {
					break
				}
				if f.connectFailure.CompareAndSwap(remaining, remaining-1) {
					return nil, errors.New("simulated CONNECT failure")
				}
			}
			return conn, nil
		},
	}
	f.transport = NewPredictiveTransport(NewTransport(d, c), PredictiveConfig{
		Size:           size,
		MaxEndpoints:   4,
		MinHits:        minHits,
		Window:         time.Minute,
		MaxConcurrent:  2,
		Lifetime:       time.Minute,
		IdleTimeout:    time.Minute,
		RefillInterval: 5 * time.Millisecond,
		ConnectTimeout: time.Second,
		CheckTimeout:   -1,
	}, xlogger.Nop(), "test-hop", "test-node")
	return f
}

func (f *predictiveTestFixture) close() {
	_ = f.transport.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, peer := range f.peers {
		_ = peer.Close()
	}
}

func (f *predictiveTestFixture) closePeer(index int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= 0 && index < len(f.peers) {
		_ = f.peers[index].Close()
	}
}

func waitForPredictiveAvailable(t testing.TB, tr *PredictiveTransport, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.PredictiveStats().Available == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("predictive pool did not reach %d available tunnels: %+v", want, tr.PredictiveStats())
}

func predictiveDial(ctx context.Context, tr *PredictiveTransport, target string) (net.Conn, error) {
	return tr.DialConnect(ctx, "proxy.example:443", "tcp", target)
}

func TestPredictiveLearnsAndUsesConnectedTunnel(t *testing.T) {
	const legDelay = 15 * time.Millisecond
	f := newPredictiveTestFixture(1, 2, legDelay)
	defer f.close()

	for range 2 {
		conn, err := predictiveDial(context.Background(), f.transport, "frequent.example:443")
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
	waitForPredictiveAvailable(t, f.transport, 1)
	stats := f.transport.PredictiveStats()
	if stats.HotEndpoints != 1 || stats.Misses != 2 {
		t.Fatalf("unexpected learned pool stats: %+v", stats)
	}

	started := time.Now()
	conn, err := predictiveDial(context.Background(), f.transport, "frequent.example:443")
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	_ = conn.Close()
	if elapsed >= legDelay {
		t.Fatalf("predictive hit took %v; upstream setup remained on the critical path", elapsed)
	}
	if stats := f.transport.PredictiveStats(); stats.Hits != 1 {
		t.Fatalf("predictive hits = %d, want 1", stats.Hits)
	}
}

func TestPredictiveDoesNotPromoteInfrequentEndpoint(t *testing.T) {
	f := newPredictiveTestFixture(1, 2, 0)
	defer f.close()

	conn, err := predictiveDial(context.Background(), f.transport, "once.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	time.Sleep(20 * time.Millisecond)
	stats := f.transport.PredictiveStats()
	if stats.TrackedEndpoints != 1 || stats.HotEndpoints != 0 || stats.Available != 0 {
		t.Fatalf("one request unexpectedly created a predictive tunnel: %+v", stats)
	}
}

func TestPredictiveEvictsLeastRecentlyUsedEndpoint(t *testing.T) {
	f := newPredictiveTestFixture(1, 100, 0)
	defer f.close()
	f.transport.pool.cfg.MaxEndpoints = 2

	for _, target := range []string{
		"one.example:443",
		"two.example:443",
		"one.example:443",
		"three.example:443",
	} {
		conn, err := predictiveDial(context.Background(), f.transport, target)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		time.Sleep(time.Millisecond)
	}

	f.transport.pool.mu.Lock()
	_, hasOne := f.transport.pool.targets[newPredictiveKey("tcp", "one.example:443")]
	_, hasTwo := f.transport.pool.targets[newPredictiveKey("tcp", "two.example:443")]
	_, hasThree := f.transport.pool.targets[newPredictiveKey("tcp", "three.example:443")]
	f.transport.pool.mu.Unlock()
	if !hasOne || hasTwo || !hasThree {
		t.Fatalf("unexpected LRU contents: one=%v two=%v three=%v", hasOne, hasTwo, hasThree)
	}
}

func TestPredictiveExpiresInactiveEndpoint(t *testing.T) {
	f := newPredictiveTestFixture(1, 1, 0)
	defer f.close()
	f.transport.pool.cfg.Window = 15 * time.Millisecond
	f.transport.pool.cfg.RefillInterval = 2 * time.Millisecond

	conn, err := predictiveDial(context.Background(), f.transport, "idle.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	waitForPredictiveAvailable(t, f.transport, 1)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := f.transport.PredictiveStats()
		if stats.TrackedEndpoints == 0 && stats.Available == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("inactive endpoint was not removed: %+v", f.transport.PredictiveStats())
}

func TestPredictiveExhaustionFallsBack(t *testing.T) {
	const legDelay = 10 * time.Millisecond
	f := newPredictiveTestFixture(1, 2, legDelay)
	defer f.close()

	for range 2 {
		conn, err := predictiveDial(context.Background(), f.transport, "busy.example:443")
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
	waitForPredictiveAvailable(t, f.transport, 1)

	reserved, err := predictiveDial(context.Background(), f.transport, "busy.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()

	started := time.Now()
	fallback, err := predictiveDial(context.Background(), f.transport, "busy.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer fallback.Close()
	if elapsed := time.Since(started); elapsed < 2*legDelay {
		t.Fatalf("exhausted pool did not use the normal setup path: %v", elapsed)
	}
	stats := f.transport.PredictiveStats()
	if stats.Hits != 1 || stats.Misses != 3 {
		t.Fatalf("unexpected exhaustion stats: %+v", stats)
	}
}

func TestPredictiveRejectsClosedIdleTunnel(t *testing.T) {
	f := newPredictiveTestFixture(1, 1, 0)
	defer f.close()
	f.transport.pool.cfg.CheckTimeout = 2 * time.Millisecond

	conn, err := predictiveDial(context.Background(), f.transport, "stale.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	waitForPredictiveAvailable(t, f.transport, 1)
	// Peer 0 belongs to the foreground request; peer 1 is the predicted tunnel.
	f.closePeer(1)

	conn, err = predictiveDial(context.Background(), f.transport, "stale.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	stats := f.transport.PredictiveStats()
	if stats.Stale != 1 || stats.Hits != 0 || stats.Misses != 2 {
		t.Fatalf("unexpected stale-tunnel stats: %+v", stats)
	}
}

func TestPredictivePreservesServerFirstByteDuringCheck(t *testing.T) {
	f := newPredictiveTestFixture(1, 1, 0)
	defer f.close()
	f.transport.pool.cfg.CheckTimeout = 100 * time.Millisecond

	conn, peer := net.Pipe()
	now := time.Now()
	key := newPredictiveKey("tcp", "banner.example:22")
	f.transport.pool.mu.Lock()
	f.transport.pool.proxyAddr = "proxy.example:443"
	f.transport.pool.targets[key] = &predictiveTarget{
		key:      key,
		hot:      true,
		lastSeen: now,
		idle: []*predictiveEntry{{
			conn:      conn,
			createdAt: now,
			idleAt:    now,
		}},
	}
	f.transport.pool.mu.Unlock()

	writeDone := make(chan error, 1)
	go func() {
		_, err := peer.Write([]byte{'S'})
		writeDone <- err
	}()
	got := f.transport.pool.get("tcp", "banner.example:22")
	if got == nil {
		t.Fatal("server-first tunnel was rejected")
	}
	defer got.Close()
	var b [1]byte
	if _, err := got.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	if b[0] != 'S' {
		t.Fatalf("preserved byte = %q, want S", b[0])
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
}

func TestPredictiveBoundsEndpointsAndBackgroundCreation(t *testing.T) {
	f := newPredictiveTestFixture(1, 1, 10*time.Millisecond)
	defer f.close()
	f.transport.pool.cfg.MaxEndpoints = 2
	f.transport.pool.cfg.MaxConcurrent = 1
	var maxCreating atomic.Int64
	stopSampling := make(chan struct{})
	doneSampling := make(chan struct{})
	go func() {
		defer close(doneSampling)
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				creating := int64(f.transport.PredictiveStats().Creating)
				for {
					maximum := maxCreating.Load()
					if creating <= maximum || maxCreating.CompareAndSwap(maximum, creating) {
						break
					}
				}
			case <-stopSampling:
				return
			}
		}
	}()

	for _, target := range []string{"one.example:443", "two.example:443", "three.example:443"} {
		conn, err := predictiveDial(context.Background(), f.transport, target)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
	deadline := time.Now().Add(time.Second)
	for f.transport.PredictiveStats().Creating > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stopSampling)
	<-doneSampling
	stats := f.transport.PredictiveStats()
	if stats.TrackedEndpoints > 2 || stats.HotEndpoints > 2 {
		t.Fatalf("endpoint bound exceeded: %+v", stats)
	}
	if got := maxCreating.Load(); got > 1 {
		t.Fatalf("background creation concurrency = %d, want <= 1", got)
	}
}

func TestPredictiveRetriesBackgroundFailure(t *testing.T) {
	f := newPredictiveTestFixture(1, 1, 0)
	defer f.close()
	f.transport.StartPrewarm("proxy.example:443")
	f.connectFailure.Store(1)
	key := newPredictiveKey("tcp", "retry.example:443")
	f.transport.pool.mu.Lock()
	target := &predictiveTarget{
		key:         key,
		hits:        1,
		windowStart: time.Now(),
		lastSeen:    time.Now(),
		hot:         true,
	}
	f.transport.pool.targets[key] = target
	f.transport.pool.scheduleTargetLocked(target)
	f.transport.pool.mu.Unlock()

	waitForPredictiveAvailable(t, f.transport, 1)
	if stats := f.transport.PredictiveStats(); stats.ReplenishmentFailures != 1 {
		t.Fatalf("replenishment failures = %d, want 1", stats.ReplenishmentFailures)
	}
}

func TestPredictiveLogsLifecycleEvents(t *testing.T) {
	var output lockedBuffer
	f := newPredictiveTestFixture(1, 1, 0)
	defer f.close()
	f.transport.pool.log = xlogger.NewLogger(
		xlogger.OutputOption(&output),
		xlogger.LevelOption(logger.DebugLevel),
	)

	conn, err := predictiveDial(context.Background(), f.transport, "logged.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	waitForPredictiveAvailable(t, f.transport, 1)
	conn, err = predictiveDial(context.Background(), f.transport, "logged.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		logs := output.String()
		found := true
		for _, event := range []string{"predictive_promoted", "predictive_established", "predictive_used"} {
			if !strings.Contains(logs, `"event":"`+event+`"`) {
				found = false
				break
			}
		}
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing predictive lifecycle event in logs:\n%s", output.String())
}

type routeFastPathTransport struct {
	options   corechain.TransportOptions
	fastCalls atomic.Int64
	dialCalls atomic.Int64
}

func (tr *routeFastPathTransport) Dial(context.Context, string) (net.Conn, error) {
	tr.dialCalls.Add(1)
	return nil, errors.New("ordinary Dial should not run")
}

func (tr *routeFastPathTransport) Handshake(context.Context, net.Conn) (net.Conn, error) {
	return nil, errors.New("ordinary Handshake should not run")
}

func (tr *routeFastPathTransport) Connect(context.Context, net.Conn, string, string) (net.Conn, error) {
	return nil, errors.New("ordinary Connect should not run")
}

func (tr *routeFastPathTransport) DialConnect(_ context.Context, proxyAddr, network, address string) (net.Conn, error) {
	tr.fastCalls.Add(1)
	if proxyAddr == "" || network != "tcp" || address != "destination.example:443" {
		return nil, errors.New("unexpected fast-path arguments")
	}
	return &testConn{}, nil
}

func (tr *routeFastPathTransport) Bind(context.Context, net.Conn, string, string, ...connector.BindOption) (net.Listener, error) {
	return nil, connector.ErrBindUnsupported
}

func (tr *routeFastPathTransport) Multiplex() bool { return false }

func (tr *routeFastPathTransport) Options() *corechain.TransportOptions { return &tr.options }

func (tr *routeFastPathTransport) Copy() corechain.Transporter { return tr }

func TestChainRouteUsesDialConnectFastPath(t *testing.T) {
	tr := &routeFastPathTransport{}
	node := corechain.NewNode("predictive", "127.0.0.1:443", corechain.TransportNodeOption(tr))
	route := NewRoute()
	route.addNode(node)

	conn, err := route.Dial(context.Background(), "tcp", "destination.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if got := tr.fastCalls.Load(); got != 1 {
		t.Fatalf("DialConnect calls = %d, want 1", got)
	}
	if got := tr.dialCalls.Load(); got != 0 {
		t.Fatalf("ordinary Dial calls = %d, want 0", got)
	}
}

func BenchmarkPredictiveConnect(b *testing.B) {
	const simulatedLeg = time.Millisecond

	b.Run("learned_and_populated", func(b *testing.B) {
		f := newPredictiveTestFixture(1, 1, simulatedLeg)
		defer f.close()
		// Entries are injected per iteration; disable asynchronous refill so the
		// benchmark measures checkout rather than pool maintenance.
		f.transport.pool.cfg.Size = 0
		f.transport.pool.cfg.CheckTimeout = 0
		for range b.N {
			b.StopTimer()
			now := time.Now()
			conn, peer := net.Pipe()
			key := newPredictiveKey("tcp", "benchmark.example:443")
			f.mu.Lock()
			f.peers = append(f.peers, peer)
			f.mu.Unlock()
			f.transport.pool.mu.Lock()
			f.transport.pool.proxyAddr = "proxy.example:443"
			f.transport.pool.targets[key] = &predictiveTarget{
				key:      key,
				hot:      true,
				lastSeen: now,
				idle: []*predictiveEntry{{
					conn:      conn,
					createdAt: now,
					idleAt:    now,
				}},
			}
			f.transport.pool.mu.Unlock()
			b.StartTimer()
			got, err := predictiveDial(context.Background(), f.transport, "benchmark.example:443")
			if err != nil {
				b.Fatal(err)
			}
			_ = got.Close()
		}
	})

	b.Run("unlearned", func(b *testing.B) {
		f := newPredictiveTestFixture(1, b.N+1, simulatedLeg)
		defer f.close()
		b.ResetTimer()
		for range b.N {
			conn, err := predictiveDial(context.Background(), f.transport, "benchmark.example:443")
			if err != nil {
				b.Fatal(err)
			}
			_ = conn.Close()
		}
	})

	b.Run("learned_but_exhausted", func(b *testing.B) {
		f := newPredictiveTestFixture(1, 1, simulatedLeg)
		defer f.close()
		f.transport.StartPrewarm("proxy.example:443")
		key := newPredictiveKey("tcp", "benchmark.example:443")
		f.transport.pool.mu.Lock()
		f.transport.pool.cfg.MaxConcurrent = 0
		f.transport.pool.targets[key] = &predictiveTarget{
			key:         key,
			hits:        1,
			windowStart: time.Now(),
			lastSeen:    time.Now(),
			hot:         true,
		}
		f.transport.pool.mu.Unlock()
		b.ResetTimer()
		for range b.N {
			conn, err := predictiveDial(context.Background(), f.transport, "benchmark.example:443")
			if err != nil {
				b.Fatal(err)
			}
			_ = conn.Close()
		}
	})
}
