package chain

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/dialer"
	"github.com/go-gost/core/logger"
	xlogger "github.com/go-gost/x/logger"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type prewarmTestFixture struct {
	transport    *PrewarmTransport
	dials        atomic.Int64
	handshakes   atomic.Int64
	connects     atomic.Int64
	activeDials  atomic.Int64
	maxDials     atomic.Int64
	dialFailures atomic.Int64
	mu           sync.Mutex
	peers        []net.Conn
	delay        time.Duration
}

func newPrewarmTestFixture(size int, delay time.Duration) *prewarmTestFixture {
	f := &prewarmTestFixture{delay: delay}
	d := &testDialer{
		dialFn: func(ctx context.Context, _ string, _ ...dialer.DialOption) (net.Conn, error) {
			f.dials.Add(1)
			active := f.activeDials.Add(1)
			defer f.activeDials.Add(-1)
			for {
				maxActive := f.maxDials.Load()
				if active <= maxActive || f.maxDials.CompareAndSwap(maxActive, active) {
					break
				}
			}
			if err := waitPrewarmDelay(ctx, f.delay); err != nil {
				return nil, err
			}
			for {
				remaining := f.dialFailures.Load()
				if remaining <= 0 {
					break
				}
				if f.dialFailures.CompareAndSwap(remaining, remaining-1) {
					return nil, errors.New("simulated dial failure")
				}
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
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
	}
	base := NewTransport(d, &testConnector{
		connectFn: func(_ context.Context, conn net.Conn, _, _ string, _ ...connector.ConnectOption) (net.Conn, error) {
			f.connects.Add(1)
			return conn, nil
		},
	})
	f.transport = NewPrewarmTransport(base, PrewarmConfig{
		Size:           size,
		MaxConcurrent:  size,
		Lifetime:       time.Minute,
		IdleTimeout:    time.Minute,
		RefillInterval: 5 * time.Millisecond,
		DialTimeout:    time.Second,
		CheckTimeout:   time.Millisecond,
	}, xlogger.Nop(), "test-hop", "test-node")
	return f
}

func waitPrewarmDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *prewarmTestFixture) close() {
	f.transport.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, peer := range f.peers {
		peer.Close()
	}
}

func (f *prewarmTestFixture) closePeer(index int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index >= 0 && index < len(f.peers) {
		f.peers[index].Close()
	}
}

func waitForPrewarmAvailable(t testing.TB, tr *PrewarmTransport, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.PrewarmStats().Available == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("prewarm pool did not reach %d available connections: %+v", want, tr.PrewarmStats())
}

func dialThroughPrewarm(ctx context.Context, tr *PrewarmTransport) (net.Conn, error) {
	conn, err := tr.Dial(ctx, "proxy.example:443")
	if err != nil {
		return nil, err
	}
	conn, err = tr.Handshake(ctx, conn)
	if err != nil {
		return nil, err
	}
	return tr.Connect(ctx, conn, "tcp", "destination.example:443")
}

func TestPrewarmHitSkipsDialAndHandshake(t *testing.T) {
	f := newPrewarmTestFixture(1, 20*time.Millisecond)
	defer f.close()
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)
	if got := f.connects.Load(); got != 0 {
		t.Fatalf("connector ran during prewarming: %d calls", got)
	}

	started := time.Now()
	conn, err := dialThroughPrewarm(context.Background(), f.transport)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if elapsed := time.Since(started); elapsed >= 20*time.Millisecond {
		t.Fatalf("pool hit took %v; upstream dial or handshake remained on the critical path", elapsed)
	}

	stats := f.transport.PrewarmStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("unexpected stats after hit: %+v", stats)
	}
	if got := f.connects.Load(); got != 1 {
		t.Fatalf("connector calls = %d, want 1 after checkout", got)
	}
}

func TestPrewarmLogsEstablishedAndUsedEvents(t *testing.T) {
	var output lockedBuffer
	f := newPrewarmTestFixture(1, 0)
	defer f.close()
	f.transport.pool.log = xlogger.NewLogger(
		xlogger.OutputOption(&output),
		xlogger.LevelOption(logger.DebugLevel),
	)
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)

	conn, err := dialThroughPrewarm(context.Background(), f.transport)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		logs := output.String()
		if strings.Contains(logs, `"event":"prewarm_established"`) &&
			strings.Contains(logs, `"event":"prewarm_used"`) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expected established and used log events, got:\n%s", output.String())
}

func TestPrewarmPoolExhaustionFallsBackConcurrently(t *testing.T) {
	const (
		poolSize = 2
		requests = 6
	)
	f := newPrewarmTestFixture(poolSize, 25*time.Millisecond)
	defer f.close()
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, poolSize)

	start := make(chan struct{})
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, err := dialThroughPrewarm(context.Background(), f.transport)
			if conn != nil {
				conn.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	stats := f.transport.PrewarmStats()
	if stats.Hits != poolSize || stats.Misses != requests-poolSize {
		t.Fatalf("unexpected exhaustion stats: %+v", stats)
	}
	if stats.Creating > poolSize {
		t.Fatalf("background creation exceeded configured bound: %+v", stats)
	}
}

func TestPrewarmRejectsRemotelyClosedIdleConnection(t *testing.T) {
	f := newPrewarmTestFixture(1, 5*time.Millisecond)
	defer f.close()
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)
	f.closePeer(0)

	conn, err := dialThroughPrewarm(context.Background(), f.transport)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	stats := f.transport.PrewarmStats()
	if stats.Stale != 1 || stats.Misses != 1 || stats.Hits != 0 {
		t.Fatalf("unexpected stale-connection stats: %+v", stats)
	}
}

func TestPrewarmAsynchronouslyReplenishes(t *testing.T) {
	f := newPrewarmTestFixture(1, 5*time.Millisecond)
	defer f.close()
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)

	conn, err := dialThroughPrewarm(context.Background(), f.transport)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	waitForPrewarmAvailable(t, f.transport, 1)
	if got := f.dials.Load(); got < 2 {
		t.Fatalf("expected an asynchronous replacement dial, got %d total dials", got)
	}
}

func TestPrewarmBoundsBackgroundCreation(t *testing.T) {
	f := newPrewarmTestFixture(5, 10*time.Millisecond)
	defer f.close()
	f.transport.pool.cfg.MaxConcurrent = 2
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 5)
	if got := f.maxDials.Load(); got > 2 {
		t.Fatalf("maximum concurrent background dials = %d, want <= 2", got)
	}
}

func TestPrewarmRetriesBackgroundFailure(t *testing.T) {
	f := newPrewarmTestFixture(1, 2*time.Millisecond)
	defer f.close()
	f.dialFailures.Store(1)
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)
	stats := f.transport.PrewarmStats()
	if stats.ReplenishmentFailures != 1 {
		t.Fatalf("replenishment failures = %d, want 1", stats.ReplenishmentFailures)
	}
	if got := f.dials.Load(); got < 2 {
		t.Fatalf("dial attempts = %d, want at least 2", got)
	}
}

func TestPrewarmExpiresIdleConnection(t *testing.T) {
	f := newPrewarmTestFixture(1, 0)
	defer f.close()
	f.transport.pool.cfg.IdleTimeout = 10 * time.Millisecond
	f.transport.pool.cfg.RefillInterval = 2 * time.Millisecond
	f.transport.StartPrewarm("proxy.example:443")
	waitForPrewarmAvailable(t, f.transport, 1)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if f.transport.PrewarmStats().Stale > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("idle connection was not expired: %+v", f.transport.PrewarmStats())
}

func BenchmarkPrewarmConnect(b *testing.B) {
	const simulatedLeg = time.Millisecond

	b.Run("without_prewarm", func(b *testing.B) {
		f := newPrewarmTestFixture(0, simulatedLeg)
		defer f.close()
		b.ResetTimer()
		for range b.N {
			conn, err := dialThroughPrewarm(context.Background(), f.transport)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})

	b.Run("populated_pool", func(b *testing.B) {
		f := newPrewarmTestFixture(0, simulatedLeg)
		defer f.close()
		f.transport.pool.cfg.CheckTimeout = 0
		for range b.N {
			b.StopTimer()
			now := time.Now()
			f.transport.pool.mu.Lock()
			f.transport.pool.addr = "proxy.example:443"
			f.transport.pool.idle = append(f.transport.pool.idle, &prewarmEntry{
				conn:      &testConn{},
				createdAt: now,
				idleAt:    now,
			})
			f.transport.pool.mu.Unlock()
			b.StartTimer()
			conn, err := dialThroughPrewarm(context.Background(), f.transport)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})

	b.Run("exhausted_pool", func(b *testing.B) {
		f := newPrewarmTestFixture(1, simulatedLeg)
		f.transport.StartPrewarm("proxy.example:443")
		waitForPrewarmAvailable(b, f.transport, 1)
		reserved, err := dialThroughPrewarm(context.Background(), f.transport)
		if err != nil {
			b.Fatal(err)
		}
		f.transport.Close()
		defer func() {
			reserved.Close()
			f.close()
		}()
		b.ResetTimer()
		for range b.N {
			conn, err := dialThroughPrewarm(context.Background(), f.transport)
			if err != nil {
				b.Fatal(err)
			}
			conn.Close()
		}
	})
}
