package chain

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	corechain "github.com/go-gost/core/chain"
	"github.com/go-gost/core/connector"
	"github.com/go-gost/core/logger"
	"github.com/go-gost/core/metrics"
	xmetrics "github.com/go-gost/x/metrics"
)

const (
	defaultPrewarmLifetime       = 5 * time.Minute
	defaultPrewarmIdleTimeout    = time.Minute
	defaultPrewarmInactivity     = 5 * time.Minute
	defaultPrewarmRefillInterval = time.Second
	defaultPrewarmDialTimeout    = 15 * time.Second
	defaultPrewarmCheckTimeout   = time.Millisecond
)

// PrewarmConfig controls a pool of fully handshaken, otherwise unused
// connections to a proxy node. A connection is removed permanently as soon as
// it is checked out; tunneled connections are never returned to the pool.
type PrewarmConfig struct {
	Size          int
	MaxConcurrent int
	Lifetime      time.Duration
	IdleTimeout   time.Duration
	// InactivityTimeout suspends the entire pool after this long without a
	// Dial request. A negative value disables inactivity suspension.
	InactivityTimeout time.Duration
	RefillInterval    time.Duration
	DialTimeout       time.Duration
	CheckTimeout      time.Duration
}

// PrewarmStats is a point-in-time view of a prewarmed transport.
type PrewarmStats struct {
	Active                bool
	Available             int
	Creating              int
	Hits                  uint64
	Misses                uint64
	Stale                 uint64
	Suspensions           uint64
	ReplenishmentFailures uint64
	LastEstablishDuration time.Duration
}

// PrewarmTransport decorates a node transport with a bounded, single-use
// connection pool. The underlying connector is deliberately not involved in
// pool creation: pooled connections have completed the transport handshake
// (for an HTTPS node, TCP and TLS) but have not sent HTTP CONNECT.
type PrewarmTransport struct {
	base *Transport
	pool *prewarmPool
}

// NewPrewarmTransport creates and starts a dormant pool. Call StartPrewarm to
// provide the resolved node address and begin filling it.
func NewPrewarmTransport(base *Transport, cfg PrewarmConfig, log logger.Logger, hop, node string) *PrewarmTransport {
	cfg = normalizePrewarmConfig(cfg)
	return &PrewarmTransport{
		base: base,
		pool: newPrewarmPool(base, cfg, log, hop, node),
	}
}

func normalizePrewarmConfig(cfg PrewarmConfig) PrewarmConfig {
	if cfg.Size < 0 {
		cfg.Size = 0
	}
	if cfg.MaxConcurrent <= 0 || cfg.MaxConcurrent > cfg.Size {
		cfg.MaxConcurrent = cfg.Size
	}
	if cfg.Lifetime <= 0 {
		cfg.Lifetime = defaultPrewarmLifetime
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultPrewarmIdleTimeout
	}
	if cfg.InactivityTimeout < 0 {
		cfg.InactivityTimeout = 0
	} else if cfg.InactivityTimeout == 0 {
		cfg.InactivityTimeout = defaultPrewarmInactivity
	}
	if cfg.RefillInterval <= 0 {
		cfg.RefillInterval = defaultPrewarmRefillInterval
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultPrewarmDialTimeout
	}
	if cfg.CheckTimeout < 0 {
		cfg.CheckTimeout = 0
	} else if cfg.CheckTimeout == 0 {
		cfg.CheckTimeout = defaultPrewarmCheckTimeout
	}
	return cfg
}

// StartPrewarm asynchronously fills the pool for addr. Calling it again with
// the same address is a no-op. A changed resolved address invalidates idle
// connections and starts a new pool generation.
func (tr *PrewarmTransport) StartPrewarm(addr string) {
	if tr == nil || tr.pool == nil {
		return
	}
	tr.pool.start(addr)
}

func (tr *PrewarmTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	tr.pool.activate(addr)
	if conn := tr.pool.get(addr); conn != nil {
		return &prewarmedConn{Conn: conn}, nil
	}
	return tr.base.Dial(ctx, addr)
}

func (tr *PrewarmTransport) Handshake(ctx context.Context, conn net.Conn) (net.Conn, error) {
	if conn, ok := conn.(*prewarmedConn); ok {
		return conn.Conn, nil
	}
	return tr.base.Handshake(ctx, conn)
}

func (tr *PrewarmTransport) Connect(ctx context.Context, conn net.Conn, network, address string) (net.Conn, error) {
	return tr.base.Connect(ctx, conn, network, address)
}

func (tr *PrewarmTransport) Bind(ctx context.Context, conn net.Conn, network, address string, opts ...connector.BindOption) (net.Listener, error) {
	return tr.base.Bind(ctx, conn, network, address, opts...)
}

func (tr *PrewarmTransport) Multiplex() bool {
	return tr.base.Multiplex()
}

func (tr *PrewarmTransport) Options() *corechain.TransportOptions {
	return tr.base.Options()
}

func (tr *PrewarmTransport) Copy() corechain.Transporter {
	// Prewarming is currently enabled only for the non-multiplexed HTTP/TLS
	// transport, so route splitting does not call Copy in normal use. Preserve
	// the shared single-use pool if a caller nevertheless copies the transport.
	return &PrewarmTransport{
		base: tr.base.Copy().(*Transport),
		pool: tr.pool,
	}
}

func (tr *PrewarmTransport) Close() error {
	if tr == nil || tr.pool == nil {
		return nil
	}
	return tr.pool.close()
}

func (tr *PrewarmTransport) PrewarmStats() PrewarmStats {
	if tr == nil || tr.pool == nil {
		return PrewarmStats{}
	}
	return tr.pool.stats()
}

// prewarmedConn tells Handshake that this connection has already completed
// the underlying dialer's handshake. It is unwrapped before reaching a
// connector, so the rest of the chain sees the original connection type.
type prewarmedConn struct {
	net.Conn
}

type prewarmEntry struct {
	conn      net.Conn
	createdAt time.Time
	idleAt    time.Time
}

func (e *prewarmEntry) expired(now time.Time, cfg PrewarmConfig) bool {
	return now.Sub(e.createdAt) >= cfg.Lifetime || now.Sub(e.idleAt) >= cfg.IdleTimeout
}

type prewarmPool struct {
	base   *Transport
	cfg    PrewarmConfig
	log    logger.Logger
	labels metrics.Labels

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	addr       string
	generation uint64
	idle       []*prewarmEntry
	creating   int
	started    bool
	active     bool
	closed     bool
	lastDemand time.Time

	hits        atomic.Uint64
	misses      atomic.Uint64
	stale       atomic.Uint64
	suspensions atomic.Uint64
	failures    atomic.Uint64
	lastDialNS  atomic.Int64
}

func newPrewarmPool(base *Transport, cfg PrewarmConfig, log logger.Logger, hop, node string) *prewarmPool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &prewarmPool{
		base:   base,
		cfg:    cfg,
		log:    log,
		labels: metrics.Labels{"hop": hop, "node": node},
		ctx:    ctx,
		cancel: cancel,
	}
	return p
}

func (p *prewarmPool) start(addr string) {
	p.configure(addr, false)
}

// activate records real demand for the pool. Unlike start, it resumes a pool
// which was suspended for inactivity.
func (p *prewarmPool) activate(addr string) {
	p.configure(addr, true)
}

func (p *prewarmPool) configure(addr string, demand bool) {
	if addr == "" || p.cfg.Size == 0 {
		return
	}

	var (
		old     []*prewarmEntry
		resumed bool
	)
	now := time.Now()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if !p.started {
		p.started = true
		p.active = true
		p.lastDemand = now
		p.setActiveMetricLocked()
		p.wg.Add(1)
		go p.maintain()
	}
	if p.addr != addr {
		p.addr = addr
		p.generation++
		old = p.idle
		p.idle = nil
		p.setAvailableMetricLocked()
	}
	if demand {
		p.lastDemand = now
		if !p.active {
			p.active = true
			resumed = true
			p.setActiveMetricLocked()
		}
	}
	if p.active {
		p.scheduleLocked()
	}
	p.mu.Unlock()

	closePrewarmEntries(old)
	if resumed && p.log != nil {
		p.log.WithFields(map[string]any{
			"event":  "prewarm_resumed",
			"remote": addr,
		}).Debug("prewarm pool resumed")
	}
}

func (p *prewarmPool) get(addr string) net.Conn {
	defer p.replenish()

	for {
		p.mu.Lock()
		if p.closed || p.addr != addr || len(p.idle) == 0 {
			p.mu.Unlock()
			p.misses.Add(1)
			xmetrics.GetCounter(xmetrics.MetricPrewarmMissesCounter, p.labels).Inc()
			return nil
		}
		i := len(p.idle) - 1
		entry := p.idle[i]
		p.idle[i] = nil
		p.idle = p.idle[:i]
		available := len(p.idle)
		p.setAvailableMetricLocked()
		p.mu.Unlock()

		if entry.expired(time.Now(), p.cfg) || !prewarmConnAlive(entry.conn, p.cfg.CheckTimeout) {
			entry.conn.Close()
			p.stale.Add(1)
			xmetrics.GetCounter(xmetrics.MetricPrewarmStaleCounter, p.labels).Inc()
			continue
		}

		p.hits.Add(1)
		xmetrics.GetCounter(xmetrics.MetricPrewarmHitsCounter, p.labels).Inc()
		if p.log != nil {
			p.log.WithFields(map[string]any{
				"event":     "prewarm_used",
				"remote":    addr,
				"available": available,
				"capacity":  p.cfg.Size,
			}).Debug("prewarmed connection used")
		}
		return entry.conn
	}
}

func prewarmConnAlive(conn net.Conn, timeout time.Duration) bool {
	if conn == nil {
		return false
	}
	if timeout <= 0 {
		return true
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	resetErr := conn.SetReadDeadline(time.Time{})
	if resetErr != nil || n != 0 {
		return false
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

func (p *prewarmPool) replenish() {
	p.mu.Lock()
	p.scheduleLocked()
	p.mu.Unlock()
}

func (p *prewarmPool) scheduleLocked() {
	if p.closed || !p.active || p.addr == "" || p.cfg.Size == 0 {
		return
	}
	need := p.cfg.Size - len(p.idle) - p.creating
	slots := p.cfg.MaxConcurrent - p.creating
	if need > slots {
		need = slots
	}
	if need <= 0 {
		return
	}

	addr, generation := p.addr, p.generation
	p.creating += need
	for range need {
		p.wg.Add(1)
		go p.create(addr, generation)
	}
}

func (p *prewarmPool) create(addr string, generation uint64) {
	defer p.wg.Done()
	started := time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.DialTimeout)
	defer cancel()

	conn, err := p.base.Dial(ctx, addr)
	if err == nil {
		conn, err = p.base.Handshake(ctx, conn)
	}
	duration := time.Since(started)
	p.lastDialNS.Store(int64(duration))
	xmetrics.GetObserver(xmetrics.MetricPrewarmEstablishDurationObserver, p.labels).Observe(duration.Seconds())

	accepted := false
	available := 0
	p.mu.Lock()
	p.creating--
	if err == nil && conn != nil && !p.closed && p.addr == addr && p.generation == generation && len(p.idle) < p.cfg.Size {
		now := time.Now()
		p.idle = append(p.idle, &prewarmEntry{
			conn:      conn,
			createdAt: started,
			idleAt:    now,
		})
		accepted = true
		available = len(p.idle)
		p.setAvailableMetricLocked()
	}
	if accepted {
		p.scheduleLocked()
	}
	p.mu.Unlock()

	if !accepted && conn != nil {
		conn.Close()
	}
	if accepted && p.log != nil {
		p.log.WithFields(map[string]any{
			"event":     "prewarm_established",
			"remote":    addr,
			"available": available,
			"capacity":  p.cfg.Size,
			"duration":  duration.String(),
		}).Debug("prewarmed connection established")
	}
	if err != nil && p.ctx.Err() == nil {
		p.failures.Add(1)
		xmetrics.GetCounter(xmetrics.MetricPrewarmFailuresCounter, p.labels).Inc()
		if p.log != nil {
			p.log.Debugf("prewarm %s: %v", addr, err)
		}
	}
}

func (p *prewarmPool) maintain() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.RefillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.expireAndRefill()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *prewarmPool) expireAndRefill() {
	now := time.Now()
	var (
		expired   []*prewarmEntry
		suspended []*prewarmEntry
		remote    string
	)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.active && p.cfg.InactivityTimeout > 0 &&
		!p.lastDemand.IsZero() && now.Sub(p.lastDemand) >= p.cfg.InactivityTimeout {
		p.active = false
		p.generation++
		p.suspensions.Add(1)
		suspended = p.idle
		p.idle = nil
		remote = p.addr
		p.setAvailableMetricLocked()
		p.setActiveMetricLocked()
		p.mu.Unlock()

		closePrewarmEntries(suspended)
		if p.log != nil {
			p.log.WithFields(map[string]any{
				"event":       "prewarm_suspended",
				"remote":      remote,
				"idle_closed": len(suspended),
				"inactivity":  p.cfg.InactivityTimeout.String(),
			}).Debug("prewarm pool suspended after inactivity")
		}
		return
	}
	n := 0
	for _, entry := range p.idle {
		if entry.expired(now, p.cfg) {
			expired = append(expired, entry)
			continue
		}
		p.idle[n] = entry
		n++
	}
	p.idle = p.idle[:n]
	if len(expired) > 0 {
		p.setAvailableMetricLocked()
	}
	p.scheduleLocked()
	p.mu.Unlock()

	if len(expired) > 0 {
		p.stale.Add(uint64(len(expired)))
		counter := xmetrics.GetCounter(xmetrics.MetricPrewarmStaleCounter, p.labels)
		for range expired {
			counter.Inc()
		}
		closePrewarmEntries(expired)
	}
}

func (p *prewarmPool) setAvailableMetricLocked() {
	xmetrics.GetGauge(xmetrics.MetricPrewarmAvailableGauge, p.labels).Set(float64(len(p.idle)))
}

func (p *prewarmPool) setActiveMetricLocked() {
	value := 0.0
	if p.active {
		value = 1
	}
	xmetrics.GetGauge(xmetrics.MetricPrewarmActiveGauge, p.labels).Set(value)
}

func (p *prewarmPool) stats() PrewarmStats {
	p.mu.Lock()
	active, available, creating := p.active, len(p.idle), p.creating
	p.mu.Unlock()
	return PrewarmStats{
		Active:                active,
		Available:             available,
		Creating:              creating,
		Hits:                  p.hits.Load(),
		Misses:                p.misses.Load(),
		Stale:                 p.stale.Load(),
		Suspensions:           p.suspensions.Load(),
		ReplenishmentFailures: p.failures.Load(),
		LastEstablishDuration: time.Duration(p.lastDialNS.Load()),
	}
}

func (p *prewarmPool) close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active = false
	p.cancel()
	idle := p.idle
	p.idle = nil
	p.setAvailableMetricLocked()
	p.setActiveMetricLocked()
	p.mu.Unlock()

	closePrewarmEntries(idle)
	p.wg.Wait()
	return nil
}

func closePrewarmEntries(entries []*prewarmEntry) {
	for _, entry := range entries {
		if entry != nil && entry.conn != nil {
			entry.conn.Close()
		}
	}
}
