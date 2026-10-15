package chain

import (
	"context"
	"io"
	"net"
	"strings"
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
	defaultPredictiveMaxEndpoints  = 8
	defaultPredictiveMinHits       = 2
	defaultPredictiveWindow        = time.Minute
	defaultPredictiveLifetime      = time.Minute
	defaultPredictiveIdleTimeout   = 30 * time.Second
	defaultPredictiveRefill        = time.Second
	defaultPredictiveConnect       = 15 * time.Second
	defaultPredictiveCheckTimeout  = time.Millisecond
	defaultPredictiveMaxConcurrent = 2
)

// PredictiveConfig controls endpoint-specific pools whose connections have
// completed TCP, TLS, and HTTP CONNECT. Size is the number of idle tunnels
// maintained for each hot endpoint.
type PredictiveConfig struct {
	Size           int
	MaxEndpoints   int
	MinHits        int
	Window         time.Duration
	MaxConcurrent  int
	Lifetime       time.Duration
	IdleTimeout    time.Duration
	RefillInterval time.Duration
	ConnectTimeout time.Duration
	CheckTimeout   time.Duration
}

// PredictiveStats is a point-in-time view of a predictive tunnel pool.
type PredictiveStats struct {
	TrackedEndpoints      int
	HotEndpoints          int
	Available             int
	Creating              int
	Hits                  uint64
	Misses                uint64
	Stale                 uint64
	ReplenishmentFailures uint64
	LastEstablishDuration time.Duration
}

// PredictiveTransport decorates an HTTP/TLS node transport with learned,
// single-use pools of fully CONNECTed tunnels. The chain route uses
// DialConnect when this transport is the only hop, allowing a prediction hit
// to avoid opening an otherwise unnecessary upstream connection.
type PredictiveTransport struct {
	base corechain.Transporter
	pool *predictivePool
}

// NewPredictiveTransport creates a predictive transport. Pool maintenance
// starts lazily when the upstream proxy address is known.
func NewPredictiveTransport(base corechain.Transporter, cfg PredictiveConfig, log logger.Logger, hop, node string) *PredictiveTransport {
	cfg = normalizePredictiveConfig(cfg)
	tr := &PredictiveTransport{base: base}
	tr.pool = newPredictivePool(tr, cfg, log, hop, node)
	return tr
}

func normalizePredictiveConfig(cfg PredictiveConfig) PredictiveConfig {
	if cfg.Size < 0 {
		cfg.Size = 0
	}
	if cfg.MaxEndpoints <= 0 {
		cfg.MaxEndpoints = defaultPredictiveMaxEndpoints
	}
	if cfg.MinHits <= 0 {
		cfg.MinHits = defaultPredictiveMinHits
	}
	if cfg.Window <= 0 {
		cfg.Window = defaultPredictiveWindow
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = defaultPredictiveMaxConcurrent
	}
	maxCreating := cfg.Size * cfg.MaxEndpoints
	if cfg.MaxConcurrent > maxCreating {
		cfg.MaxConcurrent = maxCreating
	}
	if cfg.Lifetime <= 0 {
		cfg.Lifetime = defaultPredictiveLifetime
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultPredictiveIdleTimeout
	}
	if cfg.RefillInterval <= 0 {
		cfg.RefillInterval = defaultPredictiveRefill
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultPredictiveConnect
	}
	if cfg.CheckTimeout < 0 {
		cfg.CheckTimeout = 0
	} else if cfg.CheckTimeout == 0 {
		cfg.CheckTimeout = defaultPredictiveCheckTimeout
	}
	return cfg
}

// StartPrewarm makes PredictiveTransport compatible with the existing
// first-hop startup hook. It starts both an underlying TLS prewarm pool, when
// present, and the predictive pool.
func (tr *PredictiveTransport) StartPrewarm(addr string) {
	if tr == nil || tr.pool == nil {
		return
	}
	if starter, ok := tr.base.(prewarmStarter); ok {
		starter.StartPrewarm(addr)
	}
	tr.pool.start(addr)
}

func (tr *PredictiveTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	tr.StartPrewarm(addr)
	return tr.base.Dial(ctx, addr)
}

func (tr *PredictiveTransport) Handshake(ctx context.Context, conn net.Conn) (net.Conn, error) {
	return tr.base.Handshake(ctx, conn)
}

func (tr *PredictiveTransport) Connect(ctx context.Context, conn net.Conn, network, address string) (net.Conn, error) {
	if predictiveNetwork(network) {
		if predicted := tr.pool.get(network, address); predicted != nil {
			if conn != nil {
				_ = conn.Close()
			}
			tr.pool.observeSuccess(network, address)
			return predicted, nil
		}
	}

	cc, err := tr.base.Connect(ctx, conn, network, address)
	if err == nil && predictiveNetwork(network) {
		tr.pool.observeSuccess(network, address)
	}
	return cc, err
}

// DialConnect combines the one-hop Dial, Handshake, and Connect sequence. It
// is intentionally an optional x/chain fast path rather than part of the core
// Transporter interface.
func (tr *PredictiveTransport) DialConnect(ctx context.Context, proxyAddr, network, address string) (net.Conn, error) {
	tr.StartPrewarm(proxyAddr)
	if predictiveNetwork(network) {
		if predicted := tr.pool.get(network, address); predicted != nil {
			tr.pool.observeSuccess(network, address)
			return predicted, nil
		}
	}

	conn, err := tr.dialTunnel(ctx, proxyAddr, network, address)
	if err == nil && predictiveNetwork(network) {
		tr.pool.observeSuccess(network, address)
	}
	return conn, err
}

func (tr *PredictiveTransport) dialTunnel(ctx context.Context, proxyAddr, network, address string) (net.Conn, error) {
	raw, err := tr.base.Dial(ctx, proxyAddr)
	if err != nil {
		return nil, err
	}

	conn, err := tr.base.Handshake(ctx, raw)
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		_ = raw.Close()
		return nil, err
	}

	cc, err := tr.base.Connect(ctx, conn, network, address)
	if err != nil {
		if cc != nil {
			_ = cc.Close()
		}
		_ = conn.Close()
		return nil, err
	}
	return cc, nil
}

func (tr *PredictiveTransport) Bind(ctx context.Context, conn net.Conn, network, address string, opts ...connector.BindOption) (net.Listener, error) {
	return tr.base.Bind(ctx, conn, network, address, opts...)
}

func (tr *PredictiveTransport) Multiplex() bool {
	return tr.base.Multiplex()
}

func (tr *PredictiveTransport) Options() *corechain.TransportOptions {
	return tr.base.Options()
}

func (tr *PredictiveTransport) Copy() corechain.Transporter {
	return &PredictiveTransport{
		base: tr.base.Copy(),
		pool: tr.pool,
	}
}

func (tr *PredictiveTransport) Close() error {
	if tr == nil {
		return nil
	}
	if tr.pool != nil {
		_ = tr.pool.close()
	}
	if closer, ok := tr.base.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (tr *PredictiveTransport) PredictiveStats() PredictiveStats {
	if tr == nil || tr.pool == nil {
		return PredictiveStats{}
	}
	return tr.pool.stats()
}

func predictiveNetwork(network string) bool {
	switch strings.ToLower(network) {
	case "tcp", "tcp4", "tcp6":
		return true
	default:
		return false
	}
}

type predictiveKey struct {
	network string
	address string
}

func newPredictiveKey(network, address string) predictiveKey {
	return predictiveKey{network: strings.ToLower(network), address: address}
}

type predictiveEntry struct {
	conn      net.Conn
	createdAt time.Time
	idleAt    time.Time
}

func (e *predictiveEntry) expired(now time.Time, cfg PredictiveConfig) bool {
	return now.Sub(e.createdAt) >= cfg.Lifetime || now.Sub(e.idleAt) >= cfg.IdleTimeout
}

type predictiveTarget struct {
	key         predictiveKey
	hits        int
	windowStart time.Time
	lastSeen    time.Time
	hot         bool
	idle        []*predictiveEntry
	creating    int
}

type predictivePool struct {
	transport *PredictiveTransport
	cfg       PredictiveConfig
	log       logger.Logger
	labels    metrics.Labels

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.Mutex
	proxyAddr  string
	generation uint64
	targets    map[predictiveKey]*predictiveTarget
	creating   int
	started    bool
	closed     bool

	hits       atomic.Uint64
	misses     atomic.Uint64
	stale      atomic.Uint64
	failures   atomic.Uint64
	lastDialNS atomic.Int64
}

func newPredictivePool(transport *PredictiveTransport, cfg PredictiveConfig, log logger.Logger, hop, node string) *predictivePool {
	ctx, cancel := context.WithCancel(context.Background())
	return &predictivePool{
		transport: transport,
		cfg:       cfg,
		log:       log,
		labels:    metrics.Labels{"hop": hop, "node": node},
		ctx:       ctx,
		cancel:    cancel,
		targets:   make(map[predictiveKey]*predictiveTarget),
	}
}

func (p *predictivePool) start(proxyAddr string) {
	if proxyAddr == "" || p.cfg.Size == 0 {
		return
	}

	var old []*predictiveEntry
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if !p.started {
		p.started = true
		p.wg.Add(1)
		go p.maintain()
	}
	if p.proxyAddr != proxyAddr {
		p.proxyAddr = proxyAddr
		p.generation++
		for _, target := range p.targets {
			old = append(old, target.idle...)
			target.idle = nil
		}
		p.setAvailableMetricLocked()
		p.scheduleAllLocked()
	}
	p.mu.Unlock()

	closePredictiveEntries(old)
}

func (p *predictivePool) get(network, address string) net.Conn {
	key := newPredictiveKey(network, address)
	defer p.replenish(key)

	for {
		p.mu.Lock()
		target := p.targets[key]
		if p.closed || p.proxyAddr == "" || target == nil || !target.hot || len(target.idle) == 0 {
			p.mu.Unlock()
			p.misses.Add(1)
			xmetrics.GetCounter(xmetrics.MetricPredictiveMissesCounter, p.labels).Inc()
			return nil
		}
		i := len(target.idle) - 1
		entry := target.idle[i]
		target.idle[i] = nil
		target.idle = target.idle[:i]
		available := len(target.idle)
		p.setAvailableMetricLocked()
		p.mu.Unlock()

		if entry.expired(time.Now(), p.cfg) {
			_ = entry.conn.Close()
			p.recordStale()
			continue
		}
		conn, alive := predictiveConnAlive(entry.conn, p.cfg.CheckTimeout)
		if !alive {
			_ = entry.conn.Close()
			p.recordStale()
			continue
		}

		p.hits.Add(1)
		xmetrics.GetCounter(xmetrics.MetricPredictiveHitsCounter, p.labels).Inc()
		if p.log != nil {
			p.log.WithFields(map[string]any{
				"event":     "predictive_used",
				"target":    address,
				"available": available,
				"capacity":  p.cfg.Size,
			}).Debug("predictive CONNECT tunnel used")
		}
		return conn
	}
}

func (p *predictivePool) observeSuccess(network, address string) {
	key := newPredictiveKey(network, address)
	now := time.Now()
	var (
		closed   []*predictiveEntry
		promoted bool
		hits     int
	)

	p.mu.Lock()
	if p.closed || p.proxyAddr == "" || p.cfg.Size == 0 {
		p.mu.Unlock()
		return
	}
	target := p.targets[key]
	if target == nil {
		if len(p.targets) >= p.cfg.MaxEndpoints {
			var oldest *predictiveTarget
			for _, candidate := range p.targets {
				if oldest == nil || candidate.lastSeen.Before(oldest.lastSeen) {
					oldest = candidate
				}
			}
			if oldest != nil {
				delete(p.targets, oldest.key)
				closed = append(closed, oldest.idle...)
				oldest.idle = nil
			}
		}
		target = &predictiveTarget{
			key:         key,
			windowStart: now,
		}
		p.targets[key] = target
	}
	if now.Sub(target.windowStart) >= p.cfg.Window {
		target.windowStart = now
		target.hits = 0
	}
	target.hits++
	target.lastSeen = now
	hits = target.hits
	if !target.hot && target.hits >= p.cfg.MinHits {
		target.hot = true
		promoted = true
	}
	if target.hot {
		p.scheduleTargetLocked(target)
	}
	p.setAvailableMetricLocked()
	p.setHotMetricLocked()
	p.mu.Unlock()

	closePredictiveEntries(closed)
	if promoted && p.log != nil {
		p.log.WithFields(map[string]any{
			"event":  "predictive_promoted",
			"target": address,
			"hits":   hits,
			"window": p.cfg.Window.String(),
		}).Debug("CONNECT target promoted")
	}
}

func (p *predictivePool) replenish(key predictiveKey) {
	p.mu.Lock()
	if target := p.targets[key]; target != nil {
		p.scheduleTargetLocked(target)
	}
	p.mu.Unlock()
}

func (p *predictivePool) scheduleTargetLocked(target *predictiveTarget) {
	if p.closed || p.proxyAddr == "" || p.cfg.Size == 0 || target == nil || !target.hot {
		return
	}
	need := p.cfg.Size - len(target.idle) - target.creating
	slots := p.cfg.MaxConcurrent - p.creating
	if need > slots {
		need = slots
	}
	if need <= 0 {
		return
	}

	proxyAddr, generation := p.proxyAddr, p.generation
	target.creating += need
	p.creating += need
	for range need {
		p.wg.Add(1)
		go p.create(proxyAddr, generation, target)
	}
}

func (p *predictivePool) scheduleAllLocked() {
	for _, target := range p.targets {
		p.scheduleTargetLocked(target)
		if p.creating >= p.cfg.MaxConcurrent {
			return
		}
	}
}

func (p *predictivePool) create(proxyAddr string, generation uint64, target *predictiveTarget) {
	defer p.wg.Done()
	started := time.Now()
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ConnectTimeout)
	defer cancel()

	conn, err := p.transport.dialTunnel(ctx, proxyAddr, target.key.network, target.key.address)
	duration := time.Since(started)
	p.lastDialNS.Store(int64(duration))
	xmetrics.GetObserver(xmetrics.MetricPredictiveEstablishDurationObserver, p.labels).Observe(duration.Seconds())

	accepted := false
	available := 0
	current := false
	p.mu.Lock()
	p.creating--
	target.creating--
	current = p.targets[target.key] == target && p.proxyAddr == proxyAddr && p.generation == generation
	if err == nil && conn != nil && !p.closed && current && target.hot && len(target.idle) < p.cfg.Size {
		now := time.Now()
		target.idle = append(target.idle, &predictiveEntry{
			conn:      conn,
			createdAt: started,
			idleAt:    now,
		})
		accepted = true
		available = len(target.idle)
		p.setAvailableMetricLocked()
	}
	if accepted {
		p.scheduleAllLocked()
	}
	p.mu.Unlock()

	if !accepted && conn != nil {
		_ = conn.Close()
	}
	if accepted && p.log != nil {
		p.log.WithFields(map[string]any{
			"event":     "predictive_established",
			"proxy":     proxyAddr,
			"target":    target.key.address,
			"available": available,
			"capacity":  p.cfg.Size,
			"duration":  duration.String(),
		}).Debug("predictive CONNECT tunnel established")
	}
	if err != nil && current && p.ctx.Err() == nil {
		p.failures.Add(1)
		xmetrics.GetCounter(xmetrics.MetricPredictiveFailuresCounter, p.labels).Inc()
		if p.log != nil {
			p.log.WithFields(map[string]any{
				"event":  "predictive_failure",
				"proxy":  proxyAddr,
				"target": target.key.address,
			}).Debugf("predictive CONNECT establishment failed: %v", err)
		}
	}
}

func (p *predictivePool) maintain() {
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

func (p *predictivePool) expireAndRefill() {
	now := time.Now()
	var (
		closed  []*predictiveEntry
		expired int
		demoted []string
	)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	for key, target := range p.targets {
		if !target.lastSeen.IsZero() && now.Sub(target.lastSeen) >= p.cfg.Window {
			delete(p.targets, key)
			closed = append(closed, target.idle...)
			target.idle = nil
			if target.hot {
				demoted = append(demoted, target.key.address)
			}
			continue
		}

		n := 0
		for _, entry := range target.idle {
			if entry.expired(now, p.cfg) {
				closed = append(closed, entry)
				expired++
				continue
			}
			target.idle[n] = entry
			n++
		}
		target.idle = target.idle[:n]
	}
	p.setAvailableMetricLocked()
	p.setHotMetricLocked()
	p.scheduleAllLocked()
	p.mu.Unlock()

	closePredictiveEntries(closed)
	for range expired {
		p.recordStale()
	}
	if p.log != nil {
		for _, address := range demoted {
			p.log.WithFields(map[string]any{
				"event":  "predictive_demoted",
				"target": address,
			}).Debug("CONNECT target expired")
		}
	}
}

func (p *predictivePool) recordStale() {
	p.stale.Add(1)
	xmetrics.GetCounter(xmetrics.MetricPredictiveStaleCounter, p.labels).Inc()
}

func (p *predictivePool) setAvailableMetricLocked() {
	available := 0
	for _, target := range p.targets {
		available += len(target.idle)
	}
	xmetrics.GetGauge(xmetrics.MetricPredictiveAvailableGauge, p.labels).Set(float64(available))
}

func (p *predictivePool) setHotMetricLocked() {
	hot := 0
	for _, target := range p.targets {
		if target.hot {
			hot++
		}
	}
	xmetrics.GetGauge(xmetrics.MetricPredictiveHotEndpointsGauge, p.labels).Set(float64(hot))
}

func (p *predictivePool) stats() PredictiveStats {
	p.mu.Lock()
	tracked, hot, available := len(p.targets), 0, 0
	for _, target := range p.targets {
		if target.hot {
			hot++
		}
		available += len(target.idle)
	}
	creating := p.creating
	p.mu.Unlock()
	return PredictiveStats{
		TrackedEndpoints:      tracked,
		HotEndpoints:          hot,
		Available:             available,
		Creating:              creating,
		Hits:                  p.hits.Load(),
		Misses:                p.misses.Load(),
		Stale:                 p.stale.Load(),
		ReplenishmentFailures: p.failures.Load(),
		LastEstablishDuration: time.Duration(p.lastDialNS.Load()),
	}
}

func (p *predictivePool) close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancel()
	var idle []*predictiveEntry
	for _, target := range p.targets {
		idle = append(idle, target.idle...)
		target.idle = nil
	}
	p.targets = make(map[predictiveKey]*predictiveTarget)
	p.setAvailableMetricLocked()
	p.setHotMetricLocked()
	p.mu.Unlock()

	closePredictiveEntries(idle)
	p.wg.Wait()
	return nil
}

func closePredictiveEntries(entries []*predictiveEntry) {
	for _, entry := range entries {
		if entry != nil && entry.conn != nil {
			_ = entry.conn.Close()
		}
	}
}

func predictiveConnAlive(conn net.Conn, timeout time.Duration) (net.Conn, bool) {
	if conn == nil {
		return nil, false
	}
	if timeout <= 0 {
		return conn, true
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, false
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
		return nil, false
	}
	if n > 0 {
		return &prefixedConn{Conn: conn, prefix: append([]byte(nil), b[:n]...)}, true
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return conn, true
	}
	return nil, false
}

// prefixedConn preserves bytes sent by a server-first destination while a
// pooled tunnel is checked for liveness.
type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}
