package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/boxmgr"
	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

// Logger defines logging interface.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type Option func(*Manager)

func WithLogger(l Logger) Option { return func(m *Manager) { m.logger = l } }

// runtimeManager makes the refresh transaction testable without opening proxy listeners.
type runtimeManager interface {
	CurrentConfig() *config.Config
	CurrentPortMap() map[string]uint16
	ReloadWithPortMap(*config.Config, map[string]uint16) error
}

// transactionalRuntime serializes snapshot, node/settings mutation, reload,
// persistence and rollback with runtime management operations.
type transactionalRuntime interface {
	ReloadWithCurrentConfig(update func(*config.Config) error, persist func(*config.Config) error) error
}

type subscriptionConfigRuntime interface {
	CurrentSubscriptionConfig() *config.Config
}

type refreshCall struct {
	done    chan struct{}
	version uint64
	err     error // Published by closing done.
}

// Manager owns one worker and one immutable lifetime context. Disabling the
// timer never disables manual refresh; updating URLs never replaces the worker.
type Manager struct {
	mu         sync.RWMutex
	baseCfg    *config.Config // Private copy; never shares mutable config with the WebUI/runtime.
	boxMgr     runtimeManager
	logger     Logger
	httpClient *http.Client
	status     monitor.SubscriptionStatus
	ctx        context.Context
	cancel     context.CancelFunc
	startOnce  sync.Once
	loopDone   chan struct{}
	updates    chan struct{}
	requests   chan *refreshCall
	stopped    bool
	version    uint64
	active     *refreshCall
	pending    *refreshCall
	// Explicit manager edits may precede runtime publication (direct callers,
	// unlike the WebUI). Only that known baseline may temporarily differ from
	// the private fetch config; unrelated runtime reloads are authoritative.
	pendingSettings bool
	pendingBaseline *config.Config

	lastSubHash      string
	lastNodesModTime time.Time
	lastNodesSize    int64
}

func cloneConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return &config.Config{}
	}
	copyCfg := *cfg
	copyCfg.Nodes = append([]config.NodeConfig(nil), cfg.Nodes...)
	copyCfg.Subscriptions = append([]string(nil), cfg.Subscriptions...)
	if cfg.Management.Enabled != nil {
		value := *cfg.Management.Enabled
		copyCfg.Management.Enabled = &value
	}
	if cfg.Pool.RetryEnabled != nil {
		value := *cfg.Pool.RetryEnabled
		copyCfg.Pool.RetryEnabled = &value
	}
	if cfg.Sticky.SessionEnabled != nil {
		value := *cfg.Sticky.SessionEnabled
		copyCfg.Sticky.SessionEnabled = &value
	}
	return &copyCfg
}

func New(cfg *config.Config, boxMgr *boxmgr.Manager, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 10,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		ExpectContinueTimeout: time.Second, ResponseHeaderTimeout: 10 * time.Second,
	}
	m := &Manager{
		baseCfg: cloneConfig(cfg), ctx: ctx, cancel: cancel,
		loopDone: make(chan struct{}), updates: make(chan struct{}, 1), requests: make(chan *refreshCall, 1),
		httpClient: &http.Client{Transport: transport},
	}
	// Do not store a typed-nil pointer in the interface.
	if boxMgr != nil {
		m.boxMgr = boxMgr
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	var subscriptionNodes []config.NodeConfig
	for _, node := range m.baseCfg.Nodes {
		if node.Source == config.NodeSourceSubscription {
			subscriptionNodes = append(subscriptionNodes, node)
		}
	}
	m.status.NodeCount = len(subscriptionNodes)
	if len(subscriptionNodes) > 0 {
		m.lastSubHash = m.computeNodesHash(subscriptionNodes)
		if info, err := os.Stat(nodesFilePath(m.baseCfg)); err == nil {
			m.lastNodesModTime, m.lastNodesSize = info.ModTime(), info.Size()
		}
	}
	return m
}

// Start starts the worker even when periodic refresh is disabled or URLs are
// not yet configured. It is idempotent.
func (m *Manager) Start() {
	m.startOnce.Do(func() { go m.refreshLoop() })
}

// Stop cancels in-flight fetches and joins the worker before runtime shutdown.
func (m *Manager) Stop() {
	m.mu.Lock()
	m.stopped = true
	m.status.NextRefresh = time.Time{}
	m.cancel()
	m.mu.Unlock()
	m.Start()
	<-m.loopDone
	m.httpClient.CloseIdleConnections()
}

func (m *Manager) updateConfig(urls []string, enabled bool, interval time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return errors.New("subscription manager stopped")
	}
	candidate := cloneConfig(m.baseCfg)
	candidate.Subscriptions = append([]string(nil), urls...)
	candidate.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		candidate.SubscriptionRefresh.Interval = interval
	}
	if candidate.SubscriptionRefresh.Interval <= 0 {
		candidate.SubscriptionRefresh.Interval = time.Hour
	}
	if err := candidate.SaveSubscriptionSettings(); err != nil {
		return fmt.Errorf("save subscription config: %w", err)
	}
	current := m.runtimeSubscriptionConfig()
	m.pendingSettings = current != nil && !sameSubscriptionConfig(current, candidate)
	m.pendingBaseline = current
	m.baseCfg = candidate
	m.version++
	select {
	case m.updates <- struct{}{}:
	default:
	}
	return nil
}

// UpdateConfig updates only subscription-owned settings and queues a refresh.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	if err := m.updateConfig(urls, enabled, interval); err != nil {
		m.logger.Errorf("%v", err)
		return
	}
	m.Start()
	if len(urls) > 0 {
		_, _ = m.requestRefresh()
	}
}

func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	return m.UpdateConfigAndRefreshContext(context.Background(), urls, enabled, interval)
}

// UpdateConfigAndRefreshContext cancels the caller's wait, not a refresh shared
// with other callers. The refresh itself is canceled by Stop.
func (m *Manager) UpdateConfigAndRefreshContext(ctx context.Context, urls []string, enabled bool, interval time.Duration) error {
	if err := m.updateConfig(urls, enabled, interval); err != nil {
		return err
	}
	m.Start()
	if len(urls) == 0 {
		return nil
	}
	return m.RefreshNowContext(ctx)
}

func (m *Manager) RefreshNow() error { return m.RefreshNowContext(context.Background()) }

func (m *Manager) requestRefresh() (*refreshCall, error) {
	m.Start()
	m.syncRuntimeConfig()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return nil, errors.New("subscription manager stopped")
	}
	if len(m.baseCfg.Subscriptions) == 0 {
		return nil, errors.New("no subscription URLs configured")
	}
	if m.pending != nil {
		// An unstarted refresh always uses the latest settings, so rapid edits
		// collapse into one follow-up refresh rather than an unbounded queue.
		m.pending.version = m.version
		return m.pending, nil
	}
	if m.active != nil && m.active.version == m.version {
		return m.active, nil
	}
	call := &refreshCall{done: make(chan struct{}), version: m.version}
	m.pending = call
	m.requests <- call // One pending call; the channel is necessarily empty.
	return call, nil
}

func (m *Manager) RefreshNowContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	call, err := m.requestRefresh()
	if err != nil {
		return err
	}
	m.mu.RLock()
	refresh := m.baseCfg.SubscriptionRefresh
	urls := len(m.baseCfg.Subscriptions)
	m.mu.RUnlock()
	timeout := refresh.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	concurrency := refresh.FetchConcurrency
	if concurrency <= 0 {
		concurrency = 16
	}
	if concurrency > 32 {
		concurrency = 32
	}
	// Fetch timeout is per URL, not for the entire batch. Account for queued
	// batches and core validation/draining rather than timing out prematurely.
	batches := (urls + concurrency - 1) / concurrency
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(batches)*timeout+refresh.HealthCheckTimeout+refresh.DrainTimeout)
	defer cancel()
	select {
	case <-call.done:
		return call.err
	case <-m.ctx.Done():
		return errors.New("subscription manager stopped")
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

func (m *Manager) Status() monitor.SubscriptionStatus {
	m.syncRuntimeConfig()
	modified := m.CheckNodesModified()
	m.mu.RLock()
	defer m.mu.RUnlock()
	status := m.status
	status.NodesModified = modified
	return status
}

func (m *Manager) refreshLoop() {
	defer close(m.loopDone)
	var timer *time.Timer
	var timerCh <-chan time.Time
	resetTimer := func() {
		m.syncRuntimeConfig()
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		interval := m.baseCfg.SubscriptionRefresh.Interval
		if interval <= 0 {
			interval = time.Hour
		}
		if !m.stopped && m.baseCfg.SubscriptionRefresh.Enabled && len(m.baseCfg.Subscriptions) > 0 {
			if timer == nil {
				timer = time.NewTimer(interval)
			} else {
				timer.Reset(interval)
			}
			timerCh = timer.C
			m.status.NextRefresh = time.Now().Add(interval)
		} else {
			timerCh = nil
			m.status.NextRefresh = time.Time{}
		}
	}
	resetTimer()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		m.mu.Lock()
		if m.pending != nil {
			m.pending.err = errors.New("subscription manager stopped")
			close(m.pending.done)
			m.pending = nil
		}
		m.status.NextRefresh = time.Time{}
		m.mu.Unlock()
	}()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.updates:
			resetTimer()
		case <-timerCh:
			// A file/runtime reload can disable the timer without calling
			// UpdateConfig. Recheck authority before honoring an old timer tick.
			m.syncRuntimeConfig()
			m.mu.RLock()
			autoEnabled := m.baseCfg.SubscriptionRefresh.Enabled && len(m.baseCfg.Subscriptions) > 0
			m.mu.RUnlock()
			if autoEnabled {
				_, _ = m.requestRefresh()
			} else {
				resetTimer()
			}
		case call := <-m.requests:
			m.mu.Lock()
			m.pending = nil
			m.active = call
			call.version = m.version
			cfg := cloneConfig(m.baseCfg)
			m.status.IsRefreshing = true
			m.mu.Unlock()
			err := m.doRefresh(cfg, call.version)
			m.mu.Lock()
			m.status.IsRefreshing = false
			m.status.RefreshCount++
			m.status.LastRefresh = time.Now()
			m.status.LastError = ""
			if err != nil {
				m.status.LastError = err.Error()
			}
			call.err = err
			close(call.done)
			m.active = nil
			m.mu.Unlock()
			if err != nil {
				m.logger.Errorf("subscription refresh failed: %v", err)
			}
			resetTimer()
		}
	}
}

// doRefresh validates/reloads before touching the persisted cache. A failed
// cache write rolls back the runtime as well; rollback errors are reported.
func (m *Manager) doRefresh(cfg *config.Config, version uint64) error {
	m.logger.Infof("starting subscription refresh")
	var startingConfig *config.Config
	if m.boxMgr != nil {
		startingConfig = m.boxMgr.CurrentConfig()
	}
	nodes, err := m.fetchSubscriptions(cfg)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("no nodes fetched from subscriptions")
	}
	if err := m.ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	stale := m.version != version
	m.mu.RUnlock()
	if stale {
		return errors.New("subscription configuration changed during refresh; newer refresh queued")
	}
	if m.boxMgr == nil {
		return errors.New("proxy runtime is not initialized")
	}

	var nodesFile string
	if runtime, ok := m.boxMgr.(transactionalRuntime); ok {
		err := runtime.ReloadWithCurrentConfig(func(current *config.Config) error {
			if err := m.ctx.Err(); err != nil {
				return err
			}
			m.mu.RLock()
			stale := m.version != version
			m.mu.RUnlock()
			if stale {
				return errors.New("subscription configuration changed during refresh; newer refresh queued")
			}
			// The WebUI publishes under the runtime lock before it updates this
			// worker's private fetch settings. Do not overwrite that newer
			// publication in the narrow interval between these two operations.
			if !sameSubscriptionConfig(current, cfg) && !sameSubscriptionConfig(current, startingConfig) {
				return errors.New("subscription configuration changed during refresh; preserving newly published settings")
			}
			candidate := createConfig(current, nodes)
			candidate.Subscriptions = append([]string(nil), cfg.Subscriptions...)
			candidate.SubscriptionRefresh = cfg.SubscriptionRefresh
			*current = *candidate
			return nil
		}, func(candidate *config.Config) error {
			nodesFile = nodesFilePath(candidate)
			return m.writeNodesToFile(nodesFile, nodes)
		})
		if err != nil {
			return fmt.Errorf("refresh transaction: %w", err)
		}
		m.recordRefresh(nodesFile, nodes)
		return nil
	}

	// Fetch may be slow. Read the current config only now, preserving settings
	// changes and inline-node CRUD performed while the HTTP fetch was running.
	oldCfg := m.boxMgr.CurrentConfig()
	if oldCfg == nil {
		return errors.New("proxy runtime configuration is not initialized")
	}
	if !sameSubscriptionConfig(oldCfg, cfg) && !sameSubscriptionConfig(oldCfg, startingConfig) {
		return errors.New("subscription configuration changed during refresh; preserving newly published settings")
	}
	newCfg := createConfig(oldCfg, nodes)
	newCfg.Subscriptions = append([]string(nil), cfg.Subscriptions...)
	newCfg.SubscriptionRefresh = cfg.SubscriptionRefresh
	portMap := oldCfg.BuildPortMap()
	if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
		return fmt.Errorf("reload: %w", err)
	}
	nodesFile = nodesFilePath(newCfg)
	if err := m.writeNodesToFile(nodesFile, nodes); err != nil {
		rollbackErr := m.boxMgr.ReloadWithPortMap(oldCfg, portMap)
		if rollbackErr != nil {
			return fmt.Errorf("persist subscription cache: %w; runtime rollback failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("persist subscription cache: %w; runtime rolled back", err)
	}
	m.recordRefresh(nodesFile, nodes)
	return nil
}

func sameSubscriptionConfig(a, b *config.Config) bool {
	return a != nil && b != nil && slices.Equal(a.Subscriptions, b.Subscriptions) && a.SubscriptionRefresh == b.SubscriptionRefresh
}

// runtimeSubscriptionConfig must be called without holding runtime lifecycle
// or config locks. The lightweight optional API avoids copying large node sets
// when the dashboard polls Status.
func (m *Manager) runtimeSubscriptionConfig() *config.Config {
	if m.boxMgr == nil {
		return nil
	}
	if runtime, ok := m.boxMgr.(subscriptionConfigRuntime); ok {
		return runtime.CurrentSubscriptionConfig()
	}
	return m.boxMgr.CurrentConfig()
}

func (m *Manager) syncRuntimeConfig() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	current := m.runtimeSubscriptionConfig()
	if current == nil {
		return
	}
	if sameSubscriptionConfig(current, m.baseCfg) {
		m.pendingSettings = false
		m.pendingBaseline = nil
		return
	}
	if m.pendingSettings && sameSubscriptionConfig(current, m.pendingBaseline) {
		return // A deliberate direct API edit is waiting to be activated.
	}
	// A different runtime/file reload supersedes stale worker settings. Never
	// persist this synchronization: the authoritative publisher owns disk.
	m.baseCfg.Subscriptions = append([]string(nil), current.Subscriptions...)
	m.baseCfg.SubscriptionRefresh = current.SubscriptionRefresh
	m.baseCfg.NodesFile = current.NodesFile
	if current.FilePath() != "" {
		m.baseCfg.SetFilePath(current.FilePath())
	}
	m.pendingSettings = false
	m.pendingBaseline = nil
	m.version++
	if !current.SubscriptionRefresh.Enabled || len(current.Subscriptions) == 0 {
		m.status.NextRefresh = time.Time{}
	}
	select {
	case m.updates <- struct{}{}:
	default:
	}
}

func (m *Manager) recordRefresh(nodesFile string, nodes []config.NodeConfig) {
	m.mu.Lock()
	m.lastSubHash = m.computeNodesHash(nodes)
	if info, err := os.Stat(nodesFile); err == nil {
		m.lastNodesModTime, m.lastNodesSize = info.ModTime(), info.Size()
	}
	m.status.NodesModified = false
	m.status.NodeCount = len(nodes)
	m.mu.Unlock()
	m.logger.Infof("subscription refresh completed, %d subscription nodes active", len(nodes))
}

func nodesFilePath(cfg *config.Config) string {
	if cfg.NodesFile != "" {
		return cfg.NodesFile
	}
	return filepath.Join(filepath.Dir(cfg.FilePath()), "nodes.txt")
}

func (m *Manager) getNodesFilePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return nodesFilePath(m.baseCfg)
}

func (m *Manager) writeNodesToFile(path string, nodes []config.NodeConfig) error {
	return config.WriteNodesToFile(path, nodes)
}

func (m *Manager) computeNodesHash(nodes []config.NodeConfig) string {
	uris := make([]string, 0, len(nodes))
	for _, node := range nodes {
		uris = append(uris, node.URI)
	}
	hash := sha256.Sum256([]byte(strings.Join(uris, "\n")))
	return hex.EncodeToString(hash[:])
}

// CheckNodesModified caches the comparison result, not just the stat timestamp.
// A dirty node file stays dirty across polling until it is restored or refreshed.
func (m *Manager) CheckNodesModified() bool {
	m.mu.RLock()
	lastHash, lastMod, lastSize, modified := m.lastSubHash, m.lastNodesModTime, m.lastNodesSize, m.status.NodesModified
	path := nodesFilePath(m.baseCfg)
	m.mu.RUnlock()
	if lastHash == "" {
		return modified
	}
	info, err := os.Stat(path)
	if err != nil {
		m.mu.Lock()
		m.status.NodesModified = true
		m.mu.Unlock()
		return true
	}
	if info.ModTime().Equal(lastMod) && info.Size() == lastSize {
		return modified
	}
	nodes, err := config.LoadNodesFromFile(path)
	if err != nil {
		return modified
	}
	changed := m.computeNodesHash(nodes) != lastHash
	m.mu.Lock()
	// A refresh may have established a new baseline while this file was read.
	if m.lastSubHash == lastHash {
		m.lastNodesModTime, m.lastNodesSize = info.ModTime(), info.Size()
		m.status.NodesModified = changed
	} else {
		changed = m.status.NodesModified
	}
	m.mu.Unlock()
	return changed
}

func (m *Manager) MarkNodesModified() {
	m.mu.Lock()
	m.status.NodesModified = true
	m.mu.Unlock()
}

func (m *Manager) fetchSubscriptions(cfg *config.Config) ([]config.NodeConfig, error) {
	nodes, stats := config.FetchSubscriptionNodes(m.ctx, cfg.Subscriptions, config.SubscriptionFetchOptions{
		Timeout: cfg.SubscriptionRefresh.Timeout, Concurrency: cfg.SubscriptionRefresh.FetchConcurrency,
		Client: m.httpClient, Loggerf: m.logger.Infof,
	})
	// A running proxy already has a usable last-known-good set. Do not silently
	// report success or discard one provider's nodes when any fetch failed.
	if stats.Failed > 0 || stats.Empty > 0 {
		return nil, fmt.Errorf("subscription refresh incomplete (%d/%d URLs failed, %d returned no usable nodes); keeping current nodes: %v", stats.Failed, stats.UniqueURLs, stats.Empty, stats.LastError)
	}
	return nodes, nil
}

// createNewConfig is also used by focused node-source tests.
func (m *Manager) createNewConfig(nodes []config.NodeConfig) *config.Config {
	m.mu.RLock()
	base := cloneConfig(m.baseCfg)
	m.mu.RUnlock()
	return createConfig(base, nodes)
}

func createConfig(base *config.Config, nodes []config.NodeConfig) *config.Config {
	newCfg := cloneConfig(base)
	newCfg.Nodes = nil
	for _, node := range base.Nodes {
		if node.Source == config.NodeSourceInline {
			newCfg.Nodes = append(newCfg.Nodes, node)
		}
	}
	for _, node := range nodes {
		node.Source = config.NodeSourceSubscription
		newCfg.Nodes = append(newCfg.Nodes, node)
	}
	for i := range newCfg.Nodes {
		node := &newCfg.Nodes[i]
		node.Name, node.URI = strings.TrimSpace(node.Name), strings.TrimSpace(node.URI)
		if node.Name == "" {
			node.Name = config.ExtractNodeName(node.URI)
		}
		if node.Name == "" {
			node.Name = fmt.Sprintf("node-%d", i)
		}
	}
	return newCfg
}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) { log.Printf("[subscription] "+format, args...) }
func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[subscription] WARN: "+format, args...)
}
func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[subscription] ERROR: "+format, args...)
}
