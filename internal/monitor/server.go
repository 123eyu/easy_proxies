package monitor

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/geoip"
	"easy_proxies/internal/routing"
)

//go:embed assets/index.html
var embeddedFS embed.FS

// Session represents a user session with expiration.
type Session struct {
	Token     string
	CreatedAt time.Time
	ExpiresAt time.Time
}

// NodeManager exposes config node CRUD and reload operations.
type NodeManager interface {
	ListConfigNodes(ctx context.Context) ([]config.NodeConfig, error)
	CreateNode(ctx context.Context, node config.NodeConfig) (config.NodeConfig, error)
	UpdateNode(ctx context.Context, name string, node config.NodeConfig) (config.NodeConfig, error)
	DeleteNode(ctx context.Context, name string) error
	TriggerReload(ctx context.Context) error
}

// settingsLocker shares the manager's lifecycle/CRUD lock when available. The
// lock order is manager first, then cfgMu, matching manager -> SetConfig calls.
type settingsLocker interface {
	LockSettings() func()
}

// Sentinel errors for node operations.
var (
	ErrNodeNotFound = errors.New("节点不存在")
	ErrNodeConflict = errors.New("节点名称或端口已存在")
	ErrInvalidNode  = errors.New("无效的节点配置")
)

// SubscriptionRefresher interface for subscription manager.
type SubscriptionRefresher interface {
	RefreshNow() error
	Status() SubscriptionStatus
	UpdateConfig(urls []string, enabled bool, interval time.Duration)
	UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error
}

// Context-aware refreshers stop waiting when an HTTP client disconnects, while
// the background refresh remains governed by the subscription manager lifecycle.
type contextualSubscriptionRefresher interface {
	RefreshNowContext(context.Context) error
	UpdateConfigAndRefreshContext(context.Context, []string, bool, time.Duration) error
}

// SubscriptionStatus represents subscription refresh status.
type SubscriptionStatus struct {
	LastRefresh   time.Time `json:"last_refresh"`
	NextRefresh   time.Time `json:"next_refresh"`
	NodeCount     int       `json:"node_count"`
	LastError     string    `json:"last_error,omitempty"`
	RefreshCount  int       `json:"refresh_count"`
	IsRefreshing  bool      `json:"is_refreshing"`
	NodesModified bool      `json:"nodes_modified"` // True if nodes.txt was modified since last refresh
}

// Server exposes HTTP endpoints for monitoring.
type Server struct {
	cfg          Config
	cfgMu        sync.RWMutex   // 保护动态配置字段
	cfgSrc       *config.Config // 可持久化的配置对象
	routingStore *routing.Store // Proxy affinity bindings, separate from login sessions.
	mgr          *Manager
	srv          *http.Server
	logger       *log.Logger

	// Session management
	sessionMu  sync.RWMutex
	sessions   map[string]*Session
	sessionTTL time.Duration

	ctx           context.Context
	cancel        context.CancelFunc
	startOnce     sync.Once
	trafficClient *http.Client

	// probeAllInFlight bounds batch "probe all" to a single concurrent run.
	// Without it, N simultaneous requests each spin up to `concurrency` probes,
	// multiplying total in-flight dials and starving host fd/memory limits.
	probeAllInFlight atomic.Bool

	subRefresher SubscriptionRefresher
	nodeMgr      NodeManager
}

// NewServer constructs a server; it can be nil when disabled.
func NewServer(cfg Config, mgr *Manager, logger *log.Logger) *Server {
	if !cfg.Enabled || mgr == nil {
		return nil
	}
	if logger == nil {
		logger = log.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:        cfg,
		mgr:        mgr,
		logger:     logger,
		sessions:   make(map[string]*Session),
		sessionTTL: 24 * time.Hour,
		ctx:        ctx,
		cancel:     cancel,
		trafficClient: &http.Client{Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ResponseHeaderTimeout: 5 * time.Second,
			MaxIdleConns:          2, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second,
		}},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/auth", s.handleAuth)
	mux.HandleFunc("/api/settings", s.withAuth(s.handleSettings))
	mux.HandleFunc("/api/proxy-sessions", s.withAuth(s.handleProxySessions))
	mux.HandleFunc("/api/proxy-sessions/links", s.withAuth(s.handleProxySessionLinks))
	mux.HandleFunc("/api/proxy-sessions/", s.withAuth(s.handleProxySessionItem))
	mux.HandleFunc("/api/nodes", s.withAuth(s.handleNodes))
	mux.HandleFunc("/api/nodes/config", s.withAuth(s.handleConfigNodes))
	mux.HandleFunc("/api/nodes/config/", s.withAuth(s.handleConfigNodeItem))
	mux.HandleFunc("/api/nodes/probe-all", s.withAuth(s.handleProbeAll))
	mux.HandleFunc("/api/nodes/", s.withAuth(s.handleNodeAction))
	mux.HandleFunc("/api/debug", s.withAuth(s.handleDebug))
	mux.HandleFunc("/api/export", s.withAuth(s.handleExport))
	mux.HandleFunc("/api/subscription/status", s.withAuth(s.handleSubscriptionStatus))
	mux.HandleFunc("/api/subscription/refresh", s.withAuth(s.handleSubscriptionRefresh))
	mux.HandleFunc("/api/subscription/config", s.withAuth(s.handleSubscriptionConfig))
	mux.HandleFunc("/api/reload", s.withAuth(s.handleReload))
	mux.HandleFunc("/api/traffic", s.withAuth(s.handleTraffic))
	mux.HandleFunc("/api/logs", s.withAuth(s.handleLogs))
	s.srv = &http.Server{
		Addr: cfg.Listen, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 32 * 1024,
		BaseContext:    func(net.Listener) context.Context { return s.ctx },
	}
	return s
}

// SetSubscriptionRefresher sets the subscription refresher for API endpoints.
func (s *Server) SetSubscriptionRefresher(sr SubscriptionRefresher) {
	if s != nil {
		s.cfgMu.Lock()
		s.subRefresher = sr
		s.cfgMu.Unlock()
	}
}

// SetNodeManager enables config-node CRUD endpoints.
func (s *Server) SetNodeManager(nm NodeManager) {
	if s != nil {
		s.cfgMu.Lock()
		s.nodeMgr = nm
		s.cfgMu.Unlock()
	}
}

func (s *Server) subscriptionRefresher() SubscriptionRefresher {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.subRefresher
}

func (s *Server) nodeManager() NodeManager {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.nodeMgr
}

// SetConfig binds the persistable config object for settings API.
func (s *Server) SetConfig(cfg *config.Config) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	// The supplied snapshot is authoritative, including explicit empty
	// subscriptions and zero/default refresh settings. Never backfill or mutate
	// fields owned by the node/subscription lifecycle here.
	s.cfgSrc = cfg
	if cfg != nil {
		s.setPasswordLocked(cfg.Management.Password)
		s.cfg.ExternalIP = cfg.ExternalIP
		s.cfg.ProbeTarget = cfg.Management.ProbeTarget
		s.cfg.SkipCertVerify = cfg.SkipCertVerify
		// Sync probe concurrency to the manager so periodic health checks pick
		// up WebUI changes after a reload (batch probes read it per request).
		if s.mgr != nil {
			s.mgr.SetProbeConcurrency(cfg.ProbeConcurrencyOrDefault())
			// Re-derive the probe destination and strict-TLS mode so changes to
			// probe_target / skip_cert_verify take effect on the long-lived
			// manager without a full process restart.
			s.mgr.SetProbeTarget(cfg.Management.ProbeTarget, cfg.SkipCertVerify)
		}
		// Sync proxy credentials based on mode
		if cfg.Mode == "multi-port" || cfg.Mode == "hybrid" {
			s.cfg.ProxyUsername = cfg.MultiPort.Username
			s.cfg.ProxyPassword = cfg.MultiPort.Password
		} else {
			s.cfg.ProxyUsername = cfg.Listener.Username
			s.cfg.ProxyPassword = cfg.Listener.Password
		}
	}
}

// currentProbeConcurrency returns the probe concurrency from the live config,
// clamped to a safe range. Read per batch-probe request so WebUI changes apply
// after a reload without restarting the process.
func (s *Server) currentProbeConcurrency() int64 {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfgSrc != nil {
		return int64(s.cfgSrc.ProbeConcurrencyOrDefault())
	}
	return 32
}

// Start launches the HTTP server.
func (s *Server) Start(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.startOnce.Do(func() {
		if s.ctx.Err() != nil {
			return
		}
		listener, err := net.Listen("tcp", s.srv.Addr)
		if err != nil {
			s.logger.Printf("❌ Monitor server error: %v", err)
			return
		}
		s.logger.Printf("✅ Monitor server started on http://%s", listener.Addr())
		go s.cleanupExpiredSessions()
		go func() {
			if err := s.srv.Serve(listener); err != nil && err != http.ErrServerClosed {
				s.logger.Printf("❌ Monitor server error: %v", err)
			}
		}()
		go func() {
			select {
			case <-ctx.Done():
				s.Shutdown(context.Background())
			case <-s.ctx.Done():
			}
		}()
	})
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.trafficClient != nil {
		s.trafficClient.CloseIdleConnections()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	if err := s.srv.Shutdown(ctx); err != nil {
		// Long-lived SSE streams need an explicit close after the drain budget.
		_ = s.srv.Close()
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	data, err := embeddedFS.ReadFile("assets/index.html")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// 返回全量节点，前端据此按状态统计（健康/拉黑/异常）并展示可解封的拉黑节点。
	// 之前只返回 SnapshotFiltered(true)，导致 dashboard 的拉黑/异常计数恒为 0，
	// 且拉黑节点不出现在表格里、无法解封。
	allNodes := s.mgr.Snapshot()
	totalNodes := len(allNodes)

	// Calculate region statistics
	regionStats := make(map[string]int)
	regionHealthy := make(map[string]int)
	for _, snap := range allNodes {
		region := snap.Region
		if region == "" {
			region = "other"
		}
		regionStats[region]++
		// Count healthy nodes per region
		if snap.InitialCheckDone && snap.Available && !snap.Blacklisted {
			regionHealthy[region]++
		}
	}

	sweepActive, sweepDone, sweepTotal, sweepOK, sweepFail := s.mgr.ProbeSweepProgress()
	payload := map[string]any{
		"nodes":          allNodes,
		"total_nodes":    totalNodes,
		"region_stats":   regionStats,
		"region_healthy": regionHealthy,
		"probe_sweep": map[string]any{
			"active":    sweepActive,
			"done":      sweepDone,
			"total":     sweepTotal,
			"available": sweepOK,
			"failed":    sweepFail,
		},
	}
	writeJSON(w, payload)
}

func (s *Server) handleDebug(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snapshots := s.mgr.Snapshot()
	var totalCalls, totalSuccess int64
	debugNodes := make([]map[string]any, 0, len(snapshots))
	for _, snap := range snapshots {
		totalCalls += snap.SuccessCount + int64(snap.FailureCount)
		totalSuccess += snap.SuccessCount
		debugNodes = append(debugNodes, map[string]any{
			"tag":                snap.Tag,
			"name":               snap.Name,
			"mode":               snap.Mode,
			"port":               snap.Port,
			"failure_count":      snap.FailureCount,
			"success_count":      snap.SuccessCount,
			"active_connections": snap.ActiveConnections,
			"last_latency_ms":    snap.LastLatencyMs,
			"last_success":       snap.LastSuccess,
			"last_failure":       snap.LastFailure,
			"last_error":         snap.LastError,
			"blacklisted":        snap.Blacklisted,
			"timeline":           snap.Timeline,
		})
	}
	var successRate float64
	if totalCalls > 0 {
		successRate = float64(totalSuccess) / float64(totalCalls) * 100
	}
	writeJSON(w, map[string]any{
		"nodes":         debugNodes,
		"total_calls":   totalCalls,
		"total_success": totalSuccess,
		"success_rate":  successRate,
	})
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tag := parts[0]
	if tag == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	switch action {
	case "probe":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		latency, err := s.mgr.Probe(ctx, tag)
		if err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		latencyMs := latency.Milliseconds()
		if latencyMs == 0 && latency > 0 {
			latencyMs = 1 // Round up sub-millisecond latencies to 1ms
		}
		writeJSON(w, map[string]any{"message": "探测成功", "latency_ms": latencyMs})
	case "release":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := s.mgr.Release(tag); err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": "已解除拉黑"})
	case "blacklist":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Duration string `json:"duration"` // e.g. "1h", "24h", "30m"
		}
		if r.ContentLength != 0 {
			if err := decodeManagementJSON(w, r, 4096, &req); err != nil {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "请求格式错误"})
				return
			}
		}
		if req.Duration == "" {
			req.Duration = "24h"
		}
		duration, err := time.ParseDuration(req.Duration)
		if err != nil || duration <= 0 {
			writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "duration 须为正数时间间隔"})
			return
		}
		if err := s.mgr.ManualBlacklist(tag, duration); err != nil {
			writeJSON(w, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"message": fmt.Sprintf("已拉黑 %s", duration)})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// handleProbeAll probes all nodes in batches and returns results via SSE
func (s *Server) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	_, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Only one batch probe-all may run at a time. A second concurrent request
	// would multiply total in-flight probes (each request bounds itself, but
	// not the others), so reject it cleanly instead.
	if !s.probeAllInFlight.CompareAndSwap(false, true) {
		busy, _ := json.Marshal(map[string]any{"type": "error", "message": "批量探测已在进行中，请稍候"})
		if err := writeSSE(w, busy); err != nil {
			return
		}
		return
	}
	defer s.probeAllInFlight.Store(false)

	// Get all nodes
	snapshots := s.mgr.Snapshot()
	total := len(snapshots)
	if total == 0 {
		emptyData, _ := json.Marshal(map[string]any{"type": "complete", "total": 0, "success": 0, "failed": 0})
		if err := writeSSE(w, emptyData); err != nil {
			return
		}
		return
	}

	// Send start event
	startData, _ := json.Marshal(map[string]any{"type": "start", "total": total})
	if err := writeSSE(w, startData); err != nil {
		return
	}

	// Read concurrency from the live config so WebUI changes take effect after
	// a reload (no process restart required). Workers are bounded per request
	// rather than creating one waiting goroutine for every registered node.
	concurrency := s.currentProbeConcurrency()

	// Create context with a timeout scaled to node count and concurrency so that
	// large inventories (e.g. thousands of nodes) are not cut off. Each probe
	// still has its own 10s deadline; here we bound the total wall time as
	// ceil(total / concurrency) * perProbe + slack, which is the expected
	// completion time given the worker limit. We deliberately do NOT cap this below
	// the estimate: a shorter deadline would cancel still-queued probes and
	// report reachable nodes as failures (which can then blacklist them). The
	// client can still abort early by closing the SSE connection (r.Context()).
	perProbe := 10 * time.Second
	batches := (int64(total) + concurrency - 1) / concurrency
	totalTimeout := time.Duration(batches)*perProbe + 30*time.Second
	if totalTimeout < 2*time.Minute {
		totalTimeout = 2 * time.Minute
	}
	if totalTimeout > 30*time.Minute && s.logger != nil {
		s.logger.Printf("⚠️  批量探测节点数较多(%d, 并发%d)，预计最长耗时约 %s", total, concurrency, totalTimeout.Round(time.Second))
	}
	ctx, cancel := context.WithTimeout(r.Context(), totalTimeout)
	defer cancel()

	// Probe all nodes using a fixed-size worker pool
	type probeResult struct {
		tag     string
		name    string
		latency int64
		err     string
	}
	workerCount := min(total, int(concurrency))
	jobs := make(chan Snapshot)
	results := make(chan probeResult, workerCount)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var snap Snapshot
				select {
				case <-ctx.Done():
					return
				case next, open := <-jobs:
					if !open {
						return
					}
					snap = next
				}
				probeCtx, probeCancel := context.WithTimeout(ctx, perProbe)
				latency, err := s.mgr.Probe(probeCtx, snap.Tag)
				probeCancel()
				result := probeResult{tag: snap.Tag, name: snap.Name, latency: latency.Milliseconds()}
				if err != nil {
					result.latency, result.err = -1, err.Error()
				} else if latency > 0 && result.latency == 0 {
					result.latency = 1
				}
				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, snap := range snapshots {
			select {
			case jobs <- snap:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	successCount := 0
	failedCount := 0
	count := 0

	for result := range results {
		count++
		if result.err != "" {
			failedCount++
		} else {
			successCount++
		}

		status := "success"
		if result.err != "" {
			status = "error"
		}

		eventPayload := map[string]any{
			"type":     "progress",
			"tag":      result.tag,
			"name":     result.name,
			"latency":  result.latency,
			"status":   status,
			"error":    result.err,
			"current":  count,
			"total":    total,
			"progress": float64(count) / float64(total) * 100,
		}
		eventData, _ := json.Marshal(eventPayload)
		if err := writeSSE(w, eventData); err != nil {
			return
		}
	}

	// Send complete event
	completeData, _ := json.Marshal(map[string]any{"type": "complete", "total": total, "success": successCount, "failed": failedCount})
	if err := writeSSE(w, completeData); err != nil {
		return
	}
}

// A stalled browser must not retain a probe batch or upstream traffic stream.
func writeSSE(w http.ResponseWriter, data []byte) error {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return controller.Flush()
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// withAuth protects management endpoints and prevents browser cross-origin
// mutation requests, including deployments intentionally configured without a password.
func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !sameOriginMutation(r) {
			writeStatusJSON(w, http.StatusForbidden, map[string]any{"error": "跨站请求已拒绝"})
			return
		}
		s.cfgMu.RLock()
		password := s.cfg.Password
		s.cfgMu.RUnlock()
		if password == "" || s.validateSession(requestSessionToken(r)) {
			next(w, r)
			return
		}
		writeStatusJSON(w, http.StatusUnauthorized, map[string]any{"error": "未授权，请先登录"})
	}
}

func sameOriginMutation(r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
		return true
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // Non-browser API clients do not send Origin.
	}
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, r.Host)
}

func requestSessionToken(r *http.Request) string {
	if header := r.Header.Get("Authorization"); header != "" {
		scheme, token, ok := strings.Cut(header, " ")
		if strings.EqualFold(scheme, "Bearer") {
			if ok {
				return token
			}
			return ""
		}
		// Reverse proxies may forward their own Basic/Digest authentication.
		// Only an explicit Bearer token overrides the management login cookie.
	}
	if cookie, err := r.Cookie("session_token"); err == nil {
		return cookie.Value
	}
	return ""
}

// setPasswordLocked updates live authentication and revokes old login sessions.
// The caller holds cfgMu; the lock order is cfgMu -> sessionMu everywhere.
func (s *Server) setPasswordLocked(password string) bool {
	if password == s.cfg.Password {
		return false
	}
	s.cfg.Password = password
	s.sessionMu.Lock()
	s.sessions = make(map[string]*Session)
	s.sessionMu.Unlock()
	return true
}

func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !sameOriginMutation(r) {
		writeStatusJSON(w, http.StatusForbidden, map[string]any{"error": "跨站请求已拒绝"})
		return
	}
	if r.Method == http.MethodDelete {
		s.sessionMu.Lock()
		delete(s.sessions, requestSessionToken(r))
		s.sessionMu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "session_token", Path: "/", HttpOnly: true,
			Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: -1})
		writeJSON(w, map[string]any{"message": "已退出登录"})
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.cfgMu.RLock()
	password := s.cfg.Password
	s.cfgMu.RUnlock()
	if r.Method == http.MethodGet || password == "" {
		writeJSON(w, map[string]any{"no_password": password == "", "authenticated": password == "" || s.validateSession(requestSessionToken(r))})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := decodeManagementJSON(w, r, 4096, &req); err != nil {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "请求格式错误"})
		return
	}
	if !secureCompareStrings(req.Password, password) {
		// Bound a failed attempt while allowing a disconnected request to stop immediately.
		timer := time.NewTimer(time.Duration(100+mathrand.Intn(200)) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		writeStatusJSON(w, http.StatusUnauthorized, map[string]any{"error": "密码错误"})
		return
	}
	// Do not mint a session for an old password after a concurrent password rotation.
	s.cfgMu.RLock()
	if password != s.cfg.Password {
		s.cfgMu.RUnlock()
		writeStatusJSON(w, http.StatusUnauthorized, map[string]any{"error": "密码已更新，请重新登录"})
		return
	}
	session, err := s.createSession()
	s.cfgMu.RUnlock()
	if err != nil {
		s.logger.Printf("Failed to create session: %v", err)
		writeStatusJSON(w, http.StatusInternalServerError, map[string]any{"error": "服务器错误"})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: "session_token", Value: session.Token, Path: "/", HttpOnly: true,
		Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode,
		MaxAge: int(s.sessionTTL.Seconds()),
	})
	writeJSON(w, map[string]any{"message": "登录成功", "token": session.Token})
}

// handleExport 导出所有可用代理池节点的代理 URI，每行一个。
// query 参数:
//   - scheme=http   (默认)
//   - scheme=socks5
//   - scheme=all    (同时导出 HTTP 和 SOCKS5)
//
// 在 pool/hybrid 模式下，还会导出 Pool 代理池入口和 GeoIP 分区路由入口。
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	scheme := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("scheme")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "socks5" && scheme != "all" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "invalid scheme, use http/socks5/all"})
		return
	}

	// all=true 时导出全部节点（不论死活）；否则只导出初始检查通过的可用节点。
	includeAll := false
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("all"))) {
	case "1", "true", "yes":
		includeAll = true
	}
	var snapshots []Snapshot
	if includeAll {
		snapshots = s.mgr.Snapshot()
	} else {
		snapshots = s.mgr.SnapshotFiltered(true)
	}
	var lines []string

	seen := make(map[string]bool)

	// Snapshot settings once so an export cannot mix credentials from two
	// concurrent settings updates, or borrow per-node credentials for the pool.
	s.cfgMu.RLock()
	mode := ""
	externalIP := s.cfg.ExternalIP
	username, password := s.cfg.ProxyUsername, s.cfg.ProxyPassword
	var listenerCfg config.ListenerConfig
	var geoipCfg config.GeoIPConfig
	if s.cfgSrc != nil {
		mode = s.cfgSrc.Mode
		externalIP = s.cfgSrc.ExternalIP
		listenerCfg = s.cfgSrc.Listener
		geoipCfg = s.cfgSrc.GeoIP
		if mode == "hybrid" || mode == "multi-port" {
			username, password = s.cfgSrc.MultiPort.Username, s.cfgSrc.MultiPort.Password
		} else {
			username, password = listenerCfg.Username, listenerCfg.Password
		}
	}
	s.cfgMu.RUnlock()

	exportHost := func(address string) string {
		if externalIP != "" {
			return proxyLinkHost(externalIP, r)
		}
		// Preserve a concrete listen address, but replace unspecified IPv4/IPv6
		// addresses with the request host rather than exporting a wildcard.
		return proxyLinkHost(address, r)
	}
	appendURI := func(protocol, address string, port uint16, user, pass string) {
		link := url.URL{Scheme: protocol, Host: net.JoinHostPort(exportHost(address), strconv.Itoa(int(port)))}
		if user != "" {
			link.User = url.UserPassword(user, pass)
		}
		uri := link.String()
		if !seen[uri] {
			lines = append(lines, uri)
			seen[uri] = true
		}
	}
	appendProxySchemes := func(address string, port uint16, user, pass string) {
		if scheme == "http" || scheme == "all" {
			appendURI("http", address, port, user, pass)
		}
		if scheme == "socks5" || scheme == "all" {
			appendURI("socks5", address, port, user, pass)
		}
	}

	// Pool 代理池入口（pool 或 hybrid 模式）
	if (mode == "pool" || mode == "hybrid") && listenerCfg.Port > 0 {
		lines = append(lines, "# Pool 代理池入口")
		appendProxySchemes(listenerCfg.Address, listenerCfg.Port, listenerCfg.Username, listenerCfg.Password)
	}

	// GeoIP 分区路由入口（保持其仅支持 HTTP 的导出行为）
	if geoipCfg.Enabled && geoipCfg.Port > 0 {
		regions := geoip.AllRegions()
		var pathParts []string
		for _, region := range regions {
			if region != "other" {
				pathParts = append(pathParts, fmt.Sprintf("/%s/", region))
			}
		}
		lines = append(lines, fmt.Sprintf("# GeoIP 分区路由入口 (支持路径: %s)", strings.Join(pathParts, " ")))
		appendURI("http", geoipCfg.Listen, geoipCfg.Port, listenerCfg.Username, listenerCfg.Password)
	}

	// Multi-port 独立节点，使用独立入口凭据，与 hybrid 的 pool 凭据区分。
	if len(snapshots) > 0 && (mode == "hybrid" || mode == "multi-port" || mode == "") {
		lines = append(lines, "# Multi-port 独立节点")
	}
	for _, snap := range snapshots {
		if snap.ListenAddress == "" || snap.Port == 0 {
			continue
		}
		appendProxySchemes(snap.ListenAddress, snap.Port, username, password)
	}

	// 返回纯文本，每行一个 URI
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	filename := "proxy_pool.txt"
	if scheme == "socks5" {
		filename = "proxy_pool_socks5.txt"
	} else if scheme == "all" {
		filename = "proxy_pool_all.txt"
	}
	if includeAll {
		filename = "full_" + filename
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	_, _ = w.Write([]byte(strings.Join(lines, "\n")))
}

// handleSettings handles GET/PUT for dynamic settings (external_ip, probe_target, skip_cert_verify, log).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// One snapshot prevents mixing fields from concurrent settings saves.
		s.cfgMu.RLock()
		extIP, probeTarget, skipCertVerify := s.cfg.ExternalIP, s.cfg.ProbeTarget, s.cfg.SkipCertVerify
		var logCfg config.LogConfig
		var cfg *config.Config
		if s.cfgSrc != nil {
			snapshot := s.cfgSrc.SettingsSnapshot()
			cfg = &snapshot
			logCfg = snapshot.Log
		}
		s.cfgMu.RUnlock()

		resp := map[string]any{
			"external_ip":      extIP,
			"probe_target":     probeTarget,
			"skip_cert_verify": skipCertVerify,
			"log": map[string]any{
				"output":      logCfg.Output,
				"file":        logCfg.File,
				"max_size":    logCfg.MaxSize,
				"max_backups": logCfg.MaxBackups,
				"max_age":     logCfg.MaxAge,
				"compress":    logCfg.Compress,
			},
			"geoip": map[string]any{
				"enabled":              false,
				"database_path":        "",
				"listen":               "",
				"port":                 0,
				"auto_update_enabled":  false,
				"auto_update_interval": "",
			},
		}
		if cfg != nil {
			resp["mode"] = cfg.Mode
			resp["listener"] = map[string]any{
				"address":  cfg.Listener.Address,
				"port":     cfg.Listener.Port,
				"username": cfg.Listener.Username,
				"password": cfg.Listener.Password,
			}
			resp["multi_port"] = map[string]any{
				"address":   cfg.MultiPort.Address,
				"base_port": cfg.MultiPort.BasePort,
				"username":  cfg.MultiPort.Username,
				"password":  cfg.MultiPort.Password,
			}
			resp["pool"] = map[string]any{
				"mode":                 cfg.Pool.Mode,
				"failure_threshold":    cfg.Pool.FailureThreshold,
				"blacklist_duration":   cfg.Pool.BlacklistDuration.String(),
				"transient_cooldown":   cfg.Pool.TransientCooldown.String(),
				"rate_limit_cooldown":  cfg.Pool.RateLimitCooldown.String(),
				"blacklist_failed_pin": cfg.Pool.BlacklistFailedPinOrDefault(),
			}
			resp["sticky"] = map[string]any{
				"enabled":         cfg.Sticky.Enabled,
				"port":            cfg.Sticky.Port,
				"session_enabled": cfg.Sticky.SessionRoutingEnabled(),
				"session_ttl":     cfg.Sticky.SessionTTLOrDefault().String(),
				"max_sessions":    cfg.Sticky.MaxSessionsOrDefault(),
			}
			resp["management"] = map[string]any{
				"listen":            cfg.Management.Listen,
				"password":          cfg.Management.Password,
				"probe_concurrency": cfg.ProbeConcurrencyOrDefault(),
			}
			resp["geoip"] = map[string]any{
				"enabled":              cfg.GeoIP.Enabled,
				"database_path":        cfg.GeoIP.DatabasePath,
				"listen":               cfg.GeoIP.Listen,
				"port":                 cfg.GeoIP.Port,
				"auto_update_enabled":  cfg.GeoIP.AutoUpdateEnabled,
				"auto_update_interval": cfg.GeoIP.AutoUpdateInterval.String(),
			}
		}
		writeJSON(w, resp)
	case http.MethodPut:
		var req struct {
			ExternalIP     *string `json:"external_ip"`
			ProbeTarget    *string `json:"probe_target"`
			SkipCertVerify *bool   `json:"skip_cert_verify"`
			Mode           string  `json:"mode,omitempty"`
			Listener       *struct {
				Address  string `json:"address"`
				Port     uint16 `json:"port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"listener,omitempty"`
			MultiPort *struct {
				Address  string `json:"address"`
				BasePort uint16 `json:"base_port"`
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"multi_port,omitempty"`
			Pool *struct {
				Mode               string `json:"mode"`
				FailureThreshold   int    `json:"failure_threshold"`
				BlacklistDuration  string `json:"blacklist_duration"`
				TransientCooldown  string `json:"transient_cooldown"`
				RateLimitCooldown  string `json:"rate_limit_cooldown"`
				BlacklistFailedPin *bool  `json:"blacklist_failed_pin"`
			} `json:"pool,omitempty"`
			Sticky *struct {
				Enabled        *bool   `json:"enabled"`
				Port           *uint16 `json:"port"`
				SessionEnabled *bool   `json:"session_enabled"`
				SessionTTL     *string `json:"session_ttl"`
				MaxSessions    *int    `json:"max_sessions"`
			} `json:"sticky,omitempty"`
			Management *struct {
				Listen           string `json:"listen"`
				Password         string `json:"password"`
				ProbeConcurrency int    `json:"probe_concurrency"`
			} `json:"management,omitempty"`
			Log *struct {
				Output     string `json:"output"`
				MaxSize    int    `json:"max_size"`
				MaxBackups int    `json:"max_backups"`
				MaxAge     int    `json:"max_age"`
				Compress   bool   `json:"compress"`
			} `json:"log"`
			GeoIP *struct {
				Enabled            bool   `json:"enabled"`
				DatabasePath       string `json:"database_path"`
				Listen             string `json:"listen"`
				Port               uint16 `json:"port"`
				AutoUpdateEnabled  bool   `json:"auto_update_enabled"`
				AutoUpdateInterval string `json:"auto_update_interval"`
			} `json:"geoip"`
		}
		if err := decodeManagementJSON(w, r, 1<<20, &req); err != nil {
			writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "请求格式错误"})
			return
		}

		var durationInputs []struct{ name, value string }
		if req.Pool != nil {
			durationInputs = append(durationInputs,
				struct{ name, value string }{"pool.blacklist_duration", req.Pool.BlacklistDuration},
				struct{ name, value string }{"pool.transient_cooldown", req.Pool.TransientCooldown},
				struct{ name, value string }{"pool.rate_limit_cooldown", req.Pool.RateLimitCooldown})
		}
		for _, input := range durationInputs {
			if input.value == "" {
				continue
			}
			if duration, err := time.ParseDuration(input.value); err != nil || duration <= 0 {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": input.name + " 须为正数时间间隔"})
				return
			}
		}
		if req.Management != nil && req.Management.ProbeConcurrency < 0 {
			writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "management.probe_concurrency 须为非负整数"})
			return
		}
		if req.Log != nil && (req.Log.MaxSize < 0 || req.Log.MaxBackups < 0 || req.Log.MaxAge < 0) {
			writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "日志轮转参数须为非负整数"})
			return
		}

		var geoUpdateInterval time.Duration
		if req.GeoIP != nil && req.GeoIP.AutoUpdateInterval != "" {
			var err error
			geoUpdateInterval, err = time.ParseDuration(req.GeoIP.AutoUpdateInterval)
			if err != nil || geoUpdateInterval < 0 || (geoUpdateInterval == 0 && req.GeoIP.AutoUpdateEnabled) {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "geoip.auto_update_interval 须为有效时间间隔，开启自动更新时须大于 0"})
				return
			}
			if geoUpdateInterval == 0 {
				geoUpdateInterval = 24 * time.Hour // Legacy disabled-feature GET/PUT round-trip.
			}
		}

		// Validate session settings before touching either the live config or disk.
		var sessionTTL time.Duration
		if req.Sticky != nil {
			if req.Sticky.SessionTTL != nil {
				var err error
				sessionTTL, err = time.ParseDuration(*req.Sticky.SessionTTL)
				if err != nil || sessionTTL <= 0 || sessionTTL > config.MaxSessionTTL {
					writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "sticky.session_ttl 必须为大于 0 且不超过 8760h 的时间间隔"})
					return
				}
			}
			if req.Sticky.MaxSessions != nil && (*req.Sticky.MaxSessions < 1 || *req.Sticky.MaxSessions > config.MaxSessionCapacity) {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("sticky.max_sessions 必须在 1 到 %d 之间", config.MaxSessionCapacity)})
				return
			}
		}

		// Stage all updates in a value copy. A failed save leaves runtime settings
		// untouched, and a successful response corresponds to exactly one save.
		if locker, ok := s.nodeManager().(settingsLocker); ok {
			unlock := locker.LockSettings()
			defer unlock()
		}
		s.cfgMu.Lock()
		if s.cfgSrc == nil {
			s.cfgMu.Unlock()
			writeStatusJSON(w, http.StatusInternalServerError, map[string]any{"error": "配置存储未初始化"})
			return
		}
		candidate := s.cfgSrc.SettingsSnapshot()
		if req.ExternalIP != nil {
			candidate.ExternalIP = strings.TrimSpace(*req.ExternalIP)
		}
		if req.ProbeTarget != nil {
			candidate.Management.ProbeTarget = strings.TrimSpace(*req.ProbeTarget)
		}
		if req.SkipCertVerify != nil {
			candidate.SkipCertVerify = *req.SkipCertVerify
		}
		if req.Mode != "" {
			candidate.Mode = req.Mode
		}
		if req.Listener != nil {
			candidate.Listener.Address = req.Listener.Address
			candidate.Listener.Port = req.Listener.Port
			candidate.Listener.Username = req.Listener.Username
			candidate.Listener.Password = req.Listener.Password
		}
		if req.MultiPort != nil {
			candidate.MultiPort.Address = req.MultiPort.Address
			candidate.MultiPort.BasePort = req.MultiPort.BasePort
			candidate.MultiPort.Username = req.MultiPort.Username
			candidate.MultiPort.Password = req.MultiPort.Password
		}
		if req.Pool != nil {
			candidate.Pool.Mode = req.Pool.Mode
			candidate.Pool.FailureThreshold = req.Pool.FailureThreshold
			if req.Pool.BlacklistFailedPin != nil {
				value := *req.Pool.BlacklistFailedPin
				candidate.Pool.BlacklistFailedPin = &value
			}
			if req.Pool.BlacklistDuration != "" {
				if d, err := time.ParseDuration(req.Pool.BlacklistDuration); err == nil {
					candidate.Pool.BlacklistDuration = d
				}
			}
			// Retain the existing API behavior for legacy cooldown fields.
			if d, err := time.ParseDuration(req.Pool.TransientCooldown); err == nil && d > 0 {
				candidate.Pool.TransientCooldown = d
			}
			if d, err := time.ParseDuration(req.Pool.RateLimitCooldown); err == nil && d > 0 {
				candidate.Pool.RateLimitCooldown = d
			}
		}
		if req.Sticky != nil {
			if req.Sticky.Enabled != nil {
				candidate.Sticky.Enabled = *req.Sticky.Enabled
			}
			if req.Sticky.Port != nil {
				candidate.Sticky.Port = *req.Sticky.Port
			}
			if req.Sticky.SessionEnabled != nil {
				candidate.Sticky.SessionEnabled = req.Sticky.SessionEnabled
			}
			if req.Sticky.SessionTTL != nil {
				candidate.Sticky.SessionTTL = sessionTTL
			}
			if req.Sticky.MaxSessions != nil {
				candidate.Sticky.MaxSessions = *req.Sticky.MaxSessions
			}
		}
		if req.Management != nil {
			candidate.Management.Listen = req.Management.Listen
			candidate.Management.Password = req.Management.Password
			if req.Management.ProbeConcurrency > 0 {
				candidate.Management.ProbeConcurrency = req.Management.ProbeConcurrency
			}
		}
		if req.Log != nil {
			candidate.Log.Output = req.Log.Output
			if req.Log.MaxSize > 0 {
				candidate.Log.MaxSize = req.Log.MaxSize
			}
			if req.Log.MaxBackups > 0 {
				candidate.Log.MaxBackups = req.Log.MaxBackups
			}
			if req.Log.MaxAge > 0 {
				candidate.Log.MaxAge = req.Log.MaxAge
			}
			candidate.Log.Compress = req.Log.Compress
		}
		if req.GeoIP != nil {
			candidate.GeoIP.Enabled = req.GeoIP.Enabled
			candidate.GeoIP.DatabasePath = req.GeoIP.DatabasePath
			candidate.GeoIP.Listen = req.GeoIP.Listen
			candidate.GeoIP.Port = req.GeoIP.Port
			candidate.GeoIP.AutoUpdateEnabled = req.GeoIP.AutoUpdateEnabled
			if geoUpdateInterval > 0 {
				candidate.GeoIP.AutoUpdateInterval = geoUpdateInterval
			} else if candidate.GeoIP.AutoUpdateInterval <= 0 {
				candidate.GeoIP.AutoUpdateInterval = 24 * time.Hour
			}
			if candidate.GeoIP.Enabled && candidate.GeoIP.DatabasePath == "" {
				candidate.GeoIP.DatabasePath = "./GeoLite2-Country.mmdb"
			}
		}
		if err := candidate.ValidateManagementSettings(); err != nil {
			s.cfgMu.Unlock()
			writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if err := candidate.SaveManagementSettings(); err != nil {
			s.cfgMu.Unlock()
			writeStatusJSON(w, http.StatusInternalServerError, map[string]any{"error": fmt.Sprintf("保存配置失败: %v", err)})
			return
		}
		// Publish only settings-owned fields. Node and subscription managers own
		// the remaining fields and may have updated them during the file save.
		authChanged := s.setPasswordLocked(candidate.Management.Password)
		s.cfgSrc.ExternalIP = candidate.ExternalIP
		s.cfgSrc.Management = candidate.Management
		s.cfgSrc.SkipCertVerify = candidate.SkipCertVerify
		s.cfgSrc.Log = candidate.Log
		s.cfgSrc.GeoIP = candidate.GeoIP
		s.cfgSrc.Mode = candidate.Mode
		s.cfgSrc.Listener = candidate.Listener
		s.cfgSrc.MultiPort = candidate.MultiPort
		s.cfgSrc.Pool = candidate.Pool
		s.cfgSrc.Sticky = candidate.Sticky
		s.cfg.ExternalIP = candidate.ExternalIP
		s.cfg.ProbeTarget = candidate.Management.ProbeTarget
		s.cfg.SkipCertVerify = candidate.SkipCertVerify
		if s.routingStore != nil {
			s.routingStore.Configure(candidate.Sticky.MaxSessionsOrDefault(), candidate.Sticky.SessionTTLOrDefault())
		}
		s.cfgMu.Unlock()

		writeJSON(w, map[string]any{
			"message":          "设置已保存",
			"external_ip":      candidate.ExternalIP,
			"probe_target":     candidate.Management.ProbeTarget,
			"skip_cert_verify": candidate.SkipCertVerify,
			"need_reload":      true,
			"auth_changed":     authChanged,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleSubscriptionStatus returns the current subscription refresh status.
func (s *Server) handleSubscriptionStatus(w http.ResponseWriter, r *http.Request) {
	refresher := s.subscriptionRefresher()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if refresher == nil {
		writeJSON(w, map[string]any{
			"enabled": false,
			"message": "订阅刷新未启用",
		})
		return
	}

	status := refresher.Status()
	writeJSON(w, map[string]any{
		"enabled":        true,
		"last_refresh":   status.LastRefresh,
		"next_refresh":   status.NextRefresh,
		"node_count":     status.NodeCount,
		"last_error":     status.LastError,
		"refresh_count":  status.RefreshCount,
		"is_refreshing":  status.IsRefreshing,
		"nodes_modified": status.NodesModified,
	})
}

func contextualRefresher(refresher SubscriptionRefresher) (contextualSubscriptionRefresher, bool) {
	contextual, ok := refresher.(contextualSubscriptionRefresher)
	return contextual, ok
}

// handleSubscriptionRefresh triggers an immediate subscription refresh.
func (s *Server) handleSubscriptionRefresh(w http.ResponseWriter, r *http.Request) {
	refresher := s.subscriptionRefresher()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if refresher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "订阅刷新未启用"})
		return
	}

	var refreshErr error
	if contextual, ok := contextualRefresher(refresher); ok {
		refreshErr = contextual.RefreshNowContext(r.Context())
	} else {
		refreshErr = refresher.RefreshNow()
	}
	if err := refreshErr; err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}

	status := refresher.Status()
	writeJSON(w, map[string]any{
		"message":    "刷新成功",
		"node_count": status.NodeCount,
	})
}

// handleSubscriptionConfig handles GET/PUT for subscription configuration.
func (s *Server) handleSubscriptionConfig(w http.ResponseWriter, r *http.Request) {
	refresher := s.subscriptionRefresher()
	switch r.Method {
	case http.MethodGet:
		s.cfgMu.RLock()
		var urls []string
		var enabled bool
		var interval string
		if s.cfgSrc != nil {
			urls = append([]string(nil), s.cfgSrc.Subscriptions...)
			enabled = s.cfgSrc.SubscriptionRefresh.Enabled
			interval = s.cfgSrc.SubscriptionRefresh.Interval.String()
		}
		s.cfgMu.RUnlock()
		writeJSON(w, map[string]any{
			"subscriptions": urls,
			"enabled":       enabled,
			"interval":      interval,
		})

	case http.MethodPut:
		var req struct {
			Subscriptions []string `json:"subscriptions"`
			Enabled       bool     `json:"enabled"`
			Interval      string   `json:"interval"` // e.g. "1h", "30m"
		}
		if err := decodeManagementJSON(w, r, 1<<20, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}

		interval := time.Hour
		if req.Interval != "" {
			var err error
			interval, err = time.ParseDuration(req.Interval)
			if err != nil || interval < 5*time.Minute {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "interval 须为不少于 5m 的时间间隔"})
				return
			}
		}
		var cleanURLs []string
		seenURLs := make(map[string]bool)
		for _, raw := range req.Subscriptions {
			u := strings.TrimSpace(raw)
			if u == "" {
				continue
			}
			parsed, err := url.Parse(u)
			if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
				writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "订阅地址须为有效的 HTTP 或 HTTPS URL"})
				return
			}
			if !seenURLs[u] {
				cleanURLs = append(cleanURLs, u)
				seenURLs[u] = true
			}
		}

		// Stage and save only subscription-owned fields. Save failures must not
		// alter runtime state or overwrite concurrent settings/node changes.
		unlock := func() {}
		if locker, ok := s.nodeManager().(settingsLocker); ok {
			unlock = locker.LockSettings()
		}
		s.cfgMu.Lock()
		if s.cfgSrc == nil {
			s.cfgMu.Unlock()
			unlock()
			writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "配置存储未初始化"})
			return
		}
		candidate := s.cfgSrc.SettingsSnapshot()
		candidate.Subscriptions = cleanURLs
		candidate.SubscriptionRefresh = s.cfgSrc.SubscriptionRefresh
		candidate.SubscriptionRefresh.Enabled = req.Enabled
		candidate.SubscriptionRefresh.Interval = interval
		if err := candidate.SaveSubscriptionSettings(); err != nil {
			s.cfgMu.Unlock()
			unlock()
			writeStatusJSON(w, http.StatusInternalServerError, map[string]any{"error": fmt.Sprintf("保存配置失败: %v", err)})
			return
		}
		s.cfgSrc.Subscriptions = append([]string(nil), candidate.Subscriptions...)
		s.cfgSrc.SubscriptionRefresh = candidate.SubscriptionRefresh
		s.cfgMu.Unlock()
		unlock() // Refresh may invoke reload, which requires the same manager lock.

		// Hot-reload subscription manager and wait for refresh to complete
		if refresher != nil {
			var refreshErr error
			if contextual, ok := contextualRefresher(refresher); ok {
				refreshErr = contextual.UpdateConfigAndRefreshContext(r.Context(), cleanURLs, req.Enabled, interval)
			} else {
				refreshErr = refresher.UpdateConfigAndRefresh(cleanURLs, req.Enabled, interval)
			}
			if err := refreshErr; err != nil {
				// Config was saved but refresh failed — report partial success
				writeJSON(w, map[string]any{
					"message":       fmt.Sprintf("订阅配置已保存，但刷新失败: %v", err),
					"subscriptions": cleanURLs,
					"enabled":       req.Enabled,
					"interval":      interval.String(),
					"refresh_error": err.Error(),
				})
				return
			}
		}

		status := SubscriptionStatus{}
		if refresher != nil {
			status = refresher.Status()
		}
		writeJSON(w, map[string]any{
			"message":       "订阅配置已更新并生效",
			"subscriptions": cleanURLs,
			"enabled":       req.Enabled,
			"interval":      interval.String(),
			"node_count":    status.NodeCount,
		})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// nodePayload is the JSON request body for node CRUD operations.
type nodePayload struct {
	Name     string `json:"name"`
	URI      string `json:"uri"`
	Port     uint16 `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

func (p nodePayload) toConfig() config.NodeConfig {
	return config.NodeConfig{
		Name:     p.Name,
		URI:      p.URI,
		Port:     p.Port,
		Username: p.Username,
		Password: p.Password,
	}
}

// handleConfigNodes handles GET (list) and POST (create) for config nodes.
func (s *Server) handleConfigNodes(w http.ResponseWriter, r *http.Request) {
	nodeMgr := s.nodeManager()
	if !ensureNodeManager(w, nodeMgr) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		nodes, err := nodeMgr.ListConfigNodes(r.Context())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"nodes": nodes})
	case http.MethodPost:
		var payload nodePayload
		if err := decodeManagementJSON(w, r, 1<<20, &payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := nodeMgr.CreateNode(r.Context(), payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"node": node, "message": "节点已添加，请点击重载使配置生效"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleConfigNodeItem handles PUT (update) and DELETE for a specific config node.
func (s *Server) handleConfigNodeItem(w http.ResponseWriter, r *http.Request) {
	nodeMgr := s.nodeManager()
	if !ensureNodeManager(w, nodeMgr) {
		return
	}

	namePart := strings.TrimPrefix(r.URL.EscapedPath(), "/api/nodes/config/")
	nodeName, err := url.PathUnescape(namePart)
	if err != nil || nodeName == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, map[string]any{"error": "节点名称无效"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		var payload nodePayload
		if err := decodeManagementJSON(w, r, 1<<20, &payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, map[string]any{"error": "请求格式错误"})
			return
		}
		node, err := nodeMgr.UpdateNode(r.Context(), nodeName, payload.toConfig())
		if err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"node": node, "message": "节点已更新，请点击重载使配置生效"})
	case http.MethodDelete:
		if err := nodeMgr.DeleteNode(r.Context(), nodeName); err != nil {
			s.respondNodeError(w, err)
			return
		}
		writeJSON(w, map[string]any{"message": "节点已删除，请点击重载使配置生效"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleReload triggers a configuration reload.
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodeMgr := s.nodeManager()
	if !ensureNodeManager(w, nodeMgr) {
		return
	}

	if err := nodeMgr.TriggerReload(r.Context()); err != nil {
		s.respondNodeError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"message": "重载成功，现有连接已被中断",
	})
}

func ensureNodeManager(w http.ResponseWriter, nodeMgr NodeManager) bool {
	if nodeMgr == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]any{"error": "节点管理未启用"})
		return false
	}
	return true
}

func (s *Server) respondNodeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNodeNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrNodeConflict), errors.Is(err, ErrInvalidNode):
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	writeJSON(w, map[string]any{"error": err.Error()})
}

// handleTraffic streams real-time traffic from sing-box Clash API as SSE.
// Clash API /traffic returns newline-delimited JSON; we convert to SSE for browser EventSource.
func (s *Server) handleTraffic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_, ok := w.(http.Flusher)
	if !ok {
		writeStatusJSON(w, http.StatusInternalServerError, map[string]any{"error": "SSE not supported"})
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://127.0.0.1:9092/traffic", nil)
	if err != nil {
		writeStatusJSON(w, http.StatusBadGateway, map[string]any{"error": "流量统计请求创建失败"})
		return
	}
	resp, err := s.trafficClient.Do(request)
	if err != nil {
		if r.Context().Err() == nil {
			writeStatusJSON(w, http.StatusBadGateway, map[string]any{"error": "流量统计接口未就绪", "details": err.Error()})
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeStatusJSON(w, http.StatusBadGateway, map[string]any{"error": "流量统计接口未就绪", "upstream_status": resp.StatusCode})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")
	// Scanner preserves a complete JSON record across arbitrary network chunks.
	// A bounded record size also prevents an invalid upstream from growing memory.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !json.Valid([]byte(line)) {
			continue
		}
		if err := writeSSE(w, []byte(line)); err != nil {
			return
		}
	}
}

// handleLogs returns recent console log content from the in-memory ring buffer.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	content := SharedLogBuffer.Content()
	writeJSON(w, map[string]any{"logs": content})
}

// Session management functions

// generateSessionToken creates a cryptographically secure random token.
func (s *Server) generateSessionToken() (string, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("failed to generate session token: %w", err)
	}
	return hex.EncodeToString(tokenBytes), nil
}

// createSession creates a new session with expiration.
func (s *Server) createSession() (*Session, error) {
	token, err := s.generateSessionToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	session := &Session{
		Token:     token,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionTTL),
	}

	s.sessionMu.Lock()
	const maxLoginSessions = 1024
	oldestToken := ""
	var oldestTime time.Time
	for token, existing := range s.sessions {
		if !now.Before(existing.ExpiresAt) {
			delete(s.sessions, token)
			continue
		}
		if oldestToken == "" || existing.CreatedAt.Before(oldestTime) {
			oldestToken, oldestTime = token, existing.CreatedAt
		}
	}
	if len(s.sessions) >= maxLoginSessions {
		delete(s.sessions, oldestToken)
	}
	s.sessions[token] = session
	s.sessionMu.Unlock()

	return session, nil
}

// validateSession checks if a session token is valid and not expired.
func (s *Server) validateSession(token string) bool {
	s.sessionMu.RLock()
	session, exists := s.sessions[token]
	s.sessionMu.RUnlock()

	if !exists {
		return false
	}

	// Check if expired
	if !time.Now().Before(session.ExpiresAt) {
		s.sessionMu.Lock()
		delete(s.sessions, token)
		s.sessionMu.Unlock()
		return false
	}

	return true
}

// cleanupExpiredSessions periodically removes expired sessions.
func (s *Server) cleanupExpiredSessions() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		s.sessionMu.Lock()
		for token, session := range s.sessions {
			if now.After(session.ExpiresAt) {
				delete(s.sessions, token)
			}
		}
		s.sessionMu.Unlock()
	}
}

// secureCompareStrings performs constant-time string comparison to prevent timing attacks.
func secureCompareStrings(a, b string) bool {
	aHash, bHash := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}
