package config

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Config describes the high level settings for the proxy pool server.
type Config struct {
	Mode                string                    `yaml:"mode"`
	Listener            ListenerConfig            `yaml:"listener"`
	MultiPort           MultiPortConfig           `yaml:"multi_port"`
	Pool                PoolConfig                `yaml:"pool"`
	Sticky              StickyConfig              `yaml:"sticky"`
	Management          ManagementConfig          `yaml:"management"`
	SubscriptionRefresh SubscriptionRefreshConfig `yaml:"subscription_refresh"`
	GeoIP               GeoIPConfig               `yaml:"geoip"`
	Log                 LogConfig                 `yaml:"log"`
	Nodes               []NodeConfig              `yaml:"nodes"`
	NodesFile           string                    `yaml:"nodes_file"`    // 节点文件路径，每行一个 URI
	Subscriptions       []string                  `yaml:"subscriptions"` // 订阅链接列表
	ExternalIP          string                    `yaml:"external_ip"`   // 外部 IP 地址，用于导出时替换 0.0.0.0
	LogLevel            string                    `yaml:"log_level"`
	SkipCertVerify      bool                      `yaml:"skip_cert_verify"` // 全局跳过 SSL 证书验证

	filePath string `yaml:"-"` // 配置文件路径，用于保存
}

// LogConfig controls log output and rotation.
type LogConfig struct {
	Output     string `yaml:"output"`      // 日志输出: "stdout", "file", 默认 "stdout"
	File       string `yaml:"file"`        // 日志文件路径，默认 "logs/easy_proxies.log"
	MaxSize    int    `yaml:"max_size"`    // 单个日志文件最大 MB，默认 50
	MaxBackups int    `yaml:"max_backups"` // 保留旧日志文件个数，默认 3
	MaxAge     int    `yaml:"max_age"`     // 保留旧日志文件天数，默认 7
	Compress   bool   `yaml:"compress"`    // 是否压缩旧日志，默认 false
}

// GeoIPConfig controls GeoIP-based region routing.
type GeoIPConfig struct {
	Enabled            bool          `yaml:"enabled"`              // 是否启用 GeoIP 地域分区
	DatabasePath       string        `yaml:"database_path"`        // GeoLite2-Country.mmdb 文件路径
	Listen             string        `yaml:"listen"`               // GeoIP 路由监听地址，默认使用 listener 配置
	Port               uint16        `yaml:"port"`                 // GeoIP 路由监听端口，默认 1221
	AutoUpdateEnabled  bool          `yaml:"auto_update_enabled"`  // 是否启用自动更新数据库
	AutoUpdateInterval time.Duration `yaml:"auto_update_interval"` // 自动更新间隔，默认 24 小时
}

// ListenerConfig defines how the HTTP/SOCKS5 mixed proxy should listen for clients.
type ListenerConfig struct {
	Address  string `yaml:"address"`
	Port     uint16 `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// StickyConfig controls authenticated username.session routing on the main
// pool listener and the optional legacy source-IP sticky entry. The legacy
// port reuses the listener's address and credentials.
type StickyConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    uint16 `yaml:"port"`
	// SessionEnabled enables authenticated username.session routing on the
	// regular pool listener. Unset defaults to true; Enabled/Port above retain
	// the legacy source-IP entry point for backward compatibility.
	SessionEnabled *bool         `yaml:"session_enabled,omitempty"`
	SessionTTL     time.Duration `yaml:"session_ttl,omitempty"`
	MaxSessions    int           `yaml:"max_sessions,omitempty"`
}

const (
	DefaultSessionTTL  = 24 * time.Hour
	DefaultMaxSessions = 10000
	MaxSessionCapacity = 100000
	MaxSessionTTL      = 365 * 24 * time.Hour
)

func (s StickyConfig) SessionRoutingEnabled() bool {
	return s.SessionEnabled == nil || *s.SessionEnabled
}

func (s StickyConfig) SessionTTLOrDefault() time.Duration {
	if s.SessionTTL <= 0 {
		return DefaultSessionTTL
	}
	return s.SessionTTL
}

func (s StickyConfig) MaxSessionsOrDefault() int {
	if s.MaxSessions <= 0 {
		return DefaultMaxSessions
	}
	return s.MaxSessions
}

// PoolConfig configures scheduling + failure handling.
//
// Failures are split three ways: rate limiting (HTTP 429) uses RateLimitCooldown,
// other transient faults (timeout / connection reset / 503) use TransientCooldown,
// and permanent faults (handshake, cert, 404, …) still accumulate toward
// FailureThreshold and then trigger the full BlacklistDuration.
type PoolConfig struct {
	Mode              string        `yaml:"mode"`
	FailureThreshold  int           `yaml:"failure_threshold"`
	BlacklistDuration time.Duration `yaml:"blacklist_duration"`
	// TransientCooldown is how long a node is skipped after a transient failure
	// (timeout, connection reset, 503). These errors usually clear on their own,
	// so the node is briefly parked instead of counted toward the permanent
	// blacklist. Default 60s when <= 0.
	TransientCooldown time.Duration `yaml:"transient_cooldown,omitempty"`
	// RateLimitCooldown is the cooldown applied when the upstream reports rate
	// limiting (HTTP 429). Quota resets are typically far slower than a network
	// blip, so this can be set much higher (e.g. 2h) than TransientCooldown.
	// Falls back to the effective TransientCooldown when <= 0.
	RateLimitCooldown time.Duration `yaml:"rate_limit_cooldown,omitempty"`
	// RetryEnabled toggles automatic fail-over to another member when a dial fails.
	// nil/unset → default true. Use *bool so users can explicitly disable via YAML.
	RetryEnabled *bool `yaml:"retry_enabled,omitempty"`
	// BlacklistFailedPin blacklists a node immediately when a named session's
	// pinned dial fails on a permanent fault, instead of waiting for
	// FailureThreshold. nil/unset → default true.
	BlacklistFailedPin *bool `yaml:"blacklist_failed_pin,omitempty"`
	// RetryAttempts is the maximum total dial attempts per request (including the first).
	// Default 3. Values <= 0 are normalized to 3.
	// For pools with multiple members, each retry picks a different member when possible.
	// For single-member pools (e.g. per-node multi-port pools), retries dial the same member.
	RetryAttempts int `yaml:"retry_attempts,omitempty"`
}

// RetryEnabledOrDefault reports whether retry is enabled (default true).
func (p PoolConfig) RetryEnabledOrDefault() bool {
	if p.RetryEnabled == nil {
		return true
	}
	return *p.RetryEnabled
}

// BlacklistFailedPinOrDefault reports whether a failed session pin blacklists
// its node immediately (default true).
func (p PoolConfig) BlacklistFailedPinOrDefault() bool {
	if p.BlacklistFailedPin == nil {
		return true
	}
	return *p.BlacklistFailedPin
}

// MultiPortConfig defines address/credential defaults for multi-port mode.
type MultiPortConfig struct {
	Address  string `yaml:"address"`
	BasePort uint16 `yaml:"base_port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// ManagementConfig controls the monitoring HTTP endpoint.
type ManagementConfig struct {
	Enabled          *bool  `yaml:"enabled"`
	Listen           string `yaml:"listen"`
	ProbeTarget      string `yaml:"probe_target"`
	Password         string `yaml:"password"`          // WebUI 访问密码，为空则不需要密码
	ProbeConcurrency int    `yaml:"probe_concurrency"` // 并发探测线程数（8-1024，默认 32），大规模节点可调高以加快探测
}

// SubscriptionRefreshConfig controls subscription auto-refresh and reload settings.
type SubscriptionRefreshConfig struct {
	Enabled            bool          `yaml:"enabled"`              // 是否启用定时刷新
	Interval           time.Duration `yaml:"interval"`             // 刷新间隔，默认 1 小时
	Timeout            time.Duration `yaml:"timeout"`              // 获取订阅的超时时间
	HealthCheckTimeout time.Duration `yaml:"health_check_timeout"` // 新节点健康检查超时
	DrainTimeout       time.Duration `yaml:"drain_timeout"`        // 旧实例排空超时时间
	MinAvailableNodes  int           `yaml:"min_available_nodes"`  // 最少可用节点数，低于此值不切换
	FetchConcurrency   int           `yaml:"fetch_concurrency"`    // 订阅抓取并发数，默认 16，最大 32
}

// NodeSource indicates where a node configuration originated from.
type NodeSource string

const (
	NodeSourceInline       NodeSource = "inline"       // Defined directly in config.yaml nodes array
	NodeSourceFile         NodeSource = "nodes_file"   // Loaded from external nodes file
	NodeSourceSubscription NodeSource = "subscription" // Fetched from subscription URL
)

const (
	defaultSubscriptionFetchConcurrency = 16
	maxSubscriptionFetchConcurrency     = 32
	maxSubscriptionBodySize             = 10 * 1024 * 1024
)

// SubscriptionFetchStats describes a subscription loading attempt.
type SubscriptionFetchStats struct {
	RequestedURLs int
	UniqueURLs    int
	Successful    int
	Failed        int
	Empty         int
	Nodes         int
	DedupedURLs   int
	DedupedNodes  int
	LastError     error
}

// SubscriptionFetchOptions controls concurrent subscription loading.
type SubscriptionFetchOptions struct {
	Timeout     time.Duration
	Concurrency int
	Client      *http.Client
	Loggerf     func(format string, args ...any)
}

// NodeConfig describes a single upstream proxy endpoint expressed as URI.
type NodeConfig struct {
	Name     string     `yaml:"name" json:"name"`
	URI      string     `yaml:"uri" json:"uri"`
	Port     uint16     `yaml:"port,omitempty" json:"port,omitempty"`
	Username string     `yaml:"username,omitempty" json:"username,omitempty"`
	Password string     `yaml:"password,omitempty" json:"password,omitempty"`
	Source   NodeSource `yaml:"-" json:"source,omitempty"` // Runtime only, not persisted
}

// NodeKey returns a stable identifier for the node, used to preserve port
// assignments across subscription refreshes and reloads.
//
// The identity deliberately ignores the parts of a proxy URI that subscription
// providers commonly mutate without changing the underlying server: the
// display name (#fragment) and query-parameter ordering. As long as the
// scheme, credentials, host, port and parameter set are unchanged, a node
// keeps the same key — and therefore the same proxy port.
func (n *NodeConfig) NodeKey() string {
	return stableNodeKey(n.URI)
}

// stableNodeKey derives a port-stable identity from a proxy URI by stripping the
// volatile display name and canonicalizing query order. It never errors: on any
// parse failure it falls back to the raw URI minus its fragment, so the result
// is always at least as stable as the previous full-URI behavior.
func stableNodeKey(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return ""
	}

	// VMess accepts both base64 JSON and URL-format links. Only the encoded
	// payload needs special treatment; URL links are canonicalized below.
	if strings.HasPrefix(uri, "vmess://") && !strings.Contains(uri, "@") {
		if idx := strings.Index(uri, "#"); idx != -1 {
			uri = strings.TrimSpace(uri[:idx])
		}
		payload := strings.TrimPrefix(uri, "vmess://")
		for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
			decoded, err := encoding.DecodeString(payload)
			if err != nil {
				continue
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(decoded, &fields); err != nil || len(fields) == 0 {
				continue
			}
			// VMess's display name lives inside its encoded JSON, not just in a
			// URL fragment. It must not change a session's upstream identity.
			delete(fields, "ps")
			canonical, err := json.Marshal(fields)
			if err == nil {
				return "vmess://" + base64.RawURLEncoding.EncodeToString(canonical)
			}
		}
		return uri
	}

	u, err := url.Parse(uri)
	if err != nil {
		if idx := strings.LastIndex(uri, "#"); idx != -1 {
			return strings.TrimSpace(uri[:idx])
		}
		return uri
	}

	u.Fragment = "" // display name — volatile, not part of node identity
	if u.RawQuery != "" {
		// Encode() sorts keys, so reordered parameters yield the same key.
		u.RawQuery = u.Query().Encode()
	}
	return u.String()
}

// Load reads YAML config from disk and applies defaults/validation.
func Load(path string) (*Config, error) {
	data, err := readFileWithLock(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	cfg.filePath = path
	// Track YAML-owned ports before normalize adds provisional assignments and
	// appends file/subscription nodes. Explicit inline ports are user intent.
	explicitPorts := make([]bool, len(cfg.Nodes))
	for idx := range cfg.Nodes {
		explicitPorts[idx] = cfg.Nodes[idx].Port > 0
	}

	// Resolve nodes_file path relative to config file directory
	if cfg.NodesFile != "" && !filepath.IsAbs(cfg.NodesFile) {
		configDir := filepath.Dir(path)
		cfg.NodesFile = filepath.Join(configDir, cfg.NodesFile)
	}

	if err := cfg.normalize(); err != nil {
		return nil, err
	}

	// Restore persisted proxy ports so a restart keeps the same port per node.
	if err := cfg.applyPersistedPorts(explicitPorts); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// ExtractNodeName extracts a human-readable name from a proxy URI.
// For standard URIs (vless://, ss://, trojan://), it extracts from the URL fragment (#name).
// For vmess:// URIs, it base64-decodes the payload and extracts the "ps" field.
func ExtractNodeName(uri string) string {
	uri = strings.TrimSpace(uri)

	// Handle vmess:// specially - it's base64-encoded JSON, not a standard URL
	if strings.HasPrefix(uri, "vmess://") && !strings.Contains(uri, "@") {
		payload := strings.TrimPrefix(uri, "vmess://")
		// Remove any fragment that might be appended
		if idx := strings.Index(payload, "#"); idx != -1 {
			payload = payload[:idx]
		}
		payload = strings.TrimSpace(payload)
		// Try standard base64 first, then raw/URL-safe variants
		var decoded []byte
		var err error
		decoded, err = decodeProxyBase64(payload)
		if err == nil {
			var vmess struct {
				PS string `json:"ps"`
			}
			if json.Unmarshal(decoded, &vmess) == nil && vmess.PS != "" {
				return strings.TrimSpace(vmess.PS)
			}
		}
		return ""
	}

	// For standard URIs, extract from URL fragment (#name)
	if idx := strings.LastIndex(uri, "#"); idx != -1 && idx < len(uri)-1 {
		fragment := uri[idx+1:]
		if decoded, err := url.PathUnescape(fragment); err == nil && decoded != "" {
			return strings.TrimSpace(decoded)
		}
		return strings.TrimSpace(fragment)
	}

	return ""
}

func (c *Config) normalize() error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	// Normalize mode name: support both multi-port and multi_port
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("unsupported mode %q (use 'pool', 'multi-port', or 'hybrid')", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if mode, err := normalizePoolMode(c.Pool.Mode); err != nil {
		return err
	} else {
		c.Pool.Mode = mode
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
	}
	if c.Pool.TransientCooldown <= 0 {
		c.Pool.TransientCooldown = 60 * time.Second
	}
	if c.Pool.RateLimitCooldown <= 0 {
		c.Pool.RateLimitCooldown = c.Pool.TransientCooldown
	}
	if c.Pool.RetryAttempts <= 0 {
		c.Pool.RetryAttempts = 3
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "127.0.0.1:9091"
	}
	if c.Management.ProbeTarget == "" {
		c.Management.ProbeTarget = "www.apple.com:80"
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}

	// Subscription refresh defaults
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 60 * time.Second
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = normalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)

	// Mark inline nodes with source
	for idx := range c.Nodes {
		c.Nodes[idx].Source = NodeSourceInline
	}

	// Load nodes from file if specified (but NOT if subscriptions exist - subscription takes priority)
	if c.NodesFile != "" && len(c.Subscriptions) == 0 {
		fileNodes, err := loadNodesFromFile(c.NodesFile)
		if err != nil {
			return fmt.Errorf("load nodes from file %q: %w", c.NodesFile, err)
		}
		for idx := range fileNodes {
			fileNodes[idx].Source = NodeSourceFile
		}
		c.Nodes = append(c.Nodes, fileNodes...)
	}

	// Load nodes from subscriptions (highest priority - writes to nodes.txt)
	if len(c.Subscriptions) > 0 {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
			c.NodesFile = nodesFilePath
		}
		cachedNodes, cacheErr := loadNodesFromFile(nodesFilePath)

		subNodes, stats := FetchSubscriptionNodes(context.Background(), c.Subscriptions, SubscriptionFetchOptions{
			Timeout:     c.SubscriptionRefresh.Timeout,
			Concurrency: c.SubscriptionRefresh.FetchConcurrency,
			Loggerf:     log.Printf,
		})
		if stats.Failed > 0 {
			log.Printf("⚠️  Subscription initialization skipped %d/%d unique URLs; last error: %v", stats.Failed, stats.UniqueURLs, stats.LastError)
		}
		log.Printf("✅ Subscription initialization fetched %d nodes from %d/%d unique URLs in parallel (deduped_urls=%d, deduped_nodes=%d)",
			stats.Nodes, stats.Successful, stats.UniqueURLs, stats.DedupedURLs, stats.DedupedNodes)
		// Mark subscription nodes and write to nodes.txt
		for idx := range subNodes {
			subNodes[idx].Source = NodeSourceSubscription
		}
		useCached, reason := shouldUseCachedSubscriptionNodes(subNodes, cachedNodes, cacheErr, stats)
		if useCached {
			log.Printf("⚠️  Keeping cached subscription nodes from %s (%d cached vs %d fetched): %s",
				nodesFilePath, len(cachedNodes), len(subNodes), reason)
			for idx := range cachedNodes {
				cachedNodes[idx].Source = NodeSourceSubscription
			}
			subNodes = cachedNodes
		} else if len(subNodes) > 0 {
			// Write subscription nodes to nodes.txt
			if err := writeNodesToFile(nodesFilePath, subNodes); err != nil {
				log.Printf("⚠️ Failed to write nodes to %q: %v", nodesFilePath, err)
			} else {
				log.Printf("✅ Written %d subscription nodes to %s", len(subNodes), nodesFilePath)
			}
		}
		c.Nodes = append(c.Nodes, subNodes...)
	}

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty (configure nodes in config or use nodes_file)")
	}
	// portCursor is an int (not uint16) so the >65535 exhaustion guard fires
	// instead of wrapping to 0 and assigning unbindable low ports.
	portCursor := int(c.MultiPort.BasePort)
	reservedPorts := c.reservedListenerPorts()
	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)

		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		// Fallback to default name if still empty
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

		// Provisional port assignment only. The real, bind-checked, collision-safe
		// assignment is done once by applyPersistedPorts → NormalizeWithPortMap
		// right after normalize() returns. Probing IsPortAvailable per node here
		// would double the socket open/close work at startup (16k+ syscalls for
		// 8k nodes) for a result that is immediately discarded.
		if c.Nodes[idx].Port == 0 && (c.Mode == "multi-port" || c.Mode == "hybrid") {
			for portCursor <= 65535 && reservedPorts[uint16(portCursor)] {
				portCursor++
			}
			// A narrow provisional range may be exhausted even when every node
			// has a valid saved assignment elsewhere. Defer that decision until
			// applyPersistedPorts has restored the sidecar instead of failing early.
			if portCursor <= 65535 {
				c.Nodes[idx].Port = uint16(portCursor)
				portCursor++
			}
		} else if c.Nodes[idx].Port == 0 && portCursor <= 65535 {
			c.Nodes[idx].Port = uint16(portCursor)
			portCursor++
		}

		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			if c.Nodes[idx].Username == "" {
				c.Nodes[idx].Username = c.MultiPort.Username
				c.Nodes[idx].Password = c.MultiPort.Password
			}
		}
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	// Log config defaults
	c.normalizeLogConfig()

	// Auto-fix port conflicts in hybrid mode (pool port vs multi-port)
	if c.Mode == "hybrid" {
		poolPort := c.Listener.Port
		usedPorts := c.reservedListenerPorts()
		for idx := range c.Nodes {
			usedPorts[c.Nodes[idx].Port] = true
		}
		for idx := range c.Nodes {
			if c.Nodes[idx].Port == poolPort {
				// Find next available port
				newPort := int(c.Nodes[idx].Port) + 1
				for newPort <= 65535 && (usedPorts[uint16(newPort)] || !IsPortAvailable(c.MultiPort.Address, uint16(newPort))) {
					newPort++
					if newPort > 65535 {
						return fmt.Errorf("no available port for node %q after conflict with pool port %d", c.Nodes[idx].Name, poolPort)
					}
				}
				if newPort > 65535 {
					return fmt.Errorf("no available port after pool port %d", poolPort)
				}
				log.Printf("⚠️  Node %q port %d conflicts with pool port, reassigned to %d", c.Nodes[idx].Name, poolPort, newPort)
				usedPorts[uint16(newPort)] = true
				c.Nodes[idx].Port = uint16(newPort)
			}
		}
	}

	if err := c.normalizeSticky(); err != nil {
		return err
	}

	return nil
}

// BuildPortMap creates a mapping from node URI to port for existing nodes.
// This is used to preserve port assignments when reloading configuration.
func (c *Config) BuildPortMap() map[string]uint16 {
	portMap := make(map[string]uint16)
	for _, node := range c.Nodes {
		if node.Port > 0 {
			portMap[node.NodeKey()] = node.Port
		}
	}
	return portMap
}

// nodePortMapFile is the sidecar file storing node→port assignments so they
// survive a process restart.
const nodePortMapFile = "node_ports.json"

// portMapPath returns the path of the port-map sidecar, located next to the
// main config file. It is empty when the config path is unknown.
func (c *Config) portMapPath() string {
	if c.filePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(c.filePath), nodePortMapFile)
}

// loadNodePortMap reads a previously saved stableNodeKey→port mapping. It
// returns nil on any error (missing file, unreadable, bad JSON); callers treat
// that as "no persisted ports", so a corrupt sidecar never blocks startup.
func loadNodePortMap(path string) map[string]uint16 {
	if path == "" {
		return nil
	}
	data, err := readFileWithLock(path)
	if err != nil {
		return nil
	}
	var m map[string]uint16
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}

// bridgeLegacyPortKeys upgrades a port-map sidecar written by an older version
// (keyed by raw full URIs or an earlier canonicalization) to the current
// stable-key scheme. Normalizing stored keys also preserves renamed encoded
// VMess nodes when upgrading from versions whose identity included `ps`.
// This makes a one-time format upgrade
// transparent: ports are preserved instead of all being reassigned on the first
// boot after upgrading. It mutates saved in place and is a no-op once the
// sidecar has been rewritten with stable keys.
func bridgeLegacyPortKeys(nodes []NodeConfig, saved map[string]uint16) {
	// Prefer the exact retained URI before considering aliases for removed or
	// renamed duplicate entries in an older sidecar.
	for i := range nodes {
		stableKey := nodes[i].NodeKey()
		if saved[stableKey] > 0 {
			continue
		}
		legacyKey := strings.TrimSpace(nodes[i].URI)
		keys := []string{legacyKey}
		if index := strings.LastIndex(legacyKey, "#"); index != -1 {
			keys = append(keys, legacyKey[:index])
		}
		for _, key := range keys {
			if port := saved[key]; port > 0 {
				saved[stableKey] = port
				break
			}
		}
	}
	keys := make([]string, 0, len(saved))
	for key := range saved {
		keys = append(keys, key)
	}
	sort.Strings(keys) // deterministic if legacy aliases share one identity
	for _, legacyKey := range keys {
		stableKey := stableNodeKey(legacyKey)
		if _, exists := saved[stableKey]; !exists && saved[legacyKey] > 0 {
			saved[stableKey] = saved[legacyKey]
		}
	}
}

// SaveNodePortMap persists the current node→port assignments next to the config
// file so a restart can restore them. It is a no-op in pool mode, where nodes
// have no per-node ports.
func (c *Config) SaveNodePortMap() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	path := c.portMapPath()
	if path == "" {
		return errors.New("config file path is unknown")
	}
	data, err := json.MarshalIndent(c.BuildPortMap(), "", "  ")
	if err != nil {
		return fmt.Errorf("encode port map: %w", err)
	}
	if err := writeFileWithLock(path, data, 0o644); err != nil {
		return fmt.Errorf("write port map %q: %w", path, err)
	}
	return nil
}

// applyPersistedPorts restores the on-disk node→port mapping so a restart keeps
// every node on the proxy port it previously used, then rewrites the mapping so
// the sidecar exists from first boot and drops entries for removed nodes.
//
// It clears only the provisional ports assigned by normalize() first, making
// NormalizeWithPortMap the single, collision-safe authority: nodes whose stable
// identity matches a saved entry get their saved port, and the rest get fresh,
// non-conflicting ports. Explicit inline ports take precedence over that node's
// old saved assignment. A corrupt or missing sidecar means "no saved ports".
func (c *Config) applyPersistedPorts(explicitPorts []bool) error {
	if c.Mode != "multi-port" && c.Mode != "hybrid" {
		return nil
	}
	// normalize() only assigned provisional ports (no bind checks). Run the
	// authoritative, bind-checked assignment exactly once here. A saved sidecar
	// supplies preserved ports; an empty/missing one means "assign all fresh".
	saved := loadNodePortMap(c.portMapPath())
	if len(saved) > 0 {
		// Migrate any legacy entries: sidecars written before stableNodeKey was
		// keyed by the raw full URI. Without this bridge, every node's stable key
		// would miss on the first post-upgrade boot and all proxy ports would be
		// reassigned at once. Map legacy raw-URI keys onto the new stable keys.
		bridgeLegacyPortKeys(c.Nodes, saved)
	}
	for i := range c.Nodes {
		if i < len(explicitPorts) && explicitPorts[i] {
			delete(saved, c.Nodes[i].NodeKey())
		} else {
			c.Nodes[i].Port = 0
		}
	}
	if err := c.NormalizeWithPortMap(saved); err != nil {
		return fmt.Errorf("restore persisted ports: %w", err)
	}
	// Persisting is best-effort by design: the proxy runs correctly without the
	// sidecar; only a subsequent restart would re-derive ports. A write failure
	// is logged rather than fatal.
	if err := c.SaveNodePortMap(); err != nil {
		log.Printf("⚠️  Failed to persist node ports: %v", err)
	}
	return nil
}

// NormalizeWithPortMap applies defaults and validation, preserving port assignments
// for nodes that exist in the provided port map.
func (c *Config) NormalizeWithPortMap(portMap map[string]uint16) error {
	if c.Mode == "" {
		c.Mode = "pool"
	}
	if c.Mode == "multi_port" {
		c.Mode = "multi-port"
	}
	switch c.Mode {
	case "pool", "multi-port", "hybrid":
	default:
		return fmt.Errorf("unsupported mode %q (use 'pool', 'multi-port', or 'hybrid')", c.Mode)
	}
	if c.Listener.Address == "" {
		c.Listener.Address = "0.0.0.0"
	}
	if c.Listener.Port == 0 {
		c.Listener.Port = 2323
	}
	if mode, err := normalizePoolMode(c.Pool.Mode); err != nil {
		return err
	} else {
		c.Pool.Mode = mode
	}
	if c.Pool.FailureThreshold <= 0 {
		c.Pool.FailureThreshold = 3
	}
	if c.Pool.BlacklistDuration <= 0 {
		c.Pool.BlacklistDuration = 24 * time.Hour
	}
	if c.Pool.TransientCooldown <= 0 {
		c.Pool.TransientCooldown = 60 * time.Second
	}
	if c.Pool.RateLimitCooldown <= 0 {
		c.Pool.RateLimitCooldown = c.Pool.TransientCooldown
	}
	if c.Pool.RetryAttempts <= 0 {
		c.Pool.RetryAttempts = 3
	}
	if c.MultiPort.Address == "" {
		c.MultiPort.Address = "0.0.0.0"
	}
	if c.MultiPort.BasePort == 0 {
		c.MultiPort.BasePort = 24000
	}
	if c.Management.Listen == "" {
		c.Management.Listen = "127.0.0.1:9091"
	}
	if c.Management.ProbeTarget == "" {
		c.Management.ProbeTarget = "www.apple.com:80"
	}
	if c.Management.Enabled == nil {
		defaultEnabled := true
		c.Management.Enabled = &defaultEnabled
	}
	if c.SubscriptionRefresh.Interval <= 0 {
		c.SubscriptionRefresh.Interval = 1 * time.Hour
	}
	if c.SubscriptionRefresh.Timeout <= 0 {
		c.SubscriptionRefresh.Timeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.HealthCheckTimeout <= 0 {
		c.SubscriptionRefresh.HealthCheckTimeout = 60 * time.Second
	}
	if c.SubscriptionRefresh.DrainTimeout <= 0 {
		c.SubscriptionRefresh.DrainTimeout = 30 * time.Second
	}
	if c.SubscriptionRefresh.MinAvailableNodes <= 0 {
		c.SubscriptionRefresh.MinAvailableNodes = 1
	}
	c.SubscriptionRefresh.FetchConcurrency = normalizeSubscriptionFetchConcurrency(c.SubscriptionRefresh.FetchConcurrency)

	if len(c.Nodes) == 0 {
		return errors.New("config.nodes cannot be empty")
	}

	// Build set of ports already assigned from portMap
	usedPorts := c.reservedListenerPorts()
	preservedNodePorts := make([]bool, len(c.Nodes))

	// First pass: assign ports from portMap for existing nodes
	preservedPorts := 0
	duplicatePortHits := 0
	for idx := range c.Nodes {
		c.Nodes[idx].Name = strings.TrimSpace(c.Nodes[idx].Name)
		c.Nodes[idx].URI = strings.TrimSpace(c.Nodes[idx].URI)
		if c.Nodes[idx].URI == "" {
			return fmt.Errorf("node %d is missing uri", idx)
		}

		// Auto-extract name from URI if not provided
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = ExtractNodeName(c.Nodes[idx].URI)
		}
		if c.Nodes[idx].Name == "" {
			c.Nodes[idx].Name = fmt.Sprintf("node-%d", idx)
		}

		// Check if this node has a preserved port from portMap. Guard against a
		// port that was already claimed by an earlier node sharing the same
		// stable key (e.g. a subscription listing the same server twice under
		// different display names): preserving it again would bind the same
		// proxy port twice (EADDRINUSE). Such a node is left at Port==0 so the
		// second pass assigns it a fresh, collision-free port.
		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			nodeKey := c.Nodes[idx].NodeKey()
			if existingPort, ok := portMap[nodeKey]; ok && existingPort > 0 {
				if usedPorts[existingPort] {
					c.Nodes[idx].Port = 0
					duplicatePortHits++
				} else {
					c.Nodes[idx].Port = existingPort
					usedPorts[existingPort] = true
					preservedNodePorts[idx] = true
					preservedPorts++
				}
			}
		}
	}
	// Reserve every saved assignment before accepting new explicit ports. A new
	// node appearing earlier in a subscription must not steal an existing node's
	// stable port; reject the conflict regardless of input order instead.
	if c.Mode == "multi-port" || c.Mode == "hybrid" {
		for idx := range c.Nodes {
			if preservedNodePorts[idx] || c.Nodes[idx].Port == 0 {
				continue
			}
			if usedPorts[c.Nodes[idx].Port] {
				return fmt.Errorf("node %q port %d conflicts with another listener", c.Nodes[idx].Name, c.Nodes[idx].Port)
			}
			usedPorts[c.Nodes[idx].Port] = true
		}
	}

	// Final pass: assign new ports for nodes without preserved ports. portCursor
	// is an int (not uint16) so the >65535 exhaustion guard actually fires:
	// a uint16 cursor would wrap to 0 and silently hand out unbindable low ports.
	portCursor := int(c.MultiPort.BasePort)
	newPorts := 0
	for idx := range c.Nodes {
		if c.Nodes[idx].Port == 0 && (c.Mode == "multi-port" || c.Mode == "hybrid") {
			// Check before conversion: 65536 must not wrap to an ephemeral port.
			if portCursor > 65535 {
				return fmt.Errorf("no available ports found starting from %d", c.MultiPort.BasePort)
			}
			// Find next available port that's not used
			for usedPorts[uint16(portCursor)] || !IsPortAvailable(c.MultiPort.Address, uint16(portCursor)) {
				portCursor++
				if portCursor > 65535 {
					return fmt.Errorf("no available ports found starting from %d", c.MultiPort.BasePort)
				}
			}
			c.Nodes[idx].Port = uint16(portCursor)
			usedPorts[uint16(portCursor)] = true
			newPorts++
			portCursor++
		} else if c.Nodes[idx].Port == 0 && portCursor <= 65535 {
			c.Nodes[idx].Port = uint16(portCursor)
			portCursor++
		}

		// Apply default credentials
		if c.Mode == "multi-port" || c.Mode == "hybrid" {
			if c.Nodes[idx].Username == "" {
				c.Nodes[idx].Username = c.MultiPort.Username
				c.Nodes[idx].Password = c.MultiPort.Password
			}
		}
	}
	if c.Mode == "multi-port" || c.Mode == "hybrid" {
		log.Printf("✅ Port normalization complete: preserved=%d, new=%d, duplicate_identity_conflicts=%d, total_nodes=%d",
			preservedPorts, newPorts, duplicatePortHits, len(c.Nodes))
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}

	c.normalizeLogConfig()

	if err := c.normalizeSticky(); err != nil {
		return err
	}

	return nil
}

// normalizeSticky applies defaults and validation for the optional sticky entry port.
// Sticky sessions only apply to the shared pool entry, so they are disabled
// outside pool/hybrid mode. Must run after node ports are assigned.
func (c *Config) normalizeSticky() error {
	if c.Sticky.SessionTTL < 0 || c.Sticky.SessionTTL > MaxSessionTTL {
		return fmt.Errorf("sticky.session_ttl must be positive and at most %s", MaxSessionTTL)
	}
	if c.Sticky.MaxSessions < 0 || c.Sticky.MaxSessions > MaxSessionCapacity {
		return fmt.Errorf("sticky.max_sessions must be between 1 and %d", MaxSessionCapacity)
	}
	c.Sticky.SessionTTL = c.Sticky.SessionTTLOrDefault()
	c.Sticky.MaxSessions = c.Sticky.MaxSessionsOrDefault()
	if !c.Sticky.Enabled {
		return nil
	}
	if c.Mode != "pool" && c.Mode != "hybrid" {
		log.Printf("⚠️  sticky.enabled is set but mode is %q; sticky only applies to pool/hybrid mode, disabling", c.Mode)
		c.Sticky.Enabled = false
		return nil
	}
	if c.Sticky.Port == 0 {
		if c.Listener.Port >= 65535 {
			return fmt.Errorf("cannot auto-assign sticky.port (listener.port is %d); set sticky.port explicitly", c.Listener.Port)
		}
		c.Sticky.Port = c.Listener.Port + 1
	}
	if c.Sticky.Port == c.Listener.Port {
		return fmt.Errorf("sticky.port %d conflicts with listener.port", c.Sticky.Port)
	}
	for idx := range c.Nodes {
		if c.Nodes[idx].Port == c.Sticky.Port {
			return fmt.Errorf("sticky.port %d conflicts with node %q port", c.Sticky.Port, c.Nodes[idx].Name)
		}
	}
	return nil
}

// normalizeLogConfig applies defaults to the log config.
func (c *Config) normalizeLogConfig() {
	if c.Log.Output == "" {
		c.Log.Output = "stdout"
	}
	if c.Log.File == "" {
		c.Log.File = "logs/easy_proxies.log"
	}
	// Resolve relative log file path against config dir
	if c.filePath != "" && !filepath.IsAbs(c.Log.File) {
		c.Log.File = filepath.Join(filepath.Dir(c.filePath), c.Log.File)
	}
	if c.Log.MaxSize <= 0 {
		c.Log.MaxSize = 50
	}
	if c.Log.MaxBackups <= 0 {
		c.Log.MaxBackups = 3
	}
	if c.Log.MaxAge <= 0 {
		c.Log.MaxAge = 7
	}
}

// ManagementEnabled reports whether the monitoring endpoint should run.
func (c *Config) ManagementEnabled() bool {
	if c.Management.Enabled == nil {
		return true
	}
	return *c.Management.Enabled
}

// ProbeConcurrencyOrDefault returns the configured probe concurrency clamped
// to a safe range (1-1024). When unset or invalid, a sensible default is used.
func (c *Config) ProbeConcurrencyOrDefault() int {
	v := c.Management.ProbeConcurrency
	if v <= 0 {
		return 32
	}
	if v > 1024 {
		return 1024
	}
	return v
}

// loadNodesFromFile reads a nodes file where each line is a proxy URI
// Lines starting with # are comments, empty lines are ignored
func loadNodesFromFile(path string) ([]NodeConfig, error) {
	data, err := readFileWithLock(path)
	if err != nil {
		return nil, err
	}
	return parseNodesFromContent(string(data))
}

// LoadNodesFromFile reads a nodes file where each non-comment line is a proxy URI.
func LoadNodesFromFile(path string) ([]NodeConfig, error) {
	return loadNodesFromFile(path)
}

func shouldUseCachedSubscriptionNodes(fetched []NodeConfig, cached []NodeConfig, cacheErr error, stats SubscriptionFetchStats) (bool, string) {
	if cacheErr != nil || len(cached) == 0 {
		return false, ""
	}
	if len(fetched) == 0 {
		return true, "all subscriptions failed or returned no usable nodes"
	}
	if stats.Failed > 0 && len(fetched) < len(cached) {
		return true, "partial refresh would reduce the node set"
	}
	return false, ""
}

func normalizeSubscriptionFetchConcurrency(v int) int {
	if v <= 0 {
		return defaultSubscriptionFetchConcurrency
	}
	if v > maxSubscriptionFetchConcurrency {
		return maxSubscriptionFetchConcurrency
	}
	return v
}

func newSubscriptionHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   minDuration(timeout, 10*time.Second),
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   minDuration(timeout, 10*time.Second),
		ResponseHeaderTimeout: minDuration(timeout, 15*time.Second),
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if a < b {
		return a
	}
	return b
}

// RedactURL removes credentials and query data from a URL before logging.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	u.User = nil
	if u.Path != "" && u.Path != "/" {
		u.Path = "/..."
		u.RawPath = ""
	}
	if u.RawQuery != "" {
		u.RawQuery = "redacted=1"
	}
	u.Fragment = ""
	return u.String()
}

func dedupeSubscriptionURLs(urls []string) (unique []string, deduped int) {
	seen := make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			deduped++
			continue
		}
		seen[raw] = struct{}{}
		unique = append(unique, raw)
	}
	return unique, deduped
}

func dedupeNodesByKey(nodes []NodeConfig) ([]NodeConfig, int) {
	if len(nodes) < 2 {
		return nodes, 0
	}
	seen := make(map[string]struct{}, len(nodes))
	out := nodes[:0]
	deduped := 0
	for _, node := range nodes {
		node.URI = strings.TrimSpace(node.URI)
		if node.URI == "" {
			deduped++
			continue
		}
		key := node.NodeKey()
		if key == "" {
			key = node.URI
		}
		if _, ok := seen[key]; ok {
			deduped++
			continue
		}
		seen[key] = struct{}{}
		out = append(out, node)
	}
	return out, deduped
}

// FetchSubscriptionNodes fetches subscription URLs concurrently, parses all
// supported subscription formats, and deduplicates URLs/nodes by stable identity.
func FetchSubscriptionNodes(ctx context.Context, urls []string, opts SubscriptionFetchOptions) ([]NodeConfig, SubscriptionFetchStats) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	concurrency := normalizeSubscriptionFetchConcurrency(opts.Concurrency)
	uniqueURLs, dedupedURLs := dedupeSubscriptionURLs(urls)
	stats := SubscriptionFetchStats{
		RequestedURLs: len(urls),
		UniqueURLs:    len(uniqueURLs),
		DedupedURLs:   dedupedURLs,
	}
	if len(uniqueURLs) == 0 {
		return nil, stats
	}

	client := opts.Client
	if client == nil {
		client = newSubscriptionHTTPClient(timeout)
	}

	type result struct {
		url   string
		nodes []NodeConfig
		err   error
	}
	jobs := make(chan string)
	results := make(chan result, len(uniqueURLs))

	workerCount := concurrency
	if workerCount > len(uniqueURLs) {
		workerCount = len(uniqueURLs)
	}
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for subURL := range jobs {
				nodes, err := fetchSubscriptionWithClient(ctx, client, subURL, timeout)
				results <- result{url: subURL, nodes: nodes, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, subURL := range uniqueURLs {
			select {
			case <-ctx.Done():
				return
			case jobs <- subURL:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	allNodes := make([]NodeConfig, 0)
	processed := 0
	for res := range results {
		processed++
		if res.err != nil {
			stats.Failed++
			stats.LastError = res.err
			if opts.Loggerf != nil {
				opts.Loggerf("⚠️ Failed to load subscription %s: %v (skipping)", RedactURL(res.url), res.err)
			}
			continue
		}
		if len(res.nodes) == 0 {
			stats.Empty++
		} else {
			stats.Successful++
		}
		if opts.Loggerf != nil {
			opts.Loggerf("✅ Loaded %d nodes from subscription %s", len(res.nodes), RedactURL(res.url))
		}
		allNodes = append(allNodes, res.nodes...)
	}
	if missing := len(uniqueURLs) - processed; missing > 0 {
		if err := ctx.Err(); err != nil {
			stats.Failed += missing
			stats.LastError = err
			if opts.Loggerf != nil {
				opts.Loggerf("⚠️ Subscription fetch context ended before %d subscription URLs were processed: %v", missing, err)
			}
		}
	}

	stats.Nodes = len(allNodes)
	allNodes, stats.DedupedNodes = dedupeNodesByKey(allNodes)
	return allNodes, stats
}

// loadNodesFromSubscription fetches and parses nodes from a subscription URL
// Supports multiple formats: base64 encoded, plain text, clash yaml, etc.
func loadNodesFromSubscription(subURL string, timeout time.Duration) ([]NodeConfig, error) {
	return fetchSubscriptionWithClient(context.Background(), newSubscriptionHTTPClient(timeout), subURL, timeout)
}

func fetchSubscriptionWithClient(ctx context.Context, client *http.Client, subURL string, timeout time.Duration) ([]NodeConfig, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if ctx == nil {
		ctx = context.Background()
	}
	parsed, err := url.Parse(subURL)
	if err != nil {
		return nil, fmt.Errorf("parse subscription url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("unsupported subscription scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("subscription URL is missing host")
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, "GET", subURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Use clash-compatible User-Agent to get Clash YAML format from subscription servers
	// This ensures we receive structured YAML with all proxy types (AnyTLS, TUIC, etc.)
	// instead of base64-encoded content that may only contain basic SS nodes
	req.Header.Set("User-Agent", "clash-verge/v2.2.3")
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, redactSubscriptionError("fetch subscription", subURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subscription returned status %d", resp.StatusCode)
	}

	limitedReader := io.LimitReader(resp.Body, maxSubscriptionBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(body) > maxSubscriptionBodySize {
		return nil, fmt.Errorf("subscription response exceeds %d bytes", maxSubscriptionBodySize)
	}

	content := string(body)

	// Try to detect and parse different formats
	return parseSubscriptionContent(content)
}

func redactSubscriptionError(op, rawURL string, err error) error {
	if err == nil {
		return nil
	}
	redacted := RedactURL(rawURL)
	message := strings.ReplaceAll(err.Error(), rawURL, redacted)
	if parsed, parseErr := url.Parse(rawURL); parseErr == nil {
		message = strings.ReplaceAll(message, parsed.String(), redacted)
	}
	return fmt.Errorf("%s: %s", op, message)
}

// parseSubscriptionContent tries to parse subscription content in various formats (optimized)
func parseSubscriptionContent(content string) ([]NodeConfig, error) {
	content = strings.TrimSpace(content)

	// Detect Clash YAML by a line-anchored top-level "proxies:" key anywhere in
	// the document. The whole content is scanned (not just a 16 KB prefix): full
	// Clash configs often place proxies after large dns / rule / proxy-provider
	// sections, so a fixed-size window would misdetect them as base64/plaintext
	// and drop every node. Line-anchoring avoids matching a stray "proxies:"
	// inside a base64 blob.
	if strings.HasPrefix(content, "proxies:") || strings.Contains(content, "\nproxies:") {
		return parseClashYAML(content)
	}

	if isBase64(content) {
		if decoded, err := decodeProxyBase64(content); err == nil {
			content = string(decoded)
		}
	}

	// Parse as plain text (one URI per line)
	return parseNodesFromContent(content)
}

// ParseSubscriptionContent parses subscription content in various formats (base64, plain text, Clash YAML).
// This is exported for use by the subscription manager.
func ParseSubscriptionContent(content string) ([]NodeConfig, error) {
	return parseSubscriptionContent(content)
}

// parseNodesFromContent parses nodes from plain text content (one URI per line)
func parseNodesFromContent(content string) ([]NodeConfig, error) {
	var nodes []NodeConfig
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check if it's a valid proxy URI
		if IsProxyURI(line) {
			nodes = append(nodes, NodeConfig{
				URI: line,
			})
		}
	}

	return nodes, nil
}

// isBase64 checks if a string looks like base64 encoded content (optimized version)
func isBase64(s string) bool {
	// Remove whitespace
	s = strings.Join(strings.Fields(s), "")
	if len(s) == 0 {
		return false
	}

	// Remove newlines for checking
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")

	// Quick check: if it contains proxy URI schemes, it's not base64
	if strings.Contains(s, "://") {
		return false
	}

	// Check character set - base64 only contains A-Za-z0-9+/=
	// This is much faster than trying to decode
	for _, c := range s {
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=' || c == '-' || c == '_') {
			return false
		}
	}

	// Length must be multiple of 4 (with padding)
	return len(s)%4 != 1
}

// IsProxyURI checks if a string is a valid proxy URI
func IsProxyURI(s string) bool {
	schemes := []string{"vmess://", "vless://", "trojan://", "ss://", "shadowsocks://", "ssr://", "hysteria://", "hysteria2://", "hy2://", "tuic://", "socks5://", "socks5h://", "socks://", "http://", "https://", "anytls://"}
	lower := strings.ToLower(s)
	for _, scheme := range schemes {
		if strings.HasPrefix(lower, scheme) {
			return true
		}
	}
	return false
}

// clashConfig represents a minimal Clash configuration for parsing proxies
// flexInt handles YAML values that may be either int or quoted string.
type flexInt int

func (fi *flexInt) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var intVal int
	if err := unmarshal(&intVal); err == nil {
		*fi = flexInt(intVal)
		return nil
	}
	var strVal string
	if err := unmarshal(&strVal); err != nil {
		return fmt.Errorf("cannot unmarshal port: expected int or string")
	}
	parsed, err := strconv.Atoi(strVal)
	if err != nil {
		return fmt.Errorf("cannot parse port %q as int: %w", strVal, err)
	}
	*fi = flexInt(parsed)
	return nil
}

type clashConfig struct {
	Proxies []yaml.Node `yaml:"proxies"`
}

type clashProxy struct {
	Name              string                 `yaml:"name"`
	Type              string                 `yaml:"type"`
	Server            string                 `yaml:"server"`
	Port              flexInt                `yaml:"port"`
	Ports             string                 `yaml:"ports"`
	UUID              string                 `yaml:"uuid"`
	Username          string                 `yaml:"username"`
	Password          string                 `yaml:"password"`
	Cipher            string                 `yaml:"cipher"`
	AlterId           int                    `yaml:"alterId"`
	Network           string                 `yaml:"network"`
	TLS               bool                   `yaml:"tls"`
	SkipCertVerify    bool                   `yaml:"skip-cert-verify"`
	ServerName        string                 `yaml:"servername"`
	SNI               string                 `yaml:"sni"`
	Flow              string                 `yaml:"flow"`
	UDP               bool                   `yaml:"udp"`
	WSOpts            *clashWSOptions        `yaml:"ws-opts"`
	GrpcOpts          *clashGrpcOptions      `yaml:"grpc-opts"`
	RealityOpts       *clashRealityOptions   `yaml:"reality-opts"`
	ClientFingerprint string                 `yaml:"client-fingerprint"`
	Obfs              string                 `yaml:"obfs"`
	ObfsPassword      string                 `yaml:"obfs-password"`
	Plugin            string                 `yaml:"plugin"`
	PluginOpts        map[string]interface{} `yaml:"plugin-opts"`
	// TUIC-specific fields
	ALPN                 []string `yaml:"alpn"`
	CongestionController string   `yaml:"congestion-controller"`
	UDPRelayMode         string   `yaml:"udp-relay-mode"`
	// ShadowsocksR-specific fields
	Protocol      string `yaml:"protocol"`
	ProtocolParam string `yaml:"protocol-param"`
	ObfsParam     string `yaml:"obfs-param"`
	// Hysteria v1-specific fields
	AuthStr        string `yaml:"auth-str"`
	Auth           string `yaml:"auth"`
	UpMbps         int    `yaml:"up"`
	DownMbps       int    `yaml:"down"`
	PeerSNI        string `yaml:"peer"`
	RecvWindow     uint64 `yaml:"recv-window"`
	RecvWindowConn uint64 `yaml:"recv-window-conn"`
	DisableMTU     bool   `yaml:"disable_mtu_discovery"`
}

type clashWSOptions struct {
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
}

type clashGrpcOptions struct {
	GrpcServiceName string `yaml:"grpc-service-name"`
}

type clashRealityOptions struct {
	PublicKey string `yaml:"public-key"`
	ShortID   string `yaml:"short-id"`
}

// parseClashYAML parses Clash YAML format and converts to NodeConfig.
// Per-proxy decoding: a single malformed entry won't fail the whole subscription;
// failed proxies are logged and skipped (fixes #23).
func parseClashYAML(content string) ([]NodeConfig, error) {
	var clash clashConfig
	if err := yaml.Unmarshal([]byte(content), &clash); err != nil {
		return nil, fmt.Errorf("parse clash yaml: %w", err)
	}

	var nodes []NodeConfig
	skipped := 0
	for i, raw := range clash.Proxies {
		var proxy clashProxy
		if err := raw.Decode(&proxy); err != nil {
			skipped++
			log.Printf("[subscription] WARN: skip proxy #%d (decode error): %v", i, err)
			continue
		}
		uri := convertClashProxyToURI(proxy)
		if uri == "" {
			skipped++
			log.Printf("[subscription] WARN: skip proxy %q (unsupported type %q)", proxy.Name, proxy.Type)
			continue
		}
		nodes = append(nodes, NodeConfig{
			Name: proxy.Name,
			URI:  uri,
		})
	}
	if skipped > 0 {
		log.Printf("[subscription] parsed %d nodes, skipped %d malformed/unsupported entries", len(nodes), skipped)
	}

	return nodes, nil
}

// convertClashProxyToURI converts a Clash proxy config to a standard URI
func convertClashProxyToURI(p clashProxy) string {
	switch strings.ToLower(p.Type) {
	case "vmess":
		return buildVMessURI(p)
	case "vless":
		return buildVLESSURI(p)
	case "trojan":
		return buildTrojanURI(p)
	case "anytls":
		return buildAnyTLSURI(p)
	case "ss", "shadowsocks":
		return buildShadowsocksURI(p)
	case "hysteria2", "hy2":
		return buildHysteria2URI(p)
	case "tuic":
		return buildTUICURI(p)
	case "ssr", "shadowsocksr":
		return buildShadowsocksRURI(p)
	case "hysteria":
		return buildHysteriaURI(p)
	case "http", "https":
		return buildHTTPProxyURI(p)
	default:
		return ""
	}
}

func buildHTTPProxyURI(p clashProxy) string {
	scheme := strings.ToLower(p.Type)
	if scheme == "http" && p.TLS {
		scheme = "https"
	}
	port := int(p.Port)
	if port == 0 {
		if scheme == "https" {
			port = 443
		} else {
			port = 8080
		}
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(p.Server, strconv.Itoa(port)),
	}
	if p.Password != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	} else if p.Username != "" {
		u.User = url.User(p.Username)
	}
	if p.Name != "" {
		u.Fragment = p.Name
	}
	return u.String()
}

func clashNodeURI(scheme string, p clashProxy, port int, user *url.Userinfo, params url.Values) string {
	u := url.URL{Scheme: scheme, Host: net.JoinHostPort(p.Server, strconv.Itoa(port)), User: user, RawQuery: params.Encode(), Fragment: p.Name}
	return u.String()
}

func buildVMessURI(p clashProxy) string {
	params := url.Values{}
	if p.AlterId != 0 {
		params.Set("alterId", strconv.Itoa(p.AlterId))
	}
	if p.Cipher != "" {
		params.Set("encryption", p.Cipher)
	}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.GrpcOpts != nil && p.GrpcOpts.GrpcServiceName != "" {
		params.Set("serviceName", p.GrpcOpts.GrpcServiceName)
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return clashNodeURI("vmess", p, int(p.Port), url.User(p.UUID), params)
}

func buildVLESSURI(p clashProxy) string {
	params := url.Values{}
	params.Set("encryption", "none")

	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.Flow != "" {
		params.Set("flow", p.Flow)
	}
	if p.TLS {
		params.Set("security", "tls")
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		} else if p.SNI != "" {
			params.Set("sni", p.SNI)
		}
	}
	if p.RealityOpts != nil {
		params.Set("security", "reality")
		if p.RealityOpts.PublicKey != "" {
			params.Set("pbk", p.RealityOpts.PublicKey)
		}
		if p.RealityOpts.ShortID != "" {
			params.Set("sid", p.RealityOpts.ShortID)
		}
		if p.ServerName != "" {
			params.Set("sni", p.ServerName)
		}
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.GrpcOpts != nil && p.GrpcOpts.GrpcServiceName != "" {
		params.Set("serviceName", p.GrpcOpts.GrpcServiceName)
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return clashNodeURI("vless", p, int(p.Port), url.User(p.UUID), params)
}

func buildTrojanURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.Network != "" && p.Network != "tcp" {
		params.Set("type", p.Network)
	}
	if p.WSOpts != nil {
		if p.WSOpts.Path != "" {
			params.Set("path", p.WSOpts.Path)
		}
		if host, ok := p.WSOpts.Headers["Host"]; ok {
			params.Set("host", host)
		}
	}
	if p.GrpcOpts != nil && p.GrpcOpts.GrpcServiceName != "" {
		params.Set("serviceName", p.GrpcOpts.GrpcServiceName)
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return clashNodeURI("trojan", p, int(p.Port), url.User(p.Password), params)
}

func buildAnyTLSURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.ClientFingerprint != "" {
		params.Set("fp", p.ClientFingerprint)
	}

	return clashNodeURI("anytls", p, int(p.Port), url.User(p.Password), params)
}

func buildShadowsocksURI(p clashProxy) string {
	// Encode method:password in base64
	userInfo := base64.RawURLEncoding.EncodeToString([]byte(p.Cipher + ":" + p.Password))
	return clashNodeURI("ss", p, int(p.Port), url.User(userInfo), nil)
}

func buildHysteria2URI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfs-password", p.ObfsPassword)
		}
	}
	if strings.TrimSpace(p.Ports) != "" {
		params.Set("ports", normalizeHysteria2PortsValue(strings.TrimSpace(p.Ports)))
	}

	port := int(p.Port)
	if port <= 0 {
		port = 443
	}

	return clashNodeURI("hysteria2", p, port, url.User(p.Password), params)
}

func normalizeHysteria2PortsValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	parts := strings.Split(value, ",")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, ":") {
			normalized = append(normalized, part)
			continue
		}
		if strings.Count(part, "-") == 1 {
			normalized = append(normalized, strings.Replace(part, "-", ":", 1))
			continue
		}
		normalized = append(normalized, part)
	}

	return strings.Join(normalized, ",")
}

func buildTUICURI(p clashProxy) string {
	params := url.Values{}
	if p.ServerName != "" {
		params.Set("sni", p.ServerName)
	} else if p.SNI != "" {
		params.Set("sni", p.SNI)
	}
	if p.SkipCertVerify {
		params.Set("allowInsecure", "1")
	}
	if p.CongestionController != "" {
		params.Set("congestion_control", p.CongestionController)
	}
	if p.UDPRelayMode != "" {
		params.Set("udp_relay_mode", p.UDPRelayMode)
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}

	// TUIC URI format: tuic://uuid:password@server:port?params#name
	return clashNodeURI("tuic", p, int(p.Port), url.UserPassword(p.UUID, p.Password), params)
}

// buildShadowsocksRURI converts a Clash SSR proxy config to an SSR URI.
// Format: ssr://base64(host:port:protocol:method:obfs:base64(password)/?obfsparam=base64&protoparam=base64&remarks=base64)
func buildShadowsocksRURI(p clashProxy) string {
	passwordB64 := base64.URLEncoding.EncodeToString([]byte(p.Password))

	main := fmt.Sprintf("%s:%d:%s:%s:%s:%s",
		p.Server, int(p.Port),
		defaultStr(p.Protocol, "origin"),
		defaultStr(p.Cipher, "none"),
		defaultStr(p.Obfs, "plain"),
		passwordB64,
	)

	var params []string
	if p.ObfsParam != "" {
		params = append(params, "obfsparam="+base64.URLEncoding.EncodeToString([]byte(p.ObfsParam)))
	}
	if p.ProtocolParam != "" {
		params = append(params, "protoparam="+base64.URLEncoding.EncodeToString([]byte(p.ProtocolParam)))
	}
	if p.Name != "" {
		params = append(params, "remarks="+base64.URLEncoding.EncodeToString([]byte(p.Name)))
	}

	payload := main
	if len(params) > 0 {
		payload += "/?" + strings.Join(params, "&")
	}
	return "ssr://" + base64.URLEncoding.EncodeToString([]byte(payload))
}

// buildHysteriaURI converts a Clash Hysteria v1 proxy config to a hysteria:// URI.
// Format: hysteria://host:port?protocol=udp&auth=xxx&peer=sni&insecure=1&upmbps=N&downmbps=N&alpn=h3&obfs=xplus#name
func buildHysteriaURI(p clashProxy) string {
	params := url.Values{}
	params.Set("protocol", "udp")

	auth := p.AuthStr
	if auth == "" {
		auth = p.Auth
	}
	if auth == "" {
		auth = p.Password
	}
	if auth != "" {
		params.Set("auth", auth)
	}
	if p.ServerName != "" {
		params.Set("peer", p.ServerName)
	} else if p.SNI != "" {
		params.Set("peer", p.SNI)
	} else if p.PeerSNI != "" {
		params.Set("peer", p.PeerSNI)
	}
	if p.SkipCertVerify {
		params.Set("insecure", "1")
	}
	if p.UpMbps > 0 {
		params.Set("upmbps", strconv.Itoa(p.UpMbps))
	}
	if p.DownMbps > 0 {
		params.Set("downmbps", strconv.Itoa(p.DownMbps))
	}
	if len(p.ALPN) > 0 {
		params.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if p.Obfs != "" {
		params.Set("obfs", p.Obfs)
		if p.ObfsPassword != "" {
			params.Set("obfsParam", p.ObfsPassword)
		}
	}

	return clashNodeURI("hysteria", p, int(p.Port), nil, params)
}

func defaultStr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// FilePath returns the config file path.
func (c *Config) FilePath() string {
	if c == nil {
		return ""
	}
	return c.filePath
}

// SetFilePath sets the config file path (used when creating config programmatically).
func (c *Config) SetFilePath(path string) {
	if c != nil {
		c.filePath = path
	}
}

// writeNodesToFile writes nodes to a file (one URI per line) with file locking.
func writeNodesToFile(path string, nodes []NodeConfig) error {
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	// Use file locking for safe concurrent writes
	return writeFileWithLock(path, []byte(content), 0o644)
}

// WriteNodesToFile persists a complete subscription cache through the common
// locked writer, without changing inline nodes or management settings.
func WriteNodesToFile(path string, nodes []NodeConfig) error {
	return writeNodesToFile(path, nodes)
}

// SaveNodes persists nodes to their appropriate locations based on source.
// - subscription/nodes_file nodes → nodes.txt (or configured nodes_file)
// - inline nodes → config.yaml nodes array
// Config.yaml structure (subscriptions, nodes_file) is preserved.
func (c *Config) SaveNodes() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.filePath == "" {
		return errors.New("config file path is unknown")
	}

	// Check if config file is writable before attempting save
	if err := checkFileWritable(c.filePath); err != nil {
		return fmt.Errorf("config file not writable: %w (check file permissions and Docker volume mounts)", err)
	}

	// Separate nodes by source
	var inlineNodes []NodeConfig
	var fileNodes []NodeConfig

	for _, node := range c.Nodes {
		// Create a clean copy without runtime fields for saving
		cleanNode := NodeConfig{
			Name:     node.Name,
			URI:      node.URI,
			Port:     node.Port,
			Username: node.Username,
			Password: node.Password,
		}
		switch node.Source {
		case NodeSourceInline:
			inlineNodes = append(inlineNodes, cleanNode)
		case NodeSourceFile, NodeSourceSubscription:
			fileNodes = append(fileNodes, cleanNode)
		default:
			// Default to file nodes for unknown source
			fileNodes = append(fileNodes, cleanNode)
		}
	}

	// Write file-based nodes to nodes.txt
	if len(fileNodes) > 0 || c.NodesFile != "" {
		nodesFilePath := c.NodesFile
		if nodesFilePath == "" {
			nodesFilePath = filepath.Join(filepath.Dir(c.filePath), "nodes.txt")
		}
		// Check writability before writing
		if err := checkFileWritable(nodesFilePath); err != nil {
			return fmt.Errorf("nodes file not writable: %w (check file permissions and Docker volume mounts)", err)
		}
		if err := writeNodesToFile(nodesFilePath, fileNodes); err != nil {
			return fmt.Errorf("write nodes file %q: %w", nodesFilePath, err)
		}
		log.Printf("✅ Saved %d nodes to %s", len(fileNodes), nodesFilePath)
	}

	if err := updateConfigFile(c.filePath, func(saved *Config) { saved.Nodes = inlineNodes }); err != nil {
		return err
	}
	log.Printf("✅ Saved %d inline nodes to %s", len(inlineNodes), c.filePath)

	return nil
}

// Save is deprecated, use SaveNodes instead.
// This method is kept for backward compatibility but now delegates to SaveNodes.
func (c *Config) Save() error {
	return c.SaveNodes()
}

// SettingsSnapshot copies only fields owned by the management settings API.
// The caller must synchronize settings access; runtime node/subscription fields
// deliberately are not read or copied.
func (c *Config) SettingsSnapshot() Config {
	return Config{
		Mode: c.Mode, Listener: c.Listener, MultiPort: c.MultiPort, Pool: c.Pool,
		Sticky: c.Sticky, Management: c.Management, ExternalIP: c.ExternalIP,
		SkipCertVerify: c.SkipCertVerify, Log: c.Log, GeoIP: c.GeoIP,
		filePath: c.filePath,
	}
}

// SaveManagementSettings leaves subscription and node data in the on-disk
// configuration intact; they have separate management endpoints/lifecycles.
func (c *Config) SaveManagementSettings() error {
	if err := c.ValidateManagementSettings(); err != nil {
		return err
	}
	return c.saveSettings(false)
}

// SaveSubscriptionSettings updates only subscription ownership fields.
func (c *Config) SaveSubscriptionSettings() error {
	if c == nil || c.filePath == "" {
		return errors.New("config file path is unknown")
	}
	return updateConfigFile(c.filePath, func(saved *Config) {
		saved.Subscriptions = append([]string(nil), c.Subscriptions...)
		saved.SubscriptionRefresh = c.SubscriptionRefresh
	})
}

// SaveSettings persists config settings and subscription configuration
// without touching nodes.txt. Use this for settings API updates.
func (c *Config) SaveSettings() error {
	return c.saveSettings(true)
}

func (c *Config) saveSettings(includeSubscriptions bool) error {
	if c == nil {
		return errors.New("config is nil")
	}
	if c.filePath == "" {
		return errors.New("config file path is unknown")
	}

	return updateConfigFile(c.filePath, func(saveCfg *Config) {
		saveCfg.ExternalIP = c.ExternalIP
		saveCfg.SkipCertVerify = c.SkipCertVerify
		saveCfg.Log = c.Log
		if includeSubscriptions {
			saveCfg.Subscriptions = append([]string(nil), c.Subscriptions...)
			saveCfg.SubscriptionRefresh = c.SubscriptionRefresh
		}
		saveCfg.GeoIP = c.GeoIP
		saveCfg.Mode = c.Mode
		saveCfg.Listener = c.Listener
		saveCfg.MultiPort = c.MultiPort
		saveCfg.Pool = c.Pool
		saveCfg.Sticky = c.Sticky
		saveCfg.Management = c.Management
	})
}

// IsPortAvailable checks if a port is available for binding.
func IsPortAvailable(address string, port uint16) bool {
	addr := net.JoinHostPort(address, strconv.Itoa(int(port)))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

// checkFileWritable checks if a file is writable. Creates parent directories if needed.
func checkFileWritable(path string) error {
	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create directory %q: %w", dir, err)
	}

	// Check if file exists
	info, err := os.Stat(path)
	if err == nil {
		// File exists, check if writable
		if info.Mode().Perm()&0o222 == 0 {
			return fmt.Errorf("file %q is read-only (mode: %s)", path, info.Mode())
		}
		// Try to open for writing
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return fmt.Errorf("cannot open for writing: %w", err)
		}
		f.Close()
		return nil
	}

	if !os.IsNotExist(err) {
		return fmt.Errorf("stat file: %w", err)
	}

	// File doesn't exist, check if directory is writable
	f, err := os.CreateTemp(dir, ".easy-proxies-permission-*")
	if err != nil {
		return fmt.Errorf("directory %q is not writable: %w", dir, err)
	}
	f.Close()
	os.Remove(f.Name())
	return nil
}
