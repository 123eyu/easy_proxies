package monitor

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"easy_proxies/internal/config"
	"easy_proxies/internal/routing"
)

// SetRoutingStore shares the proxy pool's bindings with management endpoints.
// These bindings are unrelated to browser authentication sessions.
func (s *Server) SetRoutingStore(store *routing.Store) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	s.routingStore = store
	s.cfgMu.Unlock()
}

func (s *Server) proxyRoutingSnapshot() (*config.Config, *routing.Store) {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfgSrc == nil {
		return nil, s.routingStore
	}
	cfg := s.cfgSrc.SettingsSnapshot()
	return &cfg, s.routingStore
}

func proxySessionAvailability(cfg *config.Config) (bool, string) {
	if cfg == nil {
		return false, "配置存储未初始化"
	}
	if cfg.Mode != "" && cfg.Mode != "pool" && cfg.Mode != "hybrid" {
		return false, "会话路由需要 pool 或 hybrid 模式"
	}
	if !cfg.Sticky.SessionRoutingEnabled() {
		return false, "会话路由已关闭"
	}
	if cfg.Listener.Username == "" {
		return false, "请先设置共享代理入口的用户名"
	}
	return true, ""
}

func (s *Server) handleProxySessions(w http.ResponseWriter, r *http.Request) {
	cfg, store := s.proxyRoutingSnapshot()
	switch r.Method {
	case http.MethodGet:
		enabled, reason := proxySessionAvailability(cfg)
		if store == nil && enabled {
			enabled, reason = false, "会话路由存储未初始化"
		}
		ttl, maxSessions := config.DefaultSessionTTL, config.DefaultMaxSessions
		if cfg != nil {
			ttl, maxSessions = cfg.Sticky.SessionTTLOrDefault(), cfg.Sticky.MaxSessionsOrDefault()
		}
		sessions := make([]routing.Binding, 0)
		if store != nil {
			sessions = store.List()
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, map[string]any{
			"enabled": enabled, "reason": reason, "total": len(sessions),
			"ttl": ttl.String(), "max_sessions": maxSessions, "sessions": sessions,
		})
	case http.MethodDelete:
		if store == nil {
			writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "会话路由存储未初始化"})
			return
		}
		count := len(store.List())
		store.Clear()
		writeJSON(w, map[string]any{"message": "已重置全部会话路由，下次请求将重新选择节点", "cleared": count})
	default:
		w.Header().Set("Allow", "GET, DELETE")
		writeStatusJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "仅支持 GET 或 DELETE"})
	}
}

func (s *Server) handleProxySessionItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", "DELETE")
		writeStatusJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "仅支持 DELETE"})
		return
	}
	// Decode exactly one escaped path segment; usernames can themselves contain
	// characters such as '/' and must not be interpreted as nested API paths.
	escapedID := strings.TrimPrefix(r.URL.EscapedPath(), "/api/proxy-sessions/")
	id, err := url.PathUnescape(escapedID)
	if err != nil || id == "" || strings.Contains(escapedID, "/") {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "会话 ID 无效"})
		return
	}
	_, store := s.proxyRoutingSnapshot()
	if store == nil {
		writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "会话路由存储未初始化"})
		return
	}
	if !store.Delete(id) {
		writeStatusJSON(w, http.StatusNotFound, map[string]any{"error": "会话路由不存在或已过期"})
		return
	}
	writeJSON(w, map[string]any{"message": "会话路由已重置，下次请求将重新选择节点", "deleted": true})
}

// proxyLinkHost avoids exporting wildcard listener addresses. Configured
// external_ip wins, followed by the management request's host, then loopback.
func proxyLinkHost(externalIP string, r *http.Request) string {
	for _, candidate := range []string{externalIP, r.Host} {
		host := strings.TrimSpace(candidate)
		if host == "" {
			continue
		}
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
			host = host[1 : len(host)-1]
		}
		if ip, err := netip.ParseAddr(host); err == nil {
			if !ip.IsUnspecified() {
				return host
			}
			continue
		}
		if host != "" && !strings.ContainsAny(host, ":[]% /\\@?#\t\r\n") {
			return host
		}
	}
	return "127.0.0.1"
}

func (s *Server) handleProxySessionLinks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeStatusJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "仅支持 POST"})
		return
	}
	var req struct {
		SessionID string `json:"session_id"`
		Scheme    string `json:"scheme"`
	}
	if err := decodeManagementJSON(w, r, 4096, &req); err != nil {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "请求格式错误"})
		return
	}
	if req.Scheme == "" {
		req.Scheme = "http"
	}
	if req.Scheme != "http" && req.Scheme != "socks5" {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "scheme 仅支持 http 或 socks5"})
		return
	}
	if req.SessionID != "" && !routing.ValidSessionName(req.SessionID) {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "session_id 须为 1 到 64 位字母、数字、下划线或短横线"})
		return
	}
	cfg, store := s.proxyRoutingSnapshot()
	if cfg == nil {
		writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "配置存储未初始化"})
		return
	}
	if cfg.Mode != "" && cfg.Mode != "pool" && cfg.Mode != "hybrid" {
		writeStatusJSON(w, http.StatusConflict, map[string]any{"error": "当前模式没有共享代理入口，请使用 pool 或 hybrid 模式"})
		return
	}
	enabled, reason := proxySessionAvailability(cfg)
	if req.SessionID != "" && !enabled {
		writeStatusJSON(w, http.StatusConflict, map[string]any{"error": reason})
		return
	}
	if store == nil {
		enabled = false
		if req.SessionID != "" {
			writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "会话路由存储未初始化"})
			return
		}
	}
	if cfg.Listener.Port == 0 {
		writeStatusJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "共享代理入口端口未配置"})
		return
	}
	username := cfg.Listener.Username
	if req.SessionID != "" {
		username += "." + req.SessionID
	}
	if req.Scheme == "socks5" && (len(username) > 255 || len(cfg.Listener.Password) > 255) {
		writeStatusJSON(w, http.StatusBadRequest, map[string]any{"error": "SOCKS5 用户名和密码各须不超过 255 字节，请缩短会话 ID 或使用 HTTP"})
		return
	}
	link := url.URL{Scheme: req.Scheme, Host: net.JoinHostPort(proxyLinkHost(cfg.ExternalIP, r), strconv.Itoa(int(cfg.Listener.Port)))}
	if username != "" {
		link.User = url.UserPassword(username, cfg.Listener.Password)
	}
	masked := link
	if username != "" {
		masked.User = url.UserPassword(username, "********")
	}
	// Link generation never dials upstreams or creates an affinity binding.
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, map[string]any{
		"url": link.String(), "masked_url": masked.String(), "username": username,
		"session_id": req.SessionID, "scheme": req.Scheme, "enabled": enabled,
	})
}

func writeStatusJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeJSON(w, payload)
}

// decodeManagementJSON accepts one bounded JSON object, not a valid prefix of an
// oversized or malformed body. Unknown fields remain compatible with clients.
func decodeManagementJSON(w http.ResponseWriter, r *http.Request, limit int64, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("expected a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("unexpected trailing JSON value")
	}
	return json.Unmarshal(raw, target)
}
