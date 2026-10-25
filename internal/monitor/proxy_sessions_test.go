package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/routing"
	"gopkg.in/yaml.v3"
)

func newProxySessionTestServer(t *testing.T, cfg *config.Config) (*Server, *routing.Store) {
	t.Helper()
	mgr, err := NewManager(Config{})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{Enabled: true, Listen: "127.0.0.1:0"}, mgr, nil)
	s.SetConfig(cfg)
	t.Cleanup(func() { s.Shutdown(context.Background()); mgr.Stop() })
	store := routing.NewStore(100, time.Hour)
	s.SetRoutingStore(store)
	return s, store
}

func proxySessionTestConfig() *config.Config {
	return &config.Config{
		Mode: "pool",
		Listener: config.ListenerConfig{
			Address: "0.0.0.0", Port: 2323, Username: "wang", Password: "1234",
		},
	}
}

func proxyAPIRequest(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://dashboard.example:9091"+path, strings.NewReader(body))
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	return w
}

func decodeProxyResponse(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode %d %s: %v", w.Code, w.Body.String(), err)
	}
	return response
}

func bindProxyTestSession(t *testing.T, store *routing.Store, id string) {
	t.Helper()
	lease, err := store.Acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	lease.Bind("opaque-id", "node-tag", "Display node", "initial")
	lease.Release()
}

func TestProxySessionLinks(t *testing.T) {
	tests := []struct {
		name, externalIP, host, username, password, session, scheme, wantHost string
	}{
		{"named HTTP", "", "dashboard.example:9091", "wang", "1234", "account-1", "http", "dashboard.example:2323"},
		{"named SOCKS5", "", "localhost:9091", "wang", "1234", "Account_2", "socks5", "localhost:2323"},
		{"ordinary HTTP", "", "localhost:9091", "wang", "1234", "", "", "localhost:2323"},
		{"external wins", "proxy.example", "internal.example:9091", "wang", "1234", "one", "http", "proxy.example:2323"},
		{"external IPv6", "2001:db8::7", "internal.example:9091", "wang", "1234", "one", "http", "[2001:db8::7]:2323"},
		{"bracketed IPv6", "[2001:db8::8]", "internal.example:9091", "wang", "1234", "one", "http", "[2001:db8::8]:2323"},
		{"request IPv6", "", "[2001:db8::9]:9091", "wang", "1234", "one", "http", "[2001:db8::9]:2323"},
		{"request IPv6 zone", "", "[fe80::1%eth0]:9091", "wang", "1234", "one", "http", "[fe80::1%eth0]:2323"},
		{"wildcard external", "0.0.0.0", "proxy.example:9091", "wang", "1234", "one", "http", "proxy.example:2323"},
		{"wildcard fallback", "::", "0.0.0.0:9091", "wang", "1234", "one", "http", "127.0.0.1:2323"},
		{"IPv6 wildcard fallback", "", "[::]:9091", "wang", "1234", "one", "http", "127.0.0.1:2323"},
		{"encoded credentials", "", "localhost:9091", "wang+name/@", "p@ss:word/with?#%密", "one", "http", "localhost:2323"},
		{"anonymous ordinary", "", "localhost:9091", "", "", "", "http", "localhost:2323"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := proxySessionTestConfig()
			cfg.ExternalIP, cfg.Listener.Username, cfg.Listener.Password = tt.externalIP, tt.username, tt.password
			s, store := newProxySessionTestServer(t, cfg)
			body, _ := json.Marshal(map[string]string{"session_id": tt.session, "scheme": tt.scheme})
			r := httptest.NewRequest(http.MethodPost, "/api/proxy-sessions/links", strings.NewReader(string(body)))
			r.Host = tt.host
			w := httptest.NewRecorder()
			s.srv.Handler.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			response := decodeProxyResponse(t, w)
			link, err := url.Parse(response["url"].(string))
			if err != nil {
				t.Fatal(err)
			}
			if link.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", link.Host, tt.wantHost)
			}
			wantUsername := tt.username
			if tt.session != "" {
				wantUsername += "." + tt.session
			}
			if response["username"] != wantUsername || response["session_id"] != tt.session {
				t.Errorf("wrong username/session: %v", response)
			}
			if wantUsername != "" {
				password, _ := link.User.Password()
				if link.User.Username() != wantUsername || password != tt.password {
					t.Errorf("credentials did not round trip: %s", response["url"])
				}
				masked, _ := url.Parse(response["masked_url"].(string))
				maskedPassword, _ := masked.User.Password()
				if maskedPassword != "********" {
					t.Errorf("masked password = %q", maskedPassword)
				}
			} else if link.User != nil {
				t.Errorf("anonymous link includes credentials: %s", link)
			}
			if len(store.List()) != 0 {
				t.Error("generating a link created a routing binding")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Error("credential response must not be cached")
			}
		})
	}
}

func TestProxySessionLinkValidation(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	for _, id := range []string{"has.dot", "has space", "../node", "中文", "one@two", strings.Repeat("a", 101)} {
		t.Run(id, func(t *testing.T) {
			body, _ := json.Marshal(map[string]string{"session_id": id})
			w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", string(body))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	for _, body := range []string{`{"scheme":"ftp"}`, `{"scheme":"HTTP"}`, `{"session_id":123}`, `{`, `null`, `[]`, `{} {}`, `{} garbage`, `{}` + strings.Repeat(" ", 5000)} {
		w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body=%q status=%d", body[:min(len(body), 80)], w.Code)
		}
	}
	w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{"session_id":"`+strings.Repeat("a", 100)+`"}`)
	if w.Code != http.StatusOK {
		t.Errorf("valid 100-byte ID: status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestProxySessionLinksAvailability(t *testing.T) {
	for _, mode := range []string{"pool", "hybrid", "multi-port"} {
		for _, disabled := range []bool{false, true} {
			for _, username := range []string{"wang", ""} {
				cfg := proxySessionTestConfig()
				cfg.Mode, cfg.Listener.Username = mode, username
				enabled := !disabled
				cfg.Sticky.SessionEnabled = &enabled
				s, _ := newProxySessionTestServer(t, cfg)
				w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{"session_id":"one"}`)
				wantStatus := http.StatusOK
				if mode == "multi-port" || disabled || username == "" {
					wantStatus = http.StatusConflict
				}
				if w.Code != wantStatus {
					t.Errorf("mode=%s disabled=%v username=%q status=%d, want=%d", mode, disabled, username, w.Code, wantStatus)
				}
				ordinary := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{}`)
				if mode != "multi-port" && ordinary.Code != http.StatusOK {
					t.Errorf("ordinary proxy should still work: %d %s", ordinary.Code, ordinary.Body.String())
				}
			}
		}
	}
	// Hybrid links use pool credentials, not the separate per-node account.
	cfg := proxySessionTestConfig()
	cfg.Mode = "hybrid"
	cfg.MultiPort.Username, cfg.MultiPort.Password = "node-user", "node-password"
	s, _ := newProxySessionTestServer(t, cfg)
	w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{"session_id":"one"}`)
	if response := decodeProxyResponse(t, w); response["username"] != "wang.one" {
		t.Errorf("used wrong hybrid credentials: %v", response)
	}
}

func TestProxySessionsListAndReset(t *testing.T) {
	cfg := proxySessionTestConfig()
	cfg.Sticky.SessionTTL, cfg.Sticky.MaxSessions = time.Hour, 100
	s, store := newProxySessionTestServer(t, cfg)
	bindProxyTestSession(t, store, "wang.second")
	bindProxyTestSession(t, store, "wang.first")
	before := store.List()
	w := proxyAPIRequest(t, s, http.MethodGet, "/api/proxy-sessions", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	response := decodeProxyResponse(t, w)
	if response["enabled"] != true || response["total"] != float64(2) || response["ttl"] != "1h0m0s" || response["max_sessions"] != float64(100) {
		t.Fatalf("bad metadata: %v", response)
	}
	if strings.Contains(w.Body.String(), "1234") || strings.Contains(w.Body.String(), "password") {
		t.Errorf("list disclosed a password: %s", w.Body.String())
	}
	sessions := response["sessions"].([]any)
	first := sessions[0].(map[string]any)
	if first["id"] != "wang.first" || first["node_id"] != "opaque-id" || first["node_name"] != "Display node" {
		t.Errorf("bad binding metadata: %v", first)
	}
	if _, err := time.Parse(time.RFC3339Nano, first["last_seen"].(string)); err != nil {
		t.Errorf("last_seen timestamp: %v", err)
	}
	if !reflect.DeepEqual(before, store.List()) {
		t.Error("list API refreshed idle TTL")
	}
	w = proxyAPIRequest(t, s, http.MethodDelete, "/api/proxy-sessions/wang.first", "")
	if w.Code != http.StatusOK || len(store.List()) != 1 {
		t.Errorf("single reset failed: %d %s", w.Code, w.Body.String())
	}
	w = proxyAPIRequest(t, s, http.MethodDelete, "/api/proxy-sessions/wang.missing", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("missing reset status=%d", w.Code)
	}
	w = proxyAPIRequest(t, s, http.MethodDelete, "/api/proxy-sessions", "")
	if w.Code != http.StatusOK || len(store.List()) != 0 || decodeProxyResponse(t, w)["cleared"] != float64(1) {
		t.Errorf("clear failed: %d %s", w.Code, w.Body.String())
	}
	list := decodeProxyResponse(t, proxyAPIRequest(t, s, http.MethodGet, "/api/proxy-sessions", ""))
	if sessions, ok := list["sessions"].([]any); !ok || len(sessions) != 0 {
		t.Errorf("empty list must be []: %v", list)
	}
}

func TestProxySessionResetEscapedAccount(t *testing.T) {
	s, store := newProxySessionTestServer(t, proxySessionTestConfig())
	id := "wang/name+one.one"
	bindProxyTestSession(t, store, id)
	w := proxyAPIRequest(t, s, http.MethodDelete, "/api/proxy-sessions/"+url.PathEscape(id), "")
	if w.Code != http.StatusOK || len(store.List()) != 0 {
		t.Fatalf("escaped session reset: %d %s", w.Code, w.Body.String())
	}
}

func TestProxySessionsRequireAuthentication(t *testing.T) {
	s, store := newProxySessionTestServer(t, proxySessionTestConfig())
	s.cfg.Password = "management-password"
	bindProxyTestSession(t, store, "wang.one")
	for _, endpoint := range []struct{ method, path string }{
		{http.MethodGet, "/api/proxy-sessions"},
		{http.MethodDelete, "/api/proxy-sessions"},
		{http.MethodDelete, "/api/proxy-sessions/wang.one"},
		{http.MethodPost, "/api/proxy-sessions/links"},
	} {
		w := proxyAPIRequest(t, s, endpoint.method, endpoint.path, `{"session_id":"one"}`)
		if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "1234") {
			t.Errorf("unauthenticated %s %s: %d %s", endpoint.method, endpoint.path, w.Code, w.Body.String())
		}
	}
	if len(store.List()) != 1 {
		t.Error("unauthenticated reset changed the store")
	}
	s.sessionMu.Lock()
	s.sessions["valid-token"] = &Session{ExpiresAt: time.Now().Add(time.Hour)}
	s.sessionMu.Unlock()
	r := httptest.NewRequest(http.MethodPost, "/api/proxy-sessions/links", strings.NewReader(`{"session_id":"one"}`))
	r.Header.Set("Authorization", "Bearer valid-token")
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("authenticated link generation: %d %s", w.Code, w.Body.String())
	}
}

func TestProxySessionAPIMethodsAndUninitializedState(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	for _, endpoint := range []struct{ method, path string }{
		{http.MethodPost, "/api/proxy-sessions"},
		{http.MethodGet, "/api/proxy-sessions/links"},
		{http.MethodGet, "/api/proxy-sessions/wang.one"},
	} {
		w := proxyAPIRequest(t, s, endpoint.method, endpoint.path, `{}`)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
			t.Errorf("method check: %d %s", w.Code, w.Body.String())
		}
	}
	s.SetRoutingStore(nil)
	link := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{"session_id":"one"}`)
	if link.Code != http.StatusServiceUnavailable {
		t.Errorf("uninitialized named routing: %d %s", link.Code, link.Body.String())
	}
	for _, path := range []string{"/api/proxy-sessions", "/api/proxy-sessions/wang.one"} {
		w := proxyAPIRequest(t, s, http.MethodDelete, path, "")
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("uninitialized store: %d %s", w.Code, w.Body.String())
		}
	}
	response := decodeProxyResponse(t, proxyAPIRequest(t, s, http.MethodGet, "/api/proxy-sessions", ""))
	if response["enabled"] != false || response["reason"] == "" {
		t.Errorf("missing disabled reason: %v", response)
	}
	s.SetConfig(nil)
	w := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", `{}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("uninitialized config: %d %s", w.Code, w.Body.String())
	}
}

func loadProxySettingsFixture(t *testing.T) (*config.Config, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := `mode: pool
external_ip: proxy.example
skip_cert_verify: true
listener:
  address: 127.0.0.1
  port: 2323
  username: wang
  password: "1234"
management:
  probe_target: www.example.com:443
sticky:
  enabled: true
  port: 2324
  session_enabled: false
  session_ttl: 2h
  max_sessions: 123
geoip:
  enabled: true
  database_path: geo.mmdb
nodes:
  - name: fixture
    uri: vless://00000000-0000-0000-0000-000000000000@127.0.0.1:443?security=tls
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, path
}

func TestSettingsSessionDefaultsAndPersistence(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	response := decodeProxyResponse(t, proxyAPIRequest(t, s, http.MethodGet, "/api/settings", ""))
	sticky := response["sticky"].(map[string]any)
	if sticky["session_enabled"] != true || sticky["session_ttl"] != "24h0m0s" || sticky["max_sessions"] != float64(10000) {
		t.Errorf("bad default sticky settings: %v", sticky)
	}
	cfg, path := loadProxySettingsFixture(t)
	s.SetConfig(cfg)
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"sticky":{"session_enabled":true,"session_ttl":"30m","max_sessions":50}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("save: %d %s", w.Code, w.Body.String())
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Sticky.SessionRoutingEnabled() || loaded.Sticky.SessionTTL != 30*time.Minute || loaded.Sticky.MaxSessions != 50 {
		t.Errorf("session settings not persisted: %#v", loaded.Sticky)
	}
	if !loaded.Sticky.Enabled || loaded.Sticky.Port != 2324 || loaded.ExternalIP != "proxy.example" || !loaded.SkipCertVerify || !loaded.GeoIP.Enabled {
		t.Errorf("partial update changed unrelated settings: %#v", loaded)
	}
	// An old client only knows enabled/port and must preserve newer settings.
	w = proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"external_ip":"new.example","sticky":{"enabled":false,"port":2325}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("old client save: %d %s", w.Code, w.Body.String())
	}
	loaded, err = config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Sticky.SessionRoutingEnabled() || loaded.Sticky.SessionTTL != 30*time.Minute || loaded.Sticky.MaxSessions != 50 || loaded.Sticky.Enabled || loaded.Sticky.Port != 2325 {
		t.Errorf("old client reset new fields: %#v", loaded.Sticky)
	}
}

func TestSettingsRejectsInvalidSessionSettingsWithoutMutation(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	before, _ := os.ReadFile(path)
	original := *cfg
	for _, sticky := range []string{
		`{"session_ttl":"garbage"}`, `{"session_ttl":""}`, `{"session_ttl":"0s"}`, `{"session_ttl":"-1h"}`, `{"session_ttl":"8761h"}`,
		`{"max_sessions":0}`, `{"max_sessions":-1}`, `{"max_sessions":100001}`, `{"max_sessions":1.5}`, `{"session_enabled":"yes"}`,
	} {
		w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"external_ip":"changed.example","sticky":`+sticky+`}`)
		if w.Code != http.StatusBadRequest {
			t.Errorf("sticky=%s status=%d body=%s", sticky, w.Code, w.Body.String())
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(before) || !reflect.DeepEqual(*cfg, original) {
			t.Fatalf("invalid settings mutated config: %s", sticky)
		}
	}
}

func TestSettingsSaveFailureDoesNotPublish(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	original := *cfg
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"external_ip":"changed.example","sticky":{"session_enabled":true,"session_ttl":"30m","max_sessions":50}}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !reflect.DeepEqual(*cfg, original) || s.cfg.ExternalIP != original.ExternalIP {
		t.Error("failed save published staged settings")
	}
}

func TestSettingsRejectsMalformedJSONWithoutMutation(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	before, _ := os.ReadFile(path)
	original := *cfg
	for _, body := range []string{`null`, `[]`, `{} {}`, `{} garbage`, `{} ` + strings.Repeat(" ", 1<<20)} {
		w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body=%q status=%d", body[:min(len(body), 80)], w.Code)
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(before) || !reflect.DeepEqual(*cfg, original) {
			t.Fatal("malformed JSON mutated settings")
		}
	}
}

func TestSettingsSessionLimitsApplyToStore(t *testing.T) {
	cfg, _ := loadProxySettingsFixture(t)
	s, store := newProxySessionTestServer(t, cfg)
	bindProxyTestSession(t, store, "wang.one")
	bindProxyTestSession(t, store, "wang.two")
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"sticky":{"session_ttl":"30m","max_sessions":1}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("save: %d %s", w.Code, w.Body.String())
	}
	bindings := store.List()
	if len(bindings) != 1 || bindings[0].ExpiresAt.Sub(bindings[0].LastSeen) != 30*time.Minute {
		t.Errorf("store limits were not configured: %#v", bindings)
	}
	// Explicit false and the upper bounds remain valid and survive persistence.
	w = proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"sticky":{"session_enabled":false,"session_ttl":"8760h","max_sessions":100000}}`)
	if w.Code != http.StatusOK || cfg.Sticky.SessionRoutingEnabled() || cfg.Sticky.SessionTTL != config.MaxSessionTTL || cfg.Sticky.MaxSessions != config.MaxSessionCapacity {
		t.Errorf("explicit false or upper bound rejected: %d %s %#v", w.Code, w.Body.String(), cfg.Sticky)
	}
}

func TestProxySessionSOCKS5CredentialLength(t *testing.T) {
	for _, tc := range []struct {
		username, password, session string
		wantStatus                  int
	}{
		{strings.Repeat("a", 190), "secret", strings.Repeat("b", 64), http.StatusOK},
		{strings.Repeat("a", 191), "secret", strings.Repeat("b", 64), http.StatusBadRequest},
		{"wang", strings.Repeat("密", 85), "one", http.StatusOK},
		{"wang", strings.Repeat("密", 86), "one", http.StatusBadRequest},
	} {
		cfg := proxySessionTestConfig()
		cfg.Listener.Username, cfg.Listener.Password = tc.username, tc.password
		s, _ := newProxySessionTestServer(t, cfg)
		body, _ := json.Marshal(map[string]string{"session_id": tc.session, "scheme": "socks5"})
		response := proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", string(body))
		if response.Code != tc.wantStatus {
			t.Errorf("username=%d password=%d suffix=%d bytes status=%d, want=%d", len(tc.username), len(tc.password), len(tc.session), response.Code, tc.wantStatus)
		}
		body, _ = json.Marshal(map[string]string{"session_id": tc.session, "scheme": "http"})
		response = proxyAPIRequest(t, s, http.MethodPost, "/api/proxy-sessions/links", string(body))
		if response.Code != http.StatusOK {
			t.Errorf("HTTP incorrectly inherited SOCKS5 credential bounds: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestSettingsDoesNotOverwriteConcurrentNodeAndSubscriptionUpdates(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	data, _ := os.ReadFile(path)
	var diskCfg config.Config
	if err := yaml.Unmarshal(data, &diskCfg); err != nil {
		t.Fatal(err)
	}
	diskCfg.Subscriptions = []string{"https://fixture.invalid/subscription"}
	diskCfg.SubscriptionRefresh.Interval = 3 * time.Hour
	diskCfg.NodesFile = "disk-nodes.txt"
	data, _ = yaml.Marshal(diskCfg)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	// A subscription updater has a separate lock from monitor.cfgMu. Saving
	// settings must neither read nor overwrite its live-owned fields.
	const updates = 10000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; index < updates; index++ {
			cfg.Nodes = append(cfg.Nodes, config.NodeConfig{Name: "node-" + strconv.Itoa(index)})
			cfg.Subscriptions = append(cfg.Subscriptions, "subscription-"+strconv.Itoa(index))
			cfg.NodesFile = "live-nodes-" + strconv.Itoa(index)
			cfg.SubscriptionRefresh.Interval = time.Duration(index+1) * time.Minute
			if index%10 == 0 {
				runtime.Gosched()
			}
		}
	}()
	for index := 0; index < 20; index++ {
		response := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"sticky":{"session_ttl":"30m","max_sessions":50}}`)
		if response.Code != http.StatusOK {
			t.Errorf("settings save status=%d body=%s", response.Code, response.Body.String())
		}
		_ = proxyAPIRequest(t, s, http.MethodGet, "/api/settings", "")
		_ = proxyAPIRequest(t, s, http.MethodGet, "/api/proxy-sessions", "")
	}
	<-done
	if len(cfg.Nodes) != updates+1 || len(cfg.Subscriptions) != updates || cfg.NodesFile != "live-nodes-9999" || cfg.SubscriptionRefresh.Interval != updates*time.Minute {
		t.Fatalf("settings overwrote live node/subscription updates: nodes=%d subscriptions=%d file=%q interval=%s", len(cfg.Nodes), len(cfg.Subscriptions), cfg.NodesFile, cfg.SubscriptionRefresh.Interval)
	}
	data, _ = os.ReadFile(path)
	var after config.Config
	if err := yaml.Unmarshal(data, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Nodes, diskCfg.Nodes) || !reflect.DeepEqual(after.Subscriptions, diskCfg.Subscriptions) || after.NodesFile != diskCfg.NodesFile || after.SubscriptionRefresh.Interval != 3*time.Hour {
		t.Fatal("management settings save overwrote disk node/subscription settings")
	}
}

type testSettingsLocker struct {
	NodeManager
	mu     sync.Mutex
	locked chan struct{}
}

func (locker *testSettingsLocker) LockSettings() func() {
	locker.mu.Lock()
	close(locker.locked)
	return locker.mu.Unlock
}

func TestSettingsUsesManagerLock(t *testing.T) {
	cfg, _ := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	locker := &testSettingsLocker{locked: make(chan struct{})}
	s.SetNodeManager(locker)
	response := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"sticky":{"session_enabled":true}}`)
	if response.Code != http.StatusOK {
		t.Fatalf("settings save status=%d body=%s", response.Code, response.Body.String())
	}
	select {
	case <-locker.locked:
	default:
		t.Fatal("settings save did not acquire the manager lock")
	}
	if !locker.mu.TryLock() {
		t.Fatal("settings handler did not release the manager lock")
	}
	locker.mu.Unlock()
}

func TestMonitorShutdownClosesLongLivedHandlers(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	defer server.Close()
	result := make(chan error, 1)
	go func() {
		response, err := server.Client().Get(server.URL)
		if response != nil {
			response.Body.Close()
		}
		result <- err
	}()
	<-entered
	s := &Server{srv: server.Config}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s.Shutdown(ctx)
	select {
	case err := <-result:
		if err == nil {
			t.Error("active streaming connection was not force-closed")
		}
	case <-time.After(time.Second):
		t.Error("shutdown did not close the active streaming connection")
	}
	close(release)
}

func TestSettingsBlacklistFailedPinRoundTrip(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	response := decodeProxyResponse(t, proxyAPIRequest(t, s, http.MethodGet, "/api/settings", ""))
	if got := response["pool"].(map[string]any)["blacklist_failed_pin"]; got != true {
		t.Fatalf("default blacklist_failed_pin = %v, want true", got)
	}
	cfg, path := loadProxySettingsFixture(t)
	s.SetConfig(cfg)
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"pool":{"mode":"balance","failure_threshold":3,"blacklist_failed_pin":false}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("save: %d %s", w.Code, w.Body.String())
	}
	if cfg.Pool.BlacklistFailedPinOrDefault() {
		t.Fatal("runtime settings kept the immediate pin blacklist enabled")
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Pool.BlacklistFailedPinOrDefault() {
		t.Fatal("blacklist_failed_pin was not persisted")
	}
	// Omitting the field keeps the stored value; an explicit true re-enables it.
	if w = proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"external_ip":"other.example"}`); w.Code != http.StatusOK {
		t.Fatalf("partial save: %d %s", w.Code, w.Body.String())
	}
	loaded, err = config.Load(path)
	if err != nil || loaded.Pool.BlacklistFailedPinOrDefault() {
		t.Fatalf("partial save reset blacklist_failed_pin: %+v, %v", loaded.Pool, err)
	}
	if w = proxyAPIRequest(t, s, http.MethodPut, "/api/settings", `{"pool":{"mode":"balance","failure_threshold":3,"blacklist_failed_pin":true}}`); w.Code != http.StatusOK {
		t.Fatalf("re-enable save: %d %s", w.Code, w.Body.String())
	}
	loaded, err = config.Load(path)
	if err != nil || !loaded.Pool.BlacklistFailedPinOrDefault() {
		t.Fatalf("blacklist_failed_pin was not re-enabled: %+v, %v", loaded.Pool, err)
	}
}
