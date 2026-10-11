package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"gopkg.in/yaml.v3"
)

type monitorRoundTripper func(*http.Request) (*http.Response, error)

func (fn monitorRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

type fragmentedReader struct{ chunks []string }

func (reader *fragmentedReader) Read(p []byte) (int, error) {
	if len(reader.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, reader.chunks[0])
	reader.chunks[0] = reader.chunks[0][n:]
	if reader.chunks[0] == "" {
		reader.chunks = reader.chunks[1:]
	}
	return n, nil
}

func TestTrafficFramesCompleteNDJSONAcrossNetworkChunks(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	s.trafficClient = &http.Client{Transport: monitorRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Host != "127.0.0.1:9092" {
			t.Errorf("unexpected traffic request: %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(&fragmentedReader{chunks: []string{`{"up":`, "12,\"down\":34}\n{\"up\":56", ",\"down\":78}\n\nnot-json\n", `{"up":0,"down":0}`}})}, nil
	})}
	w := proxyAPIRequest(t, s, http.MethodGet, "/api/traffic", "")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("traffic response: %d %s", w.Code, w.Body)
	}
	want := "data: {\"up\":12,\"down\":34}\n\ndata: {\"up\":56,\"down\":78}\n\ndata: {\"up\":0,\"down\":0}\n\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("SSE frames=%q want=%q", got, want)
	}
}

func TestTrafficRejectsUpstreamErrorAndInvalidMethods(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	var calls atomic.Int32
	s.trafficClient = &http.Client{Transport: monitorRoundTripper(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
	})}
	w := proxyAPIRequest(t, s, http.MethodGet, "/api/traffic", "")
	if w.Code != 502 || !strings.Contains(w.Body.String(), `"upstream_status":503`) {
		t.Fatalf("upstream status not propagated: %d %s", w.Code, w.Body)
	}
	w = proxyAPIRequest(t, s, http.MethodPost, "/api/traffic", "")
	if w.Code != 405 || calls.Load() != 1 {
		t.Fatalf("invalid method dialed upstream: %d calls=%d", w.Code, calls.Load())
	}
	transport := NewServer(Config{Enabled: true}, s.mgr, nil).trafficClient.Transport.(*http.Transport)
	if transport.ResponseHeaderTimeout == 0 {
		t.Fatal("missing upstream response-header timeout")
	}
	transport.CloseIdleConnections()
}

func TestTrafficCancellationClosesUpstreamWhileReadIsBlocked(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer upstream.Close()
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	s.trafficClient = &http.Client{Transport: monitorRoundTripper(func(r *http.Request) (*http.Response, error) {
		rewritten := r.Clone(r.Context())
		rewritten.URL.Scheme = "http"
		rewritten.URL.Host = strings.TrimPrefix(upstream.URL, "http://")
		return transport.RoundTrip(rewritten)
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/api/traffic", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { s.handleTraffic(httptest.NewRecorder(), r); close(done) }()
	<-started
	cancel()
	for _, ch := range []chan struct{}{done, canceled} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("traffic cancellation retained blocked read/upstream connection")
		}
	}
}

func TestSubscriptionConfigWithoutRefresherPersistsWithoutPanic(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/subscription/config", `{"subscriptions":[" https://fixture.invalid/sub "],"enabled":false,"interval":"2h"}`)
	if w.Code != 200 {
		t.Fatalf("save without refresher: %d %s", w.Code, w.Body)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "https://fixture.invalid/sub" || cfg.SubscriptionRefresh.Interval != 2*time.Hour {
		t.Fatalf("subscription settings not published: %+v", cfg.SubscriptionRefresh)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "https://fixture.invalid/sub") {
		t.Fatal("subscription settings not persisted")
	}
}

func TestSubscriptionConfigSaveFailureDoesNotPublish(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	cfg.Subscriptions = []string{"https://old.invalid/sub"}
	cfg.SubscriptionRefresh.Interval = 3 * time.Hour
	s, _ := newProxySessionTestServer(t, cfg)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	w := proxyAPIRequest(t, s, http.MethodPut, "/api/subscription/config", `{"subscriptions":["https://new.invalid/sub"],"enabled":true,"interval":"2h"}`)
	if w.Code != 500 || len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "https://old.invalid/sub" || cfg.SubscriptionRefresh.Interval != 3*time.Hour {
		t.Fatalf("failed save changed subscription settings: %d %+v", w.Code, cfg.SubscriptionRefresh)
	}
}

func TestPasswordRotationRevokesSessionsAndLogoutInvalidatesToken(t *testing.T) {
	cfg, _ := loadProxySettingsFixture(t)
	cfg.Management.Password = "old-password"
	s, _ := newProxySessionTestServer(t, cfg)
	session, _ := s.createSession()
	r := httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(fmt.Sprintf(`{"management":{"listen":%q,"password":"new-password","probe_concurrency":32}}`, cfg.Management.Listen)))
	r.Header.Set("Authorization", "Bearer "+session.Token)
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"auth_changed":true`) {
		t.Fatalf("password rotation: %d %s", w.Code, w.Body)
	}
	if s.validateSession(session.Token) {
		t.Fatal("old login session survived password rotation")
	}
	r = httptest.NewRequest(http.MethodGet, "/api/nodes", nil)
	r.Header.Set("Authorization", "Bearer "+session.Token)
	w = httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("revoked token authorized: %d", w.Code)
	}
	w = proxyAPIRequest(t, s, http.MethodPost, "/api/auth", `{"password":"new-password"}`)
	if w.Code != 200 {
		t.Fatalf("new password not immediately live: %d %s", w.Code, w.Body)
	}
	var auth struct {
		Token string `json:"token"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &auth)
	if !s.validateSession(auth.Token) {
		t.Fatal("new login token invalid")
	}
	r = httptest.NewRequest(http.MethodDelete, "/api/auth", nil)
	r.Header.Set("Authorization", "Bearer "+auth.Token)
	w = httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	if w.Code != 200 || s.validateSession(auth.Token) {
		t.Fatal("logout did not invalidate login token")
	}
	cookies := w.Result().Cookies()
	if len(cookies) == 0 || cookies[0].MaxAge != -1 {
		t.Fatal("logout did not expire browser cookie")
	}
}

func TestAuthStatusCookieSecurityAndBoundedJSON(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	s.cfgMu.Lock()
	s.setPasswordLocked("password")
	s.cfgMu.Unlock()
	w := proxyAPIRequest(t, s, http.MethodGet, "/api/auth", "")
	if !strings.Contains(w.Body.String(), `"authenticated":false`) || !strings.Contains(w.Body.String(), `"no_password":false`) {
		t.Fatalf("unexpected auth status: %s", w.Body)
	}
	for _, body := range []string{`{"password":"password"} {}`, `null`, `[]`, `{"password":"password","padding":"` + strings.Repeat("x", 5000) + `"}`} {
		w = proxyAPIRequest(t, s, http.MethodPost, "/api/auth", body)
		if w.Code != 400 {
			t.Errorf("malformed/oversized auth body accepted: %d", w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "https://dashboard.example/api/auth", strings.NewReader(`{"password":"password"}`))
	w = httptest.NewRecorder()
	s.handleAuth(w, r)
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("HTTPS login cookie missing security attributes")
	}
}

func TestProxyAuthorizationPreservesCookieAndBearerPrecedence(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	s.cfgMu.Lock()
	s.setPasswordLocked("fixture-password")
	s.cfgMu.Unlock()
	session, err := s.createSession()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		header string
		cookie string
		status int
	}{
		{"cookie", "", session.Token, http.StatusOK},
		{"proxy basic and cookie", "Basic Zml4dHVyZTpwYXNzd29yZA==", session.Token, http.StatusOK},
		{"proxy digest and cookie", `Digest username="fixture"`, session.Token, http.StatusOK},
		{"basic alone is not a login", "Basic Zml4dHVyZTpwYXNzd29yZA==", "", http.StatusUnauthorized},
		{"valid bearer overrides cookie", "Bearer " + session.Token, "invalid", http.StatusOK},
		{"case insensitive bearer", "bearer " + session.Token, "", http.StatusOK},
		{"invalid bearer overrides cookie", "Bearer invalid", session.Token, http.StatusUnauthorized},
		{"malformed bearer overrides cookie", "Bearer", session.Token, http.StatusUnauthorized},
		{"empty bearer overrides cookie", "Bearer ", session.Token, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/nodes", nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: "session_token", Value: tc.cookie})
			}
			w := httptest.NewRecorder()
			s.srv.Handler.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.status, w.Body)
			}
		})
	}
}

func TestCrossOriginMutationsRejectedWithoutPassword(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	for _, path := range []string{"/api/proxy-sessions", "/api/auth"} {
		r := httptest.NewRequest(http.MethodDelete, "http://dashboard.example"+path, nil)
		r.Header.Set("Origin", "https://unrelated.example")
		w := httptest.NewRecorder()
		s.srv.Handler.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Errorf("cross-origin mutation accepted on %s: %d", path, w.Code)
		}
	}
}

func TestBatchProbeUsesBoundedWorkersAndCancellation(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	s.cfgSrc.Management.ProbeConcurrency = 8
	started := make(chan struct{})
	var calls atomic.Int32
	for i := range 2000 {
		h := s.mgr.Register(NodeInfo{Tag: fmt.Sprint(i)})
		h.SetProbe(func(ctx context.Context) (time.Duration, error) {
			if calls.Add(1) == 8 {
				close(started)
			}
			<-ctx.Done()
			return 0, ctx.Err()
		})
	}
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest(http.MethodPost, "/api/nodes/probe-all", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { s.handleProbeAll(httptest.NewRecorder(), r); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch workers did not start")
	}
	if delta := runtime.NumGoroutine() - baseline; delta > 64 {
		t.Errorf("2000-node batch created %d goroutines, want bounded by concurrency", delta)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch cancellation did not release workers")
	}
	if s.probeAllInFlight.Load() {
		t.Fatal("batch single-flight gate stuck after cancellation")
	}
}

func TestMonitorLifecycleCancellationAndStartIdempotence(t *testing.T) {
	s, _ := newProxySessionTestServer(t, proxySessionTestConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	s.Start(ctx)
	s.Shutdown(context.Background())
	if !errors.Is(s.ctx.Err(), context.Canceled) {
		t.Fatal("monitor shutdown did not cancel background cleanup/handlers")
	}
}

func TestSettingsRejectInvalidCoreFieldsBeforeSaving(t *testing.T) {
	cases := []string{
		`{"mode":"unknown"}`, `{"pool":{"mode":"unknown"}}`,
		`{"pool":{"failure_threshold":-1}}`, `{"pool":{"blacklist_duration":"bad"}}`,
		`{"pool":{"transient_cooldown":"-1s"}}`, `{"pool":{"rate_limit_cooldown":"0s"}}`,
		`{"management":{"listen":"bad-address"}}`, `{"management":{"probe_concurrency":-1}}`,
		`{"management":{"probe_concurrency":99999}}`, `{"listener":{"address":"https://example.com","port":2323}}`,
		`{"log":{"max_size":-1}}`, `{"geoip":{"auto_update_interval":"invalid"}}`,
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			cfg, path := loadProxySettingsFixture(t)
			s, _ := newProxySessionTestServer(t, cfg)
			before, _ := os.ReadFile(path)
			original := cfg.SettingsSnapshot()
			w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", body)
			if w.Code != 400 {
				t.Fatalf("invalid settings status=%d: %s", w.Code, w.Body)
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(before) || cfg.Mode != original.Mode || cfg.Management != original.Management || cfg.Listener != original.Listener || cfg.Pool != original.Pool {
				t.Fatal("rejected settings modified file or runtime state")
			}
		})
	}
}

func TestSubscriptionConfigRejectsInvalidInputWithoutMutation(t *testing.T) {
	cfg, path := loadProxySettingsFixture(t)
	s, _ := newProxySessionTestServer(t, cfg)
	before, _ := os.ReadFile(path)
	for _, body := range []string{
		`{"subscriptions":["file:///tmp/secret"],"interval":"1h"}`,
		`{"subscriptions":["https://"],"interval":"1h"}`,
		`{"subscriptions":[],"interval":"1s"}`,
		`{"subscriptions":[],"interval":"bad"}`,
		`{"subscriptions":[],"interval":"1h"} {}`,
	} {
		w := proxyAPIRequest(t, s, http.MethodPut, "/api/subscription/config", body)
		if w.Code != 400 {
			t.Errorf("invalid subscription payload accepted: %d %s", w.Code, w.Body)
		}
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(before) || len(cfg.Subscriptions) != 0 {
		t.Fatal("invalid subscription payload modified settings")
	}
}

func TestSetConfigRespectsExplicitlyClearedSubscriptionsWithoutMutatingCaller(t *testing.T) {
	for _, subscriptions := range [][]string{nil, {}} {
		for _, refresh := range []config.SubscriptionRefreshConfig{{}, {Timeout: 17 * time.Second}} {
			oldCfg := proxySessionTestConfig()
			oldCfg.Subscriptions = []string{"https://previous.invalid/sub"}
			oldCfg.SubscriptionRefresh = config.SubscriptionRefreshConfig{
				Enabled: true, Interval: 2 * time.Hour, Timeout: time.Minute,
			}
			s, _ := newProxySessionTestServer(t, oldCfg)
			next := proxySessionTestConfig()
			next.Subscriptions, next.SubscriptionRefresh = subscriptions, refresh
			before, previous := *next, *oldCfg
			s.SetConfig(next)
			if s.cfgSrc != next || !reflect.DeepEqual(*next, before) || !reflect.DeepEqual(*oldCfg, previous) {
				t.Fatal("SetConfig mutated the new/previous caller configuration")
			}
			if len(s.cfgSrc.Subscriptions) != 0 || s.cfgSrc.SubscriptionRefresh != refresh {
				t.Fatal("SetConfig revived old subscription URLs or refresh settings")
			}
			w := proxyAPIRequest(t, s, http.MethodGet, "/api/subscription/config", "")
			var response struct {
				Subscriptions []string `json:"subscriptions"`
				Enabled       bool     `json:"enabled"`
				Interval      string   `json:"interval"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != 200 || len(response.Subscriptions) != 0 || response.Enabled || response.Interval != "0s" {
				t.Fatalf("cleared config not exposed by management API: %d %s", w.Code, w.Body)
			}
		}
	}
}

func TestSettingsDisabledGeoIPZeroIntervalRoundTrip(t *testing.T) {
	cases := []struct {
		name, interval          string
		geoEnabled, autoEnabled bool
		status                  int
	}{
		{"disabled-zero", "0s", false, false, 200},
		{"enabled-manual-zero", "0s", true, false, 200},
		{"auto-zero", "0s", true, true, 400},
		{"disabled-negative", "-1h", false, false, 400},
		{"disabled-malformed", "bad", false, false, 400},
		{"auto-default-omitted", "", true, true, 200},
		{"auto-positive", "12h", true, true, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, path := loadProxySettingsFixture(t)
			cfg.GeoIP = config.GeoIPConfig{}
			s, _ := newProxySessionTestServer(t, cfg)
			before, _ := os.ReadFile(path)
			body := fmt.Sprintf(`{"external_ip":"changed.example","geoip":{"enabled":%t,"auto_update_enabled":%t,"auto_update_interval":%q}}`, tc.geoEnabled, tc.autoEnabled, tc.interval)
			w := proxyAPIRequest(t, s, http.MethodPut, "/api/settings", body)
			if w.Code != tc.status {
				t.Fatalf("settings save=%d want=%d: %s", w.Code, tc.status, w.Body)
			}
			if tc.status == 400 {
				after, _ := os.ReadFile(path)
				if string(before) != string(after) || cfg.ExternalIP != "proxy.example" || cfg.GeoIP != (config.GeoIPConfig{}) {
					t.Fatal("invalid GeoIP interval altered runtime or persisted settings")
				}
				return
			}
			wantInterval := 24 * time.Hour
			if tc.interval == "12h" {
				wantInterval = 12 * time.Hour
			}
			if cfg.GeoIP.AutoUpdateInterval != wantInterval || cfg.GeoIP.Enabled != tc.geoEnabled || cfg.GeoIP.AutoUpdateEnabled != tc.autoEnabled || cfg.ExternalIP != "changed.example" {
				t.Fatalf("GeoIP default interval unexpectedly toggled feature or blocked unrelated change: %+v", cfg.GeoIP)
			}
			if !tc.autoEnabled && cfg.GeoIP.AutoUpdateEnabled {
				t.Fatal("default interval enabled automatic downloads")
			}
			var disk config.Config
			data, _ := os.ReadFile(path)
			if err := yaml.Unmarshal(data, &disk); err != nil {
				t.Fatal(err)
			}
			if disk.GeoIP.AutoUpdateInterval != wantInterval || disk.GeoIP.AutoUpdateEnabled != tc.autoEnabled {
				t.Fatalf("GeoIP defaults not persisted: %+v", disk.GeoIP)
			}
		})
	}
}
