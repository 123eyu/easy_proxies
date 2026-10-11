package monitor

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"easy_proxies/internal/config"
)

func exportTestLinks(t *testing.T, s *Server, query, host string) ([]*url.URL, *httptest.ResponseRecorder) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/api/export"+query, nil)
	r.Host = host
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("export status=%d body=%s", w.Code, w.Body.String())
	}
	var links []*url.URL
	for _, line := range strings.Split(w.Body.String(), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		link, err := url.Parse(line)
		if err != nil {
			t.Fatalf("invalid exported link %q: %v", line, err)
		}
		links = append(links, link)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Error("credential export must not be cached")
	}
	return links, w
}

func TestExportLinksHybridCredentialsAndDeduplication(t *testing.T) {
	for _, scheme := range []string{"http", "socks5", "all"} {
		t.Run(scheme, func(t *testing.T) {
			cfg := proxySessionTestConfig()
			cfg.Mode = "hybrid"
			cfg.Listener.Username, cfg.Listener.Password = "pool/@名", "pool@:/#?密%"
			cfg.MultiPort.Username, cfg.MultiPort.Password = "node:@名", "node@:/#密%"
			cfg.GeoIP = config.GeoIPConfig{Enabled: true, Listen: "::", Port: 1221}
			s, _ := newProxySessionTestServer(t, cfg)
			for _, tag := range []string{"first", "duplicate"} {
				h := s.mgr.Register(NodeInfo{Tag: tag, Name: tag, ListenAddress: "0.0.0.0", Port: 24000})
				h.MarkInitialCheckDone(true)
			}
			// The persisted config is authoritative, not stale export defaults.
			s.cfg.ProxyUsername, s.cfg.ProxyPassword = "stale", "stale-secret"
			links, response := exportTestLinks(t, s, "?scheme="+scheme, "dashboard.example:9091")
			wantCount := 3
			if scheme == "all" {
				wantCount = 5
			}
			if len(links) != wantCount {
				t.Fatalf("exported %d links, want %d:\n%s", len(links), wantCount, response.Body.String())
			}
			seen := make(map[string]bool)
			for _, link := range links {
				if seen[link.String()] {
					t.Errorf("duplicate link: %s", link)
				}
				seen[link.String()] = true
				if link.Hostname() != "dashboard.example" {
					t.Errorf("exported wildcard or wrong host: %s", link)
				}
				wantUsername, wantPassword := cfg.Listener.Username, cfg.Listener.Password
				if link.Port() == "24000" {
					wantUsername, wantPassword = cfg.MultiPort.Username, cfg.MultiPort.Password
				}
				password, _ := link.User.Password()
				if link.User.Username() != wantUsername || password != wantPassword {
					t.Errorf("wrong credential round trip for port %s: %s", link.Port(), link)
				}
				if link.Port() == "1221" && link.Scheme != "http" {
					t.Errorf("GeoIP export must remain HTTP-only: %s", link)
				}
				if link.Port() != "1221" && scheme != "all" && link.Scheme != scheme {
					t.Errorf("wrong requested scheme: %s", link)
				}
			}
		})
	}
}

func TestExportLinkHostsAndIPv6(t *testing.T) {
	for _, tc := range []struct{ name, address, externalIP, host, wantHost string }{
		{"wildcard IPv4", "0.0.0.0", "", "proxy.example:9091", "proxy.example"},
		{"wildcard IPv6", "::", "", "[2001:db8::9]:9091", "2001:db8::9"},
		{"external IPv6", "0.0.0.0", "2001:db8::7", "internal.example:9091", "2001:db8::7"},
		{"external hostname", "127.0.0.1", "proxy.example", "internal.example:9091", "proxy.example"},
		{"concrete IPv6", "::1", "", "dashboard.example:9091", "::1"},
		{"all unspecified", "::", "0.0.0.0", "[::]:9091", "127.0.0.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := proxySessionTestConfig()
			cfg.Mode, cfg.ExternalIP = "hybrid", tc.externalIP
			cfg.Listener.Address = tc.address
			cfg.MultiPort = config.MultiPortConfig{Address: tc.address, Username: "node", Password: "pass"}
			cfg.GeoIP = config.GeoIPConfig{Enabled: true, Listen: tc.address, Port: 1221}
			s, _ := newProxySessionTestServer(t, cfg)
			s.mgr.Register(NodeInfo{Tag: "node", ListenAddress: tc.address, Port: 24000}).MarkInitialCheckDone(true)
			links, _ := exportTestLinks(t, s, "?scheme=all", tc.host)
			if len(links) != 5 {
				t.Fatalf("exported %d links, want 5", len(links))
			}
			for _, link := range links {
				if link.Hostname() != tc.wantHost {
					t.Errorf("host=%q, want %q: %s", link.Hostname(), tc.wantHost, link)
				}
				if _, err := strconv.ParseUint(link.Port(), 10, 16); err != nil {
					t.Errorf("invalid exported IPv6/port: %s", link)
				}
			}
		})
	}
}

func TestExportEmptyPasswordAndAnonymousCredentials(t *testing.T) {
	for _, username := range []string{"wang", ""} {
		t.Run("username="+username, func(t *testing.T) {
			cfg := proxySessionTestConfig()
			cfg.Mode = "hybrid"
			cfg.Listener.Username, cfg.Listener.Password = username, ""
			cfg.MultiPort.Username, cfg.MultiPort.Password = username, ""
			cfg.GeoIP = config.GeoIPConfig{Enabled: true, Port: 1221}
			s, _ := newProxySessionTestServer(t, cfg)
			s.mgr.Register(NodeInfo{Tag: "node", ListenAddress: "0.0.0.0", Port: 24000}).MarkInitialCheckDone(true)
			links, _ := exportTestLinks(t, s, "?scheme=all", "dashboard.example:9091")
			for _, link := range links {
				if username == "" {
					if link.User != nil {
						t.Errorf("anonymous URI includes userinfo: %s", link)
					}
					continue
				}
				if link.User == nil || link.User.Username() != username {
					t.Fatalf("empty password removed the username: %s", link)
				}
				password, present := link.User.Password()
				if password != "" || !present {
					t.Errorf("empty password did not round trip: %s", link)
				}
			}
		})
	}
}

func TestExportHealthFilteringAndFilenames(t *testing.T) {
	cfg := proxySessionTestConfig()
	cfg.Mode = "multi-port"
	s, _ := newProxySessionTestServer(t, cfg)
	for index, tag := range []string{"healthy", "unhealthy", "unchecked", "blacklisted"} {
		handle := s.mgr.Register(NodeInfo{Tag: tag, ListenAddress: "0.0.0.0", Port: uint16(24000 + index)})
		if tag != "unchecked" {
			handle.MarkInitialCheckDone(tag != "unhealthy")
		}
		if tag == "blacklisted" {
			handle.ref.mu.Lock()
			handle.ref.blacklist = true
			handle.ref.mu.Unlock()
		}
	}
	for _, tc := range []struct {
		query, filename string
		count           int
	}{
		{"", "proxy_pool.txt", 1},
		{"?scheme=socks5", "proxy_pool_socks5.txt", 1},
		{"?scheme=all", "proxy_pool_all.txt", 2},
		{"?all=true", "full_proxy_pool.txt", 4},
		{"?all=1&scheme=all", "full_proxy_pool_all.txt", 8},
	} {
		links, response := exportTestLinks(t, s, tc.query, "dashboard.example:9091")
		if len(links) != tc.count {
			t.Errorf("query=%s got %d links, want %d", tc.query, len(links), tc.count)
		}
		if !strings.Contains(response.Header().Get("Content-Disposition"), tc.filename) {
			t.Errorf("wrong export filename: %s", response.Header().Get("Content-Disposition"))
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		response := proxyAPIRequest(t, s, method, "/api/export", "")
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("invalid method=%s status=%d", method, response.Code)
		}
	}
	response := proxyAPIRequest(t, s, http.MethodGet, "/api/export?scheme=ftp", "")
	if response.Code != http.StatusBadRequest {
		t.Errorf("invalid scheme status=%d", response.Code)
	}
}

func TestPoolExportDoesNotDuplicateNodeEntry(t *testing.T) {
	cfg := proxySessionTestConfig()
	s, _ := newProxySessionTestServer(t, cfg)
	for _, tag := range []string{"one", "two"} {
		s.mgr.Register(NodeInfo{Tag: tag, ListenAddress: cfg.Listener.Address, Port: cfg.Listener.Port}).MarkInitialCheckDone(true)
	}
	links, response := exportTestLinks(t, s, "?scheme=all", "dashboard.example:9091")
	if len(links) != 2 || strings.Contains(response.Body.String(), "# Multi-port") {
		t.Errorf("pool entries duplicated: %s", response.Body.String())
	}
}
