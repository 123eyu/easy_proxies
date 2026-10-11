package config

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

func TestSubscriptionBase64Variants(t *testing.T) {
	content := "trojan://password@fixture.invalid:443#名字\nvmess://uuid@fixture.invalid:443?type=grpc#URL%20Node\n"
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		encoded := encoding.EncodeToString([]byte(content))
		encoded = encoded[:12] + "\r\n" + encoded[12:]
		nodes, err := ParseSubscriptionContent(encoded)
		if err != nil || len(nodes) != 2 {
			t.Fatalf("base64 variant lost nodes: count=%d err=%v", len(nodes), err)
		}
	}
	if name := ExtractNodeName("vmess://uuid@fixture.invalid:443#URL%20Node"); name != "URL Node" {
		t.Fatalf("URL VMess name=%q", name)
	}
}

func TestClashURIIPv6AndCredentials(t *testing.T) {
	for _, scheme := range []string{"vmess", "vless", "trojan", "anytls", "hysteria2", "tuic", "http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			proxy := clashProxy{Type: scheme, Server: "2001:db8::1", Port: 443, UUID: "uuid", Username: "user", Password: "p@ss:word/?#%+秘密", Name: "Name + 空格"}
			uri := convertClashProxyToURI(proxy)
			u, err := url.Parse(uri)
			if err != nil {
				t.Fatal(err)
			}
			if u.Hostname() != proxy.Server || u.Port() != "443" || u.Fragment != proxy.Name {
				t.Fatalf("URI data changed: %s", uri)
			}
			if name := ExtractNodeName(uri); name != proxy.Name {
				t.Fatalf("roundtrip node name changed: %q", name)
			}
			switch scheme {
			case "trojan", "anytls", "hysteria2":
				if u.User.Username() != proxy.Password {
					t.Fatal("password changed")
				}
			case "tuic", "http", "https":
				pass, _ := u.User.Password()
				if pass != proxy.Password {
					t.Fatal("password changed")
				}
			}
		})
	}
	uri := buildVMessURI(clashProxy{Server: "fixture.invalid", Port: 443, UUID: "uuid", AlterId: 64, Cipher: "aes-128-gcm"})
	u, _ := url.Parse(uri)
	if u.Query().Get("alterId") != "64" || u.Query().Get("encryption") != "aes-128-gcm" {
		t.Fatal("VMess authentication options lost")
	}
}

func TestValidateSettingsRejectsInvalidValues(t *testing.T) {
	for _, mutate := range []func(*Config){func(c *Config) { c.Mode = "invalid" }, func(c *Config) { c.Pool.Mode = "invalid" }, func(c *Config) { c.Listener.Address = "host.invalid" }, func(c *Config) { c.Management.Listen = "127.0.0.1:65536" }, func(c *Config) { c.Pool.BlacklistDuration = -1 }, func(c *Config) { c.Management.ProbeConcurrency = 9999 }} {
		cfg := &Config{Mode: "pool"}
		mutate(cfg)
		if cfg.ValidateManagementSettings() == nil {
			t.Fatal("invalid setting accepted")
		}
	}
	if mode, err := normalizePoolMode("round-robin"); err != nil || mode != "sequential" {
		t.Fatal("legacy round-robin alias not normalized")
	}
	if strings.Contains(buildTrojanURI(clashProxy{Password: "x#secret", Server: "fixture.invalid", Port: 443}), "x#secret") {
		t.Fatal("password must be encoded")
	}
}
