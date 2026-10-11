package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestSessionRoutingDefaultsAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		max     int
		wantErr bool
	}{
		{name: "defaults"},
		{name: "custom", ttl: time.Hour, max: 20},
		{name: "negative ttl", ttl: -time.Second, wantErr: true},
		{name: "excessive ttl", ttl: MaxSessionTTL + time.Second, wantErr: true},
		{name: "negative capacity", max: -1, wantErr: true},
		{name: "excessive capacity", max: MaxSessionCapacity + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Mode: "pool", Sticky: StickyConfig{SessionTTL: tc.ttl, MaxSessions: tc.max}}
			err := cfg.normalizeSticky()
			if (err != nil) != tc.wantErr {
				t.Fatalf("normalizeSticky error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if !cfg.Sticky.SessionRoutingEnabled() {
				t.Fatal("session routing should default to enabled")
			}
			if cfg.Sticky.SessionTTL <= 0 || cfg.Sticky.MaxSessions <= 0 {
				t.Fatal("defaults not applied")
			}
			disabled := false
			cfg.Sticky.SessionEnabled = &disabled
			if cfg.Sticky.SessionRoutingEnabled() {
				t.Fatal("explicit disable ignored")
			}
		})
	}
}

func TestSaveSettingsPersistsStickyRouting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("mode: pool\nnodes:\n  - name: keep\n    uri: http://127.0.0.1:8080\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		cfg := &Config{Mode: "pool", Sticky: StickyConfig{
			Enabled: true, Port: 2324, SessionEnabled: &enabled, SessionTTL: 2 * time.Hour, MaxSessions: 321,
		}}
		cfg.SetFilePath(path)
		if err := cfg.SaveSettings(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved Config
		if err := yaml.Unmarshal(data, &saved); err != nil {
			t.Fatal(err)
		}
		if saved.Sticky.SessionEnabled == nil || *saved.Sticky.SessionEnabled != enabled || saved.Sticky.SessionTTL != 2*time.Hour || saved.Sticky.MaxSessions != 321 || !saved.Sticky.Enabled || saved.Sticky.Port != 2324 {
			t.Fatalf("sticky settings not persisted: %+v", saved.Sticky)
		}
		if len(saved.Nodes) != 1 || saved.Nodes[0].Name != "keep" {
			t.Fatal("saving settings changed inline nodes")
		}
	}
}

func TestVMessURLIdentityIgnoresParameterOrder(t *testing.T) {
	a := NodeConfig{URI: "vmess://uuid@example.com:443?type=grpc&serviceName=service&security=tls#First"}
	b := NodeConfig{URI: "vmess://uuid@example.com:443?security=tls&serviceName=service&type=grpc#Renamed"}
	if a.NodeKey() != b.NodeKey() {
		t.Fatal("VMess URL identity changed with parameter order/name")
	}
}

func TestVMessJSONIdentityAndLegacyPortMigration(t *testing.T) {
	original := `{"v":"2","ps":"OldName","add":"example.com","port":"443","id":"11111111-1111-1111-1111-111111111111","net":"grpc","path":"service"}`
	renamed := `{"path":"service","net":"grpc","id":"11111111-1111-1111-1111-111111111111","port":"443","add":"example.com","ps":"NewName","v":"2"}`
	oldURI := "vmess://" + base64.StdEncoding.EncodeToString([]byte(original))
	key := stableNodeKey(oldURI)
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		newURI := "vmess://" + encoding.EncodeToString([]byte(renamed)) + "#also-renamed"
		if stableNodeKey(newURI) != key {
			t.Fatal("VMess JSON identity changed with display name, key order, or base64 encoding")
		}
		saved := map[string]uint16{oldURI: 24444}
		bridgeLegacyPortKeys([]NodeConfig{{URI: newURI}}, saved)
		if saved[key] != 24444 {
			t.Fatal("old sidecar lost port during identity upgrade")
		}
	}
	changed := "vmess://" + base64.StdEncoding.EncodeToString([]byte(`{"v":"2","ps":"NewName","add":"different.example.com","port":"443","id":"11111111-1111-1111-1111-111111111111","net":"grpc","path":"service"}`))
	if stableNodeKey(changed) == key {
		t.Fatal("different VMess servers must have different identities")
	}
	if got := stableNodeKey("vmess://not-valid-base64#name"); got != "vmess://not-valid-base64" {
		t.Fatalf("malformed payload fallback changed: %q", got)
	}
}

func TestLegacyPortMigrationPrefersRetainedExactAlias(t *testing.T) {
	encode := func(name string) string {
		return "vmess://" + base64.StdEncoding.EncodeToString([]byte(`{"ps":"`+name+`","add":"example.com","id":"uuid","port":443}`))
	}
	removed, retained := encode("A"), encode("B")
	saved := map[string]uint16{removed: 24444, retained: 24445}
	bridgeLegacyPortKeys([]NodeConfig{{URI: retained}}, saved)
	if got := saved[stableNodeKey(retained)]; got != 24445 {
		t.Fatalf("retained node's exact port changed: %d", got)
	}
}
