package builder

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	"easy_proxies/internal/inbound/session"
	"easy_proxies/internal/outbound/pool"
	C "github.com/sagernet/sing-box/constant"
)

func TestPrintedProxyLinksMaskPasswords(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	printProxyLinks(&config.Config{
		Mode:      "hybrid",
		Listener:  config.ListenerConfig{Address: "::", Port: 2323, Username: "wang", Password: "sensitive-pool-password"},
		MultiPort: config.MultiPortConfig{Address: "::1", Username: "node", Password: "sensitive-multi-password"},
		Nodes:     []config.NodeConfig{{Name: "one", Port: 24000, Username: "direct", Password: "sensitive-node-password"}},
		Sticky:    config.StickyConfig{Enabled: true, Port: 2324},
	}, nil)
	for _, secret := range []string{"sensitive-pool-password", "sensitive-multi-password", "sensitive-node-password"} {
		if strings.Contains(output.String(), secret) {
			t.Fatal("password appeared in startup logs")
		}
	}
	if !strings.Contains(output.String(), "wang.SESSION") || !strings.Contains(output.String(), "[::1]:24000") {
		t.Fatal("session/IPv6 entry points missing")
	}
}

func TestBuildSessionRoutingMainEntry(t *testing.T) {
	for _, tc := range []struct {
		name        string
		username    string
		disabled    bool
		legacy      bool
		wantSession bool
	}{
		{name: "authenticated default", username: "wang", wantSession: true},
		{name: "legacy coexists", username: "wang", legacy: true, wantSession: true},
		{name: "anonymous unchanged"},
		{name: "explicitly disabled", username: "wang", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Mode: "pool", Listener: config.ListenerConfig{Address: "127.0.0.1", Port: 2323, Username: tc.username, Password: "secret"}, Nodes: []config.NodeConfig{{Name: "one", URI: "http://127.0.0.1:8080"}}}
			if tc.disabled {
				enabled := false
				cfg.Sticky.SessionEnabled = &enabled
			}
			if tc.legacy {
				cfg.Sticky.Enabled = true
				cfg.Sticky.Port = 2324
			}
			opts, err := Build(cfg)
			if err != nil {
				t.Fatal(err)
			}
			wantType := C.TypeMixed
			if tc.wantSession {
				wantType = session.Type
			}
			if opts.Inbounds[0].Type != wantType {
				t.Fatalf("main entry type = %q, want %q", opts.Inbounds[0].Type, wantType)
			}
			for _, ob := range opts.Outbounds {
				if ob.Tag == pool.Tag {
					p := ob.Options.(*pool.Options)
					if p.SessionRouting != tc.wantSession {
						t.Fatal("main pool session routing mismatch")
					}
					if len(p.Metadata["one"].Identity) != 64 {
						t.Fatal("stable node identity must be a SHA256 digest, not URI credentials")
					}
				}
			}
			if tc.legacy && (len(opts.Inbounds) != 2 || opts.Inbounds[1].Type != C.TypeMixed || opts.Inbounds[1].Tag != "sticky-in") {
				t.Fatal("legacy entry compatibility lost")
			}
		})
	}
}

func TestBuildClashAPIRequiresFeatureAndManagement(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		cfg := &config.Config{
			Mode:       "pool",
			Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: 2323},
			Management: config.ManagementConfig{Enabled: &enabled},
			Nodes:      []config.NodeConfig{{Name: "one", URI: "http://127.0.0.1:8080"}},
		}
		opts, err := Build(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got := opts.Experimental != nil && opts.Experimental.ClashAPI != nil
		if got != (enabled && clashAPIEnabled) {
			t.Fatalf("clash API enabled=%v, feature=%v, management=%v", got, clashAPIEnabled, enabled)
		}
	}
}
