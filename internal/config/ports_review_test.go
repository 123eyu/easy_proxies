package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func reviewPortsBase(t *testing.T) uint16 {
	t.Helper()
	// Darwin's ephemeral allocator can return a run of ports near 65535;
	// selecting a free fixture port from a bounded low range gives deterministic
	// headroom without relying on ten ephemeral retries to wrap the allocator.
	for port := 20000; port < 25000; port++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		listener.Close()
		return uint16(port)
	}
	t.Fatal("could not find a local test port with allocation headroom")
	return 0
}

func reviewPortsNode(name string, port uint16) NodeConfig {
	return NodeConfig{Name: name, URI: "http://" + name + ".fixture.invalid:8080", Port: port}
}

func assertReviewPortsDistinct(t *testing.T, nodes []NodeConfig, reserved map[uint16]bool) {
	t.Helper()
	seen := make(map[uint16]string)
	for _, node := range nodes {
		if node.Port == 0 || reserved[node.Port] {
			t.Fatalf("node %q assigned absent/reserved port %d", node.Name, node.Port)
		}
		if previous, exists := seen[node.Port]; exists {
			t.Fatalf("nodes %q and %q share port %d", previous, node.Name, node.Port)
		}
		seen[node.Port] = node.Name
	}
}

func TestPortsReviewAutoAllocationReservesActiveListeners(t *testing.T) {
	for _, defaultSticky := range []bool{false, true} {
		t.Run(fmt.Sprintf("default_sticky_%t", defaultSticky), func(t *testing.T) {
			base := reviewPortsBase(t)
			cfg := &Config{
				Mode:       "hybrid",
				Listener:   ListenerConfig{Address: "127.0.0.1", Port: base},
				MultiPort:  MultiPortConfig{Address: "127.0.0.1", BasePort: base},
				Sticky:     StickyConfig{Enabled: true, Port: base + 1},
				Management: ManagementConfig{Listen: fmt.Sprintf("127.0.0.1:%d", base+2)},
				GeoIP:      GeoIPConfig{Enabled: true, Listen: "127.0.0.1", Port: base + 3},
				Nodes:      []NodeConfig{reviewPortsNode("first", 0), reviewPortsNode("second", 0)},
			}
			if defaultSticky {
				cfg.Sticky.Port = 0
			}
			if err := cfg.NormalizeWithPortMap(nil); err != nil {
				t.Fatal(err)
			}
			reserved := map[uint16]bool{base: true, base + 1: true, base + 2: true, base + 3: true, 9092: true}
			assertReviewPortsDistinct(t, cfg.Nodes, reserved)
			if cfg.Sticky.Port != base+1 {
				t.Fatalf("sticky port = %d, want %d", cfg.Sticky.Port, base+1)
			}
		})
	}
}

func TestPortsReviewHybridProvisionalConflictSkipsReservedListeners(t *testing.T) {
	base := reviewPortsBase(t)
	cfg := &Config{
		Mode:       "hybrid",
		Listener:   ListenerConfig{Address: "127.0.0.1", Port: base},
		MultiPort:  MultiPortConfig{Address: "127.0.0.1", BasePort: base},
		Sticky:     StickyConfig{Enabled: true},
		Management: ManagementConfig{Listen: fmt.Sprintf("127.0.0.1:%d", base+2)},
		GeoIP:      GeoIPConfig{Enabled: true, Listen: "127.0.0.1", Port: base + 3},
		Nodes:      []NodeConfig{reviewPortsNode("pool-conflict", base)},
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	assertReviewPortsDistinct(t, cfg.Nodes, map[uint16]bool{base: true, base + 1: true, base + 2: true, base + 3: true, 9092: true})
}

func TestPortsReviewSavedPortsCannotClaimReservedListeners(t *testing.T) {
	for _, reserved := range []uint16{1221, 2323, 2324, 9091, 9092} {
		t.Run(fmt.Sprint(reserved), func(t *testing.T) {
			node := reviewPortsNode("existing", 0)
			cfg := &Config{
				Mode:       "hybrid",
				Listener:   ListenerConfig{Address: "127.0.0.1", Port: 2323},
				MultiPort:  MultiPortConfig{Address: "127.0.0.1", BasePort: reviewPortsBase(t)},
				Sticky:     StickyConfig{Enabled: true},
				Management: ManagementConfig{Listen: "127.0.0.1:9091"},
				GeoIP:      GeoIPConfig{Enabled: true},
				Nodes:      []NodeConfig{node},
			}
			if err := cfg.NormalizeWithPortMap(map[string]uint16{node.NodeKey(): reserved}); err != nil {
				t.Fatal(err)
			}
			assertReviewPortsDistinct(t, cfg.Nodes, map[uint16]bool{1221: true, 2323: true, 2324: true, 9091: true, 9092: true})
		})
	}
}

func TestPortsReviewControllerAndManagementAllocation(t *testing.T) {
	for _, managementEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("management_%t", managementEnabled), func(t *testing.T) {
			cfg := &Config{
				Mode:       "multi-port",
				MultiPort:  MultiPortConfig{Address: "127.0.0.1", BasePort: 9091},
				Management: ManagementConfig{Enabled: &managementEnabled, Listen: "127.0.0.1:9091"},
				Nodes:      []NodeConfig{reviewPortsNode("fresh", 0)},
			}
			if err := cfg.NormalizeWithPortMap(nil); err != nil {
				t.Fatal(err)
			}
			if managementEnabled && cfg.Nodes[0].Port <= 9092 {
				t.Fatalf("active management/controller port assigned to node: %d", cfg.Nodes[0].Port)
			}
			if cfg.reservedListenerPorts()[9091] != managementEnabled || cfg.reservedListenerPorts()[9092] != managementEnabled {
				t.Fatal("disabled management must not reserve otherwise unused ports")
			}
		})
	}
}

func TestPortsReviewAllocationExhaustionNeverWraps(t *testing.T) {
	for _, reserveLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("last_reserved_%t", reserveLast), func(t *testing.T) {
			disabled := false
			cfg := &Config{
				Mode:       "multi-port",
				MultiPort:  MultiPortConfig{Address: "127.0.0.1", BasePort: 65535},
				Management: ManagementConfig{Enabled: &disabled},
				Nodes:      []NodeConfig{reviewPortsNode("first", 0), reviewPortsNode("second", 0)},
			}
			if reserveLast {
				enabled := true
				cfg.Management = ManagementConfig{Enabled: &enabled, Listen: "127.0.0.1:65535"}
			}
			if err := cfg.NormalizeWithPortMap(nil); err == nil {
				t.Fatal("exhausted port range accepted")
			}
			for _, node := range cfg.Nodes {
				if node.Port > 0 && node.Port < 65535 {
					t.Fatalf("allocation wrapped to low port %d", node.Port)
				}
			}
		})
	}

	t.Run("hybrid_pool_last_explicit_collision", func(t *testing.T) {
		cfg := &Config{
			Mode:      "hybrid",
			Listener:  ListenerConfig{Address: "127.0.0.1", Port: 65535},
			MultiPort: MultiPortConfig{Address: "127.0.0.1", BasePort: 65535},
			Nodes:     []NodeConfig{reviewPortsNode("collision", 65535)},
		}
		if err := cfg.normalize(); err == nil {
			t.Fatal("pool conflict at last port should fail instead of wrapping")
		}
		if cfg.Nodes[0].Port != 65535 {
			t.Fatalf("conflicting port wrapped to %d", cfg.Nodes[0].Port)
		}
	})
}

func TestPortsReviewDuplicateSavedPortPreservesFirstOwner(t *testing.T) {
	base := reviewPortsBase(t)
	first, duplicate, untouched := reviewPortsNode("first", 0), reviewPortsNode("duplicate", base+1), reviewPortsNode("untouched", 0)
	cfg := &Config{Mode: "multi-port", MultiPort: MultiPortConfig{Address: "127.0.0.1", BasePort: base}, Nodes: []NodeConfig{first, duplicate, untouched}}
	saved := map[string]uint16{first.NodeKey(): base + 10, duplicate.NodeKey(): base + 10, untouched.NodeKey(): base + 11}
	if err := cfg.NormalizeWithPortMap(saved); err != nil {
		t.Fatal(err)
	}
	if cfg.Nodes[0].Port != base+10 || cfg.Nodes[2].Port != base+11 {
		t.Fatalf("unrelated stable ports changed: %+v", cfg.Nodes)
	}
	assertReviewPortsDistinct(t, cfg.Nodes, nil)
}

func TestPortsReviewExplicitDuplicatesAreRejected(t *testing.T) {
	base := reviewPortsBase(t)
	cfg := &Config{Mode: "multi-port", MultiPort: MultiPortConfig{Address: "127.0.0.1", BasePort: base}, Nodes: []NodeConfig{reviewPortsNode("first", base), reviewPortsNode("second", base)}}
	if err := cfg.NormalizeWithPortMap(nil); err == nil {
		t.Fatal("explicit duplicate ports accepted")
	}
}

func TestPortsReviewExplicitNewNodeCannotDisplaceSavedOwner(t *testing.T) {
	for _, newFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("new_first_%t", newFirst), func(t *testing.T) {
			base := reviewPortsBase(t)
			existing, added := reviewPortsNode("existing", 0), reviewPortsNode("added", base+10)
			cfg := &Config{Mode: "multi-port", MultiPort: MultiPortConfig{Address: "127.0.0.1", BasePort: base}, Nodes: []NodeConfig{existing, added}}
			if newFirst {
				cfg.Nodes = []NodeConfig{added, existing}
			}
			err := cfg.NormalizeWithPortMap(map[string]uint16{existing.NodeKey(): base + 10})
			if err == nil {
				t.Fatal("explicit newcomer conflicting with saved owner should be rejected")
			}
			for _, node := range cfg.Nodes {
				if node.NodeKey() == existing.NodeKey() && node.Port != base+10 {
					t.Fatalf("explicit newcomer displaced existing saved port %d -> %d", base+10, node.Port)
				}
			}
		})
	}
}

func reviewPortsInlineConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "mode: multi-port\n" +
		"multi_port:\n  address: 127.0.0.1\n  base_port: 23999\n" +
		"management:\n  enabled: false\n" +
		"nodes:\n" +
		"  - name: automatic\n    uri: http://automatic.fixture.invalid:8080\n" +
		"  - name: manual\n    uri: http://manual.fixture.invalid:8080\n    port: 23999\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertReviewInlinePort(t *testing.T, cfg *Config) {
	t.Helper()
	if len(cfg.Nodes) != 2 {
		t.Fatalf("node count=%d, want 2", len(cfg.Nodes))
	}
	if cfg.Nodes[1].Name != "manual" || cfg.Nodes[1].Port != 23999 {
		t.Fatalf("inline manually specified port changed: %+v", cfg.Nodes[1])
	}
	if cfg.Nodes[0].Port == 0 || cfg.Nodes[0].Port == 23999 {
		t.Fatalf("automatic node claimed manual port: %+v", cfg.Nodes[0])
	}
	assertReviewPortsDistinct(t, cfg.Nodes, nil)
}

func TestPortsReviewLoadPreservesInlineManualPortWithoutSidecar(t *testing.T) {
	path := reviewPortsInlineConfig(t)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	assertReviewInlinePort(t, cfg)
	ports := loadNodePortMap(filepath.Join(filepath.Dir(path), nodePortMapFile))
	if ports[cfg.Nodes[1].NodeKey()] != 23999 {
		t.Fatalf("manual port not recorded in new sidecar: %v", ports)
	}
}

func TestPortsReviewLoadManualPortOverridesStaleSelfMapping(t *testing.T) {
	path := reviewPortsInlineConfig(t)
	automatic := reviewPortsNode("automatic", 0)
	manual := reviewPortsNode("manual", 23999)
	stale := map[string]uint16{manual.NodeKey(): 24042, automatic.NodeKey(): 24043}
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), nodePortMapFile), mustJSON(t, stale), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	assertReviewInlinePort(t, cfg)
	if cfg.Nodes[0].Port != 24043 {
		t.Fatalf("unrelated automatic node lost its saved port: got %d want 24043", cfg.Nodes[0].Port)
	}
	ports := loadNodePortMap(filepath.Join(filepath.Dir(path), nodePortMapFile))
	if ports[manual.NodeKey()] != 23999 || ports[automatic.NodeKey()] != 24043 {
		t.Fatalf("sidecar did not publish edited manual and unchanged stable ports: %v", ports)
	}
}

func TestPortsReviewLoadHighBaseUsesSavedPortsBeforeCheckingExhaustion(t *testing.T) {
	for _, withSidecar := range []bool{false, true} {
		t.Run(fmt.Sprintf("saved_%t", withSidecar), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			content := "mode: multi-port\n" +
				"multi_port:\n  address: 127.0.0.1\n  base_port: 65535\n" +
				"management:\n  enabled: false\n" +
				"nodes:\n" +
				"  - name: first\n    uri: http://first.fixture.invalid:8080\n" +
				"  - name: second\n    uri: http://second.fixture.invalid:8080\n"
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			first, second := reviewPortsNode("first", 0), reviewPortsNode("second", 0)
			if withSidecar {
				saved := map[string]uint16{first.NodeKey(): 24040, second.NodeKey(): 24041}
				if err := os.WriteFile(filepath.Join(filepath.Dir(path), nodePortMapFile), mustJSON(t, saved), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(path)
			if !withSidecar {
				if err == nil {
					t.Fatal("two new nodes beyond the final available port should be rejected")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid saved ports rejected before authoritative allocation: %v", err)
			}
			if cfg.Nodes[0].Port != 24040 || cfg.Nodes[1].Port != 24041 {
				t.Fatalf("saved ports below edited base changed: %+v", cfg.Nodes)
			}
			assertReviewPortsDistinct(t, cfg.Nodes, nil)
		})
	}
}
