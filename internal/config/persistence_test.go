package config

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestConcurrentPersistencePreservesSeparateOwners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("mode: pool\nlistener:\n  address: 127.0.0.1\n  username: old\nnodes: []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	management := &Config{Mode: "pool", Listener: ListenerConfig{Address: "127.0.0.1", Username: "new-user"}}
	nodes := &Config{Nodes: []NodeConfig{{Name: "new-node", URI: "http://127.0.0.1:8080", Source: NodeSourceInline}}}
	sub := &Config{Subscriptions: []string{"http://fixture.invalid/sub"}, SubscriptionRefresh: SubscriptionRefreshConfig{Interval: 2 * time.Hour}}
	for _, cfg := range []*Config{management, nodes, sub} {
		cfg.SetFilePath(path)
	}
	errors := make(chan error, 3)
	var workers sync.WaitGroup
	for _, save := range []func() error{management.SaveManagementSettings, nodes.SaveNodes, sub.SaveSubscriptionSettings} {
		workers.Add(1)
		go func(save func() error) {
			defer workers.Done()
			for i := 0; i < 30; i++ {
				if err := save(); err != nil {
					errors <- err
					return
				}
			}
		}(save)
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	data, err := readFileWithLock(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved Config
	if err := yaml.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Listener.Username != "new-user" || len(saved.Nodes) != 1 || saved.Nodes[0].Name != "new-node" || len(saved.Subscriptions) != 1 || saved.SubscriptionRefresh.Interval != 2*time.Hour {
		t.Fatalf("concurrent save lost data: %+v", saved)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("existing private file mode changed: %v", info.Mode())
	}
}

func TestLockedWriteIsAtomicForUnlockedReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.txt")
	first, second := bytes.Repeat([]byte("a"), 65536), bytes.Repeat([]byte("b"), 65536)
	if err := writeFileWithLock(path, first, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	errors := make(chan error, 1)
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			data := first
			if i%2 == 0 {
				data = second
			}
			if err := writeFileWithLock(path, data, 0600); err != nil {
				errors <- err
				return
			}
		}
	}()
	for {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, first) && !bytes.Equal(data, second) {
			t.Fatal("reader observed truncated or mixed file")
		}
		select {
		case <-done:
			select {
			case err := <-errors:
				t.Fatal(err)
			default:
			}
			return
		default:
		}
	}
}

func TestLockedWriteFailureAndBindMountFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := []byte("original configuration")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockFile(file); err != nil {
		t.Fatal(err)
	}
	transaction := &lockedPath{path: path, file: file, forceInPlace: true}
	if err := transaction.write([]byte("short"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := unlockFile(file); err != nil {
		t.Fatal(err)
	}
	file.Close()
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "short" {
		t.Fatalf("in-place fallback failed: %q %v", data, err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	if err := writeFileWithLock(path, original, 0600); err == nil {
		t.Fatal("read-only destination must fail")
	}
	data, _ = os.ReadFile(path)
	if string(data) != "short" {
		t.Fatal("failed write modified original")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("invalid: ["), 0600); err != nil {
		t.Fatal(err)
	}
	if err := updateConfigFile(path, func(c *Config) { c.Mode = "pool" }); err == nil {
		t.Fatal("invalid original must not be silently replaced")
	}
	data, _ = os.ReadFile(path)
	if string(data) != "invalid: [" {
		t.Fatal("decode failure overwrote config")
	}
}

func TestAtomicWriterHonorsInPlaceWriterLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := lockFile(file); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- writeFileWithLock(path, []byte("new"), 0600) }()
	select {
	case err := <-done:
		unlockFile(file)
		t.Fatalf("writer ignored inode lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	data, _ := io.ReadAll(io.NewSectionReader(file, 0, 1<<63-1))
	if string(data) != "old" {
		t.Fatal("blocked writer changed destination")
	}
	if err := unlockFile(file); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
