package subscription

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Warnf(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}

type fakeRuntime struct {
	mu        sync.Mutex
	cfg       *config.Config
	reloads   int
	reloadErr error
}

func (f *fakeRuntime) CurrentConfig() *config.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneConfig(f.cfg)
}
func (f *fakeRuntime) CurrentPortMap() map[string]uint16 { return f.CurrentConfig().BuildPortMap() }
func (f *fakeRuntime) ReloadWithPortMap(cfg *config.Config, _ map[string]uint16) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloads++
	if f.reloadErr != nil {
		return f.reloadErr
	}
	f.cfg = cloneConfig(cfg)
	return nil
}
func testManager(t *testing.T, urls []string, enabled bool) (*Manager, *fakeRuntime) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		Mode: "pool", Subscriptions: urls, NodesFile: filepath.Join(dir, "nodes.txt"),
		Listener: config.ListenerConfig{Username: "before"},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Enabled: enabled, Interval: time.Hour, Timeout: time.Second,
			HealthCheckTimeout: time.Second, DrainTimeout: time.Second,
		},
		Nodes: []config.NodeConfig{{Name: "old", URI: "trojan://pw@old.example:443#old", Source: config.NodeSourceSubscription}},
	}
	cfg.SetFilePath(filepath.Join(dir, "config.yaml"))
	if err := os.WriteFile(cfg.FilePath(), []byte("mode: pool\nlistener:\n  username: before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.NodesFile, []byte(cfg.Nodes[0].URI+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := New(cfg, nil, WithLogger(quietLogger{}))
	runtime := &fakeRuntime{cfg: cloneConfig(cfg)}
	m.boxMgr = runtime
	t.Cleanup(m.Stop)
	return m, runtime
}

func TestManualRefreshWorksWhenPeriodicDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	m.Start()
	m.Start()
	if err := m.RefreshNow(); err != nil {
		t.Fatal(err)
	}
	status := m.Status()
	if status.RefreshCount != 1 || status.LastError != "" || status.NodeCount != 1 || !status.NextRefresh.IsZero() {
		t.Fatalf("unexpected status: %+v", status)
	}
	if runtime.CurrentConfig().Nodes[0].Name != "new" {
		t.Fatal("new nodes not activated")
	}
}

func TestFastRefreshCompletionDoesNotMissNotification(t *testing.T) {
	m, _ := testManager(t, []string{"invalid://subscription"}, false)
	for i := 0; i < 5; i++ {
		start := time.Now()
		err := m.RefreshNow()
		if err == nil || !strings.Contains(err.Error(), "incomplete") {
			t.Fatalf("unexpected error: %v", err)
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatal("fast completion was missed")
		}
	}
	if count := m.Status().RefreshCount; count != 5 {
		t.Fatalf("refresh count = %d, want 5", count)
	}
}

func TestConcurrentManualRefreshesShareOneCompletion(t *testing.T) {
	var hits atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, _ := testManager(t, []string{server.URL}, false)
	first, err := m.requestRefresh()
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	const clients = 25
	calls := make([]*refreshCall, clients)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func(i int) { defer wg.Done(); calls[i], _ = m.requestRefresh() }(i)
	}
	wg.Wait()
	for i, call := range calls {
		if call != first {
			t.Fatalf("client %d queued duplicate refresh", i)
		}
	}
	close(release)
	<-first.done
	if first.err != nil {
		t.Fatal(first.err)
	}
	if hits.Load() != 1 || m.Status().RefreshCount != 1 {
		t.Fatal("concurrent refreshes were not deduplicated")
	}
}

func TestReloadFailureLeavesPersistedCacheUntouched(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	runtime.reloadErr = errors.New("fixture reload rejected")
	before, _ := os.ReadFile(m.getNodesFilePath())
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "reload rejected") {
		t.Fatalf("unexpected error: %v", err)
	}
	after, _ := os.ReadFile(m.getNodesFilePath())
	if string(after) != string(before) {
		t.Fatal("failed reload polluted last-known-good cache")
	}
}

func TestCacheWriteFailureRollsBackRuntime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	path := m.getNodesFilePath()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "runtime rolled back") {
		t.Fatalf("unexpected error: %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.reloads != 2 || runtime.cfg.Nodes[0].Name != "old" {
		t.Fatal("runtime was not rolled back after cache write failed")
	}
}

func TestPartialRefreshReportsFailureAndKeepsCurrentNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/bad" {
			http.Error(w, "failed", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL, server.URL + "/bad"}, false)
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "1/2 URLs failed") {
		t.Fatalf("unexpected error: %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.reloads != 0 || runtime.cfg.Nodes[0].Name != "old" {
		t.Fatal("partial refresh discarded a provider's nodes")
	}
}

func TestRefreshPreservesLatestRuntimeSettingsAndInlineNodes(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	call, _ := m.requestRefresh()
	<-entered
	runtime.mu.Lock()
	runtime.cfg.Listener.Username = "after"
	runtime.cfg.Nodes = append(runtime.cfg.Nodes, config.NodeConfig{Name: "user-added", URI: "trojan://pw@inline.example:443", Source: config.NodeSourceInline})
	runtime.mu.Unlock()
	close(release)
	<-call.done
	if call.err != nil {
		t.Fatal(call.err)
	}
	cfg := runtime.CurrentConfig()
	if cfg.Listener.Username != "after" || len(cfg.Nodes) != 2 || cfg.Nodes[0].Name != "user-added" {
		t.Fatalf("refresh overwrote latest config: %+v", cfg)
	}
}

func TestNodesModifiedStatusPersistsAcrossPollsAndClearsOnRestore(t *testing.T) {
	m, _ := testManager(t, nil, false)
	path := m.getNodesFilePath()
	old, _ := os.ReadFile(path)
	m.lastSubHash = m.computeNodesHash(m.baseCfg.Nodes)
	info, _ := os.Stat(path)
	m.lastNodesModTime, m.lastNodesSize = info.ModTime(), info.Size()
	if err := os.WriteFile(path, []byte("trojan://pw@changed.example:443\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if !m.Status().NodesModified {
			t.Fatalf("poll %d lost dirty status", i)
		}
	}
	if err := os.WriteFile(path, old, 0o600); err != nil {
		t.Fatal(err)
	}
	if m.Status().NodesModified {
		t.Fatal("restoring baseline did not clear dirty status")
	}
	m.MarkNodesModified()
	if !m.Status().NodesModified {
		t.Fatal("explicit dirty marker was discarded")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if !m.Status().NodesModified {
		t.Fatal("deleted node file not marked dirty")
	}
}

func TestCallerCancellationDoesNotCancelSharedRefresh(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, _ := testManager(t, []string{server.URL}, false)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- m.RefreshNowContext(ctx) }()
	<-entered
	shared, _ := m.requestRefresh()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation error: %v", err)
	}
	close(release)
	<-shared.done
	if shared.err != nil {
		t.Fatalf("shared refresh was canceled by one caller: %v", shared.err)
	}
}

func TestStopCancelsInFlightFetchAndNeverRestarts(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	m, _ := testManager(t, []string{server.URL}, true)
	call, _ := m.requestRefresh()
	<-entered
	m.Stop()
	<-call.done
	if call.err == nil {
		t.Fatal("stopped refresh unexpectedly succeeded")
	}
	if err := m.RefreshNow(); err == nil {
		t.Fatal("stopped manager restarted")
	}
	if !m.Status().NextRefresh.IsZero() {
		t.Fatal("stopped manager still has scheduled refresh")
	}
}

func TestConfigUpdateQueuesNewGenerationWithoutAnotherWorker(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var oldHits, newHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/old" {
			oldHits.Add(1)
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte("trojan://pw@oldfetch.example:443#obsolete\n"))
			return
		}
		newHits.Add(1)
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL + "/old"}, false)
	old, _ := m.requestRefresh()
	<-entered
	if err := m.updateConfig([]string{server.URL + "/new"}, false, time.Hour); err != nil {
		t.Fatal(err)
	}
	latest, _ := m.requestRefresh()
	if latest == old {
		t.Fatal("new settings joined obsolete refresh")
	}
	close(release)
	<-old.done
	<-latest.done
	if old.err == nil || !strings.Contains(old.err.Error(), "configuration changed") {
		t.Fatalf("obsolete refresh error = %v", old.err)
	}
	if latest.err != nil {
		t.Fatal(latest.err)
	}
	if oldHits.Load() != 1 || newHits.Load() != 1 {
		t.Fatal("configuration update started duplicate worker")
	}
	if runtime.CurrentConfig().Nodes[0].Name != "new" {
		t.Fatal("obsolete fetch overwrote new configuration")
	}
}

func TestManagerClonesInputAndUpdateURLSlice(t *testing.T) {
	m, _ := testManager(t, nil, false)
	urls := []string{"http://fixture.invalid/one"}
	if err := m.updateConfig(urls, false, time.Hour); err != nil {
		t.Fatal(err)
	}
	urls[0] = "http://fixture.invalid/mutated"
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.baseCfg.Subscriptions[0] != "http://fixture.invalid/one" {
		t.Fatal("caller URL backing array was retained")
	}
}

func TestPeriodicTimerCanBeDisabledWithoutDisablingManualRefresh(t *testing.T) {
	var hits atomic.Int32
	first := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			close(first)
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, true)
	m.mu.Lock()
	m.baseCfg.SubscriptionRefresh.Interval = 10 * time.Millisecond
	m.mu.Unlock()
	runtime.mu.Lock()
	runtime.cfg.SubscriptionRefresh.Interval = 10 * time.Millisecond
	runtime.mu.Unlock()
	m.Start()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("periodic refresh never ran")
	}
	if err := m.updateConfig([]string{server.URL}, false, time.Hour); err != nil {
		t.Fatal(err)
	}
	// Join any just-started periodic round. This must remain possible while
	// automatic refresh is now disabled.
	if err := m.RefreshNow(); err != nil {
		t.Fatal(err)
	}
	before := hits.Load()
	time.Sleep(40 * time.Millisecond)
	if after := hits.Load(); after != before {
		t.Fatalf("disabled timer kept firing: %d -> %d", before, after)
	}
	if !m.Status().NextRefresh.IsZero() {
		t.Fatal("disabled timer kept a next-refresh timestamp")
	}
	if err := m.RefreshNow(); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != before+1 {
		t.Fatal("manual refresh no longer worked after timer was disabled")
	}
}

func TestEmptySubscriptionsFailFastAndSaveFailureDoesNotPublish(t *testing.T) {
	m, _ := testManager(t, nil, false)
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "no subscription") {
		t.Fatalf("unexpected empty subscription error: %v", err)
	}
	m.mu.Lock()
	m.baseCfg.SetFilePath(filepath.Join(t.TempDir(), "missing.yaml"))
	m.mu.Unlock()
	if err := m.UpdateConfigAndRefresh([]string{"http://fixture.invalid/sub"}, true, time.Minute); err == nil {
		t.Fatal("missing config file unexpectedly saved")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.baseCfg.Subscriptions) != 0 || m.baseCfg.SubscriptionRefresh.Enabled || m.version != 0 {
		t.Fatal("failed save polluted private subscription configuration")
	}
}

func TestCreateNewConfigDoesNotMutateCallerNodes(t *testing.T) {
	input := []config.NodeConfig{{Name: " subscribed ", URI: "trojan://pw@new.example:443", Source: config.NodeSourceFile}}
	mgr := &Manager{baseCfg: &config.Config{Mode: "pool"}}
	cfg := mgr.createNewConfig(input)
	if input[0].Source != config.NodeSourceFile || input[0].Name != " subscribed " {
		t.Fatal("building refresh config modified the input slice")
	}
	if cfg.Nodes[0].Source != config.NodeSourceSubscription || cfg.Nodes[0].Name != "subscribed" {
		t.Fatalf("unexpected merged node: %+v", cfg.Nodes[0])
	}
}

type transactionalFakeRuntime struct {
	*fakeRuntime
	transactionMu sync.Mutex
	transactions  int
	beforeUpdate  func()
}

func (f *transactionalFakeRuntime) ReloadWithCurrentConfig(update func(*config.Config) error, persist func(*config.Config) error) error {
	f.transactionMu.Lock()
	defer f.transactionMu.Unlock()
	f.transactions++
	if f.beforeUpdate != nil {
		f.beforeUpdate()
	}
	old := f.CurrentConfig()
	candidate := cloneConfig(old)
	if err := update(candidate); err != nil {
		return err
	}
	if err := f.ReloadWithPortMap(candidate, old.BuildPortMap()); err != nil {
		return err
	}
	if err := persist(candidate); err != nil {
		if rollbackErr := f.ReloadWithPortMap(old, old.BuildPortMap()); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return errors.New("persistence failed; runtime rolled back: " + err.Error())
	}
	return nil
}

func TestRefreshUsesAtomicCurrentConfigTransaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	transaction := &transactionalFakeRuntime{fakeRuntime: runtime}
	transaction.beforeUpdate = func() {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		runtime.cfg.Listener.Username = "latest-at-transaction-start"
		runtime.cfg.Nodes = append(runtime.cfg.Nodes, config.NodeConfig{
			Name: "added-at-transaction-start", URI: "trojan://pw@inline.example:443", Source: config.NodeSourceInline,
		})
	}
	m.boxMgr = transaction
	if err := m.RefreshNow(); err != nil {
		t.Fatal(err)
	}
	cfg := runtime.CurrentConfig()
	if transaction.transactions != 1 || cfg.Listener.Username != "latest-at-transaction-start" || len(cfg.Nodes) != 2 || cfg.Nodes[0].Name != "added-at-transaction-start" {
		t.Fatalf("refresh bypassed atomic latest-config update: %+v", cfg)
	}
	data, err := os.ReadFile(m.getNodesFilePath())
	if err != nil || !strings.Contains(string(data), "new.example") {
		t.Fatalf("transaction did not persist activated nodes: %q, %v", data, err)
	}
}

func TestAtomicRefreshPersistenceFailureRollsBack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, false)
	m.boxMgr = &transactionalFakeRuntime{fakeRuntime: runtime}
	path := m.getNodesFilePath()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "runtime rolled back") {
		t.Fatalf("unexpected transaction error: %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.reloads != 2 || runtime.cfg.Nodes[0].Name != "old" {
		t.Fatal("atomic transaction did not restore previous runtime")
	}
}

func TestEmptyProviderResponseKeepsAllCurrentNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/empty" {
			_, _ = w.Write([]byte("<html>fixture subscription login page</html>"))
			return
		}
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL, server.URL + "/empty"}, false)
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "1 returned no usable nodes") {
		t.Fatalf("unexpected empty-provider error: %v", err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.reloads != 0 || runtime.cfg.Nodes[0].Name != "old" {
		t.Fatal("empty provider response discarded cached provider nodes")
	}
}

func TestRefreshDoesNotOverwriteNewlyPublishedSubscriptions(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte("trojan://pw@obsolete.example:443#obsolete\n"))
	}))
	defer server.Close()
	for _, atomic := range []bool{false, true} {
		t.Run(map[bool]string{false: "fallback", true: "transaction"}[atomic], func(t *testing.T) {
			// Use independent channels/server for the second subtest.
			if atomic {
				entered, release = make(chan struct{}), make(chan struct{})
			}
			m, runtime := testManager(t, []string{server.URL + "/old"}, false)
			if atomic {
				m.boxMgr = &transactionalFakeRuntime{fakeRuntime: runtime}
			}
			call, _ := m.requestRefresh()
			<-entered
			// Reproduce the monitor publish -> private worker update interval:
			// runtime has newer settings, but m.version has not changed yet.
			runtime.mu.Lock()
			runtime.cfg.Subscriptions = []string{server.URL + "/new"}
			runtime.mu.Unlock()
			close(release)
			<-call.done
			if call.err == nil || !strings.Contains(call.err.Error(), "preserving newly published") {
				t.Fatalf("unexpected publication-conflict result: %v", call.err)
			}
			cfg := runtime.CurrentConfig()
			if cfg.Subscriptions[0] != server.URL+"/new" || cfg.Nodes[0].Name != "old" {
				t.Fatalf("obsolete refresh overwrote newly published subscriptions: %+v", cfg)
			}
		})
	}
}

func TestAuthoritativeRuntimeClearNeverRevivesOldSubscriptions(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	for _, atomicRuntime := range []bool{false, true} {
		t.Run(map[bool]string{false: "fallback", true: "transaction"}[atomicRuntime], func(t *testing.T) {
			m, runtime := testManager(t, []string{server.URL}, true)
			if atomicRuntime {
				m.boxMgr = &transactionalFakeRuntime{fakeRuntime: runtime}
			}
			m.Start()
			// Simulate an authoritative config-file reload that clears URLs.
			runtime.mu.Lock()
			runtime.cfg.Subscriptions = nil
			runtime.cfg.SubscriptionRefresh.Enabled = false
			runtime.mu.Unlock()
			before := hits.Load()
			if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "no subscription URLs") {
				t.Fatalf("old private URLs were not cleared: %v", err)
			}
			if hits.Load() != before || len(runtime.CurrentConfig().Subscriptions) != 0 {
				t.Fatal("a cleared runtime subscription was fetched or resurrected")
			}
			if !m.Status().NextRefresh.IsZero() {
				t.Fatal("cleared subscriptions still had a scheduled refresh")
			}
		})
	}
}

func TestAuthoritativeRuntimeDisableStopsOldTimerButAllowsManual(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL}, true)
	m.mu.Lock()
	m.baseCfg.SubscriptionRefresh.Interval = 20 * time.Millisecond
	m.mu.Unlock()
	runtime.mu.Lock()
	runtime.cfg.SubscriptionRefresh.Interval = 20 * time.Millisecond
	runtime.mu.Unlock()
	m.Start()
	// Wait until the worker has armed its timer, without fetching anything.
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.RLock()
		armed := !m.status.NextRefresh.IsZero()
		m.mu.RUnlock()
		if armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic timer was never armed")
		}
		time.Sleep(time.Millisecond)
	}
	runtime.mu.Lock()
	runtime.cfg.SubscriptionRefresh.Enabled = false
	runtime.mu.Unlock()
	// Do not poll Status: the stale timer tick itself must check authority.
	time.Sleep(75 * time.Millisecond)
	if hits.Load() != 0 {
		t.Fatal("old timer fetched after authoritative runtime disable")
	}
	if !m.Status().NextRefresh.IsZero() {
		t.Fatal("runtime disable did not clear next-refresh timestamp")
	}
	if err := m.RefreshNow(); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 || runtime.CurrentConfig().SubscriptionRefresh.Enabled {
		t.Fatal("manual refresh either stopped working or reenabled periodic refresh")
	}
}

func TestDirectAPIEditActivatesWhileRuntimeStillHasPriorSubscriptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("trojan://pw@new.example:443#new\n"))
	}))
	defer server.Close()
	m, runtime := testManager(t, []string{server.URL + "/old"}, false)
	if err := m.UpdateConfigAndRefresh([]string{server.URL + "/new"}, false, time.Hour); err != nil {
		t.Fatal(err)
	}
	cfg := runtime.CurrentConfig()
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != server.URL+"/new" || cfg.Nodes[0].Name != "new" {
		t.Fatalf("explicit direct API edit was overwritten by old runtime config: %+v", cfg)
	}
}

func TestAuthoritativeClearSupersedesPendingDirectEdit(t *testing.T) {
	m, runtime := testManager(t, []string{"http://fixture.invalid/old"}, false)
	if err := m.updateConfig([]string{"http://fixture.invalid/new"}, false, time.Hour); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	runtime.cfg.Subscriptions = nil
	runtime.mu.Unlock()
	if err := m.RefreshNow(); err == nil || !strings.Contains(err.Error(), "no subscription URLs") {
		t.Fatalf("authoritative clear lost to a pending worker edit: %v", err)
	}
	if len(runtime.CurrentConfig().Subscriptions) != 0 {
		t.Fatal("pending direct edit resurrected cleared URLs")
	}
}
