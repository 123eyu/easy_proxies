package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func bindForTest(t *testing.T, store *Store, key, nodeID string) {
	t.Helper()
	lease, err := store.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire %q: %v", key, err)
	}
	defer lease.Release()
	lease.Bind(nodeID, "tag-"+nodeID, "name-"+nodeID, "dial_failed")
}

func TestStoreSuccessfulBindingAndRename(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	store := NewStore(2, time.Hour)
	store.now = func() time.Time { return now }
	bindForTest(t, store, "wang.session", "node-a")
	first := store.List()[0]
	if first.ID != "wang.session" || first.NodeID != "node-a" || first.LastReason != "initial" || first.SwitchCount != 0 {
		t.Fatalf("unexpected first binding: %+v", first)
	}
	if first.CreatedAt != now || first.LastSeen != now || first.ExpiresAt != now.Add(time.Hour) {
		t.Fatalf("unexpected binding times: %+v", first)
	}

	now = now.Add(time.Minute)
	lease, err := store.Acquire(context.Background(), first.ID)
	if err != nil {
		t.Fatal(err)
	}
	lease.Bind("node-a", "renamed-tag", "Renamed node", "node_unavailable")
	lease.Release()
	renamed := store.List()[0]
	if renamed.NodeTag != "renamed-tag" || renamed.NodeName != "Renamed node" || renamed.SwitchCount != 0 || renamed.CreatedAt != first.CreatedAt {
		t.Fatalf("rename changed node identity/history: %+v", renamed)
	}
	bindForTest(t, store, first.ID, "node-b")
	switched := store.List()[0]
	if switched.NodeID != "node-b" || switched.SwitchCount != 1 || switched.LastReason != "dial_failed" || switched.CreatedAt != first.CreatedAt {
		t.Fatalf("unexpected switched binding: %+v", switched)
	}
	// The API snapshot is a copy, not mutable shared cache state.
	snapshot := store.List()
	snapshot[0].NodeID = "modified"
	if store.List()[0].NodeID != "node-b" {
		t.Fatal("mutating a snapshot changed the stored binding")
	}
}

func TestStoreIdleExpiryAndLRUEviction(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	store := NewStore(2, time.Hour)
	store.now = func() time.Time { return now }
	bindForTest(t, store, "a", "node-a")
	firstA := store.List()[0]
	now = now.Add(time.Minute)
	bindForTest(t, store, "b", "node-b")
	now = now.Add(time.Minute)
	lease, _ := store.Acquire(context.Background(), "a")
	binding, exists := lease.Binding()
	lease.Release()
	if !exists || !binding.LastSeen.Equal(firstA.LastSeen) || !binding.ExpiresAt.Equal(firstA.ExpiresAt) {
		t.Fatalf("lookup extended the idle lifetime: %+v, exists=%v", binding, exists)
	}
	bindForTest(t, store, "c", "node-c")
	bindings := store.List()
	if len(bindings) != 2 || bindings[0].ID != "b" || bindings[1].ID != "c" {
		t.Fatalf("expected LRU eviction of the least recently dialed session, got %+v", bindings)
	}
	// Listing is observational: even repeated reads must not keep a session alive.
	now = now.Add(59 * time.Minute)
	_ = store.List()
	now = now.Add(time.Minute)
	if bindings = store.List(); len(bindings) != 0 || bindings == nil {
		t.Fatalf("expected non-nil empty snapshot at idle expiry, got %+v", bindings)
	}
}

func TestStoreConfigurePreservesBindingsAndAppliesLimits(t *testing.T) {
	store := NewStore(0, 0)
	if store.max != DefaultMaxBindings || store.ttl != DefaultIdleTTL {
		t.Fatalf("unexpected defaults: max=%d ttl=%v", store.max, store.ttl)
	}
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	bindForTest(t, store, "a", "node-a")
	now = now.Add(time.Minute)
	bindForTest(t, store, "b", "node-b")
	store.Configure(1, 10*time.Minute)
	bindings := store.List()
	if len(bindings) != 1 || bindings[0].ID != "b" || bindings[0].ExpiresAt != now.Add(10*time.Minute) {
		t.Fatalf("configure lost live state or ignored limits: %+v", bindings)
	}
	now = now.Add(11 * time.Minute)
	store.Configure(1, time.Minute)
	if len(store.List()) != 0 {
		t.Fatal("shortened TTL retained an already expired binding")
	}
	bindForTest(t, store, "c", "node-c")
	now = now.Add(2 * time.Minute)
	store.Configure(1, time.Hour)
	if len(store.List()) != 0 {
		t.Fatal("increasing TTL resurrected a previously expired session")
	}
}

func TestStoreAdministrativeUnbindInvalidatesInFlightCommit(t *testing.T) {
	for _, clear := range []bool{false, true} {
		name := "delete"
		if clear {
			name = "clear"
		}
		t.Run(name, func(t *testing.T) {
			store := NewStore(2, time.Hour)
			bindForTest(t, store, "session", "node-a")
			lease, _ := store.Acquire(context.Background(), "session")
			if clear {
				store.Clear()
			} else if !store.Delete("session") {
				t.Fatal("delete did not report an existing binding")
			}
			lease.Bind("node-b", "tag-b", "B", "dial_failed")
			lease.Release()
			if len(store.List()) != 0 {
				t.Fatal("in-flight dial resurrected an administrative unbind")
			}
			bindForTest(t, store, "session", "node-c")
			if binding := store.List()[0]; binding.NodeID != "node-c" || binding.SwitchCount != 0 {
				t.Fatalf("new request could not establish a fresh binding: %+v", binding)
			}
		})
	}

	// Deleting before an initial successful dial also invalidates that lease.
	store := NewStore(1, time.Hour)
	lease, _ := store.Acquire(context.Background(), "new")
	if store.Delete("new") {
		t.Fatal("delete reported a binding that has not been committed")
	}
	lease.Bind("node-a", "tag-a", "A", "initial")
	lease.Release()
	if len(store.List()) != 0 {
		t.Fatal("initial in-flight commit survived deletion")
	}
}

func TestStoreAcquireWaitIsCancellableAndKeysAreIndependent(t *testing.T) {
	store := NewStore(2, time.Hour)
	lease, _ := store.Acquire(context.Background(), "a")
	other, err := store.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatalf("an unrelated key was blocked: %v", err)
	}
	other.Release()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		waiting, err := store.Acquire(ctx, "a")
		if waiting != nil {
			waiting.Release()
		}
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting request returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not interrupt gate wait")
	}
	lease.Release()
	lease.Release() // Release is idempotent.
	lease.Bind("node-a", "tag-a", "A", "initial")
	if _, exists := lease.Binding(); exists || len(store.List()) != 0 {
		t.Fatal("a released lease could still read or commit a binding")
	}
	if len(store.gates) != 0 {
		t.Fatalf("released/cancelled gates leaked: %d", len(store.gates))
	}
}

func TestStoreConcurrentSameKeyCommitsAreSerialized(t *testing.T) {
	store := NewStore(2, time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := store.Acquire(context.Background(), "session")
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer lease.Release()
			lease.Bind("node-a", "tag-a", "A", "initial")
		}()
	}
	wg.Wait()
	bindings := store.List()
	if len(bindings) != 1 || bindings[0].SwitchCount != 0 || len(store.gates) != 0 {
		t.Fatalf("unexpected concurrent commit state: %+v, gates=%d", bindings, len(store.gates))
	}
}

func TestRoutingContextPreservesOpaqueSessionKey(t *testing.T) {
	store := NewStore(1, time.Hour)
	key := "wang.user:name 服务 +/%"
	ctx := WithSession(WithStore(context.Background(), store), key)
	if SessionFromContext(ctx) != key || StoreFromContext(ctx) != store {
		t.Fatal("context lost the store or changed the session identifier")
	}
	if SessionFromContext(context.Background()) != "" || StoreFromContext(context.Background()) != nil {
		t.Fatal("empty context unexpectedly enables session routing")
	}
}
