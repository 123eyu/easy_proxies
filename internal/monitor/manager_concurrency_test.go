package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerCallbacksAndRegistrationAreRaceFree(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	handle.SetProbe(func(context.Context) (time.Duration, error) { return time.Millisecond, nil })
	handle.SetRelease(func() {})
	var wg sync.WaitGroup
	for lane := range 4 {
		wg.Add(1)
		go func(lane int) {
			defer wg.Done()
			for i := range 500 {
				switch lane {
				case 0:
					mgr.Register(NodeInfo{Tag: "fixture", Name: fmt.Sprint(i)})
				case 1:
					handle.SetProbe(func(context.Context) (time.Duration, error) { return time.Millisecond, nil })
					handle.SetRelease(func() {})
				case 2:
					_ = mgr.Snapshot()
				case 3:
					_, _ = mgr.Probe(context.Background(), "fixture")
					_ = mgr.Release("fixture")
				}
			}
		}(lane)
	}
	wg.Wait()
}

func TestProbeSingleFlightBoundsCallbacksIgnoringCancellation(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	var calls atomic.Int32
	release := make(chan struct{})
	handle.SetProbe(func(context.Context) (time.Duration, error) { calls.Add(1); <-release; return time.Millisecond, nil })
	defer close(release)
	for range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		_, err := mgr.Probe(ctx, "fixture")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Probe error=%v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("ignored cancellation launched %d callbacks, want one", got)
	}
}

func TestCanceledProbeDoesNotPoisonHealthyState(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	handle.MarkInitialCheckDone(true)
	handle.RecordFailure(errors.New("previous diagnostic"))
	started := make(chan struct{})
	handle.SetProbe(func(ctx context.Context) (time.Duration, error) { close(started); <-ctx.Done(); return 0, ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { _, err := mgr.Probe(ctx, "fixture"); finished <- err }()
	<-started
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("Probe=%v", err)
	}
	snap := mgr.Snapshot()[0]
	if !snap.Available || !snap.InitialCheckDone || snap.LastError != "previous diagnostic" {
		t.Fatalf("canceled caller changed node health: %+v", snap)
	}
}

func TestJoinedProbeDeadlineDoesNotPoisonHealthyState(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	handle.MarkInitialCheckDone(true)
	started, release := make(chan struct{}), make(chan struct{})
	handle.SetProbe(func(context.Context) (time.Duration, error) { close(started); <-release; return time.Millisecond, nil })
	finished := make(chan struct{})
	go func() { _, _ = mgr.Probe(context.Background(), "fixture"); close(finished) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	_, err := mgr.Probe(ctx, "fixture")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Probe=%v", err)
	}
	if !mgr.Snapshot()[0].Available {
		t.Error("short-lived joined caller marked healthy node offline")
	}
	close(release)
	<-finished
}

func TestSuccessfulProbeClearsErrorAndSnapshotExpiresBlacklist(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	handle.RecordFailure(errors.New("old error"))
	handle.Blacklist(time.Now().Add(-time.Second))
	handle.SetProbe(func(context.Context) (time.Duration, error) { return time.Millisecond, nil })
	if _, err := mgr.Probe(context.Background(), "fixture"); err != nil {
		t.Fatal(err)
	}
	snap := mgr.Snapshot()[0]
	if snap.LastError != "" || !snap.Available || snap.Blacklisted {
		t.Fatalf("stale node state: %+v", snap)
	}
	if len(mgr.SnapshotFiltered(true)) != 1 {
		t.Fatal("expired blacklist still excluded healthy node")
	}
}

func TestLateProbeResultDoesNotOverwriteNewerHealthyRun(t *testing.T) {
	mgr, _ := NewManager(Config{})
	defer mgr.Stop()
	handle := mgr.Register(NodeInfo{Tag: "fixture"})
	oldError := errors.New("previous probe failed")
	handle.SetProbe(func(context.Context) (time.Duration, error) { return 0, oldError })
	// Capture a completed first-run result while simulating a delayed caller
	// that has not yet published its health state.
	oldLatency, err, _, oldGeneration := handle.ref.waitProbe(context.Background())
	if !errors.Is(err, oldError) {
		t.Fatal(err)
	}
	handle.SetProbe(func(context.Context) (time.Duration, error) { return time.Millisecond, nil })
	if _, err := mgr.Probe(context.Background(), "fixture"); err != nil {
		t.Fatal(err)
	}
	if handle.ref.applyProbeResult(oldGeneration, oldLatency, oldError) {
		t.Fatal("older probe generation was allowed to update node health")
	}
	snap := mgr.Snapshot()[0]
	if !snap.Available || snap.LastError != "" || snap.LastLatencyMs != 1 {
		t.Fatalf("late failure overwrote newer success: %+v", snap)
	}
}
