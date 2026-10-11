package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

func TestSessionRoutingUnavailablePinMovesWithoutAffectingOtherKeys(t *testing.T) {
	p := newSessionTestPool(nil, newSessionTestOutbound("a"), newSessionTestOutbound("b"))
	alice, bob := sessionTestContext(p, "account-session-alice"), sessionTestContext(p, "account-session-bob")
	mustSessionTestDial(t, alice, p, "a")
	mustSessionTestDial(t, bob, p, "b")
	bobPin := sessionTestBinding(t, p.sessionStore, "account-session-bob")
	a := p.members[0].shared
	a.recordFailure(errors.New("upstream 429 too many requests"), failurePolicy{RateLimitCooldown: time.Hour})
	mustSessionTestDial(t, alice, p, "b")
	after := sessionTestBinding(t, p.sessionStore, "account-session-alice")
	if after.SwitchCount != 1 || after.LastReason != "node_unavailable" {
		t.Fatalf("cooldown did not migrate the unavailable pin: %+v", after)
	}
	mustSessionTestDial(t, bob, p, "b")
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, bobPin.ID), bobPin)
	// Recovery only makes A eligible for new selections; migrated sessions
	// must not bounce back to their original upstream on its recovery.
	a.forceRelease()
	mustSessionTestDial(t, alice, p, "b")
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, after.ID), after)
}

func TestSessionRoutingDisabledRetryPreservesFailedPin(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-no-retry")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-no-retry")
	p.options.RetryEnabled = false
	a.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	if _, err := sessionTestDialTag(ctx, p); err == nil {
		t.Fatal("failed dial unexpectedly succeeded with retry disabled")
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
	if calls, _, _ := b.counts(); calls != 0 {
		t.Fatalf("retry-disabled request reached an alternate member %d times", calls)
	}
}

func TestSessionRoutingPacketFailureFallbackPreservesTCPPin(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-packet-failure")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-packet-failure")
	a.packetHook = func(context.Context) error { return errors.New("fixture packet failure") }
	conn, err := p.ListenPacket(ctx, M.ParseSocksaddr("example.test:53"))
	if err != nil {
		t.Fatalf("packet fallback failed: %v", err)
	}
	defer conn.Close()
	if got := conn.LocalAddr().String(); got != "b" {
		t.Fatalf("packet fallback used %q, want b", got)
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
	mustSessionTestDial(t, ctx, p, "a")
}

func TestSessionRoutingConcurrentFailoverCommitsOneMigration(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx, cancel := context.WithTimeout(sessionTestContext(p, "account-session-migrate"), 5*time.Second)
	defer cancel()
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-migrate")
	entered, release := make(chan struct{}), make(chan struct{})
	a.setDialHook(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return errors.New("fixture dial failure")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	type result struct {
		tag string
		err error
	}
	results := make(chan result, 2)
	dial := func() {
		tag, err := sessionTestDialTag(ctx, p)
		results <- result{tag, err}
	}
	go dial()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("failed pinned dial did not start")
	}
	go dial()
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil || got.tag != "b" {
				t.Fatalf("concurrent failover returned %+v, want b", got)
			}
		case <-ctx.Done():
			t.Fatal("concurrent failover did not finish")
		}
	}
	if calls, _, _ := a.counts(); calls != 2 {
		t.Fatalf("waiter retried stale pin a: calls=%d, want initial+leader only", calls)
	}
	after := sessionTestBinding(t, p.sessionStore, before.ID)
	if after.NodeID != "identity-b" || after.SwitchCount != 1 || after.CreatedAt != before.CreatedAt {
		t.Fatalf("concurrent failover did not commit exactly one migration: %+v", after)
	}
}
