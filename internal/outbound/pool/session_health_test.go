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

func TestSessionRoutingDisabledRetryStillReroutesStalePin(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-no-retry")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-no-retry")
	p.options.RetryEnabled = false
	a.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	// A failed dial through the pinned node is a stale-binding error, not a
	// pool scheduling retry: the request re-routes once even with retries
	// disabled, because otherwise that session would keep failing until the
	// pin idled out or an operator reset it.
	mustSessionTestDial(t, ctx, p, "b")
	after := sessionTestBinding(t, p.sessionStore, before.ID)
	if after.NodeID != "identity-b" || after.SwitchCount != before.SwitchCount+1 || after.LastReason != "dial_failed" {
		t.Fatalf("stale pin was not migrated: %+v", after)
	}
	if calls, _, _ := a.counts(); calls != 2 {
		t.Fatalf("stale pin caused %d dials through a, want 2", calls)
	}
	// If the re-route fails too, the pin is kept so a transient outage does not
	// silently move the session to another exit.
	b.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	if _, err := sessionTestDialTag(ctx, p); err == nil {
		t.Fatal("failed dial unexpectedly succeeded with retry disabled")
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, after.ID), after)
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

// A transport fault seen through a session's pinned node blacklists that node
// right away, so neither the session nor ordinary scheduling keeps retrying it.
func TestSessionRoutingPinnedFailureBlacklistsNodeImmediately(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-blacklist")
	mustSessionTestDial(t, ctx, p, "a")
	a.setDialHook(func(context.Context) error {
		return errors.New("tls handshake: remote error: tls: unrecognized name")
	})
	mustSessionTestDial(t, ctx, p, "b")
	pin := p.members[0]
	if remaining := pin.shared.blacklistRemaining(time.Now()); remaining < time.Hour {
		t.Fatalf("pinned node blacklist = %v, want the configured duration", remaining)
	}
	if pin.shared.banIsCooldown {
		t.Fatal("transport fault was recorded as a cooldown, not a blacklist")
	}
	// Another session must not be handed the blacklisted node.
	mustSessionTestDial(t, sessionTestContext(p, "account-session-blacklist-other"), p, "b")
}

// A pinned node that only reports 429/timeout keeps the short cooldown: those
// failures say nothing about the node's long-term health and expire on their own.
func TestSessionRoutingPinnedTransientFailureCoolsDown(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-cooldown")
	mustSessionTestDial(t, ctx, p, "a")
	a.setDialHook(func(context.Context) error { return errors.New("i/o timeout") })
	mustSessionTestDial(t, ctx, p, "b")
	pin := p.members[0]
	remaining := pin.shared.blacklistRemaining(time.Now())
	if remaining <= 0 || remaining > 2*time.Minute {
		t.Fatalf("transient failure pause = %v, want the transient cooldown", remaining)
	}
	if !pin.shared.banIsCooldown {
		t.Fatal("transient failure was recorded as a permanent blacklist")
	}
	mustSessionTestDial(t, sessionTestContext(p, "account-session-cooldown-other"), p, "b")
}

// blacklist_failed_pin: false keeps the re-route but returns the node to the
// ordinary threshold path, so an operator who disabled it keeps the node in
// rotation until failure_threshold dials count up.
func TestSessionRoutingFailedPinBlacklistCanBeDisabled(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	disabled := false
	p.options.BlacklistFailedPin = &disabled
	ctx := sessionTestContext(p, "account-session-blacklist-off")
	mustSessionTestDial(t, ctx, p, "a")
	a.setDialHook(func(context.Context) error {
		return errors.New("tls handshake: remote error: tls: unrecognized name")
	})
	mustSessionTestDial(t, ctx, p, "b")
	if got := sessionTestBinding(t, p.sessionStore, "account-session-blacklist-off").NodeID; got != "identity-b" {
		t.Fatalf("binding = %q, want the re-routed identity-b", got)
	}
	if remaining := p.members[0].shared.blacklistRemaining(time.Now()); remaining != 0 {
		t.Fatalf("disabled immediate blacklist still banned the node for %v", remaining)
	}
	p.members[0].shared.mu.Lock()
	failures := p.members[0].shared.failures
	p.members[0].shared.mu.Unlock()
	if failures != 1 {
		t.Fatalf("failures = %d, want one counted toward failure_threshold", failures)
	}
}
