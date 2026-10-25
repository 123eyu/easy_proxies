package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"easy_proxies/internal/routing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	singlog "github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// These fixtures never open sockets. Hooks make failures and in-flight dials
// deterministic, while call counters also work under the race detector.
type sessionTestOutbound struct {
	outbound.Adapter
	mu          sync.Mutex
	dialCalls   int
	packetCalls int
	inflight    int
	maxInflight int
	dialHook    func(context.Context) error
	packetHook  func(context.Context) error
}

func newSessionTestOutbound(tag string, networks ...string) *sessionTestOutbound {
	if len(networks) == 0 {
		networks = []string{N.NetworkTCP, N.NetworkUDP}
	}
	return &sessionTestOutbound{Adapter: outbound.NewAdapter("session-test", tag, networks, nil)}
}

func (o *sessionTestOutbound) DialContext(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
	o.mu.Lock()
	o.dialCalls++
	o.inflight++
	if o.inflight > o.maxInflight {
		o.maxInflight = o.inflight
	}
	hook := o.dialHook
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.inflight--
		o.mu.Unlock()
	}()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	client, peer := net.Pipe()
	_ = peer.Close()
	return &sessionTestConn{Conn: client, tag: o.Tag()}, nil
}

func (o *sessionTestOutbound) ListenPacket(ctx context.Context, _ M.Socksaddr) (net.PacketConn, error) {
	o.mu.Lock()
	o.packetCalls++
	hook := o.packetHook
	o.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	return &sessionTestPacketConn{tag: o.Tag()}, nil
}

func (o *sessionTestOutbound) setDialHook(hook func(context.Context) error) {
	o.mu.Lock()
	o.dialHook = hook
	o.mu.Unlock()
}

func (o *sessionTestOutbound) counts() (dial, packet, maxInflight int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dialCalls, o.packetCalls, o.maxInflight
}

type sessionTestAddr string

func (a sessionTestAddr) Network() string { return "session-test" }
func (a sessionTestAddr) String() string  { return string(a) }

type sessionTestConn struct {
	net.Conn
	tag string
}

func (c *sessionTestConn) LocalAddr() net.Addr { return sessionTestAddr(c.tag) }

type sessionTestPacketConn struct{ tag string }

func (c *sessionTestPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}
func (c *sessionTestPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	return len(b), nil
}
func (c *sessionTestPacketConn) Close() error                     { return nil }
func (c *sessionTestPacketConn) LocalAddr() net.Addr              { return sessionTestAddr(c.tag) }
func (c *sessionTestPacketConn) SetDeadline(time.Time) error      { return nil }
func (c *sessionTestPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sessionTestPacketConn) SetWriteDeadline(time.Time) error { return nil }

func newSessionTestPool(store *routing.Store, nodes ...*sessionTestOutbound) *poolOutbound {
	if store == nil {
		store = routing.NewStore(100, time.Hour)
	}
	options := Options{
		Mode: modeSequential, SessionRouting: true, RetryEnabled: true,
		RetryAttempts: len(nodes), FailureThreshold: 100,
		Metadata: make(map[string]MemberMeta, len(nodes)),
	}
	members := make([]*memberState, 0, len(nodes))
	for _, node := range nodes {
		tag := node.Tag()
		options.Members = append(options.Members, tag)
		options.Metadata[tag] = MemberMeta{Name: tag, Identity: "identity-" + tag}
		members = append(members, &memberState{tag: tag, outbound: node, shared: &sharedMemberState{}})
	}
	options = normalizeOptions(options)
	return &poolOutbound{
		Adapter: outbound.NewAdapter(Type, "session-test-pool", []string{N.NetworkTCP, N.NetworkUDP}, options.Members),
		ctx:     context.Background(), logger: singlog.NewNOPFactory().Logger(),
		options: options, mode: options.Mode, members: members, sessionStore: store,
	}
}

func sessionTestContext(p *poolOutbound, id string) context.Context {
	return routing.WithSession(routing.WithStore(context.Background(), p.sessionStore), id)
}

func sessionTestDialTag(ctx context.Context, p *poolOutbound) (string, error) {
	conn, err := p.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.test:443"))
	if err != nil {
		return "", err
	}
	tag := conn.LocalAddr().String()
	if err := conn.Close(); err != nil {
		return "", err
	}
	return tag, nil
}

func mustSessionTestDial(t *testing.T, ctx context.Context, p *poolOutbound, want string) {
	t.Helper()
	got, err := sessionTestDialTag(ctx, p)
	if err != nil || got != want {
		t.Fatalf("dial = (%q, %v), want (%q, nil)", got, err, want)
	}
}

func sessionTestBinding(t *testing.T, store *routing.Store, id string) routing.Binding {
	t.Helper()
	for _, binding := range store.List() {
		if binding.ID == id {
			return binding
		}
	}
	t.Fatalf("session %q has no binding; bindings = %+v", id, store.List())
	return routing.Binding{}
}

func assertSessionTestSamePin(t *testing.T, got, want routing.Binding) {
	t.Helper()
	if got.ID != want.ID || got.NodeID != want.NodeID || got.NodeTag != want.NodeTag ||
		got.NodeName != want.NodeName || got.SwitchCount != want.SwitchCount || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("binding changed: got %+v, want same pin as %+v", got, want)
	}
}

func assertSessionTestHealthy(t *testing.T, member *memberState) {
	t.Helper()
	member.shared.mu.Lock()
	defer member.shared.mu.Unlock()
	if member.shared.failures != 0 || member.shared.blacklisted || member.shared.active.Load() != 0 {
		t.Fatalf("caller cancellation polluted %s: failures=%d blacklisted=%v active=%d",
			member.tag, member.shared.failures, member.shared.blacklisted, member.shared.active.Load())
	}
}

func TestSessionRoutingStableSameSessionAndIndependentSessions(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	alice, bob := "account-session-alice", "account-session-bob"
	source := &adapter.InboundContext{Source: M.ParseSocksaddr("198.51.100.10:12345")}
	aliceCtx := adapter.WithContext(sessionTestContext(p, alice), source)
	bobCtx := adapter.WithContext(sessionTestContext(p, bob), source)
	mustSessionTestDial(t, aliceCtx, p, "a")
	first := sessionTestBinding(t, p.sessionStore, alice)
	for i := 0; i < 3; i++ {
		mustSessionTestDial(t, aliceCtx, p, "a")
	}
	mustSessionTestDial(t, bobCtx, p, "b")
	mustSessionTestDial(t, aliceCtx, p, "a")
	mustSessionTestDial(t, bobCtx, p, "b")
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, alice), first)
	if got := sessionTestBinding(t, p.sessionStore, bob).NodeID; got == first.NodeID {
		t.Fatalf("distinct complete usernames from the same IP shared node ID %q", got)
	}
	if len(p.sessionStore.List()) != 2 {
		t.Fatalf("bindings = %+v, want two sessions", p.sessionStore.List())
	}
}

func TestSessionRoutingCommitsOnlySuccessfulDial(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	a.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	b.setDialHook(func(context.Context) error {
		if got := p.sessionStore.List(); len(got) != 0 {
			t.Errorf("binding committed before fallback dial succeeded: %+v", got)
		}
		return nil
	})
	mustSessionTestDial(t, sessionTestContext(p, "account-session-new"), p, "b")
	got := sessionTestBinding(t, p.sessionStore, "account-session-new")
	if got.NodeID != "identity-b" || got.NodeTag != "b" || got.SwitchCount != 0 {
		t.Fatalf("first successful binding = %+v, want b with zero switches", got)
	}
	if got.CreatedAt.IsZero() || got.LastSeen.Before(got.CreatedAt) || !got.ExpiresAt.After(got.LastSeen) {
		t.Fatalf("invalid binding lifetime: %+v", got)
	}
}

func TestSessionRoutingFailedMigrationKeepsPinUntilSuccess(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-existing")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-existing")
	fail := func(context.Context) error { return errors.New("fixture dial failure") }
	a.setDialHook(fail)
	b.setDialHook(fail)
	if _, err := sessionTestDialTag(ctx, p); err == nil {
		t.Fatal("all failed dials unexpectedly succeeded")
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
	b.setDialHook(func(context.Context) error {
		assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
		return nil
	})
	mustSessionTestDial(t, ctx, p, "b")
	after := sessionTestBinding(t, p.sessionStore, before.ID)
	if after.NodeID != "identity-b" || after.SwitchCount != before.SwitchCount+1 || !after.CreatedAt.Equal(before.CreatedAt) {
		t.Fatalf("successful migration = %+v, previous = %+v", after, before)
	}
	b.setDialHook(nil)
	mustSessionTestDial(t, ctx, p, "b")
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), after)
}

func TestSessionRoutingAllFailuresCreateNoBinding(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	for _, node := range []*sessionTestOutbound{a, b} {
		node.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	}
	p := newSessionTestPool(nil, a, b)
	if _, err := sessionTestDialTag(sessionTestContext(p, "account-session-new"), p); err == nil {
		t.Fatal("all failed dials unexpectedly succeeded")
	}
	if got := p.sessionStore.List(); len(got) != 0 {
		t.Fatalf("failed candidates created bindings: %+v", got)
	}
}

func TestSessionRoutingCallerCancellationPreservesPinAndHealth(t *testing.T) {
	for _, phase := range []string{"before_selection", "during_dial", "deadline_during_dial"} {
		t.Run(phase, func(t *testing.T) {
			a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
			p := newSessionTestPool(nil, a, b)
			base := sessionTestContext(p, "account-session-cancel")
			mustSessionTestDial(t, base, p, "a")
			before := sessionTestBinding(t, p.sessionStore, "account-session-cancel")
			ctx, cancel := context.WithCancel(base)
			wantErr := error(context.Canceled)
			switch phase {
			case "before_selection":
				cancel()
			case "during_dial":
				a.setDialHook(func(ctx context.Context) error {
					cancel()
					return ctx.Err()
				})
			case "deadline_during_dial":
				cancel()
				ctx, cancel = context.WithTimeout(base, 20*time.Millisecond)
				wantErr = context.DeadlineExceeded
				a.setDialHook(func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				})
			}
			defer cancel()
			if _, err := sessionTestDialTag(ctx, p); !errors.Is(err, wantErr) {
				t.Fatalf("cancelled dial = %v, want %v", err, wantErr)
			}
			assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
			for _, member := range p.members {
				assertSessionTestHealthy(t, member)
			}
			wantCalls := 2
			if phase == "before_selection" {
				wantCalls = 1
			}
			if calls, _, _ := a.counts(); calls != wantCalls {
				t.Fatalf("a dial calls = %d, want %d", calls, wantCalls)
			}
			if calls, _, _ := b.counts(); calls != 0 {
				t.Fatalf("caller cancellation caused %d fallback dials", calls)
			}
		})
	}
}

func TestSessionRoutingConcurrentInitializationSerializesSameKey(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx, cancel := context.WithTimeout(sessionTestContext(p, "account-session-concurrent"), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	a.setDialHook(func(ctx context.Context) error {
		blocked := false
		once.Do(func() { close(entered); blocked = true })
		if blocked {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	type result struct {
		tag string
		err error
	}
	const waiters = 12
	results := make(chan result, waiters+1)
	go func() { tag, err := sessionTestDialTag(ctx, p); results <- result{tag, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first dial did not enter outbound")
	}
	if got := p.sessionStore.List(); len(got) != 0 {
		t.Fatalf("in-flight first dial created binding: %+v", got)
	}
	var started sync.WaitGroup
	started.Add(waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			started.Done()
			tag, err := sessionTestDialTag(ctx, p)
			results <- result{tag, err}
		}()
	}
	started.Wait()
	// A second dial completing while the first is blocked proves the gate is
	// missing, regardless of which member the ordinary scheduler picked.
	select {
	case got := <-results:
		close(release)
		t.Fatalf("same-key dial overtook in-flight initialization: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for i := 0; i < waiters+1; i++ {
		select {
		case got := <-results:
			if got.err != nil || got.tag != "a" {
				t.Fatalf("concurrent dial = %+v, want a", got)
			}
		case <-ctx.Done():
			t.Fatal("same-key dials did not finish")
		}
	}
	if _, _, max := a.counts(); max != 1 {
		t.Fatalf("same-key maximum concurrent outbound dials = %d, want 1", max)
	}
	if calls, _, _ := b.counts(); calls != 0 {
		t.Fatalf("uncommitted initialization caused %d dials through b", calls)
	}
	if got := sessionTestBinding(t, p.sessionStore, "account-session-concurrent"); got.SwitchCount != 0 {
		t.Fatalf("concurrent initialization switched pin: %+v", got)
	}
}

func TestSessionRoutingGateWaitCanBeCancelled(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	base, stop := context.WithTimeout(sessionTestContext(p, "account-session-gate"), 5*time.Second)
	defer stop()
	entered, release := make(chan struct{}), make(chan struct{})
	a.setDialHook(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() { _, err := sessionTestDialTag(base, p); done <- err }()
	select {
	case <-entered:
	case <-base.Done():
		t.Fatal("first dial did not enter outbound")
	}
	ctx, cancel := context.WithTimeout(base, 20*time.Millisecond)
	defer cancel()
	if _, err := sessionTestDialTag(ctx, p); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("gate wait = %v, want deadline exceeded", err)
	}
	if calls, _, _ := a.counts(); calls != 1 {
		t.Fatalf("cancelled gate waiter reached a (%d calls)", calls)
	}
	if calls, _, _ := b.counts(); calls != 0 {
		t.Fatalf("cancelled gate waiter reached b (%d calls)", calls)
	}
	if got := p.sessionStore.List(); len(got) != 0 {
		t.Fatalf("cancelled waiter created a binding: %+v", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("initial dial = %v", err)
	}
	for _, member := range p.members {
		assertSessionTestHealthy(t, member)
	}
}

func TestSessionRoutingDifferentKeysDoNotBlockEachOther(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	entered, release := make(chan struct{}), make(chan struct{})
	aliceCtx, cancel := context.WithTimeout(sessionTestContext(p, "account-session-alice"), 5*time.Second)
	defer cancel()
	a.setDialHook(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() { _, err := sessionTestDialTag(aliceCtx, p); done <- err }()
	select {
	case <-entered:
	case <-aliceCtx.Done():
		t.Fatal("alice dial did not enter outbound")
	}
	bobCtx, stop := context.WithTimeout(sessionTestContext(p, "account-session-bob"), 250*time.Millisecond)
	defer stop()
	got, err := sessionTestDialTag(bobCtx, p)
	close(release)
	if err != nil || got != "b" {
		t.Fatalf("independent session blocked by alice: tag=%q error=%v", got, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("alice dial = %v", err)
	}
}

func TestSessionRoutingIdentitySurvivesPoolReloadAndRename(t *testing.T) {
	store := routing.NewStore(100, time.Hour)
	old := newSessionTestPool(store, newSessionTestOutbound("old-a"), newSessionTestOutbound("old-b"))
	old.options.Metadata["old-a"] = MemberMeta{Identity: "stable-a", Name: "old name"}
	mustSessionTestDial(t, sessionTestContext(old, "account-session-reload"), old, "old-a")
	before := sessionTestBinding(t, store, "account-session-reload")
	// Reverse the order and change both tags, so matching by tag or choosing a
	// fresh round-robin candidate would incorrectly use the other node.
	next := newSessionTestPool(store, newSessionTestOutbound("new-b"), newSessionTestOutbound("new-a"))
	next.options.Metadata["new-b"] = MemberMeta{Identity: "stable-b", Name: "other node"}
	next.options.Metadata["new-a"] = MemberMeta{Identity: "stable-a", Name: "renamed node"}
	mustSessionTestDial(t, sessionTestContext(next, before.ID), next, "new-a")
	after := sessionTestBinding(t, store, before.ID)
	if after.NodeID != before.NodeID || after.NodeTag != "new-a" || after.NodeName != "renamed node" ||
		after.SwitchCount != before.SwitchCount || !after.CreatedAt.Equal(before.CreatedAt) {
		t.Fatalf("reload changed stable binding: before=%+v after=%+v", before, after)
	}
}

func TestSessionRoutingUDPFallbackPreservesTCPPin(t *testing.T) {
	for _, tcpOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("tcp_only=%v", tcpOnly), func(t *testing.T) {
			a := newSessionTestOutbound("a")
			wantUDP := "a"
			if tcpOnly {
				a = newSessionTestOutbound("a", N.NetworkTCP)
				wantUDP = "b"
			}
			b := newSessionTestOutbound("b")
			p := newSessionTestPool(nil, a, b)
			ctx := sessionTestContext(p, "account-session-udp")
			mustSessionTestDial(t, ctx, p, "a")
			before := sessionTestBinding(t, p.sessionStore, "account-session-udp")
			packet, err := p.ListenPacket(ctx, M.ParseSocksaddr("example.test:53"))
			if err != nil {
				t.Fatalf("UDP listen = %v", err)
			}
			got := packet.LocalAddr().String()
			_ = packet.Close()
			if got != wantUDP {
				t.Fatalf("UDP member = %q, want %q", got, wantUDP)
			}
			assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
			mustSessionTestDial(t, ctx, p, "a")
			for _, member := range p.members {
				assertSessionTestHealthy(t, member)
			}
		})
	}
}

func TestSessionRoutingLegacyStickyAndOrdinaryScheduling(t *testing.T) {
	t.Run("legacy_source_ip_sticky", func(t *testing.T) {
		p := newSessionTestPool(nil, newSessionTestOutbound("a"), newSessionTestOutbound("b"))
		p.options.SessionRouting = false
		p.options.Sticky, p.sticky = true, true
		p.stickyMap = make(map[string]string)
		for _, id := range []string{"account-session-one", "account-session-two", "account-session-one"} {
			ctx := adapter.WithContext(sessionTestContext(p, id), &adapter.InboundContext{Source: M.ParseSocksaddr("198.51.100.10:12345")})
			mustSessionTestDial(t, ctx, p, "a")
		}
		if got := p.stickyMap["198.51.100.10"]; got != "a" {
			t.Fatalf("legacy source IP pin = %q, want a", got)
		}
		if got := p.sessionStore.List(); len(got) != 0 {
			t.Fatalf("legacy sticky unexpectedly created session bindings: %+v", got)
		}
	})
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordinary_session_routing=%v", enabled), func(t *testing.T) {
			p := newSessionTestPool(nil, newSessionTestOutbound("a"), newSessionTestOutbound("b"))
			p.options.SessionRouting = enabled
			ctx := context.Background()
			if !enabled {
				// An explicit session marker has no effect on an ordinary pool.
				ctx = sessionTestContext(p, "account-session-disabled")
			}
			for _, want := range []string{"a", "b", "a", "b"} {
				mustSessionTestDial(t, ctx, p, want)
			}
			if got := p.sessionStore.List(); len(got) != 0 {
				t.Fatalf("ordinary scheduling created session bindings: %+v", got)
			}
		})
	}
}

// A session pinned to a node that stops answering must not be trapped there:
// the pinned failure proves the binding stale, so the same request re-routes
// even when retries are disabled, and the successful dial migrates the pin.
func TestSessionRoutingPinnedFailureReroutesWithoutRetry(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	p.options.RetryEnabled = false
	ctx := sessionTestContext(p, "account-session-stale-pin")
	mustSessionTestDial(t, ctx, p, "a")
	a.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	mustSessionTestDial(t, ctx, p, "b")
	got := sessionTestBinding(t, p.sessionStore, "account-session-stale-pin")
	if got.NodeID != "identity-b" || got.NodeTag != "b" || got.SwitchCount != 1 || got.LastReason != "dial_failed" {
		t.Fatalf("stale pin was not migrated: %+v", got)
	}
	// The healthiest node is preferred first; a is only retried after b fails.
	a.setDialHook(nil)
	mustSessionTestDial(t, ctx, p, "b")
	if calls, _, _ := a.counts(); calls != 2 {
		t.Fatalf("failed pin caused %d dials through a, want 2", calls)
	}
}

// A single-member pool has nothing to re-route to, so the request still fails
// once instead of looping on the broken node, and no binding is fabricated.
func TestSessionRoutingPinnedFailureSingleMemberDoesNotLoop(t *testing.T) {
	a := newSessionTestOutbound("a")
	p := newSessionTestPool(nil, a)
	p.options.RetryEnabled = false
	ctx := sessionTestContext(p, "account-session-single")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-single")
	a.setDialHook(func(context.Context) error { return errors.New("fixture dial failure") })
	if _, err := sessionTestDialTag(ctx, p); err == nil {
		t.Fatal("single-member pool dial unexpectedly succeeded")
	}
	if calls, _, _ := a.counts(); calls != 2 {
		t.Fatalf("a dial calls = %d, want 2", calls)
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
}

// Lookups are observational: only a successful dial renews a binding, so a
// session whose every dial fails can idle out instead of living forever.
func TestSessionRoutingFailedLookupDoesNotExtendIdleTTL(t *testing.T) {
	a, b := newSessionTestOutbound("a"), newSessionTestOutbound("b")
	p := newSessionTestPool(nil, a, b)
	ctx := sessionTestContext(p, "account-session-idle")
	mustSessionTestDial(t, ctx, p, "a")
	before := sessionTestBinding(t, p.sessionStore, "account-session-idle")
	fail := func(context.Context) error { return errors.New("fixture dial failure") }
	a.setDialHook(fail)
	b.setDialHook(fail)
	for i := 0; i < 3; i++ {
		if _, err := sessionTestDialTag(ctx, p); err == nil {
			t.Fatal("all failed dials unexpectedly succeeded")
		}
	}
	assertSessionTestSamePin(t, sessionTestBinding(t, p.sessionStore, before.ID), before)
}
