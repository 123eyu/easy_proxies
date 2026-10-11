package pool

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/monitor"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func localProbeResponse(t *testing.T, response string) (net.Conn, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		request, err := http.ReadRequest(bufio.NewReader(server))
		if err != nil {
			return
		}
		request.Body.Close()
		_, _ = io.WriteString(server, response)
	}()
	t.Cleanup(func() {
		client.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("local response worker leaked")
		}
	})
	return client, done
}
func TestHTTPProbeRequiresValidSuccessfulStatusAndBoundedHeaders(t *testing.T) {
	for _, test := range []struct {
		name, response string
		ok             bool
	}{
		{"204", "HTTP/1.1 204 No Content\r\n\r\n", true},
		{"200", "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", true},
		{"redirect", "HTTP/1.1 302 Found\r\nLocation: /\r\nContent-Length: 0\r\n\r\n", true},
		{"authentication", "HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n", false},
		{"rate-limit", "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n", false},
		{"unavailable", "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n", false},
		{"garbage", "garbage\r\n\r\n", false},
		{"oversized-header", "HTTP/1.1 200 OK\r\nX-Fixture: " + strings.Repeat("x", maxProbeHeaderBytes) + "\r\n\r\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := localProbeResponse(t, test.response)
			_, err := httpProbe(conn, "fixture.test")
			if (err == nil) != test.ok {
				t.Fatalf("httpProbe error = %v, success want %v", err, test.ok)
			}
		})
	}
}
func TestHTTPProbeDoesNotWaitForBody(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		request, err := http.ReadRequest(bufio.NewReader(server))
		if err != nil {
			return
		}
		request.Body.Close()
		_, _ = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
		<-stop
	}()
	defer func() { close(stop); <-done }()
	result := make(chan error, 1)
	go func() { _, err := httpProbe(client, "fixture.test"); result <- err }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe waited for response body")
	}
}
func TestManualBlacklistSurvivesProbeAndPoolRecovery(t *testing.T) {
	tag := "fixture-manual-ban"
	state := acquireSharedState(tag)
	defer sharedStateStore.Delete(tag)
	blacklistSharedMember(tag, time.Hour)
	if state.releaseIfPermanent(time.Now()) {
		t.Fatal("automatic health success released an operator ban")
	}
	p := newTestPool(state)
	if p.releaseIfAllBlacklistedLocked(time.Now()) {
		t.Fatal("single-member pool recovery released an operator ban")
	}
	// A concurrent fault must not turn an operator ban into an auto-releasable one.
	state.recordFailure(errors.New("handshake failure"), failurePolicy{Threshold: 1, BlacklistDuration: 2 * time.Hour})
	if state.releaseIfPermanent(time.Now()) {
		t.Fatal("concurrent failure lost operator ban policy")
	}
	state.forceRelease()
	if state.isBlacklisted(time.Now()) {
		t.Fatal("explicit release did not clear operator ban")
	}
	blacklistSharedMember(tag, time.Hour)
	if !state.releaseIfPermanent(time.Now().Add(2 * time.Hour)) {
		t.Fatal("expired operator ban did not recover")
	}
}
func TestBalanceRotatesTiesWithoutSelectingBusierMembers(t *testing.T) {
	p := newSessionTestPool(nil, newSessionTestOutbound("a"), newSessionTestOutbound("b"), newSessionTestOutbound("c"))
	p.mode = modeBalance
	p.members[2].shared.active.Store(5)
	counts := map[string]int{}
	for i := 0; i < 30; i++ {
		counts[p.selectByMode(p.members).tag]++
	}
	if counts["a"] != 15 || counts["b"] != 15 || counts["c"] != 0 {
		t.Fatalf("imbalanced tie selections: %v", counts)
	}
	p.members[0].shared.active.Store(2)
	for i := 0; i < 5; i++ {
		if selected := p.selectByMode(p.members); selected.tag != "b" {
			t.Fatalf("selected busier member: %s", selected.tag)
		}
	}
}
func TestSessionIdentityIndexRespectsTriedHealthAndProtocol(t *testing.T) {
	a, b := newSessionTestOutbound("a", N.NetworkTCP), newSessionTestOutbound("b", N.NetworkTCP, N.NetworkUDP)
	p := newSessionTestPool(nil, a, b)
	p.indexMembersLocked()
	member, err := p.pickMemberForSession(N.NetworkTCP, nil, "identity-a")
	if err != nil || member.tag != "a" {
		t.Fatalf("healthy indexed pin = %v, %v", member, err)
	}
	for _, test := range []struct {
		name, network string
		tried         map[string]bool
		ban           bool
	}{
		{"tried", N.NetworkTCP, map[string]bool{"a": true}, false}, {"udp-unsupported", N.NetworkUDP, nil, false}, {"manual-ban", N.NetworkTCP, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.ban {
				p.members[0].shared.mu.Lock()
				p.members[0].shared.blacklisted = true
				p.members[0].shared.blacklistedUntil = time.Now().Add(time.Hour)
				p.members[0].shared.manualUntil = p.members[0].shared.blacklistedUntil
				p.members[0].shared.mu.Unlock()
			}
			member, err := p.pickMemberForSession(test.network, test.tried, "identity-a")
			if err != nil || member.tag != "b" {
				t.Fatalf("ineligible indexed pin was used: %v, %v", member, err)
			}
		})
	}
}

type localProbeOutbound struct {
	*sessionTestOutbound
	conn net.Conn
}

func (o *localProbeOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return o.conn, nil
}
func TestCancelledProbeDoesNotRecordNodeFailure(t *testing.T) {
	mgr, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Stop()
	entry := mgr.Register(monitor.NodeInfo{Tag: "fixture"})
	client, server := net.Pipe()
	defer server.Close()
	outbound := &localProbeOutbound{sessionTestOutbound: newSessionTestOutbound("fixture"), conn: client}
	member := &memberState{outbound: outbound, tag: "fixture", entry: entry, shared: &sharedMemberState{}}
	p := newSessionTestPool(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.probeMember(ctx, member, M.ParseSocksaddr("fixture.test:80"), "fixture.test", false); err == nil {
		t.Fatal("cancelled probe succeeded")
	}
	if snapshot := mgr.Snapshot()[0]; snapshot.FailureCount != 0 || snapshot.SuccessCount != 0 || snapshot.LastError != "" {
		t.Fatalf("cancelled probe changed node counters: %+v", snapshot)
	}
}

func BenchmarkSessionIdentitySelection(b *testing.B) {
	for _, count := range []int{32, 1024, 8192} {
		b.Run(fmt.Sprintf("nodes=%d", count), func(b *testing.B) {
			nodes := make([]*sessionTestOutbound, count)
			for i := range nodes {
				nodes[i] = newSessionTestOutbound(fmt.Sprintf("node-%d", i))
			}
			p := newSessionTestPool(nil, nodes...)
			p.indexMembersLocked()
			identity := p.memberIdentity(p.members[count-1])
			for _, indexed := range []bool{false, true} {
				name := "normal-scan"
				pin := ""
				if indexed {
					name = "named-session-index"
					pin = identity
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := p.pickMemberForSession(N.NetworkTCP, nil, pin); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func TestClosingPoolCannotRemoveReplacementDialer(t *testing.T) {
	old := newSessionTestPool(nil, newSessionTestOutbound("old"))
	replacement := newSessionTestPool(nil, newSessionTestOutbound("new"))
	tag := old.Tag()
	registerDialer(tag, old)
	registerDialer(tag, replacement)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	dialer, exists := GetDialer(tag)
	if !exists || dialer.(*poolDialerAdapter).pool != replacement {
		t.Fatal("closing old pool removed its replacement")
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	if _, exists := GetDialer(tag); exists {
		t.Fatal("closed pool retained a registered dialer")
	}
}
