package boxmgr

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
	"easy_proxies/internal/routing"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/protocol/socks/socks5"
)

// A local HTTP proxy accepts CONNECT, then serves a synthetic HTTP destination
// from the tunnel. Neither the proxy nor the destination touches the Internet.
type sessionLocalUpstream struct {
	listener net.Listener
	node     string
	reject   atomic.Bool
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func newSessionLocalUpstream(t *testing.T, node string) *sessionLocalUpstream {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	u := &sessionLocalUpstream{listener: listener, node: node, conns: make(map[net.Conn]struct{})}
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			u.mu.Lock()
			u.conns[conn] = struct{}{}
			u.mu.Unlock()
			u.wg.Add(1)
			go u.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		u.mu.Lock()
		for conn := range u.conns {
			_ = conn.Close()
		}
		u.mu.Unlock()
		u.wg.Wait()
	})
	return u
}

func (u *sessionLocalUpstream) URI() string { return "http://" + u.listener.Addr().String() }

func (u *sessionLocalUpstream) serve(conn net.Conn) {
	defer u.wg.Done()
	defer func() {
		_ = conn.Close()
		u.mu.Lock()
		delete(u.conns, conn)
		u.mu.Unlock()
	}()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	connect, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	_ = connect.Body.Close()
	if connect.Method != http.MethodConnect {
		_, _ = io.WriteString(conn, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	if u.reject.Load() {
		_, _ = io.WriteString(conn, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	request, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	_ = request.Body.Close()
	// Expose a credential leak to the test as an unexpected body, not a log.
	body := u.node
	if request.Header.Get("Proxy-Authorization") != "" {
		body = "proxy-authorization-leaked"
	}
	_ = (&http.Response{
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, StatusCode: http.StatusOK,
		Header: make(http.Header), ContentLength: int64(len(body)), Close: true,
		Body: io.NopCloser(strings.NewReader(body)),
	}).Write(conn)
}

func sessionLocalPort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return uint16(port)
}

func startSessionLocalManager(t *testing.T, a, b *sessionLocalUpstream, sessionEnabled *bool) (*Manager, *config.Config) {
	t.Helper()
	disableManagement := false
	cfg := &config.Config{
		Mode: "pool", LogLevel: "error",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: sessionLocalPort(t), Username: "wang", Password: "1234"},
		Pool:       config.PoolConfig{Mode: "sequential", FailureThreshold: 100, RetryAttempts: 2},
		Sticky:     config.StickyConfig{SessionEnabled: sessionEnabled, SessionTTL: time.Hour, MaxSessions: 100},
		Management: config.ManagementConfig{Enabled: &disableManagement},
		Nodes:      []config.NodeConfig{{Name: "node-a", URI: a.URI()}, {Name: "node-b", URI: b.URI()}},
	}
	m := New(cfg, monitor.Config{Enabled: false, ProbeTarget: ""})
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("manager close: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := m.Start(ctx); err != nil {
		t.Fatalf("local session manager start: %v", err)
	}
	return m, cfg
}

func sessionLocalDial(t *testing.T, cfg *config.Config) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(cfg.Listener.Port))), 2*time.Second)
	if err != nil {
		t.Fatalf("dial local ingress: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn
}

func sessionLocalHTTPResponse(conn net.Conn, reader *bufio.Reader, username, password string, closeConn bool) (*http.Response, error) {
	closeHeader := ""
	if closeConn {
		closeHeader = "Connection: close\r\n"
	}
	credentials := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	_, err := fmt.Fprintf(conn, "GET http://session-fixture.test/ HTTP/1.1\r\nHost: session-fixture.test\r\nProxy-Authorization: Basic %s\r\n%s\r\n", credentials, closeHeader)
	if err != nil {
		return nil, err
	}
	return http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
}

func sessionLocalBody(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read local response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("local destination status = %d, body = %q", response.StatusCode, body)
	}
	return string(body)
}

func sessionLocalHTTPNode(t *testing.T, cfg *config.Config, username string) string {
	t.Helper()
	conn := sessionLocalDial(t, cfg)
	defer conn.Close()
	response, err := sessionLocalHTTPResponse(conn, bufio.NewReader(conn), username, "1234", true)
	if err != nil {
		t.Fatalf("HTTP proxy request: %v", err)
	}
	return sessionLocalBody(t, response)
}

func sessionLocalTunnelNode(t *testing.T, conn net.Conn, reader *bufio.Reader) string {
	t.Helper()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: session-fixture.test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatalf("write tunneled request: %v", err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read tunneled response: %v", err)
	}
	return sessionLocalBody(t, response)
}

func sessionLocalCONNECTNode(t *testing.T, cfg *config.Config, username string) string {
	t.Helper()
	conn := sessionLocalDial(t, cfg)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	credentials := base64.StdEncoding.EncodeToString([]byte(username + ":1234"))
	if _, err := fmt.Fprintf(conn, "CONNECT session-fixture.test:80 HTTP/1.1\r\nHost: session-fixture.test:80\r\nProxy-Authorization: Basic %s\r\n\r\n", credentials); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT handshake: response=%+v error=%v", response, err)
	}
	return sessionLocalTunnelNode(t, conn, reader)
}

var errSessionLocalSOCKSAuth = errors.New("SOCKS authentication rejected")

func sessionLocalSOCKSAuthenticate(conn net.Conn, reader *bufio.Reader, username, password string) error {
	if _, err := conn.Write([]byte{5, 1, 2}); err != nil {
		return err
	}
	var response [2]byte
	if _, err := io.ReadFull(reader, response[:]); err != nil {
		return err
	}
	if response != [2]byte{5, 2} {
		return fmt.Errorf("SOCKS auth method = %v", response)
	}
	request := append([]byte{1, byte(len(username))}, username...)
	request = append(request, byte(len(password)))
	request = append(request, password...)
	if _, err := conn.Write(request); err != nil {
		return err
	}
	if _, err := io.ReadFull(reader, response[:]); err != nil {
		return err
	}
	if response != [2]byte{1, 0} {
		return errSessionLocalSOCKSAuth
	}
	return nil
}

func sessionLocalSOCKSNode(t *testing.T, cfg *config.Config, username string) string {
	t.Helper()
	conn := sessionLocalDial(t, cfg)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := sessionLocalSOCKSAuthenticate(conn, reader, username, "1234"); err != nil {
		t.Fatalf("SOCKS authentication: %v", err)
	}
	if err := socks5.WriteRequest(conn, socks5.Request{Command: socks5.CommandConnect, Destination: M.ParseSocksaddr("session-fixture.test:80")}); err != nil {
		t.Fatalf("SOCKS connect request: %v", err)
	}
	response, err := socks5.ReadResponse(reader)
	if err != nil || response.ReplyCode != socks5.ReplyCodeSuccess {
		t.Fatalf("SOCKS connect handshake: response=%+v error=%v", response, err)
	}
	return sessionLocalTunnelNode(t, conn, reader)
}

func sessionLocalBinding(t *testing.T, m *Manager, username string) routing.Binding {
	t.Helper()
	for _, binding := range m.routingStore.List() {
		if binding.ID == username {
			return binding
		}
	}
	t.Fatalf("session %q has no binding: %+v", username, m.routingStore.List())
	return routing.Binding{}
}

func TestSessionRoutingLocalHTTPConnectAndSOCKS(t *testing.T) {
	a, b := newSessionLocalUpstream(t, "node-a"), newSessionLocalUpstream(t, "node-b")
	m, cfg := startSessionLocalManager(t, a, b, nil)
	// Exact base credentials retain ordinary pool scheduling and create no pin.
	for _, want := range []string{"node-a", "node-b"} {
		if got := sessionLocalHTTPNode(t, cfg, "wang"); got != want {
			t.Fatalf("base account scheduled %q, want %q", got, want)
		}
	}
	if got := m.routingStore.List(); len(got) != 0 {
		t.Fatalf("base account created sessions: %+v", got)
	}
	for _, request := range []struct {
		name string
		call func(*testing.T, *config.Config, string) string
	}{
		{"http", sessionLocalHTTPNode},
		{"connect", sessionLocalCONNECTNode},
		{"socks5", sessionLocalSOCKSNode},
		{"http_repeat", sessionLocalHTTPNode},
	} {
		t.Run(request.name, func(t *testing.T) {
			if got := request.call(t, cfg, "wang.abc"); got != "node-a" {
				t.Fatalf("same session via %s used %q, want node-a", request.name, got)
			}
		})
	}
	if got := sessionLocalHTTPNode(t, cfg, "wang.def"); got != "node-b" {
		t.Fatalf("independent session used %q, want node-b", got)
	}
	// Authentication and session metadata must be reevaluated on every request,
	// even when different usernames share one HTTP keep-alive connection.
	conn := sessionLocalDial(t, cfg)
	reader := bufio.NewReader(conn)
	for _, request := range []struct{ username, want string }{{"wang.abc", "node-a"}, {"wang.def", "node-b"}, {"wang.abc", "node-a"}} {
		response, err := sessionLocalHTTPResponse(conn, reader, request.username, "1234", false)
		if err != nil {
			_ = conn.Close()
			t.Fatalf("keep-alive request for %s: %v", request.username, err)
		}
		if got := sessionLocalBody(t, response); got != request.want {
			_ = conn.Close()
			t.Fatalf("keep-alive request for %s used %q, want %q", request.username, got, request.want)
		}
	}
	_ = conn.Close()
	before := sessionLocalBinding(t, m, "wang.abc")
	if before.NodeName != "node-a" || before.SwitchCount != 0 {
		t.Fatalf("initial real-socket binding = %+v", before)
	}
	// A failing pinned upstream must fail over once and commit the successful
	// node, even though the old node has not hit its blacklist threshold.
	a.reject.Store(true)
	if got := sessionLocalHTTPNode(t, cfg, "wang.abc"); got != "node-b" {
		t.Fatalf("failed upstream fallback used %q, want node-b", got)
	}
	a.reject.Store(false)
	if got := sessionLocalSOCKSNode(t, cfg, "wang.abc"); got != "node-b" {
		t.Fatalf("successful fallback was not retained: %q", got)
	}
	after := sessionLocalBinding(t, m, "wang.abc")
	if after.NodeID == before.NodeID || after.SwitchCount != 1 {
		t.Fatalf("fallback binding did not record one switch: before=%+v after=%+v", before, after)
	}
	// The manager owns the store across reloads; changed display names/tags do
	// not invalidate the upstream URI's stable identity.
	reloaded := *cfg
	reloaded.Nodes = []config.NodeConfig{{Name: "renamed-a", URI: a.URI()}, {Name: "renamed-b", URI: b.URI()}}
	if err := m.Reload(&reloaded); err != nil {
		t.Fatalf("reload local manager: %v", err)
	}
	if got := sessionLocalCONNECTNode(t, &reloaded, "wang.abc"); got != "node-b" {
		t.Fatalf("reload lost successful node identity: %q", got)
	}
	renamed := sessionLocalBinding(t, m, "wang.abc")
	if renamed.NodeID != after.NodeID || renamed.NodeName != "renamed-b" || renamed.SwitchCount != 1 || !renamed.CreatedAt.Equal(after.CreatedAt) {
		t.Fatalf("reload did not preserve stable binding: before=%+v after=%+v", after, renamed)
	}
}

func TestSessionRoutingLocalAuthenticationRejectsInvalidCredentials(t *testing.T) {
	a, b := newSessionLocalUpstream(t, "node-a"), newSessionLocalUpstream(t, "node-b")
	m, cfg := startSessionLocalManager(t, a, b, nil)
	for _, credentials := range []struct{ username, password string }{
		{"wang.abc", "wrong-password"}, {"different.abc", "1234"},
		{"wang.", "1234"}, {"wang.a.b", "1234"},
	} {
		t.Run(credentials.username, func(t *testing.T) {
			conn := sessionLocalDial(t, cfg)
			response, err := sessionLocalHTTPResponse(conn, bufio.NewReader(conn), credentials.username, credentials.password, true)
			if err != nil {
				_ = conn.Close()
				t.Fatalf("invalid HTTP credentials response: %v", err)
			}
			_ = response.Body.Close()
			_ = conn.Close()
			if response.StatusCode != http.StatusProxyAuthRequired || response.Header.Get("Proxy-Authenticate") == "" {
				t.Fatalf("invalid credentials status=%d challenge=%q", response.StatusCode, response.Header.Get("Proxy-Authenticate"))
			}
			conn = sessionLocalDial(t, cfg)
			err = sessionLocalSOCKSAuthenticate(conn, bufio.NewReader(conn), credentials.username, credentials.password)
			_ = conn.Close()
			if !errors.Is(err, errSessionLocalSOCKSAuth) {
				t.Fatalf("invalid SOCKS credentials accepted: %v", err)
			}
		})
	}
	if got := m.routingStore.List(); len(got) != 0 {
		t.Fatalf("rejected credentials created session bindings: %+v", got)
	}
	if got := sessionLocalHTTPNode(t, cfg, "wang.abc"); got != "node-a" {
		t.Fatalf("rejected credentials affected pool scheduling: %q", got)
	}
}

func TestSessionRoutingLocalExplicitDisableRetainsBaseAccount(t *testing.T) {
	a, b := newSessionLocalUpstream(t, "node-a"), newSessionLocalUpstream(t, "node-b")
	disabled := false
	m, cfg := startSessionLocalManager(t, a, b, &disabled)
	for _, request := range []struct {
		call func(*testing.T, *config.Config, string) string
		want string
	}{{sessionLocalHTTPNode, "node-a"}, {sessionLocalCONNECTNode, "node-b"}, {sessionLocalSOCKSNode, "node-a"}} {
		if got := request.call(t, cfg, "wang"); got != request.want {
			t.Fatalf("disabled-session base request used %q, want %q", got, request.want)
		}
	}
	conn := sessionLocalDial(t, cfg)
	response, err := sessionLocalHTTPResponse(conn, bufio.NewReader(conn), "wang.abc", "1234", true)
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_ = response.Body.Close()
	_ = conn.Close()
	if response.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("disabled session username status = %d, want 407", response.StatusCode)
	}
	if got := m.routingStore.List(); len(got) != 0 {
		t.Fatalf("disabled feature created bindings: %+v", got)
	}
}
