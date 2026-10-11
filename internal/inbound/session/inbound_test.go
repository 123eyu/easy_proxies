package session

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/routing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks/socks5"
)

type testRouter struct {
	adapter.Router
	tcp func(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc)
	udp func(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc)
}

func (r *testRouter) RouteConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go r.tcp(ctx, conn, metadata, onClose)
}

func (r *testRouter) RoutePacketConnectionEx(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	go r.udp(ctx, conn, metadata, onClose)
}

type routeResult struct {
	user, session, authorization string
}

func startTestConnection(t *testing.T, router *testRouter, anonymous bool, users ...auth.User) (net.Conn, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	server, err := ln.Accept()
	ln.Close()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	client.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { client.Close(); server.Close() })
	h := &Inbound{Adapter: inbound.NewAdapter(Type, "test-in"), router: router, sessionEnabled: true}
	if !anonymous {
		h.users = []auth.User{{Username: "wang.name", Password: "secret"}}
	}
	if len(users) > 0 {
		h.users = append([]auth.User(nil), users...)
	}
	closed := make(chan error, 1)
	go h.NewConnectionEx(context.Background(), server, adapter.InboundContext{Source: M.SocksaddrFromNet(server.RemoteAddr()).Unwrap()}, func(err error) { closed <- err })
	return client, closed
}

func basicHeader(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

func TestHTTPPerRequestSessionIsolation(t *testing.T) {
	results := make(chan routeResult, 3)
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		defer req.Body.Close()
		results <- routeResult{md.User, routing.SessionFromContext(ctx), req.Header.Get("Proxy-Authorization")}
		body := routing.SessionFromContext(ctx)
		fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	}}
	client, _ := startTestConnection(t, router, false)
	reader := bufio.NewReader(client)
	for _, username := range []string{"wang.name.first", "wang.name", "wang.name.second"} {
		fmt.Fprintf(client, "GET http://example.com/path HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\n\r\n", basicHeader(username, "secret"))
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("response status %d, body error %v", resp.StatusCode, err)
		}
		wantSession := username
		if username == "wang.name" {
			wantSession = ""
		}
		if string(body) != wantSession {
			t.Fatalf("response used session %q, want %q", body, wantSession)
		}
		got := <-results
		if got.user != username || got.session != wantSession || got.authorization != "" {
			t.Fatalf("incorrect routed identity or leaked credentials: %+v", got)
		}
	}
}

func TestHTTPAuthenticationRejectedWithoutLeakingCredentials(t *testing.T) {
	for i, header := range []string{
		"", basicHeader("wang.name.xyz", "incorrect"), basicHeader("wang.name.", "secret"),
		basicHeader("wang.name.bad.dot", "secret"), basicHeader("other.xyz", "secret"), "Basic invalid-base64",
	} {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			client, closed := startTestConnection(t, &testRouter{tcp: func(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc) {
				t.Error("unauthenticated request was routed")
			}}, false)
			fmt.Fprintf(client, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\n\r\n", header)
			resp, err := http.ReadResponse(bufio.NewReader(client), nil)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 407 || resp.Header.Get("Proxy-Authenticate") == "" {
				t.Fatalf("expected authentication challenge, got %d", resp.StatusCode)
			}
			gotErr := <-closed
			if gotErr == nil || strings.Contains(gotErr.Error(), "secret") || strings.Contains(gotErr.Error(), "wang") || strings.Contains(string(body), header) && header != "" {
				t.Fatalf("authentication failure leaked credentials or omitted error: %v", gotErr)
			}
		})
	}
}

func TestCONNECTPreservesPipelinedPayload(t *testing.T) {
	results := make(chan routeResult, 1)
	router := echoRouter(results)
	client, _ := startTestConnection(t, router, false)
	payload := "buffered-TLS-payload"
	fmt.Fprintf(client, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n%s", basicHeader("wang.name.sessionA", "secret"), payload)
	reader := bufio.NewReader(client)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("CONNECT failed: response %v, error %v", resp, err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != payload {
		t.Fatalf("lost pipelined CONNECT payload: %q, %v", got, err)
	}
	identity := <-results
	if identity.user != "wang.name.sessionA" || identity.session != identity.user {
		t.Fatalf("wrong CONNECT identity: %+v", identity)
	}
}

func echoRouter(results chan<- routeResult) *testRouter {
	return &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		if onClose != nil {
			defer onClose(nil)
		}
		results <- routeResult{user: md.User, session: routing.SessionFromContext(ctx)}
		if err := N.ReportConnHandshakeSuccess(conn, conn); err != nil {
			return
		}
		io.Copy(conn, conn)
	}}
}

func negotiateSOCKS5(t *testing.T, client net.Conn, username, password string, anonymous bool) *bufio.Reader {
	t.Helper()
	method := socks5.AuthTypeUsernamePassword
	if anonymous {
		method = socks5.AuthTypeNotRequired
	}
	if err := socks5.WriteAuthRequest(client, socks5.AuthRequest{Methods: []byte{method}}); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	response, err := socks5.ReadAuthResponse(reader)
	if err != nil || response.Method != method {
		t.Fatalf("SOCKS5 method negotiation: %+v, %v", response, err)
	}
	if !anonymous {
		socks5.WriteUsernamePasswordAuthRequest(client, socks5.UsernamePasswordAuthRequest{Username: username, Password: password})
		response, err := socks5.ReadUsernamePasswordAuthResponse(reader)
		if err != nil || response.Status != socks5.UsernamePasswordStatusSuccess {
			t.Fatalf("SOCKS5 authentication: %+v, %v", response, err)
		}
	}
	return reader
}

func TestSOCKS5ConnectPreservesSessionAndBufferedPayload(t *testing.T) {
	for _, anonymous := range []bool{false, true} {
		t.Run(fmt.Sprint("anonymous=", anonymous), func(t *testing.T) {
			results := make(chan routeResult, 1)
			client, _ := startTestConnection(t, echoRouter(results), anonymous)
			reader := negotiateSOCKS5(t, client, "wang.name.sessionB", "secret", anonymous)
			var request bytes.Buffer
			socks5.WriteRequest(&request, socks5.Request{Command: socks5.CommandConnect, Destination: M.ParseSocksaddr("example.com:80")})
			payload := "pipelined-socks-payload"
			request.WriteString(payload)
			client.Write(request.Bytes())
			response, err := socks5.ReadResponse(reader)
			if err != nil || response.ReplyCode != socks5.ReplyCodeSuccess {
				t.Fatalf("SOCKS5 CONNECT: %+v, %v", response, err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(reader, got); err != nil || string(got) != payload {
				t.Fatalf("lost SOCKS5 payload: %q, %v", got, err)
			}
			identity := <-results
			want := "wang.name.sessionB"
			if anonymous {
				want = ""
			}
			if identity.user != want || identity.session != want {
				t.Fatalf("wrong routed SOCKS5 identity: %+v", identity)
			}
		})
	}
}

func TestSOCKS5AuthenticationFailure(t *testing.T) {
	for _, username := range []string{"wang.name.sessionC", "wang.name.", "other.sessionC"} {
		t.Run(username, func(t *testing.T) {
			client, closed := startTestConnection(t, &testRouter{}, false)
			reader := bufio.NewReader(client)
			socks5.WriteAuthRequest(client, socks5.AuthRequest{Methods: []byte{socks5.AuthTypeUsernamePassword}})
			socks5.ReadAuthResponse(reader)
			password := "secret"
			if username == "wang.name.sessionC" {
				password = "incorrect"
			}
			socks5.WriteUsernamePasswordAuthRequest(client, socks5.UsernamePasswordAuthRequest{Username: username, Password: password})
			response, err := socks5.ReadUsernamePasswordAuthResponse(reader)
			if err != nil || response.Status != socks5.UsernamePasswordStatusFailure {
				t.Fatalf("expected SOCKS5 authentication failure: %+v, %v", response, err)
			}
			gotErr := <-closed
			if gotErr == nil || gotErr.Error() != errAuthentication.Error() {
				t.Fatalf("expected generic authentication error, got %v", gotErr)
			}
		})
	}
}

func TestSOCKS5NoAcceptedMethods(t *testing.T) {
	client, _ := startTestConnection(t, &testRouter{}, false)
	socks5.WriteAuthRequest(client, socks5.AuthRequest{Methods: []byte{socks5.AuthTypeNotRequired}})
	response, err := socks5.ReadAuthResponse(bufio.NewReader(client))
	if err != nil || response.Method != socks5.AuthTypeNoAcceptedMethods {
		t.Fatalf("expected no acceptable authentication method: %+v, %v", response, err)
	}
}

func TestSOCKS5UDPAssociateSessionAndControlLifetime(t *testing.T) {
	results := make(chan routeResult, 1)
	packetClosed := make(chan struct{})
	router := &testRouter{udp: func(ctx context.Context, conn N.PacketConn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer close(packetClosed)
		defer conn.Close()
		if onClose != nil {
			defer onClose(nil)
		}
		results <- routeResult{user: md.User, session: routing.SessionFromContext(ctx)}
		for {
			packet := buf.NewPacket()
			packet.Advance(256)
			destination, err := conn.ReadPacket(packet)
			if err != nil {
				packet.Release()
				return
			}
			if err := conn.WritePacket(packet, destination); err != nil {
				return
			}
		}
	}}
	client, _ := startTestConnection(t, router, false)
	reader := negotiateSOCKS5(t, client, "wang.name.udpSession", "secret", false)
	socks5.WriteRequest(client, socks5.Request{Command: socks5.CommandUDPAssociate, Destination: M.ParseSocksaddr("0.0.0.0:0")})
	response, err := socks5.ReadResponse(reader)
	if err != nil || response.ReplyCode != socks5.ReplyCodeSuccess {
		t.Fatalf("UDP associate response: %+v, %v", response, err)
	}
	udp, err := net.Dial("udp", response.Bind.String())
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	udp.SetDeadline(time.Now().Add(5 * time.Second))
	var packet bytes.Buffer
	packet.Write([]byte{0, 0, 0})
	M.SocksaddrSerializer.WriteAddrPort(&packet, M.ParseSocksaddr("example.com:53"))
	packet.WriteString("udp-payload")
	udp.Write(packet.Bytes())
	got := make([]byte, 512)
	n, err := udp.Read(got)
	if err != nil || !bytes.Equal(got[:n], packet.Bytes()) {
		t.Fatalf("UDP payload mismatch: %q, %v", got[:n], err)
	}
	identity := <-results
	if identity.user != "wang.name.udpSession" || identity.session != identity.user {
		t.Fatalf("wrong routed UDP identity: %+v", identity)
	}
	client.Close()
	select {
	case <-packetClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("UDP association remained alive after TCP control closed")
	}
}

func TestHTTPOversizedHeadersRejected(t *testing.T) {
	client, closed := startTestConnection(t, &testRouter{}, false)
	request := "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nX-Large: " + strings.Repeat("a", maxHTTPHeaderBytes) + "\r\n\r\n"
	// A server may reject before the final bytes have been sent.
	go io.WriteString(client, request)
	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("expected 431 for oversized headers, got %d", resp.StatusCode)
	}
	if err := <-closed; err != errHTTPHeadersTooLarge {
		t.Fatalf("expected sanitized header limit error, got %v", err)
	}
}

func TestHTTPPOSTBodyExceedingHeaderLimit(t *testing.T) {
	payload := strings.Repeat("body-data", maxHTTPHeaderBytes/4)
	results := make(chan string, 1)
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		body, _ := io.ReadAll(req.Body)
		req.Body.Close()
		results <- string(body)
		fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	}}
	client, _ := startTestConnection(t, router, false)
	fmt.Fprintf(client, "POST http://example.com/upload HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\nContent-Length: %d\r\n\r\n%s", basicHeader("wang.name.upload", "secret"), len(payload), payload)
	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("large POST was rejected: response %v, error %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := <-results; got != payload {
		t.Fatalf("POST body corrupted: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestCONNECTUpstreamFailureReturns502(t *testing.T) {
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		N.CloseOnHandshakeFailure(conn, onClose, fmt.Errorf("upstream dial failed"))
	}}
	client, _ := startTestConnection(t, router, false)
	fmt.Fprintf(client, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Authorization: %s\r\n\r\n", basicHeader("wang.name.sessionA", "secret"))
	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("expected 502 before successful CONNECT handshake, got %d", resp.StatusCode)
	}
}

func TestHTTPUpgradePreservesBufferedPayload(t *testing.T) {
	results := make(chan routeResult, 1)
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		reader := bufio.NewReader(conn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		results <- routeResult{md.User, routing.SessionFromContext(ctx), req.Header.Get("Proxy-Authorization")}
		io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		io.Copy(conn, reader)
	}}
	client, _ := startTestConnection(t, router, false)
	payload := "buffered-upgrade-data"
	fmt.Fprintf(client, "GET http://example.com/socket HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nProxy-Authorization: %s\r\n\r\n%s", basicHeader("wang.name.ws", "secret"), payload)
	reader := bufio.NewReader(client)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade failed: response %v, error %v", resp, err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != payload {
		t.Fatalf("lost buffered upgrade payload: %q, %v", got, err)
	}
	identity := <-results
	if identity.user != "wang.name.ws" || identity.session != identity.user || identity.authorization != "" {
		t.Fatalf("incorrect upgrade identity or leaked proxy auth: %+v", identity)
	}
}

func TestAnonymousHTTPRemainsAvailable(t *testing.T) {
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		if md.User != "" || routing.SessionFromContext(ctx) != "" || req.Header.Get("Proxy-Authorization") != "" {
			t.Error("anonymous ingress adopted unverified identity or leaked credentials")
		}
		io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	}}
	client, _ := startTestConnection(t, router, true)
	fmt.Fprintf(client, "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\nConnection: close\r\n\r\n", basicHeader("ignored.session", "arbitrary"))
	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("anonymous HTTP failed: response %v, error %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func TestHTTPChunkedPOSTPreservesTrailers(t *testing.T) {
	results := make(chan string, 1)
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		body, _ := io.ReadAll(req.Body)
		req.Body.Close()
		results <- string(body) + "/" + req.Trailer.Get("X-Checksum")
		io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	}}
	client, _ := startTestConnection(t, router, false)
	fmt.Fprintf(client, "POST http://example.com/upload HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n7\r\npayload\r\n0\r\nX-Checksum: checksum-value\r\n\r\n", basicHeader("wang.name.chunked", "secret"))
	resp, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("chunked POST failed: response %v, error %v", resp, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := <-results; got != "payload/checksum-value" {
		t.Fatalf("chunked body or trailers not preserved: %q", got)
	}
}

func TestHTTPExpectContinueClientWaitsForInterimResponse(t *testing.T) {
	payload := "body-after-100-continue"
	results := make(chan string, 1)
	router := &testRouter{tcp: func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
		defer conn.Close()
		defer onClose(nil)
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		if req.Header.Get("Expect") != "" || req.Header.Get("Proxy-Authorization") != "" {
			t.Error("proxy forwarded Expect or proxy credentials upstream")
		}
		body, _ := io.ReadAll(req.Body)
		req.Body.Close()
		results <- string(body)
		io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
	}}
	client, _ := startTestConnection(t, router, false)
	// Do not send the body yet: this matches clients that wait for 100 Continue.
	fmt.Fprintf(client, "POST http://example.com/upload HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\nContent-Length: %d\r\nExpect: 100-continue\r\n\r\n", basicHeader("wang.name.waited", "secret"), len(payload))
	reader := bufio.NewReader(client)
	interim, err := http.ReadResponse(reader, nil)
	if err != nil || interim.StatusCode != http.StatusContinue {
		t.Fatalf("expected 100 before sending body: response %v, error %v", interim, err)
	}
	interim.Body.Close()
	if _, err := io.WriteString(client, payload); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != 200 {
		t.Fatalf("expected final 200: response %v, error %v", response, err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if got := <-results; got != payload {
		t.Fatalf("expected complete post-continue body, got %q", got)
	}
}

func TestHTTPExpectationRejectedBeforeInterimResponse(t *testing.T) {
	for _, tt := range []struct {
		name, target, password, expectation string
		status                              int
	}{
		{name: "wrong_auth", target: "http://example.com/upload", password: "incorrect", expectation: "100-continue", status: 407},
		{name: "invalid_destination", target: "/upload", password: "secret", expectation: "100-continue", status: 400},
		{name: "unknown_expectation", target: "http://example.com/upload", password: "secret", expectation: "unsupported-expectation", status: 417},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := startTestConnection(t, &testRouter{tcp: func(context.Context, net.Conn, adapter.InboundContext, N.CloseHandlerFunc) {
				t.Error("rejected expectation request was routed upstream")
			}}, false)
			fmt.Fprintf(client, "POST %s HTTP/1.1\r\nHost: example.com\r\nProxy-Authorization: %s\r\nContent-Length: 7\r\nExpect: %s\r\n\r\n", tt.target, basicHeader("wang.name.waited", tt.password), tt.expectation)
			response, err := http.ReadResponse(bufio.NewReader(client), nil)
			if err != nil || response.StatusCode != tt.status {
				t.Fatalf("expected %d without an interim response: response %v, error %v", tt.status, response, err)
			}
			response.Body.Close()
		})
	}
}
