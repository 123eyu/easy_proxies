package session

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/auth"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks/socks4"
)

func TestSOCKS4ConnectCompatibilityAndBufferedPayload(t *testing.T) {
	for _, tt := range []struct {
		name, username, destination, wantUser, wantSession string
		anonymous                                          bool
	}{
		{name: "ipv4_base_empty_password", username: "wang.name", destination: "192.0.2.1:80", wantUser: "wang.name"},
		{name: "socks4a_named_session", username: "wang.name.s4Session", destination: "example.com:443", wantUser: "wang.name.s4Session", wantSession: "wang.name.s4Session"},
		{name: "anonymous_unverified_username", username: "wang.name.unverified", destination: "example.com:80", anonymous: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			results := make(chan routeResult, 1)
			destinations := make(chan M.Socksaddr, 1)
			router := echoRouter(results)
			original := router.tcp
			router.tcp = func(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
				destinations <- md.Destination
				original(ctx, conn, md, onClose)
			}
			var users []auth.User
			if !tt.anonymous {
				users = []auth.User{{Username: "wang.name", Password: ""}}
			}
			client, _ := startTestConnection(t, router, tt.anonymous, users...)
			var request bytes.Buffer
			if err := socks4.WriteRequest(&request, socks4.Request{Command: socks4.CommandConnect, Destination: M.ParseSocksaddr(tt.destination), Username: tt.username}); err != nil {
				t.Fatal(err)
			}
			payload := "buffered-socks4-payload"
			request.WriteString(payload)
			if _, err := client.Write(request.Bytes()); err != nil {
				t.Fatal(err)
			}
			reader := bufio.NewReader(client)
			response, err := socks4.ReadResponse(reader)
			if err != nil || response.ReplyCode != socks4.ReplyCodeGranted {
				t.Fatalf("SOCKS4 CONNECT failed: response %+v, error %v", response, err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(reader, got); err != nil || string(got) != payload {
				t.Fatalf("buffered SOCKS4 payload lost: %q, %v", got, err)
			}
			identity := <-results
			if identity.user != tt.wantUser || identity.session != tt.wantSession {
				t.Fatalf("incorrect SOCKS4 routed identity: %+v", identity)
			}
			if got := <-destinations; got != M.ParseSocksaddr(tt.destination) {
				t.Fatalf("incorrect SOCKS4 destination: %s", got)
			}
		})
	}
}

func TestSOCKS4AuthenticationRejectsNonemptyPasswordAndInvalidUsers(t *testing.T) {
	for _, tt := range []struct {
		name, username, password string
	}{
		{name: "nonempty_password_base", username: "wang.name", password: "secret"},
		{name: "nonempty_password_session", username: "wang.name.s4Session", password: "secret"},
		{name: "wrong_account", username: "other.s4Session"},
		{name: "empty_suffix", username: "wang.name."},
		{name: "invalid_suffix", username: "wang.name.bad.dot"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, closed := startTestConnection(t, &testRouter{}, false, auth.User{Username: "wang.name", Password: tt.password})
			socks4.WriteRequest(client, socks4.Request{Command: socks4.CommandConnect, Destination: M.ParseSocksaddr("192.0.2.1:80"), Username: tt.username})
			response, err := socks4.ReadResponse(bufio.NewReader(client))
			if err != nil || response.ReplyCode != socks4.ReplyCodeRejectedOrFailed {
				t.Fatalf("expected complete SOCKS4 rejection: response %+v, error %v", response, err)
			}
			if got := <-closed; got != errAuthentication {
				t.Fatalf("expected generic authentication error, got %v", got)
			}
		})
	}
}

func TestSOCKS4RejectsUnsupportedCommandAndMalformedRequest(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%v", malformed), func(t *testing.T) {
			client, closed := startTestConnection(t, &testRouter{}, false, auth.User{Username: "wang.name", Password: ""})
			var request bytes.Buffer
			if malformed {
				request.Write([]byte{socks4.Version, socks4.CommandConnect, 0, 80, 192, 0, 2, 1})
				request.WriteString(strings.Repeat("x", 256))
				request.WriteByte(0)
			} else {
				socks4.WriteRequest(&request, socks4.Request{Command: socks4.CommandBind, Destination: M.ParseSocksaddr("192.0.2.1:80"), Username: "wang.name"})
			}
			client.Write(request.Bytes())
			response, err := socks4.ReadResponse(bufio.NewReader(client))
			if err != nil || response.ReplyCode != socks4.ReplyCodeRejectedOrFailed {
				t.Fatalf("expected complete SOCKS4 rejection: response %+v, error %v", response, err)
			}
			if got := <-closed; got == nil || strings.Contains(got.Error(), "wang") || strings.Contains(got.Error(), "xxxx") {
				t.Fatalf("missing or unsanitized SOCKS4 error: %v", got)
			}
		})
	}
}
