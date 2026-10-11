package geoip

import (
	"bufio"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureDialer func(context.Context, string, string) (net.Conn, error)

func (d fixtureDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d(ctx, network, address)
}

// A pointer avoids making the cache key an uncomparable function value.
type pointerDialer struct{ dial fixtureDialer }

func (d *pointerDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dial(ctx, network, address)
}

type fixtureHijacker struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (w fixtureHijacker) Header() http.Header         { return make(http.Header) }
func (w fixtureHijacker) Write(p []byte) (int, error) { return len(p), nil }
func (w fixtureHijacker) WriteHeader(int)             {}
func (w fixtureHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(w.reader, bufio.NewWriter(w.conn)), nil
}

func fixtureTunnel(t *testing.T, router *Router, ctx context.Context, payload string) (net.Conn, net.Conn, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	target, upstream := net.Pipe()
	done := make(chan struct{})
	reader := bufio.NewReader(io.MultiReader(strings.NewReader(payload), server))
	if payload == "" {
		reader = bufio.NewReader(server)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
		target.Close()
		upstream.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("CONNECT handler did not exit")
		}
	})
	go func() {
		router.handleConnect(fixtureHijacker{server, reader}, (&http.Request{}).WithContext(ctx), &pointerDialer{dial: func(context.Context, string, string) (net.Conn, error) { return target, nil }}, "example.test:443")
		close(done)
	}()
	client.SetDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("CONNECT response = %v, error = %v", resp, err)
	}
	return client, upstream, done
}

func TestCONNECTPreservesHijackedBuffer(t *testing.T) {
	client, upstream, _ := fixtureTunnel(t, NewRouter(RouterConfig{}, nil), context.Background(), "pipelined-TLS-data")
	go io.Copy(upstream, upstream)
	got := make([]byte, len("pipelined-TLS-data"))
	if _, err := io.ReadFull(client, got); err != nil || string(got) != "pipelined-TLS-data" {
		t.Fatalf("tunnel payload = %q, error = %v", got, err)
	}
}

func TestCONNECTClosesPeerOnDisconnect(t *testing.T) {
	for _, side := range []string{"client", "upstream"} {
		t.Run(side, func(t *testing.T) {
			client, upstream, done := fixtureTunnel(t, NewRouter(RouterConfig{}, nil), context.Background(), "")
			if side == "client" {
				client.Close()
			} else {
				upstream.Close()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("one-sided disconnect leaked CONNECT handler")
			}
		})
	}
}

func TestCONNECTClosesOnRequestCancellationAndRouterStop(t *testing.T) {
	for _, action := range []string{"cancel", "stop"} {
		t.Run(action, func(t *testing.T) {
			router := NewRouter(RouterConfig{}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, _, done := fixtureTunnel(t, router, ctx, "")
			if action == "cancel" {
				cancel()
			} else if err := router.Stop(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("tunnel did not close")
			}
			router.mu.RLock()
			count := len(router.tunnels)
			router.mu.RUnlock()
			if count != 0 {
				t.Fatalf("tracked tunnels = %d", count)
			}
		})
	}
}

func TestRouterStartReportsListenerFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	p, _ := strconv.Atoi(port)
	router := NewRouter(RouterConfig{Listen: host, Port: uint16(p)}, nil)
	if err := router.Start(context.Background()); err == nil {
		router.Stop()
		t.Fatal("Start accepted an occupied port")
	}
}

func TestRouterStopUnblocksContextWatcherAndIsIdempotent(t *testing.T) {
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	if err := router.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := router.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := router.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-router.stopCh:
	default:
		t.Fatal("stop watcher was not notified")
	}
}

func TestParseRequestPreservesEscapedRegionalPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.test/jp/a%2Fb?q=one%2Ftwo", nil)
	region, _, err := (&Router{}).parseRequest(req)
	if err != nil || region != RegionJP || req.URL.EscapedPath() != "/a%2Fb" || req.URL.RawQuery != "q=one%2Ftwo" {
		t.Fatalf("incorrect regional rewrite: region=%q url=%s err=%v", region, req.URL, err)
	}
}

func TestProxyAuthAcceptsCaseInsensitiveSchemeAndRejectsDuplicates(t *testing.T) {
	router := NewRouter(RouterConfig{Username: "fixture-user", Password: "fixture-pass"}, nil)
	req := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
	value := "basic " + base64.StdEncoding.EncodeToString([]byte("fixture-user:fixture-pass"))
	req.Header.Set("Proxy-Authorization", value)
	if !router.checkProxyAuth(req) {
		t.Fatal("case-insensitive Basic scheme rejected")
	}
	req.Header.Add("Proxy-Authorization", value)
	if router.checkProxyAuth(req) {
		t.Fatal("duplicate proxy credentials accepted")
	}
}

func TestHTTPForwardsHostBodyAndTrailersWithoutHopHeaders(t *testing.T) {
	type received struct {
		host          string
		length        int64
		body, trailer string
		headers       http.Header
	}
	requests := make(chan received, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		requests <- received{req.Host, req.ContentLength, string(body), req.Trailer.Get("X-Request-Trailer"), req.Header.Clone()}
		w.Header().Add("Connection", "X-Response-One")
		w.Header().Add("Connection", "X-Response-Two")
		w.Header().Set("X-Response-One", "private")
		w.Header().Set("X-Response-Two", "private")
		w.Header().Set("Keep-Alive", "timeout=10")
		w.Header().Set("Trailer", "X-Response-Trailer")
		io.WriteString(w, "upstream-response")
		w.Header().Set("X-Response-Trailer", "done")
	}))
	defer upstream.Close()
	router := NewRouter(RouterConfig{}, nil)
	defer router.Stop()
	dialer := &pointerDialer{dial: (&net.Dialer{}).DialContext}
	router.SetGlobalPool(dialer)
	for _, chunked := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodPost, upstream.URL+"/us/path", strings.NewReader("body"))
		req.Host = "custom.virtual.test"
		req.Header.Add("Connection", "X-Request-One")
		req.Header.Add("Connection", "X-Request-Two")
		req.Header.Set("X-Request-One", "private")
		req.Header.Set("X-Request-Two", "private")
		req.Header.Set("Proxy-Authorization", "Basic secret")
		req.Header.Set("TE", "trailers")
		if chunked {
			req.ContentLength = -1
			req.TransferEncoding = []string{"chunked"}
			req.Trailer = http.Header{"X-Request-Trailer": []string{"sent"}}
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		resp := recorder.Result()
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(body) != "upstream-response" {
			t.Fatalf("response=%d body=%q", resp.StatusCode, body)
		}
		got := <-requests
		if got.host != "custom.virtual.test" || got.body != "body" || !chunked && got.length != 4 || chunked && got.trailer != "sent" {
			t.Fatalf("request metadata lost: %+v", got)
		}
		for _, name := range []string{"Connection", "X-Request-One", "X-Request-Two", "Proxy-Authorization", "TE"} {
			if got.headers.Get(name) != "" {
				t.Fatalf("leaked request hop header %s", name)
			}
		}
		for _, name := range []string{"Connection", "X-Response-One", "X-Response-Two", "Keep-Alive"} {
			if resp.Header.Get(name) != "" {
				t.Fatalf("leaked response hop header %s", name)
			}
		}
		if resp.Trailer.Get("X-Response-Trailer") != "done" {
			t.Fatalf("response trailer lost: %v", resp.Trailer)
		}
	}
}

type countedConn struct {
	net.Conn
	closes *atomic.Int32
}

func (c *countedConn) Close() error { c.closes.Add(1); return c.Conn.Close() }
func TestPoolReplacementAndStopCloseIdleTransports(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { io.WriteString(w, "ok") }))
	defer upstream.Close()
	router := NewRouter(RouterConfig{}, nil)
	defer router.Stop()
	var closes atomic.Int32
	dialer := &pointerDialer{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &countedConn{conn, &closes}, nil
	}}
	for _, action := range []string{"pool", "global", "stop"} {
		router.SetGlobalPool(dialer)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, upstream.URL, nil))
		if recorder.Code != 200 {
			t.Fatal(recorder.Code)
		}
		before := closes.Load()
		switch action {
		case "pool":
			router.SetPool(RegionJP, dialer)
		case "global":
			router.SetGlobalPool(dialer)
		case "stop":
			router.Stop()
		}
		deadline := time.Now().Add(time.Second)
		for closes.Load() == before && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if closes.Load() == before {
			t.Fatalf("%s discarded idle transport without closing its socket", action)
		}
	}
}

func TestHTTPStreamsChunkBeforeUpstreamEOF(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: ready\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-req.Context().Done():
		}
	}))
	defer upstream.Close()
	router := NewRouter(RouterConfig{}, nil)
	router.SetGlobalPool(&pointerDialer{dial: (&net.Dialer{}).DialContext})
	proxy := httptest.NewServer(router)
	defer proxy.Close()
	defer router.Stop()
	defer close(release)
	req, err := http.NewRequest(http.MethodGet, proxy.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Origin-form requests select the upstream through Host, like CONNECT's
	// matching route. This keeps the test entirely on loopback addresses.
	req.Host = strings.TrimPrefix(upstream.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("stream headers delayed until EOF: %v", err)
	}
	defer resp.Body.Close()
	chunk := make([]byte, len("data: ready\n\n"))
	if _, err := io.ReadFull(resp.Body, chunk); err != nil || string(chunk) != "data: ready\n\n" {
		t.Fatalf("stream buffered until EOF: %q %v", chunk, err)
	}
}

func TestRouterStartRejectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	router := NewRouter(RouterConfig{Listen: "127.0.0.1"}, nil)
	if err := router.Start(ctx); err != context.Canceled {
		t.Fatalf("Start error=%v, want context.Canceled", err)
	}
	router.Stop()
}
