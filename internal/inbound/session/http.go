package session

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/pipe"
)

const maxHTTPHeaderBytes = 64 * 1024

var errHTTPHeadersTooLarge = errors.New("proxy HTTP headers too large")

// peekHTTPHeaders bounds only the header block. Body and pipelined tunnel
// bytes remain in the same reader and are never counted against this limit.
func peekHTTPHeaders(reader *bufio.Reader) error {
	scanned, lineStart := 0, 0
	for scanned < maxHTTPHeaderBytes {
		if _, err := reader.Peek(scanned + 1); err != nil {
			return err
		}
		available := reader.Buffered()
		if available > maxHTTPHeaderBytes {
			available = maxHTTPHeaderBytes
		}
		data, _ := reader.Peek(available)
		for i := scanned; i < len(data); i++ {
			if data[i] != '\n' {
				continue
			}
			if i == lineStart || i == lineStart+1 && data[lineStart] == '\r' {
				return nil
			}
			lineStart = i + 1
		}
		scanned = len(data)
	}
	return errHTTPHeadersTooLarge
}

func (h *Inbound) handleHTTP(ctx context.Context, conn net.Conn, reader *bufio.Reader, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (bool, error) {
	// The fixed-size peek buffer prevents unbounded header allocation. One
	// wrapper is shared by every request, rather than chaining readers per request.
	reader = bufio.NewReaderSize(reader, maxHTTPHeaderBytes+1)
	for {
		conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
		if err := peekHTTPHeaders(reader); err != nil {
			if errors.Is(err, errHTTPHeadersTooLarge) {
				writeHTTPError(conn, http.StatusRequestHeaderFieldsTooLarge)
				return false, errHTTPHeadersTooLarge
			}
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, errors.New("invalid proxy HTTP headers")
		}
		req, err := http.ReadRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, errors.New("invalid proxy HTTP request")
		}
		conn.SetReadDeadline(time.Time{})
		requestCtx := ctx // Authentication and session identity are per request.
		if len(h.users) != 0 {
			username, password, ok := proxyCredentials(req.Header)
			if ok {
				requestCtx, ok = h.authenticate(ctx, username, password)
			}
			if !ok {
				writeHTTPError(conn, http.StatusProxyAuthRequired)
				return false, errAuthentication
			}
		}
		req.Header.Del("Proxy-Authorization")
		if req.Method == http.MethodConnect {
			destination := M.ParseSocksaddr(req.URL.Host).Unwrap()
			if destination.Port == 0 {
				destination.Port = 443
			}
			if !destination.IsValid() {
				writeHTTPError(conn, http.StatusBadRequest)
				return false, errors.New("invalid CONNECT destination")
			}
			metadata.Destination = destination
			tunnel := &httpTunnelConn{bufferedConn: bufferedConn{Conn: conn, reader: reader}}
			h.routeConnection(requestCtx, tunnel, metadata, onClose)
			return true, nil
		}
		if req.URL.Host == "" || req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			writeHTTPError(conn, http.StatusBadRequest)
			return false, errors.New("invalid proxy HTTP destination")
		}
		if err := handleHTTPExpectation(conn, req); err != nil {
			return false, err
		}
		if err := h.forwardHTTP(requestCtx, conn, reader, req, metadata); err != nil {
			return false, err
		}
		if req.Close || isUpgrade(req.Header) {
			return false, nil
		}
	}
}

// http.ReadRequest does not provide http.Server's automatic 100-continue
// handling. A client waiting for that interim response would otherwise stall
// forever while the upstream transport tries to read its request body.
func handleHTTPExpectation(conn net.Conn, req *http.Request) error {
	expect := strings.TrimSpace(strings.Join(req.Header.Values("Expect"), ","))
	if expect == "" {
		req.Header.Del("Expect")
		return nil
	}
	if !strings.EqualFold(expect, "100-continue") || !req.ProtoAtLeast(1, 1) {
		writeHTTPError(conn, http.StatusExpectationFailed)
		return errors.New("unsupported proxy HTTP expectation")
	}
	// Authentication and destination validation have already succeeded. The
	// proxy accepts the body locally, so the upstream must not emit a second
	// expectation handshake for the same request.
	req.Header.Del("Expect")
	if req.Body == nil || req.Body == http.NoBody {
		return nil
	}
	if _, err := io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n"); err != nil {
		return errors.New("proxy HTTP interim response failed")
	}
	return nil
}

func proxyCredentials(header http.Header) (username, password string, ok bool) {
	values := header.Values("Proxy-Authorization")
	if len(values) != 1 || len(values[0]) > 2048 {
		return "", "", false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Basic") {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", false
	}
	username, password, ok = strings.Cut(string(decoded), ":")
	return
}

func writeHTTPError(conn net.Conn, status int) error {
	headers := make(http.Header)
	if status == http.StatusProxyAuthRequired {
		headers.Set("Proxy-Authenticate", `Basic realm="Proxy"`)
	}
	return (&http.Response{
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		StatusCode: status, Header: headers, ContentLength: 0, Close: true,
	}).Write(conn)
}

// forwardHTTP deliberately creates one transport per request. A pool-wide
// connection cache would silently carry the next session over the prior
// session's egress, bypassing the pool selector entirely.
func (h *Inbound) forwardHTTP(ctx context.Context, conn net.Conn, reader *bufio.Reader, req *http.Request, metadata adapter.InboundContext) error {
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	transport := &http.Transport{
		DisableCompression: true, DisableKeepAlives: true,
		ResponseHeaderTimeout: handshakeTimeout,
		TLSHandshakeTimeout:   handshakeTimeout,
		DialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			client, upstream := pipe.Pipe()
			md := metadata
			md.Destination = M.ParseSocksaddr(address).Unwrap()
			go h.routeConnection(dialCtx, upstream, md, func(error) {
				client.Close()
				upstream.Close()
			})
			return client, nil
		},
	}
	defer transport.CloseIdleConnections()
	upgrade := isUpgrade(req.Header)
	outReq := req.WithContext(requestCtx)
	// Keep the incoming body and Trailer map paired: chunked body parsing fills
	// trailers lazily. Only headers need an independent copy for stripping.
	outReq.Header = req.Header.Clone()
	outReq.RequestURI = ""
	stripProxyHeaders(outReq.Header, upgrade)
	resp, err := transport.RoundTrip(outReq)
	if err != nil {
		writeHTTPError(conn, http.StatusBadGateway)
		return errors.New("proxy HTTP forwarding failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusSwitchingProtocols && upgrade {
		upstream, ok := resp.Body.(io.ReadWriteCloser)
		if !ok {
			return errors.New("invalid HTTP upgrade response")
		}
		stripProxyHeaders(resp.Header, true)
		if _, err := fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\n"); err != nil {
			return err
		}
		if err := resp.Header.Write(conn); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "\r\n"); err != nil {
			return err
		}
		done := make(chan struct{})
		go func() {
			io.Copy(upstream, reader)
			upstream.Close()
			close(done)
		}()
		io.Copy(conn, upstream)
		conn.Close()
		upstream.Close()
		<-done
		return nil
	}
	stripProxyHeaders(resp.Header, false)
	resp.Close = req.Close
	return resp.Write(conn)
}

func isUpgrade(headers http.Header) bool {
	if headers.Get("Upgrade") == "" {
		return false
	}
	for _, token := range strings.Split(strings.Join(headers.Values("Connection"), ","), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

func stripProxyHeaders(headers http.Header, upgrade bool) {
	for _, token := range strings.Split(strings.Join(headers.Values("Connection"), ","), ",") {
		token = strings.TrimSpace(token)
		if upgrade && strings.EqualFold(token, "upgrade") {
			continue
		}
		headers.Del(token)
	}
	for _, name := range []string{"Proxy-Authorization", "Proxy-Authenticate", "Proxy-Connection", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding"} {
		headers.Del(name)
	}
	if upgrade {
		headers.Set("Connection", "Upgrade")
	} else {
		headers.Del("Connection")
		headers.Del("Upgrade")
	}
}

// httpTunnelConn delays CONNECT success until sing-box finishes dialing. This
// allows the pool to fail over before any success is announced to the client.
type httpTunnelConn struct {
	bufferedConn
	mu      sync.Mutex
	replied bool
}

func (c *httpTunnelConn) ConnHandshakeSuccess(net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replied {
		return nil
	}
	c.replied = true
	_, err := io.WriteString(c.Conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
	return err
}

func (c *httpTunnelConn) HandshakeFailure(error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replied {
		return nil
	}
	c.replied = true
	return writeHTTPError(c.Conn, http.StatusBadGateway)
}
