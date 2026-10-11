// Package session implements a mixed HTTP/SOCKS4/SOCKS5 ingress with authenticated,
// dynamically named sessions. It leaves transport routing to sing-box.
package session

import (
	"bufio"
	"context"
	"errors"
	"net"
	"time"

	"easy_proxies/internal/routing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/common/listener"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/common/uot"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/auth"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks/socks4"
	"github.com/sagernet/sing/protocol/socks/socks5"
)

const Type = "session-mixed"
const handshakeTimeout = 30 * time.Second

var errAuthentication = errors.New("proxy authentication failed")

// Options retains sing-box's mixed listener options and opts into named sessions.
type Options struct {
	option.HTTPMixedInboundOptions
	SessionEnabled bool
}

func Register(registry *inbound.Registry) {
	inbound.Register[Options](registry, Type, NewInbound)
}

var _ adapter.TCPInjectableInbound = (*Inbound)(nil)

type Inbound struct {
	inbound.Adapter
	router         adapter.ConnectionRouterEx
	logger         log.ContextLogger
	listener       *listener.Listener
	tlsConfig      tls.ServerConfig
	users          []auth.User
	sessionEnabled bool
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options Options) (adapter.Inbound, error) {
	h := &Inbound{
		Adapter:        inbound.NewAdapter(Type, tag),
		router:         uot.NewRouter(router, logger),
		logger:         logger,
		users:          append([]auth.User(nil), options.Users...),
		sessionEnabled: options.SessionEnabled,
	}
	if options.TLS != nil {
		var err error
		h.tlsConfig, err = tls.NewServer(ctx, logger, common.PtrValueOrDefault(options.TLS))
		if err != nil {
			return nil, err
		}
	}
	h.listener = listener.New(listener.Options{
		Context: ctx, Logger: logger, Network: []string{N.NetworkTCP},
		Listen: options.ListenOptions, ConnectionHandler: h,
		SetSystemProxy: options.SetSystemProxy, SystemProxySOCKS: true,
	})
	return h, nil
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	if h.tlsConfig != nil {
		if err := h.tlsConfig.Start(); err != nil {
			return err
		}
	}
	return h.listener.Start()
}

func (h *Inbound) Close() error { return common.Close(h.listener, h.tlsConfig) }

func (h *Inbound) NewConnectionEx(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	owned, err := h.newConnection(ctx, conn, metadata, onClose)
	if !owned {
		conn.Close()
		if onClose != nil {
			onClose(err)
		}
	}
	// Parser errors can contain raw HTTP headers; never include them (or any
	// credentials) in ingress logs.
	if err != nil && h.logger != nil {
		h.logger.DebugContext(ctx, "session ingress connection closed")
	}
}

func (h *Inbound) newConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	if h.tlsConfig != nil {
		// TLS handshakes write as well as read; a peer that does not consume
		// handshake records must not hold a goroutine indefinitely.
		conn.SetWriteDeadline(time.Now().Add(handshakeTimeout))
		var err error
		conn, err = tls.ServerHandshake(ctx, conn, h.tlsConfig)
		if err != nil {
			return false, errors.New("proxy TLS handshake failed")
		}
		conn.SetWriteDeadline(time.Time{})
	}
	reader := bufio.NewReader(conn)
	first, err := reader.Peek(1)
	if err != nil {
		return false, err
	}
	switch first[0] {
	case socks4.Version:
		return h.handleSOCKS4(ctx, conn, reader, metadata, onClose)
	case socks5.Version:
		return h.handleSOCKS5(ctx, conn, reader, metadata, onClose)
	}
	return h.handleHTTP(ctx, conn, reader, metadata, onClose)
}

func (h *Inbound) authenticate(ctx context.Context, username, password string) (context.Context, bool) {
	// Exact accounts take precedence over suffix parsing if multiple users have
	// overlapping names, e.g. "wang" and "wang.name".
	exactMatch := false
	for _, user := range h.users {
		if username == user.Username {
			exactMatch = true
			_, ok := routing.VerifyProxyCredentials(user.Username, user.Password, username, password, false)
			if ok {
				return auth.ContextWithUser(routing.WithSession(ctx, ""), username), true
			}
		}
	}
	if exactMatch {
		return ctx, false
	}
	for _, user := range h.users {
		name, ok := routing.VerifyProxyCredentials(user.Username, user.Password, username, password, h.sessionEnabled)
		if ok {
			if name != "" {
				ctx = routing.WithSession(ctx, username)
			}
			return auth.ContextWithUser(ctx, username), true
		}
	}
	return ctx, false
}

func (h *Inbound) routeConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.Inbound, metadata.InboundType = h.Tag(), h.Type()
	metadata.User, _ = auth.UserFromContext[string](ctx)
	h.router.RouteConnectionEx(ctx, conn, metadata, onClose)
}

func (h *Inbound) routePacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) {
	metadata.Inbound, metadata.InboundType = h.Tag(), h.Type()
	metadata.User, _ = auth.UserFromContext[string](ctx)
	h.router.RoutePacketConnectionEx(ctx, conn, metadata, onClose)
}

// bufferedConn preserves bytes already read past a protocol handshake. It does
// not expose a replaceable upstream reader, which would discard those bytes.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
