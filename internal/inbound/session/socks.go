package session

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	singbufio "github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
	"github.com/sagernet/sing/protocol/socks/socks5"
)

func (h *Inbound) handleSOCKS5(ctx context.Context, conn net.Conn, reader *bufio.Reader, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (bool, error) {
	greeting, err := socks5.ReadAuthRequest(reader)
	if err != nil {
		return false, errors.New("invalid SOCKS5 greeting")
	}
	method := socks5.AuthTypeNotRequired
	if len(h.users) != 0 {
		method = socks5.AuthTypeUsernamePassword
	}
	accepted := false
	for _, offered := range greeting.Methods {
		if offered == method {
			accepted = true
		}
	}
	if !accepted {
		socks5.WriteAuthResponse(conn, socks5.AuthResponse{Method: socks5.AuthTypeNoAcceptedMethods})
		return false, errors.New("no supported SOCKS5 authentication method")
	}
	if err := socks5.WriteAuthResponse(conn, socks5.AuthResponse{Method: method}); err != nil {
		return false, err
	}
	if method == socks5.AuthTypeUsernamePassword {
		credentials, err := socks5.ReadUsernamePasswordAuthRequest(reader)
		if err != nil {
			socks5.WriteUsernamePasswordAuthResponse(conn, socks5.UsernamePasswordAuthResponse{Status: socks5.UsernamePasswordStatusFailure})
			return false, errAuthentication
		}
		var ok bool
		ctx, ok = h.authenticate(ctx, credentials.Username, credentials.Password)
		status := socks5.UsernamePasswordStatusSuccess
		if !ok {
			status = socks5.UsernamePasswordStatusFailure
		}
		if err := socks5.WriteUsernamePasswordAuthResponse(conn, socks5.UsernamePasswordAuthResponse{Status: status}); err != nil {
			return false, err
		}
		if !ok {
			return false, errAuthentication
		}
	}
	request, err := socks5.ReadRequest(reader)
	if err != nil {
		return false, errors.New("invalid SOCKS5 request")
	}
	conn.SetReadDeadline(time.Time{})
	switch request.Command {
	case socks5.CommandConnect:
		metadata.Destination = request.Destination
		lazy := socks.NewLazyConn(&bufferedConn{Conn: conn, reader: reader}, socks5.Version)
		h.routeConnection(ctx, lazy, metadata, onClose)
		return true, nil
	case socks5.CommandUDPAssociate:
		return h.handleUDPAssociate(ctx, conn, reader, metadata, onClose)
	default:
		socks5.WriteResponse(conn, socks5.Response{ReplyCode: socks5.ReplyCodeUnsupported})
		return false, errors.New("unsupported SOCKS5 command")
	}
}

func (h *Inbound) handleUDPAssociate(ctx context.Context, conn net.Conn, reader *bufio.Reader, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (bool, error) {
	localIP := M.AddrFromNet(conn.LocalAddr())
	address := M.SocksaddrFrom(localIP, 0).String()
	network := M.NetworkFromNetAddr(N.NetworkUDP, localIP)
	var listenConfig net.ListenConfig
	var udp net.PacketConn
	var err error
	if h.listener != nil {
		udp, err = h.listener.ListenPacket(listenConfig, ctx, network, address)
	} else {
		udp, err = listenConfig.ListenPacket(ctx, network, address)
	}
	if err != nil {
		socks5.WriteResponse(conn, socks5.Response{ReplyCode: socks5.ReplyCodeFailure})
		return false, errors.New("SOCKS5 UDP listener failed")
	}
	peerIP := metadata.Source.Addr.Unmap()
	filtered := &clientPacketConn{PacketConn: udp, sourceIP: peerIP}
	packet := socks.NewAssociatePacketConn(filtered, M.Socksaddr{}, conn)
	if err := socks5.WriteResponse(conn, socks5.Response{ReplyCode: socks5.ReplyCodeSuccess, Bind: M.SocksaddrFromNet(udp.LocalAddr()).Unwrap()}); err != nil {
		packet.Close()
		return false, err
	}
	// UDP association lifetime is bounded by its TCP control connection.
	go func() {
		io.Copy(io.Discard, reader)
		packet.Close()
	}()
	udp.SetReadDeadline(time.Now().Add(handshakeTimeout))
	firstPacket := buf.NewPacket()
	destination, err := packet.ReadPacket(firstPacket)
	if err != nil {
		firstPacket.Release()
		packet.Close()
		return false, errors.New("SOCKS5 UDP association closed")
	}
	udp.SetReadDeadline(time.Time{})
	cached := singbufio.NewCachedPacketConn(packet, firstPacket, destination)
	metadata.Destination = destination
	h.routePacketConnection(ctx, cached, metadata, onClose)
	return true, nil
}

// Only the authenticated control connection's client IP may claim an
// association. Its first UDP source port is then pinned for the association.
type clientPacketConn struct {
	net.PacketConn
	sourceIP netip.Addr
	peerMu   sync.RWMutex
	peer     net.Addr
}

func (c *clientPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, addr, err := c.PacketConn.ReadFrom(p)
		if err != nil {
			return n, addr, err
		}
		if c.sourceIP.IsValid() && M.AddrFromNet(addr).Unmap() != c.sourceIP {
			continue
		}
		c.peerMu.Lock()
		if c.peer == nil {
			c.peer = addr
		}
		accepted := addr.String() == c.peer.String()
		c.peerMu.Unlock()
		if accepted {
			return n, addr, nil
		}
	}
}

// Present UDP datagrams as net.Conn reads/writes for sing's SOCKS wrapper. The
// learned peer is synchronized because packet forwarding reads and writes on
// different goroutines; a mutable shared server-packet RemoteAddr is not safe.
func (c *clientPacketConn) Read(p []byte) (int, error) {
	n, _, err := c.ReadFrom(p)
	return n, err
}

func (c *clientPacketConn) Write(p []byte) (int, error) {
	peer := c.RemoteAddr()
	if peer == nil {
		return 0, errors.New("SOCKS5 UDP peer not established")
	}
	return c.PacketConn.WriteTo(p, peer)
}

func (c *clientPacketConn) RemoteAddr() net.Addr {
	c.peerMu.RLock()
	defer c.peerMu.RUnlock()
	return c.peer
}
