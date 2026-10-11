package session

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/protocol/socks"
	"github.com/sagernet/sing/protocol/socks/socks4"
)

// SOCKS4 carries only a username. As with the original mixed ingress, an
// authenticated SOCKS4 client is accepted only if its account password is empty.
func (h *Inbound) handleSOCKS4(ctx context.Context, conn net.Conn, reader *bufio.Reader, metadata adapter.InboundContext, onClose N.CloseHandlerFunc) (bool, error) {
	version, err := reader.ReadByte()
	if err != nil || version != socks4.Version {
		return false, errors.New("invalid SOCKS4 greeting")
	}
	request, err := socks4.ReadRequest0(reader)
	if err != nil {
		writeSOCKS4Rejected(conn)
		return false, errors.New("invalid SOCKS4 request")
	}
	if request.Command != socks4.CommandConnect {
		writeSOCKS4Rejected(conn)
		return false, errors.New("unsupported SOCKS4 command")
	}
	if len(h.users) > 0 {
		var ok bool
		ctx, ok = h.authenticate(ctx, request.Username, "")
		if !ok {
			writeSOCKS4Rejected(conn)
			return false, errAuthentication
		}
	}
	// Anonymous SOCKS4 username fields are unverified and do not establish
	// named sessions (or an authenticated metadata.User).
	conn.SetReadDeadline(time.Time{})
	metadata.Destination = request.Destination
	lazy := socks.NewLazyConn(&bufferedConn{Conn: conn, reader: reader}, socks4.Version)
	h.routeConnection(ctx, lazy, metadata, onClose)
	return true, nil
}

func writeSOCKS4Rejected(conn net.Conn) error {
	// An explicit IPv4 zero address makes sing's serializer emit the complete
	// eight-byte SOCKS4 response, including its four-byte address field.
	return socks4.WriteResponse(conn, socks4.Response{
		ReplyCode:   socks4.ReplyCodeRejectedOrFailed,
		Destination: M.SocksaddrFrom(netip.IPv4Unspecified(), 0),
	})
}
