package outbounds

import (
	"errors"
	"net"

	"github.com/apernet/hysteria/core/v2/client"
)

// hysteria2Outbound borrows a shared client. Only the application owns its
// lifetime; closing a relayed stream or UDP session must not close the client.
type hysteria2Outbound struct{ client client.Client }

func NewHysteria2Outbound(c client.Client) PluggableOutbound {
	return &hysteria2Outbound{client: c}
}

func (o *hysteria2Outbound) TCP(addr *AddrEx) (net.Conn, error) {
	// Preserve the requested host, as SOCKS5 does: the exit resolves domains.
	return o.client.TCP(addr.String())
}
func (o *hysteria2Outbound) UDP(addr *AddrEx) (UDPConn, error) {
	return nil, errors.New("UDP is not supported by this outbound")
}
func (o *hysteria2Outbound) CheckUDP(addr *AddrEx) error { return nil }
