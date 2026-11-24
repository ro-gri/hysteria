package outbounds

import (
	"io"
	"net"
	"sync"

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
	conn, err := o.client.UDP()
	if err != nil {
		return nil, err
	}
	return &hysteria2UDPConn{conn: conn}, nil
}
func (o *hysteria2Outbound) CheckUDP(addr *AddrEx) error { return nil }

// Each inbound UDP session owns its own upstream session. It never owns the
// shared client. The client Send implementation reuses its serialization buffer.
type hysteria2UDPConn struct {
	conn      client.HyUDPConn
	sendMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

func (c *hysteria2UDPConn) ReadFrom(b []byte) (int, *AddrEx, error) {
	payload, address, err := c.conn.Receive()
	if err != nil {
		return 0, nil, err
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return 0, nil, err
	}
	p, err := parsePortUint16(port)
	if err != nil {
		return 0, nil, err
	}
	addr := &AddrEx{Host: host, Port: p}
	n := copy(b, payload)
	if n < len(payload) {
		return n, addr, io.ErrShortBuffer
	}
	return n, addr, nil
}
func (c *hysteria2UDPConn) WriteTo(b []byte, addr *AddrEx) (int, error) {
	c.sendMu.Lock()
	err := c.conn.Send(b, addr.String())
	c.sendMu.Unlock()
	if err != nil {
		return 0, err
	}
	return len(b), nil
}
func (c *hysteria2UDPConn) Close() error {
	c.closeOnce.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}
