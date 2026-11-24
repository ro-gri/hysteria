package outbounds

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/extras/v2/outbounds/acl"
	"github.com/stretchr/testify/require"
)

type relayTestClient struct {
	tcp    func(string) (net.Conn, error)
	udp    func() (client.HyUDPConn, error)
	closed bool
}

func (c *relayTestClient) TCP(addr string) (net.Conn, error) { return c.tcp(addr) }
func (c *relayTestClient) UDP() (client.HyUDPConn, error)    { return c.udp() }
func (c *relayTestClient) Close() error                      { c.closed = true; return nil }

func TestHysteria2TCP(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	require.NoError(t, remote.SetDeadline(time.Now().Add(5*time.Second)))
	c := &relayTestClient{tcp: func(addr string) (net.Conn, error) {
		require.Equal(t, "example.com:443", addr)
		return local, nil
	}}
	ob := NewHysteria2Outbound(c)
	conn, err := ob.TCP(&AddrEx{Host: "example.com", Port: 443, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(192, 0, 2, 1)}})
	require.NoError(t, err)
	require.Same(t, local, conn) // No local socket or extra relay around the QUIC stream.
	done := make(chan error, 1)
	go func() { _, err := io.Copy(remote, remote); done <- err }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	b := make([]byte, 5)
	_, err = io.ReadFull(conn, b)
	require.NoError(t, err)
	require.Equal(t, "hello", string(b))
	require.NoError(t, conn.Close())
	require.NoError(t, <-done)
	require.False(t, c.closed)
	failure := errors.New("exit unavailable")
	c.tcp = func(string) (net.Conn, error) { return nil, failure }
	_, err = ob.TCP(&AddrEx{Host: "example.com", Port: 443})
	require.ErrorIs(t, err, failure)
}

type relayTestUDP struct {
	data   []byte
	addr   string
	err    error
	closes int
}

func (c *relayTestUDP) Send(b []byte, addr string) error {
	if c.err != nil {
		return c.err
	}
	c.data = append([]byte(nil), b...)
	c.addr = addr
	return nil
}
func (c *relayTestUDP) Receive() ([]byte, string, error) { return c.data, c.addr, c.err }
func (c *relayTestUDP) Close() error                     { c.closes++; return nil }

func TestHysteria2UDP(t *testing.T) {
	var sessions []*relayTestUDP
	c := &relayTestClient{udp: func() (client.HyUDPConn, error) {
		u := &relayTestUDP{}
		sessions = append(sessions, u)
		return u, nil
	}}
	ob := NewHysteria2Outbound(c)
	a, err := ob.UDP(&AddrEx{Host: "example.com", Port: 53})
	require.NoError(t, err)
	b, err := ob.UDP(&AddrEx{Host: "example.com", Port: 53})
	require.NoError(t, err)
	require.Len(t, sessions, 2)
	for _, size := range []int{0, 1, 1200, 65507} {
		payload := make([]byte, size)
		n, err := a.WriteTo(payload, &AddrEx{Host: "2001:db8::1", Port: 53})
		require.NoError(t, err)
		require.Equal(t, size, n)
		buf := make([]byte, size)
		n, addr, err := a.ReadFrom(buf)
		require.NoError(t, err)
		require.Equal(t, size, n)
		require.Equal(t, "[2001:db8::1]:53", addr.String())
	}
	_, err = a.WriteTo([]byte("abc"), &AddrEx{Host: "example.com", Port: 53})
	require.NoError(t, err)
	_, _, err = a.ReadFrom(make([]byte, 2))
	require.ErrorIs(t, err, io.ErrShortBuffer)
	sessions[0].addr = "invalid"
	_, _, err = a.ReadFrom(make([]byte, 5))
	require.Error(t, err)
	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
	require.Equal(t, 1, sessions[0].closes)
	require.Zero(t, sessions[1].closes)
	require.False(t, c.closed)
	failure := errors.New("session closed")
	sessions[1].err = failure
	_, err = b.WriteTo(nil, &AddrEx{Host: "example.com", Port: 53})
	require.ErrorIs(t, err, failure)
	_, _, err = b.ReadFrom(nil)
	require.ErrorIs(t, err, failure)
	c.udp = func() (client.HyUDPConn, error) { return nil, failure }
	_, err = ob.UDP(&AddrEx{})
	require.ErrorIs(t, err, failure)
}

// Routing stays in the existing ACL engine, not in the new transport.
func TestHysteria2ACL(t *testing.T) {
	exitErr := errors.New("upstream unavailable")
	var destination string
	c := &relayTestClient{
		tcp: func(addr string) (net.Conn, error) { destination = addr; return nil, exitErr },
		udp: func() (client.HyUDPConn, error) { return nil, exitErr },
	}
	direct := &mockPluggableOutbound{}
	socks := &mockPluggableOutbound{}
	rules, err := acl.TranslateText("direct(192.0.2.0/24)\nsocks(suffix:other.example)\nreject(198.51.100.0/24)\nexit(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{
		{Name: "exit", Outbound: NewHysteria2Outbound(c)},
		{Name: "direct", Outbound: direct},
		{Name: "socks", Outbound: socks},
	}, nil)
	require.NoError(t, err)
	directAddr := &AddrEx{Host: "192.0.2.3", Port: 443, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(192, 0, 2, 3)}}
	direct.EXPECT().TCP(directAddr).Return(nil, nil).Once()
	_, err = engine.TCP(directAddr)
	require.NoError(t, err)
	socksAddr := &AddrEx{Host: "www.other.example", Port: 443}
	socks.EXPECT().TCP(socksAddr).Return(nil, nil).Once()
	_, err = engine.TCP(socksAddr)
	require.NoError(t, err)
	_, err = engine.TCP(&AddrEx{Host: "198.51.100.4", Port: 443, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(198, 51, 100, 4)}})
	require.ErrorIs(t, err, errRejected)
	// Failure in the selected exit is returned, never retried through DIRECT.
	_, err = engine.TCP(&AddrEx{Host: "remote.example", Port: 443})
	require.ErrorIs(t, err, exitErr)
	require.Equal(t, "remote.example:443", destination)
	group, err := engine.UDP(&AddrEx{Host: "203.0.113.1", Port: 53})
	require.NoError(t, err)
	defer group.Close()
	_, err = group.WriteTo(nil, &AddrEx{Host: "203.0.113.1", Port: 53})
	require.ErrorIs(t, err, exitErr)
	require.ErrorIs(t, engine.CheckUDP(&AddrEx{Host: "198.51.100.4", Port: 53, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(198, 51, 100, 4)}}), errRejected)
	// Locally resolved IP controls ACL while Host remains available to the exit.
	resolved := &AddrEx{Host: "local.example", Port: 53, ResolveInfo: &ResolveInfo{IPv4: net.IPv4(192, 0, 2, 3)}}
	direct.EXPECT().CheckUDP(resolved).Return(nil).Twice()
	direct.EXPECT().UDP(resolved).Return(nil, exitErr).Once()
	group2, err := engine.UDP(resolved)
	require.NoError(t, err)
	defer group2.Close()
	_, err = group2.WriteTo(nil, resolved)
	require.ErrorIs(t, err, exitErr)
	direct.AssertExpectations(t)
	socks.AssertExpectations(t)
}
