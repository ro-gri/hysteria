package outbounds

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
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
