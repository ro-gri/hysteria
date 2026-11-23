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
