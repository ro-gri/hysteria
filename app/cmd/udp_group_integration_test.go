package cmd

import (
	"bytes"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/stretchr/testify/require"
)

type countedUDPClient struct {
	client.Client
	sessions     atomic.Int32
	destinations chan string
}

func (c *countedUDPClient) UDP() (client.HyUDPConn, error) {
	conn, err := c.Client.UDP()
	if err != nil {
		return nil, err
	}
	c.sessions.Add(1)
	return &countedUDPSession{HyUDPConn: conn, destinations: c.destinations}, nil
}

type countedUDPSession struct {
	client.HyUDPConn
	destinations chan string
}

func (c *countedUDPSession) Send(b []byte, addr string) error {
	c.destinations <- addr
	return c.HyUDPConn.Send(b, addr)
}

func TestUDPGroupDirectAndHysteriaInOneSession(t *testing.T) {
	_, baddr := cascadeServer(t, "127.0.0.1:0", nil)
	upstream, _, cleanup, err := innerRunClient(&clientConfig{Server: baddr.String(), Auth: "cascade-test", TLS: clientConfigTLS{Insecure: true}, Lazy: true}, true, true)
	require.NoError(t, err)
	defer cleanup()
	counted := &countedUDPClient{Client: upstream, destinations: make(chan string, 8)}
	echo := func() *net.UDPConn {
		c, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		require.NoError(t, e)
		t.Cleanup(func() { c.Close() })
		go func() {
			buf := make([]byte, 4096)
			for {
				n, a, e := c.ReadFromUDP(buf)
				if e != nil {
					return
				}
				c.WriteToUDP(buf[:n], a)
			}
		}()
		return c
	}
	direct, remote := echo(), echo()
	config := serverConfig{
		Listen:    "127.0.0.1:0",
		TLS:       &serverConfigTLS{Cert: "../../core/internal/integration_tests/test.crt", Key: "../../core/internal/integration_tests/test.key"},
		Auth:      serverConfigAuth{Type: "password", Password: "cascade-test"},
		Outbounds: []serverConfigOutboundEntry{{Name: "exit", Type: "hysteria2"}, {Name: "direct", Type: "direct"}},
		ACL: serverConfigACL{Rules: []serverConfigACLRule{
			{Outbound: "direct", Match: fmt.Sprintf("dst.port == %d", direct.LocalAddr().(*net.UDPAddr).Port)},
			{Outbound: "exit"},
		}},
	}
	hyConfig, err := config.Config(counted)
	require.NoError(t, err)
	a, err := server.NewServer(hyConfig)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Serve() }()
	defer func() {
		a.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	}()
	incoming, _, err := client.NewClient(&client.Config{ServerAddr: hyConfig.Conn.LocalAddr(), Auth: "cascade-test", TLSConfig: client.TLSConfig{InsecureSkipVerify: true}})
	require.NoError(t, err)
	defer incoming.Close()
	session, err := incoming.UDP()
	require.NoError(t, err)
	defer session.Close()
	for i, dest := range []net.Addr{direct.LocalAddr(), remote.LocalAddr(), direct.LocalAddr(), remote.LocalAddr()} {
		data := []byte(fmt.Sprintf("packet-%d", i))
		if i == 1 {
			data = bytes.Repeat([]byte{0xAB}, 4000)
		}
		require.NoError(t, session.Send(data, dest.String()))
		type reply struct {
			data []byte
			addr string
			err  error
		}
		result := make(chan reply, 1)
		go func() { b, a, e := session.Receive(); result <- reply{b, a, e} }()
		select {
		case r := <-result:
			require.NoError(t, r.err)
			require.Equal(t, data, r.data)
			require.Equal(t, dest.String(), r.addr)
		case <-time.After(5 * time.Second):
			t.Fatal("UDP reply timed out")
		}
	}
	require.EqualValues(t, 1, counted.sessions.Load())
	require.Len(t, counted.destinations, 2)
	require.Equal(t, remote.LocalAddr().String(), <-counted.destinations)
	require.Equal(t, remote.LocalAddr().String(), <-counted.destinations)
}
