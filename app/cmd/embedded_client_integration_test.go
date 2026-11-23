package cmd

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

func cascadeServer(t *testing.T, listen string, upstream client.Client) (server.Server, net.Addr) {
	t.Helper()
	if logger == nil {
		logger = zap.NewNop()
	}
	config := serverConfig{
		Listen: listen,
		TLS:    &serverConfigTLS{Cert: "../../core/internal/integration_tests/test.crt", Key: "../../core/internal/integration_tests/test.key"},
		Auth:   serverConfigAuth{Type: "password", Password: "cascade-test"},
	}
	if upstream != nil {
		config.Outbounds = []serverConfigOutboundEntry{{Name: "exit", Type: "hysteria2"}}
	}
	c, err := config.Config(upstream)
	require.NoError(t, err)
	s, err := server.NewServer(c)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})
	return s, c.Conn.LocalAddr()
}

func cascadeTCPEcho(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	return l.Addr().String()
}

func assertCascadeTCP(t *testing.T, c client.Client, addr string) {
	t.Helper()
	conn, err := c.TCP(addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	payload := bytes.Repeat([]byte("cascade"), 10000)
	done := make(chan error, 1)
	go func() { _, err := conn.Write(payload); done <- err }()
	received := make([]byte, len(payload))
	_, err = io.ReadFull(conn, received)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.Equal(t, payload, received)
}

func TestEmbeddedCascadeTCPUDPAndReconnect(t *testing.T) {
	b, baddr := cascadeServer(t, "127.0.0.1:0", nil)
	embedded, _, cleanup, err := innerRunClient(&clientConfig{Server: baddr.String(), Auth: "cascade-test", TLS: clientConfigTLS{Insecure: true}, QUIC: clientConfigQUIC{MaxIdleTimeout: 4 * time.Second, KeepAlivePeriod: 2 * time.Second}, Lazy: true}, true, true)
	require.NoError(t, err)
	defer cleanup()
	_, aaddr := cascadeServer(t, "127.0.0.1:0", embedded)
	c, _, err := client.NewClient(&client.Config{ServerAddr: aaddr, Auth: "cascade-test", TLSConfig: client.TLSConfig{InsecureSkipVerify: true}})
	require.NoError(t, err)
	defer c.Close()
	echo := cascadeTCPEcho(t)
	assertCascadeTCP(t, c, echo)
	assertCascadeTCP(t, c, echo) // Closing a stream must leave the shared client usable.
	udpEcho, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udpEcho.Close()
	go func() {
		buf := make([]byte, 65535)
		for {
			n, a, err := udpEcho.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = udpEcho.WriteTo(buf[:n], a)
		}
	}()
	for i := 0; i < 2; i++ {
		u, err := c.UDP()
		require.NoError(t, err)
		payload := bytes.Repeat([]byte{byte(i + 1)}, []int{128, 4000}[i])
		require.NoError(t, u.Send(payload, udpEcho.LocalAddr().String()))
		type reply struct {
			data []byte
			addr string
			err  error
		}
		done := make(chan reply, 1)
		go func() { d, a, e := u.Receive(); done <- reply{d, a, e} }()
		select {
		case r := <-done:
			require.NoError(t, r.err)
			require.Equal(t, payload, r.data)
			require.Equal(t, udpEcho.LocalAddr().String(), r.addr)
		case <-time.After(5 * time.Second):
			_ = u.Close()
			t.Fatal("UDP cascade timed out")
		}
		require.NoError(t, u.Close())
	}
	_ = b.Close()
	// A broken transport fails the current request; only later requests reconnect.
	conn, err := embedded.TCP(echo)
	if conn != nil {
		conn.Close()
	}
	require.Error(t, err)
	cascadeServer(t, baddr.String(), nil)
	assertCascadeTCP(t, embedded, echo)
	assertCascadeTCP(t, c, echo)
	require.NoError(t, cleanup())
	require.NoError(t, cleanup())
	_, err = embedded.TCP(echo)
	require.Error(t, err)
}

func TestEmbeddedSOCKSUsesSharedClient(t *testing.T) {
	_, baddr := cascadeServer(t, "127.0.0.1:0", nil)
	// Reserve then release a port: the normal mode runner owns binding it.
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := reserved.Addr().String()
	reserved.Close()
	_, _, cleanup, err := innerRunClient(&clientConfig{Server: baddr.String(), Auth: "cascade-test", TLS: clientConfigTLS{Insecure: true}, Lazy: true, SOCKS5: &socks5Config{Listen: addr}}, true, true)
	require.NoError(t, err)
	defer cleanup()
	require.Eventually(t, func() bool {
		c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if c != nil {
			c.Close()
		}
		return e == nil
	}, 3*time.Second, 10*time.Millisecond)
	dialer, err := proxy.SOCKS5("tcp", addr, nil, &net.Dialer{Timeout: 3 * time.Second})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", cascadeTCPEcho(t))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	data := make([]byte, 5)
	_, err = io.ReadFull(conn, data)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}
