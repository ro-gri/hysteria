package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/client"
	"github.com/apernet/hysteria/core/v2/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

type sharedModesAuthenticator struct{ connections atomic.Int32 }

func (a *sharedModesAuthenticator) Authenticate(_ net.Addr, password string, _ uint64) (bool, string) {
	if password != "shared-modes" {
		return false, ""
	}
	a.connections.Add(1)
	return true, "shared-modes"
}

func TestServerSOCKSAndHTTPShareClientConcurrently(t *testing.T) {
	if logger == nil {
		logger = zap.NewNop()
	}
	config := serverConfig{
		Listen: "127.0.0.1:0",
		TLS:    &serverConfigTLS{Cert: "../../core/internal/integration_tests/test.crt", Key: "../../core/internal/integration_tests/test.key"},
		Auth:   serverConfigAuth{Type: "password", Password: "shared-modes"},
	}
	bconfig, err := config.Config(nil)
	require.NoError(t, err)
	auth := &sharedModesAuthenticator{}
	bconfig.Authenticator = auth
	b, err := server.NewServer(bconfig)
	require.NoError(t, err)
	bdone := make(chan error, 1)
	go func() { bdone <- b.Serve() }()
	defer func() {
		b.Close()
		select {
		case <-bdone:
		case <-time.After(3 * time.Second):
			t.Error("exit did not stop")
		}
	}()
	socksPort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	httpPort, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	socksAddr, httpAddr := socksPort.Addr().String(), httpPort.Addr().String()
	socksPort.Close()
	httpPort.Close()
	shared, modes, cleanup, err := innerRunClient(&clientConfig{
		Server: bconfig.Conn.LocalAddr().String(), Auth: "shared-modes", TLS: clientConfigTLS{Insecure: true}, Lazy: true,
		SOCKS5: &socks5Config{Listen: socksAddr}, HTTP: &httpConfig{Listen: httpAddr},
	}, true, true)
	require.NoError(t, err)
	defer cleanup()
	_, aaddr := cascadeServer(t, "127.0.0.1:0", shared)
	incoming, _, err := client.NewClient(&client.Config{ServerAddr: aaddr, Auth: "cascade-test", TLSConfig: client.TLSConfig{InsecureSkipVerify: true}})
	require.NoError(t, err)
	defer incoming.Close()
	for _, addr := range []string{socksAddr, httpAddr} {
		require.Eventually(t, func() bool {
			c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if c != nil {
				c.Close()
			}
			return e == nil
		}, 3*time.Second, 10*time.Millisecond)
	}
	echo := cascadeTCPEcho(t)
	direct, err := incoming.TCP(echo)
	require.NoError(t, err)
	defer direct.Close()
	socksDialer, err := proxy.SOCKS5("tcp", socksAddr, nil, &net.Dialer{Timeout: 3 * time.Second})
	require.NoError(t, err)
	socks, err := socksDialer.Dial("tcp", echo)
	require.NoError(t, err)
	defer socks.Close()
	connect, err := net.DialTimeout("tcp", httpAddr, 3*time.Second)
	require.NoError(t, err)
	defer connect.Close()
	require.NoError(t, connect.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprintf(connect, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echo, echo)
	require.NoError(t, err)
	reader := bufio.NewReader(connect)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Zero(t, reader.Buffered())
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Write(data)
	}))
	defer target.Close()
	proxyURL, err := url.Parse("http://" + httpAddr)
	require.NoError(t, err)
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	run := func(conns map[string]net.Conn) {
		start := make(chan struct{})
		results := make(chan error, len(conns)+1)
		for name, conn := range conns {
			go func(name string, conn net.Conn) {
				<-start
				payload := bytes.Repeat([]byte(name+"/"), 8192)
				for i := 0; i < 8; i++ {
					if e := conn.SetDeadline(time.Now().Add(5 * time.Second)); e != nil {
						results <- e
						return
					}
					if _, e := conn.Write(payload); e != nil {
						results <- fmt.Errorf("%s write: %w", name, e)
						return
					}
					received := make([]byte, len(payload))
					if _, e := io.ReadFull(conn, received); e != nil {
						results <- fmt.Errorf("%s read: %w", name, e)
						return
					}
					if !bytes.Equal(payload, received) {
						results <- fmt.Errorf("%s payload mismatch", name)
						return
					}
				}
				results <- nil
			}(name, conn)
		}
		go func() {
			<-start
			for i := 0; i < 8; i++ {
				payload := bytes.Repeat([]byte("http-post"), 4096)
				r, e := httpClient.Post(target.URL, "application/octet-stream", bytes.NewReader(payload))
				if e != nil {
					results <- e
					return
				}
				data, e := io.ReadAll(r.Body)
				r.Body.Close()
				if e != nil {
					results <- e
					return
				}
				if r.StatusCode != http.StatusOK || !bytes.Equal(data, payload) {
					results <- fmt.Errorf("HTTP response mismatch: status=%d got=%d want=%d body-prefix=%q", r.StatusCode, len(data), len(payload), data[:min(len(data), 120)])
					return
				}
			}
			results <- nil
		}()
		close(start)
		for i := 0; i < len(conns)+1; i++ {
			select {
			case e := <-results:
				require.NoError(t, e)
			case <-time.After(15 * time.Second):
				t.Fatal("parallel modes timed out")
			}
		}
	}
	run(map[string]net.Conn{"server": direct, "socks": socks, "connect": connect})
	require.NoError(t, socks.Close())
	run(map[string]net.Conn{"server": direct, "connect": connect})
	replacement, err := socksDialer.Dial("tcp", echo)
	require.NoError(t, err)
	defer replacement.Close()
	run(map[string]net.Conn{"server": direct, "socks": replacement, "connect": connect})
	require.EqualValues(t, 1, auth.connections.Load(), "all paths must share one upstream QUIC connection")
	select {
	case result := <-modes:
		t.Fatalf("local mode stopped: %+v", result)
	default:
	}
}
