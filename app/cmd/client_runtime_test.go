package cmd

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestLoadEmbeddedClientConfig(t *testing.T) {
	v := viper.New()
	v.SetConfigFile(filepath.Join(t.TempDir(), "missing.yaml"))
	_, err := readClientConfig(v)
	require.Error(t, err)
	p := filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, os.WriteFile(p, []byte("server: localhost:443\nauth: client-password\nlazy: true\nsocks5:\n  listen: 127.0.0.1:1080\n"), 0o600))
	v.SetConfigFile(p)
	c, err := readClientConfig(v)
	require.NoError(t, err)
	require.Equal(t, "localhost:443", c.Server)
	require.Equal(t, "client-password", c.Auth)
	require.True(t, c.Lazy)
	require.NotNil(t, c.SOCKS5)
	require.NoError(t, os.WriteFile(p, []byte("auth: ["), 0o600))
	_, err = readClientConfig(v)
	require.Error(t, err)
}

func TestOutboundOnlyClientRemainsRunning(t *testing.T) {
	_, results, cleanup, err := innerRunClient(&clientConfig{Server: "127.0.0.1:1", Lazy: true}, true, true)
	require.NoError(t, err)
	defer cleanup()
	select {
	case <-results:
		t.Fatal("outbound-only client must remain running")
	default:
	}
}

func TestHysteria2OutboundRequiresClient(t *testing.T) {
	config := serverConfig{Outbounds: []serverConfigOutboundEntry{{Name: "exit", Type: "hysteria2"}}}
	_, err := config.outboundEntries(nil)
	require.ErrorContains(t, err, "requires --client-config")
	// Existing outbound-only server configurations need no client file.
	config.Outbounds[0].Type = "direct"
	entries, err := config.outboundEntries(nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestEmbeddedClientLocalModeFailureStops(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	_, results, cleanup, err := innerRunClient(&clientConfig{Server: "127.0.0.1:1", Lazy: true, SOCKS5: &socks5Config{Listen: occupied.Addr().String()}}, true, true)
	require.NoError(t, err)
	defer cleanup()
	select {
	case result := <-results:
		require.False(t, result.OK)
		require.Error(t, result.Err)
	case <-time.After(3 * time.Second):
		t.Fatal("listener failure was not reported")
	}
	require.NoError(t, cleanup())
}

func TestStandaloneClientStillRequiresMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server: 127.0.0.1:1\nlazy: true\n"), 0o600))
	v := viper.New()
	v.SetConfigFile(path)
	require.ErrorContains(t, runClient(v), "no mode specified")
}

func TestStandaloneClientReportsLocalModeFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	path := filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server: 127.0.0.1:1\nlazy: true\nsocks5:\n  listen: "+occupied.Addr().String()+"\n"), 0o600))
	v := viper.New()
	v.SetConfigFile(path)
	require.ErrorContains(t, runClient(v), "failed to run SOCKS5 server")
}
