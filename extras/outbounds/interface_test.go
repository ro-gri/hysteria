package outbounds

import (
	"net"
	"testing"

	"github.com/apernet/hysteria/core/v2/server"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPluggableOutboundAdapter(t *testing.T) {
	ob := newMockPluggableOutbound(t)
	adapter := &PluggableOutboundAdapter{PluggableOutbound: ob}
	info := &server.RequestInfo{
		AuthID:     "gura",
		ClientAddr: &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234},
	}

	ob.EXPECT().TCP(&AddrEx{
		Host: "only.fans",
		Port: 443,
		Info: info,
	}).Return(nil, nil).Once()
	conn, err := adapter.TCP(info, "only.fans:443")
	assert.Nil(t, conn)
	assert.Nil(t, err)

	mc := newMockUDPConn(t)
	mc.EXPECT().ReadFrom(mock.Anything).RunAndReturn(func(bs []byte) (int, *AddrEx, error) {
		return copy(bs, "gura"), &AddrEx{
			Host: "gura.com",
			Port: 2333,
		}, nil
	}).Once()
	mc.EXPECT().WriteTo([]byte("gawr"), &AddrEx{
		Host: "another.hololive.tv",
		Port: 1551,
	}).Return(4, nil).Once()
	ob.EXPECT().UDP(&AddrEx{
		Host: "hololive.tv",
		Port: 8999,
		Info: info,
	}).Return(mc, nil).Once()

	uConn, err := adapter.UDP(info, "hololive.tv:8999")
	assert.Nil(t, err)
	assert.NotNil(t, uConn)
	n, err := uConn.WriteTo([]byte("gawr"), "another.hololive.tv:1551")
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	bs := make([]byte, 1024)
	n, addr, err := uConn.ReadFrom(bs)
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "gura", string(bs[:n]))
	assert.Equal(t, "gura.com:2333", addr)

	ob.EXPECT().CheckUDP(&AddrEx{
		Host: "hololive.tv",
		Port: 9000,
		Info: info,
	}).Return(nil).Once()
	assert.Nil(t, adapter.CheckUDP(info, "hololive.tv:9000"))
}

func TestUDPAdapterCachesLastResolvedDestination(t *testing.T) {
	ob := newMockPluggableOutbound(t)
	mc := newMockUDPConn(t)
	var lookups []string
	resolve := func(addr *AddrEx) {
		lookups = append(lookups, addr.Host)
		addr.ResolveInfo = &ResolveInfo{IPv4: net.ParseIP("192.0.2.1")}
	}
	ob.EXPECT().UDP(mock.Anything).RunAndReturn(func(addr *AddrEx) (UDPConn, error) {
		resolve(addr) // The initial request passes through the resolver pipeline.
		return mc, nil
	}).Once()
	adapter := &PluggableOutboundAdapter{PluggableOutbound: ob, Resolve: resolve}
	conn, err := adapter.UDP(nil, "first.example:53")
	assert.NoError(t, err)
	mc.EXPECT().Close().Return(nil).Once()
	defer conn.Close()
	for _, host := range []string{"first.example", "first.example", "second.example", "second.example", "first.example"} {
		mc.EXPECT().WriteTo(mock.Anything, mock.Anything).RunAndReturn(func(b []byte, addr *AddrEx) (int, error) {
			assert.Equal(t, host, addr.Host)
			assert.Equal(t, uint16(53), addr.Port)
			assert.True(t, net.ParseIP("192.0.2.1").Equal(addr.ResolveInfo.IPv4))
			// A downstream ACL rewrite must not change the cached address.
			addr.Host = "rewritten.example"
			addr.ResolveInfo = &ResolveInfo{IPv4: net.ParseIP("198.51.100.1")}
			return len(b), nil
		}).Once()
		_, err = conn.WriteTo([]byte("packet"), net.JoinHostPort(host, "53"))
		assert.NoError(t, err)
	}
	assert.Equal(t, []string{"first.example", "first.example", "second.example", "first.example"}, lookups)
}
