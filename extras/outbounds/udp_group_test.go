package outbounds

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/outbounds/acl"
	"github.com/stretchr/testify/require"
)

type groupPacket struct {
	data []byte
	addr *AddrEx
	err  error
}
type groupTestConn struct {
	writeStarted chan struct{}
	reads        chan groupPacket
	writes       chan groupPacket
	done         chan struct{}
	once         sync.Once
	closes       atomic.Int32
}

func newGroupTestConn() *groupTestConn {
	return &groupTestConn{reads: make(chan groupPacket, 32), writes: make(chan groupPacket, 32), done: make(chan struct{})}
}
func (c *groupTestConn) ReadFrom(b []byte) (int, *AddrEx, error) {
	select {
	case p := <-c.reads:
		return copy(b, p.data), p.addr, p.err
	case <-c.done:
		return 0, nil, net.ErrClosed
	}
}
func (c *groupTestConn) WriteTo(b []byte, a *AddrEx) (int, error) {
	if c.writeStarted != nil {
		c.writeStarted <- struct{}{}
	}
	cp := *a
	select {
	case c.writes <- groupPacket{data: append([]byte(nil), b...), addr: &cp}:
		return len(b), nil
	case <-c.done:
		return 0, net.ErrClosed
	}
}
func (c *groupTestConn) Close() error {
	c.once.Do(func() { c.closes.Add(1); close(c.done) })
	return nil
}

type groupTestOutbound struct {
	conn  *groupTestConn
	dials int
}

func (o *groupTestOutbound) TCP(*AddrEx) (net.Conn, error) { return nil, errors.New("not TCP") }
func (o *groupTestOutbound) CheckUDP(*AddrEx) error        { return nil }
func (o *groupTestOutbound) UDP(*AddrEx) (UDPConn, error)  { o.dials++; return o.conn, nil }

func groupFixture(t *testing.T) (*UDPConnGroup, *groupTestOutbound, *groupTestOutbound) {
	t.Helper()
	a := &groupTestOutbound{conn: newGroupTestConn()}
	b := &groupTestOutbound{conn: newGroupTestConn()}
	rules, err := acl.TranslateText("a(first.example)\nreject(blocked.example)\nb(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{{"a", a}, {"b", b}}, nil)
	require.NoError(t, err)
	conn, err := engine.UDP(&AddrEx{Host: "first.example", Port: 53})
	require.NoError(t, err)
	g := conn.(*UDPConnGroup)
	t.Cleanup(func() { g.Close() })
	return g, a, b
}
func groupRead(t *testing.T, g *UDPConnGroup) groupPacket {
	t.Helper()
	ch := make(chan groupPacket, 1)
	go func() {
		buf := make([]byte, server.MaxUDPSize)
		n, a, e := g.ReadFrom(buf)
		ch <- groupPacket{buf[:n], a, e}
	}()
	select {
	case p := <-ch:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("group read blocked")
		return groupPacket{}
	}
}
func TestUDPConnGroupRoutesAndMerges(t *testing.T) {
	g, a, b := groupFixture(t)
	require.Equal(t, 3, cap(g.replies)) // Two configured exits and the built-in DIRECT.
	require.Zero(t, a.dials)
	require.Zero(t, b.dials)
	for _, host := range []string{"first.example", "second.example", "first.example", "third.example"} {
		_, err := g.WriteTo([]byte(host), &AddrEx{Host: host, Port: 53})
		require.NoError(t, err)
	}
	require.Equal(t, 1, a.dials)
	require.Equal(t, 1, b.dials)
	require.Equal(t, "first.example", (<-a.conn.writes).addr.Host)
	require.Equal(t, "second.example", (<-b.conn.writes).addr.Host)
	_, err := g.WriteTo(nil, &AddrEx{Host: "blocked.example", Port: 53})
	require.ErrorIs(t, err, errRejected)
	// A silent first exit must not block replies from the other exit.
	b.conn.reads <- groupPacket{data: []byte("second"), addr: &AddrEx{Host: "second.example", Port: 53}}
	p := groupRead(t, g)
	require.NoError(t, p.err)
	require.Equal(t, "second", string(p.data))
	require.Equal(t, "second.example", p.addr.Host)
	for _, data := range []string{"one", "two", "three", ""} {
		a.conn.reads <- groupPacket{data: []byte(data), addr: &AddrEx{Host: "first.example", Port: 53}}
	}
	for _, data := range []string{"one", "two", "three", ""} {
		p = groupRead(t, g)
		require.NoError(t, p.err)
		require.Equal(t, data, string(p.data))
	}
}
func TestUDPConnGroupReadErrorIsQueuedUntilOwnerCloses(t *testing.T) {
	g, a, b := groupFixture(t)
	for _, host := range []string{"first.example", "second.example"} {
		_, err := g.WriteTo(nil, &AddrEx{Host: host})
		require.NoError(t, err)
	}
	a.conn.reads <- groupPacket{data: []byte("already received"), addr: &AddrEx{Host: "first.example"}}
	require.Eventually(t, func() bool { return len(g.replies) == 1 }, time.Second, time.Millisecond)
	failure := errors.New("exit failed")
	b.conn.reads <- groupPacket{err: failure}
	require.Eventually(t, func() bool { return len(g.replies) == 2 }, time.Second, time.Millisecond)
	p := groupRead(t, g)
	require.NoError(t, p.err)
	require.Equal(t, "already received", string(p.data))
	p = groupRead(t, g)
	require.ErrorIs(t, p.err, failure)
	require.Zero(t, a.conn.closes.Load())
	require.Zero(t, b.conn.closes.Load())
	require.NoError(t, g.Close())
	require.NoError(t, g.Close())
	require.EqualValues(t, 1, a.conn.closes.Load())
	require.EqualValues(t, 1, b.conn.closes.Load())
	// Reused connections report their own closed state; the group does not reopen them.
	conn, err := g.getOrCreateConnection(b, &AddrEx{Host: "second.example"})
	require.NoError(t, err)
	require.Same(t, b.conn, conn)
	require.Equal(t, 1, b.dials)
}
func TestUDPConnGroupCloseUnblocksReaders(t *testing.T) {
	g, a, b := groupFixture(t)
	for _, host := range []string{"first.example", "second.example"} {
		_, err := g.WriteTo(nil, &AddrEx{Host: host})
		require.NoError(t, err)
	}
	// One reader waits for delivery/consumption; another is blocked in ReadFrom.
	a.conn.reads <- groupPacket{data: []byte("queued"), addr: &AddrEx{Host: "first.example"}}
	require.Eventually(t, func() bool { return len(g.replies) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, g.Close())
	reply := groupRead(t, g)
	require.NoError(t, reply.err)
	require.Equal(t, "queued", string(reply.data))
	require.ErrorIs(t, groupRead(t, g).err, net.ErrClosed)
	require.EqualValues(t, 1, a.conn.closes.Load())
	require.EqualValues(t, 1, b.conn.closes.Load())
}
func TestUDPConnGroupShortBuffer(t *testing.T) {
	g, a, _ := groupFixture(t)
	_, err := g.WriteTo(nil, &AddrEx{Host: "first.example"})
	require.NoError(t, err)
	a.conn.reads <- groupPacket{data: []byte("long"), addr: &AddrEx{Host: "first.example"}}
	buf := make([]byte, 1)
	n, _, err := g.ReadFrom(buf)
	require.Equal(t, 1, n)
	require.Equal(t, "l", string(buf))
	require.NoError(t, err)
	a.conn.reads <- groupPacket{data: []byte("next"), addr: &AddrEx{Host: "first.example"}}
	require.Equal(t, "next", string(groupRead(t, g).data))
}
func TestUDPConnGroupResolverAndAuth(t *testing.T) {
	a := &groupTestOutbound{conn: newGroupTestConn()}
	b := &groupTestOutbound{conn: newGroupTestConn()}
	legacy, err := acl.TranslateText("a(127.0.0.0/8)\nb(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(legacy, []OutboundEntry{{"a", a}, {"b", b}}, nil)
	require.NoError(t, err)
	resolver := NewSystemResolver(engine)
	adapter := &PluggableOutboundAdapter{PluggableOutbound: resolver, Resolve: resolver.(Resolver).Resolve}
	info := &server.RequestInfo{AuthID: "alice"}
	conn, err := adapter.UDP(info, "192.0.2.1:53")
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.WriteTo(nil, "127.0.0.1:53")
	require.NoError(t, err)
	packet := <-a.conn.writes
	require.Equal(t, info, packet.addr.Info)
	require.NotNil(t, packet.addr.ResolveInfo.IPv4)
	_, err = conn.WriteTo(nil, "192.0.2.1:53")
	require.NoError(t, err)
	<-b.conn.writes
	require.Equal(t, 1, a.dials)
	require.Equal(t, 1, b.dials)
	rules := []acl.Rule{{Outbound: "a", Match: `auth.id == "alice"`}, {Outbound: "b"}}
	authEngine, err := NewACLEngine(rules, []OutboundEntry{{"a", a}, {"b", b}}, nil)
	require.NoError(t, err)
	authAdapter := &PluggableOutboundAdapter{PluggableOutbound: authEngine}
	authConn, err := authAdapter.UDP(info, "example.com:53")
	require.NoError(t, err)
	defer authConn.Close()
	_, err = authConn.WriteTo(nil, "other.example:53")
	require.NoError(t, err)
	require.Equal(t, "other.example", (<-a.conn.writes).addr.Host)
}

func TestUDPConnGroupHijack(t *testing.T) {
	ob := &groupTestOutbound{conn: newGroupTestConn()}
	rules, err := acl.TranslateText("exit(original.example,*,192.0.2.5)\nexit(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{{"exit", ob}}, nil)
	require.NoError(t, err)
	adapter := &PluggableOutboundAdapter{PluggableOutbound: engine}
	group, err := adapter.UDP(nil, "original.example:53")
	require.NoError(t, err)
	defer group.Close()
	for i := 0; i < 2; i++ {
		_, err = group.WriteTo(nil, "original.example:53")
		require.NoError(t, err)
		require.Equal(t, "192.0.2.5", (<-ob.conn.writes).addr.Host)
		require.Equal(t, "original.example", group.(*udpConnAdapter).lastWrite.Load().Host)
	}
	_, err = group.WriteTo(nil, "other.example:53")
	require.NoError(t, err)
	require.Equal(t, "other.example", (<-ob.conn.writes).addr.Host)
	require.Equal(t, 1, ob.dials)
}

func TestUDPConnGroupBoundDirectFamilies(t *testing.T) {
	v4, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	defer v4.Close()
	v6, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1")})
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer v6.Close()
	for _, conn := range []*net.UDPConn{v4, v6} {
		go func(c *net.UDPConn) {
			buf := make([]byte, 64)
			for {
				n, a, e := c.ReadFromUDP(buf)
				if e != nil {
					return
				}
				c.WriteToUDP(buf[:n], a)
			}
		}(conn)
	}
	direct := NewDirectOutboundSimple(DirectOutboundModeAuto).(*directOutbound)
	direct.BindIP4 = net.ParseIP("127.0.0.1")
	direct.BindIP6 = net.ParseIP("::1")
	rules, err := acl.TranslateText("exit(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{{"exit", direct}}, nil)
	require.NoError(t, err)
	resolver := NewSystemResolver(engine)
	adapter := &PluggableOutboundAdapter{PluggableOutbound: resolver, Resolve: resolver.(Resolver).Resolve}
	conn, err := adapter.UDP(nil, v4.LocalAddr().String())
	require.NoError(t, err)
	defer conn.Close()
	for _, dest := range []string{v4.LocalAddr().String(), v6.LocalAddr().String(), v4.LocalAddr().String()} {
		_, err = conn.WriteTo([]byte(dest), dest)
		require.NoError(t, err)
		done := make(chan groupPacket, 1)
		go func() {
			buf := make([]byte, 64)
			n, a, e := conn.ReadFrom(buf)
			done <- groupPacket{data: buf[:n], addr: &AddrEx{Host: a}, err: e}
		}()
		select {
		case p := <-done:
			require.NoError(t, p.err)
			require.Equal(t, dest, string(p.data))
			require.Equal(t, dest, p.addr.Host)
		case <-time.After(3 * time.Second):
			t.Fatal("UDP echo timed out")
		}
	}
	group := conn.(*udpConnAdapter).UDPConn.(*UDPConnGroup)
	require.Len(t, group.connections, 2)
}

func TestUDPConnGroupConcurrentWriteAndClose(t *testing.T) {
	g, _, _ := groupFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = g.WriteTo([]byte("packet"), &AddrEx{Host: "first.example"}) }()
	}
	require.NoError(t, g.Close())
	wg.Wait()
	_, err := g.WriteTo(nil, &AddrEx{Host: "second.example"})
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestUDPConnGroupReusesOutboundAcrossNames(t *testing.T) {
	ob := &groupTestOutbound{conn: newGroupTestConn()}
	rules, err := acl.TranslateText("first(first.example)\nalias(second.example)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{{"first", ob}, {"alias", ob}}, nil)
	require.NoError(t, err)
	group, err := engine.UDP(&AddrEx{Host: "first.example"})
	require.NoError(t, err)
	defer group.Close()
	require.Equal(t, 2, cap(group.(*UDPConnGroup).replies)) // One exit and the built-in DIRECT.
	for _, host := range []string{"first.example", "second.example", "default.example"} {
		_, err = group.WriteTo(nil, &AddrEx{Host: host})
		require.NoError(t, err)
	}
	require.Equal(t, 1, ob.dials)
}

func TestUDPConnGroupBoundDirectRequiresResolvedAddress(t *testing.T) {
	direct := NewDirectOutboundSimple(DirectOutboundModeAuto).(*directOutbound)
	direct.BindIP4 = net.ParseIP("127.0.0.1")
	rules, err := acl.TranslateText("exit(all)", "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, []OutboundEntry{{"exit", direct}}, nil)
	require.NoError(t, err)
	group, err := engine.UDP(&AddrEx{Host: "localhost", Port: 53})
	require.NoError(t, err)
	defer group.Close()
	_, err = group.WriteTo(nil, &AddrEx{Host: "localhost", Port: 53})
	require.ErrorAs(t, err, new(noAddressError))
	require.Empty(t, group.(*UDPConnGroup).connections)
}

func TestUDPConnGroupBlockedWriteDoesNotBlockOtherExit(t *testing.T) {
	g, a, _ := groupFixture(t)
	a.conn.writeStarted = make(chan struct{}, 1)
	// Fill the fake socket's queue so the first WriteTo blocks inside it.
	for i := 0; i < cap(a.conn.writes); i++ {
		a.conn.writes <- groupPacket{}
	}
	first := make(chan error, 1)
	go func() { _, err := g.WriteTo(nil, &AddrEx{Host: "first.example"}); first <- err }()
	select {
	case <-a.conn.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	second := make(chan error, 1)
	go func() { _, err := g.WriteTo(nil, &AddrEx{Host: "second.example"}); second <- err }()
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second exit blocked behind first")
	}
	require.NoError(t, g.Close())
	select {
	case err := <-first:
		require.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock write")
	}
}

func TestUDPConnGroupConcurrentCreationReusesConnection(t *testing.T) {
	g, a, _ := groupFixture(t)
	start := make(chan struct{})
	results := make(chan error, 16)
	for i := 0; i < cap(results); i++ {
		go func() {
			<-start
			_, err := g.WriteTo(nil, &AddrEx{Host: "first.example", Port: 53})
			results <- err
		}()
	}
	close(start)
	for i := 0; i < cap(results); i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("concurrent write did not finish")
		}
	}
	require.Equal(t, 1, a.dials)
	require.Len(t, a.conn.writes, cap(results))
}

func TestUDPConnGroupMaxSizeReply(t *testing.T) {
	g, a, _ := groupFixture(t)
	_, err := g.WriteTo(nil, &AddrEx{Host: "first.example"})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{0xAB}, server.MaxUDPSize)
	a.conn.reads <- groupPacket{data: payload, addr: &AddrEx{Host: "first.example"}}
	reply := groupRead(t, g)
	require.NoError(t, reply.err)
	require.Equal(t, payload, reply.data)
}

func TestUDPConnGroupCloseDrainsQueuedReplies(t *testing.T) {
	g, a, b := groupFixture(t)
	for _, host := range []string{"first.example", "second.example"} {
		_, err := g.WriteTo(nil, &AddrEx{Host: host})
		require.NoError(t, err)
	}
	a.conn.reads <- groupPacket{data: []byte("first"), addr: &AddrEx{Host: "first.example"}}
	b.conn.reads <- groupPacket{data: []byte("second"), addr: &AddrEx{Host: "second.example"}}
	require.Eventually(t, func() bool { return len(g.replies) == 2 }, time.Second, time.Millisecond)
	require.NoError(t, g.Close())
	var received []string
	for i := 0; i < 2; i++ {
		reply := groupRead(t, g)
		require.NoError(t, reply.err)
		received = append(received, string(reply.data))
	}
	require.ElementsMatch(t, []string{"first", "second"}, received)
	require.ErrorIs(t, groupRead(t, g).err, net.ErrClosed)
}

func TestUDPConnGroupQueuesAllConfiguredOutbounds(t *testing.T) {
	const count = 20
	var entries []OutboundEntry
	var rulesText []string
	var exits []*groupTestOutbound
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("exit%d", i)
		ob := &groupTestOutbound{conn: newGroupTestConn()}
		exits = append(exits, ob)
		entries = append(entries, OutboundEntry{Name: name, Outbound: ob})
		rulesText = append(rulesText, fmt.Sprintf("%s(%s.example)", name, name))
	}
	direct := NewDirectOutboundSimple(DirectOutboundModeAuto).(*directOutbound)
	direct.BindIP4 = net.ParseIP("127.0.0.1")
	direct.BindIP6 = net.ParseIP("::1")
	entries = append(entries, OutboundEntry{Name: "direct", Outbound: direct})
	rules, err := acl.TranslateText(strings.Join(rulesText, "\n"), "")
	require.NoError(t, err)
	engine, err := NewACLEngine(rules, entries, nil)
	require.NoError(t, err)
	conn, err := engine.UDP(&AddrEx{Host: "exit0.example"})
	require.NoError(t, err)
	g := conn.(*UDPConnGroup)
	t.Cleanup(func() { g.Close() })
	// Twenty configured exits plus the bound DIRECT's two address families.
	// The implicit default aliases exit0; reject cannot create a connection.
	require.Equal(t, count+2, cap(g.replies))
	var expected []string
	for i, ob := range exits {
		host := fmt.Sprintf("exit%d.example", i)
		expected = append(expected, host)
		_, err := g.WriteTo(nil, &AddrEx{Host: host})
		require.NoError(t, err)
		ob.conn.reads <- groupPacket{data: []byte(host), addr: &AddrEx{Host: host}}
	}
	// Every exit must enqueue its reply before the caller starts reading,
	// including when there are more exits than the former fixed limit of 16.
	require.Eventually(t, func() bool { return len(g.replies) == count }, time.Second, time.Millisecond)
	require.NoError(t, g.Close())
	var received []string
	for range count {
		reply := groupRead(t, g)
		require.NoError(t, reply.err)
		received = append(received, string(reply.data))
	}
	require.ElementsMatch(t, expected, received)
	require.ErrorIs(t, groupRead(t, g).err, net.ErrClosed)
}
