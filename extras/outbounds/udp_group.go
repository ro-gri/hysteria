package outbounds

import (
	"errors"
	"net"
	"sync"

	"github.com/apernet/hysteria/core/v2/server"
	"github.com/apernet/hysteria/extras/v2/outbounds/acl"
)

type udpGroupKey struct {
	outbound PluggableOutbound
	family   int
}

type udpGroupReply struct {
	err      error
	data     []byte
	addr     *AddrEx
	consumed chan struct{}
}

// UDPConnGroup routes each datagram independently and merges replies. A read
// failure is queued for the owner to consume and close the group.
type UDPConnGroup struct {
	engine      *aclEngine
	info        *server.RequestInfo
	mu          sync.RWMutex
	connections map[udpGroupKey]UDPConn
	replies     chan udpGroupReply
	closed      chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func newUDPConnGroup(engine *aclEngine, info *server.RequestInfo) *UDPConnGroup {
	return &UDPConnGroup{engine: engine, info: info, connections: make(map[udpGroupKey]UDPConn),
		replies: make(chan udpGroupReply, engine.maxUDPConnections), closed: make(chan struct{})}
}

func (g *UDPConnGroup) WriteTo(b []byte, addr *AddrEx) (int, error) {
	addr.Info = g.info
	outbound := g.engine.handle(addr, acl.ProtocolUDP)
	conn, err := g.getOrCreateConnection(outbound, addr)
	if err != nil {
		return 0, err
	}
	return conn.WriteTo(b, addr)
}

func (g *UDPConnGroup) getOrCreateConnection(outbound PluggableOutbound, addr *AddrEx) (UDPConn, error) {
	if err := outbound.CheckUDP(addr); err != nil {
		return nil, err
	}
	key := udpGroupKey{outbound: outbound}
	// A bound DIRECT socket can be restricted to one address family. Keep
	// separate sockets for the family selected by the DIRECT policy.
	if d, ok := outbound.(*directOutbound); ok && (d.BindIP4 != nil || d.BindIP6 != nil) {
		if addr.ResolveInfo == nil {
			return nil, noAddressError{IPv4: true, IPv6: true}
		}
		preferIPv6 := d.Mode == DirectOutboundMode6 || d.Mode == DirectOutboundMode64
		if addr.ResolveInfo.IPv6 != nil && (addr.ResolveInfo.IPv4 == nil || preferIPv6) {
			key.family = 6
		} else {
			key.family = 4
		}
	}
	g.mu.RLock()
	conn := g.connections[key]
	g.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	select {
	case <-g.closed:
		return nil, net.ErrClosed
	default:
	}
	if conn := g.connections[key]; conn != nil {
		return conn, nil
	}
	conn, err := outbound.UDP(addr)
	if err != nil {
		return nil, err
	}
	g.connections[key] = conn
	go g.receive(conn)
	return conn, nil
}

func (g *UDPConnGroup) receive(conn UDPConn) {
	buf := make([]byte, server.MaxUDPSize)
	consumed := make(chan struct{}, 1)
	for {
		n, addr, err := conn.ReadFrom(buf)
		select {
		case g.replies <- udpGroupReply{data: buf[:n], addr: addr, consumed: consumed, err: err}:
		case <-g.closed:
			return
		}
		if err != nil {
			return
		}
		// The next read must not overwrite bytes still being copied by ReadFrom.
		select {
		case <-consumed:
		case <-g.closed:
			return
		}
	}
}

func (g *UDPConnGroup) ReadFrom(b []byte) (int, *AddrEx, error) {
	var reply udpGroupReply
	select {
	case reply = <-g.replies:
	case <-g.closed:
		// Closing must not discard replies already queued for the caller.
		select {
		case reply = <-g.replies:
		default:
			return 0, nil, net.ErrClosed
		}
	}
	if reply.err != nil {
		return 0, nil, reply.err
	}
	n := copy(b, reply.data)
	reply.consumed <- struct{}{}
	return n, reply.addr, nil
}

func (g *UDPConnGroup) Close() error {
	g.closeOnce.Do(func() {
		g.mu.Lock()
		close(g.closed)
		connections := g.connections
		g.mu.Unlock()
		for _, conn := range connections {
			g.closeErr = errors.Join(g.closeErr, conn.Close())
		}
	})
	return g.closeErr
}
