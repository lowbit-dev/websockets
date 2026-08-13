package websockets

import (
	"context"
	"crypto/tls"
	"net"
)

// ConnectionPool manages the lifecycle of Conn structs and their shared memory,
// eliminating heap allocations during high-churn connection spikes.
type ConnPool struct {
	connPool     TypedPool[Conn]
	writePool    TypedPool[[]byte]
	streamPool   TypedPool[[]byte]
	maxFrameSize int64
	maxReadLimit int64
}

func NewConnPool(maxReadLimit, maxFrameSize int64) *ConnPool {
	p := &ConnPool{
		maxFrameSize: maxFrameSize,
		writePool: *NewTypedPool(func() *[]byte {
			b := make([]byte, 14+maxFrameSize)
			return &b
		}),
		streamPool: *NewTypedPool(func() *[]byte {
			b := make([]byte, maxFrameSize*2)
			return &b
		}),
	}

	p.connPool = *NewTypedPool(func() *Conn {
		return &Conn{
			maxFrameSize: p.maxFrameSize,
			maxReadLimit: p.maxReadLimit,
		}
	})

	return p
}

func (p *ConnPool) SetFactory(factory func() *Conn) {
	p.connPool = *NewTypedPool(factory)
}

// Acquire retrieves a clean Conn and binds it to the incoming socket.
func (p *ConnPool) Acquire(conn net.Conn, maxReadLimit, maxFrameSize int64) *Conn {
	c := p.connPool.Get()

	c.ctx, c.ctxCancel = context.WithCancelCause(context.Background())

	c.underlying = conn
	c.writeBufPool = &p.writePool
	c.streamBufPool = &p.streamPool

	if maxReadLimit > 0 {
		c.maxReadLimit = maxReadLimit
	}

	if maxFrameSize > 0 {
		c.maxFrameSize = maxFrameSize
	}

	c.pingHandler = func(d []byte) error { return c.WriteFrame(true, 0, OpCodePong, d) }
	c.pongHandler = func(_ []byte) error { return nil }
	c.closeHandler = func(oc CloseCode, b []byte) {}

	if tcp, ok := conn.(*net.TCPConn); ok {
		c.writeTCP = tcp
	} else if tc, ok := conn.(*tls.Conn); ok {
		c.writeTLS = tc
	}

	return c
}

// Release strips the socket and returns the memory footprint to the application.
func (p *ConnPool) Release(c *Conn) {
	if c.ctxCancel != nil {
		c.ctxCancel(ErrConnectionReleasedToPool)
	}

	c.ctx = nil
	c.ctxCancel = nil
	c.underlying = nil
	c.writeTCP = nil
	c.pingHandler = nil
	c.pongHandler = nil
	c.closeHandler = nil
	c.maxFrameSize = p.maxFrameSize
	c.maxReadLimit = p.maxReadLimit
	p.connPool.Put(c)
}
