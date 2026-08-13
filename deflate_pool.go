package websockets

import (
	"bytes"
	"compress/flate"
	"io"
)

type DeflateConnPool struct {
	wrapperPool        TypedPool[PerMessageDeflateConn]
	compressorPool     TypedPool[Compressor]
	decompressorPool   TypedPool[io.ReadCloser]
	compressBufferPool TypedPool[bytes.Buffer]
}

// NewDeflateConectionPool creates a new connection pool for the PerMessageDeflateConn's
// IMPORATANT: `no_context_takeover` must be negotiated
// during handshake since no state is kept between messages
func NewDeflateConnectionPool(level int) *DeflateConnPool {
	return &DeflateConnPool{
		wrapperPool: *NewTypedPool(func() *PerMessageDeflateConn {
			return &PerMessageDeflateConn{
				compressed: make([]byte, 0, 1024),
			}
		}),

		compressorPool: *NewTypedPool(func() *Compressor {
			w, _ := flate.NewWriter(nil, level)
			var comp Compressor = w
			return &comp
		}),

		compressBufferPool: *NewTypedPool(func() *bytes.Buffer {
			return &bytes.Buffer{}
		}),

		decompressorPool: *NewTypedPool(func() *io.ReadCloser {
			rc := flate.NewReader(bytes.NewReader(nil))
			return &rc
		}),
	}
}

func (p *DeflateConnPool) Acquire(base *Conn) *PerMessageDeflateConn {
	dc := p.wrapperPool.Get()
	dc.Conn = base

	dc.decompressorPool = &p.decompressorPool
	dc.compBufPool = &p.compressBufferPool
	dc.compressorPool = &p.compressorPool

	return dc
}

func (p *DeflateConnPool) Release(dc *PerMessageDeflateConn) {
	dc.Conn = nil
	dc.compressed = dc.compressed[:0]
	p.wrapperPool.Put(dc)
}
