package rpc

import (
	"bufio"
	"encoding/gob"
	//"errors"
	//"fmt"
	"io"
	"log"
)

type ServerCodec interface {
	ReadRpcHeader(h *RpcHdr) error

	ReadRequestHeader(*Request) error
	ReadRequestBody(interface{}) error
	WriteRequest(hdr *RpcHdr, r *Request, body interface{}) error

	ReadResponseHeader(r *Response) error
	ReadResponseBody(body interface{}) error
	WriteResponse(*RpcHdr, *Response, interface{}) error

	// Close can be called multiple times and must be idempotent.
	Close() error
}

///////////////////////////////////

type gobServerCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
	closed bool
}

//////////////////////////////////

func (c *gobServerCodec) ReadRpcHeader(h *RpcHdr) error {
	return c.dec.Decode(h)
}

func (c *gobServerCodec) ReadRequestHeader(r *Request) error {
	return c.dec.Decode(r)
}

func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *gobServerCodec) WriteRequest(hdr *RpcHdr, r *Request, body interface{}) (err error) {
	if err = c.enc.Encode(hdr); err != nil {
		return
	}
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return c.encBuf.Flush()
}

///////////////////////////////////////

func (c *gobServerCodec) ReadResponseHeader(r *Response) error {
	return c.dec.Decode(r)
}

func (c *gobServerCodec) ReadResponseBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *gobServerCodec) WriteResponse(hdr *RpcHdr, r *Response, body interface{}) (err error) {
	if err = c.enc.Encode(hdr); err != nil {
		if c.encBuf.Flush() == nil {
			// Gob couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Println("rpc: gob error encoding hdr:", err)
			c.Close()
		}
		return
	}
	if err = c.enc.Encode(r); err != nil {
		if c.encBuf.Flush() == nil {
			// Gob couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Println("rpc: gob error encoding response:", err)
			c.Close()
		}
		return
	}
	if err = c.enc.Encode(body); err != nil {
		if c.encBuf.Flush() == nil {
			// Was a gob problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			log.Println("rpc: gob error encoding body:", err)
			c.Close()
		}
		return
	}
	return c.encBuf.Flush()
}

func (c *gobServerCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

func NewGobCodec(conn io.ReadWriteCloser) *gobServerCodec {
	buf := bufio.NewWriter(conn)

	return &gobServerCodec{
		rwc:    conn,
		dec:    gob.NewDecoder(conn),
		enc:    gob.NewEncoder(buf),
		encBuf: buf,
	}
}
