package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jiho-dev/rpc/internal/svc"
)

const (
	// Defaults used by HandleHTTP
	DefaultRPCPath   = "/_goRPC_"
	DefaultDebugPath = "/debug/rpc"
)

type OnNewConnection func(s *Server, c *Connection) bool

type Connection struct {
	Id             uuid.UUID
	IsServer       bool
	Codec          ServerCodec
	RecvReqPending *svc.Pending
	Sending        sync.Mutex
	Wg             sync.WaitGroup

	// for client side
	Mutex          sync.Mutex // protects following
	Seq            uint64
	SendReqPending map[uint64]*Call

	Closing  bool // user has called Close
	Shutdown bool // server has told us to stop
}

// Request is a header written before every RPC call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Request struct {
	ServiceMethod string   // format: "Service.Method"
	Seq           uint64   // sequence number chosen by client
	next          *Request // for free list in Server
}

// Response is a header written before every RPC return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Response struct {
	ServiceMethod string    // echoes that of the Request
	Seq           uint64    // echoes that of the request
	Error         string    // error, if any.
	next          *Response // for free list in Server
}

type Notify struct {
	Code   int
	Notice string
}

// Server represents an RPC Server.
type Server struct {
	serviceMap sync.Map   // map[string]*service
	reqLock    sync.Mutex // protects freeReq
	freeReq    *Request
	respLock   sync.Mutex // protects freeResp
	freeResp   *Response

	// for client
	Conns     sync.Map // map[string]Connection
	onNewConn OnNewConnection
}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
var invalidRequest = struct{}{}

// NewServer returns a new Server.
func NewServer() *Server {
	s := &Server{}
	s.RegisterName("_goRPC_", &svc.GoRPC{})
	return s
}

func (server *Server) newConnection(codec ServerCodec, isServer bool) *Connection {
	clientId, err := uuid.NewUUID()
	if err != nil {
		return nil
	}

	return &Connection{
		Id:             clientId,
		IsServer:       isServer,
		Codec:          codec,
		RecvReqPending: svc.NewPending(),
		SendReqPending: make(map[uint64]*Call),
	}
}

// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
// ServeConn uses the gob wire format (see package gob) on the
// connection. To use an alternate codec, use ServeCodec.
// See NewClient's comment for information about concurrent access.
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	codec := NewGobCodec(conn)
	server.ServeCodec(codec)
}

// ServeCodec is like ServeConn but uses the specified codec to
// decode requests and encode responses.
func (server *Server) ServeCodec(codec ServerCodec) {
	conn := server.newConnection(codec, true)
	server.Conns.Store(conn.Id, conn)

	if server.onNewConn != nil {
		server.onNewConn(server, conn)
	}

	server.runRecvLoop(conn)

	// We've seen that there are no more requests.
	// Wait for responses to be sent before closing codec.
	conn.Wg.Wait()
	conn.Codec.Close()

	server.Conns.Delete(conn.Id)
}

func (server *Server) runRecvLoop(conn *Connection) {
	var lasterr error

	for {
		msgtype, keepReading, err := server.readRpcHeader(conn.Codec)
		lasterr = err
		if err != nil {
			if err != io.EOF {
				log.Printf("rpc: %s", err)
			}

			if !keepReading {
				break
			}

			continue
		}

		switch msgtype {
		default:
			fallthrough
		case MSG_TYPE_UNKNOWN:
			log.Printf("RPC HDR is Unknown: %d", msgtype)
			continue
		case MSG_TYPE_REQUEST:
			//log.Printf("RPC HDR is Request: %p", server)
			keepReading = server.onRequest(conn)
		case MSG_TYPE_RESPONSE:
			//log.Printf("RPC HDR is Response")
			keepReading = server.onResponse(conn)
		case MSG_TYPE_NOTIFY:
			log.Printf("RPC HDR is Notify")
			continue
		}

		if !keepReading {
			break
		}
	}

	if conn.IsServer {
		log.Printf("Stop Server side receiving service")
	} else {
		// Terminate pending calls.
		conn.Sending.Lock()
		conn.Mutex.Lock()
		conn.Shutdown = true
		closing := conn.Closing

		if lasterr == io.EOF {
			if closing {
				lasterr = ErrShutdown
			} else {
				lasterr = io.ErrUnexpectedEOF
			}
		}

		for _, call := range conn.SendReqPending {
			call.Error = lasterr
			call.done()
		}

		conn.Mutex.Unlock()
		conn.Sending.Unlock()

		if debugLog && lasterr != io.EOF && !closing {
			log.Println("rpc: client protocol error:", lasterr)
		}

		log.Printf("Stop client side receiving service")
		conn.Wg.Done()
	}
}

func (server *Server) onRequest(conn *Connection) (keepReading bool) {
	service, mtype, req, argv, replyv, keepReading, err := server.readRequest(conn.Codec)
	if err != nil {
		if debugLog && err != io.EOF {
			log.Println("rpc:", err)
		}
		if !keepReading {
			return
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			server.sendResponse(&conn.Sending, req, invalidRequest, conn.Codec, err.Error())
			server.freeRequest(req)
		}

		return
	}

	conn.Wg.Add(1)
	go service.call(server, &conn.Sending, conn.RecvReqPending, &conn.Wg, mtype, req, argv, replyv, conn.Codec)

	return
}

func (server *Server) onResponse(conn *Connection) (keepReading bool) {
	_, keepReading, err := server.readResponse(conn)
	if err != nil {
		if debugLog && err != io.EOF {
			log.Println("rpc:", err)
		}

		return
	}

	return
}

// ServeRequest is like ServeCodec but synchronously serves a single request.
// It does not close the codec upon completion.
func (server *Server) ServeRequest(codec ServerCodec) error {
	sending := new(sync.Mutex)
	pending := svc.NewPending()
	service, mtype, req, argv, replyv, keepReading, err := server.readRequest(codec)
	if err != nil {
		if !keepReading {
			return err
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			server.sendResponse(sending, req, invalidRequest, codec, err.Error())
			server.freeRequest(req)
		}
		return err
	}
	service.call(server, sending, pending, nil, mtype, req, argv, replyv, codec)
	return nil
}

func (server *Server) getRequest() *Request {
	server.reqLock.Lock()
	req := server.freeReq
	if req == nil {
		req = new(Request)
	} else {
		server.freeReq = req.next
		*req = Request{}
	}
	server.reqLock.Unlock()
	return req
}

func (server *Server) freeRequest(req *Request) {
	server.reqLock.Lock()
	req.next = server.freeReq
	server.freeReq = req
	server.reqLock.Unlock()
}

func (server *Server) getResponse() *Response {
	server.respLock.Lock()
	resp := server.freeResp
	if resp == nil {
		resp = new(Response)
	} else {
		server.freeResp = resp.next
		*resp = Response{}
	}
	server.respLock.Unlock()
	return resp
}

func (server *Server) freeResponse(resp *Response) {
	server.respLock.Lock()
	resp.next = server.freeResp
	server.freeResp = resp
	server.respLock.Unlock()
}

func (server *Server) readRequest(codec ServerCodec) (service *service, mtype *methodType, req *Request, argv, replyv reflect.Value, keepReading bool, err error) {

	service, mtype, req, keepReading, err = server.readRequestHeader(codec)
	if err != nil {
		log.Printf("ERR after readRequestHeader: %s", err)

		if !keepReading {
			return
		}
		// discard body
		codec.ReadRequestBody(nil)
		return
	}

	// Decode the argument value.
	argIsValue := false // if true, need to indirect before calling.
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true
	}
	// argv guaranteed to be a pointer now.
	if err = codec.ReadRequestBody(argv.Interface()); err != nil {
		return
	}
	if argIsValue {
		argv = argv.Elem()
	}

	replyv = reflect.New(mtype.ReplyType.Elem())

	switch mtype.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(mtype.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(mtype.ReplyType.Elem(), 0, 0))
	}
	return
}

func (server *Server) readResponse(conn *Connection) (res *Response, keepReading bool, err error) {
	res, keepReading, err = server.readResponseHeader(conn.Codec)
	if err != nil {
		log.Printf("ERR after readRequestHeader: %s", err)

		if !keepReading {
			return
		}
		// discard body
		conn.Codec.ReadRequestBody(nil)
		return
	}

	seq := res.Seq
	conn.Mutex.Lock()
	call := conn.SendReqPending[seq]
	delete(conn.SendReqPending, seq)
	conn.Mutex.Unlock()

	switch {
	case call == nil:
		// We've got no pending call. That usually means that
		// WriteRequest partially failed, and call was already
		// removed; response is a server telling us about an
		// error reading request body. We should still attempt
		// to read error body, but there's no one to give it to.
		err = conn.Codec.ReadResponseBody(nil)
		if err != nil {
			err = errors.New("reading error body: " + err.Error())
		}
	case res.Error != "":
		// We've got an error response. Give this to the request;
		// any subsequent requests will get the ReadResponseBody
		// error if there is one.
		call.Error = ServerError(res.Error)
		err = conn.Codec.ReadResponseBody(nil)
		if err != nil {
			err = errors.New("reading error body: " + err.Error())
		}
		call.done()
	default:
		err = conn.Codec.ReadResponseBody(call.Reply)
		if err != nil {
			call.Error = errors.New("reading body " + err.Error())
		}
		call.done()
	}

	return
}

func (server *Server) readRpcHeader(codec ServerCodec) (msgtype MessageType, keepReading bool, err error) {
	var hdr RpcHdr

	err = codec.ReadRpcHeader(&hdr)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}

		err = errors.New("rpc: server cannot decode RPC Header: " + err.Error())
		return
	}

	keepReading = true

	//log.Printf("##### RpcHdr: ver:%d:%d, type:%d #####", hdr.MsgVerMajor, hdr.MsgVerMinor, hdr.MsgType)

	if hdr.MsgMagicCode != MSG_MAGIC_CODE {
		err = fmt.Errorf("rpc: RPC Header Magic Code mismatched: Server magic: %x, Received magic: %x",
			MSG_MAGIC_CODE, hdr.MsgMagicCode)
	} else if hdr.MsgVerMajor != DEF_MSG_VER_MAJ || hdr.MsgVerMinor != DEF_MSG_VER_MIN {
		err = fmt.Errorf("rpc: RPC Header Version mismatched: Server ver: %d:%d, Received ver: %d:%d",
			DEF_MSG_VER_MAJ, DEF_MSG_VER_MIN, hdr.MsgVerMajor, hdr.MsgVerMinor)
	}

	msgtype = hdr.MsgType

	return
}

func (server *Server) readRequestHeader(codec ServerCodec) (svc *service, mtype *methodType, req *Request, keepReading bool, err error) {
	// Grab the request header.
	req = server.getRequest()

	err = codec.ReadRequestHeader(req)
	if err != nil {
		req = nil
		log.Printf("ERR ReadRequest: %s", err)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.New("rpc: server cannot decode request: " + err.Error())
		return
	}

	// We read the header successfully. If we see an error now,
	// we can still recover and move on to the next request.
	keepReading = true

	dot := strings.LastIndex(req.ServiceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc: service/method request ill-formed: " + req.ServiceMethod)
		return
	}
	serviceName := req.ServiceMethod[:dot]
	methodName := req.ServiceMethod[dot+1:]

	// Look up the request.
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc: can't find service " + req.ServiceMethod)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + req.ServiceMethod)
	}
	return
}

func (server *Server) readResponseHeader(codec ServerCodec) (res *Response, keepReading bool, err error) {
	// Grab the response header.
	res = server.getResponse()

	err = codec.ReadResponseHeader(res)
	if err != nil {
		res = nil
		log.Printf("ERR ReadResponse: %s", err)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return
		}
		err = errors.New("rpc: server cannot decode Response: " + err.Error())
		return
	}

	// We read the header successfully. If we see an error now,
	// we can still recover and move on to the next response.
	keepReading = true

	return
}

func (server *Server) sendRequest(conn *Connection, call *Call) {
	conn.Sending.Lock()
	defer conn.Sending.Unlock()

	// Register this call.
	conn.Mutex.Lock()

	if conn.Shutdown || conn.Closing {
		conn.Mutex.Unlock()
		call.Error = ErrShutdown
		call.done()
		return
	}

	if call.seq != 0 {
		// It has already been canceled, don't bother sending
		call.Error = context.Canceled
		conn.Mutex.Unlock()
		call.done()
		return
	}

	conn.Seq++
	seq := conn.Seq
	call.seq = seq

	conn.SendReqPending[seq] = call
	conn.Mutex.Unlock()

	// Encode and send the request.
	hdr := NewRpcHeader(MSG_TYPE_REQUEST)

	var req Request
	req.Seq = seq
	req.ServiceMethod = call.ServiceMethod
	err := conn.Codec.WriteRequest(hdr, &req, call.Args)
	if err != nil {
		conn.Mutex.Lock()
		call = conn.SendReqPending[seq]
		delete(conn.SendReqPending, seq)
		conn.Mutex.Unlock()

		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (server *Server) sendResponse(sending *sync.Mutex, req *Request, reply interface{}, codec ServerCodec, errmsg string) {
	hdr := NewRpcHeader(MSG_TYPE_RESPONSE)

	resp := server.getResponse()
	// Encode the response header
	resp.ServiceMethod = req.ServiceMethod
	if errmsg != "" {
		resp.Error = errmsg
		reply = invalidRequest
	}
	resp.Seq = req.Seq
	sending.Lock()
	err := codec.WriteResponse(hdr, resp, reply)
	if debugLog && err != nil {
		log.Println("rpc: writing response:", err)
	}
	sending.Unlock()
	server.freeResponse(resp)
}

/*
func (m *methodType) NumCalls() (n uint) {
	m.Lock()
	n = m.numCalls
	m.Unlock()
	return n
}
*/

func (server *Server) writeRequest(conn *Connection, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}

	call.Done = done
	server.sendRequest(conn, call)

	return call
}

func (server *Server) Call(ctx context.Context, conn *Connection, serviceMethod string, args interface{}, reply interface{}) error {
	ch := make(chan *Call, 2) // 2 for this call and cancel
	call := server.writeRequest(conn, serviceMethod, args, reply, ch)
	select {
	case <-call.Done:
		return call.Error
	case <-ctx.Done():
		// Cancel the pending request on the client
		conn.Mutex.Lock()
		seq := call.seq
		_, ok := conn.SendReqPending[seq]
		delete(conn.SendReqPending, seq)
		if seq == 0 {
			// hasn't been sent yet, non-zero will prevent send
			call.seq = 1
		}
		conn.Mutex.Unlock()

		// Cancel running request on the server
		if seq != 0 && ok {
			server.writeRequest(conn, "_goRPC_.Cancel", &svc.CancelArgs{Seq: seq}, nil, ch)
		}

		return ctx.Err()
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection. Accept blocks until the listener
// returns a non-nil error. The caller typically invokes Accept in a
// go statement.
func (server *Server) Accept(lis net.Listener, cb OnNewConnection) {
	server.onNewConn = cb

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Print("rpc.Serve: accept:", err.Error())
			return
		}

		go server.ServeConn(conn)
	}
}

// ServeConn runs the DefaultServer on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
// ServeConn uses the gob wire format (see package gob) on the
// connection. To use an alternate codec, use ServeCodec.
// See NewClient's comment for information about concurrent access.
func ServeConn(conn io.ReadWriteCloser) {
	DefaultServer.ServeConn(conn)
}

// ServeCodec is like ServeConn but uses the specified codec to
// decode requests and encode responses.
func ServeCodec(codec ServerCodec) {
	DefaultServer.ServeCodec(codec)
}

// ServeRequest is like ServeCodec but synchronously serves a single request.
// It does not close the codec upon completion.
func ServeRequest(codec ServerCodec) error {
	return DefaultServer.ServeRequest(codec)
}

// Accept accepts connections on the listener and serves requests
// to DefaultServer for each incoming connection.
// Accept blocks; the caller typically invokes it in a go statement.
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis, nil)
}
