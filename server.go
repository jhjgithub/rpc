// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	Package rpc is a fork of the stdlib net/rpc which is frozen. It adds
	support for context.Context on the client and server, including
	propogating cancellation. See the README at
	https://github.com/keegancsmith/rpc for motivation why this exists.

	The API is exactly the same, except Client.Call takes a context.Context,
	and Server methods are expected to take a context.Context as the first
	argument. The following is the original rpc godoc updated to include
	context.Context. Additionally the wire protocol is unchanged, so is
	backwards compatible with net/rpc clients.

	Package rpc provides access to the exported methods of an object across a
	network or other I/O connection.  A server registers an object, making it visible
	as a service with the name of the type of the object.  After registration, exported
	methods of the object will be accessible remotely.  A server may register multiple
	objects (services) of different types but it is an error to register multiple
	objects of the same type.

	Only methods that satisfy these criteria will be made available for remote access;
	other methods will be ignored:

		- the method's type is exported.
		- the method is exported.
		- the method has three arguments.
		- the method's first argument has type context.Context.
		- the method's last two arguments are exported (or builtin) types.
		- the method's third argument is a pointer.
		- the method has return type error.

	In effect, the method must look schematically like

		func (t *T) MethodName(ctx context.Context, argType T1, replyType *T2) error

	where T1 and T2 can be marshaled by encoding/gob.
	These requirements apply even if a different codec is used.
	(In the future, these requirements may soften for custom codecs.)

	The method's second argument represents the arguments provided by the caller; the
	third argument represents the result parameters to be returned to the caller.
	The method's return value, if non-nil, is passed back as a string that the client
	sees as if created by errors.New.  If an error is returned, the reply parameter
	will not be sent back to the client.

	The server may handle requests on a single connection by calling ServeConn.  More
	typically it will create a network listener and call Accept or, for an HTTP
	listener, HandleHTTP and http.Serve.

	A client wishing to use the service establishes a connection and then invokes
	NewClient on the connection.  The convenience function Dial (DialHTTP) performs
	both steps for a raw network connection (an HTTP connection).  The resulting
	Client object has two methods, Call and Go, that specify the service and method to
	call, a pointer containing the arguments, and a pointer to receive the result
	parameters.

	The Call method waits for the remote call to complete while the Go method
	launches the call asynchronously and signals completion using the Call
	structure's Done channel.

	Unless an explicit codec is set up, package encoding/gob is used to
	transport the data.

	Here is a simple example.  A server wishes to export an object of type Arith:

		package server

		import "errors"

		type Args struct {
			A, B int
		}

		type Quotient struct {
			Quo, Rem int
		}

		type Arith int

		func (t *Arith) Multiply(ctx context.Context, args *Args, reply *int) error {
			*reply = args.A * args.B
			return nil
		}

		func (t *Arith) Divide(ctx context.Context, args *Args, quo *Quotient) error {
			if args.B == 0 {
				return errors.New("divide by zero")
			}
			quo.Quo = args.A / args.B
			quo.Rem = args.A % args.B
			return nil
		}

	The server calls (for HTTP service):

		arith := new(Arith)
		rpc.Register(arith)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp", ":1234")
		if e != nil {
			log.Fatal("listen error:", e)
		}
		go http.Serve(l, nil)

	At this point, clients can see a service "Arith" with methods "Arith.Multiply" and
	"Arith.Divide".  To invoke one, a client first dials the server:

		client, err := rpc.DialHTTP("tcp", serverAddress + ":1234")
		if err != nil {
			log.Fatal("dialing:", err)
		}

	Then it can make a remote call:

		// Synchronous call
		args := &server.Args{7,8}
		var reply int
		err = client.Call(context.Background(), "Arith.Multiply", args, &reply)
		if err != nil {
			log.Fatal("arith error:", err)
		}
		fmt.Printf("Arith: %d*%d=%d", args.A, args.B, reply)

	or

		// Asynchronous call
		quotient := new(Quotient)
		divCall := client.Go("Arith.Divide", args, quotient, nil)
		replyCall := <-divCall.Done	// will be equal to divCall
		// check errors, print, etc.

	A server implementation will often provide a simple, type-safe wrapper for the
	client.

	The net/rpc package is frozen and is not accepting new features.
*/
package rpc

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jiho-dev/rpc/internal/svc"
)

const (
	// Defaults used by HandleHTTP
	DefaultRPCPath   = "/_goRPC_"
	DefaultDebugPath = "/debug/rpc"
)

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()
var typeOfCtx = reflect.TypeOf((*context.Context)(nil)).Elem()

type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

type MessageType int
type MessageVersion uint16
type OnNewSvcConn func(s *Server, c *SvcConn) bool

const (
	MSG_TYPE_UNKNOWN MessageType = iota
	MSG_TYPE_REQUEST
	MSG_TYPE_RESPONSE
	MSG_TYPE_NOTIFY
)

const (
	DEF_MSG_VER_MAJ = 1
	DEF_MSG_VER_MIN = 0
	MSG_MAGIC_CODE  = 0x43606326
)

type RpcHdr struct {
	MsgMagicCode uint32         // MSG_MAGIC_CODE
	MsgVerMajor  MessageVersion // DEF_MSG_VER_MAJ
	MsgVerMinor  MessageVersion // DEF_MSG_VER_MIN
	MsgType      MessageType    // MSG_TYPE_*
}

type Notify struct {
	Code   int
	Notice string
}

type SvcConn struct {
	Id             uuid.UUID
	IsServer       bool
	Codec          ServerCodec
	RecvReqPending *svc.Pending
	Sending        sync.Mutex
	Wg             sync.WaitGroup

	// for client side
	//ReqMutex    sync.Mutex // protects following
	Req            Request
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

// Server represents an RPC Server.
type Server struct {
	serviceMap sync.Map   // map[string]*service
	reqLock    sync.Mutex // protects freeReq
	freeReq    *Request
	respLock   sync.Mutex // protects freeResp
	freeResp   *Response

	// for client
	SvcConns     sync.Map // map[string]SvcConn
	onNewSvcConn OnNewSvcConn
}

// NewServer returns a new Server.
func NewServer() *Server {
	s := &Server{}
	s.RegisterName("_goRPC_", &svc.GoRPC{})
	return s
}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

func NewRpcHeader(t MessageType) *RpcHdr {
	return &RpcHdr{
		MsgMagicCode: MSG_MAGIC_CODE,
		MsgVerMajor:  DEF_MSG_VER_MAJ,
		MsgVerMinor:  DEF_MSG_VER_MIN,
		MsgType:      t,
	}
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method of exported type
//	- two arguments, both of exported type
//	- the second argument is a pointer
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (server *Server) Register(rcvr interface{}) error {
	return server.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.register(rcvr, name, true)
}

func (server *Server) register(rcvr interface{}, name string, useName bool) error {
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		s := "rpc.Register: no service name for type " + s.typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "rpc.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	s.name = sname

	// Install the methods
	s.method = suitableMethods(s.typ, true)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}

	if _, dup := server.serviceMap.LoadOrStore(sname, s); dup {
		return errors.New("rpc: service already defined: " + sname)
	}
	return nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method needs four ins: receiver, ctx, *args, *reply.
		if mtype.NumIn() != 4 {
			if reportErr {
				log.Printf("rpc.Register: method %q has %d input parameters; needs exactly three\n", mname, mtype.NumIn())
			}
			continue
		}
		// First arg must be context.Context
		if ctxType := mtype.In(1); ctxType != typeOfCtx {
			if reportErr {
				log.Printf("rpc.Register: return type of method %q is %q, must be error\n", mname, ctxType)
			}
			continue
		}
		// Second arg need not be a pointer.
		argType := mtype.In(2)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Printf("rpc.Register: argument type of method %q is not exported: %q\n", mname, argType)
			}
			continue
		}
		// Third arg must be a pointer.
		replyType := mtype.In(3)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Printf("rpc.Register: reply type of method %q is not a pointer: %q\n", mname, replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Printf("rpc.Register: reply type of method %q is not exported: %q\n", mname, replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Printf("rpc.Register: method %q has %d output parameters; needs exactly one\n", mname, mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Printf("rpc.Register: return type of method %q is %q, must be error\n", mname, returnType)
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
var invalidRequest = struct{}{}

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

func (m *methodType) NumCalls() (n uint) {
	m.Lock()
	n = m.numCalls
	m.Unlock()
	return n
}

func (s *service) call(server *Server, sending *sync.Mutex, pending *svc.Pending, wg *sync.WaitGroup, mtype *methodType, req *Request, argv, replyv reflect.Value, codec ServerCodec) {
	if wg != nil {
		defer wg.Done()
	}
	// _goRPC_ service calls require internal state.
	if s.name == "_goRPC_" {
		switch v := argv.Interface().(type) {
		case *svc.CancelArgs:
			v.SetPending(pending)
		}
	}
	mtype.Lock()
	mtype.numCalls++
	mtype.Unlock()
	ctx := pending.Start(req.Seq)
	defer pending.Cancel(req.Seq)
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, reflect.ValueOf(ctx), argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	server.sendResponse(sending, req, replyv.Interface(), codec, errmsg)
	server.freeRequest(req)
}

type gobServerCodec struct {
	rwc    io.ReadWriteCloser
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
	closed bool
}

func (c *gobServerCodec) ReadRpcHeader(h *RpcHdr) error {
	return c.dec.Decode(h)
}

func (c *gobServerCodec) ReadRequestHeader(r *Request) error {
	return c.dec.Decode(r)
}

func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
	return c.dec.Decode(body)
}

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

func (c *gobServerCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}

// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
// ServeConn uses the gob wire format (see package gob) on the
// connection. To use an alternate codec, use ServeCodec.
// See NewClient's comment for information about concurrent access.
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	buf := bufio.NewWriter(conn)
	codec := &gobServerCodec{
		rwc:    conn,
		dec:    gob.NewDecoder(conn),
		enc:    gob.NewEncoder(buf),
		encBuf: buf,
	}
	server.ServeCodec(codec)
}

func (server *Server) newServiceConn(codec ServerCodec, isServer bool) *SvcConn {
	clientId, err := uuid.NewUUID()
	if err != nil {
		return nil
	}

	return &SvcConn{
		Id:             clientId,
		IsServer:       isServer,
		Codec:          codec,
		RecvReqPending: svc.NewPending(),
		SendReqPending: make(map[uint64]*Call),
	}
}

// ServeCodec is like ServeConn but uses the specified codec to
// decode requests and encode responses.
func (server *Server) ServeCodec(codec ServerCodec) {
	svcconn := server.newServiceConn(codec, true)
	server.SvcConns.Store(svcconn.Id, svcconn)

	if server.onNewSvcConn != nil {
		server.onNewSvcConn(server, svcconn)
	}

	server.runRecvLoop(svcconn)

	// We've seen that there are no more requests.
	// Wait for responses to be sent before closing codec.
	svcconn.Wg.Wait()
	svcconn.Codec.Close()

	server.SvcConns.Delete(svcconn.Id)
}

func (server *Server) runRecvLoop(svcconn *SvcConn) {
	var lasterr error

	for {
		msgtype, keepReading, err := server.readRpcHeader(svcconn.Codec)
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
			keepReading = server.onRequest(svcconn)
		case MSG_TYPE_RESPONSE:
			//log.Printf("RPC HDR is Response")
			keepReading = server.onResponse(svcconn)
		case MSG_TYPE_NOTIFY:
			log.Printf("RPC HDR is Notify")
			continue
		}

		if !keepReading {
			break
		}
	}

	if svcconn.IsServer {
		log.Printf("Stop Server side receiving service")
	} else {
		// Terminate pending calls.
		svcconn.Sending.Lock()
		svcconn.Mutex.Lock()
		svcconn.Shutdown = true
		closing := svcconn.Closing

		if lasterr == io.EOF {
			if closing {
				lasterr = ErrShutdown
			} else {
				lasterr = io.ErrUnexpectedEOF
			}
		}

		for _, call := range svcconn.SendReqPending {
			call.Error = lasterr
			call.done()
		}

		svcconn.Mutex.Unlock()
		svcconn.Sending.Unlock()

		if debugLog && lasterr != io.EOF && !closing {
			log.Println("rpc: client protocol error:", lasterr)
		}

		log.Printf("Stop client side receiving service")
		svcconn.Wg.Done()
	}
}

func (server *Server) onRequest(svcconn *SvcConn) (keepReading bool) {
	service, mtype, req, argv, replyv, keepReading, err := server.readRequest(svcconn.Codec)
	if err != nil {
		if debugLog && err != io.EOF {
			log.Println("rpc:", err)
		}
		if !keepReading {
			return
		}
		// send a response if we actually managed to read a header.
		if req != nil {
			server.sendResponse(&svcconn.Sending, req, invalidRequest, svcconn.Codec, err.Error())
			server.freeRequest(req)
		}

		return
	}

	svcconn.Wg.Add(1)
	go service.call(server, &svcconn.Sending, svcconn.RecvReqPending, &svcconn.Wg, mtype, req, argv, replyv, svcconn.Codec)

	return
}

func (server *Server) onResponse(svcconn *SvcConn) (keepReading bool) {
	_, keepReading, err := server.readResponse(svcconn)
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

func (server *Server) readResponse(conn *SvcConn) (res *Response, keepReading bool, err error) {
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

func (server *Server) sendRequest(conn *SvcConn, call *Call) {
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

	conn.Req.Seq = seq
	conn.Req.ServiceMethod = call.ServiceMethod
	err := conn.Codec.WriteRequest(hdr, &conn.Req, call.Args)
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

func (server *Server) writeRequest(conn *SvcConn, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
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

func (server *Server) Call(ctx context.Context, conn *SvcConn, serviceMethod string, args interface{}, reply interface{}) error {
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
func (server *Server) Accept(lis net.Listener, cb OnNewSvcConn) {
	server.onNewSvcConn = cb

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Print("rpc.Serve: accept:", err.Error())
			return
		}

		go server.ServeConn(conn)
	}
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func RegisterName(name string, rcvr interface{}) error {
	return DefaultServer.RegisterName(name, rcvr)
}

// A ServerCodec implements reading of RPC requests and writing of
// RPC responses for the server side of an RPC session.
// The server calls ReadRequestHeader and ReadRequestBody in pairs
// to read requests from the connection, and it calls WriteResponse to
// write a response back. The server calls Close when finished with the
// connection. ReadRequestBody may be called with a nil
// argument to force the body of the request to be read and discarded.
// See NewClient's comment for information about concurrent access.
type ServerCodec interface {
	ReadRpcHeader(h *RpcHdr) error
	ReadRequestHeader(*Request) error
	ReadRequestBody(interface{}) error
	WriteResponse(*RpcHdr, *Response, interface{}) error

	ReadResponseHeader(r *Response) error
	ReadResponseBody(body interface{}) error
	WriteRequest(hdr *RpcHdr, r *Request, body interface{}) error

	// Close can be called multiple times and must be idempotent.
	Close() error
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

// Can connect to RPC service using HTTP CONNECT to rpcPath.
var connected = "200 Connected to Go RPC"

// ServeHTTP implements an http.Handler that answers RPC requests.
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath,
// and a debugging handler on debugPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func (server *Server) HandleHTTP(rpcPath, debugPath string) {
	http.Handle(rpcPath, server)
	http.Handle(debugPath, debugHTTP{server})
}

// HandleHTTP registers an HTTP handler for RPC messages to DefaultServer
// on DefaultRPCPath and a debugging handler on DefaultDebugPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func HandleHTTP() {
	DefaultServer.HandleHTTP(DefaultRPCPath, DefaultDebugPath)
}

func DialBiServer(network, address string) (*Server, *SvcConn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, nil, err
	}

	s := &Server{}

	encBuf := bufio.NewWriter(conn)
	codec := &gobServerCodec{
		rwc:    conn,
		dec:    gob.NewDecoder(conn),
		enc:    gob.NewEncoder(encBuf),
		encBuf: encBuf,
	}

	c := s.newServiceConn(codec, false)
	s.SvcConns.Store(c.Id, c)

	return s, c, nil
}

func (server *Server) RunBiService(conn *SvcConn) {
	// run service
	conn.Wg.Add(1)
	go server.runRecvLoop(conn)
}
