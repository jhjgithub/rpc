package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	//"net/rpc"
	"github.com/jiho-dev/rpc"
)

// Creating an object that the server will register
// This type must be exported
type Greeting string

var myserver *rpc.Server
var myconn *rpc.SvcConn

func Test1(cname string) {
	var reply string

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	t := r1.Intn(5)
	time.Sleep(time.Duration(t) * time.Second)

	var name string = "Server-" + cname
	var expect string = "Hello " + name

	log.Printf("Send name: %s", name)

	err := myserver.Call(context.Background(), myconn, "Greeting.SayHello", name, &reply)
	if err != nil {
		log.Printf("Write ERR: %s", err)
		return
	}

	if expect != reply {
		fmt.Printf("@@@@@@@@@@@@@@ Mismatched REPLY: %s:%s", expect, reply)
	}
}

// See the "net/rpc" package to understand which criteria must be met
// so that a method can be made available for remote access
func (g *Greeting) SayHello(ctx context.Context, name string, reply *string) error {
	log.Printf("Called SayHello in Server")
	log.Printf("Recv name: %s", name)

	go Test1(name)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	t := r1.Intn(5)
	time.Sleep(time.Duration(t) * time.Second)

	*reply = "Hello " + name
	//log.Printf("Send Res: %s", *reply)

	return nil
}

func OnAccept(s *rpc.Server, c *rpc.SvcConn) bool {

	log.Printf("Got new client: %v, %v", s, c)

	myserver = s
	myconn = c
	return true
}

func main() {
	greeting := new(Greeting)

	// Registering greeting so the method defined on it can be called remotely
	//rpc.Register(greeting)
	//rpc.RegisterName("Greeting", greeting)

	s := rpc.NewServer()
	s.RegisterName("Greeting", greeting)

	l, err := net.Listen("tcp", ":8899")
	if err != nil {
		log.Println(err)
		return
	}

	//rpc.Accept(l)
	s.Accept(l, OnAccept)
}
