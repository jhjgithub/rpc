package main

import (
	"log"
	"net"
	//"net/rpc"
	"github.com/keegancsmith/rpc"
)

// Creating an object that the server will register
// This type must be exported
type Greeting string

// See the "net/rpc" package to understand which criteria must be met
// so that a method can be made available for remote access
func (g *Greeting) SayHello(name string, reply *string) error {
	*reply = "Hello " + name
	return nil
}

func main() {
	greeting := new(Greeting)
	// Registering greeting so the method defined on it can be called remotely
	rpc.Register(greeting)
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Println(err)
		return
	}
	rpc.Accept(l)
}
