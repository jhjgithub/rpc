package main

import (
	"context"
	"fmt"
	"log"
	//"rpc"
	//"net/rpc"
	"github.com/jiho-dev/rpc"
	"strconv"
)

var name string = "Omar"

// Creating an object that the server will register
// This type must be exported
type Greeting string

// See the "net/rpc" package to understand which criteria must be met
// so that a method can be made available for remote access
func (g *Greeting) SayHello(ctx context.Context, name string, reply *string) error {
	log.Printf("Called SayHello in Client")
	log.Printf("Recv name: %s", name)

	*reply = "Hello " + name
	//log.Printf("Send Res: %s", *reply)

	return nil
}

func main1() {
	client, err := rpc.Dial("tcp", "127.0.0.1:8899")
	if err != nil {
		log.Println(err)
		return
	}

	var reply string
	err = client.Call(context.Background(), "Greeting.SayHello", name, &reply)
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("%s\n", reply)
}

func do_request(client *rpc.Server, conn *rpc.Connection, id int) {
	var reply string

	s := strconv.FormatInt(int64(id), 10)
	var myname = name + s
	var expect = "Hello " + myname

	log.Printf("Send REQ: %d: %s", id, myname)
	err := client.Call(context.Background(), conn, "Greeting.SayHello", myname, &reply)
	if err != nil {
		log.Printf("Write ERR: id=%d, %s", id, err)
		return
	}

	if expect != reply {
		fmt.Printf("@@@@@@@@@@@@ Mismatched REPLY: id=%d, %s:%s", id, expect, reply)
	}
}

func main2() {
	greeting := new(Greeting)

	client, conn, err := rpc.DialBiClient("tcp", "127.0.0.1:8899")
	if err != nil {
		log.Println(err)
		return
	}

	client.RegisterName("Greeting", greeting)
	client.RunBiClient(conn)

	/*
		var reply string

		err = client.Call(context.Background(), conn, "Greeting.SayHello", name, &reply)
		if err != nil {
			log.Printf("Write ERR: %s", err)
			return
		}

		fmt.Printf("REPLY: %s\n", reply)
	*/

	for i := 1; i <= 20; i++ {
		go do_request(client, conn, i)
	}

	conn.Wg.Wait()
	conn.Codec.Close()
	client.Conns.Delete(conn.Id)
}

func main() {
	main2()
}
