package main

import (
	"context"
	"fmt"
	"github.com/jiho-dev/rpc"
	"log"
)

var name string = "Omar"

func main() {
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
