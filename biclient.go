package rpc

import (
	"bufio"
	"encoding/gob"
	//"errors"
	//"fmt"
	//"log"
	"net"
)

func DialBiClient(network, address string) (*Server, *Connection, error) {
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

	c := s.newConnection(codec, false)
	s.Conns.Store(c.Id, c)

	return s, c, nil
}

func (server *Server) RunBiClient(conn *Connection) {
	// run service
	conn.Wg.Add(1)
	go server.runRecvLoop(conn)
}
