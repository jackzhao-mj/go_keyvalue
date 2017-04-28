// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

const (
	connhost = "localhost"
	conntype = "tcp"
)

type keyValueServer struct {
	// TODO: implement this!
	clientCount int
	listener    net.Listener
	connections map[net.Conn]bool
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	kvserver := keyValueServer{0, nil, make(map[net.Conn]bool)}
	init_db()
	return &kvserver
}

//
func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	listener, err := net.Listen(conntype, connhost+":"+strconv.Itoa(port))
	fmt.Println(connhost + ":" + strconv.Itoa(port))
	if listener == nil {
		panic("couldn't start listening: " + err.Error())
	}
	// defer listener.Close()
	// fmt.Println("started listening")
	// conns := kvs.clientConns(listener)
	go kvs.clientConns(listener)
	// for {
	// 	// start a goroutine for handling this client
	// 	go kvs.handleConn(<-conns)
	// }

	// fmt.Println("Start out of for loop")
	return nil
}

func (kvs *keyValueServer) clientConns(listener net.Listener) {
	// ch := make(chan net.Conn)
	// i := 0
	// go func() {
	for {
		// fmt.Println("before accepting")
		// client is of type net.Conn
		client, err := listener.Accept()
		if client == nil {
			fmt.Printf("couldn't accept: " + err.Error())
			continue
		}
		fmt.Println("accpted")
		// i++
		kvs.clientCount++
		// kvs.connections[client] = true
		fmt.Printf("%d: %v <-> %v\n", kvs.clientCount, client.LocalAddr(), client.RemoteAddr())
		// ch <- client
		go kvs.handleConn(client)
		// kvs.clientCount++
	}
	// }()
	// return ch
}

// func put(key string, value []byte)
// func get(key string) []byte

func (kvs *keyValueServer) handleConn(client net.Conn) {
	b := bufio.NewReader(client)
	// TODO when client close connection
	for {
		// ReadBytes include \n
		line, err := b.ReadBytes('\n')
		if err != nil { // EOF, or worse
			break
		}
		fmt.Println("recv line " + string(line[:]))
		// if string(line[:3]) == "put" {

		// } else {
		// 	fmt.Println(string(line[:]) + "is get request")
		// }
		elements := bytes.Split(line[:(len(line)-1)], []byte(","))
		if len(elements) == 2 {
			// it is get command
			value := get(string(elements[1]))
			fmt.Println("the value for " + string(elements[1]) + " is " + string(value))
			// TODO send to all connected clients
			client.Write(bytes.Join([][]byte{elements[1], value}, []byte(",")))
			client.Write([]byte("\n"))
		} else {
			// it is put command
			put(string(elements[1]), elements[2])
		}
	}
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return kvs.clientCount
}

// TODO: add additional methods/functions below!
