// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

// mindset: keep launching thread(goroutine) for different sockets (listening socket, client socket, and etc.)
// synchronize through communication on channel

const (
	connhost = "localhost"
	conntype = "tcp"
)

type message struct {
	key   []byte
	value []byte
}

type keyValueServer struct {
	// TODO: implement this!
	clientCount int
	listener    net.Listener
	connections map[net.Conn]bool
	// aggregator  chan int
	// updateSelectCh chan int
	kvstoreCh chan message
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	kvserver := keyValueServer{0, nil, make(map[net.Conn]bool) /*, make(chan int, 1) make(chan int, 10000),*/, make(chan message)}
	init_db()

	return &kvserver
}

//
func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	listener, _ := net.Listen(conntype, connhost+":"+strconv.Itoa(port))
	// fmt.Println(connhost + ":" + strconv.Itoa(port))
	for listener == nil {
		listener, _ = net.Listen(conntype, connhost+":"+strconv.Itoa(port))
	}
	//  {
	// 	// panic("couldn't start listening: " + err.Error())
	// 	continue
	// }
	// defer listener.Close()
	// fmt.Println("started listening")
	// conns := kvs.clientConns(listener)
	go kvs.clientConns(listener)
	// go kvs.selectConns()
	go kvs.gokvstore()
	// for {
	// 	// start a goroutine for handling this client
	// 	go kvs.handleConn(<-conns)
	// }

	// fmt.Println("Start out of for loop")
	return nil
}

// use a unique goroutine to represent the mutual exclusion entity
func (kvs *keyValueServer) gokvstore() {
	for {
		m := <-kvs.kvstoreCh
		if m.value == nil {
			// get
			value := get(string(m.key))
			// fmt.Println("the value for " + string(elements[1]) + " is " + string(value))
			// send to all connected clients
			content := bytes.Join([][]byte{m.key, value}, []byte(","))
			whole := bytes.Join([][]byte{content, []byte("\n")}, []byte(""))

			for connection := range kvs.connections {
				// can't split into two Write because goroutine yields when using such func
				connection.Write(whole)
			}
		} else {
			put(string(m.key), m.value)
		}
	}
}

// func (kvs *keyValueServer) selectConns() {
// 	for {
// 		// only allow one goroutine to be activated at a time
// 		select {
// 		case <-kvs.aggregator:
// 		case <-kvs.updateSelectCh:
// 			// clear the kvs.updateSelectCh since it is a waste of
// 			// time restarting select many times for different new clients
// 			chanlen := len(kvs.updateSelectCh)
// 			for i := 0; i < chanlen; i++ {
// 				<-kvs.updateSelectCh
// 			}
// 		}
// 	}
// }

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
		kvs.connections[client] = true
		fmt.Printf("%d: %v <-> %v\n", kvs.clientCount, client.LocalAddr(), client.RemoteAddr())
		// ch <- client
		// ch := make(chan int)
		// go func(c chan int) {
		// 	for {
		// 		// chain two different channels
		// 		val := <-c
		// 		kvs.aggregator <- val
		// 	}
		// }(ch)

		// kvs.updateSelectCh <- 0

		go kvs.handleConn(client /*, ch*/)
	}
	// }()
	// return ch
}

// func put(key string, value []byte)
// func get(key string) []byte

func (kvs *keyValueServer) handleConn(client net.Conn /*, ch chan int*/) {
	b := bufio.NewReader(client)
	// TODO when client close connection
	for {
		// ReadBytes include \n
		line, err := b.ReadBytes('\n')
		// the value doesn't matter
		// By default, sends and receives block
		// goroutines are scheduled cooperatively
		// so we don't need to worry about multiple enter critical area at the same time
		// ch <- 0
		if err != nil { // EOF, or worse
			fmt.Println("kvs.connections")
			delete(kvs.connections, client)
			kvs.clientCount--
			break
		}
		// fmt.Println("recv line " + string(line[:]))
		// if string(line[:3]) == "put" {

		// } else {
		// 	fmt.Println(string(line[:]) + "is get request")
		// }
		elements := bytes.Split(line[:(len(line)-1)], []byte(","))
		if len(elements) == 2 {
			// it is get command
			// value := get(string(elements[1]))
			// // fmt.Println("the value for " + string(elements[1]) + " is " + string(value))
			// // send to all connected clients
			// content := bytes.Join([][]byte{elements[1], value}, []byte(","))
			// whole := bytes.Join([][]byte{content, []byte("\n")}, []byte(""))

			// for connection := range kvs.connections {
			// 	// can't split into two Write because goroutine yields when using such func
			// 	connection.Write(whole)
			// }
			m := message{elements[1], nil}
			kvs.kvstoreCh <- m
		} else {
			// it is put command
			// fmt.Println("kvstore")
			// put(string(elements[1]), elements[2])
			m := message{elements[1], elements[2]}
			kvs.kvstoreCh <- m
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
