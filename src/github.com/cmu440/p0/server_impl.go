// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
)

// mindset:
// 1. keep launching thread(goroutine) for different sockets (listening socket, client socket, and etc.)
// 2. synchronize through communication on channel
// 3. use a unique goroutine to represent the mutual exclusion entity

const (
	connhost = "localhost"
	conntype = "tcp"
)

type kvstoreMessage struct {
	key   []byte
	value []byte
}

type connectionMessage struct {
	client net.Conn
	choice int
}

type keyValueServer struct {
	clientCount  int
	listener     net.Listener
	connections  map[net.Conn]bool
	connectionCh chan connectionMessage
	kvstoreCh    chan kvstoreMessage
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	kvserver := keyValueServer{0, nil, make(map[net.Conn]bool), make(chan connectionMessage), make(chan kvstoreMessage)}
	init_db()

	return &kvserver
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO fix listening problem
	kvs.listener, _ = net.Listen(conntype, connhost+":"+strconv.Itoa(port))
	for kvs.listener == nil {
		kvs.listener, _ = net.Listen(conntype, connhost+":"+strconv.Itoa(port))
	}
	go kvs.clientConns(kvs.listener)
	go kvs.gokvstore()
	go kvs.goconnections()
	return nil
}

func (kvs *keyValueServer) goconnections() {
	for {
		m := <-kvs.connectionCh
		if m.choice == 0 {
			// add
			kvs.connections[m.client] = true
			kvs.clientCount++
		} else {
			// delete
			delete(kvs.connections, m.client)
			kvs.clientCount--
		}

	}
}

// use a unique goroutine to represent the mutual exclusion entity
func (kvs *keyValueServer) gokvstore() {
	for {
		m := <-kvs.kvstoreCh
		if m.value == nil {
			// get
			value := get(string(m.key))
			content := bytes.Join([][]byte{m.key, value}, []byte(","))
			whole := bytes.Join([][]byte{content, []byte("\n")}, []byte(""))

			// send to all connected clients
			for connection := range kvs.connections {
				connection.Write(whole)
			}
		} else {
			put(string(m.key), m.value)
		}
	}
}

func (kvs *keyValueServer) clientConns(listener net.Listener) {
	for {
		client, err := listener.Accept()
		if client == nil {
			fmt.Printf("couldn't accept: " + err.Error())
			continue
		}
		fmt.Println("accpted")
		m := connectionMessage{client, 0}
		kvs.connectionCh <- m
		fmt.Printf("%d: %v <-> %v\n", kvs.clientCount, client.LocalAddr(), client.RemoteAddr())
		go kvs.handleConn(client)
	}
}

func (kvs *keyValueServer) handleConn(client net.Conn) {
	b := bufio.NewReader(client)
	for {
		// ReadBytes include '\n'
		line, err := b.ReadBytes('\n')
		if err != nil { // EOF, or worse
			m := connectionMessage{client, 1}
			kvs.connectionCh <- m
			client.Close()
			break
		}
		elements := bytes.Split(line[:(len(line)-1)], []byte(","))
		if len(elements) == 2 {
			// it is get command
			m := kvstoreMessage{elements[1], nil}
			kvs.kvstoreCh <- m
		} else {
			// it is put command
			m := kvstoreMessage{elements[1], elements[2]}
			kvs.kvstoreCh <- m
		}
	}
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	// kvs.listener.Close()
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	return kvs.clientCount
}

// TODO: add additional methods/functions below!
