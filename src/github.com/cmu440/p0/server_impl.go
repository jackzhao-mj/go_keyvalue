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

// I did three versions
// 1. reflect SelectCase
// 2. aggregator channel
// 3. use goroutine to represent mutual exclusion entity

const (
	connhost = "0.0.0.0"
	conntype = "tcp"
)

type kvstoreMessage struct {
	key   []byte
	value []byte
}

type connectionMessage struct {
	client net.TCPConn
	choice int
}

type clientState struct {
	connection net.TCPConn
	toBeSent   chan []byte
}

type keyValueServer struct {
	clientCount  int
	clientStates []clientState
	connectionCh chan connectionMessage
	kvstoreCh    chan kvstoreMessage
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	kvserver := keyValueServer{0, []clientState{}, make(chan connectionMessage), make(chan kvstoreMessage)}
	init_db()
	return &kvserver
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO fix listening problem
	tcpAddr, err := net.ResolveTCPAddr(conntype, connhost+":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("something is wrong with ResolveTCPAddr")
	}
	listener, err := net.ListenTCP(conntype, tcpAddr)
	for listener == nil {
		listener, err = net.ListenTCP(conntype, tcpAddr)
	}
	go kvs.clientConns(listener)
	go kvs.gokvstore()
	go kvs.goconnections()
	return nil
}

func (kvs *keyValueServer) goconnections() {
	for {
		m := <-kvs.connectionCh
		if m.choice == 0 {
			// add
			// kvs.connections[m.client] = true
			ch := make(chan []byte, 1000)
			kvs.clientStates = append(kvs.clientStates, clientState{m.client, ch})
			kvs.clientCount++
		} else {
			// delete
			// delete(kvs.connections, m.client)
			for i, cs := range kvs.clientStates {
				if cs.connection == m.client {
					kvs.clientStates = append(kvs.clientStates[:i], kvs.clientStates[i+1:]...)
					break
				}
			}
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
			for _, clientState := range kvs.clientStates {
				// delegate to a goroutine to do the writting for us
				// introduce buffer; send no more than 500 messages to socket
				clientState.connection.Write(whole)
				// write whole to channel responding to the all connections
			}
		} else {
			put(string(m.key), m.value)
		}
	}
}

func (kvs *keyValueServer) clientConns(listener *net.TCPListener) {
	defer listener.Close()
	for {
		client, err := listener.AcceptTCP()
		if client == nil {
			fmt.Printf("couldn't accept: " + err.Error())
			continue
		}
		fmt.Println("accpted")
		m := connectionMessage{*client, 0}
		kvs.connectionCh <- m
		// fmt.Printf("%d: %v <-> %v\n", kvs.clientCount, client.LocalAddr(), client.RemoteAddr())
		go kvs.handleConn(client)
	}
}

func (kvs *keyValueServer) handleConn(client *net.TCPConn) {
	defer client.Close()
	b := bufio.NewReader(client)
	for {
		// ReadBytes include '\n'
		line, err := b.ReadBytes('\n')
		if err != nil { // EOF, or worse
			m := connectionMessage{*client, 1}
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
