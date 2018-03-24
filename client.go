package msgbus

import (
	"log"
	//	"log"
	"net"
	"sync"
	"time"
)

type MsgBus interface {
	Event(msgid string, body []byte)
}

type ClusterConnect struct {
	clusterAddr []string
	curAddr     int
	timeout     time.Duration
	connect     net.Conn
}

type Client struct {
	server ClusterConnect
	lock   sync.RWMutex
	name   string
	event  map[string]MsgBus

	recv chan []byte
	send chan []byte
}

func NewClient(name string, addr []string, timeout int) *Client {
	if len(addr) == 0 {
		return nil
	}

	c := new(Client)

	c.server.clusterAddr = addr
	c.server.timeout = time.Duration(timeout) * time.Millisecond

	c.name = name
	c.event = make(map[string]MsgBus)

	c.recv = make(chan []byte, 100)
	c.send = make(chan []byte, 100)

	return c
}

func recvtask(timeout time.Duration, wait *sync.WaitGroup, conn net.Conn, que chan []byte) {
	defer conn.Close()
	defer wait.Done()

	buf := make([]byte, 1024)

	for {

		err := conn.SetReadDeadline(timeout)
		if err != nil {
			log.Println(err.Error())
			return
		}

		cnt, err := conn.Read(buf)
		if err != nil {
			log.Println(err.Error())
			return
		}

		tmp := make([]byte, cnt)
		copy(tmp, buf[:cnt])

		que <- tmp
	}
}

// 发送封装的接口
func fullywrite(timeout time.Duration, conn net.Conn, buf []byte) error {
	totallen := len(buf)
	sendcnt := 0

	for {

		err := conn.SetWriteDeadline(timeout)
		if err != nil {
			log.Println(err.Error())
			return
		}

		cnt, err := conn.Write(buf[sendcnt:])
		if err != nil {
			return err
		}
		if cnt+sendcnt >= totallen {
			return nil
		}
		sendcnt += cnt
	}
}

func sendtask(wait *sync.WaitGroup, conn net.Conn, que chan []byte) {
	defer conn.Close()
	defer wait.Done()

	for {

		buf := <-que

		err := fullywrite(conn, buf)
		if err != nil {
			log.Println(err.Error())
			return
		}
	}
}

func (c *Client) Run() error {
	var curAddr int

	for {
		addr := c.server.clusterAddr[curAddr]

		conn, err := net.Dial("tcp", addr)
		if err != nil {

			log.Println(err.Error())
			time.Sleep(1 * time.Second)
			curAddr = (curAddr + 1) % len(c.server.clusterAddr)

			continue
		}

		wait := new(sync.WaitGroup)
		wait.Add(2)

		go recvtask(wait, conn, c.recv)
		go sendtask(wait, conn, c.send)

		wait.Wait()

		time.Sleep(1 * time.Second)
		curAddr = (curAddr + 1) % len(c.server.clusterAddr)
	}

	return nil
}

func (c *Client) Sub(msgid string, msg MsgBus) error {

	return nil
}

func (c *Client) Pub(msgid string, body []byte, ttl int) error {

	return nil
}
