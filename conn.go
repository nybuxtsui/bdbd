package bdbd

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"
)

type Conn struct {
	conn net.Conn

	rb *bufio.Reader
	wb *bufio.Writer
}

var (
	ErrRequest = errors.New("invalid request")
)

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		rb:   bufio.NewReaderSize(c, 1024*1024),
		wb:   bufio.NewWriterSize(c, 16*1024),
	}
}

func (c *Conn) readLine() ([]byte, error) {
	line, err := c.rb.ReadSlice('\n')
	if err != nil {
		log.Println("ReadSlice:", err)
		return nil, err
	}
	if len(line) <= 2 {
		log.Println("empty line")
		return nil, ErrRequest
	}
	if line[len(line)-2] != '\r' {
		log.Println("line invalid")
		return nil, ErrRequest
	}
	return line[:len(line)-2], nil
}

func (c *Conn) readNumber(buff []byte) (int64, error) {
	if len(buff) == 0 {
		log.Println("number empty")
		return 0, ErrRequest
	}
	var sign int64 = 1
	var r int64 = 0
	for i, c := range buff {
		if i == 0 && c == '-' {
			sign = -1
		} else if c < '0' || c > '9' {
			log.Println("number invalid")
			return 0, ErrRequest
		} else {
			r *= 10
			r += int64(c - '0')
		}
	}
	return r * sign, nil
}

func (c *Conn) readCount(tag byte) (int64, error) {
	line, err := c.readLine()
	if err != nil {
		log.Println("ReadLine:", err)
		return 0, err
	}
	if len(line) < 2 {
		log.Println("line invalid")
		return 0, ErrRequest
	}
	if line[0] != tag {
		log.Println("tag invalid")
		return 0, ErrRequest
	}
	count, err := c.readNumber(line[1:])
	if err != nil {
		log.Println("readNumber:", err)
		return 0, ErrRequest
	}
	return count, nil
}

func (c *Conn) processRequest() error {
	req, err := c.readRequest()
	if err != nil {
		log.Println("readRequest:", err)
		return err
	}
	cmd := strings.ToLower(string(req[0]))
	if f, ok := commandMap[cmd]; ok {
		err = f(c.wb, req[1:])
		if err != nil {
			log.Println("f:", err)
			return err
		}
	} else {
		c.wb.WriteString("-ERR unknown command '" + cmd + "'\r\n")
	}
	if err = c.wb.Flush(); err != nil {
		log.Println("Flush:", err)
		return err
	} else {
		return nil
	}
}

func (c *Conn) readRequest() ([][]byte, error) {
	count, err := c.readCount('*')
	if err != nil {
		log.Println("readCount:", err)
		return nil, err
	}
	if count <= 0 {
		log.Println("count <= 0:", err)
		return nil, ErrRequest
	}
	var r = make([][]byte, count)
	var i int64
	for i = 0; i < count; i++ {
		length, err := c.readCount('$')
		if err != nil {
			log.Println("readCount:", err)
			return nil, err
		}
		buff := make([]byte, length+2)
		_, err = io.ReadFull(c.rb, buff)
		if err != nil {
			log.Println("readfull:", err)
			return nil, err
		}
		if buff[length+1] != '\n' || buff[length] != '\r' {
			log.Println("request crlf invalid")
			return nil, ErrRequest
		}
		r[i] = buff[0:length]
	}
	return r, nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Start() {
	defer func() {
		c.Close()
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	for {
		err := c.processRequest()
		if err != nil {
			log.Println("processRequest:", err)
			break
		}
	}
}
