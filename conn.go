package main

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type conn struct {
	conn net.Conn
	lock sync.Mutex

	reader *bufio.Reader
	writer *bufio.Writer

	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) Conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriterSize(netConn, 4*1024*1024),
		br:           bufio.NewReaderSize(netConn, 4*1024*1024),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

var ErrRequest = errors.New("bdbd: bad request")

func (c *conn) readLine() ([]byte, error) {
	p, err := c.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		log.Println("request too long")
		return nil, ErrRequest
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		log.Println("bad line terminator")
		return nil, ErrRequest
	}
	return p[:i], nil
}

func (c *conn) readNumber(line []byte) (n int32, err error) {
	line, err := c.readLine()
	if len(line) == 0 {
		log.Println("bad argument count")
		return 0, ErrRequest
	}
	n = 0
	for i := 1; i < len(line); i++ {
		ch := line[i]
		if ch < '0' || ch > '9' {
			log.Println("bad argument count number")
			return 0, ErrRequest
		}
		count *= 10
		count += ch - '0'
	}
}

func (c *conn) readData(int length) ([]byte, error) {
	data, err := c.reader
	if len(data) == length+2 {
	}
}

func (c *conn) readRequest() (interface{}, error) {
	if ch, err := c.reader.ReadByte(); err != nil {
		return nil, err
	} else if ch != '*' {
		log.Println("bdbd: bad argument count tag")
		return nil, ErrRequest
	}
	count, err := c.readNumber(line)
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		if ch, err := c.reader.ReadByte(); err != nil {
			return nil, err
		} else if ch != '$' {
			log.Println("bdbd: bad argument length tag")
			return nil, ErrRequest
		}
		argumentlen, err := c.readNumber(line)
		c.reader.ReadSlice('\n')
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(c.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := c.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, errors.New("bdbd: bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, errors.New("bdbd: unexpected response line")
}
