package main

import (
	"github.com/nybuxtsui/bdbd"
	"log"
	"net"
	"os"
)

/*
#cgo CFLAGS: -I/home/xubin/local/bdb/include
#cgo LDFLAGS: -L/home/xubin/local/bdb/lib
#cgo LDFLAGS: -l:libdb.a
#include "rep_common.c"
#include "rep_mgr.c"
*/
import "C"

func main() {
	log.Println("start")
	// master
	//go run main.go -h db1 -l 127.0.0.1:2345 -M
	// slave
	//go run main.go -h db2 -l 127.0.0.1:2346 -R 127.0.0.1:2345
	argv := make([]*C.char, len(os.Args))
	for i, arg := range os.Args {
		argv[i] = C.CString(arg)
	}
	argv[0] = C.CString("bdb")

	//C.main1(C.int(len(argv)), &argv[0])

	addr, err := net.ResolveTCPAddr("tcp", ":2323")
	if err != nil {
		log.Panicln(err)
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}
	for {
		client, err := server.AcceptTCP()
		if err != nil {
			log.Println("AcceptTCP failed:", err)
			continue
		}
		conn := bdbd.NewConn(client)
		go conn.Start()
	}
}
