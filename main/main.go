package main

import (
	"github.com/nybuxtsui/bdbd"
	"github.com/nybuxtsui/bdbd/bdb"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

//var redisAddr = flag.String("l", ":2323", "redis listen port")

func main() {
	log.Println("start")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// master
	//go run main.go -h db1 -M -l 127.0.0.1:2345 -r 127.0.0.1:2346
	// slave
	//go run main.go -h db2 -C -l 127.0.0.1:2346 -r 127.0.0.1:2345
	bdb.Start(os.Args)

	go func() {
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
	}()

	<-signalChan
	bdb.Exit()
	log.Println("bye")
}
