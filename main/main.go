package main

import (
	"github.com/BurntSushi/toml"
	"github.com/nybuxtsui/bdbd/bdb"
	mylog "github.com/nybuxtsui/bdbd/log"
	"github.com/nybuxtsui/bdbd/server"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

//var redisAddr = flag.String("l", ":2323", "redis listen port")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 4)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// master
	//go run main.go -h db1 -M -l 127.0.0.1:2345 -r 127.0.0.1:2346
	// slave
	//go run main.go -h db2 -C -l 127.0.0.1:2346 -r 127.0.0.1:2345

	configFile := "bdbd.conf"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}
	configstr, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("ERROR: read config file failed:", err)
		os.Exit(1)
	}
	var config struct {
		Bdb    bdb.BdbConfig        `toml:"bdb"`
		Logger []mylog.LoggerDefine `toml:"logger"`
		Server struct {
			Listen string
		} `toml:"server"`
	}
	_, err = toml.Decode(string(configstr), &config)
	if err != nil {
		log.Println("ERROR: decode config failed:", err)
		os.Exit(1)
	}

	mylog.Init(config.Logger)

	mylog.Info("bdb|starting")
	dbenv := bdb.Start(config.Bdb)
	mylog.Info("bdb|started")

	connmap := make(map[net.Conn]struct{})
	var connlock sync.Mutex
	var listener *net.TCPListener
	var wait sync.WaitGroup

	go func() {
		addr, err := net.ResolveTCPAddr("tcp", config.Server.Listen)
		if err != nil {
			mylog.Fatal("ResolveTCPAddr|%s", err.Error())
		}
		listener, err = net.ListenTCP("tcp", addr)
		if err != nil {
			mylog.Fatal("Server|ListenTCP|%s", err.Error())
		}
		for {
			client, err := listener.AcceptTCP()
			if err != nil {
				if err.Error() == "use of closed network connection" {
					mylog.Info("Server|closing")
					break
				} else {
					mylog.Error("Server|%s", err.Error())
					continue
				}
			}
			connlock.Lock()
			connmap[client] = struct{}{}
			connlock.Unlock()
			wait.Add(1)
			client.SetKeepAlive(true)
			conn := server.NewConn(client, dbenv)
			go func() {
				conn.Start()
				connlock.Lock()
				delete(connmap, client)
				connlock.Unlock()
				wait.Done()
			}()
		}
	}()

	mylog.Info("start")
	<-signalChan

	listener.Close()
	time.Sleep(time.Millisecond * 10)
	for k, _ := range connmap {
		k.Close()
	}
	wait.Wait()

	dbenv.Exit()
	mylog.Info("bye")
}
