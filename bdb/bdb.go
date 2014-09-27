package bdb

import (
	"github.com/nybuxtsui/bdbd/log"
	"strconv"
	"sync"
	"unsafe"
)

/*
#cgo CFLAGS: -I/home/xubin/local/bdb/include
#cgo LDFLAGS: -L/home/xubin/local/bdb/lib
#cgo LDFLAGS: -l:libdb.a
int start_base(int argc, char *argv[]);
int start_mgr(int argc, char *argv[]);
*/
import "C"

type BdbConfig struct {
	UseRepMgr       bool
	AckAll          bool
	Bulk            bool
	Master          bool
	HomeDir         string
	LocalAddr       string
	GroupCreator    bool
	Priority        int
	DisableElection bool
	RemoteAddr      []string
	RemotePeer      string
	Verbose         bool
}

var (
	waitStop  sync.WaitGroup
	waitExit  sync.WaitGroup
	waitReady sync.WaitGroup
	dbenv     unsafe.Pointer
)

func Start(config BdbConfig) {
	waitStop.Add(1)
	waitExit.Add(1)
	waitReady.Add(1)

	args := make([]string, 0, 20)
	if config.AckAll && config.UseRepMgr {
		args = append(args, "-a")
	}
	if config.Bulk {
		args = append(args, "-b")
	}
	if config.Master {
		args = append(args, "-M")
	} else if !config.UseRepMgr {
		args = append(args, "-C")
	}

	if config.HomeDir == "" {
		log.Fatal("bdb|homedir_missing")
	}
	args = append(args, "-h", config.HomeDir)
	if config.LocalAddr == "" {
		log.Fatal("bdb|localaddr_missing")
	}
	if config.UseRepMgr && config.GroupCreator {
		args = append(args, "-L", config.LocalAddr)
	} else {
		args = append(args, "-l", config.LocalAddr)
	}

	if config.DisableElection {
		args = append(args, "-p", "0")
	} else if config.Priority != 0 {
		args = append(args, "-p", strconv.FormatInt(int64(config.Priority), 10))
	}

	if config.UseRepMgr && config.RemotePeer != "" {
		if len(config.RemoteAddr) != 0 {
			log.Fatal("bdb|conflict_RemoteAddr_RemotePeer")
		}
		args = append(args, "-R", config.RemotePeer)
	} else {
		for _, remote := range config.RemoteAddr {
			args = append(args, "-r", remote)
		}
	}

	if config.Verbose {
		args = append(args, "-v")
	}

	go func() {
		argv := make([]*C.char, len(args)+1)
		for i, arg := range args {
			argv[i+1] = C.CString(arg)
		}
		argv[0] = C.CString("bdbd")

		if config.UseRepMgr {
			log.Info("start_repmgr|%v", args)
			C.start_mgr(C.int(len(argv)), &argv[0])
		} else {
			log.Info("start_base|%v", args)
			C.start_base(C.int(len(argv)), &argv[0])
		}

		waitExit.Done()
	}()
	waitReady.Wait()
}

func Exit() {
	waitStop.Done()
	waitExit.Wait()
}

//export Wait
func Wait(_dbenv unsafe.Pointer) {
	dbenv = _dbenv
	waitReady.Done()
	waitStop.Wait()
}
