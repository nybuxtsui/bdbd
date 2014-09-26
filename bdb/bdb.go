package bdb

import (
	"sync"
	"unsafe"
)

/*
#cgo CFLAGS: -I/home/xubin/local/bdb/include
#cgo LDFLAGS: -L/home/xubin/local/bdb/lib
#cgo LDFLAGS: -l:libdb.a
int start(int argc, char *argv[]);
*/
import "C"

var wait1 sync.WaitGroup
var wait2 sync.WaitGroup
var dbenv unsafe.Pointer

func Start(args []string) {
	wait1.Add(1)
	wait2.Add(1)
	go func() {
		argv := make([]*C.char, len(args))
		for i, arg := range args {
			argv[i] = C.CString(arg)
		}
		argv[0] = C.CString("bdb")

		C.start(C.int(len(argv)), &argv[0])

		wait2.Done()
	}()
}

func Exit() {
	wait1.Done()
	wait2.Wait()
}

//export Wait
func Wait(dbenv unsafe.Pointer) {
	wait1.Wait()
}
