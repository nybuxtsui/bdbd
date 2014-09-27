package bdb

import (
	"errors"
	"github.com/nybuxtsui/bdbd/log"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

/*
#cgo CFLAGS: -I/home/xubin/local/bdb/include
#cgo LDFLAGS: -L/home/xubin/local/bdb/lib
#cgo LDFLAGS: -l:libdb.a
#include <stdlib.h>
#include <errno.h>
#include <db.h>
#include "bdb.h"
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

type DbEnv struct {
	ptr         *C.DB_ENV
	shared_data *C.SHARED_DATA
	dbmap       map[string]*Db
	lock        sync.RWMutex
	waitStop    sync.WaitGroup
	waitExit    sync.WaitGroup
	waitReady   sync.WaitGroup
	closeChan   chan CloseHandler
}

type Db struct {
	ptr *C.DB
}

type CloseHandler struct {
	tm time.Time
	db *C.DB
}

var (
	ErrNotReady = errors.New("not_ready")
	ErrNotExist = errors.New("not_exist")
	ErrDeadLock = errors.New("dead_lock")
	ErrRepDead  = errors.New("rep_dead")
	ErrLockout  = errors.New("lockout")
	ErrReadonly = errors.New("readonly")
	ErrInval    = errors.New("inval")
	ErrNotFound = errors.New("not_found")
	ErrKeyExist = errors.New("key_exist")
	ErrUnknown  = errors.New("unknown")
)

func (dbenv *DbEnv) closeWork() {
	dbenv.waitStop.Add(1)
	for c := range dbenv.closeChan {
		if C.is_finished(dbenv.shared_data) == 0 {
			d := c.tm.Sub(time.Now())
			if d > 0 {
				time.Sleep(d)
			}
		}
		C.db_close(c.db)
	}
	dbenv.waitStop.Done()
}

func ResultToError(r C.int) error {
	switch r {
	case 0:
		return nil
	case C.DB_LOCK_DEADLOCK:
		return ErrDeadLock
	case C.DB_REP_HANDLE_DEAD:
		return ErrRepDead
	case C.DB_REP_LOCKOUT:
		return ErrLockout
	case C.DB_NOTFOUND:
		return ErrNotFound
	case C.EINVAL:
		return ErrInval
	case C.DB_KEYEXIST:
		return ErrKeyExist
	case C.EACCES:
		return ErrReadonly
	default:
		return ErrUnknown
	}
}

func SplitKey(_key []byte) (string, []byte) {
	var table string = ""
	var name []byte = nil
	for i := 0; i < len(_key); i++ {
		if _key[i] == ':' {
			table = string(_key[:i])
			name = _key[i+1:]
			break
		}
	}
	if table == "" && name == nil {
		table = "__"
		name = _key
	}
	if len(table) == 0 {
		table = "__"
	}
	if len(name) == 0 {
		name = []byte{0}
	}
	return table, name
}

func (env *DbEnv) SetValue(_key []byte, value []byte) error {
	table, key := SplitKey(_key)
	db, err := env.GetDb(table)
	if err != nil {
		log.Error("GetValue|GetDb|%s", err.Error())
		return err
	}
	ret := C.db_put(
		db.ptr,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])),
		C.uint(len(value)),
	)
	err = ResultToError(ret)
	if err == ErrRepDead {
		env.Close(table)
	}
	return err
}

func (env *DbEnv) GetValue(_key []byte) ([]byte, error) {
	table, key := SplitKey(_key)
	db, err := env.GetDb(table)
	if err != nil {
		log.Error("GetValue|GetDb|%s", err.Error())
		return nil, err
	}
	var data *C.char
	var datalen C.uint

	ret := C.db_get(
		db.ptr,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		&data,
		&datalen,
	)
	if ret == 0 {
		if data == nil {
			return nil, nil
		} else {
			r := C.GoBytes(unsafe.Pointer(data), C.int(datalen))
			C.free(unsafe.Pointer(data))
			return r, nil
		}
	}
	err = ResultToError(ret)
	if err == ErrRepDead {
		env.Close(table)
	}
	return nil, err
}

func (dbenv *DbEnv) Close(name string) {
	dbenv.lock.Lock()
	defer dbenv.lock.Unlock()
	if db, ok := dbenv.dbmap[name]; ok {
		delete(dbenv.dbmap, name)
		dbenv.closeChan <- CloseHandler{
			db: db.ptr,
			tm: time.Now().Add(5 * time.Second),
		}
	}
}

func (dbenv *DbEnv) GetDb(name string) (*Db, error) {
	dbenv.lock.RLock()
	db, ok := dbenv.dbmap[name]
	dbenv.lock.RUnlock()
	if ok {
		return db, nil
	}
	dbenv.lock.Lock()
	defer dbenv.lock.Unlock()
	db, ok = dbenv.dbmap[name]
	if ok {
		return db, nil
	}

	var dbp *C.DB
	cname := []byte(name)
	r := C.env_get(dbenv.ptr, dbenv.shared_data, (*C.char)(unsafe.Pointer(&cname[0])), &dbp)
	switch r {
	case 0:
		db := &Db{dbp}
		dbenv.dbmap[name] = db
		return db, nil
	case -1:
		return db, ErrNotExist
	case -2:
		return nil, ErrDeadLock
	default:
		return nil, ErrUnknown
	}
}

func Start(config BdbConfig) *DbEnv {

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

	dbenv := new(DbEnv)
	dbenv.dbmap = make(map[string]*Db)
	dbenv.waitStop.Add(1)
	dbenv.waitExit.Add(1)
	dbenv.waitReady.Add(1)
	go func() {
		argv := make([]*C.char, len(args)+1)
		for i, arg := range args {
			argv[i+1] = C.CString(arg)
		}
		argv[0] = C.CString("bdbd")

		if config.UseRepMgr {
			log.Info("start_repmgr|%v", args)
			C.start_mgr(C.int(len(argv)), &argv[0], unsafe.Pointer(dbenv))
		} else {
			log.Info("start_base|%v", args)
			C.start_base(C.int(len(argv)), &argv[0], unsafe.Pointer(dbenv))
		}

		for _, arg := range argv {
			C.free(unsafe.Pointer(arg))
		}

		dbenv.waitExit.Done()
	}()
	dbenv.waitReady.Wait()
	return dbenv
}

func (dbenv *DbEnv) Exit() {
	close(dbenv.closeChan)
	dbenv.waitStop.Done()
	dbenv.waitExit.Wait()
}

//export Wait
func Wait(env *C.DB_ENV, shared_data *C.SHARED_DATA, ptr unsafe.Pointer) {
	dbenv := (*DbEnv)(ptr)
	dbenv.ptr = env
	dbenv.shared_data = shared_data

	dbenv.closeChan = make(chan CloseHandler, 1000)
	go dbenv.closeWork()

	dbenv.waitReady.Done()
	dbenv.waitStop.Wait()
}
