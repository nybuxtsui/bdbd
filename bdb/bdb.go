package bdb

import (
	"errors"
	"github.com/nybuxtsui/bdbd/log"
	"os"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

/*
#cgo CFLAGS: -O2
#cgo LDFLAGS: -l:libdb.a
#include <stdlib.h>
#include <errno.h>
#include "bdb.h"
*/
import "C"

const (
	DBTYPE_BTREE   = 1
	DBTYPE_HASH    = 2
	DBTYPE_HEAP    = 6
	DBTYPE_RECNO   = 3
	DBTYPE_QUEUE   = 4
	DBTYPE_UNKNOWN = 5
)

type BdbConfig struct {
	RepMgr          bool
	AckAll          bool
	Bulk            bool
	Master          bool
	HomeDir         string
	LocalAddr       string
	Priority        int
	DisableElection bool
	RemoteAddr      []string
	RemotePeer      string
	Verbose         bool
}

type DbEnv struct {
	env         *C.DB_ENV
	shared_data *C.SHARED_DATA
	waitStop    sync.WaitGroup
	waitExit    sync.WaitGroup
	waitReady   sync.WaitGroup
}

type Db struct {
	Name string
	db   *C.DB
}

var (
	ErrNotReady = errors.New("not_ready")
	ErrNotExist = errors.New("not_exist")
	ErrDeadLock = errors.New("dead_lock")
	ErrRepDead  = errors.New("rep_dead")
	ErrLockout  = errors.New("lockout")
	ErrAccess   = errors.New("access")
	ErrInval    = errors.New("inval")
	ErrNotFound = errors.New("not_found")
	ErrKeyExist = errors.New("key_exist")
	ErrUnknown  = errors.New("unknown")
)

func Start(config BdbConfig) *DbEnv {
	args := make([]string, 0, 20)
	if config.AckAll && config.RepMgr {
		args = append(args, "-a")
	}
	if config.Bulk {
		args = append(args, "-b")
	}
	if config.Master {
		args = append(args, "-M")
	} else if !config.RepMgr {
		args = append(args, "-C")
	}

	if config.HomeDir == "" {
		log.Fatal("bdb|homedir_missing")
	}
	args = append(args, "-h", config.HomeDir)
	_, err := os.Stat(config.HomeDir)
	if err != nil && os.IsNotExist(err) {
		os.Mkdir(config.HomeDir, os.ModePerm)
	}
	if config.LocalAddr == "" {
		log.Fatal("bdb|localaddr_missing")
	}
	if config.RepMgr && config.Master {
		args = append(args, "-L", config.LocalAddr)
	} else {
		args = append(args, "-l", config.LocalAddr)
	}

	if config.DisableElection {
		args = append(args, "-p", "0")
	} else if config.Priority != 0 {
		args = append(args, "-p", strconv.FormatInt(int64(config.Priority), 10))
	}

	if config.RepMgr && config.RemotePeer != "" {
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
	dbenv.waitStop.Add(1)
	dbenv.waitExit.Add(1)
	dbenv.waitReady.Add(1)
	go func() {
		argv := make([]*C.char, len(args)+1)
		for i, arg := range args {
			argv[i+1] = C.CString(arg)
		}
		argv[0] = C.CString("bdbd")

		if config.RepMgr {
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

//export Wait
func Wait(env *C.DB_ENV, shared_data *C.SHARED_DATA, ptr unsafe.Pointer) {
	dbenv := (*DbEnv)(ptr)
	dbenv.env = env
	dbenv.shared_data = shared_data

	dbenv.waitReady.Done()
	dbenv.waitStop.Wait()
}

//export Info
func Info(msg *C.char) {
	log.Info(C.GoString(msg))
}

//export Error
func Error(msg *C.char, arg *C.char) {
	if arg != nil {
		log.Error(C.GoString(msg))
	} else {
		log.Error(C.GoString(msg), C.GoString(arg))
	}
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
	case C.ENOENT:
		return ErrNotExist
	case C.EINVAL:
		return ErrInval
	case C.DB_KEYEXIST:
		return ErrKeyExist
	case C.EACCES:
		return ErrAccess
	default:
		return ErrUnknown
	}
}

func SplitKey(_key []byte) (string, []byte) {
	var _table *C.char
	var _name *C.char
	var namelen C.int
	C.split_key(
		(*C.char)(unsafe.Pointer(&_key[0])),
		C.int(len(_key)),
		&_table,
		&_name,
		&namelen,
	)
	table := C.GoString(_table)
	name := C.GoBytes(unsafe.Pointer(_name), namelen)

	C.free(unsafe.Pointer(_table))
	C.free(unsafe.Pointer(_name))

	return table, name
}

func (dbenv *DbEnv) GetDb(name string, dbtype int) (*Db, error) {
	var dbp *C.DB
	cname := []byte(name + ".db")
	ret := C.get_db(
		dbenv.env,
		dbenv.shared_data,
		(*C.char)(unsafe.Pointer(&cname[0])),
		C.DBTYPE(dbtype),
		0,
		&dbp,
	)
	err := ResultToError(ret)
	if err == nil {
		return &Db{db: dbp, Name: name}, nil
	} else {
		return nil, err
	}
}

func (dbenv *DbEnv) Exit() {
	dbenv.waitStop.Done()
	dbenv.waitExit.Wait()
}

func (db *Db) Set(key []byte, value []byte) error {
	ret := C.db_put(
		db.db,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])),
		C.uint(len(value)),
	)
	return ResultToError(ret)
}

func (db *Db) Get(key []byte, getbuff *uintptr) ([]byte, error) {
	var data *C.char = (*C.char)(unsafe.Pointer(*getbuff))
	var datalen C.uint

	ret := C.db_get(
		db.db,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		&data,
		&datalen,
	)
	err := ResultToError(ret)
	if err == nil {
		*getbuff = uintptr(unsafe.Pointer(data))
		if data == nil {
			return nil, nil
		} else {
			r := C.GoBytes(unsafe.Pointer(data), C.int(datalen))
			//C.free(unsafe.Pointer(data))
			return r, nil
		}
	}
	return nil, err
}

func (db *Db) Close() {
	ret := C.db_close(db.db)
	if ret == 0 {
		return
	} else if ret == C.EINVAL {
		log.Error("Close|db_close|EINVAL")
		return
	}
	go func() {
		for {
			ret := C.db_close(db.db)
			if ret == 0 {
				break
			} else if ret == C.EINVAL {
				log.Error("Close|db_close|EINVAL")
				break
			} else if ret == C.DB_LOCK_DEADLOCK {
				log.Error("Close|db_close|DB_LOCK_DEADLOCK")
			} else if ret == C.DB_LOCK_NOTGRANTED {
				log.Error("Close|db_close|DB_LOCK_NOTGRANTED")
			} else {
				log.Error("Close|db_close|unknown|%v", ret)
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
}

func (dbenv *DbEnv) CheckExpire() error {
	var dbp *C.DB
	cname := []byte("__expire.db")
	ret := C.get_db(
		dbenv.env,
		dbenv.shared_data,
		(*C.char)(unsafe.Pointer(&cname[0])),
		C.DBTYPE(DBTYPE_BTREE),
		0,
		&dbp,
	)
	err := ResultToError(ret)
	if err != nil {
		return err
	}
	/*
			db := &Db{db: dbp, Name: "__expire"}
		    for {
		    }
	*/
	return nil
}
