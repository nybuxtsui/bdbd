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
#cgo CXXFLAGS: -O2 -std=c++11
#cgo LDFLAGS: -l:libdb.a
#include <stdlib.h>
#include <errno.h>
#include "bdb.h"
*/
import "C"

const (
	DBTYPE_BTREE   = C.DB_BTREE
	DBTYPE_HASH    = C.DB_HASH
	DBTYPE_HEAP    = C.DB_HEAP
	DBTYPE_RECNO   = C.DB_RECNO
	DBTYPE_QUEUE   = C.DB_QUEUE
	DBTYPE_UNKNOWN = C.DB_UNKNOWN

	DB_READ_COMMITTED   = C.DB_READ_COMMITTED
	DB_READ_UNCOMMITTED = C.DB_READ_UNCOMMITTED
	DB_TXN_SYNC         = C.DB_TXN_SYNC

	DB_NOOVERWRITE = C.DB_NOOVERWRITE
	DB_RMW         = C.DB_RMW
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
	Flush           int
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

type Txn struct {
	txn *C.DB_TXN
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
			C.start_mgr(C.int(len(argv)), &argv[0], C.int(config.Flush), unsafe.Pointer(dbenv))
		} else {
			log.Info("start_base|%v", args)
			C.start_base(C.int(len(argv)), &argv[0], C.int(config.Flush), unsafe.Pointer(dbenv))
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
func Error(msg *C.char) {
	log.Error(C.GoString(msg))
}

//export Debug
func Debug(msg *C.char) {
	log.Debug(C.GoString(msg))
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
	var tablelen C.int
	var _name *C.char
	var namelen C.int
	C.split_key(
		(*C.char)(unsafe.Pointer(&_key[0])),
		C.int(len(_key)),
		&_table,
		&tablelen,
		&_name,
		&namelen,
	)
	table := C.GoStringN(_table, tablelen)
	name := C.GoBytes(unsafe.Pointer(_name), namelen)

	return table, name
}

func (dbenv *DbEnv) GetDb(name string, dbtype int) (*Db, error) {
	var dbp *C.DB
	cname := []byte(name + ".db")
	ret := C.get_db(
		dbenv.env,
		dbenv.shared_data,
		(*C.char)(unsafe.Pointer(&cname[0])),
		C.int(dbtype),
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

func (dbenv *DbEnv) Begin(flags uint32) (*Txn, error) {
	txn := new(Txn)
	ret := C.txn_begin(dbenv.env, &txn.txn, C.uint(flags))
	if err := ResultToError(ret); err != nil {
		return nil, err
	} else {
		return txn, nil
	}
}

func (txn *Txn) Commit() error {
	ret := C.txn_commit(txn.txn)
	return ResultToError(ret)
}

func (txn *Txn) Abort() error {
	ret := C.txn_abort(txn.txn)
	return ResultToError(ret)
}

func SetExpire(expiredb *Db, indexdb *Db, txn *Txn, key []byte, sec uint32, seq uint32, tid uint32) error {
	ret := C.db_set_expire(
		expiredb.db,
		indexdb.db,
		txn.txn,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		C.uint(sec),
		C.uint(seq),
		C.uint(tid),
	)
	return ResultToError(ret)
}

func (db *Db) Set(_txn *Txn, key []byte, value []byte, flags uint32) error {
	var txn *C.DB_TXN = nil
	if _txn != nil {
		txn = _txn.txn
	}
	ret := C.db_put(
		db.db,
		txn,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])),
		C.uint(len(value)),
		C.uint(flags),
	)
	return ResultToError(ret)
}

func (db *Db) Get(_txn *Txn, key []byte, getbuff *uintptr, flags uint32) ([]byte, error) {
	var data *C.char = (*C.char)(unsafe.Pointer(*getbuff))
	var datalen C.uint

	var txn *C.DB_TXN = nil
	if _txn != nil {
		txn = _txn.txn
	}

	ret := C.db_get(
		db.db,
		txn,
		(*C.char)(unsafe.Pointer(&key[0])),
		C.uint(len(key)),
		&data,
		&datalen,
		C.uint(flags),
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
