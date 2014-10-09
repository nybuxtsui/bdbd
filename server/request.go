package server

import (
	"github.com/nybuxtsui/bdbd/bdb"
	"github.com/nybuxtsui/bdbd/log"
	"strconv"
	"sync"
	"unsafe"
)

//#include <stdlib.h>
import "C"

type cmdDef struct {
	fun     func(*Conn, [][]byte) error
	minArgs int
	maxArgs int
}

type bdbGetReq struct {
	key  []byte
	resp chan bdbGetResp
}

type bdbGetResp struct {
	value []byte
	err   error
}

type bdbSetReq struct {
	key         []byte
	value       []byte
	sec         uint32
	nooverwrite bool
	resp        chan bdbSetResp
}

type bdbSetResp struct {
	err error
}

type bdbIncrByReq struct {
	key  []byte
	inc  int64
	resp chan bdbIncrByResp
}

type bdbIncrByResp struct {
	result int64
	err    error
}

var cmdMap = map[string]cmdDef{
	"get":    cmdDef{cmdGet, 1, 1},
	"set":    cmdDef{cmdSet, 2, 2},
	"setex":  cmdDef{cmdSetEx, 3, 3},
	"setnx":  cmdDef{cmdSetNx, 2, 2},
	"incrby": cmdDef{cmdIncrBy, 2, 2},
	"incr":   cmdDef{cmdIncr, 1, 1},
}

var workWait sync.WaitGroup
var workChan = make(chan interface{}, 10000)

func Start(dbenv *bdb.DbEnv) {
	for i := 0; i < 4; i++ {
		w := NewWorker(i, dbenv)
		go w.start()
	}
}

func (w *Worker) start() {
	workWait.Add(1)
	defer func() {
		for _, db := range w.dbmap {
			db.Close()
		}
		if w.getbuff != 0 {
			C.free(unsafe.Pointer(w.getbuff))
		}
		log.Info("server|close|work|%d", w.id)
		workWait.Done()
	}()
	for req := range workChan {
		switch req := req.(type) {
		case bdbSetReq:
			if req.sec == 0 {
				w.bdbSet(&req)
			} else {
				w.bdbSetEx(&req)
			}
		case bdbGetReq:
			w.bdbGet(&req)
		case bdbIncrByReq:
			w.bdbIncrBy(&req)
		}
	}
}

func Exit() {
	close(workChan)
	workWait.Wait()
}

type Worker struct {
	dbmap       map[string]*bdb.Db
	expiredb    *bdb.Db
	expireindex *bdb.Db
	dbenv       *bdb.DbEnv
	id          uint32
	seq         uint32
	getbuff     uintptr
}

func NewWorker(id int, dbenv *bdb.DbEnv) *Worker {
	return &Worker{
		dbmap:   make(map[string]*bdb.Db),
		dbenv:   dbenv,
		id:      uint32(id),
		getbuff: 0,
	}
}

func (w *Worker) getdb(table string, dbtype int) (*bdb.Db, error) {
	db := w.dbmap[table]
	if db == nil {
		var err error
		db, err = w.dbenv.GetDb(table, dbtype)
		if err != nil {
			log.Error("worker|GetDb|%s", err.Error())
			return nil, err
		}
		w.dbmap[table] = db
	}
	return db, nil
}

func (w *Worker) checkerr(err error, db *bdb.Db) {
	if err == bdb.ErrRepDead {
		delete(w.dbmap, db.Name)
		db.Close()
	}
}

func (w *Worker) bdbSet(req *bdbSetReq) {
	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		req.resp <- bdbSetResp{err}
	} else {
		var flags uint32 = 0
		if req.nooverwrite {
			flags = flags | bdb.DB_NOOVERWRITE
		}
		err := db.Set(nil, name, req.value, flags)
		if err != nil {
			w.checkerr(err, db)
			req.resp <- bdbSetResp{err}
		} else {
			req.resp <- bdbSetResp{nil}
		}
	}
}

func (w *Worker) bdbSetEx(req *bdbSetReq) {
	var err error
	if w.expiredb == nil {
		w.expiredb, err = w.dbenv.GetDb("__expire", bdb.DBTYPE_BTREE)
		if err != nil {
			log.Error("worker|GetDb|%s", err.Error())
			req.resp <- bdbSetResp{err}
			return
		}
	}
	if w.expireindex == nil {
		w.expireindex, err = w.dbenv.GetDb("__expire.index", bdb.DBTYPE_HASH)
		if err != nil {
			log.Error("worker|GetDb|%s", err.Error())
			req.resp <- bdbSetResp{err}
			return
		}
	}

	txn, err := w.dbenv.Begin(bdb.DB_READ_UNCOMMITTED)
	if err != nil {
		req.resp <- bdbSetResp{err}
		return
	}
	defer func() {
		if txn != nil {
			txn.Abort()
		}
	}()
	w.seq++
	err = bdb.SetExpire(w.expiredb, w.expireindex, txn, req.key, req.sec, w.seq, w.id)
	if err != nil {
		if err == bdb.ErrRepDead {
			w.expiredb.Close()
			w.expiredb = nil
			w.expireindex.Close()
			w.expireindex = nil
		}
		log.Error("worker|SetExpire|%s", err.Error())
		req.resp <- bdbSetResp{err}
		return
	}

	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		w.checkerr(err, db)
		req.resp <- bdbSetResp{err}
		return
	}
	var flags uint32 = 0
	if req.nooverwrite {
		flags = flags | bdb.DB_NOOVERWRITE
	}
	err = db.Set(txn, name, req.value, flags)
	if err != nil {
		w.checkerr(err, db)
		req.resp <- bdbSetResp{err}
		return
	}

	txn.Commit()
	txn = nil
	req.resp <- bdbSetResp{nil}
}

func (w *Worker) bdbGet(req *bdbGetReq) {
	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		req.resp <- bdbGetResp{nil, err}
	} else {
		value, err := db.Get(nil, name, &w.getbuff, 0)
		if err != nil {
			if err == bdb.ErrNotFound {
				req.resp <- bdbGetResp{nil, nil}
			} else {
				w.checkerr(err, db)
				req.resp <- bdbGetResp{nil, err}
			}
		} else {
			req.resp <- bdbGetResp{value, nil}
		}
	}
}

func (w *Worker) bdbIncrBy(req *bdbIncrByReq) {
	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		req.resp <- bdbIncrByResp{0, err}
	} else {
		txn, err := w.dbenv.Begin(bdb.DB_READ_COMMITTED)
		if err != nil {
			req.resp <- bdbIncrByResp{0, err}
			return
		}
		defer func() {
			if txn != nil {
				txn.Abort()
			}
		}()

		_value, err := db.Get(txn, name, &w.getbuff, bdb.DB_RMW)
		var value int64 = 0
		if err != nil {
			if err != bdb.ErrNotFound {
				w.checkerr(err, db)
				req.resp <- bdbIncrByResp{0, err}
				return
			}
		} else {
			value, err = readNumber(_value)
			if err != nil {
				req.resp <- bdbIncrByResp{0, err}
				return
			}
		}
		value += req.inc
		var buf [64]byte
		err = db.Set(txn, name, strconv.AppendInt(buf[:0], value, 10), 0)
		if err != nil {
			req.resp <- bdbIncrByResp{0, err}
			return
		}
		err = txn.Commit()
		if err != nil {
			req.resp <- bdbIncrByResp{0, err}
			return
		} else {
			txn = nil
		}
		req.resp <- bdbIncrByResp{value, nil}
	}
}

func cmdGet(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdGet|%s", args[0])
	respChan := make(chan bdbGetResp, 1)
	workChan <- bdbGetReq{args[0], respChan}
	resp := <-respChan
	if resp.err != nil {
		_, err = conn.wb.WriteString("-ERR dberr\r\n")
	} else if resp.value == nil {
		_, err = conn.wb.WriteString("$-1\r\n")
	} else {
		conn.writeLen('$', len(resp.value))
		conn.wb.Write(resp.value)
		_, err = conn.wb.WriteString("\r\n")
	}
	return
}

func cmdSet(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdSet|%s|%v", args[0], args[1])
	respChan := make(chan bdbSetResp, 1)
	workChan <- bdbSetReq{args[0], args[1], 0, false, respChan}
	resp := <-respChan
	if resp.err != nil {
		conn.wb.WriteString("-ERR ")
		conn.wb.WriteString(resp.err.Error())
		_, err = conn.wb.WriteString("\r\n")
	} else {
		_, err = conn.wb.WriteString("+OK\r\n")
	}
	return
}

func cmdSetEx(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdSetEx|%s|%s|%s", args[0], args[1], args[2])
	if sec, err := strconv.ParseUint(string(args[1]), 10, 32); err == nil {
		respChan := make(chan bdbSetResp, 1)
		workChan <- bdbSetReq{args[0], args[2], uint32(sec), false, respChan}
		resp := <-respChan
		if resp.err != nil {
			conn.wb.WriteString("-ERR ")
			conn.wb.WriteString(resp.err.Error())
			_, err = conn.wb.WriteString("\r\n")
		} else {
			_, err = conn.wb.WriteString("+OK\r\n")
		}
	} else {
		_, err = conn.wb.WriteString("-ERR argument err")
	}
	return
}

func cmdSetNx(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdSetNx|%s|%v", args[0], args[1])
	respChan := make(chan bdbSetResp, 1)
	workChan <- bdbSetReq{args[0], args[1], 0, true, respChan}
	resp := <-respChan
	if resp.err != nil {
		if resp.err == bdb.ErrKeyExist {
			_, err = conn.wb.WriteString(":0\r\n")
		} else {
			conn.wb.WriteString("-ERR ")
			conn.wb.WriteString(resp.err.Error())
			_, err = conn.wb.WriteString("\r\n")
		}
	} else {
		_, err = conn.wb.WriteString(":1\r\n")
	}
	return
}

func cmdIncrBy(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdIncrBy|%s|%v", args[0], args[1])
	if inc, err := strconv.ParseInt(string(args[1]), 10, 64); err == nil {
		respChan := make(chan bdbIncrByResp, 1)
		workChan <- bdbIncrByReq{args[0], inc, respChan}
		resp := <-respChan
		if resp.err != nil {
			if resp.err == ErrRequest {
				_, err = conn.wb.WriteString("-ERR value is not an integer or out of range\r\n")
			} else {
				conn.wb.WriteString("-ERR ")
				conn.wb.WriteString(resp.err.Error())
				_, err = conn.wb.WriteString("\r\n")
			}
		} else {
			var buf [64]byte
			out := strconv.AppendInt(buf[:0], resp.result, 10)
			conn.wb.WriteString(":")
			conn.wb.Write(out)
			_, err = conn.wb.WriteString("\r\n")
		}
	} else {
		_, err = conn.wb.WriteString("-ERR argument err")
	}
	return
}

func cmdIncr(conn *Conn, args [][]byte) (err error) {
	log.Debug("cmdIncr|%s", args[0])
	respChan := make(chan bdbIncrByResp, 1)
	workChan <- bdbIncrByReq{args[0], 1, respChan}
	resp := <-respChan
	if resp.err != nil {
		if resp.err == ErrRequest {
			_, err = conn.wb.WriteString("-ERR value is not an integer or out of range\r\n")
		} else {
			conn.wb.WriteString("-ERR ")
			conn.wb.WriteString(resp.err.Error())
			_, err = conn.wb.WriteString("\r\n")
		}
	} else {
		_, err = conn.wb.WriteString("+OK\r\n")
	}
	return
}
