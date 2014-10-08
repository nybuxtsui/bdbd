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
	key   []byte
	value []byte
	resp  chan bdbSetResp
}

type bdbSetResp struct {
	err error
}

type bdbSetExReq struct {
	key   []byte
	value []byte
	sec   uint32
	resp  chan bdbSetExResp
}

type bdbSetExResp struct {
	err error
}

var cmdMap = map[string]cmdDef{
	"get":   cmdDef{cmdGet, 1, 1},
	"set":   cmdDef{cmdSet, 2, 2},
	"setex": cmdDef{cmdSetEx, 3, 3},
}

var workWait sync.WaitGroup
var workChan = make(chan interface{}, 10000)

func Start(dbenv *bdb.DbEnv) {
	for i := 0; i < 4; i++ {
		w := NewWorker(i, dbenv)
		go w.start()
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
		err := db.Set(nil, name, req.value)
		if err != nil {
			w.checkerr(err, db)
			req.resp <- bdbSetResp{err}
		} else {
			req.resp <- bdbSetResp{nil}
		}
	}
}

func (w *Worker) bdbSetEx(req *bdbSetExReq) {
	var err error
	if w.expiredb == nil {
		w.expiredb, err = w.dbenv.GetDb("__expire", bdb.DBTYPE_BTREE)
		if err != nil {
			log.Error("worker|GetDb|%s", err.Error())
			req.resp <- bdbSetExResp{err}
			return
		}
	}
	if w.expireindex == nil {
		w.expireindex, err = w.dbenv.GetDb("__expire.index", bdb.DBTYPE_HASH)
		if err != nil {
			log.Error("worker|GetDb|%s", err.Error())
			req.resp <- bdbSetExResp{err}
			return
		}
	}

	txn, err := w.dbenv.Begin(bdb.DB_READ_UNCOMMITTED)
	if err != nil {
		req.resp <- bdbSetExResp{err}
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
		req.resp <- bdbSetExResp{err}
		return
	}

	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		w.checkerr(err, db)
		req.resp <- bdbSetExResp{err}
		return
	}
	err = db.Set(txn, name, req.value)
	if err != nil {
		w.checkerr(err, db)
		req.resp <- bdbSetExResp{err}
		return
	}

	txn.Commit()
	txn = nil
	req.resp <- bdbSetExResp{nil}
}

func (w *Worker) bdbGet(req *bdbGetReq) {
	table, name := bdb.SplitKey(req.key)
	db, err := w.getdb(table, bdb.DBTYPE_HASH)
	if err != nil {
		req.resp <- bdbGetResp{nil, err}
	} else {
		value, err := db.Get(nil, name, &w.getbuff)
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
			w.bdbSet(&req)
		case bdbGetReq:
			w.bdbGet(&req)
		case bdbSetExReq:
			w.bdbSetEx(&req)
		}
	}
}

func cmdGet(conn *Conn, args [][]byte) error {
	log.Debug("cmdGet|%s", args[0])
	respChan := make(chan bdbGetResp, 1)
	workChan <- bdbGetReq{args[0], respChan}
	resp := <-respChan
	if resp.err != nil {
		conn.wb.WriteString("-ERR dberr\r\n")
	} else if resp.value == nil {
		conn.wb.WriteString("$-1\r\n")
	} else {
		conn.writeLen('$', len(resp.value))
		conn.wb.Write(resp.value)
		conn.wb.WriteString("\r\n")
	}
	return nil
}

func cmdSet(conn *Conn, args [][]byte) error {
	log.Debug("cmdSet|%s|%v", args[0], args[1])
	respChan := make(chan bdbSetResp, 1)
	workChan <- bdbSetReq{args[0], args[1], respChan}
	resp := <-respChan
	if resp.err != nil {
		conn.wb.WriteString("-ERR dberr\r\n")
	} else {
		conn.wb.WriteString("+OK\r\n")
	}
	return nil
}

func cmdSetEx(conn *Conn, args [][]byte) error {
	log.Debug("cmdSetEx|%s|%s|%s", args[0], args[1], args[2])
	respChan := make(chan bdbSetExResp, 1)
	sec, err := strconv.ParseUint(string(args[1]), 10, 32)
	if err != nil {
		conn.wb.WriteString("-ERR argument err")
		return nil
	}
	workChan <- bdbSetExReq{args[0], args[2], uint32(sec), respChan}
	resp := <-respChan
	if resp.err != nil {
		conn.wb.WriteString("-ERR dberr\r\n")
	} else {
		conn.wb.WriteString("+OK\r\n")
	}
	return nil
}
