package server

import (
	"github.com/nybuxtsui/bdbd/bdb"
	"github.com/nybuxtsui/bdbd/log"
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
	table string
	key   []byte
	resp  chan bdbGetResp
}

type bdbGetResp struct {
	value []byte
	err   error
}

type bdbSetReq struct {
	table string
	key   []byte
	value []byte
	resp  chan bdbSetResp
}

type bdbSetResp struct {
	err error
}

var cmdMap = map[string]cmdDef{
	"get": cmdDef{cmdGet, 1, 1},
	"set": cmdDef{cmdSet, 2, 2},
}

var workWait sync.WaitGroup
var workChan = make(chan interface{}, 10000)

func Start(dbenv *bdb.DbEnv) {
	for i := 0; i < 4; i++ {
		go worker(i, dbenv)
	}
}

func Exit() {
	close(workChan)
	workWait.Wait()
}

func worker(id int, dbenv *bdb.DbEnv) {
	workWait.Add(1)
	dbmap := make(map[string]*bdb.Db)
	getdb := func(table string) (*bdb.Db, error) {
		db := dbmap[table]
		if db == nil {
			var err error
			db, err = dbenv.GetDb(table)
			if err != nil {
				log.Error("worker|GetDb|%s", err.Error())
				return nil, err
			}
			dbmap[table] = db
		}
		return db, nil
	}
	checkerr := func(err error, db *bdb.Db) {
		if err == bdb.ErrRepDead {
			delete(dbmap, db.Name)
			db.Close()
		}
	}
	var getbuff uintptr = 0
	defer func() {
		for _, db := range dbmap {
			db.Close()
		}
		if getbuff != 0 {
			C.free(unsafe.Pointer(getbuff))
		}
		log.Info("server|close|work|%d", id)
		workWait.Done()
	}()
	for req := range workChan {
		switch req := req.(type) {
		case bdbSetReq:
			db, err := getdb(req.table)
			if err != nil {
				req.resp <- bdbSetResp{err}
			} else {
				err := db.Set(req.key, req.value)
				if err != nil {
					checkerr(err, db)
					req.resp <- bdbSetResp{err}
				} else {
					req.resp <- bdbSetResp{nil}
				}
			}
		case bdbGetReq:
			db, err := getdb(req.table)
			if err != nil {
				req.resp <- bdbGetResp{nil, err}
			} else {
				value, err := db.Get(req.key, &getbuff)
				if err != nil {
					if err == bdb.ErrNotFound {
						req.resp <- bdbGetResp{nil, nil}
					} else {
						checkerr(err, db)
						req.resp <- bdbGetResp{nil, err}
					}
				} else {
					req.resp <- bdbGetResp{value, nil}
				}
			}
		}
	}
}

func cmdGet(conn *Conn, args [][]byte) error {
	log.Debug("cmdGet|%s", args[0])
	table, key := bdb.SplitKey(args[0])
	respChan := make(chan bdbGetResp, 1)
	workChan <- bdbGetReq{table, key, respChan}
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
	table, key := bdb.SplitKey(args[0])
	respChan := make(chan bdbSetResp, 1)
	workChan <- bdbSetReq{table, key, args[1], respChan}
	resp := <-respChan
	if resp.err != nil {
		conn.wb.WriteString("-ERR dberr\r\n")
	} else {
		conn.wb.WriteString("+OK\r\n")
	}
	return nil
}
