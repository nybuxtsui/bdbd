package server

import (
	"github.com/nybuxtsui/bdbd/bdb"
	"github.com/nybuxtsui/bdbd/log"
)

type cmdDef struct {
	fun     func(*Conn, [][]byte) error
	minArgs int
	maxArgs int
}

var cmdMap = map[string]cmdDef{
	"get": cmdDef{cmdGet, 1, 1},
	"set": cmdDef{cmdSet, 2, 2},
}

func cmdGet(conn *Conn, args [][]byte) error {
	if len(args[0]) == 0 {
		conn.wb.WriteString("-ERR wrong length of key for 'get' command\r\n")
		return nil
	}

	table, key := bdb.SplitKey(args[0])
	db := conn.dbmap[table]
	if db == nil {
		var err error
		db, err = conn.dbenv.GetDb(table)
		if err != nil {
			log.Error("cmdGet|GetDb|%s", err.Error())
			conn.wb.WriteString("-ERR dberr")
			return nil
		}
	}
	value, err := db.Get(key)
	if err != nil {
		if err == bdb.ErrNotFound {
			conn.wb.WriteString("$-1\r\n")
			return nil
		}
		log.Error("cmdGet|GetValue|%s", err.Error())
		conn.wb.WriteString("-ERR dberr\r\n")
		return nil
	} else {
		conn.writeLen('$', len(value))
		conn.wb.Write(value)
		_, err := conn.wb.WriteString("\r\n")
		return err
	}
}

func cmdSet(conn *Conn, args [][]byte) error {
	if len(args[0]) == 0 {
		_, err := conn.wb.WriteString("-ERR wrong length of key for 'get' command\r\n")
		return err
	}
	table, key := bdb.SplitKey(args[0])
	db := conn.dbmap[table]
	if db == nil {
		var err error
		db, err = conn.dbenv.GetDb(table)
		if err != nil {
			log.Error("cmdGet|GetDb|%s", err.Error())
			conn.wb.WriteString("-ERR dberr\r\n")
			return nil
		}
	}
	err := db.Set(key, args[1])
	if err != nil {
		log.Error("cmdGet|GetValue|%s", err.Error())
		conn.wb.WriteString("-ERR dberr\r\n")
		return nil
	} else {
		_, err := conn.wb.WriteString("+OK\r\n")
		return err
	}
}
