package server

import (
	"bufio"
	"github.com/nybuxtsui/bdbd/bdb"
	"github.com/nybuxtsui/bdbd/log"
)

var commandMap = map[string]func(*bufio.Writer, *bdb.DbEnv, [][]byte) error{
	"get": cmdGet,
	"set": cmdSet,
}

func writeLen(w *bufio.Writer, prefix byte, n int) error {
	var buff [64]byte
	buff[len(buff)-1] = '\n'
	buff[len(buff)-2] = '\r'
	i := len(buff) - 3
	for {
		buff[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	buff[i] = prefix
	_, err := w.Write(buff[i:])
	return err
}

func cmdGet(w *bufio.Writer, dbenv *bdb.DbEnv, args [][]byte) error {
	if len(args) != 1 {
		w.WriteString("-ERR wrong number of arguments for 'get' command")
		return nil
	}
	if len(args[0]) == 0 {
		w.WriteString("-ERR wrong length of key for 'get' command")
		return nil
	}
	value, err := dbenv.GetValue(args[0])
	if err != nil {
		if err == bdb.ErrNotFound {
			w.WriteString("$-1\r\n")
			return nil
		}
		log.Error("cmdGet|GetValue|%s", err.Error())
		w.WriteString("-ERR dberr\r\n")
		return nil
	}
	writeLen(w, '$', len(value))
	w.Write(value)
	w.WriteString("\r\n")
	return nil
}

func cmdSet(w *bufio.Writer, dbenv *bdb.DbEnv, args [][]byte) error {
	if len(args) != 2 {
		_, err := w.WriteString("-ERR wrong number of arguments for 'get' command")
		return err
	}
	if len(args[0]) == 0 {
		_, err := w.WriteString("-ERR wrong length of key for 'get' command")
		return err
	}
	dbenv.SetValue(args[0], args[1])
	_, err := w.WriteString("+OK\r\n")
	return err
}
