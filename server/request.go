package server

import (
	"bufio"
)

var commandMap = map[string]func(*bufio.Writer, [][]byte) error{
	"get": cmdGet,
}

func cmdGet(w *bufio.Writer, args [][]byte) error {
	w.WriteString("-ERR unknown command 'zzz'\r\n")
	return nil
}
