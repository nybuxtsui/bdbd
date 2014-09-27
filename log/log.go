package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEBUG   = iota
	INFO    = iota
	WARN    = iota
	ERROR   = iota
	DISABLE = iota
	FATAL   = iota
)

var (
	defaultlogger   *Logger
	lastTime        uint32
	lastDate        uint32
	lastDateTimeStr string
	buffPool        = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0))
		},
	}
	loggerMap = make(map[string]*Logger)
)

func getBuff() *bytes.Buffer {
	return buffPool.Get().(*bytes.Buffer)
}

func putBuff(buf *bytes.Buffer) {
	buf.Reset()
	buffPool.Put(buf)
}

var ErrNameNotFound = errors.New("name_not_found")
var ErrIndexOutOfBound = errors.New("index_out_of_bound")

func SetLevel(name string, index int, level string) error {
	l, ok := loggerMap[name]
	if !ok {
		fmt.Printf("ERROR: log name not found: %v\n", name)
		return ErrNameNotFound
	}
	if index >= len(l.log) {
		fmt.Printf("ERROR: log index exceed: %v, %v\n", len(l.log), index)
		return ErrIndexOutOfBound
	}
	if index == -1 {
		for i := 0; i < len(l.log); i++ {
			l.log[i].SetLevel(getLevelFromStr(level))
		}
	} else {
		l.log[index].SetLevel(getLevelFromStr(level))
	}
	return nil
}

func getLevelStr(level int) byte {
	switch level {
	case DEBUG:
		return 'D'
	case INFO:
		return 'I'
	case WARN:
		return 'W'
	case ERROR:
		return 'E'
	case FATAL:
		return 'F'
	default:
		fmt.Printf("ERROR: logger level unknown: %v\n", level)
		return 'I'
	}
}

func getLevelFromStr(level string) int {
	switch strings.ToLower(level) {
	case "d":
		return DEBUG
	case "i":
		return INFO
	case "w":
		return WARN
	case "e":
		return ERROR

	case "debug":
		return DEBUG
	case "info":
		return INFO
	case "warn":
		return WARN
	case "warning":
		return WARN
	case "err":
		return ERROR
	case "error":
		return ERROR
	case "disable":
		return DISABLE
	default:
		fmt.Printf("ERROR: logger level unknown: %v\n", level)
		return INFO
	}
}

type LoggerDev interface {
	io.Writer
	Sync()
	SetLevel(level int)
	GetLevel() int
}

type LoggerDefine struct {
	Name   string `toml:"name"`
	Output string `toml:"output"`
	Level  string `toml:"level"`
}

type ConsoleLogger struct {
	writer io.Writer
	level  int
}

func (l *ConsoleLogger) Write(p []byte) (n int, err error) {
	return l.writer.Write(p)
}

func (l *ConsoleLogger) Sync() {
}

func (l *ConsoleLogger) SetLevel(level int) {
	l.level = level
}

func (l *ConsoleLogger) GetLevel() int {
	return l.level
}

type FileLogger struct {
	writer   *os.File
	prefix   string
	lock     sync.Mutex
	lastdate uint32
	level    int
}

func (l *FileLogger) SetLevel(level int) {
	l.level = level
}

func (l *FileLogger) GetLevel() int {
	return l.level
}

func (l *FileLogger) Write(p []byte) (n int, err error) {
	n = 0
	date := atomic.LoadUint32(&lastDate)
	l.lock.Lock()
	if l.lastdate != date {
		if l.writer != nil {
			err = l.writer.Close()
			if err != nil {
				fmt.Printf("ERROR: logger cannot close file: %v\n", err.Error())
			}
			l.writer = nil
		}
	}
	if l.writer == nil {
		filename := fmt.Sprintf("%s-%v.log", l.prefix, date)
		l.writer, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			l.lock.Unlock()
			fmt.Printf("ERROR: logger cannot open file: %v\n", err.Error())
			return
		}
		l.lastdate = date
	}
	n, err = l.writer.Write(p)
	l.lock.Unlock()
	if err != nil {
		fmt.Printf("ERROR: logger cannot write file: %v\n", err.Error())
	}
	return
}

func (l *FileLogger) Sync() {
	l.lock.Lock()
	if l.writer != nil {
		l.writer.Sync()
	}
	l.lock.Unlock()
}

type Logger struct {
	log []LoggerDev
}

func (l *Logger) Debug(format string, a ...interface{}) {
	l.writeLogger(DEBUG, format, a...)
}

func (l *Logger) Info(format string, a ...interface{}) {
	l.writeLogger(INFO, format, a...)
}

func (l *Logger) Warn(format string, a ...interface{}) {
	l.writeLogger(WARN, format, a...)
}

func (l *Logger) Error(format string, a ...interface{}) {
	l.writeLogger(ERROR, format, a...)
}

func (l *Logger) Fatal(format string, a ...interface{}) {
	l.writeLogger(FATAL, format, a...)
	os.Exit(1)
}

func (l *Logger) writeLogger(level int, format string, a ...interface{}) {
	ok := false
	for _, log := range l.log {
		if level >= log.GetLevel() {
			ok = true
			break
		}
	}
	if !ok {
		return
	}
	buff := getBuff()
	buff.WriteByte(getLevelStr(level))
	buff.WriteString(lastDateTimeStr)
	_, file, line, ok := runtime.Caller(2)
	if ok {
		buff.WriteByte(' ')
		var i int = len(file) - 2
		for ; i >= 0; i-- {
			if file[i] == '/' {
				i++
				break
			}
		}
		buff.WriteString(file[i:])
		buff.WriteByte(':')
		buff.WriteString(strconv.FormatInt(int64(line), 10))
	}
	buff.WriteString("] ")
	if len(a) == 0 {
		buff.WriteString(format)
	} else {
		buff.WriteString(fmt.Sprintf(format, a...))
	}
	buff.WriteByte('\n')
	for _, log := range l.log {
		if level >= log.GetLevel() {
			log.Write(buff.Bytes())
		}
	}
	putBuff(buff)
}

func updateNow() {
	t := time.Now()
	dt := uint32(t.Year()%100*10000 + int(t.Month())*100 + t.Day())
	tm := uint32(t.Hour()*10000 + t.Minute()*100 + t.Second())
	atomic.StoreUint32(&lastDate, dt)
	atomic.StoreUint32(&lastTime, tm)
	lastDateTimeStr = fmt.Sprintf("%04d %06d", dt%10000, tm)
}

func Init(defines []LoggerDefine) {
	for _, logger := range defines {
		l, ok := loggerMap[logger.Name]
		if !ok {
			l = &Logger{[]LoggerDev{}}
			loggerMap[logger.Name] = l
		}
		logger.Name = strings.ToLower(logger.Name)
		logger.Output = strings.ToLower(logger.Output)
		if logger.Output == "console" {
			l.log = append(l.log, &ConsoleLogger{
				writer: os.Stdout,
				level:  getLevelFromStr(logger.Level),
			})
		} else {
			l.log = append(l.log, &FileLogger{
				prefix: logger.Output,
				level:  getLevelFromStr(logger.Level),
			})
		}
	}
	_, ok := loggerMap["default"]
	if !ok {
		loggerMap["default"] = &Logger{
			[]LoggerDev{
				&ConsoleLogger{
					writer: os.Stdout,
					level:  getLevelFromStr("info"),
				},
			},
		}
	}

	updateNow()
	defaultlogger = loggerMap["default"]
	go func() {
		for {
			time.Sleep(time.Second)
			for _, v := range loggerMap {
				for _, l := range v.log {
					l.Sync()
				}
			}
			updateNow()
		}
	}()
}

func GetLogger(name string) *Logger {
	if logger, ok := loggerMap[name]; ok {
		return logger
	} else {
		return loggerMap["default"]
	}
}

func Debug(format string, a ...interface{}) {
	defaultlogger.writeLogger(DEBUG, format, a...)
}

func Info(format string, a ...interface{}) {
	defaultlogger.writeLogger(INFO, format, a...)
}

func Warn(format string, a ...interface{}) {
	defaultlogger.writeLogger(WARN, format, a...)
}

func Error(format string, a ...interface{}) {
	defaultlogger.writeLogger(ERROR, format, a...)
}

func Fatal(format string, a ...interface{}) {
	defaultlogger.writeLogger(FATAL, format, a...)
	os.Exit(1)
}
