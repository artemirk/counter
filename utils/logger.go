package utils

import (
    "io"
    "io/ioutil"
    "log"
    "os"
)

const (
	Debug = "debug"
	Info = "info"
	Warning = "warning"
	Error = "error"
	Nil = "nil"

)

type ILoggerHandler interface {
	SetOutput(w io.Writer)
	Output(calldepth int, s string) error
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
	Flags() int
	SetFlags(flag int)
	Prefix() string
	SetPrefix(prefix string)
}

type LoggerNil struct {
}

func NewLoggerNil() *LoggerNil {
	return &LoggerNil{}
}

// Set fake handlers that do nothing
func (l *LoggerNil) SetOutput(w io.Writer) {}
func (l *LoggerNil) Output(calldepth int, s string) error {return nil}
func (l *LoggerNil) Printf(format string, v ...interface{}) {}
func (l *LoggerNil) Print(v ...interface{}) {}
func (l *LoggerNil) Println(v ...interface{}) {}
func (l *LoggerNil) Fatal(v ...interface{}) {}
func (l *LoggerNil) Fatalf(format string, v ...interface{}) {}
func (l *LoggerNil) Fatalln(v ...interface{}) {}
func (l *LoggerNil) Panic(v ...interface{}) {}
func (l *LoggerNil) Panicf(format string, v ...interface{}) {}
func (l *LoggerNil) Panicln(v ...interface{}) {}
func (l *LoggerNil) Flags() int {return 0}
func (l *LoggerNil) SetFlags(flag int) {}
func (l *LoggerNil) Prefix() string {return ""}
func (l *LoggerNil) SetPrefix(prefix string) {}

type Logger struct {
	Debug ILoggerHandler
	Info ILoggerHandler
	Warning ILoggerHandler
	Error ILoggerHandler
	Any ILoggerHandler
}


func getLogInstance(w io.Writer, prefix string, logFmt int) ILoggerHandler {
	var logHandler ILoggerHandler
	if w == ioutil.Discard {
		logHandler = NewLoggerNil()
	} else {
		logHandler = log.New(w, prefix, logFmt)
	}
	return logHandler
}


func getLogger(
	debugHandle io.Writer,
	infoHandle io.Writer,
    warningHandle io.Writer,
    errorHandle io.Writer) *Logger {

	log_fmt := log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile

	return &Logger{
		Debug: getLogInstance(debugHandle, "DEBUG: ", log_fmt),
		Info: getLogInstance(infoHandle, "INFO: ", log_fmt),
		Warning: getLogInstance(warningHandle, "WARNING: ", log_fmt),
		Error: getLogInstance(errorHandle, "ERROR: ", log_fmt),
		Any: getLogInstance(os.Stdout, "", log_fmt),
	}
}


func GetLogger(mode string) *Logger {
	switch mode {
		case Debug:
			return getLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
		case Info:
			return getLogger(ioutil.Discard, os.Stdout, os.Stdout, os.Stderr)
		case Warning:
			return getLogger(ioutil.Discard, ioutil.Discard, os.Stdout, os.Stderr)
		case Error:
			return getLogger(ioutil.Discard, ioutil.Discard, ioutil.Discard, os.Stderr)
		case Nil:
			return getLogger(ioutil.Discard, ioutil.Discard, ioutil.Discard, ioutil.Discard)
	}
	return nil
	
}
