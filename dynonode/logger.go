package dynonode

import (
	"bytes"
	"fmt"
	"os"
)

var DEBUG_PREFIX = []byte("[DEBUG]")
var INFO_PREFIX = []byte("[INFO]")

type LogWriter struct {
	logLevel int // 0-debug, 1-info, 2-error
}

func NewLogWriter(logLevel int) *LogWriter {
	return &LogWriter{logLevel}
}

func (lw *LogWriter) Write(p []byte) (int, error) {
	if lw.logLevel > 0 {
		if bytes.Contains(p, DEBUG_PREFIX) {
			return 0, nil
		}
		if lw.logLevel > 1 {
			if bytes.Contains(p, INFO_PREFIX) {
				return 0, nil
			}
		}
	}
	return fmt.Fprintf(os.Stderr, string(p))
}
