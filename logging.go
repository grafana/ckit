package ckit

import (
	"bytes"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// memberListOutputLogger will do best-effort classification of the logging level that memberlist uses and use the
// corresponding level when logging with logger. This helps us surface only desired log messages from memberlist.
// If classification fails, debug level is used as a fallback.
type memberListOutputLogger struct {
	logger log.Logger
}

var _ io.Writer = (*memberListOutputLogger)(nil)

var (
	errPrefix  = []byte("[ERR]")
	warnPrefix = []byte("[WARN]")
	infoPrefix = []byte("[INFO]")
)

func (m *memberListOutputLogger) Write(p []byte) (int, error) {
	var err error

	if bytes.HasPrefix(p, errPrefix) {
		err = level.Error(m.logger).Log("msg", p)
	} else if bytes.HasPrefix(p, warnPrefix) {
		err = level.Warn(m.logger).Log("msg", p)
	} else if bytes.HasPrefix(p, infoPrefix) {
		err = level.Info(m.logger).Log("msg", p)
	} else {
		err = level.Debug(m.logger).Log("msg", p)
	}

	if err != nil {
		return 0, err
	}
	return len(p), nil
}
