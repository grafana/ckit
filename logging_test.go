package ckit

import (
	"fmt"
	golog "log"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogging(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "debug level",
			message:  "[DEBUG] memberlist: Stream connection from=127.0.0.1:56631\n",
			expected: `level="debug", msg="Stream connection from=127.0.0.1:56631"`,
		},
		{
			name:     "info level",
			message:  "[INFO] memberlist: Suspect node-a has failed, no acks received\n",
			expected: `level="info", msg="Suspect node-a has failed, no acks received"`,
		},
		{
			name:     "warn level",
			message:  "[WARN] memberlist: Refuting a dead message (from: node-b)\n",
			expected: `level="warn", msg="Refuting a dead message (from: node-b)"`,
		},
		{
			name:     "error level",
			message:  "[ERR] memberlist: Failed fallback TCP ping: io: read/write on closed pipe\n",
			expected: `level="error", msg="Failed fallback TCP ping: io: read/write on closed pipe"`,
		},
		{
			name:     "error without memberlist",
			message:  "[ERR] Failed to shutdown transport: test\n",
			expected: `level="error", msg="Failed to shutdown transport: test"`,
		},
		{
			name:     "default level",
			message:  "a message without level\n",
			expected: `level="info", msg="a message without level"`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			capture := newCaptureLogger()
			logger := newMemberListLogger(capture)
			logger.Println(c.message)
			require.Len(t, capture.lines, 1)
			require.Equal(t, c.expected, capture.lines[0])
		})
	}
}

func newCaptureLogger() *captureLogger {
	c := &captureLogger{
		lines: make([]string, 0),
	}
	c.adapter = golog.New(c, "", 0)
	return c
}

type captureLogger struct {
	lines   []string
	adapter *golog.Logger
}

func (c *captureLogger) Write(p []byte) (n int, err error) {
	c.lines = append(c.lines, string(p))
	return len(p), nil
}

func (c *captureLogger) Log(keyvals ...interface{}) error {
	lineParts := make([]string, 0)
	for i := 0; i < len(keyvals); i += 2 {
		lineParts = append(lineParts, fmt.Sprintf("%v=%q", keyvals[i], keyvals[i+1]))
	}
	c.lines = append(c.lines, strings.Join(lineParts, ", "))
	return nil
}
