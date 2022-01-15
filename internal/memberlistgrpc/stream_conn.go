package memberlistgrpc

import (
	"bytes"
	context "context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// streamClient is implemented by both Transport_StreamPacketsClient and
// Transport_StreamPacketsServer
type streamClient interface {
	Send(*Message) error
	Recv() (*Message, error)
}

type packetsClientConn struct {
	cli     streamClient
	onClose func()
	closed  bool

	localAddr, remoteAddr net.Addr

	readCnd           *sync.Cond         // Signals waiting readers
	spawnReader       sync.Once          // Used to lazily launch a cli reader
	readMessages      chan readResult    // Messages read from the cli reader
	readTimeout       time.Time          // Read deadline
	readTimeoutCancel context.CancelFunc // Cancel read deadline and wake up goroutines
	readBuffer        bytes.Buffer       // Data buffer ready for immediate reading

	writeMut sync.Mutex
}

type readResult struct {
	Message *Message
	Error   error
}

func (c *packetsClientConn) Read(b []byte) (n int, err error) {
	// Lazily spawn a background goroutine to reaed from our stream client.
	c.spawnReader.Do(func() {
		go func() {
			defer close(c.readMessages)

			for {
				msg, err := c.cli.Recv()
				c.readCnd.Broadcast() // Wake up sleeping goroutines
				c.readMessages <- readResult{
					Message: msg,
					Error:   err,
				}
				if err != nil {
					return
				}
			}
		}()
	})

	for n == 0 {
		n2, err := c.readOrBlock(b)
		if err != nil {
			return n2, err
		}
		n += n2
	}
	return n, nil
}

func (c *packetsClientConn) readOrBlock(b []byte) (n int, err error) {
	c.readCnd.L.Lock()
	defer c.readCnd.L.Unlock()
	if !c.readTimeout.IsZero() && !time.Now().Before(c.readTimeout) {
		return 0, os.ErrDeadlineExceeded
	}

	// Read from the existing buffer first.
	n, err = c.readBuffer.Read(b)
	if err != nil && !errors.Is(err, io.EOF) {
		return
	} else if n != 0 {
		return
	}

	// We've emptied our buffer. Pull the next message in or wait for a message
	// to be available.
	select {
	case msg, ok := <-c.readMessages:
		if !ok {
			return n, io.EOF
		}
		_, err = c.readBuffer.Write(msg.Message.Data)
		if err != nil {
			return n, err
		}
		return n, msg.Error
	default:
		c.readCnd.Wait() // Wait for something to be written or for the timeout to fire
		return 0, nil
	}
}

func (c *packetsClientConn) Write(b []byte) (n int, err error) {
	c.writeMut.Lock()
	defer c.writeMut.Unlock()

	err = c.cli.Send(&Message{Data: b})
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *packetsClientConn) Close() error {
	c.writeMut.Lock()
	defer c.writeMut.Unlock()

	if clientStream, ok := c.cli.(Transport_StreamPacketsClient); ok {
		return clientStream.CloseSend()
	}

	if !c.closed {
		c.closed = true
		if c.onClose != nil {
			c.onClose()
		}
	}
	return nil
}

func (c *packetsClientConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *packetsClientConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *packetsClientConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}

func (c *packetsClientConn) SetReadDeadline(t time.Time) error {
	c.readCnd.L.Lock()
	defer c.readCnd.L.Unlock()

	c.readTimeout = t

	// There should only be one deadline goroutine at a time, so cancel it if it
	// already exists.
	if c.readTimeoutCancel != nil {
		c.readTimeoutCancel()
		c.readTimeoutCancel = nil
	}
	c.readTimeoutCancel = c.deadlineTimer(t)
	return nil
}

func (c *packetsClientConn) deadlineTimer(t time.Time) context.CancelFunc {
	if t.IsZero() {
		// Deadline of zero means to wait forever.
		return nil
	}
	if t.Before(time.Now()) {
		c.readCnd.Broadcast()
	}
	ctx, cancel := context.WithDeadline(context.Background(), t)
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.readCnd.Broadcast()
		}
	}()
	return cancel
}

func (c *packetsClientConn) SetWriteDeadline(t time.Time) error {
	// no-op: writes finish immediately
	return nil
}
