package monitor

import (
	"io"
	"sync"
)

// LogBuffer is a fixed-capacity, thread-safe ring capturing recent log output.
// Writes retain only the newest bytes without allocating in proportion to the
// caller's write size or periodically copying all previously retained logs.
type LogBuffer struct {
	mu    sync.Mutex
	buf   []byte
	size  int
	start int
	count int
}

func NewLogBuffer(size int) *LogBuffer {
	if size < 0 {
		size = 0
	}
	return &LogBuffer{buf: make([]byte, size), size: size}
}

func (lb *LogBuffer) Write(p []byte) (n int, err error) {
	n = len(p)
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.size == 0 || n == 0 {
		return n, nil
	}
	if n >= lb.size {
		copy(lb.buf, p[n-lb.size:])
		lb.start, lb.count = 0, lb.size
		return n, nil
	}
	end := (lb.start + lb.count) % lb.size
	first := copy(lb.buf[end:], p)
	copy(lb.buf, p[first:])
	if overflow := lb.count + n - lb.size; overflow > 0 {
		lb.start = (lb.start + overflow) % lb.size
		lb.count = lb.size
	} else {
		lb.count += n
	}
	return n, nil
}

func (lb *LogBuffer) Content() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.count == 0 {
		return ""
	}
	content := make([]byte, lb.count)
	first := copy(content, lb.buf[lb.start:])
	copy(content[first:], lb.buf[:lb.count-first])
	return string(content)
}

// SharedLogBuffer is the global log buffer accessible by the server.
var SharedLogBuffer = NewLogBuffer(64 * 1024)

func LogWriter() io.Writer { return SharedLogBuffer }
