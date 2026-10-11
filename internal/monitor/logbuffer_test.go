package monitor

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestLogBufferRingKeepsExactTail(t *testing.T) {
	for _, size := range []int{-1, 0, 1, 7, 64} {
		lb := NewLogBuffer(size)
		var full []byte
		for _, text := range []string{"abc", "d", "efghijk", "", strings.Repeat("z", 100), "tail", "!"} {
			n, err := lb.Write([]byte(text))
			if n != len(text) || err != nil {
				t.Fatalf("Write=%d, %v", n, err)
			}
			full = append(full, text...)
			capacity := max(size, 0)
			want := full[max(0, len(full)-capacity):]
			if got := lb.Content(); got != string(want) {
				t.Fatalf("size %d content=%q want=%q", size, got, want)
			}
			if len(lb.buf) != capacity || cap(lb.buf) != capacity {
				t.Fatal("ring allocation grew")
			}
		}
	}
}

func TestLogBufferConcurrentReadersAndWriters(t *testing.T) {
	lb := NewLogBuffer(128)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				_, _ = lb.Write([]byte("message\n"))
				if len(lb.Content()) > 128 {
					t.Error("ring exceeded capacity")
				}
			}
		}()
	}
	wg.Wait()
}

// The legacy append+tail implementation is retained only as a benchmark fixture.
func BenchmarkLogBufferWrite(b *testing.B) {
	for _, size := range []int{512, 1024 * 1024} {
		p := bytes.Repeat([]byte("x"), size)
		name := "512B"
		if size > 512 {
			name = "1MiB"
		}
		b.Run(name+"/ring", func(b *testing.B) {
			lb := NewLogBuffer(64 * 1024)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				_, _ = lb.Write(p)
			}
		})
		b.Run(name+"/legacy", func(b *testing.B) {
			buf := make([]byte, 0, 64*1024)
			var mu sync.Mutex
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for range b.N {
				mu.Lock()
				buf = append(buf, p...)
				if len(buf) > 64*1024 {
					buf = buf[len(buf)-64*1024:]
				}
				mu.Unlock()
			}
		})
	}
}
