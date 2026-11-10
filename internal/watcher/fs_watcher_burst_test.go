package watcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Many removals whose debounce ends while a slow upload holds the watcher
// must all be reported. They used to pass through a 32-slot channel with a
// non-blocking send, which dropped the rest.
func TestLocalWatcher_RemovalsSurviveSlowCallback(t *testing.T) {
	dir := t.TempDir()
	const n = 200
	files := make([]string, n)
	for i := range files {
		files[i] = filepath.Join(dir, fmt.Sprintf("note-%03d.md", i))
		if err := os.WriteFile(files[i], []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	removed := make(map[string]bool)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	lw := NewLocalWatcher(dir, NewSuppressSet(), func(string, fsnotify.Op) {
		time.Sleep(400 * time.Millisecond)
	}, func(path string) {
		mu.Lock()
		removed[path] = true
		mu.Unlock()
	}, WithDebounce(50*time.Millisecond))

	errCh := make(chan error, 1)
	go func() { errCh <- lw.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)

	// The removals are queued first; the write behind them starts the slow
	// callback, during which every removal's debounce ends.
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "slow.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := waitForEvent(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(removed) == n
	})
	if !got {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("%d of %d removals were reported", len(removed), n)
	}

	cancel()
	_ = errCh
}
