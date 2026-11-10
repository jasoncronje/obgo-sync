package watcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const defaultDebounceDur = 500 * time.Millisecond
const defaultRescanInterval = 30 * time.Second

type LocalWatcherOption func(*LocalWatcher)

func WithDebounce(d time.Duration) LocalWatcherOption {
	return func(w *LocalWatcher) { w.debounceDur = d }
}

func WithRescanInterval(d time.Duration) LocalWatcherOption {
	return func(w *LocalWatcher) { w.rescanInterval = d }
}

func WithSyncHidden(syncHidden bool) LocalWatcherOption {
	return func(w *LocalWatcher) { w.syncHidden = syncHidden }
}

// LocalWatcher watches the local vault directory for changes and pushes them to CouchDB.
type LocalWatcher struct {
	dir            string
	suppress       *SuppressSet
	onChange       func(path string, op fsnotify.Op)
	onRemove       func(path string)
	debounceDur    time.Duration
	rescanInterval time.Duration
	syncHidden     bool
	// pendingRemoves holds each removed path with the time its debounce ends.
	// Only the Run goroutine touches it, so however many removals arrive while
	// a slow callback runs, none is dropped.
	pendingRemoves map[string]time.Time
	// pendingChecks holds each path whose change event was suppressed, with
	// the time its suppression ends. The event may have been the app's own
	// write or a real edit made right after it, so the path is reported once
	// more then and the callback decides.
	pendingChecks map[string]time.Time
	watchedDirs   map[string]struct{}
}

// NewLocalWatcher creates a new LocalWatcher.
func NewLocalWatcher(dir string, suppress *SuppressSet, onChange func(string, fsnotify.Op), onRemove func(string), opts ...LocalWatcherOption) *LocalWatcher {
	w := &LocalWatcher{
		dir:            dir,
		suppress:       suppress,
		onChange:       onChange,
		onRemove:       onRemove,
		debounceDur:    defaultDebounceDur,
		rescanInterval: defaultRescanInterval,
		pendingRemoves: make(map[string]time.Time),
		pendingChecks:  make(map[string]time.Time),
		watchedDirs:    make(map[string]struct{}),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run starts the local filesystem watcher. Blocks until ctx is cancelled.
func (w *LocalWatcher) Run(ctx context.Context) error {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fs watcher: create: %w", err)
	}
	defer fw.Close()

	w.watchedDirs = make(map[string]struct{})

	// Add root dir and all subdirectories recursively.
	if err := w.addDirRecursive(fw, w.dir); err != nil {
		return err
	}

	var rescanC <-chan time.Time
	var rescanTicker *time.Ticker
	if w.rescanInterval > 0 {
		rescanTicker = time.NewTicker(w.rescanInterval)
		rescanC = rescanTicker.C
		defer rescanTicker.Stop()
	}
	removeTicker := time.NewTicker(w.removeCheckInterval())
	defer removeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.cancelAllPendingRemoves()
			return ctx.Err()
		case event, ok := <-fw.Events:
			if !ok {
				return nil
			}
			w.handleEvent(fw, event)
		case <-rescanC:
			if err := w.addDirRecursive(fw, w.dir); err != nil {
				fmt.Fprintf(os.Stderr, "fs watcher: rescan: %v\n", err)
			}
		case <-removeTicker.C:
			w.flushDueRemoves()
			w.flushDueChecks()
		case err, ok := <-fw.Errors:
			if !ok {
				return nil
			}
			fmt.Fprintf(os.Stderr, "fs watcher: %v\n", err)
		}
	}
}

func (w *LocalWatcher) addDirRecursive(fw *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable paths
		}
		if d.IsDir() {
			if w.shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return w.addWatchDir(fw, path)
		}
		return nil
	})
}

// emitExistingFiles fires onChange for every eligible file already present
// under dir. Used when a directory appears fully-formed (a move or rename), a
// case for which the kernel emits no per-file events. Hidden entries follow
// the same opt-in policy as handleEvent.
func (w *LocalWatcher) emitExistingFiles(dir string) {
	if w.onChange == nil || !w.inVault(dir) {
		return
	}
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable paths
		}
		name := d.Name()
		if d.IsDir() {
			if path != dir && w.shouldSkipDir(name) {
				return filepath.SkipDir
			}
			return nil
		}
		if w.shouldSkipPath(path) {
			return nil
		}
		if w.suppress.IsSuppressed(path) {
			w.checkLater(path)
			return nil
		}
		w.cancelPendingRemove(path)
		w.onChange(path, fsnotify.Create)
		return nil
	})
}

func (w *LocalWatcher) addWatchDir(fw *fsnotify.Watcher, dir string) error {
	dir = filepath.Clean(dir)
	if _, ok := w.watchedDirs[dir]; ok {
		return nil
	}
	if err := fw.Add(dir); err != nil {
		fmt.Fprintf(os.Stderr, "fs watcher: add %s: %v\n", dir, err)
		return err
	}
	w.watchedDirs[dir] = struct{}{}
	return nil
}

func (w *LocalWatcher) removeWatchDirRecursive(fw *fsnotify.Watcher, dir string) {
	dir = filepath.Clean(dir)
	for watched := range w.watchedDirs {
		if watched == dir || isPathInside(watched, dir) {
			_ = fw.Remove(watched)
			delete(w.watchedDirs, watched)
		}
	}
}

// inVault reports whether path lies under the watched directory.
func (w *LocalWatcher) inVault(path string) bool {
	return path != "" && isPathInside(path, w.dir)
}

func isPathInside(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (w *LocalWatcher) handleEvent(fw *fsnotify.Watcher, event fsnotify.Event) {
	// fsnotify's kqueue backend can report an event for a watch that is
	// already gone, with an empty name, and then a Create for "." (the name
	// cleaned). Acting on that walks the working directory of the process,
	// which under launchd is the whole filesystem.
	if !w.inVault(event.Name) {
		return
	}
	if w.shouldSkipPath(event.Name) {
		return
	}

	switch {
	case event.Has(fsnotify.Create):
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			_ = w.addDirRecursive(fw, event.Name)
			// A directory can appear already populated — `mv` of a whole tree,
			// an extracted archive, a restored backup. The kernel reports only
			// the directory itself and emits no per-file Create events, so
			// without walking it here the contained files are never pushed.
			w.emitExistingFiles(event.Name)
			return
		}
		w.cancelPendingRemove(event.Name)
		fallthrough
	case event.Has(fsnotify.Write):
		w.cancelPendingRemove(event.Name)
		if w.suppress.IsSuppressed(event.Name) {
			w.checkLater(event.Name)
			return
		}
		if w.onChange != nil {
			w.onChange(event.Name, event.Op)
		}
	case event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename):
		w.removeWatchDirRecursive(fw, event.Name)
		if w.suppress.IsSuppressed(event.Name) {
			return
		}
		w.pendingRemoves[event.Name] = time.Now().Add(w.debounceDur)
	}
}

// removeCheckInterval is how often due removals are looked for: a fraction of
// the debounce, so a removal fires close to when its debounce ends.
func (w *LocalWatcher) removeCheckInterval() time.Duration {
	d := w.debounceDur / 4
	if d < 5*time.Millisecond {
		return 5 * time.Millisecond
	}
	if d > 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	return d
}

// flushDueRemoves reports every removal whose debounce has ended, parents
// before their children.
func (w *LocalWatcher) flushDueRemoves() {
	if len(w.pendingRemoves) == 0 {
		return
	}
	now := time.Now()
	var due []string
	for path, at := range w.pendingRemoves {
		if !now.Before(at) {
			due = append(due, path)
		}
	}
	sort.Strings(due)
	for _, path := range due {
		delete(w.pendingRemoves, path)
		if w.onRemove != nil && !w.suppress.IsSuppressed(path) {
			w.onRemove(path)
		}
	}
}

// checkLater schedules one more change report for a path whose event was
// suppressed, for just after its suppression ends.
func (w *LocalWatcher) checkLater(path string) {
	w.pendingChecks[path] = time.Now().Add(w.suppress.TTL() + w.removeCheckInterval())
}

// flushDueChecks reports the paths whose suppression has ended. A path that
// was written by the app again in the meantime waits for that suppression.
func (w *LocalWatcher) flushDueChecks() {
	if len(w.pendingChecks) == 0 {
		return
	}
	now := time.Now()
	var due []string
	for path, at := range w.pendingChecks {
		if !now.Before(at) {
			due = append(due, path)
		}
	}
	sort.Strings(due)
	for _, path := range due {
		delete(w.pendingChecks, path)
		if w.suppress.IsSuppressed(path) {
			w.checkLater(path)
			continue
		}
		if info, err := os.Lstat(path); err != nil || info.IsDir() {
			continue
		}
		if w.onChange != nil {
			w.onChange(path, fsnotify.Write)
		}
	}
}

func (w *LocalWatcher) shouldSkipDir(name string) bool {
	if name == "." {
		return false
	}
	if isProtectedName(name) {
		return true
	}
	return !w.syncHidden && isHiddenName(name)
}

func (w *LocalWatcher) shouldSkipPath(path string) bool {
	base := filepath.Base(path)
	if isProtectedName(base) {
		return true
	}
	return !w.syncHidden && isHiddenName(base)
}

func isHiddenName(name string) bool {
	return len(name) > 0 && name[0] == '.'
}

func isProtectedName(name string) bool {
	return name == ".git" || strings.HasPrefix(name, ".obgo")
}

func (w *LocalWatcher) cancelPendingRemove(path string) {
	delete(w.pendingRemoves, path)
}

func (w *LocalWatcher) cancelAllPendingRemoves() {
	clear(w.pendingRemoves)
}
