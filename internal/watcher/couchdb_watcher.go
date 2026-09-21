package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/localstate"
)

// RemoteWatcher watches the CouchDB _changes feed and applies remote changes to disk.
type RemoteWatcher struct {
	db      couchdb.Client
	dataDir string
	onEvent func(ctx context.Context, event couchdb.ChangeEvent) error
	seqFile string // path to .obgo_seq file for persistence
}

// NewRemoteWatcher creates a new RemoteWatcher.
// Failed events are durably retained and retried without blocking other files.
func NewRemoteWatcher(db couchdb.Client, dataDir string, onEvent func(context.Context, couchdb.ChangeEvent) error) *RemoteWatcher {
	return &RemoteWatcher{
		db:      db,
		dataDir: dataDir,
		onEvent: onEvent,
		seqFile: filepath.Join(dataDir, ".obgo_seq"),
	}
}

// Run starts the remote watcher. Blocks until ctx is cancelled or the changes feed terminates.
func (w *RemoteWatcher) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	since := w.loadSeq()
	pending := map[string]PendingEvent{}
	pendingFile := filepath.Join(w.dataDir, ".obgo_pending")
	if err := localstate.Read(pendingFile, &pending); err != nil {
		return err
	}
	retry := func() error {
		for id, entry := range pending {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Never replay stale content over a newer initial pull or remote edit.
			doc, err := w.db.GetMeta(ctx, id)
			if errors.Is(err, couchdb.ErrNotFound) {
				entry.Event.Deleted = true
				entry.Event.Doc = nil
			} else if err != nil {
				entry.Error = err.Error()
				pending[id] = entry
				continue
			} else {
				entry.Event.Doc = doc
				entry.Event.Deleted = doc.Deleted
			}
			if err := w.onEvent(ctx, entry.Event); err != nil {
				entry.Error = err.Error()
				pending[id] = entry
				fmt.Fprintf(os.Stderr, "remote watcher: retry %q: %v\n", id, err)
			} else {
				delete(pending, id)
			}
		}
		return localstate.Write(pendingFile, pending)
	}
	if len(pending) > 0 {
		if err := retry(); err != nil {
			return err
		}
	}

	ch, err := w.db.Changes(ctx, since)
	if err != nil {
		return fmt.Errorf("remote watcher: open changes: %w", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(pending) > 0 {
				if err := retry(); err != nil {
					return err
				}
			}
		case event, ok := <-ch:
			if !ok {
				return ctx.Err()
			}
			_, wasPending := pending[event.ID]
			if err := w.onEvent(ctx, event); err != nil {
				fmt.Fprintf(os.Stderr, "remote watcher: deferred event %q: %v\n", event.ID, err)
				pending[event.ID] = PendingEvent{Event: event, Error: err.Error()}
				wasPending = true
			} else {
				delete(pending, event.ID)
			}
			// Advance only after success or durable retention for retry.
			if wasPending {
				if err := localstate.Write(pendingFile, pending); err != nil {
					return err
				}
			}
			if event.Seq != nil {
				if err := w.saveSeq(fmt.Sprint(event.Seq)); err != nil {
					return err
				}
			}
		}
	}
}

// loadSeq reads the last persisted sequence number from the seq file.
// Returns "now" if no file exists, so we only watch new changes rather than
// replaying all history (which would overwrite locally-edited files).
func (w *RemoteWatcher) loadSeq() string {
	data, err := os.ReadFile(w.seqFile)
	if err != nil {
		return "now"
	}
	return string(data)
}

// saveSeq persists the current sequence number to the seq file.
func (w *RemoteWatcher) saveSeq(seq string) error {
	return localstate.WriteBytes(w.seqFile, []byte(seq))
}
