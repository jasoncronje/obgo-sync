package watcher_test

import (
	"context"
	"errors"
	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/localstate"
	"github.com/jookos/obgo-sync/internal/watcher"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPendingWriteFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".obgo_seq"), []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	ch := make(chan couchdb.ChangeEvent, 1)
	ch <- couchdb.ChangeEvent{ID: "bad", Seq: "after"}
	close(ch)
	rw := watcher.NewRemoteWatcher(&mockChangesClient{ch: ch}, dir, func(ctx context.Context, e couchdb.ChangeEvent) error {
		if err := os.Mkdir(filepath.Join(dir, ".obgo_pending"), 0700); err != nil {
			return err
		}
		return errors.New("missing chunk")
	})
	if err := rw.Run(context.Background()); err == nil {
		t.Fatal("persistence failure not propagated")
	}
	seq, _ := os.ReadFile(filepath.Join(dir, ".obgo_seq"))
	if string(seq) != "before" {
		t.Fatalf("advanced despite failed persistence: %q", seq)
	}
}

func TestFailedEventRetriesWithoutNewEvents(t *testing.T) {
	dir := t.TempDir()
	ch := make(chan couchdb.ChangeEvent, 1)
	ch <- couchdb.ChangeEvent{ID: "bad", Seq: "1"}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	calls := 0
	rw := watcher.NewRemoteWatcher(&mockChangesClient{ch: ch}, dir, func(ctx context.Context, e couchdb.ChangeEvent) error {
		calls++
		if calls == 1 {
			return errors.New("temporary failure")
		}
		cancel()
		return nil
	})
	_ = rw.Run(ctx)
	if calls != 2 {
		t.Fatalf("idle retry calls: %d", calls)
	}
	pending := map[string]watcher.PendingEvent{}
	if err := localstate.Read(filepath.Join(dir, ".obgo_pending"), &pending); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatal("successful idle retry still pending")
	}
}

func TestNewSuccessSupersedesPendingEvent(t *testing.T) {
	dir := t.TempDir()
	ch := make(chan couchdb.ChangeEvent, 2)
	ch <- couchdb.ChangeEvent{ID: "same", Seq: "1"}
	ch <- couchdb.ChangeEvent{ID: "same", Seq: "2"}
	close(ch)
	rw := watcher.NewRemoteWatcher(&mockChangesClient{ch: ch}, dir, func(ctx context.Context, e couchdb.ChangeEvent) error {
		if e.Seq == "1" {
			return errors.New("temporary failure")
		}
		return nil
	})
	if err := rw.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	pending := map[string]watcher.PendingEvent{}
	if err := localstate.Read(filepath.Join(dir, ".obgo_pending"), &pending); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatal("stale event retained after newer success")
	}
}

type pendingMetaClient struct {
	*mockChangesClient
	doc *couchdb.MetaDoc
}

func (c *pendingMetaClient) GetMeta(ctx context.Context, id string) (*couchdb.MetaDoc, error) {
	return c.doc, nil
}

func TestRetryPreservesApplicationDeletionMarker(t *testing.T) {
	dir := t.TempDir()
	if err := watcher.DeferEvent(dir, couchdb.ChangeEvent{ID: "file"}, errors.New("retry")); err != nil {
		t.Fatal(err)
	}
	ch := make(chan couchdb.ChangeEvent)
	close(ch)
	c := &pendingMetaClient{mockChangesClient: &mockChangesClient{ch: ch}, doc: &couchdb.MetaDoc{ID: "file", Path: "note.md", DeletedApp: true}}
	called := false
	rw := watcher.NewRemoteWatcher(c, dir, func(ctx context.Context, e couchdb.ChangeEvent) error {
		called = true
		if e.Deleted || e.Doc == nil || !e.Doc.DeletedApp {
			t.Fatal("retry changed app deletion into transport tombstone")
		}
		return nil
	})
	if err := rw.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("pending event not retried")
	}
}
