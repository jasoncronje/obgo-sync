package sync_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/crypto"
	"github.com/jookos/obgo-sync/internal/localstate"
	syncsvc "github.com/jookos/obgo-sync/internal/sync"
	"github.com/jookos/obgo-sync/internal/watcher"
)

func TestInternalOwnershipConsistentAcrossPullWatchPush(t *testing.T) {
	db := &internalEventClient{mockClient: newMockClient()}
	db.chunkDocs["h:old"] = couchdb.ChunkDoc{ID: "h:old", Data: "old"}
	db.chunkDocs["h:new"] = couchdb.ChunkDoc{ID: "h:new", Data: "new"}
	ordinary := couchdb.MetaDoc{ID: "f:ordinary", Path: ".codex/config", Type: "plain", Children: []string{"h:old"}}
	internal := couchdb.MetaDoc{ID: "i:f:internal", Path: "i:.codex/config", Type: "plain", Children: []string{"h:new"}}
	db.metaDocs = []couchdb.MetaDoc{ordinary, internal}
	root := t.TempDir()
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	for _, filter := range []string{".codex/config", ".codex/", ""} {
		if err := s.Pull(context.Background(), filter); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(filepath.Join(root, ".codex/config"))
		if string(got) != "new" {
			t.Fatalf("filter %q got %q", filter, got)
		}
	}
	db.events = []couchdb.ChangeEvent{{ID: ordinary.ID, Doc: &ordinary}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(root, ".codex/config"))
	if string(got) != "new" {
		t.Fatal("ordinary watch event overwrote internal content")
	}
	if err := os.WriteFile(filepath.Join(root, ".codex/config"), []byte("local"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.Push(context.Background(), ".codex/config"); !errors.Is(err, syncsvc.ErrReceiveOnly) {
		t.Fatalf("push: %v", err)
	}
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, ".codex/config")); err != nil {
		t.Fatal(err)
	}
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("receive-only path produced remote writes")
	}
}

func TestNewInternalOwnershipBeforeInternalEvent(t *testing.T) {
	db := &internalEventClient{mockClient: newMockClient()}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:.codex/config", Path: "i:.codex/config", Type: "plain"}}
	db.events = []couchdb.ChangeEvent{{ID: ".codex/config", Doc: &couchdb.MetaDoc{ID: ".codex/config", Path: ".codex/config", Type: "plain"}}}
	root := t.TempDir()
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex/config")); !os.IsNotExist(err) {
		t.Fatal("ordinary event applied before internal event")
	}
}

func TestInternalIDBlocksOrdinaryWriterWithUnreadableMetadata(t *testing.T) {
	db := newMockClient()
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:.codex/config", Path: "/\\:unreadable", Type: "plain"}}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".codex"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".codex/config"), []byte("local"), 0644); err != nil {
		t.Fatal(err)
	}
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Push(context.Background(), ".codex/config"); !errors.Is(err, syncsvc.ErrReceiveOnly) {
		t.Fatalf("push: %v", err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("unreadable internal metadata allowed duplicate upload")
	}
}

func TestInternalDeletionSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Path: "i:.codex/config", Type: "plain"}}
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	db.metaDocs = nil
	db.events = []couchdb.ChangeEvent{{ID: "i:f:one", Seq: "43", Deleted: true}}
	restarted := syncsvc.New(db, crypto.New(""), root)
	restarted.SetSyncHidden(true)
	if err := restarted.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex/config")); !os.IsNotExist(err) {
		t.Fatal("deleted file survived restart")
	}
}

func TestInternalFailedWatchRetriesLatestRevisionAfterRestart(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	doc := couchdb.MetaDoc{ID: "i:f:one", Path: "i:.codex/config", Type: "plain", Children: []string{"h:missing"}}
	db.metaDocs = []couchdb.MetaDoc{doc}
	db.events = []couchdb.ChangeEvent{{ID: doc.ID, Seq: "42", Doc: &doc}}
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	pending := map[string]watcher.PendingEvent{}
	if err := localstate.Read(filepath.Join(root, ".obgo_pending"), &pending); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatal("failed event not retained")
	}
	checkpoint, _ := os.ReadFile(filepath.Join(root, ".obgo_seq"))
	if string(checkpoint) != "42" {
		t.Fatal("checkpoint not advanced after durable retention")
	}
	db.metaDocs[0].Children = []string{"h:latest"}
	db.chunkDocs["h:latest"] = couchdb.ChunkDoc{ID: "h:latest", Data: "latest revision"}
	db.events = nil
	restarted := syncsvc.New(db, crypto.New(""), root)
	restarted.SetSyncHidden(true)
	if err := restarted.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(root, ".codex/config"))
	if string(got) != "latest revision" {
		t.Fatalf("retry got %q", got)
	}
	pending = map[string]watcher.PendingEvent{}
	if err := localstate.Read(filepath.Join(root, ".obgo_pending"), &pending); err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatal("successful retry not cleared")
	}
}

func TestInternalUnsupportedDoesNotStopOtherFiles(t *testing.T) {
	for _, conflicted := range []bool{false, true} {
		root := t.TempDir()
		db := newMockClient()
		bad := couchdb.MetaDoc{ID: "i:f:one", Path: "i:.codex/config", Type: "plain"}
		if conflicted {
			bad.Conflicts = []string{"2-other"}
		} else {
			bad.Eden = map[string]interface{}{"h:x": "embedded"}
		}
		db.metaDocs = []couchdb.MetaDoc{bad, {ID: "i:f:two", Path: "i:.codex/second", Type: "plain"}}
		s := syncsvc.New(db, crypto.New(""), root)
		s.SetSyncHidden(true)
		if err := s.Pull(context.Background(), ""); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(root, ".codex/second")); err != nil {
			t.Fatal(err)
		}
		pending := map[string]watcher.PendingEvent{}
		if err := localstate.Read(filepath.Join(root, ".obgo_pending"), &pending); err != nil {
			t.Fatal(err)
		}
		if len(pending) != 1 || pending[bad.ID].Error == "" {
			t.Fatal("unsupported document not reported durably")
		}
		if db.putMetaCalled {
			t.Fatal("modified remote conflicts")
		}
	}
}
