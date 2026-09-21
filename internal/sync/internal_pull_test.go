package sync_test

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/crypto"
	syncsvc "github.com/jookos/obgo-sync/internal/sync"
)

type internalEventClient struct {
	*mockClient
	events []couchdb.ChangeEvent
}

func (m *internalEventClient) Changes(ctx context.Context, since string) (<-chan couchdb.ChangeEvent, error) {
	ch := make(chan couchdb.ChangeEvent, len(m.events))
	for _, event := range m.events {
		ch <- event
	}
	close(ch)
	return ch, nil
}

func TestWatchInternalEncryptedAndDeletion(t *testing.T) {
	root := t.TempDir()
	cr := crypto.New("test-password")
	cr.SetSalt(make([]byte, 32))
	data, err := cr.EncryptContent([]byte(base64.StdEncoding.EncodeToString([]byte("hidden watch content"))))
	if err != nil {
		t.Fatal(err)
	}
	path, err := cr.EncryptPath("i:.codex/config", 1, 2, 20, []string{"h:test"})
	if err != nil {
		t.Fatal(err)
	}
	db := &internalEventClient{mockClient: newMockClient()}
	db.chunkDocs["h:test"] = couchdb.ChunkDoc{ID: "h:test", Data: data, Encrypted: true}
	db.events = []couchdb.ChangeEvent{{ID: "i:f:hash", Doc: &couchdb.MetaDoc{ID: "i:f:hash", Type: "newnote", Path: path}}}
	s := syncsvc.New(db, cr, root)
	s.SetSyncHidden(true)
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, ".codex/config"))
	if err != nil || string(got) != "hidden watch content" {
		t.Fatalf("%q %v", got, err)
	}
	db.events = []couchdb.ChangeEvent{{ID: "i:f:hash", Deleted: true}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex/config")); !os.IsNotExist(err) {
		t.Fatal("lean tombstone did not delete mapped path")
	}
	if db.putMetaCalled {
		t.Fatal("watch modified remote metadata")
	}
}

func TestPullInternalFiles(t *testing.T) {
	for _, kind := range []string{"plain", "newnote"} {
		for _, enabled := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-off", true: "-on"}[enabled], func(t *testing.T) {
				root := t.TempDir()
				db := newMockClient()
				data := "actual hidden content"
				if kind == "newnote" {
					data = base64.StdEncoding.EncodeToString([]byte(data))
				}
				db.chunkDocs["h:test"] = couchdb.ChunkDoc{ID: "h:test", Data: data}
				db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:hash", Path: "i:.codex/config.toml", Type: kind, Children: []string{"h:test"}}}
				s := syncsvc.New(db, crypto.New(""), root)
				s.SetSyncHidden(enabled)
				if err := s.Pull(context.Background(), ".codex/"); err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(filepath.Join(root, ".codex/config.toml"))
				if enabled && (err != nil || string(got) != "actual hidden content") {
					t.Fatalf("%q %v", got, err)
				}
				if !enabled && !os.IsNotExist(err) {
					t.Fatal("hidden file written while disabled")
				}
				if db.putMetaCalled {
					t.Fatal("pull modified remote metadata")
				}
			})
		}
	}
}

func TestPullInternalRejectsUnsafePathsAndConflicts(t *testing.T) {
	for _, path := range []string{"../escape", "/tmp/escape", ".codex/../../escape"} {
		t.Run(path, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Symlink(t.TempDir(), filepath.Join(root, "link")); err != nil {
				t.Fatal(err)
			}
			db := newMockClient()
			db.metaDocs = []couchdb.MetaDoc{{ID: "i:test", Path: "i:" + path, Type: "plain"}}
			s := syncsvc.New(db, crypto.New(""), root)
			s.SetSyncHidden(true)
			if err := s.Pull(context.Background(), path); err == nil {
				t.Fatal("accepted unsafe path")
			}
		})
	}
	root := t.TempDir()
	db := newMockClient()
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:test", Path: "i:.codex/config", Type: "plain", Conflicts: []string{"2-conflict"}}}
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Pull(context.Background(), ".codex/config"); err == nil || db.putMetaCalled {
		t.Fatal("conflict must remain untouched")
	}
}

func TestPullInternalProtectsLocalState(t *testing.T) {
	for _, name := range []string{".git/config", ".obgo_seq"} {
		root := t.TempDir()
		file := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte("keep"), 0644); err != nil {
			t.Fatal(err)
		}
		db := newMockClient()
		db.metaDocs = []couchdb.MetaDoc{{ID: "i:" + name, Path: "i:" + name, DeletedApp: true}}
		s := syncsvc.New(db, crypto.New(""), root)
		s.SetSyncHidden(true)
		if err := s.Pull(context.Background(), ""); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(file)
		if err != nil || string(got) != "keep" {
			t.Fatal("protected file changed")
		}
	}
}

func TestPullInternalPreservesSymlink(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "config"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, ".alias")); err != nil {
		t.Fatal(err)
	}
	db := newMockClient()
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:.alias/config", Path: "i:.alias/config", Type: "plain"}}
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(target, "config"))
	if err != nil || string(got) != "keep" {
		t.Fatal("symlink target changed")
	}
}
