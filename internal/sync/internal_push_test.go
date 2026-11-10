package sync_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/crypto"
	syncsvc "github.com/jookos/obgo-sync/internal/sync"
)

func newInternalPushService(db couchdb.Client, cr *crypto.Service, root string) *syncsvc.Service {
	s := syncsvc.New(db, cr, root)
	s.SetSyncHidden(true)
	s.SetInternalPush(true)
	return s
}

func writeHidden(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestPushInternalCreatesInternalDocument(t *testing.T) {
	for rel, wantType := range map[string]string{".agents/skills/job/SKILL.md": "plain", ".codex/config.toml": "newnote"} {
		t.Run(rel, func(t *testing.T) {
			root := t.TempDir()
			db := newMockClient()
			writeHidden(t, root, rel, "hidden content")
			s := newInternalPushService(db, crypto.New(""), root)
			if err := s.Push(context.Background(), rel); err != nil {
				t.Fatal(err)
			}
			if len(db.putMetaDocs) != 1 {
				t.Fatalf("expected one meta write, got %d", len(db.putMetaDocs))
			}
			doc := db.putMetaDocs[0]
			if doc.ID != "i:"+strings.ToLower(rel) || doc.Path != "i:"+rel || doc.Type != wantType {
				t.Fatalf("unexpected internal document: %+v", doc)
			}
			if doc.Size != int64(len("hidden content")) || len(doc.Children) != 1 || doc.IsDeleted() {
				t.Fatalf("unexpected internal metadata: %+v", doc)
			}
		})
	}
}

func TestPushInternalUpdatesOwningDocument(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.chunkDocs["h:old"] = couchdb.ChunkDoc{ID: "h:old", Data: "old"}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Rev: "4-abc", Path: "i:.codex/notes.md", Type: "plain", Size: 3, CTime: 7, Children: []string{"h:old"}}}
	writeHidden(t, root, ".codex/notes.md", "new text")
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if len(db.putMetaDocs) != 1 {
		t.Fatalf("expected one meta write, got %d", len(db.putMetaDocs))
	}
	doc := db.putMetaDocs[0]
	if doc.ID != "i:f:one" || doc.Rev != "4-abc" || doc.CTime != 7 || doc.Size != 8 {
		t.Fatalf("owning document was not updated in place: %+v", doc)
	}
}

func TestPushInternalSkipsUnchangedContent(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.chunkDocs["h:a"] = couchdb.ChunkDoc{ID: "h:a", Data: "same "}
	db.chunkDocs["h:b"] = couchdb.ChunkDoc{ID: "h:b", Data: "text"}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Rev: "2-abc", Path: "i:.codex/notes.md", Type: "plain", Size: 9, Children: []string{"h:a", "h:b"}}}
	writeHidden(t, root, ".codex/notes.md", "same text")
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Push(context.Background(), ".codex/notes.md"); err != nil {
		t.Fatal(err)
	}
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("unchanged hidden file produced remote writes")
	}
}

func TestPushInternalTombstonesMissingFile(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Rev: "2-abc", Path: "i:.codex/notes.md", Type: "plain", Size: 3, MTime: 5, Children: []string{"h:old"}}}
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Push(context.Background(), ".codex/notes.md"); err != nil {
		t.Fatal(err)
	}
	if len(db.putMetaDocs) != 1 {
		t.Fatalf("expected one meta write, got %d", len(db.putMetaDocs))
	}
	doc := db.putMetaDocs[0]
	if doc.ID != "i:f:one" || !doc.DeletedApp || doc.Deleted || doc.Size != 0 || len(doc.Children) != 0 || doc.MTime <= 5 {
		t.Fatalf("unexpected internal tombstone: %+v", doc)
	}
	if doc.Path != "i:.codex/notes.md" {
		t.Fatalf("tombstone lost its path: %q", doc.Path)
	}
}

func TestPushInternalKeepsExistingFileOnTombstone(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.chunkDocs["h:a"] = couchdb.ChunkDoc{ID: "h:a", Data: "kept"}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Path: "i:.codex/notes.md", Type: "plain", Size: 4, Children: []string{"h:a"}}}
	writeHidden(t, root, ".codex/notes.md", "kept")
	s := newInternalPushService(db, crypto.New(""), root)
	// A whole-folder push must never tombstone i: documents on its own.
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled {
		t.Fatal("existing hidden file was rewritten or tombstoned")
	}
}

func TestPushInternalSkipsSymlinksAndProtectedNames(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	writeHidden(t, root, ".agents/skills/job/SKILL.md", "canonical")
	if err := os.MkdirAll(filepath.Join(root, ".claude/skills"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../../.agents/skills/job", filepath.Join(root, ".claude/skills/job")); err != nil {
		t.Fatal(err)
	}
	ignored := []string{".codex/.DS_Store", ".codex/node_modules/pkg/index.js", ".trash/old.md", ".obsidian/plugins/obsidian-livesync/data.json"}
	for _, rel := range ignored {
		writeHidden(t, root, rel, "ignored")
	}
	s := newInternalPushService(db, crypto.New(""), root)
	for _, rel := range append([]string{".claude/skills/job/SKILL.md"}, ignored...) {
		if err := s.Push(context.Background(), rel); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("symlinked or ignored hidden path produced remote writes")
	}
}

func TestPushInternalEncryptedRoundTrip(t *testing.T) {
	for _, rel := range []string{".agents/skills/job/SKILL.md", ".claude/skills/job/scripts/tool.bin"} {
		t.Run(rel, func(t *testing.T) {
			content := "line one\nline two\n"
			if strings.HasSuffix(rel, ".bin") {
				content = string([]byte{0xff, 0x00, 0x10, 0x80, 0x7f})
			}
			cr := crypto.New("test-password")
			cr.SetSalt(make([]byte, 32))
			source := t.TempDir()
			db := newMockClient()
			writeHidden(t, source, rel, content)
			pusher := newInternalPushService(db, cr, source)
			pusher.SetObfuscationMode("on")
			if err := pusher.Push(context.Background(), rel); err != nil {
				t.Fatal(err)
			}
			if len(db.putMetaDocs) != 1 {
				t.Fatalf("expected one meta write, got %d", len(db.putMetaDocs))
			}
			pushed := *db.putMetaDocs[0]
			// LiveSync hashes the ID of an i: document but leaves its metadata
			// readable; only the chunks are encrypted.
			if !strings.HasPrefix(pushed.ID, "i:f:") || pushed.Path != "i:"+rel || pushed.Encrypted || len(pushed.Children) == 0 {
				t.Fatalf("internal document does not match LiveSync's format: %+v", pushed)
			}

			// A second device must be able to pull exactly what was pushed.
			db.metaDocs = []couchdb.MetaDoc{pushed}
			target := t.TempDir()
			puller := syncsvc.New(db, cr, target)
			puller.SetSyncHidden(true)
			if err := puller.Pull(context.Background(), ""); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(target, filepath.FromSlash(rel)))
			if err != nil || string(got) != content {
				t.Fatalf("round trip mismatch: %q %v", got, err)
			}
		})
	}
}

func TestWatchInternalSkipsOwnRevision(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	rel := ".codex/notes.md"
	writeHidden(t, root, rel, "pushed")
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	id := db.putMetaDocs[0].ID
	// The file is edited again before the change feed reports the push.
	writeHidden(t, root, rel, "edited again")
	db.chunkDocs["h:echo"] = couchdb.ChunkDoc{ID: "h:echo", Data: "pushed"}
	echo := couchdb.MetaDoc{ID: id, Rev: "1-abc", Type: "plain", Path: "i:" + rel, Size: 6, Children: []string{"h:echo"}}
	db.events = []couchdb.ChangeEvent{{ID: id, Doc: &echo}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "edited again" {
		t.Fatalf("own revision was applied over a newer local edit: %q", got)
	}

	// A revision written by another device is still applied.
	db.chunkDocs["h:other"] = couchdb.ChunkDoc{ID: "h:other", Data: "from elsewhere"}
	other := couchdb.MetaDoc{ID: id, Rev: "2-def", Type: "plain", Path: "i:" + rel, Size: 14, Children: []string{"h:other"}}
	db.events = []couchdb.ChangeEvent{{ID: id, Doc: &other}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "from elsewhere" {
		t.Fatalf("remote revision was not applied: %q", got)
	}
}

// pullDoc is the remote i: document the pull tests reconcile against.
func pullDoc(db *mockClient, rel, content string, mtime int64) {
	db.chunkDocs["h:remote"] = couchdb.ChunkDoc{ID: "h:remote", Data: content}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Rev: "3-abc", Path: "i:" + rel, Type: "plain", Size: int64(len(content)), MTime: mtime, Children: []string{"h:remote"}}}
}

func TestPullInternalKeepsNewerLocalEdit(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := ".codex/notes.md"
	pullDoc(db, rel, "remote", time.Now().Add(-time.Hour).UnixMilli())
	writeHidden(t, root, rel, "edited while stopped")
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "edited while stopped" {
		t.Fatalf("pull replaced a newer local edit: %q", got)
	}
	if len(db.putMetaDocs) != 1 || db.putMetaDocs[0].ID != "i:f:one" || db.putMetaDocs[0].Size != int64(len("edited while stopped")) {
		t.Fatalf("newer local edit was not uploaded: %+v", db.putMetaDocs)
	}
}

func TestPullInternalAppliesNewerRemote(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := ".codex/notes.md"
	remoteMTime := time.Now().Add(-time.Minute).UnixMilli()
	pullDoc(db, rel, "remote", remoteMTime)
	writeHidden(t, root, rel, "stale local")
	old := time.Now().Add(-24 * time.Hour)
	if err := os.Chtimes(filepath.Join(root, rel), old, old); err != nil {
		t.Fatal(err)
	}
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "remote" {
		t.Fatalf("newer remote version was not applied: %q", got)
	}
	info, err := os.Stat(filepath.Join(root, rel))
	if err != nil || info.ModTime().UnixMilli()/1000 != remoteMTime/1000 {
		t.Fatalf("pulled file does not carry the document mtime: %v %v", info.ModTime(), err)
	}
	if db.putMetaCalled {
		t.Fatal("applying a remote version produced a remote write")
	}
}

func TestPullInternalLeavesMatchingFileAlone(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := ".codex/notes.md"
	remoteMTime := time.Now().Add(-time.Hour).UnixMilli()
	pullDoc(db, rel, "same", remoteMTime)
	writeHidden(t, root, rel, "same")
	s := newInternalPushService(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, rel))
	if err != nil || info.ModTime().UnixMilli()/1000 != remoteMTime/1000 {
		t.Fatalf("matching file was not aligned to the document mtime: %v %v", info.ModTime(), err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("matching file produced remote writes")
	}
}

func TestPullInternalDeletion(t *testing.T) {
	for name, tc := range map[string]struct {
		tombstoneAge time.Duration
		wantKept     bool
	}{
		"edited after the deletion":   {time.Hour, true},
		"deleted after the last edit": {-time.Hour, false},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			db := newMockClient()
			rel := ".codex/notes.md"
			db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Rev: "5-abc", Path: "i:" + rel, Type: "plain", DeletedApp: true, MTime: time.Now().Add(-tc.tombstoneAge).UnixMilli(), Children: []string{}}}
			writeHidden(t, root, rel, "local")
			s := newInternalPushService(db, crypto.New(""), root)
			if err := s.Pull(context.Background(), ""); err != nil {
				t.Fatal(err)
			}
			_, err := os.Stat(filepath.Join(root, rel))
			if kept := err == nil; kept != tc.wantKept {
				t.Fatalf("kept=%v, want %v", kept, tc.wantKept)
			}
			if uploaded := len(db.putMetaDocs) == 1 && !db.putMetaDocs[0].IsDeleted(); uploaded != tc.wantKept {
				t.Fatalf("uploaded=%v, want %v: %+v", uploaded, tc.wantKept, db.putMetaDocs)
			}
		})
	}
}

func TestPushInternalKeepsDocumentForCaseVariant(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.chunkDocs["h:a"] = couchdb.ChunkDoc{ID: "h:a", Data: "kept"}
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Path: "i:.codex/Notes.md", Type: "plain", Size: 4, Children: []string{"h:a"}}}
	writeHidden(t, root, ".codex/Notes.md", "kept")
	s := newInternalPushService(db, crypto.New(""), root)
	// On a case-sensitive filesystem this path is missing, but its document is
	// the one that still backs Notes.md.
	if err := s.Push(context.Background(), ".codex/notes.md"); err != nil {
		t.Fatal(err)
	}
	for _, doc := range db.putMetaDocs {
		if doc.IsDeleted() {
			t.Fatalf("document of a case-variant file was tombstoned: %+v", doc)
		}
	}
}

func TestPushInternalDisabledStaysReceiveOnly(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	db.metaDocs = []couchdb.MetaDoc{{ID: "i:f:one", Path: "i:.codex/notes.md", Type: "plain"}}
	writeHidden(t, root, ".codex/notes.md", "local")
	s := syncsvc.New(db, crypto.New(""), root)
	s.SetSyncHidden(true)
	if err := s.Push(context.Background(), ".codex/"); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("internal push ran without OBGO_INTERNAL_PUSH")
	}
}
