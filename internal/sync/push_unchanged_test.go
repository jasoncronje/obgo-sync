package sync_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/crypto"
	syncsvc "github.com/jookos/obgo-sync/internal/sync"
)

func TestPushSkipsUnchangedNote(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := "notes/plan.md"
	writeHidden(t, root, rel, "same text")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	if len(db.putMetaDocs) != 1 {
		t.Fatalf("first push wrote %d documents, want 1", len(db.putMetaDocs))
	}
	stored := *db.putMetaDocs[0]
	stored.Rev = "1-abc"
	db.metaDocs = []couchdb.MetaDoc{stored}

	// Same content again: by file, by folder and for the whole vault. The last
	// two also reconcile, which must not tombstone the skipped note.
	db.putMetaCalled, db.bulkDocsCalled = false, false
	for _, target := range []string{rel, "notes/", ""} {
		if err := s.Push(context.Background(), target); err != nil {
			t.Fatalf("push %q: %v", target, err)
		}
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("unchanged note produced remote writes")
	}

	// The same content split into other chunks by another client still counts.
	db.chunkDocs["h:a"] = couchdb.ChunkDoc{ID: "h:a", Data: "same "}
	db.chunkDocs["h:b"] = couchdb.ChunkDoc{ID: "h:b", Data: "text"}
	db.metaDocs[0].Children = []string{"h:a", "h:b"}
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("note with matching remote content produced remote writes")
	}

	// A real edit is uploaded.
	writeHidden(t, root, rel, "new text!")
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	if !db.putMetaCalled {
		t.Fatal("edited note was not uploaded")
	}
}

func TestPushUploadsNoteOverTombstone(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := "notes/plan.md"
	writeHidden(t, root, rel, "")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	stored := *db.putMetaDocs[0]
	stored.Rev = "1-abc"
	stored.DeletedApp = true
	db.metaDocs = []couchdb.MetaDoc{stored}
	db.putMetaCalled = false
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	if !db.putMetaCalled {
		t.Fatal("a note recreated after its deletion was not uploaded")
	}
}

func TestWatchSkipsOwnRevisionOfNote(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	rel := "notes/plan.md"
	writeHidden(t, root, rel, "pushed")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	id := db.putMetaDocs[0].ID
	// The note is edited again before the change feed reports the push.
	writeHidden(t, root, rel, "edited again")
	db.chunkDocs["h:echo"] = couchdb.ChunkDoc{ID: "h:echo", Data: "pushed"}
	echo := couchdb.MetaDoc{ID: id, Rev: "1-abc", Type: "plain", Path: rel, Size: 6, Children: []string{"h:echo"}}
	db.events = []couchdb.ChangeEvent{{ID: id, Doc: &echo}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "edited again" {
		t.Fatalf("own revision was applied over a newer local edit: %q", got)
	}

	// A revision written by another device is still applied.
	db.chunkDocs["h:other"] = couchdb.ChunkDoc{ID: "h:other", Data: "from elsewhere"}
	other := couchdb.MetaDoc{ID: id, Rev: "2-def", Type: "plain", Path: rel, Size: 14, Children: []string{"h:other"}}
	db.events = []couchdb.ChangeEvent{{ID: id, Doc: &other}}
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, rel)); string(got) != "from elsewhere" {
		t.Fatalf("remote revision was not applied: %q", got)
	}
}
