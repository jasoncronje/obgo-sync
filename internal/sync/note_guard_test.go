package sync_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/crypto"
	syncsvc "github.com/jookos/obgo-sync/internal/sync"
)

// remoteNote makes rel the only note on the mock server.
func remoteNote(db *mockClient, rel, content string, mtime int64) {
	chunk := "h:" + rel
	db.chunkDocs[chunk] = couchdb.ChunkDoc{ID: chunk, Data: content}
	db.metaDocs = []couchdb.MetaDoc{{ID: rel, Rev: "1-aaa", Type: "plain", Path: rel, MTime: mtime, Size: int64(len(content)), Children: []string{chunk}}}
}

func readNote(t *testing.T, root, rel string) string {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func noteMTime(t *testing.T, root, rel string) time.Time {
	t.Helper()
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatal(err)
	}
	return info.ModTime()
}

// editNote rewrites a note and gives it a modification time later than before.
func editNote(t *testing.T, root, rel, content string, mtime time.Time) {
	t.Helper()
	writeHidden(t, root, rel, content)
	if err := os.Chtimes(filepath.Join(root, filepath.FromSlash(rel)), mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// pushedNote uploads a note and returns the service, its document ID and the
// modification time the upload recorded.
func pushedNote(t *testing.T, db couchdb.Client, mock *mockClient, root, rel, content string) (*syncsvc.Service, string, time.Time) {
	t.Helper()
	writeHidden(t, root, rel, content)
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Push(context.Background(), rel); err != nil {
		t.Fatal(err)
	}
	if len(mock.putMetaDocs) != 1 {
		t.Fatalf("push wrote %d documents, want 1", len(mock.putMetaDocs))
	}
	return s, mock.putMetaDocs[0].ID, noteMTime(t, root, rel)
}

func watchRemote(t *testing.T, s *syncsvc.Service, db *internalEventClient, events ...couchdb.ChangeEvent) {
	t.Helper()
	db.events = events
	if err := s.Watch(context.Background(), false, true); err != nil {
		t.Fatal(err)
	}
}

func TestPullKeepsNoteEditedWhileStopped(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := "notes/plan.md"
	remoteNote(db, rel, "remote", time.Now().Add(-time.Hour).UnixMilli())
	writeHidden(t, root, rel, "edited while obgo was stopped")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if got := readNote(t, root, rel); got != "edited while obgo was stopped" {
		t.Fatalf("pull replaced a newer local note: %q", got)
	}
	if !db.putMetaCalled {
		t.Fatal("the newer local note was not uploaded")
	}
}

func TestPullAppliesNewerRemoteNote(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := "notes/plan.md"
	mtime := time.Now().Add(time.Hour).UnixMilli()
	remoteNote(db, rel, "remote", mtime)
	writeHidden(t, root, rel, "stale")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if got := readNote(t, root, rel); got != "remote" {
		t.Fatalf("pull kept an older local note: %q", got)
	}
	if got := noteMTime(t, root, rel).UnixMilli(); got != mtime {
		t.Fatalf("pulled note has mtime %d, want the document's %d", got, mtime)
	}
	if db.putMetaCalled {
		t.Fatal("pull uploaded although the remote note was newer")
	}
}

func TestPullLeavesMatchingNoteAlone(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	rel := "notes/plan.md"
	mtime := time.Now().Add(-time.Hour).UnixMilli()
	remoteNote(db, rel, "same", mtime)
	writeHidden(t, root, rel, "same")
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Pull(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if db.putMetaCalled || db.bulkDocsCalled {
		t.Fatal("a note that matches the remote produced remote writes")
	}
	if got := noteMTime(t, root, rel).UnixMilli(); got != mtime {
		t.Fatalf("matching note has mtime %d, want the document's %d", got, mtime)
	}
}

// The upload of a note is reported back by the change feed. If the note was
// edited in between, the report must not put the uploaded content back.
func TestWatchKeepsNoteChangedAfterItsUpload(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	rel := "notes/plan.md"
	s, id, uploaded := pushedNote(t, db, db.mockClient, root, rel, "as uploaded")
	editNote(t, root, rel, "edited seconds later", uploaded.Add(5*time.Second))

	// A revision that is not on record as this process's own, carrying the
	// uploaded content and time.
	db.chunkDocs["h:up"] = couchdb.ChunkDoc{ID: "h:up", Data: "as uploaded"}
	report := couchdb.MetaDoc{ID: id, Rev: "9-other", Type: "plain", Path: rel, MTime: uploaded.UnixMilli(), Size: 11, Children: []string{"h:up"}}
	watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Doc: &report})

	if got := readNote(t, root, rel); got != "edited seconds later" {
		t.Fatalf("a remote revision replaced a note changed after its upload: %q", got)
	}
	if len(db.putMetaDocs) != 2 {
		t.Fatalf("the newer local note was not uploaded: %d writes", len(db.putMetaDocs))
	}
}

// A file that has not changed since it was last in sync takes the remote
// version whatever the times say, for example a note restored from a backup
// on another device.
func TestWatchAppliesRemoteNoteOverUntouchedFile(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	rel := "notes/plan.md"
	s, id, _ := pushedNote(t, db, db.mockClient, root, rel, "current")

	db.chunkDocs["h:old"] = couchdb.ChunkDoc{ID: "h:old", Data: "restored"}
	restored := couchdb.MetaDoc{ID: id, Rev: "2-def", Type: "plain", Path: rel, MTime: 1000, Size: 8, Children: []string{"h:old"}}
	watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Doc: &restored})

	if got := readNote(t, root, rel); got != "restored" {
		t.Fatalf("remote revision was not applied to an untouched note: %q", got)
	}
}

func TestWatchSkipsRevisionOlderThanOwnWrite(t *testing.T) {
	root := t.TempDir()
	db := &internalEventClient{mockClient: newMockClient()}
	db.putMetaRev = "3-own"
	rel := "notes/plan.md"
	s, id, _ := pushedNote(t, db, db.mockClient, root, rel, "third revision")

	// An earlier revision arrives late and claims to be newer than anything.
	db.chunkDocs["h:two"] = couchdb.ChunkDoc{ID: "h:two", Data: "second revision"}
	late := couchdb.MetaDoc{ID: id, Rev: "2-old", Type: "plain", Path: rel, MTime: time.Now().Add(time.Hour).UnixMilli(), Size: 15, Children: []string{"h:two"}}
	watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Doc: &late})
	if got := readNote(t, root, rel); got != "third revision" {
		t.Fatalf("a revision older than this process's own write was applied: %q", got)
	}

	// A later revision from another device is applied.
	db.chunkDocs["h:four"] = couchdb.ChunkDoc{ID: "h:four", Data: "fourth revision"}
	next := couchdb.MetaDoc{ID: id, Rev: "4-new", Type: "plain", Path: rel, Size: 15, Children: []string{"h:four"}}
	watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Doc: &next})
	if got := readNote(t, root, rel); got != "fourth revision" {
		t.Fatalf("a later remote revision was not applied: %q", got)
	}
}

func TestWatchRemoteDeletionSparesNoteChangedSinceSync(t *testing.T) {
	for _, hard := range []bool{false, true} {
		root := t.TempDir()
		db := &internalEventClient{mockClient: newMockClient()}
		rel := "notes/plan.md"
		s, id, uploaded := pushedNote(t, db, db.mockClient, root, rel, "as uploaded")
		editNote(t, root, rel, "edited after upload", uploaded.Add(5*time.Second))

		gone := couchdb.MetaDoc{ID: id, Rev: "9-other", Type: "plain", Path: rel, DeletedApp: !hard, Deleted: hard}
		watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Deleted: hard, Doc: &gone})

		if got := readNote(t, root, rel); got != "edited after upload" {
			t.Fatalf("hard=%v: note changed after its upload was lost: %q", hard, got)
		}
		if len(db.putMetaDocs) != 2 {
			t.Fatalf("hard=%v: the changed note was not uploaded again: %d writes", hard, len(db.putMetaDocs))
		}
	}
}

func TestWatchRemoteDeletionRemovesUntouchedNote(t *testing.T) {
	for _, hard := range []bool{false, true} {
		root := t.TempDir()
		db := &internalEventClient{mockClient: newMockClient()}
		rel := "notes/plan.md"
		s, id, _ := pushedNote(t, db, db.mockClient, root, rel, "as uploaded")

		gone := couchdb.MetaDoc{ID: id, Rev: "9-other", Type: "plain", Path: rel, DeletedApp: !hard, Deleted: hard}
		watchRemote(t, s, db, couchdb.ChangeEvent{ID: id, Deleted: hard, Doc: &gone})

		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); !os.IsNotExist(err) {
			t.Fatalf("hard=%v: an untouched note survived its remote deletion", hard)
		}
	}
}

// Removing several folders must not list the whole database once per folder.
func TestWatch_DirectoryRemovalsListRemoteOnce(t *testing.T) {
	root := t.TempDir()
	db := newMockClient()
	paths := []string{"lit/a.md", "lit/sub/b.md", "old/c.md", "keep.md"}
	for _, p := range paths {
		writeHidden(t, root, p, "body of "+p)
	}
	s := syncsvc.New(db, crypto.New(""), root)
	if err := s.Push(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]string, len(paths))
	for _, doc := range db.putMetaDocs {
		stored := *doc
		stored.Rev = "1-abc"
		db.metaDocs = append(db.metaDocs, stored)
		ids[stored.Path] = stored.ID
	}
	db.putMetaDocs = nil
	listings := db.allMetaCalls

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx, true, false) }()
	time.Sleep(200 * time.Millisecond)

	outside := t.TempDir()
	for _, dir := range []string{"lit", "old"} {
		if err := os.Rename(filepath.Join(root, dir), filepath.Join(outside, dir)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-done

	tombstoned := make(map[string]bool)
	for _, doc := range db.putMetaDocs {
		if doc.DeletedApp {
			tombstoned[doc.ID] = true
		}
	}
	for _, p := range []string{"lit/a.md", "lit/sub/b.md", "old/c.md"} {
		if !tombstoned[ids[p]] {
			t.Errorf("%s was not tombstoned", p)
		}
	}
	if tombstoned[ids["keep.md"]] {
		t.Error("keep.md was tombstoned")
	}
	if got := db.allMetaCalls - listings; got > 1 {
		t.Errorf("two folder removals listed the remote %d times, want at most 1", got)
	}
}
