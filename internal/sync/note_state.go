package sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/lib/livesync"
)

// putOwn writes a document and records the new revision as this process's
// own. The lock is held across the request: the change feed can report the
// write before its response has been read, and isOwnRevision must not look
// until the revision is on record.
func (s *Service) putOwn(ctx context.Context, doc *couchdb.MetaDoc) (string, error) {
	s.ownMu.Lock()
	defer s.ownMu.Unlock()
	rev, err := s.db.PutMeta(ctx, doc)
	if err == nil && rev != "" {
		if s.ownRevs == nil {
			s.ownRevs = make(map[string]string)
		}
		s.ownRevs[doc.ID] = rev
	}
	return rev, err
}

// revGeneration returns the generation number of a CouchDB revision, or 0.
func revGeneration(rev string) int {
	gen, _, _ := strings.Cut(rev, "-")
	n, err := strconv.Atoi(gen)
	if err != nil {
		return 0
	}
	return n
}

// isOwnRevision reports whether a change event describes a revision this
// process wrote itself, or one older than its last write to that document.
// Such an event needs no applying, and writing it to disk could replace a file
// that was edited again in the meantime.
func (s *Service) isOwnRevision(event couchdb.ChangeEvent) bool {
	s.ownMu.Lock()
	defer s.ownMu.Unlock()
	own, ok := s.ownRevs[event.ID]
	if !ok {
		return false
	}
	if event.Doc != nil && event.Doc.Rev != "" {
		if event.Doc.Rev == own {
			return true
		}
		gen := revGeneration(event.Doc.Rev)
		return gen > 0 && gen < revGeneration(own)
	}
	for _, change := range event.Changes {
		if change.Rev == own {
			return true
		}
	}
	return false
}

// noteRef names a live note document and the path it stores.
type noteRef struct {
	id   string
	path string
}

// seedNotePaths fills the note path index from a full document listing.
func (s *Service) seedNotePaths(docs []couchdb.MetaDoc) {
	paths := make(map[string]string, len(docs))
	for _, doc := range docs {
		if doc.IsDeleted() || strings.HasPrefix(doc.ID, "i:") {
			continue
		}
		if doc.Path == "" || isMetaBlob(doc.Path) || livesync.IsObfuscatedDocID(doc.Path) {
			continue
		}
		paths[doc.ID] = doc.Path
	}
	s.notesMu.Lock()
	s.notePaths = paths
	s.notesMu.Unlock()
}

// rememberNote and forgetNote keep the index current as notes are pushed,
// pulled and deleted. Both do nothing until the index has been loaded.
func (s *Service) rememberNote(id, path string) {
	s.notesMu.Lock()
	if s.notePaths != nil {
		s.notePaths[id] = path
	}
	s.notesMu.Unlock()
}

func (s *Service) forgetNote(id string) {
	s.notesMu.Lock()
	if s.notePaths != nil {
		delete(s.notePaths, id)
	}
	s.notesMu.Unlock()
}

// noteIsLive reports whether the index holds a live document with this ID.
// It is false while the index is not loaded.
func (s *Service) noteIsLive(id string) bool {
	s.notesMu.Lock()
	defer s.notesMu.Unlock()
	_, ok := s.notePaths[id]
	return ok
}

// notesUnder returns the live notes whose path lies under prefix. The index is
// seeded by pull; without one it is built once from a full listing, which on a
// large vault takes minutes and used to be repeated for every removed folder.
func (s *Service) notesUnder(ctx context.Context, prefix string) ([]noteRef, error) {
	s.notesMu.Lock()
	loaded := s.notePaths != nil
	s.notesMu.Unlock()
	if !loaded {
		docs, err := s.db.AllMetaDocs(ctx)
		if err != nil {
			return nil, err
		}
		s.decryptDocPaths(docs)
		s.seedNotePaths(docs)
	}
	s.notesMu.Lock()
	defer s.notesMu.Unlock()
	var refs []noteRef
	for id, path := range s.notePaths {
		if strings.HasPrefix(path, prefix) {
			refs = append(refs, noteRef{id: id, path: path})
		}
	}
	return refs, nil
}

// syncedNote is what a note looked like on disk when it last matched its
// document.
type syncedNote struct {
	mtime time.Time
	sum   [sha256.Size]byte
}

// markSynced records a file's modification time and content while they match
// the remote document, so a later change on disk can be told apart from a file
// that was merely pulled, and a remote revision that only repeats that content
// can be told apart from a remote edit.
func (s *Service) markSynced(absPath string, mtime time.Time, content []byte) {
	s.syncedMu.Lock()
	if s.synced == nil {
		s.synced = make(map[string]syncedNote)
	}
	s.synced[absPath] = syncedNote{mtime: mtime, sum: sha256.Sum256(content)}
	s.syncedMu.Unlock()
}

func (s *Service) forgetSynced(absPath string) {
	s.syncedMu.Lock()
	delete(s.synced, absPath)
	s.syncedMu.Unlock()
}

func (s *Service) syncedRecord(absPath string) (syncedNote, bool) {
	s.syncedMu.Lock()
	defer s.syncedMu.Unlock()
	note, ok := s.synced[absPath]
	return note, ok
}

// alignNoteMTime gives a pulled note the modification time of its document
// and records the file as in sync.
//
// A file whose time already matches to the millisecond is left untouched, so
// an editor or agent that has it open does not see it change for no reason.
func (s *Service) alignNoteMTime(absPath string, doc couchdb.MetaDoc, content []byte) {
	info, err := os.Lstat(absPath)
	if err != nil {
		return
	}
	if doc.MTime > 0 && info.ModTime().UnixMilli() != doc.MTime {
		mtime := time.UnixMilli(doc.MTime)
		if os.Chtimes(absPath, mtime, mtime) == nil {
			if info, err = os.Lstat(absPath); err != nil {
				return
			}
		}
	}
	s.markSynced(absPath, info.ModTime(), content)
}

// keepLocalNote decides whether a remote note may replace or delete an
// existing local file. It reports true when the file must be left alone.
//
// The file is compared with what it held when it last matched its document:
//
//   - it already holds the remote content: nothing to do;
//   - it is unchanged since then: the remote version is applied;
//   - it changed, and the remote revision only repeats the content it was last
//     in sync with (another client saved the note again without editing it):
//     the file is kept and uploaded, whatever the times say;
//   - both sides changed, or nothing is on record because obgo was not
//     running: the newer modification time wins, and a newer file is uploaded.
//
// A remote deletion only yields to a file on record as changed, because
// tombstones carry no reliable time. content is the remote content, ignored
// for a deletion.
func (s *Service) keepLocalNote(ctx context.Context, absPath string, doc couchdb.MetaDoc, content []byte) (bool, error) {
	info, err := os.Lstat(absPath)
	if err != nil || !info.Mode().IsRegular() {
		return false, nil
	}
	local, err := os.ReadFile(absPath)
	if err != nil {
		return false, nil
	}
	deleted := doc.IsDeleted()
	if !deleted && bytes.Equal(local, content) {
		// A time later than the document's would make this file look edited
		// at the next start. An earlier one is harmless and is left alone.
		if info.ModTime().UnixMilli() > doc.MTime {
			s.alignNoteMTime(absPath, doc, content)
		} else {
			s.markSynced(absPath, info.ModTime(), content)
		}
		return true, nil
	}
	base, known := s.syncedRecord(absPath)
	switch {
	case known && sha256.Sum256(local) == base.sum:
		return false, nil
	case deleted:
		if !known {
			return false, nil
		}
	case known && sha256.Sum256(content) == base.sum:
		// Only the local side changed.
	default:
		if doc.MTime <= 0 || info.ModTime().UnixMilli() <= doc.MTime {
			return false, nil
		}
	}
	if err := s.pushFile(ctx, absPath); err != nil && !errors.Is(err, errUnchanged) {
		return false, fmt.Errorf("upload newer local %q: %w", doc.Path, err)
	}
	if s.OnWatchEvent != nil {
		s.OnWatchEvent(doc.Path, true)
	}
	return true, nil
}
