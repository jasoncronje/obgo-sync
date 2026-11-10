package sync

import (
	"bytes"
	"context"
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

// markSynced records the modification time a file had while its content
// matched the remote document, so a later change on disk can be told apart
// from a file that was merely pulled.
func (s *Service) markSynced(absPath string, mtime time.Time) {
	s.syncedMu.Lock()
	if s.syncedMTimes == nil {
		s.syncedMTimes = make(map[string]time.Time)
	}
	s.syncedMTimes[absPath] = mtime
	s.syncedMu.Unlock()
}

func (s *Service) forgetSynced(absPath string) {
	s.syncedMu.Lock()
	delete(s.syncedMTimes, absPath)
	s.syncedMu.Unlock()
}

// changedSinceSync reports whether a file's modification time differs from
// the one on record, and whether there is a record at all.
func (s *Service) changedSinceSync(absPath string, info os.FileInfo) (changed, known bool) {
	s.syncedMu.Lock()
	defer s.syncedMu.Unlock()
	mtime, ok := s.syncedMTimes[absPath]
	if !ok {
		return false, false
	}
	return !info.ModTime().Equal(mtime), true
}

// alignNoteMTime gives a pulled note the modification time of its document
// and records the file as in sync.
//
// A file whose time already matches to the millisecond is left untouched, so
// an editor or agent that has it open does not see it change for no reason.
func (s *Service) alignNoteMTime(absPath string, doc couchdb.MetaDoc) {
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
	s.markSynced(absPath, info.ModTime())
}

// keepLocalNote decides whether a remote note may replace or delete an
// existing local file. It reports true when the file must be left alone:
//
//   - it already holds the remote content;
//   - it changed on disk since it was last in sync and is newer than the
//     remote version, in which case it is uploaded instead;
//   - nothing is on record for it (obgo was not running when it last changed)
//     and it is newer than the remote version, with the same result.
//
// A remote deletion only yields to a file that is known to have changed since
// it was last in sync, because tombstones carry no reliable time. content is
// the remote content, ignored for a deletion.
func (s *Service) keepLocalNote(ctx context.Context, absPath string, doc couchdb.MetaDoc, content []byte) (bool, error) {
	info, err := os.Lstat(absPath)
	if err != nil || !info.Mode().IsRegular() {
		return false, nil
	}
	changed, known := s.changedSinceSync(absPath, info)
	if doc.IsDeleted() {
		if !known || !changed {
			return false, nil
		}
	} else {
		if local, rerr := os.ReadFile(absPath); rerr == nil && bytes.Equal(local, content) {
			s.alignNoteMTime(absPath, doc)
			return true, nil
		}
		if known && !changed {
			return false, nil
		}
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
