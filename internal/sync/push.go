package sync

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/lib/livesync"
)

// Push reads files from OBGO_DATA and upserts them to CouchDB.
// filter is a vault-relative path: empty means all files, a path ending with "/"
// pushes that folder and its contents, otherwise pushes the single named file.
func (s *Service) Push(ctx context.Context, filter string) error {
	// 1. Ensure salt is set (create if needed) when E2EE is enabled.
	if s.crypto.Enabled() {
		if err := s.ensureSalt(ctx); err != nil {
			return fmt.Errorf("push: salt: %w", err)
		}
	}

	// 2. Pre-load remote docs to detect obfuscation mode and to reuse for reconcile.
	remoteDocs, err := s.db.AllMetaDocs(ctx)
	if reader, ok := s.db.(interface {
		AllPullMetaDocs(context.Context) ([]couchdb.MetaDoc, error)
	}); ok {
		remoteDocs, err = reader.AllPullMetaDocs(ctx)
	}
	if err != nil {
		return fmt.Errorf("push: list remote: %w", err)
	}
	s.detectObfuscation(remoteDocs)
	s.decryptDocPaths(remoteDocs)
	if err := s.rememberInternal(remoteDocs); err != nil {
		return err
	}

	// 3. Single-file shortcut.
	if filter != "" && !strings.HasSuffix(filter, "/") {
		if s.internalPushEnabled() && isHiddenRelPath(filter) {
			absPath := filepath.Join(s.dataDir, filepath.FromSlash(filter))
			if _, err := os.Lstat(absPath); err != nil {
				done, terr := s.tombstoneInternal(ctx, filter)
				if terr != nil {
					return fmt.Errorf("push: tombstone %q: %w", filter, terr)
				}
				if done && s.OnTombstone != nil {
					s.OnTombstone(filter)
				}
				return nil
			}
			if err := s.pushInternalFile(ctx, absPath, filter); err != nil {
				if errors.Is(err, errUnchanged) {
					return nil
				}
				return err
			}
			if s.OnPushFile != nil {
				s.OnPushFile(1)
			}
			return nil
		}
		owned, err := s.checkInternalOwner(ctx, filter)
		if err != nil {
			return err
		}
		if owned {
			return fmt.Errorf("%w: %s", ErrReceiveOnly, filter)
		}
		// A single named file is always evaluated as hidden-or-not on its own
		// merits (no filterRoot exemption) — naming one hidden file explicitly
		// does not opt it in.
		if shouldSkipLocalPath(filter, "", s.syncHidden) {
			return nil
		}
		absPath := filepath.Join(s.dataDir, filepath.FromSlash(filter))
		if _, err := os.Stat(absPath); err != nil {
			// File missing locally: tombstone the remote doc if it exists.
			docID := s.encodeDocID(filter)
			existing, gerr := s.db.GetMeta(ctx, docID)
			if gerr == nil && !existing.IsDeleted() {
				existing.Deleted = false
				existing.DeletedApp = true
				existing.Children = nil
				if _, perr := s.db.PutMeta(ctx, existing); perr != nil {
					return fmt.Errorf("push: tombstone %q: %w", filter, perr)
				}
				if s.OnTombstone != nil {
					s.OnTombstone(filter)
				}
			}
			return nil
		}
		if err := s.pushFile(ctx, absPath); err != nil {
			if errors.Is(err, errUnchanged) {
				return nil
			}
			return err
		}
		if s.OnPushFile != nil {
			s.OnPushFile(1)
		}
		return nil
	}

	// 4. Walk a directory (whole vault or a folder prefix).
	walkRoot := s.dataDir
	walkFilter := "" // vault-relative filter, trailing slash trimmed
	if filter != "" {
		walkRoot = filepath.Join(s.dataDir, filepath.FromSlash(filter))
		walkFilter = strings.TrimSuffix(filter, "/")
	}
	pushedPaths := make(map[string]struct{})
	var count int
	if err := filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Protected names (.git, .obgo*) are pruned even at walkRoot itself;
			// hidden-but-not-protected names are only pruned below walkRoot, so
			// an explicitly requested hidden folder (e.g. "obgo push .obsidian/")
			// still works without OBGO_SYNC_HIDDEN.
			if isProtectedLocalName(d.Name()) || (path != walkRoot && shouldSkipLocalName(d.Name(), s.syncHidden)) {
				return filepath.SkipDir
			}
			return nil
		}
		relPath, _ := filepath.Rel(s.dataDir, path)
		relPath = filepath.ToSlash(relPath)
		if s.internalOwns(relPath) && !s.internalPushEnabled() {
			return nil
		}
		if shouldSkipLocalPath(relPath, walkFilter, s.syncHidden) {
			return nil
		}
		if err := s.pushFile(ctx, path); err != nil {
			if errors.Is(err, errUnchanged) {
				// Still present locally: keep it out of the reconcile below,
				// which tombstones every remote note that was not seen here.
				pushedPaths[relPath] = struct{}{}
				return nil
			}
			return err
		}
		pushedPaths[relPath] = struct{}{}
		count++
		if s.OnPushFile != nil {
			s.OnPushFile(count)
		}
		return nil
	}); err != nil {
		return err
	}

	// 5. Reconcile: tombstone remote docs that no longer exist locally.
	// remoteDocs was already loaded and paths decrypted in step 2.
	for _, remote := range remoteDocs {
		if strings.HasPrefix(remote.ID, "i:") || s.internalOwns(remote.Path) {
			continue
		}
		if remote.IsDeleted() {
			continue // already tombstoned
		}
		if shouldSkipLocalPath(remote.Path, walkFilter, s.syncHidden) {
			continue
		}
		// Only reconcile paths inside the walked scope.
		if filter != "" && !strings.HasPrefix(remote.Path, filter) {
			continue
		}
		if _, ok := pushedPaths[remote.Path]; ok {
			continue // was just pushed — still exists
		}
		// Re-fetch the raw document before tombstoning. decryptDocPaths (step 2)
		// replaced remote.Path with the PLAINTEXT path in place, and PutMeta
		// serialises the struct verbatim — writing this copy back would replace
		// an obfuscated vault's encrypted path blob with the cleartext path,
		// leaking the path of every tombstoned document into the database.
		raw, gerr := s.db.GetMeta(ctx, remote.ID)
		if gerr != nil {
			fmt.Fprintf(os.Stderr, "push: tombstone %q: fetch: %v\n", remote.Path, gerr)
			continue
		}
		raw.Deleted = false
		raw.DeletedApp = true
		raw.Children = nil
		if _, err := s.db.PutMeta(ctx, raw); err != nil {
			fmt.Fprintf(os.Stderr, "push: tombstone %q: %v\n", remote.Path, err)
			continue
		}
		if s.OnTombstone != nil {
			s.OnTombstone(remote.Path)
		}
	}
	return nil
}

// encodeDocID returns the CouchDB document ID for the given vault-relative path,
// using an obfuscated (f: prefixed) ID when path obfuscation is active.
func (s *Service) encodeDocID(relPath string) string {
	if s.obfuscated {
		return s.crypto.ObfuscateDocID(relPath)
	}
	return livesync.EncodeDocID(relPath)
}

// ensureSalt loads the HKDF salt from CouchDB or generates and stores a new one.
func (s *Service) ensureSalt(ctx context.Context) error {
	params, err := s.db.GetLocal(ctx, "obsidian_livesync_sync_parameters")
	if err != nil && !errors.Is(err, couchdb.ErrNotFound) {
		return err
	}
	if params != nil {
		if saltB64, ok := params["pbkdf2salt"].(string); ok {
			saltBytes, decErr := base64.StdEncoding.DecodeString(saltB64)
			if decErr == nil {
				s.crypto.SetSalt(saltBytes)
				return nil
			}
		}
	}

	// Generate new random salt and persist it.
	salt := make([]byte, 32)
	if _, err := cryptorand.Read(salt); err != nil {
		return fmt.Errorf("ensureSalt: generate: %w", err)
	}
	s.crypto.SetSalt(salt)

	rev := ""
	if params != nil {
		if r, ok := params["_rev"].(string); ok {
			rev = r
		}
	}
	doc := map[string]interface{}{
		"pbkdf2salt": base64.StdEncoding.EncodeToString(salt),
	}
	if rev != "" {
		doc["_rev"] = rev
	}
	return s.db.PutLocal(ctx, "obsidian_livesync_sync_parameters", doc)
}

// pushFile reads a local file, splits it into chunks compatible with the
// Obsidian Livesync protocol, uploads them via BulkDocs, then upserts the
// meta document.
//
// Plain text files are split at DefaultTextChunkSize (1000) Unicode code
// points; each chunk's data field is the raw UTF-8 string.
//
// Binary files are base64-encoded as a whole, then split at 102400 characters;
// each chunk's data field is the base64 substring.
//
// Chunk IDs are computed with crypto.ChunkID, which uses xxHash-64 of
// "${data}-${charCount}" encoded as base36 — matching Obsidian's algorithm.
func (s *Service) pushFile(ctx context.Context, absPath string) error {
	relPath, err := filepath.Rel(s.dataDir, absPath)
	if err != nil {
		return err
	}
	relPath = filepath.ToSlash(relPath)
	// With internal push on, every hidden path belongs to an i: document and
	// is never uploaded as an ordinary one.
	if s.internalPushEnabled() && isHiddenRelPath(relPath) {
		return s.pushInternalFile(ctx, absPath, relPath)
	}

	// Taken before the read, so the document never claims a later time than
	// the content it describes.
	before, err := os.Stat(absPath)
	if err != nil {
		return fmt.Errorf("read %q: %w", absPath, err)
	}
	content, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("read %q: %w", absPath, err)
	}
	owned, err := s.checkInternalOwner(ctx, relPath)
	if err != nil {
		return err
	}
	if owned {
		return fmt.Errorf("%w: %s", ErrReceiveOnly, relPath)
	}

	// A note that still holds what it was last in sync with, and whose
	// document is still live, needs no request at all.
	docID := s.encodeDocID(relPath)
	if base, ok := s.syncedRecord(absPath); ok && s.noteIsLive(docID) && sha256.Sum256(content) == base.sum {
		return errUnchanged
	}

	// Determine doc type: plain (text) or newnote (binary).
	isText := utf8.Valid(content)
	docType := "newnote"
	if isText {
		docType = "plain"
	}

	// Build data chunks (the strings stored in each chunk doc's data field).
	//
	// Obsidian plain:   split UTF-8 text at 1000 code points
	// Obsidian newnote: base64-encode full file, split at 102400 chars
	chunkIDs, chunkDocs, err := s.encodeChunks(content, isText)
	if err != nil {
		return err
	}

	// The document describes what was read: its time is the one taken before
	// the read and its size is the size of the uploaded content.
	mtime := before.ModTime().UnixMilli()
	size := int64(len(content))

	// Fetch existing doc for rev and original ctime.
	existing, err := s.db.GetMeta(ctx, docID)
	var rev string
	var ctime int64 = mtime
	if err == nil {
		if s.holdsContent(ctx, *existing, content, chunkIDs) {
			s.markSynced(absPath, before.ModTime(), content)
			s.rememberNote(docID, relPath)
			return errUnchanged
		}
		rev = existing.Rev
		if existing.CTime > 0 {
			ctime = existing.CTime
		}
	} else if !errors.Is(err, couchdb.ErrNotFound) {
		return fmt.Errorf("get existing meta for %q: %w", relPath, err)
	}

	// Upload chunks.
	if err := s.db.BulkDocs(ctx, chunkDocs); err != nil {
		return fmt.Errorf("upload chunks for %q: %w", relPath, err)
	}

	// Build path field: encrypted blob for obfuscated vaults, plain path otherwise.
	pathField := relPath
	if s.obfuscated {
		pathField, err = s.crypto.EncryptPath(relPath, ctime, mtime, size, chunkIDs)
		if err != nil {
			return fmt.Errorf("encrypt path for %q: %w", relPath, err)
		}
	}

	meta := &couchdb.MetaDoc{
		ID:        docID,
		Rev:       rev,
		Type:      docType,
		Path:      pathField,
		CTime:     ctime,
		MTime:     mtime,
		Size:      size,
		Children:  chunkIDs,
		Eden:      map[string]interface{}{},
		Encrypted: s.crypto.Enabled(),
	}
	if _, err := s.putOwn(ctx, meta); err != nil {
		return fmt.Errorf("put meta %q: %w", relPath, err)
	}
	s.markSynced(absPath, before.ModTime(), content)
	s.rememberNote(docID, relPath)
	return nil
}

// holdsContent reports whether a remote note already stores exactly this
// content, so uploading it again would only add a revision. A pulled note
// raises a write event of its own; once the watcher runs behind, that event
// outlives the echo suppression and the note used to be uploaded straight
// back, which the other devices then pulled and echoed in turn.
func (s *Service) holdsContent(ctx context.Context, doc couchdb.MetaDoc, content []byte, chunkIDs []string) bool {
	if doc.IsDeleted() || len(doc.Conflicts) > 0 || len(doc.Eden) > 0 {
		return false
	}
	if err := s.decodeMetaBlob(&doc); err != nil {
		return false
	}
	if doc.Size != int64(len(content)) {
		return false
	}
	if slices.Equal(doc.Children, chunkIDs) {
		return true
	}
	// Another client may have split the same content into different chunks.
	remote, err := s.readRemoteContent(ctx, doc)
	return err == nil && bytes.Equal(remote, content)
}
