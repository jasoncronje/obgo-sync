package sync

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/lib/livesync"
)

// errUnchanged reports that a file already matches its remote document, so
// nothing was uploaded. Callers treat it as a silent no-op.
var errUnchanged = errors.New("file unchanged")

// plainTextExtensions mirrors LiveSync's isPlainText: hidden files with these
// extensions are stored as "plain" documents, everything else as "newnote".
var plainTextExtensions = []string{".md", ".txt", ".svg", ".html", ".csv", ".css", ".js", ".xml", ".canvas"}

// SetInternalPush lets local changes to hidden paths be uploaded as LiveSync
// i: documents. It only takes effect together with SetSyncHidden(true).
func (s *Service) SetInternalPush(internalPush bool) {
	s.internalPush = internalPush
}

func (s *Service) internalPushEnabled() bool {
	return s.internalPush && s.syncHidden
}

// isHiddenRelPath reports whether any component of a vault-relative path is
// dot-prefixed, i.e. whether LiveSync would handle it as a hidden file.
func isHiddenRelPath(path string) bool {
	for _, part := range splitPathParts(path) {
		if isHiddenLocalName(part) {
			return true
		}
	}
	return false
}

// isInternalPushIgnored mirrors LiveSync's stock hidden-file ignore list.
// LiveSync never applies these paths, so uploading them is wasted work, and
// its own plugin folder holds per-device settings that must not travel.
func isInternalPushIgnored(relPath string) bool {
	parts := splitPathParts(relPath)
	// LiveSync only scans dot-prefixed entries at the vault root. A hidden name
	// inside an ordinary folder (an exporter's temp file, a .keep) never reaches
	// other devices through it, so it is not uploaded or tombstoned either.
	if len(parts) > 0 && !isHiddenLocalName(parts[0]) {
		return true
	}
	for _, part := range parts {
		switch part {
		case ".DS_Store", ".trash", "node_modules", "obsidian-livesync":
			return true
		}
	}
	return shouldSkipLocalPath(relPath, "", true)
}

// mtimeResolution is the step, in milliseconds, in which modification times
// are compared. LiveSync uses the same two seconds.
const mtimeResolution = 2000

// alignInternalMTime gives a hidden file the modification time of the document
// it now holds, so a later local edit is recognisable as newer.
func alignInternalMTime(absPath string, doc couchdb.MetaDoc) {
	if doc.MTime <= 0 {
		return
	}
	mtime := time.UnixMilli(doc.MTime)
	_ = os.Chtimes(absPath, mtime, mtime)
}

// keepLocalInternal decides whether a remote i: document may replace or delete
// an existing hidden file on a device that also uploads hidden files. It
// reports true when the file must be left alone: it already holds the remote
// content, or it was modified after the remote version (typically while obgo
// was stopped or still pulling) and is uploaded instead. content is the remote
// content, ignored when the document is a deletion.
func (s *Service) keepLocalInternal(ctx context.Context, absPath string, doc couchdb.MetaDoc, content []byte) (bool, error) {
	if !s.internalPushEnabled() || isInternalPushIgnored(doc.Path) {
		return false, nil
	}
	info, err := os.Lstat(absPath)
	if err != nil || !info.Mode().IsRegular() {
		return false, nil
	}
	if !doc.IsDeleted() {
		if local, err := os.ReadFile(absPath); err == nil && bytes.Equal(local, content) {
			alignInternalMTime(absPath, doc)
			return true, nil
		}
	}
	if doc.MTime <= 0 || info.ModTime().UnixMilli()/mtimeResolution <= doc.MTime/mtimeResolution {
		return false, nil
	}
	if err := s.pushInternalFile(ctx, absPath, doc.Path); err != nil && !errors.Is(err, errUnchanged) {
		return false, fmt.Errorf("upload newer local %q: %w", doc.Path, err)
	}
	if s.OnWatchEvent != nil {
		s.OnWatchEvent(doc.Path, true)
	}
	return true, nil
}

func internalDocType(path string, content []byte) string {
	if !utf8.Valid(content) {
		return "newnote"
	}
	for _, ext := range plainTextExtensions {
		if strings.HasSuffix(path, ext) {
			return "plain"
		}
	}
	return "newnote"
}

// internalDocID returns the i: document that owns relPath. An ID learned from
// the remote wins, so an existing document is updated rather than duplicated.
func (s *Service) internalDocID(relPath string) string {
	want := "i:" + livesync.EncodeDocID(relPath)
	if s.obfuscated {
		want = "i:" + s.crypto.ObfuscateDocID("i:"+relPath)
	}
	lower := strings.ToLower(relPath)
	found := ""
	s.internalPaths.Range(func(id, path interface{}) bool {
		if strings.ToLower(path.(string)) != lower {
			return true
		}
		found = id.(string)
		return found != want
	})
	if found != "" {
		return found
	}
	return want
}

// decodeMetaBlob restores the metadata LiveSync keeps inside an obfuscated
// path field.
func (s *Service) decodeMetaBlob(doc *couchdb.MetaDoc) error {
	if !isMetaBlob(doc.Path) {
		return nil
	}
	blob, err := s.crypto.DecryptPathBlob(doc.Path)
	if err != nil {
		return fmt.Errorf("decrypt path: %w", err)
	}
	if blob == nil {
		return nil
	}
	doc.Path = blob.Path
	if len(blob.Children) > 0 {
		doc.Children = blob.Children
	}
	if blob.MTime > 0 {
		doc.MTime = blob.MTime
	}
	if blob.CTime > 0 {
		doc.CTime = blob.CTime
	}
	if blob.Size > 0 {
		doc.Size = blob.Size
	}
	return nil
}

// isMetaBlob reports whether a path field holds encrypted metadata. LiveSync
// only encrypts the metadata of f: documents; i: documents keep a readable
// path, mtime, size and children even when path obfuscation is on.
func isMetaBlob(path string) bool {
	return strings.HasPrefix(path, "/\\:")
}

// pushInternalFile uploads a hidden file as a LiveSync i: document. It returns
// errUnchanged when the remote document already holds this content,
// which is what stops a pulled file from being echoed straight back.
func (s *Service) pushInternalFile(ctx context.Context, absPath, relPath string) error {
	if err := s.loadInternalState(); err != nil {
		return err
	}
	if isInternalPushIgnored(relPath) {
		return errUnchanged
	}
	if err := validateInternalPath(s.dataDir, relPath); err != nil {
		if errors.Is(err, errInternalSymlink) {
			// Local aliases are never uploaded; their canonical files sync separately.
			return errUnchanged
		}
		return err
	}
	content, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("read %q: %w", absPath, err)
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return err
	}
	mtime := info.ModTime().UnixMilli()
	size := int64(len(content))

	docID := s.internalDocID(relPath)
	existing, err := s.db.GetMeta(ctx, docID)
	if err != nil && !errors.Is(err, couchdb.ErrNotFound) {
		return fmt.Errorf("get existing meta for %q: %w", relPath, err)
	}
	var rev string
	var ctime int64
	if err == nil && existing != nil {
		if len(existing.Conflicts) > 0 {
			return fmt.Errorf("internal file %q has unresolved conflicts", relPath)
		}
		current := *existing
		if err := s.decodeMetaBlob(&current); err != nil {
			return fmt.Errorf("internal file %q: %w", relPath, err)
		}
		// A document stored with encrypted metadata is unreadable to LiveSync,
		// so it is rewritten even when the content matches.
		if !isMetaBlob(existing.Path) && !current.IsDeleted() && current.Size == size && len(current.Eden) == 0 {
			if remote, rerr := s.readRemoteContent(ctx, current); rerr == nil && bytes.Equal(remote, content) {
				return errUnchanged
			}
		}
		rev = existing.Rev
		ctime = current.CTime
	}

	docType := internalDocType(relPath, content)
	chunkIDs, chunkDocs, err := s.encodeChunks(content, docType == "plain")
	if err != nil {
		return err
	}
	if len(chunkDocs) > 0 {
		if err := s.db.BulkDocs(ctx, chunkDocs); err != nil {
			return fmt.Errorf("upload chunks for %q: %w", relPath, err)
		}
	}
	meta := &couchdb.MetaDoc{
		ID:       docID,
		Rev:      rev,
		Type:     docType,
		Path:     "i:" + relPath,
		CTime:    ctime,
		MTime:    mtime,
		Size:     size,
		Children: chunkIDs,
		Eden:     map[string]interface{}{},
	}
	if _, err := s.putOwn(ctx, meta); err != nil {
		return fmt.Errorf("put meta %q: %w", relPath, err)
	}
	return s.rememberInternal([]couchdb.MetaDoc{{ID: docID, Path: "i:" + relPath}})
}

// tombstoneInternal marks the i: document for relPath as deleted, the way
// LiveSync does: an app-level flag with a fresh mtime and no content. It
// reports whether a live document was tombstoned.
func (s *Service) tombstoneInternal(ctx context.Context, relPath string) (bool, error) {
	if err := s.loadInternalState(); err != nil {
		return false, err
	}
	if isInternalPushIgnored(relPath) {
		return false, nil
	}
	// A remove event can outlive the file's absence (atomic saves); only a file
	// that is really gone may be deleted for every other device. Document IDs
	// ignore case, so a sibling that differs only in case counts as present.
	if _, err := os.Lstat(resolveCase(s.dataDir, relPath)); err == nil {
		return false, nil
	}
	docID := s.internalDocID(relPath)
	existing, err := s.db.GetMeta(ctx, docID)
	if errors.Is(err, couchdb.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if existing.IsDeleted() {
		return false, nil
	}
	if len(existing.Conflicts) > 0 {
		return false, fmt.Errorf("internal file %q has unresolved conflicts", relPath)
	}
	current := *existing
	if err := s.decodeMetaBlob(&current); err != nil {
		return false, fmt.Errorf("internal file %q: %w", relPath, err)
	}
	existing.Path = current.Path
	existing.CTime = current.CTime
	existing.Encrypted = false
	existing.Deleted = false
	existing.DeletedApp = true
	existing.Children = []string{}
	existing.Eden = map[string]interface{}{}
	existing.Size = 0
	existing.MTime = time.Now().UnixMilli()
	if _, err := s.putOwn(ctx, existing); err != nil {
		return false, err
	}
	return true, nil
}

// tombstoneInternalTree handles the removal of a hidden file or directory. The
// kernel reports one event for a removed directory and none for its children,
// so every owned path beneath it is tombstoned too.
func (s *Service) tombstoneInternalTree(ctx context.Context, relPath string) error {
	if err := s.loadInternalState(); err != nil {
		return err
	}
	paths := []string{relPath}
	prefix := strings.ToLower(relPath) + "/"
	s.internalPaths.Range(func(_, path interface{}) bool {
		if strings.HasPrefix(strings.ToLower(path.(string)), prefix) {
			paths = append(paths, path.(string))
		}
		return true
	})
	var firstErr error
	for _, path := range paths {
		done, err := s.tombstoneInternal(ctx, path)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("tombstone %q: %w", path, err)
			}
			continue
		}
		if done && s.OnTombstone != nil {
			s.OnTombstone(path)
		}
	}
	return firstErr
}

// encodeChunks splits content into LiveSync chunk documents: text at
// DefaultTextChunkSize code points, binary as base64 in DefaultChunkSize
// pieces. It returns the chunk IDs in order and the documents to upload.
func (s *Service) encodeChunks(content []byte, isText bool) ([]string, []interface{}, error) {
	var dataChunks []string
	if isText {
		dataChunks = livesync.SplitText(string(content), 0)
	} else {
		encoded := base64.StdEncoding.EncodeToString(content)
		for len(encoded) > 0 {
			size := livesync.DefaultChunkSize
			if size > len(encoded) {
				size = len(encoded)
			}
			dataChunks = append(dataChunks, encoded[:size])
			encoded = encoded[size:]
		}
	}

	chunkIDs := make([]string, 0, len(dataChunks))
	chunkDocs := make([]interface{}, 0, len(dataChunks))
	for _, chunk := range dataChunks {
		id := s.crypto.ChunkID(chunk)
		chunkIDs = append(chunkIDs, id)

		data := chunk
		if s.crypto.Enabled() {
			encrypted, err := s.crypto.EncryptContent([]byte(chunk))
			if err != nil {
				return nil, nil, fmt.Errorf("encrypt chunk: %w", err)
			}
			data = encrypted
		}
		chunkDocs = append(chunkDocs, &couchdb.ChunkDoc{
			ID:        id,
			Data:      data,
			Type:      "leaf",
			Encrypted: s.crypto.Enabled(),
		})
	}
	return chunkIDs, chunkDocs, nil
}
