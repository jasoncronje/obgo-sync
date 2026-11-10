package sync

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/watcher"
	"github.com/jookos/obgo-sync/lib/livesync"
)

// Pull fetches remote documents and writes them to OBGO_DATA.
// filter is a vault-relative path: empty means all docs, a path ending with "/"
// pulls that folder and its contents, otherwise pulls the single named file.
// If E2EE is enabled, it loads the HKDF salt from CouchDB _local doc first.
func (s *Service) Pull(ctx context.Context, filter string) error {
	// 1. Load HKDF salt from CouchDB _local doc.
	if err := s.loadSalt(ctx); err != nil {
		return fmt.Errorf("pull: %w", err)
	}

	// 2. Fetch all meta docs; detect + handle obfuscation before any filtering.
	var docs []couchdb.MetaDoc
	var err error
	if reader, ok := s.db.(interface {
		AllPullMetaDocs(context.Context) ([]couchdb.MetaDoc, error)
	}); ok && s.syncHidden {
		docs, err = reader.AllPullMetaDocs(ctx)
	} else {
		docs, err = s.db.AllMetaDocs(ctx)
	}
	if err != nil {
		return fmt.Errorf("pull: list docs: %w", err)
	}
	s.detectObfuscation(docs)
	s.decryptDocPaths(docs)
	if err := s.rememberInternal(docs); err != nil {
		return fmt.Errorf("persist internal ownership: %w", err)
	}

	// 3. Single-file shortcut: find the requested doc in the already-loaded list.
	if filter != "" && !strings.HasSuffix(filter, "/") {
		for _, doc := range docs {
			if !strings.HasPrefix(doc.ID, "i:") && s.internalOwnsDoc(doc.ID, doc.Path) {
				continue
			}
			if strings.TrimPrefix(doc.Path, "i:") == filter {
				resolved, rerr := s.resolveConflicts(ctx, doc)
				if rerr != nil {
					fmt.Fprintf(os.Stderr, "pull: resolve conflicts %q: %v\n", doc.Path, rerr)
					resolved = doc
				}
				if err := s.applyRemoteDoc(ctx, resolved); err != nil {
					return fmt.Errorf("pull: apply %q: %w", doc.Path, err)
				}
				if s.OnPullFile != nil {
					s.OnPullFile(1)
				}
				return nil
			}
		}
		return fmt.Errorf("pull: %q not found in remote vault", filter)
	}

	// 4. For each meta doc, apply to disk (skipping those outside the folder filter).
	var count int
	for _, doc := range docs {
		if !strings.HasPrefix(doc.ID, "i:") && s.internalOwnsDoc(doc.ID, doc.Path) {
			continue
		}
		if filter != "" && !strings.HasPrefix(strings.TrimPrefix(doc.Path, "i:"), filter) {
			continue
		}
		resolved, rerr := s.resolveConflicts(ctx, doc)
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "pull: resolve conflicts %q: %v\n", doc.Path, rerr)
			resolved = doc
		}
		if err := s.applyRemoteDoc(ctx, resolved); err != nil {
			if strings.HasPrefix(doc.ID, "i:") {
				if qerr := watcher.DeferEvent(s.dataDir, couchdb.ChangeEvent{ID: doc.ID, Doc: &doc}, err); qerr != nil {
					return qerr
				}
				fmt.Fprintf(os.Stderr, "pull: deferred %q: %v\n", doc.ID, err)
				continue
			}
			return fmt.Errorf("pull: apply %q: %w", doc.Path, err)
		}
		count++
		if s.OnPullFile != nil {
			s.OnPullFile(count)
		}
	}

	return nil
}

// applyRemoteDoc fetches chunks for a meta doc, assembles the content and
// writes it to the local filesystem. If the doc signals deletion (either a
// CouchDB tombstone _deleted:true or a Livesync app-level deleted:true field),
// the corresponding local file is removed instead.
func (s *Service) applyRemoteDoc(ctx context.Context, doc couchdb.MetaDoc) error {
	if err := s.loadInternalState(); err != nil {
		return err
	}
	// Decrypt obfuscated path field ("/\:" prefix) if present.
	// decryptDocPaths does this in bulk for the main loop, but watch mode and
	// conflict-resolved docs may call applyRemoteDoc directly.
	// Important: also restore Children from the blob — Obsidian Livesync stores
	// chunk IDs inside the encrypted blob when "Obfuscate Properties" is on.
	if strings.HasPrefix(doc.Path, "/\\:") {
		blob, err := s.crypto.DecryptPathBlob(doc.Path)
		if err != nil {
			return fmt.Errorf("decrypt path: %w", err)
		}
		if blob != nil {
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
		}
	}

	if strings.HasPrefix(doc.ID, "i:") {
		if !s.syncHidden {
			return nil
		}
		path := strings.TrimPrefix(doc.Path, "i:")
		if path == "" {
			path = strings.TrimPrefix(doc.ID, "i:")
		}
		// Hashed tombstones have no recoverable path. Never treat a hash as a filename.
		if livesync.IsObfuscatedDocID(path) {
			known, ok := s.internalPaths.Load(doc.ID)
			if !ok {
				return fmt.Errorf("internal tombstone %q has no persisted path", doc.ID)
			}
			path = known.(string)
		}
		if err := validateInternalPath(s.dataDir, path); err != nil {
			if errors.Is(err, errInternalSymlink) {
				// Keep local aliases intact; their canonical files sync separately.
				return nil
			}
			return err
		}
		doc.Path = path
		if shouldSkipLocalPath(path, "", s.syncHidden) {
			return nil
		}
		if err := s.rememberInternal([]couchdb.MetaDoc{doc}); err != nil {
			return err
		}
		if len(doc.Conflicts) > 0 {
			return fmt.Errorf("internal file %q has unresolved conflicts", doc.ID)
		}
		if !doc.IsDeleted() && len(doc.Eden) > 0 {
			return fmt.Errorf("internal file %q uses unsupported embedded chunks", doc.ID)
		}
	} else {
		if s.internalOwnsDoc(doc.ID, doc.Path) {
			return nil
		}
		if doc.Path != "" {
			owned, err := s.checkInternalOwner(ctx, doc.Path)
			if err != nil {
				return err
			}
			if owned {
				return nil
			}
		}
	}

	// Handle remote deletions: remove the local file.
	if doc.Deleted || doc.DeletedApp {
		// Lean tombstones (from Obsidian/PouchDB HTTP DELETE) have no path field;
		// fall back to decoding the document ID.
		path := doc.Path
		if path == "" {
			path, _ = livesync.DecodeDocID(doc.ID)
		}
		// Skip if path resolved to an opaque f: hash (can't find the local file).
		if path == "" || livesync.IsObfuscatedDocID(path) {
			return nil
		}
		// Mirrors the push-side policy: never touch .git/.obgo* locally, and
		// leave hidden paths alone unless this device opted in. Without this,
		// a hidden file another (opted-in) device deleted remotely could still
		// trigger a local remove/suppress on a device that never pulled it.
		if shouldSkipLocalPath(path, "", s.syncHidden) {
			return nil
		}
		// resolveCase handles case mismatches between the lowercase doc ID
		// and the original-cased path stored on a case-sensitive filesystem.
		absPath := resolveCase(s.dataDir, path)
		s.suppress.Add(absPath)
		if err := os.Remove(absPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if s.OnDeleteFile != nil {
			s.OnDeleteFile(path)
		}
		return nil
	}
	// Skip internal state files, and hidden paths unless this device opted
	// in via OBGO_SYNC_HIDDEN — otherwise a device with the flag off would
	// pull a file it will then refuse to push or delete (see push.go).
	if shouldSkipLocalPath(doc.Path, "", s.syncHidden) {
		return nil
	}
	content, err := s.readRemoteContent(ctx, doc)
	if err != nil {
		return err
	}

	// Write to disk.
	absPath := filepath.Join(s.dataDir, filepath.FromSlash(doc.Path))
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	s.suppress.Add(absPath)
	return os.WriteFile(absPath, content, 0o644)
}

// readRemoteContent fetches a meta doc's chunks and assembles the file content.
func (s *Service) readRemoteContent(ctx context.Context, doc couchdb.MetaDoc) ([]byte, error) {
	// Fetch chunks.
	chunks, err := s.db.BulkGet(ctx, doc.Children)
	if err != nil {
		return nil, fmt.Errorf("fetch chunks: %w", err)
	}

	// Build a map for ordering; BulkGet does not guarantee order.
	chunkMap := make(map[string]couchdb.ChunkDoc, len(chunks))
	for _, c := range chunks {
		chunkMap[c.ID] = c
	}

	// Assemble content: each chunk is decrypted (if vault is encrypted) and
	// then decoded (if file is binary) before concatenation.
	var content []byte
	for _, id := range doc.Children {
		chunk, ok := chunkMap[id]
		if !ok {
			return nil, fmt.Errorf("missing chunk %q", id)
		}
		data := chunk.Data

		var piece []byte
		var err error
		// Decrypt only chunks actually flagged encrypted (e_). This vault is a
		// MIX of encrypted and plaintext chunks — E2EE was enabled on an
		// already-populated vault without a full rebuild, so old chunks remain
		// plaintext and some even start with "%" (e.g. Templater directives).
		// The ciphertext-prefix heuristic is therefore unreliable; the per-chunk
		// e_ flag is the only correct signal, and it is what the official
		// LiveSync client keys off.
		if s.crypto.Enabled() && chunk.Encrypted {
			piece, err = s.crypto.DecryptContent(data)
			if err != nil {
				return nil, fmt.Errorf("decrypt chunk %q: %w", id, err)
			}
		} else {
			piece = []byte(data)
		}

		if doc.Type == "newnote" {
			// Binary file: chunks are base64-encoded (either raw or inside ciphertext).
			decoded, err := base64.StdEncoding.DecodeString(string(piece))
			if err != nil {
				return nil, fmt.Errorf("decode chunk %q: %w", id, err)
			}
			piece = decoded
		}
		content = append(content, piece...)
	}
	return content, nil
}
