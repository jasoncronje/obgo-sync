package sync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/localstate"
	"github.com/jookos/obgo-sync/lib/livesync"
)

var ErrReceiveOnly = errors.New("LiveSync internal path is receive-only")

func (s *Service) loadInternalState() error {
	s.internalStateOnce.Do(func() {
		paths := map[string]string{}
		s.internalStateErr = localstate.Read(filepath.Join(s.dataDir, ".obgo_internal_paths"), &paths)
		if s.internalStateErr == nil {
			for id, path := range paths {
				s.indexInternal(id, path)
			}
		}
	})
	return s.internalStateErr
}

// Persist ownership before applying content or allowing ordinary-file writers.
func (s *Service) rememberInternal(docs []couchdb.MetaDoc) error {
	if err := s.loadInternalState(); err != nil {
		return err
	}
	s.internalStateMu.Lock()
	defer s.internalStateMu.Unlock()
	paths := map[string]string{}
	s.internalPaths.Range(func(id, path interface{}) bool { paths[id.(string)] = path.(string); return true })
	changed := false
	for _, doc := range docs {
		if !strings.HasPrefix(doc.ID, "i:") {
			continue
		}
		path := strings.TrimPrefix(doc.Path, "i:")
		if path == "" || strings.HasPrefix(path, "/\\:") || livesync.IsObfuscatedDocID(path) {
			continue
		}
		if err := validateInternalPath(s.dataDir, path); err != nil && !errors.Is(err, errInternalSymlink) {
			continue
		}
		if shouldSkipLocalPath(path, "", true) {
			continue
		}
		if old, ok := paths[doc.ID]; !ok || old != path {
			paths[doc.ID] = path
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := localstate.Write(filepath.Join(s.dataDir, ".obgo_internal_paths"), paths); err != nil {
		return err
	}
	for id, path := range paths {
		s.indexInternal(id, path)
	}
	return nil
}

func (s *Service) indexInternal(id, path string) {
	s.internalPaths.Store(id, path)
	s.internalOwnerPaths.Store(strings.ToLower(path), true)
	s.internalOwnerIDs.Store(livesync.EncodeDocID(path), true)
	s.internalOwnerIDs.Store(s.crypto.ObfuscateDocID(path), true)
}

func (s *Service) internalOwns(path string) bool {
	_, owned := s.internalOwnerPaths.Load(strings.ToLower(path))
	return owned
}

func (s *Service) internalOwnsDoc(id, path string) bool {
	if s.internalOwns(path) {
		return true
	}
	_, owned := s.internalOwnerIDs.Load(id)
	return owned
}

// Probe both LiveSync ID forms when a new hidden path arrives after the pull.
// This makes ownership independent of the order of ordinary and i: events.
func (s *Service) checkInternalOwner(ctx context.Context, path string) (bool, error) {
	if err := s.loadInternalState(); err != nil {
		return false, err
	}
	if s.internalOwns(path) {
		return true, nil
	}
	if !shouldSkipLocalPath(path, "", false) {
		return false, nil
	}
	for _, id := range []string{"i:" + livesync.EncodeDocID(path), "i:" + s.crypto.ObfuscateDocID("i:"+path)} {
		doc, err := s.db.GetMeta(ctx, id)
		if errors.Is(err, couchdb.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if doc == nil {
			return false, fmt.Errorf("missing internal metadata %q", id)
		}
		docs := []couchdb.MetaDoc{*doc}
		// The lookup identity establishes ownership even when the metadata
		// cannot be decrypted yet. Never fall back to an ordinary writer.
		docs[0].Path = "i:" + path
		if err := s.rememberInternal(docs); err != nil {
			return false, err
		}
		return s.internalOwns(path), nil
	}
	return false, nil
}
