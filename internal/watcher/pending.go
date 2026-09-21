package watcher

import (
	"github.com/jookos/obgo-sync/internal/couchdb"
	"github.com/jookos/obgo-sync/internal/localstate"
	"path/filepath"
)

type PendingEvent struct {
	Event couchdb.ChangeEvent `json:"event"`
	Error string              `json:"error"`
}

// DeferEvent is used during the initial pull, before the watcher starts.
// The durable queue permits unrelated documents to continue syncing.
func DeferEvent(dir string, event couchdb.ChangeEvent, cause error) error {
	pending := map[string]PendingEvent{}
	file := filepath.Join(dir, ".obgo_pending")
	if err := localstate.Read(file, &pending); err != nil {
		return err
	}
	pending[event.ID] = PendingEvent{Event: event, Error: cause.Error()}
	return localstate.Write(file, pending)
}
