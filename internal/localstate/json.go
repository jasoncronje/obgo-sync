// Package localstate persists private sync state before checkpoints advance.
package localstate

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func Write(path string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return WriteBytes(path, data)
}

func WriteBytes(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".obgo-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func Read(path string, value interface{}) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}
