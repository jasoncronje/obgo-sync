package sync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errInternalSymlink = errors.New("internal file path crosses symlink")

// validateInternalPath prevents internal metadata from escaping the vault,
// including through an existing symlink. Do not clean away traversal first.
func validateInternalPath(root, path string) error {
	if path == "" || filepath.IsAbs(path) || strings.Contains(path, "\\") {
		return fmt.Errorf("invalid internal file path %q", path)
	}
	current := root
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid internal file path %q", path)
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %q", errInternalSymlink, path)
		}
	}
	return nil
}
