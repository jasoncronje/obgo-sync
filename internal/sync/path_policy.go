package sync

import (
	"path/filepath"
	"strings"
)

func shouldSkipLocalName(name string, syncHidden bool) bool {
	if name == "." {
		return false
	}
	if isProtectedLocalName(name) {
		return true
	}
	return !syncHidden && isHiddenLocalName(name)
}

func shouldSkipLocalPath(path string, syncHidden bool) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == "" {
			continue
		}
		if shouldSkipLocalName(part, syncHidden) {
			return true
		}
	}
	return false
}

func isHiddenLocalName(name string) bool {
	return len(name) > 0 && name[0] == '.'
}

func isProtectedLocalName(name string) bool {
	return name == ".git" || strings.HasPrefix(name, ".obgo")
}
