package s3fs

import (
	"fmt"
	"io/fs"
	"path"
	"strings"
	"unicode/utf8"
)

type logicalPath string

func (p logicalPath) String() string {
	if p.root() {
		return rootName
	}
	return string(p)
}

func (p logicalPath) Base() string {
	if p.root() {
		return rootName
	}
	return path.Base(string(p))
}

func (p logicalPath) root() bool {
	return p == ""
}

func normalizePath(name, marker string, allowRoot bool) (logicalPath, error) {
	if name == "" {
		return "", fs.ErrInvalid
	}
	if name == rootName {
		if allowRoot {
			return "", nil
		}
		return "", fs.ErrInvalid
	}
	if strings.HasPrefix(name, pathSeparator) || strings.HasSuffix(name, pathSeparator) {
		return "", fs.ErrInvalid
	}
	if strings.Contains(name, pathSeparator+pathSeparator) {
		return "", fs.ErrInvalid
	}

	parts := strings.Split(name, pathSeparator)
	for _, part := range parts {
		if !validEntryName(part, marker) {
			return "", fmt.Errorf("%q: %w", part, fs.ErrInvalid)
		}
	}
	return logicalPath(name), nil
}

func normalizePrefix(prefix, marker string) (string, error) {
	if strings.HasPrefix(prefix, pathSeparator) {
		return "", fs.ErrInvalid
	}
	prefix = strings.TrimSuffix(prefix, pathSeparator)
	if prefix == "" {
		return "", nil
	}
	p, err := normalizePath(prefix, marker, false)
	if err != nil {
		return "", err
	}
	return p.String(), nil
}

func normalizeMarker(marker string) (string, error) {
	if !validEntryName(marker, "") {
		return "", fs.ErrInvalid
	}
	return marker, nil
}

func validEntryName(name, marker string) bool {
	if name == "" || name == rootName || name == ".." || strings.Contains(name, pathSeparator) || !utf8.ValidString(name) {
		return false
	}
	if marker != "" && name == marker {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < 0x20 || name[i] == 0x7f {
			return false
		}
	}
	return true
}
