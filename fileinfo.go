package s3fs

import (
	"io/fs"
	"time"
)

type FileInfo struct {
	name    string
	path    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	etag    string
}

func directoryFileInfo(name, fullPath string) FileInfo {
	return FileInfo{
		name: name,
		path: fullPath,
		mode: 0o755 | fs.ModeDir,
	}
}

func regularFileInfo(name, fullPath string, size int64, modTime time.Time, etag string) FileInfo {
	return FileInfo{
		name:    name,
		path:    fullPath,
		size:    size,
		mode:    0o644,
		modTime: modTime,
		etag:    etag,
	}
}

func (i FileInfo) Name() string       { return i.name }
func (i FileInfo) Size() int64        { return i.size }
func (i FileInfo) Mode() fs.FileMode  { return i.mode }
func (i FileInfo) ModTime() time.Time { return i.modTime }
func (i FileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i FileInfo) Sys() any           { return nil }
func (i FileInfo) Type() fs.FileMode  { return i.mode.Type() }
