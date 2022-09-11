package s3fs

import (
	"io/fs"
	"time"
)

type FileInfo struct {
	modTime time.Time
	name    string
	size    int64
	mode    fs.FileMode
}

func directoryFileInfo(name string) FileInfo {
	return FileInfo{
		name:    name,
		mode:    0o755 | fs.ModeDir,
		modTime: time.Now(),
	}
}

func regularFileInfo(name string, size int64, modTime time.Time) FileInfo {
	return FileInfo{
		name:    name,
		size:    size,
		mode:    0o644,
		modTime: modTime,
	}
}

func (i *FileInfo) Name() string               { return i.name }
func (i *FileInfo) Size() int64                { return i.size }
func (i *FileInfo) Type() fs.FileMode          { return i.mode }
func (i *FileInfo) ModTime() time.Time         { return i.modTime }
func (i *FileInfo) IsDir() bool                { return i.mode&fs.ModeDir != 0 }
func (i *FileInfo) Sys() interface{}           { return nil }
func (i *FileInfo) Info() (fs.FileInfo, error) { return i, nil }
func (i *FileInfo) Mode() fs.FileMode          { return i.mode }
