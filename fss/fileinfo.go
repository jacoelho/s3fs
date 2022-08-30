package fss

import (
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ fs.FileInfo = (*fileInfo)(nil)

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	mode    fs.FileMode
	isDir   bool
}

func (s fileInfo) Name() string       { return s.name }
func (s fileInfo) Size() int64        { return s.size }
func (s fileInfo) ModTime() time.Time { return s.modTime }
func (s fileInfo) IsDir() bool        { return s.isDir }
func (s fileInfo) Sys() any           { return nil }
func (s fileInfo) Mode() fs.FileMode  { return s.mode }

func newFileInfo(name string, h *s3.HeadObjectOutput) fileInfo {
	var modTime time.Time
	if h.LastModified == nil {
		modTime = time.Unix(0, 0).UTC()
	} else {
		modTime = h.LastModified.UTC()
	}

	return fileInfo{
		name:    name,
		size:    h.ContentLength,
		modTime: modTime,
	}
}
