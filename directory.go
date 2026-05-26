package s3fs

import (
	"context"
	"io"
	"io/fs"
	"sync"
)

var (
	_ fs.File        = (*Directory)(nil)
	_ fs.ReadDirFile = (*Directory)(nil)
)

const maxReadDirBatch = 64

type Directory struct {
	mu     sync.Mutex
	fs     *Fs
	info   FileInfo
	path   logicalPath
	cursor *dirCursor
	closed bool
}

func newDirectory(fsys *Fs, info FileInfo, p logicalPath) *Directory {
	return &Directory{fs: fsys, info: info, path: p}
}

func (d *Directory) Stat() (fs.FileInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, pathError("stat", d.info.path, fs.ErrClosed)
	}
	return d.info, nil
}

func (d *Directory) Read(_ []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return 0, pathError("read", d.info.path, fs.ErrClosed)
	}
	return 0, pathError("read", d.info.path, fs.ErrInvalid)
}

func (d *Directory) ReadDir(n int) ([]fs.DirEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, pathError("readdir", d.info.path, fs.ErrClosed)
	}
	if d.cursor == nil {
		d.cursor = newDirCursor(d.fs, d.path)
	}
	if n <= 0 {
		var entries []fs.DirEntry
		for {
			entry, err := d.cursor.next(context.Background())
			if err == io.EOF {
				return entries, nil
			}
			if err != nil {
				if len(entries) > 0 {
					return entries, err
				}
				return nil, err
			}
			entries = append(entries, entry)
		}
	}

	limit := min(n, maxReadDirBatch)
	entries := make([]fs.DirEntry, 0, limit)
	for len(entries) < limit {
		entry, err := d.cursor.next(context.Background())
		if err == io.EOF {
			if len(entries) > 0 {
				return entries, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			if len(entries) > 0 {
				return entries, err
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (d *Directory) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.cursor = nil
	d.closed = true
	return nil
}

type dirEntry struct {
	info FileInfo
}

func (d dirEntry) Name() string {
	return d.info.Name()
}

func (d dirEntry) IsDir() bool {
	return d.info.IsDir()
}

func (d dirEntry) Type() fs.FileMode {
	return d.info.Type()
}

func (d dirEntry) Info() (fs.FileInfo, error) {
	return d.info, nil
}
