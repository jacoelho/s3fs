package s3fs

import (
	"io/fs"
)

var (
	_ fs.File     = (*Directory)(nil)
	_ fs.DirEntry = (*Directory)(nil)
)

type Directory struct {
	fs       *Fs
	fileInfo FileInfo
}

func (d *Directory) Name() string               { return d.fileInfo.Name() }
func (d *Directory) IsDir() bool                { return d.fileInfo.IsDir() }
func (d *Directory) Type() fs.FileMode          { return d.fileInfo.Type() }
func (d *Directory) Info() (fs.FileInfo, error) { return d.fileInfo.Info() }
func (d *Directory) Stat() (fs.FileInfo, error) { return &d.fileInfo, nil }
func (d *Directory) Close() error               { return nil }

func (d *Directory) Read(_ []byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.fileInfo.Name(), Err: fs.ErrInvalid}
}
