package fss

import (
	"context"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.File           = (*File)(nil)
	_ fs.FileInfo       = (*File)(nil)
	_ io.ReadSeekCloser = (*File)(nil)
)

type File struct {
	fs       *Fs
	name     string
	fileInfo os.FileInfo
	reader   *manager.Downloader
	offset   int64
}

func (f File) Stat() (fs.FileInfo, error) { return f.fileInfo, nil }
func (f File) Name() string               { return f.name }
func (f File) Size() int64                { return f.fileInfo.Size() }
func (f File) Mode() fs.FileMode          { return f.fileInfo.Mode() }
func (f File) ModTime() time.Time         { return f.fileInfo.ModTime() }
func (f File) IsDir() bool                { return f.fileInfo.IsDir() }
func (f File) Sys() any                   { return f.fileInfo.Sys() }

func NewFile(fs *Fs, name string) *File {
	return &File{
		fs:     fs,
		name:   name,
		offset: 0,
		reader: manager.NewDownloader(s3.NewFromConfig(fs.config), func(d *manager.Downloader) {
			d.Concurrency = 1
			d.PartSize = 1 << 25
		}),
	}
}

func (f File) Read(p []byte) (n int, err error) {
	b := manager.NewWriteAtBuffer(p)

	f.reader.Download(context.Background(), b)
}

func (f File) Seek(offset int64, whence int) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (f File) Close() error {
	//TODO implement me
	panic("implement me")
}
