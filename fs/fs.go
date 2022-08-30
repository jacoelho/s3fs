package fs

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eikenb/pipeat"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"
)

var _ fs.FS = (*Fs)(nil)

type Fs struct {
	client   *s3.Client
	bucket   string
	prefix   string
	timeout  time.Duration
	partSize int64
}

func (f *Fs) Open(name string) (fs.File, error) {
	file := &File{
		fs: f,
		info: FileInfo{
			name: name,
		},
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), f.timeout)
	defer cancelFn()

	obj, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(path.Join(f.prefix, name)),
	})
	if err != nil {
		return nil, err
	}

	file.info.size = obj.ContentLength
	file.info.modTime = getOrElse(obj.LastModified, time.Now)

	return file, file.openAt(io.SeekStart)
}

func (f *Fs) Create(name string) (*File, error) {
	r, w, err := pipeat.PipeInDir("")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	uploader := manager.NewUploader(f.client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = f.partSize
	})

	go func() {
		defer cancel()

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.fileWithPrefix(name)),
			Body:   r,
		})
		_ = w.CloseWithError(err)
	}()

	file := &File{
		fs: f,
		info: FileInfo{
			name: name,
		},
		writer:         w,
		writerCancelFn: cancel,
	}

	return file, err
}

func (f *Fs) fileWithPrefix(name string) string {
	return path.Join(f.prefix, name)
}

func normalizeName(prefix, key string) (string, fs.FileMode) {
	trimmed := strings.TrimPrefix(key, prefix)

	if strings.HasSuffix(trimmed, delimiter) {
		return trimmed[:len(trimmed)-1], fs.ModeDir
	}

	return trimmed, 0
}

func getOrElse[T any](v *T, fallback func() T) T {
	if v == nil {
		return fallback()
	}
	return *v
}
