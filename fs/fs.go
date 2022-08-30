package s3fs

import (
	"context"
	"io"
	"io/fs"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eikenb/pipeat"
)

const delimiter = "/"

var _ fs.FS = (*Fs)(nil)

type Fs struct {
	client   *s3.Client
	bucket   string
	prefix   string
	timeout  time.Duration
	partSize int64
}

func New(client *s3.Client, bucket string, prefix string) *Fs {
	return &Fs{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		timeout:  time.Second,
		partSize: 5 * 1024 * 1024,
	}
}

func (f *Fs) Open(name string) (fs.File, error) {
	info, err := f.stat(name)
	if err != nil {
		return nil, err
	}

	file := &File{
		fs:   f,
		info: info,
	}

	return file, file.openAt(io.SeekStart)
}

func (f *Fs) stat(name string) (FileInfo, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), f.timeout)
	defer cancelFn()

	obj, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.fileWithPrefix(name)),
	})
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{
		name:    name,
		size:    obj.ContentLength,
		modTime: getOrElse(obj.LastModified, time.Now),
	}, nil
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
		_ = r.CloseWithError(err)
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
