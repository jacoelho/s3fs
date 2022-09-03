package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const delimiter = "/"

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrNotDirectory     = errors.New("not a directory")
	ErrKeyAlreadyExists = errors.New("object already exists")
)

var (
	_ fs.FS        = (*Fs)(nil)
	_ fs.ReadDirFS = (*Fs)(nil)
)

type Fs struct {
	client   s3ApiClient
	bucket   string
	prefix   string
	timeout  time.Duration
	partSize int64
	tempDir  string
}

type Option func(*Fs)

// WithPrefix defines a common prefix inside a bucket.
func WithPrefix(prefix string) Option {
	return func(f *Fs) {
		f.prefix = strings.TrimPrefix(prefix, delimiter)
	}
}

// WithTimeout sets the timeout when interacting with S3.
func WithTimeout(d time.Duration) Option {
	return func(f *Fs) {
		f.timeout = d
	}
}

// WithPartSize sets the part size used on multipart download or upload.
func WithPartSize(size int64) Option {
	return func(f *Fs) {
		f.partSize = size
	}
}

// WithTemporaryDirectory sets the temporary directory
// where the unlinked temporary files will be created.
func WithTemporaryDirectory(dirName string) Option {
	return func(f *Fs) {
		f.tempDir = dirName
	}
}

// New creates a S3 fs abstraction
func New(client s3ApiClient, bucket string, opts ...Option) *Fs {
	f := &Fs{
		client:   client,
		bucket:   bucket,
		timeout:  time.Second,
		partSize: 5 * 1024 * 1024,
	}

	for _, o := range opts {
		o(f)
	}

	return f
}

// Open opens the named file or directory for reading.
func (f *Fs) Open(name string) (fs.File, error) {
	info, err := f.statObject(name)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	if err == nil {
		file := &File{
			fs:   f,
			info: info,
		}
		return file, file.openReaderAt(0)
	}

	info, err = f.statDirectory(name)
	if err != nil {
		return nil, err
	}

	return &Directory{
		fs:       f,
		fileInfo: info,
	}, nil
}

// statObject gets the named file fileinfo using head-object
func (f *Fs) statObject(name string) (FileInfo, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), f.timeout)
	defer cancelFn()

	obj, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.withPrefix(name)),
	})
	if err != nil {
		if isErrNotFound(err) {
			return FileInfo{}, ErrKeyNotFound
		}
		return FileInfo{}, err
	}

	return FileInfo{
		name:    name,
		size:    obj.ContentLength,
		modTime: getOrElse(obj.LastModified, time.Now),
	}, nil
}

// stat gets the name fileinfo using list-objects
func (f *Fs) statDirectory(name string) (FileInfo, error) {
	opts := &s3.ListObjectsV2Input{
		Bucket:    aws.String(f.bucket),
		Prefix:    aws.String(f.withPrefix(name) + "/"),
		Delimiter: aws.String(delimiter),
		MaxKeys:   1,
	}

	ctx, cancelFn := context.WithTimeout(context.Background(), f.timeout)
	defer cancelFn()

	res, err := f.client.ListObjectsV2(ctx, opts)
	if err != nil {
		return FileInfo{}, err
	}

	if res.KeyCount == 0 {
		return FileInfo{}, ErrKeyNotFound
	}

	return FileInfo{
		name:    name,
		mode:    fs.ModeDir,
		modTime: time.Now(),
	}, nil
}

// Create opens a named file for writing.
func (f *Fs) Create(name string) (*File, error) {
	info, err := f.statDirectory(name)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	if info.IsDir() {
		return nil, fmt.Errorf("named file is a directory: %w", ErrKeyAlreadyExists)
	}

	file := &File{
		fs: f,
		info: FileInfo{
			name: name,
		},
	}

	return file, file.openWriter()
}

// CreateDir creates a name directory
// Since S3 doesn't have the concept of directories, an empty file .keep is created.
func (f *Fs) CreateDir(name string) (fs.DirEntry, error) {
	_, err := f.statObject(name)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	if err == nil {
		return nil, fmt.Errorf("a file with the same name already exists: %w", ErrKeyAlreadyExists)
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()

	_, err = f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.withPrefix(name, ".keep")),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		return nil, err
	}

	dir := &Directory{
		fs: f,
		fileInfo: FileInfo{
			name:    name,
			mode:    fs.ModeDir,
			modTime: time.Now(),
		},
	}

	return dir, nil
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (f *Fs) ReadDir(dirName string) ([]fs.DirEntry, error) {
	_, err := f.statDirectory(dirName)
	if err != nil {
		return nil, err
	}

	opts := &s3.ListObjectsV2Input{
		Bucket:    aws.String(f.bucket),
		Prefix:    aws.String(f.withPrefix(dirName) + delimiter),
		Delimiter: aws.String(delimiter),
	}

	seenPrefixes := map[string]struct{}{
		".":       {},
		delimiter: {},
		dirName:   {},
	}

	paginator := s3.NewListObjectsV2Paginator(f.client, opts)
	var result []fs.DirEntry

	for paginator.HasMorePages() {
		ctx, cancelFn := context.WithTimeout(context.Background(), f.timeout)

		page, err := paginator.NextPage(ctx)
		cancelFn()
		if err != nil {
			return nil, err
		}

		for _, p := range page.CommonPrefixes {
			if p.Prefix == nil {
				continue
			}

			dir, mode := baseName(*p.Prefix)

			if _, found := seenPrefixes[dir]; found {
				continue
			}

			seenPrefixes[dir] = struct{}{}

			result = append(result, &Directory{
				fs: f,
				fileInfo: FileInfo{
					name:    dir,
					mode:    mode,
					modTime: time.Now(),
				},
			})
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			name, mode := baseName(*obj.Key)
			if name == "" || name == delimiter {
				continue
			}

			if mode&fs.ModeDir != 0 {
				if _, found := seenPrefixes[name]; found {
					continue
				}
				seenPrefixes[name] = struct{}{}
			}

			result = append(result, &File{
				fs: f,
				info: FileInfo{
					name:    name,
					size:    obj.Size,
					mode:    mode,
					modTime: getOrElse(obj.LastModified, time.Now),
				},
			})
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name() < result[j].Name() })

	return result, nil
}

func (f *Fs) withPrefix(name ...string) string {
	p := path.Join(append([]string{f.prefix}, name...)...)
	if p == "." {
		return ""
	}
	return p
}

func baseName(name string) (string, fs.FileMode) {
	base := path.Base(name)

	if strings.HasSuffix(name, delimiter) {
		return base, fs.ModeDir
	}

	return base, 0
}

func getOrElse[T any](v *T, fallback func() T) T {
	if v == nil {
		return fallback()
	}
	return *v
}

func isErrNotFound(err error) bool {
	if err == nil {
		return false
	}

	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.Response != nil {
		return re.Response.StatusCode == http.StatusNotFound
	}

	return false
}
