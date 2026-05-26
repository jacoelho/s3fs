package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const (
	pathSeparator = "/"
	rootName      = "."

	defaultDirectoryFile = ".keep"

	minPartSize     int64 = 5 << 20
	defaultPartSize int64 = 50 << 20
	maxPartSize     int64 = 5 << 30
	maxUploadParts        = 10_000
)

var (
	_ fs.FS        = (*Fs)(nil)
	_ fs.ReadDirFS = (*Fs)(nil)
	_ fs.StatFS    = (*Fs)(nil)

	ErrWriteTooLarge  = errors.New("write too large")
	ErrTooManyWriters = errors.New("too many active writers")
)

// Fs is an S3-backed io/fs filesystem.
type Fs struct {
	client           s3ApiClient
	writerSlots      chan struct{}
	bucket           string
	prefix           string
	directoryFile    string
	timeout          time.Duration
	partSize         int64
	maxWriteSize     int64
	maxActiveWriters int
	maxWriteSizeSet  bool
}

// Open opens name for reading.
func (f *Fs) Open(name string) (fs.File, error) {
	return f.OpenWithContext(context.Background(), name)
}

// OpenWithContext opens name for reading.
func (f *Fs) OpenWithContext(ctx context.Context, name string) (fs.File, error) {
	p, err := normalizePath(name, f.directoryFile, true)
	if err != nil {
		return nil, pathError("open", name, err)
	}

	info, kind, err := f.statPath(ctx, "open", p)
	if err != nil {
		return nil, err
	}
	if kind == pathDirectory {
		return newDirectory(f, info, p), nil
	}

	return newFile(ctx, f, info, f.key(p))
}

// Stat returns a FileInfo describing name.
func (f *Fs) Stat(name string) (fs.FileInfo, error) {
	return f.StatWithContext(context.Background(), name)
}

// StatWithContext returns a FileInfo describing name.
func (f *Fs) StatWithContext(ctx context.Context, name string) (fs.FileInfo, error) {
	p, err := normalizePath(name, f.directoryFile, true)
	if err != nil {
		return nil, pathError("stat", name, err)
	}

	info, _, err := f.statPath(ctx, "stat", p)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (f *Fs) statPath(ctx context.Context, op string, p logicalPath) (FileInfo, pathKind, error) {
	if p.root() {
		return directoryFileInfo(rootName, rootName), pathDirectory, nil
	}

	exists, err := f.dirExists(ctx, p)
	if err != nil {
		return FileInfo{}, pathMissing, pathError(op, p.String(), err)
	}
	if exists {
		return directoryFileInfo(p.Base(), p.String()), pathDirectory, nil
	}

	info, err := f.headFile(ctx, op, p)
	if err != nil {
		return FileInfo{}, pathMissing, err
	}
	return info, pathFile, nil
}

// Create opens name for writing.
func (f *Fs) Create(name string) (*Writer, error) {
	return f.CreateWithContext(context.Background(), name)
}

// CreateWithContext opens name for writing.
func (f *Fs) CreateWithContext(ctx context.Context, name string) (*Writer, error) {
	p, err := normalizePath(name, f.directoryFile, false)
	if err != nil {
		return nil, pathError("create", name, err)
	}

	_, kind, err := f.statPath(ctx, "create", p)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if kind == pathDirectory {
		return nil, pathError("create", p.String(), fs.ErrExist)
	}

	if !f.acquireWriter() {
		return nil, pathError("create", p.String(), ErrTooManyWriters)
	}
	return newWriter(ctx, writerConfig{
		client:           f.client,
		bucket:           f.bucket,
		key:              f.key(p),
		path:             p.String(),
		partSize:         f.partSize,
		maxWriteSize:     f.maxWriteSize,
		operationContext: f.operationContext,
		cleanupContext:   f.cleanupContext,
		release:          f.releaseWriter,
	}), nil
}

// CreateDir creates an empty directory marker.
func (f *Fs) CreateDir(name string) error {
	return f.CreateDirWithContext(context.Background(), name)
}

// CreateDirWithContext creates an empty directory marker.
func (f *Fs) CreateDirWithContext(ctx context.Context, name string) error {
	p, err := normalizePath(name, f.directoryFile, false)
	if err != nil {
		return pathError("mkdir", name, err)
	}

	_, kind, err := f.statPath(ctx, "mkdir", p)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if kind != pathMissing {
		return pathError("mkdir", p.String(), fs.ErrExist)
	}

	opCtx, cancel := f.operationContext(ctx)
	defer cancel()

	_, err = f.client.PutObject(opCtx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.markerKey(p)),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		return pathError("mkdir", p.String(), err)
	}
	return nil
}

// ReadDir reads dirName and returns entries sorted by name.
func (f *Fs) ReadDir(dirName string) ([]fs.DirEntry, error) {
	return f.ReadDirWithContext(context.Background(), dirName)
}

// ReadDirWithContext reads dirName and returns entries sorted by name.
func (f *Fs) ReadDirWithContext(ctx context.Context, dirName string) ([]fs.DirEntry, error) {
	p, err := normalizePath(dirName, f.directoryFile, true)
	if err != nil {
		return nil, pathError("readdir", dirName, err)
	}

	entries, err := f.readDirEntries(ctx, p)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// Remove removes a file.
func (f *Fs) Remove(name string) error {
	return f.RemoveWithContext(context.Background(), name)
}

// RemoveWithContext removes a file.
func (f *Fs) RemoveWithContext(ctx context.Context, name string) error {
	p, err := normalizePath(name, f.directoryFile, false)
	if err != nil {
		return pathError("remove", name, err)
	}

	_, kind, err := f.statPath(ctx, "remove", p)
	if err != nil {
		return err
	}
	if kind != pathFile {
		return pathError("remove", p.String(), fs.ErrInvalid)
	}

	opCtx, cancel := f.operationContext(ctx)
	defer cancel()

	_, err = f.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key(p)),
	})
	if err != nil {
		return pathError("remove", p.String(), err)
	}
	return nil
}

// RemoveDir removes an empty directory marker.
func (f *Fs) RemoveDir(name string) error {
	return f.RemoveDirWithContext(context.Background(), name)
}

// RemoveDirWithContext removes an empty directory marker.
func (f *Fs) RemoveDirWithContext(ctx context.Context, name string) error {
	p, err := normalizePath(name, f.directoryFile, false)
	if err != nil {
		return pathError("rmdir", name, err)
	}

	state, err := f.emptyDirMarkers(ctx, p)
	if err != nil {
		return pathError("rmdir", p.String(), err)
	}
	if !state.exists {
		_, err := f.headFile(ctx, "rmdir", p)
		if err == nil {
			return pathError("rmdir", p.String(), fs.ErrInvalid)
		}
		return err
	}
	if !state.empty {
		return pathError("rmdir", p.String(), fs.ErrInvalid)
	}

	for _, key := range state.markers {
		opCtx, cancel := f.operationContext(ctx)
		_, err := f.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(key),
		})
		cancel()
		if err != nil {
			return pathError("rmdir", p.String(), err)
		}
	}
	return nil
}

func (f *Fs) headFile(ctx context.Context, op string, p logicalPath) (FileInfo, error) {
	key := f.key(p)
	opCtx, cancel := f.operationContext(ctx)
	defer cancel()

	out, err := f.client.HeadObject(opCtx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return FileInfo{}, pathError(op, p.String(), fs.ErrNotExist)
		}
		return FileInfo{}, pathError(op, p.String(), err)
	}
	return regularFileInfo(p.Base(), p.String(), aws.ToInt64(out.ContentLength), aws.ToTime(out.LastModified), aws.ToString(out.ETag)), nil
}

func (f *Fs) key(p logicalPath) string {
	if f.prefix == "" {
		return p.String()
	}
	if p.root() {
		return f.prefix
	}
	return f.prefix + pathSeparator + p.String()
}

func (f *Fs) dirPrefix(p logicalPath) string {
	if f.prefix == "" {
		if p.root() {
			return ""
		}
		return p.String() + pathSeparator
	}
	if p.root() {
		return f.prefix + pathSeparator
	}
	return f.prefix + pathSeparator + p.String() + pathSeparator
}

func (f *Fs) markerKey(p logicalPath) string {
	return f.dirPrefix(p) + f.directoryFile
}

func (f *Fs) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if f.timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, f.timeout)
}

func (f *Fs) cleanupContext() (context.Context, context.CancelFunc) {
	if f.timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), f.timeout)
}

func (f *Fs) acquireWriter() bool {
	select {
	case f.writerSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *Fs) releaseWriter() {
	<-f.writerSlots
}

type pathKind uint8

const (
	pathMissing pathKind = iota
	pathFile
	pathDirectory
)

func pathError(op, name string, err error) error {
	return &fs.PathError{Op: op, Path: name, Err: err}
}

func isNotFound(err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NoSuchBucket", "NotFound", "404":
		return true
	default:
		return false
	}
}
