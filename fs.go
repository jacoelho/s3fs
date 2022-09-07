package s3fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	// pathSeparator is prefix separator.
	pathSeparator = "/"
	// currentDirName is the directory name entry when listed.
	currentDirName = "."
	// directoryFile is a file created to ensure a directory (prefix) exists.
	directoryFile = ".keep"
	// minPartSize is the minimum size allowed in multipart download/uploads.
	minPartSize = 5 * 1024 * 1024
)

var (
	_ fs.FS        = (*Fs)(nil)
	_ fs.ReadDirFS = (*Fs)(nil)
)

// Fs is fs.FS S3 filesystem abstraction.
type Fs struct {
	client        s3ApiClient
	bucket        string
	prefix        string
	timeout       time.Duration
	partSize      int64
	tempDir       string
	directoryFile string
}

// Option is a Fs configuration.
type Option func(*Fs)

// WithPrefix defines a common prefix inside a bucket.
func WithPrefix(prefix string) Option {
	return func(f *Fs) {
		if s := cleanPath(prefix); s != "" {
			f.prefix = strings.Trim(s, pathSeparator)
		}
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
		if size > minPartSize {
			f.partSize = size
		}
	}
}

// WithTemporaryDirectory sets the temporary directory
// where the unlinked temporary files will be created.
func WithTemporaryDirectory(dirName string) Option {
	return func(f *Fs) {
		f.tempDir = dirName
	}
}

// WithDirectoryFile sets the file created when CreateDir is used.
func WithDirectoryFile(s string) Option {
	return func(f *Fs) {
		if s != "" {
			f.directoryFile = s
		}
	}
}

// New creates a S3 fs abstraction
func New(client s3ApiClient, bucket string, opts ...Option) *Fs {
	f := &Fs{
		client:        client,
		bucket:        bucket,
		partSize:      minPartSize,
		directoryFile: directoryFile,
	}

	for _, o := range opts {
		o(f)
	}

	return f
}

// Open opens the named file or directory for reading.
func (f *Fs) Open(name string) (fs.File, error) {
	return f.OpenWithContext(context.Background(), name)
}

// OpenWithContext opens the named file or directory for reading.
func (f *Fs) OpenWithContext(ctx context.Context, name string) (fs.File, error) {
	info, err := f.StatWithContext(ctx, name)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return &Directory{
			fs:       f,
			fileInfo: info,
		}, nil
	}

	file := &File{
		fs:   f,
		info: info,
	}
	return file, file.openReaderAt(ctx, 0)
}

// Stat returns a FileInfo describing the named file.
func (f *Fs) Stat(name string) (FileInfo, error) {
	return f.StatWithContext(context.Background(), name)
}

// StatWithContext returns a FileInfo describing the named file.
func (f *Fs) StatWithContext(ctx context.Context, name string) (FileInfo, error) {
	// "." and "/" are always directories
	if cleanPath(name) == "" {
		return directoryFileInfo(currentDirName), nil
	}

	opts := &s3.ListObjectsV2Input{
		Bucket:    aws.String(f.bucket),
		Prefix:    aws.String(f.withPrefix(name)),
		Delimiter: aws.String(pathSeparator),
		MaxKeys:   1,
	}

	if f.timeout > 0 {
		var cancelFn context.CancelFunc
		ctx, cancelFn = context.WithTimeout(ctx, f.timeout)
		defer cancelFn()
	}

	res, err := f.client.ListObjectsV2(ctx, opts)
	if err != nil {
		return FileInfo{}, err
	}

	prefixedName := f.withPrefix(name)

	for _, el := range res.CommonPrefixes {
		if *el.Prefix == prefixedName+pathSeparator {
			return directoryFileInfo(cleanPath(name)), nil
		}
	}

	for _, el := range res.Contents {
		if *el.Key == prefixedName {
			return regularFileInfo(cleanPath(name), el.Size, getOrElse(el.LastModified, time.Now)), nil
		}
	}

	return FileInfo{}, fs.ErrNotExist
}

// Create opens a named file for writing.
func (f *Fs) Create(name string) (*File, error) {
	return f.CreateWithContext(context.Background(), name)
}

// CreateWithContext opens a named file for writing.
func (f *Fs) CreateWithContext(ctx context.Context, name string) (*File, error) {
	info, err := f.StatWithContext(ctx, name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if info.IsDir() {
		return nil, fmt.Errorf("named file is a directory: %w", fs.ErrExist)
	}

	file := &File{
		fs:   f,
		info: regularFileInfo(cleanPath(name), 0, time.Now()),
	}

	return file, file.openWriter(ctx)
}

// CreateDir creates a name directory
// Since S3 doesn't have the concept of directories, an empty file .keep is created.
func (f *Fs) CreateDir(name string) (fs.DirEntry, error) {
	return f.CreateDirWithContext(context.Background(), name)
}

// CreateDirWithContext creates a name directory
// Since S3 doesn't have the concept of directories, an empty file .keep is created.
func (f *Fs) CreateDirWithContext(ctx context.Context, name string) (fs.DirEntry, error) {
	info, err := f.StatWithContext(ctx, name)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	if err == nil && !info.IsDir() {
		return nil, fmt.Errorf("a file with the same name already exists: %w", fs.ErrExist)
	}

	if info.IsDir() {
		return nil, fmt.Errorf("a directory with the same name already exists: %w", fs.ErrExist)
	}

	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	_, err = f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.withPrefix(name, f.directoryFile)),
		Body:   bytes.NewReader(nil),
	})
	if err != nil {
		return nil, err
	}

	dir := &Directory{
		fs:       f,
		fileInfo: directoryFileInfo(cleanPath(name)),
	}

	return dir, nil
}

// ReadDir reads the named directory
// and returns a list of directory entries sorted by filename.
func (f *Fs) ReadDir(dirName string) ([]fs.DirEntry, error) {
	return f.ReadDirWithContext(context.Background(), dirName)
}

// ReadDirWithContext reads the named directory
// and returns a list of directory entries sorted by filename.
func (f *Fs) ReadDirWithContext(ctx context.Context, dirName string) ([]fs.DirEntry, error) {
	dirName = cleanPath(dirName)

	info, err := f.StatWithContext(ctx, dirName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []fs.DirEntry{}, nil
		}
		return nil, err
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("cannot list a file: %w", fs.ErrInvalid)
	}

	opts := &s3.ListObjectsV2Input{
		Bucket:    aws.String(f.bucket),
		Prefix:    aws.String(f.withPrefix(dirName) + pathSeparator),
		Delimiter: aws.String(pathSeparator),
	}

	if dirName == "" {
		opts.Prefix = nil
	}

	seenPrefixes := map[string]struct{}{
		currentDirName: {},
		pathSeparator:  {},
	}

	paginator := s3.NewListObjectsV2Paginator(f.client, opts)

	result := []fs.DirEntry{
		&Directory{
			fs:       f,
			fileInfo: directoryFileInfo(currentDirName),
		},
	}

	for paginator.HasMorePages() {
		var cancelFn context.CancelFunc
		if f.timeout > 0 {
			ctx, cancelFn = context.WithTimeout(ctx, f.timeout)
		}

		page, err := paginator.NextPage(ctx)

		if cancelFn != nil {
			cancelFn()
		}
		if err != nil {
			return nil, err
		}

		for _, p := range page.CommonPrefixes {
			if p.Prefix == nil {
				continue
			}

			dir, _ := baseName(*p.Prefix)

			if _, found := seenPrefixes[dir]; found {
				continue
			}

			seenPrefixes[dir] = struct{}{}

			result = append(result, &Directory{
				fs:       f,
				fileInfo: directoryFileInfo(dir),
			})
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			name, mode := baseName(*obj.Key)
			if name == "" || name == pathSeparator || name == f.directoryFile {
				continue
			}

			if mode&fs.ModeDir != 0 {
				if _, found := seenPrefixes[name]; found {
					continue
				}
				seenPrefixes[name] = struct{}{}
			}

			result = append(result, &File{
				fs:   f,
				info: regularFileInfo(name, obj.Size, getOrElse(obj.LastModified, time.Now)),
			})
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name() < result[j].Name() })

	return result, nil
}

// Remove removes the named file.
func (f *Fs) Remove(filename string) error {
	return f.RemoveWithContext(context.Background(), filename)
}

// RemoveWithContext removes the named file.
func (f *Fs) RemoveWithContext(ctx context.Context, fileName string) error {
	info, err := f.StatWithContext(ctx, fileName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}

	if info.IsDir() {
		return fmt.Errorf("named file is a directory: %w", fs.ErrInvalid)
	}

	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	_, err = f.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.withPrefix(fileName)),
	})
	return err
}

// Rename renames (moves) oldpath to newpath.
// If newpath already exists and is not a directory, Rename replaces it.
func (f *Fs) Rename(oldpath, newpath string) error {
	return f.RenameWithContext(context.Background(), oldpath, newpath)
}

// RenameWithContext renames (moves) oldpath to newpath.
// If newpath already exists and is not a directory, Rename replaces it.
func (f *Fs) RenameWithContext(ctx context.Context, oldpath, newpath string) error {
	oldInfo, err := f.StatWithContext(ctx, oldpath)
	if err != nil {
		return err
	}

	if oldInfo.IsDir() {
		return fmt.Errorf("oldpath is a directory: %w", fs.ErrInvalid)
	}

	newInfo, err := f.StatWithContext(ctx, newpath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	if newInfo.IsDir() {
		return fmt.Errorf("newpath is a directory: %w", fs.ErrInvalid)
	}

	if f.timeout > 0 {
		var cancelFn context.CancelFunc
		ctx, cancelFn = context.WithTimeout(ctx, f.timeout)
		defer cancelFn()
	}

	_, err = f.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(f.bucket),
		Key:        aws.String(f.withPrefix(newpath)),
		CopySource: aws.String(path.Join(f.bucket, f.withPrefix(oldpath))),
	})
	if err != nil {
		return err
	}

	return f.RemoveWithContext(ctx, oldpath)
}

// RemoveDir removes an empty directory.
func (f *Fs) RemoveDir(name string) error {
	return f.RemoveDirWithContext(context.Background(), name)
}

// RemoveDirWithContext removes an empty directory.
func (f *Fs) RemoveDirWithContext(ctx context.Context, name string) error {
	entries, err := f.ReadDirWithContext(ctx, name)
	if err != nil {
		return err
	}

	if len(entries) == 1 && entries[0].Name() == currentDirName {
		return f.Remove(path.Join(name, f.directoryFile))
	}

	return fmt.Errorf("directory not empty: %w", fs.ErrInvalid)
}

func (f *Fs) withPrefix(name ...string) string {
	p := path.Join(append([]string{f.prefix}, name...)...)

	return cleanPath(p)
}

func cleanPath(name string) string {
	name = path.Clean(name)

	if name == currentDirName || name == pathSeparator {
		return ""
	}

	return strings.TrimLeft(name, pathSeparator)
}

func baseName(name string) (string, fs.FileMode) {
	base := path.Base(name)

	if strings.HasSuffix(name, pathSeparator) {
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
