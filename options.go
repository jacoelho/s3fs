package s3fs

import (
	"errors"
	"fmt"
	"io/fs"
	"time"
)

// Option configures Fs.
type Option func(*Fs)

// WithPrefix scopes all keys to prefix.
func WithPrefix(prefix string) Option {
	return func(f *Fs) {
		f.prefix = prefix
	}
}

// WithTimeout sets a timeout for each S3 operation.
func WithTimeout(d time.Duration) Option {
	return func(f *Fs) {
		f.timeout = d
	}
}

// WithPartSize sets multipart part size. Default is 50 MiB.
func WithPartSize(size int64) Option {
	return func(f *Fs) {
		f.partSize = size
	}
}

// WithMaxWriteSize caps object size accepted by Writer.
func WithMaxWriteSize(size int64) Option {
	return func(f *Fs) {
		f.maxWriteSize = size
		f.maxWriteSizeSet = true
	}
}

// WithMaxActiveWriters caps concurrent writers and therefore write memory.
func WithMaxActiveWriters(n int) Option {
	return func(f *Fs) {
		f.maxActiveWriters = n
	}
}

// WithDirectoryFile sets marker object basename for empty directories.
func WithDirectoryFile(name string) Option {
	return func(f *Fs) {
		f.directoryFile = name
	}
}

// New creates an S3 filesystem.
func New(client s3ApiClient, bucket string, opts ...Option) (*Fs, error) {
	f := &Fs{
		client:           client,
		bucket:           bucket,
		directoryFile:    defaultDirectoryFile,
		partSize:         defaultPartSize,
		maxActiveWriters: 1,
	}

	for _, opt := range opts {
		opt(f)
	}

	if err := f.validate(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Fs) validate() error {
	if f.client == nil {
		return errors.New("nil s3 client")
	}
	if f.bucket == "" {
		return errors.New("empty bucket")
	}
	if f.timeout < 0 {
		return fmt.Errorf("negative timeout: %w", fs.ErrInvalid)
	}
	if f.partSize < minPartSize || f.partSize > maxPartSize {
		return fmt.Errorf("part size must be between %d and %d: %w", minPartSize, maxPartSize, fs.ErrInvalid)
	}
	if f.maxActiveWriters <= 0 {
		return fmt.Errorf("max active writers must be positive: %w", fs.ErrInvalid)
	}
	f.writerSlots = make(chan struct{}, f.maxActiveWriters)

	marker, err := normalizeMarker(f.directoryFile)
	if err != nil {
		return fmt.Errorf("directory file: %w", err)
	}
	f.directoryFile = marker

	prefix, err := normalizePrefix(f.prefix, f.directoryFile)
	if err != nil {
		return fmt.Errorf("prefix: %w", err)
	}
	f.prefix = prefix

	derivedMax := f.partSize * maxUploadParts
	if !f.maxWriteSizeSet {
		f.maxWriteSize = derivedMax
	}
	if f.maxWriteSize <= 0 || f.maxWriteSize > derivedMax {
		return fmt.Errorf("max write size must be between 1 and %d: %w", derivedMax, fs.ErrInvalid)
	}
	return nil
}
