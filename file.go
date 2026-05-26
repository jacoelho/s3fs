package s3fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.File     = (*File)(nil)
	_ io.ReaderAt = (*File)(nil)
	_ io.Seeker   = (*File)(nil)
)

type File struct {
	body    io.ReadCloser
	readErr error
	fs      *Fs
	key     string
	info    FileInfo
	offset  int64
	mu      sync.Mutex
	closed  bool
}

func newFile(ctx context.Context, fsys *Fs, info FileInfo, key string) (*File, error) {
	f := &File{fs: fsys, info: info, key: key}
	if err := f.openAt(ctx, 0); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) Stat() (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, pathError("stat", f.info.path, fs.ErrClosed)
	}
	return f.info, nil
}

func (f *File) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, pathError("read", f.info.path, fs.ErrClosed)
	}
	if f.body == nil {
		if f.readErr != nil && f.offset < f.info.Size() {
			return 0, f.readErr
		}
		return 0, io.EOF
	}

	n, err := f.body.Read(p)
	f.offset += int64(n)
	if err != nil {
		_ = f.body.Close()
		f.body = nil
		if err != io.EOF {
			err = pathError("read", f.info.path, err)
			f.readErr = err
		} else {
			f.offset = f.info.Size()
			f.readErr = nil
		}
	}
	return n, err
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return 0, pathError("readat", f.info.path, fs.ErrClosed)
	}
	info := f.info
	key := f.key
	fsys := f.fs
	f.mu.Unlock()

	if off < 0 {
		return 0, pathError("readat", info.path, fs.ErrInvalid)
	}
	if off >= info.Size() {
		return 0, io.EOF
	}

	readSize := min(int64(len(p)), info.Size()-off)
	end := off + readSize - 1
	opCtx, cancel := fsys.operationContext(context.Background())
	defer cancel()

	input := &s3.GetObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
	}
	if info.etag != "" {
		input.IfMatch = aws.String(info.etag)
	}

	out, err := fsys.client.GetObject(opCtx, input)
	if err != nil {
		return 0, pathError("readat", info.path, err)
	}
	defer func() { _ = out.Body.Close() }()

	n, err := io.ReadFull(out.Body, p[:readSize])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if err != nil && err != io.EOF {
		err = pathError("readat", info.path, err)
	}
	if n < len(p) && err == nil {
		err = io.EOF
	}
	return n, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, pathError("seek", f.info.path, fs.ErrClosed)
	}

	var next int64
	var ok bool
	switch whence {
	case io.SeekStart:
		next = offset
		ok = true
	case io.SeekCurrent:
		next, ok = addOffset(f.offset, offset)
	case io.SeekEnd:
		next, ok = addOffset(f.info.Size(), offset)
	default:
		return 0, pathError("seek", f.info.path, fs.ErrInvalid)
	}
	if !ok || next < 0 || next > f.info.Size() {
		return 0, pathError("seek", f.info.path, fs.ErrInvalid)
	}
	if next == f.offset && (f.body != nil || next == f.info.Size()) {
		return next, nil
	}

	body, err := f.bodyAt(context.Background(), "seek", next)
	if err != nil {
		return 0, err
	}
	if err := f.closeBody(); err != nil {
		_ = body.Close()
		return 0, err
	}
	f.body = body
	f.offset = next
	f.readErr = nil
	return next, nil
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	f.closed = true
	return f.closeBody()
}

func (f *File) openAt(ctx context.Context, offset int64) error {
	body, err := f.bodyAt(ctx, "open", offset)
	if err != nil {
		return err
	}
	f.body = body
	f.offset = offset
	f.readErr = nil
	return nil
}

func (f *File) bodyAt(ctx context.Context, op string, offset int64) (io.ReadCloser, error) {
	if offset >= f.info.Size() {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	opCtx, cancel := f.fs.operationContext(ctx)

	input := &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.key),
	}
	if offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}
	if f.info.etag != "" {
		input.IfMatch = aws.String(f.info.etag)
	}

	out, err := f.fs.client.GetObject(opCtx, input)
	if err != nil {
		cancel()
		return nil, pathError(op, f.info.path, err)
	}
	return &cancelOnClose{ReadCloser: out.Body, cancel: cancel}, nil
}

func addOffset(base, delta int64) (int64, bool) {
	if delta > 0 && base > math.MaxInt64-delta {
		return 0, false
	}
	if delta < 0 && base < math.MinInt64-delta {
		return 0, false
	}
	return base + delta, true
}

func (f *File) closeBody() error {
	if f.body == nil {
		return nil
	}
	err := f.body.Close()
	f.body = nil
	if err != nil {
		return pathError("close", f.info.path, err)
	}
	return nil
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
}

func (r *cancelOnClose) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.cancel)
	return err
}
