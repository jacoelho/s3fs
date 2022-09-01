package s3fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	_ fs.File        = (*File)(nil)
	_ fs.DirEntry    = (*File)(nil)
	_ WriterCloserAt = (*File)(nil)
)

type File struct {
	fs   *Fs
	info FileInfo

	reader io.ReadCloser
	offset int64

	writer         WriterCloserAt
	writerCancelFn context.CancelFunc
}

func (f *File) Name() string               { return f.info.Name() }
func (f *File) IsDir() bool                { return f.info.IsDir() }
func (f *File) Type() fs.FileMode          { return f.info.Type() }
func (f *File) Info() (fs.FileInfo, error) { return f.info.Info() }
func (f *File) Stat() (fs.FileInfo, error) { return &f.info, nil }

func (f *File) Read(b []byte) (int, error) {
	if f.reader == nil {
		if err := f.openReaderAt(io.SeekStart); err != nil {
			return 0, err
		}
	}

	n, err := f.reader.Read(b)
	if err != nil {
		return n, err
	}

	f.offset += int64(n)

	return n, nil
}

func (f *File) ReadAt(b []byte, offset int64) (int, error) {
	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return f.Read(b)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	var start int64

	switch whence {
	case io.SeekStart:
		start = offset

	case io.SeekCurrent:
		start = f.offset + offset

	case io.SeekEnd:
		start = f.info.Size() - offset
	}

	if start < 0 || start > f.info.Size() {
		return 0, &fs.PathError{Op: "seek", Path: f.info.name, Err: fs.ErrInvalid}
	}

	if err := f.reader.Close(); err != nil {
		return 0, err
	}

	return start, f.openReaderAt(start)
}

func (f *File) openReaderAt(offset int64) error {
	var streamRange *string

	if offset > 0 {
		streamRange = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := f.fs.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.fs.withPrefix(f.info.name)),
		Range:  streamRange,
	})
	if err != nil {
		return err
	}

	f.offset = offset
	f.reader = resp.Body

	return nil
}

func (f *File) Write(p []byte) (n int, err error) {
	if f.writer == nil {
		return 0, fmt.Errorf("file not open for writing: %w", fs.ErrClosed)
	}
	return f.writer.Write(p)
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	if f.writer == nil {
		return 0, fmt.Errorf("file not open for writing: %w", fs.ErrClosed)
	}
	return f.writer.WriteAt(p, off)
}

func (f *File) Close() error {
	if f.reader != nil {
		if err := f.reader.Close(); err != nil {
			return err
		}
	}

	if f.writer != nil {
		if err := f.writer.Close(); err != nil {
			return err
		}
	}

	if f.writerCancelFn != nil {
		f.writerCancelFn()
	}

	return nil
}
