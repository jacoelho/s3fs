package s3fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eikenb/pipeat"
)

var (
	_ fs.File        = (*File)(nil)
	_ fs.DirEntry    = (*File)(nil)
	_ writerCloserAt = (*File)(nil)
)

type File struct {
	fs   *Fs
	info FileInfo

	offset         int64
	reader         readerCloserAt
	readerCancelFn context.CancelFunc

	writer         writerCloserAt
	writerCancelFn context.CancelFunc
}

func (f *File) Name() string               { return f.info.Name() }
func (f *File) IsDir() bool                { return f.info.IsDir() }
func (f *File) Type() fs.FileMode          { return f.info.Type() }
func (f *File) Info() (fs.FileInfo, error) { return f.info.Info() }
func (f *File) Stat() (fs.FileInfo, error) { return &f.info, nil }

func (f *File) Read(b []byte) (int, error) {
	if f.reader == nil {
		return 0, fmt.Errorf("file not open for reading: %w", fs.ErrClosed)
	}

	n, err := f.reader.Read(b)
	if err != nil {
		return n, err
	}

	f.offset += int64(n)

	return n, nil
}

func (f *File) ReadAt(b []byte, offset int64) (int, error) {
	if f.reader == nil {
		return 0, fmt.Errorf("file not open for reading: %w", fs.ErrClosed)
	}
	return f.reader.ReadAt(b, offset)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	if f.reader == nil {
		return 0, fmt.Errorf("seek only supported for reading: %w", fs.ErrClosed)
	}

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

	return start, f.openReaderAt(context.Background(), start)
}

func (f *File) openReaderAt(ctx context.Context, offset int64) error {
	if f.readerCancelFn != nil {
		f.readerCancelFn()
	}

	if f.reader != nil {
		if err := f.Close(); err != nil {
			return err
		}
	}

	r, w, err := pipeat.PipeInDir(f.fs.tempDir)
	if err != nil {
		return err
	}

	ctx, cancelFn := context.WithCancel(ctx)
	downloader := manager.NewDownloader(f.fs.client, func(d *manager.Downloader) {
		d.Concurrency = 1
		d.PartSize = f.fs.partSize
	})

	var streamRange *string
	if offset > 0 {
		streamRange = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}

	go func() {
		defer cancelFn()

		_, err := downloader.Download(ctx, w, &s3.GetObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.fs.withPrefix(f.Name())),
			Range:  streamRange,
		})
		_ = w.CloseWithError(err)
	}()

	f.offset = offset
	f.reader = r
	f.readerCancelFn = cancelFn

	return nil
}

func (f *File) openWriter(ctx context.Context) error {
	r, w, err := pipeat.PipeInDir(f.fs.tempDir)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	uploader := manager.NewUploader(f.fs.client, func(u *manager.Uploader) {
		u.Concurrency = 1
		u.PartSize = f.fs.partSize
	})

	go func() {
		defer cancel()

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.fs.withPrefix(f.Name())),
			Body:   r,
		})
		_ = r.CloseWithError(err)
	}()

	f.writer = w
	f.writerCancelFn = cancel

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

	if f.readerCancelFn != nil {
		f.readerCancelFn()
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
