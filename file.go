package s3fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	transfertypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eikenb/pipeat"
)

var (
	_ fs.File        = (*File)(nil)
	_ fs.DirEntry    = (*File)(nil)
	_ writerCloserAt = (*File)(nil)
)

type File struct {
	reader         readerCloserAt
	writer         writerCloserAt
	fs             *Fs
	readerCancelFn context.CancelFunc
	writerCancelFn context.CancelFunc
	info           FileInfo
	offset         int64
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
		start = f.info.Size() + offset

	default:
		return 0, &fs.PathError{Op: "seek", Path: f.info.name, Err: fs.ErrInvalid}
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
	transferClient := f.transferClient()

	go func() {
		defer cancelFn()

		err := f.downloadAt(ctx, transferClient, w, offset)
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
	transferClient := f.transferClient()

	go func() {
		defer cancel()

		_, err := transferClient.UploadObject(ctx, &transfermanager.UploadObjectInput{
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

func (f *File) transferClient() *transfermanager.Client {
	return transfermanager.New(f.fs.client, func(o *transfermanager.Options) {
		o.Concurrency = 1
		o.PartSizeBytes = f.fs.partSize
		o.MultipartUploadThreshold = f.fs.partSize
		o.GetObjectType = transfertypes.GetObjectRanges
	})
}

func (f *File) downloadAt(ctx context.Context, client *transfermanager.Client, w io.WriterAt, offset int64) error {
	if offset >= f.info.Size() {
		return nil
	}

	if offset == 0 {
		_, err := client.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
			Bucket:   aws.String(f.fs.bucket),
			Key:      aws.String(f.fs.withPrefix(f.Name())),
			WriterAt: w,
		})
		return err
	}

	writer, ok := w.(io.Writer)
	if !ok {
		return fmt.Errorf("range download writer missing io.Writer: %w", fs.ErrInvalid)
	}

	res, err := f.fs.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.fs.withPrefix(f.Name())),
		Range:  aws.String(fmt.Sprintf("bytes=%d-", offset)),
	})
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, res.Body)
	if closeErr := res.Body.Close(); err == nil {
		err = closeErr
	}
	return err
}

// Write implements io.Writer interface.
func (f *File) Write(p []byte) (n int, err error) {
	if f.writer == nil {
		return 0, fmt.Errorf("file not open for writing: %w", fs.ErrClosed)
	}
	return f.writer.Write(p)
}

// WriteAt implements io.WriterAt interface.
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	if f.writer == nil {
		return 0, fmt.Errorf("file not open for writing: %w", fs.ErrClosed)
	}
	return f.writer.WriteAt(p, off)
}

// Close implements io.Closer interface.
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
