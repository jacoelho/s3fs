package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var _ io.WriteCloser = (*Writer)(nil)

type Writer struct {
	ctx         context.Context
	failureErr  error
	terminalErr error
	uploadID    *string
	buf         []byte
	completed   []types.CompletedPart
	cfg         writerConfig
	total       int64
	releaseOnce sync.Once
	mu          sync.Mutex
	nextPart    int32
	state       writerState
}

type writerState uint8

const (
	writerBuffering writerState = iota
	writerMultipart
	writerCompleting
	writerFailed
	writerClosed
)

type writerConfig struct {
	client           s3ApiClient
	operationContext func(context.Context) (context.Context, context.CancelFunc)
	cleanupContext   func() (context.Context, context.CancelFunc)
	release          func()
	bucket           string
	key              string
	path             string
	partSize         int64
	maxWriteSize     int64
}

func newWriter(ctx context.Context, cfg writerConfig) *Writer {
	if cfg.operationContext == nil {
		cfg.operationContext = noTimeoutContext
	}
	if cfg.cleanupContext == nil {
		cfg.cleanupContext = backgroundContext
	}
	if cfg.release == nil {
		cfg.release = func() {}
	}
	return &Writer{
		cfg:      cfg,
		ctx:      ctx,
		nextPart: 1,
		state:    writerBuffering,
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.state {
	case writerClosed, writerCompleting:
		return 0, pathError("write", w.cfg.path, fs.ErrClosed)
	case writerFailed:
		return 0, w.terminalErr
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > w.cfg.maxWriteSize-w.total {
		err := pathError("write", w.cfg.path, ErrWriteTooLarge)
		return 0, w.failLocked(err)
	}

	written := 0
	for len(p) > 0 {
		n, err := w.writeSomeLocked(p)
		if err != nil {
			return written, err
		}
		if n == 0 {
			err := pathError("write", w.cfg.path, io.ErrNoProgress)
			return written, w.failLocked(err)
		}
		written += n
		w.total += int64(n)
		p = p[n:]
	}
	return written, nil
}

func (w *Writer) writeSomeLocked(p []byte) (int, error) {
	if w.state == writerBuffering {
		return w.writeBufferingLocked(p)
	}
	return w.writeMPULocked(p)
}

func (w *Writer) writeBufferingLocked(p []byte) (int, error) {
	if len(w.buf) == 0 && int64(len(p)) > w.cfg.partSize {
		if err := w.startMultipartLocked(); err != nil {
			return 0, w.failLocked(err)
		}
		return w.writeMPULocked(p)
	}
	if int64(len(w.buf)) == w.cfg.partSize {
		if err := w.startMultipartLocked(); err != nil {
			return 0, w.failLocked(err)
		}
		if err := w.flushBufferLocked(); err != nil {
			return 0, w.failLocked(err)
		}
		return w.writeMPULocked(p)
	}

	space := w.cfg.partSize - int64(len(w.buf))
	if int64(len(p)) <= space {
		w.appendPartial(p)
		return len(p), nil
	}

	take := int(space)
	w.appendBuffer(p[:take])
	if err := w.startMultipartLocked(); err != nil {
		return 0, w.failLocked(err)
	}
	if err := w.flushBufferLocked(); err != nil {
		return 0, w.failLocked(err)
	}
	return take, nil
}

func (w *Writer) writeMPULocked(p []byte) (int, error) {
	written := 0
	if len(w.buf) > 0 {
		space := w.cfg.partSize - int64(len(w.buf))
		take := len(p)
		if int64(take) > space {
			take = int(space)
		}
		w.appendBuffer(p[:take])
		p = p[take:]
		if int64(len(w.buf)) < w.cfg.partSize {
			return take, nil
		}
		if err := w.flushBufferLocked(); err != nil {
			return 0, w.failLocked(err)
		}
		written += take
	}

	for int64(len(p)) >= w.cfg.partSize {
		part := p[:w.cfg.partSize]
		if err := w.uploadDirectPartLocked(part); err != nil {
			return written, w.failLocked(err)
		}
		p = p[len(part):]
		written += len(part)
	}

	if len(p) > 0 {
		w.appendPartial(p)
		written += len(p)
	}
	return written, nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.state {
	case writerClosed, writerFailed:
		if w.state == writerFailed && w.uploadID != nil {
			return w.closeFailedLocked()
		}
		return w.terminalErr
	}
	return w.finishLocked()
}

func (w *Writer) finishLocked() error {
	if w.state == writerBuffering {
		if err := w.putObjectLocked(); err != nil {
			return w.failLocked(err)
		}
		w.buf = nil
		w.state = writerClosed
		w.release()
		return nil
	}
	if w.state == writerCompleting {
		return w.completeAndCloseLocked()
	}

	if len(w.buf) > 0 {
		if err := w.flushBufferLocked(); err != nil {
			return w.failLocked(err)
		}
	}

	w.buf = nil
	w.state = writerCompleting
	w.release()
	return w.completeAndCloseLocked()
}

func (w *Writer) completeAndCloseLocked() error {
	if err := w.completeLocked(); err != nil {
		if w.retryComplete(err) {
			return err
		}
		return w.failLocked(err)
	}
	w.uploadID = nil
	w.buf = nil
	w.completed = nil
	w.state = writerClosed
	w.release()
	return nil
}

func (w *Writer) retryComplete(err error) bool {
	if ctxErr := w.ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return false
	}

	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return true
	}
	switch apiErr.ErrorCode() {
	case "AccessDenied", "EntityTooSmall", "InvalidPart", "InvalidPartOrder", "InvalidRequest", "NoSuchUpload":
		return false
	default:
		return true
	}
}

func (w *Writer) appendPartial(p []byte) {
	w.appendBuffer(p)
}

func (w *Writer) appendBuffer(p []byte) {
	need := len(w.buf) + len(p)
	if cap(w.buf) < need {
		next := make([]byte, len(w.buf), nextBufferCap(cap(w.buf), need, w.cfg.partSize))
		copy(next, w.buf)
		w.buf = next
	}
	w.buf = append(w.buf, p...)
}

func nextBufferCap(current, need int, limit int64) int {
	next := need
	if current > 0 && current <= maxInt-current {
		next = max(need, current*2)
	}
	if limit <= int64(maxInt) && int64(next) > limit {
		return int(limit)
	}
	return next
}

func (w *Writer) putObjectLocked() error {
	ctx, cancel := w.cfg.operationContext(w.ctx)
	defer cancel()

	_, err := w.cfg.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.cfg.bucket),
		Key:    aws.String(w.cfg.key),
		Body:   bytes.NewReader(w.buf),
	})
	if err != nil {
		return pathError("close", w.cfg.path, err)
	}
	return nil
}

func (w *Writer) startMultipartLocked() error {
	ctx, cancel := w.cfg.operationContext(w.ctx)
	defer cancel()

	out, err := w.cfg.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(w.cfg.bucket),
		Key:               aws.String(w.cfg.key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return pathError("write", w.cfg.path, err)
	}
	w.uploadID = out.UploadId
	w.state = writerMultipart
	return nil
}

func (w *Writer) flushBufferLocked() error {
	err := w.uploadPartLocked(bytes.NewReader(w.buf), int64(len(w.buf)))
	if err == nil {
		w.buf = w.buf[:0]
	}
	return err
}

func (w *Writer) uploadDirectPartLocked(part []byte) error {
	return w.uploadPartLocked(bytes.NewReader(part), int64(len(part)))
}

func (w *Writer) uploadPartLocked(body io.Reader, size int64) error {
	if w.nextPart > maxUploadParts {
		return pathError("write", w.cfg.path, ErrWriteTooLarge)
	}

	ctx, cancel := w.cfg.operationContext(w.ctx)
	defer cancel()

	partNumber := w.nextPart
	out, err := w.cfg.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:            aws.String(w.cfg.bucket),
		Key:               aws.String(w.cfg.key),
		UploadId:          w.uploadID,
		PartNumber:        aws.Int32(partNumber),
		Body:              body,
		ContentLength:     aws.Int64(size),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		return pathError("write", w.cfg.path, err)
	}

	w.completed = append(w.completed, types.CompletedPart{
		PartNumber:        aws.Int32(partNumber),
		ETag:              out.ETag,
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumSHA512:    out.ChecksumSHA512,
		ChecksumXXHASH128: out.ChecksumXXHASH128,
		ChecksumXXHASH3:   out.ChecksumXXHASH3,
		ChecksumXXHASH64:  out.ChecksumXXHASH64,
	})
	w.nextPart++
	return nil
}

func (w *Writer) completeLocked() error {
	ctx, cancel := w.cfg.operationContext(w.ctx)
	defer cancel()

	_, err := w.cfg.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String(w.cfg.bucket),
		Key:           aws.String(w.cfg.key),
		UploadId:      w.uploadID,
		MpuObjectSize: aws.Int64(w.total),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: w.completed,
		},
	})
	if err != nil {
		return pathError("close", w.cfg.path, err)
	}
	return nil
}

func (w *Writer) failLocked(err error) error {
	if w.state == writerFailed {
		return w.closeFailedLocked()
	}
	w.state = writerFailed
	w.failureErr = err
	w.terminalErr = err
	w.buf = nil
	w.completed = nil
	w.release()
	return w.closeFailedLocked()
}

func (w *Writer) closeFailedLocked() error {
	if w.uploadID != nil {
		if abortErr := w.abortLocked(); abortErr != nil {
			w.terminalErr = errors.Join(w.failureErr, abortErr)
			return w.terminalErr
		}
	}
	w.terminalErr = w.failureErr
	return w.terminalErr
}

func (w *Writer) abortLocked() error {
	ctx, cancel := w.cfg.cleanupContext()
	defer cancel()

	_, err := w.cfg.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.cfg.bucket),
		Key:      aws.String(w.cfg.key),
		UploadId: w.uploadID,
	})
	if err != nil {
		if apiErrorCode(err) == "NoSuchUpload" {
			w.uploadID = nil
			return nil
		}
		return pathError("abort", w.cfg.path, err)
	}
	w.uploadID = nil
	return nil
}

func (w *Writer) release() {
	w.releaseOnce.Do(w.cfg.release)
}

func noTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func backgroundContext() (context.Context, context.CancelFunc) {
	return context.Background(), func() {}
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return ""
	}
	return apiErr.ErrorCode()
}

const maxInt = int(^uint(0) >> 1)
