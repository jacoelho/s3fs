package s3fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestWriterUsesPutObjectUntilPartSizeIsCrossed(t *testing.T) {
	tests := []struct {
		name       string
		body       []byte
		wantPut    bool
		wantUpload int
	}{
		{name: "empty", body: nil, wantPut: true},
		{name: "less than part", body: []byte("abc"), wantPut: true},
		{name: "equal part", body: []byte("abcde"), wantPut: true},
		{name: "cross part", body: []byte("abcdef"), wantUpload: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeS3()
			w := newTestWriter(client, "file", 5, 100, nil)
			if _, err := w.Write(tt.body); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			got, ok := client.object("file")
			if !ok {
				t.Fatal("object missing")
			}
			if !bytes.Equal(got, tt.body) {
				t.Fatalf("body = %q, want %q", got, tt.body)
			}
			if got := len(client.callsFor("PutObject")); (got == 1) != tt.wantPut {
				t.Fatalf("PutObject calls = %d, want put %v", got, tt.wantPut)
			}
			if got := len(client.callsFor("UploadPart")); got != tt.wantUpload {
				t.Fatalf("UploadPart calls = %d, want %d", got, tt.wantUpload)
			}
		})
	}
}

func TestWriterManySmallWrites(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 3, 100, nil)

	for _, chunk := range []string{"a", "bc", "d", "efg", "h"} {
		if _, err := io.WriteString(w, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, _ := client.object("file")
	if string(got) != "abcdefgh" {
		t.Fatalf("body = %q", got)
	}
	if got := len(client.callsFor("UploadPart")); got != 3 {
		t.Fatalf("UploadPart calls = %d, want 3", got)
	}
}

func TestWriterBufferCapacityNeverExceedsPartSize(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 5, 100, nil)

	for range 20 {
		if _, err := w.Write([]byte("a")); err != nil {
			t.Fatal(err)
		}
		if cap(w.buf) > 5 {
			t.Fatalf("buffer cap = %d, want <= 5", cap(w.buf))
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterExactPartThenMoreMakesProgress(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 3, 100, nil)

	n, err := w.Write([]byte("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("first Write() n = %d, want 3", n)
	}
	n, err = w.Write([]byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("second Write() n = %d, want 1", n)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, ok := client.object("file")
	if !ok {
		t.Fatal("object missing")
	}
	if string(got) != "abcd" {
		t.Fatalf("body = %q, want abcd", got)
	}
	if got := len(client.callsFor("UploadPart")); got != 2 {
		t.Fatalf("UploadPart calls = %d, want 2", got)
	}
}

func TestWriterDirectFullPartsFromLargeWrite(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 4, 100, nil)

	n, err := w.Write([]byte("abcdefghijkl"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 12 {
		t.Fatalf("Write() n = %d, want 12", n)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	parts := client.callsFor("UploadPart")
	if len(parts) != 3 {
		t.Fatalf("UploadPart calls = %d, want 3", len(parts))
	}
	for i, part := range parts {
		if part.size != 4 {
			t.Fatalf("part %d size = %d, want 4", i, part.size)
		}
	}
}

func TestWriterMaxWriteSizeRejectsBeforeAcceptingBytes(t *testing.T) {
	client := newFakeS3()
	releases := 0
	w := newTestWriter(client, "file", 5, 5, func() { releases++ })

	n, err := w.Write([]byte("abcdef"))
	if n != 0 {
		t.Fatalf("Write() n = %d, want 0", n)
	}
	if !errors.Is(err, ErrWriteTooLarge) {
		t.Fatalf("Write() error = %v, want ErrWriteTooLarge", err)
	}
	if err := w.Close(); !errors.Is(err, ErrWriteTooLarge) {
		t.Fatalf("Close() error = %v, want ErrWriteTooLarge", err)
	}
	if _, ok := client.object("file"); ok {
		t.Fatal("object stored after cap breach")
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if w.buf != nil {
		t.Fatalf("buffer retained after overflow: len=%d cap=%d", len(w.buf), cap(w.buf))
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, ErrWriteTooLarge) {
		t.Fatalf("Write() after overflow = %v, want ErrWriteTooLarge", err)
	}
}

func TestWriterUploadFailureReturnsAcceptedPrefixAndAborts(t *testing.T) {
	client := newFakeS3()
	uploadErr := errors.New("upload failed")
	client.fail("UploadPart", uploadErr)
	releases := 0
	w := newTestWriter(client, "file", 3, 100, func() { releases++ })
	if _, err := w.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}

	n, err := w.Write([]byte("cdef"))
	if n != 0 {
		t.Fatalf("Write() n = %d, want 0", n)
	}
	if !errors.Is(err, uploadErr) {
		t.Fatalf("Write() error = %v, want upload failure", err)
	}
	if got := len(client.callsFor("AbortMultipartUpload")); got != 1 {
		t.Fatalf("AbortMultipartUpload calls = %d, want 1", got)
	}
	if _, ok := client.object("file"); ok {
		t.Fatal("object stored after failed MPU")
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if w.buf != nil {
		t.Fatalf("buffer retained after upload failure: len=%d cap=%d", len(w.buf), cap(w.buf))
	}
	if err := w.Close(); !errors.Is(err, uploadErr) {
		t.Fatalf("Close() error = %v, want upload failure", err)
	}
}

func TestWriterCompleteFailureCanBeRetried(t *testing.T) {
	client := newFakeS3()
	completeErr := errors.New("complete failed")
	client.fail("CompleteMultipartUpload", completeErr)
	releases := 0
	w := newTestWriter(client, "file", 3, 100, func() { releases++ })
	if _, err := w.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}

	err := w.Close()
	if !errors.Is(err, completeErr) {
		t.Fatalf("Close() error = %v, want complete failed", err)
	}
	if got := len(client.callsFor("AbortMultipartUpload")); got != 0 {
		t.Fatalf("AbortMultipartUpload calls = %d, want 0", got)
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if w.buf != nil {
		t.Fatalf("buffer retained after complete failure: len=%d cap=%d", len(w.buf), cap(w.buf))
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Write() after failed Close = %v, want fs.ErrClosed", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := len(client.callsFor("CompleteMultipartUpload")); got != 2 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 2", got)
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	got, ok := client.object("file")
	if !ok {
		t.Fatal("object missing")
	}
	if string(got) != "abcd" {
		t.Fatalf("body = %q, want abcd", got)
	}
}

func TestWriterCompleteFailureAfterExactPartDropsBuffer(t *testing.T) {
	client := newFakeS3()
	completeErr := errors.New("complete failed")
	client.fail("CompleteMultipartUpload", completeErr)
	releases := 0
	w := newTestWriter(client, "file", 3, 100, func() { releases++ })
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("def")); err != nil {
		t.Fatal(err)
	}
	if len(w.buf) != 0 || cap(w.buf) != 3 {
		t.Fatalf("buffer before Close = len %d cap %d, want 0/3", len(w.buf), cap(w.buf))
	}

	if err := w.Close(); !errors.Is(err, completeErr) {
		t.Fatalf("Close() error = %v, want complete failed", err)
	}
	if w.buf != nil {
		t.Fatalf("buffer retained after complete failure: len=%d cap=%d", len(w.buf), cap(w.buf))
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestWriterPermanentCompleteFailureAbortsAndReleases(t *testing.T) {
	client := newFakeS3()
	completeErr := &smithy.GenericAPIError{Code: "InvalidPart", Message: "invalid part"}
	abortErr := errors.New("abort failed")
	client.fail("CompleteMultipartUpload", completeErr)
	client.fail("AbortMultipartUpload", abortErr)
	f, err := New(client, "bucket", WithMaxActiveWriters(1), WithPartSize(minPartSize), WithMaxWriteSize(minPartSize*2))
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.Create("file")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), int(minPartSize)+1)); err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if !errors.Is(err, completeErr) || !errors.Is(err, abortErr) {
		t.Fatalf("Close() error = %v, want complete and abort errors", err)
	}
	if w.completed != nil {
		t.Fatalf("completed parts retained after permanent complete failure: %d", len(w.completed))
	}
	if _, err := f.Create("next"); err != nil {
		t.Fatalf("Create() after permanent complete failure = %v", err)
	}
}

func TestWriterCanceledCompleteFailureAbortsAndReleases(t *testing.T) {
	client := newFakeS3()
	f, err := New(client, "bucket", WithMaxActiveWriters(1), WithPartSize(minPartSize), WithMaxWriteSize(minPartSize*2))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	w, err := f.CreateWithContext(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("a"), int(minPartSize)+1)); err != nil {
		t.Fatal(err)
	}
	cancel()

	err = w.Close()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want context canceled", err)
	}
	if _, err := f.Create("next"); err != nil {
		t.Fatalf("Create() after canceled complete = %v", err)
	}
	if got := len(client.callsFor("AbortMultipartUpload")); got != 1 {
		t.Fatalf("AbortMultipartUpload calls = %d, want 1", got)
	}
}

func TestWriterAbortFailureCanBeRetried(t *testing.T) {
	client := newFakeS3()
	uploadErr := errors.New("upload failed")
	abortErr := errors.New("abort failed")
	client.fail("UploadPart", uploadErr)
	client.fail("AbortMultipartUpload", abortErr)
	releases := 0
	w := newTestWriter(client, "file", 3, 100, func() { releases++ })
	if _, err := w.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}

	_, err := w.Write([]byte("cdef"))
	if !errors.Is(err, uploadErr) || !errors.Is(err, abortErr) {
		t.Fatalf("Write() error = %v, want upload and abort errors", err)
	}
	if w.uploadID == nil {
		t.Fatal("upload ID cleared after failed abort")
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if w.completed != nil {
		t.Fatalf("completed parts retained after failed abort: %d", len(w.completed))
	}
	if err := w.Close(); !errors.Is(err, uploadErr) {
		t.Fatalf("Close() error = %v, want upload failure", err)
	}
	if w.uploadID != nil {
		t.Fatal("upload ID retained after abort retry")
	}
}

func TestWriterAbortNoSuchUploadIsCleanupSuccess(t *testing.T) {
	client := newFakeS3()
	uploadErr := errors.New("upload failed")
	client.fail("UploadPart", uploadErr)
	client.fail("AbortMultipartUpload", &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"})
	w := newTestWriter(client, "file", 3, 100, nil)
	if _, err := w.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}

	_, err := w.Write([]byte("cdef"))
	if !errors.Is(err, uploadErr) {
		t.Fatalf("Write() error = %v, want upload failure only", err)
	}
	if w.uploadID != nil {
		t.Fatal("upload ID retained after NoSuchUpload abort")
	}
	if err := w.Close(); !errors.Is(err, uploadErr) {
		t.Fatalf("Close() error = %v, want upload failure", err)
	}
	if got := len(client.callsFor("AbortMultipartUpload")); got != 1 {
		t.Fatalf("AbortMultipartUpload calls = %d, want 1", got)
	}
}

func TestWriterPutObjectFailurePreservesErrorAndReleases(t *testing.T) {
	client := newFakeS3()
	putErr := errors.New("put failed")
	client.fail("PutObject", putErr)
	releases := 0
	w := newTestWriter(client, "file", 5, 100, func() { releases++ })
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}

	err := w.Close()
	if !errors.Is(err, putErr) {
		t.Fatalf("Close() error = %v, want put failed", err)
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
	if w.buf != nil {
		t.Fatalf("buffer retained after put failure: len=%d cap=%d", len(w.buf), cap(w.buf))
	}
	if _, ok := client.object("file"); ok {
		t.Fatal("object stored after failed PutObject")
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, putErr) {
		t.Fatalf("Write() after fail = %v, want put failed", err)
	}
}

func TestWriterCloseIsIdempotent(t *testing.T) {
	client := newFakeS3()
	releases := 0
	w := newTestWriter(client, "file", 3, 100, func() { releases++ })
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := len(client.callsFor("PutObject")); got != 1 {
		t.Fatalf("PutObject calls = %d, want 1", got)
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
}

func TestWriterRejectsWriteAfterClose(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 3, 100, nil)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Write() error = %v, want fs.ErrClosed", err)
	}
}

func TestActiveWriterLimit(t *testing.T) {
	client := newFakeS3()
	f, err := New(client, "bucket", WithMaxActiveWriters(1))
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.Create("a")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.Create("b"); !errors.Is(err, ErrTooManyWriters) {
		t.Fatalf("Create() error = %v, want ErrTooManyWriters", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Create("b"); err != nil {
		t.Fatalf("Create() after close = %v", err)
	}
}

func TestCreateDoesNotAllocatePartBuffer(t *testing.T) {
	client := newFakeS3()
	f, err := New(client, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.Create("file")
	if err != nil {
		t.Fatal(err)
	}
	if len(w.buf) != 0 || cap(w.buf) != 0 {
		t.Fatalf("writer buffer len=%d cap=%d, want zero", len(w.buf), cap(w.buf))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterCopiesCompletedPartMetadata(t *testing.T) {
	client := newFakeS3()
	w := newTestWriter(client, "file", 3, 100, nil)
	if _, err := w.Write([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	complete := client.callsFor("CompleteMultipartUpload")
	if len(complete) != 1 {
		t.Fatalf("CompleteMultipartUpload calls = %d, want 1", len(complete))
	}
	if got := complete[0].completed[0].ChecksumCRC32; got == nil {
		t.Fatal("completed part missing checksum")
	}
	if got := complete[0].completed[0].ETag; got == nil {
		t.Fatal("completed part missing etag")
	}
	if got := client.callsFor("CreateMultipartUpload")[0].checksum; got != types.ChecksumAlgorithmCrc32 {
		t.Fatalf("checksum = %q, want crc32", got)
	}
}

func TestNewValidatesWriteLimits(t *testing.T) {
	client := newFakeS3()

	if _, err := New(client, "bucket", WithPartSize(minPartSize-1)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
	if _, err := New(client, "bucket", WithMaxWriteSize(0)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
	if _, err := New(client, "bucket", WithMaxWriteSize(defaultPartSize*maxUploadParts+1)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
	if _, err := New(client, "bucket", WithMaxActiveWriters(0)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
	if _, err := New(client, "bucket", WithMaxActiveWriters(-1)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
}

func FuzzWriter(f *testing.F) {
	f.Add([]byte("abcdef"), []byte{1, 2, 3}, int64(100), -1)
	f.Add([]byte(""), []byte{1}, int64(100), -1)
	f.Add([]byte("abcdef"), []byte{6}, int64(5), -1)
	f.Add([]byte("abcdefghijkl"), []byte{1, 1, 1, 1}, int64(100), 1)

	f.Fuzz(func(t *testing.T, body []byte, splits []byte, capSize int64, failAt int) {
		if len(body) > 256 {
			body = body[:256]
		}
		if capSize < 0 {
			capSize = 0
		}
		if capSize > 512 {
			capSize = 512
		}

		client := newFakeS3()
		if failAt >= 0 {
			for range failAt % 8 {
				client.fail("UploadPart", nil)
			}
			client.fail("UploadPart", &smithy.GenericAPIError{Code: "Injected", Message: "injected"})
		}
		w := newTestWriter(client, "file", 7, max(1, capSize), nil)

		writeErr := writeChunks(w, body, splits)
		closeErr := w.Close()
		stored, ok := client.object("file")

		if writeErr == nil && closeErr == nil {
			if !ok {
				t.Fatal("object missing after successful write")
			}
			if !bytes.Equal(stored, body) {
				t.Fatalf("stored body mismatch")
			}
			return
		}

		if ok {
			t.Fatalf("object stored after failure: write=%v close=%v", writeErr, closeErr)
		}
	})
}

func writeChunks(w io.Writer, body []byte, splits []byte) error {
	if len(splits) == 0 {
		_, err := w.Write(body)
		return err
	}

	for offset, split := 0, 0; offset < len(body); split++ {
		size := int(splits[split%len(splits)]%16) + 1
		end := min(offset+size, len(body))
		if _, err := w.Write(body[offset:end]); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

func newTestWriter(client *fakeS3, key string, partSize, maxWriteSize int64, release func()) *Writer {
	return newWriter(context.Background(), writerConfig{
		client:           client,
		bucket:           "bucket",
		key:              key,
		path:             key,
		partSize:         partSize,
		maxWriteSize:     maxWriteSize,
		operationContext: noTimeoutContext,
		cleanupContext:   backgroundContext,
		release:          release,
	})
}
