package s3fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math"
	"testing"
	"time"
)

func TestFileReadSeekAndReadAt(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("0123456789"), etag: etag([]byte("0123456789"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 2)
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "01" {
		t.Fatalf("Read() = %q", buf)
	}

	seeker := file.(io.Seeker)
	pos, err := seeker.Seek(5, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 5 {
		t.Fatalf("Seek() = %d, want 5", pos)
	}

	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "56789" {
		t.Fatalf("ReadAll() = %q", got)
	}

	readerAt := file.(io.ReaderAt)
	buf = make([]byte, 4)
	if _, err := readerAt.ReadAt(buf, 3); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "3456" {
		t.Fatalf("ReadAt() = %q", buf)
	}

	calls := client.callsFor("GetObject")
	if calls[len(calls)-1].rangeHdr != "bytes=3-6" {
		t.Fatalf("range = %q, want bytes=3-6", calls[len(calls)-1].rangeHdr)
	}
	for i, call := range calls {
		if call.ifMatch == "" {
			t.Fatalf("GetObject call %d missing IfMatch", i)
		}
	}
}

func TestFileReadAtShortReadReturnsEOF(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 5)
	n, err := file.(io.ReaderAt).ReadAt(buf, 1)
	if n != 2 {
		t.Fatalf("ReadAt() n = %d, want 2", n)
	}
	if err != io.EOF {
		t.Fatalf("ReadAt() error = %v, want io.EOF", err)
	}
}

func TestFileReadKeepsOperationContextUntilBodyClose(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	client.readCtx = true
	fsys := newTestFS(t, client, WithTimeout(time.Minute))

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 1)
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(buf) != "a" {
		t.Fatalf("Read() = %q, want a", buf)
	}
}

func TestFileWrapsBodyReadErrors(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	boom := errors.New("boom")
	client.readErr = boom
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	_, err = file.Read(make([]byte, 1))
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("Read() error = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "read" || !errors.Is(pathErr.Err, boom) {
		t.Fatalf("Read() error = %v, want read boom", err)
	}

	_, err = file.(io.ReaderAt).ReadAt(make([]byte, 1), 0)
	if !errors.As(err, &pathErr) {
		t.Fatalf("ReadAt() error = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "readat" || !errors.Is(pathErr.Err, boom) {
		t.Fatalf("ReadAt() error = %v, want readat boom", err)
	}
}

func TestFileReadAfterBodyErrorRepeatsError(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	boom := errors.New("boom")
	client.readErr = boom
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, boom) {
		t.Fatalf("first Read() error = %v, want boom", err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, boom) {
		t.Fatalf("second Read() error = %v, want boom", err)
	}
}

func TestFileSeekCurrentZeroDoesNotFetch(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 1)
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Fatal(err)
	}
	calls := len(client.callsFor("GetObject"))
	client.fail("GetObject", errors.New("boom"))

	pos, err := file.(io.Seeker).Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 1 {
		t.Fatalf("Seek() = %d, want 1", pos)
	}
	if got := len(client.callsFor("GetObject")); got != calls {
		t.Fatalf("GetObject calls = %d, want %d", got, calls)
	}
}

func TestFileSeekCurrentZeroReopensAfterReadError(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	boom := errors.New("boom")
	client.readErr = boom
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, boom) {
		t.Fatalf("Read() error = %v, want boom", err)
	}
	client.readErr = nil

	pos, err := file.(io.Seeker).Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Fatalf("Seek() = %d, want 0", pos)
	}

	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abc" {
		t.Fatalf("ReadAll() = %q, want abc", got)
	}
}

func TestFileSeekFailurePreservesCurrentStream(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 1)
	if _, err := io.ReadFull(file, buf); err != nil {
		t.Fatal(err)
	}

	client.fail("GetObject", errors.New("boom"))
	_, err = file.(io.Seeker).Seek(2, io.SeekStart)
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("Seek() error = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "seek" {
		t.Fatalf("PathError.Op = %q, want seek", pathErr.Op)
	}

	rest, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != "bc" {
		t.Fatalf("ReadAll() after failed Seek = %q, want bc", rest)
	}
}

func TestFileRejectsInvalidSeekAndClosedRead(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.(io.Seeker).Seek(-1, io.SeekStart); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Seek() error = %v, want fs.ErrInvalid", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Read() error = %v, want fs.ErrClosed", err)
	}
	if _, err := file.(io.ReaderAt).ReadAt(make([]byte, 1), 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("ReadAt() error = %v, want fs.ErrClosed", err)
	}
	if _, err := file.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Stat() error = %v, want fs.ErrClosed", err)
	}
}

func TestFileRejectsSeekOverflow(t *testing.T) {
	file := &File{
		info:   regularFileInfo("file", "file", math.MaxInt64, time.Time{}, ""),
		offset: math.MaxInt64,
		body:   io.NopCloser(bytes.NewReader(nil)),
	}

	if _, err := file.Seek(1, io.SeekCurrent); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("SeekCurrent overflow error = %v, want fs.ErrInvalid", err)
	}
	if _, err := file.Seek(1, io.SeekEnd); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("SeekEnd overflow error = %v, want fs.ErrInvalid", err)
	}
}

func TestReadClosesBodies(t *testing.T) {
	client := newFakeS3()
	client.objects["file"] = fakeObject{body: []byte("abc"), etag: etag([]byte("abc"))}
	fsys := newTestFS(t, client)

	file, err := fsys.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(file); err != nil || string(got) != "abc" {
		t.Fatalf("ReadAll() = %q, %v", got, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if client.closes != 1 {
		t.Fatalf("body closes = %d, want 1", client.closes)
	}
}

func FuzzReader(f *testing.F) {
	f.Add([]byte("abcdef"), []byte{0, 1, 2, 3})
	f.Add([]byte(""), []byte{0})

	f.Fuzz(func(t *testing.T, body []byte, ops []byte) {
		if len(body) > 256 {
			body = body[:256]
		}
		client := newFakeS3()
		client.objects["file"] = fakeObject{body: append([]byte(nil), body...), etag: etag(body)}
		fsys := newTestFS(t, client)

		file, err := fsys.Open("file")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()

		want := bytes.NewReader(body)
		for _, op := range ops {
			switch op % 3 {
			case 0:
				size := int(op % 17)
				gotBuf := make([]byte, size)
				wantBuf := make([]byte, size)
				gotN, gotErr := file.Read(gotBuf)
				wantN, wantErr := want.Read(wantBuf)
				if gotN != wantN || !sameReadErr(gotErr, wantErr) || !bytes.Equal(gotBuf[:gotN], wantBuf[:wantN]) {
					t.Fatalf("Read mismatch: got %d %v %q want %d %v %q", gotN, gotErr, gotBuf[:gotN], wantN, wantErr, wantBuf[:wantN])
				}
			case 1:
				if len(body) == 0 {
					continue
				}
				off := int64(op) % int64(len(body))
				size := int(op%17) + 1
				gotBuf := make([]byte, size)
				wantBuf := make([]byte, size)
				gotN, gotErr := file.(io.ReaderAt).ReadAt(gotBuf, off)
				wantN, wantErr := want.ReadAt(wantBuf, off)
				if gotN != wantN || !sameReadErr(gotErr, wantErr) || !bytes.Equal(gotBuf[:gotN], wantBuf[:wantN]) {
					t.Fatalf("ReadAt mismatch")
				}
			case 2:
				off := int64(0)
				if len(body) > 0 {
					off = int64(op) % int64(len(body)+1)
				}
				gotPos, gotErr := file.(io.Seeker).Seek(off, io.SeekStart)
				wantPos, wantErr := want.Seek(off, io.SeekStart)
				if gotPos != wantPos || !sameReadErr(gotErr, wantErr) {
					t.Fatalf("Seek mismatch: got %d %v want %d %v", gotPos, gotErr, wantPos, wantErr)
				}
			}
		}
	})
}

func sameReadErr(got, want error) bool {
	if got == nil || want == nil {
		return got == want
	}
	return errors.Is(got, want)
}
