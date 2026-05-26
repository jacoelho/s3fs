package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

func TestReadDirProjectsFilesAndDirectories(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/file.txt", "file")
	putFake(client, "dir/a/nested.txt", "a")
	putFake(client, "dir/b/nested.txt", "b")
	putFake(client, "dir/.keep", "")
	putFake(client, "other/file.txt", "")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir("dir")
	if err != nil {
		t.Fatal(err)
	}

	want := []struct {
		name string
		dir  bool
	}{
		{name: "a", dir: true},
		{name: "b", dir: true},
		{name: "file.txt"},
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %d, want %d: %#v", len(entries), len(want), entries)
	}
	for i, want := range want {
		if entries[i].Name() != want.name || entries[i].IsDir() != want.dir {
			t.Fatalf("entry %d = %s dir=%v, want %s dir=%v", i, entries[i].Name(), entries[i].IsDir(), want.name, want.dir)
		}
	}
}

func TestReadDirDoesNotReturnDot(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/.keep", "")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir("dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %v, want empty", entries)
	}
}

func TestOpenDirectoryReadDirLoadsEntriesLazily(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/file.txt", "file")
	putFake(client, "dir/sub/nested.txt", "nested")
	fsys := newTestFS(t, client)

	file, err := fsys.Open("dir")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	if got := len(client.callsFor("ListObjectsV2")); got != 1 {
		t.Fatalf("Open list calls = %d, want 1", got)
	}

	entries, err := file.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(client.callsFor("ListObjectsV2")); got != 2 {
		t.Fatalf("ReadDir list calls = %d, want 2", got)
	}
	if len(entries) != 2 || entries[0].Name() != "file.txt" || entries[1].Name() != "sub" {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestDirectoryReadDirUsesBackgroundAfterOpenContextIsCanceled(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/file.txt", "file")
	fsys := newTestFS(t, client)

	ctx, cancel := context.WithCancel(t.Context())
	file, err := fsys.OpenWithContext(ctx, "dir")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	cancel()

	entries, err := file.(fs.ReadDirFile).ReadDir(-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "file.txt" {
		t.Fatalf("entries = %#v", entries)
	}
}

func TestDirectoryWinsOverObjectShadow(t *testing.T) {
	client := newFakeS3()
	putFake(client, "a", "file")
	putFake(client, "a/b", "child")
	fsys := newTestFS(t, client)

	info, err := fsys.Stat("a")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("Stat(a).IsDir() = false, want true")
	}
}

func TestDirectoryFileInfoHasStableZeroModTime(t *testing.T) {
	info := directoryFileInfo("a", "a")
	if !info.ModTime().IsZero() {
		t.Fatalf("ModTime() = %v, want zero", info.ModTime())
	}
	if info.Name() != "a" || !info.IsDir() || info.Mode()&fs.ModeDir == 0 || info.Type() != fs.ModeDir {
		t.Fatalf("directory info = name %q mode %v type %v", info.Name(), info.Mode(), info.Type())
	}
	if info.Size() != 0 || info.Sys() != nil {
		t.Fatalf("directory info size/sys = %d/%v, want zero/nil", info.Size(), info.Sys())
	}
}

func TestDirectoryFileInterface(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/file.txt", "file")
	fsys := newTestFS(t, client)

	file, err := fsys.Open("dir")
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Name() != "dir" {
		t.Fatalf("Stat() = %q dir=%v, want dir", info.Name(), info.IsDir())
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Read() error = %v, want fs.ErrInvalid", err)
	}

	entries, err := file.(fs.ReadDirFile).ReadDir(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("ReadDir(1) entries = %d, want 1", len(entries))
	}
	if entries[0].Type() != 0 {
		t.Fatalf("entry Type() = %v, want regular file", entries[0].Type())
	}
	entryInfo, err := entries[0].Info()
	if err != nil {
		t.Fatal(err)
	}
	if entryInfo.Name() != "file.txt" || entryInfo.Size() != 4 {
		t.Fatalf("entry Info() = %q size=%d", entryInfo.Name(), entryInfo.Size())
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Stat() after Close = %v, want fs.ErrClosed", err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("Read() after Close = %v, want fs.ErrClosed", err)
	}
	if _, err := file.(fs.ReadDirFile).ReadDir(1); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("ReadDir() after Close = %v, want fs.ErrClosed", err)
	}
}

func TestCustomDirectoryFileMarker(t *testing.T) {
	client := newFakeS3()
	fsys := newTestFS(t, client, WithDirectoryFile("_dir"))

	if err := fsys.CreateDir("dir"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.object("dir/_dir"); !ok {
		t.Fatal("custom marker missing")
	}
	if _, ok := client.object("dir/.keep"); ok {
		t.Fatal("default marker created with custom marker")
	}

	entries, err := fsys.ReadDir("dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %#v, want custom marker hidden", entries)
	}
}

func TestDirectoryWinsOverObjectShadowAcrossPages(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	putFake(client, "a", "file")
	putFake(client, "a/b", "child")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "a" || !entries[0].IsDir() {
		t.Fatalf("entries = %#v, want directory a", entries)
	}
}

func TestReadDirFollowsPagination(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	putFake(client, "dir/b.txt", "b")
	putFake(client, "dir/a.txt", "a")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir("dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name() != "a.txt" || entries[1].Name() != "b.txt" {
		t.Fatalf("entries = %#v", entries)
	}
	if got := len(client.callsFor("ListObjectsV2")); got != 2 {
		t.Fatalf("ListObjectsV2 calls = %d, want 2", got)
	}
}

func TestDirectoryReadDirNDoesNotLoadAllPages(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	putFake(client, "a.txt", "")
	putFake(client, "b.txt", "")
	putFake(client, "c.txt", "")
	fsys := newTestFS(t, client)

	file, err := fsys.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	entries, err := file.(fs.ReadDirFile).ReadDir(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Fatalf("entries = %#v, want a.txt", entries)
	}
	if got := len(client.callsFor("ListObjectsV2")); got != 2 {
		t.Fatalf("ListObjectsV2 calls = %d, want 2", got)
	}
}

func TestDirectoryReadDirLargeNDoesNotPreallocateN(t *testing.T) {
	client := newFakeS3()
	for i := range maxReadDirBatch + 1 {
		putFake(client, fmt.Sprintf("%03d.txt", i), "")
	}
	fsys := newTestFS(t, client)

	file, err := fsys.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	entries, err := file.(fs.ReadDirFile).ReadDir(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != maxReadDirBatch {
		t.Fatalf("entries = %d, want %d", len(entries), maxReadDirBatch)
	}
	if cap(entries) > maxReadDirBatch {
		t.Fatalf("entries cap = %d, want <= %d", cap(entries), maxReadDirBatch)
	}

	entries, err = file.(fs.ReadDirFile).ReadDir(1 << 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "064.txt" {
		t.Fatalf("entries = %#v, want 064.txt", entries)
	}
}

func TestDirectoryReadDirNDirectoryWinsOverObjectShadowAcrossPages(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	putFake(client, "a", "file")
	putFake(client, "a/b", "child")
	putFake(client, "b", "file")
	fsys := newTestFS(t, client)

	file, err := fsys.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	entries, err := file.(fs.ReadDirFile).ReadDir(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "a" || !entries[0].IsDir() {
		t.Fatalf("entries = %#v, want directory a", entries)
	}
}

func TestDirectoryReadDirAllReturnsPartialEntriesOnPageError(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 2
	putFake(client, "a.txt", "")
	putFake(client, "b.txt", "")
	putFake(client, "c.txt", "")
	boom := errors.New("boom")
	client.fail("ListObjectsV2", nil)
	client.fail("ListObjectsV2", boom)
	fsys := newTestFS(t, client)

	file, err := fsys.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	entries, err := file.(fs.ReadDirFile).ReadDir(-1)
	if !errors.Is(err, boom) {
		t.Fatalf("ReadDir() error = %v, want boom", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Fatalf("entries = %#v, want partial a.txt", entries)
	}
}

func TestRemoveDirSeesNonEmptyDirectoryAcrossPages(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	putFake(client, "a/.keep", "")
	putFake(client, "a/file.txt", "")
	fsys := newTestFS(t, client)

	if err := fsys.RemoveDir("a"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("RemoveDir() error = %v, want fs.ErrInvalid", err)
	}
	if _, ok := client.object("a/.keep"); !ok {
		t.Fatal("marker removed from non-empty directory")
	}
}

func TestInvalidUTF8ListEntryIsHiddenAndBlocksRemoveDir(t *testing.T) {
	client := newFakeS3()
	putFake(client, "a/"+string([]byte{0xff}), "hidden")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %v, want empty", entries)
	}
	if err := fsys.RemoveDir("a"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("RemoveDir() error = %v, want fs.ErrInvalid", err)
	}
}

func TestHiddenInvalidChildrenBlockRemoveDir(t *testing.T) {
	client := newFakeS3()
	putFake(client, "a/\n", "hidden")
	fsys := newTestFS(t, client)

	entries, err := fsys.ReadDir("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %v, want empty", entries)
	}
	if err := fsys.RemoveDir("a"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("RemoveDir() error = %v, want fs.ErrInvalid", err)
	}
}

func TestCreateDirAndRemoveDirUseMarker(t *testing.T) {
	client := newFakeS3()
	fsys := newTestFS(t, client)

	if err := fsys.CreateDir("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.object("a/.keep"); !ok {
		t.Fatal("marker missing")
	}
	if err := fsys.RemoveDir("a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.object("a/.keep"); ok {
		t.Fatal("marker still present")
	}
}

func TestRemoveDistinguishesFileAndDirectory(t *testing.T) {
	client := newFakeS3()
	putFake(client, "a/file.txt", "")
	putFake(client, "file.txt", "")
	fsys := newTestFS(t, client)

	if err := fsys.Remove("a"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("Remove(dir) error = %v, want fs.ErrInvalid", err)
	}
	if err := fsys.Remove("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Remove(missing) error = %v, want fs.ErrNotExist", err)
	}
	if err := fsys.Remove("file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.object("file.txt"); ok {
		t.Fatal("file still present")
	}
}

func TestReadDirAndRemoveDirDistinguishFileAndMissing(t *testing.T) {
	client := newFakeS3()
	putFake(client, "file.txt", "file")
	fsys := newTestFS(t, client)

	if _, err := fsys.ReadDir("file.txt"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("ReadDir(file) error = %v, want fs.ErrInvalid", err)
	}
	if _, err := fsys.ReadDir("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadDir(missing) error = %v, want fs.ErrNotExist", err)
	}
	if err := fsys.RemoveDir("file.txt"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("RemoveDir(file) error = %v, want fs.ErrInvalid", err)
	}
	if err := fsys.RemoveDir("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("RemoveDir(missing) error = %v, want fs.ErrNotExist", err)
	}
}

func TestOpenMissingPathErrorUsesOpenOp(t *testing.T) {
	client := newFakeS3()
	fsys := newTestFS(t, client)

	_, err := fsys.Open("missing")
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("Open() error = %T, want *fs.PathError", err)
	}
	if pathErr.Op != "open" {
		t.Fatalf("PathError.Op = %q, want open", pathErr.Op)
	}
}

func TestDirExistsAndEmptyScanStopEarly(t *testing.T) {
	client := newFakeS3()
	client.pageSize = 1
	for i := range 100 {
		putFake(client, "dir/file-"+string(rune('a'+i%26)), "")
	}
	fsys := newTestFS(t, client)
	p, err := normalizePath("dir", fsys.directoryFile, true)
	if err != nil {
		t.Fatal(err)
	}

	exists, err := fsys.dirExists(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("dirExists() = false, want true")
	}
	if got := len(client.callsFor("ListObjectsV2")); got != 1 {
		t.Fatalf("dirExists ListObjectsV2 calls = %d, want 1", got)
	}
	state, err := fsys.emptyDirMarkers(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if state.empty {
		t.Fatal("emptyDirMarkers reported non-empty directory as empty")
	}
	if got := len(client.callsFor("ListObjectsV2")); got != 2 {
		t.Fatalf("emptyDirMarkers ListObjectsV2 calls = %d, want total 2", got)
	}
	if len(state.markers) != 0 {
		t.Fatalf("markers = %v, want none for non-empty early stop", state.markers)
	}
}

func TestPrefixCannotEscape(t *testing.T) {
	client := newFakeS3()
	if _, err := New(client, "bucket", WithPrefix("../x")); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("New() error = %v, want fs.ErrInvalid", err)
	}
}

func FuzzListProjection(f *testing.F) {
	f.Add("a/b/c.txt")
	f.Add("a/%2F")
	f.Add("a/\n")
	f.Add("a/.keep")

	f.Fuzz(func(t *testing.T, key string) {
		if len(key) > 64 {
			key = key[:64]
		}
		client := newFakeS3()
		putFake(client, key, "x")
		fsys := newTestFS(t, client)

		entries, err := fsys.ReadDir(".")
		if err != nil {
			t.Skip()
		}
		for _, entry := range entries {
			if !validEntryName(entry.Name(), defaultDirectoryFile) {
				t.Fatalf("invalid entry name %q from key %q", entry.Name(), key)
			}
		}
	})
}

func putFake(client *fakeS3, key, body string) {
	client.objects[key] = fakeObject{body: []byte(body), etag: etag([]byte(body))}
}
