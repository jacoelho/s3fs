package tests

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jacoelho/s3fs"
)

func TestDirectoryCreate(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	dir, err := fsClient.CreateDir("some-directory")
	require.NoError(t, err)
	assert.Equal(t, "some-directory", dir.Name())
	assert.True(t, dir.IsDir())

	info, err := dir.Info()
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, int64(0), info.Size())
	assert.Equal(t, fs.ModeDir, info.Mode())
}

func TestDirectoryCreateTwice(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("some-directory")
	require.NoError(t, err)

	_, err = fsClient.CreateDir("some-directory")
	require.ErrorIs(t, err, fs.ErrExist)
}

func TestDirectoryReadNonExisting(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	files, err := fs.ReadDir(fsClient, "some-directory")
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestDirectoryReadEmpty(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")
	_, err := fsClient.CreateDir("some-directory")
	require.NoError(t, err)

	entries, err := fs.ReadDir(fsClient, "some-directory")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".", entries[0].Name())
	require.True(t, entries[0].IsDir())
}

func TestDirectoryReadEmptyCustomDirectoryFile(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test", s3fs.WithDirectoryFile(".dir"))
	_, err := fsClient.CreateDir("some-directory")
	require.NoError(t, err)

	entries, err := fs.ReadDir(fsClient, "some-directory")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".", entries[0].Name())
	require.True(t, entries[0].IsDir())
}

func TestDirectoryReadReturnsAnError(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "some-directory/test.txt", strings.NewReader(""))

	fsClient := s3fs.New(client, "test")
	dir, err := fsClient.Open("some-directory")
	require.NoError(t, err)

	_, err = dir.Read(make([]byte, 100))
	require.Error(t, err)
}

func TestDirectoryRead(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/test.txt", strings.NewReader(""))
	createObject(t, "test", "some-directory/a/test.txt", strings.NewReader(""))
	createObject(t, "test", "some-directory/b/test.txt", strings.NewReader(""))
	createObject(t, "test", "other-directory/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	entries, err := fs.ReadDir(fsClient, "some-directory")
	require.NoError(t, err)

	expected := []struct {
		name  string
		isDir bool
	}{
		{name: ".", isDir: true},
		{name: "a", isDir: true},
		{name: "b", isDir: true},
		{name: "test.txt", isDir: false},
	}

	require.Len(t, entries, len(expected))

	for i, want := range expected {
		require.Equal(t, want.name, entries[i].Name())
		require.Equal(t, want.isDir, entries[i].IsDir())
	}
}

func TestDirectoryReadCurrent(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/a/b/c/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	entries, err := fs.ReadDir(fsClient, ".")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, ".", entries[0].Name())
	assert.Equal(t, "some-directory", entries[1].Name())
}

func TestDirectoryReadNestedObject(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/a/b/c/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	entries, err := fs.ReadDir(fsClient, "some-directory/a/b/c")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, ".", entries[0].Name())
	assert.Equal(t, "test.txt", entries[1].Name())
}

func TestDirectoryReadNestedObjectWithPrefix(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/a/b/c/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test", s3fs.WithPrefix("some-directory/a/b"))

	entries, err := fs.ReadDir(fsClient, "c")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, ".", entries[0].Name())
	assert.Equal(t, "test.txt", entries[1].Name())
}

func TestDirectoryReadIsFile(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	_, err := fs.ReadDir(fsClient, "test.txt")
	require.ErrorIs(t, err, fs.ErrInvalid)
}

func TestDirectoryCreateIsFile(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("test.txt")
	require.ErrorIs(t, err, fs.ErrExist)
}

func TestDirectoryRename(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "a/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	err := fsClient.Rename("a", "b")
	require.ErrorIs(t, err, fs.ErrInvalid)
}

func TestDirectoryRemove(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "a/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	err := fsClient.Remove("a")
	require.ErrorIs(t, err, fs.ErrInvalid)
}

func TestDirectoryCreatedNestedCanBeListed(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("/a/b/c/d/e")
	require.NoError(t, err)

	entries, err := fsClient.ReadDir("a/b/c/d/")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, ".", entries[0].Name())
	require.True(t, entries[0].IsDir())
	require.Equal(t, "e", entries[1].Name())
	require.True(t, entries[1].IsDir())
}

func TestDirectoryCreatedNestedCanBeStat(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("/a/b/c/d/e")
	require.NoError(t, err)

	entries, err := fsClient.ReadDir("a/b/c/d/e")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ".", entries[0].Name())
	require.True(t, entries[0].IsDir())
}

func TestDirectoryRemoveDir(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("a")
	require.NoError(t, err)

	err = fsClient.RemoveDir("a")
	require.NoError(t, err)
}

func TestDirectoryRemoveDirNested(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fsClient.CreateDir("a/b/c/d")
	require.NoError(t, err)

	err = fsClient.RemoveDir("a/b/c/d")
	require.NoError(t, err)
}

func TestDirectoryRemoveDirNotEmpty(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "a/test.txt", strings.NewReader(""))

	fsClient := s3fs.New(client, "test")

	err := fsClient.RemoveDir("a")
	require.ErrorIs(t, err, fs.ErrInvalid)
}

func TestDirectoryRemoveDirFile(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "a/test.txt", strings.NewReader(""))

	fsClient := s3fs.New(client, "test")

	err := fsClient.RemoveDir("a/test.txt")
	require.ErrorIs(t, err, fs.ErrInvalid)
}
