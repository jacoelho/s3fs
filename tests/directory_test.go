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
	require.NoError(t, err)
}

func TestDirectoryReadNonExisting(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")

	_, err := fs.ReadDir(fsClient, "some-directory")
	require.ErrorIs(t, err, s3fs.ErrKeyNotFound)
}

func TestDirectoryReadEmpty(t *testing.T) {
	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")
	_, err := fsClient.CreateDir("some-directory")
	require.NoError(t, err)

	entries, err := fs.ReadDir(fsClient, "some-directory")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, ".keep", entries[0].Name())
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
	require.Len(t, entries, 3)

	assert.Equal(t, "a", entries[0].Name())
	assert.True(t, entries[0].IsDir())

	assert.Equal(t, "b", entries[1].Name())
	assert.True(t, entries[1].IsDir())

	assert.Equal(t, "test.txt", entries[2].Name())
	assert.False(t, entries[2].IsDir())
}

func TestDirectoryReadNestedObject(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/a/b/c/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	entries, err := fs.ReadDir(fsClient, "some-directory/a/b/c")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "test.txt", entries[0].Name())
}

func TestDirectoryReadNestedObjectWithPrefix(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "some-directory/a/b/c/test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test", s3fs.WithPrefix("some-directory/a/b"))

	entries, err := fs.ReadDir(fsClient, "c")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "test.txt", entries[0].Name())
}

func TestDirectoryReadIsFile(t *testing.T) {
	createBucket(t, "test")

	createObject(t, "test", "test.txt", strings.NewReader(""))
	fsClient := s3fs.New(client, "test")

	_, err := fs.ReadDir(fsClient, "test.txt")
	require.ErrorIs(t, err, s3fs.ErrNotDirectory)
}
