package tests

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jacoelho/s3fs/v2"
)

// a bit arbitrary value
const memoryLimit = 90 * 1024 * 1024

func TestFileRead(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	fileSizes := []int64{
		5 * 1024 * 1024,
		50 * 1024 * 1024,
		256 * 1024 * 1024,
	}

	createBucket(t, "test")
	fsClient := newFS(t)

	for i, tc := range fileSizes {
		t.Run(fmt.Sprintf("file size %d", tc), func(t *testing.T) {
			runtime.GC()

			fileName := fmt.Sprintf("file_read_%0d.txt", i)

			sum := createObjectRandomContentsWithSize(t, "test", fileName, tc)

			f, err := fsClient.Open(fileName)
			require.NoError(t, err)

			assert.Equal(t, sum, sha256sum(t, f))
			assert.NoError(t, f.Close())

			runtime.GC()

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			assert.Truef(t, m.Alloc <= memoryLimit, "got %dmb, want %dmb", m.Alloc/1024/1024, memoryLimit/1024/1024)
		})
	}
}

func TestFileReadChunks(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	size := int64(256 * 1024 * 1024)
	chunkSize := 10 * 1024 * 1024

	createBucket(t, "test")

	fsClient := newFS(t)
	checksumSource := createObjectRandomContentsWithSize(t, "test", "file", size)
	source, err := fsClient.Open("file")
	require.NoError(t, err)
	sourceAt, ok := source.(io.ReaderAt)
	require.True(t, ok)

	dst, err := os.Create(filepath.Join(t.TempDir(), "file"))
	require.NoError(t, err)

	chunks := calculateChunks(size, int64(chunkSize))

	var wg sync.WaitGroup
	errs := make(chan error, len(chunks))

	for i, c := range chunks {
		wg.Go(func() {
			buf := make([]byte, c)
			_, err := sourceAt.ReadAt(buf, int64(i*chunkSize))
			if err != nil {
				errs <- fmt.Errorf("read chunk %d: %w", i, err)
				return
			}

			_, err = dst.WriteAt(buf, int64(i*chunkSize))
			if err != nil {
				errs <- fmt.Errorf("write chunk %d: %w", i, err)
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	checksumDestination := fileChecksum(t, dst)
	assert.Equal(t, checksumSource, checksumDestination)
	assert.NoError(t, dst.Close())
}

func TestFileWrite(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	fileSizes := []int64{
		5 * 1024 * 1024,
		50 * 1024 * 1024,
		256 * 1024 * 1024,
	}

	createBucket(t, "test")
	fsClient := newFS(t)

	for i, tc := range fileSizes {
		t.Run(fmt.Sprintf("file size %d", tc), func(t *testing.T) {
			runtime.GC()

			fileName := fmt.Sprintf("file_write_%0d.txt", i)

			sourceFile, checksum := createFileWithSize(t, tc)

			f, err := fsClient.Create(fileName)
			require.NoError(t, err)

			_, err = io.Copy(f, sourceFile)
			require.NoError(t, err)
			assert.NoError(t, sourceFile.Close())
			assert.NoError(t, f.Close())
			assert.Equal(t, checksum, objectChecksum(t, "test", fileName))

			runtime.GC()

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			assert.Truef(t, m.Alloc < memoryLimit, "got %dmb, want %dmb", m.Alloc/1024/1024, memoryLimit/1024/1024)
		})
	}
}

func TestFileWriteChunksSequential(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	fileSize := int64(256 * 1024 * 1024)
	chunkSize := 10 * 1024 * 1024
	sourceAt, checksumSource := createFileWithSize(t, fileSize)

	createBucket(t, "test")
	fsClient := newFS(t)
	destination, err := fsClient.Create("file")
	require.NoError(t, err)

	chunks := calculateChunks(fileSize, int64(chunkSize))

	for i, c := range chunks {
		buf := make([]byte, c)
		_, err := sourceAt.ReadAt(buf, int64(i*chunkSize))
		require.NoError(t, err)
		_, err = destination.Write(buf)
		require.NoError(t, err)
	}

	require.NoError(t, destination.Close())

	checksumDestination := objectChecksum(t, "test", "file")
	assert.Equal(t, checksumSource, checksumDestination)
	assert.NoError(t, sourceAt.Close())
}

func TestFileCreateReturnsWriteOnlyFile(t *testing.T) {
	createBucket(t, "test")
	fsClient := newFS(t)
	destination, err := fsClient.Create("file")
	require.NoError(t, err)

	_, ok := any(destination).(io.Reader)
	require.False(t, ok)
	_, ok = any(destination).(io.ReaderAt)
	require.False(t, ok)
}

func TestFileSeek(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "file", strings.NewReader("0123456789"))
	fsClient := newFS(t)

	tests := []struct {
		name       string
		beforeRead int
		offset     int64
		whence     int
		wantPos    int64
		want       string
	}{
		{
			name:    "start",
			offset:  4,
			whence:  io.SeekStart,
			wantPos: 4,
			want:    "456789",
		},
		{
			name:       "current",
			beforeRead: 2,
			offset:     3,
			whence:     io.SeekCurrent,
			wantPos:    5,
			want:       "56789",
		},
		{
			name:    "end negative offset",
			offset:  -3,
			whence:  io.SeekEnd,
			wantPos: 7,
			want:    "789",
		},
		{
			name:    "end",
			offset:  0,
			whence:  io.SeekEnd,
			wantPos: 10,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := fsClient.Open("file")
			require.NoError(t, err)
			defer func() { assert.NoError(t, f.Close()) }()

			if tt.beforeRead > 0 {
				buf := make([]byte, tt.beforeRead)
				_, err = io.ReadFull(f, buf)
				require.NoError(t, err)
			}

			seeker, ok := f.(io.Seeker)
			require.True(t, ok)

			pos, err := seeker.Seek(tt.offset, tt.whence)
			require.NoError(t, err)
			require.Equal(t, tt.wantPos, pos)

			got, err := io.ReadAll(f)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

func TestFileSeekInvalid(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "file", strings.NewReader("0123456789"))
	fsClient := newFS(t)

	tests := []struct {
		name   string
		offset int64
		whence int
	}{
		{
			name:   "before start",
			offset: -1,
			whence: io.SeekStart,
		},
		{
			name:   "past end",
			offset: 1,
			whence: io.SeekEnd,
		},
		{
			name:   "invalid whence",
			offset: 0,
			whence: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := fsClient.Open("file")
			require.NoError(t, err)
			defer func() { assert.NoError(t, f.Close()) }()

			seeker, ok := f.(io.Seeker)
			require.True(t, ok)

			_, err = seeker.Seek(tt.offset, tt.whence)
			require.ErrorIs(t, err, fs.ErrInvalid)
		})
	}
}

func TestFileCreateExistingDirectory(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "some-directory/a/test.txt", strings.NewReader(""))
	fsClient := newFS(t)

	_, err := fsClient.Create("some-directory/a")
	require.ErrorIs(t, err, fs.ErrExist)
}

func TestFileRemove(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "some-directory/a/test.txt", strings.NewReader(""))
	fsClient := newFS(t, s3fs.WithPrefix("some-directory/a"))

	err := fsClient.Remove("test.txt")
	require.NoError(t, err)
	assertObjectRemoved(t, "test", "some-directory/a/test.txt")
}

func TestFileStatHighNumberInRootDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	createBucket(t, "test")
	files := createObjects(t, "test", "", "example", 1000)
	fsClient := newFS(t)

	info, err := fsClient.Stat(files[len(files)-1])

	require.NoError(t, err)
	require.Equal(t, path.Base(files[len(files)-1]), info.Name())
}

func TestFileStatHighNumberInNestedDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	createBucket(t, "test")
	files := createObjects(t, "test", "some-directory", "example", 1500)
	fsClient := newFS(t, s3fs.WithPrefix("some-directory"))

	info, err := fsClient.Stat(files[len(files)-1])

	require.NoError(t, err)
	require.Equal(t, path.Base(files[len(files)-1]), info.Name())
}
