package tests

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jacoelho/s3fs"
)

// a bit arbitrary value
const memoryLimit = 20 * 1024 * 1024

func TestFileRead(t *testing.T) {
	fileSizes := []int64{
		5 * 1024 * 1024,
		50 * 1024 * 1024,
		256 * 1024 * 1024,
	}

	createBucket(t, "test")
	fsClient := s3fs.New(client, "test")

	for i, tc := range fileSizes {
		t.Run(fmt.Sprintf("file size %d", tc), func(t *testing.T) {
			fileName := fmt.Sprintf("file_read_%0d.txt", i)

			sum := createObjectRandomContentsWithSize(t, "test", fileName, tc)

			f, err := fsClient.Open(fileName)
			require.NoError(t, err)

			assert.Equal(t, sum, sha256sum(t, f))
			assert.NoError(t, err, f.Close())

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			assert.True(t, m.Alloc < memoryLimit)
		})
	}
}

func TestFileReadChunks(t *testing.T) {
	size := int64(256 * 1024 * 1024)
	chunkSize := 10 * 1024 * 1024

	createBucket(t, "test")

	fsClient := s3fs.New(client, "test")
	checksumSource := createObjectRandomContentsWithSize(t, "test", "file", size)
	source, err := fsClient.Open("file")
	sourceAt, ok := source.(io.ReaderAt)
	require.True(t, ok)
	require.NoError(t, err)

	dst, err := os.Create(filepath.Join(t.TempDir(), "file"))
	require.NoError(t, err)

	chunks := calculateChunks(size, int64(chunkSize))

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for i, c := range chunks {
		go func(t *testing.T, i, chunk int) {
			defer wg.Done()

			buf := make([]byte, chunk)
			_, err := sourceAt.ReadAt(buf, int64(i*chunkSize))
			require.NoError(t, err)

			_, err = dst.WriteAt(buf, int64(i*chunkSize))
			require.NoError(t, err)
		}(t, i, c)
	}
	wg.Wait()

	checksumDestination := fileChecksum(t, dst)
	assert.Equal(t, checksumSource, checksumDestination)
	assert.NoError(t, dst.Close())
}

func TestFileWrite(t *testing.T) {
	fileSizes := []int64{
		5 * 1024 * 1024,
		50 * 1024 * 1024,
		256 * 1024 * 1024,
	}

	createBucket(t, "test")
	fsClient := s3fs.New(client, "test")

	for i, tc := range fileSizes {
		t.Run(fmt.Sprintf("file size %d", tc), func(t *testing.T) {
			runtime.GC()

			fileName := fmt.Sprintf("file_write_%0d.txt", i)

			sourceFile, checksum := createFileWithSize(t, tc)

			f, err := fsClient.Create(fileName)
			require.NoError(t, err)

			_, err = io.Copy(f, sourceFile)
			require.NoError(t, err)
			assert.NoError(t, err, sourceFile.Close())
			assert.NoError(t, err, f.Close())
			assert.Equal(t, checksum, objectChecksum(t, "test", fileName))

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			assert.True(t, m.Alloc < memoryLimit)
		})
	}
}

func TestFileWriteChunks(t *testing.T) {
	fileSize := int64(256 * 1024 * 1024)
	chunkSize := 10 * 1024 * 1024
	sourceAt, checksumSource := createFileWithSize(t, fileSize)

	createBucket(t, "test")
	fsClient := s3fs.New(client, "test")
	destination, err := fsClient.Create("file")
	require.NoError(t, err)

	chunks := calculateChunks(fileSize, int64(chunkSize))

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	for i, c := range chunks {
		go func(t *testing.T, i, chunk int) {
			defer wg.Done()

			buf := make([]byte, chunk)
			_, err := sourceAt.ReadAt(buf, int64(i*chunkSize))
			require.NoError(t, err)

			_, err = destination.WriteAt(buf, int64(i*chunkSize))
			require.NoError(t, err)
		}(t, i, c)
	}
	wg.Wait()
	require.NoError(t, destination.Close())

	checksumDestination := objectChecksum(t, "test", "file")
	assert.Equal(t, checksumSource, checksumDestination)
	assert.NoError(t, sourceAt.Close())
}

func TestFileReadWhenFileCreatedFails(t *testing.T) {
	createBucket(t, "test")
	fsClient := s3fs.New(client, "test")
	destination, err := fsClient.Create("file")
	require.NoError(t, err)

	_, err = destination.Read(make([]byte, 100))
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestFileReadAtWhenFileCreatedFails(t *testing.T) {
	createBucket(t, "test")
	fsClient := s3fs.New(client, "test")
	destination, err := fsClient.Create("file")
	require.NoError(t, err)

	_, err = destination.ReadAt(make([]byte, 100), 0)
	require.ErrorIs(t, err, os.ErrClosed)
}
