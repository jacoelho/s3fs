package tests

import (
	"fmt"
	"io"
	"runtime"
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
