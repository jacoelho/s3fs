package tests

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jacoelho/s3fs"
)

func TestCreateDirectory(t *testing.T) {
	createBucket(t, "test")
	defer deleteBucket(t, "test")

	fsClient := s3fs.New(client, "test", "")
	dir, err := fsClient.CreateDir("some-directory")

	require.NoError(t, err)
	assert.Equal(t, "some-directory", dir.Name())
	assert.True(t, dir.IsDir())

	info, err := dir.Info()
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())
	assert.Equal(t, fs.ModeDir, info.Mode())
}
