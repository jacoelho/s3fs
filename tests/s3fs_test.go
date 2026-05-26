package tests

import (
	"testing"

	"github.com/jacoelho/s3fs/v2"
	"github.com/stretchr/testify/require"
)

func newFS(t *testing.T, opts ...s3fs.Option) *s3fs.Fs {
	t.Helper()

	fsys, err := s3fs.New(client, "test", opts...)
	require.NoError(t, err)
	return fsys
}
