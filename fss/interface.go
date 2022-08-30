package fss

import (
	"io"
	"os"
)

var _ OsFiler = (*S3Fs)(nil)

type OsFiler interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt

	Chdir() error
	Chmod(mode os.FileMode) error
	Chown(uid int, gid int) error
	Fd() uintptr
	Name() string
	ReadDir(n int) ([]os.DirEntry, error)
	ReadFrom(r io.Reader) (n int64, err error)
	Readdir(n int) ([]os.FileInfo, error)
	Readdirnames(n int) (names []string, err error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	WriteString(s string) (n int, err error)
}

// https://github.com/fclairamb/afero-s3/blob/main/s3_file.go
// https://github.com/a1comms/gcs-sftp-server/blob/2968b66b1e5556a552e9b883781bc33796dabe51/handler/defines_writeat.go
// https://github.com/blankenshipz/s3tp/blob/d9f02cf073d0e98c2a8dc44522e126218afb3c2e/s3-file.go
// https://github.com/drakkan/sftpgo/blob/a5e41c93362751534ae278309bc2b2cc0cf62b3e/vfs/s3fs.go
// https://github.com/jszwec/s3fs
// https://github.com/uber/storagetapper/blob/master/pipe/s3.go
// https://github.com/sourcegraph/s3vfs/blob/master/s3vfs.go
// https://github.com/fclairamb/afero-s3/blob/main/s3_file.go
// https://github.com/a1comms/gcs-sftp-server/blob/2968b66b1e5556a552e9b883781bc33796dabe51/handler/defines_writeat.go
// https://github.com/blankenshipz/s3tp/blob/d9f02cf073d0e98c2a8dc44522e126218afb3c2e/s3-file.go
// https://github.com/PullRequestInc/iostream/tree/master
