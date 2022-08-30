package fss

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type Fs struct {
	bucket string
	client S3API
	config aws.Config
}

func NewFs(bucket string, config aws.Config) *Fs {
	return &Fs{
		bucket: bucket,
		client: s3.NewFromConfig(config),
		config: config,
	}
}

func (s *Fs) Close() error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) ReadAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Seek(offset int64, whence int) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Write(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) WriteAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Chdir() error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Chmod(mode os.FileMode) error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Chown(uid int, gid int) error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Fd() uintptr {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) ReadDir(n int) ([]os.DirEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) ReadFrom(r io.Reader) (n int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Readdir(n int) ([]os.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Readdirnames(n int) (names []string, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) stat(name string) (os.FileInfo, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	}

	resp, err := s.client.HeadObject(context.Background(), req)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, &os.PathError{
				Op:   "stat",
				Path: name,
				Err:  nsk,
			}
		}

		return nil, err
	}

	return newFileInfo(name, resp), nil
}

func (s *Fs) Stat() (os.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Sync() error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) Truncate(size int64) error {
	//TODO implement me
	panic("implement me")
}

func (s *Fs) WriteString(a string) (n int, err error) {
	//TODO implement me
	panic("implement me")
}
