package fs

import (
	"context"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var _ fs.ReadDirFile = (*Directory)(nil)

const delimiter = "/"

type Directory struct {
	fs       *Fs
	fileInfo FileInfo
}

func (d *Directory) Name() string               { return d.fileInfo.Name() }
func (d *Directory) IsDir() bool                { return d.fileInfo.IsDir() }
func (d *Directory) Type() fs.FileMode          { return d.fileInfo.Type() }
func (d *Directory) Info() (fs.FileInfo, error) { return d.fileInfo.Info() }
func (d *Directory) Stat() (fs.FileInfo, error) { return &d.fileInfo, nil }
func (d *Directory) Close() error               { return nil }

func (d *Directory) Read(_ []byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.fileInfo.Name(), Err: fs.ErrInvalid}
}

func (d *Directory) ReadDir(n int) ([]fs.DirEntry, error) {
	opts := &s3.ListObjectsV2Input{
		Bucket:    aws.String(d.fs.bucket),
		Prefix:    aws.String(d.fs.prefix),
		Delimiter: aws.String(delimiter),
	}

	if n > 0 && n <= 1000 {
		opts.MaxKeys = int32(n)
	}

	seenPrefixes := make(map[string]struct{})
	result := make([]fs.DirEntry, 0, 0)

	paginator := s3.NewListObjectsV2Paginator(d.fs.client, opts)
	for paginator.HasMorePages() {
		if n > 0 && len(result) >= n {
			break
		}

		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))

		page, err := paginator.NextPage(ctx)
		cancelFn()
		if err != nil {
			return nil, err
		}

		for _, p := range page.CommonPrefixes {
			if p.Prefix == nil {
				continue
			}

			dirName, mode := normalizeName(d.fs.prefix, *p.Prefix)

			if _, found := seenPrefixes[dirName]; found {
				continue
			}

			seenPrefixes[dirName] = struct{}{}

			result = append(result, &Directory{
				fs: d.fs,
				fileInfo: FileInfo{
					name:    dirName,
					mode:    mode,
					modTime: time.Now(),
				},
			})
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			name, mode := normalizeName(d.fs.prefix, *obj.Key)
			if name == "" || name == delimiter {
				continue
			}

			if mode&fs.ModeDir != 0 {
				if _, found := seenPrefixes[name]; found {
					continue
				}
				seenPrefixes[name] = struct{}{}
			}

			result = append(result, &File{
				fs: d.fs,
				info: FileInfo{
					name:    name,
					size:    obj.Size,
					mode:    mode,
					modTime: getOrElse(obj.LastModified, time.Now),
				},
			})
		}
	}

	if n > 0 && len(result) > n {
		return result[:n], nil
	}

	return result, nil
}
