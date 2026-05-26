package s3fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func BenchmarkWriterSmallPut(b *testing.B) {
	body := bytes.Repeat([]byte("a"), 1024)
	client := &countingS3{}
	for b.Loop() {
		w := newCountingWriter(client, "file", 8<<10, 1<<20)
		if _, err := w.Write(body); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriterMultipart(b *testing.B) {
	body := bytes.Repeat([]byte("a"), 64<<10)
	client := &countingS3{}
	for b.Loop() {
		w := newCountingWriter(client, "file", 8<<10, 1<<20)
		if _, err := w.Write(body); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadDirProjection(b *testing.B) {
	client := newFakeS3()
	for i := range 1000 {
		putFake(client, fmt.Sprintf("dir/file-%04d.txt", i), "")
	}
	fsys := newTestFS(b, client)

	for b.Loop() {
		entries, err := fsys.ReadDir("dir")
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1000 {
			b.Fatalf("entries = %d, want 1000", len(entries))
		}
	}
}

func BenchmarkDirScanProjectionPrebuiltPage(b *testing.B) {
	page := &s3.ListObjectsV2Output{Contents: make([]types.Object, 1000)}
	for i := range page.Contents {
		page.Contents[i] = types.Object{
			Key:  aws.String(fmt.Sprintf("dir%%2Ffile-%04d.txt", i)),
			Size: aws.Int64(1),
		}
	}
	client := &listPageS3{page: page}
	fsys := newTestFS(b, client)
	p, err := normalizePath("dir", fsys.directoryFile, true)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		entries, err := fsys.readDirEntries(context.Background(), p)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 1000 {
			b.Fatalf("entries = %d, want 1000", len(entries))
		}
	}
}

func BenchmarkReadAt(b *testing.B) {
	client := newFakeS3()
	body := bytes.Repeat([]byte("a"), 64<<10)
	putFake(client, "file", string(body))
	fsys := newTestFS(b, client)
	file, err := fsys.Open("file")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, 4<<10)
	readerAt := file.(io.ReaderAt)
	for b.Loop() {
		if _, err := readerAt.ReadAt(buf, 8<<10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMissingStat(b *testing.B) {
	client := newFakeS3()
	fsys := newTestFS(b, client)

	for b.Loop() {
		if _, err := fsys.Stat("missing"); err == nil {
			b.Fatal("Stat(missing) succeeded")
		}
	}
}

func newCountingWriter(client *countingS3, key string, partSize, maxWriteSize int64) *Writer {
	return newWriter(context.Background(), writerConfig{
		client:           client,
		bucket:           "bucket",
		key:              key,
		path:             key,
		partSize:         partSize,
		maxWriteSize:     maxWriteSize,
		operationContext: noTimeoutContext,
		cleanupContext:   backgroundContext,
	})
}

type countingS3 struct{}

type listPageS3 struct {
	countingS3
	page *s3.ListObjectsV2Output
}

func (c *listPageS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return c.page, nil
}

func (c *countingS3) HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, notFoundErr()
}

func (c *countingS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if _, err := io.Copy(io.Discard, in.Body); err != nil {
		return nil, err
	}
	return &s3.PutObjectOutput{ETag: aws.String(`"etag"`)}, nil
}

func (c *countingS3) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, notFoundErr()
}

func (c *countingS3) DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return &s3.DeleteObjectOutput{}, nil
}

func (c *countingS3) ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{}, nil
}

func (c *countingS3) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
}

func (c *countingS3) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if _, err := io.Copy(io.Discard, in.Body); err != nil {
		return nil, err
	}
	return &s3.UploadPartOutput{
		ETag:          aws.String(`"etag"`),
		ChecksumCRC32: aws.String("checksum"),
	}, nil
}

func (c *countingS3) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *countingS3) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

var _ s3ApiClient = (*countingS3)(nil)
