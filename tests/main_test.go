package tests

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/smithy-go/logging"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	endpoints "github.com/aws/smithy-go/endpoints"
	"github.com/stretchr/testify/require"
)

var (
	client *s3.Client
	debug  = flag.Bool("debug", false, "debug mode.")
)

func TestMain(m *testing.M) {
	flag.Parse()

	opts := []func(*config.LoadOptions) error{
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "dummy")),
	}

	if *debug {
		opts = append(opts,
			config.WithLogger(logging.NewStandardLogger(os.Stdout)),
			config.WithClientLogMode(aws.LogRequest))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		log.Fatalf("Could not create config: %v", err)
	}

	client = s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = true
		opt.EndpointResolverV2 = &staticResolverS3{endpoint: "http://localhost:4566"}
	})

	code := m.Run()

	os.Exit(code)
}

type staticResolverS3 struct {
	endpoint string
}

func (r *staticResolverS3) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (
	endpoints.Endpoint, error,
) {
	u, err := url.Parse(r.endpoint)
	if err != nil {
		return endpoints.Endpoint{}, err
	}
	endpoint, err := s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
	if err != nil {
		return endpoints.Endpoint{}, err
	}
	endpoint.URI.Host = u.Host
	endpoint.URI.Scheme = u.Scheme
	return endpoint, err
}

func createBucket(t *testing.T, bucket string) {
	t.Helper()

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	t.Cleanup(func() { deleteBucket(t, bucket) })
}

func deleteBucket(t *testing.T, bucket string) {
	t.Helper()

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			t.Fatalf("failed to get next page: %v", err)
		}

		if len(page.Contents) == 0 {
			break
		}

		opt := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: make([]types.ObjectIdentifier, len(page.Contents)),
			},
		}

		for i := range page.Contents {
			opt.Delete.Objects[i] = types.ObjectIdentifier{
				Key: page.Contents[i].Key,
			}
		}

		_, err = client.DeleteObjects(context.Background(), opt)
		if err != nil {
			t.Fatalf("failed to delete object: %v", err)
		}
	}

	_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to delete bucket: %v", err)
	}
}

func createObject(t *testing.T, bucket, path string, body io.Reader) {
	t.Helper()

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
		Body:   body,
	})
	if err != nil {
		t.Fatalf("failed to create object: %v", err)
	}
}

func createObjects(t *testing.T, bucket, dirName, filePrefix string, count int) []string {
	t.Helper()

	result := make([]string, count)

	for i := 0; i < count; i++ {
		fileName := fmt.Sprintf("%s_%000000d.txt", filePrefix, i)

		_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path.Join(dirName, fileName)),
			Body:   strings.NewReader("data"),
		})
		if err != nil {
			t.Fatalf("failed to create object: %v", err)
		}

		result[i] = fileName
	}

	return result
}

func assertObjectRemoved(t *testing.T, bucket, path string) {
	t.Helper()

	_, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	require.True(t, isErrNotFound(err))
}

func createFileWithSize(t *testing.T, size int64) (*os.File, string) {
	t.Helper()

	f, err := os.Create(filepath.Join(t.TempDir(), "file"))
	require.NoError(t, err)

	h := sha256.New()
	tee := io.TeeReader(rand.Reader, h)
	_, err = io.CopyN(f, tee, size)
	require.NoError(t, err)

	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	return f, hex.EncodeToString(h.Sum(nil))
}

func createObjectRandomContentsWithSize(t *testing.T, bucket, path string, size int64) string {
	t.Helper()

	f, sum := createFileWithSize(t, size)
	defer func() { _ = f.Close() }()

	createObject(t, bucket, path, f)

	return sum
}

func sha256sum(t *testing.T, r io.Reader) string {
	t.Helper()

	h := sha256.New()
	_, err := io.Copy(h, r)
	require.NoError(t, err)

	return hex.EncodeToString(h.Sum(nil))
}

func fileChecksum(t *testing.T, f *os.File) string {
	t.Helper()

	_, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)

	return sha256sum(t, f)
}

func objectChecksum(t *testing.T, bucket, path string) string {
	t.Helper()

	resp, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	return sha256sum(t, resp.Body)
}

func calculateChunks(fileSize, chunkSize int64) []int {
	chunks := make([]int, int(fileSize/chunkSize))
	for i := range chunks {
		chunks[i] = int(chunkSize)
	}

	if rem := fileSize % chunkSize; rem > 0 {
		chunks = append(chunks, int(rem))
	}

	return chunks
}

func isErrNotFound(err error) bool {
	if err == nil {
		return false
	}

	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.Response != nil {
		return re.Response.StatusCode == http.StatusNotFound
	}

	return false
}
