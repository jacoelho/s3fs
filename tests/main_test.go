package tests

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

const portEdge = "4566/tcp"

var client *s3.Client

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}

	resource, err := pool.Run("localstack/localstack", "", []string{
		"EAGER_SERVICE_LOADING=1",
		"SERVICES=s3",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %v", err)
	}

	err = pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%s/health", resource.GetPort(portEdge)), nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return errors.New("not ready")
		}

		var status localstackHealthResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return err
		}

		if status.Services.S3 != "running" {
			return errors.New("s3 not running")
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Could not connect to localstack: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("foobar", "foobar", "foobar")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           fmt.Sprintf("http://localhost:%s", resource.GetPort(portEdge)),
				PartitionID:   "aws",
				SigningRegion: "us-east-1",
			}, nil
		})))
	if err != nil {
		log.Fatalf("Could not create config: %v", err)
	}

	client = s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = true
	})

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %v", err)
	}

	os.Exit(code)
}

type localstackHealthResponse struct {
	Services struct {
		S3 string `json:"s3"`
	} `json:"services"`
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
