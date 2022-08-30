package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ory/dockertest/v3"
)

const portEdge = "4566/tcp"

type healthResponse struct {
	Services struct {
		S3 string `json:"s3"`
	} `json:"services"`
}

func NewS3Client(t *testing.T) *s3.Client {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %v", err)
	}
	pool.MaxWait = time.Minute * 2

	resource, err := pool.Run("localstack/localstack", "", []string{
		"EAGER_SERVICE_LOADING=1",
		"SERVICES=s3",
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}

	if err := pool.Retry(func() error {
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

		var status healthResponse
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return err
		}

		if status.Services.S3 != "running" {
			return errors.New("s3 not running")
		}

		return nil
	}); err != nil {
		t.Fatalf("Could not connect to localstack: %v", err)
	}

	//t.Cleanup(func() {
	//	_ = pool.Purge(resource)
	//})

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
		t.Fatalf("Could not create config: %v", err)
	}

	return s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = true
	})
}
