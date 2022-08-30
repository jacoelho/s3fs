package tests

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/jacoelho/s3fs"
)

func TestFoo(t *testing.T) {
	client := NewS3Client(t)

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("test"),
	})
	if err != nil {
		t.Fatal(err)
	}

	fs := s3fs.New(client, "test", "bar")

	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := io.WriteString(f, "test"); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRead(t *testing.T) {
	client := NewS3Client(t)

	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("test"),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("test"),
		Key:    aws.String("bar/test.txt"),
		Body:   strings.NewReader("something"),
	})
	if err != nil {
		t.Fatal(err)
	}

	fs := s3fs.New(client, "test", "bar")

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(string(data), len(data))
}
