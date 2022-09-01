package tests

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/jacoelho/s3fs"
)

func generateFile(t *testing.T, name string, size int64) {
	t.Helper()

	f, err := os.Open(name)
	if err == nil {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		t.Skip("file exists")
	}

	f, err = os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if _, err := io.CopyN(f, rand.Reader, size); err != nil {
		t.Fatal(err)
	}
}

func TestFile10MB(t *testing.T) {
	generateFile(t, "fixture_10mb.txt", 10*1024*1024)
}

func TestFile256MB(t *testing.T) {
	generateFile(t, "fixture_256mb.txt", 256*1024*1024)
}

func TestFile1G(t *testing.T) {
	generateFile(t, "fixture_1G.txt", 1*1024*1024*1024)
}

func TestFoo(t *testing.T) {
	createBucket(t, "test")
	defer deleteBucket(t, "test")

	fs := s3fs.New(client, "test")

	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	source, err := os.Open("fixture_256mb.txt")
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = source.Close() }()

	sourceHasher := sha256.New()
	tr := io.TeeReader(source, sourceHasher)
	if _, err := io.Copy(f, tr); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
	fmt.Printf("\thash = %v\n", hex.EncodeToString(sourceHasher.Sum(nil)))

	file, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	destHasher := sha256.New()
	if _, err := io.Copy(destHasher, file); err != nil {
		t.Fatal(err)
	}

	var p runtime.MemStats
	runtime.ReadMemStats(&p)

	fmt.Printf("Alloc = %v MiB", bToMb(p.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(p.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(p.Sys))
	fmt.Printf("\tNumGC = %v\n", p.NumGC)
	fmt.Printf("\thash = %v\n", hex.EncodeToString(destHasher.Sum(nil)))

}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func TestRead(t *testing.T) {
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

	fs := s3fs.New(client, "test")

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
