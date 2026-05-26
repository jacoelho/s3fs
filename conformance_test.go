package s3fs

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestFakeS3ConformanceListPrefixDelimiterPaginationAndEncoding(t *testing.T) {
	client := newFakeS3()
	putFake(client, "dir/a.txt", "")
	putFake(client, "dir/sub/b.txt", "")
	putFake(client, "dir/space name.txt", "")

	page1, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:       aws.String("bucket"),
		Prefix:       aws.String("dir/"),
		Delimiter:    aws.String("/"),
		MaxKeys:      aws.Int32(2),
		EncodingType: types.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(page1.IsTruncated) {
		t.Fatal("page1 not truncated")
	}

	page2, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:            aws.String("bucket"),
		Prefix:            aws.String("dir/"),
		Delimiter:         aws.String("/"),
		ContinuationToken: page1.NextContinuationToken,
		EncodingType:      types.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatal(err)
	}

	var keys []string
	for _, obj := range append(page1.Contents, page2.Contents...) {
		key, err := decodeS3Key(aws.ToString(obj.Key))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if !contains(keys, "dir/a.txt") || !contains(keys, "dir/space name.txt") {
		t.Fatalf("keys = %v", keys)
	}

	var prefixes []string
	for _, prefix := range append(page1.CommonPrefixes, page2.CommonPrefixes...) {
		key, err := decodeS3Key(aws.ToString(prefix.Prefix))
		if err != nil {
			t.Fatal(err)
		}
		prefixes = append(prefixes, key)
	}
	if !contains(prefixes, "dir/sub/") {
		t.Fatalf("prefixes = %v", prefixes)
	}
}

func TestFakeS3ConformanceListEncodingRawValues(t *testing.T) {
	client := newFakeS3()
	cases := []struct {
		key string
		raw string
	}{
		{key: "dir/space name.txt", raw: "dir%2Fspace%20name.txt"},
		{key: "dir/plus+name.txt", raw: "dir%2Fplus%2Bname.txt"},
		{key: "dir/percent%name.txt", raw: "dir%2Fpercent%25name.txt"},
		{key: "dir/café.txt", raw: "dir%2Fcaf%C3%A9.txt"},
	}
	for _, tt := range cases {
		putFake(client, tt.key, "")
	}
	putFake(client, "dir/sub/file.txt", "")

	page, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:       aws.String("bucket"),
		Prefix:       aws.String("dir/"),
		Delimiter:    aws.String("/"),
		EncodingType: types.EncodingTypeUrl,
	})
	if err != nil {
		t.Fatal(err)
	}

	var rawKeys []string
	for _, obj := range page.Contents {
		rawKeys = append(rawKeys, aws.ToString(obj.Key))
	}
	for _, tt := range cases {
		if !contains(rawKeys, tt.raw) {
			t.Fatalf("raw keys = %v, missing %q", rawKeys, tt.raw)
		}
		decoded, err := decodeS3Key(tt.raw)
		if err != nil {
			t.Fatal(err)
		}
		if decoded != tt.key {
			t.Fatalf("decode(%q) = %q, want %q", tt.raw, decoded, tt.key)
		}
	}

	var rawPrefixes []string
	for _, prefix := range page.CommonPrefixes {
		rawPrefixes = append(rawPrefixes, aws.ToString(prefix.Prefix))
	}
	if !contains(rawPrefixes, "dir%2Fsub%2F") {
		t.Fatalf("raw prefixes = %v, want dir%%2Fsub%%2F", rawPrefixes)
	}
}

func TestFakeS3ConformanceRangeIfMatchAndClose(t *testing.T) {
	client := newFakeS3()
	putFake(client, "file", "0123456789")

	head, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("file"),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  aws.String("bucket"),
		Key:     aws.String("file"),
		Range:   aws.String("bytes=3-6"),
		IfMatch: head.ETag,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(body) != "3456" {
		t.Fatalf("range body = %q", body)
	}
	if client.closes != 1 {
		t.Fatalf("closes = %d, want 1", client.closes)
	}

	_, err = client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  aws.String("bucket"),
		Key:     aws.String("file"),
		IfMatch: aws.String(`"wrong"`),
	})
	if err == nil {
		t.Fatal("IfMatch mismatch succeeded")
	}
}

func TestFakeS3ConformanceMPUCompleteAndAbort(t *testing.T) {
	client := newFakeS3()

	create, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String("bucket"),
		Key:               aws.String("file"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatal(err)
	}

	part1, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:            aws.String("bucket"),
		Key:               aws.String("file"),
		UploadId:          create.UploadId,
		PartNumber:        aws.Int32(1),
		Body:              bytes.NewReader([]byte("abc")),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatal(err)
	}
	part2, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:            aws.String("bucket"),
		Key:               aws.String("file"),
		UploadId:          create.UploadId,
		PartNumber:        aws.Int32(2),
		Body:              bytes.NewReader([]byte("def")),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String("bucket"),
		Key:           aws.String("file"),
		UploadId:      create.UploadId,
		MpuObjectSize: aws.Int64(6),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: part1.ETag, ChecksumCRC32: part1.ChecksumCRC32},
			{PartNumber: aws.Int32(2), ETag: part2.ETag, ChecksumCRC32: part2.ChecksumCRC32},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := client.object("file"); !ok || string(got) != "abcdef" {
		t.Fatalf("completed object = %q, ok=%v", got, ok)
	}

	create, err = client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("aborted"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("bucket"),
		Key:      aws.String("aborted"),
		UploadId: create.UploadId,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.object("aborted"); ok {
		t.Fatal("aborted upload created object")
	}
}

func TestFakeS3ConformanceMPUKeyMismatchFails(t *testing.T) {
	client := newFakeS3()
	create, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("file"),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:     aws.String("bucket"),
		Key:        aws.String("other"),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("abc")),
	}); err == nil {
		t.Fatal("UploadPart key mismatch succeeded")
	}

	part, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:     aws.String("bucket"),
		Key:        aws.String("file"),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("abc")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String("bucket"),
		Key:           aws.String("other"),
		UploadId:      create.UploadId,
		MpuObjectSize: aws.Int64(3),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: part.ETag},
		}},
	}); err == nil {
		t.Fatal("CompleteMultipartUpload key mismatch succeeded")
	}
	if _, err := client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("bucket"),
		Key:      aws.String("other"),
		UploadId: create.UploadId,
	}); err == nil {
		t.Fatal("AbortMultipartUpload key mismatch succeeded")
	}
	if _, err := client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("bucket"),
		Key:      aws.String("file"),
		UploadId: create.UploadId,
	}); err != nil {
		t.Fatal(err)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
