package tests

import (
	"bytes"
	"io"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestS3ConformanceListRangeAndMPU(t *testing.T) {
	createBucket(t, "test")
	createObject(t, "test", "dir/a.txt", bytes.NewReader(nil))
	createObject(t, "test", "dir/sub/b.txt", bytes.NewReader(nil))
	createObject(t, "test", "dir/space name.txt", bytes.NewReader(nil))
	createObject(t, "test", "dir/plus+name.txt", bytes.NewReader(nil))
	createObject(t, "test", "dir/percent%name.txt", bytes.NewReader(nil))
	createObject(t, "test", "dir/café.txt", bytes.NewReader(nil))
	createObject(t, "test", "file", bytes.NewReader([]byte("0123456789")))

	page, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:       aws.String("test"),
		Prefix:       aws.String("dir/"),
		Delimiter:    aws.String("/"),
		MaxKeys:      aws.Int32(2),
		EncodingType: types.EncodingTypeUrl,
	})
	require.NoError(t, err)
	require.NotZero(t, len(page.Contents)+len(page.CommonPrefixes))
	require.True(t, aws.ToBool(page.IsTruncated))

	all, err := client.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket:       aws.String("test"),
		Prefix:       aws.String("dir/"),
		Delimiter:    aws.String("/"),
		EncodingType: types.EncodingTypeUrl,
	})
	require.NoError(t, err)
	rawKeys := make([]string, 0, len(all.Contents))
	for _, obj := range all.Contents {
		rawKeys = append(rawKeys, aws.ToString(obj.Key))
	}
	for _, tt := range []struct {
		key string
		raw string
	}{
		{key: "dir/space name.txt", raw: "dir/space%20name.txt"},
		{key: "dir/plus+name.txt", raw: "dir/plus%2Bname.txt"},
		{key: "dir/percent%name.txt", raw: "dir/percent%25name.txt"},
		{key: "dir/café.txt", raw: "dir/caf%C3%A9.txt"},
	} {
		require.Contains(t, rawKeys, tt.raw)
		decoded, err := url.PathUnescape(tt.raw)
		require.NoError(t, err)
		require.Equal(t, tt.key, decoded)
	}
	rawPrefixes := make([]string, 0, len(all.CommonPrefixes))
	for _, prefix := range all.CommonPrefixes {
		rawPrefixes = append(rawPrefixes, aws.ToString(prefix.Prefix))
	}
	require.Contains(t, rawPrefixes, "dir/sub/")

	head, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
		Bucket: aws.String("test"),
		Key:    aws.String("file"),
	})
	require.NoError(t, err)

	rangeOut, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket:  aws.String("test"),
		Key:     aws.String("file"),
		Range:   aws.String("bytes=3-6"),
		IfMatch: head.ETag,
	})
	require.NoError(t, err)
	got, err := io.ReadAll(rangeOut.Body)
	require.NoError(t, err)
	require.NoError(t, rangeOut.Body.Close())
	require.Equal(t, "3456", string(got))

	upload, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("test"),
		Key:    aws.String("mpu"),
	})
	require.NoError(t, err)

	part1, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:     aws.String("test"),
		Key:        aws.String("mpu"),
		UploadId:   upload.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(bytes.Repeat([]byte("a"), 5<<20)),
	})
	require.NoError(t, err)
	part2, err := client.UploadPart(t.Context(), &s3.UploadPartInput{
		Bucket:     aws.String("test"),
		Key:        aws.String("mpu"),
		UploadId:   upload.UploadId,
		PartNumber: aws.Int32(2),
		Body:       bytes.NewReader([]byte("b")),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{
		Bucket:        aws.String("test"),
		Key:           aws.String("mpu"),
		UploadId:      upload.UploadId,
		MpuObjectSize: aws.Int64(5<<20 + 1),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: part1.ETag},
			{PartNumber: aws.Int32(2), ETag: part2.ETag},
		}},
	})
	require.NoError(t, err)

	upload, err = client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("test"),
		Key:    aws.String("aborted"),
	})
	require.NoError(t, err)
	_, err = client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("test"),
		Key:      aws.String("aborted"),
		UploadId: upload.UploadId,
	})
	require.NoError(t, err)
}
