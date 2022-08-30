package s3fs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type ListObjectsV2APIClient interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type HeadObjectClient interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}
