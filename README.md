# s3fs

A [S3](https://aws.amazon.com/s3/) filesystem implementation of [io.Fs](https://pkg.go.dev/io/fs).

Supports handling directories and files transparently, while being memory efficient, which allows handles large files without being limited by the available memory.

## Install

```bash
go get -u github.com/jacoelho/s3fs
```

## Example

```go
package main

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jacoelho/s3fs"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil { panic(err) }

	client := s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = true
	})

	filesystem := s3fs.New(client, "test")
	data, err := fs.ReadFile(filesystem, "a-file") // not recommend when handling large files
	if err != nil { panic(err) }

	fmt.Println(string(data))
}
```

## Policies

| policy                      | resource                               |
|-----------------------------|----------------------------------------|
| s3:ListBucket               | arn:aws:s3:::YOUR_BUCKET               |
| s3:GetObject                | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |
| s3:PutObject                | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |
| s3:DeleteObject             | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |
| s3:ListMultipartUploadParts | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |
| s3:AbortMultipartUpload     | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |

## License

MIT License

See [LICENSE](LICENSE) to see the full text.
