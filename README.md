# s3fs

A [S3](https://aws.amazon.com/s3/) filesystem implementation of [io.Fs](https://pkg.go.dev/io/fs).

Supports handling directories and files transparently, while being memory efficient, which allows handling large files without being limited by available memory.

## Install

Requires Go 1.26+.

```bash
go get -u github.com/jacoelho/s3fs/v2
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
	"github.com/jacoelho/s3fs/v2"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil { panic(err) }

	client := s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = true
	})

	filesystem, err := s3fs.New(client, "test")
	if err != nil { panic(err) }
	data, err := fs.ReadFile(filesystem, "a-file") // not recommended when handling large files
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
| s3:AbortMultipartUpload     | arn:aws:s3:::YOUR_BUCKET/YOUR_PREFIX/* |

## Architecture

### Overview

S3FS exposes an `io/fs` filesystem backed by S3 objects. Current design favors bounded memory, explicit path rules, and synchronous multipart upload orchestration over background pipe-based transfer machinery.

### Core Components

#### `Fs`

`Fs` owns bucket, prefix, path policy, S3 operation timeout, multipart part size, max accepted write size, and active writer slots.

Implemented filesystem surfaces:

- `fs.FS`
- `fs.StatFS`
- `fs.ReadDirFS`
- context-aware variants for open, stat, create, directory, and remove operations

Write controls:

- `WithPartSize` controls multipart part size. Default is 50 MiB.
- `WithMaxWriteSize` caps accepted object size. Default is `partSize * 10000`.
- `WithMaxActiveWriters` limits concurrent writers and therefore live write buffers.

#### `Writer`

`Writer` is the only write surface returned by `Create`.

Write path:

- Buffer below or equal to `partSize`, then use `PutObject` on `Close`.
- Start multipart only when size crosses `partSize`.
- Upload parts synchronously, one at a time.
- Keep at most one buffered part per writer.
- Reject writes before accepting bytes that would exceed max write size.
- Drop the part buffer and release the active writer slot once all parts are uploaded, before completion retry state.
- Retry `CompleteMultipartUpload` by allowing later `Close` calls after retryable complete failure.
- Abort multipart uploads on terminal write failure, report abort failure with the original error, and retry abort cleanup on later `Close`.

There is no `WriteAt`; random writes are intentionally not modeled.

#### `File`

`File` is read-only and implements:

- `fs.File`
- `io.ReaderAt`
- `io.Seeker`

Read path:

- `Open` first runs `statPath`: `ListObjectsV2` checks directory projection, then `HeadObject` checks file metadata, then files are opened with `GetObject`.
- `ReadAt` uses S3 range requests and does not mutate sequential read offset.
- `Seek` reopens the object at the new offset.
- `IfMatch` is set on reads when an ETag is known, so one `File` does not mix object versions.
- Body read errors are wrapped as `fs.PathError`.

#### `Directory`

`Directory` is a lazy `fs.ReadDirFile`.

- `Open` of a directory only proves existence.
- `ReadDir(n > 0)` uses a paged cursor and returns a bounded batch, keeping only the current S3 page plus one pending entry.
- `ReadDir(n <= 0)` returns entries read before a late page error with that error.
- `Read` on a directory returns `fs.ErrInvalid`.

#### Directory Projection

S3 directory state is projected by `dirscan.go`.

- A shared page walker handles `ListObjectsV2` with `Delimiter=/` and `EncodingType=url`.
- Focused consumers implement existence, entry listing, and empty-directory marker collection.
- Invalid or hidden children make a directory exist and block `RemoveDir`, but are not returned as entries.
- `Fs.ReadDir` returns all entries sorted by name; `Directory.ReadDir` follows S3 page order with one-entry lookahead so directory shadows win.

#### Path Model

Public paths are logical `io/fs`-style paths.

- Root is `"."`.
- Empty path, absolute path, trailing slash, `.` component, `..` component, marker basename, ASCII control characters, DEL, and invalid UTF-8 are rejected.
- Prefix and directory marker options are normalized at construction.

### S3 Mapping

| Filesystem concept | S3 representation |
| --- | --- |
| File | Object at normalized key |
| Directory | Prefix containing visible children or marker object |
| Empty directory | Marker object named by `WithDirectoryFile`, default `.keep` |
| Root | Bucket plus optional normalized prefix |

### Testing

Main package tests use an in-memory fake S3 client with conformance tests for:

- `ListObjectsV2` pagination and URL encoding
- range reads and `IfMatch`
- multipart upload completion and abort
- writer failure cleanup
- reader/seek contracts
- path and directory projection fuzzing

`tests/` contains LocalStack integration tests and is intentionally skipped by unit gates unless Docker/S3 test infrastructure is running.

## License

MIT License

See [LICENSE](LICENSE) to see the full text.
