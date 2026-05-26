package s3fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type fakeS3 struct {
	readErr  error
	objects  map[string]fakeObject
	uploads  map[string]*fakeUpload
	failures map[string][]error
	calls    []fakeCall
	nextID   int
	closes   int
	pageSize int
	mu       sync.Mutex
	readCtx  bool
}

type fakeObject struct {
	modTime time.Time
	etag    string
	body    []byte
}

type fakeUpload struct {
	parts    map[int32]fakePart
	key      string
	checksum types.ChecksumAlgorithm
	aborted  bool
}

type fakePart struct {
	out  s3.UploadPartOutput
	body []byte
}

type fakeCall struct {
	op        string
	key       string
	rangeHdr  string
	ifMatch   string
	checksum  types.ChecksumAlgorithm
	uploadID  string
	completed []types.CompletedPart
	size      int64
	part      int32
}

func newFakeS3() *fakeS3 {
	return &fakeS3{
		objects:  make(map[string]fakeObject),
		uploads:  make(map[string]*fakeUpload),
		failures: make(map[string][]error),
	}
}

func (s *fakeS3) fail(op string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[op] = append(s.failures[op], err)
}

func (s *fakeS3) object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	return append([]byte(nil), obj.body...), ok
}

func (s *fakeS3) callsFor(op string) []fakeCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var calls []fakeCall
	for _, call := range s.calls {
		if call.op == op {
			calls = append(calls, call)
		}
	}
	return calls
}

func (s *fakeS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "HeadObject", key: key})
	if err := s.nextFailure("HeadObject"); err != nil {
		return nil, err
	}

	obj, ok := s.objects[key]
	if !ok {
		return nil, notFoundErr()
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(obj.body))),
		LastModified:  aws.Time(obj.modTime),
		ETag:          aws.String(obj.etag),
	}, nil
}

func (s *fakeS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "PutObject", key: key, size: int64(len(body))})
	if err := s.nextFailure("PutObject"); err != nil {
		return nil, err
	}

	obj := fakeObject{body: body, etag: etag(body), modTime: time.Unix(1, 0).UTC()}
	s.objects[key] = obj
	return &s3.PutObjectOutput{ETag: aws.String(obj.etag)}, nil
}

func (s *fakeS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := aws.ToString(in.Key)
	call := fakeCall{op: "GetObject", key: key, rangeHdr: aws.ToString(in.Range), ifMatch: aws.ToString(in.IfMatch)}
	s.calls = append(s.calls, call)
	if err := s.nextFailure("GetObject"); err != nil {
		return nil, err
	}

	obj, ok := s.objects[key]
	if !ok {
		return nil, notFoundErr()
	}
	if in.IfMatch != nil && aws.ToString(in.IfMatch) != obj.etag {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "precondition failed"}
	}

	start, end, err := parseRange(aws.ToString(in.Range), int64(len(obj.body)))
	if err != nil {
		return nil, err
	}
	var body io.Reader = bytes.NewReader(obj.body[start : end+1])
	if s.readErr != nil {
		body = errReader{err: s.readErr}
	}
	if s.readCtx {
		body = contextReader{ctx: ctx, reader: body}
	}
	return &s3.GetObjectOutput{
		Body:          &trackedReadCloser{Reader: body, onClose: func() { s.addClose() }},
		ContentLength: aws.Int64(end - start + 1),
		ETag:          aws.String(obj.etag),
	}, nil
}

func (s *fakeS3) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "DeleteObject", key: key})
	if err := s.nextFailure("DeleteObject"); err != nil {
		return nil, err
	}
	delete(s.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func (s *fakeS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := aws.ToString(in.Prefix)
	s.calls = append(s.calls, fakeCall{op: "ListObjectsV2", key: prefix})
	if err := s.nextFailure("ListObjectsV2"); err != nil {
		return nil, err
	}

	var items []listItem
	seenPrefix := map[string]struct{}{}
	for key, obj := range s.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if delimiter := aws.ToString(in.Delimiter); delimiter != "" {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				common := prefix + rest[:idx+len(delimiter)]
				if _, ok := seenPrefix[common]; !ok {
					seenPrefix[common] = struct{}{}
					items = append(items, listItem{prefix: common})
				}
				continue
			}
		}
		items = append(items, listItem{key: key, obj: obj})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].value() < items[j].value()
	})

	start := 0
	if token := aws.ToString(in.ContinuationToken); token != "" {
		n, err := strconv.Atoi(token)
		if err != nil {
			return nil, err
		}
		start = n
	}
	limit := len(items)
	pageSize := s.effectivePageSize(in.MaxKeys)
	if pageSize < limit-start {
		limit = start + pageSize
	}

	out := &s3.ListObjectsV2Output{}
	for _, item := range items[start:limit] {
		if item.prefix != "" {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(encodeKey(in.EncodingType, item.prefix))})
			continue
		}
		out.Contents = append(out.Contents, types.Object{
			Key:          aws.String(encodeKey(in.EncodingType, item.key)),
			Size:         aws.Int64(int64(len(item.obj.body))),
			LastModified: aws.Time(item.obj.modTime),
			ETag:         aws.String(item.obj.etag),
		})
	}
	if limit < len(items) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(strconv.Itoa(limit))
	}
	return out, nil
}

func (s *fakeS3) effectivePageSize(maxKeys *int32) int {
	pageSize := 1000
	if s.pageSize > 0 {
		pageSize = s.pageSize
	}
	if maxKeys != nil && int(aws.ToInt32(maxKeys)) < pageSize {
		pageSize = int(aws.ToInt32(maxKeys))
	}
	if pageSize <= 0 {
		return 1000
	}
	return pageSize
}

func (s *fakeS3) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "CreateMultipartUpload", key: key, checksum: in.ChecksumAlgorithm})
	if err := s.nextFailure("CreateMultipartUpload"); err != nil {
		return nil, err
	}

	s.nextID++
	id := fmt.Sprintf("upload-%d", s.nextID)
	s.uploads[id] = &fakeUpload{key: key, parts: make(map[int32]fakePart), checksum: in.ChecksumAlgorithm}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (s *fakeS3) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := aws.ToString(in.UploadId)
	partNumber := aws.ToInt32(in.PartNumber)
	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "UploadPart", key: key, uploadID: id, part: partNumber, size: int64(len(body)), checksum: in.ChecksumAlgorithm})
	if err := s.nextFailure("UploadPart"); err != nil {
		return nil, err
	}

	up, ok := s.uploads[id]
	if !ok || up.aborted {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"}
	}
	if up.key != key {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequest", Message: "upload key mismatch"}
	}
	checksum := crc32Like(body)
	out := s3.UploadPartOutput{
		ETag:          aws.String(etag(body)),
		ChecksumCRC32: aws.String(checksum),
	}
	up.parts[partNumber] = fakePart{body: body, out: out}
	return &out, nil
}

func (s *fakeS3) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	id := aws.ToString(in.UploadId)
	key := aws.ToString(in.Key)
	parts := append([]types.CompletedPart(nil), in.MultipartUpload.Parts...)
	s.calls = append(s.calls, fakeCall{op: "CompleteMultipartUpload", key: key, uploadID: id, size: aws.ToInt64(in.MpuObjectSize), completed: parts})
	if err := s.nextFailure("CompleteMultipartUpload"); err != nil {
		return nil, err
	}

	up, ok := s.uploads[id]
	if !ok || up.aborted {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"}
	}
	if up.key != key {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequest", Message: "upload key mismatch"}
	}

	var body []byte
	var last int32
	for _, completed := range parts {
		partNumber := aws.ToInt32(completed.PartNumber)
		if partNumber != last+1 {
			return nil, &smithy.GenericAPIError{Code: "InvalidPartOrder", Message: "invalid part order"}
		}
		part, ok := up.parts[partNumber]
		if !ok || aws.ToString(completed.ETag) != aws.ToString(part.out.ETag) {
			return nil, &smithy.GenericAPIError{Code: "InvalidPart", Message: "invalid part"}
		}
		body = append(body, part.body...)
		last = partNumber
	}
	if in.MpuObjectSize != nil && aws.ToInt64(in.MpuObjectSize) != int64(len(body)) {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequest", Message: "size mismatch"}
	}

	obj := fakeObject{body: body, etag: etag(body), modTime: time.Unix(1, 0).UTC()}
	s.objects[key] = obj
	delete(s.uploads, id)
	return &s3.CompleteMultipartUploadOutput{ETag: aws.String(obj.etag)}, nil
}

func (s *fakeS3) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	id := aws.ToString(in.UploadId)
	key := aws.ToString(in.Key)
	s.calls = append(s.calls, fakeCall{op: "AbortMultipartUpload", key: key, uploadID: id})
	if err := s.nextFailure("AbortMultipartUpload"); err != nil {
		return nil, err
	}

	up, ok := s.uploads[id]
	if !ok || up.aborted {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "missing upload"}
	}
	if up.key != key {
		return nil, &smithy.GenericAPIError{Code: "InvalidRequest", Message: "upload key mismatch"}
	}
	up.aborted = true
	delete(s.uploads, id)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (s *fakeS3) nextFailure(op string) error {
	failures := s.failures[op]
	if len(failures) == 0 {
		return nil
	}
	err := failures[0]
	s.failures[op] = failures[1:]
	return err
}

func (s *fakeS3) addClose() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
}

type listItem struct {
	key    string
	prefix string
	obj    fakeObject
}

func (i listItem) value() string {
	if i.prefix != "" {
		return i.prefix
	}
	return i.key
}

type trackedReadCloser struct {
	io.Reader
	onClose func()
	closed  bool
}

type errReader struct {
	err error
}

func (r errReader) Read([]byte) (int, error) {
	return 0, r.err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (r *trackedReadCloser) Close() error {
	if !r.closed {
		r.closed = true
		r.onClose()
	}
	return nil
}

func notFoundErr() error {
	return &smithy.GenericAPIError{Code: "NoSuchKey", Message: "not found"}
}

func etag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func crc32Like(body []byte) string {
	sum := sha256.Sum256(body)
	return base64.StdEncoding.EncodeToString(sum[:4])
}

func parseRange(header string, size int64) (int64, int64, error) {
	if size == 0 {
		return 0, -1, nil
	}
	if header == "" {
		return 0, size - 1, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range: %q", header)
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range: %q", header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end := size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	if start < 0 || end < start || start >= size {
		return 0, 0, fmt.Errorf("invalid range: %q", header)
	}
	if end >= size {
		end = size - 1
	}
	return start, end, nil
}

func encodeKey(encoding types.EncodingType, key string) string {
	if encoding == types.EncodingTypeUrl {
		return strings.ReplaceAll(url.QueryEscape(key), "+", "%20")
	}
	return key
}

func newTestFS(t testingT, client s3ApiClient, opts ...Option) *Fs {
	t.Helper()

	f, err := New(client, "bucket", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

type testingT interface {
	Helper()
	Fatal(args ...any)
}
