package s3fs

import (
	"cmp"
	"context"
	"io"
	"io/fs"
	"net/url"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (f *Fs) readDirEntries(ctx context.Context, p logicalPath) ([]fs.DirEntry, error) {
	var state dirEntries
	err := f.walkDirItems(ctx, p, nil, func(item dirItem) bool {
		state.add(item)
		return true
	})
	if err != nil {
		return nil, pathError("readdir", p.String(), err)
	}
	if !p.root() && !state.exists() {
		_, err := f.headFile(ctx, "readdir", p)
		if err == nil {
			return nil, pathError("readdir", p.String(), fs.ErrInvalid)
		}
		return nil, err
	}

	entries := make([]fs.DirEntry, 0, len(state.entries))
	for _, entry := range state.entries {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})
	return entries, nil
}

func (f *Fs) dirExists(ctx context.Context, p logicalPath) (bool, error) {
	exists := false
	err := f.walkDirItems(ctx, p, aws.Int32(1), func(dirItem) bool {
		exists = true
		return false
	})
	return exists, err
}

func (f *Fs) emptyDirMarkers(ctx context.Context, p logicalPath) (emptyDir, error) {
	state := emptyDir{empty: true}
	err := f.walkDirItems(ctx, p, nil, func(item dirItem) bool {
		state.exists = true
		if item.kind == dirItemMarker {
			state.markers = append(state.markers, item.key)
			return true
		}
		state.empty = false
		return false
	})
	return state, err
}

type dirEntries struct {
	entries map[string]dirEntry
	seen    bool
}

func (s dirEntries) exists() bool {
	return s.seen || len(s.entries) > 0
}

func (s *dirEntries) add(item dirItem) {
	s.seen = true
	if item.kind != dirItemDir && item.kind != dirItemFile {
		return
	}
	if s.entries == nil {
		s.entries = make(map[string]dirEntry)
	}
	if existing, ok := s.entries[item.entry.Name()]; ok && existing.IsDir() {
		return
	}
	s.entries[item.entry.Name()] = item.entry
}

type emptyDir struct {
	markers []string
	exists  bool
	empty   bool
}

type dirItemKind uint8

const (
	dirItemMarker dirItemKind = iota
	dirItemHidden
	dirItemDir
	dirItemFile
)

type dirItem struct {
	key   string
	entry dirEntry
	kind  dirItemKind
}

type dirPage struct {
	next  *string
	items []dirItem
}

type dirCursor struct {
	fs         *Fs
	token      *string
	path       logicalPath
	items      []dirItem
	pending    dirEntry
	index      int
	hasPending bool
	seen       bool
	done       bool
}

func newDirCursor(fsys *Fs, p logicalPath) *dirCursor {
	return &dirCursor{fs: fsys, path: p}
}

func (c *dirCursor) next(ctx context.Context) (fs.DirEntry, error) {
	for {
		item, ok, err := c.nextItem(ctx)
		if err != nil {
			return nil, pathError("readdir", c.path.String(), err)
		}
		if !ok {
			if c.hasPending {
				entry := c.pending
				c.hasPending = false
				return entry, nil
			}
			if !c.path.root() && !c.seen {
				return nil, c.missingDirErr(ctx)
			}
			return nil, io.EOF
		}

		c.seen = true
		if item.kind != dirItemDir && item.kind != dirItemFile {
			continue
		}
		if !c.hasPending {
			c.pending = item.entry
			c.hasPending = true
			continue
		}
		if c.pending.Name() == item.entry.Name() {
			if item.entry.IsDir() {
				c.pending = item.entry
			}
			continue
		}

		entry := c.pending
		c.pending = item.entry
		return entry, nil
	}
}

func (c *dirCursor) nextItem(ctx context.Context) (dirItem, bool, error) {
	for c.index >= len(c.items) {
		if c.done {
			return dirItem{}, false, nil
		}
		page, err := c.fs.listDirPage(ctx, c.path, c.token, nil)
		if err != nil {
			return dirItem{}, false, err
		}
		c.items = page.items
		c.index = 0
		c.token = page.next
		if c.token == nil {
			c.done = true
		}
	}

	item := c.items[c.index]
	c.index++
	return item, true, nil
}

func (c *dirCursor) missingDirErr(ctx context.Context) error {
	_, err := c.fs.headFile(ctx, "readdir", c.path)
	if err == nil {
		return pathError("readdir", c.path.String(), fs.ErrInvalid)
	}
	return err
}

func (f *Fs) walkDirItems(ctx context.Context, p logicalPath, maxKeys *int32, yield func(dirItem) bool) error {
	token := (*string)(nil)

	for {
		page, err := f.listDirPage(ctx, p, token, maxKeys)
		if err != nil {
			return err
		}

		for _, item := range page.items {
			if !yield(item) {
				return nil
			}
		}
		token = page.next
		if token == nil {
			return nil
		}
	}
}

func (f *Fs) listDirPage(ctx context.Context, p logicalPath, token *string, maxKeys *int32) (dirPage, error) {
	prefix := f.dirPrefix(p)
	opCtx, cancel := f.operationContext(ctx)
	out, err := f.client.ListObjectsV2(opCtx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(f.bucket),
		Prefix:            aws.String(prefix),
		Delimiter:         aws.String(pathSeparator),
		ContinuationToken: token,
		EncodingType:      types.EncodingTypeUrl,
		MaxKeys:           maxKeys,
	})
	cancel()
	if err != nil {
		return dirPage{}, err
	}

	items := make([]dirItem, 0, len(out.CommonPrefixes)+len(out.Contents))
	items = f.appendPrefixes(prefix, out.CommonPrefixes, items)
	items = f.appendObjects(prefix, out.Contents, items)
	slices.SortFunc(items, compareDirItems)

	var next *string
	if aws.ToBool(out.IsTruncated) {
		next = out.NextContinuationToken
	}
	return dirPage{items: items, next: next}, nil
}

func (f *Fs) appendPrefixes(prefix string, prefixes []types.CommonPrefix, items []dirItem) []dirItem {
	for _, commonPrefix := range prefixes {
		if commonPrefix.Prefix == nil {
			continue
		}
		key, err := decodeS3Key(*commonPrefix.Prefix)
		if err != nil {
			items = append(items, dirItem{kind: dirItemHidden, key: *commonPrefix.Prefix})
			continue
		}
		name, ok := immediateChild(prefix, key, true)
		if !ok || !validEntryName(name, f.directoryFile) {
			items = append(items, dirItem{kind: dirItemHidden, key: key})
			continue
		}
		items = append(items, dirItem{kind: dirItemDir, key: key, entry: dirEntry{info: directoryFileInfo(name, name)}})
	}
	return items
}

func (f *Fs) appendObjects(prefix string, objects []types.Object, items []dirItem) []dirItem {
	for _, obj := range objects {
		if obj.Key == nil {
			continue
		}
		key, err := decodeS3Key(*obj.Key)
		if err != nil {
			items = append(items, dirItem{kind: dirItemHidden, key: *obj.Key})
			continue
		}
		if key == prefix {
			items = append(items, dirItem{kind: dirItemMarker, key: key})
			continue
		}
		if key == prefix+f.directoryFile {
			items = append(items, dirItem{kind: dirItemMarker, key: key})
			continue
		}

		name, ok := immediateChild(prefix, key, false)
		if !ok || !validEntryName(name, f.directoryFile) {
			items = append(items, dirItem{kind: dirItemHidden, key: key})
			continue
		}
		info := regularFileInfo(name, name, aws.ToInt64(obj.Size), aws.ToTime(obj.LastModified), aws.ToString(obj.ETag))
		items = append(items, dirItem{kind: dirItemFile, key: key, entry: dirEntry{info: info}})
	}
	return items
}

func compareDirItems(a, b dirItem) int {
	if n := cmp.Compare(a.sortName(), b.sortName()); n != 0 {
		return n
	}
	return cmp.Compare(a.kind, b.kind)
}

func (i dirItem) sortName() string {
	if i.kind == dirItemDir || i.kind == dirItemFile {
		return i.entry.Name()
	}
	return i.key
}

func decodeS3Key(key string) (string, error) {
	return url.PathUnescape(key)
}

func immediateChild(prefix, key string, dir bool) (string, bool) {
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	if dir {
		rest = strings.TrimSuffix(rest, pathSeparator)
	}
	if rest == "" || strings.Contains(rest, pathSeparator) {
		return "", false
	}
	return rest, true
}
