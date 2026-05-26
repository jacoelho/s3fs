package s3fs

import (
	"errors"
	"io/fs"
	"testing"
)

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		wantErr   error
		name      string
		want      logicalPath
		allowRoot bool
	}{
		{name: ".", allowRoot: true, want: ""},
		{name: "", allowRoot: true, wantErr: fs.ErrInvalid},
		{name: "test", want: "test"},
		{name: "test/file.txt", want: "test/file.txt"},
		{name: "/", allowRoot: true, wantErr: fs.ErrInvalid},
		{name: "/test", wantErr: fs.ErrInvalid},
		{name: "test/", wantErr: fs.ErrInvalid},
		{name: "test//file.txt", wantErr: fs.ErrInvalid},
		{name: "test/../file.txt", wantErr: fs.ErrInvalid},
		{name: "test/./file.txt", wantErr: fs.ErrInvalid},
		{name: ".keep", wantErr: fs.ErrInvalid},
		{name: "a/.keep", wantErr: fs.ErrInvalid},
		{name: "a/\n", wantErr: fs.ErrInvalid},
		{name: "a/\v", wantErr: fs.ErrInvalid},
		{name: "a/\r", wantErr: fs.ErrInvalid},
		{name: "a/" + string(rune(0x7f)), wantErr: fs.ErrInvalid},
		{name: string([]byte{'a', '/', 0xff}), wantErr: fs.ErrInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizePath(tt.name, defaultDirectoryFile, tt.allowRoot)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("normalizePath() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("normalizePath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizePrefix(t *testing.T) {
	got, err := normalizePrefix("a/b/", defaultDirectoryFile)
	if err != nil {
		t.Fatal(err)
	}
	if got != "a/b" {
		t.Fatalf("normalizePrefix() = %q, want %q", got, "a/b")
	}

	if _, err := normalizePrefix("/", defaultDirectoryFile); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("normalizePrefix() error = %v, want fs.ErrInvalid", err)
	}
}
