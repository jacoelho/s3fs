package s3fs

import (
	"fmt"
	"testing"
)

func TestClean(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: ".",
			want: "",
		},
		{
			name: "/",
			want: "",
		},

		{
			name: "/test",
			want: "test",
		},
		{
			name: "/test/file.txt",
			want: "test/file.txt",
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("name: %s", tt.name), func(t *testing.T) {
			if got := cleanPath(tt.name); got != tt.want {
				t.Errorf("cleanPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
