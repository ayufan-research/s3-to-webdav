package fs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketAndKeyFromPathAndPathFromBucketAndKey(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBucket string
		wantKey    string
		wantOk     bool
		wantPath   string
	}{
		{
			name:       "root path",
			path:       "",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
			wantPath:   "",
		},
		{
			name:       "empty path",
			path:       "",
			wantBucket: "",
			wantKey:    "",
			wantOk:     false,
			wantPath:   "",
		},
		{
			name:       "bucket only",
			path:       "mybucket",
			wantBucket: "mybucket",
			wantKey:    "",
			wantOk:     true,
			wantPath:   "mybucket",
		},
		{
			name:       "bucket with trailing slash",
			path:       "mybucket/",
			wantBucket: "mybucket",
			wantKey:    "",
			wantOk:     true,
			wantPath:   "mybucket",
		},
		{
			name:       "bucket with key",
			path:       "mybucket/mykey",
			wantBucket: "mybucket",
			wantKey:    "mykey",
			wantOk:     true,
			wantPath:   "mybucket/mykey",
		},
		{
			name:       "bucket with nested key",
			path:       "mybucket/folder/file.txt",
			wantBucket: "mybucket",
			wantKey:    "folder/file.txt",
			wantOk:     true,
			wantPath:   "mybucket/folder/file.txt",
		},
		{
			name:       "bucket with deeply nested key",
			path:       "mybucket/folder1/folder2/folder3/file.txt",
			wantBucket: "mybucket",
			wantKey:    "folder1/folder2/folder3/file.txt",
			wantOk:     true,
			wantPath:   "mybucket/folder1/folder2/folder3/file.txt",
		},
		{
			name:       "no leading slash",
			path:       "mybucket/mykey",
			wantBucket: "mybucket",
			wantKey:    "mykey",
			wantOk:     true,
			wantPath:   "mybucket/mykey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, ok := BucketAndKeyFromPath(tt.path)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantKey, key)
			assert.Equal(t, tt.wantOk, ok)

			if tt.wantOk {
				reconstructedPath := PathFromBucketAndKey(bucket, key)
				assert.Equal(t, tt.wantPath, reconstructedPath)
			}
		})
	}
}

func TestPathFromBucketAndKeyEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		key    string
		want   string
	}{
		{
			name:   "empty bucket with key",
			bucket: "",
			key:    "somekey",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PathFromBucketAndKey(tt.bucket, tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBaseDirEntries(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []EntryInfo
	}{
		{
			name: "root path",
			path: "",
			want: []EntryInfo{},
		},
		{
			name: "bucket path",
			path: "mybucket",
			want: []EntryInfo{},
		},
		{
			name: "file in bucket",
			path: "mybucket/file.txt",
			want: []EntryInfo{
				{
					Path:         "mybucket/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
			},
		},
		{
			name: "nested file",
			path: "mybucket/folder/file.txt",
			want: []EntryInfo{
				{
					Path:         "mybucket/folder/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
				{
					Path:         "mybucket/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
			},
		},
		{
			name: "deeply nested file",
			path: "mybucket/folder1/folder2/folder3/file.txt",
			want: []EntryInfo{
				{
					Path:         "mybucket/folder1/folder2/folder3/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
				{
					Path:         "mybucket/folder1/folder2/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
				{
					Path:         "mybucket/folder1/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
				{
					Path:         "mybucket/",
					Size:         0,
					LastModified: 0,
					IsDir:        true,
					Processed:    true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BaseDirEntries(tt.path)
			require.Equal(t, len(tt.want), len(got))
			for i, entry := range got {
				want := tt.want[i]
				assert.Equal(t, want.Path, entry.Path)
				assert.Equal(t, want.Size, entry.Size)
				assert.Equal(t, want.LastModified, entry.LastModified)
				assert.Equal(t, want.IsDir, entry.IsDir)
				assert.Equal(t, want.Processed, entry.Processed)
			}
		})
	}
}
