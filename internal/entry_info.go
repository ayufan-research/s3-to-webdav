package internal

import (
	"strings"
)

type EntryInfo struct {
	Path         string
	Bucket       string
	Key          string
	Size         int64
	LastModified int64
	IsDir        bool
	ProcessedAt  int64
}

// BucketAndKeyFromPath extracts bucketi and key from path
func BucketAndKeyFromPath(path string) (string, string, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", "", nil
	}
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")
	return bucket, key, nil
}

// PathFromBucketAndKey creates path from bucket and key
func PathFromBucketAndKey(bucket, key string) string {
	if bucket == "" {
		return "/"
	}
	if key == "" {
		return "/" + bucket
	}
	return "/" + bucket + "/" + key
}