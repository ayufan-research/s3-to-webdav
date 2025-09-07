package fs

import (
	"path/filepath"
	"strings"
)

type EntryInfo struct {
	Path         string
	Size         int64
	LastModified int64
	IsDir        bool
	Processed    bool
}

// BucketAndKeyFromPath extracts bucket and key from path
func BucketAndKeyFromPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", "", false
	}
	bucket := parts[0]
	key := strings.Join(parts[1:], "/")
	return bucket, key, true
}

// PathFromBucketAndKey creates path from bucket and key
func PathFromBucketAndKey(bucket, key string) string {
	if bucket == "" {
		return ""
	}
	if key == "" {
		return bucket
	}
	return bucket + "/" + key
}

func BaseDirEntries(path string) []EntryInfo {
	var entryInfos []EntryInfo

	for {
		path = filepath.Dir(path)
		if path == "." || path == "/" {
			break
		}

		entryInfos = append(entryInfos, EntryInfo{
			Path:         path + "/",
			Size:         0,
			LastModified: 0,
			IsDir:        true,
			Processed:    true,
		})
	}

	return entryInfos
}
