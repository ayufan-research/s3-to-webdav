package cache

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"s3-to-webdav/internal/fs"
)

func forEachBenchmarkBackend(t *testing.B, testFunc func(t *testing.B, cache Cache)) {
	tempDir, err := os.MkdirTemp("", "cache_test_backends_*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	t.Run("SQLite", func(t *testing.B) {
		cache, err := NewCacheDB(fmt.Sprintf("%s/sqlite_cache.db", tempDir))
		require.NoError(t, err)
		t.Cleanup(func() { cache.Close() })
		testFunc(t, cache)
	})
}

func generateSHA256Hex() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	hash := sha256.Sum256(bytes)
	return fmt.Sprintf("%x", hash)
}

func createTestObjects(count int, buckets ...string) []fs.EntryInfo {
	objects := make([]fs.EntryInfo, 0, count*2)
	now := time.Now().Unix()
	dirMap := make(map[string]bool)
	folders := []string{"folder-a", "folder-b"}

	for i := 0; i < count; i++ {
		hex := generateSHA256Hex()
		folder := folders[i%len(folders)]
		bucket := buckets[i%len(buckets)]

		key := fmt.Sprintf("%s/%s/%s", folder, hex[0:4], hex)
		path := fs.PathFromBucketAndKey(bucket, key)

		objects = append(objects, fs.EntryInfo{
			Path:         path,
			Size:         int64(1000 + i%10000),
			LastModified: now - int64(i%1000),
			IsDir:        false,
			Processed:    i%3 == 0,
		})

		for _, entry := range fs.BaseDirEntries(path) {
			if !dirMap[entry.Path] {
				dirMap[entry.Path] = true
				objects = append(objects, entry)
			}
		}
	}

	return objects
}

func BenchmarkList(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, cache Cache) {
		objects := createTestObjects(10000, "test-bucket")

		require.NoError(b, cache.Insert(objects...))
		require.NoError(b, cache.Optimise())

		b.Run("all objects", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := cache.List("test-bucket/", "", false, 100)
				require.NoError(b, err)
			}
		})

		b.Run("with prefix", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := cache.List("test-bucket/folder-b/", "", false, 100)
				require.NoError(b, err)
			}
		})

		b.Run("with invalid prefix", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := cache.List("test-bucket/folder-c/", "", false, 100)
				require.NoError(b, err)
			}
		})

		b.Run("with marker", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := cache.List("test-bucket/folder-a/", "test-bucket/folder-a/8000", false, 100)
				require.NoError(b, err)
			}
		})

		b.Run("dir only", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := cache.List("test-bucket/folder-a/", "", true, 100)
				require.NoError(b, err)
			}
		})

		b.Run("traverse all", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				marker := ""
				for {
					results, truncated, err := cache.List("test-bucket/", marker, false, 500)
					require.NoError(b, err)
					if !truncated || len(results) == 0 {
						break
					}
					marker = results[len(results)-1].Path
				}
			}
		})
	})
}

func BenchmarkListPendingDirs(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, cache Cache) {
		dirObjects := make([]fs.EntryInfo, 1000)
		now := time.Now().Unix()

		for i := 0; i < 1000; i++ {
			hex := generateSHA256Hex()
			folderType := "folder-a"
			if i%2 == 1 {
				folderType = "folder-b"
			}

			key := fmt.Sprintf("%s/%s/", folderType, hex[0:4])
			path := fs.PathFromBucketAndKey("test-bucket", key)

			dirObjects[i] = fs.EntryInfo{
				Path:         path,
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    i%3 != 0,
			}
		}

		require.NoError(b, cache.Insert(dirObjects...))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := cache.ListPendingDirs("test-bucket/", 100)
			require.NoError(b, err)
		}
	})
}

func BenchmarkListDanglingDirs(b *testing.B) {
	forEachBenchmarkBackend(b, func(b *testing.B, cache Cache) {
		dirObjects := make([]fs.EntryInfo, 1000)
		now := time.Now().Unix()

		for i := 0; i < 1000; i++ {
			hex := generateSHA256Hex()
			folderType := "folder-a"
			if i%2 == 1 {
				folderType = "folder-b"
			}

			key := fmt.Sprintf("%s/%s/", folderType, hex[0:4])
			path := fs.PathFromBucketAndKey("test-bucket", key)

			dirObjects[i] = fs.EntryInfo{
				Path:         path,
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    true,
			}
		}

		require.NoError(b, cache.Insert(dirObjects...))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := cache.ListDanglingDirs("test-bucket/", 100)
			require.NoError(b, err)
		}
	})
}
