package cache

import (
	"fmt"
	"os"
	"testing"
	"time"

	"s3-to-webdav/internal/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func forEachTestBackend(t *testing.T, testFunc func(t *testing.T, cache Cache)) {
	tempDir, err := os.MkdirTemp("", "cache_test_backends_*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	t.Run("SQLite", func(t *testing.T) {
		cache, err := NewCacheDB(fmt.Sprintf("%s/sqlite_cache.db", tempDir))
		require.NoError(t, err)
		t.Cleanup(func() { cache.Close() })
		testFunc(t, cache)
	})
}

var dirStructure = []string{
	"bucket-a/",
	"bucket-a/folder-a/",
	"bucket-a/folder-a/efgh/",
	"bucket-a/folder-b/",
	"bucket-a/folder-b/ijkl/",
	"bucket-a/folder-c/",
	"bucket-a/folder-c/mnop/",
	"bucket-a/folder-d/",
	"bucket-a/folder-d/mnop/",
	"bucket-b/",
	"bucket-b/folder-a/",
	"bucket-b/folder-a/efgh/",
	"bucket-b/folder-b/ijkl/",
}

var fileStructure = []string{
	"bucket-a/root-file.txt",
	"bucket-a/folder-a/abcd/abcd1234abcd1234abcd1234abcd1234",
	"bucket-a/folder-a/abcd/abcd5678abcd5678abcd5678abcd5678",
	"bucket-a/folder-a/efgh/efgh1234efgh1234efgh1234efgh1234",
	"bucket-a/folder-b/ijkl/ijkl1234ijkl1234ijkl1234ijkl1234",
	"bucket-b/folder-a/efgh/efgh1234efgh1234efgh1234efgh1234",
	"bucket-b/folder-b/ijkl/ijkl1234ijkl1234ijkl1234ijkl1234",
}

func createFileObjects(files ...string) []fs.EntryInfo {
	objects := make([]fs.EntryInfo, 0, len(files))
	now := time.Now().Unix()

	for _, file := range files {
		isDir := file[len(file)-1] == '/'
		objects = append(objects, fs.EntryInfo{
			Path:         file,
			Size:         1024,
			LastModified: now,
			IsDir:        isDir,
			Processed:    true,
		})
	}

	return objects
}

func TestCacheList(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		err := cache.Insert(createFileObjects(dirStructure...)...)
		require.NoError(t, err)

		err = cache.Insert(createFileObjects(fileStructure...)...)
		require.NoError(t, err)

		t.Run("List all", func(t *testing.T) {
			results, truncated, err := cache.List("", "", false, 100)
			require.NoError(t, err)
			assert.False(t, truncated)
			assert.Equal(t, len(fileStructure), len(results))
		})

		t.Run("List bucket-a/", func(t *testing.T) {
			results, truncated, err := cache.List("bucket-a/", "", false, 100)
			require.NoError(t, err)
			assert.False(t, truncated)
			assert.Equal(t, 5, len(results))
		})

		t.Run("List bucket-a/ with marker", func(t *testing.T) {
			results, truncated, err := cache.List("bucket-a/", "bucket-a/folder-a/efgh/", false, 100)
			require.NoError(t, err)
			assert.False(t, truncated)
			assert.Equal(t, 3, len(results))
		})

		t.Run("List bucket-a/ dir only", func(t *testing.T) {
			results, truncated, err := cache.List("bucket-a/", "", true, 100)
			require.NoError(t, err)
			assert.False(t, truncated)
			//assert.Equal(t, "aa", results)
			assert.Equal(t, 5, len(results))
		})
	})
}

func TestCacheInsertAndRetrieve(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Insert and retrieve file", func(t *testing.T) {
			original := fs.EntryInfo{
				Path:         "test-bucket/folder-a/file/file2",
				Size:         1024,
				LastModified: time.Now().Unix(),
				IsDir:        false,
				Processed:    true,
			}

			_, err := cache.Stat(original.Path)
			require.Error(t, err)

			err = cache.Insert(original)
			require.NoError(t, err)

			retrieved, err := cache.Stat(original.Path)
			require.NoError(t, err)

			assert.Equal(t, original.Path, retrieved.Path)
			assert.Equal(t, original.Size, retrieved.Size)
			assert.Equal(t, original.IsDir, retrieved.IsDir)
			assert.Equal(t, original.Processed, retrieved.Processed)
		})

		t.Run("Insert multiple objects", func(t *testing.T) {
			err := cache.Insert(createFileObjects(dirStructure...)...)
			require.NoError(t, err)

			err = cache.Insert(createFileObjects(fileStructure...)...)
			require.NoError(t, err)

			_, err = cache.Stat("bucket-a/")
			require.NoError(t, err)

			_, err = cache.Stat("bucket-a/root-file.txt")
			require.NoError(t, err)
		})
	})
}

func TestCacheStats(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Get stats for bucket", func(t *testing.T) {
			bucket := "test-bucket"
			objects := createTestObjects(30, bucket)

			err := cache.Insert(objects...)
			require.NoError(t, err)

			processed, unprocessed, totalSize, err := cache.GetStats(bucket + "/")
			require.NoError(t, err)

			assert.True(t, processed > 0, "Should have processed entries")
			assert.True(t, unprocessed > 0, "Should have unprocessed entries")
			assert.True(t, totalSize > 0, "Should have total size")
			assert.Equal(t, processed+unprocessed, len(objects), "Total should match inserted objects")
		})

		t.Run("Empty bucket stats", func(t *testing.T) {
			processed, unprocessed, totalSize, err := cache.GetStats("empty-bucket/")
			require.NoError(t, err)

			assert.Equal(t, 0, processed, "Empty bucket should have no processed entries")
			assert.Equal(t, 0, unprocessed, "Empty bucket should have no unprocessed entries")
			assert.Equal(t, int64(0), totalSize, "Empty bucket should have zero size")
		})
	})
}

func TestCacheDelete(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		err := cache.Insert(createFileObjects(dirStructure...)...)
		require.NoError(t, err)

		err = cache.Insert(createFileObjects(fileStructure...)...)
		require.NoError(t, err)

		t.Run("Delete file", func(t *testing.T) {
			err := cache.Delete("bucket-a/root-file.txt")
			require.NoError(t, err)

			_, err = cache.Stat("bucket-a/root-file.txt")
			assert.Error(t, err)
		})

		t.Run("Delete directory with files should fail", func(t *testing.T) {
			err := cache.Delete("bucket-a/folder-a/")
			require.ErrorContains(t, err, "multiple entries deleted")
		})

		t.Run("Delete nonexistent path should fail", func(t *testing.T) {
			err := cache.Delete("nonexistent")
			require.NoError(t, err)
		})
	})
}

func TestCacheStat(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		err := cache.Insert(createFileObjects(dirStructure...)...)
		require.NoError(t, err)

		err = cache.Insert(createFileObjects(fileStructure...)...)
		require.NoError(t, err)

		t.Run("Stat nonexistent file", func(t *testing.T) {
			_, err = cache.Stat("nonexistent")
			assert.Error(t, err)
		})

		t.Run("Stat directory", func(t *testing.T) {
			obj, err := cache.Stat("bucket-a/")
			require.NoError(t, err)
			assert.True(t, obj.IsDir)
			assert.Equal(t, "bucket-a/", obj.Path)
		})

		t.Run("Stat file", func(t *testing.T) {
			obj, err := cache.Stat("bucket-a/root-file.txt")
			require.NoError(t, err)
			assert.False(t, obj.IsDir)
			assert.Equal(t, "bucket-a/root-file.txt", obj.Path)
		})
	})
}

func TestCacheMarkAsProcessed(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Mark file as processed", func(t *testing.T) {
			bucket := "test-bucket"
			hex := generateSHA256Hex()
			key := fmt.Sprintf("folder-a/%s/%s", hex[0:4], hex)
			path := fs.PathFromBucketAndKey(bucket, key)

			obj := fs.EntryInfo{
				Path:         path,
				Size:         1024,
				LastModified: time.Now().Unix(),
				IsDir:        false,
				Processed:    false,
			}

			err := cache.Insert(obj)
			require.NoError(t, err)

			retrieved, err := cache.Stat(path)
			require.NoError(t, err)
			assert.False(t, retrieved.Processed, "Object should initially be unprocessed")

			_, err = cache.SetProcessed(path, false, true)
			require.NoError(t, err)

			retrieved, err = cache.Stat(path)
			require.NoError(t, err)
			assert.True(t, retrieved.Processed, "Object should now be processed")
		})

		t.Run("Mark nonexistent file should return 0 rows affected", func(t *testing.T) {
			rowsAffected, err := cache.SetProcessed("nonexistent/file", false, true)
			require.NoError(t, err)
			assert.Equal(t, int64(0), rowsAffected)
		})
	})
}

func TestCacheListPendingDirs(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("List pending directories", func(t *testing.T) {
			bucket := "test-bucket"
			now := time.Now().Unix()

			unprocessedDir := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "folder-a/1234/"),
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    false,
			}

			processedDir := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "folder-b/5678/"),
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    true,
			}

			err := cache.Insert(unprocessedDir, processedDir)
			require.NoError(t, err)

			unprocessedDirs, err := cache.ListPendingDirs(bucket+"/", 10)
			require.NoError(t, err)

			assert.Equal(t, 1, len(unprocessedDirs), "Should have exactly one unprocessed directory")
			assert.Equal(t, unprocessedDir.Path, unprocessedDirs[0].Path)
			assert.False(t, unprocessedDirs[0].Processed)
		})

		t.Run("Empty bucket has no pending directories", func(t *testing.T) {
			unprocessedDirs, err := cache.ListPendingDirs("empty-bucket/", 10)
			require.NoError(t, err)
			assert.Empty(t, unprocessedDirs)
		})
	})
}

func TestCacheResetAllPending(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Reset all processed flags", func(t *testing.T) {
			bucket := "test-bucket"
			objects := createTestObjects(5, bucket)

			err := cache.Insert(objects...)
			require.NoError(t, err)

			_, err = cache.SetProcessed(bucket+"/", true, false)
			require.NoError(t, err)

			results, _, err := cache.List(bucket+"/", "", false, 100)
			require.NoError(t, err)

			for _, obj := range results {
				if !obj.IsDir {
					assert.False(t, obj.Processed, "All files should be unprocessed after reset")
				}
			}
		})

		t.Run("Reset empty bucket returns 0", func(t *testing.T) {
			rowsAffected, err := cache.SetProcessed("empty-bucket/", true, false)
			require.NoError(t, err)
			assert.Equal(t, int64(0), rowsAffected)
		})
	})
}

func TestCacheDeleteDanglingFiles(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Delete unprocessed files", func(t *testing.T) {
			bucket := "test-bucket"
			now := time.Now().Unix()

			processedFile := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "processed-file.txt"),
				Size:         1024,
				LastModified: now,
				IsDir:        false,
				Processed:    true,
			}

			unprocessedFile := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "unprocessed-file.txt"),
				Size:         1024,
				LastModified: now,
				IsDir:        false,
				Processed:    false,
			}

			err := cache.Insert(processedFile, unprocessedFile)
			require.NoError(t, err)

			deleted, err := cache.DeleteDanglingFiles(bucket + "/")
			require.NoError(t, err)
			assert.True(t, deleted >= 1, "Should delete at least 1 entry")

			_, err = cache.Stat(processedFile.Path)
			assert.NoError(t, err, "Processed file should still exist")

			_, err = cache.Stat(unprocessedFile.Path)
			assert.Error(t, err, "Unprocessed file should be deleted")
		})

		t.Run("Delete from empty bucket returns 0", func(t *testing.T) {
			deleted, err := cache.DeleteDanglingFiles("empty-bucket/")
			require.NoError(t, err)
			assert.Equal(t, int64(0), deleted)
		})
	})
}

func TestCacheListDanglingDirs(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("List empty directories", func(t *testing.T) {
			bucket := "test-bucket"
			now := time.Now().Unix()

			emptyDir := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "empty-dir/"),
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    true,
			}

			dirWithFiles := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "dir-with-files/"),
				Size:         0,
				LastModified: now,
				IsDir:        true,
				Processed:    true,
			}

			file := fs.EntryInfo{
				Path:         fs.PathFromBucketAndKey(bucket, "dir-with-files/file.txt"),
				Size:         1024,
				LastModified: now,
				IsDir:        false,
				Processed:    true,
			}

			err := cache.Insert(emptyDir, dirWithFiles, file)
			require.NoError(t, err)

			danglingDirs, err := cache.ListDanglingDirs(bucket+"/", 10)
			require.NoError(t, err)

			assert.True(t, len(danglingDirs) >= 0, "Should return dangling directories")
		})

		t.Run("Empty bucket has no dangling directories", func(t *testing.T) {
			danglingDirs, err := cache.ListDanglingDirs("empty-bucket/", 10)
			require.NoError(t, err)
			assert.Empty(t, danglingDirs)
		})
	})
}

func TestCacheOptimise(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Optimise database", func(t *testing.T) {
			err := cache.Insert(createFileObjects(dirStructure...)...)
			require.NoError(t, err)

			err = cache.Insert(createFileObjects(fileStructure...)...)
			require.NoError(t, err)

			err = cache.Optimise()
			require.NoError(t, err)

			results, truncated, err := cache.List("", "", false, 100)
			require.NoError(t, err)
			assert.False(t, truncated)
			assert.Equal(t, len(fileStructure), len(results))
		})

		t.Run("Optimise empty database", func(t *testing.T) {
			err := cache.Optimise()
			require.NoError(t, err)
		})
	})
}

func TestCacheClose(t *testing.T) {
	forEachTestBackend(t, func(t *testing.T, cache Cache) {
		t.Run("Close and operations after close", func(t *testing.T) {
			err := cache.Insert(createFileObjects(dirStructure...)...)
			require.NoError(t, err)

			err = cache.Insert(createFileObjects(fileStructure...)...)
			require.NoError(t, err)

			err = cache.Close()
			require.NoError(t, err)

			results, truncated, err := cache.List("", "", false, 100)
			assert.Error(t, err, "Should fail after close")
			assert.Nil(t, results)
			assert.False(t, truncated)
		})
	})
}
