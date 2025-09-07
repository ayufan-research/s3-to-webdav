package sync

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"s3-to-webdav/internal/cache"
	"s3-to-webdav/internal/fs"
	"s3-to-webdav/internal/tests"
)

func setupSyncTest(t *testing.T) (*Sync, cache.Cache, *tests.FakeWebDAVServer, func()) {
	webdavServer := tests.NewFakeWebDAVServer()

	log.SetOutput(io.Discard)

	webdavFs, err := webdavServer.CreateWebDAVFs()
	require.NoError(t, err)

	db, err := cache.NewCacheDB(":memory:")
	require.NoError(t, err)

	sync := New(webdavFs, db)

	cleanup := func() {
		webdavServer.Close()
		db.Close()
		log.SetOutput(os.Stderr)
	}

	return sync, db, webdavServer, cleanup
}

func TestSyncEmptyBucket(t *testing.T) {
	sync, db, _, cleanup := setupSyncTest(t)
	defer cleanup()

	err := sync.Sync("empty-bucket")
	require.NoError(t, err)

	entry, err := db.Stat("empty-bucket/")
	require.NoError(t, err)
	assert.True(t, entry.IsDir)
}

func TestSyncWithFiles(t *testing.T) {
	tests := []struct {
		name          string
		files         map[string][]byte
		expectedFiles int
		expectedDirs  int
	}{
		{
			name: "single file",
			files: map[string][]byte{
				"/test-bucket/file1.txt": []byte("content1"),
			},
			expectedFiles: 1,
			expectedDirs:  1,
		},
		{
			name: "multiple files in root",
			files: map[string][]byte{
				"/test-bucket/file1.txt": []byte("content1"),
				"/test-bucket/file2.txt": []byte("content2"),
				"/test-bucket/file3.txt": []byte("content3"),
			},
			expectedFiles: 3,
			expectedDirs:  1,
		},
		{
			name: "nested directory structure",
			files: map[string][]byte{
				"/test-bucket/file1.txt":             []byte("content1"),
				"/test-bucket/dir1/file2.txt":        []byte("content2"),
				"/test-bucket/dir1/subdir/file3.txt": []byte("content3"),
				"/test-bucket/dir2/file4.txt":        []byte("content4"),
			},
			expectedFiles: 4,
			expectedDirs:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sync, db, webdav, cleanup := setupSyncTest(t)
			defer cleanup()

			for path, content := range tt.files {
				webdav.AddFile(path, content)
			}

			err := sync.Sync("test-bucket")
			require.NoError(t, err)

			processedCount, unprocessedCount, totalSize, err := db.GetStats("test-bucket/")
			require.NoError(t, err)

			assert.Equal(t, 0, unprocessedCount, "Should have no unprocessed entries after sync")
			assert.Equal(t, tt.expectedFiles+tt.expectedDirs, processedCount, "Should have correct number of processed entries")
			assert.Greater(t, totalSize, int64(0), "Total size should be greater than 0")

			for path, expectedContent := range tt.files {
				dbPath := strings.TrimPrefix(path, "/")
				entry, err := db.Stat(dbPath)
				require.NoError(t, err, "File %s should exist in cache (looking for %s)", path, dbPath)
				assert.False(t, entry.IsDir, "File %s should not be a directory", path)
				assert.Equal(t, int64(len(expectedContent)), entry.Size, "File %s should have correct size", path)
			}
		})
	}
}

func TestSyncAlreadyProcessed(t *testing.T) {
	sync, db, webdav, cleanup := setupSyncTest(t)
	defer cleanup()

	webdav.AddFile("/test-bucket/file1.txt", []byte("content1"))

	err := sync.Sync("test-bucket")
	require.NoError(t, err)

	processedBefore, unprocessedBefore, _, err := db.GetStats("test-bucket/")
	require.NoError(t, err)
	assert.Equal(t, 0, unprocessedBefore)
	assert.Equal(t, 2, processedBefore)

	err = sync.Sync("test-bucket")
	require.NoError(t, err)

	processedAfter, unprocessedAfter, _, err := db.GetStats("test-bucket/")
	require.NoError(t, err)
	assert.Equal(t, 0, unprocessedAfter)
	assert.Equal(t, processedBefore, processedAfter)
}

func TestSyncNewFilesAdded(t *testing.T) {
	sync, db, webdav, cleanup := setupSyncTest(t)
	defer cleanup()

	webdav.AddFile("/test-bucket/file1.txt", []byte("content1"))

	err := sync.Sync("test-bucket")
	require.NoError(t, err)

	processedBefore, _, _, err := db.GetStats("test-bucket/")
	require.NoError(t, err)

	webdav.AddFile("/test-bucket/file2.txt", []byte("content2"))

	_, err = db.SetProcessed("test-bucket/", true, false)
	require.NoError(t, err)

	err = sync.Sync("test-bucket")
	require.NoError(t, err)

	processedAfter, unprocessedAfter, _, err := db.GetStats("test-bucket/")
	require.NoError(t, err)
	assert.Equal(t, 0, unprocessedAfter)
	assert.Greater(t, processedAfter, processedBefore)

	entry, err := db.Stat("test-bucket/file2.txt")
	require.NoError(t, err)
	assert.False(t, entry.IsDir)
}

func TestCleanEmptyDirectories(t *testing.T) {
	tests := []struct {
		name            string
		setupDirs       []string
		webdavFiles     map[string][]byte
		expectedCleaned int
	}{
		{
			name:            "no empty directories",
			setupDirs:       []string{},
			webdavFiles:     map[string][]byte{},
			expectedCleaned: 0,
		},
		{
			name:            "clean truly empty directory",
			setupDirs:       []string{"test-bucket/empty-dir/"},
			webdavFiles:     map[string][]byte{},
			expectedCleaned: 1,
		},
		{
			name:      "keep directory with files",
			setupDirs: []string{"test-bucket/dir-with-files/"},
			webdavFiles: map[string][]byte{
				"/test-bucket/dir-with-files/file.txt": []byte("content"),
			},
			expectedCleaned: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sync, db, webdav, cleanup := setupSyncTest(t)
			defer cleanup()

			for _, dir := range tt.setupDirs {
				err := db.Insert(fs.EntryInfo{
					Path:         dir,
					Size:         0,
					LastModified: time.Now().Unix(),
					IsDir:        true,
					Processed:    true,
				})
				require.NoError(t, err)
			}

			for path, content := range tt.webdavFiles {
				webdav.AddFile(path, content)
			}

			err := sync.Clean("test-bucket")
			require.NoError(t, err)
		})
	}
}

func TestCleanMissingDirectories(t *testing.T) {
	sync, db, _, cleanup := setupSyncTest(t)
	defer cleanup()

	err := db.Insert(fs.EntryInfo{
		Path:         "test-bucket/missing-dir/",
		Size:         0,
		LastModified: time.Now().Unix(),
		IsDir:        true,
		Processed:    true,
	})
	require.NoError(t, err)

	_, err = db.Stat("test-bucket/missing-dir/")
	require.NoError(t, err, "Directory should exist in cache before cleaning")

	err = sync.Clean("test-bucket")
	require.NoError(t, err)

	_, err = db.Stat("test-bucket/missing-dir/")
	assert.Error(t, err, "Directory should be removed from cache after cleaning")
}

func TestWalkDir(t *testing.T) {
	tests := []struct {
		name        string
		setupFiles  map[string][]byte
		walkPath    string
		expectError bool
	}{
		{
			name: "walk directory with files",
			setupFiles: map[string][]byte{
				"/test-bucket/file1.txt": []byte("content1"),
				"/test-bucket/file2.txt": []byte("content2"),
			},
			walkPath:    "test-bucket/",
			expectError: false,
		},
		{
			name: "walk nested directory",
			setupFiles: map[string][]byte{
				"/test-bucket/subdir/file1.txt": []byte("content1"),
				"/test-bucket/subdir/file2.txt": []byte("content2"),
			},
			walkPath:    "test-bucket/subdir/",
			expectError: false,
		},
		{
			name:        "walk non-existent directory",
			setupFiles:  map[string][]byte{},
			walkPath:    "test-bucket/missing-dir/",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sync, db, webdav, cleanup := setupSyncTest(t)
			defer cleanup()

			for path, content := range tt.setupFiles {
				webdav.AddFile(path, content)
			}

			err := db.Insert(fs.EntryInfo{
				Path:         tt.walkPath,
				Size:         0,
				LastModified: time.Now().Unix(),
				IsDir:        true,
				Processed:    false,
			})
			require.NoError(t, err)

			err = sync.walkDir(tt.walkPath)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if !tt.expectError {
				entry, err := db.Stat(tt.walkPath)
				require.NoError(t, err)
				assert.True(t, entry.Processed, "Directory should be marked as processed")
			}
		})
	}
}

func TestSyncConcurrency(t *testing.T) {
	sync, db, webdav, cleanup := setupSyncTest(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			path := fmt.Sprintf("/test-bucket/dir%d/file%d.txt", i, j)
			content := fmt.Sprintf("content-%d-%d", i, j)
			webdav.AddFile(path, []byte(content))
		}
	}

	err := sync.Sync("test-bucket")
	require.NoError(t, err)

	processedCount, unprocessedCount, _, err := db.GetStats("test-bucket/")
	require.NoError(t, err)
	assert.Equal(t, 0, unprocessedCount)
	assert.Greater(t, processedCount, 100)
}

func TestPrintStats(t *testing.T) {
	sync, db, _, cleanup := setupSyncTest(t)
	defer cleanup()

	err := db.Insert(fs.EntryInfo{
		Path:         "test-bucket/",
		Size:         0,
		LastModified: time.Now().Unix(),
		IsDir:        true,
		Processed:    true,
	})
	require.NoError(t, err)

	sync.printStats("test-bucket/")

	assert.True(t, sync.lastStatus.After(time.Time{}))
}
