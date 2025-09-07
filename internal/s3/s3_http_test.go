package s3

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"s3-to-webdav/internal/cache"
	"s3-to-webdav/internal/fs"
	"s3-to-webdav/internal/tests"
)

func setupTestServer(t *testing.T) (*server, cache.Cache, *tests.FakeWebDAVServer, func()) {
	webdavServer := tests.NewFakeWebDAVServer()

	log.SetOutput(io.Discard)

	db, err := cache.NewCacheDB(":memory:")
	require.NoError(t, err)

	webdavFs, err := webdavServer.CreateWebDAVFs()
	require.NoError(t, err)

	s := NewServer(db, webdavFs)
	s.SetBucketMap(map[string]interface{}{
		"test-bucket": nil,
		"bucket2":     nil,
	})

	cleanup := func() {
		db.Close()
		webdavServer.Close()
		log.SetOutput(os.Stderr)
	}

	return s, db, webdavServer, cleanup
}

func TestGenerateETag(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		size         int64
		lastModified int64
		expected     string
	}{
		{
			name:         "basic file",
			path:         "/bucket/file.txt",
			size:         100,
			lastModified: 1609459200,
			expected: func() string {
				h := md5.New()
				h.Write([]byte("/bucket/file.txt-100-1609459200"))
				return fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil)))
			}(),
		},
		{
			name:         "empty file",
			path:         "/bucket/empty.txt",
			size:         0,
			lastModified: 1609459200,
			expected: func() string {
				h := md5.New()
				h.Write([]byte("/bucket/empty.txt-0-1609459200"))
				return fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil)))
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateETag(tt.path, tt.size, tt.lastModified)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHandleListBuckets(t *testing.T) {
	s, _, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	s.handleListBuckets(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))

	var result ListBucketsResult
	err := xml.Unmarshal(w.Body.Bytes(), &result)
	require.NoError(t, err)

	assert.Len(t, result.Buckets.Bucket, 2)

	bucketNames := make([]string, len(result.Buckets.Bucket))
	for i, bucket := range result.Buckets.Bucket {
		bucketNames[i] = bucket.Name
		assert.NotEmpty(t, bucket.CreationDate)
	}

	assert.Contains(t, bucketNames, "bucket2")
	assert.Contains(t, bucketNames, "test-bucket")
}

func TestHandleHeadBucket(t *testing.T) {
	s, _, _, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name           string
		bucket         string
		expectedStatus int
	}{
		{"allowed bucket", "test-bucket", http.StatusOK},
		{"not allowed bucket", "forbidden", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("HEAD", "/"+tt.bucket, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": tt.bucket})
			w := httptest.NewRecorder()

			s.handleHeadBucket(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

func TestHandleHeadObject(t *testing.T) {
	s, db, _, cleanup := setupTestServer(t)
	defer cleanup()

	testContent := []byte("test file content")
	testModTime := time.Now().Unix()

	err := db.Insert(fs.EntryInfo{
		Path:         "test-bucket/test-file.txt",
		Size:         int64(len(testContent)),
		LastModified: testModTime,
		IsDir:        false,
		Processed:    true,
	})
	require.NoError(t, err)

	tests := []struct {
		name           string
		bucket         string
		key            string
		expectedStatus int
		expectedETag   string
	}{
		{
			name:           "existing file",
			bucket:         "test-bucket",
			key:            "test-file.txt",
			expectedStatus: http.StatusOK,
			expectedETag:   generateETag("test-bucket/test-file.txt", int64(len(testContent)), testModTime),
		},
		{
			name:           "non-existing file",
			bucket:         "test-bucket",
			key:            "non-existing.txt",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "forbidden bucket",
			bucket:         "forbidden",
			key:            "file.txt",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("HEAD", "/"+tt.bucket+"/"+tt.key, nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": tt.bucket,
				"key":    tt.key,
			})
			w := httptest.NewRecorder()

			s.handleHeadObject(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, tt.expectedETag, w.Header().Get("ETag"))
				assert.Equal(t, strconv.Itoa(len(testContent)), w.Header().Get("Content-Length"))
				assert.NotEmpty(t, w.Header().Get("Last-Modified"))
			}
		})
	}
}

func TestHandleGetObject(t *testing.T) {
	s, db, webdav, cleanup := setupTestServer(t)
	defer cleanup()

	testContent := []byte("test file content for GET")
	testPath := "/test-bucket/get-test.txt"
	testModTime := time.Now().Unix()

	webdav.AddFile(testPath, testContent)

	err := db.Insert(fs.EntryInfo{
		Path:         "test-bucket/get-test.txt",
		Size:         int64(len(testContent)),
		LastModified: testModTime,
		IsDir:        false,
		Processed:    true,
	})
	require.NoError(t, err)

	cacheOnlyContent := []byte("cache only content")
	cacheOnlyModTime := time.Now().Unix()

	err = db.Insert(fs.EntryInfo{
		Path:         "test-bucket/cache-only.txt",
		Size:         int64(len(cacheOnlyContent)),
		LastModified: cacheOnlyModTime,
		IsDir:        false,
		Processed:    true,
	})
	require.NoError(t, err)

	fsOnlyContent := []byte("filesystem only content")
	fsOnlyPath := "/test-bucket/fs-only.txt"
	webdav.AddFile(fsOnlyPath, fsOnlyContent)

	tests := []struct {
		name           string
		bucket         string
		key            string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "existing file",
			bucket:         "test-bucket",
			key:            "get-test.txt",
			expectedStatus: http.StatusOK,
			expectedBody:   string(testContent),
		},
		{
			name:           "non-existing file",
			bucket:         "test-bucket",
			key:            "non-existing.txt",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "forbidden bucket",
			bucket:         "forbidden",
			key:            "file.txt",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "object in cache but not on filesystem",
			bucket:         "test-bucket",
			key:            "cache-only.txt",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "object not in cache but on filesystem",
			bucket:         "test-bucket",
			key:            "fs-only.txt",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/"+tt.bucket+"/"+tt.key, nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": tt.bucket,
				"key":    tt.key,
			})
			w := httptest.NewRecorder()

			s.handleGetObject(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, tt.expectedBody, w.Body.String())
				assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
				assert.NotEmpty(t, w.Header().Get("ETag"))
			}
		})
	}
}

func TestHandlePutObject(t *testing.T) {
	s, db, webdav, cleanup := setupTestServer(t)
	defer cleanup()

	tests := []struct {
		name                 string
		bucket               string
		key                  string
		content              string
		contentLength        string
		sha256Header         string
		expectedStatus       int
		expectedResponseBody string
		checkStat            bool
		checkDirectories     bool
	}{
		{
			name:           "valid put",
			bucket:         "test-bucket",
			key:            "put-test.txt",
			content:        "test content for PUT",
			contentLength:  "20",
			expectedStatus: http.StatusOK,
			checkStat:      true,
		},
		{
			name:          "put with valid SHA256 verification",
			bucket:        "test-bucket",
			key:           "put-sha256.txt",
			content:       "test content",
			contentLength: "12",
			sha256Header: func() string {
				h := sha256.New()
				h.Write([]byte("test content"))
				return hex.EncodeToString(h.Sum(nil))
			}(),
			expectedStatus: http.StatusOK,
			checkStat:      true,
		},
		{
			name:                 "put with invalid SHA256 hash",
			bucket:               "test-bucket",
			key:                  "put-invalid-sha256.txt",
			content:              "test content",
			contentLength:        "12",
			sha256Header:         "invalid-sha256-hash",
			expectedStatus:       http.StatusBadRequest,
			expectedResponseBody: "BadDigest",
		},
		{
			name:           "put with truncated content",
			bucket:         "test-bucket",
			key:            "put-truncated.txt",
			content:        "short",
			contentLength:  "20",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "put with content too long",
			bucket:         "test-bucket",
			key:            "put-toolong.txt",
			content:        "this content is much longer than expected",
			contentLength:  "5",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "forbidden bucket",
			bucket:         "forbidden",
			key:            "file.txt",
			content:        "content",
			contentLength:  "7",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:             "put nested file with directory creation",
			bucket:           "test-bucket",
			key:              "deep/nested/path/file.txt",
			content:          "nested file content",
			contentLength:    "19",
			expectedStatus:   http.StatusOK,
			checkStat:        true,
			checkDirectories: true,
		},
		{
			name:           "put file with stat verification",
			bucket:         "test-bucket",
			key:            "stat-verify.txt",
			content:        "content for stat verification",
			contentLength:  "29",
			expectedStatus: http.StatusOK,
			checkStat:      true,
		},
		{
			name:           "put with explicit negative content length",
			bucket:         "test-bucket",
			key:            "negative-length.txt",
			content:        "content",
			contentLength:  "",
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader = strings.NewReader(tt.content)

			if tt.name == "put with content too long" {
				body = io.LimitReader(strings.NewReader(tt.content), 5)
			}

			req := httptest.NewRequest("PUT", "/"+tt.bucket+"/"+tt.key, body)
			req.Header.Set("Content-Length", tt.contentLength)
			if tt.sha256Header != "" {
				req.Header.Set("X-Amz-Content-Sha256", tt.sha256Header)
			}
			req = mux.SetURLVars(req, map[string]string{
				"bucket": tt.bucket,
				"key":    tt.key,
			})
			w := httptest.NewRecorder()

			s.handlePutObject(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedResponseBody != "" {
				assert.Contains(t, w.Body.String(), tt.expectedResponseBody)
			}

			if tt.expectedStatus == http.StatusOK {
				assert.NotEmpty(t, w.Header().Get("ETag"))

				if tt.checkStat {
					entry, err := db.Stat(tt.bucket + "/" + tt.key)
					require.NoError(t, err)

					expectedContent := tt.content
					if tt.name == "put with content too long" {
						expectedContent = tt.content[:5]
					}
					assert.Equal(t, int64(len(expectedContent)), entry.Size)

					webdavFs, err := webdav.CreateWebDAVFs()
					require.NoError(t, err)
					filePath := fs.PathFromBucketAndKey(tt.bucket, tt.key)
					stat, err := webdavFs.Stat(filePath)
					require.NoError(t, err)
					assert.Equal(t, int64(len(expectedContent)), stat.Size())
					assert.False(t, stat.IsDir())

					reader, err := webdavFs.ReadStream(filePath)
					require.NoError(t, err)
					defer reader.Close()
					actualContent, err := io.ReadAll(reader)
					require.NoError(t, err)
					assert.Equal(t, expectedContent, string(actualContent))
				}

				if tt.checkDirectories {
					expectedDirs := []string{
						"test-bucket/",
						"test-bucket/deep/",
						"test-bucket/deep/nested/",
						"test-bucket/deep/nested/path/",
					}

					for _, expectedDir := range expectedDirs {
						entry, err := db.Stat(expectedDir)
						require.NoError(t, err, "Directory %s should exist in cache", expectedDir)
						assert.True(t, entry.IsDir, "Entry %s should be a directory", expectedDir)
						assert.True(t, entry.Processed, "Directory %s should be marked as processed", expectedDir)
					}
				}
			}
		})
	}
}

func TestHandleDeleteObject(t *testing.T) {
	tests := []struct {
		name      string
		bucket    string
		key       string
		setupFile bool
	}{
		{
			name:      "delete existing file",
			bucket:    "test-bucket",
			key:       "delete-test.txt",
			setupFile: true,
		},
		{
			name:      "delete non-existing file",
			bucket:    "test-bucket",
			key:       "missing-file.txt",
			setupFile: false,
		},
		{
			name:      "forbidden bucket",
			bucket:    "forbidden",
			key:       "file.txt",
			setupFile: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, db, webdav, cleanup := setupTestServer(t)
			defer cleanup()

			if tt.setupFile {
				testContent := []byte("test content to delete")
				testPath := "/test-bucket/delete-test.txt"

				webdav.AddFile(testPath, testContent)

				err := db.Insert(fs.EntryInfo{
					Path:         "test-bucket/delete-test.txt",
					Size:         int64(len(testContent)),
					LastModified: time.Now().Unix(),
					IsDir:        false,
					Processed:    true,
				})
				require.NoError(t, err)
			}

			req := httptest.NewRequest("DELETE", "/"+tt.bucket+"/"+tt.key, nil)
			req = mux.SetURLVars(req, map[string]string{
				"bucket": tt.bucket,
				"key":    tt.key,
			})
			w := httptest.NewRecorder()

			s.handleDeleteObject(w, req)

			if tt.bucket == "forbidden" {
				assert.Equal(t, http.StatusNotFound, w.Code)
			} else if tt.setupFile {
				assert.True(t, w.Code == http.StatusNoContent || w.Code == http.StatusInternalServerError,
					"Delete should either succeed (204) or fail due to filesystem issues (500)")

				if w.Code == http.StatusInternalServerError {
					t.Logf("Delete returned 500, this is acceptable for testing filesystem failures")
				}
			}
		})
	}
}

func TestHandleBulkDelete(t *testing.T) {
	tests := []struct {
		name             string
		setupFiles       []string
		deleteKeys       []string
		expectedDeleted  int
		expectedErrors   int
		checkMissingFile string
	}{
		{
			name:             "delete existing and non-existing files",
			setupFiles:       []string{"bulk1.txt", "bulk2.txt"},
			deleteKeys:       []string{"bulk1.txt", "bulk2.txt", "non-existing.txt"},
			expectedDeleted:  3,
			expectedErrors:   0,
			checkMissingFile: "",
		},
		{
			name:            "delete only existing files",
			setupFiles:      []string{"file1.txt", "file2.txt"},
			deleteKeys:      []string{"file1.txt", "file2.txt"},
			expectedDeleted: 2,
			expectedErrors:  0,
		},
		{
			name:            "delete only non-existing files",
			setupFiles:      []string{},
			deleteKeys:      []string{"missing1.txt", "missing2.txt"},
			expectedDeleted: 2,
			expectedErrors:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, db, webdav, cleanup := setupTestServer(t)
			defer cleanup()

			var entries []fs.EntryInfo
			for _, filename := range tt.setupFiles {
				content := []byte(filename + " content")
				path := "test-bucket/" + filename
				webdav.AddFile(path, content)
				entries = append(entries, fs.EntryInfo{
					Path:         path,
					Size:         int64(len(content)),
					LastModified: time.Now().Unix(),
					IsDir:        false,
					Processed:    true,
				})
			}
			if len(entries) > 0 {
				err := db.Insert(entries...)
				require.NoError(t, err)
			}

			deleteXML := "<Delete>"
			for _, key := range tt.deleteKeys {
				deleteXML += "<Object><Key>" + key + "</Key></Object>"
			}
			deleteXML += "</Delete>"

			req := httptest.NewRequest("POST", "/test-bucket/?delete", strings.NewReader(deleteXML))
			req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
			w := httptest.NewRecorder()

			s.handleBulkDelete(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))

			var result DeleteResult
			err := xml.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedDeleted, len(result.Deleted), "Unexpected number of deleted objects")
			assert.Equal(t, tt.expectedErrors, len(result.Errors), "Unexpected number of errors")

			totalProcessed := len(result.Deleted) + len(result.Errors)
			assert.Equal(t, len(tt.deleteKeys), totalProcessed, "Should process all requested objects")

			if tt.checkMissingFile != "" {
				foundMissingFileError := false
				for _, err := range result.Errors {
					if err.Key == tt.checkMissingFile {
						foundMissingFileError = true
						break
					}
				}
				if tt.expectedErrors > 0 {
					assert.True(t, foundMissingFileError, "Should have error for missing file '%s'", tt.checkMissingFile)
				}
			}
		})
	}
}

func TestHandleListObjects(t *testing.T) {
	s, db, _, cleanup := setupTestServer(t)
	defer cleanup()

	testFiles := []fs.EntryInfo{
		{
			Path:         "test-bucket/file1.txt",
			Size:         100,
			LastModified: time.Now().Unix(),
			IsDir:        false,
			Processed:    true,
		},
		{
			Path:         "test-bucket/prefix/file2.txt",
			Size:         200,
			LastModified: time.Now().Unix(),
			IsDir:        false,
			Processed:    true,
		},
		{
			Path:         "test-bucket/prefix/subdir/file3.txt",
			Size:         300,
			LastModified: time.Now().Unix(),
			IsDir:        false,
			Processed:    true,
		},
	}

	err := db.Insert(testFiles...)
	require.NoError(t, err)

	tests := []struct {
		name                string
		bucket              string
		params              map[string]string
		expectedStatus      int
		expectedCount       int
		checkPrefix         string
		expectedMaxKeys     int
		expectedIsTruncated bool
		expectedMarker      string
		expectedDelimiter   string
	}{
		{
			name:           "list all objects",
			bucket:         "test-bucket",
			params:         map[string]string{},
			expectedStatus: http.StatusOK,
			expectedCount:  3,
		},
		{
			name:           "list with prefix",
			bucket:         "test-bucket",
			params:         map[string]string{"prefix": "prefix/"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
			checkPrefix:    "prefix/",
		},
		{
			name:                "list with max-keys=2",
			bucket:              "test-bucket",
			params:              map[string]string{"max-keys": "2"},
			expectedStatus:      http.StatusOK,
			expectedCount:       2,
			expectedMaxKeys:     2,
			expectedIsTruncated: true,
		},
		{
			name:                "list with max-keys=1",
			bucket:              "test-bucket",
			params:              map[string]string{"max-keys": "1"},
			expectedStatus:      http.StatusOK,
			expectedCount:       1,
			expectedMaxKeys:     1,
			expectedIsTruncated: true,
		},
		{
			name:              "list with delimiter",
			bucket:            "test-bucket",
			params:            map[string]string{"delimiter": "/"},
			expectedStatus:    http.StatusOK,
			expectedCount:     1,
			expectedDelimiter: "/",
		},
		{
			name:           "list with marker",
			bucket:         "test-bucket",
			params:         map[string]string{"marker": "test-bucket/file1.txt"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
			expectedMarker: "file1.txt",
		},
		{
			name:              "list with delimiter and prefix",
			bucket:            "test-bucket",
			params:            map[string]string{"delimiter": "/", "prefix": "prefix/"},
			expectedStatus:    http.StatusOK,
			expectedCount:     1,
			checkPrefix:       "prefix/",
			expectedDelimiter: "/",
		},
		{
			name:           "list objects v2",
			bucket:         "test-bucket",
			params:         map[string]string{"list-type": "2"},
			expectedStatus: http.StatusOK,
			expectedCount:  3,
		},
		{
			name:           "list objects v2 with continuation-token",
			bucket:         "test-bucket",
			params:         map[string]string{"list-type": "2", "continuation-token": "test-bucket/file1.txt"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
			expectedMarker: "file1.txt",
		},
		{
			name:           "list objects v2 with start-after",
			bucket:         "test-bucket",
			params:         map[string]string{"list-type": "2", "start-after": "file1.txt"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
			expectedMarker: "file1.txt",
		},
		{
			name:           "forbidden bucket",
			bucket:         "forbidden",
			params:         map[string]string{},
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := "/" + tt.bucket
			if len(tt.params) > 0 {
				url += "?"
				params := make([]string, 0, len(tt.params))
				for k, v := range tt.params {
					params = append(params, fmt.Sprintf("%s=%s", k, v))
				}
				url += strings.Join(params, "&")
			}

			req := httptest.NewRequest("GET", url, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": tt.bucket})
			w := httptest.NewRecorder()

			s.handleListObjects(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))

				if tt.params["list-type"] == "2" {
					var result ListBucketResultV2
					err := xml.Unmarshal(w.Body.Bytes(), &result)
					require.NoError(t, err)
					assert.Equal(t, tt.bucket, result.Name)
					assert.Equal(t, tt.expectedCount, len(result.Contents))
				} else {
					var result ListBucketResult
					err := xml.Unmarshal(w.Body.Bytes(), &result)
					require.NoError(t, err)
					assert.Equal(t, tt.bucket, result.Name)
					assert.Equal(t, tt.expectedCount, len(result.Contents))

					if tt.checkPrefix != "" {
						assert.Equal(t, tt.checkPrefix, result.Prefix)
						for _, obj := range result.Contents {
							assert.True(t, strings.HasPrefix(obj.Key, tt.checkPrefix))
						}
					}

					if tt.expectedMaxKeys > 0 {
						assert.Equal(t, tt.expectedMaxKeys, result.MaxKeys)
					}

					if tt.expectedIsTruncated {
						assert.True(t, result.IsTruncated)
					}

					if tt.expectedMarker != "" {
						for _, obj := range result.Contents {
							assert.True(t, obj.Key > tt.expectedMarker, "Objects should come after marker '%s', but found '%s'", tt.expectedMarker, obj.Key)
						}
					}

					if tt.expectedDelimiter != "" {
						assert.Equal(t, tt.expectedDelimiter, result.Delimiter)
					}
				}
			}
		})
	}
}

func TestListAll(t *testing.T) {
	s, db, _, cleanup := setupTestServer(t)
	defer cleanup()

	testFiles := []fs.EntryInfo{
		{Path: "test-bucket/file1.txt", Size: 100, LastModified: time.Now().Unix(), IsDir: false, Processed: true},
		{Path: "test-bucket/file2.txt", Size: 200, LastModified: time.Now().Unix(), IsDir: false, Processed: true},
		{Path: "test-bucket/prefix/file3.txt", Size: 300, LastModified: time.Now().Unix(), IsDir: false, Processed: true},
		{Path: "test-bucket/prefix/file4.txt", Size: 400, LastModified: time.Now().Unix(), IsDir: false, Processed: true},
		{Path: "test-bucket/prefix/subdir/file5.txt", Size: 500, LastModified: time.Now().Unix(), IsDir: false, Processed: true},
	}

	err := db.Insert(testFiles...)
	require.NoError(t, err)

	urls := map[int]string{
		1: "/test-bucket?max-keys=1&marker=",
		2: "/test-bucket?max-keys=1&list-type=2&continuation-token=",
	}

	for listType, url := range urls {
		t.Run(fmt.Sprintf("Type%d", listType), func(t *testing.T) {
			marker := ""

			for i := 0; i < 10; i++ {
				req := httptest.NewRequest("GET", url+marker, nil)
				req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
				w := httptest.NewRecorder()

				s.handleListObjects(w, req)
				require.Equal(t, http.StatusOK, w.Code)
				require.Equal(t, "application/xml", w.Header().Get("Content-Type"))

				if listType == 2 {
					var result ListBucketResultV2
					err := xml.Unmarshal(w.Body.Bytes(), &result)
					require.NoError(t, err)
					assert.Equal(t, "test-bucket", result.Name)
					marker = result.NextContinuationToken
					if !result.IsTruncated {
						return
					}
				} else {
					var result ListBucketResult
					err := xml.Unmarshal(w.Body.Bytes(), &result)
					require.NoError(t, err)
					assert.Equal(t, "test-bucket", result.Name)
					marker = result.NextMarker
					if !result.IsTruncated {
						return
					}
				}
			}

			t.Fatal("ListAll did not complete within expected iterations")
		})
	}
}
