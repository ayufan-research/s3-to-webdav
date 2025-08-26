package internal

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sort"
	"time"

	"github.com/gorilla/mux"
	"github.com/studio-b12/gowebdav"
)

func parseInt(s string) int {
	if val, err := strconv.Atoi(s); err == nil {
		return val
	}
	return 0
}

// generateETag generates an ETag from file metadata
func generateETag(path string, size int64, lastModified int64) string {
	h := md5.New()
	h.Write([]byte(fmt.Sprintf("%s-%d-%d", path, size, lastModified)))
	return fmt.Sprintf("\"%s\"", hex.EncodeToString(h.Sum(nil)))
}

type S3Server struct {
	db        *DBCache
	client    *gowebdav.Client
	accessKey string
	secretKey string
	bucketMap map[string]interface{}
}

type ListBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets Buckets  `xml:"Buckets"`
}

type Buckets struct {
	Bucket []Bucket `xml:"Bucket"`
}

type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListBucketResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextMarker  string   `xml:"NextMarker,omitempty"`
	Contents    []Object `xml:"Contents"`
}

type ListBucketResultV2 struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	MaxKeys               int      `xml:"MaxKeys"`
	IsTruncated           bool     `xml:"IsTruncated"`
	KeyCount              int      `xml:"KeyCount"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	StartAfter            string   `xml:"StartAfter,omitempty"`
	Contents              []Object `xml:"Contents"`
}

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func NewS3Server(db *DBCache, client *gowebdav.Client, accessKey, secretKey string) *S3Server {
	return &S3Server{
		db:        db,
		client:    client,
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

// SetBucketMap sets the map of buckets to expose via S3 API
func (s *S3Server) SetBucketMap(buckets map[string]interface{}) {
	s.bucketMap = buckets
}

// isBucketAllowed checks if a bucket is allowed based on the bucket map
func (s *S3Server) isBucketAllowed(bucket string) bool {
	// Check if bucket is in the allowed map (O(1) lookup)
	_, exists := s.bucketMap[bucket]
	return exists
}

func (s *S3Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	AddLogContext(r, "list-buckets")

	// Use specified bucket map (buckets are required)
	buckets := make([]string, 0, len(s.bucketMap))
	for bucket := range s.bucketMap {
		buckets = append(buckets, bucket)
	}

	sort.Strings(buckets)

	result := ListBucketsResult{
		Buckets: Buckets{
			Bucket: make([]Bucket, len(buckets)),
		},
	}

	for i, bucket := range buckets {
		result.Buckets.Bucket[i] = Bucket{
			Name:         bucket,
			CreationDate: time.Now().Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (s *S3Server) handleListObjects(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Validate bucket is allowed
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Check if this is ListObjectsV2 request
	isV2 := r.URL.Query().Get("list-type") == "2"

	var prefix, marker string
	if isV2 {
		// ListObjectsV2 parameters
		prefix = r.URL.Query().Get("prefix")
		marker = r.URL.Query().Get("continuation-token")
		if marker == "" {
			marker = r.URL.Query().Get("start-after")
		}
		AddLogContext(r, fmt.Sprintf("list-objects-v2:%s", bucket))
	} else {
		// ListObjects (V1) parameters
		prefix = r.URL.Query().Get("prefix")
		marker = r.URL.Query().Get("marker")
		AddLogContext(r, fmt.Sprintf("list-objects:%s", bucket))
	}

	// Default limit to 1000, but allow customization via max-keys parameter
	limit := 1000
	if maxKeysStr := r.URL.Query().Get("max-keys"); maxKeysStr != "" {
		if maxKeysInt := parseInt(maxKeysStr); maxKeysInt > 0 && maxKeysInt <= 1000 {
			limit = maxKeysInt
		}
	}

	files, truncated, err := s.db.ListObjects(bucket, prefix, marker, limit)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	objects := make([]Object, len(files))
	for i, file := range files {
		etag := generateETag(file.Path, file.Size, file.LastModified)
		objects[i] = Object{
			Key:          file.Key,
			LastModified: time.Unix(file.LastModified, 0).Format(time.RFC3339),
			ETag:         etag,
			Size:         file.Size,
			StorageClass: "STANDARD",
		}
	}

	w.Header().Set("Content-Type", "application/xml")

	if isV2 {
		// ListObjectsV2 response
		var nextContinuationToken string
		if truncated && len(objects) > 0 {
			nextContinuationToken = objects[len(objects)-1].Key
		}

		resultV2 := ListBucketResultV2{
			Name:                  bucket,
			Prefix:                prefix,
			MaxKeys:               limit,
			IsTruncated:           truncated,
			KeyCount:              len(objects),
			ContinuationToken:     r.URL.Query().Get("continuation-token"),
			NextContinuationToken: nextContinuationToken,
			StartAfter:            r.URL.Query().Get("start-after"),
			Contents:              objects,
		}
		xml.NewEncoder(w).Encode(resultV2)
	} else {
		// ListObjects (V1) response
		var nextMarker string
		if truncated && len(objects) > 0 {
			nextMarker = objects[len(objects)-1].Key
		}

		result := ListBucketResult{
			Name:        bucket,
			Prefix:      prefix,
			MaxKeys:     limit,
			IsTruncated: truncated,
			NextMarker:  nextMarker,
			Contents:    objects,
		}
		xml.NewEncoder(w).Encode(result)
	}
}

func (s *S3Server) handleHeadBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	AddLogContext(r, fmt.Sprintf("head-bucket:%s", bucket))

	// Validate bucket is allowed (buckets are required)
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Return 200 OK with no body for HEAD bucket request
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	AddLogContext(r, fmt.Sprintf("head:%s/%s", bucket, key))

	// Validate bucket is allowed
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	entryInfo, exists := s.db.ObjectExists(bucket, key)
	if !exists || entryInfo.IsDir {
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}

	etag := generateETag(entryInfo.Path, entryInfo.Size, entryInfo.LastModified)

	// Check If-None-Match header for conditional requests
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if ifNoneMatch == "*" || ifNoneMatch == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", entryInfo.Size))
	w.Header().Set("Last-Modified", time.Unix(entryInfo.LastModified, 0).Format(http.TimeFormat))
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	AddLogContext(r, fmt.Sprintf("get:%s/%s", bucket, key))

	// Validate bucket is allowed
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	entryInfo, exists := s.db.ObjectExists(bucket, key)
	if !exists || entryInfo.IsDir {
		http.Error(w, "Object not found", http.StatusNotFound)
		AddLogContext(r, "local-fail")
		return
	}

	etag := generateETag(entryInfo.Path, entryInfo.Size, entryInfo.LastModified)

	// Check If-None-Match header for conditional requests
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if ifNoneMatch == "*" || ifNoneMatch == etag {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", entryInfo.Size))
	w.Header().Set("Last-Modified", time.Unix(entryInfo.LastModified, 0).Format(http.TimeFormat))
	w.Header().Set("ETag", etag)

	reader, err := s.client.ReadStream(entryInfo.Path)
	if err != nil {
		http.Error(w, "Object not found", http.StatusNotFound)
		AddLogContext(r, "remote-fail")
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	io.Copy(w, reader)
}

func (s *S3Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	path := PathFromBucketAndKey(bucket, key)

	AddLogContext(r, fmt.Sprintf("put:%s/%s", bucket, key))

	// Validate bucket is allowed
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	err := s.client.WriteStream(path, r.Body, 0644)
	if err != nil {
		http.Error(w, "Failed to upload object", http.StatusInternalServerError)
		AddLogContext(r, "remote-fail")
		return
	}

	// Get file info from WebDAV to update database
	stat, err := s.client.Stat(path)
	if err != nil {
		http.Error(w, "Failed to stat uploaded object", http.StatusInternalServerError)
		AddLogContext(r, "stat-fail")
		return
	}

	entryInfo := EntryInfo{
		Path:         path,
		Bucket:       bucket,
		Key:          key,
		Size:         stat.Size(),
		LastModified: stat.ModTime().Unix(),
		IsDir:        stat.IsDir(),
		ProcessedAt:  time.Now().Unix(),
	}

	s.db.InsertObject(entryInfo)

	etag := generateETag(entryInfo.Path, entryInfo.Size, entryInfo.LastModified)
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	path := PathFromBucketAndKey(bucket, key)

	AddLogContext(r, fmt.Sprintf("delete:%s/%s", bucket, key))

	// Validate bucket is allowed
	if !s.isBucketAllowed(bucket) {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Remove from database immediately
	s.db.DeleteObject(path)

	err := s.client.Remove(path)
	if err != nil {
		http.Error(w, "Failed to delete object", http.StatusInternalServerError)
		AddLogContext(r, "remote-fail")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// SetupS3Routes sets up all S3 API routes with the given router
func (s *S3Server) SetupS3Routes(r *mux.Router) {
	r.HandleFunc("/", s.handleListBuckets).Methods("GET")
	r.HandleFunc("/{bucket}", s.handleListObjects).Methods("GET")
	r.HandleFunc("/{bucket}/", s.handleListObjects).Methods("GET")
	r.HandleFunc("/{bucket}", s.handleHeadBucket).Methods("HEAD")
	r.HandleFunc("/{bucket}/", s.handleHeadBucket).Methods("HEAD")
	r.HandleFunc("/{bucket}/{key:.*}", s.handleGetObject).Methods("GET")
	r.HandleFunc("/{bucket}/{key:.*}", s.handlePutObject).Methods("PUT")
	r.HandleFunc("/{bucket}/{key:.*}", s.handleHeadObject).Methods("HEAD")
	r.HandleFunc("/{bucket}/{key:.*}", s.handleDeleteObject).Methods("DELETE")
}
