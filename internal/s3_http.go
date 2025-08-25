package internal

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

type S3Server struct {
	db        *DBCache
	client    *gowebdav.Client
	accessKey string
	secretKey string
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

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
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


func (s *S3Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	AddLogContext(r, "list-buckets")
	buckets, err := s.db.ListBuckets()
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

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
	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")

	AddLogContext(r, fmt.Sprintf("list-objects:%s", bucket))

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
		objects[i] = Object{
			Key:          file.Key,
			LastModified: time.Unix(file.LastModified, 0).Format(time.RFC3339),
			Size:         file.Size,
			StorageClass: "STANDARD",
		}
	}

	// Set NextMarker if results are truncated
	var nextMarker string
	if truncated && len(objects) > 0 {
		// NextMarker should be the key of the last object returned
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

	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (s *S3Server) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	AddLogContext(r, fmt.Sprintf("head:%s/%s", bucket, key))

	entryInfo, exists := s.db.ObjectExists(bucket, key)
	if !exists || entryInfo.IsDir {
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", entryInfo.Size))
	w.Header().Set("Last-Modified", time.Unix(entryInfo.LastModified, 0).Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]

	AddLogContext(r, fmt.Sprintf("get:%s/%s", bucket, key))

	entryInfo, exists := s.db.ObjectExists(bucket, key)
	if !exists || entryInfo.IsDir {
		http.Error(w, "Object not found", http.StatusNotFound)
		AddLogContext(r, "local-fail")
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", entryInfo.Size))
	w.Header().Set("Last-Modified", time.Unix(entryInfo.LastModified, 0).Format(http.TimeFormat))

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

	s.db.InsertObject(EntryInfo{
		Path:         path,
		Bucket:       bucket,
		Key:          key,
		Size:         stat.Size(),
		LastModified: stat.ModTime().Unix(),
		IsDir:        stat.IsDir(),
		ProcessedAt:  time.Now().Unix(),
	})

	w.WriteHeader(http.StatusOK)
}

func (s *S3Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	path := PathFromBucketAndKey(bucket, key)

	AddLogContext(r, fmt.Sprintf("delete:%s/%s", bucket, key))

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
	r.HandleFunc("/{bucket}/{key:.*}", s.handleGetObject).Methods("GET")
	r.HandleFunc("/{bucket}/{key:.*}", s.handlePutObject).Methods("PUT")
	r.HandleFunc("/{bucket}/{key:.*}", s.handleHeadObject).Methods("HEAD")
	r.HandleFunc("/{bucket}/{key:.*}", s.handleDeleteObject).Methods("DELETE")
}
