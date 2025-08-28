package internal

import (
	"s3-to-webdav/internal/fs"
)

type Cache interface {
	Close() error
	InsertObjects(objects ...fs.EntryInfo) error
	ListObjects(bucket, prefix, marker string, limit int) ([]fs.EntryInfo, bool, error)
	ListUnprocessedDirs(bucket string, limit int) ([]fs.EntryInfo, error)
	ListEmptyDirs(bucket string, limit int) ([]fs.EntryInfo, error)
	Stat(path string) (fs.EntryInfo, error)
	StatObject(bucket, key string) (fs.EntryInfo, error)
	GetStats(bucket string) (processed int, unprocessed int, totalSize int64, err error)
	DeleteObject(path string) (int64, error)
	DeleteDir(path string) (int64, error)
	DeleteUnprocessed(bucket string) (int64, error)
	SetProcessed(path string, processed bool) error
	ResetProcessedFlags(bucket string) error
}
