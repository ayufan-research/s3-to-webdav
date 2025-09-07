package cache

import (
	"s3-to-webdav/internal/fs"
)

type Cache interface {
	Close() error
	Optimise() error

	Insert(objects ...fs.EntryInfo) error
	List(prefix, marker string, dirOnly bool, limit int) ([]fs.EntryInfo, bool, error)
	Stat(path string) (fs.EntryInfo, error)
	Delete(path string) error

	GetStats(prefix string) (processed int, unprocessed int, totalSize int64, err error)

	ListPendingDirs(prefix string, limit int) ([]fs.EntryInfo, error)
	ListDanglingDirs(prefix string, limit int) ([]fs.EntryInfo, error)
	DeleteDanglingFiles(prefix string) (int64, error)
	SetProcessed(prefix string, recursive, processed bool) (int64, error)
	DeleteDangling(prefix string, recursive bool) (int64, error)
}
