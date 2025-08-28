package internal

type Cache interface {
	Close() error
	InsertObjects(objects ...EntryInfo) error
	ListObjects(bucket, prefix, marker string, limit int) ([]EntryInfo, bool, error)
	ListUnprocessedDirs(bucket string, limit int) ([]EntryInfo, error)
	ListEmptyDirs(bucket string, limit int) ([]EntryInfo, error)
	Stat(path string) (EntryInfo, error)
	StatObject(bucket, key string) (EntryInfo, error)
	GetStats(bucket string) (processed int, unprocessed int, totalSize int64, err error)
	DeleteObject(path string) (int64, error)
	DeleteDir(path string) (int64, error)
	DeleteUnprocessed(bucket string) (int64, error)
	SetProcessed(path string, processed bool) error
	ResetProcessedFlags(bucket string) error
}
