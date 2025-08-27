package internal

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// DBSync handles synchronization between WebDAV server and database
type DBSync struct {
	client     Fs
	db         *DBCache
	persistDir string

	// Statistics
	totalSize   int64
	objectCount int64
	dirCount    int64
	lastStatus  time.Time
}

// NewDBSync creates a new WebDAV synchronizer
func NewDBSync(client Fs, db *DBCache, persistDir string) *DBSync {
	return &DBSync{
		client:     client,
		db:         db,
		persistDir: persistDir,
	}
}

func (ws *DBSync) SyncTime(bucket string, reset bool) (int64, error) {
	syncPath := filepath.Join(ws.persistDir, "sync."+bucket)

	if !reset {
		data, err := ioutil.ReadFile(syncPath)
		if err == nil {
			return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		} else if !os.IsNotExist(err) {
			return 0, err
		}
	}

	syncTime := time.Now().Unix()

	err := ioutil.WriteFile(syncPath, []byte(strconv.FormatInt(syncTime, 10)), 0644)
	if err != nil {
		return 0, err
	}

	return syncTime, nil
}

// Sync performs a sync of WebDAV content to the database
func (ws *DBSync) Sync(bucket string) error {
	start := time.Now()

	syncTime, err := ws.SyncTime(bucket, false)
	if err != nil {
		return err
	}

	count, err := ws.db.GetCount(bucket, time.Now().Unix())
	if err != nil {
		return err
	}

	// Get list of unprocessed directories to sync
	queue, err := ws.db.GetDirs(bucket, syncTime)
	if err != nil {
		return err
	}

	if len(queue) == 0 {
		if count > 0 {
			return nil
		}
		queue = append(queue, bucket)
		log.Printf("Sync: Starting WebDAV sync for specified: %v", bucket)
	} else {
		log.Printf("Sync: Found %d unprocessed directories, resuming sync... for: %v", len(queue), bucket)
	}

	const maxParallel = 2

	send := make(chan string)
	recv := make(chan []string)

	for i := 0; i < maxParallel; i++ {
		go func() {
			for dir := range send {
				dirs, err := ws.walkWebDAVDirectory(dir, syncTime)
				if err != nil {
					log.Printf("Sync: Error walking directory %s: %v", dir, err)
				}
				recv <- dirs
			}
		}()
	}

	pending := 0

	for len(queue) > 0 || pending > 0 {
	check_pending:
		for len(queue) > 0 {
			dir := queue[len(queue)-1]
			select {
			case send <- dir:
				queue = queue[:len(queue)-1]
				pending++
			default:
				break check_pending
			}
		}

		if pending > 0 {
			select {
			case dirs := <-recv:
				if dirs != nil {
					queue = append(queue, dirs...)
					ws.printStats(len(queue))
				}
				pending--
			}
		}
	}

	if err := ws.db.DeleteOld(bucket, syncTime); err != nil {
		log.Printf("Sync: Failed to delete old entries for bucket %s: %v", bucket, err)
	}

	log.Printf("Sync: WebDAV sync completed in %v", time.Since(start))
	log.Printf("Sync: Loaded %d directories and %d objects (%.2f MB total) into database",
		ws.dirCount, ws.objectCount, float64(ws.totalSize)/1024/1024)

	return nil
}

// walkWebDAVDirectory recursively walks WebDAV directories and sends objects to channels
func (ws *DBSync) walkWebDAVDirectory(path string, cutoff int64) ([]string, error) {
	// Ignore recently processed
	if entryInfo, ok := ws.db.PathExists(path); ok && entryInfo.IsDir && entryInfo.ProcessedAt > cutoff {
		return nil, nil
	}

	infos, err := ws.client.ReadDir(path)
	if err != nil {
		log.Printf("Sync: Failed to read directory %s: %v", path, err)
		return nil, err
	}

	batchInfos := make([]EntryInfo, 0, len(infos))
	dirs := []string{}

	for _, info := range infos {
		fullPath := filepath.Join(path, info.Name())
		fullPath = strings.ReplaceAll(fullPath, "\\", "/")

		bucket, key, err := BucketAndKeyFromPath(fullPath)
		if err != nil {
			log.Printf("Sync: Failed to parse path %s: %v", fullPath, err)
			continue
		}

		// Ignore files that appear as buckets
		if key == "" && !info.IsDir() {
			continue
		}

		fileInfo := EntryInfo{
			Path:         fullPath,
			Bucket:       bucket,
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime().Unix(),
			IsDir:        info.IsDir(),
		}
		if !info.IsDir() {
			fileInfo.ProcessedAt = time.Now().Unix()
		}
		batchInfos = append(batchInfos, fileInfo)

		if info.IsDir() {
			atomic.AddInt64(&ws.dirCount, 1)
			dirs = append(dirs, fullPath)
		} else {
			atomic.AddInt64(&ws.objectCount, 1)
			atomic.AddInt64(&ws.totalSize, info.Size())
		}
	}

	err = ws.db.BatchInsertObjects(batchInfos)
	if err != nil {
		return nil, err
	}

	err = ws.db.MarkAsProcessed(path)
	if err != nil {
		return nil, err
	}

	return dirs, err
}

func (ws *DBSync) printStats(queue int) {
	if time.Since(ws.lastStatus) < time.Second {
		return
	}
	ws.lastStatus = time.Now()

	log.Printf("Sync: Processed %d objects and %d directories (%.2f MB total) so far... (%d in queue)",
		atomic.LoadInt64(&ws.objectCount), atomic.LoadInt64(&ws.dirCount), float64(atomic.LoadInt64(&ws.totalSize))/1024/1024,
		queue)
}
