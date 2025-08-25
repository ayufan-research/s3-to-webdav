package internal

import (
	"log"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/studio-b12/gowebdav"
)

// WebDAVSync handles synchronization between WebDAV server and database
type WebDAVSync struct {
	client *gowebdav.Client
	db     *DBCache

	// Statistics
	totalSize   int64
	objectCount int64
	dirCount    int64
	lastStatus  time.Time
}

// NewWebDAVSync creates a new WebDAV synchronizer
func NewWebDAVSync(client *gowebdav.Client, db *DBCache) *WebDAVSync {
	return &WebDAVSync{
		client: client,
		db:     db,
	}
}

// Sync performs a sync of WebDAV content to the database
func (ws *WebDAVSync) Sync() error {
	start := time.Now()

	count, err := ws.db.GetCount(time.Now().Unix())
	if err != nil {
		return err
	}

	// Get list of unprocessed directories to sync
	queue, err := ws.db.GetDirs(0)
	if err != nil {
		return err
	}

	if len(queue) == 0 {
		if count > 0 {
			return nil
		}
		queue = []string{"/"}
		log.Printf("Sync: Starting WebDAV sync to database...")
	} else {
		log.Printf("Sync: Found %d unprocessed directories, resuming sync...", len(queue))
	}

	const maxParallel = 10

	send := make(chan string)
	recv := make(chan []string)

	for i := 0; i < maxParallel; i++ {
		go func() {
			for dir := range send {
				dirs, err := ws.walkWebDAVDirectory(dir)
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

	log.Printf("Sync: WebDAV sync completed in %v", time.Since(start))
	log.Printf("Sync: Loaded %d directories and %d objects (%.2f MB total) into database",
		ws.dirCount, ws.objectCount, float64(ws.totalSize)/1024/1024)

	return nil
}

// walkWebDAVDirectory recursively walks WebDAV directories and sends objects to channels
func (ws *WebDAVSync) walkWebDAVDirectory(path string) ([]string, error) {
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

func (ws *WebDAVSync) printStats(queue int) {
	if time.Since(ws.lastStatus) < time.Second {
		return
	}
	ws.lastStatus = time.Now()

	log.Printf("Sync: Processed %d objects and %d directories (%.2f MB total) so far... (%d in queue)",
		atomic.LoadInt64(&ws.objectCount), atomic.LoadInt64(&ws.dirCount), float64(atomic.LoadInt64(&ws.totalSize))/1024/1024,
		queue)
}
