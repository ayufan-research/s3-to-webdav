package internal

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DBSync handles synchronization between WebDAV server and database
type DBSync struct {
	client     Fs
	db         *DBCache
	persistDir string

	// Statistics
	lastStatus time.Time
}

// NewDBSync creates a new WebDAV synchronizer
func NewDBSync(client Fs, db *DBCache, persistDir string) *DBSync {
	return &DBSync{
		client:     client,
		db:         db,
		persistDir: persistDir,
	}
}

// Sync performs a sync of WebDAV content to the database
func (ws *DBSync) Sync(bucket string) error {
	start := time.Now()

	// Ensure root directory entry exists
	if entry, err := ws.db.Stat(bucket); err != nil || !entry.IsDir {
		err := ws.db.InsertObjects(EntryInfo{
			Path:         bucket,
			Bucket:       bucket,
			Key:          "",
			Size:         0,
			LastModified: time.Now().Unix(),
			IsDir:        true,
			Processed:    false,
		})
		if err != nil {
			return fmt.Errorf("failed to create root directory entry for %s: %v", bucket, err)
		}
		log.Printf("Sync: Created root directory entry for %s", bucket)
	}

	if processedCount, unprocessedCount, _, err := ws.db.GetStats(bucket); err != nil {
		return err
	} else if unprocessedCount == 0 {
		log.Printf("Sync: No unprocessed entries for %s, skipping sync", bucket)
		return nil
	} else {
		log.Printf("Sync: %d processed and %d unprocessed entries for %s, starting sync",
			processedCount, unprocessedCount, bucket)
	}

	const maxParallel = 2

	send := make(chan EntryInfo)
	recv := make(chan error)
	wg := sync.WaitGroup{}
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		go func() {
			defer wg.Done()
			for dir := range send {
				err := ws.walkDir(dir.Path)
				if err != nil {
					log.Printf("Sync: Error walking directory %s: %v", dir, err)
				}
				recv <- err
			}
		}()
	}

	pending := 0

	for {
		queue, err := ws.db.ListUnprocessedDirs(bucket, 50)
		if err != nil {
			log.Printf("Sync: Failed to list unprocessed directories: %v", err)
			break
		}
		if len(queue) == 0 && pending == 0 {
			break
		}

		for len(queue) > 0 {
			dir := queue[len(queue)-1]
			select {
			case send <- dir:
				queue = queue[:len(queue)-1]
				pending++
			case <-recv:
				pending--
			}
			ws.printStats(bucket)
		}

		if pending > 0 {
			select {
			case <-recv:
				pending--
			}
		}
	}

	close(send)
	wg.Wait()
	close(recv)

	if deleted, err := ws.db.DeleteUnprocessed(bucket); err != nil {
		log.Printf("Sync: Failed to delete old entries for bucket %s: %v", bucket, err)
	} else if deleted > 0 {
		log.Printf("Sync: Deleted %d old unprocessed entries for bucket %s", deleted, bucket)
	}

	if processedCount, _, totalSize, err := ws.db.GetStats(bucket); err == nil {
		log.Printf("Sync: Loaded %d objects (%.2f MB total) into database",
			processedCount, float64(totalSize)/1024/1024)
	}

	log.Printf("Sync: WebDAV sync completed in %v", time.Since(start))
	return nil
}

func (ws *DBSync) walkDir(path string) error {
	// Ignore recently processed
	if entryInfo, err := ws.db.Stat(path); err == nil && (!entryInfo.IsDir || entryInfo.Processed) {
		return nil
	}

	// Read directory
	infos, err := ws.client.ReadDir(path)
	if err != nil {
		log.Printf("Sync: Failed to read directory %s: %v", path, err)
		return err
	}

	batchInfos := make([]EntryInfo, 0, len(infos))

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
			Processed:    !info.IsDir(),
		}
		batchInfos = append(batchInfos, fileInfo)
	}

	err = ws.db.InsertObjects(batchInfos...)
	if err != nil {
		return err
	}

	err = ws.db.MarkAsProcessed(path)
	if err != nil {
		return err
	}

	return nil
}

func (ws *DBSync) printStats(bucket string) {
	if time.Since(ws.lastStatus) < time.Second {
		return
	}
	ws.lastStatus = time.Now()

	processedCount, unprocessedCount, totalSize, err := ws.db.GetStats(bucket)
	if err != nil {
		return
	}

	log.Printf("Sync: Processed %d objects, %d in queue (%.2f MB total) so far...",
		processedCount, unprocessedCount, float64(totalSize)/1024/1024)
}
