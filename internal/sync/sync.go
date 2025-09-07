package sync

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"s3-to-webdav/internal/cache"
	"s3-to-webdav/internal/fs"
)

// Sync handles synchronization between WebDAV server and database
type Sync struct {
	client fs.Fs
	db     cache.Cache

	// Statistics
	lastStatus time.Time
}

// New creates a new WebDAV synchronizer
func New(client fs.Fs, db cache.Cache) *Sync {
	return &Sync{
		client: client,
		db:     db,
	}
}

func (ws *Sync) Clean(bucket string) error {
	start := time.Now()

	missing := 0
	removed := 0
	rescanned := 0
	errors := 0

	for {
		dirs, err := ws.db.ListDanglingDirs(bucket+"/", 50)
		if err != nil {
			return fmt.Errorf("failed to list empty dirs: %v", err)
		} else if len(dirs) == 0 {
			break
		}

		for _, dir := range dirs {
			infos, err := ws.client.ReadDir(dir.Path)

			if fs.IsNotFound(err) {
				if err := ws.db.Delete(dir.Path); err != nil {
					log.Printf("Clean: Failed to delete missing dir %s from database: %v", dir.Path, err)
					errors++
				}
				missing++
			} else if err != nil && !os.IsNotExist(err) {
				log.Printf("Clean: Failed to read dir %s: %v", dir.Path, err)
				errors++
			} else if len(infos) > 0 {
				// Has files, re-process directory
				if _, err := ws.db.SetProcessed(dir.Path, false, false); err != nil {
					log.Printf("Clean: Failed to mark dir %s as unprocessed: %v", dir.Path, err)
					errors++
				} else {
					rescanned++
				}
			} else {
				if err := ws.client.Remove(dir.Path + "/"); err == nil {
					ws.db.Delete(dir.Path)
					removed++
				} else {
					log.Printf("Clean: Failed to delete empty dir %s: %v", dir.Path, err)
					errors++
				}
			}
		}

		ws.printStats(bucket)
	}

	log.Printf("Clean: Found %d missing, %d removed, %d rescanned, %d errors",
		missing, removed, rescanned, errors)
	log.Printf("Clean: Completed in %v for %s bucket", time.Since(start), bucket)
	return nil
}

// Sync performs a sync of WebDAV content to the database
func (ws *Sync) Sync(bucket string) error {
	start := time.Now()
	prefix := bucket + "/"

	// Ensure root directory entry exists
	if entry, err := ws.db.Stat(prefix); err != nil || !entry.IsDir {
		err := ws.db.Insert(fs.EntryInfo{
			Path:         prefix,
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

	if processedCount, unprocessedCount, _, err := ws.db.GetStats(prefix); err != nil {
		return err
	} else if unprocessedCount == 0 {
		log.Printf("Sync: No unprocessed entries for %s, skipping sync", bucket)
		return nil
	} else {
		log.Printf("Sync: %d processed and %d unprocessed entries for %s, starting sync",
			processedCount, unprocessedCount, bucket)
	}

	const maxParallel = 2

	send := make(chan fs.EntryInfo)
	recv := make(chan error)
	wg := sync.WaitGroup{}
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		go func() {
			defer wg.Done()
			for dir := range send {
				err := ws.walkDir(dir.Path)
				if err != nil {
					log.Printf("Sync: Error walking directory %s: %v", dir.Path, err)
				}
				recv <- err
			}
		}()
	}

	pending := 0

	for {
		queue, err := ws.db.ListPendingDirs(prefix, 50)
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

	if deleted, err := ws.db.DeleteDanglingFiles(prefix); err != nil {
		log.Printf("Sync: Failed to delete old entries for bucket %s: %v", bucket, err)
	} else if deleted > 0 {
		log.Printf("Sync: Deleted %d old unprocessed entries for bucket %s", deleted, bucket)
	}

	if processedCount, _, totalSize, err := ws.db.GetStats(prefix); err == nil {
		log.Printf("Sync: Loaded %d objects (%.2f MB total) into database",
			processedCount, float64(totalSize)/1024/1024)
	}

	log.Printf("Sync: WebDAV sync completed in %v", time.Since(start))
	return nil
}

func (ws *Sync) walkDir(path string) error {
	// Ignore recently processed
	if entryInfo, err := ws.db.Stat(path); err == nil && (!entryInfo.IsDir || entryInfo.Processed) {
		return nil
	}

	// Read directory
	infos, err := ws.client.ReadDir(path)
	if fs.IsNotFound(err) {
		_, err = ws.db.SetProcessed(path, false, true)
		return err
	} else if err != nil {
		log.Printf("Sync: Failed to read directory %s: %v", path, err)
		return err
	}

	batchInfos := make([]fs.EntryInfo, 0, len(infos))

	for _, info := range infos {
		fullPath := filepath.Join(path, info.Name())
		fullPath = strings.ReplaceAll(fullPath, "\\", "/")
		if info.IsDir() {
			fullPath += "/"
		}

		fileInfo := fs.EntryInfo{
			Path:         fullPath,
			Size:         info.Size(),
			LastModified: info.ModTime().Unix(),
			IsDir:        info.IsDir(),
			Processed:    !info.IsDir(),
		}
		batchInfos = append(batchInfos, fileInfo)
	}

	err = ws.db.Insert(batchInfos...)
	if err != nil {
		return err
	}

	_, err = ws.db.SetProcessed(path, false, true)
	if err != nil {
		return err
	}

	return nil
}

func (ws *Sync) printStats(bucket string) {
	if time.Since(ws.lastStatus) < time.Second {
		return
	}
	ws.lastStatus = time.Now()

	processedCount, unprocessedCount, totalSize, err := ws.db.GetStats(bucket + "/")
	if err != nil {
		return
	}

	log.Printf("Sync: Processed %d objects, %d in queue (%.2f MB total) so far...",
		processedCount, unprocessedCount, float64(totalSize)/1024/1024)
}
