package internal

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DBCache handles all database operations for the S3-to-WebDAV server
type DBCache struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewDBCache initializes a new database cache
func NewDBCache(dbPath string) (*DBCache, error) {
	db, err := initDatabase(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	cache := &DBCache{
		db: db,
	}

	return cache, nil
}

// Close closes the database connection
func (c *DBCache) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// initDatabase creates and configures the SQLite database
func initDatabase(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Enable performance optimizations
	pragmas := `
	PRAGMA journal_mode = WAL;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = 1000000;
	PRAGMA temp_store = memory;
	PRAGMA mmap_size = 268435456;
	PRAGMA foreign_keys = ON;
	`
	if _, err := db.Exec(pragmas); err != nil {
		return nil, fmt.Errorf("failed to set pragmas: %v", err)
	}

	// Create simple single table schema
	schema := `
	-- Single entries table for all files and directories
	CREATE TABLE IF NOT EXISTS entries (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		path TEXT NOT NULL UNIQUE,
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		size INTEGER NOT NULL,
		last_modified INTEGER NOT NULL,
		is_dir INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		processed_at INTEGER NOT NULL
	);

	-- Indexes for performance
	CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_path ON entries(path);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_bucket_key ON entries(bucket, key);
	CREATE INDEX IF NOT EXISTS idx_entries_bucket ON entries(bucket);
	CREATE INDEX IF NOT EXISTS idx_entries_bucket_prefix ON entries(bucket, key COLLATE NOCASE);
	CREATE INDEX IF NOT EXISTS idx_entries_is_dir ON entries(is_dir);
	CREATE INDEX IF NOT EXISTS idx_entries_updated_at ON entries(updated_at);
	CREATE INDEX IF NOT EXISTS idx_entries_processed_at ON entries(processed_at);
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	return db, nil
}

// InsertObject inserts a single object into the database
func (c *DBCache) InsertObject(fileInfo EntryInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()

	// Insert entry
	_, err := c.db.Exec(`
		INSERT OR REPLACE INTO entries (path, bucket, key, size, last_modified, is_dir, updated_at, processed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		fileInfo.Path, fileInfo.Bucket, fileInfo.Key, fileInfo.Size, fileInfo.LastModified,
		map[bool]int{false: 0, true: 1}[fileInfo.IsDir], now, fileInfo.ProcessedAt)

	return err
}

// DeleteObject removes an object from the database
func (c *DBCache) DeleteObject(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Delete entry
	_, err := c.db.Exec("DELETE FROM entries WHERE path = ?", path)
	return err
}

// BatchInsertObjects inserts multiple objects in a single transaction
func (c *DBCache) BatchInsertObjects(objects []EntryInfo) error {
	if len(objects) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO entries (path, bucket, key, size, last_modified, is_dir, updated_at, processed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	now := time.Now().Unix()

	for _, obj := range objects {
		_, err := stmt.Exec(obj.Path, obj.Bucket, obj.Key, obj.Size,
			obj.LastModified, map[bool]int{false: 0, true: 1}[obj.IsDir], now, obj.ProcessedAt)
		if err != nil {
			return fmt.Errorf("failed to insert object %s: %v", obj.Path, err)
		}
	}

	return tx.Commit()
}

// ClearAllObjects removes all objects from the database
func (c *DBCache) ClearAllObjects() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear entries table
	_, err := c.db.Exec("DELETE FROM entries")
	return err
}

// ListObjects retrieves objects from a bucket with optional prefix and marker
// Returns objects up to the specified limit, ordered by path
// Also returns whether results were truncated
func (c *DBCache) ListObjects(bucket, prefix, marker string, limit int) ([]EntryInfo, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Base query
	query := "SELECT path, bucket, key, size, last_modified, is_dir, processed_at FROM entries"
	query += " WHERE bucket = ? AND is_dir = 0"
	args := []interface{}{bucket}

	if prefix != "" {
		query += " AND key LIKE ?"
		args = append(args, prefix+"%")
	}
	if marker != "" {
		query += " AND key > ?"
		args = append(args, marker)
	}

	// Query for limit+1 to determine if results are truncated
	query += " ORDER BY path LIMIT ?"
	args = append(args, limit+1)

	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query objects: %v", err)
	}
	defer rows.Close()

	var files []EntryInfo
	for rows.Next() {
		var path, bucket, key string
		var size, lastModified, processedAt int64
		var isDir int

		if err := rows.Scan(&path, &bucket, &key, &size, &lastModified, &isDir, &processedAt); err != nil {
			return nil, false, fmt.Errorf("failed to scan row: %v", err)
		}

		files = append(files, EntryInfo{
			Path:         path,
			Bucket:       bucket,
			Key:          key,
			Size:         size,
			LastModified: lastModified,
			IsDir:        isDir == 1,
			ProcessedAt:  processedAt,
		})
	}

	// Determine if results were truncated
	truncated := len(files) > limit
	if truncated {
		// Remove the extra item we fetched for truncation detection
		files = files[:limit]
	}

	return files, truncated, nil
}

// ObjectExists checks if an object exists and returns its metadata
func (c *DBCache) ObjectExists(bucket, key string) (EntryInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var fileInfo EntryInfo
	var lastModified, processedAt int64
	var isDir int

	err := c.db.QueryRow(`
		SELECT path, bucket, key, size, last_modified, is_dir, processed_at 
		FROM entries WHERE bucket = ? AND key = ?`, bucket, key).Scan(
		&fileInfo.Path, &fileInfo.Bucket, &fileInfo.Key, &fileInfo.Size, &lastModified, &isDir, &processedAt)

	if err != nil {
		return EntryInfo{}, false
	}

	fileInfo.LastModified = lastModified
	fileInfo.ProcessedAt = processedAt
	fileInfo.IsDir = isDir == 1
	return fileInfo, true
}

// GetCount returns the number of entries processed at or before the cutoff time
func (c *DBCache) GetCount(bucket string, cutoff int64) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var count int
	err := c.db.QueryRow("SELECT COUNT(*) FROM entries WHERE bucket = ? AND processed_at <= ?",
		bucket, cutoff).Scan(&count)
	return count, err
}

// GetDirs returns a list of directories processed at or before the cutoff time
func (c *DBCache) GetDirs(bucket string, cutoff int64) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rows, err := c.db.Query("SELECT DISTINCT path FROM entries WHERE bucket = ? AND processed_at <= ? AND is_dir = 1 ORDER BY path",
		bucket, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to query unprocessed directories: %v", err)
	}
	defer rows.Close()

	var dirs []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("failed to scan directory: %v", err)
		}
		dirs = append(dirs, path)
	}

	return dirs, nil
}

// MarkAsProcessed marks a single entry as processed
func (c *DBCache) MarkAsProcessed(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec("UPDATE entries SET processed_at = ? WHERE path = ?", time.Now().Unix(), path)
	return err
}
