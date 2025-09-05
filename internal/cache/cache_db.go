package cache

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"s3-to-webdav/internal/fs"
)

// cacheDB handles all database operations for the S3-to-WebDAV server
type cacheDB struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewCacheDB initializes a new database cache
func NewCacheDB(dbPath string) (Cache, error) {
	db, err := initDatabase(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}

	cache := &cacheDB{
		db: db,
	}

	return cache, nil
}

// Close closes the database connection
func (c *cacheDB) Close() error {
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
	PRAGMA optimize;
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
		processed INTEGER NOT NULL
	);

	-- Indexes for performance
	CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_path ON entries(path);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_bucket_key ON entries(bucket, key);
	CREATE INDEX IF NOT EXISTS idx_entries_bucket_key_depth ON entries(bucket, LENGTH(key) - LENGTH(REPLACE(key, '/', '')), key);
	CREATE INDEX IF NOT EXISTS idx_entries_bucket_processed_isdir ON entries(bucket, processed, is_dir, path);
	CREATE INDEX IF NOT EXISTS idx_entries_bucket_dirname ON entries (bucket, rtrim(path, replace(path, '/', '')));
	ANALYZE;
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	return db, nil
}

// InsertObjects inserts multiple objects in a single transaction
func (c *cacheDB) InsertObjects(objects ...fs.EntryInfo) error {
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
		INSERT INTO entries (path, bucket, key, size, last_modified, is_dir, updated_at, processed)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			bucket = excluded.bucket, key = excluded.key, size = excluded.size,
			is_dir = excluded.is_dir, updated_at = excluded.updated_at,
			last_modified = MAX(excluded.last_modified, last_modified),
			processed = MAX(excluded.processed, processed)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	now := time.Now().Unix()

	for _, obj := range objects {
		_, err := stmt.Exec(obj.Path, obj.Bucket, obj.Key, obj.Size,
			obj.LastModified, obj.IsDir, now, obj.Processed)
		if err != nil {
			return fmt.Errorf("failed to insert object %s: %v", obj.Path, err)
		}
	}

	return tx.Commit()
}

func (c *cacheDB) scanEntry(scanner func(dest ...any) error) (fs.EntryInfo, error) {
	var path, bucket, key string
	var size, lastModified int64
	var isDir, processed int

	if err := scanner(&path, &bucket, &key, &size, &lastModified, &isDir, &processed); err != nil {
		return fs.EntryInfo{}, fmt.Errorf("failed to scan row: %v", err)
	}

	return fs.EntryInfo{
		Path:         path,
		Bucket:       bucket,
		Key:          key,
		Size:         size,
		LastModified: lastModified,
		IsDir:        isDir == 1,
		Processed:    processed == 1,
	}, nil
}

func (c *cacheDB) findObject(where string, args ...any) (fs.EntryInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	row := c.db.QueryRow(`
		SELECT path, bucket, key, size, last_modified, is_dir, processed 
		FROM entries WHERE `+where, args...)
	return c.scanEntry(row.Scan)
}

func (c *cacheDB) findObjects(where string, args ...any) ([]fs.EntryInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rows, err := c.db.Query(`
		SELECT path, bucket, key, size, last_modified, is_dir, processed 
		FROM entries WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query objects: %v", err)
	}
	defer rows.Close()

	var entries []fs.EntryInfo
	for rows.Next() {
		entry, err := c.scanEntry(rows.Scan)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// ListObjects retrieves objects from a bucket with optional prefix and marker
// Returns objects up to the specified limit, ordered by path
// Also returns whether results were truncated
func (c *cacheDB) ListObjects(bucket, prefix, marker string, dirOnly bool, limit int) ([]fs.EntryInfo, bool, error) {
	// Base query
	query := "bucket = ?"
	args := []interface{}{bucket}

	if prefix != "" {
		query += " AND key LIKE ?"
		args = append(args, prefix+"%")
	}
	if marker != "" {
		query += " AND key > ?"
		args = append(args, marker)
	}

	if dirOnly {
		query += " AND key <> ''"
		query += " AND LENGTH(key) - LENGTH(REPLACE(key, '/', '')) = ?"
		args = append(args, strings.Count(prefix, "/"))
	} else {
		query += " AND is_dir = 0"
	}

	// Query for limit+1 to determine if results are truncated
	query += " ORDER BY path LIMIT ?"
	args = append(args, limit+1)

	files, err := c.findObjects(query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query objects: %v", err)
	}

	// Determine if results were truncated
	truncated := len(files) > limit
	if truncated {
		// Remove the extra item we fetched for truncation detection
		files = files[:limit]
	}

	return files, truncated, nil
}

// ListUnprocessedDirs returns a list of unprocessed directory entries up to the specified limit
func (c *cacheDB) ListUnprocessedDirs(bucket string, limit int) ([]fs.EntryInfo, error) {
	return c.findObjects("bucket = ? AND processed = 0 AND is_dir = 1 ORDER BY path LIMIT ?", bucket, limit)
}

func (c *cacheDB) ListEmptyDirs(bucket string, limit int) ([]fs.EntryInfo, error) {
	return c.findObjects(`bucket = ? AND processed = 1 AND is_dir=1 AND key != '' AND path || '/' NOT IN (
		SELECT DISTINCT rtrim(path, replace(path, '/', ''))
		FROM entries WHERE bucket = ?
	) ORDER BY path DESC LIMIT ?`, bucket, bucket, limit)
}

// Stat checks if an object exists and returns its metadata
func (c *cacheDB) Stat(path string) (fs.EntryInfo, error) {
	return c.findObject("path = ?", path)
}

// StatObject checks if an object exists and returns its metadata
func (c *cacheDB) StatObject(bucket, key string) (fs.EntryInfo, error) {
	return c.findObject("bucket = ? AND key = ?", bucket, key)
}

// GetStats returns the number of processed and unprocessed entries
func (c *cacheDB) GetStats(bucket string) (processed int, unprocessed int, totalSize int64, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	err = c.db.QueryRow("SELECT SUM(processed==1), SUM(processed==0), SUM(size) FROM entries WHERE bucket = ?",
		bucket).Scan(&processed, &unprocessed, &totalSize)
	if err != nil {
		return 0, 0, 0, err
	}
	return processed, unprocessed, totalSize, err
}

func (c *cacheDB) execSql(query string, args ...any) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	result, err := c.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	return rowsAffected, err
}

func (c *cacheDB) DeleteObject(bucket, key string) (int64, error) {
	return c.execSql("DELETE FROM entries WHERE bucket = ? AND key = ?", bucket, key)
}

func (c *cacheDB) DeleteDir(path string) (int64, error) {
	return c.execSql("DELETE FROM entries WHERE path = ? OR path LIKE ? || '/%'", path, path)
}

func (c *cacheDB) DeleteUnprocessed(bucket string) (int64, error) {
	return c.execSql("DELETE FROM entries WHERE bucket = ? AND processed = 0", bucket)
}

func (c *cacheDB) SetProcessed(path string, processed bool) error {
	_, err := c.execSql("UPDATE entries SET processed = ?, updated_at = ? WHERE path = ?", processed, time.Now().Unix(), path)
	return err
}

func (c *cacheDB) ResetProcessedFlags(bucket string) error {
	_, err := c.execSql("UPDATE entries SET processed = 0 WHERE bucket = ?", bucket)
	return err
}
