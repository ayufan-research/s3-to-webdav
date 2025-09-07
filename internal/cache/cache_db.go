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
	PRAGMA case_sensitive_like = ON;
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
		size INTEGER NOT NULL,
		last_modified INTEGER NOT NULL,
		is_dir INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		processed INTEGER NOT NULL
	);

	-- Indexes for performance
	CREATE INDEX IF NOT EXISTS idx_entries_path_dirname ON entries (rtrim(path, replace(path, '/', '')));
	ANALYZE;
	`

	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}
	return db, nil
}

func (c *cacheDB) Optimise() error {
	_, err := c.db.Exec("ANALYZE")
	return err
}

// Insert inserts multiple objects in a single transaction
func (c *cacheDB) Insert(objects ...fs.EntryInfo) error {
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
		INSERT INTO entries (path, size, last_modified, is_dir, updated_at, processed)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			size = excluded.size,
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
		if strings.HasPrefix(obj.Path, "/") {
			return fmt.Errorf("object path cannot start with '/': %s", obj.Path)
		}
		if obj.IsDir {
			if !strings.HasSuffix(obj.Path, "/") {
				return fmt.Errorf("directory path must end with '/': %s", obj.Path)
			}
		} else {
			if strings.HasSuffix(obj.Path, "/") {
				return fmt.Errorf("file path cannot end with '/': %s", obj.Path)
			}
		}

		_, err := stmt.Exec(obj.Path, obj.Size,
			obj.LastModified, obj.IsDir, now, obj.Processed)
		if err != nil {
			return fmt.Errorf("failed to insert object %s: %v", obj.Path, err)
		}
	}

	return tx.Commit()
}

func (c *cacheDB) scanEntry(scanner func(dest ...any) error) (fs.EntryInfo, error) {
	var path string
	var size, lastModified int64
	var isDir, processed int

	if err := scanner(&path, &size, &lastModified, &isDir, &processed); err != nil {
		return fs.EntryInfo{}, fmt.Errorf("failed to scan row: %v", err)
	}

	return fs.EntryInfo{
		Path:         path,
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
		SELECT path, size, last_modified, is_dir, processed
		FROM entries WHERE `+where, args...)
	return c.scanEntry(row.Scan)
}

func (c *cacheDB) findObjects(where string, args ...any) ([]fs.EntryInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rows, err := c.db.Query(`
		SELECT path, size, last_modified, is_dir, processed
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

// List retrieves objects from a bucket with optional prefix and marker
// Returns objects up to the specified limit, ordered by path
// Also returns whether results were truncated
func (c *cacheDB) List(prefix, marker string, dirOnly bool, limit int) ([]fs.EntryInfo, bool, error) {
	if strings.HasPrefix(prefix, "/") {
		return nil, false, fmt.Errorf("prefix cannot start with '/': %s", prefix)
	}
	if !strings.HasSuffix(prefix, "/") && prefix != "" {
		return nil, false, fmt.Errorf("prefix must end with '/' if not empty: %s", prefix)
	}
	if strings.HasPrefix(marker, "/") {
		return nil, false, fmt.Errorf("marker cannot start with '/': %s", marker)
	}

	// Base query
	query := "1=1"
	args := []interface{}{}

	if marker != "" {
		query += " AND path > ?"
		args = append(args, marker)
	}

	if prefix != "" {
		query += " AND path > ? AND path < ?"
		args = append(args, prefix, prefix+"\xFF")
	}

	if dirOnly {
		query += " AND rtrim(path, '/') NOT LIKE ?"
		args = append(args, prefix+"%/%")
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

// Stat checks if an object exists and returns its metadata
func (c *cacheDB) Stat(path string) (fs.EntryInfo, error) {
	if strings.HasPrefix(path, "/") {
		return fs.EntryInfo{}, fmt.Errorf("object path cannot start with '/': %s", path)
	}
	return c.findObject("path = ?", path)
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

func (c *cacheDB) Delete(path string) error {
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("object path cannot start with '/': %s", path)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := "DELETE FROM entries WHERE 1=1"
	args := []any{}

	if strings.HasSuffix(path, "/") {
		query += " AND path LIKE ?"
		args = append(args, path+"%")
	} else {
		query += " AND path = ?"
		args = append(args, path)
	}

	result, err := tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete entry: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %v", err)
	}
	if rowsAffected == 0 {
		return nil
		// return fmt.Errorf("no entry found for path: %s", path)
	}
	if rowsAffected > 1 {
		return fmt.Errorf("multiple entries deleted for path: %s", path)
	}

	return tx.Commit()
}

// GetStats returns the number of processed and pending entries
func (c *cacheDB) GetStats(prefix string) (processed int, pending int, totalSize int64, err error) {
	if strings.HasPrefix(prefix, "/") {
		return 0, 0, 0, fmt.Errorf("object path cannot start with '/': %s", prefix)
	}
	if !strings.HasSuffix(prefix, "/") {
		return 0, 0, 0, fmt.Errorf("prefix must end with '/': %s", prefix)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	err = c.db.QueryRow(`SELECT
		COALESCE(SUM(processed==1), 0),
		COALESCE(SUM(processed==0), 0),
		COALESCE(SUM(size), 0)
		FROM entries WHERE path LIKE ?`,
		prefix+"%").Scan(&processed, &pending, &totalSize)
	if err != nil {
		return 0, 0, 0, err
	}
	return processed, pending, totalSize, err
}

func (c *cacheDB) ListPendingDirs(prefix string, limit int) ([]fs.EntryInfo, error) {
	if strings.HasPrefix(prefix, "/") {
		return nil, fmt.Errorf("prefix cannot start with '/': %s", prefix)
	}
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("prefix must end with '/': %s", prefix)
	}

	return c.findObjects("path LIKE ? AND processed = 0 AND is_dir = 1 ORDER BY path LIMIT ?", prefix+"%", limit)
}

func (c *cacheDB) ListDanglingDirs(prefix string, limit int) ([]fs.EntryInfo, error) {
	if strings.HasPrefix(prefix, "/") {
		return nil, fmt.Errorf("prefix cannot start with '/': %s", prefix)
	}
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("prefix must end with '/': %s", prefix)
	}

	return c.findObjects(`path LIKE ? AND processed = 1 AND is_dir=1 AND path || '/' NOT IN (
		SELECT DISTINCT rtrim(path, replace(path, '/', ''))
		FROM entries WHERE path LIKE ?
	) ORDER BY path DESC LIMIT ?`, prefix+"%", prefix+"%", limit)
}

func (c *cacheDB) DeleteDanglingFiles(prefix string) (int64, error) {
	if strings.HasPrefix(prefix, "/") {
		return 0, fmt.Errorf("prefix cannot start with '/': %s", prefix)
	}
	if !strings.HasSuffix(prefix, "/") {
		return 0, fmt.Errorf("prefix must end with '/': %s", prefix)
	}
	return c.execSql("DELETE FROM entries WHERE path LIKE ? AND is_dir = 0 AND processed = 0", prefix+"%")
}

func (c *cacheDB) SetProcessed(prefix string, recursive, processed bool) (int64, error) {
	if strings.HasPrefix(prefix, "/") {
		return 0, fmt.Errorf("prefix cannot start with '/': %s", prefix)
	}

	if strings.HasSuffix(prefix, "/") && recursive {
		return c.execSql("UPDATE entries SET processed = ? WHERE processed <> ? AND path LIKE ?", processed, processed, prefix+"%")
	}
	return c.execSql("UPDATE entries SET processed = ? WHERE processed <> ? AND path = ?", processed, processed, prefix)
}
