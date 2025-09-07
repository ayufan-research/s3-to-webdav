package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"s3-to-webdav/internal/access_log"
	"s3-to-webdav/internal/cache"
	"s3-to-webdav/internal/fs"
	"s3-to-webdav/internal/helpers"
	"s3-to-webdav/internal/s3"
	"s3-to-webdav/web"
)

var (
	// SFTP configuration
	sftpHost     = flag.String("sftp-host", os.Getenv("SFTP_HOST"), "SFTP server hostname")
	sftpPort     = flag.Int("sftp-port", getEnvOrDefaultInt("SFTP_PORT", 22), "SFTP server port")
	sftpUser     = flag.String("sftp-user", os.Getenv("SFTP_USER"), "SFTP username")
	sftpPassword = flag.String("sftp-password", os.Getenv("SFTP_PASSWORD"), "SFTP password")
	sftpHostKey  = flag.String("sftp-hostkey", os.Getenv("SFTP_HOSTKEY"), "SFTP host key fingerprint (required)")
	sftpBasePath = flag.String("sftp-base-path", getEnvOrDefault("SFTP_BASE_PATH", ""), "SFTP base path")

	// S3/AWS configuration
	accessKey      = flag.String("aws-access-key", os.Getenv("AWS_ACCESS_KEY_ID"), "S3 access key")
	secretKey      = flag.String("aws-secret-key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "S3 secret key")
	accessInsecure = flag.Bool("aws-access-insecure", getEnvOrDefault("AWS_ACCESS_INSECURE", "false") == "true", "Allow insecure, secret-less access")

	// Server configuration
	httpPort = flag.String("http-port", getEnvOrDefault("HTTP_PORT", "8080"), "HTTP/HTTPS server port")
	httpOnly = flag.Bool("http-only", getEnvOrDefault("HTTP_ONLY", "false") == "true", "Enable HTTP only mode")

	// TLS configuration
	tlsCert = flag.String("tls-cert", os.Getenv("TLS_CERT"), "TLS certificate file path")
	tlsKey  = flag.String("tls-key", os.Getenv("TLS_KEY"), "TLS key file path")

	// Persistence configuration
	persistDir = flag.String("persist-dir", getEnvOrDefault("PERSIST_DIR", "./data"), "Directory to store persistent data")

	// Bucket configuration
	buckets = flag.String("buckets", os.Getenv("BUCKETS"), "Comma-separated list of bucket names to sync (required)")

	// Help
	help = flag.Bool("help", false, "Show help message")

	// Read-only mode
	readOnly = flag.Bool("read-only", getEnvOrDefault("READ_ONLY", "false") == "true", "Enable read-only mode (disables PUT, DELETE operations)")

	// Browser mode
	browser = flag.Bool("browser", getEnvOrDefault("BROWSER", "false") == "true", "Enable built-in browser")

	// Maintenance commands
	scan  = flag.Bool("scan", true, "Scan and sync existing files from SFTP to the database")
	clean = flag.Bool("clean", false, "Clean empty directories after scan")
	serve = flag.Bool("serve", true, "Run the server after scan")
)

func getEnvOrDefault(envKey, defaultValue string) string {
	if value := os.Getenv(envKey); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(envKey string, defaultValue int) int {
	if value := os.Getenv(envKey); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func usage() {
	fmt.Println("S3-to-SFTP Bridge Server")
	fmt.Println("========================")
	fmt.Println("A simple S3-compatible API server that uses SFTP as the underlying storage backend.")
	fmt.Println()
	fmt.Println("Usage:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Environment variables (used as defaults for flags):")
	fmt.Println("  SFTP_HOST             - SFTP server hostname (required)")
	fmt.Println("  SFTP_USER             - SFTP username (required)")
	fmt.Println("  SFTP_PASSWORD         - SFTP password (required)")
	fmt.Println("  SFTP_HOSTKEY          - SFTP host key fingerprint (required)")
	fmt.Println("  SFTP_BASE_PATH        - SFTP base path (default: /)")
	fmt.Println("  AWS_ACCESS_KEY_ID     - S3 access key for authentication (optional)")
	fmt.Println("  AWS_SECRET_ACCESS_KEY - S3 secret key for authentication (optional)")
	fmt.Println("  AWS_ACCESS_INSECURE   - Allow insecure, secret-less access to S3 (default: false)")
	fmt.Println("  HTTP_PORT             - Server port (default: 8080)")
	fmt.Println("  HTTP_ONLY             - Enable HTTP only (no HTTPS) (default: false)")
	fmt.Println("  TLS_CERT              - TLS certificate file path (optional)")
	fmt.Println("  TLS_KEY               - TLS key file path (optional)")
	fmt.Println("  PERSIST_DIR           - Directory for persistent data (certificates and keys) (default: ./data)")
	fmt.Println("  BUCKETS               - Comma-separated list of bucket names to sync (required)")
	fmt.Println("  READ_ONLY             - Enable read-only mode (disables PUT, DELETE operations) (default: false)")
	fmt.Println("  BROWSER               - Enable built-in browser under the `/-/browser/` (default: false)")
	fmt.Println()
	os.Exit(0)
}

func loadAccessKeys() s3.AuthConfig {
	// Get or generate S3 credentials
	if *accessInsecure {
		if *accessKey != "" || *secretKey != "" {
			log.Fatalf("Cannot use -aws-access-insecure with provided access or secret keys")
		}
		log.Printf("S3: Authentication disabled")
		return s3.AuthConfig{}
	}

	if *accessKey != "" && *secretKey != "" {
		log.Printf("S3: Using provided credentials")
		log.Printf("S3: Access Key: %s", *accessKey)
		return s3.AuthConfig{
			AccessKey: *accessKey,
			SecretKey: *secretKey,
		}
	}

	log.Printf("S3: Generated/loaded credentials from %s", *persistDir)
	accessKey, err := helpers.GetOrCreateRandomSecret(filepath.Join(*persistDir, "access_key"), 20)
	if err != nil {
		log.Fatalf("Failed to get/create S3 access key: %v", err)
	}
	secretKey, err := helpers.GetOrCreateRandomSecret(filepath.Join(*persistDir, "secret_key"), 20)
	if err != nil {
		log.Fatalf("Failed to get/create S3 secret key: %v", err)
	}
	log.Printf("S3: Access Key: %s", accessKey)
	log.Printf("S3: Secret Key: %s", secretKey)
	return s3.AuthConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
}

func loadCerts() (string, string) {
	if *tlsCert != "" || *tlsKey != "" {
		return *tlsCert, *tlsKey
	}

	// Generate certificates if those are missing
	tlsCert, tlsKey, err := helpers.GetOrCreateCertificates(*persistDir)
	if err != nil {
		log.Fatalf("Failed to get/create certificates: %v", err)
	}
	return tlsCert, tlsKey
}

func runServe(db cache.Cache, client fs.Fs, bucketMap map[string]interface{}) {
	s3Server := s3.NewServer(db, client)
	s3Server.SetBucketMap(bucketMap)

	s3AuthConfig := loadAccessKeys()

	// Setup S3 API routes with auth
	s3Router := mux.NewRouter()
	s3Server.SetupReadRoutes(s3Router)
	if !*readOnly {
		s3Server.SetupWriteRoutes(s3Router)
	} else {
		log.Printf("Read-Only: Write operations are disabled")
	}
	s3Handler := s3.AuthMiddleware(s3AuthConfig, s3Router)

	// Setup main router
	mainRouter := mux.NewRouter()

	// Add browser endpoint (outside of auth)
	if *browser {
		mainRouter.HandleFunc("/-/browser/{key:.*}", func(w http.ResponseWriter, req *http.Request) {
			query := req.URL.Query()

			// Check if access key is missing and server requires auth
			if s3AuthConfig.AccessKey != "" && query.Get("access_key") == "" {
				query.Set("access_key", s3AuthConfig.AccessKey)
			}

			// Check if read_only parameter is missing when server is in read-only mode
			if *readOnly && query.Get("read_only") == "" {
				query.Set("read_only", "true")
			}

			if req.URL.Query().Encode() != query.Encode() {
				redirectURL := *req.URL
				redirectURL.RawQuery = query.Encode()
				http.Redirect(w, req, redirectURL.String(), http.StatusTemporaryRedirect)
				return
			}

			w.Header().Set("Content-Type", "text/html")
			w.Write(web.IndexHTML)
		})
	}

	// Mount authenticated S3 routes
	mainRouter.PathPrefix("/").Handler(s3Handler)

	// Wrap with access logging middleware
	handler := access_log.AccessLogMiddleware(mainRouter)

	// Start server with or without TLS
	if *httpOnly {
		log.Printf("HTTP: Server ready! Listening on http://:%s", *httpPort)
		log.Fatal(http.ListenAndServe(":"+*httpPort, handler))
		return
	}

	tlsCert, tlsKey := loadCerts()
	log.Printf("TLS: Certificate: %s / %s", tlsCert, tlsKey)
	if fingerprint, err := helpers.GetCertificateFingerprint(tlsCert); err == nil {
		log.Printf("TLS: Fingerprint: %s", fingerprint)
	}
	log.Printf("HTTPS: Server ready! Listening on https://:%s", *httpPort)
	log.Fatal(http.ListenAndServeTLS(":"+*httpPort, tlsCert, tlsKey, handler))
}

func runScan(client fs.Fs, db cache.Cache, bucketMap map[string]interface{}) {
	now := time.Now()

	for bucket := range bucketMap {
		log.Printf("Scan: Scanning bucket: %s", bucket)

		entries, err := client.Tree(bucket)
		if err != nil {
			log.Printf("Scan: Failed to read existing entries for bucket %s: %v", bucket, err)
			continue
		}

		for i := range entries {
			entries[i].Processed = true
		}

		if *clean && len(entries) > 0 {
			log.Printf("Scan: Cleaning empty directories in bucket %s...", bucket)

			for i := len(entries) - 1; i >= 0; i-- {
				entry := entries[i]
				if !entry.IsDir {
					continue
				}

				dir := entry.Path
				last := ""
				if i+1 < len(entries) {
					last = filepath.Dir(strings.TrimSuffix(entries[i+1].Path, "/")) + "/"
				}

				if dir == last {
					continue
				}

				err := client.Remove(dir)
				if err == nil {
					log.Printf("Scan: Removed empty directory: %s", dir)
					entries = append(entries[:i], entries[i+1:]...)
				} else if !fs.IsNotFound(err) {
					log.Printf("Scan: Failed to remove directory %s: %v", dir, err)
				}
			}
		}

		if _, err = db.SetProcessed(bucket+"/", true, false); err != nil {
			log.Printf("Scan: Failed to mark existing entries as processed for bucket %s: %v", bucket, err)
			continue
		}

		if err = db.Insert(entries...); err != nil {
			log.Printf("Scan: Failed to insert existing entries for bucket %s: %v", bucket, err)
			continue
		}

		dangling, err := db.DeleteDangling(bucket+"/", true)
		if err != nil {
			log.Printf("Scan: Failed to delete dangling entries for bucket %s: %v", bucket, err)
			continue
		}

		processed, _, totalSize, err := db.GetStats(bucket + "/")
		if err != nil {
			log.Printf("Scan: Failed to get stats for bucket %s: %v", bucket, err)
			continue
		}

		log.Printf("Scan: Bucket %s: %d entries, %d processed, %d dangling, total size %d MiB",
			bucket, len(entries), processed, dangling, totalSize/1024/1024)
	}

	log.Printf("Scan: Scan completed in %v.", time.Since(now))
}

func main() {
	log.SetOutput(os.Stderr)
	flag.Parse()

	if *help {
		usage()
	}

	if *buckets == "" {
		log.Fatal("Bucket list is required (use -buckets flag or BUCKETS environment variable)")
	}
	if *persistDir == "" {
		log.Fatal("Persist directory is required (use -persist-dir flag or PERSIST_DIR environment variable)")
	}

	// Validate SFTP configuration
	if *sftpHost == "" {
		log.Fatal("SFTP hostname is required (use -sftp-host flag or SFTP_HOST environment variable)")
	}
	if *sftpUser == "" {
		log.Fatal("SFTP username is required (use -sftp-user flag or SFTP_USER environment variable)")
	}
	if *sftpPassword == "" {
		log.Fatal("SFTP password is required (use -sftp-password flag or SFTP_PASSWORD environment variable)")
	}
	if *sftpHostKey == "" {
		log.Fatal("SFTP host key fingerprint is required (use -sftp-hostkey flag or SFTP_HOSTKEY environment variable)")
	}

	// Initialize SFTP filesystem client
	log.Printf("Starting S3-to-SFTP bridge server...")
	log.Printf("SFTP: Connecting to %s@%s:%d", *sftpUser, *sftpHost, *sftpPort)
	log.Printf("SFTP: Base path: %s", *sftpBasePath)
	log.Printf("SFTP: Expected host key fingerprint: %s", *sftpHostKey)

	client, err := fs.NewSftpFs(*sftpHost, *sftpUser, *sftpPassword, *sftpPort, *sftpHostKey, *sftpBasePath)
	if err != nil {
		log.Fatalf("Failed to create SFTP client: %v", err)
	}
	defer client.Close()

	// Parse bucket list into map
	bucketMap := make(map[string]interface{})
	for _, bucket := range strings.Split(*buckets, ",") {
		if bucket = strings.TrimSpace(bucket); bucket != "" {
			bucketMap[bucket] = struct{}{}
		}
	}
	log.Printf("Buckets: %v", getMapKeys(bucketMap))

	// Create database cache
	db, err := cache.NewCacheDB(filepath.Join(*persistDir, "metadata3.db"))
	if err != nil {
		log.Fatalf("Failed to initialize database cache: %v", err)
	}

	// Perform sync
	if *scan {
		runScan(client, db, bucketMap)
	}

	if *serve {
		runServe(db, client, bucketMap)
	}
}
