package main

import (
	_ "embed"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"s3-to-webdav/internal/access_log"
	"s3-to-webdav/internal/cache"
	"s3-to-webdav/internal/fs"
	"s3-to-webdav/internal/helpers"
	"s3-to-webdav/internal/s3"
	"s3-to-webdav/internal/sync"
)

//go:embed web/index.html
var browserHTML []byte

var (
	// WebDAV configuration
	webdavURL      = flag.String("webdav-url", os.Getenv("WEBDAV_URL"), "WebDAV server URL")
	webdavUser     = flag.String("webdav-user", os.Getenv("WEBDAV_USER"), "WebDAV username")
	webdavPassword = flag.String("webdav-password", os.Getenv("WEBDAV_PASSWORD"), "WebDAV password")
	webdavInsecure = flag.Bool("webdav-insecure", getEnvOrDefault("WEBDAV_INSECURE", "false") == "true", "Allow self-signed certificates for WebDAV")

	// Local filesystem configuration
	localPath = flag.String("local-path", os.Getenv("LOCAL_PATH"), "Local filesystem path (alternative to WebDAV)")

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

	// Debug mode
	browser = flag.Bool("browser", getEnvOrDefault("BROWSER", "false") == "true", "Enable built-in browser")

	// Maintenance commands
	clean  = flag.Bool("clean", false, "Clean empty directories and exit")
	scan   = flag.Bool("scan", true, "Scan on startup")
	rescan = flag.Bool("rescan", false, "Re-scan and exit")
)

func getEnvOrDefault(envKey, defaultValue string) string {
	if value := os.Getenv(envKey); value != "" {
		return value
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
	fmt.Println("S3-to-WebDAV Bridge Server")
	fmt.Println("==========================")
	fmt.Println("A simple S3-compatible API server that uses WebDAV as the underlying storage backend.")
	fmt.Println()
	fmt.Println("Usage:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Environment variables (used as defaults for flags):")
	fmt.Println("  WEBDAV_URL            - WebDAV server URL")
	fmt.Println("  WEBDAV_USER           - WebDAV username")
	fmt.Println("  WEBDAV_PASSWORD       - WebDAV password")
	fmt.Println("  WEBDAV_INSECURE       - Allow self-signed certificates for WebDAV (default: false)")
	fmt.Println("  LOCAL_PATH            - Local filesystem path (alternative to WebDAV)")
	fmt.Println("  AWS_ACCESS_KEY_ID     - S3 access key for authentication (optional)")
	fmt.Println("  AWS_SECRET_ACCESS_KEY - S3 secret key for authentication (optional)")
	fmt.Println("  AWS_ACCESS_INSECURE   - Allow insecure, secret-less access to S3 (default: false)")
	fmt.Println("  HTTP_PORT             - Server port (default: 8080)")
	fmt.Println("  HTTP_ONLY             - Enable HTTP only (no HTTPS) (default: false)")
	fmt.Println("  TLS_CERT              - TLS certificate file path (optional)")
	fmt.Println("  TLS_KEY               - TLS key file path (optional)")
	fmt.Println("  PERSIST_DIR           - Directory for persistent data (certificates and keys) (default: ./data)")
	fmt.Println("  BUCKETS               - Comma-separated list of bucket names to sync (required)")
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

	// Setup S3 API routes with auth
	s3AuthConfig := loadAccessKeys()
	s3Router := mux.NewRouter()
	s3Server.SetupS3Routes(s3Router)
	s3Handler := s3.AuthMiddleware(s3AuthConfig, s3Router)

	// Setup main router
	mainRouter := mux.NewRouter()

	// Add browser endpoint (outside of auth)
	if *browser {
		mainRouter.HandleFunc("/-/browser/{key:.*}", func(w http.ResponseWriter, req *http.Request) {
			// Check if access key is missing and server requires auth
			if s3AuthConfig.AccessKey != "" && req.URL.Query().Get("access_key") == "" {
				// Redirect to add access key parameter
				redirectURL := *req.URL
				query := redirectURL.Query()
				query.Set("access_key", s3AuthConfig.AccessKey)
				redirectURL.RawQuery = query.Encode()

				http.Redirect(w, req, redirectURL.String(), http.StatusTemporaryRedirect)
				return
			}

			w.Header().Set("Content-Type", "text/html")
			if os.Getenv("DEBUG") == "1" {
				http.ServeFile(w, req, "web/index.html")
			} else {
				w.Write(browserHTML)
			}
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
	sync := sync.New(client, db)

	if *rescan {
		// Reset marker files
		for bucket := range bucketMap {
			if err := db.ResetProcessedFlags(bucket); err != nil {
				log.Fatalf("Failed to perform rescan: %v", err)
			}
		}
	}

	for bucket := range bucketMap {
		if err := sync.Sync(bucket); err != nil {
			log.Fatalf("Failed to perform initial sync for bucket %s: %v", bucket, err)
		}
	}

	if *rescan {
		log.Printf("Rescan: Completed rescan for all buckets")
		os.Exit(0)
	}
}

func runClean(client fs.Fs, db cache.Cache, bucketMap map[string]interface{}) {
	sync := sync.New(client, db)

	for bucket := range bucketMap {
		if err := sync.Clean(bucket); err != nil {
			log.Fatalf("Failed to perform clean for bucket %s: %v", bucket, err)
		}
	}

	log.Printf("Clean: Completed cleaning for all buckets")
	os.Exit(0)
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

	// Validate that either WebDAV or local path is configured, but not both
	if *webdavURL != "" && *localPath != "" {
		log.Fatal("Cannot use both WebDAV and local filesystem - choose one")
	}
	if *webdavURL == "" && *localPath == "" {
		log.Fatal("Either WebDAV URL or local path is required")
	}

	// Initialize filesystem client
	var client fs.Fs
	var err error

	if *localPath != "" {
		log.Printf("Starting S3-to-Local bridge server...")
		client, err = fs.NewLocalFs(*localPath)
		if err != nil {
			log.Fatalf("Failed to create local filesystem: %v", err)
		}
	} else {
		if *webdavUser == "" || *webdavPassword == "" {
			log.Fatal("WebDAV username and password are required")
		}
		log.Printf("Starting S3-to-WebDAV bridge server...")
		client, err = fs.NewWebDAVFs(*webdavURL, *webdavUser, *webdavPassword, *webdavInsecure)
		if err != nil {
			log.Fatalf("Failed to create WebDAV client: %v", err)
		}
	}

	// Parse bucket list into map
	bucketMap := make(map[string]interface{})
	for _, bucket := range strings.Split(*buckets, ",") {
		if bucket = strings.TrimSpace(bucket); bucket != "" {
			bucketMap[bucket] = struct{}{}
		}
	}
	log.Printf("Buckets: %v", getMapKeys(bucketMap))

	// Create database cache
	db, err := cache.NewCacheDB(filepath.Join(*persistDir, "metadata2.db"))
	if err != nil {
		log.Fatalf("Failed to initialize database cache: %v", err)
	}

	// Perform sync
	if *scan {
		runScan(client, db, bucketMap)
	}
	if *clean {
		runClean(client, db, bucketMap)
	}

	runServe(db, client, bucketMap)
}
