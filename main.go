package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"s3-to-webdav/internal"
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

func main() {
	log.SetOutput(os.Stderr)

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
		httpPort = flag.String("http-port", getEnvOrDefault("HTTP_PORT", "8080"), "HTTP server port")
		httpOnly = flag.Bool("http-only", getEnvOrDefault("HTTP_ONLY", "false") == "true", "Enable HTTP only (no HTTPS)")

		// TLS configuration
		tlsCert = flag.String("tls-cert", os.Getenv("TLS_CERT"), "TLS certificate file path")
		tlsKey  = flag.String("tls-key", os.Getenv("TLS_KEY"), "TLS key file path")

		// Persistence configuration
		dbPath     = flag.String("db-path", getEnvOrDefault("DB_PATH", "metadata.db"), "SQLite database path")
		persistDir = flag.String("persist-dir", getEnvOrDefault("PERSIST_DIR", "./data"), "Directory to store persistent data (certificates and keys)")

		// Bucket configuration
		buckets = flag.String("buckets", os.Getenv("BUCKETS"), "Comma-separated list of bucket names to sync (required)")

		// Help
		help = flag.Bool("help", false, "Show help message")
	)

	flag.Parse()

	if *help {
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
		fmt.Println("  DB_PATH               - SQLite database path (default: metadata.db)")
		fmt.Println("  TLS_CERT              - TLS certificate file path (optional)")
		fmt.Println("  TLS_KEY               - TLS key file path (optional)")
		fmt.Println("  PERSIST_DIR           - Directory for persistent data (certificates and keys) (default: ./data)")
		fmt.Println("  BUCKETS               - Comma-separated list of bucket names to sync (required)")
		os.Exit(0)
	}

	if *buckets == "" {
		log.Fatal("Bucket list is required (use -buckets flag or BUCKETS environment variable)")
	}

	// Validate that either WebDAV or local path is configured, but not both
	if *webdavURL != "" && *localPath != "" {
		log.Fatal("Cannot use both WebDAV and local filesystem - choose one")
	}
	if *webdavURL == "" && *localPath == "" {
		log.Fatal("Either WebDAV URL or local path is required")
	}

	// Initialize filesystem client
	var client internal.Fs
	var err error

	if *localPath != "" {
		log.Printf("Starting S3-to-Local bridge server...")
		client, err = internal.NewLocalFs(*localPath)
		if err != nil {
			log.Fatalf("Failed to create local filesystem: %v", err)
		}
	} else {
		if *webdavUser == "" || *webdavPassword == "" {
			log.Fatal("WebDAV username and password are required")
		}
		log.Printf("Starting S3-to-WebDAV bridge server...")
		client, err = internal.NewWebDAVFs(*webdavURL, *webdavUser, *webdavPassword, *webdavInsecure)
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
	db, err := internal.NewDBCache(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database cache: %v", err)
	}

	// Perform initial sync
	sync := internal.NewDBSync(client, db)
	for bucket := range bucketMap {
		if err := sync.Sync(bucket); err != nil {
			log.Fatalf("Failed to perform initial sync for bucket %s: %v", bucket, err)
		}
	}

	// Get or generate S3 credentials
	if *accessInsecure {
		if *accessKey != "" || *secretKey != "" {
			log.Fatalf("Cannot use -aws-access-insecure with provided access or secret keys")
		}
		log.Printf("S3: Authentication disabled")
	} else if *accessKey == "" && *secretKey == "" && *persistDir != "" {
		log.Printf("S3: Generated/loaded credentials from %s", *persistDir)
		*accessKey, err = internal.GetOrCreateRandomSecret(filepath.Join(*persistDir, "access_key"), 20)
		if err != nil {
			log.Fatalf("Failed to get/create S3 access key: %v", err)
		}
		*secretKey, err = internal.GetOrCreateRandomSecret(filepath.Join(*persistDir, "secret_key"), 20)
		if err != nil {
			log.Fatalf("Failed to get/create S3 secret key: %v", err)
		}
		log.Printf("S3: Access Key: %s", *accessKey)
		log.Printf("S3: Secret Key: %s", *secretKey)
	} else if *accessKey != "" && *secretKey != "" {
		log.Printf("S3: Using provided credentials")
		log.Printf("S3: Access Key: %s", *accessKey)
	}

	s3Server := internal.NewS3Server(db, client, *accessKey, *secretKey)
	s3Server.SetBucketMap(bucketMap)

	r := mux.NewRouter()

	// Setup S3 API routes
	s3Server.SetupS3Routes(r)

	// Apply authentication middleware
	var handler http.Handler = r
	handler = internal.S3AuthMiddleware(internal.S3AuthConfig{
		AccessKey: *accessKey,
		SecretKey: *secretKey,
	}, handler)

	// Wrap with access logging middleware
	handler = internal.AccessLogMiddleware(handler)

	if !*httpOnly && *tlsCert == "" && *tlsKey == "" && *persistDir != "" {
		// Get or create certificates
		*tlsCert, *tlsKey, err = internal.GetOrCreateCertificates(*persistDir)
		if err != nil {
			log.Fatalf("Failed to get/create certificates: %v", err)
		}
	}

	// Start server with or without TLS
	if *tlsCert != "" && *tlsKey != "" {
		if *httpOnly {
			log.Fatal("Cannot use TLS with HTTP only mode")
		}
		log.Printf("TLS: Certificate: %s / %s", *tlsCert, *tlsKey)
		if fingerprint, err := internal.GetCertificateFingerprint(*tlsCert); err == nil {
			log.Printf("TLS: Fingerprint: %s", fingerprint)
		}
		log.Printf("HTTPS: Server ready! Listening on https://:%s", *httpPort)
		log.Fatal(http.ListenAndServeTLS(":"+*httpPort, *tlsCert, *tlsKey, handler))
	} else {
		log.Printf("HTTP: Server ready! Listening on http://:%s", *httpPort)
		log.Fatal(http.ListenAndServe(":"+*httpPort, handler))
	}
}
