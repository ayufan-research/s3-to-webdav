# S3-to-WebDAV Bridge

A simple S3-compatible API server that uses WebDAV as the underlying storage backend.

## ⚠️ Security Notice

**This server is NOT intended for internet exposure without proper TLS.** It implements AWS v2 signature authentication and should be used on trusted networks or with HTTPS enabled.

## How It Works

The server connects to your WebDAV server, scans specified bucket directories into a SQLite database for fast lookups, and provides an S3-compatible HTTP API. When you upload/download files through the S3 API, they are stored on/retrieved from the WebDAV server. The database cache is kept in sync automatically.

**Bucket Filtering**: You must specify which WebDAV directories to expose as S3 buckets. Only the specified buckets will be synced and accessible via the S3 API.

This server is designed for use with Proxmox Backup Server and connecting it to Hetzner Storage Box WebDAV.

## Configuration

Configure via environment variables or command-line flags:

### Required Settings

```bash
WEBDAV_URL="https://your-webdav-server.com/dav"
WEBDAV_USER="your-username"
WEBDAV_PASSWORD="your-password"
BUCKETS="bucket1,bucket2,bucket3"    # Comma-separated list of bucket names to sync
```

### Optional Settings

```bash
HTTP_PORT="8080"                    # HTTP server port
WEBDAV_INSECURE="false"        # Allow self-signed WebDAV certificates
AWS_ACCESS_KEY_ID="key"        # S3 access key (optional - auto-generated if not provided)
AWS_SECRET_ACCESS_KEY="secret" # S3 secret key (optional - auto-generated if not provided)
AWS_ACCESS_INSECURE="false"    # Allow insecure access without authentication
DB_PATH="metadata.db"          # SQLite database file path
TLS_CERT="cert.pem"           # Custom TLS certificate
TLS_KEY="key.pem"             # Custom TLS private key
PERSIST_DIR="./data"          # Directory for persistent data (certificates and S3 keys)
```

### Authentication

- **Secure Mode (default)**: S3 keys are auto-generated and stored in `PERSIST_DIR`, or use provided `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Requests must include proper AWS v2 signature authentication.
- **Insecure Mode**: Set `AWS_ACCESS_INSECURE=true` to disable authentication entirely (not recommended).

### TLS Options

- **Auto-generated**: Use `PERSIST_DIR` for self-signed certificates (10-year validity) (default)
- **Custom certificates**: Use `TLS_CERT` and `TLS_KEY`
- **HTTP**: Run without TLS. Use `HTTP_ONLY`

### Command Line

```bash
./s3-to-webdav -webdav-url "https://your-server.com/dav" \
               -webdav-user "user" \
               -webdav-password "pass" \
               -buckets "bucket1,bucket2,bucket3"
```

## Usage with S3 Tools

```bash
# With auto-generated credentials (check server logs for keys)
aws configure set aws_access_key_id <generated-access-key>
aws configure set aws_secret_access_key <generated-secret-key>
aws --endpoint-url http://localhost:8080 s3 ls

# With provided credentials
aws configure set aws_access_key_id your-access-key
aws configure set aws_secret_access_key your-secret-key
aws --endpoint-url http://localhost:8080 s3 ls

# Insecure mode (authentication disabled)
AWS_ACCESS_INSECURE=true ./s3-to-webdav [other-flags]
aws configure set aws_access_key_id dummy
aws configure set aws_secret_access_key dummy
aws --endpoint-url http://localhost:8080 s3 ls
```

## Docker

### Basic Usage

```bash
docker run -v /path/to/data:/data \
           -e WEBDAV_URL="https://your-server.com/dav" \
           -e WEBDAV_USER="user" \
           -e WEBDAV_PASSWORD="pass" \
           -e BUCKETS="bucket1,bucket2,bucket3" \
           -p 8080:8080 s3-to-webdav
```

### Docker Compose

Use the included `docker-compose.yml` file:

```bash
# Edit docker-compose.yml with your WebDAV credentials and bucket list
docker-compose up -d

# View logs to see generated credentials
docker-compose logs s3-to-webdav
```

## Reading Generated Credentials & Certificate Info

When the server starts, it displays all important information in the logs:

### Server Startup Logs Example

```
S3: Generated/loaded credentials from ./data
S3: Access Key: a1b2c3d4e5f67890
S3: Secret Key: 1a2b3c4d5e6f7890abcdef1234567890abcdef12
TLS: Certificate: ./data/cert.pem / ./data/key.pem
TLS: Fingerprint: SHA256:1A:2B:3C:4D:5E:6F:78:90:AB:CD:EF:12:34:56:78:90:AB:CD:EF:12:34:56:78:90:AB:CD:EF:12:34:56:78:90
HTTPS: Server ready! Listening on https://:8080
```

### Reading from Docker Logs

```bash
docker logs <container-name>
docker-compose logs s3-to-webdav
```
