# S3-to-WebDAV Bridge

A simple S3-compatible API server that uses WebDAV as the underlying storage backend.

## ⚠️ Security Notice

**This server is NOT intended for internet exposure. It cannot handle such load.** It implements AWS v2 signature authentication and should be used on trusted networks or with HTTPS enabled. The intent usage is to run it locally to PBS, and use it only with PBS.

## Buy me a Coffee

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/Y8Y8GCP24)

If you found it useful :)

## How It Works

The server connects to the WebDAV server, scans specified bucket directories into a SQLite database for fast lookups, and provides an S3-compatible HTTP API. When you upload/download files through the S3 API, they are stored on/retrieved from the WebDAV server. The database cache is kept in sync automatically.

The initial sync for buckets might take significant amount of time. No data will be served once the buckets are scanned. The database might become out of sync if files are manually created on bucket, in such case the `metadata.db` has to be removed.

This server is designed for use with Proxmox Backup Server and connecting it to Hetzner Storage Box WebDAV, and supports limited amount of features to make it work with PBS.

## Configuration

**Bucket Filtering**: You must specify which WebDAV directories to expose as S3 buckets. Only the specified buckets will be synced and accessible via the S3 API.

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
TLS_CERT="cert.pem"           # Custom TLS certificate
TLS_KEY="key.pem"             # Custom TLS private key
PERSIST_DIR="./data"          # Directory for persistent data (certificates and S3 keys)
```

### Authentication

- **Secure Mode (default)**: S3 keys are auto-generated and stored in `PERSIST_DIR`, or use provided `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Requests must include proper AWS signature authentication (supports both v2 and v4 signatures).
- **Insecure Mode**: Set `AWS_ACCESS_INSECURE=true` to disable authentication entirely (not recommended).

**Signature Support**: The server supports both AWS Signature Version 2 and Version 4 authentication:
- **AWS v2**: Traditional `Authorization: AWS AccessKey:Signature` headers and presigned URLs
- **AWS v4**: Modern `Authorization: AWS4-HMAC-SHA256 ...` headers and presigned URLs with `X-Amz-*` parameters

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

## Proxmox Backup Server Integration

### Deploy with Hetzner Storage Box

This uses the [Proxmox Backup Server in a Container](https://github.com/ayufan/pve-backup-server-dockerfiles).

**Docker Compose:**

```yaml
services:
  pbs:
    image: ayufan/proxmox-ve:latest
    ports:
      - 8007:8007
    mem_limit: 2G
    volumes:
      - ./pbs-etc:/etc/proxmox-backup
      - ./pbs-log:/var/log/proxmox-backup
      - ./pbs-lib:/var/lib/proxmox-backup
      - ./pbs-backups:/backups
    tmpfs:
      - /run
    restart: unless-stopped
    stop_signal: SIGHUP

  hetzner-s3:
    build: https://github.com/ayufan-research/s3-to-webdav.git
    restart: unless-stopped
    volumes:
      - ./hetzner-s3:/data
    environment:
      WEBDAV_URL: "https://your-username.your-server.de"
      WEBDAV_USER: "your-username"
      WEBDAV_PASSWORD: "your-password"
      BUCKETS: "pbs-backups"
```

### Configure Proxmox Backup Server

In PBS web interface: **Configuration → S3 Endpoints → Add**

- **S3 Endpoint ID**: `hetzner-s3`
- **Port**: `8080`
- **Region**: not relevant
- **Access Key ID**: *[from container logs]*
- **Secret Access Key**: *[from container logs]*
- **Fingerprint**: *[from container logs]*
- **⚠️ Path Style**: ✅ **Must be enabled**

Then add datastore: : **Datastore → Add Datastore**.

### Get S3 Credentials and fingerprints

```bash
docker-compose logs hetzner-s3
```

## License

Good for personal use.

## Author

Kamil Trzciński, 2025, with the help of Claude Code.
