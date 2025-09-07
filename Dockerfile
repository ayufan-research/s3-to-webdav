FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN go build
RUN go build -o s3-to-sftp ./cmd/s3-to-sftp

FROM alpine:latest
COPY --from=builder /app/s3-to-webdav /
COPY --from=builder /app/s3-to-sftp /

EXPOSE 8080

# Persist data
VOLUME ["/data"]
ENV PERSIST_DIR="/data"

ENTRYPOINT ["/s3-to-webdav"]
