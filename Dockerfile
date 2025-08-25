FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .
RUN go build

FROM alpine:latest
COPY --from=builder /app/s3-to-webdav /

EXPOSE 8080

# Persist data
VOLUME ["/data"]
ENV DB_PATH="/data/metadata.db" \
  PERSIST_DIR="/data"

ENTRYPOINT ["/s3-to-webdav"]
