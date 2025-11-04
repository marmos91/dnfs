# DNFS - Distributed NFS Server

A lightweight, Go-based NFS version 3 server implementation that provides an NFS-compatible gateway for distributed storage systems.

## Overview

DNFS implements the NFSv3 protocol (RFC 1813) and Mount protocol to provide a standard NFS interface. It's designed to be modular and extensible, allowing custom backends for metadata and data storage.

## Features

- **NFSv3 Protocol Support**: Implements core NFSv3 operations
- **Mount Protocol**: Full mount/unmount capability
- **Pluggable Storage**: Abstract repository interface for custom backends
- **In-Memory Repository**: Built-in memory-based storage for testing
- **Structured Logging**: Configurable log levels (DEBUG, INFO, WARN, ERROR)

## Architecture

```
dnfs/
├── cmd/dnfs/           # Main application entry point
├── internal/
│   ├── logger/         # Structured logging
│   ├── metadata/       # File metadata and repository interface
│   │   └── persistence/ # Repository implementations
│   ├── protocol/       # Protocol implementations
│   │   ├── rpc/        # RPC layer (RFC 5531)
│   │   ├── mount/      # Mount protocol (RFC 1813 Appendix I)
│   │   └── nfs/        # NFS protocol (RFC 1813)
│   └── server/         # NFS server core
```

## Quick Start

### Build

```bash
go build -o dnfs cmd/dnfs/main.go
```

### Run

```bash
# Default port 2049
./dnfs

# Custom port with debug logging
./dnfs -port 2050 -log-level debug
```

### Mount (from client)

```bash
# Linux
sudo mount -t nfs -o nfsvers=3,tcp localhost:/export /mnt/nfs

# macOS
sudo mount -t nfs -o nfsvers=3,tcp,resvport localhost:/export /mnt/nfs
```

## Configuration

Environment variables:

- `LOG_LEVEL`: Set logging level (DEBUG, INFO, WARN, ERROR)

Command-line flags:

- `-port`: Server port (default: 2049)
- `-log-level`: Logging level (default: INFO)

## Protocol Implementation Status

### Mount Protocol

- [x] NULL - Connectivity test
- [x] MNT - Mount export
- [ ] DUMP - List mounts
- [ ] UMNT - Unmount
- [ ] UMNTALL - Unmount all
- [ ] EXPORT - List exports

### NFS Protocol

- [x] NULL - Connectivity test
- [x] GETATTR - Get file attributes
- [ ] SETATTR - Set file attributes
- [ ] LOOKUP - Lookup file
- [ ] ACCESS - Check access
- [ ] READ - Read file data
- [ ] WRITE - Write file data
- [ ] CREATE - Create file
- [ ] MKDIR - Create directory
- [ ] REMOVE - Delete file
- [ ] RMDIR - Delete directory
- [x] FSSTAT - Filesystem statistics
- [x] FSINFO - Filesystem info
- [x] PATHCONF - POSIX info

## Development

### Adding a New Procedure

1. Define constants in `internal/protocol/nfs/constants.go` or `internal/protocol/mount/constants.go`
2. Create request/response types and handlers in the appropriate protocol package
3. Add the procedure to the handler interface in `internal/server/handler.go`
4. Implement the routing in `internal/server/conn.go`

### Custom Repository

Implement the `metadata.Repository` interface to create a custom storage backend:

```go
type Repository interface {
    GetOrCreateRootHandle(handle FileHandle, exportPath string) (FileHandle, error)
    CreateFile(handle FileHandle, attr *FileAttr) error
    GetFile(handle FileHandle) (*FileAttr, error)
    // ... other methods
}
```

## References

- [RFC 1813](https://tools.ietf.org/html/rfc1813) - NFS Version 3 Protocol
- [RFC 5531](https://tools.ietf.org/html/rfc5531) - RPC: Remote Procedure Call Protocol Specification Version 2
NFS-compatible gateway
