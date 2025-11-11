// Package server provides the DittoServer, which manages multiple protocol facades
// (NFS, SMB, WebDAV, etc.) that share common metadata and content repositories.
package server

import (
	"context"

	"github.com/marmos91/dittofs/internal/content"
	"github.com/marmos91/dittofs/internal/metadata"
)

// Facade represents a protocol-specific server that can be started and stopped
// by DittoServer. Each facade implements a specific protocol (NFS, SMB, etc.)
// and shares the same metadata and content repositories with other facades.
type Facade interface {
	// Start initializes and starts the facade server.
	// It receives the shared metadata and content repositories.
	// This method should block until the server is ready or an error occurs.
	Serve(ctx context.Context) error

	SetRepositories(metadata metadata.Repository, content content.Repository)

	// Stop gracefully shuts down the facade server.
	Stop(ctx context.Context) error

	// Protocol returns the protocol name (e.g., "NFS", "SMB", "WebDAV").
	Protocol() string

	// Port returns the port the facade is listening on.
	Port() int
}
