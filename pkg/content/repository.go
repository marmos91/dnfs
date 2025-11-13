package content

import (
	"context"
	"io"

	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// DEPRECATED: This file is deprecated. Use store.go instead.
// ============================================================================
//
// The interfaces in this file are deprecated in favor of the new ContentStore
// interface hierarchy defined in store.go. The new design provides:
//
//   - Better documentation and organization
//   - Standard error handling (errors.go)
//   - Additional capabilities (garbage collection, statistics, multipart)
//   - Consistent naming with MetadataStore
//
// Migration Guide:
//   - Repository → ContentStore
//   - WriteRepository → WritableContentStore
//   - SeekableContentRepository → SeekableContentStore
//
// This file will be removed in a future version.
// ============================================================================

// Repository defines the interface for reading file content
// Deprecated: Use ContentStore instead
type Repository interface {
	// ReadContent returns a reader for the content identified by the given ID
	// Returns an error if the content doesn't exist or can't be read
	ReadContent(ctx context.Context, id metadata.ContentID) (io.ReadCloser, error)

	// GetContentSize returns the size of the content in bytes
	// This is useful for NFS operations that need to know file size without reading
	GetContentSize(ctx context.Context, id metadata.ContentID) (uint64, error)

	// ContentExists checks if content with the given ID exists
	ContentExists(ctx context.Context, id metadata.ContentID) (bool, error)
}

// WriteRepository extends Repository with write operations.
// Deprecated: Use WritableContentStore instead
type WriteRepository interface {
	Repository

	// WriteAt writes data at the specified offset
	// If the file doesn't exist, it will be created
	WriteAt(ctx context.Context, id metadata.ContentID, data []byte, offset int64) error

	// Truncate changes the size of the content to the specified size.
	// If size is less than current size, content is truncated.
	// If size is greater than current size, content is extended (usually with zeros).
	Truncate(ctx context.Context, id metadata.ContentID, size uint64) error
}

// SeekableContentRepository is an optional extended interface
// for repositories that support random access reads
// Deprecated: Use SeekableContentStore instead
type SeekableContentRepository interface {
	Repository

	// ReadContentSeekable returns a seeker for random access
	// This is useful for efficient partial reads
	ReadContentSeekable(ctx context.Context, id metadata.ContentID) (io.ReadSeekCloser, error)
}
