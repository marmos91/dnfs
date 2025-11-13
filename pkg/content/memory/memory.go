package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/marmos91/dittofs/pkg/content"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// MemoryContentStore implements ContentStore using in-memory storage.
//
// This implementation stores all content in memory using a map. It's designed for:
//   - Testing and development
//   - Small-scale deployments
//   - Temporary/ephemeral storage
//   - Performance-critical scenarios with small data
//
// Characteristics:
//   - Fast: All operations are memory-speed
//   - Volatile: Data lost on restart
//   - Memory-bound: Limited by available RAM
//   - Thread-safe: Protected by RWMutex
//   - Full-featured: Implements all optional interfaces
//
// Implemented Interfaces:
//   - ContentStore (base read operations)
//   - WritableContentStore (write operations)
//   - SeekableContentStore (seekable reads)
//   - GarbageCollectableStore (cleanup operations)
//
// Thread Safety:
// All operations are protected by a sync.RWMutex. Multiple concurrent readers
// are allowed, but writes are exclusive. Copying data on read/write prevents
// data races with caller-owned buffers.
//
// Memory Management:
// Content is stored as byte slices. Large content may cause memory pressure.
// Consider using filesystem or S3 storage for large files.
type MemoryContentStore struct {
	// data stores the actual file content keyed by ContentID
	data map[metadata.ContentID][]byte

	// mu protects concurrent access to data map
	mu sync.RWMutex
}

// NewMemoryContentStore creates a new in-memory content store.
//
// The store starts empty. All data is stored in memory and will be lost
// when the store is garbage collected or the process exits.
//
// Parameters:
//   - ctx: Context for cancellation (checked before initialization)
//
// Returns:
//   - *MemoryContentStore: Initialized store
//   - error: Only returns error if context is cancelled
func NewMemoryContentStore(ctx context.Context) (*MemoryContentStore, error) {
	// ========================================================================
	// Step 1: Check context before initialization
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Initialize the store
	// ========================================================================

	return &MemoryContentStore{
		data: make(map[metadata.ContentID][]byte),
	}, nil
}

// ============================================================================
// ContentStore Interface Implementation
// ============================================================================

// ReadContent returns a reader for the content identified by the given ID.
//
// The returned reader reads from a copy of the content, so modifications
// to the store after this call won't affect the reader.
//
// Context Cancellation:
// Only checked before acquiring the lock. Once the reader is returned,
// it's independent of the context.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier to read
//
// Returns:
//   - io.ReadCloser: Reader for the content (closing is a no-op)
//   - error: ErrContentNotFound if content doesn't exist, or context errors
func (s *MemoryContentStore) ReadContent(ctx context.Context, id metadata.ContentID) (io.ReadCloser, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and get content
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if content exists
	data, exists := s.data[id]
	if !exists {
		return nil, fmt.Errorf("content %s: %w", id, content.ErrContentNotFound)
	}

	// ========================================================================
	// Step 3: Return a reader over a copy of the data
	// ========================================================================
	// This prevents data races if the content is later modified

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return io.NopCloser(bytes.NewReader(dataCopy)), nil
}

// GetContentSize returns the size of the content in bytes.
//
// This is a lightweight operation that just returns the length of the
// stored byte slice.
//
// Context Cancellation:
// Only checked before acquiring the lock.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier
//
// Returns:
//   - uint64: Size of the content in bytes
//   - error: ErrContentNotFound if content doesn't exist, or context errors
func (s *MemoryContentStore) GetContentSize(ctx context.Context, id metadata.ContentID) (uint64, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and get size
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if content exists
	data, exists := s.data[id]
	if !exists {
		return 0, fmt.Errorf("content %s: %w", id, content.ErrContentNotFound)
	}

	return uint64(len(data)), nil
}

// ContentExists checks if content with the given ID exists.
//
// This is a lightweight existence check that just checks the map.
//
// Context Cancellation:
// Only checked before acquiring the lock.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier to check
//
// Returns:
//   - bool: True if content exists, false otherwise
//   - error: Only returns error for context cancellation
func (s *MemoryContentStore) ContentExists(ctx context.Context, id metadata.ContentID) (bool, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return false, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and check existence
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[id]
	return exists, nil
}

// GetStorageStats returns statistics about the in-memory storage.
//
// Statistics are calculated on-the-fly from the current state:
//   - TotalSize: Unlimited (^uint64(0))
//   - UsedSize: Sum of all content sizes
//   - AvailableSize: Unlimited (^uint64(0))
//   - ContentCount: Number of content items
//   - AverageSize: UsedSize / ContentCount (0 if empty)
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//
// Returns:
//   - *StorageStats: Current storage statistics
//   - error: Only returns error for context cancellation
func (s *MemoryContentStore) GetStorageStats(ctx context.Context) (*content.StorageStats, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and calculate statistics
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Calculate current statistics
	usedSize := uint64(0)
	for _, data := range s.data {
		usedSize += uint64(len(data))
	}

	contentCount := uint64(len(s.data))

	averageSize := uint64(0)
	if contentCount > 0 {
		averageSize = usedSize / contentCount
	}

	return &content.StorageStats{
		TotalSize:     ^uint64(0), // Unlimited
		UsedSize:      usedSize,
		AvailableSize: ^uint64(0), // Unlimited
		ContentCount:  contentCount,
		AverageSize:   averageSize,
	}, nil
}

// ============================================================================
// WritableContentStore Interface Implementation
// ============================================================================

// WriteAt writes data at the specified offset.
//
// This implements sparse file semantics:
//   - If content doesn't exist: create with zeros up to offset, then data
//   - If offset > current size: extend with zeros, then write data
//   - If offset < current size: overwrite existing data
//
// Context Cancellation:
// Checked before acquiring the lock. The write itself is atomic.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier (created if doesn't exist)
//   - data: Data to write
//   - offset: Byte offset where writing begins
//
// Returns:
//   - error: Returns error if offset is negative or context is cancelled
func (s *MemoryContentStore) WriteAt(ctx context.Context, id metadata.ContentID, data []byte, offset int64) error {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return err
	}

	// ========================================================================
	// Step 2: Validate offset
	// ========================================================================

	if offset < 0 {
		return fmt.Errorf("offset %d: %w", offset, content.ErrInvalidOffset)
	}

	// ========================================================================
	// Step 3: Acquire write lock and perform write
	// ========================================================================

	s.mu.Lock()
	defer s.mu.Unlock()

	// Get existing content or create new
	existing, exists := s.data[id]

	// Calculate new size needed
	newSize := int(offset) + len(data)

	// Create new buffer
	var result []byte
	if exists && len(existing) > newSize {
		// Existing content is larger, preserve it
		result = make([]byte, len(existing))
		copy(result, existing)
	} else {
		// Need to expand
		result = make([]byte, newSize)
		if exists {
			// Copy existing data
			copy(result, existing)
		}
		// Gap between old size and offset is already zeros (from make())
	}

	// Write new data at offset
	copy(result[offset:], data)

	// Store updated content
	s.data[id] = result

	return nil
}

// Truncate changes the size of the content.
//
// Truncate Semantics:
//   - If newSize < currentSize: Content is truncated (trailing data removed)
//   - If newSize > currentSize: Content is extended with zeros
//   - If newSize == currentSize: No-op (succeeds immediately)
//
// Context Cancellation:
// Checked before acquiring the lock. The truncate itself is atomic.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier
//   - newSize: New size in bytes
//
// Returns:
//   - error: ErrContentNotFound if content doesn't exist, or context errors
func (s *MemoryContentStore) Truncate(ctx context.Context, id metadata.ContentID, newSize uint64) error {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return err
	}

	// ========================================================================
	// Step 2: Acquire write lock and perform truncate
	// ========================================================================

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if content exists
	existing, exists := s.data[id]
	if !exists {
		return fmt.Errorf("truncate failed for %s: %w", id, content.ErrContentNotFound)
	}

	currentSize := uint64(len(existing))

	// No-op if size is the same
	if newSize == currentSize {
		return nil
	}

	// Create new buffer with new size
	newData := make([]byte, newSize)

	// Copy existing data (up to newSize)
	if newSize < currentSize {
		// Truncating: copy only newSize bytes
		copy(newData, existing[:newSize])
	} else {
		// Extending: copy all existing data, rest is zeros
		copy(newData, existing)
	}

	s.data[id] = newData

	return nil
}

// Delete removes content from the store.
//
// The operation is idempotent - deleting non-existent content returns nil.
// Memory is reclaimed immediately by the Go garbage collector.
//
// Context Cancellation:
// Checked before acquiring the lock. The delete itself is atomic.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier to delete
//
// Returns:
//   - error: Only returns error for context cancellation
func (s *MemoryContentStore) Delete(ctx context.Context, id metadata.ContentID) error {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return err
	}

	// ========================================================================
	// Step 2: Acquire write lock and delete
	// ========================================================================

	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete is idempotent - no error if content doesn't exist
	delete(s.data, id)

	return nil
}

// WriteContent writes the entire content in one operation.
//
// This is a convenience method that replaces any existing content with
// the new data.
//
// Context Cancellation:
// Checked before acquiring the lock. The write itself is atomic.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier (created if doesn't exist, replaced if exists)
//   - data: Complete content data
//
// Returns:
//   - error: Returns error if context is cancelled
func (s *MemoryContentStore) WriteContent(ctx context.Context, id metadata.ContentID, data []byte) error {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return err
	}

	// ========================================================================
	// Step 2: Acquire write lock and write content
	// ========================================================================

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a copy to prevent external modifications
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	s.data[id] = dataCopy

	return nil
}

// ============================================================================
// SeekableContentStore Interface Implementation
// ============================================================================

// ReadContentSeekable returns a seekable reader for the content.
//
// The in-memory implementation supports efficient seeking. The reader
// operates on a copy of the content to prevent data races.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - id: Content identifier to read
//
// Returns:
//   - io.ReadSeekCloser: Seekable reader (closing is a no-op)
//   - error: ErrContentNotFound if content doesn't exist, or context errors
func (s *MemoryContentStore) ReadContentSeekable(ctx context.Context, id metadata.ContentID) (io.ReadSeekCloser, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and get content
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if content exists
	data, exists := s.data[id]
	if !exists {
		return nil, fmt.Errorf("content %s: %w", id, content.ErrContentNotFound)
	}

	// ========================================================================
	// Step 3: Return a seekable reader over a copy of the data
	// ========================================================================

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &seekableReader{
		Reader: bytes.NewReader(dataCopy),
	}, nil
}

// seekableReader wraps bytes.Reader to add a Close method.
// bytes.Reader implements Read and Seek, we just add Close.
type seekableReader struct {
	*bytes.Reader
}

// Close implements io.Closer. This is a no-op for in-memory readers.
func (r *seekableReader) Close() error {
	return nil
}

// ============================================================================
// GarbageCollectableStore Interface Implementation
// ============================================================================

// ListAllContent returns all content IDs stored in memory.
//
// This returns a snapshot of all ContentIDs at the time of the call.
// The list may become stale if content is added/removed concurrently.
//
// Context Cancellation:
// Checked before and during iteration. For large stores, we check
// periodically during iteration.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//
// Returns:
//   - []metadata.ContentID: List of all content IDs
//   - error: Returns error for context cancellation
func (s *MemoryContentStore) ListAllContent(ctx context.Context) ([]metadata.ContentID, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Acquire read lock and build list
	// ========================================================================

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Pre-allocate slice with exact capacity
	contentIDs := make([]metadata.ContentID, 0, len(s.data))

	i := 0
	for id := range s.data {
		// Check context periodically (every 100 items)
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		contentIDs = append(contentIDs, id)
		i++
	}

	return contentIDs, nil
}

// DeleteBatch removes multiple content items in one operation.
//
// For in-memory storage, this performs deletions atomically under a
// single write lock. The operation is best-effort - partial failures
// are allowed and returned in the map.
//
// Context Cancellation:
// Checked before acquiring lock and periodically during iteration.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - ids: Content identifiers to delete
//
// Returns:
//   - map[metadata.ContentID]error: Map of failed deletions (empty = all succeeded)
//   - error: Only returns error for context cancellation
func (s *MemoryContentStore) DeleteBatch(ctx context.Context, ids []metadata.ContentID) (map[metadata.ContentID]error, error) {
	// ========================================================================
	// Step 1: Check context before acquiring lock
	// ========================================================================

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// ========================================================================
	// Step 2: Acquire write lock and delete batch
	// ========================================================================

	s.mu.Lock()
	defer s.mu.Unlock()

	failures := make(map[metadata.ContentID]error)

	for i, id := range ids {
		// Check context periodically (every 100 deletions)
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				// Context cancelled - mark remaining as failed
				for j := i; j < len(ids); j++ {
					failures[ids[j]] = ctx.Err()
				}
				return failures, ctx.Err()
			}
		}

		// Delete is idempotent, so no error checking needed
		delete(s.data, id)
	}

	return failures, nil
}
