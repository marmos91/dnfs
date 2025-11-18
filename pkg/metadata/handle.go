package metadata

import (
	"crypto/sha256"
	"encoding/binary"
)

// ============================================================================
// File Handle Utilities
// ============================================================================

// HandleToFileID converts a FileHandle to a uint64 file ID (inode number).
//
// This is the CANONICAL implementation for converting file handles to inode
// numbers. All MetadataStore implementations and protocol handlers MUST use
// this function to ensure consistent inode numbers across the system.
//
// IMPORTANT FOR CUSTOM MetadataStore IMPLEMENTATIONS:
//
// If you implement your own MetadataStore, you MUST use this function when:
//  1. Populating file IDs for directory entries (ReadDirectory)
//  2. Generating inode numbers for file attributes
//  3. Any other operation that requires converting handles to numeric IDs
//
// Failure to use this function will result in:
//   - Inconsistent inode numbers between operations
//   - Circular directory structures (rm -rf failures)
//   - Client cache coherency issues
//   - find/ls reporting duplicate inodes
//
// Algorithm:
//
// Uses SHA-256 hash of the entire handle, taking the first 8 bytes as a
// uint64 in big-endian format. SHA-256 provides:
//   - Excellent collision resistance (critical for inode uniqueness)
//   - Deterministic output (same handle always produces same inode)
//   - Standard, well-tested algorithm
//   - Fast computation
//
// Parameters:
//   - handle: The file handle to convert (typically a path-based handle)
//
// Returns:
//   - uint64: File ID (inode number), or 0 if handle is empty
//
// Example:
//
//	handle := FileHandle([]byte("/export:/path/to/file.txt"))
//	inode := HandleToFileID(handle)
//	// Use inode in directory entries or file attributes
func HandleToFileID(handle FileHandle) uint64 {
	if len(handle) == 0 {
		return 0
	}

	// Compute SHA-256 hash of entire handle
	hash := sha256.Sum256(handle)

	// Use first 8 bytes as uint64 inode number (big-endian)
	return binary.BigEndian.Uint64(hash[:8])
}
