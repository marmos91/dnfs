package badger

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/marmos91/dittofs/pkg/store/metadata"
	"github.com/marmos91/dittofs/pkg/store/metadata/internal"
)

// File Handle Generation Strategy
// ================================
//
// DittoFS uses path-based file handles to enable filesystem import/export and
// metadata reconstruction from content stores. The handle format is designed to
// be deterministic (same path = same handle) and reversible (can extract path
// from handle).
//
// Handle Format:
//    Format: "shareName:fullPath"
//    Example: "/export:/images/photo.jpg" → []byte("/export:/images/photo.jpg")
//    Size: Variable, typically 20-200 bytes
//    Limit: 256 bytes (aligned with typical OS filename limits)
//
// If a path would exceed 256 bytes, the operation fails with an appropriate error.
// This limit is chosen to align with common filesystem constraints (255-byte
// filename limit on most systems) while staying within reasonable NFS handle sizes.

const (
	// maxNFSHandleSize is the maximum file handle size we support.
	// Increased from RFC 1813's 64-byte recommendation to 256 bytes to accommodate
	// longer paths while staying within typical OS limits (255-char filenames).
	maxNFSHandleSize = 256
)

// generateFileHandle creates a deterministic file handle from a share name and path.
//
// The handle format is simple: "shareName:fullPath"
// If the resulting handle exceeds maxNFSHandleSize (256 bytes), an error is returned.
//
// Thread Safety: Safe for concurrent use (pure function).
//
// Parameters:
//   - shareName: The name of the share (e.g., "/export")
//   - fullPath: The full path within the share (e.g., "/images/photo.jpg")
//
// Returns:
//   - metadata.FileHandle: Deterministic handle for the file
//   - error: Error if the handle would exceed size limit
//
// Example:
//
//	handle, err := generateFileHandle("/export", "/images/photo.jpg")
//	// Returns: []byte("/export:/images/photo.jpg"), nil
func generateFileHandle(shareName, fullPath string) (metadata.FileHandle, error) {
	// Construct the path-based handle format
	pathBased := shareName + ":" + fullPath

	// Check if it fits within our size limit
	if len(pathBased) > maxNFSHandleSize {
		return nil, fmt.Errorf("file handle too long (%d bytes, max %d): path too deep or filename too long",
			len(pathBased), maxNFSHandleSize)
	}

	return metadata.FileHandle([]byte(pathBased)), nil
}

// decodeFileHandle extracts the share name and full path from a file handle.
//
// Handles are always in the format: "shareName:fullPath"
// This function simply splits on the first ":" character.
//
// Thread Safety: Safe for concurrent use (pure function).
//
// Parameters:
//   - handle: The file handle to decode
//
// Returns:
//   - shareName: The share name
//   - fullPath: The full path
//   - error: Error if handle format is invalid
//
// Example:
//
//	share, path, err := decodeFileHandle([]byte("/export:/file.txt"))
//	// Returns: "/export", "/file.txt", nil
func decodeFileHandle(handle metadata.FileHandle) (shareName, fullPath string, err error) {
	if len(handle) == 0 {
		return "", "", fmt.Errorf("empty file handle")
	}

	// Decode path-based handle
	s := string(handle)
	idx := strings.Index(s, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("invalid handle format: missing separator")
	}

	shareName = s[:idx]
	fullPath = s[idx+1:]

	// Validate the components
	if shareName == "" {
		return "", "", fmt.Errorf("invalid handle: empty share name")
	}
	if fullPath == "" {
		return "", "", fmt.Errorf("invalid handle: empty path")
	}

	return shareName, fullPath, nil
}

// handleToKey converts a FileHandle to a string key for database indexing.
//
// This is a helper for using FileHandles as database keys. It converts the
// byte slice to a string using a simple cast.
//
// Thread Safety: Safe for concurrent use.
//
// Parameters:
//   - handle: The file handle to convert
//
// Returns:
//   - string: String representation for database indexing
func handleToKey(handle metadata.FileHandle) string {
	return string(handle)
}

// buildFullPath constructs the full path for a file by walking up the parent chain.
//
// This is used during file creation to determine the complete path from the root
// to the file being created. It's necessary for generating deterministic handles.
//
// Thread Safety: Must be called with appropriate locking on the store.
//
// Parameters:
//   - store: The BadgerDB store (for parent lookups)
//   - parentHandle: The parent directory handle
//   - childName: The name of the child being created
//
// Returns:
//   - string: The full path (e.g., "/images/photo.jpg")
//   - error: Error if parent chain is broken or lookup fails
//
// Example:
//
//	path, err := buildFullPath(store, dirHandle, "photo.jpg")
//	// Returns: "/images/photo.jpg" if dirHandle points to "/images"
func buildFullPath(store pathBuilder, parentHandle metadata.FileHandle, childName string) (string, error) {
	// If parent is empty, this is a root
	if len(parentHandle) == 0 {
		return "/" + childName, nil
	}

	// Try to decode the parent handle to get its path
	_, parentPath, isHash, err := decodeFileHandle(parentHandle)
	if err != nil {
		return "", fmt.Errorf("failed to decode parent handle: %w", err)
	}

	// If parent uses hash-based handle, we need to look it up
	if isHash {
		fullHandleStr, err := store.lookupHashedHandlePath(parentHandle)
		if err != nil {
			return "", fmt.Errorf("failed to lookup hashed parent handle: %w", err)
		}
		// lookupHashedHandlePath returns "shareName:fullPath", extract just the path
		idx := strings.Index(fullHandleStr, ":")
		if idx < 0 {
			return "", fmt.Errorf("invalid hashed handle mapping format: %s", fullHandleStr)
		}
		parentPath = fullHandleStr[idx+1:]
	}

	// Construct the full path
	if parentPath == "/" {
		return "/" + childName, nil
	}

	return parentPath + "/" + childName, nil
}

// buildContentID is a convenience wrapper around internal.BuildContentID.
// See internal.BuildContentID for full documentation.
func buildContentID(shareName, fullPath string) string {
	return internal.BuildContentID(shareName, fullPath)
}

// pathBuilder is an interface for looking up paths from hashed handles.
// This allows buildFullPath to work without circular dependencies.
type pathBuilder interface {
	lookupHashedHandlePath(handle metadata.FileHandle) (string, error)
}

// fileHandleToID converts a FileHandle ([]byte) to a uint64 file ID.
//
// This is a thin wrapper around metadata.HandleToFileID() which provides the
// canonical implementation for converting file handles to inode numbers.
//
// This is used for directory entries where NFS requires a uint64 file ID.
//
// IMPORTANT: All MetadataStore implementations should use metadata.HandleToFileID()
// directly to ensure consistent inode generation across the system.
//
// See: metadata.HandleToFileID() for implementation details
func fileHandleToID(handle metadata.FileHandle) uint64 {
	return metadata.HandleToINode(handle)
}
