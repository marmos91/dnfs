package memory

import (
	"context"
	"fmt"
	"slices"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// File Read Operations
// ============================================================================

// ReadDir reads directory entries with pagination support.
//
// This implements support for the READDIR NFS procedure (RFC 1813 section 3.3.16).
// It provides efficient directory listing with cookie-based pagination to handle
// directories with many entries.
//
// Cookie Semantics:
//   - 0: Start of directory (returns "." first)
//   - 1: After "." entry (returns ".." next)
//   - 2: After ".." entry (returns regular entries)
//   - 3+: After each regular entry (one cookie per entry)
//
// The cookie values are opaque to the client and must be treated as continuation
// tokens. The server guarantees that using a returned cookie will resume the
// listing at the next entry.
//
// Pagination:
// The count parameter is used as a hint to limit response size. The server
// estimates the XDR-encoded size of each entry and stops when adding another
// entry would exceed the count. This prevents:
//   - Client buffer overflows
//   - Excessive network transmission times
//   - Server memory exhaustion
//
// Stable Ordering:
// Entries are returned in a stable, sorted order (alphabetical by name) to
// ensure consistent pagination. Without stable ordering, entries could be
// skipped or duplicated if the directory changes between requests.
//
// RFC 1813 Requirements:
//   - Check read and execute permission on the directory
//   - Include "." and ".." entries
//   - Provide stable ordering for pagination
//   - Return EOF flag when all entries have been sent
//
// Parameters:
//   - ctx: Authentication context for access control
//   - dirHandle: Directory to read
//   - cookie: Starting position (0 = beginning)
//   - count: Maximum response size in bytes (approximate)
//
// Returns:
//   - []DirEntry: List of entries starting from cookie
//   - bool: EOF flag (true if all entries returned)
//   - error: Access denied, context cancelled, or I/O errors
func (r *MemoryRepository) ReadDir(
	ctx *metadata.AuthContext,
	dirHandle metadata.FileHandle,
	cookie uint64,
	count uint32,
) ([]metadata.DirEntry, bool, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, false, fmt.Errorf("context cancelled before reading directory: %w", err)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, false, fmt.Errorf("context cancelled while reading directory: %w", err)
	}

	// ========================================================================
	// Step 1: Verify directory exists and is a directory
	// ========================================================================

	dirKey := handleToKey(dirHandle)
	dirAttr, exists := r.files[dirKey]
	if !exists {
		return nil, false, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "directory not found",
		}
	}

	// Verify it's actually a directory
	if dirAttr.Type != metadata.FileTypeDirectory {
		return nil, false, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "not a directory",
		}
	}

	// ========================================================================
	// Step 2: Check read/execute permission on directory
	// ========================================================================
	// Execute (search) permission is required to read directory contents
	// Read permission is required to list the directory

	if !hasReadPermission(ctx, dirAttr) || !hasExecutePermission(ctx, dirAttr) {
		return nil, false, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "read/execute permission denied on directory",
		}
	}

	// ========================================================================
	// Step 3: Build entries list with pagination
	// ========================================================================

	entries := make([]metadata.DirEntry, 0)
	currentCookie := uint64(1)

	// Track estimated size incrementally as we add entries
	// XDR encoding overhead per entry:
	//   4 bytes (value_follows) + 8 bytes (fileid) +
	//   4 bytes (name length) + name bytes + padding (0-3 bytes) + 8 bytes (cookie)
	//   = 24 bytes + name length + padding
	estimatedSize := uint32(0)

	// Reserve space for response overhead (status, attrs, verifier, eof, end marker)
	const responseOverhead = 200
	estimatedSize += responseOverhead

	// Extract directory file ID for "." entry
	dirFileid := extractFileIDFromHandle(dirHandle)

	// ========================================================================
	// Add "." entry (cookie 1)
	// ========================================================================

	if cookie == 0 {
		entry := metadata.DirEntry{
			Fileid: dirFileid,
			Name:   ".",
			Cookie: currentCookie,
		}

		// Calculate size for this entry
		nameLen := len(entry.Name)
		padding := (4 - (nameLen % 4)) % 4
		entrySize := 24 + uint32(nameLen) + uint32(padding)

		entries = append(entries, entry)
		estimatedSize += entrySize
	}
	currentCookie++

	// ========================================================================
	// Add ".." entry (cookie 2)
	// ========================================================================

	if cookie <= 1 {
		// Get parent file ID
		parentFileid := dirFileid // Default to self if no parent
		if parentHandle, err := r.GetParent(context.Background(), dirHandle); err == nil {
			parentFileid = extractFileIDFromHandle(parentHandle)
		}

		entry := metadata.DirEntry{
			Fileid: parentFileid,
			Name:   "..",
			Cookie: currentCookie,
		}

		// Calculate size for this entry
		nameLen := len(entry.Name)
		padding := (4 - (nameLen % 4)) % 4
		entrySize := 24 + uint32(nameLen) + uint32(padding)

		entries = append(entries, entry)
		estimatedSize += entrySize
	}
	currentCookie++

	// ========================================================================
	// Add regular entries (cookies 3+)
	// ========================================================================

	// Get all children
	children := r.children[dirKey]
	if children != nil {
		// We need a stable ordering for pagination to work correctly
		// Sort names alphabetically for consistent iteration
		names := make([]string, 0, len(children))
		for name := range children {
			names = append(names, name)
		}

		// Sort for stable ordering (O(n log n) with optimized quicksort)
		slices.Sort(names)

		// Iterate through children, skipping entries before cookie
		for _, name := range names {
			// Check context periodically during iteration
			if err := ctx.Context.Err(); err != nil {
				return nil, false, fmt.Errorf("context cancelled during directory read: %w", err)
			}

			handle := children[name]

			// Skip entries before the requested cookie
			if currentCookie <= cookie {
				currentCookie++
				continue
			}

			// Calculate size for this entry BEFORE adding it
			nameLen := len(name)
			padding := (4 - (nameLen % 4)) % 4
			entrySize := 24 + uint32(nameLen) + uint32(padding)

			// Check if adding this entry would exceed the count limit
			if estimatedSize+entrySize > count {
				// We've reached the count limit, but haven't seen all entries
				// Return what we have so far (EOF = false)
				logger.Debug("ReadDir: pagination limit reached at entry '%s' (cookie=%d, estimated=%d, count=%d)",
					name, currentCookie, estimatedSize+entrySize, count)
				return entries, false, nil
			}

			// Extract file ID from handle
			fileid := extractFileIDFromHandle(handle)

			entry := metadata.DirEntry{
				Fileid: fileid,
				Name:   name,
				Cookie: currentCookie,
			}

			// Add entry and increment size (O(1) operation)
			entries = append(entries, entry)
			estimatedSize += entrySize

			currentCookie++
		}
	}

	// ========================================================================
	// Step 4: Return results with EOF flag
	// ========================================================================

	logger.Debug("ReadDir: completed listing directory %x (entries=%d, eof=true)", dirHandle, len(entries))

	// We've returned all entries - EOF = true
	return entries, true, nil
}

// ReadSymlink reads the target path of a symbolic link with access control.
//
// This implements support for the READLINK NFS procedure (RFC 1813 section 3.3.5).
// Symbolic links are special files that contain a path string pointing to another
// file or directory. Reading a symlink returns this path without following it.
//
// Security:
// This method checks read permission on the symlink itself. Note that:
//   - Reading a symlink requires read permission on the symlink, not the target
//   - Symlinks themselves have permissions, though they're often ignored
//   - Following the symlink (accessing the target) requires separate permissions
//
// RFC 1813 Requirements:
//   - Verify the handle refers to a symbolic link
//   - Check read permission on the symlink
//   - Return the target path string
//   - Return symlink attributes for client cache consistency
//
// Parameters:
//   - ctx: Authentication context for access control
//   - handle: File handle of the symbolic link
//
// Returns:
//   - string: The symlink target path
//   - *metadata.FileAttr: Symlink attributes (for cache consistency)
//   - error: Returns error if:
//   - Context is cancelled
//   - Handle not found
//   - Handle is not a symlink
//   - Access denied (no read permission)
//   - Target path is missing or empty
//   - I/O error
func (r *MemoryRepository) ReadSymlink(
	ctx *metadata.AuthContext,
	handle metadata.FileHandle,
) (string, *metadata.FileAttr, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return "", nil, fmt.Errorf("context cancelled before reading symlink: %w", err)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return "", nil, fmt.Errorf("context cancelled while reading symlink: %w", err)
	}

	// ========================================================================
	// Step 1: Get file attributes
	// ========================================================================

	key := handleToKey(handle)
	attr, exists := r.files[key]
	if !exists {
		return "", nil, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "symbolic link not found",
		}
	}

	// ========================================================================
	// Step 2: Verify it's a symbolic link
	// ========================================================================

	if attr.Type != metadata.FileTypeSymlink {
		return "", nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "not a symbolic link",
		}
	}

	// ========================================================================
	// Step 3: Check read permission
	// ========================================================================

	if !hasReadPermission(ctx, attr) {
		return "", nil, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "read permission denied on symbolic link",
		}
	}

	// ========================================================================
	// Step 4: Get symlink target
	// ========================================================================

	if attr.SymlinkTarget == "" {
		return "", nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "symbolic link has no target",
		}
	}

	logger.Debug("ReadSymlink: read symlink %x -> '%s'", handle, attr.SymlinkTarget)

	return attr.SymlinkTarget, attr, nil
}
