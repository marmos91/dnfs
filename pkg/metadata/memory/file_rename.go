package memory

import (
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// File Rename Operations
// ============================================================================

// RenameFile renames or moves a file from one directory to another.
//
// This implements support for the RENAME NFS procedure (RFC 1813 section 3.3.14).
// RENAME is used to change a file's name within the same directory, move a file
// to a different directory, or atomically replace an existing file.
//
// Atomicity:
// The implementation strives for atomicity by:
//  1. Validating all preconditions before making any changes
//  2. Performing the minimal set of operations to complete the rename
//  3. Rolling back on failure (though true transactional rollback is not implemented)
//
// In a production implementation, you would use proper transaction semantics
// or a write-ahead log to ensure true atomicity.
//
// Replacement Semantics:
// When the destination name already exists:
//   - File over file: Allowed (atomic replacement)
//   - Directory over empty directory: Allowed
//   - Directory over non-empty directory: Not allowed (NFS3ErrNotEmpty)
//   - File over directory: Not allowed (NFS3ErrExist)
//   - Directory over file: Not allowed (NFS3ErrExist)
//
// RFC 1813 Requirements:
//   - Check write permission on source directory (to remove entry)
//   - Check write permission on destination directory (to add entry)
//   - Verify source file/directory exists
//   - Handle atomic replacement of destination if allowed
//   - Ensure destination is not a non-empty directory
//   - Update parent relationships for cross-directory moves
//   - Update directory timestamps (mtime, ctime) for both directories
//   - Prevent renaming "." or ".." (validated by protocol layer)
//
// Special Cases:
//   - Same directory, same name: Success (no-op)
//   - Same directory, different name: Simple rename
//   - Different directory: Move with potential rename
//   - Over existing file: Replace atomically
//
// Parameters:
//   - ctx: Authentication context for access control
//   - fromDirHandle: Source directory handle
//   - fromName: Current name of the file/directory
//   - toDirHandle: Destination directory handle
//   - toName: New name for the file/directory
//
// Returns error if:
//   - Context is cancelled
//   - Source file/directory not found
//   - Source or destination directory not found
//   - Access denied (no write permission on either directory)
//   - Destination is a non-empty directory
//   - Type mismatch (file vs directory) when replacing
//   - I/O error
func (r *MemoryRepository) RenameFile(
	ctx *metadata.AuthContext,
	fromDirHandle metadata.FileHandle,
	fromName string,
	toDirHandle metadata.FileHandle,
	toName string,
) error {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled before renaming file: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled while renaming file: %w", err)
	}

	// ========================================================================
	// Step 1: Verify source directory exists and is a directory
	// ========================================================================

	fromDirKey := handleToKey(fromDirHandle)
	fromDirAttr, exists := r.files[fromDirKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "source directory not found",
		}
	}

	if fromDirAttr.Type != metadata.FileTypeDirectory {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "source is not a directory",
		}
	}

	// ========================================================================
	// Step 2: Verify destination directory exists and is a directory
	// ========================================================================

	toDirKey := handleToKey(toDirHandle)
	toDirAttr, exists := r.files[toDirKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "destination directory not found",
		}
	}

	if toDirAttr.Type != metadata.FileTypeDirectory {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "destination is not a directory",
		}
	}

	// ========================================================================
	// Step 3: Check write permission on source directory
	// ========================================================================
	// Need write permission to remove the entry from source directory

	if !hasWritePermission(ctx, fromDirAttr) {
		return &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on source directory",
		}
	}

	// ========================================================================
	// Step 4: Check write permission on destination directory
	// ========================================================================
	// Need write permission to add the entry to destination directory

	if !hasWritePermission(ctx, toDirAttr) {
		return &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on destination directory",
		}
	}

	// ========================================================================
	// Step 5: Verify source file/directory exists
	// ========================================================================

	if r.children[fromDirKey] == nil {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("source not found: %s", fromName),
		}
	}

	sourceHandle, exists := r.children[fromDirKey][fromName]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("source not found: %s", fromName),
		}
	}

	// Get source attributes to check type
	sourceKey := handleToKey(sourceHandle)
	sourceAttr, exists := r.files[sourceKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "source handle exists but attributes missing",
		}
	}

	// ========================================================================
	// Step 6: Check if this is a no-op (same directory, same name)
	// ========================================================================

	if fromDirKey == toDirKey && fromName == toName {
		// Rename to same name in same directory - this is a no-op success
		logger.Debug("RenameFile: no-op rename (same location) for '%s'", fromName)
		return nil
	}

	// ========================================================================
	// Step 7: Check if destination already exists
	// ========================================================================

	if r.children[toDirKey] == nil {
		r.children[toDirKey] = make(map[string]metadata.FileHandle)
	}

	destHandle, destExists := r.children[toDirKey][toName]

	if destExists {
		// Destination exists - need to handle replacement

		// Get destination attributes to check type
		destKey := handleToKey(destHandle)
		destAttr, exists := r.files[destKey]
		if !exists {
			return &metadata.ExportError{
				Code:    metadata.ExportErrServerFault,
				Message: "destination handle exists but attributes missing",
			}
		}

		// ====================================================================
		// Step 7a: Validate replacement is allowed
		// ====================================================================

		// Cannot rename file over directory or directory over file
		if sourceAttr.Type == metadata.FileTypeDirectory && destAttr.Type != metadata.FileTypeDirectory {
			return &metadata.ExportError{
				Code:    metadata.ExportErrServerFault,
				Message: "cannot rename directory over file",
			}
		}

		if sourceAttr.Type != metadata.FileTypeDirectory && destAttr.Type == metadata.FileTypeDirectory {
			return &metadata.ExportError{
				Code:    metadata.ExportErrServerFault,
				Message: "cannot rename file over directory",
			}
		}

		// If renaming directory over directory, destination must be empty
		if sourceAttr.Type == metadata.FileTypeDirectory && destAttr.Type == metadata.FileTypeDirectory {
			destChildren := r.children[destKey]
			if len(destChildren) > 0 {
				return &metadata.ExportError{
					Code:    metadata.ExportErrServerFault,
					Message: fmt.Sprintf("destination directory not empty: contains %d entries", len(destChildren)),
				}
			}
		}

		// ====================================================================
		// Step 7b: Remove destination (atomic replacement)
		// ====================================================================

		logger.Debug("RenameFile: replacing existing destination '%s'", toName)

		// Remove destination from parent's children map
		delete(r.children[toDirKey], toName)

		// Delete destination metadata
		delete(r.files, destKey)

		// Delete destination's children map if it's a directory
		if destAttr.Type == metadata.FileTypeDirectory {
			delete(r.children, destKey)
		}

		// Remove parent relationship
		delete(r.parents, destKey)
	}

	// ========================================================================
	// Step 8: Perform the rename
	// ========================================================================

	// Remove source from its current parent
	delete(r.children[fromDirKey], fromName)

	// Add source to destination parent with new name
	r.children[toDirKey][toName] = sourceHandle

	// ========================================================================
	// Step 9: Update parent relationship if moving to different directory
	// ========================================================================

	if fromDirKey != toDirKey {
		r.parents[sourceKey] = toDirHandle
	}

	// ========================================================================
	// Step 10: Update timestamps
	// ========================================================================

	now := time.Now()

	// Update source file/directory change time (metadata changed)
	sourceAttr.Ctime = now
	r.files[sourceKey] = sourceAttr

	// Update source directory modification time (contents changed)
	fromDirAttr.Mtime = now
	fromDirAttr.Ctime = now
	r.files[fromDirKey] = fromDirAttr

	// Update destination directory modification time if different from source
	if fromDirKey != toDirKey {
		toDirAttr.Mtime = now
		toDirAttr.Ctime = now
		r.files[toDirKey] = toDirAttr
	}

	logger.Debug("RenameFile: renamed '%s' -> '%s' (from dir %x to dir %x)",
		fromName, toName, fromDirHandle, toDirHandle)

	return nil
}
