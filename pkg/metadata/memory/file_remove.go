package memory

import (
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// File Removal Operations
// ============================================================================

// RemoveFile removes a file (not a directory) from a directory.
//
// This implements support for the REMOVE NFS procedure (RFC 1813 section 3.3.12).
// It performs complete cleanup including:
//   - Permission validation
//   - File deletion
//   - Directory entry removal
//   - Parent timestamp updates
//
// This is distinct from RMDIR which removes directories. Attempting to remove
// a directory with REMOVE will fail - this enforces proper directory handling
// and prevents accidental removal of non-empty directories.
//
// RFC 1813 Requirements:
//   - Check write permission on parent directory
//   - Verify the file exists and is not a directory
//   - Remove the directory entry
//   - Update parent directory timestamps (mtime, ctime)
//   - Return the removed file's attributes for client cache updates
//
// Parameters:
//   - ctx: Authentication context for access control
//   - parentHandle: Handle of the parent directory
//   - filename: Name of the file to remove
//
// Returns:
//   - *metadata.FileAttr: The attributes of the removed file (for response)
//   - error: Returns error if:
//   - Context is cancelled
//   - Access denied (no write permission on parent)
//   - File not found
//   - File is a directory (use RemoveDirectory instead)
//   - Parent is not a directory
//   - I/O error
func (r *MemoryRepository) RemoveFile(
	ctx *metadata.AuthContext,
	parentHandle metadata.FileHandle,
	filename string,
) (*metadata.FileAttr, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before removing file: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled while removing file: %w", err)
	}

	// ========================================================================
	// Step 1: Verify parent directory exists
	// ========================================================================

	parentKey := handleToKey(parentHandle)
	parentAttr, exists := r.files[parentKey]
	if !exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "parent directory not found",
		}
	}

	// Verify parent is a directory
	if parentAttr.Type != metadata.FileTypeDirectory {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "parent is not a directory",
		}
	}

	// ========================================================================
	// Step 2: Check write permission on parent directory
	// ========================================================================

	if !hasWritePermission(ctx, parentAttr) {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on parent directory",
		}
	}

	// ========================================================================
	// Step 3: Verify file exists in directory
	// ========================================================================

	if r.children[parentKey] == nil {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("file not found: %s", filename),
		}
	}

	fileHandle, exists := r.children[parentKey][filename]
	if !exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("file not found: %s", filename),
		}
	}

	// ========================================================================
	// Step 4: Get file attributes and verify it's not a directory
	// ========================================================================

	fileKey := handleToKey(fileHandle)
	fileAttr, exists := r.files[fileKey]
	if !exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "file handle exists but attributes missing",
		}
	}

	// Don't allow removing directories with REMOVE (use RMDIR instead)
	if fileAttr.Type == metadata.FileTypeDirectory {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "cannot remove directory with REMOVE (use RMDIR)",
		}
	}

	// ========================================================================
	// Step 5: Remove file from parent directory
	// ========================================================================

	delete(r.children[parentKey], filename)

	// Remove parent relationship
	delete(r.parents, fileKey)

	// ========================================================================
	// Step 6: Delete file metadata
	// ========================================================================

	delete(r.files, fileKey)

	// ========================================================================
	// Step 7: Update parent directory timestamps
	// ========================================================================

	now := time.Now()
	parentAttr.Mtime = now
	parentAttr.Ctime = now
	r.files[parentKey] = parentAttr

	logger.Debug("RemoveFile: removed file '%s' from parent %x", filename, parentHandle)

	// Return a copy of the file attributes for the response
	// (we make a copy since we just deleted the original)
	removedFileAttr := *fileAttr

	return &removedFileAttr, nil
}

// RemoveDirectory removes an empty directory from a parent directory.
//
// This implements support for the RMDIR NFS procedure (RFC 1813 section 3.3.13).
// Unlike REMOVE which handles files, RMDIR specifically handles directory removal
// with the additional requirement that the directory must be empty.
//
// Empty Directory Check:
// A directory is considered empty if it has no children. The "." and ".." entries
// are virtual and not stored in the children map, so they don't count against
// the empty check.
//
// This separation between REMOVE and RMDIR serves two purposes:
//  1. Prevents accidental removal of non-empty directories
//  2. Provides clear error messages for incorrect operation usage
//
// RFC 1813 Requirements:
//   - Check write permission on parent directory
//   - Verify the target is a directory
//   - Verify the directory is empty
//   - Remove the directory entry from parent
//   - Update parent directory timestamps (mtime, ctime)
//   - Return appropriate errors for non-empty directories
//
// Parameters:
//   - ctx: Authentication context for access control
//   - parentHandle: Handle of the parent directory
//   - name: Name of the directory to remove
//
// Returns error if:
//   - Context is cancelled
//   - Access denied (no write permission on parent)
//   - Directory not found
//   - Target is not a directory
//   - Directory is not empty
//   - Parent is not a directory
//   - I/O error
func (r *MemoryRepository) RemoveDirectory(
	ctx *metadata.AuthContext,
	parentHandle metadata.FileHandle,
	name string,
) error {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled before removing directory: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled while removing directory: %w", err)
	}

	// ========================================================================
	// Step 1: Verify parent directory exists
	// ========================================================================

	parentKey := handleToKey(parentHandle)
	parentAttr, exists := r.files[parentKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "parent directory not found",
		}
	}

	// Verify parent is a directory
	if parentAttr.Type != metadata.FileTypeDirectory {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "parent is not a directory",
		}
	}

	// ========================================================================
	// Step 2: Verify directory exists as a child of parent
	// ========================================================================

	if r.children[parentKey] == nil {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("directory not found: %s", name),
		}
	}

	dirHandle, exists := r.children[parentKey][name]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: fmt.Sprintf("directory not found: %s", name),
		}
	}

	// ========================================================================
	// Step 3: Verify target is actually a directory
	// ========================================================================

	dirKey := handleToKey(dirHandle)
	dirAttr, exists := r.files[dirKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "directory metadata not found",
		}
	}

	if dirAttr.Type != metadata.FileTypeDirectory {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "not a directory",
		}
	}

	// ========================================================================
	// Step 4: Check if directory is empty
	// ========================================================================
	// A directory is empty if it has no children (the "." and ".." entries
	// are virtual and not stored in the children map)

	dirChildren := r.children[dirKey]
	if len(dirChildren) > 0 {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("directory not empty: contains %d entries", len(dirChildren)),
		}
	}

	// ========================================================================
	// Step 5: Check write permission on parent directory
	// ========================================================================

	if !hasWritePermission(ctx, parentAttr) {
		return &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on parent directory",
		}
	}

	// ========================================================================
	// Step 6: Remove directory entry from parent
	// ========================================================================

	delete(r.children[parentKey], name)

	// ========================================================================
	// Step 7: Delete directory metadata and children map
	// ========================================================================

	delete(r.files, dirKey)
	delete(r.children, dirKey)

	// ========================================================================
	// Step 8: Remove parent relationship
	// ========================================================================

	delete(r.parents, dirKey)

	// ========================================================================
	// Step 9: Update parent directory timestamps
	// ========================================================================
	// The parent directory's mtime and ctime should be updated when a child
	// is removed, as this modifies the directory's contents.

	now := time.Now()
	parentAttr.Mtime = now
	parentAttr.Ctime = now
	r.files[parentKey] = parentAttr

	logger.Debug("RemoveDirectory: removed directory '%s' from parent %x", name, parentHandle)

	return nil
}
