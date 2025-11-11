package memory

import (
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/pkg/content"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// File Modification Operations
// ============================================================================

// SetFileAttributes updates file attributes with access control.
//
// This implements support for the SETATTR NFS procedure (RFC 1813 section 3.3.2).
// It handles selective attribute updates based on the Set* flags in the attrs
// parameter, with proper permission checking and validation.
//
// Permission Requirements:
//   - Mode changes: Only owner or root
//   - UID changes: Only root
//   - GID changes: Only root (or owner if in supplementary groups)
//   - Size changes: Write permission required
//   - Time changes: Write permission or owner
//
// The method automatically updates ctime (change time) whenever any attribute
// is modified, as required by RFC 1813.
//
// Size Changes:
// Size modifications require coordination with the content repository:
//   - Truncation (new < old): Remove trailing content
//   - Extension (new > old): Pad with zeros
//   - Zero: Delete all content
//
// Note: In this in-memory implementation, content coordination is not yet
// implemented. A production implementation would delegate to a content
// repository for actual data truncation/extension.
//
// Parameters:
//   - ctx: Authentication context for access control
//   - handle: The file handle to update
//   - attrs: The attributes to set (only Set* = true are modified)
//
// Returns error if:
//   - Context is cancelled
//   - File not found
//   - Access denied (insufficient permissions)
//   - Not owner (for ownership/permission changes)
//   - Invalid attribute values (e.g., negative size)
//   - Attempting to set size on directory or special file
//   - I/O error
func (r *MemoryRepository) SetFileAttributes(
	ctx *metadata.AuthContext,
	handle metadata.FileHandle,
	attrs *metadata.SetAttrs,
) error {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled before setting file attributes: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled while setting file attributes: %w", err)
	}

	// ========================================================================
	// Step 1: Verify file exists and get current attributes
	// ========================================================================

	key := handleToKey(handle)
	fileAttr, exists := r.files[key]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "file not found",
		}
	}

	// Track if any attributes were modified (for ctime update)
	modified := false

	// ========================================================================
	// Step 2: Check permissions and apply attribute updates
	// ========================================================================

	// ------------------------------------------------------------------------
	// Mode (permissions) - only owner or root can change
	// ------------------------------------------------------------------------

	if attrs.SetMode {
		// Check if user is owner or root
		if !isOwnerOrRoot(ctx, fileAttr) {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "only owner or root can change permissions",
			}
		}

		// Validate mode value (only use lower 12 bits)
		if attrs.Mode > 0o7777 {
			return &metadata.ExportError{
				Code:    metadata.ExportErrServerFault,
				Message: fmt.Sprintf("invalid mode value: 0%o (max 0o7777)", attrs.Mode),
			}
		}

		fileAttr.Mode = attrs.Mode
		modified = true

		logger.Debug("SetFileAttributes: mode changed to 0%o for handle %x", attrs.Mode, handle)
	}

	// ------------------------------------------------------------------------
	// UID (owner) - only root can change ownership
	// ------------------------------------------------------------------------

	if attrs.SetUID {
		// Only root can change file ownership
		if ctx == nil || ctx.AuthFlavor == 0 || ctx.UID == nil || *ctx.UID != 0 {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "only root can change file ownership",
			}
		}

		fileAttr.UID = attrs.UID
		modified = true

		logger.Debug("SetFileAttributes: uid changed to %d for handle %x", attrs.UID, handle)
	}

	// ------------------------------------------------------------------------
	// GID (group) - only root or owner (if in target group) can change
	// ------------------------------------------------------------------------

	if attrs.SetGID {
		// Check if user can change group
		if !canChangeGroup(ctx, fileAttr, attrs.GID) {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "only root or owner (if in target group) can change group",
			}
		}

		fileAttr.GID = attrs.GID
		modified = true

		logger.Debug("SetFileAttributes: gid changed to %d for handle %x", attrs.GID, handle)
	}

	// ------------------------------------------------------------------------
	// Size - write permission required, only valid for regular files
	// ------------------------------------------------------------------------

	if attrs.SetSize {
		// Verify this is a regular file
		if fileAttr.Type != metadata.FileTypeRegular {
			return &metadata.ExportError{
				Code:    metadata.ExportErrServerFault,
				Message: fmt.Sprintf("cannot set size on non-regular file (type=%d)", fileAttr.Type),
			}
		}

		// Check write permission
		if !hasWritePermission(ctx, fileAttr) {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "write permission denied for size change",
			}
		}

		// Update size
		// TODO: In a production implementation, coordinate with content repository
		// to actually truncate or extend the file content:
		//   - If attrs.Size < fileAttr.Size: Truncate content
		//   - If attrs.Size > fileAttr.Size: Extend with zeros
		//   - If attrs.Size == 0: Delete all content
		oldSize := fileAttr.Size
		fileAttr.Size = attrs.Size
		modified = true

		logger.Debug("SetFileAttributes: size changed from %d to %d for handle %x", oldSize, attrs.Size, handle)

		// Update mtime when size changes (POSIX semantics)
		fileAttr.Mtime = time.Now()
	}

	// ------------------------------------------------------------------------
	// Atime (access time) - owner or write permission required
	// ------------------------------------------------------------------------

	if attrs.SetAtime {
		// Check if user can set atime (owner or write permission)
		if !isOwnerOrRoot(ctx, fileAttr) && !hasWritePermission(ctx, fileAttr) {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "insufficient permission to set atime",
			}
		}

		fileAttr.Atime = attrs.Atime
		modified = true

		logger.Debug("SetFileAttributes: atime changed to %v for handle %x", attrs.Atime, handle)
	}

	// ------------------------------------------------------------------------
	// Mtime (modification time) - owner or write permission required
	// ------------------------------------------------------------------------

	if attrs.SetMtime {
		// Check if user can set mtime (owner or write permission)
		if !isOwnerOrRoot(ctx, fileAttr) && !hasWritePermission(ctx, fileAttr) {
			return &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "insufficient permission to set mtime",
			}
		}

		fileAttr.Mtime = attrs.Mtime
		modified = true

		logger.Debug("SetFileAttributes: mtime changed to %v for handle %x", attrs.Mtime, handle)
	}

	// ========================================================================
	// Step 3: Update ctime if any attributes were modified
	// ========================================================================
	// Per RFC 1813, ctime (change time) is automatically updated by the
	// server whenever any file metadata changes. Clients cannot set it.

	if modified {
		fileAttr.Ctime = time.Now()
		logger.Debug("SetFileAttributes: ctime updated to %v for handle %x", fileAttr.Ctime, handle)
	}

	// ========================================================================
	// Step 4: Save updated attributes
	// ========================================================================

	r.files[key] = fileAttr

	return nil
}

// WriteFile writes data to a file with proper permission checking and metadata updates.
//
// This implements the business logic for the WRITE NFS procedure (RFC 1813 section 3.3.7).
// It handles the complete write workflow including permission validation, size management,
// and timestamp updates.
//
// Write Operation Flow:
//  1. Verify file exists and is a regular file
//  2. Check write permission on the file
//  3. Capture pre-operation attributes (WCC data)
//  4. Delegate actual data write to content repository
//  5. Update file size if writing beyond EOF
//  6. Update file timestamps (mtime always, ctime if size changed)
//  7. Return updated attributes and WCC data
//
// Permission Check:
// Write permission requires one of:
//   - Owner with write bit (mode & 0200)
//   - Group member with write bit (mode & 0020)
//   - Other with write bit (mode & 0002)
//
// File Size Management:
// When offset + len(data) > current size:
//   - File is extended to new size
//   - Bytes between old EOF and write offset are implicitly zero (sparse)
//   - This is standard Unix behavior for positional writes
//
// Timestamp Updates:
// Per POSIX and NFS semantics:
//   - mtime (modification time): Always updated on successful write
//   - ctime (change time): Updated when metadata changes (e.g., size extension)
//   - atime (access time): Not updated on write
//
// Weak Cache Consistency (WCC):
// The returned WccAttr contains size and timestamps before the operation.
// Clients use this to detect concurrent modifications by comparing with
// their cached attributes.
//
// Parameters:
//   - ctx: Authentication context for permission checking
//   - handle: File handle to write to
//   - newSize: New file size after write operation
//
// Returns:
//   - *FileAttr: Updated file attributes after the write
//   - uint64: Pre-operation size (for WCC)
//   - time.Time: Pre-operation mtime (for WCC)
//   - time.Time: Pre-operation ctime (for WCC)
//   - error: Returns ExportError with specific code if operation fails:
//   - Context is cancelled
//   - ExportErrNotFound: File doesn't exist
//   - ExportErrAccessDenied: No write permission
//   - ExportErrServerFault: Not a regular file or other validation errors
//
// Example:
//
//	newAttr, preSize, preMtime, preCtime, err := repo.WriteFile(ctx, handle, 1024)
//	if err != nil {
//	    // Handle error (permission denied, file not found, etc.)
//	}
//	// File size now reflects write
//	// newAttr.Mtime reflects write time
func (r *MemoryRepository) WriteFile(
	ctx *metadata.AuthContext,
	handle metadata.FileHandle,
	newSize uint64,
) (*metadata.FileAttr, uint64, time.Time, time.Time, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, 0, time.Time{}, time.Time{}, fmt.Errorf("context cancelled before writing file: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, 0, time.Time{}, time.Time{}, fmt.Errorf("context cancelled while writing file: %w", err)
	}

	// ========================================================================
	// Step 1: Verify file exists
	// ========================================================================

	key := handleToKey(handle)
	attr, exists := r.files[key]
	if !exists {
		return nil, 0, time.Time{}, time.Time{}, &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "file not found",
		}
	}

	// ========================================================================
	// Step 2: Verify it's a regular file
	// ========================================================================

	if attr.Type != metadata.FileTypeRegular {
		return nil, 0, time.Time{}, time.Time{}, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("not a regular file: type=%d", attr.Type),
		}
	}

	// ========================================================================
	// Step 3: Capture pre-operation attributes
	// ========================================================================

	preSize := attr.Size
	preMtime := attr.Mtime
	preCtime := attr.Ctime

	// ========================================================================
	// Step 4: Check write permission
	// ========================================================================

	if !hasWritePermission(ctx, attr) {
		return nil, preSize, preMtime, preCtime, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied",
		}
	}

	// ========================================================================
	// Step 5: Ensure file has a content ID
	// ========================================================================

	if attr.ContentID == "" {
		var handlePart []byte
		if len(handle) >= 16 {
			handlePart = handle[:16]
		} else {
			handlePart = handle
		}
		attr.ContentID = content.ContentID(fmt.Sprintf("file-%x", handlePart))
	}

	// ========================================================================
	// Step 6: Update file size if needed
	// ========================================================================

	sizeChanged := false
	if newSize > attr.Size {
		attr.Size = newSize
		sizeChanged = true
	}

	// ========================================================================
	// Step 7: Update file timestamps
	// ========================================================================

	now := time.Now()
	attr.Mtime = now

	if sizeChanged {
		attr.Ctime = now
	}

	// ========================================================================
	// Step 8: Save updated attributes
	// ========================================================================

	r.files[key] = attr

	logger.Debug("WriteFile: updated file %x (oldSize=%d, newSize=%d, sizeChanged=%v)",
		handle, preSize, attr.Size, sizeChanged)

	// Return a copy of the attributes
	attrCopy := *attr

	return &attrCopy, preSize, preMtime, preCtime, nil
}
