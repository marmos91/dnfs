package memory

import (
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// ============================================================================
// File Creation Operations
// ============================================================================

// CreateLink creates a hard link to an existing file.
//
// This implements support for the LINK NFS procedure (RFC 1813 section 3.3.15).
// A hard link creates a new directory entry that references the same file data
// as an existing file. Both names refer to identical file content and attributes.
//
// Implementation Notes:
//   - This implementation uses a handle-based system where multiple directory
//     entries can reference the same handle without tracking a separate link count
//   - In a real implementation with link counting, you would increment nlink here
//   - The file's change time (ctime) is updated to reflect metadata modification
//
// RFC 1813 Requirements:
//   - The link and file must be on the same filesystem
//   - The server should check write permission on the target directory
//   - The file's ctime should be updated
//   - Hard links to directories are typically not allowed
//
// Parameters:
//   - ctx: Authentication context for access control
//   - dirHandle: Target directory where the link will be created
//   - name: Name for the new link
//   - fileHandle: File to link to
//
// Returns error if:
//   - Context is cancelled
//   - Source file not found
//   - Target directory not found or not a directory
//   - Access denied (no write permission on directory)
//   - Name already exists in directory
//   - Cross-filesystem link attempted (implementation-specific)
func (r *MemoryRepository) CreateLink(
	ctx *metadata.AuthContext,
	dirHandle metadata.FileHandle,
	name string,
	fileHandle metadata.FileHandle,
) error {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled before creating link: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return fmt.Errorf("context cancelled while creating link: %w", err)
	}

	// ========================================================================
	// Step 1: Verify source file exists
	// ========================================================================

	fileKey := handleToKey(fileHandle)
	fileAttr, exists := r.files[fileKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "source file not found",
		}
	}

	// ========================================================================
	// Step 2: Verify target directory exists and is a directory
	// ========================================================================

	dirKey := handleToKey(dirHandle)
	dirAttr, exists := r.files[dirKey]
	if !exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrNotFound,
			Message: "target directory not found",
		}
	}

	if dirAttr.Type != metadata.FileTypeDirectory {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: "target is not a directory",
		}
	}

	// ========================================================================
	// Step 3: Check write access to directory
	// ========================================================================

	if !hasWritePermission(ctx, dirAttr) {
		return &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on target directory",
		}
	}

	// ========================================================================
	// Step 4: Verify name doesn't already exist
	// ========================================================================

	if r.children[dirKey] == nil {
		r.children[dirKey] = make(map[string]metadata.FileHandle)
	}

	if _, exists := r.children[dirKey][name]; exists {
		return &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("name already exists: %s", name),
		}
	}

	// ========================================================================
	// Step 5: Create the link
	// ========================================================================

	// Add the directory entry pointing to the existing file handle
	r.children[dirKey][name] = fileHandle

	// ========================================================================
	// Step 6: Update timestamps
	// ========================================================================

	now := time.Now()

	// Update directory modification time (contents changed)
	dirAttr.Mtime = now
	dirAttr.Ctime = now
	r.files[dirKey] = dirAttr

	// Update file change time (metadata changed - link count would increase)
	fileAttr.Ctime = now
	r.files[fileKey] = fileAttr

	logger.Debug("CreateLink: created link '%s' in directory %x to file %x", name, dirHandle, fileHandle)

	// Note: In a real implementation with link counting, you would also
	// increment fileAttr.Nlink here. However, in this handle-based system,
	// the link count is computed dynamically when needed.

	return nil
}

// CreateDirectory creates a new directory with the specified attributes.
//
// This implements support for the MKDIR NFS procedure (RFC 1813 section 3.3.9).
// It handles the complete directory creation workflow including permission checks,
// attribute completion with defaults, and parent directory updates.
//
// Attribute Completion:
// The protocol layer provides partial attributes (type, mode, uid, gid) which
// may have defaults applied. This method completes them with:
//   - Size: 4096 (standard directory size)
//   - Timestamps: Current time for atime, mtime, ctime
//   - ContentID: Empty (directories don't have content blobs)
//
// RFC 1813 Requirements:
//   - Check write permission on parent directory
//   - Verify name doesn't already exist
//   - Apply appropriate default permissions if not specified
//   - Update parent directory timestamps (mtime, ctime)
//
// Parameters:
//   - ctx: Authentication context for access control
//   - parentHandle: Handle of the parent directory
//   - name: Name for the new directory
//   - attr: Partial attributes (type, mode, uid, gid) - may have defaults
//
// Returns:
//   - FileHandle: Handle of the newly created directory
//   - error: Returns error if:
//   - Context is cancelled
//   - Access denied (no write permission on parent)
//   - Name already exists
//   - Parent is not a directory
//   - I/O error
func (r *MemoryRepository) CreateDirectory(
	ctx *metadata.AuthContext,
	parentHandle metadata.FileHandle,
	name string,
	attr *metadata.FileAttr,
) (metadata.FileHandle, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before creating directory: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled while creating directory: %w", err)
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
	// Step 2: Check write access to parent directory
	// ========================================================================

	if !hasWritePermission(ctx, parentAttr) {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on parent directory",
		}
	}

	// ========================================================================
	// Step 3: Verify name doesn't already exist
	// ========================================================================

	if r.children[parentKey] == nil {
		r.children[parentKey] = make(map[string]metadata.FileHandle)
	}

	if _, exists := r.children[parentKey][name]; exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("directory already exists: %s", name),
		}
	}

	// ========================================================================
	// Step 4: Complete directory attributes with defaults
	// ========================================================================

	now := time.Now()

	// Start with the attributes provided by the protocol layer
	completeAttr := &metadata.FileAttr{
		Type: metadata.FileTypeDirectory, // Always directory
		Mode: attr.Mode,                  // From protocol layer (or default 0755)
		UID:  attr.UID,                   // From protocol layer (or authenticated user)
		GID:  attr.GID,                   // From protocol layer (or authenticated group)

		// Server-assigned attributes
		Size:      4096, // Standard directory size
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
		ContentID: "", // Directories don't have content blobs
	}

	// ========================================================================
	// Step 5: Generate unique directory handle
	// ========================================================================

	dirHandle := r.generateFileHandle(name)

	// ========================================================================
	// Step 6: Create directory metadata
	// ========================================================================

	dirKey := handleToKey(dirHandle)
	r.files[dirKey] = completeAttr

	// Initialize empty children map for the new directory
	r.children[dirKey] = make(map[string]metadata.FileHandle)

	// ========================================================================
	// Step 7: Link directory to parent
	// ========================================================================

	r.children[parentKey][name] = dirHandle

	// Set parent relationship
	r.parents[dirKey] = parentHandle

	// ========================================================================
	// Step 8: Update parent directory timestamps
	// ========================================================================
	// The parent directory's mtime and ctime should be updated when a child
	// is added, as this modifies the directory's contents.

	parentAttr.Mtime = now
	parentAttr.Ctime = now
	r.files[parentKey] = parentAttr

	logger.Debug("CreateDirectory: created directory '%s' in parent %x with handle %x", name, parentHandle, dirHandle)

	return dirHandle, nil
}

// CreateSpecialFile creates a special file (device, socket, or FIFO).
//
// This implements support for the MKNOD NFS procedure (RFC 1813 section 3.3.11).
// Special files are non-regular files that represent:
//   - Character devices (terminals, serial ports, etc.)
//   - Block devices (disks, partitions, etc.)
//   - Sockets (IPC endpoints)
//   - FIFOs/named pipes (IPC channels)
//
// Device Files:
// For character and block devices, the majorDevice and minorDevice parameters
// specify the device numbers. In this in-memory implementation, device numbers
// are stored in the SymlinkTarget field as "device:major:minor". A production
// implementation would use proper device-specific storage.
//
// Security:
// Device file creation typically requires root privileges (UID 0) to prevent
// unauthorized hardware access. This is enforced by checking the authenticated
// user's UID.
//
// RFC 1813 Requirements:
//   - Check write permission on parent directory
//   - Check privilege requirements (root for devices)
//   - Verify name doesn't already exist
//   - Apply appropriate default permissions
//   - Update parent directory timestamps
//
// Parameters:
//   - ctx: Authentication context for access control
//   - parentHandle: Handle of the parent directory
//   - name: Name for the new special file
//   - attr: Partial attributes (type, mode, uid, gid) from protocol layer
//   - majorDevice: Major device number (for block/char devices, 0 otherwise)
//   - minorDevice: Minor device number (for block/char devices, 0 otherwise)
//
// Returns:
//   - FileHandle: Handle of the newly created special file
//   - error: Returns error if:
//   - Context is cancelled
//   - Access denied (no write permission or insufficient privileges)
//   - Name already exists
//   - Parent is not a directory
//   - Invalid file type
//   - I/O error
func (r *MemoryRepository) CreateSpecialFile(
	ctx *metadata.AuthContext,
	parentHandle metadata.FileHandle,
	name string,
	attr *metadata.FileAttr,
	majorDevice uint32,
	minorDevice uint32,
) (metadata.FileHandle, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before creating special file: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled while creating special file: %w", err)
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
	// Step 2: Check write access to parent directory
	// ========================================================================

	if !hasWritePermission(ctx, parentAttr) {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on parent directory",
		}
	}

	// ========================================================================
	// Step 3: Check privilege requirements for device creation
	// ========================================================================
	// Device files (character and block devices) typically require root
	// privileges to create. This is a security measure to prevent
	// unauthorized hardware access.

	if attr.Type == metadata.FileTypeChar || attr.Type == metadata.FileTypeBlock {
		// Check if user has sufficient privileges (root or CAP_MKNOD)
		if ctx == nil || ctx.UID == nil || *ctx.UID != 0 {
			// Non-root user attempting to create a device file
			return nil, &metadata.ExportError{
				Code:    metadata.ExportErrAccessDenied,
				Message: "device file creation requires root privileges",
			}
		}
	}

	// ========================================================================
	// Step 4: Verify name doesn't already exist
	// ========================================================================

	if r.children[parentKey] == nil {
		r.children[parentKey] = make(map[string]metadata.FileHandle)
	}

	if _, exists := r.children[parentKey][name]; exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("file already exists: %s", name),
		}
	}

	// ========================================================================
	// Step 5: Complete file attributes with defaults
	// ========================================================================

	now := time.Now()

	// Start with the attributes provided by the protocol layer
	completeAttr := &metadata.FileAttr{
		Type: attr.Type, // FileTypeChar, FileTypeBlock, FileTypeSocket, or FileTypeFifo
		Mode: attr.Mode, // From protocol layer (or default)
		UID:  attr.UID,  // From protocol layer (or authenticated user)
		GID:  attr.GID,  // From protocol layer (or authenticated group)

		// Server-assigned attributes
		Size:      0, // Special files have no content
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
		ContentID: "", // Special files don't have content blobs
	}

	// Apply default mode if not specified
	if completeAttr.Mode == 0 {
		completeAttr.Mode = 0644 // Default: rw-r--r--
	}

	// ========================================================================
	// Step 6: Store device numbers (implementation-specific)
	// ========================================================================
	// In this in-memory implementation, we store device numbers in the
	// SymlinkTarget field as a formatted string. In a real implementation,
	// you would store this in a proper device-specific field.
	//
	// Note: This is a demonstration of how to handle device numbers.
	// A production implementation should use a proper storage mechanism.

	if attr.Type == metadata.FileTypeChar || attr.Type == metadata.FileTypeBlock {
		// Store device numbers as a string in the format "device:major:minor"
		// This is just for demonstration - a real implementation would
		// store these properly in the filesystem metadata
		completeAttr.SymlinkTarget = fmt.Sprintf("device:%d:%d", majorDevice, minorDevice)
	}

	// ========================================================================
	// Step 7: Generate unique file handle
	// ========================================================================

	fileHandle := r.generateFileHandle(name)

	// ========================================================================
	// Step 8: Create special file metadata
	// ========================================================================

	fileKey := handleToKey(fileHandle)
	r.files[fileKey] = completeAttr

	// ========================================================================
	// Step 9: Link special file to parent
	// ========================================================================

	r.children[parentKey][name] = fileHandle

	// Set parent relationship
	r.parents[fileKey] = parentHandle

	// ========================================================================
	// Step 10: Update parent directory timestamps
	// ========================================================================
	// The parent directory's mtime and ctime should be updated when a child
	// is added, as this modifies the directory's contents.

	parentAttr.Mtime = now
	parentAttr.Ctime = now
	r.files[parentKey] = parentAttr

	logger.Debug("CreateSpecialFile: created special file '%s' (type=%d) in parent %x with handle %x",
		name, attr.Type, parentHandle, fileHandle)

	return fileHandle, nil
}

// CreateSymlink creates a symbolic link with the specified target path.
//
// This implements support for the SYMLINK NFS procedure (RFC 1813 section 3.3.10).
// A symbolic link is a special file that contains a pathname to another file or
// directory. Unlike hard links, symlinks can span filesystems and can point to
// directories.
//
// Implementation Notes:
//   - The target path is stored in attr.SymlinkTarget without validation
//   - The symlink size is set to the length of the target path
//   - The protocol layer provides attributes with proper defaults via convertSetAttrsToMetadata
//   - Actual access control is applied when following the symlink, not on the symlink itself
//
// RFC 1813 Requirements:
//   - Check write permission on parent directory
//   - Verify name doesn't already exist
//   - Store target path without validation (dangling symlinks are allowed)
//   - Update parent directory timestamps (mtime, ctime)
//   - Set appropriate symlink attributes
//
// Parameters:
//   - ctx: Authentication context for access control
//   - parentHandle: Handle of the parent directory
//   - name: Name for the new symbolic link
//   - target: Path that the symlink will point to
//   - attr: Partial attributes (mode, uid, gid) from protocol layer with proper defaults
//
// Returns:
//   - FileHandle: Handle of the newly created symlink
//   - error: Returns error if:
//   - Context is cancelled
//   - Access denied (no write permission on parent)
//   - Name already exists
//   - Parent is not a directory
//   - I/O error
func (r *MemoryRepository) CreateSymlink(
	ctx *metadata.AuthContext,
	parentHandle metadata.FileHandle,
	name string,
	target string,
	attr *metadata.FileAttr,
) (metadata.FileHandle, error) {
	// Check context before acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before creating symlink: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Context.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled while creating symlink: %w", err)
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
	// Step 2: Check write access to parent directory
	// ========================================================================

	if !hasWritePermission(ctx, parentAttr) {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrAccessDenied,
			Message: "write permission denied on parent directory",
		}
	}

	// ========================================================================
	// Step 3: Verify name doesn't already exist
	// ========================================================================

	if r.children[parentKey] == nil {
		r.children[parentKey] = make(map[string]metadata.FileHandle)
	}

	if _, exists := r.children[parentKey][name]; exists {
		return nil, &metadata.ExportError{
			Code:    metadata.ExportErrServerFault,
			Message: fmt.Sprintf("file already exists: %s", name),
		}
	}

	// ========================================================================
	// Step 4: Complete symlink attributes with server-assigned values
	// ========================================================================

	now := time.Now()

	// The protocol layer has already applied proper defaults via convertSetAttrsToMetadata,
	// including UID/GID from auth context. We just need to complete the remaining fields.
	completeAttr := &metadata.FileAttr{
		Type: metadata.FileTypeSymlink, // Always symlink
		Mode: attr.Mode,                // From protocol layer (default 0777 already applied)
		UID:  attr.UID,                 // From protocol layer (auth context UID or client-provided)
		GID:  attr.GID,                 // From protocol layer (auth context GID or client-provided)

		// Server-assigned attributes
		Size:          uint64(len(target)), // Size is length of target path
		Atime:         now,
		Mtime:         now,
		Ctime:         now,
		ContentID:     "",     // Symlinks don't have content blobs
		SymlinkTarget: target, // Store the target path
	}

	// ========================================================================
	// Step 5: Generate unique symlink handle
	// ========================================================================

	symlinkHandle := r.generateFileHandle(name)

	// ========================================================================
	// Step 6: Create symlink metadata
	// ========================================================================

	symlinkKey := handleToKey(symlinkHandle)
	r.files[symlinkKey] = completeAttr

	// ========================================================================
	// Step 7: Link symlink to parent
	// ========================================================================

	r.children[parentKey][name] = symlinkHandle

	// Set parent relationship
	r.parents[symlinkKey] = parentHandle

	// ========================================================================
	// Step 8: Update parent directory timestamps
	// ========================================================================
	// The parent directory's mtime and ctime should be updated when a child
	// is added, as this modifies the directory's contents.

	parentAttr.Mtime = now
	parentAttr.Ctime = now
	r.files[parentKey] = parentAttr

	logger.Debug("CreateSymlink: created symlink '%s' -> '%s' in parent %x with handle %x",
		name, target, parentHandle, symlinkHandle)

	return symlinkHandle, nil
}
