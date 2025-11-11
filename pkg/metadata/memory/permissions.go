package memory

import "github.com/marmos91/dittofs/pkg/metadata"

// ============================================================================
// Permission Helper Functions
// ============================================================================

// hasWritePermission checks if the user has write permission on a file/directory.
//
// Permission check logic:
//   - Root (UID 0): Always granted
//   - Owner: Check owner write bit (mode & 0200)
//   - Group member: Check group write bit (mode & 0020)
//   - Other: Check other write bit (mode & 0002)
//   - AUTH_NULL: Only if world-writable (mode & 0002)
//
// Parameters:
//   - attr: File attributes containing mode, uid, gid
//   - ctx: Authentication context (may be nil for unauthenticated access)
//
// Returns:
//   - bool: true if write permission granted, false otherwise
func hasWritePermission(ctx *metadata.AuthContext, attr *metadata.FileAttr) bool {
	// No authentication context: deny by default
	if ctx == nil {
		return false
	}

	// AUTH_NULL: only grant write if world-writable
	if ctx.AuthFlavor == 0 {
		return (attr.Mode & 0002) != 0
	}

	// Authenticated access requires both UID and GID
	if ctx.UID == nil || ctx.GID == nil {
		return false
	}

	uid := *ctx.UID
	gid := *ctx.GID

	// Root user bypasses all permission checks
	if uid == 0 {
		return true
	}

	// Owner permissions
	if uid == attr.UID {
		return (attr.Mode & 0200) != 0
	}

	// Group permissions
	if gid == attr.GID || containsGID(ctx.GIDs, attr.GID) {
		return (attr.Mode & 0020) != 0
	}

	// Other permissions
	return (attr.Mode & 0002) != 0
}

// hasReadPermission checks if the user has read permission on a file/directory.
//
// Permission check logic follows the same pattern as hasWritePermission
// but checks read bits (0400/0040/0004) instead of write bits.
//
// Parameters:
//   - attr: File attributes containing mode, uid, gid
//   - ctx: Authentication context (may be nil for unauthenticated access)
//
// Returns:
//   - bool: true if read permission granted, false otherwise
func hasReadPermission(ctx *metadata.AuthContext, attr *metadata.FileAttr) bool {
	// No authentication context: deny by default
	if ctx == nil {
		return false
	}

	// AUTH_NULL: only grant read if world-readable
	if ctx.AuthFlavor == 0 {
		return (attr.Mode & 0004) != 0
	}

	// Authenticated access requires both UID and GID
	if ctx.UID == nil || ctx.GID == nil {
		return false
	}

	uid := *ctx.UID
	gid := *ctx.GID

	// Root user bypasses all permission checks
	if uid == 0 {
		return true
	}

	// Owner permissions
	if uid == attr.UID {
		return (attr.Mode & 0400) != 0
	}

	// Group permissions
	if gid == attr.GID || containsGID(ctx.GIDs, attr.GID) {
		return (attr.Mode & 0040) != 0
	}

	// Other permissions
	return (attr.Mode & 0004) != 0
}

// hasExecutePermission checks if the user has execute permission on a directory.
//
// Execute permission on directories (also called "search" permission) is required
// to access files within the directory or traverse through it.
//
// Parameters:
//   - attr: Directory attributes containing mode, uid, gid
//   - ctx: Authentication context (may be nil for unauthenticated access)
//
// Returns:
//   - bool: true if execute permission granted, false otherwise
func hasExecutePermission(ctx *metadata.AuthContext, attr *metadata.FileAttr) bool {
	// No authentication context: deny by default
	if ctx == nil {
		return false
	}

	// AUTH_NULL: only grant execute if world-executable
	if ctx.AuthFlavor == 0 {
		return (attr.Mode & 0001) != 0
	}

	// Authenticated access requires both UID and GID
	if ctx.UID == nil || ctx.GID == nil {
		return false
	}

	uid := *ctx.UID
	gid := *ctx.GID

	// Root user bypasses all permission checks
	if uid == 0 {
		return true
	}

	// Owner permissions
	if uid == attr.UID {
		return (attr.Mode & 0100) != 0
	}

	// Group permissions
	if gid == attr.GID || containsGID(ctx.GIDs, attr.GID) {
		return (attr.Mode & 0010) != 0
	}

	// Other permissions
	return (attr.Mode & 0001) != 0
}

// isOwnerOrRoot checks if the authenticated user is the file owner or root.
//
// This is commonly used for operations that require ownership, such as:
//   - Changing file permissions (chmod)
//   - Changing file timestamps
//   - Setting extended attributes
//
// Parameters:
//   - attr: File attributes containing uid
//   - ctx: Authentication context (may be nil)
//
// Returns:
//   - bool: true if user is owner or root, false otherwise
func isOwnerOrRoot(ctx *metadata.AuthContext, attr *metadata.FileAttr) bool {
	if ctx == nil || ctx.AuthFlavor == 0 || ctx.UID == nil {
		return false
	}

	uid := *ctx.UID
	return uid == 0 || uid == attr.UID
}

// canChangeGroup checks if the user can change a file's group to the specified GID.
//
// Group changes are allowed if:
//   - User is root (UID 0)
//   - User is owner AND is a member of the target group
//
// Parameters:
//   - attr: File attributes containing uid
//   - targetGID: The new group ID being set
//   - ctx: Authentication context (may be nil)
//
// Returns:
//   - bool: true if group change is allowed, false otherwise
func canChangeGroup(ctx *metadata.AuthContext, attr *metadata.FileAttr, targetGID uint32) bool {
	if ctx == nil || ctx.AuthFlavor == 0 || ctx.UID == nil || ctx.GID == nil {
		return false
	}

	uid := *ctx.UID
	gid := *ctx.GID

	// Root can always change group
	if uid == 0 {
		return true
	}

	// Owner can change if they're in the target group
	if uid != attr.UID {
		return false
	}

	// Check if user is in the target group
	return gid == targetGID || containsGID(ctx.GIDs, targetGID)
}
