package metadata

import "github.com/marmos91/dittofs/pkg/store/metadata"

const (
	// DefaultAnonUID is the default anonymous UID (typically "nobody")
	// Per Unix conventions, 65534 is reserved for the "nobody" user
	DefaultAnonUID uint32 = 65534

	// DefaultAnonGID is the default anonymous GID (typically "nogroup")
	// Per Unix conventions, 65534 is reserved for the "nogroup" group
	DefaultAnonGID uint32 = 65534
)

// ApplySquashing applies UID/GID squashing rules based on export options.
//
// This function implements the standard NFS squashing behaviors:
//   - MapAllToAnonymous: Maps all users to anonymous UID/GID
//   - MapPrivilegedToAnonymous: Maps root (UID 0) to anonymous UID/GID
//   - AUTH_NULL: Always uses anonymous UID/GID
//
// Squashing is a security and access control mechanism that prevents
// clients from using their own UID/GID for file operations. This is
// particularly useful for:
//   - Public/world-accessible exports (MapAllToAnonymous)
//   - Preventing root privilege escalation (MapPrivilegedToAnonymous)
//   - Anonymous access scenarios
//
// Parameters:
//   - opts: Export options containing identity mapping configuration
//   - authFlavor: Authentication method (0=AUTH_NULL, 1=AUTH_UNIX)
//   - uid: Original UID from client credentials (nil for AUTH_NULL)
//   - gid: Original GID from client credentials (nil for AUTH_NULL)
//   - gids: Original supplementary GIDs from client credentials
//
// Returns:
//   - effectiveUID: UID to use for permission checks
//   - effectiveGID: GID to use for permission checks
//   - effectiveGIDs: Supplementary GIDs to use for permission checks
func ApplySquashing(
	opts *metadata.ShareOptions,
	authFlavor uint32,
	uid *uint32,
	gid *uint32,
	gids []uint32,
) (effectiveUID *uint32, effectiveGID *uint32, effectiveGIDs []uint32) {
	// If no identity mapping is configured, use original credentials
	if opts.IdentityMapping == nil {
		return uid, gid, gids
	}

	idMapping := opts.IdentityMapping

	// Determine anonymous UID/GID (use configured values or defaults)
	var anonUID uint32
	if idMapping.AnonymousUID == nil {
		anonUID = DefaultAnonUID
	} else {
		anonUID = *idMapping.AnonymousUID
	}

	var anonGID uint32
	if idMapping.AnonymousGID == nil {
		anonGID = DefaultAnonGID
	} else {
		anonGID = *idMapping.AnonymousGID
	}

	// AUTH_NULL always gets anonymous credentials
	if authFlavor == 0 { // AUTH_NULL
		return &anonUID, &anonGID, []uint32{}
	}

	// AUTH_UNIX with MapAllToAnonymous: map all users to anonymous
	if idMapping.MapAllToAnonymous {
		return &anonUID, &anonGID, []uint32{}
	}

	// AUTH_UNIX with MapPrivilegedToAnonymous: map root to anonymous
	if idMapping.MapPrivilegedToAnonymous && uid != nil && *uid == 0 {
		squashedUID := anonUID
		squashedGID := anonGID

		// If original GID is also 0, squash it too
		if gid != nil && *gid == 0 {
			return &squashedUID, &squashedGID, gids
		}

		// GID is not root, keep original GID but squash UID
		return &squashedUID, gid, gids
	}

	// No squashing: use original credentials
	return uid, gid, gids
}
