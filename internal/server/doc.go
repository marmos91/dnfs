// Package server provides the DittoServer, which manages multiple protocol facades
// (NFS, SMB, WebDAV, etc.) that share common metadata and content repositories.
//
// The server implements a facade pattern where each protocol (NFS, SMB, etc.) is
// represented by a Facade implementation that shares common backend repositories.
// This allows DittoFS to expose the same file system through multiple protocols
// simultaneously while maintaining a single source of truth for metadata and content.
package server
