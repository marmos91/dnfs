package metadata

import (
	"time"

	"github.com/marmos91/dittofs/internal/content"
)

// FileHandle represents a unique identifier for a file or directory
type FileHandle []byte

// FileAttr contains the metadata for a file or directory
type FileAttr struct {
	Type          FileType
	Mode          uint32
	UID           uint32
	GID           uint32
	Size          uint64
	Atime         time.Time
	Mtime         time.Time
	Ctime         time.Time
	ContentID     content.ContentID // ID referencing the actual file content
	SymlinkTarget string            // For symlinks, the target path
}

// FSStat contains dynamic filesystem statistics.
// This structure is returned by the repository to inform NFS clients
// about current filesystem capacity and usage.
type FSStat struct {
	// TotalBytes is the total size of the filesystem in bytes
	TotalBytes uint64

	// FreeBytes is the free space available in bytes
	FreeBytes uint64

	// AvailBytes is the free space available to non-privileged users
	// May be less than FreeBytes if space is reserved for root
	AvailBytes uint64

	// TotalFiles is the total number of file slots (inodes)
	TotalFiles uint64

	// FreeFiles is the number of free file slots
	FreeFiles uint64

	// AvailFiles is the free file slots available to non-privileged users
	AvailFiles uint64

	// Invarsec is the number of seconds for which the filesystem is not
	// expected to change. A value of 0 means the filesystem is expected to
	// change at any time.
	Invarsec uint32
}

// FileType represents the type of file
type FileType uint32

const (
	FileTypeRegular   FileType = 1
	FileTypeDirectory FileType = 2
	FileTypeBlock     FileType = 3
	FileTypeChar      FileType = 4
	FileTypeSymlink   FileType = 5
	FileTypeSocket    FileType = 6
	FileTypeFifo      FileType = 7
)
