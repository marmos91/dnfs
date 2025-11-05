package nfs

// NFSv3 Procedure Numbers
// These identify the different NFS operations as defined in RFC 1813.
const (
	// NFSProcNull - Do nothing (connectivity test)
	NFSProcNull = 0

	// NFSProcGetAttr - Get file attributes
	NFSProcGetAttr = 1

	// NFSProcSetAttr - Set file attributes
	NFSProcSetAttr = 2

	// NFSProcLookup - Lookup filename
	NFSProcLookup = 3

	// NFSProcAccess - Check access permission
	NFSProcAccess = 4

	// NFSProcReadLink - Read symbolic link
	NFSProcReadLink = 5

	// NFSProcRead - Read from file
	NFSProcRead = 6

	// NFSProcWrite - Write to file
	NFSProcWrite = 7

	// NFSProcCreate - Create a file
	NFSProcCreate = 8

	// NFSProcMkdir - Create a directory
	NFSProcMkdir = 9

	// NFSProcSymlink - Create a symbolic link
	NFSProcSymlink = 10

	// NFSProcMknod - Create a special device
	NFSProcMknod = 11

	// NFSProcRemove - Remove a file
	NFSProcRemove = 12

	// NFSProcRmdir - Remove a directory
	NFSProcRmdir = 13

	// NFSProcRename - Rename a file or directory
	NFSProcRename = 14

	// NFSProcLink - Create a hard link
	NFSProcLink = 15

	// NFSProcReadDir - Read directory entries
	NFSProcReadDir = 16

	// NFSProcReadDirPlus - Extended read directory (with attributes)
	NFSProcReadDirPlus = 17

	// NFSProcFsStat - Get dynamic file system information
	NFSProcFsStat = 18

	// NFSProcFsInfo - Get static file system information
	NFSProcFsInfo = 19

	// NFSProcPathConf - Get POSIX information
	NFSProcPathConf = 20

	// NFSProcCommit - Commit cached data to stable storage
	NFSProcCommit = 21

	// NFS3ErrInval - Invalid argument
	NFS3ErrInval = 22
)

// NFS Status Codes
// These are the error codes that can be returned by NFSv3 procedures.
// Defined in RFC 1813 Section 3.3.
const (
	// NFS3OK - Success
	NFS3OK = 0

	// NFS3ErrPerm - Not owner
	NFS3ErrPerm = 1

	// NFS3ErrNoEnt - No such file or directory
	NFS3ErrNoEnt = 2

	// NFS3ErrIO - I/O error
	NFS3ErrIO = 5

	// NFS3ErrAcces - Permission denied
	NFS3ErrAcces = 13

	// NFS3ErrExist - File exists
	NFS3ErrExist = 17

	// NFS3ErrNotDir - Not a directory
	NFS3ErrNotDir = 20

	// NFS3ErrIsDir - Is a directory
	NFS3ErrIsDir = 21

	// NFS3ErrFBig - File too large
	NFS3ErrFBig = 27

	// NFS3ErrNoSpc - No space left on device
	NFS3ErrNoSpc = 28

	// NFS3ErrRofs - Read-only file system
	NFS3ErrRofs = 30

	// NFS3ErrNameTooLong - Filename too long
	NFS3ErrNameTooLong = 63

	// NFS3ErrNotEmpty - Directory not empty
	NFS3ErrNotEmpty = 66

	// NFS3ErrStale - Stale file handle
	NFS3ErrStale = 70

	// NFS3ErrNotSync - Update synchronization mismatch
	NFS3ErrNotSync = 10002
)

// FSInfo property flags (RFC 1813 Section 3.3.19)
const (
	FSFLink        = 0x0001 // Hard links supported
	FSFSymlink     = 0x0002 // Symbolic links supported
	FSFHomogeneous = 0x0008 // PATHCONF valid for all files
	FSFCanSetTime  = 0x0010 // Server can set times
)
