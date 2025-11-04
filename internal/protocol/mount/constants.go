package mount

// Mount Protocol Procedure Numbers
// These identify the different Mount operations as defined in RFC 1813 Appendix I.
const (
	// MountProcNull - Do nothing (connectivity test)
	MountProcNull = 0

	// MountProcMnt - Add mount entry
	MountProcMnt = 1

	// MountProcDump - Return mount entries
	MountProcDump = 2

	// MountProcUmnt - Remove mount entry
	MountProcUmnt = 3

	// MountProcUmntAll - Remove all mount entries
	MountProcUmntAll = 4

	// MountProcExport - Return export list
	MountProcExport = 5
)

// Mount Status Codes
// These are the error codes that can be returned by Mount protocol procedures.
const (
	// MountOK - Success
	MountOK = 0

	// MountErrPerm - Not owner
	MountErrPerm = 1

	// MountErrNoEnt - No such file or directory
	MountErrNoEnt = 2

	// MountErrIO - I/O error
	MountErrIO = 5

	// MountErrAccess - Permission denied
	MountErrAccess = 13

	// MountErrNotDir - Not a directory
	MountErrNotDir = 20

	// MountErrInval - Invalid argument
	MountErrInval = 22

	// MountErrNameTooLong - Filename too long
	MountErrNameTooLong = 63

	// MountErrNotSupp - Operation not supported
	MountErrNotSupp = 10004

	// MountErrServerFault - Server fault
	MountErrServerFault = 10006
)
