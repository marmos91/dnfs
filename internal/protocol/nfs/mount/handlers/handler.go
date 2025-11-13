package handlers

import "github.com/marmos91/dittofs/pkg/metadata"

type MountHandler interface {
	// MountNull does nothing. This is used to test connectivity.
	// RFC 1813 Appendix I
	MountNull(ctx *NullContext, repository metadata.MetadataStore, req *NullRequest) (*NullResponse, error)

	// Mount returns a file handle for the requested export path.
	// This is the primary procedure used to mount a file system.
	// RFC 1813 Appendix I
	Mount(ctx *MountContext, repository metadata.MetadataStore, req *MountRequest) (*MountResponse, error)

	// Dump returns a list of all mounted file systems.
	// RFC 1813 Appendix I
	Dump(ctx *DumpContext, repository metadata.MetadataStore, req *DumpRequest) (*DumpResponse, error)

	// Umnt removes a mount entry from the mount list.
	// RFC 1813 Appendix I
	Umnt(ctx *UmountContext, repository metadata.MetadataStore, req *UmountRequest) (*UmountResponse, error)

	// UmntAll removes all mount entries for the calling client.
	// RFC 1813 Appendix I
	UmntAll(ctx *UmountAllContext, repository metadata.MetadataStore, req *UmountAllRequest) (*UmountAllResponse, error)

	// Export returns a list of all exported file systems.
	// RFC 1813 Appendix I
	Export(ctx *ExportContext, repository metadata.MetadataStore, req *ExportRequest) (*ExportResponse, error)
}
