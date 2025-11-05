package mount

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
	"github.com/marmos91/dittofs/internal/protocol/rpc"
	xdr "github.com/rasky/go-xdr/xdr2"
)

// DefaultMountHandler implements the Mount protocol handlers defined in RFC 1813 Appendix I.
// It provides the standard implementation for mount operations, allowing NFS clients
// to obtain file handles for exported filesystems.
type DefaultMountHandler struct{}

// MountRequest represents a MOUNT (MNT) request from an NFS client.
// The client sends the path of the directory they wish to mount.
// This structure is decoded from XDR-encoded data received over the network.
//
// RFC 1813 Appendix I specifies the MOUNT procedure as:
//
//	MNT(dirpath) -> fhstatus3
type MountRequest struct {
	// DirPath is the absolute path on the server that the client wants to mount.
	// This must match an exported path configured in the server's repository.
	// Example: "/export" or "/data/shared"
	DirPath string
}

// MountResponse represents the response to a MOUNT (MNT) request.
// It contains the status of the mount operation and, if successful,
// the file handle and supported authentication methods.
//
// The response is encoded in XDR format before being sent back to the client.
type MountResponse struct {
	// Status indicates the result of the mount operation.
	// Common values:
	//   - MountOK (0): Success
	//   - MountErrNoEnt (2): Export path not found
	//   - MountErrAccess (13): Permission denied
	//   - MountErrServerFault (10006): Internal server error
	Status uint32

	// FileHandle is the opaque file handle for the root of the mounted filesystem.
	// This handle is used in subsequent NFS operations to identify the filesystem.
	// Only present when Status == MountOK.
	// The handle format is server-specific; clients treat it as opaque data.
	FileHandle []byte

	// AuthFlavors is a list of authentication flavors supported by the server
	// for this mount. Only present when Status == MountOK.
	// Common values:
	//   - 0: AUTH_NULL (no authentication)
	//   - 1: AUTH_UNIX (Unix-style authentication)
	// Currently, this implementation only supports AUTH_NULL.
	AuthFlavors []int32
}

// DecodeMountRequest decodes a MOUNT request from XDR-encoded bytes.
// It uses the XDR unmarshaling library to parse the incoming data according
// to the Mount protocol specification.
//
// Parameters:
//   - data: XDR-encoded bytes containing the mount request
//
// Returns:
//   - *MountRequest: The decoded mount request containing the directory path
//   - error: Any error encountered during decoding
//
// Example:
//
//	data := []byte{...} // XDR-encoded mount request
//	req, err := DecodeMountRequest(data)
//	if err != nil {
//	    // handle error
//	}
//	fmt.Println("Mount path:", req.DirPath)
func DecodeMountRequest(data []byte) (*MountRequest, error) {
	req := &MountRequest{}
	_, err := xdr.Unmarshal(bytes.NewReader(data), req)

	if err != nil {
		return nil, err
	}
	return req, nil
}

// Encode serializes the MountResponse into XDR-encoded bytes suitable for
// transmission over the network.
//
// The encoding follows RFC 1813 Appendix I specifications:
//  1. Status code (4 bytes)
//  2. If status == MountOK:
//     a. File handle length (4 bytes)
//     b. File handle data (variable length)
//     c. Padding to 4-byte boundary
//     d. Auth flavors count (4 bytes)
//     e. Auth flavor values (4 bytes each)
//
// XDR encoding requires all data to be aligned to 4-byte boundaries,
// so padding bytes are added after variable-length fields.
//
// Returns:
//   - []byte: The XDR-encoded response ready to send to the client
//   - error: Any error encountered during encoding
//
// Example:
//
//	resp := &MountResponse{
//	    Status:      MountOK,
//	    FileHandle:  []byte{0x01, 0x02, 0x03, 0x04},
//	    AuthFlavors: []int32{0},
//	}
//	data, err := resp.Encode()
//	if err != nil {
//	    // handle error
//	}
//	// Send 'data' to client
func (resp *MountResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	// If status is not OK, we're done - only status is returned for errors
	if resp.Status != MountOK {
		return buf.Bytes(), nil
	}

	// Write file handle (opaque data)
	// XDR opaque format: length followed by data
	handleLen := uint32(len(resp.FileHandle))
	if err := binary.Write(&buf, binary.BigEndian, handleLen); err != nil {
		return nil, fmt.Errorf("write handle length: %w", err)
	}

	buf.Write(resp.FileHandle)

	// Add padding to 4-byte boundary (XDR alignment requirement)
	padding := rpc.XdrPadding(handleLen)
	buf.Write(make([]byte, padding))

	// Write auth flavors array
	// XDR array format: count followed by elements
	authCount := uint32(len(resp.AuthFlavors))
	if err := binary.Write(&buf, binary.BigEndian, authCount); err != nil {
		return nil, fmt.Errorf("write auth count: %w", err)
	}

	for _, flavor := range resp.AuthFlavors {
		if err := binary.Write(&buf, binary.BigEndian, flavor); err != nil {
			return nil, fmt.Errorf("write auth flavor: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// Mount handles the MOUNT (MNT) procedure, which is the primary operation
// used by NFS clients to obtain a file handle for an exported filesystem.
//
// The mount process:
//  1. Validates that the requested path corresponds to a configured export
//  2. Retrieves the root file handle for that export from the metadata repository
//  3. Returns the handle along with supported authentication flavors
//
// This implementation currently supports only AUTH_NULL (no authentication).
// In a production system, you might want to add:
//   - Authentication/authorization checks
//   - Export access control (read-only, client restrictions)
//   - Mount tracking for the DUMP procedure
//
// Parameters:
//   - repository: The metadata repository containing export configurations
//   - req: The mount request containing the directory path to mount
//
// Returns:
//   - *MountResponse: The mount response with status and file handle (if successful)
//   - error: Always returns nil; errors are indicated via response Status field
//
// RFC 1813 Appendix I: MOUNT Procedure
//
// Example:
//
//	handler := &DefaultMountHandler{}
//	req := &MountRequest{DirPath: "/export"}
//	resp, err := handler.Mount(repository, req)
//	if err != nil {
//	    // handle error
//	}
//	if resp.Status == MountOK {
//	    // Use resp.FileHandle for NFS operations
//	}
func (h *DefaultMountHandler) Mount(
	repository metadata.Repository,
	req *MountRequest,
) (*MountResponse, error) {
	logger.Info("Mount called for path: %s", req.DirPath)

	// Check if the export exists
	export, err := repository.FindExport(req.DirPath)
	if err != nil {
		logger.Warn("Export not found: %s", req.DirPath)

		return &MountResponse{
			Status: MountErrNoEnt,
		}, nil
	}

	logger.Info("Found export: %s (readonly=%v)", export.Path, export.Options.ReadOnly)

	// Get the root handle for this export
	handleBytes, err := repository.GetRootHandle(export.Path)
	if err != nil {
		logger.Warn("Failed to get root handle: %v", err)
		return &MountResponse{
			Status: MountErrServerFault,
		}, nil
	}

	return &MountResponse{
		Status:      MountOK,
		FileHandle:  handleBytes,
		AuthFlavors: []int32{0}, // AUTH_NULL
	}, nil
}
