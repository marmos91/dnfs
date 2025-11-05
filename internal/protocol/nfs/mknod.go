package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// Device types for MKNOD (RFC 1813 Section 3.3.11)
const (
	NF3REG  = 1 // Regular file
	NF3DIR  = 2 // Directory
	NF3BLK  = 3 // Block special device
	NF3CHR  = 4 // Character special device
	NF3LNK  = 5 // Symbolic link
	NF3SOCK = 6 // Socket
	NF3FIFO = 7 // Named pipe (FIFO)
)

// MknodRequest represents an NFS MKNOD request (RFC 1813 Section 3.3.11).
// The MKNOD procedure creates a special file (device, socket, or FIFO).
type MknodRequest struct {
	// DirHandle is the file handle of the parent directory
	DirHandle []byte

	// Name is the name of the special file to create
	Name string

	// Type is the file type (NF3CHR, NF3BLK, NF3SOCK, NF3FIFO)
	Type uint32

	// Attr contains attributes for the new special file
	Attr SetAttrs

	// Spec contains device-specific data (for block/char devices)
	Spec DeviceSpec
}

// DeviceSpec contains device-specific data for block and character devices.
type DeviceSpec struct {
	// SpecData1 is the major device number
	SpecData1 uint32

	// SpecData2 is the minor device number
	SpecData2 uint32
}

// MknodResponse represents an NFS MKNOD response (RFC 1813 Section 3.3.11).
type MknodResponse struct {
	// Status is the NFS status code
	Status uint32

	// FileHandle is the handle of the created special file (only if Status == NFS3OK)
	FileHandle []byte

	// Attr contains post-operation attributes of the special file
	Attr *FileAttr

	// DirAttrBefore contains pre-operation weak cache consistency data for the directory
	DirAttrBefore *WccAttr

	// DirAttrAfter contains post-operation attributes for the directory
	DirAttrAfter *FileAttr
}

// Mknod creates a special device file.
//
// This implements the NFS MKNOD procedure as defined in RFC 1813 Section 3.3.11.
// It creates special files like device files, sockets, and named pipes.
//
// Authentication: This layer performs no authentication. Auth checks should be
// performed by the caller based on the authenticated RPC credentials.
//
// Error Handling:
//   - Returns NFS3ErrNoEnt if the parent directory doesn't exist
//   - Returns NFS3ErrNotDir if DirHandle is not a directory
//   - Returns NFS3ErrExist if the file already exists
//   - Returns NFS3ErrIO for metadata repository errors
//   - Returns NFS3ErrInval for invalid file types
func (h *DefaultNFSHandler) Mknod(repository metadata.Repository, req *MknodRequest) (*MknodResponse, error) {
	logger.Debug("MKNOD: creating '%s' type=%d in directory %x", req.Name, req.Type, req.DirHandle)

	// Get directory attributes
	dirAttr, err := repository.GetFile(metadata.FileHandle(req.DirHandle))
	if err != nil {
		logger.Warn("Directory not found: %v", err)
		return &MknodResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify it's a directory
	if dirAttr.Type != metadata.FileTypeDirectory {
		logger.Warn("Handle is not a directory")
		return &MknodResponse{Status: NFS3ErrNotDir}, nil
	}

	// Capture pre-operation directory attributes for WCC
	wccDirAttr := captureWccAttr(dirAttr)

	// Check if file already exists
	_, err = repository.GetChild(metadata.FileHandle(req.DirHandle), req.Name)
	if err == nil {
		logger.Warn("File already exists: %s", req.Name)
		return &MknodResponse{
			Status:        NFS3ErrExist,
			DirAttrBefore: wccDirAttr,
		}, nil
	}

	// Validate file type
	var metaType metadata.FileType
	switch req.Type {
	case NF3CHR:
		metaType = metadata.FileTypeChar
	case NF3BLK:
		metaType = metadata.FileTypeBlock
	case NF3SOCK:
		metaType = metadata.FileTypeSocket
	case NF3FIFO:
		metaType = metadata.FileTypeFifo
	default:
		logger.Warn("Invalid file type for MKNOD: %d", req.Type)
		return &MknodResponse{Status: NFS3ErrInval}, nil
	}

	// Create special file attributes
	now := time.Now()
	fileAttr := &metadata.FileAttr{
		Type:      metaType,
		Mode:      0644, // Default mode
		UID:       0,
		GID:       0,
		Size:      0,
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
		ContentID: "",
	}

	// Apply optional attributes
	applySetAttrs(fileAttr, &req.Attr)

	// For device files, store device numbers
	if req.Type == NF3CHR || req.Type == NF3BLK {
		// Device numbers would typically be stored in the metadata
		// This is implementation-specific
		logger.Debug("Device file: major=%d, minor=%d", req.Spec.SpecData1, req.Spec.SpecData2)
	}

	// Add special file to directory
	fileHandle, err := repository.AddFileToDirectory(
		metadata.FileHandle(req.DirHandle),
		req.Name,
		fileAttr,
	)
	if err != nil {
		logger.Error("Failed to create special file: %v", err)
		return &MknodResponse{Status: NFS3ErrIO}, nil
	}

	// Update directory timestamps
	dirAttr.Mtime = now
	dirAttr.Ctime = now
	if err := repository.UpdateFile(metadata.FileHandle(req.DirHandle), dirAttr); err != nil {
		logger.Warn("Failed to update directory: %v", err)
	}

	// Generate response
	fileID := extractFileID(fileHandle)
	nfsFileAttr := MetadataToNFSAttr(fileAttr, fileID)

	dirID := extractFileID(metadata.FileHandle(req.DirHandle))
	nfsDirAttr := MetadataToNFSAttr(dirAttr, dirID)

	logger.Info("MKNOD created: %s (type=%d)", req.Name, req.Type)

	return &MknodResponse{
		Status:        NFS3OK,
		FileHandle:    fileHandle,
		Attr:          nfsFileAttr,
		DirAttrBefore: wccDirAttr,
		DirAttrAfter:  nfsDirAttr,
	}, nil
}

// DecodeMknodRequest decodes an XDR-encoded MKNOD request.
//
// The request format (RFC 1813 Section 3.3.11):
//
//	struct MKNOD3args {
//	    diropargs3   where;
//	    mknoddata3   what;
//	};
func DecodeMknodRequest(data []byte) (*MknodRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	reader := bytes.NewReader(data)

	// Decode directory handle
	dirHandle, err := decodeOpaque(reader)
	if err != nil {
		return nil, fmt.Errorf("decode directory handle: %w", err)
	}

	// Decode filename
	filename, err := decodeString(reader)
	if err != nil {
		return nil, fmt.Errorf("decode filename: %w", err)
	}

	// Decode file type
	var fileType uint32
	if err := binary.Read(reader, binary.BigEndian, &fileType); err != nil {
		return nil, fmt.Errorf("decode file type: %w", err)
	}

	req := &MknodRequest{
		DirHandle: dirHandle,
		Name:      filename,
		Type:      fileType,
	}

	// Decode attributes using the shared helper
	attr, err := decodeSetAttrs(reader)
	if err != nil {
		return nil, fmt.Errorf("decode attributes: %w", err)
	}
	req.Attr = *attr

	// Decode device spec if it's a device file
	if fileType == NF3CHR || fileType == NF3BLK {
		if err := binary.Read(reader, binary.BigEndian, &req.Spec.SpecData1); err != nil {
			return nil, fmt.Errorf("decode spec data1: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &req.Spec.SpecData2); err != nil {
			return nil, fmt.Errorf("decode spec data2: %w", err)
		}
	}

	return req, nil
}

// Encode encodes a MKNOD response to XDR format.
//
// The response format (RFC 1813 Section 3.3.11):
//
//	struct MKNOD3resok {
//	    post_op_fh3   obj;
//	    post_op_attr  obj_attributes;
//	    wcc_data      dir_wcc;
//	};
//
//	struct MKNOD3resfail {
//	    wcc_data      dir_wcc;
//	};
func (resp *MknodResponse) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Encode status
	if err := binary.Write(buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("encode status: %w", err)
	}

	if resp.Status == NFS3OK {
		// Encode post_op_fh3 (optional file handle)
		if err := encodeOptionalOpaque(buf, resp.FileHandle); err != nil {
			return nil, fmt.Errorf("encode file handle: %w", err)
		}

		// Encode post_op_attr (optional attributes)
		if err := encodeOptionalFileAttr(buf, resp.Attr); err != nil {
			return nil, fmt.Errorf("encode file attributes: %w", err)
		}
	}

	// Encode wcc_data for directory (always present)
	if err := encodeWccData(buf, resp.DirAttrBefore, resp.DirAttrAfter); err != nil {
		return nil, fmt.Errorf("encode directory wcc data: %w", err)
	}

	return buf.Bytes(), nil
}
