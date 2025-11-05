package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// AccessRequest represents an ACCESS request
type AccessRequest struct {
	Handle []byte
	Access uint32 // Bitmap of desired access permissions
}

// AccessResponse represents an ACCESS response
type AccessResponse struct {
	Status uint32
	Attr   *FileAttr // Post-op attributes (optional)
	Access uint32    // Bitmap of granted access permissions
}

// Access checks access permissions for a file system object.
// RFC 1813 Section 3.3.4
func (h *DefaultNFSHandler) Access(repository metadata.Repository, req *AccessRequest) (*AccessResponse, error) {
	logger.Debug("ACCESS for handle: %x, requested access: 0x%x", req.Handle, req.Access)

	// Get file attributes
	attr, err := repository.GetFile(metadata.FileHandle(req.Handle))
	if err != nil {
		logger.Warn("File not found: %v", err)
		return &AccessResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Generate file ID
	fileid := binary.BigEndian.Uint64(req.Handle[:8])
	nfsAttr := MetadataToNFSAttr(attr, fileid)

	// For simplicity, grant all requested permissions
	// In a real implementation, you would check user/group/permissions
	grantedAccess := req.Access

	// If it's a directory, ensure lookup, modify, extend and delete are granted
	if attr.Type == metadata.FileTypeDirectory {
		grantedAccess |= AccessLookup | AccessModify | AccessExtend | AccessDelete
	}

	// If it's a regular file, ensure read, modify, and extend are granted
	if attr.Type == metadata.FileTypeRegular {
		grantedAccess |= AccessRead | AccessModify | AccessExtend
	}

	logger.Debug("ACCESS granted: 0x%x (requested: 0x%x)", grantedAccess, req.Access)

	return &AccessResponse{
		Status: NFS3OK,
		Attr:   nfsAttr,
		Access: grantedAccess,
	}, nil
}

func DecodeAccessRequest(data []byte) (*AccessRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	// Read handle length
	var handleLen uint32
	if err := binary.Read(reader, binary.BigEndian, &handleLen); err != nil {
		return nil, fmt.Errorf("read handle length: %w", err)
	}

	// Read handle
	handle := make([]byte, handleLen)
	if err := binary.Read(reader, binary.BigEndian, &handle); err != nil {
		return nil, fmt.Errorf("read handle: %w", err)
	}

	// Skip padding
	padding := (4 - (handleLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read access bitmap
	var access uint32
	if err := binary.Read(reader, binary.BigEndian, &access); err != nil {
		return nil, fmt.Errorf("read access: %w", err)
	}

	return &AccessRequest{
		Handle: handle,
		Access: access,
	}, nil
}

func (resp *AccessResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	// If status is not OK, return just the status
	if resp.Status != NFS3OK {
		return buf.Bytes(), nil
	}

	// Write post-op attributes
	if resp.Attr != nil {
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}
		if err := encodeFileAttr(&buf, resp.Attr); err != nil {
			return nil, err
		}
	} else {
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
	}

	// Write granted access
	if err := binary.Write(&buf, binary.BigEndian, resp.Access); err != nil {
		return nil, fmt.Errorf("write access: %w", err)
	}

	return buf.Bytes(), nil
}
