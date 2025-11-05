package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// ReadLinkRequest represents a READLINK request
type ReadLinkRequest struct {
	Handle []byte
}

// ReadLinkResponse represents a READLINK response
type ReadLinkResponse struct {
	Status uint32
	Attr   *FileAttr // Post-op attributes (optional)
	Target string    // The symbolic link target path
}

// ReadLink reads the data associated with a symbolic link.
// RFC 1813 Section 3.3.5
func (h *DefaultNFSHandler) ReadLink(repository metadata.Repository, req *ReadLinkRequest) (*ReadLinkResponse, error) {
	logger.Debug("READLINK for handle: %x", req.Handle)

	// Get file attributes
	attr, err := repository.GetFile(metadata.FileHandle(req.Handle))
	if err != nil {
		logger.Warn("File not found: %v", err)
		return &ReadLinkResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify it's a symlink
	if attr.Type != metadata.FileTypeSymlink {
		logger.Warn("Handle is not a symlink, type=%d", attr.Type)
		return &ReadLinkResponse{Status: NFS3ErrInval}, nil
	}

	// Generate file ID
	fileid := binary.BigEndian.Uint64(req.Handle[:8])
	nfsAttr := MetadataToNFSAttr(attr, fileid)

	// Get the symlink target
	target := attr.SymlinkTarget
	if target == "" {
		logger.Warn("Symlink has no target")
		return &ReadLinkResponse{Status: NFS3ErrIO}, nil
	}

	logger.Info("READLINK successful: target=%s", target)

	return &ReadLinkResponse{
		Status: NFS3OK,
		Attr:   nfsAttr,
		Target: target,
	}, nil
}

func DecodeReadLinkRequest(data []byte) (*ReadLinkRequest, error) {
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

	return &ReadLinkRequest{Handle: handle}, nil
}

func (resp *ReadLinkResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	// If status is not OK, return just the status
	if resp.Status != NFS3OK {
		// Write empty post-op attributes
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
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

	// Write target path as a string (length + data + padding)
	targetLen := uint32(len(resp.Target))
	if err := binary.Write(&buf, binary.BigEndian, targetLen); err != nil {
		return nil, fmt.Errorf("write target length: %w", err)
	}

	buf.Write([]byte(resp.Target))

	// Add padding to 4-byte boundary
	padding := (4 - (targetLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		buf.WriteByte(0)
	}

	return buf.Bytes(), nil
}
