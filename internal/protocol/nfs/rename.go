package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// RenameRequest represents a RENAME request
type RenameRequest struct {
	FromDirHandle []byte
	FromName      string
	ToDirHandle   []byte
	ToName        string
}

// RenameResponse represents a RENAME response
type RenameResponse struct {
	Status      uint32
	FromDirAttr *WccAttr // Pre-op attributes of source dir
	ToDirAttr   *WccAttr // Pre-op attributes of target dir
}

// Rename renames a file or directory.
// RFC 1813 Section 3.3.14
func (h *DefaultNFSHandler) Rename(repository metadata.Repository, req *RenameRequest) (*RenameResponse, error) {
	logger.Debug("RENAME from '%s' in dir %x to '%s' in dir %x",
		req.FromName, req.FromDirHandle, req.ToName, req.ToDirHandle)

	// Get source directory attributes
	fromDirAttr, err := repository.GetFile(metadata.FileHandle(req.FromDirHandle))
	if err != nil {
		logger.Warn("Source directory not found: %v", err)
		return &RenameResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify source is a directory
	if fromDirAttr.Type != metadata.FileTypeDirectory {
		logger.Warn("Source handle is not a directory")
		return &RenameResponse{Status: NFS3ErrNotDir}, nil
	}

	// Get target directory attributes
	toDirAttr, err := repository.GetFile(metadata.FileHandle(req.ToDirHandle))
	if err != nil {
		logger.Warn("Target directory not found: %v", err)
		return &RenameResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify target is a directory
	if toDirAttr.Type != metadata.FileTypeDirectory {
		logger.Warn("Target handle is not a directory")
		return &RenameResponse{Status: NFS3ErrNotDir}, nil
	}

	// Get the file handle of the source file
	fileHandle, err := repository.GetChild(metadata.FileHandle(req.FromDirHandle), req.FromName)
	if err != nil {
		logger.Debug("Source file '%s' not found: %v", req.FromName, err)
		return &RenameResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Check if target already exists
	_, err = repository.GetChild(metadata.FileHandle(req.ToDirHandle), req.ToName)
	if err == nil {
		// Target exists - remove it first
		logger.Debug("Target file '%s' exists, removing it", req.ToName)
		if err := repository.DeleteChild(metadata.FileHandle(req.ToDirHandle), req.ToName); err != nil {
			logger.Error("Failed to remove target file: %v", err)
			return &RenameResponse{Status: NFS3ErrIO}, nil
		}
	}

	// Remove from source directory
	if err := repository.DeleteChild(metadata.FileHandle(req.FromDirHandle), req.FromName); err != nil {
		logger.Error("Failed to remove from source directory: %v", err)
		return &RenameResponse{Status: NFS3ErrIO}, nil
	}

	// Add to target directory
	if err := repository.AddChild(metadata.FileHandle(req.ToDirHandle), req.ToName, fileHandle); err != nil {
		logger.Error("Failed to add to target directory: %v", err)
		// Try to restore in source directory
		repository.AddChild(metadata.FileHandle(req.FromDirHandle), req.FromName, fileHandle)
		return &RenameResponse{Status: NFS3ErrIO}, nil
	}

	// Update parent if directories are different
	if !bytes.Equal(req.FromDirHandle, req.ToDirHandle) {
		if err := repository.SetParent(fileHandle, metadata.FileHandle(req.ToDirHandle)); err != nil {
			logger.Warn("Failed to update parent: %v", err)
		}
	}

	logger.Info("RENAME successful: '%s' -> '%s'", req.FromName, req.ToName)

	return &RenameResponse{
		Status: NFS3OK,
	}, nil
}

func DecodeRenameRequest(data []byte) (*RenameRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	// Read source directory handle length
	var fromHandleLen uint32
	if err := binary.Read(reader, binary.BigEndian, &fromHandleLen); err != nil {
		return nil, fmt.Errorf("read from handle length: %w", err)
	}

	// Read source directory handle
	fromDirHandle := make([]byte, fromHandleLen)
	if err := binary.Read(reader, binary.BigEndian, &fromDirHandle); err != nil {
		return nil, fmt.Errorf("read from handle: %w", err)
	}

	// Skip padding
	padding := (4 - (fromHandleLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read source filename length
	var fromNameLen uint32
	if err := binary.Read(reader, binary.BigEndian, &fromNameLen); err != nil {
		return nil, fmt.Errorf("read from name length: %w", err)
	}

	// Read source filename
	fromNameBytes := make([]byte, fromNameLen)
	if err := binary.Read(reader, binary.BigEndian, &fromNameBytes); err != nil {
		return nil, fmt.Errorf("read from name: %w", err)
	}

	// Skip padding
	padding = (4 - (fromNameLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read target directory handle length
	var toHandleLen uint32
	if err := binary.Read(reader, binary.BigEndian, &toHandleLen); err != nil {
		return nil, fmt.Errorf("read to handle length: %w", err)
	}

	// Read target directory handle
	toDirHandle := make([]byte, toHandleLen)
	if err := binary.Read(reader, binary.BigEndian, &toDirHandle); err != nil {
		return nil, fmt.Errorf("read to handle: %w", err)
	}

	// Skip padding
	padding = (4 - (toHandleLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read target filename length
	var toNameLen uint32
	if err := binary.Read(reader, binary.BigEndian, &toNameLen); err != nil {
		return nil, fmt.Errorf("read to name length: %w", err)
	}

	// Read target filename
	toNameBytes := make([]byte, toNameLen)
	if err := binary.Read(reader, binary.BigEndian, &toNameBytes); err != nil {
		return nil, fmt.Errorf("read to name: %w", err)
	}

	return &RenameRequest{
		FromDirHandle: fromDirHandle,
		FromName:      string(fromNameBytes),
		ToDirHandle:   toDirHandle,
		ToName:        string(toNameBytes),
	}, nil
}

func (resp *RenameResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	// Write WCC data for source directory (we'll skip for now - optional)
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}

	// Write WCC data for target directory (we'll skip for now - optional)
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
