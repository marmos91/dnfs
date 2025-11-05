package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// MkdirRequest represents a MKDIR request
type MkdirRequest struct {
	DirHandle []byte
	Name      string
	Attr      SetAttrs
}

// MkdirResponse represents a MKDIR response
type MkdirResponse struct {
	Status  uint32
	Handle  []byte    // File handle of new directory (optional)
	Attr    *FileAttr // Attributes of new directory (optional)
	DirAttr *WccAttr  // Pre-op and post-op attributes of parent (optional)
}

// Mkdir creates a new directory.
// RFC 1813 Section 3.3.9
func (h *DefaultNFSHandler) Mkdir(repository metadata.Repository, req *MkdirRequest) (*MkdirResponse, error) {
	logger.Debug("MKDIR '%s' in directory %x, mode=%o", req.Name, req.DirHandle, req.Attr.Mode)

	// Get parent directory attributes
	parentAttr, err := repository.GetFile(metadata.FileHandle(req.DirHandle))
	if err != nil {
		logger.Warn("Parent directory not found: %v", err)
		return &MkdirResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify parent is a directory
	if parentAttr.Type != metadata.FileTypeDirectory {
		logger.Warn("Parent handle is not a directory")
		return &MkdirResponse{Status: NFS3ErrNotDir}, nil
	}

	// Check if directory already exists
	_, err = repository.GetChild(metadata.FileHandle(req.DirHandle), req.Name)
	if err == nil {
		logger.Debug("Directory '%s' already exists", req.Name)
		return &MkdirResponse{Status: NFS3ErrExist}, nil
	}

	// Create directory attributes
	now := time.Now()
	mode := uint32(0755) // Default directory permissions
	if req.Attr.SetMode {
		mode = req.Attr.Mode
	}

	uid := uint32(0)
	if req.Attr.SetUID {
		uid = req.Attr.UID
	}

	gid := uint32(0)
	if req.Attr.SetGID {
		gid = req.Attr.GID
	}

	dirAttr := &metadata.FileAttr{
		Type:      metadata.FileTypeDirectory,
		Mode:      mode,
		UID:       uid,
		GID:       gid,
		Size:      4096, // Standard directory size
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
		ContentID: "", // Directories don't have content
	}

	// Use repository's AddFileToDirectory helper
	newHandle, err := repository.(interface {
		AddFileToDirectory(metadata.FileHandle, string, *metadata.FileAttr) (metadata.FileHandle, error)
	}).AddFileToDirectory(metadata.FileHandle(req.DirHandle), req.Name, dirAttr)

	if err != nil {
		logger.Error("Failed to create directory: %v", err)
		return &MkdirResponse{Status: NFS3ErrIO}, nil
	}

	// Generate file ID
	fileid := binary.BigEndian.Uint64(newHandle[:8])
	nfsAttr := MetadataToNFSAttr(dirAttr, fileid)

	logger.Info("MKDIR successful: '%s' -> handle %x", req.Name, newHandle)

	return &MkdirResponse{
		Status: NFS3OK,
		Handle: newHandle,
		Attr:   nfsAttr,
	}, nil
}

func DecodeMkdirRequest(data []byte) (*MkdirRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short")
	}

	reader := bytes.NewReader(data)

	// Read directory handle length
	var handleLen uint32
	if err := binary.Read(reader, binary.BigEndian, &handleLen); err != nil {
		return nil, fmt.Errorf("read handle length: %w", err)
	}

	// Read directory handle
	dirHandle := make([]byte, handleLen)
	if err := binary.Read(reader, binary.BigEndian, &dirHandle); err != nil {
		return nil, fmt.Errorf("read handle: %w", err)
	}

	// Skip padding
	padding := (4 - (handleLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read directory name length
	var nameLen uint32
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return nil, fmt.Errorf("read name length: %w", err)
	}

	// Read directory name
	nameBytes := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &nameBytes); err != nil {
		return nil, fmt.Errorf("read name: %w", err)
	}

	// Skip padding
	padding = (4 - (nameLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read attributes (same format as SETATTR)
	attr := SetAttrs{}

	// Mode
	var setMode uint32
	if err := binary.Read(reader, binary.BigEndian, &setMode); err != nil {
		return nil, fmt.Errorf("read set_mode: %w", err)
	}
	attr.SetMode = (setMode == 1)
	if attr.SetMode {
		if err := binary.Read(reader, binary.BigEndian, &attr.Mode); err != nil {
			return nil, fmt.Errorf("read mode: %w", err)
		}
	}

	// UID
	var setUID uint32
	if err := binary.Read(reader, binary.BigEndian, &setUID); err != nil {
		return nil, fmt.Errorf("read set_uid: %w", err)
	}
	attr.SetUID = (setUID == 1)
	if attr.SetUID {
		if err := binary.Read(reader, binary.BigEndian, &attr.UID); err != nil {
			return nil, fmt.Errorf("read uid: %w", err)
		}
	}

	// GID
	var setGID uint32
	if err := binary.Read(reader, binary.BigEndian, &setGID); err != nil {
		return nil, fmt.Errorf("read set_gid: %w", err)
	}
	attr.SetGID = (setGID == 1)
	if attr.SetGID {
		if err := binary.Read(reader, binary.BigEndian, &attr.GID); err != nil {
			return nil, fmt.Errorf("read gid: %w", err)
		}
	}

	// Size (skip - not used for directories)
	var setSize uint32
	if err := binary.Read(reader, binary.BigEndian, &setSize); err != nil {
		return nil, fmt.Errorf("read set_size: %w", err)
	}
	if setSize == 1 {
		var size uint64
		binary.Read(reader, binary.BigEndian, &size)
	}

	// Atime (skip)
	var setAtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setAtime); err != nil {
		return nil, fmt.Errorf("read set_atime: %w", err)
	}
	if setAtime == 1 {
		var seconds, nseconds uint32
		binary.Read(reader, binary.BigEndian, &seconds)
		binary.Read(reader, binary.BigEndian, &nseconds)
	}

	// Mtime (skip)
	var setMtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setMtime); err != nil {
		return nil, fmt.Errorf("read set_mtime: %w", err)
	}
	if setMtime == 1 {
		var seconds, nseconds uint32
		binary.Read(reader, binary.BigEndian, &seconds)
		binary.Read(reader, binary.BigEndian, &nseconds)
	}

	return &MkdirRequest{
		DirHandle: dirHandle,
		Name:      string(nameBytes),
		Attr:      attr,
	}, nil
}

func (resp *MkdirResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	if resp.Status != NFS3OK {
		// Write empty WCC data
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// Write file handle (optional)
	if resp.Handle != nil {
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}

		handleLen := uint32(len(resp.Handle))
		if err := binary.Write(&buf, binary.BigEndian, handleLen); err != nil {
			return nil, err
		}
		buf.Write(resp.Handle)

		// Add padding
		padding := (4 - (handleLen % 4)) % 4
		for i := uint32(0); i < padding; i++ {
			buf.WriteByte(0)
		}
	} else {
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
	}

	// Write post-op attributes (optional)
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

	// Write WCC data (pre-op and post-op attributes - optional, we'll skip for now)
	// Pre-op attributes
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	// Post-op attributes
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
