package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// SymlinkRequest represents a SYMLINK request
type SymlinkRequest struct {
	DirHandle []byte
	Name      string
	Target    string   // The symbolic link target path
	Attr      SetAttrs // Attributes for the new symlink
}

// SymlinkResponse represents a SYMLINK response
type SymlinkResponse struct {
	Status        uint32
	FileHandle    []byte    // Handle of created symlink (optional)
	Attr          *FileAttr // Post-op attributes of symlink (optional)
	DirAttrBefore *WccAttr  // Pre-op dir attributes (optional)
	DirAttrAfter  *FileAttr // Post-op dir attributes (optional)
}

// Symlink creates a symbolic link.
// RFC 1813 Section 3.3.10
func (h *DefaultNFSHandler) Symlink(repository metadata.Repository, req *SymlinkRequest) (*SymlinkResponse, error) {
	logger.Debug("SYMLINK: creating '%s' -> '%s' in directory %x", req.Name, req.Target, req.DirHandle)

	// Get directory attributes
	dirAttr, err := repository.GetFile(metadata.FileHandle(req.DirHandle))
	if err != nil {
		logger.Warn("Directory not found: %v", err)
		return &SymlinkResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Verify it's a directory
	if dirAttr.Type != metadata.FileTypeDirectory {
		logger.Warn("Handle is not a directory")
		return &SymlinkResponse{Status: NFS3ErrNotDir}, nil
	}

	// Store pre-op dir attributes for WCC
	dirFileid := binary.BigEndian.Uint64(req.DirHandle[:8])
	wccDirAttr := &WccAttr{
		Size: dirAttr.Size,
		Mtime: TimeVal{
			Seconds:  uint32(dirAttr.Mtime.Unix()),
			Nseconds: uint32(dirAttr.Mtime.Nanosecond()),
		},
		Ctime: TimeVal{
			Seconds:  uint32(dirAttr.Ctime.Unix()),
			Nseconds: uint32(dirAttr.Ctime.Nanosecond()),
		},
	}

	// Check if file already exists
	_, err = repository.GetChild(metadata.FileHandle(req.DirHandle), req.Name)
	if err == nil {
		logger.Warn("File already exists: %s", req.Name)
		return &SymlinkResponse{
			Status:        NFS3ErrExist,
			DirAttrBefore: wccDirAttr,
		}, nil
	}

	// Create symlink attributes
	now := time.Now()
	symlinkAttr := &metadata.FileAttr{
		Type:          metadata.FileTypeSymlink,
		Mode:          0777, // Symlinks typically have 777 permissions
		UID:           0,
		GID:           0,
		Size:          uint64(len(req.Target)), // Size is the length of the target path
		Atime:         now,
		Mtime:         now,
		Ctime:         now,
		ContentID:     "",
		SymlinkTarget: req.Target, // Store the target path
	}

	// Apply optional attributes if set
	if req.Attr.SetMode {
		symlinkAttr.Mode = req.Attr.Mode
	}
	if req.Attr.SetUID {
		symlinkAttr.UID = req.Attr.UID
	}
	if req.Attr.SetGID {
		symlinkAttr.GID = req.Attr.GID
	}

	// Add symlink to directory
	symlinkHandle, err := repository.AddFileToDirectory(
		metadata.FileHandle(req.DirHandle),
		req.Name,
		symlinkAttr,
	)
	if err != nil {
		logger.Error("Failed to create symlink: %v", err)
		return &SymlinkResponse{Status: NFS3ErrIO}, nil
	}

	// Update directory mtime
	dirAttr.Mtime = now
	dirAttr.Ctime = now
	if err := repository.UpdateFile(metadata.FileHandle(req.DirHandle), dirAttr); err != nil {
		logger.Error("Failed to update directory: %v", err)
	}

	// Generate response
	symlinkFileid := binary.BigEndian.Uint64(symlinkHandle[:8])
	nfsSymlinkAttr := MetadataToNFSAttr(symlinkAttr, symlinkFileid)
	nfsDirAttr := MetadataToNFSAttr(dirAttr, dirFileid)

	logger.Info("SYMLINK created: %s -> %s", req.Name, req.Target)

	return &SymlinkResponse{
		Status:        NFS3OK,
		FileHandle:    symlinkHandle,
		Attr:          nfsSymlinkAttr,
		DirAttrBefore: wccDirAttr,
		DirAttrAfter:  nfsDirAttr,
	}, nil
}

func DecodeSymlinkRequest(data []byte) (*SymlinkRequest, error) {
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

	// Read symlink name
	var nameLen uint32
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return nil, fmt.Errorf("read name length: %w", err)
	}

	nameBytes := make([]byte, nameLen)
	if err := binary.Read(reader, binary.BigEndian, &nameBytes); err != nil {
		return nil, fmt.Errorf("read name: %w", err)
	}

	// Skip padding
	padding = (4 - (nameLen % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		reader.ReadByte()
	}

	// Read attributes (sattr3)
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

	// Size (usually not set for symlinks)
	var setSize uint32
	if err := binary.Read(reader, binary.BigEndian, &setSize); err != nil {
		return nil, fmt.Errorf("read set_size: %w", err)
	}
	attr.SetSize = (setSize == 1)
	if attr.SetSize {
		if err := binary.Read(reader, binary.BigEndian, &attr.Size); err != nil {
			return nil, fmt.Errorf("read size: %w", err)
		}
	}

	// Atime
	var setAtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setAtime); err != nil {
		return nil, fmt.Errorf("read set_atime: %w", err)
	}
	if setAtime == 1 {
		attr.SetAtime = true
		if err := binary.Read(reader, binary.BigEndian, &attr.Atime.Seconds); err != nil {
			return nil, fmt.Errorf("read atime seconds: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &attr.Atime.Nseconds); err != nil {
			return nil, fmt.Errorf("read atime nseconds: %w", err)
		}
	} else if setAtime == 2 {
		// SET_TO_SERVER_TIME
		attr.SetAtime = true
		now := time.Now()
		attr.Atime = TimeVal{
			Seconds:  uint32(now.Unix()),
			Nseconds: uint32(now.Nanosecond()),
		}
	}

	// Mtime
	var setMtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setMtime); err != nil {
		return nil, fmt.Errorf("read set_mtime: %w", err)
	}
	if setMtime == 1 {
		attr.SetMtime = true
		if err := binary.Read(reader, binary.BigEndian, &attr.Mtime.Seconds); err != nil {
			return nil, fmt.Errorf("read mtime seconds: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &attr.Mtime.Nseconds); err != nil {
			return nil, fmt.Errorf("read mtime nseconds: %w", err)
		}
	} else if setMtime == 2 {
		// SET_TO_SERVER_TIME
		attr.SetMtime = true
		now := time.Now()
		attr.Mtime = TimeVal{
			Seconds:  uint32(now.Unix()),
			Nseconds: uint32(now.Nanosecond()),
		}
	}

	// Read target path
	var targetLen uint32
	if err := binary.Read(reader, binary.BigEndian, &targetLen); err != nil {
		return nil, fmt.Errorf("read target length: %w", err)
	}

	targetBytes := make([]byte, targetLen)
	if err := binary.Read(reader, binary.BigEndian, &targetBytes); err != nil {
		return nil, fmt.Errorf("read target: %w", err)
	}

	return &SymlinkRequest{
		DirHandle: dirHandle,
		Name:      string(nameBytes),
		Target:    string(targetBytes),
		Attr:      attr,
	}, nil
}

func (resp *SymlinkResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write status
	if err := binary.Write(&buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("write status: %w", err)
	}

	if resp.Status != NFS3OK {
		// Write WCC data even on failure
		if resp.DirAttrBefore != nil {
			if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Size); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Mtime.Seconds); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Mtime.Nseconds); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Ctime.Seconds); err != nil {
				return nil, err
			}
			if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Ctime.Nseconds); err != nil {
				return nil, err
			}
		} else {
			if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
				return nil, err
			}
		}

		// Post-op dir attributes
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	// Write post-op file handle
	if resp.FileHandle != nil {
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}
		handleLen := uint32(len(resp.FileHandle))
		if err := binary.Write(&buf, binary.BigEndian, handleLen); err != nil {
			return nil, err
		}
		buf.Write(resp.FileHandle)
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

	// Write WCC data for directory
	// Pre-op attributes
	if resp.DirAttrBefore != nil {
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Size); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Mtime.Seconds); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Mtime.Nseconds); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Ctime.Seconds); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.BigEndian, resp.DirAttrBefore.Ctime.Nseconds); err != nil {
			return nil, err
		}
	} else {
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
	}

	// Post-op dir attributes
	if resp.DirAttrAfter != nil {
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}
		if err := encodeFileAttr(&buf, resp.DirAttrAfter); err != nil {
			return nil, err
		}
	} else {
		if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
