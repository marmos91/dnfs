package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// Device types for MKNOD
const (
	NF3REG  = 1 // Regular file
	NF3DIR  = 2 // Directory
	NF3BLK  = 3 // Block special device
	NF3CHR  = 4 // Character special device
	NF3LNK  = 5 // Symbolic link
	NF3SOCK = 6 // Socket
	NF3FIFO = 7 // Named pipe (FIFO)
)

// MknodRequest represents a MKNOD request
type MknodRequest struct {
	DirHandle []byte
	Name      string
	Type      uint32   // File type (NF3CHR, NF3BLK, NF3SOCK, NF3FIFO)
	Attr      SetAttrs // Attributes for the new device
	Spec      DeviceSpec
}

// DeviceSpec contains device-specific data
type DeviceSpec struct {
	SpecData1 uint32 // For devices: major number
	SpecData2 uint32 // For devices: minor number
}

// MknodResponse represents a MKNOD response
type MknodResponse struct {
	Status        uint32
	FileHandle    []byte    // Handle of created device (optional)
	Attr          *FileAttr // Post-op attributes of device (optional)
	DirAttrBefore *WccAttr  // Pre-op dir attributes (optional)
	DirAttrAfter  *FileAttr // Post-op dir attributes (optional)
}

// Mknod creates a special device file.
// RFC 1813 Section 3.3.11
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
		return &MknodResponse{
			Status:        NFS3ErrExist,
			DirAttrBefore: wccDirAttr,
		}, nil
	}

	// Validate type - we only support FIFO and SOCK for now
	// Block and character devices would require kernel-level support
	var fileType metadata.FileType
	switch req.Type {
	case NF3FIFO:
		fileType = metadata.FileTypeFifo
	case NF3SOCK:
		fileType = metadata.FileTypeSocket
	case NF3CHR, NF3BLK:
		// Character and block devices require special kernel support
		logger.Warn("Character and block devices not supported")
		return &MknodResponse{
			Status:        NFS3ErrNotSupp,
			DirAttrBefore: wccDirAttr,
		}, nil
	default:
		logger.Warn("Invalid file type for MKNOD: %d", req.Type)
		return &MknodResponse{
			Status:        NFS3ErrInval,
			DirAttrBefore: wccDirAttr,
		}, nil
	}

	// Create device attributes
	now := time.Now()
	deviceAttr := &metadata.FileAttr{
		Type:      fileType,
		Mode:      0666,
		UID:       0,
		GID:       0,
		Size:      0, // Special files have zero size
		Atime:     now,
		Mtime:     now,
		Ctime:     now,
		ContentID: "",
	}

	// Apply optional attributes if set
	if req.Attr.SetMode {
		deviceAttr.Mode = req.Attr.Mode
	}
	if req.Attr.SetUID {
		deviceAttr.UID = req.Attr.UID
	}
	if req.Attr.SetGID {
		deviceAttr.GID = req.Attr.GID
	}

	// Add device to directory
	deviceHandle, err := repository.AddFileToDirectory(
		metadata.FileHandle(req.DirHandle),
		req.Name,
		deviceAttr,
	)
	if err != nil {
		logger.Error("Failed to create device: %v", err)
		return &MknodResponse{Status: NFS3ErrIO}, nil
	}

	// Update directory mtime
	dirAttr.Mtime = now
	dirAttr.Ctime = now
	if err := repository.UpdateFile(metadata.FileHandle(req.DirHandle), dirAttr); err != nil {
		logger.Error("Failed to update directory: %v", err)
	}

	// Generate response
	deviceFileid := binary.BigEndian.Uint64(deviceHandle[:8])
	nfsDeviceAttr := MetadataToNFSAttr(deviceAttr, deviceFileid)
	nfsDirAttr := MetadataToNFSAttr(dirAttr, dirFileid)

	logger.Info("MKNOD created: %s (type=%d)", req.Name, req.Type)

	return &MknodResponse{
		Status:        NFS3OK,
		FileHandle:    deviceHandle,
		Attr:          nfsDeviceAttr,
		DirAttrBefore: wccDirAttr,
		DirAttrAfter:  nfsDirAttr,
	}, nil
}

func DecodeMknodRequest(data []byte) (*MknodRequest, error) {
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

	// Read name
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

	// Read file type
	var fileType uint32
	if err := binary.Read(reader, binary.BigEndian, &fileType); err != nil {
		return nil, fmt.Errorf("read file type: %w", err)
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

	// Size
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
		attr.SetMtime = true
		now := time.Now()
		attr.Mtime = TimeVal{
			Seconds:  uint32(now.Unix()),
			Nseconds: uint32(now.Nanosecond()),
		}
	}

	// Read device spec (for CHR and BLK types)
	spec := DeviceSpec{}
	if fileType == NF3CHR || fileType == NF3BLK {
		if err := binary.Read(reader, binary.BigEndian, &spec.SpecData1); err != nil {
			return nil, fmt.Errorf("read spec data1: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &spec.SpecData2); err != nil {
			return nil, fmt.Errorf("read spec data2: %w", err)
		}
	}

	return &MknodRequest{
		DirHandle: dirHandle,
		Name:      string(nameBytes),
		Type:      fileType,
		Attr:      attr,
		Spec:      spec,
	}, nil
}

func (resp *MknodResponse) Encode() ([]byte, error) {
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
