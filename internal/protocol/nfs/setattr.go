package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// SetAttrRequest represents an NFS SETATTR request (RFC 1813 Section 3.3.2).
// The SETATTR procedure changes one or more attributes of a file system object.
type SetAttrRequest struct {
	// Handle is the file handle of the object
	Handle []byte

	// NewAttr contains the attributes to set
	NewAttr SetAttrs

	// Guard provides a conditional update mechanism
	Guard TimeGuard
}

// TimeGuard is used for conditional updates based on ctime.
// If Check is true and the server's current ctime doesn't match Time,
// the operation fails with NFS3ErrNotSync.
type TimeGuard struct {
	// Check indicates whether to perform guard checking
	Check bool

	// Time is the expected ctime value
	Time TimeVal
}

// SetAttrResponse represents an NFS SETATTR response (RFC 1813 Section 3.3.2).
type SetAttrResponse struct {
	// Status is the NFS status code
	Status uint32

	// AttrBefore contains pre-operation weak cache consistency data
	AttrBefore *WccAttr

	// AttrAfter contains post-operation attributes
	AttrAfter *FileAttr
}

// SetAttr sets the attributes for a file system object.
//
// This implements the NFS SETATTR procedure as defined in RFC 1813 Section 3.3.2.
// It allows changing file attributes like mode, owner, size, and timestamps.
//
// Authentication: This layer performs no authentication. Auth checks should be
// performed by the caller based on the authenticated RPC credentials.
//
// Error Handling:
//   - Returns NFS3ErrNoEnt if the file doesn't exist
//   - Returns NFS3ErrNotSync if guard check fails
//   - Returns NFS3ErrIO for metadata repository errors
func (h *DefaultNFSHandler) SetAttr(repository metadata.Repository, req *SetAttrRequest) (*SetAttrResponse, error) {
	logger.Debug("SETATTR for handle: %x", req.Handle)

	// Get current file attributes
	attr, err := repository.GetFile(metadata.FileHandle(req.Handle))
	if err != nil {
		logger.Warn("File not found: %v", err)
		return &SetAttrResponse{Status: NFS3ErrNoEnt}, nil
	}

	// Capture pre-operation attributes for WCC
	wccAttr := captureWccAttr(attr)

	// Check guard if present (for conditional updates)
	if req.Guard.Check {
		currentCtime := timeToTimeVal(attr.Ctime)
		if currentCtime.Seconds != req.Guard.Time.Seconds ||
			currentCtime.Nseconds != req.Guard.Time.Nseconds {
			logger.Debug("Guard check failed: expected %d.%d, got %d.%d",
				req.Guard.Time.Seconds, req.Guard.Time.Nseconds,
				currentCtime.Seconds, currentCtime.Nseconds)
			return &SetAttrResponse{
				Status:     NFS3ErrNotSync,
				AttrBefore: wccAttr,
			}, nil
		}
	}

	// Apply new attributes
	modified := false

	if req.NewAttr.SetMode {
		attr.Mode = req.NewAttr.Mode
		modified = true
		logger.Debug("Setting mode to %o", req.NewAttr.Mode)
	}

	if req.NewAttr.SetUID {
		attr.UID = req.NewAttr.UID
		modified = true
		logger.Debug("Setting UID to %d", req.NewAttr.UID)
	}

	if req.NewAttr.SetGID {
		attr.GID = req.NewAttr.GID
		modified = true
		logger.Debug("Setting GID to %d", req.NewAttr.GID)
	}

	if req.NewAttr.SetSize {
		// Note: Changing size would require content repository integration
		// For now, we'll just update the metadata
		// TODO: Implement truncation via content repository
		attr.Size = req.NewAttr.Size
		modified = true
		logger.Debug("Setting size to %d", req.NewAttr.Size)
	}

	if req.NewAttr.SetAtime {
		attr.Atime = req.NewAttr.Atime
		modified = true
		logger.Debug("Setting atime to %v", req.NewAttr.Atime)
	}

	if req.NewAttr.SetMtime {
		attr.Mtime = req.NewAttr.Mtime
		modified = true
		logger.Debug("Setting mtime to %v", req.NewAttr.Mtime)
	}

	// Update ctime if anything was modified
	if modified {
		attr.Ctime = time.Now()
	}

	// Save updated attributes
	if err := repository.UpdateFile(metadata.FileHandle(req.Handle), attr); err != nil {
		logger.Error("Failed to update file: %v", err)
		return &SetAttrResponse{Status: NFS3ErrIO}, nil
	}

	// Generate response
	fileid := extractFileID(metadata.FileHandle(req.Handle))
	nfsAttr := MetadataToNFSAttr(attr, fileid)

	logger.Info("SETATTR successful")

	return &SetAttrResponse{
		Status:     NFS3OK,
		AttrBefore: wccAttr,
		AttrAfter:  nfsAttr,
	}, nil
}

// DecodeSetAttrRequest decodes an XDR-encoded SETATTR request.
//
// The request format (RFC 1813 Section 3.3.2):
//
//	struct SETATTR3args {
//	    nfs_fh3      object;
//	    sattr3       new_attributes;
//	    sattrguard3  guard;
//	};
func DecodeSetAttrRequest(data []byte) (*SetAttrRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	reader := bytes.NewReader(data)

	// Decode file handle
	handle, err := decodeOpaque(reader)
	if err != nil {
		return nil, fmt.Errorf("decode handle: %w", err)
	}

	// Decode new attributes using the shared helper
	newAttr, err := decodeSetAttrs(reader)
	if err != nil {
		return nil, fmt.Errorf("decode attributes: %w", err)
	}

	// Decode guard
	guard := TimeGuard{}
	var guardCheck uint32
	if err := binary.Read(reader, binary.BigEndian, &guardCheck); err != nil {
		return nil, fmt.Errorf("decode guard check: %w", err)
	}
	guard.Check = (guardCheck == 1)
	if guard.Check {
		if err := binary.Read(reader, binary.BigEndian, &guard.Time.Seconds); err != nil {
			return nil, fmt.Errorf("decode guard time seconds: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &guard.Time.Nseconds); err != nil {
			return nil, fmt.Errorf("decode guard time nseconds: %w", err)
		}
	}

	return &SetAttrRequest{
		Handle:  handle,
		NewAttr: *newAttr,
		Guard:   guard,
	}, nil
}

// Encode encodes a SETATTR response to XDR format.
//
// The response format (RFC 1813 Section 3.3.2):
//
//	struct SETATTR3resok {
//	    wcc_data  obj_wcc;
//	};
//
//	struct SETATTR3resfail {
//	    wcc_data  obj_wcc;
//	};
func (resp *SetAttrResponse) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Encode status
	if err := binary.Write(buf, binary.BigEndian, resp.Status); err != nil {
		return nil, fmt.Errorf("encode status: %w", err)
	}

	// Encode WCC data (always present, regardless of status)
	if err := encodeWccData(buf, resp.AttrBefore, resp.AttrAfter); err != nil {
		return nil, fmt.Errorf("encode wcc data: %w", err)
	}

	return buf.Bytes(), nil
}
