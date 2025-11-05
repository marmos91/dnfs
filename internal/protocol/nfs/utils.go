package nfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/marmos91/dittofs/internal/metadata"
)

// Helper to convert metadata.FileAttr to NFS FileAttr
func MetadataToNFSAttr(mdAttr *metadata.FileAttr, fileid uint64) *FileAttr {
	return &FileAttr{
		Type:  uint32(mdAttr.Type),
		Mode:  mdAttr.Mode,
		Nlink: 1,
		UID:   mdAttr.UID,
		GID:   mdAttr.GID,
		Size:  mdAttr.Size,
		Used:  mdAttr.Size,
		Rdev: SpecData{
			Major: 0,
			Minor: 0,
		},
		Fsid:   0,
		Fileid: fileid,
		Atime: TimeVal{
			Seconds:  uint32(mdAttr.Atime.Unix()),
			Nseconds: uint32(mdAttr.Atime.Nanosecond()),
		},
		Mtime: TimeVal{
			Seconds:  uint32(mdAttr.Mtime.Unix()),
			Nseconds: uint32(mdAttr.Mtime.Nanosecond()),
		},
		Ctime: TimeVal{
			Seconds:  uint32(mdAttr.Ctime.Unix()),
			Nseconds: uint32(mdAttr.Ctime.Nanosecond()),
		},
	}
}

// captureWccAttr captures weak cache consistency attributes from file metadata.
// This is used to provide before/after snapshots for NFS clients to detect changes.
func captureWccAttr(attr *metadata.FileAttr) *WccAttr {
	if attr == nil {
		return nil
	}

	return &WccAttr{
		Size: attr.Size,
		Mtime: TimeVal{
			Seconds:  uint32(attr.Mtime.Unix()),
			Nseconds: uint32(attr.Mtime.Nanosecond()),
		},
		Ctime: TimeVal{
			Seconds:  uint32(attr.Ctime.Unix()),
			Nseconds: uint32(attr.Ctime.Nanosecond()),
		},
	}
}

// extractFileID extracts a file ID from a file handle.
// Uses the first 8 bytes of the handle as the file ID.
func extractFileID(handle metadata.FileHandle) uint64 {
	if len(handle) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(handle[:8])
}

// applySetAttrs applies set attributes to file metadata.
// Only applies attributes that are explicitly set in the request.
func applySetAttrs(fileAttr *metadata.FileAttr, setAttrs *SetAttrs) {
	if setAttrs == nil {
		return
	}

	if setAttrs.SetMode {
		fileAttr.Mode = setAttrs.Mode
	}

	if setAttrs.SetUID {
		fileAttr.UID = setAttrs.UID
	}

	if setAttrs.SetGID {
		fileAttr.GID = setAttrs.GID
	}

	if setAttrs.SetSize {
		fileAttr.Size = setAttrs.Size
	}

	if setAttrs.SetAtime {
		fileAttr.Atime = setAttrs.Atime
	}

	if setAttrs.SetMtime {
		fileAttr.Mtime = setAttrs.Mtime
	}
}

// ============================================================================
// XDR Decoding Helpers
// ============================================================================

// decodeOpaque decodes XDR opaque data (variable-length byte array).
// Format: [length:uint32][data:bytes][padding:0-3 bytes]
func decodeOpaque(reader io.Reader) ([]byte, error) {
	// Read length
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}

	// Read data
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	// Skip padding (align to 4-byte boundary)
	padding := (4 - (length % 4)) % 4
	if padding > 0 {
		if _, err := io.CopyN(io.Discard, reader, int64(padding)); err != nil {
			return nil, fmt.Errorf("skip padding: %w", err)
		}
	}

	return data, nil
}

// decodeString decodes XDR string (same as opaque but returns string).
// Format: [length:uint32][data:bytes][padding:0-3 bytes]
func decodeString(reader io.Reader) (string, error) {
	data, err := decodeOpaque(reader)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// decodeSetAttrs decodes NFS sattr3 (set attributes) structure.
// Format per RFC 1813 Section 2.5.3:
//
//	struct sattr3 {
//	    set_mode3   mode;
//	    set_uid3    uid;
//	    set_gid3    gid;
//	    set_size3   size;
//	    set_atime   atime;
//	    set_mtime   mtime;
//	};
func decodeSetAttrs(reader io.Reader) (*SetAttrs, error) {
	attr := &SetAttrs{}

	// Decode mode
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

	// Decode UID
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

	// Decode GID
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

	// Decode Size
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

	// Decode Atime
	var setAtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setAtime); err != nil {
		return nil, fmt.Errorf("read set_atime: %w", err)
	}
	if setAtime == 1 { // SET_TO_CLIENT_TIME
		attr.SetAtime = true
		var seconds, nseconds uint32
		if err := binary.Read(reader, binary.BigEndian, &seconds); err != nil {
			return nil, fmt.Errorf("read atime seconds: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &nseconds); err != nil {
			return nil, fmt.Errorf("read atime nseconds: %w", err)
		}
		attr.Atime = timeValToTime(seconds, nseconds)
	} else if setAtime == 2 { // SET_TO_SERVER_TIME
		attr.SetAtime = true
		attr.Atime = getCurrentTime()
	}

	// Decode Mtime
	var setMtime uint32
	if err := binary.Read(reader, binary.BigEndian, &setMtime); err != nil {
		return nil, fmt.Errorf("read set_mtime: %w", err)
	}
	if setMtime == 1 { // SET_TO_CLIENT_TIME
		attr.SetMtime = true
		var seconds, nseconds uint32
		if err := binary.Read(reader, binary.BigEndian, &seconds); err != nil {
			return nil, fmt.Errorf("read mtime seconds: %w", err)
		}
		if err := binary.Read(reader, binary.BigEndian, &nseconds); err != nil {
			return nil, fmt.Errorf("read mtime nseconds: %w", err)
		}
		attr.Mtime = timeValToTime(seconds, nseconds)
	} else if setMtime == 2 { // SET_TO_SERVER_TIME
		attr.SetMtime = true
		attr.Mtime = getCurrentTime()
	}

	return attr, nil
}

// ============================================================================
// XDR Encoding Helpers
// ============================================================================

// encodeOptionalOpaque encodes optional XDR opaque data.
// Format: [present:uint32][length:uint32][data:bytes][padding:0-3 bytes]
// If data is nil or empty, only writes present=0.
func encodeOptionalOpaque(buf *bytes.Buffer, data []byte) error {
	if len(data) == 0 {
		// Not present
		return binary.Write(buf, binary.BigEndian, uint32(0))
	}

	// Present flag
	if err := binary.Write(buf, binary.BigEndian, uint32(1)); err != nil {
		return fmt.Errorf("write present flag: %w", err)
	}

	// Length
	length := uint32(len(data))
	if err := binary.Write(buf, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	// Data
	if _, err := buf.Write(data); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	// Padding
	padding := (4 - (length % 4)) % 4
	for i := uint32(0); i < padding; i++ {
		if err := buf.WriteByte(0); err != nil {
			return fmt.Errorf("write padding: %w", err)
		}
	}

	return nil
}

// encodeOptionalFileAttr encodes optional NFS file attributes.
// Format: [present:uint32][fattr3]
func encodeOptionalFileAttr(buf *bytes.Buffer, attr *FileAttr) error {
	if attr == nil {
		// Not present
		return binary.Write(buf, binary.BigEndian, uint32(0))
	}

	// Present flag
	if err := binary.Write(buf, binary.BigEndian, uint32(1)); err != nil {
		return fmt.Errorf("write present flag: %w", err)
	}

	// Encode file attributes
	return encodeFileAttr(buf, attr)
}

// encodeWccData encodes weak cache consistency data.
// Format per RFC 1813 Section 2.6:
//
//	struct wcc_data {
//	    pre_op_attr   before;
//	    post_op_attr  after;
//	};
func encodeWccData(buf *bytes.Buffer, before *WccAttr, after *FileAttr) error {
	// Encode pre-op attributes
	if before != nil {
		if err := binary.Write(buf, binary.BigEndian, uint32(1)); err != nil {
			return fmt.Errorf("write before present: %w", err)
		}
		if err := encodeWccAttr(buf, before); err != nil {
			return fmt.Errorf("encode before attributes: %w", err)
		}
	} else {
		if err := binary.Write(buf, binary.BigEndian, uint32(0)); err != nil {
			return fmt.Errorf("write before not present: %w", err)
		}
	}

	// Encode post-op attributes
	if err := encodeOptionalFileAttr(buf, after); err != nil {
		return fmt.Errorf("encode after attributes: %w", err)
	}

	return nil
}

// encodeWccAttr encodes pre-operation WCC attributes.
// Format: [size:uint64][mtime:nfstime3][ctime:nfstime3]
func encodeWccAttr(buf *bytes.Buffer, attr *WccAttr) error {
	if err := binary.Write(buf, binary.BigEndian, attr.Size); err != nil {
		return fmt.Errorf("write size: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, attr.Mtime.Seconds); err != nil {
		return fmt.Errorf("write mtime seconds: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, attr.Mtime.Nseconds); err != nil {
		return fmt.Errorf("write mtime nseconds: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, attr.Ctime.Seconds); err != nil {
		return fmt.Errorf("write ctime seconds: %w", err)
	}

	if err := binary.Write(buf, binary.BigEndian, attr.Ctime.Nseconds); err != nil {
		return fmt.Errorf("write ctime nseconds: %w", err)
	}

	return nil
}

// ============================================================================
// Time Conversion Helpers
// ============================================================================

// timeValToTime converts NFS time to Go time.Time.
func timeValToTime(seconds, nseconds uint32) time.Time {
	return time.Unix(int64(seconds), int64(nseconds))
}

// getCurrentTime returns the current time.
// Separated into a function for easier testing/mocking.
func getCurrentTime() time.Time {
	return time.Now()
}

func timeToTimeVal(t time.Time) TimeVal {
	return TimeVal{
		Seconds:  uint32(t.Unix()),
		Nseconds: uint32(t.Nanosecond()),
	}
}
