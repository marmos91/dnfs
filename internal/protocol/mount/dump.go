package mount

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// DumpRequest represents a DUMP request (void - no parameters)
type DumpRequest struct {
	// DUMP has no parameters
}

// DumpResponse represents a DUMP response
type DumpResponse struct {
	Mounts []*MountEntry
}

// MountEntry represents a single mount entry
type MountEntry struct {
	Hostname string // Client hostname
	Dir      string // Mounted directory path
}

// Dump returns a list of all mounted file systems.
// RFC 1813 Appendix I
func (h *DefaultMountHandler) Dump(repository metadata.Repository) (*DumpResponse, error) {
	logger.Debug("DUMP called")

	// In a real implementation, you would track active mounts per client
	// For this simple implementation, we'll return an empty list
	// A production server would maintain a table of:
	// - client hostname/IP
	// - mounted path
	// - mount time
	// - mount options

	logger.Info("DUMP returning 0 mounts (not tracked in this implementation)")

	return &DumpResponse{
		Mounts: []*MountEntry{},
	}, nil
}

func DecodeDumpRequest(data []byte) (*DumpRequest, error) {
	// DUMP has no parameters
	return &DumpRequest{}, nil
}

func (resp *DumpResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write mount list
	for _, entry := range resp.Mounts {
		// value_follows = TRUE
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}

		// Write hostname
		hostnameLen := uint32(len(entry.Hostname))
		if err := binary.Write(&buf, binary.BigEndian, hostnameLen); err != nil {
			return nil, fmt.Errorf("write hostname length: %w", err)
		}
		buf.Write([]byte(entry.Hostname))

		// Add padding
		padding := (4 - (hostnameLen % 4)) % 4
		for i := uint32(0); i < padding; i++ {
			buf.WriteByte(0)
		}

		// Write directory
		dirLen := uint32(len(entry.Dir))
		if err := binary.Write(&buf, binary.BigEndian, dirLen); err != nil {
			return nil, fmt.Errorf("write dir length: %w", err)
		}
		buf.Write([]byte(entry.Dir))

		// Add padding
		padding = (4 - (dirLen % 4)) % 4
		for i := uint32(0); i < padding; i++ {
			buf.WriteByte(0)
		}
	}

	// value_follows = FALSE (end of list)
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
