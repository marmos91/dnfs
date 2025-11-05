package mount

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// ExportRequest represents an EXPORT request (void - no parameters)
type ExportRequest struct {
	// EXPORT has no parameters
}

// ExportResponse represents an EXPORT response
type ExportResponse struct {
	Exports []*ExportEntry
}

// ExportEntry represents a single export entry
type ExportEntry struct {
	Dir    string   // Export directory path
	Groups []string // List of groups that can mount (empty = everyone)
}

// Export returns a list of all exported file systems.
// RFC 1813 Appendix I
func (h *DefaultMountHandler) Export(repository metadata.Repository) (*ExportResponse, error) {
	logger.Debug("EXPORT called")

	// Get all exports from repository
	exports, err := repository.GetExports()
	if err != nil {
		logger.Error("Failed to get exports: %v", err)
		return &ExportResponse{Exports: []*ExportEntry{}}, nil
	}

	// Convert to export entries
	exportEntries := make([]*ExportEntry, 0, len(exports))
	for _, exp := range exports {
		entry := &ExportEntry{
			Dir:    exp.Path,
			Groups: []string{}, // Empty means everyone can mount
		}
		exportEntries = append(exportEntries, entry)
		logger.Debug("Export entry: %s", exp.Path)
	}

	logger.Info("EXPORT returning %d exports", len(exportEntries))

	return &ExportResponse{
		Exports: exportEntries,
	}, nil
}

func DecodeExportRequest(data []byte) (*ExportRequest, error) {
	// EXPORT has no parameters
	return &ExportRequest{}, nil
}

func (resp *ExportResponse) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// Write export list
	for _, entry := range resp.Exports {
		// value_follows = TRUE
		if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
			return nil, err
		}

		// Write directory path
		dirLen := uint32(len(entry.Dir))
		if err := binary.Write(&buf, binary.BigEndian, dirLen); err != nil {
			return nil, fmt.Errorf("write dir length: %w", err)
		}
		buf.Write([]byte(entry.Dir))

		// Add padding
		padding := (4 - (dirLen % 4)) % 4
		for i := uint32(0); i < padding; i++ {
			buf.WriteByte(0)
		}

		// Write groups list
		if len(entry.Groups) == 0 {
			// No groups specified = everyone can mount
			// Write empty list (value_follows = FALSE)
			if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
				return nil, err
			}
		} else {
			// Write groups
			for _, group := range entry.Groups {
				// value_follows = TRUE
				if err := binary.Write(&buf, binary.BigEndian, uint32(1)); err != nil {
					return nil, err
				}

				groupLen := uint32(len(group))
				if err := binary.Write(&buf, binary.BigEndian, groupLen); err != nil {
					return nil, err
				}
				buf.Write([]byte(group))

				// Add padding
				padding := (4 - (groupLen % 4)) % 4
				for i := uint32(0); i < padding; i++ {
					buf.WriteByte(0)
				}
			}

			// End of groups list
			if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
				return nil, err
			}
		}
	}

	// value_follows = FALSE (end of export list)
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
