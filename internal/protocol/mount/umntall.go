package mount

import (
	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// UmntAllRequest represents an UMNTALL request (void - no parameters)
type UmntAllRequest struct {
	// UMNTALL has no parameters
}

// UmntAllResponse represents an UMNTALL response (void - no response data)
type UmntAllResponse struct {
	// UMNTALL has no response data
}

// UmntAll removes all mount entries for the calling client.
// RFC 1813 Appendix I
func (h *DefaultMountHandler) UmntAll(repository metadata.Repository) (*UmntAllResponse, error) {
	logger.Info("UMNTALL called")

	// In a real implementation, you would:
	// 1. Identify the calling client (from RPC credentials or connection info)
	// 2. Remove all mount entries for that client from the mount table
	// 3. Perform any cleanup needed

	// For this simple implementation, we just acknowledge the request
	// The client is responsible for actually unmounting on their side

	return &UmntAllResponse{}, nil
}

func DecodeUmntAllRequest(data []byte) (*UmntAllRequest, error) {
	// UMNTALL has no parameters
	return &UmntAllRequest{}, nil
}

func (resp *UmntAllResponse) Encode() ([]byte, error) {
	// UMNTALL returns void (no data)
	return []byte{}, nil
}
