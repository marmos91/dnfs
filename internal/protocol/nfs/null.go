package nfs

import "github.com/cubbit/dnfs/internal/metadata"

// MountNull does nothing. This is used to test connectivity.
// RFC 1813 Appendix I
func (h *DefaultNFSHandler) Null(repository metadata.Repository) ([]byte, error) {
	return []byte{}, nil
}
