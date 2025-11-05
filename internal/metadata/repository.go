package metadata

// Interface for NFS metadata persistence
type Repository interface {
	// Export operations
	AddExport(path string, options ExportOptions, rootAttr *FileAttr) error
	GetExports() ([]Export, error)
	FindExport(path string) (*Export, error)
	DeleteExport(path string) error

	// GetRootHandle returns the root file handle for an export path
	GetRootHandle(exportPath string) (FileHandle, error)

	// File operations
	CreateFile(handle FileHandle, attr *FileAttr) error
	GetFile(handle FileHandle) (*FileAttr, error)
	UpdateFile(handle FileHandle, attr *FileAttr) error
	DeleteFile(handle FileHandle) error

	// Directory hierarchy operations
	SetParent(child FileHandle, parent FileHandle) error
	GetParent(child FileHandle) (FileHandle, error)

	AddChild(parent FileHandle, name string, child FileHandle) error
	GetChild(parent FileHandle, name string) (FileHandle, error)
	GetChildren(parent FileHandle) (map[string]FileHandle, error)
	DeleteChild(parent FileHandle, name string) error

	// Helper method to add files/directories to a parent
	AddFileToDirectory(parentHandle FileHandle, name string, attr *FileAttr) (FileHandle, error)
}
