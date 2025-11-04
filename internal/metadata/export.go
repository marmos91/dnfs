package metadata

type Export struct {
	Path    string
	Options ExportOptions
}

type ExportOptions struct {
	ReadOnly bool
	Async    bool
}
