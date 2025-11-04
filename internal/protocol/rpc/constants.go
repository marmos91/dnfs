package rpc

// RPC Program Numbers
// These identify the different RPC programs supported by the server.
const (
	// ProgramPortmap is the port mapper program number (RFC 1833)
	ProgramPortmap = 100000

	// ProgramNFS is the NFS version 3 program number (RFC 1813)
	ProgramNFS = 100003

	// ProgramMount is the Mount protocol program number (RFC 1813 Appendix I)
	ProgramMount = 100005
)

// RPC Message Types
const (
	// RPCCall indicates an RPC call message
	RPCCall = 0

	// RPCReply indicates an RPC reply message
	RPCReply = 1
)

// RPC Reply States
const (
	// RPCMsgAccepted indicates the RPC call was accepted
	RPCMsgAccepted = 0

	// RPCMsgDenied indicates the RPC call was denied
	RPCMsgDenied = 1
)

// RPC Accept Status
const (
	// RPCSuccess indicates successful RPC execution
	RPCSuccess = 0

	// RPCProgMismatch indicates program version mismatch
	RPCProgMismatch = 3

	// RPCProcUnavail indicates the procedure is unavailable
	RPCProcUnavail = 3
)
