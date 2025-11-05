package rpc

type RPCCallMessage struct {
	XID        uint32
	MsgType    uint32
	RPCVersion uint32
	Program    uint32
	Version    uint32
	Procedure  uint32
	Cred       OpaqueAuth
	Verf       OpaqueAuth
}

type RPCReplyMessage struct {
	XID        uint32
	MsgType    uint32 // 1 = REPLY
	ReplyState uint32 // 0 = MSG_ACCEPTED
	Verf       OpaqueAuth
	AcceptStat uint32 // 0 = SUCCESS
	// Reply data follows
}

type OpaqueAuth struct {
	Flavor uint32
	Body   []byte `xdr:"opaque"`
}

// GetAuthFlavor returns the authentication flavor from the RPC call credentials.
// Common values:
//   - 0: AUTH_NULL (no authentication)
//   - 1: AUTH_UNIX (Unix-style authentication with UID/GID)
//   - 2: AUTH_SHORT (short hand Unix credential)
//   - 3: AUTH_DES (DES encryption-based authentication)
func (c *RPCCallMessage) GetAuthFlavor() uint32 {
	return c.Cred.Flavor
}

// GetAuthBody returns the authentication body data.
// For AUTH_NULL, this will be empty.
// For AUTH_UNIX, this contains the Unix credentials (timestamp, machine name, UID, GID, etc.)
func (c *RPCCallMessage) GetAuthBody() []byte {
	return c.Cred.Body
}
