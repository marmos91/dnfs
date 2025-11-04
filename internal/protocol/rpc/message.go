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
