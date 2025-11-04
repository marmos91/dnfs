package rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"

	xdr "github.com/rasky/go-xdr/xdr2"
)

type Serializable interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

func ReadCall(data []byte) (*RPCCallMessage, error) {
	call := &RPCCallMessage{}
	_, err := xdr.Unmarshal(bytes.NewReader(data), call)
	if err != nil {
		return nil, fmt.Errorf("unmarshal RPC call: %w", err)
	}

	if call.MsgType != 0 {
		return nil, fmt.Errorf("expected CALL (0), got %d", call.MsgType)
	}

	return call, nil
}

// Returns the data after the RPC header
func ReadData(message []byte, call *RPCCallMessage) ([]byte, error) {
	reader := bytes.NewReader(message)

	// Skip the RPC header fields we already parsed
	// XID, MsgType, RPCVersion, Program, Version, Procedure = 6 * 4 bytes = 24 bytes
	offset := 24

	// Skip credentials
	offset += 4 // flavor
	credLen := binary.BigEndian.Uint32(message[offset : offset+4])
	offset += 4 + int(credLen)

	// Skip padding
	padding := (4 - (credLen % 4)) % 4
	offset += int(padding)

	// Skip verifier
	offset += 4 // flavor
	verfLen := binary.BigEndian.Uint32(message[offset : offset+4])
	offset += 4 + int(verfLen)

	// Skip padding
	padding = (4 - (verfLen % 4)) % 4
	offset += int(padding)

	if offset >= len(message) {
		return []byte{}, nil
	}

	reader.Seek(int64(offset), 0)
	remaining := message[offset:]

	return remaining, nil
}

func MakeSuccessReply(xid uint32, data []byte) ([]byte, error) {
	reply := RPCReplyMessage{
		XID:        xid,
		MsgType:    1, // REPLY
		ReplyState: 0, // MSG_ACCEPTED
		Verf: OpaqueAuth{
			Flavor: 0, // AUTH_NULL
			Body:   []byte{},
		},
		AcceptStat: 0, // SUCCESS
	}

	var buf bytes.Buffer

	// Marshal the reply header
	_, err := xdr.Marshal(&buf, &reply)
	if err != nil {
		return nil, fmt.Errorf("marshal reply: %w", err)
	}

	// Append any additional data
	buf.Write(data)

	// Prepend RPC fragment header
	replyData := buf.Bytes()
	fragmentHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(fragmentHeader, 0x80000000|uint32(len(replyData))) // Last fragment bit set

	return append(fragmentHeader, replyData...), nil
}
