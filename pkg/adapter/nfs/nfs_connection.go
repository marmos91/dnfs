package nfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/marmos91/dittofs/internal/logger"
	nfs "github.com/marmos91/dittofs/internal/protocol/nfs"
	"github.com/marmos91/dittofs/internal/protocol/nfs/rpc"
	"github.com/marmos91/dittofs/pkg/store/metadata"
)

type NFSConnection struct {
	server *NFSAdapter
	conn   net.Conn
}

type fragmentHeader struct {
	IsLast bool
	Length uint32
}

func NewNFSConnection(server *NFSAdapter, conn net.Conn) *NFSConnection {
	return &NFSConnection{
		server,
		conn,
	}
}

// serve handles all RPC requests for this connection.
// It implements panic recovery to prevent a single misbehaving connection
// from crashing the entire server.
//
// The connection is automatically closed when:
// - The context is cancelled (server shutdown)
// - An idle timeout occurs
// - A read or write timeout occurs
// - An unrecoverable error occurs
// - The client closes the connection
//
// Context cancellation is checked at the beginning of each request loop,
// ensuring graceful shutdown and proper cleanup of resources.
func (c *NFSConnection) Serve(ctx context.Context) {
	defer func() {
		// Panic recovery - prevents a single connection from crashing the server
		if r := recover(); r != nil {
			logger.Error("Panic in connection handler from %s: %v",
				c.conn.RemoteAddr().String(), r)
		}
		_ = c.conn.Close()
	}()

	clientAddr := c.conn.RemoteAddr().String()
	logger.Debug("New connection from %s", clientAddr)

	// Set initial idle timeout
	if c.server.config.Timeouts.Idle > 0 {
		if err := c.conn.SetDeadline(time.Now().Add(c.server.config.Timeouts.Idle)); err != nil {
			logger.Warn("Failed to set deadline for %s: %v", clientAddr, err)
		}
	}

	for {
		// Check for context cancellation before processing next request
		// This provides graceful shutdown capability
		select {
		case <-ctx.Done():
			logger.Debug("Connection from %s closed due to context cancellation", clientAddr)
			return
		case <-c.server.shutdown:
			logger.Debug("Connection from %s closed due to server shutdown", clientAddr)
			return
		default:
		}

		startTime := time.Now()
		err := c.handleRequest(ctx)
		duration := time.Since(startTime)

		if err != nil {
			if err == io.EOF {
				logger.Debug("Connection from %s closed by client", clientAddr)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.Debug("Connection from %s timed out: %v", clientAddr, err)
			} else if err == context.Canceled || err == context.DeadlineExceeded {
				logger.Debug("Connection from %s cancelled: %v", clientAddr, err)
			} else {
				logger.Debug("Error handling request from %s: %v", clientAddr, err)
			}
			return
		}

		// Record successful request (actual success/error determined in handler)
		// This records the request was processed, not necessarily successful
		_ = duration // Will be recorded in handleRPCCall

		// Reset idle timeout after successful request
		if c.server.config.Timeouts.Idle > 0 {
			if err := c.conn.SetDeadline(time.Now().Add(c.server.config.Timeouts.Idle)); err != nil {
				logger.Warn("Failed to reset deadline for %s: %v", clientAddr, err)
			}
		}
	}
}

// handleRequest processes a single RPC request.
//
// It reads the fragment header, validates the message size, reads the RPC message,
// parses it, and dispatches it to the appropriate handler.
//
// The context is passed through to handlers to enable cancellation of long-running
// operations.
//
// Returns an error if:
// - Context is cancelled
// - Network error occurs
// - Message is malformed or too large
// - Handler returns an error
func (c *NFSConnection) handleRequest(ctx context.Context) error {
	// Check context before starting request processing
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Apply read timeout if configured
	if c.server.config.Timeouts.Read > 0 {
		deadline := time.Now().Add(c.server.config.Timeouts.Read)
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
	}

	// Read fragment header
	header, err := c.readFragmentHeader()
	if err != nil {
		// Don't log EOF as an error - it's a normal client disconnect
		if err != io.EOF {
			logger.Debug("Error reading fragment header from %s: %v", c.conn.RemoteAddr().String(), err)
		}
		return err
	}
	logger.Debug("Read fragment header from %s: last=%v length=%d", c.conn.RemoteAddr().String(), header.IsLast, header.Length)

	// Validate fragment size to prevent memory exhaustion
	const maxFragmentSize = 1 << 20 // 1MB - NFS messages are typically much smaller
	if header.Length > maxFragmentSize {
		logger.Warn("Fragment size %d exceeds maximum %d from %s",
			header.Length, maxFragmentSize, c.conn.RemoteAddr().String())
		return fmt.Errorf("fragment too large: %d bytes", header.Length)
	}

	// Check context before reading potentially large message
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Read RPC message (now uses buffer pool)
	message, err := c.readRPCMessage(header.Length)
	if err != nil {
		return fmt.Errorf("read RPC message: %w", err)
	}
	// NOTE: Buffer is returned AFTER handler completes to allow zero-copy
	// operations where procedureData is a slice into the original buffer
	defer nfs.PutBuffer(message)

	// Parse RPC call
	call, err := rpc.ReadCall(message)
	if err != nil {
		logger.Debug("Error parsing RPC call: %v", err)
		return nil
	}

	logger.Debug("RPC Call: XID=0x%x Program=%d Version=%d Procedure=%d",
		call.XID, call.Program, call.Version, call.Procedure)

	// Extract procedure data (returns slice into message buffer - zero-copy)
	procedureData, err := rpc.ReadData(message, call)
	if err != nil {
		return fmt.Errorf("extract procedure data: %w", err)
	}

	// Check context before dispatching to handler
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Handle the call with context
	// IMPORTANT: procedureData is a slice into the pooled message buffer
	// The buffer will be returned to the pool when this function exits
	return c.handleRPCCall(ctx, call, procedureData)
}

// readFragmentHeader reads the 4-byte RPC fragment header.
//
// The fragment header contains:
// - Bit 31: Last fragment flag (1 = last, 0 = more fragments)
// - Bits 0-30: Fragment length in bytes
//
// Returns the parsed header or an error if reading fails.
func (c *NFSConnection) readFragmentHeader() (*fragmentHeader, error) {
	var buf [4]byte
	_, err := io.ReadFull(c.conn, buf[:])
	if err != nil {
		return nil, err
	}

	header := binary.BigEndian.Uint32(buf[:])
	return &fragmentHeader{
		IsLast: (header & 0x80000000) != 0,
		Length: header & 0x7FFFFFFF,
	}, nil
}

// readRPCMessage reads an RPC message of the specified length.
//
// It uses a buffer pool to reduce allocations for frequently sized messages.
// The caller is responsible for returning the buffer to the pool via PutBuffer.
//
// Returns the message buffer or an error if reading fails.
func (c *NFSConnection) readRPCMessage(length uint32) ([]byte, error) {
	// Get buffer from pool
	message := nfs.GetBuffer(length)

	// Read directly into pooled buffer
	_, err := io.ReadFull(c.conn, message)
	if err != nil {
		// Return buffer to pool on error
		nfs.PutBuffer(message)
		return nil, fmt.Errorf("read message: %w", err)
	}

	return message, nil
}

// handleRPCCall dispatches an RPC call to the appropriate handler.
//
// It routes calls to either NFS or MOUNT handlers based on the program number,
// records metrics, and sends the reply back to the client.
//
// The context is passed through to handlers to enable cancellation of
// long-running operations like large file reads/writes or directory scans.
//
// Returns an error if:
// - Context is cancelled during processing
// - Handler returns an error
// - Reply cannot be sent
func (c *NFSConnection) handleRPCCall(ctx context.Context, call *rpc.RPCCallMessage, procedureData []byte) error {
	var replyData []byte
	var err error

	clientAddr := c.conn.RemoteAddr().String()

	logger.Debug("RPC Call Details: Program=%d Version=%d Procedure=%d",
		call.Program, call.Version, call.Procedure)

	// Check context before dispatching to handler
	select {
	case <-ctx.Done():
		logger.Debug("RPC call cancelled before handler dispatch: XID=0x%x client=%s error=%v",
			call.XID, clientAddr, ctx.Err())
		return ctx.Err()
	default:
	}

	switch call.Program {
	case rpc.ProgramNFS:
		replyData, err = c.handleNFSProcedure(ctx, call, procedureData, clientAddr)
	case rpc.ProgramMount:
		replyData, err = c.handleMountProcedure(ctx, call, procedureData, clientAddr)
	default:
		logger.Debug("Unknown program: %d", call.Program)
		return nil
	}

	if err != nil {
		// Check if error was due to context cancellation
		if err == context.Canceled || err == context.DeadlineExceeded {
			logger.Debug("Handler cancelled: program=%d procedure=%d xid=0x%x client=%s error=%v",
				call.Program, call.Procedure, call.XID, clientAddr, err)
			return err
		}

		logger.Debug("Handler error: %v", err)
		return fmt.Errorf("handle program %d: %w", call.Program, err)
	}

	return c.sendReply(call.XID, replyData)
}

// extractShareName attempts to extract the share name from NFS request data.
//
// Most NFS procedures include a file handle at the beginning of the request.
// This function decodes the file handle and uses the registry to determine
// which share it belongs to.
//
// Parameters:
//   - ctx: Context for cancellation
//   - data: Raw procedure data (includes file handle)
//
// Returns:
//   - string: Share name, or empty string if no handle present (e.g., NULL procedure)
//   - error: NFS3ErrBadHandle if handle is malformed or share not found
func (c *NFSConnection) extractShareName(ctx context.Context, data []byte) (string, error) {
	// Need at least 4 bytes for handle length
	if len(data) < 4 {
		return "", nil // No handle present (e.g., NULL procedure)
	}

	reader := bytes.NewReader(data)

	// Read handle length (4 bytes, big-endian)
	var handleLen uint32
	if err := binary.Read(reader, binary.BigEndian, &handleLen); err != nil {
		return "", nil // Failed to read, assume no handle
	}

	// No handle present (length is 0)
	if handleLen == 0 {
		return "", nil
	}

	// Validate handle length (NFS v3 handles must be <= 64 bytes per RFC 1813)
	if handleLen > 64 {
		return "", fmt.Errorf("handle length %d exceeds maximum 64 bytes", handleLen)
	}

	// Check if we have enough data for the handle
	if len(data) < int(4+handleLen) {
		return "", fmt.Errorf("incomplete file handle: need %d bytes, have %d", 4+handleLen, len(data))
	}

	// Read the handle bytes
	handleBytes := make([]byte, handleLen)
	if _, err := reader.Read(handleBytes); err != nil {
		return "", fmt.Errorf("failed to read file handle: %w", err)
	}

	// Convert to FileHandle type
	handle := metadata.FileHandle(handleBytes)

	// Extract share name from handle using registry
	shareName, err := c.server.registry.GetShareNameForHandle(ctx, handle)
	if err != nil {
		return "", fmt.Errorf("failed to resolve share from handle: %w", err)
	}

	return shareName, nil
}

// logNFSRequest logs an NFS request with procedure, share, and auth information.
//
// This consolidates all the conditional logging logic in one place for cleaner code.
func (c *NFSConnection) logNFSRequest(procedure, share string, authCtx *nfs.NFSAuthContext) {
	var parts []string

	parts = append(parts, fmt.Sprintf("NFS %s", procedure))

	if share != "" {
		parts = append(parts, fmt.Sprintf("share=%s", share))
	}

	if authCtx.UID != nil {
		parts = append(parts, fmt.Sprintf("uid=%d gid=%d ngids=%d",
			*authCtx.UID, *authCtx.GID, len(authCtx.GIDs)))
	} else {
		parts = append(parts, fmt.Sprintf("auth_flavor=%d", authCtx.AuthFlavor))
	}

	logger.Debug("%s", parts[0])
	for i := 1; i < len(parts); i++ {
		logger.Debug("  %s", parts[i])
	}
}

// handleNFSProcedure dispatches an NFS procedure call to the appropriate handler.
//
// It looks up the procedure in the dispatch table, extracts authentication
// context from the RPC call, and invokes the handler with the context.
//
// The context enables handlers to:
// - Respect cancellation during long operations (READ, WRITE, READDIR)
// - Implement request timeouts
// - Support graceful server shutdown
//
// Returns the reply data or an error if the handler fails.
func (c *NFSConnection) handleNFSProcedure(ctx context.Context, call *rpc.RPCCallMessage, data []byte, clientAddr string) ([]byte, error) {
	// Look up procedure in dispatch table
	procInfo, ok := nfs.NfsDispatchTable[call.Procedure]
	if !ok {
		logger.Debug("Unknown NFS procedure: %d", call.Procedure)
		return []byte{}, nil
	}

	// Extract share name from file handle (best effort for metrics)
	share, extractErr := c.extractShareName(ctx, data)
	if extractErr != nil {
		logger.Debug("NFS %s: failed to extract share (handle may be invalid): %v", procInfo.Name, extractErr)
		// Continue anyway - handler will validate and return proper NFS error
		share = ""
	}

	// Extract authentication context (includes share for handlers)
	authCtx := nfs.ExtractAuthContext(ctx, call, clientAddr, share, procInfo.Name)

	// Log request with clean helper
	c.logNFSRequest(procInfo.Name, share, authCtx)

	// Check context before dispatching to handler
	select {
	case <-ctx.Done():
		logger.Debug("NFS %s cancelled before handler: xid=0x%x", procInfo.Name, call.XID)
		return nil, ctx.Err()
	default:
	}

	// Record request start in metrics
	c.server.metrics.RecordRequestStart(procInfo.Name, share)
	defer c.server.metrics.RecordRequestEnd(procInfo.Name, share)

	// Dispatch to handler with context and record metrics
	startTime := time.Now()
	result, err := procInfo.Handler(
		authCtx,
		c.server.nfsHandler,
		c.server.registry,
		data,
	)
	duration := time.Since(startTime)

	// Record request completion in metrics with NFS status code
	var errorCode string
	if result != nil {
		errorCode = nfs.NFSStatusToString(result.NFSStatus)
	} else if err != nil {
		errorCode = "ERROR_NO_RESULT"
	}
	c.server.metrics.RecordRequest(procInfo.Name, share, duration, errorCode)

	if result == nil {
		return nil, err
	}
	return result.Data, err
}

// handleMountProcedure dispatches a MOUNT procedure call to the appropriate handler.
//
// It looks up the procedure in the dispatch table, extracts authentication
// context from the RPC call, and invokes the handler with the context.
//
// The context enables handlers to respect cancellation and timeouts.
//
// Returns the reply data or an error if the handler fails.
func (c *NFSConnection) handleMountProcedure(ctx context.Context, call *rpc.RPCCallMessage, data []byte, clientAddr string) ([]byte, error) {
	// Look up procedure in dispatch table
	procInfo, ok := nfs.MountDispatchTable[call.Procedure]
	if !ok {
		logger.Debug("Unknown Mount procedure: %d", call.Procedure)
		return []byte{}, nil
	}

	// Mount requests don't have file handles, so no share
	share := ""

	// Extract authentication context (includes empty share for mount requests)
	authCtx := nfs.ExtractAuthContext(ctx, call, clientAddr, share, procInfo.Name)

	// Log request with clean helper (MOUNT_ prefix)
	procedureName := "MOUNT_" + procInfo.Name
	c.logNFSRequest(procedureName, share, authCtx)

	// Check context before dispatching to handler
	select {
	case <-ctx.Done():
		logger.Debug("MOUNT %s cancelled before handler: xid=0x%x", procInfo.Name, call.XID)
		return nil, ctx.Err()
	default:
	}

	// Record request start in metrics
	c.server.metrics.RecordRequestStart(procedureName, share)
	defer c.server.metrics.RecordRequestEnd(procedureName, share)

	// Dispatch to handler with context and record metrics
	startTime := time.Now()
	result, err := procInfo.Handler(
		authCtx,
		c.server.mountHandler,
		c.server.registry,
		data,
	)
	duration := time.Since(startTime)

	// Record request completion in metrics with Mount status code
	var errorCode string
	if result != nil {
		errorCode = nfs.MountStatusToString(result.NFSStatus)
	} else if err != nil {
		errorCode = "ERROR_NO_RESULT"
	}
	c.server.metrics.RecordRequest(procedureName, share, duration, errorCode)

	if result == nil {
		return nil, err
	}
	return result.Data, err
}

// sendReply sends an RPC reply to the client.
//
// It applies write timeout if configured, constructs the RPC success reply,
// and writes it to the connection.
//
// Returns an error if:
// - Write timeout cannot be set
// - Reply construction fails
// - Network write fails
func (c *NFSConnection) sendReply(xid uint32, data []byte) error {
	if c.server.config.Timeouts.Write > 0 {
		deadline := time.Now().Add(c.server.config.Timeouts.Write)
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("set write deadline: %w", err)
		}
	}

	reply, err := rpc.MakeSuccessReply(xid, data)
	if err != nil {
		return fmt.Errorf("make reply: %w", err)
	}

	_, err = c.conn.Write(reply)
	if err != nil {
		return fmt.Errorf("write reply: %w", err)
	}

	logger.Debug("Sent reply for XID=0x%x (%d bytes)", xid, len(reply))
	return nil
}
