package metrics

import (
	"time"
)

// NFSMetrics provides observability for NFS adapter operations.
//
// Implementations can collect metrics about NFS requests, connection lifecycle,
// throughput, and errors. This interface is optional - if not provided to the
// NFS adapter, a no-op implementation is used with zero overhead.
//
// Example usage:
//
//	// With metrics enabled
//	metrics := metrics.NewNFSMetrics()
//	adapter := nfs.New(config, metrics)
//
//	// Without metrics (no-op)
//	adapter := nfs.New(config, nil)
type NFSMetrics interface {
	// RecordRequest records a completed NFS request with its procedure name,
	// share, duration, and outcome.
	//
	// Parameters:
	//   - procedure: NFS procedure name (e.g., "LOOKUP", "READ", "WRITE")
	//   - share: Share name (e.g., "/export", "/archive")
	//   - duration: Time taken to process the request
	//   - errorCode: NFS error code if request failed (e.g., "NFS3ERR_NOENT"), empty if successful
	RecordRequest(procedure string, share string, duration time.Duration, errorCode string)

	// RecordRequestStart increments the in-flight request counter.
	// Should be called when starting to process a request.
	//
	// Parameters:
	//   - procedure: NFS procedure name
	//   - share: Share name
	RecordRequestStart(procedure string, share string)

	// RecordRequestEnd decrements the in-flight request counter.
	// Should be called when request processing completes.
	//
	// Parameters:
	//   - procedure: NFS procedure name
	//   - share: Share name
	RecordRequestEnd(procedure string, share string)

	// RecordBytesTransferred records bytes read or written.
	//
	// Parameters:
	//   - procedure: NFS procedure name (e.g., "READ", "WRITE")
	//   - share: Share name
	//   - direction: "read" or "write"
	//   - bytes: Number of bytes transferred
	RecordBytesTransferred(procedure string, share string, direction string, bytes uint64)

	// RecordOperationSize records the size of a READ or WRITE operation.
	//
	// Parameters:
	//   - operation: "read" or "write"
	//   - share: Share name
	//   - bytes: Size of the operation in bytes
	RecordOperationSize(operation string, share string, bytes uint64)

	// SetActiveConnections updates the current connection count.
	//
	// Parameters:
	//   - count: Current number of active connections
	SetActiveConnections(count int32)

	// RecordConnectionAccepted increments the total accepted connections counter.
	RecordConnectionAccepted()

	// RecordConnectionClosed increments the total closed connections counter.
	RecordConnectionClosed()

	// RecordConnectionForceClosed increments the force-closed connections counter.
	// Called when connections are forcibly closed after shutdown timeout.
	RecordConnectionForceClosed()
}

// noopNFSMetrics is a no-op implementation of NFSMetrics with zero overhead.
type noopNFSMetrics struct{}

func (noopNFSMetrics) RecordRequest(procedure string, share string, duration time.Duration, errorCode string) {
}
func (noopNFSMetrics) RecordRequestStart(procedure string, share string) {}
func (noopNFSMetrics) RecordRequestEnd(procedure string, share string)   {}
func (noopNFSMetrics) RecordBytesTransferred(procedure string, share string, direction string, bytes uint64) {
}
func (noopNFSMetrics) RecordOperationSize(operation string, share string, bytes uint64) {}
func (noopNFSMetrics) SetActiveConnections(count int32)                                 {}
func (noopNFSMetrics) RecordConnectionAccepted()                                        {}
func (noopNFSMetrics) RecordConnectionClosed()                                          {}
func (noopNFSMetrics) RecordConnectionForceClosed()                                     {}

// NewNoopNFSMetrics returns a no-op implementation of NFSMetrics.
// This is useful for testing or when metrics collection is disabled.
func NewNoopNFSMetrics() NFSMetrics {
	return &noopNFSMetrics{}
}
