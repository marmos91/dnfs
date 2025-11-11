package nfs

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marmos91/dittofs/internal/content"
	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
	mount "github.com/marmos91/dittofs/internal/protocol/nfs/mount/handlers"
	v3 "github.com/marmos91/dittofs/internal/protocol/nfs/v3/handlers"
)

type NFSFacade struct {
	config       NFSConfig
	listener     net.Listener
	nfsHandler   v3.NFSHandler
	mountHandler mount.MountHandler
	repository   metadata.Repository
	content      content.Repository

	// Connection tracking for graceful shutdown
	activeConns  sync.WaitGroup
	shutdownOnce sync.Once
	shutdown     chan struct{}

	// Connection limiting
	connCount     atomic.Int32
	connSemaphore chan struct{} // Semaphore for connection limiting

	// Context for request cancellation
	// This context is cancelled on shutdown to abort in-flight requests
	shutdownCtx    context.Context
	cancelRequests context.CancelFunc
}

// ServerConfig holds configuration parameters for the NFS server.
// These values control connection limits, timeouts, and resource management.
type NFSConfig struct {
	// Port is the TCP port to listen on (e.g., "2049")
	Port int

	// MaxConnections limits the number of concurrent client connections.
	// Zero means unlimited. Default: 0 (unlimited)
	MaxConnections int

	// ReadTimeout is the maximum duration for reading a complete RPC request.
	// This prevents slow clients from holding connections indefinitely.
	// Zero means no timeout. Default: 30s
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration for writing an RPC response.
	// Zero means no timeout. Default: 30s
	WriteTimeout time.Duration

	// IdleTimeout is the maximum duration a connection can remain idle
	// between requests before being closed.
	// Zero means no timeout. Default: 5m
	IdleTimeout time.Duration

	// ShutdownTimeout is the maximum duration to wait for active connections
	// to complete during graceful shutdown.
	// Default: 30s
	ShutdownTimeout time.Duration

	// MetricsLogInterval is the interval at which to log server metrics.
	// Set to 0 to disable periodic metrics logging.
	// Default: 5 minutes
	MetricsLogInterval time.Duration
}

// New creates a new NFSServer with the specified configuration.
// The repositories are provided later via the Start() method.
// The server must be started with Start() (or Serve()) before it will accept connections.
func New(config NFSConfig) *NFSFacade {
	var connSemaphore chan struct{}
	if config.MaxConnections > 0 {
		connSemaphore = make(chan struct{}, config.MaxConnections)
	}

	shutdownCtx, cancelRequests := context.WithCancel(context.Background())

	return &NFSFacade{
		config:         config,
		nfsHandler:     &v3.DefaultNFSHandler{},
		mountHandler:   &mount.DefaultMountHandler{},
		shutdown:       make(chan struct{}),
		connSemaphore:  connSemaphore,
		shutdownCtx:    shutdownCtx,
		cancelRequests: cancelRequests,
	}
}

func (s *NFSFacade) SetRepositories(metadataRepo metadata.Repository, contentRepo content.Repository) {
	s.repository = metadataRepo
	s.content = contentRepo
}

// Serve starts the NFS server and blocks until the context is cancelled
// or an unrecoverable error occurs.
//
// When the context is cancelled, Serve initiates a graceful shutdown:
// 1. Stops accepting new connections
// 2. Cancels all in-flight request contexts
// 3. Waits for active connections to complete (up to ShutdownTimeout)
// 4. Forcibly closes any remaining connections
//
// The provided context controls the server lifecycle. When cancelled, it triggers
// graceful shutdown. Additionally, the server creates a child context (shutdownCtx)
// that is passed to all request handlers, allowing them to detect shutdown and abort
// long-running operations gracefully.
//
// Context cancellation flow:
//   - Server context cancelled → initiateShutdown()
//   - shutdownCtx cancelled → all in-flight NFS operations receive cancellation
//   - Handlers check context and abort (e.g., READDIR stops scanning directories)
//   - Connections close after completing or aborting their current request
//
// Returns nil on graceful shutdown, or an error if startup fails.
func (s *NFSFacade) Serve(ctx context.Context) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	s.listener = listener
	logger.Info("NFS server started on port %d", s.config.Port)
	logger.Debug("Server config: max_connections=%d read_timeout=%v write_timeout=%v idle_timeout=%v",
		s.config.MaxConnections, s.config.ReadTimeout, s.config.WriteTimeout, s.config.IdleTimeout)

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		logger.Info("Shutdown signal received, initiating graceful shutdown")
		s.initiateShutdown()
	}()

	// Accept connections until shutdown
	for {
		select {
		case <-s.shutdown:
			return s.gracefulShutdown()
		default:
		}

		// Check connection limit before accepting
		if s.connSemaphore != nil {
			select {
			case s.connSemaphore <- struct{}{}:
				// Acquired semaphore, can accept connection
			case <-s.shutdown:
				return s.gracefulShutdown()
			}
		}

		tcpConn, err := s.listener.Accept()
		if err != nil {
			// Release semaphore on accept error
			if s.connSemaphore != nil {
				<-s.connSemaphore
			}

			select {
			case <-s.shutdown:
				return s.gracefulShutdown()
			default:
				logger.Debug("Error accepting connection: %v", err)
				continue
			}
		}

		// Track connection for graceful shutdown
		s.activeConns.Add(1)
		s.connCount.Add(1)

		conn := s.newConn(tcpConn)
		go func() {
			defer func() {
				s.activeConns.Done()
				s.connCount.Add(-1)
				if s.connSemaphore != nil {
					<-s.connSemaphore
				}
			}()
			// Pass the shutdown context to the connection handler
			// This allows requests to detect shutdown and abort gracefully
			conn.serve(s.shutdownCtx)
		}()
	}
}

// initiateShutdown signals the server to begin graceful shutdown.
// This is called automatically when the context is cancelled.
// It's safe to call multiple times.
//
// During shutdown:
// 1. The shutdown channel is closed to signal all goroutines
// 2. The listener is closed to stop accepting new connections
// 3. All in-flight request contexts are cancelled via cancelRequests()
//
// The cancellation propagates through the entire request stack:
//   - Connection handlers receive cancelled context
//   - RPC dispatchers receive cancelled context
//   - NFS procedure handlers receive cancelled context
//   - Repository operations can detect cancellation
//
// This enables graceful abort of long-running operations like:
//   - Directory scans (READDIR/READDIRPLUS)
//   - Large file reads/writes
//   - Metadata operations on large directory trees
func (s *NFSFacade) initiateShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdown)
		if s.listener != nil {
			// Stop accepting new connections
			s.listener.Close()
		}
		// Cancel all in-flight request contexts
		// This signals to all NFS procedure handlers that they should abort
		s.cancelRequests()
		logger.Debug("Request cancellation signal sent to all in-flight operations")
	})
}

// gracefulShutdown waits for active connections to complete or timeout.
// Returns nil if all connections complete gracefully, or an error if
// the shutdown timeout is exceeded.
func (s *NFSFacade) gracefulShutdown() error {
	logger.Info("Waiting for %d active connections to complete (timeout: %v)",
		s.connCount.Load(), s.config.ShutdownTimeout)

	// Wait for active connections with timeout
	done := make(chan struct{})
	go func() {
		s.activeConns.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All connections closed gracefully")
		return nil
	case <-time.After(s.config.ShutdownTimeout):
		remaining := s.connCount.Load()
		logger.Warn("Shutdown timeout exceeded, %d connections still active", remaining)
		return fmt.Errorf("shutdown timeout exceeded with %d active connections", remaining)
	}
}

// Stop initiates graceful shutdown of the server.
// It stops accepting new connections, cancels all in-flight requests,
// and waits for active connections to complete up to ShutdownTimeout.
//
// Stop is safe to call multiple times and safe to call concurrently with Serve.
// After Stop returns, no new connections will be accepted and all active
// connections will have been given a chance to complete.
//
// The shutdown process:
//   - Listener closed → no new connections accepted
//   - Context cancelled → all in-flight operations receive abort signal
//   - Wait for connections → up to ShutdownTimeout
//   - Force close → any remaining connections after timeout
//
// Returns nil on successful shutdown, or an error if the shutdown timeout
// is exceeded.
func (s *NFSFacade) Stop(ctx context.Context) error {
	s.initiateShutdown()
	return s.gracefulShutdown()
}

// GetActiveConnections returns the current number of active connections.
func (s *NFSFacade) GetActiveConnections() int32 {
	return s.connCount.Load()
}

// newConn creates a new connection wrapper for a TCP connection.
func (s *NFSFacade) newConn(tcpConn net.Conn) *conn {
	return &conn{
		server: s,
		conn:   tcpConn,
	}
}

func (s *NFSFacade) Port() int {
	return s.config.Port
}

func (s *NFSFacade) Protocol() string {
	return "NFS"
}
