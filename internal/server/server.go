package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/marmos91/dittofs/internal/content"
	"github.com/marmos91/dittofs/internal/logger"
	"github.com/marmos91/dittofs/internal/metadata"
)

// DittoServer manages multiple protocol facades that share common repositories.
// It coordinates the lifecycle of all facades and ensures proper shutdown.
type DittoServer struct {
	metadata metadata.Repository
	content  content.Repository
	facades  []Facade
	mu       sync.RWMutex
}

// New creates a new DittoServer with the provided repositories.
// The repositories are shared across all facades added to this server.
func New(metadata metadata.Repository, content content.Repository) *DittoServer {
	return &DittoServer{
		metadata: metadata,
		content:  content,
		facades:  make([]Facade, 0),
	}
}

// AddFacade registers a new protocol facade with the server.
// This method can be called multiple times to add different protocol facades.
// It returns the server instance to allow method chaining.
func (s *DittoServer) AddFacade(facade Facade) *DittoServer {
	s.mu.Lock()
	defer s.mu.Unlock()

	facade.SetRepositories(s.metadata, s.content)
	s.facades = append(s.facades, facade)
	logger.Info("Added facade: %s on port %d", facade.Protocol(), facade.Port())

	return s
}

// Serve starts all registered facades and blocks until the context is cancelled
// or an error occurs. When the context is cancelled, it gracefully shuts down
// all facades in reverse order.
//
// If any facade fails to start, Serve stops all previously started facades
// and returns the error.
func (s *DittoServer) Serve(ctx context.Context) error {
	s.mu.RLock()
	facades := make([]Facade, len(s.facades))
	copy(facades, s.facades)
	s.mu.RUnlock()

	if len(facades) == 0 {
		return fmt.Errorf("no facades registered")
	}

	// Channel to collect errors from facade goroutines
	errChan := make(chan error, len(facades))

	// WaitGroup to track facade goroutines
	var wg sync.WaitGroup

	// Start all facades
	for _, facade := range facades {
		wg.Add(1)
		go func(f Facade) {
			defer wg.Done()

			logger.Info("Starting %s facade on port %d", f.Protocol(), f.Port())

			if err := f.Serve(ctx); err != nil {
				logger.Error("%s facade failed: %v", f.Protocol(), err)
				errChan <- fmt.Errorf("%s facade error: %w", f.Protocol(), err)
			} else {
				logger.Info("%s facade stopped", f.Protocol())
			}
		}(facade)
	}

	// Wait for either context cancellation or facade error
	select {
	case <-ctx.Done():
		logger.Info("Shutdown signal received, stopping all facades")
		s.stopAllFacades(facades)
	case err := <-errChan:
		logger.Error("Facade error detected, stopping all facades: %v", err)
		s.stopAllFacades(facades)
		wg.Wait()
		return err
	}

	// Wait for all facades to finish
	wg.Wait()
	logger.Info("All facades stopped")

	return ctx.Err()
}

// stopAllFacades gracefully stops all facades in reverse order.
// This is called during shutdown to ensure proper cleanup.
func (s *DittoServer) stopAllFacades(facades []Facade) {
	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop facades in reverse order
	for i := len(facades) - 1; i >= 0; i-- {
		facade := facades[i]
		logger.Info("Stopping %s facade", facade.Protocol())

		if err := facade.Stop(ctx); err != nil {
			logger.Error("Error stopping %s facade: %v", facade.Protocol(), err)
		}
	}
}

// Facades returns a copy of the currently registered facades.
// This method is safe for concurrent use.
func (s *DittoServer) Facades() []Facade {
	s.mu.RLock()
	defer s.mu.RUnlock()

	facades := make([]Facade, len(s.facades))
	copy(facades, s.facades)
	return facades
}
