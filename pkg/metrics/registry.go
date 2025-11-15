// Package metrics provides Prometheus metrics collection for DittoFS components.
//
// All metrics are optional - if not initialized, components use no-op implementations
// that have zero overhead. This allows DittoFS to run with or without metrics
// collection enabled.
//
// Usage:
//
//	// Initialize global registry (typically in main.go)
//	metrics.InitRegistry()
//
//	// Create metrics instances for components
//	nfsMetrics := metrics.NewNFSMetrics()
//	s3Metrics := metrics.NewS3Metrics()
//
//	// Or use nil for no-op behavior
//	adapter := nfs.New(config, nil) // No metrics
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// registry is the global Prometheus registry for all DittoFS metrics
	// Protected by registryOnce for write-once, read-many pattern
	registry     *prometheus.Registry
	registryOnce sync.Once
)

// InitRegistry initializes the global Prometheus registry.
//
// This must be called before creating any metrics instances. It's safe to call
// multiple times - subsequent calls are ignored.
//
// If not called, GetRegistry() will return nil and all metrics constructors
// will return no-op implementations.
//
// Thread safety:
// sync.Once provides the necessary memory barriers to ensure the registry
// write is visible to all subsequent reads.
func InitRegistry() {
	registryOnce.Do(func() {
		registry = prometheus.NewRegistry()
	})
}

// GetRegistry returns the global Prometheus registry.
//
// Returns nil if InitRegistry() has not been called, indicating metrics
// are disabled.
//
// Thread safety:
// Safe to call concurrently. The sync.Once in InitRegistry() provides
// a happens-before relationship ensuring the registry value is visible.
func GetRegistry() *prometheus.Registry {
	return registry
}

// IsEnabled returns true if metrics collection is enabled.
//
// Metrics are enabled if InitRegistry() has been called.
func IsEnabled() bool {
	return GetRegistry() != nil
}
