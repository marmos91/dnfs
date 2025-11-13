package testing

import (
	"context"
	"testing"

	"github.com/marmos91/dittofs/pkg/content"
)

// StoreTestSuite is a comprehensive test suite for ContentStore implementations.
// It tests the interface contract, not implementation details, making it reusable
// across different implementations (memory, filesystem, S3, etc.).
//
// Usage:
//
//	func TestMyContentStore(t *testing.T) {
//	    suite := &testing.StoreTestSuite{
//	        NewStore: func() content.ContentStore {
//	            return mystore.New()
//	        },
//	    }
//	    suite.Run(t)
//	}
type StoreTestSuite struct {
	// NewStore is a factory function that creates a fresh ContentStore instance
	// for each test. This ensures test isolation.
	NewStore func() content.ContentStore
}

// Run executes all tests in the suite.
func (suite *StoreTestSuite) Run(t *testing.T) {
	t.Run("BasicOperations", suite.RunBasicTests)
	t.Run("WriteOperations", suite.RunWriteTests)
	t.Run("SeekableOperations", suite.RunSeekableTests)
	t.Run("GarbageCollection", suite.RunGCTests)
	t.Run("Statistics", suite.RunStatsTests)
}

// testContext returns a standard test context.
func testContext() context.Context {
	return context.Background()
}
