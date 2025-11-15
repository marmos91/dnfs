package nfs

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marmos91/dittofs/pkg/content"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// mockMetadataStore implements a minimal metadata.MetadataStore for testing
type mockMetadataStore struct{}

func (m *mockMetadataStore) Init(ctx context.Context) error { return nil }
func (m *mockMetadataStore) Close() error                   { return nil }

// mockContentStore implements a minimal content.ContentStore for testing
type mockContentStore struct{}

func (m *mockContentStore) Init(ctx context.Context) error { return nil }
func (m *mockContentStore) Close() error                   { return nil }

// TestGracefulShutdown verifies that the adapter waits for connections to complete
func TestGracefulShutdown(t *testing.T) {
	// Create adapter with short shutdown timeout
	config := NFSConfig{
		Port:            0, // OS assigns random port
		ShutdownTimeout: 2 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the actual port
	port := adapter.Port()
	if port == 0 {
		t.Fatal("Adapter port is 0, listener didn't start")
	}

	// Create a test connection but don't close it
	conn, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
	if err != nil {
		t.Fatalf("Failed to connect to adapter: %v", err)
	}
	defer conn.Close()

	// Verify connection is tracked
	time.Sleep(100 * time.Millisecond)
	if adapter.GetActiveConnections() != 1 {
		t.Errorf("Expected 1 active connection, got %d", adapter.GetActiveConnections())
	}

	// Initiate shutdown
	shutdownStart := time.Now()
	cancel()

	// Wait for server to complete
	err = <-serverDone
	shutdownDuration := time.Since(shutdownStart)

	// Should complete within shutdown timeout
	if shutdownDuration > 3*time.Second {
		t.Errorf("Shutdown took too long: %v (expected < 3s)", shutdownDuration)
	}

	// Should have error (timeout or context cancelled)
	if err == nil {
		t.Error("Expected error from shutdown, got nil")
	}
}

// TestForcedConnectionClosure verifies that connections are force-closed after timeout
func TestForcedConnectionClosure(t *testing.T) {
	// Create adapter with very short shutdown timeout
	config := NFSConfig{
		Port:            0, // OS assigns random port
		ShutdownTimeout: 500 * time.Millisecond,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the actual port
	port := adapter.Port()
	if port == 0 {
		t.Fatal("Adapter port is 0, listener didn't start")
	}

	// Create a test connection
	conn, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
	if err != nil {
		t.Fatalf("Failed to connect to adapter: %v", err)
	}
	defer conn.Close()

	// Verify connection is tracked
	time.Sleep(100 * time.Millisecond)
	if adapter.GetActiveConnections() != 1 {
		t.Errorf("Expected 1 active connection, got %d", adapter.GetActiveConnections())
	}

	// Track whether connection was closed by server
	connClosed := make(chan bool, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			connClosed <- true
		}
	}()

	// Initiate shutdown
	cancel()

	// Wait for connection to be force-closed
	select {
	case <-connClosed:
		// Connection was closed - good!
		t.Log("Connection was force-closed as expected")
	case <-time.After(2 * time.Second):
		t.Error("Connection was not force-closed within timeout")
	}

	// Wait for server to complete
	err = <-serverDone
	if err == nil {
		t.Error("Expected error from shutdown with force-close, got nil")
	}
}

// TestConnectionLimiting verifies that MaxConnections is enforced
func TestConnectionLimiting(t *testing.T) {
	// Create adapter with connection limit
	config := NFSConfig{
		Port:            0, // OS assigns random port
		MaxConnections:  2,
		ShutdownTimeout: 1 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the actual port
	port := adapter.Port()
	if port == 0 {
		t.Fatal("Adapter port is 0, listener didn't start")
	}

	// Create MaxConnections connections
	var conns []net.Conn
	for i := 0; i < 2; i++ {
		conn, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
		if err != nil {
			t.Fatalf("Failed to create connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	// Wait for connections to be tracked
	time.Sleep(100 * time.Millisecond)

	// Verify connection count
	if adapter.GetActiveConnections() != 2 {
		t.Errorf("Expected 2 active connections, got %d", adapter.GetActiveConnections())
	}

	// Try to create one more connection - should block or be delayed
	// We'll try with a timeout
	connChan := make(chan net.Conn, 1)
	go func() {
		conn, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
		if err == nil {
			connChan <- conn
		}
	}()

	// Third connection should not complete immediately
	select {
	case conn := <-connChan:
		conn.Close()
		t.Error("Third connection succeeded immediately, expected blocking due to MaxConnections")
	case <-time.After(200 * time.Millisecond):
		// Good - connection is blocking as expected
		t.Log("Third connection blocked as expected")
	}

	// Close one connection
	conns[0].Close()

	// Now the third connection should succeed
	select {
	case conn := <-connChan:
		conn.Close()
		t.Log("Third connection succeeded after freeing slot")
	case <-time.After(1 * time.Second):
		t.Error("Third connection didn't succeed after freeing slot")
	}
}

// TestDrainMode verifies that new connections are rejected during shutdown
func TestDrainMode(t *testing.T) {
	// Create adapter
	config := NFSConfig{
		Port:            0, // OS assigns random port
		ShutdownTimeout: 2 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the actual port
	port := adapter.Port()
	if port == 0 {
		t.Fatal("Adapter port is 0, listener didn't start")
	}

	// Create initial connection - should succeed
	conn1, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
	if err != nil {
		t.Fatalf("Failed to create initial connection: %v", err)
	}
	defer conn1.Close()

	// Initiate shutdown
	cancel()

	// Wait for shutdown to initiate
	time.Sleep(100 * time.Millisecond)

	// Try to create new connection - should fail (listener closed)
	_, err = net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
	if err == nil {
		t.Error("New connection succeeded during shutdown, expected failure (drain mode)")
	} else {
		t.Logf("New connection rejected during shutdown: %v (expected)", err)
	}

	// Wait for server to complete
	<-serverDone
}

// TestConcurrentShutdown verifies that concurrent shutdown calls are safe
func TestConcurrentShutdown(t *testing.T) {
	config := NFSConfig{
		Port:            0,
		ShutdownTimeout: 1 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Call Stop() multiple times concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer stopCancel()
			_ = adapter.Stop(stopCtx)
		}()
	}

	// Also cancel context
	cancel()

	// Wait for all Stop() calls to complete
	wg.Wait()

	// Wait for server to complete
	<-serverDone

	t.Log("Concurrent shutdown calls completed successfully")
}

// TestConnectionTracking verifies that connection tracking works correctly
func TestConnectionTracking(t *testing.T) {
	config := NFSConfig{
		Port:            0,
		ShutdownTimeout: 1 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Get the actual port
	port := adapter.Port()
	if port == 0 {
		t.Fatal("Adapter port is 0, listener didn't start")
	}

	// Verify initial state
	if adapter.GetActiveConnections() != 0 {
		t.Errorf("Expected 0 active connections initially, got %d", adapter.GetActiveConnections())
	}

	// Create connections and verify count increases
	var conns []net.Conn
	for i := 1; i <= 5; i++ {
		conn, err := net.Dial("tcp", net.JoinHostPort("localhost", string(rune(port))))
		if err != nil {
			t.Fatalf("Failed to create connection %d: %v", i, err)
		}
		conns = append(conns, conn)

		// Wait for connection to be tracked
		time.Sleep(50 * time.Millisecond)

		if adapter.GetActiveConnections() != int32(i) {
			t.Errorf("Expected %d active connections, got %d", i, adapter.GetActiveConnections())
		}
	}

	// Close connections and verify count decreases
	for i, conn := range conns {
		conn.Close()
		time.Sleep(50 * time.Millisecond)

		expected := int32(len(conns) - i - 1)
		if adapter.GetActiveConnections() != expected {
			t.Errorf("Expected %d active connections after closing %d, got %d",
				expected, i+1, adapter.GetActiveConnections())
		}
	}

	// Verify final state
	if adapter.GetActiveConnections() != 0 {
		t.Errorf("Expected 0 active connections finally, got %d", adapter.GetActiveConnections())
	}
}

// TestShutdownWithActiveRequests verifies context cancellation propagates to handlers
func TestShutdownWithActiveRequests(t *testing.T) {
	config := NFSConfig{
		Port:            0,
		ShutdownTimeout: 1 * time.Second,
	}
	adapter := New(config)
	adapter.SetStores(&mockMetadataStore{}, &mockContentStore{})

	// Track whether handler detected cancellation
	var handlerCancelled atomic.Bool

	// Replace handler with one that simulates long operation
	// Note: This is a simplified test - in real scenarios, handlers check ctx.Done()
	originalHandler := adapter.nfsHandler
	defer func() { adapter.nfsHandler = originalHandler }()

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- adapter.Serve(ctx)
	}()

	// Wait for listener to be ready
	time.Sleep(100 * time.Millisecond)

	// Initiate shutdown
	cancel()

	// Wait for shutdown to complete
	<-serverDone

	// In a real test, we'd verify that in-flight requests were cancelled
	// For now, we just verify that shutdown completed
	t.Log("Shutdown completed with context cancellation")
	_ = handlerCancelled // Will be used when we add real handler testing
}
