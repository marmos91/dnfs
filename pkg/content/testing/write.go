package testing

import (
	"testing"

	"github.com/marmos91/dittofs/pkg/content"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunWriteTests executes all WritableContentStore operation tests.
func (suite *StoreTestSuite) RunWriteTests(t *testing.T) {
	t.Run("WriteContent_Basic", suite.testWriteContentBasic)
	t.Run("WriteContent_Overwrite", suite.testWriteContentOverwrite)
	t.Run("WriteAt_Basic", suite.testWriteAtBasic)
	t.Run("WriteAt_CreateNew", suite.testWriteAtCreateNew)
	t.Run("WriteAt_SparseFile", suite.testWriteAtSparseFile)
	t.Run("WriteAt_NegativeOffset", suite.testWriteAtNegativeOffset)
	t.Run("Truncate_Shrink", suite.testTruncateShrink)
	t.Run("Truncate_Grow", suite.testTruncateGrow)
	t.Run("Truncate_NotFound", suite.testTruncateNotFound)
	t.Run("Delete_Success", suite.testDeleteSuccess)
	t.Run("Delete_Idempotent", suite.testDeleteIdempotent)
	t.Run("WriteAt_Append", suite.testWriteAtAppend)
}

// ============================================================================
// WriteContent Tests
// ============================================================================

func (suite *StoreTestSuite) testWriteContentBasic(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("write-basic")
	testData := []byte("Hello, World!")

	// Write content
	mustWriteContent(t, writable, id, testData)

	// Verify content
	assertContentEquals(t, store, id, testData)
	assertContentSize(t, store, id, uint64(len(testData)))
}

func (suite *StoreTestSuite) testWriteContentOverwrite(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("write-overwrite")
	oldData := []byte("Old data")
	newData := []byte("New data that is longer")

	// Write initial content
	mustWriteContent(t, writable, id, oldData)
	assertContentEquals(t, store, id, oldData)

	// Overwrite with new content
	mustWriteContent(t, writable, id, newData)
	assertContentEquals(t, store, id, newData)
	assertContentSize(t, store, id, uint64(len(newData)))
}

// ============================================================================
// WriteAt Tests
// ============================================================================

func (suite *StoreTestSuite) testWriteAtBasic(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("writeat-basic")

	// Write at offset 0
	mustWriteAt(t, writable, id, []byte("Hello"), 0)
	assertContentEquals(t, store, id, []byte("Hello"))

	// Write at offset 5 (append)
	mustWriteAt(t, writable, id, []byte(", World"), 5)
	assertContentEquals(t, store, id, []byte("Hello, World"))
}

func (suite *StoreTestSuite) testWriteAtCreateNew(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("writeat-create")
	testData := []byte("Created via WriteAt")

	// WriteAt should create new content
	mustWriteAt(t, writable, id, testData, 0)
	assertContentExists(t, store, id, true)
	assertContentEquals(t, store, id, testData)
}

func (suite *StoreTestSuite) testWriteAtSparseFile(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("writeat-sparse")

	// Write at offset 100 (should fill 0-99 with zeros)
	testData := []byte("Data")
	mustWriteAt(t, writable, id, testData, 100)

	// Verify size
	assertContentSize(t, store, id, 104) // 100 zeros + 4 bytes

	// Verify content
	data := mustReadContent(t, store, id)
	assert.Equal(t, 104, len(data))

	// Check zeros before data
	for i := 0; i < 100; i++ {
		assert.Equal(t, byte(0), data[i], "Expected zero at position %d", i)
	}

	// Check actual data
	assert.Equal(t, testData, data[100:104])
}

func (suite *StoreTestSuite) testWriteAtNegativeOffset(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("writeat-negative")

	// Negative offset should error
	err := writable.WriteAt(testContext(), id, []byte("data"), -1)
	AssertErrorIs(t, content.ErrInvalidOffset, err)
}

func (suite *StoreTestSuite) testWriteAtAppend(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("writeat-append")

	// Write initial data
	mustWriteContent(t, writable, id, []byte("Hello"))

	// Append via WriteAt
	mustWriteAt(t, writable, id, []byte(" World"), 5)

	// Verify
	assertContentEquals(t, store, id, []byte("Hello World"))
}

// ============================================================================
// Truncate Tests
// ============================================================================

func (suite *StoreTestSuite) testTruncateShrink(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("truncate-shrink")
	testData := []byte("Hello, World!")

	// Write content
	mustWriteContent(t, writable, id, testData)
	assertContentSize(t, store, id, uint64(len(testData)))

	// Truncate to 5 bytes
	mustTruncate(t, writable, id, 5)

	// Verify
	assertContentSize(t, store, id, 5)
	assertContentEquals(t, store, id, []byte("Hello"))
}

func (suite *StoreTestSuite) testTruncateGrow(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("truncate-grow")
	testData := []byte("Hello")

	// Write content
	mustWriteContent(t, writable, id, testData)
	assertContentSize(t, store, id, uint64(len(testData)))

	// Truncate to 10 bytes (extend with zeros)
	mustTruncate(t, writable, id, 10)

	// Verify
	assertContentSize(t, store, id, 10)
	data := mustReadContent(t, store, id)
	assert.Equal(t, []byte("Hello"), data[0:5])
	assert.Equal(t, []byte{0, 0, 0, 0, 0}, data[5:10])
}

func (suite *StoreTestSuite) testTruncateNotFound(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("truncate-notfound")

	// Truncate non-existent content should error
	err := writable.Truncate(testContext(), id, 100)
	AssertErrorIs(t, content.ErrContentNotFound, err)
}

// ============================================================================
// Delete Tests
// ============================================================================

func (suite *StoreTestSuite) testDeleteSuccess(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("delete-success")
	testData := []byte("To be deleted")

	// Write content
	mustWriteContent(t, writable, id, testData)
	assertContentExists(t, store, id, true)

	// Delete content
	mustDelete(t, writable, id)

	// Verify deleted
	assertContentExists(t, store, id, false)

	// Read should fail
	_, err := store.ReadContent(testContext(), id)
	AssertErrorIs(t, content.ErrContentNotFound, err)
}

func (suite *StoreTestSuite) testDeleteIdempotent(t *testing.T) {
	store := suite.NewStore()
	writable, ok := store.(content.WritableContentStore)
	if !ok {
		t.Skip("Store does not implement WritableContentStore")
	}

	id := generateTestID("delete-idempotent")

	// Delete non-existent content should succeed (idempotent)
	err := writable.Delete(testContext(), id)
	require.NoError(t, err)

	// Delete again should still succeed
	err = writable.Delete(testContext(), id)
	require.NoError(t, err)
}
