# UUID-Based File Handle Implementation Plan

## Problem Statement

Current file handle format (`/export:/path/to/file`) exceeds NFS RFC 1813's 64-byte limit for deeply nested paths:
- Example: `/export:/Downloads/zen-browser-main/themes/Frappe/Blue/userChrome.css` = **73 bytes** ✗
- Causes: "Input/output error" on macOS during large folder copies
- Hash-based handles (`H:...`) broke the registry pattern (no share name extraction)

## Solution: UUID-Based File Handles

### Design Decisions

1. **Use UUID v4 (random)** instead of uint64 for file IDs
   - Standard across protocols (NFS, SMB uses GUIDs)
   - No collision risk, no coordination needed
   - Distributed-ready for future clustering
   - Handle format: `/export:550e8400-e29b-41d4-a716-446655440000` = **45 bytes** ✓

2. **Separate File (identity) from FileAttr (attributes)**
   - File = WHO the file is (ID, ShareName, Path)
   - FileAttr = WHAT the file is (size, mode, times, etc.)
   - NO duplication between structs

3. **Pass File struct to content stores**
   - Each store decides: use `file.ID` or `file.Path`
   - S3 uses `file.Path` as object key
   - Filesystem could use either

### New Structures

```go
// pkg/store/metadata/file.go

import "github.com/google/uuid"

// File represents a file's complete identity and attributes.
// Embeds FileAttr for convenience (file.Mode vs file.Attr.Mode)
type File struct {
    // ID is a unique identifier for this file
    // Used in protocol handles: "/export:550e8400-e29b-41d4-a716-446655440000"
    ID uuid.UUID

    // ShareName is the share this file belongs to (e.g., "/export")
    ShareName string

    // Path is the full path within the share (e.g., "/documents/report.pdf")
    // Used by storage backends that need paths (S3, human-readable filesystems)
    Path string

    // Embedded FileAttr for convenient access to attributes
    // Access via: file.Mode, file.Size, file.Mtime (not file.Attr.Mode)
    FileAttr
}

// FileAttr remains UNCHANGED
type FileAttr struct {
    Type      FileType
    Mode      uint32
    Size      uint64
    UID       uint32
    GID       uint32
    Atime     time.Time
    Mtime     time.Time
    Ctime     time.Time
    ContentID ContentID
    LinkTarget string
}
```

## Implementation Steps

### Phase 1: Add File Struct and UUID Support

**Files to modify:**
1. `pkg/store/metadata/file.go`
   - Add `File` struct (as above)
   - Keep `FileAttr` unchanged
   - Add helper: `EncodeFileHandle(file *File) FileHandle`
   - Add helper: `DecodeFileHandle(handle FileHandle) (*File, error)`

2. `go.mod`
   - Add dependency: `github.com/google/uuid v1.6.0`

### Phase 2: Update Handle Encoding

**Files to modify:**
1. `pkg/store/metadata/handle.go`
   - Update `EncodeShareHandle()` to use UUID format
   - Update `DecodeShareHandle()` to parse UUID
   - Format: `shareName:uuid` (e.g., `/export:550e8400-e29b-41d4-a716-446655440000`)

2. `pkg/store/metadata/badger/handle.go`
   - Remove hash-based handle logic entirely (lines 40-95)
   - Update `generateFileHandle()` to:
     ```go
     func generateFileHandle(file *metadata.File) (metadata.FileHandle, error) {
         handle := file.ShareName + ":" + file.ID.String()
         if len(handle) > 64 {
             return nil, fmt.Errorf("handle too long") // Should never happen
         }
         return metadata.FileHandle([]byte(handle)), nil
     }
     ```
   - Update `decodeFileHandle()` to parse UUID from handle
   - Remove `isHashBased` logic, `lookupHashedHandlePath()`, etc.

### Phase 3: Update BadgerDB Internal Storage

**Efficiency Considerations:**
- Keys: Minimal (just UUID, 36 bytes)
- Values: Store `File` directly as JSON (no conversion overhead)
- Indexes: Keep parent→children for READDIR performance
- NO duplication: File embeds FileAttr, stored once

**Files to modify:**
1. `pkg/store/metadata/badger/serialization.go`
   - **REMOVE** `fileData` struct entirely
   - Store `*metadata.File` directly:
     ```go
     // Just use File everywhere - no intermediate struct needed!

     func encodeFile(file *metadata.File) ([]byte, error) {
         return json.Marshal(file)
     }

     func decodeFile(data []byte) (*metadata.File, error) {
         var file metadata.File
         err := json.Unmarshal(data, &file)
         return &file, err
     }
     ```
   - JSON structure (all fields at root, FileAttr embedded):
     ```json
     {
       "id": "550e8400-e29b-41d4-a716-446655440000",
       "share_name": "/export",
       "path": "/documents/report.pdf",
       "type": 0,
       "mode": 644,
       "uid": 1000,
       "gid": 1000,
       "size": 4096,
       "atime": "2025-11-21T14:00:00Z",
       "mtime": "2025-11-21T14:00:00Z",
       "ctime": "2025-11-21T14:00:00Z",
       "content_id": "export/documents/report.pdf"
     }
     ```

2. **Database Key Structure (IMPORTANT for efficiency):**
   ```
   Keys:
   - file:{uuid}           → *File (JSON encoded, all data in one place)
   - dir:{parentUUID}:{childName} → childUUID (for LOOKUP)
   - children:{parentUUID} → []childEntry (for READDIR)

   Example:
   - file:550e8400-... → {id, share_name, path, type, mode, size, ...}
   - dir:root-uuid:report.pdf → 550e8400-...
   - children:root-uuid → [{name: "report.pdf", id: 550e8400-...}, ...]
   ```

   **Why this structure:**
   - LOOKUP: O(1) - direct key `dir:{parent}:{name}`
   - READDIR: O(1) - direct key `children:{parent}`
   - GETATTR: O(1) - direct key `file:{uuid}`
   - No path walking needed!

3. `pkg/store/metadata/badger/file.go`
   - Update `CreateFile()`:
     ```go
     CreateFile(ctx, dirHandle, name, ...) (*metadata.File, error) {
         id := uuid.New()
         parentFile := loadFile(dirHandle) // get parent File
         fullPath := parentFile.Path + "/" + name

         file := &metadata.File{
             ID:        id,
             ShareName: parentFile.ShareName,
             Path:      fullPath,
             FileAttr:  FileAttr{
                 Type:      metadata.FileTypeRegular,
                 Mode:      0644,
                 UID:       ctx.UID,
                 GID:       ctx.GID,
                 Size:      0,
                 Atime:     time.Now(),
                 Mtime:     time.Now(),
                 Ctime:     time.Now(),
                 ContentID: buildContentID(parentFile.ShareName, fullPath),
             },
         }

         // Write 3 keys atomically (File stored directly, no conversion):
         txn.Set([]byte("file:" + id.String()), encodeFile(file))
         txn.Set([]byte("dir:" + parentID.String() + ":" + name), id.Bytes())
         txn.Update("children:" + parentID.String(), append(child))

         return file, nil
     }
     ```

4. `pkg/store/metadata/badger/directory.go`
   - `Lookup()` now:
     ```go
     Lookup(ctx, dirHandle, name) (*metadata.File, error) {
         // 1. Get child UUID from index
         childID := get("dir:" + dirUUID + ":" + name)

         // 2. Load File directly (no conversion needed!)
         file := decodeFile(get("file:" + childID))

         return file, nil
     }
     ```
   - `ReadDirectory()`:
     ```go
     ReadDirectory(ctx, dirHandle) ([]*metadata.File, error) {
         children := get("children:" + dirUUID)
         files := make([]*metadata.File, len(children))
         for i, child := range children {
             files[i] = decodeFile(get("file:" + child.ID))
         }
         return files, nil
     }
     ```

5. **Migration Strategy:**
   ```go
   // One-time migration function
   func migrateToUUID(db *badger.DB) error {
       // For each old "file:{shareName}:{path}" key:
       txn := db.NewTransaction(true)

       iter := txn.NewIterator(badger.DefaultIteratorOptions)
       for iter.Rewind(); iter.Valid(); iter.Next() {
           item := iter.Item()
           oldKey := string(item.Key())

           if !strings.HasPrefix(oldKey, "file:/") {
               continue // skip non-path-based keys
           }

           // Generate new UUID
           id := uuid.New()

           // Load old file data
           oldFile := decodeFile(item.Value()) // might need old decoder

           // Create new File with UUID
           newFile := &metadata.File{
               ID:        id,
               ShareName: extractShare(oldKey),
               Path:      extractPath(oldKey),
               FileAttr:  oldFile.FileAttr, // copy attributes
           }

           // Write new key (File stored directly)
           txn.Set([]byte("file:" + id.String()), encodeFile(newFile))

           // Delete old key
           txn.Delete(item.Key())
       }

       return txn.Commit()
   }
   ```

### Phase 4: Update Metadata Store Interface

**Files to modify:**
1. `pkg/store/metadata/store.go`
   - Update interface methods to return `*File` (which embeds FileAttr):
     ```go
     type MetadataStore interface {
         // Returns File (has ID, ShareName, Path, and embedded FileAttr)
         Lookup(ctx, dirHandle, name) (*File, error)
         CreateFile(ctx, dirHandle, name, ...) (*File, error)
         CreateDirectory(ctx, dirHandle, name, ...) (*File, error)
         GetAttributes(ctx, handle) (*File, error)
         SetFileAttributes(ctx, handle, attrs) (*File, error)
         // ... etc
     }
     ```
   - Note: File embeds FileAttr, so `file.Mode`, `file.Size` work directly

2. `pkg/store/metadata/memory/store.go`
   - Same interface updates as BadgerDB
   - Use `uuid.New()` for file IDs
   - Store: `map[uuid.UUID]*File` (efficient, no duplication)

### Phase 5: Update Content Store Interface

**Key Design Decision:**
Each content store decides how to generate ContentIDs based on its needs:
- S3 uses path as object key
- Filesystem could use ID or path
- Future stores can use custom schemes

**Files to modify:**
1. `pkg/store/content/store.go`
   - Add `GenerateContentID()` method:
     ```go
     type ContentStore interface {
         // GenerateContentID generates a ContentID for a new file.
         // Each store implementation decides what to use (ID, path, or custom scheme).
         //
         // Parameters:
         //   - fileID: Unique UUID for the file
         //   - path: Full path within share (e.g., "/documents/report.pdf")
         //
         // Returns:
         //   - ContentID: Store-specific identifier for content operations
         GenerateContentID(fileID uuid.UUID, path string) ContentID

         // Content operations use ContentID (not File)
         WriteAt(contentID ContentID, data []byte, offset int64) error
         ReadAt(contentID ContentID, offset int64, length int) ([]byte, error)
         Truncate(contentID ContentID, size int64) error
         Delete(contentID ContentID) error
     }
     ```

2. `pkg/store/content/s3/store.go`
   - Implement `GenerateContentID()`:
     ```go
     func (s *S3Store) GenerateContentID(fileID uuid.UUID, path string) metadata.ContentID {
         // S3 uses path as object key (human-readable, path-based)
         // Example: "export/documents/report.pdf"
         return metadata.ContentID(buildS3Key(path))
     }
     ```

3. `pkg/store/content/fs/store.go`
   - Implement `GenerateContentID()`:
     ```go
     func (s *FilesystemStore) GenerateContentID(fileID uuid.UUID, path string) metadata.ContentID {
         // Option 1: Use path for human-readable structure
         return metadata.ContentID(filepath.Join(s.basePath, path))

         // Option 2: Use UUID for flat structure
         // return metadata.ContentID(fileID.String())

         // Filesystem can choose based on configuration
     }
     ```

4. `pkg/store/content/memory/store.go`
   - Implement `GenerateContentID()`:
     ```go
     func (s *MemoryStore) GenerateContentID(fileID uuid.UUID, path string) metadata.ContentID {
         // Memory uses UUID (simple, unique)
         return metadata.ContentID(fileID.String())
     }
     ```

### Phase 6: Update Protocol Handlers

**Files to modify:**
1. `internal/protocol/nfs/v3/handlers/*.go`
   - All handlers receive `*File` from metadata store
   - Convert `file.ID` to NFS handle for responses
   - Extract `file` from handle in requests

2. `pkg/adapter/nfs/nfs_connection.go`
   - Update metrics recording to use `file.Path` for logging
   - Handle encoding/decoding in dispatch layer

3. `internal/protocol/nfs/dispatch.go`
   - Update handlers to work with `*File`
   - Convert between `FileHandle` and `*File` at dispatch boundary

### Phase 7: Update Registry

**Files to modify:**
1. `pkg/registry/access.go`
   - `GetShareNameForHandle()` should decode UUID handle
   - Extract share name from handle format: `shareName:uuid`

### Phase 8: Make BadgerDB Cache Configurable

**Performance Issue**: During large folder operations, BadgerDB shows cache thrashing:
- Cache hit ratio: 2% (98% misses)
- Symptoms: "Block cache might be too small" warnings
- Cause: Default 256MB block cache insufficient for large working sets

**Files to modify:**
1. `pkg/store/metadata/badger/store.go`
   - Add `mapstructure` tags to existing cache fields (lines 109, 113):
     ```go
     // BlockCacheSizeMB is BadgerDB's block cache size in MB (default: 256)
     // This caches LSM-tree data blocks for faster reads
     BlockCacheSizeMB int64 `mapstructure:"block_cache_size_mb"`

     // IndexCacheSizeMB is BadgerDB's index cache size in MB (default: 128)
     // This caches LSM-tree indices for faster lookups
     IndexCacheSizeMB int64 `mapstructure:"index_cache_size_mb"`
     ```
   - The fields already exist, they just need mapstructure tags for YAML decoding

2. `config.yaml` (or equivalent config format)
   - Add cache configuration section:
     ```yaml
     metadata:
       stores:
         persistent-meta:
           type: badger
           badger:
             db_path: /data/metadata
             block_cache_size_mb: 512   # Increase for large operations
             index_cache_size_mb: 256   # Increase for large operations
     ```

3. Update documentation:
   - Document cache size recommendations:
     - Small workloads (<1000 files): 256MB block / 128MB index (default)
     - Medium workloads (1000-10000 files): 512MB block / 256MB index
     - Large workloads (>10000 files): 1024MB block / 512MB index
   - Add monitoring section: how to interpret BadgerDB cache warnings

**Testing:**
- Test with various cache sizes
- Monitor cache hit ratio during large folder operations
- Verify no "Block cache might be too small" warnings with increased cache

## Testing Strategy

### Unit Tests
1. Test UUID generation and uniqueness
2. Test handle encoding/decoding with UUIDs
3. Test File struct marshaling/unmarshaling
4. Test content store operations with File struct

### Integration Tests
1. Create files with long paths (>64 bytes when path-based)
2. Verify handles are <64 bytes
3. Test LOOKUP, GETATTR, READ, WRITE with UUID handles
4. Test directory operations (READDIR, etc.)

### E2E Tests
1. Mount NFS share
2. Copy large folder with deeply nested paths
3. Verify no "Input/output error" or "Stale NFS file handle"
4. Check Prometheus metrics work correctly

## Migration Notes

### Existing Data
- Old BadgerDB databases with path-based handles need migration
- Migration script:
  1. Read all `file:*` keys
  2. For each file, generate UUID
  3. Create new `File` struct with UUID + path
  4. Write to new key format
  5. Delete old keys

### Backward Compatibility
- NOT backward compatible with existing NFS clients
- Clients must remount after upgrade
- Persistent file handles will be invalidated

## Dependencies

```bash
go get github.com/google/uuid@v1.6.0
```

## Success Criteria

- ✅ All file handles <64 bytes (NFS RFC 1813 compliant)
- ✅ Large folder copies work without errors
- ✅ No "Stale NFS file handle" errors
- ✅ Content stores can use either ID or Path
- ✅ All tests pass
- ✅ Prometheus metrics continue working

## Known Issues to Address

1. **Current bug**: Hash handles (`H:...`) break SETATTR
   - Fixed by: Removing hash handles entirely
2. **Content store interface**: Currently uses `ContentID string`
   - Fixed by: Passing `*File` instead, stores choose ID or Path
3. **BadgerDB cache thrashing**: Cache hit ratio of 2% during large folder operations
   - Problem: Default 256MB block cache too small for large working sets
   - Symptoms: "Block cache might be too small" warnings, slow performance
   - Solution: Make cache sizes configurable via config file
   - Recommendation: Allow 512MB-1GB for large folder operations

## Notes

- UUID format: RFC 4122 version 4 (random)
- Handle format: `shareName:uuid` (no colons in UUID string representation)
- Max handle size: 45 bytes (well under 64-byte limit)
- Future SMB support: Can use UUID directly as persistent file ID

## Key Simplifications

1. **One struct to rule them all**: `File` everywhere
   - No `fileData` intermediate struct
   - No conversion overhead
   - Embedded `FileAttr` for convenient access: `file.Mode` vs `file.Attr.Mode`

2. **Clean storage**: BadgerDB stores `*File` directly as JSON
   - All fields at root level (flat structure)
   - Simple encode/decode: `json.Marshal(file)`

3. **UUID handles**: Always <64 bytes (NFS RFC compliant)
   - Format: `/export:550e8400-e29b-41d4-a716-446655440000`
   - No more path length issues!

## Summary

**Before (path-based):**
- Handle: `/export:/Downloads/zen-browser-main/themes/Frappe/Blue/userChrome.css` (73 bytes ✗)
- Problem: Exceeds NFS 64-byte limit
- Solution: Hash handles → broke registry pattern

**After (UUID-based):**
- Handle: `/export:550e8400-e29b-41d4-a716-446655440000` (45 bytes ✓)
- File struct has ID, ShareName, Path, and embedded FileAttr
- Content stores receive `*File` and choose to use ID or Path
- S3 uses `file.Path`, filesystem could use either
- No intermediate structs, no conversion overhead
