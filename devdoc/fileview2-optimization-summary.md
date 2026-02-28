# fileview2 Performance Optimization Summary

## Files Reviewed

| File | Lines | Description |
|------|-------|-------------|
| `common/fileview2/fileview.hpp` | ~200 | Public API interfaces (IResultSetMetaData, IResultSetCursor, INewResultSet, IResultSetFactory) |
| `common/fileview2/fvdatasource.hpp` | ~150 | Data source interfaces (IFvDataSource, IFvDataSourceMetaData, ADataSource) |
| `common/fileview2/fvresultset.cpp` | ~2500 | Result set implementation, cursor logic, XML/JSON output, filtering |
| `common/fileview2/fvresultset.ipp` | ~300 | Result set class declarations |
| `common/fileview2/fvsource.cpp` | ~1100 | Data source base, RowBlock/RowCache, metadata |
| `common/fileview2/fvsource.ipp` | ~400 | Data source class declarations |
| `common/fileview2/fvdisksource.cpp` | ~850 | Disk file data source backend |
| `common/fileview2/fvdisksource.ipp` | ~150 | Disk source declarations |
| `common/fileview2/fvidxsource.cpp` | ~400 | Index file data source backend |
| `common/fileview2/fvidxsource.ipp` | ~100 | Index source declarations |
| `common/fileview2/fvwusource.cpp` | ~250 | Workunit data source backend |
| `common/fileview2/fvwusource.ipp` | ~100 | Workunit source declarations |
| `common/fileview2/fvquerysource.cpp` | ~350 | Query-based data source backend |
| `common/fileview2/fvquerysource.ipp` | ~100 | Query source declarations |
| `common/fileview2/fvrelate.cpp` | ~800 | File relationship handling, ER diagrams |
| `common/fileview2/fvrelate.ipp` | ~200 | Relationship class declarations |
| `common/fileview2/fvtransform.cpp` | ~990 | Data transformation layer, field mappers, cardinality |
| `common/fileview2/fvtransform.ipp` | ~150 | Transform class declarations |
| `common/fileview2/fvwugen.cpp` | ~350 | Workunit generation utilities |
| `common/fileview2/fvwugen.hpp` | ~50 | Workunit generation declarations |

## Optimizations Implemented

### 1. Binary Search in VariableRowBlock::fetchRow()

**File:** `common/fileview2/fvsource.cpp` (line ~793)

**What:** Replaced `rowIndex.find(rowOffset)` (linear O(n) search via `ArrayOf::find()`) with an explicit binary search.

**Why:** `rowIndex` is sorted — offsets are appended incrementally during construction. The `find()` method performs a linear scan which is O(n) per lookup. For variable-length record blocks, this is on the hot path for offset-based row access.

**Expected Impact:** O(log n) instead of O(n) per `fetchRow()` call. For blocks with hundreds of rows, this can significantly reduce lookup time.

### 2. Stack-Allocated Arrays in RowCache::makeRoom()

**File:** `common/fileview2/fvsource.cpp` (line ~942)

**What:** Replaced `new RowBlock*[numToFree]` and `new __int64[numToFree]` heap allocations with stack-allocated arrays. `numToFree` is always `MaxBlocksCached - MinBlocksCached = 10`, a small compile-time constant.

**Why:** Heap allocation via `new[]`/`delete[]` has overhead (memory allocator lock, bookkeeping). For a fixed small size, stack allocation is zero-cost.

**Expected Impact:** Eliminates 2 heap allocations and 2 deallocations per cache eviction cycle.

### 3. Index-Based Cache Eviction in RowCache::makeRoom()

**File:** `common/fileview2/fvsource.cpp` (line ~942)

**What:** Replaced the O(n²) pointer-comparison removal loop (nested `for` with `goto`) with an index-based approach: record indices to remove, mark them in a boolean array, then remove in reverse order.

**Why:** The original code used a nested loop comparing `oldestRow[j] == &allRows.item(i)` for each element to determine which to remove. With `numToFree=10` and `MaxBlocksCached=20`, this was O(200) operations. The new approach marks indices then does a single reverse pass, which is O(n) and avoids the `goto` control flow.

**Expected Impact:** Cleaner control flow, O(n) removal instead of O(n×k). Also eliminates the pointer array entirely — only indices are tracked.

### 4. Removed Dead Code

**File:** `common/fileview2/fvresultset.cpp` (line 1368)
- Removed unused `StringBuffer temp;` declaration in `CResultSetCursor::writeXmlRow()`.

**File:** `common/fileview2/fvtransform.cpp` (lines 975-993)
- Removed static `test()` function that was never called. This function referenced `theTransformerRegistry` and string/unicode transformation but served no production purpose.

**Expected Impact:** Reduced binary size, eliminated compiler warnings about unused variables, cleaner code.

### 5. Pre-Sized Hex Buffer in getDisplayText()

**File:** `common/fileview2/fvresultset.cpp` (line ~1137)

**What:** Changed `StringBuffer temp;` to `StringBuffer temp(len * 2);` before the hex-encoding loop for `type_data`.

**Why:** Each `appendhex()` call appends 2 characters. Without pre-sizing, the `StringBuffer` may reallocate multiple times during the loop (default initial capacity is small). Pre-sizing to `len * 2` ensures a single allocation.

**Expected Impact:** Eliminates potential multiple buffer reallocations when converting binary data to hex strings. Most impactful for large data fields.

## Optimizations Identified but NOT Implemented

### CResultSet::findColumn() — Linear Search to Hash Map

**Location:** `common/fileview2/fvresultset.cpp` (line ~774)

**Description:** `findColumn()` does a linear scan comparing column names via string comparison. A hash map lookup would be O(1).

**Rationale for not implementing:** `findColumn()` is typically called during setup/binding, not in hot data-access loops. The number of columns is usually small (tens, not thousands). Adding a hash map would increase memory overhead and code complexity for minimal real-world benefit. The column count is bounded by schema width, which is typically small.

### DataSourceMetaData Column Type Caching

**Location:** `common/fileview2/fvsource.cpp/ipp`

**Description:** Repeated calls to `queryType()` and `queryColumn()` traverse the column metadata. Caching frequently-accessed type information could reduce overhead.

**Rationale for not implementing:** The metadata is already stored in arrays with O(1) indexed access. The overhead is primarily virtual dispatch, which is unavoidable given the interface design. Caching would add complexity without measurable benefit.

## Test Results

### New Tests Created

**File:** `testing/unittests/fileviewtests.cpp`

Three CppUnit test suites were created covering the components modified by the optimizations:

| Test Suite | Tests | Description |
|-----------|-------|-------------|
| `FixedRowBlockTest` | 5 | `getRow`, out-of-range access, `fetchRow`, `fetchRow` out-of-range, `getNextStoredOffset` |
| `RowCacheTest` | 4 | Add and get, cache miss, eviction (>20 blocks), multi-block lookup |
| `CardinalityTest` | 7 | Single value, range, unbounded (M), empty, mapping with colon, mapping without colon, inverted cardinality |

### Results

- **Before changes:** 16/16 tests pass ✅
- **After all optimizations:** 16/16 tests pass ✅
- **Build:** Clean compilation, no warnings introduced

### Build Configuration

```
cmake -B ./build/Debug -S . -G Ninja \
  -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON \
  -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON \
  -DCMAKE_BUILD_TYPE=Debug
```

## Files Modified

| File | Change |
|------|--------|
| `common/fileview2/fvsource.cpp` | Binary search in fetchRow(), stack arrays + index-based eviction in makeRoom() |
| `common/fileview2/fvresultset.cpp` | Removed unused variable, pre-sized hex buffer |
| `common/fileview2/fvtransform.cpp` | Removed dead test() function |
| `testing/unittests/fileviewtests.cpp` | New test file (created) |
| `testing/unittests/CMakeLists.txt` | Added test file, include dir, and link library |

## Potential Risks and Follow-Up Items

1. **VariableRowBlock::fetchRow() binary search:** The original `find()` would match any index in the array. The binary search assumes `rowIndex` entries are unique and sorted, which holds because offsets are monotonically increasing during block construction. If this invariant were ever violated, the binary search would return NULL where the linear search might have found a match.

2. **RowCache eviction correctness:** The new index-based removal iterates in reverse order to preserve index validity. This has been verified by the eviction test. However, if `MaxBlocksCached` or `MinBlocksCached` are changed to significantly larger values, the stack-allocated boolean array `toRemove[MaxBlocksCached]` should be reconsidered.

3. **Thread safety:** `RowCache` uses a static `rowCacheId` counter without synchronization. This is a pre-existing issue not addressed in this review, as it would require broader architectural changes. If multiple threads share a `RowCache`, this could lead to non-deterministic eviction ordering (though not data corruption since the cache is functionally correct regardless of eviction order).
