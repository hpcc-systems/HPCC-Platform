# JTree (Property Tree) Optimization Summary

## Files Reviewed

| File | Lines | Description |
|------|-------|-------------|
| `system/jlib/jptree.hpp` | 477 | Public `IPropertyTree` interface (~75 methods), flags, factory functions |
| `system/jlib/jptree.ipp` | 1204 | Internal classes: PTree, CAtomPTree, LocalPTree, ChildMap, CPTArray, CPTValue, AttrStr/AttrStrAtom, PtrStrUnion |
| `system/jlib/jptree.cpp` | 10573 | Full implementation: XPath evaluation, attribute management, serialization, XML/JSON/YAML parsing |
| `system/jlib/jptree-attrs.hpp` | ~200 | Read-only attribute name atom table (auto-generated) |
| `system/jlib/jptree-attrvalues.hpp` | ~100 | Read-only attribute value atom table (auto-generated) |

## Optimizations Implemented

### 1. Fix double atomic load in CPTArray destructor (BUG FIX)
**File:** `system/jlib/jptree.cpp` (line 1203)
**What:** `if (map.load()) delete map.load();` performed two separate atomic loads. In a concurrent environment, the second load could theoretically return a different value than the first.
**Fix:** Load into a local variable once: `CQualifierMap *m = map.load(); if (m) delete m;`
**Impact:** Eliminates a potential race condition and removes a redundant atomic operation.

### 2. Eliminate redundant std::string temporaries in CValueMap (PERFORMANCE)
**File:** `system/jlib/jptree.cpp` (lines 591-616)
**What:** `emplace(std::make_pair(std::string(v), *elements))` creates an intermediate `std::pair` and then moves it into the map. Similarly for `insertEntry()`.
**Fix:** Changed to `emplace(v, *elements)` which constructs the key `std::string` directly in-place within the map node, avoiding the temporary pair construction and move.
**Impact:** Reduces allocations in hot paths (qualifier map construction and updates). CValueMap is used for fast O(1) lookups when sibling arrays exceed the mapping threshold.

### 3. Eliminate redundant std::string temporary in CQualifierMap::addMapping (PERFORMANCE)
**File:** `system/jlib/jptree.cpp` (line 662)
**What:** `attrValueMaps.emplace(std::make_pair(std::string(lhs), valueMap))` — same pattern as #2.
**Fix:** Changed to `attrValueMaps.emplace(lhs, valueMap)` for in-place construction.
**Impact:** Reduces allocation on qualifier map creation.

### 4. Simplify SeriesPTIterator::first() (CODE QUALITY)
**File:** `system/jlib/jptree.cpp` (line 958)
**What:** `if (nextIterator()) return true; else return false;` — redundant conditional.
**Fix:** Simplified to `return nextIterator();`
**Impact:** Cleaner code, no functional change.

### 5. Optimize isEmptyPTree attribute check (PERFORMANCE)
**File:** `system/jlib/jptree.cpp` (line 4131)
**What:** Created a full `IAttributeIterator` via `getAttributes()` just to check if any attributes exist — involves heap allocation and virtual dispatch.
**Fix:** Replaced with `getAttributeCount() > 0` which is a simple field read (`return numAttrs;`).
**Impact:** Eliminates heap allocation and iterator overhead in a utility function used for tree emptiness checks.

### 6. Replace macros with constexpr (CODE QUALITY)
**Files:** `system/jlib/jptree.ipp` (line 34), `system/jlib/jptree.cpp` (lines 58-59)
**What:** `#define ANE_APPEND -1`, `#define ANE_SET -2`, `#define PTREE_COMPRESS_THRESHOLD (4*1024)`, `#define PTREE_COMPRESS_BOTHER_PECENTAGE (80)` (note: original had typo "PECENTAGE").
**Fix:** Replaced with `static constexpr` variables: `aneAppend`, `aneSet`, `ptreeCompressThreshold`, `ptreeCompressBotherPercentage`. Fixed the "PECENTAGE" typo to "Percentage".
**Impact:** Type-safe constants, proper scoping, debugger visibility.

## Optimizations Identified But NOT Implemented

### A. Heterogeneous lookup for std::unordered_map/multimap
**Rationale:** The `equal_range()` and `find()` calls in CValueMap and CQualifierMap still construct `std::string` temporaries from `const char*` because `std::unordered_map` doesn't support heterogeneous lookup until C++20 (with custom hash/equal). The HPCC codebase targets C++17, and adding custom transparent hashers would add significant complexity for marginal gain.

### B. Replace IptFlagTst/Set/Clr macros with inline functions
**Rationale:** These bit manipulation macros (`#define IptFlagTst(fs, f) (0!=(fs&(f)))` etc.) are used extremely widely across the codebase (not just jptree). Changing them would touch many files and the current form is well-understood. The compiler generates identical code for both.

### C. Optimize findAttribute linear scan
**Rationale:** `findAttribute()` performs a linear scan through the `attrs` array. For typical PTree nodes with few attributes (1-5), this is actually optimal — the array fits in a single cache line and avoids hash table overhead. The existing atom table and PtrStrUnion optimizations already minimize per-attribute memory. A hash-based approach would only help for nodes with many attributes, which are rare.

### D. Replace raw malloc/realloc in LocalPTree::setAttribute with RAII
**Rationale:** The `realloc()` pattern for growing the `attrs` array is intentional — it's a flat POD array of `AttrValue` structs that can be safely memcpy'd. RAII wrappers would add overhead for a critical hot path that already handles cleanup correctly in destructors.

## Test Results

### Before Changes
- `JlibIPTTest`: 7 tests — **OK**
- `PTreeSerializationDeserializationTest`: 3 tests — **OK**

### After Changes
- `JlibIPTTest`: 7 tests — **OK**
- `PTreeSerializationDeserializationTest`: 3 tests — **OK**

## Potential Risks

- **CValueMap emplace change**: The `emplace(v, tree)` form relies on implicit conversion from `const char*` to `std::string` for the map key. This is well-defined C++ behavior and tested to work correctly.
- **constexpr rename**: The `ANE_APPEND`/`ANE_SET` macros were only used internally in jptree.cpp (verified by grep). The `PTREE_COMPRESS_*` macros were also only used internally.

## Follow-up Items

1. Consider adding C++20 heterogeneous lookup support when the codebase migrates to C++20
2. The `PTREE_COMPRESS_BOTHER_PECENTAGE` typo was fixed in the constexpr name — downstream references (comments, docs) may still use the old spelling
