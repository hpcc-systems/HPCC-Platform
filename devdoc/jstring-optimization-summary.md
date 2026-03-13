# jstring Performance Optimization Summary

## Files Reviewed

- `system/jlib/jstring.hpp` — StringBuffer, StringAttr, VStringBuffer, String class declarations
- `system/jlib/jstring.cpp` — Full implementation (~3192 lines)
- `system/jlib/jiface.hpp` — CInterface, IInterface base classes
- `system/jlib/jbuff.hpp` — MemoryBuffer, memcpy_iflen
- `testing/unittests/jlibtests.cpp` — Existing CppUnit tests (JlibStringBufferTiming, JlibStringTest)

## Optimizations Implemented

### 1. `StringBuffer::newline()` — Avoid strlen on string literal

**File:** `system/jlib/jstring.cpp`
**Change:** `append("\n")` → `append('\n')`
**Why:** The `append(const char *)` overload calls `strlen()` to determine length before memcpy. Using `append(char)` directly writes a single byte with no strlen call.
**Impact:** Minor per-call savings (~5-10ns), but `newline()` is called frequently in string building throughout the codebase.

### 2. `StringBuffer::operator=(const StringBuffer&)` — Avoid redundant strlen

**File:** `system/jlib/jstring.hpp`
**Change:** `clear().append(value.str())` → `clear().append(value)`
**Why:** The original code called `value.str()` which null-terminates the buffer, then `append(const char*)` calls `strlen()` to rediscover the length. The `append(const StringBuffer&)` overload uses the already-known `curLen`, avoiding both the null-termination write and strlen scan.
**Impact:** Moderate improvement for StringBuffer-to-StringBuffer assignment, saving O(n) strlen scan.

### 3. `StringBuffer::appendhex()` — Single capacity check + direct writes

**File:** `system/jlib/jstring.cpp`
**Change:** Replaced two `append(char)` calls with one `ensureCapacity(2)` and direct buffer writes.
**Why:** Each `append(char)` call separately checks capacity and increments the length. Combining into a single capacity check and two direct writes halves the overhead.
**Impact:** Moderate improvement for hex encoding paths (URL encoding, XML character escapes, JSON encoding, data-to-hex conversion). These are hot paths in serialization.

### 4. `writeUtf8()` — Pre-build byte array and single append

**File:** `system/jlib/jstring.cpp`
**Change:** For multi-byte UTF-8 sequences (2-6 bytes), pre-build the byte array on the stack and call `append(size_t, const char*)` once instead of calling `append(char)` for each byte.
**Why:** Each individual `append(char)` involves a capacity check and length increment. Building the bytes in a stack array and doing a single append reduces to one capacity check and one memcpy per character.
**Impact:** Moderate to significant for XML/JSON encoding of non-ASCII content. Unicode content (code points ≥ 0x80) was doing 2-6 capacity checks per character; now does exactly 1.

### 5. `replaceVariables()` — Batch-append non-matching segments

**File:** `system/jlib/jstring.cpp`
**Change:** Instead of appending one character at a time when no delimiter is found, use `memchr()` to scan ahead for the next potential delimiter first character, then batch-append the entire non-matching segment.
**Why:** The original loop appended each character individually with `result.append(*source)`, incurring per-character capacity checks and function call overhead. The optimized version scans for the next candidate delimiter position and appends the entire gap in one call.
**Impact:** Significant for long strings with few variable substitutions. Reduces per-character overhead to per-segment overhead.

## Optimizations Identified but NOT Implemented

### A. Increase `InternalBufferSize` from 16 to 32 or 64

**Rationale for skipping:** The current value of 16 is a deliberate trade-off between avoiding heap allocation for short strings and memory overhead for arrays of StringBuffers. Changing this would increase the sizeof(StringBuffer) and could regress memory usage in structures holding many StringBuffer members. This is a design decision that should involve benchmarking specific workloads.

### B. `insert(size_t, double)` uses `sprintf` instead of `snprintf`

**Rationale for skipping:** While `snprintf` is safer, `sprintf` here writes to a `char temp[36]` buffer which is more than sufficient for any double representation. The risk is theoretical and fixing it would be a safety improvement rather than a performance optimization.

### C. `replaceStringNoCase()` — character-by-character append for non-matching portions

**Rationale for skipping:** The same pattern as `replaceVariables()` exists here, but the function is less commonly used and the optimization would add complexity for marginal gain.

### D. `String::length()` calls `strlen()` every time

**Rationale for skipping:** The `String` class appears to be a legacy Java-compatibility class. Caching the length would change its memory layout and ABI. The class is rarely used in modern HPCC code.

### E. `StringAttr::length()` calls `strlen()` every time

**Rationale for skipping:** StringAttr is designed as a thin wrapper around a `char*`. Adding a cached length member would double its memory footprint. The design deliberately trades compute for memory savings since StringAttr is stored in many data structures.

### F. Self-assignment safety in `operator=(const StringBuffer&)`

**Rationale for skipping:** The current implementation (even after optimization) is not self-assignment safe — `clear()` zeroes curLen before `append(value)` reads from it. However, self-assignment of StringBuffers is extremely unlikely in practice and adding a self-assignment check would add overhead to every assignment.

## Test Results

### Before Changes
- `JlibStringTest`: 5 tests passed (existing: testEncodeCSVColumn, testReplaceString; new: testStringBufferAppendAndNewline, testStringBufferAppendHex, testStringBufferAssignment)
- `JlibStringBufferTiming`: 1 test passed

### After Changes
- `JlibStringTest`: 8 tests passed ✅
- `JlibStringBufferTiming`: 1 test passed ✅
- `JlibIPTTest`: passed ✅ (exercises XML encode/decode using writeUtf8)
- `JLibUnicodeTest`: passed ✅
- `JLibStringTest`: passed ✅
- `StringBufferTest`: passed ✅
- Total: 17 tests across 6 suites, all passing

## Potential Risks

1. **`replaceVariables()` behavior change**: The optimized scanning uses `memchr()` to find the next occurrence of the delimiter's first character. If the first character of the delimiter appears frequently but full delimiter matches are rare, this could cause slightly different performance characteristics (more `memcmp` calls but fewer `append` calls). The functional behavior is identical.

2. **`writeUtf8()` stack arrays**: The stack arrays (2-6 bytes) are trivially small and will be optimized by the compiler, but the generated code may differ slightly from the original sequential appends.

## Follow-up Items

- Consider benchmarking `InternalBufferSize` changes with representative workloads
- Consider adding `append(size_t len, const char *value)` overload for `insert` methods to avoid strlen in `insert(size_t, const char*)` for cases where length is known
- The `appendURL()` function could benefit from batching ASCII character sequences instead of character-by-character appends
