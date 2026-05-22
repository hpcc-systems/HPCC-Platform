---
applyTo:
  - "**/*.cpp"
  - "**/*.hpp"
  - "**/*.h"
---
# C++ Code Style and Development Instructions

## Important Formatting Rule: Trailing Whitespaces
- **NO TRAILING WHITESPACES**: This is a strict rule. Never introduce lines with trailing whitespaces anywhere in the code. Ensure any generated or modified code is completely free of spaces or tabs at the end of lines.

## Code Style (Essential Rules)
- Use Allman brace style, but allow single-line blocks to have no braces, unless nested.
- Indent with 4 spaces; no tabs.
- `CamelCase` for class names; `camelCase` for variables, functions, and methods.
- Use `constexpr` over macros.
- Use `#pragma once` for header guards.
- Use `Owned<>` vs `Linked<>` for object ownership.
- Avoid default parameters (use method overloading instead).
- Use `%u` for unsigned integers, `%d` for signed integers.
- Mark all virtual function declarations with `virtual`; mark all derived overrides with `override`.
- Complete style guide: `devdoc/StyleGuide.md`.

## Memory Management and Pointers
- `Owned<X>` takes ownership of a new/returned pointer; `Linked<X>` shares ownership. Never mix them up.
- **`queryFoo()`** returns are NOT linked — the caller must call `Link()` explicitly to retain the pointer beyond its guaranteed lifetime.
- **`getFoo()`** returns ARE linked — assign to `Owned<>` or return directly; do not call `Link()` again.
- Are `CInterface`-derived objects properly release-counted? Check `beforeDispose()` usage.
- Verify resources are released on all code paths, including exception and early-return paths.

## Thread Safety
- Shared mutable state must be protected by a `CriticalSection`, `ReadWriteLock`, or an atomic.
- `CriticalBlock`, `ReadLockBlock`, and `WriteLockBlock` must be correctly scoped — the lock must be released before any I/O, long operations, or calls that could re-acquire the same lock.
- Minimize critical section scope: avoid holding locks across blocking calls, I/O, or memory allocations.
- Check for TOCTOU (time-of-check-time-of-use) patterns where state can change between a check and its use.
- Verify lock ordering is consistent across all code paths to prevent deadlocks.
- Avoid misuse of `volatile`; prefer `std::atomic` with explicit memory ordering.
- Look for race conditions and unnecessary contention on shared resources.

## HPCC Interface Architecture (Java-Style Macros)
- Use standard HPCC macros for inheritance to denote intent:
  - `interface`: Use to declare a pure virtual class (macro expands to `struct`).
  - `extends`: Use when one interface inherits from another (macro expands to `public`).
  - `implements`: Use when a concrete class implements an interface (macro expands to `public`).
- **Naming Conventions**:
  - Interfaces must be prefixed with `I` (e.g., `IMyService`).
  - Concrete class implementations should be prefixed with `C` (e.g., `CMyService : implements IMyService`).

## Classes & Initialization
- **Member vs Parameter Naming**: Use strict `camelCase`. When a parameter's sole purpose is to initialize a member variable of the same name (like in a setter or constructor), prefix the parameter with an underscore (e.g., assigning parameter `_count` to member `count`).

## Exception Handling
- Throw and catch HPCC native exceptions utilizing `IException`. Use `ThrowStringException(...)` or `MakeStringException(...)` formats instead of throwing raw `std::runtime_error` or `std::exception`.
- Exceptions must be caught at the correct level and must not leak resources.

## Security
- Validate all user-supplied inputs; bound all buffer and string lengths.
- Authorization must fail-safe — deny by default; explicitly grant access.
- Do not log, expose in URLs, or include in error messages any secrets, credentials, or sensitive data.
- Guard against SQL/command injection in any code that constructs queries or shell commands from input.
- Check for buffer overflow potential and denial-of-service vectors (unbounded loops, allocations proportional to input size).

## Performance
- Prefer `StringBuffer` over repeated `std::string` concatenation in hot paths.
- Avoid unnecessary copies of large objects; use references, `std::move`, or `Owned<>`/`Linked<>` transfer.
- Choose containers appropriate to the access pattern (e.g., avoid linear scan of large collections).
- Flag algorithmic complexity issues (e.g., O(n²) where O(n log n) is achievable).
- Distinguish hot paths (latency/throughput critical, worth optimising) from cold paths (readability may take priority).

## Correctness & Edge Cases
- Check for null pointer dereferences, empty input handling, integer overflow, and off-by-one errors.
- Error reporting must be consistent; return codes and error states must be checked.
- Complex or counter-intuitive code must have a comment explaining *why*, not just *what*.

## API & Backward Compatibility
- Changes must not break existing callers or ECL code that depends on current behaviour.
- Data formats and serialized structures must be compatible across platform versions and in mixed-version environments.
- Minimize the surface area of public interface changes; document any that are unavoidable.

## HPCC Pattern Alignment
- Prefer `jlib`/`system` helpers over custom reimplementations of the same functionality.
- Use `OwnedRoxieString`, `StringBuffer`, `CriticalSection`, `Owned<>`, `Linked<>` idiomatically.
- Flag new code that duplicates functionality already available in the HPCC codebase.
