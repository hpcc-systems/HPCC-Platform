# Expression Creation and Hash-Consing

## Overview

The eclcc expression graph is an immutable, hash-consed DAG (directed acyclic graph). Every expression node in the compiler is stored in a global cache (`exprCache`). When a new expression is constructed, `closeExpr()` computes its hash and attempts to find a structurally-equivalent node already in the cache. If found, the new allocation is discarded and the existing node is returned. This deduplication ("commoning up") is fundamental to eclcc's memory efficiency and equality-by-pointer semantics.

**Key source files:**
- `ecl/hql/hqlexpr.ipp` — Class hierarchy definitions
- `ecl/hql/hqlexpr.cpp` — Expression creation, hash-consing, lifecycle
- `ecl/hql/hqlexpr.hpp` — Public interfaces and inline helpers

---

## Class Hierarchy

```
CHqlExpression (base — owns hashcode, transformExtra, operands, op)
├── CHqlRealExpression (adds infoFlags, cachedCRC, attributes)
│   ├── CHqlExpressionWithTables (adds CUsedTables — scope tracking)
│   │   ├── CHqlExpressionWithType (adds ITypeInfo* type — most operators)
│   │   │   ├── CHqlNamedExpression (adds IIdAtom* id)
│   │   │   ├── CHqlField (fields)
│   │   │   ├── CHqlRow (no_left, no_right, no_self, etc.)
│   │   │   ├── CHqlDataset (datasets — adds container, metaProperty)
│   │   │   ├── CHqlDictionary
│   │   │   ├── CHqlExternalCall
│   │   │   ├── CHqlDelayedCall
│   │   │   └── ... (CHqlParameter, CHqlSequence, CHqlMacro, etc.)
│   │   ├── CHqlRecord (implements ITypeInfo + IHqlSimpleScope)
│   │   ├── CHqlAttribute
│   │   └── CHqlAlienType
│   ├── CHqlSelectBaseExpression (no_select — specialized for selectors)
│   │   ├── CHqlNormalizedSelectExpression
│   │   └── CHqlSelectExpression (has extra `normalized` member)
│   └── CHqlConstant (no_constant — adds IValue*)
└── CHqlAnnotation (wraps a body expression — separate hash-consing)
    ├── CHqlSymbolAnnotation (named symbols, definitions)
    ├── CHqlLocationAnnotation (source location info)
    └── CHqlWarningAnnotation
```

### Memory Layout (CHqlExpression base, 64-bit)

| Field | Size | Notes |
|-------|------|-------|
| vtable pointer | 8 bytes | Inherited from CInterface |
| xxcount (atomic ref count) | 4 bytes | From CInterfaceOf |
| hashcode | 4 bytes | Stored to avoid recomputation; also pads alignment |
| transformExtra[1] | 8 bytes | IInterface* slot for transform passes |
| operands (HqlExprArray) | ~24 bytes | Dynamic array of IHqlExpression* children |
| op (node_operator) | 2 bytes | Expression operator enum |
| infoFlags2 | 2 bytes | Additional flags, packed with op |
| transformDepth[1] | 1 byte | Tracks which transform depth owns transformExtra |
| observed | 1 byte | Whether this expr is in exprCache |

Total CHqlExpression base: ~56 bytes (with padding on 64-bit).

Additional per-subclass:
- `CHqlRealExpression`: +4 (cachedCRC) +4 (infoFlags) +8 (atomic attributes pointer) = +16
- `CHqlExpressionWithTables`: +`CUsedTables` (contains `UsedExpressionHashTable`) ≈ +24–48
- `CHqlExpressionWithType`: +8 (ITypeInfo* type)

The design note in `hqlexpr.ipp` explains why the hierarchy exists: "one representative large example has 12M+ instances, and not including the tables/type save 16 and 8 bytes each."

---

## Expression Creation Flow

### Factory Functions

Expressions are created via factory functions that all follow the same pattern:

```cpp
// Scalar values
IHqlExpression * createValue(node_operator op, ITypeInfo * type, ...operands);

// Datasets
IHqlExpression * createDataset(node_operator op, HqlExprArray & parms);

// Rows
IHqlExpression * createRow(node_operator op, ...);

// Dictionaries
IHqlExpression * createDictionary(node_operator op, ...);

// Records
IHqlExpression * createRecord(...);
```

Each factory:
1. Builds a `HqlExprArray` of operands
2. Allocates the appropriate subclass (e.g., `new CHqlExpressionWithType(op, type, operands)`)
3. Calls `closeExpr()` on the new object
4. Returns the result (which may be a *different* pointer if commoned up)

### `expandOperands()`

Helper that converts `std::initializer_list<IHqlExpression*>` into a flat `HqlExprArray`. Handles:
- NULL entries (skipped)
- `no_comma` unwinding: if `expandCommas=true` and an operand is `no_comma`, it recursively flattens the comma-tree into individual operands
- Pre-counts the exact number of operands to avoid array reallocation

### `closeExpr()` — The Sealing Step

```
CHqlExpression::closeExpr()
├── assert not already closed
├── sethash()          ← compute the hash
└── commonUpExpression()  ← insert into / look up from cache
```

For `CHqlRealExpression`:
```
CHqlRealExpression::closeExpr()
├── updateFlagsAfterOperands()  ← finalize infoFlags from children
├── operands.trimMemory()       ← shrink the operand array to exact size
└── CHqlExpression::closeExpr() ← hash + common-up
```

After `closeExpr()`, the expression is immutable. No further operands can be added. The `hashcode != 0` invariant serves as the "closed" sentinel.

---

## Hash Computation

### `sethash()`

```cpp
void CHqlExpression::sethash()
{
    // Default: hash = op + bottom-32-bits-of-type-pointer
    setInitialHash((unsigned)(memsize_t)queryType());
}

void CHqlExpression::setInitialHash(unsigned typeHash)
{
    hashcode = op + typeHash;
    unsigned kids = operands.ordinality();
    if (kids)
        hashcode = hashc((const unsigned char *)operands.getArray(),
                         kids * sizeof(IHqlExpression *), hashcode);
}
```

**Key properties:**
- Uses `hashc()` (CRC-based) over the raw pointer array of operands
- Because the graph is hash-consed, structurally identical subtrees share the same pointer → same hash contribution
- Type identity is by pointer (types are also commoned up)
- The hash function is fast but not cryptographic — it's optimized for low collision rate among expression nodes

### Annotation Hash

```cpp
void CHqlAnnotation::sethash()
{
    hashcode = 0;
    HASHFIELD(body);  // hash of the body pointer
}
```

Annotations hash based solely on their body pointer. Since different annotation types override `equals()` with `getAnnotationKind()` checks, collisions between annotation types are resolved at equality comparison.

---

## Hash-Consing: `commonUpExpression()`

### The Global Cache

```cpp
const unsigned InitialExprCacheSize = 0x1000U; // 4096 buckets

class HqlExprCache : public JavaHashTableOf<CHqlExpression>
{
    // keep=false → uses observer pattern (no extra link count)
    HqlExprCache() : JavaHashTableOf<CHqlExpression>(InitialExprCacheSize, false) {}

    unsigned getHashFromElement(const void * et) const
    { return static_cast<const CHqlExpression *>(et)->queryHash(); }

    bool matchesFindParam(const void * element, const void * key, unsigned fphash) const
    {
        // Fast-path: hash must match (already guaranteed by bucket)
        // Then: full structural equality check
        return (element->queryHash() == fphash) && element->equals(*key);
    }

    unsigned getTableLimit(unsigned max) { return max/2; }  // 50% load factor
};

static HqlExprCache *exprCache;
static CriticalSection *exprCacheCS;
```

**Cache properties:**
- Open-addressing hash table (`SuperHashTableOf`) — no chaining
- 50% maximum load factor (doubles when reached)
- Observer pattern: the cache does NOT hold a reference count on entries
- Protected by `exprCacheCS` critical section

### The Commoning Algorithm

```cpp
IHqlExpression * CHqlExpression::commonUpExpression()
{
    // Certain ops are never commoned:
    switch (op) {
    case no_uncommoned_comma: return this;  // temporary construction nodes
    case no_service: return this;           // unique service definitions
    case no_privatescope:
    case no_mergedscope: return this;       // scope objects
    }

    HqlCriticalBlock block(*exprCacheCS);    // acquire lock
    IHqlExpression * match = exprCache->addOrFind(*this);

    if (match == this)
        return this;  // We were added as new — we are the canonical instance

    // Found an existing match — but is it still alive?
    if (!static_cast<CHqlExpression *>(match)->isAliveAndLink())
    {
        // The existing entry is being destroyed (refcount → 0)
        // Replace it with ourselves
        exprCache->replace(*this);
        return this;
    }

    // Successfully commoned up — discard ourselves, return the existing one
    Release();  // destroy this newly-created duplicate
    return match;
}
```

### Race Condition Protection: `isAliveAndLink()`

Because the cache uses the observer pattern (no owning reference), there's a window where an expression in the cache has refcount=0 and is being destroyed. `isAliveAndLink()` uses atomic compare-and-swap to safely attempt to increment the refcount only if it's > 0:

```cpp
inline bool isAliveAndLink() const {
    unsigned expected = xxcount.load(std::memory_order_acquire);
    for (;;) {
        if ((expected-1) >= (DEAD_PSEUDO_COUNT-1))
            return false;  // already dead or dying
        if (xxcount.compare_exchange_weak(expected, expected+1, std::memory_order_acq_rel))
            return true;   // successfully linked
    }
}
```

---

## Expression Lifecycle

### Observer Pattern

The cache is constructed with `keep=false`, meaning:
- `onAdd()` calls `addObserver(*this)` → sets `observed = true`
- `onRemove()` calls `removeObserver(*this)` → sets `observed = false`
- The cache does NOT increment the reference count

This means entries can be garbage-collected when all external references are released, without the cache itself holding them alive.

### `beforeDispose()` — Cache Removal

When an expression's refcount reaches zero and `Release()` triggers destruction:

```cpp
void CHqlExpression::beforeDispose()
{
    if (observed)
    {
        HqlCriticalBlock block(*exprCacheCS);
        if (observed)  // double-check under lock
            exprCache->removeExact(this);
    }
}
```

This ensures the dying expression is removed from the cache before its memory is freed, preventing dangling pointers.

### Lifecycle Summary

```
1. new CHqlXxx(op, type, operands)    → allocate, refcount = 1
2. closeExpr()
   ├── sethash()                      → compute hashcode
   └── commonUpExpression()
       ├── [new entry] addOrFind adds to cache
       │   └── onAdd → observed = true
       │   └── return this (refcount still 1, cache doesn't own)
       └── [existing match]
           ├── isAliveAndLink() on match → match refcount++
           ├── this->Release() → this destroyed (refcount 0→beforeDispose)
           └── return match
3. Normal usage: Link()/Release() by holders
4. Last Release() → refcount reaches 0
   └── beforeDispose()
       └── exprCache->removeExact(this) → onRemove → observed = false
   └── destructor runs
```

---

## Structural Equality

```cpp
bool CHqlRealExpression::equals(const IHqlExpression & other) const
{
    if (this == &other) return true;
    if (other.isAnnotation()) return false;
    if (op != other.getOperator()) return false;

    // For most ops, types must be identical (pointer equality!)
    switch (op) {
    case no_record: case no_type: case no_scope: ...
        break;  // these don't compare type
    default:
        if (queryType() != other.queryType()) return false;
    }

    // Operand count must match
    unsigned kids = other.numChildren();
    if (kids != operands.ordinality()) return false;

    // All children must be pointer-identical (hash-consing guarantee!)
    for (unsigned kid = 0; kid < kids; kid++) {
        if (&operands.item(kid) != other.queryChild(kid))
            return false;
    }
    return true;
}
```

**Critical insight:** Because the entire graph is hash-consed, equality of children is by pointer comparison. This makes `equals()` O(n) in the number of *direct* children, not O(tree-size).

---

## Annotations

Annotations wrap a body expression to add metadata (name, source location, warnings) without changing the expression's semantics.

```cpp
class CHqlAnnotation : public CHqlExpression
{
    IHqlExpression *body;  // the wrapped expression
};
```

### Key behaviors:
- `queryBody()` returns the unwrapped body (one level)
- `queryBody(true)` still returns one level (single-level unwrap)
- Most methods (queryType, queryChild, numChildren, etc.) delegate to body
- Annotations are independently hash-consed: same body + same annotation kind/params = same node
- `cloneAnnotation(newBody)` creates a new annotation wrapping a different body

### Annotation equals:
```cpp
bool CHqlAnnotation::equals(const IHqlExpression & other) const
{
    if (getAnnotationKind() != other.getAnnotationKind()) return false;
    if (body != cast->body) return false;  // body pointer equality
    return true;  // subclasses may add additional checks
}
```

### Performance impact:
Annotations increase the number of nodes in the cache but don't significantly impact hash distribution. The `queryBody()` accessor is called frequently by transforms to bypass annotations and operate on the underlying expression.

---

## Expressions NOT Commoned Up

| Operator | Reason |
|----------|--------|
| `no_uncommoned_comma` | Temporary comma-trees during construction |
| `no_service` | Service definitions are unique |
| `no_privatescope` | Module scope objects |
| `no_mergedscope` | Merged module scopes |
| `no_remotescope` (with annotation) | Remote plugin scopes |

---

## Performance Characteristics

### Cache Size
- Initial: 4096 buckets
- Grows by doubling when 50% full
- Typical large compilation: millions of entries (12M+ noted for large ECL programs)
- Hash distribution relies on pointer uniqueness of children and types

### Critical Section
- `exprCacheCS` is held during `commonUpExpression()` — both lookup and insert
- Single global lock; not sharded
- The lock is also acquired during `beforeDispose()` for removal
- This is a potential bottleneck but eclcc is largely single-threaded for expression manipulation

### Memory Efficiency
- Hash-consing eliminates duplicate subtrees (significant for large JOIN expressions, repeated record definitions)
- The observer pattern avoids extra refcount overhead in the cache
- `operands.trimMemory()` at close time frees unused array capacity

### Known Design Trade-offs
- `HQLEXPR_MULTI_THREADED` is always defined, using `CInterfaceOf<IHqlExpression>` (atomic refcount) — adds ~3-4% overhead on x64
- `NUM_PARALLEL_TRANSFORMS = 1` means each expression has a single transform slot (8 bytes + 1 byte depth)
- The global critical section means only one thread can create/lookup expressions at a time

---

## Key Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `InitialExprCacheSize` | 0x1000 (4096) | Initial hash table bucket count |
| `NUM_PARALLEL_TRANSFORMS` | 1 | Transform slots per expression |
| `HQLEXPR_MULTI_THREADED` | defined | Use atomic refcounts |
| `USE_SELSEQ_UID` | defined | Unique selector sequences enabled |
| `GATHER_COMMON_STATS` | optional | Statistics on cache hit rates per operator |
| `DEBUG_TRACK_INSTANCEID` | optional | Adds 8-byte seqid for debugging |
