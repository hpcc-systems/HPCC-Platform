# Transformation Framework Performance Analysis (`ecl/hql/hqltrans.cpp`)

> Last verified: 2026-05

## 1. Architecture Overview

The HQL transformation framework is the backbone of eclcc's compilation pipeline. Every optimization, normalization, and code-generation pass operates through one of a handful of base transformer classes. Expressions are immutable DAGs; transformers create new expression nodes when changes are needed, and return the original when nothing changes.

### Class Hierarchy

```
HqlTransformerBase
├── QuickHqlTransformer          — Simple, uses queryTransformExtra() directly on nodes
│   ├── QuickExpressionReplacer
│   ├── HqlSectionAnnotator
│   ├── HqlTreeNormalizer
│   ├── ParameterBindTransformer
│   └── ~15 other lightweight transformers
│
└── ANewHqlTransformer           — Allocates ANewTransformInfo per expression
    └── NewHqlTransformer        — Main workhorse, selector-aware, multi-pass
        ├── HqlMapTransformer    — Simple key→value replacement
        ├── HqlMapDatasetTransformer — Replacement with orphaned selector fixup
        ├── ConditionalHqlTransformer — Unconditional/conditional analysis
        │   └── HoistingHqlTransformer — Hoists globals/independent expressions
        ├── NestedHqlTransformer — Scoped push/pop of mappings
        ├── MergingHqlTransformer — Context-dependent, scope-aware transforms
        │   └── HqlScopedMapSelectorTransformer
        ├── NewSelectorReplacingTransformer — Dual hidden/visible selector mapping
        ├── ScopedTransformer    — Full LEFT/RIGHT/dataset scope tracking
        │   └── ScopedDependentTransformer — MergingTransformInfo + scope
        ├── CTreeOptimizer       — The main optimizer (hqlopt.cpp)
        └── ~40+ other derived transformers
```

### Critical Hot Paths

A large ECL compilation (10K+ activities) may invoke 30–50 separate transformation passes. Each pass traverses the entire expression DAG, which can contain 100K–1M+ nodes. The total transform calls across a compilation can easily reach 10–100 million.

Key operations on the hot path:
1. `transform()` → `queryAlreadyTransformed()` (fast cache lookup)
2. `createTransformed()` → dispatch on `getOperator()`
3. `optimizedTransformChildren()` → iterate children, call `transform()` recursively
4. `setTransformed()` → `setTransformExtra()` on expression node
5. `createTransformInfo()` → heap allocate `NewTransformInfo` per unique expression

## 2. Core Mechanisms

### Transform State Storage

Each `CHqlExpression` node stores per-transform state directly:
```cpp
IInterface * transformExtra[NUM_PARALLEL_TRANSFORMS];  // 8 bytes (one pointer)
transformdepth_t transformDepth[NUM_PARALLEL_TRANSFORMS]; // 2 bytes
```

The `transformDepth` field identifies which transform "generation" the stored extra belongs to. This allows nested transformers (depth-stacking) without clearing state between passes.

### The Transform Cycle

For `NewHqlTransformer::transform(expr)` (line 1878):
1. **Cache check**: `queryAlreadyTransformed(expr)` — reads `transformExtra` from the expression, checks depth matches, casts to `NewTransformInfo`, returns `queryTransformed()`
2. **If miss**: calls `createTransformed(expr)` — the virtual dispatch
3. **Store result**: `setTransformed(expr, transformed)` — pushes prior state onto `TransformTrackingInfo::transformStack` if depth differs, stores new extra
4. **Annotation propagation**: loops through annotation layers (`queryBody(true)`), mapping each body to the corresponding transformed body

### The `optimizedTransformChildren` Optimization

This is the most critical function (line 454). It avoids heap-allocating an `HqlExprArray` when all children transform to themselves:

```cpp
bool optimizedTransformChildren(IHqlExpression * expr, HqlExprArray & children)
{
    unsigned numDone = children.ordinality();
    unsigned max = expr->numChildren();
    // Phase 1: Skip children already in the array that match originals
    for (idx = 0; idx < numDone; idx++)
        if (&children.item(idx) != expr->queryChild(idx))
            break;

    if (idx == numDone) {
        // Phase 2: Transform remaining children without storing until one differs
        for (; idx < max; idx++) {
            IHqlExpression * child = expr->queryChild(idx);
            lastTransformedChild.setown(transform(child));
            if (child != lastTransformedChild) break;
        }
        if (idx == max) return true;  // ← FAST PATH: nothing changed

        // Phase 3: Backfill and continue
        children.ensureCapacity(max);
        for (i = numDone; i < idx; i++)
            children.append(*LINK(expr->queryChild(i)));
        children.append(*lastTransformedChild.getClear());
        idx++;
    }
    // Phase 4: Transform remaining
    for (; idx < max; idx++)
        children.append(*transform(expr->queryChild(idx)));
    return false;
}
```

**Key insight**: When no children change (the common case for most nodes in most passes), this function does NO heap allocation and returns `true` immediately. The caller then returns `LINK(expr)`.

### The `OPTIMIZE_TRANSFORM_ALLOCATOR` (Currently Disabled)

Lines 22-28 of `hqltrans.ipp` define a commented-out arena allocator for `NewTransformInfo` objects:
```cpp
//#define OPTIMIZE_TRANSFORM_ALLOCATOR
```

When enabled, all `NewTransformInfo` allocations go through a large-block arena (`CLargeMemoryAllocator`) owned by the transformer, freed in one shot at transformer destruction. This eliminates millions of individual `new`/`delete` calls per pass.

## 3. Performance Characteristics

### What's Already Well-Optimized

1. **Cache-hit fast path**: `queryTransformExtra()` is a single pointer read + depth comparison — essentially free
2. **No-change fast path**: `optimizedTransformChildren` avoids array allocation when nothing changes
3. **`quickTransformTransform`**: Skips LHS of assigns, skips sub-expressions that can't contain active datasets
4. **Annotation propagation**: The post-transform body-mapping loop avoids re-transforming annotation wrappers
5. **`NTFtransformOriginal` flag**: In `NewTransformInfo`, if the transformed expression IS the original, it stores a flag instead of a pointer (saves a `Link()/Release()` pair)
6. **`CSingleThreadSimpleInterfaceOf`**: `ANewTransformInfo` uses non-atomic reference counting since transforms are single-threaded

### Memory Overhead Per Transform Pass

For a DAG of N unique expression nodes:
- **N × `NewTransformInfo`** heap allocations (40 bytes each: vtable + refcount + lastPass + flags + 2 spare bytes + original pointer + transformed pointer + transformedSelector pointer)
- Each `setTransformExtra` call pushes the expression pointer + prior depth onto `TransformTrackingInfo::transformStack`
- The `transformStack` is a `PointerArray` that grows via `realloc` — amortized O(1)

For 30 passes over 500K nodes: **~15M small heap allocations** for transform info objects alone.

## 4. Identified Optimization Opportunities

### 4.1 Enable `OPTIMIZE_TRANSFORM_ALLOCATOR` (High Impact)

**Current state**: Disabled (`#define` commented out).  
**Problem**: Each transformer pass allocates one `NewTransformInfo` (or derived) object per unique expression visited. For a 500K-node graph across 30 passes, this is ~15M individual `new`/`delete` calls. Each allocation hits the system allocator, causing fragmentation and cache thrashing.  
**Fix**: Enable the arena allocator. All `NewTransformInfo` objects are allocated from a contiguous block owned by the transformer. On destruction, the entire block is freed in one operation. No individual destructors needed (transform infos hold `HqlExprAttr` which are just pointer+refcount — the referenced expressions outlive the arena anyway).  
**Complication**: The arena allocator requires that `NewTransformInfo` destructors don't run (no `delete`). The `HqlExprAttr` members (`transformed`, `transformedSelector`) hold linked references that would leak. Resolution: either (a) explicitly walk and release during transformer destruction, or (b) use a custom destructor-calling arena that batch-destroys. Option (a) is simpler: maintain a linked list of allocated infos and iterate at end.  
**Impact**: Estimated 10–20% compile-time reduction for large queries. Eliminates millions of malloc/free calls per compilation. Improves cache locality (infos allocated contiguously).

### 4.2 Avoid `HqlExprArray` Stack Allocation in `completeTransform` (Medium Impact)

**Current state** (line 196):
```cpp
inline IHqlExpression * completeTransform(IHqlExpression * expr)
{
    HqlExprArray done;          // ← stack-allocated but Allocator initializes _head=nullptr
    return completeTransform(expr, done);
}
```
Called from `createTransformed` default case (line 1637). The `HqlExprArray` is zero-initialized on the stack (3 members: `_head`, `used`, `max`). If all children are the same, `optimizedTransformChildren` returns `true` without touching `done`. If any child differs, `ensureCapacity(max)` does a single heap allocation.

**Problem**: Even though the array is cheap to construct, it's constructed on every `createTransformed` call for the default case. Since many `switch` cases in `createTransformed` also declare `HqlExprArray children`, there's redundant stack initialization for the common "nothing changed" path.

**Fix**: The `completeTransform(expr)` overload could be changed to first call a lightweight "any child changed?" check that doesn't require an array at all. Something like:
```cpp
inline IHqlExpression * completeTransform(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned i = 0; i < max; i++)
    {
        IHqlExpression * child = expr->queryChild(i);
        IHqlExpression * tchild = transform(child);
        if (child != tchild)
        {
            // Build array from scratch
            HqlExprArray done;
            done.ensureCapacity(max);
            for (unsigned j = 0; j < i; j++)
                done.append(*LINK(expr->queryChild(j)));
            done.append(*tchild);
            for (unsigned j = i+1; j < max; j++)
                done.append(*transform(expr->queryChild(j)));
            return expr->clone(done);
        }
    }
    return LINK(expr);
}
```
However, this duplicates the logic of `optimizedTransformChildren`. The actual win would be marginal since `HqlExprArray` construction is just zeroing 12-16 bytes. **Verdict: Low priority** — the existing pattern is already near-optimal.

### 4.3 Reduce `NewTransformInfo` Size (Medium Impact)

**Current state**: `NewTransformInfo` is 40–48 bytes (platform-dependent due to alignment):
```
ANewTransformInfo:
  vtable pointer          8 bytes
  refcount (unsigned)     4 bytes
  lastPass (byte)         1 byte
  flags (byte)            1 byte
  spareByte1              1 byte
  spareByte2              1 byte
  original (IHqlExpr*)    8 bytes
  --- padding ---         0-4 bytes
NewTransformInfo:
  transformed (HqlExprAttr = Linked<IHqlExpr> = IHqlExpr*)  8 bytes
  transformedSelector (HqlExprAttr)                          8 bytes
```
Total: ~40 bytes minimum, 48 with padding.

**Problem**: The `original` pointer is redundant in most cases — the expression node already stores a pointer back to itself via `transformExtra`. When `setTransformExtra(info)` is called, we could retrieve `original` from the expression node that owns the extra.  
**Fix**: Remove `original` from `ANewTransformInfo`. Instead, the expression pointer is available from the transform tracking stack or from the call site. However, some uses of `original` exist in derived classes (e.g., `AMergingTransformInfo::recurseParentScopes()` and `OptTransformInfo`).  
**Complication**: `original` is used by the `analysisGathering` logic and in derived `createTransformInfo`. Removing it requires passing the expression as a parameter to all virtual methods that currently read `original`. This is a larger refactor.  
**Impact**: Saves 8 bytes per info → ~4MB less per pass on 500K-node graphs. More importantly, improves cache line density (more infos fit per cache line).

### 4.4 Avoid Virtual Dispatch in `NewTransformInfo` (Medium Impact)

**Current state**: `NewTransformInfo::queryTransformed()`, `setTransformed()`, etc. are virtual:
```cpp
virtual IHqlExpression * queryTransformed() { return inlineQueryTransformed(); }
virtual void setTransformed(IHqlExpression * expr) { inlineSetTransformed(expr); }
```
The `inline` versions exist precisely because the virtual call overhead was noticed. In `NewHqlTransformer`, the base `queryTransformExtra()` always returns `ANewTransformInfo*` which is then accessed via virtual calls.

**Problem**: In the hot loop of `transform()`, every cache miss calls `queryTransformed()` via vtable. For `NewHqlTransformer` (not `MergingHqlTransformer`), the info is ALWAYS `NewTransformInfo`. The virtual dispatch adds an indirect branch that's unpredictable when there are multiple derived info types in the same pass.

**Fix**: In `NewHqlTransformer::queryAlreadyTransformed()` and `setTransformed()`, cast directly to `NewTransformInfo*` and call the `inline` variants:
```cpp
IHqlExpression * NewHqlTransformer::queryAlreadyTransformed(IHqlExpression * expr)
{
    IInterface * extra = expr->queryTransformExtra();
    if (extra)
        return static_cast<NewTransformInfo *>(extra)->inlineQueryTransformed();
    return NULL;
}
```
This is already partially done in `NewTransformInfo` via the `inline` prefix functions. But the call site (`queryAlreadyTransformed` at line 1857) goes through the virtual:
```cpp
return ((NewTransformInfo *)extra)->queryTransformed();  // virtual!
```
**Impact**: Eliminates one indirect branch per expression per transform. For 10M+ transform calls per compilation, this removes ~10M branch mispredictions. Estimated 2–5% improvement in transform-heavy passes.

### 4.5 `getTransformedChildren` Unnecessarily Always Allocates (Low-Medium Impact)

**Current state** (line 1720):
```cpp
IHqlExpression * NewHqlTransformer::getTransformedChildren(IHqlExpression * expr)
{
    ...
    HqlExprArray children;
    children.ensureCapacity(max);       // ← ALWAYS allocates
    for (idx=0; idx<max; idx++) {
        IHqlExpression * child = expr->queryChild(idx);
        IHqlExpression * tchild = getTransformedChildren(child);
        children.append(*tchild);
        if (child != tchild) same = false;
    }
    ...
}
```
Unlike `optimizedTransformChildren`, this function ALWAYS calls `ensureCapacity` upfront, even when all children are identical.

**Fix**: Use the same deferred-allocation pattern as `optimizedTransformChildren`.  
**Impact**: `getTransformedChildren` is less frequently called than `transform`/`createTransformed`, but when used (e.g., for records), it processes every field. For records with 100+ fields where nothing changes, this saves one heap allocation per record.

### 4.6 `transformChildren` Always Allocates Even When Same (Low Impact)

**Current state** (line 525):
```cpp
bool HqlTransformerBase::transformChildren(IHqlExpression * expr, HqlExprArray & children)
{
    ...
    children.ensureCapacity(max);       // ← ALWAYS allocates if max > 0
    for (idx=numDone; idx<max; idx++) {
        IHqlExpression * child = expr->queryChild(idx);
        IHqlExpression * tchild = transform(child);
        children.append(*tchild);
        if (child != tchild) same = false;
    }
    return same;
}
```
This is the non-optimized variant. It's called from several places in `createTransformed` where the caller needs to check other conditions besides "children same" (e.g., type changes). The array is always populated regardless of whether anything changed.

**Fix**: For callers that only check `same`, use `optimizedTransformChildren` instead. For callers that need to inspect children even when same, this can't be easily fixed.  
**Impact**: Low — most critical paths already use `optimizedTransformChildren` via `completeTransform`.

### 4.7 `MergingTransformInfo` Lazy Allocation of Maps (Already Partially Done)

**Current state**: `MergingTransformInfo` stores:
```cpp
MAPPINGCLASS * mapped;           // = FastScopeMapping* 
MAPPINGCLASS * mappedSelector;   // = FastScopeMapping*
```
These are heap-allocated on first use. The `MergingTransformSimpleInfo` variant (used for expressions that can only be transformed once) avoids the maps entirely, using the base `NewTransformInfo` fields directly.

**Problem**: The `onlyTransformOnce()` classification is conservative. For large expression graphs, many expressions ARE only transformed in one context but get `MergingTransformInfo` anyway (48+ bytes vs 40 bytes, plus potential map allocations).

**Fix**: The heuristic in `onlyTransformOnce(expr)` could be expanded. Expressions without any dataset dependencies (e.g., scalar arithmetic, constants) will never be context-dependent.  
**Impact**: Reduces memory for MergingHqlTransformer passes by 15–30%.

### 4.8 Short-Circuit for Expressions Without Active Datasets (High Impact, Complex)

**Current state**: The long comment at lines 635–685 discusses this exact optimization:
> "It would be sensible to optimize the transformation so you don't recurse over expressions that can never be transformed."
> The problem is dataset selectors...

The comment identifies two solutions:
1. Recursively check selectors — expensive but cuts 50% off pathological cases
2. Change representation to use unique sequence IDs — eliminates need to remap selectors

**Problem**: Most expressions in the graph are scalar computations that don't reference any dataset selectors. Transforming them is a no-op (they always return themselves). But the framework traverses them anyway because a dataset reference MIGHT be hiding inside via a `no_select` chain.

**Fix**: Add a per-expression flag `containsActiveDatasetReference` (computed once during expression creation). If false AND the transform doesn't modify the expression type, short-circuit immediately in `transform()`:
```cpp
IHqlExpression * NewHqlTransformer::transform(IHqlExpression * expr)
{
    IHqlExpression * transformed = queryAlreadyTransformed(expr);
    if (transformed)
        return LINK(transformed);
    
    // NEW: fast path for expressions with no dataset dependencies
    if (!expr->containsActiveDatasetReference() && !mustTransformNode(expr))
    {
        setTransformed(expr, expr);
        return LINK(expr);
    }
    ...
}
```
**Complication**: The `containsActiveDatasetReference` flag would need to be accurate, accounting for `no_select` chains. And `mustTransformNode` needs to cover passes that modify non-dataset expressions (like constant folding). This makes it pass-specific.  
**Impact**: Could reduce transform calls by 30–60% for passes that only modify dataset-level operations. However, this is the hardest optimization to implement correctly.

### 4.9 `TransformTrackingInfo::transformStack` Depth Stack Optimization (Low Impact)

**Current state**: When a nested transformer sets an expression's extra while a prior transformer's extra already exists, the prior value is pushed onto `transformStack` (a `PointerArray`) and `depthStack` (a `UnsignedArray`). On unlock, these are popped and restored.

**Problem**: For deeply nested transformers (depth 3+), each `setTransformExtra` call pushes 2–3 entries. The `transformStack` and `depthStack` grow via realloc. For a 500K-node graph at depth 3, this stack can reach 1.5M entries.

**Fix**: Pre-allocate the stacks based on expected graph size (available from the transformer's analysis pass, which counts nodes). Add a `reserveTransformStack(expectedSize)` call in `lockTransformMutex()` or the transformer constructor.  
**Impact**: Eliminates realloc overhead during deep transforms. Minor (< 1%) for typical compiles but noticeable for pathological nesting.

### 4.10 Reduce Link/Release Overhead for Unchanged Transforms (Medium Impact)

**Current state**: When `transform(expr)` finds a cache hit and returns `LINK(transformed)`, it increments the reference count. The caller (e.g., `optimizedTransformChildren`) stores it in `lastTransformedChild` (an `OwnedHqlExpr`), then if `child == lastTransformedChild`, the loop continues and the `OwnedHqlExpr` releases it on the next iteration.

**Problem**: For the very common case where `transformed == expr` (expression is unchanged), the sequence is: Link(expr) → compare → Release(expr). This is a redundant ref-count increment/decrement pair. While `CSingleThreadSimpleInterfaceOf` makes these non-atomic, they still cost memory writes.

**Fix**: In `optimizedTransformChildren`, use a non-owning check first:
```cpp
for (; idx < max; idx++)
{
    IHqlExpression * child = expr->queryChild(idx);
    IHqlExpression * raw = queryAlreadyTransformed(child);  // doesn't Link
    if (raw && raw == child)
        continue;  // unchanged, no ref-count touch needed
    lastTransformedChild.setown(transform(child));  // full transform with Link
    if (child != lastTransformedChild)
        break;
}
```
This adds a speculative cache check before the full `transform()` call. Since `queryAlreadyTransformed` is cheap (pointer read + depth check), and the "same" case dominates, this eliminates millions of Link/Release pairs.

**Impact**: Estimated 3–8% improvement for passes with high "same" ratios (which is most passes). The savings are in reduced memory writes (better cache behavior) rather than reduced instructions.

### 4.11 `quickTransformMustTraverse` Could Be Cached (Medium Impact)

**Current state** (line 1239):
```cpp
inline bool quickTransformMustTraverse(IHqlExpression * expr)
{
    return containsNonActiveDataset(expr) || containsActiveNonSelector(expr) || containsMustHoist(expr);
}
```
These three functions check flags stored directly in the expression (`infoFlags`/`infoFlags2`), so they're fast individual checks. However, they're called for EVERY assignment RHS in `quickAnalyseTransform` and `quickTransformTransform`.

**Problem**: While each call is O(1), the three separate flag checks add up across millions of transform assignments. More importantly, for transforms with the `TCOtransformNonActive` flag set, ALL assignments route through this.

**Fix**: Pre-compute a combined flag on expression creation:
```cpp
bool CHqlExpression::mayContainActiveReference() const { return (infoFlags2 & HEF2mayContainActive) != 0; }
```
Set `HEF2mayContainActive = containsNonActiveDataset | containsActiveNonSelector | containsMustHoist` once during `closeExpr()`.

**Impact**: Marginal per-call (saves 2 extra flag reads), but multiplied across millions of calls. Estimated 1–2%.

### 4.12 `createTransformed` Switch Dispatch Optimization (Low Impact)

**Current state**: `NewHqlTransformer::createTransformed` uses a large `switch(op)` over ~30 cases, with a `default: return completeTransform(expr, children)` fallthrough.

**Problem**: The switch covers only special cases. For the vast majority of operator types (200+), the default case is taken. The compiler may generate a jump table or binary search, but either way the most common path (default) is not the fastest to reach.

**Fix**: Check for the common case first before the switch:
```cpp
IHqlExpression * NewHqlTransformer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if (likely(op > no_last_special_transform_op))  // hypothetical boundary
        return completeTransform(expr);
    switch (op) { ... }
}
```
Alternatively, use `[[likely]]` attributes on the default case (C++20).

**Impact**: Negligible on modern CPUs with good branch prediction. Not recommended as a standalone change.

## 5. Priority Ranking

| Priority | Optimization | Expected Impact | Complexity |
|----------|-------------|-----------------|------------|
| **High** | 4.1 Arena allocator for transform infos | 10–20% compile time | Medium |
| **High** | 4.10 Speculative cache check in optimizedTransformChildren | 3–8% compile time | Low |
| **High** | 4.4 Devirtualize `queryTransformed`/`setTransformed` | 2–5% compile time | Low |
| **Medium** | 4.8 Short-circuit for non-dataset expressions | 30–60% fewer transforms | High |
| **Medium** | 4.3 Reduce NewTransformInfo size | Memory reduction | Medium |
| **Medium** | 4.7 Expand `onlyTransformOnce` heuristic | Memory for merging passes | Low |
| **Medium** | 4.11 Combined active-reference flag | 1–2% compile time | Low |
| **Low** | 4.5 Deferred allocation in getTransformedChildren | Minor for records | Low |
| **Low** | 4.6 Replace transformChildren with optimized variant | Minor | Low |
| **Low** | 4.9 Pre-allocate transform stacks | Minor for deep nesting | Low |
| **Low** | 4.12 Switch dispatch optimization | Negligible | Low |

## 6. Implementation Notes

### For 4.1 (Arena Allocator)

The infrastructure already exists (`OPTIMIZE_TRANSFORM_ALLOCATOR` define, `CLargeMemoryAllocator`). The key challenge is destructor management. Current approach uses `CREATE_NEWTRANSFORMINFO` macro for allocation. To make it work:

1. Override `operator new` in `ANewTransformInfo` to use arena (already coded)
2. On transformer destruction, walk all allocated infos and call `Release()` on their `transformed`/`transformedSelector` members
3. Alternative: store a separate list of "infos with non-null transformed" to avoid scanning empty slots

The `transformStack` in `TransformTrackingInfo::unlock()` already iterates all modified expressions — this could be leveraged to also release transform info members.

### For 4.10 (Speculative Cache Check)

Minimal code change in `optimizedTransformChildren`:
```cpp
// In the inner loop (Phase 2), before calling transform():
IHqlExpression * cached = queryAlreadyTransformed(child);
if (cached)
{
    if (cached == child) continue;  // fast path: unchanged
    // Changed — need to backfill and break
    ...
}
// Fall through to full transform(child)
```

This requires exposing `queryAlreadyTransformed` as accessible from `optimizedTransformChildren`. Since it's in the same class hierarchy, this is straightforward.

### For 4.4 (Devirtualize)

Change `NewHqlTransformer::queryAlreadyTransformed` (line 1857) from:
```cpp
return ((NewTransformInfo *)extra)->queryTransformed();
```
to:
```cpp
return static_cast<NewTransformInfo *>(extra)->inlineQueryTransformed();
```

Similarly for `setTransformed`. The `inline` methods are already defined and correct. The only concern is derived classes that override `createTransformInfo` to return a different type — but they also override `queryAlreadyTransformed`/`setTransformed`, so the base class change is safe.

## 7. Measurement Strategy

The framework already has comprehensive instrumentation (`TRANSFORM_STATS`, `TRANSFORM_STATS_TIME`, `TRANSFORM_STATS_DETAILS`, `TRANSFORM_STATS_OPS`). To measure optimization impact:

1. **Enable `TRANSFORM_STATS_ONEXIT`** and compile a large ECL file
2. Record per-transformer stats: `numTransforms`, `numTransformsSame`, `numTransformCalls`, `numTransformCallsSame`, `totalTime`
3. Apply optimization
4. Re-run and compare

Key metrics:
- `numTransformCallsSame / numTransformCalls` — cache hit ratio (should be high)
- `numTransformsSame / numTransforms` — "nothing changed" ratio
- `totalTime - childTime` — self-time per transformer
- Peak RSS — memory impact

The `--leakcheck` eclcc option causes stats to be printed to stdout.
