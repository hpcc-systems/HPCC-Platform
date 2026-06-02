# Child Query Code Generation

## Overview

During the code generation phase, the ECL compiler must decide how to evaluate each dataset expression that appears within a transform (or similar context). The two fundamental strategies are:

1. **Inline evaluation** — the dataset is computed directly within the parent activity's C++ method using loops, conditionals, and direct memory manipulation.
2. **Child query evaluation** — the dataset expression is extracted into a separate subgraph (a "child query") that is executed as an independent unit of work, with results passed back via graph-result channels.

This decision profoundly affects generated code structure, performance characteristics, and the overall activity graph topology.

## Key Source Files

| File | Role |
|------|------|
| `ecl/hqlcpp/hqlinline.cpp` | Classification engine (`calcInlineFlags`) |
| `ecl/hqlcpp/hqlhtcpp.cpp` | Transform building + child dataset spotting |
| `ecl/hqlcpp/hqlcppds.cpp` | Child graph construction + dataset assignment |
| `ecl/hqlcpp/hqlcppds.hpp` | `ChildGraphExprBuilder` / `ChildGraphBuilder` classes |
| `ecl/hqlcpp/hqlhoist.hpp` | `ConditionalContextTransformer` base class |
| `ecl/hqlcpp/hqlresource.cpp` | `resourceNewChildGraph` — activity-level resourcing |
| `ecl/hqlcpp/hqlcse.cpp` | `spotScalarCSE` — common subexpression elimination |

## The Inline Classification Engine

### `calcInlineFlags()` (hqlinline.cpp:71)

Returns a bitmask describing how an expression can be evaluated:

| Return Value | Meaning |
|---|---|
| `RETevaluate` (0x0F) | Fully materialized; all elements accessible directly |
| `RETiterate` (0x07) | Can iterate element-by-element without full materialization |
| `RETassign` (0x03) | Can be assigned to a temporary (e.g., serialized to memory) |
| `0` | Cannot be evaluated inline; **requires child query** |

### Context Sensitivity

The very first check in `calcInlineFlags` is context-dependent:

```cpp
if (ctx) {
    if (expr->isDataset() || expr->isDictionary()) {
        if (ctx->queryMatchExpr(expr))    // already bound in this BuildCtx
            return RETevaluate;
    } else {
        if (ctx->queryAssociation(expr, AssocRow, NULL))
            return RETevaluate;
    }
}
```

This means an expression that would normally require a child query can be treated as inline if it has already been evaluated and its result is associated in the current `BuildCtx`. This is crucial for child-query results that are consumed by subsequent assignments.

### What Forces a Child Query (returns 0)

- **Grouped datasets** — grouping semantics require activity-level processing
- **Parallel operations** — cannot be serialized inline
- `no_forcegraph` — explicit annotation forcing graph creation
- `no_catchds` — error-catching semantics require activity boundaries
- `no_table` — disk/index reads
- **Most JOINs** — except ALL-joins with iterable inputs
- **SORT, DISTRIBUTE, DEDUP(ALL)** — operations requiring full dataset materialization

### What Can Be Inlined (returns non-zero)

- `FILTER`, `PROJECT`, `CHOOSEN`, `LIMIT`, `ADDFILES`
- `CREATEROW`, `INLINETABLE`, `SELECTNTH`
- `NEWAGGREGATE` — only if its child input is also inline
- ALL-JOIN with iterable inputs
- Any expression already bound in the current context

### Helper Predicates

```
canProcessInline(ctx, expr)      →  getInlineFlags(ctx, expr) != 0
canAssignInline(ctx, expr)       →  canAssignNoSpill(flags)  [flags >= RETassign]
canEvaluateInline(ctx, expr)     →  canEvaluateNoSpill(flags) [flags == RETevaluate]
canIterateInline(ctx, expr)      →  canIterateNoSpill(flags)  [flags >= RETiterate]
```

## Transform Processing Pipeline

### Entry Points

All transform code generation flows through one of:

1. **`buildTransformBody()`** → `doBuildTransformBody()` → `doTransform()`
2. **Activity-specific builders** (e.g., project, iterate) that create a `TransformBuilder` directly

### `TransformBuilder::doTransform()` (hqlhtcpp.cpp:1054)

The core orchestration:

```
1. Strip annotations (recurse through annotate_meta/annotate_symbol)
2. If not a "known transform" → doUserTransform() [handles arbitrary user-defined expressions]
3. Call filterExpandAssignments() → extracts individual assignments
4. Call buildTransformChildren() → walks record structure, matches assignments to fields
5. flush() → triggers child dataset spotting + code generation
```

### `filterExpandAssignments()` (hqlhtcpp.cpp:1417)

This function preprocesses the transform before field-by-field processing:

1. **CSE spotting**: Calls `spotScalarCSE(transform)` to identify common subexpressions across assignments and wrap them in `no_alias` nodes
2. **Assignment extraction**: `doFilterAssignments()` recursively flattens `no_assignall`/`no_transform` into individual `no_assign` statements; `no_skip`/`no_alias` become action nodes processed by the builder

### The `DelayedStatementExecutor` Pattern

`TransformBuilder` inherits from `DelayedStatementExecutor`, which **accumulates** assignment statements rather than immediately generating code. This deferred execution enables:

1. **Child dataset spotting** — analyze all pending assignments together
2. **Condition combining** — merge multiple IF/CHOOSE with same guard
3. **Assignment optimization** — reorder/eliminate redundant assignments

### `flush()` — The Critical Trigger

When `flush(ctx)` is called on the `DelayedStatementExecutor`:

```cpp
void flush(BuildCtx & ctx) {
    spotChildDatasets(false);    // ← child query detection
    combineConditions();         // ← merge same-guard conditionals
    optimizeAssigns();           // ← subclass hook (UpdateTransformBuilder)
    ForEachItemIn(i, pending)
        translator.buildStmt(ctx, &pending.item(i));  // ← actual code gen
    pending.kill();
}
```

## Child Dataset Spotting

### `spotChildDatasets()` (hqlhtcpp.cpp:762)

This is invoked with `forceRoot=false` during normal transform flushing, or `forceRoot=true` for prefetch graph generation.

```
1. Guard: if already processed, or commonUpChildGraphs is disabled → return
2. Extract RHS values from all pending assignments
3. Create NewChildDatasetSpotter
4. Call analyseNeedsTransform() → multi-pass analysis
5. If candidates found and worth hoisting:
   - transformAll(pending) → replaces dataset exprs with no_getgraphresult references
   - Prepends no_childquery nodes to the pending list
```

### `NewChildDatasetSpotter` (hqlhtcpp.cpp:365)

Extends `ConditionalContextTransformer` — a sophisticated framework for hoisting expressions out of conditional branches.

**Key design decision**: `createRootGraph = true`

This forces all child queries to be placed at the root (before any assignments), rather than at the first unconditional use point. The comment explains why:

> "if an optimization causes an expression containing the no_getgraphresult to be hoisted so it is evaluated before the no_childquery it creates an out-of-order dependency"

#### Analysis Passes

The `ConditionalContextTransformer` runs multiple passes:

| Pass | Purpose |
|------|---------|
| `PassFindConditions` | Identify conditional branch structure |
| `PassFindCandidates` | Mark expressions that need child queries (`markHoistPoints`) |
| `PassFindParents` | Determine common parent locations |
| `PassGatherGuards` | Compute guard conditions for conditional candidates |

#### `markHoistPoints()` (hqlhtcpp.cpp:420)

```cpp
void markHoistPoints(IHqlExpression * expr) {
    if (expr->isDataset() || (expr->isDatarow() && (op != no_select))) {
        if (!translator.canAssignInline(&ctx, expr)) {
            noteCandidate(expr);     // ← this needs a child query
            return;
        }
        if (!walkFurtherDownTree(expr))
            return;                   // ← inline, no need to look deeper
    }
    doAnalyseExpr(expr);              // ← recurse into children
}
```

#### `walkFurtherDownTree()` — Transparent Containers

Even if an expression *can* be inlined, certain operators are "transparent" — their children might still contain non-inlineable datasets:

- `no_createrow` / `no_inlinetable` — transforms within may reference datasets
- `no_addfiles` — either branch might need a child query
- `no_datasetfromrow` / `no_datasetfromdictionary`
- `no_if` — condition or branches may need hoisting

#### `transformCandidate()` — Guard Handling

When a candidate is conditionally used:

1. Compute guard condition (the boolean that determines if this branch executes)
2. If the expression is "used unconditionally enough" (via aggregate/selectnth/filter chains from unconditional use), treat as unconditional
3. **Current policy**: disable conditional candidates entirely — the comment explains:
   > "Often including conditions improves the code, but sometimes the duplicate evaluation of the guard conditions in the parent and the child causes excessive code generation"

This means conditional child queries are either:
- Promoted to unconditional (if `isUsedUnconditionallyEnough` returns true)
- Left in-place without hoisting (computed at point of use)

The comments following the `invalid = true` line elaborate the specific problems encountered: duplicate guard evaluation in parent and child causes excessive code generation, and forcing into an alias doesn't help because it isn't currently executed in the parent. The commented-out `guardContainsCandidate` check represents an abandoned alternative approach.

### Candidate Deduplication

`ChildGraphExprBuilder::addDataset()` deduplicates by expression body:

```cpp
ForEachItemIn(i, results) {
    if (expr->queryBody() == curSetResult.queryChild(0)->queryBody()) {
        resultNumExpr.set(curSetResult.queryChild(2));  // reuse slot
        break;
    }
}
```

If the same dataset expression appears in multiple assignments within the same transform, it generates only **one** graph result and multiple `no_getgraphresult` references to the same slot.

## The Fallback Path in `buildDatasetAssign()`

### Decision Logic (hqlcppds.cpp:2755)

After the large `switch` handling special cases (calls, graph results, inline tables, aliases, compounds, IF, serialize/deserialize, select, type transfer, dictionaries):

```cpp
if (!canAssignInline(&ctx, expr) && (op != no_translated)) {
    buildAssignChildDataset(ctx, target, expr);
    return;
}
```

This is the **final safety net** — if the `NewChildDatasetSpotter` didn't already extract this expression (perhaps because `commonUpChildGraphs` is disabled, or the expression wasn't seen during the spotting pass), it gets wrapped in a child query here.

### `buildAssignChildDataset()` (hqlcppds.cpp:1818)

Creates a minimal single-result child graph:

```cpp
ChildGraphExprBuilder builder(0);
call.setown(builder.addDataset(expr));       // → no_getgraphresult
OwnedHqlExpr subquery = builder.getGraph();  // → no_childquery
buildStmt(ctx, subquery);                    // → generates the graph
buildExprAssign(ctx, target, call);          // → reads graph result
```

## Child Graph Execution Pipeline

### `getResourcedChildGraph()` (hqlcppds.cpp:1845)

Once a `no_childquery` node is created, its execution goes through:

```
1. CompoundSourceTransformer(CSFpreload|csfFlags)
   → Combines adjacent disk reads into compound activities
   
2. optimizeHqlExpression()
   → Standard optimizer pass on the child subgraph
   
3. resourceNewChildGraph() / resourceLoopGraph()
   → Activity-level resourcing (splitter insertion, memory budgets)
   
4. optimizeGraphPostResource()
   → Post-resourcing cleanup
   
5. convertSpillsToActivities() [if optimizeSpillProject]
   → Convert spill markers to actual temporary-write activities
```

### `ChildGraphBuilder::generateGraph()` (hqlcppds.cpp:1554)

Generates the actual C++ code for executing a child graph:

1. Call `getResourcedChildGraph()` to get the final activity graph
2. Create a `ParentExtract` — the serialized row data passed from parent to child
3. Call `doBuildThorSubGraph()` — generates activity classes for the subgraph
4. Create graph lookup instance (`IEclGraphResults`)
5. Generate `evaluateChildQueryInstance()` call — executes the graph and returns results
6. Associate `resultsExpr` in context — so `no_getgraphresult` can find the results

### Result Consumption

When `buildStmt` encounters `no_getgraphresult` (via `doBuildExprGetGraphResult`):

- If the `externalAtom` attribute is present → call `buildGetLocalResult()` to fetch from the `IEclGraphResults` interface
- Otherwise → treat as an alias (already evaluated and available)

## Prefetch Optimization

### `getPrefetchGraph()` (hqlhtcpp.cpp:713)

For `PROJECT` activities, the child dataset spotter runs with `forceRoot=true`. If it detects child queries, they are extracted into a separate **prefetch function** (`preTransform`):

```
1. getPrefetchGraph() calls spotChildDatasets(true)
2. If first pending item is no_childquery → extract it
3. Generate preTransform() function containing the child graph execution
4. Generate queryChild() returning the graph instance
5. In the main transform(), associate results so no_getgraphresult works
```

This allows the engine runtime to execute the child graph **ahead of the main transform**, potentially overlapping I/O with computation.

### Why Only PROJECT?

The prefetch pattern requires a specific helper interface that separates child graph execution from the main transform. Only `IHThorPrefetchProjectArg` (eclhelper.hpp) provides this two-phase API:

```cpp
struct IHThorPrefetchProjectArg : public IHThorArg {
    virtual IThorChildGraph *queryChild() = 0;                              // child graph access
    virtual bool preTransform(rtlRowBuilder & extract, const void * _left,  // phase 1: execute child
                              unsigned __int64 _counter) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left,// phase 2: use results
                               IEclGraphResults * results, unsigned __int64 _counter) = 0;
    virtual unsigned getLookahead() = 0;                                    // pipeline depth
};
```

Other activity helpers (JOIN, NORMALIZE, DENORMALIZE) have a single `transform()` method with no mechanism to separate child graph execution from the main computation. This is a fundamental API design constraint — extending prefetch to other activities would require new helper interfaces and runtime support.

## Conditional Branches in Transforms

### IF Expressions and Child Queries

When a transform assigns `SELF.field := IF(cond, ds1, ds2)` where both `ds1` and `ds2` require child queries:

1. **StatementCollection::combineConditions()** — if multiple assignments share the same IF condition, they are combined into a single conditional block
2. **buildDatasetAssignIf()** — for `no_if` nodes where both branches can be evaluated inline, generates conditional assignment without child query
3. **Fallback** — if branches cannot be inlined, the entire IF falls through to `buildAssignChildDataset()`, creating a child query containing the IF logic

### The `isUsedUnconditionallyEnough()` Heuristic

An expression within a conditional branch can be treated as unconditional if it's accessed through a chain of "trivial" operations:

```cpp
// These operators are considered trivial wrappers:
no_newaggregate  (unless hash aggregate)
no_selectnth
no_filter
no_select (if isNewSelector)
```

By this point in the compilation pipeline, `EXISTS(ds)` and `COUNT(ds)` have already been lowered to `SELECT(SELECTNTH(NEWAGGREGATE(ds), 1), field)` — so the existing chain of `no_select → no_selectnth → no_newaggregate` in `isUsedUnconditionallyEnough` already handles these common patterns. There may be additional operators worth adding to this heuristic in the future.

## The `no_alias` Mechanism

### Role in Sharing

`spotScalarCSE` identifies expressions used multiple times within a transform and wraps them in `no_alias` nodes. During `doFilterAssignment()`:

```cpp
case no_alias:
    if (builder)
        builder->processAlias(ctx, cur);  // → added to pending list
    else
        buildStmt(ctx, cur);
```

The alias is queued in the `DelayedStatementExecutor::pending` list alongside regular assignments. When `flush()` runs, child dataset spotting sees the alias value and can:
- Include it in the child query if it references non-inlineable datasets
- Evaluate it once and associate in the context for reuse

### Dataset Aliases

For `no_alias` wrapping a dataset (in `doBuildDataset`):

```cpp
case no_alias:
    doBuildExprAlias(ctx, expr, &tgt, NULL);
    return;
```

And in `buildDatasetAssign`:

```cpp
case no_alias:
    CHqlBoundExpr bound;
    buildDataset(ctx, expr, bound, FormatNatural);
    OwnedHqlExpr translated = bound.getTranslatedExpr();
    buildDatasetAssign(ctx, target, translated);
    return;
```

The alias evaluates the dataset once, then the translated (already-bound) result can be assigned to any target without re-evaluation.

## UpdateTransformBuilder and Child Queries

`UpdateTransformBuilder` extends `TransformBuilder` to allow in-place row mutation rather than cloning. Its `optimizeAssigns()` hook removes leading assignments that are unchanged from the previous row and protects field references that would become invalid after earlier fields are overwritten.

This optimization operates on assignment order and aliasing — it does **not** affect whether child queries are executed. The child dataset spotting occurs in the base class (`DelayedStatementExecutor::flush()`), which runs before `optimizeAssigns()` examines the pending statements. If a child query is detected and hoisted, the `no_childquery` statement will appear in the pending list regardless of whether subsequent assignment optimizations remove references to its results.

## Loop Invariant Motion

### `LoopInvariantHelper` (hqlcpp.ipp:2283)

When building expressions inside loops (e.g., row iteration), this helper uses `BuildCtx::selectBestContext()` to move invariant computations to an outer scope:

```cpp
bool LoopInvariantHelper::getBestContext(BuildCtx & ctx, IHqlExpression * expr) {
    active = ctx.selectBestContext(expr);  // walks up context tree
    return (active != NULL);
}
```

`selectBestContext()` examines which tables/cursors the expression depends on, then finds the highest scope where all dependencies are satisfied. This is particularly relevant for child datasets that don't depend on the iteration variable — they can be computed once outside the loop.

## End-to-End Flow Summary

```
Transform expression (no_transform)
    │
    ▼
filterExpandAssignments()
    ├── spotScalarCSE() → wraps shared subexprs in no_alias
    └── doFilterAssignments() → flattens into individual assignments
            │
            ▼
    TransformBuilder::buildTransformChildren()
        ├── walks record field-by-field
        ├── matches each field to its assignment
        └── calls processAssign() → queues in pending list
                │
                ▼
        DelayedStatementExecutor::flush()
            ├── spotChildDatasets()
            │       ├── NewChildDatasetSpotter analysis (4 passes)
            │       ├── markHoistPoints() → identifies non-inlineable datasets
            │       ├── transformCandidate() → builds graph results
            │       └── transformAll() → replaces exprs with no_getgraphresult,
            │                            prepends no_childquery to pending
            │
            ├── combineConditions() → merge same-guard IF/CHOOSE
            ├── optimizeAssigns() → (UpdateTransformBuilder hook)
            │
            └── buildStmt() for each pending item:
                    ├── no_childquery → buildChildGraph()
                    │       ├── getResourcedChildGraph()
                    │       │       ├── CompoundSourceTransformer
                    │       │       ├── optimizeHqlExpression
                    │       │       ├── resourceNewChildGraph
                    │       │       └── optimizeGraphPostResource
                    │       └── ChildGraphBuilder::generateGraph()
                    │               ├── ParentExtract creation
                    │               ├── doBuildThorSubGraph (activity classes)
                    │               └── evaluateChildQueryInstance call
                    │
                    ├── no_assign → buildDatasetAssign() / buildExprAssign()
                    │       └── [if still not inline] → buildAssignChildDataset()
                    │
                    └── no_alias / no_skip → direct code generation
```

## Design Trade-offs and Observations

### Why Not Always Use Child Queries?

Child queries have overhead:
- **Serialization**: Parent extract must serialize all referenced parent columns
- **Graph setup**: Activity class instantiation, result buffer allocation
- **Indirection**: Results must be fetched through `IEclGraphResults` interface

For simple operations (filter + project), inline code is vastly more efficient — just a loop with conditionals.

### Why Not Always Inline?

Some operations are fundamentally incompatible with inline evaluation:
- **Sort** requires seeing all rows before producing output
- **Group** requires maintaining group boundaries across iterations
- **Disk read** requires I/O scheduling
- **Parallel** requires multi-threaded execution

### The Guard Condition Dilemma

The comment in `transformCandidate()` reveals an unresolved tension:

> "Often including conditions improves the code, but sometimes the duplicate evaluation of the guard conditions in the parent and the child causes excessive code generation"

If a child query is guarded: the guard must be evaluated in the parent (to decide whether to execute the graph) AND inside the child (because the guard might reference parent data). This duplication can cause code bloat. The current conservative policy disables conditional child queries entirely, preferring to either:
- Promote them to unconditional (cheap enough to always run)
- Leave them non-hoisted (computed at point of use in the fallback path)

### Prefetch as a Middle Ground

The prefetch pattern (`getPrefetchGraph` with `forceRoot=true`) provides a way to:
1. Extract all child queries into a single pre-computation step
2. Allow the runtime to pipeline/overlap child query execution
3. Keep the main transform simple (just reads from pre-computed results)

This is currently used only for PROJECT activities.

### Deduplication Semantics

The `ChildGraphExprBuilder::addDataset` comparison uses `queryBody()` — this strips all annotations but preserves the structural identity. Two references to the "same" dataset (structurally identical after annotation removal) share a single graph result slot, even if they appear in different assignments or at different locations in the expression tree.

### The `commonUpChildGraphs` Option

The `commonUpChildGraphs` option (default: `true`) controls whether the `NewChildDatasetSpotter` runs at all. Like many code generation options in the compiler, this is a **debug/testing flag** — not intended to be changed in production. It was introduced to test the effects of the child-query commoning optimization when first implemented, allowing comparison of generated code with and without the feature. Disabling it causes each non-inlineable dataset to fall through to `buildAssignChildDataset()` individually, without cross-assignment deduplication.
