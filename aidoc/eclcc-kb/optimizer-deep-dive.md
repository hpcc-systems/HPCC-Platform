# Optimizer Deep-Dive: `hqlopt.cpp` and `hqlfold.cpp`

> Last verified: 2026-05

## 1. Architecture Overview

### 1.1 Class Hierarchy

The tree optimizer is implemented in `CTreeOptimizer` (defined in [hqlopt.ipp](../../ecl/hql/hqlopt.ipp)) which inherits from:
- **`NewHqlTransformer`** — the standard graph-walking/rebuilding base class
- **`NullFolderMixin`** — provides `foldNullDataset()` and `queryOptimizeAggregateInline()` methods

The constant folder is implemented as `CExprFolderTransformer` (in [hqlfold.cpp](../../ecl/hql/hqlfold.cpp)) with a separate entry point function `foldConstantOperator()` for scalar expression folding.

### 1.2 Key Support Classes

| Class | Purpose |
|-------|---------|
| `OptTransformInfo` | Per-expression metadata: `useCount`, `stopHoist` flag |
| `ExpandMonitor` | Tracks dataset changes when expanding fields through a project mapper |
| `ExpandSelectorMonitor` | Detects illegal expansions (selector in base dataset expanded) |
| `ExpandComplexityMonitor` | Determines if a transform is too complex to merge/expand |
| `TableProjectMapper` | Maps fields through project transforms for filter/expression hoisting |

### 1.3 Dispatch Mechanism

`CTreeOptimizer::createTransformed()` delegates to `doCreateTransformed()` after:
1. Calling `defaultCreateTransformed()` to recursively transform children
2. Calling `updateOrphanedSelectors()` to fix any selector mismatches
3. The return value is re-transformed if different from input (enabling iterative optimization)

`doCreateTransformed()` first calls `foldNullDataset()` (null/empty dataset folding), then dispatches based on `node_operator`.

### 1.4 Usage Counting

The optimizer maintains a **usage count** per expression node. This is critical because:
- **Shared nodes** (useCount > 1) are NOT split or modified to avoid duplicating work
- **Unshared nodes** (useCount == 1) can be freely rearranged
- Nodes like `no_spill`, `no_split`, `no_spillgraphresult` are always treated as shared
- `no_null` is always treated as unshared

## 2. Optimization Flags (`HOO*`)

| Flag | Value | Meaning |
|------|-------|---------|
| `HOOfold` | 0x0001 | Enable constant folding |
| `HOOcompoundproject` | 0x0002 | Allow projects to be merged into aggregates (non-compound context) |
| `HOOnoclonelimit` | 0x0004 | Move LIMIT inside compound source instead of cloning |
| `HOOnocloneindexlimit` | 0x0008 | Same as above but only for index reads |
| `HOOinsidecompound` | 0x0010 | We are inside a compound activity (enables project removal for counts) |
| `HOOfiltersharedproject` | 0x0020 | Allow filter hoisting over a shared project |
| `HOOhascompoundaggregate` | 0x0040 | Engine supports compound aggregate activities |
| `HOOfoldconstantdatasets` | 0x0080 | Fold operations on constant inline tables |
| `HOOalwayslocal` | 0x0100 | All activities are local (single-node mode) |
| `HOOexpensive` | 0x0200 | Include potentially expensive optimizations (e.g., selseq-aware project matching) |
| `HOOexpandselectcreaterow` | 0x0400 | Extract select from createrow even if scope-dependent |
| `HOOminimizeNetworkAndMemory` | 0x0800 | Don't move row-size-increasing projects over sorts/distributes |

---

## 3. `hqlopt.cpp` — Tree Optimizer Transformations

### 3.1 `no_createrow` — Redundant Row Construction

**Pattern:** `CREATEROW(TRANSFORM(SELF.x := <row>.x, SELF.y := <row>.y, ...))` where all assignments just copy from the same source row with matching record.

**Output:** Return the source `<row>` directly.

**Guards:** Transform must not contain SKIP. All fields must map to the same source.

---

### 3.2 `no_if` — Conditional Dataset Optimization

#### 3.2.1 Branch Elimination by Constant Condition
**Pattern:** `IF(const, trueExpr, falseExpr)` where condition is a constant value.
**Output:** The appropriate branch (true or false) with the other branch's usage decremented recursively.

#### 3.2.2 Identical Branches
**Pattern:** `IF(cond, A, B)` where `A.body == B.body` (or structurally equivalent projects with only differing selseq).
**Output:** Return `A` (remove the IF entirely).
**Flags:** `HOOexpensive` enables selseq-aware matching for projects.

#### 3.2.3 Nested IF Merging
**Pattern:** `IF(c1, IF(c2, x, y), z)` where `y==z` → `IF(c1 AND c2, x, z)`
**Pattern:** `IF(c1, IF(c2, x, y), z)` where `x==z` → `IF(c1 AND NOT c2, y, z)`
**Pattern:** `IF(c1, z, IF(c2, x, y))` where `x==z` → `IF(c1 OR c2, z, y)`
**Pattern:** `IF(c1, z, IF(c2, x, y))` where `y==z` → `IF(c1 OR NOT c2, z, x)`
**Guards:** Only for datasets. Child IF must not be shared. `c2` must not introduce new dependencies beyond `c1`.

#### 3.2.4 IF-to-Filter Conversion (`optimizeDatasetIf`)
**Pattern:** `IF(cond, ds(filt1), ds(filt2))` where both branches filter the same base dataset.
**Output:** `ds(IF(cond, filt1, filt2))` — single filter with conditional expression.
**Guards:** Unfiltered datasets must share the same body.

#### 3.2.5 IF-Append Extraction (`optimizeIfAppend`)
**Pattern:** `IF(a, b+c, b)` → `b + IF(a, DATASET(c))`
**Pattern:** `IF(a, b+c, b+d)` → `b + IF(a, DATASET(c), DATASET(d))`
**Pattern:** `IF(a, b, b+c)` → `b + IF(a, DATASET(), DATASET(c))`
**Guards:** Shared branches can prevent transformation. Remaining attributes must match.

---

### 3.3 `no_filter` — Filter Optimizations

#### 3.3.1 Filter Merging
**Pattern:** `FILTER(FILTER(ds, cond1), cond2)`
**Output:** `FILTER(ds, cond1, cond2)` (single filter with all conditions).
**Guards:** Child filter must not be shared.

#### 3.3.2 Filter Hoisting Over Project (`hoistFilterOverProject`)
**Pattern:** `FILTER(PROJECT(ds, t), cond)` where `cond` can be mapped through `t` back to `ds`.
**Output:** `PROJECT(FILTER(ds, expandedCond), t)`
**Guards:** Project must not have `_countProject`, `prefetch`, or be an aggregate. Transform must be known. Expanded condition must not be complex. When children are shared, only keyed filters are hoisted (`HOOfiltersharedproject` relaxes this).

#### 3.3.3 Filter Swap Over Sort/Distribute/Group
**Pattern:** `FILTER(SORT/DISTRIBUTE/GROUP/SORTED/...(ds), cond)`
**Output:** `SORT/DISTRIBUTE/GROUP/...(FILTER(ds, cond))`
**Guards:** Child must not be shared.

#### 3.3.4 Filter Swap Over Compound Source
**Pattern:** `FILTER(COMPOUND_DISKREAD/INDEXREAD/...(ds), cond)`
**Output:** `COMPOUND_...(FILTER(ds, cond))`
**Guards:** Child must not be limited.

#### 3.3.5 Filter Over Keyed Limit
**Pattern:** `FILTER(KEYEDLIMIT(ds, ..., onFail(t)), cond)`
**Output:** `KEYEDLIMIT(FILTER(ds, cond), ..., onFail(t + SKIP(!cond)))`
**Guards:** The filter is merged into the onFail transform as a SKIP condition.

#### 3.3.6 Filter Hoisting Over Join (`getHoistedFilter`)
**Pattern:** `FILTER(JOIN(L, R, cond), filterCond)`
**Output:** Hoists filter conditions to left/right inputs or merges into join condition.
**Guards:** Join must be pure, known transform, not limited, not full/only outer. Left conditions hoistable only for inner/left joins. Right conditions not hoistable for keyed joins.

#### 3.3.7 Filter Over Select (`moveFilterOverSelect`)
**Pattern:** `FILTER(ds.child, cond)` where some conditions don't reference `child`.
**Output:** `FILTER(ds, hoistedCond).child(remainingCond)`
**Guards:** Must be a "new" selector on a dataset.

#### 3.3.8 Filter on IF/NONEMPTY/ADDFILES
**Pattern:** `FILTER(IF(...), cond)` or `FILTER(ADDFILES(...), cond)`
**Output:** Push filter into each branch via `swapIntoIf` / `swapIntoAddFiles`.

#### 3.3.9 Filter on Inline Table (constant folding)
**Pattern:** `FILTER(INLINETABLE([t1, t2, ...]), cond)` where conditions can be evaluated per-row.
**Output:** Reduced inline table with only rows satisfying the filter.
**Flags:** `HOOfoldconstantdatasets` required.

---

### 3.4 `no_hqlproject` / `no_newusertable` — Project Optimizations

#### 3.4.1 Project Merging (Project over Project)
**Pattern:** `PROJECT(PROJECT(ds, t1), t2)` where `t2` can be expanded through `t1`.
**Output:** `PROJECT(ds, combined_transform)` — single project with merged transform.
**Guards:** Both must be pure. Child transform must not contain SKIP. Count projects can't be merged if both have COUNTER. KEYED attribute on child prevents removal if parent is not keyed.

#### 3.4.2 Project Merging (Table over Project, Project over Table)
Same as above but handling `no_newusertable` (TABLE) combined with `no_hqlproject`.

#### 3.4.3 Project Over Activity (Join/Fetch/Normalize/etc.)
**Pattern:** `PROJECT(JOIN/FETCH/NORMALIZE/...(ds, ..., t1), t2)` where `t2` maps through `t1`.
**Output:** Activity with combined transform via `expandProjectedDataset`.
**Guards:** Both activities must be pure. No count-project on the project.

#### 3.4.4 Project Swap Over Sort/Distribute
**Pattern:** `PROJECT(SORT/DISTRIBUTE(ds, ...), t)`
**Output:** `SORT/DISTRIBUTE(PROJECT(ds, t), mapped_args)` via `moveProjectionOverSimple`.
**Guards:** Not a count-project. `HOOminimizeNetworkAndMemory` blocks if project increases row size.

#### 3.4.5 Project Swap Over Sorted/Distributed/Grouped/Unordered (Metadata)
**Pattern:** `PROJECT(SORTED/DISTRIBUTED/GROUPED/UNORDERED(ds), t)`
**Output:** `SORTED/DISTRIBUTED/...(PROJECT(ds, t), mapped_args)`
**Guards:** Not a count-project.

#### 3.4.6 Project Swap Over Limit/Choosen
**Pattern:** `PROJECT(LIMIT/CHOOSEN(ds), t)` where it's worth moving (disk/index read below).
**Output:** `LIMIT/CHOOSEN(PROJECT(ds, t))` or via `moveProjectionOverLimit` (for onFail).
**Guards:** `isWorthMovingProjectOverLimit()` checks that below the limit there is a compound source, join, or another project.

#### 3.4.7 Project Over Compound Source
**Pattern:** `PROJECT(COMPOUND_DISKREAD/INDEXREAD/...(ds), t)`
**Output:** `COMPOUND_...(PROJECT(ds, t))` — moves project inside compound.
**Guards:** Not a count-project.

#### 3.4.8 Project Over Inline Table (`optimizeProjectInlineTable`)
**Pattern:** `PROJECT(INLINETABLE([t1, t2, ...], rec), transform)`
**Output:** `INLINETABLE([expanded_t1, expanded_t2, ...], new_rec)` — applies project to each inline row at compile time.
**Guards:** Inline table must be pure. Single-row tables always eligible; multi-row only with `HOOfoldconstantdatasets`. Shared inline tables only if fully constant.

#### 3.4.9 Project Into IF/ADDFILES
**Pattern:** `PROJECT(IF(...), t)` or `PROJECT(ADDFILES(...), t)`
**Output:** `IF(cond, PROJECT(branch1, t), PROJECT(branch2, t))` etc.
**Guards:** Transform must not be complex. SKIP-containing transforms block NONEMPTY.

#### 3.4.10 Remove Unused COUNTER
**Pattern:** `PROJECT(ds, t, _countProject(counter))` where transform doesn't reference counter.
**Output:** `PROJECT(ds, t)` — removes _countProject attribute.

---

### 3.5 `no_projectrow` — Row Projection

#### 3.5.1 ProjectRow over CreateRow/ProjectRow
**Pattern:** `PROJECTROW(CREATEROW(t1), t2)` or `PROJECTROW(PROJECTROW(ds, t1), t2)`
**Output:** Merged single row operation with combined transform.
**Guards:** Both pure, known transform.

---

### 3.6 `no_choosen` — CHOOSEN Optimizations

#### 3.6.1 Remove Redundant CHOOSEN
**Pattern:** `CHOOSEN(ds, CHOOSEN_ALL_LIMIT)` without start offset.
**Output:** `ds` (remove CHOOSEN).

#### 3.6.2 Merge Nested CHOOSEN
**Pattern:** `CHOOSEN(CHOOSEN(ds, m), n)` (neither grouped, no start offsets).
**Output:** `CHOOSEN(ds, MIN(m, n))`.

#### 3.6.3 CHOOSEN Swap Over Project
**Pattern:** `CHOOSEN(PROJECT(ds, t), n)` where project is pure and non-aggregate.
**Output:** `PROJECT(CHOOSEN(ds, n), t)` (with `stopHoist` set).
**Guards:** Count-project with start value blocks the swap. Grouped count-project blocks entirely.

#### 3.6.4 CHOOSEN Over Sort → TOPN
**Pattern:** `CHOOSEN(SORT(ds, sortOrder), n)` where `n <= 1000` and not grouped.
**Output:** `CHOOSEN(TOPN(ds, sortOrder, n), n)`.

#### 3.6.5 CHOOSEN Into IF/NONEMPTY/CHOOSEDS
**Pattern:** `CHOOSEN(IF/NONEMPTY/CHOOSEDS(...), n)`
**Output:** Pushed into each branch.

---

### 3.7 `no_limit` — LIMIT Optimizations

#### 3.7.1 LIMIT Swap Over Project
**Pattern:** `LIMIT(PROJECT(ds, t), n)` where project is pure, not aggregate, and no onFail.
**Output:** `PROJECT(LIMIT(ds, n), t)` (with `stopHoist`).

#### 3.7.2 Merge Nested LIMIT
**Pattern:** `LIMIT(LIMIT(ds, m), n)` with same skip/onFail attributes.
**Output:** `LIMIT(ds, MIN(m, n))` or remove parent if parent ≤ child.

#### 3.7.3 LIMIT Over Compound Source
**Pattern:** `LIMIT(COMPOUND_DISKREAD/INDEXREAD(ds), n)` where child is not already limited.
**Output:** Moves limit inside compound (either swap or clone depending on flags).
**Flags:** `HOOnoclonelimit` / `HOOnocloneindexlimit` control the behavior.

#### 3.7.4 Remove Redundant LIMIT
**Pattern:** `LIMIT(CHOOSEN(ds, m), n)` where n > m.
**Output:** Remove LIMIT (parent).
Similar for `LIMIT(TOPN(ds, sort, m), n)` where n > m.

---

### 3.8 `no_dedup` — DEDUP Optimizations

#### 3.8.1 Subsume Parent DEDUP (before sharing check)
**Pattern:** `DEDUP(DEDUP(ds, criteria1), criteria2)` where criteria2 is a no-op relative to criteria1.
**Output:** Remove the parent DEDUP.

#### 3.8.2 Subsume Child DEDUP (after sharing check)
**Pattern:** `DEDUP(DEDUP(ds, criteria1), criteria2)` where criteria2 subsumes criteria1.
**Output:** Remove the child DEDUP (keep the parent).

---

### 3.9 `no_sort` / `no_subsort` — Sort Optimizations

#### 3.9.1 Remove Redundant Child Sort
**Pattern:** `SORT(SORT/SUBSORT/DISTRIBUTED/DISTRIBUTE/KEYEDDISTRIBUTE(ds, ...), newOrder)` where parent is non-local or child is local.
**Output:** Remove the child operation.

#### 3.9.2 Merge SUBSORT into SORT
**Pattern:** `SUBSORT(SORT(ds, partialOrder), ..., restOrder)` where both are local (or `HOOalwayslocal`).
**Output:** `SORT(ds, combinedFullOrder)`.
**Guards:** Must not be grouped. Must derive a valid sort order.

---

### 3.10 `no_distribute` / `no_keyeddistribute` — Distribution Optimizations

#### 3.10.1 Remove Redundant Child Operations
**Pattern:** `DISTRIBUTE(DISTRIBUTED/DISTRIBUTE/SORT/SUBSORT(ds, ...), hash)` without merge attribute.
**Output:** Remove the child distribution/sort.

#### 3.10.2 DISTRIBUTE-DEDUP Optimization (`optimizeDistributeDedup`)
**Pattern:** `DISTRIBUTE(DEDUP(ds, x, y, ALL), hash(x))` where distribution matches dedup equalities.
**Output:** `DEDUP(DISTRIBUTE(ds, hash(x)), x, y, ALL, LOCAL, HASH)` — eliminates a distribution step.
**Guards:** Dedup must be ALL, not local, not grouped, have equalities. With `MANY` attribute, adds a local dedup before the distribute.

#### 3.10.3 Same Distribution Removal
**Pattern:** `DISTRIBUTE(ds, hash)` where `ds` already has that distribution (but is grouped).
**Output:** `GROUP(ds)` — just remove the grouping.

---

### 3.11 `no_aggregate` / `no_newaggregate` — Aggregate Optimizations

#### 3.11.1 Strip Operations Before Aggregate (`optimizeAggregateDataset`)
Walks the aggregate's input chain and removes unnecessary operations:
- **Projects** (pure, non-aggregate): Expand aggregate expressions through the project mapper
- **Sorts/Subsorts/Distributes**: Remove entirely (if aggregate is non-local)
- **Groups**: Remove for scalar aggregates
- **Iterates**: Remove for simple count (if no SKIP)
- **Fetches**: Replace with RHS for simple count
- **Preloads**: Pass through but track as wrapper

#### 3.11.2 Strip Unshared Pre-Aggregate Operations (`optimizeAggregateUnsharedDataset`)
On unshared datasets before aggregation:
- Remove SORT/SUBSORT/DISTRIBUTE (if non-local)
- Convert TOPN to CHOOSEN (for simple counts)
- Remove pure projects (for simple counts inside compound, with `HOOinsidecompound`)

#### 3.11.3 Compound Aggregate (`optimizeAggregateCompound`)
**Pattern:** `AGGREGATE(COMPOUND_DISKREAD/INDEXREAD/...(ds), ...)`
**Output:** `COMPOUND_DISKAGGREGATE/INDEXAGGREGATE/...(AGGREGATE(ds, ...))` — wraps in compound operator.
**Flags:** `HOOhascompoundaggregate` required.

#### 3.11.4 Inline Aggregate Folding
**Pattern:** `AGGREGATE(INLINETABLE([...], rec), ...)` for simple COUNT/EXISTS.
**Output:** Constant inline table with computed value.
**Flags:** `HOOfoldconstantdatasets` required.

---

### 3.12 `no_selectnth` — Index Selection

#### 3.12.1 Index Into Inline Table
**Pattern:** `INLINETABLE[n]` where `n` is constant and table is pure.
**Output:** `CREATEROW(table[n-1])` or NULL if out of range.

#### 3.12.2 Index Into DatasetFromRow
**Pattern:** `DATASETFROMROW(row)[1]`
**Output:** `row` directly. Out-of-range returns NULL.

#### 3.12.3 SelectNth Over Sort → TOPN
**Pattern:** `SORT(ds, order)[n]` where `n <= 100` and not grouped.
**Output:** `TOPN(ds, order, n)[n]`.

#### 3.12.4 SelectNth Over Compound Index Read
**Pattern:** `COMPOUND_INDEXREAD(ds)[n]` where not limited.
**Output:** `COMPOUND_INDEXREAD(CHOOSEN(ds, n, LOCAL))[n]` — add choosen within index read.

---

### 3.13 `no_select` — Field Selection

#### 3.13.1 Select From CreateRow
**Pattern:** `CREATEROW(transform).field` where field can be extracted and is pure.
**Output:** The extracted constant/expression directly.
**Flags:** `HOOexpandselectcreaterow` required for scope-dependent values.

#### 3.13.2 Select From DatasetFromRow
**Pattern:** `DATASETFROMROW(row).field`
**Output:** `row.field` — redirect select to the inner row.

#### 3.13.3 Select From Single-Row Inline Table
**Pattern:** `INLINETABLE([singleTransform]).field` where extractable and result is simple.
**Output:** Extracted expression value.

---

### 3.14 `no_join` — Join Optimizations

#### 3.14.1 Migrate Join Conditions (`optimizeJoinCondition`)
**Pattern:** Simple inner join where condition contains terms only referencing LEFT or RIGHT.
**Output:** Moves LEFT-only terms to a filter on left input, RIGHT-only terms to a filter on right input.
**Guards:** Not keyed, not ATMOST. RIGHT-only conditions not hoisted from keyed joins.

#### 3.14.2 Remove Distribute from Lookup Join RHS
**Pattern:** `JOIN(L, DISTRIBUTE(R, ...), ..., LOOKUP)` (global, non-local).
**Output:** `JOIN(L, R, ..., LOOKUP)` — distribution of RHS is pointless for lookup joins.

#### 3.14.3 Merge Project Into Keyed/Lookup Join LHS
**Pattern:** `JOIN(PROJECT(ds, t), R, cond, ..., KEYED/LOOKUP)` where project is pure and simple.
**Output:** Expand the project transform through the join's LEFT references.
**Guards:** If expanding removes all LEFT references from condition (very-silly join), converts to an ALL join with filtered RHS. ATMOST joins check for right-only hard match.

---

### 3.15 `no_group` — Group Optimizations

#### 3.15.1 Merge Nested Groups
**Pattern:** `GROUP(GROUP(GROUP(ds, g1), g2), g3)` — chain of groups.
**Output:** Collapses to `GROUP(ds, g3)` removing intermediate groups.
**Guards:** Don't allow local groups to remove non-local groups. Don't remove `ALL` groups.

---

### 3.16 `no_normalize` — Normalize Optimizations

#### 3.16.1 NORMALIZE(ds, 0, t) → Empty
**Output:** Null dataset.

#### 3.16.2 NORMALIZE(ds, 1, t) → PROJECT
**Output:** `PROJECT(ds, t)` with COUNTER replaced by constant 1.

---

### 3.17 `no_sample` — Sample Optimization

**Pattern:** `SAMPLE(ds, 1)`
**Output:** `ds` (remove the sample — every 1st record = all records).

---

### 3.18 `no_addfiles` — Merge Inline Tables

**Pattern:** `ADDFILES(INLINETABLE([a,b]), INLINETABLE([c,d]))` (all children are inline tables).
**Output:** Single `INLINETABLE([a,b,c,d])`.

---

### 3.19 `no_datasetfromrow` — Row-to-Dataset Conversion

**Pattern:** `DATASETFROMROW(CREATEROW(t))`
**Output:** `INLINETABLE([t], rec)`.

---

### 3.20 `no_fetch` — Fetch Optimizations

Removes irrelevant child operations from the LHS disk file reference:
- `PRELOAD`, `NOFOLD`, `NOCOMBINE`, `DEDUP`, `GROUP`, `COMPOUND_DISKREAD`, `FILTER`, `SORT`, `SORTED` → strip
- **Project/Table over fetch LHS**: Expands fetch transform through project mapper.

---

### 3.21 `no_keyedlimit` / Keyed Filter — Forced Migration

`queryMoveKeyedExpr` ensures that keyed limits and keyed filters are pushed inside compound source activities by swapping over intervening operations (projects, limits, sorts, etc.).

---

### 3.22 `no_preservemeta` — Metadata Preservation

#### Over Project: Uses `hoistMetaOverProject` to push the metadata annotation inside.
#### Over Compound Source: Simple swap.

---

### 3.23 `no_unordered` — Unordered Hint

**Pattern:** `UNORDERED(ds)` where `ds` does not have an `ordered` attribute.
**Output:** Adds `ordered(false)` attribute to the child instead.

---

### 3.24 `no_split` — Redundant Split Removal

**Pattern:** `SPLIT(SPLIT(ds))`
**Output:** Remove parent split (keep child).
**Guards:** Don't convert unbalanced splitter into balanced.

---

### 3.25 `no_temptable` — Convert to Inline Table

**Pattern:** `TEMPTABLE(list_values, rec)` where values is a `no_list`.
**Output:** Converted to `INLINETABLE` via `convertTempTableToInlineTable`.

---

### 3.26 `no_extractresult` — Extract from Inline Table

**Pattern:** `EXTRACTRESULT(INLINETABLE([single_row]), select)` where field is simple.
**Output:** `SETRESULT(extracted_value)`.

---

### 3.27 `no_distributed` — Distributed Metadata

**Pattern:** `DISTRIBUTED(DISTRIBUTE(ds, hash), hash)` (same distribution).
**Output:** Remove the DISTRIBUTED annotation.

**Pattern:** Over compound sources → swap.

---

### 3.28 `no_parallel` / `no_sequential` / `no_orderedactionlist`

**Pattern:** Contains `no_null` actions.
**Output:** Remove null actions from the list; if single action remains, return it directly.

---

## 4. `hqlfold.cpp` — Constant Folder Transformations

### 4.1 `foldNullDataset()` — Null/Empty Dataset Folding

#### 4.1.1 Distribute/Sort/Subsort/Sorted
- NULL input → return NULL
- FAIL input → remove parent (pass through)
- Redundant (same sort/distribution) → remove parent
- Single row → remove sort
- All-constant sort criteria → remove sort

#### 4.1.2 Group/Grouped
- Same sort/group/distribution → remove
- NULL input → return NULL

#### 4.1.3 Join/Denormalize
- Both NULL → NULL (full join)
- LHS NULL → NULL (inner/left), or convert to project from RHS (right/full)
- RHS NULL → NULL (inner/right), or remove parent (denormalize, since transform not called), or convert to project from LHS (join)
- FALSE join condition → NULL (inner), or convert to project (left outer), since transform never matches
- TRUE join condition on single-row inputs with constant transform → DATASETFROMROW(CREATEROW(t))
- LEFT OUTER + KEEP(1) / ATMOST(1) / single-LOOKUP / single-row + no RIGHT in transform → convert to PROJECT

#### 4.1.4 Merge/AddFiles/Regroup/Nonempty/Cogroup
- Remove NULL branches. If single input remains, return it directly.
- COGROUP with single input → GROUP.

#### 4.1.5 Choosen
- NULL/FAIL input → remove parent
- Limit = 0 or negative → NULL
- Limit > 0 on single-row → remove
- `CHOOSEN_ALL_LIMIT` → remove
- Child known to have ≤ limit rows → remove

#### 4.1.6 Dedup/Rollup
- NULL or single-row or FAIL → remove parent.

#### 4.1.7 Limit
- NULL or FAIL → remove parent
- Child known to have ≤ limit rows → remove

#### 4.1.8 Filter/Sample/TopN/etc. (many operators)
- NULL or FAIL → remove parent

#### 4.1.9 Normalize
- NULL input or count=0 → NULL

#### 4.1.10 Project/ProjectRow
- Null project (identity transform) → remove
- NULL input → NULL

#### 4.1.11 Compound Sources
- Same-operator nesting or null child → remove parent (unwrap compound)

#### 4.1.12 Assert_ds
- NULL input or all asserts constant-folded away → remove

---

### 4.2 `foldConstantOperator()` — Scalar Expression Folding

#### 4.2.1 Arithmetic
| Pattern | Output |
|---------|--------|
| `x + 0` | `x` |
| `0 + x` | `x` |
| `x - 0` | `x` |
| `x * 0` or `0 * x` | `0` |
| `x * 1` or `1 * x` | `x` |
| `x / 1` (integer) | `x` |
| const op const | evaluated result |

#### 4.2.2 Boolean Logic
| Pattern | Output |
|---------|--------|
| `TRUE AND x` | `x` |
| `FALSE AND x` | `FALSE` |
| `TRUE OR x` | `TRUE` |
| `FALSE OR x` | `x` |
| `x AND TRUE` | `x` |
| `x OR FALSE` | `x` |
| `NOT NOT x` | `x` |
| `NOT (a == b)` | `a != b` (and all comparison inversions) |
| `NOT (a IN b)` | `a NOT IN b` |

#### 4.2.3 Bitwise Operations
| Pattern | Output |
|---------|--------|
| `0 BAND x` or `x BAND 0` | `0` |
| `(x BAND y) BAND z` (y,z const) | `x BAND (y AND z)` |
| `0 BOR x` or `x BOR 0` | `x` |

#### 4.2.4 Comparisons (`optimizeCompare`)
| Pattern | Output |
|---------|--------|
| `x == x` | `TRUE` (and other self-comparisons) |
| `ALL == const` | based on comparison semantics |
| const op const | evaluated result |
| `0 <= unsigned_x` | `TRUE` |
| `unsigned_x < 0` | `FALSE` |
| `x == TRUE/FALSE` | `x` / `NOT x` |
| `(cast)x op const` | `x op uncast(const)` if cast preserves value/order |
| `list[n] op const` | folded if list is constant |
| `CASE(...) op const` | transformed to IN expression |

#### 4.2.5 Cast Optimization
| Pattern | Output |
|---------|--------|
| `(T)const` | evaluated constant |
| `(T1)(T2)x` where T2→T1 preserves value | `(T1)x` |
| `(string)x` where x is already that type | `x` |
| `(T)(CASE(..))` with all-constant results | `CASE(..)` with cast pushed into results |

#### 4.2.6 IN/NOT IN
| Pattern | Output |
|---------|--------|
| `x IN ALL` | `TRUE` |
| `x IN []` | `FALSE` |
| `x IN [single]` | `x == single` |
| `const IN [...]` | remove impossible matches, return TRUE if match found |
| `CASE(x,...) IN [...]` | transformed to `x IN [matching_keys]` |
| `(cast)x IN [...]` | cast removed if preserves value |

#### 4.2.7 IF (Scalar)
| Pattern | Output |
|---------|--------|
| `IF(const, a, b)` | `a` or `b` |
| `IF(c, a, a)` | `a` |
| `IF(c, TRUE, x)` | `c OR x` |
| `IF(c, FALSE, x)` | `NOT c AND x` |
| `IF(c, x, TRUE)` | `NOT c OR x` |
| `IF(c, x, FALSE)` | `c AND x` |
| `IF(c, IF(c, a, b), y)` | `IF(c, a, y)` |
| `IF(c, x, IF(c, a, b))` | `IF(c, x, b)` |

#### 4.2.8 CASE/MAP
| Pattern | Output |
|---------|--------|
| `CASE(const, ...)` | matching branch |
| `CASE(CASE(...))` | merged single CASE |
| All results match default | default value |
| Single-case CASE | `IF(test == key, result, default)` |
| MAP with constant conditions | short-circuit to first TRUE |
| MAP where all conditions are `x IN [...]` or `x == c` | converted to CASE |
| MAP where all results equal default | default |

#### 4.2.9 BETWEEN
| Pattern | Output |
|---------|--------|
| All constant | evaluated |
| `x BETWEEN a AND a` | `x == a` |

#### 4.2.10 Substring
| Pattern | Output |
|---------|--------|
| Constant string + constant range | evaluated substring |
| `(stringN)x[1..m]` where m ≥ N | `(stringN)x` (cast already truncates) |
| `x[m..n]` where n < m or n=0 | empty string |

#### 4.2.11 Math Functions
All unary math functions (`NEGATE`, `EXP`, `LN`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `SINH`, `COSH`, `TANH`, `LOG10`, `SQRT`, `ABS`, `ROUND`, `ROUNDUP`, `TRUNCATE`) with constant arguments are evaluated at compile time.

#### 4.2.12 String Functions
- `TRIM(constant)` → evaluated
- `TRIM((stringN)x)` where cast extends → `TRIM(x)` (trim on larger type is same as trim on original)
- `INTFORMAT(const, const, const)` → evaluated
- `REALFORMAT(const, const, const)` → evaluated
- `REGEX_FIND/REPLACE(const, const, const)` → evaluated at compile time
- `CHARLEN(fixedType)` → constant
- `CHARLEN(x[a..b])` with constant range → constant

#### 4.2.13 External Function Calls
Calls to external functions marked with `FOLD` attribute with all-constant arguments are evaluated at compile time by dynamically loading the plugin DLL and invoking the function.

#### 4.2.14 SIZEOF
- `SIZEOF(fixedRecord)` → constant
- `SIZEOF(record, MAX)` → constant if derivable
- `SIZEOF(record, MIN)` → constant

---

### 4.3 `foldOrExpr()` — OR Expression Optimization

1. Remove FALSE terms, short-circuit TRUE
2. Remove duplicate terms (`a OR a` → `a`)
3. `x OR NOT x` → TRUE (when `HFOx_op_not_x` enabled)
4. Merge bitmask patterns: `(a BAND b) != 0 OR (a BAND c) != 0` → `(a BAND (b BOR c)) != 0`
5. Merge equality comparisons: `x=a OR x=b OR x=c` → `x IN [a, b, c]`

### 4.4 `foldAndExpr()` — AND Expression Optimization

1. Remove TRUE terms, short-circuit FALSE
2. Remove duplicate terms (`a AND a` → `a`)
3. `x AND NOT x` → FALSE (when `HFOx_op_not_x` enabled)

---

### 4.5 `doFoldTransformed()` — Post-Transform Dataset Folding

#### 4.5.1 NORMALIZE on Single-Row Dataset
**Pattern:** `NORMALIZE(singleRowDs, count, transform)` where LEFT can be resolved.
**Output:** `DATASET_FROM_TRANSFORM(count, expandedTransform)` — removes LEFT dependency.

#### 4.5.2 FILTER with Constant Conditions
- FALSE condition → NULL dataset
- TRUE conditions removed
- Filter over project: expand filter through project; if constant → fold inline

#### 4.5.3 FILTER on Inline Table
Evaluates filter per-row at compile time, removes non-matching rows.

#### 4.5.4 Aggregate on Inline Table / Single Row
Simple COUNT/EXISTS aggregates on known-size datasets are folded to constants.

#### 4.5.5 COUNT of Inline Table
**Pattern:** `COUNT(INLINETABLE([n items]))` (no SKIPs).
**Output:** Constant `n`.

---

## 5. Interaction Between `hqlopt.cpp` and `hqlfold.cpp`

The optimizer (`CTreeOptimizer`) calls `foldNullDataset()` at the **start** of `doCreateTransformed()` via its `NullFolderMixin` base class. This handles null propagation immediately.

After the tree optimizer completes a full pass, the top-level `optimizeHqlExpression()` function calls `foldHqlExpression()` on the result — running a complete constant-folding pass to catch new opportunities exposed by restructuring.

The constant folder (`CExprFolderTransformer`) uses `foldConstantOperator()` for scalar expressions and its own `foldNullDataset()` for datasets. It also calls `percolateConstants()` when `HFOpercolateconstants` is set, which propagates known values from single-row datasets into downstream expressions.

---

## 6. Summary of Precondition Checks

| Check | Purpose |
|-------|---------|
| `isShared(expr)` | Prevents splitting shared subgraphs |
| `childrenAreShared(expr)` | Gates most inter-node optimizations |
| `isPureActivity(expr)` | Ensures activity has no side effects |
| `hasUnknownTransform(expr)` | Blocks expansion through opaque transforms |
| `isAggregateDataset(expr)` | Distinguishes TABLE used as aggregate vs. project |
| `containsSkip(expr)` | SKIPs in transforms block many merges |
| `isComplexTransform(expr)` | Prevents expansion of expensive transforms into multiple branches |
| `isGrouped(expr)` | Many sort/distribute removals are invalid for grouped datasets |
| `isLocalActivity(expr)` | Distribution-removing optimizations check locality |
| `queryBodyExtra(expr)->getStopHoist()` | Prevents infinite optimization loops (set by `forceSwapNodeWithChild`) |
