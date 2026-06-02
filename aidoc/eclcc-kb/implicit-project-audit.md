# Implicit Project Pass Audit (`ecl/hqlcpp/hqliproj.cpp`)

> Last verified: 2026-05

## 1. Purpose and Mechanism

The implicit project pass identifies fields in dataset records that flow through the activity graph but are never consumed downstream, and removes them. By narrowing records to only the fields actually needed, this pass reduces:
- Memory consumption (smaller rows)
- Network traffic (for distributed activities on Thor)
- Disk I/O (for spilled intermediates)
- Serialization/deserialization cost

### How It Tracks Used Fields

The pass maintains a **`UsedFieldSet`** per activity, representing the set of fields from a record that are actually required. It uses **selector expressions** (`no_select` nodes representing field accesses like `ds.fieldName`) as the primary mechanism for discovering which fields are consumed. These selectors are gathered bottom-up from non-activity sub-expressions, then propagated top-down from consumers to producers.

### Core Data Structures

- **`UsedFieldSet`**: The fundamental structure representing a subset of fields from a record. It stores:
  - `fields` (`HqlExprArray`): List of field expressions that are needed
  - `nested` (`CIArrayOf<NestedField>`): Nested record fields with their own sub-field tracking
  - `all` (bool): Short-circuit flag meaning "all fields required"
  - `originalFields` (pointer to the full `UsedFieldSet` for the record): Used to check if optimization is possible
  - `finalRecord` (`OwnedHqlExpr`): The synthesized reduced record expression
  - `maxGathered` (unsigned): Tracks incremental gathering progress to avoid reprocessing

- **`NestedField`**: Represents a datarow field within a record that itself has sub-fields. Contains its own `UsedFieldSet` allowing hierarchical field tracking.

- **`SelectUsedArray`** (alias for `OptimizeSingleExprCopyArray`): Memory-optimized array of `IHqlExpression*` pointers representing field selectors used by an expression. Optimized for the common case of a single item (avoids heap allocation).

## 2. Key Classes

### `UsedFieldSet`
The set of fields from a record that are required. Key operations:
- `addUnique(field)`: Add a field to the used set (idempotent)
- `addNested(field)`: Add/find a nested record field, creating tracking structure
- `setAll()`: Mark that all fields are required (no optimization possible for this set)
- `unionFields(source)`: Add all fields from another set into this one
- `intersectFields(source)`: Keep only fields present in both sets
- `createDifference(left, right)`: Compute left − right
- `calcFinalRecord(canPack, ignoreIfEmpty, disallowEmpty)`: Synthesize the reduced record expression
- `createFilteredTransform(transform, exceptions)`: Create a new transform retaining only assignments for fields in this set
- `gatherTransformValuesUsed(...)`: Walk a transform to discover which parent/source fields the used output fields depend on
- `gatherExpandSelectsUsed(...)`: For pass-through cases, expand which source fields map to used output fields
- `requiresFewerFields(other)`: Returns true if this set is strictly smaller than another

### `NestedField`
Tracks sub-field usage for a `type_row` field within a record. Allows the pass to project nested records independently of their parent.

### `ImplicitProjectInfo` (extends `NewTransformInfo`)
Per-expression annotation attached during the transform pass. Stores:
- `kind` (`ProjectExprKind`): Classification of the expression's role
- `selectsUsed` (`SelectUsedArray`): Accumulated field-selector references used by this expression and its non-activity children
- `visited`, `gatheredSelectsUsed`: Memoization flags
- `canOptimize`, `insertProject`, `alreadyInScope`, `canReorderOutput`, etc.: Control flags

Key methods:
- `addActiveSelect(select)`: Register a field access
- `addActiveSelects(src)`: Bulk register
- `removeScopedFields(selector)`: Remove references that belong to a scoped (local) dataset
- `removeRowsFields(expr, left, right)`: Remove ROWS(LEFT/RIGHT) references

### `ComplexImplicitProjectInfo` (extends `ImplicitProjectInfo`)
Extended annotation for activity nodes and records. Stores:
- `inputs` / `outputs` (`ProjectInfoArray`): Connected predecessor/successor activities in the dataflow graph
- `outputFields` (`UsedFieldSet`): Fields required in this activity's output
- `leftFieldsRequired` / `rightFieldsRequired` (`UsedFieldSet`): Fields required from left/right inputs
- `fieldsToBlank` (`UsedFieldSet`): For iterate/rollup/denormalize — fields present in the record but not needed, which get blanked

Key methods:
- `addAllOutputs()`: Mark all output fields as required (disables optimization)
- `createOutputProject(ds)`: Synthesize a `no_hqlproject` or `no_projectrow` that trims the record
- `finalizeOutputRecord(disallowEmpty)`: Compute the reduced record from `outputFields`
- `inheritRequiredFields(requiredList)`: Union demanded fields from a consumer
- `notifyRequiredFields(whichInput)`: Push required fields to an input based on activity semantics
- `safeToReorderOutput()` / `safeToReorderInput()`: Determine if field reordering (packing) is safe
- `stopOptimizeCompound(cascade)`: Prevent compound-source optimization

### `ImplicitProjectTransformer` (extends `NewHqlTransformer`)
The main transformer class driving the entire pass. Orchestrates:
1. Analysis (graph walking and classification)
2. Field percolation
3. Project insertion decisions
4. Record finalization
5. Graph transformation (creating the new expression tree)

### `ProjectExprKind` (enum)
Classifies each expression into one of ~16 categories that determine how field requirements propagate:
- `NonActivity`: Not a dataset activity
- `CreateRecordActivity`: Produces a new record (PROJECT, AGGREGATE, etc.) — can freely reduce output
- `CreateRecordLRActivity`: JOIN, FETCH — has left+right inputs, creates new record
- `CompoundActivity`: Compound source wrapper — project insertion is free
- `CompoundableActivity`: Source that can become compound (DISK READ, INDEX READ)
- `FixedInputActivity`: Cannot change input format (PIPE, OUTPUT, LOOP, COMBINE, etc.)
- `SourceActivity`: No inputs (WORKUNIT_DATASET, etc.)
- `SimpleActivity`: Filter, sort, etc. — passes through fields, may benefit from preceding project
- `PassThroughActivity`: IF, MERGE, NONEMPTY — input record = output record
- `ScalarSelectActivity`: ds[n].field
- `DenormalizeActivity`: DENORMALIZE — left must match output, special transform semantics
- `RollupTransformActivity`: ROLLUP — input = output, LEFT must be in both
- `IterateTransformActivity`: ITERATE — input = output, LEFT must be in output
- `SinkActivity`: OUTPUT, APPLY, extractresult
- `CreateRecordSourceActivity`: INLINE TABLE, DATASET_FROM_TRANSFORM
- `CreateNonEmptyRecordSourceActivity`: Projectable CALL with embed body
- `AnyTypeActivity`: no_null, no_skip — can adopt any record type

### `ImplicitProjectOptions`
Configuration struct read from `HqlCppOptions`:
- `insertProjectCostLevel`: Threshold for inserting explicit projects (network cost must exceed this)
- `notifyOptimizedProjects`: Logging level
- `isRoxie`: Target engine affects cost calculations
- `optimizeProjectsPreservePersists`: Don't project persisted datasets
- `autoPackRecords`: Allow record field reordering for better packing
- `optimizeSpills`: Insert projects before spills
- `enableCompoundCsvRead`: Treat CSV reads as compoundable
- `projectNestedTables`: Track individual nested-table fields (vs. whole nested table)

## 3. Algorithm

The pass executes in five sequential phases within `ImplicitProjectTransformer::process()`:

### Phase 1: Analysis (`analyse` / `analyseExpr`)
**Direction: Top-down (recursive descent)**

Walks the expression graph depth-first. For each expression:
1. Classifies it via `getProjectExprKind()` into a `ProjectExprKind`
2. For activity nodes: registers them in the `activities` array, connects inputs↔outputs via `connect()`, and sets up `originalFields` relationships
3. For non-activity sub-expressions: calls `gatherFieldsUsed()` to collect field selector references (`no_select` nodes) into `selectsUsed`

Activities are appended to `activities` in **depth-first post-order** — meaning iterating backwards guarantees top-down (consumer-first) traversal.

The `gatherFieldsUsed()` method handles scoping: it inherits selectors from children, then removes those that are locally scoped (LEFT, RIGHT, dataset-scope selectors). What remains are selectors referencing parent datasets — these propagate upward.

### Phase 2: Field Percolation (`percolateFields` → `calculateFieldsUsed`)
**Direction: Top-down (from consumers to producers)**

Iterates `activities` in **reverse** order (which is top-down since they were stored bottom-up). For each activity, calls `calculateFieldsUsed()` which:

1. Asks all consumers what fields they need via `notifyRequiredFields()` → `inheritRequiredFields()`, building up `outputFields`
2. Based on activity kind, determines which output fields require which input fields:
   - **CreateRecord**: Walks the transform — for each output field needed, finds the assignment, discovers which LEFT/RIGHT/DS fields it references
   - **PassThrough**: Input requirements = output requirements
   - **SimpleActivity**: Input = output + fields used in filter/sort expressions
   - **Rollup/Iterate**: Complex bidirectional — output fields must also be in input; LEFT fields must be in output
   - **Denormalize**: LEFT references in transform must appear in both input and output
   - **Compound/Compoundable**: Output record is directly trimmed
   - **FixedInput**: Forces all fields (no optimization)
   - **Source**: Forces all output fields (nothing to optimize upstream)

### Phase 3: Project Insertion (`insertProjects`) — Thor only
**Direction: Forward through activities**

For Thor cluster target, when `insertProjectCostLevel > 0` or `optimizeSpills` is set:
- For `SimpleActivity` nodes where the input has more fields than required: marks `insertProject = true`
- For `no_commonspill`: inserts project if spill carries excess fields

This phase inserts explicit PROJECT activities before expensive operations (sorts, distributes) to reduce network traffic.

### Phase 4: Finalization (`finalizeFields`)
**Direction: Forward through activities**

For each activity, synthesizes the actual reduced record expression:
- **CreateRecord/Compound/Source**: Calls `finalizeOutputRecord()`
- **Rollup/Iterate/Denormalize**: Computes `fieldsToBlank` (fields in input but not needed in output), unions them back into outputFields, creates the finalized record
- **PassThrough**: Intersects all input records to find common minimum, finalizes
- **SimpleActivity**: If `insertProject` is set and fewer fields needed, sets output to reduced set; otherwise matches input

### Phase 5: Transformation (`transformRoot` → `createTransformed`)
**Direction: Bottom-up (standard NewHqlTransformer)**

Rebuilds the expression graph with reduced records:
- **CreateRecord**: Filters the transform to only retain assignments for needed fields
- **Compound/Compoundable**: Wraps source in a synthetic project node
- **PassThrough**: Inserts projects on branches that don't match the output record
- **SimpleActivity**: Inserts project before the activity if flagged
- **Rollup/Iterate/Denormalize**: Filters transform, blanking unneeded fields
- **AnyTypeActivity**: Replaces record argument directly
- Updates selectors throughout to match new record structures

## 4. Activity-Specific Handling

### Joins (`CreateRecordLRActivity`)
- Left and right inputs tracked independently via `leftFieldsRequired` / `rightFieldsRequired`
- Transform is analyzed: each output field's assignment is traced to discover which LEFT/RIGHT fields it uses
- Self-joins create a RIGHT selector from the same dataset
- `processTransform` handles SKIP expressions specially (forces output field inclusion)
- `onFail` transforms are also filtered to match the reduced record

### Projects / Aggregates / Normalize (`CreateRecordActivity`)
- Pure record creation — freely removes any output field not demanded downstream
- For `no_newaggregate`: ensures grouping criteria fields are always included in the output
- For `no_hqlproject` / `no_newusertable`: cascades `stopOptimizeCompound(false)` to prevent the input from becoming compound (since a project already exists)
- `_countProject_` attribute removed if counter is no longer referenced after transform filtering

### Sorts / Filters / Group (`SimpleActivity`)
- Input must include all fields needed by the output PLUS fields referenced in sort/filter/group expressions
- Fields used in DEDUP criteria (via LEFT/RIGHT selectors) are tracked
- An explicit project may be inserted before expensive operations if configured

### Disk Reads / Index Reads (`CompoundableActivity`)
- Treated as sources whose output record can be trimmed
- After finalization, wraps in compound read with projected record
- `stopOptimizeCompound` prevents input stripping when a downstream project already exists
- Persisted datasets optionally excluded from optimization

### Compound Sources (`CompoundActivity`)
- Already-compound sources (e.g., `no_compound_diskread`)
- A project is inserted inside the compound wrapper
- Internally marks `insertProject = true`

### Rollup (`RollupTransformActivity`)
- Input record = output record (constraint)
- Fields in LEFT must appear in BOTH input and output
- Fields in RIGHT must appear in output (can be blanked)
- Iterative expansion: as more output fields are discovered needed, their dependencies on input fields are propagated, which may trigger more output fields, until fixed-point
- `fieldsToBlank`: fields in the record that are NOT needed in output get assigned null values in the transform instead of being removed entirely

### Iterate (`IterateTransformActivity`)
- Same constraint as rollup: input = output
- Fields used from LEFT must be in the output
- Same iterative fixed-point expansion

### Denormalize (`DenormalizeActivity`)
- Left input record = output record (constraint)
- Transform may be called multiple times
- Any field used from LEFT in the transform must be in BOTH input and output
- Fields needed from output must also be in the left input (but could be blanked)
- Right input tracked separately

### Pass-Through (IF, MERGE, ADDFILES, etc.)
- All branches must have the same record format
- Output record is the intersection of all input branch records
- For MERGE: sort-order fields are forced into the output set
- Projects are inserted on branches that differ from the final output record

### Sinks (OUTPUT, APPLY, extractresult)
- `leftFieldsRequired` is computed from fields referenced in the activity
- Output is not modified (there's nothing downstream)

### Fixed Input (PIPE, LOOP, COMBINE, etc.)
- Forces `leftFieldsRequired.setAll()` and `rightFieldsRequired.setAllIfAny()`
- No optimization is performed

## 5. Field Propagation Rules

### Through Transforms
The pass uses `gatherTransformValuesUsed()` to walk a transform and, for each output field that is needed:
1. Finds the assignment (`SELF.field := value`)
2. Extracts the RHS value expression
3. Collects all field selectors from that value
4. Maps those selectors to LEFT, RIGHT, or dataset inputs

For nested record fields (`type_row`):
- If the value is `no_createrow`: recurses into the sub-transform
- If the value is a selector or `isAlwaysActiveRow`: uses `gatherExpandSelectsUsed` to map sub-fields
- Otherwise: marks the entire nested field as required (conservative)

### LEFT/RIGHT Tracking
- LEFT and RIGHT selectors are created from the input dataset + sequence ID
- `processMatchingSelector()` checks if a field access matches a selector and adds it to the appropriate `UsedFieldSet`
- Scoped selectors are removed after processing their scope (prevents leaking into parent)
- `removeRowsFields()` handles ROWS(LEFT) / ROWS(RIGHT) references

### SKIP Handling in Transforms
When a transform value contains SKIP:
- The output field must be forced into the output (it controls flow)
- Fields referenced in the SKIP condition are added to input requirements

### Iterative Convergence (Rollup/Iterate)
Because output fields can depend on LEFT fields which must also be in the output, the pass loops:
```
while (!outputFields.allGathered()):
    gather values for new output fields from transform
    add LEFT references to outputFields
    if all fields selected → break
```

## 6. Limitations and Gaps

### Explicitly Documented Limitations (MORE comments)

1. **`UsedFieldSet` immutability not enforced**: Once `all` is set, the structure should be immutable and shareable via link-counting. Currently it's cloned unnecessarily, wasting memory.

2. **`addUnique` missing short-circuit**: Should skip the `contains()` check when `all` is already set.

3. **`addActiveSelects` is O(N²)**: When merging selector lists, each addition checks for pre-existence against the entire existing list. Should only check against the pre-existing portion.

4. **`isSensibleRecord` not cached**: Called repeatedly for the same record; result should be memoized.

5. **`querySelectsUsedForField` could use a map**: Linear search through transform assignments. "Only ~1% of time" per comment, but could matter on very large transforms.

6. **`optimizeFieldsToBlank` is unimplemented**: The function body is empty with a TODO comment. It should optimize the blanking list by keeping fields that are cheaper to pass-through than to blank (e.g., fixed-length fields adjacent to used fields).

7. **Parent dataset projection not supported**: Selects from parent datasets (in nested contexts) don't project down the parent. Could reduce parent-scope data flow.

8. **`no_aggregate` with `mergeTransformAtom` treated as FixedInput**: Comment says "Should be able to optimize this" but it's not implemented.

9. **Nested dataset field count/size attributes**: Records with embedded datasets that have `count` or `size` attributes are excluded from optimization due to field translation limitations.

10. **Default values for nested record fields**: When creating reduced nested records, defaults are stripped because they'd have the wrong type. Ideally they'd be projected.

### Structural Gaps

11. **`FixedInputActivity` is overly broad**: The following are all treated as `FixedInputActivity` (no optimization at all):
    - `no_combine`, `no_combinegroup`, `no_regroup`
    - `no_loop`, `no_graphloop`
    - `no_filtergroup`, `no_normalizegroup`
    - `no_output`, `no_buildindex`
    - `no_serialize`, `no_deserialize`
    
    Many of these COULD have their inputs projected. For example, `no_output` to a file only needs the fields being written; `no_loop` bodies could theoretically be analyzed. The comment "MORE: Rethink these later" confirms this is known.

12. **`no_nwayjoin` and `no_nwaymerge` disable all input optimization**: All inputs are walked with `allowActivity = false`, preventing any projection. This is conservative but potentially leaves significant data volume unreduced for n-way operations.

13. **`no_process` unconditionally disabled**: Classified as `PassThroughActivity` but immediately `preventOptimization()` is called.

14. **DEDUP with whole-record match**: When `dedupMatchesWholeRecord(expr)` is true, optimization is prevented entirely. A more nuanced approach could still remove fields not in the output requirements (only ensuring the comparison includes all fields).

15. **Dictionaries**: All dictionary-typed expressions are treated as `FixedInputActivity` — no field projection is attempted.

16. **SOAP/HTTP calls**: These are `SourceActivity` — no projection of their output is attempted even when the call produces a well-defined record.

17. **`no_fromxml` / `no_fromjson`**: Treated as `SourceActivity` despite having inputs. No optimization of what they produce.

18. **IF with `_resourced_Atom`**: Resourced IF expressions are forced to all fields. This is necessary for correctness but means the resourcing pass can prevent optimization.

19. **`projectNestedTables` is a separate option (default false)**: When disabled (the default), nested table fields are tracked only at the top level — individual sub-fields within nested records are not independently projected. This is a major gap for records with large nested structures.

20. **No field projection through SORT/GROUP expressions**: While fields used in sort/group criteria are correctly added to input requirements, there's no attempt to project away fields that are only in the sort key but not needed downstream after the sort completes. (This is inherent to the design — sort doesn't change the record.)

21. **Self-referencing field lengths**: Records containing fields whose length depends on other fields in the same record (`no_selfref` counts/lengths) are excluded entirely from optimization.

### Algorithmic Gaps

22. **Single-pass percolation**: The algorithm makes exactly ONE reverse pass through activities. For complex graphs with multiple consumers, requirements are unioned. However, there's no iterative refinement — if a consumer's requirements change based on what its inputs provide, that isn't re-propagated.

23. **No cost-benefit analysis for record reduction**: The pass removes fields whenever possible, without considering whether removing a single small field from a large record is worth the overhead of inserting a project activity. The `insertProjectCostLevel` only applies to explicit project insertion before expensive activities — not to the record trimming itself.

24. **No tracking of field sizes**: The pass counts fields but doesn't consider their sizes. Removing one VARCHAR(1000) field would be far more beneficial than removing ten INTEGER4 fields, but the pass treats them equally.

25. **Compound activity interaction**: When a `CompoundableActivity` has a downstream `no_hqlproject` or `no_newusertable`, `stopOptimizeCompound(false)` is called. This prevents the compound source from also projecting, which could be overly conservative when the downstream project further reduces the record.

## 7. Interaction with Other Passes

### Invocation Points

The implicit project pass runs at **three** points in the compilation pipeline:

1. **Global projects** (`optimizeGlobalProjects` flag): Runs early, before resourcing, on the entire workflow. Operates at the logical level.

2. **Pre-resource projects** (in `getResourcedGraph`): Runs after compound source detection but before the general optimizer, on individual graph expressions. Helps the optimizer make better decisions about row sizes.

3. **Post-resource projects** (in `optimizeGraphPostResource`): Runs AFTER resourcing and compound source creation, with `optimizeSpills` potentially true. This is the most impactful invocation — it operates on the final activity graph and can insert projects before spills and network transfers.

### Relationship with the Optimizer (`hqlopt.cpp`)

- **Feeds into**: After implicit projects remove fields, the optimizer (`optimizeHqlExpression`) runs with `HOOcompoundproject` flag. It can merge the inserted projects into compound disk/index reads, eliminating the project as a separate activity.
- **Interacts with compound source detection**: `optimizeCompoundSource` runs both before and after implicit projects. The pass's `CompoundableActivity` classification specifically enables integration with compound reads.
- **Project over limit**: The post-project optimizer specifically moves projects over LIMIT operations, further reducing data flow.

### Relationship with Constant Folding

- The pass doesn't directly invoke constant folding, but by removing unused fields it can:
  - Eliminate transform assignments that referenced expensive expressions
  - Enable the subsequent optimizer pass to fold constants in simplified transforms
  - Reduce the scope for `hasSideEffects` to block optimization

### Relationship with Resourcing (`hqlresource.cpp`)

- The resourcing pass determines activity boundaries and spill points
- Implicit projects running post-resource can insert projects before spills to reduce spill sizes
- `_resourced_Atom` on IF expressions prevents the project pass from modifying already-resourced conditionals

## 8. Performance Characteristics

### Time Complexity

- **Analysis phase**: O(N) in expression graph nodes — single depth-first walk
- **Field gathering**: Each expression's selectors are gathered once (memoized via `gatheredSelectsUsed` flag). However, `addActiveSelects` has O(N²) behavior per comment
- **Percolation**: O(A) where A = number of activities. For each activity, operations are proportional to the number of output fields × transform complexity
- **Finalization**: O(A), with set operations (union, intersection) proportional to field counts
- **Transformation**: O(N) — standard NewHqlTransformer tree rebuild

### Space Complexity

- **Per-expression storage**: Every expression node gets an `ImplicitProjectInfo` (small) or `ComplexImplicitProjectInfo` (larger, for activities and records). The `ImplicitProjectInfo` is carefully packed with bit-fields to minimize size.
- **`SelectUsedArray` optimization**: Uses `OptimizeSingleExprCopyArray` which avoids heap allocation for the common single-selector case.
- **Activity list**: Stores one entry per activity node (could be 10K+ for large queries)

### Known Scalability Concerns

1. **`addActiveSelects` O(N²)**: For expressions with many unique field references being merged into large existing lists, this can become a bottleneck on wide records.

2. **`UsedFieldSet::contains` linear scan** (unless `USE_IPROJECT_HASH` is defined): For records with many fields (100+), every `addUnique` and `processMatchingSelector` call does a linear search. The `USE_IPROJECT_HASH` compile option exists but is not enabled by default.

3. **Field ordering (`RecordOrderComparer`)**: Uses `qsortvec` with `getOriginalPosition` which is itself a linear scan. For records with many fields, this sort during `calcFinalRecord` could be O(F² log F).

4. **Iterative convergence in rollup/iterate**: The while loop in rollup processing can theoretically iterate O(F) times (one field discovered per iteration), though in practice it converges quickly.

5. **Triple invocation**: The pass runs up to three times on the same expression graph (global, pre-resource, post-resource). Each invocation is independent with no caching of results. For very large queries this triples the cost.

6. **`isSensibleRecord` not cached**: Called for every activity's record, doing a recursive walk each time. Should be memoized in the record's extra info.

---

## Summary of Optimization Opportunities

| Priority | Gap | Potential Impact |
|----------|-----|-----------------|
| High | `FixedInputActivity` too broad (LOOP, COMBINE, OUTPUT, etc.) | Major for Thor queries with loops |
| High | `no_aggregate` with merge transform not optimized | Reduces distributed aggregate traffic |
| High | `optimizeFieldsToBlank` unimplemented | Better code for rollup/iterate |
| Medium | No field-size awareness | Would prioritize removing large fields |
| Medium | `projectNestedTables` defaults to false | Misses nested record projection |
| Medium | N-way join/merge inputs not projected | Wide records through n-way ops |
| Medium | `addActiveSelects` O(N²) | Compile-time for wide records |
| Medium | `USE_IPROJECT_HASH` not default | Compile-time for wide records |
| Low | `UsedFieldSet` immutability sharing | Memory for very large graphs |
| Low | `isSensibleRecord` not cached | Minor compile-time |
| Low | Triple invocation without caching | Compile-time for large queries |
