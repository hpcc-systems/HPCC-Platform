# eclcc Knowledge Base

> **Note**: This file is maintained by engineers and the `eclcc` AI agent. It tracks critical facts, high-level architecture, performance bottlenecks, and internal mechanics for the ECL compiler (eclcc).

---

## 1. System Priorities
eclcc is the ECL-to-C++ compiler for the HPCC Platform. Its primary engineering goals are:

- **Correctness**: Incorrect code generation produces corrupt data and invalid results that are extremely difficult for users to diagnose.
- **Optimization quality**: Generate output that is as efficient as possible through multiple optimization stages.
- **Scalability**: Handle multi-megabyte ECL inputs, tens of thousands of activities, and tens of megabytes of generated C++ within reasonable time and memory.

## 2. Key Architectural Principles
- **Expression Graph Immutability**: Once nodes are created they are NEVER modified. Modified graphs require creating new graphs (sharing nodes from the old).
- **Node Commoning**: Identical nodes (same operator, arguments, type) are ALWAYS unified into a single `IHqlExpression`. This keeps the graph compact, enables optimization, and deduplicates code.
- **Link Counting**: `IHqlExpression` nodes use reference counting for lifetime management. Every held reference must increment the count; forgetting to do so causes use-after-free.
- **Memory Sensitivity**: 10M–100M nodes in memory is common for large queries. A single extra pointer per node can cost gigabytes.

## 3. Compiler Pipeline (Idealized)
1. **Parse** ECL → expression graph (lexer: `hqllex.l`, grammar: `hqlgram.y`, driver: `hqlparse.cpp`)
2. **Inline expansion** of function definitions (during parsing)
3. **Normalize** expression graph (record cleanup, selector normalization, EVALUATE replacement)
4. **Scope checking** (validate field references, add "new" attributes to `no_select` nodes)
5. **Constant folding** (`hqlfold.cpp` — simplify scalar expressions, fold conditionals, percolate constants)
6. **Expression optimization** (`hqlopt.cpp` — reorder/combine dataset operations, remove redundancies)
7. **Implicit project** (`hqliproj.cpp` — track data flow, remove unused fields)
8. **Translate** logical operations → engine activities
9. **Resource** the global graph (`hqlresource.cpp` — assign activities to engines)
10. **Generate** C++ code per activity (`hqlcpp.cpp`, `hqlhtcpp.cpp`, `hqlwcpp.cpp`)

## 4. Core Source Layout
| Directory | Purpose |
|-----------|---------|
| `ecl/eclcc/` | Executable entry point and driver |
| `ecl/hql/` | Expression graph representation, parser, optimizer, transformers |
| `ecl/hqlcpp/` | C++ code generation from expression graphs to workunits |
| `common/deftype/` | Type system (`ITypeInfo` hierarchy, value representations) |
| `rtl/eclrtl/` | Runtime library called by generated C++ |
| `rtl/include/eclhelper.hpp` | Activity helper interfaces implemented by generated code |

## 5. Key Interfaces and Classes
- **IHqlExpression** (`ecl/hql/hqlexpr.hpp`): Primary interface for walking the graph. Methods: `getOperator()`, `numChildren()`, `queryChild()`, `queryType()`, `queryBody()`, `queryProperty()`.
- **node_operator** (`ecl/hql/hqlexpr.hpp`): 500+ operator kinds. Values are CRC-sensitive — never reorder.
- **IHqlScope** / **IHqlSimpleScope**: Module/record field resolution.
- **ITypeInfo** (`common/deftype/deftype.hpp`): Type hierarchy.
- **NewHqlTransformer** (`ecl/hql/hqltrans.ipp`): Base class for expression graph transformations.
- **HqlCppTranslator** (`ecl/hqlcpp/hqlcpp.hpp`): Main code generation orchestrator.
- **IErrorReceiver** (`ecl/hql/hqlerror.hpp`): Error/warning reporting interface.

## 6. Expression Graph Details
- Factory functions: `createValue()`, `createDataset()`, `createRow()`, `createDictionary()`.
- Ownership convention: arguments passed to `createX()` have their ownership transferred to the new node.
- **Annotations**: Carry metadata (symbol names, positions) without changing semantics. `queryBody()` skips annotations. Transforms must handle annotations carefully to avoid breaking commoning.
- **Attributes vs Properties**: Attributes are explicit flags on operators (`no_attr`/`no_expr_attr`). Properties are computed/cached derived information (sort order, row count, etc.).
- **Field references**: `no_select` nodes with a selector (LEFT/RIGHT or dataset) and a field. Disambiguation via `selSeq` for LEFT/RIGHT.

## 7. Transformation Rules
- Always walk the graph as a **graph**, not a tree. Never revisit already-processed nodes.
- If a node is unchanged by transformation, return a link to the original — don't create a new node.
- Check `expr->queryBody() == expr` to handle annotations correctly (transform body, then clone annotations).
- Moving expressions across conditional boundaries requires checking original vs new context.
- Field selectors change when tables change — this limits short-circuiting of transforms.

## 8. Debugging & Tracing
- `eclcc myfile.ecl --logfile myfile.log --logdetail 999` — dumps expression tree after each transformation.
- `-ftraceIR` — outputs intermediate representation (structured but verbose) instead of regenerated ECL.
- `dbglogExpr(expr)` / `EclIR::dbglogIR(expr)` — in-code tracing helpers.
- `EclIR::dump_ir(expr)` — GDB-friendly dump to stdout.
- `DEBUG_TRACK_INSTANCEID` (`hqlexpr.ipp`) — assigns unique IDs to every expression for tracking creation.

## 9. Regression Testing
- **Runtime regression**: `testing/regress/ecl`
- **Compiler regression**: `ecl/regress` (includes non-runnable tests and error tests)
- **Workflow**: Create golden reference → make changes → re-run → compare with diff tool.
- Script: `ecl/regress/regress.sh` — generates C++/workunits and compares against reference.

## 10. Detailed Documentation Index
- [Expression Creation](expression-creation.md) — Factory functions, ownership transfer, and commoning guarantees during node construction.
- [Expression Graph](expression-graph.md) — Node representation, immutability, commoning, link counting, memory layout.
- [Optimization Passes](optimization-passes.md) — Constant folding, expression optimizer, implicit project, CSE, compound activities.
- [Optimizer Deep Dive](optimizer-deep-dive.md) — Internal optimizer workflows, pass interactions, and tuning points.
- [Code Generation](code-generation.md) — Activity translation, statement building, C++ output, workunit structure.
- [Child Query Codegen](child-query-codegen.md) — Child query generation mechanics and execution model details.
- [Resource Allocator](resource-allocator.md) — Resource planning/allocation behavior for generated activity graphs.
- [Implicit Project Audit](implicit-project-audit.md) — Findings and analysis for implicit project behavior and field-flow pruning.
- [Type System](type-system.md) — ITypeInfo hierarchy, field definitions, type matching and promotion.
- [Transformations](transformations.md) — NewHqlTransformer, annotation handling, graph-walking rules.
- [Transformation Framework](transformation-framework.md) — Hot paths, transform internals, and performance opportunities in `hqltrans`.
- [Selector Mechanics](selector-mechanics.md) — Selector remapping, scope semantics, and LEFT/RIGHT sequencing behavior.
- [Optimizations Backlog](optimizations.md) — Identified opportunities for compiler improvement.
