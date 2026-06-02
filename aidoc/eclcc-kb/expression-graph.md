# Expression Graph

> Last verified: 2026-05

## Overview
The expression graph is the central data structure in eclcc. Nearly all processing — parsing, optimization, and code generation — operates on this graph.

## Key Invariants
1. **Immutability**: Once created, expression nodes are never modified. Derived information (sort order, row counts, unique hash) may be lazily computed and cached, but does not change semantics.
2. **Commoning**: If two nodes have the same operator, arguments, and type, they are represented by a single `IHqlExpression` instance. This ensures the structure remains a DAG (not a tree), enables sharing-aware optimizations, and reduces memory.
3. **Link Counting**: Reference counting controls node lifetime. Increment on hold, decrement on release, free at zero. Forward references can cause complications.

## Node Classes
The implementation uses a class hierarchy optimized for **memory**, not functionality:
- Removing a single pointer per node can save 1 GB for very complex queries.
- Classes model different expression categories (dataset, scalar, scope) rather than individual operator kinds.
- Over 500 operator kinds exist in `node_operator`; polymorphism per-operator would be unwieldy.

### Source Files
- Interface: `ecl/hql/hqlexpr.hpp`
- Implementation: `ecl/hql/hqlexpr.cpp`
- Internal details: `ecl/hql/hqlexpr.ipp`

## IHqlExpression Interface (Key Methods)
| Method | Purpose |
|--------|---------|
| `getOperator()` | Returns the `node_operator` enum value |
| `numChildren()` | Number of arguments |
| `queryChild(n)` | Nth child (NULL if out of range) |
| `queryType()` | Type of this node |
| `queryBody()` | Skip annotations to the underlying expression |
| `queryProperty(atom)` | Check for a named attribute child |
| `queryValue()` | For `no_constant`, return the value |
| `queryDataset()` | Access IHqlDataset interface |
| `queryScope()` | Access IHqlScope interface |
| `gatherTablesUsed()` | Which datasets does this expression reference? |
| `isIndependentOfScope()` | Quick check: no dataset references? |
| `usesSelector(sel)` | Does this expression reference a specific selector? |

## Factory Functions
- `createValue(op, type, ...)` — general scalar/action expressions
- `createDataset(op, ...)` — dataset expressions
- `createRow(op, ...)` — single-row expressions
- `createDictionary(op, ...)` — dictionary expressions
- `createSelectExpr(selector, field)` — field selection (`no_select`)
- `createExtraAttribute(name, ...)` — attribute with optional args (default choice)
- `createAttribute(name)` — attribute guaranteed to have no args / never transformed

**Ownership**: Arguments passed to `createX()` have ownership transferred to the new node.

## Annotations
Annotations attach metadata (symbol names, source positions) to expressions without changing their meaning.

- `getAnnotationKind()` — what kind of annotation
- `queryAnnotation()` — access annotation details
- `queryBody()` — strips all annotations, returning the underlying expression

### Critical Rule for Transforms
When transforming, if `expr->queryBody() != expr`, the expression is annotated. The transformer must:
1. Transform the body (`expr->queryBody()`)
2. Clone the annotation onto the transformed body

Failure to do this can break commoning (turning a graph into a tree).

## Field References (`no_select`)
A field reference is always `no_select(selector, field)`.

### Selector Forms
1. **LEFT/RIGHT**: `no_left(record, selSeq)` or `no_right(record, selSeq)`. The `selSeq` disambiguates nested uses.
2. **Active dataset**: The input dataset (or any upstream dataset up to the nearest table) used directly as selector.
3. **Implicit**: Parser expands bare field names to form (2).

### The "new" Attribute
- A `no_select` with a `"new"` attribute child means the dataset must be **created** (iterated).
- Without `"new"`, it references an **active cursor**.
- Added during normalization (not present in parser output).
- For nested rows, "new" is on the outermost dataset selection.

### Normalized Selectors
After normalization, the selector in a field reference is `inputDataset->queryNormalizedSelector()` (currently the table node). This means table-modifying transforms force selector changes, limiting short-circuiting.

## Derived Properties (Caching)
| Mechanism | Access | Examples |
|-----------|--------|----------|
| Boolean flags | `getInfoFlags()` / `getInfoFlags2()` | is constant, has side-effects, references datasets |
| Table usage | `gatherTablesUsed()` | which datasets are referenced |
| Type-stored | `isGrouped(expr)`, sort order | grouping, distribution, ordering |
| Arbitrary cached | `hqlattr.cpp` | row count, max record size, location-independent representation |
| Helper functions | various in `hqlutil.hpp` | `queryOriginalRecord()`, `hasUnknownTransform()` |

## Memory Optimization Notes
- Node count can reach 10M–100M for production queries.
- Each extra pointer per node = ~80MB–800MB additional memory.
- The class hierarchy exists purely to reduce per-node overhead.
- `closeExpr()` finalizes staged nodes (e.g., records, modules) — no modification after this call.
