# Transformations

> Last verified: 2026-05

## Overview
Expression graph transformations are the primary mechanism for optimization and code preparation in eclcc. A transformation walks the graph and either gathers information, produces a modified graph, or both.

## Core Principles

### Walk as a Graph, Not a Tree
The expression graph is a DAG. If you revisit already-processed nodes, execution time becomes exponential with depth. Every transformer must cache results for previously seen nodes.

### Preserve Sharing
If a node is unchanged by transformation, return a link to the original — never create a duplicate. Creating unnecessary copies breaks commoning and bloats memory.

### Annotation Safety
An expression used in different contexts may have different annotations (e.g., two different symbol names):
```
A := x;  B := x;  C := A + B;
```
If the body `x` is transformed differently depending on annotation context, the graph becomes a tree:
```
A' := x'; B' := x''; C' := A' + B';  // WRONG — x split into x' and x''
```
**Rule**: Check `expr->queryBody() == expr`. If not equal, transform the body first, then clone annotations onto the result.

### Conditional Context Preservation  
When moving an expression to another position in the graph, verify:
- Was the original context conditional?
- Is the new context unconditional?
Moving a conditionally-evaluated expression to an unconditional position can cause incorrect evaluation.

### Context-Dependent Meaning
References to active datasets can be ambiguous. The meaning of a field reference depends on which datasets are active in scope. Transforms must preserve or correctly update this context.

## Base Classes

### NewHqlTransformer (`ecl/hql/hqltrans.ipp`)
The primary base class for graph transformations. Provides:
- Node caching (avoid re-processing)
- Annotation handling
- Child-node recursion
- Selector mapping for table changes

### QuickHqlTransformer
Lightweight variant for simpler transforms that don't need full table-selector tracking.

### MergingHqlTransformer
Variant that handles merging multiple graphs or combining operations.

## Common Transform Patterns

### Information Gathering
Walk the graph collecting data without modification:
- Which datasets are used?
- Is this expression constant?
- What fields are referenced?
- Are there side-effects?

### Expression Simplification
Reduce complexity while preserving semantics:
- Constant folding
- Dead code elimination
- Redundant operation removal

### Expression Reordering
Change the order of operations for better performance:
- Move filters before sorts
- Move filters before projects
- Combine adjacent compatible operations

### Graph Restructuring
Change the shape of the graph:
- Split compound operations
- Merge simple operations into compounds
- Move subexpressions to different evaluation contexts (global vs local vs child query)

## Specific Transforms (in approximate order)

| Transform | File | Purpose |
|-----------|------|---------|
| Normalization | `ecl/hql/hqlgram2.cpp` | Structural cleanup of parser output |
| Scope checking | (multiple) | Validate field references, add "new" attributes |
| Constant folding | `ecl/hql/hqlfold.cpp` | Evaluate constants, simplify conditionals |
| Expression optimizer | `ecl/hql/hqlopt.cpp` | Reorder/combine dataset operations |
| Implicit project | `ecl/hqlcpp/hqliproj.cpp` | Remove unused fields |
| CSE | `ecl/hqlcpp/hqlcse.cpp` | Common sub-expression elimination |
| Filter hoisting | `ecl/hqlcpp/hqlhoist.cpp` | Move filters earlier |
| Resource allocation | `ecl/hqlcpp/hqlresource.cpp` | Assign activities to engines |
| Thor/Roxie specific | `ecl/hqlcpp/hqlttcpp.cpp` | Engine-specific transformations |

## Debugging Transforms
- Use `--logdetail 999` to see the expression tree after each transformation stage.
- Add `dbglogExpr(expr)` or `EclIR::dbglogIR(expr)` at strategic points.
- Use `DEBUG_TRACK_INSTANCEID` to trace node creation/destruction.
- Compare pre/post transform output to identify which pass introduces issues.

## Performance Considerations
- Large queries (10M+ nodes) make transformation cost significant.
- Hash-based node lookup for caching is critical.
- Minimize allocation during transformation — reuse containers where possible.
- Short-circuit: if no changes are needed in a subtree, don't recurse into it (but beware of selector changes that force recursion).
