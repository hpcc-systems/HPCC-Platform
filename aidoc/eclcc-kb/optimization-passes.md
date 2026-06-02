# Optimization Passes

> Last verified: 2026-05

## Overview
eclcc contains multiple optimization stages that progressively simplify and improve the expression graph before code generation. The order matters — earlier passes create opportunities for later ones.

## Pass Ordering (Approximate)
1. Normalization (structural cleanup)
2. Scope checking
3. Constant folding (`foldHqlExpression`)
4. Expression optimization (`optimizeHqlExpression`)
5. Implicit project (`insertImplicitProjects`)
6. Common sub-expression elimination
7. Filter hoisting
8. Compound activity formation
9. Resource allocation
10. Per-activity optimization and generation

## Constant Folding (`ecl/hql/hqlfold.cpp`)

### Purpose
Simplify the expression tree by evaluating constant expressions and eliminating dead paths.

### Key Transformations
- Arithmetic on constants: `1 + 2 → 3`
- Conditional elimination: `IF(true, x, y) → x`
- Empty dataset shortcuts: `COUNT(<empty>) → 0`
- Conditional comparison folding: `IF(a=b, 'c', 'd') = 'd' → a != b`
- Sort/project/filter simplification on empty datasets

### Constant Percolation
When enabled, if a PROJECT assigns a constant to a field, that constant can be substituted wherever the field is subsequently referenced. This creates further folding and field-removal opportunities.

### Source Files
- `ecl/hql/hqlfold.cpp` — main implementation
- `ecl/hql/hqlfold.hpp` — public interface
- `ecl/hql/hqlfold.ipp` — internal details

## Expression Optimizer (`ecl/hql/hqlopt.cpp`)

### Purpose
Simplify, combine, and reorder dataset expressions while being sharing-aware.

### Key Design Constraint
The optimizer counts how many times each expression is used. Transformations that would cause duplication of shared nodes are suppressed. Example: swapping a filter past a sort is good, but not if two filters share the same sort (it would duplicate the sort).

### Key Transformations
- Remove redundant operations: `COUNT(SORT(x)) → COUNT(x)`
- Move filters before projects, joins, sorts
- Combine adjacent projects; combine projects with joins
- Remove redundant sorts/distributes
- Move filters from JOINs to their inputs
- Combine activities: `CHOOSEN(SORT(x)) → TOPN(x)`
- Move filters into IFs (sometimes)
- Expand field selections from single-row datasets
- Compound disk reads (combine filters + projects into single I/O)

### Optimization Flags (`hqlopt.hpp`)
| Flag | Hex | Meaning |
|------|-----|---------|
| `HOOfold` | 0x0001 | Enable folding |
| `HOOcompoundproject` | 0x0002 | Merge adjacent projects |
| `HOOnoclonelimit` | 0x0004 | Don't clone LIMIT |
| `HOOnocloneindexlimit` | 0x0008 | Don't clone index LIMIT |
| `HOOinsidecompound` | 0x0010 | Currently inside compound activity |
| `HOOfiltersharedproject` | 0x0020 | Filter shared projects |
| `HOOhascompoundaggregate` | 0x0040 | Has compound aggregate |
| `HOOfoldconstantdatasets` | 0x0080 | Fold constant datasets |
| `HOOalwayslocal` | 0x0100 | Always local context |
| `HOOexpensive` | 0x0200 | Include expensive optimizations |
| `HOOexpandselectcreaterow` | 0x0400 | Expand select from createrow |
| `HOOminimizeNetworkAndMemory` | 0x0800 | Minimize network/memory usage |

### Source Files
- `ecl/hql/hqlopt.cpp` — main optimizer implementation
- `ecl/hql/hqlopt.hpp` — public interface and flags
- `ecl/hql/hqlopt.ipp` — internal classes

## Implicit Project (`ecl/hqlcpp/hqliproj.cpp`)

### Purpose
Track data flow at each point in the graph and remove fields that are never consumed downstream.

### Interaction with Other Passes
- Constant percolation removes the need for some fields → implicit project removes them.
- Removing fields can allow LEFT OUTER JOINs to be converted to simpler PROJECTs.

### Source Files
- `ecl/hqlcpp/hqliproj.cpp` — implementation
- `ecl/hqlcpp/hqliproj.hpp` — public interface
- `ecl/hqlcpp/hqliproj.ipp` — internal classes

## Common Sub-Expression Elimination (`ecl/hqlcpp/hqlcse.cpp`)

### Purpose
Identify expressions computed multiple times and arrange for single evaluation with result reuse.

### Source Files
- `ecl/hqlcpp/hqlcse.cpp` — implementation
- `ecl/hqlcpp/hqlcse.ipp` — internal classes

## Filter Hoisting (`ecl/hqlcpp/hqlhoist.cpp`)

### Purpose
Move filter conditions earlier in the pipeline to reduce the volume of rows processed by downstream activities.

### Source Files
- `ecl/hqlcpp/hqlhoist.cpp` — implementation
- `ecl/hqlcpp/hqlhoist.hpp` — public interface

## Resource Optimization (`ecl/hqlcpp/hqlresource.cpp`)

### Purpose
Determine how activities are allocated across engines, handle graph splitting, and manage resource constraints.

### Source Files
- `ecl/hqlcpp/hqlresource.cpp` — implementation
- `ecl/hqlcpp/hqlresource.hpp` — public interface
- `ecl/hqlcpp/hqlresource.ipp` — internal classes

## Compound Activity Formation

### Purpose
Merge adjacent simple activities (filter + project + disk read) into compound activities that can be executed as a single engine operation, reducing activity-graph overhead.

### Key Pattern
```
FILTER(PROJECT(DISK_READ)) → COMPOUND_DISK_READ(filter, project)
```

## Potential Optimization Opportunities
See [optimizations.md](optimizations.md) for tracked backlog of improvement ideas.
