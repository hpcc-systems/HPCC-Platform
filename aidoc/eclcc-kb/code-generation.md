# Code Generation

> Last verified: 2026-05

## Overview
The code generation phase converts the optimized expression graph into C++ source code and a workunit XML description. The generated code implements activity helper interfaces that the runtime engines (Thor, Roxie, hThor) call to execute queries.

## Output Structure

### Workunit
A workunit completely describes a generated query:
- **XML component**: workflow, execution graphs, options, input/result schemas.
- **Shared object**: compiled C++ containing helper classes/functions. The XML is often compressed and embedded as a resource within the shared object.

### Generated C++ Conventions
- **Minimal includes**: Reduces compile time. No jlib/boost/icu in public headers.
- **Thread-safe**: Activity helper members must be usable from concurrent threads.
- **Activity helper naming**: Class `cAc<N>`, factory `fAc<N>` where N is the activity ID.
- **Interfaces**: Implement `IHThorArg`-derived interfaces from `rtl/include/eclhelper.hpp`.

## Key Source Files
| File | Role |
|------|------|
| `ecl/hqlcpp/hqlcpp.cpp` | `HqlCppTranslator` — main orchestrator |
| `ecl/hqlcpp/hqlcpp.hpp` | Public interface |
| `ecl/hqlcpp/hqlcpp.ipp` | Internal classes |
| `ecl/hqlcpp/hqlhtcpp.cpp` | Activity-specific code generation (hthor/thor patterns) |
| `ecl/hqlcpp/hqlhtcpp.ipp` | Internal helpers for activity generation |
| `ecl/hqlcpp/hqlwcpp.cpp` | C++ text output writer |
| `ecl/hqlcpp/hqlwcpp.hpp` | Writer interface |
| `ecl/hqlcpp/hqlstmt.cpp` | Statement/block building |
| `ecl/hqlcpp/hqlstmt.hpp` | Statement interface |
| `ecl/hqlcpp/hqlsource.cpp` | Source activity (disk read, index read) generation |
| `ecl/hqlcpp/hqlcppc.cpp` | Serialization/deserialization code generation |
| `ecl/hqlcpp/hqlecl.cpp` | ECL regeneration from expression graph |
| `ecl/hqlcpp/hqlgraph.cpp` | Graph XML generation |
| `ecl/hqlcpp/hqlres.cpp` | Resource embedding |
| `ecl/hqlcpp/hqllib.cpp` | Library/plugin support |

## Workflow
The workunit's actions are divided into workflow items:
- Stored in `<Workflow>` XML section.
- Generated class with `perform()` method per workflow item.
- Factory function: `createProcess()`.
- Workflow items typically call back into the engine to execute a graph.

## Activity Graph XML
Each activity in the graph contains:
- Unique ID
- `ThorActivityKind` from `eclhelper.hpp`
- Original ECL source and location
- Record size, row count, sort order information
- Hints controlling execution (e.g., sort thread count)
- Post-execution: record counts and statistics

## Statement Generation (`hqlstmt.cpp`)
- Builds imperative C++ statement blocks from declarative expressions.
- Handles control flow (if/else, loops), variable declarations, assignments.
- Manages temporary variable naming and scoping.

## Declarative-to-Imperative Challenges
The core challenge is converting pure declarative ECL into imperative C++ while ensuring:
1. Code is only evaluated when required (lazy evaluation).
2. Code is only evaluated once (avoid redundant computation).

These goals conflict when a global dataset expression is used inside a child query — evaluate once before the activity (possibly wasteful) or on-demand each time (possibly inefficient)?

Current approach: mostly static decisions. Long-term plan: support more dynamic lazy evaluation in engines.

## Activity Helper Interfaces
Defined in `rtl/include/eclhelper.hpp`:
- `IHThorArg` — base for all activity helpers
- `IHThorFilterArg` — filter condition
- `IHThorProjectArg` — field projection/transformation
- `IHThorJoinArg` — join condition and transform
- `IHThorSortArg` — sort criteria
- `IHThorAggregateArg` — aggregation
- Many more (~100+ activity kinds)

Each generated activity class implements the appropriate interface, providing the specific logic (filter predicate, project transform, sort key comparator, etc.) as virtual method overrides.
