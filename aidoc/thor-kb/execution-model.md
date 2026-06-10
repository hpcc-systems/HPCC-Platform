# Thor Execution Model

This document summarizes the execution path that matters most when analyzing or optimizing Thor.

## 1. Workunit to Thor Queue
The workflow described in [../../devdoc/Workunits.md](../../devdoc/Workunits.md) is the starting point for Thor execution:

- Workunits are compiled through the standard workunit pipeline.
- Thor graph work is associated with the `<cluster>.thor` queue, while `<cluster>.agent` is used for workunits executed on hThor or Thor.
- In the generated workflow helper, `ctx->executeGraph()` transfers control into the engine-specific execution path. For Thor, that means queueing graph work rather than executing the graph locally inside the workflow step.

## 2. Graphs, Subgraphs, and Helpers
Thor and hThor execute **one subgraph at a time**. Each activity within a graph is described by:

- a `ThorActivityKind`
- a helper implementation derived from `IHThorArg`
- XGMML graph metadata describing dependencies, edges, record sizes, and hints

This means performance work often needs to follow all of these layers together: generated helper -> graph metadata -> runtime activity implementation.

## 3. Child Graph Evaluation
Thor child-graph evaluation is not free. In [../../thorlcr/graph/thgraph.cpp#L2248](../../thorlcr/graph/thgraph.cpp#L2248), `CGraphBase::evaluate()` takes `evaluateCrit` before creating local results and executing the child graph:

- the `evaluateCrit` lock serializes entry to `evaluate()` for that graph instance
- fresh local results are created for each evaluation
- repeated child-query execution can amplify both synchronization and temporary-result overhead

When analyzing per-row child datasets, normalizes, projects, or loop-like behavior, inspect this path first.

## 4. Why This Matters for Optimization
The most expensive Thor problems often cross abstraction boundaries:

- a workflow-level decision may determine which graph runs and how often
- a graph-level dependency may force extra materialization or serialization
- an activity-level choice may introduce skew, spill, or single-node bottlenecks

The practical rule is to trace end-to-end from workflow item to graph to activity before deciding where the real bottleneck lives.

## 5. Initial Watch List
- child-query workloads with very high evaluation counts
- cases where graph-level timing is missing or misleading
- activities whose helper semantics imply local-only behavior, distribution, or ordering guarantees that block a simpler optimization