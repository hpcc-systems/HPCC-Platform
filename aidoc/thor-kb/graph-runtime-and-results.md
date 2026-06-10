# Thor Graph Runtime and Results

This document covers the graph/runtime layer above the shared buffering and spilling infrastructure. The graph layer does not implement spilling itself, but it strongly shapes when results are materialized, how often child graphs are evaluated, and how much spill-heavy work overlaps.

## 1. Graph-Side Role
The graph/runtime layer primarily controls:

- **execution boundaries**: when a graph or subgraph is started, queued, waited on, and completed
- **result ownership**: whether graph results sit in simple in-memory buffers or overflow-capable wrappers
- **child-query reuse and serialization**: whether a child graph is pooled, cloned, or serialized behind `evaluateCrit`
- **master/slave materialization**: when results are fetched from slaves or collated on the master into new buffers

That means graph code is often the reason buffered rows live longer, overlap more, or cross an extra materialization boundary.

## 2. Result Wrappers and Materialization Boundaries
[../../thorlcr/graph/thgraph.cpp#L100](../../thorlcr/graph/thgraph.cpp#L100) defines `CThorGraphResult`, the graph-owned result wrapper that chooses between simple buffering and overflow-capable buffering.

This is the main graph-side entry point into spill-capable result handling.

Two important downstream materialization boundaries are:

- slave-side distributed-result promotion in [../../thorlcr/graph/thgraphslave.cpp#L1478](../../thorlcr/graph/thgraphslave.cpp#L1478), where `CThorSlaveGraphResults::getResult()` lazily fetches a global result
- master-side collation in [../../thorlcr/graph/thgraphmaster.cpp#L2389](../../thorlcr/graph/thgraphmaster.cpp#L2389), where `CCollatedResult::ensure()` gathers per-slave rows into a new result object

These boundaries matter because they can duplicate buffering above the original activity result stream.

## 3. Execute Boundaries and Scheduling
The main execute boundary is [../../thorlcr/graph/thgraph.cpp#L1481](../../thorlcr/graph/thgraph.cpp#L1481), where `CGraphBase::execute()` decides whether the graph runs inline or is queued for async execution.

The main scheduler boundary is [../../thorlcr/graph/thgraph.cpp#L2612](../../thorlcr/graph/thgraph.cpp#L2612), where `CGraphExecutor::add()` enforces the `concurrentSubGraphs` limit.

This makes concurrent subgraph count a first-order memory/spill control:

- too little overlap can serialize otherwise independent spill-heavy work
- too much overlap can drive multiple spill-capable activities into memory pressure at the same time

## 4. Child Graph Evaluation and Reuse
In [../../thorlcr/graph/thgraph.cpp#L2248](../../thorlcr/graph/thgraph.cpp#L2248), `CGraphBase::evaluate()` takes `evaluateCrit`, recreates local results, and executes the child graph.

This is the core graph-side serialization point for hot child queries.

In [../../thorlcr/graph/thgraph.cpp#L1921](../../thorlcr/graph/thgraph.cpp#L1921), `CChildParallelFactory` pools/clones child graph instances so parallel child evaluations can reuse graph objects instead of rebuilding them each time.

In [../../thorlcr/graph/thgraph.cpp#L2108](../../thorlcr/graph/thgraph.cpp#L2108), `CGraphBase::executeChildGraphs()` controls whether sink child graphs run in sequence or by iterator order. This directly affects how much sibling spill-heavy work can overlap.

## 5. Context Shipping and Slave Activation
Graph/runtime behavior also includes create/start context transfer and delayed slave activation:

- [../../thorlcr/graph/thgraphmaster.cpp#L2735](../../thorlcr/graph/thgraphmaster.cpp#L2735) sends activity init data from the master
- [../../thorlcr/graph/thgraphslave.cpp#L1007](../../thorlcr/graph/thgraphslave.cpp#L1007) receives that context on the slave
- [../../thorlcr/graph/thgraphslave.cpp#L1192](../../thorlcr/graph/thgraphslave.cpp#L1192) executes the slave subgraph after outputs are prepared

This means some setup, buffering, and ownership decisions are made before a spill-heavy activity looks active from the activity's own perspective.

## 6. Reusable Graph-Level Themes
- **graph orchestration can dominate memory behavior**: spill cost is not only about activity internals; overlap, collation, and result fetching matter too
- **child queries pay graph-level setup every time**: repeated child evaluations incur locks and fresh result setup even before activity-local work begins
- **result boundaries can force re-materialization**: slave fetch and master collation can duplicate ownership and buffering
- **temp lifetime is graph-scoped**: graph-side cleanup order determines when spill-related state is actually released
- **the master is not a child-query executor**: master-side logic mostly fetches, collates, and coordinates rather than reusing slave child-query execution paths

## 7. Why This Slice Matters
The shared buffering/spilling layer explains how rows are stored and spilled. The activity layer explains who consumes those primitives. The graph/runtime layer explains when those consumers overlap, serialize, fetch, collate, and release their results.

That makes this the right final layer for reasoning about spill-heavy behavior across child queries, asynchronous subgraphs, and distributed result handling.