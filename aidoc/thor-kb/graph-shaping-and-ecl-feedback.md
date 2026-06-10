# Thor Graph Shaping and ECL Feedback

This note tracks optimization opportunities that live above a single Thor activity implementation. The common pattern is simple: the emitted graph shape can determine whether Thor handles millions of rows or billions of rows, whether spill happens on a small intermediate or after fanout, and whether a stage stays distributed or collapses onto one node.

## 1. Why This Deserves Its Own Note
Some of the highest-value opportunities from the current workload are not purely engine-local changes.

They include:

- reducing rows before a fanout-heavy stage
- preserving distribution that the graph already has
- choosing a cheaper join kind
- splitting one logical stage into two if that creates a better spill or reduction boundary
- warning when a graph shape is known to behave badly in child queries or heavily distributed graphs

These are still part of Thor optimization because they determine the workload Thor receives.

## 2. The Core Rule
For the expensive jobs in the current workload, the best graph is often the one that makes Thor do less work before the hardest stage starts.

The common example is a transitive-closure style pattern:

- spilling or reducing 100M rows before a self join is far better than spilling 100B rows after the self join has expanded the graph

That is the main design rule for this note: optimize where the graph can shrink, split, or preserve locality before the multiplicative stage.

## 3. High-Value Graph-Shaping Patterns

### 3.1 Reduce Before Fanout
Look for opportunities to reduce cardinality before:

- self joins
- funnels
- broad redistributes
- iterative or loop-driven stages
- wide child-query materialization paths

Good candidates include dedup, early filters, `MANY`, or helper-side reductions that avoid creating a much larger intermediate.

### 3.2 Preserve Distribution
If a dataset is already distributed in a useful way, avoid turning it into a centralized or keyed path unless semantics genuinely require it.

Current warning patterns include:

- a large join that looks like it should have stayed `LOOKUP`
- a keyed join that appears to be fed from an earlier join and read from one node
- distribute and merge work that collapses onto one node even though the original graph had broader parallelism

### 3.3 Choose The Cheapest Join Kind That Preserves Semantics
Several workload notes already point at joins that look structurally more expensive than necessary.

The main cases to watch are:

- tiny RHS joins that should use a lookup-style path when semantics allow it
- keyed joins used where a distribution-preserving join would be cheaper
- joins that rehash or rematerialize more than necessary because the graph shape no longer carries useful distribution information

### 3.4 Split A Logical Stage If It Creates A Better Boundary
Sometimes the best optimization is to split one logical activity sequence into two stages so that Thor can:

- spill earlier on a smaller intermediate
- reduce rows before a downstream explosion
- materialize once at a cheaper boundary instead of repeatedly at a more expensive one
- preserve an existing distribution before a later stage destroys it

This is especially relevant for very large graphs where a slightly better phase boundary can save hours.

### 3.5 Treat Ordered Funnel From Child Datasets As A Warning Shape
Current workload evidence already says ordered funnel from child datasets is terrible in child queries.

That should be treated as a graph-shape warning because the cost is not only inside the funnel implementation. It also comes from:

- repeated child evaluation
- result packaging and replay
- ordering constraints that remove cheaper alternatives

## 4. Slow-Job Patterns Worth Converting Into Warnings Or Hints
The current [../../devdoc/thoropt.md](../../devdoc/thoropt.md) notes suggest several graph-shape hints that would be valuable even before automatic rewrites exist.

### 4.1 `LOOKUP` Join Feedback
When the graph shape suggests a large join should have been `LOOKUP`, that should be surfaced clearly.

### 4.2 `MANY` Or Early Dedup Hints
The notes already point at `MANY` or early dedup before hash aggregate, distribute, selfjoin, and funnel stages as potentially hour-scale savings.

### 4.3 Preserve-Distribution Warnings
Warn when an already-distributed flow is about to be forced into a keyed or single-node pattern without an obvious semantic need.

### 4.4 Helper-Heavy Boundary Warnings
If an expensive helper is attached to a join or similar wide stage, the graph should make it obvious that the helper cost may multiply with row fanout.

## 5. What Thor Might Exploit Even Without Full Graph Rewriting
Thor may not be able to perform arbitrary graph rewrites, but it can still benefit from this note in several ways:

- stronger diagnostics and lint-style warnings for bad shapes
- preserving useful distribution metadata farther through the graph
- better heuristics for earlier spill or reduction boundaries
- allowing a split between logically separate phases when that reduces materialization or fanout cost
- exporting enough counters that the graph builder can learn which shapes are consistently bad

## 6. Relationship To The Rest Of The KB
- [cross-cutting-optimization-themes.md](cross-cutting-optimization-themes.md) explains why graph shaping matters at the strategy level.
- [top-10-priority-actions.md](top-10-priority-actions.md) ranks graph shaping near the top for the current production workload.
- [optimizations.md](optimizations.md) keeps the detailed backlog entries that graph-shaping work should influence.