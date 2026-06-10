# Thor Activity Family Sweep

This document tracks the staged, small-scope sweep of Thor activity families. Each family is analyzed separately so variant-specific behavior is not blurred together.

## Status Legend
- **covered**: family has a dedicated analysis document
- **queued**: family is scheduled for a dedicated pass but not yet documented
- **deferred**: family is active but intentionally out of scope for the current sweep

## Families
- **covered**: [Local and Grouped Sort](activity-family-local-grouped-sort.md)
- **covered**: [Global and Distributed Sort](activity-family-global-distributed-sort.md)
- **covered**: [Hash Distribute and Variants](activity-family-hash-distribute.md)
- **covered**: [Local and Grouped Join](activity-family-local-grouped-join.md)
- **covered**: [Global and Distributed Join](activity-family-global-distributed-join.md)
- **covered**: [SelfJoin Family](activity-family-selfjoin.md)
- **covered**: [Lookup Join Family](activity-family-lookup-join.md)
- **covered**: [Keyed Join Family](activity-family-keyed-join.md)
- **covered**: [Group / Degroup / Rollover Family](activity-family-group-degroup-rollover.md)
- **covered**: [Rollup / Dedup / Aggregate Family](activity-family-rollup-dedup-aggregate.md)
- **covered**: [Funnel / Merge / TopN Family](activity-family-funnel-merge-topn.md)
- **covered**: [Disk Read / Fetch / Normalize Family](activity-family-disk-read-fetch-normalize.md)
- **covered**: [Project / Filter / CountProject / Apply Family](activity-family-project-filter-countproject-apply.md)
- **covered**: [Limit / FirstN / Sample / SelectNth / ChooseSets Family](activity-family-limit-firstn-sample-selectnth-choosesets.md)
- **covered**: [Result / Materialization / Write Sinks Family](activity-family-result-materialization-write-sinks.md)
- **covered**: [Structured Sources and Adapters Family](activity-family-structured-sources-and-adapters.md)
- **covered**: [Soapcall / Httpcall Family](activity-family-soapcall-httpcall.md)
- **covered**: [Iterate / Process Family](activity-family-iterate-process.md)
- **covered**: [Loop Family](activity-family-loop.md)
- **deferred**: KeyDiff / KeyPatch Family

## Notes
- The earlier documents in this knowledge base cover shared buffering, graph runtime, spill I/O, and representative activity consumers.
- The family sweep is where variant-specific optimization opportunities are separated into **confirmed**, **plausible but unverified**, and **needs measurement** buckets.
- KeyDiff and KeyPatch are still wired into Thor and regression coverage, but they are niche maintenance paths and are intentionally deferred from the current optimization sweep.