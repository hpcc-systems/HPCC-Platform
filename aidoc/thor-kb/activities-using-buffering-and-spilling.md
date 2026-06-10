# Thor Activities Using Buffering and Spilling

This document focuses on how Thor activity classes consume the shared buffering/spilling infrastructure from [shared-buffering-and-spilling.md](shared-buffering-and-spilling.md). The aim is to understand activity-side usage patterns before looking at graph classes.

## 1. Main Consumption Patterns
Thor activities mostly consume the shared layer in three ways:

- **row-loader path**: create an `IThorRowLoader`, call `load()` or `loadGroup()`, then read back a final stream
- **row-collector path**: create an `IThorRowCollector`, write rows through an `IRowWriter`, then hand the result back as a stream
- **explicit spillable replay path**: materialize rows for inspection and then wrap them in a `CThorSpillableRowArray` to replay through a spill-capable stream

The shared factories behind these patterns are in [../../thorlcr/thorutil/thmem.cpp#L2069](../../thorlcr/thorutil/thmem.cpp#L2069) and [../../thorlcr/thorutil/thmem.cpp#L2167](../../thorlcr/thorutil/thmem.cpp#L2167).

## 2. Canonical Loader Consumer: LocalSort
The cleanest loader example is `CLocalSortSlaveActivity` in [../../thorlcr/activities/msort/thgroupsortslave.cpp#L52](../../thorlcr/activities/msort/thgroupsortslave.cpp#L52).

At startup it creates a row loader with mixed spill policy and a sort-specific spill priority. In [../../thorlcr/activities/msort/thgroupsortslave.cpp#L67](../../thorlcr/activities/msort/thgroupsortslave.cpp#L67), it then chooses between `loadGroup()` and `load()` based on grouped versus ungrouped execution.

This makes LocalSort the best anchor for:

- baseline loader cost
- grouped versus ungrouped loader behavior
- the direct user-visible effect of shared loader decisions on a common activity family

## 3. Canonical Collector Consumer: Group
The cleanest collector example is `GroupSlaveActivity` in [../../thorlcr/activities/group/thgroupslave.cpp#L102](../../thorlcr/activities/group/thgroupslave.cpp#L102).

When rollover handling is enabled, it creates an `IThorRowCollector`, obtains an `IRowWriter`, buffers the first group that must be sent backward, and then converts the collector back into a stream at [../../thorlcr/activities/group/thgroupslave.cpp#L128](../../thorlcr/activities/group/thgroupslave.cpp#L128).

This is the canonical write-buffer-stream handoff pattern. Large rollover groups therefore stress the shared collector and spill path, not just the grouping logic itself.

## 4. Richest Mixed Consumer: LookupJoin
`CLookupJoinActivityBase` is the most informative activity family because it uses multiple shared-layer patterns and changes behavior when spill appears.

### Collector setup by policy
In [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2292](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2292), LookupJoin chooses collector policy based on whether it is operating in smart/global mode or memory-only local mode.

### Spill can trigger algorithm failover
In [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2623](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2623), a spilled collector causes LookupJoin to abandon in-memory lookup handling and fail over toward standard join behavior.

### Final handoff path
In [../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2667](../../thorlcr/activities/lookupjoin/thlookupjoinslave.cpp#L2667), the collector returns either a stream or all-memory rows depending on whether spill occurred and whether local hash-table preparation succeeded.

This makes LookupJoin the best activity-level anchor for:

- spill-sensitive algorithm selection
- collector shrink/spill behavior under pressure
- the boundary between in-memory lookup processing and standard join fallback

## 5. Secondary Activity Anchors
Some additional activity families are worth keeping in mind, but they are secondary to LocalSort, Group, and LookupJoin:

- **FilterGroup**: [../../thorlcr/activities/filter/thfilterslave.cpp#L258](../../thorlcr/activities/filter/thfilterslave.cpp#L258), [../../thorlcr/activities/filter/thfilterslave.cpp#L298](../../thorlcr/activities/filter/thfilterslave.cpp#L298), and [../../thorlcr/activities/filter/thfilterslave.cpp#L312](../../thorlcr/activities/filter/thfilterslave.cpp#L312) show the pattern of whole-group inspection followed by explicit spillable replay.
- **Rollup/Dedup helpers**: [../../thorlcr/activities/rollup/throllupslave.cpp#L88](../../thorlcr/activities/rollup/throllupslave.cpp#L88), [../../thorlcr/activities/rollup/throllupslave.cpp#L114](../../thorlcr/activities/rollup/throllupslave.cpp#L114), and [../../thorlcr/activities/rollup/throllupslave.cpp#L554](../../thorlcr/activities/rollup/throllupslave.cpp#L554) show memory-first group materialization patterns.
- **HashJoin setup**: [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4011](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4011) and [../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4023](../../thorlcr/activities/hashdistrib/thhashdistribslave.cpp#L4023) show asymmetric loader policy choices for distributed join preparation.

## 6. What Not To Overweight In This Phase
- **TopN** is useful as a merger-side consumer, but not the best anchor for loader/collector lifetime reasoning.
- **Merge** is useful for fan-in behavior, but it is not the clearest spill-contract example.
- **Funnel** should stay out of this phase because it is not a direct user of the `thmem.cpp` loader/collector/spillable-row-array APIs in this slice.

## 7. Reusable Activity-Level Themes
- many activities materialize an entire group before making a decision, so very large groups remain a primary pressure point even when a shared loader is used
- spill can change the activity's algorithm, not merely where rows are stored
- explicit `CThorSpillableRowArray` use tends to appear after an activity has already needed random access or whole-group inspection and now wants replay with spill safety
- the most reusable reasoning path is: policy choice -> load or collect -> spill observation -> final handoff behavior