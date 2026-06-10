# Thor Activity Family: Structured Sources and Adapters

This document covers the remaining read-side operators that are not part of the plain diskread or fetch family: [../../thorlcr/activities/indexread](../../thorlcr/activities/indexread), [../../thorlcr/activities/csvread](../../thorlcr/activities/csvread), [../../thorlcr/activities/xmlread](../../thorlcr/activities/xmlread), [../../thorlcr/activities/xmlparse](../../thorlcr/activities/xmlparse), [../../thorlcr/activities/parse](../../thorlcr/activities/parse), [../../thorlcr/activities/piperead](../../thorlcr/activities/piperead), [../../thorlcr/activities/wuidread](../../thorlcr/activities/wuidread), and [../../thorlcr/activities/external](../../thorlcr/activities/external).

## 1. Family Summary
This family is not one inheritance tree. It splits into three runtime shapes.

- File-backed structured readers such as `csvread` and `xmlread/jsonread` reuse the common disk-read mapping and part-open path.
- Index-backed readers are their own branch, with master-side TLK and part pruning plus slave-side remote or local key access.
- Adapter readers sit on top of non-file sources: `parse` and `xmlparse` consume upstream rows, `piperead` turns child-process output into rows, `wuidread` pulls serialized results from the master unless it is rewritten earlier, and `external` delegates row production to helper code.

The strongest confirmed optimization surfaces in this family are both narrow and explicit in source: CSV’s sequential test path and the physical-size validation cost in the shared disk-backed part-open path.

## 2. Main Anchors
- [../../thorlcr/activities/csvread/thcsvrslave.cpp#L240](../../thorlcr/activities/csvread/thcsvrslave.cpp#L240) CSV header-line handoff and test wait path
- [../../thorlcr/activities/thdiskbaseslave.cpp#L147](../../thorlcr/activities/thdiskbaseslave.cpp#L147) shared disk-backed physical-size validation note
- [../../thorlcr/activities/indexread/thindexreadslave.cpp#L245](../../thorlcr/activities/indexread/thindexreadslave.cpp#L245) remote filtered index path and stats gap
- [../../thorlcr/activities/indexread/thindexreadslave.cpp#L351](../../thorlcr/activities/indexread/thindexreadslave.cpp#L351) key-merger cache stats gap
- [../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L124](../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L124) per-row XML parser creation
- [../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L20](../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L20) master round-trip for raw workunit result fetch

## 3. Confirmed Optimization Opportunities
### A. Remove or keep quarantined the CSV sequential test path
`csvread` still contains the `csvWaitAllSubs` option at [../../thorlcr/activities/csvread/thcsvrslave.cpp#L253](../../thorlcr/activities/csvread/thcsvrslave.cpp#L253), and the source is explicit that this makes workers process CSV reads sequentially, “massively slowing down throughput” at [../../thorlcr/activities/csvread/thcsvrslave.cpp#L259](../../thorlcr/activities/csvread/thcsvrslave.cpp#L259).

This is a confirmed performance liability whenever that option is enabled.

### B. Revisit physical-size validation cost in the shared disk-backed open path
The common part-open path used by disk-backed structured readers explicitly notes at [../../thorlcr/activities/thdiskbaseslave.cpp#L147](../../thorlcr/activities/thdiskbaseslave.cpp#L147) that checking physical size likely adds latency, especially for remote or API-backed reads.

This is a confirmed I/O cost surface for file-backed structured sources.

### C. Export the indexread read-path statistics the code already knows are missing
`indexread` directly marks the lack of remote-handler stats at [../../thorlcr/activities/indexread/thindexreadslave.cpp#L254](../../thorlcr/activities/indexread/thindexreadslave.cpp#L254) and key-merger cache stats at [../../thorlcr/activities/indexread/thindexreadslave.cpp#L351](../../thorlcr/activities/indexread/thindexreadslave.cpp#L351).

That is a confirmed observability defect blocking deeper optimization work in this family.

## 4. Plausible But Unverified Opportunities
### A. Revisit master concentration in non-rewritten `wuidread`
Raw `wuidread` still round-trips through the master at [../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L20](../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L20) through [../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L44](../../thorlcr/activities/wuidread/thwuidreadslave.cpp#L44). Large rowsets may bottleneck there unless the diskread rewrite path fires earlier.

### B. Revisit per-row parser construction in `xmlparse`
`xmlparse` creates a new XML parser for each input row at [../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L124](../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L124) through [../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L131](../../thorlcr/activities/xmlparse/thxmlparseslave.cpp#L131). For many small documents, setup cost may dominate.

### C. Revisit pipe startup cost in `piperead`
`piperead` always pays child-process launch and stream framing overhead. That may dominate bursty or small workloads, but this slice did not measure it directly.

## 5. Areas That Need Measurement
- frequency of `csvWaitAllSubs` use outside test-only scenarios
- latency added by physical-size validation on remote structured reads
- `indexread` remote-handler bytes, cache hits, and merger behavior
- `wuidread` master round-trip volume when diskread rewrite does not apply
- per-row parser setup cost for `xmlparse`
- startup and steady-state cost split for `piperead`

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/thdiskbaseslave.cpp](../../thorlcr/activities/thdiskbaseslave.cpp)
2. [../../thorlcr/activities/csvread/thcsvrslave.cpp](../../thorlcr/activities/csvread/thcsvrslave.cpp)
3. [../../thorlcr/activities/indexread/thindexreadslave.cpp](../../thorlcr/activities/indexread/thindexreadslave.cpp)
4. [../../thorlcr/activities/xmlparse/thxmlparseslave.cpp](../../thorlcr/activities/xmlparse/thxmlparseslave.cpp)
5. [../../thorlcr/activities/wuidread/thwuidreadslave.cpp](../../thorlcr/activities/wuidread/thwuidreadslave.cpp)