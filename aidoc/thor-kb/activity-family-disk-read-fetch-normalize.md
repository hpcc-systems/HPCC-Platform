# Thor Activity Family: Disk Read / Fetch / Normalize

This document covers the source-and-expansion family rooted in [../../thorlcr/activities/diskread](../../thorlcr/activities/diskread), [../../thorlcr/activities/fetch](../../thorlcr/activities/fetch), and [../../thorlcr/activities/normalize](../../thorlcr/activities/normalize). Most of this family is streaming. The main optimization questions are in translation setup, remote-read pushdown, and random-access fetch mechanics rather than in spill-heavy buffering.

## 1. Family Summary
This family splits into three different roles.

- `diskread` is the sequential flat-file source. It preserves grouped part order, can choose remote filtered reads for eligible replicas, and rebuilds translation/filter state per part open.
- `fetch` is the random-access companion. It redistributes requests by owning node, then performs per-`fpos` reads plus optional layout translation and final transform.
- `normalize` is a row-expansion family. Generic normalize expands upstream rows and only preserves grouping when a group emits at least one output row. `disknormalize` reuses diskread plumbing but emits an ungrouped stream.

There is very little spill-oriented buffering in this slice. The stronger optimization surfaces are repeated translation setup in diskread, incomplete remote pushdown in remote disk reads, and per-row reopen behavior in flat/CSV fetch.

## 2. Main Anchors
- [../../thorlcr/activities/thdiskbase.cpp#L35](../../thorlcr/activities/thdiskbase.cpp#L35) `CDiskReadMasterBase::init()`
- [../../thorlcr/activities/diskread/thdiskread.cpp#L34](../../thorlcr/activities/diskread/thdiskread.cpp#L34) master validation and grouping checks
- [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L53](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L53) `CDiskReadSlaveActivityRecord`
- [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L69](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L69) per-part `getTranslators()`
- [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L251](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L251) `CDiskRecordPartHandler::open()`
- [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L310](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L310) remote filtered read creation
- [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L585](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L585) `remoteLimit` pushdown setup
- [../../thorlcr/activities/fetch/thfetch.cpp#L17](../../thorlcr/activities/fetch/thfetch.cpp#L17) `CFetchActivityMaster`
- [../../thorlcr/activities/fetch/thfetchslave.cpp#L53](../../thorlcr/activities/fetch/thfetchslave.cpp#L53) `CFetchStream`
- [../../thorlcr/activities/fetch/thfetchslave.cpp#L380](../../thorlcr/activities/fetch/thfetchslave.cpp#L380) cached fetch layout translations
- [../../thorlcr/activities/fetch/thfetchslave.cpp#L610](../../thorlcr/activities/fetch/thfetchslave.cpp#L610) flat fetch implementation
- [../../thorlcr/activities/fetch/thfetchslave.cpp#L647](../../thorlcr/activities/fetch/thfetchslave.cpp#L647) CSV fetch implementation
- [../../thorlcr/activities/fetch/thfetchslave.cpp#L685](../../thorlcr/activities/fetch/thfetchslave.cpp#L685) XML fetch implementation
- [../../thorlcr/activities/normalize/thnormalizeslave.cpp#L28](../../thorlcr/activities/normalize/thnormalizeslave.cpp#L28) `NormalizeSlaveActivity`
- [../../thorlcr/activities/normalize/thnormalizeslave.cpp#L100](../../thorlcr/activities/normalize/thnormalizeslave.cpp#L100) `CNormalizeChildSlaveActivity`
- [../../thorlcr/activities/normalize/thnormalizeslave.cpp#L169](../../thorlcr/activities/normalize/thnormalizeslave.cpp#L169) `CNormalizeLinkedChildSlaveActivity`

## 3. Confirmed Optimization Opportunities
### A. Cache diskread translators across parts the way fetch already does
`CDiskRecordPartHandler::open()` rebuilds translation state with `getTranslators(*partDesc)` on each part open at [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L251](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L251). By contrast, fetch precomputes and reuses part translations at [../../thorlcr/activities/fetch/thfetchslave.cpp#L380](../../thorlcr/activities/fetch/thfetchslave.cpp#L389) via [../../thorlcr/thorutil/thormisc.cpp#L1560](../../thorlcr/thorutil/thormisc.cpp#L1560).

For many-part reads with stable layouts, diskread is doing repeated setup work that already has a same-family precedent for reuse.

### B. Push more `LIMIT` and `SKIP` logic into remote filtered reads
`diskread` computes `remoteLimit` and passes it into `createRemoteFilteredFile()` at [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L310](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L310), but the code explicitly notes at [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L589](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L589) that the remote side could handle skip as well.

This is a confirmed pushdown opportunity already acknowledged in the source.

### C. Reuse or buffer flat and CSV fetch readers
Flat fetch opens an unbuffered serial stream for each fetch at [../../thorlcr/activities/fetch/thfetchslave.cpp#L617](../../thorlcr/activities/fetch/thfetchslave.cpp#L617), and CSV fetch does the same at [../../thorlcr/activities/fetch/thfetchslave.cpp#L672](../../thorlcr/activities/fetch/thfetchslave.cpp#L672). XML fetch in the same file keeps per-part buffered streams alive and seeks them at [../../thorlcr/activities/fetch/thfetchslave.cpp#L709](../../thorlcr/activities/fetch/thfetchslave.cpp#L742).

That makes stream reuse or buffering a confirmed optimization surface for the flat and CSV variants.

## 4. Plausible But Unverified Opportunities
### A. Special-case local or grouped fetch to bypass generic distributor machinery
Local/grouped fetch avoids a real MP tag in some paths, but `CFetchStream` still connects through the generic hash distributor at [../../thorlcr/activities/fetch/thfetchslave.cpp#L176](../../thorlcr/activities/fetch/thfetchslave.cpp#L180). I did not trace enough runtime behavior to call this confirmed waste.

### B. Carry resolved part index with redistributed key rows
`CFPosHandler::hash()` already resolves ownership at [../../thorlcr/activities/fetch/thfetchslave.cpp#L97](../../thorlcr/activities/fetch/thfetchslave.cpp#L110), while multi-part fetch later does another lookup in `nextRow()` at [../../thorlcr/activities/fetch/thfetchslave.cpp#L221](../../thorlcr/activities/fetch/thfetchslave.cpp#L254). If that second search is hot, a wider routed payload could remove it.

### C. Cache remote-read replica checks across repeated part opens
Remote streaming currently probes for local copies before committing to a remote candidate in [../../thorlcr/activities/diskread/thdiskreadslave.cpp#L256](../../thorlcr/activities/diskread/thdiskreadslave.cpp#L283). On remote-heavy jobs this may add metadata traffic, but I did not measure whether it is material.

## 5. Areas That Need Measurement
- translator construction cost per part for diskread workloads with many parts
- remote filtered read usage rate, failover rate, and open latency
- how much `SKIP` remains local after current remote pushdown
- fetch seek distance and locality per part
- flat/CSV fetch reopen count versus rows returned
- fetch redistribution counts and bytes by owner node
- grouped output suppression frequency in diskread and normalize variants

## 6. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/thdiskbase.cpp](../../thorlcr/activities/thdiskbase.cpp)
2. [../../thorlcr/activities/diskread/thdiskread.cpp](../../thorlcr/activities/diskread/thdiskread.cpp)
3. [../../thorlcr/activities/diskread/thdiskreadslave.cpp](../../thorlcr/activities/diskread/thdiskreadslave.cpp)
4. [../../thorlcr/activities/fetch/thfetch.cpp](../../thorlcr/activities/fetch/thfetch.cpp)
5. [../../thorlcr/activities/fetch/thfetchslave.cpp](../../thorlcr/activities/fetch/thfetchslave.cpp)
6. [../../thorlcr/activities/normalize/thnormalizeslave.cpp](../../thorlcr/activities/normalize/thnormalizeslave.cpp)