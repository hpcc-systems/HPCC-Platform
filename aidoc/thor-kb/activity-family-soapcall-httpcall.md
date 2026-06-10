# Thor Activity Family: Soapcall and Httpcall

This document covers Thor's external service-call activities implemented by [../../thorlcr/activities/soapcall/thsoapcallslave.cpp](../../thorlcr/activities/soapcall/thsoapcallslave.cpp) and the shared helper layer in [../../common/thorhelper/thorsoapcall.cpp](../../common/thorhelper/thorsoapcall.cpp). The same family includes both `SOAPCALL` and `HTTPCALL`, row-returning and dataset-returning variants, and the row and dataset action forms.

## 1. Family Summary
This family splits into four runtime shapes.

- Row call activities build a helper and pull rows directly from the helper output stream.
- Dataset call activities expose the upstream input through `IWSCRowProvider` so helper threads can batch rows into requests.
- Row action activities run the helper for side effects only.
- Dataset action activities combine the dataset-row provider with the action-only completion path.

The slave wrappers are thin. The real behavior lives in `CWSCHelper` and `CWSCAsyncFor`, where thread allocation, batching, DNS/connect handling, retries, persistence, and response processing are implemented. That means the dominant costs in this family are not Thor spill paths but endpoint latency, batching policy, row-provider contention, and retry behavior.

## 2. Main Anchors
- [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L35](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L35) row call slave wrapper
- [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L131](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L131) dataset call slave wrapper
- [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L229](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L229) row action slave wrapper
- [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L287](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L287) dataset action slave wrapper
- [../../common/thorhelper/thorsoapcall.hpp#L35](../../common/thorhelper/thorsoapcall.hpp#L35) `IWSCRowProvider` contract
- [../../common/thorhelper/thorsoapcall.hpp#L52](../../common/thorhelper/thorsoapcall.hpp#L52) `IWSCHelper` contract
- [../../common/thorhelper/thorsoapcall.cpp#L986](../../common/thorhelper/thorsoapcall.cpp#L986) `CWSCHelper` setup and thread policy
- [../../common/thorhelper/thorsoapcall.cpp#L1672](../../common/thorhelper/thorsoapcall.cpp#L1672) request batching in `CWSCHelperThread::processQuery`
- [../../common/thorhelper/thorsoapcall.cpp#L2524](../../common/thorhelper/thorsoapcall.cpp#L2524) per-request DNS/connect/send/read/retry loop in `CWSCAsyncFor::Do`
- [../../thorlcr/thorutil/thormisc.cpp#L83](../../thorlcr/thorutil/thormisc.cpp#L83) shared soapcall statistics surface

## 3. Confirmed Optimization Opportunities
### A. Dataset modes serialize input fetch behind one lock
Dataset call and dataset action both guard `getNextRow()` with a `CriticalSection` at [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L215](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L215) and [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L351](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L351).

But helper threads are created in [../../common/thorhelper/thorsoapcall.cpp#L1289](../../common/thorhelper/thorsoapcall.cpp#L1289) and repeatedly pull provider rows in [../../common/thorhelper/thorsoapcall.cpp#L1751](../../common/thorhelper/thorsoapcall.cpp#L1751). For small `numRecordsPerBatch`, the row-provider lock is a confirmed scaling choke point.

### B. Dataset thread partitioning can waste part of the requested parallelism
`CWSCHelper` derives `numUrlThreads` at [../../common/thorhelper/thorsoapcall.cpp#L1275](../../common/thorhelper/thorsoapcall.cpp#L1275), then integer-divides into `numRowThreads` at [../../common/thorhelper/thorsoapcall.cpp#L1277](../../common/thorhelper/thorsoapcall.cpp#L1277). Each row thread then fans its request batch across URLs at [../../common/thorhelper/thorsoapcall.cpp#L1717](../../common/thorhelper/thorsoapcall.cpp#L1717).

That split leaves remainder parallelism unused and is a confirmed efficiency gap when the requested thread count does not divide cleanly by URL fan-out.

### C. Busy-server retry sleeps inside the worker thread
On busy responses, the retry path sleeps inside the request worker at [../../common/thorhelper/thorsoapcall.cpp#L2799](../../common/thorhelper/thorsoapcall.cpp#L2799). The exported signal is only the retry count increment at [../../common/thorhelper/thorsoapcall.cpp#L2856](../../common/thorhelper/thorsoapcall.cpp#L2856).

Under endpoint overload, that is a confirmed throughput and responsiveness problem: threads are occupied while sleeping instead of making forward progress or yielding cleanly to cancellation.

### D. Non-local row modes are intentionally first-node only
Non-local, non-grouped row call and row action only start helpers on `firstNode()` in [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L61](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L61) and [../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L255](../../thorlcr/activities/soapcall/thsoapcallslave.cpp#L255).

This is not a bug, but it is critical performance context. Poor cluster scaling in those modes may be design-driven rather than an accidental regression.

### E. Soapcall instrumentation is too thin for stage-level tuning
Thor exports only five family-specific counters through [../../thorlcr/thorutil/thormisc.cpp#L83](../../thorlcr/thorutil/thormisc.cpp#L83) and [../../thorlcr/thorutil/thormisc.cpp#L99](../../thorlcr/thorutil/thormisc.cpp#L99). HTTP status is attached only to the request span at [../../common/thorhelper/thorsoapcall.cpp#L2746](../../common/thorhelper/thorsoapcall.cpp#L2746), and activity-span DNS/connect attributes are overwritten per request at [../../common/thorhelper/thorsoapcall.cpp#L2561](../../common/thorhelper/thorsoapcall.cpp#L2561) and [../../common/thorhelper/thorsoapcall.cpp#L2678](../../common/thorhelper/thorsoapcall.cpp#L2678).

That is a confirmed diagnostics gap when trying to distinguish row-provider contention, DNS/connect cost, server busy behavior, and response failures.

## 4. Plausible But Unverified Opportunities
### A. Revisit DNS and connect timeout budgeting
The source leaves explicit timeout-budgeting questions in the DNS and connect path at [../../common/thorhelper/thorsoapcall.cpp#L2556](../../common/thorhelper/thorsoapcall.cpp#L2556) and [../../common/thorhelper/thorsoapcall.cpp#L2585](../../common/thorhelper/thorsoapcall.cpp#L2585).

This may allow slow DNS or connect attempts to consume more of the activity time budget than intended, but this slice did not prove the runtime effect.

## 5. Areas That Need Measurement
- row-provider lock hold time and contention rate for dataset modes
- realized helper-thread utilization versus requested `numParallelThreads`
- response-time impact of in-thread busy retries under overloaded endpoints
- incidence of first-node-only row modes on real Thor workloads
- per-request DNS, connect, send, and read latency histograms across URLs

## 6. Observability Notes
The current stats surface is good enough to prove that the family made network calls, but not good enough to tune it confidently.

- the exported counters do not separate provider wait, queueing, DNS, connect, send, read, parse, and retry-sleep costs cleanly
- request-level spans know more than the activity-level counters expose
- the activity span only retains the last-seen DNS/connect attribute values rather than a distribution

## 7. Practical Reading Order
For this family, the best path is:

1. [../../thorlcr/activities/soapcall/thsoapcallslave.cpp](../../thorlcr/activities/soapcall/thsoapcallslave.cpp)
2. [../../common/thorhelper/thorsoapcall.hpp](../../common/thorhelper/thorsoapcall.hpp)
3. [../../common/thorhelper/thorsoapcall.cpp](../../common/thorhelper/thorsoapcall.cpp)
4. [../../thorlcr/thorutil/thormisc.cpp](../../thorlcr/thorutil/thormisc.cpp)