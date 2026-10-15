# Roxie Server Threading Model: Sinks and Record Pullers

> Scope: how the Roxie **server** side creates OS threads for (a) readahead / record puller
> threads and (b) parallel graph sink execution, and what the existing "pooled" options do.
> Keywords for future greps: `RecordPullerThread`, `RestartableThread`, `ReadaheadMode`,
> `CRoxieServerReadAheadInput`, `CRoxieThreadedConcatReader`, `CActivityGraph::doExecute`,
> `SinkThread`, `SinkPooledThread`, `SinkMode`, `CPooledPersistent`, `GlobalThreadPool`,
> `CThreadPool`, `IPooledThread`, `ITaskScheduler`, `queryIOTaskScheduler`.

## 1. Thread-creation primitives available

| Primitive | File | Semantics |
|---|---|---|
| `Thread` (jthread.hpp:103) / `CThreaded` (184) | `system/jlib/jthread.hpp` | One OS thread per instance, created on `start()`, destroyed after `run()` returns. |
| `CThreadedPersistent` (jthread.hpp:210) | `system/jlib/jthread.hpp` | Owns **one dedicated OS thread for the lifetime of the object**; `start()`/`join()` are just semaphore handshakes. Thread exists even while idle. |
| `CPooledPersistent` (jthread.hpp:195) | `system/jlib/jthread.hpp` | Thin wrapper over a process-wide `GlobalThreadPool`; `start()` grabs a pooled thread, `join()` waits for the handle. |
| `IPooledThread` (289) / `IThreadPool` (305) / `createThreadPool` (325) | `system/jlib/jthread.hpp` | Generic pool of `IPooledThread`; supports `start`, `startNoBlock`, `join(handle)`, `setPoolSize`, `waitAvailable`, `setStartDelayTracing`, `setNiceValue`. |
| `ITaskScheduler` (jtask.hpp:149) | `system/jlib/jtask.hpp` | Work-stealing task scheduler. `queryTaskScheduler()` = `min(maxTasks, getAffinityCpus())` threads (CPU-bound, must not block); `queryIOTaskScheduler()` = `getAffinityCpus()*2` (short IO blocks only). |

### GlobalThreadPool (backs `CPooledPersistent`)
[system/jlib/jthread.cpp](system/jlib/jthread.cpp#L573-L611)

```
defaultmax      = 0     // NO LIMIT -> availsem throttling is entirely disabled
delay           = 0
stacksize       = 0     // default
timeoutOnRelease= 0
targetpoolsize  = 200   // used if the pool is later changed to a bounded size
```

Because `defaultmax == 0`:
- `CThreadPool::_start` skips the `availsem` wait entirely ([jthread.cpp#L1153-L1160](system/jlib/jthread.cpp#L1153)), so **starts never block and never throw** — the pool is elastic/unbounded.
- `notifyStopped` skips the trimming branch when `defaultmax==0`, so wrappers are retained at the pool high-water mark and then recycled. `targetpoolsize` only becomes relevant if the pool is later changed to a bounded configuration.
- `CPooledPersistent::start()` now supports per-start context inheritance, so logging context, jobId, trace flags, and active span can be propagated to pooled jobs.
- `CPooledPersistent::stop()` forwards to the owner's non-blocking `IThreaded::requestAbort()` hook. Owners must implement that hook when a pooled job needs active cancellation.
- `CPooledThreadWrapper::run` calls `callThreadTerminationHooks(true)` after every job ([jthread.cpp#L1064](system/jlib/jthread.cpp#L1064)), which resets per-thread plugin state (py3embed/javaembed/mysql contexts, `eclrtl` unicode converter map).

## 2. Record puller threads

`RecordPullerThread` : `RestartableThread` : `CInterface`
- Definition: [roxie/ccd/ccdserver.cpp#L2606](roxie/ccd/ccdserver.cpp#L2606)
- `RestartableThread` selects a raw `Thread` or `CPooledPersistent` backend for each start, preserves the composed diagnostic name, rejects restart while a previous join is active, and makes concurrent joiners wait for worker completion.
- Thread start selects pooled mode from `ctx->queryOptions().readaheadMode`, only when `noThread==false`.
- Thread body: `run()` [ccdserver.cpp#L2717](roxie/ccd/ccdserver.cpp#L2717), wrapped in `TaskScopeTracker(EventTask::Readahead, activityId)`.
- Join: existing owner-specific shutdown paths interrupt semaphores or network waits where supported. `stop()` sets `eof`, forwards a nonblocking pooled abort request, joins, and only then calls `inputStream->stop()`. `stop()` and `nextRow()` are never concurrent; a stream without a separate interruption path retains the existing possibility of waiting indefinitely for `nextRow()` to return.

`ReadaheadMode` accepts `thread` (default) or `pooled`. It is configured by global `topology/@readaheadMode`, then optionally overridden by the workunit debug value `readaheadMode`. Missing or invalid values select `thread`; there is no per-subgraph hint.

### Instances (one thread each, per activity instance, per graph iteration)
| Activity | `puller` member | `start()` call |
|---|---|---|
| `CRoxieServerReadAheadInput` | [#L2829](roxie/ccd/ccdserver.cpp#L2829) | [#L2878](roxie/ccd/ccdserver.cpp#L2878) |
| Pipe through/read | [#L10013](roxie/ccd/ccdserver.cpp#L10013) | [#L10068](roxie/ccd/ccdserver.cpp#L10068) (preload forced 0) |
| `CRoxieThreadedConcatReader` (one per input!) | [#L14015](roxie/ccd/ccdserver.cpp#L14015) | [#L13916](roxie/ccd/ccdserver.cpp#L13916) |
| `CRoxieServerPrefetchProjectActivity` | [#L15453](roxie/ccd/ccdserver.cpp#L15453) | [#L15530](roxie/ccd/ccdserver.cpp#L15530) (`noThread = !isThreaded`) |
| Fetch | [#L25622](roxie/ccd/ccdserver.cpp#L25622) | [#L25691](roxie/ccd/ccdserver.cpp#L25691) |
| Full keyed join | [#L26339](roxie/ccd/ccdserver.cpp#L26339) | [#L26469](roxie/ccd/ccdserver.cpp#L26469) |
| Keyed join base (half keyed) | [#L26689](roxie/ccd/ccdserver.cpp#L26689) | [#L27179](roxie/ccd/ccdserver.cpp#L27179), [#L27349](roxie/ccd/ccdserver.cpp#L27349) (`noThread = isSimple`) |
| Also `CRoxieServerTwoInputActivity::puller` (join/denormalize rhs) | [#L3021](roxie/ccd/ccdserver.cpp#L3021) | created at [#L13339](roxie/ccd/ccdserver.cpp#L13339), [#L19695](roxie/ccd/ccdserver.cpp#L19695), [#L20191](roxie/ccd/ccdserver.cpp#L20191) |

### Puller threads BLOCK — this is essential to their design
- Producer back-pressure: `CRoxieServerReadAheadInput::processRow` does `ready.signal(); space.wait();` [ccdserver.cpp#L2971-L2977](roxie/ccd/ccdserver.cpp#L2971); same pattern in `CRoxieThreadedConcatReader::processRow` [#L13954](roxie/ccd/ccdserver.cpp#L13954). `READAHEAD_SIZE`=1000, `CONCAT_READAHEAD`=1000.
- The puller also calls `inputStream->nextRow()`, which for a remote activity blocks in `CRemoteResultAdaptor` on `sentsome` / message collation ([#L5095](roxie/ccd/ccdserver.cpp#L5095)).
- Keyed join pullers call `remote.getMem()` → `send(b->flush())` [#L4752-L4769](roxie/ccd/ccdserver.cpp#L4752), i.e. network dispatch with its own throttling.
- Consequence: **a record puller can block indefinitely, and it is only unblocked by its consumer**. Consumer and producer form a bounded-buffer pair.

## 3. Graph sink execution

`SinkMode` enum: [roxie/ccd/ccd.hpp#L284-L294](roxie/ccd/ccd.hpp#L284). Text mapping in `getSinkMode` [ccdserver.cpp#L571](roxie/ccd/ccdserver.cpp#L571):
`parallel`, `parallelpersistent`, `sequential`, `automatic` (default), `automatic-parallel`, `automatic-persistent`, `pooled` (→ `AutomaticPooled`).

Configured from, in precedence order:
- global `topology/@sinkMode` → `defaultSinkMode` [ccdmain.cpp#L1446-L1448](roxie/ccd/ccdmain.cpp#L1446), default `Automatic` [ccdmain.cpp#L159](roxie/ccd/ccdmain.cpp#L159)
- workunit debug value `sinkMode` [ccdquery.cpp#L514](roxie/ccd/ccdquery.cpp#L514)
- per-subgraph ECL hint `hint[@name='sinkmode']` [ccdserver.cpp#L643-L645](roxie/ccd/ccdserver.cpp#L643)
- inherited from the parent activity factory for child graphs [ccdserver.cpp#L28444-L28448](roxie/ccd/ccdserver.cpp#L28444)

### "Automatic" sink grouping
[ccdserver.cpp#L28510-L28527](roxie/ccd/ccdserver.cpp#L28510): for `sinkMode >= Automatic`, any sink **without** `RSC::hasRowLatency | RSC::slowDependencies | RSC::urgentStart` is folded into a single `CSequentialSinkSet` ([#L28155](roxie/ccd/ccdserver.cpp#L28155)) that runs its members sequentially on one thread. This is the main existing mitigation for thread explosion.

### `CActivityGraph::doExecute` [ccdserver.cpp#L28640-L28758](roxie/ccd/ccdserver.cpp#L28640)
- 1 sink → executed inline on the caller's thread.
- `probeManager` set, or graph `isSequential()`, or `SinkMode::Sequential` → sequential loop.
- `ParallelPersistent` / `AutomaticPersistent` → `SinkThread` ([#L28340](roxie/ccd/ccdserver.cpp#L28340), `CThreadedPersistent`) cached in `CIArrayOf<SinkThread> threads`, **one dedicated OS thread held for the lifetime of the graph instance**. Start [#L28365](roxie/ccd/ccdserver.cpp#L28365), join [#L28369](roxie/ccd/ccdserver.cpp#L28369). Last sink runs on the caller thread.
- `Parallel` / `Automatic` / `AutomaticParallel` → `CAsyncFor::For(n, n)` [#L28707](roxie/ccd/ccdserver.cpp#L28707), which creates **n-1 brand new `Thread`s per execution** ([jthread.cpp#L751-L830](system/jlib/jthread.cpp#L751)). This is the default path.
- `AutomaticPooled` → `SinkPooledThread` ([#L28378](roxie/ccd/ccdserver.cpp#L28378), `CPooledPersistent`), created fresh per execution but the *OS thread* comes from `GlobalThreadPool`. Start [#L28401](roxie/ccd/ccdserver.cpp#L28401), join [#L28405](roxie/ccd/ccdserver.cpp#L28405).

Exceptions are funnelled through `CActivityGraph::noteException` and re-thrown by `checkAbort()`.

## 4. Key invariants / hazards
1. **Sinks and pullers both block, and they nest.** A sink thread executes an activity subtree that may itself start readahead/keyed-join pullers, which in turn may start child graphs with their own sinks. Depth is bounded only by the query graph.
2. **Nothing thread-local in `roxiemem`** — grep for `thread_local` under `roxie/` finds only `ccdcache.cpp` (sigbus jmp buffers). Row allocators are per-activity objects, not per-thread, so migrating work between threads is safe from a memory-manager point of view.
3. Thread-local state that *does* matter: jlib logging context (`defaultJobId`, `default_thread_logctx`, `threadTraceFlags`, `threadActiveSpan` — [jthread.cpp#L2911-L2934](system/jlib/jthread.cpp#L2911)) and embedded-plugin contexts registered via `addThreadTermFunc`.
4. Pooled pullers and sinks call `CPooledPersistent::start(true)`, which captures job ID, logging context, trace flags, and active span for each start.
5. The agent side already uses a **bounded** pool: `RoxieQueue` / `createThreadPool(..., numWorkers)` [ccdqueue.cpp#L1397](roxie/ccd/ccdqueue.cpp#L1397) with `numAgentThreads` default 30 ([ccdmain.cpp#L73](roxie/ccd/ccdmain.cpp#L73), `topology/@agentThreads`). Agent work is designed not to block on other agent work; server-side sink/puller work is not.
