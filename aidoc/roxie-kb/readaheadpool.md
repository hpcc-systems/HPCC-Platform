# Roxie Implementation Brief: Use the Global Pool for Sink and Readahead Threads

## Context

The jlib work is complete in these commits:

- `8d5688ad44`: pooled persistent thread context inheritance, naming, and exception propagation
- `e419d86474`: pooled-thread abort support and lock-safe stop handling
- `46851cb1c4`: O(1) free-wrapper allocation
- `fdc31d0339`: free-list membership safety fix
- `e6712ed0e4`: jlib documentation cleanup

The remaining work is Roxie-only. Do not modify jlib unless compilation exposes a genuine missing API.

Implemented in these Roxie commits:

- `b0499365fe`: selectable raw/pooled restartable backend and lifecycle test
- `e4a89b249a`: topology/workunit `readaheadMode` configuration and puller wiring
- `1ec1f0c000`: pooled sink exception reporting fix
- `de39f33dcc`: concurrent join serialization and lifecycle race test

## Confirmed decisions

- Add a raw-thread versus pooled-thread switch for **all** `RecordPullerThread` paths, not only a pilot activity.
- Keep raw threads as the default initially. Changing the default is a later, measurement-driven change.
- Expose the mode through topology and workunit/debug configuration, following the existing `sinkMode` pattern. A per-subgraph ECL hint is not required.
- Leave the `Automatic` sink default unchanged. Audit and preserve the existing `AutomaticPooled` path, but do not make pooled sinks the default in this work.
- `inputStream->stop()` and `inputStream->nextRow()` must never execute concurrently. Existing owner-specific semaphore and network interruption remains responsible for unblocking pullers where supported; `inputStream->stop()` remains post-join.
- Update the Roxie threading documentation with the implementation.
- Keep the Roxie work in separate commits for the pooled restartable-thread/puller changes, configuration changes, and sink fixes/tests where practical.

The objective is to allow both:

- Parallel graph sink execution
- Record puller/readahead execution

to use the existing elastic `GlobalThreadPool` through `CPooledPersistent`.

Do not use `queryTaskScheduler()` or `queryIOTaskScheduler()`. Sink and readahead work can block indefinitely and may depend cyclically on each other, so a bounded task scheduler can deadlock.

## Existing Roxie Code

Primary file:

- `ccdserver.cpp`

Relevant classes:

- `RestartableThread`, near the top of `ccdserver.cpp`
- `RestartableThread`, which now selects either a raw or pooled backend per start
- `RecordPullerThread`
- `SinkThread`
- `SinkPooledThread`
- `CActivityGraph::doExecute`

Existing sink modes are defined in:

- `ccd.hpp`
- `getSinkMode()` in `ccdserver.cpp`
- default configuration in `ccdmain.cpp`

The existing `AutomaticPooled` sink path already uses `SinkPooledThread` and `CPooledPersistent`. Preserve that behavior and improve it only where necessary.

## Required Changes

### 1. Make `RestartableThread` support a pooled backend

The former standalone `PooledRestartableThread` has been folded into `RestartableThread`, which owns either a raw `Thread` or `CPooledPersistent` backend for each start/join cycle.

It must preserve the behavior of `RestartableThread`:

- Same `start(logPrefix, ...)` semantics
- Same `join()` behavior
- Same restart behavior
- Same abort behavior
- Same thread naming
- Same exception propagation
- Same lifetime and ownership rules

Fix any issues caused by the existing pooled wrapper:

- Ensure the composed name passed to `CPooledPersistent` is not discarded.
- Ensure the name remains useful in pooled-thread diagnostics.
- Ensure `stop()` causes the same cooperative EOF/semaphore interruption as the non-pooled implementation.
- Ensure an abort request is forwarded through the new jlib `requestAbort()` mechanism.
- Ensure `join()` still reports exceptions correctly.

The pooled implementation must not call blocking user code while holding a jlib pool lock. The jlib pool now handles this correctly.

### 2. Allow `RecordPullerThread` to use the pooled implementation

`RecordPullerThread` is currently based on `RestartableThread`.

Change the design so every puller can select either:

- A normal short-lived OS thread
- A `CPooledPersistent`-backed thread from `GlobalThreadPool`

Prefer a small local abstraction rather than duplicating all puller logic.

`RestartableThread` internally chooses between a raw `Thread` and `CPooledPersistent`, preserving one copy of the puller logic.

Do not disturb existing inline execution. Existing `noThread` paths must continue to execute synchronously.

All puller call sites must remain behaviorally equivalent:

- `CRoxieServerReadAheadInput`
- Pipe-through/read
- `CRoxieThreadedConcatReader`
- `CRoxieServerPrefetchProjectActivity`
- Fetch
- Full keyed join
- Half-keyed join
- Two-input activity RHS pullers

Current puller creation/start locations are documented in:

- `threading-model.md`

### 3. Add an explicit readahead thread mode

Follow the existing `sinkMode` configuration pattern.

Add a readahead mode that can distinguish at least:

- Normal raw thread
- Pooled thread

The setting should be available at these scopes:

- Global topology configuration
- Workunit/debug configuration

Choose names consistent with existing Roxie configuration. For example:

- `readaheadMode="thread"`
- `readaheadMode="pooled"`

Use a separate `ReadaheadMode` enum rather than extending `SinkMode`. The missing/invalid value must resolve to the current raw-thread behavior.

Do not silently change all production queries to pooled readahead without an opt-in or measured default change.

### 4. Preserve puller cancellation semantics

A puller can block in several places:

- `ready.wait()`
- `space.wait()`
- `inputStream->nextRow()`
- Remote network dispatch
- Keyed-join buffering

The pooled version must preserve the current shutdown sequence:

1. Set the EOF/stop state.
2. Interrupt the relevant owner-specific semaphores or network operation.
3. Request the pooled worker to stop through `CPooledPersistent::stop()`. The callback only updates cooperative state and must not call `inputStream->stop()`.
4. Join the worker.
5. Call `inputStream->stop()` after the worker can no longer be executing `nextRow()`.
6. Re-throw or report any worker exception exactly as before.

Do not implement stop as merely setting a flag. A pooled worker may remain blocked indefinitely unless the existing semaphores/network operations are interrupted.

Do not make `requestAbort()` block. It only sets the concrete puller's EOF state and returns; the default `IThreaded::requestAbort()` returns false and is not sufficient.

The pooled wrapper must remain alive until `join()` completes because `CPooledPersistent` holds a non-owning owner pointer. Reject or serialize restart attempts while a previous pooled job is still active, and preserve the raw implementation's restartability after join.

### 5. Audit `SinkPooledThread`

Review the existing `SinkPooledThread` path in `CActivityGraph::doExecute`.

Verify:

- It uses the new per-start context inheritance correctly.
- Its name is passed to `CPooledPersistent`.
- Its `stop()`/abort behavior is correct.
- Exceptions are captured and rethrown through `join()` or the existing `noteException()` path, but not reported twice.
- It preserves `TaskScopeTracker` behavior.
- It does not retain query-owned state after `join()`.

Puller exceptions must continue to reach `IRecordPullerCallback::fireException()` exactly once. Sink exceptions must not be both recorded through `noteException()` and independently reported a second time by pooled `join()`.

Do not replace the existing `AutomaticPooled` implementation with a new task scheduler.

### 6. Decide how pooled sink mode is exposed

The existing `AutomaticPooled` mode already exists.

Initially:

- Keep the current mode available.
- Compare it against `Automatic`.
- Do not change the default until measurements are available.

If changing the default is later desired, that should be a separate commit.

The automatic sink grouping behavior must remain unchanged:

- Cheap/non-latency-sensitive sinks should continue to be folded into `CSequentialSinkSet`.
- Only sinks requiring parallelism should consume pooled workers.

### 7. Use the existing global elastic pool

Both sink and readahead workers should use the existing `GlobalThreadPool` through `CPooledPersistent`.

Do not add a bounded pool.

Do not use `ITaskScheduler`.

The reason is structural:

- A sink may wait for a puller.
- A puller may wait for its sink consumer.
- A sink may start child graphs.
- Child graphs may start additional sinks and pullers.

A bounded shared task scheduler can exhaust all workers with blocked tasks and deadlock. The current global pooled implementation is elastic (`defaultmax == 0`) and is therefore appropriate for this first migration.

### 8. Preserve thread-local context

Pooled execution must preserve:

- Job ID
- Logging context
- Trace flags
- Active OpenTelemetry span

Use the jlib `CPooledPersistent::start(bool inheritThreadContext)` path. Do not add manual context copying in Roxie unless the existing interfaces require it.

Verify that the pooled puller and pooled sink have the same logging and tracing context as their raw-thread equivalents.

### 9. Avoid unrelated optimization work

Do not include these in the Roxie change:

- Separate sink and readahead pools
- Bounded pool limits
- Work-stealing conversion
- Continuation/strand redesign
- jlib hash-map handle lookup
- jlib free-list redesign
- Global default sink-mode changes unless separately requested

This task is only about routing eligible Roxie sink and readahead workers through the existing global elastic pool.

## Testing Requirements

Add or update focused tests where practical.

At minimum, verify:

1. Existing raw-thread sink mode remains unchanged.
2. Existing pooled sink mode still executes and joins correctly.
3. A pooled sink exception reaches the graph error path once.
4. A pooled puller can start, produce rows, stop, and join.
5. A pooled puller blocked on readahead back-pressure can be stopped.
6. Inline `noThread` paths remain inline.
7. Thread names and logging context are preserved.
8. Repeated start/join cycles reuse pooled workers.
9. Nested graphs with both sinks and pullers complete without deadlock.
10. Multiple concurrent queries do not corrupt pooled wrapper state.

Run the narrow Roxie/unit tests available in the build. For stress-sensitive behavior, run repeated query executions and monitor:

- Total OS thread count
- Roxie query latency
- Pool high-water mark
- Pooled worker reuse
- Abort latency
- Exception and logging output

## Acceptance Criteria

The implementation is complete when:

- Every `RecordPullerThread` path can opt into `GlobalThreadPool`, while raw-thread and synchronous `noThread` behavior remain available.
- Existing pooled sink execution continues to work.
- Both pooled sinks and pooled pullers preserve context, names, exceptions, and abort semantics.
- No bounded task scheduler is used.
- No deadlock occurs when sinks and pullers block on each other.
- Raw-thread and inline execution modes remain available.
- Changes are limited to Roxie, except for compilation fixes that are genuinely required by the already-completed jlib API.