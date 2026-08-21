# DFS FileAccess Audit Centralization

## Purpose

This page documents the DFS-owned FileAccess auditing model introduced for HPCC-35853.

The design goal is to move FileAccess emission responsibility into DFS so callers pass context and intent, while DFS decides:

- whether a call should emit an audit line,
- which verb to emit,
- what DFS-resolved enrichment fields to include.

## Record format

DFS emits a CSV-prefix plus logfmt-extras hybrid line:

``,FileAccess,<component>,<action>[,<logfmt extras>]``

The format is produced by `DFSAuditContext::buildFileAccessAuditLine()` and emitted by `DFSAuditContext::logFileAccess()`.

Current policy is to keep this format (no JSON migration in this change set).

## DFSAuditContext contract

`DFSAuditContext` is a key/value context bag carried through DFS APIs.

Core methods:

- `add()` returns a new context with extra key/value pairs
- `nested()` returns a copy marked nested
- `isNested()` reports whether nested suppression is active
- `setValue()` update keys
- `toLogfmt()` and `fromLogfmt()` serialize/deserialize wire context

### Process defaults

`initDFSAudit()` installs default process context fields from config:

- `component` (from component config tag)
- `instance` (from process name)

Callers should still provide operation-scoped fields (user, wuid, lfn, etc).

### Nested suppression protocol

Nested suppression prevents duplicate emits from inner DFS calls.

Rule:

- outer owner call uses normal context and may emit,
- inner DFS calls receive `auditCtx.nested()` and must not emit.

Every emit point must guard on:

`if (!auditCtx.isNested()) { ... emit ... }`

## AccessMode gating and verb mapping

FileAccess emission is gated to content access, not metadata-only access.

Content gate:

`AccessMode::none == (accessMode & AccessMode::meta)`

Verb mapping for lookup:

- `READ` for content lookups
- `EXTEND` when `AccessMode::extend` bit is set (for append/extend intent)

Metadata-only access modes (for example `readMeta`, `writeMeta`) do not emit READ/EXTEND.

## DFS emission owner points

Primary emit points in DFS:

- `lookup(...)` emits `READ`/`EXTEND` for content access
- `attach(...)` emits `CREATED`
- `detach(...)` / remove-entry path emits `DELETED`
- logical rename and renamePhysical paths emit `RENAMED`

### Superfile lookup behavior

For content reads, superfile lookup emits:

- one record for the superfile lookup,
- one record per leaf subfile resolved.

## Enrichment fields

DFS enriches records with fields callers may not reliably know:

- `lfn` (target logical file)
- `lfn2` (source logical file for rename-style operations)
- `cluster` when unambiguous
- `fileSize` and `diskSize` where available via metadata-only size paths

Size fields use current vocabulary:

- `fileSize`
- `diskSize`

## Remote and foreign boundary behavior

Current policy for remote/foreign content lookup is dual emission:

- local/client side may emit,
- server/service side may emit.

Records should remain distinguishable by `component` and context fields.

## Migration pattern for callers

When migrating a caller component:

1. Build a base `DFSAuditContext` once per request/job (component, user, wuid, peer, instance-specific data).
2. Add per-call fields (`lfn`, `lfn2`, `graph`, `cluster`, `jobid`, etc) via `add()` or `setValue*()`.
3. Pass the context into DFS API calls (`lookup`, `attach`, `detach`, `removeEntry`, `rename`, `renamePhysical`).
4. Remove direct `LOG(MCauditInfo, ",FileAccess,...")` or equivalent caller-side emit.
5. Use `nested()` when the caller intentionally keeps ownership of an operation-level verb and must suppress inner DFS emits.

## Known exceptions and operation-specific notes

Some operations do not map cleanly to simple DFS create/delete/rename/read owner points. In those cases, retain explicit operation-level policy and document it in code/PR notes.

Examples currently handled with explicit policy include:

- DFU operation verbs such as `MOVE` and `REPLICATE`
- COPYENSURE physical-attach path represented as `ATTACH`

## Guidance for log consumers

Consumers should treat these records as the stable source of DFS file access events and parse:

- fixed CSV prefix fields (`FileAccess`, component, action),
- remaining extras as logfmt key/value data.

Consumers should not assume all optional fields exist on every line. Field presence depends on operation type and available DFS metadata.
