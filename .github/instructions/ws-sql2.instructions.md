---
applyTo: 'esp/services/ws_sql2/**'
---

# Instructions for the WsSQL2 Service

## Overview

- WsSQL2 is an ESP service that executes SQL against HPCC logical files.
- The DuckDB `hpcc_scan` table function and type mapping live in `duckdb-hpcc/`; its public surface is `duckdb-hpcc/extension.hpp`. `extension.cpp` owns scan binding and execution orchestration, `optimizer.cpp` owns filter/limit pushdown and explain descriptions, and `materialization.cpp` converts HPCC rows to DuckDB vectors and maps runtime record metadata to field descriptors. Their declarations are separated into `optimizer.hpp` and `materialization.hpp`; `hpcc_scan.hpp` holds scan state shared by the modules. `typemapping.*` maps serialized HPCC record metadata and `ITypeInfo` directly to DuckDB `LogicalType` values, including numeric decimal width/scale and nested child types. Field descriptors retain paths, children, and fallback metadata, not source or target type strings; SQL schema text is generated from the logical types only for display. It does not parse ECL record-definition strings.
- The WsSQL2 service owns the short-form `hpcc_scan(logicalName)` metadata expansion (`metadata.*`) and the query engine (`engine.*`). The extension resolves the logical file with the request user's Dali credentials through the `ScanMetadataProvider` interface.
- Both serialized-metadata and runtime-metadata adapters use `mapScalarFieldType` in `typemapping.*` for scalar DuckDB types and fallback flags; metadata extraction, nested traversal, and warning emission remain adapter-specific.
- The ESP service layer (`service.*`, `plugin.cpp`) owns request handling, configuration, cursor lifecycle and request limits; `engine.*` owns DuckDB query execution and row-batch serialization.

## Security Model

- The ESP service owns a single in-memory `CSharedQueryDatabase`, built once at `init()` with external file access disabled and the configuration locked, so the configured `memoryLimit`/`threads`/`maxTempDirectorySize` bound the whole service instead of being multiplied per request. Each request creates its own `CQueryEngine`, which attaches its own DuckDB `Connection` to that shared instance (concurrent connections into one DuckDB instance are safe). Cursor-backed query execution accepts exactly one `SELECT` statement. `ExecuteQuery` only prepares it (returning the `CursorId` and metadata); the first `FetchResultSet` runs it, and `CloseResultSet` or the cursor lifetime limit can interrupt a running fetch, which then fails with a `WsSQL2ErrorCode` (`QueryCancelled` 20001, `QueryLifetimeExceeded` 20002). `SplitQuery` only parses a multi-statement script into its statement strings so clients can execute each `SELECT` separately. `CQueryEngine` also has standalone constructors (each owning a private DuckDB instance) used only by unit tests that validate per-instance configuration in isolation.
- All connections share the one spill subdirectory of `tempDirectory`, which is the only directory DuckDB may access. `tempDirectory` must point at genuine disk-backed storage in production: a tmpfs-backed path defeats spilling as a way to relieve memory pressure.
- An unfinished result cursor retains its engine until exhaustion, explicit close, idle timeout, or maximum lifetime. Cursor IDs are unpredictable and bound to the authenticated user; global and per-user cursor limits apply.
- The long form `hpcc_scan(record, parts, options)` reads caller-supplied physical paths without any Dali authorization check, so the WsSQL2 service path always rejects it. `LogicalFileMetadata` resolves a logical file with Dali authorization and returns the inputs needed for the long-form call.
- Results are streamed in row batches. A `FetchResultSet` `FetchSize` of 0 uses the configured `defaultFetchSize`; all batch sizes are capped by `maxFetchSize` and `maxByteLimit`.
- In addition to these instructions, follow the [ESP services instructions](esp-services.instructions.md) and the [C++ instructions](cpp.instructions.md).

## Keep the DuckDB Extension Well Encapsulated

Keep the DuckDB extension in `duckdb-hpcc` self-contained:

- Keep long-form table-function execution, filter/limit pushdown, optimization, and scan code inside `duckdb-hpcc`.
- Keep logical-file lookup and conversion to long-form scan metadata in `ws_sql2`. Pass the resulting record definition, parts, and options through the explicit scan-metadata provider interface; the extension must not resolve Dali files itself.
- Do not make the extension depend on ESP types (`IEspContext`, generated ESDL request/response classes, SOAP bindings, etc.).
- Do not let the ESP service reach into extension internals (bind data, scan state, pushdown helpers).
- Keep the extension's public surface in `extension.hpp` minimal; prefer `static` functions or anonymous namespaces for internals.
- New extension behaviour should be testable without a running ESP (for example via the long-form `hpcc_scan(record, parts, options)` overload with `CEngineOptions::allowLongFormScan`, as in `pushdowntests.cpp`).

## References

- When changing file access or scan behavior, inspect existing HPCC readers before designing the read path. Compare their handling of local versus remote reads, retries/failover, compression, record boundaries, and file-part metadata:
  - DAFS remote file server: `fs/dafilesrv/dafilesrv.cpp` and `fs/dafsserver/dafsserver.cpp`.
  - Roxie disk reads: `roxie/ccd/ccdactivities.cpp` and `roxie/ccd/ccdfile.cpp` (`IDirectReader` and file resolution).
  - FileView2 disk and index reads: `common/fileview2/fvdisksource.cpp` and `common/fileview2/fvidxsource.cpp`; `ecl/regress/fileview2.ecl` covers related behavior.
  - Thor disk reads: `thorlcr/activities/diskread/thdiskreadslave.cpp`, along with shared disk-read handling in `thorlcr/activities/thdiskbase.cpp` and `thorlcr/activities/thdiskbaseslave.cpp`.
  - Prefer established HPCC file and remote-reader APIs over implementing a separate low-level read path; preserve WsSQL2's authorization and scan-metadata boundaries.
- DuckDB extension documentation: [Extensions – DuckDB](https://duckdb.org/docs/current/extensions/overview)
- Reference implementation for a similar flat-file scan extension: [duckdb/duckdb-avro](https://github.com/duckdb/duckdb-avro). Prefer its patterns for table-function binding, projection/filter pushdown, multi-file/partitioned scanning and type mapping.

## Build and Deploy

Build the platform and restart the ESP that hosts WsSQL2:

```bash
cmake --build ./build --target install --parallel && sudo /etc/init.d/hpcc-init restart -c myesp
```

## Testing

Run the unit tests from the build's `bin` directory:

```bash
./unittests WsSql2TypeMappingTests WsSql2MetadataTests WsSql2QueryEngineTests -v
```

Run the catalog tests against the deployed service:

```bash
./esp/services/ws_sql2/tests/read-regress.py
```

- The script runs bounded `hpcc_scan` queries across the `regress::*` logical files. Only when files fail does it write a JSON report listing those failures (`read-regress-errors.json` by default), and a passing run deletes any stale report. Use `--help` for options such as `--endpoint`, `--scope` and `--limit`.
- The catalog tests call `LogicalFileMetadata`, `ExecuteQuery`, and `FetchResultSet`; they also compare authorized logical-file scans with the long-form `hpcc_scan` overload.
- `dg_var` is expected to fail; any other new failure should be treated as a regression.
