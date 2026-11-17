# WsSQL2 quick start

Run these commands from the repository root. The examples assume the build directory is `./build`, with WsSQL2 and CppUnit enabled.

## Build

```bash
cmake --build ./build --target ws_sql2 --parallel
```

The `duckdb-hpcc/` sources are compiled into `ws_sql2`; they do not produce a separate shared library.

## Deploy

Install the updated platform and restart the ESP instance hosting WsSQL2 (replace `myesp` with your ESP instance name):

```bash
cmake --build ./build --target install --parallel
sudo /etc/init.d/hpcc-init restart -c myesp
# as single command
cmake --build ./build --target install --parallel && sudo /etc/init.d/hpcc-init restart -c myesp
```

`ExecuteQuery` accepts exactly one read-only `SELECT` statement; a trailing semicolon is allowed. To run a script that contains several statements, call `SplitQuery` first and execute each `SELECT` separately; each one then gets its own independently pageable cursor. `SplitQuery` only parses `SqlText` with the same DuckDB parser that `ExecuteQuery` uses; it does not bind or execute anything, so it reads no logical files. It returns `Statements`, one string per statement in order: the original source, including comments, without surrounding whitespace or the terminating semicolon. Statements other than `SELECT` are returned rather than rejected; `ExecuteQuery` rejects them if they are run. Empty or whitespace-only text returns no statements. If any statement has a syntax error, the request fails.

`ExecuteQuery` parses, binds, and plans the query without running it, and returns an opaque `CursorId`, `InitMs`, `PrepareMs`, and (when requested) `ResultSetMetadata`. The query runs when the first `FetchResultSet` is called, so runtime errors are reported by that fetch. Each `FetchResultSet` returns a JSON row batch in `RowsJSON`, `ExecuteMs` (server-side time spent producing that batch), and `Complete`, which is `true` once the server has observed end-of-stream. `RowCount` on `FetchResultSet` is the requested batch size; zero (the default) uses the configured `defaultFetchSize` (1000 unless configured), and requests are capped at `maxFetchSize` (10000 by default). `maxByteLimit` caps each response batch in bytes (64 MiB by default); zero means unlimited. The service enforces `maxActiveCursors` (100) and `maxCursorsPerUser` (10), and expires idle cursors after `cursorIdleTimeoutSeconds` (300) or any cursor after `cursorMaxLifetimeSeconds` (3600).

A batch that fills exactly retains its cursor without a read-ahead; the next fetch may therefore be empty with `Complete` set. When a batch observes end-of-stream, the server releases the DuckDB query and its spill resources immediately, and the `CursorId` is no longer valid. `CloseResultSet` releases an unfinished cursor at any point, including while a fetch is running: the running query is interrupted and that fetch fails with error code `20001` (`QueryCancelled`). `CloseResultSet` never fails: `Closed` is `true` when the call released an open cursor, and `false` when there was nothing to close because the cursor already completed, failed, expired, or was closed, or the ID is unknown or owned by another user. Clients can therefore always call it during cleanup. A cursor that reaches `cursorMaxLifetimeSeconds` is cancelled the same way and an in-flight fetch fails with code `20002` (`QueryLifetimeExceeded`); both codes are declared by the `WsSQL2ErrorCode` enum in `ws_sql2.ecm`. The idle timeout restarts after each fetch that leaves the cursor open, and a cursor with a fetch in progress is never idle; the maximum lifetime counts from `ExecuteQuery` and never restarts. Each open cursor counts against the cursor limits until it completes or is closed. Cursor IDs are bound to the authenticated user, and concurrent fetches for one cursor are serialized.

All requests share one DuckDB instance and its spill directory. DuckDB's internal spill I/O is permitted there, but SQL file functions and SQL file writes are blocked, including within that directory. Configure `tempDirectory` on disk-backed storage accessible only to the ESP service account; the spill directory is not a per-user boundary.

Set `IncludeResultSetMetadata` to return a `ResultSetMetadata` entry for each output column in the `ExecuteQuery` response (the column list is known once the query is bound); each entry contains its JDBC-style column label (`ColumnLabel`), type name (`ColumnTypeName`), precision, and scale. Precision and scale are zero when unavailable. `IncludeRuntimeGraph` requests the runtime graph: DuckDB's profiled query tree as text (`RuntimeGraph`) and JSON (`RuntimeGraphJson`), with actual row counts and timings. DuckDB completes the profile at end-of-stream, so both are returned by the `FetchResultSet` that reports `Complete`. In the runtime graph, each `HPCC_SCAN` reports the `Keyed Filters` and `Post Filters` chosen when the scan started, so they include filters pushed down at runtime by joins; filters on the same KEY field are intersected into one keyed filter. `ExplainQuery` returns the optimized static plan, with estimates but no runtime figures, as text (`Plan`) and JSON (`PlanJson`), plus result-set metadata, without executing the query. HPCC-specific logical-file details remain under `LogicalFileMetadata`.

Set `IncludeDebug` on `ExecuteQuery` to have each `FetchResultSet` return a `DebugInfo` struct reporting memory usage and checking for leaks. Each `MemoryUsage` snapshot has DuckDB buffer-manager memory and spill (`DuckDBUsedBytes`, `DuckDBTempBytes`), the bytes allocated through malloc (`HeapInUseBytes`, from jemalloc when it is preloaded, otherwise glibc), and process resident memory (`ResidentBytes`, `PeakResidentBytes`). `Start` is taken in `ExecuteQuery` before the query's engine is created and `Current` after the batch is produced. Once end-of-stream is observed, the engine is released and `Released` is reported, along with `Retained` (`Released` minus `Start`). `LeakCheck` is then `Passed`, or `PossibleLeak` if DuckDB or heap memory grew by more than the `leakCheckThresholdBytes` service setting (default 1048576, 1 MiB). One-time initialization can cause growth on first use, so repeat the query to confirm. These figures are instance- and process-wide, so if any other query engine existed during the measurement window the result is `Inconclusive`. A cursor left open reports `Pending`; it carries the setting forward, and the final `FetchResultSet` returns the verdict.

Debug builds (`_DEBUG`) also count the `hpcc_scan` objects each engine creates and destroys: bind data, global states and local states. These counts are returned as `ScanObjectsCreated` and `ScanObjectsLive`. The counts belong to the query's engine, so they are exact even when other queries run concurrently. If any of those objects is still live after the engine is released, `LeakCheck` is `Leaked`. Release builds omit these fields and use only the memory check.

`LogicalFileMetadata` accepts a logical file name and returns a `LogicalFileMetadata` struct. Its fields include the ECL record, physical parts (`FileParts`), the total `RecordCount` when Dali has it (otherwise the field is omitted), and each top-level scan option; `FormatOptions` and `ProviderOptions` contain JSON objects. When `RecordCount` is known, `SELECT COUNT(*) FROM hpcc_scan('logical-name')` is answered from metadata without reading file parts. Filtered counts and scans without a known count still execute normally. A client can also use the count returned by `LogicalFileMetadata` to answer an unfiltered `COUNT(*)` without fetching rows. `SampleSQL` contains a query with the record, parts, and options embedded in a long-form `hpcc_scan(record, parts, options)` call for use by a host that permits the long form; it cannot be executed through WsSQL2. The short-form scan and `LogicalFileMetadata` reject external and foreign logical names, including compound names containing either, before Dali lookup. The long form reads supplied physical parts without Dali file authorization and is therefore disabled in WsSQL2; it remains available to other hosts of the DuckDB extension. Record layouts that require a compiled helper are not supported in this release.

## Decimal fields

HPCC `DECIMAL` and `UDECIMAL` fields support widths from 1 through 38, preserving their declared precision and scale. Wider HPCC decimal fields and scales greater than the width are rejected during query binding (`ExecuteQuery` or `ExplainQuery`), before a cursor or result-set metadata is returned.

## Run C++ unit tests

```bash
./build/Debug/bin/unittests WsSql2TypeMappingTests WsSql2MetadataTests WsSql2QueryEngineTests -v
```

## Run Python tests

Both scripts test a running WsSQL2 service (default endpoint `http://127.0.0.1:8510/WsSQL2/`) and require the relevant `regress::*` logical files to be available. Deploy/start the service and ensure the regression files are present before running them. The `dg_var` layout is currently expected to return an unsupported-layout error.

```bash
./esp/services/ws_sql2/tests/sql-test --pq 10
./esp/services/ws_sql2/tests/read-regress.py
```

The SQL test runner reads its connection settings from `tests/sql-test.json` and discovers SQL cases from the `sql/` and `sql-*/` directories:

- `sql/` compares query results with JSON files under `sql/ExecuteQuery/`.
- `sql-xfail/` checks that the query fails (in `ExecuteQuery` or its first fetch) with the error text specified in `ExecuteQuery/`.
- `sql-cursor/` checks fetch batches, cursor lifetime, and explicit close behavior, and cancelling a running first fetch with `CloseResultSet`, using assertions under `Cursor/`.
- `sql-equivalence/` compares the results of an optimized query with a semantically equivalent residual-filter query, using assertions under `Compare/`.
- `sql-metrics/` checks query rows and required runtime-graph fields under `ExecuteQuery/`.
- `sql-explain/` checks optimized-plan text and `HPCC_SCAN` details returned by `ExplainQuery`, using assertions under `ExplainQuery/`.
- `sql-split/` checks the statement text that `SplitQuery` returns for a multi-statement script, or its expected parse error, using assertions under `SplitQuery/`. These cases parse SQL without executing it.
- `sql-ecl/` pairs each `.sql` file with a same-named `.ecl` file, runs SQL through WsSQL2 and ECL through `ecl run`, and compares their results' columns (case-insensitively, as ECL lower-cases field names), row order, and values. The SQL file may hold several statements: the runner splits it with `SplitQuery`, runs each statement in turn, and compares the Nth statement's result with the ECL workunit's Nth output Dataset. It reports every mismatched or failed result and any difference in result counts, rather than stopping at the first. Drawing on ECL regression cases, the suite covers ranges, filters, sorts, grouped counts, index and KEY reads (`dg_index`, `dg_keyed_index`, `searchindex`), plus inner, left outer, and anti joins over `regress::multi` files. The `key_*` cases check `searchindex` key pushdown — keyed and payload filters, out-of-range and contradictory bounds, ordered limits, and distinct-key joins — against the equivalent ECL index reads. The `search_source_word_counts`, `search_index_word_counts`, and `search_source_key_doc_join` cases aggregate the multi-million-row `searchsource` and `searchindex` files before returning bounded results; they take longer than the small-file cases. Each PASS line reports, in aligned columns, the SQL `ExecuteMs` summed over every `FetchResultSet` response (server-side execution, excluding HTTP and query preparation) and the ECL workunit's `TotalClusterTime` from `WsWorkunits/WUQuery` as `ECL <time>`, with `<`, `>`, or `=` between them showing whether SQL was faster, slower, or equal to the millisecond, followed in parentheses by end-to-end wall-clock times for SQL (the HTTP request, preparation, and fetching every result batch) and ECL (including compilation and result retrieval), again with `<`, `>`, or `=` between them.

The KEY scan plan cases that declare `scanPartsFromLogicalFile` call `LogicalFileMetadata` to derive their expected physical-part and data-part/thread counts (excluding any top-level KEY part). LIMIT/OFFSET pushdown expectations follow the resulting data-part count rather than assuming a fixed layout.

`--updateResults` refreshes result snapshots for `sql/` and `sql-metrics/`; it does not rewrite the behavioral assertions in the other suites. Use `--pq N` to run SQL cases concurrently without a runner-imposed cap; requesting more parallel queries than the service allows can fail the tests. Override the endpoint with `WS_SQL2_ENDPOINT` or `--server`; use `--config` to provide a different SQL test config file. The runner prints and flushes a `START` line when each case begins, then reports its `PASS` or `FAIL` status and elapsed time when it completes. Parallel cases are reported in completion order, so a started case without a completion line is still running. The runner prints a summary and exits non-zero if any case fails. The catalog test accepts `--endpoint`, `--dfu-endpoint` (default `http://localhost:8010/WsDfu/DFUQuery.json`), `--scope`, and other options; run it with `--help` for details. It gets logical-file names from WsDFU. Only when files fail does it write a JSON report listing those failures (`read-regress-errors.json` by default, override with `--report`), and a passing run deletes any stale report.

For `sql-ecl/`, `ecl` must be on `PATH` and the configured ECL service must be available. `sql-test.json` supplies `espIp`, `espSocket`, `eclTarget`, and optionally `eclCluster`; `--eclTarget` and `--eclCluster` override the target and cluster. The SQL and ECL operations use the configured test user. Once the ECL run returns a WUID, the runner deletes that workunit via `WUDelete` after comparing results and retrieving the cluster time, including when a case fails. Use an ECL `OUTPUT(..., NAMED('Result'))` with field names and ordering matching the SQL projection; ECL XML values are compared to SQL JSON values using SQL's boolean/numeric types.

Run only the SQL/ECL comparisons with `./esp/services/ws_sql2/tests/sql-test --suite sql-ecl`; add a SQL filename after the option to run one paired case. The `--suite` option works with the other suite directories as well.