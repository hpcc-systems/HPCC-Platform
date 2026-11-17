#ifndef _ESP_WS_SQL2_ENGINE_HPP_
#define _ESP_WS_SQL2_ENGINE_HPP_

#include "jlib.hpp"
#include "duckdb.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

interface IUserDescriptor;

namespace duckdb
{
namespace hpcc
{
class ScanMetadataProvider;
#ifdef _DEBUG
struct ScanObjectCounters;
#endif
} // namespace hpcc
} // namespace duckdb

#ifdef _DEBUG
struct CEngineScanObjectCounts
{
    unsigned __int64 bindData = 0;
    unsigned __int64 globalStates = 0;
    unsigned __int64 localStates = 0;

    unsigned __int64 total() const { return bindData + globalStates + localStates; }
};

// Debug builds only: the hpcc_scan objects created through one engine's connection. It outlives the engine, so
// the host can confirm that every object was destroyed along with it, regardless of other concurrent engines.
class CScanObjectAudit
{
    std::shared_ptr<const duckdb::hpcc::ScanObjectCounters> counters;

public:
    CScanObjectAudit() = default;
    explicit CScanObjectAudit(std::shared_ptr<const duckdb::hpcc::ScanObjectCounters> _counters) : counters(std::move(_counters)) {}

    bool isValid() const { return counters != nullptr; }
    CEngineScanObjectCounts queryCreated() const;
    CEngineScanObjectCounts queryLive() const;
};
#endif

struct CQueryResultColumnMetadata
{
    std::string name;
    std::string typeName;
    int precision = 0;
    int scale = 0;
};

struct CQueryTimings
{
    // Parse, bind (including hpcc_scan metadata resolution) and optimize.
    int64_t prepareNs = 0;
    // Execute, including fetching and serializing the (streamed) result of the last statement.
    int64_t executeNs = 0;
};

// Point-in-time memory usage. The DuckDB figures cover the whole (possibly shared) instance and the heap/resident
// figures cover the whole process, so they include any concurrent requests; liveEngines/createdEngines let callers
// tell whether a measurement window was isolated from other engines.
struct CEngineMemoryUsage
{
    unsigned __int64 duckdbUsedBytes = 0;
    unsigned __int64 duckdbTempBytes = 0;
    // Bytes allocated through malloc and not yet freed; 0 when heapAvailable is false.
    unsigned __int64 heapInUseBytes = 0;
    bool heapAvailable = false;
    unsigned __int64 residentBytes = 0;
    unsigned __int64 peakResidentBytes = 0;
    unsigned liveEngines = 0;
    unsigned __int64 createdEngines = 0;
};

struct CEngineOptions
{
    // Empty or zero values keep DuckDB's defaults (80% of system memory, all cores, unlimited spill size). Since
    // CSharedQueryDatabase applies these to a single instance shared by every concurrent request, they bound the
    // whole service rather than being multiplied per request.
    std::string memoryLimit;
    unsigned threads = 0;
    std::string maxTempDirectorySize;
    // Parent of the engine's spill directory (default ".tmp" relative to the working directory). Each DuckDB
    // instance (one per standalone CQueryEngine, or one shared CSharedQueryDatabase) spills into its own uniquely
    // named subdirectory, which is the only location DuckDB may access, and which DuckDB removes when the owning
    // instance is destroyed.
    std::string tempDirectory;
    // Allow the long form hpcc_scan(record, parts, options), which reads caller-supplied physical files without any
    // Dali authorization check. This is service-wide and does not distinguish callers; only enable it if untrusted
    // callers cannot access the service or deployment.
    bool allowLongFormScan = false;
};

// A single DuckDB instance (and its configured memory_limit/threads/spill-size budget) shared by every concurrent
// request, instead of each request creating its own instance with its own budget. duckdb::Connection is safe to use
// concurrently from multiple threads as long as each thread uses its own Connection into a shared duckdb::DuckDB, so
// CQueryEngine still creates one Connection per request; only the underlying buffer manager and thread pool (and
// therefore the memory/CPU budget) are shared.
class CSharedQueryDatabase
{
    // Must be declared before database: it supplies the database's restricted configuration.
    duckdb::DBConfig config;
    std::string tempDirectory;
    duckdb::DuckDB database;

public:
    explicit CSharedQueryDatabase(const CEngineOptions& options);

    duckdb::DuckDB& queryDatabase() { return database; }
    const std::string& queryTempDirectory() const { return tempDirectory; }
    CEngineMemoryUsage captureMemoryUsage();
};

class CQueryEngine
{
    // Must be declared before ownedDatabase: it supplies the database's restricted configuration. Unused when this
    // engine attaches to an externally owned CSharedQueryDatabase instead.
    duckdb::DBConfig config;
    std::string tempDirectory;
    // Non-null only when this engine owns a private DuckDB instance (standalone construction, used by unit tests).
    // Null when attached to a CSharedQueryDatabase, in which case database references that shared instance instead.
    duckdb::unique_ptr<duckdb::DuckDB> ownedDatabase;
    duckdb::DuckDB& database;
    duckdb::Connection connection;
    // A prepared cursor query that has not yet executed; the first fetchCursorBatch executes it into cursorResult.
    duckdb::unique_ptr<duckdb::PendingQueryResult> cursorPending;
    duckdb::unique_ptr<duckdb::QueryResult> cursorResult;
    duckdb::unique_ptr<duckdb::DataChunk> cursorChunk;
    duckdb::idx_t cursorChunkRow = 0;
    bool cursorCaptureRuntimeGraph = false;
    // DuckDB clears its own interrupt flag when the next statement begins, so remember it across statements.
    std::atomic<bool> interruptRequested{false};
#ifdef _DEBUG
    std::shared_ptr<duckdb::hpcc::ScanObjectCounters> scanObjectCounters;
#endif

    void registerScanMetadataProvider(duckdb::shared_ptr<duckdb::hpcc::ScanMetadataProvider> provider);

public:
    // Standalone construction: this engine owns a private DuckDB instance for its exclusive use. Used by unit tests
    // that need to validate configuration (memory/thread limits, spill isolation, blocked statements) in isolation.
    CQueryEngine();
    explicit CQueryEngine(IUserDescriptor* requestUser);
    CQueryEngine(IUserDescriptor* requestUser, const CEngineOptions& options);
    // Shared construction: this engine attaches its own Connection to an already-running, externally owned
    // CSharedQueryDatabase. Used by the ESP service so concurrent requests share one memory/thread budget; long-form
    // scans remain disabled on this path.
    CQueryEngine(CSharedQueryDatabase& sharedDatabase, IUserDescriptor* requestUser);
    ~CQueryEngine();

#ifdef _USE_CPPUNIT
    void setTestScanMetadata(const char* recordDefinition, const char* parts, const char* options, bool hasRecordCount, uint64_t recordCount);
#endif

    bool isInitialized();
    const std::string& queryTempDirectory() const { return tempDirectory; }
    CEngineMemoryUsage captureMemoryUsage();
#ifdef _DEBUG
    CScanObjectAudit queryScanObjectAudit() const { return CScanObjectAudit(scanObjectCounters); }
#endif
    // Runs the statements in order and returns the last statement's result as JSON. Results from earlier SELECT
    // statements are discarded. Only table-level DDL/DML statements are allowed. A rowLimit or byteLimit of 0 is
    // unlimited.
    bool executeQuery(const char* sql, unsigned rowLimit, unsigned byteLimit, std::string& resultText, std::string& errorMessage, CQueryTimings* timings = nullptr, std::string* runtimeGraphJson = nullptr, std::string* runtimeGraph = nullptr, std::vector<CQueryResultColumnMetadata>* resultMetadata = nullptr);
    // Parses (without binding or executing) the text into its statements, in order. Each is the original source text,
    // including comments, without surrounding whitespace or the terminating semicolon.
    bool splitQuery(const char* sql, std::vector<std::string>& statements, std::string& errorMessage);
    bool explainQuery(const char* sql, std::string& planText, std::string& planJson, StringArray& projectedColumns, StringArray& residualFilters, std::vector<CQueryResultColumnMetadata>& resultMetadata, std::string& errorMessage);
    // Parses, binds and plans exactly one SELECT statement without executing it, so the result metadata is available
    // and the query can be interrupted before any rows are produced. Only timings->prepareTime is recorded.
    bool startCursorQuery(const char* sql, bool captureRuntimeGraph, std::vector<CQueryResultColumnMetadata>* resultMetadata, std::string& errorMessage, CQueryTimings* timings = nullptr);
    // The first call executes the prepared query; blocking operators (aggregates, sorts, ...) run to completion
    // before it returns its first batch.
    bool fetchCursorBatch(unsigned fetchSize, unsigned byteLimit, std::string& rowsJson, bool& complete, std::string& errorMessage, std::string* runtimeGraphJson = nullptr, std::string* runtimeGraph = nullptr, unsigned* rowsReturned = nullptr);
    // Safe to call from another thread while a cursor query is being prepared or executing; the in-flight and any
    // later cursor operation fails.
    void interrupt();
};

#endif