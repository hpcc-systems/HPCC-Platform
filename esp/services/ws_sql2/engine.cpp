#include "engine.hpp"
#include "duckdb-hpcc/extension.hpp"
#include "metadata.hpp"

#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "jfile.hpp"
#include "fileview.hpp"
#include "jdebug.hpp"
#include "seclib.hpp"
#include "thorcommon.hpp"
#include "thorread.hpp"
#include "jutil.hpp"

#include <atomic>
#include <cctype>
#include <random>
#include <thread>
#ifdef __GLIBC__
#include <malloc.h>
#endif
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_error.h>
#include <sys/resource.h>
#endif
#ifndef _WIN32
#include <dlfcn.h>
#endif

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "daclient.hpp"
#include "rtlfield.hpp"

void runWsSql2PushdownTestSuite();
#endif

using duckdb::hpcc::Extension;
using duckdb::hpcc::ScanMetadata;
using duckdb::hpcc::ScanMetadataProvider;

// DuckDB routes both SQL file functions and internal spill I/O through the database's filesystem. Its allowed
// directories cannot distinguish the two, but the former pass a client opener and the latter a database opener.
class CRestrictedLocalFileSystem final : public duckdb::LocalFileSystem
{
    static void rejectClient(duckdb::optional_ptr<duckdb::FileOpener> opener)
    {
        if (duckdb::FileOpener::TryGetClientContext(opener))
            throw duckdb::PermissionException("SQL file access is disabled");
    }

public:
    duckdb::unique_ptr<duckdb::FileHandle> OpenFile(const duckdb::string& path, duckdb::FileOpenFlags flags, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::OpenFile(path, flags, opener);
    }

    bool DirectoryExists(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::DirectoryExists(path, opener);
    }

    void CreateDirectory(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        LocalFileSystem::CreateDirectory(path, opener);
    }

    void RemoveDirectory(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        LocalFileSystem::RemoveDirectory(path, opener);
    }

    void MoveFile(const duckdb::string& source, const duckdb::string& target, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        LocalFileSystem::MoveFile(source, target, opener);
    }

    bool FileExists(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::FileExists(path, opener);
    }

    bool IsPipe(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::IsPipe(path, opener);
    }

    void RemoveFile(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        LocalFileSystem::RemoveFile(path, opener);
    }

    bool ListFiles(const duckdb::string& path, const std::function<void(const duckdb::string&, bool)>& callback, duckdb::FileOpener* opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::ListFiles(path, callback, opener);
    }

    duckdb::vector<duckdb::OpenFileInfo> Glob(const duckdb::string& path, duckdb::FileOpener* opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::Glob(path, opener);
    }

    duckdb::string CanonicalizePath(const duckdb::string& path, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::CanonicalizePath(path, opener);
    }

protected:
    bool ListFilesExtended(const duckdb::string& path, const std::function<void(duckdb::OpenFileInfo&)>& callback, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::ListFilesExtended(path, callback, opener);
    }

    duckdb::unique_ptr<duckdb::MultiFileList> GlobFilesExtended(const duckdb::string& path, const duckdb::FileGlobInput& input, duckdb::optional_ptr<duckdb::FileOpener> opener) override
    {
        rejectClient(opener);
        return LocalFileSystem::GlobFilesExtended(path, input, opener);
    }
};

// Used both for a standalone CQueryEngine's own private spill directory and for a CSharedQueryDatabase's one spill
// directory shared by every engine attached to it; either way, the leaf directory is unique per DuckDB instance.
static std::string createPrivateTempDirectory(const CEngineOptions& options)
{
    static std::atomic<unsigned> nextEngineId{ 0 };
    std::string parent = options.tempDirectory.empty() ? ".tmp" : options.tempDirectory;
    // DuckDB only creates (and later removes) the leaf directory, so the shared parent must already exist.
    if (!recursiveCreateDirectory(parent.c_str()))
        throw makeStringExceptionV(-1, "Could not create ws_sql2 temp directory %s", parent.c_str());
    if (parent.back() != PATHSEPCHAR)
        parent.push_back(PATHSEPCHAR);
    return parent + "ws_sql2_" + std::to_string(GetCurrentProcessId()) + "_" + std::to_string(++nextEngineId);
}

static duckdb::DBConfig* prepareRestrictedConfig(duckdb::DBConfig& config, const std::string& tempDirectory, const CEngineOptions& options)
{
    if (!options.memoryLimit.empty())
        config.SetOptionByName("memory_limit", duckdb::Value(options.memoryLimit));
    if (options.threads)
        config.SetOptionByName("threads", duckdb::Value::BIGINT(options.threads));
    if (!options.maxTempDirectorySize.empty())
        config.SetOptionByName("max_temp_directory_size", duckdb::Value(options.maxTempDirectorySize));
    config.SetOptionByName("temp_directory", duckdb::Value(tempDirectory));
    // An in-memory database cannot use access_mode=READ_ONLY. DuckDB requires the spill directory to be allow-listed
    // even for internal I/O, so reject client file operations independently of that allow-list.
    config.SetOptionByName("enable_external_access", duckdb::Value::BOOLEAN(false));
    // Never download or implicitly load DuckDB extensions; only statically linked ones are available.
    config.SetOptionByName("autoinstall_known_extensions", duckdb::Value::BOOLEAN(false));
    config.SetOptionByName("autoload_known_extensions", duckdb::Value::BOOLEAN(false));
    config.file_system = duckdb::make_uniq<duckdb::VirtualFileSystem>(duckdb::make_uniq<CRestrictedLocalFileSystem>());
    config.AddAllowedDirectory(tempDirectory);
    config.SetOptionByName("lock_configuration", duckdb::Value::BOOLEAN(true));
    return &config;
}

static void appendJsonString(std::string& output, const std::string& value)
{
    StringBuffer encoded;
    encodeJSON(encoded, static_cast<unsigned>(value.size()), value.data());
    output.push_back('"');
    output.append(encoded.str(), encoded.length());
    output.push_back('"');
}

static void appendDuckDBValue(StringBuffer& output, const char* name, const duckdb::Value& value)
{
    if (name && !*name)
    {
        delimitJSON(output).append("\"\": ");
        appendDuckDBValue(output, nullptr, value);
        return;
    }

    if (value.IsNull())
    {
        appendJSONNameOrDelimit(output, name).append("null");
        return;
    }

    const auto type = value.type().id();
    if (type == duckdb::LogicalTypeId::LIST || type == duckdb::LogicalTypeId::ARRAY)
    {
        appendJSONNameOrDelimit(output, name).append('[');
        const auto& children = type == duckdb::LogicalTypeId::LIST ? duckdb::ListValue::GetChildren(value) : duckdb::ArrayValue::GetChildren(value);
        for (const auto& child : children)
            appendDuckDBValue(output, nullptr, child);
        output.append(']');
        return;
    }
    if (type == duckdb::LogicalTypeId::STRUCT)
    {
        appendJSONNameOrDelimit(output, name).append('{');
        const auto& children = duckdb::StructValue::GetChildren(value);
        for (duckdb::idx_t index = 0; index < children.size(); ++index)
            appendDuckDBValue(output, duckdb::StructType::GetChildName(value.type(), index).c_str(), children[index]);
        output.append('}');
        return;
    }
    if (type == duckdb::LogicalTypeId::MAP)
    {
        appendJSONNameOrDelimit(output, name).append('{');
        for (const auto& entry : duckdb::MapValue::GetChildren(value))
        {
            const auto& pair = duckdb::StructValue::GetChildren(entry);
            if (pair.size() == 2 && !pair[0].IsNull())
                appendDuckDBValue(output, pair[0].ToString().c_str(), pair[1]);
        }
        output.append('}');
        return;
    }

    if (type == duckdb::LogicalTypeId::BLOB)
    {
        const std::string& data = duckdb::StringValue::Get(value);
        appendJSONDataValue(output, name, static_cast<unsigned>(data.size()), data.data());
        return;
    }
    if (type == duckdb::LogicalTypeId::BOOLEAN)
    {
        appendJSONValue(output, name, value.GetValue<bool>());
        return;
    }
    if (type == duckdb::LogicalTypeId::FLOAT)
    {
        appendJSONRealValue(output, name, duckdb::FloatValue::Get(value));
        return;
    }
    if (type == duckdb::LogicalTypeId::DOUBLE)
    {
        appendJSONRealValue(output, name, duckdb::DoubleValue::Get(value));
        return;
    }
    switch (type)
    {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UHUGEINT:
    case duckdb::LogicalTypeId::DECIMAL:
        appendJSONNameOrDelimit(output, name).append(value.ToString());
        return;
    default:
        break;
    }

    // Anything not known to render as a JSON number (text, temporal, enum, bit, union, ...) is quoted.
    const std::string text = value.ToString();
    appendJSONStringValue(output, name, static_cast<unsigned>(text.size()), text.data(), true);
}

static std::string resultJsonSuffix(unsigned __int64 rowCount, unsigned __int64 returnedRowCount, bool truncated)
{
    return "],\"rowCount\":" + std::to_string(rowCount) + ",\"returnedRowCount\":" + std::to_string(returnedRowCount) + ",\"truncated\":" + (truncated ? "true}" : "false}");
}

static void extractResultMetadata(const duckdb::vector<std::string>& names, const duckdb::vector<duckdb::LogicalType>& types, std::vector<CQueryResultColumnMetadata>& resultMetadata)
{
    resultMetadata.clear();
    resultMetadata.reserve(names.size());
    for (duckdb::idx_t index = 0; index < names.size(); ++index)
    {
        CQueryResultColumnMetadata column;
        column.name = names[index];
        column.typeName = types[index].ToString();
        if (types[index].id() == duckdb::LogicalTypeId::DECIMAL)
        {
            column.precision = duckdb::DecimalType::GetWidth(types[index]);
            column.scale = duckdb::DecimalType::GetScale(types[index]);
        }
        resultMetadata.emplace_back(std::move(column));
    }
}

// Serializes a (preferably streaming) result, fetching only as many rows as fit within the limits. rowCount counts the
// rows fetched, including the first row that did not fit, so a truncated result has rowCount > returnedRowCount.
static bool resultAsJson(duckdb::QueryResult& result, unsigned rowLimit, unsigned byteLimit, std::string& output, std::string& errorMessage)
{
    output = "{\"columns\":[";
    for (duckdb::idx_t column = 0; column < result.names.size(); ++column)
    {
        if (column)
            output.push_back(',');
        appendJsonString(output, result.names[column]);
    }
    output.append("],\"rows\":[");
    // Conservative bound for the suffix, so rows are only appended when the suffix is guaranteed to fit.
    const size_t suffixReserve = resultJsonSuffix(UINT64_MAX, UINT64_MAX, false).size();
    if (byteLimit && output.size() + suffixReserve > byteLimit)
    {
        errorMessage = "ByteLimit is too small for the result's column names";
        return false;
    }

    unsigned __int64 rowCount = 0;
    unsigned __int64 returnedRowCount = 0;
    bool truncated = false;
    StringBuffer rowText;
    while (!truncated)
    {
        auto chunk = result.Fetch();
        if (result.HasError())
        {
            errorMessage = result.GetError();
            return false;
        }
        if (!chunk || chunk->size() == 0)
            break;
        for (duckdb::idx_t row = 0; row < chunk->size(); ++row)
        {
            ++rowCount;
            if (rowLimit && returnedRowCount >= rowLimit)
            {
                truncated = true;
                break;
            }
            rowText.clear().append(returnedRowCount ? ",[" : "[");
            for (duckdb::idx_t column = 0; column < chunk->ColumnCount(); ++column)
                appendDuckDBValue(rowText, nullptr, chunk->GetValue(column, row));
            rowText.append(']');
            if (byteLimit && output.size() + rowText.length() + suffixReserve > byteLimit)
            {
                truncated = true;
                break;
            }
            output.append(rowText.str(), rowText.length());
            ++returnedRowCount;
        }
    }
    // Stop the query (and release its resources) rather than producing rows that will never be returned.
    if (truncated && result.type == duckdb::QueryResultType::STREAM_RESULT)
        result.Cast<duckdb::StreamQueryResult>().Close();

    output.append(resultJsonSuffix(rowCount, returnedRowCount, truncated));
    return true;
}

static bool discardQueryResult(duckdb::QueryResult& result, std::string& errorMessage)
{
    while (result.Fetch())
    {
        if (result.HasError())
        {
            errorMessage = result.GetError();
            return false;
        }
    }
    if (result.HasError())
    {
        errorMessage = result.GetError();
        return false;
    }
    return true;
}

class CScanMetadataContext : public ScanMetadataProvider
{
    Linked<IUserDescriptor> user;
    const bool allowLongForm;

public:
    CScanMetadataContext(IUserDescriptor* requestUser, bool _allowLongForm) : allowLongForm(_allowLongForm)
    {
        user.set(requestUser);
    }

    bool allowsLongFormScan() const override
    {
        return allowLongForm;
    }

    void resolveScanMetadata(const std::string& logicalName, ScanMetadata& metadata) override
    {
        try
        {
            Owned<IDistributedFile> file = CMetadataResolver::resolveLogicalFile(logicalName.c_str(), user.get());
            CMetadataResolver::buildScanMetadata(*file, metadata, nullptr);
        }
        catch (IException* exception)
        {
            StringBuffer message;
            exception->errorMessage(message);
            exception->Release();
            throw duckdb::BinderException(message.str());
        }
    }
};

void CQueryEngine::registerScanMetadataProvider(duckdb::shared_ptr<ScanMetadataProvider> provider)
{
#ifdef _DEBUG
    if (!scanObjectCounters)
        scanObjectCounters = std::make_shared<duckdb::hpcc::ScanObjectCounters>();
    provider->scanObjectCounters = scanObjectCounters;
#endif
    connection.context->registered_state->Remove(ScanMetadataProvider::stateKey);
    connection.context->registered_state->Insert(ScanMetadataProvider::stateKey, std::move(provider));
}

#ifdef _DEBUG
static CEngineScanObjectCounts readScanObjectCounts(const std::atomic<uint64_t>* counts)
{
    using duckdb::hpcc::ScanObjectKind;
    CEngineScanObjectCounts result;
    result.bindData = counts[static_cast<unsigned>(ScanObjectKind::BindData)].load();
    result.globalStates = counts[static_cast<unsigned>(ScanObjectKind::GlobalState)].load();
    result.localStates = counts[static_cast<unsigned>(ScanObjectKind::LocalState)].load();
    return result;
}

CEngineScanObjectCounts CScanObjectAudit::queryCreated() const
{
    return counters ? readScanObjectCounts(counters->created) : CEngineScanObjectCounts();
}

CEngineScanObjectCounts CScanObjectAudit::queryLive() const
{
    return counters ? readScanObjectCounts(counters->live) : CEngineScanObjectCounts();
}
#endif

#ifdef _USE_CPPUNIT
class CTestScanMetadataContext : public ScanMetadataProvider
{
    ScanMetadata metadata;

public:
    CTestScanMetadataContext(const char* recordDefinition, const char* parts, const char* options, bool hasRecordCount, uint64_t recordCount)
    {
        metadata.deserializeLongForm(recordDefinition, parts, options);
        if (hasRecordCount)
            metadata.recordCount = recordCount;
    }

    bool allowsLongFormScan() const override
    {
        return true;
    }

    void resolveScanMetadata(const std::string&, ScanMetadata& resolved) override
    {
        metadata.copyTo(resolved);
    }
};

void CQueryEngine::setTestScanMetadata(const char* recordDefinition, const char* parts, const char* options, bool hasRecordCount, uint64_t recordCount)
{
    registerScanMetadataProvider(duckdb::make_shared_ptr<CTestScanMetadataContext>(recordDefinition, parts, options, hasRecordCount, recordCount));
}
#endif

CQueryEngine::CQueryEngine() : CQueryEngine(nullptr, CEngineOptions())
{
}

CQueryEngine::CQueryEngine(IUserDescriptor* requestUser) : CQueryEngine(requestUser, CEngineOptions())
{
}

static std::atomic<unsigned> liveEngineCount{ 0 };
static std::atomic<unsigned __int64> createdEngineCount{ 0 };

static void noteEngineCreated()
{
    ++createdEngineCount;
    ++liveEngineCount;
}

#ifndef _WIN32
typedef int (*JemallocMallctl)(const char* name, void* oldValue, size_t* oldLength, void* newValue, size_t newLength);
#endif

// Bytes currently allocated through malloc. HPCC processes are normally run with jemalloc preloaded, which glibc's
// mallinfo2() knows nothing about, so prefer jemalloc's own statistics when it is the active allocator.
static bool captureHeapInUse(unsigned __int64& heapInUseBytes)
{
#ifndef _WIN32
    static const JemallocMallctl mallctl = reinterpret_cast<JemallocMallctl>(dlsym(RTLD_DEFAULT, "mallctl"));
    if (mallctl)
    {
        // Blocks freed into a thread cache still count as allocated; flush this thread's so they are returned to the
        // arenas. Failure (e.g. tcache disabled) is harmless.
        mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
        // jemalloc only refreshes its cached statistics when the epoch is advanced.
        uint64_t epoch = 1;
        size_t epochLength = sizeof(epoch);
        size_t allocated = 0;
        size_t allocatedLength = sizeof(allocated);
        if (mallctl("epoch", &epoch, &epochLength, &epoch, epochLength) == 0 && mallctl("stats.allocated", &allocated, &allocatedLength, nullptr, 0) == 0)
        {
            // Other threads (notably DuckDB's workers) cannot be flushed from here, so subtract the freed blocks held
            // in every thread cache. 4096 is MALLCTL_ARENAS_ALL; tcache_stashed_bytes only exists in newer jemallocs.
            size_t cached = 0;
            for (const char* name : {"stats.arenas.4096.tcache_bytes", "stats.arenas.4096.tcache_stashed_bytes"})
            {
                size_t value = 0;
                size_t valueLength = sizeof(value);
                if (mallctl(name, &value, &valueLength, nullptr, 0) == 0)
                    cached += value;
            }
            heapInUseBytes = cached < allocated ? allocated - cached : 0;
            return true;
        }
        return false;
    }
#endif
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
    const struct mallinfo2 heapInfo = mallinfo2();
    heapInUseBytes = heapInfo.uordblks + heapInfo.hblkhd;
    return true;
#else
    return false;
#endif
}

static CEngineMemoryUsage captureDatabaseMemoryUsage(duckdb::DuckDB& database)
{
    CEngineMemoryUsage usage;
    // Read the engine counters first, so that an engine created while the remaining figures are sampled still marks
    // the window as shared.
    usage.liveEngines = liveEngineCount.load();
    usage.createdEngines = createdEngineCount.load();
    const duckdb::BufferManager& bufferManager = duckdb::BufferManager::GetBufferManager(*database.instance);
    usage.duckdbUsedBytes = bufferManager.GetUsedMemory();
    usage.duckdbTempBytes = bufferManager.GetUsedSwap();
    usage.heapAvailable = captureHeapInUse(usage.heapInUseBytes);
#ifdef __APPLE__
    mach_task_basic_info_data_t taskInfo;
    mach_msg_type_number_t taskInfoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&taskInfo), &taskInfoCount) == KERN_SUCCESS)
        usage.residentBytes = taskInfo.resident_size;

    struct rusage resourceUsage;
    if (getrusage(RUSAGE_SELF, &resourceUsage) == 0 && resourceUsage.ru_maxrss > 0)
        usage.peakResidentBytes = resourceUsage.ru_maxrss;
#else
    ProcessInfo processInfo(ReadMemoryInfo);
    usage.residentBytes = processInfo.getActiveResidentMemory();
    usage.peakResidentBytes = processInfo.getPeakResidentMemory();
#endif
    return usage;
}

CQueryEngine::CQueryEngine(IUserDescriptor* requestUser, const CEngineOptions& options) : tempDirectory(createPrivateTempDirectory(options)),
ownedDatabase(duckdb::make_uniq<duckdb::DuckDB>(nullptr, prepareRestrictedConfig(config, tempDirectory, options))), database(*ownedDatabase), connection(database)
{
    database.LoadStaticExtension<Extension>();
    registerScanMetadataProvider(duckdb::make_shared_ptr<CScanMetadataContext>(requestUser, options.allowLongFormScan));
    noteEngineCreated();
}

CQueryEngine::CQueryEngine(CSharedQueryDatabase& sharedDatabase, IUserDescriptor* requestUser) : tempDirectory(sharedDatabase.queryTempDirectory()),
database(sharedDatabase.queryDatabase()), connection(database)
{
    // The shared database already loaded the hpcc_scan extension once; only the per-request scan-metadata context
    // (which resolves logical files with this request's Dali credentials) needs to be registered per Connection.
    // Long-form scans are intentionally unavailable through this shared service path.
    registerScanMetadataProvider(duckdb::make_shared_ptr<CScanMetadataContext>(requestUser, false));
    noteEngineCreated();
}

CQueryEngine::~CQueryEngine()
{
    --liveEngineCount;
}

CEngineMemoryUsage CQueryEngine::captureMemoryUsage()
{
    return captureDatabaseMemoryUsage(database);
}

CSharedQueryDatabase::CSharedQueryDatabase(const CEngineOptions& options) : tempDirectory(createPrivateTempDirectory(options)),
database(nullptr, prepareRestrictedConfig(config, tempDirectory, options))
{
    database.LoadStaticExtension<Extension>();
}

CEngineMemoryUsage CSharedQueryDatabase::captureMemoryUsage()
{
    return captureDatabaseMemoryUsage(database);
}

void CQueryEngine::interrupt()
{
    interruptRequested = true;
    connection.Interrupt();
}

static constexpr const char* interruptedMessage = "Query was interrupted";

bool CQueryEngine::isInitialized()
{
    return database.ExtensionIsLoaded("hpcc_scan");
}

bool CQueryEngine::executeQuery(const char* sql, unsigned rowLimit, unsigned byteLimit, std::string& resultText, std::string& errorMessage, CQueryTimings* timings, std::string* runtimeGraphJson, std::string* runtimeGraph, std::vector<CQueryResultColumnMetadata>* resultMetadata)
{
    resultText.clear();
    if (resultMetadata)
        resultMetadata->clear();
    if (runtimeGraphJson)
        runtimeGraphJson->clear();
    if (runtimeGraph)
        runtimeGraph->clear();
    duckdb::vector<duckdb::unique_ptr<duckdb::SQLStatement>> statements;
    try
    {
        statements = connection.ExtractStatements(sql ? sql : "");
        if (statements.empty())
        {
            errorMessage = "SqlText contains no SQL statements";
            return false;
        }
        // Let DuckDB stop producing rows once the row limit (plus one lookahead row, to detect truncation) is reached.
        auto& last = statements.back();
        if (rowLimit && last->type == duckdb::StatementType::SELECT_STATEMENT)
        {
            const uint64_t executionLimit = static_cast<uint64_t>(rowLimit) + 1;
            const std::string queryText = "SELECT * FROM (" + last->ToString() + ") AS ws_sql2_request LIMIT " + std::to_string(executionLimit);
            auto wrapped = connection.ExtractStatements(queryText);
            last = std::move(wrapped.front());
        }
    }
    catch (const std::exception& exception)
    {
        errorMessage = exception.what();
        return false;
    }

    class ScopedProfiling
    {
        duckdb::Connection& connection;
        bool enabled;

    public:
        ScopedProfiling(duckdb::Connection& _connection, bool _enabled) : connection(_connection), enabled(_enabled)
        {
            if (enabled)
                connection.EnableProfiling();
        }
        ~ScopedProfiling()
        {
            if (enabled)
                connection.DisableProfiling();
        }
    } profiling(connection, runtimeGraphJson != nullptr || runtimeGraph != nullptr);

    // Statements run in order, so later statements see the effects of earlier ones; only the last result is returned.
    if (timings)
        *timings = CQueryTimings();
    for (size_t index = 0; index < statements.size(); ++index)
    {
        const bool isLast = index + 1 == statements.size();
        const bool isSelect = statements[index]->type == duckdb::StatementType::SELECT_STATEMENT;
        CCycleTimer timer;
        auto pending = connection.PendingQuery(std::move(statements[index]), isLast || isSelect);
        if (timings)
            timings->prepareNs += timer.elapsedNs();
        if (!pending)
        {
            errorMessage = "DuckDB returned no pending query";
            return false;
        }
        if (pending->HasError())
        {
            errorMessage = pending->GetError();
            return false;
        }

        timer.reset();
        auto result = pending->Execute();
        if (!result)
        {
            errorMessage = "DuckDB returned no query result";
            return false;
        }
        if (result->HasError())
        {
            errorMessage = result->GetError();
            return false;
        }
        if (isLast && resultMetadata)
            extractResultMetadata(result->names, result->types, *resultMetadata);
        if (!isLast && isSelect && !discardQueryResult(*result, errorMessage))
            return false;
        // The last result streams, so its execution time includes fetching and serializing the returned rows.
        if (isLast && !resultAsJson(*result, rowLimit, byteLimit, resultText, errorMessage))
        {
            resultText.clear();
            return false;
        }
        if (isLast && (runtimeGraphJson || runtimeGraph))
        {
            if (runtimeGraph)
                *runtimeGraph = connection.GetProfilingInformation(duckdb::ProfilerPrintFormat::QUERY_TREE);
            if (runtimeGraphJson)
                *runtimeGraphJson = connection.GetProfilingInformation(duckdb::ProfilerPrintFormat::JSON);
            if ((runtimeGraph && runtimeGraph->empty()) || (runtimeGraphJson && runtimeGraphJson->empty()))
            {
                errorMessage = "DuckDB returned no runtime graph";
                return false;
            }
        }
        if (timings)
            timings->executeNs += timer.elapsedNs();
    }
    errorMessage.clear();
    return true;
}

bool CQueryEngine::startCursorQuery(const char* sql, bool captureRuntimeGraph, std::vector<CQueryResultColumnMetadata>* resultMetadata, std::string& errorMessage, CQueryTimings* timings)
{
    if (timings)
        *timings = CQueryTimings();
    if (cursorPending || cursorResult)
    {
        errorMessage = "A cursor query is already active";
        return false;
    }
    if (resultMetadata)
        resultMetadata->clear();

    auto disableCursorProfiling = [this]()
    {
        if (cursorCaptureRuntimeGraph)
        {
            connection.DisableProfiling();
            cursorCaptureRuntimeGraph = false;
        }
    };
    try
    {
        CCycleTimer timer;
        auto statements = connection.ExtractStatements(sql ? sql : "");
        if (statements.empty())
        {
            errorMessage = "SqlText contains no SQL statements";
            return false;
        }
        if (statements.size() > 1)
        {
            errorMessage = "SqlText must contain exactly one SELECT statement";
            return false;
        }
        if (statements.front()->type != duckdb::StatementType::SELECT_STATEMENT)
        {
            errorMessage = "Only SELECT statements are supported for cursor queries";
            return false;
        }
        if (interruptRequested)
        {
            errorMessage = interruptedMessage;
            return false;
        }
        if (captureRuntimeGraph)
        {
            connection.EnableProfiling();
            cursorCaptureRuntimeGraph = true;
        }
        auto pending = connection.PendingQuery(std::move(statements.front()), true);
        if (timings)
            timings->prepareNs = timer.elapsedNs();
        if (!pending)
        {
            errorMessage = "DuckDB returned no pending query";
            disableCursorProfiling();
            return false;
        }
        if (pending->HasError())
        {
            errorMessage = pending->GetError();
            disableCursorProfiling();
            return false;
        }
        // DuckDB clears its own interrupt flag when a statement begins, so an interrupt during preparation is only
        // visible here.
        if (interruptRequested)
        {
            errorMessage = interruptedMessage;
            disableCursorProfiling();
            return false;
        }
        if (resultMetadata)
            extractResultMetadata(pending->names, pending->types, *resultMetadata);
        cursorPending = std::move(pending);
    }
    catch (const std::exception& exception)
    {
        errorMessage = exception.what();
        cursorPending.reset();
        disableCursorProfiling();
        return false;
    }
    errorMessage.clear();
    return true;
}

bool CQueryEngine::fetchCursorBatch(unsigned fetchSize, unsigned byteLimit, std::string& rowsJson, bool& complete, std::string& errorMessage, std::string* runtimeGraphJson, std::string* runtimeGraph, unsigned* returnedRowCount)
{
    rowsJson.clear();
    complete = false;
    if (returnedRowCount)
        *returnedRowCount = 0;
    if ((!cursorPending && !cursorResult) || !fetchSize)
    {
        errorMessage = !fetchSize ? "FetchSize must be greater than zero" : "No active cursor query";
        return false;
    }
    if (byteLimit && byteLimit < 2)
    {
        errorMessage = "ByteLimit is too small for a result batch";
        return false;
    }

    auto cleanupCursor = [this]()
    {
        cursorPending.reset();
        cursorResult.reset();
        cursorChunk.reset();
        cursorChunkRow = 0;
        if (cursorCaptureRuntimeGraph)
        {
            cursorCaptureRuntimeGraph = false;
            try
            {
                connection.DisableProfiling();
            }
            catch (...)
            {
                ERRLOG("WsSQL2: Failed to disable DuckDB profiling while cleaning up a cursor");
            }
        }
    };

    try
    {
        if (cursorPending)
        {
            if (interruptRequested)
            {
                errorMessage = interruptedMessage;
                cleanupCursor();
                return false;
            }
            auto result = cursorPending->Execute();
            cursorPending.reset();
            if (!result)
            {
                errorMessage = "DuckDB returned no query result";
                cleanupCursor();
                return false;
            }
            if (result->HasError())
            {
                errorMessage = interruptRequested ? interruptedMessage : result->GetError();
                cleanupCursor();
                return false;
            }
            cursorResult = std::move(result);
        }

        rowsJson.push_back('[');
        unsigned rowsReturned = 0;
        StringBuffer rowText;
        while (rowsReturned < fetchSize)
        {
            if (!cursorChunk || cursorChunkRow >= cursorChunk->size())
            {
                if (interruptRequested)
                {
                    errorMessage = interruptedMessage;
                    cleanupCursor();
                    rowsJson.clear();
                    return false;
                }
                cursorChunk = cursorResult->Fetch();
                cursorChunkRow = 0;
                if (cursorResult->HasError())
                {
                    errorMessage = cursorResult->GetError();
                    cleanupCursor();
                    rowsJson.clear();
                    return false;
                }
                if (!cursorChunk || !cursorChunk->size())
                {
                    complete = true;
                    break;
                }
            }

            rowText.clear().append(rowsReturned ? ",[" : "[");
            for (duckdb::idx_t column = 0; column < cursorChunk->ColumnCount(); ++column)
                appendDuckDBValue(rowText, nullptr, cursorChunk->GetValue(column, cursorChunkRow));
            rowText.append(']');
            if (byteLimit && rowsJson.size() + rowText.length() + 1 > byteLimit)
            {
                if (!rowsReturned)
                {
                    errorMessage = "ByteLimit is too small for the next result row";
                    rowsJson.clear();
                    return false;
                }
                break;
            }
            rowsJson.append(rowText.str(), rowText.length());
            ++cursorChunkRow;
            ++rowsReturned;
        }
        rowsJson.push_back(']');

        if (complete)
        {
            if (cursorCaptureRuntimeGraph)
            {
                if (runtimeGraph)
                    *runtimeGraph = connection.GetProfilingInformation(duckdb::ProfilerPrintFormat::QUERY_TREE);
                if (runtimeGraphJson)
                    *runtimeGraphJson = connection.GetProfilingInformation(duckdb::ProfilerPrintFormat::JSON);
                if ((runtimeGraph && runtimeGraph->empty()) || (runtimeGraphJson && runtimeGraphJson->empty()))
                {
                    errorMessage = "DuckDB returned no runtime graph";
                    cleanupCursor();
                    rowsJson.clear();
                    return false;
                }
                connection.DisableProfiling();
                cursorCaptureRuntimeGraph = false;
            }
            cursorResult.reset();
            cursorChunk.reset();
        }
        errorMessage.clear();
        if (returnedRowCount)
            *returnedRowCount = rowsReturned;
        return true;
    }
    catch (IException* exception)
    {
        StringBuffer message;
        exception->errorMessage(message);
        exception->Release();
        errorMessage = message.str();
    }
    catch (const std::exception& exception)
    {
        errorMessage = exception.what();
    }
    catch (...)
    {
        ERRLOG("WsSQL2: Unknown exception while fetching a DuckDB cursor batch");
        errorMessage = "Unknown error while fetching a result batch";
    }

    cleanupCursor();
    rowsJson.clear();
    complete = false;
    return false;
}

static bool extractExplainPlan(duckdb::Connection& connection, const std::string& explainSql, std::string& plan, std::string& errorMessage)
{
    auto result = connection.Query(explainSql);
    if (!result)
    {
        errorMessage = "DuckDB returned no explain result";
        return false;
    }
    if (result->HasError())
    {
        errorMessage = result->GetError();
        return false;
    }
    if (result->type != duckdb::QueryResultType::MATERIALIZED_RESULT)
    {
        errorMessage = "DuckDB returned a non-materialized explain result";
        return false;
    }
    auto chunk = result->Cast<duckdb::MaterializedQueryResult>().Fetch();
    if (!chunk || chunk->size() != 1 || chunk->ColumnCount() < 2)
    {
        errorMessage = "DuckDB returned an unexpected explain result";
        return false;
    }
    plan = chunk->GetValue(1, 0).ToString();
    return true;
}

static void appendLeafValues(IPropertyTree& tree, StringArray& values)
{
    if (!tree.hasChildren())
    {
        const char* value = tree.queryProp(nullptr);
        if (!isEmptyString(value) && !values.contains(value))
            values.append(value);
        return;
    }
    Owned<IPropertyTreeIterator> children = tree.getElements("*");
    ForEach(*children)
        appendLeafValues(children->query(), values);
}

static IPropertyTree* queryDirectChild(IPropertyTree& tree, const char* name)
{
    Owned<IPropertyTreeIterator> children = tree.getElements("*");
    ForEach(*children)
    {
        if (streq(children->query().queryName(), name))
            return &children->query();
    }
    return nullptr;
}

static void appendDirectChildValues(IPropertyTree& tree, const char* name, StringArray& values)
{
    Owned<IPropertyTreeIterator> children = tree.getElements("*");
    ForEach(*children)
    {
        if (streq(children->query().queryName(), name))
            appendLeafValues(children->query(), values);
    }
}

static void extractHpccScanPlan(IPropertyTree& node, StringArray& projectedColumns, StringArray& residualFilters)
{
    if (!node.hasChildren())
        return;
    IPropertyTree* name = queryDirectChild(node, "name");
    // Profiler (ANALYZE) output names operators with "operator_name" rather than EXPLAIN's "name".
    if (!name)
        name = queryDirectChild(node, "operator_name");
    if (name && streq(name->queryProp(nullptr), "HPCC_SCAN"))
    {
        IPropertyTree* extraInfo = queryDirectChild(node, "extra_info");
        if (extraInfo)
        {
            appendDirectChildValues(*extraInfo, "Reader_sProjection", projectedColumns);
            appendDirectChildValues(*extraInfo, "Residual_sFilters", residualFilters);
        }
    }
    Owned<IPropertyTreeIterator> children = node.getElements("*");
    ForEach(*children)
        extractHpccScanPlan(children->query(), projectedColumns, residualFilters);
}

bool CQueryEngine::splitQuery(const char* sql, std::vector<std::string>& statements, std::string& errorMessage)
{
    statements.clear();
    try
    {
        auto parsed = connection.ExtractStatements(sql ? sql : "");
        auto isSpace = [](char c) { return isspace(static_cast<unsigned char>(c)) != 0; };
        for (const auto& statement : parsed)
        {
            // statement->query is the statement's exact source text, including any leading comments.
            const std::string& text = statement->query;
            size_t begin = 0;
            size_t end = text.size();
            while (begin < end && isSpace(text[begin]))
                ++begin;
            while (end > begin && (isSpace(text[end - 1]) || text[end - 1] == ';'))
                --end;
            statements.emplace_back(text.substr(begin, end - begin));
        }
    }
    catch (const std::exception& exception)
    {
        statements.clear();
        errorMessage = exception.what();
        return false;
    }
    errorMessage.clear();
    return true;
}

bool CQueryEngine::explainQuery(const char* sql, std::string& planText, std::string& planJson, StringArray& projectedColumns, StringArray& residualFilters, std::vector<CQueryResultColumnMetadata>& resultMetadata, std::string& errorMessage)
{
    std::string queryText;
    resultMetadata.clear();
    try
    {
        auto statements = connection.ExtractStatements(sql ? sql : "");
        if (statements.size() != 1 || statements.front()->type != duckdb::StatementType::SELECT_STATEMENT)
        {
            errorMessage = "Only a single SELECT statement is supported";
            return false;
        }
        queryText = statements.front()->ToString();
        auto prepared = connection.Prepare(queryText);
        if (!prepared || prepared->HasError())
        {
            errorMessage = prepared ? prepared->GetError() : "DuckDB returned no prepared statement";
            return false;
        }
        extractResultMetadata(prepared->GetNames(), prepared->GetTypes(), resultMetadata);
    }
    catch (const std::exception& exception)
    {
        errorMessage = exception.what();
        return false;
    }

    if (!extractExplainPlan(connection, "EXPLAIN " + queryText, planText, errorMessage))
        return false;
    if (!extractExplainPlan(connection, "EXPLAIN (FORMAT JSON) " + queryText, planJson, errorMessage))
        return false;
    try
    {
        Owned<IPropertyTree> planTree = createPTreeFromJSONString(planJson.c_str());
        extractHpccScanPlan(*planTree, projectedColumns, residualFilters);
    }
    catch (IException* exception)
    {
        StringBuffer message;
        exception->errorMessage(message);
        exception->Release();
        errorMessage = message.str();
        return false;
    }
    errorMessage.clear();
    return true;
}

#ifdef _USE_CPPUNIT
class CTestTempDirectory
{
    std::string path;

public:
    CTestTempDirectory()
    {
        StringBuffer tempPath;
        getSystemTempDir(tempPath);
        std::string tempRoot = tempPath.str();
        if (!tempRoot.empty() && tempRoot.back() != '/' && tempRoot.back() != '\\')
            tempRoot += PATHSEPCHAR;

        std::random_device random;
        for (unsigned attempt = 0; attempt < 16; ++attempt)
        {
            path = tempRoot + "ws_sql2_test_" + std::to_string(random()) + "_" + std::to_string(random());
            OwnedIFile tempDir = createIFile(path.c_str());
            if (tempDir->createDirectory())
                return;
        }
        CPPUNIT_ASSERT_MESSAGE("Could not create test directory", false);
    }

    ~CTestTempDirectory()
    {
        recursiveRemoveDirectory(path.c_str());
    }

    const std::string& query() const { return path; }
};

class WsSql2QueryEngineTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(WsSql2QueryEngineTests);
    CPPUNIT_TEST(testQueryEngineSafety);
    CPPUNIT_TEST(testSpilling);
    CPPUNIT_TEST(testResultLimitsAndExplain);
    CPPUNIT_TEST(testSplitQuery);
    CPPUNIT_TEST(testCursorBatches);
    CPPUNIT_TEST(testDecimalBinding);
    CPPUNIT_TEST(testLongFormScanPolicy);
    CPPUNIT_TEST(testEclMetadata);
    CPPUNIT_TEST(testPushdownExecution);
    CPPUNIT_TEST(testSharedDatabase);
    CPPUNIT_TEST(testSharedSpillDirectoryIsolation);
    CPPUNIT_TEST(testMemoryUsage);
    CPPUNIT_TEST_SUITE_END();

public:
    void testQueryEngineSafety()
    {
        CQueryEngine engine;
        CPPUNIT_ASSERT(engine.isInitialized());
        std::string queryResult;
        std::string queryError;
        CPPUNIT_ASSERT(!engine.executeQuery("select * from hpcc_scan('invalid')", 0, 0, queryResult, queryError));

        Owned<IUserDescriptor> requestUser = createUserDescriptor();
        CQueryEngine contextualEngine(requestUser);
        CPPUNIT_ASSERT(contextualEngine.isInitialized());
        CPPUNIT_ASSERT(contextualEngine.queryTempDirectory() != engine.queryTempDirectory());

        CPPUNIT_ASSERT(contextualEngine.executeQuery("CREATE TABLE scratch AS SELECT 41 AS value; SELECT value + 1 AS answer FROM scratch", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT(queryResult.find("[[42]]") != std::string::npos);
        CPPUNIT_ASSERT(contextualEngine.executeQuery("INSERT INTO scratch VALUES (1); UPDATE scratch SET value = value + 1; DELETE FROM scratch WHERE value = 2", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT_MESSAGE(queryResult, queryResult.find("\"rows\":[[1]]") != std::string::npos);
        CPPUNIT_ASSERT(contextualEngine.executeQuery("DROP TABLE scratch", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT 1; SELECT 2", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT(queryResult.find("\"rows\":[[2]]") != std::string::npos);
        CPPUNIT_ASSERT(!contextualEngine.executeQuery("  ;  ", 0, 0, queryResult, queryError));

        CTestTempDirectory blockedFileDirectory;
        const std::string blockedFile = blockedFileDirectory.query() + "/blocked.csv";
        const std::string spillFile = contextualEngine.queryTempDirectory() + "/blocked.csv";
        const char* blockedStatements[] = {
            "COPY (SELECT 1 AS value) TO '{F}'",
            "ATTACH '{F}' AS blocked",
            "EXPORT DATABASE '{F}'",
            "SET enable_external_access = true",
            "LOAD '{F}'",
            "INSTALL httpfs",
        };
        for (const char* statement : blockedStatements)
        {
            for (const std::string& target : { blockedFile, spillFile })
            {
                std::string sql = statement;
                const size_t placeholder = sql.find("{F}");
                if (placeholder != std::string::npos)
                    sql.replace(placeholder, 3, target);
                CPPUNIT_ASSERT_MESSAGE(sql, !contextualEngine.executeQuery(sql.c_str(), 0, 0, queryResult, queryError));
                CPPUNIT_ASSERT_MESSAGE(sql, !checkFileExists(target.c_str()));
            }
        }
        CPPUNIT_ASSERT(!contextualEngine.executeQuery(("SELECT * FROM read_csv('" + blockedFile + "')").c_str(), 0, 0, queryResult, queryError));
    }

    void testSpilling()
    {
        Owned<IUserDescriptor> requestUser = createUserDescriptor();
        CTestTempDirectory spillParentDirectory;
        const std::string& spillParent = spillParentDirectory.query();
        const std::string spillDirectory = spillParent + "/spill";
        std::string queryResult;
        std::string queryError;
        bool spillOk = false;
        bool spillDirectoryCreated = false;
        std::string privateDirectory;
        {
            CEngineOptions options;
            options.memoryLimit = "32MB";
            options.threads = 1;
            options.tempDirectory = spillDirectory;
            CQueryEngine spillEngine(requestUser, options);
            privateDirectory = spillEngine.queryTempDirectory();
            spillOk = spillEngine.executeQuery("SELECT count(*), sum(i) FROM (SELECT i, md5(i::VARCHAR) AS h FROM range(3000000) t(i) ORDER BY h)", 0, 0, queryResult, queryError);
            if (spillOk)
                spillDirectoryCreated = checkFileExists(privateDirectory.c_str());
        }
        const bool spillDirectoryRemoved = !checkFileExists(privateDirectory.c_str());
        CPPUNIT_ASSERT_MESSAGE(queryError, spillOk);
        CPPUNIT_ASSERT(queryResult.find("[[3000000, 4499998500000]]") != std::string::npos);
        CPPUNIT_ASSERT(privateDirectory.rfind(spillDirectory + "/", 0) == 0);
        CPPUNIT_ASSERT(spillDirectoryCreated);
        CPPUNIT_ASSERT(spillDirectoryRemoved);
    }

    void testResultLimitsAndExplain()
    {
        Owned<IUserDescriptor> requestUser = createUserDescriptor();
        CQueryEngine contextualEngine(requestUser);
        std::string queryResult;
        std::string queryError;

        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT * FROM range(1000000);", 1, 0, queryResult, queryError));
        CPPUNIT_ASSERT(queryResult.find("\"returnedRowCount\":1") != std::string::npos);
        CPPUNIT_ASSERT(queryResult.find("\"truncated\":true") != std::string::npos);
        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT * FROM range(10000);", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT(queryResult.size() > 65536);
        Owned<IPropertyTree> unboundedJson = createPTreeFromJSONString(queryResult.c_str());
        CPPUNIT_ASSERT(unboundedJson);
        CPPUNIT_ASSERT(!unboundedJson->getPropBool("truncated"));
        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT * FROM range(1000000);", 0, 1024, queryResult, queryError));
        CPPUNIT_ASSERT(queryResult.size() <= 1024);
        Owned<IPropertyTree> limitedJson = createPTreeFromJSONString(queryResult.c_str());
        CPPUNIT_ASSERT(limitedJson);
        CPPUNIT_ASSERT(limitedJson->getPropBool("truncated"));
        CPPUNIT_ASSERT(limitedJson->getPropInt64("rowCount") == limitedJson->getPropInt64("returnedRowCount") + 1);
        for (unsigned byteLimit = 60; byteLimit < 160; ++byteLimit)
        {
            if (contextualEngine.executeQuery("SELECT 123456789 AS value FROM range(100)", 0, byteLimit, queryResult, queryError))
                CPPUNIT_ASSERT_MESSAGE(queryResult, queryResult.size() <= byteLimit);
        }
        CPPUNIT_ASSERT(!contextualEngine.executeQuery("SELECT 1 AS value", 0, 8, queryResult, queryError));

        std::string planText;
        std::string planJson;
        StringArray projectedColumns;
        StringArray residualFilters;
        std::vector<CQueryResultColumnMetadata> resultMetadata;
        CPPUNIT_ASSERT(contextualEngine.explainQuery("SELECT CAST(1.25 AS DECIMAL(6,2)) AS amount, 'x' AS label", planText, planJson, projectedColumns, residualFilters, resultMetadata, queryError));
        CPPUNIT_ASSERT(!planText.empty());
        CPPUNIT_ASSERT(!planJson.empty() && planJson.front() == '[' && planJson.back() == ']');
        CPPUNIT_ASSERT_EQUAL(size_t(2), resultMetadata.size());
        CPPUNIT_ASSERT_EQUAL(std::string("amount"), resultMetadata[0].name);
        CPPUNIT_ASSERT_EQUAL(std::string("DECIMAL(6,2)"), resultMetadata[0].typeName);
        CPPUNIT_ASSERT_EQUAL(6, resultMetadata[0].precision);
        CPPUNIT_ASSERT_EQUAL(2, resultMetadata[0].scale);
        CPPUNIT_ASSERT_EQUAL(std::string("label"), resultMetadata[1].name);
        CPPUNIT_ASSERT_EQUAL(std::string("VARCHAR"), resultMetadata[1].typeName);
        std::string runtimeGraph;
        std::string runtimeGraphJson;
        resultMetadata.clear();
        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT i AS number FROM range(10) t(i)", 0, 0, queryResult, queryError, nullptr, &runtimeGraphJson, &runtimeGraph, &resultMetadata));
        CPPUNIT_ASSERT_EQUAL(size_t(1), resultMetadata.size());
        CPPUNIT_ASSERT_EQUAL(std::string("number"), resultMetadata[0].name);
        CPPUNIT_ASSERT_EQUAL(std::string("BIGINT"), resultMetadata[0].typeName);
        CPPUNIT_ASSERT_EQUAL(0, resultMetadata[0].precision);
        CPPUNIT_ASSERT_EQUAL(0, resultMetadata[0].scale);
        CPPUNIT_ASSERT(!runtimeGraph.empty());
        Owned<IPropertyTree> profile = createPTreeFromJSONString(runtimeGraphJson.c_str());
        CPPUNIT_ASSERT(profile);
        CPPUNIT_ASSERT_EQUAL(10, profile->getPropInt("rows_returned"));
        CPPUNIT_ASSERT(profile->hasProp("latency"));
        IPropertyTree* scan = profile->queryPropTree("children[1]");
        CPPUNIT_ASSERT(scan);
        CPPUNIT_ASSERT(streq(scan->queryProp("operator_name"), "RANGE"));
        CPPUNIT_ASSERT_EQUAL(10, scan->getPropInt("operator_cardinality"));
        CPPUNIT_ASSERT(scan->hasProp("operator_timing"));
        CPPUNIT_ASSERT(queryResult.find("\"rows\":[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]") != std::string::npos);
        CPPUNIT_ASSERT(queryResult.find("\"rowCount\":10") != std::string::npos);
        CPPUNIT_ASSERT(queryResult.find("\"returnedRowCount\":10") != std::string::npos);

        CPPUNIT_ASSERT(contextualEngine.executeQuery("SELECT [1, 2] AS items, {'name': 'x', 'control': chr(1)} AS details", 0, 0, queryResult, queryError));
        Owned<IPropertyTree> nestedJson = createPTreeFromJSONString(queryResult.c_str());
        CPPUNIT_ASSERT(nestedJson);
        CPPUNIT_ASSERT_MESSAGE(queryError, contextualEngine.executeQuery("SELECT MAP { '': 1 } AS details", 0, 0, queryResult, queryError));
        Owned<IPropertyTree> emptyNameJson = createPTreeFromJSONString(queryResult.c_str());
        CPPUNIT_ASSERT(emptyNameJson);
        CPPUNIT_ASSERT_MESSAGE(queryResult, queryResult.find("\"rows\":[[{\"\": 1}]]") != std::string::npos);
    }

    void testSplitQuery()
    {
        CQueryEngine engine;
        std::vector<std::string> statements;
        std::string error;
        const std::string sql = "  -- first\nSELECT 'a;b' AS x ;\n\nCREATE TABLE t (i INTEGER);  SELECT 'é'\n";
        CPPUNIT_ASSERT_MESSAGE(error, engine.splitQuery(sql.c_str(), statements, error));
        CPPUNIT_ASSERT_EQUAL(size_t(3), statements.size());
        CPPUNIT_ASSERT_EQUAL(std::string("-- first\nSELECT 'a;b' AS x"), statements[0]);
        CPPUNIT_ASSERT_EQUAL(std::string("CREATE TABLE t (i INTEGER)"), statements[1]);
        CPPUNIT_ASSERT_EQUAL(std::string("SELECT 'é'"), statements[2]);

        CPPUNIT_ASSERT(engine.splitQuery("  ", statements, error));
        CPPUNIT_ASSERT(statements.empty());
        CPPUNIT_ASSERT(!engine.splitQuery("SELECT 1; SELEC 2", statements, error));
        CPPUNIT_ASSERT(!error.empty());
        CPPUNIT_ASSERT(statements.empty());
    }

    void testLongFormScanPolicy()
    {
        Owned<IUserDescriptor> requestUser = createUserDescriptor();
        std::string queryResult;
        std::string queryError;
        {
            CQueryEngine engine(requestUser);
            CPPUNIT_ASSERT(!engine.executeQuery("SELECT * FROM hpcc_scan('{}', '{}', '{}')", 0, 0, queryResult, queryError));
            CPPUNIT_ASSERT_MESSAGE(queryError, queryError.find("disabled") != std::string::npos);
        }

        {
            CEngineOptions options;
            options.allowLongFormScan = true;
            CQueryEngine engine(requestUser, options);
            CPPUNIT_ASSERT(!engine.executeQuery("SELECT * FROM hpcc_scan('{\"bad\":1}', '[]', '{}')", 0, 0, queryResult, queryError));
            CPPUNIT_ASSERT_MESSAGE(queryError, queryError.find("invalid ECL record definition") != std::string::npos);
            CPPUNIT_ASSERT(!engine.executeQuery("SELECT * FROM hpcc_scan('RECORD unsigned4 id; IFBLOCK(true) string name; END; END;', '[]', '{}')", 0, 0, queryResult, queryError));
            CPPUNIT_ASSERT_MESSAGE(queryError, queryError.find("unsupported record layout") != std::string::npos);
        }
    }

    // Unlike the other tests above (which each build their own private DuckDB instance via the standalone
    // constructors), this exercises the production path: one CSharedQueryDatabase, with every CQueryEngine
    // attaching its own Connection to it, as the ESP service does for every request.
    void testSharedDatabase()
    {
        Owned<IUserDescriptor> requestUser = createUserDescriptor();
        CEngineOptions options;
        CSharedQueryDatabase sharedDatabase(options);

        CQueryEngine engineA(sharedDatabase, requestUser);
        CQueryEngine engineB(sharedDatabase, requestUser);
        CPPUNIT_ASSERT(engineA.isInitialized());
        CPPUNIT_ASSERT(engineB.isInitialized());
        // All engines attached to one CSharedQueryDatabase share its single spill directory, unlike standalone
        // engines (testQueryEngineSafety), which each get their own.
        CPPUNIT_ASSERT_EQUAL(sharedDatabase.queryTempDirectory(), engineA.queryTempDirectory());
        CPPUNIT_ASSERT_EQUAL(engineA.queryTempDirectory(), engineB.queryTempDirectory());

        // allowLongFormScan is enforced per Connection, so it must still be honoured even though the underlying
        // DuckDB instance is shared.
        std::string queryResult;
        std::string queryError;
        CPPUNIT_ASSERT(!engineA.executeQuery("SELECT * FROM hpcc_scan('{}', '{}', '{}')", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT_MESSAGE(queryError, queryError.find("disabled") != std::string::npos);

        // Creating a table on one connection must not be visible on another connection's default (temporary, in
        // DuckDB's default in-memory catalog) scope, and concurrent queries on separate engines sharing one
        // database must not cross-contaminate each other's results.
        CPPUNIT_ASSERT(engineA.executeQuery("CREATE TEMP TABLE scratch AS SELECT 1 AS value", 0, 0, queryResult, queryError));
        CPPUNIT_ASSERT(!engineB.executeQuery("SELECT * FROM scratch", 0, 0, queryResult, queryError));

        constexpr unsigned workerCount = 8;
        constexpr unsigned queriesPerWorker = 25;
        std::atomic<unsigned> mismatches{ 0 };
        std::vector<std::thread> workers;
        for (unsigned worker = 0; worker < workerCount; worker++)
        {
            workers.emplace_back([&sharedDatabase, &requestUser, &mismatches, worker]()
            {
                CQueryEngine engine(sharedDatabase, requestUser);
                for (unsigned iteration = 0; iteration < queriesPerWorker; iteration++)
                {
                    std::string result;
                    std::string error;
                    const std::string expected = "[[" + std::to_string(worker * 1000 + iteration) + "]]";
                    if (!engine.executeQuery(("SELECT " + std::to_string(worker) + " * 1000 + " + std::to_string(iteration) + " AS value").c_str(), 0, 0, result, error))
                    {
                        mismatches++;
                        continue;
                    }
                    if (result.find(expected) == std::string::npos)
                        mismatches++;
                }
            });
        }
        for (std::thread& worker : workers)
            worker.join();
        CPPUNIT_ASSERT_EQUAL(0u, mismatches.load());
    }

    void testSharedSpillDirectoryIsolation()
    {
        CTestTempDirectory spillParentDirectory;
        const std::string& spillParent = spillParentDirectory.query();
        const std::string canary = "WSSQL2_SPILL_ISOLATION_CANARY";
        std::string spillDirectory;
        std::string canaryPath;
        bool readAllowed = false;
        bool blobReadAllowed = false;
        bool csvReadAllowed = false;
        bool listed = false;
        bool writeAllowed = false;
        {
            CEngineOptions options;
            options.tempDirectory = spillParent;
            CSharedQueryDatabase sharedDatabase(options);
            spillDirectory = sharedDatabase.queryTempDirectory();
            CPPUNIT_ASSERT(recursiveCreateDirectory(spillDirectory.c_str()));
            canaryPath = spillDirectory + "/canary.txt";
            OwnedIFile file = createIFile(canaryPath.c_str());
            {
                OwnedIFileIO io = file->open(IFOcreate);
                CPPUNIT_ASSERT(io);
                CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(canary.size()), io->write(0, canary.size(), canary.data()));
            }

            Owned<IUserDescriptor> owner = createUserDescriptor();
            owner->set("spill_owner", "");
            Owned<IUserDescriptor> other = createUserDescriptor();
            other->set("spill_other", "");
            CQueryEngine ownerEngine(sharedDatabase, owner);
            CQueryEngine otherEngine(sharedDatabase, other);
            CPPUNIT_ASSERT_EQUAL(ownerEngine.queryTempDirectory(), otherEngine.queryTempDirectory());

            std::string result;
            std::string error;
            readAllowed = otherEngine.executeQuery(("SELECT content FROM read_text('" + canaryPath + "')").c_str(), 0, 0, result, error);
            blobReadAllowed = otherEngine.executeQuery(("SELECT content FROM read_blob('" + canaryPath + "')").c_str(), 0, 0, result, error);
            csvReadAllowed = otherEngine.executeQuery(("SELECT * FROM read_csv('" + canaryPath + "')").c_str(), 0, 0, result, error);
            listed = otherEngine.executeQuery(("SELECT file FROM glob('" + spillDirectory + "/*')").c_str(), 0, 0, result, error);
            const std::string plantedPath = spillDirectory + "/planted.csv";
            writeAllowed = otherEngine.executeQuery(("COPY (SELECT 1) TO '" + plantedPath + "'").c_str(), 0, 0, result, error);
            if (checkFileExists(plantedPath.c_str()))
            {
                OwnedIFile plantedFile = createIFile(plantedPath.c_str());
                CPPUNIT_ASSERT(plantedFile->remove());
            }

            CPPUNIT_ASSERT(file->remove());
        }
        if (checkFileExists(spillDirectory.c_str()))
        {
            OwnedIFile directory = createIFile(spillDirectory.c_str());
            CPPUNIT_ASSERT(directory->remove());
        }
        OwnedIFile directory = createIFile(spillParent.c_str());
        CPPUNIT_ASSERT(directory->remove());
        CPPUNIT_ASSERT_MESSAGE("A different user read a file inside the shared spill directory", !readAllowed);
        CPPUNIT_ASSERT_MESSAGE("A different user read a blob inside the shared spill directory", !blobReadAllowed);
        CPPUNIT_ASSERT_MESSAGE("A different user read CSV inside the shared spill directory", !csvReadAllowed);
        CPPUNIT_ASSERT_MESSAGE("A different user listed a file inside the shared spill directory", !listed);
        CPPUNIT_ASSERT_MESSAGE("A different user wrote a file inside the shared spill directory", !writeAllowed);
    }

    void testCursorBatches()
    {
        CQueryEngine engine;
        std::string error;
        std::string rows;
        bool complete = false;
        unsigned rowsReturned = 0;
        std::vector<CQueryResultColumnMetadata> metadata;

        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT i AS number FROM range(5) t(i)", false, &metadata, error));
        CPPUNIT_ASSERT_EQUAL(size_t(1), metadata.size());
        CPPUNIT_ASSERT_EQUAL(std::string("number"), metadata[0].name);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error, nullptr, nullptr, &rowsReturned));
        CPPUNIT_ASSERT_EQUAL(std::string("[[0],[1]]"), rows);
        CPPUNIT_ASSERT_EQUAL(2U, rowsReturned);
        CPPUNIT_ASSERT(!complete);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error, nullptr, nullptr, &rowsReturned));
        CPPUNIT_ASSERT_EQUAL(std::string("[[2],[3]]"), rows);
        CPPUNIT_ASSERT_EQUAL(2U, rowsReturned);
        CPPUNIT_ASSERT(!complete);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error, nullptr, nullptr, &rowsReturned));
        CPPUNIT_ASSERT_EQUAL(std::string("[[4]]"), rows);
        CPPUNIT_ASSERT_EQUAL(1U, rowsReturned);
        CPPUNIT_ASSERT(complete);

        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT i FROM range(4) t(i)", false, nullptr, error));
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error));
        CPPUNIT_ASSERT_EQUAL(std::string("[[0],[1]]"), rows);
        CPPUNIT_ASSERT(!complete);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error));
        CPPUNIT_ASSERT_EQUAL(std::string("[[2],[3]]"), rows);
        CPPUNIT_ASSERT(!complete);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error));
        CPPUNIT_ASSERT_EQUAL(std::string("[]"), rows);
        CPPUNIT_ASSERT(complete);

        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT i FROM range(0) t(i)", false, nullptr, error));
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error));
        CPPUNIT_ASSERT_EQUAL(std::string("[]"), rows);
        CPPUNIT_ASSERT(complete);

        std::string runtimeGraph;
        std::string runtimeGraphJson;
        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT i FROM range(1) t(i)", true, nullptr, error));
        CPPUNIT_ASSERT(engine.fetchCursorBatch(1, 0, rows, complete, error, &runtimeGraphJson, &runtimeGraph));
        CPPUNIT_ASSERT(!complete);
        CPPUNIT_ASSERT(engine.fetchCursorBatch(1, 0, rows, complete, error, &runtimeGraphJson, &runtimeGraph));
        CPPUNIT_ASSERT(complete);
        CPPUNIT_ASSERT(!runtimeGraph.empty());
        CPPUNIT_ASSERT(!runtimeGraphJson.empty());

        CPPUNIT_ASSERT(!engine.startCursorQuery("SELECT 1; SELECT i AS answer FROM range(2) t(i)", false, &metadata, error));
        CPPUNIT_ASSERT(error.find("exactly one SELECT statement") != std::string::npos);
        CPPUNIT_ASSERT(!engine.startCursorQuery("SELECT 1; CREATE TABLE disallowed (value INTEGER)", false, nullptr, error));
        CPPUNIT_ASSERT(error.find("exactly one SELECT statement") != std::string::npos);
        CPPUNIT_ASSERT(!engine.startCursorQuery("CREATE TABLE disallowed (value INTEGER)", false, nullptr, error));
        CPPUNIT_ASSERT(error.find("Only SELECT statements") != std::string::npos);
        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT 7 AS answer;", false, &metadata, error));
        CPPUNIT_ASSERT(engine.fetchCursorBatch(2, 0, rows, complete, error));
        CPPUNIT_ASSERT_EQUAL(std::string("[[7]]"), rows);

        // Starting a cursor only prepares the query: metadata is available, and runtime errors surface on the
        // first fetch.
        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT error('deferred failure')::INTEGER AS failing", false, &metadata, error));
        CPPUNIT_ASSERT_EQUAL(size_t(1), metadata.size());
        CPPUNIT_ASSERT_EQUAL(std::string("failing"), metadata[0].name);
        CPPUNIT_ASSERT(!engine.fetchCursorBatch(1, 0, rows, complete, error));
        CPPUNIT_ASSERT(error.find("deferred failure") != std::string::npos);
        CPPUNIT_ASSERT(engine.startCursorQuery("SELECT 1", false, nullptr, error));
        CPPUNIT_ASSERT(engine.fetchCursorBatch(1, 0, rows, complete, error));

        // An interrupt before the first fetch stops the query without executing it.
        {
            CQueryEngine preparedOnly;
            CPPUNIT_ASSERT(preparedOnly.startCursorQuery("SELECT count(*) FROM range(10000000000000) a(i)", false, nullptr, error));
            preparedOnly.interrupt();
            CPPUNIT_ASSERT(!preparedOnly.fetchCursorBatch(1, 0, rows, complete, error));
            CPPUNIT_ASSERT(error.find("interrupted") != std::string::npos);
        }

        // An interrupt from another thread stops a long-running first fetch and every later operation.
        CQueryEngine interrupted;
        CPPUNIT_ASSERT(interrupted.startCursorQuery("SELECT count(*) FROM range(10000000000000) a(i)", false, nullptr, error));
        std::thread interrupter([&interrupted]() { MilliSleep(200); interrupted.interrupt(); });
        const bool succeeded = interrupted.fetchCursorBatch(1, 0, rows, complete, error);
        interrupter.join();
        CPPUNIT_ASSERT(!succeeded);
        CPPUNIT_ASSERT(error.find("interrupted") != std::string::npos);
        CPPUNIT_ASSERT(!interrupted.startCursorQuery("SELECT 1", false, nullptr, error));
        CPPUNIT_ASSERT(error.find("interrupted") != std::string::npos);
    }

    void testDecimalBinding()
    {
        CEngineOptions options;
        options.allowLongFormScan = true;
        const char* partsJson = "{\"part\":[{\"copy\":[\"decimal-binding-unused-part\"]}]}";
        const char* optionsJson = "{\"@format\":\"flat\",\"@kind\":\"flat\"}";
        std::string error;
        std::vector<CQueryResultColumnMetadata> metadata;
        const unsigned widthsAndScales[][2] = { { 8, 2 }, { 18, 2 }, { 19, 2 }, { 20, 2 }, { 32, 2 }, { 38, 6 } };
        for (const char* decimalType : { "DECIMAL", "UDECIMAL" })
        {
            for (const auto& widthAndScale : widthsAndScales)
            {
                const unsigned width = widthAndScale[0];
                const unsigned scale = widthAndScale[1];
                const std::string recordEcl = "RECORD " + std::string(decimalType) + std::to_string(width) + "_" + std::to_string(scale) + " amount; END;";
                const std::string longScan = "hpcc_scan('" + recordEcl + "', '" + partsJson + "', '" + optionsJson + "')";
                for (const std::string& scan : { std::string("hpcc_scan('decimal-binding')"), longScan })
                {
                    CQueryEngine engine(nullptr, options);
                    engine.setTestScanMetadata(recordEcl.c_str(), partsJson, optionsJson, false, 0);
                    metadata.clear();
                    const bool prepared = engine.startCursorQuery(("SELECT amount FROM " + scan).c_str(), false, &metadata, error);
                    CPPUNIT_ASSERT_MESSAGE(error, prepared);
                    CPPUNIT_ASSERT_EQUAL(size_t{ 1 }, metadata.size());
                    CPPUNIT_ASSERT_EQUAL(std::string("DECIMAL(") + std::to_string(width) + "," + std::to_string(scale) + ")", metadata[0].typeName);
                    CPPUNIT_ASSERT_EQUAL(static_cast<int>(width), metadata[0].precision);
                    CPPUNIT_ASSERT_EQUAL(static_cast<int>(scale), metadata[0].scale);
                }
            }
        }
    }

    void testEclMetadata()
    {
        Owned<IOutputMetaData> eclMetadata = createWsSql2OutputMetaDataFromEcl("RECORD\n" "    UNSIGNED4 value;\n" "END;", "flat", false);
        CPPUNIT_ASSERT(eclMetadata);
        CPPUNIT_ASSERT(eclMetadata->queryTypeInfo());
        CPPUNIT_ASSERT(eclMetadata->queryTypeInfo()->getType() == type_record);
        CPPUNIT_ASSERT_EQUAL(1U, eclMetadata->queryRecordAccessor(true).getNumFields());
        Owned<IOutputMetaData> conciseEclMetadata = createWsSql2OutputMetaDataFromEcl("{ UNSIGNED4 value };", "flat", false);
        CPPUNIT_ASSERT(conciseEclMetadata);
        CPPUNIT_ASSERT_EQUAL(1U, conciseEclMetadata->queryRecordAccessor(true).getNumFields());
    }

    void testPushdownExecution()
    {
        runWsSql2PushdownTestSuite();
    }

    void testMemoryUsage()
    {
        CSharedQueryDatabase sharedDatabase{ CEngineOptions() };
        const CEngineMemoryUsage start = sharedDatabase.captureMemoryUsage();
        CPPUNIT_ASSERT(start.residentBytes > 0);
        CPPUNIT_ASSERT(start.peakResidentBytes >= start.residentBytes);
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 33))
        CPPUNIT_ASSERT(start.heapAvailable);
        CPPUNIT_ASSERT(start.heapInUseBytes > 0);
#endif
        {
            CQueryEngine engine(sharedDatabase, nullptr);
            std::string error;
            std::string rows;
            bool complete = false;
            CPPUNIT_ASSERT(engine.startCursorQuery("SELECT i FROM range(200000) t(i) ORDER BY i DESC", false, nullptr, error));
            CPPUNIT_ASSERT(engine.fetchCursorBatch(1, 0, rows, complete, error));
            CPPUNIT_ASSERT(!complete);
            const CEngineMemoryUsage current = engine.captureMemoryUsage();
            CPPUNIT_ASSERT_EQUAL(start.liveEngines + 1, current.liveEngines);
            CPPUNIT_ASSERT_EQUAL(start.createdEngines + 1, current.createdEngines);
            CPPUNIT_ASSERT(current.duckdbUsedBytes > start.duckdbUsedBytes);
        }
        const CEngineMemoryUsage released = sharedDatabase.captureMemoryUsage();
        CPPUNIT_ASSERT_EQUAL(start.liveEngines, released.liveEngines);
        CPPUNIT_ASSERT_EQUAL(start.createdEngines + 1, released.createdEngines);
        CPPUNIT_ASSERT_EQUAL(start.duckdbUsedBytes, released.duckdbUsedBytes);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(WsSql2QueryEngineTests);
#endif // _USE_CPPUNIT