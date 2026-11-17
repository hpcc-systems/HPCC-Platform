#ifndef _ESP_WS_SQL2_SERVICE_HPP_
#define _ESP_WS_SQL2_SERVICE_HPP_

#include "ws_sql2_esp.ipp"
#include "engine.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

class CWsSQL2Ex : public CWsSQL2
{
    struct CActiveResultSet
    {
        std::unique_ptr<CQueryEngine> engine;
        std::string owner;
        std::mutex operationMutex;
        std::chrono::steady_clock::time_point created;
        std::chrono::steady_clock::time_point lastAccess;
        // The CWsSQL2ErrorCode reported for an operation stopped by CloseResultSet or the cursor's maximum
        // lifetime; zero when the cursor has not been cancelled. The first reason recorded is kept.
        std::atomic<int> cancelCode{0};
        bool includeRuntimeGraph = false;
        bool includeDebug = false;
        CEngineMemoryUsage debugStart;
#ifdef _DEBUG
        CScanObjectAudit scanObjectAudit;
#endif
    };

    CEngineOptions engineOptions;
    // A single DuckDB instance shared by every concurrent request, so engineOptions' memoryLimit/threads bound the
    // whole service instead of being multiplied by the number of concurrent requests. Declared before
    // activeResultSets so it outlives every CQueryEngine's duckdb::Connection into it, even during destruction.
    std::unique_ptr<CSharedQueryDatabase> sharedQueryDatabase;
    // Maximum serialized result batch size in bytes; 0 means unlimited.
    unsigned maxByteLimit = 64 * 1024 * 1024;
    unsigned defaultFetchSize = 1000;
    unsigned maxFetchSize = 10000;
    unsigned maxActiveCursors = 100;
    unsigned maxCursorsPerUser = 10;
    unsigned cursorIdleTimeoutSeconds = 300;
    unsigned cursorMaxLifetimeSeconds = 3600;
    // Memory growth, after an IncludeDebug query is released, above which the leak check reports a possible leak.
    unsigned leakCheckThresholdBytes = 1024 * 1024;
    std::mutex cursorRegistryMutex;
    std::condition_variable cursorCleanupCondition;
    std::unordered_map<std::string, std::shared_ptr<CActiveResultSet>> activeResultSets;
    std::thread cursorCleanupThread;
    bool stopCursorCleanup = false;

    unsigned resolveFetchSize(unsigned requested) const;
    static void recordCancellation(CActiveResultSet& resultSet, CWsSQL2ErrorCode code);
    [[noreturn]] static void throwResultSetFailure(const CActiveResultSet& resultSet, const char* message);
    void reportDebugInfo(IEspDebugInfo& debugInfo, const CActiveResultSet& resultSet, const CEngineMemoryUsage& current, bool complete, unsigned __int64 responseBytes);
    void removeExpiredCursors();
    void cursorCleanupLoop();

public:
    IMPLEMENT_IINTERFACE;

    ~CWsSQL2Ex();
    void init(IPropertyTree* cfg, const char* process, const char* service) override;
    bool onSplitQuery(IEspContext& context, IEspSplitQueryRequest& req, IEspSplitQueryResponse& resp);
    bool onExplainQuery(IEspContext& context, IEspExplainQueryRequest& req, IEspExplainQueryResponse& resp);
    bool onExecuteQuery(IEspContext& context, IEspExecuteQueryRequest& req, IEspExecuteQueryResponse& resp);
    bool onFetchResultSet(IEspContext& context, IEspFetchResultSetRequest& req, IEspFetchResultSetResponse& resp);
    bool onCloseResultSet(IEspContext& context, IEspCloseResultSetRequest& req, IEspCloseResultSetResponse& resp);
    bool onLogicalFileMetadata(IEspContext& context, IEspLogicalFileMetadataRequest& req, IEspLogicalFileMetadataResponse& resp);
};

class CWsSQL2SoapBindingEx : public CWsSQL2SoapBinding
{
public:
    CWsSQL2SoapBindingEx(http_soap_log_level level = hsl_none) : CWsSQL2SoapBinding(level) {}
    CWsSQL2SoapBindingEx(IPropertyTree* cfg, const char* bindName, const char* procName, http_soap_log_level level = hsl_none)
        : CWsSQL2SoapBinding(cfg, bindName, procName, level) {
    }
};

#endif