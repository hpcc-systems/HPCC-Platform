/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
############################################################################## */

#ifndef _ESPWIZ_WS_SQL_HPP__
#define _ESPWIZ_WS_SQL_HPP__

#include <build-config.h>

#include "ws_sql.hpp"
#include "ws_sql_esp.ipp"

#include "ws_ecl_wuinfo.hpp"
#include "ws_workunitsHelpers.hpp"
#include "ws_dfuService.hpp"
#include "fileview.hpp"
#include "environment.hpp"
#include "TpWrapper.hpp"

#include "HPCCFileCache.hpp"
#include "HPCCFile.hpp"
#include "ECLEngine.hpp"
#include "SQLTable.hpp"

#include "HPCCSQLLexer.h"
#include "HPCCSQLParser.h"

#include "HPCCSQLTreeWalker.hpp"

#include "dautils.hpp"

#define EMBEDDEDSQLQUERYCOMMENT "\n\n/****************************************************\nOriginal SQL:   \"%s\"\nNormalized SQL: \"%s\"\n****************************************************/\n"

static const char* WSSQLACCESS = "WsSqlAccess";
static const char* WSSQLRESULT = "WsSQLResult";
static const char* WSSQLCOUNT  = "WsSQLCount";
static const char* WSSQLRESULTSCHEMA = "WsSQLResultSchema";

static StringBuffer g_wssqlBuildVersion;

class CwssqlSoapBindingEx : public CwssqlSoapBinding
{
public:
    CwssqlSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CwssqlSoapBinding(cfg, name, process, llevel)
    {
    }
};

class CwssqlEx : public Cwssql
{
private:
    IPropertyTree *cfg;
    std::map<std::string,std::string> cachedSQLQueries;

    static const unsigned int ExpireSeconds = 60 * 60;

    CriticalSection critCache;
    bool isQueryCached(const char * sqlQuery);
    bool getCachedQuery(const char * sqlQuery, StringBuffer & wuid);
    bool addQueryToCache(const char * sqlQuery, const char * wuid);
    void removeQueryFromCache(const char * sqlQuery);
    time_t cacheFlushTime;

    bool isCacheExpired()
    {
        time_t timeNow;
        time(&timeNow);
        return difftime(timeNow, cacheFlushTime) > ExpireSeconds;
    }

    void setNewCacheFlushTime()
    {
        time(&cacheFlushTime);
    }

    void setWsSqlBuildVersion(const char* buildVersion)
    {
        g_wssqlBuildVersion.clear();
        if(buildVersion&&*buildVersion)
            g_wssqlBuildVersion.set(buildVersion);

        g_wssqlBuildVersion.trim();
    }

public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *_cfg, const char *_process, const char *_service);

    bool onEcho(IEspContext &context, IEspEchoRequest &req, IEspEchoResponse &resp);
    bool onPrepareSQL(IEspContext &context, IEspPrepareSQLRequest &req, IEspPrepareSQLResponse &resp);
    bool onExecuteSQL(IEspContext &context, IEspExecuteSQLRequest &req, IEspExecuteSQLResponse &resp);
    bool getWUResult(IEspContext &context, const char * wuid, StringBuffer &resp, unsigned start, unsigned count, int sequence, const char * dsname, const char * schemaname);
    bool onExecutePreparedSQL(IEspContext &context, IEspExecutePreparedSQLRequest &req, IEspExecutePreparedSQLResponse &resp);
    bool onGetDBSystemInfo(IEspContext &context, IEspGetDBSystemInfoRequest &req, IEspGetDBSystemInfoResponse &resp);
    bool onGetDBMetaData(IEspContext &context, IEspGetDBMetaDataRequest &req, IEspGetDBMetaDataResponse &resp);
    bool onGetResults(IEspContext &context, IEspGetResultsRequest &req, IEspGetResultsResponse &resp);
    bool onGetRelatedIndexes(IEspContext &context, IEspGetRelatedIndexesRequest &req, IEspGetRelatedIndexesResponse &resp);
    bool onSetRelatedIndexes(IEspContext &context, IEspSetRelatedIndexesRequest &req, IEspSetRelatedIndexesResponse &resp);
    bool onCreateTableAndLoad(IEspContext &context, IEspCreateTableAndLoadRequest &req, IEspCreateTableAndLoadResponse &resp);

    void processMultipleClusterOption(StringArray & clusters, const char  * targetcluster, StringBuffer & hashoptions);

    void fetchRequiredHpccFiles(IArrayOf<SQLTable> * sqltables);
    static void fetchRequiredHpccFiles(IArrayOf<SQLTable> * sqltables, HpccFiles * hpccfilecache);
    HPCCSQLTreeWalker * parseSQL(IEspContext &context, StringBuffer & sqltext, bool attemptParameterization = true);

    bool executePublishedQueryByName(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count);
    bool executePublishedQueryByWuId(IEspContext &context, const char * targetwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count);
    bool executePublishedQuery(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &resp, int start, int count, int waittime);
    bool executePublishedQuery(IEspContext &context, const char * wuid, StringBuffer &resp, int start, int count, int waittime);
    bool cloneAndExecuteWU(IEspContext &context, const char * originalwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, const char * targetcluster);
    bool publishWorkunit(IEspContext &context, const char * queryname, const char * wuid, const char * targetcluster);

    static void createWUXMLParams(StringBuffer & xmlparams, HPCCSQLTreeWalker* parsedSQL, IArrayOf<IConstNamedValue> *variables, IConstWorkUnit * cw);
    static void createWUXMLParams(StringBuffer & xmlparams, const IArrayOf <ISQLExpression> * parameterlist);

    const char* getWsSqlBuildVersion()
    {
        return g_wssqlBuildVersion.str();
    }
};

#endif //_ESPWIZ_WS_SQL_HPP__
