/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

static const char* WSSQLACCESS = "SQLAccess";

class Cws_sqlSoapBindingEx : public Cws_sqlSoapBinding
{
public:
    Cws_sqlSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : Cws_sqlSoapBinding(cfg, name, process, llevel)
    {
    }
};

class Cws_sqlEx : public Cws_sql
{
private:
    BoolHash validClusters;
    CriticalSection crit;

    IPropertyTree *cfg;
    std::map<std::string,std::string> cachedSQLQueries;
    Owned<IEnvironmentFactory>  m_envFactory;
    CTpWrapper                  m_TpWrapper;

public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *_cfg, const char *_process, const char *_service);

    bool onPrepareSQL(IEspContext &context, IEspPrepareSQLRequest &req, IEspPrepareSQLResponse &resp);
    bool onExecuteSQL(IEspContext &context, IEspExecuteSQLRequest &req, IEspExecuteSQLResponse &resp);
    bool getWUResult(IEspContext &context, const char * wuid, StringBuffer &resp, unsigned start, unsigned count);
    bool onExecutePreparedSQL(IEspContext &context, IEspExecutePreparedSQLRequest &req, IEspExecutePreparedSQLResponse &resp);
    bool onGetDBSystemInfo(IEspContext &context, IEspGetDBSystemInfoRequest &req, IEspGetDBSystemInfoResponse &resp);
    bool onGetDBMetaData(IEspContext &context, IEspGetDBMetaDataRequest &req, IEspGetDBMetaDataResponse &resp);
    bool onGetResults(IEspContext &context, IEspGetResultsRequest &req, IEspGetResultsResponse &resp);
    void refreshValidClusters();
    bool isValidCluster(const char *cluster);

    void fetchRequiredHpccFiles(IArrayOf<SQLTable> * sqltables);
    static void fetchRequiredHpccFiles(IArrayOf<SQLTable> * sqltables, HpccFiles * hpccfilecache);
    HPCCSQLTreeWalker * parseSQL(IEspContext &context, StringBuffer & sqltext);

    bool executePublishedQueryByname(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count);
    bool executePublishedQueryByWuId(IEspContext &context, const char * targetwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, const char * targetcluster, int start, int count);
    bool executePublishedQuery(IEspContext &context, const char * queryset, const char * queryname, StringBuffer &resp, int start, int count, int waittime);
    bool executePublishedQuery(IEspContext &context, const char * wuid, StringBuffer &resp, int start, int count, int waittime);

    bool cloneAndExecuteWU(IEspContext &context, const char * originalwuid, StringBuffer &clonedwuid, const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, const char * targetcluster);
    bool publishWorkunit(IEspContext &context, const char * queryname, const char * wuid, const char * targetcluster);

    void createXMLParams(StringBuffer & xmlparams, HPCCSQLTreeWalker* parsedSQL, IArrayOf<IConstNamedValue> *variables, IConstWorkUnit * cw);
};

#endif //_ESPWIZ_WS_SQL_HPP__
