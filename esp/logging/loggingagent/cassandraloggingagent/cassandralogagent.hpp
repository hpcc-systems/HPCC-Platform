/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems.

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

#pragma warning (disable : 4786)

#ifndef _CASSABDRALOGAGENT_HPP__
#define _CASSABDRALOGAGENT_HPP__

#include "jmisc.hpp"
#include "eclhelper.hpp"
#include "cassandra.h"
#include "cassandraembed.hpp"
#include "loggingcommon.hpp"
#include "loggingagentbase.hpp"
#include "dbfieldmap.hpp"

using namespace cassandraembed;

#ifdef WIN32
    #ifdef CASSABDRALOGAGENT_EXPORTS
        #define CASSABDRALOGAGENT_API __declspec(dllexport)
    #else
        #define CASSABDRALOGAGENT_API __declspec(dllimport)
    #endif
#else
    #define CASSABDRALOGAGENT_API
#endif

class CCassandraLogAgent : public CInterface, implements IEspLogAgent
{
    StringBuffer dbServer, defaultDB, dbUserID, dbPassword, transactionSeed;
    StringAttr agentName, defaultLogGroup, defaultTransactionApp, loggingTransactionApp;
    unsigned logSourceCount, loggingTransactionCount, maxTriesGTS;
    MapStringToMyClass<CLogGroup> logGroups;
    MapStringToMyClass<CLogSource> logSources;
    Owned<CassandraClusterSession> cassSession;
    CriticalSection uniqueIDCrit, transactionSeedCrit;

    CLogGroup* checkLogSource(IPropertyTree* logRequest, StringBuffer& source, StringBuffer& logDB);
    void getLoggingTransactionID(StringBuffer& id);
    bool buildUpdateLogStatement(IPropertyTree* logRequest, const char* logDB, CLogTable& table, StringBuffer& logID, StringBuffer& cqlStatement);
    void addField(CLogField& logField, const char* name, StringBuffer& value, StringBuffer& fields, StringBuffer& values);
    void appendFieldInfo(const char* field, StringBuffer& value, StringBuffer& fields, StringBuffer& values, bool quoted);
    void addMissingFields(CIArrayOf<CLogField>& logFields, BoolHash& HandledFields, StringBuffer& fields, StringBuffer& values);

    void initKeySpace();
    void ensureKeySpace();
    void setKeySpace(const char *keyspace);
    void initTransSeedTable();
    void queryTransactionSeed(const char* appName, StringBuffer& seed);
    void createTable(const char *dbName, const char *tableName, StringArray& columnNames, StringArray& columnTypes, const char* keys);
    //void executeSimpleStatement(CassSession *session, const char *st);
    void executeSimpleStatement(const char *st);
    void executeSimpleStatement(StringBuffer& st);
    unsigned executeSimpleSelectStatement(const char* st, unsigned& resultValue);

public:
    IMPLEMENT_IINTERFACE;

    CCassandraLogAgent() {};
    virtual ~CCassandraLogAgent() { logGroups.kill(); logSources.kill(); };

    virtual bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);
    virtual bool getTransactionSeed(IEspGetTransactionSeedRequest& req, IEspGetTransactionSeedResponse& resp);
    virtual bool updateLog(IEspUpdateLogRequestWrap& req, IEspUpdateLogResponse& resp);
    virtual void filterLogContent(IEspUpdateLogRequestWrap* req);
};

#endif //_CASSABDRALOGAGENT_HPP__
