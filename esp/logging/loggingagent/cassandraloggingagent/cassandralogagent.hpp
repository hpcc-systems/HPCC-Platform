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

#ifndef _CASSANDRALOGAGENT_HPP__
#define _CASSANDRALOGAGENT_HPP__

#include "jmisc.hpp"
#include "eclhelper.hpp"
#include "cassandra.h"
#include "cassandraembed.hpp"
#include "loggingcommon.hpp"
#include "loggingagentbase.hpp"
#include "datafieldmap.hpp"

using namespace cassandraembed;

#ifdef CASSANDRALOGAGENT_EXPORTS
    #define CASSANDRALOGAGENT_API DECL_EXPORT
#else
    #define CASSANDRALOGAGENT_API DECL_IMPORT
#endif

class CCassandraLogAgent : public CDBLogAgentBase
{
    StringBuffer dbServer, dbUserID, dbPassword;
    Owned<CassandraClusterSession> cassSession;
    CriticalSection transactionSeedCrit;

    void initKeySpace();
    void ensureDefaultKeySpace();
    void setSessionOptions(const char *keyspace);
    void ensureTransSeedTable();
    void createTable(const char *dbName, const char *tableName, StringArray& columnNames, StringArray& columnTypes, const char* keys);
    void executeSimpleStatement(const char *st);
    void executeSimpleStatement(StringBuffer& st);
    bool executeSimpleSelectStatement(const char* st, unsigned& resultValue);

    virtual void queryTransactionSeed(const char* appName, StringBuffer& seed);
    virtual void addField(CLogField& logField, const char* name, StringBuffer& value, StringBuffer& fields, StringBuffer& values);
    virtual void setUpdateLogStatement(const char* dbName, const char* tableName,
        const char* fields, const char* values, StringBuffer& statement);
    virtual void executeUpdateLogStatement(StringBuffer& statement);

public:
    IMPLEMENT_IINTERFACE;

    CCassandraLogAgent() {};
    virtual ~CCassandraLogAgent() { logGroups.kill(); logSources.kill(); };

    virtual bool init(const char* name, const char* type, IPropertyTree* cfg, const char* process);
};

#endif //_CASSANDRALOGAGENT_HPP__
