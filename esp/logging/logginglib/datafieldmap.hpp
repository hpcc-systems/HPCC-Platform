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

#ifndef _DATAFIELDMAP_HPP__
#define _DATAFIELDMAP_HPP__

#pragma warning (disable : 4786)

#include "jiface.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "loggingcommon.hpp"

class CLogField : public CInterface
{
    StringAttr  name;
    StringAttr  mapTo;
    StringAttr  type;
    StringAttr  defaultValue;

public:
    CLogField(const char* _name, const char* _mapTo, const char* _type)
        : name(_name), mapTo(_mapTo), type(_type) {};
    virtual ~CLogField() {};

    const char* getName() { return name.get(); };
    const char* getMapTo() { return mapTo.get(); };
    const char* getType() { return type.get(); };
    const char* getDefault() { return defaultValue.get(); };
    void setDefault(const char* value) { defaultValue.set(value); };
};

class CLogTable : public CInterface
{
    StringAttr tableName;
    bool enableLogID;
    CIArrayOf<CLogField> logFields;
public:
    CLogTable(const char* _tableName, bool _enableLogID) : tableName(_tableName), enableLogID(_enableLogID) {};
    virtual ~CLogTable() {};

    const char* getTableName() { return tableName.get(); };
    bool getEnableLogID() { return enableLogID; }
    void setTableName(const char* _tableName) { return tableName.set(_tableName); };
    void loadMappings(IPropertyTree& cfg);
    CIArrayOf<CLogField>& getLogFields() { return logFields; };
};

class CLogGroup : public CInterface, implements IInterface
{
    StringAttr name;
    CIArrayOf<CLogTable> logTables;
public:
    IMPLEMENT_IINTERFACE;
    CLogGroup(const char* _name) : name(_name) {};
    virtual ~CLogGroup() {};

    void loadMappings(IPropertyTree& cfg);
    CIArrayOf<CLogTable>& getLogTables() { return logTables; };
};

class CLogSource : public CInterface, implements IInterface
{
    StringAttr name;
    StringAttr groupName;
    StringAttr dbName;
public:
    IMPLEMENT_IINTERFACE;
    CLogSource(const char* _name, const char* _groupName, const char* _dbName)
        : name(_name), groupName(_groupName), dbName(_dbName) {};
    virtual ~CLogSource() {};

    const char* getName() { return name.get(); };
    const char* getGroupName() { return groupName.get(); };
    const char* getDBName() { return dbName.get(); };
};

extern LOGGINGCOMMON_API void ensureInputString(const char* input, bool lowerCase, StringBuffer& outputStr, int code, const char* msg);
extern LOGGINGCOMMON_API void readLogGroupCfg(IPropertyTree* cfg, StringAttr& defaultLogGroup, MapStringToMyClass<CLogGroup>& logGroups);
extern LOGGINGCOMMON_API void readLogSourceCfg(IPropertyTree* cfg, unsigned& logSourceCount, StringAttr& logSourcePath, MapStringToMyClass<CLogSource>& logGroups);

#endif // !_DATAFIELDMAP_HPP__
