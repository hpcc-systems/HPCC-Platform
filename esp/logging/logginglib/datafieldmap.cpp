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

#include "datafieldmap.hpp"
static const char* const defaultLogSourcePath = "Source";

void ensureInputString(const char* input, bool lowerCase, StringBuffer& outputStr, int code, const char* msg)
{
    outputStr.set(input).trim();
    if (outputStr.isEmpty())
        throw MakeStringException(code, "%s", msg);
    if (lowerCase)
        outputStr.toLowerCase();
}

void readLogGroupCfg(IPropertyTree* cfg, StringAttr& defaultLogGroup, MapStringToMyClass<CLogGroup>& logGroups)
{
    StringBuffer groupName;
    Owned<IPropertyTreeIterator> iter = cfg->getElements("LogGroup");
    ForEach(*iter)
    {
        ensureInputString(iter->query().queryProp("@name"), true, groupName, -1, "LogGroup @name required");
        Owned<CLogGroup> logGroup = new CLogGroup(groupName.str());
        logGroup->loadMappings(iter->query());
        logGroups.setValue(groupName.str(), logGroup);
        if (defaultLogGroup.isEmpty())
            defaultLogGroup.set(groupName.str());
    }
}

void readLogSourceCfg(IPropertyTree* cfg, unsigned& logSourceCount, StringAttr& logSourcePath, MapStringToMyClass<CLogSource>& logSources)
{
    logSourceCount = 0;
    StringBuffer name, groupName, dbName;
    Owned<IPropertyTreeIterator> iter = cfg->getElements("LogSourceMap/LogSource");
    ForEach(*iter)
    {
        ensureInputString(iter->query().queryProp("@name"), false, name, -1, "LogSource @name required");
        ensureInputString(iter->query().queryProp("@mapToLogGroup"), true, groupName, -1, "LogSource @mapToLogGroup required");
        ensureInputString(iter->query().queryProp("@mapToDB"), true, dbName, -1, "LogSource @mapToDB required");
        Owned<CLogSource> logSource = new CLogSource(name.str(), groupName.str(), dbName.str());
        logSources.setValue(name.str(), logSource);
        logSourceCount++;
    }

    //xpath to read log source from log request
    logSourcePath.set(cfg->hasProp("LogSourcePath") ? cfg->queryProp("LogSourcePath") : defaultLogSourcePath);
}

void CLogTable::loadMappings(IPropertyTree& fieldList)
{
    StringBuffer name, mapTo, fieldType, defaultValue;
    Owned<IPropertyTreeIterator> itr = fieldList.getElements("Field");
    ForEach(*itr)
    {
        IPropertyTree &map = itr->query();

        ensureInputString(map.queryProp("@name"), false, name, -1, "Field @name required");
        ensureInputString(map.queryProp("@mapTo"), true, mapTo, -1, "Field @mapTo required");
        ensureInputString(map.queryProp("@type"), true, fieldType, -1, "Field @type required");
        defaultValue = map.queryProp("@default");
        defaultValue.trim();

        Owned<CLogField> field = new CLogField(name.str(), mapTo.str(), fieldType.str());
        if (!defaultValue.isEmpty())
            field->setDefault(defaultValue.str());
        logFields.append(*field.getClear());
    }
}

void CLogGroup::loadMappings(IPropertyTree& fieldList)
{
    StringBuffer tableName;
    bool enableLogID;
    Owned<IPropertyTreeIterator> itr = fieldList.getElements("Fieldmap");
    ForEach(*itr)
    {
        ensureInputString(itr->query().queryProp("@table"), true, tableName, -1, "Fieldmap @table required");
        enableLogID = itr->query().getPropBool("@enableLogID", true);

        Owned<CLogTable> table = new CLogTable(tableName.str(), enableLogID);
        table->loadMappings(itr->query());
        CIArrayOf<CLogField>& logFields = table->getLogFields();
        if (logFields.length() < 1)
            throw MakeStringException(-1,"No Fieldmap for %s", tableName.str());

        logTables.append(*table.getClear());
    }
}
