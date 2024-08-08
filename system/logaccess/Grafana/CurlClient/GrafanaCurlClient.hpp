/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#pragma once

#include "jlog.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <ctime>
#include "jsecrets.hpp"

#define COMPONENT_NAME "GrafanaLogAccessCurlClient"

static constexpr const char * DEFAULT_DATASOURCE_NAME = "Loki";
static constexpr const char * DEFAULT_DATASOURCE_TYPE = "loki";
static constexpr const char * DEFAULT_DATASOURCE_INDEX = "1";

struct GrafanaDataSource
{
    StringAttr type = DEFAULT_DATASOURCE_TYPE;
    StringAttr name = DEFAULT_DATASOURCE_NAME;
    StringAttr id = DEFAULT_DATASOURCE_INDEX; 
    StringAttr uid;
    //Other Grafana datasource attributes:
    //basicAuthPassword, version, basicAuthUser, access = proxy, isDefault
    //withCredentials, url http://myloki4hpcclogs:3100, secureJsonFields
    //user, password, basicAuth, jsonData, typeLogoUrl, readOnly, database
};

struct LogField
{
    StringAttr name; 
    bool isStream;
    LogField(const char * name, bool isStream = false) : name(name), isStream(isStream) {}
};

static constexpr int defaultEntryLimit = 100;
static constexpr int defaultEntryStart = 0;

class GrafanaLogAccessCurlClient : public CInterfaceOf<IRemoteLogAccess>
{
private:
    static constexpr const char * type = "grafanaloganalyticscurl";
    Owned<IPropertyTree> m_pluginCfg;
    StringBuffer m_grafanaConnectionStr;
    GrafanaDataSource m_targetDataSource;

    StringBuffer m_grafanaUserName;
    StringBuffer m_grafanaPassword;
    StringBuffer m_dataSourcesAPIURI;
    StringAttr m_targetNamespace;

    LogField m_globalSearchCol = LogField("log");
    LogField m_workunitsColumn = LogField("JOBID");
    LogField m_componentsColumn = LogField("component", true);
    LogField m_audienceColumn = LogField("AUD");
    LogField m_classColumn = LogField("CLS");
    LogField m_instanceColumn = LogField("instance", true);
    LogField m_podColumn = LogField("pod", true);
    LogField m_containerColumn = LogField("container", true);
    LogField m_messageColumn = LogField("log");
    LogField m_nodeColumn = LogField("node_name", true);
    LogField m_logDateTimstampColumn = LogField("tsNs");
    //LogField m_logTimestampColumn = LogField("TIME");
    //LogField m_logDatestampColumn = LogField("DATE");
    //LogField m_logSequesnceColumn = LogField("MID");
    //LogField m_logProcIDColumn = LogField("PID");
    //LogField m_logThreadIDColumn = LogField("TID");
    //LogField m_logTraceIDColumn = LogField("TRC");
    //LogField m_logSpanIDColumn = LogField("SPN");

    StringAttr m_expectedLogFormat; //json|table|xml

public:
    GrafanaLogAccessCurlClient(IPropertyTree & logAccessPluginConfig);
    void processQueryJsonResp(LogQueryResultDetails & resultDetails, const std::string & retrievedDocument, StringBuffer & returnbuf, const LogAccessLogFormat format, const LogAccessReturnColsMode retcolmode, bool reportHeader); 
    void processDatasourceJsonResp(const std::string & retrievedDocument);
    void processValues(StringBuffer & returnbuf, IPropertyTreeIterator * valuesIter, IPropertyTree * stream, LogAccessLogFormat format, const LogAccessReturnColsMode retcolmode, bool & isFirstLine);
    void fetchDatasourceByName(const char * targetDataSourceName);
    void fetchDatasources(std::string & readBuffer);
    void fetchLabels(std::string & readBuffer);
    void submitQuery(std::string & readBuffer, const char * targetURI);

    void populateQueryFilterAndStreamSelector(StringBuffer & queryString, StringBuffer & streamSelector, const ILogAccessFilter * filter);
    static void timestampQueryRangeString(StringBuffer & range, std::time_t from, std::time_t to);

    // IRemoteLogAccess methods
    virtual bool fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format) override;
    virtual const char * getRemoteLogAccessType() const override { return type; }
    virtual IPropertyTree * queryLogMap() const override { return m_pluginCfg->queryPropTree(""); }
    virtual const char * fetchConnectionStr() const override { return m_grafanaConnectionStr.str(); }
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format) override;
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize) override;
    virtual bool supportsResultPaging() const override { return false;}
};