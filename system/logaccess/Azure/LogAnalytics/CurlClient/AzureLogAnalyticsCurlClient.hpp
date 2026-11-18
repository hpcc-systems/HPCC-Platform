/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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
#include "jmutex.hpp"
#include <ctime>
#include <chrono>
#include "jsecrets.hpp"

#define COMPONENT_NAME "AzureLogAnalyticsCurlClient"

static constexpr int defaultEntryLimit = 100;
static constexpr int defaultEntryStart = 0;

// Authentication mode is auto-detected once at construction, in priority order matching
// common/remote/hooks/azure/azureapiutils.cpp's getAzureManagedIdentityCredential(): Workload
// Identity (AKS-injected federated token), then legacy Managed Identity (IMDS), else client secret.
enum class AzureLogAccessAuthMode
{
    ClientSecret,
    WorkloadIdentity,
    ManagedIdentity
};

class AzureLogAnalyticsCurlClient : public CInterfaceOf<IRemoteLogAccess>
{
private:
    static constexpr const char * type = "azureloganalyticscurl";
    Owned<IPropertyTree> m_pluginCfg;

    StringBuffer m_globalIndexSearchPattern;
    StringBuffer m_globalSearchColName;
    StringBuffer m_globalIndexTimestampField;

    StringBuffer m_workunitSearchColName;
    StringBuffer m_workunitIndexSearchPattern;

    bool m_disableComponentNameJoins = false;
    StringBuffer m_componentsSearchColName;
    StringBuffer m_componentsIndexSearchPattern;
    StringBuffer m_componentsTimestampField;

    StringBuffer m_audienceSearchColName;
    StringBuffer m_audienceIndexSearchPattern;

    StringBuffer m_classSearchColName;
    StringBuffer m_classIndexSearchPattern;

    StringBuffer m_instanceSearchColName;
    StringBuffer m_instanceIndexSearchPattern;

    StringBuffer m_hostSearchColName;
    StringBuffer m_hostIndexSearchPattern;

    StringBuffer m_podIndexSearchPattern;
    StringBuffer m_podSearchColName;

    StringBuffer m_traceIndexSearchPattern;
    StringBuffer m_traceSearchColName;

    StringBuffer m_spanIndexSearchPattern;
    StringBuffer m_spanSearchColName;

    StringBuffer m_logAnalyticsWorkspaceID;
    StringBuffer m_aadTenantID;
    StringBuffer m_aadClientID;
    StringBuffer m_aadClientSecret;
    StringBuffer m_workloadIdentityFederatedTokenFile;

    AzureLogAccessAuthMode m_authMode = AzureLogAccessAuthMode::ClientSecret;
    mutable CriticalSection m_accessTokenCS{SYNC_LOCATION};
    mutable CriticalSection m_accessTokenRefreshCS{SYNC_LOCATION};
    mutable CriticalSection m_authFailureCS{SYNC_LOCATION};
    mutable StringBuffer m_cachedAccessToken;
    mutable std::chrono::steady_clock::time_point m_accessTokenExpires;
    mutable StringBuffer m_cachedAuthFailure;
    mutable std::chrono::steady_clock::time_point m_authFailureUntil;

    StringBuffer m_componentsLookupKeyColumn;
    StringBuffer m_instanceLookupKeyColumn;

    bool targetIsContainerLogV2 = false;
    bool m_blobMode = false;

    // Returns true (and sets token) if a cached, unexpired access token is available.
    bool tryGetCachedAccessToken(StringBuffer & token) const;
    // Throws if a recent authentication failure is still within its cooldown window.
    void checkAuthFailureCooldown() const;
    void requestAccessToken(StringBuffer & token) const;

public:
    AzureLogAnalyticsCurlClient(IPropertyTree & logAccessPluginConfig);

    void getMinReturnColumns(StringBuffer & columns, const bool includeComponentName);
    void getDefaultReturnColumns(StringBuffer & columns, const bool includeComponentName);
    void searchMetaData(StringBuffer & search, const LogAccessReturnColsMode retcolmode, const  StringArray & selectcols, bool & includeComponentName, unsigned size = defaultEntryLimit, offset_t from = defaultEntryStart);
    void populateKQLQueryString(StringBuffer & queryString, StringBuffer& queryIndex, const LogAccessConditions & options);
    void populateKQLQueryString(StringBuffer & queryString, StringBuffer& queryIndex, const ILogAccessFilter * filter);
    void declareContainerIndexJoinTable(StringBuffer & queryString, const LogAccessConditions & options);
    static void azureLogAnalyticsTimestampQueryRangeString(StringBuffer & range, const char * timestampfield, std::time_t from, std::time_t to);
    static void azureLogAnalyticsQueryTimeSpanString(StringBuffer & queryTimeSpan, std::time_t from, std::time_t to);
    static unsigned processHitsJsonResp(IPropertyTreeIterator * lines, IPropertyTreeIterator * columns, StringBuffer & returnbuf, LogAccessLogFormat format, bool wrapped, bool reportHeader);
    static bool processSearchJsonResp(LogQueryResultDetails & resultDetails, const std::string & retrievedDocument, StringBuffer & returnbuf, LogAccessLogFormat format, bool reportHeader);

    // IRemoteLogAccess methods
    virtual bool fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format) override;
    virtual const char * getRemoteLogAccessType() const override { return type; }
    virtual IPropertyTree * queryLogMap() const override { return m_pluginCfg->queryPropTree(""); }
    virtual const char * fetchConnectionStr() const override { return m_logAnalyticsWorkspaceID.str(); }
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format) override;
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize) override;
    virtual bool supportsResultPaging() const override { return false;}
    virtual void healthReport(LogAccessHealthReportOptions options, LogAccessHealthReportDetails & report) override;
};
