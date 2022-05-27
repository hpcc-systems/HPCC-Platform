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

#include "AzureLogAnalyticsCurlClient.hpp"

#include "platform.h"
#include <curl/curl.h>
#include <string>
#include <vector>

#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <ctime> 

static constexpr const char * DEFAULT_TS_NAME = "TimeGenerated";
static constexpr const char * DEFAULT_INDEX_PATTERN = "ContainerLog";

static constexpr const char * DEFAULT_HPCC_LOG_SEQ_COL         = "hpcc_log_sequence";
static constexpr const char * DEFAULT_HPCC_LOG_TIMESTAMP_COL   = "hpcc_log_timestamp";
static constexpr const char * DEFAULT_HPCC_LOG_PROCID_COL      = "hpcc_log_procid";
static constexpr const char * DEFAULT_HPCC_LOG_THREADID_COL    = "hpcc_log_threadid";
static constexpr const char * DEFAULT_HPCC_LOG_MESSAGE_COL     = "hpcc_log_message";
static constexpr const char * DEFAULT_HPCC_LOG_JOBID_COL       = "hpcc_log_jobid";
static constexpr const char * DEFAULT_HPCC_LOG_COMPONENT_COL   = "kubernetes_container_name";
static constexpr const char * DEFAULT_HPCC_LOG_TYPE_COL        = "hpcc_log_class";
static constexpr const char * DEFAULT_HPCC_LOG_AUD_COL         = "hpcc_log_audience";

static constexpr const char * LOGMAP_INDEXPATTERN_ATT = "@storeName";
static constexpr const char * LOGMAP_SEARCHCOL_ATT = "@searchColumn";
static constexpr const char * LOGMAP_TIMESTAMPCOL_ATT = "@timeStampColumn";

static constexpr std::size_t  DEFAULT_MAX_RECORDS_PER_FETCH = 100;

static bool getEnvVar(const char * varName, StringBuffer & varValue)
{
    if (isEmptyString(varName))
        return false;

    try
    {
        varValue.set(std::getenv(varName));
        if (varValue.isEmpty())
        {
            PROGLOG("Could not find environment variable '%s'!", varName);
            return false;
        }
    }
    catch(const std::exception& e)
    {
        ERRLOG("Encountered error fetching '%s' environment variable: '%s'.", varName, e.what());
        return false;
    }

    return true;
}

static bool getAzureLogAnalyticsAPIAccessTokenEnvVars(StringBuffer & tenantID, StringBuffer & clientID, StringBuffer & clientSecret)
{
    bool success = true;
    success &= getEnvVar("AZURE_TENANT_ID", tenantID);
    success &= getEnvVar("AZURE_CLIENT_ID", clientID);
    success &= getEnvVar("AZURE_CLIENT_SECRET", clientSecret);

   return success;
}

static size_t captureIncomingCURLReply(void* contents, size_t size, size_t nmemb, void* userp)
{
    size_t          incomingDataSize = size * nmemb;
    MemoryBuffer*   mem = static_cast<MemoryBuffer*>(userp);
    size_t          MAX_BUFFER_SIZE = 4194304; // 2^22

    if ((mem->length() + incomingDataSize) < MAX_BUFFER_SIZE)
    {
        mem->append(incomingDataSize, contents);
    }
    else
    {
        // Signals an error to libcurl
        incomingDataSize = 0;
        PROGLOG("%s::captureIncomingCURLReply exceeded buffer size %zu", COMPONENT_NAME, MAX_BUFFER_SIZE);
    }

    return incomingDataSize;
}

static void requestLogAnalyticsAccessToken(StringBuffer & token, const char * clientID, const char * clientSecret, const char * tenantID)
{
    if (isEmptyString(clientID))
        throw makeStringExceptionV(-1, "%s Access token request: Azure Active Directory Application clientID is required!", COMPONENT_NAME);

    if (isEmptyString(tenantID))
        throw makeStringExceptionV(-1, "%s Access token request: Azure tenantID is required!", COMPONENT_NAME);

    if (isEmptyString(clientSecret))
        throw makeStringExceptionV(-1, "%s Access token request: Azure Active Directory Application Secret is required!", COMPONENT_NAME);

    CURL * curlHandle = curl_easy_init();
    if (curlHandle)
    {
        CURLcode                curlResponseCode;
        size_t                  INITIAL_BUFFER_SIZE = 32768; // 2^15
        MemoryBuffer            captureBuffer(INITIAL_BUFFER_SIZE);
        char                    curlErrBuffer[CURL_ERROR_SIZE];
        curlErrBuffer[0] = '\0';

        VStringBuffer tokenRequestURL("https://login.microsoftonline.com/%s/oauth2/token", tenantID);
        VStringBuffer tokenRequestFields("grant_type=client_credentials&resource=https://api.loganalytics.io&client_secret=%s&client_id=%s", clientSecret, clientID);

        try
        {
            /*"curl -X POST -d 'grant_type=client_credentials&client_id=<ADApplicationID>&client_secret=<thesecret>&resource=https://api.loganalytics.io'
               https://login.microsoftonline.com/<tenantID>/oauth2/token
            "*/

            curl_easy_setopt(curlHandle, CURLOPT_URL, tokenRequestURL.str());
            curl_easy_setopt(curlHandle, CURLOPT_POST, 1);
            curl_easy_setopt(curlHandle, CURLOPT_POSTFIELDS, tokenRequestFields.str());
            curl_easy_setopt(curlHandle, CURLOPT_NOPROGRESS, 1);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, captureIncomingCURLReply);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, static_cast<void*>(&captureBuffer));
            curl_easy_setopt(curlHandle, CURLOPT_ERRORBUFFER, curlErrBuffer);
            curl_easy_setopt(curlHandle, CURLOPT_USERAGENT, "HPCC Systems Log Access client");
            curl_easy_setopt(curlHandle, CURLOPT_FAILONERROR, 1L); // non HTTP Success treated as error

            curlResponseCode = curl_easy_perform(curlHandle);
        }
        catch (...)
        {
            throw makeStringExceptionV(-1, "%s Access token request: Unknown error!", COMPONENT_NAME);
        }

        if (curlResponseCode == CURLE_OK && captureBuffer.length() > 0)
        {
            try
            {
                std::string responseStr = std::string(captureBuffer.toByteArray(), captureBuffer.length());

                /*Expected response format
                {  "ext_expires_in": "3599", "expires_on": "1653408922", "access_token": "XYZ",
                    "expires_in": "3599", "not_before": "1653405022", "token_type": "Bearer",
                    "resource": "https://api.loganalytics.io"
                }*/

                Owned<IPropertyTree> tokenResponse = createPTreeFromJSONString(responseStr.c_str());
                
                PROGLOG("%s: Azure Log Analytics API access Bearer token generated! Expires in '%s' ", COMPONENT_NAME, tokenResponse->queryProp("expires_in"));
                token.set(tokenResponse->queryProp("access_token"));
            }
            catch(const std::exception& e)
            {
                throw makeStringExceptionV(-1, "%s Could not parse Azure Log Analytics API access token: %s", COMPONENT_NAME, e.what());
            }
        }
        else
        {
            if (curlResponseCode != CURLE_OK)
                throw makeStringExceptionV(-1, "%s token request error: libcurl error (%d): %s", COMPONENT_NAME, curlResponseCode, (curlErrBuffer[0] ? curlErrBuffer : "<unknown>"));
            else
                throw makeStringExceptionV(-1, "%s token request error: No content from Azure", COMPONENT_NAME);
        }

        curl_easy_cleanup(curlHandle);
    }
}

size_t stringCallback(char *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

static void submitKQLQuery(std::string & readBuffer, const char * token, const char * kql, const char * workspaceID)
{
    if (isEmptyString(token))
        throw makeStringExceptionV(-1, "%s KQL request: Empty LogAnalytics Workspace Token detected!", COMPONENT_NAME);

    if (isEmptyString(kql))
        throw makeStringExceptionV(-1, "%s KQL request: Empty KQL query detected!", COMPONENT_NAME);

    if (isEmptyString(workspaceID))
        throw makeStringExceptionV(-1, "%s KQL request: Empty WorkspaceID detected!", COMPONENT_NAME);

    CURL*   curlHandle = curl_easy_init();
    if (curlHandle)
    {
        CURLcode                curlResponseCode;
        struct curl_slist*      headers = nullptr;
        char                    curlErrBuffer[CURL_ERROR_SIZE];
        curlErrBuffer[0] = '\0';

        char * encodedKQL = curl_easy_escape(curlHandle, kql, strlen(kql));

        VStringBuffer tokenRequestURL("https://api.loganalytics.io/v1/workspaces/%s/query?query=%s", workspaceID, encodedKQL);
        VStringBuffer bearerHeader("Authorization: Bearer %s", token);

        try
        {
            /*curl -X GET 
            -H "Authorization: Bearer <TOKEN>"
             "https://api.loganalytics.io/v1/workspaces/<workspaceID>/query?query=ContainerLog20%7C%20limit%20100"
            */

            headers = curl_slist_append(headers, bearerHeader.str());
            curl_easy_setopt(curlHandle, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curlHandle, CURLOPT_URL, tokenRequestURL.str());
            curl_easy_setopt(curlHandle, CURLOPT_POST, 0);
            curl_easy_setopt(curlHandle, CURLOPT_HTTPGET, 1);
            curl_easy_setopt(curlHandle, CURLOPT_NOPROGRESS, 1);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, stringCallback);
            curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, &readBuffer);
            curl_easy_setopt(curlHandle, CURLOPT_USERAGENT, "HPCC Systems Log Access client");
            curl_easy_setopt(curlHandle, CURLOPT_ERRORBUFFER, curlErrBuffer);
            curl_easy_setopt(curlHandle, CURLOPT_FAILONERROR, 1L); // non HTTP Success treated as error
 
            curlResponseCode = curl_easy_perform(curlHandle);
        }
        catch (...)
        {
            throw makeStringExceptionV(-1, "%s KQL request: Unknown libcurl error", COMPONENT_NAME);
        }

        if (curlResponseCode != CURLE_OK)
            throw makeStringExceptionV(-1, "%s KQL request: Error (%d): %s", COMPONENT_NAME, curlResponseCode, (curlErrBuffer[0] ? curlErrBuffer : "<unknown>"));
        else if (readBuffer.length() <= 0)
            throw makeStringExceptionV(-1, "%s KQL request: Empty response!", COMPONENT_NAME);

        try
        {
            if (headers)
            {
                curl_slist_free_all(headers);
                headers = nullptr;
            }
            curl_easy_cleanup(curlHandle);
        }
        catch(...)
        {
            PROGLOG("%s KQL request: Issue during libcurl cleanup!", COMPONENT_NAME);
        }
    }
}

AzureLogAnalyticsCurlClient::AzureLogAnalyticsCurlClient(IPropertyTree & logAccessPluginConfig)
{
    PROGLOG("%s: Resolving all required configuration values...", COMPONENT_NAME);
    if (!getEnvVar("AZURE_TENANT_ID", m_tenantID))
    {
        WARNLOG("%s: Environment variable 'AZURE_TENANT_ID' not found!", COMPONENT_NAME);
        m_tenantID.set(logAccessPluginConfig.queryProp("connection/@tenantID"));

        if (m_tenantID.isEmpty())
           throw makeStringException(-1, "Could not determine Azure Tenant ID, set 'AZURE_TENANT_ID' env var, or connection/@tenantID in AzureClient LogAccess configuration!");
    }

    if (!getEnvVar("AZURE_CLIENT_ID", m_clientID))
    {
        WARNLOG("%s: Environment variable 'AZURE_CLIENT_ID' not found!", COMPONENT_NAME);
        m_clientID.set(logAccessPluginConfig.queryProp("connection/@clientID"));

        if (m_clientID.isEmpty())
           throw makeStringException(-1, "Could not find Azure AD client ID, set 'AZURE_CLIENT_ID' env var, or connection/@clientID in AzureClient LogAccess configuration - format is '00000000-0000-0000-0000-000000000000'!");
    }

    if (!getEnvVar("AZURE_CLIENT_SECRET", m_clientSecret))
    {
        WARNLOG("%s: Environment variable 'AZURE_CLIENT_SECRET' not found!", COMPONENT_NAME);
        m_clientSecret.set(logAccessPluginConfig.queryProp("connection/@clientSecret"));

        if (m_clientSecret.isEmpty())
           throw makeStringException(-1, "Could not determine Azure AD client secret, set 'AZURE_CLIENT_SECRET' env var, or connection/@clientSecret in AzureClient LogAccess configuration!");
    }

    if (!getEnvVar("AZURE_LOGANALYTICS_WORKSPACE_ID", m_logAnalyticsWorkspaceID))
    {
        WARNLOG("%s: Environment variable 'AZURE_LOGANALYTICS_WORKSPACE_ID' not found!", COMPONENT_NAME);
        m_logAnalyticsWorkspaceID.set(logAccessPluginConfig.queryProp("connection/@workspaceID"));

        if (m_logAnalyticsWorkspaceID.isEmpty())
           throw makeStringException(-1, "Could not determine Azure LogAnalytics workspace ID (aka workspace customer ID), set 'AZURE_LOGANALYTICS_WORKSPACE_ID' env var, or connection/@workspaceID in AzureClient LogAccess configuration!");
    }

    m_pluginCfg.set(&logAccessPluginConfig);

    m_globalIndexTimestampField.set(DEFAULT_TS_NAME);
    m_globalIndexSearchPattern.set(DEFAULT_INDEX_PATTERN);
    m_globalSearchColName.set(DEFAULT_HPCC_LOG_MESSAGE_COL);

    m_classSearchColName.set(DEFAULT_HPCC_LOG_TYPE_COL);
    m_workunitSearchColName.set(DEFAULT_HPCC_LOG_JOBID_COL);
    m_componentsSearchColName.set(DEFAULT_HPCC_LOG_COMPONENT_COL);
    m_audienceSearchColName.set(DEFAULT_HPCC_LOG_AUD_COL);

    Owned<IPropertyTreeIterator> logMapIter = m_pluginCfg->getElements("logMaps");
    ForEach(*logMapIter)
    {
        IPropertyTree & logMap = logMapIter->query();
        const char * logMapType = logMap.queryProp("@type");
        if (streq(logMapType, "global"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_globalIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_globalSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
            if (logMap.hasProp(LOGMAP_TIMESTAMPCOL_ATT))
                m_globalIndexTimestampField = logMap.queryProp(LOGMAP_TIMESTAMPCOL_ATT);
        }
        else if (streq(logMapType, "workunits"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_workunitIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_workunitSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "components"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_componentsIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_componentsSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "class"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_classIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_classSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "audience"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_audienceIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_audienceSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "instance"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_instanceIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_instanceSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "host"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_hostIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_hostSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else
        {
            ERRLOG("Encountered invalid LogAccess field map type: '%s'", logMapType);
        }
    }
}

void AzureLogAnalyticsCurlClient::getMinReturnColumns(StringBuffer & columns)
{
    //timestamp, source component, message
    columns.appendf("\n| project %s, %s, %s", m_globalIndexTimestampField.str(), m_componentsSearchColName.str(), m_globalSearchColName.str());
}

void AzureLogAnalyticsCurlClient::getDefaultReturnColumns(StringBuffer & columns)
{
    //timestamp, source component, all hpcc.log fields
    columns.appendf("\n| project %s, %s, %s, %s, %s, %s, %s, %s",
    m_globalIndexTimestampField.str(), m_componentsSearchColName.str(), m_globalSearchColName.str(), m_classSearchColName.str(),
    m_audienceSearchColName.str(), m_workunitSearchColName.str(), DEFAULT_HPCC_LOG_SEQ_COL, DEFAULT_HPCC_LOG_THREADID_COL);
}

void AzureLogAnalyticsCurlClient::getAllColumns(StringBuffer & columns)
{
    columns.append("");
}

bool generateHPCCLogColumnstAllColumns(StringBuffer & kql, const char * colName)
{
    if (isEmptyString(colName))
    {
        ERRLOG("%s Cannot generate HPCC Log Columns, empty source column detected", COMPONENT_NAME);
        return false;
    }

    kql.appendf("\n| extend hpcclogfields = extract_all(@\"([0-9A-Fa-f]+)\\s+(OPR|USR|PRG|AUD|UNK)\\s+(DIS|ERR|WRN|INF|PRO|MET|UNK)\\s+(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(UNK|[A-Z]\\d{8}-\\d{6}(?:-\\d+)?)\\s+(.*)\", %s)[0]", colName);
    kql.appendf("\n| extend %s = tostring(hpcclogfields.[0])", DEFAULT_HPCC_LOG_SEQ_COL);
    kql.appendf("\n| extend %s = tostring(hpcclogfields.[1])", DEFAULT_HPCC_LOG_AUD_COL);
    kql.appendf("\n| extend %s = tostring(hpcclogfields.[2])", DEFAULT_HPCC_LOG_TYPE_COL);
    kql.appendf("\n| extend %s = hpcclogfields.[3]", DEFAULT_HPCC_LOG_TIMESTAMP_COL);
//    kql.append("\n| extend hpcc_log_ts2 = make_datetime(");
//    kql.append("\n    toint(substring(hpcc_log_ts, 0, 4)),");
//    kql.append("\n    toint(substring(hpcc_log_ts, 5, 2)),");
//    kql.append("\n    toint(substring(hpcc_log_ts, 8, 2)),");
//    kql.append("\n    toint(substring(hpcc_log_ts, 11, 2)),");
//    kql.append("\n    toint(substring(hpcc_log_ts, 14, 2)),");
//    kql.append("\n    toint(substring(hpcc_log_ts, 17, 2)))");
    kql.appendf("\n| extend %s = toint(hpcclogfields.[4])", DEFAULT_HPCC_LOG_PROCID_COL);
    kql.appendf("\n| extend %s = toint(hpcclogfields.[5])",  DEFAULT_HPCC_LOG_THREADID_COL);
    kql.appendf("\n| extend %s = tostring(hpcclogfields.[6])", DEFAULT_HPCC_LOG_JOBID_COL);
    kql.appendf("\n| extend %s = tostring(hpcclogfields.[7])", DEFAULT_HPCC_LOG_MESSAGE_COL);

    return true;
}

void AzureLogAnalyticsCurlClient::searchMetaData(StringBuffer & search, const LogAccessReturnColsMode retcolmode, const  StringArray & selectcols, unsigned size, offset_t from)
{
    switch (retcolmode)
    {
    // in KQL, no project ALL supported
    //case RETURNCOLS_MODE_all:
    //    getAllColumns(search);
    //    break;
    case RETURNCOLS_MODE_min:
        getMinReturnColumns(search);
        break;
    case RETURNCOLS_MODE_default:
        getDefaultReturnColumns(search);
        break;
    case RETURNCOLS_MODE_custom:
    {
        if (selectcols.length() > 0)
        {
            search.append("\n| project ");
            ForEachItemIn(idx, selectcols)
            {
                search.append(selectcols.item(idx));
                if (idx < selectcols.length() -1)
                    search.append(",");
            }
        }
        else
        {
            throw makeStringExceptionV(-1, "%s: Custom return columns specified, but no columns provided", COMPONENT_NAME);
        }
        break;
    }
    default:
        throw makeStringExceptionV(-1, "%s: Could not determine return colums mode", COMPONENT_NAME);
    }

    /* KQL doesn't support offset, we could serialize, and assign row numbers but this is expensive
    | serialize
    | extend _row=row_number()
    | where _row > 20
    */
    search.appendf("\n| limit %s", std::to_string(size).c_str());
}

void azureLogAnalyticsTimestampQueryRangeString(StringBuffer & range, const char * timestampfield, std::time_t from, std::time_t to)
{
    if (isEmptyString(timestampfield))
        throw makeStringExceptionV(-1, "%s: TimeStamp Field must be provided", COMPONENT_NAME);

    //let startDateTime = datetime('2022-05-11T06:45:00.000Z');
    //let endDateTime = datetime('2022-05-11T13:00:00.000Z');
    //| where TimeGenerated >= startDateTime and TimeGenerated < endDateTime
    range.setf("%s >= unixtime_milliseconds_todatetime(%s)", timestampfield, std::to_string(from*1000).c_str());
    if (to != -1) //aka 'to' has been initialized
    {
        range.setf(" and %s < unixtime_milliseconds_todatetime(%s) \n", timestampfield, std::to_string(to*1000).c_str());
    }
}

void AzureLogAnalyticsCurlClient::populateKQLQueryString(StringBuffer & queryString, StringBuffer & queryIndex, const ILogAccessFilter * filter)
{
    if (filter == nullptr)
        throw makeStringExceptionV(-1, "%s: Null filter detected while creating Elastic Stack query string", COMPONENT_NAME);

    StringBuffer queryValue;
    std::string queryField = m_globalSearchColName.str();

    filter->toString(queryValue);
    switch (filter->filterType())
    {
    case LOGACCESS_FILTER_jobid:
    {
        if (m_workunitSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'JobID' log entry field not configured", COMPONENT_NAME);

        queryField = m_workunitSearchColName.str();

        if (!m_workunitIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_workunitIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_workunitIndexSearchPattern.str());
            queryIndex = m_workunitIndexSearchPattern;
        }

        DBGLOG("%s: Searching log entries by jobid: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_class:
    {
        if (m_classSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Class' log entry field not configured", COMPONENT_NAME);

        queryField = m_classSearchColName.str();

        if (!m_classIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_classIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_classIndexSearchPattern.str());
            queryIndex = m_classIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by class: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_audience:
    {
        if (m_audienceSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Audience' log entry field not configured", COMPONENT_NAME);
        
        queryField = m_audienceSearchColName.str();

        if (!m_audienceIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_audienceIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_audienceIndexSearchPattern.str());

            queryIndex = m_audienceIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by target audience: '%s'...", COMPONENT_NAME, queryValue.str());
        break;
    }
    case LOGACCESS_FILTER_component:
    {
        if (m_componentsSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Host' log entry field not configured", COMPONENT_NAME);

        queryField = m_componentsSearchColName.str();

        if (!m_componentsIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_componentsIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_componentsIndexSearchPattern.str());

            queryIndex = m_componentsIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching '%s' component log entries...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_host:
    {
        if (m_hostSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Host' log entry field not configured", COMPONENT_NAME);

        queryField = m_hostSearchColName.str();

        if (!m_hostIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_hostIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_hostIndexSearchPattern.str());

            queryIndex = m_hostIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by host: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_instance:
    {
        if (m_instanceSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Instance' log entry field not configured", COMPONENT_NAME);

        queryField = m_instanceSearchColName.str();

        if (!m_instanceIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.isEmpty() && strcmp(queryIndex.str(), m_instanceIndexSearchPattern.str()) != 0)
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.str(), m_instanceIndexSearchPattern.str());

            queryIndex = m_instanceIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by HPCC component instance: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_wildcard:
        throw makeStringExceptionV(-1, "%s: Wild Card filter detected within exact term filter!", COMPONENT_NAME);
    case LOGACCESS_FILTER_or:
    case LOGACCESS_FILTER_and:
    {
        StringBuffer op(logAccessFilterTypeToString(filter->filterType()));
        queryString.append(" ( ");
        populateKQLQueryString(queryString, queryIndex, filter->leftFilterClause());
        queryString.append(" ");
        queryString.append(op.toLowerCase()); //KQL or | and
        queryString.append(" ");
        populateKQLQueryString(queryString, queryIndex, filter->rightFilterClause());
        queryString.append(" ) ");
        return; // queryString populated, need to break out
    }
    case LOGACCESS_FILTER_column:
        if (filter->getFieldName() == nullptr)
            throw makeStringExceptionV(-1, "%s: empty field name detected in filter by column!", COMPONENT_NAME);
        queryField = filter->getFieldName();
        break;
    default:
        throw makeStringExceptionV(-1, "%s: Unknown query criteria type encountered: '%s'", COMPONENT_NAME, queryValue.str());
    }

    if (queryIndex.isEmpty())
        queryIndex = m_globalIndexSearchPattern.str();

    //KQL structure:
    //TableName
    //| where Fieldname =~ 'value'
    //queryString.append("\n| where ").append(queryField.c_str()).append(" =~ '").append(queryValue.str()).append("'");
    queryString.append(" ").append(queryField.c_str()).append(" =~ '").append(queryValue.str()).append("'");
}

void AzureLogAnalyticsCurlClient::populateKQLQueryString(StringBuffer & queryString, StringBuffer & queryIndex, const LogAccessConditions & options)
{
    try
    {
        const LogAccessTimeRange & trange = options.getTimeRange();
        if (trange.getStartt().isNull())
            throw makeStringExceptionV(-1, "%s: start time must be provided!", COMPONENT_NAME);

        //Forced to format log structure in query until a proper log ingest rule is created
        queryIndex.set(m_globalIndexSearchPattern.str());
        queryString.append(queryIndex);
        generateHPCCLogColumnstAllColumns(queryString, m_globalSearchColName.str()); 

        if (options.queryFilter()->filterType() == LOGACCESS_FILTER_wildcard) // No filter
        {
            //no where clause
            queryIndex.set(m_globalIndexSearchPattern.str());
        }
        else
        {
            queryString.append("\n| where ");
            populateKQLQueryString(queryString, queryIndex, options.queryFilter());
        }

        StringBuffer range;
        azureLogAnalyticsTimestampQueryRangeString(range, m_globalIndexTimestampField.str(), trange.getStartt().getSimple(),trange.getEndt().isNull() ? -1 : trange.getEndt().getSimple());
        queryString.append("\n| where ").append(range.str());

        searchMetaData(queryString, options.getReturnColsMode(), options.getLogFieldNames(), options.getLimit(), options.getStartFrom());

        DBGLOG("%s: Search string '%s'", COMPONENT_NAME, queryString.str());
    }
    catch (std::runtime_error &e)
    {
        throw makeStringExceptionV(-1, "%s: Error populating KQL search string: %s", COMPONENT_NAME, e.what());
    }
    catch (IException * e)
    {
        StringBuffer mess;
        e->errorMessage(mess);
        e->Release();
        throw makeStringExceptionV(-1, "%s: Error populating KQL search string: %s", COMPONENT_NAME, mess.str());
    }
}

unsigned processHitsJsonResp(IPropertyTreeIterator * lines, IPropertyTreeIterator * columns, StringBuffer & returnbuf, LogAccessLogFormat format, bool wrapped, bool reportHeader)
{
    if (!lines)
        throw makeStringExceptionV(-1, "%s: Detected null 'rows' Azure Log Analytics KQL response", COMPONENT_NAME);

    StringArray header;
    ForEach(*columns)
    {
        Owned<IPropertyTreeIterator> names = columns->query().getElements("name");
        ForEach(*names)
        {
            header.append(names->query().queryProp("."));
        }
    }

    unsigned recsProcessed = 0;
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            if (wrapped)
                returnbuf.append("<lines>");

            ForEach(*lines)
            {
                returnbuf.append("<line>");
                Owned<IPropertyTreeIterator> fields = lines->query().getElements("rows");
                int idx = 0;
                ForEach(*fields)
                {
                    const char * fieldName = header.item(idx++);
                    returnbuf.appendf("<%s>%s</%s>", fieldName, fields->query().queryProp("."), fieldName);
                }
                returnbuf.append("</line>");
                recsProcessed++;
            }
            if (wrapped)
                returnbuf.append("</lines>");
            break;
        }
        case LOGACCESS_LOGFORMAT_json:
        {
            if (wrapped)
                returnbuf.append("{\"lines\": [");

            StringBuffer hitchildjson;
            bool firstLine = true;
            ForEach(*lines)
            {
                if (!firstLine)
                    returnbuf.append(",\n");

                Owned<IPropertyTreeIterator> fields = lines->query().getElements("rows");
                bool firstField = true;
                int idx = 0;
                ForEach(*fields)
                {
                    if (!firstField)
                        hitchildjson.append(", ");

                    hitchildjson.appendf("{\"%s\":\"%s\"}", header.item(idx++), fields->query().queryProp("."));

                    firstField = false;
                }

                firstLine = false;
                returnbuf.appendf("{\"fields\": [ %s ]}", hitchildjson.str());
                hitchildjson.clear();
                recsProcessed++;
            }
            if (wrapped)
                returnbuf.append("]}");
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
        {
            if (reportHeader)
            {
                bool first = true;
                ForEachItemIn(idx, header)
                {
                    if (!first)
                        returnbuf.append(", ");
                    else
                        first = false;

                    returnbuf.append(header.item(idx));
                }
                returnbuf.newline();
            }

            ForEach(*lines)
            {
                Owned<IPropertyTreeIterator> fieldElementsItr = lines->query().getElements("*");

                bool first = true;
                ForEach(*fieldElementsItr)
                {
                    if (!first)
                        returnbuf.append(", ");
                    else
                        first = false;

                    fieldElementsItr->query().getProp(nullptr, returnbuf); // commas in data should be escaped
                    recsProcessed++;
                }
                returnbuf.newline();
            }
            break;
        }
        default:
            break;
    }
    return recsProcessed;
}

bool processSearchJsonResp(LogQueryResultDetails & resultDetails, const std::string & retrievedDocument, StringBuffer & returnbuf, LogAccessLogFormat format, bool reportHeader)
{
    Owned<IPropertyTree> tree = createPTreeFromJSONString(retrievedDocument.c_str());
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse query response", COMPONENT_NAME);

    resultDetails.totalReceived = processHitsJsonResp(tree->getElements("tables/rows"), tree->getElements("tables/columns"), returnbuf, format, true, reportHeader);
    resultDetails.totalAvailable = 0;
    return true;
}

bool AzureLogAnalyticsCurlClient::fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    StringBuffer token;
    requestLogAnalyticsAccessToken(token, m_clientID, m_clientSecret, m_tenantID); //throws if issues encountered

    if (token.isEmpty())
        throw makeStringExceptionV(-1, "%s Could not fetch valid Azure Log Analytics access token!", COMPONENT_NAME);

    StringBuffer queryString, queryIndex;
    populateKQLQueryString(queryString, queryIndex, options);

    std::string readBuffer;
    submitKQLQuery(readBuffer, token.str(), queryString.str(), m_logAnalyticsWorkspaceID.str());

    return processSearchJsonResp(resultDetails, readBuffer, returnbuf, format, true);
}

class AZURE_LOGANALYTICS_CURL_LOGACCESS_API AzureLogAnalyticsStream : public CInterfaceOf<IRemoteLogAccessStream>
{
public:
    virtual bool readLogEntries(StringBuffer & record, unsigned & recsRead) override
    {
        LogQueryResultDetails  resultDetails;
        m_remoteLogAccessor->fetchLog(resultDetails, m_options, record, m_outputFormat);
        recsRead = resultDetails.totalReceived;

        return false;
    }

    AzureLogAnalyticsStream(IRemoteLogAccess * azureQueryClient, const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
    {
        m_remoteLogAccessor.set(azureQueryClient);
        m_outputFormat = format;
        m_pageSize = pageSize;
        m_options = options;
    }

    virtual ~AzureLogAnalyticsStream() override = default;

private:
    unsigned int m_pageSize;
    bool m_hasBeenScrolled = false;
    LogAccessLogFormat m_outputFormat;
    LogAccessConditions m_options;
    Owned<IRemoteLogAccess> m_remoteLogAccessor;
};

IRemoteLogAccessStream * AzureLogAnalyticsCurlClient::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format)
{
    return getLogReader(options, format, DEFAULT_MAX_RECORDS_PER_FETCH);
}

IRemoteLogAccessStream * AzureLogAnalyticsCurlClient::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
{
    return new AzureLogAnalyticsStream(this, options, format, pageSize);
}

extern "C" IRemoteLogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
    return new AzureLogAnalyticsCurlClient(logAccessPluginConfig);
}
