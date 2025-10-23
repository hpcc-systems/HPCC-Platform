/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

#include "ElasticStackLogAccess.hpp"

#include "platform.h"

#include <string>
#include <vector>
#include <iostream>
#include <json/json.h>
#include <json/writer.h>
#include "jsecrets.hpp"


#ifdef _CONTAINERIZED
//In containerized world, most likely Elastic Search host is their default k8s hostname
static constexpr const char * DEFAULT_ES_HOST = "elasticsearch-master";
#else
//In baremetal, localhost is good guess as any
static constexpr const char * DEFAULT_ES_HOST = "localhost";
#endif

static constexpr const char * DEFAULT_ES_PROTOCOL = "http";
static constexpr const char * DEFAULT_ES_DOC_TYPE = "_doc";
static constexpr const char * DEFAULT_ES_PORT = "9200";

static constexpr int DEFAULT_ES_DOC_LIMIT = 100;
static constexpr int DEFAULT_ES_DOC_START = 0;

static constexpr const char * DEFAULT_TS_NAME = "@timestamp"; //as of hpccpipeline 10206 contents of hpcc.log.timestamp are super imposed onto @timestamp
static constexpr const char * DEFAULT_INDEX_PATTERN = "hpcc-logs*";

static constexpr const char * DEFAULT_HPCC_LOG_SEQ_COL         = "hpcc.log.sequence";
static constexpr const char * DEFAULT_HPCC_LOG_TIMESTAMP_COL   = "hpcc.log.timestamp"; //as of hpccpipeline 10206 contents of hpcc.log.timestamp are super imposed onto @timestamp
static constexpr const char * DEFAULT_HPCC_LOG_PROCID_COL      = "hpcc.log.procid";
static constexpr const char * DEFAULT_HPCC_LOG_THREADID_COL    = "hpcc.log.threadid";
static constexpr const char * DEFAULT_HPCC_LOG_MESSAGE_COL     = "hpcc.log.message";
static constexpr const char * DEFAULT_HPCC_LOG_JOBID_COL       = "hpcc.log.jobid";
static constexpr const char * DEFAULT_HPCC_LOG_COMPONENT_COL   = "kubernetes.container.name";
static constexpr const char * DEFAULT_HPCC_LOG_TYPE_COL        = "hpcc.log.class";
static constexpr const char * DEFAULT_HPCC_LOG_AUD_COL         = "hpcc.log.audience";
static constexpr const char * DEFAULT_HPCC_LOG_POD_COL         = "kubernetes.pod.name";
static constexpr const char * DEFAULT_HPCC_LOG_TRACE_COL       = "hpcc.log.traceid";
static constexpr const char * DEFAULT_HPCC_LOG_SPAN_COL        = "hpcc.log.spanid";


static constexpr const char * LOGMAP_INDEXPATTERN_ATT = "@storeName";
static constexpr const char * LOGMAP_SEARCHCOL_ATT = "@searchColumn";
static constexpr const char * LOGMAP_TIMESTAMPCOL_ATT = "@timeStampColumn";

static constexpr const char * DEFAULT_SCROLL_TIMEOUT = "1m"; //Elastic Time Units (i.e. 1m = 1 minute).
static constexpr std::size_t  DEFAULT_MAX_RECORDS_PER_FETCH = 100;
static constexpr int DEFAULT_CONN_TIMEOUT_MS = 2000;
static constexpr int DEFAULT_HTTP_REQ_TIMEOUT_MS = 3000;

static constexpr unsigned int HTTP_SUCCESS_CLASS = 2;
static constexpr size_t MAX_CURL_REPLY_BUFFER_SIZE = 4194304; // 2^22
static constexpr size_t DEFAULT_CURL_REPLY_BUFFER_SIZE = 32768; // 2^15

void ElasticStackLogAccess::getMinReturnColumns(std::string & columns)
{
    //timestamp, source component, message
    columns.append(" \"").append(m_globalIndexTimestampField).append("\", \"").append(m_componentsSearchColName.str()).append("\", \"").append(m_globalSearchColName).append("\" ");
}

void ElasticStackLogAccess::getDefaultReturnColumns(std::string & columns)
{
    //timestamp, source component, all hpcc.log fields
    columns.append(" \"").append(m_globalIndexTimestampField).append("\", \"").append(m_componentsSearchColName.str()).append("\", \"").append(m_podSearchColName.str()).append("\", \"hpcc.log.*\" ");
}

void ElasticStackLogAccess::getAllColumns(std::string & columns)
{
    columns.append( " \"*\" ");
}

ElasticStackLogAccess::ElasticStackLogAccess(const std::vector<std::string> &hostUrlList, IPropertyTree & logAccessPluginConfig) : m_esClient(hostUrlList)
{
    if (!hostUrlList.at(0).empty())
        m_esConnectionStr.set(hostUrlList.at(0).c_str());

    m_pluginCfg.set(&logAccessPluginConfig);

    //Client::TimeoutOption - HTTP request timeout in ms.
    m_reqTimeOutMs = m_pluginCfg->getPropInt("connection/@httpReqTimeoutMs", DEFAULT_HTTP_REQ_TIMEOUT_MS);
    if (m_reqTimeOutMs >= 0)
        m_esClient.setClientOption(elasticlient::Client::TimeoutOption{m_reqTimeOutMs});

    //Client::ConnectTimeoutOption - Connect timeout in ms.
    m_connTimeOutMs = m_pluginCfg->getPropInt("connection/@connTimeoutMs", DEFAULT_CONN_TIMEOUT_MS);
    if (m_connTimeOutMs >= 0)
        m_esClient.setClientOption(elasticlient::Client::ConnectTimeoutOption{m_connTimeOutMs});

    if (m_pluginCfg->hasProp("connection/ssl"))
    {
        //Client::SSLOption::CertFile - path to the SSL certificate file.
        m_sCertFile.set(m_pluginCfg->queryProp("connection/ssl/@certFilePath"));

        //Client::SSLOption::KeyFile - path to the SSL certificate key file.
        m_skeyFile.set(m_pluginCfg->queryProp("connection/ssl/@keyFilePath"));

        //Client::SSLOption::CaInfo - path to the CA bundle if custom CA is used.
        m_caInfo.set(m_pluginCfg->queryProp("connection/ssl/@caInfoPath"));

        //Client::SSLOption::VerifyHost - verify the certificate's name against host.
        m_bVerifyHost = m_pluginCfg->getPropBool("connection/ssl/@verifyHost", true);

        //Client::SSLOption::VerifyPeer - verify the peer's SSL certificate.
        m_bVerifyPeer = m_pluginCfg->getPropBool("connection/ssl/@verifyPeer", true);

        if (!isEmptyString(m_caInfo) && !isEmptyString(m_sCertFile) && !isEmptyString(m_skeyFile))
        {
            m_esClient.setClientOption(elasticlient::Client::SSLOption{
                elasticlient::Client::SSLOption::VerifyHost{m_bVerifyHost},
                elasticlient::Client::SSLOption::VerifyPeer{m_bVerifyPeer},
                elasticlient::Client::SSLOption::CaInfo{m_caInfo.str()},
                elasticlient::Client::SSLOption::CertFile{m_sCertFile.str()},
                elasticlient::Client::SSLOption::KeyFile{m_skeyFile.str()}
            });
        }
        else if (!isEmptyString(m_caInfo) && !isEmptyString(m_sCertFile))
        {
            m_esClient.setClientOption(elasticlient::Client::SSLOption{
                elasticlient::Client::SSLOption::VerifyHost{m_bVerifyHost},
                elasticlient::Client::SSLOption::VerifyPeer{m_bVerifyPeer},
                elasticlient::Client::SSLOption::CaInfo{m_caInfo.str()},
                elasticlient::Client::SSLOption::CertFile{m_sCertFile.str()}
            });
        }
        else if (!isEmptyString(m_caInfo))
        {
            m_esClient.setClientOption(elasticlient::Client::SSLOption{
                elasticlient::Client::SSLOption::VerifyHost{m_bVerifyHost},
                elasticlient::Client::SSLOption::VerifyPeer{m_bVerifyPeer},
                elasticlient::Client::SSLOption::CaInfo{m_caInfo.str()}
            });
        }
        else
        {
            m_esClient.setClientOption(elasticlient::Client::SSLOption{
                elasticlient::Client::SSLOption::VerifyHost{m_bVerifyHost},
                elasticlient::Client::SSLOption::VerifyPeer{m_bVerifyPeer}
            });
        }
    }

    m_globalIndexTimestampField.set(DEFAULT_TS_NAME);
    m_globalIndexSearchPattern.set(DEFAULT_INDEX_PATTERN);
    m_globalSearchColName.set(DEFAULT_HPCC_LOG_MESSAGE_COL);

    m_classSearchColName.set(DEFAULT_HPCC_LOG_TYPE_COL);
    m_workunitSearchColName.set(DEFAULT_HPCC_LOG_JOBID_COL);
    m_componentsSearchColName.set(DEFAULT_HPCC_LOG_COMPONENT_COL);
    m_audienceSearchColName.set(DEFAULT_HPCC_LOG_AUD_COL);
    m_podSearchColName.set(DEFAULT_HPCC_LOG_POD_COL);
    m_traceSearchColName.set(DEFAULT_HPCC_LOG_TRACE_COL);
    m_spanSearchColName.set(DEFAULT_HPCC_LOG_SPAN_COL);

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
        else if (streq(logMapType, "pod"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_podIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_podSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "instance"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_instanceIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_instanceSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "node"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_hostIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_hostSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "host"))
        {
            OWARNLOG("%s: 'host' LogMap entry is deprecated and replaced by 'node'!", COMPONENT_NAME);
            if (isEmptyString(m_hostIndexSearchPattern) && isEmptyString(m_hostSearchColName))
            {
                if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                    m_hostIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);
                if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                    m_hostSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
            }
            else
                OERRLOG("%s: Possible LogMap collision detected, 'host' and 'node' refer to same log column!", COMPONENT_NAME); 
        }
        else if (streq(logMapType, "traceid"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_traceIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);

            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_traceSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else if (streq(logMapType, "spanid"))
        {
            if (logMap.hasProp(LOGMAP_INDEXPATTERN_ATT))
                m_spanIndexSearchPattern = logMap.queryProp(LOGMAP_INDEXPATTERN_ATT);

            if (logMap.hasProp(LOGMAP_SEARCHCOL_ATT))
                m_spanSearchColName = logMap.queryProp(LOGMAP_SEARCHCOL_ATT);
        }
        else
        {
            ERRLOG("Encountered invalid LogAccess field map type: '%s'", logMapType);
        }
    }

#ifdef LOGACCESSDEBUG
    StringBuffer out;
    const IPropertyTree * status = getESStatus();
    toXML(status, out);
    fprintf(stdout, "ES Status: %s", out.str());

    const IPropertyTree * is =  getIndexSearchStatus(m_globalIndexSearchPattern);
    toXML(is, out);
    fprintf(stdout, "ES available indexes: %s", out.str());

    const IPropertyTree * ts = getTimestampTypeFormat(m_globalIndexSearchPattern, m_globalIndexTimestampField);
    toXML(ts, out);
    fprintf(stdout, "ES %s timestamp info: '%s'", m_globalIndexSearchPattern.str(), out.str());
#endif

}

static size_t captureIncomingCURLReply(void* contents, size_t size, size_t nmemb, void* userp)
{
    size_t          incomingDataSize = size * nmemb;
    MemoryBuffer*   mem = static_cast<MemoryBuffer*>(userp);

    if ((mem->length() + incomingDataSize) < MAX_CURL_REPLY_BUFFER_SIZE)
    {
        mem->append(incomingDataSize, contents);
    }
    else
    {
        // Signals an error to libcurl
        incomingDataSize = 0;
        WARNLOG("%s::captureIncomingCURLReply exceeded buffer size %zu", COMPONENT_NAME, MAX_CURL_REPLY_BUFFER_SIZE);
    }

    return incomingDataSize;
}

static void curlPingURL(StringBuffer & resp, const char * targetURL, const char * certFile, const char * caCertFile, bool verifyHost, bool verifyPeer)
{
    if (isEmptyString(targetURL))
        throw makeStringExceptionV(-1, "%s Curl ping: targetURL required!", COMPONENT_NAME);

    OwnedPtrCustomFree<CURL, curl_easy_cleanup> curlHandle = curl_easy_init();
    if (curlHandle)
    {
        CURLcode                curlResponseCode;
        MemoryBuffer            captureBuffer(DEFAULT_CURL_REPLY_BUFFER_SIZE);

        if (curl_easy_setopt(curlHandle, CURLOPT_URL, targetURL) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_URL' (%s)!", COMPONENT_NAME,  targetURL);

        if (curl_easy_setopt(curlHandle, CURLOPT_NOPROGRESS, 1) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not disable 'CURLOPT_NOPROGRESS' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_WRITEFUNCTION, captureIncomingCURLReply) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_WRITEFUNCTION' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_WRITEDATA, static_cast<void*>(&captureBuffer)) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_WRITEDATA' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_USERAGENT, "HPCC Systems Log Access client") != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_USERAGENT' option!", COMPONENT_NAME);

        if (curl_easy_setopt(curlHandle, CURLOPT_FAILONERROR, 0L) != CURLE_OK) // Do not treat non-ok HTTP codes as an error
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_FAILONERROR' option!", COMPONENT_NAME);

        if (!isEmptyString(certFile))
        {
            /* set the cert for client authentication */
            if (curl_easy_setopt(curlHandle, CURLOPT_SSLCERT, certFile) != CURLE_OK)
                throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_SSLCERT' option!", COMPONENT_NAME);
        }

        if (!isEmptyString(caCertFile))
        {
            /* set the file with the certs validating the server */
            if (curl_easy_setopt(curlHandle, CURLOPT_CAINFO, caCertFile) != CURLE_OK)
                throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_CAINFO' option!", COMPONENT_NAME);
        }

        if (curl_easy_setopt(curlHandle, CURLOPT_SSL_VERIFYPEER, verifyPeer ? 1L : 0L) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_SSL_VERIFYPEER' option!", COMPONENT_NAME);

        //CURLOPT_SSL_VERIFYHOST should use 2L for verification enabled (per libcurl documentation: 0L disables verification, 1L is not a valid value, and 2L enables full verification of the hostname.)
        if (curl_easy_setopt(curlHandle, CURLOPT_SSL_VERIFYHOST, verifyHost ? 2L : 0L) != CURLE_OK)
            throw makeStringExceptionV(-1, "%s: Curl Ping: Could not set 'CURLOPT_SSL_VERIFYHOST' option!", COMPONENT_NAME);

        try
        {
            curlResponseCode = curl_easy_perform(curlHandle);
        }
        catch (...)
        {
            throw makeStringExceptionV(-1, "%s: Curl Ping: Unknown error!", COMPONENT_NAME);
        }

        if (captureBuffer.length() > 0)
        {
            resp.append(captureBuffer.length(), (const char *)captureBuffer.toByteArray());
        }

        if (curlResponseCode == CURLE_OK) // this is not the same as http success
        {
            long httpResponseCode;
            curl_easy_getinfo(curlHandle, CURLINFO_RESPONSE_CODE, &httpResponseCode);

            if (httpResponseCode / 100 == HTTP_SUCCESS_CLASS) // HTTP OK
            {
                resp.set("Success");
            }
            else
            {
                if (resp.isEmpty())
                    resp.set("Unknown error");

                resp.appendf(" (HTTP %ld)", httpResponseCode);
            }
        }
        else
        {
            throw makeStringExceptionV(-1, "%s: Curl Ping: CURL ACTION FAILED! '%s'", COMPONENT_NAME, curl_easy_strerror(curlResponseCode));
        }
    }
}

const IPropertyTree * ElasticStackLogAccess::performAndLogESRequest(Client::HTTPMethod httpmethod, const char * url, const char * reqbody, const char * logmessageprefix, LogMsgCategory reqloglevel = MCdebugProgress, LogMsgCategory resploglevel = MCdebugProgress)
{
    try
    {
        LOG(reqloglevel,"ESLogAccess: Requesting '%s'... ", logmessageprefix );
        cpr::Response esREsponse = m_esClient.performRequest(httpmethod,url,reqbody);
        if (esREsponse.status_code / 100 != HTTP_SUCCESS_CLASS)
        {
            WARNLOG("ESLogAccess request failed: HTTP code: (%ld), reason: '%s', status: '%s', raw header: '%s', response: '%s'", esREsponse.status_code, esREsponse.reason.c_str(), esREsponse.status_line.c_str(), esREsponse.raw_header.c_str(), esREsponse.text.c_str());
        }
        else
        {
            LOG(resploglevel,"ESLogAccess: '%s' HTTP code: (%ld), status: '%s', responseText: '%s'", logmessageprefix, esREsponse.status_code, esREsponse.status_line.c_str(), esREsponse.text.c_str());
        }
        Owned<IPropertyTree> response = createPTreeFromJSONString(esREsponse.text.c_str());
        return response.getClear();
    }
    catch (ConnectionException & ce)//std::runtime_error
    {
        LOG(MCuserError, "ESLogAccess: Encountered error requesting '%s': '%s'", logmessageprefix, ce.what());
    }
    catch (...)
    {
        LOG(MCuserError, "ESLogAccess: Encountered error requesting '%s'", logmessageprefix);
    }
    return nullptr;

}

const IPropertyTree * ElasticStackLogAccess::getTimestampTypeFormat(const char * indexpattern, const char * fieldname)
{
    if (isEmptyString(indexpattern))
        throw makeStringException(-1, "ElasticStackLogAccess::getTimestampTypeFormat: indexpattern must be provided");

    if (isEmptyString(fieldname))
        throw makeStringException(-1, "ElasticStackLogAccess::getTimestampTypeFormat: fieldname must be provided");

    VStringBuffer timestampformatreq("%s/_mapping/field/created_ts?include_type_name=true&format=JSON", indexpattern);
    return performAndLogESRequest(Client::HTTPMethod::GET, timestampformatreq.str(), "", "getTimestampTypeFormat");
}

const IPropertyTree * ElasticStackLogAccess::getIndexSearchStatus(const char * indexpattern)
{
    if (!indexpattern || !*indexpattern)
        throw makeStringException(-1, "ElasticStackLogAccess::getIndexSearchStatus: indexpattern must be provided");

    VStringBuffer indexsearch("_cat/indices/%s?format=JSON", indexpattern);
    return performAndLogESRequest(Client::HTTPMethod::GET, indexsearch.str(), "", "List of available indexes");
}

const IPropertyTree * ElasticStackLogAccess::getESStatus()
{
    return performAndLogESRequest(Client::HTTPMethod::GET, "_cluster/health", "", "Target cluster health");
}

 void ElasticStackLogAccess::healthReport(LogAccessHealthReportOptions options, LogAccessHealthReportDetails & report)
 {
    LogAccessHealthStatus status = LOGACCESS_STATUS_success;
    try
    {
        {
            StringBuffer configuration;
            if (m_pluginCfg)
            {
                if (options.IncludeConfiguration)
                    toJSON(m_pluginCfg, configuration, 0, JSON_Format);
            }
            else
            {
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
                status.appendMessage("Elastic Plug-in Configuration tree is empty!!!");
            }

            report.Configuration.set(configuration.str()); //will be empty if !options.IncludeConfiguration
        }

        if (m_esConnectionStr.length() == 0)
        {
            status.appendMessage("Connection String to target Elastic instance is empty!");
            status.escalateStatusCode(LOGACCESS_STATUS_fail);
        }

        {
            StringBuffer debugReport;
            debugReport.set("{ \"ConnectionInfo\": {");
            appendJSONStringValue(debugReport, "ConnectionString", m_esConnectionStr.str(), true);
            appendJSONValue(debugReport, "httpReqTimeoutMs", m_reqTimeOutMs);
            appendJSONValue(debugReport, "connTimeoutMs", m_connTimeOutMs);
            debugReport.appendf(", \"credentials\": {");
            if (m_pluginCfg)
            {
                StringBuffer credentialsSecretKey;
                m_pluginCfg->getProp("credentials/@secretName", credentialsSecretKey);
                appendJSONStringValue(debugReport, "secretName", credentialsSecretKey.str(), true);

                StringBuffer credentialsVaultId;
                m_pluginCfg->getProp("credentials/@vaultId", credentialsVaultId);
                appendJSONStringValue(debugReport, "vaultId", credentialsVaultId.str(), true);
            }
            debugReport.append(" }"); //close credentials
            debugReport.appendf(", \"ssl\": {");
            appendJSONStringValue(debugReport, "CertFile", m_sCertFile.str(), true);
            appendJSONStringValue(debugReport, "KeyFile", m_skeyFile.str(), true);
            appendJSONStringValue(debugReport, "CaInfo", m_caInfo.str(), true);
            appendJSONValue(debugReport, "VerifyHost", m_bVerifyHost);
            appendJSONValue(debugReport, "VerifyPeer", m_bVerifyPeer);
            debugReport.append(" }"); //close ssl
            debugReport.append( "}"); //close conninfo

            debugReport.appendf(", \"LogMaps\": {");
            debugReport.appendf("\"Global\": { ");
            appendJSONStringValue(debugReport, "ColName", m_globalSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_globalIndexSearchPattern.str(), true);
            appendJSONStringValue(debugReport, "TimeStampCol", m_globalIndexTimestampField.str(), true);
            debugReport.append(" }"); // end Global
            debugReport.appendf(", \"Components\": { ");
            appendJSONStringValue(debugReport, "ColName", m_componentsSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_componentsIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end Components
            debugReport.appendf(", \"Workunits\": { ");
            appendJSONStringValue(debugReport, "ColName", m_workunitSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_workunitIndexSearchPattern.str(), true);
            debugReport.append(" }"); // end Workunits
            debugReport.appendf(", \"Audience\": { ");
            appendJSONStringValue(debugReport, "ColName", m_audienceSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_audienceIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end Audience
            debugReport.appendf(", \"Class\": { ");
            appendJSONStringValue(debugReport, "ColName", m_classSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_classIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end Class
            debugReport.appendf(", \"Instance\": { ");
            appendJSONStringValue(debugReport, "ColName", m_instanceSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_instanceIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end Instance
            debugReport.appendf(", \"Pod\": { ");
            appendJSONStringValue(debugReport, "ColName", m_podSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_podIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end Pod
            debugReport.appendf(", \"TraceID\": { ");
            appendJSONStringValue(debugReport, "ColName", m_traceSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_traceIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end TraceID
            debugReport.appendf(", \"SpanID\": { ");
            appendJSONStringValue(debugReport, "ColName", m_spanSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_spanIndexSearchPattern.str(), true);
            debugReport.appendf(" }"); // end SpanID
            debugReport.appendf(", \"Host\": { ");
            appendJSONStringValue(debugReport, "ColName", m_hostSearchColName.str(), true);
            appendJSONStringValue(debugReport, "Source", m_hostIndexSearchPattern.str(), true);
            debugReport.append(" }"); // end Host
            debugReport.append(" }"); //close logmaps

            debugReport.append(" }"); //close debugreport

            if (options.IncludeDebugReport)
                report.DebugReport.PluginDebugReport.set(debugReport);

            debugReport.append("{ \"CurlPing\": ");
            try
            {
                StringBuffer resp;
                curlPingURL(resp, m_esConnectionStr.str(), m_sCertFile.str(), m_caInfo.str(), m_bVerifyHost, m_bVerifyPeer);
                debugReport.appendf("\"%s\"", resp.str());
            }
            catch(IException * e)
            {
                VStringBuffer description("Exception pinging ES server (%d) - ", e->errorCode());
                e->errorMessage(description);
                debugReport.appendf("\"%s\"", description.str());
                status.appendMessage(description.str());
                e->Release();
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }
            catch(...)
            {
                debugReport.append("\"Unknown exception while pinging ES server\"");
                status.appendMessage("Unknown exception while pinging ES server");
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }
            debugReport.append(" } "); //close CurlPing

            debugReport.set("{ \"AvailableIndices\": "); //start server debugreport and availableindices
            try
            {
                const IPropertyTree * is =  getIndexSearchStatus(m_globalIndexSearchPattern);
                if (is)
                {
                    toJSON(is, debugReport); //returns json array 
                }
                else
                {
                    debugReport.append("\"Unable to fetch Available Indices for Global Index Pattern\"");
                    status.appendMessage("Could not populate Available Indices for Global Index Pattern");
                    status.escalateStatusCode(LOGACCESS_STATUS_warning);
                }
            }
            catch(IException * e)
            {
                VStringBuffer description("Exception fetching available ES indices (");
                description.append(e->errorCode()).append(") - ");
                e->errorMessage(description);
                e->Release();
                debugReport.append("\"Unable to fetch Available Indices\"");
                status.appendMessage(description.str());
                status.escalateStatusCode(LOGACCESS_STATUS_warning);
            }
            catch(...)
            {
                debugReport.append("\"Unable to fetch Available Indices\"");
                status.appendMessage("Unknown exception while fetching available ES indices");
                status.escalateStatusCode(LOGACCESS_STATUS_warning);
            }

            debugReport.append(", \"TimestampField\": ");
            try
            {
                const IPropertyTree * ts = getTimestampTypeFormat(m_globalIndexSearchPattern, m_globalIndexTimestampField);
                if (ts)
                {
                    toJSON(ts, debugReport);
                }
                else
                {
                    debugReport.appendf("\"Could not populate ES timestamp format for IndexPattern %s\"", m_globalIndexSearchPattern.str());
                    status.appendMessage("Could not populate Available Indices for Global Index Pattern");
                    status.escalateStatusCode(LOGACCESS_STATUS_warning);
                }
            }
            catch(IException * e)
            {
                VStringBuffer description("\"Exception fetching target ES timestamp format (%d) - ", e->errorCode());
                e->errorMessage(description);
                debugReport.appendf("\"%s\"", description.str());
                status.appendMessage(description.str());
                e->Release();
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }
            catch(...)
            {
                debugReport.append("\"Unknown exception while fetching target ES timestamp format\"");
                status.appendMessage("Unknown exception while fetching target ES timestamp format");
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }

            debugReport.append(", \"ESStatus\": ");
            try
            {
                StringBuffer out;
                const IPropertyTree * esStatus = getESStatus();
                if (esStatus)
                {
                    toJSON(esStatus, debugReport); //extract esstatus info to set status green/yellow/red?
                }
                else
                {
                    status.appendMessage("Could not populate ES Status");
                    debugReport.append("\"Could not populate ES Status\"");
                    status.escalateStatusCode(LOGACCESS_STATUS_warning);
                }
            }
            catch(IException * e)
            {
                StringBuffer description;
                e->errorMessage(description);
                debugReport.appendf("\"Exception fetching ES Status (%d) - %s\"", e->errorCode(), description.str());
                e->Release();
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
                status.appendMessage(description.str());
            }
            catch(...)
            {
                status.appendMessage("Unknown exception while fetching ES Status");
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }

            debugReport.append(" } "); //close Server

            if (options.IncludeDebugReport)
                report.DebugReport.ServerDebugReport.set(debugReport);
        }

        {
            StringBuffer sampleQuery;
            sampleQuery.append("{ \"Query\": { ");
            try
            {
                appendJSONStringValue(sampleQuery, "LogFormat", "JSON", false);
                LogAccessLogFormat outputFormat = LOGACCESS_LOGFORMAT_json;
                LogAccessConditions queryOptions;
                sampleQuery.appendf(", \"Filter\": { ");
                appendJSONStringValue(sampleQuery, "type", "byComponent", false);
                appendJSONStringValue(sampleQuery, "value", "eclwatch", false);
                queryOptions.setFilter(getComponentLogAccessFilter("eclwatch"));
                struct LogAccessTimeRange range;
                CDateTime endtt;
                endtt.setNow();
                range.setEnd(endtt);
                StringBuffer endstr;
                endtt.getString(endstr, true); //local time!

                CDateTime startt;
                startt.setNow();
                startt.adjustTimeSecs(-60); //an hour ago
                range.setStart(startt);

                StringBuffer startstr;
                startt.getString(startstr, true); //local time!
                sampleQuery.append(", \"TimeRange\": { ");
                appendJSONStringValue(sampleQuery, "Start", startstr.str(), false);
                appendJSONStringValue(sampleQuery, "End", endstr.str(), false);
                sampleQuery.append(" }, "); //end TimeRange
                queryOptions.setTimeRange(range);
                queryOptions.setLimit(5);
                appendJSONStringValue(sampleQuery, "Limit", "5", false);
                sampleQuery.append(" }, ");//end filter

                StringBuffer logs;
                LogQueryResultDetails  resultDetails;
                fetchLog(resultDetails, queryOptions, logs, outputFormat);
                appendJSONValue(sampleQuery, "ResultCount", resultDetails.totalReceived);

                if (resultDetails.totalReceived == 0)
                {
                    status.escalateStatusCode(LOGACCESS_STATUS_warning);
                    status.appendMessage("Sample query returned zero log records!");
                }

                sampleQuery.appendf(", \"Results\": %s", logs.str());
            }
            catch(IException * e)
            {
                VStringBuffer description("Exception while executing sample query (%d) - ", e->errorCode());
                e->errorMessage(description);
                appendJSONStringValue(sampleQuery, "Results", description.str(), true);
                status.appendMessage(description.str());
                e->Release();
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }
            catch(...)
            {
                appendJSONStringValue(sampleQuery, "Results", "Unknown exception while executing samplequery", false);
                status.appendMessage("Unknown exception while executing samplequery");
                status.escalateStatusCode(LOGACCESS_STATUS_fail);
            }
            sampleQuery.append(" }}"); //close query, and top level json container

            if (options.IncludeSampleQuery)
                report.DebugReport.SampleQueryReport.set(sampleQuery);
        }
    }
    catch(...)
    {
        status.escalateStatusCode(LOGACCESS_STATUS_fail);
        status.appendMessage("Encountered unexpected exception during health report");
    }

    report.status = std::move(status);
 }

/*
 * Transform iterator of hits/fields to back-end agnostic response
 *
 */
unsigned processHitsJsonResp(IPropertyTreeIterator * iter, StringBuffer & returnbuf, LogAccessLogFormat format, bool wrapped, bool reportHeader)
{
    if (!iter)
        throw makeStringExceptionV(-1, "%s: Detected null 'hits' ElasticSearch response", COMPONENT_NAME);

    unsigned recsProcessed = 0;
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            if (wrapped)
                returnbuf.append("<lines>");

            ForEach(*iter)
            {
                IPropertyTree & cur = iter->query();
                returnbuf.append("<line>");
                toXML(&cur,returnbuf);
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
            bool first = true;
            ForEach(*iter)
            {
                IPropertyTree & cur = iter->query();
                toJSON(&cur,hitchildjson.clear());
                if (!first)
                    returnbuf.append(", ");

                first = false;
                returnbuf.appendf("{\"fields\": [ %s ]}", hitchildjson.str());
                recsProcessed++;
            }
            if (wrapped)
                returnbuf.append("]}");
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
        {
            ForEach(*iter)
            {
                IPropertyTree & cur = iter->query();
                Owned<IPropertyTreeIterator> fieldElementsItr = cur.getElements("*");

                bool first = true;
                if (reportHeader)
                {
                    ForEach(*fieldElementsItr)
                    {
                        if (!first)
                            returnbuf.append(", ");
                        else
                            first = false;
                        fieldElementsItr->query().getName(returnbuf);
                    }
                    returnbuf.newline();
                    first = true;

                    reportHeader = false;
                }

                //Process each column
                ForEach(*fieldElementsItr)
                {
                    if (!first)
                        returnbuf.append(", ");
                    else
                        first = false;

                    fieldElementsItr->query().getProp(nullptr, returnbuf); // commas in data should be escaped
                }
                returnbuf.newline();
                recsProcessed++;
            }
            break;
        }
        default:
            break;
    }
    return recsProcessed;
}

/*
 * Transform ES query response to back-end agnostic response
 *
 */
bool processESSearchJsonResp(LogQueryResultDetails & resultDetails, const cpr::Response & retrievedDocument, StringBuffer & returnbuf, LogAccessLogFormat format, bool reportHeader)
{
    if (retrievedDocument.status_code != 200)
        throw makeStringExceptionV(-1, "ElasticSearch request failed: '%s'", retrievedDocument.text.c_str());

    if (retrievedDocument.error)
        throw makeStringExceptionV(-1, "ElasticSearch request failed: CPR error: '%s'", retrievedDocument.error.message.c_str());

#ifdef _DEBUG
    DBGLOG("Retrieved ES JSON DOC: %s", retrievedDocument.text.c_str());
#endif

    Owned<IPropertyTree> tree = createPTreeFromJSONString(retrievedDocument.text.c_str());
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse ElasticSearch query response", COMPONENT_NAME);

    if (tree->getPropBool("timed_out", false))
        LOG(MCuserProgress,"ES Log Access: timeout reported");
    if (tree->getPropInt("_shards/failed",0) > 0)
        LOG(MCuserProgress,"ES Log Access: failed _shards reported");

    resultDetails.totalAvailable = tree->getPropInt("hits/total/value");
    PROGLOG("ES Log Access: hit count: '%d'", resultDetails.totalAvailable);

    Owned<IPropertyTreeIterator> hitsFieldsElements = tree->getElements("hits/hits/fields");
    resultDetails.totalReceived = processHitsJsonResp(hitsFieldsElements, returnbuf, format, true, reportHeader);

    return true;
}

/*
 * Transform ES scroll query response to back-end agnostic response
 *
 */
void processESScrollJsonResp(const char * retValue, StringBuffer & returnbuf, LogAccessLogFormat format, bool wrapped, bool header)
{
    Owned<IPropertyTree> tree = createPTreeFromJSONString(retValue);
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse ElasticSearch query response", COMPONENT_NAME);

    Owned<IPropertyTreeIterator> hitsFieldsElements = tree->getElements("hits/fields");
    processHitsJsonResp(hitsFieldsElements, returnbuf, format, wrapped, header);
}

void esTimestampQueryRangeString(std::string & range, const char * timestampfield, std::time_t from, std::time_t to)
{
    if (isEmptyString(timestampfield))
        throw makeStringException(-1, "ES Log Access: TimeStamp Field must be provided");

    //Elastic Search Date formats can be customized, but if no format is specified then it uses the default:
    //"strict_date_optional_time||epoch_millis"
    // "%Y-%m-%d"'T'"%H:%M:%S"

    //We'll report the timestamps as epoch_millis
    range = "\"range\": { \"";
    range += timestampfield;
    range += "\": {";
    range += "\"gte\": \"";
    range += std::to_string(from*1000);
    range += "\"";

    if (to != -1) //aka 'to' has been initialized
    {
        range += ",\"lte\": \"";
        range += std::to_string(to*1000);
        range += "\"";
    }
    range += "} }";
}

/*
 * Constructs ElasticSearch term clause
 * Use for exact term matches such as a price, a product ID, or a username.
 */
void esTermQueryString(std::string & search, const char *searchval, const char *searchfield)
{
    //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
    //You can use the term query to find documents based on a precise value such as a price, a product ID, or a username.

    //Avoid using the term query for text fields.
    //By default, Elasticsearch changes the values of text fields as part of analysis. This can make finding exact matches for text field values difficult.
    if (isEmptyString(searchval) || isEmptyString(searchfield))
        throw makeStringException(-1, "Could not create ES term query string: Either search value or search field is empty");

    search += "\"term\": { \"";
    search += searchfield;
    search += "\" : { \"value\": \"";
    search += searchval;
    search += "\" } }";
}

/*
 * Constructs ElasticSearch match clause
 * Use for full-text search
 */
void esMatchQueryString(std::string & search, const char *searchval, const char *searchfield)
{
    //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
    //Returns documents that match a provided text, number, date or boolean value. The provided text is analyzed before matching.
    //The match query is the standard query for performing a full-text search, including options for fuzzy matching.
    if (isEmptyString(searchval) || isEmptyString(searchfield))
        throw makeStringException(-1, "Could not create ES match query string: Either search value or search field is empty");

    search += "\"match\": { \"";
    search += searchfield;
    search += "\" : \"";
    search += searchval;
    search += "\" }";
}

/*
Translates LogAccess defined SortBy direction enum value to 
the Elastic Search query language corresponding counterpart
*/
const char * ElasticStackLogAccess::sortByDirectionToES(SortByDirection direction)
{
    switch (direction)
    {
    case SORTBY_DIRECTION_ascending:
        return "asc";
    case SORTBY_DIRECTION_descending:
        return "desc";
    case SORTBY_DIRECTION_none:
    default:
        return nullptr;
    }
}

/*
 * Construct Elasticsearch query directives string
 */
void ElasticStackLogAccess::esSearchMetaData(std::string & search, const LogAccessReturnColsMode retcolmode, const StringArray & selectcols, const SortByConditions & sortByConditions, unsigned size = DEFAULT_ES_DOC_LIMIT, offset_t from = DEFAULT_ES_DOC_START)
{
    //Query parameters:
    //https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-body.html

    //_source: https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-source-filtering.html
    search += "\"_source\": false, \"fields\": [" ;

    switch (retcolmode)
    {
    case RETURNCOLS_MODE_all:
        getAllColumns(search);
        break;
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
            StringBuffer sourcecols;
            ForEachItemIn(idx, selectcols)
            {
                sourcecols.appendf("\"%s\"", selectcols.item(idx));
                if (idx < selectcols.length() -1)
                    sourcecols.append(",");
            }

            search += sourcecols.str();
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

    search += "],";
    search += "\"from\": ";
    search += std::to_string(from);
    search += ", \"size\": ";
    search += std::to_string(size);
    search += ", ";

    if (sortByConditions.length() > 0)
    {
        bool first = true;
        search += "\"sort\" : [{ ";
        ForEachItemIn(index, sortByConditions)
        {
            if (!first)
                search += ", ";

            SortByCondition condition = sortByConditions.item(index);
            search += "\"";
            const char * sortByFieldName = nullptr;
            const char * format = nullptr;
            {
                switch (condition.byKnownField)
                {
                case LOGACCESS_MAPPEDFIELD_timestamp:
                    sortByFieldName = m_globalIndexTimestampField.str();
                    format = "strict_date_optional_time_nanos";
                    break;
                case LOGACCESS_MAPPEDFIELD_jobid:
                    sortByFieldName = m_workunitSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_component:
                    sortByFieldName = m_componentsSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_class:
                    sortByFieldName = m_classSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_audience:
                    sortByFieldName = m_audienceSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_instance:
                    sortByFieldName = m_instanceSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_host:
                    sortByFieldName = m_hostSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_traceid:
                    sortByFieldName = m_traceSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_spanid:
                    sortByFieldName = m_spanSearchColName.str();
                    break;
                case LOGACCESS_MAPPEDFIELD_unmapped:
                default:
                    sortByFieldName = condition.fieldName.get();
                    break;
                }
                search += sortByFieldName;
            }
            search += "\" : {";
            const char * direction = sortByDirectionToES(condition.direction);
            if (!isEmptyString(direction))
            {
                search += "\"order\" : \"";
                search += direction;
                search += "\"";
            }
            if (!isEmptyString(format))
            {
                if (!isEmptyString(direction))
                    search += ", ";

                search += "\"format\" : \"";
                search += format;
                search += "\"";
            }
            search += "}";
            first = false;
        }
        search += " }], ";
    }
}

/*
 * Constructs ElasticSearch querystring clause
 * Use for exact term matches based on operators such as AND or OR
 */
void ElasticStackLogAccess::populateESQueryQueryString(std::string & queryString, std::string & queryIndex, const ILogAccessFilter * filter)
{
    //https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
    //Returns documents based on a provided query string, using a parser with a strict syntax.

    //This query uses a syntax to parse and split the provided query string based on operators, 
    //such as AND or NOT. The query then analyzes each split text independently before returning matching documents.

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
            if (!queryIndex.empty() && queryIndex != m_workunitIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_workunitIndexSearchPattern.str());
            queryIndex = m_workunitIndexSearchPattern;
        }

        DBGLOG("%s: Searching log entries by jobid: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_trace:
    {
        if (m_traceSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'traceid' log entry field not configured", COMPONENT_NAME);

        queryField = m_traceSearchColName.str();

        if (!m_traceIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.empty() && queryIndex != m_traceIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_workunitIndexSearchPattern.str());
            queryIndex = m_traceIndexSearchPattern;
        }

        DBGLOG("%s: Searching log entries by traceid: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_span:
    {
        if (m_spanSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'spanid' log entry field not configured", COMPONENT_NAME);

        queryField = m_spanSearchColName.str();

        if (!m_spanIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.empty() && queryIndex != m_spanIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_workunitIndexSearchPattern.str());
            queryIndex = m_spanIndexSearchPattern;
        }

        DBGLOG("%s: Searchingsort log entries by spanid: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_class:
    {
        if (m_classSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Class' log entry field not configured", COMPONENT_NAME);

        queryField = m_classSearchColName.str();

        if (!m_classIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.empty() && queryIndex != m_classIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_classIndexSearchPattern.str());
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
            if (!queryIndex.empty() && queryIndex != m_audienceIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_audienceIndexSearchPattern.str());

            queryIndex = m_audienceIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by target audience: '%s'...", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_component:
    {
        if (m_componentsSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'Host' log entry field not configured", COMPONENT_NAME);

        queryField = m_componentsSearchColName.str();

        if (!m_componentsIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.empty() && queryIndex != m_componentsIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_componentsIndexSearchPattern.str());

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
            if (!queryIndex.empty() && queryIndex != m_hostIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_hostIndexSearchPattern.str());

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
            if (!queryIndex.empty() && queryIndex != m_instanceIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_instanceIndexSearchPattern.str());

            queryIndex = m_instanceIndexSearchPattern.str();
        }

        DBGLOG("%s: Searching log entries by HPCC component instance: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    case LOGACCESS_FILTER_wildcard:
        if (queryValue.isEmpty())
            throw makeStringExceptionV(-1, "%s: Wildcard filter cannot be empty!", COMPONENT_NAME);

        DBGLOG("%s: Searching log entries by wildcard filter: '%s: %s'...", COMPONENT_NAME, queryField.c_str(), queryValue.str());
        break;
    case LOGACCESS_FILTER_or:
    case LOGACCESS_FILTER_and:
        queryString += " ( ";
        populateESQueryQueryString(queryString, queryIndex, filter->leftFilterClause());
        queryString.append(" ");
        queryString += logAccessFilterTypeToString(filter->filterType());
        queryString.append(" ");

        populateESQueryQueryString(queryString, queryIndex, filter->rightFilterClause());
        queryString += " ) ";
        return; // queryString populated, need to break out
    case LOGACCESS_FILTER_column:
        if (filter->getFieldName() == nullptr)
            throw makeStringExceptionV(-1, "%s: empty field name detected in filter by column!", COMPONENT_NAME);
        queryField = filter->getFieldName();
        break;
    case LOGACCESS_FILTER_pod:
    {
        if (m_podSearchColName.isEmpty())
            throw makeStringExceptionV(-1, "%s: 'pod' log entry field not configured", COMPONENT_NAME);

        queryField = m_podSearchColName.str();

        if (!m_podIndexSearchPattern.isEmpty())
        {
            if (!queryIndex.empty() && queryIndex != m_podIndexSearchPattern.str())
                throw makeStringExceptionV(-1, "%s: Multi-index query not supported: '%s' - '%s'", COMPONENT_NAME, queryIndex.c_str(), m_instanceIndexSearchPattern.str());

            queryIndex = m_podIndexSearchPattern.str();
        }
        DBGLOG("%s: Searching log entries by Pod: '%s'", COMPONENT_NAME, queryValue.str() );
        break;
    }
    default:
        throw makeStringExceptionV(-1, "%s: Unknown query criteria type encountered: '%s'", COMPONENT_NAME, queryValue.str());
    }

    queryString += queryField + ":" + queryValue.str();

    if (queryIndex.empty())
        queryIndex = m_globalIndexSearchPattern.str();
}

void ElasticStackLogAccess::populateQueryStringAndQueryIndex(std::string & queryString, std::string & queryIndex, const LogAccessConditions & options)
{
    try
    {
        queryString = "{";
        esSearchMetaData(queryString, options.getReturnColsMode(), options.getLogFieldNames(), options.getSortByConditions(), options.getLimit(), options.getStartFrom());

        queryString += "\"query\": { \"bool\": { \"filter\": [ ";
        if (options.queryFilter() == nullptr || options.queryFilter()->filterType() == LOGACCESS_FILTER_wildcard) // No filter
        {
            queryIndex = m_globalIndexSearchPattern.str();
        }
        else
        {
            queryString += "{ \"query_string\": { \"query\": \"";
            populateESQueryQueryString(queryString, queryIndex, options.queryFilter());
            queryString += "\" } },";
        }

        std::string range;
        const LogAccessTimeRange & trange = options.getTimeRange();
        //Bail out earlier?
        if (trange.getStartt().isNull())
            throw makeStringExceptionV(-1, "%s: start time must be provided!", COMPONENT_NAME);

        esTimestampQueryRangeString(range, m_globalIndexTimestampField.str(), trange.getStartt().getSimple(),trange.getEndt().isNull() ? -1 : trange.getEndt().getSimple());

        queryString += "{ " + range;
        queryString += "}]}}}"; //end range, filter array, bool, query, and request

        DBGLOG("%s: Search string '%s'", COMPONENT_NAME, queryString.c_str());
    }
    catch (std::runtime_error &e)
    {
        const char * wha = e.what();
        throw makeStringExceptionV(-1, "%s: Error populating ES search string: %s", COMPONENT_NAME, wha);
    }
    catch (IException * e)
    {
        StringBuffer mess;
        e->errorMessage(mess);
        e->Release();
        throw makeStringExceptionV(-1, "%s: Error populating ES search string: %s", COMPONENT_NAME, mess.str());
    }
}

/*
 * Construct ES query string, execute query
 */
cpr::Response ElasticStackLogAccess::performESQuery(const LogAccessConditions & options)
{
    try
    {
        std::string queryString;
        std::string queryIndex;
        populateQueryStringAndQueryIndex(queryString, queryIndex, options);
        return m_esClient.search(queryIndex.c_str(), DEFAULT_ES_DOC_TYPE, queryString);
    }
    catch (std::runtime_error &e)
    {
        const char * wha = e.what();
        throw makeStringExceptionV(-1, "%s: fetchLog: Error searching doc: %s", COMPONENT_NAME, wha);
    }
    catch (IException * e)
    {
        StringBuffer mess;
        e->errorMessage(mess);
        e->Release();
        throw makeStringExceptionV(-1, "%s: fetchLog: Error searching doc: %s", COMPONENT_NAME, mess.str());
    }
}

bool ElasticStackLogAccess::fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    cpr::Response esresp = performESQuery(options);
    return processESSearchJsonResp(resultDetails, esresp, returnbuf, format, true);
}

class ElasticStackLogStream : public CInterfaceOf<IRemoteLogAccessStream>
{
public:
    virtual bool readLogEntries(StringBuffer & record, unsigned & recsRead) override
    {
        Json::Value res;
        recsRead = 0;

        if (m_esSroller.next(res))
        {
            if (!res["hits"].empty())
            {
                recsRead = res["hits"].size();
                std::ostringstream sout;
                m_jsonWriter->write(res, &sout); // serialize Json object to string for processing
                processESScrollJsonResp(sout.str().c_str(), record, m_outputFormat, false, !m_hasBeenScrolled); // convert Json string to target format
                m_hasBeenScrolled = true;
                return true;
            }
        }

        return false;
    }

    ElasticStackLogStream(std::string & queryString, const char * connstr, const char * indexsearchpattern, LogAccessLogFormat format,  std::size_t pageSize, std::string scrollTo)
     : m_esSroller(std::make_shared<elasticlient::Client>(std::vector<std::string>({connstr})), pageSize, scrollTo)
    {
        m_outputFormat = format;
        m_esSroller.init(indexsearchpattern, DEFAULT_ES_DOC_TYPE, queryString);
        m_jsonWriter.reset(m_jsonStreamBuilder.newStreamWriter());
    }

    virtual ~ElasticStackLogStream() override = default;

private:
    elasticlient::Scroll m_esSroller;

    bool m_hasBeenScrolled = false;
    LogAccessLogFormat m_outputFormat;
    Json::StreamWriterBuilder m_jsonStreamBuilder;
    std::unique_ptr<Json::StreamWriter> m_jsonWriter;
};

IRemoteLogAccessStream * ElasticStackLogAccess::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format)
{
    return getLogReader(options, format, DEFAULT_MAX_RECORDS_PER_FETCH);
}

IRemoteLogAccessStream * ElasticStackLogAccess::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
{
    std::string queryString;
    std::string queryIndex;
    populateQueryStringAndQueryIndex(queryString, queryIndex, options);
    return new ElasticStackLogStream(queryString, m_esConnectionStr.str(), queryIndex.c_str(), format, pageSize, DEFAULT_SCROLL_TIMEOUT);
}

extern "C" IRemoteLogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
    //constructing ES Connection string(s) here b/c ES Client explicit ctr requires conn string array

    StringBuffer encodedPassword, encodedUserName;
    StringBuffer credentialsSecretKey;
    logAccessPluginConfig.getProp("connection/credentials/@secretName", credentialsSecretKey);  // vault/secrets key
    if (!credentialsSecretKey.isEmpty())
    {
        StringBuffer credentialsVaultId;
        logAccessPluginConfig.getProp("connection/credentials/@vaultId", credentialsVaultId);//optional HashiCorp vault ID

        PROGLOG("%s: Retrieving host authentication username/password from secrets tree '%s', from vault '%s'",
               COMPONENT_NAME, credentialsSecretKey.str(), !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "N/A");

        Owned<const IPropertyTree> secretTree(getSecret("esp", credentialsSecretKey.str(), credentialsVaultId, nullptr));
        if (secretTree == nullptr)
        {
            WARNLOG("%s: Unable to load secret tree '%s', from vault '%s'",
                COMPONENT_NAME, credentialsSecretKey.str(), !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "N/A");
        }

        StringBuffer userName, password;
        if (!getSecretKeyValue(userName, secretTree, "username") || !getSecretKeyValue(password, secretTree, "password"))
        {
            WARNLOG("ElasticMetricSink: Missing username and/or password from secrets tree '%s', vault '%s'",
                    credentialsSecretKey.str(), !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "n/a");
        }

        encodeUrlUseridPassword(encodedPassword, password.str());
        encodeUrlUseridPassword(encodedUserName, userName.str());
    }

    const char * protocol = logAccessPluginConfig.queryProp("connection/@protocol");
    const char * host = logAccessPluginConfig.queryProp("connection/@host");
    const char * port = logAccessPluginConfig.queryProp("connection/@port");

    std::string elasticSearchConnString;
    elasticSearchConnString = isEmptyString(protocol) ? DEFAULT_ES_PROTOCOL : protocol;
    elasticSearchConnString.append("://");
    if (!isEmptyString(encodedUserName) && !isEmptyString(encodedPassword))
    {
        elasticSearchConnString.append(encodedUserName).append(":").append(encodedPassword).append("@");
    }
    elasticSearchConnString.append(isEmptyString(host) ? DEFAULT_ES_HOST : host);
    elasticSearchConnString.append(":").append((!port || !*port) ? DEFAULT_ES_PORT : port);
    elasticSearchConnString.append("/"); // required!

    return new ElasticStackLogAccess({elasticSearchConnString}, logAccessPluginConfig);
}
