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

#include "AzureBlobLogAccess.hpp"

#include "platform.h"

#include <string>
#include <vector>

#include <cstdio>
#include <iostream>
#include <stdexcept>


static constexpr int DEFAULT_ES_DOC_LIMIT = 100;
static constexpr int DEFAULT_ES_DOC_START = 0;

static constexpr const char * DEFAULT_TS_NAME = "@timestamp";
static constexpr const char * DEFAULT_INDEX_PATTERN = "hpcc-logs*";

static constexpr const char * DEFAULT_HPCC_LOG_SEQ_COL         = "hpcc.log.sequence";
static constexpr const char * DEFAULT_HPCC_LOG_TIMESTAMP_COL   = "hpcc.log.timestamp";
static constexpr const char * DEFAULT_HPCC_LOG_PROCID_COL      = "hpcc.log.procid";
static constexpr const char * DEFAULT_HPCC_LOG_THREADID_COL    = "hpcc.log.threadid";
static constexpr const char * DEFAULT_HPCC_LOG_MESSAGE_COL     = "hpcc.log.message";
static constexpr const char * DEFAULT_HPCC_LOG_JOBID_COL       = "hpcc.log.jobid";
static constexpr const char * DEFAULT_HPCC_LOG_COMPONENT_COL   = "kubernetes.container.name";
static constexpr const char * DEFAULT_HPCC_LOG_TYPE_COL        = "hpcc.log.class";
static constexpr const char * DEFAULT_HPCC_LOG_AUD_COL         = "hpcc.log.audience";

static constexpr const char * LOGMAP_INDEXPATTERN_ATT = "@storeName";
static constexpr const char * LOGMAP_SEARCHCOL_ATT = "@searchColumn";
static constexpr const char * LOGMAP_TIMESTAMPCOL_ATT = "@timeStampColumn";

static constexpr const char * DEFAULT_SCROLL_TIMEOUT = "1m"; //Elastic Time Units (i.e. 1m = 1 minute).
static constexpr std::size_t  DEFAULT_MAX_RECORDS_PER_FETCH = 100;

const char * GetConnectionString()
{
    try
    {
        const char * envConnectionString = std::getenv("AZURE_STORAGE_CONNECTION_STRING");

        if (!envConnectionString)
            WARNLOG("Environment variable 'AZURE_STORAGE_CONNECTION_STRING' not found!");
        else
            return envConnectionString;
    }
    catch(const std::exception& e)
    {
        ERRLOG("Encountered error fetching 'AZURE_STORAGE_CONNECTION_STRING' environment variable: '%s'.", e.what());
    }
   return "";
}

AzureBlobLogAccess::AzureBlobLogAccess(const char * blobConnString, IPropertyTree & logAccessPluginConfig)
: m_containerClient(blobConnString)
{
    m_esConnectionStr.set(blobConnString);

    //const std::string containerName = "$logs";
    const std::string containerName = "rjptestblobcontainer2";
    //const std::string blobName = "logs/2022/05/18/01";
    const std::string blobName = "some blob name";
    const std::string connstr = m_esConnectionStr.str();

    m_containerClient = BlobContainerClient::CreateFromConnectionString(connstr, containerName);

    try
    {
        m_containerClient.CreateIfNotExists();
    }
    catch(const std::exception& e)
    {
        ERRLOG("Encountered issue attempting to create Azure container client: '%s'", e.what());
        return;
    }

#ifdef LOGACCESSDEBUG
    try
    {
        const std::string blobContent = "00000001 PRG INF 2022-05-11 21:03:53.562     1     1 W20220511-210301 \"Rodrigos log messsage\"";
        BlockBlobClient blobClient = m_containerClient.GetBlockBlobClient(blobName);

        std::vector<uint8_t> buffer(blobContent.begin(), blobContent.end());

        auto properties = blobClient.GetProperties().Value;
        for (auto metadata : properties.Metadata)
        {
            std::cout << metadata.first << ":" << metadata.second << std::endl;
        }

        // We know blob size is small, so it's safe to cast here.
        buffer.resize(static_cast<size_t>(properties.BlobSize));

        blobClient.DownloadTo(buffer.data(), buffer.size());

        std::cout << std::string(buffer.begin(), buffer.end()) << std::endl;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
#endif

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

bool AzureBlobLogAccess::fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    //cpr::Response esresp = performESQuery(options);
    //return processESSearchJsonResp(resultDetails, esresp, returnbuf, format, true);
    return false;
}

class AZUREBLOB_LOGACCESS_API AzureBlobLogStream : public CInterfaceOf<IRemoteLogAccessStream>
{
public:
    virtual bool readLogEntries(StringBuffer & record, unsigned & recsRead) override
    {
        return false;
    }

    AzureBlobLogStream(std::string & queryString, const char * connstr, const char * indexsearchpattern, LogAccessLogFormat format,  std::size_t pageSize, std::string scrollTo)
     //: m_esSroller(std::make_shared<elasticlient::Client>(std::vector<std::string>({connstr})), pageSize, scrollTo)
    {
        m_outputFormat = format;
    }

    virtual ~AzureBlobLogStream() override = default;

private:
    bool m_hasBeenScrolled = false;
    LogAccessLogFormat m_outputFormat;
};

IRemoteLogAccessStream * AzureBlobLogAccess::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format)
{
    return getLogReader(options, format, DEFAULT_MAX_RECORDS_PER_FETCH);
}

IRemoteLogAccessStream * AzureBlobLogAccess::getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize)
{
    return nullptr;
}

extern "C" IRemoteLogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
    const char * protocol = logAccessPluginConfig.queryProp("connection/@protocol");
    const char * host = logAccessPluginConfig.queryProp("connection/@host");
    const char * port = logAccessPluginConfig.queryProp("connection/@port");

    return new AzureBlobLogAccess(GetConnectionString(), logAccessPluginConfig);
}
