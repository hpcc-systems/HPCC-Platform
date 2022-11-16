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

static constexpr const char * LOGMAP_INDEXPATTERN_ATT = "@storeName";
static constexpr const char * LOGMAP_SEARCHCOL_ATT = "@searchColumn";
static constexpr const char * LOGMAP_TIMESTAMPCOL_ATT = "@timeStampColumn";

static constexpr const char * DEFAULT_SCROLL_TIMEOUT = "1m"; //Elastic Time Units (i.e. 1m = 1 minute).
static constexpr std::size_t  DEFAULT_MAX_RECORDS_PER_FETCH = 100;

void ElasticStackLogAccess::getMinReturnColumns(std::string & columns)
{
    //timestamp, source component, message
    columns.append(" \"").append(m_globalIndexTimestampField).append("\", \"").append(m_componentsSearchColName.str()).append("\", \"").append(m_globalSearchColName).append("\" ");
}

void ElasticStackLogAccess::getDefaultReturnColumns(std::string & columns)
{
    //timestamp, source component, all hpcc.log fields
    columns.append(" \"").append(m_globalIndexTimestampField).append("\", \"").append(m_componentsSearchColName.str()).append("\", \"hpcc.log.*\" ");
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
        else if (streq(logMapType, "host") || streq(logMapType, "node"))
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

const IPropertyTree * ElasticStackLogAccess::performAndLogESRequest(Client::HTTPMethod httpmethod, const char * url, const char * reqbody, const char * logmessageprefix, LogMsgCategory reqloglevel = MCdebugProgress, LogMsgCategory resploglevel = MCdebugProgress)
{
    try
    {
        LOG(reqloglevel,"ESLogAccess: Requesting '%s'... ", logmessageprefix );
        cpr::Response esREsponse = m_esClient.performRequest(httpmethod,url,reqbody);
        Owned<IPropertyTree> response = createPTreeFromJSONString(esREsponse.text.c_str());
        LOG(resploglevel,"ESLogAccess: '%s' response: '%s'", logmessageprefix, esREsponse.text.c_str());

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
        throw makeStringExceptionV(-1, "%s: Wild Card filter detected within exact term filter!", COMPONENT_NAME);
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
        if (options.queryFilter()->filterType() == LOGACCESS_FILTER_wildcard) // No filter
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

    const char * protocol = logAccessPluginConfig.queryProp("connection/@protocol");
    const char * host = logAccessPluginConfig.queryProp("connection/@host");
    const char * port = logAccessPluginConfig.queryProp("connection/@port");

    std::string elasticSearchConnString;
    elasticSearchConnString = isEmptyString(protocol) ? DEFAULT_ES_PROTOCOL : protocol;
    elasticSearchConnString.append("://");
    elasticSearchConnString.append(isEmptyString(host) ? DEFAULT_ES_HOST : host);
    elasticSearchConnString.append(":").append((!port || !*port) ? DEFAULT_ES_PORT : port);
    elasticSearchConnString.append("/"); // required!

    return new ElasticStackLogAccess({elasticSearchConnString}, logAccessPluginConfig);
}
