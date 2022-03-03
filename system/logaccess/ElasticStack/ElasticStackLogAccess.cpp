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

static constexpr const char * DEFAULT_TS_NAME = "@timestamp";
static constexpr const char * DEFAULT_INDEX_PATTERN = "filebeat*";

static constexpr const char * DEFAULT_HPCC_LOG_SEQ_COL         = "hpcc.log.sequence";
static constexpr const char * DEFAULT_HPCC_LOG_TIMESTAMP_COL   = "hpcc.log.timestamp";
static constexpr const char * DEFAULT_HPCC_LOG_PROCID_COL      = "hpcc.log.procid";
static constexpr const char * DEFAULT_HPCC_LOG_THREADID_COL    = "hpcc.log.threadid";
static constexpr const char * DEFAULT_HPCC_LOG_MESSAGE_COL     = "hpcc.log.message";
static constexpr const char * DEFAULT_HPCC_LOG_JOBID_COL       = "hpcc.log.jobid";
static constexpr const char * DEFAULT_HPCC_LOG_COMPONENT_COL   = "kubernetes.container.name";
static constexpr const char * DEFAULT_HPCC_LOG_TYPE_COL        = "hpcc.log.class";
static constexpr const char * DEFAULT_HPCC_LOG_AUD_COL         = "hpcc.log.audience";

static constexpr const char * LOGMAP_INDEXPATTERN_ATT = "@storename";
static constexpr const char * LOGMAP_SEARCHCOL_ATT = "@searchcolumn";
static constexpr const char * LOGMAP_TIMESTAMPCOL_ATT = "@timestampcolumn";

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
 * Transform ES query response to back-end agnostic response
 *
 */
void processESJsonResp(const cpr::Response & retrievedDocument, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    if (retrievedDocument.status_code != 200)
        throw makeStringExceptionV(-1, "ElasticSearch request failed: %s", retrievedDocument.status_line.c_str());

    DBGLOG("Retrieved ES JSON DOC: %s", retrievedDocument.text.c_str());

    Owned<IPropertyTree> tree = createPTreeFromJSONString(retrievedDocument.text.c_str());
    if (!tree)
        throw makeStringExceptionV(-1, "%s: Could not parse ElasticSearch query response", COMPONENT_NAME);

    if (tree->getPropBool("timed_out", 0))
        LOG(MCuserProgress,"ES Log Access: timeout reported");
    if (tree->getPropInt("_shards/failed",0) > 0)
        LOG(MCuserProgress,"ES Log Access: failed _shards reported");

    DBGLOG("ES Log Access: hit count: '%d'", tree->getPropInt("hits/total/value"));

    Owned<IPropertyTreeIterator> iter = tree->getElements("hits/hits/fields");
    switch (format)
    {
        case LOGACCESS_LOGFORMAT_xml:
        {
            returnbuf.append("<lines>");

            ForEach(*iter)
            {
                IPropertyTree & cur = iter->query();
                returnbuf.append("<line>");
                toXML(&cur,returnbuf);
                returnbuf.append("</line>");
            }
            returnbuf.append("</lines>");
            break;
        }
        case LOGACCESS_LOGFORMAT_json:
        {
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
            }
            returnbuf.append("]}");
            break;
        }
        case LOGACCESS_LOGFORMAT_csv:
        {
            ForEach(*iter)
            {
                IPropertyTree & cur = iter->query();
                bool first = true;
                Owned<IPropertyTreeIterator> fieldelementsitr = cur.getElements("*");
                ForEach(*fieldelementsitr)
                {
                    if (!first)
                        returnbuf.append(", ");
                    else
                        first = false;

                    returnbuf.append(fieldelementsitr->query().queryProp(".")); // commas in data should be escaped
                }
                returnbuf.append("\n");
            }
            break;
        }
        default:
            break;
    }
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
 * Constructs ElasticSearch match clause
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
 * Construct Elasticsearch query directives string
 */
void esSearchMetaData(std::string & search, const  StringArray & selectcols, unsigned size = DEFAULT_ES_DOC_LIMIT, offset_t from = DEFAULT_ES_DOC_START)
{
    //Query parameters:
    //https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-body.html

    //_source: https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-source-filtering.html
    search += "\"_source\": false, \"fields\": [" ;

    if (selectcols.length() > 0)
    {
        StringBuffer sourcecols;
        ForEachItemIn(idx, selectcols)
        {
            sourcecols.appendf("\"%s\"", selectcols.item(idx));
            if (idx < selectcols.length() -1)
                sourcecols.append(",");
        }

        if (!sourcecols.isEmpty())
            search += sourcecols.str() ;
        else
            search += "\"*\"";
            //search += "!*.keyword"; //all fields ignoring the .keyword postfixed fields
    }
    else
        //search += "!*.keyword"; //all fields ignoring the .keyword postfixed fields
        search += "\"*\"";

    search += "],";
    search += "\"from\": ";
    search += std::to_string(from);
    search += ", \"size\": ";
    search += std::to_string(size);
    search += ", ";
}

/*
 * Construct ES query string, execute query
 */
cpr::Response ElasticStackLogAccess::performESQuery(const LogAccessConditions & options)
{
    try
    {
        StringBuffer queryValue;
        std::string queryField = m_globalSearchColName.str();
        std::string queryIndex = m_globalIndexSearchPattern.str();

        bool fullTextSearch = true;
        bool wildCardSearch = false;

        options.queryFilter()->toString(queryValue);
        switch (options.queryFilter()->filterType())
        {
        case LOGACCESS_FILTER_jobid:
        {
            if (!m_workunitSearchColName.isEmpty())
            {
                queryField = m_workunitSearchColName.str();
                fullTextSearch = false; //found dedicated components column
            }

            if (!m_workunitIndexSearchPattern.isEmpty())
            {
                queryIndex = m_workunitIndexSearchPattern.str();
            }

            DBGLOG("%s: Searching log entries by jobid: '%s'...", COMPONENT_NAME, queryValue.str() );
            break;
        }
        case LOGACCESS_FILTER_class:
        {
            if (!m_classSearchColName.isEmpty())
            {
                queryField = m_classSearchColName.str();
                fullTextSearch = false; //found dedicated components column
            }

            if (!m_classIndexSearchPattern.isEmpty())
            {
                queryIndex = m_classIndexSearchPattern.str();
            }

            DBGLOG("%s: Searching log entries by class: '%s'...", COMPONENT_NAME, queryValue.str() );
            break;
        }
        case LOGACCESS_FILTER_audience:
        {
            if (!m_audienceSearchColName.isEmpty())
            {
                queryField = m_audienceSearchColName.str();
                fullTextSearch = false; //found dedicated components column
            }

            if (!m_audienceIndexSearchPattern.isEmpty())
            {
                queryIndex = m_audienceIndexSearchPattern.str();
            }

            DBGLOG("%s: Searching log entries by target audience: '%s'...", COMPONENT_NAME, queryValue.str() );
            break;
        }
        case LOGACCESS_FILTER_component:
        {
            if (!m_componentsSearchColName.isEmpty())
            {
                queryField = m_componentsSearchColName.str();
                fullTextSearch = false; //found dedicated components column
            }

            if (!m_componentsIndexSearchPattern.isEmpty())
            {
                queryIndex = m_componentsIndexSearchPattern.str();
            }

            DBGLOG("%s: Searching '%s' component log entries...", COMPONENT_NAME, queryValue.str() );
            break;
        }
        case LOGACCESS_FILTER_wildcard:
        {
            wildCardSearch = true;
            DBGLOG("%s: Performing wildcard log entry search...", COMPONENT_NAME);
            break;
        }
        case LOGACCESS_FILTER_or:
            throw makeStringExceptionV(-1, "%s: Compound query criteria not currently supported: '%s'", COMPONENT_NAME, queryValue.str());
            //"query":{"bool":{"must":[{"match":{"kubernetes.container.name.keyword":{"query":"eclwatch","operator":"or"}}},{"match":{"container.image.name.keyword":"hpccsystems\\core"}}]} }
        case LOGACCESS_FILTER_and:
            throw makeStringExceptionV(-1, "%s: Compound query criteria not currently supported: '%s'", COMPONENT_NAME, queryValue.str());
            //"query":{"bool":{"must":[{"match":{"kubernetes.container.name.keyword":{"query":"eclwatch","operator":"and"}}},{"match":{"created_ts":"2021-08-25T20:23:04.923Z"}}]} }
        default:
            throw makeStringExceptionV(-1, "%s: Unknown query criteria type encountered: '%s'", COMPONENT_NAME, queryValue.str());
        }

        std::string fullRequest = "{";
        esSearchMetaData(fullRequest, options.getLogFieldNames(), options.getLimit(), options.getStartFrom());

        fullRequest += "\"query\": { \"bool\": {";

        if(!wildCardSearch)
        {
            fullRequest += " \"must\": { ";

            std::string criteria;
            if (fullTextSearch) //are we performing a query on a blob, or exact term match?
                esMatchQueryString(criteria, queryValue.str(), queryField.c_str());
            else
                esTermQueryString(criteria, queryValue.str(), queryField.c_str());

            fullRequest += criteria;
            fullRequest += "}, "; //end must, expect filter to follow
        }

        std::string filter = "\"filter\": {";
        std::string range;

        const LogAccessTimeRange & trange = options.getTimeRange();
        //Bail out earlier?
        if (trange.getStartt().isNull())
            throw makeStringExceptionV(-1, "%s: start time must be provided!", COMPONENT_NAME);

        esTimestampQueryRangeString(range, m_globalIndexTimestampField.str(), trange.getStartt().getSimple(),trange.getEndt().isNull() ? -1 : trange.getEndt().getSimple());

        filter += range;
        filter += "}"; //end filter

        fullRequest += filter;
        fullRequest += "}}}"; //end bool and query

        DBGLOG("%s: Search string '%s'", COMPONENT_NAME, fullRequest.c_str());

        return m_esClient.search(queryIndex.c_str(), DEFAULT_ES_DOC_TYPE, fullRequest);
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

bool ElasticStackLogAccess::fetchLog(const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format)
{
    cpr::Response esresp = performESQuery(options);
    processESJsonResp(esresp, returnbuf, format);

    return true;
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
