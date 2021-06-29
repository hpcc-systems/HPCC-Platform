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
#include <ctime>
#include <iomanip>
#include <json/json.h>

ElasticStackLogAccess::ElasticStackLogAccess(const std::vector<std::string> &hostUrlList, IPropertyTree & logAccessPluginConfig) : m_esClient(hostUrlList)
{
    m_pluginCfg.set(&logAccessPluginConfig);

#ifdef _DEBUG
    StringBuffer xml;
    toXML(m_pluginCfg, xml);
    fprintf(stdout, "%s", xml.str());
#endif

    m_globalIndexSearchPattern = m_pluginCfg->queryProp("logmap/global/@indexsearchpattern");
    m_globalSearchColName = m_pluginCfg->queryProp("logmap/global/@searchcolumn");

    getESStatus();
    getIndexSearchStatus();


#ifdef _DEBUG
    std::string document {"{\"kubernetes.container.name\": \"eclwatch\", \"container.image.name\": \"hpccsystems\\\\core\", \"message\": \"00000777 USR 2021-06-18 17:55:03.316 975333 975333 W20210101-121212 \\\"CSmartSocketFactory::CSmartSocketFactory(192.168.1.140:9876)\\\"\"}"};

    std::string testindex = "filebeat-7.9.3-2021.06.18-000001";

    try
    {
        // Index the document, index "testindex" must be created before
        cpr::Response indexResponse = m_esClient.index(testindex, DEFAULT_ES_DOC_TYPE, "", document);
        // 200
        std::cout << indexResponse.status_code << std::endl;
        // application/json; charset=UTF-8
        std::cout << "Search results: "<< std::endl;
        // Elasticsearch response (JSON text string)
        std::cout << indexResponse.text << std::endl;

    }
    catch (std::runtime_error &e)
    {
        const char * wha = e.what();
        fprintf(stdout, "Error creating index %s", wha);
    }
    catch (IException * e)
    {
        StringBuffer mess;
        const char * message = e->errorMessage(mess);
        fprintf(stdout, "Error creating index %s", mess.str());
    }

    try
    {
        // Retrieve the document
        cpr::Response retrievedDocument = m_esClient.get(testindex, DEFAULT_ES_DOC_TYPE, "");
        // 200
        std::cout << retrievedDocument.status_code << std::endl;
        // application/json; charset=UTF-8
        std::cout << retrievedDocument.header["content-type"] << std::endl;
        // Elasticsearch response (JSON text string) where key "_source" contain:
        // {"message": "Hello world!"}
        Owned<IPropertyTree> resptree = createPTreeFromJSONString(retrievedDocument.text.c_str());
        std::cout << retrievedDocument.text << std::endl;

        Json::Value root;
        Json::CharReaderBuilder builder;
        Json::CharReader * reader = builder.newCharReader();

        std::string errors;

        bool parsingSuccessful = reader->parse(retrievedDocument.text.c_str(), retrievedDocument.text.c_str() + retrievedDocument.text.size(), &root, &errors);
        delete reader;

        if (!parsingSuccessful)
            return;

        const Json::Value &items = root["items"];
    }
    catch (std::runtime_error &e)
    {
        const char * wha = e.what();
        fprintf(stdout, "Error retrieving doc: %s", wha);
    }
    catch (IException * e)
    {
        StringBuffer mess;
        const char * message = e->errorMessage(mess);
        fprintf(stdout, "Error retrieving doc: %s", mess.str());
    }
#endif
}

IPropertyTree * ElasticStackLogAccess::getIndexSearchStatus()
{
    try
    {
        VStringBuffer indexsearch("_cat/indices/%s?format=JSON", m_globalIndexSearchPattern.c_str());
        LOG(MCuserProgress,"ES Log Access: Seeking list of available indexes: %s", indexsearch.str());
        cpr::Response indexResponse = m_esClient.performRequest(Client::HTTPMethod::GET, indexsearch.str(), "");
        Owned<IPropertyTree> esStatus = createPTreeFromJSONString(indexResponse.text.c_str());
        LOG(MCuserProgress,"ES Log Access: List of available indexes: '%s'", indexResponse.text.c_str());

        return esStatus.getClear();
    }
    catch (ConnectionException & ce)//std::runtime_error
    {
        LOG(MCuserError, "ES Log Access: Encountered error searching available indexes: '%s'", ce.what());
    }
    catch (...)
    {
        LOG(MCuserError, "ES Log Access: Encountered error searching available indexes");
    }
    return nullptr;
}

IPropertyTree * ElasticStackLogAccess::getESStatus()
{
    try
    {
        LOG(MCuserProgress,"ES Log Access: Seeking target cluster health...");
        cpr::Response indexResponse = m_esClient.performRequest(Client::HTTPMethod::GET, "_cluster/health", "");
        Owned<IPropertyTree> esStatus = createPTreeFromJSONString(indexResponse.text.c_str());
        LOG(MCuserProgress,"ES Log Access: Cluster health response: '%s'", indexResponse.text.c_str());

        return esStatus.getClear();
    }
    catch (ConnectionException & ce)//std::runtime_error
    {
        LOG(MCuserError, "ES Log Access: Encountered error while seeking target cluster health: '%s'", ce.what());
    }
    catch (...)
    {
        LOG(MCuserError, "ES Log Access: Encountered unknow error while seeking target cluster health");
    }

    return nullptr;
}

void processESJsonResp(const cpr::Response & retrievedDocument, StringBuffer & returnbuf)
{
    if (retrievedDocument.status_code == 200)
    {
        DBGLOG("Retrieved ES JSON DOC: %s", retrievedDocument.text.c_str());

        Json::Value root;
        Json::CharReaderBuilder builder;
        Json::CharReader * reader = builder.newCharReader();

        std::string errors;

        bool parsingSuccessful = reader->parse(retrievedDocument.text.c_str(), retrievedDocument.text.c_str() + retrievedDocument.text.size(), &root, &errors);
        delete reader;

        if (!parsingSuccessful)
            throw MakeStringException(-1, "Could not parse ElasticSearch query response: %s", errors.c_str());

        if (root["timed_out"] == true)
            LOG(MCuserProgress,"ES Log Access: timeout reported");

        if (root["_shards"]["failed"] > 0)
            LOG(MCuserProgress,"ES Log Access: failed _shards reported");

        DBGLOG("ES Log Access: hit count: '%s'", root["hits"]["total"]["value"].asCString());

        const Json::Value & hitsarray = root["hits"]["hits"];
        for (int hitindex = 0; hitindex < hitsarray.size(); hitindex++)
        {
            const Json::Value &fieldsarray = hitsarray[hitindex]["fields"];
            for (int fieldindex = 0; fieldindex < fieldsarray.getMemberNames().size(); fieldindex++)
            {
                returnbuf.append(fieldsarray.getMemberNames()[fieldindex].c_str()).append(": ");
                returnbuf.append(fieldsarray[fieldsarray.getMemberNames()[fieldindex]][0].asString().c_str()).append("\n");
            }
        }
    }
    else
    {
        throw MakeStringException(-1, "ElasticSearch request failed: %s", retrievedDocument.status_line.c_str());
    }
}

void esTimestampQueryRangeString(std::string & search, const char * timestampfield)
{
    if (timestampfield && *timestampfield)
    {
        search += "\"query\": { \"range\": { \"";
        search += timestampfield;
        search += "\": {";


        auto t = std::time(nullptr);
        auto tm = *std::localtime(&t);

        std::cout << std::put_time(&tm, "%Y-%m-dT%H:%M:%S.%z%z") << "\n";

        tm.tm_hour = tm.tm_hour +1;

        std::cout << std::put_time(&tm, "%Y-%m-dT%H:%M:%S.%z%z") << "\n";


        //            "gte": "2021-06-18T04:33:36.549Z",
        //            "lte": "2021-06-19T04:33:36.549Z"

        //            "gte": "2021-06-18T04:33:36.549Z",
        //            "lte": "2021-06-19T04:33:36.549Z"
        search += "} } }";
    }
    else
        throw MakeStringException(-1, "Could not create ES range string: Either search value or search field is empty");
}
/*
 * Constructs ElasticSearch query string where condition is <search field> = <searchval>
 */
void esSearchString(std::string & search, const char *searchval, const char *searchfield, StringArray & selectcols, int size = 10, int from = 0)
{
    if (searchval && *searchval && searchfield && *searchfield)
    {
        search = "{\"_source\": false, \"fields\": [\"" ;

        if (selectcols.length() > 0)
        {
            StringBuffer sourcecols;
            selectcols.getString(sourcecols, ",");
            if (!sourcecols.isEmpty())
                search += sourcecols.str() ;
            else
                search += "*";
        }
        else
            search += "*";

        search += "\"],";
        search += "\"from\": ";
        search += std::to_string(from);
        search += ", \"size\": ";
        search += std::to_string(size);
        search += ", \"query\": {\"match\": { \"";
        search += searchfield;
        search += "\" : \"";
        search += searchval;
        search += "\" } } }";

        DBGLOG("ES search string: '%s'", search.c_str());
    }
    else
        throw MakeStringException(-1, "Could not create ES search string: Either search value or search field is empty");
}

bool ElasticStackLogAccess::fetchLog(LogAccessConditions & options, StringBuffer & returnbuf)
{
    try
    {
        switch (options.filter.filterType)
        {
        case ALF_WorkUnit:
        {
            DBGLOG("Searching '%s' WUID log entries...", options.filter.workUnit.str());

            std::string to;
            std::string from;
            switch (options.range.rangeType)
            {
                case ACRTLastNYears:
                {
                    auto t = std::time(nullptr);
                    auto tm = *std::localtime(&t);

                    char mbstr[100];
                    if (std::strftime(mbstr, sizeof(mbstr), "%Y-%m-dT%H:%M:%S.%z%z", &tm))
                    {
                        std::cout <<mbstr << '\n';
                    }
                    //to = std::put_time(&tm, "%Y-%m-dT%H:%M:%S.%z%z");
                    tm.tm_year = tm.tm_year - options.range.quantity;
                    if (std::strftime(mbstr, sizeof(mbstr), "%Y-%m-dT%H:%M:%S.%z%z", &tm))
                    {
                        std::cout <<mbstr << '\n';
                    }
                    //from = std::put_time(&tm, "%Y-%m-dT%H:%M:%S.%z%z");
                    break;
                }

                    //    ACRTLastNMonths,
                    //    ACRTLastNWeeks,
                    //    ACRTLastNDays,
                    //    ACRTLastNHours,
                    //    ACRTLastNMinutes,
                    //    ACRTLastPage,
                    //    ACRTGoToPage,
                    //    ACRTLastNRows,
                     //   ACRTTimeRange,:


                default:
                    break;
            }
            std::string querystr;
            esSearchString(querystr, options.filter.workUnit.str(), m_pluginCfg->queryProp("logmap/workunits/@searchcolumn"), /*m_wuidFieldName.str(),*/ options.logFieldNames, options.range.rangeType == AccessLogRangeType::ACRTAllAvailable ? 100 : 10, 0);

            cpr::Response retrievedDocument = m_esClient.search(m_pluginCfg->queryProp("logmap/workunits/@indexsearchpattern"), DEFAULT_ES_DOC_TYPE, querystr);
            processESJsonResp(retrievedDocument, returnbuf);
            break;
        }
        case ALF_Component:
        {
            DBGLOG("Searching '%s' component log entries...", options.filter.componentName.str());

            std::string querystr;
            esSearchString(querystr, options.filter.componentName.str(), m_pluginCfg->queryProp("logmap/components/@searchcolumn"), options.logFieldNames, options.range.rangeType == AccessLogRangeType::ACRTAllAvailable ? 100 : 10, 0);

            cpr::Response retrievedDocument = m_esClient.search(m_pluginCfg->queryProp("logmap/components/@defaultindexsearchpattern"), DEFAULT_ES_DOC_TYPE, querystr);
            processESJsonResp(retrievedDocument, returnbuf);
            break;
        }
        case ALF_Audience:
        {
            DBGLOG("Searching '%s' audience log entries...", options.filter.targetAudience.item(0));

            std::string querystr;
            esSearchString(querystr, options.filter.workUnit.str(), m_pluginCfg->queryProp("logmap/audiences/@searchcolumn"), options.logFieldNames, options.range.rangeType == AccessLogRangeType::ACRTAllAvailable ? 100 : 10, 0);

            cpr::Response retrievedDocument = m_esClient.search(m_globalIndexSearchPattern, DEFAULT_ES_DOC_TYPE, querystr);
            processESJsonResp(retrievedDocument, returnbuf);
            break;
        }
        default:
            break;
        }
    }
    catch (std::runtime_error &e)
    {
        const char * wha = e.what();
        throw MakeStringException(-1, "ElasticStackLogAccess::fetchLog: Error searching doc: %s", wha);
    }
    catch (IException * e)
    {
        StringBuffer mess;
        e->errorMessage(mess);
        throw MakeStringException(-1, "ElasticStackLogAccess::fetchLog: Error searching doc: %s", mess.str());
    }

    return false;
}

bool ElasticStackLogAccess::fetchWULog(StringBuffer & returnbuf, const char * wu, LogAccessRange range, StringArray & cols)
{
    LogAccessConditions logFetchoptions;
    logFetchoptions.range = range;
    logFetchoptions.filter.filterType = ALF_WorkUnit;
    logFetchoptions.filter.workUnit.set(wu);

    if (cols.ordinality())
    {
        logFetchoptions.copyLogFieldNames(&cols); //ensure these fields are declared in m_logMapping->queryProp("WorkUnits/@contentcolumn")? or in LogMap/Fields?"
    }

    fetchLog(logFetchoptions, returnbuf);
    return true;
}

bool ElasticStackLogAccess::fetchComponentLog(StringBuffer & returnbuf, const char * component, LogAccessRange range, StringArray & cols)
{
    LogAccessConditions logFetchoptions;
    logFetchoptions.range = range;
    logFetchoptions.filter.filterType = ALF_Component;
    logFetchoptions.filter.componentName.set(component);

    if (cols.ordinality())
    {
        logFetchoptions.copyLogFieldNames(&cols); //ensure these fields are declared in m_logMapping->queryProp("WorkUnits/@contentcolumn")? or in LogMap/Fields?"
    }

    fetchLog(logFetchoptions, returnbuf);
    return true;
}

bool ElasticStackLogAccess::fetchLogByAudience(StringBuffer & returnbuf, const char * audience, LogAccessRange range, StringArray & cols)
{
    LogAccessConditions logFetchoptions;
    logFetchoptions.range = range;
    logFetchoptions.filter.filterType = ALF_Audience;
    logFetchoptions.filter.targetAudience.append(audience);

    if (cols.ordinality())
    {
        logFetchoptions.copyLogFieldNames(&cols); //ensure these fields are declared in m_logMapping->queryProp("WorkUnits/@contentcolumn")? or in LogMap/Fields?"
    }

    fetchLog(logFetchoptions, returnbuf);
    return true;
}

//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
extern "C" IRemoteLogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
    //constructing ES Connection string(s) here b/c ES Client explicit ctr requires conn string array

    const char * protocol = logAccessPluginConfig.queryProp("connection/@protocol");
    const char * host = logAccessPluginConfig.queryProp("connection/@host");
    const char * port = logAccessPluginConfig.queryProp("connection/@port");

    std::string elasticSearchConnString;
    elasticSearchConnString = !protocol || !*protocol ? "http" : protocol;
    elasticSearchConnString.append("://");
    elasticSearchConnString.append((!host || !*host) ? "localhost" : host);
    elasticSearchConnString.append(":").append((!port || !*port) ? "9200" : port);
    elasticSearchConnString.append("/"); // required!

    return new ElasticStackLogAccess({elasticSearchConnString}, logAccessPluginConfig);
}
