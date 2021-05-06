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
//#include <cstdio>
#include "platform.h"

#include <string>
#include <vector>
#include <iostream>
#include <json/json.h>

ElasticStackLogAccess::ElasticStackLogAccess(IPropertyTree & logAccessPluginConfig)
{
	//StringBuffer xpath;
	//    xpath.appendf("LogAccess/[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
//	    m_serviceCfg.setown(cfg->getPropTree(xpath.str()));

	/*    Owned<IPropertyTree> testTree = createPTreeFromXMLString(
	                    "<LogAccess name='localES' type='elasticsearch' libName='libelasticstacklogaccess.so'>"
	                    " <Connection protocol='http' host='somehost.somewhere'>"
	    				"   <Credentials secret='somek8ssecretspath'/>"
	    				"   <DefaultIndexName>SomeIndexName</DefaultIndexName>"
	            		" </Connection>"
	            		*/
    //const char * compindex = m_logMap->queryProp("Components/@index");


//	m_pluginCfg.setown(logAccessPluginConfig.getPropTree("LogAccess/[@type=\"elasticsearch\"]"));
    m_pluginCfg.setown(&logAccessPluginConfig);
	const char * protocol = m_pluginCfg->queryProp("Connection/@protocol");
	const char * host = m_pluginCfg->queryProp("Connection/@host");
	const char * port = m_pluginCfg->queryProp("Connection/@port");

	m_elasticSearchConnString.set((!protocol || !*protocol) ? "http" : protocol).append("://");
    m_elasticSearchConnString.append((!host || !*host) ? "localhost" : host);
	m_elasticSearchConnString.append(":").append((!port || !*port) ? "9200" : port);
	m_elasticSearchConnString.append("/"); // required

    m_defaultIndex.set(m_pluginCfg->queryProp("Connection/DefaultIndexName"));
	//m_defaultDocType
	//m_docId

	// Prepare Client for nodes of one Elasticsearch cluster
	//elasticlient::Client client({"http://localhost:9200/"}); // last / is mandatory
    elasticlient::Client client({m_elasticSearchConnString.str()});
    //m_esClient({m_elasticSearchConnString.str()});

    try
    {
    	LOG(MCuserProgress,"ES Log Access: Accessing cluster health: '%s'", m_elasticSearchConnString.str());
    	cpr::Response indexResponse = client.performRequest(Client::HTTPMethod::GET, "_cluster/health", "");
    	LOG(MCuserProgress,"ES Log Access: Cluster health response: '%s'", indexResponse.text.c_str());

    	LOG(MCuserProgress,"ES Log Access: Seeking list of available indexes: '%s'_cat/indices/filebeat-*", m_elasticSearchConnString.str());
    	indexResponse = client.performRequest(Client::HTTPMethod::GET, "_cat/indices/filebeat-*", "");
    	LOG(MCuserProgress,"ES Log Access: List of available indexes: '%s'_cat/indices/%s", indexResponse.text.c_str(), DEFAULT_ES_LOG_INDEX_SEARCH_NAME);
    }
    catch (ConnectionException & ce)//std::runtime_error
    {
    	LOG(MCuserError, "ES Log Access: Encountered error: '%s'", ce.what());
    }

    //std::string document {"{\"container.image.name\": \"hpccsystems\\core\", \"kubernetes.container.name\": \"eclwatch\", \"message\": \"00000041 USR 2021-05-05 17:55:03.316 975333 975333 \"CSmartSocketFactory::CSmartSocketFactory(192.168.1.140:9876)\"\"}"};
    std::string document {"{\"container.image.name\": \"hpccsystems\\\\core\", \"message\": \"00000041 USR 2021-05-05 17:55:03.316 975333 975333 \\\"CSmartSocketFactory::CSmartSocketFactory(192.168.1.140:9876)\\\"\"}"};
    //std::string document {"{\"message\": \"00000999 PRO 2021-06-06 17:55:03.316 975333 975333 \\\"Something logged from HPCC\\\"\"}"};
    std::string targetindex {"filebeat-7.9.3-2021.04.23-000003"};

    try
    {
    	// Index the document, index "testindex" must be created before
    	cpr::Response indexResponse = client.index(targetindex, DEFAULT_ES_DOC_TYPE, "", document);
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
    	std::cout << "Searching '975333' " << std::endl;
    	cpr::Response retrievedDocument = client.search(targetindex, DEFAULT_ES_DOC_TYPE, "{\"query\":{ \"match\":{ \"phrase\": { \"query\" : \"00000041\" } } } }");

		// 200
		//std::cout << retrievedDocument.status_code << std::endl;
		// application/json; charset=UTF-8
		//std::cout << retrievedDocument.header["content-type"] << std::endl;
		// Elasticsearch response (JSON text string) where key "_source" contain:
		// {"message": "Hello world!"}
		std::cout << retrievedDocument.text << std::endl;

		Json::Value root;
		Json::Reader reader;
		if (!reader.parse(retrievedDocument.text, root, false))
		{
			// probably whole bulk has failed
			//errCount += size;
			return;
		}
		const Json::Value &items = root["_id"];
	}
	catch (std::runtime_error &e)
	{
		const char * wha = e.what();
		fprintf(stdout, "Error searching doc: %s", wha);
	}
	catch (IException * e)
	{
		StringBuffer mess;
		const char * message = e->errorMessage(mess);
		fprintf(stdout, "Error searching doc: %s", mess.str());
	}

    try
    {
	    // Retrieve the document
	    cpr::Response retrievedDocument = client.get(targetindex, DEFAULT_ES_DOC_TYPE, "");
	    // 200
	    std::cout << retrievedDocument.status_code << std::endl;
	    // application/json; charset=UTF-8
	    std::cout << retrievedDocument.header["content-type"] << std::endl;
	    // Elasticsearch response (JSON text string) where key "_source" contain:
	    // {"message": "Hello world!"}
	    Owned<IPropertyTree> resptree = createPTreeFromJSONString(retrievedDocument.text.c_str());
	    std::cout << retrievedDocument.text << std::endl;

	    Json::Value root;
	        Json::Reader reader;
	        if (!reader.parse(retrievedDocument.text, root, false))
	        {
	            // probably whole bulk has failed
	            //errCount += size;
	            return;
	        }
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

	try
	{
	    // Remove the document
	    cpr::Response removedDocument = client.remove(targetindex, DEFAULT_ES_DOC_TYPE, "");
	    // 200
	    std::cout << removedDocument.status_code << std::endl;
	    // application/json; charset=UTF-8
	    std::cout << removedDocument.header["content-type"] << std::endl;
	}
    catch (std::runtime_error &e)
	{
		const char * wha = e.what();
		fprintf(stdout, "Error removing doc: %s", wha);
	}
	catch (IException * e)
	{
		StringBuffer mess;
		const char * message = e->errorMessage(mess);
		fprintf(stdout, "Error removing doc: %s", mess.str());
	}
}

/*void ElasticStackLogAccess::init(const char * connectstring, const char * host, const char * port, const char * defaultIndex, const char * defDocType)
{


//    if (!defaultIndex || !*defaultIndex)
//    	Log("ElasticStackLogAccess: no default index defined");

    m_defaultIndex = defaultIndex;
	m_defaultDocType = defDocType;
//	m_docId; ??


}*/

bool ElasticStackLogAccess::fetchLog(LogAccessConditions options, StringBuffer & returnbuf)
{
	switch (options.filter.filterType)
	{
		case ALF_WorkUnit:
			returnbuf.set("Searching for WU Logs ").append(options.filter.workUnit.str());
			return true;
			break;
		default:
			break;
	}

	return false;
}

bool ElasticStackLogAccess::fetchWULog(StringBuffer & returnbuf, const char * wu, LogAccessRange range, StringArray * cols)
{
	LogAccessConditions logFetchoptions;
	logFetchoptions.range = range;
	logFetchoptions.filter.filterType = ALF_WorkUnit;
	logFetchoptions.filter.workUnit.set(wu);

	fetchLog(logFetchoptions, returnbuf);
	return true;
}

bool ElasticStackLogAccess::fetchComponentLog(const char * component, LogAccessRange range, StringBuffer & returnbuf)
{
	return false;
}
bool ElasticStackLogAccess::fetchLogByAudience(const char * audience, LogAccessRange range, StringBuffer & returnbuf)
{
	return false;
}

//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
extern "C" ILogAccess * createInstance(IPropertyTree & logAccessPluginConfig)
{
	return new ElasticStackLogAccess(logAccessPluginConfig);
}
