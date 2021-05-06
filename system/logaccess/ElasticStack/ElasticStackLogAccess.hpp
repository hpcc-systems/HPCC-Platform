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


#pragma once

#include "jlog.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "jfile.ipp"


#ifndef ELASTICSTACKLOGACCESS_EXPORTS
#define ELASTICSTACKLOGACCESS_API DECL_IMPORT
#else
#define ELASTICSTACKLOGACCESS_API DECL_EXPORT
#endif

#define DEFAULT_ES_DOC_TYPE "_doc"
#define DEFAULT_ES_LOG_INDEX_SEARCH_NAME "filebeat-*"


/* undef verify definitions to avoid collision in cpr submodule */
#ifdef verify
    //#pragma message("UNDEFINING 'verify' - Will be redefined by cpr" )
    #undef verify
#endif

#include <cpr/response.h>
#include <elasticlient/client.h>

using namespace elasticlient;

class ELASTICSTACKLOGACCESS_API ElasticStackLogAccess : public CInterface, implements ILogAccess
{
private:
	Owned<IPropertyTree> m_pluginCfg;
	StringBuffer m_elasticSearchConnString; //	"http://elastic1.host:9200/"}, 6000,
	StringBuffer m_defaultIndex; //default index to query
	StringBuffer m_defaultDocType; //default doc type to query
	StringBuffer m_docId;

	//elasticlient::Client m_esClient;
public:
    IMPLEMENT_IINTERFACE
	ElasticStackLogAccess(IPropertyTree & logAccessPluginConfig);
    ElasticStackLogAccess(const char * protocol, const char * host, const char * port, const char * defaultIndex);
    virtual ~ElasticStackLogAccess() override = default;

    virtual bool fetchLog(LogAccessConditions options, StringBuffer & returnbuf);
    //virtual bool fetchWULog(const char * wu, LogAccessRange range, StringBuffer & returnbuf);
    virtual bool fetchWULog(StringBuffer & returnbuf, const char * wu, LogAccessRange range, StringArray * cols);
    virtual bool fetchComponentLog(const char * component, LogAccessRange range, StringBuffer & returnbuf);
    virtual bool fetchLogByAudience(const char * audience, LogAccessRange range, StringBuffer & returnbuf);
};

//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance(IPropertyTree & logAccessPluginConfig);
//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance();
