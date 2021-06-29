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

class ELASTICSTACKLOGACCESS_API ElasticStackLogAccess : public CInterface, implements IRemoteLogAccess
{
private:
	const char * type = "elasticstack";

	Owned<IPropertyTree> m_pluginCfg;

	std::string m_globalIndexSearchPattern;
	StringBuffer m_globalSearchColName;
	StringBuffer m_defaultDocType; //default doc type to query

	elasticlient::Client m_esClient;
public:
    IMPLEMENT_IINTERFACE

	ElasticStackLogAccess(const std::vector<std::string> &hostUrlList, IPropertyTree & logAccessPluginConfig);
    virtual ~ElasticStackLogAccess() override = default;

    virtual bool fetchLog(LogAccessConditions & options, StringBuffer & returnbuf);
    virtual bool fetchWULog(StringBuffer & returnbuf, const char * wu, LogAccessRange range, StringArray & cols);
    virtual bool fetchComponentLog(StringBuffer & returnbuf, const char * component, LogAccessRange range, StringArray & cols);
    virtual bool fetchLogByAudience(StringBuffer & returnbuf, const char * audience, LogAccessRange range, StringArray & cols);
    virtual IPropertyTree * getESStatus();
    IPropertyTree * getIndexSearchStatus();

    virtual IPropertyTree * getRemoteLogStoreStatus() { return getESStatus();};
    virtual const char * getRemoteLogAccessType() { return type; }
    virtual IPropertyTree * fetchLogMap() { return m_pluginCfg->queryPropTree("logmap");}
};

//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance(IPropertyTree & logAccessPluginConfig);
//extern "C" ELASTICSTACKLOGACCESS_API ILogAccess * createInstance();
