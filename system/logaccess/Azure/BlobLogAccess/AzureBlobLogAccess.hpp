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
//#include "jfile.ipp"


#ifndef AZUREBLOB_LOGACCESS_EXPORTS
#define AZUREBLOB_LOGACCESS_API DECL_IMPORT
#else
#define AZUREBLOB_LOGACCESS_API DECL_EXPORT
#endif

#define COMPONENT_NAME "Azure Blob Log Access"

/* undef verify definitions to avoid collision in cpr submodule */
//#ifdef verify
    //#pragma message("UNDEFINING 'verify' - Will be redefined by cpr" )
//    #undef verify
//#endif

//#include <cpr/response.h>
//#include <elasticlient/client.h>
//#include <elasticlient/scroll.h>

//using namespace elasticlient;

class AZUREBLOB_LOGACCESS_API AzureBlobLogAccess : public CInterfaceOf<IRemoteLogAccess>
{
private:
    static constexpr const char * type = "azureblob";

    Owned<IPropertyTree> m_pluginCfg;

    StringBuffer m_globalIndexSearchPattern;
    StringBuffer m_globalSearchColName;
    StringBuffer m_globalIndexTimestampField;

    StringBuffer m_workunitSearchColName;
    StringBuffer m_workunitIndexSearchPattern;

    StringBuffer m_componentsSearchColName;
    StringBuffer m_componentsIndexSearchPattern;

    StringBuffer m_audienceSearchColName;
    StringBuffer m_audienceIndexSearchPattern;

    StringBuffer m_classSearchColName;
    StringBuffer m_classIndexSearchPattern;

    StringBuffer m_instanceSearchColName;
    StringBuffer m_instanceIndexSearchPattern;

    StringBuffer m_hostSearchColName;
    StringBuffer m_hostIndexSearchPattern;

    StringBuffer m_esConnectionStr;

    void getMinReturnColumns(std::string & columns);
    void getDefaultReturnColumns(std::string & columns);
    void getAllColumns(std::string & columns);
//    void populateESQueryQueryString(std::string & queryString, std::string & queryIndex, const ILogAccessFilter * filter);
public:
    AzureBlobLogAccess(const std::vector<std::string> &hostUrlList, IPropertyTree & logAccessPluginConfig);
    virtual ~AzureBlobLogAccess() override = default;

    //void populateQueryStringAndQueryIndex(std::string & queryString, std::string & queryIndex, const LogAccessConditions & options);

    // IRemoteLogAccess methods
    virtual bool fetchLog(LogQueryResultDetails & resultDetails, const LogAccessConditions & options, StringBuffer & returnbuf, LogAccessLogFormat format) override;
    virtual const char * getRemoteLogAccessType() const override { return type; }
    virtual IPropertyTree * queryLogMap() const override { return m_pluginCfg->queryPropTree("logmap");}
    virtual const char * fetchConnectionStr() const override { return m_esConnectionStr.str();}
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format) override;
    virtual IRemoteLogAccessStream * getLogReader(const LogAccessConditions & options, LogAccessLogFormat format, unsigned int pageSize) override;
};
