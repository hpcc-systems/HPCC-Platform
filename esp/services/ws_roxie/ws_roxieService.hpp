/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _ESPWIZ_ws_roxie_HPP__
#define _ESPWIZ_ws_roxie_HPP__

#include "ws_roxie_esp.ipp"
#include "roxieconfig.hpp"

class CRoxieEx : public CRoxie
{
public:
   IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

/*
    bool onIndex(IEspContext &context, IEspIndexRequest &req, IEspIndexResponse &resp);
    bool onContent(IEspContext &context, IEspContentRequest &req, IEspContentResponse &resp);

    bool onGetQueryList(IEspContext &context, IEspGetQueryListRequest &req, IEspGetQueryListResponse &resp);
    bool onDebugXML(IEspContext &context, IEspDebugXMLRequest &req, IEspDebugXMLResponse &resp);
    bool onAddQuery(IEspContext &context, IEspAddQueryRequest &req, IEspAddQueryResponse &resp);
    bool onAddAttribute(IEspContext &context, IEspAddAttributeRequest &req, IEspAddAttributeResponse &resp);
    bool onRemoveQuery(IEspContext &context, IEspRemoveQueryRequest &req, IEspRemoveQueryResponse &resp);
    bool onPublishQuery(IEspContext &context, IEspPublishQueryRequest &req, IEspPublishQueryResponse &resp);
    bool onPublishConfiguration(IEspContext &context, IEspPublishConfigurationRequest &req, IEspPublishConfigurationResponse &resp);
    bool onTestQuery(IEspContext &context, IEspTestQueryRequest &req, IEspTestQueryResponse &resp);
    bool onRemoveDll(IEspContext &context, IEspRemoveDllRequest &req, IEspRemoveDllResponse &resp);
    
    bool onSetActiveVersion(IEspContext &context, IEspSetActiveVersionRequest &req, IEspSetActiveVersionResponse &resp);
    bool onGetActiveVersion(IEspContext &context, IEspGetActiveVersionRequest &req, IEspGetActiveVersionResponse &resp);
    bool onGetQueryVersionList(IEspContext &context, IEspGetQueryVersionListRequest &req, IEspGetQueryVersionListResponse &resp);
    bool onGetResources(IEspContext &context, IEspGetResourcesRequest &req, IEspGetResourcesResponse &resp);
    bool onAppendQuery(IEspContext &context, IEspAppendQueryRequest &req, IEspAppendQueryResponse &resp);
    bool onRealignQuery(IEspContext &context, IEspRealignQueryRequest &req, IEspRealignQueryResponse &resp);

    bool onGetFileList(IEspContext &context, IEspGetFileListRequest &req, IEspGetFileListResponse &resp);
    bool onGetFileVersionList(IEspContext &context, IEspGetFileVersionListRequest &req, IEspGetFileVersionListResponse &resp);
    bool onAppendFile(IEspContext &context, IEspAppendFileRequest &req, IEspAppendFileResponse &resp);
    bool onRemoveFile(IEspContext &context, IEspRemoveFileRequest &req, IEspRemoveFileResponse &resp);
    bool onSetRedundancy(IEspContext &context, IEspSetRedundancyRequest &req, IEspSetRedundancyResponse &resp);
    bool onGetRedundancy(IEspContext &context, IEspGetRedundancyRequest &req, IEspGetRedundancyResponse &resp);
    void GenerateCurrentXml(const char * localXml, const char * statusXml, StringBuffer &resp);
*/

protected:
    StringBuffer daliServers_;
    StringBuffer queueServer_;
    StringBuffer eclServer_;
    StringBuffer roxieServer_;
    RoxieConfig *rc_;
};

#endif //_ESPWIZ_ws_roxie_HPP__

