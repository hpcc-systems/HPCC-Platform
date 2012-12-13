/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef _ESPWIZ_ws_workunits_HPP__
#define _ESPWIZ_ws_workunits_HPP__

#include "ws_workunits_esp.ipp"
#include "workunit.hpp"
#include "ws_workunitsHelpers.hpp"

class CWsWorkunitsEx : public CWsWorkunits
{
public:
    IMPLEMENT_IINTERFACE;

    CWsWorkunitsEx(){port=8010;}

    virtual ~CWsWorkunitsEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual void setContainer(IEspContainer * container)
    {
        CWsWorkunits::setContainer(container);
        m_sched.setContainer(container);
    }
    void refreshValidClusters();
    bool isValidCluster(const char *cluster);
    void deploySharedObject(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp, const char *dir, const char *xml=NULL);
    void deploySharedObject(IEspContext &context, StringBuffer &wuid, const char *filename, const char *cluster, const char *name, const MemoryBuffer &obj, const char *dir, const char *xml=NULL);
    void checkAndTrimWorkunit(const char* methodName, StringBuffer& input);

    bool onWUQuery(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    bool onWUPublishWorkunit(IEspContext &context, IEspWUPublishWorkunitRequest & req, IEspWUPublishWorkunitResponse & resp);
    bool onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp);
    bool onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp);
    bool onWUMultiQuerysetDetails(IEspContext &context, IEspWUMultiQuerySetDetailsRequest &req, IEspWUMultiQuerySetDetailsResponse &resp);
    bool onWUQuerysetQueryAction(IEspContext &context, IEspWUQuerySetQueryActionRequest & req, IEspWUQuerySetQueryActionResponse & resp);
    bool onWUQuerysetAliasAction(IEspContext &context, IEspWUQuerySetAliasActionRequest &req, IEspWUQuerySetAliasActionResponse &resp);
    bool onWUQueryConfig(IEspContext &context, IEspWUQueryConfigRequest &req, IEspWUQueryConfigResponse &resp);
    bool onWUQuerysetCopyQuery(IEspContext &context, IEspWUQuerySetCopyQueryRequest &req, IEspWUQuerySetCopyQueryResponse &resp);
    bool onWUCopyLogicalFiles(IEspContext &context, IEspWUCopyLogicalFilesRequest &req, IEspWUCopyLogicalFilesResponse &resp);

    bool onWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp);
    bool onWUInfoDetails(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp);
    bool onWUFile(IEspContext &context,IEspWULogFileRequest &req, IEspWULogFileResponse &resp);
    bool onWUResult(IEspContext &context,IEspWUResultRequest &req, IEspWUResultResponse &resp);
    bool onWUResultView(IEspContext &context, IEspWUResultViewRequest &req, IEspWUResultViewResponse &resp);
    bool onWUResultSummary(IEspContext &context, IEspWUResultSummaryRequest &req, IEspWUResultSummaryResponse &resp);
    bool onWUResultBin(IEspContext &context, IEspWUResultBinRequest &req, IEspWUResultBinResponse &resp);
    bool onWUGraphInfo(IEspContext &context,IEspWUGraphInfoRequest &req, IEspWUGraphInfoResponse &resp);
    bool onWUGVCGraphInfo(IEspContext &context,IEspWUGVCGraphInfoRequest &req, IEspWUGVCGraphInfoResponse &resp);
    bool onWUProcessGraph(IEspContext &context,IEspWUProcessGraphRequest &req, IEspWUProcessGraphResponse &resp);
    bool onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp);

    bool onWUAction(IEspContext &context, IEspWUActionRequest &req, IEspWUActionResponse &resp);
    bool onWUShowScheduled(IEspContext &context, IEspWUShowScheduledRequest &req, IEspWUShowScheduledResponse &resp);

    bool onWUUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp);
    bool onWUDelete(IEspContext &context, IEspWUDeleteRequest &req, IEspWUDeleteResponse &resp);
    bool onWUProtect(IEspContext &context, IEspWUProtectRequest &req, IEspWUProtectResponse &resp);

    bool onWUAbort(IEspContext &context, IEspWUAbortRequest &req, IEspWUAbortResponse &resp);
    bool onWUSchedule(IEspContext &context, IEspWUScheduleRequest &req, IEspWUScheduleResponse &resp);
    bool onWUSubmit(IEspContext &context, IEspWUSubmitRequest &req, IEspWUSubmitResponse &resp);
    bool onWURun(IEspContext &context, IEspWURunRequest &req, IEspWURunResponse &resp);
    bool onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp);
    bool onWUCreateAndUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp);
    bool onWUResubmit(IEspContext &context, IEspWUResubmitRequest &req, IEspWUResubmitResponse &resp);
    bool onWUPushEvent(IEspContext &context, IEspWUPushEventRequest &req, IEspWUPushEventResponse &resp);

    bool onWUExport(IEspContext &context, IEspWUExportRequest &req, IEspWUExportResponse &resp);
    bool onWUWaitCompiled(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp);
    bool onWUWaitComplete(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp);

    bool onWUSyntaxCheckECL(IEspContext &context, IEspWUSyntaxCheckRequest &req, IEspWUSyntaxCheckResponse &resp);
    bool onWUCompileECL(IEspContext &context, IEspWUCompileECLRequest &req, IEspWUCompileECLResponse &resp);
    bool onWUJobList(IEspContext &context, IEspWUJobListRequest &req, IEspWUJobListResponse &resp);
    bool onWUGetGraph(IEspContext& context, IEspWUGetGraphRequest& req, IEspWUGetGraphResponse& resp);
    bool onWUGraphTiming(IEspContext& context, IEspWUGraphTimingRequest& req, IEspWUGraphTimingResponse& resp);
    bool onWUGetDependancyTrees(IEspContext& context, IEspWUGetDependancyTreesRequest& req, IEspWUGetDependancyTreesResponse& resp);

    bool onWUListLocalFileRequired(IEspContext& context, IEspWUListLocalFileRequiredRequest& req, IEspWUListLocalFileRequiredResponse& resp);
    bool onWUAddLocalFileToWorkunit(IEspContext& context, IEspWUAddLocalFileToWorkunitRequest& req, IEspWUAddLocalFileToWorkunitResponse& resp);

    bool onWUClusterJobQueueXLS(IEspContext &context, IEspWUClusterJobQueueXLSRequest &req, IEspWUClusterJobQueueXLSResponse &resp);
    bool onWUClusterJobQueueLOG(IEspContext &context,IEspWUClusterJobQueueLOGRequest  &req, IEspWUClusterJobQueueLOGResponse &resp);
    bool onWUClusterJobXLS(IEspContext &context, IEspWUClusterJobXLSRequest &req, IEspWUClusterJobXLSResponse &resp);
    bool onWUClusterJobSummaryXLS(IEspContext &context, IEspWUClusterJobSummaryXLSRequest &req, IEspWUClusterJobSummaryXLSResponse &resp);

    bool onWUCDebug(IEspContext &context, IEspWUDebugRequest &req, IEspWUDebugResponse &resp);
    bool onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp);

    void setPort(unsigned short _port){port=_port;}

    bool isQuerySuspended(const char* query, IConstWUClusterInfo *clusterInfo, unsigned wait, StringBuffer& errorMessage);

private:
    unsigned awusCacheMinutes;
    StringBuffer queryDirectory;
    StringAttr daliServers;
    Owned<DataCache> dataCache;
    Owned<ArchivedWuCache> archivedWuCache;
    BoolHash validClusters;
    CriticalSection crit;
    WUSchedule m_sched;
    unsigned short port;
    Owned<IPropertyTree> directories;
};

class CWsWorkunitsSoapBindingEx : public CWsWorkunitsSoapBinding
{
public:
    CWsWorkunitsSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel) : CWsWorkunitsSoapBinding(cfg, name, process, llevel)
    {
        VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]/BatchWatch", process, name);
        batchWatchFeaturesOnly = cfg->getPropBool(xpath.str(), false);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (!batchWatchFeaturesOnly)
        {
            IPropertyTree *folder = ensureNavFolder(data, "ECL", "Run Ecl code and review Ecl workunits", NULL, false, 2);
            ensureNavLink(*folder, "Search Workunits", "/WsWorkunits/WUQuery?form_", "Search Workunits", NULL, NULL, 1);
            ensureNavLink(*folder, "Browse Workunits", "/esp/files/stub.htm?Widget=WUQueryWidget", "Browse Workunits", NULL, NULL, 2);
            ensureNavLink(*folder, "ECL Playground", "/esp/files/stub.htm?Widget=ECLPlaygroundWidget", "ECL Editor, Executor, Graph and Result Viewer", NULL, NULL, 4);

            IPropertyTree *folderQueryset = ensureNavFolder(data, "Queries", NULL, NULL, false, 3);
            ensureNavLink(*folderQueryset, "Browse", "/WsWorkunits/WUQuerySets", "Browse Published Queries");
        }
    }

    int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    int onGet(CHttpRequest* request, CHttpResponse* response);

    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service)
    {
        CWsWorkunitsEx* srv = dynamic_cast<CWsWorkunitsEx*>(&service);
        if (srv)
            srv->setPort(port);
        CWsWorkunitsSoapBinding::addService(name, host, port, service);
    }


private:
    bool batchWatchFeaturesOnly;
};

#endif
