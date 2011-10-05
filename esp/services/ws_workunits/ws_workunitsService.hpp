/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

    bool onWUQuery(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    bool onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp);
    bool onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp);
    bool onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp);
    bool onWUQuerysetActionQueries(IEspContext &context, IEspWUQuerySetActionQueriesRequest & req, IEspWUQuerySetActionQueriesResponse & resp);
    bool onWUQuerysetActionAliases(IEspContext &context, IEspWUQuerySetActionAliasesRequest & req, IEspWUQuerySetActionAliasesResponse & resp);
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

    void setPort(unsigned short _port){port=_port;}

private:
    unsigned awusCacheMinutes;

    Owned<DataCache> dataCache;
    Owned<ArchivedWuCache> archivedWuCache;
    WUSchedule m_sched;
    unsigned short port;
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
            IPropertyTree *folder = ensureNavFolder(data, "ECL Workunits", "ECL Workunits", NULL, false, 2);
            ensureNavLink(*folder, "Search", "/WsWorkunits/WUQuery?form_", "Search Workunits", NULL, NULL, 1);
            ensureNavLink(*folder, "Browse", "/WsWorkunits/WUQuery", "Browse Workunits", NULL, NULL, 2);

            IPropertyTree *folderQueryset = ensureNavFolder(data, "Query Sets", "Queryset Management", NULL, false, 3);
            ensureNavLink(*folderQueryset, "Browse", "/WsWorkunits/WUQuerySets", "Browse Querysets");
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
