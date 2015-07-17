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
#include "dasds.hpp"
#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

#define UFO_DIRTY                                0x01
#define UFO_RELOAD_TARGETS_CHANGED_PMID          0x02
#define UFO_RELOAD_MAPPED_QUERIES                0x04
#define UFO_REMOVE_QUERIES_NOT_IN_QUERYSET       0x08

class QueryFilesInUse : public CInterface, implements ISDSSubscription
{
    mutable CriticalSection crit;
    MapStringTo<IUserDescriptor *> roxieUserMap;
    IArrayOf<IUserDescriptor> roxieUsers;

    Owned<IPropertyTree> tree;
    SubscriptionId qsChange;
    SubscriptionId pmChange;
    SubscriptionId psChange;
    mutable CriticalSection dirtyCrit; //if there were an atomic_or I would just use atomic
    unsigned dirty;
    bool aborting;
private:
    void loadTarget(IPropertyTree *tree, const char *target, unsigned flags);
    void loadTargets(IPropertyTree *tree, unsigned flags);
    void load(unsigned flags)
    {
        Owned<IPropertyTree> t = createPTreeFromIPT(tree);
        loadTargets(t, flags);
        tree.setown(t.getClear());
    }

    void updateUsers()
    {
        Owned<IStringIterator> clusters = getTargetClusters("RoxieCluster", NULL);
        ForEach(*clusters)
        {
            SCMStringBuffer target;
            clusters->str(target);

            Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target.str());
            Owned<IUserDescriptor> user = createUserDescriptor();
            user->set(info->getLdapUser(), info->getLdapPassword());
            roxieUserMap.setValue(target.str(), user);
            roxieUsers.append(*user.getClear());
        }
    }

public:
    IMPLEMENT_IINTERFACE;
    QueryFilesInUse() : aborting(false), qsChange(0), pmChange(0), psChange(0), dirty(UFO_DIRTY)
    {
        tree.setown(createPTree("QueryFilesInUse"));
        updateUsers();
    }

    virtual void notify(SubscriptionId subid, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        Linked<QueryFilesInUse> me = this;  // Ensure that I am not released by the notify call (which would then access freed memory to release the critsec)
        CriticalBlock b(dirtyCrit);
        if (subid == qsChange)
            dirty |= UFO_REMOVE_QUERIES_NOT_IN_QUERYSET;
        else if (subid == pmChange)
            dirty |= UFO_RELOAD_MAPPED_QUERIES;
        else if (subid == psChange)
            dirty |= UFO_RELOAD_TARGETS_CHANGED_PMID;
    }
    virtual void subscribe()
    {
        CriticalBlock b(crit);
        try
        {
            qsChange = querySDS().subscribe("QuerySets", *this, true);
            pmChange = querySDS().subscribe("PackageMaps", *this, true);
            psChange = querySDS().subscribe("PackageSets", *this, true);
        }
        catch (IException *E)
        {
            //TBD failure to subscribe implies dali is down...
            E->Release();
        }
    }
    virtual void unsubscribe()
    {
        CriticalBlock b(crit);
        try
        {
            if (qsChange)
                querySDS().unsubscribe(qsChange);
            if (pmChange)
                querySDS().unsubscribe(pmChange);
            if (psChange)
                querySDS().unsubscribe(psChange);
        }
        catch (IException *E)
        {
            E->Release();
        }
        qsChange = 0;
        pmChange = 0;
        psChange = 0;
    }

    void abort()
    {
        aborting=true;
        CriticalBlock b(crit);
    }
    IPropertyTree *getTree()
    {
        CriticalBlock b(crit);
        unsigned flags;
        {
            CriticalBlock b(dirtyCrit);
            flags = dirty;
            dirty = 0;
        }
        if (flags)
            load(flags);
        return LINK(tree);
    }

    IPropertyTreeIterator *findAllQueriesUsingFile(const char *lfn);
    IPropertyTreeIterator *findQueriesUsingFile(const char *target, const char *lfn, StringAttr &pmid);
    StringBuffer &toStr(StringBuffer &s)
    {
        Owned<IPropertyTree> t = getTree();
        return toXML(t, s);
    }
};

class CWsWorkunitsEx : public CWsWorkunits
{
public:
    IMPLEMENT_IINTERFACE;

    CWsWorkunitsEx() : maxRequestEntityLength(0) {port=8010;}

    virtual ~CWsWorkunitsEx()
    {
        filesInUse.unsubscribe();
        filesInUse.abort();
        clusterQueryStatePool.clear();
    };
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual void setContainer(IEspContainer * container)
    {
        CWsWorkunits::setContainer(container);
        m_sched.setContainer(container);
    }
    void refreshValidClusters();
    bool isValidCluster(const char *cluster);
    void deploySharedObjectReq(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp, const char *dir, const char *xml=NULL);
    unsigned getGraphIdsByQueryId(const char *target, const char *queryId, StringArray& graphIds);
    bool getQueryFiles(const char* query, const char* target, StringArray& logicalFiles, IArrayOf<IEspQuerySuperFile> *superFiles);
    void getGraphsByQueryId(const char *target, const char *queryId, const char *graphName, const char *subGraphId, IArrayOf<IEspECLGraphEx>& ECLGraphs);
    void checkAndSetClusterQueryState(IEspContext &context, const char* cluster, const char* querySetId, IArrayOf<IEspQuerySetQuery>& queries);
    void checkAndSetClusterQueryState(IEspContext &context, const char* cluster, StringArray& querySetIds, IArrayOf<IEspQuerySetQuery>& queries);

    bool onWUQuery(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    bool onWUPublishWorkunit(IEspContext &context, IEspWUPublishWorkunitRequest & req, IEspWUPublishWorkunitResponse & resp);
    bool onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp);
    bool onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp);
    bool onWUMultiQuerysetDetails(IEspContext &context, IEspWUMultiQuerySetDetailsRequest &req, IEspWUMultiQuerySetDetailsResponse &resp);
    bool onWUQuerysetQueryAction(IEspContext &context, IEspWUQuerySetQueryActionRequest & req, IEspWUQuerySetQueryActionResponse & resp);
    bool onWUQuerysetAliasAction(IEspContext &context, IEspWUQuerySetAliasActionRequest &req, IEspWUQuerySetAliasActionResponse &resp);
    bool onWUQueryConfig(IEspContext &context, IEspWUQueryConfigRequest &req, IEspWUQueryConfigResponse &resp);
    bool onWUQuerysetCopyQuery(IEspContext &context, IEspWUQuerySetCopyQueryRequest &req, IEspWUQuerySetCopyQueryResponse &resp);
    bool onWUCopyQuerySet(IEspContext &context, IEspWUCopyQuerySetRequest &req, IEspWUCopyQuerySetResponse &resp);
    bool onWUCopyLogicalFiles(IEspContext &context, IEspWUCopyLogicalFilesRequest &req, IEspWUCopyLogicalFilesResponse &resp);
    bool onWUQueryDetails(IEspContext &context, IEspWUQueryDetailsRequest & req, IEspWUQueryDetailsResponse & resp);
    bool onWUListQueries(IEspContext &context, IEspWUListQueriesRequest &req, IEspWUListQueriesResponse &resp);
    bool onWUListQueriesUsingFile(IEspContext &context, IEspWUListQueriesUsingFileRequest &req, IEspWUListQueriesUsingFileResponse &resp);
    bool onWUQueryFiles(IEspContext &context, IEspWUQueryFilesRequest &req, IEspWUQueryFilesResponse &resp);

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
    bool onWUQueryGetGraph(IEspContext& context, IEspWUQueryGetGraphRequest& req, IEspWUQueryGetGraphResponse& resp);
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
    bool onWUCreateZAPInfo(IEspContext &context, IEspWUCreateZAPInfoRequest &req, IEspWUCreateZAPInfoResponse &resp);
    bool onWUGetZAPInfo(IEspContext &context, IEspWUGetZAPInfoRequest &req, IEspWUGetZAPInfoResponse &resp);
    bool onWUCheckFeatures(IEspContext &context, IEspWUCheckFeaturesRequest &req, IEspWUCheckFeaturesResponse &resp);
    bool onWUGetStats(IEspContext &context, IEspWUGetStatsRequest &req, IEspWUGetStatsResponse &resp);

    bool onWUListArchiveFiles(IEspContext &context, IEspWUListArchiveFilesRequest &req, IEspWUListArchiveFilesResponse &resp);
    bool onWUGetArchiveFile(IEspContext &context, IEspWUGetArchiveFileRequest &req, IEspWUGetArchiveFileResponse &resp);
private:
    void addProcessLogfile(Owned<IConstWorkUnit> &cwu, WsWuInfo &winfo, const char * process, const char* path);
    void createZAPWUInfoFile(IEspWUCreateZAPInfoRequest &req, Owned<IConstWorkUnit>& cwu, const char* pathNameStr);
    void createZAPWUXMLFile(WsWuInfo &winfo, const char* pathNameStr);
    void createZAPECLQueryArchiveFiles(Owned<IConstWorkUnit>& cwu, const char* pathNameStr);
    void createZAPFile(const char* fileName, size32_t len, const void* data);
    void cleanZAPFolder(IFile* zipDir, bool removeFolder);
    IPropertyTree* sendControlQuery(IEspContext &context, const char* target, const char* query, unsigned timeout);
    bool resetQueryStats(IEspContext &context, const char* target, IProperties* queryIds, IEspWUQuerySetQueryActionResponse& resp);
    void readGraph(IEspContext& context, const char* subGraphId, WUGraphIDType& id, bool running,
        IConstWUGraph* graph, IArrayOf<IEspECLGraphEx>& graphs);
    IPropertyTree* getWorkunitArchive(IEspContext &context, WsWuInfo& winfo, const char* wuid, unsigned cacheMinutes);

    unsigned awusCacheMinutes;
    StringBuffer queryDirectory;
    StringBuffer envLocalAddress;
    StringAttr daliServers;
    Owned<DataCache> dataCache;
    Owned<ArchivedWuCache> archivedWuCache;
    Owned<WUArchiveCache> wuArchiveCache;
    StringAttr sashaServerIp;
    unsigned short sashaServerPort;
    BoolHash validClusters;
    CriticalSection crit;
    WUSchedule m_sched;
    unsigned short port;
    Owned<IPropertyTree> directories;
    int maxRequestEntityLength;
    Owned<IThreadPool> clusterQueryStatePool;
public:
    QueryFilesInUse filesInUse;
};

class CWsWorkunitsSoapBindingEx : public CWsWorkunitsSoapBinding
{
public:
    CWsWorkunitsSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel) : CWsWorkunitsSoapBinding(cfg, name, process, llevel)
    {
        wswService = NULL;
        VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]/BatchWatch", process, name);
        batchWatchFeaturesOnly = cfg->getPropBool(xpath.str(), false);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (!batchWatchFeaturesOnly)
        {
            IPropertyTree *folder = ensureNavFolder(data, "ECL", "Run Ecl code and review Ecl workunits", NULL, false, 2);
            ensureNavLink(*folder, "Search Workunits", "/WsWorkunits/WUQuery?form_", "Search Workunits", NULL, NULL, 1);
            ensureNavLink(*folder, "Browse Workunits", "/WsWorkunits/WUQuery", "Browse Workunits", NULL, NULL, 2);
            ensureNavLink(*folder, "ECL Playground", "/esp/files/stub.htm?Widget=ECLPlaygroundWidget", "ECL Editor, Executor, Graph and Result Viewer", NULL, NULL, 4);

            IPropertyTree *folderQueryset = ensureNavFolder(data, "Queries", NULL, NULL, false, 3);
            ensureNavLink(*folderQueryset, "Browse", "/WsWorkunits/WUQuerySets", "Browse Published Queries");
        }
    }

    int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    int onGet(CHttpRequest* request, CHttpResponse* response);

    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service)
    {
        wswService = dynamic_cast<CWsWorkunitsEx*>(&service);
        if (wswService)
            wswService->setPort(port);
        CWsWorkunitsSoapBinding::addService(name, host, port, service);
    }

private:
    bool batchWatchFeaturesOnly;
    CWsWorkunitsEx *wswService;
};

void deploySharedObject(IEspContext &context, StringBuffer &wuid, const char *filename, const char *cluster, const char *name, const MemoryBuffer &obj, const char *dir, const char *xml=NULL);

class CClusterQueryStateParam : public CInterface
{
    Linked<CWsWorkunitsEx>          wsWorkunitsService;
    IEspContext&                    context;
    StringAttr                      cluster;
    StringAttr                      querySetId;
    IArrayOf<IEspQuerySetQuery>&    queries;

public:
    IMPLEMENT_IINTERFACE;
    CClusterQueryStateParam(CWsWorkunitsEx* _service, IEspContext& _context, const char* _cluster, const char* _querySetId, IArrayOf<IEspQuerySetQuery>& _queries )
       : wsWorkunitsService(_service), context(_context), cluster(_cluster), querySetId(_querySetId), queries(_queries)
    {
    }

    virtual void doWork()
    {
        wsWorkunitsService->checkAndSetClusterQueryState(context, cluster.get(), querySetId.get(), queries);
    }
};

class CClusterQueryStateThreadFactory : public CInterface, public IThreadFactory
{
    class CClusterQueryStateThread : public CInterface, implements IPooledThread
    {
        Owned<CClusterQueryStateParam> param;
    public:
        IMPLEMENT_IINTERFACE;

        void init(void *_param)
        {
            param.setown((CClusterQueryStateParam *)_param);
        }
        void main()
        {
            param->doWork();
            param.clear();
        }
        bool canReuse()
        {
            return true;
        }
        bool stop()
        {
            return true;
        }
    };

public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CClusterQueryStateThread();
    }
};

#endif
