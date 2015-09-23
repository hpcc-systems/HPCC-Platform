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

#ifndef _ESPWIZ_WsSMC_HPP__
#define _ESPWIZ_WsSMC_HPP__

#include "ws_smc_esp.ipp"
#include "dautils.hpp"
#include "wujobq.hpp"
#include "TpWrapper.hpp"
#include "WUXMLInfo.hpp"

enum BulletType
{
    bulletNONE = 0,
    bulletOrange = 1,
    bulletYellow = 2,
    bulletWhite = 3,
    bulletGreen = 4,
    bulletError = 5
};

enum ClusterStatusType
{
    RunningNormal = 0,
    QueuePausedOrStoppedWithJobs = 1,
    QueuePausedOrStoppedWithNoJob = 2,
    QueuePausedOrStoppedNotFound = 3,
    QueueRunningNotFound = 4
};

enum WsSMCStatusServerType
{
    WsSMCSSTThorLCRCluster = 0,
    WsSMCSSTRoxieCluster = 1,
    WsSMCSSTHThorCluster = 2,
    WsSMCSSTECLagent = 3,
    WsSMCSSTterm = 4
};

static const char *WsSMCStatusServerTypeNames[] = { "ThorMaster", "RoxieServer", "HThorServer", "ECLagent" };

class CWsSMCQueue
{
public:
    SCMStringBuffer queueName;
    StringBuffer queueState, queueStateDetails;
    bool foundQueueInStatusServer;
    unsigned countRunningJobs;
    unsigned countQueuedJobs;
    ClusterStatusType statusType;

    CWsSMCQueue(bool foundQueue = false): countRunningJobs(0), countQueuedJobs(0), statusType(RunningNormal)
    {
        foundQueueInStatusServer = foundQueue;
        countRunningJobs = 0;
        countQueuedJobs = 0;
    }
    virtual ~CWsSMCQueue(){};
};

class CWsSMCTargetCluster : public CInterface
{
public:
    ClusterType clusterType;
    StringAttr clusterName;
    StringAttr queueName;
    StringAttr queueStatus;
    StringAttr warning;
    int clusterSize;
    SCMStringBuffer statusServerName;
    StringBuffer clusterStatus;
    StringBuffer clusterStatusDetails;
    CWsSMCQueue clusterQueue;
    CWsSMCQueue agentQueue;
    CWsSMCQueue serverQueue;
    StringArray queuedWUIDs;
    StringArray wuidsOnServerQueue;

    CWsSMCTargetCluster(){};
    virtual ~CWsSMCTargetCluster(){};
};

class CActivityInfo : public CInterface, implements IInterface
{
    CDateTime timeCached;
    BoolHash uniqueECLWUIDs;

    Owned<IJQSnapshot> jobQueueSnapshot;

    CIArrayOf<CWsSMCTargetCluster> thorTargetClusters;
    CIArrayOf<CWsSMCTargetCluster> roxieTargetClusters;
    CIArrayOf<CWsSMCTargetCluster> hthorTargetClusters;

    IArrayOf<IEspActiveWorkunit> aws;
    IArrayOf<IEspServerJobQueue> serverJobQueues;
    IArrayOf<IEspDFUJob> DFURecoveryJobs;

    void readTargetClusterInfo(CConstWUClusterInfoArray& clusters, IPropertyTree* serverStatusRoot);
    void readTargetClusterInfo(IConstWUClusterInfo& cluster, IPropertyTree* serverStatusRoot, CWsSMCTargetCluster* targetCluster);
    bool findQueueInStatusServer(IPropertyTree* serverStatusRoot, const char* serverName, const char* queueName);
    const char *getStatusServerTypeName(WsSMCStatusServerType type);
    bool readJobQueue(const char* queueName, StringArray& wuids, StringBuffer& state, StringBuffer& stateDetails);
    void readActiveWUsAndQueuedWUs(IEspContext& context, IPropertyTree* envRoot, IPropertyTree* serverStatusRoot);
    void readRunningWUsOnStatusServer(IEspContext& context, IPropertyTree* serverStatusRoot, WsSMCStatusServerType statusServerType);
    void readWUsInTargetClusterJobQueues(IEspContext& context, CIArrayOf<CWsSMCTargetCluster>& targetClusters);
    void readWUsInTargetClusterJobQueue(IEspContext& context, CWsSMCTargetCluster& targetCluster, CWsSMCQueue& jobQueue, const char* queueName);
    void readRunningWUsAndJobQueueforOtherStatusServers(IEspContext& context, IPropertyTree* serverStatusRoot);
    void createActiveWorkUnit(IEspContext& context, Owned<IEspActiveWorkunit>& ownedWU, const char* wuid, const char* location,
        unsigned index, const char* serverName, const char* queueName, const char* instanceName, const char* targetClusterName, bool useContext);
    void getDFUServersAndWUs(IPropertyTree* envRoot, IPropertyTree* serverStatusRoot);
    unsigned readDFUWUIDs(IPropertyTree* serverStatusRoot, const char* queueName, StringArray& wuidList);
    void readDFUWUDetails(const char* queueName, const char* serverName, StringArray& wuidList, unsigned runningWUCount);
    void getDFURecoveryJobs();
    void getServerJobQueue(const char* queueName, const char* serverName, const char* serverType, const char* networkAddress, unsigned port);
    void readServerJobQueueStatus(IEspServerJobQueue* jobQueue);
    CWsSMCTargetCluster* findWUClusterInfo(const char* wuid, bool isOnECLAgent, CIArrayOf<CWsSMCTargetCluster>& targetClusters,
        CIArrayOf<CWsSMCTargetCluster>& targetClusters1, CIArrayOf<CWsSMCTargetCluster>& targetClusters2);
    CWsSMCTargetCluster* findTargetCluster(const char* clusterName, CIArrayOf<CWsSMCTargetCluster>& targetClusters);
    bool checkSetUniqueECLWUID(const char* wuid);
    void addQueuedServerQueueJob(IEspContext& context, const char* serverName, const char* queueName, const char* instanceName, CIArrayOf<CWsSMCTargetCluster>& targetClusters);

public:
    IMPLEMENT_IINTERFACE;

    CActivityInfo() {};
    virtual ~CActivityInfo() { jobQueueSnapshot.clear(); };

    bool isCachedActivityInfoValid(unsigned timeOutSeconds);
    void createActivityInfo(IEspContext& context);

    inline CIArrayOf<CWsSMCTargetCluster>& queryThorTargetClusters() { return thorTargetClusters; };
    inline CIArrayOf<CWsSMCTargetCluster>& queryRoxieTargetClusters() { return roxieTargetClusters; };
    inline CIArrayOf<CWsSMCTargetCluster>& queryHThorTargetClusters() { return hthorTargetClusters; };

    inline IArrayOf<IEspActiveWorkunit>& queryActiveWUs() { return aws; };
    inline IArrayOf<IEspServerJobQueue>& queryServerJobQueues() { return serverJobQueues; };
    inline IArrayOf<IEspDFUJob>& queryDFURecoveryJobs() { return DFURecoveryJobs; };
};

class CWsSMCEx : public CWsSMC
{
    long m_counter;
    CTpWrapper m_ClusterStatus;
    CriticalSection getActivityCrit;
    Owned<CActivityInfo> activityInfoCache;
    unsigned activityInfoCacheSeconds;

    StringBuffer m_ChatURL;
    StringBuffer m_Banner;
    StringBuffer m_BannerSize;
    StringBuffer m_BannerColor;
    StringBuffer m_BannerScroll;
    StringBuffer m_PortalURL;
    int m_BannerAction;
    bool m_EnableChatURL;
    CriticalSection crit;

public:
    IMPLEMENT_IINTERFACE;
    CWsSMCEx(){}
    virtual ~CWsSMCEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    bool onMoveJobDown(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobUp(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobBack(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onMoveJobFront(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onRemoveJob(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp);
    bool onStopQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onResumeQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onPauseQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onClearQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp);
    bool onIndex(IEspContext &context, IEspSMCIndexRequest &req, IEspSMCIndexResponse &resp);
    bool onActivity(IEspContext &context, IEspActivityRequest &req, IEspActivityResponse &resp);
    bool onSetJobPriority(IEspContext &context, IEspSMCPriorityRequest &req, IEspSMCPriorityResponse &resp);
    bool onGetThorQueueAvailability(IEspContext &context, IEspGetThorQueueAvailabilityRequest &req, IEspGetThorQueueAvailabilityResponse& resp);
    bool onSetBanner(IEspContext &context, IEspSetBannerRequest &req, IEspSetBannerResponse& resp);
    bool onNotInCommunityEdition(IEspContext &context, IEspNotInCommunityEditionRequest &req, IEspNotInCommunityEditionResponse &resp);

    virtual bool onBrowseResources(IEspContext &context, IEspBrowseResourcesRequest & req, IEspBrowseResourcesResponse & resp);
    virtual bool onRoxieControlCmd(IEspContext &context, IEspRoxieControlCmdRequest &req, IEspRoxieControlCmdResponse &resp);
    virtual bool onGetStatusServerInfo(IEspContext &context, IEspGetStatusServerInfoRequest &req, IEspGetStatusServerInfoResponse &resp);
    virtual bool onLockQuery(IEspContext &context, IEspLockQueryRequest &req, IEspLockQueryResponse &resp);
private:
    void addCapabilities( IPropertyTree* pFeatureNode, const char* access, IArrayOf<IEspCapability>& capabilities);
    void addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* serverName,
        const char* serverType, const char* networkAddress, unsigned port, IJQSnapshot* jobQueueSnapshot);
    void addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* serverName,
        const char* serverType, const char* networkAddress, unsigned port, const char* queueState, const char* queueStateDetails);
    void readBannerAndChatRequest(IEspContext& context, IEspActivityRequest &req, IEspActivityResponse& resp);
    void setBannerAndChatData(double version, IEspActivityResponse& resp);
    void getServersAndWUs(IEspContext &context, IEspActivityRequest &req, IPropertyTree* envRoot, CConstWUClusterInfoArray& clusters,
        CActivityInfo* activityInfo);

    void sortTargetClusters(IArrayOf<IEspTargetCluster>& clusters, const char* sortBy, bool descending);
    void addToTargetClusterList(IArrayOf<IEspTargetCluster>& clusters, IEspTargetCluster* cluster, const char* sortBy, bool descending);
    void getClusterQueueStatus(const CWsSMCTargetCluster& targetCluster, ClusterStatusType& queueStatusType, StringBuffer& queueStatusDetails);
    void setClusterStatus(IEspContext& context, const CWsSMCTargetCluster& targetCluster, IEspTargetCluster* returnCluster);
    void getTargetClusterAndWUs(IEspContext& context, CConstWUClusterInfoArray& clusters, IConstWUClusterInfo& cluster,
        IPropertyTree* serverStatusRoot, IPropertyTreeIterator* itStatusECLagent, IEspTargetCluster* returnCluster, IArrayOf<IEspActiveWorkunit>& aws);
    const char* createQueueActionInfo(IEspContext &context, const char* action, IEspSMCQueueRequest &req, StringBuffer& info);
    void setJobPriority(IEspContext &context, IWorkUnitFactory* factory, const char* wuid, const char* queue, WUPriorityClass& priority);

    void setESPTargetClusters(IEspContext& context, const CIArrayOf<CWsSMCTargetCluster>& targetClusters, IArrayOf<IEspTargetCluster>& respTargetClusters);
    void clearActivityInfoCache();
    CActivityInfo* getActivityInfo(IEspContext &context);
    void setActivityResponse(IEspContext &context, CActivityInfo* activityInfo, IEspActivityRequest &req, IEspActivityResponse& resp);
    void addWUsToResponse(IEspContext &context, const IArrayOf<IEspActiveWorkunit>& aws, IEspActivityResponse& resp);

    void getStatusServerInfo(IEspContext &context, const char *serverType, const char *serverName, const char *networkAddress, unsigned port, IEspStatusServerInfo& statusServerInfo);
    void setTargetClusterInfo(IEspContext &context, const char *serverType, const char *serverName, const CIArrayOf<CWsSMCTargetCluster>& targetClusters,
        const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo);
    void setServerQueueInfo(IEspContext &context, const char *serverType, const char *serverName, const IArrayOf<IEspServerJobQueue>& serverJobQueues,
        const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo);
    void setServerQueueInfo(IEspContext &context, const char *serverType, const char *networkAddress, unsigned port, const IArrayOf<IEspServerJobQueue>& serverJobQueues,
        const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo);
    void setESPTargetCluster(IEspContext &context, const CWsSMCTargetCluster& targetCluster, IEspTargetCluster* espTargetCluster);
    void setActiveWUs(IEspContext &context, const char *serverType, const char *clusterName, const char *queueName, const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo);
    void setActiveWUs(IEspContext &context, const char *serverType, const char *instance, const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo);
    void setActiveWUs(IEspContext &context, IEspActiveWorkunit& wu, IEspActiveWorkunit* wuToSet);
    void addLockInfo(CLockMetaData& lD, const char* xPath, const char* lfn, unsigned msNow, time_t ttNow, IArrayOf<IEspLock>& locks);
};


class CWsSMCSoapBindingEx : public CWsSMCSoapBinding
{
    StringBuffer m_portalURL;
public:
    CWsSMCSoapBindingEx(){}
    CWsSMCSoapBindingEx(IPropertyTree* cfg, const char *bindname=NULL, const char *procname=NULL):CWsSMCSoapBinding(cfg, bindname, procname)
    {
        if (!procname || !*procname)
            return;

        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", procname);
        const char* portalURL = cfg->queryProp(xpath.str());
        if (portalURL && *portalURL)
            m_portalURL.append(portalURL);
    }

    virtual ~CWsSMCSoapBindingEx(){}
    virtual const char* getRootPage(IEspContext* ctx)
    {
        if (ctx->queryRequestParameters()->hasProp("legacy"))
            return NULL;
        return "stub.htm";
    }
    virtual int onGet(CHttpRequest* request,  CHttpResponse* response);
    void handleHttpPost(CHttpRequest *request, CHttpResponse *response);
    int onHttpEcho(CHttpRequest* request,  CHttpResponse* response);


    virtual int onGetRoot(IEspContext &context, CHttpRequest* request,  CHttpResponse* response)
    {
        return  onGetInstantQuery(context, request, response, "WsSMC", "Activity");
    }

    virtual int onGetIndex(IEspContext &context, CHttpRequest* request,  CHttpResponse* response, const char *service)
    {
        return  onGetInstantQuery(context, request, response, "WsSMC", "Activity");
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        data.setProp("@appName", "EclWatch");
        data.setProp("@start_page", "/WsSMC/Activity");
        IPropertyTree *folder = ensureNavFolder(data, "Clusters", NULL, NULL, false, 1);
        ensureNavLink(*folder, "Activity", "/WsSMC/Activity", "Display Activity on all target clusters in an environment", NULL, NULL, 1);
        ensureNavLink(*folder, "Scheduler", "/WsWorkunits/WUShowScheduled", "Access the ECL Scheduler to view and manage scheduled workunits or events", NULL, NULL, 2);
#ifndef USE_RESOURCE
        if (m_portalURL.length() > 0)
        {
            IPropertyTree *folderTools = ensureNavFolder(data, "Resources", NULL, NULL, false, 8);
            ensureNavLink(*folderTools, "Browse", "/WsSMC/BrowseResources", "Browse a list of resources available for download, such as the ECL IDE, documentation, examples, etc. These are only available if optional packages are installed on the ESP Server.", NULL, NULL, 1);
        }
#else
        IPropertyTree *folderTools = ensureNavFolder(data, "Resources", NULL, NULL, false, 8);
        ensureNavLink(*folderTools, "Browse", "/WsSMC/BrowseResources", "Browse a list of resources available for download, such as the ECL IDE, documentation, examples, etc. These are only available if optional packages are installed on the ESP Server.", NULL, NULL, 1);
#endif
    }
    virtual void getNavSettings(int &width, bool &resizable, bool &scroll)
    {
        width=168;
        resizable=false;
        scroll=true;
    }

    virtual int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
};

#endif //_ESPWIZ_WsSMC_HPP__
