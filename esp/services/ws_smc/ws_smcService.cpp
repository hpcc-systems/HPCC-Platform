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

#pragma warning (disable : 4786)

#include "build-config.h"

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif
#include "ws_smcService.hpp"
#include "wshelpers.hpp"

#include "dalienv.hpp"
#include "dasds.hpp"
#include "WUWrapper.hpp"
#include "dfuwu.hpp"
#include "exception_util.hpp"

#include "roxiecontrol.hpp"
#include "workunit.hpp"

#define STATUS_SERVER_THOR "ThorMaster"
#define STATUS_SERVER_HTHOR "HThorServer"
#define STATUS_SERVER_ROXIE "RoxieServer"
#define STATUS_SERVER_DFUSERVER "DFUserver"
#define STATUS_SERVER_ECLSERVER "ECLserver"
#define STATUS_SERVER_ECLCCSERVER "ECLCCserver"
#define STATUS_SERVER_ECLAGENT "ECLagent"

#define CLUSTER_TYPE_THOR "Thor"
#define CLUSTER_TYPE_HTHOR "HThor"
#define CLUSTER_TYPE_ROXIE "Roxie"

static const char* FEATURE_URL = "SmcAccess";
const char* THORQUEUE_FEATURE = "ThorQueueAccess";
static const char* ROXIE_CONTROL_URL = "RoxieControlAccess";
static const char* OWN_WU_ACCESS = "OwnWorkunitsAccess";
static const char* OTHERS_WU_ACCESS = "OthersWorkunitsAccess";

const char* PERMISSIONS_FILENAME = "espsmc_permissions.xml";
const unsigned DEFAULTACTIVITYINFOCACHETIMEOUTSECOND = 10;

void AccessSuccess(IEspContext& context, char const * msg,...) __attribute__((format(printf, 2, 3)));
void AccessSuccess(IEspContext& context, char const * msg,...)
{
    StringBuffer buf;
    buf.appendf("User %s: ",context.queryUserId());
    va_list args;
    va_start(args, msg);
    buf.valist_appendf(msg, args);
    va_end(args);
    AUDIT(AUDIT_TYPE_ACCESS_SUCCESS,buf.str());
}

void AccessFailure(IEspContext& context, char const * msg,...) __attribute__((format(printf, 2, 3)));
void AccessFailure(IEspContext& context, char const * msg,...)
{
    StringBuffer buf;
    buf.appendf("User %s: ",context.queryUserId());
    va_list args;
    va_start(args, msg);
    buf.valist_appendf(msg, args);
    va_end(args);

    AUDIT(AUDIT_TYPE_ACCESS_FAILURE,buf.str());
}

struct QueueLock
{
    QueueLock(IJobQueue* q): queue(q) { queue->lock(); }
    ~QueueLock() 
    { 
        queue->unlock(); 
    }

    Linked<IJobQueue> queue;
};

static int sortTargetClustersByNameDescending(IInterface * const *L, IInterface * const *R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return strcmp(right->getClusterName(), left->getClusterName());
}

static int sortTargetClustersByNameAscending(IInterface * const *L, IInterface * const *R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return strcmp(left->getClusterName(), right->getClusterName());
}

static int sortTargetClustersBySizeDescending(IInterface * const *L, IInterface * const *R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return right->getClusterSize() - left->getClusterSize();
}

static int sortTargetClustersBySizeAscending(IInterface * const *L, IInterface * const *R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return left->getClusterSize() - right->getClusterSize();
}

void CWsSMCEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }

    m_BannerAction = 0;
    m_EnableChatURL = false;
    m_BannerSize = "4";
    m_BannerColor = "red";
    m_BannerScroll = "2";

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name='%s']/@portalurl", process);
    const char* portalURL = cfg->queryProp(xpath.str());
    if (portalURL && *portalURL)
        m_PortalURL.append(portalURL);

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/ActivityInfoCacheSeconds", process, service);
    activityInfoCacheSeconds = cfg->getPropInt(xpath.str(), DEFAULTACTIVITYINFOCACHETIMEOUTSECOND);
}

struct CActiveWorkunitWrapper: public CActiveWorkunit
{
    CActiveWorkunitWrapper(IEspContext &context, const char* wuid,const char* location = NULL,unsigned index=0): CActiveWorkunit("","")
    {
        CWUWrapper wu(wuid, context);
        setActiveWorkunit(wu, wuid, location, index, context.getClientVersion(), false);
    }

    CActiveWorkunitWrapper(const char* wuid, const char* location = NULL, unsigned index=0): CActiveWorkunit("","")
    {
        CWUWrapper wu(wuid);
        setActiveWorkunit(wu, wuid, location, index, 0.0, true);
    }

    CActiveWorkunitWrapper(const char* wuid,const char* owner, const char* jobname, const char* state, const char* priority): CActiveWorkunit("","")
    {
        setWuid(wuid);
        setState(state);
        setOwner(owner);
        setJobname(jobname);
        setPriority(priority);
    }

    void setActiveWorkunit(CWUWrapper& wu, const char* wuid, const char* location, unsigned index, double version, bool notCheckVersion)
    {
        SCMStringBuffer stateEx;
        setWuid(wuid);
        const char *state = wu->queryStateDesc();
        setStateID(wu->getState());
        if (wu->getState() == WUStateBlocked)
        {
            wu->getStateEx(stateEx);
            if (notCheckVersion || (version > 1.00))
                setExtra(stateEx.str());
        }
        buildAndSetState(state, stateEx.str(), location, index);
        if ((notCheckVersion || (version > 1.09)) && (wu->getState() == WUStateFailed))
            setWarning("The job will ultimately not complete. Please check ECLAgent.");

        setOwner(wu->queryUser());
        setJobname(wu->queryJobName());
        setPriorityStr(wu->getPriority());

        if ((notCheckVersion || (version > 1.08)) && wu->isPausing())
        {
            setIsPausing(true);
        }
        if (notCheckVersion || (version > 1.14))
        {
            setClusterName(wu->queryClusterName());
        }
    }

    void buildAndSetState(const char* state, const char* stateEx, const char* location, unsigned index)
    {
        if (!state || !*state)
            return;
        StringBuffer stateStr;
        if(index && location && *location)
            stateStr.setf("queued(%d) [%s on %s]", index, state, location);
        else if(index)
            stateStr.setf("queued(%d) [%s]", index, state);
        else if(location && *location)
            stateStr.setf("%s [%s]", state, location);
        else
            stateStr.set(state);
        if (stateEx && *stateEx)
            stateStr.appendf(" %s", stateEx);
        setState(stateStr.str());
    }

    void setPriorityStr(unsigned priorityType)
    {
        switch(priorityType)
        {
            case PriorityClassHigh: setPriority("high"); break;
            default:
            case PriorityClassNormal: setPriority("normal"); break;
            case PriorityClassLow: setPriority("low"); break;
        }
        return;
    }
};

bool CActivityInfo::isCachedActivityInfoValid(unsigned timeOutSeconds)
{
    CDateTime timeNow;
    timeNow.setNow();
    return timeNow.getSimple() <= timeCached.getSimple() + timeOutSeconds;;
}

void CActivityInfo::createActivityInfo(IEspContext& context)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Failed to get environment information.");

    CConstWUClusterInfoArray clusters;
    Owned<IPropertyTree> envRoot= &env->getPTree();
    getEnvironmentClusterInfo(envRoot, clusters);

    try
    {
        jobQueueSnapshot.setown(createJQSnapshot());
    }
    catch(IException* e)
    {
        EXCLOG(e, "CActivityInfo::createActivityInfo");
        e->Release();
    }

    Owned<IRemoteConnection> connStatusServers = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,30000);
    if (!connStatusServers)
        throw MakeStringException(ECLWATCH_CANNOT_GET_STATUS_INFO, "Failed to get status server information.");

    IPropertyTree* serverStatusRoot = connStatusServers->queryRoot();

    readTargetClusterInfo(clusters, serverStatusRoot);
    readActiveWUsAndQueuedWUs(context, envRoot, serverStatusRoot);

    timeCached.setNow();
}

void CActivityInfo::readTargetClusterInfo(CConstWUClusterInfoArray& clusters, IPropertyTree* serverStatusRoot)
{
    ForEachItemIn(c, clusters)
    {
        IConstWUClusterInfo &cluster = clusters.item(c);
        Owned<CWsSMCTargetCluster> targetCluster = new CWsSMCTargetCluster();
        readTargetClusterInfo(cluster, serverStatusRoot, targetCluster);
        if (cluster.getPlatform() == ThorLCRCluster)
            thorTargetClusters.append(*targetCluster.getClear());
        else if (cluster.getPlatform() == RoxieCluster)
            roxieTargetClusters.append(*targetCluster.getClear());
        else
            hthorTargetClusters.append(*targetCluster.getClear());
    }
}

void CActivityInfo::readTargetClusterInfo(IConstWUClusterInfo& cluster, IPropertyTree* serverStatusRoot, CWsSMCTargetCluster* targetCluster)
{
    SCMStringBuffer clusterName;
    cluster.getName(clusterName);
    targetCluster->clusterName.set(clusterName.str());
    targetCluster->clusterType = cluster.getPlatform();
    targetCluster->clusterSize = cluster.getSize();
    cluster.getServerQueue(targetCluster->serverQueue.queueName);
    cluster.getAgentQueue(targetCluster->agentQueue.queueName);

    StringBuffer statusServerName;
    CWsSMCQueue* smcQueue = NULL;
    if (targetCluster->clusterType == ThorLCRCluster)
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTThorLCRCluster));
        smcQueue = &targetCluster->clusterQueue;
        cluster.getThorQueue(smcQueue->queueName);
    }
    else if (targetCluster->clusterType == RoxieCluster)
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTRoxieCluster));
        smcQueue = &targetCluster->agentQueue;
    }
    else
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTHThorCluster));
        smcQueue = &targetCluster->agentQueue;
    }

    targetCluster->statusServerName.set(statusServerName.str());
    targetCluster->queueName.set(smcQueue->queueName.str());

    bool validQueue = readJobQueue(smcQueue->queueName.str(), targetCluster->queuedWUIDs, smcQueue->queueState, smcQueue->queueStateDetails);
    if (validQueue && smcQueue->queueState.length())
        targetCluster->queueStatus.set(smcQueue->queueState.str());

    if (serverStatusRoot)
    {
        smcQueue->foundQueueInStatusServer = findQueueInStatusServer(serverStatusRoot, statusServerName.str(), targetCluster->queueName.get());
        if (!smcQueue->foundQueueInStatusServer)
            targetCluster->clusterStatusDetails.appendf("Cluster %s not attached; ", clusterName.str());
    }

    readJobQueue(targetCluster->serverQueue.queueName.str(), targetCluster->wuidsOnServerQueue, targetCluster->serverQueue.queueState, targetCluster->serverQueue.queueStateDetails);
}

bool CActivityInfo::readJobQueue(const char* queueName, StringArray& wuids, StringBuffer& state, StringBuffer& stateDetails)
{
    if (!queueName || !*queueName)
        return false;

    if (!jobQueueSnapshot)
    {
        WARNLOG("CActivityInfo::readJobQueue: jobQueueSnapshot not found.");
        return false;
    }

    Owned<IJobQueueConst> jobQueue = jobQueueSnapshot->getJobQueue(queueName);
    if (!jobQueue)
    {
        WARNLOG("CActivityInfo::readJobQueue: failed to get info for job queue %s", queueName);
        return false;
    }

    CJobQueueContents queuedJobs;
    jobQueue->copyItemsAndState(queuedJobs, state, stateDetails);

    Owned<IJobQueueIterator> iter = queuedJobs.getIterator();
    ForEach(*iter)
    {
        const char* wuid = iter->query().queryWUID();
        if (wuid && *wuid)
            wuids.append(wuid);
    }
    return true;
}

const char *CActivityInfo::getStatusServerTypeName(WsSMCStatusServerType type)
{
    return (type < WsSMCSSTterm) ? WsSMCStatusServerTypeNames[type] : NULL;
}

bool CActivityInfo::findQueueInStatusServer(IPropertyTree* serverStatusRoot, const char* serverName, const char* queueName)
{
    VStringBuffer path("Server[@name=\"%s\"]", serverName);
    Owned<IPropertyTreeIterator> it(serverStatusRoot->getElements(path.str()));
    ForEach(*it)
    {
        IPropertyTree& serverStatusNode = it->query();
        const char* queue = serverStatusNode.queryProp("@queue");
        if (!queue || !*queue)
            continue;

        StringArray qlist;
        qlist.appendListUniq(queue, ",");
        ForEachItemIn(q, qlist)
        {
            if (strieq(qlist.item(q), queueName))
                return true;
        }
    }
    return false;
}

void CActivityInfo::readActiveWUsAndQueuedWUs(IEspContext& context, IPropertyTree* envRoot, IPropertyTree* serverStatusRoot)
{
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTThorLCRCluster);
    readWUsInTargetClusterJobQueues(context, thorTargetClusters);
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTRoxieCluster);
    readWUsInTargetClusterJobQueues(context, roxieTargetClusters);
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTHThorCluster);
    readWUsInTargetClusterJobQueues(context, hthorTargetClusters);

    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTECLagent);
    readRunningWUsAndJobQueueforOtherStatusServers(context, serverStatusRoot);
    //TODO: add queued WUs for ECLCCServer/ECLServer here. Right now, they are under target clusters.

    getDFUServersAndWUs(envRoot, serverStatusRoot);
    getDFURecoveryJobs();
}

void CActivityInfo::readRunningWUsOnStatusServer(IEspContext& context, IPropertyTree* serverStatusRoot, WsSMCStatusServerType statusServerType)
{
    const char* serverName = getStatusServerTypeName(statusServerType);
    if (!serverName || !*serverName)
        return;
    bool isECLAgent = (statusServerType == WsSMCSSTECLagent);
    VStringBuffer path("Server[@name=\"%s\"]", serverName);
    Owned<IPropertyTreeIterator> itrStatusServer(serverStatusRoot->getElements(path.str()));
    ForEach(*itrStatusServer)
    {
        IPropertyTree& serverStatusNode = itrStatusServer->query();

        StringBuffer serverInstance;
        if ((statusServerType == WsSMCSSTThorLCRCluster) || (statusServerType == WsSMCSSTRoxieCluster))
            serverStatusNode.getProp("@cluster", serverInstance);
        else
            serverInstance.appendf("%s on %s", serverName, serverStatusNode.queryProp("@node"));

        Owned<IPropertyTreeIterator> wuids(serverStatusNode.getElements("WorkUnit"));
        ForEach(*wuids)
        {
            const char* wuid=wuids->query().queryProp(NULL);
            if (!wuid || !*wuid || checkSetUniqueECLWUID(wuid))
                continue;

            CWsSMCTargetCluster* targetCluster;
            if (statusServerType == WsSMCSSTRoxieCluster)
                targetCluster = findWUClusterInfo(wuid, isECLAgent, roxieTargetClusters, thorTargetClusters, hthorTargetClusters);
            else if (statusServerType == WsSMCSSTHThorCluster)
                targetCluster = findWUClusterInfo(wuid, isECLAgent, hthorTargetClusters, thorTargetClusters, roxieTargetClusters);
            else
                targetCluster = findWUClusterInfo(wuid, isECLAgent, thorTargetClusters, roxieTargetClusters, hthorTargetClusters);
            if (!targetCluster)
                continue;

            const char* targetClusterName = targetCluster->clusterName.get();
            CWsSMCQueue* jobQueue;
            if (statusServerType == WsSMCSSTThorLCRCluster)
                jobQueue = &targetCluster->clusterQueue;
            else
                jobQueue = &targetCluster->agentQueue;

            Owned<IEspActiveWorkunit> wu;
            if (!isECLAgent)
            {
                const char *cluster = serverStatusNode.queryProp("Cluster");
                StringBuffer queueName;
                if (cluster) // backward compat check.
                    getClusterThorQueueName(queueName, cluster);
                else
                    queueName.append(targetCluster->queueName.get());

                createActiveWorkUnit(context, wu, wuid, !strieq(targetClusterName, serverInstance.str()) ? serverInstance.str() : NULL, 0, serverName,
                    queueName, serverInstance.str(), targetClusterName, false);

                if (wu->getStateID() == WUStateRunning) //'aborting' may be another possible status
                {
                    int sgDuration = serverStatusNode.getPropInt("@sg_duration", -1);
                    int subgraph = serverStatusNode.getPropInt("@subgraph", -1);
                    if (subgraph > -1 && sgDuration > -1)
                    {
                        const char* graph = serverStatusNode.queryProp("@graph");
                        VStringBuffer durationStr("%d min", sgDuration);
                        VStringBuffer subgraphStr("%d", subgraph);

                        wu->setGraphName(graph);
                        wu->setDuration(durationStr.str());
                        wu->setGID(subgraphStr.str());
                    }

                    if (serverStatusNode.getPropInt("@memoryBlocked ", 0) != 0)
                        wu->setMemoryBlocked(1);
                }
            }
            else
            {
                createActiveWorkUnit(context, wu, wuid, serverInstance.str(), 0, serverName, serverName, serverInstance.str(), targetClusterName, false);

                if (targetCluster->clusterType == ThorLCRCluster)
                    wu->setClusterType(CLUSTER_TYPE_THOR);
                else if (targetCluster->clusterType == RoxieCluster)
                    wu->setClusterType(CLUSTER_TYPE_ROXIE);
                else
                    wu->setClusterType(CLUSTER_TYPE_HTHOR);
                wu->setClusterQueueName(targetCluster->queueName.get());

                if (wu->getStateID() != WUStateRunning)
                {
                    const char *extra = wu->getExtra();
                    if (wu->getStateID() != WUStateBlocked || !extra || !*extra)  // Blocked on persist treated as running here
                    {
                        aws.append(*wu.getClear());
                        jobQueue->countQueuedJobs++;
                        continue;
                    }
                }

                //Should this be set only if wu->getStateID() == WUStateRunning?
                if (serverStatusNode.getPropInt("@memoryBlocked ", 0) != 0)
                    wu->setMemoryBlocked(1);
            }

            aws.append(*wu.getClear());
            jobQueue->countRunningJobs++;
        }
    }
}

bool CActivityInfo::checkSetUniqueECLWUID(const char* wuid)
{
    bool* idFound = uniqueECLWUIDs.getValue(wuid);
    if (!idFound || !*idFound)
        uniqueECLWUIDs.setValue(wuid, true);
    return idFound && *idFound;
}

CWsSMCTargetCluster* CActivityInfo::findWUClusterInfo(const char* wuid, bool isOnECLAgent, CIArrayOf<CWsSMCTargetCluster>& targetClusters,
    CIArrayOf<CWsSMCTargetCluster>& targetClusters1, CIArrayOf<CWsSMCTargetCluster>& targetClusters2)
{
    StringAttr clusterName;
    try
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
        if (!cw)
            return NULL;
        clusterName.set(cw->queryClusterName());
        if (!clusterName.length())
            return NULL;
    }
    catch (IException *e)
    {//Exception may be thrown when the openWorkUnit() is called inside the CWUWrapper
        StringBuffer msg;
        WARNLOG("Failed to open workunit %s: %s", wuid, e->errorMessage(msg).str());
        e->Release();
        return NULL;
    }

    const char* cluster = clusterName.str();
    CWsSMCTargetCluster* targetCluster = findTargetCluster(cluster, targetClusters);
    if (targetCluster || !isOnECLAgent)
        return targetCluster;

    targetCluster = findTargetCluster(cluster, targetClusters1);
    if (targetCluster)
        return targetCluster;

    return findTargetCluster(cluster, targetClusters2);
}


CWsSMCTargetCluster* CActivityInfo::findTargetCluster(const char* clusterName, CIArrayOf<CWsSMCTargetCluster>& targetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        if (strieq(targetCluster.clusterName.get(), clusterName))
            return &targetCluster;
    }
    return NULL;
}

void CActivityInfo::createActiveWorkUnit(IEspContext& context, Owned<IEspActiveWorkunit>& ownedWU, const char* wuid, const char* location,
    unsigned index, const char* serverName, const char* queueName, const char* instanceName, const char* targetClusterName, bool useContext)
{
    try
    {
        if (useContext)
            ownedWU.setown(new CActiveWorkunitWrapper(context, wuid, location, index));
        else
            ownedWU.setown(new CActiveWorkunitWrapper(wuid, location, index));
    }
    catch (IException *e)
    {   //if the wu cannot be opened for some reason, the openWorkUnit() inside the CActiveWorkunitWrapper() may throw an exception.
        //We do not want the exception stops this process of retrieving/showing all active WUs. And that WU should still be displayed
        //with the exception.
        StringBuffer msg;
        ownedWU.setown(new CActiveWorkunitWrapper(wuid, "", "", e->errorMessage(msg).str(), "normal"));
        ownedWU->setStateID(WUStateUnknown);
        e->Release();
    }

    ownedWU->setServer(serverName);
    ownedWU->setQueueName(queueName);
    if (instanceName && *instanceName)
        ownedWU->setInstance(instanceName); // JCSMORE In thor case at least, if queued it is unknown which instance it will run on..
    if (targetClusterName && *targetClusterName)
        ownedWU->setTargetClusterName(targetClusterName);
}

void CActivityInfo::readWUsInTargetClusterJobQueues(IEspContext& context, CIArrayOf<CWsSMCTargetCluster>& targetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        if (targetCluster.clusterType == ThorLCRCluster)
            readWUsInTargetClusterJobQueue(context, targetCluster, targetCluster.clusterQueue, targetCluster.clusterName.get());
        if (targetCluster.agentQueue.queueName.length())
            readWUsInTargetClusterJobQueue(context, targetCluster, targetCluster.agentQueue, targetCluster.agentQueue.queueName.str());
        if (targetCluster.serverQueue.queueName.length()) //TODO: queued WUs for ECLCCServer/ECLServer should not be here.
            readWUsInTargetClusterJobQueue(context, targetCluster, targetCluster.serverQueue, targetCluster.serverQueue.queueName.str());
    }
}

void CActivityInfo::readWUsInTargetClusterJobQueue(IEspContext& context, CWsSMCTargetCluster& targetCluster, CWsSMCQueue& jobQueue, const char* queueName)
{
    ForEachItemIn(i, targetCluster.queuedWUIDs)
    {
        const char* wuid = targetCluster.queuedWUIDs.item(i);
        if (!wuid || !*wuid || checkSetUniqueECLWUID(wuid))
            continue;

        Owned<IEspActiveWorkunit> wu;
        createActiveWorkUnit(context, wu, wuid, jobQueue.queueName.str(), ++jobQueue.countQueuedJobs, targetCluster.statusServerName.str(),
            queueName, NULL, targetCluster.clusterName.get(), false);
        aws.append(*wu.getClear());
    }
}

void CActivityInfo::addQueuedServerQueueJob(IEspContext& context, const char* serverName, const char* queueName, const char* instanceName,  CIArrayOf<CWsSMCTargetCluster>& targetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        if (!targetCluster.wuidsOnServerQueue.length() || !strieq(queueName, targetCluster.serverQueue.queueName.str()))
            continue;

        ForEachItemIn(i1, targetCluster.wuidsOnServerQueue)
        {
            const char* wuid = targetCluster.wuidsOnServerQueue.item(i1);
            if (!wuid || !*wuid) //Multiple servers may monitor one queue. The WU may be shown under the multiple servers.
                continue;

            Owned<IEspActiveWorkunit> wu;
            createActiveWorkUnit(context, wu, wuid, NULL, 0, serverName, queueName, instanceName, NULL, false);
            aws.append(*wu.getClear());
        }
    }
}

void CActivityInfo::readRunningWUsAndJobQueueforOtherStatusServers(IEspContext& context, IPropertyTree* serverStatusRoot)
{
    BoolHash uniqueServers;
    Owned<IPropertyTreeIterator> it(serverStatusRoot->getElements("Server"));
    ForEach(*it)
    {
        IPropertyTree& serverNode = it->query();
        const char* serverName = serverNode.queryProp("@name");
        const char* node = serverNode.queryProp("@node");
        const char* queueName = serverNode.queryProp("@queue");
        unsigned port = serverNode.getPropInt("@mpport", 0);
        if (!serverName || !*serverName || !node || !*node || strieq(serverName, STATUS_SERVER_DFUSERVER)
            || strieq(serverName, getStatusServerTypeName(WsSMCSSTThorLCRCluster)) || strieq(serverName, getStatusServerTypeName(WsSMCSSTRoxieCluster))
            || strieq(serverName, getStatusServerTypeName(WsSMCSSTHThorCluster)) || strieq(serverName, getStatusServerTypeName(WsSMCSSTECLagent)))
            continue; //target clusters, ECLAgent, DFUServer already handled separately

        VStringBuffer instanceName("%s_on_%s:%d", serverName, node, port); //where to get a better instance name?
        Owned<IPropertyTreeIterator> wuids(serverNode.getElements("WorkUnit"));
        ForEach(*wuids)
        {
            const char* wuid=wuids->query().queryProp(NULL);
            if (!wuid || !*wuid || checkSetUniqueECLWUID(wuid))
                continue;

            Owned<IEspActiveWorkunit> wu;
            createActiveWorkUnit(context, wu, wuid, NULL, 0, serverName, queueName, instanceName.str(), NULL, false);
            aws.append(*wu.getClear());
        }

        bool* found = uniqueServers.getValue(instanceName);
        if (!found || !*found)
        {
            uniqueServers.setValue(instanceName, true);
            getServerJobQueue(queueName, instanceName, serverName, node, port);

            //Now, we found a new server. we need to add queued jobs from the queues the server is monitoring.
            StringArray qList;
            qList.appendListUniq(queueName, ",");
            ForEachItemIn(q, qList)
            {
                addQueuedServerQueueJob(context, serverName, qList.item(q), instanceName.str(), thorTargetClusters);
                addQueuedServerQueueJob(context, serverName, qList.item(q), instanceName.str(), roxieTargetClusters);
                addQueuedServerQueueJob(context, serverName, qList.item(q), instanceName.str(), hthorTargetClusters);
            }
        }
    }

    return;
}

void CActivityInfo::getDFUServersAndWUs(IPropertyTree* envRoot, IPropertyTree* serverStatusRoot)
{
    if (!envRoot)
        return;

    VStringBuffer path("Software/%s", eqDfu);
    Owned<IPropertyTreeIterator> services = envRoot->getElements(path);
    ForEach(*services)
    {
        IPropertyTree &serviceTree = services->query();
        const char *qname = serviceTree.queryProp("@queue");
        if (!qname || !*qname)
            continue;

        StringArray queues;
        queues.appendListUniq(qname, ",");
        const char *serverName = serviceTree.queryProp("@name");
        ForEachItemIn(q, queues)
        {
            StringArray wuidList;
            const char *queueName = queues.item(q);
            readDFUWUDetails(queueName, serverName, wuidList, readDFUWUIDs(serverStatusRoot, queueName, wuidList));
            getServerJobQueue(queueName, serverName, STATUS_SERVER_DFUSERVER, NULL, 0);
        }
    }
}

unsigned CActivityInfo::readDFUWUIDs(IPropertyTree* serverStatusRoot, const char* queueName, StringArray& wuidList)
{
    if (!queueName || !*queueName)
    {
        WARNLOG("CActivityInfo::readDFUWUIDs: queue name not specified");
        return 0;
    }

    unsigned runningWUCount = 0;
    VStringBuffer path("Server[@name=\"DFUserver\"]/Queue[@name=\"%s\"]",queueName);
    Owned<IPropertyTreeIterator> iter = serverStatusRoot->getElements(path.str());
    ForEach(*iter)
    {
        Owned<IPropertyTreeIterator> iterj = iter->query().getElements("Job");
        ForEach(*iterj)
        {
            const char *wuid = iterj->query().queryProp("@wuid");
            if (wuid && *wuid && (*wuid!='!')) // filter escapes -- see queuedJobs() in dfuwu.cpp
            {
                wuidList.append(wuid);
                runningWUCount++;
            }
        }
    }

    if (!jobQueueSnapshot)
        return runningWUCount;

    //Read queued jobs
    Owned<IJobQueueConst> jobQueue = jobQueueSnapshot->getJobQueue(queueName);
    if (!jobQueue)
    {
        WARNLOG("CActivityInfo::readDFUWUIDs: failed to get info for job queue %s.", queueName);
        return runningWUCount;
    }

    CJobQueueContents jobList;
    jobQueue->copyItems(jobList);
    Owned<IJobQueueIterator> iterq = jobList.getIterator();
    ForEach(*iterq)
    {
        const char* wuid = iterq->query().queryWUID();
        if (wuid && *wuid)
            wuidList.append(wuid);
    }
    return runningWUCount;
}

void CActivityInfo::readDFUWUDetails(const char* queueName, const char* serverName, StringArray& wuidList, unsigned runningWUCount)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    ForEachItemIn(i, wuidList)
    {
        StringBuffer jname, uname, state, error;
        const char *wuid = wuidList.item(i);
        if (i<runningWUCount)
            state.set("running");
        else
            state.set("queued");

        try
        {
            Owned<IConstDFUWorkUnit> dfuwu = factory->openWorkUnit(wuid, false);
            dfuwu->getUser(uname);
            dfuwu->getJobName(jname);
        }
        catch (IException *e)
        {
            e->errorMessage(error);
            state.appendf(" (%s)", error.str());
            e->Release();
        }

        Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(wuid, uname.str(), jname.str(), state.str(), "normal"));
        wu->setServer(STATUS_SERVER_DFUSERVER);
        wu->setInstance(serverName);
        wu->setQueueName(queueName);
        aws.append(*wu.getClear());
    }
}

void CActivityInfo::getDFURecoveryJobs()
{
    Owned<IRemoteConnection> connDFURecovery = querySDS().connect("DFU/RECOVERY",myProcessSession(), RTM_LOCK_READ, 30000);
    if (!connDFURecovery)
        return;

    Owned<IPropertyTreeIterator> it(connDFURecovery->queryRoot()->getElements("job"));
    ForEach(*it)
    {
        IPropertyTree &jb=it->query();
        if (!jb.getPropBool("Running",false))
            continue;

        unsigned done = 0, total = 0;
        Owned<IPropertyTreeIterator> it = jb.getElements("DFT/progress");
        ForEach(*it)
        {
            IPropertyTree &p=it->query();
            if (p.getPropInt("@done",0))
                done++;
            total++;
        }

        StringBuffer cmd;
        cmd.append(jb.queryProp("@command")).append(" ").append(jb.queryProp("@command_parameters"));

        Owned<IEspDFUJob> job = new CDFUJob("","");
        job->setTimeStarted(jb.queryProp("@time_started"));
        job->setDone(done);
        job->setTotal(total);
        job->setCommand(cmd.str());
        DFURecoveryJobs.append(*job.getClear());
    }
}

void CActivityInfo::getServerJobQueue(const char* queueName, const char* serverName,
    const char* serverType, const char* networkAddress, unsigned port)
{
    if (!queueName || !*queueName || !serverName || !*serverName || !serverType || !*serverType)
        return;

    Owned<IEspServerJobQueue> jobQueue = createServerJobQueue("", "");
    jobQueue->setQueueName(queueName);
    jobQueue->setServerName(serverName);
    jobQueue->setServerType(serverType);
    if (networkAddress && *networkAddress)
    {
        jobQueue->setNetworkAddress(networkAddress);
        jobQueue->setPort(port);
    }

    readServerJobQueueStatus(jobQueue);

    serverJobQueues.append(*jobQueue.getClear());
}

void CActivityInfo::readServerJobQueueStatus(IEspServerJobQueue* jobQueue)
{
    if (!jobQueueSnapshot)
    {
        WARNLOG("CActivityInfo::readServerJobQueueStatus: jobQueueSnapshot not found.");
        return;
    }

    StringBuffer queueStateDetails;
    bool jobQueueFound = false, hasRunning = false,  hasPaused =  false;

    StringArray qlist;
    qlist.appendListUniq(jobQueue->getQueueName(), ",");
    ForEachItemIn(i, qlist)
    {
        const char* qname = qlist.item(i);
        Owned<IJobQueueConst> queue = jobQueueSnapshot->getJobQueue(qname);
        if (!queue)
            continue;

        jobQueueFound = true;

        StringBuffer status, details;
        queue->getState(status, details);
        if (!status || !*status)
            continue;

        if (strieq(status.str(), "paused"))
            hasPaused =  true;
        else if (!strieq(status.str(), "stopped"))
            hasRunning =  true;

        if (details && *details)
            queueStateDetails.appendf("%s: queue %s; %s;", qname, status.str(), details.str());
        else
            queueStateDetails.appendf("%s: queue %s;", qname, status.str());
    }

    if (hasRunning)
        jobQueue->setQueueStatus("running");
    else if (hasPaused)
        jobQueue->setQueueStatus("paused");
    else
    {
        jobQueue->setQueueStatus("stopped");
        if (!jobQueueFound)
            queueStateDetails.setf("%s not found in Status Server list", jobQueue->getQueueName());
        else if (!queueStateDetails.length())
            queueStateDetails.setf("No status set in Status Server list for %s", jobQueue->getQueueName());
    }
    jobQueue->setStatusDetails(queueStateDetails.str());
}

bool CWsSMCEx::onIndex(IEspContext &context, IEspSMCIndexRequest &req, IEspSMCIndexResponse &resp)
{
    resp.setRedirectUrl("/");
    return true;
}

void CWsSMCEx::readBannerAndChatRequest(IEspContext& context, IEspActivityRequest &req, IEspActivityResponse& resp)
{
    StringBuffer chatURLStr, bannerStr;
    const char* chatURL = req.getChatURL();
    const char* banner = req.getBannerContent();
    //Filter out invalid chars
    if (chatURL && *chatURL)
    {
        const char* pStr = chatURL;
        unsigned len = strlen(chatURL);
        for (unsigned i = 0; i < len; i++)
        {
            if (isprint(*pStr))
                chatURLStr.append(*pStr);
            pStr++;
        }
    }
    if (banner && *banner)
    {
        const char* pStr = banner;
        unsigned len = strlen(banner);
        for (unsigned i = 0; i < len; i++)
        {
            bannerStr.append(isprint(*pStr) ? *pStr : '.');
            pStr++;
        }
    }
    chatURLStr.trim();
    bannerStr.trim();

    if (!req.getBannerAction_isNull() && req.getBannerAction() && (bannerStr.length() < 1))
        throw MakeStringException(ECLWATCH_MISSING_BANNER_CONTENT, "If a Banner is enabled, the Banner content must be specified.");

    if (!req.getEnableChatURL_isNull() && req.getEnableChatURL() && (chatURLStr.length() < 1))
        throw MakeStringException(ECLWATCH_MISSING_CHAT_URL, "If a Chat is enabled, the Chat URL must be specified.");

    //Now, store the strings since they are valid.
    m_ChatURL = chatURLStr;
    m_Banner = bannerStr;

    const char* bannerSize = req.getBannerSize();
    if (bannerSize && *bannerSize)
        m_BannerSize.set(bannerSize);

    const char* bannerColor = req.getBannerColor();
    if (bannerColor && *bannerColor)
        m_BannerColor.set(bannerColor);

    const char* bannerScroll = req.getBannerScroll();
    if (bannerScroll && *bannerScroll)
        m_BannerScroll.set(bannerScroll);

    m_BannerAction = req.getBannerAction();
    if(!req.getEnableChatURL_isNull())
        m_EnableChatURL = req.getEnableChatURL();
}

void CWsSMCEx::setBannerAndChatData(double version, IEspActivityResponse& resp)
{
    resp.setShowBanner(m_BannerAction);
    resp.setShowChatURL(m_EnableChatURL);
    resp.setBannerContent(m_Banner.str());
    resp.setBannerSize(m_BannerSize.str());
    resp.setBannerColor(m_BannerColor.str());
    resp.setChatURL(m_ChatURL.str());
    if (version >= 1.08)
        resp.setBannerScroll(m_BannerScroll.str());
}

void CWsSMCEx::sortTargetClusters(IArrayOf<IEspTargetCluster>& clusters, const char* sortBy, bool descending)
{
    if (!sortBy || !*sortBy || strieq(sortBy, "name"))
        clusters.sort(descending ? sortTargetClustersByNameDescending : sortTargetClustersByNameAscending);
    else
        clusters.sort(descending ? sortTargetClustersBySizeDescending : sortTargetClustersBySizeAscending);
}

void CWsSMCEx::getClusterQueueStatus(const CWsSMCTargetCluster& targetCluster, ClusterStatusType& queueStatusType, StringBuffer& queueStatusDetails)
{
    const CWsSMCQueue* jobQueue = &targetCluster.clusterQueue;
    if (targetCluster.clusterType != ThorLCRCluster)
        jobQueue = &targetCluster.agentQueue;
    if (!jobQueue->queueName.length())
        return;

    bool queuePausedOrStopped = false;
    //get queueStatusDetails
    if (targetCluster.clusterStatusDetails.length())
        queueStatusDetails.set(targetCluster.clusterStatusDetails.str());
    if (jobQueue->queueState.length())
    {
        const char* queueState = jobQueue->queueState.str();
        queueStatusDetails.appendf("%s: queue %s; ", jobQueue->queueName.str(), queueState);
        if (jobQueue->queueStateDetails.length())
            queueStatusDetails.appendf(" %s;", jobQueue->queueStateDetails.str());

        if (strieq(queueState,"stopped") || strieq(queueState,"paused"))
            queuePausedOrStopped = true;
    }

    //get queueStatusType
    if (!jobQueue->foundQueueInStatusServer)
    {
        if (queuePausedOrStopped)
            queueStatusType = QueuePausedOrStoppedNotFound;
        else
            queueStatusType = QueueRunningNotFound;
    }
    else if (!queuePausedOrStopped)
        queueStatusType = RunningNormal;
    else if (jobQueue->countRunningJobs > 0)
        queueStatusType = QueuePausedOrStoppedWithJobs;
    else
        queueStatusType = QueuePausedOrStoppedWithNoJob;

    return;
}

void CWsSMCEx::setClusterStatus(IEspContext& context, const CWsSMCTargetCluster& targetCluster, IEspTargetCluster* returnCluster)
{
    ClusterStatusType queueStatusType = RunningNormal;
    StringBuffer queueStatusDetails;
    getClusterQueueStatus(targetCluster, queueStatusType, queueStatusDetails);

    returnCluster->setClusterStatus(queueStatusType);
    //Set 'Warning' which may be displayed beside cluster name
    if (queueStatusType == QueueRunningNotFound)
        returnCluster->setWarning("Cluster not attached");
    else if (queueStatusType == QueuePausedOrStoppedNotFound)
        returnCluster->setWarning("Queue paused or stopped - Cluster not attached");
    else if (queueStatusType != RunningNormal)
        returnCluster->setWarning("Queue paused or stopped");
    //Set 'StatusDetails' which may be displayed when a mouse is moved over cluster icon
    if (queueStatusDetails.length())
        returnCluster->setStatusDetails(queueStatusDetails.str());
}

// This method reads job information from both /Status/Servers and IJobQueue.
//
// Each server component (a thor cluster, a dfuserver, or an eclagent) is one 'Server' branch under
// /Status/Servers. A 'Server' branch has a @queue which indicates the queue name of the server.
// A 'Server' branch also contains the information about running WUs on that 'Server'. This
// method reads the information. Those WUs are displays under that server (identified by its queue name)
// on Activity page.
//
// For the WUs list inside /Status/Servers/Server[@name=ECLagent] but not list under other 'Server', the
// existing code has to find out WUID and @clusterName of the WU. Then, uses @clusterName to find out the
// queue name in IConstWUClusterInfo. Those WUs list under that server (identified by its queue name) with
// a note 'on ECLagent'. TBD: the logic here will be simpler if the /Status/Servers/Server is named the
// instance and/or cluster.
//
// In order to get information about queued WUs, this method gets queue names from both IConstWUClusterInfo
// and other environment functions. Each of those queue names is linked to one IJobQueues. From the
// IJobQueues, this method reads queued jobs for each server component and list them under the server
// component (identified by its queue name).

bool CWsSMCEx::onActivity(IEspContext &context, IEspActivityRequest &req, IEspActivityResponse& resp)
{
    context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, true);

    try
    {
        const char* build_ver = getBuildVersion();
        resp.setBuild(build_ver);

        double version = context.getClientVersion();

        bool isSuperUser = true;
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
            isSuperUser =  false;
#endif
        if(isSuperUser && req.getFromSubmitBtn())
            readBannerAndChatRequest(context, req, resp);

        if (version >= 1.12)
            resp.setSuperUser(isSuperUser);
        if (version >= 1.06)
            setBannerAndChatData(version, resp);

        Owned<CActivityInfo> activityInfo = getActivityInfo(context);
        setActivityResponse(context, activityInfo, req, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CWsSMCEx::clearActivityInfoCache()
{
    CriticalBlock b(getActivityCrit);
    activityInfoCache.clear();
}

CActivityInfo* CWsSMCEx::getActivityInfo(IEspContext &context)
{
    CriticalBlock b(getActivityCrit);

    if (activityInfoCache && activityInfoCache->isCachedActivityInfoValid(activityInfoCacheSeconds))
        return activityInfoCache.getLink();

    DBGLOG("CWsSMCEx::getActivityInfo - rebuild cached information");
    {
        EspTimeSection timer("createActivityInfo");
        activityInfoCache.setown(new CActivityInfo());
        activityInfoCache->createActivityInfo(context);
    }

    return activityInfoCache.getLink();
}

void CWsSMCEx::addWUsToResponse(IEspContext &context, const IArrayOf<IEspActiveWorkunit>& aws, IEspActivityResponse& resp)
{
    const char* user = context.queryUserId();
    IArrayOf<IEspActiveWorkunit> awsReturned;
    ForEachItemIn(i, aws)
    {
        IEspActiveWorkunit& wu = aws.item(i);
        const char* wuid = wu.getWuid();
        if (wuid[0] == 'D')//DFU WU
        {
            awsReturned.append(*LINK(&wu));
            continue;
        }
        try
        {
            //if no access, throw an exception and go to the 'catch' section.
            const char* owner = wu.getOwner();
            context.validateFeatureAccess((!owner || !*owner || (user && streq(user, owner))) ? OWN_WU_ACCESS : OTHERS_WU_ACCESS, SecAccess_Read, true);

            awsReturned.append(*LINK(&wu));
            continue;
        }
        catch (IException *e)
        {   //if the wu cannot be opened for some reason, the openWorkUnit() inside the CActiveWorkunitWrapper() may throw an exception.
            //We do not want the exception stops this process of retrieving/showing all active WUs. And that WU should still be displayed
            //with the exception.
            StringBuffer msg;
            Owned<IEspActiveWorkunit> cw = new CActiveWorkunitWrapper(wuid, "", "", e->errorMessage(msg).str(), "normal");
            cw->setStateID(WUStateUnknown);
            cw->setServer(wu.getServer());
            cw->setQueueName(wu.getQueueName());
            const char* instanceName = wu.getInstance();
            const char* targetClusterName = wu.getTargetClusterName();
            if (instanceName && *instanceName)
                cw->setInstance(instanceName); // JCSMORE In thor case at least, if queued it is unknown which instance it will run on..
            if (targetClusterName && *targetClusterName)
                cw->setTargetClusterName(targetClusterName);
            awsReturned.append(*cw.getClear());

            e->Release();
        }
    }
    resp.setRunning(awsReturned);
    return;
}

void CWsSMCEx::setActivityResponse(IEspContext &context, CActivityInfo* activityInfo, IEspActivityRequest &req, IEspActivityResponse& resp)
{
    double version = context.getClientVersion();
    const char* sortBy = req.getSortBy();
    bool descending = req.getDescending();
    if (version >= 1.16)
    {
        IArrayOf<IEspTargetCluster> thorClusters;
        IArrayOf<IEspTargetCluster> hthorClusters;
        IArrayOf<IEspTargetCluster> roxieClusters;

        setESPTargetClusters(context, activityInfo->queryThorTargetClusters(), thorClusters);
        setESPTargetClusters(context, activityInfo->queryRoxieTargetClusters(), roxieClusters);
        setESPTargetClusters(context, activityInfo->queryHThorTargetClusters(), hthorClusters);

        sortTargetClusters(thorClusters, sortBy, descending);
        sortTargetClusters(roxieClusters, sortBy, descending);

        SecAccessFlags access;
        if (context.authorizeFeature(THORQUEUE_FEATURE, access) && access>=SecAccess_Full)
            resp.setAccessRight("Access_Full");

        resp.setSortBy(sortBy);
        resp.setDescending(descending);
        resp.setThorClusterList(thorClusters);
        resp.setRoxieClusterList(roxieClusters);
        resp.setHThorClusterList(hthorClusters);
        resp.setServerJobQueues(activityInfo->queryServerJobQueues());
    }
    else
    {//for backward compatible
        IArrayOf<IEspThorCluster> thorClusters;
        CIArrayOf<CWsSMCTargetCluster>& thorTargetClusters = activityInfo->queryThorTargetClusters();
        ForEachItemIn(i, thorTargetClusters)
        {
            CWsSMCTargetCluster& targetCluster = thorTargetClusters.item(i);
            Owned<IEspThorCluster> respThorCluster = new CThorCluster("", "");
            respThorCluster->setClusterName(targetCluster.clusterName.get());
            respThorCluster->setQueueStatus(targetCluster.queueStatus.get());
            if (version >= 1.03)
                respThorCluster->setQueueName(targetCluster.queueName.get());
            if (version >= 1.11)
                respThorCluster->setClusterSize(targetCluster.clusterSize);
            thorClusters.append(*respThorCluster.getClear());
        }
        resp.setThorClusters(thorClusters);

        if (version > 1.06)
        {
            IArrayOf<IEspRoxieCluster> roxieClusters;
            CIArrayOf<CWsSMCTargetCluster>& roxieTargetClusters = activityInfo->queryRoxieTargetClusters();
            ForEachItemIn(i, roxieTargetClusters)
            {
                CWsSMCTargetCluster& targetCluster = roxieTargetClusters.item(i);
                Owned<IEspRoxieCluster> respRoxieCluster = new CRoxieCluster("", "");
                respRoxieCluster->setClusterName(targetCluster.clusterName.get());
                respRoxieCluster->setQueueStatus(targetCluster.queueStatus.get());
                respRoxieCluster->setQueueName(targetCluster.queueName.get());
                if (version >= 1.11)
                    respRoxieCluster->setClusterSize(targetCluster.clusterSize);
                roxieClusters.append(*respRoxieCluster.getClear());
            }
            resp.setRoxieClusters(roxieClusters);
        }
        if (version > 1.10)
        {
            resp.setSortBy(sortBy);
            resp.setDescending(req.getDescending());
        }
        if (version > 1.11)
        {
            IArrayOf<IEspHThorCluster> hThorClusters;
            CIArrayOf<CWsSMCTargetCluster>& hthorTargetClusters = activityInfo->queryHThorTargetClusters();
            ForEachItemIn(i, hthorTargetClusters)
            {
                CWsSMCTargetCluster& targetCluster = hthorTargetClusters.item(i);
                Owned<IEspHThorCluster> respHThorCluster = new CHThorCluster("", "");
                respHThorCluster->setClusterName(targetCluster.clusterName.get());
                respHThorCluster->setQueueStatus(targetCluster.queueStatus.get());
                respHThorCluster->setQueueName(targetCluster.queueName.get());
                respHThorCluster->setClusterSize(targetCluster.clusterSize);
                hThorClusters.append(*respHThorCluster.getClear());
            }
            resp.setHThorClusters(hThorClusters);

            SecAccessFlags access;
            if (context.authorizeFeature(THORQUEUE_FEATURE, access) && access>=SecAccess_Full)
                resp.setAccessRight("Access_Full");
        }
        if (version > 1.03)
            resp.setServerJobQueues(activityInfo->queryServerJobQueues());
    }
    resp.setDFUJobs(activityInfo->queryDFURecoveryJobs());
    addWUsToResponse(context, activityInfo->queryActiveWUs(), resp);
    return;
}

void CWsSMCEx::setESPTargetClusters(IEspContext& context, const CIArrayOf<CWsSMCTargetCluster>& targetClusters, IArrayOf<IEspTargetCluster>& respTargetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        Owned<IEspTargetCluster> respTargetCluster = new CTargetCluster("", "");
        setESPTargetCluster(context, targetClusters.item(i), respTargetCluster);
        respTargetClusters.append(*respTargetCluster.getClear());
    }
}

void CWsSMCEx::addCapabilities(IPropertyTree* pFeatureNode, const char* access, 
                                         IArrayOf<IEspCapability>& capabilities)
{
    StringBuffer xpath(access);
    xpath.append("/Capability");

    Owned<IPropertyTreeIterator> it = pFeatureNode->getElements(xpath.str());
    ForEach(*it)
    {
        IPropertyTree* pCapabilityNode = &it->query();

        IEspCapability* pCapability = new CCapability("ws_smc");
        pCapability->setName( pCapabilityNode->queryProp("@name") );
        pCapability->setDescription( pCapabilityNode->queryProp("@description") );

        capabilities.append(*pCapability);
    }
}

static void checkAccess(IEspContext &context, const char* feature,int level)
{
    if (!context.validateFeatureAccess(feature, level, false))
        throw MakeStringException(ECLWATCH_THOR_QUEUE_ACCESS_DENIED, "Failed to access the queue functions. Permission denied.");
}


bool CWsSMCEx::onMoveJobDown(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            QueueLock lock(queue);
            unsigned index=queue->findRank(req.getWuid());
            if(index<queue->ordinality())
            {
                Owned<IJobQueueItem> item0 = queue->getItem(index);
                Owned<IJobQueueItem> item = queue->getItem(index+1);
                if(item && item0 && (item0->getPriority() == item->getPriority()))
                    queue->moveAfter(req.getWuid(),item->queryWUID());
            }
        }
        AccessSuccess(context, "Changed job priority %s",req.getWuid());
        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onMoveJobUp(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            QueueLock lock(queue);
            unsigned index=queue->findRank(req.getWuid());
            if(index>0 && index<queue->ordinality())
            {
                Owned<IJobQueueItem> item0 = queue->getItem(index);
                Owned<IJobQueueItem> item = queue->getItem(index-1);
                if(item && item0 && (item0->getPriority() == item->getPriority()))
                    queue->moveBefore(req.getWuid(),item->queryWUID());
            }
        }
        AccessSuccess(context, "Changed job priority %s",req.getWuid());
        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onMoveJobBack(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            QueueLock lock(queue);

            unsigned index=queue->findRank(req.getWuid());
            if(index<queue->ordinality())
            {
                Owned<IJobQueueItem> item = queue->getItem(index);
                int priority0 = item->getPriority();
                unsigned biggestIndoxInSamePriority = index;
                unsigned nextIndex = biggestIndoxInSamePriority + 1;
                while (nextIndex<queue->ordinality())
                {
                    item.setown(queue->getItem(nextIndex));
                    if (priority0 != item->getPriority())
                    {
                        break;
                    }
                    biggestIndoxInSamePriority = nextIndex;
                    nextIndex++;
                }

                if (biggestIndoxInSamePriority != index)
                {
                    item.setown(queue->getItem(biggestIndoxInSamePriority));
                    queue->moveAfter(req.getWuid(), item->queryWUID());
                }
            }
        }
        AccessSuccess(context, "Changed job priority %s",req.getWuid());
        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onMoveJobFront(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue=createJobQueue(req.getQueueName());
            QueueLock lock(queue);

            unsigned index=queue->findRank(req.getWuid());
            if (index>0 && index<queue->ordinality())
            {
                Owned<IJobQueueItem> item = queue->getItem(index);
                int priority0 = item->getPriority();
                unsigned smallestIndoxInSamePriority = index;
                int nextIndex = smallestIndoxInSamePriority - 1;
                while (nextIndex >= 0)
                {
                    item.setown(queue->getItem(nextIndex));
                    if (priority0 != item->getPriority())
                    {
                        break;
                    }
                    smallestIndoxInSamePriority = nextIndex;
                    nextIndex--;
                }

                if (smallestIndoxInSamePriority != index)
                {
                    item.setown(queue->getItem(smallestIndoxInSamePriority));
                    queue->moveBefore(req.getWuid(), item->queryWUID());
                }
            }
        }

        AccessSuccess(context, "Changed job priority %s",req.getWuid());
        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onRemoveJob(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        secAbortWorkUnit(req.getWuid(), *context.querySecManager(), *context.queryUser());

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            QueueLock lock(queue);

            unsigned index=queue->findRank(req.getWuid());
            if(index<queue->ordinality())
            {
                if(!queue->cancelInitiateConversation(req.getWuid()))
                    throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Failed to remove the workunit %s",req.getWuid());
            }
        }
        AccessSuccess(context, "Removed job %s",req.getWuid());
        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onStopQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            StringBuffer info;
            queue->stop(createQueueActionInfo(context, "stopped", req, info));
        }
        AccessSuccess(context, "Stopped queue %s", req.getCluster());
        clearActivityInfoCache();
        double version = context.getClientVersion();
        if (version >= 1.19)
            getStatusServerInfo(context, req.getServerType(), req.getCluster(), req.getNetworkAddress(), req.getPort(), resp.updateStatusServerInfo());

        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onResumeQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            StringBuffer info;
            queue->resume(createQueueActionInfo(context, "resumed", req, info));
        }
        AccessSuccess(context, "Resumed queue %s", req.getCluster());
        clearActivityInfoCache();
        double version = context.getClientVersion();
        if (version >= 1.19)
            getStatusServerInfo(context, req.getServerType(), req.getCluster(), req.getNetworkAddress(), req.getPort(), resp.updateStatusServerInfo());

        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

const char* CWsSMCEx::createQueueActionInfo(IEspContext &context, const char* state, IEspSMCQueueRequest &req, StringBuffer& info)
{
    StringBuffer peer, currentTime;
    context.getPeer(peer);
    const char* userId = context.queryUserId();
    if (!userId || !*userId)
        userId = "Unknown user";
    CDateTime now;
    now.setNow();
    now.getString(currentTime);
    info.appendf("%s by <%s> at <%s> from <%s>", state, userId, currentTime.str(), peer.str());
    const char* comment = req.getComment();
    if (comment && *comment)
        info.append(": ' ").append(comment).append("'");
    return info.str();
}

bool CWsSMCEx::onPauseQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            StringBuffer info;
            queue->pause(createQueueActionInfo(context, "paused", req, info));
        }
        AccessSuccess(context, "Paused queue %s", req.getCluster());
        clearActivityInfoCache();
        double version = context.getClientVersion();
        if (version >= 1.19)
            getStatusServerInfo(context, req.getServerType(), req.getCluster(), req.getNetworkAddress(), req.getPort(), resp.updateStatusServerInfo());

        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onClearQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);
        {
            Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
            QueueLock lock(queue);
            for(unsigned i=0;i<queue->ordinality();i++)
            {
                Owned<IJobQueueItem> item = queue->getItem(i);
                secAbortWorkUnit(item->queryWUID(), *context.querySecManager(), *context.queryUser());
            }
            queue->clear();
        }
        AccessSuccess(context, "Cleared queue %s",req.getCluster());
        clearActivityInfoCache();
        double version = context.getClientVersion();
        if (version >= 1.19)
            getStatusServerInfo(context, req.getServerType(), req.getCluster(), req.getNetworkAddress(), req.getPort(), resp.updateStatusServerInfo());

        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsSMCEx::setJobPriority(IEspContext &context, IWorkUnitFactory* factory, const char* wuid, const char* queueName, WUPriorityClass& priority)
{
    if (!wuid || !*wuid)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not specified.");
    if (!queueName || !*queueName)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "queue not specified.");

    Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid, context.querySecManager(), context.queryUser());
    if (!lw)
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Cannot update Workunit %s", wuid);

    lw->setPriority(priority);

    // set job priority to queue
    int priorityValue = lw->getPriorityValue();
    {
        CriticalBlock b(crit);
        Owned<IJobQueue> queue = createJobQueue(queueName);
        QueueLock lock(queue);
        queue->changePriority(wuid,priorityValue);
    }

    return;
}

bool CWsSMCEx::onSetJobPriority(IEspContext &context, IEspSMCPriorityRequest &req, IEspSMCPriorityResponse &resp)
{
    try
    {
        WUPriorityClass priority = PriorityClassNormal;
        if(strieq(req.getPriority(),"high"))
            priority = PriorityClassHigh;
        else if(strieq(req.getPriority(),"low"))
            priority = PriorityClassLow;

        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            IArrayOf<IConstSMCJob>& jobs = req.getSMCJobs();
            if (!jobs.length())
                setJobPriority(context, factory, req.getWuid(), req.getQueueName(), priority);
            else
            {
                ForEachItemIn(i, jobs)
                {
                    IConstSMCJob &item = jobs.item(i);
                    const char *wuid = item.getWuid();
                    const char *queueName = item.getQueueName();
                    if (wuid && *wuid && queueName && *queueName)
                        setJobPriority(context, factory, wuid, queueName, priority);
                }
            }
        }

        clearActivityInfoCache();
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onGetThorQueueAvailability(IEspContext &context, IEspGetThorQueueAvailabilityRequest &req, IEspGetThorQueueAvailabilityResponse& resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_SMC_ACCESS_DENIED, "Failed to get Thor Queue availability. Permission denied.");

        StringArray thorNames, groupNames, targetNames, queueNames;
        getEnvironmentThorClusterNames(thorNames, groupNames, targetNames, queueNames);

        IArrayOf<IEspThorCluster> ThorClusters;
        ForEachItemIn(x, thorNames)
        {
            const char* targetName = targetNames.item(x);
            const char* queueName = queueNames.item(x);
            IEspThorCluster* returnCluster = new CThorCluster("","");
                
            returnCluster->setClusterName(targetName);
            returnCluster->setQueueName(queueName);

            StringBuffer info;
            Owned<IJobQueue> queue = createJobQueue(queueName);
            if(queue->stopped(info))
                returnCluster->setQueueStatus("stopped");
            else if (queue->paused(info))
                returnCluster->setQueueStatus("paused");
            else
                returnCluster->setQueueStatus("running");

            unsigned enqueued=0;
            unsigned connected=0;
            unsigned waiting=0;
            queue->getStats(connected,waiting,enqueued);
            returnCluster->setQueueAvailable(waiting);
            returnCluster->setJobsRunning(connected - waiting);
            returnCluster->setJobsInQueue(enqueued);

            ThorClusters.append(*returnCluster);
        }

        resp.setThorClusters(ThorClusters);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onSetBanner(IEspContext &context, IEspSetBannerRequest &req, IEspSetBannerResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(!secmgr || !secmgr->isSuperUser(context.queryUser()))
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "access denied, administrators only.");
#endif
        StringBuffer chatURLStr, bannerStr;
        const char* chatURL = req.getChatURL();
        const char* banner = req.getBannerContent();
        //Only display valid strings
        if (chatURL)
        {
            const char* pStr = chatURL;
            for (unsigned i = 0; i < strlen(chatURL); i++)
            {
                if ((pStr[0] > 31) && (pStr[0] < 127))
                    chatURLStr.append(pStr[0]);
                pStr++;
            }
        }
        if (banner)
        {
            const char* pStr = banner;
            for (unsigned i = 0; i < strlen(banner); i++)
            {
                if ((pStr[0] > 31) && (pStr[0] < 127))
                    bannerStr.append(pStr[0]);
                pStr++;
            }
        }
        chatURLStr.trim();
        bannerStr.trim();

        if (!req.getBannerAction_isNull() && req.getBannerAction() && (bannerStr.length() < 1))
        {
            throw MakeStringException(ECLWATCH_MISSING_BANNER_CONTENT, "If a Banner is enabled, the Banner content must be specified.");
        }

        if (!req.getEnableChatURL_isNull() && req.getEnableChatURL() && (!req.getChatURL() || !*req.getChatURL()))
        {
            throw MakeStringException(ECLWATCH_MISSING_CHAT_URL, "If a Chat is enabled, the Chat URL must be specified.");
        }

        m_ChatURL = chatURLStr;
        m_Banner = bannerStr;

        const char* bannerSize = req.getBannerSize();
        if (bannerSize && *bannerSize)
            m_BannerSize.clear().append(bannerSize);

        const char* bannerColor = req.getBannerColor();
        if (bannerColor && *bannerColor)
            m_BannerColor.clear().append(bannerColor);

        const char* bannerScroll = req.getBannerScroll();
        if (bannerScroll && *bannerScroll)
            m_BannerScroll.clear().append(bannerScroll);

        m_BannerAction = 0;
        if(!req.getBannerAction_isNull())
            m_BannerAction = req.getBannerAction();

        m_EnableChatURL = 0;
        if(!req.getEnableChatURL_isNull())
            m_EnableChatURL = req.getEnableChatURL();

        resp.setRedirectUrl("/WsSMC/Activity");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onNotInCommunityEdition(IEspContext &context, IEspNotInCommunityEditionRequest &req, IEspNotInCommunityEditionResponse &resp)
{
   return true;
}

bool CWsSMCEx::onBrowseResources(IEspContext &context, IEspBrowseResourcesRequest & req, IEspBrowseResourcesResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_SMC_ACCESS_DENIED, "Failed to Browse Resources. Permission denied.");

        double version = context.getClientVersion();

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = factory->openEnvironment();

        //The resource files will be downloaded from the same box of ESP (not dali)
        StringBuffer ipStr;
        IpAddress ipaddr = queryHostIP();
        ipaddr.getIpText(ipStr);
        if (ipStr.length() > 0)
        {
            resp.setNetAddress(ipStr.str());
            Owned<IConstMachineInfo> machine = constEnv->getMachineByAddress(ipStr.str());
            if (machine)
            {
                int os = machine->getOS();
                resp.setOS(os);
            }
        }

        if (m_PortalURL.length() > 0)
            resp.setPortalURL(m_PortalURL.str());

#ifndef USE_RESOURCE
        if (version > 1.12)
            resp.setUseResource(false);
#else
        if (version > 1.12)
            resp.setUseResource(true);

        //Now, get a list of resources stored inside the ESP box
        IArrayOf<IEspHPCCResourceRepository> resourceRepositories;

        Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
        const char* ossInstall = pEnvRoot->queryProp("EnvSettings/path");
        if (!ossInstall || !*ossInstall)
        {
            WARNLOG("Failed to get EnvSettings/Path in environment settings.");
            return true;
        }

        StringBuffer path;
        path.appendf("%s/componentfiles/files/downloads", ossInstall);
        Owned<IFile> f = createIFile(path.str());
        if(!f->exists() || !f->isDirectory())
        {
            WARNLOG("Invalid resource folder");
            return true;
        }

        Owned<IDirectoryIterator> di = f->directoryFiles(NULL, false, true);
        if(di.get() == NULL)
        {
            WARNLOG("Resource folder is empty.");
            return true;
        }

        ForEach(*di)
        {
            if (!di->isDir())
                continue;

            StringBuffer folder, path0, tmpBuf;
            di->getName(folder);
            if (folder.length() == 0)
                continue;

            path0.appendf("%s/%s/description.xml", path.str(), folder.str());
            Owned<IFile> f0 = createIFile(path0.str());
            if(!f0->exists())
            {
                WARNLOG("Description file not found for %s", folder.str());
                continue;
            }

            OwnedIFileIO rIO = f0->openShared(IFOread,IFSHfull);
            if(!rIO)
            {
                WARNLOG("Failed to open the description file for %s", folder.str());
                continue;
            }

            offset_t fileSize = f0->size();
            tmpBuf.ensureCapacity((unsigned)fileSize);
            tmpBuf.setLength((unsigned)fileSize);

            size32_t nRead = rIO->read(0, (size32_t) fileSize, (char*)tmpBuf.str());
            if (nRead != fileSize)
            {
                WARNLOG("Failed to read the description file for %s", folder.str());
                continue;
            }

            Owned<IPropertyTree> desc = createPTreeFromXMLString(tmpBuf.str());
            if (!desc)
            {
                WARNLOG("Invalid description file for %s", folder.str());
                continue;
            }

            Owned<IPropertyTreeIterator> fileIterator = desc->getElements("file");
            if (!fileIterator->first())
            {
                WARNLOG("Invalid description file for %s", folder.str());
                continue;
            }

            IArrayOf<IEspHPCCResource> resourcs;

            do {
                IPropertyTree &fileItem = fileIterator->query();
                const char* filename = fileItem.queryProp("filename");
                if (!filename || !*filename)
                    continue;

                const char* name0 = fileItem.queryProp("name");
                const char* description0 = fileItem.queryProp("description");
                const char* version0 = fileItem.queryProp("version");

                Owned<IEspHPCCResource> onefile = createHPCCResource();
                onefile->setFileName(filename);
                if (name0 && *name0)
                    onefile->setName(name0);
                if (description0 && *description0)
                    onefile->setDescription(description0);
                if (version0 && *version0)
                    onefile->setVersion(version0);

                resourcs.append(*onefile.getLink());
            } while (fileIterator->next());

            if (resourcs.ordinality())
            {
                StringBuffer path1;
                path1.appendf("%s/%s", path.str(), folder.str());

                Owned<IEspHPCCResourceRepository> oneRepository = createHPCCResourceRepository();
                oneRepository->setName(folder.str());
                oneRepository->setPath(path1.str());
                oneRepository->setHPCCResources(resourcs);

                resourceRepositories.append(*oneRepository.getLink());
            }
        }

        if (resourceRepositories.ordinality())
            resp.setHPCCResourceRepositories(resourceRepositories);
#endif
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

int CWsSMCSoapBindingEx::onHttpEcho(CHttpRequest* request,  CHttpResponse* response)
{
    StringBuffer xml;
    xml.append(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
          "<soap:Body>"
            "<HttpEchoResponse xmlns='urn:hpccsystems:ws:httpecho'>");

    appendXMLTag(xml, "Method", request->queryMethod());
    appendXMLTag(xml, "UrlPath", request->queryPath());
    appendXMLTag(xml, "UrlParameters", request->queryParamStr());

    appendXMLOpenTag(xml, "Headers");
    StringArray &headers = request->queryHeaders();
    headers.sortAscii(false);
    ForEachItemIn(i, headers)
    {
        const char *h = headers.item(i);
        if (strnicmp(h, "Authorization", 13))
            appendXMLTag(xml, "Header", h);
    }
    appendXMLCloseTag(xml, "Headers");

    const char *content = request->queryContent();
    if (content && *content)
        appendXMLTag(xml, "Content", content);
    xml.append("</HttpEchoResponse></soap:Body></soap:Envelope>");

    response->setContent(xml);
    response->setContentType("text/xml");
    response->send();
    return 0;
}


int CWsSMCSoapBindingEx::onGet(CHttpRequest* request,  CHttpResponse* response)
{
    const char *operation = request->queryServiceMethod();
    if (!operation || !strieq(operation, "HttpEcho"))
        return CWsSMCSoapBinding::onGet(request, response);

    return onHttpEcho(request, response);
}

void CWsSMCSoapBindingEx::handleHttpPost(CHttpRequest *request, CHttpResponse *response)
{
    sub_service sstype;
    StringBuffer operation;
    request->getEspPathInfo(sstype, NULL, NULL, &operation, false);
    if (!operation || !strieq(operation, "HttpEcho"))
        CWsSMCSoapBinding::handleHttpPost(request, response);
    else
        onHttpEcho(request, response);
}


int CWsSMCSoapBindingEx::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    try
    {
        if(stricmp(method,"NotInCommunityEdition")==0)
        {
            StringBuffer page, url, link;
            request->getParameter("EEPortal", url);
            if (url.length() > 0)
                link.appendf("Further information can be found at <a href=\"%s\" target=\"_blank\">%s</a>.", url.str(), url.str());

            page.append(
                "<html>"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/default.css\"/>"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/yui/build/fonts/fonts-min.css\" />"
                        "<title>Advanced feature in Enterprise Edition</title>"
                    "</head>"
                    "<body>"
                        "<h3 style=\"text-align:centre;\">Advanced feature in the Enterprise Edition</h4>"
                        "<p style=\"text-align:centre;\">Support for this feature is coming soon. ");
            if (link.length() > 0)
                page.append(link.str());
            page.append("</p></body>"
                "</html>");

            response->setContent(page.str());
            response->setContentType("text/html");
            response->send();
            return 0;
        }
        else if(stricmp(method,"DisabledInThisVersion")==0)
        {
            StringBuffer page;
            page.append(
                "<html>"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/default.css\"/>"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/yui/build/fonts/fonts-min.css\" />"
                        "<title>Disabled Feature in This Version</title>"
                    "</head>"
                    "<body>"
                        "<h3 style=\"text-align:centre;\">Disabled Feature in This Version</h4>"
                        "<p style=\"text-align:centre;\">This feature is disabled in this version. ");
            page.append("</p></body>"
                "</html>");

            response->setContent(page.str());
            response->setContentType("text/html");
            response->send();
            return 0;
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onGetForm(context, request, response, service, method);
}

inline const char *controlCmdMessage(int cmd)
{
    switch (cmd)
    {
    case CRoxieControlCmd_ATTACH:
        return "<control:unlockDali/>";
    case CRoxieControlCmd_DETACH:
        return "<control:lockDali/>";
    case CRoxieControlCmd_RELOAD:
        return "<control:reload/>";
    case CRoxieControlCmd_RELOAD_RETRY:
        return "<control:reload forceRetry='1' />";
    case CRoxieControlCmd_STATE:
        return "<control:state/>";
    default:
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Unknown Roxie Control Command.");
    }
    return NULL;
}

bool CWsSMCEx::onRoxieControlCmd(IEspContext &context, IEspRoxieControlCmdRequest &req, IEspRoxieControlCmdResponse &resp)
{
    if (!context.validateFeatureAccess(ROXIE_CONTROL_URL, SecAccess_Full, false))
       throw MakeStringException(ECLWATCH_SMC_ACCESS_DENIED, "Cannot Access Roxie Control. Permission denied.");

    const char *process = req.getProcessCluster();
    if (!process || !*process)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Process cluster not specified.");
    const char *controlReq = controlCmdMessage(req.getCommand());

    SocketEndpointArray addrs;
    getRoxieProcessServers(process, addrs);
    if (!addrs.length())
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Process cluster not found.");
    Owned<IPropertyTree> controlResp = sendRoxieControlAllNodes(addrs.item(0), controlReq, true, req.getWait());
    if (!controlResp)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get control response from roxie.");

    IArrayOf<IEspRoxieControlEndpointInfo> respEndpoints;
    Owned<IPropertyTreeIterator> roxieEndpoints = controlResp->getElements("Endpoint");
    ForEach(*roxieEndpoints)
    {
        IPropertyTree &roxieEndpoint = roxieEndpoints->query();
        Owned<IEspRoxieControlEndpointInfo> respEndpoint = createRoxieControlEndpointInfo();
        respEndpoint->setAddress(roxieEndpoint.queryProp("@ep"));
        respEndpoint->setStatus(roxieEndpoint.queryProp("Status"));
        if (roxieEndpoint.hasProp("Dali/@connected"))
            respEndpoint->setAttached(roxieEndpoint.getPropBool("Dali/@connected"));
        if (roxieEndpoint.hasProp("State/@hash"))
            respEndpoint->setStateHash(roxieEndpoint.queryProp("State/@hash"));
        respEndpoints.append(*respEndpoint.getClear());
    }
    resp.setEndpoints(respEndpoints);
    return true;
}

bool CWsSMCEx::onGetStatusServerInfo(IEspContext &context, IEspGetStatusServerInfoRequest &req, IEspGetStatusServerInfoResponse &resp)
{
    getStatusServerInfo(context, req.getServerType(), req.getServerName(), req.getNetworkAddress(), req.getPort(), resp.updateStatusServerInfo());
    return true;
}

void CWsSMCEx::getStatusServerInfo(IEspContext &context, const char *serverType, const char *server, const char *networkAddress, unsigned port,
    IEspStatusServerInfo& statusServerInfo)
{
    if (!serverType || !*serverType)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Server type not specified.");

    Owned<CActivityInfo> activityInfo = getActivityInfo(context);
    if (!activityInfo)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get Activity Info cache.");

    if (strieq(serverType,STATUS_SERVER_THOR))
    {
        setTargetClusterInfo(context, serverType, server, activityInfo->queryThorTargetClusters(), activityInfo->queryActiveWUs(), statusServerInfo);
    }
    else if (strieq(serverType,STATUS_SERVER_ROXIE))
    {
        setTargetClusterInfo(context, serverType, server, activityInfo->queryRoxieTargetClusters(), activityInfo->queryActiveWUs(), statusServerInfo);
    }
    else if (strieq(serverType,STATUS_SERVER_HTHOR))
    {
        setTargetClusterInfo(context, serverType, server, activityInfo->queryHThorTargetClusters(), activityInfo->queryActiveWUs(), statusServerInfo);
    }
    else if (strieq(serverType,STATUS_SERVER_DFUSERVER))
    {
        setServerQueueInfo(context, serverType, server, activityInfo->queryServerJobQueues(), activityInfo->queryActiveWUs(), statusServerInfo);
    }
    else
    {
        setServerQueueInfo(context, serverType, networkAddress, port, activityInfo->queryServerJobQueues(), activityInfo->queryActiveWUs(), statusServerInfo);
    }
}

void CWsSMCEx::setTargetClusterInfo(IEspContext &context, const char *serverType, const char *clusterName, const CIArrayOf<CWsSMCTargetCluster>& targetClusters,
    const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo)
{
    if (!clusterName || !*clusterName)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Cluster name not specified.");

    IEspTargetCluster& clusterInfo = statusServerInfo.updateTargetClusterInfo();
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        const char* name = targetCluster.clusterName.get();
        if (name && strieq(name, clusterName))
        {
            setESPTargetCluster(context, targetCluster, &clusterInfo);
            break;
        }
    }

    setActiveWUs(context, serverType, clusterName, clusterInfo.getQueueName(), aws, statusServerInfo);
}

void CWsSMCEx::setServerQueueInfo(IEspContext &context, const char *serverType, const char *serverName, const IArrayOf<IEspServerJobQueue>& serverJobQueues,
    const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo)
{
    if (!serverName || !*serverName)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Server name not specified.");

    ForEachItemIn(i, serverJobQueues)
    {
        IEspServerJobQueue& serverJobQueue = serverJobQueues.item(i);
        const char* name = serverJobQueue.getServerName();
        if (name && strieq(name, serverName))
        {
            IEspServerJobQueue& serverQueue = statusServerInfo.updateServerInfo();
            serverQueue.copy(serverJobQueue);
            break;
        }
    }

    setActiveWUs(context, serverType, serverName, aws, statusServerInfo);
}

void CWsSMCEx::setServerQueueInfo(IEspContext &context, const char *serverType, const char *networkAddress, unsigned port, const IArrayOf<IEspServerJobQueue>& serverJobQueues,
    const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo)
{
    if (!networkAddress || !*networkAddress)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Network address not specified.");

    ForEachItemIn(i, serverJobQueues)
    {
        IEspServerJobQueue& serverJobQueue = serverJobQueues.item(i);
        const char* ipAddress = serverJobQueue.getNetworkAddress();
        unsigned thePort = serverJobQueue.getPort();
        if (ipAddress && strieq(ipAddress, networkAddress) && (thePort == port))
        {
            IEspServerJobQueue& serverQueue = statusServerInfo.updateServerInfo();
            serverQueue.copy(serverJobQueue);
            break;
        }
    }

    VStringBuffer instance("%s_on_%s:%d", serverType, networkAddress, port);
    setActiveWUs(context, serverType, instance.str(), aws, statusServerInfo);
}

void CWsSMCEx::setESPTargetCluster(IEspContext &context, const CWsSMCTargetCluster& targetCluster, IEspTargetCluster* espTargetCluster)
{
    espTargetCluster->setClusterName(targetCluster.clusterName.get());
    espTargetCluster->setClusterSize(targetCluster.clusterSize);
    espTargetCluster->setClusterType(targetCluster.clusterType);
    espTargetCluster->setQueueName(targetCluster.queueName.get());
    espTargetCluster->setQueueStatus(targetCluster.queueStatus.get());
    setClusterStatus(context, targetCluster, espTargetCluster);
}

void CWsSMCEx::setActiveWUs(IEspContext &context, const char *serverType, const char *clusterName, const char *queueName, const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo)
{
    const char* clusterType = CLUSTER_TYPE_THOR;
    if (strieq(serverType,STATUS_SERVER_ROXIE))
        clusterType = CLUSTER_TYPE_ROXIE;
    else if (strieq(serverType,STATUS_SERVER_HTHOR))
        clusterType = CLUSTER_TYPE_HTHOR;

    IArrayOf<IEspActiveWorkunit> awsOnThisQueue;
    ForEachItemIn(i, aws)
    {
        IEspActiveWorkunit& wu = aws.item(i);
        const char* wuid = wu.getWuid();
        if (!wuid || !*wuid)
            continue;

        const char* wuServerType = wu.getServer();
        const char* wuClusterName = wu.getTargetClusterName();
        if (!wuServerType || !wuClusterName || !strieq(serverType, wuServerType) || !strieq(clusterName, wuClusterName))
        {
            const char* wuClusterType = wu.getClusterType();
            const char* wuClusterQueueName = wu.getClusterQueueName();
            if (!wuClusterType || !wuClusterQueueName || !strieq(clusterType, wuClusterType) || !strieq(queueName, wuClusterQueueName))
                continue;
        }

        Owned<IEspActiveWorkunit> wuOnThisQueue = new CActiveWorkunitWrapper(wuid, "", "", "", "");
        setActiveWUs(context, wu, wuOnThisQueue);
        awsOnThisQueue.append(*wuOnThisQueue.getClear());
    }
    statusServerInfo.setWorkunits(awsOnThisQueue);
}

void CWsSMCEx::setActiveWUs(IEspContext &context, const char *serverType, const char *instance, const IArrayOf<IEspActiveWorkunit>& aws, IEspStatusServerInfo& statusServerInfo)
{
    IArrayOf<IEspActiveWorkunit> awsOnThisQueue;
    ForEachItemIn(i, aws)
    {
        IEspActiveWorkunit& wu = aws.item(i);
        const char* wuid = wu.getWuid();
        if (!wuid || !*wuid)
            continue;

        const char* wuInstance = wu.getInstance();
        if (!wuInstance || !strieq(wuInstance, instance))
            continue;

        Owned<IEspActiveWorkunit> wuOnThisQueue = new CActiveWorkunitWrapper(wuid, "", "", "", "");
        setActiveWUs(context, wu, wuOnThisQueue);
        awsOnThisQueue.append(*wuOnThisQueue.getClear());
    }
    statusServerInfo.setWorkunits(awsOnThisQueue);
}

void CWsSMCEx::setActiveWUs(IEspContext &context, IEspActiveWorkunit& wu, IEspActiveWorkunit* wuToSet)
{
    try
    {
        const char* user = context.queryUserId();
        const char* owner = wu.getOwner();

        //if no access, throw an exception and go to the 'catch' section.
        context.validateFeatureAccess((!owner || !*owner || (user && streq(user, owner))) ? OWN_WU_ACCESS : OTHERS_WU_ACCESS, SecAccess_Read, true);
        wuToSet->copy(wu);
    }
    catch (IException *e)
    {   //if the wu cannot be opened for some reason, the openWorkUnit() inside the CActiveWorkunitWrapper() may throw an exception.
        //We do not want the exception stops this process of retrieving/showing all active WUs. And that WU should still be displayed
        //with the exception.
        wuToSet->setStateID(WUStateUnknown);
        wuToSet->setServer(wu.getServer());
        wuToSet->setQueueName(wu.getQueueName());
        const char* instanceName = wu.getInstance();
        const char* targetClusterName = wu.getTargetClusterName();
        if (instanceName && *instanceName)
            wuToSet->setInstance(instanceName); // JCSMORE In thor case at least, if queued it is unknown which instance it will run on..
        if (targetClusterName && *targetClusterName)
            wuToSet->setTargetClusterName(targetClusterName);

        e->Release();
    }
}

static const char *LockModeNames[] = { "ALL", "READ", "WRITE", "HOLD", "SUB" };

void CWsSMCEx::addLockInfo(CLockMetaData& lD, const char* xPath, const char* lfn, unsigned msNow, time_t ttNow, IArrayOf<IEspLock>& locks)
{
    Owned<IEspLock> lock = createLock();
    if (xPath && *xPath)
        lock->setXPath(xPath);
    else if (lfn && *lfn)
        lock->setLogicalFile(lfn);
    else
        return; //Should not happen
    lock->setEPIP(lD.queryEp());
    lock->setSessionID(lD.sessId);

    unsigned duration = msNow-lD.timeLockObtained;
    lock->setDurationMS(duration);

    CDateTime timeLocked;
    StringBuffer timeStr;
    time_t ttLocked = ttNow - duration/1000;
    timeLocked.set(ttLocked);
    timeLocked.getString(timeStr);
    lock->setTimeLocked(timeStr.str());

    unsigned mode = lD.mode;
    VStringBuffer modeStr("%x", mode);
    lock->setModes(modeStr.str());

    StringArray modes;
    if (RTM_MODE(mode, RTM_LOCK_READ))
        modes.append(LockModeNames[CLockModes_READ]);
    if (RTM_MODE(mode, RTM_LOCK_WRITE))
        modes.append(LockModeNames[CLockModes_WRITE]);
    if (RTM_MODE(mode, RTM_LOCK_HOLD)) // long-term lock
        modes.append(LockModeNames[CLockModes_HOLD]);
    if (RTM_MODE(mode, RTM_LOCK_SUB)) // locks all descendants as well as self
        modes.append(LockModeNames[CLockModes_SUB]);
    lock->setModeNames(modes);
    locks.append(*lock.getClear());
}

bool CWsSMCEx::onLockQuery(IEspContext &context, IEspLockQueryRequest &req, IEspLockQueryResponse &resp)
{
    class CLockPostFilter
    {
        CLockModes mode;
        time_t ttLTLow, ttLTHigh;
        bool checkLTLow, checkLTHigh;
        int durationLow, durationHigh;

        bool checkMode(unsigned lockMode)
        {
            unsigned modeReq;
            switch (mode)
            {
            case CLockModes_READ:
                modeReq = RTM_LOCK_READ;
                break;
            case CLockModes_WRITE:
                modeReq = RTM_LOCK_WRITE;
                break;
            case CLockModes_HOLD:
                modeReq = RTM_LOCK_HOLD;
                break;
            case CLockModes_SUB:
                modeReq = RTM_LOCK_SUB;
                break;
            default:
                return true;
            }
            if (lockMode & modeReq)
                return true;

            return false;
        }
    public:
        CLockPostFilter(IEspLockQueryRequest& req)
        {
            ttLTLow = 0;
            ttLTHigh = 0;
            mode = req.getMode();
            if (mode == LockModes_Undefined)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Lock Mode");

            if (req.getDurationMSLow_isNull())
                durationLow = -1;
            else
                durationLow = req.getDurationMSLow();
            if (req.getDurationMSHigh_isNull())
                durationHigh = -1;
            else
                durationHigh = req.getDurationMSHigh();
            const char* timeLow = req.getTimeLockedLow();
            if (!timeLow || !*timeLow)
                checkLTLow = false;
            else
            {
                CDateTime dtLow;
                dtLow.setString(timeLow, NULL, false);
                ttLTLow = dtLow.getSimple();
                checkLTLow = true;
            }
            const char* timeHigh = req.getTimeLockedHigh();
            if (!timeHigh || !*timeHigh)
                checkLTHigh = false;
            else
            {
                CDateTime dtHigh;
                dtHigh.setString(timeHigh, NULL, false);
                ttLTHigh = dtHigh.getSimple();
                checkLTHigh = true;
            }
        }
        bool check(CLockMetaData& lD, unsigned msNow, time_t ttNow)
        {
            if (!checkMode(lD.mode))
                return false;

            int duration = msNow-lD.timeLockObtained;
            if (durationLow > duration)
                return false;
            if ((durationHigh >= 0) && (durationHigh < duration))
                return false;

            if (checkLTLow && (ttNow - duration/1000 < ttLTLow))
                return false;
            if (checkLTHigh && (ttNow - duration/1000 > ttLTHigh))
                return false;

            return true;
        }
    };

    try
    {
        CLockPostFilter postFilter(req);
        StringBuffer xPath;
        if (req.getAllFileLocks())
            xPath.appendf("/%s/*", querySdsFilesRoot());
        else
            xPath = req.getXPath();

        Owned<ILockInfoCollection> lockInfoCollection = querySDS().getLocks(req.getEPIP(), xPath.str());

        IArrayOf<IEspLock> locks;
        CDateTime time;
        time.setNow();
        time_t ttNow = time.getSimple();
        unsigned msNow = msTick();
        for (unsigned l=0; l<lockInfoCollection->queryLocks(); l++)
        {
            ILockInfo& lockInfo = lockInfoCollection->queryLock(l);

            CDfsLogicalFileName dlfn;
            const char* lfn = NULL;
            const char* xPath = NULL;
            if (dlfn.setFromXPath(lockInfo.queryXPath()))
                lfn = dlfn.get();
            else
                xPath = lockInfo.queryXPath();
            for (unsigned i=0; i<lockInfo.queryConnections(); i++)
            {
                CLockMetaData& lMD = lockInfo.queryLockData(i);
                if (postFilter.check(lMD, msNow, ttNow))
                    addLockInfo(lMD, xPath, lfn, msNow, ttNow, locks);
            }
        }
        unsigned numLocks = locks.length();
        if (numLocks)
            resp.setLocks(locks);
        resp.setNumLocks(numLocks);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

