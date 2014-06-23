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

#pragma warning (disable : 4786)

#include "build-config.h"

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif
#include "ws_smcService.hpp"
#include "wshelpers.hpp"

#include "dalienv.hpp"
#include "WUWrapper.hpp"
#include "wujobq.hpp"
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

struct QueueWrapper
{
    QueueWrapper(const char* targetName, const char* queueExt)
    {
        StringBuffer name;
        name.append(targetName).append('.').append(queueExt);
        queue.setown(createJobQueue(name.str()));
    }

    QueueWrapper(const char* queueName)
    {
        queue.setown(createJobQueue(queueName));
    }

    operator IJobQueue*() { return queue.get(); }
    IJobQueue* operator->() { return queue.get(); }

    Owned<IJobQueue> queue;
};

struct QueueLock
{
    QueueLock(IJobQueue* q): queue(q) { queue->lock(); }
    ~QueueLock() 
    { 
        queue->unlock(); 
    }

    Linked<IJobQueue> queue;
};

static int sortTargetClustersByNameDescending(IInterface **L, IInterface **R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return strcmp(right->getClusterName(), left->getClusterName());
}

static int sortTargetClustersByNameAscending(IInterface **L, IInterface **R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return strcmp(left->getClusterName(), right->getClusterName());
}

static int sortTargetClustersBySizeDescending(IInterface **L, IInterface **R)
{
    IEspTargetCluster *left = (IEspTargetCluster *) *L;
    IEspTargetCluster *right = (IEspTargetCluster *) *R;
    return right->getClusterSize() - left->getClusterSize();
}

static int sortTargetClustersBySizeAscending(IInterface **L, IInterface **R)
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

static void countProgress(IPropertyTree *t,unsigned &done,unsigned &total)
{
    total = 0;
    done = 0;
    Owned<IPropertyTreeIterator> it = t->getElements("DFT/progress");
    ForEach(*it) {
        IPropertyTree &e=it->query();
        if (e.getPropInt("@done",0))
            done++;
        total++;
    }
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
        StringBuffer stateStr;
        SCMStringBuffer state, stateEx, owner, jobname;
        setWuid(wuid);
        wu->getStateDesc(state);
        setStateID(wu->getState());
        if (wu->getState() == WUStateBlocked)
        {
            wu->getStateEx(stateEx);
            if (notCheckVersion || (version > 1.00))
                setExtra(stateEx.str());
        }
        buildAndSetState(state.str(), stateEx.str(), location, index);
        if ((notCheckVersion || (version > 1.09)) && (wu->getState() == WUStateFailed))
            setWarning("The job will ultimately not complete. Please check ECLAgent.");

        setOwner(wu->getUser(owner).str());
        setJobname(wu->getJobName(jobname).str());
        setPriorityStr(wu->getPriority());

        if ((notCheckVersion || (version > 1.08)) && wu->isPausing())
        {
            setIsPausing(true);
        }
        if (notCheckVersion || (version > 1.14))
        {
            SCMStringBuffer clusterName;
            setClusterName(wu->getClusterName(clusterName).str());
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

bool CWsSMCEx::onIndex(IEspContext &context, IEspSMCIndexRequest &req, IEspSMCIndexResponse &resp)
{
    resp.setRedirectUrl("/");
    return true;
}

static int stringcmp(const char **a, const char **b)
{
    return strcmp(*a, *b);
}

bool isInWuList(IArrayOf<IEspActiveWorkunit>& aws, const char* wuid)
{
    bool bFound = false;
    if (wuid && *wuid && (aws.length() > 0))
    {
        ForEachItemIn(k, aws)
        {
            IEspActiveWorkunit& wu = aws.item(k);
            const char* wuid0 = wu.getWuid();
            const char* server0 = wu.getServer();
            if (wuid0 && !strcmp(wuid0, wuid) && (!server0 || strcmp(server0, "ECLagent")))
            {
                bFound = true;
                break;
            }
        }
    }

    return bFound;
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

void CWsSMCEx::createActiveWorkUnit(Owned<IEspActiveWorkunit>& ownedWU, IEspContext &context, const char* wuid, const char* location,
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

void CWsSMCEx::readWUsAndStateFromJobQueue(IEspContext& context, CWsSMCTargetCluster& targetCluster,
     CWsSMCQueue& jobQueue, const char* queueName, BoolHash& uniqueWUIDs, IArrayOf<IEspActiveWorkunit>& aws)
{
    CJobQueueContents contents;
    Owned<IJobQueue> queue = createJobQueue(jobQueue.queueName.str());
    queue->copyItemsAndState(contents, jobQueue.queueState, jobQueue.queueStateDetails);
    Owned<IJobQueueIterator> iter = contents.getIterator();
    jobQueue.countQueuedJobs=0;
    ForEach(*iter)
    {
        const char* wuid = iter->query().queryWUID();
        if (!wuid || !*wuid || uniqueWUIDs.getValue(wuid))
            continue;

        uniqueWUIDs.setValue(wuid, true);

        const char* queue = NULL;
        if (queueName && *queueName)
            queue = queueName;
        else
            queue = targetCluster.clusterName.get();

        Owned<IEspActiveWorkunit> wu;
        createActiveWorkUnit(wu, context, wuid, jobQueue.queueName.str(), ++jobQueue.countQueuedJobs, targetCluster.statusServerName.str(),
            queue, NULL, targetCluster.clusterName.get(), false);
        aws.append(*wu.getLink());
    }
}

bool CWsSMCEx::findQueueInStatusServer(IEspContext& context, IPropertyTree* serverStatusRoot, const char* serverName, const char* queueName)
{
    bool foundServer = false;
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
            {
                foundServer = true;
                break;
            }
        }
        if (foundServer)
            break;
    }
    return foundServer;
}

void CWsSMCEx::sortTargetClusters(IArrayOf<IEspTargetCluster>& clusters, const char* sortBy, bool descending)
{
    if (!sortBy || !*sortBy || strieq(sortBy, "name"))
        clusters.sort(descending ? sortTargetClustersByNameDescending : sortTargetClustersByNameAscending);
    else
        clusters.sort(descending ? sortTargetClustersBySizeDescending : sortTargetClustersBySizeAscending);
}

void CWsSMCEx::setClusterQueueStatus(CWsSMCTargetCluster& targetCluster)
{
    CWsSMCQueue& jobQueue = targetCluster.clusterQueue;
    if (targetCluster.clusterType != ThorLCRCluster)
        jobQueue = targetCluster.agentQueue;
    if (!jobQueue.queueName.length())
        return;

    targetCluster.clusterStatusDetails.appendf("%s: ", jobQueue.queueName.str());

    bool queuePausedOrStopped = false;
    unsigned countRunningJobs = jobQueue.countRunningJobs;
    unsigned countQueuedJobs = jobQueue.countQueuedJobs;
    if (targetCluster.clusterType == ThorLCRCluster)
    {
        countRunningJobs += targetCluster.agentQueue.countRunningJobs;
        countQueuedJobs += targetCluster.agentQueue.countQueuedJobs;
    }

    if (jobQueue.queueState.length())
    {
        const char* queueState = jobQueue.queueState.str();
        const char* queueStateDetails = jobQueue.queueStateDetails.str();
        if (queueStateDetails && *queueStateDetails)
            targetCluster.clusterStatusDetails.appendf("queue %s; %s;", queueState, queueStateDetails);
        else
            targetCluster.clusterStatusDetails.appendf("queue %s; ", queueState);
        if (strieq(queueState,"stopped") || strieq(queueState,"paused"))
            queuePausedOrStopped = true;
    }

    if (!jobQueue.foundQueueInStatusServer)
    {
        if (queuePausedOrStopped)
            jobQueue.statusType = QueuePausedOrStoppedNotFound;
        else
            jobQueue.statusType = QueueRunningNotFound;
    }
    else
    {
        if (queuePausedOrStopped)
        {
            if (jobQueue.countRunningJobs > 0)
                jobQueue.statusType = QueuePausedOrStoppedWithJobs;
            else
                jobQueue.statusType = QueuePausedOrStoppedWithNoJob;
        }
    }
}

void CWsSMCEx::setClusterStatus(IEspContext& context, CWsSMCTargetCluster& targetCluster, IEspTargetCluster* returnCluster)
{
    setClusterQueueStatus(targetCluster);

    int statusType = (targetCluster.clusterQueue.statusType > targetCluster.agentQueue.statusType) ? targetCluster.clusterQueue.statusType
        : targetCluster.agentQueue.statusType;
    returnCluster->setClusterStatus(statusType);
    //Set 'Warning' which may be displayed beside cluster name
    if (statusType == QueueRunningNotFound)
        returnCluster->setWarning("Cluster not attached");
    else if (statusType == QueuePausedOrStoppedNotFound)
        returnCluster->setWarning("Queue paused or stopped - Cluster not attached");
    else if (statusType != RunningNormal)
        returnCluster->setWarning("Queue paused or stopped");
    //Set 'StatusDetails' which may be displayed when a mouse is moved over cluster icon
    if (targetCluster.clusterStatusDetails.length())
        returnCluster->setStatusDetails(targetCluster.clusterStatusDetails.str());
}

void CWsSMCEx::getWUsNotOnTargetCluster(IEspContext &context, IPropertyTree* serverStatusRoot, IArrayOf<IEspServerJobQueue>& serverJobQueues,
     IArrayOf<IEspActiveWorkunit>& aws)
{
    BoolHash uniqueServers;
    Owned<IPropertyTreeIterator> it(serverStatusRoot->getElements("Server"));
    ForEach(*it)
    {
        IPropertyTree& serverNode = it->query();
        const char* serverName = serverNode.queryProp("@name");
        const char* instance = serverNode.queryProp("@node");
        const char* queueName = serverNode.queryProp("@queue");
        unsigned port = serverNode.getPropInt("@mpport", 0);
        if (!serverName || !*serverName || !instance || !*instance || strieq(serverName, "DFUserver") ||//DFUServer already handled separately
            strieq(serverName, "ThorMaster") || strieq(serverName, "RoxieServer") || strieq(serverName, "HThorServer"))//target clusters already handled separately
            continue;

        VStringBuffer instanceName("%s_on_%s:%d", serverName, instance, port);
        Owned<IPropertyTreeIterator> wuids(serverNode.getElements("WorkUnit"));
        ForEach(*wuids)
        {
            const char* wuid=wuids->query().queryProp(NULL);
            if (!wuid || !*wuid)
                continue;

            if (isInWuList(aws, wuid))
                continue;

            Owned<IEspActiveWorkunit> wu;
            createActiveWorkUnit(wu, context, wuid, NULL, 0, serverName, queueName, instance, NULL, false);
            aws.append(*wu.getLink());
        }
        if (!uniqueServers.getValue(instanceName))
        {
            uniqueServers.setValue(instanceName, true);
            addServerJobQueue(serverJobQueues, queueName, instanceName, serverName, instance, port);
        }
    }

    return;
}

void CWsSMCEx::readDFUWUs(IEspContext &context, const char* queueName, const char* serverName, IArrayOf<IEspActiveWorkunit>& aws)
{
    StringAttrArray wulist;
    unsigned running = queuedJobs(queueName, wulist);
    ForEachItemIn(i, wulist)
    {
        StringBuffer jname, uname, state, error;
        const char *wuid = wulist.item(i).text.get();
        if (i<running)
            state.set("running");
        else
            state.set("queued");

        try
        {
            Owned<IConstDFUWorkUnit> dfuwu = getDFUWorkUnitFactory()->openWorkUnit(wuid, false);
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
        wu->setServer("DFUserver");
        wu->setInstance(serverName);
        wu->setQueueName(queueName);
        aws.append(*wu.getLink());
    }
}

void CWsSMCEx::getDFUServersAndWUs(IEspContext &context, IPropertyTree* envRoot, IArrayOf<IEspServerJobQueue>& serverJobQueues, IArrayOf<IEspActiveWorkunit>& aws)
{
    if (!envRoot)
        return;

    VStringBuffer path("Software/%s", eqDfu);
    Owned<IPropertyTreeIterator> services = envRoot->getElements(path);
    ForEach(*services)
    {
        IPropertyTree &serviceTree = services->query();
        const char *qname = serviceTree.queryProp("@queue");
        const char *serverName = serviceTree.queryProp("@name");
        if (!qname || !*qname)
            continue;

        StringArray queues;
        queues.appendListUniq(qname, ",");
        ForEachItemIn(q, queues)
        {
            const char *queueName = queues.item(q);
            readDFUWUs(context, queueName, serverName, aws);
            addServerJobQueue(serverJobQueues, queueName, serverName, "DFUserver", NULL, 0);
        }
    }
}

void CWsSMCEx::getDFURecoveryJobs(IEspContext &context, const IPropertyTree* dfuRecoveryRoot, IArrayOf<IEspDFUJob>& jobs)
{
    if (!dfuRecoveryRoot)
        return;

    Owned<IPropertyTreeIterator> it(dfuRecoveryRoot->getElements("job"));
    ForEach(*it)
    {
        IPropertyTree &e=it->query();
        if (!e.getPropBool("Running",false))
            continue;

        StringBuffer cmd;
        unsigned done, total;
        countProgress(&e,done,total);
        cmd.append(e.queryProp("@command")).append(" ").append(e.queryProp("@command_parameters"));

        Owned<IEspDFUJob> job = new CDFUJob("","");
        job->setTimeStarted(e.queryProp("@time_started"));
        job->setDone(done);
        job->setTotal(total);
        job->setCommand(cmd.str());
        jobs.append(*job.getLink());
    }
}

bool ActivityInfo::isCachedActivityInfoValid(unsigned timeOutSeconds)
{
    CDateTime timeNow;
    timeNow.setNow();
    return timeNow.getSimple() <= timeCached.getSimple() + timeOutSeconds;;
}

void CWsSMCEx::clearActivityInfoCache()
{
    CriticalBlock b(getActivityCrit);
    activityInfoCache.clear();
}

ActivityInfo* CWsSMCEx::getActivityInfo(IEspContext &context, IEspActivityRequest &req)
{
    CriticalBlock b(getActivityCrit);

    if (activityInfoCache && activityInfoCache->isCachedActivityInfoValid(activityInfoCacheSeconds))
        return activityInfoCache.getLink();

    activityInfoCache.setown(createActivityInfo(context, req));
    return activityInfoCache.getLink();
}

ActivityInfo* CWsSMCEx::createActivityInfo(IEspContext &context, IEspActivityRequest &req)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Failed to get environment information.");

    CConstWUClusterInfoArray clusters;
    Owned<IPropertyTree> envRoot= &env->getPTree();
    getEnvironmentClusterInfo(envRoot, clusters);

    Owned<IRemoteConnection> connStatusServers = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,30000);
    IPropertyTree* serverStatusRoot = connStatusServers->queryRoot();
    if (!serverStatusRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_STATUS_INFO, "Failed to get status server information.");

    IPropertyTree* dfuRecoveryRoot = NULL;
    Owned<IRemoteConnection> connDFURecovery = querySDS().connect("DFU/RECOVERY",myProcessSession(), RTM_LOCK_READ, 30000);
    if (connDFURecovery)
        dfuRecoveryRoot = connDFURecovery->queryRoot();

    Owned<ActivityInfo> activityInfo = new ActivityInfo();
    readTargetClusterInfo(context, clusters, serverStatusRoot, activityInfo);
    readRunningWUsAndQueuedWUs(context, envRoot, serverStatusRoot, dfuRecoveryRoot, activityInfo);
    return activityInfo.getClear();
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

        Owned<ActivityInfo> activityInfo = getActivityInfo(context, req);
        setActivityResponse(context, activityInfo, req, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

const char *CWsSMCEx::getStatusServerTypeName(WsSMCStatusServerType type)
{
    return (type < WsSMCSSTterm) ? WsSMCStatusServerTypeNames[type] : NULL;
}

void CWsSMCEx::readTargetClusterInfo(IEspContext &context, CConstWUClusterInfoArray& clusters, IPropertyTree* serverStatusRoot,
    ActivityInfo* activityInfo)
{
    ForEachItemIn(c, clusters)
    {
        IConstWUClusterInfo &cluster = clusters.item(c);
        Owned<CWsSMCTargetCluster> targetCluster = new CWsSMCTargetCluster();
        readTargetClusterInfo(context, cluster, serverStatusRoot, targetCluster);
        if (cluster.getPlatform() == ThorLCRCluster)
            activityInfo->thorTargetClusters.append(*targetCluster.getClear());
        else if (cluster.getPlatform() == RoxieCluster)
            activityInfo->roxieTargetClusters.append(*targetCluster.getClear());
        else
            activityInfo->hthorTargetClusters.append(*targetCluster.getClear());
    }
}

void CWsSMCEx::readTargetClusterInfo(IEspContext& context, IConstWUClusterInfo& cluster, IPropertyTree* serverStatusRoot, CWsSMCTargetCluster* targetCluster)
{
    SCMStringBuffer clusterName;
    cluster.getName(clusterName);
    targetCluster->clusterName.set(clusterName.str());
    targetCluster->clusterType = cluster.getPlatform();
    targetCluster->clusterSize = cluster.getSize();
    cluster.getServerQueue(targetCluster->serverQueue.queueName);
    cluster.getAgentQueue(targetCluster->agentQueue.queueName);

    StringBuffer statusServerName;
    CWsSMCQueue* jobQueue = NULL;
    if (targetCluster->clusterType == ThorLCRCluster)
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTThorLCRCluster));
        jobQueue = &targetCluster->clusterQueue;
        cluster.getThorQueue(jobQueue->queueName);
    }
    else if (targetCluster->clusterType == RoxieCluster)
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTRoxieCluster));
        jobQueue = &targetCluster->agentQueue;
    }
    else
    {
        statusServerName.set(getStatusServerTypeName(WsSMCSSTHThorCluster));
        jobQueue = &targetCluster->agentQueue;
    }

    targetCluster->statusServerName.set(statusServerName.str());
    targetCluster->queueName.set(jobQueue->queueName.str());

    if (serverStatusRoot)
    {
        jobQueue->foundQueueInStatusServer = findQueueInStatusServer(context, serverStatusRoot, statusServerName.str(), targetCluster->queueName.get());
        if (!jobQueue->foundQueueInStatusServer)
            targetCluster->clusterStatusDetails.appendf("Cluster %s not attached; ", clusterName.str());
    }

    return;
}

void CWsSMCEx::readRunningWUsAndQueuedWUs(IEspContext &context, IPropertyTree* envRoot, IPropertyTree* serverStatusRoot,
    IPropertyTree* dfuRecoveryRoot, ActivityInfo* activityInfo)
{
    BoolHash uniqueWUIDs;
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTThorLCRCluster, activityInfo->thorTargetClusters, activityInfo->roxieTargetClusters, activityInfo->hthorTargetClusters, uniqueWUIDs, activityInfo->aws);
    readWUsAndStateFromJobQueue(context, activityInfo->thorTargetClusters, uniqueWUIDs, activityInfo->aws);
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTRoxieCluster, activityInfo->roxieTargetClusters, activityInfo->thorTargetClusters, activityInfo->hthorTargetClusters, uniqueWUIDs, activityInfo->aws);
    readWUsAndStateFromJobQueue(context, activityInfo->roxieTargetClusters, uniqueWUIDs, activityInfo->aws);
    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTHThorCluster, activityInfo->hthorTargetClusters, activityInfo->thorTargetClusters, activityInfo->roxieTargetClusters, uniqueWUIDs, activityInfo->aws);
    readWUsAndStateFromJobQueue(context, activityInfo->hthorTargetClusters, uniqueWUIDs, activityInfo->aws);

    readRunningWUsOnStatusServer(context, serverStatusRoot, WsSMCSSTECLagent, activityInfo->thorTargetClusters, activityInfo->roxieTargetClusters, activityInfo->hthorTargetClusters, uniqueWUIDs, activityInfo->aws);

    getWUsNotOnTargetCluster(context, serverStatusRoot, activityInfo->serverJobQueues, activityInfo->aws);
    getDFUServersAndWUs(context, envRoot, activityInfo->serverJobQueues, activityInfo->aws);
    getDFURecoveryJobs(context, dfuRecoveryRoot, activityInfo->DFURecoveryJobs);
}

void CWsSMCEx::readRunningWUsOnStatusServer(IEspContext& context, IPropertyTree* serverStatusRoot, WsSMCStatusServerType statusServerType,
        CIArrayOf<CWsSMCTargetCluster>& targetClusters, CIArrayOf<CWsSMCTargetCluster>& targetClusters1, CIArrayOf<CWsSMCTargetCluster>& targetClusters2,
        BoolHash& uniqueWUIDs, IArrayOf<IEspActiveWorkunit>& aws)
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

        StringBuffer instance;
        if ((statusServerType == WsSMCSSTThorLCRCluster) || (statusServerType == WsSMCSSTRoxieCluster))
            serverStatusNode.getProp("@cluster", instance);
        else
            instance.appendf("%s on %s", serverName, serverStatusNode.queryProp("@node"));

        const char* graph = NULL;
        int sgDuration = -1;
        int subgraph = -1;
        StringBuffer durationStr, subgraphStr;
        if (!isECLAgent)
        {
            sgDuration = serverStatusNode.getPropInt("@sg_duration", -1);
            subgraph = serverStatusNode.getPropInt("@subgraph", -1);
            graph = serverStatusNode.queryProp("@graph");
            durationStr.appendf("%d min", sgDuration);
            subgraphStr.appendf("%d", subgraph);
        }

        Owned<IPropertyTreeIterator> wuids(serverStatusNode.getElements("WorkUnit"));
        ForEach(*wuids)
        {
            const char* wuid=wuids->query().queryProp(NULL);
            if (!wuid || !*wuid || (isECLAgent && uniqueWUIDs.getValue(wuid)))
                continue;

            CWsSMCTargetCluster* targetCluster = findWUClusterInfo(context, wuid, isECLAgent, targetClusters, targetClusters1, targetClusters2);
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
                uniqueWUIDs.setValue(wuid, true);

                const char *cluster = serverStatusNode.queryProp("Cluster");
                StringBuffer queueName;
                if (cluster) // backward compat check.
                    getClusterThorQueueName(queueName, cluster);
                else
                    queueName.append(targetCluster->queueName.get());

                createActiveWorkUnit(wu, context, wuid, !strieq(targetClusterName, instance.str()) ? instance.str() : NULL, 0, serverName,
                    queueName, instance.str(), targetClusterName, false);

                if (wu->getStateID() == WUStateRunning) //'aborting' may be another possible status
                {
                    if (subgraph > -1 && sgDuration > -1)
                    {
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
                createActiveWorkUnit(wu, context, wuid, instance.str(), 0, serverName, serverName, instance.str(), targetClusterName, false);

                if (targetCluster->clusterType == ThorLCRCluster)
                    wu->setClusterType("Thor");
                else if (targetCluster->clusterType == RoxieCluster)
                    wu->setClusterType("Roxie");
                else
                    wu->setClusterType("HThor");
                wu->setClusterQueueName(targetCluster->queueName.get());

                if (wu->getStateID() != WUStateRunning)
                {
                    const char *extra = wu->getExtra();
                    if (wu->getStateID() != WUStateBlocked || !extra || !*extra)  // Blocked on persist treated as running here
                    {
                        aws.append(*wu.getLink());
                        jobQueue->countQueuedJobs++;
                        continue;
                    }
                }

                if (serverStatusNode.getPropInt("@memoryBlocked ", 0) != 0)
                    wu->setMemoryBlocked(1);
            }

            aws.append(*wu.getLink());
            jobQueue->countRunningJobs++;
        }
    }
}

void CWsSMCEx::readWUsAndStateFromJobQueue(IEspContext& context, CIArrayOf<CWsSMCTargetCluster>& targetClusters, BoolHash& uniqueWUIDs, IArrayOf<IEspActiveWorkunit>& aws)
{
    ForEachItemIn(i, targetClusters)
        readWUsAndStateFromJobQueue(context, targetClusters.item(i), uniqueWUIDs, aws);
}

void CWsSMCEx::readWUsAndStateFromJobQueue(IEspContext& context, CWsSMCTargetCluster& targetCluster, BoolHash& uniqueWUIDs, IArrayOf<IEspActiveWorkunit>& aws)
{
    if (targetCluster.clusterType == ThorLCRCluster)
    {
        readWUsAndStateFromJobQueue(context, targetCluster, targetCluster.clusterQueue, NULL, uniqueWUIDs, aws);
        targetCluster.queueStatus.set(targetCluster.clusterQueue.queueState);
    }
    if (targetCluster.agentQueue.queueName.length())
    {
        readWUsAndStateFromJobQueue(context, targetCluster, targetCluster.agentQueue, targetCluster.agentQueue.queueName.str(), uniqueWUIDs, aws);
        if (targetCluster.clusterType != ThorLCRCluster)
            targetCluster.queueStatus.set(targetCluster.agentQueue.queueState);
    }
    if (targetCluster.serverQueue.queueName.length())
        readWUsAndStateFromJobQueue(context, targetCluster, targetCluster.serverQueue, targetCluster.serverQueue.queueName.str(), uniqueWUIDs, aws);
}

CWsSMCTargetCluster* CWsSMCEx::findTargetCluster(const char* clusterName, CIArrayOf<CWsSMCTargetCluster>& targetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        if (strieq(targetCluster.clusterName.get(), clusterName))
            return &targetCluster;
    }
    return NULL;
}

CWsSMCTargetCluster* CWsSMCEx::findWUClusterInfo(IEspContext& context, const char* wuid, bool isOnECLAgent, CIArrayOf<CWsSMCTargetCluster>& targetClusters,
    CIArrayOf<CWsSMCTargetCluster>& targetClusters1, CIArrayOf<CWsSMCTargetCluster>& targetClusters2)
{
    SCMStringBuffer clusterName;
    try
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
        if (!cw)
            return NULL;
        cw->getClusterName(clusterName);
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
            awsReturned.append(*cw.getLink());

            e->Release();
        }
    }
    resp.setRunning(awsReturned);
    return;
}

void CWsSMCEx::setActivityResponse(IEspContext &context, ActivityInfo* activityInfo, IEspActivityRequest &req, IEspActivityResponse& resp)
{
    double version = context.getClientVersion();
    const char* sortBy = req.getSortBy();
    bool descending = req.getDescending();
    if (version >= 1.16)
    {
        IArrayOf<IEspTargetCluster> thorClusters;
        IArrayOf<IEspTargetCluster> hthorClusters;
        IArrayOf<IEspTargetCluster> roxieClusters;

        setESPTargetClusters(context, activityInfo->thorTargetClusters, thorClusters);
        setESPTargetClusters(context, activityInfo->roxieTargetClusters, roxieClusters);
        setESPTargetClusters(context, activityInfo->hthorTargetClusters, hthorClusters);

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
        resp.setServerJobQueues(activityInfo->serverJobQueues);
    }
    else
    {//for backward compatible
        IArrayOf<IEspThorCluster> thorClusters;
        ForEachItemIn(i, activityInfo->thorTargetClusters)
        {
            CWsSMCTargetCluster& targetCluster = activityInfo->thorTargetClusters.item(i);
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
            ForEachItemIn(i, activityInfo->roxieTargetClusters)
            {
                CWsSMCTargetCluster& targetCluster = activityInfo->roxieTargetClusters.item(i);
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
            ForEachItemIn(i, activityInfo->hthorTargetClusters)
            {
                CWsSMCTargetCluster& targetCluster = activityInfo->hthorTargetClusters.item(i);
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
            resp.setServerJobQueues(activityInfo->serverJobQueues);
    }
    resp.setDFUJobs(activityInfo->DFURecoveryJobs);
    addWUsToResponse(context, activityInfo->aws, resp);
    return;
}

void CWsSMCEx::setESPTargetClusters(IEspContext& context, CIArrayOf<CWsSMCTargetCluster>& targetClusters, IArrayOf<IEspTargetCluster>& respTargetClusters)
{
    ForEachItemIn(i, targetClusters)
    {
        CWsSMCTargetCluster& targetCluster = targetClusters.item(i);
        Owned<IEspTargetCluster> respTargetCluster = new CTargetCluster("", "");
        respTargetCluster->setClusterName(targetCluster.clusterName.get());
        respTargetCluster->setClusterSize(targetCluster.clusterSize);
        respTargetCluster->setClusterType(targetCluster.clusterType);
        respTargetCluster->setQueueName(targetCluster.queueName.get());
        respTargetCluster->setQueueStatus(targetCluster.queueStatus.get());
        setClusterStatus(context, targetCluster, respTargetCluster);
        respTargetClusters.append(*respTargetCluster.getClear());
    }
}

void CWsSMCEx::addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* serverName,
    const char* serverType, const char* networkAddress, unsigned port)
{
    if (!queueName || !*queueName || !serverName || !*serverName || !serverType || !*serverType)
        return;

    StringBuffer queueState;
    StringBuffer queueStateDetails;
    Owned<IJobQueue> queue = createJobQueue(queueName);
    if (queue->stopped(queueStateDetails))
        queueState.set("stopped");
    else if (queue->paused(queueStateDetails))
        queueState.set("paused");
    else
        queueState.set("running");
    addServerJobQueue(jobQueues, queueName, serverName, serverType, networkAddress, port, queueState.str(), queueStateDetails.str());
}

void CWsSMCEx::addServerJobQueue(IArrayOf<IEspServerJobQueue>& jobQueues, const char* queueName, const char* serverName,
    const char* serverType, const char* networkAddress, unsigned port, const char* queueState, const char* queueStateDetails)
{
    if (!queueName || !*queueName || !serverName || !*serverName || !serverType || !*serverType)
        return;

    if (!queueState || !*queueState)
        queueState = "running";

    Owned<IEspServerJobQueue> jobQueue = createServerJobQueue("", "");
    jobQueue->setQueueName(queueName);
    jobQueue->setServerName(serverName);
    jobQueue->setServerType(serverType);
    if (networkAddress && *networkAddress)
    {
        jobQueue->setNetworkAddress(networkAddress);
        jobQueue->setPort(port);
    }
    setServerJobQueueStatus(jobQueue, queueState, queueStateDetails);

    jobQueues.append(*jobQueue.getClear());
}

void CWsSMCEx::setServerJobQueueStatus(double version, IEspServerJobQueue* jobQueue, const char* status, const char* details)
{
    if (!status || !*status)
        return;

    jobQueue->setQueueStatus(status);
    if (version >= 1.17)
        setServerJobQueueStatusDetails(jobQueue, status, details);
}

void CWsSMCEx::setServerJobQueueStatus(IEspServerJobQueue* jobQueue, const char* status, const char* details)
{
    if (!status || !*status)
        return;
    jobQueue->setQueueStatus(status);
    setServerJobQueueStatusDetails(jobQueue, status, details);
}

void CWsSMCEx::setServerJobQueueStatusDetails(IEspServerJobQueue* jobQueue, const char* status, const char* details)
{
    StringBuffer queueState;
    if (details && *details)
        queueState.appendf("queue %s; %s;", status, details);
    else
        queueState.appendf("queue %s;", status);
    jobQueue->setStatusDetails(queueState.str());
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

        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        unsigned index=queue->findRank(req.getWuid());
        if(index<queue->ordinality())
        {
            IJobQueueItem * item0 = queue->getItem(index);
            IJobQueueItem * item = queue->getItem(index+1);
            if(item && item0 && (item0->getPriority() == item->getPriority()))
                queue->moveAfter(req.getWuid(),item->queryWUID());
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

        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        unsigned index=queue->findRank(req.getWuid());
        if(index>0 && index<queue->ordinality())
        {
            IJobQueueItem * item0 = queue->getItem(index);
            IJobQueueItem * item = queue->getItem(index-1);
            if(item && item0 && (item0->getPriority() == item->getPriority()))
                queue->moveBefore(req.getWuid(),item->queryWUID());
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

        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index<queue->ordinality())
        {
            int priority0 = queue->getItem(index)->getPriority();
            unsigned biggestIndoxInSamePriority = index;
            unsigned nextIndex = biggestIndoxInSamePriority + 1;
            while (nextIndex<queue->ordinality())
            {
                IJobQueueItem * item = queue->getItem(nextIndex);
                if (priority0 != item->getPriority())
                {
                    break;
                }
                biggestIndoxInSamePriority = nextIndex;
                nextIndex++;
            }

            if (biggestIndoxInSamePriority != index)
            {
                IJobQueueItem * item = queue->getItem(biggestIndoxInSamePriority);
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

bool CWsSMCEx::onMoveJobFront(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index>0 && index<queue->ordinality())
        {
            int priority0 = queue->getItem(index)->getPriority();
            unsigned smallestIndoxInSamePriority = index;
            int nextIndex = smallestIndoxInSamePriority - 1;
            while (nextIndex >= 0)
            {
                IJobQueueItem * item = queue->getItem(nextIndex);
                if (priority0 != item->getPriority())
                {
                    break;
                }
                smallestIndoxInSamePriority = nextIndex;
                nextIndex--;
            }

            if (smallestIndoxInSamePriority != index)
            {
                IJobQueueItem * item = queue->getItem(smallestIndoxInSamePriority);
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

bool CWsSMCEx::onRemoveJob(IEspContext &context, IEspSMCJobRequest &req, IEspSMCJobResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        secAbortWorkUnit(req.getWuid(), *context.querySecManager(), *context.queryUser());

        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index<queue->ordinality())
        {
            if(!queue->cancelInitiateConversation(req.getWuid()))
                throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Failed to remove the workunit %s",req.getWuid());
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

        StringBuffer info;
        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        queue->stop(createQueueActionInfo(context, "stopped", req, info));
        AccessSuccess(context, "Stopped queue %s",req.getCluster());
        clearActivityInfoCache();

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

        StringBuffer info;
        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        queue->resume(createQueueActionInfo(context, "resumed", req, info));
        AccessSuccess(context, "Resumed queue %s",req.getCluster());
        clearActivityInfoCache();

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

        StringBuffer info;
        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        queue->pause(createQueueActionInfo(context, "paused", req, info));
        AccessSuccess(context, "Paused queue %s",req.getCluster());
        clearActivityInfoCache();

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
        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        {
            QueueLock lock(queue);
            for(unsigned i=0;i<queue->ordinality();i++)
                secAbortWorkUnit(queue->getItem(i)->queryWUID(), *context.querySecManager(), *context.queryUser());
            queue->clear();
        }
        AccessSuccess(context, "Cleared queue %s",req.getCluster());
        clearActivityInfoCache();

        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onSetJobPriority(IEspContext &context, IEspSMCPriorityRequest &req, IEspSMCPriorityResponse &resp)
{
    try
    {
        const char* wuid = req.getWuid();
        if (!wuid || !*wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not specified.");

        Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
        Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
        if (!lw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Cannot update Workunit %s", wuid);

        if(stricmp(req.getPriority(),"high")==0)
            lw->setPriority(PriorityClassHigh);
        else if(stricmp(req.getPriority(),"normal")==0)
            lw->setPriority(PriorityClassNormal);
        else if(stricmp(req.getPriority(),"low")==0)
            lw->setPriority(PriorityClassLow);

        // set job priority
        int priority = lw->getPriorityValue();
        Owned<IJobQueue> queue = createJobQueue(req.getQueueName());
        QueueLock lock(queue);
        queue->changePriority(req.getWuid(),priority);

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
