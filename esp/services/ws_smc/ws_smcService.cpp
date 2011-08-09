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

#pragma warning (disable : 4786)

#include "ldapsecurity.ipp"
#include "ws_smcService.hpp"
#include "wshelpers.hpp"

#include "dalienv.hpp"
#include "WUWrapper.hpp"
#include "wujobq.hpp"
#include "dfuwu.hpp"
#include "exception_util.hpp"

static const char* FEATURE_URL = "SmcAccess";
const char* THORQUEUE_FEATURE = "ThorQueueAccess";

const char* PERMISSIONS_FILENAME = "espsmc_permissions.xml";

void AccessSuccess(IEspContext& context, char const * msg,...) __attribute__((format(printf, 2, 3)))
{
    StringBuffer buf;
    buf.appendf("User %s: ",context.queryUserId());
    va_list args;
    va_start(args, msg);
    buf.valist_appendf(msg, args);
    va_end(args);
    AUDIT(AUDIT_TYPE_ACCESS_SUCCESS,buf.str());
}

void AccessFailure(IEspContext& context, char const * msg,...) __attribute__((format(printf, 2, 3)))
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
    QueueWrapper(const char* cluster)
    {
        StringBuffer name;
        name<<cluster<<".thor";
        queue.setown(createJobQueue(name.str()));
    }

    QueueWrapper(int clusterType, const char* cluster)
    {
        if (!cluster || !*cluster)
            return;

        const char* type = eqThorCluster;
        if (clusterType < 1)
            type = eqRoxieCluster;

        CTpWrapper dummy;
        IArrayOf<IEspTpLogicalCluster> clusters;
        dummy.getTargetClusterList(clusters, type, cluster);
        if (clusters.length() < 1)
            return;

        IEspTpLogicalCluster &cluster0 = clusters.item(0);
        const char *name0 = cluster0.getName();
        if (!name0 || !*name0)
            return;

        StringBuffer name;
        name<<name0<<".thor";
        queue.setown(createJobQueue(name.str()));
    }

    QueueWrapper(const char* clusterName, const char* queueName)
    {
        StringBuffer name;
        name<<clusterName<<"."<<queueName;
        queue.setown(createJobQueue(name.str()));
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
    CActiveWorkunitWrapper(IEspContext &context, const char* wuid,unsigned index=0): CActiveWorkunit("","")
    {
        double version = context.getClientVersion();

        CWUWrapper wu(wuid, context);
        SCMStringBuffer state,owner,jobname;
        setWuid(wuid);
        if(index)
            state.s.append("queued(").append(index).append(")");
        else
            wu->getStateDesc(state);

        setState(state.str());
        setStateID(wu->getState());
        if ((version > 1.09) && (wu->getState() == WUStateFailed))
            setWarning("The job will ultimately not complete. Please check ECLAgent.");

        setOwner(wu->getUser(owner).str());
        setJobname(wu->getJobName(jobname).str());
        switch(wu->getPriority())
        {
            case PriorityClassHigh: setPriority("high"); break;
            default:
            case PriorityClassNormal: setPriority("normal"); break;
            case PriorityClassLow: setPriority("low"); break;
        }

        if (version > 1.08 && wu->isPausing())
        {
            setIsPausing(true);
        }
    }

    CActiveWorkunitWrapper(const char* wuid,const char* owner, const char* jobname, const char* state, const char* priority): CActiveWorkunit("","")
    {
        setWuid(wuid);
        setState(state);
        setOwner(owner);
        setJobname(jobname);
        setPriority(priority);
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

bool CWsSMCEx::isInWuList(IArrayOf<IEspActiveWorkunit>& aws, const char* wuid)
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

bool CWsSMCEx::onActivity(IEspContext &context, IEspActivityRequest &req, IEspActivityResponse& resp)
{
    context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, true);

    try
    {
        const char* build_ver = getBuildVersion();
        resp.setBuild(build_ver);

        double version = context.getClientVersion();

        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(req.getFromSubmitBtn() && secmgr && secmgr->isSuperUser(context.queryUser()))
        {
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

            if (!req.getEnableChatURL_isNull() && req.getEnableChatURL() && (chatURLStr.length() < 1))
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

            m_BannerAction = req.getBannerAction();
            if(!req.getEnableChatURL_isNull())
                m_EnableChatURL = req.getEnableChatURL();
        }

        if (version > 1.05)
        {
            int UserPermission = -1;
            CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
            if(secmgr && secmgr->isSuperUser(context.queryUser()))
                UserPermission = 0;

            resp.setUserPermission(UserPermission);
            resp.setShowBanner(m_BannerAction);
            resp.setShowChatURL(m_EnableChatURL);
            resp.setBannerContent(m_Banner.str());
            resp.setBannerSize(m_BannerSize.str());
            resp.setBannerColor(m_BannerColor.str());
            resp.setChatURL(m_ChatURL.str());

            if (version > 1.07)
            {
                resp.setBannerScroll(m_BannerScroll.str());
            }
        }
#if 0
Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
Owned<IPropertyTree> root = &constEnv->getPTree();
if (root)
{
StringBuffer buf;
toXML(root, buf);
if (buf.length() > 0)
{
    Owned<IFile> f = createIFile("new_env.xml");
    Owned<IFileIO> fio = f->open(IFOcreaterw);
    if (fio.get())
        fio->write(0, buf.length(), buf.str());
}
}
#endif
        Owned<IRemoteConnection> conn = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,30000);

        StringBuffer runningQueueNames[256];
        int runningQueues = 0;
        int runningJobsInQueue[256];
        for (int i = 0; i < 256; i++)
            runningJobsInQueue[i] = 0;

        IArrayOf<IEspActiveWorkunit> aws;
        if (conn.get()) 
        {

            Owned<IPropertyTreeIterator> it(conn->queryRoot()->getElements("Server"));
            ForEach(*it) 
            {
                StringBuffer instance;
                StringBuffer qname;
                int serverID = -1;
                IPropertyTree& node = it->query();
                const char* name = node.queryProp("@name");
                if(node.hasProp("@queue"))
                {
                    const char* queue=node.queryProp("@queue");
                    const char* thor=strstr(queue,".thor");
                    if(thor) 
                    {
                        qname.append(thor-queue,queue);
                        node.getProp("@thorname",instance);
                    }
                    else 
                        qname.append(queue);
                }
                else if (name && !stricmp(name, "ECLAgent"))
                {
                    qname.append(name);
                }
                if((instance.length()==0)&& name && *name)
                {
                    instance.append( !strcmp(name, "ECLagent") ? "ECL agent" : name);
                    instance.append(" on ").append(node.queryProp("@node"));
                }

                if (qname.length() > 0)
                {
                    int i = 0;
                    bool bFound = false;
                    while (i < runningQueues)
                    {
                        const char* serverName = runningQueueNames[i].str();
                        if (serverName && !stricmp(serverName, qname.str()))
                        {
                            bFound = true;
                            serverID = i;
                            break;
                        }

                        i++;
                    }

                    if (!bFound)
                    {
                        runningQueueNames[runningQueues] = qname;
                        runningQueues++;
                        serverID = runningQueues - 1;
                    }
                }

                Owned<IPropertyTreeIterator> wuids(node.getElements("WorkUnit"));
                ForEach(*wuids) 
                {
                    const char* wuid=wuids->query().queryProp(NULL);
                    if(!wuid)
                        continue;

                    try
                    {
                        IEspActiveWorkunit* wu=new CActiveWorkunitWrapper(context,wuid);
                        const char* servername = node.queryProp("@name");
                        wu->setServer(servername);
                        wu->setInstance(instance.str());
                        wu->setQueueName(qname.str());
                        double version = context.getClientVersion();
                        if (version > 1.01)
                        {
                            if (wu->getStateID() == WUStateRunning)
                            {
                                //StringBuffer buf;
                                //toXML(pNode, buf);
                                //if (buf.length() > 0)
                                //{
                                //  DBGLOG("CActiveWorkunitWrapper:%s", buf.str());
                                //}

                                int sg_duration = node.getPropInt("@sg_duration", -1);
                                const char* graph = node.queryProp("@graph");
                                int subgraph = node.getPropInt("@subgraph", -1);
                                if (subgraph > -1 && sg_duration > -1)
                                {
                                    StringBuffer durationStr;
                                    StringBuffer subgraphStr;
                                    durationStr.appendf("%d min", sg_duration);
                                    subgraphStr.appendf("%d", subgraph);
                                    wu->setGraphName(graph);
                                    wu->setDuration(durationStr.str());
                                    wu->setGID(subgraphStr.str());
                                }
                                int memoryBlocked = node.getPropInt("@memoryBlocked ", 0);
                                if (memoryBlocked != 0)
                                {
                                    wu->setMemoryBlocked(1);
                                }
                                
                                if (serverID > -1)
                                {
                                    runningJobsInQueue[serverID]++;
                                }
                            }
                        }
                        aws.append(*wu);
                    }
                    catch (IException *e)
                    {
                        StringBuffer msg;
                        Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(wuid, "", "", e->errorMessage(msg).str(), "normal"));
                        wu->setServer(node.queryProp("@name"));
                        wu->setInstance(instance.str());
                        wu->setQueueName(qname.str());
                        
                        aws.append(*wu.getLink());
                    }
                }
            }
        }

        SecAccessFlags access;
        bool doCommand=(context.authorizeFeature(THORQUEUE_FEATURE, access) && access>=SecAccess_Full);

        CTpWrapper dummy;
        IArrayOf<IEspTpCluster> clusters;
        dummy.getClusterProcessList(eqThorCluster,clusters,true);

        IArrayOf<IEspThorCluster> ThorClusters;
        ForEachItemIn(x, clusters)
        {
            IEspTpCluster& cluster = clusters.item(x);
            IEspThorCluster* returnCluster = new CThorCluster("","");
                
            returnCluster->setClusterName(cluster.getName());
            returnCluster->setQueueName(cluster.getQueueName());

            if (version > 1.08)
            {
                bool bThorLCR = dummy.getClusterLCR(eqThorCluster, cluster.getName());
                if (bThorLCR)
                    returnCluster->setThorLCR("withLCR");
                else
                    returnCluster->setThorLCR("noLCR");
            }

            int i = 0;
            int serverID = -1;
            const char* queueName = cluster.getQueueName();
            if (queueName && (runningQueues > 0))
            {
                for (int i = 0; i < runningQueues; i++)
                {
                    const char* serverName = runningQueueNames[i].str();
                    if (serverName && !stricmp(serverName, queueName))
                    {
                        serverID = i;
                        break;
                    }
                }
            }

            IArrayOf<IEspTpLogicalCluster> clusters1;
            dummy.getTargetClusterList(clusters1, eqThorCluster, cluster.getName());
            const char* queuename1 = cluster.getQueueName();
            if (clusters1.length() > 0)
            {
                IEspTpLogicalCluster& logicalCluster = clusters1.item(0);
                queuename1 = logicalCluster.getName();
            }

            QueueWrapper queue(queuename1);
            CJobQueueContents contents;
            queue->copyItems(contents);
            Owned<IJobQueueIterator> iter = contents.getIterator();
            unsigned count=0;
            ForEach(*iter) 
            {
                if (!isInWuList(aws, iter->query().queryWUID()))
                {
                    try
                    {
                            Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(context, iter->query().queryWUID(),++count));
                            wu->setServer("ThorMaster");
                            wu->setInstance(cluster.getName());
                            wu->setQueueName(cluster.getQueueName());

                            aws.append(*wu.getLink());
                    }
                    catch (IException *e)
                    {
                        StringBuffer msg;
                        Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(iter->query().queryWUID(), "", "", e->errorMessage(msg).str(), "normal"));
                        wu->setServer("ThorMaster");
                        wu->setInstance(cluster.getName());
                        wu->setQueueName(cluster.getQueueName());
                        
                        aws.append(*wu.getLink());
                    }
                }
            }

            int qStatus = 1;
            if(queue->stopped())
            {
                returnCluster->setQueueStatus("stopped");
                qStatus = 3;
            }
            else if (queue->paused())
            {
                returnCluster->setQueueStatus("paused");
                qStatus = 2;
            }
            else
            {
                returnCluster->setQueueStatus("running");
            }

            if (version > 1.06)
            {
                int color_type = 6;
                if (serverID < 0)
                {
                    if (qStatus > 1)
                        color_type = 3;
                    else
                        color_type = 5;
                }
                else if (runningJobsInQueue[serverID] > 0)
                {
                    if (qStatus > 1)
                        color_type = 1;
                    else
                        color_type = 4;
                }
                else if (qStatus > 1)
                {
                    color_type = 2;
                }
                returnCluster->setQueueStatus2(color_type);
                DBGLOG("QueueStatus2=<%d>", color_type);
            }

            returnCluster->setDoCommand(doCommand);
            ThorClusters.append(*returnCluster);
        }

        if (version > 1.06)
        {
            IArrayOf<IEspRoxieCluster> RoxieClusters;
            IArrayOf<IEspTpCluster> clusters1;
            dummy.getClusterProcessList(eqRoxieCluster,clusters1,true);
            ForEachItemIn(x1, clusters1)
            {
                IEspTpCluster& cluster = clusters1.item(x1);
                IEspRoxieCluster* returnCluster = new CRoxieCluster("","");
                    
                returnCluster->setClusterName(cluster.getName());
                returnCluster->setQueueName(cluster.getQueueName());
                
                IArrayOf<IEspTpLogicalCluster> clusters1;
                dummy.getTargetClusterList(clusters1, eqRoxieCluster, cluster.getName());
                const char* queuename1 = cluster.getQueueName();
                if (clusters1.length() > 0)
                {
                    IEspTpLogicalCluster& logicalCluster = clusters1.item(0);
                    queuename1 = logicalCluster.getName();
                }

                QueueWrapper queue(queuename1);

                CJobQueueContents contents;
                queue->copyItems(contents);
                Owned<IJobQueueIterator> iter = contents.getIterator();
                unsigned count=0;
                ForEach(*iter) 
                {
                    try
                    {
                        Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(context, iter->query().queryWUID(),++count));
                        wu->setServer("RoxieServer");
                        wu->setInstance(cluster.getName());
                        wu->setQueueName(cluster.getQueueName());

                        aws.append(*wu.getLink());
                    }
                    catch (IException *e)
                    {
                        StringBuffer msg;
                        Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(iter->query().queryWUID(), "", "", e->errorMessage(msg).str(), "normal"));
                        wu->setServer("RoxieServer");
                        wu->setInstance(cluster.getName());
                        wu->setQueueName(cluster.getQueueName());
                        
                        aws.append(*wu.getLink());
                    }
                }
                int qStatus = 1;
                if(queue->stopped())
                {
                    returnCluster->setQueueStatus("stopped");
                    qStatus = 3;
                }
                else if (queue->paused())
                {
                    returnCluster->setQueueStatus("paused");
                    qStatus = 2;
                }
                else
                {
                    returnCluster->setQueueStatus("running");
                }

                //returnCluster->setDoCommand(doCommand);
                RoxieClusters.append(*returnCluster); //Temperary add here
            }
            resp.setRoxieClusters(RoxieClusters);
        }

        IArrayOf<IConstTpEclServer> eclccservers;
        dummy.getTpEclCCServers(eclccservers);
        ForEachItemIn(x1, eclccservers)
        {
            IConstTpEclServer& eclccserver = eclccservers.item(x1);
            const char* serverName = eclccserver.getName();
            if (!serverName || !*serverName)
                continue;

            SCMStringBuffer queueName;
            getEclCCServerQueueNames(queueName, serverName);
            if (queueName.length() < 1)
                continue;

            Owned <IStringIterator> targetClusters = getTargetClusters(eqEclCCServer, serverName);
            if (!targetClusters->first())
                continue;
            
            ForEach (*targetClusters)
            {
                SCMStringBuffer targetCluster;
                targetClusters->str(targetCluster);

                QueueWrapper queue(targetCluster.str(), queueName.str());
                CJobQueueContents contents;
                queue->copyItems(contents);
                unsigned count=0;
                Owned<IJobQueueIterator> iter = contents.getIterator();
                ForEach(*iter) 
                {
                    if (isInWuList(aws, iter->query().queryWUID()))
                        continue;

                    Owned<IEspActiveWorkunit> wu(new CActiveWorkunitWrapper(context, iter->query().queryWUID(),++count));
                    wu->setServer("ECLCCserver");
                    wu->setInstance(serverName);
                    wu->setQueueName(serverName);

                    aws.append(*wu.getLink());
                }
            }
        }

        int j = runningQueues;
        while ( j > 0)
        {
            j--;
            DBGLOG("runningQueueName=<%s>: runningJobs=<%d>", runningQueueNames[j].str(), runningJobsInQueue[j]);
        }
#if 0
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        envFactory->validateCache();

        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
#else
        Owned<IPropertyTree> pEnvRoot = dummy.getEnvironment("");
        if (!pEnvRoot)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Failed to get environment information.");
#endif

        StringBuffer dirxpath;
        dirxpath.append("Software/DfuServerProcess");
        Owned<IPropertyTreeIterator> services = pEnvRoot->getElements(dirxpath);

        //Owned<IRemoteConnection> conn1 = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        //if (conn1)
        {   
            //IPropertyTree* pEnvSoftware = conn1->queryRoot();
            //Owned<IPropertyTreeIterator> services= pEnvSoftware->getElements("DfuServerProcess");
            if (services->first()) 
            {
                do 
                {
                    IPropertyTree &serviceTree = services->query();
                    const char *queuename = serviceTree.queryProp("@queue");
                    if (queuename && *queuename)
                    {
                        StringArray queues;
                        loop
                        {
                            StringAttr subq;
                            const char *comma = strchr(queuename,',');
                            if (comma) 
                                subq.set(queuename,comma-queuename);
                            else
                                subq.set(queuename);
                            bool added;
                            const char *s = strdup(subq.get());
                            queues.bAdd(s, stringcmp, added);
                            if (!added)
                                free((void *)s);
                            if (!comma)
                                break;
                            queuename = comma+1;
                            if (!*queuename)
                                break;
                        }
                        ForEachItemIn(q, queues)
                        {
                            const char *queuename = queues.item(q);
                            StringAttrArray wulist;
                            unsigned running = queuedJobs(queuename, wulist);
                            ForEachItemIn(i, wulist)
                            {
                                const char *wuid = wulist.item(i).text.get();
                                try
                                {
                                    StringBuffer jname, uname, state;
                                    Owned<IConstDFUWorkUnit> wu = getDFUWorkUnitFactory()->openWorkUnit(wuid, false);
                                    if (wu)
                                    {
                                        wu->getUser(uname);
                                        wu->getJobName(jname);
                                        if (i<running)
                                            state.append("running");
                                        else
                                            state.append("queued");

                                        Owned<IEspActiveWorkunit> wu1(new CActiveWorkunitWrapper(wuid, uname.str(), jname.str(), state.str(), "normal"));
                                        wu1->setServer("DFUserver");
                                        wu1->setInstance(queuename);
                                        wu1->setQueueName(queuename);
                                    
                                        aws.append(*wu1.getLink());
                                    }
                                }
                                catch (IException *e)
                                {
                                    StringBuffer msg;
                                    Owned<IEspActiveWorkunit> wu1(new CActiveWorkunitWrapper(wuid, "", "", e->errorMessage(msg).str(), "normal"));
                                    wu1->setServer("DFUserver");
                                    wu1->setInstance(queuename);
                                    wu1->setQueueName(queuename);
                                    
                                    aws.append(*wu1.getLink());
                                }
                            }
                        }
                    }
                } while (services->next());
            }
        }
        resp.setThorClusters(ThorClusters);
        resp.setRunning(aws);

        clusters.kill();
        dummy.getClusterProcessList(eqHoleCluster,clusters);
        IArrayOf<IEspHoleCluster> HoleClusters;
        ForEachItemIn(y, clusters)
        {
            IEspTpCluster& cluster = clusters.item(y);
            IEspHoleCluster* returnCluster = new CHoleCluster("","");

            returnCluster->setClusterName(cluster.getName());
            returnCluster->setDataModel(cluster.getDataModel());
            HoleClusters.append(*returnCluster);
        }
        resp.setHoleClusters(HoleClusters);

        IArrayOf<IEspDFUJob> jobs;
        conn.setown(querySDS().connect("DFU/RECOVERY",myProcessSession(),0, INFINITE));
        if (conn) 
        {
            Owned<IPropertyTreeIterator> it(conn->queryRoot()->getElements("job"));
            ForEach(*it) 
            {
                IPropertyTree &e=it->query();
                if (e.getPropBool("Running",false)) 
                {
                    unsigned done;
                    unsigned total;
                    countProgress(&e,done,total);
                    Owned<IEspDFUJob> job = new CDFUJob("","");

                    job->setTimeStarted(e.queryProp("@time_started"));
                    job->setDone(done);
                    job->setTotal(total);

                    StringBuffer cmd;
                    cmd.append(e.queryProp("@command")).append(" ").append(e.queryProp("@command_parameters"));
                    job->setCommand(cmd.str());
                    jobs.append(*job.getLink());
                }
            }
        }

#if 0 // handled by queuedJobs list already 
        conn.setown(querySDS().connect("DFU/WorkUnits",myProcessSession(),0, INFINITE));
        if (conn) 
        {
            Owned<IPropertyTreeIterator> it(conn->getElements("*[Recovery/Running]"));
            ForEach(*it) 
            {
                IPropertyTree &e=it->query();
                IPropertyTree *recovery  = e.queryPropTree("Recovery");
                if (recovery) 
                {
                    if (recovery->getPropBool("Running",false)) 
                    {
                        unsigned done;
                        unsigned total;
                        countProgress(recovery,done,total);

                        Owned<IEspDFUJob> job = new CDFUJob("","");

                        job->setTimeStarted(e.queryProp("Progress/@timestarted"));
                        job->setDone(done);
                        job->setTotal(total);

                        StringBuffer cmd;
                        cmd.append(e.queryName());
                        const char *user = e.queryProp("@submitID");
                        if (user)
                            cmd.append(" (").append(user).append(')');

                        job->setCommand(cmd.str());

                        jobs.append(*job.getLink());

                    }
                }
            }
        }
#endif

        resp.setDFUJobs(jobs);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}


void CWsSMCEx::addCapabilities(IPropertyTree* pFeatureNode, const char* access, 
                                         IArrayOf<IEspCapability>& capabilities)
{
    //get [Read|Write|Full]/Capability nodes under the feature node based on access value
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

        QueueWrapper queue(req.getClusterType(), req.getCluster());
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

        QueueWrapper queue(req.getClusterType(), req.getCluster());
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

        QueueWrapper queue(req.getClusterType(), req.getCluster());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index<queue->ordinality())
        {
        //  if(!queue->moveToTail(req.getWuid()))
        //      throw MakeStringException(0,"Failed to move %s",req.getWuid());
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

        QueueWrapper queue(req.getClusterType(), req.getCluster());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index>0 && index<queue->ordinality())
        {
            //if(!queue->moveToHead(req.getWuid()))
            //  throw MakeStringException(0,"Failed to move %s",req.getWuid());
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

        QueueWrapper queue(req.getClusterType(), req.getCluster());
        QueueLock lock(queue);
        
        unsigned index=queue->findRank(req.getWuid());
        if(index<queue->ordinality())
        {
            if(!queue->cancelInitiateConversation(req.getWuid()))
                throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Failed to remove the workunit %s",req.getWuid());
        }

        AccessSuccess(context, "Removed job %s",req.getWuid());
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

        QueueWrapper(req.getClusterType(), req.getCluster())->stop();
        AccessSuccess(context, "Stopped queue %s",req.getCluster());
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

        QueueWrapper(req.getClusterType(), req.getCluster())->resume();
        AccessSuccess(context, "Resumed queue %s",req.getCluster());
        resp.setRedirectUrl("/WsSMC/");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsSMCEx::onPauseQueue(IEspContext &context, IEspSMCQueueRequest &req, IEspSMCQueueResponse &resp)
{
    try
    {
        checkAccess(context,THORQUEUE_FEATURE,SecAccess_Full);

        QueueWrapper(req.getClusterType(), req.getCluster())->pause();
        AccessSuccess(context, "Paused queue %s",req.getCluster());
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
        QueueWrapper queue(req.getClusterType(), req.getCluster());
        {
            QueueLock lock(queue);
            for(unsigned i=0;i<queue->ordinality();i++)
                secAbortWorkUnit(queue->getItem(i)->queryWUID(), *context.querySecManager(), *context.queryUser());
            queue->clear();
        }
        AccessSuccess(context, "Cleared queue %s",req.getCluster());
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
        Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
        Owned<IWorkUnit> lw = factory->updateWorkUnit(req.getWuid());

        if(stricmp(req.getPriority(),"high")==0)
            lw->setPriority(PriorityClassHigh);
        else if(stricmp(req.getPriority(),"normal")==0)
            lw->setPriority(PriorityClassNormal);
        else if(stricmp(req.getPriority(),"low")==0)
            lw->setPriority(PriorityClassLow);

        // set job priority
        int priority = lw->getPriorityValue();  
        QueueWrapper queue(req.getClusterType(), req.getCluster());
        QueueLock lock(queue);
        queue->changePriority(req.getWuid(),priority);

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

        CTpWrapper dummy;
        IArrayOf<IEspTpCluster> clusters;
        dummy.getClusterProcessList(eqThorCluster,clusters,true);

        IArrayOf<IEspThorCluster> ThorClusters;
        ForEachItemIn(x, clusters)
        {
            IEspTpCluster& cluster = clusters.item(x);
            IEspThorCluster* returnCluster = new CThorCluster("","");
                
            returnCluster->setClusterName(cluster.getName());
            returnCluster->setQueueName(cluster.getQueueName());

            QueueWrapper queue(cluster.getQueueName());
            if(queue->stopped())
                returnCluster->setQueueStatus("stopped");
            else if (queue->paused())
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
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(!secmgr || !secmgr->isSuperUser(context.queryUser()))
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "access denied, administrators only.");

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

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = factory->openEnvironmentByFile();

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

        //Now, get a list of resources stored inside the ESP box
        IArrayOf<IEspHPCCResourceRepository> resourceRepositories;

        Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
        const char* ossInstall = pEnvRoot->queryProp("EnvSettings/path");
        if (!ossInstall || !*ossInstall)
        {
            DBGLOG("Failed to get EnvSettings/Path in environment settings.");
            return true;
        }

        StringBuffer path;
        path.appendf("%s/componentfiles/files/downloads", ossInstall);
        Owned<IFile> f = createIFile(path.str());
        if(!f->exists() || !f->isDirectory())
        {
            DBGLOG("Invalid resource folder");
            return true;
        }

        Owned<IDirectoryIterator> di = f->directoryFiles(NULL, false, true);
        if(di.get() == NULL)
        {
            DBGLOG("Resource folder is empty.");
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
                DBGLOG("Description file not found for %s", folder.str());
                continue;
            }

            OwnedIFileIO rIO = f0->openShared(IFOread,IFSHfull);
            if(!rIO)
            {
                DBGLOG("Failed to open the description file for %s", folder.str());
                continue;
            }

            offset_t fileSize = f0->size();
            tmpBuf.ensureCapacity((unsigned)fileSize);
            tmpBuf.setLength((unsigned)fileSize);

            size32_t nRead = rIO->read(0, (size32_t) fileSize, (char*)tmpBuf.str());
            if (nRead != fileSize)
            {
                DBGLOG("Failed to read the description file for %s", folder.str());
                continue;
            }

            Owned<IPropertyTree> desc = createPTreeFromXMLString(tmpBuf.str());
            if (!desc)
            {
                DBGLOG("Invalid description file for %s", folder.str());
                continue;
            }

            Owned<IPropertyTreeIterator> fileIterator = desc->getElements("file");
            if (!fileIterator->first())
            {
                DBGLOG("Invalid description file for %s", folder.str());
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
                        "<p style=\"text-align:centre;\">This feature is only available with the Enterprise Edition. ");
            if (link.length() > 0)
                page.append(link.str());
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
