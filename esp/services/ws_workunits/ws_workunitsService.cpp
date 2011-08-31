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
#pragma warning (disable : 4018)
#pragma warning (disable : 4244)

#include <math.h>
#include "ws_workunitsService.hpp"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jsocket.hpp"
#include "wujobq.hpp"
#include "mpbase.hpp"
#include "dllserver.hpp"
#include "wshelpers.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "jtime.ipp"
#include "dadfs.hpp"
#include "hql.hpp"
#include "TpWrapper.hpp"
#include "portlist.h"
#include "sacmd.hpp"
#include "daaudit.hpp"
#include "schedulectrl.hpp"
#include "scheduleread.hpp"
#include "eventqueue.hpp"
#include "roxiemanager.hpp"
#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif
#include "exception_util.hpp"
#include "wuwebview.hpp"

#define     File_Cpp "cpp"
#define     File_ThorLog "ThorLog"
#define     File_ThorSlaveLog "ThorSlaveLog"
#define     File_EclAgentLog "EclAgentLog"
#define     File_XML "XML"
#define     File_Res "res"
#define     File_DLL "dll"
#define     File_ArchiveQuery "ArchiveQuery"
#define     OWN_WU_URL      "OwnWorkunitsAccess"
#define     OTHERS_WU_URL   "OthersWorkunitsAccess"
#define     WUDETAILS_REFRESH_MINS 1

//19366 support adhoc / hole queries from GAB to roxie
static const long LOGFILESIZELIMIT = 500000; //Limit page size to 500k
const unsigned roxieQueryRoxieTimeOut = 60000;
const unsigned roxieQueryWUTimeOut = WAIT_FOREVER;
const unsigned MAXTHORS = 1024;
const unsigned DATA_SIZE = 16;
const unsigned AWUS_CACHE_SIZE = 16;
const unsigned AWUS_CACHE_MIN_DEFAULT = 15;
static const long MAXXLSTRANSFER = 5000000;
const char* TEMPZIPDIR = "tempzipfiles";
const unsigned MAXSUBGRAPHDAYS = 10;

class CRoxieQuery
{
public:
    StringBuffer ip;  //for roxie farm 
    unsigned port;      //for roxie farm 
    SCMStringBuffer wuid;
    StringBuffer clusterName; //Required by dali
    StringBuffer roxieClusterName; 
    StringBuffer queue;
    StringBuffer jobName;
    StringBuffer ecl;         //ecl querys
    StringBuffer status;    //status feedback from roxie
    unsigned roxieTimeOut;
    unsigned wuTimeOut;
    WUAction action;

    CRoxieQuery() {};
};

class WUJobQueue
{
public:
    WUJobQueue() {  };

    WUJobQueue(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, const char *xls) 
    {
        if(!response)
            return;
        
        unsigned maxDisplay = 125;
        IArrayOf<IEspThorQueue> items;

        CDateTime fromTime;
        CDateTime toTime;
        StringBuffer fromstr;
        StringBuffer tostr;

        if(from && *from)
        {
            fromTime.setString(from,NULL,false);
            fromTime.getString(fromstr, false);
        }

        if(to && *to)
        {
            toTime.setString(to,NULL,false);
            toTime.getString(tostr, false);
        }

        StringBuffer filter("ThorQueueMonitor");
        if(cluster && *cluster)
        {
            CTpWrapper dummy;
            IArrayOf<IEspTpCluster> clusters;
            dummy.getClusterProcessList(eqThorCluster,clusters);
            ForEachItemIn(i, clusters)
            {
                IEspTpCluster& cluster1 = clusters.item(i);
                const char* cluster0 = cluster1.getName();
                if (!cluster0 || !*cluster0)
                    continue;

                if(stricmp(cluster, cluster0)==0)
                {
                    const char* queuename = cluster1.getQueueName();
                    if (queuename && *queuename)
                        filter.appendf(",%s", queuename);

                    break;
                }
            }
        }

        StringAttrArray lines;
        queryAuditLogs(fromTime, toTime, filter.str(), lines);

        unsigned countLines = 0;
        unsigned maxConnected = 0;
        unsigned longestQueue = 0;
        ForEachItemIn(idx, lines)
        {
            const char* line = lines.item(idx).text;
            if(!line || !*line)
                continue;
            
            if (idx < (lines.length() - 1))
                getQueueInfoFromString(line, longestQueue, maxConnected, maxDisplay, 1, items);
            else
                getQueueInfoFromString(line, longestQueue, maxConnected, maxDisplay, 2, items);
            countLines++;
        }

        StringBuffer responsebuf;
        if (items.length() < 1)
        {
            responsebuf<<"<script language=\"javascript\">\r\nparent.displayQEnd(\'No data found\')</script>\r\n";
            response->sendChunk(responsebuf.str());
            return;
        }

        unsigned itemCount = items.length();
        if (itemCount > maxDisplay)
            itemCount = maxDisplay;

        responsebuf<<"<script language=\"javascript\">parent.displayQLegend()</script>\r\n";
        response->sendChunk(responsebuf.str());
        responsebuf.clear();
        responsebuf<< "<script language=\"javascript\">parent.displayQBegin(" << longestQueue << "," << maxConnected << "," << itemCount << ")</script>\r\n";
        response->sendChunk(responsebuf.str());
        responsebuf.clear();
        responsebuf<<"<script language=\"javascript\">\r\n";

        //bool displayDT = false;
        unsigned count = 0;
        unsigned jobpending=0;
        ForEachItemIn(i,items) 
        {
            IEspThorQueue& tq = items.item(i);
        
            //displayDT = !displayDT;

            count++;
            if (count > maxDisplay)
                break;

            StringBuffer countStr, dtStr;
            countStr.appendulong(count);

            //if (displayDT)
                dtStr = tq.getDT();

            responsebuf<<"parent.displayQueue(\'" << count << "\',\'" << dtStr.str() << "\',\'" << tq.getRunningWUs() << "\',"
                         "\'" << tq.getQueuedWUs() << "\',\'" << tq.getWaitingThors() <<"\',"
                         "\'" << tq.getConnectedThors() << "\',\'" << tq.getIdledThors() <<"\',"
                         "\'" << tq.getRunningWU1() << "\',\'" << tq.getRunningWU2() << "\')\r\n";
            if(++jobpending>=50)
            {
                responsebuf<<"</script>\r\n";
                response->sendChunk(responsebuf.str());
                responsebuf.clear();
                responsebuf<<"<script language=\"javascript\">\r\n";
                jobpending=0;
            }
        }

        StringBuffer countStr;
        countStr.appendulong(count);

        //<table><tr><td>Total: '+(max_queues)+' graphs (<a href=\"/WsWorkunits/WUClusterJobXLS?' + xls +'\">xls</a>...<a href=\"/WsWorkunits/WUClusterJobSummaryXLS?' + xls +'\">summary</a>)</td></tr></table>
        StringBuffer msg;
        msg << "<table><tr><td>";
        msg << "Total Records in the Time Period: " << items.length() << " (<a href=\"/WsWorkunits/WUClusterJobQueueLOG?" << xls << "\">txt</a>...<a href=\"/WsWorkunits/WUClusterJobQueueXLS?" << xls << "\">xls</a>).";
        msg << "</td></tr><tr><td>";

        if (count > maxDisplay)
            msg << "Displayed: First " << maxDisplay << ". ";
        msg << "Max. Queue Length: " << longestQueue << ".";
        msg << "</td></tr></table>";

        responsebuf<<"parent.displayQEnd(\'"<< msg <<"\')</script>\r\n";
        response->sendChunk(responsebuf.str());
    }

    void getQueueInfoFromString(const char* line, unsigned& longestQueue, unsigned& maxConnected, unsigned maxDisplay, unsigned showAll, IArrayOf<IEspThorQueue>& items)
    {
        //2009-08-12 02:44:12 ,ThorQueueMonitor,thor400_88_dev,0,0,1,1,114,---,---
        if(!line || !*line)
            return;

        Owned<IEspThorQueue> tq = createThorQueue();
        StringBuffer dt, runningWUs, queuedWUs, waitingThors, connectedThors, idledThors, runningWU1, runningWU2;

        // date/time
        const char* bptr = line;
        const char* eptr = strchr(bptr, ',');
        if(eptr)
            dt.append(eptr - bptr, bptr);
        else
            dt.append(bptr);

        tq->setDT(dt.str());
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //skip title
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //skip queue name
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //running
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            runningWUs.append(eptr - bptr, bptr);
        else
            runningWUs.append(bptr);

        tq->setRunningWUs(runningWUs.str());
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //queued
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            queuedWUs.append(eptr - bptr, bptr);
        else
            queuedWUs.append(bptr);

        if (maxDisplay > items.length())
        {
            unsigned queueLen = atoi(queuedWUs.str());
            if (queueLen > longestQueue)
                longestQueue = queueLen;
        }

        tq->setQueuedWUs(queuedWUs.str());
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //waiting
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            waitingThors.append(eptr - bptr, bptr);
        else
            waitingThors.append(bptr);

        tq->setWaitingThors(waitingThors.str());
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //connected
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            connectedThors.append(eptr - bptr, bptr);
        else
            connectedThors.append(bptr);

        if (maxDisplay > items.length())
        {
            unsigned connnectedLen = atoi(connectedThors.str());
            if (connnectedLen > maxConnected)
                maxConnected = connnectedLen;
        }

        tq->setConnectedThors(connectedThors.str());
        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //idled
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            idledThors.append(eptr - bptr, bptr);
        else
            idledThors.append(bptr);

        tq->setIdledThors(idledThors.str());
        if(!eptr)
        {
            items.append(*tq.getClear());
            return;
        }

        //runningWU1
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            runningWU1.append(eptr - bptr, bptr);
        else
        {
            runningWU1.append(bptr);
        }

        if (!strcmp(runningWU1.str(), "---"))
            runningWU1.clear();

        if (runningWU1.length() > 0)
            tq->setRunningWU1(runningWU1.str());

        if(!eptr)
        {
            if (checkNewThorQueueItem(tq, showAll, items))
                items.append(*tq.getClear());
            return;
        }

        //runningWU2
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr)
            runningWU2.append(eptr - bptr, bptr);
        else
        {
            runningWU2.append(bptr);
        }

        if (!strcmp(runningWU2.str(), "---"))
            runningWU2.clear();

        if (runningWU2.length() > 0)
            tq->setRunningWU2(runningWU2.str());

        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
    
        DBGLOG("Queue log: [%s]", line);
    }

    bool checkSameStrings(const char* s1, const char* s2)
    {
        if (s1)
        {
            if (!s2)
                return false;
            if (strcmp(s1, s2))
                return false;
        }
        else if (s2)
        {
            if (!s1)
                return false;
        }
            
        return true;
    }

    bool checkNewThorQueueItem(IEspThorQueue* tq, unsigned showAll, IArrayOf<IEspThorQueue>& items)
    {
        bool bAdd = false;
        if (showAll < 1) //show every lines
            bAdd = true;
        else if (items.length() < 1)
            bAdd = true;
        else if (showAll > 1) //last line now
        {
            IEspThorQueue& tq0 = items.item(items.length()-1);
            if (!checkSameStrings(tq->getDT(), tq0.getDT()))
                bAdd = true;
        }
        else
        {
            IEspThorQueue& tq0 = items.item(items.length()-1);
            if (!checkSameStrings(tq->getRunningWUs(), tq0.getRunningWUs()))
                bAdd = true;
            if (!checkSameStrings(tq->getQueuedWUs(), tq0.getQueuedWUs()))
                bAdd = true;
            if (!checkSameStrings(tq->getConnectedThors(), tq0.getConnectedThors()))
                bAdd = true;
            if (!checkSameStrings(tq->getConnectedThors(), tq0.getConnectedThors()))
                bAdd = true;
            if (!checkSameStrings(tq->getRunningWU1(), tq0.getRunningWU1()))
                bAdd = true;
            if (!checkSameStrings(tq->getRunningWU2(), tq0.getRunningWU2()))
                bAdd = true;
        }

        return bAdd;
    }

};

//clusterType: RoxieCluster, ThorCluster, or HoleCluster
//processName: RoxieServerProcess[1], ThorMasterProcess, or HoleControlProcess
void getClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
#if 0
    Owned<IConstEnvironment> environment = factory->openEnvironment();
#else
    Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
#endif
    Owned<IPropertyTree> pRoot = &environment->getPTree();

    StringBuffer xpath;
    xpath.appendf("Software/%s[@name='%s']", clusterType, clusterName);

    IPropertyTree* pCluster = pRoot->queryPropTree( xpath.str() );
    if (!pCluster)
        throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "'%s %s' is not defined.", clusterType, clusterName);

    xpath.clear().append(processName);
    xpath.append("@computer");
    const char* computer = pCluster->queryProp(xpath.str());
    if (!computer || strlen(computer) < 1)
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "'%s %s: %s' is not defined.", clusterType, clusterName, processName);

    xpath.clear().append(processName);
    xpath.append("@port");
    const char* port = pCluster->queryProp(xpath.str());

    Owned<IConstMachineInfo> pMachine = environment->getMachine(computer);
    if (pMachine)
    {
        SCMStringBuffer scmNetAddress;
        pMachine->getNetAddress(scmNetAddress);
        netAddress = scmNetAddress.str();
#ifdef MACHINE_IP
        if (!strcmp(netAddress.str(), "."))
            netAddress = MACHINE_IP;
#endif
        netAddress.appendf(":%s", port);
    }
    
    return;
}

void getClusterName(IEspContext& context, char const *wuid, StringBuffer& clusterName)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
   Owned<IWorkUnit> wu(factory->updateWorkUnit(wuid));
    if (wu)
    {
        SCMStringBuffer cluster;
        wu->getClusterName(cluster);    
        if (cluster.length() > 0)
        {
            clusterName = cluster.str();
        }
    }

    return;
}

void getFirstThorClusterName(StringBuffer& clusterName)
{
    /*try
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        if (conn)
        {
            IPropertyTree* root = conn->queryRoot();
            IPropertyTree* pSoftware = root->queryPropTree("Software");
            if (!pSoftware)
                return;

            const char* ClusterType = "";
            Owned<IPropertyTreeIterator> clusters= pSoftware->getElements(eqThorCluster);
            if (clusters->first()) 
            {
                do 
                {
                    IPropertyTree &cluster = clusters->query();                 
                    const char* name = cluster.queryProp("@name");

                    if (name && *name)
                    {
                        clusterName.append(name);
                        break;
                    }
                } while (clusters->next());
            }
        }
    }
    catch(IException* e)
    {   
        StringBuffer msg;
        e->errorMessage(msg);
        WARNLOG(msg.str());
        e->Release();
    }
    catch(...)
    {
        WARNLOG("Unknown Exception caught within CTpWrapper::getClusterList");
    }*/

    bool bFound = false;
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> constEnv = factory->openEnvironmentByFile();
    Owned<IPropertyTree> root = &constEnv->getPTree();
    if (!root)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    Owned<IPropertyTreeIterator> clusterIterator = root->getElements("Software/Topology/Cluster");
    if (clusterIterator->first()) 
    {
        do {
            IPropertyTree &cluster = clusterIterator->query();
            
            Owned<IPropertyTreeIterator> clusterIterator1 = cluster.getElements("ThorCluster");
            if (clusterIterator1->first())
            {
                do {
                    IPropertyTree &cluster1 = clusterIterator1->query();
                    char* name = (char*) cluster1.queryProp("@process");
                    if (name && *name)
                    {
                        clusterName.append(name);
                        bFound = true;
                    }
                } while (!bFound && clusterIterator1->next());
            }
        } while (!bFound && clusterIterator->next());
    }

    return;
}

bool isRoxieCluster(char const * targetClusterName)
{
    bool bReturn = false;
    IArrayOf<IEspTpLogicalCluster> roxieclusters;
    CTpWrapper wrapper;
    wrapper.getTargetClusterList(roxieclusters, eqRoxieCluster, NULL);
    ForEachItemIn(i, roxieclusters)
    {
        IEspTpLogicalCluster& roxiecluster = roxieclusters.item(i);
        const char *name = roxiecluster.getName();
        if (name && !stricmp(name, targetClusterName))
        {
            bReturn = true;
            break;
        }
    }
    return bReturn;
}

void formatDuration(StringBuffer &ret, unsigned ms)
{
    unsigned hours = ms / (60000*60);
    ms -= hours*(60000*60);
    unsigned mins = ms / 60000;
    ms -= mins*60000;
    unsigned secs = ms / 1000;
    ms -= secs*1000;
    bool started = false;
    if (hours > 24)
    {
        ret.appendf("%d days ", hours / 24);
        hours = hours % 24;
        started = true;
    }
    if (hours || started)
    {
        ret.appendf("%d:", hours);
        started = true;
    }
    if (mins || started)
    {
        ret.appendf("%d:", mins);
        started = true;
    }
    if (started)
        ret.appendf("%02d.%03d", secs, ms);
    else
        ret.appendf("%d.%03d", secs, ms);
}

void adjustRowvalues(IPropertyTree* xgmml, const char* popupId)
{
    const bool bPopupId = popupId && *popupId;
    StringBuffer xpath;
    if (bPopupId)
        xpath.appendf("//%s[@id='%s']", strchr(popupId, '_') ? "edge" : "node", popupId);
    else
        xpath.append("//*[att]");

    Owned<IPropertyTreeIterator> nodesAndEdges = xgmml->getElements(xpath.str());
    ForEach(*nodesAndEdges)
    {
        IPropertyTree* pNodeOrEdge = &nodesAndEdges->query();
        Owned<IPropertyTreeIterator> attrs = pNodeOrEdge->getElements("att[@name='rowvalues']");
        ForEach(*attrs)
        {
            IPropertyTree &value = attrs->query();
            const char* id = value.queryProp("@id");

            try
            {
                IPropertyTree *rv_dataset=value.setPropTree("Dataset",createPTreeFromXMLString(value.queryProp("@value"), ipt_caseInsensitive));
                value.setProp("@value","");
                    rv_dataset->setProp("@xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");

            }
            catch (IException *e)
            {
                StringBuffer errmsg;
                ERRLOG("Exception when generating row values - %s", e->errorMessage(errmsg).str());
                e->Release();
            }
        }
    }
}

int waitCompileWU(const char *wuid, IEspContext &context, bool wait)
{
    try
    {
        secWaitForWorkUnitToCompile(wuid, *context.querySecManager(), *context.queryUser(), -1);
        return CWUWrapper(wuid, context)->getState();
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return WUStateFailed;
}

void submitQueryWU(const char *wuid, IEspContext& context, CRoxieQuery& roxieQuery, bool allowNewRoxieOnDemandQuery)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
    Owned<IConstWorkUnit> workunit = factory->openWorkUnit(wuid, false);
    SocketEndpoint ep;
    ep.set(roxieQuery.ip);

    StringBuffer daliIp;
    if (workunit->hasDebugValue("lookupDaliIp"))
    {
        SCMStringBuffer ip;
        workunit->getDebugValue("lookupDaliIp", ip);
        daliIp.append(ip.str());
    }
    else
    {
        const SocketEndpoint & ep1 = queryCoven().queryComm().queryGroup().queryNode(0).endpoint();
        ep1.getUrlStr(daliIp);
    }

    StringBuffer user;
    StringBuffer password;
    context.getUserID(user);
    context.getPassword(password);

    SCMStringBuffer jobName;
    workunit->getJobName(jobName);
    if (jobName.length() == 0)
    {
        jobName.set(roxieQuery.jobName.str());
        if (!jobName.length())
            jobName.set(wuid);
    }

    Owned<IRoxieQueryManager> manager = createRoxieQueryManager(ep, roxieQuery.roxieClusterName.str(), daliIp, roxieQuery.roxieTimeOut, user.str(), password.str(), 1);  // MORE - use 1 as default traceLevel
    SCMStringBuffer result; // not used since we request result to be placed in WU - MORE - clean this up
    if (roxieQuery.action == WUActionExecuteExisting)
    {
        manager->runQuery(workunit, jobName.str(), false, allowNewRoxieOnDemandQuery, result);
    }
    else if (roxieQuery.action != WUActionCompile)
    {
        Owned<IRoxieQueryProcessingInfo> processingInfo = createRoxieQueryProcessingInfo();
        processingInfo->setResolveFileInfo(true);
        processingInfo->setLoadDataOnly(false);
        processingInfo->setResolveFileInfo(true);
        processingInfo->setNoForms(false);
        processingInfo->setDfsDaliIp(daliIp.str());
        processingInfo->setResolveKeyDiffInfo(false);
        processingInfo->setCopyKeyDiffLocationInfo(false);
        processingInfo->setLayoutTranslationEnabled(false);

        SCMStringBuffer wuCluster;
        workunit->getClusterName(wuCluster);
        Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(wuCluster.str());
        SCMStringBuffer querySetName;
        clusterInfo->getQuerySetName(querySetName);

        SCMStringBuffer deployStatus;

        manager->publishWorkunit(workunit, jobName, *processingInfo.get(), user.str(), DO_NOT_ACTIVATE, querySetName.str(), true, result, deployStatus);
        SCMStringBuffer currentQueryName;
        workunit->getDebugValue("queryid", currentQueryName);
        manager->runQuery(workunit, currentQueryName.str(), false, allowNewRoxieOnDemandQuery, result);
        if (!workunit->getDebugValueBool("@leaveWuInQuerySet", 0))
            manager->deleteQuery(currentQueryName.str(), querySetName.str(), true, deployStatus);

        Owned<IWorkUnit> wu0(factory->updateWorkUnit(wuid));
        wu0->setState(WUStateCompleted);
        wu0->commit();
        wu0.clear();
    }
}

void compileScheduledQueryWU(IEspContext& context, CRoxieQuery& roxieQuery)
{
    SocketEndpoint ep;
    ep.set(roxieQuery.ip);

    const SocketEndpoint & ep1 = queryCoven().queryComm().queryGroup().queryNode(0).endpoint();
    StringBuffer daliIp;
    ep1.getUrlStr(daliIp);

    StringBuffer user;
    StringBuffer password;
    context.getUserID(user);
    context.getPassword(password);

    Owned<IRoxieQueryManager> manager = createRoxieQueryManager(ep, roxieQuery.roxieClusterName.str(), daliIp, roxieQuery.roxieTimeOut, user.str(), password.str(), 1);  // MORE - use 1 as default traceLevel
    Owned<IRoxieQueryCompileInfo> compileInfo = createRoxieQueryCompileInfo(roxieQuery.ecl.str(),
                                                                            roxieQuery.jobName.str(),
                                                                            roxieQuery.clusterName.str(),
                                                                            "WS_WORKUNITS");
    compileInfo->setWuTimeOut(roxieQuery.wuTimeOut);

    Owned<IRoxieQueryProcessingInfo> processingInfo = createRoxieQueryProcessingInfo();
    processingInfo->setResolveFileInfo(true);
    SCMStringBuffer generatedQueryName;
    generatedQueryName.set(roxieQuery.wuid.str());

    SCMStringBuffer status;
    manager->compileQuery(roxieQuery.wuid, generatedQueryName, *compileInfo.get(), *processingInfo.get(), roxieQuery.roxieClusterName.str(), status); // not interested in warnings
        
    roxieQuery.status.append(status.str());
    return;
}

////////////////////////////////////////////////////////////////

bool switchWorkunitQueue(IWorkUnit* wu, const char *wuid, const char *cluster)
{
    if (!wu)
        return false;

    class cQswitcher: public CInterface, implements IQueueSwitcher
    {
    public:
        IMPLEMENT_IINTERFACE;
        void * getQ(const char * qname, const char * wuid)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            return q->take(wuid);
        }
        void putQ(const char * qname, const char * wuid, void * qitem)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            q->enqueue((IJobQueueItem *)qitem);
        }
        bool isAuto()
        {
            return false;
        }
    } switcher;

    return wu->switchThorQueue(cluster, &switcher);
}

class CScheduleDateTime : public CScmDateTime, public IDateTime
{
public:
    IMPLEMENT_IINTERFACE;
    CScheduleDateTime()
    {
        setSimpleLocal(0);
    }

    bool isValid() 
    { 
        unsigned year, month, day;
        cdt.getDate(year, month, day, true);
        return year>1969;
    }

    virtual IStringVal & getGmtString(IStringVal & str) const 
    {
        return CScmDateTime::getGmtString(str);
    }
    virtual IStringVal & getLocalString(IStringVal & str) const 
    {
        return CScmDateTime::getLocalString(str);
    }
    virtual IStringVal & getGmtDateString(IStringVal & str) const 
    {
        return CScmDateTime::getGmtDateString(str);
    }
    virtual IStringVal & getLocalDateString(IStringVal & str) const 
    {
        return CScmDateTime::getLocalDateString(str);
    }
    virtual IStringVal & getGmtTimeString(IStringVal & str) const 
    {
        return CScmDateTime::getGmtTimeString(str);
    }
    virtual IStringVal & getLocalTimeString(IStringVal & str) const 
    {
        return CScmDateTime::getLocalTimeString(str);
    }
    virtual void setGmtString(const char * pstr) 
    {
        CScmDateTime::setGmtString(pstr);
    }
    virtual void setLocalString(const char * pstr) 
    {
        CScmDateTime::setLocalString(pstr);
    }
    virtual void setGmtDateString(const char * pstr) 
    {
        CScmDateTime::setGmtDateString(pstr);
    }
    virtual void setLocalDateString(const char * pstr) 
    {
        CScmDateTime::setLocalDateString(pstr);
    }
    virtual void setGmtTimeString(const char * pstr) 
    {
        CScmDateTime::setGmtTimeString(pstr);
    }
    virtual void setLocalTimeString(const char * pstr) 
    {
        CScmDateTime::setLocalTimeString(pstr);
    }
};

int WUSchedule::run()
{
    try
    {
        while(true)
        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IConstWorkUnitIterator> itr = factory->getWorkUnitsByState(WUStateScheduled);
            if (itr)
            {
                itr->first();
                while(itr->isValid())
                {
                    try
                    {
                        IConstWorkUnit & wu = itr->query();
                        if (wu.aborting()) 
                        {
                            Owned<IWorkUnit> lwu = &wu.lock(); //wu.lock() cannot return NULL (if it fails will throw an exception)
                            lwu->setState(WUStateAborted);
                            lwu->commit();
                            itr->next();
                            continue;
                        }

                        CScheduleDateTime dt, now;
                        now.setNow();
                        wu.getTimeScheduled(dt);
                        if (now.compare(dt) < 0)
                        {
                            itr->next();
                            continue;
                        }

                        SCMStringBuffer wuid, cluster;
                        wu.getWuid(wuid);
                        wu.getClusterName(cluster);
                        if (!isRoxieCluster(cluster.str()))
                        {
                            {
                                Owned<IWorkUnit> lwu = &wu.lock();
                                lwu->setState(WUStateSubmitted);
                                lwu->commit();
                            } //Release the lock here. Otherwise, submitWorkUnit() will not work.
                            
                            SCMStringBuffer user, password, token;
                            wu.getSecurityToken(token).str();
                            extractToken(token.str(), wuid.str(), user, password);
                            submitWorkUnit(wuid.str(), user.str(), password.str());
                        }
                    }
                    catch(IException *e)
                    {
                        StringBuffer msg;
                        ERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                        e->Release();
                    }

                    itr->next();
                }
            }
            sleep(60);
        }
    }
    catch(IException *e)
    {
        StringBuffer msg;
        ERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception in WsWorkunits Schedule::run");
    }

    if (m_container)
        m_container->exitESP();
    return 0;
}

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

inline MemoryBuffer& operator<<(MemoryBuffer& buf, const char* s)
{
    buf.append(strlen(s),s);
    return buf;
}

static const char *getNum(const char *s,unsigned &num)
{
    while (*s&&!isdigit(*s))
        s++;
    num = 0;
    while (isdigit(*s)) {
        num = num*10+*s-'0';
        s++;
    }
    return s;
}

class CQuery: public CWUWrapper
{
public:
    CQuery(const char* wuid, IEspContext &context): CWUWrapper(wuid, context),  query(wu->getQuery())
    {
        if(!query)
            throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid);
    }

    operator IConstWUQuery* ()  { return query.get(); }
    IConstWUQuery* operator->() { return query.get(); }

    IConstWorkUnit* getWU() { return wu; }

protected:
    Owned<IConstWUQuery> query;
}; 


class NewWorkunit
{
public:
    NewWorkunit(IEspContext &context): 
      factory(context.querySecManager() ? getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser()) : getWorkUnitFactory()), 
      wu(factory->createWorkUnit(NULL, "ws_workunits", context.queryUserId()))
    {
        if(!wu)
          throw MakeStringException(ECLWATCH_CANNOT_CREATE_WORKUNIT,"Cannot create a workunit.");
        wu->setUser(context.queryUserId());
    }

    operator IWorkUnit* ()  { return wu.get(); }
    IWorkUnit* operator->() { return wu.get(); }

    void setECL(const char* ecl)
    {
        Owned<IWUQuery> query = wu->updateQuery();
        query->setQueryText(ecl);
    }


private:
    Owned<IWorkUnitFactory> factory;
    Owned<IWorkUnit> wu;
};

inline bool MatchPrefix(const char* str, const char* substr)
{
    return str!=NULL && strncmp(str, substr, strlen(substr) )==0 ;
}

void CWsWorkunitsSoapBindingEx::addLogicalClusterByName(const char* clusterName, StringArray& clusters, StringBuffer& x)
{
    bool bAdd = true;
    for(int j = 0; j < clusters.length(); j++)
    {
        StringBuffer cluster1 = clusters.item(j);
        if (stricmp(cluster1.str(), clusterName) == 0)
        {
            bAdd = false;
            break;
        }
    }

    if (bAdd)
    {
        clusters.append(clusterName);
      x.append("<Cluster>").append(clusterName).append("</Cluster>");
    }

    return;
}

int CWsWorkunitsSoapBindingEx::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    try
    {
        if(stricmp(method,"WUQuery")==0)
        {
            SecAccessFlags accessOwn;
            SecAccessFlags accessOthers;
            if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
                accessOwn = SecAccess_None;

            if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
                accessOthers = SecAccess_None;

            StringBuffer html,x("<WUQuery>");

            //throw exception if no access on both own/others' workunits
            if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
            {
                x.appendf("<ErrorMessage>Access to workunit is denied.</ErrorMessage>");
            }
            else
            {
                StringArray clusters;

                Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
                Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
                Owned<IPropertyTree> root = &environment->getPTree();
                if (!root)
                    throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

                Owned<IPropertyTreeIterator> clusterIterator = root->getElements("Software/Topology/Cluster");
                if (clusterIterator->first()) 
                {
                    do {
                        IPropertyTree &cluster = clusterIterator->query();
                        const char *clusterName = cluster.queryProp("@name");
                        if (!clusterName || !*clusterName)
                            continue;

                        addLogicalClusterByName(clusterName, clusters, x);
                    } while (clusterIterator->next());
                }


             /* There is only one sasha server per dali
            IArrayOf<IEspTpSashaServer> sashaservers;
            dummy.getServiceList("SashaServerProcess",  (IArrayOf<struct IInterface> &)sashaservers);   
            ForEachItemIn(i1, sashaservers)
             {
                 IEspTpSashaServer& sashaserver = sashaservers.item(i1);
                IArrayOf<IConstTpMachine> &sashaservermachine = sashaserver.getTpMachines();
                 x.appendf("<SashaServer Address=\"%s\">", sashaservermachine.item(0).getNetaddress());
                x.append(sashaserver.getName()).append("</SashaServer>");
            }*/
            }
            
              x.append("</WUQuery>");
              CWsWorkunitsEx::xsltTransform(x.str(), StringBuffer(getCFD()).append("./smc_xslt/wuid_search.xslt").str(),NULL,html);
              response->setContent(html.str());
              response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
              response->send();

            return 0;
         }
         else if(stricmp(method,"WUJobList")==0)
         {
              StringBuffer html,x("<WUJobList>"), defcluster, range;
              request->getParameter("Cluster",defcluster);
              request->getParameter("Range",range);

              if(range.str())
                    x.append("<Range>").append(range.str()).append("</Range>");

             CTpWrapper dummy;
             IArrayOf<IEspTpCluster> clusters;
             dummy.getClusterProcessList(eqThorCluster,clusters);
             ForEachItemIn(i, clusters)
             {
                 IEspTpCluster& cluster = clusters.item(i);
                 x.append("<Cluster");
                    if(stricmp(cluster.getName(),defcluster.str())==0)
                         x.append(" selected=\"1\"");
                    x.append(" >").append(cluster.getName()).append("</Cluster>");
              }
              x.append("</WUJobList>");

              CWsWorkunitsEx::xsltTransform(x.str(), StringBuffer(getCFD()).append("./smc_xslt/jobs_search.xslt").str(),NULL,html);

              response->addHeader("Expires", "0");
              response->setContent(html.str());
              response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
              response->send();
              return 0;

         }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onGetNotFound(context, request, response, service);
}

struct SashaServers
{
    SashaServers(const char* ip=NULL)
    {
        if(ip)
            nodes.append(ip);
        else
        {
            CTpWrapper dummy;
            IArrayOf<IConstTpSashaServer> sashas;
            dummy.getTpSashaServers(sashas);
            if(sashas.length())
            {
                IArrayOf<IConstTpMachine> &list=sashas.item(0).getTpMachines();
                ForEachItemIn(i, list)
                {
                    nodes.append(list.item(i).getNetaddress());
                }
            }
        }
    }

    unsigned get(const char* cluster,const char* xsl, unsigned start, unsigned limit, unsigned timeout, StringBuffer& responsebuf)
    {
        ForEachItemIn(n, nodes)
        {

            SocketEndpoint ep;
            ep.set(nodes.item(n),DEFAULT_SASHA_PORT);
            Owned<INode> sashaserver = createINode(ep);

            Owned<ISashaCommand> cmd = createSashaCommand();    
            cmd->setAction(SCA_GET);                            
            cmd->setOnline(false);                              
            cmd->setArchived(true);                             
            cmd->setXslt(xsl);
            if(cluster && *cluster)
                cmd->setCluster(cluster);
            cmd->setLimit(limit);
            cmd->setStart(start);

            StringBuffer ips;
            sashaserver->endpoint().getIpText(ips);
            DBGLOG("Query sasha %s",ips.str());

            if (cmd->send(sashaserver,timeout)) 
            {
                unsigned num = cmd->numResults();

                DBGLOG("Got %d results from sasha",num);
                
                for (unsigned i=0;i<num;i++) 
                {
                    StringBuffer res;
                    cmd->getResult(i,res);
                    if(res.length())
                    {
                        responsebuf<<res.str();
                    }
                }
                return num;
            }
            else
            {
                StringBuffer ips;
                sashaserver->endpoint().getIpText(ips);

                ERRLOG("Cannot connect to archive server on %s",ips.str());
            }
        }
        return 0;
    }
    StringArray nodes;
};

bool readECLWUCurrentJob(const char* curjob, const char* clusterName, const char* toDate, StringBuffer& dtStr, StringBuffer& actionStr, StringBuffer& wuidStr, StringBuffer& graphStr, StringBuffer& subGraphStr, StringBuffer& clusterStr)
{
    if(!curjob || !*curjob)
        return false;

    // startdate
    const char* bptr = curjob;
    const char* eptr = strchr(bptr, ',');
    if(!eptr)
        return false;
    
    StringBuffer dt;
    dt.clear().append(eptr - bptr, bptr);
    dt.setCharAt(10, 'T');

    if (strcmp(dt.str(), toDate) > 0)
        return false;

    CDateTime enddt;
    enddt.setString(dt.str(), NULL, true);
    dt.clear();
    enddt.getString(dt, false);
    dt.append('Z');

    //Progress
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    //Thor
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    // action name
    char action[256];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    int len = eptr - bptr;
    strncpy(action, bptr, len);
    action[len] = 0;

    // cluster name
    char cluster[256];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    len = eptr - bptr;
    strncpy(cluster, bptr, len);
    cluster[len] = 0;

    if (cluster && *cluster)
    {
        clusterStr.clear().append(cluster);
        if (clusterName && *clusterName && stricmp(cluster, clusterName))
            return false;
    }

    dtStr = dt;
    actionStr.clear().append(action);

    if (!stricmp(action, "startup") || !stricmp(action, "terminate"))
        return true;

    //WUID
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    wuidStr.clear().append(eptr - bptr, bptr);

    //graph number
    char graph[32];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    len = eptr - bptr;
    if (bptr[0] == 'g' && len > 5)
    {
        bptr += 5;
        len = eptr - bptr;
    }

    strncpy(graph, bptr, len);
    graph[len] = 0;
    graphStr.clear().append(eptr - bptr, bptr);

    if (!stricmp(action, "start") || !stricmp(action, "stop"))
        return true;

    //subgraph number
    char subgraph[32];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
    {
        strcpy(subgraph, bptr);
    }
    else
    {
        len = eptr - bptr;
        strncpy(subgraph, bptr, len);
        subgraph[len] = 0;
    }
    subGraphStr.clear().append(subgraph);

    return true;
}

void addUnfinishedECLWUs(IArrayOf<IEspECLJob>& eclJobList, const char* wuid, const char* graph, const char* subGraph, 
    const char* cluster, const char* dt, const char* dt1, StringArray& unfinishedWUIDs, StringArray& unfinishedGraphs, 
    StringArray& unfinishedSubGraphs, StringArray& unfinishedClusters, StringArray& unfinishedWUStarttime, StringArray& unfinishedWUEndtime)
{
    bool bFound = false;
    ForEachItemIn(idx, eclJobList)
    {
        IConstECLJob& curECLJob = eclJobList.item(idx);
        const char *eclwuid = curECLJob.getWuid();
        const char *eclgraph = curECLJob.getGraphNum();
        const char *eclsubgraph = curECLJob.getSubGraphNum();
        const char *ecldate = curECLJob.getFinishedDate();
        if (!eclwuid || !*eclwuid || stricmp(eclwuid, wuid))
            continue;
        if (!eclgraph || !*eclgraph || stricmp(eclgraph, graph))
            continue;
        if (!eclsubgraph || !*eclsubgraph || stricmp(eclsubgraph, subGraph))
            continue;
        //if (!ecldate || !*ecldate || (stricmp(ecldate, dt) < 0) || (stricmp(ecldate, dt1) > 0))
        //  continue;
        if (!ecldate || !*ecldate)
            continue;
        int test = stricmp(ecldate, dt);
        if (test < 0) 
            continue;
        test = stricmp(ecldate, dt1);
        if (test > 0) 
            continue;

        bFound = true;
        break;
    }

    if (!bFound)
    {
        unfinishedWUIDs.append(wuid);
        StringBuffer graph0("graph");
        graph0.append(graph);
        unfinishedGraphs.append(graph0);
        unfinishedSubGraphs.append(subGraph);
        unfinishedClusters.append(cluster);
        unfinishedWUStarttime.append(dt);
        unfinishedWUEndtime.append(dt1);
    }

    return;
}

bool getPreviousUnfinishedECLWU(CDateTime fromTime, CDateTime toTime, const char* toDate, const char* cluster, 
    StringBuffer& wuidStr, StringBuffer& graphStr, StringBuffer& subGraphStr, StringBuffer& clusterStr, StringBuffer& dtStr)
{
    bool bFound = false;

    wuidStr.clear();
    graphStr.clear();
    subGraphStr.clear();
    dtStr.clear();

    StringBuffer filter0("Progress,Thor");
    CDateTime fromTime1 = fromTime, toTime1;

    bool bStop = false;
    for (unsigned day = 0; !bStop && day < MAXSUBGRAPHDAYS; day++)
    {
        toTime1 = fromTime1;
        fromTime1.adjustTime(-1440);

        StringAttrArray jobs1;
        queryAuditLogs(fromTime1, toTime1, filter0.str(), jobs1);
#if 0
        char* str1 = "2010-10-04 07:39:04 ,Progress,Thor,StartSubgraph,thor,W20100929-073902,5,1,thor,thor.thor";
        char* str2 = "2010-10-04 15:53:43 ,Progress,Thor,Startup,thor,thor,thor.thor,//10.173.51.20/c$/thor_logs/09_29_2010_15_52_39/THORMASTER.log";
        char* str3 = "2010-10-04 17:52:31 ,Progress,Thor,Start,thor,W20100929-075230,graph1,r3gression,thor,thor.thor";
        jobs1.append(*new StringAttrItem(str2, strlen(str2)));
        jobs1.append(*new StringAttrItem(str1, strlen(str1)));
        jobs1.append(*new StringAttrItem(str3, strlen(str3)));
#endif
        ForEachItemInRev(idx1, jobs1)
        {
            const char* curjob = jobs1.item(idx1).text;
            if(!curjob || !*curjob)
                continue;

            StringBuffer actionStr, clusterStr;
            if (!readECLWUCurrentJob(curjob, cluster, toDate, dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr))
                continue;

            if (!stricmp(actionStr.str(), "StartSubgraph") && (wuidStr.length() > 0) && (graphStr.length() > 0))
            {
                bFound = true;
                bStop = true;
                break;
            }
        
            if (!stricmp(actionStr.str(), "Startup") || !stricmp(actionStr.str(), "Terminate"))
            {
                bStop = true;
                break;
            }
        }
    }

    return bFound;
}

void findUnfinishedECLWUs(IArrayOf<IEspECLJob>& eclJobList, CDateTime fromTime, CDateTime toTime, const char* toDate, const char* cluster, StringArray& unfinishedWUIDs,
    StringArray& unfinishedGraphs, StringArray& unfinishedSubGraphs, StringArray& unfinishedClusters, StringArray& unfinishedWUStarttime, StringArray& unfinishedWUEndtime)
{
    StringAttrArray jobs1;
    StringBuffer filter1("Progress,Thor");
    queryAuditLogs(fromTime, toTime, filter1.str(), jobs1);

#if 0
    StringBuffer responsebuf1;
    ForEachItemIn(idx01, jobs1)
    {
        const char* curjob = jobs1.item(idx01).text;
        if(!curjob || !*curjob)
            continue;

        responsebuf1.appendf("%s\n", curjob);
    }
    DBGLOG("Progress:\n%s", responsebuf1.str());
#endif

    //Find out which WUs stopped by abnormal thor termination

    bool bAbnormalWU = false;
    int len = jobs1.length();
    StringBuffer dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr;
    ForEachItemIn(idx1, jobs1)
    {
        const char* curjob = jobs1.item(idx1).text;
        if(!curjob || !*curjob)
            continue;

        wuidStr.clear();
        graphStr.clear();
        subGraphStr.clear();

        if (!readECLWUCurrentJob(curjob, cluster, toDate, dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr))
            continue;

        if (stricmp(actionStr.str(), "Start"))
            continue;

        bAbnormalWU = true;
        int nextIndex = idx1 + 1;
        int idx2 = nextIndex;
        while (idx2 < len)
        {
            const char* curjob1 = jobs1.item(idx2).text;
            if(!curjob1 || !*curjob1)
                continue;

            StringBuffer dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1;
            if (readECLWUCurrentJob(curjob1, cluster, toDate, dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1))
            {
                if (!stricmp(wuidStr.str(), wuidStr1.str()) && !stricmp(graphStr.str(), graphStr1.str()))
                {
                    if (!stricmp(actionStr1.str(), "Stop") )
                    {
                        bAbnormalWU = false;
                        break;
                    }
                    else if (!stricmp(actionStr1.str(), "Start"))
                    {
                        break;
                    }
                }
            }

            idx2++;
        }

        //If the WU did not finish by itself, let's check whether the cluster was stopped before the WU finished. 
        if (bAbnormalWU)
        {
            int idx2 = nextIndex;
            while (idx2 < len)
            {
                const char* curjob1 = jobs1.item(idx2).text;
                if(!curjob1 || !*curjob1)
                    continue;

                StringBuffer dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1;
                if (!readECLWUCurrentJob(curjob1, cluster, toDate, dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1))
                {
                    idx2++;
                    continue;
                }

                if (!stricmp(actionStr1.str(), "StartSubgraph") && !stricmp(wuidStr.str(), wuidStr1.str()) && !stricmp(graphStr.str(), graphStr1.str()))
                {
                    //update subgraph number
                    subGraphStr.clear().append(subGraphStr1.str());
                    dtStr.clear().append(dtStr1.str());
                    clusterStr.clear().append(clusterStr1.str());
                    idx2++;
                    continue;
                }
                if (stricmp(actionStr1.str(), "Startup") && stricmp(actionStr1.str(), "Terminate"))
                {
                    idx2++;
                    continue;
                }   
                
                addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), dtStr1.str(), 
                    unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

                bAbnormalWU = false;
                break;
            }

            if (bAbnormalWU)
            {
                addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), toDate, 
                    unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

                bAbnormalWU = false;
            }
        }
    }

    //What if a WU started before *and* ended after the search time range
    if ((eclJobList.length() < 1) && (unfinishedWUIDs.length() < 1))
    {
        if (getPreviousUnfinishedECLWU(fromTime, toTime, toDate, cluster, wuidStr, graphStr, subGraphStr, clusterStr, dtStr))
        {
            addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), toDate, 
                unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);
        }
    }
    return;
}

struct WUJobList2
{
    WUJobList2(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, bool showall, float bbtime, float betime, const char *xls) 
    {
        CDateTime fromTime;
        CDateTime toTime;
        StringBuffer fromstr;
        StringBuffer tostr;

        if(from && *from)
        {
            fromTime.setString(from,NULL,false);
            fromTime.getString(fromstr, false);
        }

        if(to && *to)
        {
            toTime.setString(to,NULL,false);
            toTime.getString(tostr, false);
        }

        StringBuffer responsebuf;
        unsigned jobpending=0;
        if(response)
        {
            responsebuf<<"<script language=\"javascript\">parent.displayLegend(\'"<<fromstr.str()<<"\',\'"<<tostr.str()<<"\', " << showall << ")</script>\r\n";
            response->sendChunk(responsebuf.str());
            responsebuf.clear();
            responsebuf<<"<script language=\"javascript\">parent.displayBegin(\'"<<fromstr.str()<<"\',\'"<<tostr.str()<<"\', " << showall << ")</script>\r\n";
            response->sendChunk(responsebuf.str());
            responsebuf.clear();
            responsebuf<<"<script language=\"javascript\">\r\n";
        }

        StringAttrArray jobs;
        StringBuffer filter("Timing,ThorGraph");
        if(cluster && *cluster)
            filter.appendf(",%s", cluster);
        queryAuditLogs(fromTime, toTime, filter.str(), jobs);

        IArrayOf<IEspECLJob> eclJobList;
        ForEachItemIn(idx, jobs)
        {
            const char* curjob = jobs.item(idx).text;
            if(!curjob || !*curjob)
                continue;

            Owned<IEspECLJob> job = createEclJobFromString(curjob);

            if(response)
            {
                responsebuf<<"parent.displayJob(\'"<<job->getWuid()<<"\',\'"<<job->getGraph()<<"\',"
                             "\'"<<job->getStartedDate()<<"\',\'"<<job->getFinishedDate()<<"\',"
                             "\'"<<job->getCluster()<<"\',\'"<<job->getState()<<"\', ''," << showall << "," << bbtime << "," << betime << ")\r\n";
                if(++jobpending>=50)
                {
                    responsebuf<<"</script>\r\n";
                    response->sendChunk(responsebuf.str());
                    responsebuf.clear();
                    responsebuf<<"<script language=\"javascript\">\r\n";
                    jobpending=0;
                }
            }
            eclJobList.append(*job.getClear());
        }

////////////////////////////////////////////////////////21492
        //Find out which WUs stopped by abnormal thor termination
        StringArray unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime;
        findUnfinishedECLWUs(eclJobList, fromTime, toTime, tostr.str(), cluster, unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);
        if (unfinishedWUIDs.ordinality())
        {
            ForEachItemIn(idx3, unfinishedWUIDs)
            {
                //Add to graph list
                const char* wuid = unfinishedWUIDs.item(idx3);
                const char* graph = unfinishedGraphs.item(idx3);
                //const char* subgraph = unfinishedSubGraphs.item(idx3);
                const char* cluster0 = unfinishedClusters.item(idx3);
                const char* startTime = unfinishedWUStarttime.item(idx3);
                const char* endTime = unfinishedWUEndtime.item(idx3);

                CDateTime tTime;
                StringBuffer started, finished; 
                unsigned year, month, day, hour, minute, second, nano;
                tTime.setString(startTime,NULL,true);
                tTime.getDate(year, month, day, true);
                tTime.getTime(hour, minute, second, nano, true);
                started.appendf("%4d-%02d-%02dT%02d:%02d:%02dZ",year,month,day,hour,minute,second);
                if (endTime && *endTime)
                {
                    tTime.setString(endTime,NULL,true);
                    tTime.getDate(year, month, day, true);
                    tTime.getTime(hour, minute, second, nano, true);
                    finished.appendf("%4d-%02d-%02dT%02d:%02d:%02dZ",year,month,day,hour,minute,second);
                }

                if(response)
                {
                    responsebuf<<"parent.displayJob(\'"<<wuid<<"\',\'"<<graph<<"\',"
                                 "\'"<<started.str()<<"\',\'"<<finished.str()<<"\',"
                                 "\'"<<cluster0<<"\',\'"<<"not finished"<<"\', ''," << showall << "," << bbtime << "," << betime << ")\r\n";
                    if(++jobpending>=50)
                    {
                        responsebuf<<"</script>\r\n";
                        response->sendChunk(responsebuf.str());
                        responsebuf.clear();
                        responsebuf<<"<script language=\"javascript\">\r\n";
                        jobpending=0;
                    }
                }
            }
        }
////////////////////////////////////////////////////////21492

        if(response)
        {
            responsebuf<<"</script>\r\n";
            response->sendChunk(responsebuf.str());
            responsebuf.clear();
            responsebuf<<"<script language=\"javascript\">\r\n";
            responsebuf<<"parent.displaySasha();\r\n";
        }

        if(response)
        {
            responsebuf<<"parent.displayEnd(\'"<< xls <<"\')</script>\r\n";
            response->sendChunk(responsebuf.str());
        }
    }

    // Sample input -
    // 2005-06-13 18:30:46 ,Timing,ThorGraph,400way,W20050613-183018,2,1,1,10022,FAILED
    IEspECLJob* createEclJobFromString(const char* str)
    {
        if(!str || !*str)
            return NULL;

        Owned<IEspECLJob> job = createECLJob("", "");

        // startdate
        const char* bptr = str;
        const char* eptr = strchr(bptr, ',');

        StringBuffer sdate;

        if(!eptr)
            sdate.append(bptr);
        else
            sdate.append(eptr - bptr, bptr);

        sdate.setCharAt(10, 'T');

        if(!eptr)
            return job.getLink();
        
        //Timing
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
            return job.getLink();

        //ThorGraph
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
            return job.getLink();

        StringBuffer bufStr;

        // cluster name
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
        {
            job->setCluster(bptr);
            return job.getLink();
        }
        else
        {
            bufStr.clear().append(eptr - bptr, bptr);
            job->setCluster(bufStr.str());
        }

        //WUID
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
        {
            job->setWuid(bptr);
            return job.getLink();
        }
        else
        {
            bufStr.clear().append(eptr - bptr, bptr);
            job->setWuid(bufStr.str());
        }
        
        //graph number
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
            return job.getLink();
        StringBuffer graph("graph");
        graph.append(eptr - bptr, bptr);
        job->setGraph(graph.str());

        bufStr.clear().append(eptr - bptr, bptr);
        job->setGraphNum(bufStr.str());

        //skip
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
            return job.getLink();

        bufStr.clear().append(eptr - bptr, bptr);
        job->setSubGraphNum(bufStr.str());

        //skip
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(!eptr)
            return job.getLink();

        bufStr.clear().append(eptr - bptr, bptr);
        job->setNumOfRuns(bufStr.str());

        //duration, state
        bptr = eptr + 1;
        eptr = strchr(bptr, ',');
        if(eptr && 0==strncmp(",FAILED", eptr, 7))
        {
            job->setState("failed");
            bufStr.clear().append(eptr - bptr, bptr);
        }
        else
        {
            job->setState("finished");
            bufStr.clear().append(bptr);
        }
        int duration = atoi(bufStr.str()) / 1000;
        CDateTime startdt;
        CDateTime enddt;
        
        enddt.setString(sdate.str(), NULL, true);
        startdt.set(enddt.getSimple() - duration);

        sdate.clear();
        startdt.getString(sdate, false);
        sdate.append('Z');
        job->setStartedDate(sdate.str());

        StringBuffer edate;
        enddt.getString(edate, false);
        edate.append('Z');
        job->setFinishedDate(edate.str());

        return job.getLink();
    }

};

struct CompareData
{
    CompareData(const char* _filter): filter(_filter) {}
    bool operator()(const StlLinked<DataCacheElement>& e) const  
    { 
        return stricmp(e->m_filter.c_str(),filter)==0; 
    }
    const char* filter;
};


DataCacheElement* DataCache::lookup(IEspContext &context, const char* filter, unsigned timeOutMin)
{
    CriticalBlock block(crit);

    if (cache.size() < 1)
        return NULL;

    //erase data if it should be 
    CDateTime timeNow;
    int timeout = timeOutMin;
    timeNow.setNow();
    timeNow.adjustTime(-timeout);
    while (true)
    {
        std::list<StlLinked<DataCacheElement> >::iterator list_iter = cache.begin(); 
        if (list_iter == cache.end())
            break;

        DataCacheElement* awu = list_iter->getLink();
        if (!awu || (awu->m_timeCached > timeNow))
            break;

        cache.pop_front();
    }

    if (cache.size() < 1)
        return NULL;

    //Check whether we have the data cache for this cluster. If yes, get the version
    std::list<StlLinked<DataCacheElement> >::iterator it = std::find_if(cache.begin(),cache.end(),CompareData(filter));
    if(it!=cache.end())
    {
        return it->getLink();
    }

    return NULL;
}
    
void DataCache::add(const char* filter, const char* data, const char* name, const char* localName, const char* wuid, 
        const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total)
{
    CriticalBlock block(crit);

    //Save new data
    Owned<DataCacheElement> e=new DataCacheElement(filter, data, name, localName, wuid, resultName, seq, start, count, requested, total);
    if (cacheSize > 0)
    {
        if (cache.size() >= cacheSize)
            cache.pop_front();

        cache.push_back(e.get());
    }

    return;
}

struct CompareArchivedWUs
{
    CompareArchivedWUs(const char* _filter): filter(_filter) {}
    bool operator()(const StlLinked<ArchivedWUsCacheElement>& e) const  
    { 
        return stricmp(e->m_filter.c_str(),filter)==0; 
    }
    const char* filter;
};


ArchivedWUsCacheElement* ArchivedWUsCache::lookup(IEspContext &context, const char* filter, const char* sashaUpdatedWhen, unsigned timeOutMin)
{
    CriticalBlock block(crit);

    if (cache.size() < 1)
        return NULL;

    //erase data if it should be 
    CDateTime timeNow;
    int timeout = timeOutMin;
    timeNow.setNow();
    timeNow.adjustTime(-timeout);
    while (true)
    {
        std::list<StlLinked<ArchivedWUsCacheElement> >::iterator list_iter = cache.begin(); 
        if (list_iter == cache.end())
            break;

        ArchivedWUsCacheElement* awu = list_iter->getLink();
        if (awu && !stricmp(sashaUpdatedWhen, awu->m_sashaUpdatedWhen.c_str()) && (awu->m_timeCached > timeNow))
            break;

        cache.pop_front();
    }
    
    if (cache.size() < 1)
        return NULL;

    //Check whether we have the data cache for this cluster. If yes, get the version
    std::list<StlLinked<ArchivedWUsCacheElement> >::iterator it = std::find_if(cache.begin(),cache.end(),CompareArchivedWUs(filter));
    if(it!=cache.end())
    {
        return it->getLink();
    }

    return NULL;
}
    
void ArchivedWUsCache::add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, IArrayOf<IEspECLWorkunit>& wus)
{
    CriticalBlock block(crit);

    //Save new data
    Owned<ArchivedWUsCacheElement> e=new ArchivedWUsCacheElement(filter, sashaUpdatedWhen, hasNextPage, /*data.str(),*/ wus);
    if (cacheSize > 0)
    {
        if (cache.size() >= cacheSize)
            cache.pop_front();

        cache.push_back(e.get());
    }

    return;
}


int CWsWorkunitsSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    try
    {
        Owned<IProperties> params = request->getParameters();

        StringBuffer path;
        request->getPath(path);
/*
         if(!path.length() || strcmp(path.str(),"/")==0)
         {
              response->redirect(*request,"/WsWorkunits/WUQuery?form");
              return 0;
         }
*/
         StringBuffer userid,password,realm;
         request->getBasicAuthorization(userid,password,realm);
         if(MatchPrefix(path.str(), "/WsWorkunits/JobList"))
         {
            StringBuffer cluster, startDate, endDate, showAllstr, bbtimestr, betimestr;
            request->getParameter("Cluster",cluster);
            request->getParameter("StartDate",startDate);
            request->getParameter("EndDate",endDate);
            request->getParameter("ShowAll", showAllstr);
            request->getParameter("BusinessStartTime", bbtimestr);
            request->getParameter("BusinessEndTime", betimestr);
            bool showall = false;
            float bbtime = 0;
            float betime = 24;
            if(showAllstr.length() > 0)
                showall = (atoi(showAllstr.str()) == 1);        

            if(showall)
            {
                if(bbtimestr.length() > 0)
                {
                    const char* bbptr = bbtimestr.str();
                    int hours = 0;
                    int mins = 0; 
                    while(isdigit(*bbptr))
                    {
                        hours = 10*hours + *bbptr - '0';
                        bbptr++;
                    }
                    if(*bbptr == ':')
                        bbptr++;
                    while(isdigit(*bbptr))
                    {
                        mins = 10*mins + (*bbptr - '0');
                        bbptr++;
                    }

                    bbtime = hours + mins/60.0;
                }

                if(betimestr.length() > 0)
                {
                    const char* beptr = betimestr.str();
                    int hours = 0;
                    int mins = 0; 
                    while(isdigit(*beptr))
                    {
                        hours = 10*hours + *beptr - '0';
                        beptr++;
                    }
                    if(*beptr == ':')
                        beptr++;
                    while(isdigit(*beptr))
                    {
                        mins = 10*mins + (*beptr - '0');
                        beptr++;
                    }

                    betime = hours + mins/60.0;
                }
                
                if(bbtime <= 0 || betime > 24 || bbtime >= betime)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
            }

            response->addHeader("Expires", "0");
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);

            StringBuffer xls = "ShowAll=1";
            if(cluster.length())
                xls.appendf("&Cluster=%s", cluster.str());
            if(startDate.length())
                xls.appendf("&StartDate=%s", startDate.str());
            if(endDate.length())
                xls.appendf("&EndDate=%s", endDate.str());
            if(bbtimestr.length())
                xls.appendf("&BusinessStartTime=%s", bbtimestr.str());
            if(betimestr.length())
                xls.appendf("&BusinessEndTime=%s", betimestr.str());

            //WUJobList jobs(*request->queryContext(), cluster.str(), startDate.str(), endDate.str(), response);
            WUJobList2 jobs(*request->queryContext(), cluster.str(), startDate.str(), endDate.str(), response, showall, bbtime, betime, xls.str());

            return 0;
        }
        else if(MatchPrefix(path.str(), "/WsWorkunits/JobQueue"))
        {
            StringBuffer cluster, startDate, endDate;
            request->getParameter("Cluster",cluster);
            request->getParameter("StartDate",startDate);
            request->getParameter("EndDate",endDate);

            response->addHeader("Expires", "0");
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);

            StringBuffer xls;
            xls.append("ShowType=InGraph");

            if(cluster.length())
                xls.appendf("&Cluster=%s", cluster.str());
            if(startDate.length())
                xls.appendf("&StartDate=%s", startDate.str());
            if(endDate.length())
                xls.appendf("&EndDate=%s", endDate.str());

            WUJobQueue queues(*request->queryContext(), cluster.str(), startDate.str(), endDate.str(), response, xls.str());
            return 0;
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(*request->queryContext(), e,  ECLWATCH_INTERNAL_ERROR);
    }


    return CWsWorkunitsSoapBinding::onGet(request,response);
}


void CWsWorkunitsEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }
    setPasswordsFromSDS(); 

    DBGLOG("Initializing %s service [process = %s]", service, process);
    StringBuffer xpath;
    xpath.clear();

    xpath.clear();
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/StyleSheets/xslt[@name=\"%s\"]", process, service,"graphupdate_gvc");
    cfg->getProp(xpath.str(), m_GraphUpdateGvcXSLT);

    StringBuffer enableNewRoxieOnDemandQuery;
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/AllowNewRoxieOnDemandQuery", process, service);
    cfg->getProp(xpath.str(), enableNewRoxieOnDemandQuery);

    if (streq(enableNewRoxieOnDemandQuery.str(), "true"))
        m_allowNewRoxieOnDemandQuery = true;
    else
        m_allowNewRoxieOnDemandQuery = false;

    m_AWUS_cache_minutes = AWUS_CACHE_MIN_DEFAULT;
    xpath.clear();
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/AWUsCacheMinutes", process, service);
    cfg->getPropInt(xpath.str(), m_AWUS_cache_minutes);
    
    m_dataCache.setown(new DataCache(DATA_SIZE));
    m_archivedWUsCache.setown(new ArchivedWUsCache(AWUS_CACHE_SIZE));

    //Create a folder for temporarily holding gzip files by WUResultBin()
    Owned<IFile> tmpdir = createIFile(TEMPZIPDIR);
    if(!tmpdir->exists())
    {
        tmpdir->createDirectory();
    }

    m_sched.setAllowNewRoxieOnDemandQuery(m_allowNewRoxieOnDemandQuery);
    m_sched.start();
}

void CWsWorkunitsEx::xsltTransform(const char* xml,const char* sheet,IProperties *params,StringBuffer& ret)
{
    StringBuffer xsl;
    if(readFile(sheet,xsl)<=0)
        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Cannot load stylesheet %s.",sheet);
    Owned<IXslProcessor> proc = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();
    trans->setXmlSource(xml, strlen(xml));
    trans->setXslSource(xsl, xsl.length());
    if (params)
    {
        Owned<IPropertyIterator> it = params->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char *key = it->getPropKey();
         //set parameter in the XSL transform skipping over the @ prefix, if any
         const char* paramName = *key == '@' ? key+1 : key;
            trans->setParameter(paramName, StringBuffer().append('\'').append(params->queryProp(key)).append('\'').str());
        }
    }
    trans->transform(ret);
}

enum { ActionDelete=0,ActionProtect,ActionAbort, ActionRestore, ActionEventSchedule, ActionEventDeschedule, 
    ActionChangeState, ActionPause, ActionPauseNow, ActionResume};
bool CWsWorkunitsEx::doAction(IEspContext& context, StringArray& wuids, int action, IProperties* params, 
                              IArrayOf<IConstWUActionResult>* results)
{
   Owned<IMultiException> me = MakeMultiException();
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
   if (!me)
        throw MakeStringException(ECLWATCH_CANNOT_ACCESS_EXCEPTION_ENGINE, "Cannot access Exception Engine.");
   if (!factory)
        throw MakeStringException(ECLWATCH_CANNOT_ACCESS_WU_ENGINE, "Cannot access WorkUnit Engine.");

    if (wuids.length() < 1)
    {
        return true;
    }

    bool bAllSuccess = true;
    for(int i=0; i<wuids.length();i++)
    {
        const char* wuid=wuids.item(i);
        if (!wuid || !*wuid)
            continue;

        try
        {
            if (action == ActionRestore)
            {
                StringBuffer sashaAddress;
                IArrayOf<IConstTpSashaServer> sashaservers;
                CTpWrapper dummy;
                dummy.getTpSashaServers(sashaservers);  
                ForEachItemIn(i, sashaservers)
                {
                    IConstTpSashaServer& sashaserver = sashaservers.item(i);
                    IArrayOf<IConstTpMachine> &sashaservermachine = sashaserver.getTpMachines();
                    sashaAddress.append(sashaservermachine.item(0).getNetaddress());
                }
                if (sashaAddress.length() < 1)
                {
                    throw MakeStringException(ECLWATCH_ARCHIVE_SERVER_NOT_FOUND,"Archive Server not found.");
                }
                
                StringBuffer strAction;
                Owned<ISashaCommand> cmd = createSashaCommand();
                if (!cmd)
                {
                    throw MakeStringException(ECLWATCH_CANNOT_CREATE_ARCHIVE_CMD,"Failed to create a command.");
                }

                cmd->addId(wuid);
                cmd->setAction(SCA_RESTORE);
                strAction.append("Restore");
                
                SocketEndpoint ep(sashaAddress.str(), DEFAULT_SASHA_PORT);
                Owned<INode> node = createINode(ep);
                if (!node)
                {
                    throw MakeStringException(ECLWATCH_INODE_NOT_FOUND,"INode not found.");
                }

                if (!cmd->send(node,1*60*1000)) 
                {
                    throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to Archive server at %s.",sashaAddress.str());
                }
                if (cmd->numIds()==0) 
                {
                    WARNLOG("Could not Archive/restore %s",wuid);
                    me->append(*MakeStringException(0,"Cannot archive/restore workunit %s.",wuid));
                }
                StringBuffer reply;
                cmd->getId(0,reply);
                AccessSuccess(context,"Updated %s",wuid);

                Owned<IEspWUActionResult> res = createWUActionResult("", "");
                if (!res)
                {
                    throw MakeStringException(ECLWATCH_CANNOT_CREATE_WUACTIONRESULT,"Failed in creating WUActionResult");
                }

                res->setWuid(wuid);
                res->setResult("Success");
                
                CWUWrapper wu(wuid, context);
                ensureWorkunitAccess(context, *wu, SecAccess_Write);
                
                if (results)
                {
                    res->setAction(strAction.str());
                    results->append(*LINK(res.getClear()));
                }
            }
            else
            {
                StringBuffer strAction = "";
                Owned<IEspWUActionResult> res = createWUActionResult("", "");
                if (!res)
                {
                    throw MakeStringException(ECLWATCH_CANNOT_CREATE_WUACTIONRESULT, "Failed in creating WUActionResult");
                }

                res->setWuid(wuid);
                res->setResult("Success");
                
                CWUWrapper wu(wuid, context);
                ensureWorkunitAccess(context, *wu, SecAccess_Write);

                if ((action == ActionDelete) && (wu->getState() == WUStateWait))
                {
                    throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Cannot delete a workunit which is in a 'Wait' status.");
                }

                try
                {
                    switch(action)
                    {
                    case ActionPause:
                        strAction = "Pause";
                        pauseWU(context, wuid, false);
                       break;
                    case ActionPauseNow:
                        strAction = "PauseNow";
                        pauseWU(context, wuid, true);
                       break;
                    case ActionResume:
                        strAction = "Resume";
                        resumeWU(context, wuid);
                       break;
                    case ActionDelete:
                        strAction = "Delete";
                        ensureWorkunitAccess(context, *wu, SecAccess_Full);
                        {
                            int state = wu->getState();
                            if (state != WUStateAborted &&  state != WUStateCompleted && state != WUStateFailed && 
                                state != WUStateArchived && state != WUStateCompiled && state != WUStateUploadingFiles)
                            {
                                Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                                if(lw && lw.get())
                                {
                                    lw->setState(WUStateFailed);
                                    lw.clear();
                                }
                            }
                            factory->deleteWorkUnitEx(wuid);
                            AccessSuccess(context,"Deleted %s",wuid);
                        }
                       break;
                    case ActionAbort:
                        strAction = "Abort";
                        ensureWorkunitAccess(context, *wu, SecAccess_Full);
                        {
                            bool aborted = false;
                            if (wu->getState() == WUStateWait)
                            {
                                Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                                if(lw && lw.get())
                                {
                                    lw->deschedule();
                                    lw->setState(WUStateAborted);
                                    aborted = true;
                                }
                                lw.clear();
                            }

                            if (!aborted)
                            {
                                secAbortWorkUnit(wuid, *context.querySecManager(), *context.queryUser());
                            }
                            AccessSuccess(context,"Aborted %s",wuid);
                        }
                        break;
                    case ActionProtect:
                        strAction = "Protect";
                        {
                            //Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                            Owned<IConstWorkUnit> lw = factory->openWorkUnit(wuid, false);
                            if (lw && lw.get())
                            {
                                lw->protect(!params || params->getPropBool("Protect",true));
                                lw.clear();
                                AccessSuccess(context,"Updated %s",wuid);
                            }
                        }
                        break;
                    /*case ActionUnprotect:
                        strAction = "Unprotect";
                        {
                            //Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                            Owned<IConstWorkUnit> lw = factory->openWorkUnit(wuid, false);
                            bool bProtect = true;
                            if (!params || params->getPropBool("Unprotect",true))
                                bProtect = false;
                            lw->protect(bProtect);
                            lw.clear();
                            AccessSuccess(context,"Updated %s",wuid);
                        }
                        break;*/
                    case ActionChangeState:
                        strAction = "ChangeState";
                        {
                            if (params)
                            {
                                WUState state = (WUState) params->getPropInt("State");
                                if (state > WUStateUnknown && state < WUStateSize)
                                {
                                    Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                                    if(lw && lw.get())
                                    {
                                        lw->setState(state);
                                        lw.clear();
                                        AccessSuccess(context,"Updated %s",wuid);
                                    }
                                }
                            }
                        }
                        break;
                    case ActionEventSchedule:
                        strAction = "EventSchedule";
                        {
                            Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                            if(lw && lw.get())
                            {
                                lw->schedule();
                                lw.clear();
                                AccessSuccess(context,"Updated %s",wuid);
                            }
                        }
                        break;
                    case ActionEventDeschedule:
                        strAction = "EventDeschedule";
                        {
                            Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
                            if(lw && lw.get())
                            {
                                lw->deschedule();
                                lw.clear();
                                AccessSuccess(context,"Updated %s",wuid);
                            }
                        }
                        break;
                    }
                }
                catch (IException *e)
                {   
                    StringBuffer eMsg;
                    eMsg = e->errorMessage(eMsg);
                    e->Release();

                    bAllSuccess = false;
                    StringBuffer failedMsg = "Failed: ";
                    failedMsg.append(eMsg);
                    res->setResult(failedMsg.str());
                
                    WARNLOG("Failed to %s workunit: %s, %s", strAction.str(), wuid, eMsg.str());
                    AccessFailure(context,"Failed to %s %s", strAction.str(), wuid);
                }

                if (results)
                {
                    res->setAction(strAction.str());
                    results->append(*LINK(res.getClear()));
                }
            }
        }
        catch (IException *E)
        {
            me->append(*E);
        }
        catch (...)
        {   
            me->append(*MakeStringException(0,"Unknown exception wuid=%s",wuid));
        }
    }

   if(me->ordinality())
       throw me.getLink();

    int timeToWait = 0;
    if (params)
         timeToWait = params->getPropInt("BlockTillFinishTimer");
    if (timeToWait != 0)
    {
        for(int i=0; i<wuids.length();i++)
        {
            const char* wuid=wuids.item(i);
            if (!wuid || !*wuid)
                continue;

            waitForWorkUnitToComplete(wuid, timeToWait);
        }
    }

    return bAllSuccess;
}

bool CWsWorkunitsEx::onWUProcessGraph(IEspContext &context,IEspWUProcessGraphRequest &req, IEspWUProcessGraphResponse &resp)
{
    try
    {
        StringBuffer wuid, graphname;
        wuid.set(req.getWuid());
        graphname.set(req.getName());

        ensureWorkunitAccess(context, wuid.str(), SecAccess_Read);

        StringBuffer x;
            
        CWUWrapper wu(wuid.str(), context);
        Owned <IConstWUGraph> graph = wu->getGraph(graphname.str());
        Owned <IPropertyTree> xgmml = graph->getXGMMLTree(true); // merge in graph progress information
        //adjustRowvalues(xgmml, popupId);

        toXML(xgmml.get(), x);
        resp.setTheGraph(x.str());

    }

    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsWorkunitsEx::getPopups(IEspContext &context, const char *wuid, const char *graphname, const char* popupId, StringBuffer &script)
{
    CWUWrapper wu(wuid, context);
    Owned <IConstWUGraph> graph = wu->getGraph(graphname);
    Owned <IPropertyTree> xgmml = graph->getXGMMLTree(true); // merge in graph progress information
    adjustRowvalues(xgmml, popupId);

    toXML(xgmml.get(), script);
}

bool CWsWorkunitsEx::onWUGetGraph(IEspContext& context, IEspWUGetGraphRequest& req, IEspWUGetGraphResponse& resp)
{
    try
    {
        const char* wuid = req.getWuid();
        CWUWrapper wu(wuid, context);
        ensureWorkunitAccess(context, *wu, SecAccess_Read);

         StringBuffer graphToFetch = req.getGraphName();
        StringBuffer subgraphToFetch = req.getSubGraphId();         

        SCMStringBuffer runningGraph;
        WUGraphIDType id;
        bool running=wu.isRunning() && wu->getRunningGraph(runningGraph,id);

        IArrayOf<IEspECLGraphEx> graphs;
        Owned<IConstWUGraphIterator> it = &wu->getGraphs(GraphTypeAny);
        ForEach(*it)
        {
            IConstWUGraph &graph = it->query();
            if(!graph.isValid())
                continue;

            SCMStringBuffer name, label, type;
            graph.getName(name);
            graph.getLabel(label);
            graph.getTypeName(type);
            const char* graphName = name.str();

            if(graphToFetch.length() < 1 || stricmp(graphName, req.getGraphName())==0)
            {
                Owned<IEspECLGraphEx> g = createECLGraphEx("","");
                g->setName(graphName); 
                g->setLabel(label.str()); 
                g->setType(type.str());
                if(running && strcmp(graphName, runningGraph.str())==0)
                {
                    g->setRunning(true);
                    g->setRunningId(id);
                }

                Owned<IPropertyTree> xgmml = graph.getXGMMLTree(true);

                // New functionality, if a subgraph id is specified and we only want to load the xgmml for that subgraph
                // then we need to conditionally pull a propertytree from the xgmml graph one and use that for the xgmml.

                StringBuffer s;
                if (subgraphToFetch.length() > 0)
                {
                    IPropertyTree *subgraphXgmml;
                    
                    StringBuffer subgraphXPath;
                    subgraphXPath.appendf("//node[@id='%s']", subgraphToFetch.str());

                    StringBuffer subgraphText;
                    subgraphXgmml = xgmml->getPropTree(subgraphXPath.str());
                    toXML(subgraphXgmml, subgraphText);

                    s.append(subgraphText.str());
                    
                }
                else
                {
                    toXML(xgmml, s);
                }
                g->setGraph(s.str());

                double version = context.getClientVersion();
                if (version > 1.20)
                {       
                    Owned<IConstWUGraphProgress> progress = wu->getGraphProgress(name.str());
                    if (progress)
                    {
                        WUGraphState graphstate= progress->queryGraphState();
                        if (graphstate == WUGraphComplete)
                        {
                            g->setComplete(true);
                        }
                        else if (graphstate == WUGraphFailed)
                        {       
                            g->setFailed(true);
                        }
                    }
                }

                graphs.append(*g.getLink());
            }
        }
        resp.setGraphs(graphs);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

int CWsWorkunitsEx::loadFile(const char* fname, int& len, unsigned char* &buf, bool binary)
{
    len = 0;
    buf = NULL;

    FILE* fp = fopen(fname, binary?"rb":"rt");
    if (fp)
    {
        char* buffer[1024];
        int bytes;
        for (;;)
        {
            bytes = fread(buffer, 1, sizeof(buffer), fp);
            if (!bytes)
                break;
            buf = (unsigned char*)realloc(buf, len + bytes + 1);
            memcpy(buf + len, buffer, bytes);
            len += bytes;
        }
        fclose(fp);
    }
    else
    {
        printf("unable to open file %s\n", fname);
        return -1;
    }

    if(buf)
        buf[len] = '\0';

    return 0;
}

bool CWsWorkunitsEx::onWUResultBin(IEspContext &context,IEspWUResultBinRequest &req, IEspWUResultBinResponse &resp)
{
    try
    {
        ensureWorkunitAccess(context, req.getWuid(), SecAccess_Read);

        MemoryBuffer x;
        __int64 total=0;
        __int64 start = req.getStart() > 0 ? req.getStart() : 0;
        unsigned count = req.getCount(), requested=count;
        SCMStringBuffer name;
        bool bGetWorkunitResults = false;
        if (req.getWuid() && *req.getWuid() && req.getResultName()  && *req.getResultName())
        {
            name.set(req.getResultName());
            getWorkunitResults(context, req.getWuid(),0,start,count,total,name,stricmp(req.getFormat(),"raw")==0,x);
            bGetWorkunitResults = true;
        }
        else if (req.getWuid() && *req.getWuid() && (req.getSequence() > -1))
        {
            getWorkunitResults(context, req.getWuid(),req.getSequence(),start,count,total,name,stricmp(req.getFormat(),"raw")==0,x);
            bGetWorkunitResults = true;
        }
        else if (req.getLogicalName() && *req.getLogicalName())
        {
            const char* logicalName = req.getLogicalName();
            Owned<IUserDescriptor> userdesc;
             StringBuffer username;
            context.getUserID(username);
            const char* passwd = context.queryPassword();
            userdesc.setown(createUserDescriptor());
            userdesc->set(username.str(), passwd);
            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
            if (!df)
            {
                throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find file %s.",logicalName);
                return false;
            }

            const char* wuid = df->queryProperties().queryProp("@workunit");
            if (wuid && *wuid)
            {
                 CWUWrapper wu(wuid, context);
                Owned<IConstWUResultIterator> it = &wu->getResults();
                ForEach(*it)
                {
                    IConstWUResult &r = it->query();
                    SCMStringBuffer filenameStr;
                    r.getResultLogicalName(filenameStr);
                    const char *filename = filenameStr.str();
                    if(filename && (stricmp(filename, logicalName)==0)) 
                    {
                        getWorkunitResults(context, wuid, r.getResultSequence(),start,count,total,name,stricmp(req.getFormat(),"raw")==0,x);
                        bGetWorkunitResults = true;
                        break;
                    }
                }
            }
            else
            {
                throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.",logicalName);
                return false;
            }
        }

        if (!bGetWorkunitResults)
        {
            throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");
            return false;
        }

        if(stricmp(req.getFormat(),"xls")==0)
        {
            Owned<IProperties> params(createProperties());
            params->setProp("showCount",0);


            StringBuffer temp;
            temp<<"<WUResultExcel><Result>";
            temp.append(x.length(),x.toByteArray());
            temp<<"</Result></WUResultExcel>";

            if (temp.length() > MAXXLSTRANSFER)
                throw MakeStringException(ECLWATCH_TOO_BIG_DATA_SET,"The data set is too big to be converted to an Excel file. Please use the gzip link to download a compressed XML data file.");

            StringBuffer xls;
            xsltTransform(temp.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

            MemoryBuffer buff;
            buff.setBuffer(xls.length(), (void*)xls.str());
            resp.setResult(buff);
            resp.setResult_mimetype("application/vnd.ms-excel");
    //        Content-disposition: attachment; filename=fname.ext
        }
#ifdef _USE_ZLIB
        else if((stricmp(req.getFormat(),"zip")==0) || (stricmp(req.getFormat(),"gzip")==0))
        {
            bool gzip = false;
            if (stricmp(req.getFormat(),"zip")!=0) 
                gzip = true;
            
            StringBuffer temp;
            temp << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
            temp<<"<Result>";
            temp.append(x.length(),x.toByteArray());
            temp<<"</Result>";

            //DBGLOG("XML size:(%d)", temp.length());

            StringBuffer ifname;
            unsigned threadID = (unsigned)(memsize_t)GetCurrentThreadId();
            if(gzip)
                ifname.appendf("%s%sT%xAT%x", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());
            else
                ifname.appendf("%s%sT%xAT%x.zip", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());

            IZZIPor* Zipor = createZZIPor();
            int ret = 0;
            if (gzip)
                ret = Zipor->gzipToFile(temp.length(), (void*)temp.str(), ifname.str());
            else
                ret = Zipor->zipToFile(temp.length(), (void*)temp.str(), "WUResult.xml", ifname.str());

            releaseIZ(Zipor);

            if (ret < 0)
            {
                Owned<IFile> rFile = createIFile(ifname.str());
                if (rFile->exists())
                    rFile->remove();

                throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
            }

            int outlen = 0;
            unsigned char* outd = NULL;
            ret = loadFile(ifname.str(), outlen, outd); 
            if(ret < 0 || outlen < 1 || !outd || !*outd)
            {
                Owned<IFile> rFile = createIFile(ifname.str());
                if (rFile->exists())
                    rFile->remove();

                if (outd)
                    free(outd);

                throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
            }


            MemoryBuffer buff;
            buff.setBuffer(outlen, (void*)outd);
            resp.setResult(buff);
            if (gzip)
            {
                resp.setResult_mimetype("application/x-gzip");
                context.addCustomerHeader("Content-disposition", "attachment;filename=WUResult.xml.gz");
            }
            else
            {
                resp.setResult_mimetype("application/zip");
                context.addCustomerHeader("Content-disposition", "attachment;filename=WUResult.xml.zip");
            }

            Owned<IFile> rFile = createIFile(ifname.str());
            if (rFile->exists())
                rFile->remove();
            if (outd)
                free(outd);
        }
#endif
        else if(stricmp(req.getFormat(),"raw")==0)
        {
            resp.setResult(x);
        }
        else
        {
            resp.setResult(x);
        }
        
        resp.setName(name.str());
        resp.setWuid(req.getWuid());
        resp.setSequence(req.getSequence());
        resp.setStart(start);
        if (requested > total)
            requested = total;
        resp.setRequested(requested);
        resp.setCount(count);
        resp.setTotal(total);
        resp.setFormat(req.getFormat());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUResult(IEspContext &context, IEspWUResultRequest &req, IEspWUResultResponse &resp)
{
    try
    {
        ensureWorkunitAccess(context, req.getWuid(), SecAccess_Read);

        MemoryBuffer x;
        SCMStringBuffer name;
        __int64 total=0;
        __int64 start = req.getStart() > 0 ? req.getStart() : 0;
        unsigned count=req.getCount() ? req.getCount() : 100, requested=count;
        const char* clusterName = req.getCluster();
        const char* logicalName = req.getLogicalName();
        const char* wuid = req.getWuid();
        const char* resultName = req.getResultName();
        unsigned seq = req.getSequence();

        StringBuffer filter;
        filter.appendf("start=%"I64F"d;", start);
        filter.appendf("count=%d;", count);
        if (clusterName && *clusterName)
            filter.appendf("clusterName=%s;", clusterName);
        if (logicalName && *logicalName)
            filter.appendf("logicalName=%s;", logicalName);
        if (wuid && *wuid)
            filter.appendf("wuid=%s;", wuid);
        if (resultName && *resultName)
            filter.appendf("resultName=%s;", resultName);
        filter.appendf("seq=%d;", seq);

        Owned<DataCacheElement> data = m_dataCache->lookup(context, filter, m_AWUS_cache_minutes);
        if (data)
        {
            x.append(data->m_data.c_str());
            name.set(data->m_name.c_str()); 
            logicalName = data->m_logicalName.c_str();
            wuid = data->m_wuid.c_str();
            resultName = data->m_resultName.c_str();
            seq = data->m_seq;
            start = data->m_start;
            count = data->m_rowcount;
            requested = data->m_requested;
            total = data->m_total;

            if (logicalName && *logicalName)
                resp.setLogicalName(logicalName);
            else
            {
                if (wuid && *wuid)
                    resp.setWuid(wuid);
                resp.setSequence(seq);
            }
        }
        else
        {
            if(logicalName && *logicalName)
            {
                Owned<IUserDescriptor> userdesc;
                StringBuffer username;
                context.getUserID(username);
                const char* passwd = context.queryPassword();
                userdesc.setown(createUserDescriptor());
                userdesc->set(username.str(), passwd);

                Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
                if(!df)
                    throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find file %s.", logicalName);

                StringBuffer cluster;
                const char* wuid = df->queryProperties().queryProp("@workunit");
                if (wuid && *wuid)
                {
                    getWorkunitCluster(context, wuid, cluster, true);
                }

                if (cluster.length() > 0)
                {
                    getFileResults(context, logicalName,cluster.str(),start,count,total,name,false,x);
                    resp.setLogicalName(logicalName);
                }
                else if (clusterName && *clusterName)
                {
                    getFileResults(context, logicalName,clusterName,start,count,total,name,false,x);
                    resp.setLogicalName(logicalName);
                }
                else
                {
                    throw MakeStringException(ECLWATCH_INVALID_INPUT,"Need valid target cluster to browse file %s.",logicalName);
                }
            }
            else    if (wuid && *wuid && resultName  && *resultName)
            {
                name.set(resultName);
                getWorkunitResults(context, wuid,0,start,count,total,name,false,x);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            else
            {
                getWorkunitResults(context, wuid, seq,start,count,total,name,false,x);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            x.append(0);

            if (requested > total)
                requested = total;

            m_dataCache->add(filter, x.toByteArray(), name.str(), logicalName, wuid, resultName,seq, start, count, requested, total);
        }

        resp.setName(name.str());
        resp.setStart(start);
        if (clusterName && *clusterName)
            resp.setCluster(clusterName);
        resp.setRequested(requested);
        resp.setCount(count);
        resp.setTotal(total);
        resp.setResult(x.toByteArray());

        context.queryXslParameters()->setProp("escapeResults","1");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUResultView(IEspContext &context, IEspWUResultViewRequest &req, IEspWUResultViewResponse &resp)
{
    ensureWorkunitAccess(context, req.getWuid(), SecAccess_Read);
    Owned<IWuWebView> wv = createWuWebView(req.getWuid(), NULL, getCFD(), true);
    StringBuffer html;
    wv->renderSingleResult(req.getViewName(), req.getResultName(), html);
    resp.setResult(html.str());
    resp.setResult_mimetype("text/html");
    return true;
}


bool CWsWorkunitsEx::getResultSchemas(IConstWUResult &r, IArrayOf<IEspECLSchemaItem>& schemas)
{
    SCMStringBuffer schema;
    r.getResultEclSchema(schema);
    if (schema.length() < 1)
        return false;
    
    MultiErrorReceiver errs;
    Owned<IHqlExpression> expr = ::parseQuery(schema.str(), &errs);

    if (errs.errCount() != 0)
        return false;

    gatherFields(schemas, expr, false);
    return true;
}

void CWsWorkunitsEx::gatherFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional)
{
    if(!expr)
        return;

    int ret = expr->getOperator();
    switch (ret)
    {
    case no_record:
        gatherChildFields(schemas, expr, isConditional);
        break;
    case no_ifblock:
        {
            gatherChildFields(schemas, expr->queryChild(1), true);
            break;
        }
    case no_field:
        {
            if (expr->hasProperty(__ifblockAtom))
                break;
            ITypeInfo * type = expr->queryType();
            IAtom * name = expr->queryName();
            IHqlExpression * nameAttr = expr->queryProperty(namedAtom);
            StringBuffer outname;
            if (nameAttr && nameAttr->queryChild(0) && nameAttr->queryChild(0)->queryValue())
                nameAttr->queryChild(0)->queryValue()->getStringValue(outname);
            else
                outname.append(name).toLowerCase();
            if(type)
            {
                type_t tc = type->getTypeCode();
                if (tc == type_row)
                {
                    gatherChildFields(schemas, expr->queryRecord(), isConditional);
                }
                else
                {
                    if (type->getTypeCode() == type_alien)
                    {
                        IHqlAlienTypeInfo * alien = queryAlienType(type);
                        type = alien->queryPhysicalType();
                    }
                    Owned<IEspECLSchemaItem> schema = createECLSchemaItem("","");
                    
                    StringBuffer eclType;
                    type->getECLType(eclType);

                    schema->setColumnName(outname);
                    schema->setColumnType(eclType.str());
                    schema->setColumnTypeCode(tc);
                    schema->setIsConditional(isConditional);

                    schemas.append(*schema.getClear());
                }
            }
            break;
        }
    }
}

void CWsWorkunitsEx::gatherChildFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional)
{
    if(!expr)
        return;

    ForEachChild(idx, expr)
        gatherFields(schemas, expr->queryChild(idx), isConditional);
}

bool CWsWorkunitsEx::onWUResultSummary(IEspContext &context, IEspWUResultSummaryRequest &req, IEspWUResultSummaryResponse &resp)
{
    try
    {
        ensureWorkunitAccess(context, req.getWuid(), SecAccess_Read);

        CWUWrapper wu(req.getWuid(), context);

        IArrayOf<IEspECLResult> results;
        IArrayOf<IEspECLSchemaItem> schemas;
        Owned<IConstWUResultIterator> it = &wu->getResults();
        ForEach(*it)
        {
            IConstWUResult &r = it->query();
            if(r.getResultSequence()==req.getSequence()) 
            {
                WUResultFormat format = r.getResultFormat();
                    getResult(context, r, results);
                resp.setFormat(format);
                resp.setResult(results.item(0));
                break;
              }
         }

         results.kill();

         resp.setWuid(req.getWuid());
         resp.setSequence(req.getSequence());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CWsWorkunitsEx::openSaveFile(IEspContext &context, int opt, const char* filename, const char* origMimeType, MemoryBuffer& buf, IEspWULogFileResponse &resp)
{
    if (opt < 1)
    {
        resp.setThefile(buf);
        resp.setThefile_mimetype(origMimeType);
    }
    else if (opt < 2)
    {
        StringBuffer headerStr("attachment;");
        if (filename && *filename)
            headerStr.appendf("filename=%s", filename);

        MemoryBuffer buf0;
        unsigned i = 0;
        char* p = (char*) buf.toByteArray();
        while (i < buf.length())
        {
            if (p[0] != 10)
            {
                buf0.append(p[0]);
            }
            else
            {
                buf0.append(0x0d);
            }

            p++;
            i++;
        }
        resp.setThefile(buf);
        resp.setThefile_mimetype(origMimeType);
        context.addCustomerHeader("Content-disposition", headerStr.str());
    }
    else
    {
#ifndef _USE_ZLIB
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
#else
        StringBuffer fileNameStr, headerStr("attachment;");
        if (filename && *filename)
            fileNameStr.append(filename);
        else
            fileNameStr.append("file");

        if (opt > 2)
        {
            if (filename && *filename)
                headerStr.appendf("filename=%s.gz", filename);
        }
        else
        {
            if (filename && *filename)
                headerStr.appendf("filename=%s.zip", filename);
        }

        StringBuffer ifname;
        unsigned threadID = (unsigned)(memsize_t)GetCurrentThreadId();
        if (opt > 2)
            ifname.appendf("%s%sT%xAT%x", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());
        else
            ifname.appendf("%s%sT%xAT%x.zip", TEMPZIPDIR, PATHSEPSTR, threadID, msTick());

        IZZIPor* Zipor = createZZIPor();
        int ret = 0;
        if (opt > 2)
            ret = Zipor->gzipToFile(buf.length(), (void*)buf.toByteArray(), ifname.str());
        else
            ret = Zipor->zipToFile(buf.length(), (void*)buf.toByteArray(), fileNameStr.str(), ifname.str());
        releaseIZ(Zipor);

        if (ret < 0)
        {
            Owned<IFile> rFile = createIFile(ifname.str());
            if (rFile->exists())
                rFile->remove();

            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
        }

        int outlen = 0;
        unsigned char* outd = NULL;
        ret = loadFile(ifname.str(), outlen, outd); 
        if(ret < 0 || outlen < 1 || !outd || !*outd)
        {
            Owned<IFile> rFile = createIFile(ifname.str());
            if (rFile->exists())
                rFile->remove();

            if (outd)
                free(outd);

            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
        }

        MemoryBuffer membuff;
        membuff.setBuffer(outlen, (void*)outd);
        resp.setThefile(membuff);

        if (opt > 2)
        {
            resp.setThefile_mimetype("application/x-gzip");
        }
        else
        {
            resp.setThefile_mimetype("application/zip");
        }
        context.addCustomerHeader("Content-disposition", headerStr.str());

        Owned<IFile> rFile1 = createIFile(ifname.str());
        if (rFile1->exists())
            rFile1->remove();
        if (outd)
            free(outd);
#endif
    }
}

bool CWsWorkunitsEx::onWUFile(IEspContext &context,IEspWULogFileRequest &req, IEspWULogFileResponse &resp)
{
    try
    {
        DBGLOG("CWsWorkunitsEx::onWUFile WUID=%s",req.getWuid());

        ensureWorkunitAccess(context, req.getWuid(), SecAccess_Read);

        int opt = req.getOption();
        if (*req.getWuid())
        {
            MemoryBuffer buf;
            if (strcmp(File_ArchiveQuery,req.getType()) == 0)
            {
                getWorkunitArchiveQuery(context, req.getWuid(),buf);
                openSaveFile(context, opt, "ArchiveQuery.xml", HTTP_TYPE_TEXT_XML, buf, resp);
            }
            else if ((strcmp(File_Cpp,req.getType()) == 0) && req.getName() && *req.getName())
            {
                getWorkunitCpp(context, req.getName(), req.getDescription(), req.getIPAddress(),buf);
                openSaveFile(context, opt, req.getName(), HTTP_TYPE_TEXT_PLAIN, buf, resp);
            }
            else if (strcmp(File_DLL,req.getType()) == 0)
            {
                getWorkunitDll(context, req.getWuid(),buf);

                //resp.setThefile(buf);
                //resp.setThefile_mimetype(HTTP_TYPE_OCTET_STREAM);
                openSaveFile(context, opt, req.getName(), HTTP_TYPE_OCTET_STREAM, buf, resp);
            }
            else if (strcmp(File_Res,req.getType()) == 0)
            {
                getWorkunitResTxt(context, req.getWuid(),buf);
                openSaveFile(context, opt, "res.txt", HTTP_TYPE_TEXT_PLAIN, buf, resp);
            }
            else if (strncmp(req.getType(), File_ThorLog, 7) == 0)
            {
                getWorkunitThorLog(context, req.getType(), req.getWuid(),buf);
                openSaveFile(context, opt, "thormaster.log", HTTP_TYPE_TEXT_PLAIN, buf, resp);
            }
            else if (strcmp(File_ThorSlaveLog,req.getType()) == 0)
            {
                getWorkunitThorSlaveLog(context, req.getWuid(), req.getSlaveIP(),buf);
                openSaveFile(context, opt, "ThorSlave.log", HTTP_TYPE_TEXT_PLAIN, buf, resp);
            }
            else if (strcmp(File_EclAgentLog,req.getType()) == 0)
            {
                getWorkunitEclAgentLog(context, req.getWuid(),buf);
                openSaveFile(context, opt, "eclagent.log", HTTP_TYPE_TEXT_PLAIN, buf, resp);
            }
            else if (strcmp(File_XML,req.getType()) == 0)
            {
                getWorkunitXml(context, req.getWuid(), req.getPlainText(), buf);

                resp.setThefile(buf);
                const char* plainText = req.getPlainText();
                if (plainText && (!stricmp(plainText, "yes")))
                    resp.setThefile_mimetype(HTTP_TYPE_TEXT_PLAIN);
                else
                    resp.setThefile_mimetype(HTTP_TYPE_TEXT_XML);
            }
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

struct WUExceptions
{
    WUExceptions(IConstWorkUnit& wu): numerr(0), numwrn(0), numinf(0)
    {
        Owned<IConstWUExceptionIterator> it = &wu.getExceptions();
        ForEach(*it)
        {
            
            SCMStringBuffer src, msg, file;
            Owned<IEspECLException> e= createECLException("","");
            e->setCode(it->query().getExceptionCode());
            e->setSource(it->query().getExceptionSource(src).str());
            e->setMessage(it->query().getExceptionMessage(msg).str());
            e->setFileName(it->query().getExceptionFileName(file).str());
            e->setLineNo(it->query().getExceptionLineNo());
            e->setColumn(it->query().getExceptionColumn());

            const char * label = "";
            switch (it->query().getSeverity())
            {
                default:
                case ExceptionSeverityError: label = "Error"; numerr++; break;
                case ExceptionSeverityWarning: label = "Warning"; numwrn++; break;
                case ExceptionSeverityInformation: label = "Info"; numinf++; break;
            }

            e->setSeverity(label);
            errors.append(*e.getLink());
        }
    }

    operator IArrayOf<IEspECLException>&() { return errors; }
    int ErrCount() { return numerr; }
    int WrnCount() { return numwrn; }
    int InfCount() { return numinf; } 

private:
    IArrayOf<IEspECLException> errors;
    int numerr;
    int numwrn;
    int numinf;

};

struct WUList
{
    WUList(IEspContext& context,const char* owner=NULL,const char* state=NULL,const char* cluster=NULL,const char* startDate=NULL,const char* endDate=NULL,const char* ecl=NULL,const char* jobname=NULL,const char* appname=NULL,const char* appkey=NULL,const char* appvalue=NULL)
    {
        
        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        lookupAccess(context, accessOwn, accessOthers);
        

        Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
        StringBuffer path;
        path.append("*");
        
        if(ecl && *ecl)
            path.append("[Query/Text=?~\"*").append(ecl).append("*\"]");

        if(state && *state)
            path.append("[@state=\"").append(state).append("\"]");

        if(cluster && *cluster)
            path.append("[@clusterName=\"").append(cluster).append("\"]");

        if(owner && *owner)
            path.append("[@submitID=?~\"").append(owner).append("\"]");

        if(jobname && *jobname)
            path.append("[@jobName=?~\"*").append(jobname).append("*\"]");

        if(appname && *appname || appkey && *appkey || appvalue && *appvalue)
        {
            path.append("[Application/").append(appname && *appname ? appname : "*");
            path.append("/").append(appkey && *appkey ? appkey : "*");
            if(appvalue && *appvalue)
                path.append("=?~\"").append(appvalue).append("\"");

            path.append("]");
        }
        Owned<IConstWorkUnitIterator> it(factory->getWorkUnitsByXPath(path.str()));
        StringBuffer wuFrom, wuTo;
        if(startDate && *startDate)
            getWuid(startDate, wuFrom);

        if(endDate && *endDate)
            getWuid(endDate, wuTo);

        ForEach(*it)
        {

            SCMStringBuffer wuid;
            IConstWorkUnit &wu = it->query();

            //skip any workunits without access
            if (getAccess(context, wu, accessOwn, accessOthers) < SecAccess_Read)
                continue;

            wu.getWuid(wuid);

            if (wuFrom.length() && strcmp(wuid.str(),wuFrom.str())<0)
                continue;


            if (wuTo.length() && strcmp(wuid.str(),wuTo.str())>0)
                continue;

            if (state && *state)
            {
                SCMStringBuffer st;
                wu.getStateDesc(st);

                if(stricmp(st.str(),state)!=0)
                    continue;
            }

            SCMStringBuffer parent;
            if (!wu.getParentWuid(parent).length())
            {
                parent.clear();
                wuids.push_back(wu.getWuid(parent).str());
            }
        }
        std::sort(wuids.begin(),wuids.end(),std::greater<std::string>());
    }

    typedef std::vector<std::string>::iterator iterator;

    iterator begin() { return wuids.begin(); }
    
    iterator end()   { return wuids.end(); }

    iterator locate(const char* wuid)
    {
        if(wuids.size() && *wuids.begin()>wuid)
        {
            return std::lower_bound(wuids.begin(),wuids.end(),wuid,std::greater<std::string>());
        }
        return wuids.begin();
    }

     __int64 getSize() { return wuids.size(); }

private:

    StringBuffer& getWuid(const char* timestamp,StringBuffer& buf)
    {
        CDateTime wuTime;
        wuTime.setString(timestamp,NULL,true);

        unsigned year, month, day, hour, minute, second, nano;
        wuTime.getDate(year, month, day, true);
        wuTime.getTime(hour, minute, second, nano, true);
        buf.appendf("W%4d%02d%02d-%02d%02d%02d",year,month,day,hour,minute,second);
        return buf;
    }



    //finds out access on owned and others' workunits for the current context's user
    //
    void lookupAccess(IEspContext& context, SecAccessFlags& accessOwn, 
                                      SecAccessFlags& accessOthers)
    {
        if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
            accessOwn = SecAccess_None;

        if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
            accessOthers = SecAccess_None;

        //throw exception if no access on both own/others' workunits
        if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
        {
            context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

            StringBuffer msg;
            msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
        }
    }

    //returns one of the two params accessOwn or accessOthers depending on if the user is owner of wu or not
    //
    SecAccessFlags getAccess(IEspContext& context, IConstWorkUnit& wu, 
                                             SecAccessFlags accessOwn, 
                                             SecAccessFlags accessOthers)
    {
        StringBuffer user;
        context.getUserID(user);

        SCMStringBuffer owner;
        wu.getUser(owner);

        return !stricmp(user, owner.str()) ? accessOwn : accessOthers;
    }


    std::vector<std::string> wuids;
};

IEspECLWorkunit* createQueryWorkunit(const char* wuid,IEspContext &context)
{
    try
    {
        CWUWrapper wu(wuid, context);
        SCMStringBuffer buf;
        Owned<IEspECLWorkunit> info= createECLWorkunit("","");
        info->setWuid(wu->getWuid(buf).str());
        info->setProtected(wu->isProtected() ? 1 : 0);
        info->setJobname(wu->getJobName(buf).str());
        info->setOwner(wu->getUser(buf).str());

            double version = context.getClientVersion();
            if (version > 1.06)
            {       
                SCMStringBuffer roxieClusterName;
                Owned<IConstWURoxieQueryInfo> roxieQueryInfo = wu->getRoxieQueryInfo();
                if (roxieQueryInfo)
                {
                    roxieQueryInfo->getRoxieClusterName(roxieClusterName);
                    if (roxieClusterName.length() > 0)
                    {
                        info->setRoxieCluster(roxieClusterName.str());
                    }
                }
            }

        info->setCluster(wu->getClusterName(buf).str());
      info->setStateID(wu->getState());
      info->setState(wu->getStateDesc(buf).str());

        if (version > 1.29 && wu->isPausing())
        {
            info->setIsPausing(true);
        }

        if (version > 1.00)
        {       
            info->setEventSchedule(0);
            if (info->getState() && !stricmp(info->getState(), "wait"))
            {
                info->setEventSchedule(2); //Can deschedule
            }
            else
            {
                bool eventCountRemaining = false;
                bool eventCountUnlimited = false;
                Owned<IConstWorkflowItemIterator> it = wu->getWorkflowItems();
                if (it)
                {
                    ForEach(*it)
                    {
                        IConstWorkflowItem *r = it->query();
                        if (r)
                        {
                            IWorkflowEvent *wfevent = r->getScheduleEvent();
                            if (wfevent)
                            {
                                if (r->hasScheduleCount())
                                {
                                    if (r->queryScheduleCountRemaining() > 0)
                                        eventCountRemaining = true;
                                }
                                else
                                {
                                    eventCountUnlimited = true;
                                }
                            }
                        }
                    }

                    if (eventCountUnlimited || eventCountRemaining)
                    {
                        info->setEventSchedule(1); //Can reschedule
                    }
                }
            }
        }
        if (version > 1.27)
        {       
            StringBuffer totalThorTimeStr;
            unsigned totalThorTimeMS = wu->getTimerDuration("Total thor time", NULL);
            formatDuration(totalThorTimeStr, totalThorTimeMS);
            info->setTotalThorTime(totalThorTimeStr.str());
        }

        wu->getSnapshot(buf);
        if(buf.length())
            info->setSnapshot(buf.str());

        CScheduleDateTime dt;
        wu->getTimeScheduled(dt);
        if(dt.isValid())
            info->setDateTimeScheduled(dt.getString(buf).str());
        return info.getLink();
    }
    catch(...)
    {
        DBGLOG("Exception opening %s",wuid);
        return NULL;
    }
}
    
bool CWsWorkunitsEx::onWUQuery(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    try
    {
        DBGLOG("Started CWsWorkunitsEx::onWUQuery\n");

        if (req.getECL() && *req.getECL() || req.getApplicationName() && *req.getApplicationName() || req.getApplicationKey() && *req.getApplicationKey()
            || req.getApplicationData() && *req.getApplicationData())
        {
            doWUQueryByXPath(context, req, resp);
        }
        else
        {
            const char *type = req.getType();
            if (!type || !*type || stricmp(type, "archived workunits"))
                doWUQueryWithSort(context, req, resp);
            else
            {
                StringBuffer sashaAddress;
                //sashaAddress = (char *) req.getSashaNetAddress();
                IArrayOf<IConstTpSashaServer> sashaservers;
                CTpWrapper dummy;
                dummy.getTpSashaServers(sashaservers);  
                ForEachItemIn(i, sashaservers)
                {
                    IConstTpSashaServer& sashaserver = sashaservers.item(i);
                    IArrayOf<IConstTpMachine> &sashaservermachine = sashaserver.getTpMachines();
                    //sashaAddress = (char *) sashaservermachine.item(0).getNetaddress();
                    sashaAddress.append(sashaservermachine.item(0).getNetaddress());
                }

                if (sashaAddress.length() > 0)
                    doWUQueryForArchivedWUs(context, req, resp, sashaAddress.str());
            }

            ///if (sashaAddress.length() < 1)
            /// doWUQueryWithSort(context, req, resp);
            ///else
            /// doWUQueryForArchivedWUs(context, req, resp, sashaAddress.str());
        }

        double version = context.getClientVersion();

        resp.setState(req.getState());
        resp.setCluster(req.getCluster());
        if (version > 1.07)
        {
            resp.setRoxieCluster(req.getRoxieCluster());
        }
        resp.setOwner(req.getOwner());
        resp.setStartDate(req.getStartDate());
        resp.setEndDate(req.getEndDate());

        StringBuffer basicQuery;
        if (req.getState() && *req.getState())
            addToQueryString(basicQuery, "State", req.getState());
        if (req.getCluster() && *req.getCluster())
            addToQueryString(basicQuery, "Cluster", req.getCluster());
        if (version > 1.07)
        {
            if (req.getRoxieCluster() && *req.getRoxieCluster())
                addToQueryString(basicQuery, "RoxieCluster", req.getRoxieCluster());
        }
        if (req.getOwner() && *req.getOwner())
            addToQueryString(basicQuery, "Owner", req.getOwner());
        if (req.getStartDate() && *req.getStartDate())
            addToQueryString(basicQuery, "StartDate", req.getStartDate());
        if (req.getEndDate() && *req.getEndDate())
            addToQueryString(basicQuery, "EndDate", req.getEndDate());
        if (version > 1.25 && req.getLastNDays() > -1)
        {
            StringBuffer sBuf;
            sBuf.append(req.getLastNDays());
            addToQueryString(basicQuery, "LastNDays", sBuf.str());
        }
        if (req.getECL() && *req.getECL())
            addToQueryString(basicQuery, "ECL", req.getECL());
        if (req.getJobname() && *req.getJobname())
            addToQueryString(basicQuery, "Jobname", req.getJobname());
        if (req.getType() && *req.getType())
            addToQueryString(basicQuery, "Type", req.getType());
        if (req.getLogicalFile() && *req.getLogicalFile())
        {
            addToQueryString(basicQuery, "LogicalFile", req.getLogicalFile());  
            if (req.getLogicalFileSearchType() && *req.getLogicalFileSearchType())
                addToQueryString(basicQuery, "LogicalFileSearchType", req.getLogicalFileSearchType());  
        }
        resp.setFilters(basicQuery.str());

        bool bSortbyValid = false;
        if (req.getSortby() && *req.getSortby())
        {
            StringBuffer strbuf = req.getSortby();
            strbuf.append("=");

            String str(basicQuery.str());
            String str1(strbuf.str());
            if (str.indexOf(str1) < 0)
            {
                bSortbyValid = true;
            }
        }

        if (bSortbyValid)
        {
            resp.setSortby(req.getSortby());
            if (req.getDescending())
                resp.setDescending(req.getDescending());
        }

        StringBuffer eclbuf;
        if(req.getECL())
            Utils::url_encode(req.getECL(), eclbuf);
        resp.setECL(eclbuf.str());

        StringBuffer jobbuf;
        if(req.getJobname())
            Utils::url_encode(req.getJobname(), jobbuf);
        resp.setJobname(jobbuf.str());

        if (bSortbyValid)
        {
            addToQueryString(basicQuery, "Sortby", req.getSortby());
            if (req.getDescending())
                addToQueryString(basicQuery, "Descending", "1");
        }
        resp.setBasicQuery(basicQuery.str());

    /*if (resp.getCurrent() && *resp.getCurrent())
        addToQueryString(queryString, "Current", resp.getCurrent().str());
    if (resp.getCount() && *resp.getCount())
        addToQueryString(queryString, "Count", resp.getCount().str());
    resp.setQueryForPaging(basicQuery.str());*/
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void CWsWorkunitsEx::addToQueryString(StringBuffer &queryString, const char *name, const char *value)
{
    if (queryString.length() > 0)
    {
        queryString.append("&");
    }

    queryString.append(name);
    queryString.append("=");
    queryString.append(value);
}

void CWsWorkunitsEx::doWUQueryWithSort(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    DBGLOG("Started CWsWorkunitsEx::doWUQueryWithSort\n");

    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
        accessOwn = SecAccess_None;

    if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
        accessOthers = SecAccess_None;

    double version = context.getClientVersion();

    //throw exception if no access on both own/others' workunits
    if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
    {
        context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

        StringBuffer msg;
        msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
    }

    StringBuffer user;
    context.getUserID(user);

    bool bDoubleCheckState = false;
    IArrayOf<IEspECLWorkunit> results;

    if(req.getWuid() && *req.getWuid())
    {
        IEspECLWorkunit* wu=createQueryWorkunit(req.getWuid(), context);
        if(wu)
        {
            results.append(*wu);
                if (version > 1.02)
                {
                    resp.setPageSize(1);
                }
                else
                {
                    resp.setCount(1);
                }
        }
    }
    else if (req.getLogicalFile() && *req.getLogicalFile() && req.getLogicalFileSearchType() && 
        *req.getLogicalFileSearchType() && (stricmp(req.getLogicalFileSearchType(), "Created") == 0))
    {
        Owned<IUserDescriptor> userdesc;
        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();
        userdesc.setown(createUserDescriptor());
        userdesc->set(username.str(), passwd);
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(req.getLogicalFile(), userdesc);
        if (!df)
        {
            throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find file %s.",req.getLogicalFile());
        }

        const char* wuid = df->queryProperties().queryProp("@workunit");
        if (wuid && *wuid)
        {
            Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
            IConstWorkUnit* wu= factory->openWorkUnit(wuid, false);
            if (!wu)
            {
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit for file %s.",req.getLogicalFile());
            }

            SCMStringBuffer owner;
            wu->getUser(owner);
            //skip any workunits without access
            if ((!stricmp(user, owner.str()) ? accessOwn : accessOthers) < SecAccess_Read)
            {
                throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED,"Cannot access the workunit for file %s.",req.getLogicalFile());
            }

            SCMStringBuffer parent;
            if (!wu->getParentWuid(parent).length())
            {
                parent.clear();

                IEspECLWorkunit* wuEspECL=createQueryWorkunit(wu->getWuid(parent).str(), context);
                if (wuEspECL && (wu->getState() == WUStateScheduled) && wu->aborting())
                {
                    //wuEspECL->setAborting(true);
                    wuEspECL->setStateID(WUStateAborting);
                    wuEspECL->setState("aborting");
                    //wuEspECL->setStateEx(WUStateAborting);
                }

                if(wuEspECL)
                    results.append(*wuEspECL);
            }
        
            resp.setFirst(false);
            if (version > 1.02)
            {
                resp.setPageSize(1);
            }
            else
            {
                resp.setCount(1);
            }
        }   
        else
        {
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.",req.getLogicalFile());
        }
   }
    else
    {
        unsigned begin = 0;
        int count = 100;
        int pagesize = 100;
        if (version > 1.01)
        {
            pagesize = req.getPageSize();
            if (!req.getCount_isNull())
                pagesize = req.getCount();

            if(pagesize < 1)
                pagesize = 100;

            begin = req.getPageStartFrom();
        }
        else
        {
            count=req.getCount();
            if(!count)
                count=100;

            if(req.getAfter() && *req.getAfter())
            {
                begin=atoi(req.getAfter());
            }
            else if(req.getBefore() && *req.getBefore())
            {
                begin=atoi(req.getBefore())-count;
            }

            if (begin < 0)
                begin = 0;
    
            pagesize = count;
        }

        WUSortField sortorder[2] = {WUSFwuid,WUSFterm};
        sortorder[0] = (WUSortField) (sortorder[0] | WUSFreverse); //Default to WUID/descending

        if(req.getSortby() && *req.getSortby())
        {
            const char *sortby = req.getSortby();
            if (!stricmp(sortby, "Owner"))
                sortorder[0] = WUSFuser;
            else if (!stricmp(sortby, "JobName"))
                sortorder[0] = WUSFjob;
            else if (!stricmp(sortby, "Cluster"))
                sortorder[0] = WUSFcluster;
            else if (!stricmp(sortby, "RoxieCluster"))
                sortorder[0] = WUSFroxiecluster;
            else if (!stricmp(sortby, "Protected"))
                sortorder[0] = WUSFprotected;
            else if (!stricmp(sortby, "State"))
                sortorder[0] = WUSFstate;
            else if (!stricmp(sortby, "ThorTime"))
            {
                sortorder[0] = (WUSortField) (WUSFtotalthortime+WUSFnumeric);
            }
            else
                sortorder[0] = WUSFwuid;
    
            bool descending = req.getDescending();
            if (descending)
                sortorder[0] = (WUSortField) (sortorder[0] | WUSFreverse);
        }

        WUSortField filters[10];
        unsigned short filterCount = 0;
        MemoryBuffer filterbuf;

        //comment out because it does not support by getWorkUnitsSorted()
        //if(ecl && *ecl)
        //  path.append("[Query/Text=?~\"*").append(ecl).append("*\"]");

        if(req.getState() && *req.getState())
        {
            filters[filterCount] = WUSFstate;
            filterCount++;
            if (stricmp(req.getState(), "unknown") != 0)
                filterbuf.append(req.getState());
            else
                filterbuf.append("");

            if (stricmp(req.getState(), "submitted") == 0)
                bDoubleCheckState = true;
        }

        if(req.getCluster() && *req.getCluster())
        {
            filters[filterCount] = WUSFcluster;
            filterCount++;
            filterbuf.append(req.getCluster());
        }

        if (version > 1.07)
        {
            if(req.getRoxieCluster() && *req.getRoxieCluster())
            {
                filters[filterCount] = WUSFroxiecluster;
                filterCount++;
                filterbuf.append(req.getRoxieCluster());
            }
        }

        if(req.getLogicalFile() && *req.getLogicalFile())
        {
            filters[filterCount] = WUSFfileread;
            filterCount++;
            filterbuf.append(req.getLogicalFile());
        }

        if(req.getOwner() && *req.getOwner())
        {
            filters[filterCount] = WUSortField (WUSFuser | WUSFnocase);
            filterCount++;
            filterbuf.append(req.getOwner());
        }

        if(req.getJobname() && *req.getJobname())
        {
            filters[filterCount] = WUSortField (WUSFjob | WUSFnocase);
            filterCount++;
            filterbuf.append(req.getJobname());
        }

        StringBuffer wuFrom, wuTo;
        if(req.getStartDate() && *req.getStartDate())
        {
            CDateTime wuTime;
            wuTime.setString(req.getStartDate(),NULL,true);

            unsigned year, month, day, hour, minute, second, nano;
            wuTime.getDate(year, month, day, true);
            wuTime.getTime(hour, minute, second, nano, true);
            wuFrom.appendf("W%4d%02d%02d-%02d%02d%02d",year,month,day,hour,minute,second);
            filters[filterCount] = WUSFwuid;
            filterCount++;
            filterbuf.append(wuFrom.str());
        }

        if(req.getEndDate() && *req.getEndDate())
        {
            CDateTime wuTime;
            wuTime.setString(req.getEndDate(),NULL,true);

            unsigned year, month, day, hour, minute, second, nano;
            wuTime.getDate(year, month, day, true);
            wuTime.getTime(hour, minute, second, nano, true);
            wuTo.appendf("W%4d%02d%02d-%02d%02d%02d",year,month,day,hour,minute,second);
            filters[filterCount] = WUSFwuidhigh;
            filterCount++;
            filterbuf.append(wuTo.str());
        }
        filters[filterCount] = WUSFterm;

        unsigned actualCount = 0;
        __int64 cachehint=0;
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        unsigned numWUs = factory->numWorkUnitsFiltered(filters, filterbuf.bufferBase());
        Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), begin, pagesize+1, "", NULL);
        //WUList wl(context,req.getOwner(),req.getState(),req.getCluster(),req.getStartDate(),req.getEndDate(),req.getECL(),req.getJobname(),req.getApplicationName(),req.getApplicationKey(),req.getApplicationData());

        ForEach(*it) 
        {
            actualCount++;

            //skip any workunits without access
            IConstWorkUnit& wu = it->query();
            SCMStringBuffer owner;
            wu.getUser(owner);
            if ((!stricmp(user, owner.str()) ? accessOwn : accessOthers) < SecAccess_Read)
                continue;

            if (bDoubleCheckState && (wu.getState() != WUStateSubmitted))
                continue;

            SCMStringBuffer parent;
            if (!wu.getParentWuid(parent).length())
            {
                parent.clear();

                IEspECLWorkunit* wuEspECL=createQueryWorkunit(wu.getWuid(parent).str(), context);

                if (wuEspECL && (wu.getState() == WUStateScheduled) && wu.aborting())
                {
                    //wuEspECL->setAborting(true);
                    wuEspECL->setStateID(WUStateAborting);
                    wuEspECL->setState("aborting");
                    //wuEspECL->setStateEx(WUStateAborting);
                }

                if(wuEspECL)
                    results.append(*wuEspECL);
            }
        }

        if (version > 1.02)
        {
            resp.setPageStartFrom(begin+1);
            resp.setNumWUs(numWUs);
            if (results.length() > pagesize)
            {
                results.pop();
            }

            if(begin + pagesize < numWUs)
            {
                resp.setNextPage(begin + pagesize);
                resp.setPageEndAt(begin + pagesize);
                int last = begin + pagesize;
                while (last + pagesize < numWUs)
                {
                    last += pagesize;
                }
                resp.setLastPage(last);
            }
            else
            {
                resp.setNextPage(-1);
                resp.setPageEndAt(numWUs);
            }

            if(begin > 0)
            {
                resp.setFirst(false);
                if (begin - pagesize > 0)
                    resp.setPrevPage(begin - pagesize);
                else
                    resp.setPrevPage(0);
            }

            resp.setPageSize(pagesize);
        }
        else
        {
            if(begin>0 && actualCount > 0)
            { 
                char buf[10];
                itoa(begin, buf, 10);
                resp.setCurrent(buf);
            }


            if(count<actualCount)
            {
                char buf[10];
                itoa(begin+count, buf, 10);
                resp.setNext(buf);
                resp.setNumWUs(numWUs);
                if (results.length() > count)
                {
                    results.pop();
                }
            }

            if(begin == 0 && actualCount <= count)
            {
                resp.setFirst(false);
            }

            resp.setCount(abs(count));
        }
    }

    resp.setWorkunits(results);
    return;
}

int readFromCommaSeparatedString(const char *commaSeparatedString, StringBuffer* output)
{
    int numOfItems = 0;
    if (commaSeparatedString && *commaSeparatedString)
    {
        char *pStr = (char *) commaSeparatedString;
        while (pStr)
        {
            char item[1024];
            bool bFoundComma = false;
            int len = strlen(pStr);
            for (int i = 0; i < len; i++)
            {
                char *pStr1 = pStr + i;
                if (pStr1[0] != ',')
                    continue;

                strncpy(item, pStr, pStr1 - pStr);
                item[pStr1 - pStr] = 0;

                bFoundComma = true;
                if (i < len - 1)
                    pStr = pStr1 + 1;
                else
                    pStr = NULL;

                break;
            }

            if (!bFoundComma && len > 0)
            {
                strcpy(item, pStr);
                pStr = NULL;
            }
             
            output[numOfItems] = item;
            numOfItems++;
        }
    }

    return numOfItems;
}

bool CWsWorkunitsEx::onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp)
{
    IArrayOf<IEspQuerySet> querySets;

    
    Owned<IPropertyTree> queryRegistry = getQueryRegistryRoot();

    if (!queryRegistry)
    {
        return false;
    }

    Owned<IPropertyTreeIterator> querySetElements = queryRegistry->getElements("QuerySet");
    if (querySetElements->first()) 
    {
        do 
        {
            IPropertyTree &querySet = querySetElements->query();                    
            const char* name = querySet.queryProp("@id");
            Owned<IEspQuerySet> espQuerySet = createQuerySet("", "");
            espQuerySet->setQuerySetName(name);
            querySets.append(*espQuerySet.getLink());

        } while (querySetElements->next());
    }
    
    resp.setQuerysets(querySets);
    return true;
}

bool CWsWorkunitsEx::onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());

    IArrayOf<IEspQuerySetQuery> querySetQueries;
    IArrayOf<IEspQuerySetAlias> querySetAliases;

    // fetch query registry for queryset.

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(req.getQuerySetName(), true);
    if (!queryRegistry) 
    {
        return false;
    }

    Owned<IPropertyTreeIterator> queries = queryRegistry->getElements("Query");
    ForEach(*queries)
    {
        IPropertyTree &query = queries->query();
        const char *id = query.queryProp("@id");
        const char *wuid = query.queryProp("@wuid");
        const char *dll = query.queryProp("@dll");
        const char *name = query.queryProp("@name");
        bool suspended = query.getPropBool("@suspended", false);
        Owned<IEspQuerySetQuery> querySetQuery = createQuerySetQuery("", "");
        querySetQuery->setId(id);
        querySetQuery->setName(name);
        querySetQuery->setDll(dll);
        querySetQuery->setWuid(wuid);
        querySetQuery->setSuspended(suspended);
        querySetQueries.append(*querySetQuery.getLink());

    }

    resp.setQuerysetQueries(querySetQueries);


    Owned<IPropertyTreeIterator> aliases = queryRegistry->getElements("Alias");
    ForEach(*aliases)
    {
        IPropertyTree &alias = aliases->query();
        const char *id = alias.queryProp("@id");
        const char *name = alias.queryProp("@name");

        Owned<IEspQuerySetAlias> querySetAlias = createQuerySetAlias("", "");
        querySetAlias->setName(name);
        querySetAlias->setId(id);
        querySetAliases.append(*querySetAlias.getLink());

    }

    resp.setQuerysetAliases(querySetAliases);
    return true;
}

bool CWsWorkunitsEx::onWUQuerysetActionQueries(IEspContext &context, IEspWUQuerySetActionQueriesRequest & req, IEspWUQuerySetActionQueriesResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());
    resp.setRemove(req.getRemove());

    bool bRemove = req.getRemove();
    bool bToggleSuspend = req.getToggleSuspend();

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(req.getQuerySetName(), false);
    IArrayOf<IEspQuerySetQueryAction> querySetQueryActions;

    for(int qa=0; qa<req.getQuerysetQueryActions().length();qa++)
    {   

        IConstQuerySetQueryAction& item=req.getQuerysetQueryActions().item(qa);

        if (bRemove) {
            if(item.getId() && *item.getId())
            {
                // First Remove Aliases (if there are).
                // Remove Query from Queryset.
                removeAliasesFromNamedQuery(queryRegistry, item.getId());
                removeNamedQuery(queryRegistry, item.getId());

                Owned<IEspQuerySetQueryAction> querySetQueryAction = createQuerySetQueryAction("", "");
                querySetQueryAction->setId(item.getId());
                querySetQueryAction->setStatus("Completed");

                querySetQueryActions.append(*querySetQueryAction.getLink());

            }
        }
        if (bToggleSuspend) {
            if(item.getId() && *item.getId())
            {
                bool suspended = item.getSuspended();

                setQuerySuspendedState(queryRegistry, item.getId(), suspended ? false : true);

                Owned<IEspQuerySetQueryAction> querySetQueryAction = createQuerySetQueryAction("", "");
                querySetQueryAction->setId(item.getId());
                querySetQueryAction->setStatus("Completed");

                querySetQueryActions.append(*querySetQueryAction.getLink());

            }
        }
    }

    resp.setQuerysetQueryActions(querySetQueryActions);

    return true;
}

bool CWsWorkunitsEx::onWUQuerysetActionAliases(IEspContext &context, IEspWUQuerySetActionAliasesRequest & req, IEspWUQuerySetActionAliasesResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());
    resp.setRemove(req.getRemove());

    bool bRemove = req.getRemove();

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(req.getQuerySetName(), false);
    IArrayOf<IEspQuerySetAliasAction> querySetAliasActions;

    for(int aa=0; aa<req.getQuerysetAliasActions().length();aa++)
    {   

        IConstQuerySetAliasAction& item=req.getQuerysetAliasActions().item(aa);

        if (bRemove) {
            if(item.getId() && *item.getId())
            {
                // First Remove Aliases (if there are).
                // Remove Query from Queryset.
                removeAliasesFromNamedQuery(queryRegistry, item.getId());

                Owned<IEspQuerySetAliasAction> querySetAliasAction = createQuerySetAliasAction("", "");
                querySetAliasAction->setId(item.getId());
                querySetAliasAction->setStatus("Completed");

                querySetAliasActions.append(*querySetAliasAction.getLink());

            }
        }
    }

    resp.setQuerysetAliasActions(querySetAliasActions);

    return true;
}

bool CWsWorkunitsEx::onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    SCMStringBuffer wuid;
    wuid.set(req.getWuid());
    int activateOption = req.getActivate();

    if (!wuid.length())
        throw MakeStringException(ECLWATCH_NO_WUID_SPECIFIED,"No Workunit ID has been specified.");

    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
    IConstWorkUnit* wu= factory->openWorkUnit(wuid.str(), false);
    if (!wu)
    {
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", wuid.str());
    }

    SCMStringBuffer queryName;
    queryName.set(req.getJobName());

    if (!queryName.length())
    {
        queryName.set(wu->getJobName(queryName).str());
    }

    SCMStringBuffer wuCluster;
    
    wu->getClusterName(wuCluster);

    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(wuCluster.str());
    SCMStringBuffer querySetName;
    clusterInfo->getQuerySetName(querySetName); 

    SCMStringBuffer targetClusterType;
    wu->getDebugValue("targetclustertype", targetClusterType);

    if (stricmp(targetClusterType.str(), "roxie") == 0)
    {
        StringBuffer user;
        StringBuffer password;
        context.getUserID(user);
        context.getPassword(password);

        SCMStringBuffer status;
        SCMStringBuffer roxieDeployStatus;

        StringBuffer daliIp;
        const SocketEndpoint &ep1 = queryCoven().queryComm().queryGroup().queryNode(0).endpoint();
        ep1.getUrlStr(daliIp);
    
        SocketEndpoint ep;
        StringBuffer netAddress;
        getClusterConfig("RoxieCluster", wuCluster.str(), "RoxieServerProcess[1]", netAddress);
        ep.getUrlStr(netAddress);

        Owned<IRoxieQueryManager> manager = createRoxieQueryManager(ep, querySetName.str(), daliIp, roxieQueryRoxieTimeOut, user.str(), password.str(), 1);
    
        Owned<IRoxieQueryProcessingInfo> processingInfo = createRoxieQueryProcessingInfo();
        processingInfo->setLoadDataOnly(false);
        processingInfo->setResolveFileInfo(true);
        processingInfo->setNoForms(false);
        if (wu->hasDebugValue("lookupDaliIp"))
        {
            SCMStringBuffer ip;
            wu->getDebugValue("lookupDaliIp", ip);
            processingInfo->setDfsDaliIp(ip.str());
        }
        else
            processingInfo->setDfsDaliIp(daliIp.str());
        processingInfo->setResolveKeyDiffInfo(false);
        processingInfo->setCopyKeyDiffLocationInfo(false);
        processingInfo->setLayoutTranslationEnabled(false);


        manager->deployWorkunit(wuid, queryName, *processingInfo.get(), user.str(), MAKE_ACTIVATE, querySetName.str(), req.getNotifyCluster(), status, roxieDeployStatus);
    }
    else
        processWorkunit(wu, wuid.str(), queryName, wuCluster, querySetName, activateOption);
    return true;
}

void CWsWorkunitsEx::processWorkunit(IConstWorkUnit *workunit, const char* wuid, SCMStringBuffer &queryName,  SCMStringBuffer &clusterName, SCMStringBuffer &querySetName, int activateOption)
{
    Owned<IWorkUnit> wu = &workunit->lock();

    SCMStringBuffer jobName;
    wu->getJobName(jobName);

    if (stricmp(jobName.str(),queryName.str())!=0)
    {
        wu->setJobName(queryName.str());
    }

    SCMStringBuffer dllName;
    try
    {
        Owned<IConstWUQuery> q = workunit->getQuery();
        q->getQueryDllName(dllName);
    }
    catch (IException* e)
    { 
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU, "%s", msg.str());
    }


    StringBuffer queryId;
    addQueryToQuerySet(wu, querySetName.str(), queryName.str(), NULL, ACTIVATE_DELETE_PREVIOUS, queryId);
}

void CWsWorkunitsEx::doWUQueryForArchivedWUs(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp, const char *sashaAddress)
{
    DBGLOG("Started CWsWorkunitsEx::doWUQueryForArchivedWUs\n");

    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
        accessOwn = SecAccess_None;

    if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
        accessOthers = SecAccess_None;

    //throw exception if no access on both own/others' workunits
    if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
    {
        context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

        StringBuffer msg;
        msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
    }

    double version = context.getClientVersion();

    StringBuffer user;
    context.getUserID(user);

    SocketEndpoint ep;
    ep.set(sashaAddress,DEFAULT_SASHA_PORT);
    Owned<INode> sashaserver = createINode(ep);

    int pageSize = req.getPageSize();
    if(pageSize < 1)
        pageSize=100;
    __int64 displayStart = req.getPageStartFrom();
    __int64 displayEnd = displayStart + pageSize;


    unsigned dateLimit = 0;
    bool hasNextPage = true;

    CDateTime wuTimeFrom, wuTimeTo;
    if(req.getEndDate() && *req.getEndDate())
    {
        wuTimeTo.setString(req.getEndDate(),NULL,true);
    }
    else
    {
        wuTimeTo.setNow();
    }

    if(req.getStartDate() && *req.getStartDate())
    {
        wuTimeFrom.setString(req.getStartDate(),NULL,true);
        dateLimit = 1;
    }

    IArrayOf<IEspECLWorkunit> results;

    StringBuffer filter;
    if (req.getCluster() && *req.getCluster())
        filter.appendf("cluster=%s;", req.getCluster());
    if (req.getOwner() && *req.getOwner())
        filter.appendf("owner=%s;", req.getOwner());
    if (req.getJobname() && *req.getJobname())
        filter.appendf("jobName=%s;", req.getJobname());
    if (req.getState() && *req.getState())
        filter.appendf("state=%s;", req.getState());
    if (req.getLastNDays_isNull())
    {
        if (req.getStartDate() && *req.getStartDate())
            filter.appendf("wuTimeFrom=%s;", req.getStartDate());
        if (req.getEndDate() && *req.getEndDate())
            filter.appendf("wuTimeTo=%s;", req.getEndDate());
    }
    else
    {
        filter.appendf("LastNDays=%d;", req.getLastNDays());
    }
    filter.appendf("displayStart=%"I64F"d;", displayStart);
    filter.appendf("pageSize=%d", pageSize);

    Owned<ArchivedWUsCacheElement> archivedWUs = m_archivedWUsCache->lookup(context, filter, "AddWhenAvailable", m_AWUS_cache_minutes);
    if (archivedWUs)
    {
        hasNextPage = archivedWUs->m_hasNextPage;
        if (archivedWUs->m_results.length() > 0)
        {
            for (int i = 0; i < archivedWUs->m_results.length(); i++)
            {
                Owned<IEspECLWorkunit> info= createECLWorkunit("","");
                IEspECLWorkunit& info0 = archivedWUs->m_results.item(i);
                info->copy(info0);

                results.append(*info.getClear());
            }
        }
    }
    else
    {
        IArrayOf<IEspECLWorkunit> resultList;

        CDateTime timeTo = wuTimeTo;
        __int64 totalWus = 0;
        bool doLoop = true;
        while (doLoop)
        {
            CDateTime timeFrom = timeTo;
            timeFrom.adjustTime(-1439);//one day earlier
            if (dateLimit > 0 && wuTimeFrom > timeFrom)
            {
                timeFrom = wuTimeFrom;
            }

            StringBuffer wuFrom, wuTo;
            unsigned year0, month0, day0, hour0, minute0, second0, nano0;
            unsigned year, month, day, hour, minute, second, nano;
            timeFrom.getDate(year0, month0, day0, true);
            timeFrom.getTime(hour0, minute0, second0, nano0, true);
            timeTo.getDate(year, month, day, true);
            timeTo.getTime(hour, minute, second, nano, true);
            wuFrom.appendf("%4d%02d%02d%02d%02d",year0,month0,day0,hour0,minute0);
            wuTo.appendf("%4d%02d%02d%02d%02d",year,month,day,hour,minute);     

            __int64 begin = 0;
            int count = 1000;
            bool doLoop1 = true;
            while (doLoop1)
            {
                Owned<ISashaCommand> cmd = createSashaCommand();
                cmd->setAction(SCA_LIST);                           
                cmd->setOnline(false);                              
                cmd->setArchived(true);                             
                if(req.getCluster() && *req.getCluster())
                    cmd->setCluster(req.getCluster());
                if(req.getOwner() && *req.getOwner())
                    cmd->setOwner(req.getOwner());          
                if(req.getJobname() && *req.getJobname())
                    cmd->setJobName(req.getJobname());          
                if(req.getState() && *req.getState())
                    cmd->setState(req.getState());

                cmd->setAfter(wuFrom.str());      
                cmd->setBefore(wuTo.str());     

                cmd->setStart(begin);
                cmd->setLimit(count);

                cmd->setOutputFormat("owner,jobname,cluster,state");//date range/owner/jobname/state*/

                if (!cmd->send(sashaserver)) 
                {
                    StringBuffer msg;
                    msg.appendf("Cannot connect to archive server at %s.",sashaAddress);
                    throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER, "%s", msg.str());
                }

                unsigned actualCount = cmd->numIds();
                if (actualCount < 1)
                    break;

                totalWus += actualCount;

                if (actualCount < count)
                    doLoop1 = false;

                for (unsigned ii=0;ii<actualCount;ii++) 
                {
                    const char *wuidStr = cmd->queryId(ii);
                    if (!wuidStr)
                        continue;

                    StringBuffer strArray[6];
                    readFromCommaSeparatedString(wuidStr, strArray);

                      //skip any workunits without access
                    const char *owner = cmd->queryOwner();
                    if ((!stricmp(user, owner) ? accessOwn : accessOthers) < SecAccess_Read)
                        continue;

                    if (strArray[0].length() < 0)
                        continue;

                    const char* wuid = strArray[0].str();

                    __int64 addToPos = -1;
                    ForEachItemIn(i, resultList)
                    {
                        IEspECLWorkunit& wu = resultList.item(i);
                        const char *wuidStr0 = wu.getWuid();
                        if (!wuidStr0)
                            continue;

                        if (strcmp(wuid, wuidStr0)>0)
                        {
                            addToPos = i;
                            break;
                        }
                    }

                    if (addToPos < 0 && (i > displayEnd))
                        continue;

                    Owned<IEspECLWorkunit> info= createECLWorkunit("","");
                    info->setWuid(wuid);
                    if (strArray[1].length() > 0)
                          info->setOwner(strArray[1].str());
                    if (strArray[2].length() > 0)
                        info->setJobname(strArray[2].str());
                    if (strArray[3].length() > 0)
                          info->setCluster(strArray[3].str());   
                    if (strArray[4].length() > 0)
                          info->setState(strArray[4].str());   

                    if (addToPos < 0)
                        resultList.append(*info.getClear());
                    else
                        resultList.add(*info.getClear(), addToPos);

                    if (resultList.length() > displayEnd)
                        resultList.pop();
                }

                begin += count;
            }

            timeTo.adjustTime(-1440);//one day earlier
            if (dateLimit > 0 && wuTimeFrom > timeTo) //we reach the date limit
            {
                if (totalWus <= displayEnd)
                    hasNextPage = false;
                doLoop = false;
            }
            else if ( resultList.length() >= displayEnd) //we have all we need
            {
                doLoop = false;
            }
        }

        if (displayEnd > resultList.length())
            displayEnd = resultList.length();

        for (int i = displayStart; i < displayEnd; i++)
        {
            Owned<IEspECLWorkunit> info= createECLWorkunit("","");
            IEspECLWorkunit& info0 = resultList.item(i);
            info->copy(info0);

            results.append(*info.getClear());
        }

        m_archivedWUsCache->add(filter, "AddWhenAvailable", hasNextPage, results);
    }

    resp.setPageStartFrom(displayStart+1);
    resp.setPageEndAt(displayEnd);

   if(dateLimit < 1 || hasNextPage)
   {
        resp.setNextPage(displayStart + pageSize);
    }
    else
    {
        resp.setNextPage(-1);
    }

    if(displayStart > 0)
    {
      resp.setFirst(false);
        if (displayStart - pageSize > 0)
            resp.setPrevPage(displayStart - pageSize);
        else
            resp.setPrevPage(0);
    }

    resp.setPageSize(pageSize);
    resp.setWorkunits(results);
    resp.setType("archived only");
    return;
}

void CWsWorkunitsEx::doWUQueryByXPath(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    DBGLOG("Started CWsWorkunitsEx::doWUQueryByXPath\n");
    IArrayOf<IEspECLWorkunit> results;

    if(req.getWuid() && *req.getWuid())
    {
        IEspECLWorkunit* wu=createQueryWorkunit(req.getWuid(), context);
        if(wu)
        {
            results.append(*wu);
            resp.setPageSize(1);
        }
    }
    else
    {

        WUList wl(context,req.getOwner(),req.getState(),req.getCluster(),req.getStartDate(),req.getEndDate(),req.getECL(),req.getJobname(),req.getApplicationName(),req.getApplicationKey(),req.getApplicationData());

        int count=req.getPageSize();
        if(!count)
            count=100;

        WUList::iterator begin, end;

        if(req.getAfter() && *req.getAfter())
        {
            begin=wl.locate(req.getAfter());
            end=min(begin+count,wl.end());
        }
        else if(req.getBefore() && *req.getBefore())
        {
            end=wl.locate(req.getBefore());
            begin=max(end-count,wl.begin());
        }
        else
        {
            begin=wl.begin();
            end=min(begin+count,wl.end());
        }

        if(begin>wl.begin() && begin<wl.end())
        { 
            resp.setCurrent(begin->c_str());
        }

          double version = context.getClientVersion();
          if (version > 1.02)
          {
              resp.setPageStartFrom(begin - wl.begin() + 1);
               resp.setNumWUs(wl.getSize());
             resp.setCount(end - begin);
          }

        if(end<wl.end())
        {
            resp.setNext(end->c_str());
        }
    
        for(;begin!=end;begin++)
        {
            IEspECLWorkunit* wu=createQueryWorkunit(begin->c_str(), context);
            if(wu)
                results.append(*wu);
        }
        resp.setPageSize(abs(count));
    }

    resp.setWorkunits(results);

    return;
}

void CWsWorkunitsEx::getScheduledWUs(IEspContext &context, const char *serverName, const char *eventName, IArrayOf<IEspScheduledWU> & results)
{
    if (serverName && *serverName)
    {
        Owned<IScheduleReader> reader;
        reader.setown(getScheduleReader(serverName, eventName));
        Owned<IScheduleReaderIterator> iter(reader->getIterator());
        while(iter->isValidEventName())
        {
            StringBuffer buf1;
            char *eventName1 = (char *)iter->getEventName(buf1.clear()).str();
            while(iter->isValidEventText())
            {
                StringBuffer buf2;
                char *wuid = NULL;
                char *eventText = (char *)iter->getEventText(buf2.clear()).str();
                while(iter->isValidWuid())
                {
                    StringBuffer buf3;
                    wuid = (char *)iter->getWuid(buf3.clear()).str();

                    Owned<IEspScheduledWU> scheduledWU = createScheduledWU("");
                    if (wuid && *wuid)
                    {
                        scheduledWU->setWuid(wuid);
                        scheduledWU->setCluster(serverName);
                        if (eventName1 && *eventName1)
                            scheduledWU->setEventName(eventName1);
                        if (eventText && *eventText)
                            scheduledWU->setEventText(eventText);

                        try 
                        {
                            SCMStringBuffer buf;
                            CWUWrapper wu(wuid, context);
                            scheduledWU->setJobName(wu->getJobName(buf).str());
                        }
                        catch (IException *e)
                        {
                            e->Release();
                        }

                        results.append(*scheduledWU.getLink());
                    }
                    iter->nextWuid();
                }
                iter->nextEventText();
            }
            iter->nextEventName();
        }
    }

    return;
}

bool CWsWorkunitsEx::onWUShowScheduled(IEspContext &context, IEspWUShowScheduledRequest & req, IEspWUShowScheduledResponse & resp)
{
    try
    {

        DBGLOG("Started CWsWorkunitsEx::onWUShowScheduled\n");

        StringBuffer Query;
        Query.append("PageFrom=Scheduler");

        char *eventName = NULL;
        if(req.getEventName() && *req.getEventName())
        {
            eventName = (char *)req.getEventName(); 
            resp.setEventName(eventName);
            Query.appendf("&EventName=%s", eventName);
        }

        if(req.getPushEventName() && *req.getPushEventName())
        {
            resp.setPushEventName(req.getPushEventName());
        }

        if(req.getPushEventText() && *req.getPushEventText())
        {
            resp.setPushEventText(req.getPushEventText());
        }

        IArrayOf<IEspScheduledWU> results;
        const char *clusterName = req.getCluster();
        if(clusterName && *clusterName)
        {
            getScheduledWUs(context, clusterName, eventName, results);
            Query.appendf("&Cluster=%s", clusterName);
        }

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
        Owned<IPropertyTree> root = &environment->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

        unsigned i = 0;
        IArrayOf<IEspServerInfo> clusters;
        Owned<IPropertyTreeIterator> clusterIterator = root->getElements("Software/Topology/Cluster");
        if (clusterIterator->first()) 
        {
            do {
                IPropertyTree &cluster = clusterIterator->query();
                const char *clusterName0 = cluster.queryProp("@name");
                if (!clusterName0 || !*clusterName0)
                    continue;

                if(clusterName && *clusterName)
                {
                    if (!stricmp(clusterName, clusterName0))
                    {
                        resp.setClusterSelected(i+1);
                    }
                }
                else
                {
                    getScheduledWUs(context, clusterName, eventName, results);
                }

                Owned<IEspServerInfo> server = createServerInfo("");
                server->setName(clusterName0);
                clusters.append(*server.getLink());
                i++;
            } while (clusterIterator->next());
        }

        if (clusters.length() > 0)
            resp.setClusters(clusters);
        if (results.length() > 0)
            resp.setWorkunits(results);
        if (Query.length() > 0)
            resp.setQuery(Query);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUExport(IEspContext &context, IEspWUExportRequest &req, IEspWUExportResponse &resp)
{
    try
    {
    WUList wl(context,req.getOwner(),req.getState(),req.getCluster(),req.getStartDate(),req.getEndDate(),req.getECL(),req.getJobname());

        StringBuffer WorkUnitStr;
        WorkUnitStr.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Workunits>");
        for(WUList::iterator it=wl.begin();it!=wl.end();it++)
        {
            CWUWrapper wu(it->c_str(), context);
            SCMStringBuffer xml;
            exportWorkUnitToXML(wu, xml);
            WorkUnitStr.append(xml.str());
        }
        WorkUnitStr.append("</Workunits>");

        MemoryBuffer temp;
        temp.setBuffer(WorkUnitStr.length(),(void*)WorkUnitStr.str());
        resp.setExportData(temp);
        resp.setExportData_mimetype(HTTP_TYPE_TEXT_XML);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsWorkunitsEx::checkFileContent(IEspContext &context, const char * logicalName)
{
    StringBuffer username;
    context.getUserID(username);

    if(username.length() < 0)
        return true; //??TBD
    
    Owned<IUserDescriptor> userdesc(createUserDescriptor());
    userdesc->set(username.str(), context.queryPassword());

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
    if (!df)
        return false;

    bool blocked;
    if (df->isCompressed(&blocked) && !blocked)
        return false;

    IPropertyTree & properties = df->queryProperties();
    const char * format = properties.queryProp("@format");
    if (format && (stricmp(format,"csv")==0 || memicmp(format, "utf", 3) == 0))
    {
        return true;
    }
    const char * recordEcl = properties.queryProp("ECL");
    if (!recordEcl)
        return false;

    MultiErrorReceiver errs;
    Owned<IHqlExpression> ret = ::parseQuery(recordEcl, &errs);
    return errs.errCount() == 0;
}

void CWsWorkunitsEx::getResult(IEspContext &context, IConstWUResult &r,IArrayOf<IEspECLResult>& results, const char* wuid, bool SuppressSchemas)
{
    SCMStringBuffer x;
    r.getResultName(x);

    SCMStringBuffer filename;
    r.getResultLogicalName(filename);

    StringBuffer value, link;
    if (r.getResultStatus() == ResultStatusUndefined)
    {
        value<<"[undefined]";
    }
    else if (r.isResultScalar())
    {
        try
        {
            SCMStringBuffer x;
            r.getResultXml(x);

            Owned<IPropertyTree> props = createPTreeFromXMLString(x.str(), ipt_caseInsensitive);
            
            //StringBuffer buf;
            //toXML(props, buf);
            //if (buf.length() > 0)
            //{
            //  DBGLOG("getResult returns:%s", buf.str());
            //}
            IPropertyTree *val = props->queryPropTree("Row/*");
            if(val)
            {
                value<<val->queryProp(NULL);
            }
            else
            {            
                StringBuffer user, password;
                context.getUserID(user);
                context.getPassword(password);
                Owned<IResultSetFactory> resultSetFactory;
                if (context.querySecManager())
                    resultSetFactory.setown(getSecResultSetFactory(*context.querySecManager(), *context.queryUser()));
                else
                    resultSetFactory.setown(getResultSetFactory(user, password));
                Owned<INewResultSet> result;
                result.setown(resultSetFactory->createNewResultSet(&r, wuid));
                Owned<IResultSetCursor> cursor(result->createCursor());
                cursor->first();

                if (cursor->getIsAll(0))
                {
                    value << "<All/>"; 
                }
                else
                {
                    Owned<IResultSetCursor> childCursor = cursor->getChildren(0);
                    if (childCursor)
                    {
                        ForEach(*childCursor)
                        {
                            StringBuffer out;
                            StringBufferAdaptor adaptor(out);
                            childCursor->getDisplayText(adaptor, 0);
                            if (value.length() < 1)
                                value << "[";
                            else
                                value << ", ";
                            value << "'" << out.str() << "'";
                        }

                        if (value.length() > 0)
                            value << "]";
                    }
                }
            }
        }
        catch(...) 
        {
            value<<"[value not available]";
        }
    }
    else
    {
        value<<"["<<r.getResultTotalRowCount()<<" rows]";
        if(r.getResultSequence()>=0)
        {
            if(filename.length())
            {
                StringBuffer username;
                context.getUserID(username);

                Owned<IUserDescriptor> userdesc(createUserDescriptor());
                userdesc->set(username.str(), context.queryPassword());

                Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(filename.str(), userdesc);
                if(df && df->queryProperties().hasProp("ECL"))
                {
                    link<<r.getResultSequence();
                }
            }
            else
            {
                link<<r.getResultSequence();
            }
        }
    }

    Owned<IEspECLResult> result= createECLResult("","");

    if (SuppressSchemas)
    {
        IArrayOf<IEspECLSchemaItem> schemas;
        
        if (getResultSchemas(r, schemas))
        {
            result->setECLSchemas(schemas);
        }
    }

    if (filename.length() && !checkFileContent(context, filename.str()))
        result->setShowFileContent(false);

    result->setName(x.str());
    result->setLink(link.str());
    result->setSequence(r.getResultSequence());
    result->setValue(value.str());
    result->setFileName(filename.str());
    result->setIsSupplied(r.getResultStatus() == ResultStatusSupplied);
    result->setTotal(r.getResultTotalRowCount());
    results.append(*result.getLink());
}

void CWsWorkunitsEx::getHelpFiles(double version, IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers)
{
    if (!query)
        return;

    SCMStringBuffer name, Ip, description;
    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        IConstWUAssociatedFile & cur = iter->query();
        if (cur.getType() != type)
            continue;

        cur.getName(name);
      Owned<IEspECLHelpFile> h= createECLHelpFile("","");
      h->setName(name.str());
        switch (type)
        {
            case FileTypeCpp:
                h->setType("cpp");
                break;
            case FileTypeDll:
                h->setType("dll");
                break;
            default:
                h->setType("res");
                break;
        }

        if (version > 1.31)
        {
            cur.getIp(Ip);
            h->setIPAddress(Ip.str());
            Ip.clear();

            cur.getDescription(description);
            if ((description.length() < 1) && (name.length() > 0))
            {
                const char* desc = pathTail(name.str());
                if (desc && *desc)
                    description.set(desc);
            }

            if (description.length() < 1) 
                description.set("Help File");

            h->setDescription(description.str());
            description.clear();
        }
      helpers.append(*h.getLink());
      name.clear();
    }
}

void CWsWorkunitsEx::getInfo(IEspContext &context,const char* wuid0,IEspECLWorkunit *info, bool bTruncateEclTo64k, bool IncludeExceptions, bool IncludeGraphs, bool IncludeSourceFiles, bool IncludeResults, bool IncludeVariables, bool IncludeTimers, bool IncludeDebugValues, bool IncludeApplicationValues, bool IncludeWorkflows, bool SuppressResultSchemas, StringArray *resultViews)
{
    StringBuffer wuidStr = wuid0;
    wuidStr.trim();
    const char* wuid = wuidStr.str();

    CWUWrapper wu(wuid, context);
    ensureWorkunitAccess(context, *wu, SecAccess_Read);

    bool helpersException = false;
    bool graphsException = false;
    bool sourceFilesException = false;
    bool resultsException = false;
    bool variablesException = false;
    bool timersException = false;
    bool debugValuesException = false;
    bool applicationValuesException = false;
    bool workflowsException = false;

    SecAccessFlags accessFlag = getWorkunitAccess(context, *wu);

    SCMStringBuffer buf;
    info->setWuid(wu->getWuid(buf).str());
    info->setStateID(wu->getState());
    info->setState(wu->getStateDesc(buf).str());
    info->setStateEx(wu->getStateEx(buf).str());

    if ((wu->getState() == WUStateScheduled) && wu->aborting())
    {
        info->setStateID(WUStateAborting);
        info->setState("aborting");
    }

    double version = context.getClientVersion();
    if (version > 1.00)
    {       
        info->setArchived(false);
    }
    if (version > 1.32)
    {
        info->setActionEx(wu->getActionEx(buf).str());
    }
    info->setProtected(wu->isProtected() ? 1 : 0);

    SCMStringBuffer roxieClusterName;
    Owned<IConstWURoxieQueryInfo> roxieQueryInfo = wu->getRoxieQueryInfo();

    if (roxieQueryInfo)
        roxieQueryInfo->getRoxieClusterName(roxieClusterName);

    StringBuffer ClusterName = wu->getClusterName(buf).str();
    info->setCluster(ClusterName.str());
    if (version > 1.06 && roxieClusterName.length() > 0)
    {
        info->setRoxieCluster(roxieClusterName.str());
    }

    CTpWrapper dummy;
    if (version > 1.23 && ClusterName.length() > 0)
    {
        int ClusterFlag = 0;
        StringBuffer clusterProcess = ClusterName;
        IArrayOf<IEspTpLogicalCluster> clusters;
        dummy.getTargetClusterList(clusters, eqThorCluster, NULL);
        ForEachItemIn(k, clusters)
        {
            IEspTpLogicalCluster& cluster = clusters.item(k);
            const char* thorCluster = cluster.getName();
            if (thorCluster && !strcmp(thorCluster, ClusterName.str()))
            {
                ClusterFlag = 1; //Thor Cluster
                const char* thorClusterProcess = cluster.getProcess();
                if (thorClusterProcess && *thorClusterProcess)
                    clusterProcess.clear().append(thorClusterProcess);
                break;
            }
        }

        if (ClusterFlag < 1)
        {
            Owned<IStringIterator> debugs(&wu->getDebugValues());
            ForEach(*debugs)
            {
            SCMStringBuffer name, val;
            debugs->str(name);
                if (!stricmp(name.str(), "targetclustertype"))
                {
                    wu->getDebugValue(name.str(),val);
                    if (!stricmp(val.str(), "roxie"))
                        ClusterFlag = 2;
                    break;
                }
            }
        }
        info->setClusterFlag(ClusterFlag);

        if (version > 1.29 && (ClusterFlag == 1))
        {
            bool bThorLCR = dummy.getClusterLCR(eqThorCluster, clusterProcess.str());
            info->setThorLCR(bThorLCR);
            if (wu->isPausing())
            {
                info->setIsPausing(true);
            }
        }
    }

    info->setJobname(wu->getJobName(buf).str());
    info->setOwner(wu->getUser(buf).str());
    info->setPriorityClass(wu->getPriority());
    info->setPriorityLevel(wu->getPriorityLevel());
    info->setScope(wu->getWuScope(buf).str());
    info->setSnapshot(wu->getSnapshot(buf).str());
    info->setResultLimit(wu->getResultLimit());

    CScheduleDateTime dt;
    wu->getTimeScheduled(dt);
    if(dt.isValid())
        info->setDateTimeScheduled(dt.getString(buf).str());

    wu->getDebugValue("description",buf);
    info->setDescription(buf.str());

    WUExceptions errors(*wu);

    if (version > 1.16)
    {       
        unsigned sectionCount = wu->getGraphCount();
        info->setGraphCount(sectionCount);

        sectionCount = wu->getSourceFileCount();
        info->setSourceFileCount(sectionCount);

        sectionCount = wu->getVariableCount();
        info->setVariableCount(sectionCount);

        sectionCount = wu->getTimerCount();
        info->setTimerCount(sectionCount);

        sectionCount = wu->getSourceFileCount();
        info->setSourceFileCount(sectionCount);

        sectionCount = wu->getApplicationValueCount();
        info->setApplicationValueCount(sectionCount);

        info->setHasDebugValue(wu->hasDebugValue("__calculated__complexity__"));

        info->setErrorCount(errors.ErrCount());
        info->setWarningCount(errors.WrnCount());
        info->setInfoCount(errors.InfCount());
    }
    if (version > 1.21)
    {
        SCMStringBuffer params;
        info->setXmlParams(wu->getXmlParams(params).str());
    }

    if (IncludeExceptions)
    {
        info->setExceptions(errors);
    }

    bool eventCountRemaining = false;
    bool eventCountUnlimited = false;

    try
    {
        IArrayOf<IConstECLWorkflow> workflows;

        Owned<IConstWorkflowItemIterator> it = wu->getWorkflowItems();
        if (it)
        {
            ForEach(*it)
            {
                IConstWorkflowItem *r = it->query();
                if (r)
                {
                    IWorkflowEvent *wfevent = r->getScheduleEvent();
                    if (wfevent)
                    {
                        Owned<IEspECLWorkflow> g= createECLWorkflow("","");
                        StringBuffer id;
                        id.appendf("%d", r->queryWfid());
                        g->setWFID(id.str()); 
                        g->setEventName(wfevent->queryName()); 
                        g->setEventText(wfevent->queryText());
                        if (r->hasScheduleCount())
                        {
                            int count = r->queryScheduleCount();
                            int countRemaining = r->queryScheduleCountRemaining();
                            if (countRemaining > 0)
                                eventCountRemaining = true;
                            g->setCount(count); 
                            g->setCountRemaining(countRemaining); 
                        }
                        else
                        {
                            eventCountUnlimited = true;
                        }
                        workflows.append(*g.getLink());
                    }
                }
            }


            if (workflows.length() > 0)
                info->setWorkflows(workflows);
            workflows.kill();
        }
    }
    catch(IException* e)
    {   
        workflowsException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    if (version > 1.00)
    {       
        info->setEventSchedule(0);
        if (info->getState() && !stricmp(info->getState(), "wait"))
        {
            info->setEventSchedule(2); //Can deschedule
        }
        else if (eventCountUnlimited || eventCountRemaining)
        {
            info->setEventSchedule(1); //Can reschedule
        }
    }

    try
    {
        unsigned sectionCount = 0;
        IArrayOf<IEspECLResult> results;

        Owned<IConstWUResultIterator> it = &wu->getResults();
        ForEach(*it)
        {
            IConstWUResult &r = it->query();
            if(r.getResultSequence()>=0) 
            {
                getResult(context, r, results);

                sectionCount++;
            }
        }

        if (version > 1.16)
            info->setResultCount(sectionCount);

        if (IncludeResults && (results.length() > 0))   
        {
            info->setResults(results);
        }
            
        results.kill();
    }
    catch(IException* e)
    {   
        resultsException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    try
    {
        if (IncludeVariables)
        {
            IArrayOf<IEspECLResult> results;
            Owned<IConstWUResultIterator> vars = &wu->getVariables();
            ForEach(*vars)
            {
                getResult(context,vars->query(), results, NULL, SuppressResultSchemas);
            }
            info->setVariables(results);
            results.kill();
        }
    }
    catch(IException* e)
    {   
        variablesException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    try
    {
    if (IncludeSourceFiles)
    {
        Owned<IUserDescriptor> userdesc;
        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();
        userdesc.setown(createUserDescriptor());
        userdesc->set(username.str(), passwd);

        IArrayOf<IEspECLSourceFile> files;
        if (version < 1.27) 
        {
            Owned<IPropertyTreeIterator> f=&wu->getFilesReadIterator();
            ForEach(*f)
            {
                IPropertyTree &query = f->query();
                const char *clusterName = query.queryProp("@cluster");
                const char *fileName = query.queryProp("@name");
                int fileCount = query.getPropInt("@useCount");

                Owned<IEspECLSourceFile> file= createECLSourceFile("","");
                if(clusterName && *clusterName)
                {
                    file->setFileCluster(clusterName);
                }

                if (version > 1.11)
                {
                    Owned<IPropertyTreeIterator> filetrees= query.getElements("Subfile");
                    if (filetrees->first()) 
                        file->setIsSuperFile(true);
                }

                if (fileName && *fileName)
                {
                    file->setName(fileName);
                }
                
                file->setCount(fileCount);

                files.append(*file.getLink());
            }
        }
        else
        {
            StringArray fileNames;

            Owned<IPropertyTreeIterator> f=&wu->getFilesReadIterator();
            ForEach(*f)
            {
                IPropertyTree &query = f->query();
                const char *clusterName = query.queryProp("@cluster");
                const char *fileName = query.queryProp("@name");
                int fileCount = query.getPropInt("@useCount");

                bool bFound = false;
                if (fileName && *fileName && (fileNames.length() > 0))
                {
                    for (unsigned i = 0; i < fileNames.length(); i++ )
                    {
                        const char *fileName0 = fileNames.item(i);
                        if (!stricmp(fileName, fileName0))
                        {
                            bFound = true;
                            break;
                        }
                    }
                }

                if (bFound)
                    continue;

                Owned<IEspECLSourceFile> file= createECLSourceFile("","");
                if(clusterName && *clusterName)
                {
                    file->setFileCluster(clusterName);
                }

                if (fileName && *fileName)
                {
                    file->setName(fileName);
                }
                
                file->setCount(fileCount);

                Owned<IPropertyTreeIterator> filetrees= query.getElements("Subfile");
                if (filetrees->first()) 
                {
                    file->setIsSuperFile(true);
                    addSubFiles(filetrees, file,    fileNames);
                }

                files.append(*file.getLink());
            }
        }

        info->setSourceFiles(files);
    }
    }
    catch(IException* e)
    {   
        sourceFilesException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    try
    {
        if (IncludeGraphs)  
        {
            SCMStringBuffer runningGraph;
            WUGraphIDType id;
            bool running=wu.isRunning() && wu->getRunningGraph(runningGraph,id);

            IArrayOf<IEspECLGraph> graphs;
            Owned<IConstWUGraphIterator> it = &wu->getGraphs(GraphTypeAny);
            ForEach(*it)
            {
                IConstWUGraph &graph = it->query();
                if(!graph.isValid())
                    continue;

                SCMStringBuffer name, label, type;
                graph.getName(name);
                graph.getLabel(label);

                graph.getTypeName(type);

                Owned<IEspECLGraph> g= createECLGraph("","");
                g->setName(name.str()); 
                g->setLabel(label.str()); 
                g->setType(type.str());
                if(running && strcmp(name.str(),runningGraph.str())==0)
                {
                    g->setRunning(true);
                    g->setRunningId(id);
                }

                Owned<IConstWUGraphProgress> progress = wu->getGraphProgress(name.str());
                if (progress)
                {
                    WUGraphState graphstate= progress->queryGraphState();
                    if (graphstate == WUGraphComplete)
                        g->setComplete(true);
                    if (version > 1.13 && graphstate == WUGraphFailed)
                    {       
                        g->setFailed(true);
                    }
                }

                graphs.append(*g.getLink());
            }
            info->setGraphs(graphs);
        }
    }
    catch(IException* e)
    {   
        graphsException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

     if (version > 1.01)
     {      
        info->setHaveSubGraphTimings(false);
        StringBuffer path;
        path.append("/WorkUnits/").append(wuid);
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, 5*60*1000);  
        if (!conn) 
        {
            DBGLOG("Could not connect to SDS");
            return;
        }
        IPropertyTree *wu = conn->queryRoot();
        Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
        StringBuffer name;
        IArrayOf<IConstECLTimingData> timingdatarray;
        ForEach(*iter) 
        {
            if (iter->query().getProp("@name",name.clear())) 
            {
                if ((name.length()>11)&&(memcmp("Graph graph",name.str(),11)==0)) 
                {
                    unsigned gn;
                    const char *s = getNum(name.str(),gn);
                    unsigned sn;
                    s = getNum(s,sn);
                    if (gn&&sn) 
                    {
                        info->setHaveSubGraphTimings(true);
                        break;
                    }
                }
            }
        }
     }

    try
    {
        if (IncludeTimers)
        {
            IArrayOf<IEspECLTimer> timers;
            Owned<IStringIterator> it = &wu->getTimers();
            ForEach(*it)
            {
                SCMStringBuffer name;
                it->str(name);
                SCMStringBuffer value;
                unsigned count = wu->getTimerCount(name.str(), NULL);
                unsigned duration = wu->getTimerDuration(name.str(), NULL);
                StringBuffer fd;
                formatDuration(fd, duration);
                for (unsigned i = 0; i < name.length(); i++)
                 if (name.s.charAt(i)=='_') 
                     name.s.setCharAt(i, ' ');

                Owned<IEspECLTimer> t= createECLTimer("","");
                t->setName(name.str());
                t->setValue(fd.str());
                t->setCount(count);

                if (version > 1.19)
                {       
                    StringBuffer graphName;
                    unsigned subGraphNum;
                    unsigned __int64 subId;
                    if (parseGraphTimerLabel(name.str(), graphName, subGraphNum, subId))
                    {
                        if (graphName.length() > 0)
                        {
                            t->setGraphName(graphName.str());
                        }
                        if (subId > 0)
                        {
                            t->setSubGraphId(subId);
                        }
                    }
                }

                timers.append(*t.getLink());
            }
            info->setTimers(timers);
        }
    }
    catch(IException* e)
    {   
        timersException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    try
   {
        Owned <IConstWUQuery> query = wu->getQuery();
        if(query)
        {
            SCMStringBuffer qname;
            query->getQueryShortText(qname);
            if(qname.length())
            {
                if(bTruncateEclTo64k && (qname.length() > 64000))
                    qname.setLen(qname.str(), 64000);

                IEspECLQuery* q=&info->updateQuery();
                q->setText(qname.str());
            }

            if (version > 1.30)
            {
                SCMStringBuffer qText;
                query->getQueryText(qText);
                if ((qText.length() > 0) && isArchiveQuery(qText.str()))
                {       
                    info->setHasArchiveQuery(true);
                }
            }

            IArrayOf<IEspECLHelpFile> helpers;
            getHelpFiles(version, query, FileTypeCpp, helpers);
            getHelpFiles(version, query, FileTypeDll, helpers);
            getHelpFiles(version, query, FileTypeResText, helpers);

               SCMStringBuffer name;
                for (int i0 = 1; i0 < MAXTHORS; i0++)
                {
                    StringBuffer fileType;
                    if (i0 < 2)
                        fileType.append(File_ThorLog);
                    else
                        fileType.appendf("%s%d", File_ThorLog, i0);
                    wu->getDebugValue(fileType.str(), name);
                    if(name.length() < 1)
                        break;

                    Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                    h->setName(name.str());
                    h->setType(fileType.str());
                    helpers.append(*h.getLink());
                    name.clear();
                }

                wu->getDebugValue("EclAgentLog", name);
                if(name.length())
                {
                    Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                    h->setName(name.str());
                    h->setType("EclAgentLog");
                    helpers.append(*h.getLink());
                    name.clear();
                }
             info->setHelpers(helpers);
        }
    }
     catch(IException* e)
     {  
         helpersException = true;

         StringBuffer eMsg;
         e->errorMessage(eMsg);
         ERRLOG("%s", eMsg.str()); //log original exception
         e->Release();
     }

    try
    {
        if (IncludeDebugValues)
        {
        IArrayOf<IEspDebugValue> dv;
        Owned<IStringIterator> debugs(&wu->getDebugValues());
        ForEach(*debugs)
        {
            SCMStringBuffer name, val;
            debugs->str(name);
            wu->getDebugValue(name.str(),val);

            Owned<IEspDebugValue> t= createDebugValue("","");
            t->setName(name.str());
            t->setValue(val.str());
            dv.append(*t.getLink());
        }

        info->setDebugValues(dv);
        }
    }
    catch(IException* e)
    {   
        debugValuesException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    try
    {
        if (IncludeApplicationValues)
        {
        IArrayOf<IEspApplicationValue> av;
        Owned<IConstWUAppValueIterator> app(&wu->getApplicationValues());
        ForEach(*app)
        {
            IConstWUAppValue& val=app->query();
            SCMStringBuffer buf;

            Owned<IEspApplicationValue> t= createApplicationValue("","");
            t->setApplication(val.getApplication(buf).str());
            t->setValue(val.getValue(buf).str());
            t->setName(val.getName(buf).str());
            t->setValue(val.getValue(buf).str());
            av.append(*t.getLink());

        }

        info->setApplicationValues(av);
        }
    }
    catch(IException* e)
    {   
        applicationValuesException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

    if (version > 1.04)
    {
        StringArray allowedClusters;
        SCMStringBuffer val;
      wu->getAllowedClusters(val);
        if (val.length() > 0)
        {
            const char* ptr = val.str();
            while(*ptr != '\0')
            {
                StringBuffer onesub;
                while(*ptr != '\0' && *ptr != ',')
                {
                    onesub.append((char)(*ptr));
                    ptr++;
                }
                if(onesub.length() > 0)
                    allowedClusters.append(onesub.str());
                if(*ptr != '\0')
                    ptr++;
            }
        }
        if (allowedClusters.length() > 0)
        {
            info->setAllowedClusters(allowedClusters);
        }
    }

    if (version > 1.22)
    {
        info->setAccessFlag(accessFlag);
    }

    if (version > 1.28)
    {
        const char* msg = "This section cannot be dispayed due to an exception.";
        if (helpersException)
            info->setHelpersDesc(msg);
        if (graphsException)
            info->setGraphsDesc(msg);
        if (sourceFilesException)
            info->setSourceFilesDesc(msg);
        if (resultsException)
            info->setResultsDesc(msg);
        if (variablesException)
            info->setVariablesDesc(msg);
        if (timersException)
            info->setTimersDesc(msg);
        if (debugValuesException)
            info->setDebugValuesDesc(msg);
        if (applicationValuesException)
            info->setApplicationValuesDesc(msg);
        if (workflowsException)
            info->setWorkflowsDesc(msg);
    }
    try
    {
        if (resultViews)
        {
            Owned<IWuWebView> wv = createWuWebView(*wu, NULL, NULL, false);
            wv->getResultViewNames(*resultViews);
        }
    }
    catch(IException* e)
    {
        applicationValuesException = true;

        StringBuffer eMsg;
        e->errorMessage(eMsg);
        ERRLOG("%s", eMsg.str()); //log original exception
        e->Release();
    }

}

void CWsWorkunitsEx::addSubFiles(IPropertyTreeIterator* f, IEspECLSourceFile* eclSuperFile, StringArray& fileNames)
{
    IArrayOf<IEspECLSourceFile> files;

    ForEach(*f)
    {
        IPropertyTree &query = f->query();

        const char *clusterName = query.queryProp("@cluster");
        const char *fileName = query.queryProp("@name");
        int fileCount = query.getPropInt("@useCount");

        bool bFound = false;
        if (fileName && *fileName && (fileNames.length() > 0))
        {
            for (unsigned i = 0; i < fileNames.length(); i++ )
            {
                const char *fileName0 = fileNames.item(i);
                if (!stricmp(fileName, fileName0))
                {
                    bFound = true;
                    break;
                }
            }
        }

        if (bFound)
            continue;

        Owned<IEspECLSourceFile> file= createECLSourceFile("","");
        if(clusterName && *clusterName)
        {
            file->setFileCluster(clusterName);
        }

        if (fileName && *fileName)
        {
            file->setName(fileName);
            fileNames.append(fileName);
        }
        
        file->setCount(fileCount);

        Owned<IPropertyTreeIterator> filetrees= query.getElements("Subfile");
        if (filetrees->first()) 
        {
            file->setIsSuperFile(true);
            addSubFiles(filetrees, file, fileNames);
        }

        files.append(*file.getLink());
    }

    eclSuperFile->setECLSourceFiles(files);

    return;
}


#if 0 //not use for now
int CWsWorkunitsEx::addSubFiles(IArrayOf<IEspECLSourceFile>& allFiles, int k, IEspECLSourceFile* eclSuperFile)
{
    IArrayOf<IEspECLSourceFile> files;

    int i = 0;
    int subs = eclSuperFile->getSubs();

    while (i < subs)
    {
        k++;

        IEspECLSourceFile& file0 = allFiles.item(k);

        const char* name0 = file0.getName();
        int isSuperFile = file0.getIsSuperFile();
        int numOfSubFiles = file0.getSubs();

        Owned<IEspECLSourceFile> file= createECLSourceFile("","");
        if(file0.getFileCluster() && *file0.getFileCluster())
        {
            file->setFileCluster(file0.getFileCluster());
        }

        if (name0 && *name0)
        {
            file->setName(name0);
        }
        
        if (isSuperFile > 0)
        {
            file->setIsSuperFile(true);
            file->setSubs(numOfSubFiles);
        }

        if(!file0.getCount_isNull())
        {
            file->setCount(file0.getCount());
        }

        if (isSuperFile && numOfSubFiles)
             k = addSubFiles(allFiles, k, file);

        files.append(*file.getLink());

        i++;
    }

    eclSuperFile->setECLSourceFiles(files);

    return k;
}

void CWsWorkunitsEx::getSubFiles(IUserDescriptor* userdesc, const char *fileName, IEspECLSourceFile* eclSourceFile0)
{
    StringArray subfiles;
    Owned<IDistributedSuperFile> superfile = queryDistributedFileDirectory().lookupSuperFile(fileName, userdesc);
    if (!superfile)
    {
        DBGLOG("onWUDetails(): Could not find super file %s", fileName);
        return;
    }

    Owned<IDistributedFileIterator> iter=superfile->getSubFileIterator();
    ForEach(*iter) 
    {
        StringBuffer name;
        iter->getName(name);
        subfiles.append(name.str());
    }

    if (subfiles.length() < 1)
        return;

    StringArray superfiles, nonsuperfiles;

    for(int i = 0; i < subfiles.length(); i++)
    {
        const char* subfile = subfiles.item(i);
        if (!subfile || !*subfile)
            continue;

        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(subfile, userdesc);
        if(!df)
        {
            DBGLOG("onWUDetails(): Could not find file %s", subfile);
            nonsuperfiles.append(subfile);
            continue;
        }

        if(df->querySuperFile())
        {
            superfiles.append(subfile);
        }
        else
        {
            nonsuperfiles.append(subfile);
        }
    }

    IArrayOf<IEspECLSourceFile> files;

    ForEachItemIn(k, superfiles)
    {
        const char* file0 = superfiles.item(k);

        Owned<IEspECLSourceFile> file= createECLSourceFile("","");
        file->setName(file0);

        getSubFiles(userdesc, file0, file);

        if (stricmp("production_watch_thor::monitoring::nco::nco_u22056::qa::sth_address", file0)==0)
            DBGLOG("Super-file:<%s> -- subfile:<%s>", fileName, file0);
        
        files.append(*file.getLink());
    }

    ForEachItemIn(k1, nonsuperfiles)
    {
        const char* file0 = nonsuperfiles.item(k1);

        Owned<IEspECLSourceFile> file= createECLSourceFile("","");
        file->setName(file0);

        DBGLOG("Super-file:<%s> -- subfile:<%s>", fileName, file0);
        if (stricmp("production_watch_thor::monitoring::nco::nco_u22050::qa::sth_address", file0)==0)
            DBGLOG("Super-file:<%s> -- subfile:<%s>", fileName, file0);

        files.append(*file.getLink());
    }

    eclSourceFile0->setECLSourceFiles(files);
    return;
}

//check if the file exists in the list
bool CWsWorkunitsEx::checkFileInECLSourceFile(const char* file, IConstECLSourceFile& eclfile)
{
    const char* name0 = eclfile.getName();
    if (name0 && (stricmp(name0, file)==0))
        return true;
        
    bool bFound = false;

    IArrayOf<IConstECLSourceFile>& files =  eclfile.getECLSourceFiles();
    ForEachItemIn(k, files)
    {
        IConstECLSourceFile& file0 = files.item(k);
        if (checkFileInECLSourceFile(file, file0))
        {
            bFound = true;
            break;
        }
    }

    return bFound;
}

//check if the file exists in the list
bool CWsWorkunitsEx::checkFileInECLSourceFiles(const char* file, IArrayOf<IEspECLSourceFile>& eclfiles)
{
    bool bFound = false;

    ForEachItemIn(k, eclfiles)
    {
        IEspECLSourceFile& file0 = eclfiles.item(k);
        if (checkFileInECLSourceFile(file, file0))
        {
            bFound = true;
            break;
        }
    }

    return bFound;
}
#endif

bool CWsWorkunitsEx::getInfoFromSasha(IEspContext &context, const char *sashaServer, const char* wuid,IEspECLWorkunit *info)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->addId(wuid);
    cmd->setAction(SCA_GET);
    SocketEndpoint ep(sashaServer, DEFAULT_SASHA_PORT);
    Owned<INode> node = createINode(ep);
    if (!cmd->send(node,1*60*1000)) 
    {
        DBGLOG("Could not connect to Sasha server at %s",sashaServer);
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to archive server at %s.",sashaServer);
    }
    if (cmd->numIds()==0) 
    {
        DBGLOG("Could not read archived %s",wuid);
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot read workunit %s.",wuid);
    }
    
    unsigned num = cmd->numResults();
    if (num < 1)
        return false;

    StringBuffer res;
    cmd->getResult(0,res);
    if(res.length() < 1)
        return false;
    
    //DBGLOG("Result: %s", res.str());
    Owned<IPropertyTree> wu = createPTreeFromXMLString(res.str());
    if (!wu)
        return false;

    const char * state = wu->queryProp("@state");
    const char * scope = wu->queryProp("@scope");
    const char * submitID = wu->queryProp("@submitID");
    const char * cluster = wu->queryProp("@clusterName");
    const char * jobName = wu->queryProp("@jobName");
    //const char * priorityClass = wu->queryProp("@priorityClass");
    const char * protectedWU = wu->queryProp("@protected");
    const char * description = wu->queryProp("Debug/description");

    ensureArchivedWorkunitAccess(context, submitID, SecAccess_Read);

    double version = context.getClientVersion();
    info->setWuid(wuid);
    if (version > 1.00)
    {       
        info->setArchived(true);
    }
    if (state && *state)
        info->setState(state);
    if (cluster && *cluster)
        info->setCluster(cluster);
    if (submitID && *submitID)
        info->setOwner(submitID);
    if (scope && *scope)
        info->setScope(scope);
    if (jobName && *jobName)
        info->setJobname(jobName);
    if (description && *description)
        info->setDescription(description);
    //info->setPriorityClass(priorityClass);
    if (protectedWU && stricmp(protectedWU, "0"))
        info->setProtected(true);
    else
        info->setProtected(false);

    IPropertyTree* queryTree = wu->queryPropTree("Query");
    if (queryTree)
    {
      IEspECLQuery* q=&info->updateQuery();
        const char * queryText = queryTree->queryProp("Text");
      if(queryText && *queryText)
      {
            q->setText(queryText);
      }

        /*StringBuffer xpath;
        xpath.clear().append("Associated/File");
        Owned<IPropertyTreeIterator> filetrees= queryTree->getElements(xpath.str());
        if (filetrees->first()) 
        {
            do 
            {
                IPropertyTree &filetree = filetrees->query();
                const char* filename = filetree.queryProp("@filename");
                const char* filetype = filetree.queryProp("@type");
                if (!stricmp(filetype, "cpp"))
                {
                    q->setCpp(filename);
                }
                else if (!stricmp(filetype, "dll"))
                {
                    q->setDll(filename);
                }
                else if (!stricmp(filetype, "log"))
                {
                    q->setThorLog(filename);
                }
            } while (filetrees->next());
        } */
    }

    return true;
}

void CWsWorkunitsEx::getArchivedWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp)
{
    const char *wuid = req.getWuid();
    if (!wuid || !*wuid)
    {
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not specified.");
    }

    //Access should be checked inside getInfoFromSasha().
   //ensureWorkunitAccess(context, wuid, SecAccess_Read); 

    bool bWUFound = false;
    StringBuffer sashaAddress;
    IArrayOf<IConstTpSashaServer> sashaservers;
    CTpWrapper dummy;
    dummy.getTpSashaServers(sashaservers);  
    ForEachItemIn(i, sashaservers)
    {
        IConstTpSashaServer& sashaserver = sashaservers.item(i);
        IArrayOf<IConstTpMachine> &sashaservermachine = sashaserver.getTpMachines();
        sashaAddress.clear().append(sashaservermachine.item(0).getNetaddress());
        if (sashaAddress.length() > 0)
        {
            bWUFound = getInfoFromSasha(context, sashaAddress.str(), wuid, &resp.updateWorkunit());
            resp.setAutoRefresh(WUDETAILS_REFRESH_MINS);
            resp.setCanCompile(false);
        }

        if (bWUFound)
            break;
    }
    if (!bWUFound)
    {
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find workunit %s.",wuid);
    }

    return;
}

bool CWsWorkunitsEx::onWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp)
{
    try
    {
        const char *type = req.getType();

        if (type && *type && !stricmp(type, "archived workunits"))
        {
            getArchivedWUInfo(context, req, resp);
        }
        else
        {
            try
            {
                getInfo(context, req.getWuid(), &resp.updateWorkunit(), req.getTruncateEclTo64k(), req.getIncludeExceptions(), req.getIncludeGraphs(), req.getIncludeSourceFiles(), req.getIncludeResults(), req.getIncludeVariables(), req.getIncludeTimers(), req.getIncludeDebugValues(), req.getIncludeApplicationValues(), req.getIncludeWorkflows(), req.getSuppressResultSchemas(), req.getIncludeResultsViewNames() ? &resp.getResultViews() : NULL);

                int n = resp.getWorkunit().getStateID();
                if (n == WUStateCompiling || n == WUStateCompiled || n == WUStateScheduled || n == WUStateSubmitted 
                    || n == WUStateRunning || n == WUStateAborting || n == WUStateWait || n == WUStateUploadingFiles || n == WUStateDebugPaused || n == WUStateDebugRunning )
                {
                    resp.setAutoRefresh(WUDETAILS_REFRESH_MINS);
                }
                else if (n == WUStateBlocked)
                {
                    resp.setAutoRefresh(WUDETAILS_REFRESH_MINS*5);
                }

                StringBuffer usr;
                context.getUserID(usr).str();
                resp.setCanCompile(usr.length()>0);

                double version = context.getClientVersion();
                const char* slaveIP  = req.getThorSlaveIP();
                if (version > 1.24 && slaveIP && *slaveIP)
                    resp.setThorSlaveIP(slaveIP);
            }
            catch (IException *e)
            {
                StringBuffer errMsg;
                e->errorMessage(errMsg);
                if (errMsg.length() < 23)
                    throw e;

                char str[24];
                memset(str, 0, 24);
                errMsg.getChars(0, 23, str);
                if (stricmp(str, "Could not open workunit"))
                    throw e;
                getArchivedWUInfo(context, req, resp);
            }
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CWsWorkunitsEx::onWUInfoDetails(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp)
{
    return onWUInfo(context, req, resp);
}

bool CWsWorkunitsEx::onWUGraphInfo(IEspContext &context,IEspWUGraphInfoRequest &req, IEspWUGraphInfoResponse &resp)
{
    try
    {
        CWUWrapper wu(req.getWuid(), context);

        ensureWorkunitAccess(context, *wu, SecAccess_Read);

        resp.setWuid(req.getWuid());
        resp.setName(req.getName());
        resp.setRunning(wu.isRunning());
        if (req.getGID() && *req.getGID())
            resp.setGID(req.getGID());
        if(!req.getBatchWU_isNull())
            resp.setBatchWU(req.getBatchWU());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp)
{
    try
    {
        resp.setName(req.getName());
        resp.setGraphName(req.getGraphName());
        resp.setGraphType("eclwatch");

        double version = context.getClientVersion();
        if (version > 1.19)
        {
            resp.setSubGraphId(req.getSubGraphId());
        }
        if (version > 1.20)
        {
            resp.setSubGraphOnly(req.getSubGraphOnly());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGVCGraphInfo(IEspContext &context,IEspWUGVCGraphInfoRequest &req, IEspWUGVCGraphInfoResponse &resp)
{
    try
    {
        CWUWrapper wu(req.getWuid(), context);

        ensureWorkunitAccess(context, *wu, SecAccess_Read);

        resp.setWuid(req.getWuid());
        resp.setName(req.getName());
        resp.setRunning(wu.isRunning());
        if (req.getGID() && *req.getGID())
            resp.setGID(req.getGID());
        if(!req.getBatchWU_isNull())
            resp.setBatchWU(req.getBatchWU());

        //Owned <IConstWUGraph> graph = wu->getGraph(req.getName());
        //Owned <IPropertyTree> pXgmmlGraph = graph->getXGMMLTree(true); // merge in graph progress information
        //StringBuffer xml;
        //toXML(pXgmmlGraph, xml);

        //if (xml.length() > 0)
        //{
            //StringBuffer str;
            //encodeUtf8XML(xml.str(), str, 0);

        StringBuffer xml1;
        xml1.append("<Control>");
        xml1.append("<Endpoint>");
        xml1.append("<Query id=\"Gordon.Extractor.0\">");
        double version = context.getClientVersion();
        if (version > 1.17)
        {
            xml1.appendf("<Graph id=\"%s\">", req.getName());
            char buf[23];
            numtostr(buf, req.getSubgraphId_isNull() ? 0 : req.getSubgraphId());
            xml1.appendf("<Subgraph>%s</Subgraph>", buf);
        }
        else
        {
            xml1.appendf("<Graph id=\"%s\">", req.getName());
        }
        //xml1.append("<xgmml>");
        //xml1.append(str);
        //xml1.append("</xgmml>");

        xml1.append("</Graph>");
        xml1.append("</Query>");
        xml1.append("</Endpoint>");
        xml1.append("</Control>");

        resp.setTheGraph(xml1);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    
    return true;
}

void CWsWorkunitsEx::getGJSGraph(IEspContext &context, const char *wuid, const char *graphname, IProperties* params, StringBuffer &script)
{

   CWUWrapper wu(wuid, context);

   ensureWorkunitAccess(context, *wu, SecAccess_Read);

   int state = wu->getState();

   SCMStringBuffer runningGraph;
   WUGraphIDType id;

   bool hasRunningGraph = wu->getRunningGraph(runningGraph, id);


    Owned <IConstWUGraph> graph = wu->getGraph(graphname);
    Owned <IPropertyTree> pXgmmlGraph = graph->getXGMMLTree(true); // merge in graph progress information

    StringBuffer xml;
    toXML(pXgmmlGraph, xml);

    if (xml.length() > 0)
    {
        StringBuffer str;
        encodeUtf8XML(xml.str(), str, 0);

        // Append Graph Information and xgmml

        StringBuffer xml1;
        xml1.append("<Control>");
        xml1.append("<Endpoint>");
        xml1.appendf("<Graph>%s</Graph>", graphname);
        xml1.append("<isrunning>").append(wu.isRunning()).append("</isrunning>");
        if (hasRunningGraph)
        {
            xml1.appendf("<currentgraph>%s</currentgraph>", runningGraph.str());
            char buf[23];
            numtostr(buf, id);
            xml1.appendf("<currentgraphnode>%s</currentgraphnode>", buf);

        }
        char buf[23];
        numtostr(buf, state);

        xml1.appendf("<wustate>%s</wustate>", buf);

        // Append List of Graphs and each of their states.

        xml1.append("<wugraphs>");

        IArrayOf<IEspECLGraph> graphs;
        Owned<IConstWUGraphIterator> it = &wu->getGraphs(GraphTypeAny);
        ForEach(*it)
        {
            IConstWUGraph &graph = it->query();
            if(!graph.isValid())
                continue;

            SCMStringBuffer name, label;
            graph.getName(name);
            graph.getLabel(label);

            xml1.appendf("<wugraph id=\"%s\" name=\"%s\">", name.str(), label.str());

            Owned<IConstWUGraphProgress> progress = wu->getGraphProgress(name.str());
            
            if (progress)
            {

                WUGraphState graphstate = progress->queryGraphState();
                int gstate = static_cast<int>(graphstate);
                numtostr(buf, gstate);
                xml1.appendf("<graphstate>%s</graphstate>", buf);

            }
            

            xml1.append("</wugraph>");
        }

        xml1.append("</wugraphs>");
        
        xml1.append("<xgmml>");
        xml1.append(str);
        xml1.append("</xgmml>");
        xml1.append("</Endpoint>");
        xml1.append("</Control>");

        xsltTransform(xml1.str(), m_GraphUpdateGvcXSLT, params, script);

    }


    return;
}

bool CWsWorkunitsEx::onWUGraphTiming(IEspContext &context, IEspWUGraphTimingRequest &req, IEspWUGraphTimingResponse &resp)
{
    try
    {
        DBGLOG("CWsWorkunitsEx::onWUGraphTiming WUID=%s",req.getWuid());
        CWUWrapper wu(req.getWuid(), context);
        ensureWorkunitAccess(context, *wu, SecAccess_Read);

        const char* wuid = req.getWuid();
        if (wuid && *wuid)
        {
            StringBuffer path;
            path.append("/WorkUnits/").append(wuid);
            Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), 0, 5*60*1000);  
            if (!conn) 
            {
                DBGLOG("Could not connect to SDS");
                throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI,"Cannot connect to dali server.");
            }
            IPropertyTree *wu = conn->queryRoot();
            Owned<IPropertyTreeIterator> iter = wu->getElements("Timings/Timing");
            StringBuffer name;
            IArrayOf<IConstECLTimingData> timingdatarray;
            ForEach(*iter) 
            {
                if (iter->query().getProp("@name",name.clear())) 
                {
                    if ((name.length()>11)&&(memcmp("Graph graph",name.str(),11)==0)) 
                    {
                        unsigned gn;
                        const char *s = getNum(name.str(),gn);
                        unsigned sn;
                        s = getNum(s,sn);
                        if (gn&&sn) 
                        {
                            const char *gs = strchr(name.str(),'(');
                            unsigned gid = 0;
                            if (gs)
                                getNum(gs+1,gid);
                            unsigned time = iter->query().getPropInt("@duration");

                            Owned<IEspECLTimingData> g = createECLTimingData();
                            g->setName(name.str());
                            g->setGraphNum(gn);
                            g->setSubGraphNum(sn);
                            g->setGID(gid);
                            g->setMS(time);
                            g->setMin(time/60000);
                            timingdatarray.append(*g.getLink());
                        }
                    }
                }
            }
            resp.updateWorkunit().setWuid(wuid);
              resp.updateWorkunit().setTimingData(timingdatarray);
        }
        else
        {
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No workunit ID defined.");
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

SecAccessFlags CWsWorkunitsEx::getWorkunitAccess(IEspContext& context, IConstWorkUnit& wu)
{
   StringBuffer user;
    context.getUserID(user);

    SCMStringBuffer owner;
    wu.getUser(owner);

    const char* feature_url;
    if (!stricmp(user, owner.str()))
        feature_url = OWN_WU_URL;
    else
        feature_url = OTHERS_WU_URL;

    if (!context.validateFeatureAccess(feature_url, SecAccess_Read, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");

   SecAccessFlags accessFlag;
    if (!context.authorizeFeature(feature_url, accessFlag))
        accessFlag = SecAccess_None;

    return accessFlag;
}

void CWsWorkunitsEx::ensureWorkunitAccess(IEspContext& context, IConstWorkUnit& wu, 
                                          SecAccessFlags minAccess)
{
    StringBuffer user;
    context.getUserID(user);

    SCMStringBuffer owner;
    wu.getUser(owner);

    const char* feature_url;
    if (!stricmp(user, owner.str()))
        feature_url = OWN_WU_URL;
    else
        feature_url = OTHERS_WU_URL;

    if (!context.validateFeatureAccess(feature_url, minAccess, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
}

void CWsWorkunitsEx::ensureArchivedWorkunitAccess(IEspContext& context, const char *owner, 
                                          SecAccessFlags minAccess)
{
    StringBuffer user;
    context.getUserID(user);

    const char* feature_url;
    if (!stricmp(user, owner))
        feature_url = OWN_WU_URL;
    else
        feature_url = OTHERS_WU_URL;

    if (!context.validateFeatureAccess(feature_url, minAccess, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
}

void CWsWorkunitsEx::resubmitWU(IEspContext& context, const char* wuid, const char* _cluster, IStringVal& newWuid, bool forceRecompile)
{
    SCMStringBuffer cluster, snapshot;
    {
        NewWorkunit wu(context);
        CWUWrapper cwu(wuid, context);
        wu->getWuid(newWuid);
        queryExtendedWU(wu)->copyWorkUnit(cwu);
        wu->setUser(context.queryUserId());
    
        SCMStringBuffer token;
        createToken(newWuid.str(), context.queryUserId(), context.queryPassword(), token);
        wu->setSecurityToken(token.str());
    }

    submitWU(context, newWuid.str(), cluster.str(), snapshot.str(), 0, forceRecompile, true);
}

void CWsWorkunitsEx::submitWU(IEspContext& context, const char* wuid, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow)
{
    StringBuffer user, password;
    context.getUserID(user);
    context.getPassword(password);

    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
    Owned<IWorkUnit> wu(factory->updateWorkUnit(wuid));
    if(!wu.get())
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

    ensureWorkunitAccess(context, *wu.get(), SecAccess_Write);
    switch(wu->getState())
    {
        case WUStateRunning:
        case WUStateDebugPaused:
        case WUStateDebugRunning:
        case WUStateCompiling:
        case WUStateAborting:
        case WUStateBlocked:
        {
            SCMStringBuffer state;
            wu->getStateDesc(state);
            throw MakeStringException(ECLWATCH_CANNOT_SUBMIT_WORKUNIT,"Cannot submit the workunit. Workunit state is '%s'.",state.str());
        }
    }
    bool isRoxie = (cluster && *cluster && isRoxieCluster(cluster));

    wu->clearExceptions();
    if(cluster && *cluster)
        wu->setClusterName(cluster);    

    if(snapshot && *snapshot)
        wu->setSnapshot(snapshot);
    wu->setState(WUStateSubmitted);
    if(maxruntime)
        wu->setDebugValueInt("maxRunTime",maxruntime,true);
    if (resetWorkflow)
    {
        wu->resetWorkflow();
        if (!compile && !isRoxie)
            wu->schedule();
    }
    wu->commit();
    wu.clear();
    
    if (isRoxie)
    {
        try
        {
            CRoxieQuery roxieQuery;
            wu.setown(factory->updateWorkUnit(wuid));
            roxieQuery.action = wu->getAction();
            wu->setAction(WUActionCompile);
            wu->commit();
            wu.clear();

            if (context.querySecManager())
            {
                secSubmitWorkUnit(wuid, *context.querySecManager(), *context.queryUser());
            }

            roxieQuery.wuid.set(wuid);
            StringBuffer component_cluster;

            getTargetClusterComponentName(cluster, eqRoxieCluster, component_cluster);
            roxieQuery.roxieClusterName.append(component_cluster);

            roxieQuery.clusterName.append(cluster);

            roxieQuery.roxieTimeOut = roxieQueryRoxieTimeOut; //Hardcoded for now
            roxieQuery.wuTimeOut = roxieQueryWUTimeOut;

            StringBuffer netAddress;
            getClusterConfig("RoxieCluster", component_cluster, "RoxieServerProcess[1]", netAddress);
            roxieQuery.ip = netAddress.str();

            submitQueryWU(wuid, context, roxieQuery, m_allowNewRoxieOnDemandQuery);
        }
        catch(IException *e)
        {
            Owned<IWorkUnit> wu0(factory->updateWorkUnit(wuid));
            if(!wu0.get())
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

            wu0->setState(WUStateFailed);
            wu0->commit();
            wu0.clear();

            throw e;
        }
    }
    else if (!compile)
    {
        runWorkUnit(wuid);
    }
    else if (context.querySecManager())
    {
        secSubmitWorkUnit(wuid, *context.querySecManager(), *context.queryUser());
    }
    else
    {
        try
        {
            submitWorkUnit(wuid, user,password);
        }
        catch(IException *e)
        {
           Owned<IWorkUnit> wu0(factory->updateWorkUnit(wuid));
            if(!wu0.get())
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

            wu0->setState(WUStateFailed);
            wu0->commit();
            wu0.clear();

            throw e;
        }
    }

   AccessSuccess(context,"Submitted %s",wuid);
}

void CWsWorkunitsEx::scheduleWU(IEspContext& context, const char* wuid, const char* cluster, const char* when, const char* snapshot, int maxruntime)
{
    StringBuffer user, password;
    context.getUserID(user);
    context.getPassword(password);

    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
    Owned<IWorkUnit> wu(factory->updateWorkUnit(wuid));

    ensureWorkunitAccess(context, *wu.get(), SecAccess_Write);
    switch(wu->getState())
    {
        case WUStateDebugPaused:
        case WUStateDebugRunning:
        case WUStateRunning:
        case WUStateAborting:
        case WUStateBlocked:
        {
            SCMStringBuffer state;
            wu->getStateDesc(state);
            throw MakeStringException(ECLWATCH_CANNOT_SCHEDULE_WORKUNIT,"Cannot schedule the workunit. Workunit state is '%s'.",state.str());
        }
    }

    wu->clearExceptions();
    if(cluster && *cluster)
        wu->setClusterName(cluster);    

    if(when && *when)
    {
        CScheduleDateTime dt;
        dt.setString(when);
        wu->setTimeScheduled(dt);
    }

    if(snapshot && *snapshot)
        wu->setSnapshot(snapshot);

    wu->setState(WUStateScheduled);
    if(maxruntime)
        wu->setDebugValueInt("maxRunTime",maxruntime,true);
    SCMStringBuffer token;
    createToken(wuid, user.str(), password.str(), token);
    wu->setSecurityToken(token.str());
    wu->commit();
    wu.clear();
    
    AccessSuccess(context,"Scheduled %s",wuid);
}

//MORE: Should go into a shared sasha access library
IPropertyTree * getArchivedWorkUnitProperties(const char * wuid)
{
    if (!wuid || !*wuid)
        return NULL;
    
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;

    Owned<IPropertyTreeIterator> services = conn->queryRoot()->getElements("Software/SashaServerProcess");
    ForEach(*services)
    {
        IPropertyTree& serviceTree = services->query();
        Owned<IPropertyTreeIterator> instances = serviceTree.getElements("Instance");
        if (!instances->first()) 
            continue;

        IPropertyTree& instanceNode = instances->query();
        const char* computerName = instanceNode.queryProp("@computer");
        if (!computerName || !*computerName)
            continue;

        StringBuffer xpath;
        xpath.appendf("Hardware/Computer[@name=\"%s\"]", computerName);
        IPropertyTree *pMachine = conn->queryRoot()->queryPropTree(xpath.str());
        if (!pMachine)
            continue;

        const char* ipAddress = pMachine->queryProp("@netAddress");
        if (!ipAddress || !*ipAddress)
            continue;
        
        StringBuffer sashaAddress;
        if (!stricmp(ipAddress, "."))
        {
            IpAddress ipaddr = queryHostIP();
            ipaddr.getIpText(sashaAddress);
        }
        else 
        {
            sashaAddress.append(ipAddress);
        }

        SocketEndpoint ep(sashaAddress.str(), DEFAULT_SASHA_PORT);
        Owned<INode> node = createINode(ep);
        Owned<ISashaCommand> cmd = createSashaCommand();
        cmd->addId(wuid);
        cmd->setAction(SCA_GET);
        if (cmd->send(node,1*60*1000) && (cmd->numIds() > 0) && (cmd->numResults() > 0)) 
        {
            StringBuffer res;
            cmd->getResult(0,res);
            if(res.length() > 0)
            {
                Owned<IPropertyTree> wuTree = createPTreeFromXMLString(res.str());
                return wuTree.getLink();
            }
            break;
        }
    }

    return NULL;
}

void CWsWorkunitsEx::getWorkunitCluster(IEspContext &context, const char* wuid, StringBuffer& cluster, bool checkArchiveWUs)
{
    if (!wuid || !*wuid)
        return;

    try
    {
        CWUWrapper wu(wuid, context);
        if (wu)
        {   
            SCMStringBuffer cluster0;
            cluster.append(wu->getClusterName(cluster0).str());
        }
    }
    catch (IException *e)
    {
        if (checkArchiveWUs && e->errorCode() == ECLWATCH_CANNOT_OPEN_WORKUNIT)
        {
            Owned<IPropertyTree> wuProps = getArchivedWorkUnitProperties(wuid);
            if (wuProps)
            {
                const char * cluster0 = wuProps->queryProp("@clusterName");
                if (cluster0 && *cluster0)
                {
                    cluster.append(cluster0);
                }
            }
        }
    }

    return;
}

void CWsWorkunitsEx::getWorkunitXml(IEspContext &context, const char* wuid, const char* plainText, MemoryBuffer& buf)
{
    SCMStringBuffer x;
    CWUWrapper wu(wuid, context);
    exportWorkUnitToXML(wu, x);
   
    const char* header="<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";
    if (plainText && (!stricmp(plainText, "yes")))
        header="<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    buf.append(strlen(header),header);
    buf.append(x.length(),x.str());
}

void CWsWorkunitsEx::getWorkunitCpp(IEspContext &context, const char* cppname, const char* description, const char* ipAddress, MemoryBuffer& buf)
{
    if (!description || !*description)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");

    if (!ipAddress || !*ipAddress)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File location not specified.");

    if(!cppname || !*cppname)
        throw MakeStringException(ECLWATCH_INVALID_FILE_NAME, "File path not specified.");

    RemoteFilename rfn;
    rfn.setRemotePath(cppname);
    SocketEndpoint ep(ipAddress);
    rfn.setIp(ep);

    Owned<IFile> cppfile = createIFile(rfn);
    if (!cppfile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open %s.", description);

    OwnedIFileIO rIO = cppfile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);

    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    if (!ios)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);

    StringBuffer line;
    bool eof = false;
    while (!eof)
    {
        line.clear();
        loop
        {
            char c;
            size32_t numRead = ios->read(1, &c);
            if (!numRead)
            {
                eof = true;
                break;
            }
            line.append(c);
            if (c=='\n')
                break;
        }

        buf.append(line.length(), line.str());
        if (buf.length() > 640000)
            break;
    }

    return;
}

void CWsWorkunitsEx::getWorkunitResTxt(IEspContext &context, const char* wuid,MemoryBuffer& buf)
{
    CQuery query(wuid, context);
    SCMStringBuffer resname;
    query->getQueryResTxtName(resname);
    queryDllServer().getDll(resname.str(), buf);
}

void CWsWorkunitsEx::getWorkunitArchiveQuery(IEspContext &context, const char* wuid,MemoryBuffer& buf)
{
    SCMStringBuffer queryText;
    CWUWrapper lw(wuid, context);
    Owned<IConstWUQuery> query=lw->getQuery();

    query->getQueryText(queryText);
    if ((queryText.length() < 1) || !isArchiveQuery(queryText.str()))
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Archive Query not found for workunit %s.", wuid);
    
    buf.append(queryText.length(), queryText.str());
    return;
}

void CWsWorkunitsEx::getWorkunitDll(IEspContext &context, const char* wuid,MemoryBuffer& buf)
{
    CQuery query(wuid, context);
    SCMStringBuffer dllname;
    query->getQueryDllName(dllname);
    queryDllServer().getDll(dllname.str(), buf);
}

void CWsWorkunitsEx::getWorkunitResults(IEspContext &context, const char* wuid, unsigned index,__int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf)
{
    CWUWrapper wu(wuid, context);

    //Owned<IConstWUResult> wuResult = wu->getResultBySequence(index);
    //wuResult->getResultName(resname);
    Owned<IConstWUResult> wuResult;
    if (resname.length() > 0)
    {
        wuResult.setown(wu->getResultByName(resname.str()));    
        if (!wuResult)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");
    }
    else
    {
        wuResult.setown(wu->getResultBySequence(index));
        if (!wuResult)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");
        wuResult->getResultName(resname);
    }

    StringBuffer user, password;
    context.getUserID(user);
    context.getPassword(password);

    Owned<IResultSetFactory> resultSetFactory;
    if (context.querySecManager())
        resultSetFactory.setown(getSecResultSetFactory(*context.querySecManager(), *context.queryUser()));
    else
        resultSetFactory.setown(getResultSetFactory(user, password));

    SCMStringBuffer logicalName;
    wuResult->getResultLogicalName(logicalName);
    Owned<INewResultSet> result;
    if (!logicalName.length())
    {
        result.setown(resultSetFactory->createNewResultSet(wuResult, wuid));
    }
    else
    {
        SCMStringBuffer cluster;
        //MORE This is wrong cluster
        result.setown(resultSetFactory->createNewFileResultSet(logicalName.str(), wu->getClusterName(cluster).str()));
    }
    getResultView(result,start,count,total,resname,raw,buf);
}

void CWsWorkunitsEx::getFileResults(IEspContext &context, const char* logicalName, const char* cluster,__int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf)
{
    StringBuffer user, password;
    context.getUserID(user);
    context.getPassword(password);

    Owned<IResultSetFactory> resultSetFactory;
    if (context.querySecManager())
        resultSetFactory.setown(getSecResultSetFactory(*context.querySecManager(), *context.queryUser()));
    else
        resultSetFactory.setown(getResultSetFactory(user, password));

    Owned<INewResultSet> result(resultSetFactory->createNewFileResultSet(logicalName, cluster));
    getResultView(result,start,count,total,resname,raw,buf);
}

void CWsWorkunitsEx::getResultView(INewResultSet* result, __int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf)
{
    if(result)
    { 
        const IResultSetMetaData &meta = result->getMetaData();

        Owned<IResultSetCursor> cursor(result->createCursor());
        total=result->getNumRows();

        if(raw)
        {
            count=getResultBin(buf,result,start,count);
        }
        else 
        {
            struct MemoryBuffer2IStringVal : public CInterface, implements IStringVal
            {
                MemoryBuffer2IStringVal(MemoryBuffer & _buffer) : buffer(_buffer) {}
                IMPLEMENT_IINTERFACE;

                virtual const char * str() const { UNIMPLEMENTED;  }
                virtual void set(const char *val) { buffer.append(strlen(val),val); }
                virtual void clear() { } // clearing when appending does nothing
                virtual void setLen(const char *val, unsigned length) { buffer.append(length, val); }
                virtual unsigned length() const { return buffer.length(); };
                MemoryBuffer & buffer;
            } adaptor(buf);

            count=getResultXml(adaptor,result,resname.str(),start,count,"myschema");
        }
    }
}

void CWsWorkunitsEx::getWorkunitEclAgentLog(IEspContext &context, const char *wuid, MemoryBuffer& buf)
{
    SCMStringBuffer logname;
    unsigned pid;
    {
        CWUWrapper wu(wuid, context);
        wu->getDebugValue("EclAgentLog", logname);
        pid = wu->getAgentPID();
    }

    if(logname.length() == 0)
        throw MakeStringException(ECLWATCH_ECLAGENT_LOG_NOT_FOUND,"EclAgent log file not available for workunit %s.", wuid);
    Owned<IFile> rFile = createIFile(logname.str());
    if(!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if(!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logname.str());

    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    StringBuffer line;
    bool eof = false;
    StringBuffer pidstr;
    pidstr.appendf(" %5d ", pid);

    bool wuidFound = false;
    char const * pidchars = pidstr.str();
    while(!eof)
    {
        line.clear();
        loop
        {
            char c;
            size32_t numRead = ios->read(1, &c);
            if (!numRead)
            {
                eof = true;
                break;
            }
            line.append(c);
            if (c=='\n')
                break;
        }

        //Retain all rows that match a unique program instance - by retaining all rows that match a pid
        if(strstr(line.str(), pidchars))
        {
            //Check if this is a new instance using line sequence number
            if (strncmp(line.str(), "00000000", 8) == 0)
            {
                if (wuidFound) //If the correct instance has been found, return that instance before the next instance.
                    break;

                //The last instance is not a correct instance. Clean the buf in order to start a new instance. 
                buf.clear();
            }

            //If we spot the workunit id anywhere in the tacing for this pid then assume it is the correct instance.
            if(!wuidFound && strstr(line.str(), wuid))
                wuidFound = true;

            buf.append(line.length(), line.str());
        }
    }
}

void CWsWorkunitsEx::getWorkunitThorLog(IEspContext &context, const char* type, const char *wuid,MemoryBuffer& buf)
{
    SCMStringBuffer logname;
    CWUWrapper(wuid, context)->getDebugValue(type, logname);

    Owned<IFile> rFile = createIFile(logname.str());
    if (!rFile)
    throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",logname.str());
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logname.str());

    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    StringBuffer line;
    bool eof = false;
    bool include = false;
    StringBuffer startwuid;
    StringBuffer endwuid;
    startwuid.appendf("Started wuid=%s", wuid);
    endwuid.appendf("Finished wuid=%s", wuid);
    const char *sw = startwuid.str();
    const char *ew = endwuid.str();
    while (!eof)
    {
        line.clear();
        loop
        {
            char c;
            size32_t numRead = ios->read(1, &c);
            if (!numRead)
            {
                eof = true;
                break;
            }
            line.append(c);
            if (c=='\n')
                break;
        }
        if (strstr(line.str(), sw))
            include = true;
        if (include)
            buf.append(line.length(), line.str());
        if (strstr(line.str(), ew))
            include = false;
    }
}

void CWsWorkunitsEx::getWorkunitThorSlaveLog(IEspContext &context, const char *wuid, const char *slaveip,MemoryBuffer& buf)
{
   if (!wuid || !*wuid)
      throw MakeStringException(ECLWATCH_INVALID_INPUT,"Workunit ID not specified.");

   if (!slaveip || !*slaveip)
      throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave IP not specified.");

    //StringBuffer logname;
   //getWorkunitThorLog(wuid, logname);

    SCMStringBuffer logname;
   CWUWrapper(wuid, context)->getDebugValue(File_ThorLog, logname);

   StringBuffer logdir;
   splitDirTail(logname.str(),logdir);

   RemoteFilename rfn;
   rfn.setRemotePath(logdir.str());
   SocketEndpoint ep(slaveip);
   rfn.setIp(ep);

   Owned<IFile> dir = createIFile(rfn);
   Owned<IDirectoryIterator> diriter = dir->directoryFiles("*.log");
   if (!diriter->first())
      throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find Thor slave log file %s.", logdir.str());
   
    Linked<IFile> logfile = &diriter->query();
   diriter.clear();
   dir.clear();
   // logfile is now the file to load

    OwnedIFileIO rIO = logfile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logdir.str());

    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    StringBuffer line;
    bool eof = false;
#if 0
    bool include = false;
    StringBuffer startwuid;
    StringBuffer endwuid;
    startwuid.appendf("Started wuid=%s", wuid);
    endwuid.appendf("Finished wuid=%s", wuid);
    const char *sw = startwuid.str();
    const char *ew = endwuid.str();
    while (!eof)
    {
        line.clear();
        loop
        {
            char c;
            size32_t numRead = ios->read(1, &c);
            if (!numRead)
            {
                eof = true;
                break;
            }
            line.append(c);
            if (c=='\n')
                break;
        }
        if (strstr(line.str(), sw))
            include = true;
        if (include)
            buf.append(line);
        if (strstr(line.str(), ew))
            include = false;
    }
#else
    while (!eof)
    {
        line.clear();
        loop
        {
            char c;
            size32_t numRead = ios->read(1, &c);
            if (!numRead)
            {
                eof = true;
                break;
            }
            line.append(c);
            if (c=='\n')
                break;
        }
        buf.append(line.length(), line.str());
        if (buf.length() > 640000)
            break;
    }
#endif
}

bool CWsWorkunitsEx::onWUUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp)
{
    try
    {
        Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());

        if(req.getProtected() != req.getProtectedOrig()) 
        {
            CWUWrapper wu(req.getWuid(), context);
            ensureWorkunitAccess(context, *wu, SecAccess_Write);

            Owned<IConstWorkUnit> lw = factory->openWorkUnit(req.getWuid(), false);
            lw->protect(req.getProtected());
            lw.clear();
        }

        if ((req.getState() == WUStateRunning)||(req.getState() == WUStateDebugPaused)||(req.getState() == WUStateDebugRunning))
        {
            CWUWrapper wu(req.getWuid(), context);
            ensureWorkunitAccess(context, *wu, SecAccess_Read);

            getInfo(context,req.getWuid(),&resp.updateWorkunit(),true);
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(req.getWuid()).str());
            AccessSuccess(context,"Updated %s",req.getWuid());
            return true;
        }

        Owned<IWorkUnit> lw = factory->updateWorkUnit(req.getWuid());
        if(!lw.get())
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",req.getWuid());

        ensureWorkunitAccess(context, *lw.get(), SecAccess_Write);

        if(!req.getState_isNull() && (req.getStateOrig_isNull() || req.getState() != req.getStateOrig()))
        {
            CWUWrapper wu(req.getWuid(), context);
            if (!req.getStateOrig_isNull() && wu->getState() != req.getStateOrig())
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot update workunit %s because its state has been changed internally. Please refresh the page and try again.",req.getWuid());

            WUState state = (WUState) req.getState();
            if(state < WUStateSize)
                lw->setState(state);
        }

        if(req.getJobname() && *req.getJobname())
        {
            if (!req.getJobnameOrig() || stricmp(req.getJobnameOrig(), req.getJobname()))
            {
                StringBuffer buf(req.getJobname());
                lw->setJobName(buf.trim().str());
            }
        }
        else if (req.getJobnameOrig() && *req.getJobnameOrig())
        {
            lw->setJobName("");
        }


        if(req.getDescription() && *req.getDescription())
        {
            if (!req.getDescriptionOrig() || stricmp(req.getDescriptionOrig(), req.getDescription()))
            {
                StringBuffer buf(req.getDescription());
                lw->setDebugValue("description",buf.trim().str(),true);
            }
        }
        else if (req.getDescriptionOrig() && *req.getDescriptionOrig())
        {
        lw->setDebugValue("description",NULL,true);
        }

        double version = context.getClientVersion();
        if (version > 1.04) 
        { 
            const char *cluster = req.getClusterSelection(); 
            const char *clusterOrig = req.getClusterOrig(); 
            if (cluster && *cluster && clusterOrig && *clusterOrig && stricmp(clusterOrig, cluster)) 
            { 
                if (req.getState() == WUStateBlocked) 
                { 
                    switchWorkunitQueue(lw, req.getWuid(), cluster); 
                } 
                else if ((req.getState() != WUStateSubmitted) && (req.getState() != WUStateRunning) && (req.getState() != WUStateDebugPaused) && (req.getState() != WUStateDebugRunning)) 
                { 
                    lw->setClusterName(cluster); 
                } 
            } 
        }
        const char *xmlParams = req.getXmlParams();
        if (xmlParams && *xmlParams)
            lw->setXmlParams(xmlParams);

        if(req.getQueryText() && *req.getQueryText())
        {
        Owned<IWUQuery> query=lw->updateQuery();
        query->setQueryText(req.getQueryText());
        }

        if(!req.getResultLimit_isNull())
        {
        lw->setResultLimit(req.getResultLimit());
        }

        if(!req.getAction_isNull())
        {
            WUAction action = (WUAction) req.getAction();
            if(action < WUActionSize)
                lw->setAction(action);   
        }

        if(!req.getPriorityClass_isNull())
        {
            WUPriorityClass priority = (WUPriorityClass) req.getPriorityClass();
            if(priority<PriorityClassSize)
                lw->setPriority(priority);
        }

        if(!req.getPriorityLevel_isNull())
        {
        lw->setPriorityLevel(req.getPriorityLevel());
        }
        
        if(req.getScope() && *req.getScope() && (!req.getScopeOrig() || stricmp(req.getScopeOrig(), req.getScope())))
        {
            lw->setWuScope(req.getScope());
        }

        for(int di=0; di<req.getDebugValues().length();di++)
        {   
        IConstDebugValue& item=req.getDebugValues().item(di);
        if(item.getName() && *item.getName())
        {
            lw->setDebugValue(item.getName(),item.getValue(),true);
        }
        }

        for(int ai=0; ai<req.getApplicationValues().length();ai++)
        {   
        IConstApplicationValue& item=req.getApplicationValues().item(ai);
        if(item.getApplication() && *item.getApplication() && item.getName() && *item.getName())
        {
            lw->setApplicationValue(item.getApplication(),item.getName(),item.getValue(),true);
        }
        }

        lw.clear();

        getInfo(context,req.getWuid(),&resp.updateWorkunit(),true);

        StringBuffer thorSlaveIP;
        if (version > 1.24 && req.getThorSlaveIP() && *req.getThorSlaveIP())
            thorSlaveIP = req.getThorSlaveIP();

        if (thorSlaveIP.length() > 0)
        {
            StringBuffer url;
            url.appendf("/WsWorkunits/WUInfo?Wuid=%s&ThorSlaveIP=%s", req.getWuid(), thorSlaveIP.str());
            resp.setRedirectUrl(url.str());
        }
        else
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(req.getWuid()).str());

        AccessSuccess(context,"Updated %s",req.getWuid());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUPushEvent(IEspContext &context, IEspWUPushEventRequest &req, IEspWUPushEventResponse &resp)
{
    try
    {
        const char *name = req.getEventName();
        const char *text = req.getEventText();
        const char *target = NULL;
        if (name && *name && text && *text)
        {
            Owned<IScheduleEventPusher> pusher(getScheduleEventPusher()); 
            pusher->push(name, text, target); 
            
            StringBuffer redirect;
            redirect<<"/WsWorkunits/WUShowScheduled";
            redirect << "?PushEventName=" << name;
            redirect << "&PushEventText=" << text;
            resp.setRedirectUrl(redirect.str());
            return true;
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsWorkunitsEx::onWUAction(IEspContext &context, IEspWUActionRequest &req, IEspWUActionResponse &resp)
{
    try
    {
        if (!req.getActionType() || !*req.getActionType())
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Action not defined.");

        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());

        int action=-1;
        if(stricmp(req.getActionType(),"Delete")==0)
            action=ActionDelete;
        else if(stricmp(req.getActionType(),"Abort")==0)
            action=ActionAbort;
        else if(stricmp(req.getActionType(),"PauseNow")==0)
            action=ActionPauseNow;
        else if(stricmp(req.getActionType(),"Pause")==0)
            action=ActionPause;
        else if(stricmp(req.getActionType(),"Resume")==0)
            action=ActionResume;
        else if(stricmp(req.getActionType(),"Protect")==0)
        {
            action=ActionProtect;
            params->setProp("Protect", true);
        }
        else if(stricmp(req.getActionType(),"Unprotect")==0)
        {
            action=ActionProtect;
            params->setProp("Protect", false);
        }
        else if(stricmp(req.getActionType(),"Restore")==0)
            action=ActionRestore;
        else if(stricmp(req.getActionType(),"Reschedule")==0)
            action=ActionEventSchedule;
        else if(stricmp(req.getActionType(),"Deschedule")==0)
            action=ActionEventDeschedule;
        else if(stricmp(req.getActionType(),"SetToFailed")==0)
        {
            params->setProp("State",4);
            action=ActionChangeState;
        }

        bool bAllSuccess = true;
        IArrayOf<IConstWUActionResult> results;
        if(action>=0)
        {
             bAllSuccess = doAction(context,req.getWuids(), action, params, &results);
        }

        if (bAllSuccess && action!=ActionDelete)
        {
            StringBuffer redirect;
            if(req.getPageFrom() && !stricmp(req.getPageFrom(), "scheduler"))
            {
                redirect<<"/WsWorkunits/WUShowScheduled";
                const char *event = req.getEventName();
                const char *server = req.getEventServer();
                if (server && *server)
                {
                    redirect << "?Cluster=" << server;
                    if (event && *event)
                    {
                        redirect << "&EventName=" << event;
                    }
                }
                else if (event && *event)
                {
                    redirect<<"?EventName=" << event;
                }
            }
            else if(req.getPageFrom() && !stricmp(req.getPageFrom(), "wuid") && action!=ActionDelete)
            {
                redirect<<"/WsWorkunits/WUInfo?Wuid="<<req.getWuids().item(0);
            }
            else
            {
                redirect<<"/WsWorkunits/WUQuery?";
                if(req.getPageSize() && *req.getPageSize())
                    redirect<<"PageSize="<<req.getPageSize();
                if(req.getCurrentPage() && *req.getCurrentPage())
                    redirect<<"&PageStartFrom="<<req.getCurrentPage();
                if(req.getSortby() && *req.getSortby())
                    redirect<<"&Sortby="<<req.getSortby();
                if(req.getDescending())
                    redirect<<"&Descending="<<req.getDescending();
                //if(*req.getFilters())
                //  redirect<<"&"<<req.getFilters();
                if (req.getState() && *req.getState())
                    redirect<<"&State="<<req.getState();
                if (req.getCluster() && *req.getCluster())
                    redirect<<"&Cluster="<<req.getCluster();
                if (req.getOwner() && *req.getOwner())
                    redirect<<"&Owner="<<req.getOwner();
                if (req.getStartDate() && *req.getStartDate())
                    redirect<<"&StartDatee="<<req.getStartDate();
                if (req.getEndDate() && *req.getEndDate())
                    redirect<<"&EndDate="<<req.getEndDate();
                if (req.getECL() && *req.getECL())
                    redirect<<"&ECL="<<req.getECL();
                if (req.getJobname() && *req.getJobname())
                    redirect<<"&Jobname="<<req.getJobname();
            }
            resp.setRedirectUrl(redirect.str());
        }
        else
        {
            resp.setActionResults(results);
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUDelete(IEspContext &context, IEspWUDeleteRequest &req, IEspWUDeleteResponse &resp)
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());

        if (!doAction(context,req.getWuids(), ActionDelete, params, &results))
            resp.setActionResults(results);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}


bool CWsWorkunitsEx::onWUAbort(IEspContext &context, IEspWUAbortRequest &req, IEspWUAbortResponse &resp)
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());
        if (!doAction(context,req.getWuids(), ActionAbort, params))
            resp.setActionResults(results);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUProtect(IEspContext &context, IEspWUProtectRequest &req, IEspWUProtectResponse &resp)\
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params(createProperties(true));
        int action=ActionProtect;

        params->setProp("Protect", true);
        if(!req.getProtect())
            params->setProp("Protect", false);

        params->setProp("BlockTillFinishTimer", 0);
        if (!doAction(context,req.getWuids(), action, params, &results))
            resp.setActionResults(results);
        }
        catch(IException* e)
        {   
            FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}


bool CWsWorkunitsEx::onWUResubmit(IEspContext &context, IEspWUResubmitRequest &req, IEspWUResubmitResponse &resp)
{
    try
    {
        Owned<IMultiException> me = MakeMultiException();
        SCMStringBuffer lastwuid;

        for(int i=0; i<req.getWuids().length();i++)
        {
            const char* wuid=req.getWuids().item(i);

            ensureWorkunitAccess(context, wuid, SecAccess_Write);

            try
            {
                lastwuid.clear();

                Owned<IConstWorkUnit> wu;
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                wu.setown(factory->openWorkUnit(wuid, false));

                // MORE - retrieve "xml" from workunit
                SCMStringBuffer roxieClusterName;
                {
                    Owned<IConstWURoxieQueryInfo> roxieQueryInfo = wu->getRoxieQueryInfo();
                    if (roxieQueryInfo)
                        roxieQueryInfo->getRoxieClusterName(roxieClusterName);
                }

                if (roxieClusterName.length() == 0)
                {
                    if(req.getCloneWorkunit() || req.getRecompile())
                    {
                        resubmitWU(context, wuid, NULL, lastwuid, req.getRecompile());
                    }
                    else
                    {
                        submitWU(context, wuid, NULL, NULL, 0, false, req.getResetWorkflow());
                        lastwuid.set(wuid);
                    }
                }
            }
            catch (IException *E)
            {
                me->append(*E);
            }
            catch (...)
            {
                me->append(*MakeStringException(0,"Unknown exception submitting %s",wuid));
            }
        }

        if(me->ordinality())
            throw me.getLink();

        int timeToWait = req.getBlockTillFinishTimer();
        if (timeToWait != 0)
        {
            for(int i=0; i<req.getWuids().length();i++)
            {
                const char* wuid=req.getWuids().item(i);
                waitForWorkUnitToComplete(wuid, timeToWait);
            }
        }

        if(req.getWuids().length()==1)
        {
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(lastwuid).str());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUSchedule(IEspContext &context, IEspWUScheduleRequest &req, IEspWUScheduleResponse &resp)
{
    try
    {
        DBGLOG("Schedule workunit: %s", req.getWuid());
        const char* cluster = req.getCluster();
        if (!cluster)
             throw MakeStringException(ECLWATCH_INVALID_INPUT,"No Cluster defined.");

        if (!isRoxieCluster(cluster))
        {
            scheduleWU(context, req.getWuid(), cluster, req.getWhen(), req.getSnapshot(), req.getMaxRunTime());
        }
        else //Handle roxie cluster query
        {
            CRoxieQuery roxieQuery;
            roxieQuery.wuid.set(req.getWuid());
            roxieQuery.roxieClusterName.append(cluster);

            StringBuffer clusterName;
            getFirstThorClusterName(clusterName);
            if (clusterName.length() > 0)
            {
                roxieQuery.clusterName.append(clusterName);
            }
            else
            {
                roxieQuery.clusterName = "hthor"; //Hardcoded for now
            }

            roxieQuery.queue = req.getQueue();
            //roxieQuery.jobName;
            roxieQuery.roxieTimeOut = roxieQueryRoxieTimeOut; //Hardcoded for now
            roxieQuery.wuTimeOut = roxieQueryWUTimeOut;

            {  //add the scope for openWorkUnit(wuid)
                Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
                Owned<IConstWorkUnit> lw = factory->openWorkUnit(roxieQuery.wuid.str(), false);
                if(!lw.get())
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.", roxieQuery.wuid.str());

                SCMStringBuffer qname;
                Owned<IConstWUQuery> query=lw->getQuery();
                if (query)
                {
                    query->getQueryText(qname);
                    if(qname.length())
                    {
                        roxieQuery.ecl.append(qname.str());
                    }
                }
                SCMStringBuffer jn;
                roxieQuery.jobName.set(lw->getJobName(jn).str());
                roxieQuery.action = lw->getAction();
            }

            StringBuffer netAddress;
            getClusterConfig("RoxieCluster", cluster, "RoxieServerProcess[1]", netAddress);
            roxieQuery.ip = netAddress.str();

            compileScheduledQueryWU(context, roxieQuery);
            scheduleWU(context, req.getWuid(), cluster, req.getWhen(), req.getSnapshot(), req.getMaxRunTime());
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUSubmit(IEspContext &context, IEspWUSubmitRequest &req, IEspWUSubmitResponse &resp)
{
    try
    {
        const char* wuid = req.getWuid();
        if (!wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No workunit ID defined.");

        ensureWorkunitAccess(context, wuid, SecAccess_Write);

        DBGLOG("Submit workunit: %s", wuid);

        const char* cluster = req.getCluster();
        if (!cluster)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No Cluster defined.");

        double version = context.getClientVersion();
        submitWU(context, wuid, cluster, req.getSnapshot(), req.getMaxRunTime(), true, false);

        int timeToWait = req.getBlockTillFinishTimer();
        if (timeToWait != 0)
        {
            waitForWorkUnitToComplete(wuid, timeToWait);
        }

        resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(req.getWuid()).str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        SCMStringBuffer wuid;
        {
            NewWorkunit wu(context);
            wu->getWuid(wuid);
        }

        resp.updateWorkunit().setWuid(wuid.str());
        AccessSuccess(context,"Updated %s",wuid.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUCreateAndUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create and update workunit. Permission denied.");

        SCMStringBuffer wuid;
        {
            NewWorkunit wu(context);
            wu->getWuid(wuid);
        }

        req.setWuid(wuid.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onWUUpdate(context, req, resp);
}

bool CWsWorkunitsEx::onWUWaitCompiled(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp)
{
    try
    {
        secWaitForWorkUnitToCompile(req.getWuid(), *context.querySecManager(), *context.queryUser(),req.getWait());
        resp.setStateID(CWUWrapper(req.getWuid(), context)->getState());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}
 
bool CWsWorkunitsEx::onWUWaitComplete(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp)
{
    try
    {
        SCMStringBuffer wuid;
        wuid.set(req.getWuid());
        resp.setStateID(secWaitForWorkUnitToComplete(req.getWuid(), *context.querySecManager(), *context.queryUser(),req.getWait(), req.getReturnOnWait()));
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUCDebug(IEspContext &context, IEspWUDebugRequest &req, IEspWUDebugResponse &resp)
{
    try
    {
        StringBuffer result;
        secDebugWorkunit(req.getWuid(), *context.querySecManager(), *context.queryUser(),req.getCommand(), result);
        resp.setResult(result);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}
 
bool CWsWorkunitsEx::onWUSyntaxCheckECL(IEspContext &context, IEspWUSyntaxCheckRequest &req, IEspWUSyntaxCheckResponse &resp)
{
    try
    {
        SCMStringBuffer wuid;
        {
            NewWorkunit wu(context);
            wu->setAction(WUActionCheck);
            wu->setSnapshot(req.getSnapshot());

            if(req.getModuleName() && req.getAttributeName())
            {
            wu->setApplicationValue("SyntaxCheck","ModuleName",req.getModuleName(),true);
            wu->setApplicationValue("SyntaxCheck","AttributeName",req.getAttributeName(),true);
            }

            wu.setECL(req.getECL());

            wu->getWuid(wuid);

            double version = context.getClientVersion();
            if (version > 1.03)
            {
                for(int di=0; di<req.getDebugValues().length();di++)
                {   
                    IConstDebugValue& item=req.getDebugValues().item(di);
                    if(item.getName() && *item.getName())
                    {
                        wu->setDebugValue(item.getName(),item.getValue(),true);
                    }
                }
            }
        }

        submitWU(context,wuid.str(),req.getCluster(),req.getSnapshot(),0, true, false);
        int state = waitForWorkUnitToComplete(wuid.str(),req.getTimeToWait());
        {
            CWUWrapper wu(wuid.str(), context);
            WUExceptions errors(*wu);
            resp.setErrors(errors);
        }

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        factory->deleteWorkUnit(wuid.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}


bool CWsWorkunitsEx::onWUCompileECL(IEspContext &context, IEspWUCompileECLRequest &req, IEspWUCompileECLResponse &resp)
{
    try
    {
        SCMStringBuffer wuid;
        {
            NewWorkunit wu(context);
            if(req.getIncludeComplexity())
            {
                wu->setAction(WUActionCompile);
                wu->setDebugValueInt("calculateComplexity",1,true);
            }
            else
            {
                wu->setAction(WUActionCheck);
            }

            wu->setSnapshot(req.getSnapshot());
            if(req.getModuleName() && req.getAttributeName())
            {
                wu->setApplicationValue("SyntaxCheck","ModuleName",req.getModuleName(),true);
                wu->setApplicationValue("SyntaxCheck","AttributeName",req.getAttributeName(),true);
            }

            if(req.getIncludeDependencies())
                wu->setApplicationValueInt("SyntaxCheck","IncludeDependencies",1,true);

            wu.setECL(req.getECL());

            wu->getWuid(wuid);
        }

        ensureWorkunitAccess(context, wuid.str(), SecAccess_Write);
        submitWU(context,wuid.str(),req.getCluster(),req.getSnapshot(),0, true, false);
    
        int state = waitForWorkUnitToComplete(wuid.str(),req.getTimeToWait());
        {
        CWUWrapper wu(wuid.str(), context);

        SCMStringBuffer buf;
        wu->getDebugValue("__Calculated__Complexity__",buf);
        if(buf.length())
            resp.setComplexity(buf.str());

        WUExceptions errors(*wu);
        resp.setErrors(errors);

        if(!errors.ErrCount())
        {
            IArrayOf<IEspWUECLAttribute> results;
            for(unsigned count=1;;count++)
            {
                SCMStringBuffer buf;
                wu->getApplicationValue("SyntaxCheck",StringBuffer("Dependency").append(count).str(),buf);
                if(!buf.length())
                    break;
                Owned<IPropertyTree> res=createPTreeFromXMLString(buf.str(), ipt_caseInsensitive);
                if(!res)
                    continue;

                Owned<IEspWUECLAttribute> a= createWUECLAttribute("","");
                a->setModuleName(res->queryProp("@module"));
                a->setAttributeName(res->queryProp("@name"));

                int flags=res->getPropInt("@flags",0);

                if(flags&ob_locked)
                    if(flags&ob_lockedself)
                        a->setIsCheckedOut(true);
                    else
                        a->setIsLocked(true);

                if(flags&ob_sandbox)
                    a->setIsSandbox(true);

                if(flags&ob_orphaned)
                    a->setIsOrphaned(true);

                results.append(*a.getLink());
            }
            resp.setDependencies(results);
        }
        }

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        factory->deleteWorkUnit(wuid.str());
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

IEspECLJob* createEclJobFromString0(double version, const char* str)
{
    if(!str || !*str)
        return NULL;

    Owned<IEspECLJob> job = createECLJob("", "");

    // startdate
    const char* bptr = str;
    const char* eptr = strchr(bptr, ',');

    StringBuffer sdate;

    if(!eptr)
        sdate.append(bptr);
    else
        sdate.append(eptr - bptr, bptr);

    sdate.setCharAt(10, 'T');

    if(!eptr)
        return job.getLink();
    
    //Timing
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();

    //ThorGraph
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();

    char buf[256];

    // cluster name
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();
    else
    {
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
        job->setCluster(buf);
    }

    //WUID
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
    {
        job->setWuid(bptr);
        return job.getLink();
    }
    else
    {
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
        job->setWuid(buf);
    }
    
    //graph number
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();
    StringBuffer graph("graph");
    graph.append(eptr - bptr, bptr);
    job->setGraph(graph.str());

    if (version > 1.05)
    {
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
        job->setGraphNum(buf);
    }

    //skip
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();

    if (version > 1.05)
    {
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
        job->setSubGraphNum(buf);
    }

    //skip
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return job.getLink();

    if (version > 1.05)
    {
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
        job->setNumOfRuns(buf);
    }

    //duration, state
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr && 0==strncmp(",FAILED", eptr, 7))
    {
        job->setState("failed");
        int len = eptr - bptr;
        strncpy(buf, bptr, len);
        buf[len] = 0;
    }
    else
    {
        job->setState("finished");
        strcpy(buf, bptr);
    }
    int duration = atoi(buf) / 1000;
    if (version > 1.05)
       job->setDuration(duration);

    CDateTime startdt;
    CDateTime enddt;
    
    enddt.setString(sdate.str(), NULL, true);
    startdt.set(enddt.getSimple() - duration);

    sdate.clear();
    startdt.getString(sdate, false);
    sdate.append('Z');
    job->setStartedDate(sdate.str());

    StringBuffer edate;
    enddt.getString(edate, false);
    edate.append('Z');
    job->setFinishedDate(edate.str());

    return job.getLink();
}

bool getClusterJobXLS(double version, IStringVal &ret, const char* cluster, const char* startDate, const char* endDate,
                                        bool showall, const char* bbtime0, const char* betime0)
{
    float bbtime = 0;
    float betime = 24;
    StringBuffer bbtimestr = bbtime0;
    StringBuffer betimestr = betime0;

    if(showall)
    {
        if(bbtimestr.length() > 0)
        {
            const char* bbptr = bbtimestr.str();
            int hours = 0;
            int mins = 0; 
            while(isdigit(*bbptr))
            {
                hours = 10*hours + *bbptr - '0';
                bbptr++;
            }
            if(*bbptr == ':')
                bbptr++;
            while(isdigit(*bbptr))
            {
                mins = 10*mins + (*bbptr - '0');
                bbptr++;
            }

            bbtime = hours + mins/60.0;
        }

        if(betimestr.length() > 0)
        {
            const char* beptr = betimestr.str();
            int hours = 0;
            int mins = 0; 
            while(isdigit(*beptr))
            {
                hours = 10*hours + *beptr - '0';
                beptr++;
            }
            if(*beptr == ':')
                beptr++;
            while(isdigit(*beptr))
            {
                mins = 10*mins + (*beptr - '0');
                beptr++;
            }

            betime = hours + mins/60.0;
        }

        if(bbtime <= 0 || betime > 24 || bbtime >= betime)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
    }

    //WUJobList2 jobs(*request->queryContext(), cluster, startDate, endDate, response, showall, bbtime, betime);
    CDateTime fromTime;
    CDateTime toTime;
    StringBuffer fromstr;
    StringBuffer tostr;

    if(startDate && *startDate)
    {
        fromTime.setString(startDate,NULL,false);
        fromTime.getString(fromstr, false);
    }

    if(endDate && *endDate)
    {
        toTime.setString(endDate,NULL,false);
        toTime.getString(tostr, false);
    }

    StringBuffer responsebuf;

    StringBuffer text;
    text.append("<XmlSchema name=\"MySchema\">");
    ///const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
    ///StringBufferAdaptor adaptor(text);
    ///meta.getXmlSchema(adaptor, false);
    text.append(
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");

    text.append("<xs:element name=\"wuid\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"graph\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"sub-graph\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"runs\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"started\" type=\"xs:string20\"/>\n");
    text.append("<xs:element name=\"finished\" type=\"xs:string20\"/>\n");
    text.append("<xs:element name=\"duration\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"cluster\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"state\" type=\"xs:string\"/>\n");

    text.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    text.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    text.append("</xs:schema>");

    text.append("</XmlSchema>").newline();

    text.append("<Dataset");
    text.append(" name=\"data\" xmlSchema=\"MySchema\" ");
    text.append(">").newline();

    StringAttrArray jobs;
    StringBuffer filter("Timing,ThorGraph");
    if(cluster && *cluster)
        filter.appendf(",%s", cluster);
    queryAuditLogs(fromTime, toTime, filter.str(), jobs);

    IArrayOf<IEspECLJob> jobList;
    ForEachItemIn(idx, jobs)
    {
        const char* curjob = jobs.item(idx).text;
        if(!curjob || !*curjob)
            continue;

//DBGLOG(curjob);

        Owned<IEspECLJob> job = createEclJobFromString0(version, curjob);

        text.append(" <Row>");

      //  cursor->getXmlRow(adaptor);
        //responsebuf<<"parent.displayJob(\'"<<job->getWuid()<<"\',\'"<<job->getGraph()<<"\',"
        //               "\'"<<job->getStartedDate()<<"\',\'"<<job->getFinishedDate()<<"\',"
        //               "\'"<<job->getCluster()<<"\',\'"<<job->getState()<<"\', ''," << showall << "," << bbtime << "," << betime << ")\r\n";
        CDateTime tTime;
        StringBuffer started, finished; 
        unsigned year, month, day, hour, minute, second, nano;
        tTime.setString(job->getStartedDate(),NULL,true);
        tTime.getDate(year, month, day, true);
        tTime.getTime(hour, minute, second, nano, true);
        started.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);
        tTime.setString(job->getFinishedDate(),NULL,true);
        tTime.getDate(year, month, day, true);
        tTime.getTime(hour, minute, second, nano, true);
        finished.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);

        text.appendf("<wuid>%s</wuid>", job->getWuid());
        text.appendf("<graph>%s</graph>", job->getGraphNum());
        text.appendf("<sub-graph>%s</sub-graph>", job->getSubGraphNum());
        text.appendf("<runs>%s</runs>", job->getNumOfRuns());
        text.appendf("<started>%s</started>", started.str());
        text.appendf("<finished>%s</finished>", finished.str());
        text.appendf("<duration>%d</duration>", job->getDuration());
        text.appendf("<cluster>%s</cluster>", job->getCluster());
        text.appendf("<state>%s</state>", job->getState());
        text.append("</Row>");
        text.newline();

        jobList.append(*job.getClear());
    }

////////////////////////////////////////////////////////21492
    //Find out which WUs stopped by abnormal thor termination
    StringArray unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime;
    findUnfinishedECLWUs(jobList, fromTime, toTime, tostr.str(), cluster, unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

    if (unfinishedWUIDs.ordinality())
    {
        ForEachItemIn(idx3, unfinishedWUIDs)
        {
            //Add to graph list
            const char* wuid = unfinishedWUIDs.item(idx3);
            const char* graph = unfinishedGraphs.item(idx3);
            const char* subgraph = unfinishedSubGraphs.item(idx3);
            const char* startTime = unfinishedWUStarttime.item(idx3);
            const char* endTime = unfinishedWUEndtime.item(idx3);

            text.append(" <Row>");

            CDateTime tTime;
            StringBuffer started, finished; 
            unsigned year, month, day, hour, minute, second, nano;
            tTime.setString(startTime,NULL,true);
            tTime.getDate(year, month, day, true);
            tTime.getTime(hour, minute, second, nano, true);
            started.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);
            tTime.setString(endTime,NULL,true);
            tTime.getDate(year, month, day, true);
            tTime.getTime(hour, minute, second, nano, true);
            finished.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);

            text.appendf("<wuid>%s</wuid>", wuid);
            text.appendf("<graph>%s</graph>", graph);
            if (subgraph && strlen(subgraph) > 0)
                text.appendf("<sub-graph>%s</sub-graph>", subgraph);
            else
                text.append("<sub-graph></sub-graph>");
            text.append("<runs></runs>");
            text.appendf("<started>%s</started>", started.str());
            text.appendf("<finished>%s</finished>", finished.str());
            //text.appendf("<duration>%d</duration>", duration);
            text.appendf("<duration></duration>");
            if(cluster && *cluster)
                text.appendf("<cluster>%s</cluster>", cluster);
            text.append("<state>not finished</state>");
            text.append("</Row>");
            text.newline();
        }
    }
////////////////////////////////////////////////////////21492

   text.append("</Dataset>").newline();
    DBGLOG("EXL text:\n%s", text.str());
    ret.set(text.str());

    return true;
}

class CJobUsage : public CInterface
{
public:
    StringBuffer m_date;
    float m_usage, m_busage, m_nbusage;

public:
    IMPLEMENT_IINTERFACE;
    CJobUsage()
    {
        m_usage = 0.0;
        m_busage = 0.0;
        m_nbusage = 0.0;
    };
};

typedef CIArrayOf<CJobUsage> JobUsageArray;

int ZCMJD(unsigned y, unsigned m, unsigned d) 
{
  if (m<3) 
  { 
      m += 12 ; 
      y--; 
  }
  
  return -678973 + d + (((153*m-2)/5)|0) + 365*y + ((y/4)|0) - ((y/100)|0) + ((y/400)|0); 
}

void AddToClusterJobXLS(JobUsageArray& jobsummary, unsigned year0, unsigned month0, unsigned day0, unsigned hour0, unsigned minute0, unsigned second0,
                                unsigned year1, unsigned month1, unsigned day1, unsigned hour1, unsigned minute1, unsigned second1, 
                                int first, float bbtime, float betime)
{
    float bbtime0 = 3600.0*bbtime;
    float betime0 = 3600.0*betime;
    float x1 = 3600.0*hour0 + 60.0*minute0 + second0;
    int   y1 = ZCMJD(year0, month0, day0)-first;
    float x2 = 3600.0*hour1 + 60.0*minute1 + second1;
    int   y2 = ZCMJD(year1, month1, day1)-first;

    for(int y=y1; y<=y2; y++)
    {
        if ((y < 0) || (y > jobsummary.length() - 1))
            continue;

        CJobUsage& jobUsage = jobsummary.item(y);

        float xx1= (y==y1 ? x1 : 0.0);
        float xx2= (y==y2 ? x2 : 86400.0);
        jobUsage.m_usage += (xx2-xx1)/864.0;

        float bhours = ((betime0 < xx2)?betime0:xx2) - ((bbtime0 > xx1)?bbtime0:xx1);
        if(bhours < 0.0)
            bhours = 0.0;
        float nbhours = (xx2 - xx1 - bhours);

        if(bbtime0 + (86400.0 - betime0) > 0.001)
            jobUsage.m_nbusage +=  100*nbhours/(bbtime0 + (86400.0 - betime0));

        if(betime0 - bbtime0 > 0.001)
            jobUsage.m_busage += 100*bhours/(betime0 - bbtime0);
    }

    return;
}

#if 0
void AddToClusterJobXLS(JobUsageArray& jobsummary, unsigned year0, unsigned month0, unsigned day0, unsigned hour0, unsigned minute0, unsigned second0,
                                unsigned year1, unsigned month1, unsigned day1, unsigned hour1, unsigned minute1, unsigned second1, 
                                int first, float bbtime, float betime)
{
    float x1 = hour0 + minute0/60 + second0/3600;
    int   y1 = ZCMJD(year0, month0, day0)-first;
    float x2 = hour1 + minute1/60 + second1/3600;
    int   y2 = ZCMJD(year1, month1, day1)-first;

    for(int y=y1; y<=y2; y++)
    {
        float xx1= (y==y1 ? x1 : 0.0);
        float xx2= (y==y2 ? x2 : 24.0);

        CJobUsage& jobUsage = jobsummary.item(y);
        jobUsage.m_usage += 100*(xx2-xx1)/24;

        float bhours = ((betime < xx2)?betime:xx2) - ((bbtime > xx1)?bbtime:xx1);
        if(bhours < 0.0)
            bhours = 0.0;
        float nbhours = (xx2 - xx1 - bhours);

        if(bbtime + (24.0 - betime) > 0.001)
            jobUsage.m_nbusage +=  100*nbhours/(bbtime + (24.0 - betime));

        if(betime - bbtime > 0.001)
            jobUsage.m_busage += 100*bhours/(betime - bbtime);
    }

    return;
}
#endif

bool getClusterJobSummaryXLS(double version, IStringVal &ret, const char* cluster, const char* startDate, const char* endDate,
                                        bool showall, const char* bbtime0, const char* betime0)
{

    float bbtime = 0;
    float betime = 24;
    StringBuffer bbtimestr = bbtime0;
    StringBuffer betimestr = betime0;

    if(showall)
    {
        if(bbtimestr.length() > 0)
        {
            const char* bbptr = bbtimestr.str();
            int hours = 0;
            int mins = 0; 
            while(isdigit(*bbptr))
            {
                hours = 10*hours + *bbptr - '0';
                bbptr++;
            }
            if(*bbptr == ':')
                bbptr++;
            while(isdigit(*bbptr))
            {
                mins = 10*mins + (*bbptr - '0');
                bbptr++;
            }

            bbtime = hours + mins/60.0;
        }

        if(betimestr.length() > 0)
        {
            const char* beptr = betimestr.str();
            int hours = 0;
            int mins = 0; 
            while(isdigit(*beptr))
            {
                hours = 10*hours + *beptr - '0';
                beptr++;
            }
            if(*beptr == ':')
                beptr++;
            while(isdigit(*beptr))
            {
                mins = 10*mins + (*beptr - '0');
                beptr++;
            }

            betime = hours + mins/60.0;
        }

        if(bbtime <= 0 || betime > 24 || bbtime >= betime)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
    }

    //WUJobList2 jobs(*request->queryContext(), cluster, startDate, endDate, response, showall, bbtime, betime);
    CDateTime fromTime;
    CDateTime toTime;
    StringBuffer fromstr;
    StringBuffer tostr;
    int delayTime = -240; //4 hour time difference
    bool extendedToNextDay = false;

    if(startDate && *startDate)
    {
        fromTime.setString(startDate,NULL,false);
        fromTime.getString(fromstr, false);
    }

    if(endDate && *endDate)
    {
        toTime.setString(endDate,NULL,false);
        toTime.getString(tostr, false);

        unsigned year, month, day, day1;
        CDateTime tTime = toTime;
        tTime.getDate(year, month, day);
        tTime.adjustTime(delayTime);
        tTime.getDate(year, month, day1);
        if (day1 < day)
            extendedToNextDay = true;
    }

    StringBuffer responsebuf;

    StringAttrArray jobs;
    StringBuffer filter("Timing,ThorGraph");
    if(cluster && *cluster)
        filter.appendf(",%s", cluster);
    queryAuditLogs(fromTime, toTime, filter.str(), jobs);

    unsigned year, month, day;
    fromTime.getDate(year, month, day);
   int first = ZCMJD(year, month, day);
    toTime.getDate(year, month, day);
   int last = ZCMJD(year, month, day);
    if (last < first)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid day range");

    CDateTime date0(fromTime);
    JobUsageArray jobUsages;
    for (int i = first; i <= last; i++)
    {
        CJobUsage* jobUsage =  new CJobUsage();
        date0.getDateString(jobUsage->m_date);
      jobUsages.append(*::LINK(jobUsage));

        date0.adjustTime(60*24);
    }

    IArrayOf<IEspECLJob> jobList;
    ForEachItemIn(idx, jobs)
    {
        const char* curjob = jobs.item(idx).text;
        if(!curjob || !*curjob)
            continue;

//DBGLOG(curjob);

        Owned<IEspECLJob> job = createEclJobFromString0(version, curjob);

      //  cursor->getXmlRow(adaptor);
        //responsebuf<<"parent.displayJob(\'"<<job->getWuid()<<"\',\'"<<job->getGraph()<<"\',"
        //               "\'"<<job->getStartedDate()<<"\',\'"<<job->getFinishedDate()<<"\',"
        //               "\'"<<job->getCluster()<<"\',\'"<<job->getState()<<"\', ''," << showall << "," << bbtime << "," << betime << ")\r\n";
        CDateTime tTime;
        unsigned year0, month0, day0, hour0, minute0, second0, nano0;
        tTime.setString(job->getStartedDate(),NULL,true);
        tTime.adjustTime(delayTime);
        tTime.getDate(year0, month0, day0, true);
        tTime.getTime(hour0, minute0, second0, nano0, true);
        unsigned year1, month1, day1, hour1, minute1, second1, nano1;
        tTime.setString(job->getFinishedDate(),NULL,true);
        tTime.adjustTime(delayTime);
        tTime.getDate(year1, month1, day1, true);
        tTime.getTime(hour1, minute1, second1, nano1, true);

        AddToClusterJobXLS(jobUsages, year0, month0, day0, hour0, minute0, second0, year1, month1, day1, hour1, minute1, second1, first, bbtime, betime);

        jobList.append(*job.getClear());
    }

////////////////////////////////////////////////////////21492
    //Find out which WUs stopped by abnormal thor termination
    StringArray unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime;
    findUnfinishedECLWUs(jobList, fromTime, toTime, tostr.str(), cluster, unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

    if (unfinishedWUIDs.ordinality())
    {
        ForEachItemIn(idx3, unfinishedWUIDs)
        {
            //Add to graph list
            const char* startTime = unfinishedWUStarttime.item(idx3);
            const char* endTime = unfinishedWUEndtime.item(idx3);

            CDateTime tTime;
            unsigned year0, month0, day0, hour0, minute0, second0, nano0;
            tTime.setString(startTime,NULL,true);
            tTime.adjustTime(delayTime);
            tTime.getDate(year0, month0, day0, true);
            tTime.getTime(hour0, minute0, second0, nano0, true);
            unsigned year1, month1, day1, hour1, minute1, second1, nano1;
            tTime.setString(endTime,NULL,true);
            tTime.adjustTime(delayTime);
            tTime.getDate(year1, month1, day1, true);
            tTime.getTime(hour1, minute1, second1, nano1, true);

            AddToClusterJobXLS(jobUsages, year0, month0, day0, hour0, minute0, second0, year1, month1, day1, hour1, minute1, second1, first, bbtime, betime);
        }
    }
////////////////////////////////////////////////////////21492

    StringBuffer text;
    text.append("<XmlSchema name=\"MySchema\">");
    ///const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
    ///StringBufferAdaptor adaptor(text);
    ///meta.getXmlSchema(adaptor, false);
    text.append(
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");

    text.append("<xs:element name=\"Date\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"Business\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"Non-business\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"Overall\" type=\"xs:string\"/>\n");

    text.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    text.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    text.append("</xs:schema>");

    text.append("</XmlSchema>").newline();

    text.append("<Dataset");
    text.append(" name=\"data\" xmlSchema=\"MySchema\" ");
    text.append(">").newline();

    StringBuffer percentageStr;
    percentageStr.append("%");
    int lastUsage = jobUsages.length();
    if (extendedToNextDay)
        lastUsage --;
    for (int i0 = 0; i0 < lastUsage; i0++)
    {
        CJobUsage& jobUsage = jobUsages.item(i0);
        //if (jobUsage.m_usage < 0.1)
        //  continue;

        text.append(" <Row>");
        text.appendf("<Date>%s</Date>", jobUsage.m_date.str());
        text.appendf("<Business>%3.0f %s</Business>", jobUsage.m_busage, percentageStr.str());
        text.appendf("<Non-business>%3.0f %s</Non-business>", jobUsage.m_nbusage, percentageStr.str());
        text.appendf("<Overall>%3.0f %s</Overall>", jobUsage.m_usage, percentageStr.str());
        text.append(" </Row>");
    }

    text.append("</Dataset>").newline();
    DBGLOG("EXL text:\n%s", text.str());
    ret.set(text.str());

    return true;
}

bool CWsWorkunitsEx::getClusterJobQueueXLS(double version, IStringVal &ret, const char* cluster, const char* startDate, const char* endDate, const char* showType)
{
    CDateTime fromTime;
    CDateTime toTime;
    StringBuffer fromstr;
    StringBuffer tostr;

    if(startDate && *startDate)
    {
        fromTime.setString(startDate,NULL,false);
        fromTime.getString(fromstr, false);
    }

    if(endDate && *endDate)
    {
        toTime.setString(endDate,NULL,false);
        toTime.getString(tostr, false);
    }

    StringBuffer responsebuf;

    StringBuffer text;
    text.append("<XmlSchema name=\"MySchema\">");
    text.append(
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");

    text.append("<xs:element name=\"datetime\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"running\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"queued\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"connected\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"waiting\" type=\"xs:string20\"/>\n");
    text.append("<xs:element name=\"idlecount\" type=\"xs:string20\"/>\n");
    text.append("<xs:element name=\"running_wuid1\" type=\"xs:string\"/>\n");
    text.append("<xs:element name=\"running_wuid2\" type=\"xs:string\"/>\n");

    text.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    text.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    text.append("</xs:schema>");

    text.append("</XmlSchema>").newline();

    text.append("<Dataset");
    text.append(" name=\"data\" xmlSchema=\"MySchema\" ");
    text.append(">").newline();

    StringAttrArray lines;
    StringBuffer filter("ThorQueueMonitor");
    if(cluster && *cluster)
    {
        CTpWrapper dummy;
        IArrayOf<IEspTpCluster> clusters;
        dummy.getClusterProcessList(eqThorCluster,clusters);
        ForEachItemIn(i, clusters)
        {
            IEspTpCluster& cluster1 = clusters.item(i);
            const char* cluster0 = cluster1.getName();
            if (!cluster0 || !*cluster0)
                continue;

            if(stricmp(cluster, cluster0)==0)
            {
                const char* queuename = cluster1.getQueueName();
                if (queuename && *queuename)
                    filter.appendf(",%s", queuename);

                break;
            }
        }       
    }
        
    queryAuditLogs(fromTime, toTime, filter.str(), lines);

    unsigned longestQueue = 0;
    unsigned maxConnected = 0;
    unsigned maxDisplay = 0;
    IArrayOf<IEspThorQueue> items;
    ForEachItemIn(idx, lines)
    {
        const char* line = lines.item(idx).text;
        if(!line || !*line)
            continue;

        WUJobQueue jq;
        if (showType && !stricmp(showType, "InGraph"))
        {
            if (idx < (lines.length() - 1))
                jq.getQueueInfoFromString(line, longestQueue, maxConnected, maxDisplay, 1, items);
            else
                jq.getQueueInfoFromString(line, longestQueue, maxConnected, maxDisplay, 2, items);
        }
        else
            jq.getQueueInfoFromString(line, longestQueue, maxConnected, maxDisplay, 0, items);
    }

    if (items.length() > 1)
    {
        ForEachItemIn(i,items) 
        {
            IEspThorQueue& tq = items.item(i);

            text.append(" <Row>");

            text.appendf("<datetime>%s</datetime>", tq.getDT());
            text.appendf("<running>%s</running>", tq.getRunningWUs());
            text.appendf("<queued>%s</queued>", tq.getQueuedWUs());
            text.appendf("<connected>%s</connected>", tq.getConnectedThors());
            text.appendf("<waiting>%s</waiting>", tq.getWaitingThors());
            text.appendf("<idlecount>%s</idlecount>", tq.getIdledThors());
            if (tq.getRunningWU1() && *tq.getRunningWU1())
                text.appendf("<running_wuid1>%s</running_wuid1>", tq.getRunningWU1());
            if (tq.getRunningWU2() && *tq.getRunningWU2())
                text.appendf("<running_wuid2>%s</running_wuid2>", tq.getRunningWU2());
            text.append("</Row>");
            text.newline();
        }
    }

   text.append("</Dataset>").newline();
    //DBGLOG("EXL text:\n%s", text.str());
    ret.set(text.str());

    return true;
}

bool CWsWorkunitsEx::onWUClusterJobQueueXLS(IEspContext &context, IEspWUClusterJobQueueXLSRequest &req, IEspWUClusterJobQueueXLSResponse &resp)
{
    try
    {
        DBGLOG("Started CWsWorkunitsEx::onWUClusterJobQueueXLS\n");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
          accessOwn = SecAccess_None;

        if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
          accessOthers = SecAccess_None;

        //throw exception if no access on both own/others' workunits
        if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
        {
            context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

            StringBuffer msg;
            msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
        }

        MemoryBuffer text;
        struct MemoryBuffer2IStringVal : public CInterface, implements IStringVal
        {
             MemoryBuffer2IStringVal(MemoryBuffer & _buffer) : buffer(_buffer) {}
             IMPLEMENT_IINTERFACE;

             virtual const char * str() const { UNIMPLEMENTED;  }
             virtual void set(const char *val) { buffer.append(strlen(val),val); }
             virtual void clear() { } // clearing when appending does nothing
             virtual void setLen(const char *val, unsigned length) { buffer.append(length, val); }
             virtual unsigned length() const { return buffer.length(); };
             MemoryBuffer & buffer;
        } adaptor(text);

//  StringBuffer responsebuf;
//  responsebuf<<"parent.displayQEnd(\'Loading\')</script>\r\n";
//  resp->sendChunk(responsebuf.str());

        double version = context.getClientVersion();
        getClusterJobQueueXLS(version, adaptor, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowType());

        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);


        StringBuffer temp;
        temp<<"<WUResultExcel><Result>";
        temp.append(text.length(),text.toByteArray());
        temp<<"</Result></WUResultExcel>";
        //DBGLOG("XML:%s\n", temp.str());

        StringBuffer xls;
        xsltTransform(temp.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer buff;
        buff.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(buff);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUClusterJobQueueLOG(IEspContext &context,IEspWUClusterJobQueueLOGRequest  &req, IEspWUClusterJobQueueLOGResponse &resp)
{
    try
    {
        DBGLOG("Started CWsWorkunitsEx::onWUClusterJobQueueLOG\n");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
            accessOwn = SecAccess_None;

        if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
            accessOthers = SecAccess_None;

        //throw exception if no access on both own/others' workunits
        if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
        {
            context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

            StringBuffer msg;
            msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
        }

        const char* cluster = req.getCluster();
        const char* startDate = req.getStartDate();
        const char* endDate = req.getEndDate();

        CDateTime fromTime;
        CDateTime toTime;
        StringBuffer fromstr;
        StringBuffer tostr;

        if(startDate && *startDate)
        {
            fromTime.setString(startDate,NULL,false);
            fromTime.getString(fromstr, false);
        }

        if(endDate && *endDate)
        {
            toTime.setString(endDate,NULL,false);
            toTime.getString(tostr, false);
        }

        StringAttrArray lines;
        StringBuffer filter("ThorQueueMonitor");
        if(cluster && *cluster)
        {
            CTpWrapper dummy;
            IArrayOf<IEspTpCluster> clusters;
            dummy.getClusterProcessList(eqThorCluster,clusters);
            ForEachItemIn(i, clusters)
            {
                IEspTpCluster& cluster1 = clusters.item(i);
                const char* cluster0 = cluster1.getName();
                if (!cluster0 || !*cluster0)
                    continue;

                if(stricmp(cluster, cluster0)==0)
                {
                    const char* queuename = cluster1.getQueueName();
                    if (queuename && *queuename)
                        filter.appendf(",%s", queuename);

                    break;
                }
            }       
        }
            
        queryAuditLogs(fromTime, toTime, filter.str(), lines);

        StringBuffer strBuff;
        ForEachItemIn(idx, lines)
        {
            const char* line = lines.item(idx).text;
            if(!line || !*line)
                continue;

            strBuff.appendf("%s\r\n", line);
            if (strBuff.length() > LOGFILESIZELIMIT)
            {
                strBuff.appendf("... ...");
                break;
            }
        }
        
        MemoryBuffer membuff;
        membuff.setBuffer(strBuff.length(), (void*)strBuff.toCharArray());
        
        resp.setThefile_mimetype(HTTP_TYPE_TEXT_PLAIN);
        resp.setThefile(membuff);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUClusterJobXLS(IEspContext &context, IEspWUClusterJobXLSRequest &req, IEspWUClusterJobXLSResponse &resp)
{
    try
    {
        DBGLOG("Started CWsWorkunitsEx::onWUClusterJobXLS\n");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
          accessOwn = SecAccess_None;

        if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
          accessOthers = SecAccess_None;

        //throw exception if no access on both own/others' workunits
        if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
        {
            context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

            StringBuffer msg;
            msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
        }

        MemoryBuffer text;
        struct MemoryBuffer2IStringVal : public CInterface, implements IStringVal
        {
             MemoryBuffer2IStringVal(MemoryBuffer & _buffer) : buffer(_buffer) {}
             IMPLEMENT_IINTERFACE;

             virtual const char * str() const { UNIMPLEMENTED;  }
             virtual void set(const char *val) { buffer.append(strlen(val),val); }
             virtual void clear() { } // clearing when appending does nothing
             virtual void setLen(const char *val, unsigned length) { buffer.append(length, val); }
             virtual unsigned length() const { return buffer.length(); };
             MemoryBuffer & buffer;
        } adaptor(text);

        double version = context.getClientVersion();
        getClusterJobXLS(version, adaptor, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowAll(), req.getBusinessStartTime(), req.getBusinessEndTime());

        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);


        StringBuffer temp;
        temp<<"<WUResultExcel><Result>";
        temp.append(text.length(),text.toByteArray());
        temp<<"</Result></WUResultExcel>";
        //DBGLOG("XML:%s\n", temp.str());

        StringBuffer xls;
        xsltTransform(temp.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer buff;
        buff.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(buff);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUClusterJobSummaryXLS(IEspContext &context, IEspWUClusterJobSummaryXLSRequest &req, IEspWUClusterJobSummaryXLSResponse &resp)
{
    try
    {
        DBGLOG("Started CWsWorkunitsEx::onWUClusterJobSummaryXLS\n");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        if (!context.authorizeFeature(OWN_WU_URL, accessOwn))
          accessOwn = SecAccess_None;

        if (!context.authorizeFeature(OTHERS_WU_URL, accessOthers))
          accessOthers = SecAccess_None;

        //throw exception if no access on both own/others' workunits
        if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
        {
            context.AuditMessage(AUDIT_TYPE_ACCESS_FAILURE, "Authorization", "Access Denied: User can't view any workunits", "Resource: %s, %s", OWN_WU_URL, OTHERS_WU_URL);

            StringBuffer msg;
            msg.appendf("ESP Access Denied: User %s does not have rights to access any workunits.", context.queryUserId());
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
        }

        MemoryBuffer text;
        struct MemoryBuffer2IStringVal : public CInterface, implements IStringVal
        {
             MemoryBuffer2IStringVal(MemoryBuffer & _buffer) : buffer(_buffer) {}
             IMPLEMENT_IINTERFACE;

             virtual const char * str() const { UNIMPLEMENTED;  }
             virtual void set(const char *val) { buffer.append(strlen(val),val); }
             virtual void clear() { } // clearing when appending does nothing
             virtual void setLen(const char *val, unsigned length) { buffer.append(length, val); }
             virtual unsigned length() const { return buffer.length(); };
             MemoryBuffer & buffer;
        } adaptor(text);

        double version = context.getClientVersion();
        ///getClusterJobXLS(version, adaptor, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowAll(), req.getBusinessStartTime(), req.getBusinessEndTime());
        getClusterJobSummaryXLS(version, adaptor, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowAll(), req.getBusinessStartTime(), req.getBusinessEndTime());

        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);


        StringBuffer temp;
        temp<<"<WUResultExcel><Result>";
        temp.append(text.length(),text.toByteArray());
        temp<<"</Result></WUResultExcel>";
        //DBGLOG("XML:%s\n", temp.str());

        StringBuffer xls;
        xsltTransform(temp.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer buff;
        buff.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(buff);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUJobList(IEspContext &context, IEspWUJobListRequest &req, IEspWUJobListResponse &resp)
{
   return true;
}

bool CWsWorkunitsEx::onWUGetDependancyTrees(IEspContext& context, IEspWUGetDependancyTreesRequest& req, IEspWUGetDependancyTreesResponse& resp)
{
    try
    {
        DBGLOG("Enter onWUGetDependancyTrees");

        unsigned int timeMilliSec = 500;
        SCMStringBuffer wuid;
        {
            NewWorkunit wu(context);
            wu->setAction(WUActionCheck);
            if (req.getCluster() && *req.getCluster())
                wu->setClusterName(req.getCluster());
            if (req.getSnapshot() && *req.getSnapshot())
                wu->setSnapshot(req.getSnapshot());

            //items for ECL
            if (req.getItems() && *req.getItems())
                wu->setDebugValue("gatherDependenciesSelection",req.getItems(),true);
            else
                wu->setDebugValue("gatherDependenciesSelection",NULL, true);

            double version = context.getClientVersion();
            if (version > 1.12)
            {
                wu->setDebugValueInt("gatherDependencies",1,true);

                const char *timeout0 = req.getTimeoutMilliSec();
                if (timeout0 && *timeout0)
                {
                    char *timeout = (char *) timeout0;
                    while (timeout && *timeout)
                    {
                        char c = timeout[0];
                        if (!isdigit(c))
                            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Incorrect timeout value");
                        timeout++;
                    }

                    timeMilliSec = atol(timeout0);
                }
            }

            wu->getWuid(wuid);

        }

        ensureWorkunitAccess(context, wuid.str(), SecAccess_Read);
        submitWU(context,wuid.str(),req.getCluster(),req.getSnapshot(),0, true, false);

        int state = waitForWorkUnitToComplete(wuid.str(), timeMilliSec);
        {
            CWUWrapper wu(wuid.str(), context);
            WUExceptions errors(*wu);
            resp.setErrors(errors);
        
            MemoryBuffer temp;
            MemoryBuffer2IDataVal xmlresult(temp);
            Owned<IConstWUResult> result = wu->getResultBySequence(0);
            if (result)
            {
                result->getResultRaw(xmlresult, NULL, NULL);
                resp.setDependancyTrees(temp);
            }
        }

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();

        {
            Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid.str());
            if (lw)
            {
                lw->setState(WUStateAborted);
            }
        }

        factory->deleteWorkUnit(wuid.str());

        DBGLOG("Leave onWUGetDependancyTrees");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUListLocalFileRequired(IEspContext& context, IEspWUListLocalFileRequiredRequest& req, IEspWUListLocalFileRequiredResponse& resp)
{
    try
    {
        const char* wuid = req.getWuid();
        if (!wuid || !*wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not defined.");

        ensureWorkunitAccess(context, wuid, SecAccess_Read);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> lw = factory->updateWorkUnit(wuid);
        if (!lw)
        {
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Workunit %s not found.", wuid);
        }

        IArrayOf<IEspLogicalFileUpload> localFiles;
        Owned<IConstLocalFileUploadIterator> it = lw->getLocalFileUploads();
        ForEach(*it)
        {
            Owned<IConstLocalFileUpload> file = it->get();
            if(!file)
                continue;

            Owned<IEspLogicalFileUpload> file0 = createLogicalFileUpload();

            SCMStringBuffer source, destination, eventTag;
            
            file0->setType(file->queryType());
            file->getSource(source);
            if (source.length() > 0)
                file0->setSource(source.str());

            file->getDestination(destination);
            if (destination.length() > 0)
                file0->setDestination(destination.str());

            file->getEventTag(eventTag);
            if (eventTag.length() > 0)
                file0->setEventTag(eventTag.str());

            localFiles.append(*file0.getLink());
        }

        resp.setLocalFileUploads(localFiles);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUAddLocalFileToWorkunit(IEspContext& context, IEspWUAddLocalFileToWorkunitRequest& req, IEspWUAddLocalFileToWorkunitResponse& resp)
{
    try
    {
        const char* wuid = req.getWuid();
        if (!wuid || !*wuid)
        {
            resp.setResult("WUID is not defined!");
            return true;
        }

        ensureWorkunitAccess(context, wuid, SecAccess_Write);

        resp.setWuid(wuid);

        const char* varname = req.getName();
        if (!varname || !*varname)
        {
            resp.setResult("Name is not defined!");
            return true;
        }
        resp.setName(varname);

        wsEclType type = (wsEclType) req.getType();
        const char *val = req.getVal();
        const char *defval = req.getDefVal();
        unsigned len = req.getLength();

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> wu = factory->updateWorkUnit(wuid);
        if (!wu)
        {
            resp.setResult("Workunit not found!");
            return true;
        }

        Owned<IWUResult> wuRslt = wu->updateResultByName(varname);

        if (!val || !*val && defval && *defval)
            val=defval;

        if (val && *val)
        {
            switch (type)
            {
                case xsdBoolean:
                {

                    bool value = (!stricmp(val, "1") || !stricmp(val, "true") || !stricmp(val, "on"));
                    wuRslt->setResultBool(value);
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                }
                case xsdDecimal:
                case xsdFloat:
                case xsdDouble:
                {
                    wuRslt->setResultReal(atof(val));
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                }
                case xsdInteger:
                case xsdNonPositiveInteger:
                case xsdNegativeInteger:
                case xsdLong:
                case xsdInt:
                case xsdShort:
                case xsdByte:
                case xsdNonNegativeInteger:
                case xsdUnsignedLong:
                case xsdUnsignedInt:
                case xsdUnsignedShort:
                case xsdUnsignedByte:
                case xsdPositiveInteger:
                {
                    wuRslt->setResultInt(_atoi64(val));
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                }
                case tnsEspIntArray:
                case tnsEspStringArray:
                {
                    wuRslt->setResultRaw(len, val, ResultFormatXmlSet);
                    break;
                }
                case tnsRawDataFile:
                {
                    wuRslt->setResultRaw(len, val, ResultFormatRaw);
                    break;
                }

                case tnsXmlDataSet:
                {
                    wuRslt->setResultRaw(len, val, ResultFormatXml);
                    break;
                }

                case tnsCsvDataFile:
                case xsdBase64Binary:   //tbd
                case xsdHexBinary:
                    break;

                //unknown, xsdString, xsdAnyURI: xsdQName: xsdNOTATION: xsdNormalizedString: xsdToken:
                //xsdLanguage: xsdNMTOKEN: xsdNMTOKENS: xsdName: xsdNCName: xsdID: xsdIDREF: xsdIDREFS:
                //xsdENTITY: xsdENTITIES: xsdDuration: xsdDateTime: xsdTime: xsdDate: xsdYearMonth:
                //xsdYear: xsdMonthDay: xsdDay: xsdMonth: 
                default:
                {
                    wuRslt->setResultString(val, len);
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                }
            }
        }

        resp.setResult("Result has been set as required!");
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

void CWsWorkunitsEx::pauseWU(IEspContext& context, const char* wuid, bool now)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
   Owned<IWorkUnit> wu(factory->updateWorkUnit(wuid));
   if(!wu.get())
      throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

    ensureWorkunitAccess(context, *wu.get(), SecAccess_Full);

    if (now)
        wu->setAction(WUActionPauseNow);   
    else
        wu->setAction(WUActionPause);   

    return;
}

void CWsWorkunitsEx::resumeWU(IEspContext& context, const char* wuid)
{
    Owned<IWorkUnitFactory> factory = getSecWorkUnitFactory(*context.querySecManager(), *context.queryUser());
   Owned<IWorkUnit> wu(factory->updateWorkUnit(wuid));
   if(!wu.get())
      throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

    ensureWorkunitAccess(context, *wu.get(), SecAccess_Full);

    wu->setAction(WUActionResume);   
    return;
}

