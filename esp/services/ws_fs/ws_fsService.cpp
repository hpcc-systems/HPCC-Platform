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
#pragma warning (disable : 4129)

#include <math.h>
#include "jsocket.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "daclient.hpp"
#include "wshelpers.hpp"
#include "dfuwu.hpp"
#include "workunit.hpp"
#include "ws_fsService.hpp"
#ifdef _WIN32
#include "windows.h"
#endif

#include "dalienv.hpp"


#include "dfuutil.hpp"
#include "portlist.h"
#include "sacmd.hpp"
#include "exception_util.hpp"
#include "LogicFileWrapper.hpp"

#define DFU_WU_URL          "DfuWorkunitsAccess"
#define DFU_EX_URL          "DfuExceptionsAccess"
#define FILE_SPRAY_URL      "FileSprayAccess"
#define FILE_DESPRAY_URL    "FileDesprayAccess"
#define WUDETAILS_REFRESH_MINS 1

const unsigned dropZoneFileSearchMaxFiles = 1000;

void SetResp(StringBuffer &resp, IConstDFUWorkUnit * wu, bool array);
int Schedule::run()
{
    PROGLOG("DfuWorkunit WUSchedule Thread started.");
    while(!stopping)
    {
        unsigned int waitTimeMillies = 1000*60;
        if (!detached)
        {
            try
            {
                if (waitTimeMillies == (unsigned)-1)
                {
                    PROGLOG("WS_FS WUSchedule Thread Re-started.");
                    waitTimeMillies = 1000*60;
                }

                Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
                Owned<IConstDFUWorkUnitIterator> itr = factory->getWorkUnitsByState(DFUstate_scheduled);
                itr->first();
                while(itr->isValid())
                {
                    Owned<IConstDFUWorkUnit> wu = itr->get();
                    CDateTime dt, now;
                    now.setNow();
                    try
                    {
                        wu->getTimeScheduled(dt);
                        if (now.compare(dt) > 0)
                        {
                            StringAttr wuid(wu->queryId());
                            wu.clear();
                            submitDFUWorkUnit(wuid.get());
                        }
                    }
                    catch(IException *e)
                    {
                        StringBuffer msg;
                        IERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                        e->Release();
                    }
                    itr->next();
                }
            }
            catch(IException *e)
            {
                StringBuffer msg;
                IERRLOG("Exception %d:%s in WS_FS Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                e->Release();
            }
            catch(...)
            {
                IERRLOG("Unknown exception in WS_FS Schedule::run");
            }
        }
        else
        {
            OWARNLOG("Detached from DALI, WS_FS schedule interrupted");
            waitTimeMillies = (unsigned)-1;
        }
        semSchedule.wait(waitTimeMillies);
    }
    return 0;
}

void CFileSprayEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/QueueLabel", process, service);
    cfg->getProp(xpath.str(), m_QueueLabel);
    StringArray qlist;
    getDFUServerQueueNames(qlist, nullptr);
    if (qlist.ordinality())
    {
        if (!m_QueueLabel.length())
            m_QueueLabel.append(qlist.item(0));
        else
        {
            bool found = false;
            ForEachItemIn(i, qlist)
            {
                const char* qname = qlist.item(i);
                if (qname && strieq(qname, m_QueueLabel.str()))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw MakeStringException(-1, "Invalid DFU Queue Label %s in configuration file", m_QueueLabel.str());
        }
    }

    xpath.setf("Software/EspProcess[@name=\"%s\"]/@PageCacheTimeoutSeconds", process);
    if (cfg->hasProp(xpath.str()))
        setPageCacheTimeoutMilliSeconds(cfg->getPropInt(xpath.str()));
    xpath.setf("Software/EspProcess[@name=\"%s\"]/@MaxPageCacheItems", process);
    if (cfg->hasProp(xpath.str()))
        setMaxPageCacheItems(cfg->getPropInt(xpath.str()));

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/MonitorQueueLabel", process, service);
    cfg->getProp(xpath.str(), m_MonitorQueueLabel);

    directories.set(cfg->queryPropTree("Software/Directories"));

    StringBuffer prop;
    prop.appendf("queueLabel=%s", m_QueueLabel.str());
    DBGLOG("%s", prop.str());
    prop.clear();
    prop.appendf("monitorQueueLabel=%s", m_MonitorQueueLabel.str());
    DBGLOG("%s", prop.str());

    if (!daliClientActive())
    {
        OERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in your configuration file");
    }

    m_sched.start();

}

StringBuffer& CFileSprayEx::getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage)
{
    context.getAcceptLanguage(acceptLanguage);
    if (!acceptLanguage.length())
    {
        acceptLanguage.set("en");
        return acceptLanguage;
    }
    acceptLanguage.setLength(2);
    VStringBuffer languageFile("%ssmc_xslt/nls/%s/hpcc.xml", getCFD(), acceptLanguage.str());
    if (!checkFileExists(languageFile.str()))
        acceptLanguage.set("en");
    return acceptLanguage;
}


void ParsePath(const char * fullPath, StringBuffer &ip, StringBuffer &filePath, StringBuffer &title)
{
    ip.clear();
    filePath.clear();
    title.clear();

    if(fullPath == NULL || *fullPath == '\0')
        return;

    const char* ptr = fullPath;
    if(*ptr == '\\' && *(ptr+1) == '\\')
    {
        ptr += 2;
        while(*ptr != '\0' && *ptr != '\\')
            ptr++;
        ip.append(ptr - fullPath - 2, fullPath + 2);
    }

    filePath.append(ptr);

    ptr = fullPath + strlen(fullPath) - 1;
    while(ptr > fullPath && *ptr != '\\')
        ptr--;
    title.append(ptr + 1);
}

const char * const NODATETIME="1970-01-01T00:00:00Z";

// Assign from a dfuwu workunit structure to an esp request workunit structure.
static void DeepAssign(IEspContext &context, IConstDFUWorkUnit *src, IEspDFUWorkunit &dest)
{
    if(src == NULL)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "'Source DFU workunit' doesn't exist.");

    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    Owned<IPropertyTree> root = &constEnv->getPTree();

    double version = context.getClientVersion();
    StringBuffer tmp, encoded;
    dest.setID(src->queryId());
    if (src->getClusterName(tmp.clear()).length()!=0)
    {
        char *clusterName = (char *)tmp.str();
        if (clusterName && *clusterName)
        {
            StringBuffer clusterNameForDisplay(clusterName);

            Owned<IPropertyTreeIterator> clusters= root->getElements("Software/Topology/Cluster");
            if (clusters->first())
            {
                do {
                    IPropertyTree &cluster = clusters->query();
                    const char* name = cluster.queryProp("@name");
                    if (!name || !*name)
                        continue;

                    Owned<IPropertyTreeIterator> thorClusters= cluster.getElements(eqThorCluster);
                    Owned<IPropertyTreeIterator> roxieClusters= cluster.getElements(eqRoxieCluster);
                    if (thorClusters->first() || roxieClusters->first())
                    {
                        if (thorClusters->first())
                        {
                            IPropertyTree &thorCluster = thorClusters->query();
                            const char* process = thorCluster.queryProp("@process");
                            if (process && *process)
                            {
                                if (clusterName && !stricmp(clusterName, process))
                                {
                                    clusterNameForDisplay.clear().append(name);
                                    break;
                                }
                            }
                        }

                        if (roxieClusters->first())
                        {
                            IPropertyTree &roxieCluster = roxieClusters->query();
                            const char* process = roxieCluster.queryProp("@process");
                            if (process && *process)
                            {
                                if (clusterName && !stricmp(clusterName, name))
                                {
                                    clusterNameForDisplay.clear().append(name);
                                    break;
                                }
                            }
                        }
                    }
                } while (clusters->next());
            }
            dest.setClusterName(clusterNameForDisplay.str());
        }
    }

    if ((version > 1.05) && src->getDFUServerName(tmp.clear()).length())
        dest.setDFUServerName(tmp.str());

    if (src->getJobName(tmp.clear()).length()!=0)
        dest.setJobName(tmp.str());
    else
        dest.setJobName("");

    if (src->getQueue(tmp.clear()).length()!=0)
        dest.setQueue(tmp.str());
    if (src->getUser(tmp.clear()).length()!=0)
        dest.setUser(tmp.str());

    dest.setIsProtected(src->isProtected());
    dest.setCommand(src->getCommand());

    IConstDFUprogress *prog = src->queryProgress();
    if (prog != NULL)
    {
        DFUstate state = prog->getState();
        dest.setState(state);
        StringBuffer statemsg;
        encodeDFUstate(state,statemsg);
        dest.setStateMessage(statemsg.str());

        CDateTime startAt;
        CDateTime stoppAt;
        prog->getTimeStarted(startAt);
        prog->getTimeStopped(stoppAt);
        StringBuffer tmpstr;
        startAt.getDateString(tmpstr);
        tmpstr.append(" ");
        startAt.getTimeString(tmpstr);
        dest.setTimeStarted(tmpstr.str());
        tmpstr.clear();
        stoppAt.getDateString(tmpstr);
        tmpstr.append(" ");
        stoppAt.getTimeString(tmpstr);
        dest.setTimeStopped(tmpstr.str());

        StringBuffer prgmsg;
        prog->formatProgressMessage(prgmsg);
        dest.setProgressMessage(prgmsg.str());
        prog->formatSummaryMessage(prgmsg.clear());
        dest.setSummaryMessage(prgmsg.str());
        unsigned secs = prog->getSecsLeft();
        if(secs > 0)
            dest.setSecsLeft(secs);
        dest.setPercentDone(prog->getPercentDone());
    }

    IConstDFUoptions *options = src->queryOptions();
    if(options)
    {
        dest.setReplicate(options->getReplicate());
        dest.setOverwrite(options->getOverwrite());
    }

    IConstDFUfileSpec * file = src->querySource();
    if (file != NULL)
    {
        //if (file->getTitle(tmp.clear()).length()!=0)
        //  dest.setSourceTitle(tmp.str());
        StringBuffer lfn;
        file->getLogicalName(lfn);
        if (lfn.length() != 0)
            dest.setSourceLogicalName(lfn.str());
        else
            dest.setSourceFormat(file->getFormat());

        if (file->getRawDirectory(tmp.clear()).length()!=0)
            dest.setSourceDirectory(tmp.str());
        SocketEndpoint srcdali;
        StringBuffer srcdaliip;
        file->getForeignDali(srcdali);
        srcdali.getIpText(srcdaliip);
        if(srcdaliip.length() > 0 && strcmp(srcdaliip.str(), "0.0.0.0") != 0)
            dest.setSourceDali(srcdaliip.str());
        StringBuffer diffkeyname;
        file->getDiffKey(diffkeyname);
        if(diffkeyname.length() > 0)
            dest.setSourceDiffKeyName(diffkeyname.str());

        StringBuffer socket, dir, title;
        unsigned np = file->getNumParts(0);                 // should handle multiple clusters?
        if (lfn.length() == 0) { // no logical name
            if (np == 1)
            {
                Owned<IFileDescriptor> info;
                try
                {
                    info.setown(file->getFileDescriptor());
                    if(info)
                    {
                        Owned<INode> node = info->getNode(0);
                        if (node)
                        {
                            node->endpoint().getIpText(socket);
                            dest.setSourceIP(socket.str());
                        }
                        const char *defaultdir = info->queryDefaultDir();
                        if (defaultdir&&*defaultdir)
                            addPathSepChar(dir.append(defaultdir));
                        file->getRawFileMask(dir);
                        dest.setSourceFilePath(dir.str());
                    }
                }
                catch(IException *e)
                {
                    EXCLOG(e,"DeepAssign getFileDescriptor");
                    e->Release();
                }
            }
        }
        if (np)
            dest.setSourceNumParts(np);
        unsigned rs = file->getRecordSize();
        if (rs)
            dest.setSourceRecordSize(rs);
        StringBuffer rowtag;
        file->getRowTag(rowtag);
        if(rowtag.length() > 0)
            dest.setRowTag(rowtag.str());

        if (version >= 1.04 && (file->getFormat() == DFUff_csv))
        {
            StringBuffer separate, terminate, quote, escape;
            bool quotedTerminator;
            file->getCsvOptions(separate,terminate,quote, escape, quotedTerminator);
            if(separate.length() > 0)
                dest.setSourceCsvSeparate(separate.str());
            if(terminate.length() > 0)
                dest.setSourceCsvTerminate(terminate.str());
            if(quote.length() > 0)
                dest.setSourceCsvQuote(quote.str());
            if((version >= 1.05) && (escape.length() > 0))
                dest.setSourceCsvEscape(escape.str());
            if(version >=1.10)
                dest.setQuotedTerminator(quotedTerminator);
        }
    }

    file = src->queryDestination();
    if (file != NULL)
    {
        StringBuffer lfn;
        file->getLogicalName(lfn);
        if (lfn.length() != 0)
            dest.setDestLogicalName(lfn.str());
        else
            dest.setDestFormat(file->getFormat());

        if (file->getRawDirectory(tmp.clear()).length()!=0)
            dest.setDestDirectory(tmp.str());
        if (file->getGroupName(0,tmp.clear()).length()!=0)      // should handle multiple clusters?
        {
            char *clusterName = (char *)tmp.str();
            if (clusterName)
                dest.setDestGroupName(clusterName);
        }

        StringBuffer socket, dir, title;
        unsigned np = file->getNumParts(0);                 // should handle multiple clusters?
        if (lfn.length() == 0) { // no logical name
            if (np == 1)
            {
                Owned<IFileDescriptor> info;
                try
                {
                    info.setown(file->getFileDescriptor());
                    if(info)
                    {
                        Owned<INode> node = info->getNode(0);
                        if (node)
                        {
                            node->endpoint().getIpText(socket);
                            dest.setDestIP(socket.str());
                        }
                        const char *defaultdir = info->queryDefaultDir();
                        if (defaultdir&&*defaultdir)
                            addPathSepChar(dir.append(defaultdir));
                        file->getRawFileMask(dir);
                        dest.setDestFilePath(dir.str());
                    }
                }
                catch(IException *e)
                {
                    EXCLOG(e,"DeepAssign getFileDescriptor dest");
                    e->Release();
                }
            }
        }
        if (np)
            dest.setDestNumParts(np);
        unsigned rs = file->getRecordSize();
        if (rs)
            dest.setDestRecordSize(rs);

        dest.setCompress(file->isCompressed());
    }
    // monitor stuff
    IConstDFUmonitor *monitor = src->queryMonitor();
    if (monitor) {
        monitor->getEventName(tmp.clear());
        if (tmp.length())
            dest.setMonitorEventName(tmp.str());
        bool sub = monitor->getSub();
        dest.setMonitorSub(sub);
        unsigned sl = monitor->getShotLimit();
        if (sl)
            dest.setMonitorShotLimit(sl);
    }
}

bool CFileSprayEx::ParseLogicalPath(const char * pLogicalPath, const char* groupName, const char* cluster,
                                    StringBuffer &folder, StringBuffer &title, StringBuffer &defaultFolder, StringBuffer &defaultReplicateFolder)
{
    if(!pLogicalPath || !*pLogicalPath)
        return false;

    folder.clear();
    title.clear();

    defaultFolder.clear();
    defaultReplicateFolder.clear();
    DFD_OS os = DFD_OSdefault;

    if(groupName != NULL && *groupName != '\0')
    {
        StringBuffer basedir;
        GroupType groupType;
        Owned<IGroup> group = queryNamedGroupStore().lookup(groupName, basedir, groupType);
        if (group) {
            switch (queryOS(group->queryNode(0).endpoint())) {
            case MachineOsW2K:
                os = DFD_OSwindows; break;
            case MachineOsSolaris:
            case MachineOsLinux:
                os = DFD_OSunix; break;
            }
            if (directories.get())
            {
                switch (groupType)
                {
                case grp_roxie:
                    getConfigurationDirectory(directories, "data", "roxie", cluster, defaultFolder);
                    getConfigurationDirectory(directories, "data2", "roxie", cluster, defaultReplicateFolder);
                    // MORE - should extend to systems with higher redundancy
                    break;
                case grp_hthor:
                    getConfigurationDirectory(directories, "data", "hthor", cluster, defaultFolder);
                    break;
                case grp_thor:
                default:
                    getConfigurationDirectory(directories, "data", "thor", cluster, defaultFolder);
                    getConfigurationDirectory(directories, "mirror", "thor", cluster, defaultReplicateFolder);
                }
            }
        }
        else
        {
            // Error here?
        }
    }

    makePhysicalPartName(pLogicalPath,0,0,folder,false,os,defaultFolder.str());

    const char *n = pLogicalPath;
    const char* p;
    do {
        p = strstr(n,"::");
        if(p)
            n = p+2;
    } while(p);
    title.append(n);
    return true;
}

bool CFileSprayEx::ParseLogicalPath(const char * pLogicalPath, StringBuffer &title)
{
    if(!pLogicalPath || !*pLogicalPath)
        return false;

    title.clear();

    const char *n = pLogicalPath;
    const char* p;
    do {
        p = strstr(n,"::");
        if(p)
            n = p+2;
    } while(p);
    title.append(n);
    return true;
}

void setRoxieClusterPartDiskMapping(const char *clusterName, const char *defaultFolder, const char *defaultReplicateFolder,
                               bool supercopy, IDFUfileSpec *wuFSpecDest, IDFUoptions *wuOptions)
{
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();

    VStringBuffer dirxpath("Software/RoxieCluster[@name=\"%s\"]",clusterName);
    Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
    Owned<IPropertyTreeIterator> processes = pEnvRoot->getElements(dirxpath);
    if (!processes->first())
    {
        DBGLOG("Failed to get RoxieCluster settings");
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "Failed to get RoxieCluster settings. The workunit will not be created.");
    }

    IPropertyTree &process = processes->query();
    const char *slaveConfig = process.queryProp("@slaveConfig");
    if (!slaveConfig || !*slaveConfig)
    {
        DBGLOG("Failed to get RoxieCluster settings");
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "Failed to get RoxieCluster settings. The workunit will not be created.");
    }

    bool replicate =  false;
    unsigned redundancy = 0;       // Number of "spare" copies of the data
    unsigned channelsPerNode = 1;  // Overloaded and cyclic modes
    int replicateOffset = 1;        // Used In cyclic mode only

    unsigned numDataCopies = process.getPropInt("@numDataCopies", 1);

    ClusterPartDiskMapSpec spec;
    spec.setDefaultBaseDir(defaultFolder);
    if (strieq(slaveConfig, "overloaded"))
    {
        channelsPerNode = process.getPropInt("@channelsPernode", 1);
        spec.setDefaultReplicateDir(defaultReplicateFolder);
    }
    else if (strieq(slaveConfig, "full redundancy"))
    {
        redundancy = numDataCopies-1;
        replicateOffset = 0;
        replicate = true;
    }
    else if (strieq(slaveConfig, "cyclic redundancy"))
    {
        redundancy = numDataCopies-1;
        channelsPerNode = numDataCopies;
        replicateOffset = process.getPropInt("@cyclicOffset", 1);
        spec.setDefaultReplicateDir(defaultReplicateFolder);
        replicate = true;
    }
    spec.setRoxie (redundancy, channelsPerNode, replicateOffset);
    if (!supercopy)
        spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
    wuFSpecDest->setClusterPartDiskMapSpec(clusterName,spec);
    wuOptions->setReplicate(replicate);
}

StringBuffer& constructFileMask(const char* filename, StringBuffer& filemask)
{
    filemask.clear().append(filename).toLowerCase().append("._$P$_of_$N$");
    return filemask;
}

bool CFileSprayEx::onDFUWUSearch(IEspContext &context, IEspDFUWUSearchRequest & req, IEspDFUWUSearchResponse & resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Read, ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> environment = factory->openEnvironment();
        Owned<IPropertyTree> root = &environment->getPTree();

        StringArray dfuclusters;
        Owned<IPropertyTreeIterator> clusterIterator = root->getElements("Software/Topology/Cluster");
        if (clusterIterator->first())
        {
            do {
                IPropertyTree &cluster = clusterIterator->query();
                const char *clusterName = cluster.queryProp("@name");
                if (!clusterName || !*clusterName)
                    continue;

                dfuclusters.append(clusterName);
            } while (clusterIterator->next());
        }

        resp.setClusterNames(dfuclusters);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
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

bool CFileSprayEx::GetArchivedDFUWorkunits(IEspContext &context, IEspGetDFUWorkunits &req, IEspGetDFUWorkunitsResponse &resp)
{
    StringBuffer user;
    context.getUserID(user);

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

    SocketEndpoint ep;
    ep.set(sashaAddress,DEFAULT_SASHA_PORT);
    Owned<INode> sashaserver = createINode(ep);

    __int64 count=req.getPageSize();
    if(count < 1)
        count=100;

    __int64 begin=req.getPageStartFrom();
    if (begin < 0)
        begin = 0;

    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_LIST);
    cmd->setOnline(false);
    cmd->setArchived(true);
    cmd->setDFU(true);
   cmd->setLimit((int) count+1);
   cmd->setStart((int)begin);
    if(req.getCluster() && *req.getCluster())
        cmd->setCluster(req.getCluster());
    if(req.getOwner() && *req.getOwner())
        cmd->setOwner(req.getOwner());
    if(req.getJobname() && *req.getJobname())
        cmd->setJobName(req.getJobname());
    if(req.getStateReq() && *req.getStateReq())
        cmd->setState(req.getStateReq());
    cmd->setOutputFormat("owner,jobname,cluster,state,command");//date range/owner/jobname/state*/

    if (!cmd->send(sashaserver))
    {
        StringBuffer url;
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond from: Get archived workUnits.",
            ep.getUrlStr(url).str());
    }

    IArrayOf<IEspDFUWorkunit> results;
    __int64 actualCount = cmd->numIds();
    StringBuffer s;
    for (unsigned j=0;j<actualCount;j++)
    {
        const char *wuidStr = cmd->queryId(j);
        if (!wuidStr)
            continue;

        StringBuffer strArray[6];
        readFromCommaSeparatedString(wuidStr, strArray);

        //skip any workunits without access
        Owned<IEspDFUWorkunit> resultWU = createDFUWorkunit("", "");
        resultWU->setArchived(true);
        if (strArray[0].length() > 0)
            resultWU->setID(strArray[0].str());
        if (strArray[1].length() > 0)
            resultWU->setUser(strArray[1].str());
        if (strArray[2].length() > 0)
            resultWU->setJobName(strArray[2].str());
        if (strArray[3].length() > 0)
            resultWU->setClusterName(strArray[3].str());
        if (strArray[4].length() > 0)
            resultWU->setStateMessage(strArray[4].str());
        if (strArray[5].length() > 0)
            resultWU->setCommand(atoi(strArray[5].str()));

        results.append(*resultWU.getLink());
    }

    resp.setPageStartFrom(begin+1);
    resp.setNextPage(-1);
   if(count < actualCount)
   {
        if (results.length() > count)
        {
            results.pop();
        }
        resp.setNextPage(begin + count);
        resp.setPageEndAt(begin + count);
    }
    else
    {
        resp.setPageEndAt(begin + actualCount);
    }

    if(begin > 0)
    {
      resp.setFirst(false);
        if (begin - count > 0)
            resp.setPrevPage(begin - count);
        else
            resp.setPrevPage(0);
    }

   resp.setPageSize(count);
    resp.setResults(results);

    StringBuffer basicQuery;
    if (req.getStateReq() && *req.getStateReq())
    {
        resp.setStateReq(req.getStateReq());
        addToQueryString(basicQuery, "StateReq", req.getStateReq());
    }
    if (req.getCluster() && *req.getCluster())
    {
        resp.setCluster(req.getCluster());
        addToQueryString(basicQuery, "Cluster", req.getCluster());
    }
    if (req.getOwner() && *req.getOwner())
    {
        resp.setOwner(req.getOwner());
        addToQueryString(basicQuery, "Owner", req.getOwner());
    }
    if (req.getType() && *req.getType())
    {
        resp.setType(req.getType());
        addToQueryString(basicQuery, "Type", req.getType());
    }

    resp.setFilters(basicQuery.str());
    resp.setBasicQuery(basicQuery.str());

    return true;
}

bool CFileSprayEx::getOneDFUWorkunit(IEspContext& context, const char* wuid, IEspGetDFUWorkunitsResponse& resp)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(wuid, false);
    if (!wu)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Dfu workunit %s not found.", wuid);

    Owned<IEspDFUWorkunit> resultWU = createDFUWorkunit();
    resultWU->setID(wuid);
    resultWU->setCommand(wu->getCommand());
    resultWU->setIsProtected(wu->isProtected());

    StringBuffer jobname, user, cluster;
    resultWU->setJobName(wu->getJobName(jobname).str());
    resultWU->setUser(wu->getUser(user).str());

    const char* clusterName = wu->getClusterName(cluster).str();
    if (clusterName && *clusterName)
    {
        Owned<IStringIterator> targets = getTargetClusters(NULL, clusterName);
        if (!targets->first())
            resultWU->setClusterName(clusterName);
        else
        {
            SCMStringBuffer targetCluster;
            targets->str(targetCluster);
            resultWU->setClusterName(targetCluster.str());
        }
    }

    IConstDFUprogress* prog = wu->queryProgress();
    if (prog)
    {
        StringBuffer statemsg;
        DFUstate state = prog->getState();
        encodeDFUstate(state, statemsg);
        resultWU->setState(state);
        resultWU->setStateMessage(statemsg.str());
        resultWU->setPercentDone(prog->getPercentDone());
    }

    IArrayOf<IEspDFUWorkunit> result;
    result.append(*resultWU.getClear());
    resp.setResults(result);
    return true;
}

bool CFileSprayEx::onGetDFUWorkunits(IEspContext &context, IEspGetDFUWorkunits &req, IEspGetDFUWorkunitsResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Read, ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        StringBuffer wuidStr(req.getWuid());
        const char* wuid = wuidStr.trim().str();
        if (wuid && *wuid && looksLikeAWuid(wuid, 'D'))
            return getOneDFUWorkunit(context, wuid, resp);

        double version = context.getClientVersion();
        if (version > 1.02)
        {
            const char *type = req.getType();
            if (type && *type && !stricmp(type, "archived workunits"))
            {
                return GetArchivedDFUWorkunits(context, req, resp);
            }
        }

        StringBuffer clusterReq;
        const char *clusterName = req.getCluster();
        if(clusterName && *clusterName)
        {
            clusterReq.append(clusterName);
        }

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> root = &constEnv->getPTree();

        StringArray targetClusters, clusterProcesses;
        Owned<IPropertyTreeIterator> clusters= root->getElements("Software/Topology/Cluster");
        if (clusters->first())
        {
            do {
                IPropertyTree &cluster = clusters->query();
                const char* name = cluster.queryProp("@name");
                if (!name || !*name)
                    continue;

                Owned<IPropertyTreeIterator> thorClusters= cluster.getElements(eqThorCluster);
                Owned<IPropertyTreeIterator> roxieClusters= cluster.getElements(eqRoxieCluster);
                if (thorClusters->first() || roxieClusters->first())
                {
                    bool bFound = false;
                    if (thorClusters->first())
                    {
                        IPropertyTree &thorCluster = thorClusters->query();
                        const char* process = thorCluster.queryProp("@process");
                        if (process && *process)
                        {
                            targetClusters.append(name);
                            clusterProcesses.append(process);
                            if (clusterName && !stricmp(clusterName, name))
                            {
                                clusterReq.clear().append(process);
                            }
                        }
                    }

                    if (!bFound && roxieClusters->first())
                    {
                        IPropertyTree &roxieCluster = roxieClusters->query();
                        const char* process = roxieCluster.queryProp("@process");
                        if (process && *process)
                        {
                            targetClusters.append(name);
                            clusterProcesses.append(process);
                            if (clusterName && !stricmp(clusterName, name))
                            {
                                clusterReq.clear().append(process);
                            }
                        }
                    }
                }
            } while (clusters->next());
        }

        __int64 pagesize = req.getPageSize();
        __int64 pagefrom = req.getPageStartFrom();
        __int64 displayFrom = 0;
        if (pagesize < 1)
        {
            pagesize = 100;
        }
        if (pagefrom > 0)
        {
            displayFrom = pagefrom;
        }

        DFUsortfield sortorder[2] = {DFUsf_wuid, DFUsf_term};
        sortorder[0] = (DFUsortfield) (DFUsf_wuid + DFUsf_reverse);

        if(req.getSortby() && *req.getSortby())
        {
            const char *sortby = req.getSortby();
            if (!stricmp(sortby, "Owner"))
                sortorder[0] = DFUsf_user;
            else if (!stricmp(sortby, "JobName"))
                sortorder[0] = DFUsf_job;
            else if (!stricmp(sortby, "Cluster"))
                sortorder[0] = DFUsf_cluster;
            else if (!stricmp(sortby, "State"))
                sortorder[0] = DFUsf_state;
            else if (!stricmp(sortby, "Type"))
                sortorder[0] = DFUsf_command;
            else if (!stricmp(sortby, "Protected"))
                sortorder[0] = DFUsf_protected;
            else if (!stricmp(sortby, "PCTDone"))
                sortorder[0] = (DFUsortfield) (DFUsf_pcdone | DFUsf_numeric);
            else
                sortorder[0] = DFUsf_wuid;

            bool descending = req.getDescending();
            if (descending)
                sortorder[0] = (DFUsortfield) (sortorder[0] | DFUsf_reverse);
        }

        DFUsortfield filters[10];
        unsigned short filterCount = 0;
        MemoryBuffer filterbuf;

        if(req.getStateReq() && *req.getStateReq())
        {
            filters[filterCount] = DFUsf_state;
            filterCount++;
            if (stricmp(req.getStateReq(), "unknown") != 0)
                filterbuf.append(req.getStateReq());
            else
                filterbuf.append("");
        }

        if(wuid && *wuid)
        {
            filters[filterCount] = DFUsf_wildwuid;
            filterCount++;
            filterbuf.append(wuid);
        }

        if(clusterName && *clusterName)
        {
            filters[filterCount] = DFUsf_cluster;
            filterCount++;
            filterbuf.append(clusterReq.str());
        }

        if(req.getOwner() && *req.getOwner())
        {
            filters[filterCount] = DFUsortfield (DFUsf_user | DFUsf_nocase);
            filterCount++;
            filterbuf.append(req.getOwner());
        }

        if(req.getJobname() && *req.getJobname())
        {
            filters[filterCount] = DFUsortfield (DFUsf_job | DFUsf_nocase);
            filterCount++;
            filterbuf.append(req.getJobname());
        }

        filters[filterCount] = DFUsf_term;

        __int64 cacheHint = req.getCacheHint();
        if (cacheHint < 0) //Not set yet
            cacheHint = 0;

        IArrayOf<IEspDFUWorkunit> result;
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        unsigned numWUs;
        PROGLOG("GetDFUWorkunits: getWorkUnitsSorted");
        Owned<IConstDFUWorkUnitIterator> itr = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), (int) displayFrom, (int) pagesize+1, req.getOwner(), &cacheHint, &numWUs);
        if (version >= 1.07)
            resp.setCacheHint(cacheHint);
        PROGLOG("GetDFUWorkunits: getWorkUnitsSorted done");

        //unsigned actualCount = 0;
        itr->first();
        while(itr->isValid())
        {
            Owned<IConstDFUWorkUnit> wu = itr->get();
            //actualCount++;

            Owned<IEspDFUWorkunit> resultWU = createDFUWorkunit("", "");
            resultWU->setID(wu->queryId());
            StringBuffer jobname, user, cluster;
            resultWU->setJobName(wu->getJobName(jobname).str());
            resultWU->setCommand(wu->getCommand());
            resultWU->setUser(wu->getUser(user).str());

            const char* clusterName = wu->getClusterName(cluster).str();
            if (clusterName)
            {
                StringBuffer clusterForDisplay(clusterName);

                if (clusterProcesses.ordinality())
                {
                    for (unsigned i = 0; i < clusterProcesses.length(); i++)
                    {
                        const char* clusterProcessName = clusterProcesses.item(i);
                        if (!stricmp(clusterProcessName, clusterName))
                        {
                            clusterForDisplay.clear().append(targetClusters.item(i));
                            break;
                        }
                    }
                }
                resultWU->setClusterName(clusterForDisplay.str());
            }

            resultWU->setIsProtected(wu->isProtected());
            IConstDFUprogress *prog = wu->queryProgress();
            if (prog != NULL)
            {
                DFUstate state = prog->getState();
                resultWU->setState(state);
                StringBuffer statemsg;
                encodeDFUstate(state,statemsg);
                resultWU->setStateMessage(statemsg.str());
                resultWU->setPercentDone(prog->getPercentDone());
            }
            result.append(*resultWU.getLink());
            itr->next();
        }

        if (result.length() > pagesize)
            result.pop();

        resp.setPageSize(pagesize);
        resp.setNumWUs(numWUs);
        resp.setPageStartFrom(displayFrom + 1);

        if(displayFrom + pagesize < numWUs)
        {
            resp.setNextPage(displayFrom + pagesize);
            resp.setPageEndAt(pagefrom + pagesize);
            __int64 last = displayFrom + pagesize;
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

        if(displayFrom > 0)
        {
            resp.setFirst(false);
            if (displayFrom - pagesize > 0)
                resp.setPrevPage(displayFrom - pagesize);
            else
                resp.setPrevPage(0);
        }

        StringBuffer basicQuery;
        if (req.getStateReq() && *req.getStateReq())
        {
            resp.setStateReq(req.getStateReq());
            addToQueryString(basicQuery, "StateReq", req.getStateReq());
        }
        if (req.getCluster() && *req.getCluster())
        {
            resp.setCluster(req.getCluster());
            addToQueryString(basicQuery, "Cluster", req.getCluster());
        }
        if (req.getOwner() && *req.getOwner())
        {
            resp.setOwner(req.getOwner());
            addToQueryString(basicQuery, "Owner", req.getOwner());
        }
        resp.setFilters(basicQuery.str());

        if (req.getSortby() && *req.getSortby())
        {
            resp.setSortby(req.getSortby());
            if (req.getDescending())
                resp.setDescending(req.getDescending());

            StringBuffer strbuf(req.getSortby());
            strbuf.append("=");
            String str1(strbuf.str());
            String str(basicQuery.str());
            if (str.indexOf(str1) < 0)
            {
                addToQueryString(basicQuery, "Sortby", req.getSortby());
                if (req.getDescending())
                    addToQueryString(basicQuery, "Descending", "1");
            }
        }

        resp.setBasicQuery(basicQuery.str());

        resp.setResults(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CFileSprayEx::addToQueryString(StringBuffer &queryString, const char *name, const char *value)
{
    if (queryString.length() > 0)
    {
        queryString.append("&amp;");
    }

    queryString.append(name);
    queryString.append("=");
    queryString.append(value);
}

void CFileSprayEx::getInfoFromSasha(IEspContext &context, const char *sashaServer, const char* wuid, IEspDFUWorkunit *info)
{
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->addId(wuid);
    cmd->setAction(SCA_GET);
    cmd->setArchived(true);
    cmd->setDFU(true);
    SocketEndpoint ep(sashaServer, DEFAULT_SASHA_PORT);
    Owned<INode> node = createINode(ep);
    if (!cmd->send(node,1*60*1000))
    {
        StringBuffer url;
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond from: Get information for %s.",
            ep.getUrlStr(url).str(), wuid);
    }
    if (cmd->numIds()==0)
    {
        DBGLOG("Could not read archived %s",wuid);
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot read workunit %s.",wuid);
    }

    unsigned num = cmd->numResults();
    if (num < 1)
        return;

    StringBuffer res;
    cmd->getResult(0,res);
    if(res.length() < 1)
        return;

    Owned<IPropertyTree> wu = createPTreeFromXMLString(res.str());
    if (!wu)
        return;

    const char * command = wu->queryProp("@command");
    const char * submitID = wu->queryProp("@submitID");
    const char * cluster = wu->queryProp("@clusterName");
    const char * queue = wu->queryProp("@queue");
    const char * jobName = wu->queryProp("@jobName");
    const char * protectedWU = wu->queryProp("@protected");

    info->setID(wuid);
    info->setArchived(true);
    if (command && *command)
        info->setCommandMessage(command);
    if (cluster && *cluster)
        info->setClusterName(cluster);
    if (submitID && *submitID)
        info->setUser(submitID);
    if (queue && *queue)
        info->setQueue(queue);
    if (jobName && *jobName)
        info->setJobName(jobName);
    if (protectedWU && stricmp(protectedWU, "0"))
        info->setIsProtected(true);
    else
        info->setIsProtected(false);

    IPropertyTree *source = wu->queryPropTree("Source");
   if(source)
    {
        const char * directory = source->queryProp("@directory");
        const char * name = source->queryProp("@name");
        if (directory && *directory)
            info->setSourceDirectory(directory);
        if (name && *name)
            info->setSourceLogicalName(name);
    }

    IPropertyTree *dest = wu->queryPropTree("Destination");
   if(dest)
    {
        const char * directory = dest->queryProp("@directory");
        int numParts = dest->getPropInt("@numparts", -1);
        if (directory && *directory)
            info->setDestDirectory(directory);
        if (numParts > 0)
            info->setDestNumParts(numParts);
    }

    IPropertyTree *progress = wu->queryPropTree("Progress");
   if(progress)
    {
        const char * state = progress->queryProp("@state");
        const char * timeStarted = progress->queryProp("@timestarted");
        const char * timeStopped = progress->queryProp("@timestopped");

        if (state && *state)
            info->setStateMessage(state);
        if (timeStarted && *timeStarted)
        {
            StringBuffer startStr(timeStarted);
            startStr.replace('T', ' ');
            info->setTimeStarted(startStr.str());
        }
        if (timeStopped && *timeStopped)
        {
            StringBuffer stopStr(timeStopped);
            stopStr.replace('T', ' ');
            info->setTimeStopped(stopStr.str());
        }
    }
    return;
}


bool CFileSprayEx::getArchivedWUInfo(IEspContext &context, IEspGetDFUWorkunit &req, IEspGetDFUWorkunitResponse &resp)
{
    const char *wuid = req.getWuid();
    if (wuid && *wuid)
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
            throw MakeStringException(ECLWATCH_ARCHIVE_SERVER_NOT_FOUND,"Archive server not found.");
        }

        getInfoFromSasha(context, sashaAddress.str(), wuid, &resp.updateResult());
        return true;
    }

    return false;
}

bool CFileSprayEx::onGetDFUWorkunit(IEspContext &context, IEspGetDFUWorkunit &req, IEspGetDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Read, ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        const char* wuid = req.getWuid();
        if (!wuid || !*wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Dfu workunit ID not specified.");

        bool found = false;
        double version = context.getClientVersion();
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(wuid, false);
        if(wu)
        {
            IEspDFUWorkunit &result = resp.updateResult();
            PROGLOG("GetDFUWorkunit: %s", wuid);
            DeepAssign(context, wu, result);
            int n = resp.getResult().getState();
            if (n == DFUstate_scheduled || n == DFUstate_queued || n == DFUstate_started)
            {
                 resp.setAutoRefresh(WUDETAILS_REFRESH_MINS);
            }

            found = true;
        }
        else if ((version > 1.02) && getArchivedWUInfo(context, req, resp))
        {
            found = true;
        }

        if (!found)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onGetDFUProgress(IEspContext &context, IEspProgressRequest &req, IEspProgressResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Read, ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        const char* wuid = req.getWuid();
        if(!wuid || !*wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not specified.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(req.getWuid(), false);
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

        resp.setWuid(req.getWuid());

        PROGLOG("GetDFUProgress: %s", wuid);
        IConstDFUprogress *prog = wu->queryProgress();
        if (prog)
        {
            resp.setPercentDone(prog->getPercentDone());
            resp.setKbPerSec(prog->getKbPerSec());
            resp.setKbPerSecAve(prog->getKbPerSecAve());
            resp.setSecsLeft(prog->getSecsLeft());

            StringBuffer statestr;
            encodeDFUstate(prog->getState(), statestr);
            resp.setState(statestr.str());

            resp.setSlavesDone(prog->getSlavesDone());

            StringBuffer msg;
            prog->formatProgressMessage(msg);
            resp.setProgressMessage(msg.str());
            prog->formatSummaryMessage(msg.clear());
            resp.setSummaryMessage(msg.str());
            prog->getTimeTaken(msg.clear());
            resp.setTimeTaken(msg.str());
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onCreateDFUWorkunit(IEspContext &context, IEspCreateDFUWorkunit &req, IEspCreateDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to create DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        wu->commit();
        const char * d = wu->queryId();
        IEspDFUWorkunit &result = resp.updateResult();
        DeepAssign(context, wu, result);
        result.setOverwrite(false);
        result.setReplicate(true);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onUpdateDFUWorkunit(IEspContext &context, IEspUpdateDFUWorkunit &req, IEspUpdateDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to update DFU workunit. Permission denied.");

        IConstDFUWorkunit & reqWU = req.getWu();
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(reqWU.getID());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Dfu workunit %s not found.", reqWU.getID());

        PROGLOG("UpdateDFUWorkunit: %s", reqWU.getID());
        IDFUprogress *prog = wu->queryUpdateProgress();
        if (prog && req.getStateOrig() != reqWU.getState())
        {
            if (prog->getState() != req.getStateOrig())
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot update DFU workunit %s because its state has been changed internally. Please refresh the page and try again.",reqWU.getID());

            prog->setState((enum DFUstate)reqWU.getState());
        }

        const char* clusterOrig = req.getClusterOrig();
        const char* cluster = reqWU.getClusterName();
        if(cluster && (!clusterOrig || stricmp(clusterOrig, cluster)))
        {
            wu->setClusterName(reqWU.getClusterName());
        }

        const char* jobNameOrig = req.getJobNameOrig();
        const char* jobName = reqWU.getJobName();
        if(jobName && (!jobNameOrig || stricmp(jobNameOrig, jobName)))
        {
            wu->setJobName(jobName);
        }

        if (reqWU.getIsProtected() != req.getIsProtectedOrig())
            wu->protect(reqWU.getIsProtected());

        wu->commit();

        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(reqWU.getID()).str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool markWUFailed(IDFUWorkUnitFactory *f, const char *wuid)
{
    Owned<IDFUWorkUnit> wu = f->updateWorkUnit(wuid);
    if(!wu)
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Dfu workunit %s not found.", wuid);

    IDFUprogress *prog = wu->queryUpdateProgress();
    if(!prog)
        throw MakeStringException(ECLWATCH_PROGRESS_INFO_NOT_FOUND, "progress information not found for workunit %s.", wuid);
    else if(prog->getState() == DFUstate_started)
        throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "Cannot delete workunit %s because its state is Started.", wuid);
    else
    {
        prog->setState(DFUstate_failed);
        return true;
    }
    return false;
}

static unsigned NumOfDFUWUActionNames = 6;
static const char *DFUWUActionNames[] = { "Delete", "Protect" , "Unprotect" , "Restore" , "SetToFailed", "Archive" };

bool CFileSprayEx::onDFUWorkunitsAction(IEspContext &context, IEspDFUWorkunitsActionRequest &req, IEspDFUWorkunitsActionResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to update DFU workunit. Permission denied.");

        CDFUWUActions action = req.getType();
        if (action == DFUWUActions_Undefined)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Action not defined.");
        const char* actionStr = (action < NumOfDFUWUActionNames) ? DFUWUActionNames[action] : "Unknown";

        StringArray& wuids = req.getWuids();
        if (!wuids.ordinality())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit not defined.");

        if ((action == CDFUWUActions_Restore) || (action == CDFUWUActions_Archive))
        {
            StringBuffer msg;
            ForEachItemIn(i, wuids)
            {
                StringBuffer wuidStr(wuids.item(i));
                const char* wuid = wuidStr.trim().str();
                if (isEmptyString(wuid))
                    msg.appendf("Empty Workunit ID at %u. ", i);
            }
            if (!msg.isEmpty())
                throw makeStringException(ECLWATCH_INVALID_INPUT, msg);

            Owned<ISashaCommand> cmd = archiveOrRestoreWorkunits(wuids, nullptr, action == CDFUWUActions_Archive, true);
            IArrayOf<IEspDFUActionResult> results;
            ForEachItemIn(x, wuids)
            {
                Owned<IEspDFUActionResult> res = createDFUActionResult();
                res->setID(wuids.item(x));
                res->setAction(actionStr);

                StringBuffer reply;
                if (action == CDFUWUActions_Restore)
                    reply.set("Restore ID: ");
                else
                    reply.set("Archive ID: ");

                if (cmd->getId(x, reply))
                    res->setResult(reply.str());
                else
                    res->setResult("Failed to get Archive/restore ID.");

                results.append(*res.getLink());
            }
            resp.setDFUActionResults(results);
            return true;
        }

        IArrayOf<IEspDFUActionResult> results;
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        for(unsigned i = 0; i < wuids.ordinality(); ++i)
        {
            const char* wuid = wuids.item(i);

            Owned<IEspDFUActionResult> res = createDFUActionResult("", "");
            res->setID(wuid);
            res->setAction(actionStr);

            try
            {
                PROGLOG("%s %s", actionStr, wuid);
                switch (action)
                {
                case CDFUWUActions_Delete:
                    if (!markWUFailed(factory, wuid))
                        throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "Failed to mark workunit failed.");
                    if (!factory->deleteWorkUnit(wuid))
                        throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "Failed in deleting workunit.");
                    res->setResult("Success");
                    break;
                case CDFUWUActions_Protect:
                case CDFUWUActions_Unprotect:
                case CDFUWUActions_SetToFailed:
                    Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(wuid);
                    if(!wu.get())
                        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Failed in calling updateWorkUnit().");
                    switch (action)
                    {
                    case CDFUWUActions_Protect:
                        wu->protect(true);
                        break;
                    case CDFUWUActions_Unprotect:
                        wu->protect(false);
                        break;
                    case CDFUWUActions_SetToFailed:
                        IDFUprogress *prog = wu->queryUpdateProgress();
                        if (!prog)
                            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Failed in calling queryUpdateProgress().");
                        prog->setState(DFUstate_failed);
                        break;
                    }
                    wu->commit();
                    res->setResult("Success");
                    break;
                }
                PROGLOG("%s %s done", actionStr, wuid);
            }
            catch (IException *e)
            {
                StringBuffer eMsg, failedMsg("Failed: ");
                res->setResult(failedMsg.append(e->errorMessage(eMsg)).str());
                e->Release();
            }

            results.append(*res.getLink());
        }

        resp.setDFUActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onDeleteDFUWorkunits(IEspContext &context, IEspDeleteDFUWorkunits &req, IEspDeleteDFUWorkunitsResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to delete DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        StringArray & wuids = req.getWuids();
        for(unsigned i = 0; i < wuids.ordinality(); ++i)
        {
            const char* wuid = wuids.item(i);
            if (markWUFailed(factory, wuid))
            {
                factory->deleteWorkUnit(wuid);
                PROGLOG("DeleteDFUWorkunits: %s deleted", wuid);
            }
        }
        resp.setRedirectUrl("/FileSpray/GetDFUWorkunits");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onDeleteDFUWorkunit(IEspContext &context, IEspDeleteDFUWorkunit &req, IEspDeleteDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to delete DFU workunit. Permission denied.");

        const char* wuid = req.getWuid();
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        if (markWUFailed(factory, wuid))
        {
            resp.setResult(factory->deleteWorkUnit(wuid));
            PROGLOG("DeleteDFUWorkunit: %s deleted", wuid);
        }
        else
            resp.setResult(false);

        resp.setRedirectUrl("/FileSpray/GetDFUWorkunits");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onSubmitDFUWorkunit(IEspContext &context, IEspSubmitDFUWorkunit &req, IEspSubmitDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to submit DFU workunit. Permission denied.");

        if (!req.getWuid() || !*req.getWuid())
            throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Workunit ID required");
        PROGLOG("SubmitDFUWorkunit: %s", req.getWuid());

        submitDFUWorkUnit(req.getWuid());

        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(req.getWuid()).str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onAbortDFUWorkunit(IEspContext &context, IEspAbortDFUWorkunit &req, IEspAbortDFUWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to abort DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(req.getWuid());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

        PROGLOG("AbortDFUWorkunit: %s", req.getWuid());
        wu->requestAbort();
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(req.getWuid()).str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onGetDFUExceptions(IEspContext &context, IEspGetDFUExceptions &req, IEspGetDFUExceptionsResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_EX_URL, SecAccess_Read, ECLWATCH_DFU_EX_ACCESS_DENIED, "Failed to get DFU Exceptions. Permission denied.");

        IArrayOf<IEspDFUException> result;
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(req.getWuid());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

        PROGLOG("GetDFUExceptions: %s", req.getWuid());
        Owned<IExceptionIterator> itr = wu->getExceptionIterator();
        itr->first();
        while(itr->isValid())
        {
            Owned<IEspDFUException> resultE = createDFUException("", "");
            IException &e = itr->query();
            resultE->setCode(e.errorCode());
            StringBuffer msg;
            resultE->setMessage(e.errorMessage(msg).str());
            result.append(*resultE.getLink());
            itr->next();
        }

        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CFileSprayEx::readAndCheckSpraySourceReq(MemoryBuffer& srcxml, const char* srcIP, const char* srcPath,
    StringBuffer& sourceIPReq, StringBuffer& sourcePathReq)
{
    StringBuffer sourcePath(srcPath);
    sourceIPReq.set(srcIP);
    sourceIPReq.trim();
    sourcePath.trim();
    if(srcxml.length() == 0)
    {
        if (sourceIPReq.isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source network IP not specified.");
        if (sourcePath.isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source path not specified.");
    }
    getStandardPosixPath(sourcePathReq, sourcePath.str());
}

bool CFileSprayEx::onSprayFixed(IEspContext &context, IEspSprayFixed &req, IEspSprayFixedResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Spray. Permission denied.");

        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;

        const char* destNodeGroup = req.getDestGroup();
        if(destNodeGroup == NULL || *destNodeGroup == '\0')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination node group not specified.");

        MemoryBuffer& srcxml = (MemoryBuffer&)req.getSrcxml();
        StringBuffer sourceIPReq, sourcePathReq;
        readAndCheckSpraySourceReq(srcxml, req.getSourceIP(), req.getSourcePath(), sourceIPReq, sourcePathReq);
        const char* srcip = sourceIPReq.str();
        const char* srcfile = sourcePathReq.str();
        const char* destname = req.getDestLogicalName();
        if(!destname || !*destname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid destination filename:'%s'", destname);
        destname = lfn.get();
        PROGLOG("SprayFixed: DestLogicalName %s, DestGroup %s", destname, destNodeGroup);

        StringBuffer gName, ipAddr;
        const char *pTr = strchr(destNodeGroup, ' ');
        if (pTr)
        {
            gName.append(pTr - destNodeGroup, destNodeGroup);
            ipAddr.append(pTr+1);
        }
        else
            gName.append(destNodeGroup);

        if (ipAddr.length() > 0)
            ParseLogicalPath(destname, ipAddr.str(), NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);
        else
            ParseLogicalPath(destname, destNodeGroup, NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        wu->setClusterName(gName.str());

        wu->setJobName(destTitle.str());
        const char * dfuQueue = req.getDFUServerQueue();
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        if (!isEmptyString(dfuQueue))
        {
            if (!constEnv->isValidDfuQueueName(dfuQueue))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid DFU server queue name:'%s'", dfuQueue);
        }
        setDFUServerQueueReq(dfuQueue, wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        if(srcxml.length() == 0)
        {
            RemoteMultiFilename rmfn;
            SocketEndpoint ep(srcip);
            if (ep.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "SprayFixed to %s: cannot resolve source network IP from %s.", destname, srcip);

            rmfn.setEp(ep);
            StringBuffer fnamebuf(srcfile);
            fnamebuf.trim();
            rmfn.append(fnamebuf.str());    // handles comma separated files
            source->setMultiFilename(rmfn);
        }
        else
        {
            srcxml.append('\0');
            source->setFromXML((const char*)srcxml.toByteArray());
        }

        IDFUfileSpec *destination = wu->queryUpdateDestination();
        bool nosplit = req.getNosplit();
        int recordsize = req.getSourceRecordSize();
        const char* format = req.getSourceFormat();
        if ((recordsize == RECFMVB_RECSIZE_ESCAPE) || (format && strieq(format, "recfmvb")))
        {//recordsize may be set by dfuplus; format may be set by EclWatch
            source->setFormat(DFUff_recfmvb);
            destination->setFormat(DFUff_variable);
        }
        else if ((recordsize == RECFMV_RECSIZE_ESCAPE) || (format && strieq(format, "recfmv")))
        {
            source->setFormat(DFUff_recfmv);
            destination->setFormat(DFUff_variable);
        }
        else if ((recordsize == PREFIX_VARIABLE_RECSIZE_ESCAPE) || (format && strieq(format, "variable")))
        {
            source->setFormat(DFUff_variable);
            destination->setFormat(DFUff_variable);
        }
        else if((recordsize == PREFIX_VARIABLE_BIGENDIAN_RECSIZE_ESCAPE) || (format && strieq(format, "variablebigendian")))
        {
            source->setFormat(DFUff_variablebigendian);
            destination->setFormat(DFUff_variable);
        }
        else if(recordsize == 0 && !nosplit)             // -ve record sizes for blocked
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid record size");
        else
            source->setRecordSize(recordsize);

        destination->setLogicalName(destname);
        destination->setDirectory(destFolder.str());

        StringBuffer fileMask;
        constructFileMask(destTitle.str(), fileMask);
        destination->setFileMask(fileMask.str());
        destination->setGroupName(gName.str());
        const char * encryptkey = req.getEncrypt();
        if(req.getCompress()||(encryptkey&&*encryptkey))
            destination->setCompressed(true);

        ClusterPartDiskMapSpec mspec;
        destination->getClusterPartDiskMapSpec(gName.str(), mspec);
        mspec.setDefaultBaseDir(defaultFolder.str());
        mspec.setDefaultReplicateDir(defaultReplicateFolder.str());
        destination->setClusterPartDiskMapSpec(gName.str(), mspec);

        int repo = req.getReplicateOffset();
        bool isNull = req.getReplicateOffset_isNull();
        if (!isNull && (repo!=1))
            destination->setReplicateOffset(repo);
        if (req.getWrap())
            destination->setWrap(true);

        IDFUoptions *options = wu->queryUpdateOptions();
        const char * decryptkey = req.getDecrypt();
        if ((encryptkey&&*encryptkey)||(decryptkey&&*decryptkey))
            options->setEncDec(encryptkey,decryptkey);
        options->setReplicate(req.getReplicate());
        options->setOverwrite(req.getOverwrite());             // needed if target already exists
        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            options->setLengthPrefix(prefix);
        if(req.getNosplit())
            options->setNoSplit(true);
        if(req.getNorecover())
            options->setNoRecover(true);
        if(req.getMaxConnections() > 0)
            options->setmaxConnections(req.getMaxConnections());
        if(req.getThrottle() > 0)
            options->setThrottle(req.getThrottle());
        if(req.getTransferBufferSize() > 0)
            options->setTransferBufferSize(req.getTransferBufferSize());
        if (req.getPull())
            options->setPull(true);
        if (req.getPush())
            options->setPush(true);
        if (!req.getNoCommon_isNull())
            options->setNoCommon(req.getNoCommon());

        if (req.getFailIfNoSourceFile())
            options->setFailIfNoSourceFile(true);

        if (req.getRecordStructurePresent())
            options->setRecordStructurePresent(true);

        if (!req.getExpireDays_isNull())
            options->setExpireDays(req.getExpireDays());

        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onSprayVariable(IEspContext &context, IEspSprayVariable &req, IEspSprayResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Spray. Permission denied.");

        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;

        const char* destNodeGroup = req.getDestGroup();
        if(destNodeGroup == NULL || *destNodeGroup == '\0')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination node group not specified.");

        StringBuffer gName, ipAddr;
        const char *pTr = strchr(destNodeGroup, ' ');
        if (pTr)
        {
            gName.append(pTr - destNodeGroup, destNodeGroup);
            ipAddr.append(pTr+1);
        }
        else
            gName.append(destNodeGroup);

        MemoryBuffer& srcxml = (MemoryBuffer&)req.getSrcxml();
        StringBuffer sourceIPReq, sourcePathReq;
        readAndCheckSpraySourceReq(srcxml, req.getSourceIP(), req.getSourcePath(), sourceIPReq, sourcePathReq);
        const char* srcip = sourceIPReq.str();
        const char* srcfile = sourcePathReq.str();
        const char* destname = req.getDestLogicalName();
        if(!destname || !*destname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid destination filename:'%s'", destname);

        destname = lfn.get();
        PROGLOG("SprayVariable: DestLogicalName %s, DestGroup %s", destname, destNodeGroup);

        if (ipAddr.length() > 0)
            ParseLogicalPath(destname, ipAddr.str(), NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);
        else
            ParseLogicalPath(destname, destNodeGroup, NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        wu->setClusterName(gName.str());
        wu->setJobName(destTitle.str());

        const char * dfuQueue = req.getDFUServerQueue();
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        if (!isEmptyString(dfuQueue))
        {
            if (!constEnv->isValidDfuQueueName(dfuQueue))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid DFU server queue name:'%s'", dfuQueue);
        }
        setDFUServerQueueReq(dfuQueue, wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();

        if(srcxml.length() == 0)
        {
            RemoteMultiFilename rmfn;
            SocketEndpoint ep(srcip);
            if (ep.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "SprayVariable to %s: cannot resolve source network IP from %s.", destname, srcip);

            rmfn.setEp(ep);
            StringBuffer fnamebuf(srcfile);
            fnamebuf.trim();
            rmfn.append(fnamebuf.str());    // handles comma separated files
            source->setMultiFilename(rmfn);
        }
        else
        {
            srcxml.append('\0');
            source->setFromXML((const char*)srcxml.toByteArray());
        }
        source->setMaxRecordSize(req.getSourceMaxRecordSize());
        source->setFormat((DFUfileformat)req.getSourceFormat());

        StringBuffer rowtag;
        if (req.getIsJSON())
        {
            const char *srcRowPath = req.getSourceRowPath();
            if (!srcRowPath || *srcRowPath != '/')
                rowtag.append("/");
            rowtag.append(srcRowPath);
        }
        else
            rowtag.append(req.getSourceRowTag());

        // if rowTag specified, it means it's xml or json format, otherwise it's csv
        if(rowtag.length())
        {
            source->setRowTag(rowtag);
            options->setKeepHeader(true);
        }
        else
        {
            const char* cs = req.getSourceCsvSeparate();
            if (req.getNoSourceCsvSeparator())
            {
                cs = "";
            }
            else if(cs == NULL || *cs == '\0')
                cs = "\\,";

            const char* ct = req.getSourceCsvTerminate();
            if(ct == NULL || *ct == '\0')
                ct = "\\n,\\r\\n";
            const char* cq = req.getSourceCsvQuote();
            if(cq== NULL)
                cq = "\"";
            source->setCsvOptions(cs, ct, cq, req.getSourceCsvEscape(), req.getQuotedTerminator());

            options->setQuotedTerminator(req.getQuotedTerminator());
        }

        destination->setLogicalName(destname);
        destination->setDirectory(destFolder.str());
        StringBuffer fileMask;
        constructFileMask(destTitle.str(), fileMask);
        destination->setFileMask(fileMask.str());
        destination->setGroupName(gName.str());
        ClusterPartDiskMapSpec mspec;
        destination->getClusterPartDiskMapSpec(gName.str(), mspec);
        mspec.setDefaultBaseDir(defaultFolder.str());
        mspec.setDefaultReplicateDir(defaultReplicateFolder.str());
        destination->setClusterPartDiskMapSpec(gName.str(), mspec);
        const char * encryptkey = req.getEncrypt();
        if(req.getCompress()||(encryptkey&&*encryptkey))
            destination->setCompressed(true);
        const char * decryptkey = req.getDecrypt();
        if ((encryptkey&&*encryptkey)||(decryptkey&&*decryptkey))
            options->setEncDec(encryptkey,decryptkey);
        int repo = req.getReplicateOffset();
        bool isNull = req.getReplicateOffset_isNull();
        if (!isNull && (repo!=1))
            destination->setReplicateOffset(repo);

        options->setReplicate(req.getReplicate());
        options->setOverwrite(req.getOverwrite());             // needed if target already exists
        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            options->setLengthPrefix(prefix);
        if(req.getNosplit())
            options->setNoSplit(true);
        if(req.getNorecover())
            options->setNoRecover(true);
        if(req.getMaxConnections() > 0)
            options->setmaxConnections(req.getMaxConnections());
        if(req.getThrottle() > 0)
            options->setThrottle(req.getThrottle());
        if(req.getTransferBufferSize() > 0)
            options->setTransferBufferSize(req.getTransferBufferSize());
        if (req.getPull())
            options->setPull(true);
        if (req.getPush())
            options->setPush(true);

        if (req.getFailIfNoSourceFile())
            options->setFailIfNoSourceFile(true);

        if (req.getRecordStructurePresent())
            options->setRecordStructurePresent(true);

        if (!req.getExpireDays_isNull())
            options->setExpireDays(req.getExpireDays());

        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onReplicate(IEspContext &context, IEspReplicate &req, IEspReplicateResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Replicate. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");

        PROGLOG("Replicate %s", srcname);
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        StringBuffer jobname("Replicate: ");
        jobname.append(srcname);
        wu->setJobName(jobname.str());
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_replicate);

        IDFUfileSpec *source = wu->queryUpdateSource();
        if (source)
        {
            source->setLogicalName(srcname);
            int repo = req.getReplicateOffset();
            if (repo!=1)
                source->setReplicateOffset(repo);
        }
        const char* cluster = req.getCluster();
        if(cluster && *cluster)
        {
            IDFUoptions *opt = wu->queryUpdateOptions();
            opt->setReplicateMode(DFURMmissing,cluster,req.getRepeatLast(),req.getOnlyRepeated());
        }
        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CFileSprayEx::getDropZoneInfoByIP(double clientVersion, const char* ip, const char* destFileIn, StringBuffer& destFileOut, StringBuffer& umask)
{
    if (destFileIn && *destFileIn)
        destFileOut.set(destFileIn);

    if (!ip || !*ip)
        throw MakeStringExceptionDirect(ECLWATCH_INVALID_IP, "Network address must be specified for a drop zone!");

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = factory->openEnvironment();

    StringBuffer destFile;
    if (isAbsolutePath(destFileIn))
    {
        destFile.set(destFileIn);
        Owned<IConstDropZoneInfo> dropZone = constEnv->getDropZoneByAddressPath(ip, destFile.str());
        if (!dropZone)
        {
            if (constEnv->isDropZoneRestrictionEnabled())
                throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "No drop zone configured for '%s' and '%s'. Check your system drop zone configuration.", ip, destFile.str());
            else
            {
                LOG(MCdebugInfo, unknownJob, "No drop zone configured for '%s' and '%s'. Check your system drop zone configuration.", ip, destFile.str());
                return;
            }
        }


        SCMStringBuffer directory, maskBuf;
        dropZone->getDirectory(directory);
        destFileOut.set(destFile.str());
        dropZone->getUMask(maskBuf);
        if (maskBuf.length())
            umask.set(maskBuf.str());

        return;
    }

    Owned<IConstDropZoneInfoIterator> dropZoneItr = constEnv->getDropZoneIteratorByAddress(ip);
    if (dropZoneItr->count() < 1)
    {
        if (constEnv->isDropZoneRestrictionEnabled())
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Drop zone not found for network address '%s'. Check your system drop zone configuration.", ip);
        else
        {
            LOG(MCdebugInfo, unknownJob, "Drop zone not found for network address '%s'. Check your system drop zone configuration.", ip);
            return;
        }
    }

    bool dzFound = false;
    ForEach(*dropZoneItr)
    {
        IConstDropZoneInfo& dropZoneInfo = dropZoneItr->query();

        SCMStringBuffer dropZoneDirectory, dropZoneUMask;
        dropZoneInfo.getDirectory(dropZoneDirectory);
        dropZoneInfo.getUMask(dropZoneUMask);
        if (!dropZoneDirectory.length())
            continue;

        if (!dzFound)
        {
            dzFound = true;
            destFileOut.set(dropZoneDirectory.str());
            addPathSepChar(destFileOut);
            destFileOut.append(destFileIn);
            if (dropZoneUMask.length())
                umask.set(dropZoneUMask.str());
        }
        else
        {
            if (constEnv->isDropZoneRestrictionEnabled())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "> 1 drop zones found for network address '%s'.", ip);
            else
            {
                LOG(MCdebugInfo, unknownJob, "> 1 drop zones found for network address '%s'.", ip);
                return;
            }
        }
    }
    if (!dzFound)
    {
        if (constEnv->isDropZoneRestrictionEnabled())
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "No valid drop zone found for network address '%s'. Check your system drop zone configuration.", ip);
        else
            LOG(MCdebugInfo, unknownJob, "No valid drop zone found for network address '%s'. Check your system drop zone configuration.", ip);
    }
}

bool CFileSprayEx::onDespray(IEspContext &context, IEspDespray &req, IEspDesprayResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_DESPRAY_URL, SecAccess_Write, ECLWATCH_FILE_DESPRAY_ACCESS_DENIED, "Failed to do Despray. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");

        PROGLOG("Despray %s", srcname);
        double version = context.getClientVersion();
        const char* destip = req.getDestIP();
        StringBuffer destPath;
        const char* destfile = getStandardPosixPath(destPath, req.getDestPath()).str();

        MemoryBuffer& dstxml = (MemoryBuffer&)req.getDstxml();
        if(dstxml.length() == 0)
        {
            if(!destip || !*destip)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination network IP not specified.");
            if(!destfile || !*destfile)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        }

        StringBuffer srcTitle;
        ParseLogicalPath(srcname, srcTitle);

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        wu->setJobName(srcTitle.str());
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_export);

        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();

        source->setLogicalName(srcname);

        if(dstxml.length() == 0)
        {
            RemoteFilename rfn;
            SocketEndpoint ep(destip);
            if (ep.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Despray %s: cannot resolve destination network IP from %s.", srcname, destip);

            StringBuffer destfileWithPath, umask;
            getDropZoneInfoByIP(version, destip, destfile, destfileWithPath, umask);
            rfn.setPath(ep, destfileWithPath.str());
            if (umask.length())
                options->setUMask(umask.str());
            destination->setSingleFilename(rfn);
        }
        else
        {
            dstxml.append('\0');
            destination->setFromXML((const char*)dstxml.toByteArray());
        }
        destination->setTitle(srcTitle.str());

        options->setKeepHeader(true);
        options->setOverwrite(req.getOverwrite());             // needed if target already exists

        const char* splitprefix = req.getSplitprefix();
        if(splitprefix && *splitprefix)
            options->setSplitPrefix(splitprefix);

        if (version > 1.01)
        {
            if(req.getMaxConnections() > 0)
                options->setmaxConnections(req.getMaxConnections());
            else if(req.getSingleConnection())
                options->setmaxConnections(1);
        }
        else
        {
            if(req.getMaxConnections() > 0)
                options->setmaxConnections(req.getMaxConnections());
        }

        if(req.getThrottle() > 0)
            options->setThrottle(req.getThrottle());
        if(req.getTransferBufferSize() > 0)
            options->setTransferBufferSize(req.getTransferBufferSize());

        if(req.getNorecover())
            options->setNoRecover(true);

        if (req.getWrap()) {
            options->setPush();             // I think needed for a despray
            destination->setWrap(true);
        }
        if (req.getMultiCopy())
            destination->setMultiCopy(true);

        const char * encryptkey = req.getEncrypt();
        if(req.getCompress()||(encryptkey&&*encryptkey))
            destination->setCompressed(true);

        const char * decryptkey = req.getDecrypt();
        if ((encryptkey&&*encryptkey)||(decryptkey&&*decryptkey))
            options->setEncDec(encryptkey,decryptkey);

        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onCopy(IEspContext &context, IEspCopy &req, IEspCopyResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Copy. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        const char* dstname = req.getDestLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");
        if(!dstname || !*dstname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination logical file not specified.");

        PROGLOG("Copy from %s to %s", srcname, dstname);
        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;
        StringBuffer srcNodeGroup, destNodeGroup;
        bool bRoxie = false;
        const char* destNodeGroupReq = req.getDestGroup();
        if(!destNodeGroupReq || !*destNodeGroupReq)
        {
            getNodeGroupFromLFN(context, srcname, destNodeGroup);
            DBGLOG("Destination node group not specified, using source node group %s", destNodeGroup.str());
        }
        else
        {
            destNodeGroup = destNodeGroupReq;
            const char* destRoxie = req.getDestGroupRoxie();
            if (destRoxie && !stricmp(destRoxie, "Yes"))
            {
                bRoxie = true;
            }
        }

        CDfsLogicalFileName lfn; // NOTE: must not be moved into block below, or dstname will point to invalid memory
        if (!bRoxie)
        {
            if (!lfn.setValidate(dstname))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid destination filename:'%s'", dstname);
            dstname = lfn.get();
        }

        ParseLogicalPath(dstname, destNodeGroup.str(), NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

        StringBuffer fileMask;
        constructFileMask(destTitle.str(), fileMask);

        Owned<IUserDescriptor> udesc=createUserDescriptor();
        const char* srcDali = req.getSourceDali();
        const char* srcu = req.getSrcusername();
        if (!isEmptyString(srcDali) && !isEmptyString(srcu))
        {
            udesc->set(srcu, req.getSrcpassword());
        }
        else
        {
            StringBuffer user, passwd;
            context.getUserID(user);
            context.getPassword(passwd);
            udesc->set(user, passwd);
        }

        CDfsLogicalFileName logicalName;
        logicalName.set(srcname);
        if (!isEmptyString(srcDali))
        {
            SocketEndpoint ep(srcDali);
            if (ep.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Copy %s: cannot resolve SourceDali network IP from %s.", srcname, srcDali);

            logicalName.setForeign(ep,false);
        }

        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(logicalName, udesc, false, false, false, nullptr, defaultPrivilegedUser);
        if (!file)
            throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Failed to find file: %s", logicalName.get());

        bool supercopy = req.getSuperCopy();
        if (supercopy)
        {
            if (!file->querySuperFile())
                supercopy = false;
        }
        else if (file->querySuperFile() && (file->querySuperFile()->numSubFiles() > 1) && isFileKey(file))
            supercopy = true;

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        wu->setJobName(dstname);
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        if(destNodeGroup.length() > 0)
            wu->setClusterName(destNodeGroup.str());
        if (supercopy)
            wu->setCommand(DFUcmd_supercopy);
        else
            wu->setCommand(DFUcmd_copy);

        IDFUfileSpec *wuFSpecSource = wu->queryUpdateSource();
        IDFUfileSpec *wuFSpecDest = wu->queryUpdateDestination();
        IDFUoptions *wuOptions = wu->queryUpdateOptions();
        wuFSpecSource->setLogicalName(srcname);
        if(srcDali && *srcDali)
        {
            SocketEndpoint ep(srcDali);
            wuFSpecSource->setForeignDali(ep);

            const char* srcusername = req.getSrcusername();
            if(srcusername && *srcusername)
            {
                const char* srcpasswd = req.getSrcpassword();
                wuFSpecSource->setForeignUser(srcusername, srcpasswd);
            }
        }
        wuFSpecDest->setLogicalName(dstname);
        wuFSpecDest->setFileMask(fileMask.str());
        wuOptions->setOverwrite(req.getOverwrite());
        wuOptions->setPreserveCompression(req.getPreserveCompression());
        if (!req.getExpireDays_isNull())
            wuOptions->setExpireDays(req.getExpireDays());

        if(req.getNosplit())
            wuOptions->setNoSplit(true);
        if (!req.getNoCommon_isNull())
            wuOptions->setNoCommon(req.getNoCommon());

        if (bRoxie)
        {
            setRoxieClusterPartDiskMapping(destNodeGroup.str(), defaultFolder.str(), defaultReplicateFolder.str(), supercopy, wuFSpecDest, wuOptions);
            wuFSpecDest->setWrap(true);                             // roxie always wraps
            if(req.getCompress())
                wuFSpecDest->setCompressed(true);
            if (!supercopy)
                wuOptions->setSuppressNonKeyRepeats(true);            // **** only repeat last part when src kind = key
        }
        else
        {
            const char* srcDiffKeyName = req.getSourceDiffKeyName();
            const char* destDiffKeyName = req.getDestDiffKeyName();
            if (srcDiffKeyName&&*srcDiffKeyName)
                wuFSpecSource->setDiffKey(srcDiffKeyName);
            if (destDiffKeyName&&*destDiffKeyName)
                wuFSpecDest->setDiffKey(destDiffKeyName);
            wuFSpecDest->setDirectory(destFolder.str());
            wuFSpecDest->setGroupName(destNodeGroup.str());
            wuFSpecDest->setWrap(req.getWrap());
            const char * encryptkey = req.getEncrypt();
            if(req.getCompress()||(encryptkey&&*encryptkey))
                wuFSpecDest->setCompressed(true);

            wuOptions->setReplicate(req.getReplicate());
            const char * decryptkey = req.getDecrypt();
            if ((encryptkey&&*encryptkey)||(decryptkey&&*decryptkey))
                wuOptions->setEncDec(encryptkey,decryptkey);
            if(req.getNorecover())
                wuOptions->setNoRecover(true);
            if(!req.getNosplit_isNull())
                wuOptions->setNoSplit(req.getNosplit());
            if(req.getMaxConnections() > 0)
                wuOptions->setmaxConnections(req.getMaxConnections());
            if(req.getThrottle() > 0)
                wuOptions->setThrottle(req.getThrottle());
            if(req.getTransferBufferSize() > 0)
                wuOptions->setTransferBufferSize(req.getTransferBufferSize());
            if (req.getPull())
                wuOptions->setPull(true);
            if (req.getPush())
                wuOptions->setPush(true);
            if (req.getIfnewer())
                wuOptions->setIfNewer(true);
            if (req.getNosplit())
                wuOptions->setNoSplit(true);

            ClusterPartDiskMapSpec mspec;
            wuFSpecDest->getClusterPartDiskMapSpec(destNodeGroup.str(), mspec);
            mspec.setDefaultBaseDir(defaultFolder.str());
            mspec.setDefaultReplicateDir(defaultReplicateFolder.str());
            wuFSpecDest->setClusterPartDiskMapSpec(destNodeGroup.str(), mspec);
        }

        resp.setResult(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CFileSprayEx::onRename(IEspContext &context, IEspRename &req, IEspRenameResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Rename. Permission denied.");

        const char* srcname = req.getSrcname();
        const char* dstname = req.getDstname();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");
        if(!dstname || !*dstname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination logical file not specified.");

        PROGLOG("Rename from %s to %s", srcname, dstname);
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        StringBuffer destTitle;
        ParseLogicalPath(req.getDstname(), destTitle);

        wu->setJobName(destTitle.str());
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_rename);

#if 0 // TBD - Handling for multiple clusters? the cluster should be specified by user if needed
        Owned<IUserDescriptor> udesc;
        if(user.length() > 0)
        {
            const char* passwd = context.queryPassword();
            udesc.setown(createUserDescriptor());
            udesc->set(user.str(), passwd);
            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(srcname, udesc);
            if(df)
            {
                StringBuffer cluster0;
                df->getClusterName(0,cluster0);                     // TBD - Handling for multiple clusters?
                if (cluster0.length()!=0)
                {
                    wu->setClusterName(cluster0.str());
                }
                else
                {
                    const char *cluster = df->queryAttributes().queryProp("@group");
                    if (cluster && *cluster)
                    {
                        wu->setClusterName(cluster);
                    }
                }
            }
        }
#endif

        IDFUfileSpec *source = wu->queryUpdateSource();
        source->setLogicalName(srcname);

        IDFUfileSpec *destination = wu->queryUpdateDestination();
        destination->setLogicalName(dstname);

        IDFUoptions *options = wu->queryUpdateOptions();
        options->setOverwrite(req.getOverwrite());

        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CFileSprayEx::onDFUWUFile(IEspContext &context, IEspDFUWUFileRequest &req, IEspDFUWUFileResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Read, ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        if (*req.getWuid())
        {
            Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
            Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(req.getWuid(), false);
            if(!wu)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

            PROGLOG("DFUWUFile: %s", req.getWuid());
            StringBuffer xmlbuf;
            xmlbuf.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

            const char* plainText = req.getPlainText();
            if (plainText && (!stricmp(plainText, "yes")))
            {
                wu->toXML(xmlbuf);
                resp.setFile(xmlbuf.str());
                resp.setFile_mimetype(HTTP_TYPE_TEXT_PLAIN);
            }
            else
            {
                xmlbuf.append("<?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>");
                wu->toXML(xmlbuf);
                resp.setFile(xmlbuf.str());
                resp.setFile_mimetype(HTTP_TYPE_APPLICATION_XML);
            }
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onFileList(IEspContext &context, IEspFileListRequest &req, IEspFileListResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do FileList. Permission denied.");

        const char* path = req.getPath();
        if (!path || !*path)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Path not specified.");

        double version = context.getClientVersion();
        const char* netaddr = req.getNetaddr();
        if (!netaddr || !*netaddr)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Network address not specified.");
        const char* fileNameMask = req.getMask();
        bool directoryOnly = req.getDirectoryOnly();
        PROGLOG("FileList:  Netaddr %s, Path %s", netaddr, path);

        StringBuffer sPath(path);
        const char* osStr = req.getOS();
        if (osStr && *osStr)
        {
            int os = atoi(osStr);
            const char pathSep = (os == OS_WINDOWS) ? '\\' : '/';
            sPath.replace(pathSep=='\\'?'/':'\\', pathSep);
            if (*(sPath.str() + sPath.length() -1) != pathSep)
                sPath.append( pathSep );
        }

        if (!isEmptyString(fileNameMask))
        {
            const char* ext = pathExtension(sPath.str());
            if (ext && !strieq(ext, "cfg") && !strieq(ext, "log"))
                throw MakeStringException(ECLWATCH_ACCESS_TO_FILE_DENIED, "Only cfg or log file allowed.");
        }

        RemoteFilename rfn;
        SocketEndpoint ep;
#ifdef MACHINE_IP
        ep.set(MACHINE_IP);
#else
        ep.set(netaddr);
        if (ep.isNull())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "FileList: cannot resolve network IP from %s.", netaddr);
#endif
        rfn.setPath(ep, sPath.str());
        Owned<IFile> f = createIFile(rfn);
        if (f->isDirectory()!=fileBool::foundYes)
            throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", path);

        IArrayOf<IEspPhysicalFileStruct> files;
        Owned<IDirectoryIterator> di = f->directoryFiles(NULL, false, true);
        if(di.get() != NULL)
        {
            ForEach(*di)
            {
                StringBuffer fname;
                di->getName(fname);

                if (fname.length() == 0 || (directoryOnly && !di->isDir()) || (!di->isDir() && !isEmptyString(fileNameMask) && !WildMatch(fname.str(), fileNameMask, true)))
                    continue;

                Owned<IEspPhysicalFileStruct> onefile = createPhysicalFileStruct();

                onefile->setName(fname.str());
                onefile->setIsDir(di->isDir());
                onefile->setFilesize(di->getFileSize());
                CDateTime modtime;
                StringBuffer timestr;
                di->getModifiedTime(modtime);
                unsigned y,m,d,h,min,sec,nsec;
                modtime.getDate(y,m,d,true);
                modtime.getTime(h,min,sec,nsec,true);
                timestr.appendf("%04d-%02d-%02d %02d:%02d:%02d", y,m,d,h,min,sec);
                onefile->setModifiedtime(timestr.str());
                files.append(*onefile.getLink());
            }
        }

        sPath.replace('\\', '/');//XSLT cannot handle backslashes
        resp.setPath(sPath);
        resp.setFiles(files);
        resp.setNetaddr(netaddr);
        if (osStr && *osStr)
        {
            int os = atoi(osStr);
            resp.setOS(os);
        }

        if (!isEmptyString(fileNameMask))
            resp.setMask(fileNameMask);

        if (version >= 1.10)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
        resp.setDirectoryOnly(directoryOnly);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::checkDropZoneIPAndPath(double clientVersion, const char* dropZoneName, const char* netAddr, const char* path)
{
    if (isEmptyString(netAddr) || isEmptyString(path))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "NetworkAddress or Path not defined.");

    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    Owned<IConstDropZoneInfoIterator> dropZoneItr = constEnv->getDropZoneIteratorByAddress(netAddr);
    ForEach(*dropZoneItr)
    {
        SCMStringBuffer directory, name;
        IConstDropZoneInfo& dropZoneInfo = dropZoneItr->query();
        dropZoneInfo.getDirectory(directory);
        if (directory.length() && (strnicmp(path, directory.str(), directory.length()) == 0))
        {
            if (isEmptyString(dropZoneName))
                return true;

            dropZoneInfo.getName(name);
            if (strieq(name.str(), dropZoneName))
                return true;
        }
    }
    return false;
}

void CFileSprayEx::addDropZoneFile(IEspContext& context, IDirectoryIterator* di, const char* name, const char pathSep, IArrayOf<IEspPhysicalFileStruct>& files)
{
    Owned<IEspPhysicalFileStruct> aFile = createPhysicalFileStruct();

    const char* pName = strrchr(name, pathSep);
    if (!pName)
        aFile->setName(name);
    else
    {
        StringBuffer sPath;
        sPath.append(pName - name, name);
        aFile->setPath(sPath.str());

        pName++; //skip the PathSepChar
        aFile->setName(pName);
    }

    aFile->setIsDir(di->isDir());
    CDateTime modtime;
    StringBuffer timestr;
    di->getModifiedTime(modtime);
    unsigned y,m,d,h,min,sec,nsec;
    modtime.getDate(y,m,d,true);
    modtime.getTime(h,min,sec,nsec,true);
    timestr.appendf("%04d-%02d-%02d %02d:%02d:%02d", y,m,d,h,min,sec);
    aFile->setModifiedtime(timestr.str());
    aFile->setFilesize(di->getFileSize());
    files.append(*aFile.getLink());
}

void CFileSprayEx::searchDropZoneFiles(IEspContext& context, IpAddress& ip, const char* dir, const char* nameFilter, IArrayOf<IEspPhysicalFileStruct>& files, unsigned& filesFound)
{
    RemoteFilename rfn;
    SocketEndpoint ep;
    ep.ipset(ip);
    rfn.setPath(ep, dir);
    Owned<IFile> f = createIFile(rfn);
    if(f->isDirectory()!=fileBool::foundYes)
        throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", dir);

    const char pathSep = getPathSepChar(dir);
    Owned<IDirectoryIterator> di = f->directoryFiles(nameFilter, true, true);
    ForEach(*di)
    {
        StringBuffer fname;
        di->getName(fname);
        if (!fname.length())
            continue;

        filesFound++;
        if (filesFound > dropZoneFileSearchMaxFiles)
            break;

        addDropZoneFile(context, di, fname.str(), pathSep, files);
    }
}

bool CFileSprayEx::onDropZoneFileSearch(IEspContext &context, IEspDropZoneFileSearchRequest &req, IEspDropZoneFileSearchResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Access, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do FileList. Permission denied.");

        const char* dropZoneName = req.getDropZoneName();
        if (isEmptyString(dropZoneName))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "DropZone not specified.");
        const char* dropZoneServerReq = req.getServer(); //IP or hostname
        if (isEmptyString(dropZoneServerReq))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "DropZone server not specified.");
        const char* nameFilter = req.getNameFilter();
        if (isEmptyString(nameFilter))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Name Filter not specified.");

        bool validNameFilter = false;
        const char* pNameFilter = nameFilter;
        while (!isEmptyString(pNameFilter))
        {
            if (*pNameFilter != '*')
            {
                validNameFilter = true;
                break;
            }
            pNameFilter++;
        }
        if (!validNameFilter)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Name Filter '*'");

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IConstDropZoneInfo> dropZoneInfo = constEnv->getDropZone(dropZoneName);
        if (!dropZoneInfo || (req.getECLWatchVisibleOnly() && !dropZoneInfo->isECLWatchVisible()))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "DropZone %s not found.", dropZoneName);

        SCMStringBuffer directory, computer;
        dropZoneInfo->getDirectory(directory);
        if (!directory.length())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "DropZone Directory not found for %s.", dropZoneName);

        IpAddress ipToMatch;
        ipToMatch.ipset(dropZoneServerReq);
        if (ipToMatch.isNull())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid server %s specified.", dropZoneServerReq);

        double version = context.getClientVersion();
        bool serverFound = false;
        unsigned filesFound = 0;
        IArrayOf<IEspPhysicalFileStruct> files;
        Owned<IConstDropZoneServerInfoIterator> dropZoneServerItr = dropZoneInfo->getServers();
        ForEach(*dropZoneServerItr)
        {
            StringBuffer server, networkAddress;
            IConstDropZoneServerInfo& dropZoneServer = dropZoneServerItr->query();
            dropZoneServer.getServer(server);
            if (server.isEmpty())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid server for dropzone %s.", dropZoneName);

            IpAddress ipAddr;
            ipAddr.ipset(server.str());
            if (ipAddr.ipequals(ipToMatch))
            {
                serverFound = true;
                searchDropZoneFiles(context, ipAddr, directory.str(), nameFilter, files, filesFound);
                break;
            }
        }

        if (!serverFound)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Server %s not found in dropzone %s.", dropZoneServerReq, dropZoneName);

        if ((version >= 1.16) && (filesFound > dropZoneFileSearchMaxFiles))
        {
            VStringBuffer msg("More than %u files are found. Only %u files are returned.", dropZoneFileSearchMaxFiles, dropZoneFileSearchMaxFiles);
            resp.setWarning(msg.str());
        }
        resp.setFiles(files);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onDfuMonitor(IEspContext &context, IEspDfuMonitorRequest &req, IEspDfuMonitorResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do DfuMonitor. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        wu->setQueue(m_MonitorQueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
        wu->setCommand(DFUcmd_monitor);

        IDFUmonitor *monitor = wu->queryUpdateMonitor();
        IDFUfileSpec *source = wu->queryUpdateSource();
        const char *eventname  = req.getEventName();
        const char *lname = req.getLogicalName();
        if (lname&&*lname)
            source->setLogicalName(lname);
        else {
            const char *ip = req.getIp();
            const char *filename = req.getFilename();
            if (filename&&*filename) {
                RemoteFilename rfn;
                if (ip&&*ip) {
                    SocketEndpoint ep(ip);
                    if (ep.isNull())
                        throw MakeStringException(ECLWATCH_INVALID_INPUT, "DfuMonitor: cannot resolve network IP from %s.", ip);

                    rfn.setPath(ep,filename);
                }
                else
                    rfn.setRemotePath(filename);
                source->setSingleFilename(rfn);
            }
            else
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Neither logical name nor network ip/file specified for monitor.");
        }
        if (eventname)
            monitor->setEventName(eventname);
        monitor->setShotLimit(req.getShotLimit());
        monitor->setSub(req.getSub());

        resp.setWuid(wu->queryId());
        resp.setRedirectUrl(StringBuffer("/FileSpray/GetDFUWorkunit?wuid=").append(wu->queryId()).str());
        submitDFUWorkUnit(wu.getClear());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onOpenSave(IEspContext &context, IEspOpenSaveRequest &req, IEspOpenSaveResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        const char* location = req.getLocation();
        const char* path = req.getPath();
        const char* name = req.getName();
        const char* type = req.getType();
        const char* dateTime = req.getDateTime();

        if (location && *location)
            resp.setLocation(location);
        if (path && *path)
            resp.setPath(path);
        if (name && *name)
            resp.setName(name);
        if (type && *type)
            resp.setType(type);
        if (dateTime && *dateTime)
            resp.setDateTime(dateTime);

        if (req.getBinaryFile())
            resp.setViewable(false);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::getDropZoneFiles(IEspContext &context, const char* dropZone, const char* netaddr, const char* path,
                                    IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp)
{
    if (!checkDropZoneIPAndPath(context.getClientVersion(), dropZone, netaddr, path))
        throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");

    bool directoryOnly = req.getDirectoryOnly();

    RemoteFilename rfn;
    SocketEndpoint ep;
#ifdef MACHINE_IP
    ep.set(MACHINE_IP);
#else
    ep.set(netaddr);
    if (ep.isNull())
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "CFileSprayEx::getDropZoneFiles: cannot resolve network IP from %s.", netaddr);
#endif

    rfn.setPath(ep, path);
    Owned<IFile> f = createIFile(rfn);
    if(f->isDirectory()!=fileBool::foundYes)
        throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", path);

    IArrayOf<IEspPhysicalFileStruct> files;
    Owned<IDirectoryIterator> di = f->directoryFiles(NULL, false, true);
    if(di.get() != NULL)
    {
        ForEach(*di)
        {
            StringBuffer fname;
            di->getName(fname);

            if (fname.length() == 0 || (directoryOnly && !di->isDir()))
                continue;

            Owned<IEspPhysicalFileStruct> onefile = createPhysicalFileStruct();

            onefile->setName(fname.str());
            onefile->setIsDir(di->isDir());
            onefile->setFilesize(di->getFileSize());
            CDateTime modtime;
            StringBuffer timestr;
            di->getModifiedTime(modtime);
            unsigned y,m,d,h,min,sec,nsec;
            modtime.getDate(y,m,d,true);
            modtime.getTime(h,min,sec,nsec,true);
            timestr.appendf("%04d-%02d-%02d %02d:%02d:%02d", y,m,d,h,min,sec);
            onefile->setModifiedtime(timestr.str());
            files.append(*onefile.getLink());
        }
    }

    resp.setFiles(files);

    return true;
}

//This method returns all dropzones and, if NetAddress and Path specified, returns filtered list of files.
bool CFileSprayEx::onDropZoneFiles(IEspContext &context, IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        IpAddress ipToMatch;
        const char* netAddress = req.getNetAddress();
        if (!isEmptyString(netAddress))
        {
            ipToMatch.ipset(netAddress);
            if (ipToMatch.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid server %s specified.", netAddress);
        }

        bool filesFromALinux = false;
        IArrayOf<IEspDropZone> dropZoneList;
        bool ECLWatchVisibleOnly = req.getECLWatchVisibleOnly();
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IConstDropZoneInfoIterator> dropZoneItr = constEnv->getDropZoneIterator();
        ForEach(*dropZoneItr)
        {
            IConstDropZoneInfo& dropZoneInfo = dropZoneItr->query();
            if (ECLWatchVisibleOnly && !dropZoneInfo.isECLWatchVisible())
                continue;

            SCMStringBuffer dropZoneName, directory, computerName;
            dropZoneInfo.getName(dropZoneName);
            dropZoneInfo.getDirectory(directory);
            dropZoneInfo.getComputerName(computerName); //legacy env
            if (!dropZoneName.length() || !directory.length())
                continue;

            bool isLinux = getPathSepChar(directory.str()) == '/' ? true : false;
            Owned<IConstDropZoneServerInfoIterator> dropZoneServerItr = dropZoneInfo.getServers();
            ForEach(*dropZoneServerItr)
            {
                IConstDropZoneServerInfo& dropZoneServer = dropZoneServerItr->query();

                StringBuffer name, server, networkAddress;
                dropZoneServer.getName(name);
                dropZoneServer.getServer(server);
                if (name.isEmpty() || server.isEmpty())
                    continue;

                IpAddress ipAddr;
                ipAddr.ipset(server.str());
                ipAddr.getIpText(networkAddress);

                Owned<IEspDropZone> aDropZone = createDropZone();
                aDropZone->setName(dropZoneName.str());
                aDropZone->setComputer(name.str());
                aDropZone->setNetAddress(networkAddress.str());

                aDropZone->setPath(directory.str());
                if (isLinux)
                    aDropZone->setLinux("true");
                if (!isEmptyString(netAddress) && ipAddr.ipequals(ipToMatch))
                    filesFromALinux = isLinux;

                dropZoneList.append(*aDropZone.getClear());
            }
        }

        if (dropZoneList.ordinality())
            resp.setDropZones(dropZoneList);

        const char* dzName = req.getDropZoneName();
        const char* directory = req.getPath();
        const char* subfolder = req.getSubfolder();

        if (isEmptyString(netAddress) || (isEmptyString(directory) && isEmptyString(subfolder)))
            return true;

        StringBuffer netAddressStr, directoryStr, osStr;
        netAddressStr.set(netAddress);
        if (!isEmptyString(directory))
            directoryStr.set(directory);
        if (!isEmptyString(subfolder))
        {
            if (directoryStr.length())
                addPathSepChar(directoryStr);
            directoryStr.append(subfolder);
        }
        addPathSepChar(directoryStr);

        getDropZoneFiles(context, dzName, netAddress, directoryStr.str(), req, resp);

        resp.setDropZoneName(dzName);
        resp.setNetAddress(netAddress);
        resp.setPath(directoryStr.str());
        resp.setOS(filesFromALinux);
        resp.setECLWatchVisibleOnly(ECLWatchVisibleOnly);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CFileSprayEx::onDeleteDropZoneFiles(IEspContext &context, IEspDeleteDropZoneFilesRequest &req, IEspDFUWorkunitsActionResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Full, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        double version = context.getClientVersion();
        const char* dzName = req.getDropZoneName();
        const char* netAddress = req.getNetAddress();
        const char* directory = req.getPath();
        const char* osStr = req.getOS();
        StringArray & files = req.getNames();
        if (!files.ordinality())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");

        StringBuffer path(directory);
        if (!isEmptyString(osStr))
        {
            char pathSep = (atoi(osStr) == OS_WINDOWS) ? '\\' : '/';
            path.replace(pathSep=='\\' ? '/' : '\\', pathSep);
        }
        addPathSepChar(path, getPathSepChar(path.str()));

        if (!checkDropZoneIPAndPath(version, dzName, netAddress, path.str()))
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");

        RemoteFilename rfn;
        SocketEndpoint ep(netAddress);
        if (ep.isNull())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "DeleteDropZoneFiles: cannot resolve network IP from %s.", netAddress);

        rfn.setPath(ep, path.str());
        Owned<IFile> f = createIFile(rfn);
        if(f->isDirectory()!=fileBool::foundYes)
            throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", directory);

        bool bAllSuccess = true;
        IArrayOf<IEspDFUActionResult> results;

        for(unsigned i = 0; i < files.ordinality(); ++i)
        {
            const char* file = files.item(i);
            if (!file || !*file)
                continue;

            PROGLOG("DeleteDropZoneFiles: netAddress %s, path %s, file %s", netAddress, directory, file);
            Owned<IEspDFUActionResult> res = createDFUActionResult("", "");
            res->setID(files.item(i));
            res->setAction("Delete");
            res->setResult("Success");

            try
            {
                StringBuffer fileToDelete(path);
                fileToDelete.append(file);

                rfn.setPath(ep, fileToDelete.str());
                Owned<IFile> rFile = createIFile(rfn);
                if (!rFile->exists())
                    res->setResult("Warning: this file does not exist.");
                else
                    rFile->remove();
            }
            catch (IException *e)
            {
                bAllSuccess = false;
                StringBuffer eMsg;
                eMsg = e->errorMessage(eMsg);
                e->Release();

                StringBuffer failedMsg("Failed: ");
                failedMsg.append(eMsg);
                res->setResult(failedMsg.str());
            }

            results.append(*res.getLink());
        }

        resp.setFirstColumn("File");
        resp.setDFUActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CFileSprayEx::appendGroupNode(IArrayOf<IEspGroupNode>& groupNodes, const char* nodeName, const char* clusterType,
    bool replicateOutputs)
{
    Owned<IEspGroupNode> node = createGroupNode();
    node->setName(nodeName);
    node->setClusterType(clusterType);
    if (replicateOutputs)
        node->setReplicateOutputs(replicateOutputs);
    groupNodes.append(*node.getClear());
}

bool CFileSprayEx::onGetSprayTargets(IEspContext &context, IEspGetSprayTargetsRequest &req, IEspGetSprayTargetsResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> environment = factory->openEnvironment();
        Owned<IPropertyTree> root = &environment->getPTree();

        IArrayOf<IEspGroupNode> sprayTargets;
        //Fetch all the group names for all the thor instances (and dedup them)
        BoolHash uniqueThorClusterGroupNames;
        Owned<IPropertyTreeIterator> it = root->getElements("Software/ThorCluster");
        ForEach(*it)
        {
            IPropertyTree& cluster = it->query();

            StringBuffer thorClusterGroupName;
            getClusterGroupName(cluster, thorClusterGroupName);
            if (!thorClusterGroupName.length())
                continue;

            bool* found = uniqueThorClusterGroupNames.getValue(thorClusterGroupName.str());
            if (!found || !*found)
                appendGroupNode(sprayTargets, thorClusterGroupName.str(), "thor", cluster.getPropBool("@replicateOutputs", false));
        }

        //Fetch all the group names for all the hthor instances
        it.setown(root->getElements("Software/EclAgentProcess"));
        ForEach(*it)
        {
            IPropertyTree &cluster = it->query();
            const char* name = cluster.queryProp("@name");
            if (!name || !*name)
                continue;

            unsigned ins = 0;
            Owned<IPropertyTreeIterator> insts = cluster.getElements("Instance");
            ForEach(*insts)
            {
                const char *na = insts->query().queryProp("@netAddress");
                if (!na || !*na)
                    continue;

                SocketEndpoint ep(na);
                if (ep.isNull())
                    continue;

                ins++;
                VStringBuffer gname("hthor__%s", name);
                if (ins>1)
                    gname.append('_').append(ins);

                appendGroupNode(sprayTargets, gname.str(), "hthor", false);
            }
        }

        resp.setGroupNodes(sprayTargets);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void CFileSprayEx::setDFUServerQueueReq(const char* dfuServerQueue, IDFUWorkUnit* wu)
{
    wu->setQueue((dfuServerQueue && *dfuServerQueue) ? dfuServerQueue : m_QueueLabel.str());
}

void CFileSprayEx::setUserAuth(IEspContext &context, IDFUWorkUnit* wu)
{
    StringBuffer user, passwd;
    wu->setUser(context.getUserID(user).str());
    wu->setPassword(context.getPassword(passwd).str());
}

bool CFileSprayEx::onGetDFUServerQueues(IEspContext &context, IEspGetDFUServerQueuesRequest &req, IEspGetDFUServerQueuesResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        StringArray qlist;
        getDFUServerQueueNames(qlist, req.getDFUServerName());
        resp.setNames(qlist);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}
