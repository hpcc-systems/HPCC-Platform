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
#include "jlog.hpp"

#include "dalienv.hpp"


#include "dfuutil.hpp"
#include "portlist.h"
#include "sacmd.hpp"
#include "exception_util.hpp"
#include "LogicFileWrapper.hpp"
#include "dameta.hpp"
#include "daqueue.hpp"

#include "ws_dfsclient.hpp"

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
#ifndef _CONTAINERIZED
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
#else
    // Using the first queue for now.
    // TODO: Re-design support for multiple queues
    Owned<IPropertyTreeIterator> dfuQueues = getComponentConfigSP()->getElements("dfuQueues");
    ForEach(*dfuQueues)
    {
        IPropertyTree & dfuQueue = dfuQueues->query();
        const char * dfuName = dfuQueue.queryProp("@name");
        if (!isEmptyString(dfuName))
        {
            getDfuQueueName(m_QueueLabel, dfuName);
            getDfuMonitorQueueName(m_MonitorQueueLabel, dfuName);
            break;
        }
    }
#endif
    DBGLOG("queueLabel=%s", m_QueueLabel.str());
    DBGLOG("monitorQueueLabel=%s", m_MonitorQueueLabel.str());

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

    StringBuffer tmp;
    double version = context.getClientVersion();
    dest.setID(src->queryId());

#ifndef _CONTAINERIZED
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    Owned<IPropertyTree> root = &constEnv->getPTree();

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
#else
    src->getClusterName(tmp.clear());
    dest.setClusterName(tmp.str());
#endif
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
        dest.setFileAccessCost(prog->getFileAccessCost());
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
        {
            dest.setSourceLogicalName(lfn.str());
        }
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
        if (version >= 1.21)
            dest.setPreserveFileParts(file->getWrap());

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
#ifdef _CONTAINERIZED
        Owned<IPropertyTree> plane = getStoragePlane(groupName);
        if (plane)
            defaultFolder.append(plane->queryProp("@prefix"));
#else
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
                    getConfigurationDirectory(directories, "data", "eclagent", cluster, defaultFolder);
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
#endif
    }

    getLFNDirectoryUsingBaseDir(folder, pLogicalPath, defaultFolder.str());

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

static void setRoxieClusterPartDiskMapping(const char *clusterName, const char *defaultFolder, const char *defaultReplicateFolder, bool supercopy, IDFUfileSpec *wuFSpecDest, IDFUoptions *wuOptions)
{
    ClusterPartDiskMapSpec spec;
    spec.setDefaultBaseDir(defaultFolder);
#ifndef _CONTAINERIZED
    //In containerized mode, there is no need to replicate files to the local disks of the roxie cluster.
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
    wuOptions->setReplicate(replicate);
#endif
    if (!supercopy)
        spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
    wuFSpecDest->setClusterPartDiskMapSpec(clusterName,spec);
}

static StringBuffer& constructFileMask(const char* filename, StringBuffer& filemask)
{
    filemask.clear().append(filename).toLowerCase().append("._$P$_of_$N$");
    return filemask;
}

bool CFileSprayEx::onDFUWUSearch(IEspContext &context, IEspDFUWUSearchRequest & req, IEspDFUWUSearchResponse & resp)
{
    try
    {
#ifndef _CONTAINERIZED
        //The code below should be only for legacy ECLWatch.
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
#endif
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

//Sasha List command returns comma separated strings about archived DFU WUs.
//Each comma separated string is for one DFU WU: WUID,User,JobName,ClusterName,StateMessage,Command.
//Parse the string to build one DFU WU.
IEspDFUWorkunit *CFileSprayEx::createDFUWUFromSashaListResult(const char *result)
{
    StringArray wuInfoFields;
    wuInfoFields.appendList(result, ",");
    unsigned numOfWUInfoFields = wuInfoFields.length();
    if (numOfWUInfoFields == 0)
        return nullptr;

    Owned<IEspDFUWorkunit> dfuWU = createDFUWorkunit();
    dfuWU->setArchived(true);

    ForEachItemIn(i, wuInfoFields)
    {
        switch(i)
        {
            case 0:
                dfuWU->setID(wuInfoFields.item(i));
                break;
            case 1:
                dfuWU->setUser(wuInfoFields.item(i));
                break;
            case 2:
                dfuWU->setJobName(wuInfoFields.item(i));
                break;
            case 3:
                dfuWU->setClusterName(wuInfoFields.item(i));
                break;
            case 4:
                dfuWU->setStateMessage(wuInfoFields.item(i));
                break;
            case 5:
                const char *commandStr = wuInfoFields.item(5);
                if (!isEmptyString(commandStr))
                    setDFUCommand(commandStr, dfuWU);
                break;
        }
    }
    return dfuWU.getClear();
}

void CFileSprayEx::setDFUCommand(const char *commandStr, IEspDFUWorkunit *dfuWU)
{
    DFUcmd cmd = decodeDFUcommand(commandStr);
    if (cmd != DFUcmd_none)
        dfuWU->setCommand(cmd);
    else
        dfuWU->setCommand(atoi(commandStr)); //back to the old behaviour
}

bool CFileSprayEx::GetArchivedDFUWorkunits(IEspContext &context, IEspGetDFUWorkunits &req, IEspGetDFUWorkunitsResponse &resp)
{
    StringBuffer user;
    context.getUserID(user);

    SocketEndpoint ep;
    getSashaServiceEP(ep, dfuwuArchiverType, true);
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
    for (unsigned i = 0; i < actualCount; i++)
    {
        Owned<IEspDFUWorkunit> dfuWU = createDFUWUFromSashaListResult(cmd->queryId(i));
        if (dfuWU)
            results.append(*dfuWU.getLink());
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
#ifdef _CONTAINERIZED
        Owned<IStringIterator> targets = getContainerTargetClusters(nullptr, clusterName);
#else
        Owned<IStringIterator> targets = getTargetClusters(nullptr, clusterName);
#endif
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

#ifndef _CONTAINERIZED
        //The code is used to find out a target cluster from a process cluster.
        //It is not needed for containerized environment.
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
#endif
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
        Owned<IConstDFUWorkUnitIterator> itr = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), (int) displayFrom, (int) pagesize+1, req.getOwner(), &cacheHint, &numWUs, req.getPublisherWuid());
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
            if (!isEmptyString(clusterName))
            {
#ifdef _CONTAINERIZED
                resultWU->setClusterName(clusterName);
#else
                //For bare metal environment, the wu->getClusterName() may return a process
                //cluster. It has to be converted to a target cluster.
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
#endif
            }

            resultWU->setIsProtected(wu->isProtected());
            IConstDFUprogress *prog = wu->queryProgress();
            if (prog != NULL)
            {
                DFUstate state = prog->getState();
                resultWU->setState(state);
                StringBuffer prgmsg;
                encodeDFUstate(state, prgmsg);
                resultWU->setStateMessage(prgmsg.str());
                resultWU->setPercentDone(prog->getPercentDone());
                if (req.getIncludeProgressMessages())
                {
                    prog->formatProgressMessage(prgmsg.clear());
                    resultWU->setProgressMessage(prgmsg.str());
                    prog->formatSummaryMessage(prgmsg.clear());
                    resultWU->setSummaryMessage(prgmsg.str());
                }
                if (version >= 1.25)
                {
                    if (req.getIncludeTransferRate())
                    {
                        resultWU->setKbPerSec(prog->getKbPerSec());
                        resultWU->setKbPerSecAve(prog->getKbPerSecAve());
                    }
                    if (req.getIncludeTimings())
                    {
                        CDateTime startAt, stopAt;
                        prog->getTimeStarted(startAt);
                        prog->getTimeStopped(stopAt);

                        StringBuffer s;
                        resultWU->setTimeStarted(startAt.getString(s));
                        resultWU->setTimeStopped(stopAt.getString(s.clear()));
                        unsigned secs = prog->getSecsLeft();
                        if (secs > 0)
                            resultWU->setSecsLeft(secs);
                    }
                }
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
    SocketEndpoint ep(sashaServer);
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
        StringBuffer serviceEndpoint;
        getSashaService(serviceEndpoint, dfuwuArchiverType, true);
        getInfoFromSasha(context, serviceEndpoint, wuid, &resp.updateResult());
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

bool CFileSprayEx::onCreateDFUPublisherWorkunit(IEspContext &context, IEspCreateDFUPublisherWorkunit &req, IEspCreateDFUPublisherWorkunitResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(DFU_WU_URL, SecAccess_Write, ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to create DFU Publisher workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createPublisherWorkUnit();
        setDFUServerQueueReq(req.getDFUServerQueue(), wu);
        setUserAuth(context, wu);
        wu->commit();
        const char * d = wu->queryId();
        IEspDFUWorkunit &result = resp.updateResult();
        DeepAssign(context, wu, result);
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

static StringBuffer &getDropZoneHost(const char *planeName, IPropertyTree *plane, StringBuffer &host)
{
    if (!host.isEmpty())
    {
        if (!isHostInPlane(plane, host, true))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "host %s is not valid storage plane %s", host.str(), planeName);
    }
    else if (!getPlaneHost(host, plane, 0))
        host.set("localhost"); // storage plane will be mounted when not using hosts
    return host;
}

IPropertyTree *CFileSprayEx::getAndValidateDropZone(const char *path, const char *host)
{
    if (!isAbsolutePath(path)) //Relative paths permitted, check host only
        path = nullptr;

    //Call the findDropZonePlane() to resolve plane by hostname. Shouldn't resolve plane
    //by hostname in containerized but kept for backward compatibility for now.
    Owned<IPropertyTree> plane = findDropZonePlane(path, host, true, isContainerized()); // NB: don't error if missing in bare-metal
    if (plane)
        return plane.getClear();

    // NB: only ever here if bare-metal
#ifndef _CONTAINERIZED
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env->isDropZoneRestrictionEnabled())
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "DropZone Plane not found for host %s path %s.", host, path);

    LOG(MCdebugInfo, unknownJob, "No matching drop zone path on '%s' to file path: '%s'", host, path);
#endif
    return nullptr;
}

void CFileSprayEx::readAndCheckSpraySourceReq(MemoryBuffer& srcxml, const char* srcIP, const char* srcPath, const char* srcPlane,
    StringBuffer& sourcePlaneReq, StringBuffer& sourceIPReq, StringBuffer& sourcePathReq)
{
    StringBuffer sourcePath(srcPath);
    sourcePath.trim();
    if(srcxml.length() == 0)
    {
        if (sourcePath.isEmpty())
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Source path not specified.");
        if (containsRelPaths(sourcePath)) //Detect a path like: a/../../../f
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", sourcePath.str());

        if (!isEmptyString(srcPlane))
        {
            Owned<IPropertyTree> dropZone = getDropZonePlane(srcPlane);
            if (!dropZone)
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Unknown landing zone: %s", srcPlane);
            const char * dropZonePlanePath = dropZone->queryProp("@prefix");
            if (isAbsolutePath(sourcePath))
            {
                if (!startsWith(sourcePath, dropZonePlanePath))
                    throw makeStringException(ECLWATCH_INVALID_INPUT, "Invalid source path");
            }
            else
            {
                StringBuffer s(sourcePath);
                sourcePath.set(dropZonePlanePath);
                addNonEmptyPathSepChar(sourcePath);
                sourcePath.append(s);
            }
            getDropZoneHost(srcPlane, dropZone, sourceIPReq);
            sourcePlaneReq.append(srcPlane);
        }
        else
        {
            sourceIPReq.set(srcIP).trim();
            if (sourceIPReq.isEmpty())
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Source network IP not specified.");
            Owned<IPropertyTree> plane = getAndValidateDropZone(sourcePath, sourceIPReq);
            if (plane)
                sourcePlaneReq.append(plane->queryProp("@name"));
        }
    }
    getStandardPosixPath(sourcePathReq, sourcePath.str());
}

static void checkValidDfuQueue(const char * dfuQueue)
{
    if (isEmptyString(dfuQueue))
        return;
#ifndef _CONTAINERIZED
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        if (!constEnv->isValidDfuQueueName(dfuQueue))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid DFU server queue name:'%s'", dfuQueue);
#else
        bool isValidDfuQueueName = false;
        Owned<IPropertyTreeIterator> dfuServers = getComponentConfigSP()->getElements("dfuQueues");
        ForEach(*dfuServers)
        {
            IPropertyTree & dfuServer = dfuServers->query();
            const char * dfuServerName = dfuServer.queryProp("@name");
            StringBuffer knownDfuQueueName;
            getDfuQueueName(knownDfuQueueName, dfuServerName);
            if (streq(dfuQueue, knownDfuQueueName))
            {
                isValidDfuQueueName = true;
                break;
            }
        }
        if (!isValidDfuQueueName)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid DFU server queue name: '%s'", dfuQueue);
#endif
}

bool CFileSprayEx::onSprayFixed(IEspContext &context, IEspSprayFixed &req, IEspSprayFixedResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Spray. Permission denied.");

        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;

        const char* destNodeGroup = req.getDestGroup();
        if (isEmptyString(destNodeGroup))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Destination node group not specified.");

        MemoryBuffer& srcxml = (MemoryBuffer&)req.getSrcxml();
        StringBuffer sourcePlaneReq, sourceIPReq, sourcePathReq;
        readAndCheckSpraySourceReq(srcxml, req.getSourceIP(), req.getSourcePath(), req.getSourcePlane(), sourcePlaneReq, sourceIPReq, sourcePathReq);
        const char* srcfile = sourcePathReq.str();
        const char* destname = req.getDestLogicalName();
        if(isEmptyString(destname))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid destination filename:'%s'", destname);
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
        checkValidDfuQueue(dfuQueue);
        setDFUServerQueueReq(dfuQueue, wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        checkDZScopeAccessAndSetSpraySourceDFUFileSpec(context, sourcePlaneReq, sourceIPReq, srcfile, srcxml, source);

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
        if (isEmptyString(destNodeGroup))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Destination node group not specified.");

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
        StringBuffer sourcePlaneReq, sourceIPReq, sourcePathReq;
        readAndCheckSpraySourceReq(srcxml, req.getSourceIP(), req.getSourcePath(), req.getSourcePlane(), sourcePlaneReq, sourceIPReq, sourcePathReq);
        const char* srcfile = sourcePathReq.str();
        const char* destname = req.getDestLogicalName();
        if(isEmptyString(destname))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid destination filename:'%s'", destname);

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
        checkValidDfuQueue(dfuQueue);
        setDFUServerQueueReq(dfuQueue, wu);
        setUserAuth(context, wu);
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();

        checkDZScopeAccessAndSetSpraySourceDFUFileSpec(context, sourcePlaneReq, sourceIPReq, srcfile, srcxml, source);
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

void CFileSprayEx::checkDZScopeAccessAndSetSpraySourceDFUFileSpec(IEspContext &context, const char *srcPlane, const char *srcHost,
    const char *srcFile, MemoryBuffer &srcXML, IDFUfileSpec *srcDFUfileSpec)
{
    if(srcXML.length() == 0)
    {
        //The srcHost is validated in readAndCheckSpraySourceReq().
        //If the srcPlane is found by readAndCheckSpraySourceReq(), the DZFileScopePermissions
        //should be validated for every files in srcFile.
        StringBuffer fnamebuf(srcFile);
        fnamebuf.trim();
        if (!isEmptyString(srcPlane))  // must be true, unless bare-metal and isDropZoneRestrictionEnabled()==false
        {
            StringArray files;
            files.appendList(fnamebuf, ","); // handles comma separated files
            ForEachItemIn(i, files)
            {
                const char *file = files.item(i);
                if (isEmptyString(file))
                    continue;

                //Based on the tests, the dfuserver only supports the wildcard inside the file name, like '/path/f*'.
                //The dfuserver throws an error if the wildcard is inside the path, like /p*ath/file.

                SecAccessFlags permission = getDZFileScopePermissions(context, srcPlane, file, srcHost);
                if (permission < SecAccess_Read)
                    throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). Read Access Required.",
                        srcPlane, file, context.queryUserId(), getSecAccessFlagName(permission));
            }
        }

        SocketEndpoint ep(srcHost);
        if (ep.isNull())
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Cannot resolve source network IP from %s.", srcHost);

        RemoteMultiFilename rmfn;
        rmfn.setEp(ep);
        rmfn.append(fnamebuf.str());
        srcDFUfileSpec->setMultiFilename(rmfn);
    }
    else
    {   //this block is copied from the code before PR https://github.com/hpcc-systems/HPCC-Platform/pull/17144.
        //Not sure there exists any use case for spraying a file using the srcXML. JIRA-29339 is created to check
        //whether this is completely legacy. If it is, it may be removed to make the code a lot clearer.
        srcXML.append('\0');
        srcDFUfileSpec->setFromXML((const char*)srcXML.toByteArray());
    }
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

void CFileSprayEx::getDropZoneInfoByDestPlane(double clientVersion, const char* destPlane, const char* destFileIn, StringBuffer& destFileOut, StringBuffer& umask, StringBuffer & hostip)
{
    Owned<IPropertyTree> dropZone = getDropZonePlane(destPlane);
    if (!dropZone)
        throw makeStringExceptionV(ECLWATCH_DROP_ZONE_NOT_FOUND, "Unknown landing zone %s", destPlane);

    StringBuffer fullDropZoneDir(dropZone->queryProp("@prefix"));
    addPathSepChar(fullDropZoneDir);
    if (isAbsolutePath(destFileIn))
    {
        if (!startsWith(destFileIn, fullDropZoneDir))
            throw makeStringExceptionV(ECLWATCH_DROP_ZONE_NOT_FOUND, "No landing zone configured for %s:%s", destPlane, destFileIn);
    }
    else
    {
        destFileOut.append(fullDropZoneDir);
        addNonEmptyPathSepChar(destFileOut);
    }
    destFileOut.append(destFileIn).trim();
    dropZone->getProp("@umask", umask);

    getDropZoneHost(destPlane, dropZone, hostip);
}

static StringBuffer & expandLogicalAsPhysical(StringBuffer & target, const char * name, const char * separator)
{
    const char * cur = name;
    for (;;)
    {
        const char * colon = strstr(cur, "::");
        if (!colon)
            break;

        //MORE: Process special characters?
        target.append(colon - cur, cur);
        target.append(separator);
        cur = colon + 2;
    }

    return target.append(cur);
}

bool CFileSprayEx::onDespray(IEspContext &context, IEspDespray &req, IEspDesprayResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_DESPRAY_URL, SecAccess_Write, ECLWATCH_FILE_DESPRAY_ACCESS_DENIED, "Failed to do Despray. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        if (isEmptyString(srcname))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");

        PROGLOG("Despray %s", srcname);
        double version = context.getClientVersion();

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
        bool preserveFileParts = req.getWrap();

        source->setLogicalName(srcname);

        StringBuffer destip(req.getDestIP());
        const char* destPlane = req.getDestPlane();
        const char* destPathReq = req.getDestPath();
        if (containsRelPaths(destPathReq)) //Detect a path like: a/../../../f
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", destPathReq);

        StringBuffer destPath;
        StringBuffer implicitDestFile;
        const char* destfile = getStandardPosixPath(destPath, destPathReq).str();

        MemoryBuffer& dstxml = (MemoryBuffer&)req.getDstxml();
        if (dstxml.length() == 0)
        {
            if (isEmptyString(destPlane))
                destPlane = req.getDestGroup();  // allow eclwatch to continue providing storage plane as 'destgroup' field
            if (isEmptyString(destPlane))
            {
                if (destip.isEmpty())
                    throw makeStringException(ECLWATCH_INVALID_INPUT, "Neither destination storage plane or destination IP specified.");
                Owned<IPropertyTree> plane = getAndValidateDropZone(destPath, destip);
                if (plane)
                    destPlane = plane->queryProp("@name");
            }

            //If the destination filename is not provided, calculate a relative filename from the logical filename
            if (isEmptyString(destfile))
            {
                expandLogicalAsPhysical(implicitDestFile, srcname, "/");
                destfile = implicitDestFile;
            }

            StringBuffer destfileWithPath, umask;
            if (!isEmptyString(destPlane))  // must be true, unless bare-metal and isDropZoneRestrictionEnabled()==false
                getDropZoneInfoByDestPlane(version, destPlane, destfile, destfileWithPath, umask, destip);

            SecAccessFlags permission = getDZFileScopePermissions(context, destPlane, destfileWithPath, destip);
            if (permission < SecAccess_Write)
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). Write Access Required.",
                    isEmptyString(destPlane) ? destip : destPlane, destfileWithPath.str(), context.queryUserId(), getSecAccessFlagName(permission));

            RemoteFilename rfn;
            SocketEndpoint ep(destip.str());
            if (ep.isNull())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Despray %s: cannot resolve destination network IP from %s.", srcname, destip.str());

            //Ensure the filename is dependent on the file part if parts are being preserved
            if (preserveFileParts && !strstr(destfileWithPath, "$P$"))
                destfileWithPath.append("._$P$_of_$N$");

            rfn.setPath(ep, destfileWithPath.str());
            if (umask.length())
                options->setUMask(umask.str());
            destination->setSingleFilename(rfn);
        }
        else
        {   //Not sure there exists any use case for despraying a file using the dstxml. JIRA-29339 is created to check
            //whether this is completely legacy. If it is, it may be removed to make the code a lot clearer.
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

        if (preserveFileParts) {
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
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid destination filename:'%s'", dstname);
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

        Owned<IDistributedFile> file = wsdfs::lookup(logicalName, udesc, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
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
        if (containsRelPaths(path)) //Detect a path like: /var/lib/HPCCSystems/mydropzone/../../../
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", path);

        StringBuffer sPath(path);
        const char* osStr = req.getOS();
        if (!isEmptyString(osStr))
        {
            int os = atoi(osStr);
            const char pathSep = (os == OS_WINDOWS) ? '\\' : '/';
            sPath.replace(pathSep=='\\'?'/':'\\', pathSep);
        }
        addPathSepChar(sPath);

        //The FileSpray.FileList is mainly used to list dropzone files.
        //In a bare metal environment, the FileSpray.FileList is also used to retrieve a list of log files.
        bool listLogFiles = validateConfigurationDirectory(nullptr, "log", nullptr, nullptr, sPath);
        if (listLogFiles && isContainerized())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "In a containerized environment, the FileSpray.FileList cannot be used to retrieve a list of log files.");

        double version = context.getClientVersion();
        const char* dropZoneName = req.getDropZoneName();
        const char* netaddr = req.getNetaddr();
        if (isEmptyString(dropZoneName) && isEmptyString(netaddr))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "DropZoneName or Netaddr must be specified.");
        const char* fileNameMask = req.getMask();
        bool directoryOnly = req.getDirectoryOnly();
        DBGLOG("FileList:  DropZone %s, Path %s", isEmptyString(dropZoneName) ? netaddr : dropZoneName, path);

        if (!isEmptyString(fileNameMask))
        {
            const char* ext = pathExtension(sPath.str());
            if (ext && !strieq(ext, "cfg") && !strieq(ext, "log"))
                throw MakeStringException(ECLWATCH_ACCESS_TO_FILE_DENIED, "Only cfg or log file allowed.");
        }

        IArrayOf<IConstPhysicalFileStruct>& files = resp.getFiles();
        if (listLogFiles)
        {
            if (!isEmptyString(dropZoneName))
                throw makeStringException(ECLWATCH_INVALID_INPUT, "DropZone name specified when listing log files.");

            getPhysicalFiles(context, nullptr, netaddr, sPath, fileNameMask, directoryOnly, files);
        }
        else
        {
            StringBuffer dzName;
            if (isEmptyString(dropZoneName))
                dropZoneName = findDropZonePlaneName(netaddr, sPath, dzName);
            if (!isEmptyString(dropZoneName))
            {
                SecAccessFlags permission = getDZPathScopePermissions(context, dropZoneName, sPath, nullptr);
                if (permission < SecAccess_Read)
                    throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). Read Access Required.",
                        dropZoneName, sPath.str(), context.queryUserId(), getSecAccessFlagName(permission));
            }

            StringArray hosts;
            if (isEmptyString(netaddr))
            {
                Owned<IPropertyTree> dropZone = getDropZonePlane(dropZoneName);
                if (!dropZone)
                    throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Unknown landing zone: %s", dropZoneName);
                getPlaneHosts(hosts, dropZone);
                if (!hosts.ordinality())
                    hosts.append("localhost");
            }
            else
                hosts.append(netaddr);

            ForEachItemIn(i, hosts)
            {
                const char* host = hosts.item(i);
                if (validateDropZonePath(nullptr, host, sPath))
                    getPhysicalFiles(context, dropZoneName, host, sPath, fileNameMask, directoryOnly, files);
            }
        }

        sPath.replace('\\', '/');//XSLT cannot handle backslashes
        resp.setPath(sPath);
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

    bool isIPAddressReq = isIPAddress(netAddr);
    IArrayOf<IConstTpDropZone> allTpDropZones;
    CTpWrapper tpWrapper;
    tpWrapper.getTpDropZones(9999, nullptr, false, allTpDropZones); //version 9999: get the latest information about dropzone
    ForEachItemIn(i, allTpDropZones)
    {
        IConstTpDropZone& dropZone = allTpDropZones.item(i);
        if (!isEmptyString(dropZoneName) && !streq(dropZoneName, dropZone.getName()))
            continue;

        const char* prefix = dropZone.getPath();
        if (strnicmp(path, prefix, strlen(prefix)) != 0)
            continue;

        IArrayOf<IConstTpMachine>& tpMachines = dropZone.getTpMachines();
        ForEachItemIn(ii, tpMachines)
        {
            if (matchNetAddressRequest(netAddr, isIPAddressReq, tpMachines.item(ii)))
                return true;
        }
    }
    return false;
}

void CFileSprayEx::addPhysicalFile(IEspContext& context, IDirectoryIterator* di, const char* name, const char* path, const char* server, IArrayOf<IConstPhysicalFileStruct>& files)
{
    double version = context.getClientVersion();

    Owned<IEspPhysicalFileStruct> aFile = createPhysicalFileStruct();

    aFile->setName(name);
    if (!isEmptyString(path))
        aFile->setPath(path);

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
    if (version >= 1.23)
        aFile->setServer(server);
    files.append(*aFile.getLink());
}

bool CFileSprayEx::searchDropZoneFiles(IEspContext& context, const char* dropZoneName, const char* server,
    const char* dir, const char* relDir, const char* nameFilter, IArrayOf<IConstPhysicalFileStruct>& files, unsigned& filesFound)
{
    if (getDZPathScopePermissions(context, dropZoneName, dir, server) < SecAccess_Read)
        return false;

    RemoteFilename rfn;
    SocketEndpoint ep(server);
    rfn.setPath(ep, dir);
    Owned<IFile> f = createIFile(rfn);
    if(f->isDirectory()!=fileBool::foundYes)
        throw makeStringExceptionV(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", dir);

    Owned<IDirectoryIterator> di = f->directoryFiles(nullptr, false, true);
    ForEach(*di)
    {
        StringBuffer fname;
        di->getName(fname);

        if (di->isDir())
        {
            StringBuffer fullPath(dir), relPath(relDir);
            addPathSepChar(fullPath).append(fname);
            if (!relPath.isEmpty())
                addPathSepChar(relPath);
            relPath.append(fname);
            if (!searchDropZoneFiles(context, dropZoneName, server, fullPath, relPath, nameFilter, files, filesFound))
                continue;
        }
        if (!isEmptyString(nameFilter) && !WildMatch(fname, nameFilter, false))
            continue;

        addPhysicalFile(context, di, fname.str(), relDir, server, files);

        filesFound++;
        if (filesFound > dropZoneFileSearchMaxFiles)
            break;
    }
    return true;
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

        double version = context.getClientVersion();
        bool serverFound = false;
        unsigned filesFound = 0;
        IArrayOf<IConstPhysicalFileStruct> &files = resp.getFiles();
        bool isIPAddressReq = isIPAddress(dropZoneServerReq);
        IArrayOf<IConstTpDropZone> allTpDropZones;
        CTpWrapper tpWrapper;
        tpWrapper.getTpDropZones(9999, nullptr, false, allTpDropZones); //version 9999: get the latest information about dropzone
        ForEachItemIn(i, allTpDropZones)
        {
            IConstTpDropZone& dropZone = allTpDropZones.item(i);
            const char* name = dropZone.getName();
            if (!streq(dropZoneName, name))
                continue;
            if (req.getECLWatchVisibleOnly() && !dropZone.getECLWatchVisible())
                continue;

            IArrayOf<IConstTpMachine>& tpMachines = dropZone.getTpMachines();
            ForEachItemIn(ii, tpMachines)
            {
                IConstTpMachine& tpMachine = tpMachines.item(ii);
                if (isEmptyString(dropZoneServerReq) || matchNetAddressRequest(dropZoneServerReq, isIPAddressReq, tpMachine))
                {
                    searchDropZoneFiles(context, dropZoneName, tpMachine.getNetaddress(), dropZone.getPath(), nullptr, nameFilter, files, filesFound);
                    serverFound = true;
                }
            }
        }
        if (!isEmptyString(dropZoneServerReq) && !serverFound)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Server %s not found in dropzone %s.", dropZoneServerReq, dropZoneName);

        if ((version >= 1.16) && (filesFound > dropZoneFileSearchMaxFiles))
        {
            VStringBuffer msg("More than %u files are found. Only %u files are returned.", dropZoneFileSearchMaxFiles, dropZoneFileSearchMaxFiles);
            resp.setWarning(msg.str());
        }
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

void CFileSprayEx::getPhysicalFiles(IEspContext &context, const char *dropZoneName, const char *host, const char *path, const char *fileNameMask, bool directoryOnly, IArrayOf<IConstPhysicalFileStruct> &files)
{
    SocketEndpoint ep(host);
    RemoteFilename rfn;
    rfn.setPath(ep, path);
    Owned<IFile> f = createIFile(rfn);
    if (f->isDirectory()!=fileBool::foundYes)
        throw makeStringExceptionV(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", path);

    Owned<IDirectoryIterator> di = f->directoryFiles(nullptr, false, true);
    ForEach(*di)
    {
        StringBuffer fileName;
        di->getName(fileName);

        if (!di->isDir() && (directoryOnly || (!isEmptyString(fileNameMask) && !WildMatch(fileName.str(), fileNameMask, true))))
            continue;

        if (dropZoneName && di->isDir())
        {
            VStringBuffer fullPath("%s%s", path, fileName.str());
            if (getDZPathScopePermissions(context, dropZoneName, fullPath, nullptr) < SecAccess_Read)
                continue;
        }

        addPhysicalFile(context, di, fileName, path, host, files);
    }
}

void CFileSprayEx::getServersInDropZone(const char *dropZoneName, IArrayOf<IConstTpDropZone> &dropZoneList, bool isECLWatchVisibleOnly, StringArray &serverList)
{
    ForEachItemIn(i, dropZoneList)
    {
        IConstTpDropZone &dropZone = dropZoneList.item(i);
        if (!streq(dropZoneName, dropZone.getName()))
            continue;

        if (!isECLWatchVisibleOnly || dropZone.getECLWatchVisible())
        {
            IArrayOf<IConstTpMachine> &tpMachines = dropZone.getTpMachines();
            ForEachItemIn(ii, tpMachines)
            {
                IConstTpMachine &tpMachine = tpMachines.item(ii);
                serverList.append(tpMachine.getNetaddress());
            }
        }
        break;
    }
}

//This method returns all dropzones and, if NetAddress (or dropzone name) and Path specified, returns filtered list of files.
bool CFileSprayEx::onDropZoneFiles(IEspContext &context, IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        const char* netAddress = req.getNetAddress(); //the hostname or IP address of a DropZone server
        if (!isEmptyString(netAddress))
        {
            IpAddress ipToCheck;
            ipToCheck.ipset(netAddress);
            if (ipToCheck.isNull())
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid server %s specified.", netAddress);
        }

        bool filesFromALinux = false;
        IArrayOf<IEspDropZone> dropZoneList;
        bool ECLWatchVisibleOnly = req.getECLWatchVisibleOnly();
        bool isIPAddressReq = isIPAddress(netAddress);
        IArrayOf<IConstTpDropZone> allTpDropZones;
        CTpWrapper tpWrapper;
        tpWrapper.getTpDropZones(9999, nullptr, false, allTpDropZones); //version 9999: get the latest information about dropzone
        ForEachItemIn(i, allTpDropZones)
        {
            IConstTpDropZone& dropZone = allTpDropZones.item(i);
            if (ECLWatchVisibleOnly && !dropZone.getECLWatchVisible())
                continue;

            const char* dropZoneName = dropZone.getName();
            const char* prefix = dropZone.getPath();
            bool isLinux = getPathSepChar(prefix) == '/' ? true : false;

            IArrayOf<IConstTpMachine>& tpMachines = dropZone.getTpMachines();
            ForEachItemIn(ii, tpMachines)
            {
                const char* machineName = tpMachines.item(ii).getName();
                const char* netAddr = tpMachines.item(ii).getNetaddress();
                if (isEmptyString(machineName) || isEmptyString(netAddr))
                    continue;

                Owned<IEspDropZone> dropZone = createDropZone();
                dropZone->setName(dropZoneName);
                dropZone->setPath(prefix);
                dropZone->setComputer(machineName);
                dropZone->setNetAddress(netAddr);
                if (isLinux)
                    dropZone->setLinux("true");

                dropZoneList.append(*dropZone.getClear());

                if (!isEmptyString(netAddress) && matchNetAddressRequest(netAddress, isIPAddressReq, tpMachines.item(ii)))
                    filesFromALinux = isLinux;
            }
        }

        if (dropZoneList.ordinality())
            resp.setDropZones(dropZoneList);

        StringBuffer directoryStr(req.getPath());
        if (containsRelPaths(directoryStr)) //Detect a path like: a/../../../f
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", directoryStr.str());

        const char* dzName = req.getDropZoneName();
        const char* subfolder = req.getSubfolder();
        if (containsRelPaths(subfolder)) //Detect a path like: a/../../../f
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid Subfolder %s", subfolder);

        if (isEmptyString(dzName) && isEmptyString(netAddress))
            return true; //Do not report DropZone files if DropZone is not specified in the request.
        if (directoryStr.isEmpty() && isEmptyString(subfolder))
            return true; //Do not report DropZone files if file path is not specified in the request.

        if (!isEmptyString(subfolder))
        {
            if (!directoryStr.isEmpty())
                addPathSepChar(directoryStr);
            directoryStr.append(subfolder);
        }
        addPathSepChar(directoryStr);

        StringBuffer planeName;
        if (isEmptyString(dzName))
            dzName = findDropZonePlaneName(netAddress, directoryStr, planeName);
        if (!isEmptyString(dzName) && getDZPathScopePermissions(context, dzName, directoryStr, nullptr) < SecAccess_Read)
            return false;

        bool directoryOnly = req.getDirectoryOnly();
        IArrayOf<IConstPhysicalFileStruct> &files = resp.getFiles();
        if (!isEmptyString(netAddress))
        {
            if (!checkDropZoneIPAndPath(context.getClientVersion(), dzName, netAddress, directoryStr))
                throw makeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");
            getPhysicalFiles(context, dzName, netAddress, directoryStr, nullptr, directoryOnly, files);
        }
        else
        {
            //Find out all DropZone servers inside the DropZone.
            StringArray servers;
            getServersInDropZone(dzName, allTpDropZones, ECLWatchVisibleOnly, servers);
            if (servers.empty())
                return true;

            ForEachItemIn(itr, servers)
            {
                const char* host = servers.item(itr);
                if (checkDropZoneIPAndPath(context.getClientVersion(), dzName, host, directoryStr))
                    getPhysicalFiles(context, dzName, host, directoryStr, nullptr, directoryOnly, files);
            }
        }

        resp.setDropZoneName(dzName);
        resp.setNetAddress(netAddress);
        resp.setPath(directoryStr.str());
        if (!isEmptyString(netAddress))
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
        if (containsRelPaths(directory)) //Detect a path like: a/../../../f
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid path %s", directory);

        const char* osStr = req.getOS();
        StringArray & files = req.getNames();
        if (!files.ordinality())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");
        ForEachItemIn(idx, files)
        {
            const char* file = files.item(idx);
            if (containsRelPaths(file))
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid file %s", file);   
        }

        StringBuffer path(directory);
        if (!isEmptyString(osStr))
        {
            char pathSep = (atoi(osStr) == OS_WINDOWS) ? '\\' : '/';
            path.replace(pathSep=='\\' ? '/' : '\\', pathSep);
        }
        addPathSepChar(path, getPathSepChar(path.str()));

        if (!checkDropZoneIPAndPath(version, dzName, netAddress, path.str()))
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");

        checkDropZoneFileScopeAccess(context, dzName, netAddress, path, files, SecAccess_Full);

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

void CFileSprayEx::checkDropZoneFileScopeAccess(IEspContext &context, const char *dropZoneName, const char *netAddress,
    const char *dropZonePath, const StringArray &dropZoneFiles, SecAccessFlags accessReq)
{
    const char *accessReqName = getSecAccessFlagName(accessReq);
    StringBuffer dzName;
    if (isEmptyString(dropZoneName))
        dropZoneName = findDropZonePlaneName(netAddress, dropZonePath, dzName);
    if (!isEmptyString(dropZoneName))
    {
        SecAccessFlags permission = getDZPathScopePermissions(context, dropZoneName, dropZonePath, nullptr);
        if (permission < accessReq)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). %s Permission Required.",
                dropZoneName, dropZonePath, context.queryUserId(), getSecAccessFlagName(permission), accessReqName);
    }

    RemoteFilename rfn;
    SocketEndpoint ep(netAddress);
    rfn.setIp(ep);

    StringBuffer errorMessage;
    MapStringTo<bool> uniquePath;
    const char pathSep = getPathSepChar(dropZonePath);
    ForEachItemIn(i, dropZoneFiles)
    {
        const char *fileNameWithPath = dropZoneFiles.item(i);
        if (isEmptyString(fileNameWithPath))
            continue;

        StringBuffer fileToDelete(dropZonePath);
        addPathSepChar(fileToDelete).append(fileNameWithPath);

        StringBuffer pathToCheck;
        rfn.setRemotePath(fileToDelete.str());
        Owned<IFile> rFile = createIFile(rfn);
        if (rFile->isDirectory() == fileBool::foundYes)
            pathToCheck.append(fileNameWithPath);
        else
        {
            splitDirTail(fileNameWithPath, pathToCheck);
            if (pathToCheck.isEmpty())
                continue;
        }

        //a subfolder or a file under a subfolder. Check whether accessing the subfolder is allowed.
        bool *found = uniquePath.getValue(pathToCheck.str());
        if (found)
        {
            if (!*found) //found a path denied
                errorMessage.append("; ").append(fileNameWithPath);
            continue;
        }

        StringBuffer fullPath(dropZonePath);
        addPathSepChar(fullPath).append(pathToCheck);
        //If the dropzone name is not found, the DZPathScopePermissions cannot be validated.
        SecAccessFlags permission = isEmptyString(dropZoneName) ? accessReq : getDZPathScopePermissions(context, dropZoneName, fullPath, nullptr);
        if (permission < accessReq)
        {
            uniquePath.setValue(pathToCheck.str(), false); //add a path denied
            if (errorMessage.isEmpty())
                errorMessage.setf("User %s (permission:%s): failed to access the DropZone Scopes for the following file(s). %s Permission Required. %s",
                   context.queryUserId(), getSecAccessFlagName(permission), accessReqName, fileNameWithPath);
            else
                errorMessage.append("; ").append(fileNameWithPath);
        }
        else
        {
            uniquePath.setValue(pathToCheck.str(), true); //add a path allowed
        }
    }

    if (!errorMessage.isEmpty())
        throw makeStringException(ECLWATCH_INVALID_INPUT, errorMessage.str());
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
#ifdef _CONTAINERIZED
        IArrayOf<IEspGroupNode> sprayTargets;
        Owned<IPropertyTreeIterator> dataPlanes = getGlobalConfigSP()->getElements("storage/planes[@category='data']");
        ForEach(*dataPlanes)
        {
            IPropertyTree & plane = dataPlanes->query();
            const char * name = plane.queryProp("@name");
            appendGroupNode(sprayTargets, name, "storage plane", false /*replicate outputs*/);
        }
        resp.setGroupNodes(sprayTargets);
#else
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
#endif
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
#ifdef _CONTAINERIZED
        Owned<IPropertyTreeIterator> dfuQueues = getComponentConfigSP()->getElements("dfuQueues");
        ForEach(*dfuQueues)
        {
            IPropertyTree &dfuQueue = dfuQueues->query();
            const char *dfuName = dfuQueue.queryProp("@name");
            assertex(!isEmptyString(dfuName));
            StringBuffer queueLabel;
            getDfuQueueName(queueLabel, dfuName);
            qlist.append(queueLabel);
        }
#else
        getDFUServerQueueNames(qlist, req.getDFUServerName());
#endif
        resp.setNames(qlist);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}
