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
#include "TpWrapper.hpp"

#include "dalienv.hpp"


#include "dfuutil.hpp"
#include "portlist.h"
#include "sacmd.hpp"
#include "exception_util.hpp"

#define DFU_WU_URL          "DfuWorkunitsAccess" 
#define DFU_EX_URL          "DfuExceptionsAccess"
#define FILE_SPRAY_URL      "FileSprayAccess"
#define FILE_DESPRAY_URL    "FileDesprayAccess"
#define WUDETAILS_REFRESH_MINS 1

void SetResp(StringBuffer &resp, IConstDFUWorkUnit * wu, bool array);
int Schedule::run()
{
    try
    {
        while(!stopping)
        {
            {
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
                        ERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                        e->Release();
                    }
                    itr->next();
                }
            }
            semSchedule.wait(1000*60);
        }
    }
    catch(IException *e)
    {
        StringBuffer msg;
        ERRLOG("Exception %d:%s in WS_FS Schedule::run", e->errorCode(), e->errorMessage(msg).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown exception in WS_FS Schedule::run");
    }
    return 0;
}

void CFileSprayEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;
    
    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/QueueLabel", process, service);
    cfg->getProp(xpath.str(), m_QueueLabel);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/MonitorQueueLabel", process, service);
    cfg->getProp(xpath.str(), m_MonitorQueueLabel);

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/RootFolder", process, service);
    cfg->getProp(xpath.str(), m_RootFolder);

    directories.set(cfg->queryPropTree("Software/Directories"));

    StringBuffer prop;
    prop.appendf("queueLabel=%s", m_QueueLabel.str());
    PrintLog(prop.str());
    prop.clear();
    prop.appendf("monitorQueueLabel=%s", m_MonitorQueueLabel.str());
    PrintLog(prop.str());
    prop.clear();
    prop.appendf("rootFolder=%s", m_RootFolder.str());
    PrintLog(prop.str());

    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
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

    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    Owned<IPropertyTree> root = &constEnv->getPTree();
    if (!root)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

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
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
    envFactory->validateCache();

    StringBuffer dirxpath;
    dirxpath.appendf("Software/RoxieCluster[@name=\"%s\"]",clusterName);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
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

StringBuffer& getNodeGroupFromLFN(StringBuffer& nodeGroup, const char* lfn, const char* username, const char* passwd)
{
    Owned<IUserDescriptor> udesc;
    if(username != NULL && *username != '\0')
    {
        udesc.setown(createUserDescriptor());
        udesc->set(username, passwd);
    }

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(lfn, udesc);
    if (!df)
        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Failed to find file: %s", lfn);
    return df->getClusterGroupName(0, nodeGroup);
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        StringArray dfuclusters;
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> environment = factory->openEnvironment();
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
        StringBuffer msg;
        msg.appendf("Cannot connect to archive server at %s",sashaAddress.str());
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER, "%s", msg.str());
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        StringBuffer wuidStr = req.getWuid();
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

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> root = &constEnv->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

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
        Owned<IConstDFUWorkUnitIterator> itr = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), (int) displayFrom, (int) pagesize+1, req.getOwner(), &cacheHint, &numWUs);
        if (version >= 1.07)
            resp.setCacheHint(cacheHint);

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

            StringBuffer strbuf = req.getSortby();
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
        DBGLOG("Cannot connect to Sasha server at %s",sashaServer);
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to archive server at %s.",sashaServer);
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
            StringBuffer startStr = timeStarted;
            startStr.replace('T', ' ');
            info->setTimeStarted(startStr.str());
        }
        if (timeStopped && *timeStopped)
        {
            StringBuffer stopStr = timeStopped;
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        const char* wuid = req.getWuid();
        if(!wuid || !*wuid)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit ID not specified.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(req.getWuid(), false);
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

        resp.setWuid(req.getWuid());

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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to create DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to update DFU workunit. Permission denied.");

        IConstDFUWorkunit & reqWU = req.getWu();
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(reqWU.getID());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Dfu workunit %s not found.", reqWU.getID());

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

static unsigned NumOfDFUWUActionNames = 5;
static const char *DFUWUActionNames[] = { "Delete", "Protect" , "Unprotect" , "Restore" , "SetToFailed" };

bool CFileSprayEx::onDFUWorkunitsAction(IEspContext &context, IEspDFUWorkunitsActionRequest &req, IEspDFUWorkunitsActionResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to update DFU workunit. Permission denied.");

        CDFUWUActions action = req.getType();
        if (action == DFUWUActions_Undefined)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Action not defined.");

        StringArray& wuids = req.getWuids();
        if (!wuids.ordinality())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Workunit not defined.");

        Owned<INode> node;
        Owned<ISashaCommand> cmd;
        StringBuffer sashaAddress;
        if (action == CDFUWUActions_Restore)
        {
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

            SocketEndpoint ep(sashaAddress.str(), DEFAULT_SASHA_PORT);
            node.setown(createINode(ep));
            cmd.setown(createSashaCommand());
            cmd->setAction(SCA_RESTORE);
            cmd->setDFU(true);
        }

        IArrayOf<IEspDFUActionResult> results;
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        for(unsigned i = 0; i < wuids.ordinality(); ++i)
        {
            const char* wuid = wuids.item(i);

            Owned<IEspDFUActionResult> res = createDFUActionResult("", "");
            res->setID(wuid);
            res->setAction((action < NumOfDFUWUActionNames) ? DFUWUActionNames[action] : "Unknown");

            try
            {
                switch (action)
                {
                case CDFUWUActions_Delete:
                    if (!markWUFailed(factory, wuid))
                        throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "Failed to mark workunit failed.");
                    if (!factory->deleteWorkUnit(wuid))
                        throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "Failed in deleting workunit.");
                    res->setResult("Success");
                    break;
                case CDFUWUActions_Restore:
                    cmd->addId(wuid);
                    if (!cmd->send(node,1*60*1000))
                        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to archive server at %s.",sashaAddress.str());
                    {
                        StringBuffer reply = "Restore ID: ";
                        if (!cmd->getId(0, reply))
                            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Failed to get ID.");
                        res->setResult(reply.str());
                    }
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to delete DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        StringArray & wuids = req.getWuids();
        for(unsigned i = 0; i < wuids.ordinality(); ++i)
        {
            if (markWUFailed(factory, wuids.item(i)))
                factory->deleteWorkUnit(wuids.item(i));
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to delete DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        if (markWUFailed(factory, req.getWuid()))
            resp.setResult(factory->deleteWorkUnit(req.getWuid()));
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to submit DFU workunit. Permission denied.");

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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Failed to abort DFU workunit. Permission denied.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(req.getWuid());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

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
        if (!context.validateFeatureAccess(DFU_EX_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_EX_ACCESS_DENIED, "Failed to get DFU Exceptions. Permission denied.");

        IArrayOf<IEspDFUException> result;
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(req.getWuid());
        if(!wu)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

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

bool CFileSprayEx::onSprayFixed(IEspContext &context, IEspSprayFixed &req, IEspSprayFixedResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Spray. Permission denied.");

        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;

        const char* destNodeGroup = req.getDestGroup();
        if(destNodeGroup == NULL || *destNodeGroup == '\0')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination node group not specified.");

        MemoryBuffer& srcxml = (MemoryBuffer&)req.getSrcxml();
        const char* srcip = req.getSourceIP();
        const char* srcfile = req.getSourcePath();
        if(srcxml.length() == 0)
        {
            if(!srcip || !*srcip)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source network IP not specified.");
            if(!srcfile || !*srcfile)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source file not specified.");
        }

        const char* destname = req.getDestLogicalName();
        if(!destname || !*destname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid destination filename");
        destname = lfn.get();

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
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        if(srcxml.length() == 0)
        {
            RemoteMultiFilename rmfn;
            SocketEndpoint ep(srcip);
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

        if (req.getFailIfNoSourceFile())
            options->setFailIfNoSourceFile(true);

        if (req.getRecordStructurePresent())
            options->setRecordStructurePresent(true);

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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Spray. Permission denied.");

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
        const char* srcip = req.getSourceIP();
        const char* srcfile = req.getSourcePath();
        if(srcxml.length() == 0)
        {
            if(!srcip || !*srcip)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source network IP not specified.");
            if(!srcfile || !*srcfile)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source file not specified.");
        }

        const char* destname = req.getDestLogicalName();
        if(!destname || !*destname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination file not specified.");
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(destname))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid destination filename");
        destname = lfn.get();

        if (ipAddr.length() > 0)
            ParseLogicalPath(destname, ipAddr.str(), NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);
        else
            ParseLogicalPath(destname, destNodeGroup, NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        wu->setClusterName(gName.str());
        wu->setJobName(destTitle.str());
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
        wu->setCommand(DFUcmd_import);

        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();

        if(srcxml.length() == 0)
        {
            RemoteMultiFilename rmfn;
            SocketEndpoint ep(srcip);
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Replicate. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        StringBuffer jobname = "Replicate: ";
        jobname.append(srcname);
        wu->setJobName(jobname.str());
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
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

const char* CFileSprayEx::getDropZoneDirByIP(const char* ip, StringBuffer& dir)
{
    if (!ip || !*ip)
        return NULL;

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return NULL;

    Owned<IConstMachineInfo> machine = env->getMachineByAddress(ip);
    if (!machine)
    {
        IpAddress ipAddr;
        ipAddr.ipset(ip);
        if (!ipAddr.isLocal())
            return NULL;
        machine.setown(env->getMachineForLocalHost());
        if (!machine)
            return NULL;
    }
    SCMStringBuffer computer, directory;
    machine->getName(computer);
    if (!computer.length())
        return NULL;

    Owned<IConstDropZoneInfo> dropZone = env->getDropZoneByComputer(computer.str());
    if (!dropZone)
        return NULL;
    dropZone->getDirectory(directory);
    return dir.set(directory.str()).str();
}

bool CFileSprayEx::onDespray(IEspContext &context, IEspDespray &req, IEspDesprayResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FILE_DESPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_DESPRAY_ACCESS_DENIED, "Failed to do Despray. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");

        const char* destip = req.getDestIP();
        StringBuffer fnamebuf(req.getDestPath());
        const char* destfile = fnamebuf.trim().str();

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
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
        wu->setCommand(DFUcmd_export);

        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();

        source->setLogicalName(srcname);

        if(dstxml.length() == 0)
        {
            RemoteFilename rfn;
            SocketEndpoint ep(destip);
            if (isAbsolutePath(destfile))
                rfn.setPath(ep, destfile);
            else
            {
                StringBuffer buf;
                getDropZoneDirByIP(destip, buf);
                if (buf.length())
                    addPathSepChar(buf);
                rfn.setPath(ep, buf.append(destfile).str());
            }
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

        double version = context.getClientVersion();
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Copy. Permission denied.");

        const char* srcname = req.getSourceLogicalName();
        const char* dstname = req.getDestLogicalName();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");
        if(!dstname || !*dstname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination logical file not specified.");

        StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;
        StringBuffer srcNodeGroup, destNodeGroup;
        bool bRoxie = false;
        const char* destNodeGroupReq = req.getDestGroup();
        if(!destNodeGroupReq || !*destNodeGroupReq)
        {
            getNodeGroupFromLFN(destNodeGroup, srcname, context.queryUserId(), context.queryPassword());
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
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "invalid destination filename");
            dstname = lfn.get();
        }

        ParseLogicalPath(dstname, destNodeGroup.str(), NULL, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

        StringBuffer fileMask; 
        constructFileMask(destTitle.str(), fileMask);

        const char* srcDali = req.getSourceDali();
        bool supercopy = req.getSuperCopy();
        if (supercopy) 
        {
            StringBuffer user, passwd;
            context.getUserID(user);
            context.getPassword(passwd);
            StringBuffer u(user);
            StringBuffer p(passwd);
            Owned<INode> foreigndali;
            if (srcDali) 
            {
                SocketEndpoint ep(srcDali);
                foreigndali.setown(createINode(ep));
                const char* srcu = req.getSrcusername();
                if(srcu && *srcu)
                {
                    u.clear().append(srcu);
                    p.clear().append(req.getSrcpassword());
                }
            }
            Owned<IUserDescriptor> udesc=createUserDescriptor();
            udesc->set(u.str(),p.str());
            if (!queryDistributedFileDirectory().isSuperFile(srcname,udesc,foreigndali))
                supercopy = false;
        }

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        wu->setJobName(dstname);
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do Rename. Permission denied.");

        const char* srcname = req.getSrcname();
        const char* dstname = req.getDstname();
        if(!srcname || !*srcname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Source logical file not specified.");
        if(!dstname || !*dstname)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Destination logical file not specified.");

        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();

        StringBuffer destTitle;
        ParseLogicalPath(req.getDstname(), destTitle);

        wu->setJobName(destTitle.str());
        wu->setQueue(m_QueueLabel.str());
        StringBuffer user, passwd;
        wu->setUser(context.getUserID(user).str());
        wu->setPassword(context.getPassword(passwd).str());
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
        if (!context.validateFeatureAccess(DFU_WU_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to DFU workunit is denied.");

        if (*req.getWuid())
        {
            Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
            Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(req.getWuid(), false);
            if(!wu)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Dfu workunit %s not found.", req.getWuid());

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

int CFileSprayEx::doFileCheck(const char* mask, const char* netaddr, const char* osStr, const char* path)
{
    int iRet = 1;
    if (mask && *mask)
    {
        char *str = (char *) mask + strlen(mask) - 4;
        if (!stricmp(str, ".cfg") || !stricmp(str, ".log"))
            iRet = 0;
    }
    else    if (netaddr && *netaddr && path && *path)
    {
        iRet = 2;

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        factory->validateCache();
        Owned<IConstEnvironment> env = factory->openEnvironment();
        Owned<IPropertyTree> pEnvRoot = &env->getPTree();
        IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree("Software");
        Owned<IPropertyTree> pRoot = createPTreeFromXMLString("<Environment/>");
        IPropertyTree* pSoftware = pRoot->addPropTree("Software", createPTree("Software"));
        if (pEnvSoftware && pSoftware)
        {
            Owned<IPropertyTreeIterator> it = pEnvSoftware->getElements("DropZone");
            ForEach(*it)
            {
                const char* pszComputer = it->query().queryProp("@computer");
                if (!strcmp(pszComputer, "."))
                    pszComputer = "localhost";

                StringBuffer xpath, sNetAddr;
                xpath.appendf("Hardware/Computer[@name='%s']/@netAddress", pszComputer);
                const char* pszNetAddr = pEnvRoot->queryProp(xpath.str());
                if (strcmp(pszNetAddr, "."))
                {
                    sNetAddr.append(pszNetAddr);
                }
                else
                {
                    StringBuffer ipStr;
                    IpAddress ipaddr = queryHostIP();
                    ipaddr.getIpText(ipStr);
                    if (ipStr.length() > 0)
                    {
#ifdef MACHINE_IP
                        sNetAddr.append(MACHINE_IP);
#else
                        sNetAddr.append(ipStr.str());
#endif
                    }
                }
#ifdef MACHINE_IP
                if ((sNetAddr.length() > 0) && !stricmp(sNetAddr.str(), MACHINE_IP))
#else
                if ((sNetAddr.length() > 0) && !stricmp(sNetAddr.str(), netaddr))
#endif
                {
                    StringBuffer dir;
                    IPropertyTree* pDropZone = pSoftware->addPropTree("DropZone", &it->get());
                    pDropZone->getProp("@directory", dir);

                    if (osStr && *osStr)
                    {
                        int os = atoi(osStr);
                        const char pathSep = (os == OS_WINDOWS) ? '\\' : '/';
                        dir.replace(pathSep=='\\'?'/':'\\', pathSep);
                    }

                    if ((dir.length() > 0) && !strnicmp(path, dir.str(), dir.length()))
                    {
                        iRet = 0;
                        break;
                    }
                }
            }
        }
    }

    return iRet;
}

bool CFileSprayEx::onFileList(IEspContext &context, IEspFileListRequest &req, IEspFileListResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do FileList. Permission denied.");

        const char* path = req.getPath();
        if (!path || !*path)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Path not specified.");

        double version = context.getClientVersion();
        const char* netaddr = req.getNetaddr();
        const char* mask = req.getMask();
        bool directoryOnly = req.getDirectoryOnly();

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

        int checkReturn = doFileCheck(mask, netaddr, osStr, sPath.str());
        if (checkReturn > 1)
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");
        else if (checkReturn > 0)
            throw MakeStringException(ECLWATCH_ACCESS_TO_FILE_DENIED, "Access to the file path denied.");

        RemoteFilename rfn;
        SocketEndpoint ep;
#ifdef MACHINE_IP
        ep.set(MACHINE_IP);
#else
        ep.set(netaddr);
#endif
        rfn.setPath(ep, sPath.str());
        Owned<IFile> f = createIFile(rfn);
        if(!f->isDirectory())
            throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", path);

        IArrayOf<IEspPhysicalFileStruct> files;
        if (mask && !*mask)
            mask = NULL;

        Owned<IDirectoryIterator> di = f->directoryFiles(NULL, false, true);
        if(di.get() != NULL)
        {
            ForEach(*di)
            {
                StringBuffer fname;
                di->getName(fname);

                if (fname.length() == 0 || (directoryOnly && !di->isDir()) || (!di->isDir() && mask && !WildMatch(fname.str(), mask, true)))
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

        if (mask && *mask)
            resp.setMask(mask);

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

bool CFileSprayEx::onDfuMonitor(IEspContext &context, IEspDfuMonitorRequest &req, IEspDfuMonitorResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Failed to do DfuMonitor. Permission denied.");

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
                    SocketEndpoint ep;
                    ep.set(ip);
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

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

bool CFileSprayEx::getDropZoneFiles(IEspContext &context, const char* netaddr, const char* osStr, const char* path,
                                    IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp)
{
    bool directoryOnly = req.getDirectoryOnly();

    int checkReturn = doFileCheck(NULL, netaddr, osStr, path);
    if (checkReturn > 1)
        throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");
    else if (checkReturn > 0)
        throw MakeStringException(ECLWATCH_ACCESS_TO_FILE_DENIED, "Access to the file path denied.");

    RemoteFilename rfn;
    SocketEndpoint ep;
#ifdef MACHINE_IP
    ep.set(MACHINE_IP);
#else
    ep.set(netaddr);
#endif

    rfn.setPath(ep, path);
    Owned<IFile> f = createIFile(rfn);
    if(!f->isDirectory())
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

bool CFileSprayEx::onDropZoneFiles(IEspContext &context, IEspDropZoneFilesRequest &req, IEspDropZoneFilesResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        const char* netAddress = req.getNetAddress();
        const char* directory = req.getPath();
        const char* subfolder = req.getSubfolder();

        StringBuffer netAddressStr, directoryStr, osStr;
        if (netAddress && *netAddress && directory && *directory)
        {
            netAddressStr.append(netAddress);
            directoryStr.append(directory);
        }

        IArrayOf<IEspDropZone> dropZoneList;

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> m_constEnv = factory->openEnvironment();
        Owned<IPropertyTree> pEnvRoot = &m_constEnv->getPTree();
        IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree("Software");
        if (pEnvSoftware)
        {
            Owned<IPropertyTreeIterator> it = pEnvSoftware->getElements("DropZone");
            ForEach(*it)
            {
                IPropertyTree& pDropZone = it->query();

                //get IP Address of the computer associated with this drop zone
                const char* pszName = pDropZone.queryProp("@name");
                const char* pszComputer = pDropZone.queryProp("@computer");
                if (!strcmp(pszComputer, "."))
                    pszComputer = "localhost";

                StringBuffer xpath;
                xpath.appendf("Hardware/Computer[@name='%s']/@netAddress", pszComputer);

                StringBuffer sNetAddr;
                const char* pszNetAddr = pEnvRoot->queryProp(xpath.str());
                if (strcmp(pszNetAddr, "."))
                {
                    sNetAddr.append(pszNetAddr);
                }
                else
                {
                    StringBuffer ipStr;
                    IpAddress ipaddr = queryHostIP();
                    ipaddr.getIpText(ipStr);
                    if (ipStr.length() > 0)
                    {
#ifdef MACHINE_IP
                        sNetAddr.append(MACHINE_IP);
#else
                        sNetAddr.append(ipStr.str());
#endif
                    }
                }

                Owned<IConstMachineInfo> machine;
                if (strcmp(pszNetAddr, "."))
                    machine.setown(m_constEnv->getMachineByAddress(sNetAddr.str()));
                else
                {
                    machine.setown(m_constEnv->getMachineByAddress(pszNetAddr));
                    if (!machine)
                        machine.setown(m_constEnv->getMachineByAddress(sNetAddr.str()));
                }

                StringBuffer dir;
                pDropZone.getProp("@directory", dir);

                Owned<IEspDropZone> aDropZone= createDropZone("","");

                if (machine)
                {
                    if (machine->getOS() == MachineOsLinux || machine->getOS() == MachineOsSolaris)
                    {
                        dir.replace('\\', '/');//replace all '\\' by '/'
                        aDropZone->setLinux("true");
                        osStr = "1";
                    }
                    else
                    {
                        dir.replace('/', '\\');
                        dir.replace('$', ':');
                        osStr = "0";
                    }
                }

                aDropZone->setComputer(pszComputer);
                aDropZone->setPath(dir.str());
                aDropZone->setName(pszName);
                aDropZone->setNetAddress(sNetAddr.str());

                dropZoneList.append(*aDropZone.getClear());
            }
        }

        if (dropZoneList.ordinality())
            resp.setDropZones(dropZoneList);

        if (netAddressStr.length() < 1)
            return true;

        char pathSep = '/';
        if (osStr && *osStr)
        {
            int os = atoi(osStr);
            if (os == OS_WINDOWS)
                pathSep = '\\';
        }

        directoryStr.replace(pathSep=='\\'?'/':'\\', pathSep);

        if (subfolder && *subfolder)
        {
            if (*(directoryStr.str() + directoryStr.length() -1) != pathSep)
                directoryStr.append( pathSep );

            directoryStr.append(subfolder);
        }

        if (*(directoryStr.str() + directoryStr.length() -1) != pathSep)
            directoryStr.append( pathSep );

        getDropZoneFiles(context, netAddressStr.str(), osStr.str(), directoryStr.str(), req, resp);

        if (pathSep=='\\')
            directoryStr.replaceString("\\", "\\\\");

        resp.setNetAddress(netAddressStr.str());
        resp.setPath(directoryStr.str());
        resp.setOS(atoi(osStr.str()));
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Full, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        const char* netAddress = req.getNetAddress();
        const char* directory = req.getPath();
        const char* osStr = req.getOS();
        StringArray & files = req.getNames();

        if (!netAddress || !*netAddress || !directory || !*directory)
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone not specified.");

        if (!files.ordinality())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");

        char pathSep = '/';
        StringBuffer sPath(directory);
        if (osStr && *osStr)
        {
            int os = atoi(osStr);
            pathSep = (os == OS_WINDOWS) ? '\\' : '/';
            sPath.replace(pathSep=='\\'?'/':'\\', pathSep);
            if (*(sPath.str() + sPath.length() -1) != pathSep)
                sPath.append( pathSep );
        }

        int checkReturn = doFileCheck(NULL, netAddress, osStr, sPath.str());
        if (checkReturn > 1)
            throw MakeStringException(ECLWATCH_DROP_ZONE_NOT_FOUND, "Dropzone is not found in the environment settings.");
        else if (checkReturn > 0)
            throw MakeStringException(ECLWATCH_ACCESS_TO_FILE_DENIED, "Access to the file path denied.");

        RemoteFilename rfn;
        SocketEndpoint ep;
#ifdef MACHINE_IP
        ep.set(MACHINE_IP);
#else
        ep.set(netAddress);
#endif

        rfn.setPath(ep, sPath.str());
        Owned<IFile> f = createIFile(rfn);
        if(!f->isDirectory())
            throw MakeStringException(ECLWATCH_INVALID_DIRECTORY, "%s is not a directory.", directory);

        bool bAllSuccess = true;
        IArrayOf<IEspDFUActionResult> results;

        for(unsigned i = 0; i < files.ordinality(); ++i)
        {
            const char* file = files.item(i);
            if (!file || !*file)
                continue;

            Owned<IEspDFUActionResult> res = createDFUActionResult("", "");
            res->setID(files.item(i));
            res->setAction("Delete");
            res->setResult("Success");

            try
            {
                StringBuffer fileToDelete = sPath;
                if (*(fileToDelete.str() + fileToDelete.length() -1) != pathSep)
                    fileToDelete.append( pathSep );
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

                StringBuffer failedMsg = "Failed: ";
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
        if (!context.validateFeatureAccess(FILE_SPRAY_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_FILE_SPRAY_ACCESS_DENIED, "Permission denied.");

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> environment = factory->openEnvironment();
        Owned<IPropertyTree> root = &environment->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

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
