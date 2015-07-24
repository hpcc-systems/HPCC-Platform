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

#include "ws_workunitsService.hpp"
#include "ws_fs.hpp"
#include "jlib.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dfuwu.hpp"
#include "eclhelper.hpp"
#include "roxiecontrol.hpp"
#include "dfuutil.hpp"
#include "dautils.hpp"
#include "referencedfilelist.hpp"
#include "httpclient.hpp"

#define DALI_FILE_LOOKUP_TIMEOUT (1000*15*1)  // 15 seconds

const unsigned ROXIECONNECTIONTIMEOUT = 1000;   //1 second
const unsigned ROXIECONTROLQUERYTIMEOUT = 3000; //3 second
const unsigned ROXIECONTROLQUERIESTIMEOUT = 30000; //30 second
const unsigned ROXIELOCKCONNECTIONTIMEOUT = 60000; //60 second

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

bool isRoxieProcess(const char *process)
{
    if (!process)
        return false;
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        return false;

    Owned<IPropertyTree> root = &env->getPTree();
    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", process);
    return root->hasProp(xpath.str());
}

void checkUseEspOrDaliIP(SocketEndpoint &ep, const char *ip, const char *esp)
{
    if (!ip || !*ip)
        return;
    ep.set(ip, 7070);
    if (ep.isLoopBack() || *ip=='.' || (ip[0]=='0' && ip[1]=='.'))
        ep.ipset(esp);
}

static IClientWsWorkunits *ensureWsWorkunitsClient(IClientWsWorkunits *ws, IEspContext *ctx, const char *netAddress)
{
    if (ws)
        return LINK(ws);
    StringBuffer url;
    if (netAddress && *netAddress)
        url.appendf("http://%s%s/WsWorkunits", netAddress, (!strchr(netAddress, ':')) ? ":8010" : "");
    else
    {
        if (!ctx)
            throw MakeStringException(ECLWATCH_INVALID_IP, "Missing WsWorkunits service address");
        StringBuffer ip;
        short port = 0;
        ctx->getServAddress(ip, port);
        url.appendf("http://%s:%d/WsWorkunits", ip.str(), port);
    }
    Owned<IClientWsWorkunits> cws = createWsWorkunitsClient();
    cws->addServiceUrl(url);
    if (ctx && ctx->queryUserId() && *ctx->queryUserId())
        cws->setUsernameToken(ctx->queryUserId(), ctx->queryPassword(), NULL);
    return cws.getClear();
}

IClientWUQuerySetDetailsResponse *fetchQueryDetails(IClientWsWorkunits *_ws, IEspContext *ctx, const char *netAddress, const char *target, const char *queryid)
{
    Owned<IClientWsWorkunits> ws = ensureWsWorkunitsClient(_ws, ctx, netAddress);

    //using existing WUQuerysetDetails rather than extending WUQueryDetails, to support copying query meta data from prior releases
    Owned<IClientWUQuerySetDetailsRequest> reqQueryInfo = ws->createWUQuerysetDetailsRequest();
    reqQueryInfo->setClusterName(target);
    reqQueryInfo->setQuerySetName(target);
    reqQueryInfo->setFilter(queryid);
    reqQueryInfo->setFilterType("Id");
    return ws->WUQuerysetDetails(reqQueryInfo);
}

void fetchRemoteWorkunit(IClientWsWorkunits *_ws, IEspContext *ctx, const char *netAddress, const char *queryset, const char *query, const char *wuid, StringBuffer &name, StringBuffer &xml, StringBuffer &dllname, MemoryBuffer &dll, StringBuffer &daliServer)
{
    Owned<IClientWsWorkunits> ws = ensureWsWorkunitsClient(_ws, ctx, netAddress);
    Owned<IClientWULogFileRequest> req = ws->createWUFileRequest();
    if (queryset && *queryset)
        req->setQuerySet(queryset);
    if (query && *query)
        req->setQuery(query);
    if (wuid && *wuid)
        req->setWuid(wuid);
    req->setType("xml");
    Owned<IClientWULogFileResponse> resp = ws->WUFile(req);
    if (!resp || resp->getExceptions().ordinality() || !resp->getThefile().length())
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Cannot retrieve remote workunit");
    xml.append(resp->getThefile().length(), resp->getThefile().toByteArray());

    req->setType("dll");
    resp.setown(ws->WUFile(req));
    if (!resp || resp->getExceptions().ordinality() || !resp->getThefile().length())
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Cannot retrieve remote workunit shared object");
    dll.append(resp->getThefile());
    dllname.append(resp->getFileName());
    name.append(resp->getQueryName());
    SocketEndpoint ep;
    checkUseEspOrDaliIP(ep, resp->getDaliServer(), netAddress);
    if (!ep.isNull())
        ep.getUrlStr(daliServer);
}

void fetchRemoteWorkunitAndQueryDetails(IClientWsWorkunits *_ws, IEspContext *ctx, const char *netAddress, const char *queryset, const char *query, const char *wuid, StringBuffer &name, StringBuffer &xml, StringBuffer &dllname, MemoryBuffer &dll, StringBuffer &daliServer, Owned<IClientWUQuerySetDetailsResponse> &respQueryInfo)
{
    Owned<IClientWsWorkunits> ws = ensureWsWorkunitsClient(_ws, ctx, netAddress);
    fetchRemoteWorkunit(ws, ctx, netAddress, queryset, query, wuid, name, xml, dllname, dll, daliServer);
    respQueryInfo.setown(fetchQueryDetails(ws, ctx, netAddress, queryset, query));
}

void doWuFileCopy(IClientFileSpray &fs, IEspWULogicalFileCopyInfo &info, const char *logicalname, const char *cluster, bool isRoxie, bool supercopy)
{
    try
    {
        Owned<IClientCopy> req = fs.createCopyRequest();
        req->setSourceLogicalName(logicalname);
        req->setDestLogicalName(logicalname);
        req->setDestGroup(cluster);
        req->setSuperCopy(supercopy);
        if (isRoxie)
            req->setDestGroupRoxie("Yes");

        Owned<IClientCopyResponse> resp = fs.Copy(req);
        info.setDfuCopyWuid(resp->getResult());
    }
    catch (IException *e)
    {
        StringBuffer msg;
        info.setDfuCopyError(e->errorMessage(msg).str());
        e->Release();
    }
}

bool copyWULogicalFiles(IEspContext &context, IConstWorkUnit &cw, const char *cluster, bool copyLocal, IEspWUCopyLogicalClusterFileSections &lfinfo)
{
    if (isEmpty(cluster))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "copyWULogicalFiles Cluster parameter not set.");

    Owned<IUserDescriptor> udesc = createUserDescriptor();
    udesc->set(context.queryUserId(), context.queryPassword());

    IArrayOf<IEspWULogicalFileCopyInfo> foreign;
    IArrayOf<IEspWULogicalFileCopyInfo> onCluster;
    IArrayOf<IEspWULogicalFileCopyInfo> notOnCluster;
    IArrayOf<IEspWULogicalFileCopyInfo> notFound;

    Owned<IClientFileSpray> fs;
    if (copyLocal)
    {
        fs.setown(createFileSprayClient());
        VStringBuffer url("http://.:%d/FileSpray", 8010);
        fs->addServiceUrl(url.str());
    }

    bool isRoxie = isRoxieProcess(cluster);

    Owned<IConstWUGraphIterator> graphs = &cw.getGraphs(GraphTypeActivities);
    ForEach(*graphs)
    {
        Owned <IPropertyTree> xgmml = graphs->query().getXGMMLTree(false);
        Owned<IPropertyTreeIterator> iter = xgmml->getElements(".//node");
        ForEach(*iter)
        {
            try
            {
                IPropertyTree &node = iter->query();
                ThorActivityKind kind = (ThorActivityKind) node.getPropInt("att[@name='_kind']/@value", TAKnone);

                if(kind==TAKdiskwrite || kind==TAKindexwrite || kind==TAKcsvwrite || kind==TAKxmlwrite || kind==TAKjsonwrite)
                    continue;
                if (node.getPropBool("att[@name='_isSpill']/@value") || node.getPropBool("att[@name='_isTransformSpill']/@value"))
                    continue;

                Owned<IEspWULogicalFileCopyInfo> info = createWULogicalFileCopyInfo();
                const char *logicalname = node.queryProp("att[@name='_indexFileName']/@value");
                if (logicalname)
                    info->setIsIndex(true);
                else
                    logicalname = node.queryProp("att[@name='_fileName']/@value");
                info->setLogicalName(logicalname);
                if (logicalname)
                {
                    if (!strnicmp("~foreign::", logicalname, 10))
                        foreign.append(*info.getClear());
                    else
                    {
                        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalname, udesc);
                        if(!df)
                            notFound.append(*info.getClear());
                        else if (df->findCluster(cluster)!=NotFound)
                        {
                            onCluster.append(*info.getClear());
                        }
                        else
                        {
                            StringArray clusters;
                            df->getClusterNames(clusters);
                            info->setClusters(clusters);
                            if (copyLocal)
                            {
                                StringBuffer wuid;
                                bool supercopy = queryDistributedFileDirectory().isSuperFile(logicalname, udesc, NULL);
                                doWuFileCopy(*fs, *info, logicalname, cluster, isRoxie, supercopy);
                            }
                            notOnCluster.append(*info.getClear());
                        }
                    }
                }
            }
            catch(IException *e)
            {
                e->Release();
            }
        }
        lfinfo.setClusterName(cluster);
        lfinfo.setNotOnCluster(notOnCluster);
        lfinfo.setOnCluster(onCluster);
        lfinfo.setForeign(foreign);
        lfinfo.setNotFound(notFound);
    }

    return true;
}

void copyWULogicalFilesToTarget(IEspContext &context, IConstWUClusterInfo &clusterInfo, IConstWorkUnit &cw, IArrayOf<IConstWUCopyLogicalClusterFileSections> &clusterfiles, bool doLocalCopy)
{
    const StringArray &thors = clusterInfo.getThorProcesses();
    ForEachItemIn(i, thors)
    {
        Owned<IEspWUCopyLogicalClusterFileSections> files = createWUCopyLogicalClusterFileSections();
        copyWULogicalFiles(context, cw, thors.item(i), doLocalCopy, *files);
        clusterfiles.append(*files.getClear());
    }
    SCMStringBuffer roxie;
    clusterInfo.getRoxieProcess(roxie);
    if (roxie.length())
    {
        Owned<IEspWUCopyLogicalClusterFileSections> files = createWUCopyLogicalClusterFileSections();
        copyWULogicalFiles(context, cw, roxie.str(), doLocalCopy, *files);
        clusterfiles.append(*files.getClear());
    }
}

void QueryFilesInUse::loadTarget(IPropertyTree *t, const char *target, unsigned flags)
{
    if (!target || !*target)
        return;

    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
    if (!clusterInfo || !(clusterInfo->getPlatform() == RoxieCluster))
        return;

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(target, true);
    if (!queryRegistry)
        return;

    SCMStringBuffer process;
    clusterInfo->getRoxieProcess(process);
    if (!process.length())
        return;

    Owned<IHpccPackageSet> ps = createPackageSet(process.str());
    const IHpccPackageMap *pm = (ps) ? ps->queryActiveMap(target) : NULL;
    const char *pmid = (pm) ? pm->queryPackageId() : NULL;

    VStringBuffer xpath("%s/@pmid", target);
    const char *pmidPrev = t->queryProp(xpath);
    if ((flags & UFO_RELOAD_TARGETS_CHANGED_PMID) && (pmid || pmidPrev))
    {
        if (!(pmid && pmidPrev) || !streq(pmid, pmidPrev))
            t->removeProp(target);
    }
    IPropertyTree *targetTree = ensurePTree(t, target);
    if (pm)
        targetTree->setProp("@pmid", pmid);

    if (flags & UFO_REMOVE_QUERIES_NOT_IN_QUERYSET)
    {
        Owned<IPropertyTreeIterator> cachedQueries = targetTree->getElements("Query");
        ForEach(*cachedQueries)
        {
            IPropertyTree &cachedQuery = cachedQueries->query();
            VStringBuffer xpath("Query[@id='%s']", cachedQuery.queryProp("@id"));
            if (!queryRegistry->hasProp(xpath))
                targetTree->removeTree(&cachedQuery);
        }
    }

    Owned<IPropertyTreeIterator> queries = queryRegistry->getElements("Query");
    ForEach(*queries)
    {
        if (aborting)
            return;
        IPropertyTree &query = queries->query();
        const char *queryid = query.queryProp("@id");
        if (!queryid || !*queryid)
            continue;
        const char *wuid = query.queryProp("@wuid");
        if (!wuid || !*wuid)
            continue;

        const char *pkgid=NULL;
        if (pm)
         {
             const IHpccPackage *pkg = pm->matchPackage(queryid);
             if (pkg)
                 pkgid = pkg->queryId();
         }
        VStringBuffer xpath("Query[@id='%s']", queryid);
        IPropertyTree *queryTree = targetTree->queryPropTree(xpath);
        if (queryTree)
        {
            const char *cachedPkgid = queryTree->queryProp("@pkgid");
            if (pkgid && *pkgid)
            {
                if (!(flags & UFO_RELOAD_MAPPED_QUERIES) && (cachedPkgid && streq(pkgid, cachedPkgid)))
                    continue;
            }
            else if (!cachedPkgid || !*cachedPkgid)
                continue;
            targetTree->removeTree(queryTree);
            queryTree = NULL;
        }

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
        if (!cw)
            continue;

        queryTree = targetTree->addPropTree("Query", createPTree("Query"));
        queryTree->setProp("@target", target); //for reference when searching across targets
        queryTree->setProp("@id", queryid);
        if (pkgid && *pkgid)
            queryTree->setProp("@pkgid", pkgid);

        IUserDescriptor **roxieUser = roxieUserMap.getValue(target);
        Owned<IReferencedFileList> wufiles = createReferencedFileList(roxieUser ? *roxieUser : NULL, true, true);
        wufiles->addFilesFromQuery(cw, pm, queryid);
        if (aborting)
            return;
        wufiles->resolveFiles(process.str(), NULL, NULL, NULL, true, true, false);

        Owned<IReferencedFileIterator> files = wufiles->getFiles();
        ForEach(*files)
        {
            if (aborting)
                return;
            IReferencedFile &rf = files->query();
            //if (!(rf.getFlags() & RefSubFile))
            //    continue;
            const char *lfn = rf.getLogicalName();
            if (!lfn || !*lfn)
                continue;

            if (!queryTree->hasProp(xpath.setf("File[@lfn='%s']", lfn)))
            {
                IPropertyTree *fileTree = queryTree->addPropTree("File", createPTree("File"));
                fileTree->setProp("@lfn", lfn);
                if (rf.getFlags() & RefFileSuper)
                    fileTree->setPropBool("@super", true);
                if (rf.getFlags() & RefFileNotFound)
                    fileTree->setPropBool("@notFound", true);
                const char *fpkgid = rf.queryPackageId();
                if (fpkgid && *fpkgid)
                    fileTree->setProp("@pkgid", fpkgid);
                if (rf.getFileSize())
                    fileTree->setPropInt64("@size", rf.getFileSize());
                if (rf.getNumParts())
                    fileTree->setPropInt("@numparts", rf.getNumParts());
            }
        }
    }
}

void QueryFilesInUse::loadTargets(IPropertyTree *t, unsigned flags)
{
    Owned<IStringIterator> targets = getTargetClusters("RoxieCluster", NULL);
    SCMStringBuffer s;
    ForEach(*targets)
    {
        if (aborting)
            return;
        loadTarget(t, targets->str(s).str(), flags);
    }
}

IPropertyTreeIterator *QueryFilesInUse::findAllQueriesUsingFile(const char *lfn)
{
    if (!lfn || !*lfn)
        return NULL;

    Owned<IPropertyTree> t = getTree();
    VStringBuffer xpath("*/Query[File/@lfn='%s']", lfn);
    return t->getElements(xpath);
}

IPropertyTreeIterator *QueryFilesInUse::findQueriesUsingFile(const char *target, const char *lfn, StringAttr &pmid)
{
    if (!lfn || !*lfn)
        return NULL;
    if (!target || !*target)
        return findAllQueriesUsingFile(lfn);
    Owned<IPropertyTree> t = getTree();
    IPropertyTree *targetTree = t->queryPropTree(target);
    if (!targetTree)
        return NULL;
    pmid.set(targetTree->queryProp("@pmid"));

    VStringBuffer xpath("Query[File/@lfn='%s']", lfn);
    return targetTree->getElements(xpath);
}

bool CWsWorkunitsEx::onWUCopyLogicalFiles(IEspContext &context, IEspWUCopyLogicalFilesRequest &req, IEspWUCopyLogicalFilesResponse &resp)
{
    StringBuffer wuid = req.getWuid();
    WsWuHelpers::checkAndTrimWorkunit("WUCopyLogicalFiles", wuid);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s", wuid.str());

    resp.setWuid(wuid.str());

    StringAttr cluster;
    if (notEmpty(req.getCluster()))
        cluster.set(req.getCluster());
    else
        cluster.set(cw->queryClusterName());
    if (!isValidCluster(cluster))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster.str());

    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster.str());

    IArrayOf<IConstWUCopyLogicalClusterFileSections> clusterfiles;
    copyWULogicalFilesToTarget(context, *clusterInfo, *cw, clusterfiles, req.getCopyLocal());
    resp.setClusterFiles(clusterfiles);

    return true;
}

static inline unsigned remainingMsWait(unsigned wait, unsigned start)
{
    if (wait==0 || wait==(unsigned)-1)
        return wait;
    unsigned waited = msTick()-start;
    return (wait>waited) ? wait-waited : 0;
}

bool reloadCluster(IConstWUClusterInfo *clusterInfo, unsigned wait)
{
    if (0==wait || !clusterInfo || clusterInfo->getPlatform()!=RoxieCluster)
        return true;

    const SocketEndpointArray &addrs = clusterInfo->getRoxieServers();
    if (addrs.length())
    {
        try
        {
            Owned<IPropertyTree> result = sendRoxieControlAllNodes(addrs.item(0), "<control:reload/>", false, wait);
            const char *status = result->queryProp("Endpoint[1]/Status");
            if (!status || !strieq(status, "ok"))
                return false;
        }
        catch(IMultiException *me)
        {
            StringBuffer err;
            DBGLOG("ERROR control:reloading roxie query info %s", me->errorMessage(err.append(me->errorCode()).append(' ')).str());
            me->Release();
            return false;
        }
        catch(IException *e)
        {
            StringBuffer err;
            DBGLOG("ERROR control:reloading roxie query info %s", e->errorMessage(err.append(e->errorCode()).append(' ')).str());
            e->Release();
            return false;
        }
    }
    return true;
}

bool reloadCluster(const char *cluster, unsigned wait)
{
    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
    return (clusterInfo) ? reloadCluster(clusterInfo, wait) : true;
}

static inline void updateQuerySetting(bool ignore, IPropertyTree *queryTree, const char *xpath, int value)
{
    if (ignore || !queryTree)
        return;
    if (value!=0)
        queryTree->setPropInt(xpath, value);
    else
        queryTree->removeProp(xpath);
}

static inline void updateTimeLimitSetting(IPropertyTree *queryTree, bool valueNotSet, int value, IConstQuerySetQuery *srcInfo=NULL)
{
    if (valueNotSet && srcInfo && !srcInfo->getTimeLimit_isNull())
    {
        value = srcInfo->getTimeLimit();
        valueNotSet=false;
    }
    updateQuerySetting(valueNotSet, queryTree, "@timeLimit", value);
}

static inline void updateWarnTimeLimitSetting(IPropertyTree *queryTree, bool valueNotSet, int value, IConstQuerySetQuery *srcInfo=NULL)
{
    if (valueNotSet && srcInfo && !srcInfo->getWarnTimeLimit_isNull())
    {
        value = srcInfo->getWarnTimeLimit();
        valueNotSet=false;
    }
    updateQuerySetting(valueNotSet, queryTree, "@warnTimeLimit", value);
}

static inline unsigned __int64 memoryLimitUInt64FromString(const char *value)
{
    if (!value || !*value || !isdigit(*value))
        return 0;
    unsigned __int64 result = (*value - '0');
    const char *s = value+1;
    while (isdigit(*s))
    {
        result = 10 * result + ((*s) - '0');
        s++;
    }
    if (*s)
    {
        const char unit = toupper(*s++);
        if (*s && !strieq("B", s)) //more?
            return 0;
        switch (unit)
        {
            case 'E':
                result <<=60;
                break;
            case 'P':
                result <<=50;
                break;
            case 'T':
                result <<=40;
                break;
            case 'G':
                result <<=30;
                break;
            case 'M':
                result <<=20;
                break;
            case 'K':
                result <<=10;
                break;
            case 'B':
                break;
            default:
                return 0;
        }
    }
    return result;
}

const char memUnitAbbrev[] = {'B', 'K', 'M', 'G', 'T', 'P', 'E'};
#define MAX_MEMUNIT_ABBREV 6

static inline StringBuffer &memoryLimitStringFromUInt64(StringBuffer &s, unsigned __int64 in)
{
    if (!in)
        return s;
    unsigned __int64 value = in;
    unsigned char unit = 0;
    while (!(value & 0x3FF) && unit < MAX_MEMUNIT_ABBREV)
    {
        value >>= 10;
        unit++;
    }
    return s.append(value).append(memUnitAbbrev[unit]);
}

static inline void updateMemoryLimitSetting(IPropertyTree *queryTree, const char *value, IConstQuerySetQuery *srcInfo=NULL)
{
    if (!queryTree)
        return;
    if (!value && srcInfo)
        value = srcInfo->getMemoryLimit();
    if (!value)
        return;
    unsigned __int64 limit = memoryLimitUInt64FromString(value);
    if (0==limit)
        queryTree->removeProp("@memoryLimit");
    else
        queryTree->setPropInt64("@memoryLimit", limit);
}

enum QueryPriority {
    QueryPriorityNone = -1,
    QueryPriorityLow = 0,
    QueryPriorityHigh = 1,
    QueryPrioritySLA = 2,
    QueryPriorityInvalid = 3
};

static inline const char *getQueryPriorityName(int value)
{
    switch (value)
    {
    case QueryPriorityLow:
        return "LOW";
    case QueryPriorityHigh:
        return "HIGH";
    case QueryPrioritySLA:
        return "SLA";
    case QueryPriorityNone:
        return "NONE";
    }
    return "INVALID";
}
static inline void updateQueryPriority(IPropertyTree *queryTree, const char *value, IConstQuerySetQuery *srcInfo=NULL)
{
    if (!queryTree)
        return;
    if ((!value || !*value) && srcInfo)
        value = srcInfo->getPriority();
    if (!value || !*value)
        return;
    int priority = QueryPriorityInvalid;
    if (strieq("LOW", value))
        priority=QueryPriorityLow;
    else if (strieq("HIGH", value))
        priority=QueryPriorityHigh;
    else if (strieq("SLA", value))
        priority=QueryPrioritySLA;
    else if (strieq("NONE", value))
        priority=QueryPriorityNone;

    switch (priority)
    {
    case QueryPriorityInvalid:
        break;
    case QueryPriorityNone:
        queryTree->removeProp("@priority");
        break;
    default:
        queryTree->setPropInt("@priority", priority);
        break;
    }
}

void copyQueryFilesToCluster(IEspContext &context, IConstWorkUnit *cw, const char *remoteIP, const char *remotePrefix, const char *target, const char *srcCluster, const char *queryname, bool overwrite, bool allowForeignFiles)
{
    if (!target || !*target)
        return;

    SCMStringBuffer process;
    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
    if (clusterInfo && clusterInfo->getPlatform()==RoxieCluster)
    {
        clusterInfo->getRoxieProcess(process);
        if (!process.length())
            return;
        Owned<IReferencedFileList> wufiles = createReferencedFileList(context.queryUserId(), context.queryPassword(), allowForeignFiles, false);
        Owned<IHpccPackageSet> ps = createPackageSet(process.str());
        StringBuffer queryid;
        if (queryname && *queryname)
            queryname = queryid.append(queryname).append(".0").str(); //prepublish dummy version number to support fuzzy match like queries="myquery.*" in package
        wufiles->addFilesFromQuery(cw, (ps) ? ps->queryActiveMap(target) : NULL, queryname);
        wufiles->resolveFiles(process.str(), remoteIP, remotePrefix, srcCluster, !overwrite, true, true);
        StringBuffer defReplicateFolder;
        getConfigurationDirectory(NULL, "data2", "roxie", process.str(), defReplicateFolder);
        Owned<IDFUhelper> helper = createIDFUhelper();
        wufiles->cloneAllInfo(helper, overwrite, true, true, clusterInfo->getRoxieRedundancy(), clusterInfo->getChannelsPerNode(), clusterInfo->getRoxieReplicateOffset(), defReplicateFolder);
    }
}

bool CWsWorkunitsEx::isQuerySuspended(const char* query, IConstWUClusterInfo *clusterInfo, unsigned wait, StringBuffer& errorMessage)
{
    try
    {
        if (0==wait || !clusterInfo || clusterInfo->getPlatform()!=RoxieCluster)
            return false;

        const SocketEndpointArray &addrs = clusterInfo->getRoxieServers();
        if (addrs.length() < 1)
            return false;

        StringBuffer control;
        control.appendf("<control:queries><Query id='%s'/></control:queries>",  query);
        Owned<IPropertyTree> result = sendRoxieControlAllNodes(addrs.item(0), control.str(), false, wait);
        if (!result)
            return false;

        Owned<IPropertyTreeIterator> suspendedQueries = result->getElements("Endpoint/Queries/Query[@suspended='1']");
        if (!suspendedQueries->first())
            return false;

        errorMessage.set(suspendedQueries->query().queryProp("@error"));
        return true;
    }
    catch(IMultiException *me)
    {
        StringBuffer err;
        DBGLOG("ERROR control:queries roxie query info %s", me->errorMessage(err.append(me->errorCode()).append(' ')).str());
        me->Release();
        return false;
    }
    catch(IException *e)
    {
        StringBuffer err;
        DBGLOG("ERROR control:queries roxie query info %s", e->errorMessage(err.append(e->errorCode()).append(' ')).str());
        e->Release();
        return false;
    }
}

bool CWsWorkunitsEx::onWUPublishWorkunit(IEspContext &context, IEspWUPublishWorkunitRequest & req, IEspWUPublishWorkunitResponse & resp)
{
    StringBuffer wuid = req.getWuid();
    WsWuHelpers::checkAndTrimWorkunit("WUPublishWorkunit", wuid);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", wuid.str());

    resp.setWuid(wuid.str());

    StringAttr queryName;
    if (notEmpty(req.getJobName()))
        queryName.set(req.getJobName());
    else
        queryName.set(cw->queryJobName());
    if (!queryName.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query/Job name not defined for publishing workunit %s", wuid.str());

    StringAttr target;
    if (notEmpty(req.getCluster()))
        target.set(req.getCluster());
    else
        target.set(cw->queryClusterName());
    if (!target.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Cluster name not defined for publishing workunit %s", wuid.str());
    if (!isValidCluster(target.str()))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", target.str());
    StringBuffer daliIP;
    StringBuffer srcCluster;
    StringBuffer srcPrefix;
    splitDerivedDfsLocation(req.getRemoteDali(), srcCluster, daliIP, srcPrefix, req.getSourceProcess(),req.getSourceProcess(), NULL, NULL);

    if (srcCluster.length())
    {
        if (!isProcessCluster(daliIP, srcCluster))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Process cluster %s not found on %s DALI", srcCluster.str(), daliIP.length() ? daliIP.str() : "local");
    }

    if (!req.getDontCopyFiles())
        copyQueryFilesToCluster(context, cw, daliIP, srcPrefix, target.str(), srcCluster, queryName.str(), req.getUpdateDfs(), req.getAllowForeignFiles());

    WorkunitUpdate wu(&cw->lock());
    if (req.getUpdateWorkUnitName() && notEmpty(req.getJobName()))
        wu->setJobName(req.getJobName());

    StringBuffer queryId;
    WUQueryActivationOptions activate = (WUQueryActivationOptions)req.getActivate();
    addQueryToQuerySet(wu, target.str(), queryName.str(), activate, queryId, context.queryUserId());
    if (req.getMemoryLimit() || !req.getTimeLimit_isNull() || !req.getWarnTimeLimit_isNull() || req.getPriority() || req.getComment())
    {
        Owned<IPropertyTree> queryTree = getQueryById(target.str(), queryId, false);
        updateMemoryLimitSetting(queryTree, req.getMemoryLimit());
        updateQuerySetting(req.getTimeLimit_isNull(), queryTree, "@timeLimit", req.getTimeLimit());
        updateQuerySetting(req.getWarnTimeLimit_isNull(), queryTree, "@warnTimeLimit", req.getWarnTimeLimit());
        updateQueryPriority(queryTree, req.getPriority());
        if (req.getComment())
            queryTree->setProp("@comment", req.getComment());
    }
    wu->commit();
    wu.clear();

    if (queryId.length())
        resp.setQueryId(queryId.str());
    resp.setQueryName(queryName.str());
    resp.setQuerySet(target.str());

    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target.str());
    bool reloadFailed = false;
    if (0!=req.getWait() && !req.getNoReload())
        reloadFailed = !reloadCluster(clusterInfo, (unsigned)req.getWait());
    
    resp.setReloadFailed(reloadFailed);

    double version = context.getClientVersion();
    if (version > 1.38)
    {
        StringBuffer errorMessage;
        if (!reloadFailed && !req.getNoReload() && isQuerySuspended(queryName.str(), clusterInfo, (unsigned)req.getWait(), errorMessage))
        {
            resp.setSuspended(true);
            resp.setErrorMessage(errorMessage);
        }
    }

    return true;
}

bool CWsWorkunitsEx::onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp)
{
    IArrayOf<IEspQuerySet> querySets;
    Owned<IStringIterator> targets = getTargetClusters(NULL, NULL);
    SCMStringBuffer target;
    ForEach(*targets)
    {
        Owned<IEspQuerySet> qs = createQuerySet();
        qs->setQuerySetName(targets->str(target).str());
        querySets.append(*qs.getClear());

    }
    resp.setQuerysets(querySets);
    return true;
}

void addClusterQueryStates(IPropertyTree* queriesOnCluster, const char *target, const char *id, IArrayOf<IEspClusterQueryState>& clusterStates, double version)
{
    if (queriesOnCluster)
        queriesOnCluster = queriesOnCluster->queryPropTree("Endpoint[1]/Queries[1]");
    if (!queriesOnCluster)
        return;

    int reporting = queriesOnCluster->getPropInt("@reporting");

    Owned<IEspClusterQueryState> clusterState = createClusterQueryState();
    clusterState->setCluster(target);

    VStringBuffer xpath("Query[@id='%s']", id);
    IPropertyTree *query = queriesOnCluster->queryPropTree(xpath.str());
    if (!query)
        clusterState->setState("Not Found");
    else
    {
        int suspended = query->getPropInt("@suspended");
        const char* error = query->queryProp("@error");
        if (0==suspended)
            clusterState->setState("Available");
        else
        {
            clusterState->setState("Suspended");
            if (suspended<reporting)
                clusterState->setMixedNodeStates(true);
        }
        if (error && *error)
            clusterState->setErrors(error);
    }

    clusterStates.append(*clusterState.getClear());
}

void gatherQuerySetQueryDetails(IEspContext &context, IPropertyTree *query, IEspQuerySetQuery *queryInfo, const char *cluster, IPropertyTree *queriesOnCluster)
{
    queryInfo->setId(query->queryProp("@id"));
    queryInfo->setName(query->queryProp("@name"));
    queryInfo->setDll(query->queryProp("@dll"));
    queryInfo->setWuid(query->queryProp("@wuid"));
    queryInfo->setSuspended(query->getPropBool("@suspended", false));
    if (query->hasProp("@memoryLimit"))
    {
        StringBuffer s;
        memoryLimitStringFromUInt64(s, query->getPropInt64("@memoryLimit"));
        queryInfo->setMemoryLimit(s);
    }
    if (query->hasProp("@timeLimit"))
        queryInfo->setTimeLimit(query->getPropInt("@timeLimit"));
    if (query->hasProp("@warnTimeLimit"))
        queryInfo->setWarnTimeLimit(query->getPropInt("@warnTimeLimit"));
    if (query->hasProp("@priority"))
        queryInfo->setPriority(getQueryPriorityName(query->getPropInt("@priority")));
    if (query->hasProp("@comment"))
        queryInfo->setComment(query->queryProp("@comment"));
    if (query->hasProp("@snapshot"))
        queryInfo->setSnapshot(query->queryProp("@snapshot"));
    double version = context.getClientVersion();
    if (version >= 1.46)
    {
        queryInfo->setPublishedBy(query->queryProp("@publishedBy"));
        queryInfo->setIsLibrary(query->getPropBool("@isLibrary"));
    }
    if (queriesOnCluster)
    {
        IArrayOf<IEspClusterQueryState> clusters;
        addClusterQueryStates(queriesOnCluster, cluster, query->queryProp("@id"), clusters, version);
        queryInfo->setClusters(clusters);
    }
}

void gatherQuerySetAliasDetails(IPropertyTree *alias, IEspQuerySetAlias *aliasInfo)
{
    aliasInfo->setName(alias->queryProp("@name"));
    aliasInfo->setId(alias->queryProp("@id"));
}

void retrieveAllQuerysetDetails(IEspContext &context, IPropertyTree *registry, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL, const char *type=NULL, const char *value=NULL)
{
    Owned<IPropertyTreeIterator> regQueries = registry->getElements("Query");
    ForEach(*regQueries)
    {
        IPropertyTree &query = regQueries->query();
        Owned<IEspQuerySetQuery> q = createQuerySetQuery();
        gatherQuerySetQueryDetails(context, &query, q, cluster, queriesOnCluster);

        if (isEmpty(cluster) || isEmpty(type) || isEmpty(value) || !strieq(type, "Status"))
            queries.append(*q.getClear());
        else
        {
            IArrayOf<IConstClusterQueryState>& cs = q->getClusters();
            ForEachItemIn(i, cs)
            {
                IConstClusterQueryState& c = cs.item(i);
                if (strieq(c.getCluster(), cluster) && (strieq(value, "All") || strieq(c.getState(), value)))
                {
                    queries.append(*q.getClear());
                    break;
                }
            }
        }
    }

    Owned<IPropertyTreeIterator> regAliases = registry->getElements("Alias");
    ForEach(*regAliases)
    {
        IPropertyTree &alias = regAliases->query();
        Owned<IEspQuerySetAlias> a = createQuerySetAlias();
        gatherQuerySetAliasDetails(&alias, a);
        aliases.append(*a.getClear());
    }
}

void retrieveQuerysetDetailsFromAlias(IEspContext &context, IPropertyTree *registry, const char *name, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster, IPropertyTree *queriesOnCluster)
{
    StringBuffer xpath;
    xpath.append("Alias[@name='").append(name).append("']");
    IPropertyTree *alias = registry->queryPropTree(xpath);
    if (!alias)
    {
        DBGLOG("Alias %s not found", name);
        return;
    }

    Owned<IEspQuerySetAlias> a = createQuerySetAlias();
    gatherQuerySetAliasDetails(alias, a);
    xpath.clear().append("Query[@id='").append(a->getId()).append("']");
    aliases.append(*a.getClear());

    IPropertyTree *query = registry->queryPropTree(xpath);
    if (!query)
    {
        DBGLOG("No matching Query %s found for Alias %s", a->getId(), name);
        return;
    }

    Owned<IEspQuerySetQuery> q = createQuerySetQuery();
    gatherQuerySetQueryDetails(context, query, q, cluster, queriesOnCluster);
    queries.append(*q.getClear());
}

void retrieveQuerysetDetailsFromQuery(IEspContext &context, IPropertyTree *registry, const char *value, const char *type, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (!strieq(type, "Id") && !strieq(type, "Name"))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Unrecognized queryset filter type %s", type);

    StringBuffer attributeName(type);
    StringBuffer xpath;
    xpath.clear().append("Query[@").append(attributeName.toLowerCase()).append("='").append(value).append("']");
    IPropertyTree *query = registry->queryPropTree(xpath);
    if (!query)
    {
        DBGLOG("No matching Query %s found for %s", value, type);
        return;
    }

    Owned<IEspQuerySetQuery> q = createQuerySetQuery();
    gatherQuerySetQueryDetails(context, query, q, cluster, queriesOnCluster);
    xpath.clear().append("Alias[@id='").append(q->getId()).append("']");
    queries.append(*q.getClear());

    Owned<IPropertyTreeIterator> regAliases = registry->getElements(xpath.str());
    ForEach(*regAliases)
    {
        IPropertyTree &alias = regAliases->query();
        Owned<IEspQuerySetAlias> a = createQuerySetAlias();
        gatherQuerySetAliasDetails(&alias, a);
        aliases.append(*a.getClear());
    }
}

void retrieveQuerysetDetails(IEspContext &context, IPropertyTree *registry, const char *type, const char *value, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (strieq(type, "All"))
        return retrieveAllQuerysetDetails(context, registry, queries, aliases, cluster, queriesOnCluster);
    if (!value || !*value)
        return;
    if (strieq(type, "Alias"))
        return retrieveQuerysetDetailsFromAlias(context, registry, value, queries, aliases, cluster, queriesOnCluster);
    if (strieq(type, "Status") && !isEmpty(cluster))
        return retrieveAllQuerysetDetails(context, registry, queries, aliases, cluster, queriesOnCluster, type, value);
    return retrieveQuerysetDetailsFromQuery(context, registry, value, type, queries, aliases, cluster, queriesOnCluster);
}

void retrieveQuerysetDetails(IEspContext &context, IArrayOf<IEspWUQuerySetDetail> &details, IPropertyTree *registry, const char *type, const char *value, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (!registry)
        return;

    IArrayOf<IEspQuerySetQuery> queries;
    IArrayOf<IEspQuerySetAlias> aliases;
    retrieveQuerysetDetails(context, registry, type, value, queries, aliases, cluster, queriesOnCluster);

    Owned<IEspWUQuerySetDetail> detail = createWUQuerySetDetail();
    detail->setQuerySetName(registry->queryProp("@id"));
    detail->setQueries(queries);
    detail->setAliases(aliases);
    details.append(*detail.getClear());
}

void retrieveQuerysetDetails(IEspContext &context, IArrayOf<IEspWUQuerySetDetail> &details, const char *queryset, const char *type, const char *value, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (!queryset || !*queryset)
        return;
    Owned<IPropertyTree> registry = getQueryRegistry(queryset, true);
    if (!registry)
        return;
    retrieveQuerysetDetails(context, details, registry, type, value, cluster, queriesOnCluster);
}

IPropertyTree* getQueriesOnCluster(const char *target, const char *queryset)
{
    if (isEmpty(target))
        target = queryset;
    Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target);
    if (!info)
        throw MakeStringException(ECLWATCH_CANNOT_RESOLVE_CLUSTER_NAME, "Cluster %s not found", target);
    if (queryset && *queryset && !strieq(target, queryset))
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_ON_CLUSTER, "Target %s and QuerySet %s should match", target, queryset);
    if (info->getPlatform()!=RoxieCluster)
        return NULL;
    const SocketEndpointArray &eps = info->getRoxieServers();
    if (!eps.length())
        return NULL;

    try
    {
        Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), ROXIECONNECTIONTIMEOUT);
        return sendRoxieControlAllNodes(sock, "<control:queries/>", false, ROXIECONTROLQUERIESTIMEOUT);
    }
    catch(IException* e)
    {
        StringBuffer err;
        DBGLOG("Get exception in control:queries: %s", e->errorMessage(err.append(e->errorCode()).append(' ')).str());
        e->Release();
        return NULL;
    }
}

void retrieveQuerysetDetailsByCluster(IEspContext &context, IArrayOf<IEspWUQuerySetDetail> &details, const char *target, const char *queryset, const char *type, const char *value)
{
    Owned<IPropertyTree> queriesOnCluster = getQueriesOnCluster(target, queryset);
    retrieveQuerysetDetails(context, details, target, type, value, target, queriesOnCluster);
}

void retrieveAllQuerysetDetails(IEspContext &context, IArrayOf<IEspWUQuerySetDetail> &details, const char *type, const char *value)
{
    Owned<IPropertyTree> root = getQueryRegistryRoot();
    if (!root)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet Registry not found");
    Owned<IPropertyTreeIterator> querysets = root->getElements("QuerySet");
    ForEach(*querysets)
        retrieveQuerysetDetails(context, details, &querysets->query(), type, value);
}

bool CWsWorkunitsEx::onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());

    double version = context.getClientVersion();
    if (version > 1.36)
    {
        Owned<IPropertyTree> queryRegistry = getQueryRegistry(req.getQuerySetName(), false);
        resp.setClusterName(req.getClusterName());
        resp.setFilter(req.getFilter());
        resp.setFilterType(req.getFilterType());
    }

    Owned<IPropertyTree> registry = getQueryRegistry(req.getQuerySetName(), true);
    if (!registry)
        return false;

    IArrayOf<IEspQuerySetQuery> respQueries;
    IArrayOf<IEspQuerySetAlias> respAliases;

    if (isEmpty(req.getClusterName()) || isEmpty(req.getFilterTypeAsString()) || !strieq(req.getFilterTypeAsString(), "Status") || isEmpty(req.getFilter()))
    {
        const char* cluster = req.getClusterName();
        if (isEmpty(cluster))
            cluster = req.getQuerySetName();
        Owned<IPropertyTree> queriesOnCluster = getQueriesOnCluster(cluster, req.getQuerySetName());
        retrieveQuerysetDetails(context, registry, req.getFilterTypeAsString(), req.getFilter(), respQueries, respAliases, cluster, queriesOnCluster);

        resp.setQuerysetQueries(respQueries);
        resp.setQuerysetAliases(respAliases);
    }
    else
    {
        IArrayOf<IEspWUQuerySetDetail> respDetails;
        retrieveQuerysetDetailsByCluster(context, respDetails, req.getClusterName(), req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
        if (respDetails.ordinality())
        {
            IEspWUQuerySetDetail& detail = respDetails.item(0);
            resp.setQuerysetQueries(detail.getQueries());
            resp.setQuerysetAliases(detail.getAliases());
        }
    }

    return true;
}

bool CWsWorkunitsEx::onWUMultiQuerysetDetails(IEspContext &context, IEspWUMultiQuerySetDetailsRequest & req, IEspWUMultiQuerySetDetailsResponse & resp)
{
    IArrayOf<IEspWUQuerySetDetail> respDetails;

    if (notEmpty(req.getClusterName()))
        retrieveQuerysetDetailsByCluster(context, respDetails, req.getClusterName(), req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
    else if (notEmpty(req.getQuerySetName()))
        retrieveQuerysetDetails(context, respDetails, req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
    else
        retrieveAllQuerysetDetails(context, respDetails, req.getFilterTypeAsString(), req.getFilter());

    resp.setQuerysets(respDetails);

    return true;
}

bool addWUQSQueryFilter(WUQuerySortField *filters, unsigned short &count, MemoryBuffer &buff, const char* value, WUQuerySortField name)
{
    if (isEmpty(value))
        return false;
    filters[count++] = name;
    buff.append(value);
    return true;
}

bool addWUQSQueryFilterInt(WUQuerySortField *filters, unsigned short &count, MemoryBuffer &buff, int value, WUQuerySortField name)
{
    VStringBuffer vBuf("%d", value);
    filters[count++] = name;
    buff.append(vBuf.str());
    return true;
}

bool addWUQSQueryFilterInt64(WUQuerySortField *filters, unsigned short &count, MemoryBuffer &buff, __int64 value, WUQuerySortField name)
{
    VStringBuffer vBuf("%" I64F "d", value);
    filters[count++] = name;
    buff.append(vBuf.str());
    return true;
}

unsigned CWsWorkunitsEx::getGraphIdsByQueryId(const char *target, const char *queryId, StringArray& graphIds)
{
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Target name required");
    if (!queryId || !*queryId)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query Id required");

    Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target);
    if (!info || (info->getPlatform()!=RoxieCluster)) //Only roxie query has query graph.
        return 0;

    const SocketEndpointArray &eps = info->getRoxieServers();
    if (eps.empty())
        return 0;

    VStringBuffer xpath("<control:querystats><Query id='%s'/></control:querystats>", queryId);
    Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), ROXIECONNECTIONTIMEOUT);
    Owned<IPropertyTree> querystats = sendRoxieControlQuery(sock, xpath.str(), ROXIECONTROLQUERYTIMEOUT);
    if (!querystats)
        return 0;

    Owned<IPropertyTreeIterator> graphs = querystats->getElements("Endpoint/Query/Graph");
    ForEach(*graphs)
    {
        IPropertyTree &graph = graphs->query();
        const char* graphId = graph.queryProp("@id");
        if (graphId && *graphId)
            graphIds.append(graphId);
    }
    return graphIds.length();
}

//This method is thread safe because a query belongs to a single queryset. The method may be called by different threads.
//Since one thread is for one queryset and a query only belongs to a single queryset, it is impossible for different threads
//to update the same query object.
void CWsWorkunitsEx::checkAndSetClusterQueryState(IEspContext &context, const char* cluster, const char* querySetId, IArrayOf<IEspQuerySetQuery>& queries)
{
    try
    {
        double version = context.getClientVersion();
        if (isEmpty(cluster))
            cluster = querySetId;
        Owned<IPropertyTree> queriesOnCluster = getQueriesOnCluster(cluster, querySetId);
        if (!queriesOnCluster)
        {
            DBGLOG("getQueriesOnCluster() returns NULL for cluster<%s> and querySetId<%s>", cluster, querySetId);
            return;
        }

        ForEachItemIn(i, queries)
        {
            IEspQuerySetQuery& query = queries.item(i);
            const char* queryId = query.getId();
            const char* querySetId0 = query.getQuerySetId();
            if (!queryId || !querySetId0 || !strieq(querySetId0, querySetId))
                continue;

            IArrayOf<IEspClusterQueryState> clusters;
            addClusterQueryStates(queriesOnCluster, cluster, queryId, clusters, version);
            query.setClusters(clusters);
        }
    }
    catch(IException *e)
    {
        EXCLOG(e, "CWsWorkunitsEx::checkAndSetClusterQueryState: Failed to read Query State On Cluster");
        e->Release();
    }
}

void CWsWorkunitsEx::checkAndSetClusterQueryState(IEspContext &context, const char* cluster, StringArray& querySetIds, IArrayOf<IEspQuerySetQuery>& queries)
{
    UnsignedArray threadHandles;
    ForEachItemIn(i, querySetIds)
    {
        const char* querySetId = querySetIds.item(i);
        if(!querySetId || !*querySetId)
            continue;

        Owned<CClusterQueryStateParam> threadReq = new CClusterQueryStateParam(this, context, cluster, querySetId, queries);
        PooledThreadHandle handle = clusterQueryStatePool->start( threadReq.getClear() );
        threadHandles.append(handle);
    }

    //block for worker threads to finish, if necessary and then collect results
    //Not use joinAll() because multiple threads may call this method. Each call uses the pool to create
    //its own threads of checking query state. Each call should only join the ones created by that call.
    ForEachItemIn(ii, threadHandles)
        clusterQueryStatePool->join(threadHandles.item(ii));
}

bool CWsWorkunitsEx::onWUListQueries(IEspContext &context, IEspWUListQueriesRequest & req, IEspWUListQueriesResponse & resp)
{
    bool descending = req.getDescending();
    const char *sortBy =  req.getSortby();
    WUQuerySortField sortOrder[2] = {WUQSFId, WUQSFterm};
    if(notEmpty(sortBy))
    {
        if (strieq(sortBy, "Name"))
            sortOrder[0] = WUQSFname;
        else if (strieq(sortBy, "WUID"))
            sortOrder[0] = WUQSFwuid;
        else if (strieq(sortBy, "DLL"))
            sortOrder[0] = WUQSFdll;
        else if (strieq(sortBy, "Activated"))
            sortOrder[0] = WUQSFActivited;
        else if (strieq(sortBy, "MemoryLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFmemoryLimit | WUQSFnumeric);
        else if (strieq(sortBy, "TimeLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFtimeLimit | WUQSFnumeric);
        else if (strieq(sortBy, "WarnTimeLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFwarnTimeLimit | WUQSFnumeric);
        else if (strieq(sortBy, "Priority"))
            sortOrder[0] = (WUQuerySortField) (WUQSFpriority | WUQSFnumeric);
        else if (strieq(sortBy, "PublishedBy"))
            sortOrder[0] = WUQSFPublishedBy;
        else if (strieq(sortBy, "QuerySetId"))
            sortOrder[0] = WUQSFQuerySet;
        else
            sortOrder[0] = WUQSFId;

        sortOrder[0] = (WUQuerySortField) (sortOrder[0] | WUQSFnocase);
        if (descending)
            sortOrder[0] = (WUQuerySortField) (sortOrder[0] | WUQSFreverse);
    }

    WUQuerySortField filters[16];
    unsigned short filterCount = 0;
    MemoryBuffer filterBuf;
    const char* clusterReq = req.getClusterName();
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getQuerySetName(), WUQSFQuerySet);
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getQueryID(), (WUQuerySortField) (WUQSFId | WUQSFwild));
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getQueryName(), (WUQuerySortField) (WUQSFname | WUQSFwild));
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getWUID(), WUQSFwuid);
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getLibraryName(), (WUQuerySortField) (WUQSFLibrary | WUQSFnocase));
    addWUQSQueryFilter(filters, filterCount, filterBuf, req.getPublishedBy(), (WUQuerySortField) (WUQSFPublishedBy | WUQSFwild));
    if (!req.getMemoryLimitLow_isNull())
        addWUQSQueryFilterInt64(filters, filterCount, filterBuf, req.getMemoryLimitLow(), (WUQuerySortField) (WUQSFmemoryLimit | WUQSFnumeric));
    if (!req.getMemoryLimitHigh_isNull())
        addWUQSQueryFilterInt64(filters, filterCount, filterBuf, req.getMemoryLimitHigh(), (WUQuerySortField) (WUQSFmemoryLimitHi | WUQSFnumeric));
    if (!req.getTimeLimitLow_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getTimeLimitLow(), (WUQuerySortField) (WUQSFtimeLimit | WUQSFnumeric));
    if (!req.getTimeLimitHigh_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getTimeLimitHigh(), (WUQuerySortField) (WUQSFtimeLimitHi | WUQSFnumeric));
    if (!req.getWarnTimeLimitLow_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getWarnTimeLimitLow(), (WUQuerySortField) (WUQSFwarnTimeLimit | WUQSFnumeric));
    if (!req.getWarnTimeLimitHigh_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getWarnTimeLimitHigh(), (WUQuerySortField) (WUQSFwarnTimeLimitHi | WUQSFnumeric));
    if (!req.getPriorityLow_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getPriorityLow(), (WUQuerySortField) (WUQSFpriority | WUQSFnumeric));
    if (!req.getPriorityHigh_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getPriorityHigh(), (WUQuerySortField) (WUQSFpriorityHi | WUQSFnumeric));
    if (!req.getActivated_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getActivated(), (WUQuerySortField) (WUQSFActivited | WUQSFnumeric));
    if (!req.getSuspendedByUser_isNull())
        addWUQSQueryFilterInt(filters, filterCount, filterBuf, req.getSuspendedByUser(), (WUQuerySortField) (WUQSFSuspendedByUser | WUQSFnumeric));
    filters[filterCount] = WUQSFterm;

    unsigned numberOfQueries = 0;
    unsigned pageSize = req.getPageSize();
    unsigned pageStartFrom = req.getPageStartFrom();
    if(pageSize < 1)
        pageSize = 100;
    __int64 cacheHint = 0;
    if (!req.getCacheHint_isNull())
        cacheHint = req.getCacheHint();

    Owned<MapStringTo<bool> > queriesUsingFileMap;
    const char *lfn = req.getFileName();
    if (lfn && *lfn)
    {
        queriesUsingFileMap.setown(new MapStringTo<bool>());
        StringAttr dummy;
        Owned<IPropertyTreeIterator> queriesUsingFile = filesInUse.findQueriesUsingFile(clusterReq, lfn, dummy);
        ForEach (*queriesUsingFile)
        {
            IPropertyTree &queryUsingFile = queriesUsingFile->query();
            const char *queryTarget = queryUsingFile.queryProp("@target");
            const char *queryId = queryUsingFile.queryProp("@id");
            if (queryTarget && *queryTarget && queryId && *queryId)
            {
                VStringBuffer targetQuery("%s/%s", queryTarget, queryId);
                queriesUsingFileMap->setValue(targetQuery, true);
            }
        }
    }

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstQuerySetQueryIterator> it = factory->getQuerySetQueriesSorted(sortOrder, filters, filterBuf.bufferBase(), pageStartFrom, pageSize, &cacheHint, &numberOfQueries, queriesUsingFileMap);
    resp.setCacheHint(cacheHint);

    StringArray querySetIds;
    IArrayOf<IEspQuerySetQuery> queries;
    double version = context.getClientVersion();
    ForEach(*it)
    {
        IPropertyTree &query=it->query();
        const char *queryId = query.queryProp("@id");
        const char *queryTarget = query.queryProp("@querySetId");

        Owned<IEspQuerySetQuery> q = createQuerySetQuery();
        q->setId(queryId);
        q->setQuerySetId(queryTarget);
        q->setName(query.queryProp("@name"));
        q->setDll(query.queryProp("@dll"));
        q->setWuid(query.queryProp("@wuid"));
        q->setActivated(query.getPropBool("@activated", false));
        q->setSuspended(query.getPropBool("@suspended", false));
        if (query.hasProp("@memoryLimit"))
        {
            StringBuffer s;
            memoryLimitStringFromUInt64(s, query.getPropInt64("@memoryLimit"));
            q->setMemoryLimit(s);
        }
        if (query.hasProp("@timeLimit"))
            q->setTimeLimit(query.getPropInt("@timeLimit"));
        if (query.hasProp("@warnTimeLimit"))
            q->setWarnTimeLimit(query.getPropInt("@warnTimeLimit"));
        if (query.hasProp("@priority"))
            q->setPriority(getQueryPriorityName(query.getPropInt("@priority")));
        if (query.hasProp("@comment"))
            q->setComment(query.queryProp("@comment"));
        if (version >= 1.46)
        {
            q->setPublishedBy(query.queryProp("@publishedBy"));
            q->setIsLibrary(query.getPropBool("@isLibrary"));
        }

        if (!querySetIds.contains(queryTarget))
            querySetIds.append(queryTarget);
        queries.append(*q.getClear());
    }

    checkAndSetClusterQueryState(context, clusterReq, querySetIds, queries);

    resp.setQuerysetQueries(queries);
    resp.setNumberOfQueries(numberOfQueries);

    return true;
}

bool CWsWorkunitsEx::onWUListQueriesUsingFile(IEspContext &context, IEspWUListQueriesUsingFileRequest &req, IEspWUListQueriesUsingFileResponse &resp)
{
    const char *target = req.getTarget();
    const char *process = req.getProcess();

    StringBuffer lfn(req.getFileName());
    resp.setFileName(lfn.toLowerCase());
    resp.setProcess(process);

    StringArray targets;
    if (target && *target)
        targets.append(target);
    else // if (process && *process)
    {
        SCMStringBuffer targetStr;
        Owned<IStringIterator> targetClusters = getTargetClusters("RoxieCluster", process);
        ForEach(*targetClusters)
            targets.append(targetClusters->str(targetStr).str());
    }

    IArrayOf<IEspTargetQueriesUsingFile> respTargets;
    ForEachItemIn(i, targets)
    {
        target = targets.item(i);
        Owned<IEspTargetQueriesUsingFile> respTarget = createTargetQueriesUsingFile();
        respTarget->setTarget(target);

        StringAttr pmid;
        Owned<IPropertyTreeIterator> queries = filesInUse.findQueriesUsingFile(target, lfn, pmid);
        if (!pmid.isEmpty())
            respTarget->setPackageMap(pmid);
        if (queries)
        {
            IArrayOf<IEspQueryUsingFile> respQueries;
            ForEach(*queries)
            {
                IPropertyTree &query = queries->query();
                Owned<IEspQueryUsingFile> q = createQueryUsingFile();
                q->setId(query.queryProp("@id"));

                VStringBuffer xpath("File[@lfn='%s']/@pkgid", lfn.str());
                if (query.hasProp(xpath))
                    q->setPackage(query.queryProp(xpath));
                respQueries.append(*q.getClear());
            }
            respTarget->setQueries(respQueries);
        }
        respTargets.append(*respTarget.getClear());
    }
    resp.setTargets(respTargets);

    return true;
}

bool CWsWorkunitsEx::onWUQueryFiles(IEspContext &context, IEspWUQueryFilesRequest &req, IEspWUQueryFilesResponse &resp)
{
    const char *target = req.getTarget();
    const char *query = req.getQueryId();
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Target not specified");
    if (!isValidCluster(target))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid target name: %s", target);
    if (!query || !*query)
        throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query not specified");
    Owned<IPropertyTree> registeredQuery = resolveQueryAlias(target, query, true);
    if (!registeredQuery)
        throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query not found");
    StringAttr queryid(registeredQuery->queryProp("@id"));
    registeredQuery.clear();

    Owned<IPropertyTree> tree = filesInUse.getTree();
    VStringBuffer xpath("%s/Query[@id='%s']", target, queryid.get());
    IPropertyTree *queryTree = tree->queryPropTree(xpath);
    if (!queryTree)
       throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query not found in file cache (%s)", xpath.str());

    IArrayOf<IEspFileUsedByQuery> referencedFiles;
    Owned<IPropertyTreeIterator> files = queryTree->getElements("File");
    ForEach(*files)
    {
        IPropertyTree &file = files->query();
        if (file.getPropBool("@super", 0))
            continue;
        Owned<IEspFileUsedByQuery> respFile = createFileUsedByQuery();
        respFile->setFileName(file.queryProp("@lfn"));
        respFile->setFileSize(file.getPropInt64("@size"));
        respFile->setNumberOfParts(file.getPropInt("@numparts"));
        referencedFiles.append(*respFile.getClear());
    }
    resp.setFiles(referencedFiles);
    return true;
}

bool CWsWorkunitsEx::onWUQueryDetails(IEspContext &context, IEspWUQueryDetailsRequest & req, IEspWUQueryDetailsResponse & resp)
{
    const char* querySet = req.getQuerySet();
    const char* queryId = req.getQueryId();
    bool  includeStateOnClusters = req.getIncludeStateOnClusters();
    if (!querySet || !*querySet)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet not specified");
    if (!queryId || !*queryId)
        throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "QueryId not specified");
    resp.setQueryId(queryId);
    resp.setQuerySet(querySet);

    Owned<IPropertyTree> queryRegistry = getQueryRegistry(querySet, false);

    StringBuffer xpath;
    xpath.clear().append("Query[@id='").append(queryId).append("']");
    IPropertyTree *query = queryRegistry->queryPropTree(xpath.str());
    if (!query)
    {
        DBGLOG("No matching Query");
        return false;
    }

    const char* queryName = query->queryProp("@name");
    const char* wuid = query->queryProp("@wuid");
    resp.setQueryName(queryName);
    resp.setWuid(wuid);
    resp.setDll(query->queryProp("@dll"));
    resp.setPublishedBy(query->queryProp("@publishedBy"));
    resp.setSuspended(query->getPropBool("@suspended", false));
    resp.setSuspendedBy(query->queryProp("@suspendedBy"));
    resp.setComment(query->queryProp("@comment"));
    double version = context.getClientVersion();
    if (version >= 1.46)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

        if (query->hasProp("@priority"))
            resp.setPriority(getQueryPriorityName(query->getPropInt("@priority")));
        resp.setIsLibrary(query->getPropBool("@isLibrary"));
        SCMStringBuffer s;
        resp.setWUSnapShot(cw->getSnapshot(s).str()); //Label
        Owned<IConstWUStatistic> whenCompiled = cw->getStatistic(NULL, NULL, StWhenCompiled);
        if (whenCompiled)
        {
            whenCompiled->getFormattedValue(s);
            resp.setCompileTime(s.str());
        }

        StringArray libUsed, graphIds;
        Owned<IConstWULibraryIterator> libs = &cw->getLibraries();
        ForEach(*libs)
            libUsed.append(libs->query().getName(s).str());
        if (libUsed.length())
            resp.setLibrariesUsed(libUsed);
        unsigned numGraphIds = getGraphIdsByQueryId(querySet, queryId, graphIds);
        resp.setCountGraphs(numGraphIds);
        if (numGraphIds > 0)
            resp.setGraphIds(graphIds);
    }

    StringArray logicalFiles;
    IArrayOf<IEspQuerySuperFile> superFiles;
    getQueryFiles(queryId, querySet, logicalFiles, req.getIncludeSuperFiles() ? &superFiles : NULL);
    if (logicalFiles.length())
        resp.setLogicalFiles(logicalFiles);
    if (superFiles.length())
        resp.setSuperFiles(superFiles);

    if (version >= 1.42)
    {
        xpath.clear().appendf("Alias[@id='%s']", queryId);
        IPropertyTree *alias = queryRegistry->queryPropTree(xpath.str());
        if (!alias)
            resp.setActivated(false);
        else
            resp.setActivated(true);
    }
    if (includeStateOnClusters && (version >= 1.43))
    {
        Owned<IPropertyTree> queriesOnCluster = getQueriesOnCluster(querySet, querySet);
        if (queriesOnCluster)
        {
            IArrayOf<IEspClusterQueryState> clusterStates;
            addClusterQueryStates(queriesOnCluster, querySet, queryId, clusterStates, version);
            resp.setClusters(clusterStates);
        }
    }
    if (version >= 1.50)
    {
        WsWuInfo winfo(context, wuid);
        resp.setResourceURLCount(winfo.getResourceURLCount());
    }
    if (req.getIncludeWsEclAddresses())
    {
        StringArray wseclAddresses;
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> env = factory->openEnvironment();
        if (env)
        {
            Owned<IPropertyTree> root = &env->getPTree();
            Owned<IPropertyTreeIterator> services = root->getElements("Software/EspService[Properties/@type='ws_ecl']");
            StringArray serviceNames;
            VStringBuffer xpath("Target[@name='%s']", querySet);
            ForEach(*services)
            {
                IPropertyTree &service = services->query();
                if (!service.hasProp("Target") || service.hasProp(xpath))
                    serviceNames.append(service.queryProp("@name"));
            }

            Owned<IPropertyTreeIterator> processes = root->getElements("Software/EspProcess");
            ForEach(*processes)
            {
                StringArray netAddrs;
                IPropertyTree &process = processes->query();
                Owned<IPropertyTreeIterator> instances = process.getElements("Instance");
                ForEach(*instances)
                {
                    IPropertyTree &instance = instances->query();
                    const char *netAddr = instance.queryProp("@netAddress");
                    if (!netAddr || !*netAddr)
                        continue;
                    if (streq(netAddr, "."))
                        netAddrs.appendUniq(envLocalAddress); //not necessarily local to this server
                    else
                        netAddrs.appendUniq(netAddr);
                }
                Owned<IPropertyTreeIterator> bindings = process.getElements("EspBinding");
                ForEach(*bindings)
                {
                    IPropertyTree &binding = bindings->query();
                    const char *srvName = binding.queryProp("@service");
                    if (!serviceNames.contains(srvName))
                        continue;
                    const char *port = binding.queryProp("@port"); //should always be an integer, but we're just concatenating strings
                    if (!port || !*port)
                        continue;
                    ForEachItemIn(i, netAddrs)
                    {
                        VStringBuffer wseclAddr("%s:%s", netAddrs.item(i), port);
                        wseclAddresses.append(wseclAddr);
                    }
                }
            }
        }
        resp.setWsEclAddresses(wseclAddresses);
    }

    return true;
}

int EspQuerySuperFileCompareFunc(IInterface * const *i1, IInterface * const *i2)
{
    if (!i1 || !*i1 || !i2 || !*i2)
        return 0;
    IEspQuerySuperFile *sf1 = QUERYINTERFACE(*i1, IEspQuerySuperFile);
    IEspQuerySuperFile *sf2 = QUERYINTERFACE(*i2, IEspQuerySuperFile);
    if (!sf1 || !sf2)
        return 0;
    const char *name1 = sf1->getName();
    const char *name2 = sf2->getName();
    if (!name1 || !name2)
        return 0;
    return strcmp(name1, name2);
}

bool CWsWorkunitsEx::getQueryFiles(const char* query, const char* target, StringArray& logicalFiles, IArrayOf<IEspQuerySuperFile> *respSuperFiles)
{
    try
    {
        Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target);
        if (!info || (info->getPlatform()!=RoxieCluster))
            return false;

        const SocketEndpointArray &eps = info->getRoxieServers();
        if (eps.empty())
            return false;

        StringBuffer control;
        control.appendf("<control:getQueryXrefInfo full='1'><Query id='%s'/></control:getQueryXrefInfo>",  query);
        Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), ROXIECONNECTIONTIMEOUT);
        Owned<IPropertyTree> result = sendRoxieControlQuery(sock, control.str(), ROXIECONTROLQUERYTIMEOUT);
        if (!result)
            return false;

        StringBuffer xpath("Endpoint/Queries/Query/");
        if (!respSuperFiles)
            xpath.append('/');
        xpath.append("File");
        Owned<IPropertyTreeIterator> files = result->getElements(xpath);
        ForEach (*files)
        {
            IPropertyTree &file = files->query();
            const char* fileName = file.queryProp("@name");
            if (fileName && *fileName)
                logicalFiles.append(fileName);
        }
        logicalFiles.sortAscii();

        if (respSuperFiles)
        {
            Owned<IPropertyTreeIterator> superFiles = result->getElements("Endpoint/Queries/Query/SuperFile");
            ForEach (*superFiles)
            {
                IPropertyTree &super = superFiles->query();
                Owned<IEspQuerySuperFile> respSuperFile = createQuerySuperFile();
                respSuperFile->setName(super.queryProp("@name"));
                Owned<IPropertyTreeIterator> fileIter = super.getElements("File");
                StringArray respSubFiles;
                ForEach (*fileIter)
                {
                    IPropertyTree &fileItem = fileIter->query();
                    const char* fileName = fileItem.queryProp("@name");
                    if (fileName && *fileName)
                        respSubFiles.append(fileName);
                }
                respSubFiles.sortAscii();

                respSuperFile->setSubFiles(respSubFiles);
                respSuperFiles->append(*respSuperFile.getClear());
            }
            respSuperFiles->sort(EspQuerySuperFileCompareFunc);
        }
        return true;
    }
    catch(IMultiException *me)
    {
        StringBuffer err;
        DBGLOG("ERROR control:getQueryXrefInfo roxie query info %s", me->errorMessage(err.append(me->errorCode()).append(' ')).str());
        me->Release();
        return false;
    }
    catch(IException *e)
    {
        StringBuffer err;
        DBGLOG("ERROR control:getQueryXrefInfo roxie query info %s", e->errorMessage(err.append(e->errorCode()).append(' ')).str());
        e->Release();
        return false;
    }
}

inline void verifyQueryActionAllowsWild(bool &allowWildChecked, CQuerySetQueryActionTypes action)
{
    if (allowWildChecked)
        return;
    switch (action)
    {
        case CQuerySetQueryActionTypes_ToggleSuspend:
            throw MakeStringException(ECLWATCH_INVALID_ACTION, "Wildcards not supported for toggling suspended state");
        case CQuerySetQueryActionTypes_Activate:
            throw MakeStringException(ECLWATCH_INVALID_ACTION, "Wildcards not supported for Activating queries");
    }
    allowWildChecked=true;
}

inline bool verifyQueryActionAllowsAlias(CQuerySetQueryActionTypes action)
{
    return (action!=CQuerySetQueryActionTypes_Activate && action!=CQuerySetQueryActionTypes_RemoveAllAliases);
}
void expandQueryActionTargetList(IProperties *queryIds, IPropertyTree *queryset, IArrayOf<IConstQuerySetQueryActionItem> &items, CQuerySetQueryActionTypes action)
{
    bool allowWildChecked=false;
    ForEachItemIn(i, items)
    {
        const char *itemId = items.item(i).getQueryId();
        if (!isWildString(itemId))
        {
            bool suspendedByUser = false;
            const char *itemSuspendState = items.item(i).getClientState().getSuspended();
            if (itemSuspendState && (strieq(itemSuspendState, "By User") || strieq(itemSuspendState, "1")))
                suspendedByUser = true;
            if (!verifyQueryActionAllowsAlias(action))
                queryIds->setProp(itemId, suspendedByUser);
            else
            {
                Owned<IPropertyTree> query = resolveQueryAlias(queryset, itemId);
                if (query)
                {
                    const char *id = query->queryProp("@id");
                    if (id && *id)
                        queryIds->setProp(id, suspendedByUser);
                }
            }
        }
        else
        {
            verifyQueryActionAllowsWild(allowWildChecked, action);
            if (verifyQueryActionAllowsAlias(action))
            {
                Owned<IPropertyTreeIterator> active = queryset->getElements("Alias");
                ForEach(*active)
                {
                    const char *name = active->query().queryProp("@name");
                    const char *id = active->query().queryProp("@id");
                    if (name && id && WildMatch(name, itemId))
                        queryIds->setProp(id, 0);
                }
            }
            Owned<IPropertyTreeIterator> queries = queryset->getElements("Query");
            ForEach(*queries)
            {
                const char *id = queries->query().queryProp("@id");
                if (id && WildMatch(id, itemId))
                    queryIds->setProp(id, 0);
            }
        }
    }
}

void expandQueryActionTargetList(IProperties *queryIds, IPropertyTree *queryset, const char *id, CQuerySetQueryActionTypes action)
{
    IArrayOf<IConstQuerySetQueryActionItem> items;
    Owned<IEspQuerySetQueryActionItem> item = createQuerySetQueryActionItem();
    item->setQueryId(id);
    items.append(*(IConstQuerySetQueryActionItem*)item.getClear());
    expandQueryActionTargetList(queryIds, queryset, items, action);
}

bool CWsWorkunitsEx::onWUQueryConfig(IEspContext &context, IEspWUQueryConfigRequest & req, IEspWUQueryConfigResponse & resp)
{
    StringAttr target(req.getTarget());
    if (target.isEmpty())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Target name required");
    if (!isValidCluster(target))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid target name: %s", target.get());

    Owned<IPropertyTree> queryset = getQueryRegistry(target.get(), false);
    if (!queryset)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Target Queryset %s not found", req.getTarget());

    Owned<IProperties> queryIds = createProperties();
    expandQueryActionTargetList(queryIds, queryset, req.getQueryId(), QuerySetQueryActionTypes_Undefined);

    IArrayOf<IEspWUQueryConfigResult> results;
    Owned<IPropertyIterator> it = queryIds->getIterator();
    ForEach(*it)
    {
        Owned<IEspWUQueryConfigResult> result = createWUQueryConfigResult();
        result->setQueryId(it->getPropKey());

        VStringBuffer xpath("Query[@id='%s']", it->getPropKey());
        IPropertyTree *queryTree = queryset->queryPropTree(xpath);
        if (queryTree)
        {
            updateMemoryLimitSetting(queryTree, req.getMemoryLimit());
            updateQueryPriority(queryTree, req.getPriority());
            updateQuerySetting(req.getTimeLimit_isNull(), queryTree, "@timeLimit", req.getTimeLimit());
            updateQuerySetting(req.getWarnTimeLimit_isNull(), queryTree, "@warnTimeLimit", req.getWarnTimeLimit());
            if (req.getComment())
                queryTree->setProp("@comment", req.getComment());
        }

        results.append(*result.getClear());
    }
    resp.setResults(results);

    bool reloadFailed = false;
    if (0!=req.getWait() && !req.getNoReload())
        reloadFailed = !reloadCluster(target.get(), (unsigned)req.getWait());
    resp.setReloadFailed(reloadFailed);

    return true;
}

bool CWsWorkunitsEx::onWUQuerysetQueryAction(IEspContext &context, IEspWUQuerySetQueryActionRequest & req, IEspWUQuerySetQueryActionResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());
    resp.setAction(req.getAction());

    if (isEmpty(req.getQuerySetName()))
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Queryset name required");
    Owned<IPropertyTree> queryset = getQueryRegistry(req.getQuerySetName(), true);
    if (!queryset)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Queryset %s not found", req.getQuerySetName());

    Owned<IProperties> queryIds = createProperties();
    expandQueryActionTargetList(queryIds, queryset, req.getQueries(), req.getAction());
    if (req.getAction() == CQuerySetQueryActionTypes_ResetQueryStats)
        return resetQueryStats(context, req.getQuerySetName(), queryIds, resp);

    IArrayOf<IEspQuerySetQueryActionResult> results;
    Owned<IPropertyIterator> it = queryIds->getIterator();
    ForEach(*it)
    {
        const char *id = it->getPropKey();
        Owned<IEspQuerySetQueryActionResult> result = createQuerySetQueryActionResult();
        result->setQueryId(id);
        try
        {
            Owned<IPropertyTree> query = getQueryById(queryset, id);
            if (!query)
                throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query %s/%s not found.", req.getQuerySetName(), id);
            switch (req.getAction())
            {
                case CQuerySetQueryActionTypes_ToggleSuspend:
                    setQuerySuspendedState(queryset, id, !queryIds->getPropBool(id), context.queryUserId());
                    break;
                case CQuerySetQueryActionTypes_Suspend:
                    setQuerySuspendedState(queryset, id, true, context.queryUserId());
                    break;
                case CQuerySetQueryActionTypes_Unsuspend:
                    setQuerySuspendedState(queryset, id, false, NULL);
                    break;
                case CQuerySetQueryActionTypes_Activate:
                    setQueryAlias(queryset, query->queryProp("@name"), id);
                    break;
                case CQuerySetQueryActionTypes_Delete:
                    removeNamedQuery(queryset, id);
                    query.clear();
                    break;
                case CQuerySetQueryActionTypes_RemoveAllAliases:
                    removeAliasesFromNamedQuery(queryset, id);
                    break;
            }
            result->setSuccess(true);
            if (query)
                result->setSuspended(query->getPropBool("@suspended"));
        }
        catch(IException *e)
        {
            StringBuffer msg;
            result->setMessage(e->errorMessage(msg).str());
            result->setCode(e->errorCode());
            result->setSuccess(false);
            e->Release();
        }
        results.append(*result.getClear());
    }
    resp.setResults(results);
    return true;
}

bool CWsWorkunitsEx::onWUQuerysetAliasAction(IEspContext &context, IEspWUQuerySetAliasActionRequest &req, IEspWUQuerySetAliasActionResponse &resp)
{
    resp.setQuerySetName(req.getQuerySetName());
    resp.setAction(req.getAction());

    if (isEmpty(req.getQuerySetName()))
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Queryset name required");
    Owned<IPropertyTree> queryset = getQueryRegistry(req.getQuerySetName(), true);
    if (!queryset)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Queryset %s not found", req.getQuerySetName());

    IArrayOf<IEspQuerySetAliasActionResult> results;
    ForEachItemIn(i, req.getAliases())
    {
        IConstQuerySetAliasActionItem& item=req.getAliases().item(i);
        Owned<IEspQuerySetAliasActionResult> result = createQuerySetAliasActionResult();
        try
        {
            VStringBuffer xpath("Alias[@name='%s']", item.getName());
            IPropertyTree *alias = queryset->queryPropTree(xpath.str());
            if (!alias)
                throw MakeStringException(ECLWATCH_ALIAS_NOT_FOUND, "Alias %s/%s not found.", req.getQuerySetName(), item.getName());
            switch (req.getAction())
            {
                case CQuerySetAliasActionTypes_Deactivate:
                    removeQuerySetAlias(req.getQuerySetName(), item.getName());
                    break;
            }
            result->setSuccess(true);
        }
        catch(IException *e)
        {
            StringBuffer msg;
            result->setMessage(e->errorMessage(msg).str());
            result->setCode(e->errorCode());
            result->setSuccess(false);
            e->Release();
        }
        results.append(*result.getClear());
    }
    resp.setResults(results);
    return true;
}

#define QUERYPATH_SEP_CHAR '/'

bool nextQueryPathNode(const char *&path, StringBuffer &node)
{
    if (*path==QUERYPATH_SEP_CHAR)
        path++;
    while (*path && *path!=QUERYPATH_SEP_CHAR)
        node.append(*path++);
    return (*path && *++path);
}

bool splitQueryPath(const char *path, StringBuffer &netAddress, StringBuffer &queryset, StringBuffer *query)
{
    if (!path || !*path)
        return false;
    if (*path==QUERYPATH_SEP_CHAR && path[1]==QUERYPATH_SEP_CHAR)
    {
        path+=2;
        if (!nextQueryPathNode(path, netAddress))
            return false;
    }
    if (!nextQueryPathNode(path, queryset))
        return (query==NULL);
    if (!query)
        return false;
    if (nextQueryPathNode(path, *query))
        return false; //query path too deep
    return true;
}

IPropertyTree *fetchRemoteQuerySetInfo(IEspContext *context, const char *srcAddress, const char *srcTarget)
{
    if (!srcAddress || !*srcAddress || !srcTarget || !*srcTarget)
        return NULL;

    VStringBuffer url("http://%s%s/WsWorkunits/WUQuerysetDetails.xml?ver_=1.51&QuerySetName=%s&FilterType=All", srcAddress, (!strchr(srcAddress, ':')) ? ":8010" : "", srcTarget);

    Owned<IHttpClientContext> httpCtx = getHttpClientContext();
    Owned<IHttpClient> httpclient = httpCtx->createHttpClient(NULL, url);

    const char *user = context->queryUserId();
    if (user && *user)
        httpclient->setUserID(user);

    const char *pw = context->queryPassword();
    if (pw && *pw)
         httpclient->setPassword(pw);

    StringBuffer request; //empty
    StringBuffer response;
    StringBuffer status;
    if (0 > httpclient->sendRequest("GET", NULL, request, response, status) || !response.length() || strncmp("200", status, 3))
         throw MakeStringException(-1, "Error fetching remote queryset information: %s %s %s", srcAddress, srcTarget, status.str());

    return createPTreeFromXMLString(response);
}

class QueryCloner
{
public:
    QueryCloner(IEspContext *_context, const char *address, const char *source, const char *_target) :
        context(_context), cloneFilesEnabled(false), target(_target), overwriteDfs(false), srcAddress(address)
    {
        if (srcAddress.length())
            srcQuerySet.setown(fetchRemoteQuerySetInfo(context, srcAddress, source));
        else
            srcQuerySet.setown(getQueryRegistry(source, true));
        if (!srcQuerySet)
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Source Queryset %s %s not found", srcAddress.str(), source);

        destQuerySet.setown(getQueryRegistry(target, false));
        if (!destQuerySet) // getQueryRegistry should have created if not found
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Destination Queryset %s could not be created, or found", target.str());

        factory.setown(getWorkUnitFactory(context->querySecManager(), context->queryUser()));
    }

    void cloneQueryRemote(IPropertyTree *query, bool makeActive)
    {
        StringBuffer wuid = query->queryProp("Wuid");
        if (!wuid.length())
            return;
        const char *queryName = query->queryProp("Name");
        if (!queryName || !*queryName)
            return;

        StringBuffer xml;
        MemoryBuffer dll;
        StringBuffer dllname;
        StringBuffer fetchedName;
        StringBuffer remoteDfs;
        fetchRemoteWorkunit(NULL, context, srcAddress.str(), NULL, NULL, wuid, fetchedName, xml, dllname, dll, remoteDfs);
        deploySharedObject(*context, wuid, dllname, target, queryName, dll, NULL, xml.str());

        SCMStringBuffer existingQueryId;
        queryIdFromQuerySetWuid(destQuerySet, wuid, queryName, existingQueryId);
        if (existingQueryId.length())
        {
            existingQueryIds.append(existingQueryId.str());
            if (makeActive)
                activateQuery(destQuerySet, ACTIVATE_SUSPEND_PREVIOUS, queryName, existingQueryId.str(), context->queryUserId());
            return;
        }
        StringBuffer newQueryId;
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        addQueryToQuerySet(workunit, destQuerySet, queryName, makeActive ? ACTIVATE_SUSPEND_PREVIOUS : DO_NOT_ACTIVATE, newQueryId, context->queryUserId());
        copiedQueryIds.append(newQueryId);
        Owned<IPropertyTree> destQuery = getQueryById(destQuerySet, newQueryId);
        if (destQuery)
        {
            Owned<IAttributeIterator> aiter = query->getAttributes();
            ForEach(*aiter)
            {
                const char *atname = aiter->queryName();
                if (!destQuery->hasProp(atname))
                    destQuery->setProp(atname, aiter->queryValue());
            }
            if (cloneFilesEnabled && wufiles)
                wufiles->addFilesFromQuery(workunit, pm, newQueryId);
        }
    }

    void cloneQueryLocal(IPropertyTree *query, bool makeActive)
    {
        const char *wuid = query->queryProp("@wuid");
        if (!wuid || !*wuid)
            return;
        const char *queryName = query->queryProp("@name");
        if (!queryName || !*queryName)
            return;
        SCMStringBuffer existingQueryId;
        queryIdFromQuerySetWuid(destQuerySet, wuid, queryName, existingQueryId);
        if (existingQueryId.length())
        {
            existingQueryIds.append(existingQueryId.str());
            if (makeActive)
                activateQuery(destQuerySet, ACTIVATE_SUSPEND_PREVIOUS, queryName, existingQueryId.str(), context->queryUserId());
            return;
        }
        StringBuffer newQueryId;
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        addQueryToQuerySet(workunit, destQuerySet, queryName, makeActive ? ACTIVATE_SUSPEND_PREVIOUS : DO_NOT_ACTIVATE, newQueryId, context->queryUserId());
        copiedQueryIds.append(newQueryId);
        Owned<IPropertyTree> destQuery = getQueryById(destQuerySet, newQueryId);
        if (destQuery)
        {
            Owned<IAttributeIterator> aiter = query->getAttributes();
            ForEach(*aiter)
            {
                const char *atname = aiter->queryName();
                if (!destQuery->hasProp(atname))
                    destQuery->setProp(atname, aiter->queryValue());
            }
            Owned<IPropertyTreeIterator> children = query->getElements("*");
            ForEach(*children)
            {
                IPropertyTree &child = children->query();
                destQuery->addPropTree(child.queryName(), createPTreeFromIPT(&child));
            }
            if (cloneFilesEnabled && wufiles)
                wufiles->addFilesFromQuery(workunit, pm, newQueryId);
        }
    }

    void cloneActiveRemote(bool makeActive)
    {
        Owned<IPropertyTreeIterator> activeQueries = srcQuerySet->getElements("QuerysetAliases/QuerySetAlias");
        ForEach(*activeQueries)
        {
            IPropertyTree &alias = activeQueries->query();
            VStringBuffer xpath("QuerysetQueries/QuerySetQuery[Id='%s'][1]", alias.queryProp("Id"));
            IPropertyTree *query = srcQuerySet->queryPropTree(xpath);
            if (!query)
                continue;
            cloneQueryRemote(query, makeActive);
        }
    }

    void cloneActiveLocal(bool makeActive)
    {
        Owned<IPropertyTreeIterator> activeQueries = srcQuerySet->getElements("Alias");
        ForEach(*activeQueries)
        {
            IPropertyTree &alias = activeQueries->query();
            Owned<IPropertyTree> query = getQueryById(srcQuerySet, alias.queryProp("@id"));
            if (!query)
                return;
            cloneQueryLocal(query, makeActive);
        }
    }

    void cloneActive(bool makeActive)
    {
        if (srcAddress.length())
            cloneActiveRemote(makeActive);
        else
            cloneActiveLocal(makeActive);
    }

    void cloneAllRemote(bool cloneActiveState)
    {
        Owned<IPropertyTreeIterator> allQueries = srcQuerySet->getElements("QuerysetQueries/QuerySetQuery");
        ForEach(*allQueries)
        {
            IPropertyTree &query = allQueries->query();
            bool makeActive = false;
            if (cloneActiveState)
            {
                VStringBuffer xpath("QuerysetAliases/QuerySetAlias[Id='%s']", query.queryProp("Id"));
                makeActive = srcQuerySet->hasProp(xpath);
            }
            cloneQueryRemote(&query, makeActive);
        }
    }
    void cloneAllLocal(bool cloneActiveState)
    {
        Owned<IPropertyTreeIterator> allQueries = srcQuerySet->getElements("Query");
        ForEach(*allQueries)
        {
            IPropertyTree &query = allQueries->query();
            bool makeActive = false;
            if (cloneActiveState)
            {
                VStringBuffer xpath("Alias[@id='%s']", query.queryProp("@id"));
                makeActive = srcQuerySet->hasProp(xpath);
            }
            cloneQueryLocal(&query, makeActive);
        }
    }
    void cloneAll(bool cloneActiveState)
    {
        if (srcAddress.length())
            cloneAllRemote(cloneActiveState);
        else
            cloneAllLocal(cloneActiveState);
    }
    void enableFileCloning(const char *dfsServer, const char *destProcess, const char *sourceProcess, bool _overwriteDfs, bool allowForeign)
    {
        cloneFilesEnabled = true;
        overwriteDfs = _overwriteDfs;
        splitDerivedDfsLocation(dfsServer, srcCluster, dfsIP, srcPrefix, sourceProcess, sourceProcess, NULL, NULL);
        wufiles.setown(createReferencedFileList(context->queryUserId(), context->queryPassword(), allowForeign, false));
        Owned<IHpccPackageSet> ps = createPackageSet(destProcess);
        pm.set(ps->queryActiveMap(target));
        process.set(destProcess);
    }

    void cloneFiles()
    {
        if (cloneFilesEnabled)
        {
            wufiles->resolveFiles(process, dfsIP, srcPrefix, srcCluster, !overwriteDfs, true, true);
            Owned<IDFUhelper> helper = createIDFUhelper();
            Owned <IConstWUClusterInfo> cl = getTargetClusterInfo(target);
            if (cl)
            {
                SCMStringBuffer process;
                StringBuffer defReplicateFolder;
                getConfigurationDirectory(NULL, "data2", "roxie", cl->getRoxieProcess(process).str(), defReplicateFolder);
                wufiles->cloneAllInfo(helper, overwriteDfs, true, true, cl->getRoxieRedundancy(), cl->getChannelsPerNode(), cl->getRoxieReplicateOffset(), defReplicateFolder);
            }
        }
    }
private:
    Linked<IEspContext> context;
    Linked<IWorkUnitFactory> factory;
    Owned<IPropertyTree> destQuerySet;
    Owned<IPropertyTree> srcQuerySet;
    Owned<IReferencedFileList> wufiles;
    Owned<const IHpccPackageMap> pm;
    StringBuffer dfsIP;
    StringBuffer srcAddress;
    StringBuffer srcCluster;
    StringBuffer srcPrefix;
    StringAttr target;
    StringAttr process;
    bool cloneFilesEnabled;
    bool overwriteDfs;

public:
    StringArray existingQueryIds;
    StringArray copiedQueryIds;
};

bool CWsWorkunitsEx::onWUCopyQuerySet(IEspContext &context, IEspWUCopyQuerySetRequest &req, IEspWUCopyQuerySetResponse &resp)
{
    const char *source = req.getSource();
    if (!source || !*source)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "No source target specified");

    StringBuffer srcAddress;
    StringBuffer srcTarget;
    if (!splitQueryPath(source, srcAddress, srcTarget, NULL))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid source target");
    if (!srcAddress.length() && !isValidCluster(srcTarget))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid source target name: %s", source);

    const char *target = req.getTarget();
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "No destination target specified");
    if (!isValidCluster(target))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid destination target name: %s", target);

    QueryCloner cloner(&context, srcAddress, srcTarget, target);

    SCMStringBuffer process;
    if (req.getCopyFiles())
    {
        Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(target);
        if (clusterInfo && clusterInfo->getPlatform()==RoxieCluster)
        {
            clusterInfo->getRoxieProcess(process);
            if (!process.length())
                throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "DFS process cluster not found for destination target %s", target);
            cloner.enableFileCloning(req.getDfsServer(), process.str(), req.getSourceProcess(), req.getOverwriteDfs(), req.getAllowForeignFiles());
        }
    }

    if (req.getActiveOnly())
        cloner.cloneActive(req.getCloneActiveState());
    else
        cloner.cloneAll(req.getCloneActiveState());

    cloner.cloneFiles();

    resp.setCopiedQueries(cloner.copiedQueryIds);
    resp.setExistingQueries(cloner.existingQueryIds);

    return true;
}

bool CWsWorkunitsEx::onWUQuerysetCopyQuery(IEspContext &context, IEspWUQuerySetCopyQueryRequest &req, IEspWUQuerySetCopyQueryResponse &resp)
{
    unsigned start = msTick();
    const char *source = req.getSource();
    if (!source || !*source)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "No source query specified");
    const char *target = req.getTarget();
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "No destination specified");
    if (strchr(target, '/')) //for future use
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid target queryset name");
    if (req.getCluster() && *req.getCluster() && !strieq(req.getCluster(), target)) //backward compatability check
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid target cluster and queryset must match");
    if (!isValidCluster(target))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid target name: %s", target);

    StringBuffer srcAddress, srcQuerySet, srcQuery;
    if (!splitQueryPath(source, srcAddress, srcQuerySet, &srcQuery))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid source query path");

    StringAttr targetQueryName(req.getDestName());
    Owned<IClientWUQuerySetDetailsResponse> sourceQueryInfoResp;
    IConstQuerySetQuery *srcInfo=NULL;

    StringBuffer remoteIP;
    StringBuffer wuid;
    if (srcAddress.length())
    {
        StringBuffer xml;
        MemoryBuffer dll;
        StringBuffer dllname;
        StringBuffer queryName;
        fetchRemoteWorkunitAndQueryDetails(NULL, &context, srcAddress.str(), srcQuerySet.str(), srcQuery.str(), NULL, queryName, xml, dllname, dll, remoteIP, sourceQueryInfoResp);
        if (sourceQueryInfoResp && sourceQueryInfoResp->getQuerysetQueries().ordinality())
            srcInfo = &sourceQueryInfoResp->getQuerysetQueries().item(0);
        if (srcInfo)
            wuid.set(srcInfo->getWuid());
        if (targetQueryName.isEmpty())
            targetQueryName.set(queryName);
        deploySharedObject(context, wuid, dllname.str(), target, targetQueryName.get(), dll, queryDirectory.str(), xml.str());
    }
    else
    {
        //Could get the atributes without soap call, but this creates a common data structure shared with fetching remote query info
        //Get query attributes before resolveQueryAlias, to avoid deadlock
        sourceQueryInfoResp.setown(fetchQueryDetails(NULL, &context, NULL, srcQuerySet, srcQuery));
        if (sourceQueryInfoResp && sourceQueryInfoResp->getQuerysetQueries().ordinality())
            srcInfo = &sourceQueryInfoResp->getQuerysetQueries().item(0);

        Owned<IPropertyTree> queryset = getQueryRegistry(srcQuerySet.str(), true);
        if (!queryset)
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Source Queryset %s not found", srcQuery.str());

        Owned<IPropertyTree> query = resolveQueryAlias(queryset, srcQuery.str());
        if (!query)
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Source query %s not found", source);
        wuid.set(query->queryProp("@wuid"));
        if (targetQueryName.isEmpty())
            targetQueryName.set(query->queryProp("@name"));
    }

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());

    if (!req.getDontCopyFiles())
    {
        StringBuffer daliIP;
        StringBuffer srcCluster;
        StringBuffer srcPrefix;
        splitDerivedDfsLocation(req.getDaliServer(), srcCluster, daliIP, srcPrefix, req.getSourceProcess(), req.getSourceProcess(), remoteIP.str(), NULL);
        copyQueryFilesToCluster(context, cw, daliIP.str(), srcPrefix, target, srcCluster, targetQueryName.get(), req.getOverwrite(), req.getAllowForeignFiles());
    }

    WorkunitUpdate wu(&cw->lock());
    if (!wu)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Error opening wuid %s for query %s", wuid.str(), source);

    StringBuffer targetQueryId;
    WUQueryActivationOptions activate = (WUQueryActivationOptions)req.getActivate();
    addQueryToQuerySet(wu, target, targetQueryName.get(), activate, targetQueryId, context.queryUserId());

    Owned<IPropertyTree> queryTree = getQueryById(target, targetQueryId, false);
    if (queryTree)
    {
        updateMemoryLimitSetting(queryTree, req.getMemoryLimit(), srcInfo);
        updateQueryPriority(queryTree, req.getPriority(), srcInfo);
        updateTimeLimitSetting(queryTree, req.getTimeLimit_isNull(), req.getTimeLimit(), srcInfo);
        updateWarnTimeLimitSetting(queryTree, req.getWarnTimeLimit_isNull(), req.getWarnTimeLimit(), srcInfo);
        if (req.getComment())
            queryTree->setProp("@comment", req.getComment());
        else if (srcInfo && srcInfo->getComment())
            queryTree->setProp("@comment", srcInfo->getComment());
        if (srcInfo && srcInfo->getSnapshot())
            queryTree->setProp("@snapshot", srcInfo->getSnapshot());
    }
    wu.clear();

    resp.setQueryId(targetQueryId.str());

    if (0!=req.getWait() && !req.getNoReload())
        reloadCluster(target, remainingMsWait(req.getWait(), start));
    return true;
}

void CWsWorkunitsEx::getGraphsByQueryId(const char *target, const char *queryId, const char *graphId, const char *subGraphId, IArrayOf<IEspECLGraphEx>& ECLGraphs)
{
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Target name required");
    if (!queryId || !*queryId)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query Id required");

    Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target);
    if (!info || (info->getPlatform()!=RoxieCluster)) //Only support roxie for now
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid Roxie name");

    const SocketEndpointArray &eps = info->getRoxieServers();
    if (eps.empty())
        return;

    VStringBuffer control("<control:querystats><Query id='%s'/></control:querystats>", queryId);
    Owned<IPropertyTree> querystats = sendRoxieControlAllNodes(eps.item(0), control.str(), false, ROXIELOCKCONNECTIONTIMEOUT);
    if (!querystats)
        return;

    Owned<IPropertyTreeIterator> graphs = querystats->getElements("Endpoint/Query/Graph");
    ForEach(*graphs)
    {
        IPropertyTree &graph = graphs->query();
        const char* aGraphId = graph.queryProp("@id");
        if (graphId && *graphId && !strieq(graphId, aGraphId))
            continue;

        IPropertyTree* xgmml = graph.getBranch("xgmml/graph");
        if (!xgmml)
            continue;

        Owned<IEspECLGraphEx> g = createECLGraphEx("","");
        g->setName(aGraphId);

        StringBuffer xml;
        if (!subGraphId || !*subGraphId)
            toXML(xgmml, xml);
        else
        {
            VStringBuffer xpath("//node[@id='%s']", subGraphId);
            toXML(xgmml->queryPropTree(xpath.str()), xml);
        }

        g->setGraph(xml.str());
        ECLGraphs.append(*g.getClear());
    }
    return;
}

bool CWsWorkunitsEx::onWUQueryGetGraph(IEspContext& context, IEspWUQueryGetGraphRequest& req, IEspWUQueryGetGraphResponse& resp)
{
    try
    {
        IArrayOf<IEspECLGraphEx> graphs;
        getGraphsByQueryId(req.getTarget(), req.getQueryId(), req.getGraphName(), req.getSubGraphId(), graphs);
        resp.setGraphs(graphs);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::resetQueryStats(IEspContext& context, const char* target, IProperties* queryIds, IEspWUQuerySetQueryActionResponse& resp)
{
    IArrayOf<IEspQuerySetQueryActionResult> results;
    Owned<IEspQuerySetQueryActionResult> result = createQuerySetQueryActionResult();
    try
    {
        StringBuffer control;
        Owned<IPropertyIterator> it = queryIds->getIterator();
        ForEach(*it)
        {
            const char *querySetId = it->getPropKey();
            if (querySetId && *querySetId)
                control.appendf("<Query id='%s'/>", querySetId);
        }
        if (!control.length())
            throw MakeStringException(ECLWATCH_MISSING_PARAMS, "CWsWorkunitsEx::resetQueryStats: Query ID not specified");

        control.insert(0, "<control:resetquerystats>");
        control.append("</control:resetquerystats>");

        if (!sendControlQuery(context, target, control.str(), ROXIECONNECTIONTIMEOUT))
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "CWsWorkunitsEx::resetQueryStats: Failed to send roxie control query");

        result->setMessage("Query stats reset succeeded");
        result->setSuccess(true);;
    }
    catch(IMultiException *me)
    {
        StringBuffer msg;
        result->setMessage(me->errorMessage(msg).str());
        result->setCode(me->errorCode());
        result->setSuccess(false);
        me->Release();
    }
    catch(IException *e)
    {
        StringBuffer msg;
        result->setMessage(e->errorMessage(msg).str());
        result->setCode(e->errorCode());
        result->setSuccess(false);
        e->Release();
    }
    results.append(*result.getClear());
    resp.setResults(results);
    return true;
}

IPropertyTree* CWsWorkunitsEx::sendControlQuery(IEspContext& context, const char* target, const char* query, unsigned timeout)
{
    if (!target || !*target)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "CWsWorkunitsEx::sendControlQuery: target not specified");

    if (!query || !*query)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "CWsWorkunitsEx::sendControlQuery: Control query not specified");

    Owned<IConstWUClusterInfo> info = getTargetClusterInfo(target);
    if (!info || (info->getPlatform()!=RoxieCluster)) //Only support roxie for now
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "CWsWorkunitsEx::sendControlQuery: Invalid target name %s", target);

    const SocketEndpointArray &eps = info->getRoxieServers();
    if (eps.empty())
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "CWsWorkunitsEx::sendControlQuery: Server not found for %s", target);

    Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), timeout);
    return sendRoxieControlQuery(sock, query, timeout);
}
