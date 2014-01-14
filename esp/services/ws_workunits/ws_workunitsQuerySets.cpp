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

#define DALI_FILE_LOOKUP_TIMEOUT (1000*15*1)  // 15 seconds

const unsigned ROXIECONNECTIONTIMEOUT = 1000;   //1 second
const unsigned ROXIECONTROLQUERYTIMEOUT = 3000; //3 second

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5mins, 30s a bit short

bool isRoxieProcess(const char *process)
{
    if (!process)
        return false;
    Owned<IRemoteConnection> conn = querySDS().connect("Environment", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    if (!conn)
        return false;
    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", process);
    return conn->queryRoot()->hasProp(xpath.str());
}

void checkUseEspOrDaliIP(SocketEndpoint &ep, const char *ip, const char *esp)
{
    if (!ip || !*ip)
        return;
    ep.set(ip, 7070);
    if (ep.isLoopBack() || *ip=='.' || (ip[0]=='0' && ip[1]=='.'))
        ep.ipset(esp);
}

void fetchRemoteWorkunit(IEspContext &context, const char *netAddress, const char *queryset, const char *query, const char *wuid, StringBuffer &name, StringBuffer &xml, StringBuffer &dllname, MemoryBuffer &dll, StringBuffer &daliServer)
{
    Owned<IClientWsWorkunits> ws;
    ws.setown(createWsWorkunitsClient());
    VStringBuffer url("http://%s%s/WsWorkunits", netAddress, (!strchr(netAddress, ':')) ? ":8010" : "");
    ws->addServiceUrl(url.str());

    if (context.queryUserId() && *context.queryUserId())
        ws->setUsernameToken(context.queryUserId(), context.queryPassword(), NULL);

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

                if(kind==TAKdiskwrite || kind==TAKindexwrite || kind==TAKcsvwrite || kind==TAKxmlwrite)
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

bool CWsWorkunitsEx::onWUCopyLogicalFiles(IEspContext &context, IEspWUCopyLogicalFilesRequest &req, IEspWUCopyLogicalFilesResponse &resp)
{
    StringBuffer wuid = req.getWuid();
    checkAndTrimWorkunit("WUCopyLogicalFiles", wuid);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s", wuid.str());

    resp.setWuid(wuid.str());

    SCMStringBuffer cluster;
    if (notEmpty(req.getCluster()))
        cluster.set(req.getCluster());
    else
        cw->getClusterName(cluster);
    if (!isValidCluster(req.getCluster()))
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

static inline void updateMemoryLimitSetting(IPropertyTree *queryTree, const char *value)
{
    if (!value || !queryTree)
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
static inline void updateQueryPriority(IPropertyTree *queryTree, const char *value)
{
    if (!value || !*value || !queryTree)
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

void copyQueryFilesToCluster(IEspContext &context, IConstWorkUnit *cw, const char *remoteIP, const char *remotePrefix, const char *target, const char *srcCluster, const char *queryname, bool overwrite)
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
        Owned<IReferencedFileList> wufiles = createReferencedFileList(context.queryUserId(), context.queryPassword());
        Owned<IHpccPackageSet> ps = createPackageSet(process.str());
        StringBuffer queryid;
        if (queryname && *queryname)
            queryname = queryid.append(queryname).append(".0").str(); //prepublish dummy version number to support fuzzy match like queries="myquery.*" in package
        wufiles->addFilesFromQuery(cw, (ps) ? ps->queryActiveMap(target) : NULL, queryname);
        wufiles->resolveFiles(process.str(), remoteIP, remotePrefix, srcCluster, !overwrite, true);
        Owned<IDFUhelper> helper = createIDFUhelper();
        wufiles->cloneAllInfo(helper, overwrite, true);
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
    checkAndTrimWorkunit("WUPublishWorkunit", wuid);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", wuid.str());

    resp.setWuid(wuid.str());

    SCMStringBuffer queryName;
    if (notEmpty(req.getJobName()))
        queryName.set(req.getJobName());
    else
        cw->getJobName(queryName).str();
    if (!queryName.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query/Job name not defined for publishing workunit %s", wuid.str());

    SCMStringBuffer target;
    if (notEmpty(req.getCluster()))
        target.set(req.getCluster());
    else
        cw->getClusterName(target);
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
        copyQueryFilesToCluster(context, cw, daliIP, srcPrefix, target.str(), srcCluster, queryName.str(), false);

    WorkunitUpdate wu(&cw->lock());
    if (req.getUpdateWorkUnitName() && notEmpty(req.getJobName()))
        wu->setJobName(req.getJobName());

    StringBuffer queryId;
    WUQueryActivationOptions activate = (WUQueryActivationOptions)req.getActivate();
    addQueryToQuerySet(wu, target.str(), queryName.str(), NULL, activate, queryId, context.queryUserId());
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
    Owned<IEspClusterQueryState> clusterState = createClusterQueryState();
    clusterState->setCluster(target);

    VStringBuffer xpath("Endpoint/Queries/Query[@id='%s']", id);
    Owned<IPropertyTreeIterator> iter = queriesOnCluster->getElements(xpath.str());
    bool found = false;
    bool suspended = false;
    StringBuffer errors;
    ForEach (*iter)
    {
        found = true;
        if (iter->query().getPropBool("@suspended", false))
            suspended = true;
        const char* error = iter->query().queryProp("@error");
        if (error && *error && (version >=1.46))
        {
            if (errors.length())
                errors.append(";");
            errors.append(error);
        }
    }
    if (!found)
        clusterState->setState("Not Found");
    else if (suspended)
        clusterState->setState("Suspended");
    else
        clusterState->setState("Available");
    if ((version >=1.46) && errors.length())
        clusterState->setErrors(errors.str());

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
        return sendRoxieControlQuery(sock, "<control:queries/>", ROXIECONTROLQUERYTIMEOUT);
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
    VStringBuffer vBuf("%"I64F"d", value);
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
        else if (strieq(sortBy, "MemoryLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFmemoryLimit | WUQSFnumeric);
        else if (strieq(sortBy, "TimeLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFtimeLimit | WUQSFnumeric);
        else if (strieq(sortBy, "WarnTimeLimit"))
            sortOrder[0] = (WUQuerySortField) (WUQSFwarnTimeLimit | WUQSFnumeric);
        else if (strieq(sortBy, "Priority"))
            sortOrder[0] = (WUQuerySortField) (WUQSFpriority | WUQSFnumeric);
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
    filters[filterCount] = WUQSFterm;

    unsigned numberOfQueries = 0;
    unsigned pageSize = req.getPageSize();
    unsigned pageStartFrom = req.getPageStartFrom();
    if(pageSize < 1)
        pageSize = 100;
    __int64 cacheHint = 0;
    if (!req.getCacheHint_isNull())
        cacheHint = req.getCacheHint();

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstQuerySetQueryIterator> it = factory->getQuerySetQueriesSorted(sortOrder, filters, filterBuf.bufferBase(), pageStartFrom, pageSize, &cacheHint, &numberOfQueries);
    resp.setCacheHint(cacheHint);

    IArrayOf<IEspQuerySetQuery> queries;
    double version = context.getClientVersion();
    ForEach(*it)
    {
        IPropertyTree &query=it->query();
        Owned<IEspQuerySetQuery> q = createQuerySetQuery();
        q->setId(query.queryProp("@id"));
        q->setName(query.queryProp("@name"));
        q->setQuerySetId(query.queryProp("@querySetId"));
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

        try
        {
            const char* cluster = clusterReq;
            const char* querySetId = query.queryProp("@querySetId");
            if (isEmpty(cluster))
                cluster = querySetId;
            Owned<IPropertyTree> queriesOnCluster = getQueriesOnCluster(cluster, querySetId);
            if (queriesOnCluster)
            {
                IArrayOf<IEspClusterQueryState> clusters;
                addClusterQueryStates(queriesOnCluster, cluster, query.queryProp("@id"), clusters, version);
                q->setClusters(clusters);
            }
        }
        catch(IException *e)
        {
            StringBuffer err;
            DBGLOG("Get exception in WUListQueries: %s", e->errorMessage(err.append(e->errorCode()).append(' ')).str());
            e->Release();
        }
        queries.append(*q.getClear());
    }
    resp.setQuerysetQueries(queries);
    resp.setNumberOfQueries(numberOfQueries);

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
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid);

        if (query->hasProp("@priority"))
            resp.setPriority(getQueryPriorityName(query->getPropInt("@priority")));
        resp.setIsLibrary(query->getPropBool("@isLibrary"));
        SCMStringBuffer s;
        resp.setWUSnapShot(cw->getSnapshot(s).str()); //Label
        cw->getTimeStamp("Compiled", "EclCCServer", s);
        if (!s.length())
            cw->getTimeStamp("Compiled", "EclServer", s);
        if (s.length())
            resp.setCompileTime(s.str());
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
    return true;
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
                respSuperFile->setSubFiles(respSubFiles);
                respSuperFiles->append(*respSuperFile.getClear());
            }
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

bool splitQueryPath(const char *path, StringBuffer &netAddress, StringBuffer &queryset, StringBuffer &query)
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
        return false;
    if (nextQueryPathNode(path, query))
        return false; //query path too deep
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
    if (!splitQueryPath(source, srcAddress, srcQuerySet, srcQuery))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid source query path");

    StringAttr targetQueryName(req.getDestName());

    StringBuffer remoteIP;
    StringBuffer wuid;
    if (srcAddress.length())
    {
        StringBuffer xml;
        MemoryBuffer dll;
        StringBuffer dllname;
        StringBuffer queryName;
        fetchRemoteWorkunit(context, srcAddress.str(), srcQuerySet.str(), srcQuery.str(), NULL, queryName, xml, dllname, dll, remoteIP);
        if (targetQueryName.isEmpty())
            targetQueryName.set(queryName);
        deploySharedObject(context, wuid, dllname.str(), target, targetQueryName.get(), dll, queryDirectory.str(), xml.str());
    }
    else
    {
        Owned<IPropertyTree> queryset = getQueryRegistry(srcQuerySet.str(), true);
        if (!queryset)
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Source Queryset %s not found", srcQuery.str());

        IPropertyTree *query = resolveQueryAlias(queryset, srcQuery.str());
        if (!query)
            throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Source query %s not found", source);
        wuid.set(query->queryProp("@wuid"));
        if (targetQueryName.isEmpty())
            targetQueryName.set(query->queryProp("@name"));
    }

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);

    if (!req.getDontCopyFiles())
    {
        StringBuffer daliIP;
        StringBuffer srcCluster;
        StringBuffer srcPrefix;
        splitDerivedDfsLocation(req.getDaliServer(), srcCluster, daliIP, srcPrefix, req.getSourceProcess(), req.getSourceProcess(), remoteIP.str(), NULL);
        copyQueryFilesToCluster(context, cw, daliIP.str(), srcPrefix, target, srcCluster, targetQueryName.get(), req.getOverwrite());
    }

    WorkunitUpdate wu(&cw->lock());
    if (!wu)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Error opening wuid %s for query %s", wuid.str(), source);

    StringBuffer targetQueryId;
    WUQueryActivationOptions activate = (WUQueryActivationOptions)req.getActivate();
    addQueryToQuerySet(wu, target, targetQueryName.get(), NULL, activate, targetQueryId, context.queryUserId());
    if (req.getMemoryLimit() || !req.getTimeLimit_isNull() || ! req.getWarnTimeLimit_isNull() || req.getPriority())
    {
        Owned<IPropertyTree> queryTree = getQueryById(target, targetQueryId, false);
        updateMemoryLimitSetting(queryTree, req.getMemoryLimit());
        updateQueryPriority(queryTree, req.getPriority());
        updateQuerySetting(req.getTimeLimit_isNull(), queryTree, "@timeLimit", req.getTimeLimit());
        updateQuerySetting(req.getWarnTimeLimit_isNull(), queryTree, "@warnTimeLimit", req.getWarnTimeLimit());
        if (req.getComment())
            queryTree->setProp("@comment", req.getComment());
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

    VStringBuffer xpath("<control:querystats><Query id='%s'/></control:querystats>", queryId);
    Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), ROXIECONNECTIONTIMEOUT);
    Owned<IPropertyTree> querystats = sendRoxieControlQuery(sock, xpath.str(), ROXIECONTROLQUERYTIMEOUT);
    if (!querystats)
        return;

    Owned<IPropertyTreeIterator> graphs = querystats->getElements("Endpoint/Query/Graph");
    ForEach(*graphs)
    {
        IPropertyTree &graph = graphs->query();
        const char* aGraphId = graph.queryProp("@id");
        if (graphId && *graphId && !strieq(graphId, aGraphId))
            continue;

        IPropertyTree* xgmml = graph.getBranch("xgmml/graph/graph");
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
