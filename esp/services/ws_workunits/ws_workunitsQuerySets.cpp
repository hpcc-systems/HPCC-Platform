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

#include "ws_workunitsService.hpp"
#include "ws_fs.hpp"
#include "jlib.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dfuwu.hpp"
#include "eclhelper.hpp"

const unsigned roxieQueryRoxieTimeOut = 60000;

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

void fetchRemoteWorkunit(IEspContext &context, const char *netAddress, const char *queryset, const char *query, const char *wuid, StringBuffer &name, StringBuffer &xml, StringBuffer &dllname, MemoryBuffer &dll)
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
                                bool supercopy = queryDistributedFileDirectory().isSuperFile(logicalname, NULL, udesc);
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
    if (isEmpty(req.getWuid()))
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "WUCopyLogicalFiles WUID parameter not set.");

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(req.getWuid(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s", req.getWuid());

    resp.setWuid(req.getWuid());

    SCMStringBuffer cluster;
    if (notEmpty(req.getCluster()))
        cluster.set(req.getCluster());
    else
        cw->getClusterName(cluster);

    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster.str());

    IArrayOf<IConstWUCopyLogicalClusterFileSections> clusterfiles;
    copyWULogicalFilesToTarget(context, *clusterInfo, *cw, clusterfiles, req.getCopyLocal());
    resp.setClusterFiles(clusterfiles);

    return true;
}

void checkRoxieControlExceptions(IPropertyTree *msg)
{
    Owned<IMultiException> me = MakeMultiException();
    Owned<IPropertyTreeIterator> endpoints = msg->getElements("Endpoint");
    ForEach(*endpoints)
    {
        IPropertyTree &endp = endpoints->query();
        Owned<IPropertyTreeIterator> exceptions = endp.getElements("Exception");
        ForEach (*exceptions)
        {
            IPropertyTree &ex = exceptions->query();
            me->append(*MakeStringException(ex.getPropInt("Code"), "Endpoint %s: %s", endp.queryProp("@ep"), ex.queryProp("Message")));
        }
    }
    if (me->ordinality())
        throw me.getClear();
}

static inline unsigned waitMsToSeconds(unsigned wait)
{
    if (wait==0 || wait==(unsigned)-1)
        return wait;
    return wait/1000;
}

IPropertyTree *sendRoxieControlQuery(ISocket *sock, const char *msg, unsigned wait)
{
    size32_t msglen = strlen(msg);
    size32_t len = msglen;
    _WINREV(len);
    sock->write(&len, sizeof(len));
    sock->write(msg, msglen);

    StringBuffer resp;
    loop
    {
        sock->read(&len, sizeof(len));
        if (!len)
            break;
        _WINREV(len);
        size32_t size_read;
        sock->read(resp.reserveTruncate(len), len, len, size_read, waitMsToSeconds(wait));
        if (size_read<len)
            throw MakeStringException(ECLWATCH_CONTROL_QUERY_FAILED, "Error reading roxie control message response");
    }

    Owned<IPropertyTree> ret = createPTreeFromXMLString(resp.str());
    checkRoxieControlExceptions(ret);
    return ret.getClear();
}

bool sendRoxieControlLock(ISocket *sock, bool allOrNothing, unsigned wait)
{
    Owned<IPropertyTree> resp = sendRoxieControlQuery(sock, "<control:lock/>", wait);
    if (allOrNothing)
    {
        int lockCount = resp->getPropInt("Lock", 0);
        int serverCount = resp->getPropInt("NumServers", 0);
        return (lockCount && (lockCount == serverCount));
    }

    return resp->getPropInt("Lock", 0) != 0;
}

static inline unsigned remainingMsWait(unsigned wait, unsigned start)
{
    if (wait==0 || wait==(unsigned)-1)
        return wait;
    unsigned waited = msTick()-start;
    return (wait>waited) ? wait-waited : 0;
}

IPropertyTree *sendRoxieControlAllNodes(ISocket *sock, const char *msg, bool allOrNothing, unsigned wait)
{
    unsigned start = msTick();
    if (!sendRoxieControlLock(sock, allOrNothing, wait))
        throw MakeStringException(ECLWATCH_CONTROL_QUERY_FAILED, "Roxie control:lock failed");
    return sendRoxieControlQuery(sock, msg, remainingMsWait(wait, start));
}

IPropertyTree *sendRoxieControlAllNodes(const SocketEndpoint &ep, const char *msg, bool allOrNothing, unsigned wait)
{
    Owned<ISocket> sock = ISocket::connect_timeout(ep, wait);
    return sendRoxieControlAllNodes(sock, msg, allOrNothing, wait);
}

bool reloadCluster(IConstWUClusterInfo *clusterInfo, unsigned wait)
{
    if (clusterInfo->getPlatform()==RoxieCluster)
    {
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
    }
    return true;
}

bool reloadCluster(const char *cluster, unsigned wait)
{
    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
    return (clusterInfo) ? reloadCluster(clusterInfo, wait) : true;
}

bool CWsWorkunitsEx::onWUPublishWorkunit(IEspContext &context, IEspWUPublishWorkunitRequest & req, IEspWUPublishWorkunitResponse & resp)
{
    if (isEmpty(req.getWuid()))
        throw MakeStringException(ECLWATCH_NO_WUID_SPECIFIED,"No Workunit ID has been specified.");

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(req.getWuid(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit %s", req.getWuid());

    resp.setWuid(req.getWuid());

    SCMStringBuffer queryName;
    if (notEmpty(req.getJobName()))
        queryName.set(req.getJobName());
    else
        cw->getJobName(queryName).str();
    if (!queryName.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Query/Job name not defined for publishing workunit %s", req.getWuid());

    SCMStringBuffer cluster;
    if (notEmpty(req.getCluster()))
        cluster.set(req.getCluster());
    else
        cw->getClusterName(cluster);
    if (!cluster.length())
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Cluster name not defined for publishing workunit %s", req.getWuid());

    Owned <IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster.str());

    SCMStringBuffer queryset;
    clusterInfo->getQuerySetName(queryset);

    WorkunitUpdate wu(&cw->lock());
    if (notEmpty(req.getJobName()))
        wu->setJobName(req.getJobName());

    StringBuffer queryId;
    addQueryToQuerySet(wu, queryset.str(), queryName.str(), NULL, (WUQueryActivationOptions)req.getActivate(), queryId);
    wu->commit();
    wu.clear();

    if (queryId.length())
        resp.setQueryId(queryId.str());
    resp.setQueryName(queryName.str());
    resp.setQuerySet(queryset.str());

    if (req.getCopyLocal() || req.getShowFiles())
    {
        IArrayOf<IConstWUCopyLogicalClusterFileSections> clusterfiles;
        copyWULogicalFilesToTarget(context, *clusterInfo, *cw, clusterfiles, req.getCopyLocal());
        resp.setClusterFiles(clusterfiles);
    }

    bool reloaded = reloadCluster(clusterInfo, (unsigned)req.getWait());
    resp.setReloadFailed(!reloaded);

    return true;
}

bool CWsWorkunitsEx::onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp)
{
    Owned<IPropertyTree> queryRegistry = getQueryRegistryRoot();
    if (!queryRegistry)
        return false;

    IArrayOf<IEspQuerySet> querySets;
    Owned<IPropertyTreeIterator> it = queryRegistry->getElements("QuerySet");
    ForEach(*it)
    {
        Owned<IEspQuerySet> qs = createQuerySet("", "");
        qs->setQuerySetName(it->query().queryProp("@id"));
        querySets.append(*qs.getClear());

    }
    resp.setQuerysets(querySets);
    return true;
}

void gatherQuerySetQueryDetails(IPropertyTree *query, IEspQuerySetQuery *queryInfo, const char *cluster, IPropertyTree *queriesOnCluster)
{
    queryInfo->setId(query->queryProp("@id"));
    queryInfo->setName(query->queryProp("@name"));
    queryInfo->setDll(query->queryProp("@dll"));
    queryInfo->setWuid(query->queryProp("@wuid"));
    queryInfo->setSuspended(query->getPropBool("@suspended", false));
    if (queriesOnCluster)
    {
        IArrayOf<IEspClusterQueryState> clusters;
        Owned<IEspClusterQueryState> clusterState = createClusterQueryState();
        clusterState->setCluster(cluster);

        VStringBuffer xpath("Endpoint/Queries/Query[@id='%s']", query->queryProp("@id"));
        IPropertyTree *aQuery = queriesOnCluster->getBranch(xpath.str());
        if (!aQuery)
        {
            clusterState->setState("Not Found");
        }
        else if (aQuery->getPropBool("@suspended", false))
        {
            clusterState->setState("Suspended");
        }
        else
        {
            clusterState->setState("Available");
        }

        clusters.append(*clusterState.getClear());
        queryInfo->setClusters(clusters);
    }
}

void gatherQuerySetAliasDetails(IPropertyTree *alias, IEspQuerySetAlias *aliasInfo)
{
    aliasInfo->setName(alias->queryProp("@name"));
    aliasInfo->setId(alias->queryProp("@id"));
}

void retrieveAllQuerysetDetails(IPropertyTree *registry, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL, const char *type=NULL, const char *value=NULL)
{
    Owned<IPropertyTreeIterator> regQueries = registry->getElements("Query");
    ForEach(*regQueries)
    {
        IPropertyTree &query = regQueries->query();
        Owned<IEspQuerySetQuery> q = createQuerySetQuery();
        gatherQuerySetQueryDetails(&query, q, cluster, queriesOnCluster);

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

void retrieveQuerysetDetailsFromAlias(IPropertyTree *registry, const char *name, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster, IPropertyTree *queriesOnCluster)
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
    gatherQuerySetQueryDetails(query, q, cluster, queriesOnCluster);
    queries.append(*q.getClear());
}

void retrieveQuerysetDetailsFromQuery(IPropertyTree *registry, const char *value, const char *type, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
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
    gatherQuerySetQueryDetails(query, q, cluster, queriesOnCluster);
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

void retrieveQuerysetDetails(IPropertyTree *registry, const char *type, const char *value, IArrayOf<IEspQuerySetQuery> &queries, IArrayOf<IEspQuerySetAlias> &aliases, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (strieq(type, "All"))
        return retrieveAllQuerysetDetails(registry, queries, aliases, cluster, queriesOnCluster);
    if (!value || !*value)
        return;
    if (strieq(type, "Alias"))
        return retrieveQuerysetDetailsFromAlias(registry, value, queries, aliases, cluster, queriesOnCluster);
    if (strieq(type, "Status") && !isEmpty(cluster))
        return retrieveAllQuerysetDetails(registry, queries, aliases, cluster, queriesOnCluster, type, value);
    return retrieveQuerysetDetailsFromQuery(registry, value, type, queries, aliases, cluster, queriesOnCluster);
}

void retrieveQuerysetDetails(IArrayOf<IEspWUQuerySetDetail> &details, IPropertyTree *registry, const char *type, const char *value, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (!registry)
        return;

    IArrayOf<IEspQuerySetQuery> queries;
    IArrayOf<IEspQuerySetAlias> aliases;
    retrieveQuerysetDetails(registry, type, value, queries, aliases, cluster, queriesOnCluster);

    Owned<IEspWUQuerySetDetail> detail = createWUQuerySetDetail();
    detail->setQuerySetName(registry->queryProp("@id"));
    detail->setQueries(queries);
    detail->setAliases(aliases);
    details.append(*detail.getClear());
}

void retrieveQuerysetDetails(IArrayOf<IEspWUQuerySetDetail> &details, const char *queryset, const char *type, const char *value, const char *cluster=NULL, IPropertyTree *queriesOnCluster=NULL)
{
    if (!queryset || !*queryset)
        return;
    Owned<IPropertyTree> registry = getQueryRegistry(queryset, true);
    if (!registry)
        return;
    retrieveQuerysetDetails(details, registry, type, value, cluster, queriesOnCluster);
}

void retrieveQuerysetDetailsByCluster(IArrayOf<IEspWUQuerySetDetail> &details, const char *cluster, const char *queryset, const char *type, const char *value)
{
    Owned<IConstWUClusterInfo> info = getTargetClusterInfo(cluster);
    if (!info)
        throw MakeStringException(ECLWATCH_CANNOT_RESOLVE_CLUSTER_NAME, "Cluster %s not found", cluster);

    Owned<IPropertyTree> queriesOnCluster;
    if (info->getPlatform()==RoxieCluster)
    {
        const SocketEndpointArray &eps = info->getRoxieServers();
        if (eps.length())
        {
            Owned<ISocket> sock = ISocket::connect_timeout(eps.item(0), 5);
            queriesOnCluster.setown(sendRoxieControlQuery(sock, "<control:queries/>", 5));
        }
    }
    SCMStringBuffer clusterQueryset;
    info->getQuerySetName(clusterQueryset);
    if (!clusterQueryset.length())
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "No QuerySets found for cluster %s", cluster);
    if (notEmpty(queryset) && !strieq(clusterQueryset.str(), queryset))
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_ON_CLUSTER, "Cluster %s not configured to load QuerySet %s", cluster, queryset);

    retrieveQuerysetDetails(details, clusterQueryset.str(), type, value, cluster, queriesOnCluster);
}

void retrieveAllQuerysetDetails(IArrayOf<IEspWUQuerySetDetail> &details, const char *type, const char *value)
{
    Owned<IPropertyTree> root = getQueryRegistryRoot();
    if (!root)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet Registry not found");
    Owned<IPropertyTreeIterator> querysets = root->getElements("QuerySet");
    if (!root)
        throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet Registry not found");
    ForEach(*querysets)
        retrieveQuerysetDetails(details, &querysets->query(), type, value);
}

bool CWsWorkunitsEx::onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp)
{
    resp.setQuerySetName(req.getQuerySetName());

    double version = context.getClientVersion();
    if (version > 1.36)
    {
        StringBuffer currentTargetClusterType;
        Owned<IPropertyTree> queryRegistry = getQueryRegistry(req.getQuerySetName(), false);
        queryRegistry->getProp("@targetclustertype", currentTargetClusterType);
        if (strieq(currentTargetClusterType.str(), "roxie"))
        {
            StringArray clusterNames;
            getQuerySetTargetClusters(req.getQuerySetName(), clusterNames);
            if (!clusterNames.empty())
                resp.setClusterNames(clusterNames);

            resp.setClusterName(req.getClusterName());
            resp.setFilter(req.getFilter());
            resp.setFilterType(req.getFilterType());
        }
    }

    Owned<IPropertyTree> registry = getQueryRegistry(req.getQuerySetName(), true);
    if (!registry)
        return false;

    IArrayOf<IEspQuerySetQuery> respQueries;
    IArrayOf<IEspQuerySetAlias> respAliases;

    if (isEmpty(req.getClusterName()) || isEmpty(req.getFilterTypeAsString()) || !strieq(req.getFilterTypeAsString(), "Status") || isEmpty(req.getFilter()))
    {
        retrieveQuerysetDetails(registry, req.getFilterTypeAsString(), req.getFilter(), respQueries, respAliases);

        resp.setQuerysetQueries(respQueries);
        resp.setQuerysetAliases(respAliases);
    }
    else
    {
        IArrayOf<IEspWUQuerySetDetail> respDetails;
        retrieveQuerysetDetailsByCluster(respDetails, req.getClusterName(), req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
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
        retrieveQuerysetDetailsByCluster(respDetails, req.getClusterName(), req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
    else if (notEmpty(req.getQuerySetName()))
        retrieveQuerysetDetails(respDetails, req.getQuerySetName(), req.getFilterTypeAsString(), req.getFilter());
    else
        retrieveAllQuerysetDetails(respDetails, req.getFilterTypeAsString(), req.getFilter());

    resp.setQuerysets(respDetails);

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

    IArrayOf<IEspQuerySetQueryActionResult> results;
    ForEachItemIn(i, req.getQueries())
    {
        IConstQuerySetQueryActionItem& item=req.getQueries().item(i);
        Owned<IEspQuerySetQueryActionResult> result = createQuerySetQueryActionResult();
        try
        {
            VStringBuffer xpath("Query[@id='%s']", item.getQueryId());
            IPropertyTree *query = queryset->queryPropTree(xpath.str());
            if (!query)
                throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query %s/%s not found.", req.getQuerySetName(), item.getQueryId());
            switch (req.getAction())
            {
                case CQuerySetQueryActionTypes_ToggleSuspend:
                    setQuerySuspendedState(queryset, item.getQueryId(), !item.getClientState().getSuspended());
                    break;
                case CQuerySetQueryActionTypes_Suspend:
                    setQuerySuspendedState(queryset, item.getQueryId(), true);
                    break;
                case CQuerySetQueryActionTypes_Unsuspend:
                    setQuerySuspendedState(queryset, item.getQueryId(), false);
                    break;
                case CQuerySetQueryActionTypes_Activate:
                    setQueryAlias(queryset, query->queryProp("@name"), item.getQueryId());
                    break;
                case CQuerySetQueryActionTypes_Delete:
                    removeAliasesFromNamedQuery(queryset, item.getQueryId());
                    removeNamedQuery(queryset, item.getQueryId());
                    break;
                case CQuerySetQueryActionTypes_RemoveAllAliases:
                    removeAliasesFromNamedQuery(queryset, item.getQueryId());
                    break;
            }
            result->setSuccess(true);
            query = queryset->queryPropTree(xpath.str()); // refresh
            if (query)
                result->setSuspended(query->getPropBool("@suspended"));
        }
        catch(IException *e)
        {
            StringBuffer msg;
            result->setMessage(e->errorMessage(msg).str());
            result->setCode(e->errorCode());
            result->setSuccess(false);
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

    StringBuffer srcAddress, srcQuerySet, srcQuery;
    if (!splitQueryPath(source, srcAddress, srcQuerySet, srcQuery))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid source query path");

    StringBuffer targetName;
    StringBuffer wuid;
    if (srcAddress.length())
    {
        const char *cluster = req.getCluster();
        if (!cluster || !*cluster)
            throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Must specify cluster to associate with workunit when copy from remote environment.");
        StringBuffer xml;
        MemoryBuffer dll;
        StringBuffer dllname;
        fetchRemoteWorkunit(context, srcAddress.str(), srcQuerySet.str(), srcQuery.str(), NULL, targetName, xml, dllname, dll);
        deploySharedObject(context, wuid, dllname.str(), cluster, targetName.str(), dll, queryDirectory.str(), xml.str());
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
        targetName.set(query->queryProp("@name"));
    }

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Error opening wuid %s for query %s", wuid.str(), source);

    StringBuffer targetQueryId;
    addQueryToQuerySet(cw, target, targetName.str(), NULL, (WUQueryActivationOptions)req.getActivate(), targetQueryId);
    resp.setQueryId(targetQueryId.str());

    StringArray querysetClusters;
    getQuerySetTargetClusters(target, querysetClusters);
    ForEachItemIn(i, querysetClusters)
        reloadCluster(querysetClusters.item(i), remainingMsWait(req.getWait(), start));

    return true;
}
