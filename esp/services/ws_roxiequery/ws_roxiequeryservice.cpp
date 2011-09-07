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

#include <math.h>
#include "jfile.hpp"
#include "wshelpers.hpp"
#include "portlist.h"
#include "jiface.hpp"
#include "environment.hpp"
#include "TpWrapper.hpp"

#include "jstring.hpp"

#include "dautils.hpp"
#include "eclrtl.hpp"
#include "fverror.hpp"
#include "exception_util.hpp"

#include "ws_roxiequeryservice.hpp"

const unsigned int  ROXIEQUERYID                    = 1;
const unsigned int  ROXIEQUERYWUID                  = 2;
const unsigned int  ROXIEQUERYCLUSTER               = 3;
const unsigned int  ROXIEQUERYLABEL             = 4;
const unsigned int  ROXIEQUERYSUSPENDED         = 5;
const unsigned int  ROXIEQUERYHIGHPRIORITY      = 6;
const unsigned int  ROXIEQUERYERROR             = 7;
const unsigned int  ROXIEQUERYCOMMENT               = 8;
const unsigned int  ROXIEQUERYASSOCIATEDNAME    = 9;
const unsigned int  ROXIEQUERYHASALIASES            = 10;
const unsigned int  ROXIEQUERYDEPLOYEDBY            = 11;
const unsigned int  ROXIEQUERYUPDATEDBY         = 12;
const unsigned int  ROXIEMANAGER_TIMEOUT        = 60000;

static const char* FEATURE_URL="RoxieQueryAccess";
static const char* ROXIE_CLUSTER="RoxieCluster";
static const char* ROXIE_FARMERPROCESS1="RoxieServerProcess[1]";

__int64 findPositionInRoxieQueryList(int type, const char *value, bool descend, IArrayOf<IEspRoxieQuery>& queries)
{
    if (!value || (strlen(value) < 1))
    {
        if (descend)
            return -1;
        else
            return 0;
    }

    __int64 addToPos = -1;
    ForEachItemIn(i, queries)
    {
        IEspRoxieQuery& query = queries.item(i);
        char *Value = NULL;
        switch (type)
        {
        case ROXIEQUERYID:
            Value = (char *) query.getID();
            break;
        case ROXIEQUERYDEPLOYEDBY:
            Value = (char *) query.getDeployedBy();
            break;
        case ROXIEQUERYUPDATEDBY:
            Value = (char *) query.getUpdatedBy();
            break;
        case ROXIEQUERYWUID:
            Value = (char *) query.getWUID();
            break;
        case ROXIEQUERYSUSPENDED:
            Value = (char *) query.getSuspended();
            break;
        case ROXIEQUERYHIGHPRIORITY:
            Value = (char *) query.getHighPriority();
            break;
        case ROXIEQUERYERROR:
            Value = (char *) query.getError();
            break;
        case ROXIEQUERYCOMMENT:
            Value = (char *) query.getComment();
            break;
        case ROXIEQUERYHASALIASES:
            Value = (char *) query.getHasAliases();
            break;
        }

        if (!Value)
            continue;

        if (type != ROXIEQUERYID)
        {
            if (descend && strcmp(value, Value)>0)
            {
                addToPos = i;
                break;
            }
            if (!descend && strcmp(value, Value)<0)
            {
                addToPos = i;
                break;
            }
        }
        else
        {
            if (descend && stricmp(value, Value)>0)
            {
                addToPos = i;
                break;
            }
            if (!descend && stricmp(value, Value)<0)
            {
                addToPos = i;
                break;
            }
        }
    }

    return addToPos;
}

void getUserInformation(IEspContext &context, StringBuffer &username, StringBuffer &password)
{
    context.getUserID(username);
    context.getPassword(password);
}

void CWsRoxieQueryEx::init(IPropertyTree *cfg, const char *process, const char *service)
{

    IPropertyTree* pStyleSheets = cfg->queryPropTree("StyleSheets");
    const char* xslt = cfg->queryProp("xslt[@name='atts']");
    m_attsXSLT.append(getCFD()).append( xslt && *xslt ? xslt : "smc_xslt/atts.xslt");

    xslt = cfg->queryProp("xslt[@name='graphStats']");
    m_graphStatsXSLT.append(getCFD()).append( xslt && *xslt ? xslt : "smc_xslt/graphStats.xslt");
}

void CWsRoxieQueryEx::addToQueryString(StringBuffer &queryString, const char *name, const char *value)
{
    if (queryString.length() > 0)
    {
        queryString.append("&amp;");
    }

    queryString.append(name);
    queryString.append("=");
    queryString.append(value);
}

void CWsRoxieQueryEx::addToQueryStringFromInt(StringBuffer &queryString, const char *name, int value)
{
    if (queryString.length() > 0)
    {
        queryString.append("&amp;");
    }

    queryString.append(name);
    queryString.append("=");
    queryString.append(value);
}

void CWsRoxieQueryEx::getClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port)
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
        throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "Failed to get environment information for the cluster '%s %s'.", clusterType, clusterName);
    xpath.clear().append(processName);
    xpath.append("@computer");
    const char* computer = pCluster->queryProp(xpath.str());
    if (!computer || strlen(computer) < 1)
        throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "Failed to get the 'computer' information for the process '%s'. Please check environment settings for the cluster '%s %s'.", processName, clusterType, clusterName);

    xpath.clear().append(processName);
    xpath.append("@port");
    const char* portStr = pCluster->queryProp(xpath.str());
    port = ROXIE_SERVER_PORT;
    if (portStr && *portStr)
    {
        port = atoi(portStr);
    }

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
    }
    
    return;
}

bool CWsRoxieQueryEx::onRoxieQuerySearch(IEspContext &context, IEspRoxieQuerySearchRequest & req, IEspRoxieQuerySearchResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to Search Roxie Queries. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsRoxieQueryEx::onRoxieQuerySearch User=%s",username.str());

        CTpWrapper dummy;
        IArrayOf<IEspTpCluster> clusters;
        dummy.getClusterProcessList(eqRoxieCluster, clusters);

        if (clusters.length() <= 0)
            throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "Roxie cluster not found.");

        StringArray roxieclusters;
        ForEachItemIn(k, clusters)
        {
            IEspTpCluster& cluster = clusters.item(k);
            roxieclusters.append(cluster.getName());
        }

        StringArray ftarray;
        ftarray.append("Suspended and Non-Suspended");
        ftarray.append("Non-Suspended Only");
        ftarray.append("Suspended Only");

        resp.setClusterNames(roxieclusters);
        resp.setSuspendedSelections(ftarray);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsRoxieQueryEx::getAllRoxieQueries(IEspContext &context, const char* cluster, const char* suspended, const char* sortBy, bool descending, __int64 displayEnd, IArrayOf<IEspRoxieQuery>& RoxieQueryList, __int64& totalFiles)
{
    int port;
    StringBuffer netAddress;
    SocketEndpoint ep;
    getClusterConfig(ROXIE_CLUSTER, cluster, ROXIE_FARMERPROCESS1, netAddress, port);

    int suspendedFlag = 0;
    if (suspended && *suspended)
    {
        if (stricmp(suspended, "Suspended Only") == 0)
        {
            suspendedFlag = 2;
        }
        else if (stricmp(suspended, "Non-Suspended Only") == 0)
        {
            suspendedFlag = 1;
        }
    }

    ep.set(netAddress.str(), port);

    StringBuffer username, password;
    getUserInformation(context, username, password);

    StringBuffer currentDaliIp;
    const SocketEndpoint &ep1 = queryCoven().queryComm().queryGroup().queryNode(0).endpoint();
    ep1.getUrlStr(currentDaliIp);

    LogLevel loglevel = getEspLogLevel(&context);
    Owned<IRoxieQueryManager> queryManager = createRoxieQueryManager(ep, cluster, currentDaliIp, ROXIEMANAGER_TIMEOUT, username.str(), password.str(), loglevel);
    Owned<IPropertyTree> result = queryManager->retrieveQueryList("*", false, false, false, false, 0);

    Owned<IPropertyTreeIterator> queries = result->getElements("Query");
    ForEach(*queries)
    {
        IPropertyTree &query = queries->query();
        const char* id = query.queryProp("@id");
        const char* wuid = query.queryProp("@wuid");
        const char* deployedby = query.queryProp("@deployedBy");
        const char* updatedby = query.queryProp("@suspendedBy");
        const char* comment = query.queryProp("@comment");
        const char* error = query.queryProp("@error");
        bool bSuspended = query.getPropBool("@suspended", false);
        bool bHighPriority = query.getPropBool("@priority", false);
        bool bHasAliases = query.getPropBool("@hasAlias", false);

        if (suspendedFlag > 1)
        {
            if (!bSuspended) //Suspended Only
                continue;
        }
        else    if (suspendedFlag > 0)
        {
            if (bSuspended) //Non-Suspended Only
                continue;
        }

        StringBuffer strSuspended, strHighPriority, strHasAliases;
        if (bSuspended)
            strSuspended.append("Yes");
        else
            strSuspended.append("No");
        if (bHighPriority)
            strHighPriority.append("Yes");
        else
            strHighPriority.append("No");
        if (bHasAliases)
            strHasAliases.append("Yes");
        else
            strHasAliases.append("No");

        Owned<IEspRoxieQuery> roxieQuery = createRoxieQuery("","");
        if (id && *id)
            roxieQuery->setID(id);
        if (comment && *comment)
            roxieQuery->setComment(comment);
        if (error && *error)
            roxieQuery->setError("Yes");
        else
            roxieQuery->setError("No");

        roxieQuery->setHasAliases(strHasAliases.str());
        roxieQuery->setSuspended(strSuspended.str());
        roxieQuery->setHighPriority(strHighPriority.str());

        double version = context.getClientVersion();
        if (version > 1.00)
        {
            if (deployedby && *deployedby)
                roxieQuery->setDeployedBy(deployedby);
            if (updatedby && *updatedby)
                roxieQuery->setUpdatedBy(updatedby);
        }
        if (version > 1.01)
        {
            if (wuid && *wuid)
                roxieQuery->setWUID(wuid);
        }

        __int64 addToPos = -1; //Add to tail
        if (!sortBy)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYID, id, false, RoxieQueryList);
        }
        else if (stricmp(sortBy, "ID")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYID, id, descending, RoxieQueryList);
        }
        else if (stricmp(sortBy, "DeployedBy")==0)
        {
            if (version > 1.00)
            {
                addToPos = findPositionInRoxieQueryList(ROXIEQUERYDEPLOYEDBY, deployedby, descending, RoxieQueryList);
            }
        }
        else if (stricmp(sortBy, "UpdatedBy")==0)
        {
            if (version > 1.00)
            {
                addToPos = findPositionInRoxieQueryList(ROXIEQUERYUPDATEDBY, updatedby, descending, RoxieQueryList);
            }
        }
        else if (stricmp(sortBy, "WUID")==0)
        {
            if (version > 1.01)
            {
                addToPos = findPositionInRoxieQueryList(ROXIEQUERYWUID, wuid, descending, RoxieQueryList);
            }
        }
        else if (stricmp(sortBy, "Suspended")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYSUSPENDED, strSuspended.str(), descending, RoxieQueryList);
        }
        else if (stricmp(sortBy, "HighPriority")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYHIGHPRIORITY, strHighPriority.str(), descending, RoxieQueryList);
        }
        else if (stricmp(sortBy, "Error")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYERROR, error, descending, RoxieQueryList);
        }
        else if (stricmp(sortBy, "Comment")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYCOMMENT, comment, descending, RoxieQueryList);
        }
        else if (stricmp(sortBy, "HasAliases")==0)
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYHASALIASES, strHighPriority.str(), descending, RoxieQueryList);
        }
        else
        {
            addToPos = findPositionInRoxieQueryList(ROXIEQUERYID, id, false, RoxieQueryList);
        }

        totalFiles++;

        if (addToPos < 0)
            RoxieQueryList.append(*roxieQuery.getClear());
        else
            RoxieQueryList.add(*roxieQuery.getClear(), (int) addToPos);
    }

    return true;
}

bool CWsRoxieQueryEx::onRoxieQueryList(IEspContext &context, IEspRoxieQueryListRequest & req, IEspRoxieQueryListResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to List Roxie Queries. Permission denied.");

        StringBuffer username;
        context.getUserID(username);
        DBGLOG("CWsRoxieQueryEx::onRoxieQueryList User=%s",username.str());

        const char* sortBy = req.getSortby();
        bool descending = req.getDescending();

        unsigned pagesize = req.getPageSize();
        if (pagesize < 1)
        {
            pagesize = 100;
        }

        __int64 displayStartReq = 1;
        if (req.getPageStartFrom() > 0)
            displayStartReq = req.getPageStartFrom();

        __int64 displayStart = displayStartReq - 1;
        __int64 displayEnd = displayStart + pagesize;

        StringBuffer size;
        __int64 totalFiles = 0;
        IArrayOf<IEspRoxieQuery> RoxieQueryList;

        const char* name = req.getLogicalName();
        const char* cluster = req.getCluster();

        if (!cluster || !*cluster)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Cluster is not specified for retrieving Roxie queries.");

        getAllRoxieQueries(context, cluster, req.getSuspended(), sortBy, descending, displayEnd, RoxieQueryList, totalFiles);

        resp.setNumFiles(totalFiles);
        resp.setPageSize(pagesize);
        resp.setPageStartFrom(displayStart+1);
        resp.setPageEndAt(displayEnd);

        if (displayStart - pagesize > 0)
            resp.setPrevPageFrom(displayStart - pagesize + 1);
        else if(displayStart > 0)
            resp.setPrevPageFrom(1);

        if(displayEnd < totalFiles)
        {
            resp.setNextPageFrom(displayEnd+1);
            resp.setLastPageFrom(pagesize * (int) floor((double) totalFiles/pagesize) + 1);
        }

        
        StringBuffer ParametersForSorting;
        addToQueryString(ParametersForSorting, "Cluster", cluster);
        resp.setCluster(cluster);

        if (name && *name)
        {
            addToQueryString(ParametersForSorting, "LogicalName", name);
            resp.setLogicalName(name);
        }
        if (req.getSuspended() && *req.getSuspended())
            addToQueryString(ParametersForSorting, "Suspended", req.getSuspended());

        StringBuffer ParametersForPaging;
        addToQueryStringFromInt(ParametersForPaging, "PageSize", pagesize);

        descending = false;
        if (sortBy && *sortBy)
        {
            if (req.getDescending())
                descending = req.getDescending();

            resp.setSortby(sortBy);
            resp.setDescending(descending);

            addToQueryString(ParametersForPaging, "Sortby", sortBy);
            if (descending)
            {
                addToQueryString(ParametersForPaging, "Descending", "1");
            }
        }

        if (ParametersForSorting.length() > 0)
            resp.setParametersForSorting(ParametersForSorting.str());
        if (ParametersForPaging.length() > 0)
            resp.setParametersForPaging(ParametersForPaging.str());

        resp.setRoxieQueries(RoxieQueryList);
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsRoxieQueryEx::onQueryDetails(IEspContext &context, IEspRoxieQueryDetailsRequest & req, IEspRoxieQueryDetailsResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_ROXIE_QUERY_ACCESS_DENIED, "Failed to get Query Details. Permission denied.");

        StringBuffer username, password;
        getUserInformation(context, username, password);
        DBGLOG("CWsRoxieQueryEx::onQueryDetails User=%s",username.str());

        const char* queryID = req.getQueryID();
        const char* cluster = req.getCluster();
        if (!queryID || !*queryID || !cluster || !*cluster)
        {
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Query ID or Cluster is not specified for retrieving query information.");
        }

        int port;
        StringBuffer netAddress;
        SocketEndpoint ep;
        getClusterConfig(ROXIE_CLUSTER, cluster, ROXIE_FARMERPROCESS1, netAddress, port);

        ep.set(netAddress.str(), port);

        StringBuffer currentDaliIp;
        const SocketEndpoint &ep1 = queryCoven().queryComm().queryGroup().queryNode(0).endpoint();
        ep1.getUrlStr(currentDaliIp);

        LogLevel loglevel = getEspLogLevel(&context);
        Owned<IRoxieQueryManager> queryManager = createRoxieQueryManager(ep, cluster, currentDaliIp, ROXIEMANAGER_TIMEOUT, username.str(), password.str(), loglevel);
        Owned<IPropertyTree> result = queryManager->retrieveQueryList(queryID, false, false, false, false, 0);

        if (result)
        {
            Owned<IPropertyTreeIterator> queries = result->getElements("Query");
            ForEach(*queries)
            {
                IPropertyTree &query = queries->query();
                const char* wuid = query.queryProp("@wuid");
                const char* label = query.queryProp("@label");
                const char* associatedName = query.queryProp("@associatedName");
                const char* comment = query.queryProp("@comment");
                const char* error = query.queryProp("@error");
                const char* deployedby = query.queryProp("@deployedBy");
                const char* updatedby = query.queryProp("@suspendedBy");
                bool bSuspended = query.getPropBool("@suspended", false);
                bool bHighPriority = query.getPropBool("@priority", false);
                StringBuffer strSuspended, strHighPriority;
                if (bSuspended)
                    resp.setSuspended("Yes");
                else
                    resp.setSuspended("No");
                if (bHighPriority)
                    resp.setHighPriority("Yes");
                else
                    resp.setHighPriority("No");

                resp.setQueryID(queryID);
                resp.setCluster(cluster);
                if (wuid && *wuid)
                    resp.setWUID(wuid);
                if (associatedName && *associatedName)
                    resp.setAssociatedName(associatedName);
                if (label && *label)
                    resp.setLabel(label);
                if (error && *error)
                    resp.setError(error);
                if (comment && *comment)
                    resp.setComment(comment);
                    
                double version = context.getClientVersion();
                if (version > 1.00)
                {
                    if (deployedby && *deployedby)
                        resp.setDeployedBy(deployedby);
                    if (updatedby && *updatedby)
                        resp.setUpdatedBy(updatedby);
                }
                break;
            }

            IArrayOf<IEspRoxieQueryAlias> AliasList;
            Owned<IPropertyTreeIterator> queries1 = result->getElements("Alias");
            ForEach(*queries1)
            {
                IPropertyTree &query = queries1->query();
                const char* id = query.queryProp("@id");
                const char* original = query.queryProp("@original");

                Owned<IEspRoxieQueryAlias> alias = createRoxieQueryAlias("","");

                if (id && *id)
                    alias->setID(id);
                if (original && *original)
                    alias->setOriginal(original);

                AliasList.append(*alias.getClear());
            }

            resp.setAliases(AliasList);
        }
    }
    catch(IException* e)
    {   
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsRoxieQueryEx::onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp)
{
    resp.setCluster(req.getCluster());
    resp.setName(req.getName());
    resp.setGraphName(req.getGraphName());
    resp.setGraphType("roxiequery");
    return true;
}
bool CWsRoxieQueryEx::onShowGVCGraph(IEspContext &context, IEspShowGVCGraphRequest &req, IEspShowGVCGraphResponse &resp)
    {
        const char *queryName = req.getQueryId();
        if (!queryName || !*queryName)
            throw MakeStringException(0, "Missing Roxie query name!");

        const char* cluster = req.getCluster();

        int port;
        StringBuffer netAddress;
        SocketEndpoint ep;
        getClusterConfig(ROXIE_CLUSTER, cluster, ROXIE_FARMERPROCESS1, netAddress, port);
        ep.set(netAddress.str(), port);

        ep.set("10.239.219.15", port); // jo remove

        StringArray graphNames;
        enumerateGraphs(queryName, ep, graphNames);

        char graphName[256];
        if (req.getGraphName() && *req.getGraphName())
        {
            strcpy(graphName, req.getGraphName());
        }
        else if (graphNames.length() > 0)
        {
            StringBuffer graphName0 = graphNames.item(0);
            strcpy(graphName, graphName0.str());
        }
        else
        {
            throw MakeStringException(0, "Graph not found!");
        }

        Owned<IPropertyTree> pXgmmlGraph = getXgmmlGraph(queryName, ep, graphName);

        StringBuffer xml;
        toXML(pXgmmlGraph, xml);

        if (xml.length() > 0)
        {
            StringBuffer str;
            encodeUtf8XML(xml.str(), str, 0);

            StringBuffer xml1;
            xml1.append("<Control>");
            xml1.append("<Endpoint>");
            xml1.append("<Query id=\"Gordon.Extractor.0\">");
            xml1.appendf("<Graph id=\"%s\">", graphName);
            xml1.append("<xgmml>");
            xml1.append(str);
            xml1.append("</xgmml>");
            xml1.append("</Graph>");
            xml1.append("</Query>");
            xml1.append("</Endpoint>");
            xml1.append("</Control>");

            resp.setTheGraph(xml1);
        }

        if (graphName && *graphName)
            resp.setGraphName( graphName );
        resp.setQueryId( queryName );
        resp.setGraphNames( graphNames );
        return true;
    }

void CWsRoxieQueryEx::enumerateGraphs(const char *queryName, SocketEndpoint &ep, StringArray& graphNames)
{

    try
    {

        Owned<IRoxieCommunicationClient> roxieClient = createRoxieCommunicationClient(ep, ROXIEMANAGER_TIMEOUT);
        Owned<IPropertyTree> result = roxieClient->retrieveQueryStats(queryName, "listGraphNames", NULL, false);
        IPropertyTree* rootTree = result->queryPropTree("Endpoint/Query");

        if (rootTree)
        {
            Owned<IPropertyTreeIterator> graphit = rootTree->getElements("Graph");
            ForEach(*graphit)
            {
                const char* graphName = graphit->query().queryProp("@id");
                graphNames.append( graphName );
            }
        }
    }
    catch (IException *e)
    {
        StringBuffer errmsg;
        ERRLOG("Exception when enumerating graphs for %s: %s", queryName, e->errorMessage(errmsg).str());
        throw e;
    }
    catch(...)
    {
        ERRLOG("Unknown exception when enumerating graphs for %s", queryName);
        throw MakeStringException(-1, "Unknown exception while enumerating graphs for %s", queryName);
    }
}

IPropertyTree* CWsRoxieQueryEx::getXgmmlGraph(const char *queryName, SocketEndpoint &ep, const char* graphName)
{

    try
    {
        Owned<IRoxieCommunicationClient> roxieClient = createRoxieCommunicationClient(ep, ROXIEMANAGER_TIMEOUT);
        Owned<IPropertyTree> result = roxieClient->retrieveQueryStats(queryName, "selectGraph", graphName, true);

        StringBuffer xpath;
        xpath   << "Endpoint/Query/Graph[@id='" 
                << graphName
                << "']/xgmml/graph";
        IPropertyTree* pXgmmlGraph = result->queryPropTree(xpath.str());
        if (!pXgmmlGraph)
            throw MakeStringException(-1, "Graph not found!");
        return LINK( pXgmmlGraph );
    }
    catch (IException *e)
    {
        StringBuffer errmsg;
        ERRLOG("Exception when fetching graph %s for %s: %s", graphName, queryName, e->errorMessage(errmsg).str());
        throw e;
    }
    catch(...)
    {
        ERRLOG("Unknown exception when fetching graph %s for %s", graphName, queryName);
        throw MakeStringException(-1, "Unknown exception while generating graph %s for %s", graphName, queryName);
    }
}

bool CWsRoxieQueryEx::onRoxieQueryProcessGraph(IEspContext &context, IEspRoxieQueryProcessGraphRequest &req, IEspRoxieQueryProcessGraphResponse &resp)
{
    const char* cluster = req.getCluster();
    const char* graphName = req.getGraphName();
    const char *queryName = req.getQueryId();
    if (!queryName || !*queryName)
        throw MakeStringException(0, "Missing Roxie query name!");

    try
    {
        int port;
        StringBuffer netAddress;
        SocketEndpoint ep;
        getClusterConfig(ROXIE_CLUSTER, cluster, ROXIE_FARMERPROCESS1, netAddress, port);
        ep.set(netAddress.str(), port);
        ep.set("10.239.219.15", port); // jo remove
        Owned<IRoxieCommunicationClient> roxieClient = createRoxieCommunicationClient(ep, ROXIEMANAGER_TIMEOUT);

        Owned<IPropertyTree> xgmml = getXgmmlGraph(queryName, ep, graphName);

        StringBuffer x;

        toXML(xgmml.get(), x);
        resp.setTheGraph(x.str());
    }
    catch (IException *e)
    {
        StringBuffer errmsg;
        ERRLOG("Exception when fetching graph stats %s for %s: %s", graphName, queryName, e->errorMessage(errmsg).str());
        throw e;
    }
    catch(...)
    {
        ERRLOG("Unknown exception when fetching graph stats %s for %s", graphName, queryName);
        throw MakeStringException(-1, "Unknown exception while generating graph %s for %s", graphName, queryName);
    }
    return true;
}
