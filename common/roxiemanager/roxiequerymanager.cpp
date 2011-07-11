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

#include "jlib.hpp"
#include "roxiemanager.hpp"
#include "roxiequerycompiler.hpp"
#include "roxiewuprocessor.hpp"

#include "roxiecommlibscm.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"


/////////////////////////////////////////////////////////////////////////////////// 
class CRoxieQueryManager : public CInterface, implements IRoxieQueryManager
{
private:
    SocketEndpoint ep;
    unsigned roxieTimeout;
    StringBuffer roxieName;
    StringBuffer workunitDali;
    Owned<IPropertyTree> xml;
    Owned<IRoxieCommunicationClient> queryCommClient;
    StringBuffer user;
    StringBuffer password;
    int logLevel;

private:

    const char *resolveQuerySetName(const char *querySetName)
    {
        return (querySetName && *querySetName) ? querySetName : roxieName.str();
    }

    IConstWorkUnit *getWorkUnit(const char *wuid)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        IConstWorkUnit *workunit = factory->openWorkUnit(wuid, false);
        if (!workunit)
            throw MakeStringException(ROXIEMANAGER_MISSING_ID, "Unknown workunit id %s", wuid);
            
        return workunit;
    }

    void setWUException(IException* e, IWorkUnit *wu, SCMStringBuffer &status, bool &warningsOnly)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        Owned<IWUException> we = wu->createException();

        if (e->errorCode() == ROXIEMANAGER_UNRESOLVED_FILE)
        {
            DBGLOG("WARNING %s", msg.str());
            we->setSeverity(ExceptionSeverityWarning);
        }
        else
        {
            DBGLOG("ERROR %s", msg.str());
            we->setSeverity(ExceptionSeverityError);
            warningsOnly = false;
        }

        status.s.appendf("%s\n", msg.str());

        we->setExceptionMessage(msg);
        StringBuffer s;
        s.append("roxiequerymanager.cpp");
        we->setExceptionSource(s.str());
    }


    IConstWorkUnit *processQuerySetWorkunit(SCMStringBuffer &wuid, 
                                            IRoxieQueryCompileInfo &compileInfo, 
                                            IRoxieQueryProcessingInfo &processingInfo,
                                            SCMStringBuffer &status
                                            )
    {
        Owned<IConstWorkUnit> workunit;
        try
        {
            Owned<IRoxieQueryCompiler> compiler = createRoxieQueryCompiler();
            workunit.setown(compiler->compileEcl(wuid, user, password, compileInfo, processingInfo, status));
            if (!workunit)
            {
                DBGLOG("ERROR compiling query %s", status.str());
                return NULL;
            }
        }
        catch(IException *e)
        {
            // don't try and update a workunit - eclserver already did it
            StringBuffer msg;
            e->errorMessage(msg);
            status.set(msg.str());

            DBGLOG("ERROR compiling query %s", msg.str());
            e->Release();
            
            return NULL;
        }

        Owned<IWorkUnit> wu = &workunit->lock();
        wu->setState(WUStateCompiled);
        return workunit.getClear();
    }

    void processWorkunit(IConstWorkUnit *workunit, SCMStringBuffer &queryName, IRoxieQueryProcessingInfo &processingInfo, WUQueryActivationOptions activateOption, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status, SCMStringBuffer &roxieDeployStatus)
    {
        Owned<IWorkUnit> wu = &workunit->lock();

        SCMStringBuffer jobName;
        wu->getJobName(jobName);

        if (stricmp(jobName.str(),queryName.str())!=0)
            wu->setJobName(queryName.str());

        try
        {
            // look up data file info
            Owned<IRoxieWuProcessor> wuProcessor = createRoxieWuProcessor(roxieName, queryCommClient, logLevel);
            wuProcessor->lookupFileNames(wu, processingInfo, status);
            IPropertyTree *pkgInfo = wuProcessor->queryPackageInfo();
            StringBuffer newQueryId;
            const char *qsName = resolveQuerySetName(querySetName);
            addQueryToQuerySet(wu, qsName, queryName.str(), pkgInfo, activateOption, newQueryId);

            const char *queryComment = processingInfo.queryComment();
            if (queryComment)
                setQueryCommentForNamedQuery(qsName, newQueryId.str(), queryComment);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR updating query list %s", status.str());
            e->Release();
        }

        if (notifyRoxie)
        {
            queryCommClient->sendRoxieReloadControlRequest();
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryManager(SocketEndpoint& _ep, const char *_roxieName, const char *_workunitDali, unsigned _roxieTimeout, const char *_user, const char *_password, int _logLevel)
        :ep(_ep), roxieTimeout(_roxieTimeout), workunitDali(_workunitDali), roxieName(_roxieName), user(_user), password(_password), logLevel(_logLevel)
    {
        queryCommClient.setown(createRoxieCommunicationClient(ep, roxieTimeout));
    }

    void getNewQueryWorkunitId(SCMStringBuffer &wuid, SCMStringBuffer &roxieQueryName, const char *queryAppName)
    {
        Owned<IRoxieQueryCompiler> compiler = createRoxieQueryCompiler();
        Owned<IConstWorkUnit> workunit = compiler->createWorkunit(wuid, user, queryAppName);
        if (!workunit)
        {
            DBGLOG("ERROR creating workunit");
            return;
        }
    }


    bool compileQuery(SCMStringBuffer &wuid, SCMStringBuffer &roxieQueryName, IRoxieQueryCompileInfo &compileInfo, IRoxieQueryProcessingInfo &processingInfo, const char *targetClusterName, SCMStringBuffer &status)
    {
        Owned<IConstWorkUnit> workunit = processQuerySetWorkunit(wuid, compileInfo, processingInfo, status);

        if (workunit)
            return true;
        else
            return false;
    }

    bool deployQuery(SCMStringBuffer &wuid, SCMStringBuffer &roxieQueryName, IRoxieQueryCompileInfo &compileInfo, IRoxieQueryProcessingInfo &processingInfo, const char *userId, WUQueryActivationOptions activateOption, bool allowNewRoxieOnDemandQuery, const char *targetClusterName, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status, SCMStringBuffer &roxieDeployStatus)
    {
        Owned<IConstWorkUnit> workunit = processQuerySetWorkunit(wuid, compileInfo, processingInfo, status);
        if (!workunit)
            return false;

        processWorkunit(workunit, roxieQueryName, processingInfo, activateOption, querySetName, notifyRoxie, status, roxieDeployStatus);
        return true;
    }

    bool deployWorkunit(SCMStringBuffer &wuid,  SCMStringBuffer &roxieQueryName, IRoxieQueryProcessingInfo &processingInfo, const char *userId, WUQueryActivationOptions activateOption, bool allowNewRoxieOnDemandQuery, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status, SCMStringBuffer &roxieDeployStatus)
    {
        if (!wuid.length())
            throw MakeStringException(ROXIEMANAGER_MISSING_ID, "Missing workunit id");

        Owned<IConstWorkUnit> workunit = getWorkUnit(wuid.str());
        if (!roxieQueryName.length()) {
            SCMStringBuffer jobName;
            roxieQueryName.set(workunit->getJobName(jobName).str());
        }

        processWorkunit(workunit, roxieQueryName, processingInfo, activateOption, querySetName, notifyRoxie, status, roxieDeployStatus);
        return true;
    }

    const char *getQuerySetWuId(const char *name, const char *sourceQuerySetName, SCMStringBuffer &querySetWuId)
    {
        Owned<IPropertyTree> tree = getQueryRegistry(sourceQuerySetName, true);
        if (!tree)
        {
            DBGLOG("Could not find query %s in queryset %s", name, sourceQuerySetName);
            return NULL;
        }
        StringBuffer xpath;
        xpath.appendf("Query[@wuid='%s']", name);
        IPropertyTree *query = tree->queryPropTree(xpath.str());
        if (!query)
        {
            StringBuffer id;
            StringBuffer aliasxpath;
            aliasxpath.appendf("Alias[@name='%s']", name);
            IPropertyTree *alias = tree->queryPropTree(aliasxpath.str());
            if (alias)
                id.append(alias->queryProp("@id"));
            else
                id.append(name);
            xpath.clear().appendf("Query[@id='%s']", id.str());
            query = tree->queryPropTree(xpath.str());

            if (!query)
                return false;

            querySetWuId.set(query->queryProp("@wuid"));
        }
        else
            querySetWuId.set(name);

        return querySetWuId.str();
    }

    bool publishFromQuerySet(SCMStringBuffer &name, SCMStringBuffer &roxieQueryName, IRoxieQueryProcessingInfo &processingInfo, const char *userId, WUQueryActivationOptions activateOption, const char *sourceQuerySetName, const char *targetQuerySetName, const char *sourceDaliIP, const char *queryComment, bool notifyRoxie, SCMStringBuffer &status, SCMStringBuffer &roxieDeployStatus)
    {
        if (!name.length())
            throw MakeStringException(ROXIEMANAGER_MISSING_ID, "Missing id");

        SCMStringBuffer wuid;
        getQuerySetWuId(name.str(), sourceQuerySetName, wuid);

        SCMStringBuffer jobName;
        Owned<IConstWorkUnit> workunit;
        try
        {
            workunit.setown(getWorkUnit(wuid.str()));
            roxieQueryName.set(workunit->getJobName(jobName).str());
        }
        catch(IException *e)
        {
            e->Release();
        }

        processWorkunit(workunit, name, processingInfo, activateOption, targetQuerySetName, notifyRoxie, status, roxieDeployStatus);
    
        return true;
    }

    IPropertyTree *retrieveQueryList(const char *filter, bool excludeQueryNames, bool excludeAliasNames, bool excludeLibraryNames, bool excludeDataOnlyNames, unsigned version)
    {
        Owned<IPropertyTree> queryRegistry = getQueryRegistry(roxieName.str(), false);

        StringBuffer xml;
        xml.append("<QueryNames>");

        Owned<IPropertyTreeIterator> queries = queryRegistry->getElements("Query");
        ForEach(*queries)
        {
            IPropertyTree &query = queries->query();
            const char *id = query.queryProp("@id");

            if (!filter || WildMatch(id, filter, true))
            {
                const char *wuid = query.queryProp("@wuid");
                if (!wuid)
                    continue;

                try
                {
                    Owned<IConstWorkUnit> wu = getWorkUnit(wuid);

                    unsigned libraryInterfaceHash = wu->getApplicationValueInt("LibraryModule", "interfaceHash", 0);
                    if (libraryInterfaceHash && excludeLibraryNames)
                        continue;

                    bool isDataOnlyQuery = query.getPropBool("@loadDataOnly");
                    if (isDataOnlyQuery && excludeDataOnlyNames)
                        continue;

                    if (!libraryInterfaceHash && !isDataOnlyQuery && excludeQueryNames)
                        continue;

                    StringBuffer err;
                    xml.appendf(" <Query id='%s'", id);

                    xml.appendf(" wuid='%s'", wuid);
                    Owned<IConstWUQuery> q = wu->getQuery();
            
                    Owned<IConstWUAssociatedFile> associatedFile = q->getAssociatedFile(FileTypeDll, 0);
                    if (associatedFile)
                    {
                        SCMStringBuffer associateFileName;
                        associatedFile->getName(associateFileName);
                        xml.appendf(" associatedName='%s'", associateFileName.str());
                    }
                                    
                    SCMStringBuffer label;
                    wu->getSnapshot(label);
                    if (label.length())
                        xml.appendf(" label ='%s'", label.str());

                    SCMStringBuffer user;
                    wu->getDebugValue("created_for", user);
                    if (user.length())
                        xml.appendf(" deployedBy='%s'", user.str());

                    SCMStringBuffer comment;
                    wu->getDebugValue("comment", comment);
                    if (comment.length())
                        xml.appendf(" comment='%s'", comment.str());

                    xml.appendf(" priority='%d'", wu->getDebugValueInt("querypriority", 0));

                    if (libraryInterfaceHash != 0)
                    {
                        xml.appendf(" isLibrary='1'");
                        xml.appendf(" libraryInterfaceHash='%u'", libraryInterfaceHash);
                    }

                    // Must be from the queryset
                    if (query.getPropBool("@suspended", false))
                    {
                        const char *user = query.queryProp("@updatedByUserId");
                        xml.appendf(" suspendedBy='%s'", (user) ? user : "Roxie");
                        xml.append(" suspended='1'");
                    }
                    err.append(query.queryProp("@error"));
                    if (err.length())
                    {
                        StringBuffer temp;
                        encodeXML(err.str(), temp);
                        xml.appendf(" error='%s'", temp.str());
                    }

                    if (isDataOnlyQuery)
                        xml.appendf(" isLoadDataOnly='1'");

                    Owned<IPropertyTreeIterator> libraries = query.getElements("Library");
                    xml.append(">\n");
                    xml.append(" <librariesUsed>");
                    ForEach(*libraries)
                    {
                        IPropertyTree &library = libraries->query();
                        const char *libName = library.queryProp("@name");
                        xml.appendf("<library>%s</library>", libName);
                    }
                    xml.append("</librariesUsed>\n");
                    xml.append("</Query>\n");
                }
                catch(IException *e)
                {
                    StringBuffer err;
                    e->errorMessage(err);
                    xml.appendf(" <Query id='%s'", id);
                    xml.appendf(" wuid='%s'", wuid);
                    xml.appendf(" error='%s'", err.str());
                    xml.appendf(" comment='%s'", err.str());
                    xml.append(">\n");
                    xml.append("</Query>\n");
                    e->Release();
                }
            }
        }
        if (!excludeAliasNames)
        {
            Owned<IPropertyTreeIterator> aliases = queryRegistry->getElements("Alias");
            ForEach(*aliases)
            {
                IPropertyTree &alias = aliases->query();
                const char *id = alias.queryProp("@id");
                const char *name = alias.queryProp("@name");
                if (!filter || WildMatch(name, filter, true) || WildMatch(id, filter, true))
                    xml.appendf(" <Alias id='%s' name='%s'/>\n", id, name);
            }
        }
        xml.append("</QueryNames>");

        Owned<IPropertyTree> tree = createPTreeFromXMLString(xml, false);
        return tree.getClear();
    }

    void addAlias(const char *alias, const char *queryId, const char *querySetName, bool notifyRoxie, SCMStringBuffer &oldActive)
    {
        try
        {
            addQuerySetAlias(resolveQuerySetName(querySetName), alias, queryId);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            DBGLOG("ERROR addingAlias %d %s", errCode, err.str());
            e->Release();
        }

        if (notifyRoxie)
            queryCommClient->sendRoxieReloadControlRequest();
    }

    void suspendQuery(const char *id, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status)
    {
        try
        {
            setSuspendQuerySetQuery(resolveQuerySetName(querySetName), id, true);
            status.s.appendf("successfully suspended query %s", id);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR suspending query %s", status.str());
            e->Release();
        }
        if (notifyRoxie)
            queryCommClient->sendRoxieReloadControlRequest();
    }

    void unsuspendQuery(const char *id, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status)
    {
        try
        {
            setSuspendQuerySetQuery(resolveQuerySetName(querySetName), id, false);
            status.s.appendf("successfully unsuspended query %s", id);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR unsuspending query %s", status.str());
            e->Release();
        }
        if (notifyRoxie)
            queryCommClient->sendRoxieReloadControlRequest();
    }

    void deleteQuery(const char *id, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status)
    {
        try
        {
            deleteQuerySetQuery(resolveQuerySetName(querySetName), id);
            status.s.appendf("successfully deleted:\n query %s", id);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR deleting query %s", status.str());
            e->Release();
        }
        if (notifyRoxie)
            queryCommClient->sendRoxieReloadControlRequest();

    }

    void removeAllAliasForQuery(const char *queryId, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status)
    {
        try
        {
            removeQuerySetAliasesFromNamedQuery(resolveQuerySetName(querySetName), queryId);
            status.s.appendf("successfully removed all aliases for query %s", queryId);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR deleting query %s", status.str());
            e->Release();
        }

        if (notifyRoxie)
                queryCommClient->sendRoxieReloadControlRequest();
    }

    void removeAlias(const char *alias, const char *querySetName, bool notifyRoxie, SCMStringBuffer &status)
    {
        try
        {
            removeQuerySetAlias(resolveQuerySetName(querySetName), alias);
            status.s.appendf("successfully removed alias %s", alias);
        }
        catch(IException *e)
        {
            int errCode = e->errorCode();
            StringBuffer err;
            e->errorMessage(err);
            status.s.appendf("%d %s", errCode, err.str());
            DBGLOG("ERROR removing alias %s", status.str());
            e->Release();
        }

        if (notifyRoxie)
            queryCommClient->sendRoxieReloadControlRequest();
    }

    const char * runQuery(IConstWorkUnit *workunit, const char *roxieQueryName, bool outputToSocket, bool allowNewRoxieOnDemandQuery, SCMStringBuffer &response)
    {
        StringBuffer query;
        SCMStringBuffer wuid;
        query.appendf("<%s wuid='%s' roxieOnDemand='%d' outputToSocket='%d'/>", roxieQueryName, workunit->getWuid(wuid).str(), (int) allowNewRoxieOnDemandQuery, (int) outputToSocket);
        try
        {
            queryCommClient->sendRoxieOnDemandRequest(query.str(), response, false);
        }
        catch (IException *E)
        {
            Owned<IWorkUnit> wu = &workunit->lock();
            Owned<IWUException> we = wu->createException();
            we->setExceptionCode(E->errorCode());
            StringBuffer msg;
            we->setExceptionMessage(E->errorMessage(msg).str());
            we->setExceptionSource("Roxie");
            we->setSeverity(ExceptionSeverityError);
            wu->setState(WUStateFailed);
            throw;
        }
        return response.str();
    }


    void setQueryWarningTime(const char *id, unsigned warnTime, SCMStringBuffer &status)
    {
    }

    unsigned getQueryWarningTime(const char *id)
    {
        return 0;
    }

    bool updateACLInfo(bool allow, const char *restrict_ip, const char *mask, const char *query, const char *errorMsg, int errorCode, int port, SCMStringBuffer &status)
    {
        if (port == 0)   // use the default
            queryCommClient->updateACLInfo(allow, restrict_ip, mask, query, errorMsg, errorCode, NULL, status);
        else if (port == -1)  // use all ports
        {
            IntArray portNumbers;  // store unique ports
            Owned<IPropertyTree> topology = queryCommClient->retrieveTopology();

            Owned<IPropertyTreeIterator> servers = topology->getElements("Endpoint/RoxieTopology/RoxieServerProcess");
            ForEach (*servers)
            {
                IPropertyTree &server = servers->query();
                int serverPort = server.getPropInt("@port");
                const char *netAddress = server.queryProp("@netAddress");

                portNumbers.append(port);
                StringBuffer roxieAddress;
                roxieAddress.appendf("%s:%d", netAddress, serverPort);
                queryCommClient->updateACLInfo(allow, restrict_ip, mask, query, errorMsg, errorCode, roxieAddress.str(), status);
            }       
        }
        else  // use only the specific port passed in
        {
            Owned<IPropertyTree> topology = queryCommClient->retrieveTopology();
            StringBuffer xpath;
            xpath.appendf("Endpoint/RoxieTopology/RoxieServerProcess[@port='%d']", port);
            bool foundIt = false;

            Owned<IPropertyTreeIterator> servers = topology->getElements(xpath.str());
            ForEach (*servers)
            {
                IPropertyTree &server = servers->query();
                int serverPort = server.getPropInt("@port");
                const char *netAddress = server.queryProp("@netAddress");

                StringBuffer roxieAddress;
                roxieAddress.appendf("%s:%d", netAddress, serverPort);
                queryCommClient->updateACLInfo(allow, restrict_ip, mask, query, errorMsg, errorCode, roxieAddress.str(), status);
                foundIt = true;
                break;  // only need to send 1
            }

            if (!foundIt)
                status.s.appendf("Could not find port %d in the roxie configuration", port);
        }


        return true;
    }

    IRoxieCommunicationClient * queryRoxieCommunicationClient() { return queryCommClient; }

};


IRoxieQueryManager* createRoxieQueryManager(SocketEndpoint &roxieEP, const char *roxieName, const char *workunitDali, unsigned roxieTimeout, const char *_user, const char *_password, int logLevel)
{
    return new CRoxieQueryManager(roxieEP, roxieName, workunitDali, roxieTimeout, _user, _password, logLevel);
}

