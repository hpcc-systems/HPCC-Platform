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

#include "jlib.hpp"
#include "ws_workunitsHelpers.hpp"
#include "exception_util.hpp"

#include "daclient.hpp"
#include "dalienv.hpp"
#include "daaudit.hpp"
#include "portlist.h"
#include "dadfs.hpp"
#include "fileview.hpp"
#include "wuwebview.hpp"
#include "dllserver.hpp"
#include "wujobq.hpp"
#include "hqlexpr.hpp"

#ifndef _NO_LDAP
#include "ldapsecurity.ipp"
#endif

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

namespace ws_workunits {

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, const char *owner, SecAccessFlags accessOwn, SecAccessFlags accessOthers)
{
    return (isEmpty(owner) || (user && streq(user, owner))) ? accessOwn : accessOthers;
}

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, IConstWorkUnitInfo& cw, SecAccessFlags accessOwn, SecAccessFlags accessOthers)
{
    return chooseWuAccessFlagsByOwnership(user, cw.queryUser(), accessOwn, accessOthers);
}

const char *getWuAccessType(const char *owner, const char *user)
{
    return (isEmpty(owner) || (user && streq(user, owner))) ? OWN_WU_ACCESS : OTHERS_WU_ACCESS;
}

const char *getWuAccessType(IConstWorkUnit& cw, const char *user)
{
    return getWuAccessType(cw.queryUser(), user);
}

void getUserWuAccessFlags(IEspContext& context, SecAccessFlags& accessOwn, SecAccessFlags& accessOthers, bool except)
{
    if (!context.authorizeFeature(OWN_WU_ACCESS, accessOwn))
        accessOwn = SecAccess_None;

    if (!context.authorizeFeature(OTHERS_WU_ACCESS, accessOthers))
        accessOthers = SecAccess_None;

    if (except && (accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
    {
        AuditSystemAccess(context.queryUserId(), false, "Access Denied: User can't view any workunits");
        VStringBuffer msg("Access Denied: User %s does not have rights to access workunits.", context.queryUserId());
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
    }
}

SecAccessFlags getWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw)
{
    SecAccessFlags accessFlag = SecAccess_None;
    cxt.authorizeFeature(getWuAccessType(cw, cxt.queryUserId()), accessFlag);
    return accessFlag;
}

void ensureWsWorkunitAccessByOwnerId(IEspContext& cxt, const char* owner, SecAccessFlags minAccess)
{
    if (!cxt.validateFeatureAccess(getWuAccessType(owner, cxt.queryUserId()), minAccess, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
}

void ensureWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw, SecAccessFlags minAccess)
{
    if (!cxt.validateFeatureAccess(getWuAccessType(cw, cxt.queryUserId()), minAccess, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
}

void ensureWsWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess)
{
    Owned<IWorkUnitFactory> wf = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = wf->openWorkUnit(wuid);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Failed to open workunit %s when ensuring workunit access", wuid);
    ensureWsWorkunitAccess(context, *cw, minAccess);
}

void ensureWsCreateWorkunitAccess(IEspContext& cxt)
{
    if (!cxt.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");
}

StringBuffer &getWuidFromLogicalFileName(IEspContext &context, const char *logicalName, StringBuffer &wuid)
{
    Owned<IUserDescriptor> userdesc = createUserDescriptor();
    userdesc->set(context.queryUserId(), context.queryPassword(), context.querySessionToken(), context.querySignature());
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
    if (!df)
        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find file %s.",logicalName);
    return wuid.append(df->queryAttributes().queryProp("@workunit"));
}

void formatDuration(StringBuffer &s, unsigned ms)
{
    unsigned days = ms / (1000*60*60*24);
    ms %= (1000*60*60*24);
    unsigned hours = ms / (1000*60*60);
    ms %= (1000*60*60);
    unsigned mins = ms / (1000*60);
    ms %= (1000*60);
    unsigned secs = ms / 1000;
    ms %= 1000;
    if (days)
        s.appendf("%d days ", days);
    if (hours || s.length())
        s.appendf("%d:", hours);
    if (mins || s.length())
        s.appendf("%d:", mins);
    if (s.length())
        s.appendf("%02d.%03d", secs, ms);
    else
        s.appendf("%d.%03d", secs, ms);
}


WsWUExceptions::WsWUExceptions(IConstWorkUnit& wu): numerr(0), numwrn(0), numinf(0), numalert(0)
{
    Owned<IConstWUExceptionIterator> it = &wu.getExceptions();
    ForEach(*it)
    {
        IConstWUException & cur = it->query();
        SCMStringBuffer src, msg, file;
        Owned<IEspECLException> e= createECLException("","");
        e->setCode(cur.getExceptionCode());
        e->setSource(cur.getExceptionSource(src).str());
        e->setMessage(cur.getExceptionMessage(msg).str());
        e->setFileName(cur.getExceptionFileName(file).str());
        e->setLineNo(cur.getExceptionLineNo());
        e->setColumn(cur.getExceptionColumn());
        if (cur.getActivityId())
            e->setActivity(cur.getActivityId());
        if (cur.getPriority())
            e->setPriority(cur.getPriority());
        e->setScope(cur.queryScope());

        const char * label = "";
        switch (cur.getSeverity())
        {
            default:
            case SeverityError: label = "Error"; numerr++; break;
            case SeverityWarning: label = "Warning"; numwrn++; break;
            case SeverityInformation: label = "Info"; numinf++; break;
            case SeverityAlert: label = "Alert"; numalert++; break;
        }

        e->setSeverity(label);
        errors.append(*e.getLink());
    }
}

#define SDS_LOCK_TIMEOUT 30000

void getSashaNode(SocketEndpoint &ep)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (!env)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Cannot get environment information.");
    Owned<IPropertyTree> root = &env->getPTree();
    IPropertyTree *pt = root->queryPropTree("Software/SashaServerProcess[1]/Instance[1]");
    if (!pt)
        throw MakeStringException(ECLWATCH_ARCHIVE_SERVER_NOT_FOUND, "Archive Server not found.");
    ep.set(pt->queryProp("@netAddress"), pt->getPropInt("@port",DEFAULT_SASHA_PORT));
}


void WsWuInfo::getSourceFiles(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeSourceFiles))
        return;
    try
    {
        Owned<IUserDescriptor> userdesc;
        StringBuffer username;
        context.getUserID(username);
        const char* passwd = context.queryPassword();
        userdesc.setown(createUserDescriptor());
        userdesc->set(username.str(), passwd, context.querySessionToken(), context.querySignature());

        IArrayOf<IEspECLSourceFile> files;
        if (version < 1.27)
        {
            Owned<IPropertyTreeIterator> f=&cw->getFilesReadIterator();
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

            Owned<IPropertyTreeIterator> f=&cw->getFilesReadIterator();
            ForEach(*f)
            {
                IPropertyTree &query = f->query();
                const char *clusterName = query.queryProp("@cluster");
                const char *fileName = query.queryProp("@name");
                int fileCount = query.getPropInt("@useCount");

                bool bFound = false;
                if (fileName && *fileName && (fileNames.length() > 0))
                {
                    for (unsigned i = 0; i < fileNames.length(); i++ ) // MORE - unnecessary n^2 process
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
                    getSubFiles(filetrees, file,    fileNames);
                }

                files.append(*file.getLink());
            }
        }

        info.setSourceFiles(files);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setSourceFilesDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getExceptions(IEspECLWorkunit &info, unsigned long flags)
{
    if ((flags & WUINFO_IncludeExceptions) || version > 1.16)
    {
        WsWUExceptions errors(*cw);
        if (version > 1.16)
        {
            info.setErrorCount(errors.ErrCount());
            info.setWarningCount(errors.WrnCount());
            info.setInfoCount(errors.InfCount());
            info.setAlertCount(errors.AlertCount());
        }
        if ((flags & WUINFO_IncludeExceptions))
            info.setExceptions(errors);
    }
}

void WsWuInfo::getVariables(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeVariables))
        return;
    try
    {
        IArrayOf<IEspECLResult> results;
        Owned<IConstWUResultIterator> vars = &cw->getVariables();
        ForEach(*vars)
            getResult(vars->query(), results, flags);
        info.setVariables(results);
        results.kill();
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setVariablesDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::addTimerToList(SCMStringBuffer& name, const char * scope, IConstWUStatistic & stat, IArrayOf<IEspECLTimer>& timers)
{
    StringBuffer fd;
    formatStatistic(fd, stat.getValue(), stat.getMeasure());

    Owned<IEspECLTimer> t= createECLTimer("","");
    name.s.replace('_', ' '); // yuk!
    t->setName(name.str());
    t->setValue(fd.str());
    //Theoretically this could overflow, in practice it is unlikely - fix in the new stats interface when implemented
    t->setCount((unsigned)stat.getCount());

    if (version > 1.19)
    {
        StringAttr graphName;
        unsigned graphNum;
        unsigned subGraphNum = 0;
        unsigned subId = 0;

        if (parseGraphScope(scope, graphName, graphNum, subId) ||
            parseGraphTimerLabel(name.str(), graphName, graphNum, subGraphNum, subId))   // leacy
        {
            if (graphName.length() > 0)
                t->setGraphName(graphName);
            if (subId > 0)
                t->setSubGraphId((int)subId);
        }
    }

    timers.append(*t.getLink());
}

void WsWuInfo::doGetTimers(IArrayOf<IEspECLTimer>& timers)
{
    unsigned __int64 totalThorTimeValue = 0;
    unsigned __int64 totalThorTimerCount = 0; //Do we need this?

    StatisticsFilter filter;
    filter.setScopeDepth(1, 2);
    filter.setMeasure(SMeasureTimeNs);
    Owned<IConstWUStatisticIterator> it = &cw->getStatistics(&filter);
    if (it->first())
    {
        ForEach(*it)
        {
            IConstWUStatistic & cur = it->query();
            SCMStringBuffer name, scope;
            cur.getDescription(name, true);
            cur.getScope(scope);

            bool isThorTiming = false;//Should it be renamed as isClusterTiming?
            if ((cur.getCreatorType() == SCTsummary) && (cur.getKind() == StTimeElapsed) && isGlobalScope(scope.str()))
            {
                SCMStringBuffer creator;
                cur.getCreator(creator);
                if (streq(creator.str(), "thor") || streq(creator.str(), "hthor") ||
                    streq(creator.str(), "roxie"))
                    isThorTiming = true;
            }
            else if (strieq(name.str(), TOTALTHORTIME)) // legacy
                isThorTiming = true;

            if (isThorTiming)
            {
                totalThorTimeValue += cur.getValue();
                totalThorTimerCount += cur.getCount();
            }
            else
                addTimerToList(name, scope.str(), cur, timers);
        }
    }

    if (totalThorTimeValue > 0)
    {
        StringBuffer totalThorTimeText;
        formatStatistic(totalThorTimeText, totalThorTimeValue, SMeasureTimeNs);

        Owned<IEspECLTimer> t= createECLTimer("","");
        if (version > 1.52)
            t->setName(TOTALCLUSTERTIME);
        else
            t->setName(TOTALTHORTIME);
        t->setValue(totalThorTimeText.str());
        t->setCount((unsigned)totalThorTimerCount);
        timers.append(*t.getLink());
    }
}

void WsWuInfo::getTimers(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeTimers))
        return;
    try
    {
        IArrayOf<IEspECLTimer> timers;
        doGetTimers(timers);
        info.setTimers(timers);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setTimersDesc(eMsg.str());
        e->Release();
    }
}

unsigned WsWuInfo::getTimerCount()
{
    unsigned numTimers = 0;
    try
    {
        //This filter must match the filter in the function above, otherwise it will be inconsistent
        StatisticsFilter filter;
        filter.setScopeDepth(1, 2);
        filter.setMeasure(SMeasureTimeNs);
        Owned<IConstWUStatisticIterator> it = &cw->getStatistics(&filter);
        ForEach(*it)
            numTimers++;
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }
    return numTimers;
}

EnumMapping queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeSize,  NULL },
};

void WsWuInfo::getHelpers(IEspECLWorkunit &info, unsigned long flags)
{
    try
    {
        IArrayOf<IEspECLHelpFile> helpers;
        unsigned helpersCount = 2;   //  ECL + Workunit XML are also helpers...

        Owned <IConstWUQuery> query = cw->getQuery();
        if(!query)
        {
            ERRLOG("Cannot get Query for this workunit.");
            info.setHelpersDesc("Cannot get Query for this workunit.");
        }
        else
        {
            if (flags & WUINFO_IncludeECL)
            {
                SCMStringBuffer queryText;
                query->getQueryShortText(queryText);
                if (queryText.length())
                {
                    if((flags & WUINFO_TruncateEclTo64k) && (queryText.length() > 64000))
                        queryText.setLen(queryText.str(), 64000);

                    IEspECLQuery* q=&info.updateQuery();
                    q->setText(queryText.str());
                }
            }
            if (version > 1.34)
            {
                SCMStringBuffer mainDefinition;
                query->getQueryMainDefinition(mainDefinition);
                if(mainDefinition.length())
                {
                    IEspECLQuery* q=&info.updateQuery();
                    q->setQueryMainDefinition(mainDefinition.str());
                }
            }

            if (version > 1.30)
            {
                info.setHasArchiveQuery(query->hasArchive());
            }

            for (unsigned i = 0; i < FileTypeSize; i++)
                getHelpFiles(query, (WUFileType) i, helpers, flags, helpersCount);
        }

        getWorkunitThorLogInfo(helpers, info, flags, helpersCount);

        if (cw->getWuidVersion() > 0)
        {
            Owned<IPropertyTreeIterator> eclAgents = cw->getProcesses("EclAgent", NULL);
            ForEach (*eclAgents)
            {
                StringBuffer logName;
                IPropertyTree& eclAgent = eclAgents->query();
                eclAgent.getProp("@log",logName);
                if (!logName.length())
                    continue;

                helpersCount++;
                if (!(flags & WUINFO_IncludeHelpers))
                    continue;

                Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                h->setName(logName.str());
                h->setType(File_EclAgentLog);
                if (version >= 1.43)
                {
                    offset_t fileSize;
                    if (getFileSize(logName.str(), NULL, fileSize))
                        h->setFileSize(fileSize);
                    if (version >= 1.44)
                    {
                        if (eclAgent.hasProp("@pid"))
                            h->setPID(eclAgent.getPropInt("@pid"));
                        else
                            h->setPID(cw->getAgentPID());
                    }
                }
                helpers.append(*h.getLink());
            }
        }
        else // legacy wuid
        {
            Owned<IStringIterator> eclAgentLogs = cw->getLogs("EclAgent");
            ForEach (*eclAgentLogs)
            {
                SCMStringBuffer name;
                eclAgentLogs->str(name);
                if (name.length() < 1)
                    continue;

                helpersCount++;
                if (!(flags & WUINFO_IncludeHelpers))
                    break;

                Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                h->setName(name.str());
                h->setType(File_EclAgentLog);
                if (version >= 1.43)
                {
                    offset_t fileSize;
                    if (getFileSize(name.str(), NULL, fileSize))
                        h->setFileSize(fileSize);
                }
                helpers.append(*h.getLink());
                break;
            }
        }

        info.setHelpers(helpers);
        info.setHelpersCount(helpersCount);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setHelpersDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getApplicationValues(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeApplicationValues))
        return;
    try
    {
        IArrayOf<IEspApplicationValue> av;
        Owned<IConstWUAppValueIterator> app(&cw->getApplicationValues());
        ForEach(*app)
        {
            IConstWUAppValue& val=app->query();
            Owned<IEspApplicationValue> t= createApplicationValue("","");
            t->setApplication(val.queryApplication());
            t->setName(val.queryName());
            t->setValue(val.queryValue());
            av.append(*t.getLink());

        }

        info.setApplicationValues(av);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setApplicationValuesDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getDebugValues(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeDebugValues))
    {
        if (version >= 1.50)
        {
            unsigned debugValueCount = 0;
            Owned<IStringIterator> debugs(&cw->getDebugValues());
            ForEach(*debugs)
                debugValueCount++;
            info.setDebugValueCount(debugValueCount);
        }
        return;
    }
    try
    {
        IArrayOf<IEspDebugValue> dv;
        Owned<IStringIterator> debugs(&cw->getDebugValues());
        ForEach(*debugs)
        {
            SCMStringBuffer name, val;
            debugs->str(name);
            cw->getDebugValue(name.str(),val);

            Owned<IEspDebugValue> t= createDebugValue("","");
            t->setName(name.str());
            t->setValue(val.str());
            dv.append(*t.getLink());
        }
        if (version >= 1.50)
            info.setDebugValueCount(dv.length());
        info.setDebugValues(dv);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setDebugValuesDesc(eMsg.str());
        e->Release();
    }
}

const char *getGraphNum(const char *s,unsigned &num)
{
    while (*s && !isdigit(*s))
        s++;
    num = 0;
    while (isdigit(*s))
    {
        num = num*10+*s-'0';
        s++;
    }
    return s;
}


bool WsWuInfo::hasSubGraphTimings()
{
    StatisticsFilter filter;
    filter.setScopeType(SSTsubgraph);
    filter.setKind(StTimeElapsed);
    Owned<IConstWUStatisticIterator> times = &cw->getStatistics(&filter);
    return times->first();
}

void WsWuInfo::doGetGraphs(IArrayOf<IEspECLGraph>& graphs)
{
    SCMStringBuffer runningGraph;
    WUGraphIDType id;

    WUState st = cw->getState();
    bool running = (!(st==WUStateFailed || st==WUStateAborted || st==WUStateCompleted) && cw->getRunningGraph(runningGraph,id));

    Owned<IConstWUGraphMetaIterator> it = &cw->getGraphsMeta(GraphTypeAny);
    ForEach(*it)
    {
        IConstWUGraphMeta &graph = it->query();

        SCMStringBuffer name, label, type;
        graph.getName(name);
        graph.getLabel(label);

        graph.getTypeName(type);
        WUGraphState graphState = graph.getState();

        Owned<IEspECLGraph> g= createECLGraph();
        g->setName(name.str());
        g->setLabel(label.str());
        g->setType(type.str());
        if (WUGraphComplete == graphState)
            g->setComplete(true);
        else if (running && (WUGraphRunning == graphState))
        {
            g->setRunning(true);
            g->setRunningId(id);
        }
        else if (WUGraphFailed == graphState)
            g->setFailed(true);

        if (version >= 1.53)
        {
            SCMStringBuffer s;
            Owned<IConstWUStatistic> whenGraphStarted = cw->getStatistic(NULL, name.str(), StWhenStarted);
            if (!whenGraphStarted) // 6.x backward compatibility
                whenGraphStarted.setown(cw->getStatistic(NULL, name.str(), StWhenGraphStarted));
            Owned<IConstWUStatistic> whenGraphFinished = cw->getStatistic(NULL, name.str(), StWhenFinished);
            if (!whenGraphFinished) // 6.x backward compatibility
                whenGraphFinished.setown(cw->getStatistic(NULL, name.str(), StWhenGraphFinished));

            if (whenGraphStarted)
                g->setWhenStarted(whenGraphStarted->getFormattedValue(s).str());
            if (whenGraphFinished)
                g->setWhenFinished(whenGraphFinished->getFormattedValue(s).str());
        }
        graphs.append(*g.getLink());
    }
}

void WsWuInfo::getGraphInfo(IEspECLWorkunit &info, unsigned long flags)
{
     if (version > 1.01)
     {
        info.setHaveSubGraphTimings(false);

        if (hasSubGraphTimings())
            info.setHaveSubGraphTimings(true);
     }

    if (!(flags & WUINFO_IncludeGraphs))
        return;

    try
    {
        IArrayOf<IEspECLGraph> graphs;
        doGetGraphs(graphs);
        info.setGraphs(graphs);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setGraphsDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getWUGraphNameAndTypes(WUGraphType graphType, IArrayOf<IEspNameAndType>& graphNameAndTypes)
{
    Owned<IConstWUGraphMetaIterator> it = &cw->getGraphsMeta(graphType);
    ForEach(*it)
    {
        SCMStringBuffer name, type;
        IConstWUGraphMeta &graph = it->query();
        Owned<IEspNameAndType> nameAndType = createNameAndType();
        nameAndType->setName(graph.getName(name).str());
        nameAndType->setType(graph.getTypeName(type).str());
        graphNameAndTypes.append(*nameAndType.getLink());
    }
}

void WsWuInfo::getGraphTimingData(IArrayOf<IConstECLTimingData> &timingData)
{
    StatisticsFilter filter(SCTall, SSTsubgraph, SMeasureTimeNs, StTimeElapsed);
    Owned<IConstWUStatisticIterator> times = &cw->getStatistics(&filter);
    bool matched = false;
    ForEach(*times)
    {
        IConstWUStatistic & cur = times->query();
        SCMStringBuffer scope;
        cur.getScope(scope);

        StringAttr graphName;
        unsigned graphNum;
        unsigned subGraphId;
        if (parseGraphScope(scope.str(), graphName, graphNum, subGraphId))
        {
            unsigned time = (unsigned)nanoToMilli(cur.getValue());

            SCMStringBuffer name;
            cur.getDescription(name, true);

            Owned<IEspECLTimingData> g = createECLTimingData();
            g->setName(name.str());
            g->setGraphNum(graphNum);
            g->setSubGraphNum(subGraphId); // Use the Id - the number is not known
            g->setGID(subGraphId);
            g->setMS(time);
            g->setMin(time/60000);
            timingData.append(*g.getClear());
            matched = true;
        }
    }
}

void WsWuInfo::getEventScheduleFlag(IEspECLWorkunit &info)
{
    info.setEventSchedule(0);
    if (info.getState() && !stricmp(info.getState(), "wait"))
    {
        info.setEventSchedule(2); //Can deschedule
    }
    else
    {
        Owned<IConstWorkflowItemIterator> it = cw->getWorkflowItems();
        if (it)
        {
            ForEach(*it)
            {
                IConstWorkflowItem *r = it->query();
                if (!r)
                    continue;

                Owned<IWorkflowEvent> wfevent = r->getScheduleEvent();
                if (!wfevent)
                    continue;

                if ((!r->hasScheduleCount() || (r->queryScheduleCountRemaining() > 0))
                    && info.getState() && !strieq(info.getState(), "scheduled")
                    && !strieq(info.getState(), "aborting") && !strieq(info.getState(), "aborted")
                    && !strieq(info.getState(), "failed") && !strieq(info.getState(), "archived"))
                {
                    info.setEventSchedule(1); //Can reschedule
                    break;
                }
            }
        }
    }
}

unsigned WsWuInfo::getTotalThorTime(const char * scope)
{
    StatisticsFilter filter;
    filter.setCreatorType(SCTsummary);
    filter.setScope(scope);
    filter.setKind(StTimeElapsed);

    //Should only be a single value
    unsigned totalThorTimeMS = 0;
    Owned<IConstWUStatisticIterator> times = &cw->getStatistics(&filter);
    ForEach(*times)
    {
        totalThorTimeMS += (unsigned)nanoToMilli(times->query().getValue());
    }

    return totalThorTimeMS;
}

unsigned WsWuInfo::getTotalThorTime()
{
    return getTotalThorTime(GLOBAL_SCOPE) + getTotalThorTime(LEGACY_GLOBAL_SCOPE);
}

void WsWuInfo::getCommon(IEspECLWorkunit &info, unsigned long flags)
{
    info.setWuid(cw->queryWuid());
    info.setProtected(cw->isProtected() ? 1 : 0);
    info.setJobname(cw->queryJobName());
    info.setOwner(cw->queryUser());
    clusterName.set(cw->queryClusterName());
    info.setCluster(clusterName.str());
    SCMStringBuffer s;
    info.setSnapshot(cw->getSnapshot(s).str());

    if ((cw->getState() == WUStateScheduled) && cw->aborting())
    {
        info.setStateID(WUStateAborting);
        info.setState("aborting");
    }
    else
    {
        info.setStateID(cw->getState());
        info.setState(cw->queryStateDesc());
    }

    if (cw->isPausing())
        info.setIsPausing(true);

    getEventScheduleFlag(info);

    if (version > 1.27)
    {
        unsigned totalThorTimeMS = getTotalThorTime();
        if (totalThorTimeMS)
        {
            StringBuffer totalThorTimeStr;
            formatDuration(totalThorTimeStr, totalThorTimeMS);
            if (version > 1.52)
                info.setTotalClusterTime(totalThorTimeStr.str());
            else
                info.setTotalThorTime(totalThorTimeStr.str());
        }
    }

    WsWuDateTime dt;
    cw->getTimeScheduled(dt);
    if(dt.isValid())
        info.setDateTimeScheduled(dt.getString(s).str());
}

void WsWuInfo::setWUAbortTime(IEspECLWorkunit &info, unsigned __int64 abortTS)
{
    StringBuffer abortTimeStr;
    formatStatistic(abortTimeStr, abortTS, SMeasureTimestampUs);
    if ((abortTimeStr.length() > 19) && (abortTimeStr.charAt(10) == 'T') && (abortTimeStr.charAt(19) == '.'))
    {
        abortTimeStr.setCharAt(10, ' ');
        abortTimeStr.setLength(19);
    }
    info.setAbortTime(abortTimeStr.str());
}

void WsWuInfo::getInfo(IEspECLWorkunit &info, unsigned long flags)
{
    getCommon(info, flags);

    SecAccessFlags accessFlag = getWsWorkunitAccess(context, *cw);
    info.setAccessFlag(accessFlag);

    SCMStringBuffer s;
    info.setStateEx(cw->getStateEx(s).str());
    WUState state = cw->getState();
    if ((state == WUStateAborting) || (state == WUStateAborted))
    {
        unsigned __int64 abortTS = cw->getAbortTimeStamp();
        if (abortTS > 0) //AbortTimeStamp may not be set in old wu
        {
            setWUAbortTime(info, abortTS);
            cw->getAbortBy(s);
            if (s.length())
                info.setAbortBy(s.str());
        }
    }
    info.setPriorityClass(cw->getPriority());
    info.setPriorityLevel(cw->getPriorityLevel());
    if (context.querySecManager())
        info.setScope(cw->queryWuScope());
    info.setActionEx(cw->queryActionDesc());
    info.setDescription(cw->getDebugValue("description", s).str());
    if (version > 1.21)
        info.setXmlParams(cw->getXmlParams(s, true).str());

    info.setResultLimit(cw->getResultLimit());
    info.setArchived(false);
    info.setGraphCount(cw->getGraphCount());
    info.setSourceFileCount(cw->getSourceFileCount());
    info.setResultCount(cw->getResultCount());
    info.setWorkflowCount(cw->queryEventScheduledCount());
    info.setVariableCount(cw->getVariableCount());
    info.setTimerCount(getTimerCount());
    info.setSourceFileCount(cw->getSourceFileCount());
    info.setApplicationValueCount(cw->getApplicationValueCount());
    info.setHasDebugValue(cw->hasDebugValue("__calculated__complexity__"));

    getClusterInfo(info, flags);
    getExceptions(info, flags);
    getHelpers(info, flags);
    getGraphInfo(info, flags);
    getSourceFiles(info, flags);
    getResults(info, flags);
    getVariables(info, flags);
    getTimers(info, flags);
    getDebugValues(info, flags);
    getApplicationValues(info, flags);
    getWorkflow(info, flags);
}

unsigned WsWuInfo::getWorkunitThorLogInfo(IArrayOf<IEspECLHelpFile>& helpers, IEspECLWorkunit &info, unsigned long flags, unsigned& helpersCount)
{
    unsigned countThorLog = 0;

    IArrayOf<IConstThorLogInfo> thorLogList;
    if (cw->getWuidVersion() > 0)
    {
        StringAttr clusterName(cw->queryClusterName());
        if (!clusterName.length()) //Cluster name may not be set yet
            return countThorLog;

        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
        if (!clusterInfo)
        {
            WARNLOG("Cannot find TargetClusterInfo for workunit %s", cw->queryWuid());
            return countThorLog;
        }

        unsigned numberOfSlaveLogs = clusterInfo->getNumberOfSlaveLogs();

        BoolHash uniqueProcesses;
        Owned<IStringIterator> thorInstances = cw->getProcesses("Thor");
        ForEach (*thorInstances)
        {
            SCMStringBuffer processName;
            thorInstances->str(processName);
            if (processName.length() < 1)
                continue;
            bool* found = uniqueProcesses.getValue(processName.str());
            if (found && *found)
                continue;

            uniqueProcesses.setValue(processName.str(), true);

            StringBuffer groupName;
            getClusterThorGroupName(groupName, processName.str());

            Owned<IStringIterator> thorLogs = cw->getLogs("Thor", processName.str());
            ForEach (*thorLogs)
            {
                SCMStringBuffer logName;
                thorLogs->str(logName);
                if (logName.length() < 1)
                    continue;

                countThorLog++;

                StringBuffer fileType;
                if (countThorLog < 2)
                    fileType.append(File_ThorLog);
                else
                    fileType.appendf("%s%d", File_ThorLog, countThorLog);

                helpersCount++;
                if (flags & WUINFO_IncludeHelpers)
                {
                    Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                    h->setName(logName.str());
                    h->setDescription(processName.str());
                    h->setType(fileType.str());
                    if (version >= 1.43)
                    {
                        offset_t fileSize;
                        if (getFileSize(logName.str(), NULL, fileSize))
                            h->setFileSize(fileSize);
                    }
                    helpers.append(*h.getLink());
                }

                if (version < 1.38)
                    continue;

                const char* pStr = logName.str();
                const char* ppStr = strstr(pStr, "/thormaster.");
                if (!ppStr)
                {
                    WARNLOG("Invalid thorlog entry in workunit xml: %s", logName.str());
                    continue;
                }

                ppStr += 12;
                StringBuffer logDate = ppStr;
                logDate.setLength(10);

                Owned<IEspThorLogInfo> thorLog = createThorLogInfo("","");
                thorLog->setProcessName(processName.str());
                thorLog->setClusterGroup(groupName.str());
                thorLog->setLogDate(logDate.str());
                thorLog->setNumberSlaves(numberOfSlaveLogs);
                thorLogList.append(*thorLog.getLink());
            }
        }
    }
    else //legacy wuid
    {
        Owned<IStringIterator> thorLogs = cw->getLogs("Thor");
        ForEach (*thorLogs)
        {
            SCMStringBuffer name;
            thorLogs->str(name);
            if (name.length() < 1)
                continue;

            countThorLog++;

            StringBuffer fileType;
            if (countThorLog < 2)
                fileType.append(File_ThorLog);
            else
                fileType.appendf("%s%d", File_ThorLog, countThorLog);

            helpersCount++;
            if (flags & WUINFO_IncludeHelpers)
            {
                Owned<IEspECLHelpFile> h= createECLHelpFile("","");
                h->setName(name.str());
                h->setType(fileType.str());
                if (version >= 1.43)
                {
                    offset_t fileSize;
                    if (getFileSize(name.str(), NULL, fileSize))
                        h->setFileSize(fileSize);
                }
                helpers.append(*h.getLink());
            }
        }

        StringBuffer logDir;
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> logTree = &constEnv->getPTree();
        if (logTree)
             logTree->getProp("EnvSettings/log", logDir);
        if (logDir.length() > 0)
        {
            Owned<IStringIterator> debugs = cw->getLogs("Thor");
            ForEach(*debugs)
            {
                SCMStringBuffer val;
                debugs->str(val);
                if (val.length() < 1)
                    continue;

                const char* pStr = val.str();
                const char* ppStr = strstr(pStr, logDir.str());
                if (!ppStr)
                {
                    WARNLOG("Invalid thorlog entry in workunit xml: %s", val.str());
                    continue;
                }

                const char* pProcessName = ppStr + logDir.length();
                char sep = pProcessName[0];
                StringBuffer processName = pProcessName + 1;
                ppStr = strchr(pProcessName + 1, sep);
                if (!ppStr)
                {
                    WARNLOG("Invalid thorlog entry in workunit xml: %s", val.str());
                    continue;
                }
                processName.setLength(ppStr - pProcessName - 1);

                StringBuffer groupName;
                getClusterThorGroupName(groupName, processName.str());

                StringBuffer logDate = ppStr + 12;
                logDate.setLength(10);

                Owned<IEspThorLogInfo> thorLog = createThorLogInfo("","");
                thorLog->setProcessName(processName.str());
                thorLog->setClusterGroup(groupName.str());
                thorLog->setLogDate(logDate.str());
                //for legacy wuid, the log name does not contain slaveNum. So, a user may not specify
                //a slaveNum and we only display the first slave log if > 1 per IP.
                thorLog->setNumberSlaves(0);
                thorLogList.append(*thorLog.getLink());
            }
        }
    }

    if (thorLogList.length() > 0)
        info.setThorLogList(thorLogList);
    thorLogList.kill();

    return countThorLog;
}

bool WsWuInfo::getClusterInfo(IEspECLWorkunit &info, unsigned long flags)
{
    if ((flags & WUINFO_IncludeAllowedClusters) && (version > 1.04))
    {
        StringArray allowedClusters;
        SCMStringBuffer val;
        cw->getAllowedClusters(val);
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
            info.setAllowedClusters(allowedClusters);
    }

    if (version > 1.23 && clusterName.length())
    {
        int clusterTypeFlag = 0;

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> root = &constEnv->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI,"Cannot connect to DALI server.");

        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
        if (clusterInfo.get())
        {//Set thor flag or roxie flag in order to display some options for thor or roxie
            ClusterType platform = clusterInfo->getPlatform();
            if (isThorCluster(platform))
            {
                clusterTypeFlag=1;
                if (version > 1.29)
                    info.setThorLCR(ThorLCRCluster == platform);
            }
            else if (RoxieCluster == platform)
                clusterTypeFlag=2;
        }
        info.setClusterFlag(clusterTypeFlag);
    }
    return true;
}

void WsWuInfo::getWorkflow(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeWorkflows))
        return;
    try
    {
        Owned<IConstWorkflowItemIterator> it = cw->getWorkflowItems();
        if (!it)
            return;

        IArrayOf<IConstECLWorkflow> workflows;
        ForEach(*it)
        {
            IConstWorkflowItem* r = it->query();
            if (!r)
                continue;

            IWorkflowEvent* wfevent = r->getScheduleEvent();
            if (!wfevent)
                continue;

            StringBuffer id;
            Owned<IEspECLWorkflow> g = createECLWorkflow();
            g->setWFID(id.appendf("%d", r->queryWfid()).str());
            g->setEventName(wfevent->queryName());
            g->setEventText(wfevent->queryText());
            if (r->hasScheduleCount())
            {
                g->setCount(r->queryScheduleCount());
                g->setCountRemaining(r->queryScheduleCountRemaining());
            }
            workflows.append(*g.getLink());
        }
        if (workflows.length() > 0)
            info.setWorkflows(workflows);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setWorkflowsDesc(eMsg.str());
        e->Release();
    }
}

IDistributedFile* WsWuInfo::getLogicalFileData(IEspContext& context, const char* logicalName, bool& showFileContent)
{
    StringBuffer username;
    context.getUserID(username);
    Owned<IUserDescriptor> userdesc(createUserDescriptor());
    userdesc->set(username.str(), context.queryPassword(), context.querySessionToken(), context.querySignature());
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
    if (!df)
        return NULL;

    bool blocked;
    if (df->isCompressed(&blocked) && !blocked)
        return df.getClear();

    IPropertyTree& properties = df->queryAttributes();
    const char * format = properties.queryProp("@format");
    if (format && (stricmp(format,"csv")==0 || memicmp(format, "utf", 3) == 0))
    {
        showFileContent = true;
        return df.getClear();
    }
    const char * recordEcl = properties.queryProp("ECL");
    if (!recordEcl)
        return df.getClear();

    MultiErrorReceiver errs;
    Owned<IHqlExpression> ret = ::parseQuery(recordEcl, &errs);
    showFileContent = errs.errCount() == 0;
    return df.getClear();
}

void WsWuInfo::getEclSchemaChildFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional)
{
    if(!expr)
        return;

    ForEachChild(idx, expr)
        getEclSchemaFields(schemas, expr->queryChild(idx), isConditional);
}

void WsWuInfo::getEclSchemaFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional)
{
    if(!expr)
        return;

    int ret = expr->getOperator();
    switch (ret)
    {
    case no_record:
        getEclSchemaChildFields(schemas, expr, isConditional);
        break;
    case no_ifblock:
        {
            getEclSchemaChildFields(schemas, expr->queryChild(1), true);
            break;
        }
    case no_field:
        {
            if (expr->hasAttribute(__ifblockAtom))
                break;
            ITypeInfo * type = expr->queryType();
            IAtom * name = expr->queryName();
            IHqlExpression * nameAttr = expr->queryAttribute(namedAtom);
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
                    getEclSchemaChildFields(schemas, expr->queryRecord(), isConditional);
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

bool WsWuInfo::getResultEclSchemas(IConstWUResult &r, IArrayOf<IEspECLSchemaItem>& schemas)
{
    SCMStringBuffer schema;
    r.getResultEclSchema(schema);
    if (!schema.length())
        return false;

    MultiErrorReceiver errs;
    Owned<IHqlExpression> expr = ::parseQuery(schema.str(), &errs);

    if (errs.errCount() != 0)
        return false;

    getEclSchemaFields(schemas, expr, false);
    return true;
}

void WsWuInfo::getResult(IConstWUResult &r, IArrayOf<IEspECLResult>& results, unsigned long flags)
{
    SCMStringBuffer name;
    r.getResultName(name);

    SCMStringBuffer filename;
    r.getResultLogicalName(filename);

    bool showFileContent = false;
    Owned<IDistributedFile> df = NULL;
    if (filename.length())
        df.setown(getLogicalFileData(context, filename.str(), showFileContent));

    StringBuffer value, link;
    if (r.getResultStatus() == ResultStatusUndefined)
        value.set("[undefined]");

    else if (r.isResultScalar())
    {
        try
        {
            SCMStringBuffer xml;
            r.getResultXml(xml, true);

            Owned<IPropertyTree> props = createPTreeFromXMLString(xml.str(), ipt_caseInsensitive);
            IPropertyTree *val = props->queryPropTree("Row/*");
            if(val)
                value.set(val->queryProp(NULL));
            else
            {
                Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
                Owned<INewResultSet> result;
                result.setown(resultSetFactory->createNewResultSet(&r, wuid.str()));
                Owned<IResultSetCursor> cursor(result->createCursor());
                cursor->first();

                if (cursor->getIsAll(0))
                {
                    value.set("<All/>");
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
                            if (!value.length())
                                value.append('[');
                            else
                                value.append(", ");
                            value.append('\'').append(out.str()).append('\'');
                        }

                        if (value.length())
                            value.append(']');
                    }
                }
            }
        }
        catch(...)
        {
            value.append("[value not available]");
        }
    }
    else
    {
        value.append('[').append(r.getResultTotalRowCount()).append(" rows]");
        if((r.getResultSequence()>=0) && (!filename.length() || (df && df->queryAttributes().hasProp("ECL"))))
            link.append(r.getResultSequence());
    }

    Owned<IEspECLResult> result= createECLResult("","");
    if (flags & WUINFO_IncludeEclSchemas)
    {
        IArrayOf<IEspECLSchemaItem> schemas;
        if (getResultEclSchemas(r, schemas))
            result->setECLSchemas(schemas);
    }
    if (flags & WUINFO_IncludeXmlSchema)
    {
        Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
        Owned<INewResultSet> rs = resultSetFactory->createNewResultSet(&r, wuid.str());
        Owned<IResultSetCursor> cursor(rs->createCursor());

        SCMStringBuffer xsd;
        const IResultSetMetaData & meta = cursor->queryResultSet()->getMetaData();
        meta.getXmlXPathSchema(xsd, false);
        result->setXmlSchema(xsd.str());
    }

    if (filename.length())
        result->setShowFileContent(showFileContent);

    result->setName(name.str());
    result->setLink(link.str());
    result->setSequence(r.getResultSequence());
    result->setValue(value.str());
    result->setFileName(filename.str());
    result->setIsSupplied(r.getResultStatus() == ResultStatusSupplied);
    result->setTotal(r.getResultTotalRowCount());
    results.append(*result.getLink());
}


void WsWuInfo::getResults(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeResults))
        return;
    try
    {
        IArrayOf<IEspECLResult> results;
        Owned<IConstWUResultIterator> it = &(cw->getResults());
        ForEach(*it)
        {
            IConstWUResult &r = it->query();
            if(r.getResultSequence()>=0)
                getResult(r, results, flags);
        }

        if (results.length())
            info.setResults(results);

        results.kill();
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setResultsDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getStats(StatisticsFilter& filter, bool createDescriptions, IArrayOf<IEspWUStatisticItem>& statistics)
{
    Owned<IConstWUStatisticIterator> stats = &cw->getStatistics(&filter);
    ForEach(*stats)
    {
        IConstWUStatistic & cur = stats->query();
        StringBuffer xmlBuf, tsValue;
        SCMStringBuffer curCreator, curScope, curDescription, curFormattedValue;

        StatisticCreatorType curCreatorType = cur.getCreatorType();
        StatisticScopeType curScopeType = cur.getScopeType();
        StatisticMeasure curMeasure = cur.getMeasure();
        StatisticKind curKind = cur.getKind();
        unsigned __int64 value = cur.getValue();
        unsigned __int64 count = cur.getCount();
        unsigned __int64 max = cur.getMax();
        unsigned __int64 ts = cur.getTimestamp();
        cur.getCreator(curCreator);
        cur.getScope(curScope);
        cur.getDescription(curDescription, createDescriptions);
        cur.getFormattedValue(curFormattedValue);

        Owned<IEspWUStatisticItem> wuStatistic = createWUStatisticItem();

        if (version > 1.61)
            wuStatistic->setWuid(wuid);
        if (curCreatorType != SCTnone)
            wuStatistic->setCreatorType(queryCreatorTypeName(curCreatorType));
        if (curCreator.length())
            wuStatistic->setCreator(curCreator.str());
        if (curScopeType != SSTnone)
            wuStatistic->setScopeType(queryScopeTypeName(curScopeType));
        if (curScope.length())
            wuStatistic->setScope(curScope.str());
        if (curMeasure != SMeasureNone)
            wuStatistic->setMeasure(queryMeasureName(curMeasure));
        if (curKind != StKindNone)
            wuStatistic->setKind(queryStatisticName(curKind));
        wuStatistic->setRawValue(value);
        wuStatistic->setValue(curFormattedValue.str());
        if (count != 1)
            wuStatistic->setCount(count);
        if (max)
            wuStatistic->setMax(max);
        if (ts)
        {
            formatStatistic(tsValue, ts, SMeasureTimestampUs);
            wuStatistic->setTimeStamp(tsValue.str());
        }
        if (curDescription.length())
            wuStatistic->setDescription(curDescription.str());

        statistics.append(*wuStatistic.getClear());
    }
}

bool WsWuInfo::getFileSize(const char* fileName, const char* IPAddress, offset_t& fileSize)
{
    if (!fileName || !*fileName)
        return false;

    Owned<IFile> aFile;
    if (!IPAddress || !*IPAddress)
    {
        aFile.setown(createIFile(fileName));
    }
    else
    {
        RemoteFilename rfn;
        rfn.setRemotePath(fileName);
        SocketEndpoint ep(IPAddress);
        rfn.setIp(ep);
        aFile.setown(createIFile(rfn));
    }
    if (!aFile)
        return false;

    bool isDir;
    CDateTime modtime;
    if (!aFile->getInfo(isDir, fileSize, modtime) || isDir)
        return false;
    return true;
}

void WsWuInfo::getHelpFiles(IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers, unsigned long flags, unsigned& helpersCount)
{
    if (!query)
        return;

    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        SCMStringBuffer name, Ip, description;
        IConstWUAssociatedFile & cur = iter->query();
        if (cur.getType() != type)
            continue;

        helpersCount++;
        if (!(flags & WUINFO_IncludeHelpers))
            continue;

        cur.getName(name);
        Owned<IEspECLHelpFile> h= createECLHelpFile("","");
        h->setName(name.str());
        h->setType(getEnumText(type, queryFileTypes));

        if (version > 1.31)
        {
            cur.getIp(Ip);
            h->setIPAddress(Ip.str());

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
            if (version >= 1.43)
            {
                offset_t fileSize;
                if (getFileSize(name.str(), Ip.str(), fileSize))
                    h->setFileSize(fileSize);
            }
            if (version >= 1.58)
            {
                h->setMinActivityId(cur.getMinActivityId());
                h->setMaxActivityId(cur.getMaxActivityId());
            }
        }
        helpers.append(*h.getLink());
    }
}

void WsWuInfo::getSubFiles(IPropertyTreeIterator* f, IEspECLSourceFile* eclSuperFile, StringArray& fileNames)
{
    IArrayOf<IEspECLSourceFile> files;

    ForEach(*f)
    {
        IPropertyTree &query = f->query();

        const char *clusterName = query.queryProp("@cluster");
        const char *fileName = query.queryProp("@name");
        int fileCount = query.getPropInt("@useCount");

        bool bFound = false;
        if (fileName && *fileName && (fileNames.length() > 0)) // MORE - this is an n^2 process and as far as I can tell unnecessary as there will be no dups
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

        Owned<IPropertyTreeIterator> filetrees= query.getElements("Subfile"); // We do not store subfiles of subfiles like this - so this code will never be triggered
        if (filetrees->first())
        {
            file->setIsSuperFile(true);
            getSubFiles(filetrees, file, fileNames);
        }

        files.append(*file.getLink());
    }

    eclSuperFile->setECLSourceFiles(files);

    return;
}

bool WsWuInfo::getResourceInfo(StringArray &viewnames, StringArray &urls, unsigned long flags)
{
    if (!(flags & (WUINFO_IncludeResultsViewNames | WUINFO_IncludeResourceURLs)))
        return true;
    try
    {
        Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false);
        if (wv)
        {
            if (flags & WUINFO_IncludeResultsViewNames)
                wv->getResultViewNames(viewnames);
            if (flags & WUINFO_IncludeResourceURLs)
                wv->getResourceURLs(urls, NULL);
        }
        return true;
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return false;
}

unsigned WsWuInfo::getResourceURLCount()
{
    try
    {
        Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false);
        if (wv)
            return wv->getResourceURLCount();
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return 0;
}

void appendIOStreamContent(MemoryBuffer &mb, IFileIOStream *ios, bool forDownload)
{
    StringBuffer line;
    bool eof = false;
    while (!eof)
    {
        line.clear();
        for (;;)
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

        mb.append(line.length(), line.str());
        if (!forDownload && (mb.length() > 640000))
            break;
    }
}

void WsWuInfo::getWorkunitEclAgentLog(const char* fileName, const char* agentPid, MemoryBuffer& buf)
{
    if(!fileName || !*fileName)
        throw MakeStringException(ECLWATCH_ECLAGENT_LOG_NOT_FOUND,"Log file not specified");
    Owned<IFile> rFile = createIFile(fileName);
    if(!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open file %s.", fileName);
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if(!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Cannot read file %s.", fileName);
    OwnedIFileIOStream ios = createBufferedIOStream(rIO);

    StringBuffer line;
    bool eof = false;
    bool wuidFound = false;

    StringBuffer pidstr;
    if (agentPid && *agentPid)
        pidstr.appendf(" %s ", agentPid);
    else
        pidstr.appendf(" %5d ", cw->getAgentPID());
/*
    Scan the master daily logfile for given PID/WUID. We make the following assumptions
        Column ordering (time, date, pid) is unknown, but we must assume it is constant throughout the logfile.
        It is assumed that the first column is the 8 digit workunit logfile line number.
        Rows from concurrent workunits are intermixed.
        Logfiles are searched via PID and WUID. You are not assured of a match until you have both.
        PIDS and TIDS can and are reused. Beware that a TID could match the search PID.
        Once you have both, you know the offset of the PID column. It is assumed this offset remains constant.
        Search stops at EOF, or early exit if the search PID reappears on different WUID.
*/
    char const * pidchars = pidstr.str();
    size32_t pidLen = pidstr.length();
    unsigned pidOffset = 0;//offset of PID in logfile entry
    while(!eof)
    {
        line.clear();
        for (;;)
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
        const char * pPid = strstr(line.str() + pidOffset, pidchars);
        if (pPid)
        {
            //Check if this is a new instance using line sequence number (PIDs are often reused)
            if (strncmp(line.str(), "00000000", 8) == 0)
            {
                if (wuidFound) //If the correct instance has been found, return that instance before the next instance.
                    break;

                //The last instance is not a correct instance. Clean the buf in order to start a new instance.
                buf.clear();
            }

            //If we spot the workunit id anywhere in the tracing for this pid then assume it is the correct instance.
            if(!wuidFound && strstr(line.str(), wuid.str()))
            {
                pidOffset = pPid - line.str();//remember offset of PID within line
                wuidFound = true;
            }
            if (pidOffset && 0 == strncmp(line.str() + pidOffset, pidchars, pidLen))//this makes sure the match was the PID and not the TID or something else
                buf.append(line.length(), line.str());
        }
    }

    if (buf.length() < 1)
    {
        const char * msg = "(No logfile entries found for this workunit)";
        buf.append(strlen(msg), msg);
    }
}

void WsWuInfo::getWorkunitThorLog(const char* fileName, MemoryBuffer& buf)
{
    if(!fileName || !*fileName)
        throw MakeStringException(ECLWATCH_ECLAGENT_LOG_NOT_FOUND,"Log file not specified");
    Owned<IFile> rFile = createIFile(fileName);
    if (!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,"Cannot open file %s.",fileName);
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",fileName);
    OwnedIFileIOStream ios = createBufferedIOStream(rIO);

    StringBuffer line;
    bool eof = false;
    bool include = false;

    VStringBuffer startwuid("Started wuid=%s", wuid.str());
    VStringBuffer endwuid("Finished wuid=%s", wuid.str());

    const char *sw = startwuid.str();
    const char *ew = endwuid.str();

    while (!eof)
    {
        line.clear();
        for (;;)
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

void WsWuInfo::getWorkunitThorSlaveLog(const char *groupName, const char *ipAddress, const char* logDate, const char* logDir, int slaveNum, MemoryBuffer& buf, bool forDownload)
{
    if (isEmpty(logDir))
      throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave log path not specified.");
    if (isEmpty(logDate))
        throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave log date not specified.");

    StringBuffer slaveIPAddress, logName;
    if (slaveNum > 0)
    {
        if (isEmpty(groupName))
          throw MakeStringException(ECLWATCH_INVALID_INPUT,"Thor group not specified.");

        Owned<IGroup> nodeGroup = queryNamedGroupStore().lookup(groupName);
        if (!nodeGroup || (nodeGroup->ordinality() == 0))
        {
            WARNLOG("Node group %s not found", groupName);
            return;
        }

        nodeGroup->queryNode(slaveNum-1).endpoint().getIpText(slaveIPAddress);
        if (slaveIPAddress.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave log network address not found.");

        logName.appendf("thorslave.%d.%s.log", slaveNum, logDate);
    }
    else
    {//legacy wuid: a user types in an IP address for a thor slave
        if (isEmpty(ipAddress))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave address not specified.");

        //thorslave.10.239.219.6_20100.2012_05_23.log
        logName.appendf("thorslave.%s*.%s.log", ipAddress, logDate);
        const char* portPtr = strchr(ipAddress, '_');
        if (!portPtr)
            slaveIPAddress.append(ipAddress);
        else
        {
            StringBuffer ipAddressStr = ipAddress;
            ipAddressStr.setLength(portPtr - ipAddress);
            slaveIPAddress.append(ipAddressStr.str());
        }
    }

    RemoteFilename rfn;
    rfn.setRemotePath(logDir);
    SocketEndpoint ep(slaveIPAddress.str());
    rfn.setIp(ep);

    Owned<IFile> dir = createIFile(rfn);
    Owned<IDirectoryIterator> diriter = dir->directoryFiles(logName.str());
    if (!diriter->first())
      throw MakeStringException(ECLWATCH_FILE_NOT_EXIST,"Cannot find Thor slave log file %s.", logName.str());

    Linked<IFile> logfile = &diriter->query();
    diriter.clear();
    dir.clear();
    // logfile is now the file to load

    OwnedIFileIO rIO = logfile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.",logName.str());

    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    if (slaveNum > 0)
    {
        StringBuffer line;
        bool eof = false;
        bool include = false;

        VStringBuffer startwuid("Started wuid=%s", wuid.str());
        VStringBuffer endwuid("Finished wuid=%s", wuid.str());

        const char *sw = startwuid.str();
        const char *ew = endwuid.str();

        while (!eof)
        {
            line.clear();
            for (;;)
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
    else
    {//legacy wuid
        appendIOStreamContent(buf, ios.get(), forDownload);
    }
}

void WsWuInfo::getWorkunitResTxt(MemoryBuffer& buf)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    SCMStringBuffer resname;
    queryDllServer().getDll(query->getQueryResTxtName(resname).str(), buf);
}

IConstWUQuery* WsWuInfo::getEmbeddedQuery()
{
    Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false);
    if (wv)
        return wv->getEmbeddedQuery();

    return NULL;
}

void WsWuInfo::getWorkunitArchiveQuery(MemoryBuffer& buf)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    SCMStringBuffer queryText;
    query->getQueryText(queryText);
    if ((queryText.length() < 1) || !isArchiveQuery(queryText.str()))
    {
        if (!query->hasArchive())
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Archive query not found for workunit %s.", wuid.str());

        Owned<IConstWUQuery> embeddedQuery = getEmbeddedQuery();
        if (!embeddedQuery)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Embedded query not found for workunit %s.", wuid.str());

        embeddedQuery->getQueryText(queryText);
        if ((queryText.length() < 1) || !isArchiveQuery(queryText.str()))
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Archive query not found for workunit %s.", wuid.str());
    }
    buf.append(queryText.length(), queryText.str());
}

void WsWuInfo::getWorkunitQueryShortText(MemoryBuffer& buf)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    SCMStringBuffer queryText;
    query->getQueryShortText(queryText);
    if (queryText.length() < 1)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU, "No query for workunit %s.",wuid.str());
    buf.append(queryText.length(), queryText.str());
}

void WsWuInfo::getWorkunitDll(StringBuffer &dllname, MemoryBuffer& buf)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    StringBufferAdaptor isvName(dllname);
    query->getQueryDllName(isvName);
    queryDllServer().getDll(dllname.str(), buf);
}

void WsWuInfo::getWorkunitXml(const char* plainText, MemoryBuffer& buf)
{
    const char* header;
    if (plainText && (!stricmp(plainText, "yes")))
        header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    else
        header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";

    StringBuffer xml;
    exportWorkUnitToXML(cw, xml, true, false, true);

    buf.append(strlen(header), header);
    buf.append(xml.length(), xml.str());
}

void WsWuInfo::getWorkunitCpp(const char *cppname, const char* description, const char* ipAddress, MemoryBuffer& buf, bool forDownload)
{
    if (isEmpty(description))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");
    if (isEmpty(ipAddress))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File location not specified.");
    if (isEmpty(cppname))
        throw MakeStringException(ECLWATCH_INVALID_FILE_NAME, "File path not specified.");

    RemoteFilename rfn;
    rfn.setRemotePath(cppname);
    SocketEndpoint ep(ipAddress);
    rfn.setIp(ep);

    Owned<IFile> cppfile = createIFile(rfn);
    if (!cppfile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open %s.", description);
    OwnedIFileIO rIO = cppfile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);
    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    if (!ios)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);
    appendIOStreamContent(buf, ios.get(), forDownload);
}

void WsWuInfo::getWorkunitAssociatedXml(const char* name, const char* ipAddress, const char* plainText,
    const char* description, bool forDownload, bool addXMLDeclaration, MemoryBuffer& buf)
{
    if (isEmpty(description)) //'File Name' as shown in WU Details page
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");
    if (isEmpty(ipAddress))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File location not specified.");
    if (isEmpty(name)) //file name with full path
        throw MakeStringException(ECLWATCH_INVALID_FILE_NAME, "File path not specified.");

    RemoteFilename rfn;
    rfn.setRemotePath(name);
    SocketEndpoint ep(ipAddress);
    rfn.setIp(ep);

    Owned<IFile> rFile = createIFile(rfn);
    if (!rFile)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open %s.", description);
    OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
    if (!rIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);
    OwnedIFileIOStream ios = createBufferedIOStream(rIO);
    if (!ios)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read %s.", description);

    if (addXMLDeclaration)
    {
        const char* header;
        if (plainText && (!stricmp(plainText, "yes")))
            header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        else
            header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";
        buf.append(strlen(header), header);
    }

    appendIOStreamContent(buf, ios.get(), forDownload);
}

IPropertyTree* WsWuInfo::getWorkunitArchive()
{
    Owned <IConstWUQuery> query = cw->getQuery();
    if(!query)
        return NULL;

    SCMStringBuffer name, ip;
    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        IConstWUAssociatedFile& cur = iter->query();
        if (cur.getType() != FileTypeXml)
            continue;

        cur.getName(name);
        if (name.length() < 15)
            continue;
        const char* pStr = name.str() + name.length() - 15;
        if (strieq(pStr, ".archive.eclxml"))
        {
            cur.getIp(ip);
            break;
        }
    }
    if (!ip.length())
        return NULL;

    MemoryBuffer content;
    getWorkunitAssociatedXml(name.str(), ip.str(), "", "WU archive eclxml", true, false, content);
    if (!content.length())
        return NULL;
    return createPTreeFromXMLString(content.length(), content.toByteArray());
}


IEspWUArchiveFile* WsWuInfo::readArchiveFileAttr(IPropertyTree& fileTree, const char* path)
{
    const char* fileName = fileTree.queryProp("@name");
    if (isEmpty(fileName))
        return NULL;

    Owned<IEspWUArchiveFile> file= createWUArchiveFile();
    file->setName(fileName);
    if (!isEmpty(path))
        file->setPath(path);
    if (fileTree.hasProp("@key"))
        file->setKey(fileTree.queryProp("@key"));
    if (fileTree.hasProp("@sourcePath"))
        file->setSourcePath(fileTree.queryProp("@sourcePath"));
    return file.getClear();
}

IEspWUArchiveModule* WsWuInfo::readArchiveModuleAttr(IPropertyTree& moduleTree, const char* path)
{
    const char* moduleName = moduleTree.queryProp("@name");
    if (isEmpty(moduleName))
        return NULL;

    Owned<IEspWUArchiveModule> module= createWUArchiveModule();
    module->setName(moduleName);
    if (!isEmpty(path))
        module->setPath(path);
    if (moduleTree.hasProp("@fullName"))
        module->setFullName(moduleTree.queryProp("@fullName"));
    if (moduleTree.hasProp("@key"))
        module->setKey(moduleTree.queryProp("@key"));
    if (moduleTree.hasProp("@plugin"))
        module->setPlugin(moduleTree.queryProp("@plugin"));
    if (moduleTree.hasProp("@version"))
        module->setVersion(moduleTree.queryProp("@version"));
    if (moduleTree.hasProp("@sourcePath"))
        module->setSourcePath(moduleTree.queryProp("@sourcePath"));
    if (moduleTree.hasProp("@flags"))
        module->setFlags(moduleTree.getPropInt("@flags", 0));
    return module.getClear();
}

void WsWuInfo::readArchiveFiles(IPropertyTree* archiveTree, const char* path, IArrayOf<IEspWUArchiveFile>& files)
{
    Owned<IPropertyTreeIterator> iter = archiveTree->getElements("Attribute");
    ForEach(*iter)
    {
        IPropertyTree& item = iter->query();
        Owned<IEspWUArchiveFile> file = readArchiveFileAttr(item, path);
        if (file)
            files.append(*file.getClear());
    }
}

void WsWuInfo::listArchiveFiles(IPropertyTree* archiveTree, const char* path, IArrayOf<IEspWUArchiveModule>& modules, IArrayOf<IEspWUArchiveFile>& files)
{
    if (!archiveTree)
        return;

    Owned<IPropertyTreeIterator> iter = archiveTree->getElements("Module");
    ForEach(*iter)
    {
        IPropertyTree& item = iter->query();
        Owned<IEspWUArchiveModule> module = readArchiveModuleAttr(item, path);
        if (!module)
            continue;

        StringBuffer newPath;
        if (isEmpty(path))
            newPath.set(module->getName());
        else
            newPath.setf("%s/%s", path, module->getName());
        IArrayOf<IEspWUArchiveModule> modulesInModule;
        IArrayOf<IEspWUArchiveFile> filesInModule;
        listArchiveFiles(&item, newPath.str(), modulesInModule, filesInModule);
        if (modulesInModule.length())
            module->setArchiveModules(modulesInModule);
        if (filesInModule.length())
            module->setFiles(filesInModule);

        modules.append(*module.getClear());
    }

    readArchiveFiles(archiveTree, path, files);
}

void WsWuInfo::getArchiveFile(IPropertyTree* archive, const char* moduleName, const char* attrName, const char* path, StringBuffer& file)
{
    StringBuffer xPath;
    if (!isEmpty(path))
    {
        StringArray list;
        list.appendListUniq(path, "/");
        ForEachItemIn(m, list)
        {
            const char* module = list.item(m);
            if (!isEmpty(module))
                xPath.appendf("Module[@name=\"%s\"]/", module);
        }
    }
    if (isEmpty(moduleName))
        xPath.appendf("Attribute[@name=\"%s\"]", attrName);
    else
        xPath.appendf("Module[@name=\"%s\"]/Text", moduleName);

    file.set(archive->queryProp(xPath.str()));
}

WsWuSearch::WsWuSearch(IEspContext& context,const char* owner,const char* state,const char* cluster,const char* startDate,const char* endDate,const char* jobname)
{
    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnitIterator> it(factory->getWorkUnitsByOwner(owner)); // null owner means fetch all

    StringBuffer wuFrom, wuTo, jobPattern;
    if (startDate && *startDate)
        createWuidFromDate(startDate, wuFrom);
    if (endDate && *endDate)
        createWuidFromDate(endDate, wuTo);
    if (jobname && *jobname)
        jobPattern.appendf("*%s*", jobname);

    ForEach(*it)
    {
        IConstWorkUnitInfo &cw = it->query();
        if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cw, accessOwn, accessOthers) < SecAccess_Read)
            continue;
        if (state && *state && !strieq(cw.queryStateDesc(), state))
            continue;
        if (cluster && *cluster && !strieq(cw.queryClusterName(), cluster))
            continue;
        if (jobPattern.length() && !WildMatch(cw.queryJobName(), jobPattern, true))
            continue;

        const char *wuid = cw.queryWuid();
        if (wuFrom.length() && strcmp(wuid,wuFrom.str())<0)
            continue;
        if (wuTo.length() && strcmp(wuid, wuTo.str())>0)
            continue;

        wuids.push_back(wuid);
    }
    std::sort(wuids.begin(), wuids.end(),std::greater<std::string>());
}

StringBuffer& WsWuSearch::createWuidFromDate(const char* timestamp,StringBuffer& s)
{
    CDateTime wuTime;
    wuTime.setString(timestamp,NULL,true);

    unsigned year, month, day, hour, minute, second, nano;
    wuTime.getDate(year, month, day, true);
    wuTime.getTime(hour, minute, second, nano, true);
    s.appendf("W%4d%02d%02d-%02d%02d%02d",year,month,day,hour,minute,second);
    return s;
}

struct CompareData
{
    CompareData(const char* _filter): filter(_filter) {}
    bool operator()(const Linked<DataCacheElement>& e) const
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
        std::list<Linked<DataCacheElement> >::iterator list_iter = cache.begin();
        if (list_iter == cache.end())
            break;

        DataCacheElement* awu = list_iter->get();
        if (!awu || (awu->m_timeCached > timeNow))
            break;

        cache.pop_front();
    }

    if (cache.size() < 1)
        return NULL;

    //Check whether we have the data cache for this cluster. If yes, get the version
    std::list<Linked<DataCacheElement> >::iterator it = std::find_if(cache.begin(),cache.end(),CompareData(filter));
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
    bool operator()(const Linked<ArchivedWuCacheElement>& e) const
    {
        return stricmp(e->m_filter.c_str(),filter)==0;
    }
    const char* filter;
};


ArchivedWuCacheElement* ArchivedWuCache::lookup(IEspContext &context, const char* filter, const char* sashaUpdatedWhen, unsigned timeOutMin)
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
        std::list<Linked<ArchivedWuCacheElement> >::iterator list_iter = cache.begin();
        if (list_iter == cache.end())
            break;

        ArchivedWuCacheElement* awu = list_iter->get();
        if (awu && !stricmp(sashaUpdatedWhen, awu->m_sashaUpdatedWhen.c_str()) && (awu->m_timeCached > timeNow))
            break;

        cache.pop_front();
    }

    if (cache.size() < 1)
        return NULL;

    //Check whether we have the data cache for this cluster. If yes, get the version
    std::list<Linked<ArchivedWuCacheElement> >::iterator it = std::find_if(cache.begin(),cache.end(),CompareArchivedWUs(filter));
    if(it!=cache.end())
        return it->getLink();

    return NULL;
}

void ArchivedWuCache::add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, unsigned numWUsReturned, IArrayOf<IEspECLWorkunit>& wus, IArrayOf<IEspECLWorkunitLW>& lwwus)
{
    CriticalBlock block(crit);

    //Save new data
    Owned<ArchivedWuCacheElement> e=new ArchivedWuCacheElement(filter, sashaUpdatedWhen, hasNextPage, numWUsReturned, wus, lwwus);
    if (cacheSize > 0)
    {
        if (cache.size() >= cacheSize)
            cache.pop_front();

        cache.push_back(e.get());
    }

    return;
}

WsWuJobQueueAuditInfo::WsWuJobQueueAuditInfo(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, const char *xls)
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
    if(notEmpty(cluster))
        filter.appendf(",%s", cluster);

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
            getAuditLineInfo(line, longestQueue, maxConnected, maxDisplay, 1, items);
        else
            getAuditLineInfo(line, longestQueue, maxConnected, maxDisplay, 2, items);
        countLines++;
    }

    StringBuffer responsebuf;
    if (items.length() < 1)
    {
        responsebuf.append("<script language=\"javascript\">\r\nparent.displayQEnd(\'No data found\')</script>\r\n");
        response->sendChunk(responsebuf.str());
        return;
    }

    unsigned itemCount = items.length();
    if (itemCount > maxDisplay)
        itemCount = maxDisplay;

    responsebuf.append("<script language=\"javascript\">parent.displayQLegend()</script>\r\n");
    response->sendChunk(responsebuf.str());
    responsebuf.clear();
    responsebuf.append("<script language=\"javascript\">parent.displayQBegin(").append(longestQueue).append(",").append(maxConnected).append(",").append(itemCount).append(")</script>\r\n");
    response->sendChunk(responsebuf.str());
    responsebuf.clear();
    responsebuf.append("<script language=\"javascript\">\r\n");

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

        responsebuf.append("parent.displayQueue(\'").append(count).append("\',\'").append(dtStr.str()).append("\',\'").append(tq.getRunningWUs()).append("\',");
        responsebuf.append("\'").append(tq.getQueuedWUs()).append("\',\'").append(tq.getWaitingThors()).append("\',");
        responsebuf.append("\'").append(tq.getConnectedThors()).append("\',\'").append(tq.getIdledThors()).append("\',");
        responsebuf.append("\'").append(tq.getRunningWU1()).append("\',\'").append(tq.getRunningWU2()).append("\')\r\n");
        if(++jobpending>=50)
        {
            responsebuf.append("</script>\r\n");
            response->sendChunk(responsebuf.str());
            responsebuf.clear();
            responsebuf.append("<script language=\"javascript\">\r\n");
            jobpending=0;
        }
    }

    StringBuffer countStr;
    countStr.appendulong(count);

    StringBuffer msg("<table><tr><td>");
    msg.append("Total Records in the Time Period: ").append(items.length()).append(" (<a href=\"/WsWorkunits/WUClusterJobQueueLOG?").append(xls).append("\">txt</a>...<a href=\"/WsWorkunits/WUClusterJobQueueXLS?").append(xls).append("\">xls</a>).");
    msg.append("</td></tr><tr><td>");
    if (count > maxDisplay)
        msg.append("Displayed: First ").append(maxDisplay).append(". ");
    msg.append("Max. Queue Length: ").append(longestQueue).append(".");
    msg.append("</td></tr></table>");

    responsebuf.append("parent.displayQEnd(\'").append(msg).append("\')</script>\r\n");
    response->sendChunk(responsebuf.str());
}

void WsWuJobQueueAuditInfo::getAuditLineInfo(const char* line, unsigned& longestQueue, unsigned& maxConnected, unsigned maxDisplay, unsigned showAll, IArrayOf<IEspThorQueue>& items)
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
}

bool WsWuJobQueueAuditInfo::checkSameStrings(const char* s1, const char* s2)
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

bool WsWuJobQueueAuditInfo::checkNewThorQueueItem(IEspThorQueue* tq, unsigned showAll, IArrayOf<IEspThorQueue>& items)
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

void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret)
{
    StringBuffer xsl;
    if(!checkFileExists(sheet))
        throw MakeStringException(ECLWATCH_FILE_NOT_EXIST, "Could not find stylesheet %s.",sheet);
    Owned<IXslProcessor> proc = getXslProcessor();
    Owned<IXslTransform> trans = proc->createXslTransform();
    trans->setXmlSource(xml, strlen(xml));
    trans->loadXslFromFile(sheet);
    trans->copyParameters(params);
    trans->transform(ret);
}

bool addToQueryString(StringBuffer &queryString, const char *name, const char *value, const char delim)
{
    if (isEmpty(name) || isEmpty(value))
        return false;
    if (queryString.length() > 0)
        queryString.append(delim);
    queryString.append(name).append("=").append(value);
    return true;
}

int WUSchedule::run()
{
    try
    {
        PROGLOG("ECLWorkunit WUSchedule Thread started.");
        while(!stopping)
        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IConstWorkUnitIterator> itr = factory->getScheduledWorkUnits();
            if (itr)
            {
                ForEach(*itr)
                {
                    try
                    {
                        IConstWorkUnitInfo & cw = itr->query();
                        if (factory->isAborting(cw.queryWuid()))
                        {
                            WorkunitUpdate wu(factory->updateWorkUnit(cw.queryWuid()));
                            wu->setState(WUStateAborted);
                            continue;
                        }
                        WsWuDateTime dt, now;
                        now.setNow();
                        cw.getTimeScheduled(dt);
                        if (now.compare(dt)>=0)
                            runWorkUnit(cw.queryWuid(), cw.queryClusterName());
                    }
                    catch(IException *e)
                    {
                        StringBuffer msg;
                        ERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                        e->Release();
                    }
                }
            }
            semSchedule.wait(1000*60);
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

void WsWuHelpers::setXmlParameters(IWorkUnit *wu, const char *xml, bool setJobname)
{
    if (!xml || !*xml)
        return;
    Owned<IPropertyTree> tree = createPTreeFromXMLString(xml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace | ptr_ignoreNameSpaces));
    IPropertyTree *root = tree.get();
    if (strieq(root->queryName(), "Envelope"))
        root = root->queryPropTree("Body/*[1]");
    if (!root)
        return;
    if (setJobname)
    {
        const char *name = wu->queryJobName();
        if (!name || !*name)
            wu->setJobName(root->queryName());
    }
    wu->setXmlParams(LINK(root));
}

void WsWuHelpers::setXmlParameters(IWorkUnit *wu, const char *xml, IArrayOf<IConstNamedValue> *variables, bool setJobname)
{
    StringBuffer extParamXml;
    if (variables && variables->length())
    {
        Owned<IPropertyTree> paramTree = (xml && *xml) ? createPTreeFromXMLString(xml) : createPTree("input");
        ForEachItemIn(i, *variables)
        {
            IConstNamedValue &item = variables->item(i);
            const char *name = item.getName();
            const char *value = item.getValue();
            if (!name || !*name)
                continue;
            if (!value)
            {
                size_t len = strlen(name);
                char last = name[len-1];
                if (last == '-' || last == '+')
                {
                    StringAttr s(name, len-1);
                    paramTree->setPropInt(s.get(), last == '+' ? 1 : 0);
                }
                else
                    paramTree->setPropInt(name, 1);
                continue;
            }
            paramTree->setProp(name, value);
        }
        toXML(paramTree, extParamXml);
        xml=extParamXml.str();
    }
    setXmlParameters(wu, xml, setJobname);
}

void WsWuHelpers::submitWsWorkunit(IEspContext& context, IConstWorkUnit* cw, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow, bool resetVariables,
    const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications)
{
    ensureWsWorkunitAccess(context, *cw, SecAccess_Write);

#ifndef _NO_LDAP
    CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());

    // View Scope is checked only when LDAP secmgr is available AND checkViewPermissions config is also enabled.
    // Otherwise, the view permission check is skipped, and WU is submitted as normal.
    if (secmgr && secmgr->getCheckViewPermissions())
    {
        StringArray filenames, columnnames;
        if (cw->getFieldUsageArray(filenames, columnnames, cluster)) // check view permission only for a query with fieldUsage information
        {
            if (!secmgr->authorizeViewScope(*context.queryUser(), filenames, columnnames))
                throw MakeStringException(ECLWATCH_VIEW_ACCESS_DENIED, "View Access denied for a WU: %s", cw->queryWuid());
        }
    }
#endif

    switch(cw->getState())
    {
        case WUStateRunning:
        case WUStateDebugPaused:
        case WUStateDebugRunning:
        case WUStateCompiling:
        case WUStateAborting:
        case WUStateBlocked:
            throw MakeStringException(ECLWATCH_CANNOT_SUBMIT_WORKUNIT, "Cannot submit the workunit. Workunit state is '%s'.", cw->queryStateDesc());
    }

    StringAttr wuid(cw->queryWuid());

    WorkunitUpdate wu(&cw->lock());
    if(!wu.get())
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Cannot update workunit %s.", wuid.str());

    wu->clearExceptions();
    if(notEmpty(cluster))
        wu->setClusterName(cluster);
    if(notEmpty(snapshot))
        wu->setSnapshot(snapshot);
    wu->setState(WUStateSubmitted);
    if (maxruntime)
        wu->setDebugValueInt("maxRunTime",maxruntime,true);

    if (debugs && debugs->length())
    {
        ForEachItemIn(i, *debugs)
        {
            IConstNamedValue &item = debugs->item(i);
            const char *name = item.getName();
            const char *value = item.getValue();
            if (!name || !*name)
                continue;
            if (!value)
            {
                size_t len = strlen(name);
                char last = name[len-1];
                if (last == '-' || last == '+')
                {
                    StringAttr s(name, len-1);
                    wu->setDebugValueInt(s.get(), last == '+' ? 1 : 0, true);
                }
                else
                    wu->setDebugValueInt(name, 1, true);
                continue;
            }
            wu->setDebugValue(name, value, true);
        }
    }

    if (applications)
    {
        ForEachItemIn(ii, *applications)
        {
            IConstApplicationValue& item = applications->item(ii);
            if(notEmpty(item.getApplication()) && notEmpty(item.getName()))
                wu->setApplicationValue(item.getApplication(), item.getName(), item.getValue(), true);
        }
    }

    if (resetWorkflow)
        wu->resetWorkflow();
    if (!compile)
        wu->schedule();

    if (resetVariables)
    {
        SCMStringBuffer varname;
        Owned<IConstWUResultIterator> vars = &wu->getVariables();
        ForEach (*vars)
        {
            vars->query().getResultName(varname);
            Owned<IWUResult> v = wu->updateVariableByName(varname.str());
            if (v)
                v->setResultStatus(ResultStatusUndefined);
        }
    }

    setXmlParameters(wu, paramXml, variables, (wu->getAction()==WUActionExecuteExisting));

    wu->commit();
    wu.clear();

    if (!compile)
        runWorkUnit(wuid.str());
    else if (context.querySecManager())
        secSubmitWorkUnit(wuid.str(), *context.querySecManager(), *context.queryUser());
    else
        submitWorkUnit(wuid.str(), context.queryUserId(), context.queryPassword());

    AuditSystemAccess(context.queryUserId(), true, "Submitted %s", wuid.str());
}

void WsWuHelpers::submitWsWorkunit(IEspContext& context, const char *wuid, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow, bool resetVariables,
    const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    if(!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid);
    return submitWsWorkunit(context, cw, cluster, snapshot, maxruntime, compile, resetWorkflow, resetVariables, paramXml, variables, debugs, applications);
}


void WsWuHelpers::copyWsWorkunit(IEspContext &context, IWorkUnit &wu, const char *srcWuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> src(factory->openWorkUnit(srcWuid));

    queryExtendedWU(&wu)->copyWorkUnit(src, false, false);

    SCMStringBuffer token;
    wu.setSecurityToken(createToken(wu.queryWuid(), context.queryUserId(), context.queryPassword(), token).str());
    wu.commit();
}

void WsWuHelpers::runWsWorkunit(IEspContext &context, StringBuffer &wuid, const char *srcWuid, const char *cluster, const char *paramXml,
    IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications)
{
    NewWsWorkunit wu(context);
    wuid.set(wu->queryWuid());
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, wuid.str(), cluster, NULL, 0, false, true, true, paramXml, variables, debugs, applications);
}

void WsWuHelpers::runWsWorkunit(IEspContext &context, IConstWorkUnit *cw, const char *srcWuid, const char *cluster, const char *paramXml,
    IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications)
{
    WorkunitUpdate wu(&cw->lock());
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, cw, cluster, NULL, 0, false, true, true, paramXml, variables, debugs, applications);
}

IException * WsWuHelpers::noteException(IWorkUnit *wu, IException *e, ErrorSeverity level)
{
    if (wu)
    {
        Owned<IWUException> we = wu->createException();
        StringBuffer s;
        we->setExceptionMessage(e->errorMessage(s).str());
        we->setExceptionSource("WsWorkunits");
        we->setSeverity(level);
        if (level==SeverityError)
            wu->setState(WUStateFailed);
    }
    return e;
}

StringBuffer & WsWuHelpers::resolveQueryWuid(StringBuffer &wuid, const char *queryset, const char *query, bool notSuspended, IWorkUnit *wu)
{
    Owned<IPropertyTree> qs = getQueryRegistry(queryset, true);
    if (!qs)
        throw noteException(wu, MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet '%s' not found", queryset));
    Owned<IPropertyTree> q = resolveQueryAlias(qs, query);
    if (!q)
        throw noteException(wu, MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query '%s/%s' not found", queryset, query));
    if (notSuspended && q->getPropBool("@suspended"))
        throw noteException(wu, MakeStringException(ECLWATCH_QUERY_SUSPENDED, "Query '%s/%s' is suspended", queryset, query));
    return wuid.append(q->queryProp("@wuid"));
}

void WsWuHelpers::runWsWuQuery(IEspContext &context, IConstWorkUnit *cw, const char *queryset, const char *query,
    const char *cluster, const char *paramXml, IArrayOf<IConstApplicationValue> *applications)
{
    StringBuffer srcWuid;

    WorkunitUpdate wu(&cw->lock());
    resolveQueryWuid(srcWuid, queryset, query, true, wu);
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, cw, cluster, NULL, 0, false, true, true, paramXml, NULL, NULL, applications);
}

void WsWuHelpers::runWsWuQuery(IEspContext &context, StringBuffer &wuid, const char *queryset, const char *query,
    const char *cluster, const char *paramXml, IArrayOf<IConstApplicationValue> *applications)
{
    StringBuffer srcWuid;

    NewWsWorkunit wu(context);
    wuid.set(wu->queryWuid());
    resolveQueryWuid(srcWuid, queryset, query, true, wu);
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, wuid.str(), cluster, NULL, 0, false, true, true, paramXml, NULL, NULL, applications);
}

void WsWuHelpers::checkAndTrimWorkunit(const char* methodName, StringBuffer& input)
{
    const char* trimmedInput = input.trim().str();
    if (isEmpty(trimmedInput))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: Workunit ID not set", methodName);

    if (!looksLikeAWuid(trimmedInput, 'W'))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: Invalid Workunit ID: %s", methodName, trimmedInput);

    return;
}
}
