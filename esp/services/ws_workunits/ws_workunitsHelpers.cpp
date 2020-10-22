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
#include "rmtsmtp.hpp"
#include "LogicFileWrapper.hpp"

#ifndef _NO_LDAP
#include "ldapsecurity.ipp"
#endif

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

namespace ws_workunits {

const char * const timerFilterText = "measure[time],source[global],depth[1,]"; // Does not include hthor subgraph timings
const char* zipFolder = "tempzipfiles" PATHSEPSTR;

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
        context.setAuthStatus(AUTH_STATUS_NOACCESS);
        AuditSystemAccess(context.queryUserId(), false, "Access Denied: User can't view any workunits");
        VStringBuffer msg("Access Denied: User %s does not have rights to access workunits.", context.queryUserId());
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "%s", msg.str());
    }
}

SecAccessFlags getWsWorkunitAccess(IEspContext& ctx, IConstWorkUnit& cw)
{
    SecAccessFlags accessFlag = SecAccess_None;
    ctx.authorizeFeature(getWuAccessType(cw, ctx.queryUserId()), accessFlag);
    return accessFlag;
}

bool validateWsWorkunitAccess(IEspContext& ctx, const char* wuid, SecAccessFlags minAccess)
{
    Owned<IWorkUnitFactory> wf = getWorkUnitFactory(ctx.querySecManager(), ctx.queryUser());
    Owned<IConstWorkUnit> cw = wf->openWorkUnit(wuid);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Failed to open workunit %s when validating workunit access", wuid);
    return ctx.validateFeatureAccess(getWuAccessType(*cw, ctx.queryUserId()), minAccess, false);
}

void ensureWsWorkunitAccessByOwnerId(IEspContext& ctx, const char* owner, SecAccessFlags minAccess)
{
    if (!ctx.validateFeatureAccess(getWuAccessType(owner, ctx.queryUserId()), minAccess, false))
    {
        ctx.setAuthStatus(AUTH_STATUS_NOACCESS);
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
    }
}

void ensureWsWorkunitAccess(IEspContext& ctx, IConstWorkUnit& cw, SecAccessFlags minAccess)
{
    if (!ctx.validateFeatureAccess(getWuAccessType(cw, ctx.queryUserId()), minAccess, false))
    {
        ctx.setAuthStatus(AUTH_STATUS_NOACCESS);
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to access workunit. Permission denied.");
    }
}

void ensureWsWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess)
{
    Owned<IWorkUnitFactory> wf = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = wf->openWorkUnit(wuid);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Failed to open workunit %s when ensuring workunit access", wuid);
    ensureWsWorkunitAccess(context, *cw, minAccess);
}

void ensureWsCreateWorkunitAccess(IEspContext& ctx)
{
    if (!ctx.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
    {
        ctx.setAuthStatus(AUTH_STATUS_NOACCESS);
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");
    }
}

StringBuffer &getWuidFromLogicalFileName(IEspContext &context, const char *logicalName, StringBuffer &wuid)
{
    Owned<IDistributedFile> df = lookupLogicalName(context, logicalName, false, false, false, nullptr, defaultPrivilegedUser);
    if (!df)
        throw makeStringExceptionV(ECLWATCH_FILE_NOT_EXIST, "Cannot find file %s.", logicalName);
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
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> env = factory->openEnvironment();
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
        userdesc->set(username.str(), passwd, context.querySignature());

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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
    if (version >= 1.72)
    {
        StringBuffer tsText;
        unsigned __int64 ts = stat.getTimestamp();
        formatStatistic(tsText, ts,  SMeasureTimestampUs);
        t->setTimestamp(ts);
        t->setWhen(tsText.str());
    }

    timers.append(*t.getLink());
}

void WsWuInfo::doGetTimers(IArrayOf<IEspECLTimer>& timers)
{
    class TimingVisitor : public WuScopeVisitorBase
    {
    public:
        TimingVisitor(WsWuInfo & _wuInfo, IArrayOf<IEspECLTimer>& _timers) : wuInfo(_wuInfo), timers(_timers) {}

        virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
        {
            SCMStringBuffer name;
            extra.getDescription(name, true);
            const char * scope = extra.queryScope();
            wuInfo.addTimerToList(name, scope, extra, timers);

            //Aggregate all the times spent executing graphs
            if ((kind == StTimeElapsed) && (extra.getScopeType() == SSTgraph))
                totalGraphTime.noteValue(value);
        }

        void addSummary()
        {
            if (totalGraphTime.getCount())
            {
                StringBuffer totalThorTimeText;
                formatStatistic(totalThorTimeText, totalGraphTime.getSum(), SMeasureTimeNs);

                Owned<IEspECLTimer> t= createECLTimer("","");
                if (wuInfo.version > 1.52)
                    t->setName(TOTALCLUSTERTIME);
                else
                    t->setName(TOTALTHORTIME);
                t->setValue(totalThorTimeText.str());
                t->setCount((unsigned)totalGraphTime.getCount());
                timers.append(*t.getClear());
            }
        }
    protected:
        WsWuInfo & wuInfo;
        IArrayOf<IEspECLTimer>& timers;
        StatsAggregation totalGraphTime;
    } visitor(*this, timers);

    WuScopeFilter filter(timerFilterText);
    Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
    ForEach(*it)
        it->playProperties(visitor);

    visitor.addSummary();
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
        info.setTimersDesc(eMsg.str());
        e->Release();
    }
}

class TimingCounter : public WuScopeVisitorBase
{
public:
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override
    {
        numTimers++;
        if ((kind == StTimeElapsed) && (extra.getScopeType() == SSTgraph))
            hasGraphTiming = true;
    }

    unsigned getNumTimers() const
    {
        return numTimers + (hasGraphTiming ? 1 : 0);
    }
protected:
    bool hasGraphTiming = false;
    unsigned numTimers = 0;
};

unsigned WsWuInfo::getTimerCount()
{
    TimingCounter visitor;
    try
    {
        WuScopeFilter filter(timerFilterText);
        Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
        ForEach(*it)
            it->playProperties(visitor);
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        IERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return visitor.getNumTimers();
}

EnumMapping queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeLog, "log" },
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
            IERRLOG("Cannot get Query for this workunit.");
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
    try
    {
        WuScopeFilter filter("depth[3],stype[subgraph],stat[TimeElapsed],nested[0]");
        Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
        ForEach(*it)
        {
            stat_type value;
            if (it->getStat(StTimeElapsed, value))
                return true;
        }
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        IERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return false;
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
            //MORE: Will need to be prefixed with the wfid
            StringBuffer scope;
            scope.append(name);

            StringBuffer s;
            stat_type timeStamp;
            if (cw->getStatistic(timeStamp, scope.str(), StWhenStarted) ||
                cw->getStatistic(timeStamp, name.str(), StWhenGraphStarted))
            {
                g->setWhenStarted(formatStatistic(s.clear(), timeStamp, SMeasureTimestampUs));
            }

            if (cw->getStatistic(timeStamp, scope.str(), StWhenFinished) ||
               cw->getStatistic(timeStamp, name.str(), StWhenGraphFinished))
            {
                g->setWhenFinished(formatStatistic(s.clear(), timeStamp, SMeasureTimestampUs));
            }
        }
        graphs.append(*g.getLink());
    }
}

void WsWuInfo::getGraphInfo(IEspECLWorkunit &info, unsigned long flags)
{
     if ((version > 1.01) && (version < 1.71))
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
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
    class TimingVisitor : public WuScopeVisitorBase
    {
    public:
        TimingVisitor(WsWuInfo & _wuInfo, IArrayOf<IConstECLTimingData> & _timingData) : wuInfo(_wuInfo), timingData(_timingData) {}

        virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & cur) override
        {
            const char * scope = cur.queryScope();
            StringAttr graphName;
            unsigned graphNum;
            unsigned subGraphId;
            if (parseGraphScope(scope, graphName, graphNum, subGraphId))
            {
                unsigned time = (unsigned)nanoToMilli(value);

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
            }
        }

    protected:
        WsWuInfo & wuInfo;
        IArrayOf<IConstECLTimingData> & timingData;
    } visitor(*this, timingData);

    WuScopeFilter filter("stype[subgraph],stat[TimeElapsed],nested[0]");
    Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
    ForEach(*it)
        it->playProperties(visitor);
}

void WsWuInfo::getServiceNames(IEspECLWorkunit &info, unsigned long flags)
{
    if (!(flags & WUINFO_IncludeServiceNames))
        return;

    StringArray serviceNames;
    WuScopeFilter filter;
    filter.addScopeType("activity");
    filter.addOutputAttribute(WaServiceName);
    filter.addRequiredAttr(WaServiceName);
    filter.finishedFilter();
    Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
    ForEach(*it)
    {
        StringBuffer serviceName;
        const char *value = it->queryAttribute(WaServiceName, serviceName);
        if (!isEmptyString(value))
            serviceNames.append(value);
    }
    info.setServiceNames(serviceNames);
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

    //The TotalClusterTime should always be returned between versions 1.27 and 1.73.
    //After version 1.73, it should be returned only if IncludeTotalClusterTime is true.
    if ((version > 1.27) && ((version < 1.73) || (flags & WUINFO_IncludeTotalClusterTime)))
    {
        unsigned totalThorTimeMS = cw->getTotalThorTime();
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
    getServiceNames(info, flags);
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
            IWARNLOG("Cannot find TargetClusterInfo for workunit %s", cw->queryWuid());
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
                    IWARNLOG("Invalid thorlog entry in workunit xml: %s", logName.str());
                    continue;
                }

                ppStr += 12;
                StringBuffer logDate(ppStr);
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
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
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
                    IWARNLOG("Invalid thorlog entry in workunit xml: %s", val.str());
                    continue;
                }

                const char* pProcessName = ppStr + logDir.length();
                char sep = pProcessName[0];
                StringBuffer processName(pProcessName + 1);
                ppStr = strchr(pProcessName + 1, sep);
                if (!ppStr)
                {
                    IWARNLOG("Invalid thorlog entry in workunit xml: %s", val.str());
                    continue;
                }
                processName.setLength(ppStr - pProcessName - 1);

                StringBuffer groupName;
                getClusterThorGroupName(groupName, processName.str());

                StringBuffer logDate(ppStr + 12);
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

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> root = &constEnv->getPTree();

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
        IERRLOG("%s", e->errorMessage(eMsg).str());
        info.setWorkflowsDesc(eMsg.str());
        e->Release();
    }
}

IDistributedFile* WsWuInfo::getLogicalFileData(IEspContext& context, const char* logicalName, bool& showFileContent)
{
    Owned<IDistributedFile> df = lookupLogicalName(context, logicalName, false, false, false, nullptr, defaultPrivilegedUser);
    if (!df)
        return nullptr;

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

                if (r.getResultIsAll())
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
        info.setResultsDesc(eMsg.str());
        e->Release();
    }
}

class FilteredStatisticsVisitor : public WuScopeVisitorBase
{
public:
    FilteredStatisticsVisitor(WsWuInfo & _wuInfo, bool _createDescriptions, IArrayOf<IEspWUStatisticItem>& _statistics, const StatisticsFilter& _statsFilter)
        : wuInfo(_wuInfo), statistics(_statistics), statsFilter(_statsFilter), createDescriptions(_createDescriptions) {}

    virtual void noteStatistic(StatisticKind curKind, unsigned __int64 value, IConstWUStatistic & cur) override
    {
        StringBuffer xmlBuf, tsValue;
        SCMStringBuffer curCreator, curDescription, curFormattedValue;

        StatisticCreatorType curCreatorType = cur.getCreatorType();
        StatisticScopeType curScopeType = cur.getScopeType();
        StatisticMeasure curMeasure = cur.getMeasure();
        unsigned __int64 count = cur.getCount();
        unsigned __int64 max = cur.getMax();
        unsigned __int64 ts = cur.getTimestamp();
        const char * curScope = cur.queryScope();
        cur.getCreator(curCreator);
        cur.getDescription(curDescription, createDescriptions);
        cur.getFormattedValue(curFormattedValue);

        Owned<IEspWUStatisticItem> wuStatistic = createWUStatisticItem();
        if (!statsFilter.matches(curCreatorType, curCreator.str(), curScopeType, curScope, curMeasure, curKind, value))
            return;

        if (wuInfo.version > 1.61)
            wuStatistic->setWuid(wuInfo.wuid);
        if (curCreatorType != SCTnone)
            wuStatistic->setCreatorType(queryCreatorTypeName(curCreatorType));
        if (curCreator.length())
            wuStatistic->setCreator(curCreator.str());
        if (curScopeType != SSTnone)
            wuStatistic->setScopeType(queryScopeTypeName(curScopeType));
        if (!isEmpty(curScope))
            wuStatistic->setScope(curScope);
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

protected:
    WsWuInfo & wuInfo;
    const StatisticsFilter& statsFilter;
    IArrayOf<IEspWUStatisticItem>& statistics;
    bool createDescriptions;
};

void WsWuInfo::getStats(const WuScopeFilter & filter, const StatisticsFilter& statsFilter, bool createDescriptions, IArrayOf<IEspWUStatisticItem>& statistics)
{
    FilteredStatisticsVisitor visitor(*this, createDescriptions, statistics, statsFilter);
    Owned<IConstWUScopeIterator> it = &cw->getScopeIterator(filter);
    ForEach(*it)
        it->playProperties(visitor);
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
        Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false, nullptr);
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
        IERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return false;
}

unsigned WsWuInfo::getResourceURLCount()
{
    try
    {
        Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false, nullptr);
        if (wv)
            return wv->getResourceURLCount();
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        IERRLOG("%s", e->errorMessage(eMsg).str());
        e->Release();
    }

    return 0;
}

void WsWuInfo::copyContentFromRemoteFile(const char* sourceFileName, const char* sourceIPAddress,
    const char* sourceAlias, const char *outFileName)
{
    RemoteFilename rfn;
    rfn.setRemotePath(sourceFileName);
    SocketEndpoint ep(sourceIPAddress);
    rfn.setIp(ep);

    OwnedIFile source = createIFile(rfn);
    if (!source)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open %s.", sourceAlias);

    OwnedIFile target = createIFile(outFileName);
    if (!target)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open %s.", outFileName);

    copyFile(target, source);
}

void WsWuInfo::readFileContent(const char* sourceFileName, const char* sourceIPAddress,
    const char* sourceAlias, MemoryBuffer &mb, bool forDownload)
{
    RemoteFilename rfn;
    rfn.setRemotePath(sourceFileName);
    SocketEndpoint ep(sourceIPAddress);
    rfn.setIp(ep);

    OwnedIFile source = createIFile(rfn);
    if (!source)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open %s.", sourceAlias);

    OwnedIFileIO sourceIO = source->openShared(IFOread,IFSHfull);
    if (!sourceIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Cannot open %s.", sourceAlias);

    offset_t len = source->size();
    if (!forDownload && (len > 640000))
        len = 640000;

    if (read(sourceIO, 0, len, mb) != len)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE, "Cannot read %s.", sourceAlias);
}

void WsWuInfo::getWorkunitEclAgentLog(const char* processName, const char* fileName, const char* agentPid, MemoryBuffer& buf, const char* outFile)
{
    if (isEmptyString(processName) && isEmptyString(fileName))
        throw makeStringException(ECLWATCH_ECLAGENT_LOG_NOT_FOUND, "Log file or process name has to be specified");

    Owned<IFileIOStream> outIOS;
    if (!isEmptyString(outFile))
    {
        CWsWuFileHelper helper(nullptr);
        outIOS.setown(helper.createIOStreamWithFileName(outFile, IFOcreate));
    }

    StringBuffer line;
    bool wuidFound = false;
    bool wuFinish = false;

    StringBuffer pidstr;
    if (agentPid && *agentPid)
        pidstr.appendf(" %s ", agentPid);
    else
        pidstr.appendf(" %5d ", cw->getAgentPID());

    char const* pidchars = pidstr.str();
    size32_t pidLen = pidstr.length();
    unsigned pidOffset = 0;//offset of PID in logfile entry

    //If a WU runs into another day, the WU information is stored in multiple log files.
    //Find out the logSpec for each log file based on the given processName or fileName.
    StringArray logSpecs;
    getWUProcessLogSpecs(processName, fileName, nullptr, true, logSpecs);
    ForEachItemIn(i, logSpecs)
    {
        if (wuFinish)
            break;

        Owned<IFile> rFile = createIFile(logSpecs.item(i));
        if (!rFile)
            throw makeStringExceptionV(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open file %s.", logSpecs.item(i));
        OwnedIFileIO rIO = rFile->openShared(IFOread,IFSHfull);
        if (!rIO)
            throw makeStringExceptionV(ECLWATCH_CANNOT_READ_FILE, "Cannot read file %s.", logSpecs.item(i));
        OwnedIFileIOStream ios = createIOStream(rIO);
        Owned<IStreamLineReader> lineReader = createLineReader(ios, true);

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
        while (!lineReader->readLine(line.clear()))
        {
            if (pidOffset > line.length())
                continue;
            //Retain all rows that match a unique program instance - by retaining all rows that match a pid
            const char* pPid = strstr(line.str() + pidOffset, pidchars);
            if (isEmptyString(pPid))
                continue;

            //Check if this is a new instance using line sequence number (PIDs are often reused)
            if (strncmp(line.str(), "00000000", 8) == 0)
            {
                if (wuidFound) //If the correct instance has been found, return that instance before the next instance.
                {
                    wuFinish = true;
                    break;
                }

                //The last instance is not a correct instance. Clean the buf in order to start a new instance.
                if (isEmptyString(outFile))
                    buf.clear();
            }

            //If we spot the workunit id anywhere in the tracing for this pid then assume it is the correct instance.
            if(!wuidFound && strstr(line.str(), wuid.str()))
            {
                pidOffset = pPid - line.str();//remember offset of PID within line
                wuidFound = true;
            }
            if (pidOffset && 0 == strncmp(line.str() + pidOffset, pidchars, pidLen))//this makes sure the match was the PID and not the TID or something else
                outputALine(line.length(), line.str(), buf, outIOS);
        }
    }

    if (!wuidFound)
    {
        const char * msg = "(No logfile entries found for this workunit)";
        outputALine(strlen(msg), msg, buf, outIOS);
    }
}

void WsWuInfo::getWorkunitThorMasterLog(const char* processName, const char* fileName, MemoryBuffer& buf, const char* outFile)
{
    readWorkunitThorLog(processName, fileName, nullptr, 0, buf, outFile);
}

void WsWuInfo::getWorkunitThorSlaveLog(IGroup *nodeGroup, const char *ipAddress, const char* processName, const char* logDate,
    const char* logDir, int slaveNum, MemoryBuffer& buf, const char* outFile, bool forDownload)
{
    if (isEmpty(logDir))
        throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave log path not specified.");

    StringBuffer slaveIPAddress;
    if (slaveNum > 0)
    {
        nodeGroup->queryNode(slaveNum-1).endpoint().getIpText(slaveIPAddress);
        if (slaveIPAddress.length() < 1)
            throw makeStringException(ECLWATCH_INVALID_INPUT, "ThorSlave log network address not found.");

        readWorkunitThorLog(processName, logDir, slaveIPAddress, slaveNum, buf, outFile);
    }
    else
    {//legacy wuid: a user types in an IP address for a thor slave
        if (isEmpty(logDate))
            throw makeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave log date not specified.");

        if (isEmpty(ipAddress))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"ThorSlave address not specified.");

        StringBuffer logName(logDir);
        addPathSepChar(logName);

        //thorslave.10.239.219.6_20100.2012_05_23.log
        logName.appendf("thorslave.%s*.%s.log", ipAddress, logDate);
        const char* portPtr = strchr(ipAddress, '_');
        if (!portPtr)
            slaveIPAddress.append(ipAddress);
        else
        {
            StringBuffer ipAddressStr(ipAddress);
            ipAddressStr.setLength(portPtr - ipAddress);
            slaveIPAddress.append(ipAddressStr.str());
        }
        readFileContent(logName, slaveIPAddress.str(), logName, buf, forDownload);
    }
}

void WsWuInfo::getWorkunitThorSlaveLog(IPropertyTree* directories, const char *process,
    const char* instanceName, const char *ipAddress, const char* logDate, int slaveNum,
    MemoryBuffer& buf, const char* outFile, bool forDownload)
{
    StringBuffer logDir, groupName;
    getConfigurationDirectory(directories, "log", "thor", process, logDir);
    getClusterThorGroupName(groupName, instanceName);
    if (groupName.isEmpty())
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Failed to get Thor Group Name for %s", instanceName);

    Owned<IGroup> nodeGroup = queryNamedGroupStore().lookup(groupName);
    if (!nodeGroup || (nodeGroup->ordinality() == 0))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Node group %s not found", groupName.str());

    getWorkunitThorSlaveLog(nodeGroup, ipAddress, process, logDate, logDir.str(), slaveNum, buf, outFile, forDownload);
}

void WsWuInfo::readWorkunitThorLog(const char* processName, const char* log, const char* slaveIPAddress, unsigned slaveNum, MemoryBuffer& buf, const char* outFile)
{
    Owned<IFileIOStream> outIOS;
    if (!isEmptyString(outFile))
    {
        CWsWuFileHelper helper(nullptr);
        outIOS.setown(helper.createIOStreamWithFileName(outFile, IFOcreate));
    }

    StringArray logSpecs;
    if (slaveIPAddress) //thor slave
        getWUProcessLogSpecs(processName, nullptr, log, false, logSpecs); //log: logDir
    else
        getWUProcessLogSpecs(processName, log, nullptr, false, logSpecs); //log: logSpec

    unsigned processID = 0; //The processID is unknown at the begining of the first day.
    ForEachItemIn(i, logSpecs)
    {
        const char* logSpec = logSpecs.item(i);

        Owned<IFile> rFile;
        if (slaveIPAddress)
        {
            StringBuffer thorMasterLog, ext;
            splitFilename(logSpec, nullptr, nullptr, &thorMasterLog, &ext);
    
            StringBuffer logSpecStr(log);
            addPathSepChar(logSpecStr);
            //Append the file name of the slave log to logSpecStr.
            //The pattern of the file name is: thorslave.SLAVENUM.LOGDATE.log.
            //ex. thorslave.2.2020_09_16.log
            //The LOGDATE is parsed from the thorMasterLog (ex. thormaster.2020_09_16).
            logSpecStr.appendf("thorslave.%u%s%s", slaveNum, thorMasterLog.str() + strlen("thormaster"), ext.str());

            RemoteFilename rfn;
            rfn.setRemotePath(logSpecStr);
            SocketEndpoint ep(slaveIPAddress);
            rfn.setIp(ep);
            rFile.setown(createIFile(rfn));
            if (!rFile)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open file %s on %s.", logSpecStr.str(), slaveIPAddress);
        }
        else
        {
            rFile.setown(createIFile(logSpec));
            if (!rFile)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Cannot open file %s.", logSpec);
        }
    
        readWorkunitThorLogOneDay(rFile, processID, buf, outIOS);
    }
}

void WsWuInfo::readWorkunitThorLogOneDay(IFile* sourceFile, unsigned& processID, MemoryBuffer& buf, IFileIOStream* outIOS)
{
    OwnedIFileIO sourceIO = sourceFile->openShared(IFOread,IFSHfull);
    if (!sourceIO)
        throw MakeStringException(ECLWATCH_CANNOT_READ_FILE,"Cannot read file %s.", sourceFile->queryFilename());

    Owned<IFileIOStream> ios = createIOStream(sourceIO);

    VStringBuffer startwuid("Started wuid=%s", wuid.str());
    VStringBuffer endwuid("Finished wuid=%s", wuid.str());

    StringBuffer line;

    Owned<IStreamLineReader> lineReader = createLineReader(ios, true);
    bool eof = lineReader->readLine(line.clear());
    if (eof)
        return;

    // Process header for log file format
    unsigned logfields = getMessageFieldsFromHeader(line);
    if (logfields==0)   // No header line, so must be in legacy format
        logfields = MSGFIELD_LEGACY;
    else
        eof = lineReader->readLine(line.clear());

    const unsigned columnNumPID = getPositionOfField(logfields, MSGFIELD_process);
    bool outputThisLine = false;
    if (processID > 0) //after the 1st page of the log
        outputThisLine = true;
    bool foundEndWUID = false; 
    while (!eof)
    {
        if (outputThisLine)
        {
            //If the slave is restarted before WU is finished, we cannot find out the "Finished wuid=...".
            //So, we should check whether the slave is restarting or not.
            unsigned pID = 0;
            foundEndWUID = parseLogLine(line.str(), endwuid, pID, columnNumPID);
            if ((pID > 0) && (pID != processID))
                break;
            outputALine(line.length(), line.str(), buf, outIOS);
            if (foundEndWUID)
                outputThisLine = false;
        }
        else if (strstr(line.str(), startwuid))
        {
            outputThisLine = true;
            foundEndWUID = false;
            outputALine(line.length(), line.str(), buf, outIOS);
            if (processID == 0)
                parseLogLine(line.str(), nullptr, processID, columnNumPID);
        }
        eof = lineReader->readLine(line.clear());
    }
}

bool WsWuInfo::parseLogLine(const char* line, const char* endWUID, unsigned& processID, const unsigned columnNumPID)
{
    const char* bptr = line;
    for (unsigned cur=0;  cur < columnNumPID && *bptr; ++cur)
    {
        while(*bptr && *bptr!=' ') ++bptr;  // Skip field
        while(*bptr && *bptr==' ') ++bptr;  // Skip spaces
    }
    if (!*bptr) return false;

    const char* eptr = bptr + 1;
    while (*eptr && isdigit(*eptr))     //Read ProcessID
        eptr++;

    if (*eptr != ' ')
        return false;

    processID = (unsigned) atoi_l(bptr, eptr - bptr);

    return (endWUID && strstr(eptr+1, endWUID));
}

void WsWuInfo::getWUProcessLogSpecs(const char* processName, const char* logSpec, const char* logDir, bool eclAgent, StringArray& logSpecs)
{
    Owned<IStringIterator> logs;
    if (eclAgent)
        logs.setown(cw->getLogs("EclAgent"));
    else
    { //Thor
        if (!isEmptyString(processName))
            logs.setown(cw->getLogs("Thor", processName));
        else
        {
            //Parse the process name from the logSpec or logDir.
            if (isEmptyString(logSpec) && isEmptyString(logDir))
                throw makeStringException(ECLWATCH_ECLAGENT_LOG_NOT_FOUND, "Process name and log file not specified");
    
            StringBuffer path, process;
            if (!isEmptyString(logDir))
                path.set(logDir);
            else
            {
                //Parse the path from the logSpec (ex.: //10.173.123.208/mnt/disk1/var/log/HPCCSystems/mythor/thormaster.2020_01_16.log)
                splitFilename(logSpec, nullptr, &path, nullptr, nullptr);
            }

            //Parse the process name (ex. mythor) from the path (ex.: //10.173.123.208/mnt/disk1/var/log/HPCCSystems/mythor/)
            removeTrailingPathSepChar(path);
            splitFilename(path, nullptr, nullptr, &process, nullptr);
            logs.setown(cw->getLogs("Thor", process));
        }
    }

    ForEach (*logs)
    {
        SCMStringBuffer logStr;
        logs->str(logStr);
        if (logStr.length() < 1)
            continue;

        logSpecs.append(logStr.str());
    }
    if (logSpecs.length() > 1)
        logSpecs.sortAscii(false); //Sort the logSpecs from old to new
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
    Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, NULL, false, nullptr);
    if (wv)
        return wv->getEmbeddedQuery();

    return NULL;
}

void WsWuInfo::getWorkunitArchiveQuery(IStringVal& str)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    query->getQueryText(str);
    if ((str.length() < 1) || !isArchiveQuery(str.str()))
    {
        if (!query->hasArchive())
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Archive query not found for workunit %s.", wuid.str());

        Owned<IConstWUQuery> embeddedQuery = getEmbeddedQuery();
        if (!embeddedQuery)
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Embedded query not found for workunit %s.", wuid.str());

        embeddedQuery->getQueryText(str);
        if ((str.length() < 1) || !isArchiveQuery(str.str()))
            throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Archive query not found for workunit %s.", wuid.str());
    }
}

void WsWuInfo::getWorkunitArchiveQuery(StringBuffer& buf)
{
    StringBufferAdaptor queryText(buf);
    getWorkunitArchiveQuery(queryText);
}

void WsWuInfo::getWorkunitArchiveQuery(MemoryBuffer& buf)
{
    SCMStringBuffer queryText;
    getWorkunitArchiveQuery(queryText);
    buf.append(queryText.length(), queryText.str());
}

void WsWuInfo::getWorkunitQueryShortText(MemoryBuffer& buf, const char* outFile)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    SCMStringBuffer queryText;
    query->getQueryShortText(queryText);
    if (queryText.length() < 1)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU, "No query for workunit %s.",wuid.str());
    if (isEmptyString(outFile))
        buf.append(queryText.length(), queryText.str());
    else
    {
        CWsWuFileHelper helper(nullptr);
        Owned<IFileIOStream> outIOS = helper.createIOStreamWithFileName(outFile, IFOcreate);
        outIOS->write(queryText.length(), queryText.str());
    }
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

void WsWuInfo::getWorkunitCpp(const char* cppname, const char* description, const char* ipAddress, MemoryBuffer& buf, bool forDownload, const char* outFile)
{
    if (isEmpty(description))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");
    if (isEmpty(ipAddress))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File location not specified.");
    if (isEmpty(cppname))
        throw MakeStringException(ECLWATCH_INVALID_FILE_NAME, "File path not specified.");

    if (isEmpty(outFile))
        readFileContent(cppname, ipAddress, description, buf, forDownload);
    else
        copyContentFromRemoteFile(cppname, ipAddress, description, outFile);
}

void WsWuInfo::getWorkunitAssociatedXml(const char* name, const char* ipAddress, const char* plainText,
    const char* description, bool forDownload, bool addXMLDeclaration, MemoryBuffer& buf, const char* outFile)
{
    if (isEmpty(description)) //'File Name' as shown in WU Details page
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File not specified.");
    if (isEmpty(ipAddress))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "File location not specified.");
    if (isEmpty(name)) //file name with full path
        throw MakeStringException(ECLWATCH_INVALID_FILE_NAME, "File path not specified.");

    if (addXMLDeclaration)
    {
        const char* header;
        if (plainText && (!stricmp(plainText, "yes")))
            header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
        else
            header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";
        buf.append(strlen(header), header);
    }

    if (isEmpty(outFile))
        readFileContent(name, ipAddress, description, buf, forDownload);
    else
        copyContentFromRemoteFile(name, ipAddress, description, outFile);
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
    getWorkunitAssociatedXml(name.str(), ip.str(), "", "WU archive eclxml", true, false, content, nullptr);
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

void WsWuInfo::outputALine(size32_t length, const char* content, MemoryBuffer& outputBuf, IFileIOStream* outIOS)
{
    if (outIOS)
        outIOS->write(length, content);
    else
        outputBuf.append(length, content);
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
        if (!checkSameStrings(tq->getWaitingThors(), tq0.getWaitingThors()))
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
    PROGLOG("ECLWorkunit WUSchedule Thread started.");
    unsigned int waitTimeMillies = 1000*60;
    while(!stopping)
    {
        if (!m_container)
        {
            DBGLOG("ECLWorkunit WUSchedule Thread is waiting for container to be set.");
        }
        else if (!detached)
        {
            try
            {
                if (waitTimeMillies == (unsigned)-1)
                {
                    PROGLOG("ECLWorkunit WUSchedule Thread Re-started.");
                    waitTimeMillies = 1000*60;
                }

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
                            {
                                runWorkUnit(cw.queryWuid(), cw.queryClusterName());
                                if (m_container->hasCacheClient())
                                {
                                    StringArray errorMsgs;
                                    m_container->clearCacheByGroupID("ESPWsWUs", errorMsgs);
                                    if (errorMsgs.length() > 0)
                                    {
                                        ForEachItemIn(i, errorMsgs)
                                            DBGLOG("%s", errorMsgs.item(i));
                                    }
                                }
                            }
                        }
                        catch(IException *e)
                        {
                            StringBuffer msg;
                            IERRLOG("Exception %d:%s in WsWorkunits Schedule::run while processing WU", e->errorCode(), e->errorMessage(msg).str());
                            e->Release();
                        }
                    }
                }
            }
            catch(IException *e)
            {
                StringBuffer msg;
                IERRLOG("Exception %d:%s in WsWorkunits Schedule::run while fetching scheduled WUs from DALI", e->errorCode(), e->errorMessage(msg).str());
                e->Release();
            }
            catch(...)
            {
                IERRLOG("Unknown exception in WsWorkunits Schedule::run while fetching scheduled WUs from DALI");
            }
        }
        else
        {
            OWARNLOG("Detached from DALI, WSWorkunits schedule interrupted");
            waitTimeMillies = (unsigned)-1;
        }

        semSchedule.wait(waitTimeMillies);
    }
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
            StringBuffer expanded;
            if (*name=='-')
                name=expanded.append("eclcc").append(name).str();
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
    submitWsWorkunit(context, cw, cluster, snapshot, maxruntime, compile, resetWorkflow, resetVariables, paramXml, variables, debugs, applications);
}


void WsWuHelpers::copyWsWorkunit(IEspContext &context, IWorkUnit &wu, const char *srcWuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> src(factory->openWorkUnit(srcWuid));

    queryExtendedWU(&wu)->copyWorkUnit(src, false, false);

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

IFileIOStream* CWsWuFileHelper::createIOStreamWithFileName(const char* fileNameWithPath, IFOmode mode)
{
    if (isEmptyString(fileNameWithPath))
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA, "File name not specified.");
    Owned<IFile> wuInfoIFile = createIFile(fileNameWithPath);
    Owned<IFileIO> wuInfoIO = wuInfoIFile->open(mode);
    if (!wuInfoIO)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE, "Failed to open %s.", fileNameWithPath);
    return createIOStream(wuInfoIO);
}

void CWsWuFileHelper::writeToFile(const char* fileName, size32_t contentLength, const void* content)
{
    if (isEmptyString(fileName))
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA, "File name not specified.");
    Owned<IFile> wuInfoIFile = createIFile(fileName);
    Owned<IFileIO> wuInfoIO = wuInfoIFile->open(IFOcreate);
    if (wuInfoIO)
        wuInfoIO->write(0, contentLength, content);
    else
        PROGLOG("Failed to open %s.", fileName);
}

void CWsWuFileHelper::writeToFileIOStream(const char* folder, const char* file, MemoryBuffer& mb)
{
    if (isEmptyString(folder))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty folder name is not allowed to create FileIOStream.");
    if (isEmptyString(file))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty file name is not allowed to create FileIOStream.");
    VStringBuffer fileNameWithPath("%s%c%s", folder, PATHSEPCHAR, file);
    CWsWuFileHelper helper(nullptr);
    Owned<IFileIOStream> outIOS = helper.createIOStreamWithFileName(fileNameWithPath.str(), IFOcreate);
    if (outIOS)
        outIOS->write(mb.length(), mb.toByteArray());
    else
        PROGLOG("Failed to open FileIOStream for %s.", fileNameWithPath.str());
}

void CWsWuFileHelper::cleanFolder(IFile* folder, bool removeFolder)
{
    if (!folder)
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"Invalid file interface for the zip folder.");
    Owned<IDirectoryIterator> iter = folder->directoryFiles(NULL,false,false);
    ForEach(*iter)
    {
        OwnedIFile thisFile = createIFile(iter->query().queryFilename());
        if (thisFile->isFile() == fileBool::foundYes)
            thisFile->remove();
    }
    if (removeFolder)
        folder->remove();
}

void CWsWuFileHelper::createProcessLogfile(IConstWorkUnit* cwu, WsWuInfo& winfo, const char* process, const char* path)
{
    BoolHash uniqueProcesses;
    Owned<IPropertyTreeIterator> procs = cwu->getProcesses(process, NULL);
    ForEach (*procs)
    {
        StringBuffer logSpec;
        IPropertyTree& proc = procs->query();
        const char* processName = proc.queryName();
        if (isEmpty(processName))
            continue;

        //If a WU runs into another day, the procs contains >1 entries for the same process.
        //Only one entry is needed to find out the process data for creating the new process
        //log file which stores the WU information for multiple days.
        bool* found = uniqueProcesses.getValue(processName);
        if (found && *found)
            continue;
        uniqueProcesses.setValue(processName, true);

        MemoryBuffer mb;
        VStringBuffer fileName("%s%c%s", path, PATHSEPCHAR, processName);
        try
        {
            if (strieq(process, "EclAgent"))
            {
                StringBuffer pid;
                pid.appendf("%d", proc.getPropInt("@pid"));

                fileName.append("_eclagent.log");
                winfo.getWorkunitEclAgentLog(processName, nullptr, pid.str(), mb, fileName.str());
            }
            else if (strieq(process, "Thor"))
            {
                fileName.append("_thormaster.log");
                winfo.getWorkunitThorMasterLog(processName, nullptr, mb, fileName.str());
            }
        }
        catch(IException* e)
        {
            StringBuffer s;
            e->errorMessage(s);
            IERRLOG("Error accessing Process Log file %s: %s", logSpec.str(), s.str());
            writeToFile(fileName.str(), s.length(), s.str());
            e->Release();
        }
    }
}

void CWsWuFileHelper::createThorSlaveLogfile(IConstWorkUnit* cwu, WsWuInfo& winfo, const char* path)
{
    if (cwu->getWuidVersion() == 0)
        return;
    const char* clusterName = cwu->queryClusterName();
    if (isEmptyString(clusterName)) //Cluster name may not be set yet
        return;
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName);
    if (!clusterInfo)
    {
        OWARNLOG("Cannot find TargetClusterInfo for workunit %s", cwu->queryWuid());
        return;
    }

    Owned<IThreadFactory> threadFactory = new CGetThorSlaveLogToFileThreadFactory();
    Owned<IThreadPool> threadPool = createThreadPool("WsWuFileHelper GetThorSlaveLogToFile Thread Pool",
        threadFactory, NULL, thorSlaveLogThreadPoolSize, INFINITE);

    unsigned numberOfSlaveLogs = clusterInfo->getNumberOfSlaveLogs();
    BoolHash uniqueProcesses;
    Owned<IStringIterator> thorInstances = cwu->getProcesses("Thor");
    ForEach (*thorInstances)
    {
        SCMStringBuffer processName;
        thorInstances->str(processName);
        if (processName.length() == 0)
            continue;

        bool* found = uniqueProcesses.getValue(processName.str());
        if (found && *found)
            continue;
        uniqueProcesses.setValue(processName.str(), true);

        StringBuffer groupName, logDir;
        getClusterThorGroupName(groupName, processName.str());
        if (groupName.isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Failed to get Thor Group Name for %s", processName.str());

        Owned<IGroup> nodeGroup = queryNamedGroupStore().lookup(groupName);
        if (!nodeGroup || (nodeGroup->ordinality() == 0))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Node group %s not found", groupName.str());

        getConfigurationDirectory(directories, "log", "thor", processName.str(), logDir);
        for (unsigned i = 0; i < numberOfSlaveLogs; i++)
        {
            VStringBuffer fileName("%s%c%s_thorslave.%u.log", path, PATHSEPCHAR, processName.str(), i+1);
            Owned<CGetThorSlaveLogToFileThreadParam> threadParam = new CGetThorSlaveLogToFileThreadParam(
                &winfo, nodeGroup, processName.str(), logDir, i+1, fileName);
            threadPool->start(threadParam.getClear());
        }
    }
    threadPool->joinAll();
}

void CWsWuFileHelper::createZAPInfoFile(const char* url, const char* espIP, const char* thorIP, const char* problemDesc,
    const char* whatChanged, const char* timing, IConstWorkUnit* cwu, const char* pathNameStr)
{
    VStringBuffer fileName("%s.txt", pathNameStr);
    Owned<IFileIOStream> outFile = createIOStreamWithFileName(fileName.str(), IFOcreate);

    StringBuffer sb;
    sb.set("Workunit:     ").append(cwu->queryWuid()).append("\r\n");
    sb.append("User:         ").append(cwu->queryUser()).append("\r\n");
    sb.append("Build Version:").append(getBuildVersion()).append("\r\n");
    sb.append("Cluster:      ").append(cwu->queryClusterName()).append("\r\n");
    if (!isEmptyString(espIP))
        sb.append("ESP:          ").append(espIP).append("\r\n");
    else
    {
        StringBuffer espIPAddr;
        IpAddress ipaddr = queryHostIP();
        ipaddr.getIpText(espIPAddr);
        sb.append("ESP:          ").append(espIPAddr.str()).append("\r\n");
    }
    if (!isEmptyString(url))
        sb.append("URL:          ").append(url).append("\r\n");
    if (!isEmptyString(thorIP))
        sb.append("Thor:         ").append(thorIP).append("\r\n");
    outFile->write(sb.length(), sb.str());

    //Exceptions/Warnings/Info
    Owned<IConstWUExceptionIterator> exceptions = &cwu->getExceptions();
    StringBuffer info, warn, err, alert;
    ForEach(*exceptions)
    {
        const char* severityString = nullptr;
        switch (exceptions->query().getSeverity())
        {
        case SeverityInformation:
            severityString = "Information:  ";
            break;
        case SeverityWarning:
            severityString = "Warnings:     ";
            break;
        case SeverityError:
            severityString = "Exceptions:   ";
            break;
        case SeverityAlert:
            severityString = "Alert:        ";
            break;
        }

        if (!severityString)
            continue;

        SCMStringBuffer temp;
        exceptions->query().getExceptionMessage(temp);
        writeZAPWUInfoToIOStream(outFile, severityString, temp);
    }

    //User provided Information
    writeZAPWUInfoToIOStream(outFile, "Problem:      ", problemDesc);
    writeZAPWUInfoToIOStream(outFile, "What Changed: ", whatChanged);
    writeZAPWUInfoToIOStream(outFile, "Timing:       ", timing);
}

void CWsWuFileHelper::writeZAPWUInfoToIOStream(IFileIOStream* outFile, const char* name, SCMStringBuffer& value)
{
    outFile->write(strlen(name), name);
    outFile->write(3, "\r\n\t");
    outFile->write(value.length(), value.str());
    outFile->write(4, "\r\n\r\n");
}

void CWsWuFileHelper::writeZAPWUInfoToIOStream(IFileIOStream* outFile, const char* name, const char* value)
{
    outFile->write(strlen(name), name);
    outFile->write(strlen(value), value);
    outFile->write(4, "\r\n\r\n");
}

void CWsWuFileHelper::createZAPWUXMLFile(WsWuInfo& winfo, const char* pathNameStr)
{
    MemoryBuffer mb;
    winfo.getWorkunitXml(NULL, mb);
    VStringBuffer fileName("%s.xml", pathNameStr);
    writeToFile(fileName.str(), mb.length(), mb.bufferBase());
}

void CWsWuFileHelper::createZAPECLQueryArchiveFiles(IConstWorkUnit* cwu, const char* pathNameStr)
{
    Owned<IConstWUQuery> query = cwu->getQuery();
    if(!query)
        return;

    //Add archive if present
    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        IConstWUAssociatedFile& cur = iter->query();
        SCMStringBuffer ssb, ip;
        cur.getDescription(ssb);
        if (!strieq(ssb.str(), "archive"))
            continue;

        cur.getName(ssb);
        cur.getIp(ip);
        if (!ssb.length() || !ip.length())
            continue;

        StringBuffer fileName, archiveContents;
        try
        {
            SocketEndpoint ep(ip.str());
            RemoteFilename rfn;
            rfn.setRemotePath(ssb.str());
            rfn.setIp(ep);
            Owned<IFile> rFile = createIFile(rfn);
            if (!rFile)
            {
                OERRLOG("Cannot open %s on %s", ssb.str(), ip.str());
                continue;
            }
            archiveContents.loadFile(rFile);
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            OERRLOG("Error accessing archive file %s: %s", ssb.str(), s.str());
            archiveContents.insert(0, "Error accessing archive file ").appendf("%s: %s\r\n\r\n", ssb.str(), s.str());
            e->Release();
        }
        fileName.setf("%s.archive", pathNameStr);
        writeToFile(fileName.str(), archiveContents.length(), archiveContents.str());
        break;
    }

    //Add Query
    SCMStringBuffer temp;
    query->getQueryText(temp);
    if (temp.length())
    {
        VStringBuffer fileName("%s.ecl", pathNameStr);
        writeToFile(fileName.str(), temp.length(), temp.str());
    }
}

void CWsWuFileHelper::createZAPWUGraphProgressFile(const char* wuid, const char* pathNameStr)
{
    Owned<IPropertyTree> graphProgress = getWUGraphProgress(wuid, true);
    if (!graphProgress)
        return;

    StringBuffer graphProgressXML;
    toXML(graphProgress, graphProgressXML, 1, XML_Format);

    VStringBuffer fileName("%s.graphprogress", pathNameStr);
    writeToFile(fileName.str(), graphProgressXML.length(), graphProgressXML.str());
}

int CWsWuFileHelper::zipAFolder(const char* folder, const char* passwordReq, const char* zipFileNameWithPath)
{
    VStringBuffer archiveInPath("%s%c*", folder, PATHSEPCHAR);
    StringBuffer zipCommand;
    if (!isEmptyString(passwordReq))
        zipCommand.setf("zip -j --password %s %s %s", passwordReq, zipFileNameWithPath, archiveInPath.str());
    else
        zipCommand.setf("zip -j %s %s", zipFileNameWithPath, archiveInPath.str());
    return (system(zipCommand.str()));
}

int CWsWuFileHelper::zipAFolder(const char* folder, bool gzip, const char* zipFileNameWithPath)
{
    StringBuffer zipCommand;
    VStringBuffer archiveInPath("%s%c*", folder, PATHSEPCHAR);
    if (!gzip)
        zipCommand.appendf("zip -j %s %s", zipFileNameWithPath, archiveInPath.str());
    else
        zipCommand.appendf("tar -czf %s %s", zipFileNameWithPath, archiveInPath.str());
    return (system(zipCommand.str()));
}

void CWsWuFileHelper::createWUZAPFile(IEspContext& context, IConstWorkUnit* cwu, CWsWuZAPInfoReq& request,
    StringBuffer& zipFileName, StringBuffer& zipFileNameWithPath, unsigned _thorSlaveLogThreadPoolSize)
{
    StringBuffer zapReportNameStr, folderToZIP, inFileNamePrefixWithPath;
    Owned<IFile> zipDir = createWorkingFolder(context, request.wuid.str(), "ZAPReport_", zapReportNameStr, folderToZIP);
    setZAPFile(request.zapFileName.str(), zapReportNameStr.str(), zipFileName, zipFileNameWithPath);
    thorSlaveLogThreadPoolSize = _thorSlaveLogThreadPoolSize;

    //create WU ZAP files
    inFileNamePrefixWithPath.set(folderToZIP.str()).append(PATHSEPCHAR).append(zapReportNameStr.str());
    createZAPInfoFile(request.url.str(), request.espIP.str(), request.thorIP.str(), request.problemDesc.str(), request.whatChanged.str(),
        request.whereSlow.str(), cwu, inFileNamePrefixWithPath.str());
    createZAPECLQueryArchiveFiles(cwu, inFileNamePrefixWithPath.str());

    WsWuInfo winfo(context, cwu);
    createZAPWUXMLFile(winfo, inFileNamePrefixWithPath.str());
    createZAPWUGraphProgressFile(request.wuid.str(), inFileNamePrefixWithPath.str());
    createZAPWUQueryAssociatedFiles(cwu, folderToZIP);
    createProcessLogfile(cwu, winfo, "EclAgent", folderToZIP.str());
    createProcessLogfile(cwu, winfo, "Thor", folderToZIP.str());
    if (request.includeThorSlaveLog.isEmpty() || strieq(request.includeThorSlaveLog.str(), "on"))
        createThorSlaveLogfile(cwu, winfo, folderToZIP.str());

    //Write out to ZIP file
    int zipRet = zipAFolder(folderToZIP.str(), request.password.str(), zipFileNameWithPath);
    //Remove the temporary files and the folder
    cleanFolder(zipDir, true);
    if (zipRet != 0)
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"Failed to execute system command 'zip'. Please make sure that zip utility is installed.");
}

void CWsWuFileHelper::createZAPWUQueryAssociatedFiles(IConstWorkUnit* cwu, const char* pathToCreate)
{
    Owned<IConstWUQuery> query = cwu->getQuery();
    if (!query)
    {
        IERRLOG("Cannot get Query for workunit %s.", cwu->queryWuid());
        return;
    }

    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        SCMStringBuffer name, ip;
        IConstWUAssociatedFile& cur = iter->query();
        cur.getName(name);
        cur.getIp(ip);

        RemoteFilename rfn;
        SocketEndpoint ep(ip.str());
        rfn.setPath(ep, name.str());

        OwnedIFile sourceFile = createIFile(rfn);
        if (!sourceFile)
        {
            IERRLOG("Cannot open %s on %s.", name.str(), ip.str());
            continue;
        }

        StringBuffer fileName(name.str());
        getFileNameOnly(fileName, false);

        StringBuffer outFileName(pathToCreate);
        outFileName.append(PATHSEPCHAR).append(fileName);

        OwnedIFile outFile = createIFile(outFileName);
        if (!outFile)
        {
            IERRLOG("Cannot create %s.", outFileName.str());
            continue;
        }

        copyFile(outFile, sourceFile);
    }
}

void CWsWuFileHelper::setZAPFile(const char* zipFileNameReq, const char* zipFileNamePrefix,
    StringBuffer& zipFileName, StringBuffer& zipFileNameWithPath)
{
    StringBuffer outFileNameReq(zipFileNameReq);
    //Clean zipFileNameReq. The zipFileNameReq should not end with PATHSEPCHAR.
    while (!outFileNameReq.isEmpty() && (outFileNameReq.charAt(outFileNameReq.length() - 1) == PATHSEPCHAR))
        outFileNameReq.setLength(outFileNameReq.length() - 1);

    if (outFileNameReq.isEmpty())
        zipFileName.set(zipFileNamePrefix).append(".zip");
    else
    {
        zipFileName.set(outFileNameReq.str());
        const char* ext = pathExtension(zipFileName.str());
        if (!ext || !strieq(ext, ".zip"))
            zipFileName.append(".zip");
    }

    zipFileNameWithPath.set(zipFolder);
    Owned<IFile> workingDir = createIFile(zipFileNameWithPath.str());
    if (!workingDir->exists())
        workingDir->createDirectory();

    zipFileNameWithPath.append(PATHSEPCHAR).append(zipFileName);
    OwnedIFile thisFile = createIFile(zipFileNameWithPath.str());
    if (thisFile->isFile() == fileBool::foundYes)
        thisFile->remove();
}

IFile* CWsWuFileHelper::createWorkingFolder(IEspContext& context, const char* wuid, const char* namePrefix,
    StringBuffer& namePrefixStr, StringBuffer& folderName)
{
    StringBuffer userName;
    if (context.queryUser())
        userName.append(context.queryUser()->getName());
    namePrefixStr.set(namePrefix).append(wuid).append('_').append(userName.str());
    folderName.append(zipFolder).append(namePrefixStr.str());
    Owned<IFile> workingDir = createIFile(folderName.str());
    if (!workingDir->exists())
        workingDir->createDirectory();
    else
        cleanFolder(workingDir, false);
    return workingDir.getClear();
}

IFileIOStream* CWsWuFileHelper::createWUZAPFileIOStream(IEspContext& context, IConstWorkUnit* cwu,
    CWsWuZAPInfoReq& request, unsigned thorSlaveLogThreadPoolSize)
{
    StringBuffer zapFileName, zapFileNameWithPath;
    createWUZAPFile(context, cwu, request, zapFileName, zapFileNameWithPath, thorSlaveLogThreadPoolSize);

    if (request.sendEmail)
    {
        CWsWuEmailHelper emailHelper(request.emailFrom.str(), request.emailTo.str(), request.emailServer.str(), request.port);

        StringBuffer subject(request.emailSubject.str());
        if (subject.isEmpty())
            subject.append(request.wuid.str()).append(" ZAP Report");
        emailHelper.setSubject(subject.str());

        PROGLOG("Sending WU ZAP email (%s): from %s to %s", request.emailServer.str(), request.emailFrom.str(), request.emailTo.str());

        StringArray warnings;
        if (!request.attachZAPReportToEmail)
            emailHelper.send(request.emailBody.str(), "", 0, warnings);
        else
        {
            Owned<IFile> f = createIFile(zapFileNameWithPath.str());
            Owned<IFileIO> io = f->open(IFOread);
            unsigned zapFileSize = (unsigned) io->size();
            if (zapFileSize > request.maxAttachmentSize)
            {
                request.emailBody.appendf("\n\n(Failed to attach the ZAP report. The size limit is %u bytes.)", request.maxAttachmentSize);
                emailHelper.send(request.emailBody.str(), "", 0, warnings);
            }
            else
            {
                MemoryBuffer mb;
                void * data = mb.reserve(zapFileSize);
                size32_t read = io->read(0, zapFileSize, data);
                mb.setLength(read);

                emailHelper.setAttachmentName(zapFileName.str());
                emailHelper.setMimeType("application/zip, application/octet-stream");
                emailHelper.send(request.emailBody.str(), mb.toByteArray(), mb.length(), warnings);
            }
        }
    }

    VStringBuffer headerStr("attachment;filename=%s", zapFileName.str());
    context.addCustomerHeader("Content-disposition", headerStr.str());
    return createIOStreamWithFileName(zapFileNameWithPath.str(), IFOread);
}

IFileIOStream* CWsWuFileHelper::createWUFileIOStream(IEspContext& context, const char* wuid, IArrayOf<IConstWUFileOption>& wuFileOptions,
    CWUFileDownloadOption& downloadOptions, StringBuffer& contentType)
{
    StringBuffer fileName, fileNameStr, workingFolder, zipFileNameWithPath;
    Owned<IFile> zipDir;
    bool doZIP = (downloadOptions == CWUFileDownloadOption_ZIP) || (downloadOptions == CWUFileDownloadOption_GZIP);
    if (doZIP)
    {
        zipDir.setown(createWorkingFolder(context, wuid, "WUFiles_", fileNameStr, workingFolder));
    }
    else
    {
        StringBuffer userName;
        if (context.queryUser())
            userName.append(context.queryUser()->getName());
        fileName.set("WUFiles_").append(wuid).append('_').append(userName.str());
    }

    WsWuInfo winfo(context, wuid);
    ForEachItemIn(i, wuFileOptions)
    {
        if (!doZIP)
        {//If no zip, only return one file. If > 1 files, the caller throws exception.
            readWUFile(wuid, zipFolder, winfo, wuFileOptions.item(i), fileName, contentType);
            break;
        }

        StringBuffer aFileName, aFileMimeType;//Not used
        readWUFile(wuid, workingFolder.str(), winfo, wuFileOptions.item(i), aFileName, aFileMimeType);
    }
    if (!doZIP)
    {
        if (downloadOptions != CWUFileDownloadOption_OriginalText)
        {
            VStringBuffer headerStr("attachment;filename=%s", fileName.str());
            context.addCustomerHeader("Content-disposition", headerStr.str());
        }

        zipFileNameWithPath.set(zipFolder).append(fileName.str());
        return createIOStreamWithFileName(zipFileNameWithPath.str(), IFOread);
    }

    if (downloadOptions == CWUFileDownloadOption_ZIP)
        fileName.set(fileNameStr).append(".zip");
    else
        fileName.set(fileNameStr).append(".gzip");
    zipFileNameWithPath.set(zipFolder).append(fileName.str());
    {
        OwnedIFile oldFile = createIFile(zipFileNameWithPath.str());
        if (oldFile->isFile() == fileBool::foundYes)
            oldFile->remove();
    }
    int zipRet = zipAFolder(workingFolder.str(), downloadOptions == CWUFileDownloadOption_GZIP, zipFileNameWithPath);
    //Remove the temporary files and the folder
    cleanFolder(zipDir, true);
    if (zipRet != 0)
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA, "Failed to execute system command 'zip'. Please make sure that zip utility is installed.");

    contentType.set(HTTP_TYPE_OCTET_STREAM);
    VStringBuffer headerStr("attachment;filename=%s", fileName.str());
    context.addCustomerHeader("Content-disposition", headerStr.str());
    return createIOStreamWithFileName(zipFileNameWithPath.str(), IFOread);
}

void CWsWuFileHelper::readWUFile(const char* wuid, const char* workingFolder, WsWuInfo& winfo, IConstWUFileOption& item,
    StringBuffer& fileName, StringBuffer& fileMimeType)
{
    MemoryBuffer mb;
    StringBuffer fileNameWithPath;
    CWUFileType fileType = item.getFileType();
    switch (fileType)
    {
    case CWUFileType_ArchiveQuery:
        winfo.getWorkunitArchiveQuery(mb);
        fileName.set("ArchiveQuery.xml");
        fileMimeType.set(HTTP_TYPE_APPLICATION_XML);
        writeToFileIOStream(workingFolder, "ArchiveQuery.xml", mb);
        break;
    case CWUFileType_CPP:
    case CWUFileType_LOG:
    {
        const char *tail=pathTail(item.getName());
        fileName.set(tail ? tail : item.getName());
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
        winfo.getWorkunitCpp(item.getName(), item.getDescription(), item.getIPAddress(), mb, true, fileNameWithPath.str());
        break;
    }
    case CWUFileType_DLL:
    {
        const char *tail=pathTail(item.getName());
        fileName.set(tail ? tail : item.getName());
        fileMimeType.set(HTTP_TYPE_OCTET_STREAM);
        StringBuffer name;
        winfo.getWorkunitDll(name, mb);
        writeToFileIOStream(workingFolder, fileName.str(), mb);
        break;
    }
    case CWUFileType_Res:
        fileName.set("res.txt");
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        winfo.getWorkunitResTxt(mb);
        writeToFileIOStream(workingFolder, fileName.str(), mb);
        break;
    case CWUFileType_ThorLog:
        fileName.set("thormaster.log");
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
        winfo.getWorkunitThorMasterLog(nullptr, item.getName(), mb, fileNameWithPath.str());
        break;
    case CWUFileType_ThorSlaveLog:
    {
        fileName.set("ThorSlave.log");
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
        winfo.getWorkunitThorSlaveLog(directories, item.getProcess(), item.getClusterGroup(), item.getIPAddress(),
            item.getLogDate(), item.getSlaveNumber(), mb, fileNameWithPath.str(), false);

        break;
    }
    case CWUFileType_EclAgentLog:
        fileName.set("eclagent.log");
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
        winfo.getWorkunitEclAgentLog(nullptr, item.getName(), item.getProcess(), mb, fileNameWithPath.str());
        break;
    case CWUFileType_XML:
    {
        StringBuffer name(item.getName());
        if (!name.isEmpty())
        {
            const char *tail=pathTail(name.str());
            fileName.set(tail ? tail : name.str());
            fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
            winfo.getWorkunitAssociatedXml(fileName.str(), item.getIPAddress(), item.getPlainText(), item.getDescription(), true, true, mb, fileNameWithPath.str());
        }
        else
        {
            fileName.setf("%s.xml", wuid);
            winfo.getWorkunitXml(item.getPlainText(), mb);
            writeToFileIOStream(workingFolder, fileName.str(), mb);
        }
        const char* plainText = item.getPlainText();
        if (plainText && strieq(plainText, "yes"))
            fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        else
            fileMimeType.set(HTTP_TYPE_APPLICATION_XML);
        break;
    }
    case CWUFileType_WUECL:
        fileName.setf("%s.ecl", wuid);
        fileMimeType.set(HTTP_TYPE_TEXT_PLAIN);
        fileNameWithPath.set(workingFolder).append(PATHSEPCHAR).append(fileName.str());
        winfo.getWorkunitQueryShortText(mb, fileNameWithPath.str());
        break;
    default:
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Unsupported file type %d.", fileType);
    }
}

void CWsWuEmailHelper::send(const char* body, const void* attachment, size32_t lenAttachment, StringArray& warnings)
{
    if (lenAttachment == 0)
        sendEmail(to.get(), subject.get(), body, mailServer.get(), port, sender.get(), &warnings);
    else
        sendEmailAttachData(to.get(), subject.get(), body, lenAttachment, attachment, mimeType.get(),
            attachmentName.get(), mailServer.get(), port, sender.get(), &warnings);
}

}
