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

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

//#include "workunit.hpp"
//#include "daclient.hpp"
//#include "dalienv.hpp"
//#include "exception_util.hpp"
//#include "wujobq.hpp"
//#include "eventqueue.hpp"
//#include "hqlerror.hpp"
//#include "sacmd.hpp"
//#include "portlist.h"

//#define     OWN_WU_ACCESS      "OwnWorkunitsAccess"
//#define     OTHERS_WU_ACCESS   "OthersWorkunitsAccess"

//const unsigned MAXTHORS = 1024;

//#define    File_Cpp "cpp"
//#define    File_ThorLog "ThorLog"
//#define    File_ThorSlaveLog "ThorSlaveLog"
//#define    File_EclAgentLog "EclAgentLog"
//#define    File_XML "XML"
//#define    File_Res "res"
//#define    File_DLL "dll"
//#define    File_ArchiveQuery "ArchiveQuery"

namespace ws_workunits {

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, const char *owner, SecAccessFlags accessOwn, SecAccessFlags accessOthers)
{
    return (isEmpty(owner) || (user && streq(user, owner))) ? accessOwn : accessOthers;
}

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, IConstWorkUnit& cw, SecAccessFlags accessOwn, SecAccessFlags accessOthers)
{
    SCMStringBuffer owner;
    return chooseWuAccessFlagsByOwnership(user, cw.getUser(owner).str(), accessOwn, accessOthers);
}

const char *getWuAccessType(const char *owner, const char *user)
{
    return (isEmpty(owner) || (user && streq(user, owner))) ? OWN_WU_ACCESS : OTHERS_WU_ACCESS;
}

const char *getWuAccessType(IConstWorkUnit& cw, const char *user)
{
    SCMStringBuffer owner;
    return getWuAccessType(cw.getUser(owner).str(), user);
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
    Owned<IConstWorkUnit> cw = wf->openWorkUnit(wuid, false);
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
    userdesc->set(context.queryUserId(), context.queryPassword());
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


WsWUExceptions::WsWUExceptions(IConstWorkUnit& wu): numerr(0), numwrn(0), numinf(0)
{
    Owned<IConstWUExceptionIterator> it = &wu.getExceptions();
    ForEach(*it)
    {

        SCMStringBuffer src, msg, file;
        Owned<IEspECLException> e= createECLException("","");
        e->setCode(it->query().getExceptionCode());
        e->setSource(it->query().getExceptionSource(src).str());
        e->setMessage(it->query().getExceptionMessage(msg).str());
        e->setFileName(it->query().getExceptionFileName(file).str());
        e->setLineNo(it->query().getExceptionLineNo());
        e->setColumn(it->query().getExceptionColumn());

        const char * label = "";
        switch (it->query().getSeverity())
        {
            default:
            case ExceptionSeverityError: label = "Error"; numerr++; break;
            case ExceptionSeverityWarning: label = "Warning"; numwrn++; break;
            case ExceptionSeverityInformation: label = "Info"; numinf++; break;
        }

        e->setSeverity(label);
        errors.append(*e.getLink());
    }
}

#define SDS_LOCK_TIMEOUT 30000

void getSashaNode(SocketEndpoint &ep)
{
    Owned<IRemoteConnection> econn = querySDS().connect("/Environment", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (!econn)
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI,"Cannot connect to DALI server.");
    IPropertyTree *root = econn->queryRoot();
    if (!root)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO,"Cannot get environment information.");
    IPropertyTree *pt = root->queryPropTree("Software/SashaServerProcess[1]/Instance[1]");
    if (!pt)
        throw MakeStringException(ECLWATCH_ARCHIVE_SERVER_NOT_FOUND, "Archive Server not found.");
    ep.set(pt->queryProp("@netAddress"), pt->getPropInt("@port",DEFAULT_SASHA_PORT));
}


void WsWuInfo::getSourceFiles(IEspECLWorkunit &info, unsigned flags)
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
        userdesc->set(username.str(), passwd);

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

void WsWuInfo::getExceptions(IEspECLWorkunit &info, unsigned flags)
{
    if ((flags & WUINFO_IncludeExceptions) || version > 1.16)
    {
        WsWUExceptions errors(*cw);
        if (version > 1.16)
        {
            info.setErrorCount(errors.ErrCount());
            info.setWarningCount(errors.WrnCount());
            info.setInfoCount(errors.InfCount());
        }
        if ((flags & WUINFO_IncludeExceptions))
            info.setExceptions(errors);
    }
}

void WsWuInfo::getVariables(IEspECLWorkunit &info, unsigned flags)
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

void WsWuInfo::getTimers(IEspECLWorkunit &info, unsigned flags)
{
    if (!(flags & WUINFO_IncludeTimers))
        return;
    try
    {
        StringBuffer totalThorTimeValue;
        unsigned totalThorTimerCount = 0; //Do we need this?

        IArrayOf<IEspECLTimer> timers;
        Owned<IStringIterator> it = &cw->getTimers();
        ForEach(*it)
        {
            SCMStringBuffer name;
            it->str(name);
            SCMStringBuffer value;
            unsigned count = cw->getTimerCount(name.str(), NULL);
            unsigned duration = cw->getTimerDuration(name.str(), NULL);
            StringBuffer fd;
            formatDuration(fd, duration);
            for (unsigned i = 0; i < name.length(); i++)
             if (name.s.charAt(i)=='_')
                 name.s.setCharAt(i, ' ');

            if (strieq(name.str(), TOTALTHORTIME))
            {
                totalThorTimeValue = fd;
                totalThorTimerCount = count;
                continue;
            }

            Owned<IEspECLTimer> t= createECLTimer("","");
            t->setName(name.str());
            t->setValue(fd.str());
            t->setCount(count);

            if (version > 1.19)
            {
                StringBuffer graphName;
                unsigned subGraphNum;
                unsigned __int64 subId;
                if (parseGraphTimerLabel(name.str(), graphName, subGraphNum, subId))
                {
                    if (graphName.length() > 0)
                    {
                        t->setGraphName(graphName.str());
                    }
                    if (subId > 0)
                    {
                        t->setSubGraphId((int)subId);
                    }
                }
            }

            timers.append(*t.getLink());
        }

        if (totalThorTimeValue.length() > 0)
        {
            Owned<IEspECLTimer> t= createECLTimer("","");
            t->setName(TOTALTHORTIME);
            t->setValue(totalThorTimeValue.str());
            t->setCount(totalThorTimerCount);
            timers.append(*t.getLink());
        }

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

struct mapEnums { int val; const char *str; };

mapEnums queryFileTypes[] = {
   { FileTypeCpp, "cpp" },
   { FileTypeDll, "dll" },
   { FileTypeResText, "res" },
   { FileTypeHintXml, "hint" },
   { FileTypeXml, "xml" },
   { FileTypeSize,  NULL },
};

const char *getEnumText(int value, mapEnums *map)
{
    const char *defval = map->str;
    while (map->str)
    {
        if (value==map->val)
            return map->str;
        map++;
    }
    assertex(!"Unexpected value in setEnum");
    return defval;
}

void WsWuInfo::getHelpers(IEspECLWorkunit &info, unsigned flags)
{
    try
    {
        IArrayOf<IEspECLHelpFile> helpers;

        Owned <IConstWUQuery> query = cw->getQuery();
        if(!query)
        {
            ERRLOG("Cannot get Query for this workunit.");
            info.setHelpersDesc("Cannot get Query for this workunit.");
        }
        else
        {
            SCMStringBuffer qname;
            query->getQueryShortText(qname);
            if(qname.length())
            {
                if((flags & WUINFO_TruncateEclTo64k) && (qname.length() > 64000))
                    qname.setLen(qname.str(), 64000);

                IEspECLQuery* q=&info.updateQuery();
                q->setText(qname.str());
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
                SCMStringBuffer qText;
                query->getQueryText(qText);
                if ((qText.length() > 0) && isArchiveQuery(qText.str()))
                    info.setHasArchiveQuery(true);
            }

            for (unsigned i = 0; i < FileTypeSize; i++)
                getHelpFiles(query, (WUFileType) i, helpers);
        }

        getWorkunitThorLogInfo(helpers, info);

        if (cw->getWuidVersion() > 0)
        {
            IPropertyTreeIterator& eclAgents = cw->getProcesses("EclAgent", NULL);
            ForEach (eclAgents)
            {
                StringBuffer logName, agentPID;
                eclAgents.query().getProp("@log",logName);
                if (!logName.length())
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
                        if (eclAgents.query().hasProp("@pid"))
                            h->setPID(eclAgents.query().getPropInt("@pid"));
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
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setHelpersDesc(eMsg.str());
        e->Release();
    }
}

void WsWuInfo::getApplicationValues(IEspECLWorkunit &info, unsigned flags)
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
            SCMStringBuffer buf;

            Owned<IEspApplicationValue> t= createApplicationValue("","");
            t->setApplication(val.getApplication(buf).str());
            t->setValue(val.getValue(buf).str());
            t->setName(val.getName(buf).str());
            t->setValue(val.getValue(buf).str());
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

void WsWuInfo::getDebugValues(IEspECLWorkunit &info, unsigned flags)
{
    if (!(flags & WUINFO_IncludeDebugValues))
        return;
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

void WsWuInfo::getGraphInfo(IEspECLWorkunit &info, unsigned flags)
{
     if (version > 1.01)
     {
        info.setHaveSubGraphTimings(false);
        StringBuffer xpath("/WorkUnits/");
        xpath.append(wuid.str());
        Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), 0, 5*60*1000);
        if (!conn)
        {
            DBGLOG("Cannot connect to SDS");
            info.setGraphsDesc("Cannot connect to SDS");
            return;
        }
        IPropertyTree *wpt = conn->queryRoot();
        if (!wpt)
        {
            DBGLOG("Cannot get data from SDS");
            info.setGraphsDesc("Cannot get data from SDS");
            return;
        }

        Owned<IPropertyTreeIterator> iter = wpt->getElements("Timings/Timing");
        StringBuffer name;
        IArrayOf<IConstECLTimingData> timingdatarray;
        ForEach(*iter)
        {
            if (iter->query().getProp("@name",name.clear()))
            {
                if ((name.length()>11) && (strncmp("Graph graph", name.str(), 11)==0))
                {
                    unsigned gn;
                    const char *s = getGraphNum(name.str()+11, gn);
                    unsigned sn;
                    s = getGraphNum(s,sn);
                    if (gn && sn)
                    {
                        info.setHaveSubGraphTimings(true);
                        break;
                    }
                }
            }
        }
     }

    if (!(flags & WUINFO_IncludeGraphs))
        return;

    try
    {
        SCMStringBuffer runningGraph;
        WUGraphIDType id;

        WUState st = cw->getState();
        bool running = (!(st==WUStateFailed || st==WUStateAborted || st==WUStateCompleted) && cw->getRunningGraph(runningGraph,id));

        IArrayOf<IEspECLGraph> graphs;
        Owned<IConstWUGraphIterator> it = &cw->getGraphs(GraphTypeAny);
        ForEach(*it)
        {
            IConstWUGraph &graph = it->query();
            if(!graph.isValid())
                continue;

            SCMStringBuffer name, label, type;
            graph.getName(name);
            graph.getLabel(label);

            graph.getTypeName(type);

            Owned<IEspECLGraph> g= createECLGraph("","");
            g->setName(name.str());
            g->setLabel(label.str());
            g->setType(type.str());
            if(running && strcmp(name.str(),runningGraph.str())==0)
            {
                g->setRunning(true);
                g->setRunningId(id);
            }

            Owned<IConstWUGraphProgress> progress = cw->getGraphProgress(name.str());
            if (progress)
            {
                WUGraphState graphstate= progress->queryGraphState();
                if (graphstate == WUGraphComplete)
                    g->setComplete(true);
                if (version > 1.13 && graphstate == WUGraphFailed)
                {
                    g->setFailed(true);
                }
            }
            graphs.append(*g.getLink());
        }
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

void WsWuInfo::getGraphTimingData(IArrayOf<IConstECLTimingData> &timingData, unsigned flags)
{
    StringBuffer xpath("/WorkUnits/");
    xpath.append(wuid.str());

    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), 0, 5*60*1000);
    if (!conn)
    {
        DBGLOG("Could not connect to SDS");
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Cannot connect to dali server.");
    }

    IPropertyTree *wpt = conn->queryRoot();
    Owned<IPropertyTreeIterator> iter = wpt->getElements("Timings/Timing");

    ForEach(*iter)
    {
        StringBuffer name;
        if (iter->query().getProp("@name", name))
        {
            if ((name.length()>11)&&(strncmp("Graph graph", name.str(), 11)==0))
            {
                unsigned gn;
                const char *s = getGraphNum(name.str(),gn);
                unsigned sn;
                s = getGraphNum(s, sn);
                if (gn && sn)
                {
                    const char *gs = strchr(name.str(),'(');
                    unsigned gid = 0;
                    if (gs)
                        getGraphNum(gs+1, gid);
                    unsigned time = iter->query().getPropInt("@duration");

                    Owned<IEspECLTimingData> g = createECLTimingData();
                    g->setName(name.str());
                    g->setGraphNum(gn);
                    g->setSubGraphNum(sn);
                    g->setGID(gid);
                    g->setMS(time);
                    g->setMin(time/60000);
                    timingData.append(*g.getClear());
                }
            }
        }
    }
}



void WsWuInfo::getRoxieCluster(IEspECLWorkunit &info, unsigned flags)
{
    if (version > 1.06)
    {
        Owned<IConstWURoxieQueryInfo> roxieQueryInfo = cw->getRoxieQueryInfo();
        if (roxieQueryInfo)
        {
            SCMStringBuffer roxieClusterName;
            roxieQueryInfo->getRoxieClusterName(roxieClusterName);
            info.setRoxieCluster(roxieClusterName.str());
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

                IWorkflowEvent *wfevent = r->getScheduleEvent();
                if (!wfevent)
                    continue;

                if (!r->hasScheduleCount() || (r->queryScheduleCountRemaining() > 0))
                {
                    info.setEventSchedule(1); //Can reschedule
                    break;
                }
            }
        }
    }
}

void WsWuInfo::getCommon(IEspECLWorkunit &info, unsigned flags)
{
    SCMStringBuffer s;
    info.setWuid(cw->getWuid(s).str());
    info.setProtected(cw->isProtected() ? 1 : 0);
    info.setJobname(cw->getJobName(s).str());
    info.setOwner(cw->getUser(s).str());
    info.setCluster(cw->getClusterName(clusterName).str());
    info.setSnapshot(cw->getSnapshot(s).str());

    if ((cw->getState() == WUStateScheduled) && cw->aborting())
    {
        info.setStateID(WUStateAborting);
        info.setState("aborting");
    }
    else
    {
        info.setStateID(cw->getState());
        info.setState(cw->getStateDesc(s).str());
    }

    if (cw->isPausing())
        info.setIsPausing(true);

    getEventScheduleFlag(info);

    if (version > 1.27)
    {
        StringBuffer totalThorTimeStr;
        unsigned totalThorTimeMS = cw->getTimerDuration(TOTALTHORTIME, NULL);
        formatDuration(totalThorTimeStr, totalThorTimeMS);
        info.setTotalThorTime(totalThorTimeStr.str());
    }

    WsWuDateTime dt;
    cw->getTimeScheduled(dt);
    if(dt.isValid())
        info.setDateTimeScheduled(dt.getString(s).str());

    getRoxieCluster(info, flags);
}

void WsWuInfo::getInfo(IEspECLWorkunit &info, unsigned flags)
{
    getCommon(info, flags);

    SecAccessFlags accessFlag = getWsWorkunitAccess(context, *cw);
    info.setAccessFlag(accessFlag);

    SCMStringBuffer s;
    info.setStateEx(cw->getStateEx(s).str());
    info.setPriorityClass(cw->getPriority());
    info.setPriorityLevel(cw->getPriorityLevel());
    info.setScope(cw->getWuScope(s).str());
    info.setActionEx(cw->getActionEx(s).str());
    info.setDescription(cw->getDebugValue("description", s).str());
    if (version > 1.21)
        info.setXmlParams(cw->getXmlParams(s).str());

    info.setResultLimit(cw->getResultLimit());
    info.setArchived(false);
    info.setGraphCount(cw->getGraphCount());
    info.setSourceFileCount(cw->getSourceFileCount());
    info.setVariableCount(cw->getVariableCount());
    info.setTimerCount(cw->getTimerCount());
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

unsigned WsWuInfo::getWorkunitThorLogInfo(IArrayOf<IEspECLHelpFile>& helpers, IEspECLWorkunit &info)
{
    unsigned countThorLog = 0;

    IArrayOf<IConstThorLogInfo> thorLogList;
    if (cw->getWuidVersion() > 0)
    {
        SCMStringBuffer clusterName;
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cw->getClusterName(clusterName).str());
        if (!clusterInfo)
        {
            SCMStringBuffer wuid;
            WARNLOG("Cannot find TargetClusterInfo for workunit %s", cw->getWuid(wuid).str());
            return countThorLog;
        }

        unsigned numberOfSlaves = clusterInfo->getSize();

        BoolHash uniqueProcesses;
        Owned<IStringIterator> thorInstances = cw->getProcesses("Thor");
        ForEach (*thorInstances)
        {
            SCMStringBuffer processName;
            thorInstances->str(processName);
            if ((processName.length() < 1) || uniqueProcesses.getValue(processName.str()))
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
                thorLog->setNumberSlaves(numberOfSlaves);
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

        StringBuffer logDir;
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
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

bool WsWuInfo::getClusterInfo(IEspECLWorkunit &info, unsigned flags)
{
    if (version > 1.04)
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

void WsWuInfo::getWorkflow(IEspECLWorkunit &info, unsigned flags)
{
    bool eventCountRemaining = false;
    bool eventCountUnlimited = false;
    try
    {
        info.setEventSchedule(0);
        IArrayOf<IConstECLWorkflow> workflows;
        Owned<IConstWorkflowItemIterator> it = cw->getWorkflowItems();
        if (it)
        {
            ForEach(*it)
            {
                IConstWorkflowItem *r = it->query();
                if (r)
                {
                    IWorkflowEvent *wfevent = r->getScheduleEvent();
                    if (wfevent)
                    {
                        Owned<IEspECLWorkflow> g;
                        if (flags & WUINFO_IncludeWorkflows)
                        {
                            StringBuffer id;
                            g.setown(createECLWorkflow("",""));
                            g->setWFID(id.appendf("%d", r->queryWfid()).str());
                            g->setEventName(wfevent->queryName());
                            g->setEventText(wfevent->queryText());
                        }
                        if (r->hasScheduleCount())
                        {
                            if (r->queryScheduleCountRemaining() > 0)
                                eventCountRemaining = true;
                            if (flags & WUINFO_IncludeWorkflows)
                            {
                                g->setCount(r->queryScheduleCount());
                                g->setCountRemaining(r->queryScheduleCountRemaining());
                            }
                        }
                        else
                        {
                            eventCountUnlimited = true;
                        }
                        if (flags & WUINFO_IncludeWorkflows)
                            workflows.append(*g.getLink());
                    }
                }
            }
            if (workflows.length() > 0)
                info.setWorkflows(workflows);
            workflows.kill();
        }
    }
    catch(IException* e)
    {
        StringBuffer eMsg;
        ERRLOG("%s", e->errorMessage(eMsg).str());
        info.setWorkflowsDesc(eMsg.str());
        e->Release();
    }

    if (info.getState() && !stricmp(info.getState(), "wait"))
        info.setEventSchedule(2); //Can deschedule
    else if (eventCountUnlimited || eventCountRemaining)
        info.setEventSchedule(1); //Can reschedule
}

bool shouldFileContentBeShown(IEspContext &context, const char * logicalName)
{
    StringBuffer username;
    context.getUserID(username);

    Owned<IUserDescriptor> userdesc(createUserDescriptor());
    userdesc->set(username.str(), context.queryPassword());

    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName, userdesc);
    if (!df)
        return false;

    bool blocked;
    if (df->isCompressed(&blocked) && !blocked)
        return false;

    IPropertyTree & properties = df->queryAttributes();
    const char * format = properties.queryProp("@format");
    if (format && (stricmp(format,"csv")==0 || memicmp(format, "utf", 3) == 0))
    {
        return true;
    }
    const char * recordEcl = properties.queryProp("ECL");
    if (!recordEcl)
        return false;

    MultiErrorReceiver errs;
    Owned<IHqlExpression> ret = ::parseQuery(recordEcl, &errs);
    return errs.errCount() == 0;
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

void WsWuInfo::getResult(IConstWUResult &r, IArrayOf<IEspECLResult>& results, unsigned flags)
{
    SCMStringBuffer name;
    r.getResultName(name);

    SCMStringBuffer filename;
    r.getResultLogicalName(filename);

    StringBuffer value, link;
    if (r.getResultStatus() == ResultStatusUndefined)
        value.set("[undefined]");

    else if (r.isResultScalar())
    {
        try
        {
            SCMStringBuffer xml;
            r.getResultXml(xml);

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
        if(r.getResultSequence()>=0)
        {
            if(filename.length())
            {
                StringBuffer username;
                context.getUserID(username);

                Owned<IUserDescriptor> userdesc(createUserDescriptor());
                userdesc->set(username.str(), context.queryPassword());

                Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(filename.str(), userdesc);
                if(df && df->queryAttributes().hasProp("ECL"))
                    link.append(r.getResultSequence());
            }
            else
                link.append(r.getResultSequence());
        }
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
        result->setShowFileContent(shouldFileContentBeShown(context, filename.str()));

    result->setName(name.str());
    result->setLink(link.str());
    result->setSequence(r.getResultSequence());
    result->setValue(value.str());
    result->setFileName(filename.str());
    result->setIsSupplied(r.getResultStatus() == ResultStatusSupplied);
    result->setTotal(r.getResultTotalRowCount());
    results.append(*result.getLink());
}


void WsWuInfo::getResults(IEspECLWorkunit &info, unsigned flags)
{
    try
    {
        unsigned count = 0;
        IArrayOf<IEspECLResult> results;
        Owned<IConstWUResultIterator> it = &(cw->getResults());
        ForEach(*it)
        {
            IConstWUResult &r = it->query();
            if(r.getResultSequence()>=0)
            {
                if (flags & WUINFO_IncludeResults)
                    getResult(r, results, flags);
                count++;
            }
        }

        if (version >= 1.17)
            info.setResultCount(count);

        if ((flags & WUINFO_IncludeResults) && results.length() > 0)
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

void WsWuInfo::getHelpFiles(IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers)
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
        if (fileName && *fileName && (fileNames.length() > 0))
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

        Owned<IPropertyTreeIterator> filetrees= query.getElements("Subfile");
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

bool WsWuInfo::getResultViews(StringArray &viewnames, unsigned flags)
{
    if (!(flags & WUINFO_IncludeResultsViewNames))
        return true;
    try
    {
        Owned<IWuWebView> wv = createWuWebView(*cw, NULL, NULL, false);
        if (wv)
            wv->getResultViewNames(viewnames);
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

void appendIOStreamContent(MemoryBuffer &mb, IFileIOStream *ios, bool forDownload)
{
    StringBuffer line;
    bool eof = false;
    while (!eof)
    {
        line.clear();
        loop
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
    char const * pidchars = pidstr.str();
    while(!eof)
    {
        line.clear();
        loop
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
        if(strstr(line.str(), pidchars))
        {
            //Check if this is a new instance using line sequence number
            if (strncmp(line.str(), "00000000", 8) == 0)
            {
                if (wuidFound) //If the correct instance has been found, return that instance before the next instance.
                    break;

                //The last instance is not a correct instance. Clean the buf in order to start a new instance.
                buf.clear();
            }

            //If we spot the workunit id anywhere in the tacing for this pid then assume it is the correct instance.
            if(!wuidFound && strstr(line.str(), wuid.str()))
                wuidFound = true;

            buf.append(line.length(), line.str());
        }
    }

    if (buf.length() < 1)
        buf.append(47, "(Not found a log line related to this workunit)");
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
        loop
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
            loop
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

void WsWuInfo::getWorkunitArchiveQuery(MemoryBuffer& buf)
{
    Owned<IConstWUQuery> query = cw->getQuery();
    if(!query)
        throw MakeStringException(ECLWATCH_QUERY_NOT_FOUND_FOR_WU,"No query for workunit %s.",wuid.str());

    SCMStringBuffer queryText;
    query->getQueryText(queryText);
    if ((queryText.length() < 1) || !isArchiveQuery(queryText.str()))
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Archive Query not found for workunit %s.", wuid.str());
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

    SCMStringBuffer xml;
    exportWorkUnitToXML(cw, xml, true);

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
                                        const char* description, bool forDownload, MemoryBuffer& buf)
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

    const char* header;
    if (plainText && (!stricmp(plainText, "yes")))
        header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    else
        header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet href=\"../esp/xslt/xmlformatter.xsl\" type=\"text/xsl\"?>";

    buf.append(strlen(header), header);
    appendIOStreamContent(buf, ios.get(), forDownload);
}




WsWuSearch::WsWuSearch(IEspContext& context,const char* owner,const char* state,const char* cluster,const char* startDate,const char* endDate,const char* ecl,const char* jobname,const char* appname,const char* appkey,const char* appvalue)
{
    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

    StringBuffer xpath("*");
    if(ecl && *ecl)
        xpath.append("[Query/Text=?~\"*").append(ecl).append("*\"]");
    if(state && *state)
        xpath.append("[@state=\"").append(state).append("\"]");
    if(cluster && *cluster)
        xpath.append("[@clusterName=\"").append(cluster).append("\"]");
    if(owner && *owner)
        xpath.append("[@submitID=?~\"").append(owner).append("\"]");
    if(jobname && *jobname)
        xpath.append("[@jobName=?~\"*").append(jobname).append("*\"]");
    if((appname && *appname) || (appkey && *appkey) || (appvalue && *appvalue))
    {
        xpath.append("[Application/").append(appname && *appname ? appname : "*");
        xpath.append("/").append(appkey && *appkey ? appkey : "*");
        if(appvalue && *appvalue)
            xpath.append("=?~\"").append(appvalue).append("\"");
        xpath.append("]");
    }

    Owned<IConstWorkUnitIterator> it(factory->getWorkUnitsByXPath(xpath.str()));

    StringBuffer wuFrom, wuTo;
    if(startDate && *startDate)
        createWuidFromDate(startDate, wuFrom);
    if(endDate && *endDate)
        createWuidFromDate(endDate, wuTo);

    ForEach(*it)
    {
        IConstWorkUnit &cw = it->query();
        if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cw, accessOwn, accessOthers) < SecAccess_Read)
            continue;

        SCMStringBuffer wuid;
        cw.getWuid(wuid);
        if (wuFrom.length() && strcmp(wuid.str(),wuFrom.str())<0)
            continue;
        if (wuTo.length() && strcmp(wuid.str(),wuTo.str())>0)
            continue;

        if (state && *state)
        {
            SCMStringBuffer descr;
            if(!strieq(cw.getStateDesc(descr).str(),state))
                continue;
        }

        SCMStringBuffer parent;
        if (!cw.getParentWuid(parent).length())
        {
            parent.clear();
            wuids.push_back(cw.getWuid(parent).str());
        }
    }
    std::sort(wuids.begin(),wuids.end(),std::greater<std::string>());
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

        DataCacheElement* awu = list_iter->getLink();
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

        ArchivedWuCacheElement* awu = list_iter->getLink();
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

void ArchivedWuCache::add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, IArrayOf<IEspECLWorkunit>& wus)
{
    CriticalBlock block(crit);

    //Save new data
    Owned<ArchivedWuCacheElement> e=new ArchivedWuCacheElement(filter, sashaUpdatedWhen, hasNextPage, /*data.str(),*/ wus);
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

    DBGLOG("Queue log: [%s]", line);
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
        while(true)
        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IConstWorkUnitIterator> itr = factory->getWorkUnitsByState(WUStateScheduled);
            if (itr)
            {
                ForEach(*itr)
                {
                    try
                    {
                        IConstWorkUnit & cw = itr->query();
                        if (cw.aborting())
                        {
                            WorkunitUpdate wu(&cw.lock());
                            wu->setState(WUStateAborted);
                            continue;
                        }

                        WsWuDateTime dt, now;
                        now.setNow();
                        cw.getTimeScheduled(dt);
                        if (now.compare(dt)>=0)
                        {
                            SCMStringBuffer wuid;
                            runWorkUnit(cw.getWuid(wuid).str());
                        }
                    }
                    catch(IException *e)
                    {
                        StringBuffer msg;
                        ERRLOG("Exception %d:%s in WsWorkunits Schedule::run", e->errorCode(), e->errorMessage(msg).str());
                        e->Release();
                    }
                }
            }
            sleep(60);
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

}
