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

#ifndef _WIN32
#include <sys/types.h>
#include <dirent.h>
#endif

#include <stdio.h>
#include <time.h>

#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jsocket.hpp"
#include "jmutex.hpp"

#include "commonext.hpp"
#include "dasds.hpp"
#include "dafdesc.hpp"

#include "thor.hpp"
#include "thorport.hpp"
#include "thormisc.hpp"
#include "thgraph.hpp"
#include "thbufdef.hpp"
#include "thmem.hpp"
#include "thcompressutil.hpp"

#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlfield_imp.hpp"
#include "rtlds_imp.hpp"

namespace thormisc {  // Make sure we can't clash with generated versions or version check mechanism fails.
 #include "eclhelper_base.hpp" 
}

#define SDS_LOCK_TIMEOUT 30000

static INode *masterNode;
static IGroup *rawGroup;
static IGroup *nodeGroup;
static IGroup *clusterGroup;
static IGroup *slaveGroup;
static IGroup *dfsGroup;
static ICommunicator *nodeComm;


mptag_t masterSlaveMpTag;
IPropertyTree *globals;
static IMPtagAllocator *ClusterMPAllocator = NULL;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    masterNode = NULL;
    globals = NULL;
    rawGroup = NULL;
    nodeGroup = NULL;
    clusterGroup = NULL;
    slaveGroup = NULL;
    dfsGroup = NULL;
    nodeComm = NULL;
    ClusterMPAllocator = createMPtagRangeAllocator(MPTAG_THORGLOBAL_BASE,MPTAG_THORGLOBAL_COUNT);
    return true;
}

MODULE_EXIT()
{
    ::Release(masterNode);
    ::Release(rawGroup);
    ::Release(nodeGroup);
    ::Release(clusterGroup);
    ::Release(slaveGroup);
    ::Release(dfsGroup);
    ::Release(nodeComm);
    ::Release(ClusterMPAllocator);
}


#define EXTRAS 1024
#define NL 3
StringBuffer &ActPrintLogArgsPrep(StringBuffer &res, const CGraphElementBase *container, const ActLogEnum flags, const char *format, va_list args)
{
    if (format)
        res.valist_appendf(format, args).append(" - ");
    res.appendf("activity(%s, %" ACTPF "d)",activityKindStr(container->getKind()), container->queryId());
    if (0 != (flags & thorlog_ecl))
    {
        StringBuffer ecltext;
        container->getEclText(ecltext);
        ecltext.trim();
        if (ecltext.length() > 0)
            res.append(" [ecl=").append(ecltext.str()).append(']');
    }
#ifdef _WIN32
#ifdef MEMLOG
    MEMORYSTATUS mS;
    GlobalMemoryStatus(&mS);
    res.appendf(", mem=%ld",mS.dwAvailPhys);
#endif
#endif
    return res;
}

void ActPrintLogArgs(const CGraphElementBase *container, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args)
{
    if ((0 == (flags & thorlog_all)) && !container->doLogging())
        return; // suppress logging child activities unless thorlog_all flag
    StringBuffer res;
    ActPrintLogArgsPrep(res, container, flags, format, args);
    LOG(logCat, thorJob, "%s", res.str());
}

void ActPrintLogArgs(const CGraphElementBase *container, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args)
{
    StringBuffer res;
    ActPrintLogArgsPrep(res, container, flags, format, args);
    if (e)
    {
        res.append(" : ");
        e->errorMessage(res);
    }
    LOG(logCat, thorJob, "%s", res.str());
}

void ActPrintLogEx(const CGraphElementBase *container, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...)
{
    if ((0 == (flags & thorlog_all)) && (NULL != container->queryOwner().queryOwner() && !container->queryOwner().isGlobal()))
        return; // suppress logging child activities unless thorlog_all flag
    StringBuffer res;
    va_list args;
    va_start(args, format);
    ActPrintLogArgsPrep(res, container, flags, format, args);
    va_end(args);
    LOG(logCat, thorJob, "%s", res.str());
}

void ActPrintLog(const CActivityBase *activity, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(&activity->queryContainer(), thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void ActPrintLog(const CActivityBase *activity, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(&activity->queryContainer(), e, thorlog_null, MCexception(e, MSGCLS_error), format, args);
    va_end(args);
}

void ActPrintLog(const CActivityBase *activity, IException *e)
{
    ActPrintLog(activity, e, "%s", "");
}

void GraphPrintLogArgsPrep(StringBuffer &res, CGraphBase *graph, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args)
{
    if (format)
        res.valist_appendf(format, args).append(" - ");
    res.appendf("graph(%s, %" GIDPF "d)", graph->queryJob().queryGraphName(), graph->queryGraphId());
}

void GraphPrintLogArgs(CGraphBase *graph, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args)
{
    if ((0 == (flags & thorlog_all)) && (NULL != graph->queryOwner() && !graph->isGlobal()))
        return; // suppress logging from child graph unless thorlog_all flag
    StringBuffer res;
    GraphPrintLogArgsPrep(res, graph, flags, logCat, format, args);
    LOG(logCat, thorJob, "%s", res.str());
}

void GraphPrintLogArgs(CGraphBase *graph, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args)
{
    if ((0 == (flags & thorlog_all)) && (NULL != graph->queryOwner() && !graph->isGlobal()))
        return; // suppress logging from child graph unless thorlog_all flag
    StringBuffer res;
    GraphPrintLogArgsPrep(res, graph, flags, logCat, format, args);
    if (e)
    {
        res.append(" : ");
        e->errorMessage(res);
    }
    LOG(logCat, thorJob, "%s", res.str());
}

void GraphPrintLog(CGraphBase *graph, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    GraphPrintLogArgs(graph, e, thorlog_null, MCexception(e, MSGCLS_error), format, args);
    va_end(args);
}

class CThorException : public CSimpleInterface, implements IThorException
{
protected:
    ThorExceptionAction action;
    ThorActivityKind kind;
    activity_id id;
    graph_id graphId;
    StringAttr jobId;
    int errorcode;
    StringAttr msg;
    LogMsgAudience audience;
    unsigned node;
    MemoryBuffer data; // extra exception specific data
    bool notified;
    unsigned line, column;
    StringAttr file, origin;
    ErrorSeverity severity;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CThorException(LogMsgAudience _audience,int code, const char *str) 
        : audience(_audience), errorcode(code), msg(str), action(tea_null), graphId(0), id(0), node(0), line(0), column(0), severity(SeverityInformation), kind(TAKnone), notified(false) { };
    CThorException(MemoryBuffer &mb)
    {
        mb.read((unsigned &)action);
        mb.read(jobId);
        mb.read(graphId);
        mb.read((unsigned &)kind);
        mb.read(id);
        mb.read((unsigned &)audience);
        mb.read(errorcode);
        mb.read(msg);
        mb.read(file);
        mb.read(line);
        mb.read(column);
        mb.read((int &)severity);
        mb.read(origin);
        if (0 == origin.length()) // simpler to clear serialized 0 length terminated string here than check on query
            origin.clear();
        size32_t sz;
        mb.read(sz);
        if (sz)
            data.append(sz, mb.readDirect(sz));
    }

// IThorException
    ThorExceptionAction queryAction() { return action; }
    ThorActivityKind queryActivityKind() { return kind; }
    activity_id queryActivityId() { return id; }
    graph_id queryGraphId() { return graphId; }
    const char *queryJobId() { return jobId; }
    void getAssert(StringAttr &_file, unsigned &_line, unsigned &_column) { _file.set(file); _line = line; _column = column; }
    const char *queryOrigin() { return origin; }
    const char *queryMessage() { return msg; }
    ErrorSeverity querySeverity() { return severity; }
    bool queryNotified() const { return notified; }
    MemoryBuffer &queryData() { return data; }
    void setNotified() { notified = true; }
    void setActivityId(activity_id _id) { id = _id; }
    void setActivityKind(ThorActivityKind _kind) { kind = _kind; }
    void setGraphId(graph_id _graphId) { graphId = _graphId; }
    void setJobId(const char *_jobId) { jobId.set(_jobId); }
    void setAction(ThorExceptionAction _action) { action = _action; }
    void setAudience(MessageAudience _audience) { audience = _audience; }
    void setSlave(unsigned _node) { node = _node; }
    void setMessage(const char *_msg) { msg.set(_msg); }
    void setAssert(const char *_file, unsigned _line, unsigned _column) { file.set(_file); line = _line; column = _column; }
    void setOrigin(const char *_origin) { origin.set(_origin); }
    void setSeverity(ErrorSeverity _severity) { severity = _severity; }

// IException
    int errorCode() const { return errorcode; }
    StringBuffer &errorMessage(StringBuffer &str) const
    {
        if (!origin.length() || 0 != stricmp("user", origin.get())) // don't report slave in user message
        {
            if (graphId)
                str.append("Graph[").append(graphId).append("], ");
            if (kind)
                str.append(activityKindStr(kind));
            if (id)
            {
                if (kind) str.append('[');
                str.append(id);
                if (kind) str.append(']');
                str.append(": ");
            }
            if (node)
            {
                str.appendf("SLAVE #%d [", node);
                queryClusterGroup().queryNode(node).endpoint().getUrlStr(str);
                str.append("]: ");
            }
        }
        str.append(msg);
        return str;
    }
    MessageAudience errorAudience() const { return audience; }
};

CThorException *_MakeThorException(LogMsgAudience audience,int code, const char *format, va_list args) __attribute__((format(printf,3,0)));
CThorException *_MakeThorException(LogMsgAudience audience,int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new CThorException(audience, code, eStr.str());
}

CThorException *_ThorWrapException(IException *e, const char *format, va_list args) __attribute__((format(printf,2,0)));
CThorException *_ThorWrapException(IException *e, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.appendf("%d, ", e->errorCode());
    e->errorMessage(eStr).append(" : ");
    eStr.limited_valist_appendf(2048, format, args);
    CThorException *te = new CThorException(e->errorAudience(), e->errorCode(), eStr.str());
    return te;
}

// convert exception (if necessary) to an exception with action=shutdown
IThorException *MakeThorFatal(IException *e, int code, const char *format, ...)
{
    CThorException *te = QUERYINTERFACE(e, CThorException);
    if (te)
        te->Link();
    else
    {
        va_list args;
        va_start(args, format);
        if (e) te = _ThorWrapException(e, format, args);
        else te = _MakeThorException(MSGAUD_user,code, format, args);
        va_end(args);
    }
    te->setAction(tea_shutdown);
    return te;
}

IThorException *MakeThorAudienceException(LogMsgAudience audience, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeThorException(audience, code, format, args);
    va_end(args);
    return e;
}

IThorException *MakeThorOperatorException(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeThorException(MSGAUD_operator,code, format, args);
    va_end(args);
    return e;
}

void setExceptionActivityInfo(CGraphElementBase &container, IThorException *e)
{
    e->setActivityKind(container.getKind());
    e->setActivityId(container.queryId());
    e->setGraphId(container.queryOwner().queryGraphId());
}

IThorException *_MakeActivityException(CGraphElementBase &container, int code, const char *format, va_list args) __attribute__((format(printf,3,0)));
IThorException *_MakeActivityException(CGraphElementBase &container, int code, const char *format, va_list args)
{
    IThorException *e = _MakeThorException(MSGAUD_user, code, format, args);
    setExceptionActivityInfo(container, e);
    return e;
}

IThorException *_MakeActivityException(CGraphElementBase &container, IException *e, const char *_format, va_list args) __attribute__((format(printf,3,0)));
IThorException *_MakeActivityException(CGraphElementBase &container, IException *e, const char *_format, va_list args)
{
    StringBuffer msg;
    e->errorMessage(msg);
    if (_format)
        msg.append(", ").limited_valist_appendf(1024, _format, args);
    IThorException *e2 = new CThorException(e->errorAudience(), e->errorCode(), msg.str());
    setExceptionActivityInfo(container, e2);
    return e2;
}

IThorException *MakeActivityException(CActivityBase *activity, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeActivityException(activity->queryContainer(), code, format, args);
    va_end(args);
    return e;
}

IThorException *MakeActivityException(CActivityBase *activity, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e2 = _MakeActivityException(activity->queryContainer(), e, format, args);
    va_end(args);
    return e2;
}

IThorException *MakeActivityException(CActivityBase *activity, IException *e)
{
    return MakeActivityException(activity, e, "%s", "");
}

IThorException *MakeActivityWarning(CActivityBase *activity, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeActivityException(activity->queryContainer(), code, format, args);
    e->setAction(tea_warning);
    e->setSeverity(SeverityWarning);
    va_end(args);
    return e;
}

IThorException *MakeActivityWarning(CActivityBase *activity, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e2 = _MakeActivityException(activity->queryContainer(), e, format, args);
    e2->setAction(tea_warning);
    e2->setSeverity(SeverityWarning);
    va_end(args);
    return e2;
}

IThorException *MakeActivityException(CGraphElementBase *container, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeActivityException(*container, code, format, args);
    va_end(args);
    return e;
}

IThorException *MakeActivityException(CGraphElementBase *container, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e2 = _MakeActivityException(*container, e, format, args);
    va_end(args);
    return e2;
}

IThorException *MakeActivityException(CGraphElementBase *container, IException *e)
{
    return MakeActivityException(container, e, "%s", "");
}

IThorException *MakeActivityWarning(CGraphElementBase *container, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeActivityException(*container, code, format, args);
    e->setAction(tea_warning);
    e->setSeverity(SeverityWarning);
    va_end(args);
    return e;
}

IThorException *MakeActivityWarning(CGraphElementBase *container, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e2 = _MakeActivityException(*container, e, format, args);
    e2->setAction(tea_warning);
    e2->setSeverity(SeverityWarning);
    va_end(args);
    return e2;
}

IThorException *MakeThorException(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e2 = _MakeThorException(MSGAUD_user,code, format, args);
    va_end(args);
    return e2;
}

IThorException *MakeThorException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    if (te)
        return LINK(te);
    StringBuffer msg;
    return new CThorException(MSGAUD_user, e->errorCode(), e->errorMessage(msg).str());
}

IThorException *ThorWrapException(IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ThorExceptionAction action=tea_null;
    if (QUERYINTERFACE(e, ISEH_Exception))
        action = tea_shutdown;
    CThorException *te = _ThorWrapException(e, format, args);
    te->setAction(action);
    va_end(args);
    return te;
}

IThorException *MakeGraphException(CGraphBase *graph, int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IThorException *e = _MakeThorException(MSGAUD_user, code, format, args);
    e->setGraphId(graph->queryGraphId());
    va_end(args);
    return e;
}

#if 0
void SetLogName(const char *prefix, const char *logdir, const char *thorname, bool master) 
{
    StringBuffer logname;
    if (logdir && *logdir !='\0')
    {
        if (!recursiveCreateDirectory(logdir))
        {
            PrintLog("Failed to use %s as log directory, using current working directory", logdir); // default working directory should be open already
            return;
        }
        logname.append(logdir);
    }
    else
    {
        char cwd[1024];
        GetCurrentDirectory(1024, cwd);
        logname.append(cwd);
    }

    if (logname.length() && logname.charAt(logname.length()-1) != PATHSEPCHAR)
        logname.append(PATHSEPCHAR);
    logname.append(prefix);
#if 0
    time_t tNow;
    time(&tNow);
    char timeStamp[32];
#ifdef _WIN32
    struct tm *ltNow;
    ltNow = localtime(&tNow);
    strftime(timeStamp, 32, ".%m_%d_%y_%H_%M_%S", ltNow);
#else
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    strftime(timeStamp, 32, ".%m_%d_%y_%H_%M_%S", &ltNow);
#endif
    logname.append(timeStamp);
#endif
    logname.append(".log");
    StringBuffer lf;
    openLogFile(lf, logname.str());
    PrintLog("Opened log file %s", lf.str());
    PrintLog("Build %s", BUILD_TAG);
}
#endif

class CTempNameHandler
{
public:
    unsigned num;
    StringAttr tempdir, tempPrefix;
    StringAttr alttempdir; // only set if needed
    CriticalSection crit;
    bool altallowed;
    bool cleardir;

    CTempNameHandler()
    {
        num = 0;
        altallowed = false;
        cleardir = false;
    }
    ~CTempNameHandler()
    {
        if (cleardir) 
            clearDirs(false);       // don't log as jlog may have closed
    }
    const char *queryTempDir(bool alt) 
    { 
        if (alt&&altallowed) 
            return alttempdir;
        return tempdir; 
    }
    void setTempDir(const char *name, const char *_tempPrefix, bool clear)
    {
        assertex(name && *name);
        CriticalBlock block(crit);
        assertex(tempdir.isEmpty()); // should only be called once
        tempPrefix.set(_tempPrefix);
        StringBuffer base(name);
        addPathSepChar(base);
        tempdir.set(base.str());
        recursiveCreateDirectory(tempdir);
#ifdef _WIN32
        altallowed = false;
#else
        altallowed = globals->getPropBool("@thor_dual_drive",true);
#endif
        if (altallowed)
        {
            unsigned d = getPathDrive(tempdir);
            if (d>1)
                altallowed = false;
            else
            {
                StringBuffer p(tempdir);
                alttempdir.set(setPathDrive(p,d?0:1).str());
                recursiveCreateDirectory(alttempdir);
            }
        }
        cleardir = clear;
        if (clear)
            clearDirs(true);
    }
    static void clearDir(const char *dir, bool log)
    {
        if (dir&&*dir)
        {
            Owned<IDirectoryIterator> iter = createDirectoryIterator(dir);
            ForEach (*iter)
            {
                IFile &file = iter->query();
                if (file.isFile())
                {
                    if (log)
                        LOG(MCdebugInfo, thorJob, "Deleting %s", file.queryFilename());
                    try { file.remove(); }
                    catch (IException *e)
                    {
                        if (log)
                            FLLOG(MCwarning, thorJob, e);
                        e->Release();
                    }
                }
            }
        }
    }
    void clearDirs(bool log)
    {
        clearDir(tempdir,log);
        clearDir(alttempdir,log);
    }
    void getTempName(StringBuffer &name, const char *suffix,bool alt)
    {
        CriticalBlock block(crit);
        assertex(!tempdir.isEmpty()); // should only be called once
        if (alt && altallowed)
            name.append(alttempdir);
        else
            name.append(tempdir);
        name.append(tempPrefix).append((unsigned)GetCurrentProcessId()).append('_').append(++num);
        if (suffix)
            name.append("__").append(suffix);
        name.append(".tmp");
    }
} TempNameHandler;



void GetTempName(StringBuffer &name, const char *prefix,bool altdisk)
{
    TempNameHandler.getTempName(name, prefix, altdisk);
}

void SetTempDir(const char *name, const char *tempPrefix, bool clear)
{
    TempNameHandler.setTempDir(name, tempPrefix, clear);
}

void ClearDir(const char *dir)
{
    CTempNameHandler::clearDir(dir,true);
}

void ClearTempDirs()
{
    TempNameHandler.clearDirs(true);
    PROGLOG("temp directory cleared");
}


const char *queryTempDir(bool altdisk)
{
    return TempNameHandler.queryTempDir(altdisk);
}

class CBarrierAbortException: public CSimpleInterface, public IBarrierException
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
// IThorException
    int errorCode() const { return -1; }
    StringBuffer &errorMessage(StringBuffer &str) const { str.append("Barrier Aborted"); return str; }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

IBarrierException *createBarrierAbortException()
{
    return new CBarrierAbortException();
}

void loadCmdProp(IPropertyTree *tree, const char *cmdProp)
{
    StringBuffer prop("@"), val;
    while (*cmdProp && *cmdProp != '=')
        prop.append(*cmdProp++);
    if (*cmdProp)
    {
        cmdProp++;
        while (isspace(*cmdProp))
            cmdProp++;
        while (*cmdProp)
            val.append(*cmdProp++);
        prop.clip();
        val.clip();
        if (prop.length())
            tree->setProp(prop.str(), val.str());
    }
}

const LogMsgJobInfo thorJob(UnknownJob, UnknownUser); // may be improved later

void ensureDirectoryForFile(const char *fName)
{
    if (!recursiveCreateDirectoryForFile(fName))
        throw makeOsExceptionV(GetLastError(), "Failed to create directory for file: %s", fName);
}

// Not recommended to be used from slaves as tend to be one or more trying at same time.
void reportExceptionToWorkunit(IConstWorkUnit &workunit,IException *e, ErrorSeverity severity)
{
    LOG(MCwarning, thorJob, e, "Reporting exception to WU");
    Owned<IWorkUnit> wu = &workunit.lock();
    if (wu)
    {
        Owned<IWUException> we = wu->createException();
        StringBuffer s;
        we->setExceptionMessage(e->errorMessage(s.clear()).str());
        we->setExceptionCode(e->errorCode());
        IThorException *te = QUERYINTERFACE(e, IThorException);
        if (te)
        {
            we->setSeverity(te->querySeverity());
            if (!te->queryOrigin()) // will have an origin if from slaves already
                te->setOrigin("master");
            we->setExceptionSource(te->queryOrigin());
            StringAttr file;
            unsigned line, column;
            te->getAssert(file, line, column);
            if (file.length())
                we->setExceptionFileName(file);
            if (line || column)
            {
                we->setExceptionLineNo(line);
                we->setExceptionColumn(column);
            }
        }
        else
            we->setSeverity(severity);
    }
} 

StringBuffer &getCompoundQueryName(StringBuffer &compoundName, const char *queryName, unsigned version)
{
    return compoundName.append('V').append(version).append('_').append(queryName);
}

void setClusterGroup(INode *_masterNode, IGroup *_rawGroup, unsigned slavesPerNode, unsigned channelsPerSlave, unsigned portBase, unsigned portInc)
{
    ::Release(masterNode);
    ::Release(rawGroup);
    ::Release(nodeGroup);
    ::Release(clusterGroup);
    ::Release(slaveGroup);
    ::Release(dfsGroup);
    ::Release(nodeComm);
    masterNode = LINK(_masterNode);
    rawGroup = LINK(_rawGroup);

    SocketEndpointArray epa;
    OwnedMalloc<unsigned> hostStartPort, hostNextStartPort;
    hostStartPort.allocateN(rawGroup->ordinality());
    hostNextStartPort.allocateN(rawGroup->ordinality());
    for (unsigned n=0; n<rawGroup->ordinality(); n++)
    {
        SocketEndpoint ep = rawGroup->queryNode(n).endpoint();
        unsigned hostPos = epa.find(ep);
        if (NotFound == hostPos)
        {
            hostPos = epa.ordinality();
            epa.append(ep);
            hostStartPort[n] = portBase;
            hostNextStartPort[hostPos] = portBase + (slavesPerNode * channelsPerSlave * portInc);
        }
        else
        {
            hostStartPort[n] = hostNextStartPort[hostPos];
            hostNextStartPort[hostPos] += (slavesPerNode * channelsPerSlave * portInc);
        }
    }
    IArrayOf<INode> clusterGroupNodes, nodeGroupNodes;
    clusterGroupNodes.append(*LINK(masterNode));
    nodeGroupNodes.append(*LINK(masterNode));
    for (unsigned p=0; p<slavesPerNode; p++)
    {
        for (unsigned s=0; s<channelsPerSlave; s++)
        {
            for (unsigned n=0; n<rawGroup->ordinality(); n++)
            {
                SocketEndpoint ep = rawGroup->queryNode(n).endpoint();
                ep.port = hostStartPort[n] + (((p * channelsPerSlave) + s) * portInc);
                Owned<INode> node = createINode(ep);
                clusterGroupNodes.append(*node.getLink());
                if (0 == s)
                    nodeGroupNodes.append(*node.getLink());
            }
        }
    }
    // clusterGroup contains master + all slaves (including virtuals)
    clusterGroup = createIGroup(clusterGroupNodes.ordinality(), clusterGroupNodes.getArray());

    // nodeGroup container master + all slave processes (excludes virtual slaves)
    nodeGroup = createIGroup(nodeGroupNodes.ordinality(), nodeGroupNodes.getArray());

    // slaveGroup contains all slaves (including virtuals) but excludes master
    slaveGroup = clusterGroup->remove(0);

    // dfsGroup is same as slaveGroup, but stripped of ports. So is a IP group as wide as slaveGroup, used for publishing
    IArrayOf<INode> slaveGroupNodes;
    Owned<INodeIterator> nodeIter = slaveGroup->getIterator();
    ForEach(*nodeIter)
    slaveGroupNodes.append(*createINodeIP(nodeIter->query().endpoint(),0));
    dfsGroup = createIGroup(slaveGroupNodes.ordinality(), slaveGroupNodes.getArray());

    nodeComm = createCommunicator(nodeGroup);
}
bool clusterInitialized() { return NULL != nodeComm; }
INode &queryMasterNode() { return *masterNode; }
ICommunicator &queryNodeComm() { return *nodeComm; }
IGroup &queryRawGroup() { return *rawGroup; }
IGroup &queryNodeGroup() { return *nodeGroup; }
IGroup &queryClusterGroup() { return *clusterGroup; }
IGroup &querySlaveGroup() { return *slaveGroup; }
IGroup &queryDfsGroup() { return *dfsGroup; }
unsigned queryClusterWidth() { return clusterGroup->ordinality()-1; }
unsigned queryNodeClusterWidth() { return nodeGroup->ordinality()-1; }


mptag_t allocateClusterMPTag()
{
    return ClusterMPAllocator->alloc();
}

void freeClusterMPTag(mptag_t tag)
{
    ClusterMPAllocator->release(tag);
}

IThorException *deserializeThorException(MemoryBuffer &in)
{
    unsigned te;
    in.read(te);
    if (!te)
    {
        Owned<IException> e = deserializeException(in);
        StringBuffer s;
        return new CThorException(e->errorAudience(), e->errorCode(), e->errorMessage(s).str());
    }
    return new CThorException(in);
}

void serializeThorException(IException *e, MemoryBuffer &out)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    if (!te)
    {
        out.append(0);
        serializeException(e, out);
        return;
    }
    out.append(1);
    out.append((unsigned)te->queryAction());
    out.append(te->queryJobId());
    out.append(te->queryGraphId());
    out.append((unsigned)te->queryActivityKind());
    out.append(te->queryActivityId());
    out.append((unsigned)te->errorAudience());
    out.append(te->errorCode());
    out.append(te->queryMessage());
    StringAttr file;
    unsigned line, column;
    te->getAssert(file, line, column);
    out.append(file);
    out.append(line);
    out.append(column);
    out.append(te->querySeverity());
    out.append(te->queryOrigin());
    MemoryBuffer &data = te->queryData();
    out.append((size32_t)data.length());
    if (data.length())
        out.append(data.length(), data.toByteArray());
}

bool getBestFilePart(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path, IExceptionHandler *eHandler)
{
    if (0 == partDesc.numCopies()) // not sure this is poss.
        return false;
    SocketEndpoint slfEp((unsigned short)0);
    unsigned l;

    RemoteFilename rfn;
    StringBuffer locationName, primaryName;
    //First check for local matches
    for (l=0; l<partDesc.numCopies(); l++)
    {
        rfn.clear();
        partDesc.getFilename(l, rfn);
        if (0 == l)
        {
            rfn.getPath(locationName.clear());
            assertex(locationName.length());
            primaryName.append(locationName);
            locationName.clear();
        }
        if (rfn.isLocal())
        {
            rfn.getPath(locationName.clear());
            assertex(locationName.length());
            Owned<IFile> file = createIFile(locationName.str());
            try
            {
                if (file->exists())
                {
                    ifile.set(file);
                    location = l;
                    path.append(locationName);
                    return true;
                }
            }
            catch (IException *e)
            {
                ActPrintLog(&activity->queryContainer(), e, "getBestFilePart");
                e->Release();
            }
        }
    }

    //Now check for a remote match...
    for (l=0; l<partDesc.numCopies(); l++)
    {
        rfn.clear();
        partDesc.getFilename(l, rfn);
        if (!rfn.isLocal())
        {
            rfn.getPath(locationName.clear());
            assertex(locationName.length());
            Owned<IFile> file = createIFile(locationName.str());
            try
            {
                if (file->exists())
                {
                    ifile.set(file);
                    location = l;
                    if (0 != l)
                    {
                        Owned<IThorException> e = MakeActivityWarning(activity, 0, "Primary file missing: %s, using remote copy: %s", primaryName.str(), locationName.str());
                        if (!eHandler)
                            throw e.getClear();
                        eHandler->fireException(e);
                    }
                    path.append(locationName);
                    return true;
                }
            }
            catch (IException *e)
            {
                ActPrintLog(&activity->queryContainer(), e, "In getBestFilePart");
                e->Release();
            }
        }
    }
    return false;
}

StringBuffer &getFilePartLocations(IPartDescriptor &partDesc, StringBuffer &locations)
{
    unsigned l;
    for (l=0; l<partDesc.numCopies(); l++)
    {
        RemoteFilename rfn;
        partDesc.getFilename(l, rfn);
        rfn.getRemotePath(locations);
        if (l != partDesc.numCopies()-1)
            locations.append(", ");
    }
    return locations;
}

StringBuffer &getPartFilename(IPartDescriptor &partDesc, unsigned copy, StringBuffer &filePath, bool localMount)
{
    RemoteFilename rfn;
    if (localMount && copy)
    {
        partDesc.getFilename(0, rfn);
        if (!rfn.isLocal())
            localMount = false;
        rfn.clear();
    }
    partDesc.getFilename(copy, rfn);
    rfn.getPath(filePath);
    return filePath;
}

// CFifoFileCache impl.

void CFifoFileCache::deleteFile(IFile &ifile)
{
    try 
    {
        if (!ifile.remove())
            FLLOG(MCoperatorWarning, thorJob, "CFifoFileCache: Failed to remove file (missing) : %s", ifile.queryFilename());
    }
    catch (IException *e)
    {
        StringBuffer s("Failed to remove file: ");
        FLLOG(MCoperatorWarning, thorJob, e, s.append(ifile.queryFilename()));
    }
}

void CFifoFileCache::init(const char *cacheDir, unsigned _limit, const char *pattern)
{
    limit = _limit;
    Owned<IDirectoryIterator> iter = createDirectoryIterator(cacheDir, pattern);
    ForEach (*iter)
    {
        IFile &file = iter->query();
        if (file.isFile())
            deleteFile(file);
    }
}

void CFifoFileCache::add(const char *filename)
{
    unsigned pos = files.find(filename);
    if (NotFound != pos)
        files.remove(pos);
    files.add(filename, 0);
    if (files.ordinality() > limit)
    {
        const char *toRemoveFname = files.item(limit);
        PROGLOG("Removing %s from fifo cache", toRemoveFname);
        OwnedIFile ifile = createIFile(toRemoveFname);
        deleteFile(*ifile);
        files.remove(limit);
    }
}

bool CFifoFileCache::isAvailable(const char *filename)
{
    unsigned pos = files.find(filename);
    if (NotFound != pos)
    {
        OwnedIFile ifile = createIFile(filename);
        if (ifile->exists())
            return true;
    }
    return false;
}

IOutputMetaData *createFixedSizeMetaData(size32_t sz)
{
    // sure if this allowed or is cheating!
    return new thormisc::CFixedOutputMetaData(sz);
}


class CRowStreamFromNode : public CSimpleInterface, implements IRowStream
{
    CActivityBase &activity;
    unsigned node, myNode;
    ICommunicator &comm;
    MemoryBuffer mb;
    bool eos;
    const bool &abortSoon;
    mptag_t mpTag, replyTag;
    Owned<ISerialStream> bufferStream;
    CThorStreamDeserializerSource memDeserializer;
    CMessageBuffer msg;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowStreamFromNode(CActivityBase &_activity, unsigned _node, ICommunicator &_comm, mptag_t _mpTag, const bool &_abortSoon) : activity(_activity), node(_node), comm(_comm), mpTag(_mpTag), abortSoon(_abortSoon)
    {
        bufferStream.setown(createMemoryBufferSerialStream(mb));
        memDeserializer.setStream(bufferStream);
        myNode = comm.queryGroup().rank(activity.queryMPServer().queryMyNode());
        replyTag = activity.queryMPServer().createReplyTag();
        msg.setReplyTag(replyTag);
        eos = false;
    }
// IRowStream
    const void *nextRow()
    {
        if (eos) return NULL;

        loop
        {
            while (!memDeserializer.eos()) 
            {
                RtlDynamicRowBuilder rowBuilder(activity.queryRowAllocator());
                size32_t sz = activity.queryRowDeserializer()->deserialize(rowBuilder, memDeserializer);
                return rowBuilder.finalizeRowClear(sz);
            }
            // no msg just give me data
            if (!comm.send(msg, node, mpTag, LONGTIMEOUT)) // should never timeout, unless other end down
                throw MakeStringException(0, "CRowStreamFromNode: Failed to send data request from node %d, to node %d", myNode, node);
            loop
            {
                if (abortSoon)
                    break;
                if (comm.recv(msg, node, replyTag, NULL, 60000))
                    break;
                ActPrintLog(&activity, "CRowStreamFromNode, request more from node %d, tag %d timedout, retrying", node, mpTag);
            }
            if (!msg.length())
                break;
            if (abortSoon)
                break;
            msg.swapWith(mb);
            msg.clear();
        }
        eos = true;
        return NULL;
    }
    void stop()
    {
        CMessageBuffer msg;
        msg.append(1); // stop
        verifyex(comm.send(msg, node, mpTag));
    }
};

IRowStream *createRowStreamFromNode(CActivityBase &activity, unsigned node, ICommunicator &comm, mptag_t mpTag, const bool &abortSoon)
{
    return new CRowStreamFromNode(activity, node, comm, mpTag, abortSoon);
}

#define DEFAULT_ROWSERVER_BUFF_SIZE                 (0x10000)               // 64K
class CRowServer : public CSimpleInterface, implements IThreaded, implements IRowServer
{
    CThreaded threaded;
    ICommunicator &comm;
    CActivityBase *activity;
    mptag_t mpTag;
    unsigned myNode, fetchBuffSize;
    Linked<IRowStream> seq;
    bool running;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowServer(CActivityBase *_activity, IRowStream *_seq, ICommunicator &_comm, mptag_t _mpTag) 
        : activity(_activity), seq(_seq), comm(_comm), mpTag(_mpTag), threaded("CRowServer")
    {
        fetchBuffSize = DEFAULT_ROWSERVER_BUFF_SIZE;
        running = true;
        threaded.init(this);
    }
    ~CRowServer()
    {
        stop();
        threaded.join();
    }
    virtual void main()
    {
        CMessageBuffer mb;
        while (running)
        {
            rank_t sender;
            if (comm.recv(mb, RANK_ALL, mpTag, &sender))
            {
                unsigned code;
                if (mb.length())
                {
                    mb.read(code);
                    if (1 == code) // stop
                    {
                        seq->stop();
                        break;
                    }
                    else
                        throwUnexpected();
                }
                mb.clear();
                CMemoryRowSerializer mbs(mb);
                do
                {
                    OwnedConstThorRow row = seq->nextRow();
                    if (!row)
                        break;
                    activity->queryRowSerializer()->serialize(mbs,(const byte *)row.get());
                } while (mb.length() < fetchBuffSize); // NB: allows at least 1
                if (!comm.reply(mb, LONGTIMEOUT))
                    throw MakeStringException(0, "CRowStreamFromNode: Failed to send data back to node: %d", activity->queryContainer().queryJobChannel().queryMyRank());
                mb.clear();
            }
        }
        running = false;
    }
    void stop() { running = false; comm.cancel(RANK_ALL, mpTag); }
};

IRowServer *createRowServer(CActivityBase *activity, IRowStream *seq, ICommunicator &comm, mptag_t mpTag)
{
    return new CRowServer(activity, seq, comm, mpTag);
}

IRowStream *createUngroupStream(IRowStream *input)
{
    class CUngroupStream : public CSimpleInterface, implements IRowStream
    {
        IRowStream *input;
    public:
        CUngroupStream(IRowStream *_input) : input(_input) { input->Link(); }
        ~CUngroupStream() { input->Release(); }
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        virtual const void *nextRow() 
        {
            const void *ret = input->nextRow(); 
            if (ret) 
                return ret;
            else
                return input->nextRow();
        }
        virtual void stop()
        {
            input->stop();
        }
    };
    return new CUngroupStream(input);
}

void sendInChunks(ICommunicator &comm, rank_t dst, mptag_t mpTag, IRowStream *input, IRowInterfaces *rowIf)
{
    CMessageBuffer msg;
    MemoryBuffer mb;
    CMemoryRowSerializer mbs(mb);
    IOutputRowSerializer *serializer = rowIf->queryRowSerializer();
    loop
    {
        loop
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            serializer->serialize(mbs, (const byte *)row.get());
            if (mb.length() > 0x80000)
                break;
        }
        msg.clear();
        if (mb.length())
        {
            msg.append(false); // no error
            ThorCompress(mb.toByteArray(), mb.length(), msg);
            mb.clear();
        }
        comm.send(msg, dst, mpTag, LONGTIMEOUT);
        if (0 == msg.length())
            break;
    }
}

void logDiskSpace()
{
    StringBuffer diskSpaceMsg("Disk space: ");
    diskSpaceMsg.append(queryBaseDirectory(grp_unknown, 0)).append(" = ").append(getFreeSpace(queryBaseDirectory(grp_unknown, 0))/0x100000).append(" MB, ");
    diskSpaceMsg.append(queryBaseDirectory(grp_unknown, 1)).append(" = ").append(getFreeSpace(queryBaseDirectory(grp_unknown, 1))/0x100000).append(" MB, ");
    const char *tempDir = globals->queryProp("@thorTempDirectory");
    diskSpaceMsg.append(tempDir).append(" = ").append(getFreeSpace(tempDir)/0x100000).append(" MB");
    PROGLOG("%s", diskSpaceMsg.str());
}

IPerfMonHook *createThorMemStatsPerfMonHook(CJobBase &job, int maxLevel, IPerfMonHook *chain)
{
    class CPerfMonHook : public CSimpleInterfaceOf<IPerfMonHook>
    {
        CJobBase &job;
        int maxLevel;
        Linked<IPerfMonHook> chain;
    public:
        CPerfMonHook(CJobBase &_job, unsigned _maxLevel, IPerfMonHook *_chain) : chain(_chain), maxLevel(_maxLevel), job(_job)
        {
        }
        virtual void processPerfStats(unsigned processorUsage, unsigned memoryUsage, unsigned memoryTotal, unsigned __int64 firstDiskUsage, unsigned __int64 firstDiskTotal, unsigned __int64 secondDiskUsage, unsigned __int64 secondDiskTotal, unsigned threadCount)
        {
            if (chain)
                chain->processPerfStats(processorUsage, memoryUsage, memoryTotal, firstDiskUsage,firstDiskTotal, secondDiskUsage, secondDiskTotal, threadCount);
        }
        virtual StringBuffer &extraLogging(StringBuffer &extra)
        {
            if (chain)
                return chain->extraLogging(extra);
            return extra;
        }
        virtual void log(int level, const char *msg)
        {
            PROGLOG("%s", msg);
            if ((maxLevel != -1) && (level <= maxLevel)) // maxLevel of -1 means disabled
            {
                Owned<IThorException> e = MakeThorException(TE_KERN, "%s", msg);
                e->setSeverity(SeverityAlert);
                e->setAction(tea_warning);
                job.fireException(e);
            }
        }
    };
    return new CPerfMonHook(job, maxLevel, chain);
}


const StatisticsMapping spillStatistics(StTimeSpillElapsed, StTimeSortElapsed, StNumSpills, StSizeSpillFile, StKindNone);
