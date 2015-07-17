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

#include <limits.h>
#include <stdlib.h>
#include "jprop.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jlzw.hpp"
#include "jsocket.hpp"
#include "jset.hpp"
#include "jsort.hpp"
#include "portlist.h"
#include "jhtree.hpp"
#include "mputil.hpp"
#include "dllserver.hpp"
#include "dautils.hpp"
#include "danqs.hpp"
#include "daclient.hpp"
#include "daaudit.hpp"
#include "wujobq.hpp"
#include "thorport.hpp"
#include "commonext.hpp"
#include "thorxmlread.hpp"
#include "thorplugin.hpp"
#include "thormisc.hpp"
#include "thgraphmaster.ipp"
#include "thdemonserver.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thactivitymaster.ipp"
#include "thmem.hpp"
#include "thcompressutil.hpp"

using roxiemem::OwnedRoxieString;

static CriticalSection *jobManagerCrit;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    jobManagerCrit = new CriticalSection;
    return true;
}
MODULE_EXIT()
{
    delete jobManagerCrit;
}

unsigned uniqGraphId = 1;

#define FATAL_TIMEOUT 60
class CFatalHandler : public CTimeoutTrigger, implements IFatalHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CFatalHandler(unsigned timeout) : CTimeoutTrigger(timeout, "EXCEPTION")
    {
    }
    virtual bool action()
    {
        StringBuffer s("FAILED TO RECOVER FROM EXCEPTION, STOPPING THOR");
        FLLOG(MCoperatorWarning, thorJob, exception, s.str());
        Owned<IJobManager> jobManager = getJobManager();
        if (jobManager)
        {
            jobManager->fatal(exception);
            jobManager.clear();
        }
        return true;
    }
// IFatalHandler
    virtual void inform(IException *e)
    {
        CTimeoutTrigger::inform(e);
    }
    virtual void clear()
    {
        CTimeoutTrigger::clear();
    }
};

/////

CSlaveMessageHandler::CSlaveMessageHandler(CJobMaster &_job, mptag_t _mptag) : threaded("CSlaveMessageHandler"), job(_job), mptag(_mptag)
{
    stopped = false;
    threaded.init(this);
}

CSlaveMessageHandler::~CSlaveMessageHandler()
{
    stop();
}

void CSlaveMessageHandler::stop()
{
    if (!stopped)
    {
        stopped = true;
        job.queryJobComm().cancel(0, mptag);
        threaded.join();
    }
}

void CSlaveMessageHandler::main()
{
    try
    {
        loop
        {
            rank_t sender;
            CMessageBuffer msg;
            if (stopped || !job.queryJobComm().recv(msg, RANK_ALL, mptag, &sender))
                break;
            unsigned slave = ((unsigned)sender)-1;
            SlaveMsgTypes msgType;
            msg.read((int &)msgType);
            switch (msgType)
            {
                case smt_errorMsg:
                {
                    Owned<IThorException> e = deserializeThorException(msg);
                    e->setSlave(sender);
                    Owned<CGraphBase> graph = job.getGraph(e->queryGraphId());
                    if (graph)
                    {
                        activity_id id = e->queryActivityId();
                        if (id)
                        {
                            CGraphElementBase *elem = graph->queryElement(id);
                            CActivityBase *act = elem->queryActivity();
                            if (act)
                                act->fireException(e);
                            else
                                graph->fireException(e);
                        }
                        else
                            graph->fireException(e);
                    }
                    else
                        job.fireException(e);
                    if (msg.getReplyTag() <= TAG_REPLY_BASE)
                    {
                        msg.clear();
                        job.queryJobComm().reply(msg);
                    }
                    break;
                }
                case smt_dataReq:
                {
                    graph_id gid;
                    activity_id id;
                    unsigned slave;
                    msg.read(slave);
                    msg.read(gid);
                    msg.read(id);
                    msg.clear();
                    Owned<CGraphBase> graph = job.getGraph(gid);
                    if (graph)
                    {
                        CMasterGraphElement *e = (CMasterGraphElement *)graph->queryElement(id);
                        e->queryActivity()->getInitializationData(slave, msg);
                    }
                    job.queryJobComm().reply(msg);
                    break;
                }
                case smt_initGraphReq:
                {
                    graph_id gid;
                    msg.read(gid);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.getGraph(gid);
                    assertex(graph);
                    {
                        CriticalBlock b(graph->queryCreateLock());
                        Owned<IThorActivityIterator> iter = graph->getIterator();
                        // onCreate all
                        ForEach (*iter)
                        {
                            CMasterGraphElement &element = (CMasterGraphElement &)iter->query();
                            element.onCreate();
                        }
                    }
                    msg.clear();
                    graph->serializeCreateContexts(msg);
                    job.queryJobComm().reply(msg);
                    break;
                }
                case smt_initActDataReq:
                {
                    graph_id gid;
                    msg.read(gid);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.getGraph(gid);
                    assertex(graph);
                    CGraphElementArray toSerialize;
                    CriticalBlock b(graph->queryCreateLock());
                    size32_t parentExtractSz;
                    msg.read(parentExtractSz);
                    const byte *parentExtract = NULL;
                    if (parentExtractSz)
                    {
                        parentExtract = msg.readDirect(parentExtractSz);
                        StringBuffer msg("Graph(");
                        msg.append(graph->queryGraphId()).append(") - initializing master graph with parentExtract ").append(parentExtractSz).append(" bytes");
                        DBGLOG("%s", msg.str());
                        parentExtract = graph->setParentCtx(parentExtractSz, parentExtract);
                    }
                    loop
                    {
                        activity_id id;
                        msg.read(id);
                        if (!id)
                            break;
                        CMasterGraphElement *element = (CMasterGraphElement *)graph->queryElement(id);
                        assertex(element);
                        element->deserializeStartContext(msg);
                        element->doCreateActivity(parentExtractSz, parentExtract);
                        CActivityBase *activity = element->queryActivity();
                        if (activity && activity->needReInit())
                            element->sentActInitData->set(slave, 0); // clear to permit serializeActivityInitData to resend
                        toSerialize.append(*LINK(element));
                    }
                    msg.clear();
                    CMessageBuffer replyMsg;
                    mptag_t replyTag = createReplyTag();
                    msg.append(replyTag); // second reply
                    replyMsg.setReplyTag(replyTag);
                    CGraphElementArrayIterator iter(toSerialize);
                    graph->serializeActivityInitData(((unsigned)sender)-1, msg, iter);
                    job.queryJobComm().reply(msg);
                    if (!job.queryJobComm().recv(msg, sender, replyTag, NULL, MEDIUMTIMEOUT))
                        throwUnexpected();
                    bool error;
                    msg.read(error);
                    if (error)
                    {
                        Owned<IThorException> e = deserializeThorException(msg);
                        e->setSlave(sender);
                        StringBuffer tmpStr("Slave ");
                        job.queryJobGroup().queryNode(sender).endpoint().getUrlStr(tmpStr);
                        GraphPrintLog(graph, e, "%s", tmpStr.append(": slave initialization error").str());
                        throw e.getClear();
                    }
                    break;
                }
                case smt_getPhysicalName:
                {
                    LOG(MCdebugProgress, unknownJob, "getPhysicalName called from slave %d", sender-1);
                    StringAttr logicalName;
                    unsigned partNo;
                    bool create;
                    msg.read(logicalName);
                    msg.read(partNo);
                    msg.read(create);
                    msg.clear();
                    StringBuffer phys;
                    if (create && !job.queryCreatedFile(logicalName)) // not sure who would do this ever??
                        queryThorFileManager().getPublishPhysicalName(job, logicalName, partNo, phys); 
                    else
                        queryThorFileManager().getPhysicalName(job, logicalName, partNo, phys);
                    msg.append(phys);
                    job.queryJobComm().reply(msg);
                    break;
                }
                case smt_getFileOffset:
                {
                    LOG(MCdebugProgress, unknownJob, "getFileOffset called from slave %d", sender-1);
                    StringAttr logicalName;
                    unsigned partNo;
                    msg.read(logicalName);
                    msg.read(partNo);
                    msg.clear();
                    offset_t offset = queryThorFileManager().getFileOffset(job, logicalName, partNo);
                    msg.append(offset);
                    job.queryJobComm().reply(msg);
                    break;
                }
                case smt_actMsg:
                {
                    LOG(MCdebugProgress, unknownJob, "smt_actMsg called from slave %d", sender-1);
                    graph_id gid;
                    msg.read(gid);
                    activity_id id;
                    msg.read(id);
                    Owned<CMasterGraph> graph = (CMasterGraph *)job.getGraph(gid);
                    assertex(graph);
                    CMasterGraphElement *container = (CMasterGraphElement *)graph->queryElement(id);
                    assertex(container);
                    CMasterActivity *activity = (CMasterActivity *)container->queryActivity();
                    assertex(activity);
                    activity->handleSlaveMessage(msg); // don't block
                    break;
                }
                case smt_getresult:
                {
                    LOG(MCdebugProgress, unknownJob, "smt_getresult called from slave %d", sender-1);
                    graph_id gid;
                    msg.read(gid);
                    activity_id ownerId;
                    msg.read(ownerId);
                    unsigned resultId;
                    msg.read(resultId);
                    mptag_t replyTag = job.deserializeMPTag(msg);
                    Owned<IThorResult> result = job.getOwnedResult(gid, ownerId, resultId);
                    Owned<IRowStream> resultStream = result->getRowStream();
                    sendInChunks(job.queryJobComm(), sender, replyTag, resultStream, result->queryRowInterfaces());
                    break;
                }
            }
        }
    }
    catch (IException *e)
    {
        job.fireException(e);
        e->Release();
    }
}

//////////////////////

CMasterActivity::CMasterActivity(CGraphElementBase *_container) : CActivityBase(_container), threaded("CMasterActivity", this)
{
    notedWarnings = createThreadSafeBitSet();
    mpTag = TAG_NULL;
    data = new MemoryBuffer[container.queryJob().querySlaves()];
    asyncStart = false;
    if (container.isSink())
        progressInfo.append(*new ProgressInfo);
    else
    {
        unsigned o=0;
        for (; o<container.getOutputs(); o++)
            progressInfo.append(*new ProgressInfo);
    }
}

CMasterActivity::~CMasterActivity()
{
    if (asyncStart)
        threaded.join();
    notedWarnings->Release();
    container.queryJob().freeMPTag(mpTag);
    delete [] data;
}

void CMasterActivity::addReadFile(IDistributedFile *file, bool temp)
{
    readFiles.append(*LINK(file));
    if (!temp) // NB: Temps not listed in workunit
        queryThorFileManager().noteFileRead(container.queryJob(), file);
}

IDistributedFile *CMasterActivity::queryReadFile(unsigned f)
{
    if (f>=readFiles.ordinality())
        return NULL;
    return &readFiles.item(f);
}

void CMasterActivity::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    CActivityBase::preStart(parentExtractSz, parentExtract);
    IArrayOf<IDistributedFile> tmpFiles;
    tmpFiles.swapWith(readFiles);
    ForEachItemIn(f, tmpFiles)
    {
        IDistributedFile &file = tmpFiles.item(f);
        IDistributedSuperFile *super = file.querySuperFile();
        if (super)
            getSuperFileSubs(super, readFiles, true);
        else
            readFiles.append(*LINK(&file));
    }
}

MemoryBuffer &CMasterActivity::queryInitializationData(unsigned slave) const
{ // NB: not intended to be called by multiple threads.
    return data[slave].reset();
}

MemoryBuffer &CMasterActivity::getInitializationData(unsigned slave, MemoryBuffer &dst) const
{
    return dst.append(data[slave]);
}

void CMasterActivity::main()
{
    try
    {
        process();
    }
    catch (IException *e)
    {
        Owned<IException> e2;
        if (QUERYINTERFACE(e, ISEH_Exception))
            e2.setown(MakeThorFatal(e, TE_SEH, "(SEH)"));
        else
            e2.setown(MakeActivityException(this, e, "Master exception"));
        e->Release();
        ActPrintLog(e2, "In CMasterActivity::main");
        fireException(e2);
    }
    catch (CATCHALL)
    {
        Owned<IException> e = MakeThorFatal(NULL, TE_MasterProcessError, "FATAL: Unknown master process exception kind=%s, id=%" ACTPF "d", activityKindStr(container.getKind()), container.queryId());
        ActPrintLog(e, "In CMasterActivity::main");
        fireException(e);
    }
}

void CMasterActivity::init()
{
    readFiles.kill();
}

void CMasterActivity::startProcess(bool async)
{
    if (async)
    {
        asyncStart = true;
        threaded.start();
    }
    else
        main();
}

bool CMasterActivity::wait(unsigned timeout)
{
    if (!asyncStart)
        return true;
    return threaded.join(timeout);
}

void CMasterActivity::kill()
{
    CActivityBase::kill();
    readFiles.kill();
}

bool CMasterActivity::fireException(IException *_e)
{
    IThorException *e = QUERYINTERFACE(_e, IThorException);
    if (!e) return false;
    switch (e->errorCode())
    {
        case TE_LargeBufferWarning:
        case TE_MoxieIndarOverflow:
        case TE_BuildIndexFewExcess:
        case TE_FetchMisaligned:
        case TE_FetchOutOfRange:
        case TE_CouldNotCreateLookAhead:
        case TE_SpillAdded:
        case TE_ReadPartialFromPipe:
        case TE_LargeAggregateTable:
        {
            if (!notedWarnings->testSet(e->errorCode()))
                reportExceptionToWorkunit(container.queryJob().queryWorkUnit(), e);
            return true;
        }
    }
    return container.queryOwner().fireException(e);
}

void CMasterActivity::reset()
{
    asyncStart = false;
    CActivityBase::reset();
}

void CMasterActivity::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CriticalBlock b(progressCrit); // don't think needed
    unsigned __int64 localTimeNs;
    mb.read(localTimeNs);
    timingInfo.set(node, localTimeNs);
    rowcount_t count;
    ForEachItemIn(p, progressInfo)
    {
        mb.read(count);
        progressInfo.item(p).set(node, count);
    }
}

void CMasterActivity::getActivityStats(IStatisticGatherer & stats)
{
    timingInfo.getStats(stats);
}

void CMasterActivity::getEdgeStats(IStatisticGatherer & stats, unsigned idx)
{
    CriticalBlock b(progressCrit);
    if (progressInfo.isItem(idx))
        progressInfo.item(idx).getStats(stats);
}

void CMasterActivity::done()
{
    CActivityBase::done();
    ForEachItemIn(s, readFiles)
    {
        IDistributedFile &file = readFiles.item(s);
        file.setAccessed();
    }
}

//////////////////////
// CMasterGraphElement impl.
//

CMasterGraphElement::CMasterGraphElement(CGraphBase &_owner, IPropertyTree &_xgmml) : CGraphElementBase(_owner, _xgmml)
{
    sentCreateCtx = false;
}

bool CMasterGraphElement::checkUpdate()
{
    if (!onlyUpdateIfChanged)
        return false;
    if (!globals->getPropBool("@updateEnabled", true) || 0 != queryJob().getWorkUnitValueInt("disableUpdate", 0))
        return false;

    bool doCheckUpdate = false;
    OwnedRoxieString filename;
    unsigned eclCRC;
    unsigned __int64 totalCRC;
    bool temporary = false;
    switch (getKind())
    {
        case TAKindexwrite:
        {
            IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper();
            doCheckUpdate = 0 != (helper->getFlags() & TIWupdate);
            filename.set(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            break;
        }
        case TAKdiskwrite:
        case TAKcsvwrite:
        case TAKxmlwrite:
        case TAKjsonwrite:
        {
            IHThorDiskWriteArg *helper = (IHThorDiskWriteArg *)queryHelper();
            doCheckUpdate = 0 != (helper->getFlags() & TDWupdate);
            filename.set(helper->getFileName());
            helper->getUpdateCRCs(eclCRC, totalCRC);
            if (TAKdiskwrite == getKind())
                temporary = 0 != (helper->getFlags() & (TDXtemporary|TDXjobtemp));
            break;
        }
    }

    if (doCheckUpdate)
    {
        StringAttr lfn;
        Owned<IDistributedFile> file = queryThorFileManager().lookup(queryJob(), filename, temporary, true);
        if (file)
        {
            IPropertyTree &props = file->queryAttributes();
            if ((eclCRC == props.getPropInt("@eclCRC")) && (totalCRC == props.getPropInt64("@totalCRC")))
            {
                // so this needs pruning
                Owned<IThorException> e = MakeActivityWarning(this, TE_UpToDate, "output file = '%s' - is up to date - it will not be rebuilt", file->queryLogicalName());
                queryOwner().fireException(e);
                return true;
            }
        }
    }
    return false;
}

void CMasterGraphElement::initActivity()
{
    CriticalBlock b(crit);
    bool first = (NULL == activity);
    CGraphElementBase::initActivity();
    if (first || activity->needReInit())
        ((CMasterActivity *)activity.get())->init();
}

void CMasterGraphElement::doCreateActivity(size32_t parentExtractSz, const byte *parentExtract)
{
    bool ok=false;
    switch (getKind())
    {
        case TAKspill:
        case TAKdiskwrite:
        case TAKfetch:
        case TAKkeyedjoin:
        case TAKkeyeddenormalize:
        case TAKkeyeddenormalizegroup:
        case TAKworkunitwrite:
        case TAKworkunitread:
        case TAKdictionaryworkunitwrite:
        case TAKdictionaryresultwrite:
            ok = true;
            break;
        default:
        {
            if (isDiskInput(getKind()))
                ok = true;
            else if (!queryLocalOrGrouped())
                ok = true;
            break;
        }
    }
    if (!ok)
        return;
    onCreate();
    if (isDiskInput(getKind()))
       onStart(parentExtractSz, parentExtract);
    initActivity();
}

void CMasterGraphElement::slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
{
    ((CMasterActivity *)activity.get())->slaveDone(slaveIdx, mb);
}


//////

///////
class CBarrierMaster : public CInterface, implements IBarrier
{
    mptag_t tag;
    Linked<ICommunicator> comm;
    bool receiving;
public:
    IMPLEMENT_IINTERFACE;

    CBarrierMaster(ICommunicator &_comm, mptag_t _tag) : comm(&_comm), tag(_tag)
    {
        receiving = false;
    }
    virtual const mptag_t queryTag() const { return tag; }
    virtual bool wait(bool exception, unsigned timeout)
    {
        Owned<IException> e;
        CTimeMon tm(timeout);
        unsigned s=comm->queryGroup().ordinality()-1;
        bool aborted = false;
        CMessageBuffer msg;
        Owned<IBitSet> raisedSet = createThreadSafeBitSet();
        unsigned remaining = timeout;
        while (s--)
        {
            rank_t sender;
            msg.clear();
            if (INFINITE != timeout && tm.timedout(&remaining))
            {
                if (exception)
                    throw createBarrierAbortException();
                else
                    return false;
            }
            {
                BooleanOnOff onOff(receiving);
                if (!comm->recv(msg, RANK_ALL, tag, &sender, remaining))
                    break;
            }
            msg.read(aborted);
            bool hasExcept;
            msg.read(hasExcept);
            if (hasExcept && !e.get())
                e.setown(deserializeException(msg));
            sender = sender - 1; // 0 = master
            if (raisedSet->testSet(sender, true) && !aborted)
                WARNLOG("CBarrierMaster, raise barrier message on tag %d, already received from slave %d", tag, sender);
            if (aborted) break;
        }
        msg.clear();
        msg.append(aborted);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (INFINITE != timeout && tm.timedout(&remaining))
        {
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        if (!comm->send(msg, RANK_ALL_OTHER, tag, INFINITE != timeout ? remaining : LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierMaster::wait - Timeout sending to slaves");
        if (aborted)
        {
            if (!exception)
                return false;
            if (e)
                throw e.getClear();
            else
                throw createBarrierAbortException();
        }
        return true;
    }
    virtual void cancel(IException *e)
    {
        if (receiving)
            comm->cancel(RANK_ALL, tag);
        CMessageBuffer msg;
        msg.append(true);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (!comm->send(msg, RANK_ALL_OTHER, tag, LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierMaster::cancel - Timeout sending to slaves");
    }
};

/////////////


class CMasterGraphTempHandler : public CGraphTempHandler
{
public:
    CMasterGraphTempHandler(CJobBase &job, bool errorOnMissing) : CGraphTempHandler(job, errorOnMissing) { }

    virtual bool removeTemp(const char *name)
    {
        queryThorFileManager().clearCacheEntry(name);
        return true;
    }
    virtual void registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind, StringArray *clusters)
    {
        if (!temp || job.queryUseCheckpoints())
        {
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->addFile(name, clusters, usageCount, fileKind, job.queryGraphName());
        }
        else
            CGraphTempHandler::registerFile(name, graphId, usageCount, temp, fileKind, clusters);
    }
    virtual void deregisterFile(const char *name, bool kept) // NB: only called for temp files
    {
        if (kept || job.queryUseCheckpoints())
        {
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->releaseFile(name);
        }
        else
            CGraphTempHandler::deregisterFile(name);
    }
    virtual void clearTemps()
    {
        try
        {
            if (!job.queryPausing()) // temps of completed workunit will have been preserved and want to keep
            {
                Owned<IWorkUnit> lwu = &job.queryWorkUnit().lock();
                lwu->deleteTempFiles(job.queryGraphName(), false, false);
            }
        }
        catch (IException *e)
        {
            EXCLOG(e, "Problem deleting temp files");
            e->Release();
        }
        CGraphTempHandler::clearTemps();
    }
};

static const char * getResultText(StringBuffer & s, const char * stepname, unsigned sequence)
{
    switch ((int)sequence)
    {
    case -1: return s.append("STORED('").append(stepname).append("')");
    case -2: return s.append("PERSIST('").append(stepname).append("')");
    case -3: return s.append("global('").append(stepname).append("')");
    default:
        if (stepname)
            return s.append(stepname);
        return s.append('#').append(sequence);
    }
}

class CThorCodeContextMaster : public CThorCodeContextBase
{
    Linked<IConstWorkUnit> workunit;
    Owned<IDistributedFileTransaction> superfiletransaction;

    virtual IWorkUnit *updateWorkUnit() const
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        return factory->updateWorkUnit(workunit->queryWuid());
    }
    IWUResult *updateResult(const char *name, unsigned sequence)
    {
        Owned<IWorkUnit> w = updateWorkUnit();
        return updateWorkUnitResult(w, name, sequence);
    }
    IConstWUResult * getResult(const char * name, unsigned sequence)
    {
        return getWorkUnitResult(workunit, name, sequence);
    }
    #define PROTECTED_GETRESULT(STEPNAME, SEQUENCE, KIND, KINDTEXT, ACTION) \
        LOG(MCdebugProgress, unknownJob, "getResult%s(%s,%d)", KIND, STEPNAME?STEPNAME:"", SEQUENCE); \
        Owned<IConstWUResult> r = getResultForGet(STEPNAME, SEQUENCE); \
        try \
        { \
            ACTION \
        } \
        catch (IException * e) { \
            StringBuffer s, text; e->errorMessage(text); e->Release(); \
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "result %s in workunit contains an invalid " KINDTEXT " value [%s]", getResultText(s, STEPNAME, SEQUENCE), text.str()); \
        } \
        catch (CATCHALL)    { StringBuffer s; throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "value %s in workunit contains an invalid " KINDTEXT " value", getResultText(s, STEPNAME, SEQUENCE)); }

public:
    CThorCodeContextMaster(CJobBase &job, IConstWorkUnit &_workunit, ILoadedDllEntry &querySo, IUserDescriptor &userDesc) : CThorCodeContextBase(job, querySo, userDesc), workunit(&_workunit)
    {
    }

// ICodeContext
    virtual void setResultBool(const char *name, unsigned sequence, bool result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultBool(result);   
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultBool");
    }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void *result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultData(result, len);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultDecimal(const char * name, unsigned sequence, int len, int precision, bool isSigned, const void *val)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultDecimal(val, len);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultDecimal");
    }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 result, unsigned size)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultInt(result);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultInt");
    }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void *result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultRaw(len, result, ResultFormatRaw);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultReal(const char *name, unsigned sequence, double result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultReal(result);   
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultReal");
    }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void *result, ISetToXmlTransformer *)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultIsAll(isAll);
            r->setResultRaw(len, result, ResultFormatRaw);  
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultData");
    }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char *result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultString(result, len);    
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultString");
    }
    virtual void setResultUnicode(const char * name, unsigned sequence, int len, UChar const * result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultUnicode(result, len);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultUnicode");
    }
    virtual void setResultVarString(const char * name, unsigned sequence, const char *result)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultString(result, strlen(result)); 
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultVarString");
    }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 result, unsigned size)
    {
        Owned<IWUResult> r = updateResult(name, sequence);
        if (r)
        {
            r->setResultUInt(result);
            r->setResultStatus(ResultStatusCalculated);
        }
        else
            throw MakeStringException(TE_UnexpectedParameters, "Unexpected parameters to setResultUInt");
    }
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val)
    {
        setResultUnicode(stepname, sequence, rtlUnicodeStrlen(val), val);
    }
    virtual bool getResultBool(const char * stepname, unsigned sequence) 
    { 
        PROTECTED_GETRESULT(stepname, sequence, "Bool", "bool",
            return r->getResultBool();
        );
    }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "Data", "data",
            SCMStringBuffer result;
            r->getResultString(result, false);
            tlen = result.length();
            tgt = (char *)result.s.detach();
        );
    }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "Decimal", "decimal",
            r->getResultDecimal(tgt, tlen, precision, isSigned);
        );
    }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
            Variable2IDataVal result(&tlen, &tgt);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        );
    }
    virtual void getResultSet(bool & isAll, unsigned & tlen, void * & tgt, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Raw", "raw",
            Variable2IDataVal result(&tlen, &tgt);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            isAll = r->getResultIsAll();
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        );
    }
    virtual __int64 getResultInt(const char * name, unsigned sequence) 
    { 
        PROTECTED_GETRESULT(name, sequence, "Int", "integer",
            return r->getResultInt();
        );
    }
    virtual double getResultReal(const char * name, unsigned sequence)
    {
        PROTECTED_GETRESULT(name, sequence, "Real", "real",
            return r->getResultReal();
        );
    }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "String", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            tlen = result.length();
            tgt = (char *)result.s.detach();
        );
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "String", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            rtlStrToStr(tlen, tgt, result.length(), result.s.str());
        );
    }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "Unicode", "unicode",
            MemoryBuffer result;
            r->getResultUnicode(MemoryBuffer2IDataVal(result));
            tlen = result.length()/2;
            tgt = (UChar *)malloc(tlen*2);
            memcpy(tgt, result.toByteArray(), tlen*2);
        );
    }
    virtual char * getResultVarString(const char * stepname, unsigned sequence) 
    { 
        PROTECTED_GETRESULT(stepname, sequence, "VarString", "string",
            SCMStringBuffer result;
            r->getResultString(result, false);
            return result.s.detach();
        );
    }
    virtual UChar * getResultVarUnicode(const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "VarUnicode", "unicode",
            MemoryBuffer result;
            r->getResultUnicode(MemoryBuffer2IDataVal(result));
            unsigned tlen = result.length()/2;
            result.append((UChar)0);
            return (UChar *)result.detach();
        );
    }
    virtual unsigned getResultHash(const char * name, unsigned sequence) 
    { 
        PROTECTED_GETRESULT(name, sequence, "Hash", "hash",
            return r->getResultHash();
        );
    }
    virtual unsigned getExternalResultHash(const char * wuid, const char * stepname, unsigned sequence)
    {
        try
        {
            LOG(MCdebugProgress, unknownJob, "getExternalResultRaw %s", stepname);

            Owned<IConstWUResult> r = getExternalResult(wuid, stepname, sequence);
            return r->getResultHash();
        }
        catch (CATCHALL)
        {
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "Failed to retrieve external data hash %s from workunit %s", stepname, wuid);
        }
    }
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Rowset", "rowset",
            MemoryBuffer datasetBuffer;
            MemoryBuffer2IDataVal result(datasetBuffer);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
            Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(this);
            rtlDataset2RowsetX(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray(), isGrouped);
        );
    }
    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher)
    {
        tcount = 0;
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Dictionary", "dictionary",
            MemoryBuffer datasetBuffer;
            MemoryBuffer2IDataVal result(datasetBuffer);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
            Owned<IOutputRowDeserializer> deserializer = _rowAllocator->createDiskDeserializer(this);
            rtlDeserializeDictionary(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray());
        );
    }
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        tgt = NULL;
        try
        {
            LOG(MCdebugProgress, unknownJob, "getExternalResultRaw %s", stepname);

            Variable2IDataVal result(&tlen, &tgt);
            Owned<IConstWUResult> r = getExternalResult(wuid, stepname, sequence);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
        }
        catch (CATCHALL)
        {
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "Failed to retrieve external data value %s from workunit %s", stepname, wuid);
        }
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source)
    {
        DBGLOG("%s", text);
        try
        {
            Owned<IWorkUnit> w = updateWorkUnit();
            Owned<IWUException> we = w->createException();
            we->setSeverity((ErrorSeverity)severity);
            we->setExceptionMessage(text);
            we->setExceptionSource(source);
            if (code)
                we->setExceptionCode(code);
        }
        catch (IException *E)
        {
            StringBuffer m;
            E->errorMessage(m);
            DBGLOG("Unable to record exception in workunit: %s", m.str());
            E->Release();
        }
        catch (...)
        {
            DBGLOG("Unable to record exception in workunit: unknown exception");
        }
    }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        DBGLOG("%s", text);
        try
        {
            Owned<IWorkUnit> w = updateWorkUnit();
            addExceptionToWorkunit(w, SeverityError, "user", code, text, filename, lineno, column);
        }
        catch (IException *E)
        {
            StringBuffer m;
            E->errorMessage(m);
            DBGLOG("Unable to record exception in workunit: %s", m.str());
            E->Release();
        }
        catch (...)
        {
            DBGLOG("Unable to record exception in workunit: unknown exception");
        }
        if (isAbort)
            rtlFailOnAssert();      // minimal implementation
    }
    virtual unsigned __int64 getFileOffset(const char *logicalName) { assertex(false); return 0; }
    virtual unsigned getNodes() { return job.queryJobGroup().ordinality()-1; }
    virtual unsigned getNodeNum() { throw MakeThorException(0, "Unsupported. getNodeNum() called in master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) { assertex(false); return NULL; }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)
    {
        unsigned checkSum = 0;
        Owned<IDistributedFile> iDfsFile = queryThorFileManager().lookup(job, name, false, true, false); // NB: do not update accessed
        if (iDfsFile.get())
        {
            if (iDfsFile->getFileCheckSum(checkSum))
                hash ^= checkSum;
            else
            {
                StringBuffer modifiedStr;
                if (iDfsFile->queryAttributes().getProp("@modified", modifiedStr))
                    hash = rtlHash64Data(modifiedStr.length(), modifiedStr.str(), hash);
                // JCS->GH - what's the best thing to do here, if [for some reason] neither are available..
            }
        }
        return hash;
    }
    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        if (!superfiletransaction.get())
            superfiletransaction.setown(createDistributedFileTransaction(userDesc));
        return superfiletransaction.get();
    }
    virtual char *getJobName()
    {
        return strdup(workunit->queryJobName());
    }
    virtual char *getClusterName()
    {
        return strdup(workunit->queryClusterName());
    }
    virtual char *getGroupName()
    {
        StringBuffer out;
        if (globals)
            globals->getProp("@nodeGroup",out); 
        return out.detach();
    }
// ICodeContextExt impl.
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> externalWU = factory->openWorkUnit(wuid);
        externalWU->remoteCheckAccess(userDesc, false);
        return getWorkUnitResult(externalWU, name, sequence);
    }
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence)
    {
        Owned<IConstWUResult> r = getResult(name, sequence);
        if (!r || (r->getResultStatus() == ResultStatusUndefined))
        {
            StringBuffer s;
            throw MakeStringException(TE_FailedToRetrieveWorkunitValue, "value %s in workunit is undefined", getResultText(s,name,sequence));
        }
        return r.getClear();
    }
};


/////////////

//
// CJobMaster
//

void loadPlugin(SafePluginMap *pluginMap, const char *_path, const char *name)
{
    StringBuffer path(_path);
    path.append(name);

    pluginMap->addPlugin(path.str(), name);
}

CJobMaster::CJobMaster(IConstWorkUnit &_workunit, const char *graphName, const char *_querySo, bool _sendSo, const SocketEndpoint &_agentEp)
    : CJobBase(graphName), workunit(&_workunit), sendSo(_sendSo), agentEp(_agentEp)
{
    SCMStringBuffer _token, _scope;
    workunit->getScope(_scope);
    workunit->getSecurityToken(_token);
    wuid.set(workunit->queryWuid());
    user.set(workunit->queryUser());
    token.append(_token.str());
    scope.append(_scope.str());
    globalMemorySize = globals->getPropInt("@masterMemorySize", globals->getPropInt("@globalMemorySize")); // in MB
    init();

    resumed = WUActionResume == workunit->getAction();
    fatalHandler.setown(new CFatalHandler(globals->getPropInt("@fatal_timeout", FATAL_TIMEOUT)));
    querySent = spillsSaved = false;
    nodeDiskUsageCached = false;

    StringBuffer pluginsDir;
    globals->getProp("@pluginsPath", pluginsDir);
    if (pluginsDir.length())
        addPathSepChar(pluginsDir);
    Owned<IConstWUPluginIterator> pluginIter = &workunit->getPlugins();
    ForEach(*pluginIter)
    {
        IConstWUPlugin &plugin = pluginIter->query();
        SCMStringBuffer name;
        plugin.getPluginName(name);
        loadPlugin(pluginMap, pluginsDir.str(), name.str());
    }
    querySo.setown(createDllEntry(_querySo, false, NULL));
    codeCtx = new CThorCodeContextMaster(*this, *workunit, *querySo, *userDesc); 
    mpJobTag = allocateMPTag();
    slavemptag = allocateMPTag();
    slaveMsgHandler = new CSlaveMessageHandler(*this, slavemptag);
    tmpHandler.setown(createTempHandler(true));
}

CJobMaster::~CJobMaster()
{
    clean();
    if (slaveMsgHandler)
        delete slaveMsgHandler;
    freeMPTag(mpJobTag);
    freeMPTag(slavemptag);
    tmpHandler.clear();
}

static IException *createBCastException(unsigned slave, const char *errorMsg)
{
    // think this should always be fatal, could check link down here, or in general and flag as _shutdown.
    StringBuffer msg("General failure communicating to slave");
    if (slave)
        msg.append("(").append(slave).append(") ");
    else
        msg.append("s ");
    Owned<IThorException> e = MakeThorException(0, "%s", msg.append(" [").append(errorMsg).append("]").str());
    e->setAction(tea_shutdown);
    return e.getClear();
}

void CJobMaster::broadcastToSlaves(CMessageBuffer &msg, mptag_t mptag, unsigned timeout, const char *errorMsg, CReplyCancelHandler *msgHandler, bool sendOnly)
{
    mptag_t replyTag = createReplyTag();
    msg.setReplyTag(replyTag);
    if (globals->getPropBool("@broadcastSendAsync", true)) // only here in case of problems/debugging.
    {
        class CSendAsyncfor : public CAsyncFor
        {
            CJobMaster &job;
            CMessageBuffer &msg;
            mptag_t mptag;
            unsigned timeout;
            StringAttr errorMsg;
        public:
            CSendAsyncfor(CJobMaster &_job, CMessageBuffer &_msg, mptag_t _mptag, unsigned _timeout, const char *_errorMsg)
                : job(_job), msg(_msg), mptag(_mptag), timeout(_timeout), errorMsg(_errorMsg)
            {
            }
            void Do(unsigned i)
            {
                if (!job.queryJobComm().send(msg, i+1, mptag, timeout))
                    throw createBCastException(i+1, errorMsg);
            }
        } afor(*this, msg, mptag, timeout, errorMsg);
        try
        {
            afor.For(querySlaves(), querySlaves());
        }
        catch (IException *e)
        {
            EXCLOG(e, "broadcastSendAsync");
            abort(e);
            throw;
        }
    }
    else if (!queryJobComm().send(msg, RANK_ALL_OTHER, mptag, timeout))
    {
        Owned<IException> e = createBCastException(0, errorMsg);
        EXCLOG(e, NULL);
        abort(e);
        throw e.getClear();
    }
    if (sendOnly) return;
    unsigned respondents = 0;
    Owned<IBitSet> bitSet = createThreadSafeBitSet();
    loop
    {
        rank_t sender;
        CMessageBuffer msg;
        bool r = msgHandler ? msgHandler->recv(queryJobComm(), msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT)
                            : queryJobComm().recv(msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT);
        if (!r)
        {
            StringBuffer tmpStr;
            if (errorMsg)
                tmpStr.append(": ").append(errorMsg).append(" - ");
            tmpStr.append("Timeout receiving from slaves - no reply from: [");
            unsigned s = bitSet->scan(0, false);
            assertex(s<querySlaves()); // must be at least one
            tmpStr.append(s+1);
            loop
            {
                s = bitSet->scan(s+1, false);
                if (s>=querySlaves())
                    break;
                tmpStr.append(",").append(s+1);
            }
            tmpStr.append("]");
            Owned<IException> e = MakeThorFatal(NULL, 0, " %s", tmpStr.str());
            EXCLOG(e, NULL);
            throw e.getClear();
        }
        bool error;
        msg.read(error);
        if (error)
        {
            Owned<IThorException> e = deserializeThorException(msg);
            e->setSlave(sender);
            throw e.getClear();
        }
        ++respondents;
        bitSet->set((unsigned)sender-1);
        if (respondents == querySlaveGroup().ordinality())
            break;
    }
}

CGraphBase *CJobMaster::createGraph()
{
    return new CMasterGraph(*this);
}

void CJobMaster::initNodeDUCache()
{
    if (!nodeDiskUsageCached)
    {
        nodeDiskUsageCached = true;
        Owned<IPropertyTreeIterator> fileIter = &workunit->getFileIterator();
        ForEach (*fileIter)
        {
            Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(fileIter->query().queryProp("@name"), userDesc);
            if (f)
            {
                unsigned n = f->numParts();
                for (unsigned i=0;i<n;i++)
                {
                    Owned<IDistributedFilePart> part = f->getPart(i);
                    offset_t sz = part->getFileSize(false, false);
                    if (i>=nodeDiskUsage.ordinality())
                        nodeDiskUsage.append(sz);
                    else
                    {
                        sz += nodeDiskUsage.item(i);
                        nodeDiskUsage.add(sz, i);
                    }
                }
            }
        }
    }
}

IPropertyTree *CJobMaster::prepareWorkUnitInfo()
{
    Owned<IPropertyTree> workUnitInfo = createPTree("workUnitInfo");
    workUnitInfo->setProp("wuid", wuid);
    workUnitInfo->setProp("user", user);
    workUnitInfo->setProp("token", token);
    workUnitInfo->setProp("scope", scope);

    Owned<IConstWUPluginIterator> pluginIter = &queryWorkUnit().getPlugins();
    IPropertyTree *plugins = NULL;
    ForEach(*pluginIter)
    {
        IConstWUPlugin &thisplugin = pluginIter->query();
        if (!plugins)
            plugins = workUnitInfo->addPropTree("plugins", createPTree());
        SCMStringBuffer name;
        thisplugin.getPluginName(name);
        IPropertyTree *plugin = plugins->addPropTree("plugin", createPTree());
        plugin->setProp("@name", name.str());
    }
    IPropertyTree *debug = workUnitInfo->addPropTree("Debug", createPTree(ipt_caseInsensitive));
    SCMStringBuffer debugStr, valueStr;
    Owned<IStringIterator> debugIter = &queryWorkUnit().getDebugValues();
    ForEach (*debugIter)
    {
        debugIter->str(debugStr);
        queryWorkUnit().getDebugValue(debugStr.str(), valueStr);
        debug->setProp(debugStr.str(), valueStr.str());
    }
    return workUnitInfo.getClear();
}

void CJobMaster::sendQuery()
{
    CriticalBlock b(sendQueryCrit);
    if (querySent) return;
    CMessageBuffer tmp;
    tmp.append(mpJobTag);
    tmp.append(slavemptag);
    tmp.append(queryWuid());
    tmp.append(graphName);
    const char *soName = queryDllEntry().queryName();
    PROGLOG("Query dll: %s", soName);
    tmp.append(soName);
    tmp.append(sendSo);
    if (sendSo)
    {
        CTimeMon atimer;
        OwnedIFile iFile = createIFile(soName);
        OwnedIFileIO iFileIO = iFile->open(IFOread);
        size32_t sz = (size32_t)iFileIO->size();
        tmp.append(sz);
        read(iFileIO, 0, sz, tmp);
        PROGLOG("Loading query for serialization to slaves took %d ms", atimer.elapsed());
    }
    Owned<IPropertyTree> deps = createPTree(queryXGMML()->queryName());
    Owned<IPropertyTreeIterator> edgeIter = queryXGMML()->getElements("edge"); // JCSMORE trim to those actually needed
    ForEach (*edgeIter)
    {
        IPropertyTree &edge = edgeIter->query();
        deps->addPropTree("edge", LINK(&edge));
    }
    Owned<IPropertyTree> workUnitInfo = prepareWorkUnitInfo();
    workUnitInfo->serialize(tmp);
    deps->serialize(tmp);

    CMessageBuffer msg;
    msg.append(QueryInit);
    compressToBuffer(msg, tmp.length(), tmp.toByteArray());

    CTimeMon queryToSlavesTimer;
    broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "sendQuery");
    PROGLOG("Serialization of query init info (%d bytes) to slaves took %d ms", msg.length(), queryToSlavesTimer.elapsed());
    queryJobManager().addCachedSo(soName);
    querySent = true;
}

void CJobMaster::jobDone()
{
    if (!querySent) return;
    CMessageBuffer msg;
    msg.append(QueryDone);
    msg.append(queryKey());
    broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "jobDone");
}

void CJobMaster::saveSpills()
{
    CriticalBlock b(spillCrit);
    if (spillsSaved)
        return;
    spillsSaved = true;

    PROGLOG("Paused, saving spills..");
    assertex(!queryUseCheckpoints()); // JCSMORE - checkpoints probably need revisiting

    unsigned numSavedSpills = 0;
    // stash away spills ready for resume, make them owned by workunit in event of abort/delete
    Owned<IFileUsageIterator> iter = queryTempHandler()->getIterator();
    ForEach(*iter)
    {
        CFileUsageEntry &entry = iter->query();
        StringAttr tmpName = entry.queryName();
        if (WUGraphComplete == workunit->queryNodeState(queryGraphName(), entry.queryGraphId()))
        {
            IArrayOf<IGroup> groups;
            StringArray clusters;
            fillClusterArray(*this, tmpName, clusters, groups);
            Owned<IFileDescriptor> fileDesc = queryThorFileManager().create(*this, tmpName, clusters, groups, true, TDXtemporary|TDWnoreplicate);
            fileDesc->queryProperties().setPropBool("@pausefile", true); // JCSMORE - mark to keep, may be able to distinguish via other means
            fileDesc->queryProperties().setProp("@kind", "flat");

            IPropertyTree &props = fileDesc->queryProperties();
            props.setPropBool("@owned", true);
            bool blockCompressed=true; // JCSMORE, should come from helper really
            if (blockCompressed)
                props.setPropBool("@blockCompressed", true);

            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fileDesc);

            // NB: This is renaming/moving from temp path

            StringBuffer newName;
            queryThorFileManager().addScope(*this, tmpName, newName, true, true);
            verifyex(file->renamePhysicalPartFiles(newName.str(), NULL, NULL, queryBaseDirectory(grp_unknown)));

            file->attach(newName,userDesc);

            Owned<IWorkUnit> wu = &queryWorkUnit().lock();
            wu->addFile(newName, &clusters, entry.queryUsage(), entry.queryKind(), queryGraphName());
            ++numSavedSpills;
        }
    }
    PROGLOG("Paused, %d spill(s) saved.", numSavedSpills);
}

bool CJobMaster::go()
{
    class CWorkunitAbortHandler : public CInterface, implements IThreaded
    {
        CJobMaster &job;
        IConstWorkUnit &wu;
        CThreaded threaded;
        bool running;
        Semaphore sem;
    public:
        CWorkunitAbortHandler(CJobMaster &_job, IConstWorkUnit &_wu)
            : job(_job), wu(_wu), threaded("WorkunitAbortHandler")
        {
            running = true;
            wu.subscribe(SubscribeOptionAbort);
            threaded.init(this);
        }
        ~CWorkunitAbortHandler()
        {
            stop();
            threaded.join();
        }
        virtual void main()
        {
            while (running)
            {
                if (sem.wait(5000))
                    break; // signalled aborted
                if (wu.aborting())
                {
                    LOG(MCwarning, thorJob, "ABORT detected from user");
                    Owned <IException> e = MakeThorException(TE_WorkUnitAborting, "User signalled abort");
                    job.fireException(e);
                    break;
                }
            }
        }
        void stop() { running = false; sem.signal(); }
    } wuAbortHandler(*this, *workunit);
    class CWorkunitPauseHandler : public CInterface, implements ISDSSubscription
    {
        CJobMaster &job;
        IConstWorkUnit &wu;
        SubscriptionId subId;
        bool subscribed;
        CriticalSection crit;
    public:
        IMPLEMENT_IINTERFACE;

        CWorkunitPauseHandler(CJobMaster &_job, IConstWorkUnit &_wu) : job(_job), wu(_wu)
        {
            StringBuffer xpath("/WorkUnits/");
            xpath.append(wu.queryWuid()).append("/Action");  // MORE - this should not be done here!
            subId = querySDS().subscribe(xpath.str(), *this, false, true);
            subscribed = true;
        }
        ~CWorkunitPauseHandler() { stop(); }
        void stop()
        {
            CriticalBlock b(crit);
            if (subscribed)
            {
                subscribed = false;
                querySDS().unsubscribe(subId);
            }
        }
        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            CriticalBlock b(crit);
            if (!subscribed) return;
            job.markWuDirty();
            bool abort = false;
            bool pause = false;
            if (valueLen && valueLen==strlen("pause") && (0 == strncmp("pause", (const char *)valueData, valueLen)))
            {
                // pause after current subgraph
                pause = true;
            }
            else if (valueLen && valueLen==strlen("pausenow") && (0 == strncmp("pausenow", (const char *)valueData, valueLen)))
            {
                // abort current subgraph
                abort = true;
                pause = true;
            }
            else
            {
                abort = pause = false;
            }
            if (pause)
            {
                PROGLOG("Pausing job%s", abort?" [now]":"");
                job.pause(abort);
            }
        }
    } workunitPauseHandler(*this, *workunit);
    class CQueryTimeoutHandler : public CTimeoutTrigger
    {
        CJobMaster &job;
    public:
        CQueryTimeoutHandler(CJobMaster &_job, unsigned timeout) : CTimeoutTrigger(timeout, "QUERY"), job(_job)
        {
            inform(MakeThorException(TE_QueryTimeoutError, "Query took greater than %d seconds", timeout));
        }
        virtual bool action()
        {
            job.fireException(exception);
            return true;
        }
    private:
        graph_id graphId;
    };
    Owned<CTimeoutTrigger> qtHandler;
    int guillotineTimeout = workunit->getDebugValueInt("maxRunTime", 0);
    if (guillotineTimeout > 0)
        qtHandler.setown(new CQueryTimeoutHandler(*this, guillotineTimeout));
    else if (guillotineTimeout < 0)
    {
        Owned<IException> e = MakeStringException(0, "Ignoring negative maxRunTime: %d", guillotineTimeout);
        reportExceptionToWorkunit(*workunit, e);
    }
    if (WUActionPause == workunit->getAction() || WUActionPauseNow == workunit->getAction())
        throw MakeStringException(0, "Job paused at start, exiting");

    bool allDone = true;
    unsigned concurrentSubGraphs = (unsigned)getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
    try
    {
        startJob();
        workunit->setGraphState(queryGraphName(), WUGraphRunning);
        Owned<IThorGraphIterator> iter = getSubGraphs();
        CICopyArrayOf<CMasterGraph> toRun;
        ForEach(*iter)
        {
            CMasterGraph &graph = (CMasterGraph &)iter->query();
            if ((queryResumed() || queryUseCheckpoints()) && WUGraphComplete == workunit->queryNodeState(queryGraphName(), graph.queryGraphId()))
                graph.setCompleteEx();
            else
                toRun.append(graph);
        }
        ForEachItemInRev(g, toRun)
        {
            if (aborted) break;
            CMasterGraph &graph = toRun.item(g);
            if (graph.isSink())
                graph.execute(0, NULL, true, concurrentSubGraphs>1);
            if (queryPausing()) break;
        }
        graphExecutor->wait();
        workunitPauseHandler.stop();
        ForEachItemIn(tr, toRun)
        {
            CMasterGraph &graph = toRun.item(tr);
            if (!graph.isComplete())
            {
                allDone = false;
                break;
            }
        }
    }
    catch (IException *e) { fireException(e); e->Release(); }
    catch (CATCHALL) { Owned<IException> e = MakeThorException(0, "Unknown exception running sub graphs"); fireException(e); }
    workunit->setGraphState(queryGraphName(), aborted?WUGraphFailed:(allDone?WUGraphComplete:(pausing?WUGraphPaused:WUGraphComplete)));

    if (queryPausing())
        saveSpills();

    Owned<IException> jobDoneException;
    try { jobDone(); }
    catch (IException *e)
    {
        EXCLOG(e, NULL); 
        jobDoneException.setown(e);
    }
    fatalHandler->clear();
    queryTempHandler()->clearTemps();
    slaveMsgHandler->stop();
    if (jobDoneException.get())
        throw LINK(jobDoneException);
    return allDone;
}

void CJobMaster::pause(bool doAbort)
{
    pausing = true;
    if (doAbort)
    {
        // reply will trigger DAMP_THOR_REPLY_PAUSED to agent, unless all graphs are already complete.
        queryJobManager().replyException(*this, NULL);

        // abort current graph asynchronously.
        // After spill files have been saved, trigger timeout handler in case abort doesn't succeed.
        Owned<IException> e = MakeThorException(0, "Unable to recover from pausenow");
        class CAbortThread : implements IThreaded
        {
            CJobMaster &owner;
            CThreaded threaded;
            Linked<IException> exception;
        public:
            CAbortThread(CJobMaster &_owner, IException *_exception) : owner(_owner), exception(_exception), threaded("SaveSpillThread", this)
            {
                threaded.start();
            }
            ~CAbortThread()
            {
                threaded.join();
            }
        // IThreaded
            virtual void main()
            {
                owner.abort(exception);
            }
        } abortThread(*this, e);
        saveSpills();
        fatalHandler->inform(e.getClear());
    }
}

__int64 CJobMaster::queryNodeDiskUsage(unsigned node)
{
    initNodeDUCache();
    if (!nodeDiskUsage.isItem(node)) return 0;
    return nodeDiskUsage.item(node);
}

void CJobMaster::setNodeDiskUsage(unsigned node, __int64 sz)
{
    initNodeDUCache();
    while (nodeDiskUsage.ordinality() <= node)
        nodeDiskUsage.append(0);
    nodeDiskUsage.replace(sz, node);
}

__int64 CJobMaster::addNodeDiskUsage(unsigned node, __int64 sz)
{
    sz += queryNodeDiskUsage(node);
    setNodeDiskUsage(node, sz);
    return sz;
}

bool CJobMaster::queryCreatedFile(const char *file)
{
    StringBuffer scopedName;
    queryThorFileManager().addScope(*this, file, scopedName, false);
    return (NotFound != createdFiles.find(scopedName.str()));
}

void CJobMaster::addCreatedFile(const char *file)
{
    StringBuffer scopedName;
    queryThorFileManager().addScope(*this, file, scopedName, false);
    createdFiles.append(scopedName.str());
}

__int64 CJobMaster::getWorkUnitValueInt(const char *prop, __int64 defVal) const
{
    return queryWorkUnit().getDebugValueInt64(prop, defVal);
}

bool CJobMaster::getWorkUnitValueBool(const char *prop, bool defVal) const
{
    return queryWorkUnit().getDebugValueBool(prop, defVal);
}

StringBuffer &CJobMaster::getWorkUnitValue(const char *prop, StringBuffer &str) const
{
    SCMStringBuffer scmStr;
    queryWorkUnit().getDebugValue(prop, scmStr);
    return str.append(scmStr.str());
}

IBarrier *CJobMaster::createBarrier(mptag_t tag)
{
    return new CBarrierMaster(*jobComm, tag);
}

IGraphTempHandler *CJobMaster::createTempHandler(bool errorOnMissing)
{
    return new CMasterGraphTempHandler(*this, errorOnMissing);
}

bool CJobMaster::fireException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    ThorExceptionAction action;
    if (!te) action = tea_null;
    else action = te->queryAction();
    if (QUERYINTERFACE(e, IMP_Exception) && MPERR_link_closed==e->errorCode())
        action = tea_shutdown;
    else if (QUERYINTERFACE(e, ISEH_Exception))
        action = tea_shutdown;
    CriticalBlock b(exceptCrit);
    switch (action)
    {
        case tea_warning:
            LOG(MCwarning, thorJob, e);
            reportExceptionToWorkunit(*workunit, e);
            break;
        default:
        {
            LOG(MCerror, thorJob, e);
            queryJobManager().replyException(*this, e); 
            fatalHandler->inform(LINK(e));
            try { abort(e); }
            catch (IException *e)
            {
                Owned<IThorException> te = ThorWrapException(e, "Error aborting job, will cause thor restart");
                e->Release();
                reportExceptionToWorkunit(*workunit, te);
                action = tea_shutdown;
            }
            if (tea_shutdown == action)
                queryJobManager().stop();
        }
    }
    return true;
}

///////////////////

class CCollatedResult : public CSimpleInterface, implements IThorResult
{
    CMasterGraph &graph;
    CActivityBase &activity;
    IRowInterfaces *rowIf;
    unsigned id;
    CriticalSection crit;
    PointerArrayOf<CThorExpandingRowArray> results;
    Owned<IThorResult> result;
    unsigned spillPriority;
    activity_id ownerId;

    void ensure()
    {
        CriticalBlock b(crit);
        if (result)
            return;
        mptag_t replyTag = createReplyTag();
        CMessageBuffer msg;
        msg.append(GraphGetResult);
        msg.append(activity.queryJob().queryKey());
        msg.append(graph.queryGraphId());
        msg.append(ownerId);
        msg.append(id);
        msg.append(replyTag);
        ((CJobMaster &)graph.queryJob()).broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "CCollectResult", NULL, true);

        unsigned numSlaves = graph.queryJob().querySlaves();
        for (unsigned n=0; n<numSlaves; n++)
            results.item(n)->kill();
        rank_t sender;
        MemoryBuffer mb;
        Owned<ISerialStream> stream = createMemoryBufferSerialStream(mb);
        CThorStreamDeserializerSource rowSource(stream);
        unsigned todo = numSlaves;

        loop
        {
            loop
            {
                if (activity.queryAbortSoon())
                    return;
                msg.clear();
                if (activity.receiveMsg(msg, RANK_ALL, replyTag, &sender, 60*1000))
                    break;
                ActPrintLog(&activity, "WARNING: tag %d timedout, retrying", (unsigned)replyTag);
            }
            sender = sender - 1; // 0 = master
            if (!msg.length())
            {
                --todo;
                if (0 == todo)
                    break; // done
            }
            else
            {
                bool error;
                msg.read(error);
                if (error)
                {
                    Owned<IThorException> e = deserializeThorException(msg);
                    e->setSlave(sender);
                    throw e.getClear();
                }
                ThorExpand(msg, mb.clear());

                CThorExpandingRowArray *slaveResults = results.item(sender);
                while (!rowSource.eos())
                {
                    RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
                    size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, rowSource);
                    slaveResults->append(rowBuilder.finalizeRowClear(sz));
                }
            }
        }
        Owned<IThorResult> _result = ::createResult(activity, rowIf, false, spillPriority);
        Owned<IRowWriter> resultWriter = _result->getWriter();
        for (unsigned s=0; s<numSlaves; s++)
        {
            CThorExpandingRowArray *slaveResult = results.item(s);
            ForEachItemIn(r, *slaveResult)
            {
                const void *row = slaveResult->query(r);
                LinkThorRow(row);
                resultWriter->putRow(row);
            }
        }
        result.setown(_result.getClear());
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCollatedResult(CMasterGraph &_graph, CActivityBase &_activity, IRowInterfaces *_rowIf, unsigned _id, activity_id _ownerId, unsigned _spillPriority)
        : graph(_graph), activity(_activity), rowIf(_rowIf), id(_id), ownerId(_ownerId), spillPriority(_spillPriority)
    {
        for (unsigned n=0; n<graph.queryJob().querySlaves(); n++)
            results.append(new CThorExpandingRowArray(activity, rowIf));
    }
    ~CCollatedResult()
    {
        ForEachItemIn(l, results)
        {
            CThorExpandingRowArray *result = results.item(l);
            delete result;
        }
    }
    void setId(unsigned _id)
    {
        id = _id;
    }

// IThorResult
    virtual IRowWriter *getWriter() { throwUnexpected(); }
    virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count)
    {
        throwUnexpected();
    }
    virtual IRowStream *getRowStream()
    {
        ensure();
        return result->getRowStream();
    }
    virtual IRowInterfaces *queryRowInterfaces()
    {
        return rowIf;
    }
    virtual CActivityBase *queryActivity()
    {
        return &activity;
    }
    virtual bool isDistributed() const { return false; }
    virtual void serialize(MemoryBuffer &mb)
    {
        ensure();
        result->serialize(mb);
    }
    virtual void getLinkedResult(unsigned & count, byte * * & ret)
    {
        ensure();
        result->getLinkedResult(count, ret);
    }
    virtual const void * getLinkedRowResult()
    {
        ensure();
        return result->getLinkedRowResult();
    }
};

///////////////////

//
// CMasterGraph impl.
//

CMasterGraph::CMasterGraph(CJobMaster &_job) : CGraphBase(_job), jobM(_job)
{
    mpTag = job.allocateMPTag();
    startBarrierTag = job.allocateMPTag();
    waitBarrierTag = job.allocateMPTag();
    startBarrier = job.createBarrier(startBarrierTag);
    waitBarrier = job.createBarrier(waitBarrierTag);
}


CMasterGraph::~CMasterGraph()
{
    job.freeMPTag(mpTag);
    job.freeMPTag(startBarrierTag);
    job.freeMPTag(waitBarrierTag);
    if (TAG_NULL != doneBarrierTag)
        job.freeMPTag(doneBarrierTag);
    if (TAG_NULL != executeReplyTag)
        job.freeMPTag(executeReplyTag);
}

void CMasterGraph::init()
{
    CGraphBase::init();
    if (queryOwner() && isGlobal())
    {
        doneBarrierTag = job.allocateMPTag();
        doneBarrier = job.createBarrier(doneBarrierTag);
    }
}

bool CMasterGraph::fireException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    ThorExceptionAction action;
    if (!te) action = tea_null;
    else action = te->queryAction();
    if (QUERYINTERFACE(e, IMP_Exception) && MPERR_link_closed==e->errorCode())
        action = tea_shutdown;
    else if (QUERYINTERFACE(e, ISEH_Exception))
        action = tea_shutdown;
    CriticalBlock b(exceptCrit);
    switch (action)
    {
        case tea_warning:
            LOG(MCwarning, thorJob, e);
            reportExceptionToWorkunit(job.queryWorkUnit(), e);
            break;
        case tea_abort:
        {
            EXCLOG(e, NULL);
            abort(e);
            // fall through
        }
        default:
        {
            LOG(MCerror, thorJob, e);
            if (NULL != fatalHandler)
                fatalHandler->inform(LINK(e));
            if (owner)
                owner->fireException(e);
            else
                job.fireException(e);
        }
    }
    return true;
}

void CMasterGraph::reset()
{
    CGraphBase::reset();
    bcastMsgHandler.reset();
    activityInitMsgHandler.reset();
    executeReplyMsgHandler.reset();
}

void CMasterGraph::abort(IException *e)
{
    if (aborted) return;
    try { CGraphBase::abort(e); }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting master graph");
        e->Release();
    }
    bcastMsgHandler.cancel(0);
    activityInitMsgHandler.cancel(RANK_ALL);
    executeReplyMsgHandler.cancel(RANK_ALL);
    if (started && !graphDone)
    {
        try
        {
            CMessageBuffer msg;
            msg.append(GraphAbort);
            msg.append(job.queryKey());
            msg.append(queryGraphId());
            jobM.broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "abort");
        }
        catch (IException *e)
        {
            GraphPrintLog(e, "Aborting slave graph");
            if (abortException)
            {
                e->Release();
                throw LINK(abortException);
            }
            throw;
        }
    }
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->endGraph(this, !queryAborted());
    }
}

void CMasterGraph::serializeCreateContexts(MemoryBuffer &mb)
{
    CGraphBase::serializeCreateContexts(mb);
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach (*iter)
    {
        CMasterGraphElement &element = (CMasterGraphElement &)iter->query();
        if (reinit || !element.sentCreateCtx)
            element.sentCreateCtx = true;
    }
}

bool CMasterGraph::serializeActivityInitData(unsigned slave, MemoryBuffer &mb, IThorActivityIterator &iter)
{
    CriticalBlock b(createdCrit);
    DelayedSizeMarker sizeMark1(mb);
    ForEach (iter)
    {
        CMasterGraphElement &element = (CMasterGraphElement &)iter.query();
        if (!element.sentActInitData->testSet(slave))
        {
            CMasterActivity *activity = (CMasterActivity *)element.queryActivity();
            if (activity)
            {
                mb.append(element.queryId());
                DelayedSizeMarker sizeMark2(mb);
                activity->serializeSlaveData(mb, slave);
                sizeMark2.write();
            }
        }
    }
    if (0 == sizeMark1.size())
        return false;
    mb.append((activity_id)0); // terminator
    sizeMark1.write();
    return true;
}

void CMasterGraph::readActivityInitData(MemoryBuffer &mb, unsigned slave)
{
    loop
    {
        activity_id id;
        mb.read(id);
        if (0 == id)
            break;
        size32_t dataLen;
        mb.read(dataLen);

        CGraphElementBase *element = queryElement(id);
        MemoryBuffer &mbDst = element->queryActivity()->queryInitializationData(slave);
        mbDst.append(dataLen, mb.readDirect(dataLen));
    }
}

bool CMasterGraph::prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async)
{
    if (!CGraphBase::prepare(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async)) return false;
    if (aborted) return false;
    return true;
}

void CMasterGraph::create(size32_t parentExtractSz, const byte *parentExtract)
{
    {
        CriticalBlock b(createdCrit);
        CGraphBase::create(parentExtractSz, parentExtract);
    }
    if (!aborted)
    {
        if (!queryOwner()) // owning graph sends query+child graphs
        {
            jobM.sendQuery(); // if not previously sent
            if (globals->getPropBool("@watchdogProgressEnabled"))
                queryJobManager().queryDeMonServer()->startGraph(this);
            sendGraph(); // sends child graphs at same time
        }
        else
        {
            if (isGlobal())
            {
                ForEachItemIn(i, ifs)
                {
                    CGraphElementBase &ifElem = ifs.item(i);
                    if (ifElem.newWhichBranch)
                    {
                        ifElem.newWhichBranch = false;
                        sentInitData = false; // force re-request of create data.
                        break;
                    }
                }
                CMessageBuffer msg;
                if (reinit || !sentInitData)
                {
                    sentInitData = true;
                    serializeCreateContexts(msg);
                }
                else
                    msg.append((unsigned)0);
                try
                {
                    jobM.broadcastToSlaves(msg, mpTag, LONGTIMEOUT, "serializeCreateContexts", &bcastMsgHandler);
                }
                catch (IException *e)
                {
                    GraphPrintLog(e, "Aborting graph create(2)");
                    if (abortException)
                    {
                        e->Release();
                        throw LINK(abortException);
                    }
                    throw;
                }
            }
        }
    }
}

void CMasterGraph::start()
{
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach (*iter)
        iter->query().queryActivity()->startProcess();
}

void CMasterGraph::sendActivityInitData()
{
    CMessageBuffer msg;
    mptag_t replyTag = createReplyTag();
    msg.setReplyTag(replyTag);
    unsigned pos = msg.length();
    unsigned w=0;
    unsigned sentTo = 0;
    for (; w<queryJob().querySlaves(); w++)
    {
        unsigned needActInit = 0;
        Owned<IThorActivityIterator> iter = getConnectedIterator();
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            CActivityBase *activity = element.queryActivity();
            if (activity && activity->needReInit())
                element.sentActInitData->set(w, false); // force act init to be resent
            if (!element.sentActInitData->test(w)) // has it been sent
                ++needActInit;
        }
        if (needActInit)
        {
            try
            {
                msg.rewrite(pos);
                Owned<IThorActivityIterator> iter = getConnectedIterator();
                serializeActivityInitData(w, msg, *iter);
            }
            catch (IException *e)
            {
                GraphPrintLog(e);
                throw;
            }
            if (!job.queryJobComm().send(msg, w+1, mpTag, LONGTIMEOUT))
            {
                StringBuffer epStr;
                throw MakeStringException(0, "Timeout sending to slave %s", job.querySlaveGroup().queryNode(w).endpoint().getUrlStr(epStr).str());
            }
            ++sentTo;
        }
    }
    if (sentTo)
    {
        assertex(sentTo == queryJob().querySlaves());
        w=0;
        Owned<IException> e;
        // now get back initialization data from graph tag
        for (; w<queryJob().querySlaves(); w++)
        {
            rank_t sender;
            msg.clear();
            if (!activityInitMsgHandler.recv(queryJob().queryJobComm(), msg, w+1, replyTag, &sender, LONGTIMEOUT))
                throw MakeGraphException(this, 0, "Timeout receiving from slaves after graph sent");

            bool error;
            msg.read(error);
            if (error)
            {
                Owned<IThorException> se = deserializeThorException(msg);
                se->setSlave(sender);
                if (!e.get())
                {
                    StringBuffer tmpStr("Slave ");
                    queryJob().queryJobGroup().queryNode(sender).endpoint().getUrlStr(tmpStr);
                    GraphPrintLog(se, "%s", tmpStr.append(": slave initialization error").str());
                    e.setown(se.getClear());
                }
                continue; // to read other slave responses.
            }
            readActivityInitData(msg, w);
        }
        if (e.get())
            throw LINK(e);
    }
}

void CMasterGraph::serializeGraphInit(MemoryBuffer &mb)
{
    mb.append(graphId);
    mb.append(reinit);
    serializeMPtag(mb, mpTag);
    mb.append((int)startBarrierTag);
    mb.append((int)waitBarrierTag);
    mb.append((int)doneBarrierTag);
    mb.append(queryChildGraphCount());
    Owned<IThorGraphIterator> childIter = getChildGraphs();
    ForEach (*childIter)
    {
        CMasterGraph &childGraph = (CMasterGraph &)childIter->query();
        childGraph.serializeGraphInit(mb);
    }
}

// IThorChildGraph impl.
IEclGraphResults *CMasterGraph::evaluate(unsigned _parentExtractSz, const byte *parentExtract)
{
    throw MakeGraphException(this, 0, "Thor master does not support the execution of child queries");
}

void CMasterGraph::executeSubGraph(size32_t parentExtractSz, const byte *parentExtract)
{
    if (job.queryResumed()) // skip complete subgraph if resuming. NB: temp spill have been tucked away for this purpose when paused.
    {
        if (!queryOwner())
        {
            if (WUGraphComplete == job.queryWorkUnit().queryNodeState(job.queryGraphName(), graphId))
                setCompleteEx();
        }
    }
    if (isComplete())
        return;
    fatalHandler.clear();
    fatalHandler.setown(new CFatalHandler(globals->getPropInt("@fatal_timeout", FATAL_TIMEOUT)));
    CGraphBase::executeSubGraph(parentExtractSz, parentExtract);
    if (TAG_NULL != executeReplyTag)
    {
        rank_t sender;
        unsigned s=0;
        for (; s<queryJob().querySlaves(); s++)
        {
            CMessageBuffer msg;
            if (!executeReplyMsgHandler.recv(queryJob().queryJobComm(), msg, RANK_ALL, executeReplyTag, &sender))
                break;
            bool error;
            msg.read(error);
            if (error)
            {
                Owned<IThorException> exception = deserializeThorException(msg);
                exception->setSlave(sender);
                GraphPrintLog(exception, "slave execute reply exception");
                throw exception.getClear();
            }
        }
        if (fatalHandler)
            fatalHandler->clear();
    }
    fatalHandler.clear();
    Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
    queryJobManager().updateWorkUnitLog(*wu);
}

void CMasterGraph::sendGraph()
{
    CTimeMon atimer;
    CMessageBuffer msg;
    msg.append(GraphInit);
    msg.append(job.queryKey());
    node->serialize(msg); // everything
    if (TAG_NULL == executeReplyTag)
        executeReplyTag = queryJob().allocateMPTag();
    serializeMPtag(msg, executeReplyTag);
    serializeCreateContexts(msg);
    serializeGraphInit(msg);
    // slave graph data
    try
    {
        jobM.broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "sendGraph", &bcastMsgHandler);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting sendGraph");
        if (abortException)
        {
            e->Release();
            throw LINK(abortException);
        }
        throw;
    }
    GraphPrintLog("sendGraph took %d ms", atimer.elapsed());
}

bool CMasterGraph::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    started = true;
    GraphPrintLog("Processing graph");
    if (!sentStartCtx || reinit)
    {
        sentStartCtx = true;
        CMessageBuffer msg;
        serializeStartContexts(msg);
        try
        {
            jobM.broadcastToSlaves(msg, mpTag, LONGTIMEOUT, "startCtx", NULL, true);
        }
        catch (IException *e)
        {
            GraphPrintLog(e, "Aborting preStart");
            if (abortException)
            {
                e->Release();
                throw LINK(abortException);
            }
            throw;
        }
    }

    if (syncInitData())
        sendActivityInitData(); // has to be done at least once
    CGraphBase::preStart(parentExtractSz, parentExtract);
    if (isGlobal())
    {
        if (!startBarrier->wait(true)) // ensure all graphs are at start point at same time, as some may request data from each other.
            return false;
    }
    if (!queryOwner())
        job.queryWorkUnit().setNodeState(job.queryGraphName(), graphId, WUGraphRunning);
    return true;
}

void CMasterGraph::handleSlaveDone(unsigned node, MemoryBuffer &mb)
{
    graph_id gid;
    mb.read(gid);
    assertex(gid == queryGraphId());
    unsigned count;
    mb.read(count);
    while (count--)
    {
        activity_id activityId;
        mb.read(activityId);
        CMasterGraphElement *act = (CMasterGraphElement *)queryElement(activityId);
        unsigned len;
        mb.read(len);
        const void *d = mb.readDirect(len);
        MemoryBuffer sdMb;
        sdMb.setBuffer(len, (void *)d);
        act->slaveDone(node, sdMb);
    }
}

void CMasterGraph::getFinalProgress()
{
    offset_t totalDiskUsage = 0;
    offset_t minNodeDiskUsage = 0, maxNodeDiskUsage = 0;
    unsigned maxNode = (unsigned)-1, minNode = (unsigned)-1;

    CMessageBuffer msg;
    mptag_t replyTag = createReplyTag();
    msg.setReplyTag(replyTag);
    msg.append((unsigned)GraphEnd);
    msg.append(job.queryKey());
    msg.append(queryGraphId());
    if (!job.queryJobComm().send(msg, RANK_ALL_OTHER, masterSlaveMpTag, LONGTIMEOUT))
        throw MakeGraphException(this, 0, "Timeout sending to slaves");

    unsigned n=queryJob().querySlaves();
    while (n--)
    {
        rank_t sender;
        if (!job.queryJobComm().recv(msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT))
        {
            GraphPrintLog("Timeout receiving final progress from slaves, %d slaves did not respond", n+1);
            return;
        }
        bool error;
        msg.read(error);
        if (error)
        {
            Owned<IThorException> e = deserializeThorException(msg);
            e->setSlave(sender);
            throw e.getClear();
        }
        if (0 == msg.remaining())
            continue;

        handleSlaveDone(sender-1, msg);

        if (!queryOwner())
        {
            if (globals->getPropBool("@watchdogProgressEnabled"))
            {
                try
                {
                    size32_t progressLen;
                    msg.read(progressLen);
                    MemoryBuffer progressData;
                    progressData.setBuffer(progressLen, (void *)msg.readDirect(progressLen));
                    const SocketEndpoint &ep = queryClusterGroup().queryNode(sender).endpoint();
                    queryJobManager().queryDeMonServer()->takeHeartBeat(ep, progressData);
                }
                catch (IException *e)
                {
                    GraphPrintLog(e, "Failure whilst deserializing stats/progress");
                    e->Release();
                }
            }
        }
        offset_t nodeDiskUsage;
        msg.read(nodeDiskUsage);
        jobM.setNodeDiskUsage(n, nodeDiskUsage);
        if (nodeDiskUsage > maxNodeDiskUsage)
        {
            maxNodeDiskUsage = nodeDiskUsage;
            maxNode = n;
        }
        if ((unsigned)-1 == minNode || nodeDiskUsage < minNodeDiskUsage)
        {
            minNodeDiskUsage = nodeDiskUsage;
            minNode = n;
        }
        totalDiskUsage += nodeDiskUsage;
        Owned<ITimeReporter> slaveReport = createStdTimeReporter(msg);
        queryJob().queryTimeReporter().merge(*slaveReport);
    }
    if (totalDiskUsage)
    {
        Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
        wu->addDiskUsageStats(totalDiskUsage/queryJob().querySlaves(), minNode, minNodeDiskUsage, maxNode, maxNodeDiskUsage, queryGraphId());
    }
}

void CMasterGraph::done()
{
    if (started)
    {
        if (!aborted && (!queryOwner() || isGlobal()))
            getFinalProgress(); // waiting for slave graph to finish and send final progress update + extra act end info.
    }

    CGraphBase::done();
    if (started && !queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->endGraph(this, true);
    }
    if (!queryOwner())
    {
        if (queryJob().queryTimeReporter().numSections())
        {
            if (globals->getPropBool("@reportTimingsToWorkunit", true))
            {
                struct CReport : implements ITimeReportInfo
                {
                    Owned<IWorkUnit> wu;
                    CGraphBase &graph;
                    CReport(CGraphBase &_graph) : graph(_graph)
                    {
                        wu.setown(&graph.queryJob().queryWorkUnit().lock());
                    }
                    virtual void report(const char * timerScope, const char *description, const __int64 totaltime, const __int64 maxtime, const unsigned count)
                    {
                        StringBuffer timerStr(graph.queryJob().queryGraphName());
                        timerStr.append("(").append(graph.queryGraphId()).append("): ");
                        timerStr.append(description);

                        StringBuffer scope;
                        //GH-.JCS is this correct queryGraphId() is a subgraph?
                        formatGraphTimerScope(scope, graph.queryJob().queryGraphName(), graph.queryGraphId(), 0);
                        scope.append(":").append(timerScope);
                        wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTsection, scope, StTimeElapsed, timerStr.str(), totaltime, count, maxtime, StatsMergeReplace);

                    }
                } wureport(*this);
                queryJob().queryTimeReporter().report(wureport);
            }
            else
                queryJob().queryTimeReporter().printTimings();
        }
    }
}

void CMasterGraph::setComplete(bool tf)
{
    CGraphBase::setComplete(tf);
    if (tf && !queryOwner())
    {
        job.queryWorkUnit().setNodeState(job.queryGraphName(), graphId, graphDone?WUGraphComplete:WUGraphFailed);
    }
}

bool CMasterGraph::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CriticalBlock b(createdCrit);
    unsigned count, _count;
    mb.read(count);
    _count = count;
    while (count--)
    {
        activity_id activityId;
        mb.read(activityId);
        CMasterActivity *activity = NULL;
        CMasterGraphElement *element = (CMasterGraphElement *)queryElement(activityId);
        if (element)
        {
            activity = (CMasterActivity *)element->queryActivity();
            if (!activity)
            {
                CGraphBase *parentGraph = element->queryOwner().queryOwner(); // i.e. am I in a childgraph
                if (!parentGraph)
                {
                    GraphPrintLog("Activity id=%" ACTPF "d not created in master and not a child query activity", activityId);
                    return false; // don't know if or how this could happen, but all bets off with packet if did.
                }
                Owned<IException> e;
                try
                {
                    element->onCreate();
                    element->initActivity();
                    activity = (CMasterActivity *)element->queryActivity();
                    created = true; // means some activities created within this graph
                }
                catch (IException *_e)
                {
                    e.setown(_e);
                    GraphPrintLog(_e, "In deserializeStats");
                }
                if (!activity || e.get())
                {
                    GraphPrintLog("Activity id=%" ACTPF "d failed to created child query activity ready for progress", activityId);
                    return false;
                }
            }
            if (activity)
                activity->deserializeStats(node, mb);
        }
        else
        {
            GraphPrintLog("Failed to find activity, during progress deserialization, id=%" ACTPF "d", activityId);
            return false; // don't know if or how this could happen, but all bets off with packet if did.
        }
    }
    unsigned subs, _subs;
    mb.read(subs);
    _subs = subs;
    while (subs--)
    {
        graph_id subId;
        mb.read(subId);
        Owned<CMasterGraph> graph = (CMasterGraph *)job.getGraph(subId);
        if (NULL == graph.get())
            return false;
        if (!graph->deserializeStats(node, mb))
            return false;
    }
    return true;
}

IThorResult *CMasterGraph::createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, id, results->queryOwnerId(), spillPriority);
    results->setResult(id, result);
    return result;
}

IThorResult *CMasterGraph::createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, id, localResults->queryOwnerId(), spillPriority);
    localResults->setResult(id, result);
    return result;
}

IThorResult *CMasterGraph::createGraphLoopResult(CActivityBase &activity, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    Owned<CCollatedResult> result = new CCollatedResult(*this, activity, rowIf, 0, localResults->queryOwnerId(), spillPriority);
    unsigned id = graphLoopResults->addResult(result);
    result->setId(id);
    return result;
}


///////////////////////////////////////////////////

CThorStats::CThorStats(StatisticKind _kind) : kind(_kind)
{
    unsigned c = queryClusterWidth();
    while (c--) counts.append(0);
    reset();
}

void CThorStats::set(unsigned node, unsigned __int64 count)
{
    counts.replace(count, node);
}

void CThorStats::reset()
{
    tot = max = avg = 0;
    min = (unsigned __int64) -1;
    minNode = maxNode = maxSkew = minSkew = maxNode = minNode = 0;
}

void CThorStats::calculateSkew()
{
    if (max)
    {
        unsigned count = counts.ordinality();
        double _avg = (double)tot/count;
        if (_avg)
        {
            //MORE: Range protection on maxSkew?
            maxSkew = (unsigned)(10000.0 * (((double)max-_avg)/_avg));
            minSkew = (unsigned)(10000.0 * ((_avg-(double)min)/_avg));
            avg = (unsigned __int64)_avg;
        }
    }
}

void CThorStats::tallyValue(unsigned __int64 thiscount, unsigned n)
{
    tot += thiscount;
    if (thiscount > max)
    {
        max = thiscount;
        maxNode = n;
    }
    if (thiscount < min)
    {
        min = thiscount;
        minNode = n;
    }
}

void CThorStats::processInfo()
{
    reset();
    ForEachItemIn(n, counts)
    {
        unsigned __int64 thiscount = counts.item(n);
        tallyValue(thiscount, n);
    }
    calculateSkew();
}

void CThorStats::getStats(IStatisticGatherer & stats, bool suppressMinMaxWhenEqual)
{
    processInfo();
    //MORE: For most measures (not time stamps etc.) it would be sensible to output the total here....
    if (!suppressMinMaxWhenEqual || (maxSkew != minSkew))
    {
        stats.addStatistic((StatisticKind)(kind|StMinX), min);
        stats.addStatistic((StatisticKind)(kind|StMaxX), max);
        stats.addStatistic((StatisticKind)(kind|StAvgX), avg);
    }

    if (maxSkew != minSkew)
    {
        stats.addStatistic((StatisticKind)(kind|StSkewMin), -(__int64)minSkew); // Save minimum as a negative value so consistent
        stats.addStatistic((StatisticKind)(kind|StSkewMax), maxSkew);
        stats.addStatistic((StatisticKind)(kind|StNodeMin), minNode);
        stats.addStatistic((StatisticKind)(kind|StNodeMax), maxNode);
    }
}

///////////////////////////////////////////////////

CTimingInfo::CTimingInfo() : CThorStats(StTimeLocalExecute)
{
}

///////////////////////////////////////////////////

ProgressInfo::ProgressInfo() : CThorStats(StNumRowsProcessed)
{
    startcount = stopcount = 0;
}
void ProgressInfo::processInfo() // reimplement as counts have special flags (i.e. stop/start)
{
    reset();
    startcount = stopcount = 0;
    ForEachItemIn(n, counts)
    {
        unsigned __int64 thiscount = counts.item(n);
        if (thiscount & THORDATALINK_STARTED)
            startcount++;
        if (thiscount & THORDATALINK_STOPPED)
            stopcount++;
        thiscount = thiscount & THORDATALINK_COUNT_MASK;
        tallyValue(thiscount, n);
    }
    calculateSkew();
}

void ProgressInfo::getStats(IStatisticGatherer & stats)
{
    CThorStats::getStats(stats, true);
    stats.addStatistic(kind, tot);
    stats.addStatistic(StNumSlaves, counts.ordinality());
    stats.addStatistic(StNumStarted, startcount);
    stats.addStatistic(StNumStopped, stopcount);
}


///////////////////////////////////////////////////

CJobMaster *createThorGraph(const char *graphName, IPropertyTree *xgmml, IConstWorkUnit &workunit, const char *querySo, bool sendSo, const SocketEndpoint &agentEp)
{
    Owned<CJobMaster> masterJob = new CJobMaster(workunit, graphName, querySo, sendSo, agentEp);
    masterJob->setXGMML(xgmml);
    Owned<IPropertyTreeIterator> iter = xgmml->getElements("node");
    ForEach(*iter)
    {
        IPropertyTree &node = iter->query();
        Owned<CGraphBase> subGraph = masterJob->createGraph();
        subGraph->createFromXGMML(&node, NULL, NULL, NULL);
        masterJob->addSubGraph(*LINK(subGraph));
    }
    masterJob->addDependencies(xgmml);
    return LINK(masterJob);
}

static IJobManager *jobManager = NULL;
void setJobManager(IJobManager *_jobManager)
{
    CriticalBlock b(*jobManagerCrit);
    jobManager = _jobManager;
}
IJobManager *getJobManager()
{
    CriticalBlock b(*jobManagerCrit);
    return LINK(jobManager);
}
IJobManager &queryJobManager()
{
    CriticalBlock b(*jobManagerCrit);
    assertex(jobManager);
    return *jobManager;
}

