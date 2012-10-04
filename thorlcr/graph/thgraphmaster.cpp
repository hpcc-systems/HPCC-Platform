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
    notedWarnings = createBitSet();
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
        ActPrintLog(e2, NULL);
        fireException(e2);
    }
    catch (CATCHALL)
    {
        Owned<IException> e = MakeThorFatal(NULL, TE_MasterProcessError, "FATAL: Unknown master process exception kind=%s, id=%"ACTPF"d", activityKindStr(container.getKind()), container.queryId());
        ActPrintLog(e, NULL);
        fireException(e);
    }
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
    timingInfo.set(node, localTimeNs/1000000); // to milliseconds
    rowcount_t count;
    ForEachItemIn(p, progressInfo)
    {
        mb.read(count);
        progressInfo.item(p).set(node, count);
    }
}

void CMasterActivity::getXGMML(IWUGraphProgress *progress, IPropertyTree *node)
{
    timingInfo.getXGMML(node);
}

void CMasterActivity::getXGMML(unsigned idx, IPropertyTree *edge)
{
    CriticalBlock b(progressCrit);
    if (progressInfo.isItem(idx))
        progressInfo.item(idx).getXGMML(edge);
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
    StringAttr filename;
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
        case TAKworkunitwrite:
        case TAKworkunitread:
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
        CTimeMon tm(timeout);
        unsigned s=comm->queryGroup().ordinality()-1;
        bool aborted = false;
        CMessageBuffer msg;
        Owned<IBitSet> raisedSet = createBitSet();
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
            sender = sender - 1; // 0 = master
            if (raisedSet->testSet(sender, true) && !aborted)
                WARNLOG("CBarrierMaster, raise barrier message on tag %d, already received from slave %d", tag, sender);
            if (aborted) break;
        }
        msg.clear();
        msg.append(aborted);
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
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        return true;
    }
    virtual void cancel()
    {
        if (receiving)
            comm->cancel(RANK_ALL, tag);
        CMessageBuffer msg;
        msg.append(true);
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
            StringBuffer scopedName;
            queryThorFileManager().addScope(job, name, scopedName, temp || fileKind==WUFileJobOwned);
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->addFile(scopedName.str(), clusters, usageCount, fileKind, job.queryGraphName());
        }
        else
            CGraphTempHandler::registerFile(name, graphId, usageCount, temp, fileKind, clusters);
    }
    virtual void deregisterFile(const char *name, bool kept) // NB: only called for temp files
    {
        if (kept || job.queryUseCheckpoints())
        {
            StringBuffer scopedName;
            queryThorFileManager().addScope(job, name, scopedName, kept, kept);
            Owned<IWorkUnit> wu = &job.queryWorkUnit().lock();
            wu->releaseFile(scopedName.str());
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

    IWorkUnit *updateWorkUnit() 
    {
        StringAttr wuid;
        workunit->getWuid(StringAttrAdaptor(wuid));
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        return factory->updateWorkUnit(wuid);
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
    virtual void setResultInt(const char *name, unsigned sequence, __int64 result)
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
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 result)
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
            r->getResultString(result);
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
            r->getResultString(result);
            tlen = result.length();
            tgt = (char *)result.s.detach();
        );
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * stepname, unsigned sequence)
    {
        PROTECTED_GETRESULT(stepname, sequence, "String", "string",
            SCMStringBuffer result;
            r->getResultString(result);
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
            r->getResultString(result);
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
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * stepname, unsigned sequence, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer)
    {
        tgt = NULL;
        PROTECTED_GETRESULT(stepname, sequence, "Rowset", "rowset",
            MemoryBuffer datasetBuffer;
            MemoryBuffer2IDataVal result(datasetBuffer);
            Owned<IXmlToRawTransformer> rawXmlTransformer = createXmlRawTransformer(xmlTransformer);
            Owned<ICsvToRawTransformer> rawCsvTransformer = createCsvRawTransformer(csvTransformer);
            r->getResultRaw(result, rawXmlTransformer, rawCsvTransformer);
            rtlDataset2RowsetX(tcount, tgt, _rowAllocator, deserializer, datasetBuffer.length(), datasetBuffer.toByteArray(), isGrouped);
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
    virtual __int64 countDiskFile(const char * name, unsigned recordSize)
    {
        unsigned __int64 size = 0;
        Owned<IDistributedFile> f = queryThorFileManager().lookup(job, name);
        if (f) 
        {
            size = f->getFileSize(true,false);
            if (size % recordSize)
                throw MakeStringException(9001, "File %s has size %"I64F"d which is not a multiple of record size %d", name, size, recordSize);
            return size / recordSize;
        }
        DBGLOG("Error could not resolve file %s", name);
        throw MakeStringException(9003, "Error could not resolve %s", name);
    }
    virtual __int64 countIndex(__int64 activityId, IHThorCountIndexArg & arg) { UNIMPLEMENTED; }
    virtual __int64 countDiskFile(__int64 id, IHThorCountFileArg & arg)
    {
        // would have called the above function in a try block but corrupted registers whenever I tried.

        Owned<IHThorCountFileArg> a = &arg;  // make sure it gets destroyed....
        arg.onCreate(this, NULL, NULL);
        arg.onStart(NULL, NULL);

        const char *name = arg.getFileName();
        Owned<IDistributedFile> f = queryThorFileManager().lookup(job, name, 0 != ((TDXtemporary|TDXjobtemp) & arg.getFlags()));
        if (f) 
        {
            IOutputMetaData * rs = arg.queryRecordSize();
            assertex(rs->isFixedSize());
            unsigned recordSize = rs->getMinRecordSize();
            unsigned __int64 size = f->getFileSize(true,false);
            if (size % recordSize)
            {
                throw MakeStringException(0, "Physical file %s has size %"I64F"d which is not a multiple of record size %d", name, size, recordSize);
            }
            return size / recordSize;
        }
        else if (arg.getFlags() & TDRoptional)
            return 0;
        else
        {
            PrintLog("Error could not resolve file %s", name);
            throw MakeStringException(0, "Error could not resolve %s", name);
        }
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity)
    {
        DBGLOG("%s", text);
        try
        {
            Owned<IWorkUnit> w = updateWorkUnit();
            Owned<IWUException> we = w->createException();
            we->setSeverity((WUExceptionSeverity)severity);
            we->setExceptionMessage(text);
            we->setExceptionSource("user");
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
            addExceptionToWorkunit(w, ExceptionSeverityError, "user", code, text, filename, lineno, column);
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
    virtual unsigned getRecoveringCount() { UNIMPLEMENTED; }        // don't know how to implement here!
    virtual unsigned getNodes() { return job.queryJobGroup().ordinality()-1; }
    virtual unsigned getNodeNum() { throw MakeThorException(0, "Unsupported. getNodeNum() called in master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) { assertex(false); return NULL; }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)
    {
        unsigned checkSum = 0;
        Owned<IDistributedFile> iDfsFile = queryThorFileManager().lookup(job, name, false, true);
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
        SCMStringBuffer out;
        workunit->getJobName(out);
        return out.s.detach();
    }
    virtual char *getClusterName()
    {
        SCMStringBuffer out;
        workunit->getClusterName(out);
        return out.s.detach();
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
        Owned<IConstWorkUnit> externalWU = factory->openWorkUnit(wuid, false);
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

void loadPlugin(SafePluginMap *pluginMap, const char *_path, const char *name, const char *version)
{
    StringBuffer path(_path);
    path.append(name);

    OwnedIFile iFile = createIFile(path.str());
    if (!iFile->exists())
        throw MakeThorException(0, "Plugin %s not found at %s", name, path.str());

    pluginMap->addPlugin(path.str(), name); // throws if unavailable/fails to load
    Owned<ILoadedDllEntry> so = pluginMap->getPluginDll(name, version, true);
    if (NULL == so.get()) // JCSMORE - could perhaps do with a more direct way of asking.
        throw MakeThorException(0, "Incompatible plugin (%s). Version %s unavailable", name, version);
}

CJobMaster::CJobMaster(IConstWorkUnit &_workunit, const char *graphName, const char *_querySo, bool _sendSo, const SocketEndpoint &_agentEp)
    : CJobBase(graphName), workunit(&_workunit), sendSo(_sendSo), agentEp(_agentEp)
{
    SCMStringBuffer _token, _wuid, _user, _scope;
    workunit->getWuid(_wuid);
    workunit->getUser(_user);
    workunit->getScope(_scope);
    workunit->getSecurityToken(_token);
    wuid.append(_wuid.str());
    user.append(_user.str());
    token.append(_token.str());
    scope.append(_scope.str());
    init();

    resumed = WUActionResume == workunit->getAction();
    fatalHandler.setown(new CFatalHandler(globals->getPropInt("@fatal_timeout", FATAL_TIMEOUT)));
    querySent = false;
    nodeDiskUsageCached = false;

    StringBuffer pluginsDir;
    globals->getProp("@pluginsPath", pluginsDir);
    if (pluginsDir.length())
        addPathSepChar(pluginsDir);
    Owned<IConstWUPluginIterator> pluginIter = &workunit->getPlugins();
    ForEach(*pluginIter)
    {
        IConstWUPlugin &plugin = pluginIter->query();
        if (plugin.getPluginHole() || plugin.getPluginThor()) // JCSMORE ..Hole..
        {
            SCMStringBuffer name, version;
            plugin.getPluginName(name);
            plugin.getPluginVersion(version);
            loadPlugin(pluginMap, pluginsDir.str(), name.str(), version.str());
        }
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

void CJobMaster::broadcastToSlaves(CMessageBuffer &msg, mptag_t mptag, unsigned timeout, const char *errorMsg, mptag_t *_replyTag, bool sendOnly)
{
    mptag_t replyTag = createReplyTag();
    msg.setReplyTag(replyTag);
    if (!queryJobComm().send(msg, RANK_ALL_OTHER, mptag, timeout))
    {
        // think this should always be fatal, could check link down here, or in general and flag as _shutdown.
        StringBuffer msg("General failure communicating to slaves [");
        Owned<IThorException> e = MakeThorException(0, "%s", msg.append(errorMsg).append("]").str());
        e->setAction(tea_shutdown);
        EXCLOG(e, NULL);
        abort(e);
        throw e.getClear();
    }
    if (sendOnly) return;
    if (_replyTag)
        *_replyTag = replyTag;
    unsigned respondents = 0;
    loop
    {
        rank_t sender;
        CMessageBuffer msg;
        if (!queryJobComm().recv(msg, RANK_ALL, replyTag, &sender, LONGTIMEOUT))
        {
            if (_replyTag) _replyTag = NULL;
            StringBuffer tmpStr;
            Owned<IException> e = MakeThorFatal(NULL, 0, "%s - Timeout receiving from slaves", errorMsg?tmpStr.append(": ").append(errorMsg).str():"");
            EXCLOG(e, NULL);
            throw e.getClear();
        }
        if (_replyTag) _replyTag = NULL;
        bool error;
        msg.read(error);
        if (error)
        {
            Owned<IThorException> e = deserializeThorException(msg);
            e->setSlave(sender);
            throw e.getClear();
        }
        ++respondents;
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
        if (thisplugin.getPluginThor() || thisplugin.getPluginHole()) // JCSMORE ..Hole..
        {
            if (!plugins)
                plugins = workUnitInfo->addPropTree("plugins", createPTree());
            SCMStringBuffer name;
            thisplugin.getPluginName(name);
            IPropertyTree *plugin = plugins->addPropTree("plugin", createPTree());
            plugin->setProp("@name", name.str());
        }
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
            SCMStringBuffer istr;
            wu.getWuid(istr);
            xpath.append(istr.str()).append("/Action");
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
                job.stop(abort);
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
            job.fireException(LINK(exception)); 
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

    Owned<IConstWUGraphProgress> graphProgress = getGraphProgress();
    bool allDone = true;
    unsigned concurrentSubGraphs = (unsigned)getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
    try
    {
        ClearTempDirs();
        Owned<IWUGraphProgress> progress = graphProgress->update();
        progress->setGraphState(WUGraphRunning);
        progress.clear();
        
        Owned<IThorGraphIterator> iter = getSubGraphs();
        CopyCIArrayOf<CMasterGraph> toRun;
        ForEach(*iter)
        {
            CMasterGraph &graph = (CMasterGraph &)iter->query();
            if ((queryResumed() || queryUseCheckpoints()) && WUGraphComplete == graphProgress->queryNodeState(graph.queryGraphId()))
                graph.setCompleteEx();
            else
                toRun.append(graph);
        }
        graphProgress.clear();
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
    graphProgress.setown(getGraphProgress());
    Owned<IWUGraphProgress> progress = graphProgress->update();
    progress->setGraphState(aborted?WUGraphFailed:(allDone?WUGraphComplete:(pausing?WUGraphPaused:WUGraphComplete)));
    progress.clear();
    graphProgress.clear();

    if (queryPausing())
    {
        assertex(!queryUseCheckpoints()); // JCSMORE - checkpoints probably need revisiting

        // stash away spills ready for resume, make them owned by workunit in event of abort/delete
        Owned<IFileUsageIterator> iter = queryTempHandler()->getIterator();
        ForEach(*iter)
        {
            CFileUsageEntry &entry = iter->query();
            StringAttr tmpName = entry.queryName();
            Owned<IConstWUGraphProgress> graphProgress = getGraphProgress();
            if (WUGraphComplete == graphProgress->queryNodeState(entry.queryGraphId()))
            {
                IArrayOf<IGroup> groups;
                StringArray clusters;
                fillClusterArray(*this, tmpName, clusters, groups);
                Owned<IFileDescriptor> fileDesc = queryThorFileManager().create(*this, tmpName, clusters, groups, true, TDXtemporary|TDWnoreplicate);
                fileDesc->queryProperties().setPropBool("@pausefile", true); // JCSMORE - mark to keep, may be able to distinguish via other means

                IPropertyTree &props = fileDesc->queryProperties();
                props.setPropBool("@owned", true);
                bool blockCompressed=true; // JCSMORE, should come from helper really
                if (blockCompressed)
                    props.setPropBool("@blockCompressed", true);

                Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fileDesc);

                // NB: This is renaming/moving from temp path

                StringBuffer newName;
                queryThorFileManager().addScope(*this, tmpName, newName, true, true);
                verifyex(file->renamePhysicalPartFiles(newName.str(), NULL, NULL, queryBaseDirectory()));

                file->attach(newName);

                Owned<IWorkUnit> wu = &queryWorkUnit().lock();
                wu->addFile(newName, &clusters, entry.queryUsage(), entry.queryKind(), queryGraphName());
            }
        }
    }

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

void CJobMaster::stop(bool doAbort)
{
    pausing = true;
    if (doAbort)
    {
        queryJobManager().replyException(*this, NULL); 
        Owned<IException> e = MakeThorException(0, "Unable to recover from pausenow");
        fatalHandler->inform(e.getClear());
        abort(e);
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
        ((CJobMaster &)graph.queryJob()).broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, NULL, NULL, true);

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
    virtual void getResult(size32_t & retSize, void * & ret)
    {
        ensure();
        return result->getResult(retSize, ret);
    }
    virtual void getLinkedResult(unsigned & count, byte * * & ret)
    {
        ensure();
        return result->getLinkedResult(count, ret);
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
    bcastTag = TAG_NULL;
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
            abort(e);
            // fall through
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

void CMasterGraph::abort(IException *e)
{
    if (aborted) return;
    try{ CGraphBase::abort(e); }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting master graph");
        e->Release();
    }
    if (TAG_NULL != bcastTag)
        job.queryJobComm().cancel(0, bcastTag);
    if (started)
    {
        try
        {
            CMessageBuffer msg;
            msg.append(GraphAbort);
            msg.append(job.queryKey());
            msg.append(queryGraphId());
            jobM.broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "abort", &bcastTag);
        }
        catch (IException *e)
        {
            GraphPrintLog(e, "Aborting slave graph");
            if (abortException)
                throw LINK(abortException);
            throw;
        }
    }
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryJobManager().queryDeMonServer()->endGraph(this, true);
    }
}

void CMasterGraph::serializeCreateContexts(MemoryBuffer &mb)
{
    CGraphBase::serializeCreateContexts(mb);
    Owned<IThorActivityIterator> iter = (queryOwner() && !isGlobal()) ? getIterator() : getTraverseIterator();
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
                    jobM.broadcastToSlaves(msg, mpTag, LONGTIMEOUT, "serializeCreateContexts", &bcastTag);
                }
                catch (IException *e)
                {
                    GraphPrintLog(e, "Aborting graph create(2)");
                    if (abortException)
                        throw LINK(abortException);
                    throw;
                }
            }
        }
    }
}

void CMasterGraph::start()
{
    Owned<IThorActivityIterator> iter = getTraverseIterator();
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
        Owned<IThorActivityIterator> iter = getTraverseIterator();
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
                Owned<IThorActivityIterator> iter = getTraverseIterator();
                serializeActivityInitData(w, msg, *iter);
            }
            catch (IException *e)
            {
                GraphPrintLog(e, NULL);
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
            if (!job.queryJobComm().recv(msg, w+1, replyTag, &sender, LONGTIMEOUT))
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
            Owned<IConstWUGraphProgress> graphProgress = ((CJobMaster &)job).getGraphProgress();
            if (WUGraphComplete == graphProgress->queryNodeState(graphId))
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
            if (!queryJob().queryJobComm().recv(msg, RANK_ALL, executeReplyTag, &sender))
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
        jobM.broadcastToSlaves(msg, masterSlaveMpTag, LONGTIMEOUT, "sendGraph", &bcastTag);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "Aborting sendGraph");
        if (abortException)
            throw LINK(abortException);
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
            jobM.broadcastToSlaves(msg, mpTag, LONGTIMEOUT, "startCtx", &bcastTag, true);
        }
        catch (IException *e)
        {
            GraphPrintLog(e, "Aborting preStart");
            if (abortException)
                throw LINK(abortException);
            throw;
        }
    }

    if (syncInitData())
        sendActivityInitData(); // has to be done at least once
    CGraphBase::preStart(parentExtractSz, parentExtract);
    if (isGlobal())
    {
        if (!startBarrier->wait(false)) // ensure all graphs are at start point at same time, as some may request data from each other.
            return false;
    }
    if (!queryOwner())
    {
        Owned<IConstWUGraphProgress> graphProgress = ((CJobMaster &)job).getGraphProgress();
        Owned<IWUGraphProgress> progress = graphProgress->update();
        progress->setNodeState(graphId, WUGraphRunning);
        progress.clear();
    }
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
                    virtual void report(const char *name, const __int64 totaltime, const __int64 maxtime, const unsigned count)
                    {
                        StringBuffer timerStr(graph.queryJob().queryGraphName());
                        timerStr.append("(").append(graph.queryGraphId()).append("): ");
                        timerStr.append(name);
                        wu->setTimerInfo(timerStr.str(), NULL, (unsigned)totaltime, count, (unsigned)maxtime);
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
        Owned<IConstWUGraphProgress> graphProgress = ((CJobMaster &)job).getGraphProgress();
        Owned<IWUGraphProgress> progress = graphProgress->update();
        progress->setNodeState(graphId, graphDone?WUGraphComplete:WUGraphFailed);
        progress.clear();
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
                    GraphPrintLog("Activity id=%"ACTPF"d not created in master and not a child query activity", activityId);
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
                catch (IException *_e) { e.setown(_e); GraphPrintLog(_e, NULL); }
                if (!activity || e.get())
                {
                    GraphPrintLog("Activity id=%"ACTPF"d failed to created child query activity ready for progress", activityId);
                    return false;
                }
            }
            if (activity)
                activity->deserializeStats(node, mb);
        }
        else
        {
            GraphPrintLog("Failed to find activity, during progress deserialization, id=%"ACTPF"d", activityId);
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

CThorStats::CThorStats(const char *_prefix)
{
    unsigned c = queryClusterWidth();
    while (c--) counts.append(0);
    if (_prefix)
    {
        prefix.set(_prefix);
        StringBuffer tmp;
        labelMin.set(tmp.append(_prefix).append("Min"));
        labelMax.set(tmp.clear().append(_prefix).append("Max"));
        labelMinSkew.set(tmp.clear().append(_prefix).append("MinSkew"));
        labelMaxSkew.set(tmp.clear().append(_prefix).append("MaxSkew"));
        labelMinEndpoint.set(tmp.clear().append(_prefix).append("MinEndpoint"));
        labelMaxEndpoint.set(tmp.clear().append(_prefix).append("MaxEndpoint"));
    }
    else
    {
        labelMin.set("min");
        labelMax.set("max");
        labelMinSkew.set("minSkew");
        labelMaxSkew.set("maxSkew");
        labelMinEndpoint.set("minEndpoint");
        labelMaxEndpoint.set("maxEndpoint");
    }
}

void CThorStats::set(unsigned node, unsigned __int64 count)
{
    counts.replace(count, node);
}

void CThorStats::removeAttribute(IPropertyTree *node, const char *name)
{
    StringBuffer aName("@");
    node->removeProp(aName.append(name).str());
}

void CThorStats::addAttribute(IPropertyTree *node, const char *name, unsigned __int64 val)
{
    StringBuffer aName("@");
    node->setPropInt64(aName.append(name).str(), val);
}

void CThorStats::addAttribute(IPropertyTree *node, const char *name, const char *val)
{
    StringBuffer aName("@");
    node->setProp(aName.append(name).str(), val);
}

void CThorStats::reset()
{
    tot = max = avg = 0;
    min = (unsigned __int64) -1;
    minNode = maxNode = hi = lo = maxNode = minNode = 0;
}

void CThorStats::processInfo()
{
    reset();
    ForEachItemIn(n, counts)
    {
        unsigned __int64 thiscount = counts.item(n);
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
    if (max)
    {
        unsigned count = counts.ordinality();
        avg = tot/count;
        if (avg)
        {
            hi = (unsigned)((100 * (max-avg))/avg);
            lo = (unsigned)((100 * (avg-min))/avg);
        }
    }
}

void CThorStats::getXGMML(IPropertyTree *node, bool suppressMinMaxWhenEqual)
{
    processInfo();
    if (suppressMinMaxWhenEqual && (hi == lo))
    {
        removeAttribute(node, labelMin);
        removeAttribute(node, labelMax);
    }
    else
    {
        addAttribute(node, labelMin, min);
        addAttribute(node, labelMax, max);
    }
    if (hi == lo)
    {
        removeAttribute(node, labelMinEndpoint);
        removeAttribute(node, labelMaxEndpoint);
        removeAttribute(node, labelMinSkew);
        removeAttribute(node, labelMaxSkew);
    }
    else
    {
        addAttribute(node, labelMinSkew, lo);
        addAttribute(node, labelMaxSkew, hi);
        StringBuffer epStr;
        addAttribute(node, labelMinEndpoint, querySlaveGroup().queryNode(minNode).endpoint().getUrlStr(epStr).str());
        addAttribute(node, labelMaxEndpoint, querySlaveGroup().queryNode(maxNode).endpoint().getUrlStr(epStr.clear()).str());
    }
}

///////////////////////////////////////////////////

CTimingInfo::CTimingInfo() : CThorStats("time")
{
    StringBuffer tmp;
    labelMin.set(tmp.append(labelMin).append("Ms"));
    labelMax.set(tmp.clear().append(labelMax).append("Ms"));
}

///////////////////////////////////////////////////

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
    if (max)
    {
        unsigned count = counts.ordinality();
        avg = tot/count;
        if (avg)
        {
            hi = (unsigned)((100 * (max-avg))/avg);
            lo = (unsigned)((100 * (avg-min))/avg);
        }
    }
}

void ProgressInfo::getXGMML(IPropertyTree *node)
{
    CThorStats::getXGMML(node, true);
    addAttribute(node, "slaves", counts.ordinality());
    addAttribute(node, "count", tot);
    addAttribute(node, "started", startcount);
    addAttribute(node, "stopped", stopcount);
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

