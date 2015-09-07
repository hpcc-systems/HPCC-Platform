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
#include "jlzw.hpp"
#include "jhtree.hpp"
#include "daclient.hpp"
#include "commonext.hpp"
#include "thorplugin.hpp"
#include "thcodectx.hpp"
#include "thmem.hpp"
#include "thorport.hpp"
#include "slwatchdog.hpp"
#include "thgraphslave.hpp"
#include "thcompressutil.hpp"
#include "enginecontext.hpp"

//////////////////////////////////

class CBarrierSlave : public CInterface, implements IBarrier
{
    mptag_t tag;
    Linked<ICommunicator> comm;
    bool receiving;
    CJobChannel &jobChannel;

public:
    IMPLEMENT_IINTERFACE;

    CBarrierSlave(CJobChannel &_jobChannel, ICommunicator &_comm, mptag_t _tag) : jobChannel(_jobChannel), comm(&_comm), tag(_tag)
    {
        receiving = false;
    }
    virtual bool wait(bool exception, unsigned timeout)
    {
        Owned<IException> e;
        CTimeMon tm(timeout);
        unsigned remaining = timeout;
        CMessageBuffer msg;
        msg.append(false);
        msg.append(false); // no exception
        if (INFINITE != timeout && tm.timedout(&remaining))
        {
            if (exception)
                throw createBarrierAbortException();
            else
                return false;
        }
        if (!comm->send(msg, 0, tag, INFINITE != timeout ? remaining : LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierSlave::wait - Timeout sending to master");
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
            if (!comm->recv(msg, 0, tag, NULL, remaining))
                return false;
        }
        bool aborted;
        msg.read(aborted);
        bool hasExcept;
        msg.read(hasExcept);
        if (hasExcept)
            e.setown(deserializeException(msg));
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
            comm->cancel(jobChannel.queryMyRank(), tag);
        CMessageBuffer msg;
        msg.append(true);
        if (e)
        {
            msg.append(true);
            serializeException(e, msg);
        }
        else
            msg.append(false);
        if (!comm->send(msg, 0, tag, LONGTIMEOUT))
            throw MakeStringException(0, "CBarrierSlave::cancel - Timeout sending to master");
    }
    virtual const mptag_t queryTag() const { return tag; }
};

// 

CSlaveActivity::CSlaveActivity(CGraphElementBase *_container) : CActivityBase(_container)
{
    data = NULL;
}

CSlaveActivity::~CSlaveActivity()
{
    inputs.kill();
    outputs.kill();
    if (data) delete [] data;
    ActPrintLog("DESTROYED");
}

void CSlaveActivity::setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
{
    CActivityBase::setInput(index, inputActivity, inputOutIdx);
    Linked<IThorDataLink> outLink;
    if (!inputActivity)
    {
        Owned<CActivityBase> nullAct = container.factory(TAKnull);

        outLink.set(((CSlaveActivity *)(nullAct.get()))->queryOutput(0)); // NB inputOutIdx irrelevant, null has single 'fake' output
        nullAct->releaseIOs(); // normally done as graph winds up, clear now to avoid circular dependencies with outputs
    }
    else
        outLink.set(((CSlaveActivity *)inputActivity)->queryOutput(inputOutIdx));
    assertex(outLink);
    while (inputs.ordinality()<=index) inputs.append(NULL);
    inputs.replace(outLink.getClear(), index);
}

IThorDataLink *CSlaveActivity::queryOutput(unsigned index)
{
    if (index>=outputs.ordinality()) return NULL;
    return outputs.item(index);
}

IThorDataLink *CSlaveActivity::queryInput(unsigned index)
{
    if (index>=inputs.ordinality()) return NULL;
    return inputs.item(index);
}

void CSlaveActivity::startInput(IThorDataLink *itdl, const char *extra)
{
    StringBuffer s("Starting input");
    if (extra)
        s.append(" ").append(extra);
    ActPrintLog("%s", s.str());

#ifdef TRACE_STARTSTOP_EXCEPTIONS
    try
    {
        itdl->start();
    }
    catch(IException *e)
    {
        ActPrintLog(e, "%s", s.str());
        throw;
    }
#else
    itdl->start();
#endif
}

void CSlaveActivity::stopInput(IRowStream *itdl, const char *extra)
{
    StringBuffer s("Stopping input for");
    if (extra)
        s.append(" ").append(extra);
    ActPrintLog("%s", s.str());

#ifdef TRACE_STARTSTOP_EXCEPTIONS
    try
    {
        itdl->stop();
    }
    catch(IException * e)
    {
        ActPrintLog(e, "%s", s.str());
        throw;
    }
#else
    itdl->stop();
#endif
}

void CSlaveActivity::abort()
{
    CActivityBase::abort();
    CriticalBlock b(crit);
    ForEachItemIn(o, outputs)
    {
        StringBuffer msg("-------->  ");
        msg.append("GraphId = ").append(container.queryOwner().queryGraphId());
        msg.append(" ActivityId = ").append(container.queryId());
        msg.append("  OutputId = ").append(o);
        MemoryBuffer mb;
        outputs.item(o)->dataLinkSerialize(mb); // JCSMORE should add direct method
        rowcount_t count;
        mb.read(count);
        msg.append(": Count = ").append(count);
    }
}

void CSlaveActivity::releaseIOs()
{
//  inputs.kill(); // don't want inputs to die before this dies (release in deconstructor) // JCSMORE not sure why care particularly.
    outputs.kill(); // outputs tend to be self-references, this clears them explicitly, otherwise end up leaking with circular references.
}

void CSlaveActivity::clearConnections()
{
    inputs.kill();
}

MemoryBuffer &CSlaveActivity::queryInitializationData(unsigned slave) const
{
    CriticalBlock b(crit);
    if (!data)
        data = new MemoryBuffer[container.queryJob().querySlaves()];
    CMessageBuffer msg;
    graph_id gid = queryContainer().queryOwner().queryGraphId();
    msg.append(smt_dataReq);
    msg.append(slave);
    msg.append(gid);
    msg.append(container.queryId());
    if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryContainer().queryJob().querySlaveMpTag(), LONGTIMEOUT))
        throwUnexpected();
    data[slave].swapWith(msg);
    return data[slave];
}

MemoryBuffer &CSlaveActivity::getInitializationData(unsigned slave, MemoryBuffer &mb) const
{
    return mb.append(queryInitializationData(slave));
}

unsigned __int64 CSlaveActivity::queryLocalCycles() const
{
    unsigned __int64 inputCycles = 0;
    if (1 == inputs.ordinality())
    {
        IThorDataLink *input = inputs.item(0);
        inputCycles += input->queryTotalCycles();
    }
    else
    {
        switch (container.getKind())
        {
            case TAKchildif:
            case TAKchildcase:
                if (inputs.ordinality() && (((unsigned)-1) != container.whichBranch))
                {
                    IThorDataLink *input = inputs.item(container.whichBranch);
                    if (input)
                        inputCycles += input->queryTotalCycles();
                }
                break;
            default:
                ForEachItemIn(i, inputs)
                {
                    IThorDataLink *input = inputs.item(i);
                    inputCycles += input->queryTotalCycles();
                }
                break;
        }
    }
    unsigned __int64 _totalCycles = queryTotalCycles();
    if (_totalCycles < inputCycles) // not sure how/if possible, but guard against
        return 0;
    return _totalCycles-inputCycles;
}

unsigned __int64 CSlaveActivity::queryTotalCycles() const
{
    return totalCycles.totalCycles;
}

void CSlaveActivity::serializeStats(MemoryBuffer &mb)
{
    CriticalBlock b(crit);
    mb.append((unsigned __int64)cycle_to_nanosec(queryLocalCycles()));
    ForEachItemIn(i, outputs)
        outputs.item(i)->dataLinkSerialize(mb);
}

///

// CSlaveGraph

CSlaveGraph::CSlaveGraph(CJobChannel &jobChannel) : CGraphBase(jobChannel)
{
    jobS = (CJobSlave *)&jobChannel.queryJob();
}

void CSlaveGraph::init(MemoryBuffer &mb)
{
    mb.read(reinit);
    mpTag = queryJobChannel().deserializeMPTag(mb);
    startBarrierTag = queryJobChannel().deserializeMPTag(mb);
    waitBarrierTag = queryJobChannel().deserializeMPTag(mb);
    doneBarrierTag = queryJobChannel().deserializeMPTag(mb);
    startBarrier = queryJobChannel().createBarrier(startBarrierTag);
    waitBarrier = queryJobChannel().createBarrier(waitBarrierTag);
    if (doneBarrierTag != TAG_NULL)
        doneBarrier = queryJobChannel().createBarrier(doneBarrierTag);
    initialized = false;
    progressActive = progressToCollect = false;
    unsigned subCount;
    mb.read(subCount);
    while (subCount--)
    {
        graph_id gid;
        mb.read(gid);
        Owned<CSlaveGraph> subGraph = (CSlaveGraph *)queryJobChannel().getGraph(gid);
        subGraph->init(mb);
    }
}

void CSlaveGraph::initWithActData(MemoryBuffer &in, MemoryBuffer &out)
{
    CriticalBlock b(progressCrit);
    initialized = true;
    if (0 == in.length())
        return;
    activity_id id;
    loop
    {
        in.read(id);
        if (0 == id) break;
        CSlaveGraphElement *element = (CSlaveGraphElement *)queryElement(id);
        assertex(element);
        out.append(id);
        out.append((size32_t)0);
        unsigned l = out.length();
        size32_t sz;
        in.read(sz);
        unsigned aread = in.getPos();
        CSlaveActivity *activity = (CSlaveActivity *)element->queryActivity();
        if (activity)
        {
            element->sentActInitData->set(0);
            activity->init(in, out);
        }
        aread = in.getPos()-aread;
        if (aread<sz)
        {
            Owned<IException> e = MakeActivityException(element, TE_SeriailzationError, "Serialization error - activity did not read all serialized data (%d byte(s) remaining)", sz-aread);
            in.readDirect(sz-aread);
            throw e.getClear();
        }
        else if (aread>sz)
            throw MakeActivityException(element, TE_SeriailzationError, "Serialization error - activity read beyond serialized data (%d byte(s))", aread-sz);
        size32_t dl = out.length() - l;
        if (dl)
            out.writeDirect(l-sizeof(size32_t), sizeof(size32_t), &dl);
        else
            out.setLength(l-(sizeof(activity_id)+sizeof(size32_t)));
    }
    out.append((activity_id)0);
}

void CSlaveGraph::recvStartCtx()
{
    if (!sentStartCtx)
    {
        sentStartCtx = true;
        CMessageBuffer msg;

        if (!graphCancelHandler.recv(queryJobChannel().queryJobComm(), msg, 0, mpTag, NULL, LONGTIMEOUT))
            throw MakeStringException(0, "Error receiving startCtx data for graph: %" GIDPF "d", graphId);
        deserializeStartContexts(msg);
    }
}

bool CSlaveGraph::recvActivityInitData(size32_t parentExtractSz, const byte *parentExtract)
{
    bool ret = true;
    unsigned needActInit = 0;
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = (CGraphElementBase &)iter->query();
        CActivityBase *activity = element.queryActivity();
        if (activity && activity->needReInit())
            element.sentActInitData->set(0, false); // force act init to be resent
        if (!element.sentActInitData->test(0))
            ++needActInit;
    }
    if (needActInit)
    {
        mptag_t replyTag = TAG_NULL;
        size32_t len;
        CMessageBuffer actInitRtnData;
        actInitRtnData.append(false);
        CMessageBuffer msg;

        if (syncInitData())
        {
            if (!graphCancelHandler.recv(queryJobChannel().queryJobComm(), msg, 0, mpTag, NULL, LONGTIMEOUT))
                throw MakeStringException(0, "Error receiving actinit data for graph: %" GIDPF "d", graphId);
            replyTag = msg.getReplyTag();
            msg.read(len);
        }
        else
        {
            // initialize any for which no data was sent
            msg.append(smt_initActDataReq); // may cause graph to be created at master
            msg.append(queryGraphId());
            msg.append(queryJobChannel().queryMyRank()-1);
            assertex(!parentExtractSz || NULL!=parentExtract);
            msg.append(parentExtractSz);
            msg.append(parentExtractSz, parentExtract);
            Owned<IThorActivityIterator> iter = getConnectedIterator();
            ForEach(*iter)
            {
                CSlaveGraphElement &element = (CSlaveGraphElement &)iter->query();
                if (!element.sentActInitData->test(0))
                {
                    msg.append(element.queryId());
                    element.serializeStartContext(msg);
                }
            }
            msg.append((activity_id)0);
            if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                throwUnexpected();
            replyTag = queryJobChannel().deserializeMPTag(msg);
            msg.read(len);
        }
        try
        {
            MemoryBuffer actInitData;
            if (len)
                actInitData.append(len, msg.readDirect(len));
            initWithActData(actInitData, actInitRtnData);

            if (queryOwner() && !isGlobal())
            {
                // initialize any for which no data was sent
                Owned<IThorActivityIterator> iter = getConnectedIterator();
                ForEach(*iter)
                {
                    CSlaveGraphElement &element = (CSlaveGraphElement &)iter->query();
                    if (!element.sentActInitData->test(0))
                    {
                        element.sentActInitData->set(0);
                        CSlaveActivity *activity = (CSlaveActivity *)element.queryActivity();
                        if (activity)
                        {
                            MemoryBuffer in, out;
                            activity->init(in, out);
                            assertex(0 == out.length());
                        }
                    }
                }
            }
        }
        catch (IException *e)
        {
            actInitRtnData.clear();
            actInitRtnData.append(true);
            serializeThorException(e, actInitRtnData);
            e->Release();
            ret = false;
        }
        if (!queryJobChannel().queryJobComm().send(actInitRtnData, 0, replyTag, LONGTIMEOUT))
            throw MakeStringException(0, "Timeout sending init data back to master");
    }
    return ret;
}

bool CSlaveGraph::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    started = true;
    recvStartCtx();
    CGraphBase::preStart(parentExtractSz, parentExtract);

    if (!recvActivityInitData(parentExtractSz, parentExtract))
        return false;
    connect(); // only now do slave acts. have all their outputs prepared.
    if (isGlobal())
    {
        if (!startBarrier->wait(false))
            return false;
    }
    return true;
}

void CSlaveGraph::start()
{
    {
        SpinBlock b(progressActiveLock);
        progressActive = true;
        progressToCollect = true;
    }
    bool forceAsync = !queryOwner() || isGlobal();
    Owned<IThorActivityIterator> iter = getSinkIterator();
    unsigned sinks = 0;
    ForEach(*iter)
        ++sinks;
    ForEach(*iter)
    {
        CGraphElementBase &container = iter->query();
        CActivityBase *sinkAct = (CActivityBase *)container.queryActivity();
        --sinks;
        sinkAct->startProcess(forceAsync || 0 != sinks); // async, unless last
    }
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            jobS->queryProgressHandler()->startGraph(*this);
    }
}

void CSlaveGraph::connect()
{
    CriticalBlock b(progressCrit);
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach(*iter)
        iter->query().doconnect();
}

void CSlaveGraph::executeSubGraph(size32_t parentExtractSz, const byte *parentExtract)
{
    if (isComplete())
        return;
    Owned<IException> exception;
    try
    {
        CGraphBase::executeSubGraph(parentExtractSz, parentExtract);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "In executeSubGraph");
        exception.setown(e);
    }
    if (TAG_NULL != executeReplyTag)
    {
        CMessageBuffer msg;
        if (exception.get())
        {
            msg.append(true);
            serializeThorException(exception, msg);
        }
        else
            msg.append(false);
        queryJobChannel().queryJobComm().send(msg, 0, executeReplyTag, LONGTIMEOUT);
    }
    else if (exception)
        throw exception.getClear();
}

void CSlaveGraph::create(size32_t parentExtractSz, const byte *parentExtract)
{
    CriticalBlock b(progressCrit);
    if (queryOwner())
    {
        if (isGlobal())
        {
            CMessageBuffer msg;
            // nothing changed if rerunning, unless conditional branches different
            if (!graphCancelHandler.recv(queryJobChannel().queryJobComm(), msg, 0, mpTag, NULL, LONGTIMEOUT))
                throw MakeStringException(0, "Error receiving createctx data for graph: %" GIDPF "d", graphId);
            try
            {
                size32_t len;
                msg.read(len);
                if (len)
                {
                    MemoryBuffer initData;
                    initData.append(len, msg.readDirect(len));
                    deserializeCreateContexts(initData);
                }
                msg.clear();
                msg.append(false);
            }
            catch (IException *e)
            {
                msg.clear();
                msg.append(true);
                serializeThorException(e, msg);
            }
            if (!queryJobChannel().queryJobComm().send(msg, 0, msg.getReplyTag(), LONGTIMEOUT))
                throw MakeStringException(0, "Timeout sending init data back to master");
        }
        else
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
            if ((reinit || !sentInitData))
            {
                sentInitData = true;
                CMessageBuffer msg;
                msg.append(smt_initGraphReq);
                msg.append(graphId);
                if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                    throwUnexpected();
                size32_t len;
                msg.read(len);
                if (len)
                    deserializeCreateContexts(msg);

// could still request 1 off, onCreate serialization from master 1st.
                CGraphBase::create(parentExtractSz, parentExtract);
                return;
            }
        }
    }
    CGraphBase::create(parentExtractSz, parentExtract);
}

void CSlaveGraph::abort(IException *e)
{
    if (!graphDone) // set pre done(), no need to abort if got that far.
        CGraphBase::abort(e);
    getDoneSem.signal();
}

void CSlaveGraph::done()
{
    GraphPrintLog("End of sub-graph");
    {
        SpinBlock b(progressActiveLock);
        progressActive = false;
        progressToCollect = true; // NB: ensure collected after end of graph
    }
    if (!aborted && graphDone && (!queryOwner() || isGlobal()))
        getDoneSem.wait(); // must wait on master
    if (!queryOwner())
    {
        if (globals->getPropBool("@watchdogProgressEnabled"))
            jobS->queryProgressHandler()->stopGraph(*this, NULL);
    }

    Owned<IException> exception;
    try
    {
        CGraphBase::done();
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "In CSlaveGraph::done");
        exception.setown(e);
    }
    if (exception.get())
        throw LINK(exception.get());
}

void CSlaveGraph::end()
{
    CGraphBase::end();
    if (!queryOwner())
    {
        if (atomic_read(&nodesLoaded)) // wouldn't mean much if parallel jobs running
            GraphPrintLog("JHTree node stats:\ncacheAdds=%d\ncacheHits=%d\nnodesLoaded=%d\nblobCacheHits=%d\nblobCacheAdds=%d\nleafCacheHits=%d\nleafCacheAdds=%d\nnodeCacheHits=%d\nnodeCacheAdds=%d\n", atomic_read(&cacheAdds), atomic_read(&cacheHits), atomic_read(&nodesLoaded), atomic_read(&blobCacheHits), atomic_read(&blobCacheAdds), atomic_read(&leafCacheHits), atomic_read(&leafCacheAdds), atomic_read(&nodeCacheHits), atomic_read(&nodeCacheAdds));
        JSocketStatistics stats;
        getSocketStatistics(stats);
        StringBuffer s;
        getSocketStatisticsString(stats,s);
        GraphPrintLog("Socket statistics : %s\n",s.str());
        resetSocketStatistics();
    }
}

bool CSlaveGraph::serializeStats(MemoryBuffer &mb)
{
    unsigned beginPos = mb.length();
    mb.append((unsigned)queryJobChannel().queryMyRank()-1);
    mb.append(queryGraphId());
    unsigned cPos = mb.length();
    unsigned count = 0;
    mb.append(count);
    CriticalBlock b(progressCrit);
    // until started and activities initialized, activities are not ready to serlialize stats.
    if ((started&&initialized) || 0 == activityCount())
    {
        bool collect=false;
        {
            SpinBlock b(progressActiveLock);
            if (progressActive || progressToCollect)
            {
                progressToCollect = false;
                collect = true;
            }
        }
        if (collect)
        {
            unsigned sPos = mb.length();
            Owned<IThorActivityIterator> iter = getConnectedIterator();
            ForEach (*iter)
            {
                CGraphElementBase &element = iter->query();
                CSlaveActivity &activity = (CSlaveActivity &)*element.queryActivity();
                unsigned pos = mb.length();
                mb.append(activity.queryContainer().queryId());
                activity.serializeStats(mb);
                if (pos == mb.length()-sizeof(activity_id))
                    mb.rewrite(pos);
                else
                    ++count;
            }
            mb.writeDirect(cPos, sizeof(count), &count);
        }
        unsigned cqCountPos = mb.length();
        unsigned cq=0;
        mb.append(cq);
        Owned<IThorGraphIterator> childIter = getChildGraphs();
        ForEach(*childIter)
        {
            CSlaveGraph &graph = (CSlaveGraph &)childIter->query();
            if (graph.serializeStats(mb))
                ++cq;
        }
        if (count || cq)
        {
            mb.writeDirect(cqCountPos, sizeof(cq), &cq);
            return true;
        }
    }
    mb.rewrite(beginPos);
    return false;
}

void CSlaveGraph::serializeDone(MemoryBuffer &mb)
{
    mb.append(queryGraphId());
    unsigned cPos = mb.length();
    unsigned count=0;
    mb.append(count);
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        if (element.queryActivity())
        {
            CSlaveActivity &activity = (CSlaveActivity &)*element.queryActivity();
            unsigned rPos = mb.length();
            mb.append(element.queryId());
            unsigned nl=0;
            mb.append(nl); // place holder for size of mb
            unsigned l = mb.length();
            activity.processDone(mb);
            nl = mb.length()-l;
            if (0 == nl)
                mb.rewrite(rPos);
            else
            {
                mb.writeDirect(l-sizeof(nl), sizeof(nl), &nl);
                ++count;
            }
        }
    }
    mb.writeDirect(cPos, sizeof(count), &count);
}

void CSlaveGraph::getDone(MemoryBuffer &doneInfoMb)
{
    if (!started) return;
    GraphPrintLog("Entering getDone");
    if (!queryOwner() || isGlobal())
    {
        try
        {
            serializeDone(doneInfoMb);
            if (!queryOwner())
            {
                if (globals->getPropBool("@watchdogProgressEnabled"))
                    jobS->queryProgressHandler()->stopGraph(*this, &doneInfoMb);
            }
            doneInfoMb.append(job.queryMaxDiskUsage());
            queryJobChannel().queryTimeReporter().serialize(doneInfoMb);
        }
        catch (IException *)
        {
            GraphPrintLog("Leaving getDone");
            getDoneSem.signal();
            throw;
        }
    }
    GraphPrintLog("Leaving getDone");
    getDoneSem.signal();
}


class CThorSlaveGraphResults : public CThorGraphResults
{
    CSlaveGraph &graph;
    IArrayOf<IThorResult> globalResults;
    PointerArrayOf<CriticalSection> globalResultCrits;
    void ensureAtLeastGlobals(unsigned id)
    {
        while (globalResults.ordinality() < id)
        {
            globalResults.append(*new CThorUninitializedGraphResults(globalResults.ordinality()));
            globalResultCrits.append(new CriticalSection);
        }
    }
public:
    CThorSlaveGraphResults(CSlaveGraph &_graph,unsigned numResults) : CThorGraphResults(numResults), graph(_graph)
    {
    }
    ~CThorSlaveGraphResults()
    {
        clear();
    }
    virtual void clear()
    {
        CriticalBlock procedure(cs);
        results.kill();
        globalResults.kill();
        ForEachItemIn(i, globalResultCrits)
            delete globalResultCrits.item(i);
        globalResultCrits.kill();
    }
    IThorResult *getResult(unsigned id, bool distributed)
    {
        Linked<IThorResult> result;
        {
            CriticalBlock procedure(cs);
            ensureAtLeast(id+1);

            result.set(&results.item(id));
            if (!distributed || !result->isDistributed())
                return result.getClear();
            ensureAtLeastGlobals(id+1);
        }
        CriticalBlock b(*globalResultCrits.item(id)); // block other global requests for this result
        IThorResult *globalResult = &globalResults.item(id);
        if (!QUERYINTERFACE(globalResult, CThorUninitializedGraphResults))
            return LINK(globalResult);
        Owned<IThorResult> gr = graph.getGlobalResult(*result->queryActivity(), result->queryRowInterfaces(), ownerId, id);
        globalResults.replace(*gr.getLink(), id);
        return gr.getClear();
    }
};

IThorGraphResults *CSlaveGraph::createThorGraphResults(unsigned num)
{
    return new CThorSlaveGraphResults(*this, num);
}

IThorResult *CSlaveGraph::getGlobalResult(CActivityBase &activity, IRowInterfaces *rowIf, activity_id ownerId, unsigned id)
{
    mptag_t replyTag = queryMPServer().createReplyTag();
    CMessageBuffer msg;
    msg.setReplyTag(replyTag);
    msg.append(smt_getresult);
    msg.append(queryJobChannel().queryMyRank()-1);
    msg.append(graphId);
    msg.append(ownerId);
    msg.append(id);
    msg.append(replyTag);

    if (!queryJobChannel().queryJobComm().send(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
        throwUnexpected();

    Owned<IThorResult> result = ::createResult(activity, rowIf, false);
    Owned<IRowWriter> resultWriter = result->getWriter();

    MemoryBuffer mb;
    Owned<ISerialStream> stream = createMemoryBufferSerialStream(mb);
    CThorStreamDeserializerSource rowSource(stream);

    loop
    {
        loop
        {
            if (activity.queryAbortSoon())
                return NULL;
            msg.clear();
            if (activity.receiveMsg(msg, 0, replyTag, NULL, 60*1000))
                break;
            ActPrintLog(&activity, "WARNING: tag %d timedout, retrying", (unsigned)replyTag);
        }
        if (!msg.length())
            break; // done
        else
        {
            bool error;
            msg.read(error);
            if (error)
                throw deserializeThorException(msg);
            ThorExpand(msg, mb.clear());
            while (!rowSource.eos())
            {
                RtlDynamicRowBuilder rowBuilder(rowIf->queryRowAllocator());
                size32_t sz = rowIf->queryRowDeserializer()->deserialize(rowBuilder, rowSource);
                resultWriter->putRow(rowBuilder.finalizeRowClear(sz));
            }
        }
    }
    return result.getClear();
}


///////////////////////////

class CThorCodeContextSlave : public CThorCodeContextBase, implements IEngineContext
{
    mptag_t mptag;
    Owned<IDistributedFileTransaction> superfiletransaction;

    void invalidSetResult(const char * name, unsigned seq)
    {
        throw MakeStringException(0, "Attempt to output result ('%s',%d) from a child query", name ? name : "", (int)seq);
    }

public:
    CThorCodeContextSlave(CJobChannel &jobChannel, ILoadedDllEntry &querySo, IUserDescriptor &userDesc, mptag_t _mptag) : CThorCodeContextBase(jobChannel, querySo, userDesc), mptag(_mptag)
    {
    }
    virtual void setResultBool(const char *name, unsigned sequence, bool value) { invalidSetResult(name, sequence); }
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) { invalidSetResult(name, sequence); }
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) { invalidSetResult(stepname, sequence); }
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) { invalidSetResult(name, sequence); }
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) { invalidSetResult(name, sequence); }
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) { invalidSetResult(stepname, sequence); }
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) { invalidSetResult(name, sequence); }
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) { invalidSetResult(name, sequence); }
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) { invalidSetResult(name, sequence); }
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) { invalidSetResult(name, sequence); }
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) { invalidSetResult(name, sequence); }
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) { invalidSetResult(name, sequence); }

    virtual bool getResultBool(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) { throwUnexpected(); }
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual __int64 getResultInt(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual double getResultReal(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual char *getResultVarString(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) { throwUnexpected(); }
    virtual unsigned getResultHash(const char * name, unsigned sequence) { throwUnexpected(); }

    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) { throwUnexpected(); }

    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source)
    {
        DBGLOG("%s", text);
        Owned<IThorException> e = MakeThorException(code, "%s", text);
        e->setOrigin(source);
        e->setAction(tea_warning);
        e->setSeverity((ErrorSeverity)severity);
        jobChannel.fireException(e);
    }
    virtual unsigned getNodes() { return jobChannel.queryJob().querySlaves(); }
    virtual unsigned getNodeNum() { return jobChannel.queryMyRank()-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false)
    {
        CMessageBuffer msg;
        msg.append(smt_getPhysicalName);
        msg.append(logicalName);
        msg.append(getNodeNum());
        msg.append(create);
        if (!jobChannel.queryJobComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
            throwUnexpected();
        return (char *)msg.detach();
    }
    virtual unsigned __int64 getFileOffset(const char *logicalName)
    {
        CMessageBuffer msg;
        msg.append(smt_getFileOffset);
        if (!jobChannel.queryJobComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
            throwUnexpected();
        unsigned __int64 offset;
        msg.read(offset);
        return offset;
    }
    virtual IDistributedFileTransaction *querySuperFileTransaction()
    {
        // NB: shouldn't really have fileservice being called on slaves
        if (!superfiletransaction.get())
            superfiletransaction.setown(createDistributedFileTransaction(userDesc));
        return superfiletransaction.get();
    }
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) { throwUnexpected(); }
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) { throwUnexpected(); }
    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) { throwUnexpected(); }
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort)
    {
        DBGLOG("%s", text);
        Owned<IThorException> e = MakeThorException(code, "%s", text);
        e->setAssert(filename, lineno, column);
        e->setOrigin("user");
        e->setSeverity(SeverityError);
        if (!isAbort)
            e->setAction(tea_warning);
        jobChannel.fireException(e);
    }
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash)   { throwUnexpected(); }      // Should only call from master
    virtual IEngineContext *queryEngineContext() { return this; }
// IEngineContext impl.
    virtual DALI_UID getGlobalUniqueIds(unsigned num, SocketEndpoint *_foreignNode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignNode;
        if (_foreignNode && !_foreignNode->isNull())
            foreignNode.set(*_foreignNode);
        else
            foreignNode.set(globals->queryProp("@DALISERVERS"));
        return ::getGlobalUniqueIds(num, &foreignNode);
    }
    virtual bool allowDaliAccess() const
    {
        // NB. includes access to foreign Dalis.
        return jobChannel.queryJob().getOptBool("slaveDaliClient");
    }
};

class CSlaveGraphTempHandler : public CGraphTempHandler
{
public:
    CSlaveGraphTempHandler(CJobBase &job, bool errorOnMissing) : CGraphTempHandler(job, errorOnMissing)
    {
    }
    virtual bool removeTemp(const char *name)
    {
        OwnedIFile ifile = createIFile(name);
        return ifile->remove();
    }
};

#define SLAVEGRAPHPOOLLIMIT 10
CJobSlave::CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *_workUnitInfo, const char *graphName, const char *_querySo, mptag_t _mpJobTag, mptag_t _slavemptag) : CJobBase(graphName), watchdog(_watchdog)
{
    workUnitInfo.set(_workUnitInfo);
    workUnitInfo->getProp("token", token);
    workUnitInfo->getProp("user", user);
    workUnitInfo->getProp("wuid", wuid);
    workUnitInfo->getProp("scope", scope);

    init();

    oldNodeCacheMem = 0;
    mpJobTag = _mpJobTag;
    slavemptag = _slavemptag;

    IPropertyTree *plugins = workUnitInfo->queryPropTree("plugins");
    if (plugins)
    {
        StringBuffer pluginsDir, installDir, pluginsList;
        globals->getProp("@INSTALL_DIR", installDir); // could use for socachedir also?
        if (installDir.length())
            addPathSepChar(installDir);
        globals->getProp("@pluginsPath", pluginsDir);
        if (pluginsDir.length())
        {
            if (!isAbsolutePath(pluginsDir.str())) // if !absolute, then make relative to installDir if is one (e.g. master mount)
            {
                if (installDir.length())
                    pluginsDir.insert(0, installDir.str());
            }
            addPathSepChar(pluginsDir);
        }
        Owned<IPropertyTreeIterator> pluginIter = plugins->getElements("plugin");
        ForEach(*pluginIter)
        {
            StringBuffer pluginPath;
            IPropertyTree &plugin = pluginIter->query();
            pluginPath.append(pluginsDir).append(plugin.queryProp("@name"));
            if (pluginsList.length())
                pluginsList.append(ENVSEPCHAR);
            pluginsList.append(pluginPath);
        }
        pluginMap->loadFromList(pluginsList.str());
    }
#ifdef __linux__
// only relevant if dllsToSlaves=false and query_so_dir was fully qualified remote path (e.g. //<ip>/path/file
    RemoteFilename rfn;
    rfn.setRemotePath(_querySo);
    StringBuffer tempSo;
    if (!rfn.isLocal())
    {
        WARNLOG("Cannot load shared object directly from remote path, creating temporary local copy: %s", _querySo);
        GetTempName(tempSo,"so",true);
        copyFile(tempSo.str(), _querySo);
        _querySo = tempSo.str();
    }
#endif
    querySo.setown(createDllEntry(_querySo, false, NULL));
    tmpHandler.setown(createTempHandler(true));
}

void CJobSlave::addChannel(IMPServer *mpServer)
{
    jobChannels.append(*new CJobSlaveChannel(*this, mpServer, jobChannels.ordinality()));
}

void CJobSlave::startJob()
{
    CJobBase::startJob();
    unsigned minFreeSpace = (unsigned)getWorkUnitValueInt("MINIMUM_DISK_SPACE", 0);
    if (minFreeSpace)
    {
        unsigned __int64 freeSpace = getFreeSpace(queryBaseDirectory(grp_unknown, 0));
        if (freeSpace < ((unsigned __int64)minFreeSpace)*0x100000)
        {
            SocketEndpoint ep;
            ep.setLocalHost(0);
            StringBuffer s;
            throw MakeThorException(TE_NotEnoughFreeSpace, "Node %s has %u MB(s) of available disk space, specified minimum for this job: %u MB(s)", ep.getUrlStr(s).str(), (unsigned) freeSpace / 0x100000, minFreeSpace);
        }
    }
}

__int64 CJobSlave::getWorkUnitValueInt(const char *prop, __int64 defVal) const
{
    StringBuffer propName(prop);
    return workUnitInfo->queryPropTree("Debug")->getPropInt64(propName.toLowerCase().str(), defVal);
}

StringBuffer &CJobSlave::getWorkUnitValue(const char *prop, StringBuffer &str) const
{
    StringBuffer propName(prop);
    workUnitInfo->queryPropTree("Debug")->getProp(propName.toLowerCase().str(), str);
    return str;
}

bool CJobSlave::getWorkUnitValueBool(const char *prop, bool defVal) const
{
    StringBuffer propName(prop);
    return workUnitInfo->queryPropTree("Debug")->getPropBool(propName.toLowerCase().str(), defVal);
}

IGraphTempHandler *CJobSlave::createTempHandler(bool errorOnMissing)
{
    return new CSlaveGraphTempHandler(*this, errorOnMissing);
}

mptag_t CJobSlave::deserializeMPTag(MemoryBuffer &mb)
{
    mptag_t tag;
    deserializeMPtag(mb, tag);
    if (TAG_NULL != tag)
    {
        PROGLOG("CJobSlave::deserializeMPTag: tag = %d", (int)tag);
        for (unsigned c=0; c<queryJobChannels(); c++)
            queryJobChannel(c).queryJobComm().flush(tag);
    }
    return tag;
}


// IGraphCallback
CJobSlaveChannel::CJobSlaveChannel(CJobBase &_job, IMPServer *mpServer, unsigned channel) : CJobChannel(_job, mpServer, channel)
{
    codeCtx = new CThorCodeContextSlave(*this, job.queryDllEntry(), *job.queryUserDescriptor(), job.querySlaveMpTag());
}

IBarrier *CJobSlaveChannel::createBarrier(mptag_t tag)
{
    return new CBarrierSlave(*this, *jobComm, tag);
}

void CJobSlaveChannel::runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract)
{
    if (!graph.queryOwner())
        CJobChannel::runSubgraph(graph, parentExtractSz, parentExtract);
    else
        graph.doExecuteChild(parentExtractSz, parentExtract);
    CriticalBlock b(graphRunCrit);
    if (!graph.queryOwner())
        removeSubGraph(graph);
}

///////////////

bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path)
{
    StringBuffer locationName, primaryName;
    RemoteFilename primaryRfn;
    partDesc.getFilename(0, primaryRfn);
    primaryRfn.getPath(primaryName);

    OwnedIFile primaryIFile = createIFile(primaryName.str());
    try
    {
        if (primaryIFile->exists())
        {
            location = 0;
            ifile.set(primaryIFile);
            path.append(primaryName);
            return true;
        }
    }
    catch (IException *e)
    {
        ActPrintLog(&activity->queryContainer(), e, "In ensurePrimary");
        e->Release();
    }
    unsigned l;
    for (l=1; l<partDesc.numCopies(); l++)
    {
        RemoteFilename altRfn;
        partDesc.getFilename(l, altRfn);
        locationName.clear();
        altRfn.getPath(locationName);
        assertex(locationName.length());
        OwnedIFile backupIFile = createIFile(locationName.str());
        try
        {
            if (backupIFile->exists())
            {
                if (primaryRfn.isLocal())
                {
                    ensureDirectoryForFile(primaryIFile->queryFilename());
                    Owned<IException> e = MakeActivityWarning(activity, 0, "Primary file missing: %s, copying backup %s to primary location", primaryIFile->queryFilename(), locationName.str());
                    activity->fireException(e);
                    StringBuffer tmpName(primaryIFile->queryFilename());
                    tmpName.append(".tmp");
                    OwnedIFile tmpFile = createIFile(tmpName.str());
                    CFIPScope fipScope(tmpName.str());
                    copyFile(tmpFile, backupIFile);
                    try
                    {
                        tmpFile->rename(pathTail(primaryIFile->queryFilename()));
                        location = 0;
                        ifile.set(primaryIFile);
                        path.append(primaryName);
                    }
                    catch (IException *e)
                    {
                        try { tmpFile->remove(); } catch (IException *e) { ActPrintLog(&activity->queryContainer(), "Failed to delete temporary file"); e->Release(); }
                        Owned<IException> e2 = MakeActivityWarning(activity, e, "Failed to restore primary, failed to rename %s to %s", tmpName.str(), primaryIFile->queryFilename());
                        e->Release();
                        activity->fireException(e2);
                        ifile.set(backupIFile);
                        location = l;
                        path.append(locationName);
                    }
                }
                else // JCSMORE - should use daliservix perhaps to ensure primary
                {
                    Owned<IException> e = MakeActivityWarning(activity, 0, "Primary file missing: %s, using remote copy: %s", primaryIFile->queryFilename(), locationName.str());
                    activity->fireException(e);
                    ifile.set(backupIFile);
                    location = l;
                    path.append(locationName);
                }
                return true;
            }
        }
        catch (IException *e)
        {
            Owned<IThorException> e2 = MakeActivityException(activity, e);
            e->Release();
            throw e2.getClear();
        }
    }
    return false;
}

class CEnsurePrimaryPartFile : public CInterface, implements IReplicatedFile
{
    CActivityBase &activity;
    Linked<IPartDescriptor> partDesc;
    StringAttr logicalFilename;
    Owned<IReplicatedFile> part;
public:
    IMPLEMENT_IINTERFACE;

    CEnsurePrimaryPartFile(CActivityBase &_activity, const char *_logicalFilename, IPartDescriptor *_partDesc) 
        : activity(_activity), logicalFilename(_logicalFilename), partDesc(_partDesc)
    {
    }
    virtual IFile *open()
    {
        unsigned location;
        OwnedIFile iFile;
        StringBuffer filePath;
        if (globals->getPropBool("@autoCopyBackup", true)?ensurePrimary(&activity, *partDesc, iFile, location, filePath):getBestFilePart(&activity, *partDesc, iFile, location, filePath, &activity))
            return iFile.getClear();
        else
        {
            StringBuffer locations;
            IException *e = MakeActivityException(&activity, TE_FileNotFound, "No physical file part for logical file %s, found at given locations: %s (Error = %d)", logicalFilename.get(), getFilePartLocations(*partDesc, locations).str(), GetLastError());
            EXCLOG(e, NULL);
            throw e;
        }
    }

    RemoteFilenameArray &queryCopies() 
    { 
        if(!part.get()) 
            part.setown(partDesc->getReplicatedFile());
        return part->queryCopies(); 
    }
};

IReplicatedFile *createEnsurePrimaryPartFile(CActivityBase &activity, const char *logicalFilename, IPartDescriptor *partDesc)
{
    return new CEnsurePrimaryPartFile(activity, logicalFilename, partDesc);
}

///////////////

class CFileCache;
class CLazyFileIO : public CInterface, implements IFileIO, implements IDelayedFile
{
    CFileCache &cache;
    Owned<IReplicatedFile> repFile;
    Linked<IExpander> expander;
    bool compressed;
    StringAttr filename;
    CriticalSection crit;
    Owned<IFileIO> iFileIO; // real IFileIO

    void checkOpen(); // references CFileCache method

public:
    IMPLEMENT_IINTERFACE;
    CLazyFileIO(CFileCache &_cache, const char *_filename, IReplicatedFile *_repFile, bool _compressed, IExpander *_expander) : cache(_cache), filename(_filename), repFile(_repFile), compressed(_compressed), expander(_expander)
    {
    }
    ~CLazyFileIO()
    {
        iFileIO.clear();
    }

    const char *queryFindString() const { return filename.get(); } // for string HT

// IFileIO impl.
    virtual size32_t read(offset_t pos, size32_t len, void * data)
    {
        CriticalBlock b(crit);
        checkOpen();
        return iFileIO->read(pos, len, data);
    }
    virtual offset_t size()
    {
        CriticalBlock b(crit);
        checkOpen();
        return iFileIO->size();
    }
    virtual size32_t write(offset_t pos, size32_t len, const void * data)
    {
        CriticalBlock b(crit);
        checkOpen();
        return iFileIO->write(pos, len, data);
    }
    virtual void flush()
    {
        CriticalBlock b(crit);
        if (iFileIO)
            iFileIO->flush();
    }
    virtual void close()
    {
        CriticalBlock b(crit);
        if (iFileIO)
            iFileIO->close();
        iFileIO.clear();
    }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1)
    {
        CriticalBlock b(crit);
        checkOpen();
        return iFileIO->appendFile(file, pos, len);
    }
    virtual void setSize(offset_t size)
    {
        CriticalBlock b(crit);
        checkOpen();
        iFileIO->setSize(size);
    }
// IDelayedFile impl.
    virtual IMemoryMappedFile *queryMappedFile() { return NULL; }
    virtual IFileIO *queryFileIO() { return this; }
};

class CFileCache : public CInterface, implements IThorFileCache
{
    OwningStringSuperHashTableOf<CLazyFileIO> files;
    CICopyArrayOf<CLazyFileIO> openFiles;
    unsigned limit, purgeN;
    CriticalSection crit;

    class CDelayedFileWapper : public CInterface, implements IDelayedFile
    {
        CFileCache &cache;
        Linked<CLazyFileIO> lFile;
    public:
        IMPLEMENT_IINTERFACE;

        CDelayedFileWapper(CFileCache &_cache, CLazyFileIO &_lFile) : cache(_cache), lFile(&_lFile) { }

        ~CDelayedFileWapper()
        {
            cache.remove(*lFile);
        }
        // IDelayedFile impl.
        virtual IMemoryMappedFile *queryMappedFile() { return lFile->queryMappedFile(); }
        virtual IFileIO *queryFileIO() { return lFile->queryFileIO(); }
    };

    void purgeOldest()
    {
        // will be ordered oldest first.
        unsigned count = 0;
        CICopyArrayOf<CLazyFileIO> toClose;
        ForEachItemIn(o, openFiles)
        {
            CLazyFileIO &lFile = openFiles.item(o);
            toClose.append(lFile);
            if (++count>=purgeN) // crude for now, just remove oldest N
                break;
        }
        ForEachItemIn(r, toClose)
        {
            CLazyFileIO &lFile = toClose.item(r);
            lFile.close();
            openFiles.zap(lFile);
        }
    }
    bool _remove(CLazyFileIO &lFile)
    {
        bool ret = files.removeExact(&lFile);
        if (!ret) return false;
        openFiles.zap(lFile);
        return true;
    }
public:
    IMPLEMENT_IINTERFACE;

    CFileCache(unsigned _limit) : limit(_limit)
    {
        assertex(limit);
        purgeN = globals->getPropInt("@fileCachePurgeN", 10);
        if (purgeN > limit) purgeN=limit; // why would it be, but JIC.
        PROGLOG("FileCache: limit = %d, purgeN = %d", limit, purgeN);
    }

    void opening(CLazyFileIO &lFile)
    {
        CriticalBlock b(crit);
        if (openFiles.ordinality() >= limit)
        {
            purgeOldest(); // will close purgeN
            assertex(openFiles.ordinality() < limit);
        }
        openFiles.zap(lFile);
        openFiles.append(lFile);
    }

// IThorFileCache impl.
    virtual bool remove(IDelayedFile &dFile)
    {
        CLazyFileIO *lFile = QUERYINTERFACE(&dFile, CLazyFileIO);
        assertex(lFile);
        CriticalBlock b(crit);
        return _remove(*lFile);
    }
    virtual IDelayedFile *lookup(CActivityBase &activity, IPartDescriptor &partDesc, IExpander *expander)
    {
        StringBuffer filename;
        RemoteFilename rfn;
        partDesc.getFilename(0, rfn);
        rfn.getPath(filename);
        CriticalBlock b(crit);
        Linked<CLazyFileIO> file = files.find(filename.str());
        if (!file)
        {
            Owned<IReplicatedFile> repFile = createEnsurePrimaryPartFile(activity, filename.str(), &partDesc);
            bool compressed = partDesc.queryOwner().isCompressed();
            file.setown(new CLazyFileIO(*this, filename.str(), repFile.getClear(), compressed, expander));
        }
        files.replace(*LINK(file));
        return new CDelayedFileWapper(*this, *file); // to avoid circular dependency and allow destruction to remove from cache
    }
};
////
void CLazyFileIO::checkOpen()
{
    CriticalBlock b(crit);
    if (iFileIO)
        return;
    cache.opening(*this);
    Owned<IFile> iFile = repFile->open();
    if (NULL != expander.get())
        iFileIO.setown(createCompressedFileReader(iFile, expander));
    else if (compressed)
        iFileIO.setown(createCompressedFileReader(iFile));
    else
        iFileIO.setown(iFile->open(IFOread));
    if (!iFileIO.get())
        throw MakeThorException(0, "CLazyFileIO: failed to open: %s", filename.get());
}

IThorFileCache *createFileCache(unsigned limit)
{
    return new CFileCache(limit);
}
