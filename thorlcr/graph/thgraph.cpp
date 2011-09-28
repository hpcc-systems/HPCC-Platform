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

#include "thgraph.hpp"
#include "jptree.hpp"
#include "commonext.hpp"
#include "dasess.hpp"
#include "jhtree.hpp"
#include "thcodectx.hpp"
#include "thcrc.hpp"
#include "thbuf.hpp"
#include "thormisc.hpp"
#include "thbufdef.hpp"

PointerArray createFuncs;
void registerCreateFunc(CreateFunc func)
{
    createFuncs.append((void *)func);
}

///////////////////////////////////
    
//////

/////

class CThorGraphResult : public CInterface, implements IThorResult, implements IRowWriter
{
    rowcount_t rowStreamCount;
    IOutputMetaData *meta;
    Owned<IRowWriterMultiReader> rowBuffer;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    bool stopped, readers;
    bool local;

    void init()
    {
        stopped = false;
        allocator = rowIf->queryRowAllocator();
        meta = allocator->queryOutputMeta();
        rowStreamCount = 0;
        readers = false;
    }
public:
    IMPLEMENT_IINTERFACE;

    CThorGraphResult(IRowInterfaces *_rowIf, bool _local) : rowIf(_rowIf), local(_local)
    {
        init();
        rowBuffer.setown(createOverflowableBuffer(rowIf, LOCALRESULT_BUFFER_SIZE));
    }

// IRowWriter
    virtual void putRow(const void *row)
    {
        assertex(!readers);
        ++rowStreamCount;
        rowBuffer->putRow(row);
    }
    virtual void flush() { }
    virtual offset_t getPosition() { UNIMPLEMENTED; return 0; }

// IThorResult
    virtual IRowWriter *getWriter()
    {
        return LINK(this);
    }
    virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count)
    {
        assertex(!readers);
        rowBuffer.setown(stream);
        rowStreamCount = count;
    }
    virtual IRowStream *getRowStream()
    {
        readers = true;
        return rowBuffer->getReader();
    }
    virtual const bool isLocal() const { return local; }
    virtual IOutputMetaData *queryMeta() { return meta; }
    virtual void getResult(size32_t &len, void * & data)
    {
        Owned<IRowStream> stream = getRowStream();
        bool grouped = meta->isGrouped();
        MemoryBuffer mb;
        if (grouped)
        {
            OwnedConstThorRow prev = stream->nextRow();
            if (prev)
            {
                bool eog;
                loop
                {
                    eog = false;
                    OwnedConstThorRow next = stream->nextRow();
                    if (!next)
                        eog = true;
                    size32_t sz = meta->getRecordSize(prev);
                    mb.append(sz, prev.get());
                    mb.append(eog);
                    if (!next)
                    {
                        next.setown(stream->nextRow());
                        if (!next)
                            break;
                    }
                    prev.set(next);
                }
            }
        }
        else
        {
            loop
            {
                OwnedConstThorRow row = stream->nextRow();
                if (!row)
                    break;
                size32_t sz = meta->getRecordSize(row);
                mb.append(sz, row.get());
            }
        }
        len = mb.length();
        data = mb.detach();
    }
    virtual void getLinkedResult(unsigned &countResult, byte * * & result)
    {
        Owned<IRowStream> stream = getRowStream();
        countResult = 0;
        byte **rowset = allocator->createRowset(rowStreamCount);
        loop
        {
            OwnedConstThorRow row = stream->nextRow();
            if (!row)
            {
                row.setown(stream->nextRow());
                if (row)
                    rowset[countResult++] = NULL;
                else
                    break;
            }
            rowset[countResult++] = (byte *)row.getClear();
        }
        result = rowset;
    }
};

/////

class CThorGraphResults : public CInterface, implements IThorGraphResults
{
    class CThorUninitializedGraphResults : public CInterface, implements IThorResult
    {
        unsigned id;

    public:
        IMPLEMENT_IINTERFACE

        CThorUninitializedGraphResults(unsigned _id) { id = _id; }
        virtual IRowWriter *getWriter() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void setResultStream(IRowWriterMultiReader *stream, rowcount_t count) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual IRowStream *getRowStream() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual IOutputMetaData *queryMeta() { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual const bool isLocal() const { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void getResult(size32_t & retSize, void * & ret) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
        virtual void getLinkedResult(unsigned & count, byte * * & ret) { throw MakeStringException(0, "Graph Result %d accessed before it is created", id); }
    };
    IArrayOf<IThorResult> results;
    CriticalSection cs;
    void ensureAtleast(unsigned id)
    {
        while (results.ordinality() < id)
            results.append(*new CThorUninitializedGraphResults(results.ordinality()));
    }
public:
    IMPLEMENT_IINTERFACE;

    CThorGraphResults(unsigned _numResults) { }
    virtual void clear()
    {
        CriticalBlock procedure(cs);
        results.kill();
    }
    virtual IThorResult *queryResult(unsigned id)
    {
        CriticalBlock procedure(cs);
        if (results.ordinality() <= id)
            return NULL;
        IThorResult *res = &results.item(id);
        if (QUERYINTERFACE(res, CThorUninitializedGraphResults))
            return NULL;
        return res;
    }
    virtual IThorResult *getResult(unsigned id)
    {
        CriticalBlock procedure(cs);
        ensureAtleast(id+1);
        // NB: stream static after this, i.e. nothing can be added to this result
        return LINK(&results.item(id));
    }
    virtual IThorResult *createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool local=false)
    {
        Owned<CThorGraphResult> result = new CThorGraphResult(rowIf, local);
        setResult(id, result);
        return result;
    }
    virtual IThorResult *createResult(CActivityBase &activity, IRowInterfaces *rowIf, bool local=false)
    {
        return createResult(activity, results.ordinality(), rowIf, local);
    }
    virtual void setResult(unsigned id, IThorResult *result)
    {
        CriticalBlock procedure(cs);
        ensureAtleast(id);
        if (results.isItem(id))
            results.replace(*LINK(result), id);
        else
            results.append(*LINK(result));
    }
    virtual unsigned count() { return results.ordinality(); }
    virtual void getResult(size32_t & retSize, void * & ret, unsigned id)
    {
        results.item(id).getResult(retSize, ret);
    }
    virtual void getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
    {
        results.item(id).getLinkedResult(count, ret);
    }
};

IThorGraphResults *createThorGraphResults(unsigned num)
{
    return new CThorGraphResults(num);
}

//////////

class CThorBoundLoopGraph : public CInterface, implements IThorBoundLoopGraph
{
    CGraphBase *graph;
    activity_id activityId;
    Linked<IOutputMetaData> resultMeta;
    Owned<IOutputMetaData> counterMeta;
    Owned<IRowInterfaces> resultRowIf, countRowIf;

public:
    IMPLEMENT_IINTERFACE;

    CThorBoundLoopGraph(CGraphBase *_graph, IOutputMetaData * _resultMeta, unsigned _activityId) : graph(_graph), resultMeta(_resultMeta), activityId(_activityId)
    {
        counterMeta.setown(new CThorEclCounterMeta);
    }
    virtual void prepareCounterResult(CActivityBase &activity, IThorGraphResults *results, unsigned loopCounter, unsigned pos)
    {
        if (!countRowIf)
            countRowIf.setown(createRowInterfaces(counterMeta, activityId, activity.queryCodeContext()));
        RtlDynamicRowBuilder counterRow(countRowIf->queryRowAllocator());
        thor_loop_counter_t * res = (thor_loop_counter_t *)counterRow.ensureCapacity(sizeof(thor_loop_counter_t),NULL);
        *res = loopCounter;
        OwnedConstThorRow counterRowFinal = counterRow.finalizeRowClear(sizeof(thor_loop_counter_t));
        IThorResult *counterResult = results->createResult(activity, pos, countRowIf, true);
        Owned<IRowWriter> counterResultWriter = counterResult->getWriter();
        counterResultWriter->putRow(counterRowFinal.getClear());
    }
    virtual void prepareLoopResults(CActivityBase &activity, IThorGraphResults *results)
    {
        if (!resultRowIf)
            resultRowIf.setown(createRowInterfaces(resultMeta, activityId, activity.queryCodeContext()));
        IThorResult *loopResult = results->createResult(activity, 0, resultRowIf, true); // loop output, create and mark local
        IThorResult *inputResult = results->createResult(activity, 1, resultRowIf, true);
    }
    virtual IRowStream *execute(CActivityBase &activity, unsigned counter, IRowWriterMultiReader *inputStream, unsigned rowStreamCount, size32_t parentExtractSz, const byte *parentExtract)
    {
        Owned<IThorGraphResults> results = createThorGraphResults(3);
        prepareLoopResults(activity, results);
        if (counter)
        {
            prepareCounterResult(activity, results, counter, 2);
            graph->setLoopCounter(counter);
        }
        Owned<IThorResult> inputResult = results->getResult(1);
        if (inputStream)
            inputResult->setResultStream(inputStream, rowStreamCount);
        graph->executeChild(parentExtractSz, parentExtract, results, NULL);
        Owned<IThorResult> result0 = results->getResult(0);
        return result0->getRowStream();
    }
    virtual void execute(CActivityBase &activity, unsigned counter, IThorGraphResults *graphLoopResults, size32_t parentExtractSz, const byte *parentExtract)
    {
        Owned<IThorGraphResults> results = createThorGraphResults(1);
        if (counter)
        {
            prepareCounterResult(activity, results, counter, 0);
            graph->setLoopCounter(counter);
        }
        try
        {
            graph->executeChild(parentExtractSz, parentExtract, results, graphLoopResults);
        }
        catch (IException *e)
        {
            IThorException *te = QUERYINTERFACE(e, IThorException);
            if (!te)
            {
                Owned<IThorException> e2 = MakeActivityException(&activity, e, "Exception running child graphs");
                te = e2.getClear();
            }
            te->setActivityId(activity.queryActivityId());
            te->setGraphId(graph->queryGraphId());
            try { graph->abort(te); }
            catch (IException *e)
            {
                Owned<IThorException> e2 = MakeActivityException(&activity, e, "Exception whilst aborting graph");
                e->Release();
                EXCLOG(e2, NULL);
            }
            graph->queryJob().fireException(te);
            throw;
        }
    }
    virtual CGraphBase *queryGraph() { return graph; }
};

IThorBoundLoopGraph *createBoundLoopGraph(CGraphBase *graph, IOutputMetaData *resultMeta, unsigned activityId)
{
    return new CThorBoundLoopGraph(graph, resultMeta, activityId);
}

///////////////////////////////////
bool isDiskInput(ThorActivityKind kind)
{
    switch (kind)
    {
        case TAKcsvread:
        case TAKdiskread:
        case TAKdisknormalize:
        case TAKdiskaggregate:
        case TAKdiskcount:
        case TAKdiskgroupaggregate:
        case TAKindexread:
        case TAKindexcount:
        case TAKindexnormalize:
        case TAKindexaggregate:
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
            return true;
        default:
            return false;
    }
}

/////////////////////////////////// 
CGraphElementBase *createGraphElement(IPropertyTree &node, CGraphBase &owner, CGraphBase *resultsGraph)
{
    CGraphElementBase *container = NULL;
    ForEachItemIn(m, createFuncs)
    {
        CreateFunc func = (CreateFunc)createFuncs.item(m);
        container = func(node, owner, resultsGraph);
        if (container) break;
    }
    if (NULL == container)
    {
        ThorActivityKind tak = (ThorActivityKind)node.getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
        throw MakeStringException(TE_UnsupportedActivityKind, "Unsupported activity kind: %s", activityKindStr(tak));
    }
    container->setResultsGraph(resultsGraph);
    return container;
}

CGraphElementBase::CGraphElementBase(CGraphBase &_owner, IPropertyTree &_xgmml) : owner(&_owner)
{
    xgmml.setown(createPTreeFromIPT(&_xgmml));
    eclText.set(xgmml->queryProp("att[@name=\"ecl\"]/@value"));
    id = xgmml->getPropInt("@id", 0);
    kind = (ThorActivityKind)xgmml->getPropInt("att[@name=\"_kind\"]/@value", TAKnone);
    sink = isActivitySink(kind);
    bool coLocal = xgmml->getPropBool("att[@name=\"coLocal\"]/@value", false);
    isLocal = coLocal || xgmml->getPropBool("att[@name=\"local\"]/@value", false);
    isGrouped = xgmml->getPropBool("att[@name=\"grouped\"]/@value", false);
    resultsGraph = NULL;
    ownerId = xgmml->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
    onCreateCalled = onStartCalled = prepared = haveCreateCtx = haveStartCtx = nullAct = false;
    onlyUpdateIfChanged = xgmml->getPropBool("att[@name=\"_updateIfChanged\"]/@value", false);

    StringBuffer helperName("fAc");
    xgmml->getProp("@id", helperName);
    helperFactory = (EclHelperFactory) queryJob().queryDllEntry().getEntry(helperName.str());
    if (!helperFactory)
        throw MakeOsException(GetLastError(), "Failed to load helper factory method: %s (dll handle = %p)", helperName.str(), queryJob().queryDllEntry().getInstance());
    alreadyUpdated = false;
    whichBranch = (unsigned)-1;
    whichBranchBitSet.setown(createBitSet());
    newWhichBranch = false;
    isEof = false;
    sentActInitData.setown(createBitSet());
}

CGraphElementBase::~CGraphElementBase()
{
    activity.clear();
    baseHelper.clear(); // clear before dll is unloaded
}

CJobBase &CGraphElementBase::queryJob() const
{
    return owner->queryJob();
}

IGraphTempHandler *CGraphElementBase::queryTempHandler() const
{
    if (resultsGraph)
        return resultsGraph->queryTempHandler();
    else
        return queryJob().queryTempHandler();
}

void CGraphElementBase::releaseIOs()
{
    if (activity)
        activity->releaseIOs();
    connectedInputs.kill();
    inputs.kill();
    outputs.kill();
    activity.clear();
}

void CGraphElementBase::addDependsOn(CGraphBase *graph, int controlId)
{
    ForEachItemIn(i, dependsOn)
    {
        if (dependsOn.item(i).graph == graph)
            return;
    }
    dependsOn.append(*new CGraphDependency(graph, controlId));
}

IThorGraphIterator *CGraphElementBase::getAssociatedChildGraphs()
{
    return new ArrayIIteratorOf<CGraphArray, CGraphBase, IThorGraphIterator>(associatedChildGraphs);
}

IThorGraphDependencyIterator *CGraphElementBase::getDependsIterator()
{
    return new ArrayIIteratorOf<CGraphDependencyArray, CGraphDependency, IThorGraphDependencyIterator>(dependsOn);
}

void CGraphElementBase::reset()
{
    onStartCalled = false;
//  prepared = false;
    if (activity)
        activity->reset();
}

void CGraphElementBase::ActPrintLog(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(this, thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void CGraphElementBase::ActPrintLog(IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(this, e, thorlog_all, MCexception(e), format, args);
    va_end(args);
}

void CGraphElementBase::abort(IException *e)
{
    CActivityBase *activity = queryActivity();
    if (activity)
        activity->abort();
    Owned<IThorGraphIterator> graphIter = getAssociatedChildGraphs();
    ForEach (*graphIter)
    {
        graphIter->query().abort(e);
    }
}

void CGraphElementBase::doconnect()
{
    ForEachItemIn(i, connectedInputs)
    {
        CGraphElementBase *input = &connectedInputs.item(i);
        unsigned inputOutIdx = connectedInputsInputOutIdx.item(i);
        unsigned which = connectedInputsIndex.item(i);
        CActivityBase *inputActivity = input->queryActivity();
        activity->setInput(which, inputActivity, inputOutIdx);
    }
}

void CGraphElementBase::removeInput(unsigned which)
{
    assertex(activity);
    ActPrintLog("DISCONNECTING (id=%"ACTPF"d, idx=%d)", queryId(), which);
    unsigned pos = connectedInputsIndex.find(which);
    assertex(NotFound != pos);
    connectedInputsIndex.remove(pos);
    Linked<CGraphElementBase> input = &connectedInputs.item(pos);
    connectedInputs.remove(pos);
    unsigned posO = connectedInputsInputOutIdx.find(pos);
    assertex(NotFound != posO);
    connectedInputsInputOutIdx.remove(pos);
    input->connectedOutputsIndex.remove(posO);
    input->connectedOutputs.remove(posO);
    input->connectedOutputsInputIndex.remove(posO);
}

void CGraphElementBase::setInput(unsigned which, CGraphElementBase &input, unsigned inputOutIdx)
{
    assertex(activity);
    ActPrintLog("CONNECTING (id=%"ACTPF"d, idx=%d) to (id=%"ACTPF"d, idx=%d)", input.queryId(), inputOutIdx, queryId(), which);
    connectedInputsIndex.append(which);
    connectedInputs.append(*LINK(&input));
    connectedInputsInputOutIdx.append(inputOutIdx);
    input.connectedOutputsIndex.append(inputOutIdx);
    input.connectedOutputs.append(*this);
    input.connectedOutputsInputIndex.append(which);
}

void CGraphElementBase::serializeCreateContext(MemoryBuffer &mb)
{
    if (!onCreateCalled) return;
    mb.append(queryId());
    unsigned pos = mb.length();
    mb.append((size32_t)0);
    queryHelper()->serializeCreateContext(mb);
    size32_t sz = (mb.length()-pos)-sizeof(size32_t);
    mb.writeDirect(pos, sizeof(sz), &sz);
}

void CGraphElementBase::serializeStartContext(MemoryBuffer &mb)
{
    if (!onStartCalled) return;
    mb.append(queryId());
    unsigned pos = mb.length();
    mb.append((size32_t)0);
    queryHelper()->serializeStartContext(mb);
    size32_t sz = (mb.length()-pos)-sizeof(size32_t);
    mb.writeDirect(pos, sizeof(sz), &sz);
}

void CGraphElementBase::deserializeCreateContext(MemoryBuffer &mb)
{
    size32_t createCtxLen;
    mb.read(createCtxLen);
    createCtxMb.clear().append(createCtxLen, mb.readDirect(createCtxLen));
    haveCreateCtx = true;
}

void CGraphElementBase::deserializeStartContext(MemoryBuffer &mb)
{
    size32_t startCtxLen;
    mb.read(startCtxLen);
    startCtxMb.append(startCtxLen, mb.readDirect(startCtxLen));
    haveStartCtx = true;
}

void CGraphElementBase::onCreate()
{
    if (onCreateCalled) return;
    baseHelper.setown(helperFactory());
    if (!nullAct)
    {
        CGraphElementBase *ownerActivity = owner->queryOwner() ? owner->queryOwner()->queryElement(ownerId) : NULL;
        baseHelper->onCreate(queryCodeContext(), ownerActivity ? ownerActivity->queryHelper() : NULL, haveCreateCtx?&createCtxMb:NULL);
    }
    onCreateCalled = true;
}

void CGraphElementBase::onStart(size32_t parentExtractSz, const byte *parentExtract)
{
    if (nullAct ||onStartCalled) return;
    if (haveStartCtx)
    {
        baseHelper->onStart(parentExtract, &startCtxMb);
        startCtxMb.reset();
        haveStartCtx = false;
    }
    else
        baseHelper->onStart(parentExtract, NULL);
    onStartCalled = true;
}

bool CGraphElementBase::executeDependencies(size32_t parentExtractSz, const byte *parentExtract, int controlId, bool async)
{
    Owned<IThorGraphDependencyIterator> deps = getDependsIterator();
    ForEach(*deps)
    {
        CGraphDependency &dep = deps->query();
        if (dep.controlId == controlId)
            dep.graph->execute(parentExtractSz, parentExtract, true, async);
        if (owner->queryJob().queryAborted() || owner->queryAborted()) return false;
    }
    return true;
}

bool CGraphElementBase::prepareContext(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async)
{
//  if (isPrepared()) // if >1 output, another arm may have traversed already
//      return true;
//  prepared = true;

    bool _shortCircuit = shortCircuit;
    Owned<IThorGraphDependencyIterator> deps = getDependsIterator();
    bool depsDone = true;
    ForEach(*deps)
    {
        CGraphDependency &dep = deps->query();
        if (0 == dep.controlId && NotFound == owner->dependentSubGraphs.find(*dep.graph))
            owner->dependentSubGraphs.append(*dep.graph);
        if (!dep.graph->isComplete())
            depsDone = false;
    }
    if (depsDone) _shortCircuit = false;
    if (!depsDone && checkDependencies)
    {
        if (!executeDependencies(parentExtractSz, parentExtract, 0, async))
            return false;
    }
    whichBranch = (unsigned)-1;
    isEof = false;
    alreadyUpdated = false;
    switch (getKind())
    {
        case TAKindexwrite:
        case TAKdiskwrite:
        case TAKcsvwrite:
        case TAKxmlwrite:
            if (_shortCircuit) return true;
            onCreate();
            alreadyUpdated = checkUpdate();
            if (alreadyUpdated)
                return false;
            break;
        case TAKchildif:
            owner->ifs.append(*this);
            // fall through
        case TAKif:
        {
            if (_shortCircuit) return true;
            onCreate();
            onStart(parentExtractSz, parentExtract);
            IHThorIfArg *helper = (IHThorIfArg *)baseHelper.get();
            whichBranch = helper->getCondition() ? 0 : 1;       // True argument preceeds false...
            if (inputs.isItem(whichBranch))
            {
                if (!whichBranchBitSet->testSet(whichBranch)) // if not set, new
                    newWhichBranch = true;
                return inputs.item(whichBranch).prepareContext(parentExtractSz, parentExtract, checkDependencies, false, async);
            }
            return true;
        }
        case TAKcase:
        {
            if (_shortCircuit) return true;
            onCreate();
            onStart(parentExtractSz, parentExtract);
            IHThorCaseArg *helper = (IHThorCaseArg *)baseHelper.get();
            whichBranch = helper->getBranch();
            if (inputs.isItem(whichBranch))
                return inputs.item(whichBranch).prepareContext(parentExtractSz, parentExtract, checkDependencies, false, async);
            return true;
        }
        case TAKfilter:
        case TAKfiltergroup:
        case TAKfilterproject:
        {
            if (_shortCircuit) return true;
            onCreate();
            onStart(parentExtractSz, parentExtract);
            switch (getKind())
            {
                case TAKfilter:
                    isEof = !((IHThorFilterArg *)baseHelper.get())->canMatchAny();
                    break;
                case TAKfiltergroup:
                    isEof = !((IHThorFilterGroupArg *)baseHelper.get())->canMatchAny();
                    break;
                case TAKfilterproject:
                    isEof = !((IHThorFilterProjectArg *)baseHelper.get())->canMatchAny();
                    break;
            }
            if (isEof)
                return true;
            break;
        }
    }
    ForEachItemIn(i, inputs)
    {
        CGraphElementBase &input = inputs.item(i);
        if (!input.prepareContext(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async))
            return false;
    }
    return true;
}

void CGraphElementBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    activity->preStart(parentExtractSz, parentExtract);
}

void CGraphElementBase::start()
{
    queryActivity()->startProcess();
}

void CGraphElementBase::initActivity()
{
    switch (getKind())
    {
        case TAKlooprow:
        case TAKloopcount:
        case TAKloopdataset:
        case TAKgraphloop:
        case TAKparallelgraphloop:
        {
            unsigned loopId = queryXGMML().getPropInt("att[@name=\"_loopid\"]/@value");
            Owned<CGraphBase> childGraph = owner->getChildGraph(loopId);
            Owned<IThorBoundLoopGraph> boundLoopGraph = createBoundLoopGraph(childGraph, baseHelper->queryOutputMeta(), queryId());
            setBoundGraph(boundLoopGraph);
            break;
        }
    }
}

void CGraphElementBase::createActivity(size32_t parentExtractSz, const byte *parentExtract)
{
    if (activity)
        return;
    try
    {
        switch (getKind())
        {
            case TAKchildif:
            {
                if (inputs.isItem(whichBranch))
                {
                    CGraphElementBase &input = inputs.item(whichBranch);
                    input.createActivity(parentExtractSz, parentExtract);
                }
                onCreate();
                activity.setown(factory());
                initActivity();
                if (whichBranch<inputs.ordinality())
                {
                    CGraphElementBase *input = &inputs.item(whichBranch);
                    unsigned inputOutIdx = inputOutIndexes.item(whichBranch);
                    setInput(whichBranch, *input, inputOutIdx);
                }
                break;
            }
            case TAKif:
            case TAKcase:
                if (inputs.isItem(whichBranch))
                {
                    CGraphElementBase &input = inputs.item(whichBranch);
                    input.createActivity(parentExtractSz, parentExtract);
                }
                else
                {
                    onCreate();
                    activity.setown(factory(TAKnull));
                    initActivity();
                }
                break;
            default:
                if (!isEof)
                {
                    ForEachItemIn(i, inputs)
                    {
                        CGraphElementBase &input = inputs.item(i);
                        input.createActivity(parentExtractSz, parentExtract);
                    }
                }
                onCreate();
                if (isDiskInput(getKind()))
                    onStart(parentExtractSz, parentExtract);
                activity.setown(factory());
                ForEachItemIn(i2, inputs)
                {
                    CGraphElementBase *input = &inputs.item(i2);
                    unsigned inputOutIdx = inputOutIndexes.item(i2);
                    CGraphElementBase *here = this;
                    loop
                    {
                        switch (input->getKind())
                        {
                            case TAKif:
//                          case TAKchildif:
                            case TAKcase:
                            {
                                CGraphElementBase *next = input;
                                if (next->whichBranch >= input->inputs.ordinality()) // if, will have TAKnull activity, made at create time.
                                {
                                    here = NULL;
                                    break;
                                }
                                inputOutIdx = NotFound;
                                input = &input->inputs.item(next->whichBranch);
                                ForEachItemIn(o, input->outputs)
                                {
                                    CGraphElementBase &out = input->outputs.item(o);
                                    if (next == &out)
                                    {
                                        if (next->whichBranch == input->outputInputIndexes.item(o))
                                        {
                                            inputOutIdx = o;
                                            break;
                                        }
                                    }
                                }
                                assertex(NotFound!=inputOutIdx);
                                here = next;
                                break;
                            }
                            default:
                                here = NULL;
                                break;
                        }
                        if (!here)
                            break;
                    }
                    setInput(i2, *input, inputOutIdx);
                }
                initActivity();
                break;
        }
    }
    catch (IException *e) { ActPrintLog(e, NULL); activity.clear(); throw; }
}

ICodeContext *CGraphElementBase::queryCodeContext()
{
    return queryOwner().queryCodeContext();
}
/////

unsigned CGraphTableCopy::getHashFromElement(const void *et) const
{
    return hashc((const unsigned char *) &(((CGraphBase *) et)->queryGraphId()), sizeof(graph_id), 0);
}

unsigned CGraphTableCopy::getHashFromFindParam(const void *fp) const
{
    return hashc((const unsigned char *) fp, sizeof(graph_id), 0);
}

const void *CGraphTableCopy::getFindParam(const void *et) const
{
    return &(((CGraphBase *)et)->queryGraphId());
}

bool CGraphTableCopy::matchesFindParam(const void *et, const void *fp, unsigned fphash) const
{
    return (((CGraphBase *) et)->queryGraphId()) == *(graph_id *)fp;
}

/////

void CGraphTable::onRemove(void *et) { ((CGraphBase *)et)->Release(); }

/////

class CSubGraphIterator : public CInterface, implements IThorGraphIterator
{
    SuperHashIteratorOf<CGraphBase> iter;
public:
    IMPLEMENT_IINTERFACE;

    CSubGraphIterator(CGraphTable &table) : iter(table) { }
    virtual bool first() { return iter.first(); }
    virtual bool next() { return iter.next(); }
    virtual bool isValid() { return iter.isValid(); }
    virtual CGraphBase & query() { return iter.query(); }
};

// JCSMORE loop - probably need better way to check if any act in graph is global(meaning needs some synchronization between slaves in activity execution)
bool isGlobalActivity(CGraphElementBase &container)
{
    switch (container.getKind())
    {
// always global, but only co-ordinate init/done
        case TAKcsvwrite:
        case TAKxmlwrite:
        case TAKindexwrite:
        case TAKkeydiff:
        case TAKkeypatch:
            return true;
        case TAKdiskwrite:
        {
            Owned<IHThorDiskWriteArg> helper = (IHThorDiskWriteArg *)container.helperFactory();
            unsigned flags = helper->getFlags();
            return (0 == (TDXtemporary & flags)); // global if not temporary
        }
        case TAKspill:
            return false;
        case TAKlooprow:
        case TAKloopcount:
        case TAKgraphloop:
        case TAKparallelgraphloop:
        case TAKloopdataset:
// dependent on child acts?
            // JCSMORE will synchronise when gets to executing child graphs?
            // JCSMORE - well loop graph may be global, but that would mean loop act. would launch globally and sync, so ok?
//          if (queryLoopGraph()->queryGraph())
//              return queryLoopGraph()->queryGraph()->hasFinalInfo();
            return false;


// dependent on local/grouped
        case TAKkeyeddistribute:
        case TAKhashdistribute:
        case TAKhashdistributemerge:
        case TAKworkunitwrite:
        case TAKdistribution:
        case TAKpartition:
        case TAKdiskaggregate:
        case TAKdiskcount:
        case TAKdiskgroupaggregate:
        case TAKindexaggregate:
        case TAKindexcount:
        case TAKindexgroupaggregate:
        case TAKindexgroupexists:
        case TAKindexgroupcount:
        case TAKremoteresult:
        case TAKcountproject:
        case TAKcreaterowlimit:
        case TAKskiplimit:
        case TAKlimit:
        case TAKsort:
        case TAKdedup:
        case TAKjoin:
        case TAKselfjoin:
        case TAKhashjoin:
        case TAKkeyeddenormalize:
        case TAKhashdenormalize:
        case TAKdenormalize:
        case TAKlookupdenormalize:
        case TAKalldenormalize:
        case TAKdenormalizegroup:
        case TAKhashdenormalizegroup:
        case TAKlookupdenormalizegroup:
        case TAKkeyeddenormalizegroup:
        case TAKalldenormalizegroup:
        case TAKaggregate:
        case TAKexistsaggregate:
        case TAKcountaggregate:
        case TAKhashaggregate:
        case TAKhashdedup:
        case TAKrollup:
        case TAKiterate:
        case TAKselectn:
        case TAKfirstn:
        case TAKenth:
        case TAKsample:
        case TAKgroup:
        case TAKchoosesets:
        case TAKchoosesetsenth:
        case TAKchoosesetslast:
        case TAKtopn:
        case TAKprocess:
        case TAKchildcount:
            if (!container.queryLocalOrGrouped())
                return true;
            break;

        case TAKkeyedjoin:
        case TAKalljoin:
        case TAKlookupjoin:
            if (!container.queryLocal())
                return true;
// always local
        case TAKcountdisk:
        case TAKfilter:
        case TAKsplit:
        case TAKpipewrite:
        case TAKdegroup:
        case TAKproject:
        case TAKprefetchproject:
        case TAKprefetchcountproject:
        case TAKnormalize:
        case TAKnormalizechild:
        case TAKnormalizelinkedchild:
        case TAKpipethrough:
        case TAKif:
        case TAKchildif:
        case TAKcase:
        case TAKparse:
        case TAKxmlparse:
        case TAKjoinlight:
        case TAKselfjoinlight:
        case TAKdiskread:
        case TAKdisknormalize:
        case TAKchildaggregate:
        case TAKchildgroupaggregate:
        case TAKchildthroughnormalize:
        case TAKchildnormalize:

        case TAKindexread:
        case TAKindexnormalize:
        case TAKcsvread:
        case TAKxmlread:
        case TAKdiskexists:
        case TAKindexexists:
        case TAKchildexists:


        case TAKthroughaggregate:
        case TAKmerge:
        case TAKfunnel:
        case TAKregroup:
        case TAKcombine:
        case TAKrollupgroup:
        case TAKcombinegroup:
        case TAKsoap_rowdataset:
        case TAKsoap_rowaction:
        case TAKsoap_datasetdataset:
        case TAKsoap_datasetaction:
        case TAKrawiterator:
        case TAKlinkedrawiterator:
        case TAKchilditerator:
        case TAKstreamediterator:
        case TAKworkunitread:
        case TAKchilddataset:
        case TAKtemptable:
        case TAKtemprow:
        case TAKnull:
        case TAKemptyaction:
        case TAKlocalresultread:
        case TAKlocalresultwrite:
        case TAKgraphloopresultread:
        case TAKgraphloopresultwrite:
        case TAKnwaygraphloopresultread:
        case TAKapply:
        case TAKsideeffect:
        case TAKsimpleaction:
        case TAKsorted:
            break;

        case TAKnwayjoin:
        case TAKnwaymerge:
        case TAKnwaymergejoin:
        case TAKnwayinput:
        case TAKnwayselect:
            return false; // JCSMORE - I think and/or have to be for now
// undefined
        case TAKdatasetresult:
        case TAKrowresult:
        case TAKremotegraph:
        case TAKlibrarycall:
        case TAKnonempty:
        default:
            return true; // if in doubt
    }
    return false;
}

/////

CGraphBase::CGraphBase(CJobBase &_job) : job(_job)
{
    xgmml = NULL;
    parent = owner = NULL;
    graphId = 0;
    complete = false;
    reinit = false; // should graph reinitialize each time it is called (e.g. in loop graphs)
                    // This is currently for 'init' (Create time) info and onStart into
    sentInitData = false;
//  sentStartCtx = false;
    sentStartCtx = true; // JCSMORE - disable for now
    parentActivityId = 0;
    created = connected = started = graphDone = aborted = prepared = false;
    startBarrier = waitBarrier = doneBarrier = NULL;
    mpTag = waitBarrierTag = startBarrierTag = doneBarrierTag = TAG_NULL;
    executeReplyTag = TAG_NULL;
    poolThreadHandle = 0;
    parentExtractSz = 0;

}

CGraphBase::~CGraphBase()
{
    clean();
}

void CGraphBase::clean()
{
    ::Release(startBarrier);
    ::Release(waitBarrier);
    ::Release(doneBarrier);
    disconnectActivities();
    containers.kill();
    sinks.kill();
}

void CGraphBase::serializeCreateContexts(MemoryBuffer &mb, bool created)
{
    unsigned pos = mb.length();
    mb.append((unsigned)0);
    Owned<IThorActivityIterator> iter = created ? getCreatedIterator() : getTraverseIteratorCond();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        element.serializeCreateContext(mb);
    }
    mb.append((activity_id)0);
    unsigned len=mb.length()-pos-sizeof(unsigned);
    mb.writeDirect(pos, sizeof(len), &len);
}

void CGraphBase::serializeStartContexts(MemoryBuffer &mb)
{
    unsigned pos = mb.length();
    mb.append((unsigned)0);
    Owned<IThorActivityIterator> iter = getTraverseIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        element.serializeStartContext(mb);
    }
    mb.append((activity_id)0);
    unsigned len=mb.length()-pos-sizeof(unsigned);
    mb.writeDirect(pos, sizeof(len), &len);
}

void CGraphBase::deserializeCreateContexts(MemoryBuffer &mb)
{
    activity_id id;
    loop
    {
        mb.read(id);
        if (0 == id) break;
        CGraphElementBase *element = queryElement(id);
        assertex(element);
        element->deserializeCreateContext(mb);
    }
}

void CGraphBase::deserializeStartContexts(MemoryBuffer &mb)
{
    activity_id id;
    loop
    {
        mb.read(id);
        if (0 == id) break;
        CGraphElementBase *element = queryElement(id);
        assertex(element);
        element->deserializeStartContext(mb);
    }
}

void CGraphBase::reset()
{
    setCompleteEx(false);
    counter = 0;
    if (0 == containers.count())
    {
        SuperHashIteratorOf<CGraphBase> iter(childGraphs);
        ForEach(iter)
            iter.query().reset();
    }
    else
    {
        CGraphElementIterator iterC(containers);
        ForEach(iterC)
        {
            CGraphElementBase &element = iterC.query();
            element.reset();
        }
    }
    job.queryTimeReporter().reset();
    clearNodeStats();
}

void CGraphBase::addChildGraph(CGraphBase &graph)
{
    CriticalBlock b(crit);
    childGraphs.replace(graph);
    job.associateGraph(graph);
}

IThorGraphIterator *CGraphBase::getChildGraphs()
{
    CriticalBlock b(crit);
    return new CSubGraphIterator(childGraphs);
}

bool CGraphBase::fireException(IException *e)
{
    return job.fireException(e);
}

bool CGraphBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    Owned<IThorActivityIterator> iter = getTraverseIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        element.preStart(parentExtractSz, parentExtract);
    }
    return true;
}

void CGraphBase::start()
{
    Owned<IThorActivityIterator> iter = getTraverseIterator();
    ForEach (*iter)
    {
        CGraphElementBase &act = iter->query();
        act.start();
    }
}

void CGraphBase::executeSubGraph(size32_t parentExtractSz, const byte *parentExtract)
{
    if (job.queryPausing())
        return;
    Owned<IException> exception;
    try
    {
        if (!prepare(parentExtractSz, parentExtract, false, false, false))
        {
            setCompleteEx();
            return;
        }
        try
        {
            if (!queryOwner())
            {
                StringBuffer s;
                toXML(&queryXGMML(), s, 2);
                GraphPrintLog("Running graph [%s] : %s", isGlobal()?"global":"local", s.str());
            }
            create(parentExtractSz, parentExtract);
        }
        catch (IException *e)
        {
            Owned<IThorException> e2 = MakeThorException(e);
            e2->setGraphId(graphId);
            e2->setAction(tea_abort);
            job.fireException(e2);
            throw;
        }
        if (localResults)
            localResults->clear();
        doExecute(parentExtractSz, parentExtract, false);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, NULL);
        exception.setown(e);
    }
    if (!queryOwner())
    {
        GraphPrintLog("Graph Done");
        StringBuffer memStr;
        getSystemTraceInfo(memStr, PerfMonStandard | PerfMonExtended);
        GraphPrintLog("%s", memStr.str());
    }
    if (exception)
        throw exception.getClear();
}

void CGraphBase::execute(size32_t _parentExtractSz, const byte *parentExtract, bool checkDependencies, bool async)
{
    parentExtractSz = _parentExtractSz;
    if (isComplete())
        return;
    if (!prepare(parentExtractSz, parentExtract, checkDependencies, async?true:false, async))
    {
        setComplete();
        return;
    }
    if (async)
        queryJob().startGraph(*this, queryJob(), parentExtractSz, parentExtract); // may block if enough running
    else
        executeSubGraph(parentExtractSz, parentExtract);
}

void CGraphBase::join()
{
    if (poolThreadHandle)
        queryJob().joinGraph(*this);
}

void CGraphBase::doExecute(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies)
{
    if (isComplete()) return;
    if (aborted) throw MakeThorException(0, "subgraph aborted");
    if (!prepare(parentExtractSz, parentExtract, checkDependencies, false, false))
    {
        setComplete();
        return;
    }
    if (aborted) throw MakeThorException(0, "subgraph aborted");
    Owned<IException> exception;
    try
    {
        if (started)
            reset();
        Owned<IThorActivityIterator> iter = getTraverseIterator();
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            element.onStart(parentExtractSz, parentExtract);
        }
        if (!preStart(parentExtractSz, parentExtract)) return;
        start();
        postStart();
        if (!wait(aborted?MEDIUMTIMEOUT:INFINITE)) // can't wait indefinetely, query may have aborted and stall, but prudent to wait a short time for underlying graphs to unwind.
            GraphPrintLogEx(this, thorlog_null, MCuserWarning, "Graph wait cancelled, aborted=%s", aborted?"true":"false");
        graphDone = true;
    }
    catch (IException *e)
    {
        GraphPrintLog(e, NULL); exception.setown(e);
    }
    try
    {
        if (exception && !queryOwner())
        {
            StringBuffer str;
            Owned<IThorException> e = MakeThorException(exception->errorCode(), "%s", exception->errorMessage(str).str());
            e->setGraphId(graphId);
            e->setAction(tea_abort);
            fireException(e);
        }
    }
    catch (IException *e)
    {
        GraphPrintLog(e, "during abort()");
        e->Release();
    }
    try
    {
        done();
        if (doneBarrier)
            doneBarrier->wait(false);
    }
    catch (IException *e)
    {
        GraphPrintLog(e, NULL);
        if (!exception.get())
            exception.setown(e);
        else
            e->Release();
    }
    end();
    if (exception)
        throw exception.getClear();
    if (!queryAborted())
        setComplete();
}

bool CGraphBase::prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async)
{
    if (isComplete()) return false;
    bool needToExecute = false;
    ifs.kill();
    ForEachItemIn(s, sinks)
    {
        CGraphElementBase &sink = sinks.item(s);
        if (sink.prepareContext(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async))
            needToExecute = true;
    }
//  prepared = true;
    return needToExecute;
}

void CGraphBase::create(size32_t parentExtractSz, const byte *parentExtract)
{
    ForEachItemIn(s, sinks)
    {
        CGraphElementBase &sink = sinks.item(s);
        sink.createActivity(parentExtractSz, parentExtract);
    }
    ForEachItemIn(i, ifs) // because sink traversal above, stops if hits act created, ifs need traversing too if newbranch created
    {
        CGraphElementBase &ifElem = ifs.item(i);
        if (ifElem.newWhichBranch)
        {
            ifElem.newWhichBranch = false;
            if (ifElem.inputs.isItem(ifElem.whichBranch))
            {
                CGraphElementBase &input = ifElem.inputs.item(ifElem.whichBranch);
                input.createActivity(parentExtractSz, parentExtract);
            }
        }
        if (ifElem.whichBranch<ifElem.inputs.ordinality())
        {
            if (NotFound == ifElem.connectedInputsIndex.find(ifElem.whichBranch))
            {
                if (ifElem.connectedInputsIndex.ordinality())
                {
                    assertex(1 == ifElem.connectedInputsIndex.ordinality());
                    ifElem.removeInput(ifElem.connectedInputsIndex.item(0));
                }
                CGraphElementBase *input = &ifElem.inputs.item(ifElem.whichBranch);
                unsigned inputOutIdx = ifElem.inputOutIndexes.item(ifElem.whichBranch);
                ifElem.setInput(ifElem.whichBranch, *input, inputOutIdx);
            }
        }
    }
    created = true;
}

void CGraphBase::done()
{
    if (aborted) return; // activity done methods only called on success
    Owned<IThorActivityIterator> iter = getTraverseIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        element.queryActivity()->done();
    }
}

void CGraphBase::end()
{
// always called, any final action clear up
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        try
        {
            if (element.queryActivity())
                element.queryActivity()->kill();
        }
        catch (IException *e)
        {
            Owned<IException> e2 = MakeActivityException(element.queryActivity(), e, "Error calling kill()");
            GraphPrintLog(e2, NULL);
            e->Release();
        }
    }
}

class CGraphTraverseIterator : public CInterface, implements IThorActivityIterator
{
protected:
    CGraphBase &graph;
    Linked<CGraphElementBase> cur;
    CIArrayOf<CGraphElementBase> others;
    CopyCIArrayOf<CGraphElementBase> covered;

    CGraphElementBase *popNext()
    {
        if (!others.ordinality())
        {
            cur.clear();
            return NULL;
        }
        cur.setown(&others.popGet());
        return cur;
    }
    CGraphElementBase *setNext(CIArrayOf<CGraphElementBase> &inputs, unsigned whichInput=((unsigned)-1))
    {
        unsigned n = inputs.ordinality();
        if (((unsigned)-1) != whichInput)
        {
            if (whichInput >= n)
            {
                if (!popNext())
                    return NULL;
            }
            else
                cur.set(&inputs.item(whichInput));
        }
        else
        {
            if (0 == n) // get next from stack
            {
                assertex(whichInput==((unsigned)-1));
                if (!popNext())
                    return NULL;
            }
            else if (1 == n)
            {
                assertex(whichInput==((unsigned)-1));
                cur.set(&inputs.item(0));
            }
            else
            {
                unsigned i=1;
                for (; i<n; i++)
                    others.append(*LINK(&inputs.item(i)));
                cur.set(&inputs.item(0));
            }
        }
        // check haven't been here before
        loop
        {
            if (cur->getOutputs() < 2)
                break;
            else if (NotFound == covered.find(*cur))
            {
                if (!cur->alreadyUpdated)
                {
                    covered.append(*cur);
                    break;
                }
            }
            if (!popNext())
                return NULL;
        }
        return cur.get();
    }
public:
    IMPLEMENT_IINTERFACE;
    CGraphTraverseIterator(CGraphBase &_graph) : graph(_graph)
    {
    }
    virtual bool first()
    {
        covered.kill();
        others.kill();
        cur.clear();
        Owned<IThorActivityIterator> sinkIter = graph.getSinkIterator();
        if (!sinkIter->first())
            return false;
        loop
        {
            cur.set(& sinkIter->query());
            if (!cur->alreadyUpdated)
                break;
            if (!sinkIter->next())
                return false;
        }
        while (sinkIter->next())
            others.append(sinkIter->get());
        return true;
    }
    virtual bool isValid() { return NULL != cur.get(); }
    virtual CGraphElementBase & query() { return *cur; }
            CGraphElementBase & get() { return *LINK(cur); }
};

class CGraphTraverseCondIterator : public CGraphTraverseIterator
{
    bool skipCond;
public:
    CGraphTraverseCondIterator(CGraphBase &graph, bool _skipCond) : CGraphTraverseIterator(graph), skipCond(_skipCond) { }
    virtual bool next()
    {
        if (skipCond)
        {
            setNext(cur->inputs);
            loop
            {
                if (!cur)
                    return false;
                switch (cur->getKind())
                {
                    case TAKif:
                    case TAKchildif:
                    case TAKcase:
                        setNext(cur->inputs, cur->whichBranch);
                        break;
                    default:
                        return (cur!=NULL);
                }
            }
        }
        else
        {
            if (cur)
            {
                switch (cur->getKind())
                {
                    case TAKif:
                    case TAKchildif:
                    case TAKcase:
                        setNext(cur->inputs, cur->whichBranch);
                        break;
                    default:
                        setNext(cur->inputs);
                        break;
                }
                if (!cur)
                    return false;
            }
        }
        return true;
    }
};

class CGraphTraverseConnectedIterator : public CGraphTraverseIterator
{
public:
    CGraphTraverseConnectedIterator(CGraphBase &graph) : CGraphTraverseIterator(graph) { }
    virtual bool next()
    {
        if (cur->isEof)
        {
            do
            {
                if (!popNext())
                    return false;
            }
            while (cur->isEof);
        }
        else
            setNext(cur->connectedInputs);
        return NULL!=cur.get();
    }
};

IThorActivityIterator *CGraphBase::getTraverseIterator(bool connected)
{
    if (connected)
        return new CGraphTraverseConnectedIterator(*this);
    else
        return new CGraphTraverseCondIterator(*this, true);
}

IThorActivityIterator *CGraphBase::getTraverseIteratorCond()
{
    return new CGraphTraverseCondIterator(*this, false);
}

bool CGraphBase::wait(unsigned timeout)
{
    CTimeMon tm(timeout);
    unsigned remaining = timeout;
    class CWaitException
    {
        Owned<IException> exception;
    public:
        IException *get() { return exception; }
        void set(IException *e) { if (!exception) exception.setown(e); }
        void throwException()
        {
            if (exception)
                throw exception.getClear();
            throw MakeThorException(0, "Timed out waiting for graph to end");
        }
    } waitException;
    Owned<IThorActivityIterator> iter = getTraverseIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        CActivityBase *activity = element.queryActivity();
        if (INFINITE != timeout && tm.timedout(&remaining))
            waitException.throwException();
        try
        {
            if (!activity->wait(remaining))
                waitException.throwException();
        }
        catch (IException *e)
        {
            waitException.set(e);
            if (timeout == INFINITE)
            {
                unsigned e = tm.elapsed();
                if (e >= MEDIUMTIMEOUT)
                    waitException.throwException();
                timeout = MEDIUMTIMEOUT-e;
                tm.reset(timeout);
            }
        }
    }
    if (waitException.get())
        waitException.throwException();
    // synchronize all slaves to end of graphs
    if (NULL == owner || isGlobal())
    {
        if (INFINITE != timeout && tm.timedout(&remaining))
            waitException.throwException();
        if (!waitBarrier->wait(false, remaining))
            return false;
    }
    return true;
}

void CGraphBase::abort(IException *e)
{
    if (aborted) return;
    crit.enter();
    abortException.set(e);
    aborted = true;
    job.queryJobComm().cancel(0, mpTag);

    if (0 == containers.count())
    {
        Owned<IThorGraphIterator> iter = getChildGraphs();
        ForEach(*iter)
        {
            CGraphBase &graph = iter->query();
            graph.abort(e);
        }
    }
    if (started)
    {
        crit.leave();
        Owned<IThorActivityIterator> iter = getTraverseIterator();
        ForEach (*iter)
        {
            iter->query().abort(e); // JCSMORE - could do in parallel, they can take some time to timeout
        }
        if (startBarrier)
            startBarrier->cancel();
        if (waitBarrier)
            waitBarrier->cancel();
        if (doneBarrier)
            doneBarrier->cancel();
    }
    else
        crit.leave();
}

void CGraphBase::GraphPrintLog(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::GraphPrintLogArgs(this, thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void CGraphBase::GraphPrintLog(IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::GraphPrintLogArgs(this, e, thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void CGraphBase::createFromXGMML(IPropertyTree *_node, CGraphBase *_owner, CGraphBase *_parent, CGraphBase *resultsGraph)
{
    owner = _owner;
    parent = _parent?_parent:owner;
    node.setown(createPTreeFromIPT(_node));
    xgmml = node->queryPropTree("att/graph");
    sink = xgmml->getPropBool("att[@name=\"rootGraph\"]/@value", false);
    graphId = node->getPropInt("@id");
    global = false;
    parentActivityId = node->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);

    CGraphBase *graphContainer = this;
    if (resultsGraph)
        graphContainer = resultsGraph; // JCSMORE is this right?
    graphCodeContext.setContext(graphContainer, (ICodeContextExt *)&job.queryCodeContext());


    unsigned numResults = xgmml->getPropInt("att[@name=\"_numResults\"]/@value", 0);
    if (numResults)
    {
        localResults.setown(createThorGraphResults(numResults));
        resultsGraph = this;
        tmpHandler.setown(queryJob().createTempHandler());
    }
    
    Owned<IPropertyTreeIterator> nodes = xgmml->getElements("node");
    ForEach(*nodes)
    {
        IPropertyTree &e = nodes->query();
        ThorActivityKind kind = (ThorActivityKind) e.getPropInt("att[@name=\"_kind\"]/@value");
        if (TAKsubgraph == kind)
        {
            Owned<CGraphBase> subGraph = job.createGraph();
            subGraph->createFromXGMML(&e, this, parent, resultsGraph);
            addChildGraph(*LINK(subGraph));
            if (!global)
                global = subGraph->isGlobal();
        }
        else
        {
            if (owner && parentActivityId)
            {
                CGraphElementBase *parentElement = owner->queryElement(parentActivityId);
                parentElement->addAssociatedChildGraph(this);
                switch (parentElement->getKind())
                {
                    case TAKlooprow:
                    case TAKloopcount:
                    case TAKloopdataset:
                    case TAKgraphloop:
                    case TAKparallelgraphloop:
                        break;
                    default:
                        // not a loop graph, force it to be local child graph
                        if (!e.getPropBool("att[@name=\"coLocal\"]/@value", false))
                        {
                            IPropertyTree *att = createPTree("att");
                            att->setProp("@name", "coLocal");
                            att->setPropBool("@value", true);
                            e.addPropTree("att", att);
                        }
                        break;
                }
            }
            CGraphElementBase *act = createGraphElement(e, *this, resultsGraph);
            addActivity(act);
            if (!global)
                global = isGlobalActivity(*act);
        }
    }
    Owned<IPropertyTreeIterator> edges = xgmml->getElements("edge");
    ForEach(*edges)
    {
        IPropertyTree &edge = edges->query();
        unsigned sourceOutput = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
        unsigned targetInput = edge.getPropInt("att[@name=\"_targetIndex\"]/@value", 0);
        CGraphElementBase *source = queryElement(edge.getPropInt("@source"));
        CGraphElementBase *target = queryElement(edge.getPropInt("@target"));
        target->addInput(source, sourceOutput);
        source->addOutput(target, targetInput);
    }
    init();
}

void CGraphBase::executeChildGraphs(size32_t parentExtractSz, const byte *parentExtract)
{
    Owned<IThorGraphIterator> iter = getChildGraphs();
    ForEach(*iter)
    {
        CGraphBase &graph = iter->query();
        if (graph.isSink())
            graph.execute(parentExtractSz, parentExtract, true, false);
    }
}

void CGraphBase::doExecuteChild(size32_t parentExtractSz, const byte *parentExtract)
{
    reset();
    if (0 == containers.count())
        executeChildGraphs(parentExtractSz, parentExtract);
    else
        execute(parentExtractSz, parentExtract, false, false);
    queryTempHandler()->clearTemps();
}

void CGraphBase::executeChild(size32_t & retSize, void * &ret, size32_t parentExtractSz, const byte *parentExtract)
{
    reset();
    doExecute(parentExtractSz, parentExtract, false);

    UNIMPLEMENTED;

/*
    ForEachItemIn(idx1, elements)
    {
        EclGraphElement & cur = elements.item(idx1);
        if (cur.isResult)
        {
            cur.extractResult(retSize, ret);
            return;
        }
    }
*/
    throwUnexpected();
}

void CGraphBase::setResults(IThorGraphResults *results) // used by master only
{
    localResults.set(results);
}

void CGraphBase::executeChild(size32_t parentExtractSz, const byte *parentExtract, IThorGraphResults *results, IThorGraphResults *_graphLoopResults)
{
    localResults.set(results);
    graphLoopResults.set(_graphLoopResults);
    doExecuteChild(parentExtractSz, parentExtract);
    graphLoopResults.clear();
    localResults.clear();
}

StringBuffer &getGlobals(CGraphBase &graph, StringBuffer &str)
{
    bool first = true;
    Owned<IThorActivityIterator> iter = graph.getIterator();
    ForEach(*iter)
    {
        CGraphElementBase &e = iter->query();
        if (isGlobalActivity(e))
        {
            if (first)
                str.append("Graph(").append(graph.queryGraphId()).append("): [");
            else
                str.append(", ");
            first = false;

            ThorActivityKind kind = e.getKind();
            str.append(activityKindStr(kind));
            str.append("(").append(kind).append(")");
        }
    }
    if (!first)
        str.append("]");
    Owned<IThorGraphIterator> childIter = graph.getChildGraphs();
    ForEach(*childIter)
    {
        CGraphBase &childGraph = childIter->query();
        getGlobals(childGraph, str);
    }
    return str;
}

void CGraphBase::executeChild(size32_t parentExtractSz, const byte *parentExtract)
{
    assertex(localResults);
    localResults->clear();
    if (isGlobal()) // any slave
    {
        StringBuffer str("Global acts = ");
        getGlobals(*this, str);
        throw MakeThorException(0, "Global child graph? : %s", str.str());
    }
    doExecuteChild(parentExtractSz, parentExtract);
}

IThorResult *CGraphBase::queryResult(unsigned id)
{
    return localResults->queryResult(id);
}

IThorResult *CGraphBase::getResult(unsigned id)
{
    return localResults->getResult(id);
}

IThorResult *CGraphBase::queryGraphLoopResult(unsigned id)
{
    return graphLoopResults->queryResult(id);
}

IThorResult *CGraphBase::getGraphLoopResult(unsigned id)
{
    return graphLoopResults->getResult(id);
}

IThorResult *CGraphBase::createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool local)
{
    return localResults->createResult(activity, id, rowIf, local);
}

IThorResult *CGraphBase::createGraphLoopResult(CActivityBase &activity, IRowInterfaces *rowIf, bool local)
{
    return graphLoopResults->createResult(activity, rowIf, local);
}

// ILocalGraph
void CGraphBase::getResult(size32_t & len, void * & data, unsigned id)
{
    Owned<IThorResult> result = localResults->getResult(id);
    result->getResult(len, data);
}

void CGraphBase::getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
{
    Owned<IThorResult> result = localResults->getResult(id);
    result->getLinkedResult(count, ret);
}

// IThorChildGraph impl.
IEclGraphResults *CGraphBase::evaluate(unsigned parentExtractSz, const byte *parentExtract)
{
    CriticalBlock block(evaluateCrit);

    parentExtractSz = parentExtractSz;
    executeChild(parentExtractSz, parentExtract);
    //GH->JCS This needs to set up new results rather than returning a link, so helpers are thread safe
    return LINK(localResults);
}

static bool isLocalOnly(CGraphElementBase &activity);
static bool isLocalOnly(CGraphBase &graph) // checks all dependencies, if something needs to be global, whole body is forced to be execution sync.
{
    if (0 == graph.activityCount())
    {
        Owned<IThorGraphIterator> iter = graph.getChildGraphs();
        ForEach(*iter)
        {
            CGraphBase &childGraph = iter->query();
            if (childGraph.isSink())
            {
                if (!isLocalOnly(childGraph))
                    return false;
            }
        }
    }
    else
    {
        if (graph.isGlobal())
            return false;
        Owned<IThorActivityIterator> sinkIter = graph.getSinkIterator();
        ForEach(*sinkIter)
        {
            CGraphElementBase &sink = sinkIter->query();
            if (!isLocalOnly(sink))
                return false;
        }
    }
    return true;
}

static bool isLocalOnly(CGraphElementBase &activity)
{
    Owned<IThorGraphDependencyIterator> deps = activity.getDependsIterator();
    ForEach(*deps)
    {
        if (!isLocalOnly(*(deps->query().graph)))
            return false;
    }
    StringBuffer match("edge[@target=\"");
    match.append(activity.queryId()).append("\"]");
    Owned<IPropertyTreeIterator> inputs = activity.queryOwner().queryXGMML().getElements(match.str());
    ForEach(*inputs)
    {
        CGraphElementBase *sourceAct = activity.queryOwner().queryElement(inputs->query().getPropInt("@source"));
        if (!isLocalOnly(*sourceAct))
            return false;
    }
    return true;
}

bool CGraphBase::isLocalOnly() // checks all dependencies, if something needs to be global, whole body is forced to be execution sync.
{
    return ::isLocalOnly(*this);
}

////

void CGraphTempHandler::registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind, StringArray *clusters)
{
    assertex(temp);
    LOG(MCdebugProgress, thorJob, "registerTmpFile name=%s, usageCount=%d", name, usageCount);
    if (tmpFiles.find(name))
        throw MakeThorException(TE_FileAlreadyUsedAsTempFile, "File already used as temp file (%s)", name);
    tmpFiles.replace(* new CFileUsageEntry(name, graphId, fileKind, usageCount));
}

void CGraphTempHandler::deregisterFile(const char *name, bool kept)
{
    LOG(MCdebugProgress, thorJob, "deregisterTmpFile name=%s", name);
    CFileUsageEntry *fileUsage = tmpFiles.find(name);
    if (!fileUsage)
        throw MakeThorException(TE_FileNotFound, "File not found (%s) deregistering tmp file", name);
    if (0 == fileUsage->queryUsage()) // marked 'not to be deleted' until workunit complete.
        return;
    else if (1 == fileUsage->queryUsage())
    {
        tmpFiles.remove(name);
        try
        {
            if (!removeTemp(name))
                LOG(MCwarning, unknownJob, "Failed to delete tmp file : %s (not found)", name);
        }
        catch (IException *e) { StringBuffer s("Failed to delete tmp file : "); FLLOG(MCwarning, thorJob, e, s.append(name).str()); }
    }
    else
        fileUsage->decUsage();
}

void CGraphTempHandler::clearTemps()
{
    Owned<IFileUsageIterator> iter = getIterator();
    ForEach(*iter)
    {
        CFileUsageEntry &entry = iter->query();
        const char *tmpname = entry.queryName();
        try
        {
            if (!removeTemp(tmpname))
                LOG(MCwarning, thorJob, "Failed to delete tmp file : %s (not found)", tmpname);
        }
        catch (IException *e) { StringBuffer s("Failed to delete tmp file : "); FLLOG(MCwarning, thorJob, e, s.append(tmpname).str()); }
    }
    iter.clear();
    tmpFiles.kill();
}

/////

class CGraphExecutor;
class CGraphExecutorGraphInfo : public CInterface
{
public:
    CGraphExecutorGraphInfo(CGraphExecutor &_executor, CGraphBase *_subGraph, IGraphCallback &_callback, const byte *parentExtract, size32_t parentExtractSz) : executor(_executor), subGraph(_subGraph), callback(_callback)
    {
        parentExtractMb.append(parentExtractSz, parentExtract);
    }
    CGraphExecutor &executor;
    IGraphCallback &callback;
    Linked<CGraphBase> subGraph;
    MemoryBuffer parentExtractMb;
};
class CGraphExecutor : public CInterface, implements IGraphExecutor
{
    CJobBase &job;
    CIArrayOf<CGraphExecutorGraphInfo> stack, running, toRun;
    UnsignedArray seen;
    bool stopped;
    unsigned limit;
    CriticalSection crit;
    Semaphore sem;
    Owned<IThreadPool> graphPool;

    class CGraphExecutorFactory : public CInterface, implements IThreadFactory
    {
        CGraphExecutor &executor;
    public:
        IMPLEMENT_IINTERFACE;

        CGraphExecutorFactory(CGraphExecutor &_executor) : executor(_executor) { }
// IThreadFactory
        virtual IPooledThread *createNew()
        {
            class CGraphExecutorThread : public CInterface, implements IPooledThread
            {
                Owned<CGraphExecutorGraphInfo> graphInfo;
            public:
                IMPLEMENT_IINTERFACE;
                CGraphExecutorThread()
                {
                }
                void init(void *startInfo)
                {
                    graphInfo.setown((CGraphExecutorGraphInfo *)startInfo);
                }
                void main()
                {
                    Linked<CGraphBase> graph = graphInfo->subGraph;
                    Owned<IException> e;
                    try
                    {
                        graphInfo->callback.runSubgraph(*graph, graphInfo->parentExtractMb.length(), (const byte *)graphInfo->parentExtractMb.toByteArray());
                    }
                    catch (IException *_e)
                    {
                        e.setown(_e);
                    }
                    try { graphInfo->executor.graphDone(*graphInfo, e); }
                    catch (IException *e)
                    {
                        GraphPrintLog(graph, e, "graphDone");
                        e->Release();
                    }
                    if (e)
                        throw e.getClear();
                }
                bool canReuse() { return true; }
                bool stop() { return true; }
            };
            return new CGraphExecutorThread();
        }
    } *factory;
public:
    IMPLEMENT_IINTERFACE;

    CGraphExecutor(CJobBase &_job) : job(_job)
    {
        limit = (unsigned)job.getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
        stopped = false;
        factory = new CGraphExecutorFactory(*this);
        graphPool.setown(createThreadPool("CGraphExecutor pool", factory, &job, limit));
    }
    ~CGraphExecutor()
    {
        stopped = true;
        sem.signal();
        factory->Release();
    }
    void graphDone(CGraphExecutorGraphInfo &doneGraphInfo, IException *e)
    {
        CriticalBlock b(crit);
        running.zap(doneGraphInfo);

        if (e || job.queryAborted())
        {
            stopped = true;
            stack.kill();
            sem.signal();
            return;
        }
        if (job.queryPausing())
            stack.kill();
        else if (stack.ordinality())
        {
            CopyCIArrayOf<CGraphExecutorGraphInfo> toMove;
            ForEachItemIn(s, stack)
            {
                bool dependenciesDone = true;
                CGraphExecutorGraphInfo &graphInfo = stack.item(s);
                ForEachItemIn (d, graphInfo.subGraph->dependentSubGraphs)
                {
                    CGraphBase &subGraph = graphInfo.subGraph->dependentSubGraphs.item(d);
                    if (!subGraph.isComplete())
                    {
                        dependenciesDone = false;
                        break;
                    }
                }
                if (dependenciesDone)
                {
                    graphInfo.subGraph->dependentSubGraphs.kill();
                    graphInfo.subGraph->prepare(graphInfo.parentExtractMb.length(), (const byte *)graphInfo.parentExtractMb.toByteArray(), true, true, true); // now existing deps done, maybe more to prepare
                    ForEachItemIn (d, graphInfo.subGraph->dependentSubGraphs)
                    {
                        CGraphBase &subGraph = graphInfo.subGraph->dependentSubGraphs.item(d);
                        if (!subGraph.isComplete())
                        {
                            dependenciesDone = false;
                            break;
                        }
                    }
                    if (dependenciesDone)
                    {
                        graphInfo.subGraph->dependentSubGraphs.kill(); // none to track anymore
                        toMove.append(graphInfo);
                    }
                }
            }
            ForEachItemIn(m, toMove)
            {
                Linked<CGraphExecutorGraphInfo> graphInfo = &toMove.item(m);
                stack.zap(*graphInfo);
                toRun.add(*graphInfo.getClear(), 0);
            }
        }
        job.markWuDirty();
        PROGLOG("CGraphExecutor running=%d, waitingToRun=%d, dependentsWaiting=%d", running.ordinality(), toRun.ordinality(), stack.ordinality());
        sem.signal();
    }
// IGraphExecutor
    virtual void add(CGraphBase *subGraph, IGraphCallback &callback, size32_t parentExtractSz, const byte *parentExtract)
    {
        CriticalBlock b(crit);
        if (job.queryPausing())
            return;
        if (subGraph->isComplete())
            return;
        ForEachItemIn(s, seen)
        {
            if (subGraph->queryGraphId() == seen.item(s))
                return; // already queued;
        }
        seen.append(subGraph->queryGraphId());
        if (subGraph->dependentSubGraphs.ordinality())
        {
            bool dependenciesDone = true;
            ForEachItemIn (d, subGraph->dependentSubGraphs)
            {
                CGraphBase &graph = subGraph->dependentSubGraphs.item(d);
                if (!graph.isComplete())
                {
                    dependenciesDone = false;
                    break;
                }
            }
            if (dependenciesDone)
                subGraph->dependentSubGraphs.kill(); // none to track anymore
        }
        Owned<CGraphExecutorGraphInfo> graphInfo = new CGraphExecutorGraphInfo(*this, subGraph, callback, parentExtract, parentExtractSz);
        if (0 == subGraph->dependentSubGraphs.ordinality())
        {
            if (running.ordinality()<limit)
            {
                running.append(*LINK(graphInfo));
                PROGLOG("Add: Launching graph thread for graphId=%"GIDPF"d", subGraph->queryGraphId());
                PooledThreadHandle h = graphPool->start(graphInfo.getClear());
                subGraph->poolThreadHandle = h;
            }
            else
                stack.add(*graphInfo.getClear(), 0); // push to front, no dependency, free to run next.
        }
        else
            stack.append(*graphInfo.getClear()); // as dependencies finish, may move up the list
    }
    virtual IThreadPool &queryGraphPool() { return *graphPool; }

    virtual void wait()
    {
        if (0 == seen.ordinality())
            return;
        loop
        {
            if (sem.wait(MEDIUMTIMEOUT))
            {
                CriticalBlock b(crit);
                if (stopped || job.queryAborted() || job.queryPausing()) break;
                if (0 == stack.ordinality() && 0 == toRun.ordinality())
                {
                    if (running.ordinality())
                        continue;
                    break;
                }
                if (job.queryPausing()) return; // pending graphs will re-run on resubmission
                assertex(running.ordinality() <= limit);
                bool added = false;
                while (toRun.ordinality())
                {
                    Linked<CGraphExecutorGraphInfo> graphInfo = &toRun.item(0);
                    toRun.remove(0);
                    running.append(*LINK(graphInfo));
                    CGraphBase *subGraph = graphInfo->subGraph;
                    PROGLOG("Wait: Launching graph thread for graphId=%"GIDPF"d", subGraph->queryGraphId());
                    added = true;
                    PooledThreadHandle h = graphPool->start(graphInfo.getClear());
                    subGraph->poolThreadHandle = h;
                    if (running.ordinality() >= limit)
                        break;
                }
                if (!added)
                    Sleep(1000); // still more to come
            }
            else
                PROGLOG("Waiting on executing graphs to complete.");
            StringBuffer str("Currently running graphId = ");

            CriticalBlock b(crit);
            {
                if (running.ordinality())
                {
                    ForEachItemIn(r, running)
                    {
                        CGraphExecutorGraphInfo &graphInfo = running.item(r);
                        str.append(graphInfo.subGraph->queryGraphId());
                        if (r != running.ordinality()-1)
                            str.append(", ");
                    }
                    PROGLOG("%s", str.str());
                }
                if (stack.ordinality())
                {
                    str.clear().append("Queued in stack graphId = ");
                    ForEachItemIn(s, stack)
                    {
                        CGraphExecutorGraphInfo &graphInfo = stack.item(s);
                        str.append(graphInfo.subGraph->queryGraphId());
                        if (s != stack.ordinality()-1)
                            str.append(", ");
                    }
                    PROGLOG("%s", str.str());
                }
            }
        }
    }
};

////
CJobBase::CJobBase(const char *_graphName) : graphName(_graphName)
{
    maxDiskUsage = diskUsage = 0;
    dirty = true;
    aborted = false;
    codeCtx = NULL;
    mpJobTag = TAG_NULL;
    timeReporter = createStdTimeReporter();
    pluginMap = new SafePluginMap(&pluginCtx, true);

// JCSMORE - Will pass down at job creation time...
    jobGroup.set(&::queryClusterGroup());
    jobComm.setown(createCommunicator(jobGroup));
    slaveGroup.setown(jobGroup->remove(0));
    myrank = jobGroup->rank(queryMyNode());
}

void CJobBase::init()
{
    StringBuffer tmp;
    tmp.append(wuid);
    tmp.append(graphName);
    key.set(tmp.str());

    SCMStringBuffer tokenUser, password;
    extractToken(token.str(), wuid.str(), tokenUser, password);
    userDesc = createUserDescriptor();
    userDesc->set(user.str(), password.str());

    // global setting default on, can be overridden by #option
    timeActivities = 0 != getWorkUnitValueInt("timeActivities", globals->getPropBool("@timeActivities", true));
    pausing = false;
    resumed = false;
    setLCRrowCRCchecking(0 != getWorkUnitValueInt("THOR_ROWCRC", globals->getPropBool("@THOR_ROWCRC", 
#ifdef _DEBUG
        true
#else
        false
#endif
        )));

    graphExecutor.setown(new CGraphExecutor(*this));
}

CJobBase::~CJobBase()
{
    clean();
    PROGLOG("CJobBase resetting memory manager");
    resetThorMemoryManager();
    ::Release(codeCtx);
    ::Release(userDesc);
    timeReporter->Release();
    delete pluginMap;
}

void CJobBase::clean()
{
    if (graphExecutor)
    {
        graphExecutor->queryGraphPool().stopAll();
        graphExecutor.clear();
    }
    subGraphs.kill();
}

IThorGraphIterator *CJobBase::getSubGraphs()
{
    CriticalBlock b(crit);
    return new CSubGraphIterator(subGraphs);
}

static void getGlobalDeps(CGraphBase &graph, CopyCIArrayOf<CGraphDependency> &deps)
{
    Owned<IThorActivityIterator> iter = graph.getIterator();
    ForEach(*iter)
    {
        CGraphElementBase &elem = iter->query();
        Owned<IThorGraphDependencyIterator> dependIterator = elem.getDependsIterator();
        ForEach(*dependIterator)
        {
            CGraphDependency &dependency = dependIterator->query();
            if (dependency.graph->isGlobal() && NULL==dependency.graph->queryOwner())
                deps.append(dependency);
            getGlobalDeps(*dependency.graph, deps);
        }
    }
}

void CJobBase::addDependencies(IPropertyTree *xgmml, bool failIfMissing)
{
    CopyCIArrayOf<CGraphBase> childGraphs;
    CopyCIArrayOf<CGraphElementBase> targetActivities;

    Owned<IPropertyTreeIterator> iter = xgmml->getElements("edge");
    ForEach(*iter)
    {
        IPropertyTree &edge = iter->query();
        Owned<CGraphBase> source = getGraph(edge.getPropInt("@source"));
        Owned<CGraphBase> target = getGraph(edge.getPropInt("@target"));
        if (!source || !target)
        {
            if (failIfMissing)
                throwUnexpected();
            else
                continue; // expected if assigning dependencies in slaves
        }
        CGraphElementBase *targetActivity = (CGraphElementBase *)target->queryElement(edge.getPropInt("att[@name=\"_targetActivity\"]/@value"));
        CGraphElementBase *sourceActivity = (CGraphElementBase *)source->queryElement(edge.getPropInt("att[@name=\"_sourceActivity\"]/@value"));
        int controlId = 0;
        if (edge.getPropBool("att[@name=\"_dependsOn\"]/@value", false))
        {
            if (!edge.getPropBool("att[@name=\"_childGraph\"]/@value", false)) // JCSMORE - not sure if necess. roxie seem to do.
                controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
            CGraphBase &sourceGraph = sourceActivity->queryOwner();
            unsigned sourceGraphContext = sourceGraph.queryParentActivityId();

            CGraphBase *targetGraph = NULL;
            unsigned targetGraphContext = -1;
            loop
            {
                targetGraph = &targetActivity->queryOwner();
                targetGraphContext = targetGraph->queryParentActivityId();
                if (sourceGraphContext == targetGraphContext) 
                    break;
                targetActivity = targetGraph->queryElement(targetGraphContext);
            }
            assertex(targetActivity && sourceActivity);
            targetActivity->addDependsOn(source, controlId);
        }
        else if (edge.getPropBool("att[@name=\"_conditionSource\"]/@value", false))
            { /* Ignore it */ }
        else if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
        {
            // NB: any dependencies of the child acts. are dependencies of this act.
            childGraphs.append(*source);
            targetActivities.append(*targetActivity);
        }
        else
        {
            if (!edge.getPropBool("att[@name=\"_childGraph\"]/@value", false)) // JCSMORE - not sure if necess. roxie seem to do.
                controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
            targetActivity->addDependsOn(source, controlId);
        }
    }
    ForEachItemIn(c, childGraphs)
    {
        CGraphBase &childGraph = childGraphs.item(c);
        CGraphElementBase &targetActivity = targetActivities.item(c);
        if (!childGraph.isGlobal())
        {
            CopyCIArrayOf<CGraphDependency> globalChildGraphDeps;
            getGlobalDeps(childGraph, globalChildGraphDeps);
            ForEachItemIn(gcd, globalChildGraphDeps)
            {
                CGraphDependency &globalDep = globalChildGraphDeps.item(gcd);
                targetActivity.addDependsOn(globalDep.graph, globalDep.controlId);
            }
        }
    }
}

void CJobBase::startGraph(CGraphBase &graph, IGraphCallback &callback, size32_t parentExtractSize, const byte *parentExtract)
{
    graphExecutor->add(&graph, callback, parentExtractSize, parentExtract);
}

void CJobBase::joinGraph(CGraphBase &graph)
{
    if (graph.poolThreadHandle)
        graphExecutor->queryGraphPool().join(graph.poolThreadHandle);
}

ICodeContext &CJobBase::queryCodeContext() const
{
    return *codeCtx;
}

bool CJobBase::queryUseCheckpoints() const
{
    return globals->getPropBool("@checkPointRecovery") || 0 != getWorkUnitValueInt("checkPointRecovery", 0);
}

void CJobBase::abort(IException *e)
{
    aborted = true;
    Owned<IThorGraphIterator> iter = getSubGraphs();
    ForEach (*iter)
    {
        CGraphBase &graph = iter->query();
        graph.abort(e);
    }
}

void CJobBase::increase(offset_t usage, const char *key)
{
    diskUsage += usage;
    if (diskUsage > maxDiskUsage) maxDiskUsage = diskUsage;
}

void CJobBase::decrease(offset_t usage, const char *key)
{
    diskUsage -= usage;
}

mptag_t CJobBase::allocateMPTag()
{
    mptag_t tag = allocateClusterMPTag();
    jobComm->flush(tag);
    PROGLOG("allocateMPTag: tag = %d", (int)tag);
    return tag;
}

void CJobBase::freeMPTag(mptag_t tag)
{
    if (TAG_NULL != tag)
    {
        freeClusterMPTag(tag);
        PROGLOG("freeMPTag: tag = %d", (int)tag);
        jobComm->flush(tag);
    }
}

mptag_t CJobBase::deserializeMPTag(MemoryBuffer &mb)
{
    mptag_t tag;
    deserializeMPtag(mb, tag);
    if (TAG_NULL != tag)
    {
        PROGLOG("deserializeMPTag: tag = %d", (int)tag);
        jobComm->flush(tag);
    }
    return tag;
}

// IGraphCallback
void CJobBase::runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract)
{
    graph.executeSubGraph(parentExtractSz, parentExtract);
}

static IThorResource *iThorResource = NULL;
void setIThorResource(IThorResource &r)
{
    iThorResource = &r;
}

IThorResource &queryThor()
{
    return *iThorResource;
}

//
//

//
//

CActivityBase::CActivityBase(CGraphElementBase *_container) : container(*_container), timeActivities(_container->queryJob().queryTimeActivities())
{
    mpTag = TAG_NULL;
    abortSoon = actStarted = cancelledReceive = false;
    baseHelper.set(container.queryHelper());
    parentExtractSz = 0;
    parentExtract = NULL;
}

CActivityBase::~CActivityBase()
{
}

void CActivityBase::abort()
{
    if (!abortSoon) ActPrintLog("Abort condition set");
    abortSoon = true;
}

void CActivityBase::ActPrintLog(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(&queryContainer(), thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void CActivityBase::ActPrintLog(IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(&queryContainer(), e, thorlog_all, MCexception(e), format, args);
    va_end(args);
}

bool CActivityBase::fireException(IException *e)
{
    IThorException *te = QUERYINTERFACE(e, IThorException);
    StringBuffer s;
    Owned<IException> e2;
    if (!te)
    {
        IThorException *_e = MakeActivityException(this, e);
        _e->setAudience(e->errorAudience());
        e2.setown(_e);
    }
    else
    {
        if (!te->queryActivityId())
            setExceptionActivityInfo(container, te);
        e2.set(e);
    }
    return container.queryOwner().fireException(e2);
}


IEngineRowAllocator * CActivityBase::queryRowAllocator()
{
    if (CABallocatorlock.lock()) {
        if (!rowAllocator)
            rowAllocator.setown(createThorRowAllocator(queryRowMetaData(),queryActivityId()));
        CABallocatorlock.unlock();
    }
    return rowAllocator;
}
    
IOutputRowSerializer * CActivityBase::queryRowSerializer()
{
    if (CABserializerlock.lock()) {
        if (!rowSerializer)
            rowSerializer.setown(queryRowMetaData()->createRowSerializer(queryCodeContext(),queryActivityId()));
        CABserializerlock.unlock();
    }
    return rowSerializer;
}

IOutputRowDeserializer * CActivityBase::queryRowDeserializer()
{
    if (CABdeserializerlock.lock()) {
        if (!rowDeserializer)
            rowDeserializer.setown(queryRowMetaData()->createRowDeserializer(queryCodeContext(),queryActivityId()));
        CABdeserializerlock.unlock();
    }
    return rowDeserializer;
}

bool CActivityBase::receiveMsg(CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender, unsigned timeout)
{
    BooleanOnOff onOff(receiving);
    if (cancelledReceive)
        return false;
    return container.queryJob().queryJobComm().recv(mb, rank, mpTag, sender, timeout);
}

void CActivityBase::cancelReceiveMsg(const rank_t rank, const mptag_t mpTag)
{
    cancelledReceive = true;
    if (receiving)
        container.queryJob().queryJobComm().cancel(rank, mpTag);
}

