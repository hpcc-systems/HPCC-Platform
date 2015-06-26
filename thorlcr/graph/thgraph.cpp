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

#include "thgraph.hpp"
#include "jptree.hpp"
#include "commonext.hpp"
#include "dasess.hpp"
#include "jhtree.hpp"
#include "thcodectx.hpp"
#include "thbuf.hpp"
#include "thormisc.hpp"
#include "thbufdef.hpp"
#include "thmem.hpp"


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
    CActivityBase &activity;
    rowcount_t rowStreamCount;
    IOutputMetaData *meta;
    Owned<IRowWriterMultiReader> rowBuffer;
    IRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    bool stopped, readers, distributed;

    void init()
    {
        stopped = readers = false;
        allocator = rowIf->queryRowAllocator();
        meta = allocator->queryOutputMeta();
        rowStreamCount = 0;
    }
    class CStreamWriter : public CSimpleInterface, implements IRowWriterMultiReader
    {
        CThorGraphResult &owner;
        CThorExpandingRowArray rows;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStreamWriter(CThorGraphResult &_owner) : owner(_owner), rows(owner.activity, owner.rowIf, true)
        {
        }

    //IRowWriterMultiReader
        virtual void putRow(const void *row)
        {
            rows.append(row);
        }
        virtual void flush() { }
        virtual IRowStream *getReader()
        {
            return rows.createRowStream(0, (rowidx_t)-1, false);
        }
    };
public:
    IMPLEMENT_IINTERFACE;

    CThorGraphResult(CActivityBase &_activity, IRowInterfaces *_rowIf, bool _distributed, unsigned spillPriority) : activity(_activity), rowIf(_rowIf), distributed(_distributed)
    {
        init();
        if (SPILL_PRIORITY_DISABLE == spillPriority)
            rowBuffer.setown(new CStreamWriter(*this));
        else
            rowBuffer.setown(createOverflowableBuffer(activity, rowIf, true, true));
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
    virtual IRowInterfaces *queryRowInterfaces() { return rowIf; }
    virtual CActivityBase *queryActivity() { return &activity; }
    virtual bool isDistributed() const { return distributed; }
    virtual void serialize(MemoryBuffer &mb)
    {
        Owned<IRowStream> stream = getRowStream();
        bool grouped = meta->isGrouped();
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
    }
    virtual void getResult(size32_t &len, void * & data)
    {
        MemoryBuffer mb;
        serialize(mb);
        len = mb.length();
        data = mb.detach();
    }
    virtual void getLinkedResult(unsigned &countResult, byte * * & result)
    {
        assertex(rowStreamCount==((unsigned)rowStreamCount)); // catch, just in case
        Owned<IRowStream> stream = getRowStream();
        countResult = 0;
        OwnedConstThorRow _rowset = allocator->createRowset((unsigned)rowStreamCount);
        const void **rowset = (const void **)_rowset.get();
        while (countResult < rowStreamCount)
        {
            OwnedConstThorRow row = stream->nextRow();
            rowset[countResult++] = row.getClear();
        }
        result = (byte **)_rowset.getClear();
    }
    virtual const void * getLinkedRowResult()
    {
        assertex(rowStreamCount==1); // catch, just in case
        Owned<IRowStream> stream = getRowStream();
        return stream->nextRow();
    }
};

/////

IThorResult *CThorGraphResults::createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    Owned<IThorResult> result = ::createResult(activity, rowIf, distributed, spillPriority);
    setResult(id, result);
    return result;
}

/////

IThorResult *createResult(CActivityBase &activity, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    return new CThorGraphResult(activity, rowIf, distributed, spillPriority);
}

/////
class CThorBoundLoopGraph : public CInterface, implements IThorBoundLoopGraph
{
    CGraphBase *graph;
    activity_id activityId;
    Linked<IOutputMetaData> resultMeta;
    Owned<IOutputMetaData> counterMeta, loopAgainMeta;
    Owned<IRowInterfaces> resultRowIf, countRowIf, loopAgainRowIf;

public:
    IMPLEMENT_IINTERFACE;

    CThorBoundLoopGraph(CGraphBase *_graph, IOutputMetaData * _resultMeta, unsigned _activityId) : graph(_graph), resultMeta(_resultMeta), activityId(_activityId)
    {
        counterMeta.setown(createFixedSizeMetaData(sizeof(thor_loop_counter_t)));
        loopAgainMeta.setown(createFixedSizeMetaData(sizeof(bool)));
    }
    virtual void prepareCounterResult(CActivityBase &activity, IThorGraphResults *results, unsigned loopCounter, unsigned pos)
    {
        if (!countRowIf)
            countRowIf.setown(createRowInterfaces(counterMeta, activityId, activity.queryCodeContext()));
        RtlDynamicRowBuilder counterRow(countRowIf->queryRowAllocator());
        thor_loop_counter_t * res = (thor_loop_counter_t *)counterRow.ensureCapacity(sizeof(thor_loop_counter_t),NULL);
        *res = loopCounter;
        OwnedConstThorRow counterRowFinal = counterRow.finalizeRowClear(sizeof(thor_loop_counter_t));
        IThorResult *counterResult = results->createResult(activity, pos, countRowIf, false, SPILL_PRIORITY_DISABLE);
        Owned<IRowWriter> counterResultWriter = counterResult->getWriter();
        counterResultWriter->putRow(counterRowFinal.getClear());
    }
    virtual void prepareLoopAgainResult(CActivityBase &activity, IThorGraphResults *results, unsigned pos)
    {
        if (!loopAgainRowIf)
            loopAgainRowIf.setown(createRowInterfaces(loopAgainMeta, activityId, activity.queryCodeContext()));
        activity.queryGraph().createResult(activity, pos, results, loopAgainRowIf, !activity.queryGraph().isLocalChild(), SPILL_PRIORITY_DISABLE);
    }
    virtual void prepareLoopResults(CActivityBase &activity, IThorGraphResults *results)
    {
        if (!resultRowIf)
            resultRowIf.setown(createRowInterfaces(resultMeta, activityId, activity.queryCodeContext()));
        IThorResult *loopResult = results->createResult(activity, 0, resultRowIf, !activity.queryGraph().isLocalChild()); // loop output
        IThorResult *inputResult = results->createResult(activity, 1, resultRowIf, !activity.queryGraph().isLocalChild()); // loop input
    }
    virtual void execute(CActivityBase &activity, unsigned counter, IThorGraphResults *results, IRowWriterMultiReader *inputStream, rowcount_t rowStreamCount, size32_t parentExtractSz, const byte *parentExtract)
    {
        if (counter)
            graph->setLoopCounter(counter);
        Owned<IThorResult> inputResult = results->getResult(1);
        if (inputStream)
            inputResult->setResultStream(inputStream, rowStreamCount);
        graph->executeChild(parentExtractSz, parentExtract, results, NULL);
    }
    virtual void execute(CActivityBase &activity, unsigned counter, IThorGraphResults *graphLoopResults, size32_t parentExtractSz, const byte *parentExtract)
    {
        Owned<IThorGraphResults> results = graph->createThorGraphResults(1);
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
                e->Release();
                te = e2.getClear();
            }
            else if (!te->queryActivityId())
                setExceptionActivityInfo(activity.queryContainer(), te);
            try { graph->abort(te); }
            catch (IException *abortE)
            {
                Owned<IThorException> e2 = MakeActivityException(&activity, abortE, "Exception whilst aborting graph");
                abortE->Release();
                EXCLOG(e2, NULL);
            }
            graph->queryJob().fireException(te);
            throw te;
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
        case TAKxmlread:
        case TAKjsonread:
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

void CIOConnection::connect(unsigned which, CActivityBase *destActivity)
{
    destActivity->setInput(which, activity->queryActivity(), index);
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
    isLocalData = xgmml->getPropBool("att[@name=\"local\"]/@value", false); // local execute + local data access only
    isLocal = isLocalData || coLocal; // local execute
    isGrouped = xgmml->getPropBool("att[@name=\"grouped\"]/@value", false);
    resultsGraph = NULL;
    ownerId = xgmml->getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
    onCreateCalled = onStartCalled = prepared = haveCreateCtx = haveStartCtx = nullAct = false;
    onlyUpdateIfChanged = xgmml->getPropBool("att[@name=\"_updateIfChanged\"]/@value", false);

    StringBuffer helperName("fAc");
    xgmml->getProp("@id", helperName);
    helperFactory = (EclHelperFactory) queryJob().queryDllEntry().getEntry(helperName.str());
    if (!helperFactory)
        throw makeOsExceptionV(GetLastError(), "Failed to load helper factory method: %s (dll handle = %p)", helperName.str(), queryJob().queryDllEntry().getInstance());
    alreadyUpdated = false;
    whichBranch = (unsigned)-1;
    whichBranchBitSet.setown(createThreadSafeBitSet());
    newWhichBranch = false;
    isEof = false;
    log = true;
    sentActInitData.setown(createThreadSafeBitSet());
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
    loopGraph.clear();
    associatedChildGraphs.kill();
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

IThorGraphIterator *CGraphElementBase::getAssociatedChildGraphs() const
{
    return new CGraphArrayIterator(associatedChildGraphs);
}

IThorGraphDependencyIterator *CGraphElementBase::getDependsIterator() const
{
    return new ArrayIIteratorOf<const CGraphDependencyArray, CGraphDependency, IThorGraphDependencyIterator>(dependsOn);
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

void CGraphElementBase::ActPrintLog(IException *e)
{
    ActPrintLog(e, "%s", "");
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
        CIOConnection *io = connectedInputs.item(i);
        if (io)
            io->connect(i, activity);
    }
}

void CGraphElementBase::clearConnections()
{
    connectedInputs.kill();
    connectedOutputs.kill();
    if (activity)
        activity->clearConnections();
}

void CGraphElementBase::addInput(unsigned input, CGraphElementBase *inputAct, unsigned inputOutIdx)
{
    while (inputs.ordinality()<=input) inputs.append(NULL);
    inputs.replace(new COwningSimpleIOConnection(LINK(inputAct), inputOutIdx), input);
    while (inputAct->outputs.ordinality()<=inputOutIdx) inputAct->outputs.append(NULL);
    inputAct->outputs.replace(new CIOConnection(this, input), inputOutIdx);
}

void CGraphElementBase::connectInput(unsigned input, CGraphElementBase *inputAct, unsigned inputOutIdx)
{
    ActPrintLog("CONNECTING (id=%" ACTPF "d, idx=%d) to (id=%" ACTPF "d, idx=%d)", inputAct->queryId(), inputOutIdx, queryId(), input);
    while (connectedInputs.ordinality()<=input) connectedInputs.append(NULL);
    connectedInputs.replace(new COwningSimpleIOConnection(LINK(inputAct), inputOutIdx), input);
    while (inputAct->connectedOutputs.ordinality()<=inputOutIdx) inputAct->connectedOutputs.append(NULL);
    inputAct->connectedOutputs.replace(new CIOConnection(this, input), inputOutIdx);
}

void CGraphElementBase::addAssociatedChildGraph(CGraphBase *childGraph)
{
    if (!associatedChildGraphs.contains(*childGraph))
        associatedChildGraphs.append(*LINK(childGraph));
}

void CGraphElementBase::serializeCreateContext(MemoryBuffer &mb)
{
    if (!onCreateCalled) return;
    DelayedSizeMarker sizeMark(mb);
    queryHelper()->serializeCreateContext(mb);
    sizeMark.write();
}

void CGraphElementBase::serializeStartContext(MemoryBuffer &mb)
{
    if (!onStartCalled) return;
    DelayedSizeMarker sizeMark(mb);
    queryHelper()->serializeStartContext(mb);
    sizeMark.write();
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
    onStartCalled = false; // allow to be called again
}

void CGraphElementBase::onCreate()
{
    CriticalBlock b(crit);
    if (onCreateCalled)
        return;
    onCreateCalled = true;
    baseHelper.setown(helperFactory());
    if (!nullAct)
    {
        CGraphElementBase *ownerActivity = owner->queryOwner() ? owner->queryOwner()->queryElement(ownerId) : NULL;
        if (ownerActivity)
        {
            ownerActivity->onCreate(); // ensure owner created, might not be if this is child query inside another child query.
            baseHelper->onCreate(queryCodeContext(), ownerActivity->queryHelper(), haveCreateCtx?&createCtxMb:NULL);
        }
        else
            baseHelper->onCreate(queryCodeContext(), NULL, haveCreateCtx?&createCtxMb:NULL);
    }
}

void CGraphElementBase::onStart(size32_t parentExtractSz, const byte *parentExtract)
{
    if (onStartCalled)
        return;
    onStartCalled = true;
    if (!nullAct)
    {
        if (haveStartCtx)
        {
            baseHelper->onStart(parentExtract, &startCtxMb);
            startCtxMb.reset();
            haveStartCtx = false;
        }
        else
            baseHelper->onStart(parentExtract, NULL);
    }
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
    try
    {
        bool _shortCircuit = shortCircuit;
        Owned<IThorGraphDependencyIterator> deps = getDependsIterator();
        bool depsDone = true;
        ForEach(*deps)
        {
            CGraphDependency &dep = deps->query();
            if (0 == dep.controlId && NotFound == owner->dependentSubGraphs.find(*dep.graph))
            {
                owner->dependentSubGraphs.append(*dep.graph);
                if (!dep.graph->isComplete())
                    depsDone = false;
            }
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
            case TAKjsonwrite:
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
            case TAKifaction:
            {
                if (_shortCircuit) return true;
                onCreate();
                onStart(parentExtractSz, parentExtract);
                IHThorIfArg *helper = (IHThorIfArg *)baseHelper.get();
                whichBranch = helper->getCondition() ? 0 : 1;       // True argument precedes false...
                /* NB - The executeDependencies code below is only needed if actionLinkInNewGraph=true, which is no longer the default
                 * It should be removed, once we are positive there are no issues with in-line conditional actions
                 */
                if (TAKifaction == getKind())
                {
                    if (!executeDependencies(parentExtractSz, parentExtract, whichBranch+1, async)) //NB whenId 1 based
                        return false;
                }

                if (inputs.queryItem(whichBranch))
                {
                    if (!whichBranchBitSet->testSet(whichBranch)) // if not set, new
                        newWhichBranch = true;
                    return inputs.item(whichBranch)->activity->prepareContext(parentExtractSz, parentExtract, checkDependencies, false, async);
                }
                return true;
            }
            case TAKchildcase:
                owner->ifs.append(*this);
                // fall through
            case TAKcase:
            {
                if (_shortCircuit) return true;
                onCreate();
                onStart(parentExtractSz, parentExtract);
                IHThorCaseArg *helper = (IHThorCaseArg *)baseHelper.get();
                whichBranch = helper->getBranch();
                if (whichBranch >= inputs.ordinality())
                    whichBranch = inputs.ordinality()-1;
                if (inputs.queryItem(whichBranch))
                    return inputs.item(whichBranch)->activity->prepareContext(parentExtractSz, parentExtract, checkDependencies, false, async);
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
            case TAKsequential:
            case TAKparallel:
            {
                /* NB - The executeDependencies code below is only needed if actionLinkInNewGraph=true, which is no longer the default
                 * It should be removed, once we are positive there are no issues with in-line sequential/parallel activities
                 */
                for (unsigned s=1; s<=dependsOn.ordinality(); s++)
                {
                    if (!executeDependencies(parentExtractSz, parentExtract, s, async))
                        return false;
                }
                break;
            }
            case TAKwhen_dataset:
            case TAKwhen_action:
            {
                if (!executeDependencies(parentExtractSz, parentExtract, WhenBeforeId, async))
                    return false;
                if (!executeDependencies(parentExtractSz, parentExtract, WhenParallelId, async))
                    return false;
                break;
            }
        }
        ForEachItemIn(i, inputs)
        {
            CGraphElementBase *input = inputs.item(i)->activity;
            if (!input->prepareContext(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async))
                return false;
        }
        return true;
    }
    catch (IException *_e)
    {
        IThorException *e = QUERYINTERFACE(_e, IThorException);
        if (e)
        {
            if (!e->queryActivityId())
                setExceptionActivityInfo(*this, e);
        }
        else
        {
            e = MakeActivityException(this, _e);
            _e->Release();
        }
        throw e;
    }
}

void CGraphElementBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    activity->preStart(parentExtractSz, parentExtract);
}

void CGraphElementBase::initActivity()
{
    CriticalBlock b(crit);
    if (isSink())
        owner->addActiveSink(*this);
    if (activity) // no need to recreate
        return;
    activity.setown(factory());
    if (isLoopActivity(*this))
    {
        unsigned loopId = queryXGMML().getPropInt("att[@name=\"_loopid\"]/@value");
        Owned<CGraphBase> childGraph = owner->getChildGraph(loopId);
        Owned<IThorBoundLoopGraph> boundLoopGraph = createBoundLoopGraph(childGraph, baseHelper->queryOutputMeta(), queryId());
        setBoundGraph(boundLoopGraph);
    }
}

void CGraphElementBase::createActivity(size32_t parentExtractSz, const byte *parentExtract)
{
    if (connectedInputs.ordinality()) // ensure not traversed twice (e.g. via splitter)
        return;
    try
    {
        switch (getKind())
        {
            case TAKchildif:
            case TAKchildcase:
            {
                if (inputs.queryItem(whichBranch))
                {
                    CGraphElementBase *input = inputs.item(whichBranch)->activity;
                    input->createActivity(parentExtractSz, parentExtract);
                }
                onCreate();
                initActivity();
                if (inputs.queryItem(whichBranch))
                {
                    CIOConnection *inputIO = inputs.item(whichBranch);
                    connectInput(whichBranch, inputIO->activity, inputIO->index);
                }
                break;
            }
            case TAKif:
            case TAKcase:
                if (inputs.queryItem(whichBranch))
                {
                    CGraphElementBase *input = inputs.item(whichBranch)->activity;
                    input->createActivity(parentExtractSz, parentExtract);
                }
                else
                {
                    onCreate();
                    if (!activity)
                        factorySet(TAKnull);
                }
                break;
            case TAKifaction:
                if (inputs.queryItem(whichBranch))
                {
                    CGraphElementBase *input = inputs.item(whichBranch)->activity;
                    input->createActivity(parentExtractSz, parentExtract);
                }
                break;
            case TAKsequential:
            case TAKparallel:
            {
                ForEachItemIn(i, inputs)
                {
                    if (inputs.queryItem(i))
                    {
                        CGraphElementBase *input = inputs.item(i)->activity;
                        input->createActivity(parentExtractSz, parentExtract);
                    }
                }
                break;
            }
            default:
                if (!isEof)
                {
                    ForEachItemIn(i, inputs)
                    {
                        CGraphElementBase *input = inputs.item(i)->activity;
                        input->createActivity(parentExtractSz, parentExtract);
                    }
                }
                onCreate();
                if (isDiskInput(getKind()))
                    onStart(parentExtractSz, parentExtract);
                ForEachItemIn(i2, inputs)
                {
                    CIOConnection *inputIO = inputs.item(i2);
                    loop
                    {
                        CGraphElementBase *input = inputIO->activity;
                        switch (input->getKind())
                        {
                            case TAKif:
                            case TAKcase:
                            {
                                if (input->whichBranch >= input->getInputs()) // if, will have TAKnull activity, made at create time.
                                {
                                    input = NULL;
                                    break;
                                }
                                inputIO = input->inputs.item(input->whichBranch);
                                assertex(inputIO);
                                break;
                            }
                            default:
                                input = NULL;
                                break;
                        }
                        if (!input)
                            break;
                    }
                    connectInput(i2, inputIO->activity, inputIO->index);
                }
                initActivity();
                break;
        }
    }
    catch (IException *e) { ActPrintLog(e); activity.clear(); throw; }
}

ICodeContext *CGraphElementBase::queryCodeContext()
{
    return queryOwner().queryCodeContext();
}

/////

// JCSMORE loop - probably need better way to check if any act in graph is global(meaning needs some synchronization between slaves in activity execution)
bool isGlobalActivity(CGraphElementBase &container)
{
    switch (container.getKind())
    {
// always global, but only co-ordinate init/done
        case TAKcsvwrite:
        case TAKxmlwrite:
        case TAKjsonwrite:
        case TAKindexwrite:
        case TAKkeydiff:
        case TAKkeypatch:
        case TAKdictionaryworkunitwrite:
            return true;
        case TAKdiskwrite:
        {
            Owned<IHThorDiskWriteArg> helper = (IHThorDiskWriteArg *)container.helperFactory();
            unsigned flags = helper->getFlags();
            return (0 == (TDXtemporary & flags)); // global if not temporary
        }
        case TAKspill:
            return false;
        case TAKcsvread:
        {
            Owned<IHThorCsvReadArg> helper = (IHThorCsvReadArg *)container.helperFactory();
            // if header lines, then [may] need to co-ordinate across slaves
            if (container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
                return false;
            return helper->queryCsvParameters()->queryHeaderLen() > 0;
        }
// dependent on child acts?
        case TAKlooprow:
        case TAKloopcount:
        case TAKgraphloop:
        case TAKparallelgraphloop:
        case TAKloopdataset:
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
        case TAKsmartjoin:
        case TAKkeyeddenormalize:
        case TAKhashdenormalize:
        case TAKdenormalize:
        case TAKlookupdenormalize: //GH->JCS why are these here, and join not?
        case TAKalldenormalize:
        case TAKsmartdenormalize:
        case TAKdenormalizegroup:
        case TAKhashdenormalizegroup:
        case TAKlookupdenormalizegroup:
        case TAKkeyeddenormalizegroup:
        case TAKalldenormalizegroup:
        case TAKsmartdenormalizegroup:
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
        case TAKwhen_dataset:
        case TAKwhen_action:
        case TAKnonempty:
            if (!container.queryLocalOrGrouped())
                return true;
            break;
        case TAKkeyedjoin:
        case TAKalljoin:
        case TAKlookupjoin:
            if (!container.queryLocal())
                return true;
// always local
        case TAKfilter:
        case TAKfilterproject:
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
        case TAKchildcase:
        case TAKcase:
        case TAKparse:
        case TAKpiperead:
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
        case TAKxmlread:
        case TAKjsonread:
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
        case TAKhttp_rowdataset:
        case TAKsoap_rowaction:
        case TAKsoap_datasetdataset:
        case TAKsoap_datasetaction:
        case TAKlinkedrawiterator:
        case TAKchilditerator:
        case TAKstreamediterator:
        case TAKworkunitread:
        case TAKchilddataset:
        case TAKinlinetable:
        case TAKnull:
        case TAKemptyaction:
        case TAKlocalresultread:
        case TAKlocalresultwrite:
        case TAKdictionaryresultwrite:
        case TAKgraphloopresultread:
        case TAKgraphloopresultwrite:
        case TAKnwaygraphloopresultread:
        case TAKapply:
        case TAKsideeffect:
        case TAKsimpleaction:
        case TAKsorted:
        case TAKdistributed:
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
        default:
            return true; // if in doubt
    }
    return false;
}

bool isLoopActivity(CGraphElementBase &container)
{
    switch (container.getKind())
    {
        case TAKlooprow:
        case TAKloopcount:
        case TAKloopdataset:
        case TAKgraphloop:
        case TAKparallelgraphloop:
            return true;
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
    counter = 0; // loop/graph counter, will be set by loop/graph activity if needed
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
    localResults.clear();
    graphLoopResults.clear();
    childGraphsTable.kill();
    childGraphs.kill();
    disconnectActivities();
    containers.kill();
    sinks.kill();
    activeSinks.kill();
}

void CGraphBase::serializeCreateContexts(MemoryBuffer &mb)
{
    DelayedSizeMarker sizeMark(mb);
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        if (element.isOnCreated())
        {
            mb.append(element.queryId());
            element.serializeCreateContext(mb);
        }
    }
    mb.append((activity_id)0);
    sizeMark.write();
}

void CGraphBase::serializeStartContexts(MemoryBuffer &mb)
{
    DelayedSizeMarker sizeMark(mb);
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        if (element.isOnStarted())
        {
            mb.append(element.queryId());
            element.serializeStartContext(mb);
        }
    }
    mb.append((activity_id)0);
    sizeMark.write();
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
    graphCancelHandler.reset();
    if (0 == containers.count())
    {
        SuperHashIteratorOf<CGraphBase> iter(childGraphsTable);
        ForEach(iter)
            iter.query().reset();
    }
    else
    {
        Owned<IThorActivityIterator> iter = getIterator();
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            element.reset();
        }
        dependentSubGraphs.kill();
    }
    if (!queryOwner() || isGlobal())
        job.queryTimeReporter().reset();
    if (!queryOwner())
        clearNodeStats();
}

void CGraphBase::addChildGraph(CGraphBase &graph)
{
    CriticalBlock b(crit);
    childGraphs.append(graph);
    childGraphsTable.replace(graph);
    job.associateGraph(graph);
}

IThorGraphIterator *CGraphBase::getChildGraphs() const
{
    CriticalBlock b(crit);
    return new CGraphArrayCopyIterator(childGraphs);
}

bool CGraphBase::fireException(IException *e)
{
    return job.fireException(e);
}

bool CGraphBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        element.preStart(parentExtractSz, parentExtract);
    }
    return true;
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
        GraphPrintLog(e);
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
    if (async)
        queryJob().startGraph(*this, queryJob(), checkDependencies, parentExtractSz, parentExtract); // may block if enough running
    else
    {
        if (!prepare(parentExtractSz, parentExtract, checkDependencies, async, async))
        {
            setComplete();
            return;
        }
        executeSubGraph(parentExtractSz, parentExtract);
    }
}

void CGraphBase::join()
{
    if (poolThreadHandle)
        queryJob().joinGraph(*this);
}

void CGraphBase::doExecute(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies)
{
    if (isComplete()) return;
    if (queryAborted())
    {
        if (abortException)
            throw abortException.getLink();
        throw MakeGraphException(this, 0, "subgraph aborted(1)");
    }
    if (!prepare(parentExtractSz, parentExtract, checkDependencies, false, false))
    {
        setComplete();
        return;
    }
    if (queryAborted())
    {
        if (abortException)
            throw abortException.getLink();
        throw MakeGraphException(this, 0, "subgraph aborted(2)");
    }
    Owned<IException> exception;
    try
    {
        if (started)
            reset();
        Owned<IThorActivityIterator> iter = getConnectedIterator();
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            element.onStart(parentExtractSz, parentExtract);
        }
        if (!preStart(parentExtractSz, parentExtract)) return;
        start();
        if (!wait(aborted?MEDIUMTIMEOUT:INFINITE)) // can't wait indefinitely, query may have aborted and stall, but prudent to wait a short time for underlying graphs to unwind.
            GraphPrintLogEx(this, thorlog_null, MCuserWarning, "Graph wait cancelled, aborted=%s", aborted?"true":"false");
        else
            graphDone = true;
    }
    catch (IException *e)
    {
        GraphPrintLog(e);
        exception.setown(e);
    }
    try
    {
        if (!exception && abortException)
            exception.setown(abortException.getClear());
        if (exception)
        {
            if (NULL == owner || isGlobal())
                waitBarrier->cancel(exception);
            if (!queryOwner())
            {
                StringBuffer str;
                Owned<IThorException> e = MakeGraphException(this, exception->errorCode(), "%s", exception->errorMessage(str).str());
                e->setGraphId(graphId);
                e->setAction(tea_abort);
                fireException(e);
            }
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
        GraphPrintLog(e);
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
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        element.clearConnections();
    }
    activeSinks.kill(); // NB: activeSinks are added to during activity creation
    ForEachItemIn(s, sinks)
    {
        CGraphElementBase &sink = sinks.item(s);
        sink.createActivity(parentExtractSz, parentExtract);
    }
    created = true;
}

void CGraphBase::done()
{
    if (aborted) return; // activity done methods only called on success
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach (*iter)
    {
        CGraphElementBase &element = iter->query();
        element.queryActivity()->done();
    }
}

bool CGraphBase::syncInitData()
{
    CGraphElementBase *parentElement = queryOwner() ? queryOwner()->queryElement(queryParentActivityId()) : NULL;
    if (parentElement && isLoopActivity(*parentElement))
        return parentElement->queryLoopGraph()->queryGraph()->isGlobal();
    else
        return !isLocalChild();
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
            GraphPrintLog(e2);
            e->Release();
        }
    }
}

class CGraphTraverseIteratorBase : public CInterface, implements IThorActivityIterator
{
protected:
    CGraphBase &graph;
    Linked<CGraphElementBase> cur;
    CIArrayOf<CGraphElementBase> others;
    CGraphElementArrayCopy covered;

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
    CGraphElementBase *setNext(CIOConnectionArray &inputs, unsigned whichInput=((unsigned)-1))
    {
        cur.clear();
        unsigned n = inputs.ordinality();
        if (((unsigned)-1) != whichInput)
        {
            CIOConnection *io = inputs.queryItem(whichInput);
            if (io)
                cur.set(io->activity);
        }
        else
        {
            bool first = true;
            unsigned i=0;
            for (; i<n; i++)
            {
                CIOConnection *io = inputs.queryItem(i);
                if (io)
                {
                    if (first)
                    {
                        first = false;
                        cur.set(io->activity);
                    }
                    else
                        others.append(*LINK(io->activity));
                }
            }
        }
        if (!cur)
        {
            if (!popNext())
                return NULL;
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
    CGraphTraverseIteratorBase(CGraphBase &_graph) : graph(_graph)
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

class CGraphTraverseConnectedIterator : public CGraphTraverseIteratorBase
{
public:
    CGraphTraverseConnectedIterator(CGraphBase &graph) : CGraphTraverseIteratorBase(graph) { }
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

IThorActivityIterator *CGraphBase::getConnectedIterator()
{
    return new CGraphTraverseConnectedIterator(*this);
}

bool CGraphBase::wait(unsigned timeout)
{
    CTimeMon tm(timeout);
    unsigned remaining = timeout;
    class CWaitException
    {
        CGraphBase *graph;
        Owned<IException> exception;
    public:
        CWaitException(CGraphBase *_graph) : graph(_graph) { }
        IException *get() { return exception; }
        void set(IException *e)
        {
            if (!exception)
                exception.setown(e);
            else
                e->Release();
        }
        void throwException()
        {
            if (exception)
                throw exception.getClear();
            throw MakeGraphException(graph, 0, "Timed out waiting for graph to end");
        }
    } waitException(this);
    Owned<IThorActivityIterator> iter = getConnectedIterator();
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
            waitException.set(e); // will discard if already set
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
        if (!waitBarrier->wait(true, remaining))
            return false;
    }
    return true;
}

void CGraphBase::abort(IException *e)
{
    if (aborted)
        return;

    {
        CriticalBlock cb(crit);

        abortException.set(e);
        aborted = true;
        graphCancelHandler.cancel(0);

        if (0 == containers.count())
        {
            Owned<IThorGraphIterator> iter = getChildGraphs();
            ForEach(*iter)
            {
                CGraphBase &graph = iter->query();
                graph.abort(e);
            }
        }
    }
    if (started && !graphDone)
    {
        Owned<IThorActivityIterator> iter = getConnectedIterator();
        ForEach (*iter)
        {
            iter->query().abort(e); // JCSMORE - could do in parallel, they can take some time to timeout
        }
        if (startBarrier)
            startBarrier->cancel(e);
        if (waitBarrier)
            waitBarrier->cancel(e);
        if (doneBarrier)
            doneBarrier->cancel(e);
    }
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

void CGraphBase::GraphPrintLog(IException *e)
{
    GraphPrintLog(e, "%s", "");
}

void CGraphBase::setLogging(bool tf)
{
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach(*iter)
        iter->query().setLogging(tf);
}

void CGraphBase::createFromXGMML(IPropertyTree *_node, CGraphBase *_owner, CGraphBase *_parent, CGraphBase *resultsGraph)
{
    owner = _owner;
    parent = _parent?_parent:owner;
    node.setown(createPTreeFromIPT(_node));
    xgmml = node->queryPropTree("att/graph");
    sink = xgmml->getPropBool("att[@name=\"rootGraph\"]/@value", false);
    sequential = xgmml->getPropBool("@sequential");
    graphId = node->getPropInt("@id");
    global = false;
    localOnly = -1; // unset
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
        // JCSMORE - it might more sense if this temp handler was owned by parent act., which may finish(get stopped) earlier than the owning graph
        tmpHandler.setown(queryJob().createTempHandler(false));
    }

    localChild = false;
    if (owner && parentActivityId)
    {
        CGraphElementBase *parentElement = owner->queryElement(parentActivityId);
        parentElement->addAssociatedChildGraph(this);
        if (isLoopActivity(*parentElement))
            localChild = parentElement->queryOwner().isLocalChild();
        else
            localChild = true;
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
            if (localChild && !e.getPropBool("att[@name=\"coLocal\"]/@value", false))
            {
                IPropertyTree *att = createPTree("att");
                att->setProp("@name", "coLocal");
                att->setPropBool("@value", true);
                e.addPropTree("att", att);
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
        target->addInput(targetInput, source, sourceOutput);
    }
    Owned<IThorActivityIterator> iter = getIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        if (0 == element.getOutputs())
        {
            /* JCSMORE - Making some outputs conditional, will require:
             * a) Pass through information as to which dependent graph causes this graph (and this sink) to execute)
             * b) Allow the subgraph to re-executed by other dependent subgraphs and avoid re-executing completed sinks
             * c) Keep common points (splitters) around (preferably in memory), re-execution of graph will need them
             */
            sinks.append(*LINK(&element));
        }
    }
    init();
}

void CGraphBase::executeChildGraphs(size32_t parentExtractSz, const byte *parentExtract)
{
    // JCSMORE - would need to respect codegen 'sequential' flag, if these child graphs
    // could be executed in parallel.
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
            str.append("(").append(e.queryId()).append(")");
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
        throw MakeGraphException(this, 0, "Global child graph? : %s", str.str());
    }
    doExecuteChild(parentExtractSz, parentExtract);
}

IThorResult *CGraphBase::getResult(unsigned id, bool distributed)
{
    return localResults->getResult(id, distributed);
}

IThorResult *CGraphBase::getGraphLoopResult(unsigned id, bool distributed)
{
    return graphLoopResults->getResult(id, distributed);
}

IThorResult *CGraphBase::createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    return results->createResult(activity, id, rowIf, distributed, spillPriority);
}

IThorResult *CGraphBase::createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    return localResults->createResult(activity, id, rowIf, distributed, spillPriority);
}

IThorResult *CGraphBase::createGraphLoopResult(CActivityBase &activity, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority)
{
    return graphLoopResults->createResult(activity, rowIf, distributed, spillPriority);
}

// IEclGraphResults
void CGraphBase::getDictionaryResult(unsigned & count, byte * * & ret, unsigned id)
{
    Owned<IThorResult> result = getResult(id, true); // will get collated distributed result
    result->getLinkedResult(count, ret);
}

void CGraphBase::getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
{
    Owned<IThorResult> result = getResult(id, true); // will get collated distributed result
    result->getLinkedResult(count, ret);
}

const void * CGraphBase::getLinkedRowResult(unsigned id)
{
    Owned<IThorResult> result = getResult(id, true); // will get collated distributed result
    return result->getLinkedRowResult();
}

// IThorChildGraph impl.
IEclGraphResults *CGraphBase::evaluate(unsigned _parentExtractSz, const byte *parentExtract)
{
    CriticalBlock block(evaluateCrit);
    localResults.setown(createThorGraphResults(xgmml->getPropInt("att[@name=\"_numResults\"]/@value", 0)));
    parentExtractSz = _parentExtractSz;
    executeChild(parentExtractSz, parentExtract);
    return localResults.getClear();
}

static bool isLocalOnly(const CGraphElementBase &activity);
static bool isLocalOnly(const CGraphBase &graph) // checks all dependencies, if something needs to be global, whole body is forced to be execution sync.
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
        Owned<IThorActivityIterator> sinkIter = graph.getAllSinkIterator();
        ForEach(*sinkIter)
        {
            CGraphElementBase &sink = sinkIter->query();
            if (!isLocalOnly(sink))
                return false;
        }
    }
    return true;
}

static bool isLocalOnly(const CGraphElementBase &activity)
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

bool CGraphBase::isLocalOnly() const // checks all dependencies, if something needs to be global, whole body is forced to be execution sync.
{
    if (-1 == localOnly)
        localOnly = (int)::isLocalOnly(*this);
    return 1==localOnly;
}

IThorGraphResults *CGraphBase::createThorGraphResults(unsigned num)
{
    return new CThorGraphResults(num);
}


////

void CGraphTempHandler::registerFile(const char *name, graph_id graphId, unsigned usageCount, bool temp, WUFileKind fileKind, StringArray *clusters)
{
    assertex(temp);
    LOG(MCdebugProgress, thorJob, "registerTmpFile name=%s, usageCount=%d", name, usageCount);
    CriticalBlock b(crit);
    if (tmpFiles.find(name))
        throw MakeThorException(TE_FileAlreadyUsedAsTempFile, "File already used as temp file (%s)", name);
    tmpFiles.replace(* new CFileUsageEntry(name, graphId, fileKind, usageCount));
}

void CGraphTempHandler::deregisterFile(const char *name, bool kept)
{
    LOG(MCdebugProgress, thorJob, "deregisterTmpFile name=%s", name);
    CriticalBlock b(crit);
    CFileUsageEntry *fileUsage = tmpFiles.find(name);
    if (!fileUsage)
    {
        if (errorOnMissing)
            throw MakeThorException(TE_FileNotFound, "File not found (%s) deregistering tmp file", name);
        return;
    }
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
    CriticalBlock b(crit);
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
    unsigned waitOnRunning;
    CriticalSection crit;
    Semaphore sem, runningSem;
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

    CGraphExecutorGraphInfo *findRunning(graph_id gid)
    {
        ForEachItemIn(r, running)
        {
            CGraphExecutorGraphInfo *graphInfo = &running.item(r);
            if (gid == graphInfo->subGraph->queryGraphId())
                return graphInfo;
        }
        return NULL;
    }
public:
    IMPLEMENT_IINTERFACE;

    CGraphExecutor(CJobBase &_job) : job(_job)
    {
        limit = (unsigned)job.getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
        waitOnRunning = 0;
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
        if (waitOnRunning)
        {
            runningSem.signal(waitOnRunning);
            waitOnRunning = 0;
        }

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
            CICopyArrayOf<CGraphExecutorGraphInfo> toMove;
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
    virtual void add(CGraphBase *subGraph, IGraphCallback &callback, bool checkDependencies, size32_t parentExtractSz, const byte *parentExtract)
    {
        bool alreadyRunning;
        {
            CriticalBlock b(crit);
            if (job.queryPausing())
                return;
            if (subGraph->isComplete())
                return;
            alreadyRunning = NULL != findRunning(subGraph->queryGraphId());
            if (alreadyRunning)
                ++waitOnRunning;
        }
        if (alreadyRunning)
        {
            loop
            {
                PROGLOG("Waiting on subgraph %" GIDPF "d", subGraph->queryGraphId());
                if (runningSem.wait(MEDIUMTIMEOUT) || job.queryAborted() || job.queryPausing())
                    break;
            }
            return;
        }
        else
        {
            CriticalBlock b(crit);
            if (seen.contains(subGraph->queryGraphId()))
                return; // already queued;
            seen.append(subGraph->queryGraphId());
        }
        if (!subGraph->prepare(parentExtractSz, parentExtract, checkDependencies, true, true))
        {
            subGraph->setComplete();
            return;
        }
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
        CriticalBlock b(crit);
        if (0 == subGraph->dependentSubGraphs.ordinality())
        {
            if (running.ordinality()<limit)
            {
                running.append(*LINK(graphInfo));
                PROGLOG("Add: Launching graph thread for graphId=%" GIDPF "d", subGraph->queryGraphId());
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
        loop
        {
            CriticalBlock b(crit);
            if (stopped || job.queryAborted() || job.queryPausing())
                break;
            if (0 == stack.ordinality() && 0 == toRun.ordinality() && 0 == running.ordinality())
                break;
            if (job.queryPausing())
                break; // pending graphs will re-run on resubmission

            bool signalled;
            {
                CriticalUnblock b(crit);
                signalled = sem.wait(MEDIUMTIMEOUT);
            }
            if (signalled)
            {
                bool added = false;
                if (running.ordinality() < limit)
                {
                    while (toRun.ordinality())
                    {
                        Linked<CGraphExecutorGraphInfo> graphInfo = &toRun.item(0);
                        toRun.remove(0);
                        running.append(*LINK(graphInfo));
                        CGraphBase *subGraph = graphInfo->subGraph;
                        PROGLOG("Wait: Launching graph thread for graphId=%" GIDPF "d", subGraph->queryGraphId());
                        added = true;
                        PooledThreadHandle h = graphPool->start(graphInfo.getClear());
                        subGraph->poolThreadHandle = h;
                        if (running.ordinality() >= limit)
                            break;
                    }
                }
                if (!added)
                    Sleep(1000); // still more to come
            }
            else
                PROGLOG("Waiting on executing graphs to complete.");
            StringBuffer str("Currently running graphId = ");

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
};

////
// IContextLogger
class CThorContextLogger : CSimpleInterface, implements IContextLogger
{
    CJobBase &job;
    unsigned traceLevel;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorContextLogger(CJobBase &_job) : job(_job)
    {
        traceLevel = 1;
    }
    virtual void CTXLOGva(const char *format, va_list args) const
    {
        StringBuffer ss;
        ss.valist_appendf(format, args);
        LOG(MCdebugProgress, thorJob, "%s", ss.str());
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const
    {
        StringBuffer ss;
        ss.append("ERROR");
        if (E)
            ss.append(": ").append(E->errorCode());
        if (file)
            ss.appendf(": %s(%d) ", file, line);
        if (E)
            E->errorMessage(ss.append(": "));
        if (format)
            ss.append(": ").valist_appendf(format, args);
        LOG(MCoperatorProgress, thorJob, "%s", ss.str());
    }
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const
    {
    }
    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
    }
    virtual unsigned queryTraceLevel() const
    {
        return traceLevel;
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
    globalMemorySize = globals->getPropInt("@globalMemorySize"); // in MB
    oldNodeCacheMem = 0;
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

    forceLogGraphIdMin = (graph_id)getWorkUnitValueInt("forceLogGraphIdMin", 0);
    forceLogGraphIdMax = (graph_id)getWorkUnitValueInt("forceLogGraphIdMax", 0);

    logctx.setown(new CThorContextLogger(*this));

    // global setting default on, can be overridden by #option
    timeActivities = 0 != getWorkUnitValueInt("timeActivities", globals->getPropBool("@timeActivities", true));
    maxActivityCores = (unsigned)getWorkUnitValueInt("maxActivityCores", globals->getPropInt("@maxActivityCores", 0)); // NB: 0 means system decides
    pausing = false;
    resumed = false;

    bool crcChecking = 0 != getWorkUnitValueInt("THOR_ROWCRC", globals->getPropBool("@THOR_ROWCRC", false));
    bool usePackedAllocator = 0 != getWorkUnitValueInt("THOR_PACKEDALLOCATOR", globals->getPropBool("@THOR_PACKEDALLOCATOR", true));
    unsigned memorySpillAt = (unsigned)getWorkUnitValueInt("memorySpillAt", globals->getPropInt("@memorySpillAt", 80));
    thorAllocator.setown(createThorAllocator(((memsize_t)globalMemorySize)*0x100000, memorySpillAt, *logctx, crcChecking, usePackedAllocator));

    unsigned defaultMemMB = globalMemorySize*3/4;
    unsigned largeMemSize = getOptUInt("@largeMemSize", defaultMemMB);
    if (globalMemorySize && largeMemSize >= globalMemorySize)
        throw MakeStringException(0, "largeMemSize(%d) can not exceed globalMemorySize(%d)", largeMemSize, globalMemorySize);
    PROGLOG("Global memory size = %d MB, memory spill at = %d%%, large mem size = %d MB", globalMemorySize, memorySpillAt, largeMemSize);
    StringBuffer tracing("maxActivityCores = ");
    if (maxActivityCores)
        tracing.append(maxActivityCores);
    else
        tracing.append("[unbound]");
    PROGLOG("%s", tracing.str());
    setLargeMemSize(largeMemSize);
    graphExecutor.setown(new CGraphExecutor(*this));
}

void CJobBase::beforeDispose()
{
    endJob();
}

CJobBase::~CJobBase()
{
    clean();
    thorAllocator->queryRowManager()->reportMemoryUsage(false);
    PROGLOG("CJobBase resetting memory manager");
    thorAllocator.clear();

    ::Release(codeCtx);
    ::Release(userDesc);
    timeReporter->Release();
    delete pluginMap;

    StringBuffer memStatsStr;
    roxiemem::memstats(memStatsStr);
    PROGLOG("Roxiemem stats: %s", memStatsStr.str());
    memsize_t heapUsage = getMapInfo("heap");
    if (heapUsage) // if 0, assumed to be unavailable
        PROGLOG("Heap usage : %" I64F "d bytes", (unsigned __int64)heapUsage);
}

void CJobBase::startJob()
{
    LOG(MCdebugProgress, thorJob, "New Graph started : %s", graphName.get());
    ClearTempDirs();
    unsigned pinterval = globals->getPropInt("@system_monitor_interval",1000*60);
    if (pinterval)
    {
        perfmonhook.setown(createThorMemStatsPerfMonHook(*this, getOptInt(THOROPT_MAX_KERNLOG, 3)));
        startPerformanceMonitor(pinterval,PerfMonStandard,perfmonhook);
    }
    PrintMemoryStatusLog();
    logDiskSpace();
    unsigned keyNodeCacheMB = (unsigned)getWorkUnitValueInt("keyNodeCacheMB", 0);
    if (keyNodeCacheMB)
    {
        oldNodeCacheMem = setNodeCacheMem(keyNodeCacheMB * 0x100000);
        PROGLOG("Key node cache size set to: %d MB", keyNodeCacheMB);
    }
    unsigned keyFileCacheLimit = (unsigned)getWorkUnitValueInt("keyFileCacheLimit", 0);
    if (!keyFileCacheLimit)
        keyFileCacheLimit = (querySlaves()+1)*2;
    setKeyIndexCacheSize(keyFileCacheLimit);
    PROGLOG("Key file cache size set to: %d", keyFileCacheLimit);
}

void CJobBase::endJob()
{
    stopPerformanceMonitor();
    LOG(MCdebugProgress, thorJob, "Job ended : %s", graphName.get());
    clearKeyStoreCache(true);
    if (oldNodeCacheMem)
        setNodeCacheMem(oldNodeCacheMem);
    PrintMemoryStatusLog();
}

bool CJobBase::queryForceLogging(graph_id graphId, bool def) const
{
    // JCSMORE, could add comma separated range, e.g. 1-5,10-12
    if ((graphId >= forceLogGraphIdMin) && (graphId <= forceLogGraphIdMax))
        return true;
    return def;
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
    return new CGraphTableIterator(subGraphs);
}

static void getGlobalDeps(CGraphBase &graph, CICopyArrayOf<CGraphDependency> &deps)
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

static void noteDependency(CGraphElementBase *targetActivity, CGraphElementBase *sourceActivity, CGraphBase *targetGraph, CGraphBase *sourceGraph, unsigned controlId)
{
    targetActivity->addDependsOn(sourceGraph, controlId);
    // NB: record dependency in source graph, serialized to slaves, used to decided if should run dependency sinks or not
    Owned<IPropertyTree> dependencyFor = createPTree();
    dependencyFor->setPropInt("@id", sourceActivity->queryId());
    dependencyFor->setPropInt("@graphId", targetGraph->queryGraphId());
    if (controlId)
        dependencyFor->setPropInt("@conditionalId", controlId);
    sourceGraph->queryXGMML().addPropTree("Dependency", dependencyFor.getClear());
}

void CJobBase::addDependencies(IPropertyTree *xgmml, bool failIfMissing)
{
    CGraphArrayCopy childGraphs;
    CGraphElementArrayCopy targetActivities, sourceActivities;

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
            noteDependency(targetActivity, sourceActivity, target, source, controlId);
        }
        else if (edge.getPropBool("att[@name=\"_conditionSource\"]/@value", false))
        { /* Ignore it */ }
        else if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
        {
            // NB: any dependencies of the child acts. are dependencies of this act.
            childGraphs.append(*source);
            targetActivities.append(*targetActivity);
            sourceActivities.append(*sourceActivity);
        }
        else
        {
            if (!edge.getPropBool("att[@name=\"_childGraph\"]/@value", false)) // JCSMORE - not sure if necess. roxie seem to do.
                controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
            noteDependency(targetActivity, sourceActivity, target, source, controlId);
        }
    }
    ForEachItemIn(c, childGraphs)
    {
        CGraphBase &childGraph = childGraphs.item(c);
        CGraphElementBase &targetActivity = targetActivities.item(c);
        CGraphElementBase &sourceActivity = sourceActivities.item(c);
        if (!childGraph.isGlobal())
        {
            CICopyArrayOf<CGraphDependency> globalChildGraphDeps;
            getGlobalDeps(childGraph, globalChildGraphDeps);
            ForEachItemIn(gcd, globalChildGraphDeps)
            {
                CGraphDependency &globalDep = globalChildGraphDeps.item(gcd);
                noteDependency(&targetActivity, &sourceActivity, globalDep.graph, &childGraph, globalDep.controlId);
            }
        }
    }

    SuperHashIteratorOf<CGraphBase> allIter(allGraphs);
    ForEach(allIter)
    {
        CGraphBase &subGraph = allIter.query();
        if (subGraph.queryOwner() && subGraph.queryParentActivityId())
        {
            CGraphElementBase *parentElement = subGraph.queryOwner()->queryElement(subGraph.queryParentActivityId());
            if (isLoopActivity(*parentElement))
            {
                if (!parentElement->queryOwner().isLocalChild() && !subGraph.isLocalOnly())
                    subGraph.setGlobal(true);
            }
        }
        bool log = queryForceLogging(subGraph.queryGraphId(), (NULL == subGraph.queryOwner()) || subGraph.isGlobal());
        subGraph.setLogging(log);
    }
}

void CJobBase::startGraph(CGraphBase &graph, IGraphCallback &callback, bool checkDependencies, size32_t parentExtractSize, const byte *parentExtract)
{
    graphExecutor->add(&graph, callback, checkDependencies, parentExtractSize, parentExtract);
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

// these getX methods for property in workunit settings, then global setting, defaulting to provided 'dft' if not present
StringBuffer &CJobBase::getOpt(const char *opt, StringBuffer &out)
{
    if (!opt || !*opt)
        return out; // probably error
    VStringBuffer gOpt("@%s", opt);
    getWorkUnitValue(opt, out);
    if (0 == out.length())
        globals->getProp(gOpt, out);
    return out;
}

bool CJobBase::getOptBool(const char *opt, bool dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("@%s", opt);
    return getWorkUnitValueBool(opt, globals->getPropBool(gOpt, dft));
}

int CJobBase::getOptInt(const char *opt, int dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("@%s", opt);
    return (int)getWorkUnitValueInt(opt, globals->getPropInt(gOpt, dft));
}

__int64 CJobBase::getOptInt64(const char *opt, __int64 dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("@%s", opt);
    return getWorkUnitValueInt(opt, globals->getPropInt64(gOpt, dft));
}

// IGraphCallback
void CJobBase::runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract)
{
    graph.executeSubGraph(parentExtractSz, parentExtract);
}

IEngineRowAllocator *CJobBase::getRowAllocator(IOutputMetaData * meta, unsigned activityId, roxiemem::RoxieHeapFlags flags) const
{
    return thorAllocator->getRowAllocator(meta, activityId, flags);
}

roxiemem::IRowManager *CJobBase::queryRowManager() const
{
    return thorAllocator->queryRowManager();
}

IThorResult *CJobBase::getOwnedResult(graph_id gid, activity_id ownerId, unsigned resultId)
{
    Owned<CGraphBase> graph = getGraph(gid);
    if (!graph)
    {
        Owned<IThorException> e = MakeThorException(0, "getOwnedResult: graph not found");
        e->setGraphId(gid);
        throw e.getClear();
    }
    Owned<IThorResult> result;
    if (ownerId)
    {
        CGraphElementBase *container = graph->queryElement(ownerId);
        assertex(container);
        CActivityBase *activity = container->queryActivity();
        IThorGraphResults *results = activity->queryResults();
        if (!results)
            throw MakeGraphException(graph, 0, "GraphGetResult: no results created (requesting: %d)", resultId);
        result.setown(activity->queryResults()->getResult(resultId));
    }
    else
        result.setown(graph->getResult(resultId));
    if (!result)
        throw MakeGraphException(graph, 0, "GraphGetResult: result not found: %d", resultId);
    return result.getClear();
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
    abortSoon = receiving = cancelledReceive = reInit = false;
    baseHelper.set(container.queryHelper());
    parentExtractSz = 0;
    parentExtract = NULL;
    // NB: maxCores, currently only used to control # cores used by sorts
    maxCores = container.queryXGMML().getPropInt("hint[@name=\"max_cores\"]/@value", container.queryJob().queryMaxDefaultActivityCores());
}

CActivityBase::~CActivityBase()
{
}

void CActivityBase::abort()
{
    if (!abortSoon) ActPrintLog("Abort condition set");
    abortSoon = true;
}

void CActivityBase::kill()
{
    ownedResults.clear();
}

bool CActivityBase::appendRowXml(StringBuffer & target, IOutputMetaData & meta, const void * row) const
{
    if (!meta.hasXML())
    {
        target.append("<xml-unavailable/>");
        return false;
    }

    try
    {
        CommonXmlWriter xmlWrite(XWFnoindent);
        meta.toXML((byte *) row, xmlWrite);
        target.append(xmlWrite.str());
        return true;
    }
    catch (IException * e)
    {
        e->Release();
        target.append("<invalid-row/>");
        return false;
    }
}

void CActivityBase::logRow(const char * prefix, IOutputMetaData & meta, const void * row)
{
    bool blindLogging = false; // MORE: should check a workunit/global option
    if (meta.hasXML() && !blindLogging)
    {
        StringBuffer xml;
        appendRowXml(xml, meta, row);
        ActPrintLog("%s: %s", prefix, xml.str());
    }
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

void CActivityBase::ActPrintLog(IException *e)
{
    ActPrintLog(e, "%s", "");
}

bool CActivityBase::fireException(IException *e)
{
    Owned<IThorException> _te;
    IThorException *te = QUERYINTERFACE(e, IThorException);
    if (te)
    {
        if (!te->queryActivityId())
            setExceptionActivityInfo(container, te);
    }
    else
    {
        te = MakeActivityException(this, e);
        te->setAudience(e->errorAudience());
        _te.setown(te);
    }
    return container.queryOwner().fireException(te);
}

void CActivityBase::processAndThrowOwnedException(IException * _e)
{
    IThorException *e = QUERYINTERFACE(_e, IThorException);
    if (e)
    {
        if (!e->queryActivityId())
            setExceptionActivityInfo(container, e);
    }
    else
    {
        e = MakeActivityException(this, _e);
        _e->Release();
    }
    if (!e->queryNotified())
    {
        fireException(e);
        e->setNotified();
    }
    throw e;
}


IEngineRowAllocator * CActivityBase::queryRowAllocator()
{
    if (CABallocatorlock.lock()) {
        if (!rowAllocator)
            rowAllocator.setown(queryJob().getRowAllocator(queryRowMetaData(),queryActivityId()));
        CABallocatorlock.unlock();
    }
    return rowAllocator;
}
    
IOutputRowSerializer * CActivityBase::queryRowSerializer()
{
    if (CABserializerlock.lock()) {
        if (!rowSerializer)
            rowSerializer.setown(queryRowMetaData()->createDiskSerializer(queryCodeContext(),queryActivityId()));
        CABserializerlock.unlock();
    }
    return rowSerializer;
}

IOutputRowDeserializer * CActivityBase::queryRowDeserializer()
{
    if (CABdeserializerlock.lock()) {
        if (!rowDeserializer)
            rowDeserializer.setown(queryRowMetaData()->createDiskDeserializer(queryCodeContext(),queryActivityId()));
        CABdeserializerlock.unlock();
    }
    return rowDeserializer;
}

IRowInterfaces *CActivityBase::getRowInterfaces()
{
    // create an independent instance, to avoid circular link dependency problems
    return createRowInterfaces(queryRowMetaData(), container.queryId(), queryCodeContext());
}

bool CActivityBase::receiveMsg(CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender, unsigned timeout)
{
    BooleanOnOff onOff(receiving);
    CTimeMon t(timeout);
    unsigned remaining = timeout;
    // check 'cancelledReceive' every 10 secs
    while (!cancelledReceive && ((MP_WAIT_FOREVER==timeout) || !t.timedout(&remaining)))
    {
        if (container.queryJob().queryJobComm().recv(mb, rank, mpTag, sender, remaining>10000?10000:remaining))
            return true;
    }
    return false;
}

void CActivityBase::cancelReceiveMsg(const rank_t rank, const mptag_t mpTag)
{
    cancelledReceive = true;
    if (receiving)
        container.queryJob().queryJobComm().cancel(rank, mpTag);
}

StringBuffer &CActivityBase::getOpt(const char *prop, StringBuffer &out) const
{
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    if (!container.queryXGMML().getProp(path.toLowerCase().str(), out))
        queryJob().getOpt(prop, out);
    return out;
}

bool CActivityBase::getOptBool(const char *prop, bool defVal) const
{
    bool def = queryJob().getOptBool(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return container.queryXGMML().getPropBool(path.toLowerCase().str(), def);
}

int CActivityBase::getOptInt(const char *prop, int defVal) const
{
    int def = queryJob().getOptInt(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return container.queryXGMML().getPropInt(path.toLowerCase().str(), def);
}

__int64 CActivityBase::getOptInt64(const char *prop, __int64 defVal) const
{
    __int64 def = queryJob().getOptInt64(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return container.queryXGMML().getPropInt64(path.toLowerCase().str(), def);
}
