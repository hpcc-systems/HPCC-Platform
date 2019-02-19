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
#include "rtlformat.hpp"


PointerArray createFuncs;
void registerCreateFunc(CreateFunc func)
{
    createFuncs.append((void *)func);
}

///////////////////////////////////
    
//////

/////

class CThorGraphResult : implements IThorResult, implements IRowWriter, public CInterface
{
    CActivityBase &activity;
    rowcount_t rowStreamCount;
    IOutputMetaData *meta;
    Owned<IRowWriterMultiReader> rowBuffer;
    IThorRowInterfaces *rowIf;
    IEngineRowAllocator *allocator;
    bool stopped, readers;
    ThorGraphResultType resultType;

    void init()
    {
        stopped = readers = false;
        allocator = rowIf->queryRowAllocator();
        meta = allocator->queryOutputMeta();
        rowStreamCount = 0;
    }
    class CStreamWriter : implements IRowWriterMultiReader, public CSimpleInterface
    {
        CThorGraphResult &owner;
        CThorExpandingRowArray rows;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CStreamWriter(CThorGraphResult &_owner, EmptyRowSemantics emptyRowSemantics) : owner(_owner), rows(owner.activity, owner.rowIf, emptyRowSemantics)
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

    CThorGraphResult(CActivityBase &_activity, IThorRowInterfaces *_rowIf, ThorGraphResultType _resultType, unsigned spillPriority) : activity(_activity), rowIf(_rowIf), resultType(_resultType)
    {
        init();
        EmptyRowSemantics emptyRowSemantics;
        if (isGrouped())
            emptyRowSemantics = ers_eogonly;
        else if (isSparse())
            emptyRowSemantics = ers_allow;
        else
            emptyRowSemantics = ers_forbidden;
        if (SPILL_PRIORITY_DISABLE == spillPriority)
            rowBuffer.setown(new CStreamWriter(*this, emptyRowSemantics));
        else
            rowBuffer.setown(createOverflowableBuffer(activity, rowIf, emptyRowSemantics, true));
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
    virtual IThorRowInterfaces *queryRowInterfaces() { return rowIf; }
    virtual CActivityBase *queryActivity() { return &activity; }
    virtual bool isDistributed() const { return resultType & thorgraphresult_distributed; }
    virtual bool isSparse() const { return resultType & thorgraphresult_sparse; }
    virtual bool isGrouped() const { return resultType & thorgraphresult_grouped; }
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
                for (;;)
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
            for (;;)
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
    virtual void getLinkedResult(unsigned &countResult, const byte * * & result) override
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
        result = (const byte **)_rowset.getClear();
    }
    virtual const void * getLinkedRowResult()
    {
        assertex(rowStreamCount==1); // catch, just in case
        Owned<IRowStream> stream = getRowStream();
        return stream->nextRow();
    }
};

/////

IThorResult *CThorGraphResults::createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    Owned<IThorResult> result = ::createResult(activity, rowIf, resultType, spillPriority);
    setResult(id, result);
    return result;
}

/////

IThorResult *createResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    return new CThorGraphResult(activity, rowIf, resultType, spillPriority);
}

/////
class CThorBoundLoopGraph : implements IThorBoundLoopGraph, public CInterface
{
    CGraphBase *graph;
    activity_id activityId;
    Linked<IOutputMetaData> resultMeta;
    Owned<IOutputMetaData> counterMeta, loopAgainMeta;
    Owned<IThorRowInterfaces> resultRowIf, countRowIf, loopAgainRowIf;

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
            countRowIf.setown(activity.createRowInterfaces(counterMeta));
        RtlDynamicRowBuilder counterRow(countRowIf->queryRowAllocator());
        thor_loop_counter_t * res = (thor_loop_counter_t *)counterRow.ensureCapacity(sizeof(thor_loop_counter_t),NULL);
        *res = loopCounter;
        OwnedConstThorRow counterRowFinal = counterRow.finalizeRowClear(sizeof(thor_loop_counter_t));
        IThorResult *counterResult = results->createResult(activity, pos, countRowIf, thorgraphresult_nul, SPILL_PRIORITY_DISABLE);
        Owned<IRowWriter> counterResultWriter = counterResult->getWriter();
        counterResultWriter->putRow(counterRowFinal.getClear());
        graph->setLoopCounter(loopCounter);
    }
    virtual void prepareLoopAgainResult(CActivityBase &activity, IThorGraphResults *results, unsigned pos)
    {
        if (!loopAgainRowIf)
            loopAgainRowIf.setown(activity.createRowInterfaces(loopAgainMeta));
        activity.queryGraph().createResult(activity, pos, results, loopAgainRowIf, activity.queryGraph().isLocalChild() ? thorgraphresult_nul : thorgraphresult_distributed, SPILL_PRIORITY_DISABLE);
    }
    virtual void prepareLoopResults(CActivityBase &activity, IThorGraphResults *results)
    {
        if (!resultRowIf)
            resultRowIf.setown(activity.createRowInterfaces(resultMeta));
        ThorGraphResultType resultType = activity.queryGraph().isLocalChild() ? thorgraphresult_nul : thorgraphresult_distributed;
        IThorResult *loopResult =  activity.queryGraph().createResult(activity, 0, results, resultRowIf, resultType); // loop output
        IThorResult *inputResult = activity.queryGraph().createResult(activity, 1, results, resultRowIf, resultType); // loop input
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
        case TAKspillread:
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
    onCreateCalled = prepared = haveCreateCtx = nullAct = false;
    onlyUpdateIfChanged = xgmml->getPropBool("att[@name=\"_updateIfChanged\"]/@value", false);

    StringBuffer helperName("fAc");
    xgmml->getProp("@id", helperName);
    helperFactory = (EclHelperFactory) queryJob().queryDllEntry().getEntry(helperName.str());
    if (!helperFactory)
        throw makeOsExceptionV(GetLastError(), "Failed to load helper factory method: %s (dll handle = %p)", helperName.str(), queryJob().queryDllEntry().getInstance());
    alreadyUpdated = false;
    whichBranch = (unsigned)-1;
    log = true;
    sentActInitData.setown(createThreadSafeBitSet());
    maxCores = queryXGMML().getPropInt("hint[@name=\"max_cores\"]/@value", 0);
    if (0 == maxCores)
        maxCores = queryJob().queryMaxDefaultActivityCores();
    baseHelper.setown(helperFactory());
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

CJobChannel &CGraphElementBase::queryJobChannel() const
{
    return owner->queryJobChannel();
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

StringBuffer &CGraphElementBase::getOpt(const char *prop, StringBuffer &out) const
{
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    if (!queryXGMML().getProp(path.toLowerCase().str(), out))
        queryJob().getOpt(prop, out);
    return out;
}

bool CGraphElementBase::getOptBool(const char *prop, bool defVal) const
{
    bool def = queryJob().getOptBool(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return queryXGMML().getPropBool(path.toLowerCase().str(), def);
}

int CGraphElementBase::getOptInt(const char *prop, int defVal) const
{
    int def = queryJob().getOptInt(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return queryXGMML().getPropInt(path.toLowerCase().str(), def);
}

__int64 CGraphElementBase::getOptInt64(const char *prop, __int64 defVal) const
{
    __int64 def = queryJob().getOptInt64(prop, defVal);
    VStringBuffer path("hint[@name=\"%s\"]/@value", prop);
    return queryXGMML().getPropInt64(path.toLowerCase().str(), def);
}

IThorGraphDependencyIterator *CGraphElementBase::getDependsIterator() const
{
    return new ArrayIIteratorOf<const CGraphDependencyArray, CGraphDependency, IThorGraphDependencyIterator>(dependsOn);
}

void CGraphElementBase::reset()
{
    alreadyUpdated = false;
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
}

void CGraphElementBase::doconnect()
{
    ForEachItemIn(i, connectedInputs)
    {
        CIOConnection *io = connectedInputs.item(i);
        if (io)
            io->connect(i, queryActivity());
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

void CGraphElementBase::serializeCreateContext(MemoryBuffer &mb)
{
    if (!onCreateCalled) return;
    DelayedSizeMarker sizeMark(mb);
    queryHelper()->serializeCreateContext(mb);
    sizeMark.write();
    if (isSink())
        mb.append(alreadyUpdated);
}

void CGraphElementBase::deserializeCreateContext(MemoryBuffer &mb)
{
    size32_t createCtxLen;
    mb.read(createCtxLen);
    createCtxMb.clear().append(createCtxLen, mb.readDirect(createCtxLen));
    haveCreateCtx = true;
    if (isSink())
        mb.read(alreadyUpdated);
}

void CGraphElementBase::serializeStartContext(MemoryBuffer &mb)
{
    DelayedSizeMarker sizeMark(mb);
    queryHelper()->serializeStartContext(mb);
    sizeMark.write();
}

void CGraphElementBase::onCreate()
{
    CriticalBlock b(crit);
    if (onCreateCalled)
        return;
    onCreateCalled = true;
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
        if (isLoopActivity(*this))
        {
            unsigned loopId = queryXGMML().getPropInt("att[@name=\"_loopid\"]/@value");
            Owned<CGraphStub> stub = owner->getChildGraph(loopId);
            Owned<IThorBoundLoopGraph> boundLoopGraph = createBoundLoopGraph(&stub->queryOriginalGraph(), baseHelper->queryOutputMeta(), queryId());
            setBoundGraph(boundLoopGraph);
        }
    }
}

void CGraphElementBase::onStart(size32_t parentExtractSz, const byte *parentExtract, MemoryBuffer *startCtx)
{
    if (nullAct)
        return;
    CriticalBlock b(crit);
    baseHelper->onStart(parentExtract, startCtx);
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

bool CGraphElementBase::prepareContext(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async, bool connectOnly)
{
    try
    {
        bool create = true;
        if (connectOnly)
        {
            if (prepared)
                return true;
            ForEachItemIn(i, inputs)
            {
                if (!queryInput(i)->prepareContext(parentExtractSz, parentExtract, false, false, async, true))
                    return false;
            }
        }
        else
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
            switch (getKind())
            {
                case TAKindexwrite:
                case TAKdiskwrite:
                case TAKcsvwrite:
                case TAKxmlwrite:
                case TAKjsonwrite:
                case TAKspillwrite:
                    if (_shortCircuit) return true;
                    onCreate();
                    alreadyUpdated = checkUpdate();
                    if (alreadyUpdated)
                        return false;
                    break;
                case TAKchildif:
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
                        create = false;
                    }
                    break;
                }
                case TAKchildcase:
                case TAKcase:
                {
                    if (_shortCircuit) return true;
                    onCreate();
                    onStart(parentExtractSz, parentExtract);
                    IHThorCaseArg *helper = (IHThorCaseArg *)baseHelper.get();
                    whichBranch = helper->getBranch();
                    if (whichBranch >= inputs.ordinality())
                        whichBranch = inputs.ordinality()-1;
                    break;
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
                            whichBranch = ((IHThorFilterArg *)baseHelper.get())->canMatchAny() ? 0 : 1;
                            break;
                        case TAKfiltergroup:
                            whichBranch = ((IHThorFilterGroupArg *)baseHelper.get())->canMatchAny() ? 0 : 1;
                            break;
                        case TAKfilterproject:
                            whichBranch = ((IHThorFilterProjectArg *)baseHelper.get())->canMatchAny() ? 0 : 1;
                            break;
                    }
                    break;
                }
                case TAKsequential:
                case TAKparallel:
                {
                    /* NB - The executeDependencies code below is only needed if actionLinkInNewGraph=true, which is no longer the default
                     * It should be removed, once we are positive there are no issues with in-line sequential/parallel activities
                     */
                    for (unsigned s=1; s<=dependsOn.ordinality(); s++)
                        executeDependencies(parentExtractSz, parentExtract, s, async);
                    create = false;
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
            if (checkDependencies && ((unsigned)-1 != whichBranch))
            {
                if (inputs.queryItem(whichBranch))
                {
                    if (!queryInput(whichBranch)->prepareContext(parentExtractSz, parentExtract, true, false, async, connectOnly))
                        return false;
                }
                ForEachItemIn(i, inputs)
                {
                    if (i != whichBranch)
                    {
                        if (!queryInput(i)->prepareContext(parentExtractSz, parentExtract, false, false, async, true))
                            return false;
                    }
                }
            }
            else
            {
                ForEachItemIn(i, inputs)
                {
                    if (!queryInput(i)->prepareContext(parentExtractSz, parentExtract, checkDependencies, false, async, connectOnly))
                        return false;
                }
            }
        }
        if (create)
        {
            if (prepared) // no need to recreate
                return true;
            prepared = true;
            ForEachItemIn(i2, inputs)
            {
                CIOConnection *inputIO = inputs.item(i2);
                connectInput(i2, inputIO->activity, inputIO->index);
            }
            createActivity();
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

void CGraphElementBase::createActivity()
{
    CriticalBlock b(crit);
    if (activity)
        return;
    activity.setown(factory());
    if (isSink())
        owner->addActiveSink(*this);
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
        case TAKspillwrite:
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
        case TAKexternalsink:
        case TAKexternalsource:
        case TAKexternalprocess:
            return false;
// dependent on local/grouped
        case TAKkeyeddistribute:
        case TAKhashdistribute:
        case TAKhashdistributemerge:
        case TAKnwaydistribute:
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
        case TAKfiltergroup:
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
        case TAKspillread:

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
        case TAKtrace:
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

static void addDependencies(IPropertyTree *xgmml, bool failIfMissing, CGraphTableCopy &graphs)
{
    CGraphArrayCopy dependentchildGraphs;
    CGraphElementArrayCopy targetActivities, sourceActivities;

    Owned<IPropertyTreeIterator> iter = xgmml->getElements("edge");
    ForEach(*iter)
    {
        IPropertyTree &edge = iter->query();
        graph_id sourceGid = edge.getPropInt("@source");
        graph_id targetGid = edge.getPropInt("@target");
        Owned<CGraphBase> source = LINK(graphs.find(sourceGid));
        Owned<CGraphBase> target = LINK(graphs.find(targetGid));
        if (!source || !target)
        {
            if (failIfMissing)
                throwUnexpected();
            else
                continue; // expected if assigning dependencies in slaves
        }
        CGraphElementBase *targetActivity = (CGraphElementBase *)target->queryElement(edge.getPropInt("att[@name=\"_targetActivity\"]/@value"));
        CGraphElementBase *sourceActivity = (CGraphElementBase *)source->queryElement(edge.getPropInt("att[@name=\"_sourceActivity\"]/@value"));
        if (!edge.getPropBool("att[@name=\"_childGraph\"]/@value"))
        {
            if (TAKlocalresultwrite == sourceActivity->getKind() && (TAKlocalresultread != targetActivity->getKind()))
            {
                if (source->isLoopSubGraph())
                    source->setGlobal(true);
            }
        }
        int controlId = 0;
        if (edge.getPropBool("att[@name=\"_dependsOn\"]/@value", false))
        {
            if (!edge.getPropBool("att[@name=\"_childGraph\"]/@value", false)) // JCSMORE - not sure if necess. roxie seem to do.
                controlId = edge.getPropInt("att[@name=\"_when\"]/@value", 0);
            CGraphBase &sourceGraph = sourceActivity->queryOwner();
            unsigned sourceGraphContext = sourceGraph.queryParentActivityId();

            CGraphBase *targetGraph = NULL;
            unsigned targetGraphContext = -1;
            for (;;)
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
            dependentchildGraphs.append(*source);
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
    ForEachItemIn(c, dependentchildGraphs)
    {
        CGraphBase &childGraph = dependentchildGraphs.item(c);
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

    SuperHashIteratorOf<CGraphBase> allIter(graphs);
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
    }
}

void traceMemUsage()
{
    StringBuffer memStatsStr;
    roxiemem::memstats(memStatsStr);
    PROGLOG("Roxiemem stats: %s", memStatsStr.str());
    memsize_t heapUsage = getMapInfo("heap");
    if (heapUsage) // if 0, assumed to be unavailable
    {
        memsize_t rmtotal = roxiemem::getTotalMemoryLimit();
        PROGLOG("Heap usage (excluding Roxiemem) : %" I64F "d bytes", (unsigned __int64)(heapUsage-rmtotal));
    }
}

/////

CGraphBase::CGraphBase(CJobChannel &_jobChannel) : jobChannel(_jobChannel), job(_jobChannel.queryJob()), progressUpdated(false)
{
    xgmml = NULL;
    parent = owner = graphResultsContainer = NULL;
    complete = false;
    parentActivityId = 0;
    connected = started = graphDone = aborted = false;
    startBarrier = waitBarrier = doneBarrier = NULL;
    mpTag = waitBarrierTag = startBarrierTag = doneBarrierTag = TAG_NULL;
    executeReplyTag = TAG_NULL;
    parentExtractSz = 0;
    counter = 0; // loop/graph counter, will be set by loop/graph activity if needed
    loopBodySubgraph = false;
}

CGraphBase::~CGraphBase()
{
    clean();
}

CGraphBase *CGraphBase::cloneGraph()
{
    Owned<CGraphBase> subGraph = queryJobChannel().createGraph();
    CGraphTableCopy newGraphs;
    subGraph->createFromXGMML(node, owner, parent, graphResultsContainer, newGraphs);
    addDependencies(queryJob().queryXGMML(), false, newGraphs);
    return subGraph.getClear();
}

void CGraphBase::init()
{
    bool log = queryJob().queryForceLogging(queryGraphId(), (NULL == queryOwner()) || isGlobal());
    setLogging(log);
}

void CGraphBase::clean()
{
    ::Release(startBarrier);
    ::Release(waitBarrier);
    ::Release(doneBarrier);
    localResults.clear();
    graphLoopResults.clear();
    childGraphsTable.releaseAll();
    disconnectActivities();
    containers.releaseAll();
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

void CGraphBase::deserializeCreateContexts(MemoryBuffer &mb)
{
    activity_id id;
    for (;;)
    {
        mb.read(id);
        if (0 == id) break;
        CGraphElementBase *element = queryElement(id);
        assertex(element);
        element->deserializeCreateContext(mb);
    }
}

void CGraphBase::reset()
{
    setCompleteEx(false);
    clearProgressUpdated();
    graphCancelHandler.reset();
    if (0 == containers.count())
    {
        Owned<IThorGraphIterator> iter = getChildGraphIterator();
        ForEach(*iter)
            iter->query().reset();
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
    if (!queryOwner())
        clearNodeStats();
}

void CGraphBase::addChildGraph(CGraphStub *stub)
{
    CriticalBlock b(crit);
    childGraphsTable.replace(*LINK(stub));
    if (sequential)
        orderedChildGraphs.append(*stub);
}

IThorGraphStubIterator *CGraphBase::getChildStubIterator() const
{
    CriticalBlock b(crit);
    class CIter : private SuperHashIteratorOf<CGraphStub>, public CSimpleInterfaceOf<IThorGraphStubIterator>
    {
        typedef SuperHashIteratorOf<CGraphStub> PARENT;
    public:
        CIter(const CChildGraphTable &table) : PARENT(table) { }
    // IIterator
        virtual bool first() { return PARENT::first(); }
        virtual bool next() { return PARENT::next(); }
        virtual bool isValid() { return PARENT::isValid(); }
        virtual CGraphStub &query() { return PARENT::query(); }
    };
    return new CIter(childGraphsTable);
}

IThorGraphIterator *CGraphBase::getChildGraphIterator() const
{
    CriticalBlock b(crit);
    class CIter : public CSimpleInterfaceOf<IThorGraphIterator>
    {
        Owned<IThorGraphStubIterator> iter;
    public:
        CIter(IThorGraphStubIterator *_iter) : iter(_iter)
        {
        }
    // IIterator
        virtual bool first() { return iter->first(); }
        virtual bool next() { return iter->next(); }
        virtual bool isValid() { return iter->isValid(); }
        virtual CGraphBase &query()
        {
            CGraphStub &stub = iter->query();
            return stub.queryOriginalGraph();
        }
    };
    return new CIter(getChildStubIterator());
}

bool CGraphBase::fireException(IException *e)
{
    return queryJobChannel().fireException(e);
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
    CriticalBlock b(executeCrit);
    if (job.queryPausing())
        return;
    Owned<IException> exception;
    try
    {
        if (!queryOwner())
        {
            StringBuffer s;
            toXML(&queryXGMML(), s, 2);
            GraphPrintLog("Running graph [%s] : %s", isGlobal()?"global":"local", s.str());
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

void CGraphBase::onCreate()
{
    Owned<IThorActivityIterator> iter = getConnectedIterator();
    ForEach(*iter)
    {
        CGraphElementBase &element = iter->query();
        element.onCreate();
    }
}

void CGraphBase::execute(size32_t _parentExtractSz, const byte *parentExtract, bool checkDependencies, bool async)
{
    if (isComplete())
        return;
    if (async)
        queryJobChannel().startGraph(*this, checkDependencies, _parentExtractSz, parentExtract); // may block if enough running
    else
    {
        if (!prepare(_parentExtractSz, parentExtract, checkDependencies, false, false))
        {
            setComplete();
            return;
        }
        executeSubGraph(_parentExtractSz, parentExtract);
    }
}

void CGraphBase::doExecute(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies)
{
    if (isComplete()) return;
    if (queryAborted())
    {
        if (abortException)
            throw abortException.getLink();
        throw MakeGraphException(this, 0, "subgraph aborted");
    }
    GraphPrintLog("Processing graph");
    Owned<IException> exception;
    try
    {
        if (started)
            reset();
        else
            started = true;
        ++numExecuted;
        Owned<IThorActivityIterator> iter = getConnectedIterator();
        ForEach(*iter)
        {
            CGraphElementBase &element = iter->query();
            element.onStart(parentExtractSz, parentExtract);
            element.initActivity();
        }
        initialized = true;
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
    ForEachItemIn(s, sinks)
    {
        CGraphElementBase &sink = sinks.item(s);
        if (sink.prepareContext(parentExtractSz, parentExtract, checkDependencies, shortCircuit, async, false))
            needToExecute = true;
    }
    onCreate();
    return needToExecute;
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

unsigned CGraphBase::queryJobChannelNumber() const
{
    return queryJobChannel().queryChannel();
}

IMPServer &CGraphBase::queryMPServer() const
{
    return jobChannel.queryMPServer();
}

bool CGraphBase::syncInitData()
{
    if (loopBodySubgraph)
    {
        CGraphElementBase *parentElement = queryOwner() ? queryOwner()->queryElement(queryParentActivityId()) : nullptr;
        assertex(parentElement);
        return parentElement->queryLoopGraph()->queryGraph()->isGlobal();
    }
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

class CGraphTraverseIteratorBase : implements IThorActivityIterator, public CInterface
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
    void setNext(bool branchOnConditional)
    {
        if (branchOnConditional && ((unsigned)-1) != cur->whichBranch)
        {
            CIOConnection *io = cur->connectedInputs.queryItem(cur->whichBranch);
            if (io)
                cur.set(io->activity);
            else
                cur.clear();
        }
        else
        {
            CIOConnectionArray &inputs = cur->connectedInputs;
            cur.clear();
            unsigned n = inputs.ordinality();
            bool first = true;
            for (unsigned i=0; i<n; i++)
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
                return;
        }
        // check haven't been here before
        for (;;)
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
                return;
        }
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
        for (;;)
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
    bool branchOnConditional;
public:
    CGraphTraverseConnectedIterator(CGraphBase &graph, bool _branchOnConditional) : CGraphTraverseIteratorBase(graph), branchOnConditional(_branchOnConditional) { }
    virtual bool next()
    {
        setNext(branchOnConditional);
        return NULL!=cur.get();
    }
};

IThorActivityIterator *CGraphBase::getConnectedIterator(bool branchOnConditional)
{
    return new CGraphTraverseConnectedIterator(*this, branchOnConditional);
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
            Owned<IThorGraphStubIterator> iter = getChildStubIterator();
            ForEach(*iter)
            {
                CGraphStub &graph = iter->query();
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

void CGraphBase::createFromXGMML(IPropertyTree *_node, CGraphBase *_owner, CGraphBase *_parent, CGraphBase *resultsGraph, CGraphTableCopy &newGraphs)
{
    class CChildParallelFactory : public CGraphStub
    {
        Linked<CGraphBase> originalChildGraph;
        CriticalSection crit;
        CIArrayOf<CGraphBase> stack;
        CIArrayOf<CGraphBase> active;
        bool originalAvailable = true;

        CGraphBase *getGraph()
        {
            Owned<CGraphBase> childGraph;
            {
                CriticalBlock b(crit);
                if (originalAvailable)
                {
                    originalAvailable = false;
                    active.append(*originalChildGraph.getLink());
                    return originalChildGraph.getLink();
                }
                if (stack.length())
                    childGraph.setown(&stack.popGet());
            }
            if (!childGraph)
                childGraph.setown(originalChildGraph->cloneGraph());
            if (originalChildGraph->queryAborted())
                throw MakeGraphException(originalChildGraph, 0, "Job aborted");

            {
                CriticalBlock b(crit);
                active.append(*childGraph.getLink());
            }
            return childGraph.getClear();
        }
        void pushGraph(CGraphBase *childGraph)
        {
            CriticalBlock b(crit);
            verifyex(active.zap(*childGraph));
            if (childGraph == originalChildGraph)
                originalAvailable = true;
            else
                stack.append(*LINK(childGraph));
        }
    public:
        CChildParallelFactory(CGraphBase *_originalChildGraph) : originalChildGraph(_originalChildGraph)
        {
            graphId = originalChildGraph->queryGraphId();
        }
        virtual CGraphBase &queryOriginalGraph() override { return *originalChildGraph; }
        virtual void abort(IException *e) override
        {
            for (;;)
            {
                Owned<CGraphBase> activeChildGraph;
                {
                    CriticalBlock b(crit);
                    activeChildGraph.setown(&active.popGet());
                    if (!activeChildGraph)
                        break;
                }
                activeChildGraph->abort(e);
            }
        }
        virtual bool serializeStats(MemoryBuffer &mb) override
        {
            // JCSMORE - need to merge other instances
            return originalChildGraph->serializeStats(mb);
        }
        virtual IEclGraphResults * evaluate(unsigned parentExtractSz, const byte * parentExtract) override
        {
            Owned<CGraphBase> childGraph = getGraph();
            Owned<IEclGraphResults> results = childGraph->evaluate(parentExtractSz, parentExtract);
            pushGraph(childGraph);
            return results.getClear();
        }
    };
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

    graphResultsContainer = resultsGraph;
    CGraphBase *graphContainer = this;
    if (resultsGraph)
        graphContainer = resultsGraph; // JCSMORE is this right?

    graphCodeContext.setContext(this, graphContainer, (ICodeContextExt *)&jobChannel.queryCodeContext());


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
        if (isLoopActivity(*parentElement))
        {
            localChild = parentElement->queryOwner().isLocalChild();
            unsigned loopId = parentElement->queryXGMML().getPropInt("att[@name=\"_loopid\"]/@value");
            if ((graphId == loopId) || (owner->queryGraphId() == loopId))
                loopBodySubgraph = true;
            else
                localChild = true;
        }
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
            Owned<CGraphBase> subGraph = queryJobChannel().createGraph();
            subGraph->createFromXGMML(&e, this, parent, resultsGraph, newGraphs);

            activity_id subGraphParentActivityId = e.getPropInt("att[@name=\"_parentActivity\"]/@value", 0);
            if (subGraphParentActivityId) // JCS - not sure if ever false
            {
                Owned<CGraphStub> stub = new CChildParallelFactory(subGraph);
                addChildGraph(stub);
            }
            else
                addChildGraph(subGraph);
            if (!global)
                global = subGraph->isGlobal();
            newGraphs.replace(*subGraph);
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
        //Ignore edges that represent dependencies from parent activities to child activities.
        if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
            continue;

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
    if (sequential)
    {
        // JCSMORE - would need to re-think how this is done if these sibling child queries could be executed in parallel
        ForEachItemIn(o, orderedChildGraphs)
        {
            CGraphBase &graph = orderedChildGraphs.item(o).queryOriginalGraph();
            if (graph.isSink())
                graph.execute(parentExtractSz, parentExtract, true, false);
        }
    }
    else
    {
        Owned<IThorGraphIterator> iter = getChildGraphIterator();
        ForEach(*iter)
        {
            CGraphBase &graph = iter->query();
            if (graph.isSink())
                graph.execute(parentExtractSz, parentExtract, true, false);
        }
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
    Owned<IThorGraphIterator> childIter = graph.getChildGraphIterator();
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

IThorResult *CGraphBase::createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    return results->createResult(activity, id, rowIf, resultType, spillPriority);
}

IThorResult *CGraphBase::createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    return localResults->createResult(activity, id, rowIf, resultType, spillPriority);
}

IThorResult *CGraphBase::createGraphLoopResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority)
{
    return graphLoopResults->createResult(activity, rowIf, resultType, spillPriority);
}

// IEclGraphResults
void CGraphBase::getDictionaryResult(unsigned & count, const byte * * & ret, unsigned id)
{
    Owned<IThorResult> result = getResult(id, true); // will get collated distributed result
    result->getLinkedResult(count, ret);
}

void CGraphBase::getLinkedResult(unsigned & count, const byte * * & ret, unsigned id)
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
        Owned<IThorGraphIterator> iter = graph.getChildGraphIterator();
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
        IPropertyTree &edge = inputs->query();
        //Ignore edges that represent dependencies from parent activities to child activities.
        if (edge.getPropBool("att[@name=\"_childGraph\"]/@value", false))
            continue;
        CGraphElementBase *sourceAct = activity.queryOwner().queryElement(edge.getPropInt("@source"));
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
                LOG(MCuserWarning, unknownJob, "Failed to delete tmp file : %s (not found)", name);
        }
        catch (IException *e) { StringBuffer s("Failed to delete tmp file : "); FLLOG(MCuserWarning, thorJob, e, s.append(name).str()); }
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
                LOG(MCuserWarning, thorJob, "Failed to delete tmp file : %s (not found)", tmpname);
        }
        catch (IException *e) { StringBuffer s("Failed to delete tmp file : "); FLLOG(MCuserWarning, thorJob, e, s.append(tmpname).str()); }
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
class CGraphExecutor : implements IGraphExecutor, public CInterface
{
    CJobChannel &jobChannel;
    CJobBase &job;
    CIArrayOf<CGraphExecutorGraphInfo> stack, running, toRun;
    UnsignedArray seen;
    bool stopped;
    unsigned limit;
    unsigned waitOnRunning;
    CriticalSection crit;
    Semaphore runningSem;
    Owned<IThreadPool> graphPool;

    class CGraphExecutorFactory : implements IThreadFactory, public CInterface
    {
        CGraphExecutor &executor;
    public:
        IMPLEMENT_IINTERFACE;

        CGraphExecutorFactory(CGraphExecutor &_executor) : executor(_executor) { }
// IThreadFactory
        virtual IPooledThread *createNew()
        {
            class CGraphExecutorThread : implements IPooledThread, public CInterface
            {
                Owned<CGraphExecutorGraphInfo> graphInfo;
            public:
                IMPLEMENT_IINTERFACE;
                CGraphExecutorThread()
                {
                }
                virtual void init(void *startInfo) override
                {
                    graphInfo.setown((CGraphExecutorGraphInfo *)startInfo);
                }
                virtual void threadmain() override
                {
                    for (;;)
                    {
                        Linked<CGraphBase> graph = graphInfo->subGraph;
                        Owned<IException> e;
                        try
                        {
                            PROGLOG("CGraphExecutor: Running graph, graphId=%" GIDPF "d", graph->queryGraphId());
                            graphInfo->callback.runSubgraph(*graph, graphInfo->parentExtractMb.length(), (const byte *)graphInfo->parentExtractMb.toByteArray());
                        }
                        catch (IException *_e)
                        {
                            e.setown(_e);
                        }
                        Owned<CGraphExecutorGraphInfo> nextGraphInfo;
                        try
                        {
                            nextGraphInfo.setown(graphInfo->executor.graphDone(*graphInfo, e));
                        }
                        catch (IException *e)
                        {
                            GraphPrintLog(graph, e, "graphDone");
                            e->Release();
                        }
                        graphInfo.clear(); // NB: at this point the graph will be destroyed
                        if (e)
                            throw e.getClear();
                        if (!nextGraphInfo)
                            return;
                        graphInfo.setown(nextGraphInfo.getClear());
                    }
                }
                virtual bool canReuse() const override { return true; }
                virtual bool stop() override { return true; }
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

    CGraphExecutor(CJobChannel &_jobChannel) : jobChannel(_jobChannel), job(_jobChannel.queryJob())
    {
        limit = (unsigned)job.getWorkUnitValueInt("concurrentSubGraphs", globals->getPropInt("@concurrentSubGraphs", 1));
        PROGLOG("CGraphExecutor: limit = %d", limit);
        waitOnRunning = 0;
        stopped = false;
        factory = new CGraphExecutorFactory(*this);
        graphPool.setown(createThreadPool("CGraphExecutor pool", factory, &jobChannel, limit));
    }
    ~CGraphExecutor()
    {
        stopped = true;
        graphPool->joinAll();
        factory->Release();
    }
    CGraphExecutorGraphInfo *graphDone(CGraphExecutorGraphInfo &doneGraphInfo, IException *e)
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
            return NULL;
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

        while (toRun.ordinality())
        {
            if (job.queryPausing())
                return NULL;
            Linked<CGraphExecutorGraphInfo> nextGraphInfo = &toRun.item(0);
            toRun.remove(0);
            if (!nextGraphInfo->subGraph->isComplete() && (NULL == findRunning(nextGraphInfo->subGraph->queryGraphId())))
            {
                running.append(*nextGraphInfo.getLink());
                return nextGraphInfo.getClear();
            }
        }
        return NULL;
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
            for (;;)
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
                graphPool->start(graphInfo.getClear());
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
        PROGLOG("CGraphExecutor exiting, waiting on graph pool");
        graphPool->joinAll();
        PROGLOG("CGraphExecutor graphPool finished");
    }
};

////
// IContextLogger
class CThorContextLogger : implements IContextLogger, public CSimpleInterface
{
    CJobBase &job;
    unsigned traceLevel;
    StringAttr globalIdHeader;
    StringAttr callerIdHeader;
    StringAttr globalId;
    StringBuffer localId;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorContextLogger(CJobBase &_job) : job(_job)
    {
        traceLevel = 1;
        if (globals->hasProp("@httpGlobalIdHeader"))
            setHttpIdHeaders(globals->queryProp("@httpGlobalIdHeader"), globals->queryProp("@httpCallerIdHeader"));
    }
    virtual void CTXLOGva(const char *format, va_list args) const __attribute__((format(printf,2,0)))
    {
        StringBuffer ss;
        ss.valist_appendf(format, args);
        LOG(MCdebugProgress, thorJob, "%s", ss.str());
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const __attribute__((format(printf,5,0)))
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
    virtual void setGlobalId(const char *id, SocketEndpoint &ep, unsigned pid)
    {
        globalId.set(id);
        appendLocalId(localId.clear(), ep, pid);
    }
    virtual const char *queryGlobalId() const
    {
        return globalId.get();
    }
    virtual const char *queryLocalId() const
    {
        return localId.str();
    }
    virtual void setHttpIdHeaders(const char *global, const char *caller)
    {
        if (global && *global)
            globalIdHeader.set(global);
        if (caller && *caller)
            callerIdHeader.set(caller);
    }
    virtual const char *queryGlobalIdHttpHeader() const
    {
        return globalIdHeader.str();
    }
    virtual const char *queryCallerIdHttpHeader() const
    {
        return callerIdHeader.str();
    }

};

////

CJobBase::CJobBase(ILoadedDllEntry *_querySo, const char *_graphName) : querySo(_querySo), graphName(_graphName)
{
    maxDiskUsage = diskUsage = 0;
    dirty = true;
    aborted = false;
    globalMemoryMB = globals->getPropInt("@globalMemorySize"); // in MB
    channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
    numChannels = channelsPerSlave;
    pluginMap = new SafePluginMap(&pluginCtx, true);

// JCSMORE - Will pass down at job creation time...
    jobGroup.set(&::queryClusterGroup());
    slaveGroup.setown(jobGroup->remove(0));
    nodeGroup.set(&queryNodeGroup());
    myNodeRank = nodeGroup->rank(::queryMyNode());

    unsigned channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
    jobChannelSlaveNumbers.allocateN(channelsPerSlave, true); // filled when channels are added.
    jobSlaveChannelNum.allocateN(querySlaves()); // filled when channels are added.
    for (unsigned s=0; s<querySlaves(); s++)
        jobSlaveChannelNum[s] = NotFound;
    StringBuffer wuXML;
    if (!getEmbeddedWorkUnitXML(querySo, wuXML))
        throw MakeStringException(0, "Failed to locate workunit info in query : %s", querySo->queryName());
    Owned<ILocalWorkUnit> localWU = createLocalWorkUnit(wuXML);
    Owned<IConstWUGraph> graph = localWU->getGraph(graphName);
    graphXGMML.setown(graph->getXGMMLTree(false));
    if (!graphXGMML)
        throwUnexpected();
}

void CJobBase::init()
{
    StringBuffer tmp;
    tmp.append(wuid);
    tmp.append(graphName);
    key.set(tmp.str());

    StringBuffer user;
    extractFromWorkunitDAToken(token.str(), nullptr, &user, nullptr);
    userDesc = createUserDescriptor();
    userDesc->set(user.str(), token.str());//use workunit token as password

    forceLogGraphIdMin = (graph_id)getWorkUnitValueInt("forceLogGraphIdMin", 0);
    forceLogGraphIdMax = (graph_id)getWorkUnitValueInt("forceLogGraphIdMax", 0);

    logctx.setown(new CThorContextLogger(*this));

    // global setting default on, can be overridden by #option
    timeActivities = 0 != getWorkUnitValueInt("timeActivities", globals->getPropBool("@timeActivities", true));
    maxActivityCores = (unsigned)getWorkUnitValueInt("maxActivityCores", 0); // NB: 0 means system decides
    if (0 == maxActivityCores)
        maxActivityCores = getAffinityCpus();
    pausing = false;
    resumed = false;

    crcChecking = 0 != getWorkUnitValueInt("THOR_ROWCRC", globals->getPropBool("@THOR_ROWCRC", false));
    usePackedAllocator = 0 != getWorkUnitValueInt("THOR_PACKEDALLOCATOR", globals->getPropBool("@THOR_PACKEDALLOCATOR", true));
    memorySpillAtPercentage = (unsigned)getWorkUnitValueInt("memorySpillAt", globals->getPropInt("@memorySpillAt", 80));
    sharedMemoryLimitPercentage = (unsigned)getWorkUnitValueInt("globalMemoryLimitPC", globals->getPropInt("@sharedMemoryLimit", 90));
    sharedMemoryMB = globalMemoryMB*sharedMemoryLimitPercentage/100;
    failOnLeaks = getOptBool("failOnLeaks");
    maxLfnBlockTimeMins = getOptInt(THOROPT_MAXLFN_BLOCKTIME_MINS, DEFAULT_MAXLFN_BLOCKTIME_MINS);

    PROGLOG("Global memory size = %d MB, shared memory = %d%%, memory spill at = %d%%", globalMemoryMB, sharedMemoryLimitPercentage, memorySpillAtPercentage);
    StringBuffer tracing("maxActivityCores = ");
    if (maxActivityCores)
        tracing.append(maxActivityCores);
    else
        tracing.append("[unbound]");
    PROGLOG("%s", tracing.str());
}

void CJobBase::beforeDispose()
{
    endJob();
}

CJobChannel &CJobBase::queryJobChannel(unsigned c) const
{
    return jobChannels.item(c);
}

CActivityBase &CJobBase::queryChannelActivity(unsigned c, graph_id gid, activity_id id) const
{
    CJobChannel &channel = queryJobChannel(c);
    Owned<CGraphBase> graph = channel.getGraph(gid);
    dbgassertex(graph);
    CGraphElementBase *container = graph->queryElement(id);
    dbgassertex(container);
    return *container->queryActivity();
}

void CJobBase::startJob()
{
    LOG(MCdebugProgress, thorJob, "New Graph started : %s", graphName.get());
    ClearTempDirs();
    perfmonhook.setown(createThorMemStatsPerfMonHook(*this, getOptInt(THOROPT_MAX_KERNLOG, 3)));
    setPerformanceMonitorHook(perfmonhook);
    PrintMemoryStatusLog();
    logDiskSpace();
    unsigned keyNodeCacheMB = (unsigned)getWorkUnitValueInt("keyNodeCacheMB", DEFAULT_KEYNODECACHEMB * queryJobChannels());
    unsigned keyLeafCacheMB = (unsigned)getWorkUnitValueInt("keyLeafCacheMB", DEFAULT_KEYLEAFCACHEMB * queryJobChannels());
    unsigned keyBlobCacheMB = (unsigned)getWorkUnitValueInt("keyBlobCacheMB", DEFAULT_KEYBLOBCACHEMB * queryJobChannels());
    setNodeCacheMem(keyNodeCacheMB * 0x100000);
    setLeafCacheMem(keyLeafCacheMB * 0x100000);
    setBlobCacheMem(keyBlobCacheMB * 0x100000);
    PROGLOG("Key node caching setting: node=%u MB, leaf=%u MB, blob=%u MB", keyNodeCacheMB, keyLeafCacheMB, keyBlobCacheMB);

    unsigned keyFileCacheLimit = (unsigned)getWorkUnitValueInt("keyFileCacheLimit", 0);
    if (!keyFileCacheLimit)
        keyFileCacheLimit = (querySlaves()+1)*2;
    setKeyIndexCacheSize(keyFileCacheLimit);
    PROGLOG("Key file cache size set to: %d", keyFileCacheLimit);
    if (getOptBool("dumpStacks")) // mainly as an example of printAllStacks() usage
    {
        StringBuffer output;
        if (getAllStacks(output))
            PrintLogDirect(output);
        else
            WARNLOG("Failed to capture process stacks: %s", output.str());
    }
}

void CJobBase::endJob()
{
    if (jobEnded)
        return;

    jobEnded = true;
    setPerformanceMonitorHook(nullptr);
    LOG(MCdebugProgress, thorJob, "Job ended : %s", graphName.get());
    clearKeyStoreCache(true);
    PrintMemoryStatusLog();

    Owned<IMultiException> exceptions;
    ForEachItemIn(c, jobChannels)
    {
        try
        {
            jobChannels.item(c).clean();
        }
        catch (IException *e)
        {
            if (!exceptions)
                exceptions.setown(makeMultiException());
            exceptions->append(*LINK(e));
        }
    }

    try
    {
        jobChannels.kill(); // avoiding circular references. Kill before other CJobBase components are destroyed that channels reference.
        ::Release(userDesc);
        ::Release(pluginMap);

        traceMemUsage();

        if (numChannels > 1) // if only 1 - then channel allocator is same as sharedAllocator, leaks will be reported by the single channel
            checkAndReportLeaks(sharedAllocator->queryRowManager());
    }
    catch (IException *e)
    {
        if (!exceptions)
            exceptions.setown(makeMultiException());
        exceptions->append(*LINK(e));
    }
    if (exceptions && exceptions->ordinality())
        throw exceptions.getClear();
}

void CJobBase::checkAndReportLeaks(roxiemem::IRowManager *rowManager)
{
    if (!failOnLeaks) // NB: leaks reported by row manager destructor anyway
        return;
    if (rowManager->allocated())
    {
        rowManager->reportLeaks();
        throw MakeThorException(TE_RowLeaksDetected, "Row leaks detected");
    }
}

bool CJobBase::queryForceLogging(graph_id graphId, bool def) const
{
    // JCSMORE, could add comma separated range, e.g. 1-5,10-12
    if ((graphId >= forceLogGraphIdMin) && (graphId <= forceLogGraphIdMax))
        return true;
    return def;
}

void CJobBase::addSubGraph(IPropertyTree &xgmml)
{
    CriticalBlock b(crit);
    for (unsigned c=0; c<queryJobChannels(); c++)
    {
        CJobChannel &jobChannel = queryJobChannel(c);
        Owned<CGraphBase> subGraph = jobChannel.createGraph();
        subGraph->createFromXGMML(&xgmml, NULL, NULL, NULL, jobChannel.queryAllGraphs());
        jobChannel.addSubGraph(*subGraph.getClear());
    }
}

void CJobBase::addDependencies(IPropertyTree *xgmml, bool failIfMissing)
{
    for (unsigned c=0; c<queryJobChannels(); c++)
    {
        CJobChannel &jobChannel = queryJobChannel(c);
        jobChannel.addDependencies(xgmml, failIfMissing);
    }
}

bool CJobBase::queryUseCheckpoints() const
{
    return globals->getPropBool("@checkPointRecovery") || 0 != getWorkUnitValueInt("checkPointRecovery", 0);
}

void CJobBase::abort(IException *e)
{
    aborted = true;
    for (unsigned c=0; c<queryJobChannels(); c++)
    {
        CJobChannel &jobChannel = queryJobChannel(c);
        jobChannel.abort(e);
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

// these getX methods for property in workunit settings, then global setting, defaulting to provided 'dft' if not present
StringBuffer &CJobBase::getOpt(const char *opt, StringBuffer &out)
{
    if (!opt || !*opt)
        return out; // probably error
    VStringBuffer gOpt("Debug/@%s", opt);
    getWorkUnitValue(opt, out);
    if (0 == out.length())
        globals->getProp(gOpt, out);
    return out;
}

bool CJobBase::getOptBool(const char *opt, bool dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("Debug/@%s", opt);
    return getWorkUnitValueBool(opt, globals->getPropBool(gOpt, dft));
}

int CJobBase::getOptInt(const char *opt, int dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("Debug/@%s", opt);
    return (int)getWorkUnitValueInt(opt, globals->getPropInt(gOpt, dft));
}

__int64 CJobBase::getOptInt64(const char *opt, __int64 dft)
{
    if (!opt || !*opt)
        return dft; // probably error
    VStringBuffer gOpt("Debug/@%s", opt);
    return getWorkUnitValueInt(opt, globals->getPropInt64(gOpt, dft));
}

IThorAllocator *CJobBase::getThorAllocator(unsigned channel)
{
    return sharedAllocator.getLink();
}

/// CJobChannel

CJobChannel::CJobChannel(CJobBase &_job, IMPServer *_mpServer, unsigned _channel)
    : job(_job), mpServer(_mpServer), channel(_channel)
{
    aborted = false;
    thorAllocator.setown(job.getThorAllocator(channel));
    jobComm.setown(mpServer->createCommunicator(&job.queryJobGroup()));
    myrank = job.queryJobGroup().rank(queryMyNode());
    graphExecutor.setown(new CGraphExecutor(*this));
}

CJobChannel::~CJobChannel()
{
    if (!cleaned)
        clean();
}

INode *CJobChannel::queryMyNode()
{
    return mpServer->queryMyNode();
}

void CJobChannel::wait()
{
    if (graphExecutor)
        graphExecutor->wait();
}

ICodeContext &CJobChannel::queryCodeContext() const
{
    return *codeCtx;
}

ICodeContext &CJobChannel::querySharedMemCodeContext() const
{
    return *sharedMemCodeCtx;
}

mptag_t CJobChannel::deserializeMPTag(MemoryBuffer &mb)
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

IEngineRowAllocator *CJobChannel::getRowAllocator(IOutputMetaData * meta, activity_id activityId, roxiemem::RoxieHeapFlags flags) const
{
    return thorAllocator->getRowAllocator(meta, activityId, flags);
}

roxiemem::IRowManager *CJobChannel::queryRowManager() const
{
    return thorAllocator->queryRowManager();
}


void CJobChannel::addDependencies(IPropertyTree *xgmml, bool failIfMissing)
{
    ::addDependencies(xgmml, failIfMissing, allGraphs);
}

IThorGraphIterator *CJobChannel::getSubGraphs()
{
    CriticalBlock b(crit);
    return new CGraphTableIterator(subGraphs);
}

void CJobChannel::clean()
{
    if (cleaned)
        return;

    cleaned = true;
    wait();

    queryRowManager()->reportMemoryUsage(false);
    PROGLOG("CJobBase resetting memory manager");

    if (graphExecutor)
    {
        graphExecutor->queryGraphPool().stopAll();
        graphExecutor.clear();
    }
    subGraphs.kill();

    job.checkAndReportLeaks(thorAllocator->queryRowManager());

    thorAllocator.clear();
    codeCtx.clear();
}

void CJobChannel::startGraph(CGraphBase &graph, bool checkDependencies, size32_t parentExtractSize, const byte *parentExtract)
{
    graphExecutor->add(&graph, *this, checkDependencies, parentExtractSize, parentExtract);
}

IThorResult *CJobChannel::getOwnedResult(graph_id gid, activity_id ownerId, unsigned resultId)
{
    Owned<CGraphBase> graph = getGraph(gid);
    if (!graph)
    {
        Owned<IThorException> e = MakeThorException(0, "getOwnedResult: graph not found");
        e->setGraphInfo(queryJob().queryGraphName(), gid);
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

void CJobChannel::abort(IException *e)
{
    aborted = true;
    Owned<IThorGraphIterator> iter = getSubGraphs();
    ForEach (*iter)
    {
        CGraphBase &graph = iter->query();
        graph.abort(e);
    }
}

// IGraphCallback
void CJobChannel::runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract)
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
    abortSoon = receiving = cancelledReceive = initialized = reInit = false;
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

void CActivityBase::logRow(const char * prefix, IOutputMetaData & meta, const void * row) const
{
    bool blindLogging = false; // MORE: should check a workunit/global option
    if (meta.hasXML() && !blindLogging)
    {
        StringBuffer xml;
        appendRowXml(xml, meta, row);
        ActPrintLog("%s: %s", prefix, xml.str());
    }
}

void CActivityBase::ActPrintLog(const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(&queryContainer(), thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

void CActivityBase::ActPrintLog(IException *e, const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    ::ActPrintLogArgs(&queryContainer(), e, thorlog_all, MCexception(e), format, args);
    va_end(args);
}

void CActivityBase::ActPrintLog(IException *e) const
{
    ActPrintLog(e, "%s", "");
}

IThorRowInterfaces * CActivityBase::createRowInterfaces(IOutputMetaData * meta, byte seq)
{
    activity_id id = createCompoundActSeqId(queryId(), seq);
    return createThorRowInterfaces(queryRowManager(), meta, id, queryHeapFlags(), queryCodeContext());
}

IThorRowInterfaces * CActivityBase::createRowInterfaces(IOutputMetaData * meta, roxiemem::RoxieHeapFlags heapFlags, byte seq)
{
    activity_id id = createCompoundActSeqId(queryId(), seq);
    return createThorRowInterfaces(queryRowManager(), meta, id, heapFlags, queryCodeContext());
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
    throw e;
}


IEngineRowAllocator * CActivityBase::queryRowAllocator()
{
    if (CABallocatorlock.lock()) {
        if (!rowAllocator)
        {
            roxiemem::RoxieHeapFlags heapFlags = queryHeapFlags();
            rowAllocator.setown(getRowAllocator(queryRowMetaData(), heapFlags));
        }
        CABallocatorlock.unlock();
    }
    return rowAllocator;
}
    
IOutputRowSerializer * CActivityBase::queryRowSerializer()
{
    if (CABserializerlock.lock()) {
        if (!rowSerializer)
            rowSerializer.setown(queryRowMetaData()->createDiskSerializer(queryCodeContext(),queryId()));
        CABserializerlock.unlock();
    }
    return rowSerializer;
}

IOutputRowDeserializer * CActivityBase::queryRowDeserializer()
{
    if (CABdeserializerlock.lock()) {
        if (!rowDeserializer)
            rowDeserializer.setown(queryRowMetaData()->createDiskDeserializer(queryCodeContext(),queryId()));
        CABdeserializerlock.unlock();
    }
    return rowDeserializer;
}

IThorRowInterfaces *CActivityBase::getRowInterfaces()
{
    // create an independent instance, to avoid circular link dependency problems
    return createThorRowInterfaces(queryRowManager(), queryRowMetaData(), container.queryId(), queryHeapFlags(), queryCodeContext());
}

IEngineRowAllocator *CActivityBase::getRowAllocator(IOutputMetaData * meta, roxiemem::RoxieHeapFlags flags, byte seq) const
{
    activity_id actId = createCompoundActSeqId(queryId(), seq);
    return queryJobChannel().getRowAllocator(meta, actId, flags);
}

bool CActivityBase::receiveMsg(ICommunicator &comm, CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender, unsigned timeout)
{
    BooleanOnOff onOff(receiving);
    CTimeMon t(timeout);
    unsigned remaining = timeout;
    // check 'cancelledReceive' every 10 secs
    while (!cancelledReceive && ((MP_WAIT_FOREVER==timeout) || !t.timedout(&remaining)))
    {
        if (comm.recv(mb, rank, mpTag, sender, remaining>10000?10000:remaining))
            return true;
    }
    return false;
}

bool CActivityBase::receiveMsg(CMessageBuffer &mb, const rank_t rank, const mptag_t mpTag, rank_t *sender, unsigned timeout)
{
    return receiveMsg(queryJobChannel().queryJobComm(), mb, rank, mpTag, sender, timeout);
}

void CActivityBase::cancelReceiveMsg(ICommunicator &comm, const rank_t rank, const mptag_t mpTag)
{
    cancelledReceive = true;
    if (receiving)
        comm.cancel(rank, mpTag);
}

void CActivityBase::cancelReceiveMsg(const rank_t rank, const mptag_t mpTag)
{
    cancelReceiveMsg(queryJobChannel().queryJobComm(), rank, mpTag);
}
