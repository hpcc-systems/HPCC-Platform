/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "platform.h"
#include "jlib.hpp"

#include "ccd.hpp"
#include "ccdcontext.hpp"
#include "ccddali.hpp"
#include "ccdquery.hpp"
#include "ccdqueue.ipp"
#include "ccdsnmp.hpp"
#include "ccdstate.hpp"

using roxiemem::IRowManager;

//=======================================================================================================================

class InputProbe : public CInterface, implements IRoxieInput, implements IEngineRowStream // base class for the edge probes used for tracing and debugging....
{
protected:
    IRoxieInput *in;
    unsigned sourceId;
    unsigned sourceIdx;
    unsigned targetId;
    unsigned targetIdx;
    unsigned iteration;
    unsigned channel;

    IOutputMetaData *inMeta;
    IDebuggableContext *debugContext;
    unsigned rowCount;
    unsigned totalRowCount;
    size32_t maxRowSize;
    bool everStarted;
    bool hasStarted;
    bool hasStopped;

public:
    InputProbe(IRoxieInput *_in, IDebuggableContext *_debugContext,
        unsigned _sourceId, unsigned _sourceIdx, unsigned _targetId, unsigned _targetIdx, unsigned _iteration, unsigned _channel)
        : in(_in),  debugContext(_debugContext),
          sourceId(_sourceId), sourceIdx(_sourceIdx), targetId(_targetId), targetIdx(_targetIdx), iteration(_iteration), channel(_channel)
    {
        hasStarted = false;
        everStarted = false;
        hasStopped = false;
        rowCount = 0;
        totalRowCount = 0;
        maxRowSize = 0;
        inMeta = NULL;
    }

    virtual IInputSteppingMeta * querySteppingMeta()
    {
        return in->querySteppingMeta();
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector)
    {
        return in->gatherConjunctions(collector);
    }
    virtual void resetEOF()
    {
        in->resetEOF();
    }
    virtual unsigned numConcreteOutputs() const
    {
        return in->numConcreteOutputs();
    }
    virtual IRoxieInput * queryConcreteInput(unsigned idx)
    {
        // MORE - not sure what is right here!
        if (in->queryConcreteInput(idx) == in)
        {
            assertex(idx==0);
            return this;
        }
        else
            return in->queryConcreteInput(idx);
    }
    virtual IRoxieServerActivity *queryActivity()
    {
        return in->queryActivity();
    }
    virtual IIndexReadActivityInfo *queryIndexReadActivity()
    {
        return in->queryIndexReadActivity();
    }
    virtual IOutputMetaData * queryOutputMeta() const
    {
        return in->queryOutputMeta();
    }
    IEngineRowStream &queryStream()
    {
        return *this;
    }
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        // NOTE: totalRowCount/maxRowSize not reset, as we want them cumulative when working in a child query.
        rowCount = 0;
        hasStarted = true;
        hasStopped = false;
        everStarted = true;
        in->start(parentExtractSize, parentExtract, paused);
        inMeta = in->queryOutputMeta();
        assertex(inMeta);
    }
    virtual void stop()
    {
        hasStopped = true;
        in->stop();
    }
    virtual void reset()
    {
        hasStarted = false;
        in->reset();
    }
    virtual void checkAbort()
    {
        in->checkAbort();
    }
    virtual unsigned queryId() const
    {
        return in->queryId();
    }
    virtual unsigned __int64 queryTotalCycles() const
    {
        return in->queryTotalCycles();
    }
    virtual unsigned __int64 queryLocalCycles() const
    {
        return in->queryLocalCycles();
    }
    virtual IRoxieInput *queryInput(unsigned idx) const
    {
        if (!idx)
            return in;
        else
            return NULL;
    }
    virtual const void *nextRow()
    {
        const void *ret = in->nextRow();
        if (ret)
        {
            size32_t size = inMeta->getRecordSize(ret);
            if (size > maxRowSize)
                maxRowSize = size;
            rowCount++;
            totalRowCount++;
        }
        return ret;
    }
    virtual const void * nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        const void *ret = in->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret && wasCompleteMatch)  // GH is this test right?
        {
            size32_t size = inMeta->getRecordSize(ret);
            if (size > maxRowSize)
                maxRowSize = size;
            rowCount++;
            totalRowCount++;
        }
        return ret;
    }
};


class TraceProbe : public InputProbe
{
public:
    IMPLEMENT_IINTERFACE;

    TraceProbe(IRoxieInput *_in, unsigned _sourceId, unsigned _targetId, unsigned _sourceIdx, unsigned _targetIdx, unsigned _iteration, unsigned _channel)
        : InputProbe(_in, NULL, _sourceId, _sourceIdx, _targetId, _targetIdx, _iteration, _channel)
    {
    }

    bool matches(IPropertyTree &edge, bool forNode)
    {
        if (forNode)
        {
            unsigned id = edge.getPropInt("@id", 0);
            if (id && (id == sourceId || id == targetId))
            {
                return true;
            }
        }
        else
        {
            unsigned id = edge.getPropInt("@source", 0);
            if (id && id == sourceId)
            {
                id = edge.getPropInt("@target", 0);
                if (id && id == targetId)
                {
                    unsigned idx = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                    if (idx == sourceIdx)
                        return true;
                }
            }
            id = edge.getPropInt("att[@name=\"_sourceActivity\"]/@value");
            if (id && id == sourceId)
            {
                id = edge.getPropInt("att[@name=\"_targetActivity\"]/@value");
                if (id && id == targetId)
                {
                    unsigned idx = edge.getPropInt("att[@name=\"_sourceIndex\"]/@value", 0);
                    if (idx == sourceIdx)
                        return true;
                }
            }
        }
        return false;
    }

    const void * _next(const void *inputRow)
    {
        const byte *ret = (const byte *) inputRow;
        if (ret && probeAllRows)
        {
            CommonXmlWriter xmlwrite(XWFnoindent|XWFtrim|XWFopt);
            if (inMeta && inMeta->hasXML())
                inMeta->toXML(ret, xmlwrite);
            DBGLOG("ROW: [%d->%d] {%p} %s", sourceId, targetId, ret, xmlwrite.str());
        }
        return ret;
    }

    virtual const void * nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        // MORE - should probably only note them when wasCompleteMatch is true?
        return _next(InputProbe::nextRowGE(seek, numFields, wasCompleteMatch, stepExtra));
    }
    virtual const void *nextRow()
    {
        return _next(InputProbe::nextRow());
    }

    void getNodeProgressInfo(IPropertyTree &node)
    {
        // node is the input (or possibly output) of this probe edge
        unsigned started = everStarted;
        putStatsValue(&node, "_roxieStarted", "sum", started);
        unsigned id = node.getPropInt("@id", 0);
        bool isOutput = (id != 0) && (id != sourceId);
        unsigned totalTime = (unsigned) (cycle_to_nanosec(in->queryTotalCycles())/1000);
        if (isOutput)
            totalTime += 10; // Fudge factor - I don't really know the times but this makes the graph more useable than not supplying a totalTime value
        if (totalTime)
            putStatsValue(&node, "totalTime", "sum", totalTime);
        unsigned localTime = isOutput ? 10 : (unsigned) (cycle_to_nanosec(in->queryLocalCycles())/1000); // Fudge factor - I don't really know the times but this makes the graph more useable than not supplying a localTime value
        if (localTime)
            putStatsValue(&node, "localTime", "sum", localTime);
    }

    void getEdgeProgressInfo(IPropertyTree &edge)
    {
        putStatsValue(&edge, "_roxieStarted", "sum", hasStarted);
        if (hasStarted)
        {
            putStatsValue(&edge, "count", "sum", totalRowCount);
            putStatsValue(&edge, "maxrowsize", "max", maxRowSize);
        }
    }
};

class CProbeManager : public CInterface, implements IProbeManager
{
    IArrayOf<IRoxieInput> probes; // May want to replace with hash table at some point....
public:
    IMPLEMENT_IINTERFACE;

    IInputBase *createProbe(IInputBase *in, IActivityBase *inAct, IActivityBase *outAct, unsigned sourceIdx, unsigned targetIdx, unsigned iteration)
    {
        unsigned idIn = inAct->queryId();
        unsigned idOut = outAct->queryId();
        TraceProbe *probe = new TraceProbe(static_cast<IRoxieInput*>(in), idIn, idOut, sourceIdx, targetIdx, iteration, 0);
        probes.append(*probe);
        return probe;
    }

    TraceProbe *findProbe(IPropertyTree &edge, bool forNode, unsigned &startat)
    {
        // MORE - this is n-squared on number of edges in the graph. Could get painful - recode if needed
        // However I think that the "startat" cache probably prevents the pain
        unsigned probeCount = probes.ordinality();
        unsigned search = probeCount;
        unsigned idx = startat;
        while (search--)
        {
            idx++;
            if (idx>=probeCount) idx = 0;
            TraceProbe &p = static_cast<TraceProbe &> (probes.item(idx));
            if (p.matches(edge, forNode))
            {
                startat = idx;
                return &p;
            }
        }
        return NULL;
    }

    virtual void noteSink(IActivityBase *)
    {
    }

    virtual IDebugGraphManager *queryDebugManager()
    {
        return NULL;
    }

    virtual void noteDependency(IActivityBase *sourceActivity, unsigned sourceIndex, unsigned controlId, const char *edgeId, IActivityBase *targetActivity)
    {
    }

    virtual IProbeManager *startChildGraph(unsigned childGraphId, IActivityBase *parent)
    {
        return LINK(this);
    }

    virtual void endChildGraph(IProbeManager *child, IActivityBase *parent)
    {
    }

    virtual void deleteGraph(IArrayOf<IActivityBase> *activities, IArrayOf<IInputBase> *goers)
    {
        if (goers)
        {
            ForEachItemIn(probeIdx, *goers)
            {
                TraceProbe &probe = (TraceProbe &) goers->item(probeIdx);
                probes.zap(probe);
            }
        }
    }

    virtual void setNodeProperty(IActivityBase *node, const char *propName, const char *propVvalue)
    {
        // MORE - we could note these in probe mode too...
    }

    virtual void setNodePropertyInt(IActivityBase *node, const char *propName, unsigned __int64 propVvalue)
    {
        // MORE - we could note these in probe mode too...
    }

    virtual void getProbeResponse(IPropertyTree *query)
    {
        Owned<IPropertyTreeIterator> graphs = query->getElements("Graph");
        ForEach(*graphs)
        {
            IPropertyTree &graph = graphs->query();
            Owned<IPropertyTreeIterator> subgraphs = graph.getElements("xgmml/graph");
            ForEach(*subgraphs)
            {
                IPropertyTree &subgraph = subgraphs->query();
                Owned<IPropertyTreeIterator> nodes = subgraph.getElements(".//node");
                unsigned startat = 0;
                ForEach(*nodes)
                {
                    IPropertyTree &node = nodes->query();
                    TraceProbe *currentProbe = findProbe(node, true, startat);
                    if (currentProbe)
                    {
                        currentProbe->getNodeProgressInfo(node);
                    }
                }
                Owned<IPropertyTreeIterator> edges = subgraph.getElements(".//edge");
                startat = 0;
                ForEach(*edges)
                {
                    IPropertyTree &edge = edges->query();
                    if (edge.getPropInt("att[@name='_dependsOn']/@value", 0) != 0)
                    {
                        const char *targetNode = edge.queryProp("att[@name='_targetActivity']/@value");
                        if (targetNode)
                        {
                            StringBuffer xpath;
                            IPropertyTree *target = query->queryPropTree(xpath.append(".//node[@id='").append(targetNode).append("']"));
                            if (target)
                            {
                                unsigned started = target->getPropInt("att[@name='_roxieStarted']/@value", 0);
                                IPropertyTree *att = edge.queryPropTree("att[@name=\"_roxieStarted\"]");
                                if (!att)
                                {
                                    att = edge.addPropTree("att", createPTree());
                                    att->setProp("@name", "_roxieStarted");
                                }
                                else
                                    started += att->getPropInt("@value");
                                att->setPropInt("@value", started);
                            }

                        }
                    }
                    else
                    {
                        TraceProbe *currentProbe = findProbe(edge, false, startat);
                        if (currentProbe)
                        {
                            currentProbe->getEdgeProgressInfo(edge);
                        }
                        else
                        {
                            const char *targetNode = edge.queryProp("att[@name='_targetActivity']/@value");
                            if (targetNode)
                            {
                                StringBuffer xpath;
                                IPropertyTree *target = query->queryPropTree(xpath.append(".//node[@id='").append(targetNode).append("']"));
                                if (target)
                                {
                                    unsigned started = target->getPropInt("att[@name='_roxieStarted']/@value", 0);
                                    IPropertyTree *att = edge.queryPropTree("att[@name=\"_roxieStarted\"]");
                                    if (!att)
                                    {
                                        att = edge.addPropTree("att", createPTree());
                                        att->setProp("@name", "_roxieStarted");
                                    }
                                    else
                                        started += att->getPropInt("@value");
                                    att->setPropInt("@value", started);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
};

typedef const IInterface *CIptr;
typedef MapBetween<unsigned, unsigned, CIptr, CIptr> ProxyMap;
static ProxyMap *registeredProxies;
static CriticalSection proxyLock;
static memsize_t nextProxyId = 1;

static memsize_t registerProxyId(const IInterface * object)
{
    CriticalBlock b(proxyLock);
    if (!registeredProxies)
        registeredProxies = new ProxyMap;
    registeredProxies->setValue(nextProxyId, object);
    return nextProxyId++;
}

static void unregisterProxyId(memsize_t id)
{
    // CriticalBlock b(proxyLock); done by caller
    if (registeredProxies)
    {
        registeredProxies->remove(id);
        if (!registeredProxies->count())
        {
            delete registeredProxies;
            registeredProxies = NULL;
        }

    }
}

static const IInterface *getProxy(memsize_t id)
{
    CriticalBlock b(proxyLock);
    if (registeredProxies)
    {
        CIptr *proxy = registeredProxies->getValue(id);
        if (proxy)
            return LINK(*proxy);
    }
    return NULL;
}

//copied to eclagent, needs to be made common
class DebugProbe : public InputProbe, implements IActivityDebugContext
{
    Owned<IGlobalEdgeRecord> edgeRecord;
    ICopyArrayOf<IBreakpointInfo> breakpoints;
    HistoryRow *history;
    unsigned lastSequence;
    unsigned historySize;
    unsigned historyCapacity;
    unsigned nextHistorySlot;
    unsigned childGraphId;

    mutable memsize_t proxyId; // MORE - do we need a critsec to protect too?

    DebugActivityRecord *sourceAct;
    DebugActivityRecord *targetAct;

    StringAttr edgeId;
    bool forceEOF;
    bool EOGseen;
    bool EOGsent;

    static void putAttributeUInt(IXmlWriter *output, const char *name, unsigned value)
    {
        output->outputBeginNested("att", false);
        output->outputCString(name, "@name");
        output->outputInt(value, sizeof(int), "@value");
        output->outputEndNested("att");
    }

    void rowToXML(IXmlWriter *output, const void *row, unsigned sequence, unsigned rowCount, bool skipped, bool limited, bool eof, bool eog) const
    {
        output->outputBeginNested("Row", true);
        output->outputInt(sequence, sizeof(int), "@seq");
        if (skipped)
            output->outputBool(true, "@skip");
        if (limited)
            output->outputBool(true, "@limit");
        if (eof)
            output->outputBool(true, "@eof");
        if (eog)
            output->outputBool(true, "@eog");
        if (row)
        {
            output->outputInt(rowCount, sizeof(int), "@count");
            IOutputMetaData *meta = queryOutputMeta();
            output->outputInt(meta->getRecordSize(row), sizeof(int), "@size");
            meta->toXML((const byte *) row, *output);
        }
        output->outputEndNested("Row");
    }

public:
    DebugProbe(IInputBase *_in, unsigned _sourceId, unsigned _sourceIdx, DebugActivityRecord *_sourceAct, unsigned _targetId, unsigned _targetIdx, DebugActivityRecord *_targetAct, unsigned _iteration, unsigned _channel, IDebuggableContext *_debugContext)
        : InputProbe(static_cast<IRoxieInput*>(_in), _debugContext, _sourceId, _sourceIdx, _targetId, _targetIdx, _iteration, _channel),
          sourceAct(_sourceAct), targetAct(_targetAct)
    {
        historyCapacity = debugContext->getDefaultHistoryCapacity();
        nextHistorySlot = 0;
        if (historyCapacity)
            history = new HistoryRow [historyCapacity];
        else
            history = NULL;
        historySize = 0;
        lastSequence = 0;
        StringBuffer idText;
        idText.appendf("%d_%d", sourceId, sourceIdx);
        edgeRecord.setown(debugContext->getEdgeRecord(idText));
        if (iteration || channel)
            idText.appendf(".%d", iteration);
        if (channel)
            idText.appendf("#%d", channel);
        edgeId.set(idText);
        debugContext->checkDelayedBreakpoints(this);
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        proxyId = 0;
    }

    ~DebugProbe()
    {
        if (history)
        {
            for (unsigned idx = 0; idx < historyCapacity; idx++)
                ReleaseRoxieRow(history[idx].row);
            delete [] history;
        }
        ForEachItemIn(bpIdx, breakpoints)
        {
            breakpoints.item(bpIdx).removeEdge(*this);
        }
    }

    virtual void Link() const
    {
        CInterface::Link();
    }

    virtual bool Release() const
    {
        CriticalBlock b(proxyLock);
        if (!IsShared())
        {
            if (proxyId)
                unregisterProxyId(proxyId);
        }
        return CInterface::Release();
    }

    virtual memsize_t queryProxyId() const
    {
        if (!proxyId)
            proxyId = registerProxyId((const IActivityDebugContext *) this);
        return proxyId;
    }

    virtual unsigned queryChildGraphId() const
    {
        return childGraphId;
    }

    virtual void resetEOF()
    {
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        InputProbe::resetEOF();
    }
#if 0
    virtual unsigned queryId() const
    {
        return sourceId;
    }
#endif
    virtual const char *queryEdgeId() const
    {
        return edgeId.get();
    }

    virtual const char *querySourceId() const
    {
        UNIMPLEMENTED;
    }

    virtual void printEdge(IXmlWriter *output, unsigned startRow, unsigned numRows) const
    {
        output->outputBeginNested("edge", true);
        output->outputString(edgeId.length(), edgeId.get(), "@edgeId");
        if (startRow < historySize)
        {
            if (numRows > historySize - startRow)
                numRows = historySize - startRow;
            while (numRows)
            {
                IHistoryRow *rowData = queryHistoryRow(startRow+numRows-1);
                assertex(rowData);
                rowToXML(output, rowData->queryRow(), rowData->querySequence(), rowData->queryRowCount(), rowData->wasSkipped(), rowData->wasLimited(), rowData->wasEof(), rowData->wasEog());
                numRows--;
            }
        }
        output->outputEndNested("edge");
    }

    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows)
    {
        IOutputMetaData *meta = queryOutputMeta();
        bool anyMatchedYet = false;
        if (matcher->canMatchAny(meta))
        {
            for (unsigned i = 0; i < historySize; i++)
            {
                IHistoryRow *rowData = queryHistoryRow(i);
                assertex(rowData);
                const void *row = rowData->queryRow();
                if (row)
                {
                    matcher->reset();
                    meta->toXML((const byte *) rowData->queryRow(), *matcher);
                    if (matcher->matched())
                    {
                        if (!anyMatchedYet)
                        {
                            output->outputBeginNested("edge", true);
                            output->outputString(edgeId.length(), edgeId.get(), "@edgeId");
                            anyMatchedYet = true;
                        }
                        if (fullRows)
                            rowToXML(output, rowData->queryRow(), rowData->querySequence(), rowData->queryRowCount(), rowData->wasSkipped(), rowData->wasLimited(), rowData->wasEof(), rowData->wasEog());
                        else
                        {
                            output->outputBeginNested("Row", true);
                            output->outputInt(rowData->querySequence(), sizeof(int), "@sequence");
                            output->outputInt(rowData->queryRowCount(), sizeof(int), "@count");
                            output->outputEndNested("Row");
                        }
                    }
                }
            }
            if (anyMatchedYet)
                output->outputEndNested("edge");
        }
    }

    virtual void getXGMML(IXmlWriter *output) const
    {
        output->outputBeginNested("edge", false);
        sourceAct->outputId(output, "@source");
        targetAct->outputId(output, "@target");
        output->outputString(edgeId.length(), edgeId.get(), "@id");

        if (sourceIdx)
            putAttributeUInt(output, "_sourceIndex", sourceIdx);
        putAttributeUInt(output, "count", rowCount);    //changed from totalRowCount
        putAttributeUInt(output, "maxRowSize", maxRowSize);
        putAttributeUInt(output, "_roxieStarted", everStarted);
        putAttributeUInt(output, "_started", hasStarted);
        putAttributeUInt(output, "_stopped", hasStopped);
        putAttributeUInt(output, "_eofSeen", forceEOF);
        if (breakpoints.ordinality())
            putAttributeUInt(output, "_breakpoints", breakpoints.ordinality());
        output->outputEndNested("edge");
    }

    virtual IOutputMetaData *queryOutputMeta() const
    {
        return InputProbe::queryOutputMeta();
    }

    virtual IActivityDebugContext *queryInputActivity() const
    {
        IRoxieInput *x = in;
        while (x && QUERYINTERFACE(x->queryConcreteInput(0), IActivityDebugContext)==NULL)
            x = x->queryConcreteInput(0)->queryInput(0);
        return x ? QUERYINTERFACE(x->queryConcreteInput(0), IActivityDebugContext) : NULL;
    }

    // NOTE - these functions are threadsafe because only called when query locked by debugger.
    // Even though this thread may not yet be blocked on the debugger's critsec, because all manipulation (including setting history rows) is from
    // within debugger it is ok.

    virtual unsigned queryHistorySize() const
    {
        return historySize;
    }

    virtual IHistoryRow *queryHistoryRow(unsigned idx) const
    {
        assertex(idx < historySize);
        int slotNo = nextHistorySlot - idx - 1;
        if (slotNo < 0)
            slotNo += historyCapacity;
        return &history[slotNo];
    }

    virtual unsigned queryHistoryCapacity() const
    {
        return historyCapacity;
    }

    virtual unsigned queryLastSequence() const
    {
        return lastSequence;
    }

    virtual IBreakpointInfo *debuggerCallback(unsigned sequence, const void *row)
    {
        // First put the row into the history buffer...
        lastSequence = sequence;
        if (historyCapacity)
        {
            ReleaseClearRoxieRow(history[nextHistorySlot].row);
            if (row) LinkRoxieRow(row);
            history[nextHistorySlot].sequence = sequence; // MORE - timing might be interesting too, but would need to exclude debug wait time somehow...
            history[nextHistorySlot].row = row;
            history[nextHistorySlot].rowCount = rowCount;
            if (!row)
            {
                if (forceEOF)
                    history[nextHistorySlot].setEof();
                else
                    history[nextHistorySlot].setEog();
            }
            if (historySize < historyCapacity)
                historySize++;
            nextHistorySlot++;
            if (nextHistorySlot==historyCapacity)
                nextHistorySlot = 0;
        }
        // Now check breakpoints...
        ForEachItemIn(idx, breakpoints)
        {
            IBreakpointInfo &bp = breakpoints.item(idx);
            if (bp.matches(row, forceEOF, rowCount, queryOutputMeta())) // should optimize to only call queryOutputMeta once - but not that common to have multiple breakpoints
                return &bp;
        }
        return NULL;
    }

    virtual void setHistoryCapacity(unsigned newCapacity)
    {
        if (newCapacity != historyCapacity)
        {
            HistoryRow *newHistory;
            if (newCapacity)
            {
                unsigned copyCount = historySize;
                if (copyCount > newCapacity)
                    copyCount = newCapacity;
                newHistory = new HistoryRow [newCapacity];
                unsigned slot = 0;
                while (copyCount--)
                {
                    IHistoryRow *oldrow = queryHistoryRow(copyCount);
                    newHistory[slot].sequence = oldrow->querySequence();
                    newHistory[slot].row = oldrow->queryRow();
                    newHistory[slot].rowCount = oldrow->queryRowCount();
                    if (newHistory[slot].row)
                        LinkRoxieRow(newHistory[slot].row);
                    slot++;
                }
                historySize = slot;
                nextHistorySlot = slot;
                if (nextHistorySlot==historyCapacity)
                    nextHistorySlot = 0;
            }
            else
            {
                newHistory = NULL;
                historySize = 0;
                nextHistorySlot = 0;
            }
            for (unsigned idx = 0; idx < historyCapacity; idx++)
                ReleaseRoxieRow(history[idx].row);
            delete [] history;
            history = newHistory;
            historyCapacity = newCapacity;
        }
    }

    virtual void clearHistory()
    {
        for (unsigned idx = 0; idx < historyCapacity; idx++)
            ReleaseClearRoxieRow(history[idx].row);
        historySize = 0;
        nextHistorySlot = 0;
    }

    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused)
    {
        forceEOF = false;
        EOGseen = false;
        EOGsent = false;
        if (!hasStarted)
        {
            lastSequence = debugContext->querySequence();
            edgeRecord->incrementCount(0, lastSequence);
        }
        InputProbe::start(parentExtractSize, parentExtract, paused);
    }

    virtual void reset()
    {
        InputProbe::reset();
        sourceAct->updateTimes(debugContext->querySequence());
        targetAct->updateTimes(debugContext->querySequence());
    }

    virtual void stop()
    {
        InputProbe::stop();
        sourceAct->updateTimes(debugContext->querySequence());
        targetAct->updateTimes(debugContext->querySequence());
    }

    virtual const void *nextRow()
    {
        // Code is a little complex to avoid interpreting a skip on all rows in a group as EOF
        try
        {
            if (forceEOF)
                return NULL;
            loop
            {
                const void *ret = InputProbe::nextRow();
                if (!ret)
                {
                    if (EOGseen)
                        forceEOF = true;
                    else
                        EOGseen = true;
                }
                else
                    EOGseen = false;
                if (ret)
                    edgeRecord->incrementCount(1, debugContext->querySequence());
                BreakpointActionMode action = debugContext->checkBreakpoint(DebugStateEdge, this, ret);
                if (action == BreakpointActionSkip && !forceEOF)
                {
                    if (historyCapacity)
                        queryHistoryRow(0)->setSkipped();
                    if (ret)
                    {
                        edgeRecord->incrementCount(-1, debugContext->querySequence());
                        ReleaseClearRoxieRow(ret);
                        rowCount--;
                    }
                    continue;
                }
                else if (action == BreakpointActionLimit)
                {
                    // This return value implies that we should not return the current row NOR should we return any more...
                    forceEOF = true;
                    if (ret)
                        edgeRecord->incrementCount(-1, debugContext->querySequence());
                    ReleaseClearRoxieRow(ret);
                    if (historyCapacity)
                        queryHistoryRow(0)->setLimited();
                    rowCount--;
                }
                if (forceEOF || ret || !EOGsent)
                {
                    EOGsent = (ret == NULL);
                    sourceAct->updateTimes(debugContext->querySequence());
                    targetAct->updateTimes(debugContext->querySequence());
                    return ret;
                }
            }
        }
        catch (IException *E)
        {
            debugContext->checkBreakpoint(DebugStateException, this, E);
            throw;
        }
    }

    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra & stepExtra)
    {
        // MORE - not sure that skip is safe here? Should the incomplete matches even be returned?
        // Code is a little complex to avoid interpreting a skip on all rows in a group as EOF
        // MORE - should probably only note them when wasCompleteMatch is true?
        try
        {
            if (forceEOF)
                return NULL;
            loop
            {
                const void *ret = InputProbe::nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
                if (!ret)
                {
                    if (EOGseen)
                        forceEOF = true;
                    else
                        EOGseen = true;
                }
                else
                    EOGseen = false;
                if (ret)
                    edgeRecord->incrementCount(1, debugContext->querySequence());
                BreakpointActionMode action = debugContext->checkBreakpoint(DebugStateEdge, this, ret);
                if (action == BreakpointActionSkip && !forceEOF)
                {
                    if (ret)
                        edgeRecord->incrementCount(-1, debugContext->querySequence());
                    ReleaseClearRoxieRow(ret);
                    if (historyCapacity)
                        queryHistoryRow(0)->setSkipped();
                    rowCount--;
                    continue;
                }
                else if (action == BreakpointActionLimit)
                {
                    // This return value implies that we should not return the current row NOR should we return any more...
                    forceEOF = true;
                    if (ret)
                        edgeRecord->incrementCount(-1, debugContext->querySequence());
                    ReleaseClearRoxieRow(ret);
                    if (historyCapacity)
                        queryHistoryRow(0)->setLimited();
                    rowCount--;
                }
                if (forceEOF || ret || !EOGsent)
                {
                    EOGsent = (ret == NULL);
                    sourceAct->updateTimes(debugContext->querySequence());
                    targetAct->updateTimes(debugContext->querySequence());
                    return ret;
                }
            }
        }
        catch (IException *E)
        {
            debugContext->checkBreakpoint(DebugStateException, this, E);
            throw;
        }
    }

    virtual void setBreakpoint(IBreakpointInfo &bp)
    {
        if (bp.canMatchAny(queryOutputMeta()))
        {
            breakpoints.append(bp);
            bp.noteEdge(*this);
        }
    }

    virtual void removeBreakpoint(IBreakpointInfo &bp)
    {
        breakpoints.zap(bp);
        bp.removeEdge(*this);
    }
};

extern IProbeManager *createProbeManager()
{
    return new CProbeManager;
}

IDebugGraphManager *createProxyDebugGraphManager(unsigned graphId, unsigned channel, memsize_t remoteGraphId);

class CRoxieDebugGraphManager : extends CBaseDebugGraphManager
{
    unsigned subId;

public:
    CRoxieDebugGraphManager(IDebuggableContext *_debugContext, unsigned _id, const char *_graphName, unsigned _subId)
        : CBaseDebugGraphManager(_debugContext, _id, _graphName), subId(_subId)
    {
    }

    bool Release() const
    {
        CriticalBlock b(proxyLock);
        if (!IsShared())
        {
            if (!id)
                debugContext->releaseManager(const_cast<CRoxieDebugGraphManager*> (this));
            if (proxyId)
                unregisterProxyId(proxyId);
        }
        return CInterface::Release();
    }

    virtual IInputBase *createProbe(IInputBase *in, IActivityBase *sourceAct, IActivityBase *targetAct, unsigned sourceIdx, unsigned targetIdx, unsigned iteration)
    {
        CriticalBlock b(crit);
        if (!iteration)
            iteration = subId;
        unsigned channel = debugContext->queryChannel();
        unsigned sourceId = sourceAct->queryId();
        unsigned targetId = targetAct->queryId();
        DebugActivityRecord *sourceActRecord = noteActivity(sourceAct, iteration, channel, debugContext->querySequence());
        DebugActivityRecord *targetActRecord = noteActivity(targetAct, iteration, channel, debugContext->querySequence());
        DebugProbe *probe = new DebugProbe(in, sourceId, sourceIdx, sourceActRecord, targetId, targetIdx, targetActRecord, iteration, channel, debugContext);
#ifdef _DEBUG
        DBGLOG("Creating probe for edge id %s in graphManager %p", probe->queryEdgeId(), this);
#endif
        assertex(!allProbes.getValue(probe->queryEdgeId()));
        allProbes.setValue(probe->queryEdgeId(), (IActivityDebugContext *) probe);

        probe->Release(); // the allProbes map will have linked, and is enough to ensure lifespan...
        return probe;
    }

    virtual memsize_t queryProxyId() const
    {
        if (!proxyId)
            proxyId = registerProxyId((const IDebugGraphManager *) this);
        return proxyId;
    }

    virtual void deserializeProxyGraphs(DebugState state, MemoryBuffer &buff, IActivityBase *parentActivity, unsigned channel)
    {
        Linked<DebugActivityRecord> parentNode = allActivities.getValue(parentActivity);
        assertex(parentNode != NULL);

        unsigned numChildren;
        buff.read(numChildren);
        while (numChildren--)
        {
            unsigned remoteId;
            memsize_t proxyId;
            buff.read(remoteId);
            __uint64 tmp;
            buff.read(tmp);
            proxyId = (memsize_t)tmp;   // can't serialize memsize_t
            bool found = false;
            ForEachItemIn(idx, parentNode->childGraphs)
            {
                IDebugGraphManager &child = parentNode->childGraphs.item(idx);
                if (child.queryProxyId() == proxyId)
                {
                    found = true;
                    if (state == DebugStateGraphFinished)
                    {
                        parentNode->childGraphs.remove(idx);
                        debugContext->noteGraphChanged();
                    }
                    break;
                }
            }
            if (!found && state != DebugStateGraphFinished)
            {
                IDebugGraphManager *proxy = createProxyDebugGraphManager(remoteId, channel, proxyId);
                childGraphs.append(*LINK(proxy));
                parentNode->childGraphs.append(*proxy);
                debugContext->noteGraphChanged();
            }
        }
    }

    virtual IProbeManager *startChildGraph(unsigned childGraphId, IActivityBase *parent)
    {
        CriticalBlock b(crit);
        if (childGraphId || parent)
        {
            CRoxieDebugGraphManager *childManager = new CRoxieDebugGraphManager(debugContext, childGraphId, NULL, parent ? parent->queryId() : 0);
            IDebugGraphManager *graph = childManager;
            childGraphs.append(*LINK(graph));
            debugContext->noteGraphChanged();
            return childManager;
        }
        else
            return LINK(this);
    }

    virtual void deleteGraph(IArrayOf<IActivityBase> *activities, IArrayOf<IInputBase> *probes)
    {
        CriticalBlock b(crit);
        if (activities)
        {
            ForEachItemIn(idx, *activities)
            {
                IActivityBase &activity = activities->item(idx);
                if (activity.isSink())
                    sinks.zap(activity);
                Linked<DebugActivityRecord> node = allActivities.getValue(&activity);
                if (node)
                    allActivities.remove(&activity);
            }
        }
        if (probes)
        {
            IArrayOf<IRoxieInput>* fprobes = (IArrayOf<IRoxieInput>*)(probes);
            ForEachItemIn(probeIdx, *fprobes)
            {
                DebugProbe &probe = (DebugProbe &) fprobes->item(probeIdx);
#ifdef _DEBUG
                DBGLOG("removing probe for edge id %s in graphManager %p", probe.queryEdgeId(), this);
#endif
                allProbes.remove(probe.queryEdgeId());
            }
        }
        debugContext->noteGraphChanged();
    }
};

extern IProbeManager *createDebugManager(IDebuggableContext *debugContext, const char *graphName)
{
    return new CRoxieDebugGraphManager(debugContext, 0, graphName, 0);
}

enum DebugRequestType
{
    DEBUGREQUEST_OUTPUTCHILDGRAPH,
    DEBUGREQUEST_OUTPUTLINKSFORCHILDGRAPH,
    DEBUGREQUEST_LOOKUPACTIVITYBYEDGEID,
    DEBUGREQUEST_PRINTEDGE,
    DEBUGREQUEST_SETBREAKPOINT,
    DEBUGREQUEST_SEARCHHISTORIES,
    DEBUGREQUEST_GETRESETGLOBALCOUNTS
};

struct DebugRequestBase : public CInterface, implements IInterface
{
protected:
    DebugRequestType function;
    memsize_t proxyId; // MORE - at some point should really make this an int instead - but need to look into whether ever used to represent a pointer
public:
    IMPLEMENT_IINTERFACE;
    DebugRequestBase(DebugRequestType _function, memsize_t _proxyId) : function(_function), proxyId(_proxyId) {}
    DebugRequestBase(MemoryBuffer &serialized)
    {
        byte fval;
        serialized.read(fval); function = (DebugRequestType) fval;
        unsigned __int64 tmp;
        serialized.read(tmp); // can't serilalize memsize_t
        proxyId = (memsize_t) tmp;
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        buf.append((byte) function);
        buf.append((unsigned __int64) proxyId); // can't serialize memsize_t
    }
    virtual void executeRequest(IXmlWriter *output) = 0;

    inline IDebugGraphManager *getManager()
    {
        return (IDebugGraphManager *) getProxy(proxyId);
    }
    inline IActivityDebugContext *getActivity()
    {
        return (IActivityDebugContext *) getProxy(proxyId);
    }
    void inactive(IXmlWriter *output)
    {
        // MORE - what should I do here?
    }
};

struct DebugRequestOutputChildGraph : public DebugRequestBase
{
private:
    unsigned sequence;
public:
    DebugRequestOutputChildGraph(memsize_t _proxyId, unsigned _sequence) : DebugRequestBase(DEBUGREQUEST_OUTPUTCHILDGRAPH, _proxyId), sequence(_sequence)
    {
    }
    DebugRequestOutputChildGraph(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
        serialized.read(sequence);
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        DebugRequestBase::serialize(buf);
        buf.append(sequence);
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
            manager->outputChildGraph(output, sequence);
        else
            inactive(output);
    }
};

struct DebugRequestWithId : public DebugRequestBase
{
    StringAttr id;
public:
    DebugRequestWithId(DebugRequestType _function, memsize_t _proxyId, const char *_id) : DebugRequestBase(_function, _proxyId), id(_id)
    {
    }
    DebugRequestWithId(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
        serialized.read(id);
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        DebugRequestBase::serialize(buf);
        buf.append(id);
    }
};

struct DebugRequestOutputLinksForChildGraph : public DebugRequestWithId
{
public:
    DebugRequestOutputLinksForChildGraph(memsize_t _proxyId, const char *_id) : DebugRequestWithId(DEBUGREQUEST_OUTPUTLINKSFORCHILDGRAPH, _proxyId, _id) {}
    DebugRequestOutputLinksForChildGraph(MemoryBuffer &serialized) : DebugRequestWithId(serialized)
    {
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
            manager->outputLinksForChildGraph(output, id);
        else
            inactive(output);
    }
};

struct DebugRequestLookupActivityByEdgeId : public DebugRequestWithId
{
public:
    DebugRequestLookupActivityByEdgeId(memsize_t _proxyId, const char *_id) : DebugRequestWithId(DEBUGREQUEST_LOOKUPACTIVITYBYEDGEID, _proxyId, _id) {}
    DebugRequestLookupActivityByEdgeId(MemoryBuffer &serialized) : DebugRequestWithId(serialized)
    {
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
        {
            output->outputBeginNested("Result", true);
            IActivityDebugContext *edge = manager->lookupActivityByEdgeId(id);
            if (edge)
                output->outputInt(edge->queryProxyId(), sizeof(int), "@proxyId");
            output->outputEndNested("Result");
        }
        else
            inactive(output);
    }
};

struct DebugRequestPrintEdge  : public DebugRequestBase
{
private:
    unsigned startRow;
    unsigned numRows;
public:
    DebugRequestPrintEdge(memsize_t _proxyId, unsigned _startRow, unsigned _numRows)
        : DebugRequestBase(DEBUGREQUEST_PRINTEDGE, _proxyId), startRow(_startRow), numRows(_numRows)
    {
    }
    DebugRequestPrintEdge(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
        serialized.read(startRow);
        serialized.read(numRows);
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        DebugRequestBase::serialize(buf);
        buf.append(startRow);
        buf.append(numRows);
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IActivityDebugContext> activity = getActivity();
        if (activity)
            activity->printEdge(output, startRow, numRows);
        else
            inactive(output);
    }
};

struct DebugRequestSetRemoveBreakpoint : public DebugRequestBase
{
private:
    Linked<IBreakpointInfo> bp;
    bool isRemove;
public:
    inline DebugRequestSetRemoveBreakpoint(memsize_t _proxyId, IBreakpointInfo &_bp, bool _isRemove)
        : DebugRequestBase(DEBUGREQUEST_SETBREAKPOINT, _proxyId), bp(&_bp), isRemove(_isRemove)
    {
    }
    DebugRequestSetRemoveBreakpoint(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
        bp.setown(new CBreakpointInfo(serialized));
        serialized.read(isRemove);
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        DebugRequestBase::serialize(buf);
        bp->serialize(buf);
        buf.append(isRemove);
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
        {
            if (isRemove)
                manager->queryContext()->removeBreakpoint(*bp);
            else
                manager->queryContext()->addBreakpoint(*bp.getLink());
        }
        else
            inactive(output);
    }
};

struct DebugRequestSearchHistories : public DebugRequestBase
{
private:
    Linked<IRowMatcher> matcher;
    bool fullRows;
public:
    inline DebugRequestSearchHistories(memsize_t _proxyId, IRowMatcher *_matcher, bool _fullRows)
        : DebugRequestBase(DEBUGREQUEST_SEARCHHISTORIES, _proxyId), matcher(_matcher), fullRows(_fullRows)
    {
    }
    DebugRequestSearchHistories(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
        matcher.setown(createRowMatcher(serialized));
        serialized.read(fullRows);
    }
    virtual void serialize(MemoryBuffer &buf)
    {
        DebugRequestBase::serialize(buf);
        matcher->serialize(buf);
        buf.append(fullRows);
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
            manager->searchHistories(output, matcher, fullRows);
        else
            inactive(output);
    }
};

class DebugRequestGetResetGlobalCounts : public DebugRequestBase
{
public:
    inline DebugRequestGetResetGlobalCounts(memsize_t _proxyId)
        : DebugRequestBase(DEBUGREQUEST_GETRESETGLOBALCOUNTS, _proxyId)
    {
    }
    DebugRequestGetResetGlobalCounts(MemoryBuffer &serialized) : DebugRequestBase(serialized)
    {
    }
    virtual void executeRequest(IXmlWriter *output)
    {
        Owned<IDebugGraphManager> manager = getManager();
        if (manager)
            manager->queryContext()->debugCounts(output, 0, true);
        else
            inactive(output);
    }
};

extern void doDebugRequest(IRoxieQueryPacket *packet, const IRoxieContextLogger &logctx)
{
    RoxiePacketHeader newHeader(packet->queryHeader(), ROXIE_DEBUGREQUEST);
    Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
    unsigned contextLength = packet->getContextLength();
    Owned<DebugRequestBase> request;
    MemoryBuffer serialized;
    serialized.setBuffer(contextLength, (void*) packet->queryContextData(), false);
    byte fval;
    serialized.read(fval);
    serialized.reset();
    CommonXmlWriter xml(0);

    switch ((DebugRequestType) fval)
    {
    case DEBUGREQUEST_OUTPUTCHILDGRAPH:
        request.setown(new DebugRequestOutputChildGraph(serialized));
        break;
    case DEBUGREQUEST_OUTPUTLINKSFORCHILDGRAPH:
        request.setown(new DebugRequestOutputLinksForChildGraph(serialized));
        break;
    case DEBUGREQUEST_LOOKUPACTIVITYBYEDGEID:
        request.setown(new DebugRequestLookupActivityByEdgeId(serialized));
        break;
    case DEBUGREQUEST_PRINTEDGE:
        request.setown(new DebugRequestPrintEdge(serialized));
        break;
    case DEBUGREQUEST_SETBREAKPOINT:
        request.setown(new DebugRequestSetRemoveBreakpoint(serialized));
        break;
    case DEBUGREQUEST_SEARCHHISTORIES:
        request.setown(new DebugRequestSearchHistories(serialized));
        break;
    case DEBUGREQUEST_GETRESETGLOBALCOUNTS:
        request.setown(new DebugRequestGetResetGlobalCounts(serialized));
        break;
    default: throwUnexpected();
    }
    request->executeRequest(&xml);
    void *ret = output->getBuffer(xml.length()+1, true);
    memcpy(ret, xml.str(), xml.length()+1);
    output->putBuffer(ret, xml.length()+1, true);
    output->flush(true);
}

class CProxyDebugContext : public CInterface
{
protected:
    memsize_t proxyId;
    unsigned channel;
    Owned<StringContextLogger> logctx;

    void sendProxyRequest(IXmlWriter *output, DebugRequestBase &request) const
    {
        RemoteActivityId id(ROXIE_DEBUGREQUEST, 0);
        ruid_t ruid = getNextRuid();
        RoxiePacketHeader header(id, ruid, channel, 0);
        MemoryBuffer b;
        b.append(sizeof(header), &header);
        b.append ((char) LOGGING_FLAGSPRESENT);
        b.append("PROXY"); // MORE - a better log prefix might be good...
        request.serialize(b);

        Owned<IRowManager> rowManager = roxiemem::createRowManager(1, NULL, *logctx, NULL);
        Owned<IMessageCollator> mc = ROQ->queryReceiveManager()->createMessageCollator(rowManager, ruid);

        Owned<IRoxieQueryPacket> packet = createRoxiePacket(b);
        ROQ->sendPacket(packet, *logctx);

        for (unsigned retries = 1; retries <= MAX_DEBUGREQUEST_RETRIES; retries++)
        {
            bool anyActivity = false;
            Owned<IMessageResult> mr = mc->getNextResult(DEBUGREQUEST_TIMEOUT, anyActivity);
            if (mr)
            {
                unsigned roxieHeaderLen;
                const RoxiePacketHeader *header = (const RoxiePacketHeader *) mr->getMessageHeader(roxieHeaderLen);
                Owned<IMessageUnpackCursor> mu = mr->getCursor(rowManager);
                if (header->activityId == ROXIE_EXCEPTION)
                    throwRemoteException(mu);
                assertex(header->activityId == ROXIE_DEBUGREQUEST);
                RecordLengthType *rowlen = (RecordLengthType *) mu->getNext(sizeof(RecordLengthType));
                assertex(rowlen);
                RecordLengthType len = *rowlen;
                ReleaseRoxieRow(rowlen);
                const char * reply = (const char *) mu->getNext(len);
                if (output)
                {
                    output->outputString(0, NULL, NULL);
                    output->outputQuoted(reply);
                }
                ReleaseRoxieRow(reply);
                ROQ->queryReceiveManager()->detachCollator(mc);
                mc.clear();
                return;
            }
            else if (!anyActivity)
            {
                DBGLOG("Retrying debug request");
                ROQ->sendPacket(packet, *logctx);
            }
        }
        ROQ->queryReceiveManager()->detachCollator(mc);
        mc.clear();
        throwUnexpected(); // MORE - better error
    }

public:
    CProxyDebugContext(unsigned _channel, memsize_t _proxyId) : channel(_channel), proxyId(_proxyId)
    {
        logctx.setown(new StringContextLogger("CProxyDebugContext"));
    }

};

class CProxyActivityDebugContext : public CProxyDebugContext, implements IActivityDebugContext
{
    StringAttr edgeId;
public:
    IMPLEMENT_IINTERFACE;

    CProxyActivityDebugContext(unsigned _channel, memsize_t _proxyId, const char *_edgeId)
      : CProxyDebugContext(_channel, _proxyId), edgeId(_edgeId)
    {
    }

    virtual unsigned queryLastSequence() const { UNIMPLEMENTED; };
    virtual IActivityDebugContext *queryInputActivity() const { UNIMPLEMENTED; };
    virtual void getXGMML(IXmlWriter *output) const { UNIMPLEMENTED; };

    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows) { UNIMPLEMENTED; }
    virtual unsigned queryHistorySize() const { UNIMPLEMENTED; };
    virtual IHistoryRow *queryHistoryRow(unsigned idx) const { UNIMPLEMENTED; };
    virtual unsigned queryHistoryCapacity() const { UNIMPLEMENTED; };
    virtual IBreakpointInfo *debuggerCallback(unsigned sequence, const void *row)
    {
        // was done on slave, don't do here too
        return NULL;
    };
    virtual void setHistoryCapacity(unsigned newCapacity) { UNIMPLEMENTED; };
    virtual void clearHistory() { UNIMPLEMENTED; };
    virtual void printEdge(IXmlWriter *output, unsigned startRow, unsigned numRows) const
    {
        DebugRequestPrintEdge request(proxyId, startRow, numRows);
        sendProxyRequest(output, request);
    };

    virtual void setBreakpoint(IBreakpointInfo &bp) { throwUnexpected(); }
    virtual void removeBreakpoint(IBreakpointInfo &bp) { throwUnexpected(); };

    virtual const char *queryEdgeId() const
    {
        return edgeId;
    };
    virtual const char *querySourceId() const { UNIMPLEMENTED; };
    virtual unsigned queryChildGraphId() const { UNIMPLEMENTED; };

    virtual memsize_t queryProxyId() const { UNIMPLEMENTED; };
};

class CProxyDebugGraphManager : public CProxyDebugContext, implements IDebugGraphManager
{
    unsigned id;
    StringBuffer idString;
    MapStringToMyClass<IActivityDebugContext> edgeProxies;

public:
    IMPLEMENT_IINTERFACE;

    CProxyDebugGraphManager(unsigned _id, unsigned _channel, memsize_t _proxyId)
      : CProxyDebugContext(_channel, _proxyId), id(_id)
    {
        idString.append(_id).append('#').append(channel);
    }

    virtual IActivityDebugContext *lookupActivityByEdgeId(const char *edgeId)
    {
        IActivityDebugContext *edge = edgeProxies.getValue(edgeId);
        if (!edge)
        {
            const char *channelTail = strrchr(edgeId, '#');
            if (channelTail && atoi(channelTail+1)==channel)
            {
                DebugRequestLookupActivityByEdgeId request(proxyId, edgeId);
                CommonXmlWriter reply(0);
                sendProxyRequest(&reply, request);
                Owned<IPropertyTree> response = createPTreeFromXMLString(reply.str());
                if (response)
                {
                    memsize_t proxyId = (memsize_t) response->getPropInt64("@proxyId", 0);
                    if (proxyId)
                    {
                        edge = new CProxyActivityDebugContext(channel, proxyId, edgeId);
                        edgeProxies.setValue(edgeId, edge);
                        ::Release(edge);
                    }
                }
            }
        }
        return edge;
    }

    virtual const char *queryGraphName() const  { UNIMPLEMENTED; }
    virtual void getXGMML(IXmlWriter *output, unsigned sequence, bool isActive)
    {
        throwUnexpected();
    }
    virtual void setBreakpoint(IBreakpointInfo &bp)
    {
        DebugRequestSetRemoveBreakpoint request(proxyId, bp, false);
        sendProxyRequest(NULL, request);
    }
    virtual void removeBreakpoint(IBreakpointInfo &bp)
    {
        DebugRequestSetRemoveBreakpoint request(proxyId, bp, true);
        sendProxyRequest(NULL, request);
    }
    virtual void setHistoryCapacity(unsigned newCapacity)  { UNIMPLEMENTED; }
    virtual void clearHistories()  { UNIMPLEMENTED; }
    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows)
    {
        DebugRequestSearchHistories request(proxyId, matcher, fullRows);
        sendProxyRequest(output, request);
    }
    virtual void setNodeProperty(IActivityBase *node, const char *propName, const char *propVvalue)
    {
        // MORE - should I do anything here?
    }
    virtual DebugActivityRecord *getNodeByActivityBase(IActivityBase *activity) const
    {
        // MORE - should I do anything here?
        return NULL;
    }
    virtual void noteSlaveGraph(IActivityBase *parentActivity, unsigned graphId, unsigned channel, memsize_t remoteGraphId)
    {
        UNIMPLEMENTED; // MORE - can this happen? nested graphs?
    }
    virtual memsize_t queryProxyId() const
    {
        return proxyId;
    }
    virtual const char *queryIdString() const
    {
        return idString.str();
    }
    virtual unsigned queryId() const
    {
        return id;
    }
    virtual void outputChildGraph(IXmlWriter *output, unsigned sequence)
    {
        DebugRequestOutputChildGraph request(proxyId, sequence);
        sendProxyRequest(output, request);
    }
    virtual void outputLinksForChildGraph(IXmlWriter *output, const char *parentId)
    {
        DebugRequestOutputLinksForChildGraph request(proxyId, parentId);
        sendProxyRequest(output, request);
    }

    virtual void serializeProxyGraphs(MemoryBuffer &buff)
    {
        UNIMPLEMENTED;
    }
    virtual void deserializeProxyGraphs(DebugState state, MemoryBuffer &buff, IActivityBase *parentActivity, unsigned channel)
    {
        UNIMPLEMENTED;
    }
    virtual IDebuggableContext *queryContext() const
    {
        UNIMPLEMENTED;
    }
    virtual void mergeRemoteCounts(IDebuggableContext *into) const
    {
        DebugRequestGetResetGlobalCounts request(proxyId);
        CommonXmlWriter reply(0);
        reply.outputBeginNested("Counts", true);
        sendProxyRequest(&reply, request);
        reply.outputEndNested("Counts"); // strange way to do it...
        Owned<IPropertyTree> response = createPTreeFromXMLString(reply.str());
        if (response)
        {
            Owned<IPropertyTreeIterator> edges = response->getElements("edge");
            ForEach(*edges)
            {
                IPropertyTree &edge = edges->query();
                const char *edgeId = edge.queryProp("@edgeId");
                unsigned edgeCount = edge.getPropInt("@count");
                Owned<IGlobalEdgeRecord> thisEdge = into->getEdgeRecord(edgeId);
                thisEdge->incrementCount(edgeCount, into->querySequence());
            }
        }
    }

};

IDebugGraphManager *createProxyDebugGraphManager(unsigned graphId, unsigned channel, memsize_t remoteGraphId)
{
    return new CProxyDebugGraphManager(graphId, channel, remoteGraphId);
}
