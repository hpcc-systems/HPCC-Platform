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

#ifndef ROXIEDEBUG_IPP
#define ROXIEDEBUG_IPP

#include "thorhelper.hpp"
#include "thorcommon.hpp"
#include "roxiehelper.ipp"

enum DebugState
{
    DebugStateCreated,
    DebugStateLoading,
    DebugStateRunning,
    DebugStateEdge,
    DebugStateBreakpoint,
    DebugStateGraphCreate,
    DebugStateGraphStart,
    DebugStateGraphEnd,
    DebugStateException,
    DebugStateGraphAbort,
    DebugStateReady,
    DebugStateFailed,
    DebugStateFinished,
    DebugStateUnloaded,
    DebugStateQuit,
    DebugStateDetached,
    DebugStateLimit,
    DebugStateGraphFinished
};

enum BreakpointMode
{
    BreakpointModeNone,
    BreakpointModeEdge,
    BreakpointModeNode,
    BreakpointModeGraph,
    BreakpointModeGlobal
};

enum BreakpointActionMode 
{ 
    BreakpointActionBreak, 
    BreakpointActionSkip, 
    BreakpointActionLimit,
    BreakpointActionContinue
};

//==============================================================================================================

interface IRowMatcher : extends IXmlWriter 
{
    virtual void reset() = 0;
    virtual bool matched() const = 0;
    virtual const char *queryFieldName() const = 0;
    virtual const char *queryValue() const = 0;
    virtual bool queryCaseSensitive() const = 0;
    virtual bool canMatchAny(IOutputMetaData *meta) = 0;
    virtual void serialize(MemoryBuffer &out) const = 0;
};

interface IHistoryRow
{
    virtual const void *queryRow() const = 0;
    virtual unsigned querySequence() const = 0;
    virtual unsigned queryRowCount() const = 0;
    
    virtual bool wasSkipped() const = 0;
    virtual bool wasLimited() const = 0;
    virtual bool wasEof() const = 0;
    virtual bool wasEog() const = 0;

    virtual void setSkipped() = 0;
    virtual void setLimited() = 0;
    virtual void setEof() = 0;
    virtual void setEog() = 0;
};

interface IGlobalEdgeRecord : extends IInterface
{
    virtual unsigned queryCount() const = 0;
    virtual void incrementCount(int inc, unsigned sequence) = 0;
    virtual unsigned queryLastSequence() = 0;
    virtual void reset() = 0;
};

//==============================================================================================================

class DebugActivityRecord;
typedef MapXToMyClass<IActivityBase *, IActivityBase *, DebugActivityRecord> DebugActivityMap;

interface IBreakpointInfo;
interface IActivityDebugContext : extends IInterface
{
    virtual unsigned queryLastSequence() const = 0;
    virtual IActivityDebugContext *queryInputActivity() const = 0;
    virtual void getXGMML(IXmlWriter *output) const = 0;

    virtual unsigned queryHistorySize() const = 0;
    virtual IHistoryRow *queryHistoryRow(unsigned idx) const = 0;
    virtual unsigned queryHistoryCapacity() const = 0;
    virtual IBreakpointInfo *debuggerCallback(unsigned sequence, const void *row) = 0; // NOTE - we only add to history when called back to do so, to ensure consistent timing.
    virtual void setHistoryCapacity(unsigned newCapacity) = 0;
    virtual void clearHistory() = 0;
    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows) = 0;
    virtual void printEdge(IXmlWriter *output, unsigned startRow, unsigned numRows) const = 0;

    virtual void setBreakpoint(IBreakpointInfo &bp) = 0;
    virtual void removeBreakpoint(IBreakpointInfo &bp) = 0;

    virtual const char *queryEdgeId() const = 0;
    virtual const char *querySourceId() const = 0;

    virtual memsize_t queryProxyId() const = 0;
};

//==============================================================================================================

interface IDebuggableContext;

interface IDebugGraphManager : extends IInterface
{
    virtual IActivityDebugContext *lookupActivityByEdgeId(const char *edgeId) = 0;
    virtual const char *queryGraphName() const = 0;
    virtual void getXGMML(IXmlWriter *output, unsigned sequence, bool isActive) = 0;
    virtual IDebuggableContext *queryContext() const= 0;
    virtual void setBreakpoint(IBreakpointInfo &bp) = 0;
    virtual void removeBreakpoint(IBreakpointInfo &bp) = 0;
    virtual void setHistoryCapacity(unsigned newCapacity) = 0;
    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows) = 0;
    virtual void clearHistories() = 0;
    virtual memsize_t queryProxyId() const = 0;
    virtual unsigned queryId() const = 0;
    virtual const char *queryIdString() const = 0;
    virtual void outputChildGraph(IXmlWriter *output, unsigned sequence) = 0;
    virtual void outputLinksForChildGraph(IXmlWriter *output, const char *parentId) = 0;

    virtual void serializeProxyGraphs(MemoryBuffer &buff) = 0;
    virtual void deserializeProxyGraphs(DebugState state, MemoryBuffer &buff, IActivityBase *parentActivity, unsigned channel) = 0;
    virtual void mergeRemoteCounts(IDebuggableContext *into) const = 0;
    
    virtual void setNodeProperty(IActivityBase *node, const char *propName, const char *propValue) = 0;
    virtual DebugActivityRecord *getNodeByActivityBase(IActivityBase *activity) const = 0;
};

//==============================================================================================================
interface IRoxieProbe : public IInterface
{
    virtual IInputBase &queryInput() = 0;
};

interface IProbeManager : public IInterface
{
    virtual IRoxieProbe *createProbe(IInputBase *in, IActivityBase *inAct, IActivityBase *outAct, unsigned sourceIdx, unsigned targetIdx, unsigned iteration) = 0;
    virtual void getProbeResponse(IPropertyTree *query) = 0;
    virtual void noteSink(IActivityBase *sink) = 0;
    virtual void noteDependency(IActivityBase *sourceActivity, unsigned sourceIndex, unsigned controlId, const char *edgeId, IActivityBase *targetActivity) = 0;
    virtual IProbeManager *startChildGraph(unsigned id, IActivityBase *parent) = 0;
    virtual void endChildGraph(IProbeManager *child, IActivityBase *parent) = 0;
    virtual void deleteGraph(IArrayOf<IActivityBase> *activities, IArrayOf<IInputBase> *probes) = 0;
    virtual IDebugGraphManager *queryDebugManager() = 0;
    virtual void setNodeProperty(IActivityBase *node, const char *propName, const char *propValue) = 0;
    virtual void setNodePropertyInt(IActivityBase *node, const char *propName, unsigned __int64 propValue) = 0;
};
//==============================================================================================================

interface IDebuggerContext : extends IInterface
{
// called by debugger
    virtual void debugContinue(IXmlWriter *output, const char *modeString, const char *id) = 0;
    virtual void addBreakpoint(IXmlWriter *output, const char *modeString, const char *id, const char *action, 
                               const char *fieldName, const char *condition, const char *value, bool caseSensitive,
                               unsigned hitCount, const char *hitCountMode) = 0;
    virtual void removeBreakpoint(IXmlWriter *output, unsigned removeIdx) = 0;
    virtual void removeAllBreakpoints(IXmlWriter *output) = 0;
    virtual void listBreakpoint(IXmlWriter *output, unsigned idx) const = 0;
    virtual void listAllBreakpoints(IXmlWriter *output) const = 0;
    virtual void debugInterrupt(IXmlWriter *output) = 0;
    virtual void debugNext(IXmlWriter *output) = 0;
    virtual void debugOver(IXmlWriter *output) = 0;
    virtual void debugQuit(IXmlWriter *output) = 0;
    virtual void debugRun(IXmlWriter *output) = 0;
    virtual void debugSearch(IXmlWriter *output, const char *fieldName, const char *condition, const char *value, bool caseSensitive, bool fullRows) const = 0;
    virtual void debugSkip(IXmlWriter *output) = 0;
    virtual void debugStatus(IXmlWriter *output) const = 0;
    virtual void debugStep(IXmlWriter *output, const char *modeString) = 0;
    virtual void debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const = 0;
    
    // restart? set params?

    virtual void debugChanges(IXmlWriter *output, unsigned sequence) const = 0;
    virtual void debugCounts(IXmlWriter *output, unsigned sequence, bool reset) = 0;
    virtual void debugGetConfig(IXmlWriter *output, const char *name, const char *id) const = 0;
    virtual void debugSetConfig(IXmlWriter *output, const char *name, const char *value, const char *id) = 0;
    virtual void debugWhere(IXmlWriter *output) const = 0;
    virtual void debugPrint(IXmlWriter *output, const char *edgeId, unsigned startRow, unsigned numRows) const = 0;

    virtual void getCurrentGraphXGMML(IXmlWriter *output, bool original) const = 0;
    virtual void getQueryXGMML(IXmlWriter *output) const = 0;
    virtual void getGraphXGMML(IXmlWriter *output, const char *graphName) const = 0;

    virtual const char *queryQueryName() const = 0;
    virtual const char *queryDebugId() const = 0;
};

//==============================================================================================================
class RoxiePacketHeader;

interface IDeserializedRoxieQueryPacket;

interface IDebuggableContext : public IInterface
{
// Called by program being debugged
    virtual void debugInitialize(const char *id, const char *queryName, bool breakAtStart) = 0;
    virtual void debugTerminate() = 0;
    virtual BreakpointActionMode checkBreakpoint(DebugState programState, IActivityDebugContext *probe, const void *extra) = 0; // true means skip current row
    virtual void noteManager(IDebugGraphManager *mgr) = 0; // NOTE - don't link it!
    virtual void releaseManager(IDebugGraphManager *mgr) = 0;
    virtual unsigned querySequence() = 0;
    virtual void checkDelayedBreakpoints(IActivityDebugContext *newProbe) = 0;
    virtual unsigned getDefaultHistoryCapacity() const = 0;
    virtual bool getExecuteSequentially() const = 0;
    virtual unsigned queryChannel() const = 0;
    virtual IDeserializedRoxieQueryPacket *onDebugCallback(const RoxiePacketHeader &header, size32_t len, char *data) = 0;
    virtual void serialize(MemoryBuffer &buff) const = 0;
    virtual void noteGraphChanged() = 0;
    virtual void addBreakpoint(IBreakpointInfo &bp) = 0;
    virtual void removeBreakpoint(IBreakpointInfo &bp) = 0;
    virtual IGlobalEdgeRecord *getEdgeRecord(const char *edgeId) = 0;
    virtual void debugCounts(IXmlWriter *output, unsigned sequence, bool reset) = 0;
};

//=======================================================================================

#endif // ROXIEDEBUG_IPP
