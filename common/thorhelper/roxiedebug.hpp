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

#ifndef ROXIEDEBUG_HPP
#define ROXIEDEBUG_HPP

#include "thorhelper.hpp"
#include "roxiedebug.ipp"
#include "jprop.hpp"

// enum BreakpointMode { BreakpointModeNone, BreakpointModeEdge, BreakpointModeNode, BreakpointModeGraph }; in header file
static const char *BreakpointModes[] = {"none", "edge", "node", "graph", "global", NULL };
// enum BreakpointActionMode { BreakpointActionBreak, BreakpointActionSkip, BreakpointActionLimit, BreakpointActionContinue };
static const char *BreakpointActionModes[] = {"break", "skip", "limit", "continue", NULL };
enum BreakpointConditionMode { BreakpointConditionNone, BreakpointConditionEquals, BreakpointConditionContains, BreakpointConditionStartsWith, BreakpointConditionLess, 
     BreakpointConditionGreater, BreakpointConditionLessEqual, BreakpointConditionGreaterEqual, BreakpointConditionNotEqual, BreakpointConditionEOG, BreakpointConditionEOF };
static const char *BreakpointConditionModes[] = {"none", "equals", "contains", "startswith", "<", ">", "<=", ">=", "!=", "eog", "eof", NULL };
enum BreakpointCountMode { BreakpointCountNone, BreakpointCountEquals, BreakpointCountAtleast };
static const char *BreakpointCountModes[] = { "none", "equals" , "atleast", NULL };

//=======================================================================================

class THORHELPER_API CDebugCommandHandler : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    static bool checkCommand(IXmlWriter &out, const char *&supplied, const char *expected);
    void doDebugCommand(IPropertyTree *query, IDebuggerContext *debugContext, IXmlWriter &out);
    void doDebugCommand(IPropertyTree *query, IDebuggerContext *debugContext, FlushingStringBuffer &output)
    {
        CommonXmlWriter out(0, 1);
        doDebugCommand(query, debugContext, out);
        output.append(out.str());
    }
};

//=======================================================================================
interface IActivityDebugContext;
interface IBreakpointInfo : extends IInterface
{
    virtual bool equals(IBreakpointInfo &other) const = 0;
    virtual void toXML(IXmlWriter *output) const = 0;
    virtual bool matches(const void *row, bool isEOF, unsigned edgeRowCount, IOutputMetaData *outputMeta) const = 0;
    virtual bool idMatch(BreakpointMode mode, const char *id) const = 0;
    virtual BreakpointMode queryMode() const = 0;
    virtual BreakpointActionMode queryAction() const = 0;
    virtual void noteEdge(IActivityDebugContext &edge) = 0;
    virtual void removeEdge(IActivityDebugContext &edge) = 0;
    virtual bool canMatchAny(IOutputMetaData *outputMeta) = 0;
    virtual void serialize(MemoryBuffer &to) const = 0;
    virtual unsigned queryUID() const = 0;
};

//=======================================================================================

#define RESULT_FLUSH_THRESHOLD 10000u

#ifdef _DEBUG
#define SOAP_SPLIT_THRESHOLD 100u
#define SOAP_SPLIT_RESERVE 200u
#else
#define SOAP_SPLIT_THRESHOLD 64000u
#define SOAP_SPLIT_RESERVE 65535u
#endif


//=======================================================================================

class HistoryRow : implements IHistoryRow
{
    friend class DebugProbe;
    const void *row;
    unsigned sequence;
    unsigned rowCount;
    bool skipped;
    bool limited;
    bool    rowEof;
    bool    rowEog;

public:
    HistoryRow()
    {
        row = NULL;
        sequence = 0;
        rowCount = 0;
        skipped = false;
        limited = false;
        rowEof = false;
        rowEog = false;
    }
    virtual const void *queryRow() const { return row; }
    virtual unsigned querySequence() const { return sequence; }
    virtual unsigned queryRowCount() const { return rowCount; }

    virtual bool wasSkipped() const { return skipped; } 
    virtual bool wasLimited() const { return limited; } 
    virtual void setSkipped() { skipped = true; }
    virtual void setLimited() { limited = true; }

    virtual bool wasEof() const { return rowEof; }
    virtual bool wasEog() const { return rowEog; }
    virtual void setEof() { rowEof = true; }
    virtual void setEog() { rowEog = true; }
};


//=======================================================================================
extern THORHELPER_API IRowMatcher *createRowMatcher(const char *fieldName, BreakpointConditionMode condition, const char *value, bool caseSensitive);
extern THORHELPER_API IRowMatcher *createRowMatcher(MemoryBuffer &serialized);

class THORHELPER_API CBreakpointInfo : public CInterface, implements IBreakpointInfo
{
private:
    BreakpointMode mode;
    BreakpointActionMode action;
    BreakpointConditionMode condition;
    BreakpointCountMode rowCountMode;
    StringAttr id;
    unsigned rowCount;
    unsigned uid;

    ICopyArrayOf<IActivityDebugContext> activeEdges;

    Owned<IRowMatcher> rowMatcher;

    static SpinLock UIDlock;
    static unsigned nextUIDvalue;
    static inline unsigned nextUID()
    {
        SpinBlock b(UIDlock);
        return ++nextUIDvalue;
    }


    
public:
    IMPLEMENT_IINTERFACE;
    CBreakpointInfo(BreakpointMode _mode, const char *_id, BreakpointActionMode _action,
        const char *_fieldName, BreakpointConditionMode _condition, const char *_value, bool _caseSensitive,
        unsigned _rowCount, BreakpointCountMode _rowCountMode);
    CBreakpointInfo(BreakpointMode _mode);
    CBreakpointInfo(MemoryBuffer &from);
    virtual void serialize(MemoryBuffer &to) const;
    virtual unsigned queryUID() const;
    virtual void noteEdge(IActivityDebugContext &edge);
    virtual void removeEdge(IActivityDebugContext &edge);
    virtual bool equals(IBreakpointInfo &other) const;
    virtual bool matches(const void *row, bool isEOF, unsigned edgeRowCount, IOutputMetaData *outputMeta) const;
    virtual bool idMatch(BreakpointMode _mode, const char *_id) const;
    virtual BreakpointMode queryMode() const;
    virtual BreakpointActionMode queryAction() const;
    virtual void toXML(IXmlWriter *output) const;
    virtual bool canMatchAny(IOutputMetaData *meta);
};

//=======================================================================================
interface IDebugGraphManager;
class THORHELPER_API DebugActivityRecord : public CInterface, implements IInterface
{
public:
    unsigned __int64 totalCycles;
    unsigned __int64 localCycles;
    Linked<IActivityBase> activity;
    unsigned iteration;
    unsigned channel;
    unsigned sequence;
    IArrayOf<IDebugGraphManager> childGraphs;
    StringAttr idText;
    Owned<IProperties> properties;

    IMPLEMENT_IINTERFACE;
    DebugActivityRecord (IActivityBase *_activity, unsigned _iteration, unsigned _channel, unsigned _sequence);
    void outputId(IXmlWriter *output, const char *fieldName);
    const char *queryIdString();
    void outputProperties(IXmlWriter *output);
    void setProperty(const char *propName, const char *propValue, unsigned sequence);
    void updateTimes(unsigned _sequence);
};

class THORHELPER_API DebugEdgeRecord : public CInterface, implements IGlobalEdgeRecord
{
    unsigned count;
    unsigned lastSequence;

public:
    IMPLEMENT_IINTERFACE;
    DebugEdgeRecord(unsigned sequence) { count = 0; lastSequence = 0; }

    // MORE - any of these need locks??
    virtual unsigned queryCount() const { return count; }
    virtual void incrementCount(int inc, unsigned sequence) { count+= inc; lastSequence = sequence; }
    virtual unsigned queryLastSequence() { return lastSequence; }
    virtual void reset() { count = 0; lastSequence = 0; }

};

//=======================================================================================

class THORHELPER_API CBaseDebugContext : public CInterface, implements IDebuggableContext
{
protected:
    enum WatchState
    {
        WatchStateStep,
        WatchStateNext,
        WatchStateOver,
        WatchStateGraph,
        WatchStateQuit,
        WatchStateContinue
    };

    CriticalSection breakCrit;

    mutable MapStringToMyClass<IGlobalEdgeRecord> globalCounts;

    // All of these need to be serialized from server to slave...
    IArrayOf<IBreakpointInfo> breakpoints; // note- must be destroyed AFTER currentActivity/nextActivity or can have issues.

    bool running;
    bool detached;
    bool stopOnLimits;
    bool skipRequested;
    WatchState watchState;
    unsigned sequence;

    // These need to go BACK to server...
    unsigned currentBreakpointUID;

    // These I haven't thought about yet
    IDebugGraphManager *currentGraph;
    unsigned graphChangeSequence;
    unsigned prevGraphChangeSequence;
    bool pendingBreakpointsDone;

    unsigned defaultHistoryCapacity;
    bool executeSequentially;

    // These I think are maintained independently on slave and server
    Linked<DebugActivityRecord> currentNode;
    Linked<IActivityDebugContext> currentActivity;
    Linked<IActivityDebugContext> nextActivity; // Hmmm - needs to be cleared on slave when cleared on server though...
    Linked<IException> currentException; // ditto
    mutable CriticalSection debugCrit;
    const IContextLogger &logctx;
    DebugState currentState; // What program was doing when it was interrupted
    unsigned debuggerActive;
    Semaphore debuggerSem;
    cycle_t debugCyclesAdjust;

    static const char * queryStateString(DebugState state);
    bool _checkPendingBreakpoints(DebugState state, const char *graphName);
//  Owned<IProperties> completedGraphs;
    IBreakpointInfo *findBreakpoint(unsigned uid) const;

public:
    IMPLEMENT_IINTERFACE;
    CBaseDebugContext(const IContextLogger &_logctx);
    virtual void noteGraphChanged();
    virtual unsigned queryChannel() const;
    virtual void debugInitialize(const char *id, const char *_queryName, bool _breakAtStart);
    virtual void debugTerminate();
    virtual BreakpointActionMode checkBreakpoint(DebugState state, IActivityDebugContext *probe, const void *extra);
    virtual void waitForDebugger(DebugState state, IActivityDebugContext *probe) = 0;
    virtual bool onDebuggerTimeout() = 0;
    virtual void noteManager(IDebugGraphManager *mgr);
    virtual void releaseManager(IDebugGraphManager *mgr);
    virtual unsigned querySequence();
    virtual unsigned getDefaultHistoryCapacity() const;
    virtual bool getExecuteSequentially() const;
    virtual void checkDelayedBreakpoints(IActivityDebugContext *edge);
    virtual void serialize(MemoryBuffer &buff) const;
    virtual void deserialize(MemoryBuffer &buff);
    virtual void addBreakpoint(IBreakpointInfo &bp);
    virtual void removeBreakpoint(IBreakpointInfo &bp);
    virtual IGlobalEdgeRecord *getEdgeRecord(const char *edgeId);
    virtual unsigned __int64 getCyclesAdjustment() const;
};

//=======================================================================================
//Shared by CRoxieServerDebugContext and CHThorDebugContext
#define DEBUGEE_TIMEOUT 10000
class THORHELPER_API CBaseServerDebugContext : public CBaseDebugContext, implements IDebuggerContext
{
    // Some questions:
    // 1. Do we let all threads go even when say step? Probably... (may allow a thread to be suspended at some point)
    // 2. Doesn't that then make a bit of a mockery of step (when there are multiple threads active)... I _think_ it actually means we DON'T try to wait for all
    //    threads to hit a stop, but allow any that hit stop while we are paused to be queued up to be returned by step.... perhaps actually stop them in critsec rather than 
    //    semaphore and it all becomes easier to code... Anything calling checkBreakPoint while program state is "in debugger" will block on that critSec.
    // 3. I think we need to recheck breakpoints on server but just check not deleted

protected:
    StringAttr debugId;
    SafeSocket &client;

    Owned<IPropertyTree> queryXGMML;
    Semaphore debugeeSem;

    StringAttr queryName;
    unsigned previousSequence;

    void doStandardResult(IXmlWriter *output) const;
    void _listBreakpoint(IXmlWriter *output, IBreakpointInfo &bp, unsigned idx) const;
    void _continue(WatchState watch) ;
    static unsigned checkOption(const char *supplied, const char *name, const char *accepted[]);
    IBreakpointInfo *createDummyBreakpoint();
    IBreakpointInfo *_createBreakpoint(const char *modeString, const char *id, const char *action=NULL,
                                   const char *fieldName = NULL, const char *condition=NULL, const char *value=NULL, bool caseSensitive=false,
                                   unsigned hitCount=0, const char *hitCountMode=NULL);
    virtual void waitForDebugger(DebugState state, IActivityDebugContext *probe);
public:
    IMPLEMENT_IINTERFACE;
    CBaseServerDebugContext(const IContextLogger &_logctx, IPropertyTree *_queryXGMML, SafeSocket &_client) ;
    void serializeBreakpoints(MemoryBuffer &to);
    virtual void debugInitialize(const char *id, const char *_queryName, bool _breakAtStart);
    virtual void debugTerminate();
    virtual void addBreakpoint(IXmlWriter *output, const char *modeString, const char *id, const char *action, 
                               const char *fieldName, const char *condition, const char *value, bool caseSensitive,
                               unsigned hitCount, const char *hitCountMode);
    virtual void removeBreakpoint(IXmlWriter *output, unsigned removeIdx);
    virtual void removeAllBreakpoints(IXmlWriter *output);
    virtual void listBreakpoint(IXmlWriter *output, unsigned listIdx) const;
    virtual void listAllBreakpoints(IXmlWriter *output) const;
    virtual void debugInterrupt(IXmlWriter *output);
    virtual void debugContinue(IXmlWriter *output, const char *modeString, const char *id);
    virtual void debugRun(IXmlWriter *output) ;
    virtual void debugQuit(IXmlWriter *output) ;
    virtual void debugSkip(IXmlWriter *output);
    virtual void debugStatus(IXmlWriter *output) const;
    virtual void debugStep(IXmlWriter *output, const char *modeString);
    virtual void debugNext(IXmlWriter *output);
    virtual void debugOver(IXmlWriter *output);
    virtual void debugChanges(IXmlWriter *output, unsigned sinceSequence) const;
    virtual void debugCounts(IXmlWriter *output, unsigned sinceSequence, bool reset);
    virtual void debugWhere(IXmlWriter *output) const;
    virtual void debugSearch(IXmlWriter *output, const char *fieldName, const char *condition, const char *value, bool caseSensitive, bool fullRows) const;
    virtual void debugPrint(IXmlWriter *output, const char *edgeId, unsigned startRow, unsigned numRows) const;
    virtual void debugGetConfig(IXmlWriter *output, const char *name, const char *id) const;
    virtual void debugSetConfig(IXmlWriter *output, const char *name, const char *value, const char *id);
    virtual void getCurrentGraphXGMML(IXmlWriter *output, bool original) const;
    virtual void getQueryXGMML(IXmlWriter *output) const;
    virtual void getGraphXGMML(IXmlWriter *output, const char *graphName) const;
    virtual const char *queryQueryName() const;
    virtual const char *queryDebugId() const;
    virtual void debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const;
};

//=======================================================================================

class THORHELPER_API CBaseDebugGraphManager : public CInterface, implements IProbeManager, implements IDebugGraphManager
{
    class DebugDependencyRecord : public CInterface, implements IInterface
    {
    public:
        IActivityBase *sourceActivity;
        unsigned sourceIndex;
        unsigned controlId;
        unsigned sequence;
        const char *edgeId;
        IActivityBase *targetActivity;
    
        IMPLEMENT_IINTERFACE;
        DebugDependencyRecord(IActivityBase *_sourceActivity, unsigned _sourceIndex, unsigned _controlId, const char *_edgeId, IActivityBase *_targetActivity, unsigned _sequence)
            : sourceActivity(_sourceActivity),
              sourceIndex(_sourceIndex),
              controlId(_controlId),
              edgeId(_edgeId),
              targetActivity(_targetActivity),
              sequence(_sequence)
        {
        }
    };
protected:
    IArrayOf<IDebugGraphManager> childGraphs;
    MapStringToMyClass<IActivityDebugContext> allProbes;
    MapXToMyClass<IActivityBase *, IActivityBase *, DebugActivityRecord> allActivities;
    IArrayOf<IActivityBase> sinks;
    IArrayOf<DebugDependencyRecord> dependencies;

    IDebuggableContext *debugContext;
    StringAttr graphName;
    mutable CriticalSection crit;
    unsigned id;
    StringBuffer idString;
    mutable memsize_t proxyId; // MORE - does it need to be threadsafe?

    DebugActivityRecord *noteActivity(IActivityBase *activity, unsigned iteration, unsigned channel, unsigned sequence);
    void outputLinksForChildGraph(IXmlWriter *output, const char *parentId);
    void outputChildGraph(IXmlWriter *output, unsigned sequence);
public:
    virtual void Link() const;
    virtual bool Release() const;
    CBaseDebugGraphManager(IDebuggableContext *_debugContext, unsigned _id, const char *_graphName);
    virtual IDebugGraphManager *queryDebugManager();
    virtual const char *queryIdString() const;
    virtual unsigned queryId() const;
    virtual IInputBase *createProbe(IInputBase *in, IActivityBase *sourceAct, IActivityBase *targetAct, unsigned sourceIdx, unsigned targetIdx, unsigned iteration)
    {
        UNIMPLEMENTED;
    }

    virtual void noteSink(IActivityBase *sink);
    virtual void noteDependency(IActivityBase *sourceActivity, unsigned sourceIndex, unsigned controlId, const char *edgeId, IActivityBase *targetActivity);
    virtual DebugActivityRecord *getNodeByActivityBase(IActivityBase *activity) const;
    virtual void setNodeProperty(IActivityBase *node, const char *propName, const char *propValue);
    virtual void setNodePropertyInt(IActivityBase *node, const char *propName, unsigned __int64 propValue);
    virtual void getProbeResponse(IPropertyTree *query);
    virtual void setHistoryCapacity(unsigned newCapacity);
    virtual void clearHistories();
    virtual void setBreakpoint(IBreakpointInfo &bp);
    virtual void removeBreakpoint(IBreakpointInfo &bp);
    IActivityDebugContext *lookupActivityByEdgeId(const char *edgeId);
    virtual const char *queryGraphName() const;
    virtual void getXGMML(IXmlWriter *output, unsigned sequence, bool isActive);
    virtual void searchHistories(IXmlWriter *output, IRowMatcher *matcher, bool fullRows);
    virtual memsize_t queryProxyId() const
    {
        UNIMPLEMENTED;
    }

    virtual void serializeProxyGraphs(MemoryBuffer &buff);
    virtual void deserializeProxyGraphs(DebugState state, MemoryBuffer &buff, IActivityBase *parentActivity, unsigned channel)
    {
        UNIMPLEMENTED;
    }
    virtual void mergeRemoteCounts(IDebuggableContext *into) const;

    virtual IProbeManager *startChildGraph(unsigned childGraphId, IActivityBase *parent)
    {
        UNIMPLEMENTED;
    }
    virtual void endChildGraph(IProbeManager *child, IActivityBase *parent);
    virtual void deleteGraph(IArrayOf<IActivityBase> *activities, IArrayOf<IInputBase> *probes)
    {
        UNIMPLEMENTED;
    }
    virtual IDebuggableContext *queryContext() const;
};

#endif // ROXIEDEBUG_HPP;
