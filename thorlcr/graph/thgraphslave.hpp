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

#ifndef _THGRAPHSLAVE_HPP_
#define _THGRAPHSLAVE_HPP_

#ifdef GRAPHSLAVE_EXPORTS
    #define graphslave_decl DECL_EXPORT
#else
    #define graphslave_decl DECL_IMPORT
#endif

#include "platform.h"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorcommon.hpp"
#include "thgraph.hpp"
#include "jdebug.hpp"
#include "traceslave.hpp"
#include "thorstrand.hpp"

interface IStartableEngineRowStream : extends IEngineRowStream
{
    virtual void start() = 0;
};

class COutputTiming
{
public:
    ActivityTimeAccumulator slaveTimerStats;

    COutputTiming() { }

    void resetTiming() { slaveTimerStats.reset(); }
    ActivityTimeAccumulator &getTotalCyclesRef() { return slaveTimerStats; }
    unsigned __int64 queryTotalCycles() const { return slaveTimerStats.totalCycles; }
    unsigned __int64 queryEndCycles() const { return slaveTimerStats.endCycles; }
    unsigned __int64 queryBlockedCycles() const { return slaveTimerStats.blockedCycles; }
};

class CEdgeProgress
{
    CActivityBase &owner;
    rowcount_t count = 0, icount = 0;
    unsigned outputId = 0;
public:
    explicit CEdgeProgress(CActivityBase *_owner) : owner(*_owner) { }
    explicit CEdgeProgress(CActivityBase *_owner, unsigned _outputId) : owner(*_owner), outputId(_outputId) { }

    inline void dataLinkStart()
    {
#ifdef _TESTING
        owner.ActPrintLog("ITDL starting for output %d", outputId);
#endif
        if (hasStarted())
        {
            if (!hasStopped())
                throw MakeActivityException(&owner, 0, "Starting without being stopped 1st");
            else
                throw MakeActivityException(&owner, 0, "Started and stopped states both set");
        }
        icount = 0;
        count = (count & THORDATALINK_COUNT_MASK) | THORDATALINK_STARTED;
    }

    inline void dataLinkStop()
    {
        if (hasStarted())
            count = (count & THORDATALINK_COUNT_MASK) | THORDATALINK_STOPPED;
#ifdef _TESTING
        owner.ActPrintLog("ITDL output %d stopped, count was %" RCPF "d", outputId, getDataLinkCount());
#endif
    }
    inline void dataLinkIncrement() { dataLinkIncrement(1); }
    inline void dataLinkIncrement(rowcount_t v)
    {
#ifdef _TESTING
        assertex(hasStarted());
#ifdef OUTPUT_RECORDSIZE
        if (count==THORDATALINK_STARTED)
        {
            size32_t rsz = parent.queryRowMetaData(this)->getMinRecordSize();
            parent.ActPrintLog("Record size %s= %d", parent.queryRowMetaData(this)->isVariableSize()?"(min) ":"",rsz);
        }
#endif
#endif
        icount += v;
        count += v;
    }
    inline bool hasStarted() const { return (count & THORDATALINK_STARTED) ? true : false; }
    inline bool hasStopped() const { return (count & THORDATALINK_STOPPED) ? true : false; }
    inline void dataLinkSerialize(MemoryBuffer &mb) const { mb.append(count); }
    inline rowcount_t getDataLinkGlobalCount() { return (count & THORDATALINK_COUNT_MASK); }
    inline rowcount_t getDataLinkCount() const { return icount; }
    inline rowcount_t getCount() const { return count; }
};

class CThorInput : public CSimpleInterfaceOf<IInterface>
{
    Linked<IEngineRowStream> stream;
    Linked<IStartableEngineRowStream> lookAhead;


    void _startLookAhead()
    {
        assertex(nullptr != lookAhead);
        lookAhead->start();
        lookAheadActive = true;
    }
public:
    unsigned sourceIdx = 0;
    Linked<IThorDataLink> itdl;
    Linked<IThorDebug> tracingStream;
    Linked<IStrandJunction> junction;
    bool stopped = false;
    bool started = false;
    bool persistentLookAhead = false;
    bool lookAheadActive = false;

    explicit CThorInput() { }
    void set(IThorDataLink *_itdl, unsigned idx) { itdl.set(_itdl); sourceIdx = idx; }
    void reset()
    {
        started = stopped = false;
        resetJunction(junction);
    }
    bool isStopped() const { return stopped; }
    bool isStarted() const { return started; }
    bool isLookAheadActive() const { return lookAheadActive; }
    IEngineRowStream *queryStream() const
    {
        if (lookAhead && lookAheadActive)
            return lookAhead;
        else
            return stream;
    }
    void setStream(IEngineRowStream *_stream) { stream.setown(_stream); }
    bool hasLookAhead() const { return nullptr != lookAhead; }
    void setLookAhead(IStartableEngineRowStream *_lookAhead, bool persistent)
    {
        dbgassertex(!persistentLookAhead); // If persistent, must only be called once

        /* NB: if persistent, must be installed before starting input, e.g. during setInputStream wiring.
         * if not persistent, must be installed after input started, e.g. in start() after startInput(x).
         */
        dbgassertex((persistent && !isStarted()) || (!persistent && isStarted()));

        lookAhead.setown(_lookAhead); // if pre-existing lookAhead, this will replace.
        persistentLookAhead = persistent;
    }
    void startLookAhead()
    {
        dbgassertex(!persistentLookAhead);
        dbgassertex(isStarted());
        _startLookAhead();
    }
    void start()
    {
        itdl->start();
        startJunction(junction);
        if (persistentLookAhead)
            _startLookAhead();
        stopped = false;
        started = true;
    }
    void stop()
    {
        // NB: lookAhead can be installed but not used
        if (lookAheadActive)
        {
            lookAhead->stop();
            lookAheadActive = false;
        }
        else if (stream)
            stream->stop();
        stopped = true;
    }
    bool isFastThrough() const;
};
typedef IArrayOf<CThorInput> CThorInputArray;

class CSlaveGraphElement;
class graphslave_decl CSlaveActivity : public CActivityBase, public CEdgeProgress, public COutputTiming, implements IThorDataLink, implements IEngineRowStream, implements IThorSlaveActivity
{
    mutable MemoryBuffer *data;
    mutable CriticalSection crit;

protected:
    CThorInputArray inputs;
    IPointerArrayOf<IThorDataLink> outputs;
    IPointerArrayOf<IEngineRowStream> outputStreams;
    IThorDataLink *input = nullptr;
    bool inputStopped = false;
    unsigned inputSourceIdx = 0;
    IEngineRowStream *inputStream = nullptr;
    MemoryBuffer startCtx;
    bool optStableInput = true; // is the input forced to ordered?
    bool optUnstableInput = false;  // is the input forced to unordered?
    bool optUnordered = false; // is the output specified as unordered?

protected:
    unsigned __int64 queryLocalCycles() const;
    bool ensureStartFTLookAhead(unsigned index);
    bool isInputFastThrough(unsigned index) const;
    bool hasLookAhead(unsigned index) const;
    void setLookAhead(unsigned index, IStartableEngineRowStream *lookAhead, bool persistent);
    void startLookAhead(unsigned index);
    bool isLookAheadActive(unsigned index) const;

public:
    IMPLEMENT_IINTERFACE_USING(CActivityBase)

    CSlaveActivity(CGraphElementBase *container);
    ~CSlaveActivity();
    void setRequireInitData(bool tf)
    {
        // If not required, sets sentActInitdata to true, to prevent it being requested at graph initialization time.
        container.sentActInitData->set(0, !tf);
    }
    virtual void clearConnections();
    virtual void releaseIOs();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const;
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx) override;
    virtual void connectInputStreams(bool consumerOrdered);

    IThorDataLink *queryOutput(unsigned index) const;
    IThorDataLink *queryInput(unsigned index) const;
    IEngineRowStream *queryInputStream(unsigned index) const;
    IStrandJunction *queryInputJunction(unsigned index) const;
    IEngineRowStream *queryOutputStream(unsigned index) const;
    inline bool queryInputStarted(unsigned input) const { return inputs.item(input).isStarted(); }
    inline bool queryInputStopped(unsigned input) const { return inputs.item(input).isStopped(); }
    unsigned queryInputOutputIndex(unsigned inputIndex) const { return inputs.item(inputIndex).sourceIdx; }
    unsigned queryNumInputs() const { return inputs.ordinality(); }
    void appendOutput(IThorDataLink *itdl);
    void appendOutputLinked(IThorDataLink *itdl);
    void startInput(unsigned index, const char *extra=NULL);
    void startAllInputs();
    void stopInput(unsigned index, const char *extra=NULL);
    void stopAllInputs();
    virtual void serializeStats(MemoryBuffer &mb);
    virtual void serializeActivityStats(MemoryBuffer &mb) const;
    void debugRequest(unsigned edgeIdx, MemoryBuffer &msg);
    bool canStall() const;
    bool isFastThrough() const;


// IThorDataLink
    virtual CSlaveActivity *queryFromActivity() override { return this; }
    virtual IStrandJunction *getOutputStreams(CActivityBase &_ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) override;
    virtual void setOutputStream(unsigned index, IEngineRowStream *stream) override;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override { }
    virtual bool isGrouped() const override;
    virtual IOutputMetaData * queryOutputMeta() const;
    virtual void dataLinkSerialize(MemoryBuffer &mb) const override;
    virtual rowcount_t getProgressCount() const override;
    virtual bool isInputOrdered(bool consumerOrdered) const override
    {
        if (optStableInput)
            return true;
        if (optUnstableInput)
            return false;
        if (optUnordered)
            return false;
        return consumerOrdered;
    }
    virtual unsigned __int64 queryTotalCycles() const { return COutputTiming::queryTotalCycles(); }
    virtual unsigned __int64 queryBlockedCycles() const { return COutputTiming::queryBlockedCycles();}
    virtual unsigned __int64 queryEndCycles() const { return COutputTiming::queryEndCycles(); }
    virtual void debugRequest(MemoryBuffer &msg) override;

// IThorDataLink
    virtual void start() override;

// IEngineRowStream
    virtual const void *nextRow() override { throwUnexpected(); }
    virtual void stop() override;
    virtual void resetEOF() override { throwUnexpected(); }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &in, MemoryBuffer &out) { }
    virtual void setInputStream(unsigned index, CThorInput &input, bool consumerOrdered) override;
    virtual void processDone(MemoryBuffer &mb) override { };
    virtual void reset() override;
};


class graphslave_decl CSlaveLateStartActivity : public CSlaveActivity
{
    bool prefiltered = false;
    Owned<CThorInput> nullInput;

protected:
    void lateStart(bool any);

public:
    CSlaveLateStartActivity(CGraphElementBase *container) : CSlaveActivity(container)
    {
    }
    virtual void start() override;
    virtual void stop() override;
    virtual void reset() override;
};

graphslave_decl IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, Owned<IStrandJunction> &junction, bool consumerOrdered);
graphslave_decl IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, bool consumerOrdered);


#define STRAND_CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            parent->processAndThrowOwnedException(_e); \
        }

#define STRAND_CATCH_NEXTROW() \
    virtual const void *nextRow() override \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))


class CThorStrandedActivity;
class graphslave_decl CThorStrandProcessor : public CInterfaceOf<IEngineRowStream>, public COutputTiming
{
protected:
    CThorStrandedActivity &parent;
    IEngineRowStream *inputStream;
    rowcount_t numProcessedLastGroup = 0;
    const bool timeActivities;
    bool stopped = false;
    unsigned outputId; // if activity had >1 , this identifies (for tracing purposes) which output this strand belongs to.
    Linked<IHThorArg> baseHelper;
    rowcount_t rowsProcessed;

protected:
    inline IHThorArg *queryHelper() const { return baseHelper; }

public:
    explicit CThorStrandProcessor(CThorStrandedActivity &_parent, IEngineRowStream *_inputStream, unsigned _outputId);
    __declspec(noreturn) void processAndThrowOwnedException(IException *_e) __attribute__((noreturn));
    rowcount_t getCount() const { return rowsProcessed; }
    virtual void start()
    {
        rowsProcessed = 0;
        numProcessedLastGroup = 0;
        resetTiming();
    }
    virtual void reset()
    {
        rowsProcessed = 0;
        stopped = false;
    }

// IRowStream
    virtual void stop() override;
// IEngineRowStream
    virtual void resetEOF() override
    {
        inputStream->resetEOF();
    }
};

class graphslave_decl CThorStrandedActivity : public CSlaveActivity
{
protected:
    CThorStrandOptions strandOptions;
    IArrayOf<CThorStrandProcessor> strands;
    Owned<IStrandBranch> branch;
    Owned<IStrandJunction> splitter;
    Owned<IStrandJunction> sourceJunction; // A junction applied to the output of a source activity
    std::atomic<unsigned> active;
protected:
    void onStartStrands();
public:
    CThorStrandedActivity(CGraphElementBase *container)
        : CSlaveActivity(container), strandOptions(*container), active(0)
    {
    }

    void strandedStop();

    virtual void start() override;
    virtual void reset() override;
    virtual CThorStrandProcessor *createStrandProcessor(IEngineRowStream *instream) = 0;

    //MORE: Possibly this class should be split into two for sinks and non sinks...
    virtual CThorStrandProcessor *createStrandSourceProcessor(bool inputOrdered) = 0;

    inline unsigned numStrands() const { return strands.ordinality(); }

// IThorDataLink
    virtual IStrandJunction *getOutputStreams(CActivityBase &_ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered, IOrderedCallbackCollection * orderedCallbacks) override;
    virtual unsigned __int64 queryTotalCycles() const override;
    virtual void dataLinkSerialize(MemoryBuffer &mb) const override;
    virtual rowcount_t getProgressCount() const override;
};


class graphslave_decl CSlaveGraphElement : public CGraphElementBase
{
public:
    CSlaveGraphElement(CGraphBase &owner, IPropertyTree &xgmml) : CGraphElementBase(owner, xgmml)
    {
    }
};

class CJobSlave;
class graphslave_decl CSlaveGraph : public CGraphBase
{
    CJobSlave *jobS;
    Semaphore getDoneSem;
    CriticalSection progressCrit;
    bool doneInit = false;
    std::atomic_bool progressActive;

public:

    CSlaveGraph(CJobChannel &jobChannel);
    ~CSlaveGraph() { }

    void connect();
    void init(MemoryBuffer &mb);
    bool recvActivityInitData(size32_t parentExtractSz, const byte *parentExtract);
    void setExecuteReplyTag(mptag_t _executeReplyTag) { executeReplyTag = _executeReplyTag; }
    void initWithActData(MemoryBuffer &in, MemoryBuffer &out);
    void getDone(MemoryBuffer &doneInfoMb);
    void serializeDone(MemoryBuffer &mb);
    IThorResult *getGlobalResult(CActivityBase &activity, IThorRowInterfaces *rowIf, activity_id ownerId, unsigned id);

    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract) override;
    virtual bool serializeStats(MemoryBuffer &mb);
    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract) override;
    virtual void start() override;
    virtual void abort(IException *e) override;
    virtual void reset() override;
    virtual void done() override;
    virtual IThorGraphResults *createThorGraphResults(unsigned num);

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        StringBuffer s;
        if (!te || !te->queryGraphId())
        {
            Owned<IThorException> e2 = MakeGraphException(this, e);
            e2->setAudience(e->errorAudience());
            return CGraphBase::fireException(e2);
        }
        else
            return CGraphBase::fireException(e);
    }
};

interface ISlaveWatchdog;
class graphslave_decl CJobSlave : public CJobBase
{
    typedef CJobBase PARENT;
    ISlaveWatchdog *watchdog;
    Owned<IPropertyTree> workUnitInfo;
    size32_t oldNodeCacheMem;
    unsigned channelMemoryMB;
    unsigned actInitWaitTimeMins = DEFAULT_MAX_ACTINITWAITTIME_MINS;

public:
    IMPLEMENT_IINTERFACE;

    CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *workUnitInfo, const char *graphName, ILoadedDllEntry *querySo, mptag_t _slavemptag);

    virtual CJobChannel *addChannel(IMPServer *mpServer) override;
    virtual void startJob() override;
    virtual void endJob() override;
    const char *queryFindString() const { return key.get(); } // for string HT
    unsigned queryActInitWaitTimeMins() const { return actInitWaitTimeMins; }

    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing);
    ISlaveWatchdog *queryProgressHandler() { return watchdog; }
    void reportGraphEnd(graph_id gid);

    virtual mptag_t deserializeMPTag(MemoryBuffer &mb);
    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const;
    virtual bool getWorkUnitValueBool(const char *prop, bool defVal) const;
    virtual IThorAllocator *getThorAllocator(unsigned channel);
    virtual void debugRequest(MemoryBuffer &msg, const char *request) const;

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        return queryJobChannel(0).fireException(e);
    }
// IThreadFactory
    IPooledThread *createNew();
};

class graphslave_decl CJobSlaveChannel : public CJobChannel
{
    CriticalSection graphRunCrit;
public:
    CJobSlaveChannel(CJobBase &job, IMPServer *mpServer, unsigned channel);

    virtual IBarrier *createBarrier(mptag_t tag);
    virtual CGraphBase *createGraph()
    {
        return new CSlaveGraph(*this);
    }
 // IGraphCallback
    virtual void runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract);
// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        CMessageBuffer msg;
        msg.append((int)smt_errorMsg);
        msg.append(queryMyRank()-1);
        IThorException *te = QUERYINTERFACE(e, IThorException);
        bool userOrigin = false;
        if (te)
        {
            te->setJobId(queryJob().queryKey());
            te->setSlave(queryMyRank());
            if (!te->queryOrigin())
            {
                VStringBuffer msg("SLAVE #%d", queryMyRank());
                te->setOrigin(msg);
            }
            else if (0 == stricmp("user", te->queryOrigin()))
                userOrigin = true;
        }
        serializeThorException(e, msg);
        if (userOrigin)
        {
            // wait for reply
            if (!queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to sendrecv to master");
        }
        else
        {
            if (!queryJobComm().send(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to send to master");
        }
        return true;
    }
};

interface IActivityReplicatedFile : extends IReplicatedFile
{
    virtual IFile *open(CActivityBase &activity) = 0;
};

interface IPartDescriptor;
extern graphslave_decl bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path);
extern graphslave_decl IActivityReplicatedFile *createEnsurePrimaryPartFile(const char *logicalFilename, IPartDescriptor *partDesc);
extern graphslave_decl IThorFileCache *createFileCache(unsigned limit);

extern graphslave_decl bool canStall(IThorDataLink *input);


#endif
