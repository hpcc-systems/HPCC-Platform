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

#ifdef _WIN32
    #ifdef GRAPHSLAVE_EXPORTS
        #define graphslave_decl __declspec(dllexport)
    #else
        #define graphslave_decl __declspec(dllimport)
    #endif
#else
    #define graphslave_decl
#endif

#include "platform.h"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorcommon.hpp"
#include "thgraph.hpp"
#include "jdebug.hpp"

class CSlaveActivity;

class CSlaveGraphElement;
class graphslave_decl CSlaveActivity : public CActivityBase
{
    mutable MemoryBuffer *data;
    mutable CriticalSection crit;

protected:
    IPointerArrayOf<IThorDataLink> inputs, outputs;
    ActivityTimeAccumulator totalCycles;
    MemoryBuffer startCtx;

public:
    IMPLEMENT_IINTERFACE;

    CSlaveActivity(CGraphElementBase *container);
    ~CSlaveActivity();
    virtual void clearConnections();
    virtual void releaseIOs();
    virtual void init(MemoryBuffer &in, MemoryBuffer &out) { }
    virtual void processDone(MemoryBuffer &mb) { };
    virtual void abort();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const;

    IThorDataLink *queryOutput(unsigned index);
    IThorDataLink *queryInput(unsigned index);
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx);
    void appendOutput(IThorDataLink *itdl) { outputs.append(itdl); };
    void appendOutputLinked(IThorDataLink *itdl) { itdl->Link(); appendOutput(itdl); };
    void startInput(IThorDataLink *itdl, const char *extra=NULL);
    void stopInput(IRowStream *itdl, const char *extra=NULL);

    ActivityTimeAccumulator &getTotalCyclesRef() { return totalCycles; }
    unsigned __int64 queryLocalCycles() const;
    virtual unsigned __int64 queryTotalCycles() const; // some acts. may calculate accumulated total from inputs (e.g. splitter)
    virtual void serializeStats(MemoryBuffer &mb);
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
    bool initialized, progressActive, progressToCollect;
    CriticalSection progressCrit;
    SpinLock progressActiveLock;

public:

    CSlaveGraph(CJobChannel &jobChannel);
    ~CSlaveGraph() { }

    void connect();
    void init(MemoryBuffer &mb);
    void recvStartCtx();
    bool recvActivityInitData(size32_t parentExtractSz, const byte *parentExtract);
    void setExecuteReplyTag(mptag_t _executeReplyTag) { executeReplyTag = _executeReplyTag; }
    void initWithActData(MemoryBuffer &in, MemoryBuffer &out);
    void getDone(MemoryBuffer &doneInfoMb);
    void serializeDone(MemoryBuffer &mb);
    IThorResult *getGlobalResult(CActivityBase &activity, IRowInterfaces *rowIf, activity_id ownerId, unsigned id);

    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    virtual bool serializeStats(MemoryBuffer &mb);
    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void start();
    virtual void create(size32_t parentExtractSz, const byte *parentExtract);
    virtual void abort(IException *e);
    virtual void done();
    virtual void end();
    virtual IThorGraphResults *createThorGraphResults(unsigned num);

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        StringBuffer s;
        if (!te || !te->queryGraphId())
        {
            Owned<IThorException> e2 = ThorWrapException(e, "CSlaveGraph trapped");         
            e2->setGraphId(queryGraphId());
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
    ISlaveWatchdog *watchdog;
    Owned<IPropertyTree> workUnitInfo;
    size32_t oldNodeCacheMem;

public:
    IMPLEMENT_IINTERFACE;

    CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *workUnitInfo, const char *graphName, const char *querySo, mptag_t _mptag, mptag_t _slavemptag);

    virtual void addChannel(IMPServer *mpServer);
    virtual void startJob();
    const char *queryFindString() const { return key.get(); } // for string HT

    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing);
    ISlaveWatchdog *queryProgressHandler() { return watchdog; }

    virtual mptag_t deserializeMPTag(MemoryBuffer &mb);
    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const;
    virtual bool getWorkUnitValueBool(const char *prop, bool defVal) const;
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

interface IPartDescriptor;
extern graphslave_decl bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path);
extern graphslave_decl IReplicatedFile *createEnsurePrimaryPartFile(CActivityBase &activity, const char *logicalFilename, IPartDescriptor *partDesc);
extern graphslave_decl IThorFileCache *createFileCache(unsigned limit);

#endif
