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
#include "thcrc.hpp"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thgraph.hpp"
#include "jdebug.hpp"

class CSlaveActivity;

class CSlaveGraphElement;
class graphslave_decl CSlaveActivity : public CActivityBase
{
    mutable MemoryBuffer *data;
    mutable CriticalSection crit;

protected:
    PointerIArrayOf<IThorDataLink> inputs, outputs;
    unsigned __int64 totalCycles;
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
    virtual void kill() { CActivityBase::kill(); }
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const;

    IThorDataLink *queryOutput(unsigned index);
    IThorDataLink *queryInput(unsigned index);
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx);
    void appendOutput(IThorDataLink *itdl) { outputs.append(itdl); };
    void appendOutputLinked(IThorDataLink *itdl) { itdl->Link(); appendOutput(itdl); };
    void startInput(IThorDataLink *itdl, const char *extra=NULL);
    void stopInput(IThorDataLink *itdl, const char *extra=NULL);

    unsigned __int64 &getTotalCyclesRef() { return totalCycles; }
    unsigned __int64 queryLocalCycles() const;
    unsigned __int64 queryTotalCycles() const;
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
    CJobSlave &jobS;
    Owned<IInterface> progressHandler;
    Semaphore getDoneSem;
    bool needsFinalInfo;
    CriticalSection progressCrit;

public:

    CSlaveGraph(CJobSlave &_job);
    ~CSlaveGraph() { }

    void connect();
    void init(MemoryBuffer &mb);
    void recvStartCtx();
    bool recvActivityInitData();
    void setExecuteReplyTag(mptag_t _executeReplyTag) { executeReplyTag = _executeReplyTag; }
    void initWithActData(MemoryBuffer &in, MemoryBuffer &out);
    void getDone(MemoryBuffer &doneInfoMb);
    void serializeDone(MemoryBuffer &mb);
    IThorResult *getGlobalResult(CActivityBase &activity, IRowInterfaces *rowIf, unsigned id);

    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    virtual void serializeStats(MemoryBuffer &mb);
    virtual bool prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async);
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
    Owned<IPerfMonHook> perfmonhook;
    Owned<IPropertyTree> workUnitInfo;
    CriticalSection graphRunCrit;

    void startJob();
    void endJob();

public:
    IMPLEMENT_IINTERFACE;

    CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *workUnitInfo, const char *graphName, const char *querySo, mptag_t _mptag, mptag_t _slavemptag);
    ~CJobSlave();

    const char *queryFindString() const { return key.get(); } // for string HT

    ISlaveWatchdog *queryProgressHandler() { return watchdog; }

    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const;
    virtual IGraphTempHandler *createTempHandler();
    virtual CGraphBase *createGraph()
    {
        return new CSlaveGraph(*this);
    }
    virtual IBarrier *createBarrier(mptag_t tag);

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        CMessageBuffer msg;
        msg.append((int)smt_errorMsg);
        IThorException *te = QUERYINTERFACE(e, IThorException);
        if (te)
            te->setJobId(key);
        serializeThorException(e, msg);
        if (te && te->queryOrigin() && 0 == stricmp("user", te->queryOrigin()))
        {
            // wait for reply
            if (!queryJobComm().sendRecv(msg, 0, querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to sendrecv to master");
        }
        else
        {
            if (!queryJobComm().send(msg, 0, querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to send to master");
        }
        return true;
    }
// IGraphCallback
    virtual void runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract);
// IThreadFactory
    IPooledThread *createNew();
};

interface IPartDescriptor;
extern graphslave_decl bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path);
extern graphslave_decl IReplicatedFile *createEnsurePrimaryPartFile(CActivityBase &activity, const char *logicalFilename, IPartDescriptor *partDesc);
extern graphslave_decl IThorFileCache *createFileCache(unsigned limit);

#endif
