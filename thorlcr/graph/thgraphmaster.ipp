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

#ifndef _THGRAPHMASTER_IPP
#define _THGRAPHMASTER_IPP

#include "jmisc.hpp"
#include "jsuperhash.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"
#include "dllserver.hpp"

#include "thcodectx.hpp"
#include "thormisc.hpp"
#include "thgraph.hpp"
#include "thgraphmaster.hpp"
#include "thmfilemanager.hpp"
#include "thgraphmanager.hpp"


interface ILoadedDllEntry;
interface IConstWUGraphProgress;

class CJobMaster;
class CMasterGraphElement;
class graphmaster_decl CMasterGraph : public CGraphBase
{
    CJobMaster &jobM;
    CriticalSection createdCrit;
    Owned<IFatalHandler> fatalHandler;
    CriticalSection exceptCrit;
    mptag_t bcastTag;

    void sendQuery();
    void jobDone();
    void sendGraph();
    void getFinalProgress();
    void configure();
    void sendActivityInitData();
    void recvSlaveInitResp();
    void serializeGraphInit(MemoryBuffer &mb);

public:
    IMPLEMENT_IINTERFACE;

    CMasterGraph(CJobMaster &job);
    ~CMasterGraph();

    virtual void init();
    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    void handleSlaveDone(unsigned node, MemoryBuffer &mb);
    void serializeCreateContexts(MemoryBuffer &mb);
    void serializeStartCtxs(MemoryBuffer &mb);
    bool serializeActivityInitData(unsigned slave, MemoryBuffer &mb, IThorActivityIterator &iter);
    void readActivityInitData(MemoryBuffer &mb, unsigned slave);
    bool deserializeStats(unsigned node, MemoryBuffer &mb);
    virtual void setComplete(bool tf=true);
    virtual bool prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async);
    virtual void create(size32_t parentExtractSz, const byte *parentExtract);

    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void start();
    virtual void done();
    virtual void abort(IException *e);
// IExceptionHandler
    virtual bool fireException(IException *e);
// IThorChildGraph impl.
    virtual IEclGraphResults *evaluate(unsigned _parentExtractSz, const byte *parentExtract);
};

class CSlaveMessageHandler : public CInterface, implements IThreaded
{
    CJobMaster &job;
    CThreaded threaded;
    bool stopped;
    mptag_t mptag;
public:
    CSlaveMessageHandler(CJobMaster &job, mptag_t mptag);
    ~CSlaveMessageHandler();
    void stop();
    void main();
};

class graphmaster_decl CJobMaster : public CJobBase
{
    Linked<IConstWorkUnit> workunit;
    Owned<IFatalHandler> fatalHandler;
    bool querySent, sendSo;
    Int64Array nodeDiskUsage;
    bool nodeDiskUsageCached;
    StringArray createdFiles;
    CSlaveMessageHandler *slaveMsgHandler;
    SocketEndpoint agentEp;
    CriticalSection sendQueryCrit;

    void initNodeDUCache();

public:
    IMPLEMENT_IINTERFACE;

    CJobMaster(IConstWorkUnit &workunit, const char *_graphName, const char *querySo, bool _sendSo, const SocketEndpoint &_agentEp);
    ~CJobMaster();

    void registerFile(const char *logicalName, StringArray &clusters, unsigned usageCount=0, WUFileKind fileKind=WUFileStandard, bool temp=false);
    void deregisterFile(const char *logicalName, bool kept=false);
    const SocketEndpoint &queryAgentEp() const { return agentEp; }
    void broadcastToSlaves(CMessageBuffer &msg, mptag_t mptag, unsigned timeout, const char *errorMsg, mptag_t *replyTag=NULL, bool sendOnly=false);
    IPropertyTree *prepareWorkUnitInfo();
    void sendQuery();
    void jobDone();
    bool go();
    void stop(bool abort);

    virtual IConstWorkUnit &queryWorkUnit() const
    {
        CriticalBlock b(wuDirty);
        if (dirty)
        {
            dirty = false;
            workunit->forceReload();
        }
        return *workunit;
    }
    virtual void markWuDirty()
    {
        CriticalBlock b(wuDirty);
        dirty = true;
    }
    
// CJobBase impls.
    virtual IGraphTempHandler *createTempHandler();
    virtual CGraphBase *createGraph();

    CMasterGraphElement *locateActivity(activity_id id)
    {
        Owned<IThorGraphIterator> iter = getSubGraphs();
        ForEach (*iter)
        {
            CMasterGraphElement *activity = (CMasterGraphElement *)iter->query().queryElement(id);
            if (activity) return activity;
        }
        return NULL;
    }
    IConstWUGraphProgress *getGraphProgress() { return workunit->getGraphProgress(queryGraphName()); }

    CGraphTableCopy executed;
    CriticalSection exceptCrit;

    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const;
    
    virtual IBarrier *createBarrier(mptag_t tag);

// IExceptionHandler
    virtual bool fireException(IException *e);

    virtual void addCreatedFile(const char *file);
    virtual __int64 addNodeDiskUsage(unsigned node, __int64 sz);

    __int64 queryNodeDiskUsage(unsigned node);
    void setNodeDiskUsage(unsigned node, __int64 sz);
    bool queryCreatedFile(const char *file);
};

class graphmaster_decl CThorStats : public CInterface, implements IInterface
{
protected:
    unsigned __int64 max, min, tot, avg;
    unsigned hi, lo, minNode, maxNode;
    UInt64Array counts;

public:
    IMPLEMENT_IINTERFACE;

    CThorStats();
    void reset();
    void processInfo();
    static void removeAttribute(IPropertyTree *node, const char *name);
    static void addAttribute(IPropertyTree *node, const char *name, unsigned __int64 val);
    static void addAttribute(IPropertyTree *node, const char *name, const char *val);

    unsigned __int64 queryTotal() { return tot; }
    unsigned __int64 queryAverage() { return avg; }
    unsigned __int64 queryMin() { return min; }
    unsigned __int64 queryMax() { return max; }
    unsigned queryMinSkew() { return lo; }
    unsigned queryMaxSkew() { return hi; }
    unsigned queryMaxNode() { return maxNode; }
    unsigned queryMinNode() { return minNode; }

    void set(unsigned node, unsigned __int64 count);
};

class graphmaster_decl CTimingInfo : public CThorStats
{
public:
    void getXGMML(IPropertyTree *node);
};
class graphmaster_decl ProgressInfo : public CThorStats
{
    unsigned startcount, stopcount;
public:
    void processInfo();
    void getXGMML(IPropertyTree *node);
};
typedef IArrayOf<ProgressInfo> ProgressInfoArray;

class graphmaster_decl CMasterActivity : public CActivityBase, implements IThreaded
{
    CThreaded threaded;
    bool asyncStart;
    MemoryBuffer *data;
    CriticalSection progressCrit;
protected:
    ProgressInfoArray progressInfo;
    CTimingInfo timingInfo;
    IBitSet *notedWarnings;

    virtual void process() { }
public:
    IMPLEMENT_IINTERFACE;

    CMasterActivity(CGraphElementBase *container);
    ~CMasterActivity();

    virtual void deserializeStats(unsigned node, MemoryBuffer &mb);
    virtual void getXGMML(unsigned idx, IPropertyTree *edge);
    virtual void getXGMML(IWUGraphProgress *progress, IPropertyTree *node);
    virtual void init() { }
    virtual void handleSlaveMessage(CMessageBuffer &msg) { }
    virtual void reset();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &dst) const;

    virtual void configure() { }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) { }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb) { }

    virtual void startProcess(bool async=true);
    virtual bool wait(unsigned timeout);

// IExceptionHandler
    virtual bool fireException(IException *e);
// IThreaded
    virtual void main();
};

class graphmaster_decl CMasterGraphElement : public CGraphElementBase
{
public:
    IMPLEMENT_IINTERFACE;
    
    bool sentCreateCtx;

    CMasterGraphElement(CGraphBase &owner, IPropertyTree &xgmml);
    void doCreateActivity();
    virtual bool checkUpdate();

    virtual void initActivity();
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb);
};

////////////////////////

class graphmaster_decl CThorResourceMaster : public CThorResourceBase
{
public:
    IMPLEMENT_IINTERFACE;

    CThorResourceMaster()
    {
    }
    ~CThorResourceMaster()
    {
    }
// IThorResource
};

#endif

