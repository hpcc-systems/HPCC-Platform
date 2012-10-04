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
    CriticalSection &queryCreateLock() { return createdCrit; }
    void handleSlaveDone(unsigned node, MemoryBuffer &mb);
    void serializeCreateContexts(MemoryBuffer &mb);
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
    IThorResult *createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    IThorResult *createResult(CActivityBase &activity, unsigned id, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    IThorResult *createGraphLoopResult(CActivityBase &activity, IRowInterfaces *rowIf, bool distributed, unsigned spillPriority=SPILL_PRIORITY_RESULT);

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
    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing);
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
    StringAttr prefix;
    StringAttr labelMin, labelMax, labelMinSkew, labelMaxSkew, labelMinEndpoint, labelMaxEndpoint;

public:
    IMPLEMENT_IINTERFACE;

    CThorStats(const char *prefix=NULL);
    void reset();
    virtual void processInfo();
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
    void getXGMML(IPropertyTree *node, bool suppressMinMaxWhenEqual);
};

class graphmaster_decl CTimingInfo : public CThorStats
{
public:
    CTimingInfo();
    void getXGMML(IPropertyTree *node) { CThorStats::getXGMML(node, false); }
};

class graphmaster_decl ProgressInfo : public CThorStats
{
    unsigned startcount, stopcount;
public:
    virtual void processInfo();
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
    void doCreateActivity(size32_t parentExtractSz=0, const byte *parentExtract=NULL);
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

