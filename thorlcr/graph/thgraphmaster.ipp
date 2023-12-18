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

#ifndef _THGRAPHMASTER_IPP
#define _THGRAPHMASTER_IPP

#include <unordered_map>

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


class graphmaster_decl CThorStatsCollection : public CInterface
{
    std::vector<OwnedPtr<CRuntimeStatisticCollection>> nodeStats;
    const StatisticsMapping & mapping;
public:
    CThorStatsCollection(const StatisticsMapping & _mapping) : mapping(_mapping), nodeStats(queryClusterWidth())
    {
        unsigned c = queryClusterWidth();
        while (c--)
            nodeStats[c].setown(new CRuntimeStatisticCollection(mapping));
    }
    CThorStatsCollection(const CThorStatsCollection & other) = delete;
    void deserialize(unsigned node, MemoryBuffer & mb)
    {
        nodeStats[node]->deserialize(mb);
    }
    void setStatistic(unsigned node, StatisticKind kind, unsigned __int64 value)
    {
        nodeStats[node]->setStatistic(kind, value);
    }
    void getStats(IStatisticGatherer & result)
    {
        CRuntimeSummaryStatisticCollection summary(mapping);
        for (unsigned n=0; n < nodeStats.size(); n++) // NB: size is = queryClusterWidth()
            summary.merge(*nodeStats[n], n);
        summary.recordStatistics(result);
    }
    stat_type getStatisticSum(StatisticKind kind)
    {
        stat_type total = 0;
        unsigned index = mapping.getIndex(kind);
        for (unsigned n=0; n < nodeStats.size(); n++) // NB: size is = queryClusterWidth()
            total += nodeStats[n]->getValue(index);
        return total;
    }
};

class graphmaster_decl CThorEdgeCollection : public CThorStatsCollection
{
    static const StatisticsMapping edgeStatsMapping;
public:
    CThorEdgeCollection() : CThorStatsCollection(edgeStatsMapping) { }
    void set(unsigned node, unsigned __int64 value);
};

class CJobMaster;
class CMasterGraphElement;
class graphmaster_decl CMasterGraph : public CGraphBase
{
    CJobMaster *jobM;
    CriticalSection createdCrit;
    Owned<IFatalHandler> fatalHandler;
    CriticalSection exceptCrit;
    bool sentGlobalInit = false;
    CThorStatsCollection graphStats;
    CReplyCancelHandler activityInitMsgHandler, bcastMsgHandler, executeReplyMsgHandler;

    void sendQuery();
    void jobDone();
    void sendGraph();
    void getFinalProgress(bool aborting=false);
    void configure();
    void sendActivityInitData();
    void recvSlaveInitResp();
    void serializeGraphInit(MemoryBuffer &mb);

public:
    CMasterGraph(CJobChannel &jobChannel);
    ~CMasterGraph();

    virtual void init();
    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    CriticalSection &queryCreateLock() { return createdCrit; }
    void handleSlaveDone(unsigned node, MemoryBuffer &mb);
    bool serializeActivityInitData(unsigned slave, MemoryBuffer &mb, IThorActivityIterator &iter);
    void readActivityInitData(MemoryBuffer &mb, unsigned slave);
    bool deserializeStats(unsigned node, MemoryBuffer &mb);
    void getStats(IStatisticGatherer &stats);
    virtual cost_type getDiskAccessCost() override;
    virtual void setComplete(bool tf=true);
    virtual bool prepare(size32_t parentExtractSz, const byte *parentExtract, bool checkDependencies, bool shortCircuit, bool async) override;
    virtual void execute(size32_t _parentExtractSz, const byte *parentExtract, bool checkDependencies, bool async) override;

    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract) override;
    virtual void start() override;
    virtual void done() override;
    virtual void reset() override;
    virtual void abort(IException *e) override;
    IThorResult *createResult(CActivityBase &activity, unsigned id, IThorGraphResults *results, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    IThorResult *createResult(CActivityBase &activity, unsigned id, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);
    IThorResult *createGraphLoopResult(CActivityBase &activity, IThorRowInterfaces *rowIf, ThorGraphResultType resultType, unsigned spillPriority=SPILL_PRIORITY_RESULT);

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
    unsigned childGraphInitTimeout;
public:
    CSlaveMessageHandler(CJobMaster &job, mptag_t mptag);
    ~CSlaveMessageHandler();
    void stop();
    virtual void threadmain() override;
};

class graphmaster_decl CJobMaster : public CJobBase
{
    typedef CJobBase PARENT;

    Linked<IConstWorkUnit> workunit;
    Owned<IFatalHandler> fatalHandler;
    bool querySent, sendSo, spillsSaved;
    StringArray createdFiles;
    Owned<CSlaveMessageHandler> slaveMsgHandler;
    SocketEndpoint agentEp;
    CriticalSection sendQueryCrit, spillCrit;

    void initNodeDUCache();

public:
    CJobMaster(IConstWorkUnit &workunit, const char *_graphName, ILoadedDllEntry *querySo, bool _sendSo, const SocketEndpoint &_agentEp);
    virtual void endJob() override;

    virtual CJobChannel *addChannel(IMPServer *mpServer) override;

    void registerFile(const char *logicalName, StringArray &clusters, unsigned usageCount=0, WUFileKind fileKind=WUFileStandard, bool temp=false);
    void deregisterFile(const char *logicalName, bool kept=false);
    const SocketEndpoint &queryAgentEp() const { return agentEp; }
    void broadcast(ICommunicator &comm, CMessageBuffer &msg, mptag_t mptag, unsigned timeout, const char *errorMsg, CReplyCancelHandler *msgHandler=NULL, bool sendOnly=false, bool aborting=false);
    IPropertyTree *prepareWorkUnitInfo();
    void sendQuery();
    void jobDone();
    void saveSpills();
    bool go();
    void pause(bool abort);

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
    virtual mptag_t allocateMPTag();
    virtual void freeMPTag(mptag_t tag);
    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing);

    CGraphTableCopy executed;
    CriticalSection exceptCrit;

    virtual bool hasWorkUnitValue(const char *prop) const override;
    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const override;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const override;
    virtual bool getWorkUnitValueBool(const char *prop, bool defVal) const override;
    virtual double getWorkUnitValueReal(const char *prop, double defVal) const override;


// IExceptionHandler
    virtual bool fireException(IException *e);

    virtual void addCreatedFile(const char *file);

    bool queryCreatedFile(const char *file);

    virtual IFatalHandler *clearFatalHandler();
};

class graphmaster_decl CJobMasterChannel : public CJobChannel
{
public:
    CJobMasterChannel(CJobBase &job, IMPServer *mpServer, unsigned channel);

    virtual CGraphBase *createGraph();
    virtual IBarrier *createBarrier(mptag_t tag);
// IExceptionHandler
    virtual bool fireException(IException *e) { return job.fireException(e); }
};



class graphmaster_decl CMasterActivity : public CActivityBase, implements IThreaded
{
    CThreaded threaded;
    bool asyncStart;
    MemoryBuffer *data;
    CriticalSection progressCrit;
    std::vector<Owned<IDistributedFile>> readFiles;
    std::unordered_map<std::string, unsigned> readFilesMap; // NB: IDistributedFile pointers are owned by readFiles
    std::vector<unsigned> fileStatsTable;
protected:
    std::vector<OwnedPtr<CThorEdgeCollection>> edgeStatsVector;
    CThorStatsCollection statsCollection;
    IBitSet *notedWarnings;
    cost_type diskAccessCost = 0;
    std::vector<OwnedPtr<CThorStatsCollection>> fileStats;

    IDistributedFile *queryReadFile(unsigned f);
    unsigned queryReadFileId(const char *lfnName);
    IDistributedFile *findReadFile(const char *lfnName);
    IDistributedFile *lookupReadFile(const char *lfnName, AccessMode mode, bool jobTemp, bool temp, bool opt, bool statsForMultipleFiles=false, const StatisticsMapping &statsMapping=diskReadRemoteStatistics, unsigned * fileStatsStartEntry=nullptr);
    void updateFileReadCostStats();
    void updateFileWriteCostStats(IFileDescriptor & fileDesc, IPropertyTree &props, stat_type numDiskWrites);
    virtual void process() { }
public:
    IMPLEMENT_IINTERFACE_USING(CActivityBase)

    CMasterActivity(CGraphElementBase *container, const StatisticsMapping &actStatsMapping = basicActivityStatistics);
    ~CMasterActivity();

    virtual void deserializeStats(unsigned node, MemoryBuffer &mb);
    virtual void getActivityStats(IStatisticGatherer & stats);
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx);
    virtual void init();
    virtual void handleSlaveMessage(CMessageBuffer &msg) { }
    virtual void reset();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &dst) const;

    virtual void configure() { }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) { }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb) { }

    virtual void startProcess(bool async=true);
    virtual bool wait(unsigned timeout);
    virtual void done();
    virtual void kill();
    virtual cost_type getDiskAccessCost() const { return diskAccessCost; }

// IExceptionHandler
    virtual bool fireException(IException *e);
// IThreaded
    virtual void threadmain() override;
};

class graphmaster_decl CMasterGraphElement : public CGraphElementBase
{
    bool initialized = false;
public:
    CMasterGraphElement(CGraphBase &owner, IPropertyTree &xgmml);
    void doCreateActivity(size32_t parentExtractSz=0, const byte *parentExtract=nullptr, MemoryBuffer *startCtx=nullptr);
    virtual bool checkUpdate();
    virtual void initActivity() override;
    virtual void reset() override;
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

