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
#ifndef ECLAGENT_IPP_INCL
#define ECLAGENT_IPP_INCL

#include "dasds.hpp"
#include "dadfs.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "jthread.hpp"
#include "dllserver.hpp"
#include "rtldynfield.hpp"

#include "hthor.hpp"
#include "thorxmlwrite.hpp"
#include "workflow.hpp"
#include "roxierow.hpp"
#include "roxiedebug.hpp"
#include <stdexcept>
#include "thorplugin.hpp"
#include "thorcommon.hpp"
#include "enginecontext.hpp"

#define MAX_EDGEDATA_LENGTH 30000
#define MAX_HEX_SIZE 500

class EclGraph;
typedef unsigned __int64 graphid_t;
typedef unsigned __int64 activityid_t;

extern Owned<IPropertyTree> agentTopology;

//=================================================================================

//The following don't link their arguments because that creates a circular reference
//But I wish there was a better way
class IndirectAgentContext : implements IAgentContext, public CInterface
{
public:
    IndirectAgentContext(IAgentContext * _ctx = NULL) : ctx(_ctx) {}
    IMPLEMENT_IINTERFACE

    void set(IAgentContext * _ctx) { ctx = _ctx; }

    virtual ICodeContext * queryCodeContext()
    {
        return ctx->queryCodeContext();
    }
    virtual void fail(int code, const char * msg)
    {
        ctx->fail(code, msg);
    }
    virtual bool isResult(const char * name, unsigned sequence)
    {
        return ctx->isResult(name, sequence);
    }
    virtual unsigned getWorkflowIdDeprecated() override
    {
        throwUnexpected();
    }
    virtual void doNotify(char const * name, char const * text)
    {
        ctx->doNotify(name, text);
    }
    virtual void doNotify(char const * name, char const * text, const char * target)
    {
        ctx->doNotify(name, text, target);
    }
    virtual int queryLastFailCode()
    {
        return ctx->queryLastFailCode();
    }
    virtual void getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag)
    {
        ctx->getLastFailMessage(outLen, outStr, tag);
    }
    virtual void getEventName(size32_t & outLen, char * & outStr)
    {
        ctx->getEventName(outLen, outStr);
    }
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag)
    {
        ctx->getEventExtra(outLen, outStr, tag);
    }
    virtual bool fileExists(const char * filename)
    {
        return ctx->fileExists(filename);
    }
    virtual void deleteFile(const char * logicalName)
    {
        ctx->deleteFile(logicalName);
    }
    virtual void selectCluster(const char * cluster)
    {
        ctx->selectCluster(cluster);
    }
    virtual void restoreCluster()
    {
        ctx->restoreCluster();
    }
    virtual bool queryResolveFilesLocally()
    {
        return ctx->queryResolveFilesLocally();
    }
    virtual bool queryRemoteWorkunit()
    {
        return ctx->queryRemoteWorkunit();
    }
    virtual bool queryWriteResultsToStdout()
    {
        return ctx->queryWriteResultsToStdout();
    }
    virtual outputFmts queryOutputFmt()
    {
        return ctx->queryOutputFmt();
    }
    virtual VOID outputFormattedResult(const char *name, unsigned sequence, bool close)
    {
        return ctx->outputFormattedResult(name, sequence, close);
    }
    virtual unsigned __int64 queryStopAfter()
    {
        return ctx->queryStopAfter();
    }
    virtual IOrderedOutputSerializer * queryOutputSerializer()
    {
        return ctx->queryOutputSerializer();
    }
    virtual void setWorkflowCondition(bool value)
    {
        ctx->setWorkflowCondition(value);
    }
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        ctx->returnPersistVersion(logicalName, eclCRC, allCRC, isFile);
    }
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend)
    {
        ctx->setResultDataset(name, sequence, len, val, numRows, extend);
    }
    virtual void reportProgress(const char *msg, unsigned flags)
    {
        ctx->reportProgress(msg, flags);
    }
    virtual IConstWorkUnit *queryWorkUnit() const override
    {
        return ctx->queryWorkUnit();
    }
    virtual IWorkUnit *updateWorkUnit() const
    {
        return ctx->updateWorkUnit();
    }
    virtual ILocalOrDistributedFile *resolveLFN(const char *logicalName, const char *errorTxt, bool optional, bool noteRead, AccessMode accessMode, StringBuffer * expandedlfn, bool isPrivilegedUser)
    {
        return ctx->resolveLFN(logicalName, errorTxt, optional, noteRead, accessMode, expandedlfn, isPrivilegedUser);
    }
    virtual StringBuffer & getTempfileBase(StringBuffer & buff)
    {
        return ctx->getTempfileBase(buff);
    }
    virtual const char *noteTemporaryFile(const char *fname)
    {
        return ctx->noteTemporaryFile(fname);
    }
    virtual const char *noteTemporaryFilespec(const char *fspec)
    {
        return ctx->noteTemporaryFilespec(fspec);
    }
    virtual const char *queryTemporaryFile(const char *fname)
    {
        return ctx->queryTemporaryFile(fname);
    }
    virtual void reloadWorkUnit()
    {
        ctx->reloadWorkUnit();
    }
    virtual char *resolveName(const char *in, char *out, unsigned outlen)
    {
        return ctx->resolveName(in, out, outlen);
    }
    virtual void logFileAccess(IDistributedFile * file, char const * component, char const * type, EclGraph & graph)
    {
        ctx->logFileAccess(file, component, type, graph);
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, char const * source)
    {
        ctx->addWuException(text, code, severity, source);
    }
    virtual IHThorGraphResults * executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, const char * embeddedGraphName, const byte * parentExtract)
    {
        return ctx->executeLibraryGraph(libraryName, expectedInterfaceHash, activityId, embeddedGraphName, parentExtract);
    }
    virtual bool getWorkunitResultFilename(StringBuffer & diskFilename, const char * wuid, const char * name, int seq)
    {
        return ctx->getWorkunitResultFilename(diskFilename, wuid, name, seq);
    }
    virtual IHThorGraphResults * createGraphLoopResults()
    {
        return ctx->createGraphLoopResults();
    }

    virtual const char *queryAllowedPipePrograms()
    {
        return ctx->queryAllowedPipePrograms();
    }

    virtual IGroup *getHThorGroup(StringBuffer &name)
    {
        return ctx->getHThorGroup(name);
    }

    virtual const char *queryWuid()
    {
        return ctx->queryWuid();
    }

    virtual void updateWULogfile(IWorkUnit *outputWU)
    {
        return ctx->updateWULogfile(outputWU);
    }

    virtual RecordTranslationMode getLayoutTranslationMode() const override
    {
        return ctx->getLayoutTranslationMode();
    }

    virtual bool forceNewDiskReadActivity() const
    {
        return ctx->forceNewDiskReadActivity();
    }
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, char const * source) override
    {
        ctx->addWuExceptionEx(text, code, severity, audience, source);
    }
    virtual double queryAgentMachineCost() const override
    {
        return ctx->queryAgentMachineCost();
    }
    virtual void updateAggregates(IWorkUnit* lockedwu) override
    {
        ctx->updateAggregates(lockedwu);
    }
    virtual void mergeAggregatorStats(IStatisticCollection & stats, unsigned wfid, const char *graphname, unsigned sgId) override
    {
        ctx->mergeAggregatorStats(stats, wfid, graphname, sgId);
    }

protected:
    IAgentContext * ctx;
};

//---------------------------------------------------------------------------

class EclAgent;

class EclAgentWorkflowMachine : public WorkflowMachine
{
public:
    EclAgentWorkflowMachine(EclAgent & _agent);
    void returnPersistVersion(char const * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        persist.setown(new PersistVersion(logicalName, eclCRC, allCRC, isFile));
    }

protected:
    virtual void begin() override;
    virtual IRemoteConnection *startPersist(const char * logicalName) override;
    virtual void finishPersist(const char * persistName, IRemoteConnection *persistLock) override;
    virtual void deleteLRUPersists(const char * logicalName, unsigned keep) override;
    virtual void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC) override;
    virtual bool checkFreezePersists(const char *logicalName, unsigned eclCRC) override;
    virtual bool isPersistUptoDate(Owned<IRemoteConnection> &persistLock, IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile) override;
    virtual void checkPersistSupported() override;
    virtual bool isPersistAlreadyLocked(const char * logicalName) override;
    virtual bool getParallelFlag() const override;
    virtual unsigned getThreadNumFlag() const override;

    virtual void end() override;
    virtual void schedulingStart() override;
    virtual bool schedulingPull() override;
    virtual bool schedulingPullStop() override;
    virtual void reportContingencyFailure(char const * type, IException * e) override;
    virtual void checkForAbort(unsigned wfid, IException * handling) override;
    virtual void doExecutePersistItem(IRuntimeWorkflowItem & item) override;
    virtual void doExecuteCriticalItem(IRuntimeWorkflowItem & item) override;
    virtual bool getPersistTime(time_t & when, IRuntimeWorkflowItem & item) override;
    virtual void noteTiming(unsigned wfid, timestamp_type startTime, stat_type elapsedNs) override;

private:
    void prelockPersists();

private:
    EclAgent & agent;
    Owned<IRemoteConnection> runlock;
    void obtainRunlock();
    void releaseRunlock();
    void syncWorkflow();
    IRemoteConnection * obtainCriticalLock(const char *name);
    void releaseCriticalLock(IRemoteConnection *r);
    Owned<IWorkflowScheduleConnection> wfconn;
    bool persistsPrelocked;
    MapStringToMyClass<IRemoteConnection> persistCache;
};

class EclAgentQueryLibrary : public CInterface
{
public:
    void destroyGraph();
    void updateProgress();

public:
    StringAttr name;
    Linked<IConstWorkUnit> wu;
    Owned<ILoadedDllEntry> dll;
    Owned<EclGraph> graph;
};

//=======================================================================================
class CHThorDebugSocketListener;
class CHThorDebugContext : extends CBaseServerDebugContext
{
    Owned<CHThorDebugSocketListener> listener;
    EclAgent *eclAgent;

public:
    CHThorDebugContext(const IContextLogger &_logctx, IPropertyTree *_queryXGMML, EclAgent *_eclAgent);
    inline unsigned queryPort();
    inline EclAgent * getEclAgent() { return eclAgent; };

    virtual void debugInitialize(const char *id, const char *_queryName, bool _breakAtStart);
    virtual void debugPrintVariable(IXmlWriter *output, const char *name, const char *type) const;

    virtual void debugInterrupt(IXmlWriter *output);
    virtual IRoxieQueryPacket *onDebugCallback(const RoxiePacketHeader &header, size32_t len, char *data)   {throwUnexpected();};
    virtual void waitForDebugger(DebugState state, IActivityDebugContext *probe);
    virtual bool onDebuggerTimeout();
};


class CHThorDebugContext;
class EclAgent : implements IAgentContext, implements ICodeContext, implements IRowAllocatorMetaActIdCacheCallback, implements IEngineContext, public CInterface
{
private:
    friend class EclAgentWorkflowMachine;

    Owned<EclAgentWorkflowMachine> workflow;
    Owned<IConstWorkUnit> wuRead;
    Owned<roxiemem::IRowManager> rowManager;
    StringAttr wuid;
    StringAttr clusterType;
    StringBuffer logname;
    StringAttr userid;
    bool checkVersion;
    bool resetWorkflow;
    bool noRetry;
    volatile bool isAborting;
    bool useProductionLibraries;
    bool isStandAloneExe;
    bool isRemoteWorkunit;
    bool resolveFilesLocally;
    bool writeResultsToStdout;
    bool useNewDiskReadActivity;
    Owned<IUserDescriptor> standAloneUDesc;
    outputFmts outputFmt = ofSTD;
    unsigned __int64 stopAfter;
    mutable CriticalSection wusect;
    StringArray tempFiles;
    CriticalSection tfsect;
    IArray persistReadLocks;
    StringArray processedPersists;

    Owned<ILoadedDllEntry> dll;
    CIArrayOf<EclAgentQueryLibrary> queryLibraries;
    StringArray clusterNames;
    unsigned int clusterWidth;
    Owned<IDistributedFileTransaction> superfiletransaction;
    mutable Owned<IRowAllocatorMetaActIdCache> allocatorMetaCache;
    CriticalSection activeGraphCritSec;
    PointerArrayOf<EclGraph> activeGraphs;
    Owned<CHThorDebugContext> debugContext;
    Owned<IProbeManager> probeManager;
    StringAttr allowedPipeProgs;
    SafePluginMap *pluginMap;
    ILogMsgHandler *logMsgHandler;
    StringAttr agentTempDir;
    Owned<IOrderedOutputSerializer> outputSerializer;
    int retcode;
    double agentMachineCost = 0;
    StatisticsAggregator statsAggregator;

private:
    void doSetResultString(type_t type, const char * stepname, unsigned sequence, int len, const char *val);
    IEclProcess *loadProcess();
    StringBuffer & getTempfileBase(StringBuffer & buff);
    const char *queryTempfilePath();
    void deleteTempFiles();
    void restoreCluster(IWorkUnit *wu);

    bool checkPersistUptoDate(IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile, StringBuffer & errText);
    bool isPersistUptoDate(Owned<IRemoteConnection> &persistLock, IRuntimeWorkflowItem & item, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile);
    bool changePersistLockMode(IRemoteConnection *persistLock, unsigned mode, const char * name, bool repeat);
    bool expandLogicalName(StringBuffer & fullname, const char * logicalName);
    IRemoteConnection *getPersistReadLock(const char * logicalName);
    void doSimpleResult(type_t type, int size, char * buffer, int sequence);
    IConstWUResult *getResult(const char *name, unsigned sequence);
    IConstWUResult *getResultForGet(const char *name, unsigned sequence);
    IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence);
    bool arePersistsFrozen() { return (queryWorkUnit()->getDebugValueInt("freezepersists", 0) != 0); }
    void outputFormattedResult(const char *name, unsigned sequence, bool close = true);
    virtual outputFmts queryOutputFmt() { return outputFmt; }

    void loadDependencies(IConstWorkUnit * wu);
    EclAgentQueryLibrary * loadQueryLibrary(const char * name, IConstWorkUnit * wu);
    ILoadedDllEntry * loadWorkUnitDll(IConstWorkUnit * wu);
    IConstWorkUnit * resolveLibrary(const char * libraryName, unsigned expectedInterfaceHash);
    EclAgentQueryLibrary * queryEclLibrary(const char * libraryName, unsigned expectedInterfaceHash);
    EclAgentQueryLibrary * loadEclLibrary(const char * libraryName, unsigned expectedInterfaceHash, const char * embeddedGraphName);
    virtual bool getWorkunitResultFilename(StringBuffer & diskFilename, const char * wuid, const char * name, int seq);
    virtual IDebuggableContext *queryDebugContext() const { return debugContext; };
    void runWorkunitAnalyser(IWorkUnit * w, const char * optGraph);
    void runWorkunitAnalyserAfterGraph(const char * optGraph);

    //protected by critical section
    EclGraph * addGraph(const char * graphName);
    void removeGraph(EclGraph * g);
    EclGraph * loadGraph(const char * graphName, IConstWorkUnit * wu, ILoadedDllEntry * dll, bool isLibrary);
    virtual bool forceNewDiskReadActivity() const { return useNewDiskReadActivity; }

    class cAbortMonitor: public Thread, implements IExceptionHandler
    {
        EclAgent &parent;
    public:
        Semaphore sem;
        bool stopping;
        unsigned guillotinetimeout = 0;
        cost_type guillotineCost = 0;
        cAbortMonitor(EclAgent &_parent) : Thread("EclAgent Abort Monitor"), parent(_parent) { guillotinetimeout=0; guillotineCost=0; stopping=false; }
        int  run()  { parent.abortMonitor(); return 0; }
        void stop() { stopping = true; sem.signal(); join(1000*10); }
        void setGuillotineTimeout(unsigned secs) { guillotinetimeout = secs; sem.signal(); }
        void setGuillotineCost(cost_type cost) { guillotineCost = cost; }
        bool fireException(IException *e)
        {
            StringBuffer text;
            e->errorMessage(text);
            parent.fatalAbort(false,text.str());
            return false; // It returns to excsighandler() to abort!
        }
    } *abortmonitor;

public:
    IMPLEMENT_IINTERFACE;

    EclAgent(IConstWorkUnit *wu, const char *_wuid, bool _checkVersion, bool _resetWorkflow, bool _noRetry, char const * _logname, const char *_allowedPipeProgs, IPropertyTree *_queryXML, ILogMsgHandler * _logMsgHandler);
    ~EclAgent();

    void setBlocked();
    void setRunning();
    void setDebugPaused();
    void setDebugRunning();
    void setBlockedOnPersist(const char * logicalName);
    void setStandAloneOptions(bool _isStandAloneExe, bool _isRemoteWorkunit, bool _resolveFilesLocally, bool _writeResultsToStdout, outputFmts _outputFmt, IUserDescriptor *_standAloneUDesc);
    inline bool needToLockWorkunit() { return !isStandAloneExe; }           //If standalone exe then either no dali, or a unique remote workunit.

    virtual void setResultInt(const char * stepname, unsigned sequence, __int64, unsigned size);
    virtual void setResultReal(const char * stepname, unsigned sequence, double);
    virtual void setResultBool(const char * stepname, unsigned sequence, bool);
    virtual void setResultString(const char * stepname, unsigned sequence, int len, const char *);
    virtual void setResultData(const char * stepname, unsigned sequence, int len, const void *);
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend);
    virtual void setResultRaw(const char * stepname, unsigned sequence, int len, const void *);
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer);
    virtual void setResultUInt(const char * stepname, unsigned sequence, unsigned __int64, unsigned size);
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str);
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val);
    virtual void setResultVarString(const char * stepname, unsigned sequence, const char *);
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *);
    virtual __int64 getResultInt(const char * stepname, unsigned sequence);
    virtual bool getResultBool(const char * stepname, unsigned sequence);
    virtual char *getResultVarString(const char * stepname, unsigned sequence);
    virtual UChar *getResultVarUnicode(const char * stepname, unsigned sequence);
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence);
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence);
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence);
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence);
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer);
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer);
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence);
    virtual double getResultReal(const char * name, unsigned sequence);
    virtual unsigned getResultHash(const char * name, unsigned sequence);
    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer);
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence);
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) override;
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) override;
    virtual char *getJobName();
    virtual char *getJobOwner();
    virtual char *getClusterName();
    virtual char *getGroupName();
    virtual char *queryIndexMetaData(char const * lfn, char const * xpath);
    virtual void  abort();
    virtual int getRetcode();
    virtual void setRetcode(int code);

    virtual bool fileExists(const char * filename);
    virtual char * getExpandLogicalName(const char * logicalName);
    virtual void addWuException(const char * text, unsigned code, unsigned severity, char const * source);
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, char const * source);
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort);
    virtual IUserDescriptor *queryUserDescriptor();
    virtual void selectCluster(const char * cluster);
    virtual void restoreCluster();
    virtual unsigned getElapsedMs() const override {UNIMPLEMENTED;} //Implementing for roxie first, not sure what this means from an eclagent perspective.  ECL plugin implies elapsed time of entire query

    IRemoteConnection *startPersist(const char * name);
    bool alreadyLockedPersist(const char * persistName);
    void finishPersist(const char * persistName, IRemoteConnection *persistLock);
    void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC);
    void checkPersistMatches(const char * logicalName, unsigned eclCRC);
    virtual void deleteLRUPersists(const char * logicalName, unsigned keep);
    virtual bool queryResolveFilesLocally() { return resolveFilesLocally; }
    virtual bool queryRemoteWorkunit() { return isRemoteWorkunit; }
    virtual bool queryWriteResultsToStdout() { return writeResultsToStdout; }
    virtual IOrderedOutputSerializer * queryOutputSerializer() { return outputSerializer; }
    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
    virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
    virtual IEngineContext *queryEngineContext() { return this; }
    virtual char *getDaliServers();

    virtual RecordTranslationMode getLayoutTranslationMode() const override;
    unsigned __int64 queryStopAfter() { return stopAfter; }

    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name)
    {
        return queryNullSectionTimer();
    }

    virtual ISectionTimer * registerStatsTimer(unsigned activityId, const char * name, unsigned int statsOption)
    {
        return queryNullSectionTimer();
    }
// IEngineContext
    virtual DALI_UID getGlobalUniqueIds(unsigned num, SocketEndpoint *_foreignNode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignNode;
        if (_foreignNode && !_foreignNode->isNull())
            foreignNode.set(*_foreignNode);
        else
        {
            const char *dali = getDaliServers();
            if (!dali)
                return 0;
            foreignNode.set(dali);
            free((char *) dali);
        }
        return ::getGlobalUniqueIds(num, &foreignNode);
    }
    virtual bool allowDaliAccess() const  { return true; }
    virtual bool allowSashaAccess() const { return true; }
    virtual StringBuffer &getQueryId(StringBuffer &result, bool isShared) const
    {
        result.append("workunit"); // No distinction between global, workunit and query scopes for eclagent
        return result;
    }
    virtual void getManifestFiles(const char *type, StringArray &files) const override
    {
        const StringArray &dllFiles = dll->queryManifestFiles(type, wuid);
        ForEachItemIn(idx, dllFiles)
            files.append(dllFiles.item(idx));
    }

    virtual void onTermination(QueryTermCallback callback, const char *key, bool isShared) const
    {
        // No need to unregister, since scope lasts until exe terminates
    }


//New workflow interface
    virtual void setWorkflowCondition(bool value) { if(workflow) workflow->setCondition(value); }
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        if(workflow) workflow->returnPersistVersion(logicalName, eclCRC, allCRC, isFile);
    }

    virtual void fail(int code, char const * str);
    __declspec(noreturn) void failv(int code, char const * fmt, ...) __attribute__((format(printf, 3, 4), noreturn));
    virtual int queryLastFailCode();
    virtual void getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag);
    virtual void getEventName(size32_t & outLen, char * & outStr);
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag);
    //virtual void logException(IEclException *e);
    virtual char *resolveName(const char *in, char *out, unsigned outlen);
    virtual void logFileAccess(IDistributedFile * file, char const * component, char const * type, EclGraph & graph);
    virtual ILocalOrDistributedFile  *resolveLFN(const char *logicalName, const char *errorTxt, bool optional, bool noteRead, AccessMode accessMode, StringBuffer * expandedlfn, bool isPrivilegedUser);

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract);
    virtual IHThorGraphResults * executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, const char * embeddedGraphName, const byte * parentExtract);
    virtual IThorChildGraph * resolveChildQuery(__int64 subgraphId, IHThorArg * colocal);
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId);

    virtual IHThorGraphResults * createGraphLoopResults();

    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash);
    virtual void reportProgress(const char *msg, unsigned flags=0);
    virtual const char *noteTemporaryFile(const char *fname);
    virtual const char *noteTemporaryFilespec(const char *fspec);
    virtual const char *queryTemporaryFile(const char *fname);
    virtual void deleteFile(const char * logicalName);

    void addException(ErrorSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool failOnError, bool isAbort);
    void addExceptionEx(ErrorSeverity severity, MessageAudience aud, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool failOnError, bool isAbort);
    void logException(IException *e);
    void logException(WorkflowException *e);
    void logException(std::exception & e);
    void logException(ErrorSeverity severity, MessageAudience aud, unsigned code, const char * text, bool isAbort);

    void doProcess();
    void runProcess(IEclProcess *process);
    virtual void doNotify(char const * name, char const * text);
    virtual void doNotify(char const * name, char const * text, const char * target);
    void abortMonitor();
    void fatalAbort(bool userabort,const char *excepttext);

    virtual const char *loadResource(unsigned id);
    virtual ICodeContext *queryCodeContext();
    virtual bool isResult(const char * name, unsigned sequence);
    virtual unsigned getWorkflowIdDeprecated() override; // IGlobalCodeContext virtual - unused. deprecated. Left here to avoid changing interface.
    virtual IConstWorkUnit *queryWorkUnit() const override;  // no link
    virtual IWorkUnit *updateWorkUnit() const; // links
    virtual void reloadWorkUnit();
    void addTimings(IWorkUnit *w);

// ICodeContext
    virtual unsigned getGraphLoopCounter() const override { return 0; }
    virtual unsigned getNodes();
    virtual unsigned getNodeNum() { return 0; }
    virtual char *getFilePart(const char *logicalPart, bool create=false);
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { UNIMPLEMENTED; return 0; }
    virtual char *getOutputDir() { UNIMPLEMENTED; }
    virtual char *getWuid();
    virtual unsigned getWorkflowId() const override { throwUnexpected(); } // ICodeContext virtual
    virtual const char *queryWuid();
    virtual IDistributedFileTransaction *querySuperFileTransaction();
    virtual unsigned getPriority() const { return 0; }
    virtual char *getPlatform();
    virtual char *getEnv(const char *name, const char *defaultValue) const;
    virtual char *getOS()
    {
#ifdef _WIN32
        return strdup("windows");
#else
        return strdup("linux");
#endif
    }
    virtual unsigned logString(const char *text) const
    {
        if (text && *text)
        {
            DBGLOG("USER: %s", text);
            return strlen(text);
        }
        else
            return 0;
    }
    virtual const IContextLogger &queryContextLogger() const
    {
        return queryDummyContextLogger();
    }
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
    {
        return allocatorMetaCache->ensure(meta, activityId, roxiemem::RHFnone);
    }
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const
    {
        return allocatorMetaCache->ensure(meta, activityId, (roxiemem::RoxieHeapFlags)heapFlags);
    }
    virtual const char *cloneVString(const char *str) const
    {
        return rowManager->cloneVString(str);
    }
    virtual const char *cloneVString(size32_t len, const char *str) const
    {
        return rowManager->cloneVString(len, str);
    }
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToXML(lenResult, result, info, row, flags);
    }
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
    {
        convertRowToJSON(lenResult, result, info, row, flags);
    }
    virtual const char *queryAllowedPipePrograms()
    {
        return allowedPipeProgs.get();
    }

    IGroup *getHThorGroup(StringBuffer &out);

    virtual void updateWULogfile(IWorkUnit *w);

// roxiemem::IRowAllocatorMetaActIdCacheCallback
    virtual IEngineRowAllocator *createAllocator(IRowAllocatorMetaActIdCache * cache, IOutputMetaData *meta, unsigned activityId, unsigned id, roxiemem::RoxieHeapFlags flags) const
    {
        return createRoxieRowAllocator(cache, *rowManager, meta, activityId, id, flags);
    }
    virtual double queryAgentMachineCost() const
    {
        return agentMachineCost;
    }
    virtual void updateAggregates(IWorkUnit* lockedwu) override
    {
        statsAggregator.updateAggregates(lockedwu);
    }
    virtual void mergeAggregatorStats(IStatisticCollection & stats, unsigned wfid, const char *graphname, unsigned sgId) override
    {
        statsAggregator.recordStats(&stats, wfid, graphname, sgId);
    }
};

//---------------------------------------------------------------------------

class EclSubGraph;
interface IHThorActivity;

class EclCounterMeta : implements IOutputMetaData, public CInterface
{
public:
    IMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void *rec)         { return sizeof(thor_loop_counter_t); }
    virtual size32_t getMinRecordSize() const               { return sizeof(thor_loop_counter_t); }
    virtual size32_t getFixedSize() const                   { return sizeof(thor_loop_counter_t); }
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return 0; }
    virtual const RtlTypeInfo * queryTypeInfo() const { return nullptr; }
    virtual void destruct(byte * self) {}
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual ISourceRowPrefetcher * createDiskPrefetcher() { return NULL; }
    virtual IOutputMetaData * querySerializedDiskMeta() { return this; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual void process(const byte * self, IFieldProcessor & target, unsigned from, unsigned to) {}
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
    virtual IOutputMetaData * queryChildMeta(unsigned i) { return NULL; }
    virtual const RtlRecord &queryRecordAccessor(bool expand) const { throwUnexpected(); }  // Could be implemented if needed
};

class EclBoundLoopGraph : implements IHThorBoundLoopGraph, public CInterface
{
public:
    EclBoundLoopGraph(IAgentContext & _agent, IEclLoopGraph * _graph, IOutputMetaData * _resultMeta, unsigned _activityId);
    IMPLEMENT_IINTERFACE

    virtual IHThorGraphResults * execute(void * counterRow, ConstPointerArray & rows, const byte * parentExtract);
    virtual void execute(void * counterRow, IHThorGraphResults * graphLoopResults, const byte * parentExtract);

protected:
    Linked<IEclLoopGraph> graph;
    IAgentContext & agent;
    Linked<IOutputMetaData> resultMeta;
    Linked<IOutputMetaData> counterMeta;
    Owned<IEngineRowAllocator> inputAllocator;
    Owned<IEngineRowAllocator> counterAllocator;
    unsigned activityId;
};

class EclGraphElement : public CInterface
{
public:
    EclGraphElement(EclSubGraph * _subgraph, EclSubGraph * _resultsGraph, IProbeManager * _probeManager);

    void addDependsOn(EclSubGraph & other, EclGraphElement * sourceActivity, int controlId);
    bool alreadyUpToDate(IAgentContext & agent);
    void createActivity(IAgentContext & agent, EclSubGraph * owner);
    void createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * xgmml);
    void executeDependentActions(IAgentContext & agent, const byte * parentExtract, int controlId);
    void extractResult(size32_t & retSize, void * & ret);

    bool prepare(IAgentContext & agent, const byte * parentExtract, bool checkDependencies);
    IHThorInput * queryOutput(unsigned idx);
    void updateProgress(IAgentContext & agent);
    void updateProgress(IStatisticGatherer &progress);

    void ready();
    void execute() { if (!alreadyUpdated) activity->execute(); }
    void onStart(const byte * parentExtract, CHThorDebugContext * debugContext);
    void stop() { if (!alreadyUpdated) activity->stop(); }

    IHThorException * makeWrappedException(IException * e);

protected:
    IHThorArg * createHelper(IAgentContext & agent, EclSubGraph * owner);
    void callOnCreate(IHThorArg & helper, IAgentContext & agent);

public:
    unsigned id;
    EclHelperFactory helperFactory;
    Owned<IHThorArg> arg;
    Owned<IHThorActivity> activity;
    Owned<IPropertyTree> node;
    EclSubGraph * subgraph;
    EclSubGraph * resultsGraph;
    ThorActivityKind kind;
    unsigned ownerId;
    bool isSink;
    bool isLocal;
    bool isGrouped;
    bool isResult;
    bool onlyUpdateIfChanged;
    bool alreadyUpdated;
    bool isEof;
    bool isCodeSigned = false;
    unsigned whichBranch;
    CIArrayOf<EclGraphElement> branches;
    UnsignedArray branchIndexes;
    EclGraphElement * conditionalLink;
    CICopyArrayOf<EclSubGraph> dependentOn;
    CICopyArrayOf<EclGraphElement> dependentOnActivity;
    IntArray dependentControlId;
    IProbeManager * probeManager;
    const byte * savedParentExtract = nullptr;

    Owned<EclBoundLoopGraph> loopGraph;
};

class EclGraphCondition : public CInterface
{
public:
    void execute(IAgentContext & agent);

protected:
    Linked<EclGraphElement> condition;
};

//NB: I don't think the members of GraphResult need to be protected with a critical section because
// only one thing can modify a given result, and nothing can read while writing is occurring.
// getOwnRow() is an exception, but only called on one thread at a time.
class UninitializedGraphResult : implements IHThorGraphResult, public CInterface
{
public:
    UninitializedGraphResult(unsigned _id) { id = _id; }
    IMPLEMENT_IINTERFACE

    virtual void addRowOwn(const void * row) override;
    virtual const void * queryRow(unsigned whichRow) override;
    virtual void getLinkedResult(unsigned & count, const byte * * & ret) override;
    virtual const void * getOwnRow(unsigned whichRow) override;
    virtual const void * getLinkedRowResult() override;

protected:
    unsigned id;
};

class GraphResult : implements IHThorGraphResult, public CInterface
{
public:
    GraphResult(IEngineRowAllocator * _ownedRowsetAllocator) : rowsetAllocator(_ownedRowsetAllocator) { meta = _ownedRowsetAllocator->queryOutputMeta(); }
    IMPLEMENT_IINTERFACE


    virtual void addRowOwn(const void * row) override;
    virtual const void * queryRow(unsigned whichRow) override;
    virtual void getLinkedResult(unsigned & count, const byte * * & ret) override;
    virtual const void * getOwnRow(unsigned whichRow) override;
    virtual const void * getLinkedRowResult() override;

protected:
    Owned<IEngineRowAllocator> rowsetAllocator;
    IOutputMetaData * meta;
    OwnedHThorRowArray rows;
};

class GraphResults : implements IHThorGraphResults, public CInterface
{
public:
    GraphResults(unsigned _maxResults = 0);
    IMPLEMENT_IINTERFACE

    void addResult(GraphResult * result)            { results.append(*LINK(result)); }
    int  ordinality()                               { return results.ordinality(); }
    void init(unsigned _maxResults);

    virtual void clear();
    virtual IHThorGraphResult * queryResult(unsigned id) override;
    virtual IHThorGraphResult * queryGraphLoopResult(unsigned id) override;
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator) override;
    virtual IHThorGraphResult * createResult(IEngineRowAllocator * ownedRowsetAllocator) override;
    virtual IHThorGraphResult * createGraphLoopResult(IEngineRowAllocator * ownedRowsetAllocator) override { throwUnexpected(); }

    virtual void setResult(unsigned id, IHThorGraphResult * result) override;

//interface IEclGraphResults
    virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id) override
    {
        queryResult(id)->getLinkedResult(count, ret);
    }
    virtual void getDictionaryResult(unsigned & count, const byte * * & ret, unsigned id) override
    {
        queryResult(id)->getLinkedResult(count, ret);
    }
    virtual const void * getLinkedRowResult(unsigned id) override
    {
        return queryResult(id)->getLinkedRowResult();
    }
protected:
    void ensureAtleast(unsigned id);

protected:
    IArrayOf<IHThorGraphResult> results;
    CriticalSection cs;
};


class RedirectedAgentContext : public IndirectAgentContext
{
public:
    virtual ICodeContext *queryCodeContext()
    {
        return codeContext;
    }
    void setCodeContext(ICodeContext * _codeContext)
    {
        codeContext = _codeContext;
    }

protected:
    ICodeContext * codeContext;
};

class EclSubGraph : public CInterface, implements ILocalEclGraphResults, public IEclLoopGraph, public IThorChildGraph
{
    friend class EclGraphElement;
private:

    RedirectedAgentContext subgraphAgentContext;
    class SubGraphCodeContext : public IndirectCodeContextEx
    {
    public:
        void setContainer(EclSubGraph * _container)
        {
            container = _container;
        }
        void setWfid(unsigned _wfid)
        {
            wfid = _wfid;
        }
    // ICodeContext
        virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) override
        {
            if ((unsigned __int64)activityId == container->queryId())
                return container;
            return ctx->resolveLocalQuery(activityId);
        }
        virtual unsigned getWorkflowId() const override
        {
            return wfid;
        }
    protected:
        EclSubGraph * container = nullptr;
        unsigned wfid = 0;
    } subgraphCodeContext;

public:
    EclSubGraph(IAgentContext & _agent, EclGraph &parent, EclSubGraph * _owner, unsigned subGraphSeqNo, CHThorDebugContext * _debugContext, IProbeManager * _probeManager);
    IMPLEMENT_IINTERFACE

    void createFromXGMML(EclGraph * graph, ILoadedDllEntry * dll, IPropertyTree * xgmml, unsigned & subGraphSeqNo, EclSubGraph * resultsGraph);
    void execute(const byte * parentExtract);
    void executeChild(const byte * parentExtract, IHThorGraphResults * results, IHThorGraphResults * _graphLoopResults);
    void executeLibrary(const byte * parentExtract, IHThorGraphResults * results);
    void executeSubgraphs(const byte * parentExtract);
    EclGraphElement * idToActivity(unsigned id);
    void reset();
    void updateProgress(IStatisticGatherer & progress);
    void updateProgress();
    void doExecuteChild(const byte * parentExtract);
    IEclLoopGraph * resolveLoopGraph(unsigned id);

//interface IEclGraphResults
    virtual IHThorGraphResult * queryResult(unsigned id);
    virtual IHThorGraphResult * queryGraphLoopResult(unsigned id);
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator);
    virtual IHThorGraphResult * createGraphLoopResult(IEngineRowAllocator * ownedRowsetAllocator);
    virtual IEclGraphResults * evaluate(unsigned parentExtractSize, const byte * parentExtract);

    virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id) override;
    virtual void getDictionaryResult(size32_t & tcount, const byte * * & tgt, unsigned id) override;
    virtual const void * getLinkedRowResult(unsigned id);
    inline unsigned __int64 queryId() const
    {
        return id;
    }

public:
    //called from conditions, but needs restructuring
    void createActivities();

protected:
    void doExecute(const byte * parentExtract, bool checkDependencies);
    void cleanupActivities();
    bool prepare(const byte * parentExtract, bool checkDependencies);

public:
    unsigned id;
    unsigned seqNo;
    Owned<IPropertyTree> xgmml;
    bool isSink;
    bool executed;
    bool created;
    unsigned __int64 startGraphTime;
    cycle_t elapsedGraphCycles;
    EclGraph &parent;
    EclSubGraph * owner;
    unsigned parentActivityId;
    unsigned numResults;
    CIArrayOf<EclGraphElement> elements;

    IArrayOf<IHThorInput> probes;
    CIArrayOf<EclSubGraph> subgraphs;
    Owned<IHThorGraphResults> localResults;
    Owned<IHThorGraphResults> graphLoopResults;
    CIArrayOf<EclGraphElement> sinks;
    IAgentContext * agent;
    CHThorDebugContext * debugContext;
    IProbeManager * probeManager;
    CriticalSection evaluateCrit;
    bool isChildGraph;
    bool isLoopBody;
};

typedef EclSubGraph * EclSubGraphPtr;

typedef MapBetween<graphid_t, graphid_t, EclSubGraphPtr, EclSubGraphPtr> SubGraphMapping;

class EclGraph : public CInterface
{
    RedirectedAgentContext graphAgentContext;
    class SubGraphCodeContext : public IndirectCodeContextEx
    {
    public:
        void setContainer(EclGraph * _container)
        {
            container = _container;
        }
        void setWfid(unsigned _wfid)
        {
            wfid = _wfid;
        }
    // ICodeContext
        virtual IThorChildGraph * resolveChildQuery(__int64 subgraphId, IHThorArg * colocal) override
        {
            return container->resolveChildQuery((unsigned)subgraphId);
        }
        virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) override
        {
            return container->resolveLocalQuery((unsigned)activityId);
        }
        virtual unsigned getWorkflowId() const override
        {
            return wfid;
        }
    protected:
        EclGraph * container = nullptr;
        unsigned wfid = 0;
    } graphCodeContext;

public:
    EclGraph(IAgentContext & _agent, const char *_graphName, IConstWorkUnit * _wu, bool _isLibrary, CHThorDebugContext * _debugContext, IProbeManager * _probeManager, unsigned _wfid) :
                            graphName(_graphName), wu(_wu), debugContext(_debugContext), probeManager(_probeManager), wfid(_wfid)
    {
        isLibrary = _isLibrary;
        graphCodeContext.set(_agent.queryCodeContext());
        graphCodeContext.setWfid(wfid);
        graphCodeContext.setContainer(this);
        graphAgentContext.setCodeContext(&graphCodeContext);
        graphAgentContext.set(&_agent);
        agent = &graphAgentContext;
        aborted = false;
    }

    void createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * xgmml);
    void execute(const byte * parentExtract);
    void executeLibrary(const byte * parentExtract, IHThorGraphResults * results);
    IWUGraphStats *updateStats(StatisticCreatorType creatorType, const char * creator, unsigned wfid, unsigned subgraph);
    void updateWUStatistic(IWorkUnit* lockedwu, StatisticScopeType scopeType, const char* scope, StatisticKind kind, const char* descr, long long unsigned int value);
    void updateAggregates(IWorkUnit* lockedwu);
    EclSubGraph * idToGraph(unsigned id);
    EclGraphElement * idToActivity(unsigned id);
    const char *queryGraphName() { return graphName; }
    IThorChildGraph * resolveChildQuery( unsigned subgraphId);
    IEclGraphResults * resolveLocalQuery(unsigned subgraphId);
    IEclLoopGraph * resolveLoopGraph(unsigned id);
    EclGraphElement * recurseFindActivityFromId(EclSubGraph * subGraph, unsigned id);
    void updateLibraryProgress();
    void abort();

    void associateSubGraph(EclSubGraph * subgraph);

    inline bool queryLibrary() const { return isLibrary; }
    inline unsigned queryWfid() const { return wfid; }

protected:
    IAgentContext * agent;
    CIArrayOf<EclSubGraph> graphs;
    GraphResults globalResults;
    StringAttr graphName;
    SubGraphMapping subgraphMap;
    Linked<IConstWorkUnit> wu;
    bool isLibrary;
    CHThorDebugContext * debugContext;
    IProbeManager * probeManager;
    unsigned wfid;
    bool aborted;
};


#endif
