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
#ifndef ECLAGENT_IPP_INCL
#define ECLAGENT_IPP_INCL

#include "dasds.hpp"
#include "dadfs.hpp"
#include "eclhelper.hpp"
#include "deftype.hpp"
#include "jthread.hpp"
#include "dllserver.hpp"

//#include "agentctx.hpp"
#include "hthor.hpp"
#include "thorxmlwrite.hpp"
#include "workflow.hpp"
#include "roxierow.hpp"
#include "roxiedebug.hpp"
#include <stdexcept> 
#include "thorplugin.hpp"
#include "thorcommon.hpp"

#define MAX_EDGEDATA_LENGTH 30000
#define MAX_HEX_SIZE 500
#define DEFAULT_PERSIST_COPIES (-1)

class EclGraph;
typedef unsigned __int64 graphid_t;
typedef unsigned __int64 activityid_t;

//=================================================================================

//The following don't link their arguments because that creates a circular reference
//But I wish there was a better way
class IndirectAgentContext : public CInterface, implements IAgentContext
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
    virtual unsigned getWorkflowId()
    {
        return ctx->getWorkflowId();
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
    virtual IRemoteConnection *startPersist(const char * name)
    {
        return ctx->startPersist(name);
    }
    virtual void finishPersist(IRemoteConnection *persistLock)
    {
        ctx->finishPersist(persistLock);
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
    virtual void clearPersist(const char * logicalName)
    {
        ctx->clearPersist(logicalName);
    }
    virtual void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC)
    {
        ctx->updatePersist(persistLock, logicalName, eclCRC, allCRC);
    }
    virtual void checkPersistMatches(const char * logicalName, unsigned eclCRC)
    {
        ctx->checkPersistMatches(logicalName, eclCRC);
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
    virtual IConstWorkUnit *queryWorkUnit()
    {
        return ctx->queryWorkUnit();
    }
    virtual IWorkUnit *updateWorkUnit()
    {
        return ctx->updateWorkUnit();
    }
    virtual void unlockWorkUnit()
    {
        ctx->unlockWorkUnit();
    }
    virtual ILocalOrDistributedFile *resolveLFN(const char *logicalName, const char *errorTxt, bool optional, bool noteRead, bool write, StringBuffer * expandedlfn)
    {
        return ctx->resolveLFN(logicalName, errorTxt, optional, noteRead, write, expandedlfn);
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
    virtual void logFileAccess(IDistributedFile * file, char const * component, char const * type)
    {
        ctx->logFileAccess(file, component, type);
    }
    virtual IRecordLayoutTranslatorCache * queryRecordLayoutTranslatorCache() const
    {
        return ctx->queryRecordLayoutTranslatorCache();
    }
    virtual void addWuException(const char * text, unsigned code, unsigned severity, char const * source)
    {
        ctx->addWuException(text, code, severity, source);
    }
    virtual IHThorGraphResults * executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, bool embedded, const byte * parentExtract)
    {
        return ctx->executeLibraryGraph(libraryName, expectedInterfaceHash, activityId, embedded, parentExtract);
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

    virtual void updateWULogfile()                  { return ctx->updateWULogfile(); }

protected:
    IAgentContext * ctx;
};

//---------------------------------------------------------------------------

class EclAgent;

class EclAgentWorkflowMachine : public WorkflowMachine
{
private:
    class PersistVersion : public CInterface
    {
    public:
        PersistVersion(char const * _logicalName, unsigned _eclCRC, unsigned __int64 _allCRC, bool _isFile) : logicalName(_logicalName), eclCRC(_eclCRC), allCRC(_allCRC), isFile(_isFile) {}
        StringAttr logicalName;
        unsigned eclCRC;
        unsigned __int64 allCRC;
        bool isFile;
    };

public:
    EclAgentWorkflowMachine(EclAgent & _agent);
    void returnPersistVersion(char const * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        persist.setown(new PersistVersion(logicalName, eclCRC, allCRC, isFile));
    }

protected:
    virtual void begin();
    virtual void end();
    virtual void schedulingStart();
    virtual bool schedulingPull();
    virtual bool schedulingPullStop();
    virtual void reportContingencyFailure(char const * type, IException * e);
    virtual void checkForAbort(unsigned wfid, IException * handling);
    virtual void doExecutePersistItem(IRuntimeWorkflowItem & item);

private:
    void prelockPersists();

private:
    EclAgent & agent;
    Owned<IRemoteConnection> runlock;
    void obtainRunlock();
    void releaseRunlock();
    void syncWorkflow();
    Owned<IWorkflowScheduleConnection> wfconn;
    Owned<PersistVersion> persist;
    bool persistsPrelocked;
    MapStringToMyClass<IRemoteConnection> persistCache;
};

class EclAgentQueryLibrary : public CInterface
{
public:
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
    IMPLEMENT_IINTERFACE;
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
class EclAgent : public CInterface, implements IAgentContext, implements ICodeContext, implements IRowAllocatorMetaActIdCacheCallback
{
private:
    friend class EclAgentWorkflowMachine;

    Owned<EclAgentWorkflowMachine> workflow;
    Owned<IWorkUnit> wuWrite;
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
    Owned<IUserDescriptor> standAloneUDesc;
    outputFmts outputFmt;
    unsigned __int64 stopAfter;
    CriticalSection wusect;
    StringArray tempFiles;
    CriticalSection tfsect;
    Array persistReadLocks;

    Owned<ILoadedDllEntry> dll;
    CIArrayOf<EclAgentQueryLibrary> queryLibraries;
    StringArray clusterNames;
    unsigned int clusterWidth;
    Owned<IDistributedFileTransaction> superfiletransaction;
    mutable Owned<IRowAllocatorMetaActIdCache> allocatorMetaCache;
    Owned<EclGraph> activeGraph;
    Owned<IRecordLayoutTranslatorCache> rltCache;
    Owned<CHThorDebugContext> debugContext;
    Owned<IProbeManager> probeManager;
    StringAttr allowedPipeProgs;
    SafePluginMap *pluginMap;
    IProperties *globals;
    IPropertyTree *config;
    ILogMsgHandler *logMsgHandler;
    StringAttr agentTempDir;
    Owned<IOrderedOutputSerializer> outputSerializer;
    int retcode;

private:
    void doSetResultString(type_t type, const char * stepname, unsigned sequence, int len, const char *val);
    IEclProcess *loadProcess();
    StringBuffer & getTempfileBase(StringBuffer & buff);
    const char *queryTempfilePath();
    void deleteTempFiles();

    void processXmlParams(const IPropertyTree *params);
    bool checkPersistUptoDate(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile, StringBuffer & errText);
    bool isPersistUptoDate(Owned<IRemoteConnection> &persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile);
    bool changePersistLockMode(IRemoteConnection *persistLock, unsigned mode, const char * name, bool repeat);
    bool expandLogicalName(StringBuffer & fullname, const char * logicalName);
    IRemoteConnection *getPersistReadLock(const char * logicalName);
    void doSimpleResult(type_t type, int size, char * buffer, int sequence);
    IWUResult *updateResult(const char *name, unsigned sequence);
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
    EclAgentQueryLibrary * loadEclLibrary(const char * libraryName, unsigned expectedInterfaceHash, bool embedded);
    virtual bool getWorkunitResultFilename(StringBuffer & diskFilename, const char * wuid, const char * name, int seq);
    virtual IDebuggableContext *queryDebugContext() const { return debugContext; };

    EclGraph * loadGraph(const char * graphName, IConstWorkUnit * wu, ILoadedDllEntry * dll, bool isLibrary);

    class cAbortMonitor: public Thread, implements IExceptionHandler
    {
        EclAgent &parent;
    public:
        Semaphore sem;
        bool stopping;
        unsigned guillotinetimeout;
        cAbortMonitor(EclAgent &_parent) : Thread("EclAgent Abort Monitor"), parent(_parent) { guillotinetimeout=0; stopping=false; }
        int  run()  { parent.abortMonitor(); return 0; }
        void stop() { stopping = true; sem.signal(); join(1000*10); }
        void setGuillotineTimeout(unsigned secs) { guillotinetimeout = secs; sem.signal(); }
        bool fireException(IException *e)
        {
            StringBuffer text;
            e->errorMessage(text);
            parent.fatalAbort(false,text.str());
            return true; // won't return hopefully!
        }
    } *abortmonitor;

public:
    IMPLEMENT_IINTERFACE;

    EclAgent(IConstWorkUnit *wu, const char *_wuid, bool _checkVersion, bool _resetWorkflow, bool _noRetry, char const * _logname, const char *_allowedPipeProgs, IPropertyTree *_queryXML, IProperties *_globals, IPropertyTree *_config, ILogMsgHandler * _logMsgHandler);
    ~EclAgent();

    void setBlocked();
    void setRunning();
    void setDebugPaused();
    void setDebugRunning();
    void setBlockedOnPersist(const char * logicalName);
    void setStandAloneOptions(bool _isStandAloneExe, bool _isRemoteWorkunit, bool _resolveFilesLocally, bool _writeResultsToStdout, outputFmts _outputFmt, IUserDescriptor *_standAloneUDesc);
    inline bool needToLockWorkunit() { return !isStandAloneExe; }           //If standalone exe then either no dali, or a unique remote workunit.

    virtual void setResultInt(const char * stepname, unsigned sequence, __int64);
    virtual void setResultReal(const char * stepname, unsigned sequence, double);
    virtual void setResultBool(const char * stepname, unsigned sequence, bool);
    virtual void setResultString(const char * stepname, unsigned sequence, int len, const char *);
    virtual void setResultData(const char * stepname, unsigned sequence, int len, const void *);
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend);
    virtual void setResultRaw(const char * stepname, unsigned sequence, int len, const void *);
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer);
    virtual void setResultUInt(const char * stepname, unsigned sequence, unsigned __int64);
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
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer);
    virtual void getResultDictionary(size32_t & tcount, byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher);
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
    virtual void addWuException(const char * text, unsigned code, unsigned severity);
    virtual void addWuException(const char * text, unsigned code, unsigned severity, char const * source);
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort);
    virtual IUserDescriptor *queryUserDescriptor();
    virtual void selectCluster(const char * cluster);
    virtual void restoreCluster();
    virtual IRemoteConnection *startPersist(const char * name);
    virtual void finishPersist(IRemoteConnection *persistLock);
    virtual void clearPersist(const char * logicalName);
    virtual void updatePersist(IRemoteConnection *persistLock, const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC);
    virtual void checkPersistMatches(const char * logicalName, unsigned eclCRC);
    virtual void deleteLRUPersists(const char * logicalName, int keep);
    virtual bool queryResolveFilesLocally() { return resolveFilesLocally; }
    virtual bool queryRemoteWorkunit() { return isRemoteWorkunit; }
    virtual bool queryWriteResultsToStdout() { return writeResultsToStdout; }
    virtual IOrderedOutputSerializer * queryOutputSerializer() { return outputSerializer; }
    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
    virtual IEngineContext *queryEngineContext() { return NULL; }
    virtual char *getDaliServers();

    unsigned __int64 queryStopAfter() { return stopAfter; }


//New workflow interface
    virtual void setWorkflowCondition(bool value) { if(workflow) workflow->setCondition(value); }
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile)
    {
        if(workflow) workflow->returnPersistVersion(logicalName, eclCRC, allCRC, isFile);
    }

    virtual void fail(int code, char const * str); 
    void failv(int code, char const * fmt, ...) __attribute__((format(printf, 3, 4)));
    virtual int queryLastFailCode();
    virtual void getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag);
    virtual void getEventName(size32_t & outLen, char * & outStr);
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag);
    //virtual void logException(IEclException *e);  
    virtual char *resolveName(const char *in, char *out, unsigned outlen);
    virtual void logFileAccess(IDistributedFile * file, char const * component, char const * type);
    virtual IRecordLayoutTranslatorCache * queryRecordLayoutTranslatorCache() const { return rltCache; }
    virtual ILocalOrDistributedFile  *resolveLFN(const char *logicalName, const char *errorTxt=NULL, bool optional=false, bool noteRead=true, bool write=false, StringBuffer * expandedlfn=NULL);

    virtual void executeThorGraph(const char * graphName);
    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract);
    virtual IHThorGraphResults * executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, bool embedded, const byte * parentExtract);
    virtual IThorChildGraph * resolveChildQuery(__int64 subgraphId, IHThorArg * colocal);
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId);

    virtual IHThorGraphResults * createGraphLoopResults();

    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 hash);
    virtual void reportProgress(const char *msg, unsigned flags=0);
    virtual const char *noteTemporaryFile(const char *fname);
    virtual const char *noteTemporaryFilespec(const char *fspec);
    virtual const char *queryTemporaryFile(const char *fname);
    virtual void deleteFile(const char * logicalName);

    void addException(WUExceptionSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool failOnError, bool isAbort);
    void logException(IException *e);  
    void logException(WorkflowException *e);  
    void logException(std::exception & e);
    void logException(WUExceptionSeverity severity, unsigned code, const char * text, bool isAbort);

    void doProcess();
    void runProcess(IEclProcess *process);
    virtual void doNotify(char const * name, char const * text);
    virtual void doNotify(char const * name, char const * text, const char * target);
    void abortMonitor();
    void fatalAbort(bool userabort,const char *excepttext);

    virtual const char *loadResource(unsigned id);
    virtual ICodeContext *queryCodeContext();
    virtual bool isResult(const char * name, unsigned sequence);
    virtual unsigned getWorkflowId();// { return workflow->queryCurrentWfid(); }
    virtual IConstWorkUnit *queryWorkUnit();  // no link
    virtual IWorkUnit *updateWorkUnit();        // links
    virtual void unlockWorkUnit();      
    virtual void reloadWorkUnit();
    void addTimings();

// ICodeContext
    virtual unsigned getNodes();
    virtual unsigned getNodeNum() { return 0; }
    virtual char *getFilePart(const char *logicalPart, bool create=false);
    virtual unsigned __int64 getFileOffset(const char *logicalPart) { UNIMPLEMENTED; return 0; }
    virtual char *getOutputDir() { UNIMPLEMENTED; }
    virtual char *getWuid();
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
    virtual const char *queryAllowedPipePrograms()
    {
        return allowedPipeProgs.get();
    }
    
    IGroup *getHThorGroup(StringBuffer &out);
    
    virtual void updateWULogfile();

// roxiemem::IRowAllocatorMetaActIdCacheCallback
    virtual IEngineRowAllocator *createAllocator(IOutputMetaData *meta, unsigned activityId, unsigned id, roxiemem::RoxieHeapFlags flags) const
    {
        return createRoxieRowAllocator(*rowManager, meta, activityId, id, flags);
    }
};

//---------------------------------------------------------------------------

class EclSubGraph;
interface IHThorActivity;

class EclCounterMeta : public CInterface, implements IOutputMetaData
{
public:
    IMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void *rec)         { return sizeof(thor_loop_counter_t); }
    virtual size32_t getMinRecordSize() const               { return sizeof(thor_loop_counter_t); }
    virtual size32_t getFixedSize() const                   { return sizeof(thor_loop_counter_t); }
    virtual void toXML(const byte * self, IXmlWriter & out) { }
    virtual unsigned getVersion() const                     { return OUTPUTMETADATA_VERSION; }
    virtual unsigned getMetaFlags()                         { return 0; }
    virtual void destruct(byte * self) {}
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual ISourceRowPrefetcher * createDiskPrefetcher(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputMetaData * querySerializedDiskMeta() { return this; }
    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) { return NULL; }
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) {}
};

class EclBoundLoopGraph : public CInterface, implements IHThorBoundLoopGraph
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
    void updateProgress(IWUGraphProgress &progress);

    void ready() { if (!alreadyUpdated) activity->ready(); }
    void execute() { if (!alreadyUpdated) activity->execute(); }
    void done() { if (!alreadyUpdated) activity->done(); }

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
    unsigned whichBranch;
    CIArrayOf<EclGraphElement> branches;
    UnsignedArray branchIndexes;
    EclGraphElement * conditionalLink;
    CopyCIArrayOf<EclSubGraph> dependentOn;
    CopyCIArrayOf<EclGraphElement> dependentOnActivity;
    IntArray dependentControlId;
    IProbeManager * probeManager;

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
// only one thing can modify a given result, and nothing can read while writing is occuring.
// getOwnRow() is an exception, but only called on one thread at a time.
class UninitializedGraphResult : public CInterface, implements IHThorGraphResult
{
public:
    UninitializedGraphResult(unsigned _id) { id = _id; }
    IMPLEMENT_IINTERFACE

    virtual void addRowOwn(const void * row);
    virtual const void * queryRow(unsigned whichRow);
    virtual void getLinkedResult(unsigned & count, byte * * & ret);
    virtual const void * getOwnRow(unsigned whichRow);

protected:
    unsigned id;
};

class GraphResult : public CInterface, implements IHThorGraphResult
{
public:
    GraphResult(IEngineRowAllocator * _ownedRowsetAllocator) : rowsetAllocator(_ownedRowsetAllocator) { meta = _ownedRowsetAllocator->queryOutputMeta(); }
    IMPLEMENT_IINTERFACE


    virtual void addRowOwn(const void * row);
    virtual const void * queryRow(unsigned whichRow);
    virtual void getLinkedResult(unsigned & count, byte * * & ret);
    virtual const void * getOwnRow(unsigned whichRow);

protected:
    Owned<IEngineRowAllocator> rowsetAllocator;
    IOutputMetaData * meta;
    OwnedHThorRowArray rows;
};

class GraphResults : public CInterface, implements IHThorGraphResults
{
public:
    GraphResults(unsigned _maxResults = 0);
    IMPLEMENT_IINTERFACE

    void addResult(GraphResult * result)            { results.append(*LINK(result)); }
    int  ordinality()                               { return results.ordinality(); }
    void init(unsigned _maxResults);

    virtual void clear();
    virtual IHThorGraphResult * queryResult(unsigned id);
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator);
    virtual IHThorGraphResult * createResult(IEngineRowAllocator * ownedRowsetAllocator);
    virtual void setResult(unsigned id, IHThorGraphResult * result);

//interface IEclGraphResults
    virtual void getLinkedResult(unsigned & count, byte * * & ret, unsigned id)
    {
        queryResult(id)->getLinkedResult(count, ret);
    }
    virtual void getDictionaryResult(unsigned & count, byte * * & ret, unsigned id)
    {
        queryResult(id)->getLinkedResult(count, ret);
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

    class LegacyInputProbe : public CInterface, implements IHThorInput, implements IInterface
    {
        IHThorInput  *in;
        EclSubGraph  *owner;
        size32_t    maxRowSize;

        StringBuffer edgeId;

    public:
        IMPLEMENT_IINTERFACE;

        LegacyInputProbe(IHThorInput *_in, EclSubGraph *_owner, unsigned _sourceId, int outputidx)
            : in(_in), owner(_owner)
        {
            edgeId.append(_sourceId).append("_").append(outputidx);
            maxRowSize = 0;
        }

        IOutputMetaData * queryOutputMeta() const { return in->queryOutputMeta(); }

        void ready() 
        {
            in->ready();
        }
        
        void done() 
        {
            in->done();
        }

        bool isGrouped() { return in->isGrouped(); }

        bool nextGroup(ConstPointerArray & group)
        {
            const void * next;
            while ((next = nextInGroup()) != NULL)
                group.append(next);
            if (group.ordinality())
                return true;
            return false;
        }
    
        const void *nextInGroup()
        {
            const void *ret = in->nextInGroup();
            if (ret)
            {
                size32_t size = in->queryOutputMeta()->getRecordSize(ret);
                if (size > maxRowSize)
                    maxRowSize = size;
            }
            return ret;
        }

        virtual void updateProgress(IWUGraphProgress &progress) const
        {
            IPropertyTree &edge = progress.updateEdge(owner->id, edgeId);
            edge.setPropInt64("@maxrowsize", maxRowSize);
            if (in)
                in->updateProgress(progress);
        }   
    };

    RedirectedAgentContext subgraphAgentContext;
    class SubGraphCodeContext : public IndirectCodeContext
    {
    public:
        virtual IEclGraphResults * resolveLocalQuery(__int64 activityId)
        {
            if (activityId == container->queryId())
                return container;
            return ctx->resolveLocalQuery(activityId);
        }
        void setContainer(EclSubGraph * _container)
        {
            container = _container;
        }

    protected:
        EclSubGraph * container;
    } subgraphCodeContext;

    friend class LegacyInputProbe;
    bool probeEnabled;

public:
    EclSubGraph(IAgentContext & _agent, EclGraph &parent, EclSubGraph * _owner, unsigned subGraphSeqNo, bool enableProbe, CHThorDebugContext * _debugContext, IProbeManager * _probeManager);
    IMPLEMENT_IINTERFACE

    void createFromXGMML(EclGraph * graph, ILoadedDllEntry * dll, IPropertyTree * xgmml, unsigned * subGraphSeqNo, EclSubGraph * resultsGraph);
    void execute(const byte * parentExtract);
    void executeChild(const byte * parentExtract, IHThorGraphResults * results, IHThorGraphResults * _graphLoopResults);
    void executeLibrary(const byte * parentExtract, IHThorGraphResults * results);
    void executeSubgraphs(const byte * parentExtract);
    EclGraphElement * idToActivity(unsigned id);
    void reset();
    void updateProgress(IWUGraphProgress & progress);
    void updateProgress();
    void doExecuteChild(const byte * parentExtract);
    IEclLoopGraph * resolveLoopGraph(unsigned id);

    IHThorInput *createLegacyProbe(IHThorInput      *in,
                             unsigned           sourceId,
                             unsigned           targetId,
                             int                outputidx,
                             IConstWorkUnit     *workunit)
    {
        if (probeEnabled)
        {
            LegacyInputProbe *probe = new LegacyInputProbe(in, this, sourceId, outputidx);
            probes.append(*probe);
            return probe;
        }
        else
            return in;
    }

//interface IEclGraphResults
    virtual IHThorGraphResult * queryResult(unsigned id);
    virtual IHThorGraphResult * queryGraphLoopResult(unsigned id);
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator);
    virtual IHThorGraphResult * createGraphLoopResult(IEngineRowAllocator * ownedRowsetAllocator);
    virtual IEclGraphResults * evaluate(unsigned parentExtractSize, const byte * parentExtract);

    virtual void getLinkedResult(unsigned & count, byte * * & ret, unsigned id);
    virtual void getDictionaryResult(size32_t & tcount, byte * * & tgt, unsigned id);
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
    EclGraph &parent;
    EclSubGraph * owner;
    unsigned parentActivityId;
    unsigned numResults;
    CIArrayOf<EclGraphElement> elements;

    IArrayOf<LegacyInputProbe> probes;
    CIArrayOf<EclSubGraph> subgraphs;
    Owned<IHThorGraphResults> localResults;
    Owned<IHThorGraphResults> graphLoopResults;
    CIArrayOf<EclGraphElement> sinks;
    IAgentContext * agent;
    CHThorDebugContext * debugContext;
    IProbeManager * probeManager;
    CriticalSection evaluateCrit;
    bool isChildGraph;
};

typedef EclSubGraph * EclSubGraphPtr;

typedef MapBetween<graphid_t, graphid_t, EclSubGraphPtr, EclSubGraphPtr> SubGraphMapping;

class EclGraph : public CInterface
{
    RedirectedAgentContext graphAgentContext;
    class SubGraphCodeContext : public IndirectCodeContext
    {
    public:
        IThorChildGraph * resolveChildQuery(__int64 subgraphId, IHThorArg * colocal)
        {
            return container->resolveChildQuery((unsigned)subgraphId);
        }

        IEclGraphResults * resolveLocalQuery(__int64 activityId)
        { 
            return container->resolveLocalQuery((unsigned)activityId);
        }
        void setContainer(EclGraph * _container)
        {
            container = _container;
        }

    protected:
        EclGraph * container;
    } graphCodeContext;

public:
    EclGraph(IAgentContext & _agent, const char *_graphName, IConstWorkUnit * _wu, bool _isLibrary, CHThorDebugContext * _debugContext, IProbeManager * _probeManager) :
                            graphName(_graphName), wu(_wu), debugContext(_debugContext), probeManager(_probeManager)
    {
        isLibrary = _isLibrary;
        graphCodeContext.set(_agent.queryCodeContext());
        graphCodeContext.setContainer(this);
        graphAgentContext.setCodeContext(&graphCodeContext);
        graphAgentContext.set(&_agent);
        agent = &graphAgentContext;
        aborted = false;
    }

    void createFromXGMML(ILoadedDllEntry * dll, IPropertyTree * xgmml, bool enableProbe);
    void execute(const byte * parentExtract);
    void executeLibrary(const byte * parentExtract, IHThorGraphResults * results);
    IConstWUGraphProgress * getGraphProgress();
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

    inline bool queryLibrary() { return isLibrary; }

protected:
    IAgentContext * agent;
    CIArrayOf<EclSubGraph> graphs;
    StringAttr graphName;
    SubGraphMapping subgraphMap;
    Linked<IConstWorkUnit> wu;
    bool isLibrary;
    CHThorDebugContext * debugContext;
    IProbeManager * probeManager;
    bool aborted;
};


#endif
