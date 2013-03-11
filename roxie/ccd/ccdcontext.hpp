/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#ifndef _CCDCONTEXT_INCL
#define _CCDCONTEXT_INCL

#include <jlib.hpp>
#include <workunit.hpp>

interface IActivityGraph;
interface IQueryFactory;
interface IResolvedFile;
interface IRoxieDaliHelper;
interface IRoxieServerActivity;
interface IRoxieWriteHandler;
interface IWorkUnitRowReader;
interface SafeSocket;
class FlushingStringBuffer;
class HttpHelper;
class LibraryCallFactoryExtra;
class WorkflowMachine;

interface IWorkUnitRowReader : public IInterface
{
    virtual const void * nextInGroup() = 0;
    virtual void getResultRowset(size32_t & tcount, byte * * & tgt) = 0;
};

interface IRoxieServerContext;

interface IRoxieSlaveContext : extends IRoxieContextLogger
{
    virtual ICodeContext *queryCodeContext() = 0;
    virtual void checkAbort() = 0;
    virtual void notifyAbort(IException *E) = 0;
    virtual IActivityGraph * queryChildGraph(unsigned id) = 0;
    virtual void noteChildGraph(unsigned id, IActivityGraph *childGraph) = 0;
    virtual roxiemem::IRowManager &queryRowManager() = 0;
    virtual unsigned parallelJoinPreload() = 0;
    virtual unsigned concatPreload() = 0;
    virtual unsigned fetchPreload() = 0;
    virtual unsigned fullKeyedJoinPreload() = 0;
    virtual unsigned keyedJoinPreload() = 0;
    virtual unsigned prefetchProjectPreload() = 0;
    virtual void addSlavesReplyLen(unsigned len) = 0;
    virtual const char *queryAuthToken() = 0;
    virtual const IResolvedFile *resolveLFN(const char *filename, bool isOpt) = 0;
    virtual IRoxieWriteHandler *createLFN(const char *filename, bool overwrite, bool extend, const StringArray &clusters) = 0;
    virtual void onFileCallback(const RoxiePacketHeader &header, const char *lfn, bool isOpt, bool isLocal) = 0;
    virtual IActivityGraph * getLibraryGraph(const LibraryCallFactoryExtra &extra, IRoxieServerActivity *parentActivity) = 0;
    virtual void noteProcessed(const IRoxieContextLogger &_activityContext, const IRoxieServerActivity *activity, unsigned _idx, unsigned _processed, unsigned __int64 _totalCycles, unsigned __int64 _localCycles) const = 0;
    virtual IProbeManager *queryProbeManager() const = 0;
    virtual bool queryTimeActivities() const = 0;
    virtual IDebuggableContext *queryDebugContext() const = 0;
    virtual void printResults(IXmlWriter *output, const char *name, unsigned sequence) = 0;
    virtual void setWUState(WUState state) = 0;
    virtual bool checkWuAborted() = 0;
    virtual IWorkUnit *updateWorkUnit() const = 0;
    virtual IConstWorkUnit *queryWorkUnit() const = 0;
    virtual IRoxieServerContext *queryServerContext() = 0;
    virtual IWorkUnitRowReader *getWorkunitRowReader(const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, IEngineRowAllocator *rowAllocator, bool isGrouped) = 0;
};

interface IRoxieServerContext : extends IInterface
{
    virtual IGlobalCodeContext *queryGlobalCodeContext() = 0;
    virtual FlushingStringBuffer *queryResult(unsigned sequence) = 0;
    virtual void setResultXml(const char *name, unsigned sequence, const char *xml) = 0;
    virtual void appendResultDeserialized(const char *name, unsigned sequence, size32_t count, byte **data, bool extend, IOutputMetaData *meta) = 0;
    virtual void appendResultRawContext(const char *name, unsigned sequence, int len, const void * data, int numRows, bool extend, bool saveInContext) = 0;
    virtual roxiemem::IRowManager &queryRowManager() = 0;

    virtual void process() = 0;
    virtual void done(bool failed) = 0;
    virtual void flush(unsigned seqNo) = 0;
    virtual unsigned getMemoryUsage() = 0;
    virtual unsigned getSlavesReplyLen() = 0;

    virtual unsigned getXmlFlags() const = 0;
    virtual bool outputResultsToWorkUnit() const = 0;
    virtual bool outputResultsToSocket() const = 0;

    virtual IRoxieDaliHelper *checkDaliConnection() = 0;
};

interface IDeserializedResultStore : public IInterface
{
    virtual int addResult(size32_t count, byte **data, IOutputMetaData *meta) = 0;
    virtual void queryResult(int id, size32_t &count, byte ** &data) const = 0;
    virtual IWorkUnitRowReader *createDeserializedReader(int id) const = 0;
    virtual void serialize(unsigned & tlen, void * & tgt, int id, ICodeContext *ctx) const = 0;
};

typedef IEclProcess* (* EclProcessFactory)();

extern IDeserializedResultStore *createDeserializedResultStore();
extern IRoxieSlaveContext *createSlaveContext(const IQueryFactory *factory, const SlaveContextLogger &logctx, unsigned timeLimit, memsize_t memoryLimit, IRoxieQueryPacket *packet);
extern IRoxieServerContext *createRoxieServerContext(IPropertyTree *context, const IQueryFactory *factory, SafeSocket &client, bool isXml, bool isRaw, bool isBlocked, HttpHelper &httpHelper, bool trim, unsigned priority, const IRoxieContextLogger &logctx, PTreeReaderOptions xmlReadFlags);
extern IRoxieServerContext *createOnceServerContext(const IQueryFactory *factory, const IRoxieContextLogger &_logctx);
extern IRoxieServerContext *createWorkUnitServerContext(IConstWorkUnit *wu, const IQueryFactory *factory, const IRoxieContextLogger &logctx);
extern WorkflowMachine *createRoxieWorkflowMachine(IPropertyTree *_workflowInfo, bool doOnce, const IRoxieContextLogger &_logctx);

#endif
