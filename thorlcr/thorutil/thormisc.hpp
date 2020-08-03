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

#ifndef _THORMISC_
#define _THORMISC_

#include "jiface.hpp"
#include "jdebug.hpp"
#include "jthread.hpp"
#include "jexcept.hpp"
#include "jarray.hpp"
#include "jfile.hpp"
#include "jprop.hpp"
#include "jutil.hpp"
#include "jlog.hpp"
#include "mpcomm.hpp"

#include "workunit.hpp"
#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thorcommon.hpp"
#include "thor.hpp"

#ifdef GRAPH_EXPORTS
    #define graph_decl DECL_EXPORT
#else
    #define graph_decl DECL_IMPORT
#endif

/// Thor options, that can be hints, workunit options, or global settings
#define THOROPT_COMPRESS_SPILLS       "compressInternalSpills"  // Compress internal spills, e.g. spills created by lookahead or sort gathering  (default = true)
#define THOROPT_COMPRESS_SPILL_TYPE   "spillCompressorType"     // Compress spill type, e.g. FLZ, LZ4 (or other to get previous)                 (default = LZ4)
#define THOROPT_HDIST_SPILL           "hdistSpill"              // Allow distribute receiver to spill to disk, rather than blocking              (default = true)
#define THOROPT_HDIST_WRITE_POOL_SIZE "hdistSendPoolSize"       // Distribute send thread pool size                                              (default = 16)
#define THOROPT_HDIST_BUCKET_SIZE     "hdOutBufferSize"         // Distribute target bucket send size                                            (default = 1MB)
#define THOROPT_HDIST_BUFFER_SIZE     "hdInBufferSize"          // Distribute send buffer size (for all targets)                                 (default = 32MB)
#define THOROPT_HDIST_PULLBUFFER_SIZE "hdPullBufferSize"        // Distribute pull buffer size (receiver side limit, before spilling)
#define THOROPT_HDIST_CANDIDATELIMIT  "hdCandidateLimit"        // Limits # of buckets to push to the writers when send buffer is full           (default = is 50% largest)
#define THOROPT_HDIST_TARGETWRITELIMIT "hdTargetLimit"          // Limit # of writer threads working on a single target                          (default = unbound, but picks round-robin)
#define THOROPT_HDIST_COMP            "hdCompressorType"        // Distribute compressor to use                                                  (default = "LZ4")
#define THOROPT_HDIST_COMPOPTIONS     "hdCompressorOptions"     // Distribute compressor options, e.g. AES key                                   (default = "")
#define THOROPT_SPLITTER_SPILL        "splitterSpill"           // Force splitters to spill or not, default is to adhere to helper setting       (default = -1)
#define THOROPT_LOOP_MAX_EMPTY        "loopMaxEmpty"            // Max # of iterations that LOOP can cycle through with 0 results before errors  (default = 1000)
#define THOROPT_SMALLSORT             "smallSortThreshold"      // Use minisort approach, if estimate size of data to sort is below this setting (default = 0)
#define THOROPT_PARALLEL_FUNNEL       "parallelFunnel"          // Use parallel funnel impl. if !ordered                                         (default = true)
#define THOROPT_SORT_MAX_DEVIANCE     "sort_max_deviance"       // Max (byte) variance allowed during sort partitioning                          (default = 10Mb)
#define THOROPT_OUTPUT_FLUSH_THRESHOLD "output_flush_threshold" // When above limit, workunit result is flushed (committed to Dali)              (default = -1 [off])
#define THOROPT_PARALLEL_MATCH        "parallel_match"          // Use multi-threaded join helper (retains sort order without unsorted_output)   (default = false)
#define THOROPT_UNSORTED_OUTPUT       "unsorted_output"         // Allow Join results to be reodered, implies parallel match                     (default = false)
#define THOROPT_JOINHELPER_THREADS    "joinHelperThreads"       // Number of threads to use in threaded variety of join helper
#define THOROPT_LKJOIN_LOCALFAILOVER  "lkjoin_localfailover"    // Force SMART to failover to distributed local lookup join (for testing only)   (default = false)
#define THOROPT_LKJOIN_HASHJOINFAILOVER "lkjoin_hashjoinfailover" // Force SMART to failover to hash join (for testing only)                     (default = false)
#define THOROPT_MAX_KERNLOG           "max_kern_level"          // Max kernel logging level, to push to workunit, -1 to disable                  (default = 3)
#define THOROPT_COMP_FORCELZW         "forceLZW"                // Forces file compression to use LZW                                            (default = false)
#define THOROPT_COMP_FORCEFLZ         "forceFLZ"                // Forces file compression to use FLZ                                            (default = false)
#define THOROPT_COMP_FORCELZ4         "forceLZ4"                // Forces file compression to use LZ4                                            (default = false)
#define THOROPT_COMP_FORCELZ4HC       "forceLZ4HC"              // Forces file compression to use LZ4HC                                          (default = false)
#define THOROPT_TRACE_ENABLED         "traceEnabled"            // Output from TRACE activity enabled                                            (default = false)
#define THOROPT_TRACE_LIMIT           "traceLimit"              // Number of rows from TRACE activity                                            (default = 10)
#define THOROPT_READ_CRC              "crcReadEnabled"          // Enabled CRC validation on disk reads if file CRC are available                (default = true)
#define THOROPT_WRITE_CRC             "crcWriteEnabled"         // Calculate CRC's for disk outputs and store in file meta data                  (default = true)
#define THOROPT_READCOMPRESSED_CRC    "crcReadCompressedEnabled" // Enabled CRC validation on compressed disk reads if file CRC are available   (default = false)
#define THOROPT_WRITECOMPRESSED_CRC   "crcWriteCompressedEnabled" // Calculate CRC's for compressed disk outputs and store in file meta data     (default = false)
#define THOROPT_CHILD_GRAPH_INIT_TIMEOUT "childGraphInitTimeout" // Time to wait for child graphs to respond to initialization                  (default = 5*60 seconds)
#define THOROPT_SORT_COMPBLKSZ        "sortCompBlkSz"           // Block size used by compressed spill in a spilling sort                        (default = 0, uses row writer default)
#define THOROPT_KEYLOOKUP_QUEUED_BATCHSIZE "keyLookupQueuedBatchSize" // Number of rows candidates to gather before performing lookup against part (default = 1000)
#define THOROPT_KEYLOOKUP_FETCH_QUEUED_BATCHSIZE "fetchLookupQueuedBatchSize" // Number of rows candidates to gather before performing lookup against part (default = 1000)
#define THOROPT_KEYLOOKUP_MAX_LOOKUP_BATCHSIZE "keyLookupMaxLookupBatchSize"  // Maximum chunk of rows to process per cycle in lookup handler    (default = 1000)
#define THOROPT_KEYLOOKUP_MAX_THREADS "maxKeyLookupThreads"     // Maximum number of threads performing keyed lookups                            (default = 10)
#define THOROPT_KEYLOOKUP_MAX_FETCH_THREADS "maxFetchThreads"   // Maximum number of threads performing keyed lookups                            (default = 10)
#define THOROPT_KEYLOOKUP_MAX_PROCESS_THREADS "keyLookupMaxProcessThreads" // Maximum number of threads performing keyed lookups                 (default = 10)
#define THOROPT_KEYLOOKUP_MAX_QUEUED  "keyLookupMaxQueued"      // Total maximum number of rows (across all parts/threads) to queue              (default = 10000)
#define THOROPT_KEYLOOKUP_MIN_MB      "keyLookupMinJoinGroupMB" // Min(MB) for groups (across all parts/threads) to queue)                       (default = 50)
#define THOROPT_KEYLOOKUP_MAX_DONE    "keyLookupMaxDone"        // Maximum number of done items pending to be ready by next activity             (default = 10000)
#define THOROPT_KEYLOOKUP_PROCESS_BATCHLIMIT "keyLookupProcessBatchLimit" // Maximum number of key lookups on queue before passing to a processor (default = 1000)
#define THOROPT_FETCHLOOKUP_PROCESS_BATCHLIMIT "fetchLookupProcessBatchLimit" // Maximum number of fetch lookups on queue before passing to a processor (default = 10000)
#define THOROPT_REMOTE_KEYED_LOOKUP   "remoteKeyedLookup"       // Send key request to remote node unless part is local                          (default = true)
#define THOROPT_REMOTE_KEYED_FETCH    "remoteKeyedFetch"        // Send fetch request to remote node unless part is local                        (default = true)
#define THOROPT_FORCE_REMOTE_KEYED_LOOKUP "forceRemoteKeyedLookup" // force all keyed lookups, even where part local to be sent as if remote     (default = false)
#define THOROPT_FORCE_REMOTE_KEYED_FETCH "forceRemoteKeyedFetch" // force all keyed fetches, even where part local to be sent as if remote       (default = false)
#define THOROPT_KEYLOOKUP_MAX_LOCAL_HANDLERS "maxLocalHandlers" // maximum number of handlers dealing with local parts                           (default = 10)
#define THOROPT_KEYLOOKUP_MAX_REMOTE_HANDLERS "maxRemoteHandlers" // maximum number of handlers per remote slave                                 (default = 2)
#define THOROPT_KEYLOOKUP_MAX_FETCH_LOCAL_HANDLERS "maxLocalFetchHandlers" // maximum number of fetch handlers dealing with local parts          (default = 10)
#define THOROPT_KEYLOOKUP_MAX_FETCH_REMOTE_HANDLERS "maxRemoteFetchHandlers" // maximum number of fetch handlers per remote slave                (default = 2)
#define THOROPT_KEYLOOKUP_COMPRESS_MESSAGES "keyedJoinCompressMsgs" // compress key and fetch request messages                                   (default = true)
#define THOROPT_FORCE_REMOTE_DISABLED "forceRemoteDisabled"     // disable remote (via dafilesrv) reads (NB: takes precedence over forceRemoteRead) (default = false)
#define THOROPT_FORCE_REMOTE_READ     "forceRemoteRead"         // force remote (via dafilesrv) read (NB: takes precedence over environment.conf setting) (default = false)
#define THOROPT_ACTINIT_WAITTIME_MINS "actInitWaitTimeMins"     // max time to wait for slave activity initialization message from master
#define THOROPT_MAXLFN_BLOCKTIME_MINS "maxLfnBlockTimeMins"     // max time permitted to be blocked on a DFS logical file operation.
#define THOROPT_VALIDATE_FILE_TYPE    "validateFileType"        // validate file type compatibility, e.g. if on fire error if XML reading CSV    (default = true)
#define THOROPT_MIN_REMOTE_CQ_INDEX_SIZE_MB "minRemoteCQIndexSizeMb" // minimum size of index file to enable server side handling                (default = 0, meaning use heuristic to determin)

#define INITIAL_SELFJOIN_MATCH_WARNING_LEVEL 20000  // max of row matches before selfjoin emits warning

#define THOR_SEM_RETRY_TIMEOUT 2

// Logging
extern graph_decl const LogMsgJobInfo thorJob;

enum ThorExceptionAction { tea_null, tea_warning, tea_abort, tea_shutdown };

enum RegistryCode:unsigned { rc_register, rc_deregister };

#define createThorRow(size)         malloc(size)
#define destroyThorRow(ptr)         free(ptr)
#define reallocThorRow(ptr, size)   realloc(ptr, size)


//statistics gathered by the different activities
extern graph_decl const StatisticsMapping spillStatistics;
extern graph_decl const StatisticsMapping basicActivityStatistics;
extern graph_decl const StatisticsMapping groupActivityStatistics;
extern graph_decl const StatisticsMapping hashJoinActivityStatistics;
extern graph_decl const StatisticsMapping indexReadActivityStatistics;
extern graph_decl const StatisticsMapping indexWriteActivityStatistics;
extern graph_decl const StatisticsMapping joinActivityStatistics;
extern graph_decl const StatisticsMapping keyedJoinActivityStatistics;
extern graph_decl const StatisticsMapping lookupJoinActivityStatistics;
extern graph_decl const StatisticsMapping loopActivityStatistics;
extern graph_decl const StatisticsMapping diskReadActivityStatistics;
extern graph_decl const StatisticsMapping diskWriteActivityStatistics;
extern graph_decl const StatisticsMapping sortActivityStatistics;

extern graph_decl const StatisticsMapping graphStatistics;


class BooleanOnOff
{
    bool &tf;
public:
    inline BooleanOnOff(bool &_tf) : tf(_tf) { tf = true; }
    inline ~BooleanOnOff() { tf = false; }
};

class CReplyCancelHandler
{
    ICommunicator *comm;
    mptag_t mpTag;
    bool cancelled;
    SpinLock lock;

    void clear()
    {
        mpTag = TAG_NULL;
        comm = NULL;
    }
    void clearLock()
    {
        SpinBlock b(lock);
        clear();
    }
public:
    CReplyCancelHandler()
    {
        reset();
    }
    bool isCancelled() const { return cancelled; }
    void reset()
    {
        clear();
        cancelled = false;
    }
    void cancel(rank_t rank)
    {
        ICommunicator *_comm = NULL;
        mptag_t _mpTag = TAG_NULL;
        {
            SpinBlock b(lock);
            if (cancelled)
                return;
            cancelled = true;
            if (TAG_NULL == mpTag)
                return;
            // stash in case other thread waiting finishing send.
            _comm = comm;
            _mpTag = mpTag;
        }
        _comm->cancel(rank, _mpTag);
    }
    bool recv(ICommunicator &_comm, CMessageBuffer &mb, rank_t rank, const mptag_t &_mpTag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER)
    {
        bool ret=false;
        {
            SpinBlock b(lock);
            if (cancelled)
                return false;
            comm = &_comm;
            mpTag = _mpTag; // receiving
        }
        try
        {
            ret = _comm.recv(mb, rank, _mpTag, sender, timeout);
        }
        catch (IException *)
        {
            clearLock();
            throw;
        }
        clearLock();
        return ret;
    }
};


class graph_decl CTimeoutTrigger : public CInterface, implements IThreaded
{
    bool running;
    Semaphore todo;
    CriticalSection crit;
    unsigned timeout;
    StringAttr description;
    CThreaded threaded;
protected:
    Owned<IException> exception;

public:
    CTimeoutTrigger(unsigned _timeout, const char *_description) : timeout(_timeout), description(_description), threaded("TimeoutTrigger")
    {
        running = (timeout!=0);
        threaded.init(this);
    }
    virtual ~CTimeoutTrigger()
    {
        stop();
        threaded.join();
    }
    virtual void threadmain() override
    {
        while (running)
        {
            todo.wait(1000);
            CriticalBlock block(crit);
            if (exception.get())
            {
                { CriticalUnblock b(crit);
                    if (todo.wait(timeout*1000))
                    { // if signalled during timeout period, wait full timeout
                        if (running)
                            todo.wait(timeout*1000);
                    }
                }
                if (!running) break;
                if (exception.get())
                    if (action())
                        break;
            }
        }
    }
    void stop() { running = false; todo.signal(); }
    void inform(IException *e)
    {
        LOG(MCdebugProgress, thorJob, "INFORM [%s]", description.get());
        CriticalBlock block(crit);
        if (exception.get())
            e->Release();
        else
        {
            exception.setown(e);
            todo.signal();
        }
    }
    IException *clear()
    {
        CriticalBlock block(crit);
        IException *e = exception.getClear();
        if (e)
            LOG(MCdebugProgress, thorJob, "CLEARING TIMEOUT [%s]", description.get());
        todo.signal();
        return e;
    }
    virtual bool action() = 0;
};

// simple class which takes ownership of the underlying file and deletes it on destruction
class graph_decl CFileOwner : public CSimpleInterface, implements IInterface
{
    OwnedIFile iFile;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CFileOwner(IFile *_iFile) : iFile(_iFile)
    {
    }
    ~CFileOwner()
    {
        iFile->remove();
    }
    IFile &queryIFile() const { return *iFile; }
};

// stream wrapper, that takes ownership of a CFileOwner
class graph_decl CStreamFileOwner : public CSimpleInterfaceOf<IExtRowStream>
{
    Linked<CFileOwner> fileOwner;
    IExtRowStream *stream;
public:
    CStreamFileOwner(CFileOwner *_fileOwner, IExtRowStream *_stream) : fileOwner(_fileOwner)
    {
        stream = LINK(_stream);
    }
    ~CStreamFileOwner()
    {
        stream->Release();
    }
// IExtRowStream
    virtual const void *nextRow() override { return stream->nextRow(); }
    virtual void stop() override { stream->stop(NULL); }
    virtual offset_t getOffset() const override { return stream->getOffset(); }
    virtual offset_t getLastRowOffset() const override { return stream->getLastRowOffset(); }
    virtual unsigned __int64 queryProgress() const override { return stream->queryProgress(); }
    virtual void stop(CRC32 *crcout) override { stream->stop(crcout); }
    virtual const byte *prefetchRow() override { return stream->prefetchRow(); }
    virtual void prefetchDone() override { stream->prefetchDone(); }
    virtual void reinit(offset_t offset, offset_t len, unsigned __int64 maxRows) override
    {
        stream->reinit(offset, len, maxRows);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        return stream->getStatistic(kind);
    }
    virtual void setFilters(IConstArrayOf<IFieldFilter> &filters) override
    {
        return stream->setFilters(filters);
    }
};


#define DEFAULT_THORMASTERPORT 20000
#define DEFAULT_THORSLAVEPORT 20100
#define DEFAULT_SLAVEPORTINC 20
#define DEFAULT_QUERYSO_LIMIT 10
#define DEFAULT_LINGER_SECS 10

class graph_decl CFifoFileCache : public CSimpleInterface
{
    unsigned limit;
    StringArray files;
    void deleteFile(IFile &ifile);

public:
    void init(const char *cacheDir, unsigned _limit, const char *pattern);
    void add(const char *filename);
    bool isAvailable(const char *filename);
};

interface graph_decl IBarrierException : extends IException {};
extern graph_decl IBarrierException *createBarrierAbortException();

interface graph_decl IThorException : extends IException
{
    virtual ThorExceptionAction queryAction() const = 0;
    virtual ThorActivityKind queryActivityKind() const = 0;
    virtual activity_id queryActivityId() const = 0;
    virtual const char *queryGraphName() const = 0;
    virtual graph_id queryGraphId() const = 0;
    virtual const char *queryJobId() const = 0;
    virtual unsigned querySlave() const = 0;
    virtual void getAssert(StringAttr &file, unsigned &line, unsigned &column) const = 0;
    virtual const char *queryOrigin() const = 0;
    virtual ErrorSeverity querySeverity() const = 0;
    virtual const char *queryMessage() const = 0;
    virtual MemoryBuffer &queryData() = 0;
    virtual IException *queryOriginalException() const = 0;
    virtual void setAction(ThorExceptionAction _action) = 0;
    virtual void setActivityKind(ThorActivityKind _kind) = 0;
    virtual void setActivityId(activity_id id) = 0;
    virtual void setGraphInfo(const char *graphName, graph_id id) = 0;
    virtual void setJobId(const char *jobId) = 0;
    virtual void setAudience(MessageAudience audience) = 0;
    virtual void setSlave(unsigned slave) = 0;
    virtual void setMessage(const char *msg) = 0;
    virtual void setAssert(const char *file, unsigned line, unsigned column) = 0;
    virtual void setOrigin(const char *origin) = 0;
    virtual void setSeverity(ErrorSeverity severity) = 0;
    virtual void setOriginalException(IException *e) = 0;
};

class CGraphElementBase;
class CActivityBase;
class CGraphBase;
interface IRemoteConnection;
enum ActLogEnum { thorlog_null=0,thorlog_ecl=1,thorlog_all=2 };

extern graph_decl StringBuffer &ActPrintLogArgsPrep(StringBuffer &res, const CGraphElementBase *container, const ActLogEnum flags, const char *format, va_list args) __attribute__((format(printf,4,0)));
extern graph_decl void ActPrintLogEx(const CGraphElementBase *container, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...) __attribute__((format(printf, 4, 5)));
extern graph_decl void ActPrintLogArgs(const CGraphElementBase *container, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args) __attribute__((format(printf,4,0)));
extern graph_decl void ActPrintLogArgs(const CGraphElementBase *container, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args) __attribute__((format(printf,5,0)));
extern graph_decl void ActPrintLog(const CActivityBase *activity, const char *format, ...) __attribute__((format(printf, 2, 3)));
extern graph_decl void ActPrintLog(const CActivityBase *activity, unsigned traceLevel, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl void ActPrintLog(const CActivityBase *activity, IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl void ActPrintLog(const CActivityBase *activity, IException *e);

inline void ActPrintLog(const CGraphElementBase *container, const char *format, ...) __attribute__((format(printf, 2, 3)));
inline void ActPrintLog(const CGraphElementBase *container, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(container, thorlog_ecl, MCdebugProgress, format, args);
    va_end(args);
}
inline void ActPrintLog(const CGraphElementBase *container, unsigned traceLevel, const char *format, ...) __attribute__((format(printf, 3, 4)));
inline void ActPrintLog(const CGraphElementBase *container, unsigned traceLevel, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(container, thorlog_ecl, MCdebugProgress(traceLevel), format, args);
    va_end(args);
}
inline void ActPrintLogEx(const CGraphElementBase *container, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...) __attribute__((format(printf, 5, 6)));
inline void ActPrintLogEx(const CGraphElementBase *container, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(container, e, flags, logCat, format, args);
    va_end(args);
}
inline void ActPrintLog(const CGraphElementBase *container, IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
inline void ActPrintLog(const CGraphElementBase *container, IException *e, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    ActPrintLogArgs(container, e, thorlog_null, MCexception(e, MSGCLS_error), format, args);
    va_end(args);
}
inline void ActPrintLog(const CGraphElementBase *container, IException *e)
{
    ActPrintLogEx(container, e, thorlog_null, MCexception(e, MSGCLS_error), "%s", "");
}
extern graph_decl void GraphPrintLogArgsPrep(StringBuffer &res, CGraphBase *graph, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args) __attribute__((format(printf,5,0)));
extern graph_decl void GraphPrintLogArgs(CGraphBase *graph, IException *e, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args) __attribute__((format(printf,5,0)));
extern graph_decl void GraphPrintLogArgs(CGraphBase *graph, const ActLogEnum flags, const LogMsgCategory &logCat, const char *format, va_list args) __attribute__((format(printf,4,0)));
extern graph_decl void GraphPrintLog(CGraphBase *graph, IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));

inline void GraphPrintLogEx(CGraphBase *graph, ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...) __attribute__((format(printf, 4, 5)));
inline void GraphPrintLogEx(CGraphBase *graph, ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    GraphPrintLogArgs(graph, flags, logCat, format, args);
    va_end(args);
}
inline void GraphPrintLogEx(CGraphBase *graph, IException *e, ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...) __attribute__((format(printf, 5, 6)));
inline void GraphPrintLogEx(CGraphBase *graph, IException *e, ActLogEnum flags, const LogMsgCategory &logCat, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    GraphPrintLogArgs(graph, e, flags, logCat, format, args);
    va_end(args);
}
inline void GraphPrintLog(CGraphBase *graph, const char *format, ...) __attribute__((format(printf, 2, 3)));
inline void GraphPrintLog(CGraphBase *graph, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    GraphPrintLogArgs(graph, thorlog_null, MCdebugProgress, format, args);
    va_end(args);
}

inline void GraphPrintLog(CGraphBase *graph, unsigned traceLevel, const char *format, ...) __attribute__((format(printf, 3, 4)));
inline void GraphPrintLog(CGraphBase *graph, unsigned traceLevel, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    GraphPrintLogArgs(graph, thorlog_null, MCdebugInfo(traceLevel), format, args);
    va_end(args);
}

extern graph_decl IThorException *MakeActivityException(CActivityBase *activity, int code, const char *_format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityException(CActivityBase *activity, IException *e, const char *xtra, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityException(CActivityBase *activity, IException *e);
extern graph_decl IThorException *MakeActivityWarning(CActivityBase *activity, int code, const char *_format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityWarning(CActivityBase *activity, IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityException(CGraphElementBase *activity, int code, const char *_format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityException(CGraphElementBase *activity, IException *e, const char *xtra, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityException(CGraphElementBase *activity, IException *e);
extern graph_decl IThorException *MakeActivityWarning(CGraphElementBase *activity, int code, const char *_format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeActivityWarning(CGraphElementBase *activity, IException *e, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeGraphException(CGraphBase *graph, int code, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeGraphException(CGraphBase *graph, IException *e);
extern graph_decl IThorException *MakeThorException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
extern graph_decl IThorException *MakeThorException(IException *e);
extern graph_decl IThorException *MakeThorAudienceException(LogMsgAudience audience, int code, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *MakeThorOperatorException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
extern graph_decl IThorException *MakeThorFatal(IException *e, int code, const char *format, ...) __attribute__((format(printf, 3, 4)));
extern graph_decl IThorException *ThorWrapException(IException *e, const char *msg, ...) __attribute__((format(printf, 2, 3)));
extern graph_decl void setExceptionActivityInfo(CGraphElementBase &container, IThorException *e);

extern graph_decl void GetTempName(StringBuffer &name, const char *prefix=NULL,bool altdisk=false);
extern graph_decl void SetTempDir(unsigned slaveNum, const char *name, const char *tempPrefix, bool clear);
extern graph_decl void ClearDir(const char *dir);
extern graph_decl void ClearTempDirs();
extern graph_decl const char *queryTempDir(bool altdisk=false);  
extern graph_decl void loadCmdProp(IPropertyTree *tree, const char *cmdProp);

extern graph_decl void ensureDirectoryForFile(const char *fName);
extern graph_decl void reportExceptionToWorkunit(IConstWorkUnit &workunit,IException *e, ErrorSeverity severity=SeverityWarning);
extern graph_decl void reportExceptionToWorkunitCheckIgnore(IConstWorkUnit &workunit, IException *e, ErrorSeverity severity=SeverityWarning);


extern graph_decl Owned<IPropertyTree> globals;
extern graph_decl mptag_t masterSlaveMpTag;
extern graph_decl mptag_t kjServiceMpTag;
enum SlaveMsgTypes:unsigned { smt_errorMsg=1, smt_initGraphReq, smt_initActDataReq, smt_dataReq, smt_getPhysicalName, smt_getFileOffset, smt_actMsg, smt_getresult };

extern graph_decl StringBuffer &getCompoundQueryName(StringBuffer &compoundName, const char *queryName, unsigned version);

extern graph_decl void setupCluster(INode *masterNode, IGroup *processGroup, unsigned channelsPerSlave, unsigned portBase, unsigned portInc);
extern graph_decl void setClusterGroup(INode *masterNode, IGroup *group, unsigned slavesPerNode, unsigned channelsPerSlave, unsigned portBase, unsigned portInc);
extern graph_decl bool clusterInitialized();
extern graph_decl INode &queryMasterNode();
extern graph_decl IGroup &queryNodeGroup();
extern graph_decl IGroup &queryProcessGroup();
extern graph_decl ICommunicator &queryNodeComm();
extern graph_decl IGroup &queryClusterGroup();
extern graph_decl IGroup &querySlaveGroup();
extern graph_decl IGroup &queryDfsGroup();
extern graph_decl unsigned queryClusterWidth();
extern graph_decl unsigned queryNodeClusterWidth();

extern graph_decl mptag_t allocateClusterMPTag();     // should probably move into so used by master only
extern graph_decl void freeClusterMPTag(mptag_t tag); // ""

extern graph_decl IThorException *deserializeThorException(MemoryBuffer &in); 
void graph_decl serializeThorException(IException *e, MemoryBuffer &out); 

class CActivityBase;
interface IPartDescriptor;
extern graph_decl bool getBestFilePart(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path, IExceptionHandler *eHandler = NULL);
extern graph_decl StringBuffer &getFilePartLocations(IPartDescriptor &partDesc, StringBuffer &locations);
extern graph_decl StringBuffer &getPartFilename(IPartDescriptor &partDesc, unsigned copy, StringBuffer &filePath, bool localMount=false);

extern graph_decl IOutputMetaData *createFixedSizeMetaData(size32_t sz);


interface IRowServer : extends IInterface
{
    virtual void stop() = 0;
};
extern graph_decl IRowStream *createRowStreamFromNode(CActivityBase &activity, unsigned node, ICommunicator &comm, mptag_t mpTag, const bool &abortSoon);
extern graph_decl IRowServer *createRowServer(CActivityBase *activity, IRowStream *seq, ICommunicator &comm, mptag_t mpTag);

interface IEngineRowStream;
extern graph_decl IEngineRowStream *createUngroupStream(IRowStream *input);

interface IThorRowInterfaces;
extern graph_decl void sendInChunks(ICommunicator &comm, rank_t dst, mptag_t mpTag, IRowStream *input, IThorRowInterfaces *rowIf);

extern graph_decl void logDiskSpace();

class CJobBase;
extern graph_decl IPerfMonHook *createThorMemStatsPerfMonHook(CJobBase &job, int minLevel, IPerfMonHook *chain=NULL); // for passing to jdebug startPerformanceMonitor

extern graph_decl bool isOOMException(IException *e);
extern graph_decl IThorException *checkAndCreateOOMContextException(CActivityBase *activity, IException *e, const char *msg, rowcount_t numRows, IOutputMetaData *meta, const void *row);

extern graph_decl RecordTranslationMode getTranslationMode(CActivityBase &activity);
extern graph_decl void getLayoutTranslations(IConstPointerArrayOf<ITranslator> &translators, const char *fname, IArrayOf<IPartDescriptor> &partDescriptors, RecordTranslationMode translationMode, unsigned expectedFormatCrc, IOutputMetaData *expectedFormat, unsigned projectedFormatCrc, IOutputMetaData *projectedFormat);
extern graph_decl const ITranslator *getLayoutTranslation(const char *fname, IPartDescriptor &partDesc, RecordTranslationMode translationMode, unsigned expectedFormatCrc, IOutputMetaData *expectedFormat, unsigned projectedFormatCrc, IOutputMetaData *projectedFormat);
extern graph_decl bool isRemoteReadCandidate(const CActivityBase &activity, const RemoteFilename &rfn);

extern graph_decl void checkAndDumpAbortInfo(const char *cmd);

extern graph_decl void checkFileType(CActivityBase *activity, IDistributedFile *file, const char *expectedType, bool throwException);

template <class T>
inline void readUnderlyingType(MemoryBuffer &mb, T &v)
{
    mb.read(reinterpret_cast<typename std::underlying_type<T>::type &> (v));
}

constexpr unsigned thorDetailedLogLevel = 200;
constexpr LogMsgCategory MCthorDetailedDebugInfo(MCdebugInfo(thorDetailedLogLevel));


#endif

