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

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jmd5.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "jqueue.tpp"
#include "dautils.hpp"

#include "keydiff.hpp"

#include "udptopo.hpp"
#include "ccd.hpp"
#include "ccdfile.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdsnmp.hpp"
#include "rmtfile.hpp"
#include "ccdqueue.ipp"
#if defined(__linux__) || defined(__APPLE__)
#include <sys/mman.h>
#endif
#if defined (__linux__)
#include <sys/syscall.h>
#include "ioprio.h"
#endif
#include "thorcommon.hpp"
#include "eclhelper_dyn.hpp"
#include "rtldynfield.hpp"

atomic_t numFilesOpen[2];

#define MAX_READ_RETRIES 2

#ifdef _DEBUG
//#define FAIL_20_READ
//#define FAIL_20_OPEN
#endif

// We point unopened files at a FailingIO object, which avoids having to test for NULL on every access

class DECL_EXCEPTION NotYetOpenException : implements IException, public CInterface
{
public: 
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return 0; }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return msg.append("not yet open"); }
    virtual MessageAudience errorAudience() const { return MSGAUD_programmer; }
};

class CFailingFileIO : implements IFileIO, public CInterface
{
#define THROWNOTOPEN throw new NotYetOpenException()

public:
    IMPLEMENT_IINTERFACE;
    virtual size32_t read(offset_t pos, size32_t len, void * data) { THROWNOTOPEN; }
    virtual offset_t size() { THROWNOTOPEN; }
    virtual void flush() { THROWNOTOPEN; }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) { THROWNOTOPEN; }
    virtual void setSize(offset_t size) { UNIMPLEMENTED; }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { UNIMPLEMENTED; return 0; }
    virtual void close() { }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return 0; }
} failure;

class CRoxieLazyFileIO : implements ILazyFileIO, implements IDelayedFile, public CInterface
{
protected:
    IArrayOf<IFile> sources;
    Owned<IFile> logical;
    Owned<IFileIO> current;
    Owned<IMemoryMappedFile> mmapped;
    mutable CriticalSection crit;
    offset_t fileSize;
    unsigned currentIdx;
    unsigned lastAccess;
    CDateTime fileDate;
    bool copying = false;
    bool isCompressed = false;
    bool remote = false;
    IRoxieFileCache *cached = nullptr;
    unsigned fileIdx = 0;
    unsigned crc = 0;
    CRuntimeStatisticCollection fileStats;

#ifdef FAIL_20_READ
    unsigned readCount;
#endif

public:
    IMPLEMENT_IINTERFACE;

    CRoxieLazyFileIO(IFile *_logical, offset_t size, const CDateTime &_date, bool _isCompressed, unsigned _crc)
        : logical(_logical), fileSize(size), isCompressed(_isCompressed), crc(_crc), fileStats(diskLocalStatistics)
    {
        fileDate.set(_date);
        currentIdx = 0;
        current.set(&failure);
#ifdef FAIL_20_READ
        readCount = 0;
#endif
        lastAccess = msTick();
    }
    
    ~CRoxieLazyFileIO()
    {
        setFailure(); // ensures the open file count properly maintained
    }

    virtual void beforeDispose()
    {
        if (cached)
            cached->removeCache(this);
    }

    virtual unsigned getFileIdx() const override
    {
        return fileIdx;
    }

    virtual unsigned getCrc() const override
    {
        return crc;
    }

    void setCache(IRoxieFileCache *cache, unsigned _fileIdx)
    {
        assertex(!cached);
        cached = cache;
        fileIdx = _fileIdx;
    }

    void removeCache(const IRoxieFileCache *cache)
    {
        assertex(cached==cache);
        cached = NULL;
    }

    inline void setRemote(bool _remote) { remote = _remote; }

    virtual void setCopying(bool _copying)
    {
        CriticalBlock b(crit);
        if (remote && currentIdx)
        {
            // The current location is not our preferred location. Recheck whether we can now access our preferred location
            setFailure();
            currentIdx = 0;
            _checkOpen();
        }
        copying = _copying; 
    }

    virtual bool isCopying() const
    {
        CriticalBlock b(crit);
        return copying; 
    }

    virtual bool isOpen() const 
    {
        CriticalBlock b(crit);
        return current.get() != &failure; 
    }

    virtual unsigned getLastAccessed() const
    {
        CriticalBlock b(crit);
        return lastAccess;
    }

    virtual void close()
    {
        CriticalBlock b(crit);
        setFailure();
    }

    virtual bool isRemote()
    {
        CriticalBlock b(crit);
        return remote;
    }

    void setFailure()
    {
        try
        {
            if (current.get()==&failure)
                return;
            atomic_dec(&numFilesOpen[remote]);
            mergeStats(fileStats, current);
            current.set(&failure); 
        }
        catch (IException *E) 
        {
            if (traceLevel > 5)
            {
                StringBuffer s;
                DBGLOG("setFailure ignoring exception %s from IFileIO close", E->errorMessage(s).str());
            }
            E->Release(); 
        }
    }

    void checkOpen()
    {
        CriticalBlock b(crit);
        _checkOpen();
    }

    IFileIO *getCheckOpen(unsigned &activeIdx)
    {
        CriticalBlock b(crit);
        _checkOpen();
        activeIdx = currentIdx;
        return LINK(current);
    }

    void _checkOpen()
    {
        if (current.get() == &failure)
        {
            StringBuffer filesTried;
            unsigned tries = 0;
            bool firstTime = true;
            RoxieFileStatus fileStatus = FileNotFound;

            for (;;)
            {
                if (currentIdx >= sources.length())
                    currentIdx = 0;
                if (tries==sources.length())
                {
                    if (firstTime)  // if first time - reset and try again
                    {
                        firstTime = false;
                        tries = 0;
                    }
                    else
                        throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Failed to open file %s at any of the following remote locations %s", logical->queryFilename(), filesTried.str()); // operations doesn't want a trap
                }
                const char *sourceName = sources.item(currentIdx).queryFilename();
                if (traceLevel > 10)
                    DBGLOG("Trying to open %s", sourceName);
                try
                {
#ifdef FAIL_20_OPEN
                    openCount++;
                    if ((openCount % 5) == 0)
                        throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Pretending to fail on an open");
#endif
                    IFile *f = &sources.item(currentIdx);
                    fileStatus = queryFileCache().fileUpToDate(f, fileSize, fileDate, isCompressed, false);
                    if (fileStatus == FileIsValid)
                    {
                        if (isCompressed)
                            current.setown(createCompressedFileReader(f));
                        else
                            current.setown(f->open(IFOread));
                        if (current)
                        {
                            if (traceLevel > 5)
                                DBGLOG("Opening %s", sourceName);
                            disconnectRemoteIoOnExit(current);
                            break;
                        }
    //                  throwUnexpected();  - try another location if this one has the wrong version of the file
                    }
                    disconnectRemoteFile(f);
                }
                catch (IException *E)
                {
                    E->Release();
                }
                currentIdx++;
                tries++;
                if (!firstTime)  // log error on last attempt for each file name - it will have the "best" error condition
                {
                    filesTried.appendf(" %s", sourceName);  // only need to build this list once

                    switch (fileStatus)
                    {
                        case FileNotFound:
                            filesTried.append(": FileNotFound");
                            break;

                        case FileSizeMismatch:
                            filesTried.append(": FileSizeMismatch");
                            break;

                        case FileDateMismatch:
                            filesTried.append(": FileDateMismatch");
                            break;
                    }
                }
            }
            lastAccess = msTick();
            atomic_inc(&numFilesOpen[remote]);
            if ((unsigned) atomic_read(&numFilesOpen[remote]) > maxFilesOpen[remote])
                queryFileCache().closeExpired(remote); // NOTE - this does not actually do the closing of expired files (which could deadlock, or could close the just opened file if we unlocked crit)
        }
    }

    virtual void addSource(IFile *newSource)
    {
        if (newSource)
        {
            if (traceLevel > 10)
                DBGLOG("Adding information for location %s for %s", newSource->queryFilename(), logical->queryFilename());
            CriticalBlock b(crit);
            sources.append(*newSource);
        }
    }

    virtual size32_t read(offset_t pos, size32_t len, void * data) 
    {
        unsigned activeIdx;
        Owned<IFileIO> active = getCheckOpen(activeIdx);
        unsigned tries = 0;
        for (;;)
        {
            try
            {
                size32_t ret = active->read(pos, len, data);
                lastAccess = msTick();
                if (cached && !remote)
                    cached->noteRead(fileIdx, pos, ret);
                return ret;

            }
            catch (NotYetOpenException *E)
            {
                E->Release();
            }
            catch (IException *E)
            {
                EXCLOG(MCoperatorError, E, "Read error");
                E->Release();
                OERRLOG("Failed to read length %d offset %" I64F "x file %s", len, pos, sources.item(activeIdx).queryFilename());
                {
                    CriticalBlock b(crit);
                    if (currentIdx == activeIdx)
                    {
                        currentIdx = activeIdx+1;
                        setFailure();
                    }
                }
            }
            active.setown(getCheckOpen(activeIdx));
            tries++;
            if (tries == MAX_READ_RETRIES)
                throw MakeStringException(ROXIE_FILE_ERROR, "Failed to read length %d offset %" I64F "x file %s after %d attempts", len, pos, sources.item(activeIdx).queryFilename(), tries);
        }
    }

    virtual void flush()
    {
        Linked<IFileIO> active;
        {
            CriticalBlock b(crit);
            active.set(current);
        }
        if (active.get() != &failure)
            active->flush();
    }

    virtual offset_t size() 
    { 
        unsigned activeIdx;
        Owned<IFileIO> active = getCheckOpen(activeIdx);
        lastAccess = msTick();
        return active->size();
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        unsigned __int64 v = fileStats.getStatisticValue(kind);
        CriticalBlock b(crit); // don't bother with linking current and performing getStatistic outside of crit, because getStatistic is very quick
        return v + current->getStatistic(kind);
    }

    virtual size32_t write(offset_t pos, size32_t len, const void * data) { throwUnexpected(); }
    virtual void setSize(offset_t size) { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { throwUnexpected(); return 0; }

    virtual const char *queryFilename() { return logical->queryFilename(); }
    virtual bool isAliveAndLink() const { return CInterface::isAliveAndLink(); }

    virtual IMemoryMappedFile *getMappedFile() override
    {
        CriticalBlock b(crit);
        if (mmapped)
            return mmapped.getLink();
        if (!remote)
        {
            mmapped.setown(logical->openMemoryMapped());
            return mmapped.getLink();
        }
        return nullptr;
    }

    virtual IFileIO *getFileIO() override
    {
        return LINK(this);
    }


    virtual bool createHardFileLink()
    {
        unsigned tries = 0;
        for (;;)
        {
            StringBuffer filesTried;
            if (currentIdx >= sources.length())
                currentIdx = 0;
            if (tries==sources.length())
                return false;
            const char *sourceName = sources.item(currentIdx).queryFilename();
            filesTried.appendf(" %s", sourceName);
            try
            {
                if (queryFileCache().fileUpToDate(&sources.item(currentIdx), fileSize, fileDate, isCompressed) == FileIsValid)
                {
                    StringBuffer source_drive;
                    splitFilename(sourceName, &source_drive, NULL, NULL, NULL);

                    StringBuffer query_drive;
                    splitFilename(logical->queryFilename(), &query_drive, NULL, NULL, NULL);

                    // only try to create link if on the same drive
                    if ( (stricmp(query_drive.str(), source_drive.str()) == 0))
                    {
                        try
                        {
                            DBGLOG("Trying to create Hard Link for %s", sourceName);
                            createHardLink(logical->queryFilename(), sourceName);
                            current.setown(sources.item(currentIdx).open(IFOread));
                            return true;
                        }
                        catch(IException *E)
                        {
                            StringBuffer err;
                            OERRLOG("HARD LINK ERROR %s", E->errorMessage(err).str());
                            E->Release();
                        }
                    }
                }
            }
            catch (IException *E)
            {
                E->Release();
            }
            currentIdx++;
            tries++;
        }

        DBGLOG("Could not create any hard links for %s", logical->queryFilename());
        return false;  // if we get here - no hardlink
    }

    void copyComplete()
    {
        {
            CriticalBlock b(crit);
            setFailure(); // lazyOpen will then reopen it...
            currentIdx = 0;
            remote = false;
            copying = false;
            sources.kill();
            sources.add(*logical.getLink(), 0);
            if (!lazyOpen)
                _checkOpen();
        }
    }

    virtual IFile *querySource() 
    {
        CriticalBlock b(crit);
        _checkOpen();
        return &sources.item(currentIdx); 
    };
    virtual IFile *queryTarget() { return logical; }

    virtual offset_t getSize() { return fileSize; }
    virtual CDateTime *queryDateTime() { return &fileDate; }

    static int compareAccess(IInterface * const *L, IInterface * const *R)
    {
        ILazyFileIO *LL = (ILazyFileIO *) *L;
        ILazyFileIO *RR = (ILazyFileIO *) *R;
        return LL->getLastAccessed() - RR->getLastAccessed();
    }
};

//----------------------------------------------------------------------------------------------

static IPartDescriptor *queryMatchingRemotePart(IPartDescriptor *pdesc, IFileDescriptor *remoteFDesc, unsigned int partNum)
{
    if (!remoteFDesc)
        return NULL;
    IPartDescriptor *remotePDesc = remoteFDesc->queryPart(partNum);
    if (!remotePDesc)
        return NULL;
    unsigned int crc, remoteCrc;
    if (!pdesc || !pdesc->getCrc(crc)) //local crc not available, never DFS copied?
        return remotePDesc;
    if (remotePDesc->getCrc(remoteCrc) && remoteCrc==crc)
        return remotePDesc;
    return NULL;
}

static int getClusterPriority(const char *clusterName)
{
    assertex(preferredClusters);
    int *priority = preferredClusters->getValue(clusterName);
    return priority ? *priority : 100;
}

static void appendRemoteLocations(IPartDescriptor *pdesc, StringArray &locations, const char *localFileName, const char *fromCluster, bool includeFromCluster)
{
    IFileDescriptor &fdesc = pdesc->queryOwner();
    unsigned numCopies = pdesc->numCopies();
    unsigned lastClusterNo = (unsigned) -1;
    unsigned numThisCluster = 0;
    unsigned initialSize = locations.length();
    int priority = 0;
    IntArray priorities;
    for (unsigned copy = 0; copy < numCopies; copy++)
    {
        unsigned clusterNo = pdesc->copyClusterNum(copy);
        StringBuffer clusterName;
        fdesc.getClusterGroupName(clusterNo, clusterName);
        if (fromCluster && *fromCluster)
        {
            bool matches = strieq(clusterName.str(), fromCluster);
            if (matches!=includeFromCluster)
                continue;
        }
        RemoteFilename r;
        pdesc->getFilename(copy,r);
        StringBuffer path;
        r.getRemotePath(path);
        if (localFileName && r.isLocal())
        {
            StringBuffer l;
            r.getLocalPath(l);
            if (streq(l, localFileName))
                continue; // don't add ourself
        }
        if (clusterNo == lastClusterNo)
        {
            numThisCluster++;
            if (numThisCluster > 2)  // Don't add more than 2 from one cluster
                continue;
        }
        else
        {
            numThisCluster = 1;
            lastClusterNo = clusterNo;
            if (preferredClusters)
            {
                priority = getClusterPriority(clusterName);
            }
            else
                priority = copy;
        }
        if (priority >= 0)
        {
            ForEachItemIn(idx, priorities)
            {
                if (priorities.item(idx) < priority)
                    break;
            }
            priorities.add(priority, idx);
            locations.add(path.str(), idx+initialSize);
        }
    }
}

//----------------------------------------------------------------------------------------------

typedef StringArray *StringArrayPtr;

// A circular buffer recording recent disk read operations that can be used to "prewarm" the cache

struct CacheInfoEntry
{
    union
    {
        struct
        {
            bool diskCache : 1;   // false means it's in the jhtree cache, true means it's only in OS disk cache
            __uint64 page: 39;
            unsigned file: 24;
        } b;
        __uint64 u;
    };

    inline CacheInfoEntry() { u = 0; }
    inline CacheInfoEntry(unsigned _file, offset_t _pos, bool _diskCache)
    {
        b.file = _file;
        b.page = _pos >> pageBits;
        b.diskCache = _diskCache;
    }
    inline bool operator < ( const CacheInfoEntry &l) const { return u < l.u; }
    inline bool operator <= ( const CacheInfoEntry &l) const { return u <= l.u; }
    inline void operator++ () { b.page++; }

    static constexpr unsigned pageBits = 13;  // 8k 'pages'
};

class CacheReportingBuffer : public CInterfaceOf<ICacheInfoRecorder>
{
    // A circular buffer recording recent file activity. Note that noteRead() and clear() may be called from multiple threads
    // (other functions are assumed single-threaded) and that locking is kept to a minimum, even if it means information may be slightly inaccurate.
    CacheInfoEntry *recentReads = nullptr;
    std::atomic<unsigned> recentReadHead = {0};
    unsigned recentReadSize;

public:

    CacheReportingBuffer(offset_t trackSize)
    {
        recentReadSize = trackSize >> CacheInfoEntry::pageBits;
        if (traceLevel)
            DBGLOG("Creating CacheReportingBuffer with %d elements", recentReadSize);
        assertex(recentReadSize);
        recentReads = new CacheInfoEntry[recentReadSize];
        recentReadHead = 0;
    }
    CacheReportingBuffer(const CacheReportingBuffer &from)
    {
        // NOTE - from may be updated concurrently - we do not want to lock it
        // There are therefore races in here, but they do not matter (may result in very recent data being regarded as very old or vice versa).
        recentReadSize = from.recentReadSize;
        recentReadHead = from.recentReadHead.load(std::memory_order_relaxed);
        recentReads = new CacheInfoEntry[recentReadSize];
        memcpy(recentReads, from.recentReads, recentReadSize * sizeof(CacheInfoEntry));
    }

    ~CacheReportingBuffer()
    {
        delete [] recentReads;
    }

    void clear()
    {
        recentReadHead = 0;
    }

    void noteRead(unsigned fileIdx, offset_t pos, unsigned len, bool diskCache)
    {
        if (recentReads && len)
        {
            CacheInfoEntry start(fileIdx, pos, diskCache);
            CacheInfoEntry end(fileIdx, pos+len-1, diskCache);
            for(;start <= end; ++start)
            {
                recentReads[recentReadHead++ % recentReadSize] = start;
            }
        }
    }

    void sortAndDedup()
    {
        // NOTE: single-threaded
        unsigned sortSize;
        if (recentReadHead > recentReadSize)
            sortSize = recentReadSize;
        else
            sortSize = recentReadHead;
        std::sort(recentReads, recentReads + sortSize);
        CacheInfoEntry lastPos(-1,-1,false);
        unsigned dest = 0;
        for (unsigned idx = 0; idx < sortSize; idx++)
        {
            CacheInfoEntry pos = recentReads[idx];
            if (pos.b.file != lastPos.b.file || pos.b.page != lastPos.b.page)   // Ignore inNodeCache bit when deduping
            {
                recentReads[dest++] = pos;
                lastPos = pos;
            }
        }
        recentReadHead = dest;
    }

    void report(StringBuffer &ret, unsigned channel, const StringArray &cacheIndexes, const UnsignedShortArray &cacheIndexChannels)
    {
        // NOTE: single-threaded
        assertex(recentReadHead <= recentReadSize);  // Should have sorted and deduped before calling this
        unsigned lastFileIdx = (unsigned) -1;
        offset_t lastPage = (offset_t) -1;
        offset_t startRange = 0;
        bool lastDiskCache = false;
        bool includeFile = false;
        for (unsigned idx = 0; idx < recentReadHead; idx++)
        {
            CacheInfoEntry pos = recentReads[idx];
            if (pos.b.file != lastFileIdx)
            {
                if (includeFile)
                    appendRange(ret, startRange, lastPage, lastDiskCache).newline();
                lastFileIdx = pos.b.file;
                if (channel==(unsigned) -1 || cacheIndexChannels.item(lastFileIdx)==channel)
                {
                    ret.appendf("%u|%s|", cacheIndexChannels.item(lastFileIdx), cacheIndexes.item(lastFileIdx));
                    includeFile = true;
                }
                else
                    includeFile = false;
                startRange = pos.b.page;
            }
            else if ((pos.b.page == lastPage || pos.b.page == lastPage+1) && pos.b.diskCache == lastDiskCache)
            {
                // Still in current range
            }
            else
            {
                if (includeFile)
                    appendRange(ret, startRange, lastPage, lastDiskCache);
                startRange = pos.b.page;
            }
            lastPage = pos.b.page;
            lastDiskCache = pos.b.diskCache;
        }
        if (includeFile)
            appendRange(ret, startRange, lastPage, lastDiskCache).newline();
    }

    virtual void noteWarm(unsigned fileIdx, offset_t pos, unsigned len) override
    {
        noteRead(fileIdx, pos, len, false);
    }

private:
    static StringBuffer &appendRange(StringBuffer &ret, offset_t start, offset_t end, bool diskCache)
    {
        if (!diskCache)
            ret.append('*');
        if (start==end)
            ret.appendf("%" I64F "x", start);
        else
            ret.appendf("%" I64F "x-%" I64F "x", start, end);
        return ret;
    }
};

class CRoxieFileCache : implements IRoxieFileCache, implements ICopyFileProgress, public CInterface
{
    friend class CcdFileTest;
    mutable ICopyArrayOf<ILazyFileIO> todo; // Might prefer a queue but probably doesn't really matter.
    InterruptableSemaphore toCopy;
    InterruptableSemaphore toClose;
#ifdef _CONTAINERIZED
    InterruptableSemaphore cidtSleep;
#endif
    mutable CopyMapStringToMyClass<ILazyFileIO> files;
    mutable CriticalSection crit;
    CriticalSection cpcrit;
    bool started;
    bool aborting;
    std::atomic<bool> closing;
    bool closePending[2];
    StringAttrMapping fileErrorList;
#ifdef _CONTAINERIZED
    Semaphore cidtStarted;
#endif
    Semaphore bctStarted;
    Semaphore hctStarted;

    // Read-tracking code for pre-warming OS caches

    StringArray cacheIndexes;
    UnsignedShortArray cacheIndexChannels;
    CacheReportingBuffer *activeCacheReportingBuffer = nullptr;

    RoxieFileStatus fileUpToDate(IFile *f, offset_t size, const CDateTime &modified, bool isCompressed, bool autoDisconnect=true)
    {
        // Ensure that SockFile does not keep these sockets open (or we will run out)
        class AutoDisconnector
        {
        public:
            AutoDisconnector(IFile *_f, bool isEnabled) { f = isEnabled ? _f : NULL; };
            ~AutoDisconnector() { if (f) disconnectRemoteFile(f); }
        private:
            IFile *f;
        } autoDisconnector(f, autoDisconnect);

        offset_t fileSize = f->size();
        if (fileSize != (offset_t) -1)
        {
            // only check size if specified
            if ( (size != (offset_t) -1) && !isCompressed && fileSize != size) // MORE - should be able to do better on compressed you'da thunk
                return FileSizeMismatch;
            // A temporary fix - files stored on azure don't have an accurate time stamp, so treat them as up to date.
            if (isUrl(f->queryFilename()))
                return FileIsValid;
            CDateTime mt;
            return (modified.isNull() || (f->getTime(NULL, &mt, NULL) &&  mt.equals(modified, false))) ? FileIsValid : FileDateMismatch;
        }
        else
            return FileNotFound;
    }

#ifdef _CONTAINERIZED
    int runCacheInfoDump()
    {
        cidtStarted.signal();
        if (traceLevel)
            DBGLOG("Cache info dump thread %p starting", this);
        try
        {
            for (;;)
            {
                cidtSleep.wait(cacheReportPeriodSeconds * 1000);
                if (closing)
                    break;
                if (traceLevel)
                    DBGLOG("Cache info dump");
                // Note - cache info is stored in the DLLSERVER persistent area - which we should perhaps consider renaming
                const char* dllserver_root = getenv("HPCC_DLLSERVER_PATH");
                assertex(dllserver_root != nullptr);

                Owned<const ITopologyServer> topology = getTopology();
                Owned<CacheReportingBuffer> tempCacheReportingBuffer = new CacheReportingBuffer(*activeCacheReportingBuffer);
                getNodeCacheInfo(*tempCacheReportingBuffer);

                tempCacheReportingBuffer->sortAndDedup();
                StringBuffer ret;
                tempCacheReportingBuffer->report(ret, 0, cacheIndexes, cacheIndexChannels);
                if (ret.length())
                {
                    // NOTE - this location is shared with other nodes - who may also be writing
                    VStringBuffer cacheFileName("%s/%s/cacheInfo.%d", dllserver_root, roxieName.str(), 0);
                    atomicWriteFile(cacheFileName, ret);
                    if (traceLevel > 8)
                        DBGLOG("Channel 0 cache info:\n%s", ret.str());
                }
                for (unsigned channel : topology->queryChannels())
                {
                    tempCacheReportingBuffer->report(ret.clear(), channel, cacheIndexes, cacheIndexChannels);
                    if (ret.length())
                    {
                        VStringBuffer cacheFileName("%s/%s/cacheInfo.%d", dllserver_root, roxieName.str(), channel);
                        atomicWriteFile(cacheFileName, ret);
                        if (traceLevel > 8)
                            DBGLOG("Channel %u cache info:\n%s", channel, ret.str());
                    }
                }
                // We could at this point put deduped back into active
            }
        }
        catch (IException *E)
        {
            // Any exceptions terminate the thread - probably a better option than flooding the log
            if (!aborting)
                EXCLOG(MCoperatorError, E, "Cache info dumper: ");
            E->Release();
        }
        catch (...)
        {
            IERRLOG("Unknown exception in cache info dump thread");
        }
        if (traceLevel)
            DBGLOG("Cache info dump thread %p exiting", this);
        return 0;
    }
#endif

    unsigned trackCache(const char *filename, unsigned channel)
    {
        // NOTE - called from openFile, with crit already held
        if (!activeCacheReportingBuffer)
            return (unsigned) -1;
        cacheIndexes.append(filename);
        cacheIndexChannels.append(channel);
        return cacheIndexes.length()-1;
    }

    virtual void noteRead(unsigned fileIdx, offset_t pos, unsigned len) override
    {
        if (activeCacheReportingBuffer)
            activeCacheReportingBuffer->noteRead(fileIdx, pos, len, true);
    }

    ILazyFileIO *openFile(const char *lfn, unsigned partNo, unsigned channel, const char *localLocation,
                           IPartDescriptor *pdesc,
                           const StringArray &remoteLocationInfo,
                           offset_t size, const CDateTime &modified)
    {
        Owned<IFile> local = createIFile(localLocation);
        bool isCompressed = selfTestMode ? false : pdesc->queryOwner().isCompressed();
        unsigned crc = 0;
        if (!selfTestMode)
            pdesc->getCrc(crc);
        Owned<CRoxieLazyFileIO> ret = new CRoxieLazyFileIO(local.getLink(), size, modified, isCompressed, crc);
        RoxieFileStatus fileStatus = fileUpToDate(local, size, modified, isCompressed);
        if (fileStatus == FileIsValid)
        {
            ret->addSource(local.getLink());
            ret->setRemote(false);
        }
        else if (local->exists() && !ignoreOrphans)  // Implies local dali and local file out of sync
            throw MakeStringException(ROXIE_FILE_ERROR, "Local file %s does not match DFS information", localLocation);
        else
        {
            bool addedOne = false;

            // put the peerRoxieLocations next in the list
            StringArray localLocations;
            if (selfTestMode)
                localLocations.append("test.buddy");
            else
                appendRemoteLocations(pdesc, localLocations, localLocation, roxieName, true);  // Adds all locations on the same cluster
            ForEachItemIn(roxie_idx, localLocations)
            {
                try
                {
                    const char *remoteName = localLocations.item(roxie_idx);
                    Owned<IFile> remote = createIFile(remoteName);
                    RoxieFileStatus status = fileUpToDate(remote, size, modified, isCompressed);
                    if (status==FileIsValid)
                    {
                        if (miscDebugTraceLevel > 5)
                            DBGLOG("adding peer location %s", remoteName);
                        ret->addSource(remote.getClear());
                        addedOne = true;
                    }
                    else if (status==FileNotFound)
                    {
                        // Even though it's not on the buddy (yet), add it to the locations since it may well be there
                        // by the time we come to copy (and if it is, we want to copy from there)
                        if (miscDebugTraceLevel > 5)
                            DBGLOG("adding missing peer location %s", remoteName);
                        ret->addSource(remote.getClear());
                        // Don't set addedOne - we need to go to remote too
                    }
                    else if (miscDebugTraceLevel > 10)
                        DBGLOG("Checked peer roxie location %s, status=%d", remoteName, (int) status);
                }
                catch (IException *E)
                {
                    EXCLOG(MCoperatorError, E, "While creating remote file reference");
                    E->Release();
                }
                ret->setRemote(true);
            }

            if (!addedOne && (copyResources || useRemoteResources || selfTestMode))  // If no peer locations available, go to remote
            {
                ForEachItemIn(idx, remoteLocationInfo)
                {
                    try
                    {
                        const char *remoteName = remoteLocationInfo.item(idx);
                        Owned<IFile> remote = createIFile(remoteName);
                        if (traceLevel > 5)
                            DBGLOG("checking remote location %s", remoteName);
                        RoxieFileStatus status = fileUpToDate(remote, size, modified, isCompressed);
                        if (status==FileIsValid)
                        {
                            if (miscDebugTraceLevel > 5)
                                DBGLOG("adding remote location %s", remoteName);
                            RemoteFilename rfn;
                            rfn.setRemotePath(remoteName);
                            if (!rfn.isLocal())    // MORE - may still want to copy files even if they are on a posix-accessible path, for local caching? Probably really want to know if hooked or not...
                                ret->setRemote(true);
                            ret->addSource(remote.getClear());
                            addedOne = true;
                        }
                        else if (miscDebugTraceLevel > 10)
                            DBGLOG("Checked remote file location %s, status=%d", remoteName, (int) status);
                    }
                    catch (IException *E)
                    {
                        EXCLOG(MCoperatorError, E, "While creating remote file reference");
                        E->Release();
                    }
                }
            }

            if (!addedOne)
            {
                if (local->exists())  // Implies local dali and local file out of sync
                    throw MakeStringException(ROXIE_FILE_ERROR, "Local file %s does not match DFS information", localLocation);
                else
                {
                    if (traceLevel >= 2)
                    {
                        DBGLOG("Failed to open file at any of the following %d local locations:", localLocations.length());
                        ForEachItemIn(local_idx, localLocations)
                        {
                            DBGLOG("%d: %s", local_idx+1, localLocations.item(local_idx));
                        }
                        DBGLOG("Or at any of the following %d remote locations:", remoteLocationInfo.length());
                        ForEachItemIn(remote_idx, remoteLocationInfo)
                        {
                            DBGLOG("%d: %s", remote_idx+1, remoteLocationInfo.item(remote_idx));
                        }
                    }
                    throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Could not open file %s", localLocation);
                }
            }
        }
        ret->setCache(this, trackCache(local->queryFilename(), channel));
        files.setValue(local->queryFilename(), (ILazyFileIO *)ret);
        return ret.getClear();
    }

    void deleteTempFiles(const char *targetFilename)
    {
        try
        {
            StringBuffer destPath;
            StringBuffer prevTempFile;
            splitFilename(targetFilename, &destPath, &destPath, &prevTempFile, &prevTempFile);
            prevTempFile.append("*.$$$");
            Owned<IDirectoryIterator> iter = createDirectoryIterator(destPath, prevTempFile, false, false);
            ForEach(*iter)
            {
                OwnedIFile thisFile = createIFile(iter->query().queryFilename());
                if (thisFile->isFile() == fileBool::foundYes)
                    thisFile->remove();
            }
        }
        catch(IException *E)
        {
            StringBuffer err;
            OERRLOG("Could not remove tmp file %s", E->errorMessage(err).str());
            E->Release();
        }
        catch(...)
        {
        }
    }

    bool doCopyFile(ILazyFileIO *f, const char *tempFile, const char *targetFilename, const char *destPath, const char *msg, CFflags copyFlags=CFnone)
    {
        bool fileCopied = false;
        IFile *sourceFile;
        try
        {
            f->setCopying(true);
            sourceFile = f->querySource();
        }
        catch (IException *E)
        {
            f->setCopying(false);
            EXCLOG(MCoperatorError, E, "While trying to start copying file");
            throw;
        }

        unsigned __int64 freeDiskSpace = getFreeSpace(destPath);
        deleteTempFiles(targetFilename);
        offset_t fileSize = sourceFile->size();
        if ( (fileSize + minFreeDiskSpace) > freeDiskSpace)
        {
            StringBuffer err;
            err.appendf("Insufficient disk space.  File %s needs %" I64F "d bytes, but only %" I64F "d remains, and %" I64F "d is needed as a reserve", targetFilename, sourceFile->size(), freeDiskSpace, minFreeDiskSpace);
            IException *E = MakeStringException(ROXIE_DISKSPACE_ERROR, "%s", err.str());
            EXCLOG(MCoperatorError, E);
            E->Release();
            f->setCopying(false);
        }
        else
        {
            Owned<IFile> destFile = createIFile(tempFile);

            bool hardLinkCreated = false;
            unsigned start = msTick();
            try
            {
                if (useHardLink)
                    hardLinkCreated = f->createHardFileLink();

                if (hardLinkCreated)
                    msg = "Hard Link";
                else
                {
                    DBGLOG("%sing %s to %s", msg, sourceFile->queryFilename(), targetFilename);
                    if (traceLevel > 5)
                    {
                        StringBuffer str;
                        str.appendf("doCopyFile %s", sourceFile->queryFilename());
                        TimeSection timing(str.str());
                        sourceFile->copyTo(destFile,DEFAULT_COPY_BLKSIZE,NULL,false,copyFlags);
                    }
                    else
                    {
                        sourceFile->copyTo(destFile,DEFAULT_COPY_BLKSIZE,NULL,false,copyFlags);
                    }
                }
                f->setCopying(false);
                fileCopied = true;
            }
            catch(IException *E)
            {
                f->setCopying(false);
                EXCLOG(E, "Copy exception - remove templocal");
                destFile->remove();
                deleteTempFiles(targetFilename);
                throw;
            }
            catch(...)
            {
                f->setCopying(false);
                IERRLOG("%s exception - remove templocal", msg);
                destFile->remove();
                deleteTempFiles(targetFilename);
                throw;
            }
            if (!hardLinkCreated)  // for hardlinks no rename needed
            {
                try
                {
                    destFile->rename(targetFilename);
                }
                catch(IException *)
                {
                    f->setCopying(false);
                    deleteTempFiles(targetFilename);
                    throw;
                }
                unsigned elapsed = msTick() - start;
                double sizeMB = ((double) fileSize) / (1024*1024);
                double MBperSec = elapsed ? (sizeMB / elapsed) * 1000 : 0;
                DBGLOG("%s to %s complete in %d ms (%.1f MB/sec)", msg, targetFilename, elapsed, MBperSec);
            }

            f->copyComplete();
        }
        deleteTempFiles(targetFilename);
        return fileCopied;
    }

    bool doCopy(ILazyFileIO *f, bool background, bool displayFirstFileMessage, CFflags copyFlags=CFnone)
    {
        if (!f->isRemote())
            f->copyComplete();
        else
        {
            if (displayFirstFileMessage)
                DBGLOG("Received files to copy");

            const char *targetFilename = f->queryTarget()->queryFilename();
            StringBuffer tempFile(targetFilename);
            StringBuffer destPath;
            splitFilename(tempFile.str(), &destPath, &destPath, NULL, NULL);
            if (destPath.length())
                recursiveCreateDirectory(destPath.str());
            else
                destPath.append('.');
            if (!checkDirExists(destPath.str())) {
                OERRLOG("Dest directory %s does not exist", destPath.str());
                return false;
            }
            
            tempFile.append(".$$$");
            const char *msg = background ? "Background copy" : "Copy";
            return doCopyFile(f, tempFile.str(), targetFilename, destPath.str(), msg, copyFlags);
        }
        return false;  // if we get here there was no file copied
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieFileCache() :
#ifdef _CONTAINERIZED
                        cidt(*this),
#endif
                        bct(*this), hct(*this)
    {
        aborting = false;
        closing = false;
        closePending[false] = false;
        closePending[true] = false;
        started = false;
        if (!selfTestMode && !allFilesDynamic)
        {
            offset_t cacheTrackSize = queryComponentConfig().getPropInt64("@cacheTrackSize", (offset_t) -1);
            if (cacheTrackSize == (offset_t) -1)
            {
                const char *memLimit = queryComponentConfig().queryProp("resources/limits/@memory");
                if (!memLimit)
                    memLimit = queryComponentConfig().queryProp("resources/requests/@memory");
                if (memLimit)
                {
                    try
                    {
                        cacheTrackSize = friendlyStringToSize(memLimit);
                    }
                    catch (IException *E)
                    {
                        EXCLOG(E);
                        E->Release();
                        cacheTrackSize = 0;
                    }
                }
                else
                    cacheTrackSize = 0x10000 * (1<<CacheInfoEntry::pageBits);
            }
            if (cacheTrackSize)
                activeCacheReportingBuffer = new CacheReportingBuffer(cacheTrackSize);
        }
    }

    ~CRoxieFileCache()
    {
        // NOTE - I assume that by the time I am being destroyed, system is single threaded.
        // Removing any possible race between destroying of the cache and destroying of the files in it would be complex otherwise
        HashIterator h(files);
        ForEach(h)
        {
            ILazyFileIO *f = files.mapToValue(&h.query());
            f->removeCache(this);
        }
        delete activeCacheReportingBuffer;
    }

    virtual void start()
    {
        if (!started)
        {
            bct.start();
            hct.start();
            bctStarted.wait();
            hctStarted.wait();
        }
        started = true;
    }

    virtual void startCacheReporter() override
    {
#ifdef _CONTAINERIZED
        if (activeCacheReportingBuffer && cacheReportPeriodSeconds)
        {
            cidt.start();
            cidtStarted.wait();
        }
#endif
    }

#ifdef _CONTAINERIZED
    class CacheInfoDumpThread : public Thread
    {
        CRoxieFileCache &owner;
    public:
        CacheInfoDumpThread(CRoxieFileCache &_owner) : Thread("CRoxieFileCache-CacheInfoDumpThread"), owner(_owner) {}

        virtual int run()
        {
            return owner.runCacheInfoDump();
        }
    } cidt;
#endif

    class BackgroundCopyThread : public Thread
    {
        CRoxieFileCache &owner;
    public:
        BackgroundCopyThread(CRoxieFileCache &_owner) : Thread("CRoxieFileCache-BackgroundCopyThread"), owner(_owner) {}

        virtual int run()
        {
            return owner.runBackgroundCopy();
        }
    } bct;

    class HandleCloserThread : public Thread
    {
        CRoxieFileCache &owner;
    public:
        HandleCloserThread(CRoxieFileCache &_owner) : Thread("CRoxieFileCache-HandleCloserThread"), owner(_owner) {}
        virtual int run()
        {
            return owner.runHandleCloser();
        }
    } hct;

    int runBackgroundCopy()
    {
        bctStarted.signal();
#if defined(__linux__) && defined(SYS_ioprio_set)
        if (backgroundCopyClass)
            syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(backgroundCopyClass, backgroundCopyPrio));
#endif
        if (traceLevel)
        {
#if defined(__linux__) && defined(SYS_ioprio_get)
            int ioprio = syscall(SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0);
            int ioclass = IOPRIO_PRIO_CLASS(ioprio);
            ioprio = IOPRIO_PRIO_DATA(ioprio);
            DBGLOG("Background copy thread %p starting, io priority class %d, priority %d", this, ioclass, ioprio);
#else
            DBGLOG("Background copy thread %p starting", this);
#endif
        }
        try
        {
            int fileCopiedCount = 0;
            bool fileCopied = false;

            for (;;)
            {
                fileCopied = false;
                Linked<ILazyFileIO> next;
                toCopy.wait();
                {
                    CriticalBlock b(crit);
                    if (closing)
                        break;
                    if (todo.ordinality())
                    {
                        ILazyFileIO *popped = &todo.popGet();
                        if (popped->isAliveAndLink())
                        {
                            next.setown(popped);
                        }
                        numFilesToProcess--;    // must decrement counter for SNMP accuracy
                    }
                }
                if (next)
                {
                    try
                    {
                        fileCopied = doCopy(next, true, (fileCopiedCount==0) ? true : false, CFflush_rdwr);
                        CriticalBlock b(crit);
                        if (fileCopied)
                            fileCopiedCount++;
                    }
                    catch (IException *E)
                    {
                        if (aborting)
                            throw;
                        EXCLOG(MCoperatorError, E, "Roxie background copy: ");
                        E->Release();
                    }
                    catch (...) 
                    {
                        EXCLOG(MCoperatorError, "Unknown exception in Roxie background copy");
                    }
                }

                CriticalBlock b(crit);
                if ( (todo.ordinality()== 0) && (fileCopiedCount)) // finished last copy
                {
                    DBGLOG("No more data files to copy");
                    fileCopiedCount = 0;
                }
            }
        }
        catch (IException *E)
        {
            if (!aborting)
                EXCLOG(MCoperatorError, E, "Roxie background copy: ");
            E->Release();
        }
        catch (...) 
        {
            IERRLOG("Unknown exception in background copy thread");
        }
        if (traceLevel)
            DBGLOG("Background copy thread %p exiting", this);
        return 0;
    }

    int runHandleCloser()
    {
        hctStarted.signal();
        if (traceLevel)
            DBGLOG("HandleCloser thread %p starting", this);
        try
        {
            for (;;)
            {
                toClose.wait(10 * 60 * 1000);  // check expired file handles every 10 minutes 
                if (closing)
                    break;
                doCloseExpired(true);
                doCloseExpired(false);
            }
        }
        catch (IException *E)
        {
            if (!aborting)
                EXCLOG(MCoperatorError, E, "Roxie handle closer: ");
            E->Release();
        }
        catch (...) 
        {
            IERRLOG("Unknown exception in handle closer thread");
        }
        if (traceLevel)
            DBGLOG("Handle closer thread %p exiting", this);
        return 0;
    }

    virtual void join(unsigned timeout=INFINITE)
    {
        aborting = true;
        if (started)
        {
            toCopy.interrupt();
            toClose.interrupt();
            bct.join(timeout);
            hct.join(timeout);
#ifdef _CONTAINERIZED
            if (activeCacheReportingBuffer && cacheReportPeriodSeconds)
            {
                cidtSleep.interrupt();
                cidt.join(timeout);
            }
#endif
        }
    }

    virtual void wait()
    {
        closing = true;
        if (started)
        {
            toCopy.signal();
            toClose.signal();
            bct.join();
            hct.join();
#ifdef _CONTAINERIZED
            if (activeCacheReportingBuffer && cacheReportPeriodSeconds)
            {
                cidtSleep.signal();
                cidt.join();
            }
#endif
        }
    }

    virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize)
    {
        return aborting ? CFPcancel : CFPcontinue;
    }

    virtual void removeCache(ILazyFileIO *file) const
    {
        CriticalBlock b(crit);
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table.
        // So only remove from hash table if what we find there matches the item that is being deleted.
        const char *filename = file->queryFilename();
        ILazyFileIO *goer = files.getValue(filename);
        if (goer == file)
            files.remove(filename);
        ForEachItemInRev(idx, todo)
        {
            if (file == &todo.item(idx))
            {
                todo.remove(idx);
                numFilesToProcess--;    // must decrement counter for SNMP accuracy
            }
        }
    }

    virtual ILazyFileIO *lookupFile(const char *lfn, RoxieFileType fileType,
                                     IPartDescriptor *pdesc, unsigned numParts, unsigned channel,
                                     const StringArray &deployedLocationInfo, bool startFileCopy)
    {
        unsigned replicationLevel = getReplicationLevel(channel);
        IPropertyTree &partProps = pdesc->queryProperties();
        offset_t dfsSize = partProps.getPropInt64("@size", -1);
        bool local = partProps.getPropBool("@local");
        CDateTime dfsDate;
        if (checkFileDate)
        {
            const char *dateStr = partProps.queryProp("@modified");
            dfsDate.setString(dateStr);
        }

        unsigned partNo = pdesc->queryPartIndex() + 1;
        StringBuffer localLocation;

        if (local)
        {
            assertex(partNo==1 && numParts==1);
            localLocation.append(lfn);  // any resolution done earlier
        }
        else
        {
            // MORE - not at all sure about this. Foreign files should stay foreign ?
            CDfsLogicalFileName dlfn;
            dlfn.set(lfn);
            if (dlfn.isForeign())
                dlfn.clearForeign();
            makePhysicalPartName(dlfn.get(), partNo, numParts, localLocation, replicationLevel, DFD_OSdefault);
        }
        Owned<ILazyFileIO> ret;
        try
        {
            CriticalBlock b(crit);
            ILazyFileIO * match = files.getValue(localLocation);
            if (match && match->isAliveAndLink())
            {
                Owned<ILazyFileIO> f = match;
                if ((dfsSize != (offset_t) -1 && dfsSize != f->getSize()) ||
                    (!dfsDate.isNull() && !dfsDate.equals(*f->queryDateTime(), false)))
                {
                    releaseAgentDynamicFileCache();  // Agent dynamic file cache or...
                    if (fileType == ROXIE_KEY)       // ...jhtree cache can keep files active and thus prevent us from loading a new version
                        clearKeyStoreCacheEntry(f);  // Will release iff that is the only link
                    f.clear(); // Note - needs to be done before calling getValue() again, hence the need to make it separate from the f.set below
                    f.set(files.getValue(localLocation));
                    if (f)  // May have been cleared above...
                    {
                        StringBuffer modifiedDt;
                        if (!dfsDate.isNull())
                            dfsDate.getString(modifiedDt);
                        StringBuffer fileDt;
                        f->queryDateTime()->getString(fileDt);
                        if (fileErrorList.find(lfn) == 0)
                        {
                            switch (fileType)
                            {
                                case ROXIE_KEY:
                                    fileErrorList.setValue(lfn, "Key");
                                    break;

                                case ROXIE_FILE:
                                    fileErrorList.setValue(lfn, "File");
                                    break;
                            }
                        }
                        throw MakeStringException(ROXIE_MISMATCH, "Different version of %s already loaded: sizes = %" I64F "d %" I64F "d  Date = %s  %s", lfn, dfsSize, f->getSize(), modifiedDt.str(), fileDt.str());
                    }
                }
                else
                    return f.getClear();
            }

            ret.setown(openFile(lfn, partNo, channel, localLocation, pdesc, deployedLocationInfo, dfsSize, dfsDate));

            if (startFileCopy)
            {
                if (ret->isRemote())
                {
                    if (copyResources) // MORE - should always copy peer files
                    {
                        if (numParts==1 || (partNo==numParts && fileType==ROXIE_KEY))
                        {
                            ret->checkOpen();
                            doCopy(ret, false, false, CFflush_rdwr);
                            return ret.getLink();
                        }

                        // Copies are popped from end of the todo list
                        // By putting the replicates on the front we ensure they are done after the primaries
                        // and are therefore likely to result in local rather than remote copies.
                        if (replicationLevel)
                            todo.add(*ret, 0);
                        else
                            todo.append(*ret);
                        numFilesToProcess++;  // must increment counter for SNMP accuracy
                        toCopy.signal();

                    }
                }
            }
            if (!lazyOpen)
                ret->checkOpen();
        }
        catch(IException *e)
        {
            if (e->errorCode() == ROXIE_FILE_OPEN_FAIL)
            {
                if (fileErrorList.find(lfn) == 0)
                {
                    switch (fileType)
                    {
                        case ROXIE_KEY:
                            fileErrorList.setValue(lfn, "Key");
                            break;
                        
                        case ROXIE_FILE:
                            fileErrorList.setValue(lfn, "File");
                            break;
                    }
                }
            }
            throw;
        }
        return ret.getLink();
    }

    ILazyFileIO *lookupLocalFile(const char *filename)
    {
        try
        {
            CriticalBlock b(crit);
            ILazyFileIO * match = files.getValue(filename);
            if (match && match->isAliveAndLink())
                return match;
        }
        catch(IException *e)
        {
            e->Release();
        }
        return nullptr;
    }

    virtual void closeExpired(bool remote)
    {
        // This schedules a close at the next available opportunity
        CriticalBlock b(cpcrit); // paranoid...
        if (!closePending[remote])
        {
            closePending[remote] = true;
            DBGLOG("closeExpired %s scheduled - %d files open", remote ? "remote" : "local", (int) atomic_read(&numFilesOpen[remote]));
            toClose.signal();
        }
    }

    static unsigned __int64 readPage(const char * &_t)
    {
        const char *t = _t;
        unsigned __int64 v = 0;
        for (;;)
        {
            char c = *t;
            if ((c >= '0') && (c <= '9'))
                v = v * 16 + (c-'0');
            else if ((c >= 'a') && (c <= 'f'))
                v = v * 16 + (c-'a'+10);
            else if ((c >= 'A') && (c <= 'F'))
                v = v * 16 + (c-'A'+10);
            else
                break;
            t++;
        }
        _t = t;
        return v;
    }

    virtual void loadSavedOsCacheInfo() override
    {
        Owned<const ITopologyServer> topology = getTopology();
        for (unsigned channel : topology->queryChannels())
            doLoadSavedOsCacheInfo(channel);
        doLoadSavedOsCacheInfo(0);  // MORE - maybe only if I am also a server?
    }

    void doLoadSavedOsCacheInfo(unsigned channel)
    {
        const char* dllserver_root = getenv("HPCC_DLLSERVER_PATH");
        assertex(dllserver_root != nullptr);
        VStringBuffer cacheFileName("%s/%s/cacheInfo.%d", dllserver_root, roxieName.str(), channel);
        StringBuffer cacheInfo;
        try
        {
            if (checkFileExists(cacheFileName))
            {
                cacheInfo.loadFile(cacheFileName, false);
                warmOsCache(cacheInfo);
                if (traceLevel)
                    DBGLOG("Loaded cache information from %s for channel %d", cacheFileName.str(), channel);
            }
        }
        catch(IException *E)
        {
            EXCLOG(E);
            E->Release();
        }
    }


    virtual void warmOsCache(const char *cacheInfo) override
    {
        if (!cacheInfo)
            return;
#ifndef _WIN32
        size_t os_page_size = getpagesize();
#endif
        char t = 0;
        unsigned touched = 0;
        unsigned preloaded = 0;
        Owned<const ITopologyServer> topology = getTopology();
        while (*cacheInfo)
        {
            // We are parsing lines that look like:
            // <channel>|<filename>|<pagelist>
            //
            // Where pagelist is a space-separated list of page numers or (inclusive) ranges.
            // A page number or range prefixed by a * means that the page(s) was found in the jhtree cache.
            //
            // For example,
            // 1|/var/lib/HPCCSystems/hpcc-data/unknown/regress/multi/dg_index_evens._1_of_3|*0 3-4
            // Pages are always recorded and specified as 8192 bytes (unless pagebits ever changes).

            unsigned fileChannel = strtoul(cacheInfo, (char **) &cacheInfo, 10);
            if (*cacheInfo != '|')
                break;
            if (!topology->implementsChannel(fileChannel))
            {
                const char *eol = strchr(cacheInfo, '\n');
                if (!eol)
                    break;
                cacheInfo = eol+1;
                continue;
            }
            cacheInfo++;
            const char *endName = strchr(cacheInfo, '|');
            assert(endName);
            if (!endName)
                break;
            StringBuffer fileName(endName-cacheInfo, cacheInfo);
            Owned<IKeyIndex> keyIndex;
            bool keyFailed = false;
            unsigned fileIdx = (unsigned) -1;
#ifndef _WIN32
            char *file_mmap = nullptr;
            int fd = open(fileName, 0);
            struct stat file_stat;
            if (fd != -1)
            {
                fstat(fd, &file_stat);
                file_mmap = (char *) mmap((void *)0, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
                if (file_mmap == MAP_FAILED)
                {
                    DBGLOG("Failed to map file %s to pre-warm cache (error %d)", fileName.str(), errno);
                    file_mmap = nullptr;
                }
            }
            else if (traceLevel)
            {
                DBGLOG("Failed to open file %s to pre-warm cache (error %d)", fileName.str(), errno);
            }
#endif
            Owned<ILazyFileIO> localFile = lookupLocalFile(fileName);
            if (localFile)
            {
                fileIdx = localFile->getFileIdx();
            }
            cacheInfo = endName+1;  // Skip the |
            while (*cacheInfo==' ')
                cacheInfo++;
            for (;;)
            {
                bool inNodeCache = (*cacheInfo=='*');
                if (inNodeCache)
                    cacheInfo++;
                __uint64 startPage = readPage(cacheInfo);
                __uint64 endPage;
                if (*cacheInfo=='-')
                {
                    cacheInfo++;
                    endPage = readPage(cacheInfo);
                }
                else
                    endPage = startPage;
                if (traceLevel > 8)
                    DBGLOG("Touching %s %" I64F "x-%" I64F "x", fileName.str(), startPage, endPage);
                offset_t startOffset = startPage << CacheInfoEntry::pageBits;
                offset_t endOffset = (endPage+1) << CacheInfoEntry::pageBits;
                if (inNodeCache && !keyFailed && localFile && !keyIndex)
                {
                    keyIndex.setown(createKeyIndex(fileName, localFile->getCrc(), *localFile.get(), fileIdx, false, false));  // MORE - we don't know if it's a TLK, but hopefully it doesn't matter
                    if (!keyIndex)
                        keyFailed = true;
                }
                if (inNodeCache && keyIndex)
                {
                    // Round startOffset up to nearest multiple of index node size
                    unsigned nodeSize = keyIndex->getNodeSize();
                    startOffset = ((startOffset+nodeSize-1)/nodeSize)*nodeSize;
                    do
                    {
                        bool loaded = keyIndex->prewarmPage(startOffset);
                        if (!loaded)
                            break;
                        preloaded++;
                        startOffset += nodeSize;
                    }
                    while (startOffset < endOffset);
                }
#ifndef _WIN32
                else if (file_mmap)
                {
                    if (fileIdx != (unsigned) -1)
                        noteRead(fileIdx, startOffset, (endOffset-1) - startOffset);  // Ensure pages we prewarm are recorded in our cache tracker
                    do
                    {
                        if (startOffset >= (offset_t) file_stat.st_size)
                            break;    // Let's not core if the file has changed size since we recorded the info...
                        t += file_mmap[startOffset];  // NOTE - t reported below so it cannot be optimized out
                        touched++;
                        startOffset += os_page_size;
                    }
                    while (startOffset < endOffset);
                }
#endif
                if (*cacheInfo != ' ')
                    break;
                cacheInfo++;
            }
#ifndef _WIN32
            if (file_mmap)
                munmap(file_mmap, file_stat.st_size);
            if (fd != -1)
                close(fd);
#endif
            if (*cacheInfo != '\n')
                break;
            cacheInfo++;
        }
        assert(!*cacheInfo);
        if (*cacheInfo)
        {
            DBGLOG("WARNING: Unrecognized cacheInfo format at %.20s", cacheInfo);
        }
        if (traceLevel)
            DBGLOG("Touched %d pages, preloaded %d index nodes, result %d", touched, preloaded, t);  // We report t to make sure that compiler doesn't decide to optimize it away entirely
    }

    virtual void clearOsCache() override
    {
        if (activeCacheReportingBuffer)
            activeCacheReportingBuffer->clear();
    }

    virtual void reportOsCache(StringBuffer &ret, unsigned channel) const override
    {
        if (activeCacheReportingBuffer)
        {
            Owned<CacheReportingBuffer> temp = new CacheReportingBuffer(*activeCacheReportingBuffer);
            getNodeCacheInfo(*temp);
            temp->sortAndDedup();
            temp->report(ret, channel, cacheIndexes, cacheIndexChannels);
            // We could at this point put deduped back into active
        }
    }

    void doCloseExpired(bool remote)
    {
        {
            CriticalBlock b(cpcrit); // paranoid...
            closePending[remote] = false;
        }
        IArrayOf<ILazyFileIO> goers;
        {
            CriticalBlock b(crit);
            HashIterator h(files);
            ForEach(h)
            {
                ILazyFileIO * match = files.mapToValue(&h.query());
                if (match->isAliveAndLink())
                {
                    Owned<ILazyFileIO> f = match;
                    if (f->isOpen() && f->isRemote()==remote && !f->isCopying())
                    {
                        unsigned age = msTick() - f->getLastAccessed();
                        if (age > maxFileAge[remote])
                        {
                            if (traceLevel > 5)
                            {
                                // NOTE - querySource will cause the file to be opened if not already open
                                // That's OK here, since we know the file is open and remote.
                                // But don't be tempted to move this line outside these if's (eg. to trace the idle case)
                                const char *fname = remote ? f->querySource()->queryFilename() : f->queryFilename();
                                DBGLOG("Closing inactive %s file %s (last accessed %u ms ago)", remote ? "remote" : "local",  fname, age);
                            }
                            f->close();
                        }
                        else
                            goers.append(*f.getClear());
                    }
                }
            }
        }
        unsigned numFilesLeft = goers.ordinality(); 
        if (numFilesLeft > maxFilesOpen[remote])
        {
            goers.sort(CRoxieLazyFileIO::compareAccess);
            DBGLOG("Closing LRU %s files, %d files are open", remote ? "remote" : "local",  numFilesLeft);
            unsigned idx = minFilesOpen[remote];
            while (idx < numFilesLeft)
            {
                ILazyFileIO &f = goers.item(idx++);
                if (!f.isCopying())
                {
                    if (traceLevel > 5)
                    {
                        unsigned age = msTick() - f.getLastAccessed();
                        DBGLOG("Closing %s (last accessed %u ms ago)", f.queryFilename(), age);
                    }
                    f.close();
                }
            }
        }
    }

    virtual void flushUnusedDirectories(const char *origBaseDir, const char *directory, StringBuffer &xml)
    {
        Owned<IFile> dirf = createIFile(directory);
        if (dirf->exists() && dirf->isDirectory()==fileBool::foundYes)
        {
            try
            {
                Owned<IDirectoryIterator> iter = dirf->directoryFiles(NULL,false,true);
                ForEach(*iter)
                {
                    const char *thisName = iter->query().queryFilename();
                    flushUnusedDirectories(origBaseDir, thisName, xml);
                }
                
                if (stricmp(origBaseDir, directory) != 0)
                {
                    try
                    {
                        dirf->remove();
                        xml.appendf("<Directory>%s</Directory>\n", directory);
                        DBGLOG("Deleted directory %s", directory);
                    }
                    catch (IException *e)
                    {
                        // don't care if we can't delete the directory
                        e->Release();
                    }
                    catch(...)
                    {
                        // don't care if we can't delete the directory
                    }
                }
            }
            catch (IException *e)
            {
                // don't care if we can't delete the directory
                e->Release();
            }
            catch(...)
            {
                // don't care if we can't delete the directory
            }
        }
    }

    int numFilesToCopy()
    {
        CriticalBlock b(crit);
        return todo.ordinality();
    }

    virtual StringAttrMapping *queryFileErrorList() { return &fileErrorList; }  // returns list of files that could not be open

    static inline bool validFNameChar(char c)
    {
        static const char *invalids = "*\"/:<>?\\|";
        return (c>=32 && c<127 && !strchr(invalids, c));
    }
};

ILazyFileIO *createPhysicalFile(const char *id, IPartDescriptor *pdesc, IPartDescriptor *remotePDesc, RoxieFileType fileType, int numParts, bool startCopy, unsigned channel)
{
    StringArray remoteLocations;
    const char *peerCluster = pdesc->queryOwner().queryProperties().queryProp("@cloneFromPeerCluster");
    if (peerCluster)
    {
        if (*peerCluster!='-') // a remote cluster was specified explicitly
            appendRemoteLocations(pdesc, remoteLocations, NULL, peerCluster, true);  // Add only from specified cluster
    }
    else
        appendRemoteLocations(pdesc, remoteLocations, NULL, roxieName, false);      // Add from any cluster on same dali, other than mine
    if (remotePDesc)
        appendRemoteLocations(remotePDesc, remoteLocations, NULL, NULL, false);    // Then any remote on remote dali

    return queryFileCache().lookupFile(id, fileType, pdesc, numParts, channel, remoteLocations, startCopy);
}

//====================================================================================================

class CFilePartMap : implements IFilePartMap, public CInterface
{
    class FilePartMapElement
    {
    public:
        offset_t base;
        offset_t top;

        inline int compare(offset_t offset)
        {
            if (offset < base)
                return -1;
            else if (offset >= top)
                return 1;
            else
                return 0;
        }
    } *map;

    static int compareParts(const void *l, const void *r)
    {
        offset_t lp = * (offset_t *) l;
        FilePartMapElement *thisPart = (FilePartMapElement *) r;
        return thisPart->compare(lp);
    }

    unsigned numParts;
    offset_t recordCount;
    offset_t totalSize;
    StringAttr fileName;

public:
    IMPLEMENT_IINTERFACE;
    CFilePartMap(IPropertyTree &resource)
    {
        fileName.set(resource.queryProp("@id"));
        numParts = resource.getPropInt("@numparts");
        recordCount = resource.getPropInt64("@recordCount");
        totalSize = resource.getPropInt64("@size");
        assertex(numParts);
        map = new FilePartMapElement[numParts];
        for (unsigned i = 0; i < numParts; i++)
        {
            StringBuffer partPath;
            partPath.appendf("Part[@num='%d']", i+1);
            IPropertyTree *part = resource.queryPropTree(partPath.str());
            if (!part)
            {
                partPath.clear().appendf("Part_%d", i+1);   // legacy format support
                part = resource.queryPropTree(partPath.str());
            }
            assertex(part);
            offset_t size = part->getPropInt64("@size", (unsigned __int64) -1);
            assertex(size != (unsigned __int64) -1);
            map[i].base = i ? map[i-1].top : 0;
            map[i].top = map[i].base + size;
        }
        if (totalSize == (offset_t)-1)
            totalSize = map[numParts-1].top;
        else if (totalSize != map[numParts-1].top)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%" I64F "d vs %" I64F "d", map[numParts-1].top, totalSize);
    }

    CFilePartMap(const char *_fileName, IFileDescriptor &fdesc)
        : fileName(_fileName)
    {
        numParts = fdesc.numParts();
        IPropertyTree &props = fdesc.queryProperties();
        recordCount = props.getPropInt64("@recordCount", -1);
        totalSize = props.getPropInt64("@size", -1);
        assertex(numParts);
        map = new FilePartMapElement[numParts];
        for (unsigned i = 0; i < numParts; i++)
        {
            IPartDescriptor &part = *fdesc.queryPart(i);
            IPropertyTree &partProps = part.queryProperties();
            offset_t size = partProps.getPropInt64("@size", (unsigned __int64) -1);
            map[i].base = i ? map[i-1].top : 0;
            if (size==(unsigned __int64) -1)
            {
                if (i==numParts-1)
                    map[i].top = (unsigned __int64) -1;
                else
                    throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file sizes not known for file %s", fileName.get());
            }
            else
                map[i].top = map[i].base + size;
        }
        if (totalSize == (offset_t)-1)
            totalSize = map[numParts-1].top;
        else if (totalSize != map[numParts-1].top)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%" I64F "d vs %" I64F "d", map[numParts-1].top, totalSize);
    }

    ~CFilePartMap()
    {
        delete [] map;
    }
    virtual bool IsShared() const { return CInterface::IsShared(); };
    virtual unsigned mapOffset(offset_t pos) const
    {
        FilePartMapElement *part = (FilePartMapElement *) bsearch(&pos, map, numParts, sizeof(map[0]), compareParts);
        if (!part)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file position %" I64F "d in file %s out of range (max offset permitted is %" I64F "d)", pos, fileName.str(), totalSize);
        return (part-map)+1;
    }
    virtual unsigned getNumParts() const
    {
        return numParts;
    }
    virtual offset_t getTotalSize() const
    {
        return totalSize;
    }
    virtual offset_t getRecordCount() const
    {
        return recordCount;
    }
    virtual offset_t getBase(unsigned part) const
    {
        if (part > numParts || part == 0)
        {
            throw MakeStringException(ROXIE_FILE_ERROR, "Internal error - requesting base for non-existant file part %d (valid are 1-%d)", part, numParts);
        }
        return map[part-1].base;
    }
    virtual offset_t getFileSize() const
    {
        return map[numParts-1].top;
    }
};

extern IFilePartMap *createFilePartMap(const char *fileName, IFileDescriptor &fdesc)
{
    return new CFilePartMap(fileName, fdesc);
}

//====================================================================================================

class CFileIOArray : implements IFileIOArray, public CInterface
{
    mutable CriticalSection crit;
    mutable unsigned __int64 totalSize = (unsigned __int64) -1;  // Calculated on demand, and cached
    mutable StringAttr id;               // Calculated on demand, and cached
    IPointerArrayOf<IFileIO> files;
    UnsignedArray subfiles;
    StringArray filenames;
    Int64Array bases;
    int actualCrc = 0;
    unsigned valid = 0;
    bool multipleFormatsSeen = false;

    void _getId() const
    {
        md5_state_t md5;
        md5_byte_t digest[16];
        md5_init(&md5);

        ForEachItemIn(idx, files)
        {
            IFileIO *file = files.item(idx);
            if (file)
            {
                md5_append(&md5, (const md5_byte_t *) &file, sizeof(file));
            }
        }
        md5_finish(&md5, digest);
        char digestStr[33];
        for (int i = 0; i < 16; i++)
        {
            sprintf(&digestStr[i*2],"%02x", digest[i]);
        }
        id.set(digestStr, 32);
    }

public:
    IMPLEMENT_IINTERFACE;

    virtual bool IsShared() const { return CInterface::IsShared(); };

    virtual IFileIO *getFilePart(unsigned partNo, offset_t &base) const override
    {
        if (!files.isItem(partNo))
        {
            DBGLOG("getFilePart requested invalid part %d", partNo);
            throw MakeStringException(ROXIE_FILE_ERROR, "getFilePart requested invalid part %d", partNo);
        }
        IFileIO *file = files.item(partNo);
        if (!file)
        {
            base = 0;
            return NULL;
        }
        base = bases.item(partNo);
        return LINK(file);
    }

    virtual const char *queryLogicalFilename(unsigned partNo) const override
    {
        if (!filenames.isItem(partNo))
        {
            DBGLOG("queryLogicalFilename requested invalid part %d", partNo);
            throw MakeStringException(ROXIE_FILE_ERROR, "queryLogicalFilename requested invalid part %d", partNo);
        }
        return filenames.item(partNo);
    }

    void addFile(IFileIO *f, offset_t base, unsigned subfile, const char *filename, int _actualCrc)
    {
        if (f)
            valid++;
        files.append(f);
        bases.append(base);
        if (_actualCrc)
        {
            if (actualCrc && actualCrc != _actualCrc)
                multipleFormatsSeen = true;
            else
                actualCrc = _actualCrc;
        }
        // MORE - lots of duplication in subfiles and filenames arrays
        subfiles.append(subfile);
        filenames.append(filename ? filename : "");
    }

    virtual unsigned length() const override
    {
        return files.length();
    }

    virtual unsigned numValid() const override
    {
        return valid;
    }

    virtual int queryActualFormatCrc() const override
    {
        return actualCrc;
    }

    virtual bool allFormatsMatch() const override
    {
        return !multipleFormatsSeen;
    }

    virtual bool isValid(unsigned partNo) const override
    {
        if (!files.isItem(partNo))
            return false;
        IFileIO *file = files.item(partNo);
        if (!file)
            return false;
        return true;
    }

    virtual unsigned __int64 size() const override
    {
        CriticalBlock b(crit);
        if (totalSize == (unsigned __int64) -1)
        {
            totalSize = 0;
            ForEachItemIn(idx, files)
            {
                IFileIO *file = files.item(idx);
                if (file)
                    totalSize += file->size();
            }
        }
        return totalSize;
    }

    virtual StringBuffer &getId(StringBuffer &ret) const override
    {
        CriticalBlock b(crit);
        if (!id)
            _getId();
        return ret.append(id);
    }

    virtual unsigned getSubFile(unsigned partNo) const override
    {
        return subfiles.item(partNo);
    }
};

class CTranslatorSet : implements CInterfaceOf<ITranslatorSet>
{
    IConstPointerArrayOf<IDynamicTransform> transformers;
    IConstPointerArrayOf<IKeyTranslator> keyTranslators;
    IPointerArrayOf<IOutputMetaData> actualLayouts;
    const RtlRecord &targetLayout;
    int targetFormatCrc = 0;
    bool anyTranslators = false;
    bool anyKeyedTranslators = false;
    bool translatorsMatch = true;
public:
    CTranslatorSet(const RtlRecord &_targetLayout, int _targetFormatCrc)
    : targetLayout(_targetLayout), targetFormatCrc(_targetFormatCrc)
    {}

    void addTranslator(const IDynamicTransform *translator, const IKeyTranslator *keyTranslator, IOutputMetaData *actualLayout)
    {
        assertex(actualLayout);
        if (translator || keyTranslator)
            anyTranslators = true;
        if (translator && translator->keyedTranslated())
            anyKeyedTranslators = true;
        if (transformers.ordinality() && (translator != transformers.item(0)))
            translatorsMatch = false;
        transformers.append(translator);
        keyTranslators.append(keyTranslator);
        actualLayouts.append(actualLayout);
    }

    virtual const RtlRecord &queryTargetFormat() const override
    {
        return targetLayout;
    }

    virtual int queryTargetFormatCrc() const override
    {
        return targetFormatCrc;
    }

    virtual const IDynamicTransform *queryTranslator(unsigned subFile) const override
    {
        // We need to have translated partnos to subfiles before calling this!
        // Note: while the required projected format will be the same for all parts, the
        // actual layout - and thus the required translation - may not be, for example if
        // we have a superfile with mismatching formats.
        if (anyTranslators && transformers.isItem(subFile))
            return transformers.item(subFile);
        return nullptr;
    }

    virtual const IKeyTranslator *queryKeyTranslator(unsigned subFile) const override
    {
        if (anyTranslators && keyTranslators.isItem(subFile))
            return keyTranslators.item(subFile);
        return nullptr;
    }

    virtual ISourceRowPrefetcher *getPrefetcher(unsigned subFile) const override
    {
        IOutputMetaData *actualLayout = actualLayouts.item(subFile);
        assertex(actualLayout);
        return actualLayout->createDiskPrefetcher();
    }

    virtual IOutputMetaData *queryActualLayout(unsigned subFile) const override
    {
        IOutputMetaData *actualLayout = actualLayouts.item(subFile);
        assertex(actualLayout);
        return actualLayout;
    }

    virtual bool isTranslating() const override
    {
        return anyTranslators;
    }

    virtual bool isTranslatingKeyed() const override
    {
        return anyKeyedTranslators;
    }

    virtual bool hasConsistentTranslation() const override
    {
        return translatorsMatch;
    }
};

template <class X> class PerChannelCacheOf
{
    IPointerArrayOf<X> cache;
    UnsignedArray channels;
public:
    // NOTE - typically only a couple of entries (but see PerFormatCacheOf below
    void set(X *value, unsigned channel)
    {
        cache.append(value);
        channels.append(channel);
    }

    X *get(unsigned channel) const
    {
        ForEachItemIn(idx, channels)
        {
            if (channels.item(idx)==channel)
                return cache.item(idx);
        }
        return NULL;
    }
};

template <class X> class PerFormatCacheOf : public PerChannelCacheOf<X>
{
    // Identical for now, but characteristics are different so implementations may diverge.
    // For example, this one may want to be a hash table, and there may be many more entries
};

class CResolvedFile : implements IResolvedFileCreator, implements ISafeSDSSubscription, public CInterface
{
protected:
    IResolvedFileCache *cached;
    StringAttr lfn;
    StringAttr physicalName;
    Owned<IDistributedFile> dFile; // NULL on copies serialized to agents. Note that this implies we keep a lock on dali file for the lifetime of this object.
    CDateTime fileTimeStamp;
    offset_t fileSize;
    unsigned fileCheckSum;
    RoxieFileType fileType;
    bool isSuper;

    StringArray subNames;
    IPointerArrayOf<IFileDescriptor> subFiles; // note - on agents, the file descriptors may have incomplete info. On originating server is always complete
    IPointerArrayOf<IFileDescriptor> remoteSubFiles; // note - on agents, the file descriptors may have incomplete info. On originating server is always complete
    IntArray formatCrcs;
    IPointerArrayOf<IOutputMetaData> diskTypeInfo;  // New info using RtlTypeInfo structures
    IArrayOf<IDistributedFile> subDFiles;  // To make sure subfiles get locked too
    IArrayOf<IResolvedFile> subRFiles;  // To make sure subfiles get locked too

    Owned <IPropertyTree> properties;
    Linked<IRoxieDaliHelper> daliHelper;
    Owned<IDaliPackageWatcher> notifier;

    virtual ISafeSDSSubscription *linkIfAlive() override { return isAliveAndLink() ? this : nullptr; }
    void addFile(const char *subName, IFileDescriptor *fdesc, IFileDescriptor *remoteFDesc)
    {
        subNames.append(subName);
        subFiles.append(fdesc);
        remoteSubFiles.append(remoteFDesc);
        IPropertyTree const & props = fdesc->queryProperties();

        // NOTE - grouping is not included in the formatCRC, nor is the trailing byte that indicates grouping
        // included in the rtlTypeInfo.
        const char *kind = props.queryProp("@kind");
        if (kind)
        {
            RoxieFileType thisFileType = streq(kind, "key") ? ROXIE_KEY : ROXIE_FILE;
            if (subFiles.length()==1)
                fileType = thisFileType;
            else
                assertex(thisFileType==fileType);
        }

        bool isGrouped = props.getPropBool("@grouped", false);
        int formatCrc = props.getPropInt("@formatCrc", 0);
        // If formatCrc and grouping are same as previous, reuse previous typeInfo
        Owned<IOutputMetaData> actualFormat;
        unsigned prevIdx = formatCrcs.length()-1;
        if (formatCrcs.length() && formatCrc == formatCrcs.item(prevIdx) &&
            diskTypeInfo.item(prevIdx) && isGrouped==diskTypeInfo.item(prevIdx)->isGrouped())
            actualFormat.set(diskTypeInfo.item(prevIdx));
        else
            actualFormat.setown(getDaliLayoutInfo(props));
        diskTypeInfo.append(actualFormat.getClear());
        formatCrcs.append(formatCrc);

        unsigned numParts = fdesc->numParts();
        offset_t base = 0;
        for (unsigned i = 0; i < numParts; i++)
        {
            IPartDescriptor *pdesc = fdesc->queryPart(i);
            IPropertyTree &partProps = pdesc->queryProperties();
            offset_t dfsSize = partProps.getPropInt64("@size");
            partProps.setPropInt64("@offset", base);
            base += dfsSize;
        }
        fileSize += base;
    }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        if (traceLevel > 2)
            DBGLOG("Superfile %s change detected", lfn.get());

        {
            CriticalBlock b(lock);
            if (cached)
            {
                cached->removeCache(this);
                cached = NULL;
            }
        }
        globalPackageSetManager->requestReload(false, false);
    }

    // We cache all the file maps/arrays etc here. 
    mutable CriticalSection lock;
    mutable Owned<IFilePartMap> fileMap;
    mutable PerChannelCacheOf<IInMemoryIndexManager> indexMap;
    mutable PerChannelCacheOf<IFileIOArray> ioArrayMap;
    mutable PerChannelCacheOf<IKeyArray> keyArrayMap;

public:
    IMPLEMENT_IINTERFACE;

    CResolvedFile(const char *_lfn, const char *_physicalName, IDistributedFile *_dFile, RoxieFileType _fileType, IRoxieDaliHelper* _daliHelper, bool isDynamic, bool cacheIt, bool writeAccess, bool _isSuperFile)
    : lfn(_lfn), physicalName(_physicalName), dFile(_dFile), fileType(_fileType), isSuper(_isSuperFile), daliHelper(_daliHelper)
    {
        cached = NULL;
        fileSize = 0;
        fileCheckSum = 0;
        if (dFile)
        {
            if (traceLevel > 5)
                DBGLOG("Roxie server adding information for file %s", lfn.get());
            bool tsSet = dFile->getModificationTime(fileTimeStamp);
            dFile->getFileCheckSum(fileCheckSum);
            assertex(tsSet); // per Nigel, is always set
            IDistributedSuperFile *superFile = dFile->querySuperFile();
            if (superFile)
            {
                isSuper = true;
                Owned<IDistributedFileIterator> subs = superFile->getSubFileIterator(true);
                ForEach(*subs)
                {
                    IDistributedFile &sub = subs->query();
                    Owned<IFileDescriptor> fDesc = sub.getFileDescriptor();
                    Owned<IFileDescriptor> remoteFDesc;
                    if (daliHelper)
                        remoteFDesc.setown(daliHelper->checkClonedFromRemote(sub.queryLogicalName(), fDesc, cacheIt, defaultPrivilegedUser));
                    subDFiles.append(OLINK(sub));
                    addFile(sub.queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
                }
                // We have to clone the properties since we don't want to keep the superfile locked
                properties.setown(createPTreeFromIPT(&dFile->queryAttributes(), ipt_lowmem));
                if (!isDynamic && !lockSuperFiles)
                {
                    notifier.setown(daliHelper->getSuperFileSubscription(lfn, this));
                    dFile.clear();  // We don't lock superfiles, except dynamic ones
                }
            }
            else // normal file, not superkey
            {
                isSuper = false;
                properties.set(&dFile->queryAttributes());
                Owned<IFileDescriptor> fDesc = dFile->getFileDescriptor();
                Owned<IFileDescriptor> remoteFDesc;
                if (daliHelper)
                    remoteFDesc.setown(daliHelper->checkClonedFromRemote(_lfn, fDesc, cacheIt, defaultPrivilegedUser));
                addFile(dFile->queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
            }
        }
    }
    virtual void beforeDispose()
    {
        if (notifier)
            daliHelper->releaseSubscription(notifier);
        notifier.clear();
        if (cached)
        {
            cached->removeCache(this);
        }
    }
    virtual unsigned numSubFiles() const
    {
        return subNames.length();
    }
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const
    {
        if (subNames.isItem(num))
        {
            name.append(subNames.item(num));
            return true;
        }
        else
        {
            return false;
        }
    }
    virtual unsigned findSubName(const char *subname) const
    {
        ForEachItemIn(idx, subNames)
        {
            if (strieq(subNames.item(idx), subname))
                return idx;
        }
        return NotFound;
    }
    virtual unsigned getContents(StringArray &contents) const
    {
        ForEachItemIn(idx, subNames)
        {
            contents.append(subNames.item(idx));
        }
        return subNames.length();
    }
    virtual bool isSuperFile() const
    {
        return isSuper;
    }
    virtual bool isKey() const
    {
        return fileType==ROXIE_KEY;
    }
    virtual IFilePartMap *getFileMap() const
    {
        CriticalBlock b(lock);
        if (!fileMap)
        {
            if (subFiles.length())
            {
                if (subFiles.length()!=1)
                    throw MakeStringException(0, "Roxie does not support FETCH or KEYED JOIN to superkey with multiple parts");
                fileMap.setown(createFilePartMap(lfn, *subFiles.item(0)));
            }
        }
        return fileMap.getLink();
    }
    virtual unsigned getNumParts() const
    {
        CriticalBlock b(lock);
        unsigned numParts = 0;
        ForEachItemIn(idx, subFiles)
        {
            unsigned thisNumParts = subFiles.item(idx)->numParts();
            if (thisNumParts > numParts)
                numParts = thisNumParts;
        }
        return numParts;
    }
    bool serializeFDesc(MemoryBuffer &mb, IFileDescriptor *fdesc, unsigned channel, bool isLocal) const
    {
        // Find all the partno's that go to this channel
        unsigned numParts = fdesc->numParts();
        if (numParts > 1 && fileType==ROXIE_KEY && isLocal)
            numParts--; // don't want to send TLK
        UnsignedArray partNos;
        for (unsigned i = 1; i <= numParts; i++)
        {
            if (getBondedChannel(i)==channel || !isLocal)
            {
                partNos.append(i-1);
            }
        }
        fdesc->serializeParts(mb, partNos);
        return partNos.length();
    }
    virtual void serializePartial(MemoryBuffer &mb, unsigned channel, bool isLocal) const override
    {
        if (traceLevel > 6)
            DBGLOG("Serializing file information for dynamic file %s, channel %d, local %d", lfn.get(), channel, isLocal);
        byte type = (byte) fileType;
        mb.append(type);
        fileTimeStamp.serialize(mb);
        mb.append(fileCheckSum);
        mb.append(fileSize);
        mb.append(isSuper);
        unsigned numSubFiles = subFiles.length();
        mb.append(numSubFiles);
        ForEachItemIn(idx, subFiles)
        {
            mb.append(subNames.item(idx));
            IFileDescriptor *fdesc = subFiles.item(idx);
            bool anyparts = serializeFDesc(mb, fdesc, channel, isLocal);
            IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
            if (remoteFDesc)
            {
                mb.append(true);
                anyparts |= serializeFDesc(mb, remoteFDesc, channel, isLocal);
            }
            else
                mb.append(false);
            mb.append(formatCrcs.item(idx));
            IOutputMetaData *diskType = diskTypeInfo.item(idx);
            if (anyparts && diskType)
            {
                if (idx && formatCrcs.item(idx)==formatCrcs.item(idx-1))
                    mb.append((byte) 3);  // indicating same format as previous
                else
                {
                    mb.append((byte) (diskType->isGrouped() ? 2 : 1));
                    verifyex(dumpTypeInfo(mb, diskType->queryTypeInfo()));  // Must be serializable, as we deserialized it...
                }
            }
            else
                mb.append((byte) 0);

        }
        if (properties)
        {
            mb.append(true);
            properties->serialize(mb);
        }
        else
            mb.append(false);
    }

    static FileFormatMode getMode(IFileDescriptor *fileDesc)
    {
        if (isFileKey(fileDesc))
            return FileFormatMode::index;
        else
        {
            const char *kind = fileDesc->queryKind();
            if (kind)
            {
                if (streq("csv", kind))
                    return FileFormatMode::csv;
                else if (streq("xml", kind))
                    return FileFormatMode::xml;
                else if (streq("json", kind))
                    return FileFormatMode::xml;   // MORE - is that right?
            }
            return FileFormatMode::flat;
        }
    }

    virtual ITranslatorSet *getTranslators(int projectedFormatCrc, IOutputMetaData *projected, int expectedFormatCrc, IOutputMetaData *expected, RecordTranslationMode mode, FileFormatMode fileMode, const char *queryName) const override
    {
        // NOTE - projected and expected and anything fetched from them such as type info may reside in dynamically loaded (and unloaded)
        // query DLLs - this means it is not safe to include them in any sort of cache that might outlive the current query.
        Owned<CTranslatorSet> result = new CTranslatorSet(expected->queryRecordAccessor(true), projectedFormatCrc);
        Owned<const IDynamicTransform> translator;    // Translates rows from actual to projected
        Owned<const IKeyTranslator> keyedTranslator;  // translate filter conditions from expected to actual
        int prevFormatCrc = 0;
        assertex(projected != nullptr);
        ForEachItemIn(idx, subFiles)
        {
            IFileDescriptor *subFile = subFiles.item(idx);
            IOutputMetaData *actual = expected;
            if (subFile)
            {
                FileFormatMode actualMode = getMode(subFile);
                const char *subname = subNames.item(idx);
                if (fileMode!=actualMode)
                {
                    if (traceLevel>0)
                        DBGLOG("In query %s: Not translating %s as file type does not match", queryName, subname);
                }
                else if (projectedFormatCrc != 0) // projectedFormatCrc is currently 0 for csv/xml which should not create translators.
                {
                    int thisFormatCrc = 0;
                    bool actualUnknown = true;
                    if (mode == RecordTranslationMode::AlwaysECL)
                    {
                        if (formatCrcs.item(idx) && expectedFormatCrc && (formatCrcs.item(idx) != expectedFormatCrc))
                            DBGLOG("Overriding stored record layout reading file %s", subname);
                    }
                    else
                    {
                        thisFormatCrc = formatCrcs.item(idx);
                        if (diskTypeInfo.item(idx))
                        {
                            actual = diskTypeInfo.item(idx);
                            actualUnknown = false;
                        }
                        else if (thisFormatCrc == expectedFormatCrc)  // Type descriptors that cannot be serialized can still be read from code
                        {
                            actual = expected;
                            actualUnknown = false;
                        }
                    }

                    assertex(actual);
                    if ((thisFormatCrc != prevFormatCrc) || (idx == 0))  // Check if same translation as last subfile
                    {
                        translator.clear();
                        keyedTranslator.clear();

                        //Check if the file requires translation, but translation is disabled
                        if (thisFormatCrc && expectedFormatCrc && (thisFormatCrc != expectedFormatCrc) && (mode == RecordTranslationMode::None))
                            throwTranslationError(actual->queryRecordAccessor(true), expected->queryRecordAccessor(true), subname);

                        if (thisFormatCrc == expectedFormatCrc && projectedFormatCrc == expectedFormatCrc && (actualUnknown || alwaysTrustFormatCrcs))
                        {
                            if (traceLevel > 5)
                                DBGLOG("In query %s: Assume no translation required for file %s, crc's match", queryName, subname);
                        }
                        else if (actualUnknown && mode != RecordTranslationMode::AlwaysECL)
                        {
                            if (thisFormatCrc)
                                throw MakeStringException(ROXIE_MISMATCH, "Untranslatable record layout mismatch detected for file %s (disk format not serialized)", subname);
                            else if (traceLevel > 5)
                                DBGLOG("In query %s: Assume no translation required for %s, disk format unknown", queryName, subname);
                        }
                        else
                        {
                            translator.setown(createRecordTranslator(projected->queryRecordAccessor(true), actual->queryRecordAccessor(true)));
                            if (traceLevel>0 && traceTranslations)
                            {
                                DBGLOG("In query %s: Record layout translator created for %s", queryName, subname);
                                translator->describe();
                            }
                            if (!translator || !translator->canTranslate())
                                throw MakeStringException(ROXIE_MISMATCH, "Untranslatable record layout mismatch detected for file %s", subname);
                            else if (translator->needsTranslate())
                            {
                                if (fileMode==FileFormatMode::index && translator->keyedTranslated())
                                    throw MakeStringException(ROXIE_MISMATCH, "Record layout mismatch detected in keyed fields for file %s", subname);
                                keyedTranslator.setown(createKeyTranslator(actual->queryRecordAccessor(true), expected->queryRecordAccessor(true)));
                            }
                            else
                                translator.clear();
                        }
                    }
                    prevFormatCrc = thisFormatCrc;
                }
            }
            else if (traceLevel > 5)
                DBGLOG("In query %s: Assume no translation required, subfile is null", queryName);
            result->addTranslator(LINK(translator), LINK(keyedTranslator), LINK(actual));
        }
        return result.getClear();
    }
    virtual IFileIOArray *getIFileIOArray(bool isOpt, unsigned channel) const
    {
        CriticalBlock b(lock);
        IFileIOArray *ret = ioArrayMap.get(channel);
        if (!ret)
        {
            ret = createIFileIOArray(isOpt, channel);
            ioArrayMap.set(ret, channel);
        }
        return LINK(ret);
    }
    IFileIOArray *createIFileIOArray(bool isOpt, unsigned channel) const
    {
        Owned<CFileIOArray> f = new CFileIOArray;
        f->addFile(nullptr, 0, 0, nullptr, 0);
        ForEachItemIn(idx, subFiles)
        {
            IFileDescriptor *fdesc = subFiles.item(idx);
            IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
            const char *subname = subNames.item(idx);
            int thisFormatCrc = formatCrcs.item(idx);
            if (fdesc)
            {
                unsigned numParts = fdesc->numParts();
                for (unsigned i = 1; i <= numParts; i++)
                {
                    if (!channel || getBondedChannel(i)==channel)
                    {
                        try
                        {
                            IPartDescriptor *pdesc = fdesc->queryPart(i-1);
                            assertex(pdesc);
                            IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, i-1);
                            Owned<ILazyFileIO> file = createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_FILE, numParts, cached != NULL, channel);
                            IPropertyTree &partProps = pdesc->queryProperties();
                            f->addFile(file.getClear(), partProps.getPropInt64("@offset"), idx, subname, thisFormatCrc);
                        }
                        catch (IException *E)
                        {
                            StringBuffer err;
                            err.append("Could not load file ");
                            fdesc->getTraceName(err);
                            DBGLOG(E, err.str());
                            if (!isOpt)
                                throw;
                            E->Release();
                            f->addFile(nullptr, 0, idx, nullptr, 0);
                        }
                    }
                    else
                        f->addFile(nullptr, 0, idx, nullptr, 0);
                }
            }
        }
        return f.getClear();
    }

    virtual IKeyArray *getKeyArray(bool isOpt, unsigned channel) const override
    {
        unsigned maxParts = 0;
        ForEachItemIn(subFile, subFiles)
        {
            IFileDescriptor *fdesc = subFiles.item(subFile);
            if (fdesc)
            {
                unsigned numParts = fdesc->numParts();
                if (numParts > 1)
                    numParts--; // Don't include TLK
                if (numParts > maxParts)
                    maxParts = numParts;
            }
        }
        CriticalBlock b(lock);
        IKeyArray *ret = keyArrayMap.get(channel);
        if (!ret)
        {
            ret = createKeyArray(isOpt, channel, maxParts);
            keyArrayMap.set(ret, channel);
        }
        return LINK(ret);
    }
    IKeyArray *createKeyArray(bool isOpt, unsigned channel, unsigned maxParts) const
    {
        Owned<IKeyArray> ret = ::createKeyArray();
        if (channel)
        {
            ret->addKey(NULL);
            for (unsigned partNo = 1; partNo <= maxParts; partNo++)
            {
                if (channel == getBondedChannel(partNo))
                {
                    Owned<IKeyIndexSet> keyset = createKeyIndexSet();
                    ForEachItemIn(idx, subFiles)
                    {
                        IFileDescriptor *fdesc = subFiles.item(idx);
                        IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);

                        Owned <ILazyFileIO> part;
                        unsigned crc = 0;
                        if (fdesc) // NB there may be no parts for this channel 
                        {
                            IPartDescriptor *pdesc = fdesc->queryPart(partNo-1);
                            if (pdesc)
                            {
                                IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, partNo-1);
                                part.setown(createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, fdesc->numParts(), cached != NULL, channel));
                                pdesc->getCrc(crc);
                            }
                        }
                        if (part)
                        {
                            if (lazyOpen)
                            {
                                // We pass the IDelayedFile interface to createKeyIndex, so that it does not open the file immediately
                                keyset->addIndex(createKeyIndex(part->queryFilename(), crc, *QUERYINTERFACE(part.get(), IDelayedFile), part->getFileIdx(), false, false));
                            }
                            else
                                keyset->addIndex(createKeyIndex(part->queryFilename(), crc, *part.get(), part->getFileIdx(), false, false));
                        }
                        else
                            keyset->addIndex(NULL);
                    }
                    ret->addKey(keyset.getClear());
                }
                else
                    ret->addKey(NULL);
            }
        }
        else
        {
            // Channel 0 means return the TLK
            Owned<IKeyIndexSet> keyset = createKeyIndexSet();
            ForEachItemIn(idx, subFiles)
            {
                IFileDescriptor *fdesc = subFiles.item(idx);
                IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
                Owned<IKeyIndexBase> key;
                if (fdesc)
                {
                    unsigned numParts = fdesc->numParts();
                    assertex(numParts > 0);
                    IPartDescriptor *pdesc = fdesc->queryPart(numParts - 1);
                    IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, numParts - 1);
                    Owned<ILazyFileIO> keyFile = createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, numParts, cached != NULL, channel);
                    unsigned crc = 0;
                    pdesc->getCrc(crc);
                    StringBuffer pname;
                    pdesc->getPath(pname);
                    if (lazyOpen)
                    {
                        // We pass the IDelayedFile interface to createKeyIndex, so that it does not open the file immediately
                        key.setown(createKeyIndex(pname.str(), crc, *QUERYINTERFACE(keyFile.get(), IDelayedFile), keyFile->getFileIdx(), numParts>1, false));
                    }
                    else
                        key.setown(createKeyIndex(pname.str(), crc, *keyFile.get(), keyFile->getFileIdx(), numParts>1, false));
                    keyset->addIndex(LINK(key->queryPart(0)));
                }
                else
                    keyset->addIndex(NULL);
            }
            if (keyset->numParts())
                ret->addKey(keyset.getClear());
            else if (!isOpt)
                throw MakeStringException(ROXIE_FILE_ERROR, "Key %s has no key parts", lfn.get());
            else if (traceLevel > 4)
                DBGLOG(ROXIE_OPT_REPORTING, "Key %s has no key parts", lfn.get());
        }
        return ret.getClear();
    }

    virtual IInMemoryIndexManager *getIndexManager(bool isOpt, unsigned channel, IOutputMetaData *preloadLayout, bool preload) const
    {
        // MORE - I don't know that it makes sense to pass isOpt in to these calls
        // Failures to resolve will not be cached, only successes.
        // MORE - preload and numkeys are all messed up - can't be specified per query have to be per file

        CriticalBlock b(lock);
        IInMemoryIndexManager *ret = indexMap.get(channel);
        if (!ret)
        {
            ret = createInMemoryIndexManager(preloadLayout->queryRecordAccessor(true), isOpt, lfn);
            Owned<IFileIOArray> files = getIFileIOArray(isOpt, channel);
            ret->load(files, preloadLayout, preload);   // note - files (passed in) are also channel specific
            indexMap.set(ret, channel);
        }
        return LINK(ret);

    }

    virtual const CDateTime &queryTimeStamp() const
    {
        return fileTimeStamp;
    }

    virtual unsigned queryCheckSum() const
    {
        return fileCheckSum;
    }

    virtual offset_t getFileSize() const
    {
        return fileSize;
    }

    virtual hash64_t addHash64(hash64_t hashValue) const
    {
        hashValue = fileTimeStamp.getHash(hashValue);
        if (fileCheckSum)
            hashValue = rtlHash64Data(sizeof(fileCheckSum), &fileCheckSum, hashValue);
        return hashValue;
    }

    virtual void addSubFile(const IResolvedFile *_sub)
    {
        const CResolvedFile *sub = static_cast<const CResolvedFile *>(_sub);
        if (subFiles.length())
            assertex(sub->fileType==fileType);
        else
            fileType = sub->fileType;
        subRFiles.append((IResolvedFile &) *LINK(_sub));
        ForEachItemIn(idx, sub->subFiles)
        {
            addFile(sub->subNames.item(idx), LINK(sub->subFiles.item(idx)), LINK(sub->remoteSubFiles.item(idx)));
        }
    }
    virtual void addSubFile(IFileDescriptor *_sub, IFileDescriptor *_remoteSub)
    {
        addFile(lfn, _sub, _remoteSub);
    }
    virtual void addSubFile(const char *localFileName)
    {
        Owned<IFile> file = createIFile(localFileName);
        assertex(file->exists());

        offset_t size = file->size();
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        if (isIndexFile(file))
            fdesc->queryProperties().setProp("@kind", "key");
        Owned<IPropertyTree> pp = createPTree("Part", ipt_lowmem);
        pp->setPropInt64("@size",size);
        pp->setPropBool("@local", true);
        fdesc->setPart(0, queryMyNode(), localFileName, pp);
        addSubFile(fdesc.getClear(), NULL);
    }

    virtual void setCache(IResolvedFileCache *cache)
    {
        if (cached)
        {
            if (traceLevel > 9)
                DBGLOG("setCache removing from prior cache %s", queryFileName());
            if (cache==NULL)
                cached->removeCache(this);
            else
                throwUnexpected();
        }
        cached = cache;
    }

    virtual bool isAliveAndLink() const
    {
        return CInterface::isAliveAndLink();
    }
    virtual const char *queryFileName() const
    {
        return lfn.get();
    }
    virtual const char *queryPhysicalName() const
    {
        return physicalName.get();
    }
    virtual const IPropertyTree *queryProperties() const
    {
        return properties;
    }
    virtual void remove()
    {
        subFiles.kill();
        subDFiles.kill();
        subRFiles.kill();
        subNames.kill();
        remoteSubFiles.kill();
        properties.clear();
        notifier.clear();
        if (isSuper)
        {
            // Because we don't lock superfiles, we need to behave differently
            UNIMPLEMENTED;
        }
        else if (dFile)
        {
            dFile->detach();
        }
        else if (!physicalName.isEmpty())
        {
            try
            {
                Owned<IFile> file = createIFile(physicalName.get());
                file->remove();
            }
            catch (IException *e)
            {
                OERRLOG(-1, "Error removing file %s (%s)", lfn.get(), physicalName.get());
                e->Release();
            }
        }
    }
    virtual bool exists() const
    {
        // MORE - this is a little bizarre. We sometimes create a resolvedFile for a file that we are intending to create.
        // This will make more sense if/when we start to lock earlier.
        if (dFile || isSuper)
            return true; // MORE - may need some thought - especially the isSuper case
        else if (!physicalName.isEmpty())
            return checkFileExists(physicalName.get());
        else
            return false;
    }
    virtual bool isRestrictedAccess() const override
    {
        return (dFile && dFile->isRestrictedAccess());
    }
};


/*----------------------------------------------------------------------------------------------------------
MORE
    - on remote() calls we can't pass the expected file date but we will pass it back with the file info.
------------------------------------------------------------------------------------------------------------*/

class CAgentDynamicFile : public CResolvedFile
{
public:
    bool isOpt; // MORE - this is not very good. Needs some thought unless you cache opt / nonOpt separately which seems wasteful
    bool isLocal;
    unsigned channel;
    ServerIdentifier serverId;

public:
    CAgentDynamicFile(const IRoxieContextLogger &logctx, const char *_lfn, RoxiePacketHeader *header, bool _isOpt, bool _isLocal)
        : CResolvedFile(_lfn, NULL, NULL, ROXIE_FILE, NULL, true, false, false, false), isOpt(_isOpt), isLocal(_isLocal), channel(header->channel), serverId(header->serverId)
    {
        // call back to the server to get the info
        IPendingCallback *callback = ROQ->notePendingCallback(*header, lfn); // note that we register before the send to avoid a race.
        try
        {
            RoxiePacketHeader newHeader(*header, ROXIE_FILECALLBACK, 0);  // subchannel not relevant
            bool ok = false;
            for (unsigned i = 0; i < callbackRetries; i++)
            {
                Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                unsigned len = strlen(lfn)+3; // 1 for isOpt, 1 for isLocal, 1 for null terminator
                char *buf = (char *) output->getBuffer(len, true);
                buf[0] = isOpt;
                buf[1] = isLocal;
                strcpy(buf+2, lfn.get());
                output->putBuffer(buf, len, true);
                output->flush();
                output.clear();
                if (callback->wait(callbackTimeout))
                {
                    ok = true;
                    break;
                }
                else
                {
                    DBGLOG("timed out waiting for server callback - retrying");
                }
            }
            if (ok)
            {
                if (traceLevel > 6)
                    { StringBuffer s; DBGLOG("Processing information from server in response to %s", newHeader.toString(s).str()); }

                MemoryBuffer &serverData = callback->queryData();
                byte type;
                serverData.read(type);
                fileType = (RoxieFileType) type;
                fileTimeStamp.deserialize(serverData);
                serverData.read(fileCheckSum);
                serverData.read(fileSize);
                serverData.read(isSuper);
                unsigned numSubFiles;
                serverData.read(numSubFiles);
                for (unsigned fileNo = 0; fileNo < numSubFiles; fileNo++)
                {
                    StringBuffer subName;
                    serverData.read(subName);
                    subNames.append(subName.str());
                    deserializeFilePart(serverData, subFiles, fileNo, false);
                    bool remotePresent;
                    serverData.read(remotePresent);
                    if (remotePresent)
                        deserializeFilePart(serverData, remoteSubFiles, fileNo, true);
                    else
                        remoteSubFiles.append(NULL);
                    unsigned formatCrc;
                    serverData.read(formatCrc);
                    formatCrcs.append(formatCrc);
                    byte diskTypeInfoPresent;
                    serverData.read(diskTypeInfoPresent);
                    switch (diskTypeInfoPresent)
                    {
                    case 0:
                        diskTypeInfo.append(NULL);
                        break;
                    case 1:
                        diskTypeInfo.append(createTypeInfoOutputMetaData(serverData, false));
                        break;
                    case 2:
                        diskTypeInfo.append(createTypeInfoOutputMetaData(serverData, true));
                        break;
                    case 3:
                        assertex(fileNo > 0);
                        diskTypeInfo.append(LINK(diskTypeInfo.item(fileNo-1)));
                        break;
                    default:
                        throwUnexpected();
                    }
                }
                bool propertiesPresent;
                serverData.read(propertiesPresent);
                if (propertiesPresent)
                    properties.setown(createPTree(serverData, ipt_lowmem));
            }
            else
                throw MakeStringException(ROXIE_CALLBACK_ERROR, "Failed to get response from server for dynamic file callback");
        }
        catch (...)
        {
            ROQ->removePendingCallback(callback);
            throw;
        }
        ROQ->removePendingCallback(callback);
    }
private:
    void deserializeFilePart(MemoryBuffer &serverData, IPointerArrayOf<IFileDescriptor> &files, unsigned fileNo, bool remote)
    {
        IArrayOf<IPartDescriptor> parts;
        deserializePartFileDescriptors(serverData, parts);
        if (parts.length())
        {
            files.append(LINK(&parts.item(0).queryOwner()));
        }
        else
        {
            if (traceLevel > 6)
                DBGLOG("No information for %s subFile %d of file %s", remote ? "remote" : "", fileNo, lfn.get());
            files.append(NULL);
        }
    }
};

extern IResolvedFileCreator *createResolvedFile(const char *lfn, const char *physical, bool isSuperFile)
{
    return new CResolvedFile(lfn, physical, NULL, ROXIE_FILE, NULL, true, false, false, isSuperFile);
}

extern IResolvedFile *createResolvedFile(const char *lfn, const char *physical, IDistributedFile *dFile, IRoxieDaliHelper *daliHelper, bool isDynamic, bool cacheIt, bool writeAccess)
{
    const char *kind = dFile ? dFile->queryAttributes().queryProp("@kind") : NULL;
    return new CResolvedFile(lfn, physical, dFile, kind && stricmp(kind, "key")==0 ? ROXIE_KEY : ROXIE_FILE, daliHelper, isDynamic, cacheIt, writeAccess, false);
}

class CAgentDynamicFileCache : implements IAgentDynamicFileCache, public CInterface
{
    unsigned tableSize;
    mutable CriticalSection crit;
    CIArrayOf<CAgentDynamicFile> files; // expect numbers to be small - probably not worth hashing

public:
    IMPLEMENT_IINTERFACE;
    CAgentDynamicFileCache(unsigned _limit) : tableSize(_limit) {}

    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, unsigned checksum, RoxiePacketHeader *header, bool isOpt, bool isLocal) override
    {
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s;
            logctx.CTXLOG("lookupDynamicFile %s for packet %s", lfn, header->toString(s).str());
        }
        // we use a fixed-size array with linear lookup for ease of initial coding - but unless we start making heavy use of the feature this may be adequate.
        CriticalBlock b(crit);
        if (!cacheDate.isNull())
        {
            unsigned idx = 0;
            while (files.isItem(idx))
            {
                CAgentDynamicFile &f = files.item(idx);
                if (f.channel==header->channel && f.serverId==header->serverId && stricmp(f.queryFileName(), lfn)==0)
                {
                    if (!cacheDate.equals(f.queryTimeStamp()) || checksum != f.queryCheckSum())
                    {
                        if (f.isKey())
                            clearKeyStoreCacheEntry(f.queryFileName());
                        files.remove(idx);
                        idx--;
                    }
                    else if ((!f.isLocal || isLocal) && f.isOpt==isOpt)
                    {
                        files.swap(idx, 0);
                        return LINK(&f);
                    }
                }
                idx++;
            }
        }
        Owned<CAgentDynamicFile> ret;
        {
            // Don't prevent access to the cache while waiting for server to reply. Can deadlock if you do, apart from being inefficient
            CriticalUnblock b1(crit);
            ret.setown(new CAgentDynamicFile(logctx, lfn, header, isOpt, isLocal));
        }
        if (!ret->isSuperFile())
        {
            // Cache results for improved performance - we DON'T cache superfiles as they are liable to change during the course of a query.
            // Note that even caching non-superfiles is also potentially going to give stale results, if the cache persists beyond the current
            // query.
            while (files.length() > tableSize)
                files.remove(files.length()-1);
            files.add(*ret.getLink(), 0);
        }
        return ret.getClear();
    }

    virtual void releaseAll() override
    {
        CriticalBlock b(crit);
        files.kill();
    }
};

static CriticalSection agentDynamicFileCacheCrit;
static Owned<IAgentDynamicFileCache> agentDynamicFileCache;

extern IAgentDynamicFileCache *queryAgentDynamicFileCache()
{
    if (!agentDynamicFileCache)
    {
        CriticalBlock b(agentDynamicFileCacheCrit);
        if (!agentDynamicFileCache)
            agentDynamicFileCache.setown(new CAgentDynamicFileCache(20));
    }
    return agentDynamicFileCache;
}

extern void releaseAgentDynamicFileCache()
{
    CriticalBlock b(agentDynamicFileCacheCrit);
    if (agentDynamicFileCache)
        agentDynamicFileCache->releaseAll();
}

static Singleton<CRoxieFileCache> fileCache;

// Initialization/termination
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{ 
    auto cache = fileCache.queryExisting();
    if (cache)
    {
        cache->join();
        cache->Release();
    }
}

extern IRoxieFileCache &queryFileCache()
{
    return *fileCache.query([] { return new CRoxieFileCache; });
}

class CRoxieWriteHandler : implements IRoxieWriteHandler, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieWriteHandler(IRoxieDaliHelper *_daliHelper, ILocalOrDistributedFile *_dFile, const StringArray &_clusters)
    : daliHelper(_daliHelper), dFile(_dFile)
    {
        ForEachItemIn(idx, _clusters)
        {
            addCluster(_clusters.item(idx));
        }
        if (dFile->queryDistributedFile())
        {
            isTemporary = (localCluster.get() == NULL); // if only writing to remote clusters, write to a temporary first, then copy
            if (isTemporary)
            {
                UNIMPLEMENTED;
            }
            else
                localFile.setown(dFile->getPartFile(0, 0));
        }
        else
        {
            isTemporary = false;
            localFile.setown(dFile->getPartFile(0, 0));
        }
        if (!recursiveCreateDirectoryForFile(localFile->queryFilename()))
            throw MakeStringException(ROXIE_FILE_ERROR, "Cannot create directory for file %s", localFile->queryFilename());
    }
    virtual IFile *queryFile() const
    {
        return localFile;
    }
    void getClusters(StringArray &clusters) const
    {
        ForEachItemIn(idx, allClusters)
        {
            clusters.append(allClusters.item(idx));
        }
    }

    virtual void finish(bool success, const IRoxiePublishCallback *activity)
    {
        if (success)
        {
            copyPhysical();
            if (daliHelper && daliHelper->connected())
                publish(activity);
        }
        if (isTemporary || !success)
        {
            localFile->remove();
        }
    }
private:
    bool isTemporary;
    Linked<IRoxieDaliHelper> daliHelper;
    Owned<ILocalOrDistributedFile> dFile;
    Owned<IFile> localFile;
    Owned<IGroup> localCluster;
    StringAttr localClusterName;
    IArrayOf<IGroup> remoteNodes;
    StringArray allClusters;

    void copyPhysical() const
    {
        RemoteFilename rfn, rdn;
        dFile->getPartFilename(rfn, 0, 0);
        StringBuffer physicalName, physicalDir, physicalBase;
        rfn.getLocalPath(physicalName);
        splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
        rdn.setLocalPath(physicalDir.str());
        if (remoteNodes.length())
        {
            ForEachItemIn(idx, remoteNodes)
            {
                rdn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
                rfn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
                Owned<IFile> targetdir = createIFile(rdn);
                Owned<IFile> target = createIFile(rfn);
                targetdir->createDirectory();
                copyFile(target, localFile);
            }
        }
    }

    void publish(const IRoxiePublishCallback *activity)
    {
        if (!dFile->isExternal())
        {
            Owned<IFileDescriptor> desc = createFileDescriptor();
            desc->setNumParts(1);

            RemoteFilename rfn;
            dFile->getPartFilename(rfn, 0, 0);
            StringBuffer physicalName, physicalDir, physicalBase;
            rfn.getLocalPath(physicalName);
            splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
            desc->setDefaultDir(physicalDir.str());
            desc->setPartMask(physicalBase.str());

            IPropertyTree &partProps = desc->queryPart(0)->queryProperties(); //properties of the first file part.
            IPropertyTree &fileProps = desc->queryProperties(); // properties of the logical file
            offset_t fileSize = localFile->size();
            fileProps.setPropInt64("@size", fileSize);
            partProps.setPropInt64("@size", fileSize);

            CDateTime createTime, modifiedTime, accessedTime;
            localFile->getTime(&createTime, &modifiedTime, &accessedTime);
            // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
            unsigned hour, min, sec, nanosec;
            modifiedTime.getTime(hour, min, sec, nanosec);
            modifiedTime.setTime(hour, min, sec, 0);
            StringBuffer timestr;
            modifiedTime.getString(timestr);
            if(timestr.length())
                partProps.setProp("@modified", timestr.str());

            ClusterPartDiskMapSpec partmap;
            if (localCluster)
            {
                desc->addCluster(localCluster, partmap);
                desc->setClusterGroupName(0, localClusterName.get());
            }
            ForEachItemIn(idx, remoteNodes)
                desc->addCluster(&remoteNodes.item(idx), partmap);

            if (activity)
                activity->setFileProperties(desc);

            Owned<IDistributedFile> publishFile = queryDistributedFileDirectory().createNew(desc); // MORE - we'll create this earlier if we change the locking paradigm
            publishFile->setAccessedTime(modifiedTime);
            IUserDescriptor * userdesc = NULL;
            if (activity)
                userdesc = activity->queryUserDescriptor();
            else
            {
                Owned<IRoxieDaliHelper> daliHelper = connectToDali(false);
                if (daliHelper)
                    userdesc = daliHelper->queryUserDescriptor();//predeployed query mode
            }

            publishFile->attach(dFile->queryLogicalName(), userdesc);
            // MORE should probably write to the roxielocalstate too in case Dali is down next time I look...
        }
    }

    void addCluster(char const * cluster)
    {
        Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
        if (!group)
            throw MakeStringException(0, "Unknown cluster %s while writing file %s",
                    cluster, dFile->queryLogicalName());
#ifdef _CONTAINERIZED // NB: really is-off-nodestorage
        localCluster.setown(group.getClear());
        localClusterName.set(cluster);
#else
        rank_t r = group->rank();
        if (RANK_NULL != r)
        {
            if (localCluster)
                throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                        cluster, dFile->queryLogicalName());
            SocketEndpointArray eps;
            SocketEndpoint me(0, myNode.getNodeAddress());
            eps.append(me);
            localCluster.setown(createIGroup(eps));
            StringBuffer clusterName(cluster);
            if (group->ordinality()>1)
                clusterName.appendf("[%u]", r+1);
            localClusterName.set(clusterName);
        }
        else
        {
            ForEachItemIn(idx, remoteNodes)
            {
                Owned<INode> other = remoteNodes.item(idx).getNode(0);
                if (group->isMember(other))
                    throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                            cluster, dFile->queryLogicalName());
            }
            remoteNodes.append(*group.getClear());
        }
#endif
        allClusters.append(cluster);
    }
};

extern IRoxieWriteHandler *createRoxieWriteHandler(IRoxieDaliHelper *_daliHelper, ILocalOrDistributedFile *_dFile, const StringArray &_clusters)
{
    return new CRoxieWriteHandler(_daliHelper, _dFile, _clusters);
}

//================================================================================================================

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class CcdFileTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CcdFileTest);
        CPPUNIT_TEST(testCopy);
    CPPUNIT_TEST_SUITE_END();
protected:

    class DummyPartDescriptor : public CInterfaceOf<IPartDescriptor>
    {
        virtual unsigned queryPartIndex() { UNIMPLEMENTED; }

        virtual unsigned numCopies() { UNIMPLEMENTED; }
        virtual INode *getNode(unsigned copy=0) { UNIMPLEMENTED; }
        virtual INode *queryNode(unsigned copy=0) { UNIMPLEMENTED; }

        virtual IPropertyTree &queryProperties() { UNIMPLEMENTED; }
        virtual IPropertyTree *getProperties() { UNIMPLEMENTED; }

        virtual RemoteFilename &getFilename(unsigned copy, RemoteFilename &rfn) { UNIMPLEMENTED; }
        virtual StringBuffer &getTail(StringBuffer &name) { UNIMPLEMENTED; }
        virtual StringBuffer &getDirectory(StringBuffer &name,unsigned copy = 0) { UNIMPLEMENTED; }
        virtual StringBuffer &getPath(StringBuffer &name,unsigned copy = 0) { UNIMPLEMENTED; }

        virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }

        virtual bool isMulti() { UNIMPLEMENTED; }
        virtual RemoteMultiFilename &getMultiFilename(unsigned copy, RemoteMultiFilename &rfn) { UNIMPLEMENTED; }

        virtual bool getCrc(unsigned &crc) { UNIMPLEMENTED; }
        virtual IFileDescriptor &queryOwner() { UNIMPLEMENTED; }
        virtual const char *queryOverrideName() { UNIMPLEMENTED; }
        virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL) { UNIMPLEMENTED; }
        virtual IReplicatedFile *getReplicatedFile() { UNIMPLEMENTED; }
    };

    void testCopy()
    {
        selfTestMode = true;
        remove("test.local");
        remove("test.remote");
        remove("test.buddy");
        StringArray remotes;
        DummyPartDescriptor pdesc;
        CDateTime dummy;
        remotes.append("test.remote");

        int f = open("test.remote", _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        CPPUNIT_ASSERT(f >= 0);
        int val = 1;
        int wrote = write(f, &val, sizeof(int));
        CPPUNIT_ASSERT(wrote==sizeof(int));
        close(f);
        CRoxieFileCache &cache = static_cast<CRoxieFileCache &>(queryFileCache());

        Owned<ILazyFileIO> io = cache.openFile("test.local", 0, 0, "test.local", NULL, remotes, sizeof(int), dummy);
        CPPUNIT_ASSERT(io != NULL);

        // Reading it should read 1
        val = 0;
        ssize_t bytesRead = io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(bytesRead==4);
        CPPUNIT_ASSERT(val==1);

        // Now create the buddy

        f = open("test.buddy", _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        val = 2;
        ssize_t numwritten = write(f, &val, sizeof(int));
        CPPUNIT_ASSERT(numwritten == sizeof(int));
        close(f);

        // Reading it should still read 1...
        val = 0;
        io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(val==1);

        // Now copy it - should copy the buddy
        cache.doCopy(io, false, false);

        // Reading it should read 2...
        val = 0;
        io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(val==2);

        // And the data in the file should be 2
        f = open("test.local", _O_RDONLY);
        val = 0;
        ssize_t numread = read(f, &val, sizeof(int));
        CPPUNIT_ASSERT(numread == sizeof(int));
        close(f);
        CPPUNIT_ASSERT(val==2);

        io.clear();
        remove("test.local");
        remove("test.remote");
        remove("test.buddy");
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( CcdFileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CcdFileTest, "CcdFileTest" );

#endif
