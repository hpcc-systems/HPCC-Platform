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

#include <deque>
#include <queue>
#include <tuple>
#include <memory>
#include "platform.h"
#include <limits.h>
#include <stddef.h>
#include "jlib.hpp"
#include "jqueue.hpp"
#include "jmisc.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsem.hpp"
#include "jthread.hpp"
#include "jarray.hpp"
#include "jiface.hpp"
#include "jfile.hpp"
#include "jset.hpp"
#include "jqueue.tpp"

#include "thmem.hpp"
#include "thalloc.hpp"
#include "thbuf.hpp"
#include "eclrtl.hpp"
#include "thgraph.hpp"

#ifdef _DEBUG
//#define _FULL_TRACE
//#define TRACE_CREATE

struct SmartBufReenterCheck
{
    bool &check; 
    SmartBufReenterCheck(bool &_check) 
        : check(_check)
    {
        assertex(!check);
        check = true;
    }
    ~SmartBufReenterCheck()
    {
        assertex(check);
        check = false;
    }
};

#define REENTRANCY_CHECK(n) SmartBufReenterCheck recheck(n);
#else
#define REENTRANCY_CHECK(n)
#endif



class CSmartRowBuffer: public CSimpleInterface, implements ISmartRowBuffer, implements IRowWriter
{
    CActivityBase *activity;
    ThorRowQueue *in;
    size32_t insz;
    ThorRowQueue *out;
    CFileOwner tmpFileOwner;
    Owned<IFileIO> tempFileIO;
    SpinLock lock;
    bool waiting;
    Semaphore waitsem;
    bool waitflush;
    Semaphore waitflushsem;
    size32_t blocksize;
    unsigned numblocks;
    Owned<IBitSet> diskfree;
    UnsignedArray diskin;
    UnsignedArray diskinlen;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<IEngineRowAllocator> allocator;
    bool eoi;
#ifdef _DEBUG
    bool putrecheck;
    bool getrecheck;
#endif

    unsigned allocblk(unsigned nb)
    {
        if (nb<=numblocks) {
            for (unsigned i=0;i<=numblocks-nb;i++) {
                unsigned j;
                for (j=i;j<i+nb;j++) 
                    if (!diskfree->test(j))
                        break;
                if (j==i+nb) {
                    while (j>i)
                        diskfree->set(--j,false);
                    return i;
                }
            }
        }
        unsigned ret = numblocks;
        numblocks += nb;
        return ret;
    }

    void freeblk(unsigned b,unsigned nb)
    {
        for (unsigned i=0;i<nb;i++)
            diskfree->set(i+b,true);
    }

    void diskflush()
    {
        struct cSaveIn {
            ThorRowQueue *rq;
            ThorRowQueue *&in;
            cSaveIn(ThorRowQueue *&_in) 
                : in(_in)
            {
                rq = in;
                in = NULL;
            }
            ~cSaveIn() 
            {
                if (!in)
                    in = rq;
            }
        } csavein(in); // mark as not there so consumer will pause
        if (out && (out->ordinality()==0) && (diskin.ordinality()==0)) {
            in = out;
            out = csavein.rq;
            insz = 0;
            return;
        }
        if (!tempFileIO) {
            SpinUnblock unblock(lock);
            tempFileIO.setown(tmpFileOwner.queryIFile().open(IFOcreaterw));
            if (!tempFileIO)
            {
                throw MakeStringException(-1,"CSmartRowBuffer::flush cannot write file %s", tmpFileOwner.queryIFile().queryFilename());
            }
        }
        MemoryBuffer mb;
        CMemoryRowSerializer mbs(mb);
        unsigned nb = 0;
        unsigned blk = 0;
        if (!waiting) {
            mb.ensureCapacity(blocksize);
            {
                SpinUnblock unblock(lock);
                byte b;
                for (unsigned i=0;i<csavein.rq->ordinality();i++) {
                    if (waiting)
                        break;
                    const void *row = csavein.rq->item(i);
                    if (row) { 
                        b = 1;
                        mb.append(b);
                        serializer->serialize(mbs,(const byte *)row);
                    }
                    else {
                        b = 2; // eog
                        mb.append(b);
                    }
                }
                b = 0;
                mb.append(b);
            }
        }
        if (!waiting) {
            nb = (mb.length()+blocksize-1)/blocksize;
            blk = allocblk(nb);
            SpinUnblock unblock(lock);
            if (mb.length()<nb*blocksize) { // bit overkill!
                size32_t left = nb*blocksize-mb.length();
                memset(mb.reserve(left),0,left);
            }
            tempFileIO->write(blk*(offset_t)blocksize,mb.length(),mb.bufferBase());
            tmpFileOwner.noteSize(numblocks*blocksize);
            mb.clear();
        }
        if (waiting) {
            // if eoi, stopped while serializing/writing, putRow may put final row to existing 'in'
            if (!eoi)
            {
                // consumer caught up
                freeblk(blk,nb);
                assertex(out->ordinality()==0);
                assertex(diskin.ordinality()==0);
                in = out;
                out = csavein.rq;
            }
        }
        else {
            diskin.append(blk);
            diskinlen.append(nb);
            while (csavein.rq->ordinality()) 
                ReleaseThorRow(csavein.rq->dequeue());
        }
        insz = 0;
    }

    void load()
    {
        ThorRowQueue *rq = out;
        out = NULL; // mark as not there so producer won't fill
        unsigned blk = diskin.item(0);
        unsigned nb = diskinlen.item(0);
        diskin.remove(0);  // better as q but given reading from disk...
        diskinlen.remove(0);  
        {
            SpinUnblock unblock(lock);
            MemoryAttr ma;
            size32_t readBlockSize = nb*blocksize;
            byte *buf = (byte *)ma.allocate(readBlockSize);
            CThorStreamDeserializerSource ds(readBlockSize,buf);
            assertex(tempFileIO.get());
            size32_t rd = tempFileIO->read(blk*(offset_t)blocksize,readBlockSize,buf);
            assertex(rd==readBlockSize);
            for (;;) {
                byte b;
                ds.read(sizeof(b),&b);
                if (!b)
                    break;
                if (b==1) {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    size32_t sz = deserializer->deserialize(rowBuilder,ds); 
                    rq->enqueue(rowBuilder.finalizeRowClear(sz));
                }
                else if (b==2)
                    rq->enqueue(NULL);

            }
        }
        freeblk(blk,nb);
        out = rq;
    }


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSmartRowBuffer(CActivityBase *_activity,IFile *_file,size32_t bufsize,IThorRowInterfaces *rowif)
        : activity(_activity), tmpFileOwner(_file, _activity->queryTempFileSizeTracker()), allocator(rowif->queryRowAllocator()), serializer(rowif->queryRowSerializer()), deserializer(rowif->queryRowDeserializer())
    {
#ifdef _DEBUG
        putrecheck = false;
        getrecheck = false;
#endif
        in = new ThorRowQueue;
        out = new ThorRowQueue;
        waiting = false;
        waitflush = false;
        blocksize = ((bufsize/2+0xfffff)/0x100000)*0x100000;
        numblocks = 0;
        insz = 0;
        eoi = false;
        diskfree.setown(createThreadSafeBitSet());

#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer create %x",(unsigned)(memsize_t)this);
#endif
    }

    ~CSmartRowBuffer()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer destroy %x",(unsigned)(memsize_t)this);
#endif
        assertex(!waiting);
        assertex(!waitflush);
        // clear in/out contents
        while (in->ordinality())
            ReleaseThorRow(in->dequeue());
        delete in;
        while (out->ordinality())
            ReleaseThorRow(out->dequeue());
        delete out;
        tempFileIO.clear();
    }

    void putRow(const void *row)
    {
        REENTRANCY_CHECK(putrecheck)
        size32_t sz = thorRowMemoryFootprint(serializer, row);
        SpinBlock block(lock);
        if (eoi) {
            ReleaseThorRow(row);
            
            return;
        }       
        assertex(in);  // reentry check
        if (sz+insz+(in->ordinality()+2)*sizeof(byte)>blocksize) // byte extra per row + end byte
            diskflush();
        in->enqueue(row);
        insz += sz;
        if (waiting) {
            waitsem.signal();
            waiting = false;
        }
    }

    void stop()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer stop %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
#ifdef _DEBUG
        if (waiting)
        {
            ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowBuffer::stop while nextRow waiting");
            PrintStackReport();
        }
#endif
        eoi = true;

        while (out&&out->ordinality()) 
            ReleaseThorRow(out->dequeue());
        while (NULL == in)
        {
            waiting = true;
            SpinUnblock unblock(lock);
            waitsem.wait();
        }
        while (out&&out->ordinality()) 
            ReleaseThorRow(out->dequeue());
        while (in&&in->ordinality()) 
            ReleaseThorRow(in->dequeue());
        diskin.kill();
        if (waiting) {
            waitsem.signal();
            waiting = false;
        }
        if (waitflush) {
            waitflushsem.signal();
            waitflush = false;
        }
    }

    const void *nextRow()
    {
        REENTRANCY_CHECK(getrecheck)
        const void * ret;
        assertex(out);
        assertex(!waiting);  // reentrancy checks
        for (;;) {
            {
                SpinBlock block(lock);
                if (out->ordinality()) {
                    ret = out->dequeue();
                    break;
                }
                if (diskin.ordinality()) {
                    load();
                    ret = out->dequeue();
                    break;
                }
                if (in) {
                    if (in->ordinality()) {
                        ret = in->dequeue();
                        if (ret) {
                            size32_t sz = thorRowMemoryFootprint(serializer, ret);
                            assertex(insz>=sz);
                            insz -= sz;
                        }
                        break;
                    }
                    else {
                        if (waitflush) {
                            waitflushsem.signal();
                            waitflush = false;
                        }
                        if (eoi) 
                            return NULL;
                    }
                }
                assertex(!waiting);  // reentrancy check
                waiting = true;
            }
            waitsem.wait();
        }
        return ret;
    }

    void flush()
    {
        // I think flush should wait til all rows read or stopped
#ifdef _FULL_TRACE
        ActPrintLog(activity, "SmartBuffer flush %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
        if (eoi) return;
        for (;;) {
            assertex(in);  // reentry check
            diskflush();
            eoi = true;
            if (waiting) {
                waitsem.signal();
                waiting = false;
            }
            if (out&&!out->ordinality()&&!diskin.ordinality()&&!in->ordinality()) 
                break;
            waitflush = true;
            SpinUnblock unblock(lock);
            while (!waitflushsem.wait(1000*60))
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowBuffer::flush stalled");
        }
    }

    IRowWriter *queryWriter()
    {
        return this;
    }
};


class CSmartRowInMemoryBuffer: public CSimpleInterface, implements ISmartRowBuffer, implements IRowWriter
{
    // NB must *not* call LinkThorRow or ReleaseThorRow (or Owned*ThorRow) if deallocator set
    CActivityBase *activity;
    IThorRowInterfaces *rowIf;
    ThorRowQueue *in;
    size32_t insz;
    SpinLock lock; // MORE: This lock is held for quite long periods.  I suspect it could be significantly optimized.
    bool waitingin;
    Semaphore waitinsem;
    bool waitingout;
    Semaphore waitoutsem;
    size32_t blocksize;
    bool eoi;
#ifdef _DEBUG
    bool putrecheck;
    bool getrecheck;
#endif

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSmartRowInMemoryBuffer(CActivityBase *_activity, IThorRowInterfaces *_rowIf, size32_t bufsize)
        : activity(_activity), rowIf(_rowIf)
    {
#ifdef _DEBUG
        putrecheck = false;
        getrecheck = false;
#endif
        in = new ThorRowQueue;
        waitingin = false;
        waitingout = false;
        blocksize = ((bufsize/2+0xfffff)/0x100000)*0x100000;
        insz = 0;
        eoi = false;
    }

    ~CSmartRowInMemoryBuffer()
    {
        // clear in contents 
        while (in->ordinality()) 
            ReleaseThorRow(in->dequeue());
        delete in;
    }

    void putRow(const void *row)
    {
        REENTRANCY_CHECK(putrecheck)
        size32_t sz = 0;
        if (row) {
            sz = thorRowMemoryFootprint(rowIf->queryRowSerializer(), row);
            assertex((int)sz>=0); // trap invalid sizes a bit earlier

#ifdef _TRACE_SMART_PUTGET
            ActPrintLog(activity, "***putRow(%x) %d  {%x}",(unsigned)row,sz,*(const unsigned *)row);
#endif
        }
        {
            SpinBlock block(lock);
            if (!eoi) {
                assertex(in);  // reentry check
                while ((sz+insz>blocksize)&&insz) {
                    waitingin = true;
                    {
                        SpinUnblock unblock(lock);
                        waitinsem.wait();
                    }
                    if (eoi)
                        break;
                }
            }
            if (!eoi) {
                in->enqueue(row);
                insz += sz;
#ifdef _TRACE_SMART_PUTGET
                ActPrintLog(activity, "***putRow2(%x) insize = %d ",insz);
#endif
                if (waitingout) {
                    waitoutsem.signal();
                    waitingout = false;
                }
                return;
            }
        }
        // cancelled
        ReleaseThorRow(row);
    }

    const void *nextRow()
    {
        REENTRANCY_CHECK(getrecheck)
        const void * ret;
        SpinBlock block(lock);
        assertex(!waitingout);  // reentrancy checks
        for (;;) {
            if (in->ordinality()) {
                ret = in->dequeue();
                if (ret) {
                    size32_t sz = thorRowMemoryFootprint(rowIf->queryRowSerializer(), ret);
#ifdef _TRACE_SMART_PUTGET
                    ActPrintLog(activity, "***dequeueRow(%x) %d insize=%d {%x}",(unsigned)ret,sz,insz,*(const unsigned *)ret);
#endif
                    if (insz<sz) {
                        ActPrintLog(activity, "CSmartRowInMemoryBuffer::nextRow(%x) %d insize=%d {%x} ord = %d",(unsigned)(memsize_t)ret,sz,insz,*(const unsigned *)ret,in->ordinality());
                        assertex(insz>=sz);
                    }
                    insz -= sz;
                }
                break;
            }
            else if (eoi) {
                ret = NULL;
                break;
            }
            assertex(!waitingout);  // reentrancy check
            waitingout = true;
            SpinUnblock unblock(lock);
            waitoutsem.wait();
        }
        if (waitingin) {
            waitinsem.signal();
            waitingin = false;
        }
        return ret;
    }

    void stop()
    {
#ifdef _FULL_TRACE
        ActPrintLog(activity, "CSmartRowInMemoryBuffer stop %x",(unsigned)(memsize_t)this);
#endif
        SpinBlock block(lock);
#ifdef _DEBUG
        if (waitingout) {
            ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowInMemoryBuffer::stop while nextRow waiting");
            PrintStackReport();
        }
#endif

        eoi = true;
        while (in->ordinality()) 
            ReleaseThorRow(in->dequeue());
        in->clear();
        if (waitingin) {
            waitinsem.signal();
            waitingin = false;
        }
    }


//  offset_t getPosition()
//  {
//      SpinBlock block(lock);
//      return pos;
//  }

    void flush()
    {
        // I think flush should wait til all rows read
        SpinBlock block(lock);
        eoi = true;
        for (;;) {
            assertex(in);  // reentry check
            if (waitingout) {
                waitoutsem.signal();
                waitingout = false;
            }
            if (!in->ordinality()) 
                break;
            waitingin = true;
            SpinUnblock unblock(lock);
            while (!waitinsem.wait(1000*60))
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "CSmartRowInMemoryBuffer::flush stalled");
        }
    }

    IRowWriter *queryWriter()
    {
        return this;
    }
};


static std::tuple<IBufferedSerialInputStream *, IFileIO *> createSerialInputStream(IFile *iFile, ICompressHandler *compressHandler, const CommonBufferRowRWStreamOptions &options, unsigned numSharingCompressionBuffer)
{
    Owned<IFileIO> iFileIO = iFile->open(IFOread);
    Owned<ISerialInputStream> in = createSerialInputStream(iFileIO);
    Owned<IBufferedSerialInputStream> inputStream = createBufferedInputStream(in, options.storageBlockSize, 0);
    if (compressHandler)
    {
        const char *decompressOptions = nullptr; // at least for now!
        Owned<IExpander> decompressor = compressHandler->getExpander(decompressOptions);
        Owned<ISerialInputStream> decompressed = createDecompressingInputStream(inputStream, decompressor);

        size32_t compressionBlockSize = (size32_t)(options.totalCompressionBufferSize / numSharingCompressionBuffer);
        if (compressionBlockSize < options.minCompressionBlockSize)
        {
            WARNLOG("Shared totalCompressionBufferSize=%" I64F "u, too small for number of numSharingCompressionBuffer(%u). Using minCompressionBlockSize(%u).", (unsigned __int64)options.totalCompressionBufferSize, numSharingCompressionBuffer, options.minCompressionBlockSize);
            compressionBlockSize = options.minCompressionBlockSize;
        }
        inputStream.setown(createBufferedInputStream(decompressed, compressionBlockSize, 0));
    }
    return { inputStream.getClear(), iFileIO.getClear() };
}

static std::tuple<IBufferedSerialOutputStream *, IFileIO *> createSerialOutputStream(IFile *iFile, ICompressHandler *compressHandler, const CommonBufferRowRWStreamOptions &options, unsigned numSharingCompressionBuffer)
{
    Owned<IFileIO> iFileIO = iFile->open(IFOcreate); // kept for stats purposes
    Owned<ISerialOutputStream> out = createSerialOutputStream(iFileIO);
    Owned<IBufferedSerialOutputStream> outputStream = createBufferedOutputStream(out, options.storageBlockSize); //prefered plane block size
    if (compressHandler)
    {
        const char *compressOptions = nullptr; // at least for now!
        Owned<ICompressor> compressor = compressHandler->getCompressor(compressOptions);
        Owned<ISerialOutputStream> compressed = createCompressingOutputStream(outputStream, compressor);
        size32_t compressionBlockSize = (size32_t)(options.totalCompressionBufferSize / numSharingCompressionBuffer);
        if (compressionBlockSize < options.minCompressionBlockSize)
        {
            WARNLOG("Shared totalCompressionBufferSize=%" I64F "u, too small for number of numSharingCompressionBuffer(%u). Using minCompressionBlockSize(%u).", (unsigned __int64)options.totalCompressionBufferSize, numSharingCompressionBuffer, options.minCompressionBlockSize);
            compressionBlockSize = options.minCompressionBlockSize;
        }

        outputStream.setown(createBufferedOutputStream(compressed, compressionBlockSize));
    }
    return { outputStream.getClear(), iFileIO.getClear() };
}

// #define TRACE_SPILLING_ROWSTREAM // traces each row read/written, and other events

// based on query that produces records with a single sequential (from 1) unsigned4
// #define VERIFY_ROW_IDS_SPILLING_ROWSTREAM

// for 'stressLookAhead' code. When enabled, reduces buffer sizes etc. to stress test the lookahead spilling
// #define STRESSTEST_SPILLING_ROWSTREAM



/* CCompressedSpillingRowStream implementation details:
 - Writer:
 - The writer to an in-memory queue, and when the queue is full, or a certain number of rows have been queued, it writes to starts writing to temp files.
 - The writer will always write to the queue if it can, even after it has started spilling.
 - The writer commits to disk at LookAheadOptions::writeAheadSize granularity. NB: size is uncompressed, measured before data is written to disk.
 - The writer creates a new temp file when the current one reaches LookAheadOptions::tempFileGranularity
 - The writer pushes the current nextOutputRow to a queue when it creates the next output file (used by the reader to know when to move to next)
 - NB: writer implements ISmartRowBuffer::flush() which has slightly weird semantics (blocks until everything is read or stopped)
-  Reader:
 - The reader will read from the queue until it is exhausted, and block to be signalled for more.
 - If the reader dequeues a row that is ahead of the expected 'nextInputRow', it will stash it, and read from disk until it catches up to that row.
 - If the reader is reading from disk and it catches up with 'committedRows' it will block until the writer has committed more rows.
 - When reading from a temp file, it will take ownership the CFileOwner and dispose of the underlying file when it has consumed it.
 - The reader will read from the stream until it hits 'currentTempFileEndRow' (initially 0), at which point it will open the next temp file.
 */

// NB: Supports being read by 1 thread and written to by another only
class CCompressedSpillingRowStream: public CSimpleInterfaceOf<ISmartRowBuffer>, implements IRowWriter
{
    typedef std::tuple<const void *, rowcount_t, size32_t> RowEntry;

    CActivityBase &activity; // ctor input parameter
    StringAttr baseTmpFilename; // ctor input parameter
    LookAheadOptions options; // ctor input parameter
    Linked<ICompressHandler> compressHandler; // ctor input parameter

    // derived from input paramter (IThorRowInterfaces *rowIf)
    Linked<IOutputMetaData> meta;
    Linked<IOutputRowSerializer> serializer;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowDeserializer> deserializer;

    // in-memory related members
    CSPSCQueue<RowEntry> inMemRows;
    std::atomic<memsize_t> inMemRowsMemoryUsage = 0; // NB updated from writer and reader threads
    Semaphore moreRows;
    std::atomic<bool> readerWaitingForQ = false; // set by reader, cleared by writer

    // temp write related members
    Owned<IBufferedSerialOutputStream> outputStream;
    std::unique_ptr<COutputStreamSerializer> outputStreamSerializer;
    memsize_t pendingFlushToDiskSz = 0;
    CFileOwner *currentOwnedOutputFile = nullptr;
    Owned<IFileIO> currentOutputIFileIO; // keep for stats
    CriticalSection outputFilesQCS;
    std::queue<CFileOwner *> outputFiles;
    unsigned writeTempFileNum = 0;
    std::atomic<rowcount_t> nextOutputRow = 0; // read by reader, updated by writer
    std::atomic<rowcount_t> committedRows = 0; // read by reader, updated by writer
    std::atomic<bool> spilt = false; // set by createOutputStream, checked by reader
    std::queue<rowcount_t> outputFileEndRowMarkers;
    bool lastWriteWasEog = false;
    bool outputComplete = false; // only accessed and modified by writer or reader within readerWriterCS
    bool recentlyQueued = false;
    CriticalSection outputStreamCS;

    // temp read related members
    std::atomic<rowcount_t> currentTempFileEndRow = 0;
    Owned<IFileIO> currentInputIFileIO; // keep for stats
    Linked<CFileOwner> currentOwnedInputFile;
    Owned<IBufferedSerialInputStream> inputStream;
    CThorStreamDeserializerSource inputDeserializerSource;
    rowcount_t nextInputRow = 0;
    bool readerWaitingForCommit = false;
    static constexpr unsigned readerWakeupGranularity = 32; // how often to wake up the reader if it is waiting for more rows
    enum ReadState { rs_fromqueue, rs_frommarker, rs_endstream, rs_stopped } readState = rs_fromqueue;
    RowEntry readFromStreamMarker = { nullptr, 0, 0 };

    // misc
    bool grouped = false; // ctor input parameter
    CriticalSection readerWriterCS;
#ifdef STRESSTEST_SPILLING_ROWSTREAM
    bool stressTest = false;
#endif

    // annoying flush semantics
    bool flushWaiting = false;
    Semaphore flushWaitSem;


    void trace(const char *format, ...)
    {
#ifdef TRACE_SPILLING_ROWSTREAM
        va_list args;
        va_start(args, format);
        VALOG(MCdebugInfo, format, args);
        va_end(args);
#endif
    }
    void createNextOutputStream()
    {
        VStringBuffer tmpFilename("%s.%u", baseTmpFilename.get(), writeTempFileNum++);
        trace("WRITE: writing to %s", tmpFilename.str());
        Owned<IFile> iFile = createIFile(tmpFilename);
        currentOwnedOutputFile = new CFileOwner(iFile, activity.queryTempFileSizeTracker()); // used by checkFlushToDisk to noteSize
        {
            CriticalBlock b(outputFilesQCS);
            outputFiles.push(currentOwnedOutputFile); // NB: takes ownership
        }

        auto res = createSerialOutputStream(iFile, compressHandler, options, 2); // (2) input & output sharing totalCompressionBufferSize
        outputStream.setown(std::get<0>(res));
        currentOutputIFileIO.setown(std::get<1>(res));
        outputStreamSerializer = std::make_unique<COutputStreamSerializer>(outputStream);
    }
    void createNextInputStream()
    {
        CFileOwner *dequeuedOwnedIFile = nullptr;
        {
            CriticalBlock b(outputFilesQCS);
            dequeuedOwnedIFile = outputFiles.front();
            outputFiles.pop();
        }
        currentOwnedInputFile.setown(dequeuedOwnedIFile);
        IFile *iFile = &currentOwnedInputFile->queryIFile();
        trace("READ: reading from %s", iFile->queryFilename());

        auto res = createSerialInputStream(iFile, compressHandler, options, 2); // (2) input & output sharing totalCompressionBufferSize
        inputStream.setown(std::get<0>(res));
        currentInputIFileIO.setown(std::get<1>(res));
        inputDeserializerSource.setStream(inputStream);
    }
    const void *readRowFromStream()
    {
        // readRowFromStream() called from readToMarker (which will block before calling this if behind committedRows),
        // or when outputComplete.
        // Either way, it will not enter this method until the writer has committed ahead of the reader nextInputRow

        // NB: currentTempFileEndRow will be 0 if 1st input read
        // nextInputRow can be > currentTempFileEndRow, because the writer/read may have used the Q
        // beyond this point, the next row in the stream could be anywhere above.
        if (nextInputRow >= currentTempFileEndRow)
        {
            createNextInputStream();
            CriticalBlock b(outputStreamCS);
            if (nextInputRow >= currentTempFileEndRow)
            {
                if (!outputFileEndRowMarkers.empty())
                {
                    currentTempFileEndRow = outputFileEndRowMarkers.front();
                    outputFileEndRowMarkers.pop();
                    assertex(currentTempFileEndRow > nextInputRow);
                }
                else
                {
                    currentTempFileEndRow = (rowcount_t)-1; // unbounded for now, writer will set when it knows
                    trace("READ: setting currentTempFileEndRow: unbounded");
                }
            }
        }
        if (grouped)
        {
            bool eog;
            inputStream->read(sizeof(bool), &eog);
            if (eog)
                return nullptr;
        }
        RtlDynamicRowBuilder rowBuilder(allocator);
        size32_t sz = deserializer->deserialize(rowBuilder, inputDeserializerSource);
        const void *row = rowBuilder.finalizeRowClear(sz);
        checkCurrentRow("S: ", row, nextInputRow);
        return row;
    }
    void writeRowToStream(const void *row, size32_t rowSz)
    {
        if (!spilt)
        {
            spilt = true;
            ActPrintLog(&activity, "Spilling to temp storage [file = %s]", baseTmpFilename.get());
            createNextOutputStream();
        }
        if (grouped)
        {
            bool eog = (nullptr == row);
            outputStream->put(sizeof(bool), &eog);
            pendingFlushToDiskSz++;
            if (nullptr == row)
                return;
        }
        serializer->serialize(*outputStreamSerializer.get(), (const byte *)row);
        pendingFlushToDiskSz += rowSz;
    }
    void checkReleaseQBlockReader()
    {
        if (readerWaitingForQ)
        {
            readerWaitingForQ = false;
            moreRows.signal();
        }
    }
    void checkReleaseReaderCommitBlocked()
    {
        if (readerWaitingForCommit)
        {
            readerWaitingForCommit = false;
            moreRows.signal();
        }
    }
    void handleInputComplete()
    {
        readState = rs_stopped;
        if (flushWaiting)
        {
            flushWaiting = false;
            flushWaitSem.signal();
        }
    }
    bool checkFlushToDisk(size32_t threshold)
    {
        if (pendingFlushToDiskSz <= threshold)
            return false;
        pendingFlushToDiskSz = 0;
        rowcount_t currentNextOutputRow = nextOutputRow.load();
        trace("WRITE: Flushed to disk. nextOutputRow = %" RCPF "u", currentNextOutputRow);
        outputStream->flush();
        offset_t currentTempFileSize = currentOutputIFileIO->getStatistic(StSizeDiskWrite);
        currentOwnedOutputFile->noteSize(currentTempFileSize);
        if (currentTempFileSize > options.tempFileGranularity)
        {
            {
                CriticalBlock b(outputStreamCS);
                // set if reader isn't bounded yet, or queue next boundary
                if ((rowcount_t)-1 == currentTempFileEndRow)
                {
                    currentTempFileEndRow = currentNextOutputRow;
                    trace("WRITE: setting currentTempFileEndRow: %" RCPF "u", currentTempFileEndRow.load());
                }
                else
                {
                    outputFileEndRowMarkers.push(currentNextOutputRow);
                    trace("WRITE: adding to tempFileEndRowMarker(size=%u): %" RCPF "u", (unsigned)outputFileEndRowMarkers.size(), currentNextOutputRow);
                }
            }
            createNextOutputStream(); // NB: creates new currentOwnedOutputFile/currentOutputIFileIO
        }
        committedRows = currentNextOutputRow;
        return true;
    }
    void addRow(const void *row)
    {
        bool queued = false;
        size32_t rowSz = row ? thorRowMemoryFootprint(serializer, row) : 0;
        if (rowSz + inMemRowsMemoryUsage <= options.inMemMaxMem)
            queued = inMemRows.enqueue({ row, nextOutputRow, rowSz }); // takes ownership of 'row' if successful
        if (queued)
        {
            trace("WRITE: Q: nextOutputRow: %" RCPF "u", nextOutputRow.load());
            inMemRowsMemoryUsage += rowSz;
            ++nextOutputRow;
            recentlyQueued = true;
        }
        else
        {
            trace("WRITE: S: nextOutputRow: %" RCPF "u", nextOutputRow.load());
            writeRowToStream(row, rowSz); // JCSMORE - rowSz is memory not disk size... does it matter that much?
            ::ReleaseThorRow(row);
            ++nextOutputRow;
            if (checkFlushToDisk(options.writeAheadSize))
            {
                CriticalBlock b(readerWriterCS);
                checkReleaseReaderCommitBlocked();
            }
        }

        // do not wake up reader every time a row is queued (but granularly) to avoid excessive flapping
        if (recentlyQueued && (0 == (nextOutputRow % readerWakeupGranularity)))
        {
            recentlyQueued = false;
            CriticalBlock b(readerWriterCS);
            checkReleaseQBlockReader();
        }
    }
    const void *getQRow(RowEntry &e)
    {
        rowcount_t writeRow = std::get<1>(e);
        inMemRowsMemoryUsage -= std::get<2>(e);
        if (writeRow == nextInputRow)
        {
#ifdef STRESSTEST_SPILLING_ROWSTREAM
            if (stressTest && (0 == (nextInputRow % 100)))
                MilliSleep(5);
#endif

            const void *row = std::get<0>(e);
            checkCurrentRow("Q: ", row, nextInputRow);
            ++nextInputRow;
            return row;
        }
        else
        {
            // queued row is ahead of reader position, save marker and read from stream until marker
            dbgassertex(writeRow > nextInputRow);
            readFromStreamMarker = e;
            readState = rs_frommarker;
            return readToMarker();
        }

    }
    inline void checkCurrentRow(const char *msg, const void *row, rowcount_t expectedId)
    {
#ifdef VERIFY_ROW_IDS_SPILLING_ROWSTREAM
        unsigned id;
        memcpy(&id, row, sizeof(unsigned));
        assertex(id-1 == expectedId);
        trace("READ: %s nextInputRow: %" RCPF "u", msg, expectedId);
#endif
    }
    const void *readToMarker()
    {
        rowcount_t markerRow = std::get<1>(readFromStreamMarker);
        if (markerRow == nextInputRow)
        {
            const void *ret = std::get<0>(readFromStreamMarker);
            checkCurrentRow("M: ", ret, nextInputRow);
            readFromStreamMarker = { nullptr, 0, 0 };
            readState = rs_fromqueue;
            ++nextInputRow;
            return ret;
        }
        else if (nextInputRow >= committedRows) // row we need have not yet been committed to disk.
        {
            CLeavableCriticalBlock b(readerWriterCS);
            if (nextInputRow >= committedRows)
            {
                // wait for writer to commit
                readerWaitingForCommit = true;
                b.leave();
                trace("READ: waiting for committedRows(currently = %" RCPF "u) to catch up to nextInputRow = %" RCPF "u", committedRows.load(), nextInputRow);
                moreRows.wait();
                assertex(nextInputRow < committedRows);
            }
        }
        const void *row = readRowFromStream();
        ++nextInputRow;
        return row;
    }
public:
    IMPLEMENT_IINTERFACE_O_USING(CSimpleInterfaceOf<ISmartRowBuffer>);

    explicit CCompressedSpillingRowStream(CActivityBase *_activity, const char *_baseTmpFilename, bool _grouped, IThorRowInterfaces *rowIf, const LookAheadOptions &_options, ICompressHandler *_compressHandler)
        : activity(*_activity), baseTmpFilename(_baseTmpFilename), grouped(_grouped), options(_options), compressHandler(_compressHandler),
          meta(rowIf->queryRowMetaData()), serializer(rowIf->queryRowSerializer()), allocator(rowIf->queryRowAllocator()), deserializer(rowIf->queryRowDeserializer())
    {
        size32_t minSize = meta->getMinRecordSize();

#ifdef STRESSTEST_SPILLING_ROWSTREAM
        stressTest = activity.getOptBool("stressLookAhead");
        if (stressTest)
        {
            options.inMemMaxMem = minSize * 4;
            options.writeAheadSize = options.inMemMaxMem * 2;
            options.tempFileGranularity = options.inMemMaxMem * 4;
            if (options.tempFileGranularity < 0x10000) // stop silly sizes (NB: this would only be set so small for testing!)
                options.tempFileGranularity = 0x10000;
        }
#endif

        if (minSize < 16)
            minSize = 16;  // not too important, just using to cap inMemRows queue size
        inMemRows.setCapacity(options.inMemMaxMem / minSize);

        assertex(options.writeAheadSize < options.tempFileGranularity);
    }
    ~CCompressedSpillingRowStream()
    {
        while (!outputFiles.empty())
        {
            ::Release(outputFiles.front());
            outputFiles.pop();
        }
        RowEntry e;
        while (true)
        {
            if (!inMemRows.dequeue(e))
                break;
            const void *row = std::get<0>(e);
            if (row)
                ReleaseThorRow(row);
        }
        const void *markerRow = std::get<0>(readFromStreamMarker);
        if (markerRow)
            ReleaseThorRow(markerRow);
    }

// ISmartRowBuffer
    virtual IRowWriter *queryWriter() override
    {
        return this;
    }
// IRowStream
    virtual const void *nextRow() override
    {
        switch (readState)
        {
            case rs_fromqueue:
            {
                while (true)
                {
                    RowEntry e;
                    if (inMemRows.dequeue(e))
                        return getQRow(e);
                    else
                    {
                        {
                            CLeavableCriticalBlock b(readerWriterCS);
                            // Recheck Q now have CS, if reader here and writer ready to signal more, then it may have just released CS
                            if (inMemRows.dequeue(e))
                            {
                                b.leave();
                                return getQRow(e);
                            }
                            else if (outputComplete)// && (nextInputRow == nextOutputRow))
                            {
                                if (nextInputRow == nextOutputRow)
                                {
                                    handleInputComplete(); // sets readState to rs_stopped
                                    return nullptr;
                                }
                                else
                                {
                                    // writer has finished, nothing is on the queue or will be queued, rest is on disk
                                    readState = rs_endstream;
                                    const void *row = readRowFromStream();
                                    ++nextInputRow;
                                    return row;
                                }
                            }
                            readerWaitingForQ = true;
                        }
                        trace("READ: waiting for Q'd rows @ %" RCPF "u (nextOutputRow = %" RCPF "u)", nextInputRow, nextOutputRow.load());
                        moreRows.wait();
                    }
                }
                return nullptr;
            }
            case rs_frommarker:
            {
                return readToMarker();
            }
            case rs_endstream:
            {
                if (nextInputRow == nextOutputRow)
                {
                    readState = rs_stopped;
                    return nullptr;
                }
                const void *row = readRowFromStream();
                ++nextInputRow;
                return row;
            }
            case rs_stopped:
                return nullptr;
        }
        throwUnexpected();
    }
    virtual void stop() override
    {
        CriticalBlock b(readerWriterCS);
        handleInputComplete();
    }
// IRowWriter
    virtual void putRow(const void *row) override
    {
        if (outputComplete)
        {
            // should never get here, but guard against.
            OwnedConstThorRow tmpRow(row);
            assertex(!row);
            return;
        }

        if (row)
        {
            lastWriteWasEog = false;
            addRow(row);
        }
        else // eog
        {
            if (lastWriteWasEog) // error, should not have two EOGs in a row
                return;
            else if (grouped)
            {
                lastWriteWasEog = true;
                addRow(nullptr);
            }
            else // non-grouped nulls unexpected
                throwUnexpected();
        }
    }
    virtual void flush() override
    {
        // semantics of ISmartRowBuffer::flush:
        // - tell smartbuf that there will be no more rows written (BUT should only be called after finished writing)
        // - wait for all rows to be read from smartbuf, or smartbuf stopped before returning.

        bool flushedToDisk = checkFlushToDisk(0);
        {
            CriticalBlock b(readerWriterCS);
            outputComplete = true;
            if (rs_stopped == readState)
                return;
            flushWaiting = true;
            if (flushedToDisk)
                checkReleaseReaderCommitBlocked();
            checkReleaseQBlockReader();
        }
        flushWaitSem.wait();
    }
};



ISmartRowBuffer * createSmartBuffer(CActivityBase *activity, const char * tempname, size32_t buffsize, IThorRowInterfaces *rowif)
{
    Owned<IFile> file = createIFile(tempname);
    return new CSmartRowBuffer(activity,file,buffsize,rowif);
}

ISmartRowBuffer * createSmartInMemoryBuffer(CActivityBase *activity, IThorRowInterfaces *rowIf, size32_t buffsize)
{
    return new CSmartRowInMemoryBuffer(activity, rowIf, buffsize);
}

ISmartRowBuffer * createCompressedSpillingRowStream(CActivityBase *activity, const char * tempBaseName, bool grouped, IThorRowInterfaces *rowif, const LookAheadOptions &options, ICompressHandler *compressHandler)
{
    return new CCompressedSpillingRowStream(activity, tempBaseName, grouped, rowif, options, compressHandler);
}

class COverflowableBuffer : public CSimpleInterface, implements IRowWriterMultiReader
{
    CActivityBase &activity;
    IThorRowInterfaces *rowIf;
    Owned<IThorRowCollector> collector;
    Owned<IRowWriter> writer;
    bool eoi, shared;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    COverflowableBuffer(CActivityBase &_activity, IThorRowInterfaces *_rowIf, EmptyRowSemantics emptyRowSemantics, bool _shared, unsigned spillPriority)
        : activity(_activity), rowIf(_rowIf), shared(_shared)
    {
        collector.setown(createThorRowCollector(activity, rowIf, NULL, stableSort_none, rc_mixed, spillPriority, emptyRowSemantics));
        writer.setown(collector->getWriter());
        eoi = false;
    }
    ~COverflowableBuffer()
    {
        writer.clear();
        collector.clear();
    }

// IRowWriterMultiReader
    virtual IRowStream *getReader()
    {
        flush();
        return collector->getStream(shared);
    }
// IRowWriter
    virtual void putRow(const void *row)
    {
        assertex(!eoi);
        writer->putRow(row);
    }
    virtual void flush()
    {
        eoi = true;
    }
};

IRowWriterMultiReader *createOverflowableBuffer(CActivityBase &activity, IThorRowInterfaces *rowIf, EmptyRowSemantics emptyRowSemantics, bool shared, unsigned spillPriority)
{
    return new COverflowableBuffer(activity, rowIf, emptyRowSemantics, shared, spillPriority);
}


#define VALIDATEEQ(LHS, RHS) if ((LHS)!=(RHS)) { StringBuffer s("FAIL(EQ) - LHS="); s.append(LHS).append(", RHS=").append(RHS); PROGLOG("%s", s.str()); throwUnexpected();  }
#define VALIDATELT(LHS, RHS) if ((LHS)>=(RHS)) { StringBuffer s("FAIL(LT) - LHS="); s.append(LHS).append(", RHS=").append(RHS); PROGLOG("%s", s.str()); throwUnexpected(); }

//#define TRACE_WRITEAHEAD
class CSharedWriteAheadBase;
class CRowSet : public CSimpleInterface, implements IInterface
{
    unsigned chunk;
    CThorExpandingRowArray rows;
    CSharedWriteAheadBase &sharedWriteAhead;
    mutable CriticalSection crit;
public:
    CRowSet(CSharedWriteAheadBase &_sharedWriteAhead, unsigned _chunk, unsigned maxRows);
    virtual void Link() const
    {
        CSimpleInterface::Link();
    }
    virtual bool Release() const;
    void clear() { rows.clearRows(); }
    void setChunk(unsigned _chunk) { chunk = _chunk; } 
    void reset(unsigned _chunk)
    {
        chunk = _chunk;
        rows.clearRows();
    }
    inline unsigned queryChunk() const { return chunk; }
    inline unsigned getRowCount() const { return rows.ordinality(); }
    inline void addRow(const void *row) { rows.append(row); }
    inline const void *getRow(unsigned r)
    {
        return rows.get(r);
    }
};

class Chunk : public CInterface
{
public:
    offset_t offset;
    size32_t size;
    Linked<CRowSet> rowSet;
    Chunk(CRowSet *_rowSet) : rowSet(_rowSet), offset(0), size(0) { }
    Chunk(offset_t _offset, size_t _size) : offset(_offset), size(_size) { }
    Chunk(const Chunk &other) : offset(other.offset), size(other.size) { }
    bool operator==(Chunk const &other) const { return size==other.size && offset==other.offset; }
    Chunk &operator=(Chunk const &other) { offset = other.offset; size = other.size; return *this; }
    offset_t endOffset() { return offset+size; }
};

typedef int (*ChunkCompareFunc)(Chunk *, Chunk *);
int chunkOffsetCompare(Chunk *lhs, Chunk *rhs)
{
    if (lhs->offset>rhs->offset)
        return 1;
    else if (lhs->offset<rhs->offset)
        return -1;
    return 0;
}
int chunkSizeCompare(Chunk **_lhs, Chunk **_rhs)
{
    Chunk *lhs = *(Chunk **)_lhs;
    Chunk *rhs = *(Chunk **)_rhs;
    return (int)lhs->size - (int)rhs->size;
}
int chunkSizeCompare2(Chunk *lhs, Chunk *rhs)
{
    return (int)lhs->size - (int)rhs->size;
}

#define MIN_POOL_CHUNKS 10
class CSharedWriteAheadBase : public CSimpleInterface, implements ISharedSmartBuffer, implements ISharedSmartBufferRowWriter
{
    size32_t totalOutChunkSize;
    bool writeAtEof;
    rowcount_t rowsWritten;
    IArrayOf<IRowStream> outputs;
    unsigned readersWaiting;
    mutable IArrayOf<CRowSet> cachedRowSets;
    CriticalSection rowSetCacheCrit;

    void reuse(CRowSet *rowset)
    {
        if (!reuseRowSets)
            return;
        rowset->clear();
        CriticalBlock b(rowSetCacheCrit);
        if (cachedRowSets.ordinality() < (outputs.ordinality()*2))
            cachedRowSets.append(*LINK(rowset));
    }
    virtual void init()
    {
        stopped = false;
        writeAtEof = false;
        rowsWritten = 0;
        readersWaiting = 0;
        totalChunksOut = lowestChunk = 0;
        lowestOutput = 0;
#ifdef TRACE_WRITEAHEAD
        totalOutChunkSize = sizeof(unsigned);
#else
        totalOutChunkSize = 0;
#endif
    }

    inline bool isEof(rowcount_t rowsRead)
    {
        return stopped || (writeAtEof && rowsWritten == rowsRead);
    }
    inline void signalReaders()
    {
        if (readersWaiting)
        {
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "signalReaders: %d", readersWaiting);
#endif
            readersWaiting = 0;
            ForEachItemIn(o, outputs)
                queryCOutput(o).wakeRead(); // if necessary
        }
    }
    CRowSet *loadMore(unsigned output)
    {
        // needs to be called in crit
        Linked<CRowSet> rowSet;
        unsigned currentChunkNum = queryCOutput(output).currentChunkNum;
        if (currentChunkNum == totalChunksOut)
        {
#ifdef TRACE_WRITEAHEAD
            PROGLOG("output=%d, chunk=%d INMEM", output, currentChunkNum);
#endif
            rowSet.set(inMemRows);
        }
        else
        {
            unsigned whichChunk = queryCOutput(output).currentChunkNum - lowestChunk;
#ifdef TRACE_WRITEAHEAD
            LOG(MCthorDetailedDebugInfo, "output=%d, chunk=%d (whichChunk=%d)", output, currentChunkNum, whichChunk);
#endif
            rowSet.setown(readRows(output, whichChunk));
            assertex(rowSet);
        }
        VALIDATEEQ(currentChunkNum, rowSet->queryChunk());
        advanceNextReadChunk(output);
        return rowSet.getClear();
    }
protected:
    class COutput : public CSimpleInterface, implements IRowStream
    {
        CSharedWriteAheadBase &parent;
        unsigned output;
        rowcount_t rowsRead;
        bool eof, readerWaiting;
        Semaphore readWaitSem;
        Owned<CRowSet> outputOwnedRows;
        CRowSet *rowSet;
        unsigned row, rowsInRowSet;

        void init()
        {
            rowsRead = 0;
            currentChunkNum = 0;
            rowsInRowSet = row = 0;
            readerWaiting = eof = false;
            rowSet = NULL;
        }
        inline void doStop()
        {
            if (eof) return;
            eof = true;
            parent.stopOutput(output);
        }
        inline void wakeRead()
        {
            if (readerWaiting)
            {
                readerWaiting = false;
                readWaitSem.signal();
            }
        }
    public:
        unsigned currentChunkNum;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        COutput(CSharedWriteAheadBase &_parent, unsigned _output) : parent(_parent), output(_output)
        {
            init();
        }
        void reset()
        {
            init();
            outputOwnedRows.clear();
        }
        inline unsigned queryOutput() const { return output; }
        inline CRowSet *queryRowSet() { return rowSet; }
        const void *nextRow()
        {
            if (eof)
                return NULL;
            if (row == rowsInRowSet)
            {
                CriticalBlock b(parent.crit);
                if (!rowSet || (row == (rowsInRowSet = rowSet->getRowCount())))
                {
                    rowcount_t totalRows = parent.readerWait(*this, rowsRead);
                    if (0 == totalRows)
                    {
                        doStop();
                        return NULL;
                    }
                    if (!rowSet || (row == (rowsInRowSet = rowSet->getRowCount()))) // may have caught up in same rowSet
                    {
                        Owned<CRowSet> newRows = parent.loadMore(output);
                        if (rowSet)
                            VALIDATEEQ(newRows->queryChunk(), rowSet->queryChunk()+1);
                        outputOwnedRows.setown(newRows.getClear());
                        rowSet = outputOwnedRows.get();
                        rowsInRowSet = rowSet->getRowCount();
                        row = 0;
                    }
                }
            }
            rowsRead++;
            const void *retrow = rowSet->getRow(row++);
            return retrow;
        }
        virtual void stop()
        {
            CriticalBlock b(parent.crit);
            doStop();
        }
    friend class CSharedWriteAheadBase;
    };
    CActivityBase *activity;
    size32_t minChunkSize;
    unsigned lowestChunk, lowestOutput, outputCount, totalChunksOut;
    rowidx_t maxRows;
    bool stopped;
    Owned<CRowSet> inMemRows;
    CriticalSection crit;
    Linked<IOutputMetaData> meta;
    Linked<IOutputRowSerializer> serializer;
    QueueOf<CRowSet, false> chunkPool;
    unsigned maxPoolChunks;
    bool reuseRowSets;

    inline rowcount_t readerWait(COutput &output, const rowcount_t rowsRead)
    {
        if (rowsRead == rowsWritten)
        {
            if (stopped || writeAtEof)
                return 0;
            output.readerWaiting = true;
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "readerWait(%d)", output.queryOutput());
#endif
            ++readersWaiting;
            {
                CriticalUnblock b(crit);
                unsigned mins=0;
                for (;;)
                {
                    if (output.readWaitSem.wait(60000))
                        break; // NB: will also be signal if aborting
                    ActPrintLog(activity, "output %d @ row # %" RCPF "d, has been blocked for %d minute(s)", output.queryOutput(), rowsRead, ++mins);
                }
            }
            if (isEof(rowsRead))
                return 0;
        }
        return rowsWritten;
    }
    CRowSet *newRowSet(unsigned chunk)
    {
        {
            CriticalBlock b(rowSetCacheCrit);
            if (cachedRowSets.ordinality())
            {
                CRowSet *rowSet = &cachedRowSets.popGet();
                rowSet->setChunk(chunk);
                return rowSet;
            }
        }
        return new CRowSet(*this, chunk, maxRows);
    }
    inline COutput &queryCOutput(unsigned i) { return (COutput &) outputs.item(i); }
    inline unsigned getLowest()
    {
        unsigned o=0;
        offset_t lsf = (unsigned)-1;
        unsigned lsfOutput = (unsigned)-1;
        for (;;)
        {
            if (queryCOutput(o).currentChunkNum < lsf)
            {
                lsf = queryCOutput(o).currentChunkNum; 
                lsfOutput = o;
            }
            if (++o == outputCount)
                break;
        }
        return lsfOutput;
    }
    void advanceNextReadChunk(unsigned output)
    {
        unsigned &currentChunkNum = queryCOutput(output).currentChunkNum;
        unsigned prevChunkNum = currentChunkNum;
        currentChunkNum++;
        if (output == lowestOutput) // might be joint lowest
        {
            lowestOutput = getLowest();
            if (currentChunkNum == queryCOutput(lowestOutput).currentChunkNum) // has new page moved up to join new lowest, in which case it was last.
            {
                if (prevChunkNum != totalChunksOut) // if equal, then prevOffset was mem page and not in pool/disk to free
                    freeOffsetChunk(prevChunkNum);
                else
                {
                    VALIDATEEQ(prevChunkNum, inMemRows->queryChunk()); // validate is current in mem page
                }
                ++lowestChunk;
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "aNRP: %d, lowestChunk=%d, totalChunksOut=%d", prevChunkNum, lowestChunk, totalChunksOut);
#endif
            }
        }
    }
    void stopOutput(unsigned output)
    {
        // mark stopped output with very high page (as if read all), will not be picked again
        unsigned lowestA = getLowest();
        if (output == lowestA)
        {
            unsigned chunkOrig = queryCOutput(output).currentChunkNum;
            queryCOutput(output).currentChunkNum = (unsigned)-1;
            unsigned lO = getLowest(); // get next lowest
            queryCOutput(output).currentChunkNum = chunkOrig;
            if (((unsigned)-1) == lO) // if last to stop
            {
                markStop();
                return;
            }
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Input %d stopped, forcing catchup to free pages", output);
#endif
            while (queryCOutput(output).currentChunkNum < queryCOutput(lO).currentChunkNum)
            {
                Owned<CRowSet> tmpChunk = loadMore(output); // discard
            }
#ifdef TRACE_WRITEAHEAD
            ActPrintLog(activity, "Input %d stopped. Caught up", output);
#endif
            queryCOutput(output).currentChunkNum = (unsigned)-1;
            lowestOutput = getLowest();
        }
        else
            queryCOutput(output).currentChunkNum = (unsigned)-1;
    }
    void stopAll()
    {
        CriticalBlock b(crit);
        markStop();
    }
    virtual void markStop()
    {
        if (stopped) return;
        stopped = true;
    }
    virtual void freeOffsetChunk(unsigned chunk) = 0;
    virtual CRowSet *readRows(unsigned output, unsigned chunk) = 0;
    virtual void flushRows(ISharedSmartBufferCallback *callback) = 0;
    virtual size32_t rowSize(const void *row) = 0;
public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSharedWriteAheadBase(CActivityBase *_activity, unsigned _outputCount, IThorRowInterfaces *rowIf)
        : activity(_activity), outputCount(_outputCount), meta(rowIf->queryRowMetaData()), serializer(rowIf->queryRowSerializer())
    {
        init();
        minChunkSize = 0x2000;
        size32_t minSize = meta->getMinRecordSize();
        if (minChunkSize<minSize*10) // if rec minSize bigish, ensure reasonable minChunkSize
        {
            minChunkSize = minSize*10;
            if (minChunkSize > 0x10000)
                minChunkSize += 2*(minSize+1);
        }
        else if (0 == minSize) // unknown
            minSize = 1;

        /* NB: HPCC-25392 (fix to avoid clearing inMemRows on child query reset)
         * maxRows is used to intialize the CRowSet::rows' initial allocated row array size.
         * These row arrays are never freed, and never resized.
         * On reset the rows are cleared, but the row array is untouched.
         * When adding rows there are never more added than maxRows added, because a new row set
         * is created when minChunkSize is exceeded, which is a multiple of min row size, (see putRow)
         *
         * This is vital, because both the writer and readers may be reading the same array
         * concurrently. Resizing the array in putRow, would invalidate the row array that a
         * reader was currently accessing.
         */
        maxRows = (minChunkSize / minSize) + 1;
        outputCount = _outputCount;
        unsigned c=0;
        for (; c<outputCount; c++)
        {
            outputs.append(* new COutput(*this, c));
        }
        inMemRows.setown(newRowSet(0));
        maxPoolChunks = MIN_POOL_CHUNKS;
        reuseRowSets = true;
    }
    ~CSharedWriteAheadBase()
    {
        reuseRowSets = false;
        ForEachItemIn(o, outputs)
        {
            COutput &output = queryCOutput(o);
            output.outputOwnedRows.clear();
        }
        inMemRows.clear();
    }

    unsigned anyReaderBehind()
    {
        unsigned reader=0;
        for (;;)
        {
            if (reader>=outputCount) // so all waiting, don't need in mem page.
                break;
            if (!queryCOutput(reader).eof && (queryCOutput(reader).rowSet != inMemRows.get())) // if output using this in mem page, then it has linked already, need to find one that is not (i.e. behind)
                return reader;
            ++reader;
        }
        return NotFound;
    }

// ISharedSmartBuffer
    virtual ISharedSmartBufferRowWriter *getWriter() override
    {
        return LINK(this);
    }
// ISharedSmartBufferRowWriter
    virtual void putRow(const void *row, ISharedSmartBufferCallback *callback) override
    {
        if (stopped)
        {
            ReleaseThorRow(row);
            return;
        }
        unsigned len=rowSize(row);
        CriticalBlock b(crit);
        bool paged = false;
        if (totalOutChunkSize >= minChunkSize) // chunks required to be at least minChunkSize
        {
            unsigned reader=anyReaderBehind();
            if (NotFound != reader)
                flushRows(callback);
            inMemRows.setown(newRowSet(++totalChunksOut));
#ifdef TRACE_WRITEAHEAD
            totalOutChunkSize = sizeof(unsigned);
#else
            totalOutChunkSize = 0;
#endif
            /* If callback used to signal paged out, only signal readers on page event,
             * This is to minimize time spent by fast readers constantly catching up and waiting and getting woken up per record
             */
            if (callback)
            {
                paged = true;
                callback->paged();
            }
        }
        inMemRows->addRow(row);

        totalOutChunkSize += len;
        rowsWritten++; // including NULLs(eogs)

        if (!callback || paged)
            signalReaders();
    }
    virtual void putRow(const void *row) override
    {
        return putRow(row, NULL);
    }
    virtual void flush() override
    {
        CriticalBlock b(crit);
        writeAtEof = true;
        signalReaders();
    }
// ISharedRowStreamReader
    virtual IRowStream *queryOutput(unsigned output) override
    {
        return &outputs.item(output);
    }
    virtual void cancel() override
    {
        CriticalBlock b(crit);
        stopAll();
        signalReaders();
    }
    virtual void reset() override
    {
        init();
        unsigned c=0;
        for (; c<outputCount; c++)
            queryCOutput(c).reset();
        inMemRows->reset(0);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) const override
    {
        return 0;
    }
friend class COutput;
friend class CRowSet;
};

CRowSet::CRowSet(CSharedWriteAheadBase &_sharedWriteAhead, unsigned _chunk, unsigned maxRows)
    : sharedWriteAhead(_sharedWriteAhead), rows(*_sharedWriteAhead.activity, _sharedWriteAhead.activity, ers_eogonly, stableSort_none, true, maxRows), chunk(_chunk)
{
}

bool CRowSet::Release() const
{
    {
        // NB: Occasionally, >1 thread may be releasing a CRowSet concurrently and miss a opportunity to reuse, but that's ok.
        //No need to protect with a lock, because if not shared then it cannot be called at the same time by another thread,
        if (!IsShared())
            sharedWriteAhead.reuse((CRowSet *)this);
    }
    return CSimpleInterface::Release();
}

static StringBuffer &getFileIOStats(StringBuffer &output, IFileIO *iFileIO)
{
    __int64 readCycles = iFileIO->getStatistic(StCycleDiskReadIOCycles);
    __int64 writeCycles = iFileIO->getStatistic(StCycleDiskWriteIOCycles);
    __int64 numReads = iFileIO->getStatistic(StNumDiskReads);
    __int64 numWrites = iFileIO->getStatistic(StNumDiskWrites);
    offset_t bytesRead = iFileIO->getStatistic(StSizeDiskRead);
    offset_t bytesWritten = iFileIO->getStatistic(StSizeDiskWrite);
    if (readCycles)
        output.appendf(", read-time(ms)=%" I64F "d", cycle_to_millisec(readCycles));
    if (writeCycles)
        output.appendf(", write-time(ms)=%" I64F "d", cycle_to_millisec(writeCycles));
    if (numReads)
        output.appendf(", numReads=%" I64F "d", numReads);
    if (numWrites)
        output.appendf(", numWrites=%" I64F "d", numWrites);
    if (bytesRead)
        output.appendf(", bytesRead=%" I64F "d", bytesRead);
    if (bytesWritten)
        output.appendf(", bytesWritten=%" I64F "d", bytesWritten);
    return output;
}

class CSharedWriteAheadDisk : public CSharedWriteAheadBase
{
    Owned<CFileOwner> tempFileOwner;
    Owned<IFileIO> tempFileIO;
    CIArrayOf<Chunk> freeChunks;
    PointerArrayOf<Chunk> freeChunksSized;
    QueueOf<Chunk, false> savedChunks;
    offset_t highOffset;
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowDeserializer> deserializer;
    IOutputMetaData *serializeMeta;

    struct AddRemoveFreeChunk
    {
        PointerArrayOf<Chunk> &chunkArray;
        Chunk *chunk;
        AddRemoveFreeChunk(PointerArrayOf<Chunk> &_chunkArray, Chunk *_chunk) : chunkArray(_chunkArray), chunk(_chunk)
        {
            chunkArray.zap(chunk);
        }
        ~AddRemoveFreeChunk()
        {
            bool isNew;
            aindex_t sizePos = chunkArray.bAdd(chunk, chunkSizeCompare, isNew);
            if (!isNew) // might match size of another block
                chunkArray.add(chunk, sizePos);
        }
    };
    unsigned findNext(Chunk **base, unsigned arrCount, Chunk *searchChunk, ChunkCompareFunc compareFunc, bool &matched)
    {
        unsigned middle;
        unsigned left = 0;
        int result;
        Chunk *CurEntry;
        unsigned right = arrCount-1;
        /*
        * Loop until we've narrowed down the search range to one or two
        * items.  This ensures that middle != 0, preventing 'right' from wrapping.
        */
        while (right - left > 1)
        {
            /* Calculate the middle entry in the array - NOT the middle offset */
            middle = (right + left) >> 1;

            CurEntry = base[middle];
            result = compareFunc(searchChunk, CurEntry);
            if (result < 0)
                right = middle - 1;
            else if (result > 0)
                left = middle + 1;
            else
            {
                matched = true;
                return middle;
            }
        }
        middle = left;
        /*
        * The range has been narrowed down to 1 or 2 items.
        * Perform an optimal check on these last two elements.
        */
        result = compareFunc(searchChunk, base[middle]);
        if (0 == result)
            matched = true;
        else if (result > 0)
        {
            ++middle;
            if (right == left + 1)
            {
                result = compareFunc(searchChunk, base[middle]);
                if (0 == result)
                    matched = true;
                else
                {
                    matched = false;
                    if (result > 0)
                        ++middle;
                }
            }
            else
                matched = false;
        }
        else
            matched = false;
        if (middle < arrCount)
            return middle;
        return NotFound;
    }
    inline Chunk *getOutOffset(size32_t required)
    {
        unsigned numFreeChunks = freeChunks.ordinality();
        if (numFreeChunks)
        {
            Chunk searchChunk(0, required);
            bool matched;
            unsigned nextPos = findNext(freeChunksSized.getArray(), freeChunksSized.ordinality(), &searchChunk, chunkSizeCompare2, matched);
            if (NotFound != nextPos)
            {
                Linked<Chunk> nextChunk = freeChunksSized.item(nextPos);
                if (nextChunk->size-required >= minChunkSize)
                {
                    Owned<Chunk> chunk = new Chunk(nextChunk->offset, required);
                    decFreeChunk(nextChunk, required);
#ifdef TRACE_WRITEAHEAD
                    ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "getOutOffset: got [free] offset = %" I64F "d, size=%d, but took required=%d", nextChunk->offset, nextChunk->size+required, required);
#endif
                    return chunk.getClear();
                }
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "getOutOffset: got [free] offset = %" I64F "d, size=%d", nextChunk->offset, nextChunk->size);
#endif
                freeChunksSized.remove(nextPos);
                freeChunks.zap(*nextChunk);
                return nextChunk.getClear(); // if smaller than minChunkSize, return all, don't leave useless fragment
            }
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLog(activity, "getOutOffset: got new upper offset # = %" I64F "d", highOffset);
#endif
        Chunk *chunk = new Chunk(highOffset, required);
        highOffset += required; // NB next
        return chunk;
    }
    inline void mergeFreeChunk(Chunk *dst, const Chunk *src)
    {
        AddRemoveFreeChunk ar(freeChunksSized, dst);
        dst->offset = src->offset;
        dst->size += src->size;
    }
    inline void decFreeChunk(Chunk *chunk, size32_t sz)
    {
        AddRemoveFreeChunk ar(freeChunksSized, chunk);
        chunk->size -= sz;
        chunk->offset += sz;
    }
    inline void incFreeChunk(Chunk *chunk, size32_t sz)
    {
        AddRemoveFreeChunk ar(freeChunksSized, chunk);
        chunk->size += sz;
    }
    inline void addFreeChunk(Chunk *freeChunk, unsigned pos=NotFound)
    {
        bool isNew;
        aindex_t sizePos = freeChunksSized.bAdd(freeChunk, chunkSizeCompare, isNew);
        if (!isNew)
            freeChunksSized.add(freeChunk, sizePos);
        if (NotFound == pos)
            freeChunks.append(*LINK(freeChunk));
        else
            freeChunks.add(*LINK(freeChunk), pos);
    }
    virtual void freeOffsetChunk(unsigned chunk)
    {
        // chunk(unused here) is nominal sequential page #, savedChunks is page # in diskfile
        assertex(savedChunks.ordinality());
        Owned<Chunk> freeChunk = savedChunks.dequeue();
        if (freeChunk->rowSet)
        {
            Owned<CRowSet> rowSet = chunkPool.dequeue(freeChunk->rowSet);
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "freeOffsetChunk (chunk=%d) chunkPool, savedChunks size=%d, chunkPool size=%d", rowSet->queryChunk(), savedChunks.ordinality(), chunkPool.ordinality());
#endif
            VALIDATEEQ(chunk, rowSet->queryChunk());
            return;
        }
        unsigned nmemb = freeChunks.ordinality();
        if (0 == nmemb)
            addFreeChunk(freeChunk);
        else
        {
            bool matched;
            unsigned nextPos = findNext(freeChunks.getArray(), freeChunks.ordinality(), freeChunk, chunkOffsetCompare, matched);
            assertex(!matched); // should never happen, have found a chunk with same offset as one being freed.
            if (NotFound != nextPos)
            {
                Chunk *nextChunk = &freeChunks.item(nextPos);
                if (freeChunk->endOffset() == nextChunk->offset)
                    mergeFreeChunk(nextChunk, freeChunk);
                else if (nextPos > 0)
                {
                    Chunk *prevChunk = &freeChunks.item(nextPos-1);
                    if (prevChunk->endOffset() == freeChunk->offset)
                        incFreeChunk(prevChunk, freeChunk->size);
                    else
                        addFreeChunk(freeChunk, nextPos);
                }
                else
                    addFreeChunk(freeChunk, nextPos);
            }
            else
                addFreeChunk(freeChunk);
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Added chunk, offset %" I64F "d size=%d to freeChunks", freeChunk->offset, freeChunk->size);
#endif
    }
    virtual CRowSet *readRows(unsigned output, unsigned whichChunk)
    {
        assertex(savedChunks.ordinality() > whichChunk);
        unsigned currentChunkNum = queryCOutput(output).currentChunkNum;
        unsigned o=0;
        for (; o<outputCount; o++)
        {
            if (o == output) continue;
            COutput &coutput = queryCOutput(o);
            if (coutput.queryRowSet() && (coutput.queryRowSet()->queryChunk() == currentChunkNum))
            {
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Output: %d, readRows found other output %d with matching offset: %d", output, o, currentChunkNum);
#endif
                return LINK(coutput.queryRowSet());
            }
        }
        Chunk &chunk = *savedChunks.item(whichChunk);
        Owned<CRowSet> rowSet;
        if (chunk.rowSet)
        {
            rowSet.set(chunk.rowSet);
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "readRows (chunk=%d) output: %d, savedChunks size=%d, chunkPool size=%d, currentChunkNum=%d, whichChunk=%d", rowSet->queryChunk(), output, savedChunks.ordinality(), chunkPool.ordinality(), currentChunkNum, whichChunk);
#endif
            VALIDATEEQ(rowSet->queryChunk(), currentChunkNum);
        }
        else
        {
            Owned<ISerialStream> stream = createFileSerialStream(tempFileIO, chunk.offset);
#ifdef TRACE_WRITEAHEAD
            unsigned diskChunkNum;
            stream->get(sizeof(diskChunkNum), &diskChunkNum);
            VALIDATEEQ(diskChunkNum, currentChunkNum);
#endif
            CThorStreamDeserializerSource ds(stream);
            rowSet.setown(newRowSet(currentChunkNum));
            for (;;)
            {
                byte b;
                ds.read(sizeof(b),&b);
                if (!b)
                    break;
                if (1==b)
                {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    size32_t sz = deserializer->deserialize(rowBuilder, ds);
                    rowSet->addRow(rowBuilder.finalizeRowClear(sz));
                }
                else if (2==b)
                    rowSet->addRow(NULL);
            }
        }
        return rowSet.getClear();
    }
    virtual void flushRows(ISharedSmartBufferCallback *callback)
    {
        // NB: called in crit
        Owned<Chunk> chunk;
        if (chunkPool.ordinality() < maxPoolChunks)
        {
            chunk.setown(new Chunk(inMemRows));
            chunkPool.enqueue(inMemRows.getLink());
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "flushRows (chunk=%d) savedChunks size=%d, chunkPool size=%d", inMemRows->queryChunk(), savedChunks.ordinality()+1, chunkPool.ordinality());
#endif
        }
        else
        {
            /* It might be worth adding a heuristic here, to estimate time for readers to catch up, vs time spend writing and reading.
             * Could block for readers to catch up if cost of writing/reads outweighs avg cost of catch up...
             */
            MemoryBuffer mb;
            mb.ensureCapacity(minChunkSize); // starting size/could be more if variable and bigger
#ifdef TRACE_WRITEAHEAD
            mb.append(inMemRows->queryChunk()); // for debug purposes only
#endif
            CMemoryRowSerializer mbs(mb);
            unsigned r=0;
            for (;r<inMemRows->getRowCount();r++)
            {
                OwnedConstThorRow row = inMemRows->getRow(r);
                if (row)
                {
                    mb.append((byte)1);
                    serializer->serialize(mbs,(const byte *)row.get());
                }
                else
                    mb.append((byte)2); // eog
            }
            mb.append((byte)0);
            size32_t len = mb.length();
            chunk.setown(getOutOffset(len)); // will find space for 'len', might be bigger if from free list
            tempFileIO->write(chunk->offset, len, mb.toByteArray());
            tempFileOwner->noteSize(highOffset);
#ifdef TRACE_WRITEAHEAD
            ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Flushed chunk = %d (savedChunks pos=%d), writeOffset = %" I64F "d, writeSize = %d", inMemRows->queryChunk(), savedChunks.ordinality(), chunk->offset, len);
#endif
        }

        savedChunks.enqueue(chunk.getClear());
    }
    virtual size32_t rowSize(const void *row)
    {
        if (!row)
            return 1; // eog;
        else if (meta == serializeMeta)
            return meta->getRecordSize(row)+1; // space on disk, +1 = eog marker
        CSizingSerializer ssz;
        serializer->serialize(ssz,(const byte *)row);
        return ssz.size()+1; // space on disk, +1 = eog marker
    }
public:
    CSharedWriteAheadDisk(CActivityBase *activity, const char *spillName, unsigned outputCount, IThorRowInterfaces *rowIf) : CSharedWriteAheadBase(activity, outputCount, rowIf),
        allocator(rowIf->queryRowAllocator()), deserializer(rowIf->queryRowDeserializer()), serializeMeta(meta->querySerializedDiskMeta())
    {
        assertex(spillName);
        tempFileOwner.setown(activity->createOwnedTempFile(spillName));
        tempFileOwner->queryIFile().setShareMode(IFSHnone);
        tempFileIO.setown(tempFileOwner->queryIFile().open(IFOcreaterw));
        highOffset = 0;
    }
    ~CSharedWriteAheadDisk()
    {
        if (tempFileIO)
        {
            StringBuffer tracing;
            getFileIOStats(tracing, tempFileIO);
            activity->ActPrintLog("CSharedWriteAheadDisk: removing spill file: %s%s", tempFileOwner->queryIFile().queryFilename(), tracing.str());
            tempFileIO.clear();
        }

        for (;;)
        {
            Owned<Chunk> chunk = savedChunks.dequeue();
            if (!chunk) break;
        }
        LOG(MCthorDetailedDebugInfo, "CSharedWriteAheadDisk: highOffset=%" I64F "d", highOffset);
    }
    virtual void reset()
    {
        CSharedWriteAheadBase::reset();
        for (;;)
        {
            Owned<Chunk> chunk = savedChunks.dequeue();
            if (!chunk) break;
        }
        freeChunks.kill();
        freeChunksSized.kill();
        highOffset = 0;
        tempFileIO->setSize(0);
        tempFileOwner->noteSize(0);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) const override
    {
        if (kind==StNumSpills)
            return 1;
        return tempFileIO->getStatistic(kind);
    }
};

ISharedSmartBuffer *createSharedSmartDiskBuffer(CActivityBase *activity, const char *spillname, unsigned outputs, IThorRowInterfaces *rowIf)
{
    return new CSharedWriteAheadDisk(activity, spillname, outputs, rowIf);
}

class CSharedWriteAheadMem : public CSharedWriteAheadBase
{
    Semaphore poolSem;
    bool writerBlocked;

    virtual void markStop()
    {
        CSharedWriteAheadBase::markStop();
        if (writerBlocked)
        {
            writerBlocked = false;
            poolSem.signal();
        }
    }
    virtual void freeOffsetChunk(unsigned chunk)
    {
        Owned<CRowSet> topRowSet = chunkPool.dequeue();
        VALIDATEEQ(chunk, topRowSet->queryChunk());
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "freeOffsetChunk: Dequeue chunkPool chunks: %d, chunkPool.ordinality() = %d", topRowSet->queryChunk(), chunkPool.ordinality());
#endif
        topRowSet.clear();
        if (writerBlocked)
        {
            writerBlocked = false;
            poolSem.signal();
        }
    }
    virtual CRowSet *readRows(unsigned output, unsigned whichChunk)
    {
        Linked<CRowSet> rowSet = chunkPool.item(whichChunk);
        VALIDATEEQ(queryCOutput(output).currentChunkNum, rowSet->queryChunk());
        return rowSet.getClear();
    }
    virtual void flushRows(ISharedSmartBufferCallback *callback)
    {
        // NB: called in crit
        if (chunkPool.ordinality() >= maxPoolChunks)
        {
            writerBlocked = true;
            {
                CriticalUnblock b(crit);
                if (callback)
                    callback->blocked();
                poolSem.wait();
                if (callback)
                    callback->unblocked();
                if (stopped) return;
            }
            unsigned reader=anyReaderBehind();
            if (NotFound == reader)
            {
#ifdef TRACE_WRITEAHEAD
                ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "flushRows: caught up whilst blocked to: %d", inMemRows->queryChunk());
#endif
                return; // caught up whilst blocked
            }
            VALIDATELT(chunkPool.ordinality(), maxPoolChunks);
        }
#ifdef TRACE_WRITEAHEAD
        ActPrintLogEx(&activity->queryContainer(), thorlog_all, MCdebugProgress, "Flushed chunk = %d, chunkPool chunks = %d", inMemRows->queryChunk(), 1+chunkPool.ordinality());
#endif
        chunkPool.enqueue(inMemRows.getClear());
    }
    virtual size32_t rowSize(const void *row)
    {
        if (!row)
            return 1; // eog;
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)row);
        return ssz.size();
    }
public:
    CSharedWriteAheadMem(CActivityBase *activity, unsigned outputCount, IThorRowInterfaces *rowif, unsigned buffSize) : CSharedWriteAheadBase(activity, outputCount, rowif)
    {
        if (((unsigned)-1) == buffSize)
            maxPoolChunks = (unsigned)-1; // no limit
        else
        {
            maxPoolChunks = buffSize / minChunkSize;
            if (maxPoolChunks < MIN_POOL_CHUNKS)
                maxPoolChunks = MIN_POOL_CHUNKS;
        }
        writerBlocked = false;
    }
    ~CSharedWriteAheadMem()
    {
        for (;;)
        {
            Owned<CRowSet> rowSet = chunkPool.dequeue();
            if (!rowSet)
                break;
        }
    }
    virtual void reset()
    {
        CSharedWriteAheadBase::reset();
        for (;;)
        {
            Owned<CRowSet> rowSet = chunkPool.dequeue();
            if (!rowSet)
                break;
        }
        writerBlocked = false;
    }
};

ISharedSmartBuffer *createSharedSmartMemBuffer(CActivityBase *activity, unsigned outputs, IThorRowInterfaces *rowIf, unsigned buffSize)
{
    return new CSharedWriteAheadMem(activity, outputs, rowIf, buffSize);
}


// This implementation is supplied with the input, and reads from it on demand, initially to memory.
// It will spill to disk if the configurable memory limit is exceeded.
// The leading reader(output) causes the implementation to read more rows from the input.
// Once the leader causes the rows in memory to exceed the memory limit, it will cause a output stream to be created.
// From that point on, the leader will write blocks of rows out to disk,
// and cause all readers to read from it, once they have exhaused the in-memory row set.

class CSharedFullSpillingWriteAhead : public CInterfaceOf<ISharedRowStreamReader>
{
    typedef std::vector<const void *> Rows;
    class COutputRowStream : public CSimpleInterfaceOf<IRowStream>
    {
        CSharedFullSpillingWriteAhead &owner;
        unsigned whichOutput = 0;
        size32_t localRowsIndex = 0;
        rowcount_t lastKnownAvailable = 0;
        rowcount_t currentRow = 0;
        Rows rows;
        OwnedIFileIO iFileIO;
        Owned<IEngineRowAllocator> allocator;
        Owned<IBufferedSerialInputStream> inputStream;
        CThorStreamDeserializerSource ds;
        std::atomic<bool> eof = false;

        inline const void *getClearRow(unsigned i)
        {
            const void *row = rows[i];
            rows[i] = nullptr;
            return row;
        }
        void freeRows()
        {
            for (auto it = rows.begin() + localRowsIndex; it != rows.end(); ++it)
                ReleaseThorRow(*it);
            rows.clear();
            localRowsIndex = 0;
            allocator->emptyCache();
        }
        const void *getRowFromStream()
        {
            if (currentRow == lastKnownAvailable)
            {
                if (!owner.checkWriteAhead(lastKnownAvailable))
                {
                    eof = true;
                    return nullptr;
                }
            }
            if (owner.inputGrouped)
            {
                bool eog;
                inputStream->read(sizeof(bool), &eog);
                if (eog)
                {
                    currentRow++;
                    return nullptr;
                }
            }
            currentRow++;
            RtlDynamicRowBuilder rowBuilder(allocator);
            size32_t sz = owner.deserializer->deserialize(rowBuilder, ds);
            return rowBuilder.finalizeRowClear(sz);
        }
    public:
        explicit COutputRowStream(CSharedFullSpillingWriteAhead &_owner, unsigned _whichOutput)
            : owner(_owner), whichOutput(_whichOutput)
        {
            allocator.setown(owner.activity.getRowAllocator(owner.meta, (roxiemem::RoxieHeapFlags)owner.options.heapFlags));
        }
        ~COutputRowStream()
        {
            freeRows();
        }
        rowcount_t queryLastKnownAvailable() const
        {
            return lastKnownAvailable;
        }
        void setLastKnownAvailable(rowcount_t _lastKnownWritten)
        {
            lastKnownAvailable = _lastKnownWritten;
        }
        void cancel()
        {
            eof = true;
        }
        void reset()
        {
            freeRows();
            ds.setStream(nullptr);
            iFileIO.clear();
            inputStream.clear();
            eof = false;
            currentRow = 0;
            lastKnownAvailable = 0;
        }
        virtual const void *nextRow() override
        {
            if (eof)
                return nullptr;
            else if (localRowsIndex < rows.size()) // NB: no longer used after inputStream is set
            {
                currentRow++;
                return getClearRow(localRowsIndex++);
            }
            else if (inputStream)
                return getRowFromStream(); // NB: will increment currentRow
            else
            {
                localRowsIndex = 0;
                rows.clear();

                if (owner.getRowsInMem(rows, lastKnownAvailable))
                {
                    if (rows.empty())
                    {
                        eof = true;
                        return nullptr;
                    }
                    else
                    {
                        currentRow++;
                        return getClearRow(localRowsIndex++);
                    }
                }
                else
                {
                    auto [_inputStream, _iFileIO] = owner.getReadStream();
                    inputStream.setown(_inputStream);
                    iFileIO.setown(_iFileIO);
                    ds.setStream(inputStream);
                    return getRowFromStream(); // NB: will increment currentRow
                }
            }
        }
        virtual void stop() override
        {
            freeRows();
            ds.setStream(nullptr);

            if (inputStream)
            {
                StringBuffer tracing;
                getFileIOStats(tracing, iFileIO);
                owner.activity.ActPrintLog("CSharedFullSpillingWriteAhead::COutputRowStream: input stream finished: output=%u%s", whichOutput, tracing.str());

                iFileIO.clear();
                inputStream.clear();
            }

            // NB: this will set lastKnownAvailable to max[(rowcount_t)-1] (within owner.readAheadCS) to prevent it being considered as lowest any longer
            owner.outputStopped(whichOutput);

            eof = true;
        }
    };
    CActivityBase &activity;
    Linked<IRowStream> input;
    unsigned numOutputs = 0;
    Linked<IOutputMetaData> meta;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    Linked<IEngineRowAllocator> allocator;
    std::vector<Owned<COutputRowStream>> outputs;
    std::deque<std::tuple<const void *, size32_t>> rows;
    memsize_t rowsMemUsage = 0;
    std::atomic<rowcount_t> totalInputRowsRead = 0; // not used until spilling begins, represents count of all rows read
    rowcount_t inMemTotalRows = 0; // whilst in memory, represents count of all rows seen
    CriticalSection readAheadCS; // ensure single reader (leader), reads ahead (updates rows/totalInputRowsRead/inMemTotalRows)
    Owned<CFileOwner> tempFileOwner;
    Owned<IFileIO> iFileIO;
    Owned<IBufferedSerialOutputStream> outputStream;
    Linked<ICompressHandler> compressHandler;
    bool nextInputReadEog = false;
    bool endOfInput = false;
    bool inputGrouped = false;
    SharedRowStreamReaderOptions options;
    size32_t inMemReadAheadGranularity = 0;
    CRuntimeStatisticCollection inactiveStats;
    CRuntimeStatisticCollection previousFileStats;
    StringAttr baseTmpFilename;


    rowcount_t getLowestOutput()
    {
        // NB: must be called with readAheadCS held
        rowcount_t trailingRowPos = (rowcount_t)-1;
        for (auto &output: outputs)
        {
            rowcount_t outputLastKnownWritten = output->queryLastKnownAvailable();
            if (outputLastKnownWritten < trailingRowPos)
                trailingRowPos = outputLastKnownWritten;
        }
        return trailingRowPos;
    }
    inline rowcount_t getStartIndex()
    {
        rowcount_t nr = rows.size();
        return inMemTotalRows - nr;
    }
    inline unsigned getRelativeIndex(rowcount_t index)
    {
        rowcount_t startIndex = getStartIndex();
        return (unsigned)(index - startIndex);
    }
    void closeWriter()
    {
        if (outputStream)
        {
            outputStream.clear();
            iFileIO->flush();
            tempFileOwner->noteSize(iFileIO->getStatistic(StSizeDiskWrite));
            updateRemappedStatsDelta(inactiveStats, previousFileStats, iFileIO, diskToTempStatsMap); // NB: also updates prev to current
            previousFileStats.reset();
            iFileIO.clear();
        }
    }
    void createOutputStream()
    {
        closeWriter(); // Ensure stats from closing files are preserved in inactiveStats
        // NB: Called once, when spilling starts.
        tempFileOwner.setown(activity.createOwnedTempFile(baseTmpFilename));
        auto res = createSerialOutputStream(&(tempFileOwner->queryIFile()), compressHandler, options, numOutputs + 1);
        outputStream.setown(std::get<0>(res));
        iFileIO.setown(std::get<1>(res));
        totalInputRowsRead = inMemTotalRows;
        inactiveStats.addStatistic(StNumSpills, 1);
    }
    void writeRowsFromInput()
    {
        // NB: the leading output will be calling this, and it could populate 'outputRows' as it reads ahead
        // but we want to readahead + write to disk, more than we want to retain in memory, so keep it simple,
        // flush all to disk, meaning this output will also read them back off disk (hopefully from Linux page cache)
        rowcount_t newRowsWritten = 0;
        offset_t serializedSz = 0;
        COutputStreamSerializer outputStreamSerializer(outputStream);
        while (!activity.queryAbortSoon())
        {
            OwnedConstThorRow row = input->nextRow();
            if (nullptr == row)
            {
                if (!inputGrouped || nextInputReadEog)
                {
                    endOfInput = true;
                    break;
                }
                nextInputReadEog = true;
                outputStream->put(sizeof(bool), &nextInputReadEog);
                newRowsWritten++;
            }
            else
            {
                if (inputGrouped)
                {
                    nextInputReadEog = false;
                    outputStream->put(sizeof(bool), &nextInputReadEog);
                }
                serializer->serialize(outputStreamSerializer, (const byte *)row.get());
                newRowsWritten++;
                size32_t rowSz = thorRowMemoryFootprint(serializer, row);
                serializedSz += rowSz;
                if (serializedSz >= options.writeAheadSize)
                    break;
            }
        }
        outputStream->flush();
        totalInputRowsRead.fetch_add(newRowsWritten);
        tempFileOwner->noteSize(iFileIO->getStatistic(StSizeDiskWrite));
        updateRemappedStatsDelta(inactiveStats, previousFileStats, iFileIO, diskToTempStatsMap); // NB: also updates prev to current
        // JCSMORE - could track size written, and start new file at this point (e.g. every 100MB),
        // and track their starting points (by row #) in a vector
        // We could then tell if/when the readers catch up, and remove consumed files as they do.
    }
    void freeRows()
    {
        for (auto &row: rows)
            ReleaseThorRow(std::get<0>(row));
    }
public:
    explicit CSharedFullSpillingWriteAhead(CActivityBase *_activity, unsigned _numOutputs, IRowStream *_input, bool _inputGrouped, const SharedRowStreamReaderOptions &_options, IThorRowInterfaces *rowIf, const char *_baseTmpFilename, ICompressHandler *_compressHandler)
        : activity(*_activity), numOutputs(_numOutputs), input(_input), inputGrouped(_inputGrouped), options(_options), compressHandler(_compressHandler), baseTmpFilename(_baseTmpFilename),
        meta(rowIf->queryRowMetaData()), serializer(rowIf->queryRowSerializer()), allocator(rowIf->queryRowAllocator()), deserializer(rowIf->queryRowDeserializer()),
        inactiveStats(spillStatistics), previousFileStats(spillStatistics)
    {
        assertex(input);

        // cap inMemReadAheadGranularity to inMemMaxMem
        inMemReadAheadGranularity = options.inMemReadAheadGranularity;
        if (inMemReadAheadGranularity > options.inMemMaxMem)
            inMemReadAheadGranularity = options.inMemMaxMem;

        for (unsigned o=0; o<numOutputs; o++)
            outputs.push_back(new COutputRowStream(*this, o));
    }
    ~CSharedFullSpillingWriteAhead()
    {
        closeWriter();
        freeRows();
    }
    void outputStopped(unsigned output)
    {
        bool allStopped = false;
        {
            // Mark finished output with max, so that it is not considered by getLowestOutput()
            CriticalBlock b(readAheadCS); // read ahead could be active and considering this output
            outputs[output]->setLastKnownAvailable((rowcount_t)-1);
            if ((rowcount_t)-1 == getLowestOutput())
                allStopped = true;
        }
        if (allStopped)
        {
            if (totalInputRowsRead) // only set if spilt
            {
                StringBuffer tracing;
                getFileIOStats(tracing, iFileIO);
                activity.ActPrintLog("CSharedFullSpillingWriteAhead::outputStopped closing tempfile writer: %s %s", tempFileOwner->queryIFile().queryFilename(), tracing.str());
                closeWriter();
                tempFileOwner.clear();
            }
        }
    }
    std::tuple<IBufferedSerialInputStream *, IFileIO *> getReadStream() // also pass back IFileIO for stats purposes
    {
        return createSerialInputStream(&(tempFileOwner->queryIFile()), compressHandler, options, numOutputs + 1); // +1 for writer
    }
    bool checkWriteAhead(rowcount_t &outputRowsAvailable)
    {
        if (totalInputRowsRead == outputRowsAvailable)
        {
            CriticalBlock b(readAheadCS);
            if (totalInputRowsRead == outputRowsAvailable) // if not, then since gaining the crit, totalInputRowsRead has changed
            {
                if (endOfInput)
                    return false;
                writeRowsFromInput();
                if (totalInputRowsRead == outputRowsAvailable) // no more were written
                {
                    dbgassertex(endOfInput);
                    return false;
                }
            }
        }
        outputRowsAvailable = totalInputRowsRead;
        return true;
    }
    bool getRowsInMem(Rows &outputRows, rowcount_t &outputRowsAvailable)
    {
        CriticalBlock b(readAheadCS);
        if (outputRowsAvailable == inMemTotalRows) // load more
        {
            // prune unused rows
            rowcount_t trailingRowPosRelative = getRelativeIndex(getLowestOutput());
            for (auto it = rows.begin(); it != rows.begin() + trailingRowPosRelative; ++it)
            {
                auto [row, rowSz] = *it;
                rowsMemUsage -= rowSz;
                ReleaseThorRow(row);
            }
            rows.erase(rows.begin(), rows.begin() + trailingRowPosRelative);

            if (outputStream)
            {
                // this will be the last time this output calls getRowsInMem
                // it has exhausted 'rows', and will from here on in read from outputStream
                return false;
            }

            if (rowsMemUsage >= options.inMemMaxMem) // too much in memory, spill
            {
                // NB: this will reset rowMemUsage, however, each reader will continue to consume rows until they catch up (or stop)
                createOutputStream();
                ActPrintLog(&activity, "Spilling to temp storage [file = %s, outputRowsAvailable = %" I64F "u, start = %" I64F "u, end = %" I64F "u, count = %u]", tempFileOwner->queryIFile().queryFilename(), outputRowsAvailable, inMemTotalRows - rows.size(), inMemTotalRows, (unsigned)rows.size());
                return false;
            }

            // read more, up to inMemReadAheadGranularity or inMemReadAheadGranularityRows before relinquishing
            rowcount_t previousNumRows = rows.size();
            while (true)
            {
                const void *row = input->nextRow();
                if (row)
                {
                    nextInputReadEog = false;
                    size32_t sz = thorRowMemoryFootprint(serializer, row);
                    rows.emplace_back(row, sz);
                    rowsMemUsage += sz;
                    if ((rowsMemUsage >= options.inMemReadAheadGranularity) ||
                        (rows.size() >= options.inMemReadAheadGranularityRows))
                        break;
                }
                else
                {
                    if (!inputGrouped || nextInputReadEog)
                        break;
                    else
                    {
                        nextInputReadEog = true;
                        rows.emplace_back(nullptr, 0);
                    }
                }
            }
            inMemTotalRows += rows.size() - previousNumRows;
        }
        else
        {
            // this output has not yet reached inMemTotalRows
            dbgassertex(outputRowsAvailable < inMemTotalRows);
        }

        rowcount_t newRowsAdded = 0;
        for (auto it = rows.begin() + getRelativeIndex(outputRowsAvailable); it != rows.end(); ++it)
        {
            const void *row = std::get<0>(*it);
            LinkThorRow(row);
            outputRows.push_back(row);
            newRowsAdded++;
        }
        outputRowsAvailable = outputRowsAvailable+newRowsAdded;

        return true;
    }
// ISharedRowStreamReader impl.
    virtual IRowStream *queryOutput(unsigned output) override
    {
        return outputs[output];
    }
    virtual void cancel() override
    {
        for (auto &output: outputs)
            output->cancel();
    }
    virtual void reset() override
    {
        closeWriter();
        for (auto &output: outputs)
            output->reset();
        freeRows();
        rows.clear();
        rowsMemUsage = 0;
        totalInputRowsRead = 0;
        inMemTotalRows = 0;
        nextInputReadEog = false;
        endOfInput = false;
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) const override
    {
        return inactiveStats.getStatisticValue(kind);
    }
};

ISharedRowStreamReader *createSharedFullSpillingWriteAhead(CActivityBase *_activity, unsigned numOutputs, IRowStream *_input, bool _inputGrouped, const SharedRowStreamReaderOptions &options, IThorRowInterfaces *_rowIf, const char *tempFileName, ICompressHandler *compressHandler)
{
    return new CSharedFullSpillingWriteAhead(_activity, numOutputs, _input, _inputGrouped, options, _rowIf, tempFileName, compressHandler);
}


class CRowMultiWriterReader : public CSimpleInterface, implements IRowMultiWriterReader
{
    rowidx_t readGranularity, writeGranularity, rowPos, limit, rowsToRead;
    CThorSpillableRowArray rows;
    const void **readRows;
    CActivityBase &activity;
    IThorRowInterfaces *rowIf;
    bool readerBlocked, eos, eow;
    Semaphore emptySem, fullSem;
    unsigned numWriters, writersComplete, writersBlocked;

    class CAWriter : public CSimpleInterface, implements IRowWriter
    {
        CRowMultiWriterReader &owner;
        CThorExpandingRowArray rows;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CAWriter(CRowMultiWriterReader &_owner) : owner(_owner), rows(_owner.activity, _owner.rowIf)
        {
        }
        ~CAWriter()
        {
            flush();
            owner.writerStopped();
        }
    // IRowWriter impl.
        virtual void putRow(const void *row)
        {
            if (rows.ordinality() >= owner.writeGranularity)
                owner.addRows(rows);
            rows.append(row);
        }
        virtual void flush()
        {
            if (rows.ordinality())
                owner.addRows(rows);
        }
    };

    void addRows(CThorExpandingRowArray &inRows)
    {
        for (;;)
        {
            {
                CThorArrayLockBlock block(rows);
                if (eos)
                {
                    inRows.kill();
                    return;
                }
                if (rows.numCommitted() < limit)
                {
                    // NB: allowed to go over limit, by as much as inRows.ordinality()-1
                    rows.appendRows(inRows, true);
                    if (rows.numCommitted() >= readGranularity)
                        checkReleaseReader();
                    return;
                }
                writersBlocked++;
            }
            fullSem.wait();
        }
    }
    inline void checkReleaseReader()
    {
        if (readerBlocked)
        {
            emptySem.signal();
            readerBlocked = false;
        }
    }
    inline void checkReleaseWriters()
    {
        if (writersBlocked)
        {
            fullSem.signal(writersBlocked);
            writersBlocked = 0;
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowMultiWriterReader(CActivityBase &_activity, IThorRowInterfaces *_rowIf, unsigned _limit, unsigned _readGranularity, unsigned _writerGranularity)
        : activity(_activity), rowIf(_rowIf), rows(_activity, _rowIf), limit(_limit), readGranularity(_readGranularity), writeGranularity(_writerGranularity)
    {
        if (readGranularity > limit)
            readGranularity = limit; // readGranularity must be <= limit;
        numWriters = 0;
        readRows = static_cast<const void * *>(activity.queryRowManager()->allocate(readGranularity * sizeof(void*), activity.queryContainer().queryId()));
        eos = eow = readerBlocked = false;
        rowPos = rowsToRead = 0;
        writersComplete = writersBlocked = 0;
        rows.setup(rowIf, ers_forbidden, stableSort_none, true); // turning on throwOnOom;
    }
    ~CRowMultiWriterReader()
    {
        roxiemem::ReleaseRoxieRowRange(readRows, rowPos, rowsToRead);
        ReleaseThorRow(readRows);
    }
    void writerStopped()
    {
        CThorArrayLockBlock block(rows);
        writersComplete++;
        if (writersComplete == numWriters)
        {
            rows.flush();
            eow = true;
            checkReleaseReader();
        }
    }
// ISharedWriteBuffer impl.
    virtual IRowWriter *getWriter()
    {
        CThorArrayLockBlock block(rows);
        ++numWriters;
        return new CAWriter(*this);
    }
    virtual void abort()
    {
        CThorArrayLockBlock block(rows);
        eos = true;
        checkReleaseWriters();
        checkReleaseReader();
    }
// IRowStream impl.
    virtual const void *nextRow()
    {
        if (eos)
            return NULL;
        if (rowPos == rowsToRead)
        {
            for (;;)
            {
                {
                    CThorArrayLockBlock block(rows);
                    if (rows.numCommitted() >= readGranularity || eow)
                    {
                        rowsToRead = (eow && rows.numCommitted() < readGranularity) ? rows.numCommitted() : readGranularity;
                        if (0 == rowsToRead)
                        {
                            eos = true;
                            return NULL;
                        }
                        rows.readBlock(readRows, rowsToRead);
                        rowPos = 0;
                        checkReleaseWriters();
                        break; // fall through to return a row
                    }
                    readerBlocked = true;
                }
                emptySem.wait();
                if (eos)
                    return NULL;
            }
        }
        const void *row = readRows[rowPos];
        readRows[rowPos] = NULL;
        ++rowPos;
        return row;
    }
    virtual void stop()
    {
        eos = true;
        checkReleaseWriters();
    }
};

IRowMultiWriterReader *createSharedWriteBuffer(CActivityBase *activity, IThorRowInterfaces *rowif, unsigned limit, unsigned readGranularity, unsigned writeGranularity)
{
    return new CRowMultiWriterReader(*activity, rowif, limit, readGranularity, writeGranularity);
}




class CRCFileStream: public CSimpleInterface, implements IFileIOStream
{
    Linked<IFileIOStream> streamin;
public:
    CRC32 &crc;
    bool valid;

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRCFileStream(IFileIOStream *_stream,CRC32 &_crc)
        : streamin(_stream), crc(_crc)
    {
        valid = true;
    }


    size32_t read(size32_t len, void * data)
    {
        size32_t rd = streamin->read(len,data);
        crc.tally(rd,data);
        return rd;
    }
    
    size32_t write(size32_t len, const void * data)
    {
        throw MakeStringException(-1,"CRCFileStream does not support write");
    }

    void seek(offset_t pos, IFSmode origin)
    {
        offset_t t = streamin->tell();
        streamin->seek(pos,origin);
        if (t!=streamin->tell())  // crc invalid
            if (valid) {
                IWARNLOG("CRCFileStream::seek called - CRC will be invalid");
                valid = false;
            }
    }
    offset_t size()
    {
        return streamin->size();
    }

    virtual offset_t tell()
    {
        return streamin->tell();
    }

    void flush()
    {
        // noop as only read
    }
};

