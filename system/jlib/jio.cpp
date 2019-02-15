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


#include "platform.h"
#include <algorithm>

#include "jfile.hpp"
#include "jthread.hpp"
#include "jio.ipp"
#include "jlzw.ipp"
#include "jmisc.hpp"
#include <time.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jqueue.tpp"

#ifdef _WIN32
#include <io.h>
#endif

#define DEFAULTBUFFERSIZE 0x10000  // 64K
#define RANDOM_BUFFER_SIZE                  DEFAULTBUFFERSIZE
#define MAX_RANDOM_CACHE_SIZE               0x10000
#define RANDOM_CACHE_DEPTH                  10

#define threshold 1024
#define timelimit 100

#define MINCOMPRESSEDROWSIZE 16
#define MAXCOMPRESSEDROWSIZE 0x4000

static unsigned ioRetryCount=0;
void setIORetryCount(unsigned _ioRetryCount) // non atomic, expected to be called just once at process start up.
{
    ioRetryCount = _ioRetryCount;
    PROGLOG("setIORetryCount set to : %d", ioRetryCount);
}

extern jlib_decl offset_t checked_lseeki64( int handle, offset_t offset, int origin )
{
    offset_t ret=_lseeki64(handle,offset,origin);
    if (ret==(offset_t)-1) 
        throw makeErrnoException("checked_lseeki64");
    return ret;
}

extern jlib_decl size32_t checked_read(int file, void *buffer, size32_t len)
{
    if (0==len) return 0;
    unsigned attempts = 0;
    size32_t ret = 0;
    unsigned __int64 startCycles = get_cycles_now();
    for (;;)
    {
        ssize_t readNow = _read(file, buffer, len);
        if (readNow == (ssize_t)-1)
        {
            switch (errno)
            {
            case EINTR:
                readNow = 0;
                break;
            default:
                if (attempts < ioRetryCount)
                {
                    attempts++;
                    StringBuffer callStr("read");
                    callStr.append("[errno=").append(errno);
                    unsigned __int64 elapsedMs = cycle_to_nanosec(get_cycles_now() - startCycles)/1000000;
                    callStr.append(", took=").append(elapsedMs);
                    callStr.append(", attempt=").append(attempts).append("](handle=");
                    callStr.append(file).append(", len=").append(len).append(")");
                    PROGLOG("%s", callStr.str());
                    readNow = 0;
                    break;
                }
                throw makeErrnoException(errno, "checked_read");
            }
        }
        else if (!readNow)
            break;
        ret += readNow;
        if (readNow == (ssize_t)len)
            break;
        buffer = ((char *) buffer) + readNow;
        len -= readNow;
    }
    return ret;
}

#ifdef WIN32
static bool atomicsupported = true;
static CriticalSection atomicsection;
#endif

extern jlib_decl size32_t checked_pread(int file, void *buffer, size32_t len, offset_t pos)
{
    if (0==len) return 0;
#ifdef WIN32
    if (atomicsupported)
    {
        HANDLE hFile = (HANDLE)_get_osfhandle(file);
        DWORD rread;
        OVERLAPPED overlapped;
        memset(&overlapped, 0, sizeof(overlapped));
        overlapped.Offset = (DWORD) pos;
        overlapped.OffsetHigh = (DWORD)(pos>>32);
        if (ReadFile(hFile, buffer, len, &rread, &overlapped))
            return rread;
        int err = (int)GetLastError();
        if (err == ERROR_HANDLE_EOF)
            return 0;
        if (err == ERROR_INVALID_PARAMETER) // Win98 etc
            atomicsupported = false;
        else
            throw makeOsException(GetLastError(), "checked_pread");
    }
    {
        CriticalBlock blk(atomicsection);
        checked_lseeki64(file, pos, FILE_BEGIN);
        return checked_read(file, buffer, len);
    }
#else
    size32_t ret = 0;
    unsigned attempts = 0;
    unsigned __int64 startCycles = get_cycles_now();
    for (;;)
    {
        ssize_t readNow = ::pread(file, buffer, len, pos);
        if (readNow == (ssize_t)-1)
        {
            switch (errno)
            {
            case EINTR:
                readNow = 0;
                break;
            default:
                if (attempts < ioRetryCount)
                {
                    attempts++;
                    StringBuffer callStr("pread");
                    callStr.append("[errno=").append(errno);
                    unsigned __int64 elapsedMs = cycle_to_nanosec(get_cycles_now() - startCycles)/1000000;
                    callStr.append(", took=").append(elapsedMs);
                    callStr.append(", attempt=").append(attempts).append("](handle=");
                    callStr.append(file).append(", pos=").append(pos).append(", len=").append(len).append(")");
                    PROGLOG("%s", callStr.str());
                    readNow = 0;
                    break;
                }
                throw makeErrnoException(errno, "checked_pread");
            }
        }
        else if (!readNow)
            break;
        ret += readNow;
        if (readNow == (ssize_t)len)
            break;
        pos += readNow;
        buffer = ((char *) buffer) + readNow;
        len -= readNow;
    }
    return ret;
#endif
}

extern jlib_decl size32_t checked_write( int handle, const void *buffer, size32_t count )
{
    int ret=_write(handle,buffer,count);
    if ((size32_t)ret != count)
    {
        throw makeErrnoException((ret==-1)?errno:DISK_FULL_EXCEPTION_CODE, "checked_write");
    }
    return (size32_t)ret;
}

class CReadSeq : public IReadSeq, public CInterface
{
    int fh;
    size32_t size;  
    char *buffer;
    char *ptr;
    size32_t bufSize;
    size32_t bytesInBuffer;
    offset_t startpos;
    offset_t endpos;
    offset_t nextbufpos;
    bool compressed;
    void *prev;
    size32_t maxcompsize;
    bool first;

    inline unsigned remaining()
    {
        return (unsigned)(buffer+bytesInBuffer-ptr);
    }

    size32_t getBytes(void *dst, size32_t _size)
    {
        size32_t left = remaining();
        size32_t read = 0;
        while (_size>left) {
            if (left) {
                memcpy(dst, ptr, left);
                dst = (char *)dst + left;
                _size -= left;
                read += left;
                ptr+=left;
            }
            refill();
            left = bytesInBuffer;
            if (!left)
                return read;
        }
        memcpy(dst, ptr, _size);
        ptr += _size;
        read += _size;
        return read;
    }

    void refill()
    {
        size32_t left = remaining();
        memmove(buffer,ptr,left);
        size32_t rd=bufSize-left;
        if (endpos-nextbufpos<(offset_t)rd)
            rd = (size32_t)(endpos-nextbufpos);
        if (rd) 
            rd = checked_pread(fh, buffer+left, rd, nextbufpos);
        nextbufpos += rd;
        bytesInBuffer = left+rd;
        ptr = buffer;
    }


public:
    IMPLEMENT_IINTERFACE;

    CReadSeq(int _fh, offset_t _offset, unsigned maxrecs, size32_t _size, size32_t _bufsize, bool _compressed)
    {
        assertex(_size);
        fh = _fh; 
        size = _size;
        bufSize = (_bufsize==(unsigned) -1)?DEFAULTBUFFERSIZE:_bufsize;
        bytesInBuffer = 0;
        startpos = _offset;
        nextbufpos = _offset;   
        compressed = ((size<bufSize/2)&&(size>=MINCOMPRESSEDROWSIZE)&&(size<=MAXCOMPRESSEDROWSIZE))?_compressed:false;
        if (compressed) {
            maxcompsize = size+size/3+3; // migger than needed
            buffer = (char *) malloc(bufSize+size);
            prev = buffer+bufSize;
        }
        else
            buffer = (char *) malloc(bufSize);
        ptr = buffer;
        first = true;
        endpos = (maxrecs!=(unsigned)-1)?(_offset+(offset_t)maxrecs*(offset_t)_size):I64C(0x7ffffffffff);

    }

    ~CReadSeq() 
    {
        free(buffer);
    }

    virtual bool get(void *dst)
    {
        if (!compressed) 
            return getBytes(dst, size)==size;
        return (getn(dst,1)==1);
    }

    virtual unsigned getn(void *dst, unsigned n)
    {
        if (!compressed)
            return getBytes(dst, size*n)/size;
        byte *d = (byte *)dst;
        byte *e = d+(size*n);
        byte *p = (byte *)prev;
        unsigned ret = 0;
        while (d!=e) {
            if (first) {
                if (getBytes(d, size)!=size)
                    break;
                first = false;
            }
            else {
                if (remaining()<maxcompsize) 
                    refill();
                if (remaining()==0)
                    break;
                ptr += DiffExpand(ptr,d,p,size);
            }
            p = d;
            d += size;
            ret++;
        }
        if (ret)    // we got at least 1 so copy to prev
            memcpy(prev,e-size,size);
        return ret;
    }

    virtual unsigned getRecordSize() 
    { 
        return size; 
    }
    virtual void reset()
    {
        nextbufpos = startpos;
        ptr = buffer;
        bytesInBuffer = 0;
        first = true;
    }
    virtual void stop() 
    {
        free(buffer);   // no one should access after stop
        buffer = NULL;
    } 
};





IReadSeq *createReadSeq(int fh, offset_t _offset, size32_t size, size32_t bufsize, unsigned maxrecs, bool compressed)
{
    if (!bufsize) {
        IReadSeq *seq=new CUnbufferedReadWriteSeq(fh, _offset, size);
        seq->reset(); // not done itself 
        return seq;
    }
    return new CReadSeq(fh, _offset, maxrecs, size, bufsize, compressed);
}



//================================================================================================


class CWriteSeq : public IWriteSeq, public CInterface
{
private:
    int fh;
    size32_t size;
    char *buffer;
    char *ptr;
    size32_t bufSize;
    offset_t fpos;
    bool compressed;
    size32_t maxcompsize;
    void *prev;
    void *aux;
    bool first;

    inline size32_t remaining() 
    {
        return (size32_t)(bufSize - (ptr-buffer));
    }

    void putBytes(const void *src, size32_t _size)
    {
        fpos += _size;
        size32_t left = remaining();
        if (_size>left)
        {
            if (ptr!=buffer) { // don't buffer if entire block
                memcpy(ptr, src, left);
                ptr += left;
                src = (char *)src + left;
                _size -= left;
                flush();
                left = bufSize;
            }
            while (_size>=bufSize) // write out directly
            {
                checked_write(fh, src, bufSize); // stick to writing bufSize blocks
                src = (char *)src + bufSize;
                _size -= bufSize;
            }
        }
        memcpy(ptr, src, _size);
        ptr += _size;
    }

public:
    IMPLEMENT_IINTERFACE;

    CWriteSeq(int _fh, size32_t _size, size32_t _bufsize, bool _compressed)
    {
        assertex(_fh);
        assertex(_size);
        fh = _fh; 
        size = _size;
        fpos = 0;
        if (_bufsize == (unsigned) -1)
            _bufsize = DEFAULTBUFFERSIZE;
        bufSize = _bufsize;
        
        compressed = ((size<bufSize/2)&&(size>=MINCOMPRESSEDROWSIZE)&&(size<=MAXCOMPRESSEDROWSIZE))?_compressed:false;
        if (compressed) {
            maxcompsize = size+size/3+3; // bigger than needed
            buffer = (char *) malloc(bufSize+size+maxcompsize);
            prev = buffer+bufSize;
            aux = (char *)prev+size;
        }
        else
            buffer = (char *) malloc(bufSize);

        ptr = buffer;
        first = true;
    }

    ~CWriteSeq()
    {
        free(buffer);
    }


    void put(const void *src)
    {
        if (compressed) {
            if (first) {
                first = false;
                memcpy(prev,src,size);
            }
            else if (remaining()>=maxcompsize) {
                size32_t sz = DiffCompress(src,ptr,prev,size);
                fpos += sz;
                ptr += sz;
                return;
            }
            else { 
                putBytes(aux, DiffCompress(src,aux,prev,size));
                return;
            }
        }
        putBytes(src, size);
    }

    void putn(const void *src, unsigned numRecs)
    {
        if (compressed) {
            while (numRecs) {
                put(src);
                src = (byte *)src+size;
                numRecs--;
            }
        }
        else
            putBytes(src, size*numRecs);
    }

    void flush()
    {
        if (ptr != buffer)
        {
            checked_write(fh, buffer, (size32_t)(ptr-buffer));
            ptr = buffer;
        }
    }

    offset_t getPosition()
    {
        return fpos;
    }

    virtual size32_t getRecordSize() 
    { 
        return size; 
    }

};

IWriteSeq *createWriteSeq(int fh, size32_t size, size32_t bufsize, bool compressed)
{
    // Async TBD
    if (!bufsize)
        return new CUnbufferedReadWriteSeq(fh, 0, size);
    else
        return new CWriteSeq(fh, size, bufsize,compressed);
}

IWriteSeq *createTeeWriteSeq(IWriteSeq *f1, IWriteSeq *f2)
{
    return new CTeeWriteSeq(f1, f2);
}

//===========================================================================================

CUnbufferedReadWriteSeq::CUnbufferedReadWriteSeq(int _fh, offset_t _offset, size32_t _size)
{
    fh = _fh;
    size = _size;
    offset = _offset;
    fpos = _offset;

}

void CUnbufferedReadWriteSeq::put(const void *src)
{
    checked_write(fh, src, size);
    fpos += size;
}

void CUnbufferedReadWriteSeq::putn(const void *src, unsigned n)
{
    checked_write(fh, src, size*n);
    fpos += size*n;
}

void CUnbufferedReadWriteSeq::flush()
{}

offset_t CUnbufferedReadWriteSeq::getPosition()
{
    return fpos;
}

bool CUnbufferedReadWriteSeq::get(void *dst)
{
    size32_t toread = size;
    while (toread)
    {
        int read = checked_read(fh, dst, toread);
        if (!read) 
            return false;
        toread -= read;
        dst = (char *) dst + read;
    }
    return true;
}

unsigned CUnbufferedReadWriteSeq::getn(void *dst, unsigned n)
{
    size32_t toread = size*n;
    size32_t totread = 0;
    while (toread)
    {
        int read = checked_read(fh, dst, toread);
        if (!read) 
            break;
        toread -= read;
        totread += read;
        dst = (char *) dst + read;
    }
    return totread/size;
}

void CUnbufferedReadWriteSeq::reset()
{
    checked_lseeki64(fh, offset, SEEK_SET);
    fpos = offset;
}

//===========================================================================================


//===========================================================================================

CTeeWriteSeq::CTeeWriteSeq(IWriteSeq *_f1, IWriteSeq *_f2)
{
    w1 = _f1;
    w1->Link();
    w2 = _f2;
    w2->Link();
    assertex(w1->getRecordSize()==w2->getRecordSize());
}

CTeeWriteSeq::~CTeeWriteSeq()
{
    w1->Release();
    w2->Release();
}

void CTeeWriteSeq::put(const void *src)
{
    w1->put(src);
    w2->put(src);
}

void CTeeWriteSeq::putn(const void *src, unsigned n)
{
    w1->putn(src, n);
    w2->putn(src, n);
}

void CTeeWriteSeq::flush()
{
    w1->flush();
    w2->flush();
}

size32_t CTeeWriteSeq::getRecordSize()
{
    return w1->getRecordSize();
}

offset_t CTeeWriteSeq::getPosition()
{
    return w1->getPosition();
}

//==================================================================================================

class CFixedRecordSize: public IRecordSize, public CInterface
{
protected:
    size32_t recsize;
public:
    IMPLEMENT_IINTERFACE;

    CFixedRecordSize(size32_t _recsize) { recsize=_recsize; }
    virtual size32_t getRecordSize(const void *)
    {
        return recsize;
    }
    virtual size32_t getFixedSize() const
    {
        return recsize;
    }
    virtual size32_t getMinRecordSize() const
    {
        return recsize;
    }
};

IRecordSize *createFixedRecordSize(size32_t recsize)
{
    return new CFixedRecordSize(recsize);
}


class CDeltaRecordSize: public IRecordSize, public CInterface
{
protected:
    Owned<IRecordSize> recordSize;
    int delta;
public:
    CDeltaRecordSize(IRecordSize * _recordSize, int _delta) { recordSize.set(_recordSize); delta = _delta; }
    IMPLEMENT_IINTERFACE;

    virtual size32_t getRecordSize(const void * data)
    {
        return recordSize->getRecordSize(data) + delta;
    }
    virtual size32_t getFixedSize() const
    {
        return recordSize->getFixedSize()?recordSize->getFixedSize()+delta:0;
    }
    virtual size32_t getMinRecordSize() const
    {
        return recordSize->getMinRecordSize() + delta;
    }
};

extern jlib_decl IRecordSize *createDeltaRecordSize(IRecordSize * size, int delta)
{
    if (delta == 0)
        return LINK(size);
    return new CDeltaRecordSize(size, delta);
}

//==================================================================================================

// Elevator scanning
#define MAX_PENDING 20000

class ElevatorScanner;

class PendingFetch : public IInterface, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    static int compare(const void *a, const void *b);

    offset_t pos;
    IReceiver *receiver;
    void *target;
    IRecordFetchChannel *channel;
};

class ElevatorChannel : implements IRecordFetchChannel, public CInterface
{
private:
    bool cancelled;
    bool immediate;
    ElevatorScanner &scanner;
public:
    IMPLEMENT_IINTERFACE;

    ElevatorChannel(ElevatorScanner &, bool);
    ~ElevatorChannel();

//Interface IRecordFetchChannel
    virtual void fetch(offset_t pos, void *buffer, IReceiver *receiver);
    virtual void flush();
    virtual void abort() { cancelled = true; }
    virtual bool isAborted() { return cancelled; }
    virtual bool isImmediate() { return immediate; }
};

class ElevatorScanner : public Thread, public IRecordFetcher
{
private:
    Monitor scanlist;
    Monitor isRoom;
    PendingFetch pending[MAX_PENDING];
    unsigned nextSlot;
    size32_t recordSize;
    int file;
    offset_t reads;
    unsigned scans;
    bool stopped;
    unsigned duetime;

    void scan();
    void doFetch(PendingFetch &);
    void stop();
    void resetTimer()
    {
        duetime = msTick()+timelimit;
    }

public:
    IMPLEMENT_IINTERFACE;
    virtual void beforeDispose();
    
    ElevatorScanner(int file, size32_t recordSize);
    ~ElevatorScanner();

//Interface IRecordFetcher
    virtual IRecordFetchChannel *openChannel(bool immediate) { return new ElevatorChannel(*this, immediate); }

//Interface Thread
    virtual int run();

    void flush(IRecordFetchChannel *);
    void fetch(offset_t, void *, IReceiver *, IRecordFetchChannel *);
};
    
int PendingFetch::compare(const void *a, const void *b)
{ 
    offset_t aa = ((PendingFetch *) a)->pos;
    offset_t bb = ((PendingFetch *) b)->pos;
    if (aa > bb)
        return 1;
    else if (aa == bb)
        return 0;
    else
        return -1;
}

ElevatorChannel::ElevatorChannel(ElevatorScanner &_scanner, bool _immediate) : scanner(_scanner)
{
    scanner.Link();
    cancelled = false;
    immediate = _immediate;
}

ElevatorChannel::~ElevatorChannel()
{
    flush();
    scanner.Release();
}

void ElevatorChannel::fetch(offset_t fpos, void *buffer, IReceiver *receiver)
{
    scanner.fetch(fpos, buffer, receiver, this);
}

void ElevatorChannel::flush()
{
    scanner.flush(this);
}

ElevatorScanner::ElevatorScanner(int _file, size32_t _recordSize) : Thread("ElevatorScanner")
{
    file = _file;
    recordSize = _recordSize;
    nextSlot = 0;
    reads = 0;
    scans = 0;
    stopped = false;
    start();
}

ElevatorScanner::~ElevatorScanner()
{
    IERRLOG("Elevator scanner statistics: %" I64F "d reads (%" I64F "d bytes), %d scans", reads, reads*recordSize, scans);
}

void ElevatorScanner::beforeDispose()
{
    stop();
    join();
}

void ElevatorScanner::fetch(offset_t fpos, void *buffer, IReceiver *receiver, IRecordFetchChannel *channel)
{
    synchronized procedure(scanlist);
    if (channel->isImmediate())
    {
        // MORE - atomic seek/read would be preferable!
        checked_lseeki64(file, fpos, SEEK_SET);
        checked_read(file, buffer, recordSize);
        reads++;
        if (!receiver->takeRecord(fpos))
            channel->abort();
        return;
    }
    {
        synchronized block(isRoom);
        while (nextSlot >= MAX_PENDING)
            isRoom.wait();
    }
    if (!channel->isAborted())
    {
        pending[nextSlot].pos = fpos;
        pending[nextSlot].receiver = receiver;
        pending[nextSlot].target = buffer;
        pending[nextSlot].channel = channel;
    
        nextSlot++;
        resetTimer();
        scanlist.notify();
    }
}

void ElevatorScanner::doFetch(PendingFetch &next)
{
    if (!next.channel->isAborted())
    {
        // MORE - atomic seek/read would be preferable!
        checked_lseeki64(file, next.pos, SEEK_SET);
        checked_read(file, next.target, recordSize);
        reads++;
        if (!next.receiver->takeRecord(next.pos))
            next.channel->abort();
    }
}

void ElevatorScanner::scan()
{
    DBGLOG("Starting elevator scan of %d items", nextSlot);
    scans++;
    qsort(pending, nextSlot, sizeof(pending[0]), PendingFetch::compare);
    for (unsigned i = 0; i < nextSlot; i++)
    {
        doFetch(pending[i]);
    }
    nextSlot = 0;
    {
        synchronized block(isRoom);
        isRoom.notify();
    }
    DBGLOG("Finished elevator scan");
}

void ElevatorScanner::flush(IRecordFetchChannel *)
{
    // MORE - I could just flush what was asked for, but I may as well flush the lot.
    synchronized procedure(scanlist);
    if (nextSlot)
        scan();
}

int ElevatorScanner::run()
{
    scanlist.lock();
    for (;;)
    {
        while (nextSlot<MAX_PENDING)
        {
            if (nextSlot)
            {
                //  if (!connections.length())
                //      break;
                int timeleft = (int)(duetime-msTick());             
                if (timeleft<=0)
                    break;
            }

            if (stopped)
            {
                scanlist.unlock();
                return 0;
            }
            // MORE - need a timeout on the wait!
            scanlist.wait();
        }
        scan();
    }
}

void ElevatorScanner::stop()
{
    synchronized procedure(scanlist);
    if (!stopped)
    {
        stopped = true;
        scanlist.notify();
    }
}

extern jlib_decl IRecordFetcher *createElevatorFetcher(int file, size32_t recSize)
{
    return new ElevatorScanner(file, recSize);
}

//==================================================================================================
// chained routines allowing multiple streams to be concatenated
// all streams assumed to have same record size


class CChainedWriteSeq : public IWriteSeq, public CInterface
{
protected:
    IWriteSeq *stream;
    IWriteSeqAllocator *allocator;
    unsigned num;
    size32_t recsize;
    offset_t pos;
public:
    IMPLEMENT_IINTERFACE;
    CChainedWriteSeq(IWriteSeqAllocator *_allocator) 
    {
        allocator = _allocator;
        allocator->Link();
        num = 0;
        recsize = 0;
        pos = 0;
    }
    virtual ~CChainedWriteSeq() 
    {
        ::Release(stream);
        allocator->Release();
    }
    void flush() { if (stream) stream->flush(); }
    void put(const void *dst) { putn(dst,1); }
    void putn(const void *dst, unsigned numrecs)
    {
        if (numrecs==0) return;
        if (stream==NULL) return; // could raise exception instead
        byte *out=(byte *)dst;
        while (numrecs>num) {
            stream->putn(out,num);
            pos+=num;
            numrecs-=num;
            stream->flush();
            IWriteSeq *oldstream=stream;
            stream = allocator->next(num);
            oldstream->Release();
            if (!stream) {
                return; // could raise exception
            }
        }
        stream->putn(out,numrecs);
        pos+=numrecs;
    }
    virtual size32_t getRecordSize() { if ((recsize==0)&&stream) recsize = stream->getRecordSize(); return recsize; }
    virtual offset_t getPosition()   { return pos; }
};


class CChainedReadSeq : public IReadSeq, public CInterface
{
protected:
    IReadSeq *stream;
    IReadSeqAllocator *allocator;
    unsigned num;
    size32_t recsize;
public:
    IMPLEMENT_IINTERFACE;
    CChainedReadSeq(IReadSeqAllocator *_allocator)
    {
        allocator = _allocator;
        allocator->Link();
        stream = allocator->next();
        num = 0;
        recsize = 0;
    }
    virtual ~CChainedReadSeq()
    {
        ::Release(stream);
        allocator->Release();
    }
    virtual bool get(void *dst) { return (getn(dst,1)==1); }
    virtual unsigned getn(void *dst, unsigned n) 
    { 
        unsigned done=0;
        while (stream&&n) {
            unsigned r = stream->getn(dst,n); 
            if (r==0) {
                IReadSeq *oldstream=stream;
                stream = allocator->next();
                oldstream->Release();
            }
            else {
                n-=r;
                done+=r;
            }
        }
        return done;
    }
    virtual size32_t getRecordSize()  { return stream->getRecordSize(); }
    virtual void reset()            { stream->reset(); }
    virtual void stop()             { stream->stop(); }

};



unsigned copySeq(IReadSeq *from,IWriteSeq *to,size32_t bufsize)
{
    size32_t recsize=from->getRecordSize();
    assertex(recsize==to->getRecordSize());
    unsigned nbuf = bufsize/recsize;
    if (nbuf==0)
        nbuf = 1;
    MemoryAttr ma;
    byte *buf=(byte *)ma.allocate(nbuf*recsize);
    unsigned ret = 0;
    for (;;) {
        unsigned n = from->getn(buf,nbuf);
        if (n==0)
            break;
        to->putn(buf,n);
        ret += n;
    }
    return ret;
}

/////////////////

CBufferedIOStreamBase::CBufferedIOStreamBase(size32_t _bufferSize)
{
    bufferSize = _bufferSize;
    numInBuffer = 0;
    curBufferOffset = 0;
    reading = true;
    minDirectSize = std::min(bufferSize/4,(size32_t)0x2000);        // size where we don't bother copying into the buffer
}

size32_t CBufferedIOStreamBase::doread(size32_t len, void * data)
{
    if (!reading)
    {
        doflush();
        reading = true;
    }

    size32_t sizeGot = readFromBuffer(len, data);
    len -= sizeGot;
    if (len!=0) 
    {
        data = (char *)data + sizeGot;
        if (len >= minDirectSize)
            sizeGot += directRead(len, data);   // if direct read specified don't loop
        else 
        {
            do  
            {
                if (!fillBuffer())
                    break;
                size32_t numRead = readFromBuffer(len, data);
                sizeGot += numRead;
                len -= numRead; 
                data = (char *)data + numRead;
            } while (len);
        }
    }
    return sizeGot;
}



size32_t CBufferedIOStreamBase::dowrite(size32_t len, const void * data)
{
    if (reading)
    {
        curBufferOffset = 0;
        numInBuffer = 0;
        reading = false;
    }

    size32_t ret = len;
    while (len) {                       // tries to write in buffer size chunks, also flushes as soon as possible
        size32_t wr;
        if (numInBuffer != 0) {
            wr = std::min(len,bufferSize-curBufferOffset);
            writeToBuffer(wr, data);
            if (numInBuffer==bufferSize)
                doflush();
            len -= wr;
            if (len==0)
                break;
            data = (char *)data + wr;
        }
        if (len >= minDirectSize) 
            return directWrite(len, data)+ret-len;
        wr = std::min(len,bufferSize);
        writeToBuffer(wr, data);
        if (numInBuffer==bufferSize)
            doflush();
        len -= wr;
        data = (char *)data + wr;
    }
    return ret; // there is a bit of an assumption here that flush always works 
}

////////////////////////////

class CBufferedIIOStream : public CBufferedIOStreamBase, implements IIOStream
{
    Linked<IIOStream> io;
public:
    IMPLEMENT_IINTERFACE;

    CBufferedIIOStream(IIOStream *_io, unsigned bufSize) : CBufferedIOStreamBase(bufSize), io(_io)
    {
        buffer = new byte[bufSize];
    }

    ~CBufferedIIOStream()
    {
        delete [] buffer;
    }

    virtual void beforeDispose()
    {
        try
        {
            // NOTE - flush may throw an exception and thus cannot be done in the destructor.
            flush();
        }
        catch (IException *E)
        {
            EXCLOG(E, "ERROR - Exception in CBufferedIIOStream::flush ignored");
            E->Release();
            assert(!"ERROR - Exception in CBufferedIIOStream::flush ignored");
        }
        catch (...)
        {
            DBGLOG("ERROR - Unknown exception in CBufferedIIOStream::flush ignored");
            assert(!"ERROR - Unknown exception in CBufferedIIOStream::flush ignored");
        }
    }

    virtual bool fillBuffer()
    {
        reading = true;
        numInBuffer = io->read(bufferSize, buffer);
        curBufferOffset = 0;
        return numInBuffer!=0;
    }
    virtual size32_t directRead(size32_t len, void * data) 
    { 
        return io->read(len, data); 
    }
    virtual size32_t directWrite(size32_t len, const void * data) 
    { 
        return io->write(len,data); 
    }
    virtual void doflush()
    {
        if (!reading && numInBuffer)
        {
            //Copy numInBuffer out before flush so that destructor doesn't attempt to flush again.
            size32_t numToWrite = numInBuffer;
            numInBuffer = 0;
            io->write(numToWrite, buffer);
            curBufferOffset = 0;
        }
    }

// IIOStream impl.
    virtual size32_t read(size32_t len, void * data) { return CBufferedIOStreamBase::doread(len, data); }
    virtual size32_t write(size32_t len, const void * data) { return CBufferedIOStreamBase::dowrite(len, data); }
    virtual void flush() { doflush(); }
};

IIOStream *createBufferedIOStream(IIOStream *io, unsigned bufSize)
{
    if (bufSize == (unsigned)-1)
        bufSize = DEFAULT_BUFFER_SIZE;
    return new CBufferedIIOStream(io, bufSize);
}


IRowStream *createNullRowStream()
{
    class cNullStream: implements IRowStream, public CInterface
    {
        const void *nextRow() { return NULL; }
        void stop() {}
    public:
        IMPLEMENT_IINTERFACE;
        cNullStream() {}
    };
    return new cNullStream;
}



unsigned copyRowStream(IRowStream *in, IRowWriter *out)
{
    unsigned ret=0;
    for (;;) {
        const void *row = in->nextRow();
        if (!row)
            break;
        ret ++;
        out->putRow(row);
    }
    return ret;
}
unsigned groupedCopyRowStream(IRowStream *in, IRowWriter *out)
{
    unsigned ret=0;
    for (;;) {
        const void *row = in->nextRow();
        if (!row) {
            row = in->nextRow();
            if (!row)
                break;
            out->putRow(NULL);
        }
        ret ++;
        out->putRow(row);
    }
    return ret;
}
unsigned ungroupedCopyRowStream(IRowStream *in, IRowWriter *out)
{
    unsigned ret=0;
    for (;;) {
        const void *row = in->ungroupedNextRow();
        if (!row)
            break;
        ret ++;
        out->putRow(row);
    }
    return ret;
}

class CConcatRowStream : public IRowStream, public CInterface
{
    IArrayOf<IRowStream> oinstreams;
public:
    unsigned num;
    unsigned idx;
    IRowStream **in;
    bool grouped;
    bool needeog;
public:
    IMPLEMENT_IINTERFACE;
    CConcatRowStream(unsigned _num, IRowStream **_in,bool _grouped)
    {
        // in streams assumed valid throughout (i.e. not linked)
        num = _num;
        idx = 0;
        assertex(num);
        oinstreams.ensure(num);
        for (unsigned n = 0;n<num;n++)
            oinstreams.append(*LINK(_in[n]));
        in = oinstreams.getArray();
        grouped = _grouped;
        needeog = false;
    }

    const void *nextRow()
    {
        while (idx<num) {
            const void *row;
            if (grouped) {
                row = in[idx]->nextRow();
                if (row) {
                    needeog = true;
                    return row;
                }
                if (needeog) {
                    needeog = false;
                    return NULL;
                }
            }
            else {
                row = in[idx]->ungroupedNextRow();
                if (row)
                    return row;
            }
            idx++;
        }
        return NULL;
    }
            
    virtual void stop()  
    { 
        while (idx<num)
            in[idx++]->stop(); 
    }

            
};

extern jlib_decl IWriteSeq *createChainedWriteSeq(IWriteSeqAllocator *iwsa)
{
    return new CChainedWriteSeq(iwsa);
}

extern jlib_decl IReadSeq *createChainedReadSeq(IReadSeqAllocator *irsa)
{
    return new CChainedReadSeq(irsa);
}



IRowStream *createConcatRowStream(unsigned numstreams,IRowStream** streams,bool grouped)
{
    switch(numstreams)
    {
        case 0:
            return createNullRowStream();
        case 1:
            return LINK(streams[0]);
        default:
            return new CConcatRowStream(numstreams,streams,grouped);
    }
}


#ifdef  __x86_64__
void writeStringToStream(IIOStream &out, const char *s) { out.write((size32_t)strlen(s), s); }
void writeCharsNToStream(IIOStream &out, char c, unsigned cnt) { while(cnt--) out.write(1, &c); }
void writeCharToStream(IIOStream &out, char c) { out.write(1, &c); }
#endif

template<typename T> size32_t readSimpleStream(T &target, ISimpleReadStream &stream, size32_t readChunkSize)
{
    size32_t totalSz = 0;
    while (true)
    {
        size32_t sizeRead = stream.read(readChunkSize, target.reserve(readChunkSize));
        if (sizeRead < readChunkSize)
        {
            target.setLength(target.length() - (readChunkSize - sizeRead));
            if (!sizeRead)
                break;
        }
        totalSz += sizeRead;
    }
    return totalSz;
}
template size32_t readSimpleStream<StringBuffer>(StringBuffer &, ISimpleReadStream &, size32_t);
template size32_t readSimpleStream<MemoryBuffer>(MemoryBuffer &, ISimpleReadStream &, size32_t);

