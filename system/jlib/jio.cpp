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
#include "jlzw.hpp"
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

extern jlib_decl size32_t checked_read(const char * filename, int file, void *buffer, size32_t len)
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
                throw makeErrnoExceptionV(errno, "checked_read for file '%s'", filename);
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

extern jlib_decl size32_t checked_pread(const char * filename, int file, void *buffer, size32_t len, offset_t pos)
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
            throw makeOsExceptionV(GetLastError(), "checked_pread for file '%s'", filename);
    }
    {
        CriticalBlock blk(atomicsection);
        checked_lseeki64(file, pos, FILE_BEGIN);
        return checked_read(filename, file, buffer, len);
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
                throw makeErrnoExceptionV(errno, "checked_pread for file '%s'", filename);
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

extern jlib_decl size32_t checked_write(const char * filename, int handle, const void *buffer, size32_t count )
{
    int ret=_write(handle,buffer,count);
    if ((size32_t)ret != count)
    {
        if (-1 != ret)
            throw makeOsExceptionV(DISK_FULL_EXCEPTION_CODE, "checked_write for file '%s'", filename);
        else
            throw makeErrnoExceptionV(errno, "checked_write for file '%s'", filename);
    }
    return (size32_t)ret;
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


/////////////////

CBufferedIOStreamBase::CBufferedIOStreamBase(size32_t _bufferSize)
{
    bufferSize = _bufferSize;
    numInBuffer = 0;
    curBufferOffset = 0;
    reading = true;
    minDirectSize = bufferSize/4;        // size where we don't bother copying into the buffer
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
        oinstreams.ensureCapacity(num);
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

class CStreamLineReader : public CSimpleInterfaceOf<IStreamLineReader>
{
    Linked<ISimpleReadStream> stream;
    bool preserveEols;
    size32_t chunkSize = 8192;
    MemoryBuffer buffer;
    const char *startPtr = nullptr;
    const char *endPtr = nullptr;
    const char *currentPtr = nullptr;

    bool refill()
    {
        size32_t r = stream->read(chunkSize, buffer.bufferBase());
        startPtr = (const char *)buffer.bufferBase();
        endPtr = startPtr+r;
        currentPtr = startPtr;
        return r>0;
    }
public:
    CStreamLineReader(ISimpleReadStream *_stream, bool _preserveEols, size32_t _chunkSize) : stream(_stream), preserveEols(_preserveEols), chunkSize(_chunkSize)
    {
        buffer.reserveTruncate(chunkSize);
        startPtr = (const char *)buffer.bufferBase();
        endPtr = currentPtr = startPtr;
    }
// IStreamLineReader impl.
    virtual bool readLine(StringBuffer &out) override // returns true if end-of-stream
    {
        if (currentPtr == endPtr)
        {
            if (!refill())
                return true; // eos
        }

        startPtr = currentPtr; // mark start of line in current buffer
        while (true)
        {
            switch (*currentPtr)
            {
                case '\n':
                {
                    ++currentPtr;
                    out.append(currentPtr-startPtr-(preserveEols?0:1), startPtr);
                    return false;
                }
                case '\r':
                {
                    ++currentPtr;
                    out.append(currentPtr-startPtr-(preserveEols?0:1), startPtr);

                    // Check for '\n'
                    if (currentPtr == endPtr)
                        refill();
                    if (currentPtr < endPtr && '\n' == *currentPtr)
                    {
                        if (preserveEols)
                            out.append('\n');
                        ++currentPtr;
                    }
                    return false;
                }
                default:
                {
                    ++currentPtr;
                    if (currentPtr == endPtr)
                    {
                        out.append(currentPtr-startPtr, startPtr); // output what we have so far
                        if (!refill())
                            return false;
                    }
                    break;
                }
            }
        }
    }
};

IStreamLineReader *createLineReader(ISimpleReadStream *stream, bool preserveEols, size32_t chunkSize)
{
    return new CStreamLineReader(stream, preserveEols, chunkSize);
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

