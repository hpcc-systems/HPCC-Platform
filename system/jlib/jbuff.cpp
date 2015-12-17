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
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <math.h>

#ifndef WIN32
#include <sys/mman.h>
#define LARGEMEM_USE_MMAP_SIZE 0x10000        // in largemem use mmap for chunks bigger than 64K
#endif


#include "jbuff.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jvmem.hpp"

#ifdef _DEBUG
#define KILL_CLEARS_MEMORY  
//#define TRACE_LARGEMEM  
//#define TRACE_LARGEMEM_ALLOC
#define TRACE_LARGEMEM_OOM
#endif



#if 1
#define ChunkSize         0x10000
#define DOUBLE_LIMIT      0x7fffffff    // avoid doubling hitting 0 and infinite loop
#else
#define ChunkSize         2048
#define DOUBLE_LIMIT      4096
#endif
#define FIRST_CHUNK_SIZE  8
#define DETACH_GRANULARITY 16


#ifdef _DEBUG
#define     CHECKREADPOS(len)  assertex(readPos+(len)<=length())
#else
#define     CHECKREADPOS(len)  
#endif


//-----------------------------------------------------------------------

jlib_decl void *checked_realloc(void *orig, size_t newlen, size_t origlen,int errcode)
{
    if (newlen==0) {
        free(orig);
        return NULL;
    }
    if (orig==NULL)
        return checked_malloc(newlen,errcode);
    void *ret = realloc(orig, newlen);
    if (!ret)
        RaiseOutOfMemException(errcode, newlen, origlen);
    return ret;
}

class jlib_thrown_decl COutOfMemException: public CInterface, implements IOutOfMemException
{
    int errcode;
    size_t wanted;
    size_t got;
    static int recursion;
    bool expected;
public:
    IMPLEMENT_IINTERFACE;
    COutOfMemException(int _errcode,size_t _wanted,size_t _got,bool _expected)
    {
        errcode = _errcode;
        wanted = _wanted;
        expected = _expected;
        got = _got;
//      DebugBreak();

        if ((recursion++==0)&&!expected) {
// Bit risky if *very* out of memory so protect against recursion and catch exceptions
            try { 
                // try to log
                PROGLOG("Jbuff: Out of Memory (%d,%" I64F "d,%" I64F "dk)",_errcode,(unsigned __int64)wanted,(unsigned __int64) (got/1024));
                PrintStackReport();
                PrintMemoryReport();
            }
            catch (...) {
            }
        }
        recursion--;
    };
    
    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        str.append("Jbuff: Out of Memory (").append((unsigned __int64)wanted);
        if (got) 
            str.append(',').append((unsigned __int64)got);
        return str.append(")");
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};

int COutOfMemException::recursion=0;

IOutOfMemException *createOutOfMemException(int errcode,size_t wanted,size_t got,bool expected)
{
    return new COutOfMemException(errcode,wanted,got,expected);
}

void RaiseOutOfMemException(int errcode, size_t wanted, size_t got,bool expected)
{
    throw createOutOfMemException(errcode, wanted, got,expected);
}

MemoryAttr::MemoryAttr(size_t _len)
{
    ptr = checked_malloc(_len,-1); 
    len = _len;
}

MemoryAttr::MemoryAttr(size_t _len, const void * _ptr)
{
    len = 0;
    ptr = NULL;
    set(_len, _ptr);
}

MemoryAttr::MemoryAttr(const MemoryAttr & src)
{
    len = 0;
    ptr = NULL;
    set(src.length(), src.get());
}



void MemoryAttr::set(size_t _len, const void * _ptr)
{
    memcpy(allocate(_len), _ptr, _len);
}


void MemoryAttr::setOwn(size_t _len, void * _ptr)
{
    free(ptr);
    len = _len;
    ptr = _ptr;
}


void MemoryAttr::clear()
{
    free(ptr);
    ptr = NULL; 
    len = 0; 
}

void * MemoryAttr::detach()
{
    void * ret=ptr;
    ptr = NULL;
    len = 0;
    return ret;
}

int MemoryAttr::compare(const MemoryAttr & m1, const MemoryAttr & m2)
{
    size_t len1 = m1.length();
    size_t len2 = m2.length();
    size_t len = len1;
    
    if (len1 > len2)
        len = len2;
    int compare = memcmp(m1.get(), m2.get(), len);
    if (compare == 0)
        compare = (len1 > len2) ? +1 : (len1 < len2) ? -1 : 0;
    return compare;
}

void *  MemoryAttr::allocate(size_t _len)
{ 
    if (_len==len)
        return ptr;
    clear();
    ptr = checked_malloc(_len,-2); 
    len = _len;
    return ptr; 
}

void * MemoryAttr::ensure(size_t _len)
{
    if (_len <=len)
        return ptr;
    return reallocate(_len);
}

void *  MemoryAttr::reallocate(size_t _len)
{ 
    if (_len==len)
        return ptr;
    ptr = checked_realloc(ptr, _len, len, -9);
    len = _len;
    return ptr; 
}

//===========================================================================



void MemoryBuffer::_realloc(size32_t newLen)
{
    if (newLen > maxLen)
    {
        assertex(ownBuffer);
        size32_t newMax = maxLen;
        //double up to a certain size, otherwise go up in chunks.
        if (newLen < DOUBLE_LIMIT)
        {
            if (newMax == 0)
                newMax = FIRST_CHUNK_SIZE;
            while (newLen > newMax)
            {
                newMax += newMax;
            }
        }
        else
            /*** ((Size + 1) + (ChunkSize - 1)) & ~(ChunkSize-1) ***/
            newMax = (newLen + ChunkSize) & ~(ChunkSize-1);
        
        buffer =(char *)checked_realloc(buffer, newMax, maxLen, -7);
        maxLen = newMax;
    }
}

void MemoryBuffer::_reallocExact(size32_t newLen)
{
    if (newLen > maxLen)
    {
        assertex(ownBuffer);
        buffer =(char *)checked_realloc(buffer, newLen, maxLen, -8);
        maxLen = newLen;
    }
}


void MemoryBuffer::init()
{
    buffer = NULL;
    curLen = 0;
    maxLen = 0;
    ownBuffer = true;
    readPos = 0;
    swapEndian = false;
}

void *MemoryBuffer::insertDirect(unsigned offset, size32_t insertLen)
{
    assertex(offset<=curLen);
    unsigned newLen = insertLen + curLen;
    _realloc(newLen);
    memmove(buffer + offset + insertLen, buffer + offset, curLen - offset);
    curLen += insertLen;
    return buffer+offset;
}


void * MemoryBuffer::ensureCapacity(unsigned max)
{
    if (maxLen - curLen < max)
        _realloc(curLen + max);
    return buffer + curLen;
}

void MemoryBuffer::kill()
{
    if (ownBuffer)
        free(buffer);
}


MemoryBuffer & MemoryBuffer::_remove(unsigned start, unsigned len)
{
    if (start > curLen) start = curLen;
    if (start + len > curLen) len = curLen - start;
    unsigned start2 = start + len;
    memmove(buffer + start, buffer + start2, curLen - start2);
    setLength(curLen - len);
    return *this;
}

void * MemoryBuffer::reserve(unsigned size)
{
    _realloc(curLen + size);
    void * ret = buffer + curLen;
    curLen += size;
    return ret;
}

void * MemoryBuffer::reserveTruncate(unsigned size)
{
    curLen += size;
    _reallocExact(curLen);
    truncate();
    return buffer + curLen - size;
}

void MemoryBuffer::truncate()
{
    if (maxLen>curLen) {
        if (curLen==0) {
            free(buffer);
            buffer = NULL;
        }
        else
            buffer = (char *)realloc(buffer, curLen);
        maxLen = curLen;
    }
}



void MemoryBuffer::resetBuffer()
{
    kill();
    init();
}

MemoryBuffer & MemoryBuffer::_reverse()
{
    unsigned max = curLen/2;
    char * end = buffer + curLen;
    
    unsigned idx;
    for (idx = 0; idx < max; idx++)
    {
        char temp = buffer[idx];
        end--;
        buffer[idx] = *end;
        *end = temp;
    }
    
    return *this;
}

void MemoryBuffer::setBuffer(size_t len, void * _buffer, bool takeOwnership)
{
    assertex((size32_t)len == len);
    kill();
    buffer = (char *) _buffer;
    if (len) assertex(buffer);
    curLen = maxLen = (size32_t)len;
    ownBuffer = takeOwnership;
    readPos = 0;
}

void *MemoryBuffer::detach()
{
    void *ret;
    if (ownBuffer) { 
        if (maxLen>curLen+DETACH_GRANULARITY)
            buffer = (char *)realloc(buffer,curLen);
        ret = buffer;
    }
    else {
        ret = memcpy(checked_malloc(curLen,-3), buffer, curLen);
    }
    init();
    return ret;
}

void *MemoryBuffer::detachOwn()
{
    assertex(ownBuffer);
    void *ret = buffer;
    init();
    return ret;
}

void MemoryBuffer::setLength(unsigned len)
{
    if (len > curLen)
    {
        _realloc(len);
        memset(buffer + curLen, 0, len-curLen);
    }
    else
    {
#ifdef KILL_CLEARS_MEMORY
        if (curLen)
            memset(buffer + len, 'x', curLen-len);
#endif
    }
    curLen = len;
}

void MemoryBuffer::setWritePos(unsigned len)
{
    if (len > curLen)
        _realloc(len);
    curLen = len;
}

#define SWAP(x, y, t)  { t t_##x = x; x = y; y = t_##x; }
void MemoryBuffer::swapWith(MemoryBuffer & other)
{
    //swap two string buffers.  Used for efficiently moving a string on in a pipeline etc.
    SWAP(buffer, other.buffer, char *);
    SWAP(curLen, other.curLen, size32_t);
    SWAP(maxLen, other.maxLen, size32_t);
    SWAP(readPos, other.readPos, size32_t);
    SWAP(swapEndian, other.swapEndian, bool);
}

//-----------------------------------------------------------------------


MemoryBuffer::MemoryBuffer(size_t initial)
{
    assertex((size32_t)initial == initial);
    init();
    _realloc((size32_t)initial);
}

MemoryBuffer::MemoryBuffer(MemoryBuffer & value __attribute__((unused)))
{
    assertex(!"This should never be used");
}

MemoryBuffer::MemoryBuffer(size_t len, const void * newBuffer)
{
    init();
    assertex((size32_t)len == len);
    append((size32_t)len, newBuffer);
}

MemoryBuffer & MemoryBuffer::append(char value)
{
    _realloc(curLen + 1);
    buffer[curLen] = value;
    ++curLen;
    return *this;
}


MemoryBuffer & MemoryBuffer::append(unsigned char value)
{
    _realloc(curLen + 1);
    buffer[curLen] = value;
    ++curLen;
    return *this;
}

MemoryBuffer & MemoryBuffer::append(bool value)
{
    _realloc(curLen + 1);
    buffer[curLen] = (value==0)?0:1;
    ++curLen;
    return *this;
}

MemoryBuffer & MemoryBuffer::append(const char * value)
{
    if (value)
        return append((size32_t)strlen(value)+1,value);
    else
        return append((char)0);
}

MemoryBuffer & MemoryBuffer::append(const unsigned char * value)
{
    return append((const char *) value);
}

MemoryBuffer & MemoryBuffer::append(unsigned len, const void * value)
{
    _realloc(curLen + len);
    memcpy(buffer + curLen, value, len);
    curLen += len;
    return *this;
}

MemoryBuffer & MemoryBuffer::append(double value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(float value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(short value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(unsigned short value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(int value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(unsigned value)
{
    return appendEndian(sizeof(value), &value);
}

#if 0
MemoryBuffer & MemoryBuffer::append(long value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(unsigned long value)
{
    return appendEndian(sizeof(value), &value);
}
#endif

MemoryBuffer & MemoryBuffer::append(__int64 value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(unsigned __int64 value)
{
    return appendEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::append(const MemoryBuffer & value)
{
    size32_t SourceLen = value.length();
    
    _realloc(curLen + SourceLen);
    memcpy(buffer + curLen, value.toByteArray(), SourceLen);
    curLen += SourceLen;
    return *this;
}

MemoryBuffer & MemoryBuffer::appendBytes(unsigned char value, unsigned count)
{
    _realloc(curLen + count);
    memset(buffer+curLen, value, count);
    curLen+=count;
    return *this;
}

MemoryBuffer & MemoryBuffer::appendEndian(size32_t len, const void * value)
{
    _realloc(curLen + len);
    
    if (swapEndian)
        _cpyrevn(buffer + curLen, value, len);
    else
        memcpy(buffer + curLen, value, len);
    
    curLen += len;
    return *this;
}

MemoryBuffer & MemoryBuffer::appendSwap(size32_t len, const void * value)
{
    _realloc(curLen + len);
    _cpyrevn(buffer + curLen, value, len);
    curLen += len;
    return *this;
}

MemoryBuffer &MemoryBuffer::appendFile(const char *fileName)
{
    char buf[1024];
    int h = _open(fileName, _O_BINARY | _O_RDONLY | _O_SEQUENTIAL);
    
    if (h == HFILE_ERROR)
        throw MakeStringException(0, "MemoryBuffer: Error reading file : %s", fileName);
    
    append(fileName);
    
    unsigned fileSize = _lseek(h, 0, FILE_END);
    _lseek(h, 0, FILE_BEGIN);
    append(fileSize);
    
    int r;
    while ((r = _read(h, buf, 1024)) != 0)
    {
        if (-1==r) throw makeErrnoException("MemoryBuffer::appendFile");
        append(r, buf); 
    }
    _close(h);
    return *this;
}

MemoryBuffer & MemoryBuffer::read(char & value)
{
    CHECKREADPOS(sizeof(value));
    value = buffer[readPos++];
    return *this;
}

MemoryBuffer & MemoryBuffer::read(unsigned char & value)
{
    CHECKREADPOS(sizeof(value));
    value = buffer[readPos++];
    return *this;
}

MemoryBuffer & MemoryBuffer::read(bool & value)
{
    CHECKREADPOS(sizeof(value));
    char _value = buffer[readPos++];
    value = (_value==0 ? false : true);
    return *this;
}

MemoryBuffer & MemoryBuffer::read(StringAttr & value)
{
    char * src = buffer + readPos;
    size32_t len = (size32_t)strlen(src);
    CHECKREADPOS(len+1);
    value.set(src, len);
    readPos += (len+1);
    return *this;
}

MemoryBuffer & MemoryBuffer::read(StringBuffer & value)
{
    char * src = buffer + readPos;
    size32_t len = (size32_t)strlen(src);
    CHECKREADPOS(len+1);
    value.append(len, src);
    readPos += (len+1);
    return *this;
}

MemoryBuffer & MemoryBuffer::read(const char * &value)
{
    value = buffer+readPos;
    size32_t len = (size32_t)strlen(value);
    CHECKREADPOS(len+1);
    readPos += (len+1);
    return *this;
}

MemoryBuffer & MemoryBuffer::read(size32_t len, void * value)
{
    CHECKREADPOS(len);
    memcpy(value, buffer + readPos, len);
    readPos += len;
    return *this;
}

MemoryBuffer & MemoryBuffer::read(double & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(float & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(short & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(unsigned short & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(int & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(unsigned & value)
{
    return readEndian(sizeof(value), &value);
}

#if 0
MemoryBuffer & MemoryBuffer::read(unsigned long & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(long & value)
{
    return readEndian(sizeof(value), &value);
}
#endif

MemoryBuffer & MemoryBuffer::read(unsigned __int64 & value)
{
    return readEndian(sizeof(value), &value);
}

MemoryBuffer & MemoryBuffer::read(__int64 & value)
{
    return readEndian(sizeof(value), &value);
}

const byte * MemoryBuffer::readDirect(size32_t len)
{
    CHECKREADPOS(len);
    const byte * ret = (const byte *)buffer + readPos;
    readPos += len;
    return ret;
}

MemoryBuffer & MemoryBuffer::skip(unsigned len)
{
    CHECKREADPOS(len);
    readPos += len;
    return *this;
}

void MemoryBuffer::writeDirect(size32_t pos,size32_t len,const void *buf)
{
    assertex(pos+len<=curLen); // does not extend
    memcpy(buffer+pos,buf,len);
}

void MemoryBuffer::writeEndianDirect(size32_t pos,size32_t len,const void *buf)
{
    assertex(pos+len<=curLen); // does not extend
    if (swapEndian)
        _cpyrevn(buffer+pos,buf,len);
    else
        memcpy(buffer+pos,buf,len);
}


MemoryBuffer & MemoryBuffer::readEndian(size32_t len, void * value)
{
    CHECKREADPOS(len);
    if (swapEndian)
        _cpyrevn(value, buffer + readPos, len);
    else
        memcpy(value, buffer + readPos, len);
    
    readPos += len;
    return *this;
}

MemoryBuffer & MemoryBuffer::readSwap(size32_t len, void * value)
{
    CHECKREADPOS(len);
    _cpyrevn(value, buffer + readPos, len);
    readPos += len;
    return *this;
}

MemoryBuffer &MemoryBuffer::readFile(StringAttr &fileName)
{
    read(fileName);
    unsigned fileSize;
    read(fileSize);
    
    int h = _open(fileName.get(), _O_WRONLY|_O_CREAT|_O_TRUNC|_O_BINARY|_O_SEQUENTIAL, _S_IREAD | _S_IWRITE);
    if (h == HFILE_ERROR)
        throw MakeStringException(0, "MemoryBuffer: Unable to create file : %s, error=%d", fileName.get(), GetLastError());
    
    CHECKREADPOS(fileSize);
    int w;
    while (fileSize) {
        w = _write(h, buffer+readPos, fileSize);
        if (w == 0) {
            _close(h);
            throw MakeStringException(0, "MemoryBuffer: Disk full writing %d to file : %s", fileSize, fileName.get());
        }
        if (w == -1)
        {
            _close(h);
            throw MakeStringException(0, "MemoryBuffer: Error writing to file : %s, error=%d", fileName.get(), GetLastError());
        }
        readPos += (size32_t)w;
        fileSize -= (size32_t)w;
    }
    _close(h);
    return *this;
}



MemoryBuffer & MemoryBuffer::rewrite(size32_t pos)
{
    assertex(pos<=maxLen);
    curLen = pos;
    if (readPos>pos) 
        readPos = pos;
    return *this;
}

MemoryBuffer & MemoryBuffer::reset(size32_t pos)
{
    CHECKREADPOS(pos-readPos);
    readPos = pos;
    return *this;
}

#if 0
void MemoryBuffer::getBytes(int srcBegin, int srcEnd, char * target)
{
    memcpy(target, buffer + srcBegin, srcEnd - srcBegin);
}

MemoryBuffer & MemoryBuffer::remove(unsigned start, unsigned len)
{
    return (MemoryBuffer &)_remove(start, len);
}
#endif


int MemoryBuffer::setEndian(int endian)
{
    assertex((endian == __LITTLE_ENDIAN) || (endian == __BIG_ENDIAN));
    bool wasSwapped = setSwapEndian(endian != __BYTE_ORDER);
    return wasSwapped ? (__BYTE_ORDER ^ __LITTLE_ENDIAN ^ __BIG_ENDIAN) : __BYTE_ORDER;
}

bool MemoryBuffer::setSwapEndian(bool swap)
{
    bool saved = swapEndian;
    swapEndian = swap;
    return saved;
}



MemoryBuffer & serialize(MemoryBuffer & buffer, const MemoryAttr & value)
{
    size32_t length = (size32_t)value.length();
    buffer.append(length).append(length, value.get());
    return buffer;
}


MemoryBuffer & deserialize(MemoryBuffer & buffer, MemoryAttr & value)
{
    unsigned length;
    buffer.read(length);
    void * target = value.allocate(length);
    buffer.read(length, target);
    return buffer;
}

MemoryBuffer & serialize(MemoryBuffer & buffer, const char * value)
{
    if (value)
    {
        unsigned length = (size32_t)strlen(value);
        buffer.append(length).append(length, value);
    }
    else
        buffer.append((unsigned)-1);
    return buffer;
}


MemoryBuffer & deserialize(MemoryBuffer & buffer, StringAttr & value)
{
    unsigned length;
    buffer.read(length);
    if (length == (unsigned)-1)
        value.clear();
    else
    {
        char * target = (char *)checked_malloc(length+1,-4);
        buffer.read(length, target);
        target[length] = 0;
        value.setown(target);
    }
    return buffer;
}


// =====================================================================================================

const char * MemoryAttr2IStringVal::str() const 
{ 
    UNIMPLEMENTED;  
}

// =====================================================================================================



static memsize_t LMsemlimit=0;
static memsize_t LMtotal=0;
static CriticalSection LMsemsect;
static Owned<ILargeMemLimitNotify> LMnotify;
static bool LMlocked = false;


void setLargeMemLimitNotify(memsize_t size,ILargeMemLimitNotify *notify)
{
    CriticalBlock block(LMsemsect);
    LMsemlimit = size;
    LMnotify.set(notify);
    if (LMlocked&&(LMtotal<LMsemlimit)) 
        LMlocked = false;
}

inline void incLargeMemTotal(memsize_t sz)
{
    if (sz) {
        CriticalBlock block(LMsemsect);
        LMtotal += sz;
#ifdef TRACE_LARGEMEM
        if ((LMtotal/0x100000)!=((LMtotal-sz)/0x100000))
            PROGLOG("LARGEMEM(+): %" I64F "d",(offset_t)LMtotal);
#endif
        if (!LMlocked&&LMnotify.get()&&(LMtotal>=LMsemlimit)) {
            LMlocked = true;
            DBGLOG("LargeMemTotal limit exceeded: %" I64F "d",(offset_t)LMtotal);
            if (!LMnotify->take(LMtotal)) {
                LMtotal -= sz;
                LMlocked = false;
                throw createOutOfMemException(-9,sz, LMtotal);
            }
            DBGLOG("LargeMem taken");
        }
    }
}

inline void decLargeMemTotal(memsize_t sz)
{
    if (sz) {
        CriticalBlock block(LMsemsect);
        LMtotal -= sz;
#ifdef TRACE_LARGEMEM
        if ((LMtotal/0x100000)!=((LMtotal+sz)/0x100000))
            PROGLOG("LARGEMEM(-): %" I64F "d",(offset_t)LMtotal);
#endif
        if (LMlocked) {
            if (LMtotal<LMsemlimit) {
                DBGLOG("LargeMemTotal limit reduced to %" I64F "d",(offset_t)LMtotal);
                LMlocked = false;
                if (LMnotify.get())
                    LMnotify->give(LMtotal);
            }
        }
    }
}


void CLargeMemoryAllocator::allocchunkmem()
{
#ifdef LARGEMEM_USE_MMAP_SIZE
    size32_t masize = VMPAGEROUND(chunk.max);
    if (masize>=LARGEMEM_USE_MMAP_SIZE) { // use mmap
        chunk.base = (byte *) mmap(NULL,masize,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_NORESERVE|MAP_ANONYMOUS,-1,0);
        if (chunk.base == (byte *)MAP_FAILED)
            chunk.base = NULL;
#ifdef TRACE_LARGEMEM_ALLOC
        PROGLOG("CLargeMemoryAllocator::allocchunkmem mmaped %d at %p",masize,chunk.base);
#endif
        return;
    }
#endif
    chunk.base = (byte *)malloc(chunk.max); 
#ifdef TRACE_LARGEMEM_ALLOC
    PROGLOG("CLargeMemoryAllocator::allocchunkmem malloced %d at %p",chunk.max,chunk.base);
#endif
}

void CLargeMemoryAllocator::disposechunkmem() 
{ 
#ifdef LARGEMEM_USE_MMAP_SIZE
    size32_t masize = VMPAGEROUND(chunk.max);
    if (masize>=LARGEMEM_USE_MMAP_SIZE) { // use mmap
        munmap(chunk.base,masize);
        return;
    }
#endif
    free(chunk.base); 
}


bool CLargeMemoryAllocator::newchunk(size32_t sz,size32_t extra,bool exceptionwanted)
{
    size32_t newchunksz = (sz>chunkmin)?sz:chunkmin;

    if (maxallocated()+newchunksz+extra>totalmax) {
#ifdef TRACE_LARGEMEM_OOM
        PrintStackReport();
        PROGLOG("OOM.1 wanted sz=%d, extra = %d, maxallocated=%" I64F "d, newchunksz=%u, totalmax=%" I64F "d",sz,extra,(offset_t)maxallocated(),newchunksz,(offset_t)totalmax);
#endif
        if (exceptionwanted) {
            throw createOutOfMemException(-5,sz, maxallocated(),true);
        }
        return false;
    }
    if (chunk.size) {
        Chunk *p = new Chunk;
        *p = chunk;
        chunk.prev = p;
        atot += chunk.size;
    }
    else if (chunk.max) {
        decLargeMemTotal(chunk.max);
        amax -= chunk.max;
        disposechunkmem();
    }
    chunk.max = newchunksz;
    allocchunkmem();
    chunk.size = 0;
    if (!chunk.base) {
        // restore prev
        if (chunk.prev) {
            Chunk *p = chunk.prev; 
            chunk = *p;
            atot -= chunk.size;
            delete p;
        }
        else
            chunk.max = 0;
#ifdef TRACE_LARGEMEM_OOM
        PrintStackReport();
        PROGLOG("OOM.2 wanted sz=%d, extra = %d, maxallocated=%" I64F "d, newchunksz=%u, totalmax=%" I64F "d",sz,extra,(offset_t)maxallocated(),newchunksz,(offset_t)totalmax);
#endif
        if (throwexception) {
            throw createOutOfMemException(-6,sz, maxallocated(),true);
        }
        return false;
    }
    amax += chunk.max;
    incLargeMemTotal(newchunksz);
    return true;
}

void CLargeMemoryAllocator::reset()
{
    decLargeMemTotal(maxallocated());
    disposechunkmem();
    while (chunk.prev) {
        Chunk *p = chunk.prev;
        chunk = *chunk.prev;
        delete p;
        disposechunkmem();
    }
    chunk.max = 0;
    chunk.base = NULL;
    chunk.size = 0;
    atot = 0;
    amax = 0;
}

void CLargeMemoryAllocator::reduceSize(memsize_t amount)
{
    if (amount<=chunk.size) {
        chunk.size-=amount;
        return;
    }
    memsize_t reduced = 0;
    do {
        amount -= chunk.size;
        reduced += chunk.max;
        disposechunkmem();
        amax -= chunk.max;
        Chunk *p = chunk.prev; 
        chunk = *p;
        atot -= chunk.size;
        delete p;
    } while (amount>chunk.size);
    chunk.size-=amount;
    decLargeMemTotal(reduced);
}


void CLargeMemoryAllocator::setSize(memsize_t pos)
{
    memsize_t sz = allocated();
    assertex(sz>=pos);
    reduceSize(sz-pos);
}

byte *CLargeMemoryAllocator::next(memsize_t pos,size32_t &size) // this should not be used for small jumps as it is slow
{
    memsize_t sz = allocated();
    if (sz<=pos) {
        size = 0;
        return NULL;
    }
    memsize_t dif = sz-pos;     // how much to go back
    Chunk *p = &chunk;
    while (dif>p->size) {
        dif -= p->size;
        p = p->prev; 
    }
    size = (size32_t)dif;       // must be smaller than chunk
    return p->base+p->size-dif;
}


CLargeMemoryAllocator::CLargeMemoryAllocator()
{
    // values overwritten by init
    throwexception = true;
    totalmax = 0;
    chunkmin = 0x1000;
    chunk.prev = NULL;
    chunk.max = 0;
    chunk.base = NULL;
    chunk.size = 0;
    atot = 0;
    amax = 0;
}

void CLargeMemoryAllocator::init(memsize_t _totalmax,size32_t _chunkmin,bool _throwexception)
{
    throwexception = _throwexception;
    totalmax = _totalmax;
    chunkmin = _chunkmin;
    chunk.prev = NULL;
    chunk.max = 0;
    chunk.base = NULL;
    chunk.size = 0;
    atot = 0;
    amax = 0;
}

MemoryBuffer &CLargeMemoryAllocator::serialize(MemoryBuffer &mb)
{
    memsize_t al = allocated();
    size32_t sz = (size32_t)al;
    if (sz!=al)
        throw MakeStringException(-1,"CLargeMemoryAllocator::serialize overflow");
    byte *d = (byte *)mb.reserveTruncate(sz)+sz;
    Chunk *p = &chunk;
    while (sz&&p) {
        size32_t s = p->size;
        d -= s;
        memcpy(d,p->base,s);
        p = p->prev;
        sz -= s;
    }
    return mb;
}

MemoryBuffer &CLargeMemoryAllocator::deserialize(MemoryBuffer &mb,size32_t sz, size32_t extra)
{
    mb.read(sz,alloc(sz,extra));
    return mb;
}

void *CLargeMemoryAllocator::nextBuffer(void *prev,size32_t &sz)
{ // not fast
    Chunk *p = NULL;
    Chunk *n = &chunk;
    while (n&&(n->base!=prev)) {
        p = n;
        n = n->prev;
    }
    if (!p) {
        sz = 0;
        return NULL;
    }
    sz = p->size;
    return p->base;
}

void CJMallocLargeMemoryAllocator::allocchunkmem()
{
    chunk.base = (byte *)allocator->allocMem(chunk.max); 
#ifdef TRACE_LARGEMEM_ALLOC
    PROGLOG("CJMallocLargeMemoryAllocator::allocchunkmem malloced %d at %p",chunk.max,chunk.base);
#endif
}

void CJMallocLargeMemoryAllocator::disposechunkmem() 
{ 
    allocator->freeMem(chunk.base); 
}


CFixedSizeAllocator::CFixedSizeAllocator()
{
    chunklist = NULL;
}

CFixedSizeAllocator::CFixedSizeAllocator(size32_t _allocsize,size32_t _chunksize)
{
    chunklist = NULL;
    init(_allocsize,_chunksize);
}

void CFixedSizeAllocator::init(size32_t _allocsize,size32_t _chunksize)
{
    kill();
    allocsize = _allocsize;
    assertex(allocsize);
    if (allocsize<sizeof(void *))
        allocsize = sizeof(void *);
    chunksize = _chunksize;
    if (chunksize<allocsize*16)
        chunksize = allocsize+sizeof(void *);  // give up on sublety
}

void CFixedSizeAllocator::kill()
{
    while (chunklist) {
        void *p = chunklist;
        chunklist = *(void **)p;
        freeChunk(p);
    }
    freelist = NULL;
    numalloc = 0;
    numfree = 0;
    chunklist = NULL;
}

CFixedSizeAllocator::~CFixedSizeAllocator()
{
    kill();
}

void *CFixedSizeAllocator::allocChunk()
{
    return checked_malloc(chunksize,-5); // don't try to be clever and allocate less (fragmentation)
}

void CFixedSizeAllocator::freeChunk(void *p)
{
    free(p);
}

void *CFixedSizeAllocator::alloc()
{
    NonReentrantSpinBlock block(lock);
    void *ret;
    if (numfree) {
        numfree--;
        ret = freelist;
        freelist = *(void **)freelist;
    }
    else {
        void **newchunk = (void **)allocChunk(); 
        unsigned num = (chunksize-sizeof(void *))/allocsize;
        assertex(num);
        *newchunk = chunklist;
        chunklist = (void *)newchunk;
        newchunk++;
        ret = (void *)newchunk;
        numfree+=num-1;
        while (--num) { // we could do this on the fly but I think this marginally better
            newchunk = (void **)(((byte *)newchunk)+allocsize);
            *newchunk = freelist;
            freelist = (void *)newchunk;
        }
    }
    numalloc++;
    return ret;
}

void CFixedSizeAllocator::dealloc(void *blk)
{
    if (blk) {
        NonReentrantSpinBlock block(lock);
        *(void **)blk = freelist;
        freelist = blk;
        numfree++;
        numalloc--;
    }
}


void CFixedSizeAllocator::stats(size32_t &sizealloc, size32_t &sizeunused)
{
    NonReentrantSpinBlock block(lock);
    sizealloc = numalloc*allocsize;
    sizeunused = numfree*allocsize;
}


//============================================================

#define LARGEST_CONTIGUOUS_BLOCK (0xffff0000)

void CContiguousLargeMemoryAllocator::init(size32_t _totalmax,size32_t _chunkmin,bool _throwexception)
{
    throwexception = _throwexception;
    totalmax = (_totalmax<LARGEST_CONTIGUOUS_BLOCK)?VMPAGEROUND(_totalmax):LARGEST_CONTIGUOUS_BLOCK;
    chunkmin = _chunkmin;
    ofs = 0;
    mapped = 0;
    base = NULL; 
#ifdef WIN32
    LARGE_INTEGER li;
    li.QuadPart = totalmax; 
    hmap = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE|SEC_RESERVE, li.HighPart, li.LowPart, NULL);
#endif
}

CContiguousLargeMemoryAllocator::CContiguousLargeMemoryAllocator()
{
    // values overwritten by init
    throwexception = true;
    totalmax = 0;
    chunkmin = 0x1000;
    ofs = 0;
    mapped = 0;
    base = NULL;
}

CContiguousLargeMemoryAllocator::~CContiguousLargeMemoryAllocator()
{
    reset();
#ifdef WIN32
    if (hmap) {
        if (base) 
            UnmapViewOfFile(base);
        CloseHandle(hmap);
    }
#endif
}

void *CContiguousLargeMemoryAllocator::getBase()
{
    if (!base) {
#ifdef WIN32
        if (hmap)
            base = (byte *) MapViewOfFile(hmap, FILE_MAP_READ|FILE_MAP_WRITE, 0, 0, totalmax);
        else
            base = NULL;
#else
        base = (byte *) mmap(NULL,totalmax,PROT_NONE,MAP_PRIVATE|MAP_NORESERVE|MAP_ANONYMOUS,-1,0);
        // create initially with no access
        if (base == (byte *)MAP_FAILED)
            base = NULL;
#endif
    }
    return base;
}

bool CContiguousLargeMemoryAllocator::map(size32_t tot,size32_t sz)
{
    getBase();
    if (!base||(tot>totalmax)) {
        outOfMem(sz);
        return false;
    }
    if (tot>mapped) {
        void * a = base+mapped;
        size32_t tomap = VMPAGEROUND(chunkmin); 
#ifdef WIN32
        if (VirtualAlloc(a,tomap,MEM_COMMIT,PAGE_READWRITE)!=a) {
            outOfMem(sz);
            return false;
        }
#else
        if (mprotect(a,tomap,PROT_READ|PROT_WRITE)<0) {
            int err = errno;
            if ((err==ENOMEM)||(err==EFAULT)) {
                outOfMem(sz);
                return false;
            }
            WARNLOG("CContiguousLargeMemoryAllocator:map madvise err=%d",err);
        }

#endif
        mapped = mapped+tomap;
    }
    return true;
}




void CContiguousLargeMemoryAllocator::unmap()
{
    // ensures above ofs is unmapped
    size32_t ch = VMPAGEROUND(chunkmin);
    size32_t newmapped = ((ofs+ch-1)/ch)*ch;
    if (newmapped<mapped) {
        void * a = base+newmapped;
#ifdef WIN32
        if (newmapped==0) { // free completely
            if (base) {
                UnmapViewOfFile(base);
                base = NULL;  
            }
        }
        else {
            VirtualFree(a,mapped-newmapped,MEM_DECOMMIT); // can't fail
        }
#else
        if (newmapped==0) { // free completely
            if (base) {
                munmap(base,totalmax);
                base = NULL;  
            }
        }
        else {
            if (mprotect(a,mapped-newmapped,PROT_NONE)<0) 
                WARNLOG("CContiguousLargeMemoryAllocator:unmap mprotect err=%d",errno);
//          if (madvise(a,mapped-newmapped,MADV_DONTNEED)<0)  // not sure if this does anything but tell it anyway
//              WARNLOG("CContiguousLargeMemoryAllocator:unmap madvise err=%d",errno);
        }
#endif
        mapped = newmapped;
    }
}


void CContiguousLargeMemoryAllocator::reset()
{
    reduceSize(ofs);
}

void CContiguousLargeMemoryAllocator::setSize(size32_t pos)
{
    assertex(ofs>=pos);
    reduceSize(ofs-pos);
}

void CContiguousLargeMemoryAllocator::reduceSize(size32_t amount)
{
    assertex(ofs>=amount);
    ofs-=amount;
    unmap();
}

void *CContiguousLargeMemoryAllocator::nextBuffer(void *prev,size32_t &sz)
{
    // have to be careful as approaches 4GB

    byte *p = prev?((byte *)prev):base;
    size32_t o = p-base;
    size32_t r = (o<ofs)?ofs-o:0;
    sz = (r<=chunkmin)?0:(r-chunkmin);
    if (sz==0)
        return NULL;
    if (sz>chunkmin) 
        sz = chunkmin;
    return p+chunkmin;
}


byte *CContiguousLargeMemoryAllocator::next(size32_t pos,size32_t &size)
{
    if (ofs<=pos) {
        size = 0;
        return NULL;
    }
    size = ofs-pos;
    return base+pos;
}

MemoryBuffer &CContiguousLargeMemoryAllocator::serialize(MemoryBuffer &mb)
{
    memcpy(mb.reserveTruncate(ofs),base,ofs);
    return mb;
}

MemoryBuffer &CContiguousLargeMemoryAllocator::deserialize(MemoryBuffer &mb,size32_t sz, size32_t extra)
{
    mb.read(sz,alloc(sz,extra));
    return mb;
}




void CContiguousLargeMemoryAllocator::outOfMem(size32_t sz)
{
    if (throwexception) {
        throw createOutOfMemException(-6,sz, ofs,true);
    }
}
