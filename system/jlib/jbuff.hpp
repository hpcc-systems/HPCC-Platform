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



#ifndef JBUFF_HPP
#define JBUFF_HPP

#include "jiface.hpp"
#include "jmutex.hpp"

class StringAttr;
class StringBuffer;

class jlib_decl MemoryAttr
{
public:
    inline MemoryAttr() { len = 0; ptr = NULL; }
    MemoryAttr(size_t _len);
    MemoryAttr(size_t _len, const void * _ptr);
    MemoryAttr(const MemoryAttr & src);
    inline ~MemoryAttr() { free(ptr); }

    void *              allocate(size_t _len);
    void                clear();
    void *              detach();
    void *              ensure(size_t _len);
    void *              reallocate(size_t _len);
    inline const void * get() const         { return ptr; }
    inline byte         getByte(size_t offset) const { return ((const byte *)ptr)[offset]; }
    inline size_t       length() const      { return len; }
    inline void *       mem() const         { return ptr; }
    void                set(size_t _len, const void * _ptr);
    void                setOwn(size_t _len, void * _ptr);
    void                swapWith(MemoryAttr & other);
    
    static int          compare(const MemoryAttr & m1, const MemoryAttr & m2);

    inline void *       bufferBase() const { return ptr; } // like get except non-const

private:
    void * ptr;
    size_t len;
};

//--------------------------------------------------------------------------------------------------------------------

template <class CLASS, void (*FREE_FUNC)(CLASS *)> class OwnedPtrCustomFree
{
    typedef OwnedPtrCustomFree<CLASS, FREE_FUNC> SELF;
    void safeFree(CLASS *_ptr) { if (_ptr) FREE_FUNC(_ptr); }
protected:
    CLASS *ptr = nullptr;

public:
    OwnedPtrCustomFree() { }
    OwnedPtrCustomFree(CLASS *_ptr) : ptr(_ptr) { }
    OwnedPtrCustomFree(SELF &&_ptr) { ptr = _ptr.getClear(); }
    ~OwnedPtrCustomFree() { safeFree(ptr); }

    void operator = (CLASS * _ptr)
    {
        safeFree(ptr);
        ptr = _ptr;
    }
    inline CLASS * operator -> () const { return ptr; }
    inline operator CLASS *() const     { return ptr; }

    inline void clear()                 { CLASS *temp=ptr; ptr=nullptr; safeFree(temp); }
    inline CLASS *get() const           { return ptr; }
    inline CLASS *getClear()            { CLASS * temp = ptr; ptr=nullptr; return temp; }
    inline void setown(CLASS *_ptr)     { safeFree(ptr); ptr = _ptr; }

    void operator = (const OwnedPtrCustomFree<CLASS, FREE_FUNC> & other) = delete;
    void setown(const OwnedPtrCustomFree<CLASS, FREE_FUNC> &other) = delete;
};

//A simple object container/smart pointer
template <typename CLASS> inline void ownedPtrDoDelete(CLASS *o) { delete o; }
template <class CLASS>
using OwnedPtr = OwnedPtrCustomFree<CLASS, ownedPtrDoDelete<CLASS>>;

template <typename CLASS> void inline ownedMallocDoFree(CLASS *o) { free(o); }
template <class CLASS> class OwnedMalloc : public OwnedPtrCustomFree<CLASS, ownedMallocDoFree<CLASS>>
{
    typedef OwnedPtrCustomFree<CLASS, ownedMallocDoFree<CLASS>> PARENT;
    typedef OwnedMalloc<CLASS> SELF;
public:
    inline OwnedMalloc() : PARENT() { }
    inline OwnedMalloc(CLASS * _ptr) : PARENT(_ptr) { }
    inline OwnedMalloc(SELF && _other) = default;
    explicit inline OwnedMalloc(unsigned n, bool clearMemory = false) { doAllocate(n, clearMemory); }
    inline void allocate(bool clearMemory = false)   { allocateN(1, clearMemory); }
    inline void allocateN(unsigned n, bool clearMemory = false)
    {
        PARENT::clear();
        doAllocate(n, clearMemory);
    }

    void allocate(unsigned n, bool clearMemory = false) = delete;
    void operator = (CLASS * _ptr) = delete;
private:
    inline void doAllocate(unsigned n, bool clearMemory = false)
    {
        void * mem = clearMemory ? calloc(n, sizeof(CLASS)) : malloc(n * sizeof(CLASS));
        PARENT::ptr = static_cast<CLASS *>(mem);
    }
};


#define MEMBUFFER_MAXLEN UINT_MAX // size32_t

class jlib_decl MemoryBuffer
{
public:
    constexpr MemoryBuffer() = default;
    MemoryBuffer(size_t initial);
    MemoryBuffer(size_t len, const void * buffer);
    MemoryBuffer(MemoryBuffer & value) = delete;
    inline ~MemoryBuffer() { kill(); }
    
    MemoryBuffer &  rewrite(size32_t pos = 0);
    MemoryBuffer &  append(char value);
    MemoryBuffer &  append(unsigned char value);
    MemoryBuffer &  append(bool value);
    MemoryBuffer &  append(const char * value);
    MemoryBuffer &  append(const unsigned char * value);
    MemoryBuffer &  append(size32_t len, const void * value);
    MemoryBuffer &  append(double value);
    MemoryBuffer &  append(float value);
    MemoryBuffer &  append(short value);
    MemoryBuffer &  append(unsigned short value);
    MemoryBuffer &  append(int value);
    MemoryBuffer &  append(unsigned value);
    MemoryBuffer &  append(__int64 value);
    MemoryBuffer &  append(unsigned __int64 value);
    MemoryBuffer &  append(const MemoryBuffer & value);
    MemoryBuffer &  appendBytes(unsigned char value, unsigned count);
    MemoryBuffer &  appendEndian(size32_t len, const void * value);
    MemoryBuffer &  appendFile(const char *fileName);
    MemoryBuffer &  appendSwap(size32_t len, const void * value);
    MemoryBuffer &  appendPacked(unsigned __int64 value); // compatible with any unsigned size
    inline MemoryBuffer &  appendMemSize(memsize_t  value) { __int64 val=(__int64)value; append(val); return *this; }

    MemoryBuffer &  reset(size32_t pos = 0);
    MemoryBuffer &  read(char & value);
    MemoryBuffer &  read(unsigned char & value);
    MemoryBuffer &  read(bool & value);
    MemoryBuffer &  read(StringAttr & value);
    MemoryBuffer &  read(StringBuffer & value);
    MemoryBuffer &  read(const char * & value);
    MemoryBuffer &  read(size32_t len, void * value);
    MemoryBuffer &  read(double & value);
    MemoryBuffer &  read(float & value);
    MemoryBuffer &  read(short & value);
    MemoryBuffer &  read(unsigned short & value);
    MemoryBuffer &  read(int & value);
    MemoryBuffer &  read(unsigned & value);
    MemoryBuffer &  read(__int64 & value);
    MemoryBuffer &  read(unsigned __int64 & value);
    MemoryBuffer &  readEndian(size32_t len, void * value);
    MemoryBuffer &  readFile(StringAttr &fileName);
    MemoryBuffer &  readSwap(size32_t len, void * value);
    const byte *    readDirect(size32_t len);                                       // for efficiency
    MemoryBuffer &  readPacked(unsigned & value);
    MemoryBuffer &  readPacked(unsigned __int64 & value);

    inline MemoryBuffer &  readMemSize(memsize_t & value) { __int64 val; read(val); value = (memsize_t)val; assertex(val == (__int64) value); return *this; }
    MemoryBuffer &  skip(unsigned len);
    void            writeDirect(size32_t pos,size32_t len,const void *buf);         // NB does not extend buffer
    void            writeEndianDirect(size32_t pos,size32_t len,const void *buf);   // NB does not extend buffer
    inline size32_t         getPos() { return readPos; };                               // read ptr
    
    inline MemoryBuffer &  clear() { curLen = 0; readPos = 0; return *this; }

    inline bool     needSwapEndian() { return swapEndian; }
    int             setEndian(int endian);          // pass __[BIG|LITTLE]_ENDIAN
    bool            setSwapEndian(bool swap);
    void            swapWith(MemoryBuffer & other);
    bool            matches(const MemoryBuffer & other) const;

    inline size32_t capacity() { return (maxLen - curLen); }
    void *          ensureCapacity (unsigned max);
    inline size32_t length() const { return curLen; }
    inline size32_t remaining() const { return curLen - readPos; }

    void            resetBuffer();
    void            setBuffer(size_t len, void * buffer, bool takeOwnership=false);
    void            setLength(unsigned len);
    void            setWritePos(unsigned len);      // only use for back patching data
    void *          detach();
    void *          detachOwn();  // Never reallocates
    //Non-standard functions:
    void *          reserve(unsigned size);
    void            truncate();                     // truncates (i.e. minimizes allocation) to current size
    void *          reserveTruncate(unsigned size); // reserves and truncates to that size
    void *          insertDirect(unsigned offset, size32_t len); // insert len bytes at offset returning address to area inserted
    inline void     Release() const                         { delete this; }    // for consistency even though not link counted

    inline void *   bufferBase() const { return buffer; }
    inline const char * toByteArray() const { return curLen ? buffer : nullptr; }
    inline const byte * bytes() const { return curLen ? (const byte *)buffer : nullptr; }

private:
    MemoryBuffer &  read(unsigned long & value);    // unimplemented
    MemoryBuffer &  read(long & value);             // unimplemented
    MemoryBuffer &  append(long value);             // unimplemented
    MemoryBuffer &  append(unsigned long value);    // unimplemented

    void _insert(unsigned offset, size32_t len);
    void init();
    void kill();
    unsigned __int64 readPacked();
    void _realloc(size32_t max);
    void _reallocExact(size32_t max);
    MemoryBuffer & _remove(unsigned start, unsigned len);
    MemoryBuffer & _reverse();
    const char* _str();
    
    mutable char * buffer = nullptr;
    size32_t curLen = 0;
    size32_t maxLen = 0;
    size32_t readPos = 0;
    bool swapEndian= false;
    bool ownBuffer = true;
};

// Utility class, to back patch a scalar into current position
template <class CLASS>
class DelayedMarker
{
protected:
    MemoryBuffer &mb;
    size32_t pos;
public:
    DelayedMarker(MemoryBuffer &_mb) : mb(_mb)
    {
        restart();
    }
    inline void write(CLASS a)
    {
        mb.writeEndianDirect(pos, sizeof(a), &a);
    }
    // resets position marker and writes CLASS # bytes to be filled subsequently by write()
    inline void restart()
    {
        pos = mb.length();
        mb.appendBytes(0, sizeof(CLASS));
    }
};

// Utility class, to back patch a size into current position
class DelayedSizeMarker : private DelayedMarker<size32_t>
{
    typedef DelayedMarker<size32_t> PARENT;
public:
    DelayedSizeMarker(MemoryBuffer &mb) : PARENT(mb)
    {
    }
    inline void write()
    {
        size32_t sz = size();
        PARENT::write(sz);
    }
    inline size32_t size() const
    {
        return (size32_t)(mb.length() - (pos + sizeof(size32_t)));
    }
    inline void restart() { PARENT::restart(); }
};

interface jlib_decl serializable : extends IInterface
{
public:
    virtual void serialize(MemoryBuffer &tgt) = 0;
    virtual void deserialize(MemoryBuffer &src) = 0;
};

class jlib_decl MemoryBuffer2IDataVal : implements IDataVal
{

public:
    MemoryBuffer2IDataVal(MemoryBuffer & _buffer) : buffer(_buffer) {}

    virtual const void * data() const { return buffer.toByteArray(); }
    virtual void clear() { } // clearing when appending does nothing
    virtual void setLen(const void * val, size_t length) { buffer.append((size32_t)length, val); }
    virtual size_t length() const { return buffer.length(); }
    virtual void * reserve(size_t length) { return buffer.reserveTruncate((size32_t)length); }

private:
    MemoryBuffer & buffer;
};

class jlib_decl MemoryAttr2IStringVal : implements IStringVal
{
public:
     MemoryAttr2IStringVal(MemoryAttr & _attr) : attr(_attr) {}

     virtual const char * str() const;
     virtual void set(const char *val) { attr.set(strlen(val), val); }
     virtual void clear() { attr.clear(); } // clearing when appending does nothing
     virtual void setLen(const char *val, unsigned length) { attr.set(length, val); }
     virtual unsigned length() const { return (size32_t)attr.length(); };

protected:
     MemoryAttr & attr;
};

class jlib_decl Variable2IDataVal : implements IDataVal
{
public:
    Variable2IDataVal(unsigned * _pLen, void * * _pData) { pLen = _pLen; pData = _pData; }

    virtual const void * data() const { return *pData; };
    virtual void clear() { free(*pData); *pData = NULL; *pLen = 0; };
    virtual void setLen(const void * val, size_t length) { free(*pData); *pData = malloc(length); memcpy_iflen(*pData, val, length); *pLen = (size32_t)length; }
    virtual size_t length() const { return *pLen; };
    virtual void * reserve(size_t length) { free(*pData); *pData = malloc(length); *pLen = (size32_t)length; return *pData; }

private:
    unsigned * pLen; // Should really be a size_t
    void * * pData;
};

//Similar to above, but only used for fixed sized returns (or variable size rows with a known max length)
class jlib_decl Fixed2IDataVal : implements IDataVal
{
public:
    Fixed2IDataVal(size_t _len, void * _ptr) { len = _len; ptr = _ptr; }

    virtual const void * data() const { return ptr; };
    virtual void clear() { memset(ptr, 0, len); };
    virtual void setLen(const void * val, size_t length) { assertex(length <= len); memcpy_iflen(ptr, val, length); }
    virtual size_t length() const { return len; };
    virtual void * reserve(size_t length) { assertex(length <= len); return ptr; }

private:
    size_t len;
    void * ptr;
};

#ifdef __GNUC__
class jlib_decl GccMemoryBuffer2IDataVal
{
public:
    GccMemoryBuffer2IDataVal(MemoryBuffer & _buffer) : adaptor(_buffer) {}
    inline operator IDataVal & ()       { return adaptor; }
private:
    MemoryBuffer2IDataVal adaptor;
};

class jlib_decl GccVariable2IDataVal
{
public:
    GccVariable2IDataVal(unsigned * _pLen, void * * _pData) : adaptor(_pLen, _pData) {}
    inline operator IDataVal & ()       { return adaptor; }
private:
    Variable2IDataVal adaptor;
};


#define MemoryBuffer2IDataVal GccMemoryBuffer2IDataVal
#define Variable2IDataVal GccVariable2IDataVal
#endif

extern jlib_decl MemoryBuffer & serialize(MemoryBuffer & buffer, const MemoryAttr & value);
extern jlib_decl MemoryBuffer & deserialize(MemoryBuffer & buffer, MemoryAttr & value);
extern jlib_decl MemoryBuffer & serialize(MemoryBuffer & buffer, const char * value);
extern jlib_decl MemoryBuffer & deserialize(MemoryBuffer & buffer, StringAttr & value);


class  jlib_decl CLargeMemoryAllocator
{
protected:
    struct Chunk
    {
        Chunk *prev;
        byte  *base;
        size32_t max;
        size32_t size;
    } chunk;
    memsize_t totalmax;
    size32_t chunkmin;
    memsize_t amax; // total of max values
    memsize_t atot; // total allocated not including chunk.size
    bool throwexception;
    bool newchunk(size32_t sz,size32_t extra,bool exceptionwanted);
    virtual void allocchunkmem();
    virtual void disposechunkmem();
public:
    CLargeMemoryAllocator(memsize_t _totalmax,size32_t _chunkmin,bool _throwexception)
    {
        init(_totalmax,_chunkmin,_throwexception);
    }

    CLargeMemoryAllocator();

    void init(memsize_t _totalmax,size32_t _chunkmin,bool _throwexception);

    virtual ~CLargeMemoryAllocator()
    {
        reset();
    }

    inline void setTotalMax(memsize_t total)
    {
        totalmax = total;
    }

    inline memsize_t getTotalMax()
    {
        return totalmax;
    }

    inline byte *alloc(size32_t sz,size32_t extra=0)
    {
        size32_t chsize = chunk.size;
        if (chsize+sz>chunk.max) {
            if (!newchunk(sz,extra,throwexception))
                return NULL;
            chsize = chunk.size;
        }
        byte *ret = chunk.base+chsize;
        chunk.size = chsize+sz;
        return ret;
    }

    inline bool checkAvail(size32_t sz, size32_t sza=0,size32_t extra=0)
    {
        if (chunk.size+sz>chunk.max) {
            if (sza<sz)
                sza = sz;
            if (!newchunk(sza,extra,false))
                return false;
        }
        return true;
    }

    inline memsize_t allocated()
    {
        return chunk.size+atot;
    }

    inline memsize_t maxallocated()
    {
        return amax;
    }

    void setChunkGranularity(size32_t sz)
    {
        if (sz&&(chunkmin>sz)) 
            chunkmin -= (chunkmin%sz);
    }

    void reset();
    void setSize(memsize_t pos);
    void reduceSize(memsize_t amount);
    byte *next(memsize_t pos,size32_t &size); // this should not be used for small jumps as it is slow
    MemoryBuffer &serialize(MemoryBuffer &mb);
    MemoryBuffer &deserialize(MemoryBuffer &mb,size32_t sz, size32_t extra=0);

    void *nextBuffer(void *prev,size32_t &sz); // to enumerate buffers (NULL returns first)

};




class CLargeMemorySequentialReader
{
    size32_t left = 0;
    memsize_t pos = 0;
    const void *ptr = nullptr;
    CLargeMemoryAllocator &allocator;

    inline CLargeMemorySequentialReader(CLargeMemoryAllocator &_allocator)
        : allocator(_allocator)
    {
    }

    inline const void *read(size32_t &max)
    {
        if (!left) {
            ptr = allocator.next(pos,left);
            if (!left) 
                return NULL;
        }
        max = left;
        return ptr;
    }

    inline void skip(size32_t sz)
    {
        assertex(left>=sz);
        left -= sz;
        pos += sz;
        ptr = (const byte *)ptr+sz;
    }
};


interface IOutOfMemException;
jlib_decl IOutOfMemException *createOutOfMemException(int errcode, size_t wanted, size_t got=0, bool expected=false, const char *errMsg=nullptr);
jlib_decl void RaiseOutOfMemException(int errcode, size_t wanted, size_t got=0, bool expected=false, const char *errMsg=nullptr);

interface ILargeMemLimitNotify: extends IInterface
{
    virtual bool take(memsize_t tot)=0;     // called when a memory request about to be satisfied will exceed limit
                                            // will raise oom exception if false returned
    virtual void give(memsize_t tot)=0;     // called when the memory allocated falls back below the limit
};

extern jlib_decl void setLargeMemLimitNotify(memsize_t size,ILargeMemLimitNotify *notify);

inline void *checked_malloc(size_t len,int errcode)
{
    if (len==0)
        return NULL;
    void *ret = malloc(len);
    if (!ret)
        RaiseOutOfMemException(errcode, len);
    return ret;
}

jlib_decl void *checked_realloc(void *orig, size_t newlen, size_t origlen,int errcode);

class NonReentrantSpinLock;

class  jlib_decl CFixedSizeAllocator
{
private:
    void *freelist;
    void *chunklist;
    NonReentrantSpinLock lock;
    unsigned numalloc;
    unsigned numfree;
    size32_t allocsize;
    size32_t chunksize;
    void *allocChunk();
    void freeChunk(void *);
public:
    CFixedSizeAllocator();
    CFixedSizeAllocator(size32_t _allocsize,size32_t _chunksize=0x100000);
    virtual ~CFixedSizeAllocator();
    void init(size32_t _allocsize,size32_t _chunksize=0x100000);
    void kill(); 
    void *alloc();
    void dealloc(void *blk);
    void stats(size32_t &sizealloc, size32_t &sizeunused);
};

#endif
