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

#ifndef eclrtl_imp_hpp
#define eclrtl_imp_hpp

#include "eclrtl.hpp"

class ECLRTL_API rtlDataAttr
{
public:
    inline rtlDataAttr()                { ptr=NULL; };
    inline rtlDataAttr(void *_ptr)      { ptr = _ptr; };
    inline rtlDataAttr(size32_t size)   { ptr=rtlMalloc(size); };
    inline ~rtlDataAttr()               { rtlFree(ptr); }

    inline void clear()                 { rtlFree(ptr); ptr = NULL; }

    inline void * * addrdata()          { clear(); return (void * *)&ptr; }
    inline char * * addrstr()           { clear(); return (char * *)&ptr; }
    inline UChar * * addrustr()         { clear(); return (UChar * *)&ptr; }

    inline void * detachdata()          { void * ret = ptr; ptr = NULL; return ret; }
    inline char * detachstr()           { void * ret = ptr; ptr = NULL; return (char *)ret; }
    inline UChar * detachustr()         { void * ret = ptr; ptr = NULL; return (UChar *)ret; }

    inline byte * getbytes() const      { return (byte *)ptr; }
    inline void * getdata() const       { return ptr; }
    inline char * getstr() const        { return (char *)ptr; }
    inline UChar * getustr() const      { return (UChar *)ptr; }

    inline void * & refdata()           { clear(); return *(void * *)&ptr; }
    inline char * & refstr()            { clear(); return *(char * *)&ptr; }
    inline UChar * & refustr()          { clear(); return *(UChar * *)&ptr; }

    inline void * & refexdata()         { return *(void * *)&ptr; }
    inline char * & refexstr()          { return *(char * *)&ptr; }
    inline UChar * & refexustr()        { return *(UChar * *)&ptr; }

    inline char * & refextendstr()      { return *(char * *)&ptr; }

    inline void setown(void * _ptr)     { rtlFree(ptr); ptr = _ptr; }

private:
    //Force errors....
    inline rtlDataAttr(const rtlDataAttr &);
    inline rtlDataAttr & operator = (const rtlDataAttr & other);

protected:
    void * ptr;
};

template <size32_t SIZE>
class rtlFixedSizeDataAttr : public rtlDataAttr
{
public:
    inline rtlFixedSizeDataAttr() : rtlDataAttr(SIZE) {}
};

class ECLRTL_API rtlRowBuilder : public rtlDataAttr
{
public:
    inline rtlRowBuilder()              { maxsize = 0; }

    inline void clear()                 { maxsize = 0; rtlDataAttr::clear(); }
    inline size32_t size()              { return maxsize; }

    inline void ensureAvailable(size32_t size)
    {
        if (size > maxsize)
            forceAvailable(size);
    }

    void forceAvailable(size32_t size);

protected:
    size32_t maxsize;
};
    
class ECLRTL_API rtlCompiledStrRegex
{
public:
    inline rtlCompiledStrRegex()            { regex = 0; }
    inline ~rtlCompiledStrRegex()           { rtlDestroyCompiledStrRegExpr(regex); }

    inline ICompiledStrRegExpr * operator -> () const { return regex; }
    inline void setPattern(const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledStrRegExpr(pattern, isCaseSensitive);
        if (regex)
            rtlDestroyCompiledStrRegExpr(regex);
        regex = compiled;
    }

private:
    ICompiledStrRegExpr * regex;
};

class ECLRTL_API rtlStrRegexFindInstance
{
public:
    inline rtlStrRegexFindInstance()            { instance = 0; }
    inline ~rtlStrRegexFindInstance()           { rtlDestroyStrRegExprFindInstance(instance); }
    inline IStrRegExprFindInstance * operator -> () const { return instance; }
    
    void find(const rtlCompiledStrRegex & regex, size32_t len, const char * str, bool needToKeepSearchString)
    {
        IStrRegExprFindInstance * search = regex->find(str, 0, len, needToKeepSearchString);
        if (instance)
            rtlDestroyStrRegExprFindInstance(instance);
        instance = search;
    }

private:
    IStrRegExprFindInstance * instance;
};

class ECLRTL_API rtlCompiledUStrRegex
{
public:
    inline rtlCompiledUStrRegex()           { regex = 0; }
    inline ~rtlCompiledUStrRegex()          { rtlDestroyCompiledUStrRegExpr(regex); }

    inline ICompiledUStrRegExpr * operator -> () const { return regex; }
    inline void setPattern(const UChar * pattern, bool isCaseSensitive)
    {
        ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(pattern, isCaseSensitive);
        if (regex)
            rtlDestroyCompiledUStrRegExpr(regex);
        regex = compiled;
    }

private:
    ICompiledUStrRegExpr * regex;
};

class ECLRTL_API rtlUStrRegexFindInstance
{
public:
    inline rtlUStrRegexFindInstance()           { instance = 0; }
    inline ~rtlUStrRegexFindInstance()          { rtlDestroyUStrRegExprFindInstance(instance); }
    inline IUStrRegExprFindInstance * operator -> () const { return instance; }
    
    void find(const rtlCompiledUStrRegex & regex, size32_t len, const UChar * str)
    {
        IUStrRegExprFindInstance * search = regex->find(str, 0, len);
        if (instance)
            rtlDestroyUStrRegExprFindInstance(instance);
        instance = search;
    }

private:
    IUStrRegExprFindInstance * instance;
};

//Code is cloned from jiface.hpp + split into two to avoid including too much in generated code.
#ifdef _WIN32
typedef volatile long atomic_t;
#define atomic_set(v,i) ((*v) = (i))
#else
#ifndef atomic_set
typedef struct { volatile int counter; } atomic_t;
#define atomic_set(v,i) (((v)->counter) = (i))
#endif
#endif

class ECLRTL_API RtlCInterface
{
public:
             RtlCInterface()        { atomic_set(&xxcount, 1); }
    virtual ~RtlCInterface()        { }
//interface IInterface:
    void    Link() const;
    bool    Release(void) const;

private:
    mutable atomic_t    xxcount;
};


#define RTLIMPLEMENT_IINTERFACE                                                 \
    virtual void Link(void) const       { RtlCInterface::Link(); }              \
    virtual bool Release(void) const    { return RtlCInterface::Release(); }


//Inline definitions of the hash32 functions for small sizes - to optimize aggregate hash
#define FNV_32_PRIME_VALUE 0x1000193
#define FNV_32_HASHONE_VALUE(hval, next)            \
        ((hval * FNV_32_PRIME_VALUE) ^ next)

inline unsigned rtlHash32Data1(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(hval, buf[0]);
}

inline unsigned rtlHash32Data2(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    hval, 
                    buf[0]),
                buf[1]);
}
inline unsigned rtlHash32Data3(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        hval, 
                        buf[0]),
                    buf[1]),
                buf[2]);
}
inline unsigned rtlHash32Data4(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        FNV_32_HASHONE_VALUE(
                            hval, 
                            buf[0]),
                        buf[1]),
                    buf[2]),
                buf[3]);
}
inline unsigned rtlHash32Data5(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        FNV_32_HASHONE_VALUE(
                            FNV_32_HASHONE_VALUE(
                                hval, 
                                buf[0]),
                            buf[1]),
                        buf[2]),
                    buf[3]),
                buf[4]);
}
inline unsigned rtlHash32Data6(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        FNV_32_HASHONE_VALUE(
                            FNV_32_HASHONE_VALUE(
                                FNV_32_HASHONE_VALUE(
                                    hval, 
                                    buf[0]),
                                buf[1]),
                            buf[2]),
                        buf[3]),
                    buf[4]),
                buf[5]);
}
inline unsigned rtlHash32Data7(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        FNV_32_HASHONE_VALUE(
                            FNV_32_HASHONE_VALUE(
                                FNV_32_HASHONE_VALUE(
                                    FNV_32_HASHONE_VALUE(
                                        hval, 
                                        buf[0]),
                                    buf[1]),
                                buf[2]),
                            buf[3]),
                        buf[4]),
                    buf[5]),
                buf[6]);
}
inline unsigned rtlHash32Data8(const void *_buf, unsigned hval)
{
    const byte * buf = (const byte *)_buf;
    return FNV_32_HASHONE_VALUE(
                FNV_32_HASHONE_VALUE(
                    FNV_32_HASHONE_VALUE(
                        FNV_32_HASHONE_VALUE(
                            FNV_32_HASHONE_VALUE(
                                FNV_32_HASHONE_VALUE(
                                    FNV_32_HASHONE_VALUE(
                                        FNV_32_HASHONE_VALUE(
                                            hval, 
                                            buf[0]),
                                        buf[1]),
                                    buf[2]),
                                buf[3]),
                            buf[4]),
                        buf[5]),
                    buf[6]),
                buf[7]);
}

#endif
