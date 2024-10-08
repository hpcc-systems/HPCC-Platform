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
    inline rtlRowBuilder(size32_t size) { forceAvailable(size); }

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
    
class ECLRTL_API rtlEmptyRowBuilder
{
public:
    inline void ensureAvailable(size32_t size) {}
    inline byte * getbytes()  { return NULL; }
};

template <unsigned maxsize>
class rtlFixedRowBuilder
{
public:
    inline void ensureAvailable(size32_t size) {}
    inline byte * getbytes()  { return data; }

protected:
    byte data[maxsize];
};

class ECLRTL_API rtlCompiledStrRegex
{
public:
    inline rtlCompiledStrRegex()            { regex = 0; }

    inline rtlCompiledStrRegex(const char * pattern, bool isCaseSensitive)
    {
        regex = rtlCreateCompiledStrRegExpr(pattern, isCaseSensitive);
    }

    inline ~rtlCompiledStrRegex()           { rtlDestroyCompiledStrRegExpr(regex); }

    inline ICompiledStrRegExpr * operator -> () const { return regex; }

    inline void setPattern(const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledStrRegExpr(pattern, isCaseSensitive);
        if (regex)
            rtlDestroyCompiledStrRegExpr(regex);
        regex = compiled;
    }

    inline void setPatternTimed(ISectionTimer * timer, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledStrRegExprTimed(timer, pattern, isCaseSensitive);
        if (regex)
            rtlDestroyCompiledStrRegExpr(regex);
        regex = compiled;
    }

    inline void setPattern(unsigned int patternLen, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledStrRegExpr(patternLen, pattern, isCaseSensitive);
        if (regex)
            rtlDestroyCompiledStrRegExpr(regex);
        regex = compiled;
    }

    inline void setPatternTimed(ISectionTimer * timer, unsigned int patternLen, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledStrRegExprTimed(timer, patternLen, pattern, isCaseSensitive);
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
    
    void findTimed(ISectionTimer * timer, const rtlCompiledStrRegex & regex, size32_t len, const char * str, bool needToKeepSearchString)
    {
        IStrRegExprFindInstance * search = regex->findTimed(timer, str, 0, len, needToKeepSearchString);
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
    inline rtlCompiledUStrRegex()           { instance = 0; }
    inline rtlCompiledUStrRegex(const UChar * pattern, bool isCaseSensitive)
    {
        instance = rtlCreateCompiledUStrRegExpr(pattern, isCaseSensitive);
    }

    inline ~rtlCompiledUStrRegex()          { rtlDestroyCompiledUStrRegExpr(instance); }

    inline ICompiledUStrRegExpr * operator -> () const { return instance; }

    inline void setPattern(const UChar * pattern, bool isCaseSensitive)
    {
        ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledUStrRegExpr(instance);
        instance = compiled;
    }

    inline void setPatternTimed(ISectionTimer * timer, const UChar * pattern, bool isCaseSensitive)
    {
        ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExprTimed(timer, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledUStrRegExpr(instance);
        instance = compiled;
    }

    inline void setPattern(unsigned int patternLen, const UChar * pattern, bool isCaseSensitive)
    {
        ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExpr(patternLen, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledUStrRegExpr(instance);
        instance = compiled;
    }

    inline void setPatternTimed(ISectionTimer * timer, unsigned int patternLen, const UChar * pattern, bool isCaseSensitive)
    {
        ICompiledUStrRegExpr * compiled = rtlCreateCompiledUStrRegExprTimed(timer, patternLen, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledUStrRegExpr(instance);
        instance = compiled;
    }

private:
    ICompiledUStrRegExpr * instance;
};

class ECLRTL_API rtlCompiledU8StrRegex
{
public:
    inline rtlCompiledU8StrRegex()           { instance = 0; }
    inline rtlCompiledU8StrRegex(const char * pattern, bool isCaseSensitive)
    {
        instance = rtlCreateCompiledU8StrRegExpr(pattern, isCaseSensitive);
    }

    inline ~rtlCompiledU8StrRegex()          { rtlDestroyCompiledU8StrRegExpr(instance); }

    inline ICompiledStrRegExpr * operator -> () const { return instance; }

    inline void setPattern(const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledU8StrRegExpr(pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledU8StrRegExpr(instance);
        instance = compiled;
    }

    inline void setPatternTimed(ISectionTimer * timer, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledU8StrRegExprTimed(timer, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledU8StrRegExpr(instance);
        instance = compiled;
    }
    
    inline void setPattern(unsigned int patternLen, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledU8StrRegExpr(patternLen, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledU8StrRegExpr(instance);
        instance = compiled;
    }
    
    inline void setPatternTimed(ISectionTimer * timer, unsigned int patternLen, const char * pattern, bool isCaseSensitive)
    {
        ICompiledStrRegExpr * compiled = rtlCreateCompiledU8StrRegExprTimed(timer, patternLen, pattern, isCaseSensitive);
        if (instance)
            rtlDestroyCompiledU8StrRegExpr(instance);
        instance = compiled;
    }

private:
    ICompiledStrRegExpr * instance;
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
    
    void findTimed(ISectionTimer * timer, const rtlCompiledUStrRegex & regex, size32_t len, const UChar * str)
    {
        IUStrRegExprFindInstance * search = regex->findTimed(timer, str, 0, len);
        if (instance)
            rtlDestroyUStrRegExprFindInstance(instance);
        instance = search;
    }

private:
    IUStrRegExprFindInstance * instance;
};

class ECLRTL_API rtlU8StrRegexFindInstance
{
public:
    inline rtlU8StrRegexFindInstance()           { instance = 0; }

    inline ~rtlU8StrRegexFindInstance()          { rtlDestroyU8StrRegExprFindInstance(instance); }

    inline IStrRegExprFindInstance * operator -> () const { return instance; }
    
    void find(const rtlCompiledU8StrRegex & regex, size32_t len, const char * str, bool needToKeepSearchString)
    {
        IStrRegExprFindInstance * search = regex->find(str, 0, len, needToKeepSearchString);
        if (instance)
            rtlDestroyU8StrRegExprFindInstance(instance);
        instance = search;
    }
    
    void findTimed(ISectionTimer * timer, const rtlCompiledU8StrRegex & regex, size32_t len, const char * str, bool needToKeepSearchString)
    {
        IStrRegExprFindInstance * search = regex->findTimed(timer, str, 0, len, needToKeepSearchString);
        if (instance)
            rtlDestroyU8StrRegExprFindInstance(instance);
        instance = search;
    }

private:
    IStrRegExprFindInstance * instance;
};

class ECLRTL_API RtlCInterface
{
public:
    virtual ~RtlCInterface()        { }
//interface IInterface:
    void    Link() const;
    bool    Release(void) const;

private:
    mutable std::atomic<unsigned> xxcount{1};
};


#define RTLIMPLEMENT_IINTERFACE                                                 \
    virtual void Link(void) const override     { RtlCInterface::Link(); }              \
    virtual bool Release(void) const override  { return RtlCInterface::Release(); }


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
