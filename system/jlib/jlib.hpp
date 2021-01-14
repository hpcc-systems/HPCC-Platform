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



#ifndef JLIB_HPP
#define JLIB_HPP

#define EXPLICIT_INIT

#ifdef _MSC_VER
//disable these throughout the system because they occur a lot
#pragma warning(disable : 4275 ) // should get link errors if something is wrong...
#pragma warning(disable : 4251 ) // should get link errors if something is wrong...
#pragma warning(disable : 4786 ) // identifier was truncated to '255' characters in the debug information
#pragma warning(disable : 4355 ) // 'this' : used in base member initializer list 
#endif

#include "modinit.h"
#include "jiface.hpp"
#include <assert.h>
#include "jarray.hpp"

#define _elements_in(a) (sizeof(a)/sizeof((a)[0]))
#define _memclr(s, n)      memset(s, 0, n)
#define _clear(a)          memset(&a, 0, sizeof(a))
#define _copy(dest, src)   memcpy(&dest, &src, sizeof(src))


class jlib_decl ICopyArray : public CopyReferenceArrayOf<IInterface> {};
class jlib_decl IArray : public OwnedReferenceArrayOf<IInterface> {};
class jlib_decl IPointerArray : public OwnedPointerArrayOf<IInterface> {};
class jlib_decl IConstPointerArray : public OwnedConstPointerArrayOf<IInterface> {};

class jlib_decl CICopyArray : public CopyReferenceArrayOf<CInterface> {};
class jlib_decl CIArray : public OwnedReferenceArrayOf<CInterface> {};

class jlib_decl CharArray : public ArrayOf<char> { };
class jlib_decl IntArray : public ArrayOf<int> { };
class jlib_decl ShortArray : public ArrayOf<short> { };
class jlib_decl FloatArray : public ArrayOf<float> { };
class jlib_decl DoubleArray : public ArrayOf<double> { };
class jlib_decl UnsignedArray : public ArrayOf<unsigned> { };
class jlib_decl UnsignedShortArray : public ArrayOf<unsigned short> { };
class jlib_decl PointerArray : public ArrayOf<void *> { };
class jlib_decl ConstPointerArray : public ArrayOf<const void *> { };

#ifdef _WIN32
class jlib_decl BoolArray : public ArrayOf<bool> { };
#else
typedef CharArray BoolArray;
#endif
class jlib_decl Int64Array : public ArrayOf<__int64> { };
class jlib_decl UInt64Array : public ArrayOf<unsigned __int64> { };

template <class A, class C, class IITER>
class ArrayIIteratorOf : implements IITER, public CInterface
{
protected:
    ArrayIteratorOf <A, C &> iterator;

public: 
    IMPLEMENT_IINTERFACE;

    ArrayIIteratorOf(A &array) : iterator(array)    {   }

    virtual bool first() { return iterator.first(); }
    virtual bool next() { return iterator.next(); }
    virtual bool isValid() { return iterator.isValid(); }
    virtual C & query() { return iterator.query(); }    
};


template <class TYPE>
class CIArrayOf : public CIArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)CIArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)CIArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)CIArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)CIArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)CIArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)CIArray::detach(); }
    inline void append(TYPE& obj)                 { assert(&obj); CIArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); CIArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); CIArray::add(obj, pos); } 
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return CIArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); CIArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return CIArray::zap(obj, nodel); }
};

template <class TYPE>
class CICopyArrayOf : public CICopyArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)CICopyArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)CICopyArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)CICopyArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)CICopyArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)CICopyArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)CICopyArray::detach(); }
    inline void append(TYPE& obj)                 { assert(&obj); CICopyArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); CICopyArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); CICopyArray::add(obj, pos); } 
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return CICopyArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos)  { assert(&obj); CICopyArray::replace(obj, pos); }
    inline bool zap(TYPE & obj)                   { assert(&obj); return CICopyArray::zap(obj); }
};

template <class TYPE>
class IArrayOf : public IArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)IArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)IArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)IArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)IArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)IArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)IArray::detach(); }
    inline void append(TYPE& obj)                 { assert(&obj); IArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); IArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); IArray::add(obj, pos); } 
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return IArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); IArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return IArray::zap(obj, nodel); }
};

template <class BTYPE>
class IConstArrayOf : public IArray
{
    typedef const BTYPE TYPE;
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)IArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)IArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)IArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)IArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)IArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)IArray::detach(); }
    inline void append(TYPE& obj)                 { assert(&obj); IArray::append((BTYPE &) obj); }
    inline void appendUniq(TYPE& obj)             { assert(&obj); IArray::appendUniq((BTYPE &) obj); }
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); IArray::add((BTYPE &) obj, pos); }
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return IArray::find((BTYPE &) obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); IArray::replace((BTYPE &) obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return IArray::zap((BTYPE &) obj, nodel); }
};

template <class TYPE, class BASE>
class IBasedArrayOf : public IArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)(BASE &)IArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)(BASE &)IArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)(BASE &)IArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)(BASE &)IArray::tos(num); }
    inline void append(TYPE& obj)                 { assert(&obj); IArray::append((BASE &)obj); }
    inline void appendUniq(TYPE& obj)             { assert(&obj); IArray::appendUniq((BASE &)obj); }
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); IArray::add((BASE &)obj, pos); }
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return IArray::find((BASE&)obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); IArray::replace((BASE &)obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return IArray::zap((BASE &)obj, nodel); }
};

template <class TYPE>
class ICopyArrayOf : public ICopyArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)ICopyArray::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)ICopyArray::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)ICopyArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)ICopyArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)ICopyArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)ICopyArray::detach(); }
    inline void append(TYPE& obj)                 { assert(&obj); ICopyArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); ICopyArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); ICopyArray::add(obj, pos); } 
    inline NoBool<aindex_t> find(TYPE & obj) const        { assert(&obj); return ICopyArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos) { assert(&obj); ICopyArray::replace(obj, pos); }
    inline bool zap(TYPE & obj)                   { assert(&obj); return ICopyArray::zap(obj); }
};

template <class TYPE>
class IPointerArrayOf : public IPointerArray
{
public:
    inline TYPE * item(aindex_t pos) const        { return (TYPE *)IPointerArray::item(pos); }
    inline TYPE * popGet()                        { return (TYPE *)IPointerArray::popGet(); }
    inline TYPE * tos(void) const                 { return (TYPE *)IPointerArray::tos(); }
    inline TYPE * tos(aindex_t num) const         { return (TYPE *)IPointerArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)IPointerArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)IPointerArray::detach(); }
    inline void append(TYPE * obj)                { IPointerArray::append(obj); } 
    inline void appendUniq(TYPE * obj)            { IPointerArray::appendUniq(obj); } 
    inline void add(TYPE * obj, aindex_t pos)     { IPointerArray::add(obj, pos); } 
    inline NoBool<aindex_t> find(TYPE * obj) const        { return IPointerArray::find(obj); }
    inline void replace(TYPE * obj, aindex_t pos, bool nodel=false) { IPointerArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE * obj, bool nodel=false) { return IPointerArray::zap(obj, nodel); }
};

template <class BTYPE>
class IConstPointerArrayOf : public IConstPointerArray
{
    typedef const BTYPE TYPE;
public:
    inline TYPE * item(aindex_t pos) const        { return (TYPE *)IConstPointerArray::item(pos); }
    inline TYPE * popGet()                        { return (TYPE *)IConstPointerArray::popGet(); }
    inline TYPE * tos(void) const                 { return (TYPE *)IConstPointerArray::tos(); }
    inline TYPE * tos(aindex_t num) const         { return (TYPE *)IConstPointerArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)IConstPointerArray::getArray(pos); }
    inline TYPE **detach()                        { return (TYPE **)IConstPointerArray::detach(); }
    inline void append(TYPE * obj)                { IConstPointerArray::append(obj); }
    inline void appendUniq(TYPE * obj)            { IConstPointerArray::appendUniq(obj); }
    inline void add(TYPE * obj, aindex_t pos)     { IConstPointerArray::add(obj, pos); }
    inline NoBool<aindex_t> find(TYPE * obj) const        { return IConstPointerArray::find(obj); }
    inline void replace(TYPE * obj, aindex_t pos, bool nodel=false) { IConstPointerArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE * obj, bool nodel=false) { return IConstPointerArray::zap(obj, nodel); }
};

template <class TYPE>
class PointerArrayOf : public PointerArray
{
    typedef int (*PointerOfCompareFunc)(TYPE **, TYPE **);
public:
    inline void add(TYPE * x, aindex_t pos)     { PointerArray::add(x, pos); }
    inline void append(TYPE * x)                { PointerArray::append(x); }
    inline aindex_t bAdd(TYPE * & newItem, PointerOfCompareFunc f, bool & isNew) { return PointerArray::bAdd(*(void * *)&newItem, (CompareFunc)f, isNew); }
    inline aindex_t bSearch(const TYPE * & key, PointerOfCompareFunc f) const    { return PointerArray:: bSearch(*(void * const *)&key, (CompareFunc)f); }
    inline NoBool<aindex_t> find(TYPE * x) const        { return PointerArray::find(x); }
    inline TYPE **getArray(aindex_t pos = 0)    { return (TYPE **)PointerArray::getArray(pos); }
    inline TYPE **detach()                      { return (TYPE **)PointerArray::detach(); }
    inline TYPE * item(aindex_t pos) const      { return (TYPE *)PointerArray::item(pos); }
    inline TYPE * popGet()                      { return (TYPE *)PointerArray::popGet(); }
    inline void replace(TYPE * x, aindex_t pos) { PointerArray::replace(x, pos); }
    inline TYPE * tos(void) const               { return (TYPE *)PointerArray::tos(); }
    inline TYPE * tos(aindex_t num) const       { return (TYPE *)PointerArray::tos(num); }
    inline bool zap(TYPE * x)                   { return PointerArray::zap(x); }
};

template <class BTYPE>
class ConstPointerArrayOf : public ConstPointerArray
{
    typedef const BTYPE TYPE;
    typedef int (*PointerOfCompareFunc)(TYPE **, TYPE **);
public:
    inline void add(TYPE * x, aindex_t pos)     { ConstPointerArray::add(x, pos); }
    inline void append(TYPE * x)                { ConstPointerArray::append(x); }
    inline aindex_t bAdd(TYPE * & newItem, PointerOfCompareFunc f, bool & isNew) { return ConstPointerArray::bAdd(*(const void * *)&newItem, (CompareFunc)f, isNew); }
    inline aindex_t bSearch(const TYPE * & key, PointerOfCompareFunc f) const    { return ConstPointerArray:: bSearch(*(const void * const *)&key, (CompareFunc)f); }
    inline NoBool<aindex_t> find(TYPE * x) const        { return ConstPointerArray::find(x); }
    inline TYPE **getArray(aindex_t pos = 0)    { return (TYPE **)ConstPointerArray::getArray(pos); }
    inline TYPE **detach()                      { return (TYPE **)ConstPointerArray::detach(); }
    inline TYPE * item(aindex_t pos) const      { return (TYPE *)ConstPointerArray::item(pos); }
    inline TYPE * popGet()                      { return (TYPE *)ConstPointerArray::popGet(); }
    inline void replace(TYPE * x, aindex_t pos) { ConstPointerArray::replace(x, pos); }
    inline TYPE * tos(void) const               { return (TYPE *)ConstPointerArray::tos(); }
    inline TYPE * tos(aindex_t num) const       { return (TYPE *)ConstPointerArray::tos(num); }
    inline bool zap(TYPE * x)                   { return ConstPointerArray::zap(x); }
};

enum DAFSConnectCfg { SSLNone = 0, SSLOnly, SSLFirst, UnsecureFirst };

#include "jstring.hpp"
#include "jarray.hpp"
#include "jhash.hpp"
#include "jstream.hpp"
#include "jutil.hpp"

template <class ARRAY>
inline void appendArray(ARRAY & target, const ARRAY & source)
{
    unsigned max = source.ordinality();
    if (max)
    {
        target.ensureSpace(max);
        for (unsigned i=0; i < max; ++i)
            target.append(OLINK(source.item(i)));
    }
}

inline void appendArray(IArray & target, const IArray & source)
{
    unsigned max = source.ordinality();
    if (max)
    {
        target.ensureSpace(max);
        for (unsigned i=0; i < max; ++i)
            target.append(OLINK(source.item(i)));
    }
}


typedef bool (*boolFunc)(void);
typedef void (*voidFunc)(void);
typedef HINSTANCE SoContext;
class ModExit;
enum InitializerState { ITS_Uninitialized, ITS_Initialized };
struct InitializerType
{
    boolFunc initFunc;
    ModExit *modExit;
    unsigned int priority;
    unsigned int modpriority;
    SoContext soCtx;
    byte state;

    bool operator == (const InitializerType & other) const { return this == &other; }
};

class jlib_decl InitializerArray : public StructArrayOf<InitializerType> { };

class jlib_decl InitTable
{
public:
    InitializerArray initializers;

    static int sortFuncDescending(InitializerType const *i1, InitializerType const *i2);
    static int sortFuncAscending(InitializerType const *i1, InitializerType const *i2);
public:
    InitTable();
    void init(SoContext soCtx=0);
    void exit(SoContext soCtx=0);
    void add(InitializerType &iT);
    void add(boolFunc func, unsigned int priority, unsigned int modpriority, byte state=ITS_Uninitialized);
    count_t items() { return initializers.ordinality(); }

    InitializerType & element(aindex_t i) { return initializers.element(i); }
};

class jlib_decl ModInit
{
public:
    ModInit(boolFunc func, unsigned int _priority, unsigned int _modprio);
};

class jlib_decl ModExit
{
public:
    ModExit(voidFunc func);
#if !defined(EXPLICIT_INIT)
    ~ModExit();
#endif
    voidFunc func;
};

#ifndef MODULE_PRIORITY
#define MODULE_PRIORITY 1
#endif
#define __glue(a,b) a ## b
#define glue(a,b) __glue(a,b)
#define MODULE_INIT(PRIORITY)                           \
    static bool glue(_modInit,__LINE__) ();         \
    static ModInit glue(modInit, __LINE__) (& glue(_modInit, __LINE__), PRIORITY, MODULE_PRIORITY); \
    static bool glue(_modInit, __LINE__) ()

// Assumes related MODULE_INIT preceded the use of MODULE_EXIT
#define MODULE_EXIT()                                   \
    static void glue(_modExit, __LINE__)();         \
    static ModExit glue(modExit, __LINE__)(& glue(_modExit, __LINE__)); \
    static void glue(_modExit, __LINE__)()
    

extern jlib_decl void _InitModuleObjects();
extern jlib_decl void ExitModuleObjects();
extern jlib_decl void ExitModuleObjects(SoContext soCtx);

#define InitModuleObjects() { _InitModuleObjects(); atexit(&ExitModuleObjects); }
struct DynamicScopeCtx
{
    DynamicScopeCtx();
    ~DynamicScopeCtx();

    void processInitialization(SoContext soCtx);

    InitTable initTable;
};

typedef CIArrayOf<StringAttrItem> StringAttrArray;
typedef CIArrayOf<StringBufferItem> StringBufferArray;

//These could be template definitions, but that would potentially affect too many classes.
#define BITMASK_ENUM(X) \
inline constexpr X operator | (X l, X r) { return (X)((unsigned)l | (unsigned)r); } \
inline constexpr X operator ~ (X l) { return (X)(~(unsigned)l); } \
inline X & operator |= (X & l, X r) { l = l | r; return l; } \
inline X & operator &= (X & l, X r) { l = (X)(l & r); return l; }

#endif
