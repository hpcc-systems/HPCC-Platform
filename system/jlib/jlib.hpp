/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */



#ifndef JLIB_HPP
#define JLIB_HPP

#include "jexpdef.hpp"

#if !defined(_WIN32)
#define EXPLICIT_INIT
#endif


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

#define loop            for(;;)
#define _elements_in(a) (sizeof(a)/sizeof((a)[0]))
#define _memclr(s, n)      memset(s, 0, n)
#define _clear(a)          memset(&a, 0, sizeof(a))
#define _copy(dest, src)   memcpy(&dest, &src, sizeof(src))


inline IInterface & Array__Member2Param(IInterface * src)              { return *src; }  
inline void Array__Assign(IInterface * & dest, IInterface & src)       { dest = &src; assert(dest); }
inline bool Array__Equal(IInterface * const & m, const IInterface & p) { return m==&p; }
inline void Array__Destroy(IInterface * & next)                        { if (next) next->Release(); }
inline IInterface * Array__Member2ParamPtr(IInterface * src)           { return src; }  
inline void Array__Assign(IInterface * & dest, IInterface * src)       { dest = src; }
inline bool Array__Equal(IInterface * const & m, const IInterface * p) { return m==p; }

inline CInterface & Array__Member2Param(CInterface * src)              { return *src; }  
inline void Array__Assign(CInterface * & dest, CInterface & src)       { dest = &src; assert(dest);}
inline bool Array__Equal(CInterface * const & m, const CInterface & p) { return m==&p; }
inline void Array__Destroy(CInterface * & next)                        { if (next) next->Release(); }

// Used by StringArray
inline const char *Array__Member2Param(const char *src)         { return src; }
inline void Array__Assign(const char * & dest, const char *src) { const char *p = strdup(src); dest=p; }
inline bool Array__Equal(const char *m, const char *p)          { return strcmp(m, p)==0; }
inline void Array__Destroy(const char *p)                       { free((void*) p); }

MAKECopyArrayOf(IInterface *, IInterface &, jlib_decl CopyArray);
MAKEArrayOf(IInterface *, IInterface &, jlib_decl Array);
MAKEPtrArrayOf(IInterface *, IInterface *, jlib_decl PointerIArray);
MAKECopyArrayOf(CInterface *, CInterface &, jlib_decl CopyCIArray);
MAKEArrayOf(CInterface *, CInterface &, jlib_decl CIArray);
MAKEValueArray(char,     jlib_decl CharArray);
MAKEValueArray(int,      jlib_decl IntArray);
MAKEValueArray(short,    jlib_decl ShortArray);
MAKEValueArray(float,    jlib_decl FloatArray);
MAKEValueArray(double,   jlib_decl DoubleArray);
MAKEValueArray(unsigned, jlib_decl UnsignedArray);
MAKEValueArray(unsigned short, jlib_decl UnsignedShortArray);
MAKEValueArray(void *,   jlib_decl PointerArray);
MAKEValueArray(const void *, jlib_decl ConstPointerArray);
MAKEValueArray(unsigned char *,BytePointerArray);
#ifdef _WIN32
MAKEValueArray(bool,     jlib_decl BoolArray);
#else
typedef CharArray BoolArray;
#endif
MAKEValueArray(__int64, jlib_decl Int64Array);
MAKEValueArray(unsigned __int64, jlib_decl UInt64Array);

template <class A, class C, class IITER>
class ArrayIIteratorOf : public CInterface, implements IITER
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
    inline void append(TYPE& obj)                 { assert(&obj); CIArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); CIArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); CIArray::add(obj, pos); } 
    inline aindex_t find(TYPE & obj) const        { assert(&obj); return CIArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); CIArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return CIArray::zap(obj, nodel); }
};

template <class TYPE>
class CopyCIArrayOf : public CopyCIArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)CopyCIArray::item(pos); }
    inline TYPE & pop()                           { return (TYPE &)CopyCIArray::pop(); }
    inline TYPE & tos(void) const                 { return (TYPE &)CopyCIArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)CopyCIArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)CopyCIArray::getArray(pos); }
    inline void append(TYPE& obj)                 { assert(&obj); CopyCIArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); CopyCIArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); CopyCIArray::add(obj, pos); } 
    inline aindex_t find(TYPE & obj) const        { assert(&obj); return CopyCIArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos)  { assert(&obj); CopyCIArray::replace(obj, pos); }
    inline bool zap(TYPE & obj)                   { assert(&obj); return CopyCIArray::zap(obj); }
};

template <class TYPE>
class IArrayOf : public Array
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)Array::item(pos); }
    inline TYPE & popGet()                        { return (TYPE &)Array::popGet(); }
    inline TYPE & tos(void) const                 { return (TYPE &)Array::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)Array::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)Array::getArray(pos); }
    inline void append(TYPE& obj)                 { assert(&obj); Array::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); Array::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); Array::add(obj, pos); } 
    inline aindex_t find(TYPE & obj) const        { assert(&obj); return Array::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos, bool nodel=false) { assert(&obj); Array::replace(obj, pos, nodel); }
    inline bool zap(TYPE & obj, bool nodel=false) { assert(&obj); return Array::zap(obj, nodel); }
};

template <class TYPE>
class ICopyArrayOf : public CopyArray
{
public:
    inline TYPE & item(aindex_t pos) const        { return (TYPE &)CopyArray::item(pos); }
    inline TYPE & pop(bool nodel = 0)             { return (TYPE &)CopyArray::pop(); }
    inline TYPE & tos(void) const                 { return (TYPE &)CopyArray::tos(); }
    inline TYPE & tos(aindex_t num) const         { return (TYPE &)CopyArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)CopyArray::getArray(pos); }
    inline void append(TYPE& obj)                 { assert(&obj); CopyArray::append(obj); } 
    inline void appendUniq(TYPE& obj)             { assert(&obj); CopyArray::appendUniq(obj); } 
    inline void add(TYPE& obj, aindex_t pos)      { assert(&obj); CopyArray::add(obj, pos); } 
    inline aindex_t find(TYPE & obj) const        { assert(&obj); return CopyArray::find(obj); }
    inline void replace(TYPE &obj, aindex_t pos) { assert(&obj); CopyArray::replace(obj, pos); }
    inline bool zap(TYPE & obj)                   { assert(&obj); return CopyArray::zap(obj); }
};

template <class TYPE>
class PointerIArrayOf : public PointerIArray
{
public:
    inline TYPE * item(aindex_t pos) const        { return (TYPE *)PointerIArray::item(pos); }
    inline TYPE * popGet()                        { return (TYPE *)PointerIArray::popGet(); }
    inline TYPE * tos(void) const                 { return (TYPE *)PointerIArray::tos(); }
    inline TYPE * tos(aindex_t num) const         { return (TYPE *)PointerIArray::tos(num); }
    inline TYPE **getArray(aindex_t pos = 0)      { return (TYPE **)PointerIArray::getArray(pos); }
    inline void append(TYPE * obj)                { PointerIArray::append(obj); } 
    inline void appendUniq(TYPE * obj)            { PointerIArray::appendUniq(obj); } 
    inline void add(TYPE * obj, aindex_t pos)     { PointerIArray::add(obj, pos); } 
    inline aindex_t find(TYPE * obj) const        { return PointerIArray::find(obj); }
    inline void replace(TYPE * obj, aindex_t pos, bool nodel=false) { PointerIArray::replace(obj, pos, nodel); }
    inline bool zap(TYPE * obj, bool nodel=false) { return PointerIArray::zap(obj, nodel); }
};

template <class TYPE>
class PointerArrayOf : public PointerArray
{
    typedef int (*PointerOfCompareFunc)(TYPE **, TYPE **);
public:
    inline void add(TYPE * x, aindex_t pos)     { PointerArray::add(x, pos); }
    inline void append(TYPE * x)                { PointerArray::append(x); }
    inline aindex_t bAdd(TYPE * & newItem, PointerOfCompareFunc f, bool & isNew) { return PointerArray::bAdd(*(void * *)&newItem, (CompareFunc)f, isNew); }
    inline aindex_t bSearch(const TYPE * & key, CompareFunc f) const    { return PointerArray:: bSearch(*(const void * *)&key, f); }
    inline aindex_t find(TYPE * x) const        { return PointerArray::find(x); }
    inline TYPE **getArray(aindex_t pos = 0)    { return (TYPE **)PointerArray::getArray(pos); }
    inline TYPE * item(aindex_t pos) const      { return (TYPE *)PointerArray::item(pos); }
    inline TYPE * pop(bool nodel = 0)           { return (TYPE *)PointerArray::pop(nodel); }
    inline void replace(TYPE * x, aindex_t pos) { PointerArray::replace(x, pos); }
    inline TYPE * tos(void) const               { return (TYPE *)PointerArray::tos(); }
    inline TYPE * tos(aindex_t num) const       { return (TYPE *)PointerArray::tos(num); }
    inline bool zap(TYPE * x)                   { return PointerArray::zap(x); }
};

#include "jstring.hpp"
#include "jarray.tpp"
#include "jhash.hpp"
#include "jstream.hpp"
#include "jutil.hpp"

inline void appendArray(CIArray & target, const CIArray & source)
{
    unsigned max = source.ordinality();
    if (max)
    {
        target.ensure(target.ordinality() + max);
        for (unsigned i=0; i < max; ++i)
            target.append(OLINK(source.item(i)));
    }
}

inline void appendArray(Array & target, const Array & source)
{
    unsigned max = source.ordinality();
    if (max)
    {
        target.ensure(target.ordinality() + max);
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
};

inline InitializerType & Array__Member2Param(InitializerType &src)              { return src; }  
inline void Array__Assign(InitializerType & dest, InitializerType & src)       { dest = src; }
inline bool Array__Equal(InitializerType const & m, const InitializerType & p) { return &m==&p; }
inline void Array__Destroy(InitializerType & /*next*/) { }

MAKEArrayOf(InitializerType, InitializerType &, jlib_decl InitializerArray);

class jlib_decl InitTable
{
public:
    InitializerArray initializers;

    static int sortFuncDescending(InitializerType *i1, InitializerType *i2);
    static int sortFuncAscending(InitializerType *i1, InitializerType *i2);
public:
    InitTable();
    void init(SoContext soCtx=0);
    void exit(SoContext soCtx=0);
    void add(InitializerType &iT);
    void add(boolFunc func, unsigned int priority, unsigned int modpriority, byte state=ITS_Uninitialized);
    count_t items() { return initializers.ordinality(); }
    InitializerType &item(aindex_t i) { return initializers.item(i); }
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

// Assumes related MODULE_INIT preceeded the use of MODULE_EXIT
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
    void setSoContext(SoContext _soCtx) { soCtx = _soCtx; }
    SoContext soCtx;
    InitTable initTable;
};

#ifndef USING_MPATROL
#ifdef _WIN32
#ifndef _INC_CRTDBG
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif
#endif
#endif

typedef CIArrayOf<StringAttrItem> StringAttrArray;
class StringBuffer;

#endif
