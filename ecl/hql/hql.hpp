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
#ifndef _HQL_INCL
#define _HQL_INCL
 
#ifdef _WIN32
#ifdef HQL_EXPORTS
#define HQL_API __declspec(dllexport)
#else
#define HQL_API __declspec(dllimport)
#endif
#else
#define HQL_API
#endif
#include "hqlatoms.hpp"
#include "build-config.h"

#define stringify(x) # x
#define estringify(x) stringify(x)

/*
ECL Language version number.  Use to provide a version for the ECL language that is processed.
* Major goes up when we break everything so badly nothing is likely to compile
* Minor goes up when we add new functionality, or increase sub on a new branch
* Sub goes up when we change anything else.

We should always aim to preserve backward compatibility as much as possible.

Relevant changes include
* Changes to the ECL syntax
* Changes to the standard library
* Changes to archive formats etc.
* Changes to the system plugins

*/

#define LANGUAGE_VERSION_MAJOR      BUILD_VERSION_MAJOR
#define LANGUAGE_VERSION_MINOR      BUILD_VERSION_MINOR
#define LANGUAGE_VERSION_SUB        BUILD_VERSION_POINT

#define LANGUAGE_VERSION   estringify(LANGUAGE_VERSION_MAJOR) "." estringify(LANGUAGE_VERSION_MINOR) "." estringify(LANGUAGE_VERSION_SUB)


#define DEFAULT_INT_SIZE 8
#define DEFAULT_REAL_SIZE 8

#undef interface
#ifdef _MSC_VER
#define interface           struct  __declspec(novtable)
#else
#define interface           struct
#endif

typedef const char * user_t;


enum object_type 
{
//Flags set on symbols
    ob_private      = 0x0000,
    ob_exported     = 0x0001,
    ob_shared       = 0x0002,
    ob_import       = 0x0004,
    ob_member       = 0x0008,       // is a member of a module
    ob_virtual      = 0x0010,

//attributes returned from the repository to show the vcs status
    ob_sandbox      = 0x00010000,
    ob_orphaned     = 0x00020000,
    ob_refsandbox   = 0x00040000,
    ob_showtext     = 0x00080000,
    ob_olderver     = 0x00100000,
    ob_refolderver  = 0x00200000,
    ob_locked       = 0x00400000,
    ob_lockedself   = 0x00800000,

    ob_registryflags= 0xffff0000,
};

enum cs_access
{
    cs_none   = 0,    
    cs_access = 1,    
    cs_read   = 2,    
    cs_write  = 4,    
    cs_full   = 0x7fffffff
};


interface IHqlScope;
interface IHqlRemoteScope;
interface IHqlExpression;
interface IErrorReceiver;
interface IAtom;
interface IPropertyTree;
typedef IArrayOf<IHqlScope> HqlScopeArray;
typedef IArrayOf<IHqlRemoteScope> HqlRemoteScopeArray;
class HqlLookupContext;

//This is held in a kept hash table, but normally linked so that could be changed (see note in hqlexpr.cpp for details)
typedef IAtom ISourcePath;

struct HQL_API ECLlocation
{
public:
    inline ECLlocation() : lineno(0), column(0), position(0), sourcePath(NULL) {}
    ECLlocation(const IHqlExpression * _expr) { if (!extractLocationAttr(_expr)) clear(); }
    ECLlocation(int _line, int _column, int _position, ISourcePath * _sourcePath) { set(_line, _column, _position, _sourcePath); }

    inline void clear()
    {
        lineno = 0;
        column = 0;
        position = 0;
        sourcePath = NULL;
    }
    inline void release()
    {
    }
    inline void set(const ECLlocation & _other)
    {
        lineno = _other.lineno;
        column = _other.column;
        position = _other.position;
        sourcePath = _other.sourcePath;
    }
    inline void set(int _line, int _column, int _position, ISourcePath * _sourcePath)
    {
        lineno = _line;
        column = _column;
        position = _position;
        sourcePath = _sourcePath;
    }
    inline bool equals(const ECLlocation & _other) const
    {
        return (lineno == _other.lineno) && 
               (column == _other.column) &&
               (position == _other.position) &&
               (sourcePath == _other.sourcePath);
    }

    IHqlExpression * createLocationAttr() const;
    bool extractLocationAttr(const IHqlExpression * location);
    StringBuffer & getText(StringBuffer & text) const;

    inline ECLlocation & operator = (const ECLlocation & other)
    {
        position = other.position;
        lineno = other.lineno;
        column = other.column;
        sourcePath = other.sourcePath;
        return *this;
    }

public:
//  Linked<ISourcePath> sourcePath;
    ISourcePath * sourcePath;
    int position;
    int lineno;
    int column;
};

interface IFileContents;
interface IEclRepository: public IInterface
{
    virtual IHqlScope * queryRootScope() = 0;
};

//MORE: Make this more private
interface IEclRepositoryCallback : public IEclRepository
{
//Should only be called and implemented for concrete repositories
    virtual bool loadModule(IHqlRemoteScope * rScope, IErrorReceiver * errs, bool forceAll) = 0;
    virtual IHqlExpression * loadSymbol(IHqlRemoteScope *scope, IIdAtom * searchName) = 0;
};

interface ICodegenContextCallback : public IInterface
{
    virtual void noteCluster(const char *clusterName) = 0;
    virtual bool allowAccess(const char * category, bool isSigned) = 0;
};


#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

extern bool HQL_API extractVersion(unsigned & major, unsigned & minor, unsigned & sub, const char * version);

#endif

