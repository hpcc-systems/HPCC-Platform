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

#define LANGUAGE_VERSION_MAJOR      3
#define LANGUAGE_VERSION_MINOR      0
#define LANGUAGE_VERSION_SUB        0

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
    // Object types the GUI can query
    ob_table        = 1,
    ob_dataset      = 2,
    ob_field        = 4,
    ob_assertion    = 8,
    ob_module       = 0x10,
    ob_value        = 0x20,
    ob_declaration  = 0x40,
    ob_record       = 0x80,
    //used in repository    0x100
    //used in repository    0x200
    ob_action       = 0x400,
    ob_error        = 0x800,
    ob_exported     = 0x1000,
    ob_datarow      = 0x2000,
    ob_shared       = 0x4000,
    ob_member       = 0x8000,       // is a member of a module

    ob_sandbox      = 0x10000,
    ob_orphaned     = 0x20000,
    ob_refsandbox   = 0x40000,
    ob_showtext     = 0x80000,
    ob_olderver     = 0x100000,
    ob_refolderver  = 0x200000,
    ob_locked       = 0x400000,
    ob_lockedself   = 0x800000,

    ob_registryflags = 0x7fff0300,
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
    inline ECLlocation() {}
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

interface IEclRepository: public IInterface
{
    virtual IHqlExpression * lookupRootSymbol(IAtom * name, unsigned lookupFlags, HqlLookupContext & ctx) = 0;
    virtual void getRootScopes(HqlScopeArray & rootScopes, HqlLookupContext & ctx) = 0;
    virtual void checkCacheValid() = 0;

//Should only be called and implemented for concrete repositories
    virtual bool loadModule(IHqlRemoteScope * rScope, IErrorReceiver * errs, bool forceAll) = 0;
    virtual IHqlExpression * loadSymbol(IAtom * moduleName, IAtom * attrName) = 0;
};

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #undef new
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

extern bool HQL_API extractVersion(unsigned & major, unsigned & minor, unsigned & sub, const char * version);

#endif

