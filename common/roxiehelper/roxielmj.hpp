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

#ifndef ROXIELMJ_HPP
#define ROXIELMJ_HPP

//Limited Match Join helpers  (s[1..n])

#include "roxiehelper.hpp"
#include "roxiemem.hpp"
#include "limits.h"
#include "jqueue.tpp"
#include "thorcommon.hpp"

#ifdef _WIN32
 #ifdef ROXIEHELPER_EXPORTS
  #define ROXIEHELPER_API __declspec(dllexport)
 #else
  #define ROXIEHELPER_API __declspec(dllimport)
 #endif
#else
 #define ROXIEHELPER_API
#endif

//#pragma message("**** ROXIELMJ.HPP ***")


//===================================================================================
ROXIEHELPER_API IRHLimitedCompareHelper *createRHLimitedCompareHelper();


//===================================================================================
//CRHRollingCacheElem copied/modified from THOR CRHRollingCacheElem
class CRHRollingCacheElem
{
public:
    int cmp;
    const void *row;
    CRHRollingCacheElem();
    ~CRHRollingCacheElem();
    void set(const void *_row);
};


//CRHRollingCache copied/modified from THOR
class CRHRollingCache: extends CInterface
{
    unsigned max; // max cache size
    QueueOf<CRHRollingCacheElem,true> cache;
    IInputBase * in;
    bool eos;
public:
    ~CRHRollingCache();
    void init(IInputBase *_in, unsigned _max);
    
#ifdef TRACEROLLING
    void PrintCache();
#endif

    inline CRHRollingCacheElem *mid(int rel);
    void advance();
};

//===================================================================================
//CRHDualCache copied from THOR CDualCache, and modified to get input from IInputBase instead 
//of IReadSeqVar and to manage rows as OwnedRoxieRow types
class ROXIEHELPER_API CRHDualCache: public CInterface, public IRecordSize
{
    // similar to rolling cache - should be combined really
    IInputBase * in;
    MemoryAttr varbuf;
    bool eos;
    unsigned base;
    unsigned posL;
    unsigned posR;
    QueueOf<CRHRollingCacheElem,true> cache;
public:

    IMPLEMENT_IINTERFACE;

    CRHDualCache();
    ~CRHDualCache();
    void init(IInputBase * _in);
    inline IInputBase * input() {return in;}

#ifdef TRACEROLLING
    void PrintCache();
#endif

    bool get(unsigned n, CRHRollingCacheElem *&out);
    size32_t getRecordSize(const void *ptr);
    size32_t getFixedSize() const;
    class cOut: public CInterface, public IInputBase
    {
    private:
        CRHDualCache *parent;
        bool stopped;
        unsigned &pos;
    public:
        IMPLEMENT_IINTERFACE;
        cOut(CRHDualCache *_parent, unsigned &_pos); 
        const void * nextInGroup();
        IOutputMetaData * queryOutputMeta() const;
    private:
        void stop();
        virtual IRecordSize * queryRecordSize() { return parent; }
    } *strm1, *strm2;

    IInputBase *queryOut1() { return strm1; }
    IInputBase *queryOut2() { return strm2; }

};

//===================================================================================

inline int iabs(int a) { return a<0?-a:a; }
inline int imin(int a,int b) { return a<b?a:b; }

//CRHLimitedCompareHelper
class CRHLimitedCompareHelper: public CInterface, implements IRHLimitedCompareHelper
{
    Owned<CRHRollingCache> cache;
    unsigned atmost;
    ICompare * limitedcmp;
    ICompare * cmp;

public:
    IMPLEMENT_IINTERFACE;

    void init( unsigned _atmost,
               IInputBase *_in,
               ICompare * _cmp,
               ICompare * _limitedcmp );

    bool getGroup(OwnedRowArray &group,const void *left);

};

#endif // ROXIELMJ_HPP
