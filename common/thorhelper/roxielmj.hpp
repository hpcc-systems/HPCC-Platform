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

#ifndef ROXIELMJ_HPP
#define ROXIELMJ_HPP

//Limited Match Join helpers  (s[1..n])

#include "thorhelper.hpp"
#include "roxiehelper.hpp"
#include "roxiemem.hpp"
#include "limits.h"
#include "jqueue.tpp"
#include "thorcommon.hpp"

//===================================================================================
THORHELPER_API IRHLimitedCompareHelper *createRHLimitedCompareHelper();


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
    IRowStream * in;
    bool eos;
public:
    ~CRHRollingCache();
    void init(IRowStream *_in, unsigned _max);
    
#ifdef TRACEROLLING
    void PrintCache();
#endif

    inline CRHRollingCacheElem *mid(int rel);
    void advance();
};

//===================================================================================
//CRHDualCache copied from THOR CDualCache, and modified to get input from IRowStream instead
//of IReadSeqVar and to manage rows as OwnedRoxieRow types
// Could probably be combined with CDualCache now ?

class THORHELPER_API CRHDualCache: public CInterface
{
    // similar to rolling cache - should be combined really
    IRowStream * in;
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
    void init(IRowStream * _in);
    inline IRowStream * input() { return in; }

#ifdef TRACEROLLING
    void PrintCache();
#endif

    bool get(unsigned n, CRHRollingCacheElem *&out);

    class cOut: public CInterface, public IRowStream
    {
    private:
        CRHDualCache *parent;
        bool stopped;
        unsigned &pos;
    public:
        IMPLEMENT_IINTERFACE;
        cOut(CRHDualCache *_parent, unsigned &_pos); 
        const void * nextRow();
    private:
        void stop();
    } *strm1, *strm2;

    IRowStream *queryOut1() { return strm1; }
    IRowStream *queryOut2() { return strm2; }

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
               IRowStream *_in,
               ICompare * _cmp,
               ICompare * _limitedcmp );

    bool getGroup(OwnedRowArray &group,const void *left);

};

#endif // ROXIELMJ_HPP
