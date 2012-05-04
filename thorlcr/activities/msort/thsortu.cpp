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


#include "platform.h"
#include "limits.h"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "jqueue.tpp"


#include "jexcept.hpp"
#include "jiface.hpp"
#include "jmisc.hpp"
#include "jio.hpp"
#include "jsem.hpp"
#include "eclhelper.hpp"
#include "thorxmlwrite.hpp"
#include "thexception.hpp"

//#define TRACEROLLING

//#define TEST_PARALLEL_MATCH

#include "thsortu.hpp"

struct CRollingCacheElem
{
    int cmp;
    const void *row;
    CRollingCacheElem()
    {
        row = NULL;
        cmp = INT_MIN; 
    }
    ~CRollingCacheElem()
    {
        if (row)
            ReleaseThorRow(row);
    }
    void set(const void *_row)
    {
        if (row)
            ReleaseThorRow(row);
        row = _row;
    }
};


class CRollingCache: extends CInterface
{
    unsigned max; // max cache size
    QueueOf<CRollingCacheElem,true> cache;
    Linked<IRowStream> in;
    bool eos;
public:
    ~CRollingCache()
    {
        while (cache.ordinality())
        {
            CRollingCacheElem *e = cache.dequeue();
            delete e;
        }
    }

    void init(IRowStream *_in, unsigned _max)
    {
        max = _max;
        in.set(_in);
        cache.clear();
        cache.reserve(max);
        eos = false;
        while (cache.ordinality()<max/2)
            cache.enqueue(NULL);
        while (!eos&&(cache.ordinality()<max))
            advance();
#ifdef TRACEROLLING
        ActPrintLog("CRollingCache::initdone");
#endif
    }

#ifdef TRACEROLLING
    void PrintCache()
    {
        ActPrintLog("================================%s", eos?"EOS":"");
        for (unsigned i = 0;i<max;i++) {
            CRollingCacheElem *e = cache.item(i);
            ActPrintLog("%c%d: %s",(i==max/2)?'>':' ',i,e?(const char *)e->row():"-----");
        }
    }
#endif

    inline CRollingCacheElem *mid(int rel)
    {
        return cache.item(max/2+rel); // relies on unsigned wrap
    }

    void advance()
    {
        CRollingCacheElem *e = (cache.ordinality()==max)?cache.dequeue():NULL;
        if (!eos) {
            if (!e)
                e = new CRollingCacheElem();
            e->set(in->ungroupedNextRow());
            if (e->row) {
                cache.enqueue(e);
#ifdef TRACEROLLING
                PrintCache();
#endif
                return;
            }
            else
                eos = true;
        }
        delete e;
        cache.enqueue(NULL);
#ifdef TRACEROLLING
        PrintCache();
#endif
    }

};

class CDualCache: public CSimpleInterface
{
    // similar to rolling cache - should be combined really
    Linked<IRowStream> in;
    bool eos;
    unsigned base;
    unsigned pos1;
    unsigned pos2;
    QueueOf<CRollingCacheElem,true> cache;
public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDualCache()
    {
        strm1 = NULL;
        strm2 = NULL;
    }

    ~CDualCache()
    {
        ::Release(strm1);
        ::Release(strm2);
        while (cache.ordinality())
        {
            CRollingCacheElem *e = cache.dequeue();
            delete e;
        }
    }


    void init(IRowStream *_in)
    {
        in.set(_in);
        cache.clear();
        eos = false;
#ifdef TRACEROLLING
        ActPrintLog("CDualCache::initdone");
#endif
        base = 0;
        pos1 = 0;
        pos2 = 0;
        strm1 = new cOut(this,pos1);
        strm2 = new cOut(this,pos2) ;
    }

#ifdef TRACEROLLING
    void PrintCache()
    {
        ActPrintLog("================================%s", eos?"EOS":"");
        for (unsigned i = 0;i<max;i++) {
            CRollingCacheElem *e = cache.item(i);
            ActPrintLog("%c%d: %s",(i==max/2)?'>':' ',i,e?(const char *)e->row():"-----");
        }
    }
#endif

    bool get(unsigned n, CRollingCacheElem *&out)
    {
        // take off any no longer needed
        CRollingCacheElem *e=NULL;
        while ((base<pos1)&&(base<pos2)) {
            delete e;
            e = cache.dequeue();
            base++;
        }
        assertex(n>=base);
        while (!eos&&(n-base>=cache.ordinality())) {
            if (!e)
                e = new CRollingCacheElem;
            e->set(in->ungroupedNextRow());
            if (!e->row) {
                eos = true;
                break;
            }
            cache.enqueue(e);
            e = NULL;
#ifdef TRACEROLLING
            PrintCache();
#endif
        }
        delete e;
        if (n-base>=cache.ordinality())
            return false;
        out = cache.item(n-base);
        return true;
    }

    class cOut: public CSimpleInterface, public IRowStream
    {
    private:
        CDualCache *parent;
        bool stopped;
        unsigned &pos;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cOut(CDualCache *_parent, unsigned &_pos) 
            : pos(_pos)
        {
            parent = _parent;
            stopped = false;
        }

        const void *nextRow()
        { 
            CRollingCacheElem *e;
            if (stopped||!parent->get(pos,e)) 
                return NULL;
            LinkThorRow(e->row);
            pos++;
            return e->row;
        }


        void stop()
        {
            pos = (unsigned)-1;
            stopped = true;
        }

    } *strm1, *strm2;

    IRowStream *queryOut1() { return strm1; }
    IRowStream *queryOut2() { return strm2; }
};



#define CATCH_MEMORY_EXCEPTIONS \
catch (IException *e) {     \
  IException *ne = MakeActivityException(&activity, e); \
  ::Release(e); \
  throw ne; \
}


void swapRows(RtlDynamicRowBuilder &row1, RtlDynamicRowBuilder &row2)
{
    row1.swapWith(row2);
}

class CJoinHelper : public IJoinHelper, public CSimpleInterface
{
    CActivityBase &activity;
    ICompare *compareLR;
    ICompare *compareL; 
    ICompare *compareR; 

    ICompare *limitedCompareR;  

    CThorExpandingRowArray rightgroup;
    OwnedConstThorRow prevleft;
    OwnedConstThorRow prevright;            // used for first
    OwnedConstThorRow nextright;
    OwnedConstThorRow nextleft;
    OwnedConstThorRow denormLhs;
    RtlDynamicRowBuilder denormTmp;
    CThorExpandingRowArray denormRows;
    unsigned denormCount;
    size32_t outSz;
    unsigned rightidx;
    enum { JScompare, JSmatch, JSrightgrouponly, JSonfail } state;
    bool eofL;
    bool eofR;
    IHThorJoinArg *helper;
    IOutputMetaData * outputmetaL;
    bool leftouter;
    bool rightouter;
    bool exclude;
    bool firstonlyL;
    bool firstonlyR;
    MemoryBuffer rightgroupmatchedbuf;
    bool *rightgroupmatched;
    bool leftmatched;
    bool rightmatched;
    OwnedConstThorRow defaultLeft;
    OwnedConstThorRow defaultRight;
    Linked<IRowStream> strmL;
    Linked<IRowStream> strmR;
    bool *abort;
    bool nextleftgot;
    bool nextrightgot;
    unsigned atmost;
    rowcount_t lhsProgressCount, rhsProgressCount;
    unsigned keepmax;
    unsigned abortlimit;
    unsigned keepremaining;
    bool betweenjoin;
    Owned<IException> onFailException;
    ThorActivityKind kind;
    activity_id activityId;
    Owned<ILimitedCompareHelper> limitedhelper;
    Owned<CDualCache> dualcache;
    Linked<IEngineRowAllocator> allocator;
    IMulticoreIntercept* mcoreintercept;
    Linked<IEngineRowAllocator> allocatorL;
    Linked<IEngineRowAllocator> allocatorR;

    struct cCompareLR: public ICompare
    {
        ICompare *upper;
        ICompare *lower;
        int docompare(const void *l,const void *r) const
        {
            int cmp = upper->docompare(l,r);
            if (cmp<=0) {
                cmp = lower->docompare(l,r);
                if (cmp>0)
                    cmp = 0;    // in range
            }
            return cmp;
        }
    } btwcompLR;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CJoinHelper(CActivityBase &_activity, IHThorJoinArg *_helper, IEngineRowAllocator *_allocator)
        : activity(_activity), allocator(_allocator), denormTmp(NULL), rightgroup(_activity), denormRows(_activity)
    {
        kind = activity.queryContainer().getKind();
        helper = _helper; 
        denormCount = 0;
        outSz = 0;
        lhsProgressCount = rhsProgressCount = 0;
        keepmax = (unsigned)-1;
        abortlimit = (unsigned)-1;
        keepremaining = keepmax;
        outputmetaL = NULL;
        limitedCompareR = NULL;
        nextleftgot = false;
        nextrightgot = false;
        mcoreintercept = NULL;
    }

    ~CJoinHelper()
    {
        strmL.clear();
        strmR.clear();
        limitedhelper.clear();
    }

    bool init(
            IRowStream *_strmL,
            IRowStream *_strmR,
            IEngineRowAllocator *_allocatorL,
            IEngineRowAllocator *_allocatorR,
            IOutputMetaData * _outputmeta,
            bool *_abort,
            IMulticoreIntercept *_mcoreintercept)
    {
        //DebugBreak();

        assertex(_allocatorL);
        assertex(_allocatorR);
        mcoreintercept = _mcoreintercept;
        eofL = false;
        eofR = false;
        if (!_strmR) { // (limited) self join
           dualcache.setown(new CDualCache());
           dualcache->init(_strmL);
           strmL.set(dualcache->queryOut1());
           strmR.set(dualcache->queryOut2());
        }
        else {
            strmL.set(_strmL);
            strmR.set(_strmR);
        }
        allocatorL.set(_allocatorL);
        allocatorR.set(_allocatorR);
        state = JScompare;
        nextleftgot = false;
        nextrightgot = false;
        unsigned flags = helper->getJoinFlags();
        betweenjoin = (flags&JFslidingmatch)!=0;
        if (betweenjoin) {
            assertex(_strmR); // betweenjoin and limited not allowed!
            btwcompLR.lower = helper->queryCompareLeftRightLower();
            btwcompLR.upper = helper->queryCompareLeftRightUpper();
            compareLR = &btwcompLR;
        }
        else 
            compareLR = helper->queryCompareLeftRight();
        compareL = helper->queryCompareLeft();
        compareR = helper->queryCompareRight();
        if ((flags&JFlimitedprefixjoin)&&(helper->getJoinLimit())) {
            limitedhelper.setown(createLimitedCompareHelper());
            limitedhelper->init(helper->getJoinLimit(),strmR,compareLR,helper->queryPrefixCompare());
        }
        leftouter = (flags & JFleftouter) != 0;
        rightouter = (flags & JFrightouter) != 0;
        exclude = (flags & JFexclude) != 0;
        firstonlyL = false;
        firstonlyR = false;
        if (flags & JFfirst) {   
            firstonlyL = true;
            firstonlyR = true;
            assertex(!leftouter);
            assertex(!rightouter);
        }
        else {
            if (flags & JFfirstleft) {
                assertex(!leftouter);
                firstonlyL = true;
            }
            if (flags & JFfirstright) {
                assertex(!rightouter);
                firstonlyR = true;
            }
        }
        if (rightouter) {
            RtlDynamicRowBuilder r(allocatorL);
            size32_t sz = helper->createDefaultLeft(r);
            defaultLeft.setown(r.finalizeRowClear(sz));
        }
        if (leftouter || JFonfail & helper->getJoinFlags()) {
            RtlDynamicRowBuilder r(allocatorR);
            size32_t sz = helper->createDefaultRight(r);
            defaultRight.setown(r.finalizeRowClear(sz));
        }
        abort = _abort;
        atmost = helper->getJoinLimit();
        if (atmost)
            assertex(!rightouter);
        else
            atmost = (unsigned)-1;
        keepmax = helper->getKeepLimit();
        if (!keepmax)
            keepmax = (unsigned)-1;
        abortlimit = helper->getMatchAbortLimit();
        if (!abortlimit)
            abortlimit = (unsigned)-1;
        keepremaining = keepmax;
        outputmetaL = _outputmeta;
        if (TAKdenormalize == kind || TAKhashdenormalize == kind)
            denormTmp.setAllocator(allocator).ensureRow();
        return true;
    }

    bool getL()
    {
        if (nextleftgot)
            return true;
        keepremaining = keepmax;
        leftmatched = !leftouter;
        resetDenorm();
        prevleft.setown(nextleft.getClear());
        if (!eofL) {
            loop {
                nextleft.setown(strmL->nextRow());
                if (!nextleft) 
                    break;
                lhsProgressCount++;
                if (!firstonlyL || (lhsProgressCount==1) || (compareL->docompare(prevleft,nextleft)!=0)) {
                    denormLhs.set(nextleft.get());
                    nextleftgot = true;
                    return true;
                }
            }
            eofL = true;
        }
        return false;
    }

    bool getR()
    {
        if (nextrightgot)
            return true;
        rightmatched = !rightouter;
        prevright.setown(nextright.getClear());
        if (!eofR) {
            loop {
                nextright.setown(strmR->nextRow());
                if (!nextright) 
                    break;
                rhsProgressCount++;
                if (!firstonlyR || (rhsProgressCount==1) || (compareR->docompare(prevright,nextright)!=0)) {
                    nextrightgot = true;
                    return true;
                }
            }
            eofR = true;
        }
        return false;
    }

    inline void nextL()
    {
        nextleftgot = false;
    }

    inline void nextR()
    {
        nextrightgot = false;
    }

    enum Otype { Onext, Ogroup, Oouter };

    const void * outrow(Otype l,Otype r)
    {
        OwnedConstThorRow fret; // used if passing back a prefinalized row
        RtlDynamicRowBuilder ret(allocator);
        // this routine does quite a lot including advancing on outer
        assertex(l!=Ogroup);    // left group no longer present (asymmetry)
        size32_t gotsz = 0;
        bool denormGot = false;
        if (l==Oouter) {
            assertex(r!=Oouter);
            if (r==Onext) {
                if (!rightmatched) {
                    switch (kind) {
                        case TAKdenormalize:
                        case TAKhashdenormalize:
                        {
                            const void *lhs = defaultLeft;
                            do {
                                gotsz = helper->transform(denormTmp, lhs, nextright, ++denormCount);
                                if (gotsz) {
                                    swapRows(denormTmp, ret);
                                    lhs = (const void *)ret.getSelf();
                                }
                                nextR();
                            }
                            while (getR()&&(0 == compareR->docompare(prevright,nextright)));
                            denormCount = 0;
                            break;
                        }
                        case TAKdenormalizegroup:
                        case TAKhashdenormalizegroup:
                            assertex(!denormRows.ordinality());
                            do {
                                denormRows.append(nextright.getLink());
                                nextR();
                            }
                            while (getR()&&(0 == compareR->docompare(prevright,nextright)));
                            gotsz = helper->transform(ret, defaultLeft, denormRows.query(0), denormRows.ordinality(), denormRows.getRowArray());
                            denormRows.kill();
                            break;
                        case TAKjoin:
                        case TAKselfjoin:
                            gotsz = helper->transform(ret, defaultLeft, nextright);
                            nextR();
                            break;
                        default:
                            throwUnexpected();
                    }
                }
                else
                    nextR();
            }
            else {
                switch (kind) {
                    case TAKdenormalize:
                    case TAKhashdenormalize:
                    {
                        const void *lhs = defaultLeft;
                        do {
                            if (!rightgroupmatched[rightidx]) {
                                gotsz = helper->transform(denormTmp, lhs, rightgroup.query(rightidx), ++denormCount);
                                if (gotsz) {
                                    swapRows(denormTmp, ret);
                                    lhs = (const void *)ret.getSelf();
                                }
                            }
                            ++rightidx;
                        }
                        while (rightidx<rightgroup.ordinality());
                        denormCount = 0;
                        break;
                    }
                    case TAKdenormalizegroup:
                    case TAKhashdenormalizegroup:

                        assertex(!denormRows.ordinality());
                        do {
                            if (!rightgroupmatched[rightidx])
                                denormRows.append(rightgroup.getClear(rightidx));
                            ++rightidx;
                        }
                        while (rightidx<rightgroup.ordinality());
                        if (denormRows.ordinality())
                        {
                            gotsz = helper->transform(ret, defaultLeft, denormRows.query(0), denormRows.ordinality(), denormRows.getRowArray());
                            denormRows.kill();
                        }
                        denormCount = 0;
                        break;
                    case TAKjoin:
                    case TAKselfjoin:
                        if (!rightgroupmatched[rightidx]) 
                            gotsz = helper->transform(ret, defaultLeft, rightgroup.query(rightidx));
                        rightidx++;
                        break;
                    default:
                        throwUnexpected();
                }
            }
        }
        else if (r==Oouter) {
            if (!leftmatched) 
            {
                switch (kind) {
                    case TAKdenormalize:
                    case TAKhashdenormalize:
                        fret.set(nextleft);
                        break;
                    case TAKdenormalizegroup:
                    case TAKhashdenormalizegroup:
                        gotsz = helper->transform(ret, nextleft, NULL, 0, (const void **)NULL);
                        break;
                    case TAKjoin:
                    case TAKselfjoin:
                        gotsz = helper->transform(ret, nextleft, defaultRight);
                        break;
                    default:
                        throwUnexpected();
                }
            }
            else
            {
                // output group if needed before advancing
                if ((TAKdenormalize == kind || TAKhashdenormalize == kind) && outSz)
                    fret.setown(denormLhs.getClear()); // denormLhs holding transform progress
                else if ((TAKdenormalizegroup == kind || TAKhashdenormalizegroup == kind) && denormRows.ordinality())
                {
                    gotsz = helper->transform(ret, nextleft, denormRows.query(0), denormRows.ordinality(), denormRows.getRowArray());
                    denormRows.kill();
                }
            }
            nextL();            // output outer once
        }
        else { // not outer
            if (keepremaining==0)
                return NULL;                            //  treat KEEP expiring as match fail
            if (r==Onext) {
                // JCSMORE - I can't see when this can happen? if r==Onext, l is always Oouter.
                if (!exclude) 
                    gotsz = helper->transform(ret,nextleft,nextright);
                rightmatched = true;
            }
            else {
                if (!exclude)
                {
                    switch (kind) {
                        case TAKdenormalize:
                        case TAKhashdenormalize:
                        {
                            size32_t sz = helper->transform(ret, denormLhs, rightgroup.query(rightidx), ++denormCount);
                            if (sz)
                            {
                                denormLhs.setown(ret.finalizeRowClear(sz));
                                outSz = sz; // have feeling could use denormGot and reset it.
                                denormGot = true;
                            }
                            break;
                        }
                        case TAKdenormalizegroup:
                        case TAKhashdenormalizegroup:
                        {
                            const void *rhsRow = rightgroup.query(rightidx);
                            LinkThorRow(rhsRow);
                            denormRows.append(rhsRow);
                            denormGot = true;
                            break;
                        }
                        case TAKjoin:
                        case TAKselfjoin:
                            gotsz = helper->transform(ret,nextleft,rightgroup.query(rightidx));
                            break;
                        default:
                            throwUnexpected();
                    }
                }
                rightgroupmatched[rightidx] = true;
            }
            if ((gotsz||denormGot||fret.get())&&(keepremaining!=(unsigned)-1))
                keepremaining--;
            // treat SKIP and exclude as match success
            leftmatched = true;
        }
        if (gotsz)
            return ret.finalizeRowClear(gotsz);
        if (fret)
            return fret.getClear();
        return NULL;
    }
    void resetDenorm()
    {
        switch (kind)
        {
        case TAKdenormalizegroup:
        case TAKhashdenormalizegroup:
            denormRows.kill(); // fall through
        case TAKdenormalize:
        case TAKhashdenormalize:
            outSz = 0;
            denormLhs.clear();
            denormCount = 0;
        default:
            break;
        }
    }
    const void *nextRow()
    {
        OwnedConstThorRow ret;
        RtlDynamicRowBuilder failret(allocator, false);
        try {
    retry:
            ret.clear();
            do {
                if (*abort) 
                    return NULL;
                switch (state) {
                case JSonfail:
                    do
                    {
                        size32_t transformedSize = helper->onFailTransform(failret.ensureRow(), nextleft, defaultRight, onFailException.get());
                        nextL();
                        if (!getL()||0!=compareL->docompare(nextleft,prevleft))
                            state = JScompare;
                        if (transformedSize) {
                            if (mcoreintercept) {
                                mcoreintercept->addRow(failret.finalizeRowClear(transformedSize));
                                goto retry;
                            }
                            return failret.finalizeRowClear(transformedSize);
                        }
                    }
                    while (state == JSonfail);
                    // fall through
                case JScompare:                         
                    if (getL()) {
                        rightidx = 0;
                        rightgroupmatched = NULL;
                        if (betweenjoin) {
                            unsigned nr = 0;
                            while ((nr<rightgroup.ordinality())&&(btwcompLR.upper->docompare(nextleft,rightgroup.query(nr))>0))
                                nr++;
                            rightgroup.removeRows(0,nr);
                            rightgroupmatched = (bool *)rightgroupmatchedbuf.clear().reserve(rightgroup.ordinality());
                            memset(rightgroupmatched,rightmatched?1:0,rightgroup.ordinality());
                        }
                        else
                            rightgroup.kill();

                        // now add new
                        bool hitatmost=false;
                        int cmp = -1;
                        // now load the right group
                        if (limitedhelper) {
                            limitedhelper->getGroup(rightgroup,nextleft);
                            if (rightgroup.ordinality()) {
                                state = JSmatch;
                                rightgroupmatched = (bool *)rightgroupmatchedbuf.clear().reserve(rightgroup.ordinality());
                                memset(rightgroupmatched,1,rightgroup.ordinality()); // no outer
                            }
                            else 
                                ret.setown(outrow(Onext,Oouter)); // out left outer and advance left
                        }
                        else {
                            while (getR()) {
                                cmp = compareLR->docompare(nextleft,nextright);
                                if (cmp!=0)
                                    break;
                                if (rightgroup.ordinality()==abortlimit) {
                                    if ((helper->getJoinFlags()&JFmatchAbortLimitSkips)==0) {
                                        try
                                        {
                                            helper->onMatchAbortLimitExceeded();
                                            CommonXmlWriter xmlwrite(0);
                                            if (outputmetaL && outputmetaL->hasXML()) {
                                                outputmetaL->toXML((const byte *) nextleft.get(), xmlwrite);
                                            }
                                            throw MakeStringException(0, "More than %d match candidates in join for row %s", abortlimit, xmlwrite.str());
                                        }
                                        catch (IException *_e)
                                        {
                                            if (0 == (JFonfail & helper->getJoinFlags()))
                                                throw;
                                            onFailException.setown(_e);
                                        }
                                        do nextR(); while( getR()&&(compareLR->docompare(nextleft,nextright)==0));
                                        state = JSonfail;
                                        goto retry;
                                    }
                                    do nextR(); while( getR()&&(compareLR->docompare(nextleft,nextright)==0));
                                    // discard lhs row(s) (yes, even if it is an outer join)
                                    do { 
                                        nextL();
                                    } while(getL()&&(compareL->docompare(nextleft,prevleft)==0));
                                    goto retry;
                                }
                                else if (rightgroup.ordinality()==atmost) {
                                    do nextR(); while( getR()&&(compareLR->docompare(nextleft,nextright)==0));
                                    hitatmost = true;
                                    cmp = -1;
                                    break;
                                }
                                rightgroup.append(nextright.getClear());
                                nextR();
                            }
                            rightgroupmatched = (bool *)rightgroupmatchedbuf.clear().reserve(rightgroup.ordinality());
                            memset(rightgroupmatched,rightmatched?1:0,rightgroup.ordinality());
                            if (!hitatmost&&rightgroup.ordinality())
                                state = JSmatch;
                            else if (cmp<0)
                                ret.setown(outrow(Onext,Oouter));
                            else 
                                ret.setown(outrow(Oouter,Onext));
                        }

                    }
                    else if (getR()) 
                        ret.setown(outrow(Oouter,Onext));
                    else
                        return NULL;
                    break;
                case JSmatch: // matching left to right group       
                    if (mcoreintercept) {
                        CThorExpandingRowArray leftgroup(activity);
                        while (getL()) {
                            if (leftgroup.ordinality()) {
                                int cmp = compareL->docompare(nextleft,leftgroup.query(leftgroup.ordinality()-1));
                                if (cmp!=0)
                                    break;
                            }
                            leftgroup.append(nextleft.getClear());
                            nextL();
                        }
                        mcoreintercept->addWork(&leftgroup,&rightgroup);
                        state = JScompare;
                    }
                    else if (rightidx<rightgroup.ordinality()) {
                        if (helper->match(nextleft,rightgroup.query(rightidx)))
                            ret.setown(outrow(Onext,Ogroup));
                        rightidx++;
                    }
                    else { // all done
                        ret.setown(outrow(Onext,Oouter));
                        rightidx = 0;
                        if (getL()) {
                            int cmp = compareL->docompare(nextleft,prevleft);
                            if (cmp>0) 
                                state = JSrightgrouponly;
                            else if (cmp<0) 
                                throw MakeStringException(-1,"JOIN LHS not in sorted order");
                        }
                        else
                            state = JSrightgrouponly;
                    }
                    break;
                case JSrightgrouponly: 
                    // right group
                    if (rightidx<rightgroup.ordinality())
                        ret.setown(outrow(Oouter,Ogroup));
                    else  // all done
                        state = JScompare;
                    break;
                }
                if (mcoreintercept&&ret.get()) 
                    mcoreintercept->addRow(ret.getClear());
                
            } while (!ret.get());

        }
        CATCH_MEMORY_EXCEPTIONS
        return ret.getClear();;
    }
    virtual rowcount_t getLhsProgress() const { return lhsProgressCount; }
    virtual rowcount_t getRhsProgress() const { return rhsProgressCount; }
};

class SelfJoinHelper: public IJoinHelper, public CSimpleInterface
{
    CActivityBase &activity;
    ICompare *compare;
    CThorExpandingRowArray curgroup;
    unsigned leftidx;
    unsigned rightidx;
    bool leftmatched;
    MemoryBuffer rightmatchedbuf;
    bool *rightmatched;
    enum { JSload, JSmatch, JSleftonly, JSrightonly, JSonfail } state;
    bool eof;
    IHThorJoinArg *helper;
    IOutputMetaData * outputmetaL;
    bool leftouter;
    bool rightouter;
    bool exclude;
    bool firstonlyL;
    bool firstonlyR;
    OwnedConstThorRow defaultLeft;
    OwnedConstThorRow defaultRight;
    Owned<IRowStream> strm;
    bool *abort;
    unsigned atmost;
    rowcount_t progressCount;
    unsigned keepmax;
    unsigned abortlimit;
    unsigned keepremaining;
    OwnedConstThorRow nextrow;
    Owned<IException> onFailException;
    activity_id activityId;
    Linked<IEngineRowAllocator> allocator;
    Linked<IEngineRowAllocator> allocatorin;
    IMulticoreIntercept* mcoreintercept;
    
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SelfJoinHelper(CActivityBase &_activity, IHThorJoinArg *_helper, IEngineRowAllocator *_allocator)
        : activity(_activity), allocator(_allocator), curgroup(_activity)
    {
        helper = _helper;       
        outputmetaL = NULL;
        mcoreintercept = NULL;
    }

    bool init(
            IRowStream *_strm,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *_allocatorL,
            IEngineRowAllocator *,
            IOutputMetaData * _outputmeta,
            bool *_abort,
            IMulticoreIntercept *_mcoreintercept)
    {
        //DebugBreak();
        assertex(_allocatorL);
        mcoreintercept = _mcoreintercept;
        eof = false;
        strm.set(_strm);
        assertex(strmR==NULL);
        allocatorin.set(_allocatorL);
        state = JSload;
        unsigned flags = helper->getJoinFlags();
        assertex((flags&JFslidingmatch)==0);
        compare = helper->queryCompareLeft();
        leftouter = (flags & JFleftouter) != 0;
        rightouter = (flags & JFrightouter) != 0;
        exclude = (flags & JFexclude) != 0;
        firstonlyL = false; // I think first is depreciated but support anyway
        firstonlyR = false;
        if (flags & JFfirst) {    
            firstonlyL = true;
            firstonlyR = true;
            assertex(!leftouter);
            assertex(!rightouter);
        }
        else {
            if (flags & JFfirstleft) {
                assertex(!leftouter);
                firstonlyL = true;
            }
            if (flags & JFfirstright) {
                assertex(!rightouter);
                firstonlyR = true;
            }
        }
        if (rightouter) {
            RtlDynamicRowBuilder r(allocatorin);
            size32_t sz =helper->createDefaultLeft(r);
            defaultLeft.setown(r.finalizeRowClear(sz));
        }
        if (leftouter || JFonfail & helper->getJoinFlags()) {
            RtlDynamicRowBuilder r(allocatorin);
            size32_t sz = helper->createDefaultRight(r);
            defaultRight.setown(r.finalizeRowClear(sz));
        }
        abort = _abort;
        atmost = helper->getJoinLimit();
        if (atmost)
            assertex(!rightouter);
        else
            atmost = (unsigned)-1;
        keepmax = helper->getKeepLimit();
        if (!keepmax)
            keepmax = (unsigned)-1;
        abortlimit = helper->getMatchAbortLimit();
        if (!abortlimit)
            abortlimit = (unsigned)-1;
        keepremaining = keepmax;
        outputmetaL = _outputmeta;
        progressCount = 0;
        return true;
    }

    inline bool getRow()
    {
        if (!nextrow) {
            nextrow.setown(strm->nextRow());
            if (!nextrow)
                return false;
            progressCount++;
        }
        return true;
    }

    inline void next()
    {
        nextrow.clear();
    }

    const void *nextRow()
    {
        OwnedConstThorRow ret;
        RtlDynamicRowBuilder failret(allocator, false);
        try {
retry:
            ret.clear();
            do {
                if (*abort) 
                    return NULL;
                switch (state) {
                case JSonfail:
                    if (leftidx<curgroup.ordinality()) {
                        size32_t transformedSize = helper->onFailTransform(failret.ensureRow(), curgroup.query(leftidx), defaultRight, onFailException.get());
                        leftidx++;
                        if (transformedSize) {
                            if (mcoreintercept) {
                                mcoreintercept->addRow(failret.finalizeRowClear(transformedSize));
                                goto retry;
                            }
                            return failret.finalizeRowClear(transformedSize);
                        }
                        break;
                    }
                    else if (getRow() && (compare->docompare(nextrow,curgroup.query(0))==0)) {
                        size32_t transformedSize = helper->onFailTransform(failret, nextrow, defaultRight, onFailException.get());
                        next();
                        if (transformedSize) {
                            if (mcoreintercept) {
                                mcoreintercept->addRow(failret.finalizeRowClear(transformedSize));
                                goto retry;
                            }
                            ret.setown(failret.finalizeRowClear(transformedSize));
                        }
                        break;
                    }
                    else  // all done
                        state = JSload;
                    // fall through
                case JSload:                            
                    // fill group
                    curgroup.kill();
                    rightmatchedbuf.clear();
                    rightmatched = NULL;
                    leftmatched = false;
                    keepremaining = keepmax;
                    if (eof) 
                        return NULL;
                    unsigned ng;
                    while (getRow()&&(((ng=curgroup.ordinality())==0)||(compare->docompare(nextrow,curgroup.query(0))==0))) {
                        if ((ng==abortlimit)||(ng==atmost)) {
                            if ((ng==abortlimit)&&((helper->getJoinFlags()&JFmatchAbortLimitSkips)==0)) {
                                // abort
                                try
                                {
                                    helper->onMatchAbortLimitExceeded();
                                    CommonXmlWriter xmlwrite(0);
                                    if (outputmetaL && outputmetaL->hasXML()) {
                                        outputmetaL->toXML((const byte *) nextrow.get(), xmlwrite);
                                    }
                                    throw MakeStringException(0, "More than %d match candidates in join for row %s", abortlimit, xmlwrite.str());
                                }
                                catch (IException *_e)
                                {
                                    if (0 == (JFonfail & helper->getJoinFlags()))
                                    {
                                        curgroup.kill();
                                        throw;
                                    }
                                    onFailException.setown(_e);
                                }
                                state = JSonfail;
                                break;
                            }
                            if ((ng!=abortlimit)&&leftouter) {
                                state = JSleftonly ; // rest get done as left outer 
                                break;
                            }
                            // throw away group
                            do { // skip group
                                next();
                            } while (getRow() && (compare->docompare(nextrow,curgroup.query(0))==0));
                            curgroup.kill();
                            rightmatchedbuf.clear();
                            eof = !nextrow.get();
                            goto retry;
                        }
                        if (!firstonlyR||!firstonlyL||(ng==0)) 
                            curgroup.append(nextrow.getClear());
                        next();
                    }
                    if (curgroup.ordinality()==0) {
                        eof = 0;
                        return NULL;
                    }
                    if (curgroup.ordinality() > INITIAL_SELFJOIN_MATCH_WARNING_LEVEL) {
                        Owned<IThorException> e = MakeActivityWarning(&activity, TE_SelfJoinMatchWarning, "Exceeded initial match limit");
                        e->setAction(tea_warning);
                        e->queryData().append((unsigned)curgroup.ordinality());
                        activity.fireException(e);
                    }
                    leftidx = 0;
                    rightidx = 0;
                    leftmatched = false;
                    if (state==JSload) {     // catch atmost above
                        rightmatched = (bool *)rightmatchedbuf.clear().reserve(curgroup.ordinality());
                        memset(rightmatched,rightouter?0:1,curgroup.ordinality());
                        state = JSmatch; // ok we have group so match
                    }
                    break;
                case JSmatch: {
                        const void *l = curgroup.query(leftidx); // leftidx should be in range here
                        if (mcoreintercept) {
                            mcoreintercept->addWork(&curgroup,NULL);
                            state = JSload;
                        }
                        else if ((rightidx<curgroup.ordinality())&&(!firstonlyR||(rightidx==0))) {
                            const void *r = curgroup.query(rightidx);
                            if (helper->match(l,r)) {
                                if (keepremaining>0) {
                                    if (!exclude) {
                                        RtlDynamicRowBuilder rtmp(allocator);
                                        size32_t sz = helper->transform(rtmp,l,r);
                                        if (sz)
                                            ret.setown(rtmp.finalizeRowClear(sz));
                                    }
                                    if (ret.get()&&(keepremaining!=(unsigned)-1))
                                        keepremaining--;
                                    // treat SKIP and exclude as match success
                                    if (rightouter)
                                        rightmatched[rightidx] = true;
                                    leftmatched = true;
                                }
                                else
                                    rightidx = curgroup.ordinality()-1;
                            }
                            rightidx++;
                        }
                        else { // right all done 
                            if (leftouter&&!leftmatched) {
                                RtlDynamicRowBuilder rtmp(allocator);
                                size32_t sz = helper->transform(rtmp, l, defaultRight);
                                if (sz)
                                    ret.setown(rtmp.finalizeRowClear(sz));
                            }
                            keepremaining = keepmax; // lefts don't count in keep
                            rightidx = 0;
                            leftidx++;
                            if ((leftidx>=curgroup.ordinality())||(firstonlyL&&(leftidx>0)))
                                state = JSrightonly;
                            else
                                leftmatched = false;
                        }
                    }
                    break;
                case JSleftonly: 
                    // must be left outer after atmost to get here
                    if (leftidx<curgroup.ordinality()) {
                        RtlDynamicRowBuilder rtmp(allocator);
                        size32_t sz = helper->transform(rtmp, curgroup.query(leftidx), defaultRight);
                        if (sz)
                            ret.setown(rtmp.finalizeRowClear(sz));
                        leftidx++;
                    }
                    else if (getRow() && (compare->docompare(nextrow,curgroup.query(0))==0)) {
                        RtlDynamicRowBuilder rtmp(allocator);
                        size32_t sz = helper->transform(rtmp, nextrow, defaultRight);
                        if (sz)
                            ret.setown(rtmp.finalizeRowClear(sz));
                        next();
                    }
                    else  // all done
                        state = JSload;
                    break;
                case JSrightonly: 
                    // right group
                    if (rightouter&&(rightidx<curgroup.ordinality())) {
                        if (!rightmatched[rightidx]) {
                            RtlDynamicRowBuilder rtmp(allocator);
                            size32_t sz = helper->transform(rtmp, defaultLeft,curgroup.query(rightidx));
                            if (sz)
                                ret.setown(rtmp.finalizeRowClear(sz));
                        }
                        rightidx++;
                    }
                    else  // all done
                        state = JSload;
                    break;
                }
                if (mcoreintercept&&ret.get()) 
                    mcoreintercept->addRow(ret.getClear());
            } while (!ret.get());

        }
        CATCH_MEMORY_EXCEPTIONS
        return ret.getClear();
    }
    virtual rowcount_t getLhsProgress() const { return progressCount; }
    virtual rowcount_t getRhsProgress() const { return progressCount; }
};

IJoinHelper *createDenormalizeHelper(CActivityBase &activity, IHThorDenormalizeArg *helper, IEngineRowAllocator *allocator)
{
    return new CJoinHelper(activity, helper, allocator);
}









inline int iabs(int a) { return a<0?-a:a; }
inline int imin(int a,int b) { return a<b?a:b; }

class CLimitedCompareHelper: public CSimpleInterface, implements ILimitedCompareHelper
{

    Owned<CRollingCache> cache;
    unsigned atmost;
    ICompare * limitedcmp;
    ICompare * cmp;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    void init( unsigned _atmost,
               IRowStream *_in,
               ICompare * _cmp,
               ICompare * _limitedcmp)
    {
        atmost = _atmost;
        cache.setown(new CRollingCache());
        cache->init(_in,(atmost+1)*2);
        cmp = _cmp;
        limitedcmp = _limitedcmp;
    }


    bool getGroup(CThorExpandingRowArray &group,const void *left)
    {
        // this could be improved!


        // first move 'mid' forwards until mid>=left
        int low = 0;
        loop {
            CRollingCacheElem * r = cache->mid(0);
            if (!r)
                break; // kit eos              
            int c = cmp->docompare(left,r->row);
            if (c==0) {
                r->cmp = limitedcmp->docompare(left,r->row);
                if (r->cmp<=0) 
                    break;
            }
            else if (c<0) {
                r->cmp = -1;
                break;
            }
            else
                r->cmp = 1;
            cache->advance();
            if (cache->mid(low-1))  // only if haven't hit start
                low--;
        }
        // now scan back (note low should be filled even at eos)
        loop {
            CRollingCacheElem * pr = cache->mid(low-1);
            if (!pr)
                break; // hit start 
            int c = cmp->docompare(left,pr->row);
            if (c==0) {
                pr->cmp = limitedcmp->docompare(left,pr->row);
                if (pr->cmp==1) 
                    break;
            }
            else {
                pr->cmp = 1;  
                break;
            }
            low--;
        }
        int high = 0;
        if (cache->mid(0)) { // check haven't already hit end
            // now scan fwd
            loop {
                high++;
                CRollingCacheElem * nr = cache->mid(high);
                if (!nr) 
                    break;
                int c = cmp->docompare(left,nr->row);
                if (c==0) {
                    nr->cmp = limitedcmp->docompare(left,nr->row);
                    if (nr->cmp==-1) 
                        break;
                }
                else {
                    nr->cmp = -1;
                    break;
                }
            }
        }
        while (high-low>((int)atmost)) {
            CRollingCacheElem *rl = cache->mid(low);
            CRollingCacheElem *rh = cache->mid(high-1);
            int vl = iabs(cache->mid(low)->cmp);
            int vh = iabs(cache->mid(high-1)->cmp);
            int v;
            if (vl==0) {
                if (vh==0)  // both ends equal
                    return false;
                v = vh;
            }
            else if (vh==0)
                v = vl;
            else
                v = imin(vl,vh);
            // remove worst match from either end
            while ((low<high)&&(iabs(cache->mid(low)->cmp)==v))
                low++;
            while ((low<high)&&(iabs(cache->mid(high-1)->cmp)==v))
                high--;
            if (low>=high) 
                return true;   // couldn't make group;
        }
        for (int i=low;i<high;i++) {
            CRollingCacheElem *r = cache->mid(i);
            LinkThorRow(r->row);
            group.append(r->row);   
        }
        return group.ordinality()>0;
    }
};


ILimitedCompareHelper *createLimitedCompareHelper()
{
    return new CLimitedCompareHelper();
}

//===============================================================

class CMultiCoreJoinHelperBase: extends CInterface, implements IJoinHelper, implements IMulticoreIntercept
{
public:
    CActivityBase &activity;
    IJoinHelper *jhelper;
    bool leftouter;  
    bool rightouter;  
    bool exclude; 
    
    IHThorJoinArg *helper;  
    Linked<IEngineRowAllocator> allocator;
    OwnedConstThorRow defaultLeft;
    OwnedConstThorRow defaultRight;
    unsigned numworkers;
    ThorActivityKind kind;
    Owned<IException> exc;
    CriticalSection sect;
    bool eos;


    void setException(IException *e,const char *title)
    {
        CriticalBlock b(sect);
        EXCLOG(e,title);
        if (exc.get())
            e->Release();
        else
            exc.setown(e);
    }


    class cWorkItem
    {
        CActivityBase &activity;
    public:
        CThorExpandingRowArray lgroup;
        CThorExpandingRowArray rgroup;
        const void *row;
        inline cWorkItem(CActivityBase &_activity, CThorExpandingRowArray *_lgroup, CThorExpandingRowArray *_rgroup)
            : activity(_activity), lgroup(_activity), rgroup(_activity)
        {
            set(_lgroup,_rgroup);
        }
        inline cWorkItem(CActivityBase &_activity) : activity(_activity), lgroup(_activity), rgroup(_activity)
        {
            clear();
        }

        inline void set(CThorExpandingRowArray *_lgroup, CThorExpandingRowArray *_rgroup)
        {
            if (_lgroup)
                lgroup.transfer(*_lgroup);
            else
                lgroup.kill();
            if (_rgroup)
                rgroup.transfer(*_rgroup);
            else
                rgroup.kill();
            row = NULL;
        }
        inline void set(const void *_row)
        {
            lgroup.kill();
            rgroup.kill();
            row = _row;
        }
        inline void clear()
        {
            set(NULL);
        }
    };

    class cOutItem
    {
    public:
        cOutItem(const void *_row,bool _done)
        {
            row = _row;
            done = _done;
        }
        const void *row;
        bool done; 
    };


    void doMatch(cWorkItem &work,SimpleInterThreadQueueOf<cOutItem,false> &outqueue) 
    {
        MemoryBuffer rmatchedbuf;  
        CThorExpandingRowArray &rgroup = (kind==TAKselfjoin)?work.lgroup:work.rgroup;
        bool *rmatched;
        if (rightouter) {
            rmatched = (bool *)rmatchedbuf.clear().reserve(rgroup.ordinality());
            memset(rmatched,0,rgroup.ordinality());
        }
        ForEachItemIn(leftidx,work.lgroup)
        {
            bool lmatched = !leftouter;
            for (unsigned rightidx=0; rightidx<rgroup.ordinality(); rightidx++) {
                if (helper->match(work.lgroup.query(leftidx),rgroup.query(rightidx))) {
                    lmatched = true;
                    if (rightouter) 
                        rmatched[rightidx] = true;
                    RtlDynamicRowBuilder ret(allocator);
                    size32_t sz = exclude?0:helper->transform(ret,work.lgroup.query(leftidx),rgroup.query(rightidx));
                    if (sz) 
                        outqueue.enqueue(new cOutItem(ret.finalizeRowClear(sz),false));

                }
            }
            if (!lmatched) {
                RtlDynamicRowBuilder ret(allocator);
                size32_t sz =  helper->transform(ret, work.lgroup.query(leftidx), defaultRight);
                if (sz) 
                    outqueue.enqueue(new cOutItem(ret.finalizeRowClear(sz),false));
            }   
        }
        if (rightouter) {
            ForEachItemIn(rightidx2,rgroup) {
                if (!rmatched[rightidx2]) {
                    RtlDynamicRowBuilder ret(allocator);
                    size32_t sz =  helper->transform(ret, defaultLeft, rgroup.query(rightidx2));
                    if (sz) 
                        outqueue.enqueue(new cOutItem(ret.finalizeRowClear(sz),false));
                }
            }
        }
    }

    CMultiCoreJoinHelperBase(CActivityBase &_activity, unsigned numthreads, IJoinHelper *_jhelper, IHThorJoinArg *_helper, IEngineRowAllocator *_allocator)
        : activity(_activity), allocator(_allocator)
    {
        kind = activity.queryContainer().getKind();
        jhelper = _jhelper;
        helper = _helper;
        unsigned flags = helper->getJoinFlags();
        leftouter = (flags & JFleftouter) != 0;
        rightouter = (flags & JFrightouter) != 0;
        exclude = (flags & JFexclude) != 0;
        numworkers = numthreads;
        eos = false;
    }

    bool init(
            IRowStream *strmL,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *allocatorL,
            IEngineRowAllocator *allocatorR,
            IOutputMetaData * outputmetaL,   // for XML output 
            bool *_abort,
            IMulticoreIntercept *_mcoreintercept
        )
    {
        if (!jhelper->init(strmL,strmR,allocatorL,allocatorR,outputmetaL,_abort,this))
            return false;
        if (rightouter) {
            RtlDynamicRowBuilder r(allocatorL);
            size32_t sz = helper->createDefaultLeft(r);
            defaultLeft.setown(r.finalizeRowClear(sz));
        }
        if (leftouter) {
            RtlDynamicRowBuilder r(allocatorR);
            size32_t sz = helper->createDefaultRight(r);
            defaultRight.setown(r.finalizeRowClear(sz));
        }
        return true;
    }

    rowcount_t getLhsProgress() const
    {
        if (jhelper)
            return jhelper->getLhsProgress();
        return 0;
    }
    rowcount_t getRhsProgress() const
    {
        if (jhelper)
            return jhelper->getRhsProgress();
        return 0;
    }

};


class CMultiCoreJoinHelper: public CMultiCoreJoinHelperBase
{
    unsigned curin;         // only updated from cReader thread
    unsigned curout;            // only updated from cReader thread
    

    class cReader: public Thread
    {
    public:
        CMultiCoreJoinHelper *parent;
        cReader()
            : Thread("CMultiCoreJoinHelper::cReader")
        {
        }
        int run()
        {
            PROGLOG("CMultiCoreJoinHelper::cReader started");
            try {
                const void * row = parent->jhelper->nextRow();
                assertex(!row);
            }
            catch (IException *e) {
                parent->setException(e,"CMultiCoreJoinHelper::cReader");
            }
            for (unsigned i=0;i<parent->numworkers;i++) 
                parent->addWork(NULL,NULL);
            PROGLOG("CMultiCoreJoinHelper::cReader exit");
            return 0;
        }
    } reader;

    class cWorker: public Thread
    {
        CMultiCoreJoinHelper *parent;
    public:
        cWorkItem work;
        Semaphore workready;
        Semaphore workwait;
        SimpleInterThreadQueueOf<cOutItem,false> outqueue;

        cWorker(CActivityBase &activity, CMultiCoreJoinHelper *_parent)
            : Thread("CMultiCoreJoinHelper::cWorker"), parent(_parent), work(activity)
        {
        }

        ~cWorker()
        {
            while (outqueue.ordinality())
                delete outqueue.dequeue();
        }
        int run()
        {
            PROGLOG("CMultiCoreJoinHelper::cWorker started");
            MemoryBuffer rmatchedbuf;  
            bool selfjoin = (parent->kind==TAKselfjoin);
            loop {
                work.clear();
                workready.signal();
                workwait.wait();
                try {
                    if (work.row) {
                        outqueue.enqueue(new cOutItem(work.row,false));
                    }
                    else {
                        if (work.lgroup.ordinality()==0)
                            break;
                        parent->doMatch(work,outqueue);
                    }
                    outqueue.enqueue(new cOutItem(NULL,false));
                }
                catch (IException *e) {
                    parent->setException(e,"CMultiCoreJoinHelper::cWorker");
                    break;
                }
            }
            outqueue.enqueue(new cOutItem(NULL,true));
            PROGLOG("CMultiCoreJoinHelper::cWorker exit");


            return 0;

        }
    } **workers;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CMultiCoreJoinHelper(CActivityBase &activity, unsigned numthreads, IJoinHelper *_jhelper, IHThorJoinArg *_helper, IEngineRowAllocator *_allocator)
        : CMultiCoreJoinHelperBase(activity, numthreads, _jhelper, _helper, _allocator)
    {
        reader.parent = this;
        workers = new cWorker *[numthreads];
        curin = 0;
        curout = 0;
        for (unsigned i=0;i<numthreads;i++)
            workers[i] = new cWorker(activity, this);
    }

    ~CMultiCoreJoinHelper()
    {
        if (!reader.join(1000*60))
            ERRLOG("~CMultiCoreJoinHelper reader join timed out");
        for (unsigned i=0;i<numworkers;i++) {
            if (!workers[i]->join(1000*60))
                ERRLOG("~CMultiCoreJoinHelper worker[%d] join timed out",i);
        }
        for (unsigned i=0;i<numworkers;i++) 
            delete workers[i];
        delete workers;
        ::Release(jhelper);
    }


    bool init(
            IRowStream *strmL,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *allocatorL,
            IEngineRowAllocator *allocatorR,
            IOutputMetaData * outputmetaL,   // for XML output 
            bool *_abort,
            IMulticoreIntercept *_mcoreintercept
        )
    {
        if (!CMultiCoreJoinHelperBase::init(strmL,strmR,allocatorL,allocatorR,outputmetaL,_abort,this))
            return false;
        for (unsigned i=0;i<numworkers;i++) {
            workers[i]->outqueue.setLimit(1000);  // shouldn't be that large but just in case
            workers[i]->start();
        }
        reader.start();
        return true;
    }

    virtual const void *nextRow()
    {
        cOutItem * item;
        loop {
            if (eos)
                return NULL;
            item = workers[curout]->outqueue.dequeue(); 
            if (exc.get()) {
                CriticalBlock b(sect);
                throw exc.getClear();
            }
            if (item&&item->row)
                break;
            eos = item->done;
            delete item;
            curout = (curout+1)%numworkers;
        }

        const void * ret = item->row;
        delete item;
        return ret;
    }

    void addWork(CThorExpandingRowArray *lgroup,CThorExpandingRowArray *rgroup)
    {
        if (!lgroup||!lgroup->ordinality()) {
            PROGLOG("hello");
        }
        cWorker *worker = workers[curin];
        worker->workready.wait();
        workers[curin]->work.set(lgroup,rgroup);
        worker->workwait.signal();
        curin = (curin+1)%numworkers;
    }

    void addRow(const void *row)
    {
        cWorker *worker = workers[curin];
        worker->workready.wait();
        workers[curin]->work.set(row);
        worker->workwait.signal();
        curin = (curin+1)%numworkers;
    }

};

class CMultiCoreUnorderedJoinHelper: public CMultiCoreJoinHelperBase
{
    unsigned stoppedworkers;
    
    void setException(IException *e,const char *title)
    {
        CriticalBlock b(sect);
        EXCLOG(e,title);
        if (exc.get())
            e->Release();
        else
            exc.setown(e);
    }


    SimpleInterThreadQueueOf<cWorkItem,false> workqueue;
    SimpleInterThreadQueueOf<cOutItem,false> outqueue;          // used in unordered


    class cReader: public Thread
    {
    public:
        CMultiCoreUnorderedJoinHelper *parent;
        cReader()
            : Thread("CMulticoreUnorderedJoinHelper::cReader")
        {
        }
        int run()
        {
            PROGLOG("CMulticoreUnorderedJoinHelper::cReader started");
            try {
                const void * row = parent->jhelper->nextRow();
                assertex(!row);
            }
            catch (IException *e) {
                parent->setException(e,"CMulticoreUnorderedJoinHelper::cReader");
            }
            for (unsigned i=0;i<parent->numworkers;i++) 
                parent->workqueue.enqueue(new cWorkItem(parent->activity, NULL, NULL));
            PROGLOG("CMulticoreUnorderedJoinHelper::cReader exit");
            return 0;
        }
    } reader;


    class cWorker: public Thread
    {
        CMultiCoreUnorderedJoinHelper *parent;
    public:
        SimpleInterThreadQueueOf<cOutItem,false> outqueue;          // used in ordered
        cWorker(CMultiCoreUnorderedJoinHelper *_parent)
            : Thread("CMulticoreUnorderedJoinHelper::cWorker"), parent(_parent)
        {
        }
        int run()
        {
            PROGLOG("CMulticoreUnorderedJoinHelper::cWorker started");
            loop {
                cWorkItem *work = parent->workqueue.dequeue();
                if (!work||((work->lgroup.ordinality()==0)&&(work->rgroup.ordinality()==0))) {
                    delete work;
                    break;
                }
                try {
                    parent->doMatch(*work,outqueue);
                    delete work;
                }
                catch (IException *e) {
                    parent->setException(e,"CMulticoreUnorderedJoinHelper::cWorker");
                    delete work;
                    break;
                }
            }
            parent->outqueue.enqueue(new cOutItem(NULL,true));
            PROGLOG("CMulticoreUnorderedJoinHelper::cWorker exit");


            return 0;

        }
    } **workers;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CMultiCoreUnorderedJoinHelper(CActivityBase &activity, unsigned numthreads, IJoinHelper *_jhelper, IHThorJoinArg *_helper, IEngineRowAllocator *_allocator)
        : CMultiCoreJoinHelperBase(activity, numthreads, _jhelper, _helper, _allocator)
    {
        reader.parent = this;
        stoppedworkers = 0;
        workers = new cWorker *[numthreads];
        for (unsigned i=0;i<numthreads;i++) 
            workers[i] = new cWorker(this);
    }

    ~CMultiCoreUnorderedJoinHelper()
    {
        if (!reader.join(1000*60))
            ERRLOG("~CMulticoreUnorderedJoinHelper reader join timed out");
        for (unsigned i=0;i<numworkers;i++) {
            if (!workers[i]->join(1000*60))
                ERRLOG("~CMulticoreUnorderedJoinHelper worker[%d] join timed out",i);
        }
        while (outqueue.ordinality())
            delete outqueue.dequeue();
        while (workqueue.ordinality())
            delete workqueue.dequeue();
        for (unsigned i=0;i<numworkers;i++) 
            delete workers[i];
        delete workers;
        ::Release(jhelper);
    }


    bool init(
            IRowStream *strmL,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *allocatorL,
            IEngineRowAllocator *allocatorR,
            IOutputMetaData * outputmetaL,   // for XML output 
            bool *_abort,
            IMulticoreIntercept *_mcoreintercept
        )
    {
        if (!CMultiCoreJoinHelperBase::init(strmL,strmR,allocatorL,allocatorR,outputmetaL,_abort,this))
            return false;
        workqueue.setLimit(numworkers+1);
        outqueue.setLimit(numworkers*1000);  // shouldn't be that large but just in case
        for (unsigned i=0;i<numworkers;i++)
            workers[i]->start();
        reader.start();
        return true;
    }

    virtual const void *nextRow()
    {
        cOutItem * item;
        if (eos)
            return NULL;
        loop {
            if (stoppedworkers==numworkers) {
                eos = true;
                return NULL;
            }
            item = outqueue.dequeue(); 
            if (exc.get()) {
                delete item;
                CriticalBlock b(sect);
                throw exc.getClear();
            }
            if (item&&item->row)
                break;
            delete item;
            stoppedworkers++;
        }

        const void * ret = item->row;
        delete item;
        return ret;
    }

    void addWork(CThorExpandingRowArray *lgroup,CThorExpandingRowArray *rgroup)
    {
        cWorkItem *item = new cWorkItem(activity, lgroup, rgroup);
        workqueue.enqueue(item);
    }

    void addRow(const void *row)
    {
        assertex(row);
        cOutItem *item = new cOutItem(row,false);
        outqueue.enqueue(item);
    }

};


IJoinHelper *createJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IEngineRowAllocator *allocator, bool parallelmatch, bool unsortedoutput)
{
    // 
#ifdef TEST_PARALLEL_MATCH
    parallelmatch = true;
#endif
#ifdef TEST_UNSORTED_OUT
    unsortedoutput = true;
#endif
    IJoinHelper *jhelper = new CJoinHelper(activity, helper, allocator);
    if (!parallelmatch||helper->getKeepLimit()||((helper->getJoinFlags()&JFslidingmatch)!=0)) // currently don't support betweenjoin or keep and multicore
        return jhelper;
    unsigned numthreads = getAffinityCpus();
    if (unsortedoutput)
        return new CMultiCoreUnorderedJoinHelper(activity, numthreads, jhelper, helper, allocator);
    return new CMultiCoreJoinHelper(activity, numthreads, jhelper, helper, allocator);
}


IJoinHelper *createSelfJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IEngineRowAllocator *allocator, bool parallelmatch, bool unsortedoutput)
{
#ifdef TEST_PARALLEL_MATCH
    parallelmatch = true;
#endif
#ifdef TEST_UNSORTED_OUT
    unsortedoutput = true;
#endif
    IJoinHelper *jhelper = new SelfJoinHelper(activity, helper, allocator);
    if (!parallelmatch||helper->getKeepLimit()||((helper->getJoinFlags()&JFslidingmatch)!=0)) // currently don't support betweenjoin or keep and multicore
        return jhelper;
    unsigned numthreads = getAffinityCpus();
    if (unsortedoutput)
        return new CMultiCoreUnorderedJoinHelper(activity, numthreads, jhelper, helper, allocator);
    return new CMultiCoreJoinHelper(activity, numthreads, jhelper, helper, allocator);
}


