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

#include "jexcept.hpp"
#include "thorherror.h"
#include "roxiehelper.hpp"
#include "roxielmj.hpp"
#include "roxierow.hpp"
#include "roxierowbuff.hpp"

#include "jmisc.hpp"
#include "jfile.hpp"
#include "mpbase.hpp"
#include "dafdesc.hpp"
#include "dadfs.hpp"

unsigned traceLevel = 0;

//OwnedRowArray
void OwnedRowArray::clear()
{
    ForEachItemIn(idx, buff)
        ReleaseRoxieRow(buff.item(idx));
    buff.kill();
}

void OwnedRowArray::clearPart(aindex_t from, aindex_t to)
{
    aindex_t idx;
    for(idx = from; idx < to; idx++)
        ReleaseRoxieRow(buff.item(idx));
    buff.removen(from, to-from);
}

void OwnedRowArray::replace(const void * row, aindex_t pos)
{
    ReleaseRoxieRow(buff.item(pos));
    buff.replace(row, pos);
}

//=========================================================================================

//CRHRollingCacheElem copied/modified from THOR
CRHRollingCacheElem::CRHRollingCacheElem()
{
    row = NULL;
    cmp = INT_MIN; 
}
CRHRollingCacheElem::~CRHRollingCacheElem()
{
    if (row)
        ReleaseRoxieRow(row);
}
void CRHRollingCacheElem::set(const void *_row)
{
    if (row)
        ReleaseRoxieRow(row);
    row = _row;
}


//CRHRollingCache copied/modified from THOR CRollingCache
CRHRollingCache::~CRHRollingCache()
{
    loop 
    {  
        CRHRollingCacheElem *e = cache.dequeue();  
        if (!e)  
            break;  
        delete e;  
    }  
}

void CRHRollingCache::init(IInputBase *_in, unsigned _max)
{
    max = _max;
    in =_in;
    cache.clear();
    cache.reserve(max);
    eos = false;
    while (cache.ordinality()<max/2)
        cache.enqueue(NULL);
    while (!eos && (cache.ordinality()<max))
        advance();
}

#ifdef TRACEROLLING
void CRHRollingCache::PrintCache()
{
    for (unsigned i = 0;i<max;i++) {
        CRHRollingCacheElem *e = cache.item(i);
        if (i==0)
            DBGLOG("RC==============================");
        int ii = 0;
        if (e && e->row)
            ii = isalpha(*((char*)e->row)) ? 0 : 4;
        chas sz[100];
        sprintf(sz,"%c%d: %s",(i==max/2)?'>':' ',i,e?(const char *)e->row+ii:"-----");
        for (int xx=0; sz[xx] != NULL; xx++)
        {
            if (!isprint(sz[xx]))
            {
                sz[xx] = NULL;
                break;
            }
        }
        DBGLOG(sz);
        if (i == max-1)
            DBGLOG("RC==============================");
    }
}
#endif

CRHRollingCacheElem * CRHRollingCache::mid(int rel)
{
    return cache.item((max/2)+rel); // relies on unsigned wrap
}

void CRHRollingCache::advance()
{
    CRHRollingCacheElem *e = (cache.ordinality()==max)?cache.dequeue():NULL;    //cache full, remove head element
    if (!eos) {
        if (!e)
            e = new CRHRollingCacheElem();
        const void * nextrec = in->nextInGroup();//get row from CRHCRHDualCache::cOut, which gets from CRHCRHDualCache, which gets from input
        if (!nextrec)
            nextrec = in->nextInGroup();
        if (nextrec) {
            e->set(nextrec);
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

//=========================================================================================

//CRHDualCache copied from THOR, and modified to get input from IInputBase instead 
//of IReadSeqVar and to manage rows as OwnedRoxieRow types
CRHDualCache::CRHDualCache()
{
    strm1 = NULL;
    strm2 = NULL;
}

CRHDualCache::~CRHDualCache()
{
    ::Release(strm1);
    ::Release(strm2);
    loop 
    {  
        CRHRollingCacheElem *e = cache.dequeue();  
        if (!e)  
            break;  
        delete e;  
    }  
}

void CRHDualCache::init(IInputBase * _in)
{
    in = _in;
    cache.clear();
    eos = false;
    base = 0;
    posL = 0;
    posR = 0;
    strm1 = new cOut(this,posL);
    strm2 = new cOut(this,posR) ;
}

#ifdef TRACEROLLING
void CRHDualCache::PrintCache()
{
    for (unsigned i = 0;i<cache.ordinality();i++) {
        CRHRollingCacheElem *e = cache.item(i);
        if (i==0)
        {
            DBGLOG("DC=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-BASE:%d,posL=%d,posR=%d %s", base, posL,posR, eos?"EOS":"");
        }
        
        DBGLOG("%c%d: %s",(i==cache.ordinality()/2)?'>':' ',i,e?(const char *)e->row:"-----");
        if (i == cache.ordinality()-1)
            DBGLOG("DC=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
    }
}
#endif

bool CRHDualCache::get(unsigned n, CRHRollingCacheElem *&out)
{
    // take off any no longer needed
    CRHRollingCacheElem *e=NULL;
    while ((base<posL) && (base<posR)) {
        delete e;
        e = cache.dequeue();
        base++;
    }
    assertex(n>=base);
    while (!eos && (n-base>=cache.ordinality())) //element already in cache?
    {
        if (!e)
            e = new CRHRollingCacheElem;
        const void * nextrec = in->nextInGroup();   //get from activity
        if (!nextrec)
            nextrec = in->nextInGroup();
        if (!nextrec) {
            eos = true;
            break;
        }

        e->set(nextrec);

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

size32_t CRHDualCache::getRecordSize(const void *ptr)
{
    return in->queryOutputMeta()->getRecordSize(ptr);
}

size32_t CRHDualCache::getFixedSize() const
{
    return in->queryOutputMeta()->getFixedSize();
}

size32_t CRHDualCache::getMinRecordSize() const
{
    return in->queryOutputMeta()->getMinRecordSize();
}

CRHDualCache::cOut::cOut(CRHDualCache *_parent, unsigned &_pos) 
: pos(_pos)
{
    parent = _parent;
    stopped = false;
}

const void * CRHDualCache::cOut::nextInGroup()
{
    CRHRollingCacheElem *e;
    if (stopped || !parent->get(pos,e))
        return NULL;   //no more data
    LinkRoxieRow(e->row);
    pos++;
    return e->row;
}

IOutputMetaData * CRHDualCache::cOut::queryOutputMeta() const
{
    return parent->input()->queryOutputMeta();
}

void CRHDualCache::cOut::stop()
{
    pos = (unsigned)-1;
    stopped = true;
}

//=========================================================================================

IRHLimitedCompareHelper *createRHLimitedCompareHelper()
{
    return new CRHLimitedCompareHelper();
}

//CRHLimitedCompareHelper
void CRHLimitedCompareHelper::init( unsigned _atmost,
                                 IInputBase *_in,
                                 ICompare * _cmp,
                                 ICompare * _limitedcmp )
{
    atmost = _atmost;
    cache.setown(new CRHRollingCache());
    cache->init(_in,(atmost+1)*2);
    cmp = _cmp;
    limitedcmp = _limitedcmp;
}

bool CRHLimitedCompareHelper::getGroup(OwnedRowArray &group, const void *left)
{
    // this could be improved!
    
    // first move 'mid' forwards until mid>=left
    int low = 0;
    loop 
    {
        CRHRollingCacheElem * r = cache->mid(0);
        if (!r)
            break; // hit eos              
        int c = cmp->docompare(left,r->row);
        if (c == 0) 
        {
            r->cmp = limitedcmp->docompare(left,r->row);
            if (r->cmp <= 0) 
                break;
        }
        else if (c < 0) 
        {
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
    loop 
    {
        CRHRollingCacheElem * pr = cache->mid(low-1);
        if (!pr)
            break; // hit start 
        int c = cmp->docompare(left,pr->row);
        if (c == 0) 
        {
            pr->cmp = limitedcmp->docompare(left,pr->row);
            if (pr->cmp==1) 
                break;
        }
        else 
        {
            pr->cmp = 1;  
            break;
        }
        low--;
    }
    int high = 0;
    if (cache->mid(0)) // check haven't already hit end
    { 
        // now scan fwd
        loop 
        {
            high++;
            CRHRollingCacheElem * nr = cache->mid(high);
            if (!nr) 
                break;
            int c = cmp->docompare(left,nr->row);
            if (c==0) 
            {
                nr->cmp = limitedcmp->docompare(left,nr->row);
                if (nr->cmp==-1) 
                    break;
            }
            else 
            {
                nr->cmp = -1;
                break;
            }
        }
    }
    while (high-low>(int)atmost) 
    {
        int vl = iabs(cache->mid(low)->cmp);
        int vh = iabs(cache->mid(high-1)->cmp);
        int v;
        if (vl==0) 
        {
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
    for (int i=low;i<high;i++) 
    {
        CRHRollingCacheElem *r = cache->mid(i);
        LinkRoxieRow(r->row);
        group.append(r->row);   
    }
    return group.ordinality()>0;
}

//=========================================================================================

// default implementations - can be overridden for efficiency...
bool ISimpleInputBase::nextGroup(ConstPointerArray & group)
{
    // MORE - this should be replaced with a version that reads to a builder
    const void * next;
    while ((next = nextInGroup()) != NULL)
        group.append(next);
    if (group.ordinality())
        return true;
    return false;
}

void ISimpleInputBase::readAll(RtlLinkedDatasetBuilder &builder)
{
    loop
    {
        const void *nextrec = nextInGroup();
        if (!nextrec)
        {
            nextrec = nextInGroup();
            if (!nextrec)
                break;
            builder.appendEOG();
        }
        builder.appendOwn(nextrec);
    }
}


//=========================================================================================

// Ability to read an input stream and group and/or sort it on-the-fly

using roxiemem::OwnedConstRoxieRow;

class InputReaderBase  : public CInterfaceOf<IGroupedInput>
{
protected:
    IInputBase *input;
public:
    InputReaderBase(IInputBase *_input)
    : input(_input)
    {
    }

    virtual IOutputMetaData * queryOutputMeta() const
    {
        return input->queryOutputMeta();
    }
};

class GroupedInputReader : public InputReaderBase
{
protected:
    bool firstRead;
    bool eof;
    bool endGroupPending;
    OwnedConstRoxieRow next;
    const ICompare *compare;
public:
    GroupedInputReader(IInputBase *_input, const ICompare *_compare)
    : InputReaderBase(_input), compare(_compare)
    {
        firstRead = false;
        eof = false;
        endGroupPending = false;
    }

    virtual const void *nextInGroup()
    {
        if (!firstRead)
        {
            firstRead = true;
            next.setown(input->nextInGroup());
        }

        if (eof || endGroupPending)
        {
            endGroupPending = false;
            return NULL;
        }

        OwnedConstRoxieRow prev(next.getClear());
        next.setown(input->nextUngrouped());  // skip incoming grouping if present

        if (next)
        {
            dbgassertex(prev);  // If this fails, you have an initial empty group. That is not legal.
            if (compare && compare->docompare(prev, next) != 0)
                endGroupPending = true;
        }
        else
            eof = true;
        return prev.getClear();
    }
};

class DegroupedInputReader : public InputReaderBase
{
public:
    DegroupedInputReader(IInputBase *_input) : InputReaderBase(_input)
    {
    }
    virtual const void *nextInGroup()
    {
        return input->nextUngrouped();
    }
};

class SortedInputReader : public InputReaderBase
{
protected:
    DegroupedInputReader degroupedInput;
    Owned<ISortAlgorithm> sorter;
    bool firstRead;
public:
    SortedInputReader(IInputBase *_input, ISortAlgorithm *_sorter)
      : InputReaderBase(_input), degroupedInput(_input), sorter(_sorter), firstRead(false)
    {
        sorter->reset();
    }

    virtual const void *nextInGroup()
    {
        if (!firstRead)
        {
            firstRead = true;
            sorter->prepare(&degroupedInput);
        }
        return sorter->next();
    }
};

class SortedGroupedInputReader : public SortedInputReader
{
protected:
    bool eof;
    bool endGroupPending;
    OwnedConstRoxieRow next;
    const ICompare *compare;
public:
    SortedGroupedInputReader(IInputBase *_input, const ICompare *_compare, ISortAlgorithm *_sorter)
      : SortedInputReader(_input, _sorter), compare(_compare), eof(false), endGroupPending(false)
    {
    }

    virtual const void *nextInGroup()
    {
        if (!firstRead)
        {
            firstRead = true;
            sorter->prepare(&degroupedInput);
            next.setown(sorter->next());
        }

        if (eof || endGroupPending)
        {
            endGroupPending = false;
            return NULL;
        }

        OwnedConstRoxieRow prev(next.getClear());
        next.setown(sorter->next());

        if (next)
        {
            dbgassertex(prev);  // If this fails, you have an initial empty group. That is not legal.
            if (compare->docompare(prev, next) != 0) // MORE - could assert >=0, as input is supposed to be sorted
                 endGroupPending = true;
        }
        else
            eof = true;
        return prev.getClear();
    }
};

extern IGroupedInput *createGroupedInputReader(IInputBase *_input, const ICompare *_groupCompare)
{
    dbgassertex(_input && _groupCompare);
    return new GroupedInputReader(_input, _groupCompare);
}

extern IGroupedInput *createDegroupedInputReader(IInputBase *_input)
{
    dbgassertex(_input);
    return new DegroupedInputReader(_input);
}

extern IGroupedInput *createSortedInputReader(IInputBase *_input, ISortAlgorithm *_sorter)
{
    dbgassertex(_input && _sorter);
    return new SortedInputReader(_input, _sorter);
}

extern IGroupedInput *createSortedGroupedInputReader(IInputBase *_input, const ICompare *_groupCompare, ISortAlgorithm *_sorter)
{
    dbgassertex(_input && _groupCompare && _sorter);
    return new SortedGroupedInputReader(_input, _groupCompare, _sorter);
}

//========================================================================================= 

class CSortAlgorithm : implements CInterfaceOf<ISortAlgorithm>
{
public:
    virtual void getSortedGroup(ConstPointerArray & result)
    {
        loop
        {
            const void * row = next();
            if (!row)
                return;
            result.append(row);
        }
    }
};

class CInplaceSortAlgorithm : public CSortAlgorithm
{
protected:
    unsigned curIndex;
    ConstPointerArray sorted;
    ICompare *compare;

public:
    CInplaceSortAlgorithm(ICompare *_compare) : compare(_compare)
    {
        curIndex = 0;
    }

    virtual const void *next()
    {
        if (sorted.isItem(curIndex))
            return sorted.item(curIndex++);
        return NULL;
    }

    virtual void reset()
    {
        while (sorted.isItem(curIndex))
            ReleaseRoxieRow(sorted.item(curIndex++));
        curIndex = 0;
        sorted.kill();
    }
    virtual void getSortedGroup(ConstPointerArray & result)
    {
        sorted.swapWith(result);
        curIndex = 0;
    }
};

class CQuickSortAlgorithm : public CInplaceSortAlgorithm
{
public:
    CQuickSortAlgorithm(ICompare *_compare) : CInplaceSortAlgorithm(_compare) {}

    virtual void prepare(IInputBase *input)
    {
        curIndex = 0;
        if (input->nextGroup(sorted))
            qsortvec(const_cast<void * *>(sorted.getArray()), sorted.ordinality(), *compare);
    }
};

class CStableInplaceSortAlgorithm : public CInplaceSortAlgorithm
{
public:
    CStableInplaceSortAlgorithm(ICompare *_compare) : CInplaceSortAlgorithm(_compare) {}

    virtual void sortRows(void * * rows, size_t numRows, void * * temp) = 0;

    virtual void prepare(IInputBase *input)
    {
        curIndex = 0;
        if (input->nextGroup(sorted))
        {
            unsigned numRows = sorted.ordinality();
            void **rows = const_cast<void * *>(sorted.getArray());
            MemoryAttr tempAttr(numRows*sizeof(void **)); // Temp storage for stable sort. This should probably be allocated from roxiemem
            void **temp = (void **) tempAttr.bufferBase();
            sortRows(rows, numRows, temp);
        }
    }
};

class CStableQuickSortAlgorithm : public CStableInplaceSortAlgorithm
{
public:
    CStableQuickSortAlgorithm(ICompare *_compare) : CStableInplaceSortAlgorithm(_compare) {}

    virtual void sortRows(void * * rows, size_t numRows, void * * temp)
    {
        qsortvecstableinplace(rows, numRows, *compare, temp);
    }
};

class CMergeSortAlgorithm : public CStableInplaceSortAlgorithm
{
public:
    CMergeSortAlgorithm(ICompare *_compare) : CStableInplaceSortAlgorithm(_compare) {}

    virtual void sortRows(void * * rows, size_t numRows, void * * temp)
    {
        msortvecstableinplace(rows, numRows, *compare, temp);
    }
};

class CParallelMergeSortAlgorithm : public CStableInplaceSortAlgorithm
{
public:
    CParallelMergeSortAlgorithm(ICompare *_compare) : CStableInplaceSortAlgorithm(_compare) {}

    virtual void sortRows(void * * rows, size_t numRows, void * * temp)
    {
        parmsortvecstableinplace(rows, numRows, *compare, temp);
    }
};

#define INSERTION_SORT_BLOCKSIZE 1024

class SortedBlock : public CInterface, implements IInterface
{
    unsigned sequence;
    const void **rows;
    unsigned length;
    unsigned pos;

    SortedBlock(const SortedBlock &);
public:
    IMPLEMENT_IINTERFACE;

    SortedBlock(unsigned _sequence, roxiemem::IRowManager *rowManager, unsigned activityId) : sequence(_sequence)
    {
        rows = (const void **) rowManager->allocate(INSERTION_SORT_BLOCKSIZE * sizeof(void *), activityId);
        length = 0;
        pos = 0;
    }

    ~SortedBlock()
    {
        while (pos < length)
            ReleaseRoxieRow(rows[pos++]);
        ReleaseRoxieRow(rows);
    }

    int compareTo(SortedBlock *r, ICompare *compare)
    {
        int rc = compare->docompare(rows[pos], r->rows[r->pos]);
        if (!rc)
            rc = sequence - r->sequence;
        return rc;
    }

    const void *next()
    {
        if (pos < length)
            return rows[pos++];
        else
            return NULL;
    }

    inline bool eof()
    {
        return pos==length;
    }

    bool insert(const void *next, ICompare *_compare )
    {
        unsigned b = length;
        if (b == INSERTION_SORT_BLOCKSIZE)
            return false;
        else if (b < 7)
        {
            while (b)
            {
                if (_compare->docompare(next, rows[b-1]) >= 0)
                    break;
                b--;
            }
            if (b != length)
                memmove(&rows[b+1], &rows[b], (length - b) * sizeof(void *));
            rows[b] = next;
            length++;
            return true;
        }
        else
        {
            unsigned int a = 0;
            while ((int)a<b)
            {
                int i = (a+b)/2;
                int rc = _compare->docompare(next, rows[i]);
                if (rc>=0)
                    a = i+1;
                else
                    b = i;
            }
            if (a != length)
                memmove(&rows[a+1], &rows[a], (length - a) * sizeof(void *));
            rows[a] = next;
            length++;
            return true;
        }
    }
};

class CInsertionSortAlgorithm : public CSortAlgorithm
{
    SortedBlock *curBlock;
    unsigned blockNo;
    IArrayOf<SortedBlock> blocks;
    unsigned activityId;
    roxiemem::IRowManager *rowManager;
    ICompare *compare;

    void newBlock()
    {
        blocks.append(*curBlock);
        curBlock = new SortedBlock(blockNo++, rowManager, activityId);
    }

    inline static int doCompare(SortedBlock &l, SortedBlock &r, ICompare *compare)
    {
        return l.compareTo(&r, compare);
    }

    void makeHeap()
    {
        /* Permute blocks to establish the heap property
           For each element p, the children are p*2+1 and p*2+2 (provided these are in range)
           The children of p must both be greater than or equal to p
           The parent of a child c is given by p = (c-1)/2
        */
        unsigned i;
        unsigned n = blocks.length();
        SortedBlock **s = blocks.getArray();
        for (i=1; i<n; i++)
        {
            SortedBlock * r = s[i];
            int c = i; /* child */
            while (c > 0)
            {
                int p = (c-1)/2; /* parent */
                if ( doCompare( blocks.item(c), blocks.item(p), compare ) >= 0 )
                    break;
                s[c] = s[p];
                s[p] = r;
                c = p;
            }
        }
    }

    void remakeHeap()
    {
        /* The row associated with block[0] will have changed
           This code restores the heap property
        */
        unsigned p = 0; /* parent */
        unsigned n = blocks.length();
        SortedBlock **s = blocks.getArray();
        while (1)
        {
            unsigned c = p*2 + 1; /* child */
            if ( c >= n )
                break;
            /* Select smaller child */
            if ( c+1 < n && doCompare( blocks.item(c+1), blocks.item(c), compare ) < 0 ) c += 1;
            /* If child is greater or equal than parent then we are done */
            if ( doCompare( blocks.item(c), blocks.item(p), compare ) >= 0 )
                break;
            /* Swap parent and child */
            SortedBlock *r = s[c];
            s[c] = s[p];
            s[p] = r;
            /* child becomes parent */
            p = c;
        }
    }

public:
    CInsertionSortAlgorithm(ICompare *_compare, roxiemem::IRowManager *_rowManager, unsigned _activityId)
        : compare(_compare)
    {
        rowManager = _rowManager;
        activityId = _activityId;
        curBlock = NULL;
        blockNo = 0;
    }

    virtual void reset()
    {
        blocks.kill();
        delete curBlock;
        curBlock = NULL;
        blockNo = 0;
    }

    virtual void prepare(IInputBase *input)
    {
        blockNo = 0;
        curBlock = new SortedBlock(blockNo++, rowManager, activityId);
        loop
        {
            const void *next = input->nextInGroup();
            if (!next)
                break;
            if (!curBlock->insert(next, compare))
            {
                newBlock();
                curBlock->insert(next, compare);
            }
        }
        if (blockNo > 1)
        {
            blocks.append(*curBlock);
            curBlock = NULL;
            makeHeap();
        }
    }

    virtual const void * next()
    {
        const void *ret;
        if (blockNo==1) // single block case..
        {
            ret = curBlock->next();
        }
        else if (blocks.length())
        {
            SortedBlock &top = blocks.item(0);
            ret = top.next();
            if (top.eof())
                blocks.replace(blocks.popGet(), 0);
            remakeHeap();
        }
        else
            ret = NULL;
        return ret;
    }
};

class CHeapSortAlgorithm : public CSortAlgorithm
{
    unsigned curIndex;
    ConstPointerArray sorted;
    bool inputAlreadySorted;
    IntArray sequences;
    bool eof;
    ICompare *compare;

#ifdef _CHECK_HEAPSORT
    void checkHeap() const
    {
        unsigned n = sorted.ordinality();
        if (n)
        {
            ICompare *_compare = compare;
            void **s = sorted.getArray();
            int *sq = sequences.getArray();
            unsigned p;
#if 0
            CTXLOG("------------------------%d entries-----------------", n);
            for (p = 0; p < n; p++)
            {
                CTXLOG("HEAP %d: %d %.10s", p, sq[p], s[p] ? s[p] : "..");
            }
#endif
            for (p = 0; p < n; p++)
            {
                unsigned c = p*2+1;
                if (c<n)
                    assertex(!s[c] || (docompare(p, c, _compare, s, sq) <= 0));
                c++;
                if (c<n)
                    assertex(!s[c] || (docompare(p, c, _compare, s, sq) <= 0));
            }
        }
    }
#else
    inline void checkHeap() const {}
#endif

    const void *removeHeap()
    {
        unsigned n = sorted.ordinality();
        if (n)
        {
            const void *ret = sorted.item(0);
            if (n > 1 && ret)
            {
                ICompare *_compare = compare;
                const void **s = sorted.getArray();
                int *sq = sequences.getArray();
                unsigned v = 0; // vacancy
                loop
                {
                    unsigned c = 2*v + 1;
                    if (c < n)
                    {
                        unsigned f = c; // favourite to fill it
                        c++;
                        if (c < n && s[c] && (!s[f] || (docompare(f, c, _compare, s, sq) > 0))) // is the smaller of the children
                            f = c;
                        sq[v] = sq[f];
                        if ((s[v] = s[f]) != NULL)
                            v = f;
                        else
                            break;
                    }
                    else
                    {
                        s[v] = NULL;
                        break;
                    }
                }
            }
            checkHeap();
            return ret;
        }
        else
            return NULL;
    }

    static inline int docompare(unsigned l, unsigned r, ICompare *_compare, const void **s, int *sq)
    {
        int rc = _compare->docompare(s[l], s[r]);
        if (!rc)
            rc = sq[l] - sq[r];
        return rc;
    }

    void insertHeap(const void *next)
    {
        // Upside-down heap sort
        // Maintain a heap where every parent is lower than each of its children
        // Root (at node 0) is lowest record seen, nodes 2n+1, 2n+2 are the children
        // To insert a row, add it at end then keep swapping with parent as long as parent is greater
        // To remove a row, take row 0, then recreate heap by replacing it with smaller of two children and so on down the tree
        // Nice features:
        // 1. Deterministic
        // 2. Sort time can be overlapped with upstream/downstream processes - there is no delay between receiving last record from input and deliveriing first to output
        // 3. Already sorted case can be spotted at zero cost while reading.
        // 4. If you don't read all the results, you don't have to complete the sort
        // BUT it is NOT stable, so we have to use a parallel array of sequence numbers

        unsigned n = sorted.ordinality();
        sorted.append(next);
        sequences.append(n);
        if (!n)
            return;
        ICompare *_compare = compare;
        const void **s = sorted.getArray();
        if (inputAlreadySorted)
        {
            if (_compare->docompare(next, s[n-1]) >= 0)
                return;
            else
            {
                // MORE - could delay creating sequences until now...
                inputAlreadySorted = false;
            }
        }
        int *sq = sequences.getArray();
        unsigned q = n;
        while (n)
        {
            unsigned parent = (n-1) / 2;
            const void *p = s[parent];
            if (_compare->docompare(p, next) <= 0)
                break;
            s[n] = p;
            sq[n] = sq[parent];
            s[parent] = next;
            sq[parent] = q;
            n = parent;
        }
    }

public:
    CHeapSortAlgorithm(ICompare *_compare) : compare(_compare)
    {
        inputAlreadySorted = true;
        curIndex = 0;
        eof = false;
    }

    virtual void reset()
    {
        eof = false;
        if (inputAlreadySorted)
        {
            while (sorted.isItem(curIndex))
                ReleaseRoxieRow(sorted.item(curIndex++));
            sorted.kill();
        }
        else
        {
            roxiemem::ReleaseRoxieRows(sorted);
        }
        inputAlreadySorted = true;
        sequences.kill();
    }

    virtual void prepare(IInputBase *input)
    {
        inputAlreadySorted = true;
        curIndex = 0;
        eof = false;
        assertex(sorted.ordinality()==0);
        const void *next = input->nextInGroup();
        if (!next)
        {
            eof = true;
            return;
        }
        loop
        {
            insertHeap(next);
            next = input->nextInGroup();
            if (!next)
                break;
        }
        checkHeap();
    }

    virtual const void * next()
    {
        if (inputAlreadySorted)
        {
            if (sorted.isItem(curIndex))
            {
                return sorted.item(curIndex++);
            }
            else
                return NULL;
        }
        else
            return removeHeap();
    }
};

class CSpillingSortAlgorithm : public CSortAlgorithm, implements roxiemem::IBufferedRowCallback
{
    enum {
        InitialSortElements = 0,
        //The number of rows that can be added without entering a critical section, and therefore also the number
        //of rows that might not get freed when memory gets tight.
        CommitStep=32
    };
    roxiemem::DynamicRoxieOutputRowArray rowsToSort;
    roxiemem::RoxieSimpleInputRowArray sorted;
    ICompare *compare;
    roxiemem::IRowManager &rowManager;
    Owned<IDiskMerger> diskMerger;
    Owned<IRowStream> diskReader;
    IOutputMetaData *rowMeta;
    StringAttr tempDirectory;
    ICodeContext *ctx;
    unsigned activityId;
    bool stable;

public:
    CSpillingSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _stable)
        : rowsToSort(&_rowManager, InitialSortElements, CommitStep, _activityId),
          rowManager(_rowManager), compare(_compare), rowMeta(_rowMeta), ctx(_ctx), tempDirectory(_tempDirectory), activityId(_activityId), stable(_stable)
    {
        rowManager.addRowBuffer(this);
    }
    ~CSpillingSortAlgorithm()
    {
        rowManager.removeRowBuffer(this);
        diskReader.clear();
    }

    virtual void sortRows(void * * rows, size_t numRows, ICompare & compare, void * * stableTemp) = 0;

    virtual void prepare(IInputBase *input)
    {
        loop
        {
            const void * next = input->nextInGroup();
            if (!next)
                break;
            if (!rowsToSort.append(next))
            {
                {
                    roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
                    //We should have been called back to free any committed rows, but occasionally it may not (e.g., if
                    //the problem is global memory is exhausted) - in which case force a spill here (but add any pending
                    //rows first).
                    if (rowsToSort.numCommitted() != 0)
                    {
                        rowsToSort.flush();
                        spillRows();
                    }
                    //Ensure new rows are written to the head of the array.  It needs to be a separate call because
                    //spillRows() cannot shift active row pointer since it can be called from any thread
                    rowsToSort.flush();
                }

                if (!rowsToSort.append(next))
                {
                    ReleaseRoxieRow(next);
                    throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Insufficient memory to append sort row");
                }
            }
        }
        rowsToSort.flush();

        roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
        if (diskMerger)
        {
            spillRows();
            rowsToSort.kill();
            diskReader.setown(diskMerger->merge(compare));
        }
        else
        {
            sortCommitted();
            sorted.transferFrom(rowsToSort);
        }
    }

    virtual const void *next()
    {
        if(diskReader)
            return diskReader->nextRow();
        return sorted.dequeue();
    }

    virtual void reset()
    {
        //MORE: This could transfer any row pointer from sorted back to rowsToSort. It would trade
        //fewer heap allocations with not freeing up the memory from large group sorts.
        rowsToSort.clearRows();
        sorted.kill();
        //Disk reader must be cleared before the merger - or the files may still be locked.
        diskReader.clear();
        diskMerger.clear();
    }

//interface roxiemem::IBufferedRowCallback
    virtual unsigned getSpillCost() const
    {
        //Spill global sorts before grouped sorts
        if (rowMeta->isGrouped())
            return 20;
        return 10;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        roxiemem::RoxieOutputRowArrayLock block(rowsToSort);
        return spillRows();
    }

protected:
    void sortCommitted()
    {
        unsigned numRows = rowsToSort.numCommitted();
        if (numRows)
        {
            void ** rows = const_cast<void * *>(rowsToSort.getBlock(numRows));
            //MORE: Should this be parallel?  Should that be dependent on whether it is grouped?  Should be a hint.
            if (stable)
            {
                MemoryAttr tempAttr(numRows*sizeof(void **)); // Temp storage for stable sort. This should probably be allocated from roxiemem
                void **temp = (void **) tempAttr.bufferBase();
                sortRows(rows, numRows, *compare, temp);
            }
            else
                sortRows(rows, numRows, *compare, NULL);
        }
    }
    bool spillRows()
    {
        unsigned numRows = rowsToSort.numCommitted();
        if (numRows == 0)
            return false;

        sortCommitted();
        const void * * rows = rowsToSort.getBlock(numRows);

        Owned<IRowWriter> out = queryMerger()->createWriteBlock();
        for (unsigned i= 0; i < numRows; i++)
        {
            out->putRow(rows[i]);
        }
        rowsToSort.noteSpilled(numRows);
        return true;
    }

    IDiskMerger * queryMerger()
    {
        if (!diskMerger)
        {
            unsigned __int64 seq = (memsize_t)this ^ get_cycles_now();
            StringBuffer spillBasename;
            spillBasename.append(tempDirectory).append(PATHSEPCHAR).appendf("spill_sort_%" I64F "u", seq);
            Owned<IRowLinkCounter> linker = new RoxieRowLinkCounter();
            Owned<IRowInterfaces> rowInterfaces = createRowInterfaces(rowMeta, activityId, ctx);
            diskMerger.setown(createDiskMerger(rowInterfaces, linker, spillBasename));
        }
        return diskMerger;
    }
};


class CSpillingQuickSortAlgorithm : public CSpillingSortAlgorithm
{
public:
    CSpillingQuickSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _stable)
        : CSpillingSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, _stable)
    {
    }

    virtual void sortRows(void * * rows, size_t numRows, ICompare & compare, void * * stableTemp)
    {
        if (stableTemp)
            qsortvecstableinplace(rows, numRows, compare, stableTemp);
        else
            qsortvec(rows, numRows, compare);
    }
};


class CSpillingMergeSortAlgorithm : public CSpillingSortAlgorithm
{
public:
    CSpillingMergeSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _parallel)
        : CSpillingSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, true)
    {
        parallel = _parallel;
    }

    virtual void sortRows(void * * rows, size_t numRows, ICompare & compare, void * * stableTemp)
    {
        if (parallel)
            parmsortvecstableinplace(rows, numRows, compare, stableTemp);
        else
            msortvecstableinplace(rows, numRows, compare, stableTemp);
    }
protected:
    bool parallel;
};


extern ISortAlgorithm *createQuickSortAlgorithm(ICompare *_compare)
{
    return new CQuickSortAlgorithm(_compare);
}

extern ISortAlgorithm *createStableQuickSortAlgorithm(ICompare *_compare)
{
    return new CStableQuickSortAlgorithm(_compare);
}

extern ISortAlgorithm *createInsertionSortAlgorithm(ICompare *_compare, roxiemem::IRowManager *_rowManager, unsigned _activityId)
{
    return new CInsertionSortAlgorithm(_compare, _rowManager, _activityId);
}

extern ISortAlgorithm *createHeapSortAlgorithm(ICompare *_compare)
{
    return new CHeapSortAlgorithm(_compare);
}

extern ISortAlgorithm *createMergeSortAlgorithm(ICompare *_compare)
{
    return new CMergeSortAlgorithm(_compare);
}

extern ISortAlgorithm *createParallelMergeSortAlgorithm(ICompare *_compare)
{
    return new CParallelMergeSortAlgorithm(_compare);
}

extern ISortAlgorithm *createSpillingQuickSortAlgorithm(ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId, bool _stable)
{
    return new CSpillingQuickSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, _stable);
}

extern ISortAlgorithm *createSortAlgorithm(RoxieSortAlgorithm _algorithm, ICompare *_compare, roxiemem::IRowManager &_rowManager, IOutputMetaData * _rowMeta, ICodeContext *_ctx, const char *_tempDirectory, unsigned _activityId)
{
    switch (_algorithm)
    {
    case heapSortAlgorithm:
        return createHeapSortAlgorithm(_compare);
    case insertionSortAlgorithm:
        return createInsertionSortAlgorithm(_compare, &_rowManager, _activityId);
    case quickSortAlgorithm:
        return createQuickSortAlgorithm(_compare);
    case stableQuickSortAlgorithm:
        return createStableQuickSortAlgorithm(_compare);
    case spillingQuickSortAlgorithm:
    case stableSpillingQuickSortAlgorithm:
        return createSpillingQuickSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, _algorithm==stableSpillingQuickSortAlgorithm);
    case mergeSortAlgorithm:
        return new CMergeSortAlgorithm(_compare);
    case parallelMergeSortAlgorithm:
        return new CParallelMergeSortAlgorithm(_compare);
    case spillingMergeSortAlgorithm:
        return new CSpillingMergeSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, false);
    case spillingParallelMergeSortAlgorithm:
        return new CSpillingMergeSortAlgorithm(_compare, _rowManager, _rowMeta, _ctx, _tempDirectory, _activityId, true);
    default:
        break;
    }
    throwUnexpected();
}

//===================================================

CSafeSocket::CSafeSocket(ISocket *_sock)
{
    httpMode = false;
    mlFmt = MarkupFmt_Unknown;
    sent = 0; 
    heartbeat = false; 
    sock.setown(_sock);
}

CSafeSocket::~CSafeSocket()
{
    sock.clear();
    ForEachItemIn(idx, queued)
    {
        free(queued.item(idx));
    }
    queued.kill();
    lengths.kill();
}

unsigned CSafeSocket::bytesOut() const
{
    return sent;
}

bool CSafeSocket::checkConnection() const
{
    if (sock)
        return sock->check_connection();
    else
        return false;
}

size32_t CSafeSocket::write(const void *buf, size32_t size, bool takeOwnership)
{
    CriticalBlock c(crit); // NOTE: anyone needing to write multiple times without interleave should have already locked this. We lock again for the simple cases.
    OwnedMalloc<void> ownedBuffer;
    if (takeOwnership)
        ownedBuffer.setown((void *) buf);
    if (!size)
        return 0;
    try
    {
        if (httpMode)
        {
            if (!takeOwnership)
            {
                ownedBuffer.setown(malloc(size));
                if (!ownedBuffer)
                    throw MakeStringException(THORHELPER_INTERNAL_ERROR, "Out of memory in CSafeSocket::write (requesting %d bytes)", size);
                memcpy(ownedBuffer, buf, size);
            }
            queued.append(ownedBuffer.getClear());
            lengths.append(size);
            return size;
        }
        else
        {
            sent += size;
            size32_t written = sock->write(buf, size);
            return written;
        }
    }
    catch(...)
    {
        heartbeat = false;
        throw;
    }
}

bool CSafeSocket::readBlock(MemoryBuffer &ret, unsigned timeout, unsigned maxBlockSize)
{
    // MORE - this is still not good enough as we could get someone else's block if there are multiple input datasets
    CriticalBlock c(crit);
    try
    {
        unsigned bytesRead;
        unsigned len;
        try
        {
            sock->read(&len, sizeof (len), sizeof (len), bytesRead, timeout);
        }
        catch (IJSOCK_Exception *E)
        {
            if (E->errorCode()==JSOCKERR_graceful_close)
            {
                E->Release();
                return false;
            }
            throw;
        }
        assertex(bytesRead == sizeof(len));
        _WINREV(len);
        if (len & 0x80000000)
            len ^= 0x80000000;
        if (len > maxBlockSize)
            throw MakeStringException(THORHELPER_DATA_ERROR, "Maximum block size (%d bytes) exceeded (missing length prefix?)", maxBlockSize);
        if (len)
        {
            unsigned bytesRead;
            sock->read(ret.reserveTruncate(len), len, len, bytesRead, timeout);
        }
        return len != 0;
    }
    catch(...)
    {
        heartbeat = false;
        throw;
    }
}

int readHttpHeaderLine(IBufferedSocket *linereader, char *headerline, unsigned maxlen)
{
    Owned<IMultiException> me = makeMultiException("roxie");
    int bytesread = linereader->readline(headerline, maxlen, true, me);
    if (me->ordinality())
        throw me.getClear();
    if(bytesread <= 0 || bytesread > maxlen)
        throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Bad Request");
    return bytesread;
}

bool CSafeSocket::readBlock(StringBuffer &ret, unsigned timeout, HttpHelper *pHttpHelper, bool &continuationNeeded, bool &isStatus, unsigned maxBlockSize)
{
    continuationNeeded = false;
    isStatus = false;
    CriticalBlock c(crit);
    try
    {
        unsigned bytesRead;
        unsigned len = 0;
        try
        {
            sock->read(&len, sizeof (len), sizeof (len), bytesRead, timeout);
        }
        catch (IJSOCK_Exception *E)
        {
            if (E->errorCode()==JSOCKERR_graceful_close)
            {
                E->Release();
                return false;
            }
            throw;
        }
        assertex(bytesRead == sizeof(len));
        unsigned left = 0;
        char *buf;

        if (pHttpHelper != NULL && strncmp((char *)&len, "POST", 4) == 0)
        {
#define MAX_HTTP_HEADERSIZE 8000
            pHttpHelper->setIsHttp(true);
            char header[MAX_HTTP_HEADERSIZE + 1]; // allow room for \0
            sock->read(header, 1, MAX_HTTP_HEADERSIZE, bytesRead, timeout);
            header[bytesRead] = 0;
            char *payload = strstr(header, "\r\n\r\n");
            if (payload)
            {
                *payload = 0;
                payload += 4;
                char *str;

                pHttpHelper->parseHTTPRequestLine(header);

                // capture authentication token
                if ((str = strstr(header, "Authorization: Basic ")) != NULL)
                    pHttpHelper->setAuthToken(str+21);

                // capture content type
                if ((str = strstr(header, "Content-Type: ")) != NULL)
                    pHttpHelper->setContentType(str+14);

                // determine payload length
                str = strstr(header, "Content-Length: ");
                if (str)
                {
                    len = atoi(str + strlen("Content-Length: "));
                    buf = ret.reserveTruncate(len);
                    left = len - (bytesRead - (payload - header));
                    if (len > left)
                        memcpy(buf, payload, len - left); 
                }
                else
                    left = len = 0;
            }
            else
                left = len = 0;

            if (!len)
                throw MakeStringException(THORHELPER_DATA_ERROR, "Badly formed HTTP header");
        }
        else if (pHttpHelper != NULL && strncmp((char *)&len, "GET", 3) == 0)
        {
#define MAX_HTTP_GET_LINE 16000 //arbitrary per line limit, most web servers are lower, but urls for queries can be complex..
                pHttpHelper->setIsHttp(true);
                char headerline[MAX_HTTP_GET_LINE + 1];
                Owned<IBufferedSocket> linereader = createBufferedSocket(sock);

                int bytesread = readHttpHeaderLine(linereader, headerline, MAX_HTTP_GET_LINE);
                pHttpHelper->parseHTTPRequestLine(headerline);

                bytesread = readHttpHeaderLine(linereader, headerline, MAX_HTTP_GET_LINE);
                while(bytesread >= 0 && *headerline && *headerline!='\r')
                {
                    // capture authentication token
                    if (!strnicmp(headerline, "Authorization: Basic ", 21))
                        pHttpHelper->setAuthToken(headerline+21);
                    bytesread = readHttpHeaderLine(linereader, headerline, MAX_HTTP_GET_LINE);
                }

                StringBuffer queryName;
                const char *target = pHttpHelper->queryTarget();
                if (!target || !*target)
                    throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Target not specified");
                else if (!pHttpHelper->validateTarget(target))
                    throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Target not found");
                const char *query = pHttpHelper->queryQueryName();
                if (!query || !*query)
                    throw MakeStringException(THORHELPER_DATA_ERROR, "HTTP-GET Query not specified");

                queryName.append(query);
                Owned<IPropertyTree> req = createPTreeFromHttpParameters(queryName, pHttpHelper->queryUrlParameters(), true, pHttpHelper->queryContentFormat()==MarkupFmt_JSON);
                if (pHttpHelper->queryContentFormat()==MarkupFmt_JSON)
                    toJSON(req, ret);
                else
                    toXML(req, ret);
                return true;
        }
        else if (strnicmp((char *)&len, "STAT", 4) == 0)
            isStatus = true;
        else
        {
            _WINREV(len);
            if (len & 0x80000000)
            {
                len ^= 0x80000000;
                continuationNeeded = true;
            }
            if (len > maxBlockSize)
                throw MakeStringException(THORHELPER_DATA_ERROR, "Maximum block size (%d bytes) exceeded (missing length prefix?)", maxBlockSize);
            left = len;

            if (len)
                buf = ret.reserveTruncate(len);
        }

        if (left)
        {
            sock->read(buf + (len - left), left, left, bytesRead, timeout);
        }

        return len != 0;
    }
    catch (IException *E)
    {
        if (pHttpHelper)
            checkSendHttpException(*pHttpHelper, E, NULL);
        heartbeat = false;
        throw;
    }
    catch (...)
    {
        heartbeat = false;
        throw;
    }
}

void CSafeSocket::setHttpMode(const char *queryName, bool arrayMode, HttpHelper &httphelper)
{
    CriticalBlock c(crit); // Should not be needed
    httpMode = true;
    mlFmt = httphelper.queryContentFormat();
    heartbeat = false;
    assertex(contentHead.length()==0 && contentTail.length()==0);
    if (mlFmt==MarkupFmt_JSON)
    {
        contentHead.set("{");
        contentTail.set("}");
    }
    else
    {
        StringAttrBuilder headText(contentHead), tailText(contentTail);
        if (httphelper.getUseEnvelope())
            headText.append(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                "<soap:Body>");
        if (arrayMode)
        {
            headText.append("<").append(queryName).append("ResponseArray>");
            tailText.append("</").append(queryName).append("ResponseArray>");
        }
        if (httphelper.getUseEnvelope())
            tailText.append("</soap:Body></soap:Envelope>");
    }
}

void CSafeSocket::checkSendHttpException(HttpHelper &httphelper, IException *E, const char *queryName)
{
    if (!httphelper.isHttp())
        return;
    if (httphelper.queryContentFormat()==MarkupFmt_JSON)
        sendJsonException(E, queryName);
    else
        sendSoapException(E, queryName);
}

void CSafeSocket::sendSoapException(IException *E, const char *queryName)
{
    try
    {
        if (!queryName)
            queryName = "Unknown"; // Exceptions when parsing query XML can leave queryName unset/unknowable....

        StringBuffer response;
        response.append("<").append(queryName).append("Response");
        response.append(" xmlns=\"urn:hpccsystems:ecl:").appendLower(strlen(queryName), queryName).append("\">");
        response.appendf("<Results><Result><Exception><Source>Roxie</Source><Code>%d</Code>", E->errorCode());
        response.append("<Message>");
        StringBuffer s;
        E->errorMessage(s);
        encodeXML(s.str(), response);
        response.append("</Message></Exception></Result></Results>");
        response.append("</").append(queryName).append("Response>");
        write(response.str(), response.length());
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        EE->errorMessage(error);
        DBGLOG("%s", error.str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

void CSafeSocket::sendJsonException(IException *E, const char *queryName)
{
    try
    {
        if (!queryName)
            queryName = "Unknown"; // Exceptions when parsing query XML can leave queryName unset/unknowable....

        StringBuffer response;
        appendfJSONName(response, "%sResponse", queryName).append(" {");
        appendJSONName(response, "Results").append(" {");
        appendJSONName(response, "Exception").append(" [{");
        appendJSONValue(response, "Source", "Roxie");
        appendJSONValue(response, "Code", E->errorCode());
        StringBuffer s;
        appendJSONValue(response, "Message", E->errorMessage(s).str());
        response.append("}]}}");
        write(response.str(), response.length());
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        DBGLOG("%s", EE->errorMessage(error).str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

void CSafeSocket::setHeartBeat()
{
    CriticalBlock c(crit);
    heartbeat = true;
}

bool CSafeSocket::sendHeartBeat(const IContextLogger &logctx)
{
    if (heartbeat)
    {
        StringBuffer s;
        bool rval = false;

        unsigned replyLen = 5;
        unsigned rev = replyLen | 0x80000000; // make it a blocked msg
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        s.append('H');
        rev = (unsigned) time(NULL);
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        
        try
        {
            CriticalBlock c(crit);
            sock->write(s.str(), replyLen + sizeof(rev));
            rval = true;
        }
        catch (IException * E)
        {
            StringBuffer error("HeartBeat write failed with exception: ");
            E->errorMessage(error);
            logctx.CTXLOG("%s", error.str());
            E->Release();
        }
        catch(...)
        {
            logctx.CTXLOG("HeartBeat write failed (Unknown exception)");
        }
        return rval;
    }
    else
        return true;
};

void CSafeSocket::flush()
{
    if (httpMode)
    {
        unsigned length = contentHead.length() + contentTail.length();
        ForEachItemIn(idx, lengths)
            length += lengths.item(idx);

        StringBuffer header;
        header.append("HTTP/1.0 200 OK\r\n");
        header.append("Content-Type: ").append(mlFmt == MarkupFmt_JSON ? "application/json" : "text/xml").append("\r\n");
        header.append("Content-Length: ").append(length).append("\r\n\r\n");


        CriticalBlock c(crit); // should not be anyone writing but better to be safe
        if (traceLevel > 5)
            DBGLOG("Writing HTTP header length %d to HTTP socket", header.length());
        sock->write(header.str(), header.length());
        sent += header.length();
        if (traceLevel > 5)
            DBGLOG("Writing content head length %d to HTTP socket", contentHead.length());
        sock->write(contentHead.str(), contentHead.length());
        sent += contentHead.length();
        ForEachItemIn(idx2, queued)
        {
            unsigned length = lengths.item(idx2);
            if (traceLevel > 5)
                DBGLOG("Writing block length %d to HTTP socket", length);
            sock->write(queued.item(idx2), length);
            sent += length;
        }
        if (traceLevel > 5)
            DBGLOG("Writing content tail length %d to HTTP socket", contentTail.length());
        sock->write(contentTail.str(), contentTail.length());
        sent += contentTail.length();
        if (traceLevel > 5)
            DBGLOG("Total written %d", sent);
    }
}

void CSafeSocket::sendException(const char *source, unsigned code, const char *message, bool isBlocked, const IContextLogger &logctx)
{
    try
    {
        FlushingStringBuffer response(this, isBlocked, MarkupFmt_XML, false, httpMode, logctx);
        response.startDataset("Exception", NULL, (unsigned) -1);
        response.appendf("<Source>%s</Source><Code>%d</Code>", source, code);
        response.append("<Message>");
        response.encodeString(message, strlen(message));
        response.append("</Message>");
    }
    catch(IException *EE)
    {
        StringBuffer error("While reporting exception: ");
        EE->errorMessage(error);
        logctx.CTXLOG("%s", error.str());
        EE->Release();
    }
#ifndef _DEBUG
    catch(...) {}
#endif
}

//==============================================================================================================

#define RESULT_FLUSH_THRESHOLD 10000u

#ifdef _DEBUG
#define HTTP_SPLIT_THRESHOLD 100u
#define HTTP_SPLIT_RESERVE 200u
#else
#define HTTP_SPLIT_THRESHOLD 64000u
#define HTTP_SPLIT_RESERVE 65535u
#endif
interface IXmlStreamFlusher;

//==============================================================================================================

bool FlushingStringBuffer::needsFlush(bool closing)
{
    if (isBlocked || closing) // can't flush unblocked. MORE - may need to break it up though.... 
    {
        size32_t len = s.length() - emptyLength;
        return len > (closing ? 0 : RESULT_FLUSH_THRESHOLD);
    }
    else
        return false; // MORE - if there is a single result, it can be flushed (actually, can flush anytime all prior results have been closed)
}

void FlushingStringBuffer::startBlock()
{
    size32_t len = 0;
    s.clear();
    if (!isHttp)
        append(sizeof(size32_t), (char *) &len);
    rowCount = 0;
    if (isBlocked)
    {
        s.append('R');
        unsigned rev = sequenceNumber++;
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev);
        rev = rowCount;
        _WINREV(rev);
        s.append(sizeof(rev), (char *) &rev); // NOTE -  need to patch up later. At this point it is 0.
        s.append(strlen(name)+1, name);
    }
    emptyLength = s.length();
    // MORE - should probably pre-reserve string at RESULT_FLUSH_THRESHOLD plus a bit
}

FlushingStringBuffer::FlushingStringBuffer(SafeSocket *_sock, bool _isBlocked, TextMarkupFormat _mlFmt, bool _isRaw, bool _isHttp, const IContextLogger &_logctx)
  : sock(_sock), isBlocked(_isBlocked), mlFmt(_mlFmt), isRaw(_isRaw), isHttp(_isHttp), logctx(_logctx)
{
    sequenceNumber = 0;
    rowCount = 0;
    isSoap = false;
    isEmpty = true;
    extend = false;
    trim = false;
    emptyLength = 0;
    tagClosed = true;
}

FlushingStringBuffer::~FlushingStringBuffer()
{
    try
    {
        flush(true);
    }
    catch (IException *E)
    {
        // Ignore any socket errors that we get at termination - nothing we can do about them anyway...
        E->Release();
    }
    catch(...)
    {
    }
    ForEachItemIn(idx, queued)
    {
        free(queued.item(idx));
    }
}

//void FlushingStringBuffer::append(char data)
//{
    //append(1, &data);
//}

void FlushingStringBuffer::append(const char *data)
{
    append(strlen(data), data);
}

void FlushingStringBuffer::append(double data)
{
    if (isRaw)
        append(sizeof(data), (char *)&data);
    else
    {
        StringBuffer v;
        v.append(data);
        append(v.length(), v.str());
    }
}

void FlushingStringBuffer::append(unsigned len, const char *data)
{
    try
    {
        CriticalBlock b(crit);
        s.append(len, data);
    }
    catch (IException *E)
    {
        logctx.logOperatorException(E, __FILE__, __LINE__, "FlushingStringBuffer::append");
        throw;
    }
}

void FlushingStringBuffer::appendf(const char *format, ...)
{
    StringBuffer t;
    va_list args;
    va_start(args, format);
    t.valist_appendf(format, args);
    va_end(args);
    append(t.length(), t.str());
}

void FlushingStringBuffer::encodeString(const char *x, unsigned len, bool utf8)
{
    if (mlFmt==MarkupFmt_XML)
    {
        StringBuffer t;
        ::encodeXML(x, t, 0, len, utf8);
        append(t.length(), t.str());
    }
    else
        append(len, x);
}

void FlushingStringBuffer::encodeData(const void *data, unsigned len)
{
    static char hexchar[] = "0123456789ABCDEF";
    if (isRaw)
        append(len, (const char *) data);
    else
    {
        const byte *field = (const byte *) data;
        for (unsigned i = 0; i < len; i++)
        {
            append(hexchar[field[i] >> 4]);
            append(hexchar[field[i] & 0x0f]);
        }
    }
}

void FlushingStringBuffer::addPayload(StringBuffer &s, unsigned int reserve)
{
    if (!s.length())
        return;
    lengths.append(s.length());
    queued.append(s.detach());
    if (reserve)
        s.ensureCapacity(reserve);
}

void FlushingStringBuffer::flushXML(StringBuffer &current, bool isClosing)
{
    CriticalBlock b(crit);
    if (isHttp) // we don't do any chunking for non-HTTP yet
    {
        if (isClosing || current.length() > HTTP_SPLIT_THRESHOLD)
        {
            addPayload(s, HTTP_SPLIT_RESERVE);
            addPayload(current, isClosing ? 0 : HTTP_SPLIT_RESERVE);
        }
    }
    else if (isClosing)
        append(current.length(), current.str());
}

void FlushingStringBuffer::flush(bool closing)
{
    CriticalBlock b(crit);
    if (closing && tail.length())
    {
        s.append(tail);
        tail.clear();
    }
    if (isHttp)
    {
        if (!closing && s.length() > HTTP_SPLIT_THRESHOLD)
            addPayload(s, HTTP_SPLIT_RESERVE);
    }
    else if (needsFlush(closing))
    {
        // MORE - if not blocked we can get very large blocks.
        assertex(s.length() > sizeof(size32_t));
        unsigned replyLen = s.length() - sizeof(size32_t);
        unsigned revLen = replyLen | ((isBlocked)?0x80000000:0);
        _WINREV(revLen);
        if (logctx.queryTraceLevel() > 1)
        {
            if (isBlocked)
                logctx.CTXLOG("Sending reply: Sending blocked %s data", getFormatName(mlFmt));
            else
#ifdef _DEBUG
                logctx.CTXLOG("Sending reply length %d: %.1024s", (unsigned) (s.length() - sizeof(size32_t)), s.str()+sizeof(size32_t));
#else
                logctx.CTXLOG("Sending reply length %d: %.40s", (unsigned) (s.length() - sizeof(size32_t)), s.str()+sizeof(size32_t));
#endif
        }
        *(size32_t *) s.str() = revLen;
        if (isBlocked)
        {
            unsigned revRowCount = rowCount;
            _WINREV(revRowCount);
            *(size32_t *) (s.str()+9) = revRowCount;
        }
        if (logctx.queryTraceLevel() > 9)
            logctx.CTXLOG("writing block size %d to socket", replyLen);
        try
        {
            if (sock)
            {
                if (isHttp)
                    sock->write(s.str()+sizeof(revLen), replyLen);
                else
                    sock->write(s.str(), replyLen + sizeof(revLen));
            }
            else
                fwrite(s.str()+sizeof(revLen), replyLen, 1, stdout);
        }
        catch (...)
        {
            if (logctx.queryTraceLevel() > 9)
                logctx.CTXLOG("Exception caught FlushingStringBuffer::flush");

            s.clear();
            emptyLength = 0;
            throw;
        }

        if (logctx.queryTraceLevel() > 9)
            logctx.CTXLOG("wrote block size %d to socket", replyLen);

        if (closing)
        {
            s.clear();
            emptyLength = 0;
        }
        else
            startBlock();
    }
}

void *FlushingStringBuffer::getPayload(size32_t &length)
{
    assertex(isHttp);
    CriticalBlock b(crit);
    if (queued.ordinality())
    {
        length = lengths.item(0);
        void *ret = queued.item(0);
        queued.remove(0);
        lengths.remove(0);
        return ret;
    }
    length = s.length();
    return length ? s.detach() : NULL;
}

void FlushingStringBuffer::startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend, const IProperties *xmlns)
{
    CriticalBlock b(crit);
    extend = _extend;
    if (isEmpty || !extend)
    {
        name.clear().append(resultName ? resultName : elementName);
        sequenceNumber = 0;
        startBlock();
        if (!isBlocked)
        {
            if (mlFmt==MarkupFmt_XML)
            {
                s.append('<').append(elementName);
                if (isSoap && (resultName || (sequence != (unsigned) -1)))
                {
                    s.append(" xmlns=\'urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.str()).append(":result:");
                    if (resultName && *resultName)
                        s.appendLower(strlen(resultName), resultName).append('\'');
                    else
                        s.append("result_").append(sequence+1).append('\'');
                    if (xmlns)
                    {
                        Owned<IPropertyIterator> it = const_cast<IProperties*>(xmlns)->getIterator(); //should fix IProperties to be const friendly
                        ForEach(*it)
                        {
                            const char *name = it->getPropKey();
                            s.append(' ');
                            if (!streq(name, "xmlns"))
                                s.append("xmlns:");
                            s.append(name).append("='");
                            encodeUtf8XML(const_cast<IProperties*>(xmlns)->queryProp(name), s);
                            s.append("'");
                        }
                    }
                }
                if (resultName && *resultName)
                    s.appendf(" name='%s'",resultName);
                else if (sequence != (unsigned) -1)
                    s.appendf(" name='Result %d'",sequence+1);
                s.append(">\n");
                tail.clear().appendf("</%s>\n", elementName);
            }
        }
        isEmpty = false;
    }
}

void FlushingStringBuffer::startScalar(const char *resultName, unsigned sequence)
{
    if (s.length())
        throw MakeStringException(0, "Attempt to output scalar ('%s',%d) multiple times", resultName ? resultName : "", (int)sequence);

    CriticalBlock b(crit);
    name.clear().append(resultName ? resultName : "Dataset");

    sequenceNumber = 0;
    startBlock();
    if (!isBlocked)
    {
        if (mlFmt==MarkupFmt_XML)
        {
            tail.clear();
            s.append("<Dataset");
            if (isSoap && (resultName || (sequence != (unsigned) -1)))
            {
                s.append(" xmlns=\'urn:hpccsystems:ecl:").appendLower(queryName.length(), queryName.str()).append(":result:");
                if (resultName && *resultName)
                    s.appendLower(strlen(resultName), resultName).append('\'');
                else
                    s.append("result_").append(sequence+1).append('\'');
            }
            if (resultName && *resultName)
                s.appendf(" name='%s'>\n",resultName);
            else
                s.appendf(" name='Result %d'>\n",sequence+1);
            s.append(" <Row>");
            if (resultName && *resultName)
            {
                s.appendf("<%s>", resultName);
                tail.appendf("</%s>", resultName);
            }
            else
            {
                s.appendf("<Result_%d>", sequence+1);
                tail.appendf("</Result_%d>", sequence+1);
            }
            tail.appendf("</Row>\n</Dataset>\n");
        }
        else if (!isRaw)
        {
            tail.clear().append('\n');
        }
    }
}

void FlushingStringBuffer::setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size)
{
    startScalar(resultName, sequence);
    s.append(value);
}
void FlushingStringBuffer::setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size)
{
    startScalar(resultName, sequence);
    s.append(value);
}

void FlushingStringBuffer::incrementRowCount()
{
    CriticalBlock b(crit);
    rowCount++;
}

void FlushingJsonBuffer::append(double data)
{
    CriticalBlock b(crit);
    appendJSONRealValue(s, NULL, data);
}

void FlushingJsonBuffer::encodeString(const char *x, unsigned len, bool utf8)
{
    CriticalBlock b(crit);
    appendJSONStringValue(s, NULL, len, x, true);
}

void FlushingJsonBuffer::encodeData(const void *data, unsigned len)
{
    CriticalBlock b(crit);
    appendJSONDataValue(s, NULL, len, data);
}

void FlushingJsonBuffer::startDataset(const char *elementName, const char *resultName, unsigned sequence, bool _extend, const IProperties *xmlns)
{
    CriticalBlock b(crit);
    extend = _extend;
    if (isEmpty || !extend)
    {
        name.clear().append(resultName ? resultName : elementName);
        sequenceNumber = 0;
        startBlock();
        if (!isBlocked)
        {
            StringBuffer seqName;
            if (!resultName || !*resultName)
                resultName = seqName.appendf("result_%d", sequence+1).str();
            appendJSONName(s, resultName).append('{');
            tail.set("}");
        }
        isEmpty = false;
    }
}

void FlushingJsonBuffer::startScalar(const char *resultName, unsigned sequence)
{
    if (s.length())
        throw MakeStringException(0, "Attempt to output scalar ('%s',%d) multiple times", resultName ? resultName : "", (int)sequence);

    CriticalBlock b(crit);
    name.set(resultName ? resultName : "Dataset");

    sequenceNumber = 0;
    startBlock();
    if (!isBlocked)
    {
        StringBuffer seqName;
        if (!resultName || !*resultName)
            resultName = seqName.appendf("Result_%d", sequence+1).str();
        appendJSONName(s, resultName).append('{');
        appendJSONName(s, "Row").append("[{");
        appendJSONName(s, resultName);
        tail.set("}]}");
    }
}

void FlushingJsonBuffer::setScalarInt(const char *resultName, unsigned sequence, __int64 value, unsigned size)
{
    startScalar(resultName, sequence);
    if (size < 7) //JavaScript only supports 53 significant bits
        s.append(value);
    else
        s.append('"').append(value).append('"');
}

void FlushingJsonBuffer::setScalarUInt(const char *resultName, unsigned sequence, unsigned __int64 value, unsigned size)
{
    startScalar(resultName, sequence);
    if (size < 7) //JavaScript doesn't support unsigned, and only supports 53 significant bits
        s.append(value);
    else
        s.append('"').append(value).append('"');
}

//=====================================================================================================

ClusterWriteHandler::ClusterWriteHandler(char const * _logicalName, char const * _activityType)
    : logicalName(_logicalName), activityType(_activityType)
{
    makePhysicalPartName(logicalName.get(), 1, 1, physicalName, false);
    splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
}

void ClusterWriteHandler::addCluster(char const * cluster)
{
    Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
    if (!group)
        throw MakeStringException(0, "Unknown cluster %s while writing file %s", cluster, logicalName.get());
    if (group->isMember())
    {
        if (localCluster)
            throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s", cluster,
                    logicalName.get());
        localClusterName.set(cluster);
        localCluster.set(group);
    }
    else
    {
        ForEachItemIn(idx, remoteNodes)
        {
            Owned<INode> other = remoteNodes.item(idx).getNode(0);
            if (group->isMember(other))
                throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                        cluster, logicalName.get());
        }
        remoteNodes.append(*group.getClear());
        remoteClusters.append(cluster);
    }
}

void ClusterWriteHandler::getLocalPhysicalFilename(StringAttr & out) const
{
    if(localCluster.get())
        out.set(physicalName.str());
    else
        getTempFilename(out);
    PROGLOG("%s(CLUSTER) for logical filename %s writing to local file %s", activityType.get(), logicalName.get(), out.get());
}

void ClusterWriteHandler::splitPhysicalFilename(StringBuffer & dir, StringBuffer & base) const
{
    dir.append(physicalDir);
    base.append(physicalBase);
}

void ClusterWriteHandler::getTempFilename(StringAttr & out) const
{
    // Should be implemented by more derived (platform-specific) class, if needed
    throwUnexpected();
}

void ClusterWriteHandler::copyPhysical(IFile * source, bool noCopy) const
{
    RemoteFilename rdn, rfn;
    rdn.setLocalPath(physicalDir.str());
    rfn.setLocalPath(physicalName.str());
    ForEachItemIn(idx, remoteNodes)
    {
        rdn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
        rfn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
        Owned<IFile> targetdir = createIFile(rdn);
        Owned<IFile> target = createIFile(rfn);
        PROGLOG("%s(CLUSTER) for logical filename %s copying %s to %s", activityType.get(), logicalName.get(), source->queryFilename(), target->queryFilename());
        if(noCopy)
        {
            WARNLOG("Skipping remote copy due to debug option");
        }
        else
        {
            targetdir->createDirectory();
            copyFile(target, source);
        }
    }
}

void ClusterWriteHandler::setDescriptorParts(IFileDescriptor * desc, char const * basename, IPropertyTree * attrs) const
{
    if(!localCluster.get()&&(remoteNodes.ordinality()==0))
        throw MakeStringException(0, "Attempting to write file to no clusters");
    ClusterPartDiskMapSpec partmap; // will get this from group at some point
    desc->setNumParts(1);
    desc->setPartMask(basename);
    if (localCluster) 
        desc->addCluster(localClusterName,localCluster, partmap);
    ForEachItemIn(idx,remoteNodes) 
        desc->addCluster(remoteClusters.item(idx),&remoteNodes.item(idx), partmap);
    if (attrs) {
        // need to set part attr
        IPartDescriptor *partdesc = desc->queryPart(0);
        IPropertyTree &pprop = partdesc->queryProperties();
        // bit of a kludge (should really set properties *after* creating part rather than passing prop tree in)
        Owned<IAttributeIterator> ai = attrs->getAttributes();
        ForEach(*ai) 
            pprop.setProp(ai->queryName(),ai->queryValue());
    }
}
void ClusterWriteHandler::finish(IFile * file) const
{
    if(!localCluster.get())
    {
        PROGLOG("%s(CLUSTER) for logical filename %s removing temporary file %s", activityType.get(), logicalName.get(), file->queryFilename());
        file->remove();
    }
}

void ClusterWriteHandler::getClusters(StringArray &clusters) const
{
    if(localCluster)
        clusters.append(localClusterName);
    ForEachItemIn(c, remoteClusters)
        clusters.append(remoteClusters.item(c));
}

//=====================================================================================================

class COrderedOutputSerializer : implements IOrderedOutputSerializer, public CInterface
{
    class COrderedResult : public CInterface
    {
        bool closed;
        StringBuffer sb;
    public:
        IMPLEMENT_IINTERFACE;
        COrderedResult() : closed(false) {}
        bool flush(FILE * outFile, bool onlyClosed)
        {
            if (closed || !onlyClosed)
            {
                if (sb.length())
                {
                    ::fwrite(sb.str(), sb.length(), 1, outFile);
                    sb.clear();
                }
            }
            return closed;
        }
        size32_t printf(const char *format, va_list args)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to append to previously closed result in COrderedResult::printf");
            int prevLen = sb.length();
            sb.valist_appendf(format, args);
            return sb.length() - prevLen;
        }
        size32_t fwrite(const void * data, size32_t size, size32_t count)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to append to previously closed result in COrderedResult::fwrite");
            size32_t len = size * count;
            sb.append(len, (const char *)data);
            return len;
        }
        void close(bool nl)
        {
            if (closed)
                throw MakeStringException(0, "Attempting to reclose result in COrderedResult::close");
            if (nl)
                sb.append('\n');
            closed = true;
        }
    };

    CIArrayOf<COrderedResult> COrderedResultArr;
    int lastSeqFlushed;
    FILE * outFile;
    CriticalSection crit;

    COrderedResult * getResult(size32_t seq)
    {
        while ((int)COrderedResultArr.ordinality() < (seq+1))
            COrderedResultArr.append(*(new COrderedResult()));
        return &COrderedResultArr.item(seq);
    }

    void flushCurrent()//stream current sequence
    {
        COrderedResult &res = COrderedResultArr.item(lastSeqFlushed + 1);
        res.flush(outFile,false);
        fflush(outFile);
    }

    void flushCompleted(bool onlyClosed)//flush completed sequence(s)
    {
        int lastSeq = (int)COrderedResultArr.ordinality()-1;
        for (; lastSeqFlushed < lastSeq; lastSeqFlushed++)
        {
            COrderedResult &res = COrderedResultArr.item(lastSeqFlushed + 1);
            if (!res.flush(outFile,onlyClosed) && onlyClosed)
                break;
        }
        fflush(outFile);
    }
public:
    IMPLEMENT_IINTERFACE;
    COrderedOutputSerializer(FILE* _outFile) : lastSeqFlushed(-1), outFile(_outFile) {}
    ~COrderedOutputSerializer()
    {
        if (lastSeqFlushed != (COrderedResultArr.ordinality()-1))
            flushCompleted(false);
        COrderedResultArr.kill();
    }

    //IOrderedOutputSerializer
    size32_t fwrite(int seq, const void * data, size32_t size, size32_t count)  
    { 
        CriticalBlock c(crit);
        size32_t ret = getResult(seq)->fwrite(data,size, count);
        if (seq == (lastSeqFlushed + 1))
            flushCurrent();
        return ret;
    }
    size32_t printf(int seq, const char *format, ...) __attribute__((format(printf, 3, 4)))
    { 
        CriticalBlock c(crit);
        va_list args;
        va_start(args, format);
        int ret = getResult(seq)->printf(format, args);
        va_end(args);
        if (seq == (lastSeqFlushed + 1))
            flushCurrent();
        return ret;
    }
    void close(int seq, bool nl)
    {
        CriticalBlock c(crit);
        getResult(seq)->close(nl);
        if ( seq == (lastSeqFlushed+1) )
            flushCompleted(true);
    }
};

IOrderedOutputSerializer * createOrderedOutputSerializer(FILE * _outFile)
{
    return new COrderedOutputSerializer(_outFile);
}

//=====================================================================================================

StringBuffer & mangleHelperFileName(StringBuffer & out, const char * in, const char * wuid, unsigned int flags)
{
    out = in;
    if (flags & (TDXtemporary | TDXjobtemp))
        out.append("__").append(wuid);
    return out;
}

StringBuffer & mangleLocalTempFilename(StringBuffer & out, char const * in)
{
    char const * start = in;
    while(true)
    {
        char const * end = strstr(start, "::");
        if(end)
        {
            out.append(end-start, start).append("__scope__");
            start = end + 2;
        }
        else
        {
            out.append(start);
            break;
        }
    }
    return out;
}

static const char *skipLfnForeign(const char *lfn)
{
    // NOTE: The leading ~ and any leading spaces have already been stripped at this point
    const char *finger = lfn;
    if (strnicmp(finger, "foreign", 7)==0)
    {
        finger += 7;
        while (*finger == ' ')
            finger++;
        if (finger[0] == ':' && finger[1] == ':')
        {
            // foreign scope - need to strip off the ip and port (i.e. from here to the next ::)
            finger += 2;  // skip ::
            finger = strstr(finger, "::");
            if (finger)
            {
                finger += 2;
                while (*finger == ' ')
                    finger++;
                return finger;
            }
        }
    }
    return lfn;
}

StringBuffer & expandLogicalFilename(StringBuffer & logicalName, const char * fname, IConstWorkUnit * wu, bool resolveLocally, bool ignoreForeignPrefix)
{
    if (fname[0]=='~')
    {
        while (*fname=='~' || *fname==' ')
            fname++;
        if (ignoreForeignPrefix)
            fname = skipLfnForeign(fname);
        logicalName.append(fname);
    }
    else if (resolveLocally)
    {
        StringBuffer sb(fname);
        sb.replaceString("::",PATHSEPSTR);
        makeAbsolutePath(sb.str(), logicalName.clear());
    }
    else
    {
        SCMStringBuffer lfn;
        if (wu)
        {
            wu->getScope(lfn);
            if(lfn.length())
                logicalName.append(lfn.s).append("::");
        }
        logicalName.append(fname);
    }
    return logicalName;
}

//----------------------------------------------------------------------------------

void IRoxieContextLogger::CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    CTXLOGaeva(E, file, line, prefix, format, args);
    va_end(args);
}

void HttpHelper::parseURL()
{
    const char *start = url.str();
    while (isspace(*start))
        start++;
    if (*start=='/')
        start++;
    StringAttr path;
    const char *finger = strpbrk(start, "?");
    if (finger)
        path.set(start, finger-start);
    else
        path.set(start);
    if (path.length())
        pathNodes.appendList(path, "/");
    if (!finger)
        return;
    finger++;
    while (*finger)
    {
        StringBuffer s, prop, val;
        while (*finger && *finger != '&' && *finger != '=')
            s.append(*finger++);
        appendDecodedURL(prop, s.trim());
        if (!*finger || *finger == '&')
            val.set("1");
        else
        {
            s.clear();
            finger++;
            while (*finger && *finger != '&')
                s.append(*finger++);
            appendDecodedURL(val, s.trim());
        }
        if (prop.length())
            parameters->setProp(prop, val);
        if (*finger)
            finger++;
    }
}
