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

#include "platform.h"
#include <string.h>
#include <limits.h>
#include "jsort.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jset.hpp"
#include "jutil.hpp"

#ifdef _DEBUG
// #define PARANOID
//#define TESTPARSORT
//#define MCMERGESTATS
#endif

//#define PARANOID_PARTITION
//#define TRACE_PARTITION

#define PARALLEL_GRANULARITY 1024

static bool sortParallel(unsigned &numcpus)
{
    unsigned numCPUs = getAffinityCpus();
    if ((numcpus==0)||(numcpus>numCPUs))
        numcpus = numCPUs;
#ifdef TESTPARSORT
    numcpus = 2;
    return true;        // to test
#endif
    return (numcpus>1);
}


//define two variants of the same insertion function.
#define COMPARE(search,position)    compare(search,position)
#define INSERT(position,search)     memmove(position,search, width)

void * binary_add(const void *newitem, const void *base,
             size32_t nmemb, 
             size32_t width,
             int ( *compare)(const void *_e1, const void *_e2),
             bool * ItemAdded)
{
#include "jsort.inc"
}

#undef COMPARE
#undef INSERT

//---------------------------------------------------------------------------

#define COMPARE(search,position)    compare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define NEVER_ADD

extern jlib_decl void * binary_vec_find(const void *newitem, const void * * base,
                                        size32_t nmemb, 
                                        sortCompareFunction compare,
                                        bool * ItemAdded)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef NEVER_ADD
#undef INSERT
#undef COMPARE

//---------------------------------------------------------------------------

#define COMPARE(search,position)    compare.docompare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define NEVER_ADD

extern jlib_decl void * binary_vec_find(const void *newitem, const void * * base,
                                        size32_t nmemb, 
                                        ICompare & compare,
                                        bool * ItemAdded)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef NEVER_ADD
#undef INSERT
#undef COMPARE

//---------------------------------------------------------------------------

#define COMPARE(search,position)    compare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define ALWAYS_ADD

extern jlib_decl void * binary_vec_insert(const void *newitem, const void * * base,
                                          size32_t nmemb, 
                                          sortCompareFunction compare)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef ALWAYS_ADD
#undef INSERT
#undef COMPARE

#define COMPARE(search,position)    compare.docompare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define ALWAYS_ADD

extern jlib_decl void * binary_vec_insert(const void *newitem, const void * * base,
                                          size32_t nmemb, 
                                          ICompare const & compare)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef ALWAYS_ADD
#undef INSERT
#undef COMPARE

#define COMPARE(search,position)    compare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define ALWAYS_ADD
#define SEEK_LAST_MATCH

extern jlib_decl void * binary_vec_insert_stable(const void *newitem, const void * * base,
                                          size32_t nmemb, 
                                          sortCompareFunction compare)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef SEEK_LAST_MATCH
#undef ALWAYS_ADD
#undef INSERT
#undef COMPARE

#define COMPARE(search,position)    compare.docompare(search,*(const void * *)(position))
#define INSERT(position,search)     *(const void * *)(position) = search
#define ALWAYS_ADD
#define SEEK_LAST_MATCH

extern jlib_decl void * binary_vec_insert_stable(const void *newitem, const void * * base,
                                          size32_t nmemb, 
                                          ICompare const & compare)
{
#define width sizeof(void*)
#include "jsort.inc"
#undef width
}

#undef SEEK_LAST_MATCH
#undef ALWAYS_ADD
#undef INSERT
#undef COMPARE


//=========================================================================
// optimized quicksort for array of pointers to fixed size objects



typedef void *  ELEMENT;        
typedef void ** _VECTOR;   // bit messy but allow to be redefined later
#define VECTOR _VECTOR


static inline void swap(VECTOR a, VECTOR b)  { ELEMENT t = *a;  *a = *b; *b = t; }
#define SWAP swap


#define CMP(a,b)         memcmp(*(a),*(b),es)
#define MED3(a,b,c)      med3a(a,b,c,es)
#define RECURSE(a,b)     qsortvec(a, b, es)

static inline VECTOR med3a(VECTOR a, VECTOR b, VECTOR c, size32_t es)
{
  return CMP(a, b) < 0 ?
      (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a ))
    : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c ));
}


void qsortvec(void **a, size32_t n, size32_t es)
#include "jsort2.inc"
#undef CMP
#undef MED3
#undef RECURSE

//---------------------------------------------------------------------------

#define CMP(a,b)         (compare(*(a),*(b)))
#define MED3(a,b,c)      med3c(a,b,c,compare)
#define RECURSE(a,b)     qsortvec(a, b, compare)
static inline VECTOR med3c(VECTOR a, VECTOR b, VECTOR c, sortCompareFunction compare)
{
  return CMP(a, b) < 0 ?
      (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a ))
    : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c ));
}


void qsortvec(void **a, size32_t n, sortCompareFunction compare)
#include "jsort2.inc"

#undef CMP
#undef MED3
#undef RECURSE

#define CMP(a,b)         (compare.docompare(*(a),*(b)))
#define MED3(a,b,c)      med3ic(a,b,c,compare)
#define RECURSE(a,b)     qsortvec(a, b, compare)
static inline VECTOR med3ic(VECTOR a, VECTOR b, VECTOR c, const ICompare & compare)
{
  return CMP(a, b) < 0 ?
      (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a ))
    : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c ));
}


void qsortvec(void **a, size32_t n, const ICompare & compare)
#include "jsort2.inc"

// Parallel version (only 2 threads currently)

class cParQSortBase
{
    struct sJobItem
    {
        unsigned start;
        unsigned num;
    };

    NonReentrantSpinLock joblock;
    QueueOf<sJobItem,false> jobq;
    Semaphore jobqsem;
    unsigned waiting;
    unsigned numsubthreads;
    bool done;

    class cThread: public Thread
    {
        cParQSortBase *parent;
    public:
        cThread(cParQSortBase *_parent)
            : Thread("cParQSort")
        {
            parent = _parent;
        }
        int run()
        {
            parent->run();
            return 0;
        }
    } **threads;


    bool waitForWork(unsigned &s,unsigned &n)
    {
        NonReentrantSpinBlock block(joblock);
        while (!done) {
            sJobItem *qi = jobq.dequeue();
            if (qi) {
                s = qi->start;
                n = qi->num;
                delete qi;
                return true;
            }
            if (waiting==numsubthreads) { // well we know we are done and so are rest so exit
                done = true;
                jobqsem.signal(waiting);
                break;
            }
            waiting++;
            NonReentrantSpinUnblock unblock(joblock);
            jobqsem.wait();
        }
        s = 0; // remove uninitialised variable warnings
        n = 0;
        return false;
    }


public:

    cParQSortBase(unsigned _numsubthreads)
    {
        numsubthreads = _numsubthreads;
        done = false;
        waiting = 0;
        threads = new cThread*[numsubthreads];
        for (unsigned i=0;i<numsubthreads;i++) 
            threads[i] = new cThread(this);
    }

    ~cParQSortBase()
    {
        for (unsigned i=0;i<numsubthreads;i++)
            threads[i]->Release();
        delete [] threads;
    }

    void start()
    {
        for (unsigned i=0;i<numsubthreads;i++) 
            threads[i]->start();
    }

    void subsort(unsigned s, unsigned n)
    {
        do {
            sJobItem *qi;
            while (n>PARALLEL_GRANULARITY) {
                unsigned r1;
                unsigned r2;
                partition(s, n, r1, r2);
                unsigned n2 = n+s-r2;
                if (r1==s) {
                    n = n2;
                    s = r2;
                }
                else {
                    if (n2!=0) {
                        qi = new sJobItem;
                        qi->num = n2;
                        qi->start = r2;
                        NonReentrantSpinBlock block(joblock);
                        jobq.enqueue(qi);
                        if (waiting) {
                            jobqsem.signal(waiting);
                            waiting = 0;
                        }
                    }
                    n = r1-s;
                }
            }
            serialsort(s,n);
            NonReentrantSpinBlock block(joblock);
            if (waiting==numsubthreads) { // well we are done so are rest
                done = true;
                jobqsem.signal(waiting);
                break;
            }
        }
        while(waitForWork(s,n));
    }




    void run()
    {
        unsigned s;
        unsigned n;
        if (waitForWork(s,n))
            subsort(s,n);
    }

    void join()
    {
        for (unsigned i=0;i<numsubthreads;i++)
            threads[i]->join();
    }

    virtual void serialsort(unsigned from, unsigned len)=0;
    virtual void partition(unsigned s, unsigned n, unsigned &r1, unsigned &r2) = 0; // NB s, r1 and r2 are relative to array

};

#define DOPARTITION                                 \
        VECTOR a = array+s;                         \
        VECTOR pm = a + (n / 2);                    \
        VECTOR pl = a;                              \
        VECTOR pn = a + (n - 1) ;                   \
        if (n > 40) {                               \
            unsigned d = (n / 8);                   \
            pl = MED3(pl, pl + d, pl + 2 * d);      \
            pm = MED3(pm - d, pm, pm + d);          \
            pn = MED3(pn - 2 * d, pn - d, pn);      \
        }                                           \
        pm = MED3(pl, pm, pn);                      \
        SWAP(a, pm);                                \
        VECTOR pa = a + 1;                          \
        VECTOR pb = pa;                             \
        VECTOR pc = a + (n - 1);                    \
        VECTOR pd = pc;                             \
        int r;                                      \
        for (;;) {                                  \
            while (pb <= pc && (r = CMP(pb, a)) <= 0) { \
                if (r == 0) {                       \
                    SWAP(pa, pb);                   \
                    pa++;                           \
                }                                   \
                pb++;                               \
            }                                       \
            while (pb <= pc && (r = CMP(pc, a)) >= 0) { \
                if (r == 0) {                       \
                    SWAP(pc, pd);                   \
                    pd--;                           \
                }                                   \
                pc--;                               \
            }                                       \
            if (pb > pc)                            \
                break;                              \
            SWAP(pb, pc);                           \
            pb++;                                   \
            pc--;                                   \
        }                                           \
        pn = a + n;                                 \
        r = MIN(pa - a, pb - pa);                   \
        VECTOR v1 = a;                              \
        VECTOR v2 = pb-r;                           \
        while (r) {                                 \
            SWAP(v1,v2); v1++; v2++; r--;           \
        };                                          \
        r = MIN(pd - pc, pn - pd - 1);              \
        v1 = pb;                                    \
        v2 = pn-r;                                  \
        while (r) {                                 \
            SWAP(v1,v2); v1++; v2++; r--;           \
        };                                          \
        r1 = (pb-pa)+s;                             \
        r2 = n-(pd-pc)+s;                           


class cParQSort: public cParQSortBase
{
    VECTOR array;
    const ICompare &compare;

    void partition(unsigned s, unsigned n, unsigned &r1, unsigned &r2) // NB s, r1 and r2 are relative to array
    {
        DOPARTITION
    }

    void serialsort(unsigned from, unsigned len)
    {
        qsortvec(array+from,len,compare);
    }

public:

    cParQSort(VECTOR _a,const ICompare &_compare, unsigned _numsubthreads)
        : cParQSortBase(_numsubthreads), compare(_compare)
    {
        array = _a;
        cParQSortBase::start();
    }

};


void parqsortvec(void **a, size32_t n, const ICompare & compare, unsigned numcpus)
{
    if ((n<=PARALLEL_GRANULARITY)||!sortParallel(numcpus)) {
        qsortvec(a,n,compare);
        return;
    }
    cParQSort sorter(a,compare,numcpus-1);
    sorter.subsort(0,n);
    sorter.join();

#ifdef TESTPARSORT
    for (unsigned i=1;i<n;i++)
        if (compare.docompare(a[i-1],a[i])>0)
            IERRLOG("parqsortvec failed %d",i);
#endif

}


#undef CMP
#undef MED3
#undef RECURSE

//---------------------------------------------------------------------------

#undef VECTOR
#undef SWAP
typedef void *** _IVECTOR;
#define VECTOR _IVECTOR
static inline void swapind(VECTOR a, VECTOR b)  { void ** t = *a;  *a = *b; *b = t; }
#define SWAP swapind


#define CMP(a,b)         cmpicindstable(a,b,compare)

static inline int cmpicindstable(VECTOR a, VECTOR b, const ICompare & compare)
{
    int ret = compare.docompare(**a,**b);
    if (ret==0)
    {
        if (*a>*b)
            ret = 1;
        else if (*a<*b)
            ret = -1;
    }
    return ret;
}

#define MED3(a,b,c)      med3ic(a,b,c,compare)
#define RECURSE(a,b)     doqsortvecstable(a, b, compare)
static inline VECTOR med3ic(VECTOR a, VECTOR b, VECTOR c, const ICompare & compare)
{
  return CMP(a, b) < 0 ?
      (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a ))
    : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c ));
}


static void doqsortvecstable(VECTOR a, size32_t n, const ICompare & compare)
#include "jsort2.inc"

class cParQSortStable: public cParQSortBase
{
    VECTOR array;
    const ICompare &compare;

    void partition(unsigned s, unsigned n, unsigned &r1, unsigned &r2) // NB s, r1 and r2 are relative to array
    {
        DOPARTITION
    }

    void serialsort(unsigned from, unsigned len)
    {
        doqsortvecstable(array+from,len,compare);
    }

public:

    cParQSortStable(VECTOR _a,const ICompare &_compare, unsigned _numsubthreads)
        : cParQSortBase(_numsubthreads),compare(_compare)
    {
        array = _a;
        cParQSortBase::start();
    }

};



#undef CMP
#undef CMP1
#undef MED3
#undef RECURSE
#undef VECTOR

static void qsortvecstable(void ** const rows, size32_t n, const ICompare & compare, void *** index)
{
    for(unsigned i=0; i<n; ++i)
        index[i] = rows+i;
    doqsortvecstable(index, n, compare);
}

void qsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp)
{
    memcpy(temp, rows, n * sizeof(void*));

    qsortvecstable(temp, n, compare, (void * * *)rows);

    //I'm sure this violates the aliasing rules...
    void * * * rowsAsIndex = (void * * *)rows;
    for(unsigned i=0; i<n; ++i)
        rows[i] = *rowsAsIndex[i];
}

static void parqsortvecstable(void ** rows, size32_t n, const ICompare & compare, void *** index, unsigned numcpus)
{
    for(unsigned i=0; i<n; ++i)
        index[i] = rows+i;
    if ((n<=PARALLEL_GRANULARITY)||!sortParallel(numcpus)) {
        doqsortvecstable(index,n,compare);
        return;
    }
    cParQSortStable sorter(index,compare,numcpus-1);
    sorter.subsort(0,n);
    sorter.join();
}


void parqsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp, unsigned numcpus)
{
    memcpy(temp, rows, n * sizeof(void*));

    parqsortvecstable(temp, n, compare, (void * * *)rows, numcpus);

    //I'm sure this violates the aliasing rules...
    void * * * rowsAsIndex = (void * * *)rows;
    for(size32_t i=0; i<n; ++i)
        rows[i] = *rowsAsIndex[i];
}

//=========================================================================

bool heap_push_down(unsigned p, unsigned num, unsigned * heap, const void ** rows, ICompare * compare)
{
    bool nochange = true;
    while(1)
    {
        unsigned c = p*2 + 1;
        if(c >= num) 
            return nochange;
        if(c+1 < num)
        {
            int childcmp = compare->docompare(rows[heap[c+1]], rows[heap[c]]);
            if((childcmp < 0) || ((childcmp == 0) && (heap[c+1] < heap[c])))
                ++c;
        }
        int cmp = compare->docompare(rows[heap[c]], rows[heap[p]]);
        if((cmp > 0) || ((cmp == 0) && (heap[c] > heap[p])))
            return nochange;
        nochange = false;
        unsigned r = heap[c];
        heap[c] = heap[p];
        heap[p] = r;
        p = c;
    }
}

bool heap_push_up(unsigned c, unsigned * heap, const void ** rows, ICompare * compare)
{
    bool nochange = true;
    while(c > 0)
    {
        unsigned p = (unsigned)(c-1)/2;
        int cmp = compare->docompare(rows[heap[c]], rows[heap[p]]);
        if((cmp > 0) || ((cmp == 0) && (heap[c] > heap[p])))
            return nochange;
        nochange = false;
        unsigned r = heap[c];
        heap[c] = heap[p];
        heap[p] = r;
        c = p;
    }
    return nochange;
}

//=========================================================================

#include    <assert.h>
#include    <stdio.h>
#include    <stdlib.h>
#include    <fcntl.h>
#include    <string.h>
#include    <stddef.h>
#include    <time.h>
   
#ifdef _WIN32
#include    <io.h>
#include    <sys\types.h>
#include    <sys\stat.h>
#else
#include    <sys/types.h>
#include    <sys/stat.h>
#endif

#ifndef off_t
#define off_t __int64
#endif


typedef void ** VECTOR;

interface IMergeSorter
{
public:
    virtual IWriteSeq *getOutputStream(bool isEOF) = 0;
};

#define INSERTMAX 10000
#define BUFFSIZE 0x100000   // used for output buffer

//==================================================================================================

class CRowStreamMerger
{
    const void **pending;
    size32_t *recsize;
    unsigned *mergeheap;
    unsigned activeInputs; 
    count_t recno;
    const ICompare *icmp;
    bool partdedup;

    IRowProvider &provider;
    MemoryAttr workingbuf;
#ifdef _DEBUG
    bool *stopped;
    MemoryAttr ma;
#endif

    inline int buffCompare(unsigned a, unsigned b)
    {
        //MTIME_SECTION(defaultTimer, "CJStreamMergerBase::buffCompare");
        return icmp->docompare(pending[mergeheap[a]], pending[mergeheap[b]]);
    }

    bool promote(unsigned p)
    {
        activeInputs--;
        if(activeInputs == p)
            return false;
        mergeheap[p] = mergeheap[activeInputs];
        return true;
    }

    inline bool siftDown(unsigned p)
    {
        //MTIME_SECTION(defaultTimer, "CJStreamMergerBase::siftDown");
        // assuming that all descendants of p form a heap, sift p down to its correct position, and so include it in the heap
        bool nochange = true;
        while(1)
        {
            unsigned c = p*2 + 1;
            if(c >= activeInputs) 
                return nochange;
            if(c+1 < activeInputs)
            {
                int childcmp = buffCompare(c+1, c);
                if((childcmp < 0) || ((childcmp == 0) && (mergeheap[c+1] < mergeheap[c])))
                    ++c;
            }
            int cmp = buffCompare(c, p);
            if((cmp > 0) || ((cmp == 0) && (mergeheap[c] > mergeheap[p])))
                return nochange;
            nochange = false;
            unsigned r = mergeheap[c];
            mergeheap[c] = mergeheap[p];
            mergeheap[p] = r;
            p = c;
        }
    }

    void siftDownDedupTop()
    {
        //MTIME_SECTION(defaultTimer, "CJStreamMergerBase::siftDownDedupTop");
        // same as siftDown(0), except that it also ensures that the top of the heap is not equal to either of its children
        if(activeInputs < 2)
            return;
        unsigned c = 1;
        int childcmp = 1;
        if(activeInputs >= 3)
        {
            childcmp = buffCompare(2, 1);
            if(childcmp < 0)
                c = 2;
        }
        int cmp = buffCompare(c, 0);
        if(cmp > 0)
            return;
        // the following loop ensures the correct property holds on the smaller branch, and that childcmp==0 iff the top matches the other branch
        while(cmp <= 0)
        {
            if(cmp == 0)
            {
                if(mergeheap[c] < mergeheap[0])
                {
                    unsigned r = mergeheap[c];
                    mergeheap[c] = mergeheap[0];
                    mergeheap[0] = r;
                }
                if(!pullInput(mergeheap[c]))
                    if(!promote(c))
                        break;
                siftDown(c);
            }
            else
            {
                unsigned r = mergeheap[c];
                mergeheap[c] = mergeheap[0];
                mergeheap[0] = r;
                if(siftDown(c))
                    break;
            }
            cmp = buffCompare(c, 0);
        }
        // the following loop ensures the uniqueness property holds on the other branch too
        c = 3-c;
        if(activeInputs <= c)
            return;
        while(childcmp == 0)
        {
            if(mergeheap[c] < mergeheap[0])
            {
                unsigned r = mergeheap[c];
                mergeheap[c] = mergeheap[0];
                mergeheap[0] = r;
            }
            if(!pullInput(mergeheap[c]))
                if(!promote(c))
                    break;
            siftDown(c);
            childcmp = buffCompare(c, 0);
        }
    }


    void init()
    {
        if (activeInputs>0) {
            // setup heap property
            if (activeInputs >= 2)
                for(unsigned p = (activeInputs-2)/2; p > 0; --p)
                    siftDown(p);
            if (partdedup)
                siftDownDedupTop();
            else
                siftDown(0);
        }
        recno = 0;
    }

    inline count_t num() const { return recno; }

    
    inline bool _next()
    {
        if (!activeInputs)
            return false;
        if (recno) {
            if(!pullInput(mergeheap[0]))
                if(!promote(0))
                    return false;
            // we have changed the element at the top of the heap, so need to sift it down to maintain the heap property
            if(partdedup)
                siftDownDedupTop();
            else
                siftDown(0);
        }
        recno++;
        return true;
    }
    inline bool eof() const { return activeInputs==0; }


    bool pullInput(unsigned i)
    {
        if (pending[i]) {
            assertex(partdedup);
            provider.releaseRow(pending[i]);
        }
        pending[i] = provider.nextRow(i);
        if (pending[i])
            return true;
        provider.stop(i);
#ifdef _DEBUG
        assertex(!stopped[i]);
        stopped[i] = true;
#endif
        return false;
    }

public:
    CRowStreamMerger(IRowProvider &_provider,unsigned numstreams, const ICompare *_icmp,bool _partdedup=false)
        : provider(_provider)
    {
        partdedup = _partdedup;
        icmp = _icmp;
        recsize = NULL;
        activeInputs = 0;
#ifdef _DEBUG
        stopped = (bool *)ma.allocate(numstreams*sizeof(bool));
        memset(stopped,0,numstreams*sizeof(bool));
#endif
        unsigned i;
        recsize = NULL;
        if (numstreams) {
            byte *buf = (byte *)workingbuf.allocate(numstreams*(sizeof(void *)+sizeof(unsigned)));
            pending = (const void **)buf;
            mergeheap = (unsigned *)(pending+numstreams);
            for (i=0;i<numstreams;i++) {
                pending[i] = NULL;
                if (pullInput(i)) 
                    mergeheap[activeInputs++] = i;
            }
        }
        else {
            pending = NULL;
            mergeheap = NULL;
        }
        init();
    }
    void stop()
    {
        while (activeInputs) {
            activeInputs--;
            if (pending[mergeheap[activeInputs]]) {
                provider.releaseRow(pending[mergeheap[activeInputs]]);
#ifdef _DEBUG
                assertex(!stopped[mergeheap[activeInputs]]);
                stopped[mergeheap[activeInputs]] = true;
#endif
                provider.stop(mergeheap[activeInputs]);
            }
        }
        pending = NULL;
        mergeheap = NULL;
        workingbuf.clear();
    }

    ~CRowStreamMerger()
    {
        stop();
    }

    inline const void * next()
    {
        if (!_next())
            return NULL;
        unsigned strm = mergeheap[0];
        const void *row = pending[strm];
        pending[strm] = NULL;
        return row;
    }

};


class CMergeRowStreams : implements IRowStream, public CInterface
{
protected:
    CRowStreamMerger *merger;
    bool eos;

    class cProvider: implements IRowProvider, public CInterface
    {
        IArrayOf<IRowStream> ostreams;
        IRowStream **streams;
        Linked<IRowLinkCounter> linkcounter;
        const void *nextRow(unsigned idx)
        {
            return streams[idx]->nextRow();
        };
        void linkRow(const void *row)
        {
            linkcounter->linkRow(row);
        }
        void releaseRow(const void *row)
        {
            linkcounter->releaseRow(row);
        }
        void stop(unsigned idx)
        {
            streams[idx]->stop();
        }
    public:
        IMPLEMENT_IINTERFACE;
        cProvider(IRowStream **_streams, unsigned numstreams, IRowLinkCounter *_linkcounter)
            : linkcounter(_linkcounter)
        {
            ostreams.ensure(numstreams);
            unsigned n = 0;
            while (n<numstreams) 
                ostreams.append(*LINK(_streams[n++]));
            streams = ostreams.getArray();
        }
    } *streamprovider;

    
public:
    CMergeRowStreams(unsigned _numstreams,IRowStream **_instreams,ICompare *_icmp, bool partdedup, IRowLinkCounter *_linkcounter)
    {
        streamprovider = new cProvider(_instreams, _numstreams, _linkcounter);
        merger = new CRowStreamMerger(*streamprovider,_numstreams,_icmp,partdedup);
        eos = _numstreams==0;
        
    }

    CMergeRowStreams(unsigned _numstreams,IRowProvider &_provider,ICompare *_icmp, bool partdedup)
    {
      streamprovider = NULL;
        merger = new CRowStreamMerger(_provider,_numstreams,_icmp,partdedup);
        eos = _numstreams==0;
    }


    ~CMergeRowStreams()
    {
        delete merger;
        delete streamprovider;
    }


    IMPLEMENT_IINTERFACE;

    void stop()
    {
        if (!eos) {
            merger->stop();
            eos = true;
        }
    }

    const void *nextRow() 
    {
        if (eos)
            return NULL;
        const void *r = merger->next();
        if (!r) {
            stop(); // think ok to stop early
            return NULL;
        }
        return r;
    }

};


IRowStream *createRowStreamMerger(unsigned numstreams,IRowStream **instreams,ICompare *icmp,bool partdedup,IRowLinkCounter *linkcounter)
{
    return new CMergeRowStreams(numstreams,instreams,icmp,partdedup,linkcounter);
}


IRowStream *createRowStreamMerger(unsigned numstreams,IRowProvider &provider,ICompare *icmp,bool partdedup)
{
    return new CMergeRowStreams(numstreams,provider,icmp,partdedup);
}
