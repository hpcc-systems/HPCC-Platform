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

#ifdef _USE_TBB
#include "tbb/task.h"
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_sort.h"
#endif

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
    static unsigned numCPUs = 0;
    if (numCPUs==0) {
        numCPUs = getAffinityCpus();
    }
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

//---------------------------------------------------------------------------
// tbb versions of the quick sort to provide a useful base comparison

class TbbCompareWrapper
{
public:
    TbbCompareWrapper(const ICompare & _compare) : compare(_compare) {}
    bool operator()(void * const & l, void * const & r) const { return compare.docompare(l, r) < 0; }
    const ICompare & compare;
};


class TbbCompareIndirectWrapper
{
public:
    TbbCompareIndirectWrapper(const ICompare & _compare) : compare(_compare) {}
    bool operator()(void * * const & l, void * * const & r) const
    {
        int ret = compare.docompare(*l,*r);
        if (ret==0)
        {
            if (l < r)
                return true;
            else
                return false;
        }
        return (ret < 0);
    }
    const ICompare & compare;
};


void tbbqsortvec(void **a, size_t n, const ICompare & compare)
{
#ifdef _USE_TBB
    TbbCompareWrapper tbbcompare(compare);
    tbb::parallel_sort(a, a+n, tbbcompare);
#else
    throwUnexpectedX("TBB quicksort not available");
#endif
}

void tbbqsortstable(void ** rows, size_t n, const ICompare & compare, void ** temp)
{
#ifdef _USE_TBB
    void * * * rowsAsIndex = (void * * *)rows;
    memcpy(temp, rows, n * sizeof(void*));

    for(unsigned i=0; i<n; ++i)
        rowsAsIndex[i] = temp+i;

    TbbCompareIndirectWrapper tbbcompare(compare);
    tbb::parallel_sort(rowsAsIndex, rowsAsIndex+n, tbbcompare);

    //I'm sure this violates the aliasing rules...
    for(unsigned i=0; i<n; ++i)
        rows[i] = *rowsAsIndex[i];
#else
    throwUnexpectedX("TBB quicksort not available");
#endif
}


//---------------------------------------------------------------------------

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
            ERRLOG("parqsortvec failed %d",i);
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


//-----------------------------------------------------------------------------------------------------------------------------

inline void * * mergePartitions(const ICompare & compare, void * * result, size_t n1, void * * ret1, size_t n2, void * * ret2)
{
    void * * tgt = result;
    loop
    {
       if (compare.docompare(*ret1, *ret2) <= 0)
       {
           *tgt++ = *ret1++;
           if (--n1 == 0)
           {
               //There must be at least one row in the right partition - copy any that remain
               do
               {
                   *tgt++ = *ret2++;
               } while (--n2);
               return result;
           }
       }
       else
       {
           *tgt++ = *ret2++;
           if (--n2 == 0)
           {
               //There must be at least one row in the left partition - copy any that remain
               do
               {
                   *tgt++ = *ret1++;
               } while (--n1);
               return result;
           }
       }
    }
}

inline void * * mergePartitions(const ICompare & compare, void * * result, size_t n1, void * * ret1, size_t n2, void * * ret2, size_t n)
{
    void * * tgt = result;
    while (n--)
    {
       if (compare.docompare(*ret1, *ret2) <= 0)
       {
           *tgt++ = *ret1++;
           if (--n1 == 0)
           {
               while (n--)
               {
                   *tgt++ = *ret2++;
               }
               return result;
           }
       }
       else
       {
           *tgt++ = *ret2++;
           if (--n2 == 0)
           {
               while (n--)
               {
                   *tgt++ = *ret1++;
               }
               return result;
           }
       }
    }
    return result;
}

inline void clonePartition(void * * result, size_t n, void * * src)
{
    void * * tgt = result;
    while (n--)
       *tgt++ = *src++;
}

inline void * * mergePartitionsRev(const ICompare & compare, void * * result, size_t n1, void * * ret1, size_t n2, void * * ret2, size_t n)
{
    void * * tgt = result+n1+n2-1;
    ret1 += (n1-1);
    ret2 += (n2-1);
    while (n--)
    {
       if (compare.docompare(*ret1, *ret2) >= 0)
       {
           *tgt-- = *ret1--;
           if (--n1 == 0)
           {
               while (n--)
               {
                   *tgt-- = *ret2--;
               }
               return result;
           }
       }
       else
       {
           *tgt-- = *ret2--;
           if (--n2 == 0)
           {
               //There must be at least one row in the left partition - copy any that remain
               while (n--)
               {
                   *tgt-- = *ret1--;
               }
               return result;
           }
       }
    }
    return result;
}

static void * * mergeSort(void ** rows, size_t n, const ICompare & compare, void ** tmp, unsigned depth)
{
    void * * result = (depth & 1) ? tmp : rows;
    //This could be coded to perform an "optimal" 3 element compare, but the following code is much simpler,
    //and in performance testing it executed marginally more quickly
    if (n <= 2)
    {
        //Check for n == 1, but compare against 2 to avoid another comparison
        if (n < 2)
        {
            if (result != rows)
                result[0] = rows[0];
        }
        else
        {
            void * left = rows[0];
            void * right = rows[1];
            if (compare.docompare(left, right) <= 0)
            {
                result[0] = left;
                result[1] = right;
            }
            else
            {
                result[0] = right;
                result[1] = left;
            }
        }
        return result;
    }

    size_t n1 = (n+1)/2;
    size_t n2 = n - n1;
    void * * ret1 = mergeSort(rows, n1, compare, tmp, depth+1);
    void * * ret2 = mergeSort(rows+n1, n2, compare, tmp + n1, depth+1);
    dbgassertex(ret2 == ret1 + n1);
    dbgassertex(ret2 != result);
    return mergePartitions(compare, result, n1, ret1, n2, ret2);
}


void msortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp)
{
    if (n <= 1)
        return;
    mergeSort(rows, n, compare, temp, 0);
}

//=========================================================================

#ifdef _USE_TBB
static const unsigned numPartitionSamples = 3;
//These constants are probably architecture and number of core dependent
static const size_t singleThreadedMSortThreshold = 2000;
static const size_t multiThreadedBlockThreshold = 64;       // must be at least 2!

using tbb::task;
class TbbParallelMergeSorter
{
    class SplitTask : public tbb::task
    {
    public:
        SplitTask(task * _next1, task * _next2) : next1(_next1), next2(_next2)
        {
        }

        virtual task * execute()
        {
            if (next1->decrement_ref_count() == 0)
                spawn(*next1);
            if (next2->decrement_ref_count() == 0)
                return next2;
            return NULL;
        }
    protected:
        task * next1;
        task * next2;
    };

    class BisectTask : public tbb::task
    {
    public:
        BisectTask(TbbParallelMergeSorter & _sorter, void ** _rows, size_t _n, void ** _temp, unsigned _depth, task * _next)
        : sorter(_sorter), rows(_rows), temp(_temp), next(_next), n(_n), depth(_depth)
        {
        }
        virtual task * execute()
        {
            loop
            {
                //On entry next is assumed to be used once by this function
                if ((n <= multiThreadedBlockThreshold) || (depth >= sorter.singleThreadDepth))
                {
                    //Create a new task rather than calling sort directly, so that the successor is set up correctly
                    //It would be possible to sort then if (next->decrement_ref_count()) return next; instead
                    task * sort = new (next->allocate_child()) SubSortTask(sorter, rows, n, temp, depth);
                    return sort;
                }

                void * * result = (depth & 1) ? temp : rows;
                void * * src = (depth & 1) ? rows : temp;
                size_t n1 = (n+1)/2;
                size_t n2 = n-n1;
                task * mergeTask;
                if (depth < sorter.parallelMergeDepth)
                {
                    unsigned partitions = sorter.numPartitionCores() >> depth;
                    if (partitions > 1)
                    {
                        PartitionSplitTask * splitTask = new (allocate_root()) PartitionSplitTask(n1, src, n2, src+n1, partitions, sorter.compare);
                        for (unsigned i=0; i < partitions; i++)
                        {
                            MergeTask * mergeFwdTask = new (allocate_additional_child_of(*next)) MergeTask(sorter.compare, result, n1, src, n2, src+n1, 0);
                            mergeFwdTask->set_ref_count(1);
                            MergeTask * mergeRevTask = new (allocate_additional_child_of(*next)) MergeRevTask(sorter.compare, result, n1, src, n2, src+n1, 0);
                            mergeRevTask->set_ref_count(1);
                            splitTask->setTasks(i, mergeFwdTask, mergeRevTask);
                        }
                        next->decrement_ref_count();
                        mergeTask = splitTask;
                    }
                    else
                    {
                        task * mergeFwdTask = new (allocate_additional_child_of(*next)) MergeTask(sorter.compare, result, n1, src, n2, src+n1, n1);
                        mergeFwdTask->set_ref_count(1);
                        task * mergeRevTask = new (next->allocate_child()) MergeRevTask(sorter.compare, result, n1, src, n2, src+n1, n2);
                        mergeRevTask->set_ref_count(1);
                        mergeTask = new (allocate_root()) SplitTask(mergeFwdTask, mergeRevTask);
                    }
                }
                else
                {
                    mergeTask = new (next->allocate_child()) MergeTask(sorter.compare, result, n1, src, n2, src+n1, n);
                }

                mergeTask->set_ref_count(2);
                task * bisectRightTask = new (allocate_root()) BisectTask(sorter, rows+n1, n2, temp+n1, depth+1, mergeTask);
                spawn(*bisectRightTask);

                //recurse directly on the left side rather than creating a new task
                n = n1;
                depth = depth+1;
                next = mergeTask;
            }
        }
    protected:
        TbbParallelMergeSorter & sorter;
        void ** rows;
        void ** temp;
        task * next;
        size_t n;
        unsigned depth;
    };


    class SubSortTask : public tbb::task
    {
    public:
        SubSortTask(TbbParallelMergeSorter & _sorter, void ** _rows, size_t _n, void ** _temp, unsigned _depth)
        : sorter(_sorter), rows(_rows), temp(_temp), n(_n), depth(_depth)
        {
        }

        virtual task * execute()
        {
            mergeSort(rows, n, sorter.compare, temp, depth);
            return NULL;
        }
    protected:
        TbbParallelMergeSorter & sorter;
        void ** rows;
        void ** temp;
        size_t n;
        unsigned depth;
    };


    class MergeTask : public tbb::task
    {
    public:
        MergeTask(const ICompare & _compare, void * * _result, size_t _n1, void * * _src1, size_t _n2, void * * _src2, size_t _n)
        : compare(_compare),result(_result), src1(_src1), src2(_src2), n1(_n1), n2(_n2), n(_n)
        {
        }

        virtual task * execute()
        {
            //After the ranges are adjusted it is possible for one input to shrink to zero size (e.g., if input is sorted)
            if (n1 == 0)
            {
                assertex(n <= n2);
                clonePartition(result, n, src2);
            }
            else if (n2 == 0)
            {
                assertex(n <= n1);
                clonePartition(result, n, src1);
            }
            else
                mergePartitions(compare, result, n1, src1, n2, src2, n);
            return NULL;
        }

        void adjustRange(size_t deltaLeft, size_t numLeft, size_t deltaRight, size_t numRight, size_t num)
        {
            src1 += deltaLeft;
            n1 = numLeft;
            src2 += deltaRight;
            n2 = numRight;
            result += (deltaLeft + deltaRight);
            n = num;
        }

    protected:
        const ICompare & compare;
        void * * result;
        void * * src1;
        void * * src2;
        size_t n1;
        size_t n2;
        size_t n;
    };

    class MergeRevTask : public MergeTask
    {
    public:
        MergeRevTask(const ICompare & _compare, void * * _result, size_t _n1, void * * _src1, size_t _n2, void * * _src2, size_t _n)
        : MergeTask(_compare, _result, _n1, _src1, _n2, _src2, _n)
        {
        }

        virtual task * execute()
        {
            if (n1 == 0)
            {
                assertex(n <= n2);
                //This is a reverse merge, so copy n from the end of the input
                unsigned delta = n2 - n;
                clonePartition(result + delta, n, src2 + delta);
            }
            else if (n2 == 0)
            {
                assertex(n <= n1);
                unsigned delta = n1 - n;
                clonePartition(result + delta, n, src1 + delta);
            }
            else
                mergePartitionsRev(compare, result, n2, src2, n1, src1, n);
            return NULL;
        }
    };

    class PartitionSplitTask : public tbb::task
    {
    public:
        PartitionSplitTask(size_t _n1, void * * _src1, size_t _n2, void * * _src2, unsigned _numPartitions, const ICompare & _compare)
            : compare(_compare), numPartitions(_numPartitions), n1(_n1), n2(_n2), src1(_src1), src2(_src2)
        {
            //These could be local variables in calculatePartitions(), but placed here to simplify cleanup.  (Should consider using alloca)
            posLeft = new size_t[numPartitions+1];
            posRight = new size_t[numPartitions+1];
            tasks = new MergeTask *[numPartitions*2];
            for (unsigned i=0; i < numPartitions*2; i++)
                tasks[i] = NULL;
        }
        ~PartitionSplitTask()
        {
            delete [] posLeft;
            delete [] posRight;
            delete [] tasks;
        }

        void calculatePartitions()
        {
#ifdef PARANOID_PARTITION
            {
                for (unsigned ix=1; ix<n1; ix++)
                    if (compare.docompare(src1[ix-1], src1[ix]) > 0)
                        DBGLOG("Failure left@%u", ix);
            }
            {
                for (unsigned ix=1; ix<n2; ix++)
                    if (compare.docompare(src2[ix-1], src2[ix]) > 0)
                        DBGLOG("Failure right@%u", ix);
            }
#endif
            //If dividing into P parts, select S*P-1 even points from each side.
            unsigned numSamples = numPartitionSamples*numPartitions-1;
            QuantilePositionIterator iterLeft(n1, numSamples+1, false);
            QuantilePositionIterator iterRight(n2, numSamples+1, false);
            iterLeft.first();
            iterRight.first();

            size_t prevLeft = 0;
            size_t prevRight =0;
            posLeft[0] = 0;
            posRight[0] = 0;

            //From the merged list, for sample i [zero based], we can guarantee that there are at least (i+1)*(n1+n2)/numSamples*2
            //rows before sample i, and at most (i+2)*(n1+n2)/numSamples*2 samples after it.
            //=> pick samples [0, 2*numSamples, 4*numSamples ...]
            //NOTE: Include elements at position 0 to ensure sorted inputs are partitioned evenly
            for (unsigned part = 1; part < numPartitions; part++)
            {
                unsigned numToSkip = numPartitionSamples*2;
                if (part == 1)
                    numToSkip++;
                for (unsigned skip=numToSkip; skip-- != 0; )
                {
                    size_t leftPos = iterLeft.get();
                    size_t rightPos = iterRight.get();
                    int c;
                    if (leftPos == n1)
                        c = +1;
                    else if (rightPos == n2)
                        c = -1;
                    else
                        c = compare.docompare(src1[leftPos], src2[rightPos]);

                    if (skip == 0)
                    {
                        if (c <= 0)
                        {
                            //value in left is smallest.  Find the position of the value <= the left value
                            posLeft[part] = leftPos;
                            size_t matchRight = findFirstGE(src1[leftPos], prevRight, rightPos, src2);
                            posRight[part] = matchRight;
                            prevRight = matchRight;  // potentially reduce the search range next time
                        }
                        else
                        {
                            size_t matchLeft = findFirstGT(src2[rightPos], prevLeft, leftPos, src1);
                            posLeft[part] = matchLeft;
                            posRight[part] = rightPos;
                            prevLeft = matchLeft;  // potentially reduce the search range next time
                        }
                    }
                    if (c <= 0)
                    {
                        iterLeft.next();
                        prevLeft = leftPos;
                    }
                    else
                    {
                        iterRight.next();
                        prevRight = rightPos;
                    }
                }
            }

            posLeft[numPartitions] = n1;
            posRight[numPartitions] = n2;
#ifdef TRACE_PARTITION
            DBGLOG("%d,%d -> {", (unsigned)n1, (unsigned)n2);
#endif
            for (unsigned i= 0; i < numPartitions; i++)
            {
                size_t start = posLeft[i] + posRight[i];
                size_t end = posLeft[i+1] + posRight[i+1];
                size_t num = end - start;
                size_t numFwd = num/2;
#ifdef TRACE_PARTITION
                DBGLOG("  ([%d..%d],[%d..%d] %d,%d = %d)",
                        (unsigned)posLeft[i], (unsigned)posLeft[i+1], (unsigned)posRight[i], (unsigned)posRight[i+1],
                        (unsigned)start, (unsigned)end, (unsigned)num);
#endif

                MergeTask & mergeFwdTask = *tasks[i*2];
                MergeTask & mergeRevTask = *tasks[i*2+1];
                mergeFwdTask.adjustRange(posLeft[i], posLeft[i+1]-posLeft[i],
                                      posRight[i], posRight[i+1]-posRight[i],
                                      numFwd);
                mergeRevTask.adjustRange(posLeft[i], posLeft[i+1]-posLeft[i],
                                      posRight[i], posRight[i+1]-posRight[i],
                                      num-numFwd);
            }
        }

        virtual task * execute()
        {
            calculatePartitions();
            for (unsigned i=0; i < numPartitions*2; i++)
            {
                if (tasks[i]->decrement_ref_count() == 0)
                    spawn(*tasks[i]);
            }
            return NULL;
        }

        void setTasks(unsigned i, MergeTask * fwd, MergeTask * rev)
        {
            tasks[i*2] = fwd;
            tasks[i*2+1] = rev;
        }

    protected:
        size_t findFirstGE(void * seek, size_t low, size_t high, void * * rows)
        {
            if (low == high)
                return low;
            while (high - low > 1)
            {
                size_t mid = (low + high) / 2;
                if (compare.docompare(rows[mid], seek) < 0)
                    low = mid;
                else
                    high = mid;
            }
            if (compare.docompare(rows[low], seek) < 0)
                return low+1;
            return low;
        }

        size_t findFirstGT(void * seek, size_t low, size_t high, void * * rows)
        {
            if (low == high)
                return low;
            while (high - low > 1)
            {
                size_t mid = (low + high) / 2;
                if (compare.docompare(rows[mid], seek) <= 0)
                    low = mid;
                else
                    high = mid;
            }
            if (compare.docompare(rows[low], seek) <= 0)
                return low+1;
            return low;
        }

    protected:
        const ICompare & compare;
        unsigned numPartitions;
        size_t n1;
        size_t n2;
        void * * src1;
        void * * src2;
        size_t * posLeft;
        size_t * posRight;
        MergeTask * * tasks;
    };

public:
    TbbParallelMergeSorter(void * * _rows, const ICompare & _compare) : compare(_compare), baseRows(_rows)
    {
        //The following constants control the number of iterations to be performed in parallel.
        //The sort is split into more parts than there are cpus so that the effect of delays from one task tend to be evened out.
        //The following constants should possibly be tuned on each platform.  The following gave a good balance on a 2x8way xeon
        const unsigned extraBisectDepth = 3;
        const unsigned extraParallelMergeDepth = 3;

        unsigned numCpus = tbb::task_scheduler_init::default_num_threads();
        unsigned ln2NumCpus = (numCpus <= 1) ? 0 : getMostSignificantBit(numCpus-1);
        assertex(numCpus <= (1U << ln2NumCpus));

        //Merge in parallel once it is likely to be beneficial
        parallelMergeDepth = ln2NumCpus+ extraParallelMergeDepth;

        //Aim to execute in parallel until the width is 8*the maximum number of parallel task
        singleThreadDepth = ln2NumCpus + extraBisectDepth;
        partitionCores = numCpus / 2;
    }

    unsigned numPartitionCores() const { return partitionCores; }

    void sortRoot(void ** rows, size_t n, void ** temp)
    {
        task * end = new (task::allocate_root()) tbb::empty_task();
        end->set_ref_count(1+1);
        task * task = new (task::allocate_root()) BisectTask(*this, rows, n, temp, 0, end);
        end->spawn(*task);
        end->wait_for_all();
        end->destroy(*end);
    }

public:
    const ICompare & compare;
    unsigned singleThreadDepth;
    unsigned parallelMergeDepth;
    unsigned partitionCores;
    void * * baseRows;
};

//-------------------------------------------------------------------------------------------------------------------
void parmsortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp, unsigned ncpus)
{
    if ((n <= singleThreadedMSortThreshold) || ncpus == 1)
    {
        msortvecstableinplace(rows, n, compare, temp);
        return;
    }

    TbbParallelMergeSorter sorter(rows, compare);
    sorter.sortRoot(rows, n, temp);
}
#else
void parmsortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp, unsigned ncpus)
{
    parqsortvecstableinplace(rows, (size32_t)n, compare, temp, ncpus);
}
#endif

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


class CMergeRowStreams : public CInterface, implements IRowStream
{
protected:
    CRowStreamMerger *merger;
    bool eos;

    class cProvider: public CInterface, implements IRowProvider
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
