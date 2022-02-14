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
#include "jtask.hpp"

#ifdef _DEBUG
// #define PARANOID
//#define TESTPARSORT
//#define MCMERGESTATS
#endif

//#define PARANOID_PARTITION
//#define TRACE_PARTITION

#define PARALLEL_GRANULARITY 512            // Worth creating more tasks up to this point, should really base on number of threads and recursion depth
#define PARALLEL_THRESHOLD  8096            // Threshold for it being worth sorting in parallel

typedef void *  ELEMENT;
typedef void ** _VECTOR;   // bit messy but allow to be redefined later
#define VECTOR _VECTOR

static inline void swap(VECTOR a, VECTOR b)  { ELEMENT t = *a;  *a = *b; *b = t; }
#define SWAP swap

static bool sortParallel()
{
#ifdef TESTPARSORT
    return true;        // to test
#else
    unsigned numCPUs = getAffinityCpus();
    return (numCPUs>1);
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

class cTaskQSortBase
{
    friend class CSubSortTask;

    ITaskScheduler & taskScheduler;
    Owned<CCompletionTask> finished;

    class CSubSortTask : public CTask
    {
    public:
        CSubSortTask(cTaskQSortBase * _parent, unsigned _start, unsigned _num) :
             CTask(0), parent(_parent), start(_start), num(_num)
        {
        }

        virtual CTask * execute() override
        {
            //MORE: Does this need a memory fence to ensure that writes from other threads are updated in the cache?
            parent->doSubSort(start, num);
            return checkNextTask();
        }

    protected:
        cTaskQSortBase * parent;
        unsigned start;
        unsigned num;
    };

public:

    cTaskQSortBase() : taskScheduler(queryTaskScheduler()), finished(new CCompletionTask(queryTaskScheduler()))
    {
    }

    void sort(unsigned n)
    {
        enqueueSort(0, n);
        finished->decAndWait();
    }

private:
    //MORE: Not really sure what this should do...
    void abort()
    {
        notifyPredDone(*finished);
    }

    void doSubSort(unsigned s, unsigned n)
    {
        while (n > PARALLEL_GRANULARITY)
        {
            unsigned r1;
            unsigned r2;
            partition(s, n, r1, r2);
            unsigned n2 = n+s-r2;
            if (r1==s) {
                n = n2;
                s = r2;
            }
            else {
                if (n2!=0)
                    enqueueSort(r2, n2);
                n = r1-s;
            }
        }
        serialsort(s,n);
        notifyPredDone(*finished);
    }

    void enqueueSort(unsigned from, unsigned num)
    {
        CSubSortTask * task = new CSubSortTask(this, from, num);
        finished->addPred();
        enqueueOwnedTask(taskScheduler, *task);
    }

public:
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


class cTaskQSort: public cTaskQSortBase
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

    cTaskQSort(VECTOR _a,const ICompare &_compare)
        : compare(_compare)
    {
        array = _a;
    }

};


void taskqsortvec(void **a, size32_t n, const ICompare & compare)
{
    if ((n<=PARALLEL_THRESHOLD)||!sortParallel()) {
        qsortvec(a,n,compare);
        return;
    }
    cTaskQSort sorter(a,compare);
    sorter.sort(n);

#ifdef TESTPARSORT
    for (unsigned i=1;i<n;i++)
        if (compare.docompare(a[i-1],a[i])>0)
            IERRLOG("taskqsortvec failed %d",i);
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

class cTaskQSortStable: public cTaskQSortBase
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

    cTaskQSortStable(VECTOR _a,const ICompare &_compare)
        : cTaskQSortBase(),compare(_compare)
    {
        array = _a;
    }
};



#undef CMP
#undef CMP1
#undef MED3
#undef RECURSE
#undef VECTOR

static void taskqsortvecstable(void ** rows, size32_t n, const ICompare & compare, void *** index)
{
    for(unsigned i=0; i<n; ++i)
        index[i] = rows+i;
    if ((n<=PARALLEL_THRESHOLD)||!sortParallel()) {
        doqsortvecstable(index,n,compare);
        return;
    }
    cTaskQSortStable sorter(index,compare);
    sorter.sort(n);
}


void taskqsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp)
{
    memcpy(temp, rows, n * sizeof(void*));

    taskqsortvecstable(temp, n, compare, (void * * *)rows);

    //I'm sure this violates the aliasing rules...
    void * * * rowsAsIndex = (void * * *)rows;
    for(size32_t i=0; i<n; ++i)
        rows[i] = *rowsAsIndex[i];
}
