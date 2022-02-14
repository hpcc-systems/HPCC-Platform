/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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
#include "thorsort.hpp"
#include "jset.hpp"
#include "jlog.hpp"
#include "errorlist.h"
#include <exception>
#include "jtask.hpp"

#ifdef _USE_TBB
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
    try
    {
#ifdef _USE_TBB
        TbbCompareWrapper tbbcompare(compare);
        tbb::parallel_sort(a, a + n, tbbcompare);
#else
        throwUnexpectedX("TBB quicksort not available");
#endif
    }
    catch (const std::exception & e)
    {
        throw makeStringExceptionV(ERRORID_UNKNOWN, "TBB exception: %s", e.what());
    }
}

void tbbqsortstable(void ** rows, size_t n, const ICompare & compare, void ** temp)
{
    try
    {
#ifdef _USE_TBB
        void * * * rowsAsIndex = (void * * *)rows;
        memcpy_iflen(temp, rows, n * sizeof(void*));

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
    catch (const std::exception & e)
    {
        throw makeStringExceptionV(ERRORID_UNKNOWN, "TBB exception: %s", e.what());
    }
}

//-----------------------------------------------------------------------------------------------------------------------------

inline void * * mergePartitions(const ICompare & compare, void * * result, size_t n1, void * * ret1, size_t n2, void * * ret2)
{
    void * * tgt = result;
    for (;;)
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
       if (compare.docompare(*ret1, *ret2) <= 0)
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
       else
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

static const unsigned numPartitionSamples = 3;
//These constants are probably architecture and number of core dependent
static const size_t singleThreadedMSortThreshold = 2000;
static const size_t multiThreadedBlockThreshold = 64;       // must be at least 2!

class ParallelMergeSorter
{
    class SplitTask : public CTask
    {
    public:
        SplitTask(CTask * _succ1, CTask * _succ2) : CTask(1), succ1(_succ1), succ2(_succ2)
        {
        }

        virtual CTask * execute()
        {
            //MORE: Nothing shares either succ1 or succ2, so we could initialize predecessor count to 0, and unconditionally schedule
            if (succ1->notePredDone())
                spawnOwnedChildTask(*succ1);
            if (succ2->notePredDone())
                return succ2;
            return NULL;
        }
    protected:
        CTask * succ1;
        CTask * succ2;
    };

    class BisectTask : public CTask
    {
    public:
        BisectTask(ParallelMergeSorter & _sorter, void ** _rows, size_t _n, void ** _temp, unsigned _depth, CTask * _succ)
        : CTask(0), sorter(_sorter), rows(_rows), temp(_temp), successor(_succ), n(_n), depth(_depth)
        {
        }
        virtual CTask * execute()
        {
            for (;;)
            {
                //On entry next is assumed to be used once by this function
                if ((n <= multiThreadedBlockThreshold) || (depth >= sorter.singleThreadDepth))
                {
                    mergeSort(rows, n, sorter.compare, temp, depth);
                    if (successor->notePredDone())
                        return successor;
                    return nullptr;
                }

                void * * result = (depth & 1) ? temp : rows;
                void * * src = (depth & 1) ? rows : temp;
                size_t n1 = (n+1)/2;
                size_t n2 = n-n1;
                CTask * mergeTask;
                if (depth < sorter.parallelMergeDepth)
                {
                    unsigned partitions = sorter.numPartitionCores() >> depth;

                    //Following will create 2 * partition merge tasks, one of which will inherit the already incremented predecessor count
                    if (partitions > 1)
                    {
                        successor->incPred(2 * partitions - 1);
                        PartitionSplitTask * splitTask = new PartitionSplitTask(n1, src, n2, src+n1, partitions, sorter.compare);
                        for (unsigned i=0; i < partitions; i++)
                        {
                            MergeTask * mergeFwdTask = new MergeTask(sorter.compare, *successor, result, n1, src, n2, src+n1, 0);
                            MergeTask * mergeRevTask = new MergeRevTask(sorter.compare, *successor, result, n1, src, n2, src+n1, 0);
                            splitTask->setTasks(i, mergeFwdTask, mergeRevTask);
                        }
                        mergeTask = splitTask;
                    }
                    else
                    {
                        successor->incPred(1);
                        CTask * mergeFwdTask = new MergeTask(sorter.compare, *successor, result, n1, src, n2, src+n1, n1);
                        CTask * mergeRevTask = new MergeRevTask(sorter.compare, *successor, result, n1, src, n2, src+n1, n2);
                        mergeTask = new SplitTask(mergeFwdTask, mergeRevTask);
                    }
                }
                else
                {
                    mergeTask = new MergeTask(sorter.compare, *successor, result, n1, src, n2, src+n1, n);
                }

                mergeTask->setNumPredecessors(2);

                CTask * bisectRightTask = new BisectTask(sorter, rows+n1, n2, temp+n1, depth+1, mergeTask);
                enqueueOwnedTask(sorter.scheduler, *bisectRightTask);

                //recurse directly on the left side rather than creating a new task
                n = n1;
                depth = depth+1;
                successor = mergeTask;
            }
        }
    protected:
        ParallelMergeSorter & sorter;
        void ** rows;
        void ** temp;
        CTask * successor;
        size_t n;
        unsigned depth;
    };


    class SubSortTask : public CTask
    {
    public:
        SubSortTask(ParallelMergeSorter & _sorter, CTask & _successor, void ** _rows, size_t _n, void ** _temp, unsigned _depth)
        : CTask(0), sorter(_sorter), successor(_successor), rows(_rows), temp(_temp), n(_n), depth(_depth)
        {
        }

        virtual CTask * execute()
        {
            mergeSort(rows, n, sorter.compare, temp, depth);
            if (successor.notePredDone())
                return &successor;
            return NULL;
        }

    protected:
        ParallelMergeSorter & sorter;
        CTask & successor;
        void ** rows;
        void ** temp;
        size_t n;
        unsigned depth;
    };


    class MergeTask : public CTask
    {
    public:
        MergeTask(const ICompare & _compare, CTask & _successor, void * * _result, size_t _n1, void * * _src1, size_t _n2, void * * _src2, size_t _n)
        : CTask(1), compare(_compare), successor(_successor), result(_result), src1(_src1), src2(_src2), n1(_n1), n2(_n2), n(_n)
        {
        }

        virtual CTask * execute()
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
            return checkNextTask();
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

        CTask * checkNextTask()
        {
            if (successor.notePredDone())
                return &successor;
            return nullptr;
        }

    protected:
        const ICompare & compare;
        CTask & successor;
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
        MergeRevTask(const ICompare & _compare, CTask & _successor, void * * _result, size_t _n1, void * * _src1, size_t _n2, void * * _src2, size_t _n)
        : MergeTask(_compare, _successor, _result, _n1, _src1, _n2, _src2, _n)
        {
        }

        virtual CTask * execute()
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
                mergePartitionsRev(compare, result, n1, src1, n2, src2, n);

            return checkNextTask();
        }
    };

    class PartitionSplitTask : public CTask
    {
    public:
        PartitionSplitTask(size_t _n1, void * * _src1, size_t _n2, void * * _src2, unsigned _numPartitions, const ICompare & _compare)
            : CTask(0), compare(_compare), numPartitions(_numPartitions), n1(_n1), n2(_n2), src1(_src1), src2(_src2)
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
                            //value in left is smallest.  Find the position of the first value >= the left value
                            //do not include it since there may be more left matches to merge first
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
                size_t numFwd = (num+1)/2;
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

        virtual CTask * execute()
        {
            calculatePartitions();
            for (unsigned i=0; i < numPartitions*2; i++)
            {
                //NOTE: These tasks only hava a single successor, so simpler to set #pred to 0, and unconditionally spawn them
                //(and use continuation to return the 1st)
                if (tasks[i]->notePredDone())
                    spawnOwnedChildTask(*tasks[i]);
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
                size_t mid = low + (high - low) / 2;
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
                size_t mid = low + (high - low) / 2;
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
    ParallelMergeSorter(void * * _rows, const ICompare & _compare)
    : scheduler(queryTaskScheduler()), compare(_compare), baseRows(_rows)
    {
        //The following constants control the number of iterations to be performed in parallel.
        //The sort is split into more parts than there are cpus so that the effect of delays from one task tend to be evened out.
        //The following constants should possibly be tuned on each platform.  The following gave a good balance on a 2x8way xeon
        const unsigned extraBisectDepth = 3;
        const unsigned extraParallelMergeDepth = 3;

        unsigned numCpus = scheduler.numProcessors();
        unsigned ln2NumCpus = (numCpus <= 1) ? 0 : getMostSignificantBit(numCpus-1);
        assertex(numCpus <= (1U << ln2NumCpus));

        //Merge in parallel once it is likely to be beneficial
        parallelMergeDepth = ln2NumCpus+ extraParallelMergeDepth;

        //Aim to execute in parallel until the width is 8*the maximum number of parallel task
        singleThreadDepth = ln2NumCpus + extraBisectDepth;
        partitionCores = std::max(numCpus / 2, 1U);
    }

    unsigned numPartitionCores() const { return partitionCores; }

    void sortRoot(void ** rows, size_t n, void ** temp)
    {
        unsigned  numPred = 1 + 1; // the initial bisect task and the wait for the result.
        Owned<CCompletionTask> end = new CCompletionTask(scheduler, numPred);

        // Rely on scheduling to release the link counts for child tasks (including this completion task) to minimize atomic operations
        end->setMinimalLinking();

        //MORE: This bisection could be done in a single pass, rather than creating separate tasks - although
        //it is hard to tell what would be the most efficient...  This ensures they are spread over all cpus.
        scheduler.enqueueOwnedTask(*new BisectTask(*this, rows, n, temp, 0, end));
        end->decAndWait();
    }

public:
    const ICompare & compare;
    ITaskScheduler & scheduler;
    unsigned singleThreadDepth;
    unsigned parallelMergeDepth;
    unsigned partitionCores;
    void * * baseRows;
};

//-------------------------------------------------------------------------------------------------------------------
void parmsortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp)
{
    if ((n <= singleThreadedMSortThreshold) || queryTaskScheduler().numProcessors() == 1)
    {
        msortvecstableinplace(rows, n, compare, temp);
        return;
    }

    try
    {
        ParallelMergeSorter sorter(rows, compare);
        sorter.sortRoot(rows, n, temp);
    }
    catch (const std::exception & e)
    {
        throw makeStringExceptionV(ERRORID_UNKNOWN, "TBB exception: %s", e.what());
    }
}
