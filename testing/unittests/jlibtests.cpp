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

/*
 * Jlib regression tests
 *
 */

#ifdef _USE_CPPUNIT
#include "jsem.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jset.hpp"
#include "sockfile.hpp"
#include "jqueue.hpp"

#include "unittests.hpp"

class JlibSemTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibSemTest);
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testSimple);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testSetup()
    {
    }

    void testCleanup()
    {
    }

    void testTimedAvailable(Semaphore & sem)
    {
        unsigned now = msTick();
        sem.wait(100);
        unsigned taken = msTick() - now;
        //Shouldn't cause a reschedule, definitely shouldn't wait for 100s
        ASSERT(taken < 5);
    }
    void testTimedElapsed(Semaphore & sem, unsigned time)
    {
        unsigned now = msTick();
        sem.wait(time);
        unsigned taken = msTick() - now;
        ASSERT(taken >= time && taken < 2*time);
    }

    void testSimple()
    {
        //Some very basic semaphore tests.
        Semaphore sem;
        sem.signal();
        sem.wait();
        testTimedElapsed(sem, 100);
        sem.signal();
        testTimedAvailable(sem);

        sem.reinit(2);
        sem.wait();
        testTimedAvailable(sem);
        testTimedElapsed(sem, 5);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibSemTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSemTest, "JlibSemTest" );

/* =========================================================== */

class JlibSetTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibSetTest);
        CPPUNIT_TEST(testBitsetHelpers);
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testBitsetHelpers()
    {
        CPPUNIT_ASSERT_EQUAL(0U, countTrailingUnsetBits(1));
        CPPUNIT_ASSERT_EQUAL(31U, countLeadingUnsetBits(1));
        CPPUNIT_ASSERT_EQUAL(1U, getMostSignificantBit(1));
        CPPUNIT_ASSERT_EQUAL(4U, countTrailingUnsetBits(0x110));
        CPPUNIT_ASSERT_EQUAL(23U, countLeadingUnsetBits(0x110));
        CPPUNIT_ASSERT_EQUAL(9U, getMostSignificantBit(0x110));
        CPPUNIT_ASSERT_EQUAL(0U, countTrailingUnsetBits(0xFFFFFFFFU));
        CPPUNIT_ASSERT_EQUAL(0U, countLeadingUnsetBits(0xFFFFFFFFU));
        CPPUNIT_ASSERT_EQUAL(32U, getMostSignificantBit(0xFFFFFFFFU));
    }

    void testSet1(bool initial, IBitSet *bs, unsigned start, unsigned numBits, bool setValue, bool clearValue)
    {
        unsigned end = start+numBits;
        if (initial)
            bs->incl(start, end-1);
        for (unsigned i=start; i < end; i++)
        {
            ASSERT(bs->test(i) == clearValue);
            bs->set(i, setValue);
            ASSERT(bs->test(i) == setValue);

            bs->set(i+5, setValue);
            ASSERT(bs->scan(0, setValue) == i);
            ASSERT(bs->scan(i+1, setValue) == i+5);
            bs->set(i, clearValue);
            bs->set(i+5, clearValue);
            //Clearing i+5 above may extend the set - so need to calculate the end carefully
            unsigned last = i+5 < end ? end : i + 6;
            unsigned match1 = bs->scan(0, setValue);
            CPPUNIT_ASSERT_EQUAL((unsigned)(initial ? last : -1), match1);

            bs->invert(i);
            ASSERT(bs->test(i) == setValue);
            bs->invert(i);
            ASSERT(bs->test(i) == clearValue);

            bool wasSet = bs->testSet(i, setValue);
            ASSERT(wasSet == clearValue);
            bool wasSet2 = bs->testSet(i, clearValue);
            ASSERT(wasSet2 == setValue);
            ASSERT(bs->test(i) == clearValue);

            bs->set(i, setValue);
            unsigned match = bs->scanInvert(0, setValue);
            ASSERT(match == i);
            ASSERT(bs->test(i) == clearValue);
        }
        bs->reset();
        if (initial)
        {
            bs->incl(start, end);
            bs->excl(start+5, end-5);
        }
        else
            bs->incl(start+5, end-5);
        unsigned inclStart = bs->scan(start, setValue);
        ASSERT((start+5) == inclStart);
        unsigned inclEnd = bs->scan(start+5, clearValue);
        ASSERT((end-5) == (inclEnd-1));
    }

    void testSet(bool initial)
    {
        unsigned now = msTick();
        bool setValue = !initial;
        bool clearValue = initial;
        const unsigned numBits = 400;
        const unsigned passes = 10000;
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createThreadSafeBitSet();
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        unsigned elapsed = msTick()-now;
        fprintf(stdout, "Bit test (%u) time taken = %dms\n", initial, elapsed);
        now = msTick();
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createBitSet();
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        elapsed = msTick()-now;
        fprintf(stdout, "Bit test [thread-unsafe version] (%u) time taken = %dms\n", initial, elapsed);
        now = msTick();
        size32_t bitSetMemSz = getBitSetMemoryRequirement(numBits+5);
        MemoryBuffer mb;
        void *mem = mb.reserveTruncate(bitSetMemSz);
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createBitSet(bitSetMemSz, mem);
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        elapsed = msTick()-now;
        fprintf(stdout, "Bit test [thread-unsafe version, fixed memory] (%u) time taken = %dms\n", initial, elapsed);
    }

    class CBitThread : public CSimpleInterfaceOf<IInterface>, implements IThreaded
    {
        IBitSet &bitSet;
        unsigned startBit, numBits;
        bool initial, setValue, clearValue;
        CThreaded threaded;
        Owned<IException> exception;
        CppUnit::Exception *cppunitException;
    public:
        CBitThread(IBitSet &_bitSet, unsigned _startBit, unsigned _numBits, bool _initial)
            : threaded("CBitThread", this), bitSet(_bitSet), startBit(_startBit), numBits(_numBits), initial(_initial)
        {
            cppunitException = NULL;
            setValue = !initial;
            clearValue = initial;
        }
        void start() { threaded.start(); }
        void join()
        {
            threaded.join();
            if (exception)
                throw exception.getClear();
            else if (cppunitException)
                throw cppunitException;
        }
        virtual void main()
        {
            try
            {
                unsigned endBit = startBit+numBits-1;
                if (initial)
                    bitSet.incl(startBit, endBit);
                for (unsigned i=startBit; i < endBit; i++)
                {
                    ASSERT(bitSet.test(i) == clearValue);
                    bitSet.set(i, setValue);
                    ASSERT(bitSet.test(i) == setValue);
                    if (i < (endBit-1))
                        ASSERT(bitSet.scan(i, clearValue) == i+1); // find next unset (should be i+1)
                    bitSet.set(i, clearValue);
                    bitSet.invert(i);
                    ASSERT(bitSet.test(i) == setValue);
                    bitSet.invert(i);
                    ASSERT(bitSet.test(i) == clearValue);

                    bool wasSet = bitSet.testSet(i, setValue);
                    ASSERT(wasSet == clearValue);
                    bool wasSet2 = bitSet.testSet(i, clearValue);
                    ASSERT(wasSet2 == setValue);
                    ASSERT(bitSet.test(i) == clearValue);

                    bitSet.set(i, setValue);
                    unsigned match = bitSet.scanInvert(startBit, setValue);
                    ASSERT(match == i);
                    ASSERT(bitSet.test(i) == clearValue);
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
            }
            catch (CppUnit::Exception &e)
            {
                cppunitException = e.clone();
            }
        }
    };
    unsigned testParallelRun(IBitSet &bitSet, unsigned nThreads, unsigned bitsPerThread, bool initial)
    {
        IArrayOf<CBitThread> bitThreads;
        unsigned bitStart = 0;
        unsigned bitEnd = 0;
        for (unsigned t=0; t<nThreads; t++)
        {
            bitThreads.append(* new CBitThread(bitSet, bitStart, bitsPerThread, initial));
            bitStart += bitsPerThread;
        }
        unsigned now = msTick();
        for (unsigned t=0; t<nThreads; t++)
            bitThreads.item(t).start();
        Owned<IException> exception;
        CppUnit::Exception *cppunitException = NULL;
        for (unsigned t=0; t<nThreads; t++)
        {
            try
            {
                bitThreads.item(t).join();
            }
            catch (IException *e)
            {
                EXCLOG(e, NULL);
                if (!exception)
                    exception.setown(e);
                else
                    e->Release();
            }
            catch (CppUnit::Exception *e)
            {
                cppunitException = e;
            }
        }
        if (exception)
            throw exception.getClear();
        else if (cppunitException)
            throw *cppunitException;
        return msTick()-now;
    }

    void testSetParallel(bool initial)
    {
        unsigned numBits = 1000000; // 10M
        unsigned nThreads = getAffinityCpus();
        unsigned bitsPerThread = numBits/nThreads;
        bitsPerThread = ((bitsPerThread + (BitsPerItem-1)) / BitsPerItem) * BitsPerItem; // round up to multiple of BitsPerItem
        numBits = bitsPerThread*nThreads; // round

        fprintf(stdout, "testSetParallel, testing bit set of size : %d, nThreads=%d\n", numBits, nThreads);

        Owned<IBitSet> bitSet = createThreadSafeBitSet();
        unsigned took = testParallelRun(*bitSet, nThreads, bitsPerThread, initial);
        fprintf(stdout, "Thread safe parallel bit set test (%u) time taken = %dms\n", initial, took);

        size32_t bitSetMemSz = getBitSetMemoryRequirement(numBits);
        MemoryBuffer mb;
        void *mem = mb.reserveTruncate(bitSetMemSz);
        bitSet.setown(createBitSet(bitSetMemSz, mem));
        took = testParallelRun(*bitSet, nThreads, bitsPerThread, initial);
        fprintf(stdout, "Thread unsafe parallel bit set test (%u) time taken = %dms\n", initial, took);
    }

    void testSimple()
    {
        testSet(false);
        testSet(true);
        testSetParallel(false);
        testSetParallel(true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibSetTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSetTest, "JlibSetTest" );

/* =========================================================== */
class JlibFileIOTest : public CppUnit::TestFixture
{
    unsigned rs, nr10pct, nr150pct;
    char *record;
    StringBuffer tmpfile;
    StringBuffer server;

    CPPUNIT_TEST_SUITE( JlibFileIOTest );
        CPPUNIT_TEST(testIOSmall);
        CPPUNIT_TEST(testIORemote);
        CPPUNIT_TEST(testIOLarge);
    CPPUNIT_TEST_SUITE_END();

public:
    JlibFileIOTest()
    {
        HardwareInfo hdwInfo;
        getHardwareInfo(hdwInfo);
        rs = 65536;
        unsigned nr = (unsigned)(1024.0 * (1024.0 * (double)hdwInfo.totalMemory / (double)rs));
        nr10pct = nr / 10;
        nr150pct = (unsigned)((double)nr * 1.5);
        record = (char *)malloc(rs);
        for (unsigned i=0;i<rs;i++)
            record[i] = 'a';
        record[rs-1] = '\n';

        tmpfile.set("JlibFileIOTest.txt");
        server.set(".");
        // server.set("192.168.1.18");
    }

    ~JlibFileIOTest()
    {
        free(record);
    }

protected:
    void testIO(unsigned nr, SocketEndpoint *ep)
    {
        IFile *ifile;
        IFileIO *ifileio;
        unsigned fsize = (unsigned)(((double)nr * (double)rs) / (1024.0 * 1024.0));

        fflush(NULL);
        fprintf(stdout,"\n");
        fflush(NULL);

        for(int j=0; j<2; j++)
        {
            if (j==0)
                fprintf(stdout, "File size: %d (MB) Cache, ", fsize);
            else
                fprintf(stdout, "\nFile size: %d (MB) Nocache, ", fsize);

            if (ep != NULL)
            {
                ifile = createRemoteFile(*ep, tmpfile);
                fprintf(stdout, "Remote: (%s)\n", server.str());
            }
            else
            {
                ifile = createIFile(tmpfile);
                fprintf(stdout, "Local:\n");
            }

            ifile->remove();

            unsigned st = msTick();

            IFEflags extraFlags = IFEcache;
            if (j==1)
                extraFlags = IFEnocache;
            ifileio = ifile->open(IFOcreate, extraFlags);

            unsigned iter = nr / 40;

            __int64 pos = 0;
            for (unsigned i=0;i<nr;i++)
            {
                ifileio->write(pos, rs, record);
                pos += rs;
                if ((i % iter) == 0)
                {
                    fprintf(stdout,".");
                    fflush(NULL);
                }
            }

            ifileio->close();

            double rsec = (double)(msTick() - st)/1000.0;
            unsigned iorate = (unsigned)((double)fsize / rsec);

            fprintf(stdout, "\nwrite - elapsed time = %6.2f (s) iorate = %4d (MB/s)\n", rsec, iorate);

            st = msTick();

            extraFlags = IFEcache;
            if (j==1)
                extraFlags = IFEnocache;
            ifileio = ifile->open(IFOread, extraFlags);

            pos = 0;
            for (unsigned i=0;i<nr;i++)
            {
                ifileio->read(pos, rs, record);
                pos += rs;
                if ((i % iter) == 0)
                {
                    fprintf(stdout,".");
                    fflush(NULL);
                }
            }

            ifileio->close();

            rsec = (double)(msTick() - st)/1000.0;
            iorate = (unsigned)((double)fsize / rsec);

            fprintf(stdout, "\nread -- elapsed time = %6.2f (s) iorate = %4d (MB/s)\n", rsec, iorate);

            ifileio->Release();
            ifile->remove();
            ifile->Release();
        }
    }

    void testIOSmall()
    {
        testIO(nr10pct, NULL);
    }

    void testIOLarge()
    {
        testIO(nr150pct, NULL);
    }

    void testIORemote()
    {
        SocketEndpoint ep;
        ep.set(server, 7100);
        testIO(nr10pct, &ep);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileIOTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileIOTest, "JlibFileIOTest" );

/* =========================================================== */

class JlibStringBufferTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( JlibStringBufferTest );
        CPPUNIT_TEST(testSwap);
    CPPUNIT_TEST_SUITE_END();

public:
    JlibStringBufferTest()
    {
    }

    void testSwap()
    {
        StringBuffer l;
        StringBuffer r;
        for (unsigned len=0; len<40; len++)
        {
            const unsigned numIter = 100000000;
            cycle_t start = get_cycles_now();

            for (unsigned pass=0; pass < numIter; pass++)
            {
                l.swapWith(r);
            }
            cycle_t elapsed = get_cycles_now() - start;
            fprintf(stdout, "iterations of size %u took %.2f\n", len, (double)cycle_to_nanosec(elapsed) / numIter);
            l.append("a");
            r.append("b");
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibStringBufferTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibStringBufferTest, "JlibStringBufferTest" );



//---------------------------------------------------------------------------------------------------------------------

/*
For comparison, this example of a lock free queue is taken from http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the authors and should not be interpreted as representing official policies, either expressed or implied, of Dmitry Vyukov.
*/

template<typename T>
class mpmc_bounded_queue
{
public:
  mpmc_bounded_queue(size_t buffer_size)
    : buffer_(new cell_t [buffer_size])
    , buffer_mask_(buffer_size - 1)
  {
    assert((buffer_size >= 2) &&
      ((buffer_size & (buffer_size - 1)) == 0));
    for (size_t i = 0; i != buffer_size; i += 1)
      buffer_[i].sequence_.store(i, std::memory_order_relaxed);
    enqueue_pos_.store(0, std::memory_order_relaxed);
    dequeue_pos_.store(0, std::memory_order_relaxed);
  }

  ~mpmc_bounded_queue()
  {
    delete [] buffer_;
  }

  bool enqueue(T const& data)
  {
    cell_t* cell;
    size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
    for (;;)
    {
      cell = &buffer_[pos & buffer_mask_];
      size_t seq =
        cell->sequence_.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)pos;
      if (dif == 0)
      {
        if (enqueue_pos_.compare_exchange_weak
            (pos, pos + 1, std::memory_order_relaxed))
          break;
      }
      else if (dif < 0)
        continue;//return false;
      else
        pos = enqueue_pos_.load(std::memory_order_relaxed);
    }
    cell->data_ = data;
    cell->sequence_.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool dequeue(T& data)
  {
    cell_t* cell;
    size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
    for (;;)
    {
      cell = &buffer_[pos & buffer_mask_];
      size_t seq =
        cell->sequence_.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif == 0)
      {
        if (dequeue_pos_.compare_exchange_weak
            (pos, pos + 1, std::memory_order_relaxed))
          break;
      }
      else if (dif < 0)
        continue;//return false;
      else
        pos = dequeue_pos_.load(std::memory_order_relaxed);
    }
    data = cell->data_;
    cell->sequence_.store
      (pos + buffer_mask_ + 1, std::memory_order_release);
    return true;
  }

private:
  struct cell_t
  {
    std::atomic<size_t>   sequence_;
    T                     data_;
  };

  static size_t const     cacheline_size = 64;
  typedef char            cacheline_pad_t [cacheline_size];

  cacheline_pad_t         pad0_;
  cell_t* const           buffer_;
  size_t const            buffer_mask_;
  cacheline_pad_t         pad1_;
  std::atomic<size_t>     enqueue_pos_;
  cacheline_pad_t         pad2_;
  std::atomic<size_t>     dequeue_pos_;
  cacheline_pad_t         pad3_;

  mpmc_bounded_queue(mpmc_bounded_queue const&);
  void operator = (mpmc_bounded_queue const&);
};

template<typename T>
class spsc_bounded_queue
{
public:
  spsc_bounded_queue(size_t buffer_size)
    : buffer_(new cell_t [buffer_size])
    , buffer_mask_(buffer_size - 1)
  {
    assert((buffer_size >= 2) &&
      ((buffer_size & (buffer_size - 1)) == 0));
    for (size_t i = 0; i != buffer_size; i += 1)
      buffer_[i].sequence_.store(i, std::memory_order_relaxed);
    enqueue_pos_ = 0;
    dequeue_pos_ = 0;
  }

  ~spsc_bounded_queue()
  {
    delete [] buffer_;
  }

  bool enqueue(T const& data)
  {
    cell_t* cell;
    size_t pos = enqueue_pos_;
    for (;;)
    {
      cell = &buffer_[pos & buffer_mask_];
      size_t seq =
        cell->sequence_.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)pos;
      if (dif == 0)
      {
        enqueue_pos_ = pos + 1;
        break;
      }
      //pause/
    }
    cell->data_ = data;
    cell->sequence_.store(pos + 1, std::memory_order_release);
    return true;
  }

  bool dequeue(T& data)
  {
    cell_t* cell;
    size_t pos = dequeue_pos_;
    for (;;)
    {
      cell = &buffer_[pos & buffer_mask_];
      size_t seq =
        cell->sequence_.load(std::memory_order_acquire);
      intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
      if (dif == 0)
      {
        dequeue_pos_ = pos + 1;
        break;
      }
      //pause
    }
    data = cell->data_;
    cell->sequence_.store
      (pos + buffer_mask_ + 1, std::memory_order_release);
    return true;
  }

private:
  struct cell_t
  {
    std::atomic<size_t>   sequence_;
    T                     data_;
  };

  static size_t const     cacheline_size = 64;
  typedef char            cacheline_pad_t [cacheline_size];

  cacheline_pad_t         pad0_;
  cell_t* const           buffer_;
  size_t const            buffer_mask_;
  cacheline_pad_t         pad1_;
  size_t                  enqueue_pos_;
  cacheline_pad_t         pad2_;
  size_t                  dequeue_pos_;
  cacheline_pad_t         pad3_;

  spsc_bounded_queue(spsc_bounded_queue const&);
  void operator = (spsc_bounded_queue const&);
};

unsigned nextPowerOfTwo(unsigned value)
{
   unsigned ret = 2;
    while (ret < value)
        ret += ret;
    return ret;
}

class QueueBase : implements CInterfaceOf<IRowQueue>
{
public:
    using IRowQueue::enqueue;
    virtual unsigned enqueue(size_t count, const void * * items)
    {
        for (unsigned i = 0; i < count; i++)
            enqueue(items[i]);
        return count;
    }
};

class CMPMCQueue : public QueueBase
{
public:
    CMPMCQueue(unsigned _maxItems) : queue(nextPowerOfTwo(_maxItems)) {}

    virtual void enqueue(const void * const item)
    {
        while (!queue.enqueue(item))
        {
        }
    }
    virtual const void * dequeue()
    {
        const void * ret;
        while (!queue.dequeue(ret))
        {
        }
        return ret;
    }

private:
    mpmc_bounded_queue<const void *> queue;
};

class CSPSCQueue : public QueueBase
{
public:
    CSPSCQueue(unsigned _maxItems) : queue(nextPowerOfTwo(_maxItems)) {}

    virtual void enqueue(const void * const item)
    {
        queue.enqueue(item);
    }
    virtual const void * dequeue()
    {
        const void * ret;
        queue.dequeue(ret);
        return ret;
    }

private:
    spsc_bounded_queue<const void *> queue;
};


//Note, the semaphores don't prevent this being required to loop - just reduce it.
class CMPMCSemQueue : public QueueBase
{
public:
    CMPMCSemQueue(unsigned _maxItems) : queue(nextPowerOfTwo(_maxItems)), space(_maxItems) {}

    virtual void enqueue(const void * const item)
    {
        space.wait();
        //This still has to loop because the elements may not be returned from dequeue in order.
        while (!queue.enqueue(item))
        {}
        avail.signal();
    }
    virtual const void * dequeue()
    {
        avail.wait();
        const void * ret = NULL;
        while (!queue.dequeue(ret))
        {}
        space.signal();
        return ret;
    }

private:
    mpmc_bounded_queue<const void *> queue;
    Semaphore avail;
    Semaphore space;
};

IRowQueue * createMPMCQueue(unsigned queueElements, unsigned numConsumers, unsigned numProducers)
{
    if ((numConsumers == 1) && (numProducers == 1))
        return new CSPSCQueue(queueElements);
    return new CMPMCQueue(queueElements);
}

#if 0
#undef likely
#undef unlikely
#include "concurrentqueue.h"
#include "blockingconcurrentqueue.h"

typedef moodycamel::ConcurrentQueue<const void *> MoodyRowQueue;

class MoodyQueue : public QueueBase
{
public:
    MoodyQueue(unsigned _maxItems) : queue(_maxItems) {}

    virtual void enqueue(const void * const item)
    {
        //This still has to loop because the elements may not be returned from dequeue in order.
        while (!queue.try_enqueue(item))
        {}
    }
    virtual const void * dequeue()
    {
        const void * ret = NULL;
        while (!queue.try_dequeue(ret))
        {}
        return ret;
    }

private:
    MoodyRowQueue queue;
};

typedef moodycamel::BlockingConcurrentQueue<const void *> BlockingMoodyRowQueue;

class BlockingMoodyQueue : implements CInterfaceOf<IRowQueue>
{
public:
    BlockingMoodyQueue(unsigned _maxItems) : queue(_maxItems) {}

    virtual void enqueue(const void * const item)
    {
        //This still has to loop because the elements may not be returned from dequeue in order.
        while (!queue.try_enqueue(item))
        {}
    }
    virtual const void * dequeue()
    {
        const void * ret = NULL;
        while (!queue.try_dequeue(ret))
        {}
        return ret;
    }

private:
    BlockingMoodyRowQueue queue;
};

#endif


/* =========================================================== */

class JlibReaderWriterTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibReaderWriterTest);
    CPPUNIT_TEST(testCombinations);
    CPPUNIT_TEST_SUITE_END();

    const static unsigned spinScaling = 1000;

    static unsigned spinCalculation(unsigned prev, unsigned scale)
    {
        unsigned value = prev;
        for (unsigned i = 0; i < scale*spinScaling; i++)
        {
            value = (value * 0x1234FEDB + 0x87654321);
        }
        return value;
    }

    class Reader : public Thread
    {
    public:
        Reader(IRowQueue & _source, Semaphore & _doneSem, unsigned _workScale)
            : Thread("Reader"), source(_source), doneSem(_doneSem), workScale(_workScale), work(0)
        {
        }

        virtual int run()
        {
            loop
            {
                const void * next = source.dequeue();
                if (!next)
                    break;
                std::atomic<byte> * value = (std::atomic<byte> *)next;
                (*value)++;
                if (workScale)
                    work = spinCalculation(work, workScale);
            }
            doneSem.signal();
            return 0;
        }

    private:
        IRowQueue & source;
        Semaphore & doneSem;
        volatile unsigned work;
        unsigned workScale;
    };

    class WriterBase : public Thread
    {
    public:
        WriterBase(IRowQueue & _target, size_t _len, byte * _buffer, Semaphore & _startSem, Semaphore & _doneSem, unsigned _workScale)
            : Thread("Writer"), target(_target), len(_len), buffer(_buffer), startSem(_startSem), doneSem(_doneSem), workScale(_workScale), work(0)
        {
        }

    protected:
        size_t len;
        byte * buffer;
        IRowQueue & target;
        Semaphore & startSem;
        Semaphore & doneSem;
        volatile unsigned work;
        unsigned workScale;
    };
    class Writer : public WriterBase
    {
    public:
        Writer(IRowQueue & _target, size_t _len, byte * _buffer, Semaphore & _startSem, Semaphore & _doneSem, unsigned _workScale)
            : WriterBase(_target, _len, _buffer, _startSem, _doneSem, _workScale)
        {
        }

        virtual int run()
        {
            startSem.wait();
            for (size_t i = 0; i < len; i++)
            {
                if (workScale)
                    work = spinCalculation(work, workScale);
                const void * rows = buffer+i;
                target.enqueue(1, &rows);//buffer + i);
            }
            doneSem.signal();
            return 0;
        }
    };
    class BlockWriter : public WriterBase
    {
    public:
        BlockWriter(IRowQueue & _target, size_t _len, byte * _buffer, Semaphore & _startSem, Semaphore & _doneSem, unsigned _workScale, unsigned _blockSize)
            : WriterBase(_target, _len, _buffer, _startSem, _doneSem, _workScale), blockSize(_blockSize)
        {
            rows = new const void * [blockSize];
        }
        ~BlockWriter()
        {
            delete [] rows;
        }

        virtual int run()
        {
            startSem.wait();
            unsigned remaining = 0;
            unsigned space = blockSize;
            for (size_t i = 0; i < len || remaining;)
            {
                unsigned cnt = len - i;
                if (cnt > space)
                    cnt = space;
                if (workScale)
                    work = spinCalculation(work, workScale*cnt);
                for (unsigned j=0; j < cnt; j++)
                {
                    rows[j+remaining] = buffer+i+j;
                }

                unsigned chunkSize = remaining + cnt;
                unsigned consumed = target.enqueue(chunkSize, rows);  // should there be a call that blocks until all added?
                remaining = chunkSize - consumed;
                if (remaining)
                    memmove(rows, rows+consumed, remaining * sizeof(void*));
                space = consumed;
                i += cnt;
            }
            doneSem.signal();
            return 0;
        }
    protected:
        const void * * rows;
        unsigned blockSize;
    };
public:
    const static size_t bufferSize = 0x100000;//0x100000*64;
    void testQueue(IRowQueue & queue, unsigned numProducers, unsigned numConsumers, unsigned queueElements, unsigned readerWork, unsigned writerWork, unsigned blockSize)
    {
        const size_t sizePerProducer = bufferSize / numProducers;
        const size_t testSize = sizePerProducer * numProducers;

        OwnedMalloc<byte> buffer(bufferSize, true);
        Semaphore startSem;
        Semaphore writerDoneSem;
        Semaphore stopSem;

        Reader * * consumers = new Reader *[numConsumers];
        for (unsigned i2 = 0; i2 < numConsumers; i2++)
        {
            consumers[i2] = new Reader(queue, stopSem, readerWork);
            consumers[i2]->start();
        }

        WriterBase * * producers = new WriterBase *[numProducers];
        for (unsigned i1 = 0; i1 < numProducers; i1++)
        {
            if (blockSize == 0)
                producers[i1] = new Writer(queue, sizePerProducer, buffer + i1 * sizePerProducer, startSem, writerDoneSem, writerWork);
            else
                producers[i1] = new BlockWriter(queue, sizePerProducer, buffer + i1 * sizePerProducer, startSem, writerDoneSem, writerWork, blockSize);

            producers[i1]->start();
        }

        cycle_t startTime = get_cycles_now();

        //Start the writers
        startSem.signal(numProducers);

        //Wait for the writers to complete
        for (unsigned i7 = 0; i7 < numProducers; i7++)
            writerDoneSem.wait();

        //Now add NULL records to the queue so the consumers know to terminate
        for (unsigned i8 = 0; i8 < numConsumers; i8++)
            queue.enqueue(NULL);

        //Wait for the readers to complete
        for (unsigned i3 = 0; i3 < numConsumers; i3++)
            stopSem.wait();

        cycle_t stopTime = get_cycles_now();

        //All bytes should have been changed to 1, if not a queue item got lost.
        unsigned failures = 0;
        unsigned numClear = 0;
        size_t failPos = ~(size_t)0;
        byte failValue = 0;
        for (size_t pos = 0; pos < testSize; pos++)
        {
            if (buffer[pos] != 1)
            {
                failures++;
                if (failPos == ~(size_t)0)
                {
                    failPos = pos;
                    failValue = buffer[pos];
                }
            }

            if (buffer[pos] == 0)
                numClear++;
        }

        unsigned timeMs = cycle_to_nanosec(stopTime - startTime) / 1000000;
        if (failures)
        {
            printf("Fail: Test %u producers %u consumers %u queueItems %u(%u) mismatches fail(@%u=%u)\n", numProducers, numConsumers, queueElements, failures, numClear, (unsigned)failPos, failValue);
            ASSERT(failures == 0);
        }
        else
            printf("Pass: Test %u(@%u) producers %u(@%u) consumers %u queueItems block(%u) in %ums\n", numProducers, writerWork, numConsumers, readerWork, queueElements, blockSize, timeMs);


        for (unsigned i4 = 0; i4 < numConsumers; i4++)
        {
            consumers[i4]->join();
            consumers[i4]->Release();
        }
        delete[] consumers;

        for (unsigned i5 = 0; i5 < numProducers; i5++)
        {
            producers[i5]->join();
            producers[i5]->Release();
        }
        delete[] producers;
    }

    void testQueue(unsigned numProducers, unsigned numConsumers, unsigned numElements = 0, unsigned readWork = 0, unsigned writeWork = 0, unsigned blockSize = 0)
    {
        unsigned queueElements = (numElements != 0) ? numElements : (numProducers + numConsumers) * 2;
        Owned<IRowQueue> queue = createRowQueue(numConsumers, numProducers, queueElements, 0);
        //Owned<IRowQueue> queue = createMPMCQueue(queueElements, numConsumers, numProducers);
        //Owned<IRowQueue> queue = new CMPMCSemQueue(queueElements);
        //Owned<IRowQueue> queue = new MoodyQueue(queueElements);
        //Owned<IRowQueue> queue = new BlockingMoodyQueue(queueElements);

        testQueue(*queue, numProducers, numConsumers, queueElements, readWork, writeWork, blockSize);
    }

    void testWorkQueue(unsigned numProducers, unsigned numConsumers, unsigned numElements)
    {
        for (unsigned readWork = 1; readWork <= 8; readWork = readWork * 2)
        {
            for (unsigned writeWork = 1; writeWork <= 8; writeWork = writeWork * 2)
            {
                testQueue(numProducers, numConsumers, numElements, readWork, writeWork);
            }
        }
    }
    void testCombinations()
    {
        // 1:1
        for (unsigned i=0; i < 10; i++)
            testQueue(1, 1, 10);
        testQueue(1, 1, 10, 0, 0, 16);
        testQueue(1, 1, 64, 0, 0, 16);

        //One to Many
        testQueue(1, 10, 5);
        testQueue(1, 5, 5);
        testQueue(1, 5, 10);
        testQueue(1, 127, 10);
        testQueue(1, 127, 127);

        //Many to One
        testQueue(10, 1, 5);
        testQueue(5, 1, 5);
        testQueue(5, 1, 10);
        testQueue(127, 1, 127);

        //How does it scale with number of queue elements?
        for (unsigned elem = 16; elem < 256; elem *= 2)
        {
            testQueue(16, 1, elem, 1, 1, 0);
            testQueue(16, 1, elem, 1, 1, 1);
            testQueue(16, 1, elem, 1, 1, 23);
            testQueue(16, 1, elem, 1, 4, 23);
        }

        cycle_t startTime = get_cycles_now();
        volatile unsigned value = 0;
        for (unsigned i2 = 0; i2 < bufferSize; i2++)
            value = spinCalculation(value, 1);
        cycle_t stopTime = get_cycles_now();
        unsigned timeMs = cycle_to_nanosec(stopTime - startTime) / 1000000;
        printf("Work(1) takes %ums\n", timeMs);

#if 1
        //Many to Many
        for (unsigned readWork = 1; readWork <= 8; readWork = readWork * 2)
        {
            for (unsigned writeWork = 1; writeWork <= 8; writeWork = writeWork * 2)
            {
                testQueue(1, 1, 63, readWork, writeWork);
                testQueue(1, 2, 63, readWork, writeWork);
                testQueue(1, 4, 63, readWork, writeWork);
                testQueue(1, 8, 63, readWork, writeWork);
                testQueue(1, 16, 63, readWork, writeWork);
                testQueue(2, 1, 63, readWork, writeWork);
                testQueue(4, 1, 63, readWork, writeWork);
                testQueue(8, 1, 63, readWork, writeWork);
                testQueue(16, 1, 63, readWork, writeWork);

                testQueue(2, 2, 63, readWork, writeWork);
                testQueue(4, 4, 63, readWork, writeWork);
                testQueue(8, 8, 63, readWork, writeWork);
            }

        }
#else
        //Many to Many
        testWorkQueue(1, 1, 63);
        testWorkQueue(1, 2, 63);
        testWorkQueue(1, 4, 63);
        testWorkQueue(1, 8, 63);
        testWorkQueue(1, 16, 63);
        testWorkQueue(2, 1, 63);
        testWorkQueue(4, 1, 63);
        testWorkQueue(8, 1, 63);
        testWorkQueue(16, 1, 63);

        testWorkQueue(2, 2, 63);
        testWorkQueue(4, 4, 63);
        testWorkQueue(8, 8, 63);
#endif

        testQueue(2, 2, 4);
        testQueue(2, 2, 8);
        testQueue(2, 2, 16);
        testQueue(2, 2, 32);
        testQueue(2, 2, 100);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibReaderWriterTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibReaderWriterTest, "JlibReaderWriterTest");

#endif // _USE_CPPUNIT
