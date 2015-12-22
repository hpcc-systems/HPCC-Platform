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


/* =========================================================== */

static const unsigned split4_2[] = {0, 2, 4 };
static const unsigned split100_2[] = {0, 50, 100  };
static const unsigned split100_10[] = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100  };
static const unsigned split7_10[] = {0,1,1,2,3,3,4,5,6,6,7 };
static const unsigned split10_3[] = {0,3,7,10 };
static const unsigned split58_10[] = {0,6,12,17,23,29,35,41,46,52,58 };
static const unsigned split9_2T[] = { 0,5,9 };
static const unsigned split9_2F[] = { 0,4,9 };
static const unsigned split15_3[] = { 0,5,10,15 };

class JlibQuantileTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( JlibQuantileTest );
        CPPUNIT_TEST(testQuantile);
        CPPUNIT_TEST(testRandom);
    CPPUNIT_TEST_SUITE_END();

public:
    JlibQuantileTest()
    {
    }

    void testQuantilePos(unsigned numItems, unsigned numDivisions, bool roundUp, const unsigned * expected)
    {
        if (numDivisions == 0)
            return;

        QuantilePositionIterator iter(numItems, numDivisions, roundUp);
        QuantileFilterIterator filter(numItems, numDivisions, roundUp);

        unsigned prevPos = 0;
        iter.first();
        for (unsigned i=0; i <= numDivisions; i++)
        {
            //Check the values from the quantile iterator match those that are expected
            unsigned pos = (unsigned)iter.get();
#if 0
            printf("(%d,%d) %d=%d\n", numItems, numDivisions, i, pos);
#endif
            if (expected)
                CPPUNIT_ASSERT_EQUAL(expected[i], pos);

            //Check that the quantile filter correctly returns true and false for subsequent calls.
            while (prevPos < pos)
            {
                CPPUNIT_ASSERT(!filter.get());
                filter.next();
                prevPos++;
            }

            if (prevPos == pos)
            {
                CPPUNIT_ASSERT(filter.get());
                filter.next();
                prevPos++;
            }
            iter.next();
        }
    }

    void testQuantile()
    {
        testQuantilePos(4, 2, false, split4_2);
        testQuantilePos(100, 2, false, split100_2);
        testQuantilePos(100, 10, false, split100_10);
        testQuantilePos(7, 10, false, split7_10);
        testQuantilePos(10, 3, false, split10_3);
        testQuantilePos(10, 3, true, split10_3);
        testQuantilePos(58, 10, false, split58_10);
        //testQuantilePos(9, 2, true, split9_2T);
        testQuantilePos(9, 2, false, split9_2F);
        testQuantilePos(15, 3, false, split15_3);
        testQuantilePos(1231, 57, false, NULL);
        testQuantilePos(1, 63, false, NULL);
        testQuantilePos(10001, 17, false, NULL);
    }
    void testRandom()
    {
        //test various random combinations to ensure the results are consistent.
        for (unsigned i=0; i < 10; i++)
            testQuantilePos(random() % 1000000, random() % 10000, true, NULL);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibQuantileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibQuantileTest, "JlibQuantileTest" );


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
                const void * next;
                if (!source.dequeue(next))
                    break;
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
                target.enqueue(buffer + i);
            }
            doneSem.signal();
            return 0;
        }
    };
public:
    JlibReaderWriterTest() { unitWorkTimeMs = 0; }

    const static size_t bufferSize = 0x100000;//0x100000*64;
    void testQueue(IRowQueue & queue, unsigned numProducers, unsigned numConsumers, unsigned queueElements, unsigned readerWork, unsigned writerWork)
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
            producers[i1] = new Writer(queue, sizePerProducer, buffer + i1 * sizePerProducer, startSem, writerDoneSem, writerWork);
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
        unsigned expectedReadWorkTime = (unsigned)(((double)unitWorkTimeMs * readerWork) / numConsumers);
        unsigned expectedWriteWorkTime = (unsigned)(((double)unitWorkTimeMs * writerWork) / numProducers);
        unsigned expectedWorkTime = std::max(expectedReadWorkTime, expectedWriteWorkTime);
        if (failures)
        {
            printf("Fail: Test %u producers %u consumers %u queueItems %u(%u) mismatches fail(@%u=%u)\n", numProducers, numConsumers, queueElements, failures, numClear, (unsigned)failPos, failValue);
            ASSERT(failures == 0);
        }
        else
            printf("Pass: Test %u(@%u) producers %u(@%u) consumers %u queueItems in %ums [%dms]\n", numProducers, writerWork, numConsumers, readerWork, queueElements, timeMs, timeMs-expectedWorkTime);

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

    void testQueue(unsigned numProducers, unsigned numConsumers, unsigned numElements = 0, unsigned readWork = 0, unsigned writeWork = 0)
    {
        unsigned queueElements = (numElements != 0) ? numElements : (numProducers + numConsumers) * 2;
        Owned<IRowQueue> queue = createRowQueue(numConsumers, numProducers, queueElements, 0);
        testQueue(*queue, numProducers, numConsumers, queueElements, readWork, writeWork);
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

        cycle_t startTime = get_cycles_now();
        volatile unsigned value = 0;
        for (unsigned pass = 0; pass < 10; pass++)
        {
            for (unsigned i2 = 0; i2 < bufferSize; i2++)
                value = spinCalculation(value, 1);
        }
        cycle_t stopTime = get_cycles_now();
        unitWorkTimeMs = cycle_to_nanosec(stopTime - startTime) / (1000000 * 10);
        printf("Work(1) takes %ums\n", unitWorkTimeMs);

        //How does it scale with number of queue elements?
        for (unsigned elem = 16; elem < 256; elem *= 2)
        {
            testQueue(16, 1, elem, 1, 1);
        }

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
                testQueue(16, 8, 63, readWork, writeWork);
                testQueue(16, 16, 63, readWork, writeWork);
                testQueue(32, 1, 63, readWork, writeWork);
                testQueue(64, 1, 63, readWork, writeWork);
                testQueue(1, 32, 63, readWork, writeWork);
                testQueue(1, 64, 63, readWork, writeWork);
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

protected:
    unsigned unitWorkTimeMs;
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibReaderWriterTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibReaderWriterTest, "JlibReaderWriterTest");

#endif // _USE_CPPUNIT
