/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testSet(bool initial)
    {
        unsigned now = msTick();
        bool setValue = !initial;
        bool clearValue = initial;
        const unsigned numBits = 400;
        for (unsigned pass=0; pass< 10000; pass++)
        {
            Owned<IBitSet> bs = createBitSet();
            if (initial)
                bs->incl(0, numBits);
            for (unsigned i=0; i < numBits; i++)
            {
                ASSERT(bs->test(i) == clearValue);
                bs->set(i, setValue);
                ASSERT(bs->test(i) == setValue);

                bs->set(i+5, setValue);
                ASSERT(bs->scan(0, setValue) == i);
                ASSERT(bs->scan(i+1, setValue) == i+5);
                bs->set(i, clearValue);
                bs->set(i+5, clearValue);
                unsigned match1 = bs->scan(0, setValue);
                ASSERT(match1 == initial ? -1 : numBits);

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
        }
        unsigned elapsed = msTick()-now;
        fprintf(stdout, "Bit test (%u) time taken = %dms\n", initial, elapsed);
    }

    void testSimple()
    {
        testSet(false);
        testSet(true);
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


#endif // _USE_CPPUNIT
