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

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "hqlexpr.hpp"

class ThreadedParseStressTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ThreadedParseStressTest  );
        CPPUNIT_TEST(testAllocs);
        CPPUNIT_TEST(testThreads);
    CPPUNIT_TEST_SUITE_END();

    void testThreads(unsigned maxParallel)
    {
        const unsigned numIter = 20000;
        const unsigned numFor = 50;
        CCycleTimer timer;
        class casyncfor: public CAsyncFor
        {
        public:
            void Do(unsigned i)
            {
                if (i & 1)
                {
                    const char * query =
                            "RECORD\n"
                            "  unsigned2 v21;\n"
                            "  unsigned2 v12;\n"
                            "  unsigned2 v8;\n"
                            "  real8 factor;\n"
                            " END;\n";
                    OwnedHqlExpr parsed;
                    for (unsigned i=0; i < numIter; i++)
                    {
                        OwnedHqlExpr next = parseQuery(query, nullptr);
                    }
                }
                else
                {
                    for (unsigned i=0; i < numIter*2; i++)
                    {
                        //Allocate something the same size as CLocationAnnotation
                        free(calloc(72, 1));
                        /*
                        try
                        {
                            SocketEndpoint ep(99);
//                            Owned<ISocket> temp = ISocket::connect_timeout(ep, 0);
                            Owned<ISocket> temp = ISocket::create(99);
                        }
                        catch (IException * e)
                        {
                            e->Release();
                        }*/
                    }
                }
            }
        } afor;
        afor.For(numFor, maxParallel, false, false);
        unsigned __int64 numNs = timer.elapsedNs();
        unsigned __int64 totalIters = numFor * numIter;
        printf("test %u threads took %uns each parse\n", maxParallel, (unsigned)(numNs/(totalIters)));
    }

    void testThreads()
    {
        testThreads(2);
        testThreads(3);
        testThreads(4);
        testThreads(8);
        testThreads(16);
        testThreads(32);
    }

    void testAllocs(unsigned maxParallel)
    {
        const unsigned numIter = 100000;
        CCycleTimer timer;
        class casyncfor: public CAsyncFor
        {
        public:
            void Do(unsigned i)
            {
                OwnedHqlExpr zero = createConstant(0);
                for (unsigned i=0; i < numIter; i++)
                {
                    OwnedHqlExpr annot = createLocationAnnotation(LINK(zero), nullptr, 0, 0);
                }
            }
        } afor;
        afor.For(100, maxParallel, false, false);
        unsigned __int64 numNs = timer.elapsedNs();
        unsigned __int64 totalIters = 100 * numIter;
        printf("test %u threads took %uns each alloc\n", maxParallel, (unsigned)(numNs/(totalIters)));
    }

    void testAllocs()
    {
        testAllocs(2);
        testAllocs(3);
        testAllocs(4);
        testAllocs(8);
        testAllocs(16);
        testAllocs(32);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( ThreadedParseStressTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ThreadedParseStressTest, "ThreadedParseStressTest" );

#endif // _USE_CPPUNIT
