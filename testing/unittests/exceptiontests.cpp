/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "thormisc.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include <thread>
#include <atomic>
#include <vector>
#include <chrono>

class ExceptionTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ExceptionTest);
    CPPUNIT_TEST(testThorWrapException);
    CPPUNIT_TEST(testThorWrapExceptionConcurrentAccess);
    CPPUNIT_TEST_SUITE_END();

private:
    GraphContextCallback originalCallback;

public:
    void setUp()
    {
        // Save the original callback state
        originalCallback = nullptr;
        setGraphContextCallback(nullptr);
    }

    void tearDown()
    {
        // Restore the original callback state
        setGraphContextCallback(originalCallback);
    }

    // Test callback that provides graph context
    static void testGraphCallback(StringBuffer &graphName, graph_id &subGraphId)
    {
        graphName.append("TestGraph");
        subGraphId = 42;
    }

    // Test callback that provides empty graph name
    static void emptyGraphCallback(StringBuffer &graphName, graph_id &subGraphId)
    {
        graphName.clear();
        subGraphId = 99;
    }

    // Test callback that throws an exception
    static void failingGraphCallback(StringBuffer &graphName, graph_id &subGraphId)
    {
        throw MakeStringException(1, "Callback failure");
    }

    void testThorWrapException()
    {
        // Test 1: ThorWrapException without any callback set
        setGraphContextCallback(nullptr);
        try
        {
            throw MakeStringException(1, "Original error");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = ThorWrapException(e, "System error");

            // Should have the wrapped message but no graph context
            StringBuffer msg;
            thorEx->errorMessage(msg);

            CPPUNIT_ASSERT_EQUAL_STR("1, Original error : System error", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test 2: ThorWrapException with callback providing graph context
        setGraphContextCallback(testGraphCallback);
        try
        {
            throw MakeStringException(1, "Original error");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = ThorWrapException(e, "System error");

            // Should have the wrapped message with graph context
            StringBuffer msg;
            thorEx->errorMessage(msg);

            CPPUNIT_ASSERT_EQUAL_STR("1, Original error : System error, Graph TestGraph[42]", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test 3: ThorWrapException when callback throws an exception
        setGraphContextCallback(failingGraphCallback);
        try
        {
            throw MakeStringException(1, "Original error");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = ThorWrapException(e, "System error");

            // Should have the wrapped message but no graph context due to callback failure
            StringBuffer msg;
            thorEx->errorMessage(msg);

            CPPUNIT_ASSERT_EQUAL_STR("1, Original error : System error", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test 4: Specific graph context format
        setGraphContextCallback(testGraphCallback);
        try
        {
            throw MakeStringException(123, "Test exception");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = ThorWrapException(e, "Operation failed in %s", "worker");

            // Should have the wrapped message with graph context
            StringBuffer msg;
            thorEx->errorMessage(msg);

            CPPUNIT_ASSERT_EQUAL_STR("123, Test exception : Operation failed in worker, Graph TestGraph[42]", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test 5: Multiple callbacks - setting a new callback replaces the old one
        setGraphContextCallback(testGraphCallback);
        try
        {
            throw MakeStringException(1, "First test");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx1 = ThorWrapException(e, "First error");

            StringBuffer msg1;
            thorEx1->errorMessage(msg1);

            CPPUNIT_ASSERT_EQUAL_STR("1, First test : First error, Graph TestGraph[42]", msg1.str());

            thorEx1->Release();
            e->Release();
        }

        // Test 6: Callback that provides empty graph name
        setGraphContextCallback(emptyGraphCallback);
        try
        {
            throw MakeStringException(456, "Empty name test");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = ThorWrapException(e, "Error with empty graph name");

            StringBuffer msg;
            thorEx->errorMessage(msg);

            // Should show "Graph [subgraphid]" when graph name is empty
            CPPUNIT_ASSERT_EQUAL_STR("456, Empty name test : Error with empty graph name, Graph [99]", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test to ensure that exception handling in activities calls _ThorWrapException
        setGraphContextCallback(testGraphCallback);

        // Test 7: Test with MakeThorFatal (which internally calls _ThorWrapException)
        try
        {
            throw MakeStringException(777, "Fatal test exception");
        }
        catch (IException *e)
        {
            Owned<IThorException> thorEx = MakeThorFatal(e, TE_MasterProcessError, "Fatal error during test: %s", "master");

            StringBuffer msg;
            thorEx->errorMessage(msg);

            // Should have wrapped message with graph context
            CPPUNIT_ASSERT_EQUAL_STR("777, Fatal test exception : Fatal error during test: master, Graph TestGraph[42]", msg.str());

            thorEx->Release();
            e->Release();
        }

        // Test 8: Test exception chaining behavior
        try
        {
            throw MakeStringException(666, "Original exception");
        }
        catch (IException *e)
        {
            try
            {
                Owned<IThorException> wrapped1 = ThorWrapException(e, "First wrap");
                e->Release();

                // Throw the wrapped exception to test chaining
                throw wrapped1.getClear();
            }
            catch (IException *e2)
            {
                Owned<IThorException> wrapped2 = ThorWrapException(e2, "Second wrap");

                StringBuffer msg;
                wrapped2->errorMessage(msg);

                // Should show nested wrapping with graph context
                // When wrapping an already-wrapped exception, both graph contexts are included
                CPPUNIT_ASSERT_EQUAL_STR("666, 666, Original exception : First wrap, Graph TestGraph[42] : Second wrap, Graph TestGraph[42]", msg.str());

                wrapped2->Release();
                e2->Release();
            }
        }

        // Clean up callback
        setGraphContextCallback(nullptr);
    }

    void testThorWrapExceptionConcurrentAccess()
    {
        // Test concurrent access to graphContextCallback in _ThorWrapException

        constexpr unsigned numThreads = 10;
        constexpr unsigned iterationsPerThread = 10;

        std::atomic<unsigned> readyThreads{0};
        std::atomic<bool> startSignal{false};
        std::atomic<unsigned> completedThreads{0};
        std::atomic<unsigned> successfulWraps{0};
        std::atomic<unsigned> callbackChanges{0};

        // Test callbacks
        auto callback1 = [](StringBuffer &graphName, graph_id &subGraphId)
        {
            graphName.append("ConcurrentGraph1");
            subGraphId = 1001;
        };

        auto callback2 = [](StringBuffer &graphName, graph_id &subGraphId)
        {
            graphName.append("ConcurrentGraph2");
            subGraphId = 2002;
        };

        auto callback3 = [](StringBuffer &graphName, graph_id &subGraphId)
        {
            graphName.append("ConcurrentGraph3");
            subGraphId = 3003;
        };

        // Array of callbacks to cycle through
        GraphContextCallback callbacks[] = {nullptr, callback1, callback2, callback3};
        constexpr unsigned numCallbacks = sizeof(callbacks) / sizeof(callbacks[0]);

        // Thread function that repeatedly calls ThorWrapException while callback changes
        auto workerThread = [&](unsigned threadId)
        {
            readyThreads++;

            // Wait for all threads to be ready
            while (!startSignal.load())
            {
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }

            for (unsigned i = 0; i < iterationsPerThread; ++i)
            {
                try
                {
                    // Create a test exception
                    throw MakeStringException(1000 + threadId, "Concurrent test exception from thread %u", threadId);
                }
                catch (IException *e)
                {
                    try
                    {
                        // This is the critical line being tested - it should safely load the atomic callback
                        Owned<IThorException> thorEx = ThorWrapException(e, "Concurrent access test iteration %u", i);

                        // Verify we got a valid exception
                        if (thorEx)
                        {
                            StringBuffer msg;
                            thorEx->errorMessage(msg);

                            // The message should be valid and contain our text
                            if (msg.length() > 0 &&
                                strstr(
                                    msg.str(),
                                    VStringBuffer("Concurrent test exception from thread %u : Concurrent access test iteration %u", threadId, i).str()))
                                successfulWraps++;
                        }

                        e->Release();
                    }
                    catch (...)
                    {
                        e->Release();
                        // If we get here, there was likely a race condition or crash
                        CPPUNIT_FAIL("Exception occurred during concurrent ThorWrapException call");
                    }
                }
            }

            completedThreads++;
        };

        // Thread function that changes the callback frequently
        auto callbackChangerThread = [&]()
        {
            readyThreads++;

            // Wait for all threads to be ready
            while (!startSignal.load())
                std::this_thread::sleep_for(std::chrono::microseconds(1));

            unsigned changeCount = 0;
            const unsigned maxChanges = numThreads * iterationsPerThread / 2;

            while (completedThreads.load() < numThreads && changeCount < maxChanges)
            {
                // Cycle through different callbacks including nullptr
                GraphContextCallback newCallback = callbacks[changeCount % numCallbacks];
                setGraphContextCallback(newCallback);
                callbackChanges++;
                changeCount++;
            }
        };

        // Start all threads
        std::vector<std::thread> threads;
        for (unsigned i = 0; i < numThreads; ++i)
            threads.emplace_back(workerThread, i);

        // Create callback changer thread
        threads.emplace_back(callbackChangerThread);

        // Wait for all threads to be ready
        while (readyThreads.load() < numThreads + 1)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        // Signal all threads to start
        startSignal = true;

        // Wait for all threads to complete
        for (auto &thread : threads)
            thread.join();

        // Verify results
        CPPUNIT_ASSERT_EQUAL(numThreads, completedThreads.load());
        CPPUNIT_ASSERT_EQUAL(numThreads * iterationsPerThread, successfulWraps.load());

        CPPUNIT_ASSERT(callbackChanges.load() > 0);

        CPPUNIT_ASSERT_MESSAGE("All threads completed successfully without race conditions",
                               completedThreads.load() == numThreads);

        CPPUNIT_ASSERT_MESSAGE("All ThorWrapException calls succeeded",
                               successfulWraps.load() == numThreads * iterationsPerThread);

        // Clean up
        setGraphContextCallback(nullptr);
    }
};

// Add the test suite macros outside the class definition
CPPUNIT_TEST_SUITE_REGISTRATION(ExceptionTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ExceptionTest, "ExceptionTest");

#endif // _USE_CPPUNIT
