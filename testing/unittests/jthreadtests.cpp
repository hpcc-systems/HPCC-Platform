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
#include <atomic>
#include <chrono>
#include <thread>

#include "jthread.hpp"
#include "unittests.hpp"

constexpr unsigned fiveSeconds = 5000;

class TestPooledThread : public CInterface, implements IPooledThread
{
public:
    IMPLEMENT_IINTERFACE;

    TestPooledThread(unsigned _id, std::atomic<unsigned> &_threadStartedCount, std::atomic<unsigned> &_threadCompletedCount)
        : id(_id), threadStartedCount(_threadStartedCount), threadCompletedCount(_threadCompletedCount)
    {
    }

    virtual void init(void *param) override
    {
        if (param)
            threadRuntime = *static_cast<unsigned *>(param);
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread %u started", id);

        std::this_thread::sleep_for(std::chrono::seconds(threadRuntime));

        DBGLOG("Thread %u completed", id);
        threadCompletedCount++;
    }

    virtual bool stop() override { return true; }

    virtual bool canReuse() const override { return true; }

private:
    unsigned id;
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    unsigned threadRuntime{60};
};

class TestThreadFactory : public CInterface, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;

    TestThreadFactory(std::atomic<unsigned> &_threadStartedCount, std::atomic<unsigned> &_threadCompletedCount)
        : threadStartedCount(_threadStartedCount), threadCompletedCount(_threadCompletedCount), nextId(0)
    {
    }

    virtual IPooledThread *createNew() override
    {
        return new TestPooledThread(++nextId, threadStartedCount, threadCompletedCount);
    }

private:
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    std::atomic<unsigned> nextId;
};

class ThreadPoolTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ThreadPoolTest);
    //      CPPUNIT_TEST(testTightlyBoundThreadPool);
    CPPUNIT_TEST(testTightlyBoundThreadPoolWithInfiniteStartDelay);
    //      CPPUNIT_TEST(testThrottledThreadPool);
    //      CPPUNIT_TEST(testThrottledThreadPoolWithFastThreadCompletion);
    //      CPPUNIT_TEST(testWaitAvailable);
    //      CPPUNIT_TEST(testThreadPoolResizing);
    CPPUNIT_TEST_SUITE_END();

private:
    std::atomic<unsigned> threadStartedCount;
    std::atomic<unsigned> threadCompletedCount;
    Owned<IThreadFactory> factory;
    Owned<IThreadPool> pool;
    unsigned lifespan1second = 1;
    unsigned lifespan5seconds = 5;
    unsigned lifespan10seconds = 10;

public:
    void setUp() override
    {
        // Reset counters
        threadStartedCount = 0;
        threadCompletedCount = 0;

        // Create thread factory
        factory.setown(new TestThreadFactory(threadStartedCount, threadCompletedCount));
    }

    void tearDown() override
    {
        if (pool)
            pool->joinAll(true);

        pool.clear();
        factory.clear();
    }

    void testTightlyBoundThreadPool()
    {
        // Create thread pool with max 2 threads, testing start with and without start delay
        pool.setown(createThreadPool("TestPool", factory, true, nullptr, 2));

        pool->start(&lifespan5seconds, "Thread1");
        pool->start(&lifespan5seconds, "Thread2");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());

        pool->start(&lifespan10seconds, "Thread3", fiveSeconds);
        pool->start(&lifespan10seconds, "Thread4", fiveSeconds);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(10));
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());

        pool->start(&lifespan10seconds, "Thread5", fiveSeconds);
        pool->start(&lifespan10seconds, "Thread6", fiveSeconds);
        try
        {
            pool->start(&lifespan10seconds, "Thread7", fiveSeconds);
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be available when start delay specified");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool TestPool", msg.str());
            e->Release();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(9U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(10));
        CPPUNIT_ASSERT_EQUAL(9U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
    }

    void testTightlyBoundThreadPoolWithInfiniteStartDelay()
    {
        // Create thread pool with max 2 threads, testing start with and without start delay
        pool.setown(createThreadPool("TestPool", factory, true, nullptr, 2));

        pool->start(&lifespan5seconds, "Thread1", INFINITE);
        pool->start(&lifespan5seconds, "Thread2", INFINITE);
        pool->start(&lifespan10seconds, "Thread3", INFINITE);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(1U, pool->runningCount());
    }

    void testThrottledThreadPool()
    {
        // Test: Expected throttling delays are introduced when pool is at capacity
        pool.setown(createThreadPool("ThrottledTestPool", factory, true, nullptr, 3, 500));

        auto startTime = std::chrono::high_resolution_clock::now();

        unsigned lifespan2seconds = 2;
        pool->start(&lifespan2seconds, "Thread1");
        pool->start(&lifespan2seconds, "Thread2");
        pool->start(&lifespan2seconds, "Thread3");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        auto afterQuickStarts = std::chrono::high_resolution_clock::now();
        auto quickStartDuration = std::chrono::duration_cast<std::chrono::milliseconds>(afterQuickStarts - startTime);

        // These should start immediately without throttling
        CPPUNIT_ASSERT(quickStartDuration.count() < 100);
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // When pool is at capacity, startNoBlock should throw exception immediately
        try
        {
            pool->startNoBlock(&lifespan2seconds);
            CPPUNIT_FAIL("startNoBlock should have thrown an exception when pool is at capacity");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledTestPool", msg.str());
            e->Release();
        }

        auto throttleStartTime = std::chrono::high_resolution_clock::now();

        // This should block for approximately 100ms (start delay) then throw exception
        try
        {
            pool->start(&lifespan2seconds, "Thread4", 100); // timeout after 100ms
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be available when start delay specified");
        }
        catch (IException *e)
        {
            auto throttleEndTime = std::chrono::high_resolution_clock::now();
            auto throttleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(throttleEndTime - throttleStartTime);

            CPPUNIT_ASSERT(throttleDuration.count() >= 80 && throttleDuration.count() <= 150);

            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledTestPool", msg.str());
            e->Release();
        }

        auto longThrottleStartTime = std::chrono::high_resolution_clock::now();

        // This should block for the full 500ms delay then not throw exception
        try
        {
            pool->start(&lifespan2seconds, "Thread4"); // Will use default delay of 500ms
        }
        catch (IException *e)
        {
            StringBuffer msg("Exception encountered: ");
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        auto longThrottleEndTime = std::chrono::high_resolution_clock::now();
        auto longThrottleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(longThrottleEndTime - longThrottleStartTime);

        CPPUNIT_ASSERT(longThrottleDuration.count() >= 450 && longThrottleDuration.count() <= 600);

        // Wait for threads to complete
        std::this_thread::sleep_for(std::chrono::seconds(3));
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());

        // Test: No throttle delay is introduced after pool falls below defaultmax
        auto afterDropStartTime = std::chrono::high_resolution_clock::now();

        pool->start(&lifespan2seconds, "Thread5");
        pool->start(&lifespan2seconds, "Thread6");

        auto afterDropEndTime = std::chrono::high_resolution_clock::now();
        auto afterDropDuration = std::chrono::duration_cast<std::chrono::milliseconds>(afterDropEndTime - afterDropStartTime);

        // Should start immediately without throttling
        CPPUNIT_ASSERT(afterDropDuration.count() < 100);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test: Pool was above, fell below, and is now at defaultmax again
        pool->start(&lifespan2seconds, "Thread7"); // Now at capacity (3 threads)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        auto reThrottleStartTime = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&lifespan2seconds, "Thread8", 200); // timeout after 200ms
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be available when start delay specified");
        }
        catch (IException *e)
        {
            auto reThrottleEndTime = std::chrono::high_resolution_clock::now();
            auto reThrottleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(reThrottleEndTime - reThrottleStartTime);

            CPPUNIT_ASSERT(reThrottleDuration.count() >= 150 && reThrottleDuration.count() <= 300);
            e->Release();
        }

        // Test: waitAvailable functionality
        CPPUNIT_ASSERT(!pool->waitAvailable(100)); // Should timeout since pool is full

        // Wait for threads to finish
        std::this_thread::sleep_for(std::chrono::seconds(3));
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());

        // Now waitAvailable should succeed immediately
        auto waitStartTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        auto waitEndTime = std::chrono::high_resolution_clock::now();
        auto waitDuration = std::chrono::duration_cast<std::chrono::milliseconds>(waitEndTime - waitStartTime);

        CPPUNIT_ASSERT(waitDuration.count() < 50);
    }

    void testThrottledThreadPoolWithFastThreadCompletion()
    {
        // Test the behavior of a throttled thread pool under high load with fast-completing threads
        pool.setown(createThreadPool("ThrottledTestPoolWithFastThreadCompletion", factory, true, nullptr, 100, 1));

        for (int i = 1; i <= 10; i++)
        {
            threadStartedCount = 0;
            threadCompletedCount = 0;

            for (int i = 1; i <= 200; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%d", i);
                pool->start(&lifespan1second, threadName.str());
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));

            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
            CPPUNIT_ASSERT_EQUAL(200U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(200U, threadCompletedCount.load());

            unsigned exceptionCount = 0;
            std::atomic<long long> totalStartDuration{0};

            for (int i = 201; i <= 400; i++)
            {
                std::thread asyncStart([&i, this, &exceptionCount, &totalStartDuration]()
                                       {
                                                StringBuffer threadName;
                                                threadName.appendf("Thread%d", i);

                                                auto beforeDelayedStart = std::chrono::high_resolution_clock::now();
                                                try
                                                {
                                                    // Attempt to start thread with 100ms timeout
                                                    // This should succeed for ~100 threads and timeout for ~100 threads
                                                    this->pool->start(&lifespan1second, threadName.str(), 100);
                                                }
                                                catch(IException *E)
                                                {
                                                    // Expected behavior: some requests should timeout when pool is full
                                                    StringBuffer msg;
                                                    E->errorMessage(msg);
                                                    CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledTestPoolWithFastThreadCompletion", msg.str());
                                                    ++exceptionCount;
                                                    E->Release();
                                                }

                                                // Record total time spent waiting (includes throttle delays and timeouts)
                                                auto afterDelayedStart = std::chrono::high_resolution_clock::now();
                                                auto startDuration = std::chrono::duration_cast<std::chrono::milliseconds>(afterDelayedStart - beforeDelayedStart);
                                                totalStartDuration += startDuration.count(); });

                asyncStart.detach();
            }

            // Wait 5 seconds for all async operations to complete
            std::this_thread::sleep_for(std::chrono::seconds(5));

            CPPUNIT_ASSERT(totalStartDuration.load() >= 10000LL && totalStartDuration.load() <= 10001LL);

            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());          // Pool drained
            CPPUNIT_ASSERT_EQUAL(300U, threadStartedCount.load());   // 200 from phase 1 + ~100 from phase 2
            CPPUNIT_ASSERT_EQUAL(300U, threadCompletedCount.load()); // All started threads completed

            CPPUNIT_ASSERT(exceptionCount >= 99U && exceptionCount <= 101U);
        }
    }

    void testWaitAvailable()
    {
        // Test: Pool with unlimited capacity should always return true
        pool.setown(createThreadPool("UnlimitedTestPool", factory, true, nullptr, 0)); // defaultmax=0 means unlimited

        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(0));

        pool.clear();

        // Test: Pool with limited capacity
        pool.setown(createThreadPool("LimitedTestPool", factory, true, nullptr, 2)); // max 2 threads

        // Should return true when pool is empty
        CPPUNIT_ASSERT(pool->waitAvailable(100));

        pool->start(&lifespan5seconds, "Thread1");
        pool->start(&lifespan5seconds, "Thread2");

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test: waitAvailable should return false when pool is full and timeout expires
        auto startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(200));
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        CPPUNIT_ASSERT(duration.count() >= 180 && duration.count() <= 250);

        // Test: waitAvailable should return true immediately when slot becomes available
        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());

        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        endTime = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        CPPUNIT_ASSERT(duration.count() < 50);

        // Test: Zero timeout should return immediately
        pool->start(&lifespan5seconds, "Thread3");
        pool->start(&lifespan5seconds, "Thread4");

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(0));
        endTime = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        CPPUNIT_ASSERT(duration.count() < 20);
    }

    /* Dependent upon HPCC-34238
        void testThreadPoolResizing()
        {
            pool.setown(createThreadPool("TestPool", factory, true, nullptr));

            // Start 10 threads with 10 second lifespan
            for (int i = 1; i <= 10; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%d", i);
                pool->start(&lifespan10seconds, threadName.str());
            }
            // Start 10 threads with 5 second lifespan
            for (int i = 11; i <= 20; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%d", i);
                pool->start(&lifespan5seconds, threadName.str());
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // All 20 threads should be running
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(20U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());

            CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

            // Resize the pool to 10 threads
            pool->setPoolSize(10);

            // All 20 threads should be running
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(20U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());

            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Resize the pool to 30 threads
            pool->setPoolSize(30);

            // All 20 threads should be running
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(20U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());

            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Resize the pool to 20 threads
            pool->setPoolSize(20);

            // All 20 threads should be running
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(20U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());

            std::this_thread::sleep_for(std::chrono::seconds(3));

            // 10 threads should be running and 10 completed
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(10U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());
            CPPUNIT_ASSERT_EQUAL(10U, threadCompletedCount.load());

            // Resize the pool to 5 threads
            pool->setPoolSize(5);

            std::this_thread::sleep_for(std::chrono::seconds(1));

            // 10 threads should be running and 10 completed
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(10U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());
            CPPUNIT_ASSERT_EQUAL(10U, threadCompletedCount.load());

            std::this_thread::sleep_for(std::chrono::seconds(5));

            // All 20 threads should be completed
            CPPUNIT_ASSERT_EQUAL(20U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());
            CPPUNIT_ASSERT_EQUAL(20U, threadCompletedCount.load());

            // There should be 5 slots available before an exception is thrown
            // Start 10 threads with 1 second lifespan
            unsigned exceptionCount = 0;
            for (int i = 21; i <= 30; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%d", i);
                try
                {
                    pool->start(&lifespan1second, threadName.str(), 50); // Will use slot timeout of 50ms
                }
                catch (IException *e)
                {
                    StringBuffer msg("Exception encountered: ");
                    e->errorMessage(msg);
                    CPPUNIT_ASSERT_EQUAL_STR("Exception encountered: No threads available in pool TestPool", msg.str());
                    ++exceptionCount;
                    e->Release();
                }
            }

            // All 20 threads should be completed, 5 still running and 5 generated exceptions
            CPPUNIT_ASSERT_EQUAL(25U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(5U, pool->runningCount());
            CPPUNIT_ASSERT(pool->running());
            CPPUNIT_ASSERT_EQUAL(20U, threadCompletedCount.load());
            CPPUNIT_ASSERT_EQUAL(5U, exceptionCount);
        }
    */
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolTest, "ThreadPoolTest");

#endif // _USE_CPPUNIT
