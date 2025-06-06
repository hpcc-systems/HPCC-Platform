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

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x ? x : ""), std::string(y ? y : ""))

constexpr unsigned SMALL_THREAD_POOL_SIZE = 2;
constexpr unsigned FIVE_SECOND_START_DELAY = 5000;

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
        // Store the delay parameter if provided
        if (param)
            delaySeconds = *static_cast<unsigned *>(param);
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread %u started", id);

        // Wait 60 seconds
        std::this_thread::sleep_for(std::chrono::seconds(delaySeconds));

        DBGLOG("Thread %u completed", id);
        threadCompletedCount++;
    }

    virtual bool stop() override
    {
        return true;
    }

    virtual bool canReuse() const override
    {
        return true;
    }

private:
    unsigned id;
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    unsigned delaySeconds{60};
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
    CPPUNIT_TEST(testTightlyBoundThreadPool);
    CPPUNIT_TEST(testThrottledThreadPool);
    CPPUNIT_TEST(testThrottledThreadPoolWithFastThreadCompletion);
    CPPUNIT_TEST(testThreadPoolStopAndJoinFunctions);
    CPPUNIT_TEST_SUITE_END();

private:
    std::atomic<unsigned> threadStartedCount;
    std::atomic<unsigned> threadCompletedCount;
    Owned<IThreadFactory> factory;
    Owned<IThreadPool> pool;

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
        // Clean up - wait for all threads to complete
        if (pool)
            pool->joinAll(true);

        pool.clear();
        factory.clear();
    }

    void testTightlyBoundThreadPool()
    {
        // Create thread pool with max 2 threads and default start delay 1 second
        pool.setown(createThreadPool("TestPool", factory, true, nullptr, 2));

        // Start two 5 second lifespan threads with start delay from thread pool of 1 second
        unsigned lifespan5seconds = 5;
        pool->start(&lifespan5seconds, "Thread1");
        pool->start(&lifespan5seconds, "Thread2");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Start two 10 second lifespan threads with start delay from thread pool of 5 seconds
        unsigned lifespan10seconds = 10;
        pool->start(&lifespan10seconds, "Thread1", FIVE_SECOND_START_DELAY);
        pool->start(&lifespan10seconds, "Thread2", FIVE_SECOND_START_DELAY);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(10));
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());

        // Start three 5 second lifespan threads with start delay from thread pool of 1 second
        pool->start(&lifespan5seconds, "Thread1");
        pool->start(&lifespan5seconds, "Thread2");
        pool->start(&lifespan5seconds, "Thread3");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(7U, threadStartedCount.load());

        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(7U, threadCompletedCount.load());

        // Start three 10 second lifespan threads with start delay from thread pool of 5 seconds
        pool->start(&lifespan10seconds, "Thread1", FIVE_SECOND_START_DELAY);
        pool->start(&lifespan10seconds, "Thread2", FIVE_SECOND_START_DELAY);
        try
        {
            pool->start(&lifespan10seconds, "Thread3", FIVE_SECOND_START_DELAY);
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be avaiable when start delay specified");
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
    }

    void testThrottledThreadPool()
    {
        // Test 1: Test that expected throttling delays are introduced when pool is at capacity
        // Create a throttled thread pool: max 3 threads, delay 500ms
        pool.setown(createThreadPool("ThrottledTestPool", factory, true, nullptr, 3, 500));

        // Test 2: Test that no throttle delay is introduced if less than defaultmax
        auto startTime = std::chrono::high_resolution_clock::now();

        // Start 3 threads that run for 2 seconds - this should not cause any delay
        unsigned lifespan2seconds = 2;
        pool->start(&lifespan2seconds, "Thread1");
        pool->start(&lifespan2seconds, "Thread2");
        pool->start(&lifespan2seconds, "Thread3");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        auto afterQuickStarts = std::chrono::high_resolution_clock::now();
        auto quickStartDuration = std::chrono::duration_cast<std::chrono::milliseconds>(afterQuickStarts - startTime);

        // These should start immediately without throttling
        CPPUNIT_ASSERT(quickStartDuration.count() < 100); // Should be very fast, well under 100ms
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // Test 3: Test _start function noBlock behaviour
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

        // Test 4: Test throttling delay when pool is at capacity
        auto throttleStartTime = std::chrono::high_resolution_clock::now();

        // This should block for approximately 100ms (start delay) then throw exception
        try
        {
            pool->start(&lifespan2seconds, "Thread4", 100); // timeout after 100ms
            CPPUNIT_FAIL("start with timeout should have thrown an exception");
        }
        catch (IException *e)
        {
            auto throttleEndTime = std::chrono::high_resolution_clock::now();
            auto throttleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(throttleEndTime - throttleStartTime);

            // Should have waited close to 100ms before timing out
            CPPUNIT_ASSERT(throttleDuration.count() >= 80 && throttleDuration.count() <= 150);

            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledTestPool", msg.str());
            e->Release();
        }

        // Test 5: Test that throttle delay is introduced when defaultmax threads are running
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

        // Should have waited close to 500ms before timing out
        CPPUNIT_ASSERT(longThrottleDuration.count() >= 450 && longThrottleDuration.count() <= 600);

        // Wait for the initial threads to complete
        std::this_thread::sleep_for(std::chrono::seconds(3));
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());

        // Test 6: Test that no throttle delay is introduced after pool falls below defaultmax
        auto afterDropStartTime = std::chrono::high_resolution_clock::now();

        // Start 2 threads - should be fast since we're under the limit
        pool->start(&lifespan2seconds, "Thread5");
        pool->start(&lifespan2seconds, "Thread6");

        auto afterDropEndTime = std::chrono::high_resolution_clock::now();
        auto afterDropDuration = std::chrono::duration_cast<std::chrono::milliseconds>(afterDropEndTime - afterDropStartTime);

        // Should start immediately without throttling
        CPPUNIT_ASSERT(afterDropDuration.count() < 100);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test 7: Test scenario when pool was above, fell below, and is at defaultmax again
        pool->start(&lifespan2seconds, "Thread7"); // Now at capacity (3 threads)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // This should again trigger throttling
        auto reThrottleStartTime = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&lifespan2seconds, "Thread8", 200); // timeout after 200ms
            CPPUNIT_FAIL("start with timeout should have thrown an exception");
        }
        catch (IException *e)
        {
            auto reThrottleEndTime = std::chrono::high_resolution_clock::now();
            auto reThrottleDuration = std::chrono::duration_cast<std::chrono::milliseconds>(reThrottleEndTime - reThrottleStartTime);

            // Should have waited close to 200ms before timing out
            CPPUNIT_ASSERT(reThrottleDuration.count() >= 150 && reThrottleDuration.count() <= 300);
            e->Release();
        }

        // Test 8: Test waitAvailable functionality
        CPPUNIT_ASSERT(!pool->waitAvailable(100)); // Should timeout since pool is full

        // Wait for threads to finish
        std::this_thread::sleep_for(std::chrono::seconds(3));
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());

        // Now waitAvailable should succeed immediately
        auto waitStartTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        auto waitEndTime = std::chrono::high_resolution_clock::now();
        auto waitDuration = std::chrono::duration_cast<std::chrono::milliseconds>(waitEndTime - waitStartTime);

        // Should return immediately
        CPPUNIT_ASSERT(waitDuration.count() < 50);
    }

    void testThrottledThreadPoolWithFastThreadCompletion()
    {
        // Test the behavior of a throttled thread pool under high load with fast-completing threads
        // This test verifies that the pool correctly handles rapid thread creation/completion cycles
        // and properly throttles requests when at capacity

        // Create a throttled thread pool with:
        // - Max 100 threads (high capacity to handle load)
        // - 1ms throttle delay (very short delay for fast cycling)
        // Note: Comment above mentions "max 3 threads, delay 500ms" but actual params are different
        pool.setown(createThreadPool("ThrottledTestPoolWithFastThreadCompletion", factory, true, nullptr, 100, 1));

        // Configure threads to run for 1 second each
        unsigned lifespan1seconds = 1;

        // Repeat the test scenario 10 times to ensure consistent behavior
        // This helps catch race conditions and intermittent issues
        for (int i = 1; i <= 10; i++)
        {
            // Reset counters for each iteration to track thread lifecycle accurately
            threadStartedCount = 0;
            threadCompletedCount = 0;

            // PHASE 1: Start first batch of 200 threads synchronously
            // Since pool max is 100, this tests the pool's queuing mechanism
            // Some threads will start immediately, others will be queued
            for (int i = 1; i <= 200; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%d", i);
                pool->start(&lifespan1seconds, threadName.str());
            }

            // Wait 2 seconds (longer than thread lifespan) to ensure all threads complete
            // This allows the pool to drain completely before the next phase
            std::this_thread::sleep_for(std::chrono::seconds(2));

            // Verify all 200 threads were started and completed successfully
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());          // Pool should be empty
            CPPUNIT_ASSERT_EQUAL(200U, threadStartedCount.load());   // All threads started
            CPPUNIT_ASSERT_EQUAL(200U, threadCompletedCount.load()); // All threads completed

            // PHASE 2: Start second batch of 200 threads asynchronously with timeout
            // This simulates concurrent requests hitting the pool simultaneously
            unsigned exceptionCount = 0;
            std::atomic<long long> totalStartDuration{0};

            for (int i = 201; i <= 400; i++)
            {
                // Launch each thread start attempt in a separate async thread
                // This creates high concurrency and tests thread pool under stress
                std::thread asyncStart([&i, this, &lifespan1seconds, &exceptionCount, &totalStartDuration]()
                                       {
                                                StringBuffer threadName;
                                                threadName.appendf("Thread%d", i);

                                                // Measure how long it takes to start each thread
                                                auto beforeDelayedStart = std::chrono::high_resolution_clock::now();
                                                try
                                                {
                                                    // Attempt to start thread with 100ms timeout
                                                    // This should succeed for ~100 threads and timeout for ~100 threads
                                                    this->pool->start(&lifespan1seconds, threadName.str(), 100);
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

                // Detach threads so they run independently and concurrently
                asyncStart.detach();
            }

            // Wait 5 seconds for all async operations to complete
            // This ensures all threads have either started+completed or timed out
            std::this_thread::sleep_for(std::chrono::seconds(5));

            // Verify timing behavior: expect ~100 requests to wait 100ms each = ~10000ms total
            // The range 10000-10001 accounts for minor timing variations
            CPPUNIT_ASSERT(totalStartDuration.load() >= 10000LL && totalStartDuration.load() <= 10001LL);

            // Verify final state: pool should be empty, 300 total threads processed
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());          // Pool drained
            CPPUNIT_ASSERT_EQUAL(300U, threadStartedCount.load());   // 200 from phase 1 + ~100 from phase 2
            CPPUNIT_ASSERT_EQUAL(300U, threadCompletedCount.load()); // All started threads completed

            // Verify approximately 100 requests timed out (range accounts for timing variations)
            CPPUNIT_ASSERT(exceptionCount >= 99U && exceptionCount <= 101U);
        }
    }

    void testThreadPoolStopAndJoinFunctions()
    {
        // Create a default thread pool
        pool.setown(createThreadPool("TestPool", factory, true, nullptr));

        // Start 50 threads with 5 second lifespan
        unsigned lifespan5seconds = 5;
        for (int i = 1; i <= 49; i++)
        {
            StringBuffer threadName;
            threadName.appendf("Thread%d", i);
            pool->start(&lifespan5seconds, threadName.str());
        }
        PooledThreadHandle thread50 = pool->start(&lifespan5seconds, "Thread50");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // All 50 threads should be running
        CPPUNIT_ASSERT_EQUAL(50U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(50U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());

        // No threads should have completed yet
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

        // Test stop and join functions
        CPPUNIT_ASSERT(pool->stop(thread50));
        CPPUNIT_ASSERT(pool->join(thread50));
        CPPUNIT_ASSERT(pool->joinAll(false, 5));
        CPPUNIT_ASSERT(pool->stopAll(true));
        CPPUNIT_ASSERT(pool->joinAll(true));

        // All threads should have completed
        CPPUNIT_ASSERT_EQUAL(50U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(50U, threadStartedCount.load());

        // Pool should be empty
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());

        // Wait for threads to finish
        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(50U, threadCompletedCount.load());

        // Start 50 threads with 5 second lifespan
        for (int i = 51; i <= 100; i++)
        {
            StringBuffer threadName;
            threadName.appendf("Thread%d", i);
            pool->start(&lifespan5seconds, threadName.str());
        }
        // Test waitAvailable functionality
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT(pool->waitAvailable(1000));

        // Wait for threads to finish
        std::this_thread::sleep_for(std::chrono::seconds(5));
        CPPUNIT_ASSERT_EQUAL(100U, threadCompletedCount.load());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolTest, "ThreadPoolTest");

#endif // _USE_CPPUNIT
