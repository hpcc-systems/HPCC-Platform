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
#include <string>

#include "jthread.hpp"
#include "jsem.hpp"
#include "unittests.hpp"

constexpr unsigned fiveSeconds{5000};
constexpr unsigned twoSeconds{2000};
constexpr unsigned lifespan1second{1};
constexpr unsigned lifespan2seconds{2};
constexpr unsigned lifespan3seconds{3};
constexpr unsigned lifespan5seconds{5};
constexpr unsigned lifespan10seconds{10};

// Struct to pass both runtime and name
struct ThreadParams {
    unsigned runtime{0};
    const char* name{nullptr};
};

class TestPooledThread : public CSimpleInterfaceOf<IPooledThread>
{
public:
    TestPooledThread(std::atomic<unsigned> &_threadStartedCount, std::atomic<unsigned> &_threadCompletedCount, Semaphore &_startSemaphore)
        : threadStartedCount(_threadStartedCount), threadCompletedCount(_threadCompletedCount), startSemaphore(_startSemaphore)
    {
    }

    virtual void init(void *param) override
    {
        if (param)
        {
            ThreadParams* p = static_cast<ThreadParams*>(param);
            threadRuntime = p->runtime;
            if (p->name)
                threadName = p->name;
            else
                threadName.clear();
        }
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread '%s' started", threadName.c_str());

        // Signal after the count is incremented to ensure proper ordering
        startSemaphore.signal();

        std::this_thread::sleep_for(std::chrono::seconds(threadRuntime));

        DBGLOG("Thread '%s' completed", threadName.c_str());
        threadCompletedCount++;
    }

    virtual bool stop() override { return true; }
    virtual bool canReuse() const override { return true; }

private:
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    Semaphore &startSemaphore;
    unsigned threadRuntime{60};
    std::string threadName;
};

class TestThreadFactory : public CSimpleInterfaceOf<IThreadFactory>
{
public:
    TestThreadFactory(std::atomic<unsigned> &_threadStartedCount, std::atomic<unsigned> &_threadCompletedCount, Semaphore &_startSemaphore)
        : threadStartedCount(_threadStartedCount), threadCompletedCount(_threadCompletedCount), startSemaphore(_startSemaphore)
    {
    }

    virtual IPooledThread *createNew() override
    {
        return new TestPooledThread(threadStartedCount, threadCompletedCount, startSemaphore);
    }

private:
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    Semaphore &startSemaphore;
};

class ThreadPoolTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ThreadPoolTest);
//      CPPUNIT_TEST(testTightlyBoundThreadPool);
//      CPPUNIT_TEST(testThrottledThreadPoolWithDefaultDelay);
//      CPPUNIT_TEST(testThrottledThreadPoolAndThreadsWithInfiniteStartDelay);
//      CPPUNIT_TEST(testThrottledThreadPoolWithSpecifiedDelay);
      CPPUNIT_TEST(testThrottledThreadPoolWithFastThreadCompletion);
      CPPUNIT_TEST(testWaitAvailable);
//      CPPUNIT_TEST(testThreadPoolResizing); // Code dependent upon HPCC-34238
    CPPUNIT_TEST_SUITE_END();

private:
    std::atomic<unsigned> threadStartedCount{0};
    std::atomic<unsigned> threadCompletedCount{0};
    Owned<IThreadFactory> factory;
    Owned<IThreadPool> pool;
    Semaphore startSemaphore;
    ThreadParams params;

public:
    virtual void setUp() override
    {
        // Reset counters
        threadStartedCount = 0;
        threadCompletedCount = 0;

        // Reset semaphore state
        resetSemaphore();

        // Create thread factory with semaphore for synchronization
        factory.setown(new TestThreadFactory(threadStartedCount, threadCompletedCount, startSemaphore));
    }

    virtual void tearDown() override
    {
        if (pool)
            pool->joinAll(true);

        pool.clear();
        factory.clear();
    }

    void testTightlyBoundThreadPool()
    {
        // Create thread pool with max 2 threads, testing start with and without start delay
        pool.setown(createThreadPool("TightlyBoundTestPool", factory, true, nullptr, 2, INFINITE)); // Thread pool has an infinite start delay

        // Two new threads should start immediately
        params = {lifespan2seconds, "Thread1"};
        auto beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        params = {lifespan2seconds, "Thread2"};
        pool->start(&params, params.name);
        // Wait for both threads to signal that they've started
        auto duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Test Thread Pool start delay
        //
        // Thread3 should not start until one of the two running threads completes
        params = {lifespan2seconds, "Thread3"};
        beforeStart = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&params, params.name);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() >= 1900);
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);

        // Start Thread4 which should start immediately as only
        // Thread3 running now
        params = {lifespan2seconds, "Thread4"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Thread5 should not start as 2 threads are running and it's
        // 100 millisecond start timer should timeout
        params = {lifespan1second, "Thread5"};
        beforeStart = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&params, params.name, 100); // Wait for 100ms before timing out
            CPPUNIT_FAIL("start should have timed out and thrown an exception when pool is at capacity");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool TightlyBoundTestPool", msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 0);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Test Thread start delay
        //
        // Thread6 should start as one of the two threads will complete within it's
        // 2500 millisecond start timer
        params = {lifespan2seconds, "Thread6"};
        beforeStart = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&params, params.name, 2500); // Wait for 2500ms before timing out
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() < 2500);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 3);

        // Test Thread startNoBlock
        //
        params = {lifespan2seconds, "Thread7"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(6U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());
        // Pool is at capacity, startNoBlock should throw exception immediately
        params = {lifespan2seconds, "Thread8"};
        beforeStart = std::chrono::high_resolution_clock::now();
        try
        {
            pool->startNoBlock(&params);
            CPPUNIT_FAIL("startNoBlock should have thrown an exception when pool is at capacity");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool TightlyBoundTestPool", msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 0);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(6U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());
        // Wait for the running count to be less than 2
        while (pool->runningCount() >= 2)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // Thread8 should start immediately as the pool has capacity
        params = {lifespan2seconds, "Thread9"};
        beforeStart = std::chrono::high_resolution_clock::now();
        try
        {
            pool->startNoBlock(&params);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 0);
        CPPUNIT_ASSERT(duration.count() < 1000);

        // Test Thread Pool joinAll
        //
        // All threads should complete including Thread8
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(7U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(7U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testThrottledThreadPoolWithDefaultDelay()
    {
        // Create thread pool with max 2 threads, testing thread start with and without start delay
        pool.setown(createThreadPool("ThrottledThreadPoolWithDefaultDelay", factory, true, nullptr, 2)); // Thread pool has a default start delay of 1 second

        // Both threads should start immediately
        params = {lifespan5seconds, "Thread1"};
        auto beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread2"};
        pool->start(&params, params.name);

        // Wait for both threads to signal that they've started
        auto duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // The Thread3 should not start after waiting on it's start specified timeout as the other
        // two threads are still running when the timeout expires
        params = {lifespan5seconds, "Thread3"};
        try
        {
            pool->start(&params, params.name, twoSeconds);
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be available within thread start delay specified");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledThreadPoolWithDefaultDelay", msg.str());
        }
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // The Thread4 should start after one of the above completes
        beforeStart = std::chrono::high_resolution_clock::now();
        params = {lifespan10seconds, "Thread4"};
        try
        {
            pool->start(&params, params.name, fiveSeconds);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() >= 2990);
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);
        CPPUNIT_ASSERT(pool->runningCount() >= 1);

        // Test Thread Pool default start delay
        //
        beforeStart = std::chrono::high_resolution_clock::now();
        params = {lifespan10seconds, "Thread5"};
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() < 100);
        CPPUNIT_ASSERT(pool->runningCount() >= 2);

        // Thread6 should start after the Thread Pool default start delay of 1 second
        params = {lifespan10seconds, "Thread6"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name); // no exception should be thrown as no timeout is specified on the start
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(duration.count() >= 1000);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT(pool->runningCount() >= 3);

        // All threads should complete
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(5U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testThrottledThreadPoolAndThreadsWithInfiniteStartDelay()
    {
        // Create thread pool with max 2 threads and default delay of 1 second,
        // testing infinite thread start delay
        pool.setown(createThreadPool("ThrottledThreadPoolAndThreadsWithInfiniteStartDelay", factory, true, nullptr, 2));

        // The two threads should start immediately
        params = {lifespan5seconds, "Thread1"};
        auto beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name, INFINITE);
        params = {lifespan5seconds, "Thread2"};
        pool->start(&params, params.name, INFINITE);
        auto duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(duration.count() < 1000);
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // The third thread should start after one of the above completes
        std::thread asyncStart([this, &beforeStart]()
                               {
                                   ThreadParams asyncParams = {lifespan5seconds, "Thread3"};
                                   beforeStart = std::chrono::high_resolution_clock::now();
                                   this->pool->start(&asyncParams, asyncParams.name, INFINITE);
                               });
        asyncStart.detach();
        duration = measureThreadStartDuration(beforeStart, 1); // Wait for Thread3 to start
        CPPUNIT_ASSERT(duration.count() > 4990);
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);
        CPPUNIT_ASSERT(pool->runningCount() >= 1);

        // Test multiple threads with INFINITE timeout waiting
        std::thread asyncStart4([this]()
                                {
                                    ThreadParams asyncParams = {lifespan2seconds, "Thread4"};
                                    this->pool->start(&asyncParams, asyncParams.name, INFINITE);
                                });

        std::thread asyncStart5([this]()
                                {
                                    ThreadParams asyncParams = {lifespan2seconds, "Thread5"};
                                    this->pool->start(&asyncParams, asyncParams.name, INFINITE);
                                });

        asyncStart4.detach();
        asyncStart5.detach();

        // Wait for Thread4 and Thread5 to start (they should wait for slots to become available)
        waitForThreadsToStart(2);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 3);

        // All threads should complete including Thread3
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(5U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testThrottledThreadPoolWithSpecifiedDelay()
    {
        // Test: Expected throttling delays are introduced when pool is at capacity
        pool.setown(createThreadPool("ThrottledThreadPool", factory, true, nullptr, 3, 500));

        // Start 3 threads to fill the pool
        auto startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan3seconds, "Thread1"};
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread2"};
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread3"};
        pool->start(&params, params.name);

        // The 3 threads should start immediately without throttling
        auto duration = measureThreadStartDuration(startTime, 3);
        CPPUNIT_ASSERT(duration.count() < 100);
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // When pool is at capacity, startNoBlock should throw exception immediately
        params = {lifespan2seconds, "Thread4"};
        try
        {
            pool->startNoBlock(&params);
            CPPUNIT_FAIL("startNoBlock should have thrown an exception when pool is at capacity");
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledThreadPool", msg.str());
        }

        // This should block for a start delay of 100ms then throw exception
        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan2seconds, "Thread5"};
        try
        {
            pool->start(&params, params.name, 100); // timeout after 100ms
            CPPUNIT_FAIL("Start should have thrown an exception as no slots would be available within thread start delay specified");
        }
        catch (IException *e)
        {
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
            CPPUNIT_ASSERT(duration.count() >= 100);

            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool ThrottledThreadPool", msg.str());
        }

        // This should block for the full 500ms delay then not throw an exception as no timeout is specified on the thread start
        params = {lifespan1second, "Thread6"};
        startTime = std::chrono::high_resolution_clock::now();
        try
        {
            pool->start(&params, params.name); // Will use thread pool default delay of 500ms
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }

        duration = measureThreadStartDuration(startTime, 1);
        CPPUNIT_ASSERT(duration.count() >= 500);
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, pool->runningCount());

        // Test: No throttle delay is introduced after pool falls below defaultmax
        while(pool->runningCount() >= 3)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan2seconds, "Thread7"};
        pool->start(&params, params.name);

        // Should start immediately without throttling
        duration = measureThreadStartDuration(startTime, 1);
        CPPUNIT_ASSERT(duration.count() < 100);

        // Test: Pool was above, fell below, and is now at defaultmax again
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // All threads should complete
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(5U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testThrottledThreadPoolWithFastThreadCompletion()
    {
        // Test the behavior of a throttled thread pool under high load with fast-completing threads
        // Create a special factory without semaphore synchronization for this timing-sensitive test
        pool.setown(createThreadPool("TestPoolWithFastThreadCompletion", factory, true, nullptr, 100, 1));

        for (unsigned numIterations = 1; numIterations <= 10; numIterations++)
        {
            threadStartedCount = 0;
            threadCompletedCount = 0;

            unsigned threadId{0};
            auto startTime = std::chrono::high_resolution_clock::now();
            for (unsigned threadId = 1; threadId <= 200; threadId++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%u", threadId);
                params = {lifespan1second, threadName.str()};
                pool->start(&params, params.name);
            }

            auto duration = measureThreadStartDuration(startTime, 200);
            CPPUNIT_ASSERT(duration.count() < 200);
            CPPUNIT_ASSERT_EQUAL(200U, pool->runningCount());
            CPPUNIT_ASSERT_EQUAL(200U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

            // Wait for 100 threads to complete
            while(threadCompletedCount.load() < 100)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Test: Pool is at capacity, should throttle new thread starts
            //
            unsigned exceptionCount{0};
            std::atomic<long long> totalStartDuration{0};
            // 100 threads are still running so 100 will start with no delay
            // whilst 100 threads will wait up to 100ms before timing out
            for (threadId = 201; threadId <= 400; threadId++)
            {
                auto startThreadFunc = [threadId, this, &exceptionCount, &totalStartDuration]()
                {
                    StringBuffer threadName;
                    threadName.appendf("Thread%u", threadId);

                    auto beforeDelayedStart = std::chrono::high_resolution_clock::now();
                    try
                    {
                        // Attempt to start thread with 100ms timeout
                        // This should succeed for ~100 threads and timeout for ~100 threads
                        ThreadParams asyncParams = {lifespan1second, threadName.str()};
                        this->pool->start(&asyncParams, asyncParams.name, 100);
                    }
                    catch(IException *e)
                    {
                        // Expected behavior: some requests should timeout when pool is full
                        StringBuffer msg;
                        e->errorMessage(msg);
                        e->Release();
                        CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool TestPoolWithFastThreadCompletion", msg.str());
                        ++exceptionCount;
                    }

                    // Record total time spent waiting (includes throttle delays and timeouts)
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - beforeDelayedStart);
                    totalStartDuration += duration.count();
                };
                std::thread asyncStart(startThreadFunc);
                asyncStart.detach();
            }

            // Wait all threads to complete
            while(pool->runningCount())
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (exceptionCount < 200)
                waitForThreadsToStart(200 - exceptionCount);
            CPPUNIT_ASSERT_EQUAL(400 - exceptionCount, threadStartedCount.load()); // 200 from phase 1 + ~100 from phase 2
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount()); // Pool empty
            CPPUNIT_ASSERT_EQUAL(400 - exceptionCount, threadCompletedCount.load()); // All started threads completed
            CPPUNIT_ASSERT(exceptionCount >= 99U); // ~100 threads should have timed out
            CPPUNIT_ASSERT(totalStartDuration.load() >= 10000LL); // 100 threads would have been waiting for 100ms each
        }
    }

    void testWaitAvailable()
    {
        // Test: Pool with unlimited capacity should always return true
        pool.setown(createThreadPool("UnlimitedTestPool", factory, true, nullptr, 0)); // defaultmax=0 means unlimited

        auto startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(0));

        // waitAvailable should return true immediately as there are an infinite number of slots available
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(duration.count() <= 100);

        pool.clear();

        // Test: Pool with limited capacity
        pool.setown(createThreadPool("LimitedTestPool", factory, true, nullptr, 2)); // max 2 threads

        // Should return true when pool is empty
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(0));

        // waitAvailable should return true immediately as there are two slots available
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(duration.count() <= 100);

        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan5seconds, "Thread1"};
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread2"};
        pool->start(&params, params.name);

        // Two slots were available, so the threads should start immediately
        duration = measureThreadStartDuration(startTime, 2);
        CPPUNIT_ASSERT(duration.count() <= 100);
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test: waitAvailable should return false when pool is at capacity and timeout expires
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(200));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(duration.count() >= 200);

        // Test: waitAvailable should return true immediately when slot becomes available
        //
        // Wait for one of the running threads to complete
        while (pool->runningCount())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // Slots are now available, so waitAvailable should return immediately
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(duration.count() < 100);

        // Test: Pool capacity was at defaultMax and then fell below it,
        // start 3 thread so that pool capacity is above defaultMax
        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan5seconds, "Thread3"};
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread4"};
        pool->start(&params, params.name);
        params = {lifespan5seconds, "Thread5"};
        pool->start(&params, params.name);

        duration = measureThreadStartDuration(startTime, 2);
        CPPUNIT_ASSERT(duration.count() >= 1000);
        CPPUNIT_ASSERT_EQUAL(3U, pool->runningCount());

        // Test: waitAvailable should return false immediately when no slots are available
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(0));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(duration.count() < 100);
    }

    /* Code dependent upon HPCC-34238

        void testThreadPoolResizing()
        {
            pool.setown(createThreadPool("TestPool", factory, true, nullptr));

            // Start 10 threads with 10 second lifespan
            for (unsigned i = 1; i <= 10; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%u", i);
                params = {lifespan10seconds, threadName.str()};
                pool->start(&params, params.name);
            }
            // Start 10 threads with 5 second lifespan
            for (unsigned i = 11; i <= 20; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%u", i);
                params = {lifespan5seconds, threadName.str()};
                pool->start(&params, params.name);
            }

            waitForThreadsToStart(20);

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
            for (unsigned i = 21; i <= 30; i++)
            {
                StringBuffer threadName;
                threadName.appendf("Thread%u", i);
                try
                {
                    params = {lifespan1second, threadName.str()};
                    pool->start(&params, params.name, 50); // Will use slot timeout of 50ms
                }
                catch (IException *e)
                {
                    StringBuffer msg("Exception encountered: ");
                    e->errorMessage(msg);
                    e->Release();
                    CPPUNIT_ASSERT_EQUAL_STR("Exception encountered: No threads available in pool TestPool", msg.str());
                    ++exceptionCount;
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
private:
    // Helper method to wait for a specific number of threads to start
    void waitForThreadsToStart(unsigned expectedCount)
    {
        for (unsigned i = 0; i < expectedCount; i++)
            startSemaphore.wait();
    }

    // Helper method to reset semaphore state between test sections
    void resetSemaphore()
    {
        startSemaphore.reinit(0);
    }

    // Helper method to measure how long it takes for a given number of threads to start
    std::chrono::milliseconds measureThreadStartDuration(std::chrono::high_resolution_clock::time_point beforeStart, unsigned expectedThreadCount)
    {
        if (expectedThreadCount > 0)
            waitForThreadsToStart(expectedThreadCount);
        auto afterStart = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(afterStart - beforeStart);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolTest, "ThreadPoolTest");

#endif // _USE_CPPUNIT
