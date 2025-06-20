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

constexpr unsigned milliseconds100{100};
constexpr unsigned milliseconds200{200};
constexpr unsigned milliseconds250{250};
constexpr unsigned milliseconds1000{1000};
constexpr unsigned lifespan200milliseconds{200};
constexpr unsigned lifespan300milliseconds{300};
constexpr unsigned lifespan500milliseconds{500};
constexpr unsigned lifespan750milliseconds{750};
constexpr unsigned lifespan1000milliseconds{1000};

// Struct to pass both runtime and name to a thread
struct ThreadParams
{
    unsigned runtimeMs{0};
    const char *name{nullptr};
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
            ThreadParams *p = static_cast<ThreadParams *>(param);
            threadRuntimeMs = p->runtimeMs;
            if (p->name)
                threadName = p->name;
            else
                threadName.clear();
        }
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread %s started", threadName.c_str());

        // Signal after the count is incremented to ensure proper ordering
        startSemaphore.signal();

        std::this_thread::sleep_for(std::chrono::milliseconds(threadRuntimeMs));

        DBGLOG("Thread %s completed", threadName.c_str());
        threadCompletedCount++;
    }

    virtual bool stop() override { return true; }
    virtual bool canReuse() const override { return true; }

private:
    std::atomic<unsigned> &threadStartedCount;
    std::atomic<unsigned> &threadCompletedCount;
    Semaphore &startSemaphore;
    unsigned threadRuntimeMs{999};
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
    CPPUNIT_TEST(testTightlyBoundThreadPool);
    CPPUNIT_TEST(testThrottledThreadPoolWithDefaultDelay);
    CPPUNIT_TEST(testThrottledThreadPoolWithFastThreadCompletion);
    CPPUNIT_TEST(testWaitAvailable);
    CPPUNIT_TEST_SUITE_END();

private:
    std::atomic<unsigned> threadStartedCount{0};
    std::atomic<unsigned> threadCompletedCount{0};
    Owned<IThreadFactory> factory;
    Owned<IThreadPool> pool;
    Semaphore startSemaphore;
    static constexpr const char *noSlotsAvailableMessage = "Start should have thrown an exception as no slots would be available within thread start delay specified";

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

    // Due to variations in scheduling, measurement precision, thread execution timing and environments
    // we allow for some margin of error in our duration checks.
    bool checkDuration(unsigned actualMs, unsigned minExpectedMs)
    {
        constexpr unsigned threadCompleteMarginMs{200};
        unsigned maxExpectedMs = minExpectedMs + threadCompleteMarginMs;
        DBGLOG("checkDuration actualMs: %u minMs: %u maxMs: %u", actualMs, minExpectedMs, maxExpectedMs);

        return actualMs >= minExpectedMs && actualMs <= maxExpectedMs;
    }

    void testTightlyBoundThreadPool()
    {
        // Create thread pool with max 2 threads, testing start with and without start delay
        pool.setown(createThreadPool("TightlyBoundTestPool", factory, true, nullptr, 2, INFINITE)); // Thread pool has an infinite start delay

        // Two new threads should start immediately
        ThreadParams params = {lifespan1000milliseconds, "Thread1"};
        auto beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        params = {lifespan1000milliseconds, "Thread2"};
        pool->start(&params, params.name);
        // Wait for both threads to signal that they've started
        auto duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Test Thread Pool start delay
        //
        // Thread3 should not start until one of the two running threads completes.
        // Due to the short lifespan of the running threads, Thread3 could start
        // immediately if the pool is not at capacity.
        params = {lifespan200milliseconds, "Thread3"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT(threadStartedCount.load() >= 2);
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);

        // Wait for the running count to be 1
        while (pool->runningCount() > 1)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Start Thread4 which should start immediately as the pool has capacity
        params = {lifespan200milliseconds, "Thread4"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);

        // Thread5 should not start as 2 threads are running and it's
        // 100 millisecond start timer should timeout
        params = {lifespan1000milliseconds, "Thread5"};
        beforeStart = std::chrono::high_resolution_clock::now();
        unsigned thread5ExceptionCount = 0;
        if (attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds100, ExpectExceptions::ExceptionsExpected, &thread5ExceptionCount, "start should have timed out and thrown an exception when pool is at capacity"))
        {
            CPPUNIT_FAIL("An exception should have been thrown when starting Thread5");
        }
        duration = measureThreadStartDuration(beforeStart, 0);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 100));
        CPPUNIT_ASSERT_EQUAL(1U, thread5ExceptionCount);
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Test Thread start delay
        //
        // Thread6 should start as one of the two threads will complete within it's
        // 250 millisecond start timer
        params = {lifespan200milliseconds, "Thread6"};
        beforeStart = std::chrono::high_resolution_clock::now();
        if (!attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds250, ExpectExceptions::ExceptionsIgnored))
        {
            CPPUNIT_FAIL("An exception should not have been thrown when starting Thread6");
        }
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT(threadStartedCount.load() >= 4);
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 3);

        // Test Thread startNoBlock
        //
        params = {lifespan200milliseconds, "Thread7"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(6U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());
        // Pool is at capacity, startNoBlock should throw exception immediately
        params = {lifespan200milliseconds, "Thread8"};
        attemptThreadStartCaller(StartFunctionToUse::StartNoBlockFunction, &params, ExpectExceptions::ExceptionsExpected, &duration);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(6U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(4U, threadCompletedCount.load());
        // Wait for the running count to be less than 2
        while (pool->runningCount() >= 2)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // Thread8 should start immediately as the pool has capacity
        params = {lifespan200milliseconds, "Thread9"};
        attemptThreadStartCaller(StartFunctionToUse::StartNoBlockFunction, &params, ExpectExceptions::ExceptionsIgnored, &duration);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

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
        // Create thread pool with max 2 threads and a default start delay of 1 second.
        // Testing thread start() with and without start delay
        pool.setown(createThreadPool("ThrottledThreadPoolWithDefaultDelay", factory, true, nullptr, 2));

        // Both threads should start immediately
        ThreadParams params = {lifespan500milliseconds, "Thread1"};
        auto beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name);
        params = {lifespan500milliseconds, "Thread2"};
        pool->start(&params, params.name);

        // Wait for both threads to signal that they've started
        auto duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

        // The Thread3 should not start after waiting on it's start specified timeout as the other
        // two threads are still running when the timeout expires
        params = {lifespan500milliseconds, "Thread3"};
        unsigned thread3ExceptionCount = 0;
        if (attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds200, ExpectExceptions::ExceptionsExpected, &thread3ExceptionCount, noSlotsAvailableMessage))
            CPPUNIT_FAIL("An exception should have been thrown when starting Thread3");
        CPPUNIT_ASSERT_EQUAL(1U, thread3ExceptionCount);
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // The Thread4 should start after one of the above completes
        beforeStart = std::chrono::high_resolution_clock::now();
        params = {lifespan750milliseconds, "Thread4"};
        unsigned thread4ExceptionCount = 0;
        if (!attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds1000, ExpectExceptions::ExceptionsIgnored))
            CPPUNIT_FAIL("An exception should not have been thrown when starting Thread4");
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 200));
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 1);
        CPPUNIT_ASSERT(pool->runningCount() >= 1);

        // Test Thread Pool default start delay
        //
        beforeStart = std::chrono::high_resolution_clock::now();
        params = {lifespan1000milliseconds, "Thread5"};
        pool->start(&params, params.name);
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 2);
        CPPUNIT_ASSERT(pool->runningCount() >= 2);

        // Thread6 should start up to the Thread Pool default start delay of 1 second
        params = {lifespan1000milliseconds, "Thread6"};
        beforeStart = std::chrono::high_resolution_clock::now();
        pool->start(&params, params.name); // no exception should be thrown as no timeout is specified on the start
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 700));
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 2);
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test: Use infinite thread start delay
        //
        // Ensure no threads are running
        while (pool->runningCount())
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // The two threads should start immediately
        beforeStart = std::chrono::high_resolution_clock::now();
        params = {lifespan750milliseconds, "Thread7"};
        pool->start(&params, params.name, INFINITE);
        params = {lifespan750milliseconds, "Thread8"};
        pool->start(&params, params.name, INFINITE);
        duration = measureThreadStartDuration(beforeStart, 2);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(7U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 4);
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Thread9 should start after one of the above completes
        std::thread asyncStart([this, &beforeStart]()
                               {
                                   beforeStart = std::chrono::high_resolution_clock::now();
                                   ThreadParams asyncParams = {lifespan1000milliseconds, "Thread9"};
                                   this->pool->start(&asyncParams, asyncParams.name, INFINITE); });
        asyncStart.detach();
        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 700));
        CPPUNIT_ASSERT_EQUAL(8U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 6);
        CPPUNIT_ASSERT(pool->runningCount() >= 1);

        // Test multiple threads with INFINITE timeout waiting
        beforeStart = std::chrono::high_resolution_clock::now();
        std::thread asyncStart4([this]()
                                {
                                    ThreadParams asyncParams = {lifespan1000milliseconds, "Thread10"};
                                    this->pool->start(&asyncParams, asyncParams.name, INFINITE); });

        std::thread asyncStart5([this]()
                                {
                                    ThreadParams asyncParams = {lifespan1000milliseconds, "Thread11"};
                                    this->pool->start(&asyncParams, asyncParams.name, INFINITE); });

        asyncStart4.detach();
        asyncStart5.detach();

        // Wait for Thread4 and Thread5 to start (they should wait for slots to become available)
        duration = measureThreadStartDuration(beforeStart, 2);
        // Duration is expected to be less than 1000ms due to Thread9 started running before Thread10 and Thread11
        CPPUNIT_ASSERT(checkDuration(duration.count(), 990));
        CPPUNIT_ASSERT_EQUAL(10U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 8);

        // Test: Use startNoBlock as the start function
        //
        // Ensure no threads are running
        while (pool->runningCount())
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Fill the pool to capacity
        params = {lifespan1000milliseconds, "Thread12"};
        pool->start(&params, params.name);
        params = {lifespan1000milliseconds, "Thread13"};
        pool->start(&params, params.name);

        // When pool is at capacity, startNoBlock should throw exception immediately
        params = {lifespan1000milliseconds, "Thread14"};
        attemptThreadStartCaller(StartFunctionToUse::StartNoBlockFunction, &params, ExpectExceptions::ExceptionsExpected, &duration);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

        // This should block for a start delay of 100ms then throw exception
        params = {lifespan1000milliseconds, "Thread15"};
        unsigned thread15ExceptionCount = 0;
        if (attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds100, ExpectExceptions::ExceptionsExpected, &thread15ExceptionCount, noSlotsAvailableMessage, &duration, 100))
            CPPUNIT_FAIL("An exception should have been thrown when starting Thread15");
        CPPUNIT_ASSERT(checkDuration(duration.count(), 100));
        CPPUNIT_ASSERT_EQUAL(1U, thread15ExceptionCount);

        // This should block for up to 1000ms delay then not throw an exception as no timeout is specified on the thread start
        params = {lifespan1000milliseconds, "Thread16"};
        beforeStart = std::chrono::high_resolution_clock::now();
        if (!attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, 0, ExpectExceptions::ExceptionsIgnored)) // Zero specifies no start timeout parameter
            CPPUNIT_FAIL("An exception should not have been thrown when starting Thread16");

        duration = measureThreadStartDuration(beforeStart, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 800));
        CPPUNIT_ASSERT(threadStartedCount.load() >= 12);
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 11);
        CPPUNIT_ASSERT(pool->runningCount() >= 1);

        // All threads should complete
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(13U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(13U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testThrottledThreadPoolWithFastThreadCompletion()
    {
        // Test the behavior of a throttled thread pool with 100 default max threads
        // and 1ms default start delay under high load with fast-completing threads
        pool.setown(createThreadPool("TestPoolWithFastThreadCompletion", factory, true, nullptr, 100, 1));

        ThreadParams params;
        for (unsigned numIterations = 1; numIterations <= 10; numIterations++)
        {
            threadStartedCount = 0;
            threadCompletedCount = 0;

            unsigned threadId{0};
            StringBuffer threadName;
            auto startTime = std::chrono::high_resolution_clock::now();
            for (unsigned threadId = 0; threadId < 200; threadId++)
            {
                threadName.appendf("Thread%u", threadId);
                if (threadId < 100)
                    params = {lifespan500milliseconds, threadName.str()};
                else
                    params = {lifespan1000milliseconds, threadName.str()};
                pool->start(&params, params.name);
            }

            auto duration = measureThreadStartDuration(startTime, 200);
            CPPUNIT_ASSERT(checkDuration(duration.count(), 100));
            CPPUNIT_ASSERT_EQUAL(200U, pool->runningCount());
            CPPUNIT_ASSERT_EQUAL(200U, threadStartedCount.load());
            CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

            // Test: Pool is at capacity, should throttle new thread starts
            //
            unsigned exceptionCount{0};
            // 100 threads are still running that have approx. 500 milliseconds of runtime left
            // so with 100ms thread pool default start delay we can expect 5 threads
            // to start with no delay. Whilst 100 threads will wait up to 100ms before
            // timing out.
            startTime = std::chrono::high_resolution_clock::now();
            for (threadId = 200; threadId < 400; threadId++)
            {
                threadName.appendf("Thread%u", threadId);
                params = {lifespan1000milliseconds, threadName.str()};
                // Allow both success and timeout
                attemptThreadStartCaller(StartFunctionToUse::StartFunction, &params, milliseconds100, ExpectExceptions::ExceptionsExpectedAndDoNotThrowIfStartSucceeds, &exceptionCount);
            }
            duration = measureThreadStartDuration(startTime, 200 - exceptionCount);

            // Wait all threads to complete
            while (pool->runningCount())
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            CPPUNIT_ASSERT_EQUAL(400 - exceptionCount, threadStartedCount.load());        // 200 from phase 1 + 100 from phase 2 + > 100 that did not time out
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());                               // Pool empty
            CPPUNIT_ASSERT_EQUAL(threadStartedCount.load(), threadCompletedCount.load()); // All started threads completed
            CPPUNIT_ASSERT(exceptionCount >= 10U && exceptionCount < 100U);               // < 100 threads should have timed out, but some will have!
            CPPUNIT_ASSERT(duration.count() >= (exceptionCount * 100));                   // < 100 threads would have been waiting for 100ms each
        }

        // All threads should complete
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT(pool->running());
    }

    void testWaitAvailable()
    {
        // Test: Pool with unlimited capacity should always return true
        pool.setown(createThreadPool("UnlimitedTestPool", factory, true, nullptr, 0)); // defaultmax=0 means unlimited

        auto startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(0));

        // waitAvailable should return true immediately as there are an infinite number of slots available
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

        pool.clear();

        // Test: Pool with limited capacity
        pool.setown(createThreadPool("LimitedTestPool", factory, true, nullptr, 2)); // max 2 threads

        // Should return true when pool is empty
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(100));
        CPPUNIT_ASSERT(pool->waitAvailable(0));

        // waitAvailable should return true immediately as there are two slots available
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

        startTime = std::chrono::high_resolution_clock::now();
        ThreadParams params = {lifespan1000milliseconds, "Thread1"};
        pool->start(&params, params.name);
        params = {lifespan1000milliseconds, "Thread2"};
        pool->start(&params, params.name);

        // Two slots were available, so the threads should start immediately
        duration = measureThreadStartDuration(startTime, 2);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Test: waitAvailable should return false when pool is at capacity and timeout expires
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(200));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 200));

        // Test: waitAvailable should return true immediately when slot becomes available
        //
        // Wait for the running threads to complete
        while (pool->runningCount())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Slots are now available, so waitAvailable should return immediately
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(pool->waitAvailable(1000));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

        // Test: Pool capacity was at defaultMax and then fell below it,
        // start two threads so that pool capacity is at defaultMax
        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan1000milliseconds, "Thread3"};
        pool->start(&params, params.name);
        params = {lifespan1000milliseconds, "Thread4"};
        pool->start(&params, params.name);

        // Wait for the first 2 threads to start immediately
        duration = measureThreadStartDuration(startTime, 2);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));
        CPPUNIT_ASSERT_EQUAL(4U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

        // Now start the third thread which should be throttled
        startTime = std::chrono::high_resolution_clock::now();
        params = {lifespan1000milliseconds, "Thread5"};
        pool->start(&params, params.name);

        // Wait for Thread5 to start (it should be delayed due to throttling)
        duration = measureThreadStartDuration(startTime, 1);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 990));
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT(threadCompletedCount.load() >= 3);

        // Test: waitAvailable should return false immediately when no slots are available
        //
        // Wait for the running threads to complete
        while (pool->runningCount())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        CPPUNIT_ASSERT_EQUAL(5U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(5U, threadCompletedCount.load());

        // Two slots were available, so 3 threads should start immediately
        params = {lifespan1000milliseconds, "Thread6"};
        pool->start(&params, params.name);
        params = {lifespan1000milliseconds, "Thread7"};
        pool->start(&params, params.name);
        // Make sure that no slots are available by starting another thread
        params = {lifespan1000milliseconds, "Thread8"};
        pool->start(&params, params.name);

        // Test: waitAvailable should return false immediately when no slots are available
        startTime = std::chrono::high_resolution_clock::now();
        CPPUNIT_ASSERT(!pool->waitAvailable(0));
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        CPPUNIT_ASSERT(checkDuration(duration.count(), 0));

        // All threads should complete
        pool->joinAll(true);
        CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        CPPUNIT_ASSERT_EQUAL(8U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(8U, threadCompletedCount.load());
        CPPUNIT_ASSERT(pool->running());
    }

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

    enum class StartFunctionToUse
    {
        StartFunction,
        StartNoBlockFunction
    };
    enum class ExpectExceptions
    {
        ExceptionsExpected,
        ExceptionsIgnored,
        ExceptionsExpectedAndDoNotThrowIfStartSucceeds
    };
    struct AttemptThreadStartParameters
    {
        AttemptThreadStartParameters() = default;

        ThreadParams *threadParams{nullptr};
        StartFunctionToUse startFunctionToUse{StartFunctionToUse::StartFunction};
        unsigned timeoutMs{0};
        ExpectExceptions exceptionsExpected{ExpectExceptions::ExceptionsIgnored};
        unsigned *exceptionCount{nullptr};
        const char *failureMessage{nullptr};
        std::chrono::milliseconds *outDuration{nullptr};
        unsigned minExpectedDuration{0};
    };
    bool attemptThreadStartCaller(StartFunctionToUse _startFunctionToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected)
    {
        assertex(_threadParams);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.startFunctionToUse = _startFunctionToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(StartFunctionToUse _startFunctionToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.startFunctionToUse = _startFunctionToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(StartFunctionToUse _startFunctionToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount, const char *_failureMessage)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);
        assertex(_failureMessage);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.startFunctionToUse = _startFunctionToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        attemptThreadStartParameters.failureMessage = _failureMessage;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(StartFunctionToUse _startFunctionToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount, const char *_failureMessage, std::chrono::milliseconds *_outDuration, const unsigned _minExpectedDuration)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);
        assertex(_failureMessage);
        assertex(_outDuration);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.startFunctionToUse = _startFunctionToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        attemptThreadStartParameters.failureMessage = _failureMessage;
        attemptThreadStartParameters.outDuration = _outDuration;
        attemptThreadStartParameters.minExpectedDuration = _minExpectedDuration;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(StartFunctionToUse _startFunctionToUse, ThreadParams *_threadParams, ExpectExceptions _exceptionsExpected, std::chrono::milliseconds *_outDuration)
    {
        assertex(_threadParams);
        assertex(_outDuration);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.startFunctionToUse = _startFunctionToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.outDuration = _outDuration;
        return attemptThreadStart(attemptThreadStartParameters);
    }

    // Helper method to attempt starting a thread with timeout and count exceptions
    bool attemptThreadStart(const AttemptThreadStartParameters &attemptThreadStartParameters)
    {
        auto startTime = std::chrono::high_resolution_clock::now();
        try
        {
            if (attemptThreadStartParameters.timeoutMs == 0) // Use zero to indicate no timeout parameter
            {
                switch (attemptThreadStartParameters.startFunctionToUse)
                {
                case StartFunctionToUse::StartFunction:
                    pool->start(attemptThreadStartParameters.threadParams, attemptThreadStartParameters.threadParams->name);
                    break;
                default:
                    pool->startNoBlock(attemptThreadStartParameters.threadParams);
                    break;
                }
            }
            else
            {
                switch (attemptThreadStartParameters.startFunctionToUse)
                {
                case StartFunctionToUse::StartFunction:
                    // Attempt to start thread with specified timeout
                    pool->start(attemptThreadStartParameters.threadParams, attemptThreadStartParameters.threadParams->name, attemptThreadStartParameters.timeoutMs);
                    break;
                default:
                    pool->startNoBlock(attemptThreadStartParameters.threadParams);
                    break;
                }
            }

            // If we expected an exception on start but didn't get one, fail the test
            if (attemptThreadStartParameters.exceptionsExpected == ExpectExceptions::ExceptionsExpected)
            {
                const char *msg = attemptThreadStartParameters.failureMessage ? attemptThreadStartParameters.failureMessage : "Expected exception was not thrown";
                CPPUNIT_FAIL(msg);
            }

            return true;
        }
        catch (IException *e)
        {
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);

            // Store duration if requested
            if (attemptThreadStartParameters.outDuration)
                *attemptThreadStartParameters.outDuration = duration;

            // Check minimum duration if specified
            if (attemptThreadStartParameters.minExpectedDuration > 0)
                CPPUNIT_ASSERT(duration.count() >= attemptThreadStartParameters.minExpectedDuration);

            bool exceptionIsExpected = (attemptThreadStartParameters.exceptionsExpected == ExpectExceptions::ExceptionsExpected ||
                                        attemptThreadStartParameters.exceptionsExpected == ExpectExceptions::ExceptionsExpectedAndDoNotThrowIfStartSucceeds);
            if (exceptionIsExpected)
            {
                // Expected behavior: requests should timeout when pool is full
                if (attemptThreadStartParameters.exceptionCount)
                    ++*(attemptThreadStartParameters.exceptionCount); // We are expecting an exception, so increment the exception count before the ASSERT.
                // jthread should return sensible error exception code numbers, that can be validated properly.
                // In their absence, we can will simply validate the error message.
                StringBuffer msg;
                e->errorMessage(msg);
                e->Release();
                CPPUNIT_ASSERT(String(msg).startsWith("No threads available"));
            }
            else
            {
                // Unexpected exception - fail the test with the exception message
                StringBuffer msg;
                e->errorMessage(msg);
                e->Release();
                CPPUNIT_FAIL(msg.str());
            }

            return false;
        }
    }
};

thread_local unsigned temp = 0; // Avoids clever compilers optimizing everything away
static unsigned skip(unsigned j)
{
    temp += j;
    return j + 1;
}
static unsigned call_from_thread(unsigned count)
{
    unsigned tot = count;
    for (int j = 0; j < count; j++)
        tot += skip(j);
    return tot;
}

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolTest, "ThreadPoolTest");

#include "thorhelper.hpp"

class ThreadedPersistStressTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ThreadedPersistStressTest);
    CPPUNIT_TEST(testThreads);
    CPPUNIT_TEST_SUITE_END();

    void testThreads()
    {
        testThreadsX(0);
        testThreadsX(1);
        testThreadsX(2);
        testThreadsX(3);
        testThreadsX(4);
        testThreadsX(5);
        testThreadsX(6);
    }
    void testThreadsX(unsigned mode)
    {
        unsigned iters = 10000;
        testThreadsXX(mode, 0, iters);
        testThreadsXX(mode, 10, iters);
        testThreadsXX(mode, 1000, iters);
        testThreadsXX(mode, 2000, iters);
        testThreadsXX(mode, 4000, iters);
        testThreadsXX(mode, 8000, iters);
        testThreadsXX(mode, 16000, iters);
        testThreadsXX(mode, 32000, iters);
        testThreadsXX(mode, 64000, iters);
    }
    void testThreadsXX(unsigned mode, unsigned count, unsigned iters)
    {
        unsigned start = usTick();
        class Threaded : public IThreaded
        {
        public:
            Threaded(unsigned _count) : count(_count) {}
            virtual void threadmain() override
            {
                ret = call_from_thread(count);
            }
            unsigned count;
            unsigned ret = 0;
        } t1(count), t2(count), t3(count);
        class MyThread : public Thread
        {
        public:
            MyThread(unsigned _count) : count(_count) {}
            virtual int run() override
            {
                ret = call_from_thread(count);
                return 0;
            }
            unsigned count;
            unsigned ret = 0;
        };

        unsigned ret = 0;
        switch (mode)
        {
        case 0:
        {
            CThreadedPersistent thread1("1", &t1), thread2("2", &t2), thread3("3", &t3);
            for (unsigned i = 0; i < iters; i++)
            {
                thread1.start(false);
                thread2.start(false);
                thread3.start(false);
                ret = call_from_thread(count);
                thread1.join(INFINITE);
                thread2.join(INFINITE);
                thread3.join(INFINITE);
            }
            ret += t1.ret + t2.ret + t3.ret;
            break;
        }
        case 1:
        {
            for (unsigned i = 0; i < iters; i++)
            {
                t1.threadmain();
                t2.threadmain();
                t3.threadmain();
                ret = call_from_thread(count);
            }
            ret += t1.ret + t2.ret + t3.ret;
            break;
        }
        case 2:
        {
            CThreaded tthread1("1", &t1), tthread2("2", &t2), tthread3("3", &t3);
            for (unsigned i = 0; i < iters; i++)
            {
                tthread1.start(false);
                tthread2.start(false);
                tthread3.start(false);
                ret = call_from_thread(count);
                tthread1.join();
                tthread2.join();
                tthread3.join();
            }
            ret += t1.ret + t2.ret + t3.ret;
            break;
        }
        case 3:
        {
            for (unsigned i = 0; i < iters; i++)
            {
                class casyncfor : public CAsyncFor
                {
                public:
                    casyncfor(unsigned _count) : count(_count), ret(0) {}
                    void Do(unsigned i)
                    {
                        ret += call_from_thread(count);
                    }
                    unsigned count;
                    unsigned ret;
                } afor(count);
                afor.For(4, 4);
                ret = afor.ret;
            }
            break;
        }
        case 4:
        {
            CPersistentTask task1("1", &t1), task2("2", &t2), task3("3", &t3);
            for (unsigned i = 0; i < iters; i++)
            {
                task1.start(false);
                task2.start(false);
                task3.start(false);
                ret = call_from_thread(count);
                task1.join(INFINITE);
                task2.join(INFINITE);
                task3.join(INFINITE);
            }
            ret += t1.ret + t2.ret + t3.ret;
            break;
        }
        case 5:
        {
            MyThread thread1(count), thread2(count), thread3(count);
            for (unsigned i = 0; i < iters; i++)
            {
                thread1.start(false);
                thread2.start(false);
                thread3.start(false);
                ret = call_from_thread(count);
                thread1.join(INFINITE);
                thread2.join(INFINITE);
                thread3.join(INFINITE);
            }
            ret += thread1.ret + thread2.ret + thread3.ret;
            break;
        }
        case 6:
        {
            IArrayOf<IThread> threads;
            for (unsigned i = 0; i < iters; i++)
            {
                MyThread *thread1 = new MyThread(count);
                MyThread *thread2 = new MyThread(count);
                MyThread *thread3 = new MyThread(count);
                threads.append(*thread1);
                threads.append(*thread2);
                threads.append(*thread3);

                thread1->start(false);
                thread2->start(false);
                thread3->start(false);
                ret = call_from_thread(count);
                thread1->join(INFINITE);
                thread2->join(INFINITE);
                thread3->join(INFINITE);
                ret += thread1->ret + thread2->ret + thread3->ret;

#if 0
                if (i >= 600)
                {
                    threads.remove(0);
                    threads.remove(0);
                    threads.remove(0);
                }
#endif
            }
            break;
        }
        }
        constexpr const char *modes[] = {"ThreadedPersistant", "Sequential", "CThreaded", "AsyncFor", "PersistantTask", "Thread", "ManyThread"};
        DBGLOG("%s %d, %d [%u], %u", modes[mode], count, usTick() - start, (usTick() - start) / iters / 4, ret);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadedPersistStressTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadedPersistStressTest, "ThreadedPersistStressTest");

#endif // _USE_CPPUNIT
