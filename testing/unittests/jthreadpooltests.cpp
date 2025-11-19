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

// Struct to pass both runtime (ms) and name to a thread
struct ThreadParams
{
    unsigned runtimeMs{0};
    const char *name{nullptr};
};

class TestPooledThread : public CSimpleInterfaceOf<IPooledThread>
{
public:
    TestPooledThread(std::atomic<unsigned> &_threadStartedCount,
                     std::atomic<unsigned> &_threadCompletedCount,
                     Semaphore &_startSemaphore)
        : threadStartedCount(_threadStartedCount),
          threadCompletedCount(_threadCompletedCount),
          startSemaphore(_startSemaphore)
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
            {
                static unsigned threadCounter{0};
                threadName = VStringBuffer("Thread%u", ++threadCounter).str();
            }
        }
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread \"%s\" started", threadName.c_str());

        // Signal after the count is incremented to ensure proper ordering
        startSemaphore.signal();

        std::this_thread::sleep_for(std::chrono::milliseconds(threadRuntimeMs));

        DBGLOG("Thread \"%s\" completed", threadName.c_str());
        threadCompletedCount++;
    }

    virtual bool stop() override { return true; }
    virtual bool canReuse() const override { return false; } // Disable thread reuse to avoid double-counting

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
    TestThreadFactory(std::atomic<unsigned> &_threadStartedCount,
                      std::atomic<unsigned> &_threadCompletedCount,
                      Semaphore &_startSemaphore)
        : threadStartedCount(_threadStartedCount),
          threadCompletedCount(_threadCompletedCount),
          startSemaphore(_startSemaphore)
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
    CPPUNIT_TEST(testTableDrivenTightlyBoundThreadPool);
    CPPUNIT_TEST(testTableDrivenThrottledThreadPoolWithDefaultDelay);
    CPPUNIT_TEST(testTableDrivenThrottledThreadPoolWithFastThreadCompletion);
    CPPUNIT_TEST(testTableDrivenWaitAvailable);
    CPPUNIT_TEST_SUITE_END();

private:
    std::atomic<unsigned> threadStartedCount{0};
    std::atomic<unsigned> threadCompletedCount{0};
    Owned<IThreadFactory> factory;
    Owned<IThreadPool> pool;
    Semaphore startSemaphore;

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

    enum class FunctionToTest
    {
        StartFunction,
        StartNoBlockFunction,
        WaitAvailableFunction
    };

    enum class FunctionExceptionExpectation
    {
        ShouldSucceed,
        ShouldThrowException,
        OnlyCountExceptions
    };

    enum class ValidationPolicy
    {
        ValidateCounts,
        SkipValidation
    };

    enum class ConcurrencyMode
    {
        Sequential,
        Concurrent
    };

    enum class ExpectedTestFunctionResult
    {
        ExpectedTrue,
        ExpectedFalse,
        IgnoreResult
    };

    static constexpr unsigned DO_NOT_WAIT_FOR_RUNNING_THREADS{INFINITE}; // waitForLessThanRunningThreads

    // Helper function to convert ExpectedTestFunctionResult enum to string
    const char *expectedTestFunctionResultToStr(ExpectedTestFunctionResult result)
    {
        switch (result)
        {
        case ExpectedTestFunctionResult::ExpectedTrue:
            return "ExpectedTrue";
        case ExpectedTestFunctionResult::ExpectedFalse:
            return "ExpectedFalse";
        case ExpectedTestFunctionResult::IgnoreResult:
            return "IgnoreResult";
        default:
            return "Unknown";
        }
    }

    void testTableDrivenTightlyBoundThreadPool()
    {
        // Define test scenarios using the table-driven framework
        std::vector<PoolTestScenario> scenarios = {
            // Test Thread Pool start delay
            {
                "ThreadPoolFullStartTimeout",
                1, // maxThreads
                0, // throttleDelayMs (0=infinite)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"FillPoolThread", 1000, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},         // Fill pool slot 1
                    {"TimeoutThread", 100, 0, 100, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Pool full, timeout after 100ms
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            {"DelayedThreadStart",
             1, // maxThreads
             0, // throttleDelayMs (0=infinite)
             {
                 // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                 {"FillPoolThread", 500, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},             // Fill pool slot 1
                 {"ThrottledDelayedThread", 100, 500, 600, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait ~600ms for Thread1 to complete
             }},
            {
                "StartNoBlockFunction",
                2, // maxThreads
                0, // throttleDelayMs (0=infinite)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"FillPoolThread1", 300, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},          // Start no block should fill pool slot 1
                    {"FillPoolThread2", 300, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},          // Start no block should fill pool slot 2
                    {"StartNoBlockFail", 200, 0, 0, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Start no block and pool at capacity so an exception should be thrown
                    {"WaitBlockedThread", 200, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, 2},                                      // Start no block should fill pool slot 2 as waiting for < 2 threads running
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            // True throttling behavior - threads start after throttle delay expires
            {
                "TrueThrottlingBehavior",
                2,   // maxThreads
                400, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"LongThread1", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},      // Long-running, won't complete during test
                    {"LongThread2", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},      // Long-running, won't complete during test
                    {"ThrottledThread", 200, 400, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Wait full 400ms throttle, then start anyway
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            // Timeout before throttling completes - demonstrates true throttling timeout
            {
                "TimeoutBeforeThrottle",
                2,   // maxThreads
                600, // throttleDelayMs - threads wait this long when pool is full
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"Blocker2", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"ThrottledThread", 200, 600, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},               // Wait full 600ms throttle, then start
                    {"TimeoutThread", 200, 0, 300, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS},                // Timeout after 300ms (< 600ms throttle), never starts
                    {"StartNoBlockFail", 200, 0, INFINITE, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // startNoBlock should fail immediately
                },
                50,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (timeout doesn't start thread)
            },
            // Mixed: slot availability + throttling + timeouts
            {
                "MixedBehaviors",
                2,   // maxThreads
                300, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Quick1", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},        // Fill pool, complete quickly
                    {"Quick2", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},        // Fill pool, complete quickly
                    {"WaitForSlot", 200, 100, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait ~100ms for Quick1/2 to complete
                    {"LongRunner", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Start immediately, run long
                    {"Throttled", 200, 200, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Pool full, wait full 300ms throttle
                    {"FastTimeout", 200, 0, 50, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Timeout before throttle expires
                },
                30,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (has timeout)
            },

            // Concurrent starts - test true concurrent blocking behavior
            {
                "ConcurrentThrottling",
                2,   // maxThreads
                500, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Long-running, holds slot
                    {"Blocker2", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Long-running, holds slot
                    // These will be started concurrently to test true concurrent blocking/throttling
                    {"Concurrent1", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                    {"Concurrent2", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                    {"Concurrent3", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                },
                50,                               // durationWiggleMs
                ValidationPolicy::ValidateCounts, // validationPolicy
                ConcurrencyMode::Concurrent       // concurrencyMode
            }

        };

        for (const auto &scenario : scenarios)
            runTableDrivenScenario(scenario);
    }

    void testTableDrivenThrottledThreadPoolWithDefaultDelay()
    {
        // Define test scenarios using the table-driven framework
        std::vector<PoolTestScenario> scenarios = {
            // Test Thread Pool start delay
            {
                "StartFunctionThrottleDelay",
                2,    // maxThreads
                1000, // throttleDelayMs (0=infinite)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"FillPoolThread1", 1000, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},              // Fill pool slot 1
                    {"FillPoolThread2", 1000, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},              // Fill pool slot 2
                    {"TimeoutThread", 100, 100, 100, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS},     // Timeout after 100ms, never starts
                    {"ThrottledDelayThread", 100, 900, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait ~1000ms (pool default) for Thread1/2 to complete
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            {
                "StartNoBlockFunctionThrottleDelay",
                2,    // maxThreads
                1000, // throttleDelayMs (0=infinite)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"FillPoolThread1", 300, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},          // Fill pool slot 1
                    {"FillPoolThread2", 300, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},          // Fill pool slot 2
                    {"StartNoBlockFail", 200, 0, 0, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Start no block and pool at capacity, an exception will be thrown
                    {"WaitBlockedThread", 200, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedTrue, 2},                                      // Start no block should fill pool slot 2 as waiting for < 2 threads running
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            // True throttling behavior - threads start after throttle delay expires
            {
                "TrueThrottlingBehavior",
                2,   // maxThreads
                400, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"LongThread1", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},      // Long-running, won't complete during test
                    {"LongThread2", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},      // Long-running, won't complete during test
                    {"ThrottledThread", 200, 400, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Wait full 400ms throttle, then start anyway
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            // Timeout before throttling completes - demonstrates true throttling timeout
            {
                "TimeoutBeforeThrottle",
                2,   // maxThreads
                600, // throttleDelayMs - threads wait this long when pool is full
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"Blocker2", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"ThrottledThread", 200, 600, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},               // Wait full 600ms throttle, then start
                    {"TimeoutThread", 200, 0, 300, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS},                // Timeout after 300ms (< 600ms throttle), never starts
                    {"StartNoBlockFail", 200, 0, INFINITE, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // startNoBlock should fail immediately
                },
                50,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (timeout doesn't start thread)
            },
            // Mixed: slot availability + throttling + timeouts
            {
                "MixedBehaviors",
                2,   // maxThreads
                300, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Quick1", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},                                                                                   // Fill pool, complete quickly
                    {"Quick2", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},                                                                                   // Fill pool, complete quickly
                    {"WaitForSlot", 200, 100, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},                                                                            // Wait ~100ms for Quick1/2 to complete
                    {"LongRunner", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},                                                                              // Start immediately, run long
                    {"Throttled", 200, 200, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction},                                                                              // Pool full, wait full 300ms throttle
                    {"FastTimeout", 200, 0, 50, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Timeout before throttle expires
                },
                30,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (has timeout)
            },
            // Concurrent starts - test true concurrent blocking behavior
            {
                "ConcurrentThrottling",
                2,   // maxThreads
                500, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Long-running, holds slot
                    {"Blocker2", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Long-running, holds slot
                    // These will be started concurrently to test true concurrent blocking/throttling
                    {"Concurrent1", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                    {"Concurrent2", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                    {"Concurrent3", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction}, // Should wait 500ms throttle
                },
                50,                               // durationWiggleMs
                ValidationPolicy::ValidateCounts, // validationPolicy
                ConcurrencyMode::Concurrent       // concurrencyMode
            }};

        unsigned expectedInitialValidationCount = 2;
        unsigned furtherValidationCountLeft = expectedInitialValidationCount;
        for (const auto &scenario : scenarios)
        {
            runTableDrivenScenario(scenario);

            if (strcmp(scenario.testName, "StartFunctionThrottleDelay") == 0)
            {
                // Should have 1 exception (Thread3 timeout) and 3 successful starts (Thread1, Thread2, Thread4)
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 1 timeout exception", scenario.testName).str(),
                    1U, scenario.actualExceptionCount);
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 3 threads to start", scenario.testName).str(),
                    3U, scenario.actualThreadStartedCount);
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 3 threads to complete", scenario.testName).str(),
                    3U, scenario.actualThreadCompletedCount);
                --furtherValidationCountLeft;
            }
            else if (strcmp(scenario.testName, "StartNoBlockFunctionThrottleDelay") == 0)
            {
                // Should have 1 exception (Thread3) and 3 successful starts (Thread1, Thread2, Thread4)
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 1 startNoBlock exception", scenario.testName).str(),
                    1U, scenario.actualExceptionCount);
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 3 threads to start", scenario.testName).str(),
                    3U, scenario.actualThreadStartedCount);
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    VStringBuffer("Scenario \"%s\": expected 3 threads to complete", scenario.testName).str(),
                    3U, scenario.actualThreadCompletedCount);
                --furtherValidationCountLeft;
            }
        }
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
            VStringBuffer("Scenario \"%s\": expected extra validation for %u tests but only processed %u test",
                __func__,
                expectedInitialValidationCount,
                furtherValidationCountLeft).str(),
            0U, furtherValidationCountLeft);
    }

    void testTableDrivenThrottledThreadPoolWithFastThreadCompletion()
    {
        // Creat test threads that fill the pool and exceed the pool capacity
        std::vector<ThreadSpec> threads;
        StringBuffer threadName;
        for (unsigned threadId = 0; threadId < 200; threadId++)
        {
            threadName.clear().appendf("Thread%u", threadId);
            // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
            // With a 1ms throttle delay, threads beyond the pool capacity should start after the throttle delay expires (1ms), not after waiting for other threads to complete
            threads.emplace_back(ThreadSpec{threadName.str(), (threadId < 100 ? 500U : 1000U), (threadId < 100 ? 0U : 1U), INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS});
        }
        for (unsigned threadId = 200; threadId < 400; threadId++)
        {
            threadName.clear().appendf("Thread%u", threadId);
            // Allow both success and timeout - these threads should start immediately if slots are available,
            // or timeout after 100ms if the pool is full
            threads.emplace_back(ThreadSpec{threadName.str(), 1000, 0, 100, FunctionExceptionExpectation::OnlyCountExceptions, FunctionToTest::StartFunction, ExpectedTestFunctionResult::IgnoreResult, DO_NOT_WAIT_FOR_RUNNING_THREADS});
        }

        // Run the test scenario
        PoolTestScenario scenario = {
            "TestThrottledThreadPoolWithFastThreadCompletion",
            100, // maxThreads
            1,   // throttleDelayMs (0=infinite)
            threads,
            50,                               // durationWiggleMs
            ValidationPolicy::ValidateCounts, // validationPolicy
            ConcurrencyMode::Sequential       // concurrencyMode
        };
        runTableDrivenScenario(scenario);

        // Verify that some exceptions occurred for the timeout threads (Thread200-399)
        // The exact count is variable depending on timing, but should be > 5 and < 50
        CPPUNIT_ASSERT_MESSAGE(
            VStringBuffer("Expected some exceptions (>5) for timeout threads, got %u", scenario.actualExceptionCount).str(),
            scenario.actualExceptionCount >= 5U && scenario.actualExceptionCount < 50U);

        // Verify that 400 threads started and completed counts are 400 minus the exception count
        CPPUNIT_ASSERT_MESSAGE(
            VStringBuffer("Expected 400 threads minus the exception count started, got %u", scenario.actualThreadStartedCount).str(),
            (scenario.actualThreadStartedCount + scenario.actualExceptionCount == 400));
        CPPUNIT_ASSERT_MESSAGE(
            VStringBuffer("Expected 400 threads minus the exception count complete, got %u", scenario.actualThreadCompletedCount).str(),
            (scenario.actualThreadCompletedCount + scenario.actualExceptionCount));
    }

    void testTableDrivenWaitAvailable()
    {
        // Define test scenarios using the table-driven framework
        std::vector<PoolTestScenario> scenarios = {
            // Wait for available - Unlimited capacity pool
            {
                "PoolWithUnlimitedCapacity",
                0,    // maxThreads (unlimited)
                1000, // throttleDelayMs (default)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"waitAvailableShouldReturnTrue1", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // waitAvailable should return true
                    {"waitAvailableShouldReturnTrue2", 0, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}    // waitAvailable should return true
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },
            // Test: Pool with limited capacity
            {
                "PoolWithLimitedCapacity",
                1,    // maxThreads (limited)
                1000, // throttleDelayMs (default)
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    // Test: waitAvailable should return true when pool is empty
                    {"waitAvailableShouldReturnTrue1", 0, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // waitAvailable should return true
                    // Test: waitAvailable should return true when pool is empty
                    {"waitAvailableShouldReturnTrue2", 0, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // waitAvailable should return true
                    // Fill pool
                    {"FillPoolThread", 200, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},
                    // Test: waitAvailable should return false immediately when no slots are available
                    {"waitAvailableShouldReturnFalse1", 0, 0, 0, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // waitAvailable should return false
                    // Test: waitAvailable should return false when pool is at capacity and timeout expires
                    {"waitAvailableShouldReturnFalse2", 0, 0, 50, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // waitAvailable should return false
                    // Test: waitAvailable should return true immediately when slot becomes available
                    {"waitAvailableShouldReturnTrue", 0, 0, 250, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::WaitAvailableFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS} // waitAvailable should return true
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            }};

        for (const auto &scenario : scenarios)
            runTableDrivenScenario(scenario);
    }

    void testTableDrivenScenarios()
    {
        // Define test scenarios using the table-driven framework
        std::vector<PoolTestScenario> scenarios = {
            // Wait for slot availability - not true throttling
            {
                "WaitForSlotAvailability",
                2,    // maxThreads
                1000, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"FillPoolThread1", 300, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Fill pool slot 1
                    {"FillPoolThread2", 300, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Fill pool slot 2
                    {"ThrottledThread", 200, 300, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait ~300ms for Thread1/2 to complete
                    {"NotBlockedThread", 200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},  // Start immediately (slot available after Thread3 blocks)
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },

            // True throttling behavior - threads start after throttle delay expires
            {
                "TrueThrottlingBehavior",
                2,   // maxThreads
                400, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"LongThread1", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},      // Long-running, won't complete during test
                    {"LongThread2", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},      // Long-running, won't complete during test
                    {"ThrottledThread", 200, 400, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait full 400ms throttle, then start anyway
                },
                50,                              // durationWiggleMs
                ValidationPolicy::ValidateCounts // validationPolicy
            },

            // Timeout before throttling completes - demonstrates true throttling timeout
            {
                "TimeoutBeforeThrottle",
                2,   // maxThreads
                600, // throttleDelayMs - threads wait this long when pool is full
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"Blocker2", 1200, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},                       // Long-running blocker (1200ms)
                    {"ThrottledThread", 200, 600, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},               // Wait full 600ms throttle, then start
                    {"TimeoutThread", 200, 0, 300, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS},                // Timeout after 300ms (< 600ms throttle), never starts
                    {"StartNoBlockFail", 200, 0, INFINITE, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartNoBlockFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // startNoBlock should fail immediately
                },
                50,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (timeout doesn't start thread)
            },

            // Mixed: slot availability + throttling + timeouts
            {
                "MixedBehaviors",
                2,   // maxThreads
                300, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Quick1", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},        // Fill pool, complete quickly
                    {"Quick2", 100, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},        // Fill pool, complete quickly
                    {"WaitForSlot", 200, 100, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Wait ~100ms for Quick1/2 to complete
                    {"LongRunner", 1000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Start immediately, run long
                    {"Throttled", 200, 200, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS},   // Pool full, wait full 300ms throttle
                    {"FastTimeout", 200, 0, 50, FunctionExceptionExpectation::ShouldThrowException, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedFalse, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Timeout before throttle expires
                },
                30,                              // durationWiggleMs
                ValidationPolicy::SkipValidation // validationPolicy (has timeout)
            },

            // Concurrent starts - test true concurrent blocking behavior
            {
                "ConcurrentThrottling",
                2,   // maxThreads
                500, // throttleDelayMs
                {
                    // Thread name, runtimeMs, expectedFunctionDelayMs, functionTimeoutMs, functionExceptionExpectation, functionToTest, expectedTestFunctionResult, waitForLessThanRunningThreads
                    {"Blocker1", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Long-running, holds slot
                    {"Blocker2", 2000, 0, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Long-running, holds slot
                    // These will be started concurrently to test true concurrent blocking/throttling
                    {"Concurrent1", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Should wait 500ms throttle
                    {"Concurrent2", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Should wait 500ms throttle
                    {"Concurrent3", 200, 500, INFINITE, FunctionExceptionExpectation::ShouldSucceed, FunctionToTest::StartFunction, ExpectedTestFunctionResult::ExpectedTrue, DO_NOT_WAIT_FOR_RUNNING_THREADS}, // Should wait 500ms throttle
                },
                50,                               // durationWiggleMs
                ValidationPolicy::ValidateCounts, // validationPolicy
                ConcurrencyMode::Concurrent       // concurrencyMode
            }};

        for (const auto &scenario : scenarios)
            runTableDrivenScenario(scenario);
    }

    struct ThreadSpec
    {
        std::string name;
        unsigned runtimeMs{0};                                                                                  // How long the thread should run in ms
        unsigned expectedStartDelayMs{0};                                                                       // Expected delay before thread starts when pool is at capacity (0 = immediate)
        unsigned functionTimeoutMs{0};                                                                          // Timeout passed into test function (INFINITE = no timeout)
        FunctionExceptionExpectation functionExceptionExpectation{FunctionExceptionExpectation::ShouldSucceed}; // Whether starting this thread should throw exception
        FunctionToTest functionToTest{FunctionToTest::StartFunction};                                           // Which start function to test
        ExpectedTestFunctionResult expectedTestFunctionResult{ExpectedTestFunctionResult::ExpectedTrue};        // Expected result of start function
        unsigned waitForLessThanRunningThreads{DO_NOT_WAIT_FOR_RUNNING_THREADS};                                // Wait for less than X threads to be running before starting this thread, DO_NOT_WAIT_FOR_RUNNING_THREADS = don't wait

        ThreadSpec(const std::string &_name,
                   unsigned _runtimeMs,
                   unsigned _expectedStartDelayMs,
                   unsigned _functionTimeoutMs,
                   FunctionExceptionExpectation _functionExceptionExpectation,
                   FunctionToTest _functionToTest,
                   ExpectedTestFunctionResult _expectedTestFunctionResult,
                   unsigned _waitForLessThanRunningThreads)
            : name(_name),
              runtimeMs(_runtimeMs),
              expectedStartDelayMs(_expectedStartDelayMs),
              functionTimeoutMs(_functionTimeoutMs),
              functionExceptionExpectation(_functionExceptionExpectation),
              functionToTest(_functionToTest),
              expectedTestFunctionResult(_expectedTestFunctionResult),
              waitForLessThanRunningThreads(_waitForLessThanRunningThreads)
        {
        }
        ThreadSpec(const std::string &_name,
                   unsigned _runtimeMs,
                   unsigned _expectedStartDelayMs,
                   unsigned _functionTimeoutMs,
                   FunctionExceptionExpectation __functionExceptionExpectation,
                   FunctionToTest _functionToTest)
            : name(_name),
              runtimeMs(_runtimeMs),
              expectedStartDelayMs(_expectedStartDelayMs),
              functionTimeoutMs(_functionTimeoutMs),
              functionExceptionExpectation(__functionExceptionExpectation),
              functionToTest(_functionToTest),
              expectedTestFunctionResult(ExpectedTestFunctionResult::ExpectedTrue), // default
              waitForLessThanRunningThreads(DO_NOT_WAIT_FOR_RUNNING_THREADS)        // default
        {
        }
    };

    struct PoolTestScenario
    {
        // Test scenario attributes
        const char *testName{nullptr}; // Used as both test name and pool name
        unsigned maxThreads{0};        // Pool capacity (0 = unlimited)
        unsigned throttleDelayMs{0};   // Throttling delay when pool is at capacity (INFINITE = infinite)
        std::vector<ThreadSpec> threads;

        // Validation settings
        unsigned durationWiggleMs{50};                                       // Allowed timing variance in milliseconds
        ValidationPolicy validationPolicy{ValidationPolicy::ValidateCounts}; // Whether to validate final started/completed counts
        ConcurrencyMode concurrencyMode{ConcurrencyMode::Sequential};        // Whether to use concurrent thread starting for this scenario

        // Output fields - populated during test execution
        mutable unsigned actualExceptionCount{0};       // Actual number of exceptions generated during test execution (output field for test verification)
        mutable unsigned actualThreadStartedCount{0};   // Actual number of threads that successfully started (output field for test verification)
        mutable unsigned actualThreadCompletedCount{0}; // Actual number of threads that completed execution (output field for test verification)
    };

    // Helper method to create custom test scenarios easily
    static PoolTestScenario createScenario(const char *testName, unsigned maxThreads,
                                           unsigned throttleDelayMs = 1000, unsigned wiggleMs = 50,
                                           ValidationPolicy validationPolicy = ValidationPolicy::ValidateCounts, ConcurrencyMode concurrencyMode = ConcurrencyMode::Sequential)
    {
        return {testName, maxThreads, throttleDelayMs, {}, wiggleMs, validationPolicy, concurrencyMode, 0, 0, 0};
    }

private:
    // Execute a table-driven test scenario and populate output fields
    // The scenario output fields are populated with actual execution results:
    // - actualExceptionCount: number of exceptions that occurred during execution
    // - actualThreadStartedCount: number of threads that successfully started
    // - actualThreadCompletedCount: number of threads that completed execution
    // These allow tests to verify counts after scenario completion.
    void runTableDrivenScenario(const PoolTestScenario &scenario)
    {
        DBGLOG("Running scenario: %s", scenario.testName);

        // Ensure clean state - tearDown any previous test state first
        tearDown();

        // Reset state
        setUp();

        // Create pool with specified parameters
        pool.setown(createThreadPool(scenario.testName, factory, true, nullptr, scenario.maxThreads, scenario.throttleDelayMs));

        unsigned expectedStartedCount = 0;
        unsigned exceptionCount = 0;

        // Special handling for concurrent start scenarios
        if (scenario.concurrencyMode == ConcurrencyMode::Concurrent)
        {
            runConcurrentThrottlingScenario(scenario, expectedStartedCount, exceptionCount);
        }
        else
        {
            // Execute each thread specification sequentially
            for (const auto &threadSpec : scenario.threads)
            {
                runSingleThreadSpec(threadSpec, scenario, expectedStartedCount, exceptionCount);
            }
        }

        // Final validations
        if (scenario.validationPolicy == ValidationPolicy::ValidateCounts)
        {
            // Wait for all threads to complete
            pool->joinAll(true);

            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                VStringBuffer("Scenario \"%s\": started count mismatch", scenario.testName).str(),
                expectedStartedCount, threadStartedCount.load());

            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                VStringBuffer("Scenario \"%s\": completed count mismatch", scenario.testName).str(),
                expectedStartedCount, threadCompletedCount.load());

            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                VStringBuffer("Scenario \"%s\": running count should be 0", scenario.testName).str(),
                0U, pool->runningCount());

            // Store the actual counts in the scenario for test verification
            scenario.actualExceptionCount = exceptionCount;
            scenario.actualThreadStartedCount = threadStartedCount.load();
            scenario.actualThreadCompletedCount = threadCompletedCount.load();

            // For scenarios using OnlyCountExceptions, log the actual exception count
            bool hasCountExceptionThreads = false;
            for (const auto &threadSpec : scenario.threads)
            {
                if (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::OnlyCountExceptions)
                {
                    hasCountExceptionThreads = true;
                    break;
                }
            }

            if (hasCountExceptionThreads)
            {
                // Log the actual counts for scenarios with variable exception counts
                DBGLOG("Scenario \"%s\": Actual exception count: %u, started: %u, completed: %u",
                       scenario.testName, exceptionCount, threadStartedCount.load(), threadCompletedCount.load());
            }
        }

        DBGLOG("Scenario %s completed successfully", scenario.testName);
    }

    void runConcurrentThrottlingScenario(const PoolTestScenario &scenario, unsigned &expectedStartedCount, unsigned &exceptionCount)
    {
        // This method tests true concurrent thread starting behavior.
        // Unlike the sequential approach, all "concurrent" threads attempt to start
        // at nearly the same time, providing a more realistic test of how throttling
        // behaves when multiple threads compete for pool slots simultaneously.

        // Start enough threads to fill the pool
        for (unsigned i = 0; i < scenario.maxThreads; i++)
        {
            runSingleThreadSpec(scenario.threads[i], scenario, expectedStartedCount, exceptionCount);
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Small delay to ensure they start and fill the pool
        }

        // Start remaining threads concurrently (within a very short window)
        std::vector<std::thread> startThreads;
        std::vector<std::chrono::milliseconds> durations(scenario.threads.size());
        std::vector<bool> startSuccessful(scenario.threads.size() - scenario.maxThreads);

        auto concurrentStartTime = std::chrono::high_resolution_clock::now();

        for (unsigned i = scenario.maxThreads; i < scenario.threads.size(); i++)
        {
            unsigned idx = i - scenario.maxThreads;
            startThreads.emplace_back([this, &scenario, i, idx, &durations, &startSuccessful, concurrentStartTime]()
                                      {
                const auto& threadSpec = scenario.threads[i];
                ThreadParams params = {threadSpec.runtimeMs, threadSpec.name.c_str()};

                try
                {
                    pool->start(&params, params.name);
                    startSuccessful[idx] = (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldSucceed);

                    auto endTime = std::chrono::high_resolution_clock::now();
                    durations[idx] = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - concurrentStartTime);
                }
                catch (IException* e)
                {
                    startSuccessful[idx] = (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldThrowException);
                    e->Release();

                    auto endTime = std::chrono::high_resolution_clock::now();
                    durations[idx] = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - concurrentStartTime);
                } });
        }

        // Wait for all concurrent starts to complete
        for (auto &t : startThreads)
            t.join();

        // Validate timing for concurrent threads
        for (unsigned i = scenario.maxThreads; i < scenario.threads.size(); i++)
        {
            unsigned idx = i - scenario.maxThreads;
            const auto &threadSpec = scenario.threads[i];

            if (startSuccessful[idx])
            {
                validateThreadStartTiming(scenario.testName, threadSpec, durations[idx], scenario.durationWiggleMs);
                expectedStartedCount++;
            }
        }
    }

    void runSingleThreadSpec(const ThreadSpec &threadSpec, const PoolTestScenario &scenario,
                             unsigned &expectedStartedCount, unsigned &exceptionCount)
    {
        // Wait for running count to be less than specified value for this thread
        if (threadSpec.waitForLessThanRunningThreads != DO_NOT_WAIT_FOR_RUNNING_THREADS)
        {
            while (pool->runningCount() >= threadSpec.waitForLessThanRunningThreads)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        auto startTime = std::chrono::high_resolution_clock::now();

        ThreadParams params = {threadSpec.runtimeMs, threadSpec.name.c_str()};

        bool startSuccessful = false;
        try
        {
            switch (threadSpec.functionToTest)
            {
            case FunctionToTest::StartNoBlockFunction:
                pool->startNoBlock(&params);
                startSuccessful = (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldSucceed ||
                                   threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::OnlyCountExceptions);
                break;
            case FunctionToTest::StartFunction:
                if (threadSpec.functionTimeoutMs == INFINITE)
                    pool->start(&params, params.name);
                else
                    pool->start(&params, params.name, threadSpec.functionTimeoutMs);
                startSuccessful = (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldSucceed ||
                                   threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::OnlyCountExceptions);
                break;
            case FunctionToTest::WaitAvailableFunction:
                startSuccessful = pool->waitAvailable(threadSpec.functionTimeoutMs);
                break;
            default:
                CPPUNIT_FAIL("Invalid start function");
            }

            if (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldThrowException)
            {
                StringBuffer msg;
                msg.appendf("Thread \"%s\" should have thrown exception but didn't", threadSpec.name.c_str());
                CPPUNIT_FAIL(msg.str());
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            switch (threadSpec.functionExceptionExpectation)
            {
            case FunctionExceptionExpectation::ShouldThrowException:
            {
                // Expected exception - validate message contains pool name
                StringBuffer expectedMsg;
                expectedMsg.appendf("No threads available in pool %s", scenario.testName);
                CPPUNIT_ASSERT_EQUAL_STR(expectedMsg.str(), msg.str());
                exceptionCount++;
                break;
            }
            case FunctionExceptionExpectation::OnlyCountExceptions:
                // Count exception but don't fail test - this is expected behavior
                exceptionCount++;
                DBGLOG("Thread \"%s\" threw expected exception (count: %u): %s", threadSpec.name.c_str(), exceptionCount, msg.str());
                break;
            case FunctionExceptionExpectation::ShouldSucceed:
                CPPUNIT_FAIL(VStringBuffer("Thread \"%s\" threw unexpected exception: %s", threadSpec.name.c_str(), msg.str()).str());
                break;
            default:
                CPPUNIT_FAIL(VStringBuffer("Invalid start exception %d", static_cast<int>(threadSpec.functionExceptionExpectation)).str());
                break;
            }
        }

        // Validate thread start function return
        switch (threadSpec.expectedTestFunctionResult)
        {
        case ExpectedTestFunctionResult::IgnoreResult:
            break;
        case ExpectedTestFunctionResult::ExpectedTrue:
        case ExpectedTestFunctionResult::ExpectedFalse:
        {
            bool expectedResult = (threadSpec.expectedTestFunctionResult == ExpectedTestFunctionResult::ExpectedTrue);
            CPPUNIT_ASSERT_MESSAGE(
                VStringBuffer("Thread \"%s\" start function returned %s but expected %s",
                              threadSpec.name.c_str(), boolToStr(startSuccessful),
                              expectedTestFunctionResultToStr(threadSpec.expectedTestFunctionResult))
                    .str(),
                expectedResult == startSuccessful);
            break;
        }
        default:
            CPPUNIT_FAIL("Invalid expected start function result enum");
            break;
        }

        // Validate thread timing
        switch (threadSpec.functionToTest)
        {
        case FunctionToTest::StartNoBlockFunction:
        case FunctionToTest::StartFunction:
            // Measure actual start duration if thread started successfully
            if (startSuccessful)
            {
                auto duration = measureThreadStartDuration(startTime, 1);
                // Only validate timing for threads that have specific expectations
                // OnlyCountExceptions threads that succeed are allowed to start at any time
                if (threadSpec.functionExceptionExpectation != FunctionExceptionExpectation::OnlyCountExceptions)
                    validateThreadStartTiming(scenario.testName, threadSpec, duration, scenario.durationWiggleMs);
                expectedStartedCount++;
            }
            else
            {
                // For failed starts, just measure the duration of the failure
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - startTime);

                // If we expected the start to fail due to timeout, validate the timeout duration
                if (threadSpec.functionExceptionExpectation == FunctionExceptionExpectation::ShouldThrowException && threadSpec.functionTimeoutMs != INFINITE)
                {
                    unsigned expectedTimeout = threadSpec.functionTimeoutMs;
                    int actualDuration = duration.count();
                    int minWiggleTimeout = (expectedTimeout - scenario.durationWiggleMs);
#ifdef _DEBUG
                    if (!(actualDuration >= minWiggleTimeout))
                        DBGLOG("Thread \"%s\" timeout duration %u should be >= %u",
                               threadSpec.name.c_str(), (unsigned)duration.count(), expectedTimeout);
#endif
                    CPPUNIT_ASSERT_MESSAGE(
                        VStringBuffer("Thread \"%s\" timeout duration %u should be >= %u",
                                      threadSpec.name.c_str(), (unsigned)duration.count(), expectedTimeout)
                            .str(),
                        actualDuration >= minWiggleTimeout);
                }
            }
            break;
        case FunctionToTest::WaitAvailableFunction:
            break;
        default:
            CPPUNIT_FAIL("Invalid start function enum");
            break;
        }
    }

    void validateThreadStartTiming(const char *testName, const ThreadSpec &threadSpec, std::chrono::milliseconds actualDuration, unsigned wiggleMs)
    {
        unsigned actualMs = (unsigned)actualDuration.count();
        unsigned expectedMs = threadSpec.expectedStartDelayMs;

        if (expectedMs == 0)
        {
            // Should start immediately - allow some wiggle room for scheduling
#ifdef _DEBUG
            if (!(actualMs <= wiggleMs))
                DBGLOG("Scenario \"%s\" Thread \"%s\" should start immediately but took %ums",
                       testName, threadSpec.name.c_str(), actualMs);
#endif
            CPPUNIT_ASSERT_MESSAGE(
                VStringBuffer("Scenario \"%s\" Thread \"%s\" should start immediately but took %ums",
                              testName, threadSpec.name.c_str(), actualMs)
                    .str(),
                actualMs <= wiggleMs);
        }
        else
        {
            // Should be delayed by approximately expectedMs due to throttling when pool at capacity
            unsigned minExpected = (expectedMs > wiggleMs) ? expectedMs - wiggleMs : 0;
            unsigned maxExpected = expectedMs + wiggleMs;
#ifdef _DEBUG
            if (!(actualMs >= minExpected && actualMs <= maxExpected))
                DBGLOG("Scenario \"%s\" Thread \"%s\" expected throttling delay %ums but actual was %ums (range: %u-%u)",
                       testName, threadSpec.name.c_str(), expectedMs, actualMs, minExpected, maxExpected);
#endif
            CPPUNIT_ASSERT_MESSAGE(
                VStringBuffer("Scenario \"%s\" Thread \"%s\" expected throttling delay %ums but actual was %ums (range: %u-%u)",
                              testName, threadSpec.name.c_str(), expectedMs, actualMs, minExpected, maxExpected)
                    .str(),
                actualMs >= minExpected && actualMs <= maxExpected);
        }
    }

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
        FunctionToTest functionToTestToUse{FunctionToTest::StartFunction};
        unsigned timeoutMs{0};
        ExpectExceptions exceptionsExpected{ExpectExceptions::ExceptionsIgnored};
        unsigned *exceptionCount{nullptr};
        const char *failureMessage{nullptr};
        std::chrono::milliseconds *outDuration{nullptr};
        unsigned minExpectedDuration{0};
    };
    bool attemptThreadStartCaller(FunctionToTest _functionToTestToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected)
    {
        assertex(_threadParams);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.functionToTestToUse = _functionToTestToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(FunctionToTest _functionToTestToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.functionToTestToUse = _functionToTestToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(FunctionToTest _functionToTestToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount, const char *_failureMessage)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);
        assertex(_failureMessage);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.functionToTestToUse = _functionToTestToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        attemptThreadStartParameters.failureMessage = _failureMessage;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(FunctionToTest _functionToTestToUse, ThreadParams *_threadParams, unsigned _timeoutMs, ExpectExceptions _exceptionsExpected, unsigned *_exceptionCount, const char *_failureMessage, std::chrono::milliseconds *_outDuration, const unsigned _minExpectedDuration)
    {
        assertex(_threadParams);
        assertex(_exceptionCount);
        assertex(_failureMessage);
        assertex(_outDuration);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.functionToTestToUse = _functionToTestToUse;
        attemptThreadStartParameters.threadParams = _threadParams;
        attemptThreadStartParameters.timeoutMs = _timeoutMs;
        attemptThreadStartParameters.exceptionsExpected = _exceptionsExpected;
        attemptThreadStartParameters.exceptionCount = _exceptionCount;
        attemptThreadStartParameters.failureMessage = _failureMessage;
        attemptThreadStartParameters.outDuration = _outDuration;
        attemptThreadStartParameters.minExpectedDuration = _minExpectedDuration;
        return attemptThreadStart(attemptThreadStartParameters);
    }
    bool attemptThreadStartCaller(FunctionToTest _functionToTestToUse, ThreadParams *_threadParams, ExpectExceptions _exceptionsExpected, std::chrono::milliseconds *_outDuration)
    {
        assertex(_threadParams);
        assertex(_outDuration);

        AttemptThreadStartParameters attemptThreadStartParameters;
        attemptThreadStartParameters.functionToTestToUse = _functionToTestToUse;
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
                switch (attemptThreadStartParameters.functionToTestToUse)
                {
                case FunctionToTest::StartFunction:
                    pool->start(attemptThreadStartParameters.threadParams, attemptThreadStartParameters.threadParams->name);
                    break;
                default:
                    pool->startNoBlock(attemptThreadStartParameters.threadParams);
                    break;
                }
            }
            else
            {
                switch (attemptThreadStartParameters.functionToTestToUse)
                {
                case FunctionToTest::StartFunction:
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
        constexpr const char *modes[] = {"ThreadedPersistent", "Sequential", "CThreaded", "AsyncFor", "PersistentTask", "Thread", "ManyThread"};
        DBGLOG("%s %d, %d [%u], %u", modes[mode], count, usTick() - start, (usTick() - start) / iters / 4, ret);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadedPersistStressTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadedPersistStressTest, "ThreadedPersistStressTest");

#endif // _USE_CPPUNIT
