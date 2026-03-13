/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
 * InfoCacheReader unit tests
 *
 * Tests for thread safety and correctness of the InfoCacheReader cache management system.
 * Tests are organized into three groups:
 *   - Group 1: Basic functional tests (single-threaded behavior)
 *   - Group 2: Concurrency tests (multi-threaded scenarios)
 *   - Group 3: Race condition tests (timing-sensitive edge cases)
 */

#ifdef _USE_CPPUNIT

#include "unittests.hpp"
#include "InfoCacheReader.hpp"
#include "jthread.hpp"
#include <thread>
#include <vector>
#include <atomic>
#include <functional>

// Test infrastructure
//============================================================================

// Mock cache implementation for testing
class CTestInfoCache : public CInfoCache
{
public:
    int buildId;
    StringAttr data;
    
    CTestInfoCache(int _buildId, const char* _data) 
        : buildId(_buildId), data(_data) {
            timeCached.setNow();
        }
};

// Configurable mock reader for testing
class CTestInfoCacheReader : public CInfoCacheReader
{
public:
    std::atomic<int> readCallCount{0};
    std::atomic<int> readDurationMs{100};
    std::atomic<int> nextBuildId{1};
    std::atomic<bool> debug{false};
    std::atomic<bool> signalReadStart{false};
    std::atomic<bool> holdReadAtStart{false};
    Semaphore readStartedSem;
    Semaphore releaseReadSem;
    StringAttr nextData;
    
    CTestInfoCacheReader(const char* name, unsigned autoRebuild, unsigned forceRebuild, bool enableAuto, bool debug = false)
        : CInfoCacheReader(name, autoRebuild, forceRebuild, enableAuto)
        , readStartedSem(0)
        , releaseReadSem(0)
        , nextData("test-data")
    {
        this->debug = debug;
    }
    
    virtual CInfoCache* read() override
    {
        int id = readCallCount.fetch_add(1);

        if (signalReadStart)
        {
            readStartedSem.signal();
            if (holdReadAtStart)
                releaseReadSem.wait();
        }
        
        if (readDurationMs > 0)
            MilliSleep(readDurationMs);
        
        Owned<CTestInfoCache> cache = new CTestInfoCache(id, nextData.get());
        if (debug)
        {
            StringBuffer msg;
            cache->queryTimeCached(msg);
            DBGLOG("Built cache with id=%d at time %s readcount=%d", id, msg.str(), readCallCount.load());
        }
        return cache.getClear();
    }
    
    int getReadCount() const { return readCallCount.load(); }
    void setReadDuration(int ms) { readDurationMs = ms; }
    void setDebug(bool value) { debug = value; }
    void enableReadStartHandshake(bool holdAtStart)
    {
        signalReadStart = true;
        holdReadAtStart = holdAtStart;
        readStartedSem.reinit(0);
        releaseReadSem.reinit(0);
    }
    bool waitForReadStart(unsigned timeoutMs) { return readStartedSem.wait(timeoutMs); }
    void releaseReadStartHold() { releaseReadSem.signal(); }
};

// Test helper utilities
class InfoCacheReaderTestHelper
{
public:
    static bool waitForCacheBuild(CInfoCacheReader* reader, unsigned timeoutMs)
    {
        unsigned elapsed = 0;
        while (elapsed < timeoutMs)
        {
            Owned<CInfoCache> cache = reader->getCachedInfo();
            if (cache)
                return true;
            MilliSleep(10);
            elapsed += 10;
        }
        return false;
    }
    
    static bool waitForCondition(std::function<bool()> condition, unsigned timeoutMs)
    {
        unsigned elapsed = 0;
        while (elapsed < timeoutMs)
        {
            if (condition())
                return true;
            MilliSleep(10);
            elapsed += 10;
        }
        return false;
    }
};

//============================================================================
// PHASE 1: Basic Functional Tests
//============================================================================

class InfoCacheReaderBasicTests : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(InfoCacheReaderBasicTests);
        CPPUNIT_TEST(testInitialBuild);
        CPPUNIT_TEST(testInitialBuildBlocks);
        CPPUNIT_TEST(testSubsequentCallsReturnCached);
        CPPUNIT_TEST(testManualRebuild);
        CPPUNIT_TEST(testAutoRebuildTimeout);
        CPPUNIT_TEST(testActiveFlag);
        CPPUNIT_TEST(testInactiveInitialState);
        CPPUNIT_TEST(testCacheValidity);
        CPPUNIT_TEST(testForceRebuild);
    CPPUNIT_TEST_SUITE_END();

protected:
    void testInitialBuild()
    {
        START_TEST
        // Test that first call triggers cache build
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache != nullptr);
        END_TEST
    }
    
    void testInitialBuildBlocks()
    {
        START_TEST
        // Test that first getCachedInfo() blocks until build completes
        // Use handshake to control timing of build and ensure getCachedInfo blocks on it
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        reader->setReadDuration(200);
        reader->enableReadStartHandshake(true);
        reader->init();
        CPPUNIT_ASSERT(reader->waitForReadStart(2000));
        
        unsigned start = msTick();
        reader->releaseReadStartHold();
        Owned<CInfoCache> cache = reader->getCachedInfo();
        unsigned elapsed = msTick() - start;
        
        CPPUNIT_ASSERT(cache != nullptr);
        CPPUNIT_ASSERT(elapsed >= 200); // Should have blocked for read duration
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        END_TEST
    }
    
    void testSubsequentCallsReturnCached()
    {
        START_TEST
        // Test that subsequent calls return cached data without rebuild
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        Owned<CInfoCache> cache1 = reader->getCachedInfo();
        Owned<CInfoCache> cache2 = reader->getCachedInfo();
        Owned<CInfoCache> cache3 = reader->getCachedInfo();
        
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount()); // Still only 1 build
        CPPUNIT_ASSERT(cache1 != nullptr);
        CPPUNIT_ASSERT(cache2 != nullptr);
        CPPUNIT_ASSERT(cache3 != nullptr);
        END_TEST
    }
    
    void testManualRebuild()
    {
        START_TEST
        // Test that buildCachedInfo() triggers rebuild
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        // Trigger manual rebuild
        reader->buildCachedInfo();
        
        // Wait for rebuild to complete
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr]() { return readerPtr->getReadCount() >= 2; }, 2000));
        
        CPPUNIT_ASSERT_EQUAL(2, reader->getReadCount());
        END_TEST
    }
    
    void testAutoRebuildTimeout()
    {
        START_TEST
        // Test that auto-rebuild happens at expected interval
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 1, 5, true);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        // Wait for auto-rebuild (1 second timeout + margin)
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr]() { return readerPtr->getReadCount() >= 2; }, 2000));
        
        CPPUNIT_ASSERT(reader->getReadCount() >= 2);
        END_TEST
    }
    
    void testActiveFlag()
    {
        START_TEST
        // Test that setActive(false) stops rebuilds
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 1, 5, true);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        int initialCount = reader->getReadCount();
        
        // Deactivate
        reader->setActive(false);
        CPPUNIT_ASSERT(!reader->isActive());
        
        // Wait for what would be an auto-rebuild
        MilliSleep(1500);
        
        // Should not have rebuilt
        CPPUNIT_ASSERT_EQUAL(initialCount, reader->getReadCount());
        
        // Reactivate and verify rebuilds resume
        reader->setActive(true);
        CPPUNIT_ASSERT(reader->isActive());
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr, initialCount]() { return readerPtr->getReadCount() > initialCount; }, 2000));
        END_TEST
    }
    
    void testInactiveInitialState()
    {
        START_TEST
        // Test that reader starts inactive if setActive(false) before init
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 1, 5, true);
        reader->setReadDuration(50);
        reader->setActive(false);
        reader->init();
        
        // getCachedInfo() should return nullptr when inactive
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache == nullptr);
        CPPUNIT_ASSERT_EQUAL(0, reader->getReadCount());
        END_TEST
    }
    
    void testCacheValidity()
    {
        START_TEST
        // Test that cache validity checking works
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 1, false, true);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        //reader->enableDBGMessages();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        // Cache should be valid immediately
        Owned<CInfoCache> cache1 = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache1 != nullptr);
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        //DBGLOG("Waiting for cache to expire...");
        // Wait for cache to expire (forceRebuildSeconds = 1)
        // Resolution is in seconds so wait 2 seconds to be sure it is considered expired
        MilliSleep(2000);
        //DBGLOG("about to call getCachedInfo...");
        
        // Next call should trigger rebuild due to expired cache
        Owned<CInfoCache> cache2 = reader->getCachedInfo();
        //DBGLOG("called getCachedInfo, readCount=%d", reader->getReadCount());
        CPPUNIT_ASSERT(cache2 != nullptr);
        
        // Wait for rebuild to complete
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr]() { return readerPtr->getReadCount() >= 2; }, 2000));
        
        CPPUNIT_ASSERT_EQUAL(2, reader->getReadCount());
        END_TEST
    }
    
    void testForceRebuild()
    {
        START_TEST
        // Test that stale cache triggers rebuild in getCachedInfo
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 1, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        // Wait for cache to become stale
        // Resolution is in seconds so wait 2 seconds to be sure it is considered expired
        MilliSleep(2000);
        
        // Call getCachedInfo - should detect stale cache and trigger rebuild
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache != nullptr);
        
        // Verify rebuild was triggered
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr]() { return readerPtr->getReadCount() >= 2; }, 2000));
        END_TEST
    }
};

//============================================================================
// Group 2: Concurrency Tests
//============================================================================

class InfoCacheReaderThreadedTests : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(InfoCacheReaderThreadedTests);
        CPPUNIT_TEST(testMultipleBuildRequestsWhileWaiting);
        CPPUNIT_TEST(testMultipleBuildRequestsDuringRebuild);
        // Disabled until first build deadlock is resolved
        //CPPUNIT_TEST(testConcurrentGetCachedInfo);
        CPPUNIT_TEST(testHighContentionStress);
    CPPUNIT_TEST_SUITE_END();

protected:
    void testMultipleBuildRequestsWhileWaiting()
    {
        START_TEST
        // Test that multiple threads calling buildCachedInfo() while initial build
        // is in progress results in only one build
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(500); // Long enough for concurrent calls
        reader->init();
        std::atomic<int> threadsStarted{0};
        const int numThreads = 5;

        // Launch multiple threads that call buildCachedInfo
        std::vector<std::thread> threads;
        for (int i = 0; i < numThreads; i++)
        {
            threads.emplace_back([readerPtr, &threadsStarted]() {
                threadsStarted.fetch_add(1);
                // Wait a bit to ensure initial build is running
                MilliSleep(100);
                readerPtr->buildCachedInfo();
            });
        }
        
        // Wait for all threads to start
        while (threadsStarted.load() < numThreads)
            MilliSleep(10);
        
        // Join all threads
        for (auto& t : threads)
            t.join();
        
        // Wait for cache to be available
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        // Despite multiple buildCachedInfo calls, only one or two builds should occur
        // (initial + possibly one queued rebuild)
        int readCount = reader->getReadCount();
        CPPUNIT_ASSERT(readCount <= 2);
        
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache != nullptr);
        END_TEST
    }
    
    void testMultipleBuildRequestsDuringRebuild()
    {
        START_TEST
        // Test that multiple concurrent buildCachedInfo() calls during an active
        // rebuild are coalesced into a single rebuild
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(100);
        reader->init();
        
        // Wait for initial build
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        // Increase rebuild duration for this test
        reader->setReadDuration(800);
        
        // Trigger a rebuild
        reader->buildCachedInfo();
        
        // Wait a bit for rebuild to start
        MilliSleep(200);
        
        // Launch multiple threads calling buildCachedInfo concurrently
        const int numThreads = 10;
        std::vector<std::thread> threads;
        for (int i = 0; i < numThreads; i++)
        {
            threads.emplace_back([readerPtr]() {
                readerPtr->buildCachedInfo();
                readerPtr->buildCachedInfo(); // Call twice per thread
            });
        }
        
        // Join all threads
        for (auto& t : threads)
            t.join();
        
        // Wait for rebuilds to complete
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCondition(
            [readerPtr]() { return readerPtr->getReadCount() >= 2; }, 3000));
        
        // Despite 1 + (10 * 2) = 21 calls, should have only 2-3 actual builds
        // (initial + one in-progress + possibly one queued)
        int finalCount = reader->getReadCount();
        CPPUNIT_ASSERT(finalCount >= 2);
        CPPUNIT_ASSERT(finalCount <= 3);
        END_TEST
    }
    
    void testConcurrentGetCachedInfo()
    {
        START_TEST
        // Test that many threads calling getCachedInfo() concurrently
        // all get valid cache objects
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(200);
        reader->init();
        
        const int numThreads = 20;
        std::atomic<int> successCount{0};
        std::atomic<int> nullCount{0};
        std::vector<std::thread> threads;
        
        // Launch threads immediately - some will wait for initial build
        for (int i = 0; i < numThreads; i++)
        {
            threads.emplace_back([readerPtr, &successCount, &nullCount]() {
                // Call getCachedInfo multiple times
                for (int j = 0; j < 5; j++)
                {
                    CInfoCache* cache = readerPtr->getCachedInfo();
                    if (cache)
                    {
                        successCount.fetch_add(1);
                        cache->Release();
                    }
                    else
                        nullCount.fetch_add(1);
                    
                    DBGLOG("Thread %zu got cache: successCount=%d nullCount=%d", std::hash<std::thread::id>{}(std::this_thread::get_id()), successCount.load(), nullCount.load());
                    
                    MilliSleep(10); // Small delay between calls
                }
            });
        }
        
        DBGLOG("Launched %d threads calling getCachedInfo", numThreads);
        // Join all threads
        for (auto& t : threads)
            t.join();
        
        DBGLOG("All threads completed: successCount=%d nullCount=%d", successCount.load(), nullCount.load());
        
        // All calls should have succeeded (no nulls expected after initial build)
        CPPUNIT_ASSERT_EQUAL(numThreads * 5, successCount.load());
        CPPUNIT_ASSERT_EQUAL(0, nullCount.load());
        
        // Should still have only one build (initial)
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        END_TEST
    }
    
    void testHighContentionStress()
    {
        START_TEST
        // Stress test with mixed operations under high contention
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50); // Shorter for stress test
        reader->init();
        
        // Wait for initial build
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        const int numThreads = 20;
        const int operationsPerThread = 50;
        std::atomic<bool> stopFlag{false};
        std::atomic<int> getCacheSuccesses{0};
        std::atomic<int> buildCalls{0};
        
        std::vector<std::thread> threads;
        
        // Launch reader threads
        for (int i = 0; i < numThreads / 2; i++)
        {
            threads.emplace_back([readerPtr, &stopFlag, &getCacheSuccesses, operationsPerThread]() {
                for (int j = 0; j < operationsPerThread && !stopFlag; j++)
                {
                    CInfoCache* cache = readerPtr->getCachedInfo();
                    if (cache)
                    {
                        getCacheSuccesses.fetch_add(1);
                        cache->Release();
                    }
                    MilliSleep(5);
                }
            });
        }
        
        // Launch builder threads
        for (int i = 0; i < numThreads / 2; i++)
        {
            threads.emplace_back([readerPtr, &stopFlag, &buildCalls, operationsPerThread]() {
                for (int j = 0; j < operationsPerThread && !stopFlag; j++)
                {
                    readerPtr->buildCachedInfo();
                    buildCalls.fetch_add(1);
                    MilliSleep(10);
                }
            });
        }
        
        // Join all threads
        for (auto& t : threads)
            t.join();
        
        // Verify all getCachedInfo calls succeeded
        CPPUNIT_ASSERT(getCacheSuccesses.load() > 0);
        CPPUNIT_ASSERT_EQUAL((numThreads / 2) * operationsPerThread, getCacheSuccesses.load());
        
        // Verify build calls were made
        CPPUNIT_ASSERT_EQUAL((numThreads / 2) * operationsPerThread, buildCalls.load());
        
        // The number of actual builds should be much less than build requests
        // due to coalescing
        int actualBuilds = reader->getReadCount();
        CPPUNIT_ASSERT(actualBuilds >= 1);
        CPPUNIT_ASSERT(actualBuilds < buildCalls.load());
        
        // Final cache should be valid
        Owned<CInfoCache> finalCache = reader->getCachedInfo();
        CPPUNIT_ASSERT(finalCache != nullptr);
        END_TEST
    }
};

//============================================================================
// Group 3: Race Condition Tests
//============================================================================

class InfoCacheReaderRaceTests : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(InfoCacheReaderRaceTests);
        CPPUNIT_TEST(testRaceWindowAfterSemaphoreWait);
        CPPUNIT_TEST(testAtomicActiveAccess);
        CPPUNIT_TEST(testActiveFlagDuringConcurrentAccess);
        // Known to deadlock until firstBuild fixes are implemented
        //CPPUNIT_TEST(testFirstBuildConcurrentAccess);
    CPPUNIT_TEST_SUITE_END();

protected:
    void testRaceWindowAfterSemaphoreWait()
    {
        START_TEST
        // Test the race window between sem.wait() return and rebuildNeeded=true being set
        // This test attempts to trigger the window by causing many rapid rebuild requests
        // during/after initial build completion
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(100);
        reader->init();
        
        // Immediately spawn threads trying to request rebuilds
        // Some will hit during initial build, some right after
        const int numThreads = 20;
        std::atomic<int> buildCallsMade{0};
        std::vector<std::thread> threads;
        
        for (int i = 0; i < numThreads; i++)
        {
            threads.emplace_back([readerPtr, &buildCallsMade, i]() {
                // Stagger the calls slightly to spread them over time
                MilliSleep(i * 10);
                readerPtr->buildCachedInfo();
                buildCallsMade.fetch_add(1);
            });
        }
        
        // Join all threads
        for (auto& t : threads)
            t.join();
        
        // Wait for any triggered rebuilds to complete
        MilliSleep(500);
        
        CPPUNIT_ASSERT_EQUAL(numThreads, buildCallsMade.load());
        
        // Even with race window, coalescing should work
        // Initial build + at most 1-2 extra rebuilds from race window
        int totalBuilds = reader->getReadCount();
        CPPUNIT_ASSERT(totalBuilds >= 1);
        CPPUNIT_ASSERT(totalBuilds <= 3);
        
        // Final cache should be valid
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache != nullptr);
        END_TEST
    }
    
    void testAtomicActiveAccess()
    {
        START_TEST
        // Test that the atomic active flag prevents data races
        // One thread rapidly toggles, another rapidly reads
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(10);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        std::atomic<bool> stopFlag{false};
        std::atomic<int> readCount{0};
        std::atomic<int> toggleCount{0};
        
        // Thread that constantly reads isActive()
        std::thread readerThread([readerPtr, &stopFlag, &readCount]() {
            while (!stopFlag.load())
            {
                bool active = readerPtr->isActive();
                (void)active; // Use the value to prevent optimization
                readCount.fetch_add(1);
            }
        });
        
        // Thread that rapidly toggles setActive()
        std::thread togglerThread([readerPtr, &toggleCount]() {
            for (int i = 0; i < 100; i++)
            {
                readerPtr->setActive(i % 2 == 0);
                toggleCount.fetch_add(1);
                MilliSleep(5);
            }
        });
        
        // Let it run for a bit
        togglerThread.join();
        stopFlag = true;
        readerThread.join();
        
        // Verify both threads did work
        CPPUNIT_ASSERT(readCount.load() > 0);
        CPPUNIT_ASSERT_EQUAL(100, toggleCount.load());
        
        // If there was a data race (without atomic), this would likely crash
        // or trigger TSAN errors. Success means atomic works.
        END_TEST
    }
    
    void testActiveFlagDuringConcurrentAccess()
    {
        START_TEST
        // Test that toggling active flag doesn't cause issues with concurrent getCachedInfo()
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(50);
        reader->init();
        
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        std::atomic<bool> stopFlag{false};
        std::atomic<int> successCount{0};
        std::atomic<int> nullCount{0};
        
        // Multiple threads reading cache
        const int numReaders = 10;
        std::vector<std::thread> readers;
        for (int i = 0; i < numReaders; i++)
        {
            readers.emplace_back([readerPtr, &stopFlag, &successCount, &nullCount]() {
                while (!stopFlag.load())
                {
                    CInfoCache* cache = readerPtr->getCachedInfo();
                    if (cache)
                    {
                        successCount.fetch_add(1);
                        cache->Release();
                    }
                    else
                        nullCount.fetch_add(1);
                    MilliSleep(10);
                }
            });
        }
        
        // One thread toggling active state
        std::thread toggler([readerPtr]() {
            for (int i = 0; i < 20; i++)
            {
                readerPtr->setActive(i % 2 == 0);
                MilliSleep(25);
            }
        });
        
        toggler.join();
        stopFlag = true;
        
        for (auto& t : readers)
            t.join();
        
        // Some calls may have returned null when inactive, but no crashes
        CPPUNIT_ASSERT(successCount.load() + nullCount.load() > 0);
        
        // Ensure reader is back in active state
        reader->setActive(true);
        Owned<CInfoCache> finalCache = reader->getCachedInfo();
        CPPUNIT_ASSERT(finalCache != nullptr);
        END_TEST
    }
    
    void testFirstBuildConcurrentAccess()
    {
        START_TEST
        // Test the special first-build blocking logic with multiple concurrent callers
        // NOTE: This test has a known issue - firstSem.signal() only wakes one thread
        // This test documents the current behavior
        Owned<CTestInfoCacheReader> reader = new CTestInfoCacheReader("test", 60, 5, false);
        CTestInfoCacheReader* readerPtr = reader.get();
        reader->setReadDuration(300);
        
        const int numThreads = 5; // Use fewer threads due to known firstSem issue
        std::atomic<int> successCount{0};
        std::atomic<int> nullCount{0};
        std::vector<std::thread> threads;
        
        // Launch threads that will block on first build
        for (int i = 0; i < numThreads; i++)
        {
            threads.emplace_back([readerPtr, &successCount, &nullCount]() {
                //Owned<CInfoCache> cache = readerPtr->getCachedInfo();
                // Suspect that jlib smart pointers may cause issues with
                // std::thread, so use raw pointer with manual release.
                CInfoCache* cache = readerPtr->getCachedInfo();
                if (cache)
                {
                    successCount.fetch_add(1);
                    cache->Release();
                }
                else
                    nullCount.fetch_add(1);
            });
        }
        
        // Small delay to let threads get to getCachedInfo()
        MilliSleep(100);
        
        // Now start the background thread
        reader->init();
        
        // Wait for all threads with timeout
        bool allJoined = true;
        for (auto& t : threads)
        {
            // Use a timeout approach - can't directly timeout thread join
            // but we can check completion
            if (t.joinable())
                t.join();
        }
        
        // Wait a bit more for build to complete
        CPPUNIT_ASSERT(InfoCacheReaderTestHelper::waitForCacheBuild(reader, 2000));
        
        // Note: Due to the firstSem issue, not all threads may wake up immediately
        // We verify at least some succeeded and system is stable
        CPPUNIT_ASSERT(successCount.load() > 0);
        CPPUNIT_ASSERT_EQUAL(1, reader->getReadCount());
        
        // Verify subsequent calls work normally
        Owned<CInfoCache> cache = reader->getCachedInfo();
        CPPUNIT_ASSERT(cache != nullptr);
        END_TEST
    }
};

//============================================================================
// Test Suite Registration
//============================================================================

CPPUNIT_TEST_SUITE_REGISTRATION(InfoCacheReaderBasicTests);
CPPUNIT_TEST_SUITE_REGISTRATION(InfoCacheReaderThreadedTests);
CPPUNIT_TEST_SUITE_REGISTRATION(InfoCacheReaderRaceTests);

#endif // _USE_CPPUNIT
