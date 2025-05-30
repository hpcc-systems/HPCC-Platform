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
        // Not used in this test
    }

    virtual void threadmain() override
    {
        threadStartedCount++;
        DBGLOG("Thread %u started", id);

        // Wait 60 seconds
        std::this_thread::sleep_for(std::chrono::seconds(60));

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
    CPPUNIT_TEST(testAvailSemSignalingWithSecondThreadDelayAndNoStartDelays);
    CPPUNIT_TEST(testAvailSemSignalingWithFirstTwoThreadsHavingStartDelayAndThreadPoolDelay61Seconds);
    CPPUNIT_TEST(testAvailSemSignalingWithFirstTwoThreadsHavingStartDelayAndThreadPoolDelay30Seconds);
    CPPUNIT_TEST(testAvailSemSignalingWithAllThreadsHavingStartDelay);
    CPPUNIT_TEST(testAvailSemSignalingWithNoStartDelays);
    CPPUNIT_TEST(testAvailSemSignalingWithThirdThreadHavingStartDelay);
    CPPUNIT_TEST_SUITE_END();

public:
    void testAvailSemSignalingWithSecondThreadDelayAndNoStartDelays()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 10000);

        // Start first thread
        pool->start(nullptr, "Thread1");
        // Start second thread after 5 second delay
        std::this_thread::sleep_for(std::chrono::seconds(5));
        pool->start(nullptr, "Thread2");

        // Wait for both threads to start
        for (int i = 0; i < 50; i++)
        {
            if (threadStartedCount.load() >= 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Try to start a third thread asynchronously
        std::thread asyncStart([&pool]()
                               { pool->start(nullptr, "Thread3"); });
        asyncStart.detach(); // Let it run independently

        // Verify third thread didn't start immediately - give async thread time to try start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait for one of the first two threads to complete
        for (int i = 0; i < 601; i++)
        {
            if (threadCompletedCount.load() > 0)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        CPPUNIT_ASSERT(threadCompletedCount.load() == 1);

        // Now wait for the third thread to start - this verifies availsem.signal() worked
        for (int i = 0; i < 20; i++)
        {
            if (threadStartedCount.load() >= 3)
            {
                CPPUNIT_ASSERT(threadCompletedCount.load() == 1);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify the third thread started after a slot became available
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(1U, threadCompletedCount.load());

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }

    void testAvailSemSignalingWithFirstTwoThreadsHavingStartDelayAndThreadPoolDelay61Seconds()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 61000);

        // Start two threads
        std::thread asyncStart1([&pool]()
                                { pool->start(nullptr, "Thread1", 5000); });
        asyncStart1.detach(); // Let it run independently
        std::thread asyncStart2([&pool]()
                                { pool->start(nullptr, "Thread2", 5000); });
        asyncStart2.detach(); // Let it run independently

        // Wait for both threads to start
        for (int i = 0; i < 11; i++)
        {
            if (threadStartedCount.load() >= 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Try to start a third thread asynchronously
        std::thread asyncStart([&pool]()
                               { pool->start(nullptr, "Thread3"); });
        asyncStart.detach(); // Let it run independently

        // Verify third thread didn't start immediately - give async thread time to try start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait for one of the first two threads to complete
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());
        for (int i = 0; i < 601; i++)
        {
            if (threadCompletedCount.load() > 0)
                break;
            CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Now wait for the third thread to start - this verifies availsem.signal() worked
        for (int i = 0; i < 11; i++)
        {
            if (threadStartedCount.load() > 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify the third thread started after a slot became available
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }

    void testAvailSemSignalingWithFirstTwoThreadsHavingStartDelayAndThreadPoolDelay30Seconds()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 30000);

        // Start two threads
        std::thread asyncStart1([&pool]()
                                { pool->start(nullptr, "Thread1", 5000); });
        asyncStart1.detach(); // Let it run independently
        std::thread asyncStart2([&pool]()
                                { pool->start(nullptr, "Thread2", 5000); });
        asyncStart2.detach(); // Let it run independently

        // Wait for both threads to start
        for (int i = 0; i < 11; i++)
        {
            if (threadStartedCount.load() >= 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Try to start a third thread asynchronously
        std::thread asyncStart3([&pool]()
                                { pool->start(nullptr, "Thread3"); });
        asyncStart3.detach(); // Let it run independently

        // Verify third thread didn't start immediately - give async thread time to try start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait for one of the first two threads to complete
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());
        for (int i = 0; i < 601; i++)
        {
            if (threadCompletedCount.load() > 0)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Now wait for the third thread to start - this verifies availsem.signal() worked
        for (int i = 0; i < 11; i++)
        {
            if (threadStartedCount.load() > 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify the third thread started after a slot became available
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }

    void testAvailSemSignalingWithAllThreadsHavingStartDelay()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 61000);

        // Start two threads
        std::thread asyncStart1([&pool]()
                                { pool->start(nullptr, "Thread1", 5000); });
        asyncStart1.detach(); // Let it run independently
        std::thread asyncStart2([&pool]()
                                { pool->start(nullptr, "Thread2", 5000); });
        asyncStart2.detach(); // Let it run independently

        // Wait for both threads to start
        for (int i = 0; i < 11; i++)
        {
            if (threadStartedCount.load() >= 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Try to start a third thread asynchronously
        std::thread asyncStart([&pool]()
                               {
            try
            {
                pool->start(nullptr, "Thread3", 5000);
                CPPUNIT_FAIL("start should have thrown an exception");
            }
            catch(IException *e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                CPPUNIT_ASSERT_EQUAL_STR("No threads available in pool TestPool", msg.str());
                e->Release();
            } });
        asyncStart.detach(); // Let it run independently

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }

    void testAvailSemSignalingWithNoStartDelays()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 10000);

        // Start 2 threads
        pool->start(nullptr, "Thread1");
        pool->start(nullptr, "Thread2");

        // Wait for both threads to start
        for (int i = 0; i < 50; i++)
        {
            if (threadStartedCount.load() > 1)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait 30 seconds
        std::this_thread::sleep_for(std::chrono::seconds(30));

        // Try to start a third thread - it should start immediately as it has no timeout
        pool->start(nullptr, "Thread3");

        // Verify third thread started immediately as it has no timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());

        // Wait some more time and verify that the third thread starts after one of the first two completes
        for (int i = 0; i < 61; i++)
        {
            unsigned startedCount = threadStartedCount.load();
            unsigned completedCount = threadCompletedCount.load();
            // Verify that at least one thread completed before the third one started
            if (startedCount > 2)
            {
                // Third thread started before any of the first two completed as it had only the 10 second
                // thread pool delay on start
                break;
            }
            if (completedCount > 2)
                break;

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Verify three threads started and completed
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(0U, threadCompletedCount.load());

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }

    void testAvailSemSignalingWithThirdThreadHavingStartDelay()
    {
        std::atomic<unsigned> threadStartedCount(0);
        std::atomic<unsigned> threadCompletedCount(0);

        // Create thread factory
        Owned<IThreadFactory> factory = new TestThreadFactory(threadStartedCount, threadCompletedCount);

        // Create thread pool with max 2 threads and delay 10000ms
        Owned<IThreadPool> pool = createThreadPool("TestPool", factory, true, nullptr, 2, 10000);

        // Start 2 threads
        pool->start(nullptr, "Thread1");
        pool->start(nullptr, "Thread2");

        // Wait for both threads to start
        for (int i = 0; i < 50; i++)
        {
            if (threadStartedCount.load() >= 2)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Verify that both threads started
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait 30 seconds
        std::this_thread::sleep_for(std::chrono::seconds(30));

        // Try to start a third thread - it should not start immediately
        std::thread asyncStart([&pool]()
                               {
            try
            {
                pool->start(nullptr, "Thread3", 60000);
            }
            catch(IException *e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                CPPUNIT_FAIL(msg.str());
                e->Release();
            } });
        asyncStart.detach(); // Let it run independently

        // Verify third thread didn't start immediately
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CPPUNIT_ASSERT_EQUAL(2U, threadStartedCount.load());

        // Wait some more time and verify that the third thread starts after one of the first two completes
        for (int i = 0; i < 61; i++)
        {
            unsigned startedCount = threadStartedCount.load();
            unsigned completedCount = threadCompletedCount.load();
            // Verify that at no thread completed before the third one started
            if (startedCount > 2)
            {
                CPPUNIT_ASSERT_EQUAL(2U, completedCount);
                break;
            }

            if (completedCount > 2)
                break;

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Verify three threads started and completed
        CPPUNIT_ASSERT_EQUAL(3U, threadStartedCount.load());
        CPPUNIT_ASSERT_EQUAL(2U, threadCompletedCount.load());

        // Clean up - wait for all threads to complete
        pool->joinAll(true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolTest, "ThreadPoolTest");

#endif // _USE_CPPUNIT
