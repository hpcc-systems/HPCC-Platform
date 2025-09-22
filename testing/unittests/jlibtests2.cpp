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

/*
 * Jlib regression tests
 *
 */

#ifdef _USE_CPPUNIT
#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <iostream>
#include <random>
#include <set>
#include <vector>

#include "jsem.hpp"
#include "jfile.hpp"
#include "jevent.hpp"
#include "eventdump.h"
#include "jthread.hpp"
#include "unittests.hpp"


enum { NodeBranch, NodeLeaf };

class CNoOpEventVisitorDecorator : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return visitor->visitFile(filename, version);
    }
    virtual bool visitEvent(CEvent& event) override
    {
        return visitor->visitEvent(event);
    }
    virtual void departFile(uint32_t bytesRead) override
    {
        return visitor->departFile(bytesRead);
    }
protected:
    Linked<IEventVisitor> visitor;
public:
    CNoOpEventVisitorDecorator(IEventVisitor& _visitor) : visitor(&_visitor) {}
};

class MockEventVisitor : public CNoOpEventVisitorDecorator
{
public:
    virtual bool visitEvent(CEvent& event) override
    {
        // Timestamps and thread ID are not predictable. Hard code predictable values.
        if (event.hasAttribute(EvAttrEventTimestamp))
        {
            CDateTime dt;
            dt.setString("2025-05-08T00:00:00.000001010");
            event.setValue(EvAttrEventTimestamp, dt.getTimeStampNs());
        }
        if (event.hasAttribute(EvAttrEventThreadId))
            event.setValue(EvAttrEventThreadId, 100ULL);
        return CNoOpEventVisitorDecorator::visitEvent(event);
    }
public:
    using CNoOpEventVisitorDecorator::CNoOpEventVisitorDecorator;
};

static void removeFile(const char * filename)
{
    Owned<IFile> file = createIFile(filename);
    file->remove();
}

class JlibEventTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibEventTest);
        CPPUNIT_TEST(testEventTracing);
        CPPUNIT_TEST(testMultiBlock);
        CPPUNIT_TEST(testMultiThread);
        CPPUNIT_TEST(testBlocked);
        CPPUNIT_TEST(testReadEvents);
        CPPUNIT_TEST(testIterateAllAttributes);
        CPPUNIT_TEST(testIterateEventAttributes);
        CPPUNIT_TEST(testFailCreate);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();

    static constexpr bool cleanup = true;
    static constexpr unsigned nodeSize=8192;

    void testEventTracing()
    {
        constexpr unsigned branchOffset=32*8192;
        try
        {
            EventRecorder &recorder = queryRecorder();
            EventRecordingSummary summary;

            // Test that recording is initially inactive
            CPPUNIT_ASSERT(!recorder.isRecording());

            // Start recording
            CPPUNIT_ASSERT(recorder.startRecording("traceid", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());

            //Check that overlapping starts fail
            CPPUNIT_ASSERT(!recorder.startRecording("traceid", "eventtrace.evtxxx", false));

            // Record some events
            recorder.recordIndexCacheHit(1, branchOffset, NodeBranch, 9876, 400);
            recorder.recordIndexCacheMiss(1, nodeSize, NodeLeaf);
            recorder.recordIndexLoad(1, nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            recorder.recordIndexEviction(1, branchOffset, NodeBranch, nodeSize);

            // Stop recording
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(4U, summary.numEvents);

            // Check that stopping again fails
            CPPUNIT_ASSERT(!recorder.stopRecording(nullptr));

            // Restart recording with a different filename
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "testfile.bin", true));
            CPPUNIT_ASSERT(!recorder.isRecording());
            recorder.pauseRecording(true, false);
            CPPUNIT_ASSERT(!recorder.isRecording());

            //These should be ignored - count checked later on
            recorder.recordIndexCacheMiss(2, 400, NodeLeaf);
            recorder.recordIndexCacheMiss(1, 800, NodeLeaf);

            recorder.pauseRecording(false, true);
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record more events
            recorder.recordIndexCacheMiss(2, 400, NodeLeaf);
            recorder.recordIndexCacheMiss(1, 800, NodeLeaf);
            recorder.recordIndexLoad(2, 500, NodeLeaf, 2048, 600, 400);
            recorder.recordIndexLoad(1, 800, NodeLeaf, 2048, 600, 400);
            recorder.recordIndexCacheHit(1, 800, NodeLeaf, 2048, 600);
            recorder.recordIndexCacheMiss(1, 1200, NodeLeaf);
            recorder.recordIndexEviction(2, 500, NodeLeaf, 2048);
            recorder.recordIndexLoad(1, 1200, NodeLeaf, 2048, 600, 400);

            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 987, 100, 67);

            // Stop recording again
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(10U, summary.numEvents);        // One pause + 8 index, 2 dali, not the two logged when paused.
            CPPUNIT_ASSERT_EQUAL_STR("testfile.bin", summary.filename);        // One pause + 8 index, 2 dali, not the two logged when paused.
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    using ExpectedCounts = std::initializer_list<std::pair<EventType, __uint64>>;
    void verifyCounts(const char * filename, const ExpectedCounts & expected)
    {
        class EventCounter final : implements CInterfaceOf<IEventVisitor>
        {
        public:
            virtual bool visitEvent(CEvent& event) override
            {
                counts[event.queryType()]++;
                return true;
            }
            virtual bool visitFile(const char* filename, uint32_t version) override
            {
                return true;
            }
            virtual void departFile(uint32_t bytesRead) override
            {
            }
        public:
            __uint64 counts[EventMax] = { };
        };

        try
        {
            EventCounter counter;
            CPPUNIT_ASSERT(readEvents(filename, counter));

            for (auto & [type, expectedCount] : expected)
            {
                CPPUNIT_ASSERT_EQUAL(expectedCount, counter.counts[type]);
            }
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    void testMultiBlock()
    {
        //Add a test to write more than one block of data
        try
        {
            EventRecorder &recorder = queryRecorder();

            // Test that recording is initially inactive
            CPPUNIT_ASSERT(!recorder.isRecording());

            // Start recording
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record some events
            for (unsigned i=0; i < 100'000; i++)
            {
                recorder.recordIndexCacheMiss(1, i*nodeSize, NodeLeaf);
                recorder.recordIndexLoad(1, i*nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            }

            // Stop recording
            EventRecordingSummary summary;
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT(summary.valid);
            CPPUNIT_ASSERT_EQUAL(200'000U, summary.numEvents);

            verifyCounts("eventtrace.evt", {
                { EventIndexCacheMiss, 100'000 },
                { EventIndexLoad, 100'000 }
            });
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    class EventReporterThread : public Thread
    {
    public:
        EventReporterThread(unsigned _id, unsigned _count) : id(_id), count(_count) {}

        virtual int run()
        {
            EventRecorder &recorder = queryRecorder();
            for (unsigned i=0; i < count; i++)
            {
                recorder.recordIndexCacheMiss(id, i*nodeSize, NodeLeaf);
                recorder.recordIndexLoad(id, i*nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            }
            return 0;
        }

    private:
        unsigned id;
        unsigned count;
    };

    void testMultiThread(unsigned delay, unsigned numThreads)
    {
        //Add a test to write more than one block of data
        try
        {
            EventRecorder &recorder = queryRecorder();

            // Test that recording is initially inactive
            CPPUNIT_ASSERT(!recorder.isRecording());

            // Start recording
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());

            CIArrayOf<Thread> threads;
            for (unsigned t=0; t < numThreads; t++)
            {
                threads.append(*new EventReporterThread(t, 200'000 / numThreads));
                threads.item(t).start(true);
            }

            EventRecordingSummary summary;
            if (delay)
            {
                MilliSleep(delay);
                CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            }

            ForEachItemIn(t2, threads)
                threads.item(t2).join();

            if (!delay)
                CPPUNIT_ASSERT(recorder.stopRecording(&summary));

            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT(summary.valid);

            //The counts are only valid if we waited for the threads to finish
            if (!delay)
                verifyCounts("eventtrace.evt", { { EventIndexCacheMiss, 200'000 }, { EventIndexLoad, 200'000 } });
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }

    }

    void testMultiThread()
    {
        //Multiple threads, and writing more than one block of data
        testMultiThread(0, 2);

        //Multiple threads, stop recording while the threads are reporting
        testMultiThread(100, 8);
    }

    void testBlocked()
    {
        //Test that the recording is blocked/events are dropped when the buffer is full
        //Add a special event function in the public interface which sleeps for a given time
    }

    void testFailCreate()
    {
        EventRecorder &recorder = queryRecorder();

        // Test that recording is initially inactive
        CPPUNIT_ASSERT(!recorder.isRecording());

        // Try start recording to an invalid filename
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(recorder.startRecording("threadid", "/home/nonexistantuser/eventtrace.evt", false), "Expected startRecording() to throw an exception");

        //Check that the recorder has been left in a good state, and a subsequent recording works
        CPPUNIT_ASSERT(!recorder.isRecording());
        CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", false));
        CPPUNIT_ASSERT(recorder.isRecording());
        CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
    }

    void testCleanup()
    {
        if (cleanup)
        {
            removeFile("eventtrace.evt");
            removeFile("testfile.bin");
        }
    }

    void testReadEvents()
    {
        EventRecordingSummary summary;
        //Test reading an empty file
        try
        {
            static const char* expect="";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true,compress(0)", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            StringBuffer out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL_STR(expect, out.str());
            DBGLOG("Raw size = %llu, File size = %llu", summary.rawSize, summary.totalSize);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        try
        {
            static const char* expect = R"!!!(event: IndexEviction
attribute: EventTimestamp = '2025-05-08T00:00:00.000001010'
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: InMemorySize = 4567
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            StringBuffer out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL_STR(expect, out.str());
            DBGLOG("Raw size = %llu, File size = %llu", summary.rawSize, summary.totalSize);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
        try
        {
            static const char* expect = R"!!!(event: IndexEviction
attribute: EventTimestamp = '2025-05-08T00:00:00.000001010'
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: InMemorySize = 4567
event: DaliConnect
attribute: EventTimestamp = '2025-05-08T00:00:00.000001010'
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: Path = '/Workunits/Workunit/abc.wu'
attribute: ConnectId = 98765
attribute: ElapsedTime = 100
attribute: DataSize = 73
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all,compress(lz4hc)", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 98765, 100, 73);
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            StringBuffer out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL_STR(expect, out.str());
            DBGLOG("Raw size = %llu, File size = %llu", summary.rawSize, summary.totalSize);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testIterateAllAttributes()
    {
        std::set<unsigned> expectedDefined({EvAttrEventTimestamp, EvAttrEventTraceId, EvAttrEventThreadId, EvAttrEventStackTrace, EvAttrEnabled});
        std::set<unsigned> expectedAssigned;
        std::set<unsigned> actualDefined;
        std::set<unsigned> actualAssigned;
        size_t actualUnusedCount = 0;
        CEvent evt;
        evt.reset(EventRecordingActive);
        for (auto& attr : evt.allAttributes)
        {
            switch (attr.queryState())
            {
            case CEventAttribute::State::Defined:
                actualDefined.insert(attr.queryId());
                break;
            case CEventAttribute::State::Assigned:
                actualAssigned.insert(attr.queryId());
                break;
            case CEventAttribute::State::Unused:
                actualUnusedCount++;
                break;
            }
        }
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedDefined), containerToString(actualDefined));
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedAssigned), containerToString(actualAssigned));
        CPPUNIT_ASSERT_GREATER(size_t(0), actualUnusedCount); // can't hard-code this without breaking with each added attribute
    }

    void testIterateEventAttributes()
    {
        std::vector<unsigned> expectedDefined({EvAttrEventTimestamp, EvAttrEventTraceId, EvAttrEventThreadId, EvAttrEventStackTrace, EvAttrEnabled});
        std::vector<unsigned> expectedAssigned;
        std::vector<unsigned> actualDefined;
        std::vector<unsigned> actualAssigned;
        size_t actualUnusedCount = 0;
        CEvent evt;
        evt.reset(EventRecordingActive);
        for (auto& attr : evt.definedAttributes)
        {
            switch (attr.queryState())
            {
            case CEventAttribute::State::Defined:
                actualDefined.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Assigned:
                actualAssigned.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Unused:
                actualUnusedCount++;
                break;
            }
        }
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedDefined), containerToString(actualDefined));
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedAssigned), containerToString(actualAssigned));
        CPPUNIT_ASSERT_EQUAL(size_t(0), actualUnusedCount);
        actualDefined.clear();
        actualUnusedCount = 0;
        evt.setValue(EvAttrEnabled, true);
        expectedAssigned.push_back(EvAttrEnabled);
        expectedDefined.erase(std::find(expectedDefined.begin(), expectedDefined.end(), EvAttrEnabled));
        for (auto& attr : evt.definedAttributes)
        {
            switch (attr.queryState())
            {
            case CEventAttribute::State::Defined:
                actualDefined.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Assigned:
                actualAssigned.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Unused:
                actualUnusedCount++;
                break;
            }
        }
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedDefined), containerToString(actualDefined));
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedAssigned), containerToString(actualAssigned));
        CPPUNIT_ASSERT_EQUAL(size_t(0), actualUnusedCount);
        actualDefined.clear();
        actualAssigned.clear();
        actualUnusedCount = 0;
        expectedDefined.clear();
        for (auto& attr : evt.assignedAttributes)
        {
            switch (attr.queryState())
            {
            case CEventAttribute::State::Defined:
                actualDefined.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Assigned:
                actualAssigned.push_back(attr.queryId());
                break;
            case CEventAttribute::State::Unused:
                actualUnusedCount++;
                break;
            }
        }
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedDefined), containerToString(actualDefined));
        CPPUNIT_ASSERT_EQUAL(containerToString(expectedAssigned), containerToString(actualAssigned));
        CPPUNIT_ASSERT_EQUAL(size_t(0), actualUnusedCount);
    }

    IEventVisitor* createVisitor(StringBuffer& out)
    {
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        Owned<IEventVisitor> visitor = createDumpTextEventVisitor(*stream);
        return new MockEventVisitor(*visitor);
    }

    template <typename container_type_t>
    std::string containerToString(const container_type_t& container)
    {
        bool first = true;
        std::stringstream ss;
        ss << '[';
        for (const auto& item : container)
        {
            if (!first)
                ss << ", ";
            else
                first = false;
            ss << item;
        }
        ss << ']';
        return ss.str();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibEventTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibEventTest, "JlibEventTest" );

//---------------------------------------------------------------------------------------------------------------------

class BufferedSerialOutputStreamTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BufferedSerialOutputStreamTest);
    CPPUNIT_TEST(testPut);
    CPPUNIT_TEST(testSuspendResume);
    CPPUNIT_TEST_SUITE_END();

public:
    void testPut()
    {
        try
        {
            StringBuffer result;
            Owned<IBufferedSerialOutputStream> outputStream = createBufferedSerialOutputStream(result);
            CPPUNIT_ASSERT_EQUAL(0ULL, outputStream->tell());

            outputStream->put(10, "abcdefghij");
            CPPUNIT_ASSERT_EQUAL(10ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abcdefghij", result);

            outputStream->put(10, "0123456789");
            CPPUNIT_ASSERT_EQUAL(20ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abcdefghij0123456789", result);

            size32_t got;
            byte * buffer = outputStream->reserve(5U, got);
            CPPUNIT_ASSERT_EQUAL(20ULL, outputStream->tell());
            CPPUNIT_ASSERT(got >= 5);
            memset(buffer, '!', 4);
            outputStream->commit(4);
            CPPUNIT_ASSERT_EQUAL(24ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abcdefghij0123456789!!!!", result);

            result.clear();
            CPPUNIT_ASSERT_EQUAL(0ULL, outputStream->tell());

            outputStream->put(3, "abc");
            CPPUNIT_ASSERT_EQUAL(3ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abc", result);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testSuspendResume()
    {
        try
        {
            StringBuffer result;
            Owned<IBufferedSerialOutputStream> outputStream = createBufferedSerialOutputStream(result);

            outputStream->put(3, "abc");
            CPPUNIT_ASSERT_EQUAL(3ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abc", result);

            outputStream->suspend(1);       // abc_
            CPPUNIT_ASSERT_EQUAL(4ULL, outputStream->tell());

            outputStream->put(3, "123");    // abc_123
            CPPUNIT_ASSERT_EQUAL(7ULL, outputStream->tell());

            outputStream->suspend(4);       // abc_123____
            CPPUNIT_ASSERT_EQUAL(11ULL, outputStream->tell());

            outputStream->put(3, "XYZ");    // abc_123____XYZ
            CPPUNIT_ASSERT_EQUAL(14ULL, outputStream->tell());

            outputStream->resume(4, "!!!!");// abc_123!!!!XYZ
            CPPUNIT_ASSERT_EQUAL(14ULL, outputStream->tell());

            outputStream->put(3, "123");    // abc_123!!!!XYZ123
            CPPUNIT_ASSERT_EQUAL(17ULL, outputStream->tell());

            outputStream->suspend(2);       // abc_123!!!!XYZ123__
            CPPUNIT_ASSERT_EQUAL(19ULL, outputStream->tell());

            outputStream->put(3, "<=>");    // abc_123!!!!XYZ123__<=>
            CPPUNIT_ASSERT_EQUAL(22ULL, outputStream->tell());

            outputStream->resume(2, "%%");  // abc_123!!!!XYZ123%%<=>
            CPPUNIT_ASSERT_EQUAL(22ULL, outputStream->tell());

            outputStream->resume(1, "+");   // abc+123!!!!XYZ123%%<=>
            CPPUNIT_ASSERT_EQUAL(22ULL, outputStream->tell());
            CPPUNIT_ASSERT_EQUAL_STR("abc+123!!!!XYZ123%%<=>", result);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(BufferedSerialOutputStreamTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(BufferedSerialOutputStreamTest, "BufferedSerialOutputStreamTest");

class ThreadPoolSizeTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ThreadPoolSizeTest);
      CPPUNIT_TEST(testThreadPoolResizing);
    CPPUNIT_TEST_SUITE_END();

    class TestWorkerThread : public CSimpleInterfaceOf<IPooledThread>
    {
    private:
        Semaphore *startSem = nullptr;
        Semaphore *stopSem = nullptr;
        std::atomic<unsigned> *startCount = nullptr;
        std::atomic<unsigned> *stopCount = nullptr;

    public:
        virtual void init(void *param) override
        {
            // param is an array: [startSem, stopSem, startCount, stopCount]
            void **params = static_cast<void**>(param);
            startSem = static_cast<Semaphore*>(params[0]);
            stopSem = static_cast<Semaphore*>(params[1]);
            startCount = static_cast<std::atomic<unsigned>*>(params[2]);
            stopCount = static_cast<std::atomic<unsigned>*>(params[3]);
        }
        virtual void threadmain() override
        {
            (*startCount)++;
            startSem->signal(); // Signal that a thread has started

            stopSem->wait(); // Wait for stop signal

            (*stopCount)++;
        }
        virtual bool stop() override
        {
            stopSem->signal();
            return true;
        }
        virtual bool canReuse() const override { return true; }
    };

public:
    void testThreadPoolResizing()
    {
        try
        {
            // Create thread pool with small initial size
            const unsigned initialPoolSize = 4;
            const unsigned delay = 100; // 100ms delay to create burst conditions
            constexpr unsigned timingToleranceMs = 50; // Timing tolerance for all assertions

            class TestThreadFactory : public CInterfaceOf<IThreadFactory>
            {
            public:
                IPooledThread *createNew() override
                {
                    return new TestWorkerThread();
                }
            };

            Owned<IThreadFactory> factory = new TestThreadFactory();
            Owned<IThreadPool> pool = createThreadPool(
                "TestPool",
                factory,
                false,              // inheritThreadContext
                nullptr,            // exceptionHandler
                initialPoolSize,    // defaultmax
                delay               // delay - important for burst behavior
            );

            // Test: Resize when pool is idle
            pool->setPoolSize(8, 0);  // 0 means targetpoolsize defaults to newPoolSize
            pool->setPoolSize(2, 0);  // Set pool size to 2 to create burst conditions

            // Set up shared semaphores and counters
            Semaphore startSem, stopSem;
            std::atomic<unsigned> startCount{0};
            std::atomic<unsigned> stopCount{0};
            void *params[] = {&startSem, &stopSem, &startCount, &stopCount};

            // Test: Start threads up to pool size (should start immediately)
            CCycleTimer timer;
            std::vector<PooledThreadHandle> handles;
            handles.push_back(pool->start(params, "Thread1"));
            handles.push_back(pool->start(params, "Thread2"));

            for (unsigned i = 0; i < 2; i++)
                startSem.wait();

            unsigned elapsedMs = timer.elapsedMs();
            CPPUNIT_ASSERT_EQUAL(2U, startCount.load());
            CPPUNIT_ASSERT_EQUAL(2U, pool->runningCount());

            // Verify that threads within pool size started quickly (no delay)
            CPPUNIT_ASSERT(elapsedMs < timingToleranceMs); // Should start within tolerance

            // Test: Start threads that will become burst threads (exceed pool size limit)
            // Each thread beyond capacity is delayed by the full delay time
            // 4 threads × 100ms delay = ~400ms total
            CCycleTimer burstTimer;
            for (unsigned i = 0; i < 4; i++)
                handles.push_back(pool->start(params, "BurstThread"));

            // Wait for all burst threads to start (after cumulative delays)
            for (unsigned i = 0; i < 4; i++)
                startSem.wait();

            unsigned burstElapsedMs = burstTimer.elapsedMs();
            CPPUNIT_ASSERT_EQUAL(6U, startCount.load());
            CPPUNIT_ASSERT_EQUAL(6U, pool->runningCount());

            // Verify burst threads were properly throttled
            const unsigned expectedMinDelay = 4 * delay; // 4 burst threads × delay
            CPPUNIT_ASSERT(burstElapsedMs >= expectedMinDelay);
            CPPUNIT_ASSERT(burstElapsedMs < (expectedMinDelay + timingToleranceMs));

            // Test: Resize pool up while burst threads are running
            pool->setPoolSize(10, 0);
            CPPUNIT_ASSERT_EQUAL(6U, pool->runningCount());

            // Test: Start more threads after resize - should start immediately now
            CCycleTimer postResizeTimer;
            handles.push_back(pool->start(params, "PostResize"));
            startSem.wait();
            unsigned postResizeMs = postResizeTimer.elapsedMs();

            CPPUNIT_ASSERT_EQUAL(7U, startCount.load());
            CPPUNIT_ASSERT_EQUAL(7U, pool->runningCount());

            // Verify post-resize thread started quickly (no throttling with larger pool)
            CPPUNIT_ASSERT(postResizeMs < timingToleranceMs);

            // Test 6: Resize pool down while many threads are running
            pool->setPoolSize(5, 0);
            CPPUNIT_ASSERT_EQUAL(7U, pool->runningCount());

            // Test: Stop some threads and verify counts
            for (unsigned i = 0; i < 3; i++)
                stopSem.signal();

            for (unsigned i = 0; i < 3; i++)
                pool->join(handles[i], 1000);

            CPPUNIT_ASSERT_EQUAL(3U, stopCount.load());
            CPPUNIT_ASSERT_EQUAL(4U, pool->runningCount());

            // Test: Start new threads with reduced pool size
            // Pool size is now 5, but 4 threads are running, so 1 slot available
            // First new thread should start quickly, second should be delayed
            CCycleTimer newThreadTimer;
            for (unsigned i = 0; i < 2; i++)
                handles.push_back(pool->start(params, "NewThread"));

            for (unsigned i = 0; i < 2; i++)
                startSem.wait();
            unsigned newThreadMs = newThreadTimer.elapsedMs();

            CPPUNIT_ASSERT_EQUAL(9U, startCount.load());
            CPPUNIT_ASSERT_EQUAL(6U, pool->runningCount());

            // Verify throttling: 1 thread immediate + 1 thread delayed by 100ms
            CPPUNIT_ASSERT(newThreadMs >= delay); // Cannot be faster than 1×delay for the delayed thread
            CPPUNIT_ASSERT(newThreadMs < delay + timingToleranceMs);  // But allow tolerance

            // Test: Clean shutdown - stop all remaining threads
            unsigned remainingThreads = pool->runningCount();
            for (unsigned i = 0; i < remainingThreads; i++)
                stopSem.signal();
            pool->setPoolSize(1, 0); // reducing targetpoolsize, should free all but 1 active thread

            pool->joinAll(true, 2000);
            CPPUNIT_ASSERT_EQUAL(0U, pool->runningCount());
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ThreadPoolSizeTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ThreadPoolSizeTest, "ThreadPoolSizeTest");

//--------------------------------------------------------------------------------------------------

#include "jregexp.hpp"
#include <regex>

class RegExprTest : public CppUnit::TestFixture
{
    static constexpr const char * archivePatternJlib = "[.]{zip|tar|tar[.]gz|tgz}{/|\\\\}";
    static constexpr const char * archivePatternStdlib = "[.](zip|tar|tar[.]gz|tgz)(/|\\\\)";

    CPPUNIT_TEST_SUITE(RegExprTest);
    CPPUNIT_TEST(testRegExprMatches);
    CPPUNIT_TEST(testStdRegexMatches);
    CPPUNIT_TEST_SUITE_END();

public:
    const char *splitNameStdlib(const char *fileName)
    {
        std::regex archiveSignatureRegex(archivePatternStdlib);
        std::cmatch match;
        if (std::regex_search(fileName, match, archiveSignatureRegex))
        {
            // return the text that follows the match
            return match[0].second;
        }
        else
            return nullptr;
    }

    const char *splitNameJlib(const char *fileName)
    {
        RegExpr archiveSignature(archivePatternJlib);
        const char *sig = archiveSignature.find(fileName);
        if (sig)
            return sig+archiveSignature.findlen();
        else
            return NULL;
    }

    void testRegExprMatches()
    {
        // Pattern: "[.]{zip|tar|tar[.]gz|tgz}{/|\\}"
        RegExpr expr(archivePatternJlib);

        // Should match
        CPPUNIT_ASSERT(expr.find(".zip/"));
        CPPUNIT_ASSERT(expr.find(".zip\\"));
        CPPUNIT_ASSERT(expr.find(".tar/"));
        CPPUNIT_ASSERT(expr.find(".tar\\"));
        CPPUNIT_ASSERT(expr.find(".tar.gz/"));
        CPPUNIT_ASSERT(expr.find(".tar.gz\\"));
        CPPUNIT_ASSERT(expr.find(".tgz/"));
        CPPUNIT_ASSERT(expr.find(".tgz\\"));

        // Should not match (wrong extension)
        CPPUNIT_ASSERT(!expr.find(".rar/"));
        CPPUNIT_ASSERT(!expr.find(".zipx/"));
        CPPUNIT_ASSERT(!expr.find(".tarx/"));
        CPPUNIT_ASSERT(!expr.find(".tar.gzx/"));
        CPPUNIT_ASSERT(!expr.find(".tgzx/"));

        // Should not match (missing trailing / or \\)
        CPPUNIT_ASSERT(!expr.find(".zip"));
        CPPUNIT_ASSERT(!expr.find(".tar"));
        CPPUNIT_ASSERT(!expr.find(".tar.gz"));
        CPPUNIT_ASSERT(!expr.find(".tgz"));

        // Should match (extra characters after trailing)
        CPPUNIT_ASSERT(expr.find(".zip//"));
        CPPUNIT_ASSERT(expr.find(".tar\\abc"));
        CPPUNIT_ASSERT(expr.find(".tar.gz/abc"));
        CPPUNIT_ASSERT(expr.find(".tgz\\abc"));

        // Should match (extra characters before trailing)
        CPPUNIT_ASSERT(expr.find("abc.zip//"));
        CPPUNIT_ASSERT(expr.find("blah.zip.x.tar\\abc"));
        CPPUNIT_ASSERT(expr.find("azurefile:blah@zz.tar.gz/abc"));

        // Should not match (missing dot)
        CPPUNIT_ASSERT(!expr.find("zip/"));
        CPPUNIT_ASSERT(!expr.find("tar/"));
        CPPUNIT_ASSERT(!expr.find("tar.gz/"));
        CPPUNIT_ASSERT(!expr.find("tgz/"));

        CPPUNIT_ASSERT_EQUAL_STR("abc", splitNameJlib("azurefile:blah@zz.tar.gz/abc"));
        CPPUNIT_ASSERT_EQUAL_STR("abc.zip/xyz", splitNameJlib("azurefile:blah@zz.tar.gz/abc.zip/xyz"));
    }

    void testStdRegexMatches()
    {
        // Equivalent std::regex pattern: R"(\.(zip|tar|tar\.gz|tgz)[/\\])"
        std::regex re(archivePatternStdlib);

        // Should match
        CPPUNIT_ASSERT(std::regex_search(".zip/", re));
        CPPUNIT_ASSERT(std::regex_search(".zip\\", re));
        CPPUNIT_ASSERT(std::regex_search(".tar/", re));
        CPPUNIT_ASSERT(std::regex_search(".tar\\", re));
        CPPUNIT_ASSERT(std::regex_search(".tar.gz/", re));
        CPPUNIT_ASSERT(std::regex_search(".tar.gz\\", re));
        CPPUNIT_ASSERT(std::regex_search(".tgz/", re));
        CPPUNIT_ASSERT(std::regex_search(".tgz\\", re));

        // Should not match (wrong extension)
        CPPUNIT_ASSERT(!std::regex_search(".rar/", re));
        CPPUNIT_ASSERT(!std::regex_search(".zipx/", re));
        CPPUNIT_ASSERT(!std::regex_search(".tarx/", re));
        CPPUNIT_ASSERT(!std::regex_search(".tar.gzx/", re));
        CPPUNIT_ASSERT(!std::regex_search(".tgzx/", re));

        // Should not match (missing trailing / or \\)
        CPPUNIT_ASSERT(!std::regex_search(".zip", re));
        CPPUNIT_ASSERT(!std::regex_search(".tar", re));
        CPPUNIT_ASSERT(!std::regex_search(".tar.gz", re));
        CPPUNIT_ASSERT(!std::regex_search(".tgz", re));

        // Should match (extra characters after trailing)
        CPPUNIT_ASSERT(std::regex_search(".zip//", re));
        CPPUNIT_ASSERT(std::regex_search(".tar\\abc", re));
        CPPUNIT_ASSERT(std::regex_search(".tar.gz/abc", re));
        CPPUNIT_ASSERT(std::regex_search(".tgz\\abc", re));

        // Should match (extra characters before trailing)
        CPPUNIT_ASSERT(std::regex_search("abc.zip//", re));
        CPPUNIT_ASSERT(std::regex_search("blah.zip.x.tar\\abc", re));
        CPPUNIT_ASSERT(std::regex_search("azurefile:blah@zz.tar.gz/abc", re));

        // Should not match (missing dot)
        CPPUNIT_ASSERT(!std::regex_search("zip/", re));
        CPPUNIT_ASSERT(!std::regex_search("tar/", re));
        CPPUNIT_ASSERT(!std::regex_search("tar.gz/", re));
        CPPUNIT_ASSERT(!std::regex_search("tgz/", re));

        CPPUNIT_ASSERT_EQUAL_STR("abc", splitNameStdlib("azurefile:blah@zz.tar.gz/abc"));
        CPPUNIT_ASSERT_EQUAL_STR("abc.zip/xyz", splitNameStdlib("azurefile:blah@zz.tar.gz/abc.zip/xyz"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(RegExprTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(RegExprTest, "RegExprTest");

//--------------------------------------------------------------------------------------------------

#include "jiouring.hpp"
#include <regex>

class IOURingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(IOURingTest);
        CPPUNIT_TEST(testcallback);
        CPPUNIT_TEST(testcallbackNoThread);
        CPPUNIT_TEST(testcallback2NoThread);
        CPPUNIT_TEST(testcallbacks);
    CPPUNIT_TEST_SUITE_END();

    class SemCallback final : public IAsyncCallback
    {
    public:
        virtual void onAsyncComplete(int result) override
        {
            sem.signal();
        };

    public:
        Semaphore sem;
    };

public:
    void testcallback()
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createUring(config, true);
        if (!processor)
            return;

        SemCallback hello;
        processor->enqueueCallbackCommand(hello);
        hello.sem.wait();
    }

    void testcallbackNoThread()
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createUring(config, false);
        if (!processor)
            return;

        SemCallback hello;
        processor->enqueueCallbackCommand(hello);
        while (!hello.sem.wait(0))
            processor->checkForCompletions();
    }

    void testcallback2NoThread()
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createUring(config, false);
        if (!processor)
            return;

        // Test the non threaded uring processor - which only checks for completion when new events are submitted
        // Because no-op operations are processed immediately this test is not valid
        SemCallback action1;
        SemCallback action2;
        processor->enqueueCallbackCommand(action1);
        Sleep(10);
        processor->enqueueCallbackCommand(action2);
        CPPUNIT_ASSERT(action1.sem.wait(0));
        while (!action2.sem.wait(0))
            processor->checkForCompletions();
    }

    void testcallback2NoThreadDelay()
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createUring(config, false);
        if (!processor)
            return;

        // Test the non threaded uring processor - which only checks for completion when new events are submitted
        // The commands need to be non-no-ops Because no-op operations are processed immediately this test is not valid
        SemCallback action1;
        SemCallback action2;
        processor->enqueueCallbackCommand(action1);
        Sleep(100);
        CPPUNIT_ASSERT(!action1.sem.wait(0));
        processor->enqueueCallbackCommand(action2);
        Sleep(100);
        CPPUNIT_ASSERT(action1.sem.wait(0));
        CPPUNIT_ASSERT(!action2.sem.wait(0));
        while (!action2.sem.wait(0))
            processor->checkForCompletions();
    }

    void testcallbacks()
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createUring(config, true);
        if (!processor)
            return;

        // Test multiple actions being triggered by a single submission
        SemCallback action1;
        SemCallback action2;
        processor->enqueueCallbackCommands(std::vector<IAsyncCallback *>{&action1, &action2});
        action1.sem.wait();
        action2.sem.wait();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IOURingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IOURingTest, "IOURingTest");


#endif // _USE_CPPUNIT
