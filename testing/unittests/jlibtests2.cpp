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
        if (event.hasAttribute(EvAttrEventTraceId))
            event.setValue(EvAttrEventTraceId, "00000000000000000000000000000000");
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
        CPPUNIT_TEST(testContextOptionBaseline);
        CPPUNIT_TEST(testMultiBlock);
        CPPUNIT_TEST(testMultiThread);
        CPPUNIT_TEST(testBlocked);
        CPPUNIT_TEST(testReadEvents);
        CPPUNIT_TEST(testIterateAllAttributes);
        CPPUNIT_TEST(testIterateEventAttributes);
        CPPUNIT_TEST(testRecordingSource);
        CPPUNIT_TEST(testRecordingSourceOptional);
        CPPUNIT_TEST(testRecordingSourceMustBeFirst);
        CPPUNIT_TEST(testRecordingSourceOnlyOnce);
        CPPUNIT_TEST(testRecordingSourceRecursionLimit);
        CPPUNIT_TEST(testEventCompleteness);
        CPPUNIT_TEST(testPullEvents);
        CPPUNIT_TEST(testEventCopy);
        CPPUNIT_TEST(testFailCreate);
        CPPUNIT_TEST(testAllEventsFunction);
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
            CPPUNIT_ASSERT(recorder.startRecording("traceid", "eventtrace.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());

            //Check that overlapping starts fail
            CPPUNIT_ASSERT(!recorder.startRecording("traceid", "eventtrace.evtxxx", nullptr, 0, 0, 0, false));

            // Record some events
            recorder.recordIndexCacheHit(1, branchOffset, NodeBranch, 9876, 400);
            recorder.recordIndexCacheMiss(1, nodeSize, NodeLeaf);
            recorder.recordIndexLoad(1, nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            recorder.recordIndexEviction(1, branchOffset, NodeBranch, nodeSize);

            // Stop recording
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(4U, summary.numEvents);

            // Check that stopping again fails
            CPPUNIT_ASSERT(!recorder.stopRecording(nullptr, false));

            // Restart recording with a different filename
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "testfile.bin", nullptr, 0, 0, 0, true));
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
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
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

    void testContextOptionBaseline()
    {
        try
        {
            EventRecorder &recorder = queryRecorder();

            CPPUNIT_ASSERT(recorder.startRecording("dali=1", "context_baseline_include.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexCacheMiss(1, nodeSize, NodeLeaf);
            recorder.recordDaliGet(123, 100, 64);
            EventRecordingSummary summary;
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));

            verifyCounts("context_baseline_include.evt", {
                { EventIndexCacheMiss, 0 },
                { EventDaliGet, 1 }
            });

            CPPUNIT_ASSERT(recorder.startRecording("dali=0", "context_baseline_exclude.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexCacheMiss(1, nodeSize, NodeLeaf);
            recorder.recordDaliGet(123, 100, 64);
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));

            verifyCounts("context_baseline_exclude.evt", {
                { EventIndexCacheMiss, 1 },
                { EventDaliGet, 0 }
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
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record some events
            for (unsigned i=0; i < 100'000; i++)
            {
                recorder.recordIndexCacheMiss(1, i*nodeSize, NodeLeaf);
                recorder.recordIndexLoad(1, i*nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            }

            // Stop recording
            EventRecordingSummary summary;
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
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
        EventReporterThread(unsigned _id, unsigned _count) : Thread("EventReporterThread"), id(_id), count(_count) {}

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
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", nullptr, 0, 0, 0, false));
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
                CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
            }

            ForEachItemIn(t2, threads)
                threads.item(t2).join();

            if (!delay)
                CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));

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
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(recorder.startRecording("threadid", "/home/nonexistantuser/eventtrace.evt", nullptr, 0, 0, 0, false), "Expected startRecording() to throw an exception");

        //Check that the recorder has been left in a good state, and a subsequent recording works
        CPPUNIT_ASSERT(!recorder.isRecording());
        CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", nullptr, 0, 0, 0, false));
        CPPUNIT_ASSERT(recorder.isRecording());
        CPPUNIT_ASSERT(recorder.stopRecording(nullptr, false));
    }

    void testCleanup()
    {
        if (cleanup)
        {
            removeFile("eventtrace.evt");
            removeFile("testfile.bin");
            removeFile("recordingsource.evt");
            removeFile("recordingsource_optional.evt");
            removeFile("recordingsource_notfirst.evt");
            removeFile("recordingsource_multiple.evt");
            removeFile("recordingsource_recursion.evt");
            removeFile("pullevents.evt");
            removeFile("context_baseline_include.evt");
            removeFile("context_baseline_exclude.evt");
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
            CPPUNIT_ASSERT(recorder.startRecording("all=true,compress(0)", "eventtrace.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
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
attribute: ChannelId = 1
attribute: ReplicaId = 23
attribute: InstanceId = 57
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: InMemorySize = 4567
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", "test", 1, 23, 57, false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
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
            CPPUNIT_ASSERT(recorder.startRecording("all,compress(lz4hc)", "eventtrace.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 98765, 100, 73);
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
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
        std::set<unsigned> expectedDefined({EvAttrEventTimestamp, EvAttrEventTraceId, EvAttrEventThreadId, EvAttrEventStackTrace, EvAttrEnabled, EvAttrChannelId, EvAttrReplicaId, EvAttrInstanceId});
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
        std::set<unsigned> expectedDefined({EvAttrEventTimestamp, EvAttrEventTraceId, EvAttrEventThreadId, EvAttrEventStackTrace, EvAttrEnabled, EvAttrChannelId, EvAttrReplicaId, EvAttrInstanceId});
        std::set<unsigned> expectedAssigned;
        std::set<unsigned> actualDefined;
        std::set<unsigned> actualAssigned;
        size_t actualUnusedCount = 0;
        CEvent evt;
        evt.reset(EventRecordingActive);
        for (auto& attr : evt.definedAttributes)
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
        CPPUNIT_ASSERT_EQUAL(size_t(0), actualUnusedCount);
        actualDefined.clear();
        actualUnusedCount = 0;
        evt.setValue(EvAttrEnabled, true);
        expectedAssigned.insert(EvAttrEnabled);
        expectedDefined.erase(EvAttrEnabled);
        for (auto& attr : evt.definedAttributes)
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
        CPPUNIT_ASSERT_EQUAL(size_t(0), actualUnusedCount);
    }

    void testRecordingSource()
    {
        try
        {
            EventRecorder &recorder = queryRecorder();
            EventRecordingSummary summary;

            // Start recording
            CPPUNIT_ASSERT(recorder.startRecording("traceid", "recordingsource.evt", "test", 1, 2, 3, false));
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record IndexCacheMiss with default recordIndexCacheMiss function (no ChannelId, ReplicaId, InstanceId)
            recorder.recordIndexCacheMiss(100, 200, NodeLeaf);

            // Create and record a CEvent with IndexCacheMiss that includes ChannelId, ReplicaId, InstanceId
            CEvent event;
            event.reset(EventIndexCacheMiss);
            event.setValue(EvAttrFileId, 100U);
            event.setValue(EvAttrFileOffset, 200ULL);
            event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);
            event.setValue(EvAttrChannelId, 1U);
            event.setValue(EvAttrReplicaId, 2U);
            event.setValue(EvAttrInstanceId, 3ULL);
            recorder.recordEvent(event);

            // Stop recording
            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(3U, summary.numEvents);

            // Read and verify events
            // Note: RecordingSource is consumed internally and not exposed to visitors
            class VerifyVisitor : public CInterfaceOf<IEventVisitor>
            {
            public:
                virtual bool visitFile(const char* filename, uint32_t version) override
                {
                    return true;
                }
                virtual bool visitEvent(CEvent& event) override
                {
                    eventCount++;

                    if (eventCount == 1)
                    {
                        // First event should be IndexCacheMiss with ChannelId, ReplicaId, InstanceId
                        // assigned from RecordingSource (even though not originally recorded with them)
                        CPPUNIT_ASSERT_EQUAL((int)EventIndexCacheMiss, (int)event.queryType());
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrFileId));
                        CPPUNIT_ASSERT_EQUAL(100ULL, event.queryNumericValue(EvAttrFileId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrFileOffset));
                        CPPUNIT_ASSERT_EQUAL(200ULL, event.queryNumericValue(EvAttrFileOffset));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrNodeKind));
                        CPPUNIT_ASSERT_EQUAL((int)NodeLeaf, (int)event.queryNumericValue(EvAttrNodeKind));
                        // Verify ChannelId, ReplicaId, InstanceId are assigned with correct values
                        // from RecordingSource by the iterator
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrChannelId));
                        CPPUNIT_ASSERT_EQUAL(1ULL, event.queryNumericValue(EvAttrChannelId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrReplicaId));
                        CPPUNIT_ASSERT_EQUAL(2ULL, event.queryNumericValue(EvAttrReplicaId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrInstanceId));
                        CPPUNIT_ASSERT_EQUAL(3ULL, event.queryNumericValue(EvAttrInstanceId));
                    }
                    else if (eventCount == 2)
                    {
                        // Second event should be IndexCacheMiss with all attributes assigned
                        CPPUNIT_ASSERT_EQUAL((int)EventIndexCacheMiss, (int)event.queryType());
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrFileId));
                        CPPUNIT_ASSERT_EQUAL(100ULL, event.queryNumericValue(EvAttrFileId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrFileOffset));
                        CPPUNIT_ASSERT_EQUAL(200ULL, event.queryNumericValue(EvAttrFileOffset));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrNodeKind));
                        CPPUNIT_ASSERT_EQUAL((int)NodeLeaf, (int)event.queryNumericValue(EvAttrNodeKind));
                        // Verify ChannelId, ReplicaId, InstanceId are assigned with correct values
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrChannelId));
                        CPPUNIT_ASSERT_EQUAL(1ULL, event.queryNumericValue(EvAttrChannelId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrReplicaId));
                        CPPUNIT_ASSERT_EQUAL(2ULL, event.queryNumericValue(EvAttrReplicaId));
                        CPPUNIT_ASSERT(event.hasAttribute(EvAttrInstanceId));
                        CPPUNIT_ASSERT_EQUAL(3ULL, event.queryNumericValue(EvAttrInstanceId));
                    }

                    return true;
                }
                virtual void departFile(uint32_t bytesRead) override
                {
                }
                unsigned eventCount = 0;
            };

            VerifyVisitor visitor;
            CPPUNIT_ASSERT(readEvents("recordingsource.evt", visitor));
            CPPUNIT_ASSERT_EQUAL(2U, visitor.eventCount);

            // Also verify using IEventIterator
            Owned<IEventIterator> ei = createEventFileIterator("recordingsource.evt");
            CPPUNIT_ASSERT_MESSAGE("Should be able to create event iterator", ei.get());

            // Read first event to trigger RecordingSource processing
            CPPUNIT_ASSERT_MESSAGE("Should be able to read first event", ei->nextEvent(event));

            const EventFileProperties& props = ei->queryFileProperties();
            CPPUNIT_ASSERT_MESSAGE("Process descriptor should be set from RecordingSource", props.processDescriptor.get());
            CPPUNIT_ASSERT_MESSAGE("Process descriptor should match RecordingSource", props.processDescriptor.get() && strcmp(props.processDescriptor.get(), "test") == 0);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Channel ID should be from RecordingSource", byte(1), props.channelId);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Replica ID should be from RecordingSource", byte(2), props.replicaId);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Instance ID should be from RecordingSource", 3U, (unsigned)props.instanceId);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testRecordingSourceOptional()
    {
        try
        {
            EventRecorder &recorder = queryRecorder();
            EventRecordingSummary summary;

            CPPUNIT_ASSERT_MESSAGE("Should be able to start recording without RecordingSource event", recorder.startRecording("traceid", "recordingsource_optional.evt", nullptr, 0, 0, 0, false));
            CPPUNIT_ASSERT_MESSAGE("Recording should be active", recorder.isRecording());

            recorder.recordIndexCacheMiss(100, 200, NodeLeaf);
            recorder.recordIndexCacheHit(100, 300, NodeBranch, 1024, 500);

            CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary, false));
            CPPUNIT_ASSERT_MESSAGE("Recording should be inactive after stop", !recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Should have recorded 2 events", 2U, summary.numEvents);

            class CountingVisitor : public CInterfaceOf<IEventVisitor>
            {
            public:
                virtual bool visitFile(const char* filename, uint32_t version) override { return true; }
                virtual bool visitEvent(CEvent& event) override
                {
                    eventCount++;
                    return true;
                }
                virtual void departFile(uint32_t bytesRead) override {}
                unsigned eventCount = 0;
            };

            CountingVisitor visitor;
            CPPUNIT_ASSERT_MESSAGE("Should be able to read file without RecordingSource event", readEvents("recordingsource_optional.evt", visitor));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Should have read 2 events", 2U, visitor.eventCount);

            Owned<IEventIterator> ei = createEventFileIterator("recordingsource_optional.evt");
            CPPUNIT_ASSERT_MESSAGE("Should be able to create event iterator", ei.get());

            const EventFileProperties& props = ei->queryFileProperties();
            CPPUNIT_ASSERT_MESSAGE("Process descriptor should not be set when RecordingSource is absent", !props.processDescriptor.get());
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Channel ID should be 0 when RecordingSource is absent", byte(0), props.channelId);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Replica ID should be 0 when RecordingSource is absent", byte(0), props.replicaId);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Instance ID should be 0 when RecordingSource is absent", 0U, (unsigned)props.instanceId);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testRecordingSourceMustBeFirst()
    {
        EventRecorder &recorder = queryRecorder();
        EventRecordingSummary summary;

        CPPUNIT_ASSERT_MESSAGE("Should be able to start recording", recorder.startRecording("traceid", "recordingsource_notfirst.evt", nullptr, 0, 0, 0, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be active", recorder.isRecording());

        recorder.recordIndexCacheMiss(100, 200, NodeLeaf);
        recorder.recordRecordingSource("test", 1, 2, 3);
        recorder.recordIndexCacheHit(100, 300, NodeBranch, 1024, 500);

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be inactive after stop", !recorder.isRecording());

        class DummyVisitor : public CInterfaceOf<IEventVisitor>
        {
        public:
            virtual bool visitFile(const char* filename, uint32_t version) override { return true; }
            virtual bool visitEvent(CEvent& event) override { return true; }
            virtual void departFile(uint32_t bytesRead) override {}
        };

        DummyVisitor visitor;
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(readEvents("recordingsource_notfirst.evt", visitor), "Expected exception when RecordingSource is not the first event");
    }

    void testRecordingSourceOnlyOnce()
    {
        EventRecorder &recorder = queryRecorder();
        EventRecordingSummary summary;

        CPPUNIT_ASSERT_MESSAGE("Should be able to start recording", recorder.startRecording("traceid", "recordingsource_multiple.evt", nullptr, 0, 0, 0, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be active", recorder.isRecording());

        recorder.recordRecordingSource("test1", 1, 2, 3);
        recorder.recordIndexCacheMiss(100, 200, NodeLeaf);
        recorder.recordRecordingSource("test2", 4, 5, 6);
        recorder.recordIndexCacheHit(100, 300, NodeBranch, 1024, 500);

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be inactive after stop", !recorder.isRecording());

        class DummyVisitor : public CInterfaceOf<IEventVisitor>
        {
        public:
            virtual bool visitFile(const char* filename, uint32_t version) override { return true; }
            virtual bool visitEvent(CEvent& event) override { return true; }
            virtual void departFile(uint32_t bytesRead) override {}
        };

        DummyVisitor visitor;
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(readEvents("recordingsource_multiple.evt", visitor), "Expected exception when multiple RecordingSource events are present");
    }

    void testRecordingSourceRecursionLimit()
    {
        EventRecorder &recorder = queryRecorder();
        EventRecordingSummary summary;

        CPPUNIT_ASSERT_MESSAGE("Should be able to start recording", recorder.startRecording("traceid", "recordingsource_recursion.evt", "test1", 1, 2, 3, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be active", recorder.isRecording());

        recorder.recordRecordingSource("test1", 1, 2, 3);
        recorder.recordRecordingSource("test2", 4, 5, 6);
        recorder.recordRecordingSource("test3", 7, 8, 9);
        recorder.recordIndexCacheMiss(100, 200, NodeLeaf);

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary, false));
        CPPUNIT_ASSERT_MESSAGE("Recording should be inactive after stop", !recorder.isRecording());

        Owned<IEventIterator> ei = createEventFileIterator("recordingsource_recursion.evt");
        CPPUNIT_ASSERT_MESSAGE("Should be able to create event iterator", ei.get());

        CEvent event;
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(ei->nextEvent(event), "Expected exception when attempting to read past first RecordingSource");

        const EventFileProperties& props = ei->queryFileProperties();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("No events passed out - RecordingSource events are consumed internally", 0U, props.eventsRead);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Properties should be from first RecordingSource", byte(1), props.channelId);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Properties should be from first RecordingSource", byte(2), props.replicaId);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Properties should be from first RecordingSource", 3U, (unsigned)props.instanceId);
    }

    void testEventCompleteness()
    {
        try
        {
            // Test IndexCacheMiss event with only required attributes (no header or source)
            {
                CEvent event;
                event.reset(EventIndexCacheMiss);
                // Without required attributes, should not be complete
                CPPUNIT_ASSERT(!event.isComplete());

                // Add required attributes
                event.setValue(EvAttrFileId, 100U);
                event.setValue(EvAttrFileOffset, 200ULL);
                event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);

                // Should be complete with only required attributes (header/source are optional)
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test that header attributes are optional and don't affect completeness
            {
                CEvent event;
                event.reset(EventIndexCacheMiss);
                event.setValue(EvAttrFileId, 100U);
                event.setValue(EvAttrFileOffset, 200ULL);
                event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);

                // Should be complete without header attributes
                CPPUNIT_ASSERT(event.isComplete());

                // Add some header attributes - should still be complete
                event.setValue(EvAttrEventTimestamp, 123456ULL);
                CPPUNIT_ASSERT(event.isComplete());

                event.setValue(EvAttrEventThreadId, 789ULL);
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test that source attributes are optional and don't affect completeness
            {
                CEvent event;
                event.reset(EventIndexCacheMiss);
                event.setValue(EvAttrFileId, 100U);
                event.setValue(EvAttrFileOffset, 200ULL);
                event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);

                // Should be complete without source attributes
                CPPUNIT_ASSERT(event.isComplete());

                // Add some source attributes - should still be complete
                event.setValue(EvAttrChannelId, 1U);
                CPPUNIT_ASSERT(event.isComplete());

                event.setValue(EvAttrReplicaId, 2U);
                CPPUNIT_ASSERT(event.isComplete());

                event.setValue(EvAttrInstanceId, 3ULL);
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test with all combinations of header and source attributes
            {
                CEvent event;
                event.reset(EventIndexCacheMiss);
                event.setValue(EvAttrFileId, 100U);
                event.setValue(EvAttrFileOffset, 200ULL);
                event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);

                // Add all header attributes
                event.setValue(EvAttrEventTimestamp, 123456ULL);
                event.setValue(EvAttrEventTraceId, "00000000000000000000000000000000");
                event.setValue(EvAttrEventThreadId, 789ULL);
                event.setValue(EvAttrEventStackTrace, "stack trace");

                // Add all source attributes
                event.setValue(EvAttrChannelId, 1U);
                event.setValue(EvAttrReplicaId, 2U);
                event.setValue(EvAttrInstanceId, 3ULL);

                // Should still be complete
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test RecordingSource event requires source attributes
            {
                CEvent event;
                event.reset(EventRecordingSource);

                // Without required ProcessDescriptor, should not be complete
                CPPUNIT_ASSERT(!event.isComplete());

                // Add required ProcessDescriptor attribute
                event.setValue(EvAttrProcessDescriptor, "test");

                // Should NOT be complete without source attributes (they are required for RecordingSource)
                CPPUNIT_ASSERT(!event.isComplete());

                // Add only some source attributes - still not complete
                event.setValue(EvAttrChannelId, 1U);
                CPPUNIT_ASSERT(!event.isComplete());

                event.setValue(EvAttrReplicaId, 2U);
                CPPUNIT_ASSERT(!event.isComplete());

                // Add final source attribute - now complete
                event.setValue(EvAttrInstanceId, 3ULL);
                CPPUNIT_ASSERT(event.isComplete());

                // Add header attributes - should still be complete
                event.setValue(EvAttrEventTimestamp, 123456ULL);
                event.setValue(EvAttrEventThreadId, 789ULL);
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test IndexCacheHit with multiple required attributes
            {
                CEvent event;
                event.reset(EventIndexCacheHit);

                // Missing required attributes - not complete
                CPPUNIT_ASSERT(!event.isComplete());

                // Add some but not all required attributes
                event.setValue(EvAttrFileId, 100U);
                event.setValue(EvAttrFileOffset, 200ULL);
                CPPUNIT_ASSERT(!event.isComplete());

                // Add more required attributes
                event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);
                event.setValue(EvAttrInMemorySize, 1024U);
                CPPUNIT_ASSERT(!event.isComplete());

                // Add final required attribute
                event.setValue(EvAttrExpandTime, 500ULL);
                CPPUNIT_ASSERT(event.isComplete());

                // Event is complete with no header or source attributes
                // Now add header and source - should still be complete
                event.setValue(EvAttrEventTimestamp, 123456ULL);
                event.setValue(EvAttrChannelId, 1U);
                CPPUNIT_ASSERT(event.isComplete());
            }

            // Test RecordingActive event with boolean attribute
            {
                CEvent event;
                event.reset(EventRecordingActive);

                // Without required Enabled attribute, should not be complete
                CPPUNIT_ASSERT(!event.isComplete());

                // Add required boolean attribute
                event.setValue(EvAttrEnabled, true);

                // Should be complete
                CPPUNIT_ASSERT(event.isComplete());

                // Test with header and source attributes
                event.setValue(EvAttrEventTimestamp, 123456ULL);
                event.setValue(EvAttrChannelId, 1U);
                event.setValue(EvAttrReplicaId, 2U);
                CPPUNIT_ASSERT(event.isComplete());
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

    void testPullEvents()
    {
        try
        {
            EventRecorder &recorder = queryRecorder();
            EventRecordingSummary summary;

            // Record a variety of events to a test file
            CPPUNIT_ASSERT(recorder.startRecording("traceid", "pullevents.evt", "testprocess", 1, 2, 3, false));
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record different event types with various attributes
            recorder.recordIndexCacheMiss(100, 200, NodeLeaf);
            recorder.recordIndexCacheHit(100, 300, NodeBranch, 1024, 500);
            recorder.recordIndexLoad(200, 400, NodeLeaf, 2048, 600, 400);
            recorder.recordDaliConnect("/Test/Path", 12345, 100, 50);

            CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(5U, summary.numEvents);

            // Read events using visitor pattern (push API)
            class CollectingVisitor : public CInterfaceOf<IEventVisitor>
            {
            public:
                virtual bool visitFile(const char* filename, uint32_t version) override
                {
                    fileVisited = true;
                    visitedFilename.set(filename);
                    visitedVersion = version;
                    return true;
                }
                virtual bool visitEvent(CEvent& event) override
                {
                    eventsRead++;
                    // Store a copy of each event
                    events.emplace_back();
                    CEvent& copy = events.back();
                    copy.reset(event.queryType());
                    for (auto& attr : event.definedAttributes)
                    {
                        if (attr.isAssigned())
                        {
                            EventAttr attrId = attr.queryId();
                            if (attr.isText())
                                copy.setValue(attrId, attr.queryTextValue());
                            else if (attr.isBoolean())
                                copy.setValue(attrId, attr.queryBooleanValue());
                            else
                                copy.setValue(attrId, attr.queryNumericValue());
                        }
                    }
                    return true;
                }
                virtual void departFile(uint32_t bytesRead) override
                {
                    fileCompleted = true;
                    visitedBytesRead = bytesRead;
                }

                bool fileVisited = false;
                bool fileCompleted = false;
                StringAttr visitedFilename;
                uint32_t visitedVersion = 0;
                uint32_t visitedBytesRead = 0;
                uint32_t eventsRead = 0;
                std::vector<CEvent> events;
            };

            CollectingVisitor visitor;
            CPPUNIT_ASSERT(readEvents("pullevents.evt", visitor));
            CPPUNIT_ASSERT(visitor.fileVisited);
            CPPUNIT_ASSERT(visitor.fileCompleted);
            CPPUNIT_ASSERT_EQUAL(4U, (unsigned)visitor.events.size());

            // Read events using pull API (IEventIterator)
            Owned<IEventIterator> ei = createEventFileIterator("pullevents.evt");
            CPPUNIT_ASSERT(ei.get());

            const EventFileProperties& props = ei->queryFileProperties();

            // Verify file properties available immediately (from file header)
            CPPUNIT_ASSERT(visitor.visitedFilename.get());
            CPPUNIT_ASSERT_EQUAL_STR(visitor.visitedFilename.get(), props.path.get());
            CPPUNIT_ASSERT_EQUAL(visitor.visitedVersion, props.version);

            // Read all events using pull API and compare with visitor results
            CEvent iterEvent;
            unsigned eventIndex = 0;
            while (ei->nextEvent(iterEvent))
            {
                CPPUNIT_ASSERT(eventIndex < visitor.events.size());
                CEvent& visitorEvent = visitor.events[eventIndex];

                // Verify event types match
                CPPUNIT_ASSERT_EQUAL((int)visitorEvent.queryType(), (int)iterEvent.queryType());

                // Verify all defined attributes match
                for (auto& attr : iterEvent.definedAttributes)
                {
                    EventAttr attrId = attr.queryId();

                    // Both events should have the same attribute defined
                    CPPUNIT_ASSERT(visitorEvent.isAttribute(attrId));

                    // Check if assigned state matches
                    if (attr.isAssigned())
                    {
                        CPPUNIT_ASSERT(visitorEvent.hasAttribute(attrId));

                        // Compare values based on type
                        if (attr.isText())
                        {
                            CPPUNIT_ASSERT_EQUAL_STR(visitorEvent.queryTextValue(attrId),
                                                    iterEvent.queryTextValue(attrId));
                        }
                        else if (attr.isBoolean())
                        {
                            CPPUNIT_ASSERT_EQUAL(visitorEvent.queryBooleanValue(attrId),
                                               iterEvent.queryBooleanValue(attrId));
                        }
                        else
                        {
                            CPPUNIT_ASSERT_EQUAL(visitorEvent.queryNumericValue(attrId),
                                               iterEvent.queryNumericValue(attrId));
                        }
                    }
                    else
                    {
                        // Attribute should be defined but not assigned in both
                        CPPUNIT_ASSERT(!visitorEvent.hasAttribute(attrId));
                    }
                }

                eventIndex++;
            }

            // Verify we read the same number of events
            CPPUNIT_ASSERT_EQUAL((unsigned)visitor.events.size(), eventIndex);

            // Verify nextEvent returns false after all events are consumed
            CPPUNIT_ASSERT(!ei->nextEvent(iterEvent));

            // Now verify runtime properties that are populated during event reading
            CPPUNIT_ASSERT_EQUAL(visitor.visitedBytesRead, props.bytesRead);
            // Note: props.eventsRead only includes events passed to visitors - RecordingSource is consumed internally
            CPPUNIT_ASSERT_EQUAL(4U, props.eventsRead);
            CPPUNIT_ASSERT_EQUAL(4U, visitor.eventsRead);

            // Verify recording source attributes from the RecordingSource event were captured in properties
            CPPUNIT_ASSERT(props.processDescriptor.get());
            CPPUNIT_ASSERT_EQUAL_STR("testprocess", props.processDescriptor.get());
            CPPUNIT_ASSERT_EQUAL(byte(1), props.channelId);
            CPPUNIT_ASSERT_EQUAL(byte(2), props.replicaId);
            CPPUNIT_ASSERT_EQUAL(3U, (unsigned)props.instanceId);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testEventCopy()
    {
        try
        {
            // Dynamically allocate a CEvent to control object lifetime manually
            CEvent* e1 = new CEvent();
            e1->reset(EventIndexCacheMiss);
            e1->setValue(EvAttrFileId, 100U);

            // Force the implicit shallow copy
            // e2's internal objects (e.g., AssignedAttributes) will retain a reference
            // to e1's memory address, NOT e2's memory address.
            CEvent e2(*e1);

            // Delete the original memory allocation so that the reference dangles
            delete e1;

            // Attempting to iterate or set values on the shallow copy triggers the dangling reference.
            // This is the identical scenario to std::vector resizing.
            e2.setValue(EvAttrFileOffset, 200ULL);

            unsigned assignedCount = 0;
            for (auto& attr : e2.assignedAttributes)
            {
                assignedCount++;
            }

            // We expect at least the two attributes we assigned.
            // If the copy constructor is missing, this traversal might segfault
            // or read corrupted data from the deleted heap.
            CPPUNIT_ASSERT(assignedCount >= 2);
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    IEventVisitor* createVisitor(StringBuffer& out)
    {
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        Owned<CMetaInfoState> metaState = new CMetaInfoState();
        Owned<IEventVisitor> visitor = createDumpTextEventVisitor(*stream, *metaState, DumpMetaFlag::None);
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

    constexpr static unsigned testChannelId = 123;
    constexpr static unsigned testReplicaId = 231;
    constexpr static __uint64 testInstanceId = 789;

    void testAllRecordFunction()
    {
        START_TEST

        EventRecorder &recorder = queryRecorder();
        EventRecordingSummary summary;

        // Record a variety of events to a test file
        CPPUNIT_ASSERT(recorder.startRecording("all", "pullevents.evt", "testprocess", testChannelId, testReplicaId, testInstanceId, false));
        CPPUNIT_ASSERT(recorder.isRecording());

        // File information
        recorder.recordPlaneInformation("data", "/data", false);
        recorder.recordFileInformation(100, "/data/testfile.idx");

        // Add a demo call to each of the record() functions
        // Index-related events
        recorder.recordIndexOpen(100, 9933);

        recorder.recordIndexCacheHit(1, 8192, NodeBranch, 1024, 100);
        recorder.recordIndexCacheMiss(2, 16384, NodeLeaf);
        recorder.recordIndexLoad(3, 24576, NodeBranch, 2048, 150, 200);
        recorder.recordIndexEviction(4, 32768, NodeLeaf, 4096);
        recorder.recordIndexPayload(5, 40960, true, 250);

        // Add an EventRecordingActive event
        recorder.pauseRecording(true, true);
        recorder.pauseRecording(false, true);

        // Dali-related events
        recorder.recordDaliChangeMode(1001, 100, 256);
        recorder.recordDaliCommit(1002, 200, 512);
        recorder.recordDaliConnect("/Workunits/Workunit/test.wu", 1003, 300, 1024);
        recorder.recordDaliEnsureLocal(1004, 150, 128);
        recorder.recordDaliGet(1005, 250, 256);
        recorder.recordDaliGetChildren("/Test/Path/Children", 1006, 175, 512);
        recorder.recordDaliGetChildrenFor(1007, 225, 768);
        recorder.recordDaliGetElements("/Test/Path/Elements", 1008, 275, 1536);
        recorder.recordDaliSubscribe("/Test/Subscribe/Path", 1009, 125);

        // Query events
        recorder.recordQueryStart("TestQuery");
        recorder.recordQueryStop();

        // Task events
        recorder.recordTaskStart(EventTask::Sink);
        recorder.recordTaskStop(EventTask::Sink);
        recorder.recordTaskStart(EventTask::Running);
        recorder.recordTaskStop(EventTask::Running);

        // Lock and semaphore events
        constexpr unsigned lockId = 42;
        recorder.recordLockWait(lockId);
        recorder.recordLockAcquire(lockId);
        recorder.recordLockRelease(lockId);
        recorder.recordSemWait(lockId);
        recorder.recordSemAcquire(lockId);
        recorder.recordSemSignal(lockId);
        recorder.recordLockTryWaitAcquire(lockId);
        recorder.recordLockTryWaitFail(lockId);
        recorder.recordLockWaitTimeout(lockId);
        recorder.recordSemWaitTimeout(lockId);

        // Read/write lock events (currently only available via generic recordEvent)
        // because they will only ever be recorded in the protrace framework
        CEvent rwlockEvent;
        auto recordRwlockEvent = [&](EventType type)
        {
            rwlockEvent.reset(type);
            rwlockEvent.setValue(EvAttrLockId, lockId);
            recorder.recordEvent(rwlockEvent);
        };
        recordRwlockEvent(EventRwlockReadWait);
        recordRwlockEvent(EventRwlockReadAcquire);
        recordRwlockEvent(EventRwlockReadRelease);
        recordRwlockEvent(EventRwlockWriteWait);
        recordRwlockEvent(EventRwlockWriteAcquire);
        recordRwlockEvent(EventRwlockWriteRelease);
        recordRwlockEvent(EventRwlockReadWaitTimeout);
        recordRwlockEvent(EventRwlockWriteWaitTimeout);

        // Function events (protrace-only; recorded via generic recordEvent for completeness)
        constexpr unsigned functionId = 1;
        CEvent functionEvent;
        auto recordFunctionEvent = [&](EventType type)
        {
            functionEvent.reset(type);
            functionEvent.setValue(EvAttrFunctionId, functionId);
            recorder.recordEvent(functionEvent);
        };
        recordFunctionEvent(EventFunctionEnter);
        recordFunctionEvent(EventFunctionExit);

        // Remote events
        recorder.recordRequestSend(1, 1);
        recorder.recordRequestReceive(1, 1);
        recorder.recordWorkerStart(1, 1);
        recorder.recordWorkerStop(1, 1);
        recorder.recordResponseSend(1, 1, 1, 1);
        recorder.recordResponseReceive(1, 1, 1, 1);
        recorder.recordMpRequestSend(10, 1000);
        recorder.recordMpRequestReceive(10);
        recorder.recordMpResponseSend(20, 2000);
        recorder.recordMpResponseReceive(20);

        // Queue events
        recorder.recordEnqueue(0x1122334455667788ULL);
        recorder.recordDequeue(0x1122334455667788ULL);

        // Do not call Recording source (additional call to test multiple sources)
        // because this is done implicitly when recording is started and it is invalid to have two
        // of these events in a recording.
        // recorder.recordRecordingSource("anotherprocess", 10, 20, 30);

        // Generic event using recordEvent
        CEvent event;
        event.reset(EventIndexCacheMiss);
        event.setValue(EvAttrFileId, 99U);
        event.setValue(EvAttrFileOffset, 49152ULL);
        event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);
        recorder.recordEvent(event);

        CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
        CPPUNIT_ASSERT(!recorder.isRecording());

        END_TEST
    }

    void testAllReadFunction()
    {
        START_TEST

        // Create an array to track which event types have been seen
        bool eventTypeSeen[EventMax] = { false };

        // Open the event file created by testAllRecordFunction
        Owned<IEventIterator> ei = createEventFileIterator("pullevents.evt");
        CPPUNIT_ASSERT_MESSAGE("Should be able to create event iterator", ei.get());

        // Iterate through all events and mark them as seen
        CEvent event;
        while (ei->nextEvent(event))
        {
            EventType type = event.queryType();
            CPPUNIT_ASSERT_MESSAGE("Event type should be valid", type >= EventNone && type < EventMax);
            CPPUNIT_ASSERT_EQUAL(testChannelId, (unsigned)event.queryAttribute(EvAttrChannelId).queryNumericValue());
            CPPUNIT_ASSERT_EQUAL(testReplicaId, (unsigned)event.queryAttribute(EvAttrReplicaId).queryNumericValue());
            CPPUNIT_ASSERT_EQUAL(testInstanceId, (unsigned __int64)event.queryAttribute(EvAttrInstanceId).queryNumericValue());
            eventTypeSeen[type] = true;
        }

        // Report all event types that were not seen (except EventRecordingSource)
        StringBuffer missingEvents;
        bool first = true;
        for (int i = 0; i < EventMax; i++)
        {
            EventType type = (EventType)i;
            // Skip EventRecordingSource as requested
            if ((type == EventRecordingSource) || (type == EventNone))
                continue;

            if (!eventTypeSeen[i])
            {
                if (!first)
                    missingEvents.append(", ");
                missingEvents.append(queryEventName(type));
                first = false;
            }
        }

        // Log the result for informational purposes
        if (missingEvents.length() > 0)
            CPPUNIT_FAIL(VStringBuffer("Event types not seen in pullevents.evt: %s\n", missingEvents.str()).str());

        END_TEST
    }

    void testAllEventsFunction()
    {
        testAllRecordFunction();
        testAllReadFunction();
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
            Semaphore startSem{SYNC_LOCATION};
            Semaphore stopSem{SYNC_LOCATION};
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

class AsyncForTimingStressTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AsyncForTimingStressTest);
        CPPUNIT_TEST(runAllTests);
    CPPUNIT_TEST_SUITE_END();

    enum class Workload : unsigned
    {
        noOp,
        atomicSingle,
        atomicTwenty
    };

    struct AsyncForCase
    {
        unsigned num;
        unsigned maxAtOnce;
    };

public:
    void runAllTests()
    {
        static constexpr Workload workloads[] = {
            Workload::noOp,
            Workload::atomicSingle,
            Workload::atomicTwenty
        };

        static constexpr AsyncForCase cases[] = {
            { 4'096, 1 },
            { 4'096, 4 },
            { 4'096, 16 },
            { 32'768, 1 },
            { 32'768, 4 },
            { 32'768, 16 },
            { 131'072, 1 },
            { 131'072, 4 },
            { 131'072, 16 }
        };

        DBGLOG("AsyncFor timing stress: repeats=%u warmup=%u", repeats, warmupRuns);
        DBGLOG("%-10s %8s %10s %10s %12s %12s %12s %16s %15s %18s %13s",
               "workload", "num", "maxAtOnce", "ratio", "serialNs", "asyncNs", "deltaNs",
               "serialNsPerItem", "asyncNsPerItem", "overheadNsPerItem", "slowdownX1000");
        for (Workload workload : workloads)
        {
            for (const auto & nextCase : cases)
                runCase(workload, nextCase.num, nextCase.maxAtOnce);
        }
    }

private:
    static constexpr unsigned repeats = 7;
    static constexpr unsigned warmupRuns = 1;

    static const char * queryWorkloadName(Workload workload)
    {
        switch (workload)
        {
        case Workload::noOp:
            return "no-op";
        case Workload::atomicSingle:
            return "atomic+1";
        case Workload::atomicTwenty:
            return "atomic+20";
        }
        return "unknown";
    }

    static unsigned __int64 queryExpectedValue(Workload workload, unsigned num)
    {
        switch (workload)
        {
        case Workload::noOp:
            return 0;
        case Workload::atomicSingle:
            return num;
        case Workload::atomicTwenty:
            return (unsigned __int64) num * 20;
        }
        return 0;
    }

    static void executeWork(Workload workload, std::atomic<unsigned __int64> & counter)
    {
        switch (workload)
        {
        case Workload::noOp:
            break;
        case Workload::atomicSingle:
            counter.fetch_add(1, std::memory_order_relaxed);
            break;
        case Workload::atomicTwenty:
            for (unsigned i = 0; i < 20; i++)
                counter.fetch_add(1, std::memory_order_relaxed);
            break;
        }
    }

    static unsigned __int64 runSerial(Workload workload, unsigned num)
    {
        std::atomic<unsigned __int64> counter{0};
        CCycleTimer timer;
        for (unsigned i = 0; i < num; i++)
            executeWork(workload, counter);
        unsigned __int64 elapsed = timer.elapsedNs();
        CPPUNIT_ASSERT_EQUAL(queryExpectedValue(workload, num), counter.load(std::memory_order_relaxed));
        return elapsed;
    }

    static unsigned __int64 runAsync(Workload workload, unsigned num, unsigned maxAtOnce)
    {
        std::atomic<unsigned __int64> counter{0};
        CCycleTimer timer;
        asyncFor("AsyncForTimingStressTest", num, maxAtOnce, [&counter, workload](unsigned)
        {
            executeWork(workload, counter);
        });
        unsigned __int64 elapsed = timer.elapsedNs();
        CPPUNIT_ASSERT_EQUAL(queryExpectedValue(workload, num), counter.load(std::memory_order_relaxed));
        return elapsed;
    }

    template <class Fn>
    static unsigned __int64 measureMedianNs(Fn && fn)
    {
        for (unsigned i = 0; i < warmupRuns; i++)
            fn();

        std::vector<unsigned __int64> samples;
        samples.reserve(repeats);
        for (unsigned i = 0; i < repeats; i++)
            samples.emplace_back(fn());

        std::sort(samples.begin(), samples.end());
        return samples[samples.size()/2];
    }

    static void runCase(Workload workload, unsigned num, unsigned maxAtOnce)
    {
        unsigned __int64 serialNs = measureMedianNs([workload, num]()
        {
            return runSerial(workload, num);
        });

        unsigned __int64 asyncNs = measureMedianNs([workload, num, maxAtOnce]()
        {
            return runAsync(workload, num, maxAtOnce);
        });

        __int64 deltaNs = (__int64)asyncNs - (__int64)serialNs;
        unsigned ratio = num / maxAtOnce;
        unsigned __int64 serialNsPerItem = serialNs / num;
        unsigned __int64 asyncNsPerItem = asyncNs / num;
        __int64 overheadNsPerItem = deltaNs / (__int64)num;
        unsigned slowdownX1000 = serialNs ? (unsigned)(((unsigned __int64)asyncNs * 1000) / serialNs) : 0;

         DBGLOG("%-10s %8u %10u %8u.0 %12" I64F "u %12" I64F "u %12" I64F "d %16" I64F "u %15" I64F "u %18" I64F "d %13u",
             queryWorkloadName(workload), num, maxAtOnce, ratio,
               serialNs, asyncNs, deltaNs,
               serialNsPerItem, asyncNsPerItem, overheadNsPerItem, slowdownX1000);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AsyncForTimingStressTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AsyncForTimingStressTest, "AsyncForTimingStressTest");

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

class JlibDateTimeFormatTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibDateTimeFormatTest);
        CPPUNIT_TEST(testExplicitPrecisionRange);
        CPPUNIT_TEST(testBackwardCompatibleDefaults);
    CPPUNIT_TEST_SUITE_END();

public:
    void testExplicitPrecisionRange()
    {
        START_TEST

        CDateTime dt;
        dt.set(2026, 7, 22, 3, 4, 5, 123456789, false);

        constexpr const char * expected[] =
        {
            "2026-07-22T03:04:05",
            "2026-07-22T03:04:05.1",
            "2026-07-22T03:04:05.12",
            "2026-07-22T03:04:05.123",
            "2026-07-22T03:04:05.1234",
            "2026-07-22T03:04:05.12345",
            "2026-07-22T03:04:05.123456",
            "2026-07-22T03:04:05.1234567",
            "2026-07-22T03:04:05.12345678",
            "2026-07-22T03:04:05.123456789"
        };

        StringBuffer out;
        for (unsigned precision = DTP_Seconds; precision <= DTP_Nanos; ++precision)
        {
            out.clear();
            dt.getDateTimeString(out, true, true, precision, false);
            CPPUNIT_ASSERT_EQUAL_STR(expected[precision], out.str());
        }

        END_TEST
    }

    void testBackwardCompatibleDefaults()
    {
        START_TEST

        StringBuffer out;

        CDateTime noFraction;
        noFraction.set(2026, 7, 22, 3, 4, 5, 0, false);
        out.clear();
        noFraction.getString(out, false);
        CPPUNIT_ASSERT_EQUAL_STR("2026-07-22T03:04:05", out.str());

        CDateTime withFraction;
        withFraction.set(2026, 7, 22, 3, 4, 5, 123456789, false);

        out.clear();
        withFraction.getDateTimeString(out, true, true, DTP_MaybeMicros, false);
        CPPUNIT_ASSERT_EQUAL_STR("2026-07-22T03:04:05.123456", out.str());

        out.clear();
        withFraction.getString(out, false);
        CPPUNIT_ASSERT_EQUAL_STR("2026-07-22T03:04:05.123456", out.str());

        out.clear();
        withFraction.getTimeString(out, false);
        CPPUNIT_ASSERT_EQUAL_STR("03:04:05.123456", out.str());

        out.clear();
        withFraction.getDateString(out, false);
        CPPUNIT_ASSERT_EQUAL_STR("2026-07-22", out.str());

        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibDateTimeFormatTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibDateTimeFormatTest, "JlibDateTimeFormatTest");

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

    class SemCallback final : public CSimpleInterfaceOf<IAsyncCallback>
    {
    public:
        virtual bool onAsyncComplete(int result) override
        {
            sem.signal();
            return true;
        };

    public:
        Semaphore sem{SYNC_LOCATION};
    };

public:
    void testcallback()
    {
        START_TEST

        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createURingProcessor(config, true);
        if (!processor)
            return;

        SemCallback hello;
        processor->enqueueCallbackCommand(hello);
        hello.sem.wait();

        END_TEST
    }

    void testcallbackNoThread()
    {
        START_TEST

        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createURingProcessor(config, false);
        if (!processor)
            return;

        SemCallback hello;
        processor->enqueueCallbackCommand(hello);
        while (!hello.sem.wait(0))
            processor->checkForCompletions();

        END_TEST
    }

    void testcallback2NoThread()
    {
        START_TEST

        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createURingProcessor(config, false);
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

        END_TEST
    }

    void testcallback2NoThreadDelay()
    {
        START_TEST

        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createURingProcessor(config, false);
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

        END_TEST
    }

    void testcallbacks()
    {
        START_TEST

        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring/>");
        Owned<IAsyncProcessor> processor = createURingProcessor(config, true);
        if (!processor)
            return;

        // Test multiple actions being triggered by a single submission
        SemCallback action1;
        SemCallback action2;
        processor->enqueueCallbackCommands(std::vector<IAsyncCallback *>{&action1, &action2});
        action1.sem.wait();
        action2.sem.wait();

        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IOURingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IOURingTest, "IOURingTest");


#endif // _USE_CPPUNIT
