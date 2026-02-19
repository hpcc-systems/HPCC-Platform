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
        CPPUNIT_TEST(testFailCreate);
        CPPUNIT_TEST(testAllRecordFunctions);
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
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(4U, summary.numEvents);

            // Check that stopping again fails
            CPPUNIT_ASSERT(!recorder.stopRecording(nullptr));

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
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(recorder.startRecording("threadid", "/home/nonexistantuser/eventtrace.evt", nullptr, 0, 0, 0, false), "Expected startRecording() to throw an exception");

        //Check that the recorder has been left in a good state, and a subsequent recording works
        CPPUNIT_ASSERT(!recorder.isRecording());
        CPPUNIT_ASSERT(recorder.startRecording("threadid", "eventtrace.evt", nullptr, 0, 0, 0, false));
        CPPUNIT_ASSERT(recorder.isRecording());
        CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
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
            CPPUNIT_ASSERT(recorder.startRecording("all,compress(lz4hc)", "eventtrace.evt", nullptr, 0, 0, 0, false));
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
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
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

            CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary));
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

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary));
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

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary));
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

        CPPUNIT_ASSERT_MESSAGE("Should be able to stop recording", recorder.stopRecording(&summary));
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

            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
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

    void testAllRecordFunctions()
    {
        START_TEST

        EventRecorder &recorder = queryRecorder();
        EventRecordingSummary summary;

        // Record a variety of events to a test file
        CPPUNIT_ASSERT(recorder.startRecording("all", "pullevents.evt", "testprocess", 1, 2, 3, false));
        CPPUNIT_ASSERT(recorder.isRecording());

        // Add a demo call to each of the record() functions
        // Index-related events
        recorder.recordIndexCacheHit(1, 8192, NodeBranch, 1024, 100);
        recorder.recordIndexCacheMiss(2, 16384, NodeLeaf);
        recorder.recordIndexLoad(3, 24576, NodeBranch, 2048, 150, 200);
        recorder.recordIndexEviction(4, 32768, NodeLeaf, 4096);
        recorder.recordIndexPayload(5, 40960, true, 250);

        // Dali-related events
        recorder.recordDaliChangeMode(1001, 100, 256);
        recorder.recordDaliCommit(1002, 200, 512);
        recorder.recordDaliConnect("/Workunits/Workunit/test.wu", 1003, 300, 1024);
        recorder.recordDaliEnsureLocal(1004, 150, 128);
        recorder.recordDaliGet(1005, 250, 256);
        recorder.recordDaliGetChildren(1006, 175, 512);
        recorder.recordDaliGetChildrenFor(1007, 225, 768);
        recorder.recordDaliGetElements("/Test/Path/Elements", 1008, 275, 1536);
        recorder.recordDaliSubscribe("/Test/Subscribe/Path", 1009, 125);

        // File information
        recorder.recordFileInformation(100, "testfile.idx");

        // Query events
        recorder.recordQueryStart("TestQuery");
        recorder.recordQueryStop();

        // Recording source (additional call to test multiple sources)
        recorder.recordRecordingSource("anotherprocess", 10, 20, 30);

        // Generic event using recordEvent
        CEvent event;
        event.reset(EventIndexCacheMiss);
        event.setValue(EvAttrFileId, 99U);
        event.setValue(EvAttrFileOffset, 49152ULL);
        event.setValue(EvAttrNodeKind, (unsigned)NodeLeaf);
        recorder.recordEvent(event);

        CPPUNIT_ASSERT(recorder.stopRecording(&summary));
        CPPUNIT_ASSERT(!recorder.isRecording());

        END_TEST
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
