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
#include <memory>
#include <random>
#include <vector>

#include "jsem.hpp"
#include "jfile.hpp"
#include "jevent.hpp"

#include "unittests.hpp"


enum { NodeBranch, NodeLeaf };

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x ? x : ""),std::string(y ? y : ""))

class CNoOpEventVisitorDecorator : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return visitor->visitFile(filename, version);
    }
    virtual Continuation visitEvent(EventType id) override
    {
        return visitor->visitEvent(id);
    }
    virtual Continuation visitAttribute(EventAttr id, const char * value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual Continuation visitAttribute(EventAttr id, bool value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual Continuation visitAttribute(EventAttr id, uint8_t value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual Continuation visitAttribute(EventAttr id, uint16_t value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual Continuation visitAttribute(EventAttr id, uint32_t value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual Continuation visitAttribute(EventAttr id, uint64_t value) override
    {
        return visitor->visitAttribute(id, value);
    }
    virtual bool departEvent() override
    {
        return visitor->departEvent();
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
    virtual Continuation visitAttribute(EventAttr id, uint64_t value) override
    {
        switch (id)
        {
            case EvAttrRecordedTimestamp:
                value = 1000;
                break;
            case EvAttrEventTimeOffset:
                value = 10;
                break;
            case EvAttrEventThreadId:
                value = 100;
                break;
        }
        return CNoOpEventVisitorDecorator::visitAttribute(id, value);
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
            recorder.recordIndexLookup(1, branchOffset, NodeBranch, true);
            recorder.recordIndexLookup(1, nodeSize, NodeLeaf, false);
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
            recorder.recordIndexLookup(2, 400, NodeLeaf, false);
            recorder.recordIndexLookup(1, 800, NodeLeaf, false);

            recorder.pauseRecording(false, true);
            CPPUNIT_ASSERT(recorder.isRecording());

            // Record more events
            recorder.recordIndexLookup(2, 400, NodeLeaf, false);
            recorder.recordIndexLookup(1, 800, NodeLeaf, false);
            recorder.recordIndexLoad(2, 500, NodeLeaf, 2048, 600, 400);
            recorder.recordIndexLoad(1, 800, NodeLeaf, 2048, 600, 400);
            recorder.recordIndexLookup(1, 800, NodeLeaf, true);
            recorder.recordIndexLookup(1, 1200, NodeLeaf, false);
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
                recorder.recordIndexLookup(1, i*nodeSize, NodeLeaf, false);
                recorder.recordIndexLoad(1, i*nodeSize, NodeLeaf, nodeSize*8, 500, 300);
            }

            // Stop recording
            EventRecordingSummary summary;
            CPPUNIT_ASSERT(recorder.stopRecording(&summary));
            CPPUNIT_ASSERT(!recorder.isRecording());
            CPPUNIT_ASSERT_EQUAL(200'000U, summary.numEvents);
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
                recorder.recordIndexLookup(id, i*nodeSize, NodeLeaf, false);
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

            if (delay)
            {
                MilliSleep(delay);
                CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
            }

            ForEachItemIn(t2, threads)
                threads.item(t2).join();

            if (!delay)
                CPPUNIT_ASSERT(recorder.stopRecording(nullptr));

            CPPUNIT_ASSERT(!recorder.isRecording());
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
        //Test reading an empty file
        try
        {
            static const char* expect=R"!!!(name: eventtrace.evt
version: 1
attribute: RecordedFileSize = 16
attribute: RecordedTimestamp = 1000
attribute: RecordedOption = 'traceid'
attribute: RecordedOption = 'threadid'
attribute: RecordedOption = 'stack'
bytesRead: 16
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
            std::stringstream out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL(std::string(expect), out.str());
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
            static const char* expect = R"!!!(name: eventtrace.evt
version: 1
attribute: RecordedFileSize = 71
attribute: RecordedTimestamp = 1000
attribute: RecordedOption = 'traceid'
attribute: RecordedOption = 'threadid'
attribute: RecordedOption = 'stack'
event: IndexEviction
attribute: EventTimeOffset = 10
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: ExpandedSize = 4567
departEvent
bytesRead: 71
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
            std::stringstream out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL(std::string(expect), out.str());
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
            static const char* expect = R"!!!(name: eventtrace.evt
version: 1
attribute: RecordedFileSize = 156
attribute: RecordedTimestamp = 1000
attribute: RecordedOption = 'traceid'
attribute: RecordedOption = 'threadid'
attribute: RecordedOption = 'stack'
event: IndexEviction
attribute: EventTimeOffset = 10
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: ExpandedSize = 4567
departEvent
event: DaliConnect
attribute: EventTimeOffset = 10
attribute: EventTraceId = '00000000000000000000000000000000'
attribute: EventThreadId = 100
attribute: Path = '/Workunits/Workunit/abc.wu'
attribute: ConnectId = 98765
attribute: ElapsedTime = 100
attribute: DataSize = 73
departEvent
bytesRead: 156
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 98765, 100, 73);
            CPPUNIT_ASSERT(recorder.stopRecording(nullptr));
            std::stringstream out;
            Owned<IEventVisitor> visitor = createVisitor(out);
            CPPUNIT_ASSERT(visitor.get());
            CPPUNIT_ASSERT(readEvents("eventtrace.evt", *visitor));
            CPPUNIT_ASSERT_EQUAL(std::string(expect), out.str());
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

    IEventVisitor* createVisitor(std::stringstream& out)
    {
        Owned<IEventVisitor> visitor = createVisitTrackingEventVisitor(out);
        return new MockEventVisitor(*visitor);
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

#endif // _USE_CPPUNIT
