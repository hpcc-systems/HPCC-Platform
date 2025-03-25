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
    virtual Continuation visitAttribute(EventAttr id) override
    {
        return visitor->visitAttribute(id);
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
    virtual void leaveFile(uint32_t bytesRead) override
    {
        return visitor->leaveFile(bytesRead);
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
            case EvAttrSysStartTimestamp:
                value = 1000;
                break;
            case EvAttrSysOffsetNs:
                value = 10;
                break;
            case EvAttrSysThreadId:
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
            CPPUNIT_ASSERT(recorder.stopRecording());
            CPPUNIT_ASSERT(!recorder.isRecording());

            // Check that stopping again fails
            CPPUNIT_ASSERT(!recorder.stopRecording());

            // Restart recording with a different filename
            CPPUNIT_ASSERT(recorder.startRecording("threadid", "testfile.bin", true));
            CPPUNIT_ASSERT(!recorder.isRecording());
            recorder.pauseRecording(true, false);
            CPPUNIT_ASSERT(!recorder.isRecording());
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

            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 987);
            recorder.recordDaliDisconnect(987);

            // Stop recording again
            CPPUNIT_ASSERT(recorder.stopRecording());
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
            CPPUNIT_ASSERT(recorder.stopRecording());
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
                CPPUNIT_ASSERT(recorder.stopRecording());
            }

            ForEachItemIn(t2, threads)
                threads.item(t2).join();

            if (!delay)
                CPPUNIT_ASSERT(recorder.stopRecording());

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
attribute: SysFileSize = 16
attribute: SysStartTimestamp = 1000
attribute: SysOption = 'traceid'
attribute: SysOption = 'threadid'
attribute: SysOption = 'stack'
bytesRead: 16
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            CPPUNIT_ASSERT(recorder.stopRecording());
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
attribute: SysFileSize = 87
attribute: SysStartTimestamp = 1000
attribute: SysOption = 'traceid'
attribute: SysOption = 'threadid'
attribute: SysOption = 'stack'
event: IndexEviction
attribute: SysOffsetNs = 10
attribute: SysTraceId = '00000000000000000000000000000000'
attribute: SysThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: ExpandedSize = 4567
attribute: None
bytesRead: 87
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            CPPUNIT_ASSERT(recorder.stopRecording());
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
attribute: SysFileSize = 233
attribute: SysStartTimestamp = 1000
attribute: SysOption = 'traceid'
attribute: SysOption = 'threadid'
attribute: SysOption = 'stack'
event: IndexEviction
attribute: SysOffsetNs = 10
attribute: SysTraceId = '00000000000000000000000000000000'
attribute: SysThreadId = 100
attribute: FileId = 12345
attribute: FileOffset = 67890
attribute: NodeKind = 0
attribute: ExpandedSize = 4567
attribute: None
event: DaliConnect
attribute: SysOffsetNs = 10
attribute: SysTraceId = '00000000000000000000000000000000'
attribute: SysThreadId = 100
attribute: Path = '/Workunits/Workunit/abc.wu'
attribute: ConnectId = 98765
attribute: None
event: DaliDisconnect
attribute: SysOffsetNs = 10
attribute: SysTraceId = '00000000000000000000000000000000'
attribute: SysThreadId = 100
attribute: ConnectId = 98765
attribute: None
bytesRead: 233
)!!!";
            EventRecorder& recorder = queryRecorder();
            CPPUNIT_ASSERT(recorder.startRecording("all=true", "eventtrace.evt", false));
            CPPUNIT_ASSERT(recorder.isRecording());
            recorder.recordIndexEviction(12345, 67890, NodeBranch, 4567);
            recorder.recordDaliConnect("/Workunits/Workunit/abc.wu", 98765);
            recorder.recordDaliDisconnect(98765);
            CPPUNIT_ASSERT(recorder.stopRecording());
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

#endif // _USE_CPPUNIT
