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

#ifndef JEVENT_HPP
#define JEVENT_HPP

#include <ostream>

#include "jscm.hpp"
#include "jatomic.hpp"
#include "jbuff.hpp"
#include "jstring.hpp"
#include "jstats.h"

// The order should not be changed, or items removed. New values should always be appended before EventMax
// The meta prefix is used when there are records that provide extra meta data to help interpret
// the event data e.g. mapping file ids to filenames.
enum EventType : byte
{
    EventNone,
    EventIndexLookup,
    EventIndexLoad,
    EventIndexEviction,
    EventDaliChangeMode,
    EventDaliCommit,
    EventDaliConnect,
    EventDaliEnsureLocal,
    EventDaliGet,
    EventDaliGetChildren,
    EventDaliGetChildrenFor,
    EventDaliGetElements,
    EventDaliSubscribe,
    MetaFileInformation,          // information about a file
    EventRecordingActive,         // optional event to indicate that recording was suspended/re-enabled
    EventMax
};

// The attributes that can be associated with each event
// The order should not be changed, or items removed.  New values should always be appended before EvAttrMax
enum EventAttr : byte
{
    EvAttrNone,
    EvAttrFileId,
    EvAttrFileOffset,
    EvAttrNodeKind,
    EvAttrReadTime,
    EvAttrElapsedTime,
    EvAttrExpandedSize,
    EvAttrInCache,
    EvAttrPath,
    EvAttrConnectId,
    EvAttrEnabled,
    EvAttrRecordedFileSize,
    EvAttrRecordedTimestamp,
    EvAttrRecordedOption,
    EvAttrEventTimeOffset,
    EvAttrEventTraceId,
    EvAttrEventThreadId,
    EvAttrEventStackTrace,
    EvAttrDataSize,
    EvAttrMax
};

extern jlib_decl const char * queryEventName(EventType event);
extern jlib_decl const char * queryEventAttributeName(EventAttr attr);

struct jlib_decl EventRecordingSummary
{
    unsigned numEvents{0};
    offset_t totalSize{0};
    StringBuffer filename;
};

//---------------------------------------------------------------------------------------------------------------------
enum EventType : byte;
enum EventAttr : byte;
interface IFileIO;

// The following class is used to record events that occur during execution.
// More details are in the cpp file before the constructor.
//
// Design goals:
// - To have least possible impact if the recording is not enabled
// - To have a minimal impact when recording is enabled
//
// There is a single public function for each event that needs to be recorded.  The overhead of
// the functional call can be avoided by checking recordingEvents() first, but calls will be
// ignored if recording is not enabled.
class jlib_decl EventRecorder
{
    typedef unsigned __int64 offset_type; // sufficient for an offset into the buffer, with little chance of it wrapping
public:
    EventRecorder();

    bool isRecording() const { return recordingEvents.load(std::memory_order_acquire); }    // Are events being recorded? false if recording is paused

    bool startRecording(const char * optionsText, const char * filename, bool pause);
    bool stopRecording(EventRecordingSummary * optSummary);
    bool pauseRecording(bool pause, bool recordChange);

//Functions for each of the events that can be recorded..
    void recordIndexLookup(unsigned fileid, offset_t offset, byte nodeKind, bool hit);
    void recordIndexLoad(unsigned fileid, offset_t offset, byte nodeKind, size32_t size, __uint64 elapsedTime, __uint64 readTime);
    void recordIndexEviction(unsigned fileid, offset_t offset, byte nodeKind, size32_t size);

    void recordDaliChangeMode(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliCommit(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliConnect(const char * xpath, __int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliEnsureLocal(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliGet(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliGetChildren(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliGetChildrenFor(__int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliGetElements(const char * path, __int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliSubscribe(const char * xpath, __int64 id, stat_type elapsedNs);

    void recordFileInformation(unsigned fileid, const char * filename);

    //-------------------------- End of the public interface --------------------------

protected:
    void recordRecordingActive(bool paused);
    void recordDaliEvent(EventType event, const char * xpath, __int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliEvent(EventType event, __int64 id, stat_type elapsedNs, size32_t dataSize);

    void checkAttrValue(EventAttr attr, size_t size);

    void writeEventHeader(EventType type, offset_type & offset);
    void writeEventFooter(offset_type & offset, size32_t requiredSize, offset_t writeOffset);

    template <class T>
    void write(offset_type & offset, T value)
    {
        writeData(offset, sizeof(value), &value);
    }

    void write(offset_type & offset, const char * value)
    {
        writeData(offset, strlen(value)+1, value);
    }

    template <class T>
    void write(EventRecorder::offset_type & offset, EventAttr attr, T value)
    {
        checkAttrValue(attr, sizeof(value));        // MORE: make this debug only....
        writeData(offset, sizeof(attr), &attr);
        writeData(offset, sizeof(value), &value);
    }

    void write(EventRecorder::offset_type & offset, EventAttr attr, const char * value)
    {
        writeData(offset, sizeof(attr), &attr);
        writeData(offset, strlen(value)+1, value);
    }

    void writeByte(offset_type & offset, byte value);
    void writeData(offset_type & offset, size_t size, const void * data);

    offset_type reserveEvent(size32_t size);
    void commitEvent(offset_type startOffset, size32_t size);

    void writeBlock(offset_type startOffset, size32_t size);

    unsigned getBlockFromOffset(offset_type offset) { return (unsigned)((offset & bufferMask) / OutputBlockSize); }

protected:
    static constexpr size32_t numBlocks = 2;
    static constexpr size32_t OutputBlockSize = 0x100000;
    static constexpr size32_t OutputBufferSize = numBlocks * OutputBlockSize;
    static constexpr offset_t bufferMask = OutputBufferSize-1;
    static constexpr offset_t blockMask = OutputBlockSize-1;

    std::atomic<bool> recordingEvents{false};       // Are events being recorded? false if recording is paused.
    std::atomic<bool> isStarted{false};             // Use 2 flags for whether started and stopped to ensure clean
    std::atomic<bool> isStopped{true};              // termination in stopRecording()
    offset_type nextOffset{0};
    offset_type nextWriteOffset{0};
    offset_type numEvents{0};
    unsigned pendingEventCounts[numBlocks] = {0};
    cycle_t startCycles{0};
    MemoryAttr buffer;
    CriticalSection cs;
    unsigned sizeMessageHeaderFooter{0};
    unsigned options{0};
    bool outputToLog{false};
    StringBuffer outputFilename;
    Owned<IFileIO> output;
};

// The implementation exposes a global object so that the test for whether events are being recorded
// is as lightweight as possible.
// All access should be via the non-internal helper accessor functions

namespace EventRecorderInternal
{
extern jlib_decl EventRecorder eventRecorder;
}

inline EventRecorder & queryRecorder() { return EventRecorderInternal::eventRecorder; }
inline bool recordingEvents() { return EventRecorderInternal::eventRecorder.isRecording(); }

// Abstraction of the visitor pattern for "reading" binary event data files. The `readEvents`
// function will pass each byte of data contained within a file throwgh exactly one method of
// this interface.
//
// For each compatibile file the visitor can expect:
// 1. One call to visitFile
// 2. One call to visitAttribute for EvAttrRecordedFileSize
// 3. One call to visitAttribute for EvAttrRecordedTimestamp
// 4. Zero or more calls to visitAttribute for EvAttrRecordedOption
// 5. Zero or more sequences of:
//    a. One call to visitEvent
//    b. Zero or more calls to visitAttribute
//    c. One call to departEvent
// 6. One call to departFile
//
// Implementations may implement limited filtering during visitation. All methods, except
// `departFile`, may abort visitation. Both `visitEvent` and `visitAttribute` (in the context of
// an event) may suppress visitation of the remainder of the current event.
//
// Reasons for aborting a file include:
// - unrecognized file version; or
// - (remaining) events out of a target date range; or
// - trace IDs are required but are not present.
//
// Reasons for suppressing an event include:
// - event type not required by current use case; or
// - attribute value not out of range for current use case.
interface IEventVisitor : extends IInterface
{
    enum Continuation {
        visitContinue,
        visitSkipEvent,
        visitSkipFile
    };
    virtual bool visitFile(const char* filename, uint32_t version) = 0;
    virtual Continuation visitEvent(EventType id) = 0;
    virtual Continuation visitAttribute(EventAttr id, const char * value) = 0;
    virtual Continuation visitAttribute(EventAttr id, bool value) = 0;
    virtual Continuation visitAttribute(EventAttr id, uint8_t value) = 0;
    virtual Continuation visitAttribute(EventAttr id, uint16_t value) = 0;
    virtual Continuation visitAttribute(EventAttr id, uint32_t value) = 0;
    virtual Continuation visitAttribute(EventAttr id, uint64_t value) = 0;
    virtual bool departEvent() = 0;
    virtual void departFile(uint32_t bytesRead) = 0;
};

// Return a new event visitor that writes to standard output for every visit.
// `visitFile` adds two lines of text, and all other methods add one line of text.
extern jlib_decl IEventVisitor* createVisitTrackingEventVisitor();

// Return a new event visitor that writes to the given output stream for every visit.
// `visitFile` adds two lines of text, and all other methods add one line of text.
extern jlib_decl IEventVisitor* createVisitTrackingEventVisitor(std::ostream& out);

// Opens and parses a single binary event data file. Parsed data is passed to the given visitor
// until parsing completes or the visitor requests it to stop.
//
// Exceptions are thrown on error. False is returned if parsing was stopped prematurely. True is
// returned if all data was parsed successfully.
extern jlib_decl bool readEvents(const char* filename, IEventVisitor & visitor);

#endif
