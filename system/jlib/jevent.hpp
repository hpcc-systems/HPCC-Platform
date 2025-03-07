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

#include "jscm.hpp"
#include "jatomic.hpp"
#include "jbuff.hpp"

// The order should not be changed, or items removed. New values should always be appended before EventMax
// The meta prefix is used when there are records that provide extra meta data to help interpret
// the event data e.g. mapping file ids to filenames.
enum EventType : byte
{
    EventNone,
    EventIndexLookup,
    EventIndexLoad,
    EventIndexEviction,
    EventDaliConnect,
    EventDaliRead,
    EventDaliWrite,
    EventDaliDisconnect,
    MetaFileInformation,          // information about a file
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
    EvAttrMax
};

extern jlib_decl const char * queryEventName(EventType event);
extern jlib_decl const char * queryEventAttributeName(EventAttr attr);

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

    bool isActive() const { return recordingEvents.load(std::memory_order_acquire); }

    void startRecording(const char * optionsText, const char * filename);
    void stopRecording();

//Functions for each of the events that can be recorded..
    void recordIndexLookup(unsigned fileid, offset_t offset, byte nodeKind, bool hit);
    void recordIndexLoad(unsigned fileid, offset_t offset, byte nodeKind, size32_t size, __uint64 elapsedTime, __uint64 readTime);
    void recordIndexEviction(unsigned fileid, offset_t offset, byte nodeKind, size32_t size);

    void recordDaliConnect(const char * path, __uint64 id);
    void recordDaliDisconnect(__uint64 id);

    //-------------------------- End of the public interface --------------------------

protected:
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

    std::atomic<bool> recordingEvents{false};
    offset_type nextOffset{0};
    offset_type nextWriteOffset{0};
    unsigned counts[numBlocks] = {0};
    cycle_t startCycles{0};
    MemoryAttr buffer;
    CriticalSection cs;
    unsigned sizeMessageHeaderFooter{0};
    unsigned options{0};
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
inline bool recordingEvents() { return EventRecorderInternal::eventRecorder.isActive(); }

#endif
