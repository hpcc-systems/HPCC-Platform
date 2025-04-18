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

#include "platform.h"
#include "jevent.hpp"

#include "jdebug.hpp"
#include "jstream.hpp"
#include "jthread.hpp"
#include "jtrace.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jstring.hpp"
#include <set>
#include "jlzw.hpp"

// Should be increased if the file format changes
// Should be increased whenever new attributes are added - unless attribute types are specified in the file
const static unsigned currentVersion = 1;

static_assert(EvAttrMax <= 128, "Event attributes >=128.  Review the format to decide whether version should change or packed integers used");

//The following flags are used to control which extra pieces of information should be recorded with each event
//Meta information does not include the extra trace/thread ids etc.
//These values are persisted in the file, so should not be changed.
enum EventFlags : unsigned
{
    ERFnone         = 0x00000000,
    ERFtraceid      = 0x00000001,
    ERFthreadid     = 0x00000002,
    ERFstacktrace   = 0x00000004,   // serialize the stacktrace???
};
BITMASK_ENUM(EventFlags);

static constexpr unsigned defaultEventFlags = ERFthreadid;

inline void TRACEEVENT(char const * format, ...) __attribute__((format(printf, 1, 2)));
inline void TRACEEVENT(char const * format, ...)
{
    va_list args;
    va_start(args, format);
    VALOG(MCmonitorEvent, format, args);
    va_end(args);
}

constexpr const char magicHeader[4] = { 'H','E','V','T' };

//---------------------------------------------------------------------------------------------------------------------
//
// Meta information about events and attributes.

struct EventInformation
{
    EventType type;             // the enumeration value
    const char * name;          // text representation of the event
    bool isMeta;                // Is this meta data (see description below) rather than an event
    EventType pairedEvent;      // could be used to automaticaly link start and finish events?
};


#define DEFINE_EVENT(event) { Event##event, #event, false, EventNone }
#define DEFINE_META(meta) { Meta##meta, #meta, true, EventNone }

static constexpr EventInformation eventInformation[] {
    DEFINE_EVENT(None),
    DEFINE_EVENT(IndexLookup),
    DEFINE_EVENT(IndexLoad),
    DEFINE_EVENT(IndexEviction),
    DEFINE_EVENT(DaliChangeMode),
    DEFINE_EVENT(DaliCommit),
    DEFINE_EVENT(DaliConnect),
    DEFINE_EVENT(DaliEnsureLocal),
    DEFINE_EVENT(DaliGet),
    DEFINE_EVENT(DaliGetChildren),
    DEFINE_EVENT(DaliGetChildrenFor),
    DEFINE_EVENT(DaliGetElements),
    DEFINE_EVENT(DaliSubscribe),
    DEFINE_META(FileInformation),
    DEFINE_EVENT(RecordingActive),
};
static_assert(_elements_in(eventInformation) == EventMax);

// The different types of attributes that can be associated with an event.
enum EventAttrType
{
    EATnone,
    EATbool,
    EATu1,
    EATu2,
    EATu4,
    EATu8,
    EATtimestamp,
    EATstring,
    EATtraceid,     // should probably delete...
    EATmax
};

static constexpr unsigned attrTypeSizes[] = { 0, 1, 1, 2, 4, 8, 8, 0, 32 };
static_assert(_elements_in(attrTypeSizes) == EATmax);

struct EventAttrInformation
{
    EventAttr attr;
    const char * name;
    EventAttrType type;
    unsigned size;
};

#define DEFINE_ATTR(tag, type) { EvAttr##tag, #tag, EAT##type, attrTypeSizes[EAT##type] }

static constexpr EventAttrInformation attrInformation[] = {
    DEFINE_ATTR(None, none),
    DEFINE_ATTR(FileId, u4),
    DEFINE_ATTR(FileOffset, u8),
    DEFINE_ATTR(NodeKind, u1),
    DEFINE_ATTR(ReadTime, u8),
    DEFINE_ATTR(ElapsedTime, u8),
    DEFINE_ATTR(ExpandedSize, u4),
    DEFINE_ATTR(InCache, bool),
    DEFINE_ATTR(Path, string),
    DEFINE_ATTR(ConnectId, u8),
    DEFINE_ATTR(Enabled, bool),
    DEFINE_ATTR(FileSize, u8),
    DEFINE_ATTR(RecordedTimestamp, u8),
    DEFINE_ATTR(RecordedOption, string),
    DEFINE_ATTR(EventTimeOffset, u8),
    DEFINE_ATTR(EventTraceId, string),
    DEFINE_ATTR(EventThreadId, u8),
    DEFINE_ATTR(EventStackTrace, string),
    DEFINE_ATTR(DataSize, u4),
};

static_assert(_elements_in(attrInformation) == EvAttrMax);


const char * queryEventName(EventType event)
{
    assertex(event < EventMax);
    return eventInformation[event].name;
}

const char * queryEventAttributeName(EventAttr attr)
{
    assertex(attr < EvAttrMax);
    return attrInformation[attr].name;
}

//---------------------------------------------------------------------------------------------------------------------

// The following class is used to record events that occur during execution.  Example uses cases are starting roxie,
// while roxie queries are running, while esp is processing service calls etc..
//
// The principle use case is to record every index node lookup, load and eviction in roxie so that we can
// understand details of which parts of the files are acccessed most often, and the effectiveness of the cache.
//
// Design goals:
// - To have least possible impact if the recording is not enabled
// - To have a minimal impact when recording is enabled
// - To be flexible so it can be used for other events e.g. dali access, file access in the future
// - To be able to record events from multiple threads without significant thread contention
// - To keep the size of the binary file relatively small.
//
// How do you balance the need for a small binary file with the cost of packing fields
//
// Solution adopted is to write fields binary, with no packing, but LZ4 compress the buffers before writing to disk.
//
// How do you balance being able to interpret the data stream without having to know what each message means?
//
// The events are recorded as a sequence of (tag, values) - details given below.
// Currently each tag has an associated type - which includes the size - rather than saving the size in the file.
//
// The output is a binary file containing a sequence of events and meta data interspersed together.
// The meta data is used to provide extra infromation for example mapping from the ids used in the events to filenames.
//
// There is also a separate tool/process which reads the binary file and generates a structured, human readable
// form from it.
//
// If the recorder understands the events it makes it easy to translate them into a more structured form, but
// may break the encapsulation about what is being recorded.
//
// Mechanism for buffering:
// - There is a single circular shared buffer for all the threads.  This simplifies writing and reading,
//   but introduces potential contention.
// - Each thread requests memory in the buffer, writes data into the space, and then commits the data
// - The buffer is broken into multiple blocks, and data is written to disk when a block is complete.
//
// How can you tell when it is possible to write data to disk.
// - You can't commit it when that last byte of data is written to the block - because other threads may
//   be waiting.  Similarly you can't write when the last event in a block is complete.
// - You cannot use a count of active events getting to zero on its own, and it may not ever happen
// - It is hard to do it lock free - although could look at other queueing code for ideas.
//
// Solution adopted:
// - When reserving space, increase the count associated with that particular block
// - When commiting, decrement the count.  If it is zero, and the next allocation block differs friom this
//   then that was the last transaction.
//
// Adding blocking to the mix:  Likely to be needed for the full solution (for ensuring meta data is not dropped)
// - Introduce a maxWriteOffset, defaulting to the size of the buffer
// - When allocating space, if the offset is greater than the maxWriteOffset, then increment a blocked count,
//   and wait on a semaphore outside a critical section
// - When writing a chunk of buffer to disk, update the maxWriteOffset, and if the blocked count is non-zero
//   then signal the semaphore that many times.
// - Alternatively reserveEvent() could return -1 and the event could be dropped - probably better.
//
// File format:
// NOTE: This format is liable to change without warning until the first stable version is released.
//       No backward compatibility is currently guaranteed.
//
// version:         4 bytes - which version of the file
// options:         4 bytes - EventFlags defined above which control which extra pieces of information should be recorded with each event
// timestamp:       8 bytes - the timestamp (in ns) when the recording started
//
// events:
//   eventType:     1 byte - which event from the EventType enumeration
//   timestamp:     8 bytes - the time (in ns) since the start of recording that this event occured
//   traceId:  opt 32 bytes - the otel trace id for the query that recorded the event
//   threadId: opt  8 bytes - the id of the thread that recorded the event
//   attributes:
//     attribute:   1 byte - which attribute, from the EventAttr enumeration.  EventNone (0) marks the end of the attributes.
//     value:       variable - the value of the attribute depends on the type
//
// If the data is compressed, then the event stream is compressed as a sequence of
//
//   uncompressedSize   4 bytes
//   compressedSize     4 bytes
//   compressedData     compressedSize bytes
//
// The compression should be implemented by the streamed writing code.
//
// Should there be a terminator at the end of the file?  If will potentially cause problems if the file is compressed...

EventRecorder::EventRecorder() : buffer(OutputBufferSize), compressionType(COMPRESS_METHOD_LZ4)
{
}

void EventRecorder::checkAttrValue(EventAttr attr, size_t size)
{
    unsigned expectedSize = attrInformation[attr].size;
    assertex(expectedSize == 0 || expectedSize == size);
}

bool EventRecorder::startRecording(const char * optionsText, const char * filename, bool pause)
{
    assertex(filename);
    CriticalBlock block(cs);
    if (!isStopped)
        return false;
    assertex(!isStarted);
    isStarted = true;
    isStopped = false;

    auto processOption = [this](const char * option, const char * valueText)
    {
        bool valueBool = strToBool(valueText);

        if (strieq(option, "traceid"))
            options = (options & ~ERFtraceid) | (valueBool ? ERFtraceid : ERFnone);
        else if (strieq(option, "threadid"))
            options = (options & ~ERFthreadid) | (valueBool ? ERFthreadid : ERFnone);
        else if (strieq(option, "stack"))
            options = (options & ~ERFstacktrace) | (valueBool ? ERFstacktrace : ERFnone);
        else if (strieq(option, "all"))
            options = (valueBool ? ~ERFnone : ERFnone);
        else if (strieq(option, "compress"))
        {
            if (isdigit(*valueText))
            {
                compressionType = (byte) atoi(valueText);
                if (compressionType >= COMPRESS_METHOD_LAST)
                    compressionType = COMPRESS_METHOD_LZ4;
            }
            else
            {
                ICompressHandler * compression = queryCompressHandler(valueText);
                if (compression)
                    compressionType = compression->queryMethod();
            }
        }
        else if (strieq(option, "log"))
            outputToLog = valueBool;
    };

    options = defaultEventFlags;
    outputToLog = false;
    compressionType = COMPRESS_METHOD_LZ4;
    corruptOutput = false;

    processOptionString(optionsText, processOption);
    sizeMessageHeaderFooter = sizeof(EventType) + sizeof(__uint64) + sizeof(EventAttr); // event type, timestamp and end of attributes marker
    if (options & ERFthreadid)
        sizeMessageHeaderFooter += sizeof(__uint64);
    if (options & ERFtraceid)
        sizeMessageHeaderFooter += 16;

    outputFilename.set(filename);
    outputFile.setown(createIFile(filename));
    output.setown(outputFile->open(IFOcreate));

    Owned<ISerialOutputStream> diskStream = createSerialOutputStream(output);
    Owned<IBufferedSerialOutputStream> bufferedDiskStream = createBufferedOutputStream(diskStream, 0x100000);

    //Write the uncompressed header:
    bufferedDiskStream->put(sizeof(magicHeader), magicHeader);
    bufferedDiskStream->put(sizeof(currentVersion), &currentVersion);
    bufferedDiskStream->put(sizeof(compressionType), &compressionType);

    if (compressionType != COMPRESS_METHOD_NONE)
    {
        ICompressHandler * compressHandler = queryCompressHandler((CompressionMethod)compressionType);
        const char *compressOptions = nullptr; // at least for now!
        Owned<ICompressor> compressor = compressHandler->getCompressor(compressOptions);
        Owned<ISerialOutputStream> compressedStream = createCompressingOutputStream(bufferedDiskStream, compressor);
        outputStream.set(compressedStream);
    }
    else
        outputStream.set(bufferedDiskStream);

    __uint64 startTimestamp = getTimeStampNowValue()*1000;
    numEvents = 0;
    startCycles.store(get_cycles_now(), std::memory_order_release);

    //Revisit: If the file is being compressed, then these fields should be output uncompressed at the head
    offset_type pos = 0;
    write(pos, options);
    write(pos, startTimestamp);
    nextOffset = pos;
    nextWriteOffset = 0;
    for (unsigned i=0; i < numBlocks; i++)
        pendingEventCounts[i] = 0;

    recordingEvents.store(!pause, std::memory_order_release);
    return true;
}

bool EventRecorder::stopRecording(EventRecordingSummary * optSummary)
{
    {
        CriticalBlock block(cs);
        if (!isStarted)
            return false;
        isStarted = false;
        assertex(!isStopped);
    }

    //MORE: Protect against startRecording() being called concurrently, by introducing another boolean to
    //indicate if it is active, which is only cleared once this function completes.
    recordingEvents.store(false);

    //Need to wait until all writes have finished
    for (unsigned iter=0; iter < 1000; iter++)
    {
        {
            CriticalBlock block(cs);
            if (getBlockFromOffset(nextOffset) == getBlockFromOffset(nextWriteOffset))
                break;
        }
        MilliSleep(10);
    }

    {
        //MORE: Could avoid re-entering by using a leaveable critical block above
        CriticalBlock block(cs);
        if (getBlockFromOffset(nextOffset) != getBlockFromOffset(nextWriteOffset))
            ERRLOG("Inconsistent data write %llu v %llu", nextOffset, nextWriteOffset);

        writeBlock(nextOffset, nextOffset & blockMask);

        outputStream->flush();
        outputStream.clear();

        //Flush the data, after waiting for a little while (or until committed == offset)?
        if (optSummary)
        {
            if (corruptOutput)
                optSummary->filename.set("Output deleted because it was corrupt");
            else
                optSummary->filename.set(outputFilename);
            optSummary->numEvents = numEvents;
            optSummary->totalSize = output->getStatistic(StSizeDiskWrite);
            optSummary->rawSize = nextOffset;
        }

        output->close();
        output.clear();

        if (corruptOutput)
            outputFile->remove();

        isStopped = true;
    }

    return true;
}

bool EventRecorder::pauseRecording(bool pause, bool recordChange)
{
    CriticalBlock block(cs);
    if (!isStarted || isStopped)
        return false;

    bool recordingInFuture = !pause;
    if (recordingEvents != recordingInFuture)
    {
        if (recordingInFuture)
            recordingEvents = true;

        if (recordChange)
            recordRecordingActive(recordingInFuture);

        if (!recordingInFuture)
            recordingEvents = false;
    }
    return true;
}

//See notes above about reseving and committing events
EventRecorder::offset_type EventRecorder::reserveEvent(size32_t size)
{
    offset_type offset;
    {
        CriticalBlock block(cs);
        offset = nextOffset;
        nextOffset += size;
        pendingEventCounts[getBlockFromOffset(offset)]++;
        numEvents++;
    }
    return offset;
}

void EventRecorder::commitEvent(offset_type startOffset, size32_t size)
{
    unsigned thisBlock = getBlockFromOffset(startOffset);
    bool commitBlock = false;
    unsigned prevCount;
    {
        CriticalBlock block(cs);

        prevCount = pendingEventCounts[thisBlock];
        if (likely(prevCount != 0))
        {
            unsigned count = prevCount - 1;
            pendingEventCounts[thisBlock] = count;
            if (count == 0)
            {
                if (getBlockFromOffset(nextOffset) != thisBlock)
                    commitBlock = true;
            }
        }
    }
    //This should be asynchronous.  It could send a signal, and another thread could maintain a writeOffset, and
    //write the next chunk of data when it is signalled.
    if (commitBlock)
        writeBlock(startOffset, OutputBlockSize);

    //Sanity check - this should never occur
    assertex(prevCount != 0);
}

//Use parameter pack template functions to simplify calculating the size of the attributes when serialized.
template<typename T>
constexpr size32_t getSizeOfAttr(T arg)
{
    return sizeof(EventAttr) + sizeof(arg);
}

constexpr size32_t getSizeOfAttr(const char * arg)
{
    //strlen is a constexpr in gcc, but not in clang
    return sizeof(EventAttr) + std::char_traits<char>::length(arg) + 1;
}

template<typename... Args>
constexpr size32_t getSizeOfAttrs(Args... args)
{
    return (getSizeOfAttr(args) + ...);
}

static_assert(getSizeOfAttrs(1U, 3ULL) == 2 * sizeof(EventAttr) + 4 + 8);
static_assert(getSizeOfAttrs("gavin") == sizeof(EventAttr) + 6);
static_assert(getSizeOfAttrs(true, 32768U, 1ULL, "boris", "blob") == 5 * sizeof(EventAttr) + 1 + 4 + 8 + 6 + 5);

void EventRecorder::recordRecordingActive(bool enabled)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"RecordingActive\", \"enabled\": %s }", boolToStr(enabled));

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(enabled);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(EventRecordingActive, pos);
    write(pos, EvAttrEnabled, enabled);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordIndexLookup(unsigned fileid, offset_t offset, byte nodeKind, bool hit)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"IndexLookup\", \"file\": %u, \"offset\"=0x%llx, \"kind\": %d, \"hit\": %s }", fileid, offset, nodeKind, boolToStr(hit));

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(fileid, offset, nodeKind, hit);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(EventIndexLookup, pos);
    write(pos, EvAttrFileId, fileid);
    write(pos, EvAttrFileOffset, offset);
    write(pos, EvAttrNodeKind, nodeKind);
    write(pos, EvAttrInCache, hit);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordIndexLoad(unsigned fileid, offset_t offset, byte nodeKind, size32_t size, __uint64 elapsedTime, __uint64 readTime)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"IndexLoad\", \"file\": %u, \"offset\"=0x%llx, \"kind\": %d, \"size\": %u, \"elapsed\": %llu, \"read\": %llu }", fileid, offset, nodeKind, size, elapsedTime, readTime);

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(fileid, offset, nodeKind, size, elapsedTime, readTime);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(EventIndexLoad, pos);
    write(pos, EvAttrFileId, fileid);
    write(pos, EvAttrFileOffset, offset);
    write(pos, EvAttrNodeKind, nodeKind);
    write(pos, EvAttrExpandedSize, size);
    write(pos, EvAttrElapsedTime, elapsedTime);
    write(pos, EvAttrReadTime, readTime);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordIndexEviction(unsigned fileid, offset_t offset, byte nodeKind, size32_t size)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"IndexEviction\", \"file\": %u, \"offset\"=0x%llx, \"kind\": %d, \"size\": %u }", fileid, offset, nodeKind, size);

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(fileid, offset, nodeKind, size);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(EventIndexEviction, pos);
    write(pos, EvAttrFileId, fileid);
    write(pos, EvAttrFileOffset, offset);
    write(pos, EvAttrNodeKind, nodeKind);
    write(pos, EvAttrExpandedSize, size);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordDaliEvent(EventType event, const char * path, __int64 id, stat_type elapsedNs, size32_t dataSize)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"%s\", \"path\": \"%s\", \"id\"=0x%llx, \"elapsedNs\": %llu, \"dataSize\": %u }", queryEventName(event), path, id, elapsedNs, dataSize);

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(path, id, elapsedNs, dataSize);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(event, pos);
    write(pos, EvAttrPath, path);
    write(pos, EvAttrConnectId, id);
    write(pos, EvAttrElapsedTime, elapsedNs);
    write(pos, EvAttrDataSize, dataSize);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordDaliEvent(EventType event, __int64 id, stat_type elapsedNs, size32_t dataSize)
{
    if (!isRecording())
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"%s\", \"id\"=0x%llx, \"elapsedNs\": %llu, \"dataSize\": %u }", queryEventName(event), id, elapsedNs, dataSize);

    //MORE: Should the time stamp be adjusted by the elapsed time??
    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(id, elapsedNs, dataSize);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(event, pos);
    write(pos, EvAttrConnectId, id);
    write(pos, EvAttrElapsedTime, elapsedNs);
    write(pos, EvAttrDataSize, dataSize);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordFileInformation(unsigned fileid, const char * filename)
{
    //Meta data is logged whether or not recording is paused, check that logging is enabled.
    if (!isStarted || isStopped)
        return;

    if (unlikely(outputToLog))
        TRACEEVENT("{ \"name\": \"MetaFileInformation\", \"file\": %u, \"path\"=\"%s\" }", fileid, filename);

    size32_t requiredSize = sizeMessageHeaderFooter + getSizeOfAttrs(fileid, filename);
    offset_type writeOffset = reserveEvent(requiredSize);
    offset_type pos = writeOffset;
    writeEventHeader(MetaFileInformation, pos);
    write(pos, EvAttrFileId, fileid);
    write(pos, EvAttrPath, filename);
    writeEventFooter(pos, requiredSize, writeOffset);
}

void EventRecorder::recordDaliChangeMode(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliChangeMode, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliCommit(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliCommit, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliConnect(const char * path, __int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliConnect, path, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliEnsureLocal(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliEnsureLocal, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliGet(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliGet, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliGetChildren(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliGetChildren, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliGetChildrenFor(__int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliGetChildrenFor, id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliGetElements(const char * path, __int64 id, stat_type elapsedNs, size32_t dataSize)
{
    recordDaliEvent(EventDaliGetElements, path ? path : "", id, elapsedNs, dataSize);
}

void EventRecorder::recordDaliSubscribe(const char * xpath, __int64 id, stat_type elapsedNs)
{
    recordDaliEvent(EventDaliSubscribe, xpath, id, elapsedNs, 0);
}


void EventRecorder::writeEventHeader(EventType type, offset_type & offset)
{
    __uint64 ts = cycle_to_nanosec(get_cycles_now() - startCycles.load(std::memory_order_acquire)); // nanoseconds relative to the start of the recording

    write(offset, type);
    write(offset, ts);

    if (options & ERFtraceid)
    {
        const char * traceid = queryThreadedActiveSpan()->queryTraceId();
        assertex(strlen(traceid) == 32);
        for (unsigned i=0; i < 32; i += 2)
        {
            byte next = getHexPair(traceid + i);
            writeByte(offset, next);
        }
    }
    if (options & ERFthreadid)
    {
        __uint64 threadId = (__uint64)GetCurrentThreadId();
        write(offset, threadId);
    }
}

void EventRecorder::writeEventFooter(offset_type & offset, size32_t requiredSize, offset_t writeOffset)
{
    EventAttr endOfEvent = EvAttrNone;

    write(offset, endOfEvent);
    commitEvent(writeOffset, requiredSize);

    //Sanity check after the event is committed to avoid potential deadlock
    assertex(offset == writeOffset + requiredSize);
}

void EventRecorder::writeData(offset_type & offset, size_t size, const void * data)
{
    const byte * source = (const byte *)data;
    const offset_type startOffset = offset;
    byte * target = (byte *)buffer.mem();
    for (size32_t i=0; i < size; i++)
    {
        offset_type pos = startOffset+i;
        size32_t buffOffset = pos & bufferMask;
        target[buffOffset] = source[i];
    }
    offset += size;
}

void EventRecorder::writeByte(offset_type & offset, byte value)
{
    byte * target = (byte *)buffer.mem();
    size32_t buffOffset = offset & bufferMask;
    target[buffOffset] = value;
    offset++;
}

void EventRecorder::writeBlock(offset_type startOffset, size32_t size)
{
    //MORE: Make this asynchronous - on a background thread.
    offset_t fileOffset = startOffset & ~blockMask;

    //Internal consistency check which should never occur.  It can occur if blocking/discarding
    //is not implemented and the compression takes too long (e.g. lz4hc)
    if (fileOffset != nextWriteOffset)
    {
        OERRLOG("writeBlock: fileOffset %llu, nextWriteOffset %llu", fileOffset, nextWriteOffset);
        //Avoid writing any more data to the output and delete when the recording is stopped
        //aborting recording at this point is too complex
        corruptOutput = true;
    }

    if (!corruptOutput)
    {
        size32_t blockOffset = nextWriteOffset & blockMask;
        outputStream->put(size, (const byte *)buffer.get() + blockOffset);
    }
    nextWriteOffset += size;
}

//---------------------------------------------------------------------------------------------------------------------

//Single static instance of the class
namespace EventRecorderInternal
{
EventRecorder eventRecorder;
}

//---------------------------------------------------------------------------------------------------------------------

class CEventFileReader : public CInterface
{
private:
    Owned<IBufferedSerialInputStream> stream;
    Owned<IFile> file;
    Linked<IEventVisitor> visitor;
    unsigned version{0};
    uint32_t options{0};
    size32_t bytesRead{0};
    bool mute{false};

public:
    bool traverse(const char* filename, IEventVisitor& _visitor)
    {
        file.setown(locateEventFile(filename));
        stream.setown(openEventFileForReading(*file));
        visitor.set(&_visitor);

        //MORE: Need to handle multiple file versions
        if (version != currentVersion)
            throw makeStringExceptionV(-1, "unsupported file version %u (required %u)", version, currentVersion);

        return traverseHeader() && traverseEvents() && traverseFooter();
    }

private:
    inline bool continuing(IEventVisitor::Continuation continuation) const
    {
        return (IEventVisitor::visitContinue == continuation);
    }

    bool traverseHeader()
    {
        __uint64 startTimestamp;

        readToken(options);
        readToken(startTimestamp);
        return (visitor->visitFile(file->queryFilename(), version) &&
            continuing(visitor->visitAttribute(EvAttrRecordedTimestamp, startTimestamp)) &&
            // Pass through information about which of the extra options are provided on each of the event records
            (!(options & ERFtraceid) || continuing(visitor->visitAttribute(EvAttrRecordedOption, "traceid"))) &&
            (!(options & ERFthreadid) || continuing(visitor->visitAttribute(EvAttrRecordedOption, "threadid"))) &&
            (!(options & ERFstacktrace) || continuing(visitor->visitAttribute(EvAttrRecordedOption, "stack"))));
    }

    bool traverseEvents()
    {
        for (;;)
        {
            // no more data means no more events
            size32_t got = 0;
            stream->peek(1, got);
            if (!got)
                break;

            EventType eventType;
            readToken(eventType);
            if (eventType >= EventMax)
                throw makeStringExceptionV(-1, "invalid event type %u", eventType);
            if (!reactToVisit(visitor->visitEvent(eventInformation[eventType].type)))
                return false;

            if ((EventNone != eventType) && !traverseAttributes())
                return false;
        }
        return true;
    }

    bool traverseAttributes()
    {
        if (!finishAttribute<__uint64>(EvAttrEventTimeOffset))
            return false;
        if ((options & ERFtraceid) && !finishDataAttribute(EvAttrEventTraceId, 16))
            return false;
        if ((options & ERFthreadid) && !finishAttribute<__uint64>(EvAttrEventThreadId))
            return false;
        for (;;)
        {
            EventAttr attr;
            readToken(attr);
            if (EvAttrNone == attr)
                return finishEvent();
            if (attr >= EvAttrMax)
                throw makeStringExceptionV(-1, "invalid attribute type %u", attr);
            switch (attrInformation[attr].type)
            {
            case EATnone:
                throw makeStringExceptionV(-1, "no data type for attribute %u", attr);
                break;
            case EATbool:
                if (!finishAttribute<bool>(attr))
                    return false;
                break;
            case EATu1:
                if (!finishAttribute<uint8_t>(attr))
                    return false;
                break;
            case EATu2:
                if (!finishAttribute<uint16_t>(attr))
                    return false;
                break;
            case EATu4:
                if (!finishAttribute<uint32_t>(attr))
                    return false;
                break;
            case EATu8:
            case EATtimestamp:
                if (!finishAttribute<__uint64>(attr))
                    return false;
                break;
            case EATstring:
                if (!finishAttribute(attr))
                    return false;
                break;
            case EATtraceid:
                if (!finishAttribute(attr, 32))
                    return false;
                break;
            }
        }
        return true;
    }

    bool traverseFooter()
    {
        (void)visitor->departFile(bytesRead);
        return true;
    }

    bool finishEvent()
    {
        bool result = true;
        if (!mute)
            result = visitor->departEvent();
        else
            mute = false; // reset for next event
        return result;
    }

    template <typename T>
    bool finishAttribute(EventAttr attr)
    {
        T value;
        readToken(value);
        if (mute)
            return true;
        if (std::is_same<T, bool>::value)
            return reactToVisit(visitor->visitAttribute(attr, bool(value)));
        return reactToVisit(visitor->visitAttribute(attr, __uint64(value)));
    }

    bool finishAttribute(EventAttr attr)
    {
        StringBuffer value;
        readToken(value);
        if (mute)
            return true;
        return reactToVisit(visitor->visitAttribute(attr, value.str()));
    }

    bool finishAttribute(EventAttr attr, size32_t len)
    {
        StringBuffer value;
        readToken(value, len);
        if (mute)
            return true;
        return reactToVisit(visitor->visitAttribute(attr, value.str()));
    }

    //Read as data, but pass through as a hex encoded string
    bool finishDataAttribute(EventAttr attr, size32_t len)
    {
        MemoryAttr buffer(len);
        if (stream->read(len, buffer.mem()) != len)
            throw makeStringExceptionV(-1, "eof before end of %u byte string", len);
        bytesRead += len;

        if (mute)
            return true;

        StringBuffer hexText;
        hexText.ensureCapacity(len*2);
        for (unsigned i=0; i < len; i++)
            hexText.appendhex(buffer.getByte(i), true);
        return reactToVisit(visitor->visitAttribute(attr, hexText.str()));
    }

    bool reactToVisit(IEventVisitor::Continuation result)
    {
        switch (result)
        {
        case IEventVisitor::visitContinue:
            break;
        case IEventVisitor::visitSkipEvent:
            mute = true;
            break;
        case IEventVisitor::visitSkipFile:
            return false;
        }
        return true;
    }

    //Read a strongly typed value from a buffered stream.
    template<typename T>
    T readToken(T& token)
    {
        if (stream->read(sizeof(token), &token) != sizeof(token))
            throw makeStringException(-1, "unexpected eof");
        bytesRead += sizeof(token);
        return token;
    }

    //Read a fixed length, unterminated, string from a buffered stream.
    StringBuffer& readToken(StringBuffer& token, size32_t len)
    {
        assertex(len);
        token.setLength(len);
        if (stream->read(len, const_cast<char*>(token.str())) != len)
            throw makeStringExceptionV(-1, "eof before end of %u byte string", len);
        bytesRead += len;
        return token;
    }

    //Read a NULL terminated string from a buffered stream.
    StringBuffer& readToken(StringBuffer& token)
    {
        size32_t got = 0;
        for (;;) {
            const char *s = (const char*)stream->peek(1,got);
            if (!s)
                throw makeStringExceptionV(-1, "eof before end of NULL terminated string");
            const char *p = s;
            const char *e = p + got;
            while (p != e)
            {
                if (!*p)
                {
                    token.append(p - s, s);
                    stream->skip(p - s + 1);
                    bytesRead += token.length() + 1;
                    return token;
                }
                p++;
            }
            token.append(got, s);
            stream->skip(got);
        }
        return token;
    }

    IFile* locateEventFile(const char* filename)
    {
        if (isEmptyString(filename))
            return nullptr;
        const char * path = filename;
    #if 0
        StringBuffer outputFilename;
        if (!isAbsolutePath(filename))
        {
            getTempFilePath(outputFilename, "eventrecorder", nullptr);
            outputFilename.append(PATHSEPCHAR).append(filename);
            path = outputFilename.str();
        }
    #endif
        Owned<IFile> file = createIFile(path);
        if (!file || !file->exists())
            throw makeStringExceptionV(-1, "file '%s' not found", path);
        return file.getClear();
    }

    IBufferedSerialInputStream* openEventFileForReading(IFile& file)
    {
        Owned<IFileIO> fileIO = file.open(IFOread);
        if (!fileIO)
            throw makeStringExceptionV(-1, "file '%s' not opened for reading", file.queryFilename());
        Owned<ISerialInputStream> baseStream = createSerialInputStream(fileIO);
        Owned<IBufferedSerialInputStream> bufferedStream = createBufferedInputStream(baseStream, 0x100000, false);

        char header[sizeof(magicHeader)];
        bufferedStream->read(sizeof(header), header);
        if (memcmp(header, magicHeader, sizeof(magicHeader)) != 0)
            throw makeStringExceptionV(-1, "file '%s' is not an event file", file.queryFilename());

        bufferedStream->read(sizeof(version), &version);
        byte compressionType;
        bufferedStream->read(sizeof(compressionType), &compressionType);

        bytesRead += sizeof(header) + sizeof(version) + sizeof(compressionType);

        if (compressionType != COMPRESS_METHOD_NONE)
        {
            ICompressHandler * compressHandler = queryCompressHandler((CompressionMethod)compressionType);
            const char *compressOptions = nullptr; // at least for now!
            Owned<IExpander> expander = compressHandler->getExpander(compressOptions);

            Owned<ISerialInputStream> decompressedStream = createDecompressingInputStream(bufferedStream, expander);
            Owned<IBufferedSerialInputStream> bufferedDecompressedStream = createBufferedInputStream(decompressedStream, 0x100000, false);
            return bufferedDecompressedStream.getClear();
        }
        return bufferedStream.getClear();
    }
};

bool readEvents(const char* filename, IEventVisitor& visitor)
{
    CEventFileReader reader;
    return reader.traverse(filename, visitor);
}

// Event data may be expressed in multiple structured formats, such as XML, JSON, or YAML. Each of
// these can be represented as in an IPropertyTree. All such representations should share a common
// structure.
//
// The IPropertyTree representation of a binary event file resembles:
//     EventFile
//       ├── Header
//       │   ├── @filename
//       │   ├── @version
//       │   └── ... (other header attributes)
//       ├── Event
//       │   ├── @name
//       │   └── ... (other event attributes)
//       ├── ... (more Event elements)
//       └── Footer
//           ├── @bytesRead

constexpr static const char* DUMP_STRUCTURE_ROOT = "EventFile";
constexpr static const char* DUMP_STRUCTURE_FILE_NAME = "filename";
constexpr static const char* DUMP_STRUCTURE_HEADER = "Header";
constexpr static const char* DUMP_STRUCTURE_FILE_VERSION = "version";
constexpr static const char* DUMP_STRUCTURE_EVENT = "Event";
constexpr static const char* DUMP_STRUCTURE_EVENT_NAME = "name";
constexpr static const char* DUMP_STRUCTURE_FOOTER = "Footer";
constexpr static const char* DUMP_STRUCTURE_FILE_BYTES_READ = "bytesRead";

// Abstract base class for event visitors that store visited data in a text-based format. Text-
// based, in this context, means only that the values will be stored as strings. The actual output
// format is determined by the subclass.
//
// Subclasses must implement:
//  `virtual bool visitFile(const char* filename, uint32_t version) = 0;`
//  `virtual Continuation visitEvent(EventType id) = 0;`
//  `virtual bool departEvent() = 0;`
//  `virtual void departFile(uint32_t bytesRead) = 0;`
//  `virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;`
class CDumpEventVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    using Continuation = IEventVisitor::Continuation;

    virtual Continuation visitAttribute(EventAttr id, const char* value) override
    {
        if (EvAttrRecordedOption == id)
            recordAttribute(EvAttrNone, value, "true", false);
        else
            doVisitAttribute(id, value);
        return Continuation::visitContinue;
    }

    virtual Continuation visitAttribute(EventAttr id, bool value) override
    {
        doVisitAttribute(id, value);
        return Continuation::visitContinue;
    }

    virtual Continuation visitAttribute(EventAttr id, __uint64 value) override
    {
        doVisitAttribute(id, value);
        return Continuation::visitContinue;
    }

protected:
    void doVisitHeader(const char* filename, uint32_t version)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_NAME, filename);
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_VERSION, __uint64(version));
    }

    void doVisitEvent(EventType id)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_EVENT_NAME, queryEventName(id));
    }

    void doVisitAttribute(EventAttr id, __uint64 value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, const char* value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, bool value)
    {
        doVisitAttribute(id, queryEventAttributeName(id), value);
    }

    void doVisitAttribute(EventAttr id, const char* name, __uint64 value)
    {
        recordAttribute(id, name, StringBuffer().append(value), false);
    }

    void doVisitAttribute(EventAttr id, const char* name, const char* value)
    {
        recordAttribute(id, name, value, true);
    }

    void doVisitAttribute(EventAttr id, const char* name, bool value)
    {
        recordAttribute(id, name, (value ? "true" : "false"), false);
    }

    void doVisitFooter(uint32_t bytesRead)
    {
        doVisitAttribute(EvAttrNone, DUMP_STRUCTURE_FILE_BYTES_READ, __uint64(bytesRead));
    }

    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;
};

// Concrete extension of CDumpEventVisitor<IPTreeEventVisitor> that uses an IPropertyTree to store
// visited data. The tree is constructed in memory and is available via the `queryTree` method
// after visitation.
class CPTreeEventVisitor : public CDumpEventVisitor
{
public: // IEventVisitor
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        tree.setown(createPTree(DUMP_STRUCTURE_ROOT));
        active = tree->addPropTree(DUMP_STRUCTURE_HEADER, createPTree());
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        active = tree->addPropTree(DUMP_STRUCTURE_EVENT, createPTree());
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        active = tree.get();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        active = tree->addPropTree(DUMP_STRUCTURE_FOOTER, createPTree());
        doVisitFooter(bytesRead);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        active->addProp(VStringBuffer("@%s", name), value);
    }

public:
    virtual IPTree* queryTree() const
    {
        return tree.get();
    }

protected:
    Owned<IPTree> tree;
    IPTree* active = nullptr;
};

IEventPTreeCreator* createEventPTreeCreator()
{
    class Creator : public CInterfaceOf<IEventPTreeCreator>
    {
    public:
        virtual IEventVisitor& queryVisitor() override
        {
            return *visitor;
        }

        virtual IPTree* queryTree() const override
        {
            return visitor->queryTree();
        }

        Creator()
        {
            visitor.setown(new CPTreeEventVisitor);
        }

    private:
        Owned<CPTreeEventVisitor> visitor;
    };
    return new Creator;
}

// Abstract extension of CDumpEventVisitor that writes visited data to an output stream. Subclasses
// are responsible for formatting the data in the desired format.
class CDumpStreamEventVisitor : public CDumpEventVisitor
{
protected: // new abstract method(s)
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) = 0;

public:
    CDumpStreamEventVisitor(IBufferedSerialOutputStream& _out)
        : out(&_out)
    {
    }

protected:
    void dump(bool eoln)
    {
        out->put(markup.length(), markup.str());
        markup.clear();
        if (eoln)
            out->put(1, "\n");
    }

    bool inHeader() const { return State::Header == state; }
    bool inEvents() const { return State::Events == state; }

protected:
    enum class State : byte
    {
        Idle,
        Header,
        Events,
        Footer,
    };
    State state{State::Idle};
    Linked<IBufferedSerialOutputStream> out;
    StringBuffer markup;
};

// Concrete extension of CDumpStreamEventVisitor that writes visited data as unstructured text.
// One line of text is produced for each event and attribute visited, plus additional lines for
// the file name, version, and number of bytes read from the file.
class CDumpTextEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(queryEventName(id));
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        doVisitFooter(bytesRead);
        closeElement();
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        markup.append("attribute: ").append(name).append(" = ");
        if (quoted)
            markup.append('\'');
        markup.append(value);
        if (quoted)
            markup.append('\'');
        markup.append('\n');
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

protected:
    void openElement(const char* name)
    {
        markup.append("event: ").append(name).append('\n');
    }

    void closeElement()
    {
        dump(false);
    }
};

IEventVisitor* createDumpTextEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpTextEventVisitor(out);
}

// Extension of CDumpStreamEventVisitor that outputs XML-formatted text. Header and Footer elements
// synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpXMLEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement(DUMP_STRUCTURE_ROOT, true);
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        openElement(DUMP_STRUCTURE_EVENT);
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
        closeElement(DUMP_STRUCTURE_ROOT);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        appendXMLAttr(markup, name, value);
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

    void openElement(const char* name, bool complete = false)
    {
        markup.pad(2 * indentLevel);
        indentLevel++;
        appendXMLOpenTag(markup, name, nullptr, complete);
        if (complete)
            markup.append('\n');
    }

    void closeElement()
    {
        markup.append("/>");
        indentLevel--;
        dump(true);
    }

    void closeElement(const char* name)
    {
        appendXMLCloseTag(markup, name);
        indentLevel--;
        dump(true);
    }

private:
    unsigned indentLevel{0};
};

IEventVisitor* createDumpXMLEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpXMLEventVisitor(out);
}

// Extension of CDumpStreamEventVisitor that outputs JSON-formatted text. Header and Footer objects
// are synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpJSONEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement();
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            state = State::Events;
        }
        if (firstEvent)
        {
            openArray(DUMP_STRUCTURE_EVENT);
            openElement();
            firstEvent = false;
        }
        else
            openElement();
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        else if (inEvents())
            closeArray();
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
        closeElement(true);
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        conditionalDelimiter();
        indent();
        appendJSONStringValue(markup, name, value, true, quoted);
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

protected:
    inline void openElement()
    {
        openElement(nullptr);
    }

    void openElement(const char* name)
    {
        openContainer(name, "{ ");
    }

    void openArray(const char* name)
    {
        openContainer(name, "[ ");
    }

    void closeArray()
    {
        closeContainer("]");
    }

    void closeElement(bool done = false)
    {
        closeContainer("}");
        dump(done);
    }

    void openContainer(const char* name, const char* token)
    {
        conditionalDelimiter();
        indent();
        if (!isEmptyString(name))
            appendJSONName(markup, name);
        markup.append(token);
        firstFlags.push_back(true);
    }

    void closeContainer(const char* token)
    {
        firstFlags.pop_back();
        indent();
        markup.append(token);
    }

    void conditionalDelimiter()
    {
        if (!firstFlags.empty())
        {
            if (!firstFlags.back())
                markup.append(",");
            else
                firstFlags.back() = false;
        }
    }

    void indent()
    {
        markup.append('\n');
        if (!firstFlags.empty())
            markup.pad(2 * (firstFlags.size()));
    }

private:
    std::vector<bool> firstFlags;
    bool firstEvent{true};
};

IEventVisitor* createDumpJSONEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpJSONEventVisitor(out);
}

// Extension of CDumpStreamEventVisitor that outputs YAML-formatted text. Header and Footer objects
// are synthesized to hold the file name, version, and number of bytes read from the file.
class CDumpYAMLEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        state = State::Header;
        openElement(DUMP_STRUCTURE_HEADER);
        doVisitHeader(filename, version);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
        {
            closeElement();
            openElement(DUMP_STRUCTURE_EVENT);
            state = State::Events;
        }
        else
            openElement(nullptr);
        doVisitEvent(id);
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        closeElement();
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        if (inHeader())
            closeElement();
        state = State::Footer;
        openElement(DUMP_STRUCTURE_FOOTER);
        doVisitFooter(bytesRead);
        closeElement();
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        indent();
        markup.append(name).append(": ").append(value).append('\n');
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

    void openElement(const char* name)
    {
        if (!isEmptyString(name))
        {
            indent();
            markup.append(name).append(":\n");
        }
        indentLevel++;
        firstProp = true;
    }

    void closeElement()
    {
        indentLevel--;
        dump(false);
    }

    void indent()
    {
        if (indentLevel)
        {
            uint8_t limit = indentLevel;
            if (inEvents() && firstProp)
                limit -= 1;
            markup.pad(2 * limit);
            if (inEvents() && firstProp)
            {
                markup.append("- ");
                firstProp = false;
            }
        }
    }

protected:
    bool firstProp{true};
    uint8_t indentLevel{0};
};

IEventVisitor* createDumpYAMLEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpYAMLEventVisitor(out);
}

class CDumpCSVEventVisitor : public CDumpStreamEventVisitor
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {

        state = State::Header;
        encodeCSVColumn(markup, "EventName");
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            markup.append(",");
            encodeCSVColumn(markup, queryEventAttributeName(EventAttr(a)));
        }
        dump(true);
        return true;
    }

    virtual Continuation visitEvent(EventType id) override
    {
        if (inHeader())
            state = State::Events;
        encodeCSVColumn(markup, queryEventName(id));
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            markup.append(",");
            if (row[a].get())
                encodeCSVColumn(markup, row[a].get());
        }
        dump(true);
        // Prepare for the next event by clearing only those row values set by the departed event.
        // Header attributes are not cleared so that they can be output for each event.
        for (unsigned a = EvAttrNone + 1; a < EvAttrMax; a++)
        {
            if (row[a].get() && !headerAttrs.count(EventAttr(a)))
                row[a].clear();
        }
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
    }

protected:
    virtual void recordAttribute(EventAttr id, const char* name, const char* value, bool quoted) override
    {
        if (id != EvAttrNone)
        {
            row[id].set(value);
            if (inHeader())
                headerAttrs.insert(id);
        }
    }

public:
    using CDumpStreamEventVisitor::CDumpStreamEventVisitor;

private:
    StringAttr row[EvAttrMax];
    std::set<EventAttr> headerAttrs;
};

IEventVisitor* createDumpCSVEventVisitor(IBufferedSerialOutputStream& out)
{
    return new CDumpCSVEventVisitor(out);
}

// GH->TK
// Next steps:
//
// * Read the binary file and convert to structured text.  (Possibly create a ptree and then use existing code to convert)
// * Add calls into roxie to start and stop recording based on a control message
// * Add a function to pause/unpause collecting event information - may be needed for the next item
// * Append the meta information for files in the event log when the recording is stopped.
// * Add a tool to summarise which parts of which indexes are read, with an indication of the frequency (needs design)
//
// => milestone: A tool ecl developers can use to understand their queries better
//
// * Add an option to roxie to record all events at startup
//   note - add roxieStartRecording/roxieStopRecording function and call that from everywhere that stops.
// * Add calls from dali client code for the connection/disconnection, and when data is sent.  Ensure that
//   this allows us to trace all file meta data operations.
//
// => milestone: We have a tool that can be used to understand how much time in roxie startup is spent in file lookups.
//
// * Add an option to esp to enable event logging.
//
// => milestone: Can examine which service functions are calling lots of dali functions
//
// * Does it make sense to have a (compile time?) option to record events as text instead?  Would that make developer debugging simpler?
// * Provide a way of getting at the full path filename
// * compress the span id to 16 bytes in the binary file (it is a 32byte hex coded string)
// * Ensure the default filename is unique in some way (include the process id?)
// * Ensure files are written to the debug plane in containerized.  (Should output go to temp and then be copied??)
// * Add a function to optimize write of a single byte to avoid the loop and provide specialist template functions for byte/bool
// * Add calls from the dfs code when reading meta data from a remote esp.  Comments as above.
// * Possibly add the option to apply a delta to the timestamp (maybe when post processing??) to adjust the timestamp
//   earlier - so you can record a request to dali, and the time taken, or is it better as two events?
// * Support throwing items away if the recorder is too busy.
// * Support blocking if the recorder is too busy (for outputing essential meta-data)
// * Write the files on a separate thread in the background
// * Add compression (LZ4 as a first stab) to the binary file, possibly use the streaming classes (if nothing else
//   it is a good to stress test them).  (Should it be a config option?)
// * Could experiment with other compression options - e.g. single byte for events/attributes, packed integers.
// * Possibly add an option to control:stopRecording which will return either the binary, or a text representation
//   (possibly compressed) of the data.
// * Add esp service for sending the request to a single node/all nodes in a cluster, and retrieving the results.
// * Add an option to serialize <n> levels of stack frame and serialize it to the datafile
// * Include the process name that created the event file in the event log
// * Work out how to use gdb/other tools given the serialized stack frame and the process name to convert it to text.
// * Double check the impact on performance when disabled and enabled.
// * Start analysing which other events it would be worthwhile recording.
// * Cope with truncated event logs (e.g. from premature process termination) - warn and return as much as possible
