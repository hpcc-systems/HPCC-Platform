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
#include "jptree.hpp"
#include "jstream.hpp"
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
    EventIndexPayload,            // payload of a leaf node accessed
    EventMax
};

enum EventContext : byte
{
    EventCtxDali,
    EventCtxIndex,
    EventCtxOther,
    EventCtxMax
};

extern jlib_decl EventContext queryEventContext(EventType event);
extern jlib_decl EventContext queryEventContext(const char* name);

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
    EvAttrInMemorySize,
    EvAttrInCache,
    EvAttrPath,
    EvAttrConnectId,
    EvAttrEnabled,
    EvAttrFileSize,
    EvAttrRecordedTimestamp,
    EvAttrRecordedOption,
    EvAttrEventTimestamp,
    EvAttrEventTraceId,
    EvAttrEventThreadId,
    EvAttrEventStackTrace,
    EvAttrDataSize,
    EvAttrExpandTime,
    EvAttrFirstUse,
    EvAttrMax
};

// The different data types of attributes that can be associated with an event.
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

enum EventAttrTypeClass : byte
{
    EATCnone,
    EATCtext,
    EATCnumeric,
    EATCboolean,
    EATCtimestamp,
    EATCmax
};

extern jlib_decl EventType queryEventType(const char* name);
extern jlib_decl const char * queryEventName(EventType event);
extern jlib_decl EventAttr queryEventAttribute(const char* name);
extern jlib_decl const char * queryEventAttributeName(EventAttr attr);
extern jlib_decl EventAttrType queryEventAttributeType(EventAttr attr);

struct jlib_decl EventRecordingSummary
{
    unsigned numEvents{0};
    offset_t totalSize{0};
    offset_t rawSize{0};
    bool valid{true};
    StringBuffer filename;
};

// Encapsulation of a single attribute value for an event. Values may be stored as either text,
// unsigned integral numbers, or Booleans. Assuming that an event will have an instance of this
// for every attribute sescribed in EventAttr, the state of any instance of this is either:
// - Unused: The attribute is not part of the event, and values cannot be set.
// - Defined: The attribute is part of the event, but no value has been set.
// - Assigned: The attribute is part of the event, and a value has been set.
class jlib_decl CEventAttribute
{
public:
    enum State : byte { Unused, Defined, Assigned };

public:
    EventAttr queryId() const;
    EventAttrTypeClass queryTypeClass() const;
    inline bool isTimestamp() const { return EATCtimestamp == queryTypeClass(); }
    inline bool isText() const { return EATCtext == queryTypeClass() || isTimestamp(); }
    inline bool isNumeric() const { return EATCnumeric == queryTypeClass() || isTimestamp(); }
    inline bool isBoolean() const { return EATCboolean == queryTypeClass(); }
    inline State queryState() const { return state; }
    inline bool isUnused() const { return Unused == queryState(); }
    inline bool isDefined() const { return Defined == queryState(); }
    inline bool isAssigned() const { return Assigned == queryState(); }
    void setValue(const char* value);
    void setValue(__uint64 value);
    void setValue(bool value);
    const char* queryTextValue() const;
    __uint64 queryNumericValue() const;
    virtual bool queryBooleanValue() const;
    // Called once to identify the intended meaning of this instance
    void setup(EventAttr attr);
    // Called once per logical event to restore the original state of this instance
    void reset(State _state);

protected:
    mutable StringBuffer text; // mutable allows `queryTextValue() const` to generate timestamp strings on demand
    __uint64 number{0};
    bool boolean{false};
    EventAttr id{EvAttrNone};
    State state{Unused};
};

// Encapsulation of a single event. An event is a type and an array of attributes. Each event may
// have at most one instance of each attribute declared in EventAttr. Range-based iteraration of
// CEventAttribute instances associated with an event is supported in three ways:
// - for (auto& attr : event.allAttributes) { ... }
//   Iteration of all attributes known to the event system, attribute states will include all states
//   and are presented in numerical order of attribute IDs.
// - for (auto& attr : event.defineAttributes) { ... }
//   Iteration of all attributes defined for the event; Unused attributes are excluded. Attributes
//   are presented in the order in which the event system records them.
// - for (auto& attr : event.assignedAttributes) { ... }
//   Iteration of all attributes for which values are set and the state is Assigned. Attributes are
//   presented in the order in which the event system records them.
class jlib_decl CEvent
{
public:
    class jlib_decl AssignedAttributes
    {
    public:
        class jlib_decl iterator
        {
        public:
            iterator(CEvent& _owner, std::initializer_list<EventAttr>::const_iterator _cur)
                : owner(_owner), cur(_cur) { nextAssigned();}
            CEventAttribute& operator*() { return resolve(); }
            CEventAttribute* operator->() { return &resolve(); }
            iterator& operator++() { ++cur; nextAssigned(); return *this; }
            bool operator != (const iterator& other) const { return cur != other.cur; }
        protected:
            CEventAttribute& resolve() const { return owner.attributes[*cur]; }
            void nextAssigned();
        protected:
            CEvent& owner;
            std::initializer_list<EventAttr>::const_iterator cur;
        };

        class jlib_decl const_iterator
        {
        public:
            const_iterator(const CEvent& _owner, std::initializer_list<EventAttr>::const_iterator _cur)
                : owner(_owner), cur(_cur) { nextAssigned();}
            const CEventAttribute& operator*() { return resolve(); }
            const CEventAttribute* operator->() { return &resolve(); }
            const_iterator& operator++() { ++cur; nextAssigned(); return *this; }
            bool operator != (const const_iterator& other) const { return cur != other.cur; }
        protected:
            const CEventAttribute& resolve() const { return owner.attributes[*cur]; }
            void nextAssigned();
        protected:
            const CEvent& owner;
            std::initializer_list<EventAttr>::const_iterator cur;
        };

        iterator begin() { return iterator(owner, owner.queryOrderedAttributeIds().begin()); }
        const_iterator begin() const { return const_iterator(owner, owner.queryOrderedAttributeIds().begin()); }
        iterator end() { return iterator(owner, owner.queryOrderedAttributeIds().end()); }
        const_iterator end() const { return const_iterator(owner, owner.queryOrderedAttributeIds().end()); }

        AssignedAttributes(CEvent& _owner) : owner(_owner) {}

    protected:
        CEvent& owner;
    };

    class DefinedAttributes
    {
    public:
        class iterator
        {
        public:
            iterator(CEvent& _owner, std::initializer_list<EventAttr>::const_iterator _cur)
                : owner(_owner), cur(_cur) {}
            CEventAttribute& operator*() { return resolve(); }
            CEventAttribute* operator->() { return &resolve(); }
            iterator& operator++() { ++cur; return *this; }
            bool operator != (const iterator& other) const { return cur != other.cur; }
        protected:
            CEventAttribute& resolve() const { return owner.attributes[*cur]; }
        protected:
            CEvent& owner;
            std::initializer_list<EventAttr>::const_iterator cur;
        };

        class const_iterator
        {
        public:
            const_iterator(const CEvent& _owner, std::initializer_list<EventAttr>::const_iterator _cur)
                : owner(_owner), cur(_cur) {}
            const CEventAttribute& operator*() { return resolve(); }
            const CEventAttribute* operator->() { return &resolve(); }
            const_iterator& operator++() { ++cur; return *this; }
            bool operator != (const const_iterator& other) const { return cur != other.cur; }
        protected:
            const CEventAttribute& resolve() const { return owner.attributes[*cur]; }
        protected:
            const CEvent& owner;
            std::initializer_list<EventAttr>::const_iterator cur;
        };

        iterator begin() { return iterator(owner, owner.queryOrderedAttributeIds().begin()); }
        const_iterator begin() const { return const_iterator(owner, owner.queryOrderedAttributeIds().begin()); }
        iterator end() { return iterator(owner, owner.queryOrderedAttributeIds().end()); }
        const_iterator end() const { return const_iterator(owner, owner.queryOrderedAttributeIds().end()); }

        DefinedAttributes(CEvent& _owner) : owner(_owner) {}

    protected:
        CEvent& owner;
    };

    class AllAttributes
    {
    public:
        class iterator
        {
        public:
            iterator(CEvent& _owner, EventAttr _attr)
                : owner(_owner)
                , attr(_attr)
            {
                assertex(attr <= EvAttrMax);
                if (EvAttrNone == attr)
                    attr = EventAttr(1);
            }
            CEventAttribute& operator*() { return resolve(); }
            CEventAttribute* operator->() { return &resolve(); }
            iterator& operator++() { if (attr < EvAttrMax) ++attr; return *this; }
            bool operator != (const iterator& other) const { return attr != other.attr; }
        protected:
            CEventAttribute& resolve() const { return owner.attributes[attr]; }
        protected:
            CEvent& owner;
            unsigned attr;
        };

        class const_iterator
        {
        public:
            const_iterator(const CEvent& _owner, EventAttr _attr)
                : owner(_owner)
                , attr(_attr)
            {
                assertex(attr <= EvAttrMax);
                if (EvAttrNone == attr)
                    attr = EventAttr(1);
            }
            const CEventAttribute& operator*() { return resolve(); }
            const CEventAttribute* operator->() { return &resolve(); }
            const_iterator& operator++() { if (attr < EvAttrMax) ++attr; return *this; }
            bool operator != (const const_iterator& other) const { return attr != other.attr; }
        protected:
            const CEventAttribute& resolve() const { return owner.attributes[attr]; }
        protected:
            const CEvent& owner;
            unsigned attr;
        };

        iterator begin() { return iterator(owner, EvAttrNone); }
        const_iterator begin() const { return const_iterator(owner, EvAttrNone); }
        iterator end() { return iterator(owner, EvAttrMax); }
        const_iterator end() const { return const_iterator(owner, EvAttrMax); }

        AllAttributes(CEvent& _owner) : owner(_owner) {}

    protected:
        CEvent& owner;
    };

public:
    EventType queryType() const;
    bool isAttribute(EventAttr attr) const;
    bool hasAttribute(EventAttr attr) const;
    bool isComplete() const;
    CEventAttribute& queryAttribute(EventAttr attr);
    const CEventAttribute& queryAttribute(EventAttr attr) const;
    bool isTextAttribute(EventAttr attr) const;
    bool isNumericAttribute(EventAttr attr) const;
    bool isBooleanAttribute(EventAttr attr) const;
    const char* queryTextValue(EventAttr attr) const;
    __uint64 queryNumericValue(EventAttr attr) const;
    bool queryBooleanValue(EventAttr attr) const;
    bool setValue(EventAttr attr, const char* value);
    bool setValue(EventAttr attr, __uint64 value);
    bool setValue(EventAttr attr, bool value);

public:
    CEvent();
    void reset(EventType _type);

private:
    const std::initializer_list<EventAttr>& queryOrderedAttributeIds() const;

protected:
    EventType type{EventNone};
    CEventAttribute attributes[EvAttrMax];
public:
    AssignedAttributes assignedAttributes;
    DefinedAttributes definedAttributes;
    AllAttributes allAttributes;
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
    void recordIndexLookup(unsigned fileid, offset_t offset, byte nodeKind, bool hit, size32_t sizeIfHit, __uint64  expandTimeIfHit);
    void recordIndexLoad(unsigned fileid, offset_t offset, byte nodeKind, size32_t size, __uint64 expandTime, __uint64 readTime);
    void recordIndexEviction(unsigned fileid, offset_t offset, byte nodeKind, size32_t size);
    void recordIndexPayload(unsigned fileid, offset_t offset, bool firstUse, __uint64 expandTime);

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

    void recordEvent(CEvent& event);

    //-------------------------- End of the public interface --------------------------

protected:
    void recordRecordingActive(bool paused);
    void recordDaliEvent(EventType event, const char * xpath, __int64 id, stat_type elapsedNs, size32_t dataSize);
    void recordDaliEvent(EventType event, __int64 id, stat_type elapsedNs, size32_t dataSize);

    void checkAttrValue(EventAttr attr, size_t size);
    void checkDataWrite(offset_type offset, size_t size);

    void writeEventHeader(EventType type, offset_type & offset);
    void writeEventHeader(EventType type, offset_type & offset, __uint64 timestamp, const char* traceId, __uint64 threadId);
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

    inline void writeTraceId(offset_type & offset, const char* traceid)
    {
        assertex(strlen(traceid) == 32);
        for (unsigned i=0; i < 32; i += 2)
        {
            byte next = getHexPair(traceid + i);
            writeByte(offset, next);
        }
    }

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
    std::atomic<cycle_t> startCycles{0};
    std::atomic<__uint64> startTimestamp{0};
    MemoryAttr buffer;
    CriticalSection cs;
    unsigned sizeMessageHeaderFooter{0};
    unsigned options{0};
    byte compressionType;
    bool outputToLog{false};
    bool corruptOutput{false};
    bool createSpans{false};
    bool suppressPayloadHits{false};
    StringBuffer outputFilename;
    Owned<IFile> outputFile;
    Owned<IFileIO> output;
    Owned<ISerialOutputStream> outputStream;
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

// Abstract interface for visiting the events in a previously recorded event data file.
// An implementation will receive a sequence of calls:
// - visitFile: signals the start of a file
// - visitEvent: signals the arrival of an event, including all attribute data
// - departFile: signals the end of the file
//
// Both visitFile and visitEvent return a Boolean value. True continues file parsing.
// while false stops processing.
interface IEventVisitor : extends IInterface
{
    virtual bool visitFile(const char *filename, uint32_t version) = 0;
    virtual bool visitEvent(CEvent& event) = 0;
    virtual void departFile(uint32_t bytesRead) = 0;
};

// Opens and parses a single binary event data file. Parsed data is passed to the given visitor
// until parsing completes or the visitor requests it to stop.
//
// Exceptions are thrown on error. False is returned if parsing was stopped prematurely. True is
// returned if all data was parsed successfully.
extern jlib_decl bool readEvents(const char* filename, IEventVisitor & visitor);

extern jlib_decl bool startComponentRecording(const char * component, const char * optionsText, const char * filename, bool pause);

#endif
