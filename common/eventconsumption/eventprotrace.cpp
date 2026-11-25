/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "eventprotrace.hpp"

#include "eventmetaparser.hpp"
#include "jprotrace.hpp"
#include "jstring.hpp"
#include "jhconst.hpp"

#include <list>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace
{

struct StringHash
{
    using is_transparent = void;

    size_t operator()(std::string_view value) const
    {
        return std::hash<std::string_view>{}(value);
    }

    size_t operator()(const std::string& value) const
    {
        return operator()(std::string_view(value));
    }

    size_t operator()(const char* value) const
    {
        return operator()(std::string_view(value));
    }
};

class ProtraceState
{
public:
    const CMetaInfoState* metaState{nullptr};
    __uint64 baseTimestamp{0};
    bool hasBaseTimestamp{false};
    __uint64 previousTimestamp{0};
    bool hasPreviousTimestamp{false};
    bool sortedEvents{true};
    std::list<std::string> strings;
    std::unordered_map<std::string_view, unsigned, StringHash, std::equal_to<>> stringIndexes;
    // ProTrace display mappings do not require entries ordered by file ID. The runtime efficiency
    // of a hash table is preferred over a sorted container potentially simplifying unit tests.
    std::unordered_set<__uint64> referencedFileIds;
    bool hasFileMappings{false};
    bool hasSourceIdentity{false};
    byte sourceChannelId{0};
    byte sourceReplicaId{0};
    __uint64 sourceInstanceId{0};

    void noteFile(const EventFileProperties& properties)
    {
        if (properties.startTimestamp)
            noteTimestamp(properties.startTimestamp);
    }

    void noteTimestamp(__uint64 timestamp)
    {
        if (!hasBaseTimestamp || timestamp < baseTimestamp)
        {
            baseTimestamp = timestamp;
            hasBaseTimestamp = true;
        }
    }

    void observeTimestamp(__uint64 timestamp)
    {
        if (hasPreviousTimestamp && timestamp < previousTimestamp)
            sortedEvents = false;
        previousTimestamp = timestamp;
        hasPreviousTimestamp = true;
        noteTimestamp(timestamp);
    }

    unsigned noteString(const char* value)
    {
        if (!value)
            return 0;
        return noteString(std::string_view(value));
    }

    unsigned noteString(std::string_view value)
    {
        auto found = stringIndexes.find(value);
        if (found != stringIndexes.end())
            return found->second;
        unsigned index = (unsigned)strings.size() + 1;
        strings.emplace_back(value);
        stringIndexes.emplace(strings.back(), index);
        return index;
    }

    unsigned queryString(const char* value) const
    {
        if (!value)
            return 0;
        return queryString(std::string_view(value));
    }

    unsigned queryString(std::string_view value) const
    {
        auto found = stringIndexes.find(value);
        return found == stringIndexes.end() ? 0 : found->second;
    }

    void noteEvent(CEvent& event)
    {
        EventType type = event.queryType();
        if (type == EventNone || type >= EventMax)
            throw makeStringExceptionV(-1, "Event type %u cannot be represented in Protrace output", (unsigned)type);
        if ((type == EventTaskStart || type == EventTaskStop) && !event.hasAttribute(EvAttrTask))
            throw makeStringExceptionV(-1, "Task event %u has no task identifier", (unsigned)type);
        if (event.hasAttribute(EvAttrChannelId) || event.hasAttribute(EvAttrReplicaId) || event.hasAttribute(EvAttrInstanceId))
        {
            byte channelId = event.hasAttribute(EvAttrChannelId) ? (byte)event.queryNumericValue(EvAttrChannelId) : 0;
            byte replicaId = event.hasAttribute(EvAttrReplicaId) ? (byte)event.queryNumericValue(EvAttrReplicaId) : 0;
            __uint64 instanceId = event.hasAttribute(EvAttrInstanceId) ? event.queryNumericValue(EvAttrInstanceId) : 0;
            if (hasSourceIdentity && (channelId != sourceChannelId || replicaId != sourceReplicaId || instanceId != sourceInstanceId))
                throw makeStringException(-1, "Protrace output does not support events from multiple recording sources");
            hasSourceIdentity = true;
            sourceChannelId = channelId;
            sourceReplicaId = replicaId;
            sourceInstanceId = instanceId;
        }
        if (type != MetaFileInformation && event.hasAttribute(EvAttrFileId))
            referencedFileIds.insert(event.queryNumericValue(EvAttrFileId));

        auto noteStringAttribute = [this, &event](EventAttr attr)
        {
            if (event.hasAttribute(attr))
                noteString(event.queryTextValue(attr));
        };
        switch (type)
        {
        case EventDaliChangeMode:
        case EventDaliCommit:
        case EventDaliConnect:
        case EventDaliEnsureLocal:
        case EventDaliGet:
        case EventDaliGetChildren:
        case EventDaliGetChildrenFor:
        case EventDaliGetElements:
        case EventDaliSubscribe:
        case MetaFileInformation:
            noteStringAttribute(EvAttrPath);
            break;
        case EventQueryStart:
            noteStringAttribute(EvAttrServiceName);
            break;
        case EventRecordingSource:
            noteStringAttribute(EvAttrProcessDescriptor);
            break;
        case MetaPlaneInformation:
            noteStringAttribute(EvAttrPlane);
            noteStringAttribute(EvAttrPath);
            break;
        default:
            break;
        }
    }

    void noteFileMappings()
    {
        if (!metaState)
            return;
        hasFileMappings = false;
        for (__uint64 fileId : referencedFileIds)
        {
            if (!metaState->hasFileMapping(fileId))
                continue;
            const char* path = metaState->queryFilePath(fileId);
            hasFileMappings = true;
            noteString(path);
        }
    }
};

static void queryValue(const CEvent& event, EventAttr attr, __uint64& value)
{
    if (!event.hasAttribute(attr))
    {
        value = 0;
        return;
    }
    value = event.queryNumericValue(attr);
}

static void appendUnsigned(std::vector<__uint64>& payload, __uint64 value)
{
    payload.push_back(value);
}

static void appendValue(std::vector<__uint64>& payload, const CEvent& event, EventAttr attr)
{
    __uint64 value = 0;
    queryValue(event, attr, value);
    appendUnsigned(payload, value);
}

static void appendPackedIndexOffset(std::vector<__uint64>& payload, const CEvent& event)
{
    __uint64 offset = 0;
    __uint64 nodeKind = 0;
    __uint64 searchFlags = 0;
    queryValue(event, EvAttrFileOffset, offset);
    queryValue(event, EvAttrNodeKind, nodeKind);
    queryValue(event, EvAttrSearchFlags, searchFlags);
    // The native Protrace representation reserves the low byte of FileOffset
    // for NodeKind and traceable SearchFlags.
    offset &= ~(__uint64)(NodeTypeMask | NodeSearchFlagsMask);
    appendUnsigned(payload, offset | (nodeKind & NodeTypeMask) | (searchFlags & NodeSearchFlagsMask));
}

static void appendString(std::vector<__uint64>& payload, const CEvent& event, EventAttr attr, const ProtraceState& state)
{
    if (event.hasAttribute(attr))
        payload.push_back(state.queryString(event.queryTextValue(attr)));
    else
        payload.push_back(0);
}

static void appendPacked32Pair(std::vector<__uint64>& payload, const CEvent& event, EventAttr highAttr, EventAttr lowAttr)
{
    __uint64 high = 0;
    __uint64 low = 0;
    queryValue(event, highAttr, high);
    queryValue(event, lowAttr, low);
    appendUnsigned(payload, ((high & 0xffffffff) << 32) | (low & 0xffffffff));
}

static void appendValuePair(std::vector<__uint64>& payload, const CEvent& event, EventAttr firstAttr, EventAttr secondAttr)
{
    appendValue(payload, event, firstAttr);
    appendValue(payload, event, secondAttr);
}

static void appendPayload(std::vector<__uint64>& payload, const CEvent& event, EventType type, const ProtraceState& state)
{
    __uint64 value = 0;
    switch (type)
    {
    case EventIndexCacheHit:
    case EventIndexCacheMiss:
    case EventIndexEviction:
        appendValue(payload, event, EvAttrFileId);
        appendPackedIndexOffset(payload, event);
        break;
    case EventIndexLoad:
        queryValue(event, EvAttrReadTime, value);
        if (event.hasAttribute(EvAttrExpandTime))
            value += event.queryNumericValue(EvAttrExpandTime);
        appendUnsigned(payload, value);
        appendValue(payload, event, EvAttrFileId);
        appendPackedIndexOffset(payload, event);
        break;
    case EventDaliChangeMode:
    case EventDaliCommit:
    case EventDaliConnect:
    case EventDaliEnsureLocal:
    case EventDaliGet:
    case EventDaliGetChildren:
    case EventDaliGetChildrenFor:
    case EventDaliGetElements:
    case EventDaliSubscribe:
        appendValue(payload, event, EvAttrElapsedTime);
        appendString(payload, event, EvAttrPath, state);
        appendValue(payload, event, EvAttrConnectId);
        appendValue(payload, event, EvAttrDataSize);
        break;
    case MetaFileInformation:
        appendValue(payload, event, EvAttrFileId);
        appendString(payload, event, EvAttrPath, state);
        break;
    case EventIndexPayload:
        appendValuePair(payload, event, EvAttrFileId, EvAttrFileOffset);
        break;
    case EventQueryStart:
        appendString(payload, event, EvAttrServiceName, state);
        break;
    case EventRecordingSource:
        appendString(payload, event, EvAttrProcessDescriptor, state);
        break;
    case EventIndexOpen:
        appendValuePair(payload, event, EvAttrOpenTime, EvAttrFileId);
        break;
    case MetaPlaneInformation:
        appendString(payload, event, EvAttrPlane, state);
        appendString(payload, event, EvAttrPath, state);
        break;
    case EventRequestSend:
    case EventRequestReceive:
    case EventWorkerStart:
    case EventWorkerStop:
        appendPacked32Pair(payload, event, EvAttrRequestSeq, EvAttrRequestId);
        break;
    case EventResponseSend:
    case EventResponseReceive:
        appendPacked32Pair(payload, event, EvAttrRequestSeq, EvAttrRequestId);
        appendPacked32Pair(payload, event, EvAttrResponseSeq, EvAttrResponseId);
        break;
    case EventLockWait:
    case EventLockAcquire:
    case EventLockRelease:
    case EventSemWait:
    case EventSemAcquire:
    case EventSemSignal:
    case EventLockTryWaitAcquire:
    case EventLockTryWaitFail:
    case EventLockWaitTimeout:
    case EventSemWaitTimeout:
    case EventRwlockReadWait:
    case EventRwlockReadAcquire:
    case EventRwlockReadRelease:
    case EventRwlockWriteWait:
    case EventRwlockWriteAcquire:
    case EventRwlockWriteRelease:
    case EventRwlockReadWaitTimeout:
    case EventRwlockWriteWaitTimeout:
        appendValue(payload, event, EvAttrLockId);
        break;
    case EventMpRequestSend:
        appendValue(payload, event, EvAttrRequestId);
        appendValue(payload, event, EvAttrDataSize);
        break;
    case EventMpRequestReceive:
        appendValue(payload, event, EvAttrRequestId);
        break;
    case EventMpResponseSend:
        appendValue(payload, event, EvAttrResponseId);
        appendValue(payload, event, EvAttrDataSize);
        break;
    case EventMpResponseReceive:
        appendValue(payload, event, EvAttrResponseId);
        break;
    case EventEnqueue:
    case EventDequeue:
        appendValue(payload, event, EvAttrElementId);
        break;
    case EventFunctionEnter:
    case EventFunctionExit:
        appendValue(payload, event, EvAttrFunctionId);
        break;
    default:
        break;
    }
}

static unsigned queryOpcode(const CEvent& event)
{
    EventType type = event.queryType();
    if (type == EventTaskStart)
    {
        if (!event.hasAttribute(EvAttrTask))
            throw makeStringExceptionV(-1, "Task start event has no task identifier");
        return getTaskStartOp((EventTask)event.queryNumericValue(EvAttrTask));
    }
    if (type == EventTaskStop)
    {
        if (!event.hasAttribute(EvAttrTask))
            throw makeStringExceptionV(-1, "Task stop event has no task identifier");
        return getTaskStopOp((EventTask)event.queryNumericValue(EvAttrTask));
    }
    return (unsigned)type;
}

static void subtractDuration(__uint64 &timestamp, const CEvent &event, EventAttr attr)
{
    if (!event.hasAttribute(attr))
        return;
    __uint64 duration = event.queryNumericValue(attr);
    timestamp = duration >= timestamp ? 0 : timestamp - duration;
}

static __uint64 queryStartTimestamp(const CEvent& event)
{
    if (!event.hasAttribute(EvAttrEventTimestamp))
        return 0;

    __uint64 timestamp = event.queryNumericValue(EvAttrEventTimestamp);

    switch (event.queryType())
    {
    case EventIndexLoad:
        subtractDuration(timestamp, event, EvAttrReadTime);
        subtractDuration(timestamp, event, EvAttrExpandTime);
        break;
    case EventIndexPayload:
        subtractDuration(timestamp, event, EvAttrExpandTime);
        break;
    case EventIndexOpen:
        subtractDuration(timestamp, event, EvAttrOpenTime);
        break;
    case EventDaliChangeMode:
    case EventDaliCommit:
    case EventDaliConnect:
    case EventDaliEnsureLocal:
    case EventDaliGet:
    case EventDaliGetChildren:
    case EventDaliGetChildrenFor:
    case EventDaliGetElements:
    case EventDaliSubscribe:
        subtractDuration(timestamp, event, EvAttrElapsedTime);
        break;
    default:
        break;
    }
    return timestamp;
}

class CProtraceVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    CProtraceVisitor(ProtraceState& _state, IBufferedSerialOutputStream* _out, bool _preScan)
        : state(_state), out(_out), preScan(_preScan)
    {
        payload.reserve(8);
    }

    virtual bool visitFile(const EventFileProperties& properties) override
    {
        state.noteFile(properties);
        if (!preScan && !manifestWritten && state.hasBaseTimestamp)
            writeManifest();
        return true;
    }

    virtual bool visitEvent(CEvent& event, const EventFileProperties& properties) override
    {
        if (preScan)
        {
            state.noteEvent(event);
            if (event.hasAttribute(EvAttrEventTimestamp))
                state.observeTimestamp(queryStartTimestamp(event));
            return true;
        }

        if (!manifestWritten)
        {
            if (event.hasAttribute(EvAttrEventTimestamp))
                state.noteTimestamp(queryStartTimestamp(event));
            writeManifest();
        }

        unsigned tid = event.hasAttribute(EvAttrEventThreadId) ? (unsigned)event.queryNumericValue(EvAttrEventThreadId) : 0;
        __uint64 timestamp = event.hasAttribute(EvAttrEventTimestamp) ? queryStartTimestamp(event) : state.baseTimestamp;
        payload.clear();
        EventType type = event.queryType();
        if (type == EventTaskStart)
        {
            EventTask task = event.hasAttribute(EvAttrTask) ? (EventTask)event.queryNumericValue(EvAttrTask) : EventTask::Max;
            if (task == EventTask::Compressing)
                appendPacked32Pair(payload, event, EvAttrDataSize, EvAttrInMemorySize);
            else if (event.hasAttribute(EvAttrActivityId))
                appendUnsigned(payload, event.queryNumericValue(EvAttrActivityId));
            else if (event.hasAttribute(EvAttrDataSize))
                appendUnsigned(payload, event.queryNumericValue(EvAttrDataSize));
        }
        else
            appendPayload(payload, event, type, state);

        line.clear();
        line.append('[').append("0").append(", ").append(tid).append(", ").append(queryOpcode(event)).append(", ");
        line.append(timestamp >= state.baseTimestamp ? timestamp - state.baseTimestamp : 0);
        for (__uint64 slot : payload)
            line.append(", ").append(slot);
        line.append("]\n");
        out->put(line.length(), line.str());
        return true;
    }

    virtual void departFile(const EventFileProperties&) override
    {
        if (!preScan && !manifestWritten)
            writeManifest();
    }

private:
    void writeManifest()
    {
        if (manifestWritten)
            return;
        state.noteFileMappings();
        StringBuffer line;
        line.append("{\n")
            .append("  \"type\": \"manifest\",\n")
            .append("  \"cpus\": 1,\n")
            .append("  \"format\": \"hpcc\",\n");
        if (state.sortedEvents)
            line.append("  \"sortedEvents\": true,\n");
        line.append("  \"base_ts\": ").append(state.baseTimestamp);
        if (state.hasFileMappings)
        {
            line.append(",\n  \"displayMappings\": {\"1\": [");
            bool firstMapping = true;
            for (__uint64 fileId : state.referencedFileIds)
            {
                if (!state.metaState->hasFileMapping(fileId))
                    continue;
                const char* path = state.metaState->queryFilePath(fileId);
                if (!firstMapping)
                    line.append(',');
                firstMapping = false;
                line.append('[').append(fileId).append(',')
                    .append(state.queryString(path)).append(']');
            }
            line.append("]}");
        }
        if (!state.strings.empty())
        {
            line.append(",\n  \"stringtable\": [");
            bool firstString = true;
            for (const std::string& value : state.strings)
            {
                if (!firstString)
                    line.append(',');
                firstString = false;
                line.append('"');
                encodeJSON(line, value.c_str());
                line.append('"');
            }
            line.append(']');
        }
        line.append("\n}\n");
        out->put(line.length(), line.str());
        manifestWritten = true;
    }

    ProtraceState& state;
    IBufferedSerialOutputStream* out;
    bool preScan;
    std::vector<__uint64> payload;
    StringBuffer line;
    bool manifestWritten{false};
};

class CProtraceEventExporter : public CInterfaceOf<IProtraceEventExporter>
{
public:
    CProtraceEventExporter(IBufferedSerialOutputStream& out, const CMetaInfoState* _metaState)
        : preScanVisitor(state, nullptr, true), outputVisitor(state, &out, false)
    {
        state.metaState = _metaState;
    }

    virtual IEventVisitor& queryPreScanVisitor() override { return preScanVisitor; }
    virtual IEventVisitor& queryTerminalVisitor() override { return outputVisitor; }
    virtual bool preScanRequired() const override { return true; }

private:
    ProtraceState state;
    CProtraceVisitor preScanVisitor;
    CProtraceVisitor outputVisitor;
};

}

IProtraceEventExporter *createProtraceEventExporter(IBufferedSerialOutputStream& out)
{
    return new CProtraceEventExporter(out, nullptr);
}

IProtraceEventExporter *createProtraceEventExporter(IBufferedSerialOutputStream& out, const CMetaInfoState& metaState)
{
    return new CProtraceEventExporter(out, &metaState);
}
