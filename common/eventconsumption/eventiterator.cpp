/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "eventiterator.h"
#include "eventindex.hpp"
#include <list>
#include <map>
#include <string>

// Implementation of IEventIterator that distributes events extracted from a property tree.
// Distribution occurs in the order that events appear in the tree. If event order is important,
// such as when used with a multiplexer, the input used to create the property tree is responsible
// for ensuring event order.
class event_decl CPropertyTreeEvents : public CInterfaceOf<IEventIterator>
{
public:
    virtual bool nextEvent(CEvent& event) override;
    virtual const EventFileProperties& queryFileProperties() const override;
public:
    CPropertyTreeEvents(const IPropertyTree& events, unsigned flags);
protected:
    Linked<const IPropertyTree> events;
    Owned<IPropertyTreeIterator> eventsIt;
    EventFileProperties properties;
    unsigned flags;
    bool firstEvent{true};
    bool firstRecordingSource{true};
};

IEventIterator* createPropertyTreeEvents(const IPropertyTree& events, unsigned flags)
{
    return new CPropertyTreeEvents(events, flags);
}

bool CPropertyTreeEvents::nextEvent(CEvent& event)
{
    const IPropertyTree* node = nullptr;
    const char* typeStr = nullptr;
    EventType type = EventNone;
    while (true)
    {
        if (!eventsIt->isValid())
            return false;
        node = &eventsIt->query();
        typeStr = node->queryProp("@type");
        if (isEmptyString(typeStr))
            throw makeStringException(-1, "missing event type");
        type = queryEventType(typeStr);
        if (type != EventNone)
            break;
        if (!(flags & PTEFlenientParsing))
            throw makeStringExceptionV(-1, "unknown event type: %s", typeStr);
        firstEvent = false;
        (void)eventsIt->next();
    }
    if (EventRecordingSource == type)
    {
        if ((firstEvent && firstRecordingSource) || (flags & PTEFliteralParsing))
        {
            // Extract
            if (node->hasProp("@ProcessDescriptor"))
                properties.processDescriptor.set(node->queryProp("@ProcessDescriptor"));
            if (node->hasProp("@ChannelId"))
            {
                __uint64 channelId = node->getPropInt64("@ChannelId");
                if (channelId > UINT8_MAX)
                    throw makeStringExceptionV(0, "ChannelId value %llu exceeds maximum allowed value %u", channelId, UINT8_MAX);
                properties.channelId = static_cast<byte>(channelId);
            }
            if (node->hasProp("@ReplicaId"))
            {
                __uint64 replicaId = node->getPropInt64("@ReplicaId");
                if (replicaId > UINT8_MAX)
                    throw makeStringExceptionV(0, "ReplicaId value %llu exceeds maximum allowed value %u", replicaId, UINT8_MAX);
                properties.replicaId = static_cast<byte>(replicaId);
            }
            if (node->hasProp("@InstanceId"))
                properties.instanceId = node->getPropInt64("@InstanceId");

            if (!(flags & PTEFliteralParsing))
            {
                // Non-literal parsing requires silent consumption of this event with automatic
                // progression to the next event. The recursion will recurse exactly one time
                // neither firtEvent nor firstRecordingSource remain set.
                firstEvent = false;
                firstRecordingSource = false;
                (void)eventsIt->next();
                return nextEvent(event);
            }
        }
        else if (!firstRecordingSource)
        {
            // Non-literal parsing requires at most one RecordingSource event at the start of the
            // stream. Strict parsing is not required for enforcement.
            if (!(flags & PTEFliteralParsing))
                throw makeStringException(-1, "multiple RecordingSource events encountered");
        }
        else if (!firstEvent)
        {
            // Non-literal parsing requires the RecordingSource event to be the first event in the
            // stream. Strict parsing is not required for enforcement.
            if (!(flags & PTEFliteralParsing))
                throw makeStringException(-1, "RecordingSource event must be the first event in the stream");
        }
        firstRecordingSource = false;
    }
    firstEvent = false;
    event.reset(type);

    Owned<IAttributeIterator> attrIt = node->getAttributes();
    ForEach(*attrIt)
    {
        const char* name = attrIt->queryName();
        if ('@' == *name)
            name++;
        if (streq(name, "type"))
            continue;
        EventAttr attrId = queryEventAttribute(name);
        if (EvAttrNone == attrId)
        {
            if (!(flags & PTEFlenientParsing))
                throw makeStringExceptionV(-1, "unknown attribute: %s", name);
            continue;
        }
        if (!event.isAttribute(attrId))
        {
            if (!(flags & PTEFlenientParsing))
                throw makeStringExceptionV(-1, "unused attribute %s/%s", typeStr, name);
            continue;
        }
        const char* valueStr = attrIt->queryValue();
        if (isEmptyString(valueStr))
            continue;
        CEventAttribute& attr = event.queryAttribute(attrId);
        switch (attr.queryTypeClass())
        {
        case EATCtext:
        case EATCtimestamp:
            attr.setValue(valueStr);
            break;
        case EATCnumeric:
            if (EvAttrNodeKind == attrId)
                attr.setValue(__uint64(mapNodeKind(valueStr)));
            else
                attr.setValue(strtoull(valueStr, nullptr, 0));
            break;
        case EATCboolean:
            attr.setValue(strToBool(valueStr));
            break;
        default:
            if (!(flags & PTEFlenientParsing))
                throw makeStringExceptionV(-1, "unknown attribute type class %u for %s/%s", attr.queryTypeClass(), typeStr, name);
            break;
        }
    }
    if (!(flags & PTEFliteralParsing))
        event.fixup(properties);
    if (!(flags & PTEFlenientParsing) && !event.isComplete())
        throw makeStringExceptionV(-1, "incomplete event %s", typeStr);

    // advance to the next matching node
    (void)eventsIt->next();

    // the requested event was found
    return true;
}

const EventFileProperties& CPropertyTreeEvents::queryFileProperties() const
{
    return properties;
}

CPropertyTreeEvents::CPropertyTreeEvents(const IPropertyTree& _events, unsigned _flags)
    : events(&_events)
    , eventsIt(_events.getElements("event"))
    , flags(_flags)
{
    // enable the "next" event to populate from the first matching node
    (void)eventsIt->first();
    properties.path.set(events->queryProp("@filename"));
    properties.version = uint32_t(events->getPropInt64("@version"));
    properties.bytesRead = uint32_t(events->getPropInt("@bytesRead"));
}

// Implementation of IEventMultiplexer that merges events from multiple underlying event iterators.
// Events should be distributed in chronological order according to the EventTimestamp attribute//
// value. Events with timestamps are always distributed before events without.
//
// An event file iterator is guaranteed to provide events with timestamps and in chronological
// order.
//
// A property tree event iterator provides events in the order they appear in the tree, with or
// without timestamps. If a multiplexer source input provides events in non-chronological order,
// this iterator will distribute those events in non-chronological order.
class event_decl CEventMultiplexer : public CInterfaceOf<IEventMultiplexer>
{
public:
    using Sources = std::list<std::pair<Linked<IEventIterator>, CEvent>>;
public: // IEventMultiplexer
    virtual bool nextEvent(CEvent& event) override;
    virtual const EventFileProperties& queryFileProperties() const override;
    virtual void addSource(IEventIterator& source) override;
public:
    CEventMultiplexer(CMetaInfoState& metaState);
    bool hasSource(IEventIterator& source) const;
protected:
    CMetaInfoState& metaState;
    Owned<IEventVisitor> metaStateCollector;
    EventFileProperties properties;
    Sources sources;
    std::unordered_map<size_t, __uint64> idMap;
    std::unordered_map<std::string, __uint64> pathMap;
    bool acceptSources{true};
};

IEventMultiplexer* createEventMultiplexer(CMetaInfoState& metaState)
{
    return new CEventMultiplexer(metaState);
}

bool CEventMultiplexer::nextEvent(CEvent& event)
{
    // If at least one source has a next event with a timestamp, choose the timestamped event with
    // the lowest chronological value.
    Sources::iterator best = sources.end();
    for (Sources::iterator it = sources.begin(); it != sources.end(); ++it)
    {
        if (it->second.queryType() == EventNone)
            continue;
        if (!it->second.hasAttribute(EvAttrEventTimestamp))
            continue;
        if (sources.end() == best)
            best = it;
        else if (it->second.queryNumericValue(EvAttrEventTimestamp) < best->second.queryNumericValue(EvAttrEventTimestamp))
            best = it;
    }
    // If the loop terminated without a best candidate, all remaining sources have a next event
    // without a timestamp. No secondary sort key exists. Choose the next event from the first
    // source.
    if (sources.end() == best)
        best = sources.begin();
    // If a best candidate has been identified, prepare for its use.
    // If no best candidate has been found, no events remain.
    if (best != sources.end())
    {
        event = best->second;
        (void)metaStateCollector->visitEvent(event);
        properties.eventsRead++;
        acceptSources = false;
        if (!best->first->nextEvent(best->second))
        {
            // Source completed - accumulate final stats
            const EventFileProperties& sourceProps = best->first->queryFileProperties();
            properties.bytesRead += sourceProps.bytesRead;
            sources.erase(best);
        }
        return true;
    }
    else
        return false;
}

const EventFileProperties& CEventMultiplexer::queryFileProperties() const
{
    return properties;
}

CEventMultiplexer::CEventMultiplexer(CMetaInfoState& _metaState)
    : metaState(_metaState)
{
    metaStateCollector.setown(metaState.getCollector());
    properties.path.set("multiplexed");
}

void CEventMultiplexer::addSource(IEventIterator& source)
{
    if (!acceptSources)
        throw makeStringException(0, "event multiplexer cannot add a source after event consumption has started");
    if (this == &source)
        throw makeStringException(0, "event multiplexer cannot add itself as source");
    if (hasSource(source))
        throw makeStringException(0, "event multiplexer cannot add a source already in use");
    CEvent next;
    if (!source.nextEvent(next))
        return;
    const EventFileProperties& sourceProps = source.queryFileProperties();
    if (sources.empty())
    {
        // First source - initialize properties
        properties.processDescriptor.set(sourceProps.processDescriptor.get());
        properties.version = sourceProps.version;
        properties.channelId = sourceProps.channelId;
        properties.replicaId = sourceProps.replicaId;
        properties.instanceId = sourceProps.instanceId;
        properties.options.includeTraceIds = sourceProps.options.includeTraceIds;
        properties.options.includeThreadIds = sourceProps.options.includeThreadIds;
        properties.options.includeStackTraces = sourceProps.options.includeStackTraces;
    }
    else
    {
        const char* actual = sourceProps.processDescriptor.str();
        const char* expected = properties.processDescriptor.str();
        if (!streq(expected, actual))
            throw makeStringExceptionV(0, "file source mismatch - needed ProcessDescriptor '%s' but found '%s'", expected, actual);
        for (const Sources::value_type& entry : sources)
        {
            if (&source == entry.first.get())
                return;
            const char* path = sourceProps.path.str();
            if (!isEmptyString(path) && streq(path, entry.first->queryFileProperties().path.str()))
                return;
        }

        // Aggregate properties - use ambiguous value when sources conflict
        if (sourceProps.version != properties.version)
            properties.version = AmbiguousVersion;
        if (sourceProps.channelId != properties.channelId)
            properties.channelId = AmbiguousChannelId;
        if (sourceProps.replicaId != properties.replicaId)
            properties.replicaId = AmbiguousReplicaId;
        if (sourceProps.instanceId != properties.instanceId)
            properties.instanceId = AmbiguousInstanceId;

        // Update tri-state options
        auto updateTriState = [](EventFileOption& current, EventFileOption source) {
            if (EventFileOption::Ambiguous == current)
                return; // already ambiguous
            if (current == source)
                return; // still the same
            current = EventFileOption::Ambiguous;
        };
        updateTriState(properties.options.includeTraceIds, sourceProps.options.includeTraceIds);
        updateTriState(properties.options.includeThreadIds, sourceProps.options.includeThreadIds);
        updateTriState(properties.options.includeStackTraces, sourceProps.options.includeStackTraces);
    }
    sources.emplace_back(Linked(&source), next);
    metaStateCollector->visitFile(sourceProps.path.str(), sourceProps.version);
}

bool CEventMultiplexer::hasSource(IEventIterator& source) const
{
    for (const Sources::value_type& entry : sources)
    {
        if (&source == entry.first.get())
            return true;
        CEventMultiplexer* nested = dynamic_cast<CEventMultiplexer*>(entry.first.get());
        if (nested && nested->hasSource(source))
            return true;
    }
    return false;
}

void visitIterableEvents(IEventIterator& iter, IEventVisitor& visitor)
{
    CEvent event;
    const EventFileProperties& props = iter.queryFileProperties();
    visitor.visitFile(props.path, props.version);
    while (iter.nextEvent(event))
        visitor.visitEvent(event);
    visitor.departFile(props.bytesRead);
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventIteratorTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventIteratorTests);
    CPPUNIT_TEST(testStrictEventParsingUnknownEvent);
    CPPUNIT_TEST(testStrictEventParsingUnknownAttribute);
    CPPUNIT_TEST(testStrictEventParsingUnusedAttribute);
    CPPUNIT_TEST(testStrictEventParsingIncompleteEvent);
    CPPUNIT_TEST(testLenientEventParsing);
    CPPUNIT_TEST(testNodeKindMapping);
    CPPUNIT_TEST(testPropertyTreeEventsPassThrough);
    CPPUNIT_TEST(testMultiplexerNoSources);
    CPPUNIT_TEST(testMultiplexerSingleSource);
    CPPUNIT_TEST(testMultiplexerSingleSourceWithSiblings);
    CPPUNIT_TEST(testMultiplexerMultipleSources);
    CPPUNIT_TEST_SUITE_END();

public:
    void testStrictEventParsingUnknownEvent()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="Unknown"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingUnknownAttribute()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" unknown="foo"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingUnusedAttribute()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" InMemorySize="0"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingIncompleteEvent()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" FileId="1"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testLenientEventParsing()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="unknown"/>
                    <event type="FileInformation" unknown="foo"/>
                    <event type="FileInformation" InMemorySize="0"/>
                    <event type="FileInformation" FileId="1"/>
                    <event type="FileInformation" FileId="1" Path="foo"/>
                </input>
                <expect>
                    <event type="FileInformation"/>
                    <event type="FileInformation"/>
                    <event type="FileInformation" FileId="1"/>
                    <event type="FileInformation" FileId="1" Path="foo"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }

    void testNodeKindMapping()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="IndexCacheMiss" NodeKind="0"/>
                    <event type="IndexCacheHit" NodeKind="1"/>
                    <event type="IndexLoad" NodeKind="2"/>
                    <event type="IndexEviction" NodeKind="branch"/>
                    <event type="IndexCacheMiss" NodeKind="leaf"/>
                    <event type="IndexCacheHit" NodeKind="blob"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" NodeKind="0"/>
                    <event type="IndexCacheHit" NodeKind="1"/>
                    <event type="IndexLoad" NodeKind="2"/>
                    <event type="IndexEviction" NodeKind="0"/>
                    <event type="IndexCacheMiss" NodeKind="1"/>
                    <event type="IndexCacheHit" NodeKind="2"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }

    // Test CPropertyTreeEvents pass-through mode (consumeRecordingSource = false)
    // Events should be passed through exactly as specified in XML, including source attributes
    void testPropertyTreeEventsPassThrough()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="RecordingSource" ProcessDescriptor="should_not_be_consumed" ChannelId="99" ReplicaId="98" InstanceId="97"/>
                    <event type="FileInformation" FileId="1" Path="/test/file.dat"/>
                    <event type="IndexCacheMiss" EventTimestamp="2025-01-01T10:00:00.000000100" FileId="1" FileOffset="1024" NodeKind="0" ChannelId="5" ReplicaId="6" InstanceId="7"/>
                    <event type="IndexLoad" EventTimestamp="2025-01-01T10:00:00.000000200" FileId="1" FileOffset="2048" NodeKind="1" InMemorySize="4096" ReadTime="50" ExpandTime="25" ChannelId="10" ReplicaId="11" InstanceId="12"/>
                </input>
                <expect>
                    <event type="RecordingSource" ProcessDescriptor="should_not_be_consumed" ChannelId="99" ReplicaId="98" InstanceId="97"/>
                    <event type="FileInformation" FileId="1" Path="/test/file.dat"/>
                    <event type="IndexCacheMiss" EventTimestamp="2025-01-01T10:00:00.000000100" FileId="1" FileOffset="1024" NodeKind="0" ChannelId="5" ReplicaId="6" InstanceId="7"/>
                    <event type="IndexLoad" EventTimestamp="2025-01-01T10:00:00.000000200" FileId="1" FileOffset="2048" NodeKind="1" InMemorySize="4096" ReadTime="50" ExpandTime="25" ChannelId="10" ReplicaId="11" InstanceId="12"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing | PTEFliteralParsing);
    }

    // Test CEventMultiplexer with no source elements (direct events)
    void testMultiplexerNoSources()
    {
        const char* testData = R"!!!(
input:
  event:
  - type: RecordingSource
    ProcessDescriptor: myprocess
    ChannelId: 5
    ReplicaId: 10
    InstanceId: 15
  - type: FileInformation
    FileId: 100
    Path: /path/to/file1.dat
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-01T10:00:00.000000100'
    FileId: 100
    FileOffset: 1024
    NodeKind: 0
  - type: IndexLoad
    EventTimestamp: '2025-01-01T10:00:00.000000200'
    FileId: 100
    FileOffset: 1024
    NodeKind: 0
    InMemorySize: 8192
    ReadTime: 50
    ExpandTime: 25
expect:
  event:
  - type: RecordingSource
    ProcessDescriptor: myprocess
    ChannelId: 5
    ReplicaId: 10
    InstanceId: 15
  - type: FileInformation
    FileId: 100
    Path: /path/to/file1.dat
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-01T10:00:00.000000100'
    FileId: 100
    FileOffset: 1024
    NodeKind: 0
  - type: IndexLoad
    EventTimestamp: '2025-01-01T10:00:00.000000200'
    FileId: 100
    FileOffset: 1024
    NodeKind: 0
    InMemorySize: 8192
    ReadTime: 50
    ExpandTime: 25
)!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }

    // Test CEventMultiplexer with a single source element
    void testMultiplexerSingleSource()
    {
        const char* testData = R"!!!(
input:
  source:
  - event:
    - type: RecordingSource
      ProcessDescriptor: process1
      ChannelId: 1
      ReplicaId: 2
      InstanceId: 3
    - type: FileInformation
      FileId: 50
      Path: /data/index.idx
    - type: IndexCacheHit
      EventTimestamp: '2025-01-01T12:00:00.000000500'
      FileId: 50
      FileOffset: 2048
      NodeKind: 1
      InMemorySize: 4096
      ExpandTime: 75
    - type: IndexCacheMiss
      EventTimestamp: '2025-01-01T12:00:00.000000600'
      FileId: 50
      FileOffset: 4096
      NodeKind: 0
expect:
  event:
  - type: FileInformation
    FileId: 50
    Path: /data/index.idx
    ChannelId: 1
    ReplicaId: 2
    InstanceId: 3
  - type: IndexCacheHit
    EventTimestamp: '2025-01-01T12:00:00.000000500'
    FileId: 50
    FileOffset: 2048
    NodeKind: 1
    InMemorySize: 4096
    ExpandTime: 75
    ChannelId: 1
    ReplicaId: 2
    InstanceId: 3
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-01T12:00:00.000000600'
    FileId: 50
    FileOffset: 4096
    NodeKind: 0
    ChannelId: 1
    ReplicaId: 2
    InstanceId: 3
)!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }

    // Test CEventMultiplexer with a single source and sibling events (siblings should be ignored)
    void testMultiplexerSingleSourceWithSiblings()
    {
        const char* testData = R"!!!(
input:
  source:
  - event:
    - type: RecordingSource
      ProcessDescriptor: process2
      ChannelId: 7
      ReplicaId: 8
      InstanceId: 9
    - type: FileInformation
      FileId: 200
      Path: /var/data/test.dat
    - type: IndexLoad
      EventTimestamp: '2025-01-02T08:30:00.000001000'
      FileId: 200
      FileOffset: 8192
      NodeKind: 0
      InMemorySize: 16384
      ReadTime: 120
      ExpandTime: 30
  event:
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-02T08:30:00.000002000'
    FileId: 999
    FileOffset: 12288
    NodeKind: 0
expect:
  event:
  - type: FileInformation
    FileId: 200
    Path: /var/data/test.dat
    ChannelId: 7
    ReplicaId: 8
    InstanceId: 9
  - type: IndexLoad
    EventTimestamp: '2025-01-02T08:30:00.000001000'
    FileId: 200
    FileOffset: 8192
    NodeKind: 0
    InMemorySize: 16384
    ReadTime: 120
    ExpandTime: 30
    ChannelId: 7
    ReplicaId: 8
    InstanceId: 9
)!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }

    // Test CEventMultiplexer with multiple sources (chronological merging with file ID remapping)
    // This test covers several scenarios:
    // 1. Same path with different FileIds in different sources (deduplication)
    // 2. Same FileId with same path in different sources (handled correctly)
    // 3. Same FileId with different paths in different sources (requires remapping)
    void testMultiplexerMultipleSources()
    {
        const char* testData = R"!!!(
input:
  source:
    - event:
      - type: RecordingSource
        ProcessDescriptor: testproc
        ChannelId: 1
        ReplicaId: 1
        InstanceId: 1
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000020'
        FileId: 10
        Path: /shared/index.idx
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000030'
        FileId: 50
        Path: /local/data1.dat
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000040'
        FileId: 100
        Path: /unique/file1.idx
      - type: IndexCacheMiss
        EventTimestamp: '2025-01-03T10:00:00.000000100'
        FileId: 10
        FileOffset: 1024
        NodeKind: 0
      - type: IndexCacheMiss
        EventTimestamp: '2025-01-03T10:00:00.000000150'
        FileId: 50
        FileOffset: 2048
        NodeKind: 0
      - type: IndexLoad
        EventTimestamp: '2025-01-03T10:00:00.000000300'
        FileId: 100
        FileOffset: 3072
        NodeKind: 0
        InMemorySize: 4096
        ReadTime: 50
        ExpandTime: 10
    - event:
      - type: RecordingSource
        ProcessDescriptor: testproc
        ChannelId: 2
        ReplicaId: 1
        InstanceId: 1
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000025'
        FileId: 10
        Path: /shared/index.idx
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000035'
        FileId: 50
        Path: /other/data2.dat
      - type: FileInformation
        EventTimestamp: '2025-01-03T10:00:00.000000045'
        FileId: 100
        Path: /unique/file2.idx
      - type: IndexCacheHit
        EventTimestamp: '2025-01-03T10:00:00.000000200'
        FileId: 10
        FileOffset: 1024
        NodeKind: 0
        InMemorySize: 4096
        ExpandTime: 60
      - type: IndexCacheMiss
        EventTimestamp: '2025-01-03T10:00:00.000000250'
        FileId: 50
        FileOffset: 4096
        NodeKind: 0
      - type: IndexCacheMiss
        EventTimestamp: '2025-01-03T10:00:00.000000400'
        FileId: 100
        FileOffset: 5120
        NodeKind: 0
expect:
  event:
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000020'
    FileId: 1
    Path: /shared/index.idx
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000025'
    FileId: 1
    Path: /shared/index.idx
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000030'
    FileId: 2
    Path: /local/data1.dat
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000035'
    FileId: 3
    Path: /other/data2.dat
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000040'
    FileId: 4
    Path: /unique/file1.idx
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
  - type: FileInformation
    EventTimestamp: '2025-01-03T10:00:00.000000045'
    FileId: 5
    Path: /unique/file2.idx
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-03T10:00:00.000000100'
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
    FileId: 1
    FileOffset: 1024
    NodeKind: 0
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-03T10:00:00.000000150'
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
    FileId: 2
    FileOffset: 2048
    NodeKind: 0
  - type: IndexCacheHit
    EventTimestamp: '2025-01-03T10:00:00.000000200'
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
    FileId: 1
    FileOffset: 1024
    NodeKind: 0
    InMemorySize: 4096
    ExpandTime: 60
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-03T10:00:00.000000250'
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
    FileId: 3
    FileOffset: 4096
    NodeKind: 0
  - type: IndexLoad
    EventTimestamp: '2025-01-03T10:00:00.000000300'
    ChannelId: 1
    ReplicaId: 1
    InstanceId: 1
    FileId: 4
    FileOffset: 3072
    NodeKind: 0
    InMemorySize: 4096
    ReadTime: 50
    ExpandTime: 10
  - type: IndexCacheMiss
    EventTimestamp: '2025-01-03T10:00:00.000000400'
    ChannelId: 2
    ReplicaId: 1
    InstanceId: 1
    FileId: 5
    FileOffset: 5120
    NodeKind: 0

)!!!";
        testEventVisitationLinks(testData, PTEFlenientParsing);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventIteratorTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventIteratorTests, "eventiterator");

#endif
