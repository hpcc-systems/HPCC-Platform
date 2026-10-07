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
#include <utility>
#include <vector>

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

// Incomplete implementation of IEventMultiplexer that is an abstract base for multiple
// multiplexing strategies.
class event_decl CEventMultiplexer : public CInterfaceOf<IEventMultiplexer>
{
public:
    using IEventMultiplexer::nextEvent;
    virtual const EventFileProperties& queryFileProperties() const override;
    virtual void addSource(IEventIterator& source) override;
    virtual bool nextEvent(CEvent& event) override;
    virtual bool nextEvent(CEvent& event, EventIterationTransition& transition) final override;

public:
    CEventMultiplexer(CMetaInfoState& _metaState, bool bypassMetaCollector);
    // Tests if a subclass source collection references the specified iterator.
    virtual bool contains(IEventIterator& source) const = 0;

protected:
    // Tests if the subclass source collection contains no sources.
    virtual bool isEmpty() const = 0;
    // Tests for same instance or same path from properties. Same instance is a logic error that
    // yields an exception. Same path is a user error that blocks an add from occurring without
    // throwing an exception.
    virtual bool areSame(IEventIterator& candidate, IEventIterator& existing) const;
    // Applies subclass-specific conditions to source acceptance.
    virtual bool acceptsSource(IEventIterator& source) { return true; };
    // Adds the unique source to the subclass collection. Returns false when the source contains
    // no events and is not retained. Implementations must complete their mutation before
    // returning; callers use the result only for metadata notification.
    virtual bool onAddSource(IEventIterator& source) = 0;
    // Subclass-specific implementation of event retrieval logic following common preparation steps.
    virtual bool nextEventImpl(CEvent& event, EventIterationTransition& transition) = 0;

    CMetaInfoState& metaState;
    Owned<IEventVisitor> metaStateCollector;
    EventFileProperties properties;
    // Keep a completed source alive until the caller has consumed the transition properties.
    Linked<IEventIterator> pendingDeparture;
    bool acceptSources{true};
    bool bypassMetaCollector{false};

    void releasePendingDeparture()
    {
        pendingDeparture.clear();
    }

    void beginSource(IEventIterator& source, bool& visited, EventIterationTransition& transition)
    {
        transition.properties = &source.queryFileProperties();
        transition.firstVisit = !visited;
        visited = true;
    }
};

bool CEventMultiplexer::nextEvent(CEvent& event)
{
    EventIterationTransition transition{};
    return nextEvent(event, transition);
}

bool CEventMultiplexer::nextEvent(CEvent& event, EventIterationTransition& transition)
{
    transition = {};
    releasePendingDeparture();
    return nextEventImpl(event, transition);
}

const EventFileProperties& CEventMultiplexer::queryFileProperties() const
{
    return properties;
}

void CEventMultiplexer::addSource(IEventIterator& source)
{
    if (!acceptSources)
        throw makeStringException(0, "event multiplexer cannot add a source after event consumption has started");
    if (areSame(source, *this))
        return;
    if (!acceptsSource(source))
        return;
    const EventFileProperties& sourceProps = source.queryFileProperties();
    if (isEmpty())
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
    else if (!contains(source))
    {
        const char* actual = sourceProps.processDescriptor.str();
        const char* expected = properties.processDescriptor.str();
        if (!streq(expected, actual))
            throw makeStringExceptionV(0, "file source mismatch - needed ProcessDescriptor '%s' but found '%s'", expected, actual);

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
        auto updateTriState = [](EventFileOption& current, EventFileOption sourceOpt) {
            if (EventFileOption::Ambiguous == current)
                return; // already ambiguous
            if (current == sourceOpt)
                return; // still the same
            current = EventFileOption::Ambiguous;
        };
        updateTriState(properties.options.includeTraceIds, sourceProps.options.includeTraceIds);
        updateTriState(properties.options.includeThreadIds, sourceProps.options.includeThreadIds);
        updateTriState(properties.options.includeStackTraces, sourceProps.options.includeStackTraces);
    }
    else
        return; // Reject instances that are duplicates or already contained
    if (onAddSource(source) && metaStateCollector)
        metaStateCollector->visitFile(sourceProps.path.str(), sourceProps.version);
}

CEventMultiplexer::CEventMultiplexer(CMetaInfoState& _metaState, bool _bypassMetaCollector)
    : metaState(_metaState)
    , bypassMetaCollector(_bypassMetaCollector)
{
    if (!bypassMetaCollector)
        metaStateCollector.setown(metaState.getCollector());
    properties.path.set("multiplexed");
}

bool CEventMultiplexer::areSame(IEventIterator& needle, IEventIterator& within) const
{
    // it is a logic error for the same instance to be added twice
    if (&needle == &within)
        throw makeStringException(0, "event multiplexer cannot add a source already in use");
    // it is a recoverable user error to refer to the same file twice
    const char* path = needle.queryFileProperties().path.str();
    if (!isEmptyString(path) && streq(path, within.queryFileProperties().path.str()))
        return true;
    return false;
}

// Concrete extension of CEventMultiplexer that distributes events in chronological order according
// to the EventTimestamp attribute value. Events with timestamps are always distributed before
// events without.
//
// An event file iterator is guaranteed to provide events with timestamps and in chronological
// order.
//
// A property tree event iterator provides events in the order they appear in the tree, with or
// without timestamps. If a multiplexer source input provides events in non-chronological order,
// this iterator will distribute those events in non-chronological order.
class event_decl CChronologicalEventMultiplexer : public CEventMultiplexer
{
public:
    struct Source
    {
        Linked<IEventIterator> iterator;
        CEvent nextEvent;
        bool visited{false};
    };
    using Sources = std::list<Source>;
    using CEventMultiplexer::CEventMultiplexer;

    virtual bool nextEventImpl(CEvent& event, EventIterationTransition& transition) override;
protected:
    virtual bool contains(IEventIterator& source) const override;
    virtual bool isEmpty() const override { return sources.empty(); }
    virtual bool acceptsSource(IEventIterator& source) override;
    virtual bool onAddSource(IEventIterator& source) override;

private:
    Sources sources;
    CEvent firstEvent;
};

bool CChronologicalEventMultiplexer::nextEventImpl(CEvent& event, EventIterationTransition& transition)
{
    // If at least one source has a next event with a timestamp, choose the timestamped event with
    // the lowest chronological value.
    Sources::iterator best = sources.end();
    for (Sources::iterator it = sources.begin(); it != sources.end(); ++it)
    {
        if (it->nextEvent.queryType() == EventNone)
            continue;
        if (!it->nextEvent.hasAttribute(EvAttrEventTimestamp))
            continue;
        if (sources.end() == best)
            best = it;
        else if (it->nextEvent.queryNumericValue(EvAttrEventTimestamp) < best->nextEvent.queryNumericValue(EvAttrEventTimestamp))
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
        beginSource(*best->iterator, best->visited, transition);
        event = best->nextEvent;
        if (metaStateCollector)
            (void)metaStateCollector->visitEvent(event);
        else if (bypassMetaCollector)
            (void)metaState.tryRemapFileId(event);
        properties.eventsRead++;
        acceptSources = false;

        if (!best->iterator->nextEvent(best->nextEvent))
        {
            // Source completed - accumulate final stats
            const EventFileProperties& sourceProps = best->iterator->queryFileProperties();
            properties.bytesRead += sourceProps.bytesRead;
            transition.lastVisit = true;
            pendingDeparture.set(best->iterator);
            sources.erase(best);
        }
        return true;
    }
    else
        return false;
}

bool CChronologicalEventMultiplexer::contains(IEventIterator& source) const
{
    for (const auto& entry : sources)
    {
        if (areSame(source, *entry.iterator))
            return true;
        CEventMultiplexer* nested = dynamic_cast<CEventMultiplexer*>(entry.iterator.get());
        if (nested && nested->contains(source))
            return true;
    }
    return false;
}

bool CChronologicalEventMultiplexer::acceptsSource(IEventIterator& source)
{
    return source.nextEvent(firstEvent);
}

bool CChronologicalEventMultiplexer::onAddSource(IEventIterator& source)
{
    Source entry;
    entry.iterator.set(&source);
    entry.nextEvent = firstEvent;
    sources.emplace_back(std::move(entry));
    return true;
}

IEventMultiplexer* createChronologicalEventMultiplexer(CMetaInfoState& metaState, bool bypassMetaCollector)
{
    return new CChronologicalEventMultiplexer(metaState, bypassMetaCollector);
}

// Concrete extension of CEventMultiplexer that distributes events from each source sequentially.
class event_decl CSerialEventMultiplexer : public CEventMultiplexer
{
public:
    struct Source
    {
        Linked<IEventIterator> iterator;
        CEvent nextEvent;
        bool visited{false};
    };
    using Sources = std::vector<Source>;
    using CEventMultiplexer::CEventMultiplexer;

    virtual bool nextEventImpl(CEvent& event, EventIterationTransition& transition) override;
protected:
    virtual bool contains(IEventIterator& source) const override;
    virtual bool isEmpty() const override { return sources.empty(); }
    virtual bool acceptsSource(IEventIterator& source) override;
    virtual bool onAddSource(IEventIterator& source) override;

private:
    Sources sources;
    size_t currentSourceIndex = 0;
};

bool CSerialEventMultiplexer::nextEventImpl(CEvent& event, EventIterationTransition& transition)
{
    while (currentSourceIndex < sources.size())
    {
        Source& source = sources[currentSourceIndex];
        if (source.nextEvent.queryType() != EventNone)
        {
            beginSource(*source.iterator, source.visited, transition);
            event = source.nextEvent;
            if (!source.iterator->nextEvent(source.nextEvent))
            {
                properties.bytesRead += source.iterator->queryFileProperties().bytesRead;
                transition.lastVisit = true;
                currentSourceIndex++;
            }
            if (metaStateCollector)
                (void)metaStateCollector->visitEvent(event);
            else if (bypassMetaCollector)
                (void)metaState.tryRemapFileId(event);
            properties.eventsRead++;
            acceptSources = false;
            return true;
        }

        properties.bytesRead += source.iterator->queryFileProperties().bytesRead;
        currentSourceIndex++;
    }
    return false;
}

bool CSerialEventMultiplexer::acceptsSource(IEventIterator& source)
{
    return true;
}

bool CSerialEventMultiplexer::contains(IEventIterator& source) const
{
    for (const auto& entry : sources)
    {
        if (areSame(source, *entry.iterator))
            return true;
        CEventMultiplexer* nested = dynamic_cast<CEventMultiplexer*>(entry.iterator.get());
        if (nested && nested->contains(source))
            return true;
    }
    return false;
}
bool CSerialEventMultiplexer::onAddSource(IEventIterator& source)
{
    Source entry;
    entry.iterator.set(&source);
    if (source.nextEvent(entry.nextEvent))
    {
        sources.emplace_back(std::move(entry));
        return true;
    }
    return false;
}

IEventMultiplexer* createSerialEventMultiplexer(CMetaInfoState& metaState, bool bypassMetaCollector)
{
    return new CSerialEventMultiplexer(metaState, bypassMetaCollector);
}

void visitIterableEvents(IEventIterator& iter, IEventVisitor& visitor)
{
    CEvent event;
    const EventFileProperties& props = iter.queryFileProperties();
    visitor.visitFile(props.path, props.version);
    IEventMultiplexer* multiplexer = dynamic_cast<IEventMultiplexer*>(&iter);
    if (multiplexer)
    {
        EventIterationTransition transition;
        while (multiplexer->nextEvent(event, transition))
        {
            if (transition.firstVisit)
                visitor.visitFile(transition.properties->path, transition.properties->version);
            visitor.visitEvent(event);
            if (transition.lastVisit)
                visitor.departFile(transition.properties->bytesRead);
        }
    }
    else
    {
        while (iter.nextEvent(event))
            visitor.visitEvent(event);
    }
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
    CPPUNIT_TEST(testMultiplexerMetaCollectorEnabled);
    CPPUNIT_TEST(testMultiplexerMetaCollectorBypassed);
    CPPUNIT_TEST(testMultiplexerBypassedRemapsFileId);
    CPPUNIT_TEST(testMultiplexerVisitsNonemptySourcesLazily);
    CPPUNIT_TEST(testSerialMultiplexerVisitsNonemptySourcesLazily);
    CPPUNIT_TEST_SUITE_END();

public:
    class BoundaryVisitor : public CInterfaceOf<IEventVisitor>
    {
    public:
        virtual bool visitFile(const char* filename, uint32_t) override
        {
            files.emplace_back(filename ? filename : "");
            sequence.emplace_back(std::string("visit:") + (filename ? filename : ""));
            return true;
        }

        virtual bool visitEvent(CEvent& event) override
        {
            events++;
            sequence.emplace_back(std::string("event:") + queryEventName(event.queryType()));
            return true;
        }

        virtual void departFile(uint32_t bytesRead) override
        {
            departures++;
            departBytes.push_back(bytesRead);
            sequence.emplace_back("depart");
        }

        std::vector<std::string> files;
        std::vector<std::string> sequence;
        std::vector<uint32_t> departBytes;
        unsigned events{0};
        unsigned departures{0};
    };

    void testMultiplexerVisitsNonemptySourcesLazily()
    {
        Owned<IPropertyTree> emptyTree = createPTreeFromXMLString("<events filename='empty.evt' version='1'/>");
        Owned<IPropertyTree> firstTree = createPTreeFromXMLString("<events filename='first.evt' version='1'><event type='IndexCacheMiss' EventTimestamp='1'/><event type='IndexCacheMiss' EventTimestamp='3'/><event type='IndexCacheMiss' EventTimestamp='5'/></events>");
        Owned<IPropertyTree> secondTree = createPTreeFromXMLString("<events filename='second.evt' version='1'><event type='IndexCacheMiss' EventTimestamp='2'/></events>");
        Owned<IEventIterator> empty = createPropertyTreeEvents(*emptyTree, PTEFlenientParsing);
        Owned<IEventIterator> first = createPropertyTreeEvents(*firstTree, PTEFlenientParsing);
        Owned<IEventIterator> second = createPropertyTreeEvents(*secondTree, PTEFlenientParsing);
        CMetaInfoState metaState;
        Owned<IEventMultiplexer> multiplexer = createChronologicalEventMultiplexer(metaState, true);
        multiplexer->addSource(*empty);
        multiplexer->addSource(*first);
        multiplexer->addSource(*second);

        BoundaryVisitor visitor;
        visitIterableEvents(*multiplexer, visitor);

        CPPUNIT_ASSERT_EQUAL(size_t(3), visitor.files.size());
        CPPUNIT_ASSERT_EQUAL(std::string("multiplexed"), visitor.files[0]);
        CPPUNIT_ASSERT_EQUAL(std::string("first.evt"), visitor.files[1]);
        CPPUNIT_ASSERT_EQUAL(std::string("second.evt"), visitor.files[2]);
        CPPUNIT_ASSERT_EQUAL(unsigned(4), visitor.events);
        CPPUNIT_ASSERT_EQUAL(unsigned(3), visitor.departures);
        const std::vector<std::string> expectedSequence{
            "visit:multiplexed",
            "visit:first.evt",
            "event:IndexCacheMiss",
            "visit:second.evt",
            "event:IndexCacheMiss",
            "depart",
            "event:IndexCacheMiss",
            "event:IndexCacheMiss",
            "depart",
            "depart",
        };
        CPPUNIT_ASSERT(expectedSequence == visitor.sequence);
    }

    void testSerialMultiplexerVisitsNonemptySourcesLazily()
    {
        Owned<IPropertyTree> emptyTree = createPTreeFromXMLString("<events filename='empty.evt' version='1' bytesRead='0'/>");
        Owned<IPropertyTree> firstTree = createPTreeFromXMLString("<events filename='first.evt' version='1' bytesRead='11'><event type='IndexCacheMiss'/></events>");
        Owned<IPropertyTree> secondTree = createPTreeFromXMLString("<events filename='second.evt' version='1' bytesRead='22'><event type='IndexCacheMiss'/><event type='IndexCacheMiss'/></events>");
        Owned<IEventIterator> empty = createPropertyTreeEvents(*emptyTree, PTEFlenientParsing);
        Owned<IEventIterator> first = createPropertyTreeEvents(*firstTree, PTEFlenientParsing);
        Owned<IEventIterator> second = createPropertyTreeEvents(*secondTree, PTEFlenientParsing);
        CMetaInfoState metaState;
        Owned<IEventMultiplexer> multiplexer = createSerialEventMultiplexer(metaState, true);
        multiplexer->addSource(*empty);
        multiplexer->addSource(*first);
        multiplexer->addSource(*second);

        BoundaryVisitor visitor;
        visitIterableEvents(*multiplexer, visitor);

        const std::vector<std::string> expectedSequence{
            "visit:multiplexed",
            "visit:first.evt",
            "event:IndexCacheMiss",
            "depart",
            "visit:second.evt",
            "event:IndexCacheMiss",
            "event:IndexCacheMiss",
            "depart",
            "depart",
        };
        CPPUNIT_ASSERT(expectedSequence == visitor.sequence);
        const std::vector<uint32_t> expectedDepartBytes{11, 22, 33};
        CPPUNIT_ASSERT(expectedDepartBytes == visitor.departBytes);
    }

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
        CPPUNIT_ASSERT_MESSAGE("expected exception not thrown", testEventVisitationLinksThrowsIException(testData));
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
        CPPUNIT_ASSERT_MESSAGE("expected exception not thrown", testEventVisitationLinksThrowsIException(testData));
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
        CPPUNIT_ASSERT_MESSAGE("expected exception not thrown", testEventVisitationLinksThrowsIException(testData));
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
        CPPUNIT_ASSERT_MESSAGE("expected exception not thrown", testEventVisitationLinksThrowsIException(testData));
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

    void testMultiplexerMetaCollectorEnabled()
    {
        START_TEST
        Owned<IPropertyTree> sourceConfig = createTestConfiguration(R"!!!(
source:
  - event:
    - type: QueryStart
      EventTimestamp: '2025-01-03T10:00:00.000000100'
      EventTraceId: trace-123
      ServiceName: dali-service
  - event:
    - type: IndexCacheMiss
      EventTimestamp: '2025-01-03T10:00:00.000000200'
      EventTraceId: trace-123
)!!!");

        CMetaInfoState metaState;
        const IPropertyTree* source1Tree = sourceConfig->queryPropTree("source[1]");
        const IPropertyTree* source2Tree = sourceConfig->queryPropTree("source[2]");
        CPPUNIT_ASSERT(source1Tree != nullptr && source2Tree != nullptr);
        Owned<IEventIterator> source1 = createPropertyTreeEvents(*source1Tree, PTEFlenientParsing);
        Owned<IEventIterator> source2 = createPropertyTreeEvents(*source2Tree, PTEFlenientParsing);
        Owned<IEventMultiplexer> mux = createChronologicalEventMultiplexer(metaState, false);
        mux->addSource(*source1);
        mux->addSource(*source2);

        CEvent event;
        while (mux->nextEvent(event))
        {
        }

        const char* serviceName = metaState.queryServiceName("trace-123");
        CPPUNIT_ASSERT(serviceName != nullptr && streq(serviceName, "dali-service"));
        END_TEST
    }

    void testMultiplexerMetaCollectorBypassed()
    {
        START_TEST
        Owned<IPropertyTree> sourceConfig = createTestConfiguration(R"!!!(
source:
  - event:
    - type: QueryStart
      EventTimestamp: '2025-01-03T10:00:00.000000100'
      EventTraceId: trace-123
      ServiceName: dali-service
  - event:
    - type: IndexCacheMiss
      EventTimestamp: '2025-01-03T10:00:00.000000200'
      EventTraceId: trace-123
)!!!");

        CMetaInfoState metaState;
        const IPropertyTree* source1Tree = sourceConfig->queryPropTree("source[1]");
        const IPropertyTree* source2Tree = sourceConfig->queryPropTree("source[2]");
        CPPUNIT_ASSERT(source1Tree != nullptr && source2Tree != nullptr);
        Owned<IEventIterator> source1 = createPropertyTreeEvents(*source1Tree, PTEFlenientParsing);
        Owned<IEventIterator> source2 = createPropertyTreeEvents(*source2Tree, PTEFlenientParsing);
        Owned<IEventMultiplexer> mux = createChronologicalEventMultiplexer(metaState, true);
        mux->addSource(*source1);
        mux->addSource(*source2);

        CEvent event;
        unsigned count = 0;
        while (mux->nextEvent(event))
            count++;

        CPPUNIT_ASSERT_EQUAL(2u, count);
        const char* serviceName = metaState.queryServiceName("trace-123");
        CPPUNIT_ASSERT(isEmptyString(serviceName));
        END_TEST
    }

    // Verify that FileId remapping still occurs when the collector is bypassed, i.e. the
    // multiplexer applies tryRemapFileId() directly on emitted events in bypass mode.
    // This exercises the structural path introduced to recover second-pass performance.
    void testMultiplexerBypassedRemapsFileId()
    {
        START_TEST
        // Two sources each referencing the same physical file under different source-local FileIds.
        // Source 1: ChannelId=1, FileId=10 -> /shared/index.idx
        // Source 2: ChannelId=2, FileId=99 -> /shared/index.idx
        // After pre-scan metadata build, bypassMetaCollector=true is used for the main pass.
        // Both sources' index events must arrive with the unified runtime FileId (1).
        Owned<IPropertyTree> sourceConfig = createTestConfiguration(R"!!!(
source:
  - event:
    - type: RecordingSource
      ProcessDescriptor: testproc
      ChannelId: 1
      ReplicaId: 0
      InstanceId: 42
    - type: FileInformation
      FileId: 10
      Path: /shared/index.idx
    - type: IndexCacheMiss
      EventTimestamp: '2025-01-03T10:00:00.000000100'
      FileId: 10
      FileOffset: 0
      NodeKind: 0
  - event:
    - type: RecordingSource
      ProcessDescriptor: testproc
      ChannelId: 2
      ReplicaId: 0
      InstanceId: 42
    - type: FileInformation
      FileId: 99
      Path: /shared/index.idx
    - type: IndexCacheHit
      EventTimestamp: '2025-01-03T10:00:00.000000200'
      FileId: 99
      FileOffset: 0
      NodeKind: 0
      InMemorySize: 4096
      ExpandTime: 10
)!!!");

        // Pre-scan pass: collector active, builds sourceToProps mapping
        CMetaInfoState metaState;
        {
            const IPropertyTree* s1 = sourceConfig->queryPropTree("source[1]");
            const IPropertyTree* s2 = sourceConfig->queryPropTree("source[2]");
            CPPUNIT_ASSERT(s1 != nullptr && s2 != nullptr);
            Owned<IEventIterator> src1 = createPropertyTreeEvents(*s1, PTEFlenientParsing);
            Owned<IEventIterator> src2 = createPropertyTreeEvents(*s2, PTEFlenientParsing);
            Owned<IEventMultiplexer> preScanMux = createChronologicalEventMultiplexer(metaState, false);
            preScanMux->addSource(*src1);
            preScanMux->addSource(*src2);
            CEvent e;
            while (preScanMux->nextEvent(e))
            {
            }
        }

        // Main pass: collector bypassed, remap must still work
        const IPropertyTree* s1 = sourceConfig->queryPropTree("source[1]");
        const IPropertyTree* s2 = sourceConfig->queryPropTree("source[2]");
        CPPUNIT_ASSERT(s1 != nullptr && s2 != nullptr);
        Owned<IEventIterator> src1 = createPropertyTreeEvents(*s1, PTEFlenientParsing);
        Owned<IEventIterator> src2 = createPropertyTreeEvents(*s2, PTEFlenientParsing);
        Owned<IEventMultiplexer> mainMux = createChronologicalEventMultiplexer(metaState, true);
        mainMux->addSource(*src1);
        mainMux->addSource(*src2);

        CEvent event;
        unsigned indexEventCount = 0;
        while (mainMux->nextEvent(event))
        {
            EventType t = event.queryType();
            if (t == EventIndexCacheMiss || t == EventIndexCacheHit)
            {
                // Both index events must carry the unified runtime FileId (1) not their
                // source-local values (10 or 99).
                CPPUNIT_ASSERT_EQUAL(1u, unsigned(event.queryNumericValue(EvAttrFileId)));
                indexEventCount++;
            }
        }
        CPPUNIT_ASSERT_EQUAL(2u, indexEventCount);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventIteratorTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventIteratorTests, "eventiterator");

#endif
