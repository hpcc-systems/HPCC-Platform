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

#include "eventdescribe.h"
#include "eventmetaparser.hpp"
#include "jevent.hpp"
#include "jptree.hpp"
#include <algorithm>
#include <array>
#include <cstddef>
#include <initializer_list>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

namespace
{
// Lightweight iterable view over a contiguous enum interval [begin, end).
// Assumptions:
// 1. Enum values in the interval are dense and incrementable by +1 on the underlying type.
// 2. begin/end delimit a valid half-open range where end is a reachable sentinel.
// 3. No filtering is applied; callers provide any policy (e.g. visibility) separately.
template <typename Enum>
class EnumRange
{
public:
    class Iterator
    {
    public:
        constexpr explicit Iterator(Enum _value) : value(_value)
        {
        }

        constexpr Enum operator*() const
        {
            return value;
        }

        constexpr Iterator & operator++()
        {
            value = static_cast<Enum>(static_cast<std::underlying_type_t<Enum>>(value) + 1);
            return *this;
        }

        constexpr bool operator != (const Iterator & other) const
        {
            return value != other.value;
        }

    private:
        Enum value;
    };

    constexpr EnumRange(Enum _begin, Enum _end) : beginValue(_begin), endValue(_end)
    {
    }

    constexpr Iterator begin() const
    {
        return Iterator(beginValue);
    }

    constexpr Iterator end() const
    {
        return Iterator(endValue);
    }

private:
    const Enum beginValue;
    const Enum endValue;
};

constexpr EnumRange<EventType> allEventTypes()
{
    return EnumRange<EventType>(static_cast<EventType>(EventNone + 1), EventMax);
}

// Events can be unobservable by default in describe output unless explicitly requested.
// This currently includes RecordingSource; additional policy exclusions can be added here.
static bool isUnobservableEvent(EventType type)
{
    switch (type)
    {
    case EventRecordingSource:
        return true;
    default:
        return false;
    }
}

static bool isObservableEvent(EventType type)
{
    return !isUnobservableEvent(type);
}

struct MetaRule
{
    const char* name;
    struct Dependency
    {
        EventType event;
        EventAttr attr;
    };
    const Dependency* dependencies;
    size_t numDependencies;
};

// Capturing dependencies by reference-to-array preserves the exact element count
// at compile time, avoiding sentinel values and fixed-size limits.
template <size_t N>
constexpr MetaRule makeMetaRule(const char* name, const MetaRule::Dependency (&dependencies)[N])
{
    static_assert(N > 0, "meta rules must define at least one dependency");
    return { name, dependencies, N };
}

// Single source of truth for derived meta attributes. Adding a new meta attribute requires
// a new dependency array and a new row here. All derived tables (enum, name, rules) are
// generated from this macro. Anticipated future home: eventmetaparser.hpp.
#define FOR_EACH_META_ATTRIBUTE(DO) \
    DO(ServiceName,      EVENT_META_SERVICE_NAME,        metaServiceNameDependencies)      \
    DO(LogicalFileName,  EVENT_META_LOGICAL_FILE_NAME,   metaLogicalFileNameDependencies)  \
    DO(Path,             EVENT_META_PATH,                metaPathDependencies)             \
    DO(Plane,            EVENT_META_PLANE,               metaPlaneDependencies)

static constexpr MetaRule::Dependency metaServiceNameDependencies[] = {
    { EventNone, EvAttrEventTraceId },
    { EventQueryStart, EvAttrEventTraceId },
    { EventQueryStart, EvAttrServiceName },
};

static constexpr MetaRule::Dependency metaLogicalFileNameDependencies[] = {
    { EventNone, EvAttrFileId },
    { MetaFileInformation, EvAttrFileId },
    { MetaFileInformation, EvAttrPath },
    { MetaPlaneInformation, EvAttrPath },
    { MetaPlaneInformation, EvAttrIsStriped },
};

static constexpr MetaRule::Dependency metaPathDependencies[] = {
    { EventNone, EvAttrFileId },
    { MetaFileInformation, EvAttrFileId },
    { MetaFileInformation, EvAttrPath },
};

static constexpr MetaRule::Dependency metaPlaneDependencies[] = {
    { EventNone, EvAttrFileId },
    { MetaFileInformation, EvAttrFileId },
    { MetaFileInformation, EvAttrPath },
    { MetaPlaneInformation, EvAttrPath },
    { MetaPlaneInformation, EvAttrPlane },
};

#define META_ATTR_RULE(tag, name, deps) makeMetaRule(name, deps),
static constexpr MetaRule metaRules[] = {
    FOR_EACH_META_ATTRIBUTE(META_ATTR_RULE)
};
#undef META_ATTR_RULE

// Enumeration of derived meta attributes, parallel to EventType/EventContext/EventAttr in jevent.hpp.
// Anticipated future home: eventmetaparser.hpp, alongside queryMetaAttribute/queryMetaAttributeName.
#define META_ATTR_ENUM(tag, name, deps) MetaAttr##tag,
enum EventMetaAttr : byte
{
    FOR_EACH_META_ATTRIBUTE(META_ATTR_ENUM)
    MetaAttrMax
};
#undef META_ATTR_ENUM

#define META_ATTR_NAME(tag, name, deps) name,
static constexpr const char* metaAttrNames[] = {
    FOR_EACH_META_ATTRIBUTE(META_ATTR_NAME)
};
#undef META_ATTR_NAME
static_assert(_elements_in(metaAttrNames) == MetaAttrMax);

inline const char* queryMetaAttributeName(EventMetaAttr attr)
{
    if (attr < MetaAttrMax)
        return metaAttrNames[attr];
    return nullptr;
}

static inline const MetaRule* getMetaRule(EventMetaAttr attr)
{
    if (attr >= MetaAttrMax)
        return nullptr;
    return &metaRules[attr];
}

} // namespace

#define JEVENT_EVENT_ATTR_VALUE_ENTRY(name, type, unit) EvAttr##name,
static constexpr EventAttr eventAttrs[] = {
    FOR_EACH_EVENT_ATTR_VALUE(JEVENT_EVENT_ATTR_VALUE_ENTRY)
};
static constexpr size_t numEventAttrs = _elements_in(eventAttrs);
#undef JEVENT_EVENT_ATTR_VALUE_ENTRY

#define JEVENT_CONTEXT_VALUE_ENTRY(name, bits) EventCtx##name,
static constexpr EventContext eventContexts[] = {
    FOR_EACH_EVENT_CONTEXT_VALUE(JEVENT_CONTEXT_VALUE_ENTRY)
};
static constexpr size_t numEventContexts = _elements_in(eventContexts);
#undef JEVENT_CONTEXT_VALUE_ENTRY

#define ForEachContextBitIn(indexName, contextName, contextMask)                                \
    for (size_t indexName = 0; indexName < numEventContexts; ++indexName)                       \
        if (EventContext contextName = eventContexts[indexName]; ((contextMask) & contextName) != EventCtxNone)

#define META_ATTR_VALUE_ENTRY(tag, name, deps) MetaAttr##tag,
static constexpr EventMetaAttr eventMetaAttrs[] = {
    FOR_EACH_META_ATTRIBUTE(META_ATTR_VALUE_ENTRY)
};
static constexpr size_t numEventMetaAttrs = _elements_in(eventMetaAttrs);
#undef META_ATTR_VALUE_ENTRY

static const char* queryLocalEventContextName(EventContext context)
{
    switch (context)
    {
#define JEVENT_CONTEXT_NAME_CASE(name, bits) case EventCtx##name: return #name;
    FOR_EACH_EVENT_CONTEXT_VALUE(JEVENT_CONTEXT_NAME_CASE)
#undef JEVENT_CONTEXT_NAME_CASE
    default:
        return nullptr;
    }
}

void CDescribeEventsOp::setFormat(DescribeOutputFormat _format)
{
    format = _format;
}

void CDescribeEventsOp::setSectionOverrides(DescribeSection sections)
{
    sectionOverrides = sections;
}

void CDescribeEventsOp::addSectionOverride(DescribeSection section)
{
    sectionOverrides |= section;
}

bool CDescribeEventsOp::ready() const
{
    // Unlike most consuming operations, describe supports schema-only output.
    return out.get() != nullptr;
}

bool CDescribeEventsOp::doOp()
{
    Owned<IPropertyTree> description = createPTree("describe");
    appendDescriptionTree(*description);

    StringBuffer output;
    switch (format)
    {
    case DescribeOutputFormat::xml:
        toXML(description, output, 0, XML_Format);
        break;
    case DescribeOutputFormat::json:
        toJSON(description, output, 2, JSON_Format);
        break;
    case DescribeOutputFormat::yaml:
        toYAML(description, output, 2, 0);
        break;
    default:
        throw makeStringExceptionV(-1, "unsupported output format value: %u", static_cast<unsigned>(format));
    }

    out->put(output.length(), output.str());
    out->put(1, "\n");
    return true;
}

namespace
{
// Describe uses semantic gating for derived meta attributes so eligibility
// reflects resolvability, not only schema-level participation.
static bool isSemanticallyEligibleForMeta(EventType type, EventMetaAttr metaAttr)
{
    switch (metaAttr)
    {
    case MetaAttrServiceName:
        switch (type)
        {
        case MetaPlaneInformation:
        case MetaFileInformation:
        case EventRecordingSource:
            return false;
        default:
            return true;
        }
    case MetaAttrLogicalFileName:
    case MetaAttrPlane:
        // PlaneInformation contributes matching context but does not provide
        // independent file identity for resolvable derived values.
        return type != MetaPlaneInformation;
    default:
        return true;
    }
}

// Returns true if the event has at least one attribute satisfying any dependency
// of the meta rule (either generic or event-specific).
static bool isEventEligibleForMeta(EventType type, const MetaRule& rule)
{
    const auto& attrs = queryEventAttributeIds(type);
    for (size_t i = 0; i < rule.numDependencies; ++i)
    {
        const MetaRule::Dependency& dep = rule.dependencies[i];
        if (dep.event != EventNone && dep.event != type)
            continue;
        if (std::any_of(attrs.begin(), attrs.end(), [&dep](EventAttr a) { return a == dep.attr; }))
            return true;
    }
    return false;
}

// Aggregated cross-reference index and named selection, built once from the
// observable event set. Render functions query this instead of scanning descriptions.
class DescribeContext
{
public:
    std::vector<EventType>                             events;
    std::array<std::vector<EventType>, numEventContexts> contextEvents;
    std::array<std::vector<EventType>, EvAttrMax>      attrEvents;
    std::array<std::vector<EventType>, numEventMetaAttrs> metaEvents;

public:
    DescribeContext();

    bool eventMatches(EventType t) const;
    bool attrMatches(EventAttr a) const;
    bool contextMatches(EventContext c) const;
    bool metaMatches(EventMetaAttr idx) const;

private:
    void buildObservableEventIndex();
    void indexObservableEvent(EventType type);
    void indexEventAttributes(EventType type);
    void indexEventMeta(EventType type);
};

DescribeContext::DescribeContext()
{
    buildObservableEventIndex();
}

void DescribeContext::buildObservableEventIndex()
{
    for (EventType type : allEventTypes())
    {
        if (!isObservableEvent(type))
            continue;

        indexObservableEvent(type);
    }
}

void DescribeContext::indexObservableEvent(EventType type)
{
    events.push_back(type);
    EventContext contextMask = queryEventContext(type);
    ForEachContextBitIn(contextIndex, contextBit, contextMask)
        contextEvents[contextIndex].push_back(type);
    indexEventAttributes(type);
    indexEventMeta(type);
}

void DescribeContext::indexEventAttributes(EventType type)
{
    for (EventAttr attr : queryEventAttributeIds(type))
    {
        attrEvents[attr].push_back(type);
    }
}

void DescribeContext::indexEventMeta(EventType type)
{
    for (EventMetaAttr metaAttr : eventMetaAttrs)
    {
        if (!isSemanticallyEligibleForMeta(type, metaAttr))
            continue;
        const MetaRule* rule = getMetaRule(metaAttr);
        if (rule && isEventEligibleForMeta(type, *rule))
            metaEvents[metaAttr].push_back(type);
    }
}

bool DescribeContext::eventMatches(EventType) const
{
    return true;
}

bool DescribeContext::attrMatches(EventAttr) const
{
    return true;
}

bool DescribeContext::contextMatches(EventContext) const
{
    return true;
}

bool DescribeContext::metaMatches(EventMetaAttr) const
{
    return true;
}

class DescribeRenderer
{
public:
    DescribeRenderer(IPropertyTree& _output, const DescribeContext& _ctx)
        : output(_output)
        , ctx(_ctx)
    {
    }

    void appendContexts();
    void appendEvents();
    void appendAttributes();

private:
    template <class Func>
    void forEachRenderableMeta(Func&& func) const
    {
        for (EventMetaAttr metaAttr : eventMetaAttrs)
        {
            if (!ctx.metaMatches(metaAttr))
                continue;

            const MetaRule* rule = getMetaRule(metaAttr);
            if (!rule)
                continue;

            func(metaAttr, *rule);
        }
    }
    void appendTerseMetaItems(const char* elementName);

private:
    IPropertyTree& output;
    const DescribeContext& ctx;
};

void DescribeRenderer::appendTerseMetaItems(const char* elementName)
{
    forEachRenderableMeta([&](EventMetaAttr metaAttr, const MetaRule& rule)
    {
        for (EventType t : ctx.metaEvents[metaAttr])
        {
            if (ctx.eventMatches(t))
            {
                output.addPropTreeArrayItem(elementName, createPTree())->setProp(nullptr, rule.name);
                break;
            }
        }
    });
}

void DescribeRenderer::appendContexts()
{
    for (size_t contextIndex = 0; contextIndex < numEventContexts; ++contextIndex)
    {
        EventContext context = eventContexts[contextIndex];
        const char* contextName = queryLocalEventContextName(context);
        if (isEmptyString(contextName))
            continue;
        if (!ctx.contextMatches(context))
            continue;

        for (EventType t : ctx.contextEvents[contextIndex])
        {
            if (ctx.eventMatches(t))
            {
                output.addPropTreeArrayItem("context", createPTree())->setProp(nullptr, contextName);
                break;
            }
        }
    }
}

void DescribeRenderer::appendEvents()
{
    for (EventType t : ctx.events)
    {
        if (!ctx.eventMatches(t))
            continue;

        output.addPropTreeArrayItem("event", createPTree())->setProp(nullptr, queryEventName(t));
    }
}

void DescribeRenderer::appendAttributes()
{
    for (EventAttr attr : eventAttrs)
    {
        if (!ctx.attrMatches(attr))
            continue;

        for (EventType t : ctx.attrEvents[attr])
        {
            if (ctx.eventMatches(t))
            {
                output.addPropTreeArrayItem("attribute", createPTree())->setProp(nullptr, queryEventAttributeName(attr));
                break;
            }
        }
    }

    appendTerseMetaItems("attribute");
}
}

void CDescribeEventsOp::appendDescriptionTree(IPropertyTree& description)
{
    // Build cross-reference index once, then project into enabled sections.
    DescribeContext ctx;
    DescribeRenderer renderer(description, ctx);

    if (isSectionEnabled(DescribeSection::contexts))
        renderer.appendContexts();
    if (isSectionEnabled(DescribeSection::events))
        renderer.appendEvents();
    if (isSectionEnabled(DescribeSection::attributes))
        renderer.appendAttributes();
}

bool CDescribeEventsOp::isSectionEnabled(DescribeSection section) const
{
    if (sectionOverrides == DescribeSection::none)
        return true;
    return hasMask(sectionOverrides, section);
}

#undef ForEachContextBitIn

#ifdef _USE_CPPUNIT

#include "unittests.hpp"

class EventDescribeTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventDescribeTests);
        CPPUNIT_TEST(testUnsupportedFormatThrows);
        CPPUNIT_TEST(testNamesAttributesOmitMetadata);
        CPPUNIT_TEST(testSemanticMetaEligibilityRules);
        CPPUNIT_TEST(testSectionSelectionEachSingleSectionOnly);
        CPPUNIT_TEST(testSectionSelectionCombinedSectionsOnly);
        CPPUNIT_TEST(testNamesAllSectionsEmitCompleteExpectedNames);
    CPPUNIT_TEST_SUITE_END();

    static const IPropertyTree& queryDescribeRoot(const IPropertyTree& tree)
    {
        if (const IPropertyTree* describe = tree.queryPropTree("describe"))
            return *describe;
        return tree;
    }

    static void collectScalarList(std::set<std::string>& values, const IPropertyTree& tree, const char* sectionName)
    {
        Owned<IPropertyTreeIterator> it = tree.getElements(sectionName);
        ForEach(*it)
        {
            const char* value = it->query().queryProp(nullptr);
            if (!isEmptyString(value))
                values.emplace(value);
        }
    }

    static void setupOutput(CDescribeEventsOp& op, StringBuffer& output, Owned<IBufferedSerialOutputStream>& stream)
    {
        stream.setown(createBufferedSerialOutputStream(output));
        op.setOutput(*stream);
        CPPUNIT_ASSERT(op.ready());
    }

public:
    void testUnsupportedFormatThrows()
    {
        START_TEST
        CDescribeEventsOp op;
        op.setFormat(static_cast<DescribeOutputFormat>(255));

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT_THROWS_IEXCEPTION(op.doOp(), "unsupported output format value");
        END_TEST
    }

    void testNamesAttributesOmitMetadata()
    {
        START_TEST
        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.addSectionOverride(DescribeSection::attributes);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();
        const char* result = output.str();
        CPPUNIT_ASSERT(strstr(result, "\"attribute\"") != nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"@category\"") == nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"@presence\"") == nullptr);
        END_TEST
    }

    void testSemanticMetaEligibilityRules()
    {
        START_TEST

        // ServiceName is intentionally excluded for non-derivable metadata/source events.
        CPPUNIT_ASSERT(!isSemanticallyEligibleForMeta(EventRecordingSource, MetaAttrServiceName));
        CPPUNIT_ASSERT(!isSemanticallyEligibleForMeta(MetaFileInformation, MetaAttrServiceName));
        CPPUNIT_ASSERT(!isSemanticallyEligibleForMeta(MetaPlaneInformation, MetaAttrServiceName));

        // ServiceName remains semantically eligible for normal operational events.
        CPPUNIT_ASSERT(isSemanticallyEligibleForMeta(EventQueryStart, MetaAttrServiceName));

        // LogicalFileName and Plane are not independently derivable from MetaPlaneInformation.
        CPPUNIT_ASSERT(!isSemanticallyEligibleForMeta(MetaPlaneInformation, MetaAttrLogicalFileName));
        CPPUNIT_ASSERT(!isSemanticallyEligibleForMeta(MetaPlaneInformation, MetaAttrPlane));

        // Other event types retain default semantic eligibility for these derived fields.
        CPPUNIT_ASSERT(isSemanticallyEligibleForMeta(EventIndexLoad, MetaAttrLogicalFileName));
        CPPUNIT_ASSERT(isSemanticallyEligibleForMeta(EventIndexLoad, MetaAttrPlane));

        END_TEST
    }

    void testSectionSelectionEachSingleSectionOnly()
    {
        START_TEST
        const DescribeSection cases[] = {
            DescribeSection::contexts,
            DescribeSection::events,
            DescribeSection::attributes,
        };

        for (DescribeSection section : cases)
        {
            CDescribeEventsOp op;
            op.setFormat(DescribeOutputFormat::json);
            op.addSectionOverride(section);

            StringBuffer output;
            Owned<IBufferedSerialOutputStream> stream;
            setupOutput(op, output, stream);

            CPPUNIT_ASSERT(op.doOp());
            stream->flush();

            Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
            const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

            CPPUNIT_ASSERT(describeTree.hasProp(section == DescribeSection::contexts ? "context" : section == DescribeSection::events ? "event" : "attribute"));
            CPPUNIT_ASSERT(!describeTree.hasProp(section == DescribeSection::contexts ? "event" : "context"));
            CPPUNIT_ASSERT(!describeTree.hasProp(section == DescribeSection::attributes ? "event" : "attribute"));
            CPPUNIT_ASSERT(!describeTree.hasProp("metaAttribute"));
        }
        END_TEST
    }

    void testSectionSelectionCombinedSectionsOnly()
    {
        START_TEST
        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.addSectionOverride(DescribeSection::contexts);
        op.addSectionOverride(DescribeSection::attributes);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();
        const char* result = output.str();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(result);
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        CPPUNIT_ASSERT(describeTree.hasProp("context"));
        CPPUNIT_ASSERT(describeTree.hasProp("attribute"));
        CPPUNIT_ASSERT(!describeTree.hasProp("event"));
        CPPUNIT_ASSERT(!describeTree.hasProp("metaAttribute"));
        END_TEST
    }

    void testNamesAllSectionsEmitCompleteExpectedNames()
    {
        START_TEST
        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        // No explicit section selection means all sections are enabled.

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();
        const char* result = output.str();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(result);
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        std::set<std::string> contexts;
        std::set<std::string> events;
        std::set<std::string> attributes;

        collectScalarList(contexts, describeTree, "context");
        collectScalarList(events, describeTree, "event");
        collectScalarList(attributes, describeTree, "attribute");

        CPPUNIT_ASSERT_MESSAGE("attribute section should exist when no section filter is applied", describeTree.hasProp("attribute"));

        // Every context reachable from at least one observable event must appear.
        // Declared contexts with no observable event references are unobservable in this mode.
        std::set<std::string> expectedContexts;
        for (EventType type : allEventTypes())
        {
            if (!isObservableEvent(type))
                continue;

            EventContext contextMask = queryEventContext(type);
            for (EventContext contextBit : eventContexts)
            {
                if (!hasMask(contextMask, contextBit))
                    continue;
                const char* name = queryLocalEventContextName(contextBit);
                CPPUNIT_ASSERT_MESSAGE("missing context name", !isEmptyString(name));
                expectedContexts.emplace(name);
            }
        }
        for (const auto& expectedName : expectedContexts)
            CPPUNIT_ASSERT_MESSAGE("expected context not emitted", contexts.count(expectedName) != 0);

        // Every observable event with a name must appear.
        for (EventType type : allEventTypes())
        {
            if (!isObservableEvent(type))
                continue;
            const char* name = queryEventName(type);
            CPPUNIT_ASSERT_MESSAGE("missing event name", !isEmptyString(name));
            CPPUNIT_ASSERT_MESSAGE("expected event not emitted", events.count(name) != 0);
        }
        CPPUNIT_ASSERT_MESSAGE("unobservable event should not be emitted", events.count(queryEventName(EventRecordingSource)) == 0);

        // Every native attribute reachable from at least one observable event must appear.
        // Declared attributes with no observable event references are unobservable in this mode.
        std::set<std::string> expectedNativeAttributes;
        for (EventType type : allEventTypes())
        {
            if (!isObservableEvent(type))
                continue;

            for (EventAttr attr : queryEventAttributeIds(type))
            {
                const char* name = queryEventAttributeName(attr);
                CPPUNIT_ASSERT_MESSAGE("missing attribute name", !isEmptyString(name));
                expectedNativeAttributes.emplace(name);
            }
        }
        for (const auto& expectedName : expectedNativeAttributes)
            CPPUNIT_ASSERT_MESSAGE("expected attribute not emitted", attributes.count(expectedName) != 0);

        CPPUNIT_ASSERT_MESSAGE("sentinel attribute should not be emitted", attributes.count(queryEventAttributeName(EvAttrNone)) == 0);

        // Derived names should be present in the attribute section.
        for (EventMetaAttr metaAttr : eventMetaAttrs)
        {
            const char* name = queryMetaAttributeName(metaAttr);
            CPPUNIT_ASSERT_MESSAGE("missing meta attribute name", !isEmptyString(name));
            CPPUNIT_ASSERT_MESSAGE("expected meta attribute not emitted", attributes.count(name) != 0);
        }

        CPPUNIT_ASSERT(strstr(result, "\"context\"") != nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"event\"") != nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"attribute\"") != nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"metaAttribute\"") == nullptr);
        CPPUNIT_ASSERT(strstr(result, "\"@name\"") == nullptr);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventDescribeTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventDescribeTests, "eventdescribe");

#endif // _USE_CPPUNIT
