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
#include "jfile.hpp"
#include "jutil.hpp"
#include "jptree.hpp"
#include <algorithm>
#include <array>
#include <bitset>
#include <cstddef>
#include <initializer_list>
#include <mutex>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#ifdef _USE_CPPUNIT
class EventDescribeTests;
#endif

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

// Compile-time cap for total dependency entries per meta rule
// (generic + explicit entries). This keeps rules maintainable and bounded,
// while leaving headroom above current definitions.
static constexpr size_t maxMetaRuleDependencyEntries = 10;
static_assert(maxMetaRuleDependencyEntries < (sizeof(size_t) * 8),
    "maxMetaRuleDependencyEntries must fit combination bitmask width");

struct MetaRule
{
    const char* name;
    struct Dependency
    {
        // All listed dependencies are conjunctive (logical AND).
        // Generic dependency: event==EventNone, attr must be present on the
        // candidate event type.
        // Explicit dependency: event!=EventNone, observed-mode eligibility
        // depends on event presence. attr is retained as informative context
        // for the rule definition.
        EventType event;
        EventAttr attr;
    };

    struct DependencyRange
    {
        const Dependency* first;
        const Dependency* last;

        constexpr const Dependency* begin() const { return first; }
        constexpr const Dependency* end() const { return last; }
        constexpr size_t size() const { return static_cast<size_t>(last - first); }
    } dependencies;

    constexpr MetaRule(const char* _name, DependencyRange _dependencies)
        : name(_name), dependencies(_dependencies)
    {
    }

    MetaRule() = delete;
};

// Capturing dependencies by reference-to-array preserves the exact element
// count at compile time and enforces rule-size constraints centrally. Rule
// definitions must come from arrays with static storage duration because
// MetaRule stores a non-owning view of dependency memory.
template <size_t N>
constexpr MetaRule makeMetaRule(const char* name, const MetaRule::Dependency (&dependencies)[N])
{
    static_assert(N > 0 && N <= maxMetaRuleDependencyEntries,
        "meta rule dependency count out of allowed range");
    return { name, { dependencies, dependencies + N } };
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

inline EventMetaAttr queryMetaAttribute(const char* name)
{
    if (!isEmptyString(name))
    {
        for (unsigned m = 0; m < MetaAttrMax; ++m)
        {
            if (strieq(metaAttrNames[m], name))
                return EventMetaAttr(m);
        }
    }
    return MetaAttrMax;
}

static inline const MetaRule* getMetaRule(EventMetaAttr attr)
{
    if (attr >= MetaAttrMax)
        return nullptr;
    return &metaRules[attr];
}

class CEventCountVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    CEventCountVisitor(std::array<__uint64, EventMax>& _counts)
        : counts(_counts)
    {
    }

    virtual bool visitFile(const char* /*filename*/, uint32_t /*version*/) override
    {
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        EventType type = event.queryType();
        assertex(type < counts.size());
        counts[type]++;
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
    }

private:
    std::array<__uint64, EventMax>& counts;
};

enum class NamedSelectionKind : byte
{
    none = 0,
    context = 0x01,
    event = 0x02,
    attribute = 0x04,
    meta = 0x08,
};
BITMASK_ENUM(NamedSelectionKind);

struct NamedSelection
{
    NamedSelectionKind kinds{NamedSelectionKind::none};
    EventContext context{EventCtxInvalid};
    EventType event{EventNone};
    EventAttr attribute{EvAttrNone};
    EventMetaAttr meta{MetaAttrMax};

    bool empty() const
    {
        return kinds == NamedSelectionKind::none;
    }

    bool has(NamedSelectionKind kind) const
    {
        return hasMask(kinds, kind);
    }
};

struct DescribeNamedRequest
{
    const char* rawText{nullptr};
    NamedSelection selection;
    bool enabled{false};
};

struct DescribeRenderRequest
{
    DescribeSection sectionOverrides{DescribeSection::none};
    const std::array<__uint64, EventMax>& eventCounts;
    bool filterByObserved{false};
    const EventFileProperties* props{nullptr};
    DescribeNamedRequest named;

    DescribeRenderRequest(DescribeSection _sectionOverrides, const std::array<__uint64, EventMax>& _eventCounts, bool _filterByObserved, const EventFileProperties* _props, const DescribeNamedRequest& _named)
        : sectionOverrides(_sectionOverrides)
        , eventCounts(_eventCounts)
        , filterByObserved(_filterByObserved)
        , props(_props)
        , named(_named)
    {
    }
};

// Resolves a user-supplied name to all matching entity kinds. Matching is case-insensitive.
// A name may match at most one kind (no name currently spans multiple namespaces), but all
// matching kinds are populated for correctness if collisions are introduced later.
// Returns an empty NamedSelection if the name does not match any known entity.
static NamedSelection resolveNamedSelectionFrom(const char* text)
{
    NamedSelection selection;
    if (isEmptyString(text))
        return selection;
    if (auto context = queryEventContext(text); context != EventCtxInvalid)
    {
        selection.context = context;
        selection.kinds |= NamedSelectionKind::context;
    }
    if (auto event = queryEventType(text); event != EventNone)
    {
        selection.event = event;
        selection.kinds |= NamedSelectionKind::event;
    }
    if (auto attribute = queryEventAttribute(text); attribute != EvAttrNone)
    {
        selection.attribute = attribute;
        selection.kinds |= NamedSelectionKind::attribute;
    }
    if (auto meta = queryMetaAttribute(text); meta != MetaAttrMax)
    {
        selection.meta = meta;
        selection.kinds |= NamedSelectionKind::meta;
    }
    return selection;
}

static void appendDescriptionTree(IPropertyTree& description, const DescribeRenderRequest& request);
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

void CDescribeEventsOp::setNamedEntity(const char* _namedEntity)
{
    namedEntity.set(_namedEntity);
}

bool CDescribeEventsOp::ready() const
{
    // Unlike most consuming operations, describe supports schema-only output.
    return out.get() != nullptr;
}

bool CDescribeEventsOp::doOp()
{
    std::array<__uint64, EventMax> eventCounts{};
    bool filterByObserved = false;
    EventFileProperties fileProperties;
    const EventFileProperties* props = nullptr;

    DescribeNamedRequest namedRequest;
    if (!isEmptyString(namedEntity))
    {
        namedRequest.selection = resolveNamedSelectionFrom(namedEntity);
        if (namedRequest.selection.empty())
            throwStringExceptionV(-1, "unknown named entity '%s'", namedEntity.get());

        namedRequest.rawText = namedEntity;
        namedRequest.enabled = true;
    }

    if (!inputPaths.empty())
    {
        fileProperties = queryIteratorProperties();
        props = &fileProperties;
        CEventCountVisitor countVisitor(eventCounts);
        traverseEvents(countVisitor);
        filterByObserved = true;
    }

    Owned<IPropertyTree> description = createPTree("describe");
    DescribeRenderRequest renderRequest(sectionOverrides, eventCounts, filterByObserved, props, namedRequest);
    appendDescriptionTree(*description, renderRequest);

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

// Aggregated cross-reference index built once from the observable event set.
// Render functions query this instead of scanning descriptions.
class DescribeContext
{
public:
    std::vector<EventType>                             events;
    std::array<__uint64, EventMax>                     counts{};
    bool                                               filterByObserved{false};
    std::array<std::vector<EventType>, numEventContexts> contextEvents;
    std::array<std::vector<EventType>, EvAttrMax>      attrEvents;
    std::array<std::vector<EventType>, numEventMetaAttrs> metaEvents;
    NamedSelection                                     namedSelection;
    EventFileOption                                    includeTraceIds{EventFileOption::Ambiguous};
    EventFileOption                                    includeThreadIds{EventFileOption::Ambiguous};
    EventFileOption                                    includeStackTraces{EventFileOption::Ambiguous};

public:
    DescribeContext(const std::array<__uint64, EventMax>& _counts, bool _filterByObserved, const NamedSelection& _namedSelection, const EventFileProperties* props);

    bool anyEventPassesObservedFilter(const std::vector<EventType>& events) const;
    bool eventPassesObservedFilter(EventType type) const;
    bool eventMatches(EventType t) const;
    bool attrMatches(EventAttr a) const;
    bool contextMatches(EventContext c) const;
    bool metaMatches(EventMetaAttr idx) const;

private:
#ifdef _USE_CPPUNIT
    friend class ::EventDescribeTests;
#endif

    void buildObservableEventIndex();
    void indexObservableEvent(EventType type);
    void indexEventAttributes(EventType type);
    void indexEventMeta(EventType type);
    bool hasEligibleAttribute(EventType type, EventAttr attr) const;
    bool isEventEligibleForMeta(EventType type, const MetaRule& rule) const;
};

DescribeContext::DescribeContext(const std::array<__uint64, EventMax>& _counts, bool _filterByObserved, const NamedSelection& _namedSelection, const EventFileProperties* props)
    : counts(_counts)
    , filterByObserved(_filterByObserved)
    , namedSelection(_namedSelection)
{
    if (props)
    {
        includeTraceIds = props->options.includeTraceIds;
        includeThreadIds = props->options.includeThreadIds;
        includeStackTraces = props->options.includeStackTraces;
    }
    buildObservableEventIndex();
}

struct EventMembershipCache
{
    // Dense representation is intentional: EventType and EventAttr values are
    // bounded and non-sparse for this use case. Sparsity only emerges after
    // observed filtering, but membership must be known up front for lookups.
    std::bitset<EventMax> validEventTypes;
    std::array<std::bitset<EvAttrMax>, EventMax> attrMembership;
};

static std::once_flag eventMembershipCacheInitFlag;
static EventMembershipCache eventMembershipCache;

static EventMembershipCache buildEventMembershipCache()
{
    EventMembershipCache cache{};
    for (EventType type : allEventTypes())
    {
        const size_t typeIndex = static_cast<size_t>(type);
        if (typeIndex >= cache.attrMembership.size())
            continue;

        cache.validEventTypes.set(typeIndex);
        auto& membership = cache.attrMembership[typeIndex];
        for (EventAttr attr : queryEventAttributeIds(type))
        {
            const size_t attrIndex = static_cast<size_t>(attr);
            if (attrIndex >= EvAttrMax)
                continue;
            membership.set(attrIndex);
        }
    }
    return cache;
}

static void ensureEventMembershipCacheInitialized()
{
    std::call_once(eventMembershipCacheInitFlag, []()
    {
        eventMembershipCache = buildEventMembershipCache();
    });
}

static bool hasSchemaAttribute(EventType type, EventAttr attr)
{
    ensureEventMembershipCacheInitialized();

    const size_t typeIndex = static_cast<size_t>(type);
    if (typeIndex >= eventMembershipCache.attrMembership.size())
        return false;
    if (!eventMembershipCache.validEventTypes.test(typeIndex))
        return false;

    const size_t attrIndex = static_cast<size_t>(attr);
    if (attrIndex >= EvAttrMax)
        return false;

    return eventMembershipCache.attrMembership[typeIndex].test(attrIndex);
}

bool DescribeContext::hasEligibleAttribute(EventType type, EventAttr attr) const
{
    if (!hasSchemaAttribute(type, attr))
        return false;

    if (!filterByObserved)
        return true;

    switch (attr)
    {
    case EvAttrEventTraceId:
        return includeTraceIds != EventFileOption::Disabled;
    case EvAttrEventThreadId:
        return includeThreadIds != EventFileOption::Disabled;
    case EvAttrEventStackTrace:
        return includeStackTraces != EventFileOption::Disabled;
    default:
        return true;
    }
}

bool DescribeContext::isEventEligibleForMeta(EventType type, const MetaRule& rule) const
{
    // Conjunctive dependency semantics: every generic and explicit dependency
    // listed in the rule must be satisfied.
    bool sawGenericDependency = false;
    bool sawExplicitDependency = false;
    for (const MetaRule::Dependency& dep : rule.dependencies)
    {
        if (EventNone == dep.event)
        {
            if (!hasEligibleAttribute(type, dep.attr))
                return false;
            sawGenericDependency = true;
        }
        else
        {
            sawExplicitDependency = true;
            if (filterByObserved && counts[dep.event] == 0)
                return false;
        }
    }

    return sawGenericDependency && sawExplicitDependency;
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
    const bool applyOptionGating = filterByObserved;
    for (EventAttr attr : queryEventAttributeIds(type))
    {
        if (applyOptionGating)
        {
            switch (attr)
            {
            case EvAttrEventTraceId:
                if (includeTraceIds == EventFileOption::Disabled)
                    continue;
                break;
            case EvAttrEventThreadId:
                if (includeThreadIds == EventFileOption::Disabled)
                    continue;
                break;
            case EvAttrEventStackTrace:
                if (includeStackTraces == EventFileOption::Disabled)
                    continue;
                break;
            default:
                break;
            }
        }
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

bool DescribeContext::eventPassesObservedFilter(EventType type) const
{
    if (!filterByObserved)
        return true;

    const size_t typeIndex = static_cast<size_t>(type);
    if (typeIndex >= counts.size())
        return false;

    return counts[typeIndex] > 0;
}

bool DescribeContext::anyEventPassesObservedFilter(const std::vector<EventType>& events) const
{
    for (EventType type : events)
    {
        if (eventPassesObservedFilter(type))
            return true;
    }

    return false;
}

bool DescribeContext::eventMatches(EventType t) const
{
    if (!namedSelection.empty())
    {
        if (!namedSelection.has(NamedSelectionKind::event))
            return false;
        if (t != namedSelection.event)
            return false;
    }

    return eventPassesObservedFilter(t);
}

bool DescribeContext::attrMatches(EventAttr a) const
{
    if (!namedSelection.empty())
    {
        if (!namedSelection.has(NamedSelectionKind::attribute))
            return false;
        if (a != namedSelection.attribute)
            return false;
    }

    if (!filterByObserved)
        return true;

    const size_t attrIndex = static_cast<size_t>(a);
    if (attrIndex >= attrEvents.size())
        return false;

    return anyEventPassesObservedFilter(attrEvents[attrIndex]);
}

bool DescribeContext::contextMatches(EventContext c) const
{
    if (!namedSelection.empty())
    {
        if (!namedSelection.has(NamedSelectionKind::context))
            return false;
        if (c != namedSelection.context)
            return false;
    }

    if (!filterByObserved)
        return true;

    for (size_t contextIndex = 0; contextIndex < numEventContexts; ++contextIndex)
    {
        if (eventContexts[contextIndex] != c)
            continue;

        return anyEventPassesObservedFilter(contextEvents[contextIndex]);
    }

    return false;
}

bool DescribeContext::metaMatches(EventMetaAttr idx) const
{
    if (!namedSelection.empty())
    {
        if (!namedSelection.has(NamedSelectionKind::meta))
            return false;
        if (idx != namedSelection.meta)
            return false;
    }

    if (!filterByObserved)
        return true;

    const size_t metaIndex = static_cast<size_t>(idx);
    if (metaIndex >= metaEvents.size())
        return false;

    return anyEventPassesObservedFilter(metaEvents[metaIndex]);
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
        output.addPropTreeArrayItem(elementName, createPTree())->setProp(nullptr, rule.name);
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

        if (ctx.namedSelection.has(NamedSelectionKind::context))
        {
            if (ctx.anyEventPassesObservedFilter(ctx.contextEvents[contextIndex]))
                output.addPropTreeArrayItem("context", createPTree())->setProp(nullptr, contextName);
            continue;
        }

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

        if (ctx.namedSelection.has(NamedSelectionKind::attribute))
        {
            if (ctx.anyEventPassesObservedFilter(ctx.attrEvents[attr]))
                output.addPropTreeArrayItem("attribute", createPTree())->setProp(nullptr, queryEventAttributeName(attr));
            continue;
        }

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

namespace
{
static bool isSectionEnabled(DescribeSection sectionOverrides, DescribeSection section)
{
    if (sectionOverrides == DescribeSection::none)
        return true;
    return hasMask(sectionOverrides, section);
}

static void appendDescriptionTree(IPropertyTree& description, const DescribeRenderRequest& request)
{
    // Build cross-reference index using the pre-resolved named filter, then project into enabled sections.
    DescribeContext ctx(request.eventCounts, request.filterByObserved, request.named.selection, request.props);

    bool contextsEnabled = isSectionEnabled(request.sectionOverrides, DescribeSection::contexts);
    bool eventsEnabled = isSectionEnabled(request.sectionOverrides, DescribeSection::events);
    bool attributesEnabled = isSectionEnabled(request.sectionOverrides, DescribeSection::attributes);

    if (request.named.enabled && !ctx.namedSelection.empty())
    {
        const bool canRenderInEnabledSection =
            (contextsEnabled && ctx.namedSelection.has(NamedSelectionKind::context))
            || (eventsEnabled && ctx.namedSelection.has(NamedSelectionKind::event))
            || (attributesEnabled && (ctx.namedSelection.has(NamedSelectionKind::attribute) || ctx.namedSelection.has(NamedSelectionKind::meta)));

        if (!canRenderInEnabledSection)
        {
            const char* excludedType = nullptr;
            const char* neededSection = nullptr;
            const char* sectionFlag = nullptr;
            if (ctx.namedSelection.has(NamedSelectionKind::context))
            {
                excludedType = "context";
                neededSection = "contexts";
                sectionFlag = "-c";
            }
            else if (ctx.namedSelection.has(NamedSelectionKind::event))
            {
                excludedType = "event";
                neededSection = "events";
                sectionFlag = "-e";
            }
            else if (ctx.namedSelection.has(NamedSelectionKind::attribute) || ctx.namedSelection.has(NamedSelectionKind::meta))
            {
                excludedType = "attribute";
                neededSection = "attributes";
                sectionFlag = "-a";
            }

            if (excludedType)
                throwStringExceptionV(-1, "named entity '%s' has type '%s'; enable the %s section (%s) to show it",
                    request.named.rawText, excludedType, neededSection, sectionFlag);
        }
    }

    DescribeRenderer renderer(description, ctx);

    if (contextsEnabled)
        renderer.appendContexts();
    if (eventsEnabled)
        renderer.appendEvents();
    if (attributesEnabled)
        renderer.appendAttributes();
}
} // namespace

#undef ForEachContextBitIn

#ifdef _USE_CPPUNIT

#include "unittests.hpp"

class EventDescribeTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventDescribeTests);
        CPPUNIT_TEST(testUnsupportedFormatThrows);
        CPPUNIT_TEST(testNamesAttributesOmitMetadata);
        CPPUNIT_TEST(testMetaRulesHaveRequiredDependencies);
        CPPUNIT_TEST(testNamedEntitySelection);
        CPPUNIT_TEST(testNamedEntitySelectionCaseInsensitive);
        CPPUNIT_TEST(testNamedMetaSelectionRendersInAttributeList);
        CPPUNIT_TEST(testNamedContextSelection);
        CPPUNIT_TEST(testNamedAttributeSelection);
        CPPUNIT_TEST(testNamedEntityUnknownThrows);
        CPPUNIT_TEST(testNamedEntityUnknownThrowsBeforeTraversal);
        CPPUNIT_TEST(testNamedEntityExcludedBySectionThrows);
        CPPUNIT_TEST(testNamedContextExcludedBySectionThrows);
        CPPUNIT_TEST(testNamedAttributeExcludedBySectionThrows);
        CPPUNIT_TEST(testNamedMetaExcludedBySectionThrows);
        CPPUNIT_TEST(testSemanticMetaEligibilityRules);
        CPPUNIT_TEST(testObservedMetaEligibilityDependencyCombinations);
        CPPUNIT_TEST(testObservedMetaEligibilityOptionGating);
        CPPUNIT_TEST(testObservedNamesWithoutTraceOrThreadIds);
        CPPUNIT_TEST(testObservedNamesWithTraceAndThreadIds);
        CPPUNIT_TEST(testObservedNamesFromMultipleFiles);
        CPPUNIT_TEST(testSectionSelectionEachSingleSectionOnly);
        CPPUNIT_TEST(testSectionSelectionCombinedSectionsOnly);
        CPPUNIT_TEST(testNamesAllSectionsEmitCompleteExpectedNames);
        CPPUNIT_TEST(testNamedEntityNamesResolveToKnownKinds);
        CPPUNIT_TEST(testNamedMetaObservedInteraction);
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

    static void assertExactSectionValues(const IPropertyTree& tree, const char* sectionName, std::initializer_list<const char*> expected)
    {
        std::vector<std::string> actual;
        Owned<IPropertyTreeIterator> it = tree.getElements(sectionName);
        ForEach(*it)
        {
            const char* value = it->query().queryProp(nullptr);
            if (!isEmptyString(value))
                actual.emplace_back(value);
        }
        std::sort(actual.begin(), actual.end());

        std::vector<std::string> expectedValues;
        expectedValues.reserve(expected.size());
        for (const char* value : expected)
            expectedValues.emplace_back(value);
        std::sort(expectedValues.begin(), expectedValues.end());

        if (actual != expectedValues)
        {
            StringBuffer msg;
            msg.appendf("section '%s' mismatch", sectionName);
            CPPUNIT_FAIL(msg.str());
        }
    }

    static void setupOutput(CDescribeEventsOp& op, StringBuffer& output, Owned<IBufferedSerialOutputStream>& stream)
    {
        stream.setown(createBufferedSerialOutputStream(output));
        op.setOutput(*stream);
        CPPUNIT_ASSERT(op.ready());
    }

    static void removeFile(const char* filename)
    {
        Owned<IFile> file = createIFile(filename);
        if (!file->exists())
            return;

        try
        {
            file->remove();
        }
        catch (IException* e)
        {
            // Best-effort test cleanup: ignore remove failures.
            e->Release();
        }
    }

    static void writeObservedRecording(const char* filename, const char* options)
    {
        removeFile(filename);

        EventRecorder& recorder = queryRecorder();
        EventRecordingSummary summary;

        CPPUNIT_ASSERT(recorder.startRecording(options, filename, "describe-test", 1, 2, 3, false));
        CPPUNIT_ASSERT(recorder.isRecording());
        recorder.recordQueryStart("describe-test");
        recorder.recordIndexLoad(100, 200, static_cast<byte>(1), 4096, 500, 300);
        recorder.recordQueryStop();
        CPPUNIT_ASSERT(recorder.stopRecording(&summary, false));
        CPPUNIT_ASSERT_EQUAL_STR(filename, summary.filename.str());
        CPPUNIT_ASSERT(summary.numEvents != 0);
    }

    static std::string describeToJson(std::initializer_list<const char*> inputFiles)
    {
        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);

        for (const char* inputFile : inputFiles)
            op.setInputPath(inputFile);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();
        return output.str();
    }

    static void assertThrowsMessageContains(const std::function<void()>& action, std::initializer_list<const char*> fragments)
    {
        try
        {
            action();
            CPPUNIT_FAIL("expected IException");
        }
        catch (IException* e)
        {
            StringBuffer message;
            e->errorMessage(message);
            e->Release();

            for (const char* fragment : fragments)
            {
                StringBuffer failure;
                failure.appendf("expected exception containing '%s', actual='%s'", fragment, message.str());
                CPPUNIT_ASSERT_MESSAGE(failure.str(), strstr(message.str(), fragment) != nullptr);
            }
        }
    }

public:
    void testMetaRulesHaveRequiredDependencies()
    {
        START_TEST

        for (EventMetaAttr metaAttr : eventMetaAttrs)
        {
            const MetaRule* rule = getMetaRule(metaAttr);
            CPPUNIT_ASSERT(rule != nullptr);
            if (rule->dependencies.size() == 0)
            {
                StringBuffer msg;
                msg.appendf("meta rule has no dependencies: %s", queryMetaAttributeName(metaAttr));
                CPPUNIT_FAIL(msg.str());
            }

            bool hasGenericDependency = false;
            bool hasExplicitDependency = false;
            for (const MetaRule::Dependency& dep : rule->dependencies)
            {
                if (EventNone == dep.event)
                    hasGenericDependency = true;
                else
                    hasExplicitDependency = true;

                if (hasGenericDependency && hasExplicitDependency)
                    break;
            }

            if (hasGenericDependency && hasExplicitDependency)
                continue; // dependencies are well-formed
            if (!hasGenericDependency)
            {
                StringBuffer msg;
                msg.appendf("meta rule missing required EventNone dependency: %s", queryMetaAttributeName(metaAttr));
                CPPUNIT_FAIL(msg.str());
            }
            else if (!hasExplicitDependency)
            {
                StringBuffer msg;
                msg.appendf("meta rule missing required explicit dependency: %s", queryMetaAttributeName(metaAttr));
                CPPUNIT_FAIL(msg.str());
            }
            // both flags cannot be false
        }

        END_TEST
    }

    void testUnsupportedFormatThrows()
    {
        START_TEST
        CDescribeEventsOp op;
        op.setFormat(static_cast<DescribeOutputFormat>(255));

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "unsupported output format value" });
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

    void testNamedEntitySelection()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("QueryStart");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        assertExactSectionValues(describeTree, "context", {});
        assertExactSectionValues(describeTree, "event", { "QueryStart" });
        assertExactSectionValues(describeTree, "attribute", {});
        END_TEST
    }

    void testNamedEntitySelectionCaseInsensitive()
    {
        START_TEST

        {
            CDescribeEventsOp op;
            op.setFormat(DescribeOutputFormat::json);
            op.setNamedEntity("querystart");

            StringBuffer output;
            Owned<IBufferedSerialOutputStream> stream;
            setupOutput(op, output, stream);

            CPPUNIT_ASSERT(op.doOp());
            stream->flush();

            Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
            const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

            assertExactSectionValues(describeTree, "context", {});
            assertExactSectionValues(describeTree, "event", { "QueryStart" });
            assertExactSectionValues(describeTree, "attribute", {});
        }

        {
            CDescribeEventsOp op;
            op.setFormat(DescribeOutputFormat::json);
            op.setNamedEntity("META.pAtH");

            StringBuffer output;
            Owned<IBufferedSerialOutputStream> stream;
            setupOutput(op, output, stream);

            CPPUNIT_ASSERT(op.doOp());
            stream->flush();

            Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
            const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

            assertExactSectionValues(describeTree, "context", {});
            assertExactSectionValues(describeTree, "event", {});
            assertExactSectionValues(describeTree, "attribute", { "meta.Path" });
        }

        END_TEST
    }

    void testNamedMetaSelectionRendersInAttributeList()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("meta.Path");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        assertExactSectionValues(describeTree, "context", {});
        assertExactSectionValues(describeTree, "event", {});
        assertExactSectionValues(describeTree, "attribute", { "meta.Path" });
        END_TEST
    }

    void testNamedContextSelection()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("Index");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        assertExactSectionValues(describeTree, "context", { "Index" });
        assertExactSectionValues(describeTree, "event", {});
        assertExactSectionValues(describeTree, "attribute", {});
        END_TEST
    }

    void testNamedAttributeSelection()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("ServiceName");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        assertExactSectionValues(describeTree, "context", {});
        assertExactSectionValues(describeTree, "event", {});
        assertExactSectionValues(describeTree, "attribute", { "ServiceName" });
        END_TEST
    }

    void testNamedEntityUnknownThrows()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("NotARealEntity");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "unknown named entity", "NotARealEntity" });
        END_TEST
    }

    void testNamedEntityUnknownThrowsBeforeTraversal()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("Enqueued");
        op.setInputPath("definitely_missing_describe_input.evt");

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "unknown named entity", "Enqueued" });
        END_TEST
    }

    void testNamedEntityExcludedBySectionThrows()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("QueryStart");
        op.addSectionOverride(DescribeSection::attributes);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "QueryStart", "event", "-e" });
        END_TEST
    }

    void testNamedContextExcludedBySectionThrows()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("Query");
        op.addSectionOverride(DescribeSection::events);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "Query", "context", "-c" });
        END_TEST
    }

    void testNamedAttributeExcludedBySectionThrows()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("ServiceName");
        op.addSectionOverride(DescribeSection::events);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "ServiceName", "attribute", "-a" });
        END_TEST
    }

    void testNamedMetaExcludedBySectionThrows()
    {
        START_TEST

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("meta.ServiceName");
        op.addSectionOverride(DescribeSection::events);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        assertThrowsMessageContains([&]() { op.doOp(); }, { "meta.ServiceName", "attribute", "-a" });
        END_TEST
    }

    void testNamedEntityNamesResolveToKnownKinds()
    {
        START_TEST

        // Verify that every known entity name resolves to at least one kind.
        // Name collisions across namespaces are allowed; resolution remains valid
        // as long as the result is non-empty.
        auto hasAnyKind = [](NamedSelectionKind kinds) -> bool {
            return kinds != NamedSelectionKind::none;
        };

        auto assertResolvable = [&](const char* name) {
            NamedSelection sel = resolveNamedSelectionFrom(name);
            if (!hasAnyKind(sel.kinds))
            {
                StringBuffer msg;
                msg.appendf("entity name did not resolve to any known kind: %s", name);
                CPPUNIT_FAIL(msg.str());
            }
        };

        for (EventType type : allEventTypes())
            assertResolvable(queryEventName(type));

        for (EventContext ctx : eventContexts)
        {
            const char* name = queryLocalEventContextName(ctx);
            if (!isEmptyString(name))
                assertResolvable(name);
        }

        for (EventAttr attr : eventAttrs)
            assertResolvable(queryEventAttributeName(attr));

        for (EventMetaAttr metaAttr : eventMetaAttrs)
            assertResolvable(queryMetaAttributeName(metaAttr));

        END_TEST
    }

    void testNamedMetaObservedInteraction()
    {
        START_TEST

        // Record events that do not include MetaFileInformation. Meta attributes that
        // depend on MetaFileInformation (e.g., meta.Path) are not derivable from these
        // events and should not appear even when requested via --named in observed mode.
        const char* filename = "describe_named_meta_no_file_info.evt";
        COnScopeExit cleanup([&]() { removeFile(filename); });
        writeObservedRecording(filename, "all=false");

        CDescribeEventsOp op;
        op.setFormat(DescribeOutputFormat::json);
        op.setNamedEntity("meta.Path");
        op.setInputPath(filename);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream;
        setupOutput(op, output, stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(output.str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        // meta.Path is not derivable without MetaFileInformation being observed.
        // With --named=meta.Path, output should remain limited to that selected entity,
        // so no other section should render values either.
        assertExactSectionValues(describeTree, "context", {});
        assertExactSectionValues(describeTree, "event", {});
        assertExactSectionValues(describeTree, "attribute", {});
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

    void testObservedMetaEligibilityDependencyCombinations()
    {
        START_TEST

        constexpr EventFileOption includeTraceIds = EventFileOption::Enabled;
        constexpr EventFileOption includeThreadIds = EventFileOption::Enabled;
        constexpr EventFileOption includeStackTraces = EventFileOption::Enabled;
        EventFileProperties props;
        props.options.includeTraceIds = includeTraceIds;
        props.options.includeThreadIds = includeThreadIds;
        props.options.includeStackTraces = includeStackTraces;
        DescribeContext observedCtx({}, true, NamedSelection{}, &props);

        for (EventMetaAttr metaAttr : eventMetaAttrs)
        {
            const MetaRule* rule = getMetaRule(metaAttr);
            CPPUNIT_ASSERT(rule != nullptr);

            bool hasGenericDependency = false;
            for (const MetaRule::Dependency& dep : rule->dependencies)
            {
                if (dep.event == EventNone)
                {
                    hasGenericDependency = true;
                    break;
                }
            }
            CPPUNIT_ASSERT_MESSAGE("meta rule missing required EventNone dependency", hasGenericDependency);

            std::set<EventType> explicitDeps;
            for (const MetaRule::Dependency& dep : rule->dependencies)
            {
                if (dep.event == EventNone)
                    continue;
                explicitDeps.insert(dep.event);
            }
            CPPUNIT_ASSERT_MESSAGE("meta rule missing required explicit dependency", !explicitDeps.empty());

            std::array<__uint64, EventMax> countsWithAllDeps{};
            for (EventType depEvent : explicitDeps)
                countsWithAllDeps[depEvent] = 1;
            observedCtx.counts = countsWithAllDeps;

            EventType candidateType = EventNone;
            for (EventType t : allEventTypes())
            {
                if (!isSemanticallyEligibleForMeta(t, metaAttr))
                    continue;
                if (!observedCtx.isEventEligibleForMeta(t, *rule))
                    continue;

                candidateType = t;
                break;
            }

            CPPUNIT_ASSERT_MESSAGE("no candidate event type found for meta rule", candidateType != EventNone);

            // explicitDeps is a subset of a compile-time bounded dependency set,
            // so this shift remains within size_t width.
            const size_t combos = size_t(1) << explicitDeps.size();
            for (size_t mask = 0; mask < combos; ++mask)
            {
                std::array<__uint64, EventMax> counts{};
                size_t position = 0;
                for (EventType t : explicitDeps)
                {
                    if (mask & (size_t(1) << position))
                        counts[t] = 1;
                    ++position;
                }

                observedCtx.counts = counts;
                const bool eligible = observedCtx.isEventEligibleForMeta(candidateType, *rule);
                const bool expectEligible = (mask == combos - 1);
                if (eligible != expectEligible)
                {
                    StringBuffer msg;
                    msg.appendf("meta eligibility mismatch for %s (candidate=%s, explicitDeps=%zu, mask=%zu/%zu): expected=%s actual=%s",
                        queryMetaAttributeName(metaAttr),
                        queryEventName(candidateType),
                        explicitDeps.size(),
                        mask,
                        combos,
                        expectEligible ? "true" : "false",
                        eligible ? "true" : "false");
                    CPPUNIT_FAIL(msg.str());
                }
            }
        }

        END_TEST
    }

    void testObservedMetaEligibilityOptionGating()
    {
        START_TEST

        const EventAttr gatedAttrs[] = { EvAttrEventTraceId, EvAttrEventThreadId, EvAttrEventStackTrace };
        unsigned expectedCoveredAttrs = 0;
        for (EventAttr gatedAttr : gatedAttrs)
        {
            bool usedByAnyRule = false;
            for (EventMetaAttr metaAttr : eventMetaAttrs)
            {
                const MetaRule* rule = getMetaRule(metaAttr);
                CPPUNIT_ASSERT(rule != nullptr);

                for (const MetaRule::Dependency& dep : rule->dependencies)
                {
                    if (dep.event == EventNone && dep.attr == gatedAttr)
                    {
                        usedByAnyRule = true;
                        break;
                    }
                }

                if (usedByAnyRule)
                    break;
            }

            if (usedByAnyRule)
                expectedCoveredAttrs++;
        }

        EventFileProperties enabledProps;
        enabledProps.options.includeTraceIds = EventFileOption::Enabled;
        enabledProps.options.includeThreadIds = EventFileOption::Enabled;
        enabledProps.options.includeStackTraces = EventFileOption::Enabled;
        DescribeContext enabledCtx({}, true, NamedSelection{}, &enabledProps);

        unsigned coveredAttrs = 0;
        for (EventAttr gatedAttr : gatedAttrs)
        {
            EventFileProperties disabledProps = enabledProps;
            switch (gatedAttr)
            {
            case EvAttrEventTraceId:
                disabledProps.options.includeTraceIds = EventFileOption::Disabled;
                break;
            case EvAttrEventThreadId:
                disabledProps.options.includeThreadIds = EventFileOption::Disabled;
                break;
            case EvAttrEventStackTrace:
                disabledProps.options.includeStackTraces = EventFileOption::Disabled;
                break;
            default:
                continue;
            }
            DescribeContext disabledCtx({}, true, NamedSelection{}, &disabledProps);

            bool foundMatchingRule = false;
            for (EventMetaAttr metaAttr : eventMetaAttrs)
            {
                const MetaRule* rule = getMetaRule(metaAttr);
                CPPUNIT_ASSERT(rule != nullptr);

                bool dependsOnGatedAttr = false;
                std::set<EventType> explicitDeps;
                for (const MetaRule::Dependency& dep : rule->dependencies)
                {
                    if (dep.event == EventNone)
                    {
                        if (dep.attr == gatedAttr)
                            dependsOnGatedAttr = true;
                        continue;
                    }
                    explicitDeps.insert(dep.event);
                }

                if (!dependsOnGatedAttr)
                    continue;
                foundMatchingRule = true;

                std::array<__uint64, EventMax> countsWithAllDeps{};
                for (EventType depEvent : explicitDeps)
                    countsWithAllDeps[depEvent] = 1;
                enabledCtx.counts = countsWithAllDeps;
                disabledCtx.counts = countsWithAllDeps;

                EventType candidateType = EventNone;
                for (EventType t : allEventTypes())
                {
                    if (!isSemanticallyEligibleForMeta(t, metaAttr))
                        continue;
                    if (!enabledCtx.isEventEligibleForMeta(t, *rule))
                        continue;

                    candidateType = t;
                    break;
                }
                CPPUNIT_ASSERT_MESSAGE("no eligible candidate found in enabled mode", candidateType != EventNone);

                const bool stillEligible = disabledCtx.isEventEligibleForMeta(candidateType, *rule);
                if (stillEligible)
                {
                    StringBuffer msg;
                    msg.appendf("meta eligibility should drop when %s is disabled for %s (candidate=%s)",
                        queryEventAttributeName(gatedAttr),
                        queryMetaAttributeName(metaAttr),
                        queryEventName(candidateType));
                    CPPUNIT_FAIL(msg.str());
                }
            }

            if (foundMatchingRule)
                coveredAttrs++;
        }

        // No minimum is enforced here: expected coverage intentionally tracks
        // the current rule set, including the valid case where no rule uses
        // these gated generic dependencies.
        CPPUNIT_ASSERT_EQUAL(expectedCoveredAttrs, coveredAttrs);
        END_TEST
    }

    void testObservedNamesWithoutTraceOrThreadIds()
    {
        START_TEST

        const char* filename = "describe_observed_no_ids.evt";
        COnScopeExit cleanup([&]() { removeFile(filename); });
        writeObservedRecording(filename, "all=false");

        const std::string json = describeToJson({ filename });
        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(json.c_str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        std::set<std::string> contexts;
        std::set<std::string> events;
        std::set<std::string> attributes;
        collectScalarList(contexts, describeTree, "context");
        collectScalarList(events, describeTree, "event");
        collectScalarList(attributes, describeTree, "attribute");

        CPPUNIT_ASSERT(contexts.count("Index") != 0);
        CPPUNIT_ASSERT(contexts.count("Query") != 0);
        CPPUNIT_ASSERT(contexts.count("Other") == 0);

        CPPUNIT_ASSERT(events.count("IndexLoad") != 0);
        CPPUNIT_ASSERT(events.count("QueryStart") != 0);
        CPPUNIT_ASSERT(events.count("QueryStop") != 0);
        CPPUNIT_ASSERT(events.count("RecordingSource") == 0);

        CPPUNIT_ASSERT(attributes.count("EventTraceId") == 0);
        CPPUNIT_ASSERT(attributes.count("EventThreadId") == 0);
        CPPUNIT_ASSERT(attributes.count("EventStackTrace") == 0);
        CPPUNIT_ASSERT(attributes.count("ServiceName") != 0);
        CPPUNIT_ASSERT(attributes.count("FileId") != 0);
        CPPUNIT_ASSERT(attributes.count("FileOffset") != 0);
        CPPUNIT_ASSERT(attributes.count("NodeKind") != 0);
        CPPUNIT_ASSERT(attributes.count("InMemorySize") != 0);
        CPPUNIT_ASSERT(attributes.count("ExpandTime") != 0);
        CPPUNIT_ASSERT(attributes.count("ReadTime") != 0);
        CPPUNIT_ASSERT(attributes.count("meta.ServiceName") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.LogicalFileName") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Path") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Plane") == 0);

        END_TEST
    }

    void testObservedNamesWithTraceAndThreadIds()
    {
        START_TEST

        const char* filename = "describe_observed_with_ids.evt";
        COnScopeExit cleanup([&]() { removeFile(filename); });
        writeObservedRecording(filename, "all=false,traceid=true,threadid=true");

        const std::string json = describeToJson({ filename });
        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(json.c_str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        std::set<std::string> attributes;
        collectScalarList(attributes, describeTree, "attribute");

        CPPUNIT_ASSERT(attributes.count("EventTraceId") != 0);
        CPPUNIT_ASSERT(attributes.count("EventThreadId") != 0);
        CPPUNIT_ASSERT(attributes.count("EventStackTrace") == 0);
        CPPUNIT_ASSERT(attributes.count("ServiceName") != 0);
        CPPUNIT_ASSERT(attributes.count("meta.ServiceName") != 0);
        CPPUNIT_ASSERT(attributes.count("meta.LogicalFileName") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Path") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Plane") == 0);
        END_TEST
    }

    void testObservedNamesFromMultipleFiles()
    {
        START_TEST

        const char* filename1 = "describe_observed_no_ids_multi.evt";
        const char* filename2 = "describe_observed_with_ids_multi.evt";
        COnScopeExit cleanup1([&]() { removeFile(filename1); });
        COnScopeExit cleanup2([&]() { removeFile(filename2); });
        writeObservedRecording(filename1, "all=false");
        writeObservedRecording(filename2, "all=false,traceid=true,threadid=true");

        const std::string json = describeToJson({ filename1, filename2 });
        Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(json.c_str());
        const IPropertyTree& describeTree = queryDescribeRoot(*jsonTree);

        std::set<std::string> contexts;
        std::set<std::string> events;
        std::set<std::string> attributes;
        collectScalarList(contexts, describeTree, "context");
        collectScalarList(events, describeTree, "event");
        collectScalarList(attributes, describeTree, "attribute");

        CPPUNIT_ASSERT(contexts.count("Index") != 0);
        CPPUNIT_ASSERT(contexts.count("Query") != 0);
        CPPUNIT_ASSERT(events.count("IndexLoad") != 0);
        CPPUNIT_ASSERT(events.count("QueryStart") != 0);
        CPPUNIT_ASSERT(events.count("QueryStop") != 0);
        CPPUNIT_ASSERT(attributes.count("EventTraceId") != 0);
        CPPUNIT_ASSERT(attributes.count("EventThreadId") != 0);
        CPPUNIT_ASSERT(attributes.count("meta.ServiceName") != 0);
        CPPUNIT_ASSERT(attributes.count("meta.LogicalFileName") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Path") == 0);
        CPPUNIT_ASSERT(attributes.count("meta.Plane") == 0);
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
        CPPUNIT_ASSERT(strstr(result, "\"@name\"") == nullptr);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventDescribeTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventDescribeTests, "eventdescribe");

#endif // _USE_CPPUNIT
