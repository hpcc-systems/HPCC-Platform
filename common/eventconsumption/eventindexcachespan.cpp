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

#include "eventindexcachespan.h"
#include "eventindex.hpp"
#include "eventutility.hpp"
#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>

namespace
{
enum class CacheSpanState : unsigned
{
    Open,
    Loaded = 0x01,
    Hit = 0x02,
    Evicted = 0x04,
};
BITMASK_ENUM(CacheSpanState);
// Alternative to macro-defined hasMask, which tests if any bit of 'r' is set in 'l',
// by checking if all bits in 'r' are set in 'l'.
inline bool hasMasks(CacheSpanState l, CacheSpanState r) { return (l & r) == r; }

constexpr CacheSpanState spanComplete = CacheSpanState::Loaded | CacheSpanState::Evicted;
constexpr unsigned spanStateCount = unsigned(spanComplete | CacheSpanState::Hit) + 1;

using AccessGapRange = std::pair<__uint64, __uint64>;

class SpanMetric
{
public:
    __uint64 count{0};
    __uint64 min{0};
    __uint64 max{0};
    __uint64 sum{0};

    void accumulate(__uint64 value)
    {
        if (0 == count || value < min)
            min = value;
        if (0 == count || value > max)
            max = value;
        sum += value;
        count++;
    }

    double average() const
    {
        return count ? (double)sum / count : 0;
    }
};

class AverageMetric
{
public:
    double sum{0};
    __uint64 count{0};

    void accumulate(double value)
    {
        sum += value;
        count++;
    }

    double average() const
    {
        return count ? sum / count : 0;
    }
};

struct CacheSpanRecord
{
    __uint64 id{0};
    CacheSpanState state{CacheSpanState::Open};
    CEvent representative;
    __uint64 liveElapsed{0};
    __uint64 deadElapsed{0};
    __uint64 accesses{0};
    AccessGapRange accessGaps;
};

class CacheSpanStats
{
public:
    __uint64 spans{0};
    SpanMetric liveElapsed;
    SpanMetric deadElapsed;
    SpanMetric completeElapsed;
    SpanMetric accessCount;
    AccessGapRange accessGaps;
    __uint64 accessGapCount{0};
    AverageMetric accessGapSpanAverage;

    void accumulate(const CacheSpanRecord& span)
    {
        __uint64 spanGapCount = span.accesses > 1 ? span.accesses - 1 : 0;
        bool hasAccessGaps = accessGapCount != 0;
        spans++;
        accessCount.accumulate(span.accesses);
        if (span.accesses)
            liveElapsed.accumulate(span.liveElapsed);
        if (span.accesses && hasMasks(span.state, CacheSpanState::Evicted))
            deadElapsed.accumulate(span.deadElapsed);
        if (hasMasks(span.state, spanComplete))
            completeElapsed.accumulate(span.liveElapsed + span.deadElapsed);
        if (spanGapCount)
        {
            accessGapCount += spanGapCount;
            accessGapSpanAverage.accumulate((double)span.liveElapsed / (span.accesses - 1));
            if (!hasAccessGaps)
                accessGaps = span.accessGaps;
            else
            {
                accessGaps.first = std::min(accessGaps.first, span.accessGaps.first);
                accessGaps.second = std::max(accessGaps.second, span.accessGaps.second);
            }
        }
    }
};

class CacheSpanAccumulator
{
public:
    std::array<CacheSpanStats, spanStateCount> states;
    CacheSpanStats allSpans;

    void accumulate(const CacheSpanRecord& span)
    {
        states[unsigned(span.state)].accumulate(span);
        allSpans.accumulate(span);
    }
};

// Test implementations capture completed spans for direct visitor assertions.
class ICacheSpanRowSink
{
public:
    virtual ~ICacheSpanRowSink() = default;
    virtual void emit(const CacheSpanRecord& span) = 0;
};

class CCacheSpanGroupNode
{
public:
    CacheSpanAccumulator subTotal;
    std::unordered_multimap<__uint64, std::unique_ptr<CCacheSpanGroupNode>> children;
    std::vector<std::string> groupValues;
    std::optional<CacheSpanAccumulator> leafSummary;

    void process(const CacheSpanRecord& span, const CMetaInfoState* metaState, const std::vector<std::vector<GroupAttribute>>& hierarchy, size_t currentLevel)
    {
        subTotal.accumulate(span);

        if (currentLevel >= hierarchy.size())
        {
            if (!leafSummary)
                leafSummary.emplace();
            leafSummary->accumulate(span);
            return;
        }

        __uint64 hash = GroupAttributeExtractor::getHash(hierarchy[currentLevel], span.representative, metaState);
        auto range = children.equal_range(hash);
        auto it = range.first;
        for (; it != range.second; ++it)
        {
            if (GroupAttributeExtractor::isEqual(hierarchy[currentLevel], span.representative, metaState, it->second->groupValues))
                break;
        }

        if (it == range.second)
        {
            auto child = std::make_unique<CCacheSpanGroupNode>();
            for (const GroupAttribute& attr : hierarchy[currentLevel])
                child->groupValues.push_back(GroupAttributeExtractor::getValue(attr, span.representative, metaState));
            it = children.emplace(hash, std::move(child));
        }

        it->second->process(span, metaState, hierarchy, currentLevel + 1);
    }

    template <typename Formatter>
    void render(Formatter& formatter, const std::vector<std::string>& parentValues, const std::vector<std::vector<GroupAttribute>>& hierarchy, size_t currentLevel, bool isRoot, bool includeComplete, bool includeIncomplete) const
    {
        if (isRoot && hierarchy.empty())
        {
            formatter.outputClassified({}, subTotal, includeComplete, includeIncomplete);
            formatter.outputAll({}, subTotal);
            return;
        }

        std::vector<std::string> allValues(parentValues);
        if (!isRoot)
        {
            for (size_t i = 0; i < groupValues.size() && currentLevel != 0 && i < hierarchy[currentLevel - 1].size(); ++i)
                allValues.push_back(GroupAttributeExtractor::formatValue(hierarchy[currentLevel - 1][i], groupValues[i]));
        }

        std::vector<const CCacheSpanGroupNode*> orderedChildren;
        orderedChildren.reserve(children.size());
        for (const auto& child : children)
            orderedChildren.push_back(child.second.get());

        std::sort(orderedChildren.begin(), orderedChildren.end(),
            [](const CCacheSpanGroupNode* left, const CCacheSpanGroupNode* right)
            {
                return left->groupValues < right->groupValues;
            });

        for (const CCacheSpanGroupNode* child : orderedChildren)
            child->render(formatter, allValues, hierarchy, currentLevel + 1, false, includeComplete, includeIncomplete);

        if (leafSummary)
        {
            formatter.outputClassified(allValues, *leafSummary, includeComplete, includeIncomplete);
            formatter.outputAll(allValues, *leafSummary);
        }

        if (!children.empty() || isRoot)
        {
            formatter.outputClassified(isRoot ? std::vector<std::string>{} : allValues, subTotal, includeComplete, includeIncomplete);
            formatter.outputAll(isRoot ? std::vector<std::string>{} : allValues, subTotal);
        }
    }
};

class CCacheSpanCsvFormatter
{
public:
    CCacheSpanCsvFormatter(IBufferedSerialOutputStream& _out, const std::vector<std::vector<std::string>>& _groupAttributes)
        : out(_out), groupAttributes(_groupAttributes)
    {
    }

    void begin()
    {
        StringBuffer line;
        for (const auto& group : groupAttributes)
        {
            for (const auto& column : group)
                appendHeaderColumn(line, column.c_str());
        }
        appendHeaderColumn(line, "Loaded");
        appendHeaderColumn(line, "Hit");
        appendHeaderColumn(line, "Evicted");
        appendHeaderColumn(line, "Spans");
        appendHeaderColumn(line, "LiveElapsedMin");
        appendHeaderColumn(line, "LiveElapsedMax");
        appendHeaderColumn(line, "LiveElapsedAvg");
        appendHeaderColumn(line, "DeadElapsedMin");
        appendHeaderColumn(line, "DeadElapsedMax");
        appendHeaderColumn(line, "DeadElapsedAvg");
        appendHeaderColumn(line, "LiveDeadRatio");
        appendHeaderColumn(line, "CompleteElapsedMin");
        appendHeaderColumn(line, "CompleteElapsedMax");
        appendHeaderColumn(line, "CompleteElapsedAvg");
        appendHeaderColumn(line, "AccessesMin");
        appendHeaderColumn(line, "AccessesMax");
        appendHeaderColumn(line, "AccessesAvg");
        appendHeaderColumn(line, "AccessGapMin");
        appendHeaderColumn(line, "AccessGapMax");
        appendHeaderColumn(line, "AccessGapWeightedAvg");
        appendHeaderColumn(line, "AccessGapSpanAvg");
        line.append('\n');
        out.put(line.length(), line.str());
    }

    void outputClassified(const std::vector<std::string>& groupValues, const CacheSpanAccumulator& accumulator, bool includeComplete, bool includeIncomplete)
    {
        for (unsigned stateValue = 0; stateValue < spanStateCount; stateValue++)
        {
            CacheSpanState state = static_cast<CacheSpanState>(stateValue);
            const CacheSpanStats& stats = accumulator.states[stateValue];
            if (0 == stats.spans)
                continue;
            bool complete = hasMasks(state, spanComplete);
            if ((complete && !includeComplete) || (!complete && !includeIncomplete))
                continue;
            StringBuffer line;
            size_t expected = groupColumnCount();
            for (size_t idx = 0; idx < expected; ++idx)
            {
                if (idx != 0)
                    line.append(',');
                if (idx < groupValues.size() && !groupValues[idx].empty())
                    encodeCSVColumn(line, groupValues[idx].c_str());
            }
            appendMetricColumns(line, stats, state, expected != 0, true, hasMasks(state, spanComplete));
            line.append('\n');
            out.put(line.length(), line.str());
        }
    }

    void outputAll(const std::vector<std::string>& groupValues, const CacheSpanAccumulator& accumulator)
    {
        if (0 == accumulator.allSpans.spans)
            return;
        StringBuffer line;
        size_t expected = groupColumnCount();
        for (size_t idx = 0; idx < expected; ++idx)
        {
            if (idx != 0)
                line.append(',');
            if (idx < groupValues.size() && !groupValues[idx].empty())
                encodeCSVColumn(line, groupValues[idx].c_str());
        }
        appendMetricColumns(line, accumulator.allSpans, CacheSpanState::Open, expected != 0, false, 0 != accumulator.allSpans.completeElapsed.count);
        line.append('\n');
        out.put(line.length(), line.str());
    }

private:
    size_t groupColumnCount() const
    {
        size_t count = 0;
        for (const auto& group : groupAttributes)
            count += group.size();
        return count;
    }

    static void appendCSVColumn(StringBuffer& line, const char* value)
    {
        if (!line.isEmpty())
            line.append(',');
        if (!isEmptyString(value))
            encodeCSVColumn(line, value);
    }

    static void appendHeaderColumn(StringBuffer& line, const char* value)
    {
        if (!line.isEmpty())
            line.append(',');
        if (!isEmptyString(value))
            line.append(value);
    }

    static void appendCSVColumn(StringBuffer& line, __uint64 value)
    {
        if (!line.isEmpty())
            line.append(',');
        line.append(value);
    }

    static void appendCSVColumn(StringBuffer& line, double value)
    {
        if (!line.isEmpty())
            line.append(',');
        line.appendf("%.3f", value);
    }

    static void appendCSVColumn(StringBuffer& line, bool value, bool groupColumnsPrecedeStats)
    {
        if (line.isEmpty())
        {
            if (groupColumnsPrecedeStats)
                line.append(',');
        }
        else
            line.append(',');
        encodeCSVColumn(line, value ? "true" : "false");
    }

    static void appendStat(StringBuffer& line, const SpanMetric& stat, bool available)
    {
        if (!available)
        {
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            return;
        }
        appendCSVColumn(line, stat.min);
        appendCSVColumn(line, stat.max);
        appendCSVColumn(line, stat.average());
    }

    static void appendAccessGaps(StringBuffer& line, const CacheSpanStats& stats)
    {
        if (0 == stats.accessGapCount)
        {
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            return;
        }
        __uint64 gaps = stats.accessGapCount;
        appendCSVColumn(line, stats.accessGaps.first);
        appendCSVColumn(line, stats.accessGaps.second);
        appendCSVColumn(line, (double)stats.liveElapsed.sum / gaps);
        appendCSVColumn(line, stats.accessGapSpanAverage.average());
    }

    static void appendLiveDeadRatio(StringBuffer& line, const CacheSpanStats& stats)
    {
        if (0 == stats.deadElapsed.count || 0 == stats.deadElapsed.sum)
        {
            appendCSVColumn(line, "");
            return;
        }
        appendCSVColumn(line, (double)stats.liveElapsed.sum / stats.deadElapsed.sum);
    }

    static void appendMetricColumns(StringBuffer& line, const CacheSpanStats& stats, CacheSpanState state, bool groupColumnsPrecedeStats, bool includeStateFlags, bool includeCompleteElapsed)
    {
        if (includeStateFlags)
        {
            appendCSVColumn(line, hasMask(state, CacheSpanState::Loaded), groupColumnsPrecedeStats);
            appendCSVColumn(line, hasMask(state, CacheSpanState::Hit), false);
            appendCSVColumn(line, hasMask(state, CacheSpanState::Evicted), false);
        }
        else
        {
            if (line.isEmpty())
                line.append(groupColumnsPrecedeStats ? ",,," : ",,");
            else
                line.append(",,,");
        }
        appendCSVColumn(line, stats.spans);
        appendStat(line, stats.liveElapsed, 0 != stats.liveElapsed.count);
        appendStat(line, stats.deadElapsed, 0 != stats.deadElapsed.count);
        appendLiveDeadRatio(line, stats);
        appendStat(line, stats.completeElapsed, includeCompleteElapsed);
        appendStat(line, stats.accessCount, true);
        appendAccessGaps(line, stats);
    }

private:
    IBufferedSerialOutputStream& out;
    const std::vector<std::vector<std::string>>& groupAttributes;
};

class CCacheSpanGroupingSink : public ICacheSpanRowSink
{
public:
    CCacheSpanGroupingSink(CMetaInfoState& _metaState, const std::vector<std::vector<GroupAttribute>>& _groupAttributeIds, bool _includeComplete = false, bool _includeIncomplete = false)
        : metaState(_metaState), groupAttributeIds(_groupAttributeIds)
        , includeComplete(_includeComplete), includeIncomplete(_includeIncomplete)
    {
    }

    virtual void emit(const CacheSpanRecord& span) override
    {
        root.process(span, &metaState, groupAttributeIds, 0);
    }

    void render(CCacheSpanCsvFormatter& formatter) const
    {
        std::vector<std::string> rootValues;
        root.render(formatter, rootValues, groupAttributeIds, 0, true, includeComplete, includeIncomplete);
    }

    const CCacheSpanGroupNode& queryRoot() const { return root; }

private:
    CMetaInfoState& metaState;
    const std::vector<std::vector<GroupAttribute>>& groupAttributeIds;
    bool includeComplete;
    bool includeIncomplete;
    CCacheSpanGroupNode root;
};

class CIndexCacheSpanVisitor : public CInterfaceOf<IEventVisitor>
{
    struct LiveNode
    {
        bool startedByLoad{false};
        CEvent firstEvent;
        __uint64 loadTimestamp{0};
        __uint64 lastAccessTimestamp{0};
        __uint64 gaps{0};
        AccessGapRange accessGaps;
    };

public:
    CIndexCacheSpanVisitor(ICacheSpanRowSink& _sink) : sink(_sink)
    {
    }

    virtual bool visitFile(const char*, uint32_t) override
    {
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        switch (event.queryType())
        {
        case EventIndexLoad:
            onLoad(event);
            break;
        case EventIndexCacheHit:
            onHit(event);
            break;
        case EventIndexEviction:
            onEviction(event);
            break;
        default:
            break;
        }
        return true;
    }

    virtual void departFile(uint32_t) override
    {
        for (const auto& item : liveNodes)
            emitOpenSpan(item.second);
        liveNodes.clear();
    }

private:
    bool canUseEvent(const CEvent& event) const
    {
        return event.isComplete() && event.hasAttribute(EvAttrEventTimestamp)
            && event.hasAttribute(EvAttrFileId) && event.hasAttribute(EvAttrFileOffset);
    }

    FileIdFileOffsetKey queryKey(const CEvent& event) const
    {
        return { event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset) };
    }

    CacheSpanRecord createSpan(CacheSpanState state, const CEvent& representative)
    {
        CacheSpanRecord span;
        span.id = nextSpanId++;
        span.state = state;
        span.representative = representative;
        return span;
    }

    void onLoad(const CEvent& event)
    {
        if (!canUseEvent(event))
            return;
        FileIdFileOffsetKey key = queryKey(event);
        auto existing = liveNodes.find(key);
        if (existing != liveNodes.end())
        {
            emitOpenSpan(existing->second);
            liveNodes.erase(existing);
        }

        LiveNode& node = liveNodes[key];
        node.startedByLoad = true;
        node.firstEvent = event;
        node.loadTimestamp = event.queryNumericValue(EvAttrEventTimestamp);
        node.lastAccessTimestamp = node.loadTimestamp;
    }

    void onHit(const CEvent& event)
    {
        if (!canUseEvent(event))
            return;
        FileIdFileOffsetKey key = queryKey(event);
        __uint64 timestamp = event.queryNumericValue(EvAttrEventTimestamp);
        LiveNode& node = liveNodes[key];
        if (!node.startedByLoad && !node.lastAccessTimestamp)
        {
            node.startedByLoad = false;
            node.firstEvent = event;
            node.lastAccessTimestamp = timestamp;
            return;
        }

        if (timestamp >= node.lastAccessTimestamp)
        {
            __uint64 elapsed = timestamp - node.lastAccessTimestamp;
            if (0 == node.gaps)
                node.accessGaps = { elapsed, elapsed };
            else
            {
                node.accessGaps.first = std::min(node.accessGaps.first, elapsed);
                node.accessGaps.second = std::max(node.accessGaps.second, elapsed);
            }
            node.lastAccessTimestamp = timestamp;
            node.gaps++;
        }
    }

    void onEviction(const CEvent& event)
    {
        if (!canUseEvent(event))
            return;
        FileIdFileOffsetKey key = queryKey(event);
        auto existing = liveNodes.find(key);
        if (existing == liveNodes.end())
        {
            CacheSpanRecord span = createSpan(CacheSpanState::Evicted, event);
            sink.emit(span);
            return;
        }

        const LiveNode& node = existing->second;
        if (!node.startedByLoad)
        {
            CacheSpanRecord span = createSpan(CacheSpanState::Evicted, node.firstEvent);
            span.liveElapsed = node.lastAccessTimestamp - node.firstEvent.queryNumericValue(EvAttrEventTimestamp);
            span.accesses = node.gaps + 1;
            span.deadElapsed = event.queryNumericValue(EvAttrEventTimestamp) >= node.lastAccessTimestamp
                ? event.queryNumericValue(EvAttrEventTimestamp) - node.lastAccessTimestamp : 0;
            span.accessGaps = node.accessGaps;
            if (!node.startedByLoad || node.gaps)
                span.state |= CacheSpanState::Hit;
            sink.emit(span);
        }
        else
        {
            __uint64 evictionTimestamp = event.queryNumericValue(EvAttrEventTimestamp);
            CacheSpanRecord span = createSpan(spanComplete, node.firstEvent);
            span.liveElapsed = node.lastAccessTimestamp - node.loadTimestamp;
            span.deadElapsed = evictionTimestamp >= node.lastAccessTimestamp ? evictionTimestamp - node.lastAccessTimestamp : 0;
            span.accesses = node.gaps + 1;
            span.accessGaps = node.accessGaps;
            if (!node.startedByLoad || node.gaps)
                span.state |= CacheSpanState::Hit;
            sink.emit(span);
        }
        liveNodes.erase(existing);
    }

    void emitOpenSpan(const LiveNode& node)
    {
        CacheSpanRecord span = createSpan(node.startedByLoad ? CacheSpanState::Loaded : CacheSpanState::Open, node.firstEvent);
        span.accesses = node.gaps + 1;
        if (node.startedByLoad)
        {
            span.liveElapsed = node.lastAccessTimestamp - node.loadTimestamp;
            span.accessGaps = node.accessGaps;
        }
        else
        {
            span.liveElapsed = node.lastAccessTimestamp - node.firstEvent.queryNumericValue(EvAttrEventTimestamp);
            span.accessGaps = node.accessGaps;
        }
        if (!node.startedByLoad || node.gaps)
            span.state |= CacheSpanState::Hit;
        sink.emit(span);
    }

private:
    ICacheSpanRowSink& sink;
    std::unordered_map<FileIdFileOffsetKey, LiveNode, FileIdFileOffsetKeyHash> liveNodes;
    __uint64 nextSpanId{1};
};

} // namespace

bool CIndexCacheSpanOp::ready() const
{
    return CEventConsumingOp::ready();
}

bool CIndexCacheSpanOp::preScanRequired() const
{
    if (CEventConsumingOp::preScanRequired())
        return true;
    for (const auto& groupIds : groupAttributeIds)
    {
        for (const auto& id : groupIds)
        {
            if (id.attrId == EvAttrServiceName)
                return true;
        }
    }
    return false;
}

bool CIndexCacheSpanOp::doOp()
{
    CCacheSpanGroupingSink sink(queryMetaInfoState(), groupAttributeIds, includeComplete, includeIncomplete);
    Owned<CIndexCacheSpanVisitor> visitor = new CIndexCacheSpanVisitor(sink);
    bool result = traverseEvents(*visitor);
    CCacheSpanCsvFormatter formatter(*out, groupAttributes);
    formatter.begin();
    sink.render(formatter);
    return result;
}

void CIndexCacheSpanOp::addGroupAttribute(const std::vector<std::string>& attrs)
{
    std::vector<std::string> strippedAttrs;
    std::vector<GroupAttribute> ids;
    for (const auto& attrName : attrs)
    {
        GroupAttribute parsed = GroupAttributeExtractor::parseAttribute(attrName.c_str());
        const char* canonicalName = GroupAttributeExtractor::queryCanonicalName(parsed.attrId);
        if (!canonicalName)
            throw makeStringExceptionV(0, "Unsupported grouping attribute '%s' (id=%u). Use a recognized grouping attribute or derived canonical name.", attrName.c_str(), parsed.attrId);
        strippedAttrs.emplace_back(canonicalName);
        ids.push_back(parsed);
    }
    groupAttributes.push_back(std::move(strippedAttrs));
    groupAttributeIds.push_back(std::move(ids));
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"
#include "jstream.hpp"

namespace
{

class CCollectingCacheSpanSink : public ICacheSpanRowSink
{
public:
    virtual void emit(const CacheSpanRecord& span) override
    {
        spans.push_back(span);
    }

public:
    std::vector<CacheSpanRecord> spans;
};

CEvent makeIndexCacheEvent(EventType type, __uint64 timestamp, __uint64 fileId, offset_t offset, NodeKind nodeKind)
{
    CEvent event;
    event.reset(type);
    event.setValue(EvAttrEventTimestamp, timestamp);
    event.setValue(EvAttrFileId, fileId);
    event.setValue(EvAttrFileOffset, offset);
    event.setValue(EvAttrNodeKind, __uint64(nodeKind));
    event.setValue(EvAttrSearchFlags, __uint64(0));
    event.setValue(EvAttrInMemorySize, __uint64(8192));
    if (EventIndexLoad == type)
    {
        event.setValue(EvAttrExpandTime, __uint64(0));
        event.setValue(EvAttrReadTime, __uint64(0));
    }
    if (EventIndexCacheHit == type)
        event.setValue(EvAttrExpandTime, __uint64(0));
    return event;
}

void visitIndexCacheEvent(IEventVisitor& visitor, EventType type, __uint64 timestamp, __uint64 fileId, offset_t offset, NodeKind nodeKind)
{
    CEvent event = makeIndexCacheEvent(type, timestamp, fileId, offset, nodeKind);
    visitor.visitEvent(event);
}

} // namespace

class EventIndexCacheSpanTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventIndexCacheSpanTest);
    CPPUNIT_TEST(testCompleteSpan);
    CPPUNIT_TEST(testLoadCanBeLastAccess);
    CPPUNIT_TEST(testFirstHitIsAmbiguous);
    CPPUNIT_TEST(testFirstEvictionIsAmbiguous);
    CPPUNIT_TEST(testNoLoadIncludesFirstEviction);
    CPPUNIT_TEST(testOpenLoadedNodeIsIncomplete);
    CPPUNIT_TEST(testGroupingDoesNotSkewCompleteStats);
    CPPUNIT_TEST(testCsvOutputUsesGroupColumnsThenStats);
    CPPUNIT_TEST(testAccessGapAveragesUseDifferentWeights);
    CPPUNIT_TEST(testAccessGapsIncludeEvictionOnlySpans);
    CPPUNIT_TEST(testSpecialCasesCanBeIncluded);
    CPPUNIT_TEST(testNoGroupKeyEmitsRootSubtotalOnly);
    CPPUNIT_TEST_SUITE_END();

public:
    void testCompleteSpan()
    {
        START_TEST
        CCollectingCacheSpanSink sink;
        CIndexCacheSpanVisitor visitor(sink);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 130, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 180, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);

        CPPUNIT_ASSERT_EQUAL(size_t(1), sink.spans.size());
        const CacheSpanRecord& span = sink.spans[0];
        CPPUNIT_ASSERT((spanComplete | CacheSpanState::Hit) == span.state);
        CPPUNIT_ASSERT_EQUAL(__uint64(80), span.liveElapsed);
        CPPUNIT_ASSERT_EQUAL(__uint64(70), span.deadElapsed);
        CPPUNIT_ASSERT_EQUAL(__uint64(3), span.accesses);
        CPPUNIT_ASSERT_EQUAL(__uint64(30), span.accessGaps.first);
        CPPUNIT_ASSERT_EQUAL(__uint64(50), span.accessGaps.second);
        END_TEST
    }

    void testLoadCanBeLastAccess()
    {
        START_TEST
        CCollectingCacheSpanSink sink;
        CIndexCacheSpanVisitor visitor(sink);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);

        CPPUNIT_ASSERT_EQUAL(size_t(1), sink.spans.size());
        const CacheSpanRecord& span = sink.spans[0];
        CPPUNIT_ASSERT(spanComplete == span.state);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), span.liveElapsed);
        CPPUNIT_ASSERT_EQUAL(__uint64(150), span.deadElapsed);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), span.accesses);
        END_TEST
    }

    void testFirstHitIsAmbiguous()
    {
        START_TEST
        CCollectingCacheSpanSink sink;
        CIndexCacheSpanVisitor visitor(sink);

        visitIndexCacheEvent(visitor, EventIndexCacheHit, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);

        CPPUNIT_ASSERT_EQUAL(size_t(1), sink.spans.size());
        CPPUNIT_ASSERT((CacheSpanState::Evicted | CacheSpanState::Hit) == sink.spans[0].state);
        END_TEST
    }

    void testFirstEvictionIsAmbiguous()
    {
        START_TEST
        CCollectingCacheSpanSink sink;
        CIndexCacheSpanVisitor visitor(sink);

        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);

        CPPUNIT_ASSERT_EQUAL(size_t(1), sink.spans.size());
        CPPUNIT_ASSERT(CacheSpanState::Evicted == sink.spans[0].state);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), sink.spans[0].accesses);
        END_TEST
    }

    void testNoLoadIncludesFirstEviction()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<GroupAttribute>> groupAttrs;
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, false);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);

        const CacheSpanStats& stats = grouping.queryRoot().subTotal.states[unsigned(CacheSpanState::Evicted)];
        CPPUNIT_ASSERT_EQUAL(__uint64(1), stats.spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), stats.accessCount.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), stats.liveElapsed.count);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), stats.deadElapsed.count);
        END_TEST
    }

    void testOpenLoadedNodeIsIncomplete()
    {
        START_TEST
        CCollectingCacheSpanSink sink;
        CIndexCacheSpanVisitor visitor(sink);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitor.departFile(0);

        CPPUNIT_ASSERT_EQUAL(size_t(1), sink.spans.size());
        CPPUNIT_ASSERT(CacheSpanState::Loaded == sink.spans[0].state);
        END_TEST
    }

    void testGroupingDoesNotSkewCompleteStats()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<GroupAttribute>> groupAttrs{{GroupAttributeExtractor::parseAttribute("NodeKind")}};
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, false);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 150, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 180, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 200, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 220, 2, 4096, LeafNode);

        const CacheSpanAccumulator& totals = grouping.queryRoot().subTotal;
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[7].spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), totals.states[5].spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(50), totals.states[7].liveElapsed.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(30), totals.states[7].deadElapsed.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(80), totals.states[7].completeElapsed.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), totals.states[7].accessCount.sum);
        END_TEST
    }

    void testCsvOutputUsesGroupColumnsThenStats()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<std::string>> groupNames{{"NodeKind"}};
        std::vector<std::vector<GroupAttribute>> groupAttrs{{GroupAttributeExtractor::parseAttribute("NodeKind")}};
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, false);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 150, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 180, 1, 4096, LeafNode);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
        CCacheSpanCsvFormatter formatter(*stream, groupNames);
        formatter.begin();
        grouping.render(formatter);
        stream.clear();

        const char* expectedHeader = "NodeKind,Loaded,Hit,Evicted,Spans,LiveElapsedMin,LiveElapsedMax,LiveElapsedAvg,DeadElapsedMin,DeadElapsedMax,DeadElapsedAvg,LiveDeadRatio,CompleteElapsedMin,CompleteElapsedMax,CompleteElapsedAvg,AccessesMin,AccessesMax,AccessesAvg,AccessGapMin,AccessGapMax,AccessGapWeightedAvg,AccessGapSpanAvg\n";
        CPPUNIT_ASSERT_EQUAL(std::string(expectedHeader), std::string(output.str(), strlen(expectedHeader)));
        CPPUNIT_ASSERT(nullptr == strstr(output.str(), "RowType"));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), "\"leaf\",\"true\",\"true\",\"true\",1,50,50,50.000,30,30,30.000,1.667,80,80,80.000,2,2,2.000,50,50,50.000,50.000\n"));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), "\n,\"true\",\"true\",\"true\",1,50,50,50.000,30,30,30.000,1.667,80,80,80.000,2,2,2.000,50,50,50.000,50.000\n"));
        END_TEST
    }

    void testAccessGapAveragesUseDifferentWeights()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<GroupAttribute>> groupAttrs;
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, false);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 200, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 250, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexLoad, 300, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 301, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 302, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 350, 2, 4096, LeafNode);

        const CacheSpanStats& stats = grouping.queryRoot().subTotal.states[unsigned(spanComplete | CacheSpanState::Hit)];
        CPPUNIT_ASSERT_DOUBLES_EQUAL(34.0, (double)stats.liveElapsed.sum / (stats.accessCount.sum - stats.accessCount.count), 0.001);
        CPPUNIT_ASSERT_DOUBLES_EQUAL(50.5, stats.accessGapSpanAverage.average(), 0.001);
        END_TEST
    }

    void testAccessGapsIncludeEvictionOnlySpans()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<std::string>> groupNames;
        std::vector<std::vector<GroupAttribute>> groupAttrs;
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, true);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 110, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 130, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 150, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 200, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexLoad, 300, 3, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 330, 3, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 360, 3, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 400, 3, 4096, LeafNode);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
        CCacheSpanCsvFormatter formatter(*stream, groupNames);
        formatter.begin();
        grouping.render(formatter);
        stream.clear();

        CPPUNIT_ASSERT(nullptr != strstr(output.str(), "AccessGapMin,AccessGapMax,AccessGapWeightedAvg,AccessGapSpanAvg"));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), ",10,30,22.500,22.500\n"));
        END_TEST
    }

    void testSpecialCasesCanBeIncluded()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<GroupAttribute>> groupAttrs{{GroupAttributeExtractor::parseAttribute("NodeKind")}};
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, true);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexCacheHit, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 150, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 180, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexLoad, 200, 2, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexCacheHit, 250, 3, 4096, LeafNode);
        visitor.departFile(0);

        const CacheSpanAccumulator& totals = grouping.queryRoot().subTotal;
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[6].spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[2].spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[1].spans);
        CPPUNIT_ASSERT_EQUAL(__uint64(2), totals.states[6].accessCount.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[2].accessCount.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(1), totals.states[1].accessCount.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(30), totals.states[6].deadElapsed.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(50), totals.states[6].liveElapsed.sum);
        CPPUNIT_ASSERT_EQUAL(__uint64(50), totals.states[6].accessGaps.first);
        CPPUNIT_ASSERT_EQUAL(__uint64(50), totals.states[6].accessGaps.second);
        CPPUNIT_ASSERT_EQUAL(__uint64(0), totals.states[2].liveElapsed.sum);
        END_TEST
    }

    void testNoGroupKeyEmitsRootSubtotalOnly()
    {
        START_TEST
        CMetaInfoState metaState;
        std::vector<std::vector<GroupAttribute>> groupAttrs;
        CCacheSpanGroupingSink grouping(metaState, groupAttrs, true, false);
        CIndexCacheSpanVisitor visitor(grouping);

        visitIndexCacheEvent(visitor, EventIndexLoad, 100, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexEviction, 180, 1, 4096, LeafNode);
        visitIndexCacheEvent(visitor, EventIndexLoad, 200, 2, 4096, LeafNode);
        visitor.departFile(0);

        StringBuffer output;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
        std::vector<std::vector<std::string>> groupNames;
        CCacheSpanCsvFormatter formatter(*stream, groupNames);
        formatter.begin();
        grouping.render(formatter);
        stream.clear();

        const char* firstNewline = strchr(output.str(), '\n');
        CPPUNIT_ASSERT(firstNewline);
        const char* secondNewline = strchr(firstNewline + 1, '\n');
        CPPUNIT_ASSERT(secondNewline);
        const char* thirdNewline = strchr(secondNewline + 1, '\n');
        CPPUNIT_ASSERT(thirdNewline);
        CPPUNIT_ASSERT(nullptr == strchr(thirdNewline + 1, '\n'));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), "\"true\",\"false\",\"true\",1,0,0,0.000,80,80,80.000"));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), "\n,,,2,"));
        CPPUNIT_ASSERT(nullptr != strstr(output.str(), ",1,1,1.000,,,,\n"));
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventIndexCacheSpanTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventIndexCacheSpanTest, "eventindexcachespan");

#endif