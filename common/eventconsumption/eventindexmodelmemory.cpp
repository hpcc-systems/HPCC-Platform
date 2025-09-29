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

#include "eventindexmodel.hpp"
#include "eventindex.hpp"
#include "jevent.hpp"

MemoryModel::NodeCache::NodeCache(MemoryModel& _parent, NodeKind _kind)
    : parent(_parent)
    , kind(_kind)
{
    assertex(isNodeKind(kind));
}

void MemoryModel::NodeCache::configure(const IPropertyTree& config)
{
    if (config.hasProp("@cacheCapacity"))
        IndexMRUCache::configure(config);
}

bool MemoryModel::NodeCache::enabled() const
{
    return (capacity > 0);
}

__uint64 MemoryModel::NodeCache::size(const IndexHashKey& key)
{
    return parent.nodeEntrySize(key, kind);
}

const char* MemoryModel::NodeCache::description() const
{
    return "node cache";
}

static void parseExpansionMode(const char* modeStr, ExpansionMode& expansionMode)
{
    if (isEmptyString(modeStr) || strieq(modeStr, "ll"))
        expansionMode = ExpansionMode::OnLoad;
    else if (strieq(modeStr, "ld"))
        expansionMode = ExpansionMode::OnLoadToOnDemand;
    else if (strieq(modeStr, "dd"))
        expansionMode = ExpansionMode::OnDemand;
    else
        throw makeStringExceptionV(-1, "invalid index model expansion mode '%s'", modeStr);
}

bool MemoryModel::isCacheEnabled() const
{
    for (unsigned idx = 0; idx < NumNodeKinds; ++idx)
    {
        if (caches[idx].enabled())
            return true;
    }
    return false;
}

__uint64 MemoryModel::getCacheSize() const
{
    __uint64 total = 0;
    for (unsigned idx = 0; idx < NumNodeKinds; ++idx)
        total += caches[idx].capacity;
    return total;
}

MemoryModel::MemoryModel()
    : caches{NodeCache(*this, BranchNode), NodeCache(*this, LeafNode), NodeCache(*this, BlobNode)}
{
}

void MemoryModel::configure(const IPropertyTree& config)
{
    unsigned estimatedCount = 0;

    // Process settings for each node kind.
    for (unsigned idx = 0; idx < NumNodeKinds; ++idx)
    {
        // The xpath must support both numeric (@kind='0') and text (@kind='branch') formats
        // because configuration files may use either representation for @kind. queryPropTree
        // throws an exception for an ambiguous xpath when string values are present.
        const IPropertyTree* node = config.queryPropTree(VStringBuffer("node[@kind='%u']", idx));
        if (!node)
        {
            node = config.queryPropTree(VStringBuffer("node[@kind='%s']", nodeKindText[idx]));
            if (!node)
                continue;
        }

        // Identify per-kind overrides of the default expansion mode, except branch nodes which
        // are always on-load.
        if (idx != BranchNode && node->hasProp("@expansionMode"))
            parseExpansionMode(node->queryProp("@expansionMode"), modes[idx]);

        // MORE: Consider support for custom compressed size "estimates" if more precise values are
        // available. If configured per node element, expansion factors must become optional.
        estimates[idx].compressed = node->getPropReal("@compressed", estimates[idx].compressed);

        // Identify per-kind expansion estimation factors.
        bool haveSizeFactor = node->hasProp("@sizeFactor");
        bool haveSizeToTimeFactor = node->hasProp("@sizeToTimeFactor");
        if (haveSizeFactor != haveSizeToTimeFactor)
            throw makeStringExceptionV(-1, "index model expansion configuration for node kind %u is missing %s", idx, haveSizeFactor ? "sizeToTimeFactor" : "sizeFactor");
        if (haveSizeFactor)
        {
            double sizeFactor = node->getPropReal("@sizeFactor");
            double sizeToTimeFactor = node->getPropReal("@sizeToTimeFactor");
            assertex(sizeFactor > 0.0);
            assertex(sizeToTimeFactor > 0.0);
            estimates[idx].expanded = estimates[idx].compressed * sizeFactor;
            estimates[idx].time = estimates[idx].expanded * sizeToTimeFactor;
            estimatedCount++;
        }

        // Setup the node cache
        caches[idx].configure(*node);
    }

    // Enforce that either all or no node kinds have estimation factors.
    switch (estimatedCount)
    {
    case 0:
        estimating = false;
        break;
    case NumNodeKinds:
        estimating = true;
        break;
    default:
        throw makeStringException(0, "index model expansion configuration requires estimation factors for all or no node kinds");
    }

    // Populate the cache(s) with canned observations
    Owned<IPropertyTreeIterator> it = config.getElements("observed");
    IndexMRUNullCacheReporter reporter;
    ForEach(*it)
    {
        const IPropertyTree& entry = it->query();
        NodeKind nodeKind = mapNodeKind(entry.queryProp("@NodeKind"));
        if (!usingHistory(nodeKind))
            continue;
        IndexHashKey key;
        key.fileId = (__uint64)entry.getPropInt64("@FileId");
        key.offset = (__uint64)entry.getPropInt64("@FileOffset");
        if (estimating)
            (void)estimatedHistory.insert(key);
        else
        {
            ActualValue& value = actualHistory[key];
            value.size = (__uint64)entry.getPropInt64("@InMemorySize");
            value.time = (__uint64)entry.getPropInt64("@ExpandTime");
        }
        if (caches[nodeKind].enabled())
            (void)caches[nodeKind].insert(key, reporter);
    }
}

void MemoryModel::observePage(const CEvent& event)
{
    // Absent index cache modeling, which doesn't exist yet and may be optional when it does,
    // historical caching is only required when transforming events from on-load to on-demand.
    // And then, only when estimation is not configured.
    if (!usingHistory(event))
        return;

    IndexHashKey key(event);
    if (estimating)
    {
        // Estimated values are pre-calculated. Track the key reference only.
        (void)estimatedHistory.insert(key);
    }
    else
    {
        // Ensure a cache entry exists for the looked up page.
        auto [it, inserted] = actualHistory.insert(std::make_pair(key, ActualValue()));
        if (event.queryType() == EventIndexCacheHit)
        {
            if (inserted)
            {
                // Populate the new cache entry with existing event data.
                it->second.size = event.queryNumericValue(EvAttrInMemorySize);
                it->second.time = event.queryNumericValue(EvAttrExpandTime);
            }
            else
            {
                // Update the existing cache entry if needed based on informative event values.
                // This is unlikely to be needed, but handles fringe cases where recording missed events while paused.
                refreshPage(it, event);
            }
        }
    }
}

bool MemoryModel::checkObservedPage(const CEvent& event) const
{
    if (!usingHistory(event))
        return true;
    IndexHashKey key(event);
    if (estimating)
        return estimatedHistory.count(key) != 0;
    // IndexCacheMiss could have created a node placeholder without data. Incomplete placeholders
    // are not considered observed for the purpose of allowing eviction or payload events to be
    // processed.
    ActualHistory::const_iterator it = actualHistory.find(key);
    return (it != actualHistory.end() && it->second.size != 0);
}

bool MemoryModel::refreshObservedPage(const CEvent& event)
{
    if (!usingHistory(event))
        return true;
    IndexHashKey key(event);
    if (estimating)
        return estimatedHistory.count(key) != 0;
    ActualHistory::iterator it = actualHistory.find(key);
    if (actualHistory.end() == it)
        return false;
    refreshPage(it, event);
    return true;
}

void MemoryModel::refreshPage(ActualHistory::iterator& it, const CEvent& event)
{
    if (!it->second.size)
        it->second.size = event.queryNumericValue(EvAttrInMemorySize);
    // Always refresh the cached time to reduce the chance of keeping outlier values.
    if (event.hasAttribute(EvAttrExpandTime))
    {
        __uint64 expandTime = event.queryNumericValue(EvAttrExpandTime);
        if (expandTime)
            it->second.time = expandTime;
    }
}

void MemoryModel::describePage(const CEvent& event, ModeledPage& page) const
{
    // Only describe pages for index file events
    if (!(event.hasAttribute(EvAttrFileId) && event.hasAttribute(EvAttrFileOffset)))
        return;
    page.fileId = event.queryNumericValue(EvAttrFileId);
    page.offset = event.queryNumericValue(EvAttrFileOffset);
    page.nodeKind = queryIndexNodeKind(event);
    assertex(isNodeKind(page.nodeKind));
    page.expansionMode = modes[page.nodeKind];

    IndexHashKey key(event);
    if (estimating)
    {
        if (!usingHistory(page.nodeKind) || estimatedHistory.count(key))
        {
            page.compressed = {estimates[page.nodeKind].compressed, true};
            page.expanded = {estimates[page.nodeKind].expanded, true};
            page.expansionTime = estimates[page.nodeKind].time;
        }
    }
    else if (usingHistory(page.nodeKind))
    {
        ActualHistory::const_iterator it = actualHistory.find(key);
        if (it != actualHistory.end())
        {
            if (event.queryType() == EventIndexPayload)
            {
                switch (page.expansionMode)
                {
                case ExpansionMode::OnLoad:
                case ExpansionMode::OnLoadToOnDemand:
                    page.compressed = {estimates[page.nodeKind].compressed, true};
                    page.expanded = {it->second.size, false};
                    page.expansionTime = it->second.time;
                    break;
                case ExpansionMode::OnDemand:
                    page.compressed = {it->second.size, false};
                    page.expanded = {event.queryNumericValue(EvAttrInMemorySize), false};
                    page.expansionTime = event.queryNumericValue(EvAttrExpandTime);
                    break;
                default:
                    break;
                }
            }
            else
            {
                switch (page.expansionMode)
                {
                case ExpansionMode::OnLoad:
                case ExpansionMode::OnLoadToOnDemand:
                    page.compressed = {estimates[page.nodeKind].compressed, true};
                    page.expanded = {it->second.size, false};
                    page.expansionTime = it->second.time;
                    break;
                case ExpansionMode::OnDemand:
                    page.compressed = {it->second.size, false};
                    page.expanded = {UINT32_MAX, true}; // No estimate available for the expanded
                    page.expansionTime = it->second.time;
                    break;
                default:
                    break;
                }
            }
        }
    }
    else // if (!usingHistory(nodeKind))
    {
        if (ExpansionMode::OnDemand == page.expansionMode)
        {
            if (event.queryType() == EventIndexPayload)
            {
                page.compressed = {UINT64_MAX, true}; // No estimate available for the compressed
                page.expanded = {event.queryNumericValue(EvAttrInMemorySize), false};
            }
            else
            {
                page.compressed = {event.queryNumericValue(EvAttrInMemorySize), false};
                page.expanded = {UINT64_MAX, true}; // No estimate available for the expanded
            }
            page.expansionTime = (event.hasAttribute(EvAttrExpandTime) ? event.queryNumericValue(EvAttrExpandTime) : 0);
        }
        else // if (ExpansionMode::OnLoad == page.expansionMode)
        {
            page.compressed = {estimates[page.nodeKind].compressed, true};
            page.expanded = {event.queryNumericValue(EvAttrInMemorySize), false};
            page.expansionTime = (event.hasAttribute(EvAttrExpandTime) ? event.queryNumericValue(EvAttrExpandTime) : 0);
        }
    }

    if (caches[page.nodeKind].enabled()) // model cache enabled ==> describe model cache content
        page.cacheHit = caches[page.nodeKind].exists(key);
    else if (event.queryType() == EventIndexCacheHit) // model cache disabled ==> describe recorded state
        page.cacheHit = true;
    else if (event.queryType() == EventIndexCacheMiss) // model cache disabled ==> describe recorded state
        page.cacheHit = false;
    else // cache state assumed to be irrelevant
        page.cacheHit = false;
}

void MemoryModel::cachePage(ModeledPage& page, IndexMRUCacheReporter& reporter)
{
    (void)caches[page.nodeKind].insert(page.fileId, page.offset, reporter);
}

__uint64 MemoryModel::nodeEntrySize(const IndexHashKey& key, __uint64 kind) const
{
    switch (modes[kind])
    {
    case ExpansionMode::OnLoad:
        if (estimating)
            return estimates[kind].expanded;
        break;
    case ExpansionMode::OnLoadToOnDemand:
        return estimates[kind].compressed;
    case ExpansionMode::OnDemand:
        if (estimating)
            return estimates[kind].compressed;
        break;
    default:
        UNIMPLEMENTED;
    }
    ActualHistory::const_iterator it = actualHistory.find(key);
    if (it != actualHistory.end())
        return it->second.size;
    throw makeStringExceptionV(-1, "missing actual size for key (%llu:%llu)", key.fileId, key.offset);
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class IndexModelMemoryTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(IndexModelMemoryTest);
    CPPUNIT_TEST(testDisabledEstimation);
    CPPUNIT_TEST(testEnabledEstimation);
    CPPUNIT_TEST(testDescribeEstimation);
    CPPUNIT_TEST(testDescribeActual);
    CPPUNIT_TEST(testOnLoadConfiguration);
    CPPUNIT_TEST(testMixedConfiguration);
    CPPUNIT_TEST(testNodeCacheCapacity);
    CPPUNIT_TEST_SUITE_END();

public:
    void testDisabledEstimation()
    {
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration("<memory/>");
        memory.configure(*configTree);
        CPPUNIT_ASSERT(memory.estimating == false);
        END_TEST
    }

    void testEnabledEstimation()
    {
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
                <node kind="1" sizeFactor="1.75" sizeToTimeFactor="0.75"/>
                <node kind="2" sizeFactor="2.5" sizeToTimeFactor="1.0"/>
            </memory>)!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT(memory.estimating);
        CPPUNIT_ASSERT(memory.estimates[BranchNode].compressed == 8'192);
        CPPUNIT_ASSERT(memory.estimates[BranchNode].expanded == 8'192);
        CPPUNIT_ASSERT(memory.estimates[BranchNode].time == 8'192);
        CPPUNIT_ASSERT(memory.estimates[LeafNode].compressed == 8'192);
        CPPUNIT_ASSERT(memory.estimates[LeafNode].expanded == 14'336);
        CPPUNIT_ASSERT(memory.estimates[LeafNode].time == 10'752);
        CPPUNIT_ASSERT(memory.estimates[BlobNode].compressed == 8'192);
        CPPUNIT_ASSERT(memory.estimates[BlobNode].expanded == 20'480);
        CPPUNIT_ASSERT(memory.estimates[BlobNode].time == 20'480);
        END_TEST
    }

    void testDescribeEstimation()
    {
        START_TEST
        MemoryModel memory;
        ModeledPage page;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
                <node kind="1" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
                <node kind="2" sizeFactor="2.5" sizeToTimeFactor="1.0"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT(memory.estimating);
        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrFileId, 1ULL);
        event.setValue(EvAttrFileOffset, 0ULL);
        event.setValue(EvAttrNodeKind, BranchNode);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[BranchNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[BranchNode].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[BranchNode].time, page.expansionTime);
        event.setValue(EvAttrNodeKind, LeafNode);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].time, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 0ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].time, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].time, page.expansionTime);
        END_TEST
    }

    void testDescribeActual()
    {
        START_TEST
        MemoryModel memory;
        ModeledPage page;
        CPPUNIT_ASSERT(!memory.estimating);
        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrFileId, 1ULL);
        event.setValue(EvAttrFileOffset, 0ULL);
        event.setValue(EvAttrNodeKind, BranchNode);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[BranchNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrNodeKind, LeafNode);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 0ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(0ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        memory.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(memory.estimates[LeafNode].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(0ULL, page.expansionTime);
        END_TEST
    }

    void testOnLoadConfiguration()
    {
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory/>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BranchNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[LeafNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BlobNode]);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0"/>
                <node kind="1"/>
                <node kind="2"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BranchNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[LeafNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BlobNode]);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="branch"/>
                <node kind="leaf"/>
                <node kind="blob"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BranchNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[LeafNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BlobNode]);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0"/>
                <node kind="1" expansionMode="ll"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BranchNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[LeafNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BlobNode]);
        END_TEST
    }

    void testMixedConfiguration()
    {
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" expansionMode="ld"/>
                <node kind="1" expansionMode="dd"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        // The configured value for branch nodes is ignored.
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, memory.modes[BranchNode]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnDemand, memory.modes[LeafNode]);
        END_TEST
    }

    void testNodeCacheCapacity()
    {
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1  "/>
                <node kind="1" cacheCapacity="1 "/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1ULL, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1ULL, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 b "/>
                <node kind="1" cacheCapacity="1 b"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1ULL, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1ULL, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 kb "/>
                <node kind="1" cacheCapacity="1 kib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1000ULL, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1024ULL, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 mb "/>
                <node kind="1" cacheCapacity="1 mib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1000ULL*1000, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1024ULL*1024, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 gb "/>
                <node kind="1" cacheCapacity="1 gib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1000ULL*1000*1000, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1024ULL*1024*1024, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 tb "/>
                <node kind="1" cacheCapacity="1 tib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1000ULL*1000*1000*1000, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1024ULL*1024*1024*1024, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1 pb "/>
                <node kind="1" cacheCapacity="1 pib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1000ULL*1000*1000*1000*1000, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1024ULL*1024*1024*1024*1024, memory.caches[LeafNode].capacity);
        END_TEST
        START_TEST
        MemoryModel memory;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <memory>
                <node kind="0" cacheCapacity=" 1.5 gb "/>
                <node kind="1" cacheCapacity="1.5 gib"/>
            </memory>
        )!!!");
        memory.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(1500ULL*1000*1000, memory.caches[BranchNode].capacity);
        CPPUNIT_ASSERT_EQUAL(1536ULL*1024*1024, memory.caches[LeafNode].capacity);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexModelMemoryTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexModelMemoryTest, "indexmodelmemory");

#endif // _USE_CPPUNIT
