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
#include "jevent.hpp"

static void parseExpansionMode(const char* modeStr, ExpansionMode& mode)
{
    if (isEmptyString(modeStr) || strieq(modeStr, "ll"))
        mode = ExpansionMode::OnLoad;
    else if (strieq(modeStr, "ld"))
        mode = ExpansionMode::OnLoadToOnDemand;
    else if (strieq(modeStr, "dd"))
        mode = ExpansionMode::OnDemand;
    else
        throw makeStringExceptionV(-1, "invalid index model expansion mode '%s'", modeStr);
}

void Expansion::configure(const IPropertyTree& config)
{
    unsigned estimatedCount = 0;

    // Accepts a default expansion mode for all node kinds.
    const char* modeStr = config.queryProp("@mode");

    // Process settings for each node kind.
    for (unsigned idx = 0; idx < NumKinds; ++idx)
    {
        const IPropertyTree* node = config.queryPropTree(VStringBuffer("node[@kind=%u]", idx));
        if (!node)
            continue;

        // Identify per-kind overrides of the default expansion mode, except branch nodes which
        // are always on-load.
        if (idx != 0 && node->hasProp("@mode"))
            parseExpansionMode(node->queryProp("@mode"), modes[idx]);

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
    }

    // Enforce that either all or no node kinds have estimation factors.
    switch (estimatedCount)
    {
    case 0:
        estimating = false;
        break;
    case NumKinds:
        estimating = true;
        break;
    default:
        throw makeStringException(0, "index model expansion configuration requires estimation factors for all or no node kinds");
    }
}

bool Expansion::observePage(const CEvent& event)
{
    // Absent index cache modeling, which doesn't exist yet and may be optional when it does,
    // historical caching is only required when transforming events from on-load to on-demand.
    // And then, only when estimation is not configured.
    //
    // With index cache modeling available and enabled, historical caching is required when
    // estimation is not configured. Estimated values are always available based on node kind.
    //
    // More: As seen in IndexEventModelTests, bypassing the cache when not structly necessary
    // allows events to propagate from the model that would be suppressed by the cache. Identify
    // for which configurations this discrepancy is acceptable.

    if (!usingHistory(event))
        return false;

    IndexHashKey key(event);
    bool hit = event.queryBooleanValue(EvAttrInCache);
    if (estimating)
    {
        // Estimated values are pre-calculated. Track the key reference only.
        return estimatedHistory.insert(key).second;
    }
    else
    {
        // Ensure a cache entry exists for the looked up page.
        auto [it, inserted] = actualHistory.insert(std::make_pair(key, ActualValue()));
        if (hit)
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
        return inserted;
    }
}

bool Expansion::checkObservedPage(const CEvent& event) const
{
    if (!usingHistory(event))
        return true;
    IndexHashKey key(event);
    if (estimating)
        return estimatedHistory.count(key) != 0;
    return actualHistory.count(key) != 0;
}

bool Expansion::refreshObservedPage(const CEvent& event)
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

void Expansion::refreshPage(ActualHistory::iterator& it, const CEvent& event)
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

void Expansion::describePage(const CEvent& event, ModeledPage& page) const
{
    // Only describe pages for index file events
    if (!(event.hasAttribute(EvAttrFileId) && event.hasAttribute(EvAttrFileOffset)))
        return;
    __uint64 nodeKind = (event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1);
    assertex(nodeKind < NumKinds);
    page.expansionMode = modes[nodeKind];

    IndexHashKey key(event);
    if (estimating)
    {
        if (!usingHistory(nodeKind) || estimatedHistory.count(key))
        {
            page.compressed = {estimates[nodeKind].compressed, true};
            page.expanded = {estimates[nodeKind].expanded, true};
            page.expansionTime = estimates[nodeKind].time;
        }
    }
    else if (usingHistory(nodeKind))
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
                    page.compressed = {estimates[nodeKind].compressed, true};
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
                    page.compressed = {estimates[nodeKind].compressed, true};
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
        page.compressed = {estimates[nodeKind].compressed, true};
        page.expanded = {event.queryNumericValue(EvAttrInMemorySize), false};
        page.expansionTime = (event.hasAttribute(EvAttrExpandTime) ? event.queryNumericValue(EvAttrExpandTime) : 0);
    }
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class IndexModelExpansionTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(IndexModelExpansionTest);
    CPPUNIT_TEST(testDisabledEstimation);
    CPPUNIT_TEST(testEnabledEstimation);
    CPPUNIT_TEST(testDescribeEstimation);
    CPPUNIT_TEST(testDescribeActual);
    CPPUNIT_TEST(testOnLoadConfiguration);
    CPPUNIT_TEST(testMixedConfiguration);
    CPPUNIT_TEST_SUITE_END();

public:
    void testDisabledEstimation()
    {
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration("<expansion/>");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT(expansion.estimating == false);
        END_TEST
    }

    void testEnabledEstimation()
    {
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion>
                <node kind="0" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
                <node kind="1" sizeFactor="1.75" sizeToTimeFactor="0.75"/>
            </expansion>)!!!");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT(expansion.estimating);
        CPPUNIT_ASSERT(expansion.estimates[0].compressed == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[0].expanded == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[0].time == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[1].compressed == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[1].expanded == 14'336);
        CPPUNIT_ASSERT(expansion.estimates[1].time == 10'752);
        END_TEST
    }

    void testDescribeEstimation()
    {
        START_TEST
        Expansion expansion;
        ModeledPage page;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion>
                <node kind="0" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
                <node kind="1" sizeFactor="1.0" sizeToTimeFactor="1.0"/>
            </expansion>
        )!!!");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT(expansion.estimating);
        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrFileId, 1ULL);
        event.setValue(EvAttrFileOffset, 0ULL);
        event.setValue(EvAttrNodeKind, 0ULL);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[0].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[0].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[0].time, page.expansionTime);
        event.setValue(EvAttrNodeKind, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].time, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].time, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].expanded, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(true, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].time, page.expansionTime);
        END_TEST
    }

    void testDescribeActual()
    {
        START_TEST
        Expansion expansion;
        ModeledPage page;
        CPPUNIT_ASSERT(!expansion.estimating);
        CEvent event;
        event.reset(EventIndexLoad);
        event.setValue(EvAttrFileId, 1ULL);
        event.setValue(EvAttrFileOffset, 0ULL);
        event.setValue(EvAttrNodeKind, 0ULL);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[0].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrNodeKind, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(0ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expansionTime);
        event.setValue(EvAttrInMemorySize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(expansion.estimates[1].compressed, page.compressed.size);
        CPPUNIT_ASSERT_EQUAL(true, page.compressed.estimated);
        CPPUNIT_ASSERT_EQUAL(1ULL, page.expanded.size);
        CPPUNIT_ASSERT_EQUAL(false, page.expanded.estimated);
        CPPUNIT_ASSERT_EQUAL(0ULL, page.expansionTime);
        END_TEST
    }

    void testOnLoadConfiguration()
    {
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion/>
        )!!!");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[0]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[1]);
        END_TEST
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion>
                <node kind="0"/>
                <node kind="1"/>
            </expansion>
        )!!!");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[0]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[1]);
        END_TEST
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion mode="ld">
                <node kind="0"/>
                <node kind="1" mode="ll"/>
            </expansion>
        )!!!");
        expansion.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[0]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[1]);
        END_TEST
    }

    void testMixedConfiguration()
    {
        START_TEST
        Expansion expansion;
        Owned<IPropertyTree> configTree = createTestConfiguration(R"!!!(
            <expansion mode="ld">
                <node kind="0" mode="ld"/>
                <node kind="1" mode="dd"/>
            </expansion>
        )!!!");
        expansion.configure(*configTree);
        // The configured value for branch nodes is ignored.
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnLoad, expansion.modes[0]);
        CPPUNIT_ASSERT_EQUAL(ExpansionMode::OnDemand, expansion.modes[1]);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexModelExpansionTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexModelExpansionTest, "indexmodelexpansion");

#endif // _USE_CPPUNIT
