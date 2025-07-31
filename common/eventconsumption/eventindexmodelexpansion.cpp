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

void Expansion::configure(const IPropertyTree& config)
{
    switch (config.getCount("node"))
    {
    case 0:
        estimating = false;
        break;
    case NumKinds:
        for (unsigned idx = 0; idx < NumKinds; ++idx)
        {
            const IPropertyTree* node = config.queryPropTree(VStringBuffer("node[@kind=%u]", idx));
            assertex(node);
            double sizeFactor = node->getPropReal("@sizeFactor");
            double sizeToTimeFactor = node->getPropReal("@sizeToTimeFactor");
            assertex(sizeFactor > 0.0);
            assertex(sizeToTimeFactor > 0.0);
            estimates[idx].size = 8'192 * sizeFactor;
            estimates[idx].time = estimates[idx].size * sizeToTimeFactor;
        }
        estimating = true;
        break;
    default:
        throw makeStringException(0, "invalid index model expansion configuration");
    }
}

void Expansion::describePage(const CEvent& event, ModeledPage& page) const
{
    // Expansion description is only relevant to events using the expanded size attribute.
    if (event.hasAttribute(EvAttrExpandedSize))
    {
        __uint64 eventSize = event.queryNumericValue(EvAttrExpandedSize);
        // The expansion time attribute is not used by all events.
        __uint64 eventTime = (event.hasAttribute(EvAttrExpandTime) ? event.queryNumericValue(EvAttrExpandTime) : 0);

        if (estimating)
        {
            // IndexPayload events do not include NodeKind, which is assumed to be 1 (leaf node)
            __uint64 nodeKind = (event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1);
            page.expandedSize = estimates[nodeKind].size;
            page.expansionTime = estimates[nodeKind].time;
        }
        else
        {
            page.expandedSize = eventSize;
            page.expansionTime = eventTime;
        }
        page.expansionIsEstimated = estimating;
    }
    else
    {
        page.expandedSize = 0;
        page.expansionTime = 0;
        page.expansionIsEstimated = false;
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
        CPPUNIT_ASSERT(expansion.estimates[0].size == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[0].time == 8'192);
        CPPUNIT_ASSERT(expansion.estimates[1].size == 14'336);
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
        event.setValue(EvAttrNodeKind, 0ULL);
        event.setValue(EvAttrExpandedSize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, true);
        event.setValue(EvAttrNodeKind, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, true);
        event.setValue(EvAttrExpandedSize, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, true);
        event.setValue(EvAttrExpandedSize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 8'192ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, true);
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
        event.setValue(EvAttrNodeKind, 0ULL);
        event.setValue(EvAttrExpandedSize, 1ULL);
        event.setValue(EvAttrExpandTime, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, false);
        event.setValue(EvAttrNodeKind, 1ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, false);
        event.setValue(EvAttrExpandedSize, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 0ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, false);
        event.setValue(EvAttrExpandedSize, 1ULL);
        event.setValue(EvAttrExpandTime, 0ULL);
        expansion.describePage(event, page);
        CPPUNIT_ASSERT_EQUAL(page.expandedSize, 1ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionTime, 0ULL);
        CPPUNIT_ASSERT_EQUAL(page.expansionIsEstimated, false);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexModelExpansionTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexModelExpansionTest, "indexmodelexpansion");

#endif // _USE_CPPUNIT
