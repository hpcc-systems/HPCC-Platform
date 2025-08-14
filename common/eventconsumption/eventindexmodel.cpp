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
#include <vector>

class CIndexEventModel : public CInterfaceOf<IEventModel>
{
public: // IEventModel
    IMPLEMENT_IEVENTVISITATIONLINK;

    virtual bool visitEvent(CEvent& event) override
    {
        if (!nextLink)
            return false;
        // Dispatch an event collection initially containing the current event to the appropriate
        // event handler. An event handler may modify the event collection as needed, including
        // updating the event, discarding the event, or adding new events.
        std::vector<CEvent> events;
        events.push_back(event);
        switch (event.queryType())
        {
        case MetaFileInformation:
            onMetaFileInformation(events);
            break;
        case EventIndexCacheHit:
            onIndexCacheHit(events);
            break;
        case EventIndexCacheMiss:
            onIndexCacheMiss(events);
            break;
        case EventIndexLoad:
            onIndexLoad(events);
            break;
        case EventIndexEviction:
            onIndexEviction(events);
            break;
        case EventIndexPayload:
            onIndexPayload(events);
            break;
        default:
            break;
        }
        // Propagate each collected event until complete or aborted.
        for (CEvent& e : events)
        {
            if (!nextLink->visitEvent(e))
                return false;
        }
        // Visitation is never aborted by the model.
        return true;
    }

public:
    bool configure(const IPropertyTree& config)
    {
        const IPropertyTree* node = config.queryBranch("storage");
        if (!node)
            return false;
        storage.configure(*node);
        node = config.queryBranch("expansion");
        if (node)
            expansion.configure(*node);
        return true;
    }

protected:
    void onMetaFileInformation(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        storage.observeFile(event.queryNumericValue(EvAttrFileId), event.queryTextValue(EvAttrPath));
    }

    void onIndexCacheHit(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        ModeledPage page;
        bool firstObservation = expansion.observePage(event);
        expansion.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
        {
            // A recorded hit without an index cache may transform the recorded size and time
            // into estimated values.
            event.setValue(EvAttrInMemorySize, page.expanded.size);
            event.setValue(EvAttrExpandTime, page.expansionTime);

            // If an index cache is in use and the page is not in the cache, the size and time
            // transform to zeroes, and a simulated IndexLoad is required.
        }
        else if (ExpansionMode::OnLoadToOnDemand == page.expansionMode)
        {
            // A recorded hit without an index cache replaces the size and time with zeroes.
            event.setValue(EvAttrInMemorySize, page.compressed.size);
            event.setValue(EvAttrExpandTime, 0ULL);

            // If an index cache is in use and the page is not in the cache, the size and time
            // transform to zeroes, and a simulated IndexLoad is required.
        }
        else if (ExpansionMode::OnDemand == page.expansionMode)
        {
            // A recorded hit without an index cache may transform the recorded size to an
            // estimated value. The time should be zero.
            event.setValue(EvAttrInMemorySize, page.compressed.size);
            event.setValue(EvAttrExpandTime, 0ULL);

            // If the page was not in an active index cache, the size is also zero and a
            // simulated IndexLoad is required.
        }
    }

    void onIndexCacheMiss(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        (void)expansion.observePage(event);
    }

    void onIndexLoad(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        if (!expansion.refreshObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        storage.useAndDescribePage(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset), event.queryNumericValue(EvAttrNodeKind), page);
        event.setValue(EvAttrReadTime, page.readTime);
        expansion.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
        {
            event.setValue(EvAttrInMemorySize, page.expanded.size);
            event.setValue(EvAttrExpandTime, page.expansionTime);
        }
        else
        {
            event.setValue(EvAttrInMemorySize, page.compressed.size);
            event.setValue(EvAttrExpandTime, 0ULL);
        }
    }

    void onIndexEviction(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        if (!expansion.checkObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        expansion.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
            event.setValue(EvAttrInMemorySize, page.expanded.size);
        else
            event.setValue(EvAttrInMemorySize, page.compressed.size);
    }

    void onIndexPayload(std::vector<CEvent>& events)
    {
        CEvent& event = events.front();
        if (!expansion.checkObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        expansion.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
            event.setValue(EvAttrExpandTime, 0ULL);
        else
            event.setValue(EvAttrExpandTime, page.expansionTime);
    }

    void simulateIndexLoad(std::vector<CEvent>& events)
    {
        CEvent& lookup = events.front();
        assertex(lookup.queryType() == EventIndexCacheHit);
        events.emplace_back();
        CEvent& load = events.back();
        load.reset(EventIndexLoad);
        for (CEventAttribute& attr : load.assignedAttributes)
        {
            if (!lookup.hasAttribute(attr.queryId()))
                continue;
            if (attr.isTimestamp())
                attr.setValue(lookup.queryNumericValue(attr.queryId()) + 1ULL);
            else if (attr.isNumeric())
                attr.setValue(lookup.queryNumericValue(attr.queryId()));
            else if (attr.isBoolean())
                attr.setValue(lookup.queryBooleanValue(attr.queryId()));
            else
                attr.setValue(lookup.queryTextValue(attr.queryId()));
        }
    }

protected:
    Storage storage;
    Expansion expansion;
};

IEventModel* createIndexEventModel(const IPropertyTree& config)
{
    Owned<CIndexEventModel> model = new CIndexEventModel;
    model->configure(config);
    return model.getClear();
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class IndexEventModelTests : public CppUnit::TestFixture
{
    // Each use of testEventVisitationLinks() should generally be a separate test case. The test
    // framework will identify the test case for any failure. It will not identify which use of
    // testEventVisitationLinks() failed.

    CPPUNIT_TEST_SUITE(IndexEventModelTests);
    CPPUNIT_TEST(testImplicitNoPageCache);
    CPPUNIT_TEST(testExplicitNoPageCache);
    CPPUNIT_TEST(testPageCacheEviction);
    CPPUNIT_TEST(testNoPageCacheEviction);
    CPPUNIT_TEST(testInMemoryExpansionEstimation);
    CPPUNIT_TEST(testOnLoadExpansionEstimation);
    CPPUNIT_TEST(testTransformExpansionEstimation);
    CPPUNIT_TEST(testOnDemandExpansionEstimation);
    CPPUNIT_TEST(testOnLoadExpansionActual);
    CPPUNIT_TEST(testTransformExpansionActual);
    CPPUNIT_TEST(testOnDemandExpansionActual);
    CPPUNIT_TEST_SUITE_END();

public:
    void testImplicitNoPageCache()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                    <plane name="a" readTime="500"/>
                    </storage>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="500"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testExplicitNoPageCache()
    {
        constexpr const char* testData = R"!!!(
        {
            "link": {
                "storage": {
                    "@cache-read": 0,
                    "plane": {
                        "@name": "a",
                        "@readTime": 500
                    }
                },
                "@type": "index-events"
            },
            "input": {
                "event": [
                    {
                        "@type": "IndexLoad",
                        "@FileId": 1,
                        "@FileOffset": 0,
                        "@NodeKind": 0,
                        "@InMemorySize": 0,
                        "@ExpandTime": 0,
                        "@ReadTime": 0
                    },
                    {
                        "@type": "IndexLoad",
                        "@FileId": 1,
                        "@FileOffset": 0,
                        "@NodeKind": 0,
                        "@InMemorySize": 0,
                        "@ExpandTime": 0,
                        "@ReadTime": 0
                    }
                ]
            },
            "expect": {
                "event": [
                    {
                        "@type": "IndexLoad",
                        "@FileId": 1,
                        "@FileOffset": 0,
                        "@NodeKind": 0,
                        "@InMemorySize": 0,
                        "@ExpandTime": 0,
                        "@ReadTime": 500
                    },
                    {
                        "@type": "IndexLoad",
                        "@FileId": 1,
                        "@FileOffset": 0,
                        "@NodeKind": 0,
                        "@InMemorySize": 0,
                        "@ExpandTime": 0,
                        "@ReadTime": 500
                    }
                ]
            }
        })!!!";
        testEventVisitationLinks(testData);
    }

    void testPageCacheEviction()
    {
        constexpr const char* testData = R"!!!(
link:
- type: index-events
  storage:
    cache-read: 100
    cache-capacity: 8192
    plane:
    - name: a
      readTime: 500
input:
  event:
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
  - type: IndexLoad
    FileId: 1
    FileOffset: 8192
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
  - type: IndexLoad
    FileId: 1
    FileOffset: 8192
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 0
expect:
  event:
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 500
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 100
  - type: IndexLoad
    FileId: 1
    FileOffset: 8192
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 500
  - type: IndexLoad
    FileId: 1
    FileOffset: 8192
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 100
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 500
  - type: IndexLoad
    FileId: 1
    FileOffset: 0
    NodeKind: 0
    InMemorySize: 0
    ExpandTime: 0
    ReadTime: 100
)!!!";
        testEventVisitationLinks(testData);
    }

    void testNoPageCacheEviction()
    {
        // Same setup as onePageCache with the exception that no eviction occurs
        // because the cache is large enough to hold both pages.
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage cache-read="100" cache-capacity="16384">
                        <plane name="a" readTime="500"/>
                    </storage>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="100"/>
                    <event type="IndexLoad" FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexLoad" FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="100"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="100"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="100"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testInMemoryExpansionEstimation()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" sizeFactor="2.0" sizeToTimeFactor="0.75"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="20000" ExpandTime="1000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="30000" ExpandTime="2000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="20000" ExpandTime="1000" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="30000" ExpandTime="2000" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="2000" InMemorySize="30000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" InMemorySize="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="20000"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="30000"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0"/>
                </input>
                <expect>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="12288" ExpandTime="3072"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="16384" ExpandTime="12288"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="16384" ExpandTime="12288"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="12288" ExpandTime="3072" ReadTime="500"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="16384" ExpandTime="12288" ReadTime="500"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="12288" ExpandTime="3072" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" InMemorySize="0"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" InMemorySize="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="12288"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="12288"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnLoadExpansionEstimation()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" mode="ll" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testTransformExpansionEstimation()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" mode="ld" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnDemandExpansionEstimation()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" mode="dd" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnLoadExpansionActual()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="1" mode="ll"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testTransformExpansionActual()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="1" mode="ld"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="1000"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="8192" ExpandTime="1000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnDemandExpansionActual()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <link type="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <expansion>
                        <node kind="1" mode="dd"/>
                    </expansion>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="14576" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="33874" ExpandTime="14576"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" InMemorySize="33874" ExpandTime="14576"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexEventModelTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexEventModelTests, "indexeventmodel");

#endif