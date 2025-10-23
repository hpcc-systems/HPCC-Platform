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
#include "eventutility.hpp"
#include <vector>

void IndexMRUCache::configure(const IPropertyTree &config)
{
    const char* capacityStr = config.queryProp("@cacheCapacity");
    if (!isEmptyString(capacityStr))
        capacity = strToBytes<__uint64>(capacityStr, StrToBytesFlags::ThrowOnError);
}

bool IndexMRUCache::exists(const IndexHashKey &key)
{
    if (!enabled())
        return false;
    Hash::iterator hashIt = entries.find(key);
    if (entries.end() == hashIt)
        return false;
    mru.moveToHead(hashIt->second.get());
    return true;
}

bool IndexMRUCache::insert(const IndexHashKey &key, IndexMRUCacheReporter &reporter)
{
    if (!enabled() || exists(key))
        return false;
    __uint64 needed = size(key);
    reserve(needed, reporter);
    std::unique_ptr<Value> entry = std::make_unique<Value>(key);
    mru.enqueueHead(entry.get());
    entries[key].swap(entry);
    used += needed;
    return true;
}

void IndexMRUCache::reserve(__uint64 needed, IndexMRUCacheReporter &reporter)
{
    if (!capacity)
        return;
    if (capacity < needed)
        throw makeStringExceptionV(-1, "%s capacity %llu less than reserved page request %llu", description(), capacity, needed);
    while ((capacity - used) < needed)
    {
        Value *dead = mru.dequeueTail();
        if (!dead)
            throw makeStringExceptionV(-1, "%s MRU unexpectedly empty", description());
        __uint64 released = size(dead->key);
        reporter.reportDropped(dead->key, released);
        used -= released;
        entries.erase(dead->key);
    }
}

class CIndexEventModel : public CInterfaceOf<IEventModel>
{
public: // IEventVisitationLink
    IMPLEMENT_IEVENTVISITATIONLINK;

    virtual void configure(const IPropertyTree& config) override
    {
        const IPropertyTree* node = config.queryBranch("storage");
        if (!node)
            throw makeStringException(-1, "index event model configuration missing required <storage> element");
        storage.configure(*node);
        node = config.queryBranch("memory");
        if (node)
            memory.configure(*node);
    }

public: // IEventModel
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
        for (CEvent &e : events)
        {
            if (!nextLink->visitEvent(e))
                return false;
        }
        // Visitation is never aborted by the model.
        return true;
    }

protected:
    void onMetaFileInformation(std::vector<CEvent> &events)
    {
        CEvent &event = events.front();
        storage.observeFile(event.queryNumericValue(EvAttrFileId), event.queryTextValue(EvAttrPath));
    }

    void onIndexCacheHit(std::vector<CEvent> &events)
    {
        CEvent &event = events.at(0);
        ModeledPage page;
        memory.observePage(event);
        memory.describePage(event, page);
        if (!page.cacheHit)
        {
            storage.useAndDescribePage(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset), event.queryNumericValue(EvAttrNodeKind), page);
            CEvent simulated = event;
            event.changeEventType(EventIndexCacheMiss);
            simulated.changeEventType(EventIndexLoad);
            simulated.setValue(EvAttrEventTimestamp, simulated.queryNumericValue(EvAttrEventTimestamp) + 1ULL);
            switch (page.expansionMode)
            {
            case ExpansionMode::OnLoad:
                simulated.setValue(EvAttrInMemorySize, page.expanded.size);
                simulated.setValue(EvAttrExpandTime, page.expansionTime);
                simulated.setValue(EvAttrReadTime, page.readTime);
                break;
            case ExpansionMode::OnLoadToOnDemand:
            case ExpansionMode::OnDemand:
                simulated.setValue(EvAttrInMemorySize, page.compressed.size);
                simulated.setValue(EvAttrExpandTime, 0ULL);
                simulated.setValue(EvAttrReadTime, page.readTime);
                break;
            default:
                break;
            }
            events.emplace_back(std::move(simulated));

            if (memory.caching(page.nodeKind))
            {
                NodeCacheEvictions evictions(events);
                memory.cachePage(page, evictions);
            }
        }
        else
        {
            switch (page.expansionMode)
            {
            case ExpansionMode::OnLoad:
                event.setValue(EvAttrInMemorySize, page.expanded.size);
                event.setValue(EvAttrExpandTime, page.expansionTime);
                break;
            case ExpansionMode::OnLoadToOnDemand:
            case ExpansionMode::OnDemand:
                event.setValue(EvAttrInMemorySize, page.compressed.size);
                event.setValue(EvAttrExpandTime, 0ULL);
                break;
            default:
                break;
            }
        }
    }

    void onIndexCacheMiss(std::vector<CEvent> &events)
    {
        CEvent &event = events.front();
        ModeledPage page;
        memory.observePage(event);
        memory.describePage(event, page);
        if (page.cacheHit)
        {
            event.changeEventType(EventIndexCacheHit);
            switch (page.expansionMode)
            {
            case ExpansionMode::OnLoad:
                event.setValue(EvAttrInMemorySize, page.expanded.size);
                event.setValue(EvAttrExpandTime, page.expansionTime);
                break;
            case ExpansionMode::OnLoadToOnDemand:
            case ExpansionMode::OnDemand:
                event.setValue(EvAttrInMemorySize, page.compressed.size);
                event.setValue(EvAttrExpandTime, 0ULL);
                break;
            default:
                break;
            }
            suppressions.insert({EventIndexLoad, event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset)});
        }
    }

    void onIndexLoad(std::vector<CEvent> &events)
    {
        CEvent &event = events.front();
        std::set<Suppression>::iterator suppressionIt = suppressions.find({event.queryType(), event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset)});
        if (suppressionIt != suppressions.end())
        {
            events.clear();
            suppressions.erase(suppressionIt);
            return; // discard suppressed page
        }
        if (!memory.refreshObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        storage.useAndDescribePage(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset), event.queryNumericValue(EvAttrNodeKind), page);
        event.setValue(EvAttrReadTime, page.readTime);
        memory.describePage(event, page);
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
        if (memory.caching(page.nodeKind))
        {
            NodeCacheEvictions evictions(events);
            memory.cachePage(page, evictions);
        }
    }

    void onIndexEviction(std::vector<CEvent> &events)
    {
        CEvent &event = events.front();
        if (memory.caching(event))
        {
            events.clear();
            return; // defer evictions to the model cache
        }
        if (!memory.checkObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        memory.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
            event.setValue(EvAttrInMemorySize, page.expanded.size);
        else
            event.setValue(EvAttrInMemorySize, page.compressed.size);
    }

    void onIndexPayload(std::vector<CEvent> &events)
    {
        CEvent &event = events.front();
        if (!memory.checkObservedPage(event))
        {
            events.clear();
            return; // discard unobserved page
        }
        ModeledPage page;
        memory.describePage(event, page);
        if (ExpansionMode::OnLoad == page.expansionMode)
            event.setValue(EvAttrExpandTime, 0ULL);
        else
            event.setValue(EvAttrExpandTime, page.expansionTime);
    }

protected:
    struct Suppression
    {
        EventType eventType;
        __uint64 fileId;
        __uint64 fileOffset;

        bool operator<(const Suppression &other) const
        {
            return (eventType < other.eventType) ||
                   ((eventType == other.eventType) && (fileId < other.fileId)) ||
                   ((eventType == other.eventType) && (fileId == other.fileId) && (fileOffset < other.fileOffset));
        }
    };
    class NodeCacheEvictions : public IndexMRUCacheReporter
    {
    public:
        void reportDropped(const IndexHashKey &key, __uint64 size) override
        {
            events.push_back(events.back());
            CEvent& eviction = events.back();
            eviction.changeEventType(EventIndexEviction);
            eviction.setValue(EvAttrFileId, key.fileId);
            eviction.setValue(EvAttrFileOffset, key.offset);
            eviction.setValue(EvAttrInMemorySize, size);
            eviction.setValue(EvAttrEventTimestamp, nextTimestamp++);
        }
    public:
        NodeCacheEvictions(std::vector<CEvent>& _events)
            : events(_events)
            , nextTimestamp(events.back().queryNumericValue(EvAttrEventTimestamp) + 1ULL)
        {
        }
    private:
        std::vector<CEvent>& events;
        __uint64 nextTimestamp;
    };
std::set<Suppression> suppressions;
    Storage storage;
    MemoryModel memory;
};

IEventModel *createIndexEventModel(const IPropertyTree &config)
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
    CPPUNIT_TEST(testOnLoadToOneDemandExpansionEstimation);
    CPPUNIT_TEST(testOnDemandExpansionEstimation);
    CPPUNIT_TEST(testOnLoadExpansionActual);
    CPPUNIT_TEST(testOnLoadToOneDemandExpansionActual);
    CPPUNIT_TEST(testOnDemandExpansionActual);
    CPPUNIT_TEST(testCacheHitNotInModeledCache);
    CPPUNIT_TEST(testCacheHitInModeledCache);
    CPPUNIT_TEST(testCacheMissNotInModeledCache);
    CPPUNIT_TEST(testCacheMissInModeledCache);
    CPPUNIT_TEST(testNodeCacheEvictions);
    CPPUNIT_TEST_SUITE_END();

public:
    void testImplicitNoPageCache()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
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
        constexpr const char *testData = R"!!!(
        {
            "link": {
                "@kind": "index-events",
                "storage": {
                    "@cacheReadTime": 0,
                    "plane": {
                        "@name": "a",
                        "@readTime": 500
                    }
                }
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
        constexpr const char *testData = R"!!!(
link:
- kind: index-events
  storage:
    cacheReadTime: 100
    cacheCapacity: 8kib
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
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage cacheReadTime="100" cacheCapacity="16 KiB">
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
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" sizeFactor="2.0" sizeToTimeFactor="0.75"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="20000" ExpandTime="1000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="30000" ExpandTime="2000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="0" ExpandTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="20000" ExpandTime="1000" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="30000" ExpandTime="2000" ReadTime="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="0" ExpandTime="0" ReadTime="0"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="2000" FirstUse="true"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" FirstUse="true"/>
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
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" FirstUse="true"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" ExpandTime="0" FirstUse="true"/>
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
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" expansionMode="ll" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnLoadToOneDemandExpansionEstimation()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" expansionMode="ld" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="16384"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnDemandExpansionEstimation()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" sizeFactor="1.5" sizeToTimeFactor="0.25"/>
                        <node kind="1" expansionMode="dd" sizeFactor="4.0" sizeToTimeFactor="0.5"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="16384"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnLoadExpansionActual()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="1" expansionMode="ll"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="65536" ExpandTime="1000" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="16384"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnLoadToOneDemandExpansionActual()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="1" expansionMode="ld"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="1000" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="0"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768" ExpandTime="1000"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="32768"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="true" ExpandTime="1000"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8192"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testOnDemandExpansionActual()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="1" expansionMode="dd"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0" ReadTime="2000"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="14576"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="1"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0" ReadTime="500"/>
                    <event type="IndexPayload" FileId="1" FileOffset="0" FirstUse="false" ExpandTime="14576"/>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312" ExpandTime="0"/>
                    <event type="IndexEviction" FileId="1" FileOffset="0" NodeKind="1" InMemorySize="8312"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testCacheHitNotInModeledCache()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" cacheCapacity="1024000"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheHit" EventTimestamp="1" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" EventTimestamp="1" FileId="1" FileOffset="0" NodeKind="0"/>
                    <event type="IndexLoad" EventTimestamp="2" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500" ReadTime="500"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testCacheHitInModeledCache()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" cacheCapacity=" 1000 Kib "/>
                        <observed FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                </input>
                <expect>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testCacheMissNotInModeledCache()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" cacheCapacity="1024000"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500" ReadTime="500"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500" ReadTime="500"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testCacheMissInModeledCache()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" cacheCapacity=" 1024 kb"/>
                        <observed FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="0" NodeKind="0"/>
                    <event type="IndexLoad" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500" ReadTime="500"/>
                </input>
                <expect>
                    <event type="IndexCacheHit" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }

    void testNodeCacheEvictions()
    {
        constexpr const char *testData = R"!!!(
            <test>
                <link kind="index-events">
                    <storage>
                        <plane name="a" readTime="500"/>
                    </storage>
                    <memory>
                        <node kind="0" cacheCapacity="60kb "/>
                        <observed FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                        <observed FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="30000" ExpandTime="2500"/>
                    </memory>
                </link>
                <input>
                    <event type="IndexCacheHit" FileId="1" FileOffset="16384" NodeKind="0" InMemorySize="60000" ExpandTime="4000"/>
                </input>
                <expect>
                    <event type="IndexCacheMiss" FileId="1" FileOffset="16384" NodeKind="0"/>
                    <event type="IndexLoad" EventTimestamp="1" FileId="1" FileOffset="16384" NodeKind="0" InMemorySize="60000" ExpandTime="4000" ReadTime="500"/>
                    <event type="IndexEviction" EventTimestamp="2" FileId="1" FileOffset="0" NodeKind="0" InMemorySize="30000"/>
                    <event type="IndexEviction" EventTimestamp="3" FileId="1" FileOffset="8192" NodeKind="0" InMemorySize="30000"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexEventModelTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexEventModelTests, "indexeventmodel");

#endif
