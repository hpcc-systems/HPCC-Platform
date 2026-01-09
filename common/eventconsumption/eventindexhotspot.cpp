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

#include "eventindexhotspot.h"
#include "eventindexhotspot.hpp"
#include <array>
#include <map>
#include <set>

class CHotspotEventVisitor : public CInterfaceOf<IEventVisitor>
{
    // Manages the event data for a single file.
    class CActivity
    {
    private:
        // An array of bucket activity counters. The dimension is a pwoer of 2, simplifying the
        // conversion of a bucket number to an array index.
        using BucketSubset = std::array<stat_type, 1 << bucketArrayBits>;
        // Map of a bucket number, right shifted by `bucketArrayBits`, to a bucket subset.
        using Buckets = std::map<bucket_type, BucketSubset>;
        // The lowest and highest bucket number with observed activity.
        using Range = std::pair<bucket_type, bucket_type>;
        // The observed activity for a single bucket kind.
        class Activity
        {
        public:
            Buckets buckets;
            Range range{std::numeric_limits<bucket_type>::max(), 0};

            Activity() = default;

            bool hasActivity() const
            {
                return !buckets.empty();
            }

            void recordEvent(bucket_type bucket)
            {
                bucket_type key = bucket2activityKey(bucket);
                unsigned short index = bucket2activityIndex(bucket);
                buckets[key][index]++;
                if (bucket < range.first)
                    range.first = bucket;
                if (bucket > range.second)
                    range.second = bucket;
            }

            void forEachBucket(IBucketVisitor& visitor, NodeKind nodeKind)
            {
                if (!hasActivity())
                    return;
                for (bucket_type bucket = range.first; bucket <= range.second; bucket++)
                {
                    stat_type events = queryBucket(buckets, bucket);
                    if (events)
                        visitor.visitBucket(bucket, nodeKind, events);
                }
            }

        protected:
            bucket_type bucket2activityKey(offset_t bucket)
            {
                return bucket >> bucketArrayBits;
            }

            unsigned short bucket2activityIndex(offset_t bucket)
            {
                return bucket & bucketArrayMask;
            }

            stat_type queryBucket(const Buckets& buckets, bucket_type bucket)
            {
                Buckets::const_iterator it = buckets.find(bucket2activityKey(bucket));
                if (it != buckets.end())
                    return it->second[bucket2activityIndex(bucket)];
                return 0;
            }
        };
    public:
        CActivity(CMetaInfoState& _metaInfoState, __uint64 _id)
            : metaInfoState(_metaInfoState)
            , id(_id)
        {
        }

        void recordEvent(offset_t offset, NodeKind nodeKind, byte granularityBits)
        {
            activity[nodeKind].recordEvent(page2bucket(offset2page(offset, defaultPageBits), granularityBits));
        }

        void forEachBucket(IBucketVisitor& visitor)
        {
            if (!hasActivity()) // suppress inactive files
                return;
            const char* path = metaInfoState.queryFilePath(id);
            visitor.arrive(id, path);
            for (unsigned kind = 0; kind < NumNodeKinds; kind++)
                activity[kind].forEachBucket(visitor, NodeKind(kind));
            visitor.depart();
        }

    protected:
        bool hasActivity() const
        {
            for (unsigned kind = 0; kind < NumNodeKinds; kind++)
            {
                if (activity[kind].hasActivity())
                    return true;
            }
            return false;
        }

    private:
        CMetaInfoState& metaInfoState;
        __uint64 id{0};
        Activity activity[NumNodeKinds];
    };

public: // IEventVisitor
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        EventType type = event.queryType();
        if (MetaFileInformation == type)
            return true;
        if (observedEvents.find(type) == observedEvents.end())
            return true;
        __uint64 fileId = event.queryNumericValue(EvAttrFileId);
        NodeKind nodeKind = queryIndexNodeKind(event);
        auto [it, inserted] = activity.try_emplace(fileId, operation.queryMetaInfoState(), fileId);
        it->second.recordEvent(event.queryNumericValue(EvAttrFileOffset), nodeKind, granularityBits);
        return true;
    }

    virtual void departFile(uint32_t) override
    {
        // Intentionally empty.
    }

public:
    CHotspotEventVisitor(CIndexHotspotOp& _operation, const std::set<EventType>& _observedEvents, byte _granularityBits)
        : operation(_operation)
        , granularityBits(_granularityBits)
        , observedEvents(_observedEvents)
    {
    }

    void generateReport(IBucketVisitor& analyzer)
    {
        analyzer.begin(observedEvents, granularityBits);
        for (auto& [fileId, activity] : activity)
            activity.forEachBucket(analyzer);
        analyzer.end();
    }

private:
    CIndexHotspotOp& operation;
    // map file id (as __uint64 due to visitor interface) to index activity
    using Activity = std::map<__uint64, CActivity>;
    byte granularityBits{0};
    Activity activity;
    std::set<EventType> observedEvents;
};

bool CIndexHotspotOp::ready() const
{
    return CEventConsumingOp::ready() && !observedEvents.empty();
}

bool CIndexHotspotOp::doOp()
{
    Owned<CHotspotEventVisitor> visitor = new CHotspotEventVisitor(*this, observedEvents, granularityBits);
    if (traverseEvents(inputPath, *visitor))
    {
        Owned<IBucketVisitor> analyzer;
        if (limit)
            analyzer.setown(createTopBucketVisitor(*out, limit));
        else
            analyzer.setown(createAllBucketVisitor(*out));
        visitor->generateReport(*analyzer);
        return true;
    }
    return false;
}

void CIndexHotspotOp::addObservedEvent(EventType observedEvent)
{
    assertex(observedEvent == EventIndexCacheHit || observedEvent == EventIndexCacheMiss || observedEvent == EventIndexLoad);
    observedEvents.insert(observedEvent);
}

void CIndexHotspotOp::setGranularity(byte bits)
{
    granularityBits = bits;
}

void CIndexHotspotOp::setLimit(byte _limit)
{
    limit = _limit;
}
