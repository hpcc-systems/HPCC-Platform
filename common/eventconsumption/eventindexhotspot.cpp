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
        struct Activity
        {
            Buckets buckets;
            Range range{std::numeric_limits<bucket_type>::max(), 0};
        };
    public:
        CActivity(CHotspotEventVisitor& _container, __uint64 _id)
            : container(_container)
            , id(_id)
        {
        }

        void setPath(const char* _path)
        {
            this->path.set(_path);
        }

        void recordEvent(offset_t offset, BucketKind bucketKind)
        {
            bucket_type bucket = page2bucket(offset2page(offset, defaultPageBits), granularityBits());
            bucket_type key = bucket2activityKey(bucket);
            unsigned short index = bucket2activityIndex(bucket);
            activity[bucketKind].buckets[key][index]++;
            if (bucket < activity[bucketKind].range.first)
                activity[bucketKind].range.first = bucket;
            if (bucket > activity[bucketKind].range.second)
                activity[bucketKind].range.second = bucket;
        }

        void forEachBucket(IBucketVisitor& visitor)
        {
            Activity& leaf = activity[BucketLeaf];
            Activity& branch = activity[BucketBranch];
            visitor.arrive(id, path);
            bucket_type bucket = std::min(leaf.range.first, branch.range.first);
            if (bucket != std::numeric_limits<bucket_type>::max())
            {
                bool haveLeaves = !leaf.buckets.empty();
                bool haveBranches = !branch.buckets.empty();
                // Ambiguity exists when the greatest leaf bucket is the least branch bucket. It
                // can be ignored if the bucket visitor does not want ambiguous buckets.
                bool haveAmbiguity = visitor.wantAmbiguous() && haveLeaves && haveBranches && leaf.range.second == branch.range.first;
                if (haveAmbiguity)
                {
                    if (leaf.range.second > leaf.range.first)
                        leaf.range.second--;
                    else
                        haveLeaves = false;
                    if (branch.range.second > branch.range.first)
                        branch.range.first++;
                    else
                        haveBranches = false;
                }
                if (haveLeaves)
                {
                    while (bucket <= leaf.range.second)
                    {
                        stat_type events = queryBucket(leaf.buckets, bucket);
                        if (events)
                            visitor.visitBucket(bucket, BucketLeaf, events);
                        bucket++;
                    }
                }
                if (haveAmbiguity)
                    visitor.visitBucket(bucket, BucketAmbiguous, queryBucket(leaf.buckets, bucket) + queryBucket(branch.buckets, bucket));
                if (haveBranches)
                {
                    bucket = branch.range.first;
                    while (bucket <= branch.range.second)
                    {
                        stat_type events = queryBucket(branch.buckets, bucket);
                        if (events)
                            visitor.visitBucket(bucket, BucketBranch, events);
                        bucket++;
                    }
                }
            }
            visitor.depart();
        }

    protected:
        inline byte granularityBits() const
        {
            return container.granularityBits;
        }
        inline bucket_type bucket2activityKey(offset_t bucket)
        {
            return bucket >> bucketArrayBits;
        }

        inline unsigned short bucket2activityIndex(offset_t bucket)
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

    private:
        CHotspotEventVisitor& container; // back reference to obtain shared configuration
        __uint64 id{0};
        StringAttr path;
        Activity activity[BucketAmbiguous]; // incoming activity is never ambiguous, this storage is not ambiguous
    };

public: // IEventVisitor
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return true;
    }

    virtual bool visitEvent(CEvent& event) override
    {
        if (event.hasAttribute(EvAttrFileId))
        {
            EventType type = event.queryType();
            if (type != MetaFileInformation && type != observedEvent)
                return true;
            __uint64 fileId = event.queryNumericValue(EvAttrFileId);
            auto [it, inserted] = activity.try_emplace(fileId, *this, fileId);
            if (type == observedEvent)
                it->second.recordEvent(event.queryNumericValue(EvAttrFileOffset), event.queryNumericValue(EvAttrNodeKind)? BucketLeaf : BucketBranch);
            else
                it->second.setPath(event.queryTextValue(EvAttrPath));
        }
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
        analyzer->begin(observedEvent, granularityBits);
        for (auto& [fileId, activity] : activity)
            activity.forEachBucket(*analyzer);
        analyzer->end();
    }

public:
    CHotspotEventVisitor(IBucketVisitor& _analyzer, EventType _observedEvent, byte _granularityBits)
        : analyzer(&_analyzer)
        , observedEvent(_observedEvent)
        , granularityBits(_granularityBits)
    {
    }

private:
    // map file id (as __uint64 due to visitor interface) to index activity
    using Activity = std::map<__uint64, CActivity>;
    Linked<IBucketVisitor> analyzer;
    EventType observedEvent{EventNone};
    byte granularityBits{0};
    Activity activity;
};

bool CIndexHotspotOp::ready() const
{
    return CEventConsumingOp::ready() && observedEvent != EventNone;
}

bool CIndexHotspotOp::doOp()
{
    Owned<IBucketVisitor> analyzer;
    if (limit)
        analyzer.setown(createTopBucketVisitor(*out, limit));
    else
        analyzer.setown(createAllBucketVisitor(*out));

    CHotspotEventVisitor visitor(*analyzer, observedEvent, granularityBits);
    return traverseEvents(inputPath, visitor);
}

void CIndexHotspotOp::setObservedEvent(EventType _observedEvent)
{
    assertex(_observedEvent == EventIndexLookup || _observedEvent == EventIndexLoad);
    observedEvent = _observedEvent;
}

void CIndexHotspotOp::setGranularity(byte bits)
{
    granularityBits = bits;
}

void CIndexHotspotOp::setLimit(byte _limit)
{
    limit = _limit;
}
