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
#include "eventindex.hpp"
#include <array>
#include <map>

// Report all visited buckets, in order, with counts and bucket kinds. At most one bucket may be
// ambiguous with respect to bucket kind, as only one bucket may include the final leaf and initial
// branch pages.
class CAllBucketVisitor : public CStreamingBucketVisitor
{
public:
    virtual void visitBucket(bucket_type bucket, BucketKind bucketKind, stat_type stat) override
    {
        stats[bucketKind].recordEvent(stat);
        StringBuffer lines;
        if (firstLeaf && BucketLeaf == bucketKind)
        {
            lines.append("  leaf:\n");
            firstLeaf = false;
        }
        else if (firstBranch && BucketBranch == bucketKind)
        {
            lines.append("  branch:\n");
            firstBranch = false;
        }
        lines.append("  - first-node: ").append(bucket2page(bucket, currentGranularity)).append('\n');
        lines.append("    events: ").append(stat).append('\n');
        out->put(lines.length(), lines.str());
    }

    virtual void depart() override
    {
        StringBuffer lines;
        stats[BucketLeaf].toString(lines, 2, "leafStats");
        stats[BucketAmbiguous].toString(lines, 2, "ambiguousStats");
        stats[BucketBranch].toString(lines, 2, "branchStats");
        out->put(lines.length(), lines.str());
        firstLeaf = firstBranch = true;
    }

    virtual bool wantAmbiguous() const override
    {
        return false;
    }

protected:
    virtual void appendAnalysis(StringBuffer& lines) const override
    {
        lines.append("all buckets");
    }

public:
    using CStreamingBucketVisitor::CStreamingBucketVisitor;

private:
    bool firstLeaf{true};
    bool firstBranch{true};
};

// Report up to the N buckets of each bucket kind with the highest activity counts. Buckets with
// the same activity count as the Nth bucket are reported cumulatively, with the number of buckets
// in place of individual bucket numbers.
class CTopBucketVisitor : public CStreamingBucketVisitor
{
protected:
    using Hit = std::pair<stat_type, bucket_type>;
    using Hits = std::vector<Hit>;
    struct Info
    {
        Hits hits;
        stat_type firstDrop;
        __uint64 droppedCount;

        Info() { clear(); }
        bool empty() const { return hits.empty(); }

        void clear()
        {
            hits.clear();
            firstDrop = 0;
            droppedCount = 0;
        }
    };

public:
    virtual void visitBucket(bucket_type bucket, BucketKind bucketKind, stat_type stat) override
    {
        Info& info = (BucketLeaf == bucketKind ? leaves : branches);
        stats[bucketKind].recordEvent(stat);
        if (info.hits.size() == limit) // have top N; drop the lowest
        {
            stat_type last = info.hits.back().first;
            if (stat < last) // ignore values lower than the lowest
                return;
            if (stat == last) // drop a value equal to the lowest
            {
                if (info.firstDrop != stat)
                    info.firstDrop = stat;
                info.droppedCount++;
                return;
            }
            info.hits.pop_back(); // drop the last value lower than the new one

            if (info.hits.back().first == last)
            {
                if (last > info.firstDrop) // dropped the first occurence of the new lowest value
                {
                    info.firstDrop = last;
                    info.droppedCount = 1;
                }
                else // dropped another occurrence of the lowest value
                    info.droppedCount++;
            }
            else // no occurrences of the lowest value have been dropped yet
            {
                info.firstDrop = 0;
                info.droppedCount = 0;
            }
        }
        // insert a new top N value
        Hit v(stat, bucket);
        Hits::const_iterator it = std::lower_bound(info.hits.begin(), info.hits.end(), v, [](const Hit& a, const Hit& b) {
            return a.first > b.first;
        });
        info.hits.insert(it, v);
    }

    virtual void depart() override
    {
        summarize(leaves, stats[BucketLeaf], "leaves");
        summarize(branches, stats[BucketBranch], "branches");
        CStreamingBucketVisitor::depart();
    }

    virtual bool wantAmbiguous() const override
    {
        // Output clearly differentiates between leaf and branch buckets. Ambiguity is not needed.
        return false;
    }

protected:
    virtual void appendAnalysis(StringBuffer& lines) const override
    {
        lines.append("top ").append(int(limit)).append(" buckets");
    }

public:
    CTopBucketVisitor(IBufferedSerialOutputStream& _out, byte _limit)
        : CStreamingBucketVisitor(_out)
        , limit(_limit)
    {
    }

protected:
    void summarize(Info& info, Stats& stats, const char* label)
    {
        if (info.empty())
            return;
        StringBuffer lines;
        lines.append("  ").append(label).append(":\n");
        stats.toString(lines, 4);
        lines.append("    bucket:\n");
        for (const Hit& hit : info.hits)
        {
            lines.append("    - first-node: ").append(bucket2page(hit.second, currentGranularity)).append('\n');
            lines.append("      events: ").append(hit.first).append('\n');
        }
        if (info.droppedCount)
        {
            lines.append("    - dropped: ").append(info.droppedCount).append('\n');
            lines.append("      events: ").append(info.firstDrop).append('\n');
        }
        out->put(lines.length(), lines.str());
        info.clear();
    }

private:
    Info leaves;
    Info branches;
    byte limit;
};

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

public: // IEventAttributeVisitor
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
        analyzer.setown(new CTopBucketVisitor(*out, limit));
    else
        analyzer.setown(new CAllBucketVisitor(*out));

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
