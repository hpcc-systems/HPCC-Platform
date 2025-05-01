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

#include "evtindex.hpp"
#include "jevent.hpp"
#include <map>

#define defaultBucketCount 10
constexpr static byte bucketArrayBits = 10; // 1K buckets per activity key
constexpr static unsigned short bucketArrayMask = ((1 << bucketArrayBits) - 1);

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

#include <iostream>
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

public: // IEventVisitor
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return true;
    }

    virtual Continuation visitEvent(EventType type) override
    {
        auto prepCache = [&](EventType type) {
            currentEvent = type;
            currentFileId = 0;
            currentPath = 0;
            currentOffset = std::numeric_limits<offset_t>::max();
            currentBucketKind = BucketAmbiguous;
        };

        if (!(MetaFileInformation == type || observedEvent == type))
            return Continuation::visitSkipEvent;
        prepCache(type);
        return Continuation::visitContinue;
    }

    virtual Continuation visitAttribute(EventAttr id, const char * value) override
    {
        switch (id)
        {
        case EvAttrPath:
            currentPath.set(value);
            break;
        default:
            break;
        }
        return visitContinue;
    }

    virtual Continuation visitAttribute(EventAttr id, bool value) override
    {
        return visitContinue;
    }

    virtual Continuation visitAttribute(EventAttr id, __uint64 value) override
    {
        switch (id)
        {
        case EvAttrFileId:
            currentFileId = value;
            break;
        case EvAttrFileOffset:
            currentOffset = value;
            break;
        case EvAttrNodeKind:
            currentBucketKind = (value ? BucketLeaf : BucketBranch);
            break;
        default:
            break;
        }
        return visitContinue;
    }

    virtual bool departEvent() override
    {
        if (!currentFileId)
            return true;
        auto [it, inserted] = activity.try_emplace(currentFileId, *this, currentFileId);
        if (MetaFileInformation == currentEvent)
            it->second.setPath(currentPath);
        else if (observedEvent == currentEvent)
            it->second.recordEvent(currentOffset, currentBucketKind);
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
    EventType currentEvent{EventNone}; // last accepted event type
    __uint64 currentFileId{0}; // last observed index file id
    StringAttr currentPath; // last observed index file path
    __uint64 currentOffset{0}; // last observed index file offset
    BucketKind currentBucketKind{BucketAmbiguous}; // last observed index node kind
};

class CIndexHotspotOp : public CStreamingEventFileOp
{
public:
    virtual bool ready() const override
    {
        return CStreamingEventFileOp::ready() && observedEvent != EventNone;
    }
    virtual int doOp() override
    {
        Owned<IBucketVisitor> analyzer;
        if (limit)
            analyzer.setown(new CTopBucketVisitor(*out, limit));
        else
            analyzer.setown(new CAllBucketVisitor(*out));

        CHotspotEventVisitor visitor(*analyzer, observedEvent, granularityBits);
        return (readEvents(inputPath, visitor) ? 0 : 1);
    }

public:
    void setObservedEvent(EventType _observedEvent)
    {
        assertex(_observedEvent == EventIndexLookup || _observedEvent == EventIndexLoad);
        observedEvent = _observedEvent;
    }

    void setGranularity(byte bits)
    {
        granularityBits = bits;
    }

    void setLimit(byte _limit)
    {
        limit = _limit;
    }

protected:
    EventType observedEvent{EventNone};
    byte granularityBits{defaultGranularityBits};
    byte limit{10};
};

class CIndexHotspotCommand : public CEvToolCommand
{
public:
    virtual bool acceptParameter(const char *arg) override
    {
        iho.setInputPath(arg);
        return true;
    }
    virtual bool acceptVerboseOption(const char* opt) override
    {
        bool accepted = CEvToolCommand::acceptVerboseOption(opt);
        if (!accepted)
        {
            if (streq(opt, "lookup"))
            {
                iho.setObservedEvent(EventIndexLookup);
                accepted = true;
            }
            else if (streq(opt, "load"))
            {
                iho.setObservedEvent(EventIndexLoad);
                accepted = true;
            }
            else
            {
                // All other accepted options require a value to be provided.
                //     name '=' value
                const char* valueDelim = strchr(opt, '=');
                if (!valueDelim || !valueDelim[1])
                    return false;
                __uint64 value = 0;
                if (!strncmp(opt, "granularity", valueDelim - opt))
                {
                    if (extractVerboseValue(valueDelim + 1, value, 0, 10))
                    {
                        iho.setGranularity(byte(value));
                        accepted = true;
                    }
                }
                if (!strncmp(opt, "top", valueDelim - opt))
                {
                    if (extractVerboseValue(valueDelim + 1, value, 0, 100))
                    {
                        iho.setLimit(value);
                        accepted = true;
                    }
                }
            }
        }
        return accepted;
    }
    virtual bool isGoodRequest() override
    {
        return iho.ready();
    }
    virtual int doRequest() override
    {
        return iho.doOp();
    }

    virtual void usage(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        static const char* usageFmtStr = R"!!!(<event> [options] <filename>

Identify activity hotspots for each index file referenced within a recorded
event file. The activity to be analyzed is specified by a obligatory event
selector correlating to a single event type. Additional options determine the
analysis to be performed.

Events:
    --load                 Analyze index load events.
    --lookup               Analyze index lookup events.

Options:
    -?, -h, --help         Show this help message and exit.
    --granularity=[0..10]  Set the analysis resolution, where the resolution
                           is 2^granularity pages per bucket; default is 0, or
                           one page per bucket.
    --top=[0..100]         The maximum number of leaf and branch buckets
                           reported per file, or 0 to report all buckets;
                           default is 10.

Parameters:
    <filename>             Path to a binary event data file to be analyzed.

Activity is reported in terms of buckets. Buckets are groups of one or more
consecutive pages of an index file, where each page is a block of 8KB that
starts at an event reported ofsset. The default bucket size is one page and
provides the most detailed analysis, but may require too much memory for
large datasets and/or large index files. Consider increasing the bucket size
using the --granularity option to reduce memory consumption.

Buckets with no activity are excluded from analysis. The total number of
buckets per index file is unknown, as are the numbers of pages representing
leaf and branch index nodes. Without these numbers, the potential number of
inactive buckets for each node kind cannot be determined. Inclusion of inactive
buckets would skew the results.

Finding the Most Active Buckets:
---------------------------------

The default analysis reports the most active leaf and branch buckets for each
index file. The number of buckets reported, for each node kind, is controlled
by the --top option. The default is 10 buckets.

For each node kind of each index file, up to the N buckets with the greatest
event count are reported in descending order. In the event the first buckets
omitted from the report have the same count as the last bucket to be reported,
the quantity of buckets with that count is reported as the "dropped" bucket
value.

Statistics for each active bucket in each index file are also included. These
include the lowest event count, the average event count per active bucket, and
the highest event count.

Example:
> evtool index hotspot --load generated.evt --granularity=10 --top=2
analysis: top 2 buckets
event: IndexLoad
granularity: 10
file:
- id: 2147483651
  path: /home/tim/hpcc-systems/HPCC-Platform/../community/var/lib/HPCCSystems/hpcc-data/roxie/regress/single/searchindex._1_of_1
  leaves:
    stats:
      buckets: 5
      total: 36
      min: 1
      avg: 7.2
      max: 26
    bucket:
    - first-node: 4096
      events: 26
    - first-node: 2048
      events: 4
    - dropped: 1
      events: 4
  branches:
    stats:
      buckets: 1
      total: 11
    bucket:
    - first-node: 6144
      events: 11

Identifying All Activity
------------------------

Setting --top to zero reports all active buckets, in order of bucket position,
for each index file. The index node kind contained by the bucket is reported
with each bucket, as one of 'L' (leaf), 'B' (branch), or 'L/B' (ambiguous, or
both leaf and branch). At most one bucket may be ambiguous, as only one bucket
may include the final leaf and initial branch pages.

Statistics for each active bucket in each index file are also included. These
include the lowest event count, the average event count per active bucket, and
the highest event count.

THe "//file/bucket" array may be used to generate a heat map of index activity.
Absent a total bucket count for each index file, the final dimension of a heat
map must be determined separately. Absent a total leaf bucket count, a heat map
cannot reliably differentiate leaf and branch inactivity, if such behavior is
desired.

Example:
> evtool index hotspot --load generated.evt --granularity=10 --top=0
analysis: all buckets
event: IndexLoad
granularity: 10
file:
- id: 2147483651
  path: /home/tim/hpcc-systems/HPCC-Platform/../community/var/lib/HPCCSystems/hpcc-data/roxie/regress/single/searchindex._1_of_1
  leaf:
  - first-node: 0
    events: 4
  - first-node: 2048
    events: 4
  - first-node: 3072
    events: 1
  - first-node: 4096
    events: 26
  - first-node: 5120
    events: 1
  branch:
  - first-node: 6144
    events: 11
  leafStats:
    buckets: 5
    total: 36
    min: 1
    avg: 7.2
    max: 26
  branchStats:
    buckets: 1
    total: 11
)!!!";
        static size32_t usageFmtStrLength = size32_t(strlen(usageFmtStr));
        usagePrefix(argc, argv, pos, out);
        out.put(usageFmtStrLength, usageFmtStr);
    }

public:
    CIndexHotspotCommand()
    {
        iho.setOutput(consoleOut());
    }

protected:
    bool extractVerboseValue(const char* buffer, __uint64& value, __uint64 min, __uint64 max)
    {
        StringBuffer tmp(buffer);
        char* endptr = nullptr;
        value = strtoull(tmp.trim(), &endptr, 10);
        if ((endptr && *endptr) || (endptr == tmp.str()) || (value < min || value > max))
            return false;
        return true;
    }

protected:
    CIndexHotspotOp iho;
};

IEvToolCommand* createIndexHotspotCommand()
{
    return new CIndexHotspotCommand;
}
