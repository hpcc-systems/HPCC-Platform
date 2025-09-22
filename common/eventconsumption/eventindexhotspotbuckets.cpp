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

#include "eventindexhotspot.hpp"
#include "jstring.hpp"

// Abstract base implementation of `IBucketVisitor` that outputs bucket activity to a buffered
// stream as YAML-formatted text. Each subclass performs a specific analysis of the bucket
// activity data.
class CStreamingBucketVisitor : public CInterfaceOf<IBucketVisitor>
{
protected: // Abstract interface
    // Append a brief description of the analysis performed to the buffer.
    virtual void appendAnalysis(StringBuffer& lines) const = 0;

public: // IBucketVisitor
    virtual void begin(const std::set<EventType>& observedEvents, unsigned granularity) override
    {
        StringBuffer eventNames;
        for (EventType type : observedEvents)
        {
            if (eventNames.length())
                eventNames.append(", ");
            eventNames.append(queryEventName(type));
        }
        currentGranularity = granularity;
        StringBuffer lines;
        lines.append("analysis: ");
        appendAnalysis(lines);
        lines.append('\n');
        lines.append("event: ").append(eventNames).append('\n');
        lines.append("granularity: ").append(granularity).append('\n');
        out->put(lines.length(), lines.str());
    }

    virtual void arrive(unsigned id, const char* path) override
    {
        StringBuffer lines;
        if (!arrived)
        {
            lines.append("file:\n");
            arrived = true;
        }
        lines.append("- id: ").append(id).append('\n');
        lines.append("  path: ").append(isEmptyString(path) ? "<not available>" : path).append('\n');
        out->put(lines.length(), lines.str());
    }

    virtual void depart() override
    {
        for (byte bucketKind = BucketBranch; bucketKind < BucketKindMax; bucketKind++)
            stats[bucketKind].clear();
    }

    virtual void end() override
    {
    }

public:
    CStreamingBucketVisitor(IBufferedSerialOutputStream& _out)
        : out(&_out)
    {
    }

protected:
    struct Stats
    {
        stat_type minEventsPerBucket;
        stat_type maxEventsPerBucket;
        __uint64 totalEvents;
        bucket_type totalBuckets;

        Stats()
        {
            clear();
        }

        void recordEvent(stat_type value)
        {
            if (value < minEventsPerBucket)
                minEventsPerBucket = value;
            if (value > maxEventsPerBucket)
                maxEventsPerBucket = value;
            totalEvents += value;
            totalBuckets++;
        }

        void clear()
        {
            minEventsPerBucket = std::numeric_limits<stat_type>::max();
            maxEventsPerBucket = 0;
            totalEvents = 0;
            totalBuckets = 0;
        }

        StringBuffer& toString(StringBuffer& lines, byte indent, const char* label = nullptr) const
        {
            if (totalEvents)
            {
                if (isEmptyString(label))
                    label = "stats";
                lines.pad(indent).append(label).append(": \n");
                indent += 2;
                lines.pad(indent).append("buckets: ").append(totalBuckets).append('\n');
                lines.pad(indent).append("total: ").append(totalEvents).append('\n');
                if (totalBuckets > 1)
                {
                    lines.pad(indent).append("min: ").append(minEventsPerBucket).append('\n');
                    lines.pad(indent).append("avg: ").append(totalEvents / double(totalBuckets)).append('\n');
                    lines.pad(indent).append("max: ").append(maxEventsPerBucket).append('\n');
                }
            }
            return lines;
        }
   };
    Linked<IBufferedSerialOutputStream> out;
    Stats stats[BucketKindMax]; // keep statistics for each bucket kind
    unsigned currentGranularity{0};
    bool arrived{false};
};

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

IBucketVisitor* createAllBucketVisitor(IBufferedSerialOutputStream& out)
{
    return new CAllBucketVisitor(out);
}

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

IBucketVisitor* createTopBucketVisitor(IBufferedSerialOutputStream& out, byte limit)
{
    return new CTopBucketVisitor(out, limit);
}
