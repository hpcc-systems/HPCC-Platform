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

#include "eventindexsummarize.h"

class CSummaryCollector : public CInterfaceOf<IEventVisitor>
{
public:
    virtual void summarize() = 0;
public: // IEventVisitor
    virtual bool visitFile(const char *filename, uint32_t version) override
    {
        return true;
    }

    virtual void departFile(uint32_t bytesRead) override
    {
    }
public:
    CSummaryCollector(CIndexFileSummary& _operation, IndexSummarization _summarization, IBufferedSerialOutputStream* _out)
        : operation(_operation)
        , summarization(_summarization)
        , out(_out)
    {
    }

protected:
    // Common data structures moved from CNodeCollector to avoid replication
    struct Bucket
    {
        __uint64 total{0};
        uint32_t count{0};
        uint32_t min{UINT32_MAX};
        uint32_t max{0};
    };
    struct Events
    {
        uint32_t hits{0};
        uint32_t misses{0};
        uint32_t loads{0};
        uint32_t payloads{0};
        uint32_t evictions{0};
    };
    enum ReadBucket
    {
        Total,
        PageCache,
        LocalFile,
        RemoteFile,
        NumBuckets
    };
    // ReadBucket categorizes read durations: Total is a special cumulative bucket that tracks
    // aggregate metrics across all read operations, while PageCache, LocalFile, and RemoteFile
    // represent mutually exclusive categories selected by time/source thresholds. The bucket
    // boundaries for Total and RemoteFile are both intentionally UINT64_MAX. Total because every
    // read counts toward the total time; RemoteFile because anything not read from the page cache
    // or local storage must belong to the remaining catch-all bucket.
    static constexpr __uint64 readBucketBoundary[NumBuckets] = {UINT64_MAX, 20'000, 400'000, UINT64_MAX};

    template <typename single_bucket_t, typename cumulative_bucket_t>
    static void accumulateBucket(const single_bucket_t& single, cumulative_bucket_t& cumulative)
    {
        if (single.count)
        {
            cumulative.count += single.count;
            cumulative.total += single.total;
            if (single.min < cumulative.min)
                cumulative.min = single.min;
            if (single.max > cumulative.max)
                cumulative.max = single.max;
            assertex(cumulative.min <= cumulative.max);
        }
    }

    template <typename single_value_t, typename cumulative_bucket_t>
    static void accumulateValue(const single_value_t& value, cumulative_bucket_t& cumulative)
    {
        if (value)
        {
            cumulative.count++;
            cumulative.total += value;
            if (value < cumulative.min)
                cumulative.min = value;
            if (value > cumulative.max)
                cumulative.max = value;
            assertex(cumulative.min <= cumulative.max);
        }
    }

    // Identify the mutually exclusive read time category to which a given time belongs.
    // The result will never be Total, which is not mutually exclusive.
    static ReadBucket chooseBucketCategory(__uint64 time)
    {
        if (time <= readBucketBoundary[PageCache])
            return PageCache;
        if (time <= readBucketBoundary[LocalFile])
            return LocalFile;
        return RemoteFile;
    }

    void appendCSVColumn(StringBuffer& line, const char* value)
    {
        if (!line.isEmpty())
            line.append(',');
        if (!isEmptyString(value))
            encodeCSVColumn(line, value);
    }

    void appendCSVColumn(StringBuffer& line, __uint64 value)
    {
        if (!line.isEmpty())
            line.append(',');
        line.append(value);
    }

    template<typename... Args>
    void appendCSVColumns(StringBuffer& line, Args&&... values)
    {
        // Use fold expression to append each value in sequence
        // This allows calling appendCSVColumns(line, value1, value2, value3, ...)
        // instead of multiple individual appendCSVColumn calls
        (appendCSVColumn(line, std::forward<Args>(values)), ...);
    }

    template<typename bucket_type_t>
    void appendCSVBucket(StringBuffer& line, const bucket_type_t& bucket, bool includeCount)
    {
        if (includeCount)
            appendCSVColumn(line, bucket.count);
        if (bucket.count)
            appendCSVColumns(line, bucket.total, bucket.total / bucket.count, bucket.min, bucket.max);
        else
            appendCSVColumns(line, "", "", "", "");
    }

    void appendCSVBucketHeaders(StringBuffer& line, const char* prefix, bool includeCount)
    {
        VStringBuffer header("%s ", prefix);
        size32_t prefixLength = header.length();

        if (includeCount)
            appendCSVColumn(line, header.append("Count"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Total"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Average"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Min"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Max"));
    }

    template<typename events_type_t>
    void appendCSVEvents(StringBuffer& line, const events_type_t& events)
    {
        appendCSVColumns(line, events.hits, events.misses, events.loads, events.payloads, events.evictions);
    }

    void appendCSVEventsHeaders(StringBuffer &line, const char *prefix)
    {
        StringBuffer header;
        if (!isEmptyString(prefix))
            header.append(prefix).append(" ");
        size32_t prefixLength = header.length();

        appendCSVColumn(line, header.append("Hits"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Misses"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Loads"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Payloads"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Evictions"));
    }

    void outputLine(StringBuffer &line)
    {
        line.append('\n');
        out->put(line.length(), line.str());
        line.clear();
    }
protected:
    CIndexFileSummary& operation;
    IndexSummarization summarization;
    Linked<IBufferedSerialOutputStream> out;
};

// Explicit instantiation for commonly used types to avoid linker issues
template void CSummaryCollector::appendCSVColumns<const char*>(StringBuffer& line, const char*&& value);
template void CSummaryCollector::appendCSVColumns<__uint64>(StringBuffer& line, __uint64&& value);
template void CSummaryCollector::appendCSVColumns<const char*, __uint64>(StringBuffer& line, const char*&& value1, __uint64&& value2);
template void CSummaryCollector::appendCSVColumns<__uint64, const char*>(StringBuffer& line, __uint64&& value1, const char*&& value2);
template void CSummaryCollector::appendCSVColumns<__uint64, __uint64>(StringBuffer& line, __uint64&& value1, __uint64&& value2);
template void CSummaryCollector::appendCSVColumns<const char*, const char*>(StringBuffer& line, const char*&& value1, const char*&& value2);

class CNodeCollector : public CSummaryCollector
{
protected:
    struct NodeStats
    {
        uint32_t inMemorySize{0};
        Events events;
        Bucket readTime[NumBuckets];
        Bucket expandTime;
        __uint64 elapsedTime{0};
    };
public: // IEventVisitor
    virtual bool visitEvent(CEvent& event) override
    {
        // Implicit event filter applied unconditionally
        EventType type = event.queryType();
        switch (type)
        {
        case EventIndexCacheHit:
        case EventIndexCacheMiss:
        case EventIndexLoad:
        case EventIndexPayload:
        case EventIndexEviction:
            break;
        default:
            return true;
        }

        __uint64 fileId = event.queryNumericValue(EvAttrFileId);
        IndexHashKey key(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset));
        __uint64 nodeKind = queryIndexNodeKind(event);
        NodeStats& nodeStats = stats[nodeKind][key];
        __uint64 tmp;
        switch (type)
        {
        case EventIndexCacheHit:
            tmp = event.queryNumericValue(EvAttrInMemorySize);
            if (tmp)
                nodeStats.inMemorySize = tmp;
            nodeStats.events.hits++;
            break;
        case EventIndexCacheMiss:
            nodeStats.events.misses++;
            break;
        case EventIndexLoad:
            tmp = event.queryNumericValue(EvAttrInMemorySize);
            if (tmp)
                nodeStats.inMemorySize = tmp;
            tmp = event.queryNumericValue(EvAttrReadTime);
            if (tmp)
            {
                accumulateValue(tmp, nodeStats.readTime[Total]);
                accumulateValue(tmp, nodeStats.readTime[chooseBucketCategory(tmp)]);
                nodeStats.elapsedTime += tmp;
                tmp = event.queryNumericValue(EvAttrExpandTime);
                if (tmp)
                {
                    accumulateValue(tmp, nodeStats.expandTime);
                    nodeStats.elapsedTime += tmp;
                }
            }
            nodeStats.events.loads++;
            break;
        case EventIndexEviction:
            tmp = event.queryNumericValue(EvAttrInMemorySize);
            if (tmp)
                nodeStats.inMemorySize = tmp;
            nodeStats.events.evictions++;
            break;
        case EventIndexPayload:
            tmp = event.queryNumericValue(EvAttrExpandTime);
            if (tmp && event.queryBooleanValue(EvAttrFirstUse))
            {
                accumulateValue(tmp, nodeStats.expandTime);
                nodeStats.elapsedTime += tmp;
            }
            nodeStats.events.payloads++;
            break;
        default:
            break;
        }
        return true;
    }

public: // CSummaryCollector
    using CSummaryCollector::CSummaryCollector;

    virtual void summarize() override
    {
        switch (summarization)
        {
        case IndexSummarization::byFile:
            summarizeByFile();
            break;
        case IndexSummarization::byNodeKind:
            summarizeByNodeKind();
            break;
        case IndexSummarization::byNode:
            summarizeByNode();
            break;
        default:
            break;
        }
    }

protected:
    void summarizeByFile()
    {
        struct NodeKindStats
        {
            struct Bucket
            {
                __uint64 count{0};
                __uint64 total{0};
                __uint64 min{UINT64_MAX};
                __uint64 max{0};
            };
            struct Events
            {
                __uint64 hits{0};
                __uint64 misses{0};
                __uint64 loads{0};
                __uint64 payloads{0};
                __uint64 evictions{0};
            };
            Bucket inMemorySize;
            Events events;
            Bucket readTime[NumBuckets];
            Bucket expandTime;
            Bucket elapsedTime;
        };
        struct FileStats
        {
            NodeKindStats kinds[NumNodeKinds];
        };
        using SummaryStats = std::map<__uint64, FileStats>;
        SummaryStats summary;
        bool haveNodeKindEntries[NumNodeKinds] = {false,};
        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            for (Cache::value_type& entry : stats[nodeKind])
            {
                haveNodeKindEntries[nodeKind] = true;
                NodeKindStats& nodeKindStats = summary[entry.first.fileId].kinds[nodeKind];
                NodeStats& nodeStats = entry.second;
                accumulateValue(nodeStats.inMemorySize, nodeKindStats.inMemorySize);
                nodeKindStats.events.hits += nodeStats.events.hits;
                nodeKindStats.events.misses += nodeStats.events.misses;
                nodeKindStats.events.loads += nodeStats.events.loads;
                nodeKindStats.events.payloads += nodeStats.events.payloads;
                nodeKindStats.events.evictions += nodeStats.events.evictions;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                    accumulateBucket(nodeStats.readTime[bucket], nodeKindStats.readTime[bucket]);
                accumulateBucket(nodeStats.expandTime, nodeKindStats.expandTime);
                accumulateValue(nodeStats.elapsedTime, nodeKindStats.elapsedTime);
            }
        }

        StringBuffer line;
        appendCSVColumns(line, "File Id", "File Path");
        if (haveNodeKindEntries[BranchNode])
        {
            appendCSVBucketHeaders(line, "Branch In Memory", true);
            appendCSVEventsHeaders(line, "Branch");
            appendCSVBucketHeaders(line, "Branch Read", true);
            appendCSVBucketHeaders(line, "Branch Page Cache Read", true);
            appendCSVBucketHeaders(line, "Branch Local Read", true);
            appendCSVBucketHeaders(line, "Branch Remote Read", true);
            appendCSVColumn(line, "Contentious Branch Reads");
            appendCSVBucketHeaders(line, "Branch Expansion", true);
            appendCSVBucketHeaders(line, "Branch Elapsed Time", false);
        }
        if (haveNodeKindEntries[LeafNode])
        {
            appendCSVBucketHeaders(line, "Leaf In Memory", true);
            appendCSVEventsHeaders(line, "Leaf");
            appendCSVBucketHeaders(line, "Leaf Read", true);
            appendCSVBucketHeaders(line, "Leaf Page Cache Read", true);
            appendCSVBucketHeaders(line, "Leaf Local Read", true);
            appendCSVBucketHeaders(line, "Leaf Remote Read", true);
            appendCSVColumn(line, "Contentious Leaf Reads");
            appendCSVBucketHeaders(line, "Leaf Expansion", true);
            appendCSVBucketHeaders(line, "Leaf Elapsed Time", false);
        }
        if (haveNodeKindEntries[BlobNode])
        {
            appendCSVBucketHeaders(line, "Blob In Memory", true);
            appendCSVEventsHeaders(line, "Blob");
            appendCSVBucketHeaders(line, "Blob Read", true);
            appendCSVBucketHeaders(line, "Blob Page Cache Read", true);
            appendCSVBucketHeaders(line, "Blob Local Read", true);
            appendCSVBucketHeaders(line, "Blob Remote Read", true);
            appendCSVColumn(line, "Contentious Blob Reads");
            appendCSVBucketHeaders(line, "Blob Expansion", true);
            appendCSVBucketHeaders(line, "Blob Elapsed Time", false);
        }
        outputLine(line);

        for (SummaryStats::value_type& e : summary)
        {
            const char* filePath = operation.queryMetaInfoState().queryFilePath(e.first);
            appendCSVColumns(line, e.first, filePath);
            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
            {
                if (!haveNodeKindEntries[nodeKind])
                    continue;
                NodeKindStats& nodeStats = e.second.kinds[nodeKind];
                appendCSVBucket(line, nodeStats.inMemorySize, true);
                appendCSVEvents(line, nodeStats.events);
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                    appendCSVBucket(line, nodeStats.readTime[bucket], true);
                appendCSVColumn(line, nodeStats.events.loads - nodeStats.readTime[Total].count);
                appendCSVBucket(line, nodeStats.expandTime, true);
                appendCSVBucket(line, nodeStats.elapsedTime, false);
            }
            outputLine(line);
        }
    }

    void summarizeByNodeKind()
    {
        struct Bucket
        {
            __uint64 count{0};
            __uint64 total{0};
            uint32_t min{UINT32_MAX};
            uint32_t max{0};
        };
        struct Events
        {
            __uint64 hits{0};
            __uint64 misses{0};
            __uint64 loads{0};
            __uint64 payloads{0};
            __uint64 evictions{0};
        };

        StringBuffer line;
        appendCSVColumn(line, "Node Kind");
        appendCSVBucketHeaders(line, "In Memory", true);
        appendCSVEventsHeaders(line, nullptr);
        appendCSVBucketHeaders(line, "Read Time", true);
        appendCSVBucketHeaders(line, "Page Cache Read Time", true);
        appendCSVBucketHeaders(line, "Local Read Time", true);
        appendCSVBucketHeaders(line, "Remote Read Time", true);
        appendCSVColumn(line, "Contentious Reads");
        appendCSVBucketHeaders(line, "Expand Time", true);
        appendCSVBucketHeaders(line, "Elapsed Time", false);
        outputLine(line);

        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            if (stats[nodeKind].empty())
                continue;

            Bucket inMemorySize;
            Events events;
            Bucket readTime[NumBuckets];
            Bucket expandTime;
            Bucket elapsedTime;

            for (Cache::value_type& entry : stats[nodeKind])
            {
                NodeStats& nodeStats = entry.second;
                accumulateValue(nodeStats.inMemorySize, inMemorySize);
                events.hits += nodeStats.events.hits;
                events.misses += nodeStats.events.misses;
                events.loads += nodeStats.events.loads;
                events.payloads += nodeStats.events.payloads;
                events.evictions += nodeStats.events.evictions;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                    accumulateBucket(nodeStats.readTime[bucket], readTime[bucket]);
                accumulateBucket(nodeStats.expandTime, expandTime);
                accumulateValue(nodeStats.elapsedTime, elapsedTime);
            }

            appendCSVColumn(line, mapNodeKind((NodeKind)nodeKind));
            appendCSVBucket(line, inMemorySize, true);
            appendCSVEvents(line, events);
            for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                appendCSVBucket(line, readTime[bucket], true);
            appendCSVColumn(line, events.loads - readTime[Total].count);
            appendCSVBucket(line, expandTime, true);
            appendCSVBucket(line, elapsedTime, false);
            outputLine(line);
        }
    }

    void summarizeByNode()
    {
        StringBuffer line;
        appendCSVColumns(line, "File Id", "File Path", "File Offset", "Node Kind", "In Memory Size");
        appendCSVEventsHeaders(line, nullptr);
        appendCSVBucketHeaders(line, "Read Time", true);
        appendCSVBucketHeaders(line, "Page Cache Read Time", true);
        appendCSVBucketHeaders(line, "Local Read Time", true);
        appendCSVBucketHeaders(line, "Remote Read Time", true);
        appendCSVColumn(line, "Contentious Reads");
        appendCSVBucketHeaders(line, "Expand Time", true);
        appendCSVColumn(line, "Elapsed Time");
        outputLine(line);

        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            for (Cache::value_type& entry : stats[nodeKind])
            {
                const IndexHashKey& key = entry.first;
                NodeStats& nodeStats = entry.second;
                const char* filePath = operation.queryMetaInfoState().queryFilePath(key.fileId);
                appendCSVColumns(line, key.fileId, filePath, key.offset);
                appendCSVColumn(line, mapNodeKind((NodeKind)nodeKind));
                appendCSVColumn(line, nodeStats.inMemorySize);
                appendCSVEvents(line, nodeStats.events);
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                    appendCSVBucket(line, nodeStats.readTime[bucket], true);
                appendCSVColumn(line, nodeStats.events.loads - nodeStats.readTime[Total].count);
                appendCSVBucket(line, nodeStats.expandTime, true);
                appendCSVColumn(line, nodeStats.elapsedTime);
                outputLine(line);
            }
        }
    }

protected:
    using Cache = std::unordered_map<IndexHashKey, NodeStats, IndexHashKeyHash>;
    Cache stats[NumNodeKinds];
};

class TraceHashKey
{
public:
    StringAttr traceId;
    TraceHashKey() = default;
    TraceHashKey(const char* _traceId) : traceId(_traceId) {}
    TraceHashKey(const CEvent& event) : traceId(event.queryTextValue(EvAttrEventTraceId)) {}
    bool operator == (const TraceHashKey& other) const { return streq(traceId.str(), other.traceId.str()); }
};

class TraceHashKeyHash
{
public:
    std::size_t operator()(const TraceHashKey& key) const
    {
        return hashc((byte*)key.traceId.str(), key.traceId.length(), fnvInitialHash32);
    }
};

class CTraceCollector : public CSummaryCollector
{
protected:
    class NodeKindStats
    {
    public:
        Events events;
        Bucket readTime[NumBuckets];
        Bucket expandTime;
        __uint64 elapsedTime{0}; // Elapsed time accumulated per trace
        Bucket elapsedTimeBucket; // Accumulated across traces (for Service summarization)
        std::unordered_map<IndexHashKey, uint32_t, IndexHashKeyHash> nodeMemorySize; // Track memory size per unique node
    public:
        NodeKindStats() = default;
        bool isEmpty() const
        {
            return events.hits == 0 && events.misses == 0 && events.loads == 0 &&
                   events.payloads == 0 && events.evictions == 0;
        }
        void visit(CEvent& event)
        {
            __uint64 tmp;
            switch (event.queryType())
            {
            case EventIndexCacheHit:
                tmp = event.queryNumericValue(EvAttrInMemorySize);
                if (tmp)
                    nodeMemorySize[IndexHashKey(event)] = tmp; // Store/update memory size for this node
                events.hits++;
                break;
            case EventIndexCacheMiss:
                events.misses++;
                break;
            case EventIndexLoad:
                tmp = event.queryNumericValue(EvAttrInMemorySize);
                if (tmp)
                    nodeMemorySize[IndexHashKey(event)] = tmp; // Store/update memory size for this node
                tmp = event.queryNumericValue(EvAttrReadTime);
                if (tmp)
                {
                    accumulateValue(tmp, readTime[Total]);
                    accumulateValue(tmp, readTime[chooseBucketCategory(tmp)]);
                    elapsedTime += tmp;
                    tmp = event.queryNumericValue(EvAttrExpandTime);
                    if (tmp)
                    {
                        accumulateValue(tmp, expandTime);
                        elapsedTime += tmp;
                    }
                }
                events.loads++;
                break;
            case EventIndexEviction:
                tmp = event.queryNumericValue(EvAttrInMemorySize);
                if (tmp)
                    nodeMemorySize[IndexHashKey(event)] = tmp; // Store/update memory size for this node
                events.evictions++;
                break;
            case EventIndexPayload:
                tmp = event.queryNumericValue(EvAttrExpandTime);
                if (tmp && event.queryBooleanValue(EvAttrFirstUse))
                {
                    accumulateValue(tmp, expandTime);
                    elapsedTime += tmp;
                }
                events.payloads++;
                break;
            default:
                break;
            }
        }
        void addStats(const NodeKindStats& other)
        {
            events.hits += other.events.hits;
            events.misses += other.events.misses;
            events.loads += other.events.loads;
            events.payloads += other.events.payloads;
            events.evictions += other.events.evictions;
            for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
            {
                if (other.readTime[bucket].count == 0)
                    continue;
                accumulateBucket(other.readTime[bucket], readTime[bucket]);
            }
            if (other.expandTime.count != 0)
                accumulateBucket(other.expandTime, expandTime);
            elapsedTime += other.elapsedTime;
            nodeMemorySize.insert(other.nodeMemorySize.begin(), other.nodeMemorySize.end());
        }
    };
    class TraceStats
    {
    public:
        NodeKindStats kinds[NumNodeKinds];
        std::set<__uint64> uniqueFileIds; // Track unique files accessed in this trace
        __uint64 firstTimestamp{UINT64_MAX}; // Earliest event timestamp in this trace
        __uint64 lastTimestamp{0}; // Latest event timestamp in this trace
    public:
        TraceStats() = default;
        void visit(CEvent& event)
        {
            __uint64 fileId = event.queryNumericValue(EvAttrFileId);
            uniqueFileIds.insert(fileId);

            // Track earliest and latest timestamps for this trace
            __uint64 timestamp = event.queryNumericValue(EvAttrEventTimestamp);
            if (timestamp != 0)
            {
                firstTimestamp = std::min(firstTimestamp, timestamp);
                lastTimestamp = std::max(lastTimestamp, timestamp);
            }

            kinds[queryIndexNodeKind(event)].visit(event);
        }
    };
public: // IEventVisitor
    virtual bool visitEvent(CEvent& event) override
    {
        // Allow EventQueryStart events to pass through; trace ID to service name
        // mapping is built earlier by the metadata parser (CMetaInfoState::visitEvent)
        if (event.queryType() == EventQueryStart)
            return true;

        // Implicit event filter applied unconditionally
        if (queryEventContext(event.queryType()) != EventCtxIndex)
            return true;
        __uint64 fileId = event.queryNumericValue(EvAttrFileId);
        if (event.queryType() != MetaFileInformation)
        {
            const char* traceIdStr = "";
            if (event.hasAttribute(EvAttrEventTraceId))
                traceIdStr = event.queryTextValue(EvAttrEventTraceId);
            // Note: Missing trace ID values are considered valid and represented as empty string

            TraceHashKey key(traceIdStr);
            stats[key].visit(event);
        }
        return true;
    }

public: // CSummaryCollector
    using CSummaryCollector::CSummaryCollector;

    virtual void summarize() override
    {
        // Determine which node kinds have data
        std::fill_n(haveNodeKindEntries, NumNodeKinds, false);
        for (Cache::value_type& entry : stats)
        {
            TraceStats& traceStats = entry.second;
            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
            {
                const NodeKindStats& nodeKindStats = traceStats.kinds[nodeKind];
                if (!nodeKindStats.isEmpty())
                {
                    haveNodeKindEntries[nodeKind] = true;
                }
            }
        }
        if (summarization == IndexSummarization::byService)
            summarizeByService();
        else
            summarizeByTrace();
    }

protected:
    void summarizeByTrace()
    {
        StringBuffer line;
        appendCSVColumns(line, "Trace ID", "Service Name", "First Timestamp", "Last Timestamp", "Unique Files");
        appendNodeKindHeaders(line);
        outputLine(line);

        for (Cache::value_type& entry : stats)
        {
            const TraceHashKey& key = entry.first;
            TraceStats& traceStats = entry.second;

            // Look up service name for this trace
            const char* serviceName = operation.queryMetaInfoState().queryServiceName(key.traceId.str());
            if (!serviceName)
                serviceName = "";

            // Format timestamps using CDateTime (empty if no valid timestamps were found)
            StringBuffer earliestTsStr, latestTsStr;
            if (traceStats.firstTimestamp != UINT64_MAX)
            {
                CDateTime earliestDt;
                earliestDt.setTimeStampNs(traceStats.firstTimestamp);
                earliestDt.getString(earliestTsStr);
            }
            if (traceStats.lastTimestamp > 0)
            {
                CDateTime latestDt;
                latestDt.setTimeStampNs(traceStats.lastTimestamp);
                latestDt.getString(latestTsStr);
            }

            appendCSVColumns(line, key.traceId.str(), serviceName, earliestTsStr.str(), latestTsStr.str(), traceStats.uniqueFileIds.size());

            // Create aggregate statistics across all node kinds
            NodeKindStats aggregateStats = {};

            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
                aggregateStats.addStats(traceStats.kinds[nodeKind]);

            // Output aggregate statistics
            appendNodeKindData(line, aggregateStats);

            // Output per-node-kind statistics only for kinds that have data
            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
            {
                if (!haveNodeKindEntries[nodeKind])
                    continue;
                const NodeKindStats& nodeKindStats = traceStats.kinds[nodeKind];
                appendNodeKindData(line, nodeKindStats);
            }

            outputLine(line);
        }
    }

    void summarizeByService()
    {
        // Extended TraceStats with trace counter for service summarization
        class ServiceStats : public TraceStats
        {
        public:
            uint32_t traceCount{0}; // Count of traces represented in this service
            NodeKindStats totals;

            ServiceStats() = default;
            void addStats(const TraceStats& other)
            {
                __uint64 traceElapsed = 0;
                for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
                {
                    NodeKindStats& thisNodeKindStats = kinds[nodeKind];
                    const NodeKindStats& otherNodeKindStats = other.kinds[nodeKind];
                    thisNodeKindStats.addStats(otherNodeKindStats);
                    totals.addStats(otherNodeKindStats);

                    // Track elapsed time per trace for this service
                    if (otherNodeKindStats.elapsedTime)
                        accumulateValue(otherNodeKindStats.elapsedTime, thisNodeKindStats.elapsedTimeBucket);

                    traceElapsed += otherNodeKindStats.elapsedTime;
                }

                accumulateValue(traceElapsed, totals.elapsedTimeBucket);
            }
        };

        // Aggregate trace stats by service name
        std::unordered_map<std::string, ServiceStats> serviceStats;

        for (Cache::value_type& entry : stats)
        {
            const TraceHashKey& traceKey = entry.first;
            TraceStats& traceStats = entry.second;

            // Find service name for this trace ID, or use empty string if not found
            const char* serviceName = operation.queryMetaInfoState().queryServiceName(traceKey.traceId.str());
            if (!serviceName)
                serviceName = "";
            ServiceStats& serviceAggregateStats = serviceStats[serviceName];

            // Aggregate the stats
            serviceAggregateStats.addStats(traceStats);

            // Increment trace count
            serviceAggregateStats.traceCount++;

            // Merge unique files
            serviceAggregateStats.uniqueFileIds.insert(traceStats.uniqueFileIds.begin(), traceStats.uniqueFileIds.end());

            // Update timestamp ranges
            if (traceStats.firstTimestamp != UINT64_MAX)
                serviceAggregateStats.firstTimestamp = std::min(serviceAggregateStats.firstTimestamp, traceStats.firstTimestamp);
            if (traceStats.lastTimestamp > 0)
                serviceAggregateStats.lastTimestamp = std::max(serviceAggregateStats.lastTimestamp, traceStats.lastTimestamp);
        }

        // Output aggregated service stats
        StringBuffer line;
        appendCSVColumns(line, "Service Name", "Trace Count", "First Timestamp", "Last Timestamp", "Unique Files");
        appendNodeKindHeaders(line);
        outputLine(line);

        for (auto& serviceEntry : serviceStats)
        {
            const std::string& serviceName = serviceEntry.first;
            ServiceStats& serviceStatsEntry = serviceEntry.second;

            // Format timestamps using CDateTime (empty if no valid timestamps were found)
            StringBuffer firstTsStr, lastTsStr;
            if (serviceStatsEntry.firstTimestamp != UINT64_MAX)
            {
                CDateTime firstDt;
                firstDt.setTimeStampNs(serviceStatsEntry.firstTimestamp);
                firstDt.getString(firstTsStr);
            }
            if (serviceStatsEntry.lastTimestamp > 0)
            {
                CDateTime lastDt;
                lastDt.setTimeStampNs(serviceStatsEntry.lastTimestamp);
                lastDt.getString(lastTsStr);
            }

            const char* displayServiceName = serviceName.empty() ? "<no service>" : serviceName.c_str();
            appendCSVColumns(line, displayServiceName, serviceStatsEntry.traceCount, firstTsStr.str(), lastTsStr.str(), serviceStatsEntry.uniqueFileIds.size());
            appendNodeKindData(line, serviceStatsEntry.totals);
            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
            {
                if (!haveNodeKindEntries[nodeKind])
                    continue;
                const NodeKindStats& nodeStats = serviceStatsEntry.kinds[nodeKind];
                appendNodeKindData(line, nodeStats);
            }
            outputLine(line);
        }
    }

protected:

    void appendNodeKindHeaders(StringBuffer& line)
    {
        // Add aggregate statistics headers
        appendNodeKindHeaders(line, "Total ");

        // Add node kind specific headers only for kinds that have data
        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            if (!haveNodeKindEntries[nodeKind])
                continue;
            const char* nodeKindName = mapNodeKind((NodeKind)nodeKind);
            VStringBuffer prefix("%c%s ", toupper(*nodeKindName), nodeKindName + 1);
            appendNodeKindHeaders(line, prefix);
        }
    }

    void appendNodeKindHeaders(StringBuffer& line, const char* prefix)
    {
        appendCSVEventsHeaders(line, prefix);
        appendCSVBucketHeaders(line, VStringBuffer("%sRead Time", prefix), true);
        appendCSVBucketHeaders(line, VStringBuffer("%sPage Cache Read Time", prefix), true);
        appendCSVBucketHeaders(line, VStringBuffer("%sLocal Read Time", prefix), true);
        appendCSVBucketHeaders(line, VStringBuffer("%sRemote Read Time", prefix), true);
        appendCSVColumn(line, VStringBuffer("%sContentious Reads", prefix));
        appendCSVBucketHeaders(line, VStringBuffer("%sExpand Time", prefix), true);
        appendCSVColumn(line, VStringBuffer("%sUnique Nodes", prefix));
        appendCSVColumn(line, VStringBuffer("%sTotal Memory Size", prefix));
        if (summarization == IndexSummarization::byService)
            appendCSVBucketHeaders(line, VStringBuffer("%sElapsed Time", prefix), false);
        else
            appendCSVColumn(line, VStringBuffer("%sElapsed Time", prefix));
    }

    void appendNodeKindData(StringBuffer& line, const NodeKindStats& nodeKindStats)
    {
        appendCSVEvents(line, nodeKindStats.events);
        for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
            appendCSVBucket(line, nodeKindStats.readTime[bucket], true);
        appendCSVColumn(line, nodeKindStats.events.loads - nodeKindStats.readTime[Total].count);
        appendCSVBucket(line, nodeKindStats.expandTime, true);

        // Calculate unique nodes and total memory size
        __uint64 totalMemorySize = 0;
        for (const auto& nodeEntry : nodeKindStats.nodeMemorySize)
            totalMemorySize += nodeEntry.second;

        appendCSVColumn(line, nodeKindStats.nodeMemorySize.size());
        appendCSVColumn(line, totalMemorySize);
        if (summarization == IndexSummarization::byService)
            appendCSVBucket(line, nodeKindStats.elapsedTimeBucket, false);
        else
            appendCSVColumn(line, nodeKindStats.elapsedTime);
    }

protected:
    using Cache = std::unordered_map<TraceHashKey, TraceStats, TraceHashKeyHash>;
    Cache stats;
    bool haveNodeKindEntries[NumNodeKinds] = {false,};

};

bool CIndexFileSummary::doOp()
{
    Owned<CSummaryCollector> collector;
    switch (summarization)
    {
    case IndexSummarization::byFile:
    case IndexSummarization::byNodeKind:
    case IndexSummarization::byNode:
        collector.setown(new CNodeCollector(*this, summarization, out));
        break;
    case IndexSummarization::byTrace:
    case IndexSummarization::byService:
        collector.setown(new CTraceCollector(*this, summarization, out));
        break;
    default:
        return false;
    }
    if (!traverseEvents(*collector))
        return false;
    collector->summarize();
    return true;
}
