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
        PageCache,
        LocalFile,
        RemoteFile,
        NumBuckets
    };
    static constexpr __uint64 readBucketBoundary[NumBuckets] = {20'000, 400'000, UINT64_MAX};
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
            appendCSVColumns(line, bucket.total, bucket.min, bucket.max, bucket.total / bucket.count);
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
        appendCSVColumn(line, header.append("Min"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Max"));
        header.setLength(prefixLength);
        appendCSVColumn(line, header.append("Average"));
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
    };
public: // IEventVisitor
    virtual bool visitEvent(CEvent& event) override
    {
        // Implicit event filter applied unconditionally
        if (queryEventContext(event.queryType()) != EventCtxIndex)
            return true;
        __uint64 fileId = event.queryNumericValue(EvAttrFileId);
        if (event.queryType() == MetaFileInformation)
            return true; // Handled by CMetaInformationParser
        else
        {
            IndexHashKey key(event.queryNumericValue(EvAttrFileId), event.queryNumericValue(EvAttrFileOffset));
            __uint64 nodeKind = queryIndexNodeKind(event);
            NodeStats& nodeStats = stats[nodeKind][key];
            __uint64 tmp;
            switch (event.queryType())
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
                    unsigned bucket = NumBuckets;
                    while (bucket > 0 && tmp < readBucketBoundary[bucket - 1])
                        bucket--;
                    assertex(bucket < NumBuckets);

                    nodeStats.readTime[bucket].count++;
                    nodeStats.readTime[bucket].total += tmp;
                    nodeStats.readTime[bucket].min = std::min(nodeStats.readTime[bucket].min, uint32_t(tmp));
                    nodeStats.readTime[bucket].max = std::max(nodeStats.readTime[bucket].max, uint32_t(tmp));
                    tmp = event.queryNumericValue(EvAttrExpandTime);
                    if (tmp)
                    {
                        nodeStats.expandTime.count++;
                        nodeStats.expandTime.total += tmp;
                        nodeStats.expandTime.min = std::min(nodeStats.expandTime.min, uint32_t(tmp));
                        nodeStats.expandTime.max = std::max(nodeStats.expandTime.max, uint32_t(tmp));
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
                    nodeStats.expandTime.count++;
                    nodeStats.expandTime.total += tmp;
                    nodeStats.expandTime.min = std::min(nodeStats.expandTime.min, uint32_t(tmp));
                    nodeStats.expandTime.max = std::max(nodeStats.expandTime.max, uint32_t(tmp));
                }
                nodeStats.events.payloads++;
                break;
            default:
                break;
            }
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
                if (nodeStats.events.hits && !nodeStats.events.misses)
                    nodeKindStats.inMemorySize.count++;
                else
                    nodeKindStats.inMemorySize.count += nodeStats.events.loads;
                nodeKindStats.inMemorySize.total += nodeStats.inMemorySize;
                nodeKindStats.inMemorySize.min = std::min(nodeKindStats.inMemorySize.min, __uint64(nodeStats.inMemorySize));
                nodeKindStats.inMemorySize.max = std::max(nodeKindStats.inMemorySize.max, __uint64(nodeStats.inMemorySize));
                nodeKindStats.events.hits += nodeStats.events.hits;
                nodeKindStats.events.misses += nodeStats.events.misses;
                nodeKindStats.events.loads += nodeStats.events.loads;
                nodeKindStats.events.payloads += nodeStats.events.payloads;
                nodeKindStats.events.evictions += nodeStats.events.evictions;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                {
                    nodeKindStats.readTime[bucket].count += nodeStats.readTime[bucket].count;
                    nodeKindStats.readTime[bucket].total += nodeStats.readTime[bucket].total;
                    nodeKindStats.readTime[bucket].min = std::min(nodeKindStats.readTime[bucket].min, __uint64(nodeStats.readTime[bucket].min));
                    nodeKindStats.readTime[bucket].max = std::max(nodeKindStats.readTime[bucket].max, __uint64(nodeStats.readTime[bucket].max));
                }
                nodeKindStats.expandTime.count += nodeStats.expandTime.count;
                nodeKindStats.expandTime.total += nodeStats.expandTime.total;
                nodeKindStats.expandTime.min = std::min(nodeKindStats.expandTime.min, __uint64(nodeStats.expandTime.min));
                nodeKindStats.expandTime.max = std::max(nodeKindStats.expandTime.max, __uint64(nodeStats.expandTime.max));
            }
        }

        StringBuffer line;
        appendCSVColumns(line, "File Id", "File Path");
        if (haveNodeKindEntries[BranchNode])
        {
            appendCSVColumn(line, "Branch In Memory Size");
            appendCSVEventsHeaders(line, "Branch");
            appendCSVBucketHeaders(line, "Branch Page Cache Read", true);
            appendCSVBucketHeaders(line, "Branch Local Read", true);
            appendCSVBucketHeaders(line, "Branch Remote Read", true);
            appendCSVColumn(line, "Contentious Branch Reads");
            appendCSVBucketHeaders(line, "Branch Expansion", true);
        }
        if (haveNodeKindEntries[LeafNode])
        {
            appendCSVColumn(line, "Leaf In Memory Size");
            appendCSVEventsHeaders(line, "Leaf");
            appendCSVBucketHeaders(line, "Leaf Page Cache Read", true);
            appendCSVBucketHeaders(line, "Leaf Local Read", true);
            appendCSVBucketHeaders(line, "Leaf Remote Read", true);
            appendCSVColumn(line, "Contentious Leaf Reads");
            appendCSVBucketHeaders(line, "Leaf Expansion", true);
        }
        if (haveNodeKindEntries[BlobNode])
        {
            appendCSVColumn(line, "Blob In Memory Size");
            appendCSVEventsHeaders(line, "Blob");
            appendCSVBucketHeaders(line, "Blob Page Cache Read", true);
            appendCSVBucketHeaders(line, "Blob Local Read", true);
            appendCSVBucketHeaders(line, "Blob Remote Read", true);
            appendCSVColumn(line, "Contentious Blob Reads");
            appendCSVBucketHeaders(line, "Blob Expansion", true);
        }
        outputLine(line);

        for (SummaryStats::value_type& e : summary)
        {
            const char* filePath = operation.queryMetaInfoState().queryFilePath(e.first);
            appendCSVColumns(line, e.first, filePath ? filePath : "");
            for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
            {
                if (!haveNodeKindEntries[nodeKind])
                    continue;
                NodeKindStats& nodeStats = e.second.kinds[nodeKind];
                appendCSVBucket(line, nodeStats.inMemorySize, false);
                appendCSVEvents(line, nodeStats.events);
                __uint64 contentiousReads = nodeStats.events.loads;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                {
                    appendCSVBucket(line, nodeStats.readTime[bucket], true);
                    contentiousReads -= nodeStats.readTime[bucket].count;
                }
                appendCSVColumn(line, contentiousReads);
                appendCSVBucket(line, nodeStats.expandTime, true);
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
        appendCSVBucketHeaders(line, "In Memory Size", false);
        appendCSVEventsHeaders(line, nullptr);
        appendCSVBucketHeaders(line, "Page Cache Read Time", true);
        appendCSVBucketHeaders(line, "Local Read Time", true);
        appendCSVBucketHeaders(line, "Remote Read Time", true);
        appendCSVColumn(line, "Contentious Reads");
        appendCSVBucketHeaders(line, "Expand Time", true);
        outputLine(line);

        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            if (stats[nodeKind].empty())
                continue;

            Bucket inMemorySize;
            Events events;
            Bucket readTime[NumBuckets];
            Bucket expandTime;

            for (Cache::value_type& entry : stats[nodeKind])
            {
                NodeStats& nodeStats = entry.second;
                if (nodeStats.events.hits && !nodeStats.events.loads)
                    inMemorySize.count++;
                else
                    inMemorySize.count += nodeStats.events.loads;
                inMemorySize.total += nodeStats.inMemorySize;
                inMemorySize.min = std::min(inMemorySize.min, nodeStats.inMemorySize);
                inMemorySize.max = std::max(inMemorySize.max, nodeStats.inMemorySize);
                events.hits += nodeStats.events.hits;
                events.misses += nodeStats.events.misses;
                events.loads += nodeStats.events.loads;
                events.payloads += nodeStats.events.payloads;
                events.evictions += nodeStats.events.evictions;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                {
                    readTime[bucket].count += nodeStats.readTime[bucket].count;
                    readTime[bucket].total += nodeStats.readTime[bucket].total;
                    readTime[bucket].min = std::min(readTime[bucket].min, uint32_t(nodeStats.readTime[bucket].min));
                    readTime[bucket].max = std::max(readTime[bucket].max, uint32_t(nodeStats.readTime[bucket].max));
                }
                expandTime.count += nodeStats.expandTime.count;
                expandTime.total += nodeStats.expandTime.total;
                expandTime.min = std::min(expandTime.min, nodeStats.expandTime.min);
                expandTime.max = std::max(expandTime.max, nodeStats.expandTime.max);
            }

            appendCSVColumn(line, mapNodeKind((NodeKind)nodeKind));
            appendCSVBucket(line, inMemorySize, false);
            appendCSVEvents(line, events);
            __uint64 contentiousReads = events.loads;
            for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
            {
                appendCSVBucket(line, readTime[bucket], true);
                contentiousReads -= readTime[bucket].count;
            }
            appendCSVColumn(line, contentiousReads);
            appendCSVBucket(line, expandTime, true);
            outputLine(line);
        }
    }

    void summarizeByNode()
    {
        StringBuffer line;
        appendCSVColumns(line, "File Id", "File Path", "File Offset", "Node Kind", "In Memory Size");
        appendCSVEventsHeaders(line, nullptr);
        appendCSVBucketHeaders(line, "Page Cache Read Time", true);
        appendCSVBucketHeaders(line, "Local Read Time", true);
        appendCSVBucketHeaders(line, "Remote Read Time", true);
        appendCSVColumn(line, "Contentious Reads");
        appendCSVBucketHeaders(line, "Expand Time", true);
        outputLine(line);

        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            for (Cache::value_type& entry : stats[nodeKind])
            {
                const IndexHashKey& key = entry.first;
                NodeStats& nodeStats = entry.second;
                const char* filePath = operation.queryMetaInfoState().queryFilePath(key.fileId);
                appendCSVColumns(line, key.fileId, filePath ? filePath : "", key.offset);
                appendCSVColumn(line, mapNodeKind((NodeKind)nodeKind));
                appendCSVColumn(line, nodeStats.inMemorySize);
                appendCSVEvents(line, nodeStats.events);
                __uint64 contentiousReads = nodeStats.events.loads;
                for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
                {
                    appendCSVBucket(line, nodeStats.readTime[bucket], true);
                    contentiousReads -= nodeStats.readTime[bucket].count;
                }
                appendCSVColumn(line, contentiousReads);
                appendCSVBucket(line, nodeStats.expandTime, true);
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
                    unsigned bucket = NumBuckets;
                    while (bucket > 0 && tmp < readBucketBoundary[bucket - 1])
                        bucket--;
                    assertex(bucket < NumBuckets);

                    readTime[bucket].count++;
                    readTime[bucket].total += tmp;
                    readTime[bucket].min = std::min(readTime[bucket].min, uint32_t(tmp));
                    readTime[bucket].max = std::max(readTime[bucket].max, uint32_t(tmp));
                    tmp = event.queryNumericValue(EvAttrExpandTime);
                    if (tmp)
                    {
                        expandTime.count++;
                        expandTime.total += tmp;
                        expandTime.min = std::min(expandTime.min, uint32_t(tmp));
                        expandTime.max = std::max(expandTime.max, uint32_t(tmp));
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
                    expandTime.count++;
                    expandTime.total += tmp;
                    expandTime.min = std::min(expandTime.min, uint32_t(tmp));
                    expandTime.max = std::max(expandTime.max, uint32_t(tmp));
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
                readTime[bucket].count += other.readTime[bucket].count;
                readTime[bucket].total += other.readTime[bucket].total;
                readTime[bucket].min = std::min(readTime[bucket].min, other.readTime[bucket].min);
                readTime[bucket].max = std::max(readTime[bucket].max, other.readTime[bucket].max);
            }
            if (other.expandTime.count != 0)
            {
                expandTime.count += other.expandTime.count;
                expandTime.total += other.expandTime.total;
                expandTime.min = std::min(expandTime.min, other.expandTime.min);
                expandTime.max = std::max(expandTime.max, other.expandTime.max);
            }
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
                for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
                {
                    const NodeKindStats& nodeKindStats = other.kinds[nodeKind];
                    kinds[nodeKind].addStats(nodeKindStats);
                    totals.addStats(nodeKindStats);
                }
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
        appendCSVBucketHeaders(line, VStringBuffer("%sPage Cache Read Time", prefix), true);
        appendCSVBucketHeaders(line, VStringBuffer("%sLocal Read Time", prefix), true);
        appendCSVBucketHeaders(line, VStringBuffer("%sRemote Read Time", prefix), true);
        appendCSVColumn(line, VStringBuffer("%sContentious Reads", prefix));
        appendCSVBucketHeaders(line, VStringBuffer("%sExpand Time", prefix), true);
        appendCSVColumn(line, VStringBuffer("%sUnique Nodes", prefix));
        appendCSVColumn(line, VStringBuffer("%sTotal Memory Size", prefix));
    }

    void appendNodeKindData(StringBuffer& line, const NodeKindStats& nodeKindStats)
    {
        appendCSVEvents(line, nodeKindStats.events);
        __uint64 contentiousReads = nodeKindStats.events.loads;
        for (unsigned bucket = 0; bucket < NumBuckets; bucket++)
        {
            appendCSVBucket(line, nodeKindStats.readTime[bucket], true);
            contentiousReads -= nodeKindStats.readTime[bucket].count;
        }
        appendCSVColumn(line, contentiousReads);
        appendCSVBucket(line, nodeKindStats.expandTime, true);

        // Calculate unique nodes and total memory size
        __uint64 totalMemorySize = 0;
        for (const auto& nodeEntry : nodeKindStats.nodeMemorySize)
            totalMemorySize += nodeEntry.second;

        appendCSVColumn(line, nodeKindStats.nodeMemorySize.size());
        appendCSVColumn(line, totalMemorySize);
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
    if (!traverseEvents(inputPath, *collector))
        return false;
    collector->summarize();
    return true;
}
