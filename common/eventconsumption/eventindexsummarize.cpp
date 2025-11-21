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

bool CIndexFileSummary::visitFile(const char *filename, uint32_t version)
{
    return true;
}

bool CIndexFileSummary::visitEvent(CEvent& event)
{
    // Implicit event filter applied unconditionally
    if (queryEventContext(event.queryType()) != EventCtxIndex)
        return true;
    __uint64 fileId = event.queryNumericValue(EvAttrFileId);
    if (event.queryType() == MetaFileInformation)
        fileInfo[fileId].set(event.queryTextValue(EvAttrPath));
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
                unsigned bucket = NodeStats::NumBuckets;
                while (bucket > 0 && tmp < readBucketBoundary[bucket - 1])
                    bucket--;
                assertex(bucket < NodeStats::NumBuckets);

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

void CIndexFileSummary::departFile(uint32_t bytesRead)
{
}

bool CIndexFileSummary::doOp()
{
    if (traverseEvents(inputPath, *this))
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
            return false;
        }
        return true;
    }
    return false;
}

void CIndexFileSummary::summarizeByFile()
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
        Bucket readTime[NodeStats::NumBuckets];
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
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
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
        appendCSVColumns(line, e.first, fileInfo[e.first].str());
        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            if (!haveNodeKindEntries[nodeKind])
                continue;
            NodeKindStats& nodeStats = e.second.kinds[nodeKind];
            appendCSVBucket(line, nodeStats.inMemorySize, false);
            appendCSVEvents(line, nodeStats.events);
            __uint64 contentiousReads = nodeStats.events.loads;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
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

void CIndexFileSummary::summarizeByNodeKind()
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
        Bucket readTime[NodeStats::NumBuckets];
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
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
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
        for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
        {
            appendCSVBucket(line, readTime[bucket], true);
            contentiousReads -= readTime[bucket].count;
        }
        appendCSVColumn(line, contentiousReads);
        appendCSVBucket(line, expandTime, true);
        outputLine(line);
    }
}

void CIndexFileSummary::summarizeByNode()
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
            appendCSVColumns(line, key.fileId, fileInfo[key.fileId].str(), key.offset);
            appendCSVColumn(line, mapNodeKind((NodeKind)nodeKind));
            appendCSVColumn(line, nodeStats.inMemorySize);
            appendCSVEvents(line, nodeStats.events);
            __uint64 contentiousReads = nodeStats.events.loads;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
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

void CIndexFileSummary::appendCSVColumn(StringBuffer& line, const char* value)
{
    if (!line.isEmpty())
        line.append(',');
    if (!isEmptyString(value))
        encodeCSVColumn(line, value);
}

void CIndexFileSummary::appendCSVColumn(StringBuffer& line, __uint64 value)
{
    if (!line.isEmpty())
        line.append(',');
    line.append(value);
}

template<typename... Args>
void CIndexFileSummary::appendCSVColumns(StringBuffer& line, Args&&... values)
{
    // Use fold expression to append each value in sequence
    // This allows calling appendCSVColumns(line, value1, value2, value3, ...)
    // instead of multiple individual appendCSVColumn calls
    (appendCSVColumn(line, std::forward<Args>(values)), ...);
}

template<typename bucket_type_t>
void CIndexFileSummary::appendCSVBucket(StringBuffer& line, const bucket_type_t& bucket, bool includeCount)
{
    if (includeCount)
        appendCSVColumn(line, bucket.count);
    if (bucket.count)
        appendCSVColumns(line, bucket.total, bucket.min, bucket.max, bucket.total / bucket.count);
    else
        appendCSVColumns(line, "", "", "", "");
}

void CIndexFileSummary::appendCSVBucketHeaders(StringBuffer& line, const char* prefix, bool includeCount)
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
void CIndexFileSummary::appendCSVEvents(StringBuffer& line, const events_type_t& events)
{
    appendCSVColumns(line, events.hits, events.misses, events.loads, events.payloads, events.evictions);
}

void CIndexFileSummary::appendCSVEventsHeaders(StringBuffer &line, const char *prefix)
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

/*
Example usage:
Instead of:
    appendCSVColumn(line, e.first);
    appendCSVColumn(line, fileInfo[e.first].str());
    appendCSVColumn(line, nodeStats.hits);
    appendCSVColumn(line, nodeStats.misses);

You can write:
    appendCSVColumns(line, e.first, fileInfo[e.first].str(), nodeStats.hits, nodeStats.misses);
*/

// Explicit instantiation for commonly used types to avoid linker issues
template void CIndexFileSummary::appendCSVColumns<const char*>(StringBuffer& line, const char*&& value);
template void CIndexFileSummary::appendCSVColumns<__uint64>(StringBuffer& line, __uint64&& value);
template void CIndexFileSummary::appendCSVColumns<const char*, __uint64>(StringBuffer& line, const char*&& value1, __uint64&& value2);
template void CIndexFileSummary::appendCSVColumns<__uint64, const char*>(StringBuffer& line, __uint64&& value1, const char*&& value2);
template void CIndexFileSummary::appendCSVColumns<__uint64, __uint64>(StringBuffer& line, __uint64&& value1, __uint64&& value2);
template void CIndexFileSummary::appendCSVColumns<const char*, const char*>(StringBuffer& line, const char*&& value1, const char*&& value2);

void CIndexFileSummary::outputLine(StringBuffer &line)
{
    line.append('\n');
    out->put(line.length(), line.str());
    line.clear();
}
