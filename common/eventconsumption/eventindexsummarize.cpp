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
        __uint64 nodeKind = (event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1);
        NodeStats& nodeStats = stats[nodeKind][key];
        __uint64 tmp;
        switch (event.queryType())
        {
        case EventIndexCacheHit:
            tmp = event.queryNumericValue(EvAttrInMemorySize);
            if (tmp)
                nodeStats.inMemorySize = tmp;
            nodeStats.hits++;
            break;
        case EventIndexCacheMiss:
            nodeStats.misses++;
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
            nodeStats.loads++;
            break;
        case EventIndexEviction:
            tmp = event.queryNumericValue(EvAttrInMemorySize);
            if (tmp)
                nodeStats.inMemorySize = tmp;
            nodeStats.evictions++;
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
            nodeStats.payloads++;
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
        Bucket inMemorySize;
        __uint64 hits{0};
        __uint64 misses{0};
        __uint64 loads{0};
        __uint64 reads{0};
        __uint64 payloads{0};
        __uint64 expansions{0};
        __uint64 evictions{0};
        Bucket readTime[NodeStats::NumBuckets];
        Bucket expandTime;
    };
    struct FileStats
    {
        NodeKindStats kinds[NumNodeKinds];
    };
    using SummaryStats = std::map<__uint64, FileStats>;
    SummaryStats summary;
    for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
    {
        for (Cache::value_type& entry : stats[nodeKind])
        {
            NodeKindStats& nodeKindStats = summary[entry.first.fileId].kinds[nodeKind];
            nodeKindStats.inMemorySize.total = entry.second.inMemorySize;
            nodeKindStats.inMemorySize.min = std::min(nodeKindStats.inMemorySize.min, __uint64(entry.second.inMemorySize));
            nodeKindStats.inMemorySize.max = std::max(nodeKindStats.inMemorySize.max, __uint64(entry.second.inMemorySize));
            nodeKindStats.hits += entry.second.hits;
            nodeKindStats.misses += entry.second.misses;
            nodeKindStats.loads += entry.second.loads;
            nodeKindStats.payloads += entry.second.payloads;
            nodeKindStats.evictions += entry.second.evictions;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
            {
                nodeKindStats.readTime[bucket].count += entry.second.readTime[bucket].count;
                nodeKindStats.readTime[bucket].total += entry.second.readTime[bucket].total;
                nodeKindStats.readTime[bucket].min = std::min(nodeKindStats.readTime[bucket].min, __uint64(entry.second.readTime[bucket].min));
                nodeKindStats.readTime[bucket].max = std::max(nodeKindStats.readTime[bucket].max, __uint64(entry.second.readTime[bucket].max));
            }
            nodeKindStats.expandTime.count += entry.second.expandTime.count;
            nodeKindStats.expandTime.total += entry.second.expandTime.total;
            nodeKindStats.expandTime.min = std::min(nodeKindStats.expandTime.min, __uint64(entry.second.expandTime.min));
            nodeKindStats.expandTime.max = std::max(nodeKindStats.expandTime.max, __uint64(entry.second.expandTime.max));
        }
    }

    StringBuffer line;
    appendCSVColumn(line, "File Id");
    appendCSVColumn(line, "File Path");
    appendCSVColumn(line, "Total Branch In Memory Size");
    appendCSVColumn(line, "Min Branch In Memory Size");
    appendCSVColumn(line, "Max Branch In Memory Size");
    appendCSVColumn(line, "Average Branch In Memory Size");
    appendCSVColumn(line, "Branch Hits");
    appendCSVColumn(line, "Branch Misses");
    appendCSVColumn(line, "Branch Loads");
    appendCSVColumn(line, "Branch Payloads");
    appendCSVColumn(line, "Branch Evictions");
    appendCSVColumn(line, "Branch Page Cache Reads");
    appendCSVColumn(line, "Total Branch Page Cache Read Time");
    appendCSVColumn(line, "Min Branch Page Cache Read Time");
    appendCSVColumn(line, "Max Branch Page Cache Read Time");
    appendCSVColumn(line, "Average Branch Read Time");
    appendCSVColumn(line, "Branch Local Reads");
    appendCSVColumn(line, "Total Branch Local Read Time");
    appendCSVColumn(line, "Min Branch Local Read Time");
    appendCSVColumn(line, "Max Branch Local Read Time");
    appendCSVColumn(line, "Average Branch Local Read Time");
    appendCSVColumn(line, "Branch Remote Reads");
    appendCSVColumn(line, "Total Branch Remote Read Time");
    appendCSVColumn(line, "Min Branch Remote Read Time");
    appendCSVColumn(line, "Max Branch Remote Read Time");
    appendCSVColumn(line, "Average Branch Remote Read Time");
    appendCSVColumn(line, "Contentious Branch Reads");
    appendCSVColumn(line, "Branch Expansions");
    appendCSVColumn(line, "Total Branch Expand Time");
    appendCSVColumn(line, "Min Branch Expand Time");
    appendCSVColumn(line, "Max Branch Expand Time");
    appendCSVColumn(line, "Average Branch Expand Time");
    appendCSVColumn(line, "Total Leaf In Memory Size");
    appendCSVColumn(line, "Min Leaf In Memory Size");
    appendCSVColumn(line, "Max Leaf In Memory Size");
    appendCSVColumn(line, "Average Leaf In Memory Size");
    appendCSVColumn(line, "Leaf Hits");
    appendCSVColumn(line, "Leaf Misses");
    appendCSVColumn(line, "Leaf Loads");
    appendCSVColumn(line, "Leaf Payloads");
    appendCSVColumn(line, "Leaf Evictions");
    appendCSVColumn(line, "Leaf Page Cache Reads");
    appendCSVColumn(line, "Total Leaf Page Cache Read Time");
    appendCSVColumn(line, "Min Leaf Page Cache Read Time");
    appendCSVColumn(line, "Max Leaf Page Cache Read Time");
    appendCSVColumn(line, "Average Leaf Page Cache Read Time");
    appendCSVColumn(line, "Leaf Local Reads");
    appendCSVColumn(line, "Total Leaf Local Read Time");
    appendCSVColumn(line, "Min Leaf Local Read Time");
    appendCSVColumn(line, "Max Leaf Local Read Time");
    appendCSVColumn(line, "Average Leaf Local Read Time");
    appendCSVColumn(line, "Leaf Remote Reads");
    appendCSVColumn(line, "Total Leaf Remote Read Time");
    appendCSVColumn(line, "Min Leaf Remote Read Time");
    appendCSVColumn(line, "Max Leaf Remote Read Time");
    appendCSVColumn(line, "Average Leaf Remote Read Time");
    appendCSVColumn(line, "Contentious Leaf Reads");
    appendCSVColumn(line, "Leaf Expansions");
    appendCSVColumn(line, "Total Leaf Expand Time");
    appendCSVColumn(line, "Min Leaf Expand Time");
    appendCSVColumn(line, "Max Leaf Expand Time");
    appendCSVColumn(line, "Average Leaf Expand Time");
    outputLine(line);

    for (SummaryStats::value_type& e : summary)
    {
        appendCSVColumn(line, e.first);
        appendCSVColumn(line, fileInfo[e.first].str());
        for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
        {
            if (e.second.kinds[nodeKind].loads)
            {
                appendCSVColumn(line, e.second.kinds[nodeKind].inMemorySize.total);
                appendCSVColumn(line, e.second.kinds[nodeKind].inMemorySize.min);
                appendCSVColumn(line, e.second.kinds[nodeKind].inMemorySize.max);
                appendCSVColumn(line, e.second.kinds[nodeKind].inMemorySize.total / e.second.kinds[nodeKind].loads);
            }
            else
            {
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
            }
            appendCSVColumn(line, e.second.kinds[nodeKind].hits);
            appendCSVColumn(line, e.second.kinds[nodeKind].misses);
            appendCSVColumn(line, e.second.kinds[nodeKind].loads);
            appendCSVColumn(line, e.second.kinds[nodeKind].payloads);
            appendCSVColumn(line, e.second.kinds[nodeKind].evictions);
            appendCSVColumn(line, e.second.kinds[nodeKind].reads);
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
            {
                appendCSVColumn(line, e.second.kinds[nodeKind].readTime[bucket].count);
                if (e.second.kinds[nodeKind].readTime[bucket].count)
                {
                    appendCSVColumn(line, e.second.kinds[nodeKind].readTime[bucket].total);
                    appendCSVColumn(line, e.second.kinds[nodeKind].readTime[bucket].min);
                    appendCSVColumn(line, e.second.kinds[nodeKind].readTime[bucket].max);
                    appendCSVColumn(line, e.second.kinds[nodeKind].readTime[bucket].total / e.second.kinds[nodeKind].readTime[bucket].count);
                }
                else
                {
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                }
            }
            __uint64 reads = 0;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
                reads += e.second.kinds[nodeKind].readTime[bucket].count;
            if (e.second.kinds[nodeKind].loads != reads)
                appendCSVColumn(line, e.second.kinds[nodeKind].loads - reads);
            else
                appendCSVColumn(line, 0ULL);
            appendCSVColumn(line, e.second.kinds[nodeKind].expandTime.count);
            if (e.second.kinds[nodeKind].expandTime.count)
            {
                appendCSVColumn(line, e.second.kinds[nodeKind].expandTime.total);
                appendCSVColumn(line, e.second.kinds[nodeKind].expandTime.min);
                appendCSVColumn(line, e.second.kinds[nodeKind].expandTime.max);
                appendCSVColumn(line, e.second.kinds[nodeKind].expandTime.total / e.second.kinds[nodeKind].expandTime.count);
            }
            else
            {
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
            }
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

    StringBuffer line;
    appendCSVColumn(line, "Node Kind");
    appendCSVColumn(line, "Total In Memory Size");
    appendCSVColumn(line, "Min In Memory Size");
    appendCSVColumn(line, "Max In Memory Size");
    appendCSVColumn(line, "Average In Memory Size");
    appendCSVColumn(line, "Hits");
    appendCSVColumn(line, "Misses");
    appendCSVColumn(line, "Loads");
    appendCSVColumn(line, "Payloads");
    appendCSVColumn(line, "Evictions");
    appendCSVColumn(line, "Page Cache Reads");
    appendCSVColumn(line, "Total Page Cache Read Time");
    appendCSVColumn(line, "Min Page Cache Read Time");
    appendCSVColumn(line, "Max Page Cache Read Time");
    appendCSVColumn(line, "Average Page Cache Read Time");
    appendCSVColumn(line, "Local Reads");
    appendCSVColumn(line, "Total Local Read Time");
    appendCSVColumn(line, "Min Local Read Time");
    appendCSVColumn(line, "Max Local Read Time");
    appendCSVColumn(line, "Average Local Read Time");
    appendCSVColumn(line, "Remote Reads");
    appendCSVColumn(line, "Total Remote Read Time");
    appendCSVColumn(line, "Min Remote Read Time");
    appendCSVColumn(line, "Max Remote Read Time");
    appendCSVColumn(line, "Average Remote Read Time");
    appendCSVColumn(line, "Contentious Reads");
    appendCSVColumn(line, "Expansions");
    appendCSVColumn(line, "Total Expand Time");
    appendCSVColumn(line, "Min Expand Time");
    appendCSVColumn(line, "Max Expand Time");
    appendCSVColumn(line, "Average Expand Time");
    outputLine(line);

    for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
    {
        Bucket inMemorySize;
        __uint64 hits{0};
        __uint64 misses{0};
        __uint64 loads{0};
        __uint64 payloads{0};
        __uint64 evictions{0};
        Bucket readTime[NodeStats::NumBuckets];
        Bucket expandTime;

        for (Cache::value_type& entry : stats[nodeKind])
        {
            inMemorySize.total += entry.second.inMemorySize;
            inMemorySize.min = std::min(inMemorySize.min, entry.second.inMemorySize);
            inMemorySize.max = std::max(inMemorySize.max, entry.second.inMemorySize);
            hits += entry.second.hits;
            misses += entry.second.misses;
            loads += entry.second.loads;
            payloads += entry.second.payloads;
            evictions += entry.second.evictions;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
            {
                readTime[bucket].count += entry.second.readTime[bucket].count;
                readTime[bucket].total += entry.second.readTime[bucket].total;
                readTime[bucket].min = std::min(readTime[bucket].min, uint32_t(entry.second.readTime[bucket].min));
                readTime[bucket].max = std::max(readTime[bucket].max, uint32_t(entry.second.readTime[bucket].max));
            }
            expandTime.count += entry.second.expandTime.count;
            expandTime.total += entry.second.expandTime.total;
            expandTime.min = std::min(expandTime.min, entry.second.expandTime.min);
            expandTime.max = std::max(expandTime.max, entry.second.expandTime.max);
        }

        switch (nodeKind)
        {
        case 0:
            appendCSVColumn(line, "branch");
            break;
        case 1:
            appendCSVColumn(line, "leaf");
            break;
        default:
            throw makeStringExceptionV(-1, "unknown node kind: %u", nodeKind);
        }
        if (inMemorySize.total)
        {
            appendCSVColumn(line, inMemorySize.total);
            appendCSVColumn(line, inMemorySize.min);
            appendCSVColumn(line, inMemorySize.max);
            appendCSVColumn(line, inMemorySize.total / stats[nodeKind].size());
        }
        else
        {
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
        }
        appendCSVColumn(line, hits);
        appendCSVColumn(line, misses);
        appendCSVColumn(line, loads);
        appendCSVColumn(line, payloads);
        appendCSVColumn(line, evictions);
        for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
        {
            appendCSVColumn(line, readTime[bucket].count);
            if (readTime[bucket].count)
            {
                appendCSVColumn(line, readTime[bucket].total);
                appendCSVColumn(line, readTime[bucket].min);
                appendCSVColumn(line, readTime[bucket].max);
                appendCSVColumn(line, readTime[bucket].total / readTime[bucket].count);
            }
            else
            {
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
            }
        }
        __uint64 reads = 0;
        for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
            reads += readTime[bucket].count;
        if (loads != reads)
            appendCSVColumn(line, loads - reads);
        else
            appendCSVColumn(line, 0ULL);
        appendCSVColumn(line, expandTime.count);
        if (expandTime.count)
        {
            appendCSVColumn(line, expandTime.total);
            appendCSVColumn(line, expandTime.min);
            appendCSVColumn(line, expandTime.max);
            appendCSVColumn(line, expandTime.total / expandTime.count);
        }
        else
        {
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
            appendCSVColumn(line, "");
        }
        outputLine(line);
    }
}

void CIndexFileSummary::summarizeByNode()
{
    StringBuffer line;
    appendCSVColumn(line, "File Id");
    appendCSVColumn(line, "File Path");
    appendCSVColumn(line, "File Offset");
    appendCSVColumn(line, "Node Kind");
    appendCSVColumn(line, "In Memory Size");
    appendCSVColumn(line, "Hits");
    appendCSVColumn(line, "Misses");
    appendCSVColumn(line, "Loads");
    appendCSVColumn(line, "Payloads");
    appendCSVColumn(line, "Evictions");
    appendCSVColumn(line, "Page Cache Reads");
    appendCSVColumn(line, "Total Page Cache Read Time");
    appendCSVColumn(line, "Minimum Page Cache Read Time");
    appendCSVColumn(line, "Maximum Page Cache Read Time");
    appendCSVColumn(line, "Average Page Cache Read Time");
    appendCSVColumn(line, "Local Reads");
    appendCSVColumn(line, "Total Local Read Time");
    appendCSVColumn(line, "Minimum Local Read Time");
    appendCSVColumn(line, "Maximum Local Read Time");
    appendCSVColumn(line, "Average Local Read Time");
    appendCSVColumn(line, "Remote Reads");
    appendCSVColumn(line, "Total Remote Read Time");
    appendCSVColumn(line, "Minimum Remote Read Time");
    appendCSVColumn(line, "Maximum Remote Read Time");
    appendCSVColumn(line, "Average Remote Read Time");
    appendCSVColumn(line, "Contentious Reads");
    appendCSVColumn(line, "Expansions");
    appendCSVColumn(line, "Total Expand Time");
    appendCSVColumn(line, "Minimum Expand Time");
    appendCSVColumn(line, "Maximum Expand Time");
    appendCSVColumn(line, "Average Expand Time");
    outputLine(line);

    for (unsigned nodeKind = 0; nodeKind < NumNodeKinds; nodeKind++)
    {
        for (Cache::value_type& entry : stats[nodeKind])
        {
            appendCSVColumn(line, entry.first.fileId);
            appendCSVColumn(line, fileInfo[entry.first.fileId].str());
            appendCSVColumn(line, entry.first.offset);
            switch (nodeKind)
            {
            case 0:
                appendCSVColumn(line, "branch");
                break;
            case 1:
                appendCSVColumn(line, "leaf");
                break;
            default:
                throw makeStringExceptionV(-1, "unknown node kind: %u", nodeKind);
            }
            appendCSVColumn(line, entry.second.inMemorySize);
            appendCSVColumn(line, entry.second.hits);
            appendCSVColumn(line, entry.second.misses);
            appendCSVColumn(line, entry.second.loads);
            appendCSVColumn(line, entry.second.payloads);
            appendCSVColumn(line, entry.second.evictions);
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
            {
                appendCSVColumn(line, entry.second.readTime[bucket].count);
                if (entry.second.readTime[bucket].count)
                {
                    appendCSVColumn(line, entry.second.readTime[bucket].total);
                    appendCSVColumn(line, entry.second.readTime[bucket].min);
                    appendCSVColumn(line, entry.second.readTime[bucket].max);
                    appendCSVColumn(line, entry.second.readTime[bucket].total / entry.second.readTime[bucket].count);
                }
                else
                {
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                    appendCSVColumn(line, "");
                }
            }
            __uint64 reads = 0;
            for (unsigned bucket = 0; bucket < NodeStats::NumBuckets; bucket++)
                reads += entry.second.readTime[bucket].count;
            if (entry.second.loads != reads)
                appendCSVColumn(line, entry.second.loads - reads);
            else
                appendCSVColumn(line, 0ULL);
            appendCSVColumn(line, entry.second.expandTime.count);
            if (entry.second.expandTime.count)
            {
                appendCSVColumn(line, entry.second.expandTime.total);
                appendCSVColumn(line, entry.second.expandTime.min);
                appendCSVColumn(line, entry.second.expandTime.max);
                appendCSVColumn(line, entry.second.expandTime.total / entry.second.expandTime.count);
            }
            else
            {
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
                appendCSVColumn(line, "");
            }
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

void CIndexFileSummary::outputLine(StringBuffer& line)
{
    line.append('\n');
    out->put(line.length(), line.str());
    line.clear();
}
