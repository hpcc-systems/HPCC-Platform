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
        __uint64 nodeKind = (event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1);
        NodeKindSummary& summary = nodeSummary(fileId, nodeKind);
        switch (event.queryType())
        {
        case EventIndexLookup:
            if (event.queryBooleanValue(EvAttrInCache))
                summary.hits++;
            else
                summary.misses++;
            break;
        case EventIndexLoad:
            summary.loads++;
            summary.loadedSize += event.queryNumericValue(EvAttrInMemorySize);
            summary.read += event.queryNumericValue(EvAttrReadTime);
            summary.expandTime += event.queryNumericValue(EvAttrExpandTime);
            summary.elapsed += event.queryNumericValue(EvAttrExpandTime) + event.queryNumericValue(EvAttrReadTime);
            break;
        case EventIndexEviction:
            summary.evictions++;
            summary.evictedSize += event.queryNumericValue(EvAttrInMemorySize);
            break;
        case EventIndexPayload:
            summary.payloads++;
            if (event.queryNumericValue(EvAttrExpandTime) > 0)
            {
                summary.payloadExpansions++;
                summary.expandTime += event.queryNumericValue(EvAttrExpandTime);
                summary.elapsed += event.queryNumericValue(EvAttrExpandTime);
                summary.payloadConsumption += event.queryNumericValue(EvAttrInMemorySize);
            }
            break;
        default:
            break;
        }
    }
    return true;
}

void CIndexFileSummary::departFile(uint32_t bytesRead)
{
    static const char* header = "FileId,FilePath,BranchHits,BranchMisses,BranchLoads,BranchLoadedSize,BranchEvictions,BranchEvictedSize,BranchElapsed,BranchRead,BranchExpandTime,LeafHits,LeafMisses,LeafLoads,LeafLoadedSize,LeafPayloads,LeafPayloadExpansions,LeafPayloadExpandedSize,LeafEvictions,LeafEvictedSize,LeafElapsed,LeafRead,LeafExpandTime\n";
    static size32_t headerSize = size32_t(strlen(header));
    out->put(headerSize, header);
    StringBuffer line;
    for (const Summary::value_type& s : summary)
    {
        line.append(s.first);
        appendCSVColumn(line, fileInfo[s.first].str());
        appendCSVColumn(line, s.second.branch.hits);
        appendCSVColumn(line, s.second.branch.misses);
        appendCSVColumn(line, s.second.branch.loads);
        appendCSVColumn(line, s.second.branch.loadedSize);
        appendCSVColumn(line, s.second.branch.evictions);
        appendCSVColumn(line, s.second.branch.evictedSize);
        appendCSVColumn(line, s.second.branch.elapsed);
        appendCSVColumn(line, s.second.branch.read);
        appendCSVColumn(line, s.second.branch.expandTime);
        appendCSVColumn(line, s.second.leaf.hits);
        appendCSVColumn(line, s.second.leaf.misses);
        appendCSVColumn(line, s.second.leaf.loads);
        appendCSVColumn(line, s.second.leaf.loadedSize);
        appendCSVColumn(line, s.second.leaf.payloads);
        appendCSVColumn(line, s.second.leaf.payloadExpansions);
        appendCSVColumn(line, s.second.leaf.payloadConsumption);
        appendCSVColumn(line, s.second.leaf.evictions);
        appendCSVColumn(line, s.second.leaf.evictedSize);
        appendCSVColumn(line, s.second.leaf.elapsed);
        appendCSVColumn(line, s.second.leaf.read);
        appendCSVColumn(line, s.second.leaf.expandTime);
        line.append('\n');
        out->put(line.length(), line.str());
        line.clear();
    }
}

bool CIndexFileSummary::doOp()
{
    return traverseEvents(inputPath, *this);
}

CIndexFileSummary::NodeKindSummary& CIndexFileSummary::nodeSummary(__uint64 fileId, __uint64 nodeKind)
{
    switch (nodeKind)
    {
    case 0: // branch
        return summary[fileId].branch;
    case 1: // leaf
        return summary[fileId].leaf;
    default:
        throw makeStringExceptionV(-1, "unknown node kind: %llu", nodeKind);
    }
}

void CIndexFileSummary::appendCSVColumn(StringBuffer& line, const char* value)
{
    encodeCSVColumn(line.append(','), value);
}

void CIndexFileSummary::appendCSVColumn(StringBuffer& line, __uint64 value)
{
    line.append(',').append(value);
}
