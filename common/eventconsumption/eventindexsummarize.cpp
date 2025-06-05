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
    return eventDistributor(event, *this);
}

IEventAttributeVisitor::Continuation CIndexFileSummary::visitEvent(EventType id)
{
    switch (id)
    {
    case EventIndexLookup:
    case EventIndexLoad:
    case EventIndexEviction:
    case MetaFileInformation:
        currentEvent = id;
        return IEventAttributeVisitor::visitContinue;
    default:
        return IEventAttributeVisitor::visitSkipEvent;
    }
}

IEventAttributeVisitor::Continuation CIndexFileSummary::visitAttribute(EventAttr id, const char *value)
{
    switch (id)
    {
    case EvAttrPath:
        // assumes file ID is seen before path
        fileInfo[currentFileId].set(value);
        break;
    default:
        break;
    }
    return IEventAttributeVisitor::visitContinue;
}

IEventAttributeVisitor::Continuation CIndexFileSummary::visitAttribute(EventAttr id, bool value)
{
    switch (id)
    {
    case EvAttrInCache:
        if (value)
            nodeSummary().hits++;
        else
            nodeSummary().misses++;
        break;
    }
    return IEventAttributeVisitor::visitContinue;
}

IEventAttributeVisitor::Continuation CIndexFileSummary::visitAttribute(EventAttr id, __uint64 value)
{
    switch (id)
    {
    case EvAttrFileId:
        currentFileId = value;
        break;
    case EvAttrNodeKind:
        currentNodeKind = value;
        switch (currentEvent)
        {
        case EventIndexLoad:
            nodeSummary().loads++;
            break;
        case EventIndexEviction:
            nodeSummary().evictions++;
            break;
        default:
            break;
        }
        break;
    case EvAttrExpandedSize:
        switch (currentEvent)
        {
        case EventIndexLoad:
            nodeSummary().loadedSize += value;
            break;
        case EventIndexEviction:
            nodeSummary().evictedSize += value;
            break;
        default:
            break;
        }
        break;
    case EvAttrReadTime:
        nodeSummary().read += value;
        break;
    case EvAttrElapsedTime:
        nodeSummary().elapsed += value;
        break;
    default:
        break;
    }
    return IEventAttributeVisitor::visitContinue;
}

bool CIndexFileSummary::departEvent()
{
    currentEvent = EventNone;
    currentFileId = 0;
    currentNodeKind = 0;
    return true;
}

void CIndexFileSummary::departFile(uint32_t bytesRead)
{
    static const char* header = "FileId,FilePath,BranchHits,BranchMisses,BranchLoads,BranchLoadedSize,BranchEvictions,BranchEvictedSize,BranchElapsed,BranchRead,LeafHits,LeafMisses,LeafLoads,LeafLoadedSize,LeafEvictions,LeafEvictedSize,LeafElapsed,LeafRead\n";
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
        appendCSVColumn(line, s.second.leaf.hits);
        appendCSVColumn(line, s.second.leaf.misses);
        appendCSVColumn(line, s.second.leaf.loads);
        appendCSVColumn(line, s.second.leaf.loadedSize);
        appendCSVColumn(line, s.second.leaf.evictions);
        appendCSVColumn(line, s.second.leaf.evictedSize);
        appendCSVColumn(line, s.second.leaf.elapsed);
        appendCSVColumn(line, s.second.leaf.read);
        line.append('\n');
        out->put(line.length(), line.str());
        line.clear();
    }
}

bool CIndexFileSummary::doOp()
{
    return traverseEvents(inputPath, *this);
}

CIndexFileSummary::NodeKindSummary& CIndexFileSummary::nodeSummary()
{
    switch (currentNodeKind)
    {
    case 0: // branch
        return summary[currentFileId].branch;
    case 1: // leaf
        return summary[currentFileId].leaf;
    default:
        throw makeStringExceptionV(-1, "unknown node kind: %llu", currentNodeKind);
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
