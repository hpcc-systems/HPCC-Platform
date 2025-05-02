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

void CStreamingBucketVisitor::begin(EventType observedEvent, unsigned granularity)
{
    currentGranularity = granularity;
    StringBuffer lines;
    lines.append("analysis: ");
    appendAnalysis(lines);
    lines.append('\n');
    lines.append("event: ").append(queryEventName(observedEvent)).append('\n');
    lines.append("granularity: ").append(granularity).append('\n');
    out->put(lines.length(), lines.str());
}

void CStreamingBucketVisitor::arrive(unsigned id, const char* path)
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

void CStreamingBucketVisitor::depart()
{
    for (byte bucketKind = BucketBranch; bucketKind < BucketKindMax; bucketKind++)
        stats[bucketKind].clear();
}

void CStreamingBucketVisitor::end()
{
}

CStreamingBucketVisitor::CStreamingBucketVisitor(IBufferedSerialOutputStream& _out)
    : out(&_out)
{
}

CStreamingBucketVisitor::Stats::Stats()
{
    clear();
}

void CStreamingBucketVisitor::Stats::recordEvent(stat_type value)
{
    if (value < minEventsPerBucket)
        minEventsPerBucket = value;
    if (value > maxEventsPerBucket)
        maxEventsPerBucket = value;
    totalEvents += value;
    totalBuckets++;
}

void CStreamingBucketVisitor::Stats::clear()
{
    minEventsPerBucket = std::numeric_limits<stat_type>::max();
    maxEventsPerBucket = 0;
    totalEvents = 0;
    totalBuckets = 0;
}

StringBuffer& CStreamingBucketVisitor::Stats::toString(StringBuffer& lines, byte indent, const char* label) const
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

IEvToolCommand* createIndexCommand()
{
    return new CEvtCommandGroup({
        { "summarize", createIndexSummaryCommand },
        { "hotspot", createIndexHotspotCommand },
    });
}
