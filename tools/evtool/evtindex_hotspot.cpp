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
#include "eventindexhotspot.h"
#include "jevent.hpp"

// Connector between the CLI and index file hotspot analysis operation.
class CEvtIndexHotspotCommand : public TEventConsumingCommand<CIndexHotspotOp>
{
public:
    virtual bool acceptVerboseOption(const char* opt) override
    {
        bool accepted = TEventConsumingCommand<CIndexHotspotOp>::acceptVerboseOption(opt);
        if (!accepted)
        {
            if (streq(opt, "lookup"))
            {
                op.addObservedEvent(EventIndexCacheHit);
                op.addObservedEvent(EventIndexCacheMiss);
                accepted = true;
            }
            else if (streq(opt, "hit"))
            {
                op.addObservedEvent(EventIndexCacheHit);
                accepted = true;
            }
            else if (streq(opt, "miss"))
            {
                op.addObservedEvent(EventIndexCacheMiss);
                accepted = true;
            }
            else if (streq(opt, "load"))
            {
                op.addObservedEvent(EventIndexLoad);
                accepted = true;
            }
        }
        return accepted;
    }
    virtual bool acceptKVOption(const char* key, const char* value) override
    {
        __uint64 tmp;
        if (streq(key, "granularity"))
        {
            if (!extractVerboseValue(value, tmp, 0, 10))
                return false;
            op.setGranularity(byte(tmp));
            return true;
        }
        else if (streq(key, "top"))
        {
            if (!extractVerboseValue(value, tmp, 0, 100))
                return false;
            op.setLimit(tmp);
            return true;
        }
        return TEventConsumingCommand<CIndexHotspotOp>::acceptKVOption(key, value);
    }

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Identify activity hotspots for each index file referenced within a recorded
event file. The activity to be analyzed is specified by a obligatory event
selector correlating to a single event type. Additional options determine the
analysis to be performed.

Events:
    --hit                     Analyse index cache hit events.
    --load                    Analyze index load events.
    --lookup                  Analyze index lookup, cache hit, and cache miss
                              events.
    --miss                    Analyze index cache miss events.
)!!!";
    }

    virtual const char* getBriefDescription() const override
    {
        return "identify the most active index nodes";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append(R"!!!(<event> [options] [filters] <filename>
)!!!");
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexHotspotOp>::usageOptions(out);
        constexpr const char* usageStr =
R"!!!(    --granularity=[0..10]     Set the analysis resolution, where the resolution
                              is 2^granularity pages per bucket; default is 0,
                              or one page per bucket.
    --top=[0..100]            The maximum number of leaf and branch buckets
                              reported per file, or 0 to report all buckets;
                              default is 10.
)!!!";
        size32_t usageStrLength = strlen(usageStr);
        out.put(usageStrLength, usageStr);
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
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

The "//file/bucket" array may be used to generate a heat map of index activity.
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
        size32_t usageStrLength = strlen(usageStr);
        out.put(usageStrLength, usageStr);
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
};

IEvToolCommand* createIndexHotspotCommand()
{
    return new CEvtIndexHotspotCommand;
}
