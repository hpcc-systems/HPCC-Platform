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

#include "evtool.hpp"
#include "eventindexsummarize.h"
#include "evtindex.hpp"
#include "jevent.hpp"
#include "jstream.hpp"
#include "jstring.hpp"

// Connector between the CLI and the index file summary operation.
class CEvtIndexSummaryCommand : public TEventConsumingCommand<CIndexFileSummary>
{
public:
    virtual unsigned acceptLongOption(const char* key, const char* nextArg) override
    {
        if (streq(key, "group-by"))
        {
            if (!nextArg || nextArg[0] == '-')
                throw MakeStringException(0, "missing value for --group-by");
            StringArray attrs;
            attrs.appendList(nextArg, ",");
            std::vector<std::string> groupCols;
            ForEachItemIn(i, attrs)
                groupCols.push_back(attrs.item(i));
            op.addGroupAttribute(groupCols);
            return 2;
        }
        return 0;
    }

    virtual bool acceptTerseOption(char opt) override
    {
        switch (opt)
        {
            case 'f':
                op.setSummarization(IndexSummarization::byFile);
                break;
            case 'k':
                op.setSummarization(IndexSummarization::byNodeKind);
                break;
            case 'n':
                op.setSummarization(IndexSummarization::byNode);
                break;
            case 't':
                op.setSummarization(IndexSummarization::byTrace);
                break;
            case 's':
                op.setSummarization(IndexSummarization::byService);
                break;
            default:
                return false;
        }
        return true;
    }

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Summarize the index events in a binary event file. By default, all events are
aggregated into a single summary row. Optional parameters provide either
pre-defined summarization types or user-defined attribute groupings. All
summaries are output in CSV format, with a header row describing the columns.

Each pre-defined summarization produces one row per unique summary key. The
user-defined summaries produce rows for each unique combination of key values
plus a single sub-total row for each key group.
)!!!";
    }

    virtual const char* getBriefDescription() const override
    {
        return "describe the index event effects on performance";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append(R"!!!([ [--group-by <attributes>]... | -f | -k | -n | -s | -t ] [filters] <filename>
)!!!");
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexFileSummary>::usageOptions(out);
        constexpr const char* usageStr =
R"!!!(    --group-by <attributes>   Summarize activity by the comma-separated list
                              of attributes. Attributes are either actual event
                              attribute names or reserved names representing
                              derived values (e.g., "LogicalFileName"). May be
                              repeated to define nested sub-groupings.
    -f                        Summarize activity by file.
    -k                        Summarize activity by node kind.
    -n                        Summarize activity by node.
    -t                        Summarize activity by trace ID.
    -s                        Summarize activity by service name.

    If no summarization options are provided, a global total is generated.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr =
R"!!!(Details:
    The --group-by option deprecates all other summary types. Instead of hard-
    coding specific summary combinations, it gives the user the flexibility to
    group by any combination of attributes. Multiple attributes may be given,
    creating a group for every permutation of attribute values. Multiple
    parameters may be given to define sub-groupings.

    To illustrate the difference between one group with multiple attributes and
    multiple, nested groups, consider an example using the NodeKind and
    LogicalFileName attributes. There are four ways to request a summary using
    any two attributes:

        --group-by NodeKind,LogicalFileName
        --group-by LogicalFileName,NodeKind
        --group-by NodeKind --group-by LogicalFileName
        --group-by LogicalFileName --group-by NodeKind

    The first two requests create one summary row for each unique pair of
    attribute values, and one row totaling the summary rows. Only the output
    column order differs, as each output row begins with a column for each
    grouping attribute in the order specified.

    The third request creates, for each observed NodeKind, a single summary
    row for each observed LogicalFileName and one row with subtotals for the
    node kind. One final row, with the totals for all node kinds completes
    the output.

    The fourth request generates, for each observed LogicalFileName, a single
    summary row for each observed NodeKind and one row with subtotals for the
    file. One final row, with the totals for all files completes the output.

    Limited support for grouping an event according to values that are implied
    by its content, rather than by its actual content, is offered.

    - LogicalFileName is recognized for index events containing a FileId from
      input files that include both FileInformation and PlaneInformation meta
      events.
    - Path is recognized for index events containing a FileId from input files
      that include FileInformation meta events.
    - Plane is recognized for index events containing a FileId from input files
      that include both FileInformation and PlaneInformation meta events. Use
      with an index event model that models alternate planes will not reflect
      modeled locations.
    - ServiceName is recognized for events containing an EventTraceId from
      input files that include QueryStart events.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

IEvToolCommand* createIndexSummaryCommand()
{
    return new CEvtIndexSummaryCommand();
}
