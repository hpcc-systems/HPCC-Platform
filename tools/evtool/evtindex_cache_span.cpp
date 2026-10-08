/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "evtindex_cache_span.hpp"
#include "eventindexcachespan.h"
#include "jstring.hpp"

class CEvtIndexCacheSpanCommand : public TEventConsumingCommand<CIndexCacheSpanOp>
{
public:
    virtual unsigned acceptLongOption(const char* key, const char* nextArg) override
    {
        if (streq(key, "complete"))
        {
            op.setIncludeComplete(true);
            return 1;
        }
        if (streq(key, "incomplete"))
        {
            op.setIncludeIncomplete(true);
            return 1;
        }
        if (streq(key, "group-by"))
        {
            if (!nextArg || nextArg[0] == '-')
                throw makeStringException(0, "missing value for --group-by");
            StringArray attrs;
            attrs.appendList(nextArg, ",");
            std::vector<std::string> groupCols;
            ForEachItemIn(i, attrs)
                groupCols.push_back(attrs.item(i));
            op.addGroupAttribute(groupCols);
            return 2;
        }
        return TEventConsumingCommand<CIndexCacheSpanOp>::acceptLongOption(key, nextArg);
    }

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Report live and dead index cache spans. A live span begins at IndexLoad or at
    the first observed IndexCacheHit when no load was recorded, and ends at the
    last access before IndexEviction. A dead span begins at that last access and
    ends at IndexEviction. The default output is the all-span summary.
--complete adds classified spans with both IndexLoad and IndexEviction;
--incomplete adds all other classified spans. The options may be combined.
Incomplete spans can be first observed as IndexCacheHit, remain loaded when
recording stops, or be first observed as IndexEviction with zero accesses.
)!!!";
    }

    virtual const char* getBriefDescription() const override
    {
        return "analyze index cache live/dead spans";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append("[--complete] [--incomplete] [--group-by <attributes> [--group-by <attributes>]...] [filters] <filename>\n");
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexCacheSpanOp>::usageOptions(out);
        constexpr const char* usage = R"!!!(    --complete                Include classified spans with both IndexLoad and IndexEviction.
    --incomplete              Include classified spans missing either IndexLoad or IndexEviction.
    --group-by <attributes>   Group spans by a comma-separated list of event
                              or derived attributes, such as NodeKind or
                              meta.Path. May be repeated to define nested
                              sub-groupings. If omitted, output is an aggregate
                              subtotal across all observed nodes.
)!!!";
        out.put(strlen(usage), usage);
    }
};

IEvToolCommand* createIndexCacheSpanCommand()
{
    return new CEvtIndexCacheSpanCommand;
}