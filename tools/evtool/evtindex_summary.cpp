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
            default:
                return false;
        }
        return true;
    }

    virtual void usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexFileSummary>::usageSyntax(argc, argv, pos, out);
        constexpr const char* usageStr =
R"!!!([options] [filters] <filename>
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageSynopsis(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Summarize the index events in a binary event file. Activity can be aggregate
by either index file ID, node kind, or individual node (as identified by file
ID and file offset). One line of output is produced for each event group.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexFileSummary>::usageOptions(out);
        constexpr const char* usageStr =
R"!!!(    -f                        Summarize actibity by file.
    -k                        Summarize actibity by node kind.
    -n                        Summarize actibity by node.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

IEvToolCommand* createIndexSummaryCommand()
{
    return new CEvtIndexSummaryCommand();
}
