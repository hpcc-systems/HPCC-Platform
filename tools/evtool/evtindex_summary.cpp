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
    virtual void usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CIndexFileSummary>::usageSyntax(argc, argv, pos, out);
        static const char* usageStr =
R"!!!([options] [filters] <filename>
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageSynopsis(IBufferedSerialOutputStream& out) override
    {
        static const char* usageStr = R"!!!(
Summarize the index events in a binary event file.
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

IEvToolCommand* createIndexSummaryCommand()
{
    return new CEvtIndexSummaryCommand();
}
