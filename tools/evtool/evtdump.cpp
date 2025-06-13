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
#include "eventdump.h"
#include "jevent.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <map>
#include <set>

// Connector between the CLI and the logic of dumping an event file's data as text.
class CEvtDumpCommand : public TEventConsumingCommand<CDumpEventsOp>
{
public:
    virtual bool acceptTerseOption(char opt) override
    {
        switch (opt)
        {
        case 'j':
            op.setFormat(OutputFormat::json);
            break;
        case 'x':
            op.setFormat(OutputFormat::xml);
            break;
        case 'y':
            op.setFormat(OutputFormat::yaml);
            break;
        case 'c':
            op.setFormat(OutputFormat::csv);
            break;
        case 'p':
            op.setFormat(OutputFormat::tree);
            break;
        default:
            return TEventConsumingCommand<CDumpEventsOp>::acceptTerseOption(opt);
        }
        return true;
    }

    virtual void usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CDumpEventsOp>::usageSyntax(argc, argv, pos, out);
        static const char* usageStr =
R"!!!([options] [filters] <filename>
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageSynopsis(IBufferedSerialOutputStream& out) override
    {
        static const char* usageStr = R"!!!(
Parse a binary event file and write its contents to standard output.
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CDumpEventsOp>::usageOptions(out);
        static const char* usageStr =
R"!!!(    -c                        Output as comma separated values.
    -j                        Output as JSON.
    -x                        Output as XML.
    -y                        Output as YAML.
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        static const char* usageStr = R"!!!(
Structured output would, if represented in a property tree, resemble:
  EventFile
    ├── Header
    |   ├── @filename
    |   ├── @version
    ├── Event
    |   ├── @name
    |   └── ... (other event properties)
    ├── ... (more events)
    └── Footer
        └── @bytesRead

CSV output includes columns for event name, plus one for each event attribute
used by the event recorder.
)!!!";
        static size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

// Create a file dump command instance as needed.
IEvToolCommand* createDumpCommand()
{
    return new CEvtDumpCommand();
}
