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
#include "jevent.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <map>
#include <set>

// Future enhancements may include:
// - support for multiple input files
// - support for event filtering

// Supported dump output formats.
enum class OutputFormat : byte
{
    text,
    json,
    xml,
    yaml,
    csv,
    tree,
};

// Open and parse a binary event file, generating any one of several supported output formats. As
// an extension of CEventConsumingOp, the public interface adds:
// - `void setFormat(OutputFormat)`: set the output format
// - `bool filterEventType(const char* eventNames)`: add an event type filter to the operation
class CDumpEventsOp : public CEventConsumingOp
{
public:
    // Cache the requested output format.
    void setFormat(OutputFormat format)
    {
        this->format = format;
    }

    // Perform the requested action.
    bool doOp()
    {
        Owned<IEventVisitor> visitor;
        switch (format)
        {
        case OutputFormat::json:
            visitor.setown(createDumpJSONEventVisitor(*out));
            break;
        case OutputFormat::text:
            visitor.setown(createDumpTextEventVisitor(*out));
            break;
        case OutputFormat::xml:
            visitor.setown(createDumpXMLEventVisitor(*out));
            break;
        case OutputFormat::yaml:
            visitor.setown(createDumpYAMLEventVisitor(*out));
            break;
        case OutputFormat::csv:
            visitor.setown(createDumpCSVEventVisitor(*out));
            break;
        case OutputFormat::tree:
            {
                Owned<IEventPTreeCreator> creator = createEventPTreeCreator();
                if (traverseEvents(inputPath.str(), creator->queryVisitor()))
                {
                    StringBuffer yaml;
                    toYAML(creator->queryTree(), yaml, 2, 0);
                    out->put(yaml.length(), yaml.str());
                    out->put(1, "\n");
                    return true;
                }
                return false;
            }
        default:
            throw makeStringExceptionV(-1, "unsupported output format: %d", (int)format);
        }
        return traverseEvents(inputPath.str(), *visitor);
    }

protected:
    OutputFormat format = OutputFormat::text;
};

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
    |   └── ... (other header properties)
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
