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
#include <iostream>
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

// Open and parse a binary event file.
class EventFileDump
{
public:
    // Cache the filename to be opened and parsed.
    void setFile(const char* filename)
    {
        file.set(filename);
    }

    // Cache the requested output format.
    void setFormat(OutputFormat format)
    {
        this->format = format;
    }

    // Cache the output stream to receive the parsed data.
    // - a command line request might use std::cout
    // - a unit test might use a std::stringstream
    // - an ESP might select a string stream or something else
    void setOutput(std::ostream& out)
    {
        this->out = &out;
    }

    // Return true if a valid request can be attempted.
    bool ready() const
    {
        return !file.isEmpty() && out;
    }

    // Perform the requested action.
    bool dump()
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
                if (readEvents(file.str(), creator->queryVisitor()))
                {
                    StringBuffer yaml;
                    toYAML(creator->queryTree(), yaml, 2, 0);
                    *out << yaml.str() << std::endl;
                    return true;
                }
                return false;
            }
        default:
            throw makeStringExceptionV(-1, "unsupported output format: %d", (int)format);
        }
        return readEvents(file.str(), *visitor);
    }
protected:
    StringAttr file;
    OutputFormat format = OutputFormat::text;
    std::ostream* out = nullptr;
};

// Connector between the command line tool and the logic of dumping an event file.
class CEvtDumpCommand : public CEvToolCommand
{
public:
    virtual bool acceptTerseOption(char opt) override
    {
        bool accepted = CEvToolCommand::acceptTerseOption(opt);
        if (!accepted)
        {
            switch (opt)
            {
            case 'j':
                efd.setFormat(OutputFormat::json);
                accepted = true;
                break;
            case 'x':
                efd.setFormat(OutputFormat::xml);
                accepted = true;
                break;
            case 'y':
                efd.setFormat(OutputFormat::yaml);
                accepted = true;
                break;
            case 'c':
                efd.setFormat(OutputFormat::csv);
                accepted = true;
                break;
            case 'p':
                efd.setFormat(OutputFormat::tree);
                accepted = true;
                break;
            default:
                break;
            }
        }
        return accepted;
    }
    virtual bool acceptParameter(const char* arg) override
    {
        efd.setFile(arg);
        return true;
    }
    virtual bool isGoodRequest() override
    {
        efd.setOutput(std::cout);
        return efd.ready();
    }
    virtual int doRequest() override
    {
        try
        {
            return efd.dump() ? 0 : 1;
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            std::cerr << msg.str() << std::endl;
            return 1;
        }
    }
    virtual void usage(int argc, const char* argv[], int pos, std::ostream& out) override
    {
        usagePrefix(argc, argv, pos, out);
        out << "[options] <filename>" << std::endl << std::endl;
        out << "Parse a binary event file and write its contents to standard output." << std::endl << std::endl;
        out << "  -?, -h, --help  show this help message and exit" << std::endl;
        out << "  -c              output as comma separated values" << std::endl;
        out << "  -j              output as JSON" << std::endl;
        out << "  -x              output as XML" << std::endl;
        out << "  -y              output as YAML" << std::endl;
        out << "  <filename>      full path to a binary event data file" << std::endl;
        out << std::endl;
        out << "Structured output would, if represented in a property tree, resemble:" << std::endl;
        out << "  EventFile" << std::endl;
        out << "    ├── Header" << std::endl;
        out << "    |   ├── @filename" << std::endl;
        out << "    |   ├── @version" << std::endl;
        out << "    |   └── ... (other header properties)" << std::endl;
        out << "    ├── Event" << std::endl;
        out << "    |   ├── @name" << std::endl;
        out << "    |   ├── @id" << std::endl;
        out << "    |   └── ... (other event properties)" << std::endl;
        out << "    ├── ... (more events)" << std::endl;
        out << "    └── Footer" << std::endl;
        out << "        ├── @bytesRead" << std::endl;
        out << std::endl;
        out << "CSV output includes columns for event name and ID, plus one for each" << std::endl;
        out << "event attribute used by the event recorder." << std::endl;
    }
    CEvtDumpCommand()
    {
        efd.setOutput(std::cout);
    }
protected:
    EventFileDump efd;
};

// Create a file dump command instance as needed.
IEvToolCommand* createDumpCommand()
{
    return new CEvtDumpCommand();
}
