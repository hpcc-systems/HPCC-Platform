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
#include "evtptree.hpp"
#include "jevent.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <iostream>

// Future enhancements may include:
// - an output format selector to choose between jSON, YAML, CSV, et al
// - support for multiple input files
// - support for event filtering

// Supported dump output formats.
enum class OutputFormat : byte
{
    text,
    xml_p, // XML from property tree
    xml_d, // XML directly from visited content
};

// Extension of CPTreeEventVisitor in which the destructor outputs the complete property tree in a
// requested structured format.
class DumpPTreeEventVisitor : public CPTreeEventVisitor
{
protected:
    OutputFormat format;
public:
    DumpPTreeEventVisitor(std::ostream& out, OutputFormat _format)
        : CPTreeEventVisitor(out)
        , format(_format)
    {
    }
    ~DumpPTreeEventVisitor()
    {
        if (tree)
        {
            StringBuffer markup;
            switch (format)
            {
            case OutputFormat::xml_p:
                toXML(tree, markup);
                break;
            default:
                std::cerr << "unsupported output format " << byte(format) << std::endl;
                break;
            }
            out << markup.str() << std::endl;
        }
    }
};

// Implementation of IEventVisitor constructs and outputs one XML element at a time. The formatted
// output resembles that produced by DumpPTreeEventVisitor, with the exception that element and
// attribute ordering may differ.
class DumpXMLEventVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        appendXMLOpenTag(markup, EVT_PTREE_ROOT, nullptr, false);
        attribute(EVT_PTREE_FILE_NAME, filename);
        markup.append('>');
        appendXMLOpenTag(markup, EVT_PTREE_HEADER, nullptr, false);
        attribute(EVT_PTREE_FILE_VERSION, version);
        return true;
    }
    virtual Continuation visitEvent(EventType id) override
    {
        endElement();
        appendXMLOpenTag(markup, EVT_PTREE_EVENT, nullptr, false);
        appendXMLAttr(markup, EVT_PTREE_EVENT_NAME, queryEventName(id));
        attribute(EVT_PTREE_EVENT_ID, uint64_t(id));
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id) override
    {
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, const char* value) override
    {
        if (EvAttrSysOption == id)
            attribute(value, true);
        else
            attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, bool value) override
    {
        attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, uint8_t value) override
    {
        attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, uint16_t value) override
    {
        attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, uint32_t value) override
    {
        attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual Continuation visitAttribute(EventAttr id, uint64_t value) override
    {
        attribute(queryEventAttributeName(id), value);
        return visitContinue;
    }
    virtual void leaveFile(uint32_t bytesRead) override
    {
        endElement();
        appendXMLOpenTag(markup, EVT_PTREE_FOOTER, nullptr, false);
        attribute(EVT_PTREE_FILE_BYTES_READ, bytesRead);
        endElement();
        appendXMLCloseTag(markup, EVT_PTREE_ROOT);
        out << markup.str() << std::endl;
    }
protected:
    std::ostream& out;
    StringBuffer markup;
public:
    DumpXMLEventVisitor(std::ostream& _out) : out(_out)
    {
    }
    void attribute(const char* name, uint64_t value)
    {
        char buf[21];
        sprintf(buf, "%lu", value);
        appendXMLAttr(markup, name, buf);
    }
    void attribute(const char* name, const char* value)
    {
        appendXMLAttr(markup, name, value);
    }
    template <typename T>
    void attribute(const char* name, T value)
    {
        markup.append(' ').append(name).append("=\"").append(value).append('"');
    }
    void endElement()
    {
        if (!markup.isEmpty())
        {
            markup.append("/>");
            out << markup.str();
            markup.clear();
        }
    }
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
        case OutputFormat::text:
            visitor.setown(createVisitTrackingEventVisitor(*out));
            break;
        case OutputFormat::json_p:
        case OutputFormat::xml_p:
        case OutputFormat::yaml_p:
            visitor.setown(new DumpPTreeEventVisitor(*out, format));
            break;
        case OutputFormat::xml_d:
            visitor.setown(new DumpXMLEventVisitor(*out));
            break;
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
            case 'x':
                efd.setFormat(OutputFormat::xml_p);
                accepted = true;
                break;
            case 'X':
                efd.setFormat(OutputFormat::xml_d);
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
        out << "  -x              output a property tree as XML" << std::endl;
        out << "  -X              output directly as XML" << std::endl;
        out << "  <filename>      full path to a binary event data file" << std::endl;
        out << std::endl;
    }
protected:
    EventFileDump efd;
};

// Create a file dump command instance as needed.
IEvToolCommand* createDumpCommand()
{
    return new CEvtDumpCommand();
}
