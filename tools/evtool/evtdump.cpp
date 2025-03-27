/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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
#include "jstring.hpp"
#include <iostream>

// Initial version supports output as a sequence of visits. Future enhancements
// may include:
// - an output format selector to choose between XML, jSON, YAML, CSV, et al
// - support for multiple input files
// - support for event filtering

// Open and parse a binary event file.
class EventFileDump
{
public:
    // Cache the filename to be opened and parsed.
    void setFile(const char* filename)
    {
        file.set(filename);
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
        Owned<IEventVisitor> visitor = createVisitTrackingEventVisitor(*out);
        return readEvents(file.str(), *visitor);
    }
protected:
    StringAttr file;
    std::ostream* out = nullptr;
};

// Connector between the command line tool and the logic of dumping an event file.
class CEvtDumpCommand : public CEvToolCommand
{
public:
    virtual bool acceptParameter(const char* arg) override
    {
        efd.setFile(arg);
        return true;
    }
    virtual bool isGoodRequest() override
    {
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
        out << "  <filename>      full path to a binary event data file" << std::endl;
        out << std::endl;
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
