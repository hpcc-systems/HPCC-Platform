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
#include "jptree.hpp"
#include <iostream>

// The simulation command is included as an aid in the development and testing of evtool. It may
// be impacted by changes to the event recording API. Authors of changes to that API are requiired
// to ensure the command can compile, but are not required to ensure it functions correctly. Tool
// maintainers are solely responsible for command functionality.

// Initial version supports a single list of events to be recorded from one thread.
// Future enhancements may include:
// - support events per threads
// - support events per trace ID (requires enabling tracing)

// Record configured events to a configured location.
class EventFileSim
{
public:
    // Load configuration from a file.
    void setFile(const char* filename)
    {
        markup.clear().loadFile(filename);
    }

    // Accept a configuration string.
    void setMarkup(const char* markup)
    {
        this->markup.set(markup);
    }

    // Return true if a configuration has been provided.
    bool ready() const
    {
        return !markup.isEmpty();
    }

    // Perform the requested action.
    bool sim()
    {
        Owned<IPTree> tree;
        if (markup.charAt(0) == '<') // looks like XML
            tree.setown(createPTreeFromXMLString(markup));
        else // assume YAML
            tree.setown(createPTreeFromYAMLString(markup));
        if (!tree)
            throw makeStringException(-1, "invalid configuration");
        const char* name = tree->queryProp("@name");
        if (!name)
            throw makeStringException(-1, "missing binary output file name");
        const char* options = tree->queryProp("@options");
        EventRecorder& recorder = queryRecorder();
        if (!recorder.startRecording(options, name, false))
            throw makeStringException(-1, "failed to start event recording");
        Owned<IPTreeIterator> it = tree->getElements("Event");
        ForEach(*it)
        {
            const IPTree& evt = it->query();
            name = evt.queryProp("@name");
            if (streq(name, "IndexLookup"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropBool("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                bool hit = evt.getPropBool("@hit");
                recorder.recordIndexLookup(fileId, offset, nodeKind, hit);
            }
            else if (streq(name, "IndexLoad"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropBool("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                size32_t size = (size32_t)evt.getPropInt("@size");
                __uint64 elapsedTime = (__uint64)evt.getPropInt64("@elapsed");
                __uint64 readTime = (__uint64)evt.getPropInt64("@read");
                recorder.recordIndexLoad(fileId, offset, nodeKind, size, elapsedTime, readTime);
            }
            else if (streq(name, "IndexEviction"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropBool("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                size32_t size = (size32_t)evt.getPropInt("@size");
                recorder.recordIndexEviction(fileId, offset, nodeKind, size);
            }
            else if (streq(name, "DaliConnect"))
            {
                const char* path = evt.queryProp("@path");
                __uint64 id = (__uint64)evt.getPropInt64("@id");
                recorder.recordDaliConnect(path, id);
            }
            else if (streq(name, "DaliRead"))
            {
            }
            else if (streq(name, "DaliWrite"))
            {
            }
            else if (streq(name, "DaliDisconnect"))
            {
                __uint64 id = (__uint64)evt.getPropInt64("@id");
                recorder.recordDaliDisconnect(id);
            }
            else if (streq(name, "FileInformation"))
            {
            }
            else if (streq(name, "RecordingActive"))
            {
                bool pause = evt.getPropBool("@pause");
                bool resume = evt.getPropBool("@resume");
                recorder.pauseRecording(true, pause);
                recorder.pauseRecording(false, resume);
            }
            else
                throw makeStringExceptionV(-1, "unknown event: %s", name);;
        }
        if (!recorder.stopRecording())
            throw makeStringException(-1, "failed to stop event recording");
        return true;
    }
protected:
    StringBuffer markup;
};

// Connector between the command line tool and the logic of simulating events.
class CEvtSimCommand : public CEvToolCommand
{
    public:
    virtual bool acceptParameter(const char* arg) override
    {
        efs.setFile(arg);
        return true;
    }
    virtual bool isGoodRequest() override
    {
        return efs.ready();
    }
    virtual int doRequest() override
    {
        try
        {
            return efs.sim() ? 0 : 1;
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
        out << "Create a binary event file containing the events specified in an external" << std::endl;
        out << "configuration file. The configuration may use either XML or YAML formats." << std::endl << std::endl;
        out << "  -?, -h, --help  show this help message and exit" << std::endl;
        out << "  <filename>      full path to an XML or YAML file containing simulated" << std::endl;
        out << "                  events" << std::endl;
        out << std::endl;
    }
protected:
    EventFileSim efs;
};

// Create an event simulation command instance as needed.
IEvToolCommand* createSimCommand()
{
    return new CEvtSimCommand();
}
