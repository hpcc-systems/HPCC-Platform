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
#include "jptree.hpp"
#include "jstream.hpp"

// The simulation command is included as an aid in the development and testing of evtool. It may
// be impacted by changes to the event recording API. Authors of changes to that API are requiired
// to ensure the command can compile, but are not required to ensure it functions correctly. Tool
// maintainers are solely responsible for command functionality.

// Initial version supports a single list of events to be recorded from one thread.
// Future enhancements may include:
// - support events per threads
// - support events per trace ID (requires enabling tracing)

// Record configured events to a configured location.
class CSimulateEventsOp
{
public:
    bool ready() const
    {
        return input != nullptr;
    }

    // Perform the requested action.
    bool doOp()
    {
        const char* name = input->queryProp("@name");
        if (!name)
            throw makeStringException(-1, "missing binary output file name");
        const char* options = input->queryProp("@options");
        EventRecorder& recorder = queryRecorder();
        if (!recorder.startRecording(options, name, false))
            throw makeStringException(-1, "failed to start event recording");
        Owned<IPropertyTreeIterator> it = input->getElements("Event");
        ForEach(*it)
        {
            const IPropertyTree& evt = it->query();
            name = evt.queryProp("@name");
            if (streq(name, "IndexLookup"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropInt64("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                bool hit = evt.getPropBool("@hit");
                size32_t size = (size32_t)evt.getPropInt("@size");
                recorder.recordIndexLookup(fileId, offset, nodeKind, hit, size);
            }
            else if (streq(name, "IndexLoad"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropInt64("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                size32_t size = (size32_t)evt.getPropInt("@size");
                __uint64 elapsedTime = (__uint64)evt.getPropInt64("@elapsed");
                __uint64 readTime = (__uint64)evt.getPropInt64("@read");
                recorder.recordIndexLoad(fileId, offset, nodeKind, size, elapsedTime, readTime);
            }
            else if (streq(name, "IndexEviction"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                offset_t offset = (offset_t)evt.getPropInt64("@offset");
                byte nodeKind = (byte)evt.getPropInt("@nodeKind");
                size32_t size = (size32_t)evt.getPropInt("@size");
                recorder.recordIndexEviction(fileId, offset, nodeKind, size);
            }
            else if (streq(name, "DaliChangeMode"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliChangeMode(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliCommit"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliCommit(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliConnect"))
            {
                const char* path = evt.queryProp("@path");
                __uint64 id = (__uint64)evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliConnect(path, id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliEnsureLocal"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliEnsureLocal(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliGet"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliGet(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliGetChildren"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliGetChildren(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliGetChildrenFor"))
            {
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliGetChildrenFor(id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliGetElements"))
            {
                const char* path = evt.queryProp("@path");
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                size32_t dataSize = (size32_t)evt.getPropInt("@dataSize");
                recorder.recordDaliGetElements(path, id, elapsedNs, dataSize);
            }
            else if (streq(name, "DaliSubscribe"))
            {
                const char* path = evt.queryProp("@path");
                __int64 id = evt.getPropInt64("@id");
                __uint64 elapsedNs = (__uint64)evt.getPropInt64("@elapsedNs");
                recorder.recordDaliSubscribe(path, id, elapsedNs);
            }
            else if (streq(name, "FileInformation"))
            {
                unsigned fileId = (unsigned)evt.getPropInt("@fileId");
                const char* filename = evt.queryProp("@filename");
                recorder.recordFileInformation(fileId, filename);
            }
            else if (streq(name, "RecordingActive"))
            {
                // Always resume immediately after pausing. The simulation does not verify that
                // pausing will suppress the recording of future events. It verifies only that the
                // pause and resume events themselves are recorded when expected, based on API
                // input parameters.
                bool recordPause = evt.getPropBool("@recordPause");
                bool recordResume = evt.getPropBool("@recordResume");
                recorder.pauseRecording(true, recordPause);
                recorder.pauseRecording(false, recordResume);
            }
            else
                throw makeStringExceptionV(-1, "unknown event: %s", name);;
        }
        if (!recorder.stopRecording(nullptr))
            throw makeStringException(-1, "failed to stop event recording");
        return true;
    }

    void setInput(const IPropertyTree* _input)
    {
        input.setown(_input);
    }

protected:
    Owned<const IPropertyTree> input;
};

// Extension of TEvtCLIConnector responsible for the creation of user configured events. One
// parameter is accepted, the full path to an XML/JSON/YAML file containing the event
// specifications.
class CEvtSimCommand : public TEvtCLIConnector<CSimulateEventsOp>
{
public:
    virtual bool acceptParameter(const char* parameter) override
    {
        op.setInput(loadConfiguration(parameter));
        return true;
    }

    virtual void usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        CEvToolCommand::usageSyntax(argc, argv, pos, out);
        constexpr const char* usageStr =
R"!!!([options] <filename>
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageSynopsis(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Create a binary event file containing the events specified in an external
configuration file. The configuration may be either XML, JSON, or YAML markup.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageParameters(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Parameters:
    <filename>                Full path to an XML/JSON/YAML file containing
                              simulated event specifications.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

// Create an event simulation command instance as needed.
IEvToolCommand* createSimCommand()
{
    return new CEvtSimCommand();
}
