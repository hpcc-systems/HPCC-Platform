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
#include "eventiterator.h"
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

// Record configured events to a configured location. The input property tree must conform to the
// event format supported by CPropertyTreeEvents, and must include the following required root
// element attribute(s), and may include the following optional root element attribute(s).:
// - name: required recording output file path
// - options: optional recording options string
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
        CPropertyTreeEvents eventsIt(*input);
        CEvent event;
        while (eventsIt.nextEvent(event))
            recorder.recordEvent(event);
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

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Create a binary event file containing the events specified in an external
configuration file. The configuration may be either XML, JSON, or YAML markup.
)!!!";
    }

    virtual const char* getBriefDescription() const override
    {
        return "create a binary event file from a text file";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append(R"!!!([options] <filename>
)!!!");
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
