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
#include "eventsaveas.h"

class CEvtSaveAsCommand : public TEventConsumingCommand<CEventSavingOp>
{
public:
    virtual int doRequest() override
    {
        try
        {
            if (op.doOp())
            {
                StringBuffer summary;
                op.getResults(summary);
                consoleOut().put(summary.length(), summary.str());
                consoleOut().put(1, "\n");
                return 0;
            }
            StringBuffer error("command execution failed\n");
            consoleErr().put(error.length(), error.str());
            return 1;
        }
        catch (IException* e)
        {
            StringBuffer msg("command execution exception: ");
            e->errorMessage(msg);
            e->Release();
            msg.append('\n');
            consoleErr().put(msg.length(), msg.str());
            return 1;
        }
    }

    virtual bool acceptKVOption(const char* key, const char* value) override
    {
        if (TEventConsumingCommand<CEventSavingOp>::acceptKVOption(key, value))
            return true;
        if (strieq(key, "output-path"))
        {
            op.setOutputPath(value);
            return true;
        }
        if (strieq(key, "recording-options"))
        {
            op.setOutputOptions(value);
            return true;
        }
        return false;
    }

    virtual void usageSyntax(int argc, const char* argv[], int pos, IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CEventSavingOp>::usageSyntax(argc, argv, pos, out);
        constexpr const char* usageStr =
R"!!!([options] [filters] <filename>
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageSynopsis(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Record visited events to a new binary event file. The output file path and
recording options are specified by --output-path and --recording-options, respectively.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEventConsumingCommand<CEventSavingOp>::usageOptions(out);
        constexpr const char* usageStr =
R"!!!(    --output-path             Output event file path.
    --recording-options       Event recording options string.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
Any input event stream, optionally constrained by filters modified
by models, is saved into a new binary event file.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

// Create a save-as command instance as needed.
IEvToolCommand* createSaveAsCommand()
{
    return new CEvtSaveAsCommand();
}
