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
#include "eventindexplot.h"
#include "evtindex.hpp"
#include "jevent.hpp"
#include "jptree.hpp"
#include "jstring.hpp"

// Connector between the CLI and the index plot operation.
class CEvtIndexPlotCommand : public TEventConsumingCommand<CIndexPlotOp>
{
protected:
    bool hasConfig = false;

public:
    CEvtIndexPlotCommand()
    {
        op.setOutput(consoleOut());
    }

    virtual bool acceptKVOption(const char* key, const char* value) override
    {
        if (streq(key, "config"))
        {
            if (hasConfig)
            {
                VStringBuffer msg("Error: Configuration file already specified: %s\n", value);
                consoleErr().put(msg.length(), msg.str());
                return false;
            }
            Owned<IPropertyTree> config = loadConfiguration(value);
            if (!config)
            {
                VStringBuffer msg("Error: Failed to load configuration file: %s\n", value);
                consoleErr().put(msg.length(), msg.str());
                return false;
            }
            op.setOpConfig(*config);
            hasConfig = true;
            return true;
        }
        return CEvToolCommand::acceptKVOption(key, value);
    }

    virtual bool acceptParameter(const char* arg) override
    {
        if (op.hasInputPath())
        {
            VStringBuffer msg("Error: Event file already specified: %s\n", arg);
            consoleErr().put(msg.length(), msg.str());
            return false;
        }
        op.setInputPath(arg);
        return true;
    }

    virtual const char* getBriefDescription() const override
    {
        return "generate plot data sets from index events using the given configuration";
    }

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Generate 2D and 3D plot data sets from index events using the given
configuration. 2D plots are defined by an x-axis and a value selector. 3D plots
are defined by an x-axis, a y-axis, and a value selector. Multiple plot data
sets using the same axis and value selector definitions, but changing other
model and filter configurations are supported.
)!!!";
    }
    
    virtual void usageSyntax(StringBuffer& helpText) override
    {
        constexpr const char* usageStr =
R"!!!(--config=<filename> [<filename>]
)!!!";
        helpText.append(usageStr);
    }

    virtual void usageFilters(IBufferedSerialOutputStream& out) override
    {
        // expect filters to be specified in the configuration file
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr =
R"!!!(    --config=<filename>       Required. YAML/XML/JSON configuration file
                              specifying plot parameters and data sources.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        CEvToolCommand::usageOptions(out);
        out.put(usageStrLength, usageStr);
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
The <filename> parameter is unused when specified in the configuration file. It
is required when not specified in the configuration file.
)!!!";
        size32_t usageStrLength = size32_t(strlen(usageStr));
        out.put(usageStrLength, usageStr);
    }
};

IEvToolCommand* createIndexPlotCommand()
{
    return new CEvtIndexPlotCommand();
}