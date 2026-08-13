/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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
#include "eventdescribe.h"

class CEvtDescribeCommand : public TEvtCLIConnector<CDescribeEventsOp>
{
public:
    CEvtDescribeCommand()
    {
        op.setFormat(DescribeOutputFormat::yaml);
        op.setSectionOverrides(DescribeSection::none);
        op.setOutput(consoleOut());
    }

protected:
    virtual bool acceptTerseOption(char opt) override
    {
        switch (opt)
        {
        case 'x':
            op.setFormat(DescribeOutputFormat::xml);
            break;
        case 'j':
            op.setFormat(DescribeOutputFormat::json);
            break;
        case 'y':
            op.setFormat(DescribeOutputFormat::yaml);
            break;
        case 'c':
            op.addSectionOverride(DescribeSection::contexts);
            break;
        case 'e':
            op.addSectionOverride(DescribeSection::events);
            break;
        case 'a':
            op.addSectionOverride(DescribeSection::attributes);
            break;
        default:
            return TEvtCLIConnector<CDescribeEventsOp>::acceptTerseOption(opt);
        }
        return true;
    }

    virtual bool acceptParameter(const char* arg) override
    {
        throw makeStringExceptionV(0, "describe: input files are not supported: %s", arg);
    }

    virtual const char* getVerboseDescription() const override
    {
        return R"!!!(Describe known event contexts, events, and attributes.
Derived meta.* names are included in the attribute section. Output can be
narrowed to one or more sections and emitted as XML, JSON, or YAML.
)!!!";
    }

    virtual const char* getBriefDescription() const override
    {
        return "describe event names and sections";
    }

    virtual void usageSyntax(StringBuffer& helpText) override
    {
        helpText.append(R"!!!([options]
)!!!");
    }

    virtual void usageOptions(IBufferedSerialOutputStream& out) override
    {
        TEvtCLIConnector<CDescribeEventsOp>::usageOptions(out);
        constexpr const char* usageStr =
R"!!!(    -x, -j, -y                Output format: XML, JSON, or YAML.
                              If more than one output option is supplied,
                              the last one wins.
    -c                        Include contexts section.
    -e                        Include events section.
    -a                        Include attributes section.
)!!!";
        out.put(size32_t(strlen(usageStr)), usageStr);
    }

    virtual void usageParameters(IBufferedSerialOutputStream& out) override
    {
        // Intentionally empty: this phase documents option-only usage.
    }

    virtual void usageDetails(IBufferedSerialOutputStream& out) override
    {
        constexpr const char* usageStr = R"!!!(
    Used without input event files. All observable events, contexts, and
    attributes are described. Derived meta.* names are included in the
    attribute section.

If none of -x, -j, or -y are provided, YAML output is used.

    If none of -c, -e, or -a are provided, all sections are emitted. If one
or more section options are provided, output is limited to the selected
sections.
)!!!";
        out.put(size32_t(strlen(usageStr)), usageStr);
    }
};

IEvToolCommand* createDescribeCommand()
{
    return new CEvtDescribeCommand();
}
