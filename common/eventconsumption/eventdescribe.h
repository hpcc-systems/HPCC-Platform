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

#pragma once

#include "eventconsumption.h"
#include "eventoperation.h"
#include "jlib.hpp"
#include <array>

enum class DescribeOutputFormat : byte
{
    xml,
    json,
    yaml,
};

enum class DescribeSection : unsigned
{
    none = 0x00,
    contexts = 0x01,
    events = 0x02,
    attributes = 0x04,
};
BITMASK_ENUM(DescribeSection);

// Build an event capability description for names observable in event files.
class event_decl CDescribeEventsOp : public CEventConsumingOp
{
public:
    void setFormat(DescribeOutputFormat _format);
    void selectSection(DescribeSection section);

    virtual bool ready() const override;
    virtual bool doOp() override;

private:
    void appendDescriptionTree(IPropertyTree& description);
    bool isSectionEnabled(DescribeSection section) const;

private:
    DescribeOutputFormat format{DescribeOutputFormat::yaml};
    DescribeSection selectedSections{DescribeSection::none};
    bool explicitSectionSelection{false};
};
