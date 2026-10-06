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

#include "eventgrouping.hpp"
#include "eventoperation.h"
#include <string>
#include <vector>

class event_decl CIndexCacheSpanOp : public CEventConsumingOp
{
public:
    virtual bool ready() const override;
    virtual bool preScanRequired() const override;
    virtual bool doOp() override;

    void addGroupAttribute(const std::vector<std::string>& attrs);
    void setIncludeComplete(bool value) { includeComplete = value; }
    void setIncludeIncomplete(bool value) { includeIncomplete = value; }

private:
    std::vector<std::vector<std::string>> groupAttributes;
    std::vector<std::vector<GroupAttribute>> groupAttributeIds;
    bool includeComplete{false};
    bool includeIncomplete{false};
};