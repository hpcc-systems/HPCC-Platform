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

#pragma once

#include "eventconsumption.h"
#include "eventindex.hpp"
#include "eventgrouping.hpp"
#include "eventindexmodel.hpp"
#include "eventoperation.h"
#include "eventvisitor.h"
#include <unordered_map>
#include <string>
#include <vector>

enum class IndexSummarization
{
    byFile,
    byNodeKind,
    byNode,
    byTrace,
    byService,
    byGroup // Indicates using the generic grouping framework
};

class event_decl CIndexFileSummary : public CEventConsumingOp
{
public: // CEventConsumingOp
    virtual bool doOp() override;
protected:
    virtual bool preScanRequired() const override;
public:
    void setSummarization(IndexSummarization value) { summarization = value; }
    void addGroupAttribute(const std::vector<std::string>& attrs)
    {
        std::vector<std::string> strippedAttrs;
        std::vector<GroupAttribute> ids;
        for (const auto& a : attrs)
        {
            ids.push_back(GroupAttributeExtractor::parseAttribute(a.c_str()));
            size_t slashPos = a.find('/');
            if (slashPos != std::string::npos)
                strippedAttrs.push_back(a.substr(0, slashPos));
            else
                strippedAttrs.push_back(a);
        }
        groupAttributes.push_back(strippedAttrs);
        groupAttributeIds.push_back(ids);
        setSummarization(IndexSummarization::byGroup);
    }
protected:
    IndexSummarization summarization = IndexSummarization::byGroup;
    std::vector<std::vector<std::string>> groupAttributes;
    std::vector<std::vector<GroupAttribute>> groupAttributeIds;
};
