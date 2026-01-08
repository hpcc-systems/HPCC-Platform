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
#include "eventindexmodel.hpp"
#include "eventoperation.h"
#include "eventvisitor.h"
#include <unordered_map>

enum class IndexSummarization
{
    byFile,
    byNodeKind,
    byNode,
    byTrace,
    byService,
};

class event_decl CIndexFileSummary : public CEventConsumingOp
{
public: // CEventConsumingOp
    virtual bool doOp() override;
public:
    void setSummarization(IndexSummarization value) { summarization = value; }
protected:
    IndexSummarization summarization = IndexSummarization::byFile;
};
