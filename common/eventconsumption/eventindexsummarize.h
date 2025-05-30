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
#include "eventoperation.h"
#include "eventvisitor.h"

class event_decl CIndexFileSummary : public IEventAttributeVisitor, public CEventConsumingOp
{
public: // IEventAttributeVisitor
    virtual bool visitFile(const char *filename, uint32_t version) override;
    virtual bool visitEvent(CEvent& event) override;
    virtual Continuation visitEvent(EventType id) override;
    virtual Continuation visitAttribute(EventAttr id, const char *value) override;
    virtual Continuation visitAttribute(EventAttr id, bool value) override;
    virtual Continuation visitAttribute(EventAttr id, __uint64 value) override;
    virtual bool departEvent() override;
    virtual void departFile(uint32_t bytesRead) override;
public: // CEventConsumingOp
    virtual bool doOp() override;
public:
    IMPLEMENT_IINTERFACE;
protected:
    struct NodeKindSummary;
    NodeKindSummary& nodeSummary();
    void appendCSVColumn(StringBuffer& line, const char* value);
    void appendCSVColumn(StringBuffer& line, __uint64 value);
protected:
    using FileInfo = std::map<unsigned, StringAttr>;
    struct NodeKindSummary
    {
        __uint64 hits{0};
        __uint64 misses{0};
        __uint64 loads{0};
        __uint64 loadedSize{0};
        __uint64 evictions{0};
        __uint64 evictedSize{0};
        __uint64 elapsed{0};
        __uint64 read{0};
    };
    struct FileSummary
    {
        NodeKindSummary branch;
        NodeKindSummary leaf;
    };
    using Summary = std::map<unsigned, FileSummary>;
    FileInfo fileInfo;
    Summary summary;
    EventType currentEvent{EventNone};
    __uint64 currentFileId{0};
    __uint64 currentNodeKind{0};
};
