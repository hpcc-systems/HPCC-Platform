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
};

class event_decl CIndexFileSummary : private IEventVisitor, public CEventConsumingOp
{
protected: // IEventVisitor
    virtual bool visitFile(const char *filename, uint32_t version) override;
    virtual bool visitEvent(CEvent& event) override;
    virtual void departFile(uint32_t bytesRead) override;
public: // CEventConsumingOp
    virtual bool doOp() override;
public:
    IMPLEMENT_IINTERFACE;
    void setSummarization(IndexSummarization value) { summarization = value; }
protected:
    void summarizeByFile();
    void summarizeByNodeKind();
    void summarizeByNode();
    void appendCSVColumn(StringBuffer& line, const char* value);
    void appendCSVColumn(StringBuffer& line, __uint64 value);
    void outputLine(StringBuffer& line);
protected:
    struct NodeStats
    {
        struct Bucket
        {
            __uint64 total{0};
            uint32_t count{0};
            uint32_t min{UINT32_MAX};
            uint32_t max{0};
        };
        enum ReadBucket
        {
            PageCache,
            LocalFile,
            RemoteFile,
            NumBuckets
        };
        uint32_t inMemorySize{0};
        uint32_t hits{0};
        uint32_t misses{0};
        uint32_t loads{0};
        uint32_t payloads{0};
        uint32_t evictions{0};
        Bucket readTime[NumBuckets];
        Bucket expandTime;
    };
    using Cache = std::unordered_map<IndexHashKey, NodeStats, IndexHashKeyHash>;
    using FileInfo = std::map<unsigned, StringAttr>;
    static constexpr __uint64 readBucketBoundary[NodeStats::NumBuckets] = {20'000, 400'000, UINT64_MAX};
    FileInfo fileInfo;
    IndexSummarization summarization{IndexSummarization::byFile};
    Cache stats[NumNodeKinds];
};
