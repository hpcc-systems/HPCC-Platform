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

#include "eventvisitor.h"
#include "jstring.hpp"
#include <map>
#include <unordered_map>
#include <set>
#include <string>

// Visitor that parses and caches file ID to path mappings and trace ID to service name mappings
class event_decl CMetaInfoState : public CInterface
{
public:
    struct IndexFileProperties
    {
        Owned<String> path;
        uint32_t id;

        IndexFileProperties(String* _path, uint32_t _id) : path(_path), id(_id) {}
    };

private:
    class event_decl CCollector : public CInterfaceOf<IEventVisitationLink>
    {
    public: // IEventVisitor
        virtual bool visitFile(const char* filename, uint32_t version) override;
        virtual bool visitEvent(CEvent& event) override;
        virtual void departFile(uint32_t bytesRead) override;
    public: // IEventVisitationLink
        virtual void setNextLink(IEventVisitor& visitor) override;
        virtual void configure(const IPropertyTree& config) override;
    public:
        CCollector(CMetaInfoState& _metaState);
    private:
        Linked<CMetaInfoState> metaState;
        Linked<IEventVisitor> nextLink;
    };

public:
    IEventVisitationLink* getCollector();
    // Accessor functions for file ID to path mappings
    const char* queryFilePath(__uint64 fileId) const;
    bool hasFileMapping(__uint64 fileId) const;
    void clearFileMappings();

    // Accessor functions for trace ID to service name mappings
    const char* queryServiceName(const char* traceId) const;
    bool hasServiceMapping(const char* traceId) const;
    void clearServiceMappings();

    // Clear all mappings
    void clearAll();

private:
    void onFile(const char* filename, uint32_t version);
    void onEvent(CEvent& event);
    uint32_t generateSourceFileId(const CEvent& event);
    uint32_t generateRuntimeFileId(const CEvent& event);

private:
    size_t sourceCount{0};

    // File ID to path mappings (from MetaFileInformation events)
    std::set<IndexFileProperties, std::less<>> indexFiles;
    std::unordered_map<size_t, const IndexFileProperties*> sourceToProps;
    std::unordered_map<__uint64, Owned<String>> fileIdToPath;

    // Trace ID to service name mappings (from EventQueryStart events)
    std::unordered_map<std::string, std::string> traceIdToService;
};
