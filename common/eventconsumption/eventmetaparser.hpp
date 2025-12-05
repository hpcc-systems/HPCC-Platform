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
#include <string>

// Visitor that parses and caches file ID to path mappings and trace ID to service name mappings
class event_decl CMetaInfoState : public CInterfaceOf<IEventVisitationLink>
{
public:
    CMetaInfoState() = default;
    ~CMetaInfoState() = default;

    // IEventVisitationLink interface
    IMPLEMENT_IEVENTVISITATIONLINK
    virtual void configure(const IPropertyTree& config) override;
    virtual bool visitEvent(CEvent& event) override;

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

    // Reverse lookup functions for convenience
    __uint64 queryFileIdByPath(const char* path) const;

private:
    // File ID to path mappings (from MetaFileInformation events)
    std::unordered_map<__uint64, StringAttr> fileIdToPath;
    std::unordered_map<std::string, __uint64> pathToFileId; // For reverse lookup optimization

    // Trace ID to service name mappings (from EventQueryStart events)
    std::unordered_map<std::string, std::string> traceIdToService;
};