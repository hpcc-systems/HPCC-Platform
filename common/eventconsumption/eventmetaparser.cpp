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

#include "eventmetaparser.hpp"
#include "jevent.hpp"

void CMetaInfoState::CCollector::configure(const IPropertyTree& config)
{
}

bool CMetaInfoState::CCollector::visitEvent(CEvent& event)
{
    switch (event.queryType())
    {
    case MetaFileInformation:
        {
            __uint64 fileId = event.queryNumericValue(EvAttrFileId);
            const char* path = event.queryTextValue(EvAttrPath);
            if (!isEmptyString(path))
            {
                metaState->fileIdToPath[fileId].set(path);
                metaState->pathToFileId[path] = fileId;
            }
        }
        break;
    case EventQueryStart:
        {
            if (event.hasAttribute(EvAttrEventTraceId) && event.hasAttribute(EvAttrServiceName))
            {
                const char* traceId = event.queryTextValue(EvAttrEventTraceId);
                const char* serviceName = event.queryTextValue(EvAttrServiceName);
                if (!isEmptyString(traceId) && !isEmptyString(serviceName))
                    metaState->traceIdToService.emplace(traceId, serviceName);
            }
        }
        break;
    }

    // Always forward to next link in chain
    if (nextLink)
        return nextLink->visitEvent(event);
    return true;
}

CMetaInfoState::CCollector::CCollector(CMetaInfoState& _metaState)
    : metaState(&_metaState)
{
}

IEventVisitationLink* CMetaInfoState::getCollector()
{
    return new CCollector(*this);
}

const char* CMetaInfoState::queryFilePath(__uint64 fileId) const
{
    auto it = fileIdToPath.find(fileId);
    return (it != fileIdToPath.end()) ? it->second.str() : nullptr;
}

bool CMetaInfoState::hasFileMapping(__uint64 fileId) const
{
    return fileIdToPath.find(fileId) != fileIdToPath.end();
}

void CMetaInfoState::clearFileMappings()
{
    fileIdToPath.clear();
    pathToFileId.clear();
}

const char* CMetaInfoState::queryServiceName(const char* traceId) const
{
    if (!traceId || !*traceId)
        return nullptr;
    auto it = traceIdToService.find(traceId);
    return (it != traceIdToService.end()) ? it->second.c_str() : nullptr;
}

bool CMetaInfoState::hasServiceMapping(const char* traceId) const
{
    if (!traceId || !*traceId)
        return false;
    return traceIdToService.find(traceId) != traceIdToService.end();
}

void CMetaInfoState::clearServiceMappings()
{
    traceIdToService.clear();
}

void CMetaInfoState::clearAll()
{
    clearFileMappings();
    clearServiceMappings();
}

/**
 * Returns the fileId for the given path.
 * Returns 0 if the input path is empty.
 * Returns UINT64_MAX if the path is not found.
 */
__uint64 CMetaInfoState::queryFileIdByPath(const char* path) const
{
    if (isEmptyString(path))
        return 0;
    auto it = pathToFileId.find(std::string(path));
    return (it != pathToFileId.end()) ? it->second : UINT64_MAX;
}