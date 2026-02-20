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

bool operator < (const CMetaInfoState::IndexFileProperties& left, const CMetaInfoState::IndexFileProperties& right)
{
    return strcmp(left.path->str(), right.path->str()) < 0;
}

bool operator < (const char* left, const CMetaInfoState::IndexFileProperties& right)
{
    return strcmp(left, right.path->str()) < 0;
}

bool operator < (const CMetaInfoState::IndexFileProperties& left, const char* right)
{
    return strcmp(left.path->str(), right) < 0;
}

void CMetaInfoState::CCollector::setNextLink(IEventVisitor& visitor)
{
    nextLink.set(&visitor);
}

void CMetaInfoState::CCollector::configure(const IPropertyTree& config)
{
}

bool CMetaInfoState::CCollector::visitFile(const char* filename, uint32_t version)
{
    metaState->onFile(filename, version);
    if (nextLink)
        return nextLink->visitFile(filename, version);
    return true;
}

bool CMetaInfoState::CCollector::visitEvent(CEvent& event)
{
    metaState->onEvent(event);

    // Always forward to next link in chain
    if (nextLink)
        return nextLink->visitEvent(event);
    return true;
}

void CMetaInfoState::CCollector::departFile(uint32_t bytesRead)
{
    // Forward to next link in chain
    if (nextLink)
        nextLink->departFile(bytesRead);
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
    return (it != fileIdToPath.end()) ? it->second->str() : nullptr;
}

bool CMetaInfoState::hasFileMapping(__uint64 fileId) const
{
    return fileIdToPath.find(fileId) != fileIdToPath.end();
}

void CMetaInfoState::clearFileMappings()
{
    fileIdToPath.clear();
    sourceToProps.clear();
    indexFiles.clear();
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

void CMetaInfoState::onFile(const char* filename, uint32_t version)
{
    ++sourceCount;
}

void CMetaInfoState::onEvent(CEvent& event)
{
    if (event.hasAttribute(EvAttrFileId))
    {
        if (MetaFileInformation == event.queryType())
        {
            const char* path = event.queryTextValue(EvAttrPath);
            if (isEmptyString(path))
                return;
            if (sourceCount > 1)
            {
                auto indexFilesIt = indexFiles.find(path);
                if (indexFiles.end() == indexFilesIt)
                    indexFilesIt = indexFiles.emplace(new String(path), generateRuntimeFileId(event)).first;
                sourceToProps.emplace(generateSourceFileId(event), &(*indexFilesIt));
                fileIdToPath.emplace(std::make_pair(indexFilesIt->id, indexFilesIt->path.getLink()));
                event.setValue(EvAttrFileId, indexFilesIt->id);
            }
            else
            {
                fileIdToPath.emplace(std::make_pair(event.queryNumericValue(EvAttrFileId), new String(path)));
            }
        }
        else
        {
            if (sourceCount > 1)
            {
                auto sourcePropsIt = sourceToProps.find(generateSourceFileId(event));
                if (sourcePropsIt != sourceToProps.end())
                {
                    event.setValue(EvAttrFileId, sourcePropsIt->second->id);
                }
            }
        }
    }
    else if (EventQueryStart == event.queryType())
    {
        if (event.hasAttribute(EvAttrEventTraceId) && event.hasAttribute(EvAttrServiceName))
        {
            const char* traceId = event.queryTextValue(EvAttrEventTraceId);
            const char* serviceName = event.queryTextValue(EvAttrServiceName);
            if (!isEmptyString(traceId) && !isEmptyString(serviceName))
                traceIdToService.emplace(std::make_pair(traceId, serviceName));
        }
    }
}

uint32_t CMetaInfoState::generateSourceFileId(const CEvent& event)
{
    __uint64 parts[4] = {0,};
    if (event.hasAttribute(EvAttrChannelId))
        parts[0] = event.queryNumericValue(EvAttrChannelId);
    if (event.hasAttribute(EvAttrReplicaId))
        parts[1] = event.queryNumericValue(EvAttrReplicaId);
    if (event.hasAttribute(EvAttrInstanceId))
        parts[2] = event.queryNumericValue(EvAttrInstanceId);
    if (event.hasAttribute(EvAttrFileId))
        parts[3] = event.queryNumericValue(EvAttrFileId);
    return hashc_fnv1a(reinterpret_cast<const byte *>(&parts), sizeof(parts), fnvInitialHash32);
}

uint32_t CMetaInfoState::generateRuntimeFileId(const CEvent& event)
{
    if (indexFiles.size() == UINT32_MAX)
        throw makeStringExceptionV(-1, "Exceeded maximum number of index files (=%u) supported in event meta parser", UINT32_MAX);
    return static_cast<uint32_t>(indexFiles.size() + 1);
}
