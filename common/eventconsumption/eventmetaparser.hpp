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
#include <functional>
#include <map>
#include <unordered_map>
#include <set>
#include <string>

constexpr char EVENT_META_PREFIX[] = "meta.";
constexpr char EVENT_META_SERVICE_NAME[] = "meta.ServiceName";
constexpr char EVENT_META_LOGICAL_FILE_NAME[] = "meta.LogicalFileName";
constexpr char EVENT_META_PATH[] = "meta.Path";
constexpr char EVENT_META_PLANE[] = "meta.Plane";

// Visitor that parses and caches file ID to path mappings and trace ID to service name mappings
class event_decl CMetaInfoState : public CInterface
{
public:
    struct IndexFileProperties
    {
        std::string path;
        uint32_t id;

        IndexFileProperties(std::string _path, uint32_t _id) : path(std::move(_path)), id(_id) {}
    };

    struct PlaneInformation
    {
        std::string plane;
        std::string path;
        bool striped{false};

        PlaneInformation() = default;
        PlaneInformation(const CEvent& event)
        {
            if (event.queryType() != MetaPlaneInformation)
                throw makeStringExceptionV(0, "Invalid event type (%s) for PlaneInformation", queryEventName(event.queryType()));
            if (event.hasAttribute(EvAttrPlane))
                plane = event.queryTextValue(EvAttrPlane);
            if (event.hasAttribute(EvAttrPath))
                path = event.queryTextValue(EvAttrPath);
            if (event.hasAttribute(EvAttrIsStriped))
                striped = event.queryBooleanValue(EvAttrIsStriped);
        }

        bool operator < (const PlaneInformation& other) const
        {
            return (plane < other.plane);
        }
    };

    using Planes = std::set<PlaneInformation>;


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
        virtual bool preScanRequired() const override { return false; }
    public:
        CCollector(CMetaInfoState& _metaState);
    private:
        Linked<CMetaInfoState> metaState;
        Linked<IEventVisitor> nextLink;
    };

public:
    ~CMetaInfoState();

    // Obtain a new event visitor instance to populate this state object.
    IEventVisitationLink* getCollector();

    // Accessor functions for file ID to path mappings
    // Note: Returns "" (empty string) to represent a cache miss
    const char* queryFilePath(__uint64 fileId) const;
    bool hasFileMapping(__uint64 fileId) const;

    // Accessor functions for trace ID to service name mappings
    // Note: Returns "" (empty string) to represent a cache miss
    const char* queryServiceName(const char* traceId) const;
    bool hasServiceMapping(const char* traceId) const;

    // Accessor functions for plane information
    const char* queryLogicalFileName(const CEvent& event) const;
    const char* queryPlane(const CEvent& event) const;

    // Clear all mappings
    void clearAll();

    // Remap source-scoped FileId to runtime FileId when multi-source mappings exist.
    // Returns true if the event FileId was updated.
    bool tryRemapFileId(CEvent& event);

private:
    struct SourceFileKey
    {
        uint64_t instanceId{0};
        uint32_t sourceFileId{0};
        uint8_t  channelId{0};
        uint8_t  replicaId{0};
        bool operator==(const SourceFileKey& o) const noexcept
        {
            return instanceId == o.instanceId && sourceFileId == o.sourceFileId
                && channelId == o.channelId && replicaId == o.replicaId;
        }
    };
    struct SourceFileKeyHash
    {
        size_t operator()(const SourceFileKey& k) const noexcept
        {
            size_t h = 0;
            auto combine = [&h](size_t value)
            {
                constexpr size_t golden = (sizeof(size_t) >= 8) ? 0x9e3779b97f4a7c15ULL : 0x9e3779b9U;
                h ^= value + golden + (h << 6) + (h >> 2);
            };

            combine(std::hash<uint64_t>{}(k.instanceId));
            combine(std::hash<uint32_t>{}(k.sourceFileId));
            combine(std::hash<uint8_t>{}(k.channelId));
            combine(std::hash<uint8_t>{}(k.replicaId));
            return h;
        }
    };
    void onFile(const char* filename, uint32_t version);
    void onEvent(CEvent& event);
    static SourceFileKey makeSourceFileKey(const CEvent& event);
    uint32_t generateRuntimeFileId(const CEvent& event);
    const PlaneInformation* findBestPlaneMatch(const char* path) const;
    const char* deriveLogicalFileName(const char* path, const PlaneInformation* plane);

private:
    size_t sourceCount{0};
    SourceFileKey lastRemapInput;
    uint32_t lastRuntimeFileId{0};
    bool haveLastRemap{false};

    // File ID to path mappings (from MetaFileInformation events)
    // Note: fileIdToPath stores pointers to the string buffers owned by indexFiles.
    // This is safe because std::set is a node-based container which guarantees
    // memory stability; element pointers are never invalidated when the set grows.
    std::set<IndexFileProperties, std::less<>> indexFiles;
    std::unordered_map<SourceFileKey, const IndexFileProperties*, SourceFileKeyHash> sourceToProps;
    std::unordered_map<__uint64, const char*> fileIdToPath;

    // Trace ID to service name mappings (from EventQueryStart events)
    std::unordered_map<std::string, std::string> traceIdToService;

    // Physical file paths mapped to logical file names.
    //
    // planes tracks the configuration required to identify the logical file name of a physical file
    // logicalNamePool owns the string memory for each unique derived logical file name
    // fileIdToLogicalName caches the derived logical file name for each known file ID
    //
    // Note: fileIdToLogicalName stores pointers to the string buffers owned by logicalNamePool.
    // This is safe because std::set is a node-based container which guarantees
    // memory stability; element pointers are never invalidated when the set grows.
    Planes planes;
    // fileIdToPlane caches the storage plane identifier for each known file Id
    std::unordered_map<__uint64, const char*> fileIdToPlane;
    std::unordered_map<__uint64, const char*> fileIdToLogicalName;
    std::set<std::string, std::less<>> logicalNamePool;
};
