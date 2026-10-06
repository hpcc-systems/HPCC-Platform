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

#include "jevent.hpp"
#include "jsuperhash.hpp"
#include <functional>

constexpr byte defaultPageBits = 13; // 8K page size
constexpr __uint64 indexPageSize = __uint64(1) << defaultPageBits;

class FileIdFileOffsetKey
{
public:
    __uint64 fileId{0};
    __uint64 fileOffset{0};

    FileIdFileOffsetKey() = default;
    FileIdFileOffsetKey(__uint64 _fileId, __uint64 _fileOffset) : fileId(_fileId), fileOffset(_fileOffset) {}
    FileIdFileOffsetKey(const CEvent& event) : fileId(event.queryNumericValue(EvAttrFileId)), fileOffset(event.queryNumericValue(EvAttrFileOffset)) {}

    bool operator == (const FileIdFileOffsetKey& other) const
    {
        return fileId == other.fileId && fileOffset == other.fileOffset;
    }
};

class FileIdFileOffsetKeyHash
{
public:
    size_t operator()(const FileIdFileOffsetKey& key) const
    {
        size_t h1 = std::hash<__uint64>{}(key.fileId);
        size_t h2 = std::hash<__uint64>{}(key.fileOffset);
        return h1 ^ (h2 + fnvInitialHash32 + (h1 << 6) + (h1 >> 2));
    }
};

enum NodeKind : unsigned
{
    BranchNode,
    LeafNode,
    BlobNode,
    NumNodeKinds
};

constexpr const char* nodeKindText[NumNodeKinds] = { "branch", "leaf", "blob" };

inline bool isNodeKind(__uint64 kind)
{
    return kind < NumNodeKinds;
}

inline bool isNodeKind(unsigned kind)
{
    return kind < NumNodeKinds;
}

inline const char* mapNodeKind(NodeKind kind)
{
    if (isNodeKind(kind))
        return nodeKindText[kind];
    throw makeStringExceptionV(0, "unknown node kind %u", kind);
}

inline NodeKind mapNodeKind(const char* kindText)
{
    if (kindText)
    {
        static_assert(NumNodeKinds <= 10); // require a single digit node kind value
        char number[2] = { '0', '\0' };
        for (unsigned i = 0; i < NumNodeKinds; i++)
        {
            number[0] = '0' + i;
            if (streq(kindText, number) || strieq(kindText, nodeKindText[i]))
                return (NodeKind)i;
        }
    }
    throw makeStringExceptionV(0, "unknown node kind '%s'", (kindText ? kindText : "<null>"));
}

// Determine the logical NodeKind value for the given event. Applies only to events of the
// EventCtxIndex event context. Exceptions are thrown for any index event with an unknown node
// kind and for all non-index events.
inline NodeKind queryIndexNodeKind(const CEvent& evt)
{
    if (evt.hasAttribute(EvAttrNodeKind))
    {
        __uint64 nodeKind = evt.queryNumericValue(EvAttrNodeKind);
        if (!isNodeKind(nodeKind))
            throw makeStringExceptionV(0, "unknown node kind %llu", nodeKind);
        return static_cast<NodeKind>(nodeKind);
    }
    if (eventInAnyContext(evt.queryType(), EventCtxIndex))
        return LeafNode;
    throw makeStringExceptionV(0, "event %s does not use NodeKind", queryEventName(evt.queryType()));
}
