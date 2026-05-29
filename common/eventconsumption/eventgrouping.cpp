/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "eventgrouping.hpp"
#include "jtime.hpp"
#include "eventindex.hpp" // for mapNodeKind

// GroupAttributeExtractor implementation

static inline __uint64 fnv1a(const void* data, size_t len, __uint64 hash = 0xcbf29ce484222325ULL) {
    const unsigned char* ptr = (const unsigned char*)data;
    for (size_t i = 0; i < len; ++i) {
        hash ^= ptr[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

unsigned GroupAttributeExtractor::getAttributeId(const char* attrName)
{
    EventAttr attr = queryEventAttribute(attrName);
    if (attr != EvAttrNone)
        return attr;
    if (strieq(attrName, "LogicalFileName"))
        return EvExtAttrLogicalFileName;
    throw makeStringExceptionV(0, "Unknown grouping attribute '%s'", attrName);
}

std::string GroupAttributeExtractor::getValue(unsigned attrId, const CEvent& event, const CMetaInfoState* metaState)
{
    if (attrId == EvExtAttrLogicalFileName)
    {
        if (metaState)
        {
            const char* lfn = metaState->queryLogicalFileName(event);
            if (lfn)
                return lfn;
        }
        return "";
    }

    EventAttr attr = (EventAttr)attrId;
    switch (attr)
    {
    case EvAttrNodeKind:
        if (event.hasAttribute(attr))
        {
            return mapNodeKind((NodeKind)event.queryNumericValue(attr));
        }
        if (event.queryType() == EventIndexPayload)
        {
            return mapNodeKind((NodeKind)1); // Leaf node assumed
        }
        break;
    case EvAttrServiceName:
    case EvAttrPath:
    case EvAttrPlane:
    {
        const char * val = resolveStringAttribute(attr, event, metaState);
        return val ? val : "";
    }
    default:
        if (event.hasAttribute(attr))
        {
            if (event.isTextAttribute(attr))
            {
                const char* val = event.queryTextValue(attr);
                return val ? val : "";
            }
            if (event.isNumericAttribute(attr))
                return std::to_string(event.queryNumericValue(attr));
            if (event.isBooleanAttribute(attr))
                return event.queryBooleanValue(attr) ? "true" : "false";
        }
        break;
    }
    return "";
}

__uint64 GroupAttributeExtractor::getHash(const std::vector<unsigned>& attrIds, const CEvent& event, const CMetaInfoState* metaState)
{
    __uint64 hash = 0xcbf29ce484222325ULL;
    for (unsigned attrId : attrIds)
    {
        if (attrId == EvExtAttrLogicalFileName) {
            if (metaState) {
                const char* lfn = metaState->queryLogicalFileName(event);
                if (lfn)
                    hash = fnv1a(lfn, strlen(lfn), hash);
            }
            continue;
        }
        EventAttr attr = (EventAttr)attrId;
        switch(attr)
        {
        case EvAttrNodeKind:
        {
            __uint64 val = 0;
            if (event.hasAttribute(attr))
                val = event.queryNumericValue(attr);
            else if (event.queryType() == EventIndexPayload)
                val = 1;
            hash = fnv1a(&val, sizeof(val), hash);
            break;
        }
        case EvAttrServiceName:
        case EvAttrPath:
        case EvAttrPlane:
        {
            const char* val = resolveStringAttribute(attr, event, metaState);
            if (val)
                hash = fnv1a(val, strlen(val), hash);
            break;
        }
        default:
        {
            if (event.hasAttribute(attr))
            {
                if (event.isTextAttribute(attr)) {
                    const char* val = event.queryTextValue(attr);
                    if (val)
                        hash = fnv1a(val, strlen(val), hash);
                }
                else if (event.isNumericAttribute(attr)) {
                    __uint64 val = event.queryNumericValue(attr);
                    hash = fnv1a(&val, sizeof(val), hash);
                }
                else if (event.isBooleanAttribute(attr)) {
                    bool val = event.queryBooleanValue(attr);
                    hash = fnv1a(&val, sizeof(val), hash);
                }
            }
            break;
        }
        }
    }
    return hash;
}

bool GroupAttributeExtractor::isEqual(const std::vector<unsigned>& attrIds, const CEvent& event, const CMetaInfoState* metaState, const std::vector<std::string>& groupValues)
{
    if (attrIds.size() != groupValues.size())
        return false;

    for (size_t i = 0; i < attrIds.size(); ++i)
    {
        unsigned attrId = attrIds[i];
        const std::string& expected = groupValues[i];

        if (attrId == EvExtAttrLogicalFileName) {
            const char* lfn = metaState ? metaState->queryLogicalFileName(event) : nullptr;
            if (expected != (lfn ? lfn : ""))
                return false;
            continue;
        }

        EventAttr attr = (EventAttr)attrId;
        switch(attr)
        {
        case EvAttrNodeKind:
        {
            __uint64 val = 0;
            if (event.hasAttribute(attr))
                val = event.queryNumericValue(attr);
            else if (event.queryType() == EventIndexPayload)
                val = 1;
            if (expected != mapNodeKind((NodeKind)val))
                return false;
            break;
        }
        case EvAttrServiceName:
        case EvAttrPath:
        case EvAttrPlane:
        {
            const char* val = resolveStringAttribute(attr, event, metaState);
            if (expected != (val ? val : ""))
                return false;
            break;
        }
        default:
        {
            if (event.hasAttribute(attr))
            {
                if (event.isTextAttribute(attr)) {
                    const char* val = event.queryTextValue(attr);
                    if (expected != (val ? val : ""))
                        return false;
                }
                else if (event.isNumericAttribute(attr)) {
                    if (expected != std::to_string(event.queryNumericValue(attr)))
                        return false;
                }
                else if (event.isBooleanAttribute(attr)) {
                    const char* val = event.queryBooleanValue(attr) ? "true" : "false";
                    if (expected != val)
                        return false;
                }
            }
            else
            {
                if (!expected.empty())
                    return false;
            }
            break;
        }
        }
    }
    return true;
}

const char* GroupAttributeExtractor::resolveStringAttribute(EventAttr attr, const CEvent& event, const CMetaInfoState* metaState)
{
    if (event.hasAttribute(attr))
        return event.queryTextValue(attr);

    if (metaState)
    {
        if (attr == EvAttrServiceName && event.hasAttribute(EvAttrEventTraceId))
            return metaState->queryServiceName(event.queryTextValue(EvAttrEventTraceId));
        else if (attr == EvAttrPath && event.hasAttribute(EvAttrFileId))
            return metaState->queryFilePath(event.queryNumericValue(EvAttrFileId));
        else if (attr == EvAttrPlane)
            return metaState->queryPlane(event);
    }
    return nullptr;
}
