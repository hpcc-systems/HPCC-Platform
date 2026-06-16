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
#include "eventutility.hpp" // for strToBytes
#include "jutil.hpp"

// GroupAttributeExtractor implementation

static inline __uint64 fnv1a(const void* data, size_t len, __uint64 hash = 0xcbf29ce484222325ULL) {
    const unsigned char* ptr = (const unsigned char*)data;
    for (size_t i = 0; i < len; ++i) {
        hash ^= ptr[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

GroupAttribute GroupAttributeExtractor::parseAttribute(const char* attrDesc)
{
    GroupAttribute ret;
    std::string desc(attrDesc);
    size_t slashPos = desc.find('/');
    std::string attrName = (slashPos != std::string::npos) ? desc.substr(0, slashPos) : desc;

    EventAttr attr = queryEventAttribute(attrName.c_str());
    if (attr != EvAttrNone)
    {
        ret.attrId = attr;
        ret.unit = queryEventAttributeUnit(attr);
    }
    else if (strieq(attrName.c_str(), "LogicalFileName"))
    {
        ret.attrId = EvExtAttrLogicalFileName;
        ret.unit = EAUnone;
    }
    else
        throw makeStringExceptionV(0, "Unknown grouping attribute '%s'", attrName.c_str());

    if (slashPos != std::string::npos)
    {
        std::string mod = desc.substr(slashPos + 1);
        if (mod.empty()) throw makeStringExceptionV(0, "Missing modifier after '/' in '%s'", attrDesc);

        ret.isBucket = true;
        switch (ret.unit)
        {
        case EAUsize:
            // strToBytes is used instead of friendlyStringToSize to maintain consistency with how
            // sizes are represented in other operation contexts
            ret.bucketScale = strToBytes(mod.c_str(), StrToBytesFlags::ThrowOnError);
            break;
        case EAUscalar:
        {
            const char* in = mod.c_str();
            while (isspace((unsigned char)*in))
                in++;
            if (*in == '-')
                throw makeStringExceptionV(0, "Invalid scalar bucket modifier '%s' (cannot be negative)", mod.c_str());
            char* tail;
            ret.bucketScale = strtoull(in, &tail, 10);
            if (tail == in || *tail)
                throw makeStringExceptionV(0, "Invalid scalar bucket modifier '%s'", mod.c_str());
            break;
        }
        case EAUduration:
        case EAUtimestamp:
            ret.bucketScale = friendlyStringToDuration(mod.c_str());
            break;
        case EAUnone:
        default:
            throw makeStringExceptionV(0, "Grouping by buckets is not supported for attribute '%s'", attrName.c_str());
        }
        if (ret.bucketScale == 0)
            throw makeStringExceptionV(0, "Bucket size cannot be 0 for '%s'", attrDesc);
    }
    return ret;
}

std::string GroupAttributeExtractor::formatValue(const GroupAttribute& groupAttr, const std::string& rawValue)
{
    if (!groupAttr.isBucket)
        return rawValue;

    if (groupAttr.unit == EAUtimestamp)
    {
        try
        {
            __uint64 val = std::stoull(rawValue);
            StringBuffer text;
            formatTimestampNsText(text, val);
            return text.str();
        }
        catch (...)
        {
            // Ignore parse failures
        }
    }
    return rawValue;
}


bool GroupAttributeExtractor::isApplicable(const GroupAttribute& groupAttr, const CEvent& event)
{
    unsigned attrId = groupAttr.attrId;
    if (attrId == EvExtAttrLogicalFileName)
    {
        return event.isAttribute(EvAttrFileId);
    }

    EventAttr attr = (EventAttr)attrId;
    switch (attr)
    {
    case EvAttrNodeKind:
        return event.isAttribute(attr) || event.queryType() == EventIndexPayload;
    case EvAttrServiceName:
        return event.isAttribute(attr) || event.isAttribute(EvAttrEventTraceId);
    case EvAttrPath:
    case EvAttrPlane:
        return event.isAttribute(attr) || event.isAttribute(EvAttrFileId);
    default:
        return event.isAttribute(attr);
    }
}

std::string GroupAttributeExtractor::getValue(const GroupAttribute& groupAttr, const CEvent& event, const CMetaInfoState* metaState)
{
    unsigned attrId = groupAttr.attrId;
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
            if (groupAttr.isBucket)
            {
                __uint64 val = event.queryNumericValue(attr);
                val = (val / groupAttr.bucketScale) * groupAttr.bucketScale;
                return std::to_string(val);
            }
            if (event.isNumericAttribute(attr))
            {
                __uint64 val = event.queryNumericValue(attr);
                return std::to_string(val);
            }
            if (event.isTextAttribute(attr))
            {
                const char* val = event.queryTextValue(attr);
                return val ? val : "";
            }
            if (event.isBooleanAttribute(attr))
                return event.queryBooleanValue(attr) ? "true" : "false";
        }
        break;
    }
    return "";
}

__uint64 GroupAttributeExtractor::getHash(const std::vector<GroupAttribute>& attrs, const CEvent& event, const CMetaInfoState* metaState)
{
    __uint64 hash = 0xcbf29ce484222325ULL;
    for (const GroupAttribute& groupAttr : attrs)
    {
        unsigned attrId = groupAttr.attrId;
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
            if (event.hasAttribute(attr))
            {
                __uint64 val = event.queryNumericValue(attr);
                hash = fnv1a(&val, sizeof(val), hash);
            }
            else if (event.queryType() == EventIndexPayload)
            {
                __uint64 val = 1;
                hash = fnv1a(&val, sizeof(val), hash);
            }
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
                if (groupAttr.isBucket)
                {
                    __uint64 val = event.queryNumericValue(attr);
                    val = (val / groupAttr.bucketScale) * groupAttr.bucketScale;
                    hash = fnv1a(&val, sizeof(val), hash);
                }
                else if (event.isNumericAttribute(attr))
                {
                    __uint64 val = event.queryNumericValue(attr);
                    hash = fnv1a(&val, sizeof(val), hash);
                }
                else if (event.isTextAttribute(attr))
                {
                    const char* val = event.queryTextValue(attr);
                    if (val)
                        hash = fnv1a(val, strlen(val), hash);
                }
                else if (event.isBooleanAttribute(attr))
                {
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

bool GroupAttributeExtractor::isEqual(const std::vector<GroupAttribute>& attrs, const CEvent& event, const CMetaInfoState* metaState, const std::vector<std::string>& groupValues)
{
    if (attrs.size() != groupValues.size())
        return false;

    for (size_t i = 0; i < attrs.size(); ++i)
    {
        const GroupAttribute& groupAttr = attrs[i];
        unsigned attrId = groupAttr.attrId;
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
            if (event.hasAttribute(attr))
            {
                if (expected != mapNodeKind((NodeKind)event.queryNumericValue(attr)))
                    return false;
            }
            else if (event.queryType() == EventIndexPayload)
            {
                if (expected != mapNodeKind((NodeKind)1))
                    return false;
            }
            else
            {
                if (expected != "")
                    return false;
            }
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
                if (groupAttr.isBucket)
                {
                    __uint64 val = event.queryNumericValue(attr);
                    val = (val / groupAttr.bucketScale) * groupAttr.bucketScale;
                    if (expected != std::to_string(val))
                        return false;
                }
                else if (event.isNumericAttribute(attr))
                {
                    __uint64 val = event.queryNumericValue(attr);
                    if (expected != std::to_string(val))
                        return false;
                }
                else if (event.isTextAttribute(attr))
                {
                    const char* val = event.queryTextValue(attr);
                    if (expected != (val ? val : ""))
                        return false;
                }
                else if (event.isBooleanAttribute(attr))
                {
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

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventGroupingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventGroupingTest);
    CPPUNIT_TEST(testParseAttribute_Valid);
    CPPUNIT_TEST(testParseAttribute_Invalid);
    CPPUNIT_TEST(testFormatValue_Timestamp);
    CPPUNIT_TEST_SUITE_END();

public:
    void testParseAttribute_Valid()
    {
        GroupAttribute attr;

        // Base attribute
        attr = GroupAttributeExtractor::parseAttribute("NodeKind");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrNodeKind, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUnone, attr.unit);
        CPPUNIT_ASSERT_EQUAL(1ULL, attr.bucketScale);
        CPPUNIT_ASSERT_EQUAL(false, attr.isBucket);

        // Size bucket (kib)
        attr = GroupAttributeExtractor::parseAttribute("FileOffset/512");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrFileOffset, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUsize, attr.unit);
        CPPUNIT_ASSERT_EQUAL(512ULL, attr.bucketScale);
        CPPUNIT_ASSERT_EQUAL(true, attr.isBucket);

        attr = GroupAttributeExtractor::parseAttribute("FileOffset/1ki");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrFileOffset, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUsize, attr.unit);
        CPPUNIT_ASSERT_EQUAL(1024ULL, attr.bucketScale);
        CPPUNIT_ASSERT_EQUAL(true, attr.isBucket);

        // Decimal size bucket
        attr = GroupAttributeExtractor::parseAttribute("FileOffset/1.5k");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrFileOffset, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUsize, attr.unit);
        CPPUNIT_ASSERT_EQUAL(1500ULL, attr.bucketScale);
        CPPUNIT_ASSERT_EQUAL(true, attr.isBucket);

        // Overlapping units (m/M for megabytes vs m for minutes)
        attr = GroupAttributeExtractor::parseAttribute("FileOffset/5m");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrFileOffset, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUsize, attr.unit);
        CPPUNIT_ASSERT_EQUAL(5000000ULL, attr.bucketScale);

        attr = GroupAttributeExtractor::parseAttribute("FileOffset/5M");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrFileOffset, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUsize, attr.unit);
        CPPUNIT_ASSERT_EQUAL(5000000ULL, attr.bucketScale);

        // Duration bucket
        attr = GroupAttributeExtractor::parseAttribute("ElapsedTime/5ms");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrElapsedTime, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUduration, attr.unit);
        CPPUNIT_ASSERT_EQUAL(5000000ULL, attr.bucketScale);
        CPPUNIT_ASSERT_EQUAL(true, attr.isBucket);

        attr = GroupAttributeExtractor::parseAttribute("ElapsedTime/5m");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrElapsedTime, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUduration, attr.unit);
        CPPUNIT_ASSERT_EQUAL(300000000000ULL, attr.bucketScale); // 5 * 60 * 10^9 ns

        attr = GroupAttributeExtractor::parseAttribute("ElapsedTime/5M");
        CPPUNIT_ASSERT_EQUAL((unsigned)EvAttrElapsedTime, attr.attrId);
        CPPUNIT_ASSERT_EQUAL(EAUduration, attr.unit);
        CPPUNIT_ASSERT_EQUAL(300000000000ULL, attr.bucketScale); // 5 * 60 * 10^9 ns
    }

    void testParseAttribute_Invalid()
    {
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("UnknownAttr"), "Expected exception for unknown attribute");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("NodeKind/10M"), "Expected exception for bucketing unbucketable attribute");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("FileOffset/"), "Expected exception for missing modifier");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("FileOffset/0"), "Expected exception for zero bucket scale");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("FileOffset/-5"), "Expected exception for negative size");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("ElapsedTime/-5ms"), "Expected exception for negative duration");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("ElapsedTime/1.5ms"), "Expected exception for decimal duration");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("FileOffset/abc"), "Expected exception for non-numeric size modifier");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("ElapsedTime/abc"), "Expected exception for non-numeric duration modifier");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("FileOffset/5ms"), "Expected exception for duration unit on size attribute");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("ElapsedTime/5KB"), "Expected exception for size unit on duration attribute");
    }

    void testFormatValue_Timestamp()
    {
        GroupAttribute attr;
        attr.attrId = EvAttrEventTimestamp;
        attr.unit = EAUtimestamp;
        attr.isBucket = true;

        std::string raw = "500000000"; // Exactly a half second past epoch
        std::string formatted = GroupAttributeExtractor::formatValue(attr, raw);
        // Expecting right padding to 9 decimal places
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.500000000"), formatted);

        raw = "500000123";
        formatted = GroupAttributeExtractor::formatValue(attr, raw);
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.500000123"), formatted);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventGroupingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventGroupingTest, "eventgrouping");

#endif
