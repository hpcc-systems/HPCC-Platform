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
static StringBuffer& appendSupportedDerivedGroupNames(StringBuffer& text)
{
    unsigned numDerivedAttrs = queryDerivedMetaAttributeCount();
    bool first = true;
    for (unsigned idx = 0; idx < numDerivedAttrs; ++idx)
    {
        const char* name = queryDerivedMetaAttributeNameByIndex(idx);
        if (!name)
            continue;
        if (!first)
            text.append(", ");
        text.append(name);
        first = false;
    }
    return text;
}

static bool hasMetaPrefix(const std::string & attrName)
{
    constexpr size_t metaPrefixLength = sizeof(EVENT_META_PREFIX) - 1;
    return attrName.length() >= metaPrefixLength && strnicmp(attrName.c_str(), EVENT_META_PREFIX, metaPrefixLength) == 0;
}

// Fold a 64-bit attribute contribution into the running group hash.
// The contribution may be a precomputed string hash or a normalized scalar value.
// This maintains ordering (unlike XOR) while avoiding repeated string traversal.
static inline __uint64 foldHash(__uint64 running, __uint64 attrHash)
{
    return fnv1a64Seeded(&attrHash, sizeof(attrHash), running);
}

GroupAttribute GroupAttributeExtractor::parseAttribute(const char* attrDesc)
{
    GroupAttribute ret;
    std::string desc(attrDesc);
    size_t slashPos = desc.find('/');
    std::string attrName = (slashPos != std::string::npos) ? desc.substr(0, slashPos) : desc;
    if (hasMetaPrefix(attrName))
    {
        unsigned derivedAttrId = queryDerivedMetaAttributeId(attrName.c_str());
        if (derivedAttrId == EvAttrNone)
        {
            StringBuffer supported;
            appendSupportedDerivedGroupNames(supported);
            throw makeStringExceptionV(0, "Unsupported grouping attribute '%s'. The meta. prefix is supported for derived attributes only: %s", attrName.c_str(), supported.str());
        }
        ret.attrId = derivedAttrId;
        ret.unit = queryDerivedMetaAttributeUnit(ret.attrId);
    }
    else
    {
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
    }

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
        if (ret.unit == EAUtimestamp)
            ret.timestampPrecision = computeTimestampBucketPrecision(ret.bucketScale);
    }
    return ret;
}

const char* GroupAttributeExtractor::queryCanonicalName(unsigned attrId)
{
    const char* derivedName = queryDerivedMetaAttributeName(attrId);
    if (derivedName)
        return derivedName;
    if (attrId < EvAttrMax)
        return queryEventAttributeName((EventAttr)attrId);
    return nullptr;
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
            formatTimestampNsText(text, val, groupAttr.timestampPrecision);
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
    __uint64 hash = fnv1a64InitialHash;
    for (const GroupAttribute& groupAttr : attrs)
    {
        unsigned attrId = groupAttr.attrId;
        if (attrId == EvExtAttrLogicalFileName)
        {
            if (metaState)
            {
                __uint64 attrHash;
                if (metaState->queryLogicalFileNameHash(event, attrHash))
                    hash = foldHash(hash, attrHash);
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
                hash = fnv1a64Seeded(&val, sizeof(val), hash);
            }
            else if (event.queryType() == EventIndexPayload)
            {
                __uint64 val = 1;
                hash = fnv1a64Seeded(&val, sizeof(val), hash);
            }
            break;
        }
        case EvAttrServiceName:
        case EvAttrPath:
        case EvAttrPlane:
        {
            // Compute a per-attribute FNV hash independently of the running hash,
            // then fold it in. This keeps direct and meta-derived string values
            // consistent: service names, paths, and planes all produce fnv1a64(string_bytes)
            // before folding.
            __uint64 attrHash;
            if (event.hasAttribute(attr))
            {
                const void* ptr = nullptr;
                size_t len = 0;
                if (event.queryHashData(attr, ptr, len) && len != 0)
                    hash = foldHash(hash, fnv1a64Seeded(ptr, len, fnv1a64InitialHash));
            }
            else if (resolveMetaFnv(attr, event, metaState, attrHash))
            {
                hash = foldHash(hash, attrHash);
            }
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
                    hash = foldHash(hash, val);
                }
                else
                {
                    const void* ptr = nullptr;
                    size_t len = 0;
                    if (event.queryHashData(attr, ptr, len) && len != 0)
                        hash = fnv1a64Seeded(ptr, len, hash);
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

bool GroupAttributeExtractor::resolveMetaFnv(EventAttr attr, const CEvent& event, const CMetaInfoState* metaState, __uint64& hash)
{
    if (!metaState)
        return false;

    switch (attr)
    {
    case EvAttrServiceName:
        if (event.hasAttribute(EvAttrEventTraceId))
            return metaState->queryServiceNameHash(event.queryTextValue(EvAttrEventTraceId), hash);
        break;
    case EvAttrPath:
        if (event.hasAttribute(EvAttrFileId))
            return metaState->queryFilePathHash(event.queryNumericValue(EvAttrFileId), hash);
        break;
    case EvAttrPlane:
        return metaState->queryPlaneHash(event, hash);
    default:
        break;
    }
    return false;
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"
#include "eventindexsummarize.h"

class CIndexFileSummaryProbe : public CIndexFileSummary
{
public:
    const std::vector<std::vector<std::string>>& queryGroupAttributes() const { return groupAttributes; }
};

class EventGroupingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventGroupingTest);
    CPPUNIT_TEST(testParseAttribute_Valid);
    CPPUNIT_TEST(testParseAttribute_MetaPrefixAlias);
    CPPUNIT_TEST(testParseAttribute_Invalid);
    CPPUNIT_TEST(testCanonicalGroupNames);
    CPPUNIT_TEST(testSummarizeCanonicalGroupHeaders);
    CPPUNIT_TEST(testComputeTimestampBucketPrecision);
    CPPUNIT_TEST(testFormatValue_Timestamp);
    CPPUNIT_TEST(testGetHash_MixedAttributesAndEmptyString);
    CPPUNIT_TEST_SUITE_END();

public:
    static void assertParsedAttributeId(const char* name, unsigned expectedAttrId)
    {
        GroupAttribute attr = GroupAttributeExtractor::parseAttribute(name);
        CPPUNIT_ASSERT_EQUAL(expectedAttrId, attr.attrId);
    }

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

    void testParseAttribute_MetaPrefixAlias()
    {
        START_TEST

        assertParsedAttributeId("meta.ServiceName", (unsigned)EvAttrServiceName);
        assertParsedAttributeId("meta.Path", (unsigned)EvAttrPath);
        assertParsedAttributeId("meta.Plane", (unsigned)EvAttrPlane);
        assertParsedAttributeId("meta.LogicalFileName", (unsigned)EvExtAttrLogicalFileName);
        assertParsedAttributeId("Meta.Path", (unsigned)EvAttrPath);
        assertParsedAttributeId("META.ServiceName", (unsigned)EvAttrServiceName);

        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("meta.FileOffset/1ki"), "Expected exception for non-derived meta-prefixed attribute");

        END_TEST
    }

    void testParseAttribute_Invalid()
    {
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("UnknownAttr"), "Expected exception for unknown attribute");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(GroupAttributeExtractor::parseAttribute("meta.UnknownAttr"), "Expected exception for unknown prefixed attribute");
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

    void testCanonicalGroupNames()
    {
        CPPUNIT_ASSERT(streq(GroupAttributeExtractor::queryCanonicalName(EvAttrServiceName), EVENT_META_SERVICE_NAME));
        CPPUNIT_ASSERT(streq(GroupAttributeExtractor::queryCanonicalName(EvAttrPath), EVENT_META_PATH));
        CPPUNIT_ASSERT(streq(GroupAttributeExtractor::queryCanonicalName(EvAttrPlane), EVENT_META_PLANE));
        CPPUNIT_ASSERT(streq(GroupAttributeExtractor::queryCanonicalName(EvExtAttrLogicalFileName), EVENT_META_LOGICAL_FILE_NAME));
        CPPUNIT_ASSERT(streq(GroupAttributeExtractor::queryCanonicalName(EvAttrNodeKind), "NodeKind"));
    }

    void testSummarizeCanonicalGroupHeaders()
    {
        CIndexFileSummaryProbe summary;
        summary.addGroupAttribute({ "Path", "FileOffset/1ki" });
        summary.addGroupAttribute({ "META.ServiceName" });

        const auto& headers = summary.queryGroupAttributes();
        CPPUNIT_ASSERT_EQUAL((size_t)2, headers.size());

        CPPUNIT_ASSERT_EQUAL((size_t)2, headers[0].size());
        CPPUNIT_ASSERT_EQUAL(std::string(EVENT_META_PATH), headers[0][0]);
        CPPUNIT_ASSERT_EQUAL(std::string("FileOffset"), headers[0][1]);

        CPPUNIT_ASSERT_EQUAL((size_t)1, headers[1].size());
        CPPUNIT_ASSERT_EQUAL(std::string(EVENT_META_SERVICE_NAME), headers[1][0]);
    }

    void testComputeTimestampBucketPrecision()
    {
        // Whole-second steps: no fractional digits needed
        CPPUNIT_ASSERT_EQUAL(0U, computeTimestampBucketPrecision(1'000'000'000ULL)); // 1s
        CPPUNIT_ASSERT_EQUAL(0U, computeTimestampBucketPrecision(5'000'000'000ULL)); // 5s
        CPPUNIT_ASSERT_EQUAL(0U, computeTimestampBucketPrecision(60'000'000'000ULL)); // 1min

        // Sub-second steps
        CPPUNIT_ASSERT_EQUAL(1U, computeTimestampBucketPrecision(100'000'000ULL)); // 100ms
        CPPUNIT_ASSERT_EQUAL(1U, computeTimestampBucketPrecision(500'000'000ULL)); // 500ms
        CPPUNIT_ASSERT_EQUAL(2U, computeTimestampBucketPrecision(250'000'000ULL)); // 250ms
        CPPUNIT_ASSERT_EQUAL(3U, computeTimestampBucketPrecision(1'000'000ULL));   // 1ms
        CPPUNIT_ASSERT_EQUAL(3U, computeTimestampBucketPrecision(5'000'000ULL));   // 5ms
        CPPUNIT_ASSERT_EQUAL(6U, computeTimestampBucketPrecision(1'000ULL));       // 1us
        CPPUNIT_ASSERT_EQUAL(4U, computeTimestampBucketPrecision(500'000ULL));     // 500us
        CPPUNIT_ASSERT_EQUAL(9U, computeTimestampBucketPrecision(1ULL));           // 1ns
        CPPUNIT_ASSERT_EQUAL(9U, computeTimestampBucketPrecision(7ULL));           // 7ns (non-power-of-10)

        // Steps larger than 1s: exact multiples of 1e9 yield precision 0,
        // but steps with fractional-second components yield non-zero precision
        CPPUNIT_ASSERT_EQUAL(0U, computeTimestampBucketPrecision(2'000'000'000ULL)); // 2s (exact multiple)
        CPPUNIT_ASSERT_EQUAL(1U, computeTimestampBucketPrecision(1'500'000'000ULL)); // 1.5s (has sub-second)
    }

    void testFormatValue_Timestamp()
    {
        // Non-bucketed timestamp: precision defaults to DTP_Nanos, isBucket=false → rawValue returned as-is
        GroupAttribute nonBucketAttr;
        nonBucketAttr.attrId = EvAttrEventTimestamp;
        nonBucketAttr.unit = EAUtimestamp;
        nonBucketAttr.isBucket = false;
        CPPUNIT_ASSERT_EQUAL(std::string("500000000"), GroupAttributeExtractor::formatValue(nonBucketAttr, "500000000"));

        // Bucketed at 1s: precision 0, whole seconds only
        GroupAttribute attr1s = GroupAttributeExtractor::parseAttribute("EventTimestamp/1s");
        CPPUNIT_ASSERT_EQUAL(0U, attr1s.timestampPrecision);
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:01"), GroupAttributeExtractor::formatValue(attr1s, "1000000000"));

        // Bucketed at 500ms: precision 1 — all labels include 1 fractional digit
        GroupAttribute attr500ms = GroupAttributeExtractor::parseAttribute("EventTimestamp/500ms");
        CPPUNIT_ASSERT_EQUAL(1U, attr500ms.timestampPrecision);
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.0"), GroupAttributeExtractor::formatValue(attr500ms, "0"));
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.5"), GroupAttributeExtractor::formatValue(attr500ms, "500000000"));

        // Bucketed at 1ms: precision 3
        GroupAttribute attr1ms = GroupAttributeExtractor::parseAttribute("EventTimestamp/1ms");
        CPPUNIT_ASSERT_EQUAL(3U, attr1ms.timestampPrecision);
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.000"), GroupAttributeExtractor::formatValue(attr1ms, "0"));
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.001"), GroupAttributeExtractor::formatValue(attr1ms, "1000000"));

        // Bucketed at 1ns: precision 9
        GroupAttribute attr1ns = GroupAttributeExtractor::parseAttribute("EventTimestamp/1ns");
        CPPUNIT_ASSERT_EQUAL(9U, attr1ns.timestampPrecision);
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.000000000"), GroupAttributeExtractor::formatValue(attr1ns, "0"));
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.500000123"), GroupAttributeExtractor::formatValue(attr1ns, "500000123"));
    }

    void testGetHash_MixedAttributesAndEmptyString()
    {
        START_TEST
        CMetaInfoState state;
        Owned<IEventVisitationLink> collector = state.getCollector();

        CEvent planeInfo;
        planeInfo.reset(MetaPlaneInformation);
        planeInfo.setValue(EvAttrPlane, "myplane");
        planeInfo.setValue(EvAttrPath, "/var/lib/hpccsystems/hpcc-data/myplane/");
        planeInfo.setValue(EvAttrIsStriped, false);
        CPPUNIT_ASSERT(collector->visitEvent(planeInfo));

        CEvent fileInfo;
        fileInfo.reset(MetaFileInformation);
        fileInfo.setValue(EvAttrFileId, (__uint64)1);
        fileInfo.setValue(EvAttrPath, "/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::1");
        CPPUNIT_ASSERT(collector->visitEvent(fileInfo));

        CEvent queryStart;
        queryStart.reset(EventQueryStart);
        queryStart.setValue(EvAttrEventTraceId, "trace-1");
        queryStart.setValue(EvAttrServiceName, "service-1");
        CPPUNIT_ASSERT(collector->visitEvent(queryStart));

        std::vector<GroupAttribute> attrs;
        attrs.push_back(GroupAttributeExtractor::parseAttribute("NodeKind"));
        attrs.push_back(GroupAttributeExtractor::parseAttribute("FileOffset"));
        attrs.push_back(GroupAttributeExtractor::parseAttribute("ServiceName"));
        attrs.push_back(GroupAttributeExtractor::parseAttribute("Path"));
        attrs.push_back(GroupAttributeExtractor::parseAttribute("Plane"));

        CEvent event;
        event.reset(EventIndexCacheHit);
        event.setValue(EvAttrFileId, (__uint64)1);
        event.setValue(EvAttrEventTraceId, "trace-1");
        event.setValue(EvAttrNodeKind, (__uint64)7);
        event.setValue(EvAttrFileOffset, (__uint64)123456);

        __uint64 expected = fnv1a64InitialHash;

        __uint64 nodeKind = event.queryNumericValue(EvAttrNodeKind);
        expected = fnv1a64Seeded(&nodeKind, sizeof(nodeKind), expected);

        __uint64 fileOffset = event.queryNumericValue(EvAttrFileOffset);
        expected = fnv1a64Seeded(&fileOffset, sizeof(fileOffset), expected);

        __uint64 serviceHash = 0;
        CPPUNIT_ASSERT(state.queryServiceNameHash(event.queryTextValue(EvAttrEventTraceId), serviceHash));
        expected = fnv1a64Seeded(&serviceHash, sizeof(serviceHash), expected);

        __uint64 pathHash = 0;
        CPPUNIT_ASSERT(state.queryFilePathHash(event.queryNumericValue(EvAttrFileId), pathHash));
        expected = fnv1a64Seeded(&pathHash, sizeof(pathHash), expected);

        __uint64 planeHash = 0;
        CPPUNIT_ASSERT(state.queryPlaneHash(event, planeHash));
        expected = fnv1a64Seeded(&planeHash, sizeof(planeHash), expected);

        __uint64 actual = GroupAttributeExtractor::getHash(attrs, event, &state);
        CPPUNIT_ASSERT_EQUAL(expected, actual);

        CEvent emptyService;
        emptyService.reset(EventQueryStart);
        emptyService.setValue(EvAttrServiceName, "");

        std::vector<GroupAttribute> serviceAttrs;
        serviceAttrs.push_back(GroupAttributeExtractor::parseAttribute("ServiceName"));

        __uint64 emptyExpected = fnv1a64InitialHash;
        __uint64 emptyActual = GroupAttributeExtractor::getHash(serviceAttrs, emptyService, nullptr);
        CPPUNIT_ASSERT_EQUAL(emptyExpected, emptyActual);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventGroupingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventGroupingTest, "eventgrouping");

#endif
