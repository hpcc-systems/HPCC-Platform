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
#include "eventiterator.h"
#include "eventutility.hpp"
#include "jevent.hpp"

struct DerivedMetaAttrName
{
    unsigned attrId;
    const char* canonicalName;
    EventAttrUnit unit;
};

static constexpr DerivedMetaAttrName derivedMetaAttrNames[] = {
    { EvAttrServiceName, EVENT_META_SERVICE_NAME, EAUnone },
    { EvExtAttrLogicalFileName, EVENT_META_LOGICAL_FILE_NAME, EAUnone },
    { EvAttrPath, EVENT_META_PATH, EAUnone },
    { EvAttrPlane, EVENT_META_PLANE, EAUnone }
};

const char* queryDerivedMetaAttributeName(unsigned attrId)
{
    for (const auto& entry : derivedMetaAttrNames)
    {
        if (entry.attrId == attrId)
            return entry.canonicalName;
    }
    return nullptr;
}

unsigned queryDerivedMetaAttributeId(const char* canonicalName)
{
    if (isEmptyString(canonicalName))
        return EvAttrNone;

    for (const auto& entry : derivedMetaAttrNames)
    {
        if (strieq(entry.canonicalName, canonicalName))
            return entry.attrId;
    }
    return EvAttrNone;
}

unsigned queryDerivedMetaAttributeCount()
{
    return _elements_in(derivedMetaAttrNames);
}

const char* queryDerivedMetaAttributeNameByIndex(unsigned idx)
{
    if (idx < _elements_in(derivedMetaAttrNames))
        return derivedMetaAttrNames[idx].canonicalName;
    return nullptr;
}

EventAttrUnit queryDerivedMetaAttributeUnit(unsigned attrId)
{
    for (const auto& entry : derivedMetaAttrNames)
    {
        if (entry.attrId == attrId)
            return entry.unit;
    }
    return EAUnone;
}

namespace
{
inline bool failHashQuery(__uint64& hash)
{
    hash = 0;
    return false;
}
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

CMetaInfoState::~CMetaInfoState()
{
    clearAll();
}

IEventVisitationLink* CMetaInfoState::getCollector()
{
    return new CCollector(*this);
}

const CMetaInfoState::CachedString* CMetaInfoState::queryFilePathEntry(__uint64 fileId) const
{
    auto it = fileIdToPath.find(fileId);
    if (it == fileIdToPath.end())
        return nullptr;
    return &it->second;
}

const char* CMetaInfoState::queryFilePath(__uint64 fileId) const
{
    const CachedString* entry = queryFilePathEntry(fileId);
    return entry ? entry->view.data() : "";
}

bool CMetaInfoState::queryFilePathHash(__uint64 fileId, __uint64& hash) const
{
    const CachedString* entry = queryFilePathEntry(fileId);
    if (!entry)
        return failHashQuery(hash);
    hash = entry->hash;
    return true;
}

const CMetaInfoState::CachedString* CMetaInfoState::queryPlaneEntry(const CEvent& event) const
{
    if (!event.hasAttribute(EvAttrFileId))
        return nullptr;

    auto it = fileIdToPlane.find(event.queryNumericValue(EvAttrFileId));
    if (it == fileIdToPlane.end())
        return nullptr;
    return &it->second;
}

const char* CMetaInfoState::queryPlane(const CEvent& event) const
{
    if (event.hasAttribute(EvAttrPlane))
        return event.queryTextValue(EvAttrPlane);

    const CachedString* entry = queryPlaneEntry(event);
    return entry ? entry->view.data() : "";
}

bool CMetaInfoState::queryPlaneHash(const CEvent& event, __uint64& hash) const
{
    if (event.hasAttribute(EvAttrPlane))
    {
        // Direct attribute: compute FNV on the spot for consistency with cached plane values.
        const void* ptr; size_t len;
        if (!event.queryHashData(EvAttrPlane, ptr, len) || 0 == len)
            return failHashQuery(hash);
        hash = fnv1a64Seeded(ptr, len, fnv1a64InitialHash);
        return true;
    }
    const CachedString* entry = queryPlaneEntry(event);
    if (!entry)
        return failHashQuery(hash);
    hash = entry->hash;
    return true;
}

const CMetaInfoState::CachedString* CMetaInfoState::queryLogicalFileNameEntry(const CEvent& event) const
{
    if (!event.hasAttribute(EvAttrFileId))
        return nullptr;

    const unsigned fileId = event.queryNumericValue(EvAttrFileId);
    auto it = fileIdToLogicalName.find(fileId);
    if (it == fileIdToLogicalName.end())
        return nullptr;
    return &it->second;
}

const char* CMetaInfoState::queryLogicalFileName(const CEvent& event) const
{
    const CachedString* entry = queryLogicalFileNameEntry(event);
    return entry ? entry->view.data() : "";
}

bool CMetaInfoState::queryLogicalFileNameHash(const CEvent& event, __uint64& hash) const
{
    const CachedString* entry = queryLogicalFileNameEntry(event);
    if (!entry || entry->view.empty())
        return failHashQuery(hash);
    hash = entry->hash;
    return true;
}

bool CMetaInfoState::hasFileMapping(__uint64 fileId) const
{
    return fileIdToPath.find(fileId) != fileIdToPath.end();
}

const char* CMetaInfoState::queryServiceName(const char* traceId) const
{
    const ServiceEntry* entry = queryServiceNameEntry(traceId);
    return entry ? entry->name.c_str() : "";
}

const CMetaInfoState::ServiceEntry* CMetaInfoState::queryServiceNameEntry(const char* traceId) const
{
    if (!traceId || !*traceId)
        return nullptr;
    auto it = traceIdToService.find(traceId);
    if (it == traceIdToService.end())
        return nullptr;
    return &it->second;
}

bool CMetaInfoState::queryServiceNameHash(const char* traceId, __uint64& hash) const
{
    const ServiceEntry* entry = queryServiceNameEntry(traceId);
    if (!entry)
        return failHashQuery(hash);
    hash = entry->hash;
    return true;
}

bool CMetaInfoState::hasServiceMapping(const char* traceId) const
{
    if (!traceId || !*traceId)
        return false;
    return traceIdToService.find(traceId) != traceIdToService.end();
}

void CMetaInfoState::clearAll()
{
    fileIdToPath.clear();
    fileIdToLogicalName.clear();
    fileIdToPlane.clear();
    logicalNamePool.clear();
    sourceToProps.clear();
    indexFiles.clear();
    traceIdToService.clear();
    haveLastRemap = false;
    lastRemapInput = {};
    lastRuntimeFileId = 0;

    // Cleared last as other containers hold pointers to plane string buffers.
    planes.clear();
}

void CMetaInfoState::onFile(const char* filename, uint32_t version)
{
    ++sourceCount;
}

void CMetaInfoState::onEvent(CEvent& event)
{
    switch (event.queryType())
    {
    case MetaPlaneInformation:
    {
        PlaneInformation pi(event);
        auto it = planes.find(pi);
        if (it == planes.end())
            planes.insert(std::move(pi));
        else if (it->path != pi.path || it->striped != pi.striped)
            throw makeStringExceptionV(0, "Conflicting plane definition: '%s'", pi.plane.c_str());
        break;
    }
    case MetaFileInformation:
        if (event.hasAttribute(EvAttrFileId) && event.hasAttribute(EvAttrPath))
        {
            const unsigned fileId = event.queryNumericValue(EvAttrFileId);
            const char* path = event.queryTextValue(EvAttrPath);
            if (!isEmptyString(path))
            {
                // Emplace safely ignores duplicates and returns insertion status
                auto [indexIt, isNewPath] = indexFiles.emplace(path, generateRuntimeFileId(event));
                unsigned targetFileId = (sourceCount > 1) ? indexIt->id : fileId;

                if (sourceCount > 1)
                {
                    sourceToProps.emplace(makeSourceFileKey(event), &(*indexIt));
                    event.setValue(EvAttrFileId, targetFileId);
                }

                // In multi-source mode, file IDs are generated per unique path (so we only map if isNewPath).
                // In single-source mode, multiple unique file IDs might share the same path (so always map).
                if (isNewPath || sourceCount <= 1)
                {
                    std::string_view pathView = indexIt->path;
                    fileIdToPath.emplace(targetFileId, CachedString{pathView, fnv1a64Seeded(pathView.data(), pathView.size(), fnv1a64InitialHash)});

                    // Derive logical file name eagerly
                    const PlaneInformation* bestPlane = findBestPlaneMatch(path);
                    if (bestPlane)
                    {
                        std::string_view planeView = bestPlane->plane;
                        fileIdToPlane[targetFileId] = CachedString{planeView, fnv1a64Seeded(planeView.data(), planeView.size(), fnv1a64InitialHash)};
                    }
                    std::string_view lfnView = deriveLogicalFileName(path, bestPlane);
                    fileIdToLogicalName[targetFileId] = CachedString{lfnView, fnv1a64Seeded(lfnView.data(), lfnView.size(), fnv1a64InitialHash)};
                }
            }
        }
        break;
    case EventQueryStart:
        if (event.hasAttribute(EvAttrEventTraceId) && event.hasAttribute(EvAttrServiceName))
        {
            const char* traceId = event.queryTextValue(EvAttrEventTraceId);
            const char* serviceName = event.queryTextValue(EvAttrServiceName);
            if (!isEmptyString(traceId) && !isEmptyString(serviceName))
            {
                const void* ptr = nullptr;
                size_t len = 0;
                if (event.queryHashData(EvAttrServiceName, ptr, len))
                    traceIdToService.emplace(traceId, ServiceEntry{serviceName, fnv1a64Seeded(ptr, len, fnv1a64InitialHash)});
            }
        }
        break;
    default:
        tryRemapFileId(event);
        break;
    }
}

bool CMetaInfoState::tryRemapFileId(CEvent& event)
{
    // MORE: Excluding single input files from remapping is one reason why a FileId reported by
    // one operation might be unusable in a subsequent operation. The issue cannot be resolved
    // simply by removing this check of sourceCount since the generated ids rely on the order
    // events are processed. Correction is deferred to issue #36836.
    if (sourceCount <= 1 || !event.hasAttribute(EvAttrFileId))
        return false;

    SourceFileKey key = makeSourceFileKey(event);
    if (haveLastRemap && key == lastRemapInput)
    {
        event.setValue(EvAttrFileId, lastRuntimeFileId);
        return true;
    }

    // MORE: If sourceToProps does not contain an entry for this event the remap fails and the
    // original FileId is unchanged. This can lead to collisions between mapped and unmapped ids.
    // If the expectation that a FileInformation event will be the first occurrence of a FileId is
    // satisfied then this will not happen.
    //
    // Legacy files do exist for which the expectation is not satisfied. The evtool sim command
    // can create new files for which the expectation is not satisfied. Refactoring to remap all
    // ids, even without FileInformation events, is deferred to a future issue (perhaps #36836, or
    // another issue).
    auto sourcePropsIt = sourceToProps.find(key);
    if (sourcePropsIt == sourceToProps.end())
        return false;

    lastRemapInput = key;
    lastRuntimeFileId = sourcePropsIt->second->id;
    haveLastRemap = true;
    event.setValue(EvAttrFileId, lastRuntimeFileId);
    return true;
}

CMetaInfoState::SourceFileKey CMetaInfoState::makeSourceFileKey(const CEvent& event)
{
    return {
        event.hasAttribute(EvAttrInstanceId) ? event.queryNumericValue(EvAttrInstanceId) : 0,
        event.hasAttribute(EvAttrFileId)     ? (uint32_t)event.queryNumericValue(EvAttrFileId)     : 0,
        event.hasAttribute(EvAttrChannelId)  ? (uint8_t)event.queryNumericValue(EvAttrChannelId)  : uint8_t{0},
        event.hasAttribute(EvAttrReplicaId)  ? (uint8_t)event.queryNumericValue(EvAttrReplicaId)  : uint8_t{0},
    };
}

uint32_t CMetaInfoState::generateRuntimeFileId(const CEvent& event)
{
    if (indexFiles.size() == UINT32_MAX)
        throw makeStringExceptionV(-1, "Exceeded maximum number of index files (=%u) supported in event meta parser", UINT32_MAX);
    return static_cast<uint32_t>(indexFiles.size() + 1);
}

const CMetaInfoState::PlaneInformation* CMetaInfoState::findBestPlaneMatch(const char* path) const
{
    const PlaneInformation* bestPlane = nullptr;
    for (const auto& plane : planes)
    {
        if (!bestPlane || plane.path.length() > bestPlane->path.length())
        {
            if (!plane.path.empty() && strncmp(path, plane.path.c_str(), plane.path.length()) == 0)
            {
                char lastPlaneCh = plane.path.back();
                if (lastPlaneCh != '/' && lastPlaneCh != '\\')
                {
                    // The plane path did not end with a path separator. Only accept a match
                    // when the matching portion of the file path is either a complete match
                    // or is followed by a path separator.
                    char nextCh = path[plane.path.length()];
                    if (nextCh != '\0' && nextCh != '/' && nextCh != '\\')
                        continue;
                }
                bestPlane = &plane;
            }
        }
    }
    return bestPlane;
}

std::string_view CMetaInfoState::deriveLogicalFileName(const char* path, const CMetaInfoState::PlaneInformation* plane)
{
    // 1. Remove the plane path prefix, if a plane match was found.
    const char* start = path;
    if (plane)
        start += plane->path.length();

    //    Trim "leading" path separators.
    while (*start == '/' || *start == '\\')
        start++;

    // 2. Remove a possible striped device directory if a matching plane indicates its presence.
    if (plane && plane->striped)
    {
        const char* nextSlash = strpbrk(start, "/\\");
        if (nextSlash && nextSlash - start > 1 && start[0] == 'd')
        {
            bool isNum = true;
            for (const char* p = start + 1; p < nextSlash; ++p)
            {
                if (!isdigit((unsigned char)*p))
                {
                    isNum = false;
                    break;
                }
            }
            if (isNum)
            {
                start = nextSlash + 1;
                // trim "leading" path separators
                while (*start == '/' || *start == '\\')
                    start++;
            }
        }
    }

    // 3. Always trim a possible trailing file part suffixes of the form "._X_of_Y"
    constexpr const char* suffixPt1 = "._";
    constexpr const char* suffixPt2 = "_of_";
    constexpr size_t suffixPt1Len = 2;
    constexpr size_t suffixPt2Len = 4;
    //    Minimum remaining length needed to match a valid suffix pattern:
    //    "._" + digit + "_of_" + digit
    size_t minRemaining = suffixPt1Len + 1 + suffixPt2Len + 1;
    const char* end = start + strlen(start);
    const char* p = end - 1;
    if ((end - start) > minRemaining && isdigit((unsigned char)*p))
    {
        // Pre-decrement p since *p is a confirmed digit
        while (--p >= start && isdigit((unsigned char)*p)) {}

        minRemaining -= 1;
        if ((p - start) >= minRemaining && strncmp(p - suffixPt2Len + 1, suffixPt2, suffixPt2Len) == 0)
        {
            p -= suffixPt2Len;
            minRemaining -= suffixPt2Len;
            if ((p - start) >= minRemaining && isdigit((unsigned char)*p))
            {
                while (--p >= start && isdigit((unsigned char)*p)) {}
                minRemaining -= 1;
                if ((p - start) >= minRemaining && strncmp(p - suffixPt1Len + 1, suffixPt1, suffixPt1Len) == 0)
                    end = p - suffixPt1Len + 1;
            }
        }
    }

    // 4. Translate physical file path separators to logical file name scope separators.
    std::string finalLogical;
    //    Compute the precise file name length to avoid caching larger than needed buffers.
    size_t reservation = (end - start);
    for (p = start; p < end; ++p)
    {
        if (*p == '/' || *p == '\\')
            reservation += 1; // "::" is one char longer than '/' or '\\'
    }
    finalLogical.reserve(reservation);
    //    Assemble the logical file name in the reserved buffer.
    for (p = start; p < end; ++p)
    {
        if (*p == '/' || *p == '\\')
            finalLogical += "::";
        else
            finalLogical += *p;
    }

    // 5. Cache and return the derived name.
    auto pit = logicalNamePool.find(finalLogical);
    if (pit == logicalNamePool.end())
        pit = logicalNamePool.insert(std::move(finalLogical)).first;
    return *pit;
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventMetaStateTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventMetaStateTest);
    CPPUNIT_TEST(validateFileIdAttributeType);
    CPPUNIT_TEST(validateChannelIdAttributeType);
    CPPUNIT_TEST(validateReplicaIdAttributeType);
    CPPUNIT_TEST(validateInstanceIdAttributeType);
    CPPUNIT_TEST(testHashQueryCacheMissesAndHits);
    CPPUNIT_TEST(testLogicalFileNameHashEmptyDerivedReturnsFalse);
    CPPUNIT_TEST(testPlaneHashDirectAttributeMatchesMetaDerived);
    CPPUNIT_TEST(testHashOutputsMatchKnownFnv1aValues);
    CPPUNIT_TEST(testPlaneLookup);
    CPPUNIT_TEST(testDataDrivenLogicalFileNameDerivation);
    CPPUNIT_TEST(testConflictingPlanePath);
    CPPUNIT_TEST(testConflictingPlaneStriped);
    CPPUNIT_TEST(testBulkLogicalFileNameDerivation);
    CPPUNIT_TEST(testLongestMatchLogicalFileNameDerivation);
    CPPUNIT_TEST_SUITE_END();

    void executeTestEvents(const char* xml, CMetaInfoState& state)
    {
        Owned<IPropertyTree> pt = createPTreeFromXMLString(xml);
        if (!pt)
            return;

        Owned<IEventIterator> iter = createPropertyTreeEvents(*pt, PTEFlenientParsing);
        Owned<IEventVisitationLink> collector = state.getCollector();
        if (iter)
            visitIterableEvents(*iter, *collector);
    }

public:
    // These "tests" validate hash key generation expectations. It is assumed that all official
    // builds, including for PR commits, will run the tests, forcing code updates prior to release.
    // Test failure requires updates to:
    // - CMetaInfoState::SourceFileKey
    // - CMetaInfoState::makeSourceFileKey
    void validateFileIdAttributeType()
    {
        CPPUNIT_ASSERT_MESSAGE("Expected EvAttrFileId to be EATu4", queryEventAttributeType(EvAttrFileId) == EATu4);
    }
    void validateChannelIdAttributeType()
    {
        CPPUNIT_ASSERT_MESSAGE("Expected EvAttrChannelId to be EATu1", queryEventAttributeType(EvAttrChannelId) == EATu1);
    }
    void validateReplicaIdAttributeType()
    {
        CPPUNIT_ASSERT_MESSAGE("Expected EvAttrReplicaId to be EATu1", queryEventAttributeType(EvAttrReplicaId) == EATu1);
    }
    void validateInstanceIdAttributeType()
    {
        CPPUNIT_ASSERT_MESSAGE("Expected EvAttrInstanceId to be EATu8", queryEventAttributeType(EvAttrInstanceId) == EATu8);
    }

    void testHashQueryCacheMissesAndHits()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
    <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
    <event type="FileInformation" FileId="1" Path="/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::1" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        // Seed traceId-to-service cache via visitor pipeline to avoid XML parsing ambiguity.
        Owned<IEventVisitationLink> collector = state.getCollector();
        CEvent queryStart;
        queryStart.reset(EventQueryStart);
        queryStart.setValue(EvAttrEventTraceId, "trace-1");
        queryStart.setValue(EvAttrServiceName, "foobar");
        CPPUNIT_ASSERT(collector->visitEvent(queryStart));

        __uint64 hash = 123;
        CPPUNIT_ASSERT(!state.queryFilePathHash(999, hash));
        CPPUNIT_ASSERT_EQUAL((__uint64)0, hash);
        CPPUNIT_ASSERT(state.queryFilePathHash(1, hash));

        hash = 123;
        CPPUNIT_ASSERT(!state.queryServiceNameHash("missing-trace", hash));
        CPPUNIT_ASSERT_EQUAL((__uint64)0, hash);
        CPPUNIT_ASSERT(state.queryServiceNameHash("trace-1", hash));

        CEvent missing;
        missing.reset(MetaFileInformation);
        missing.setValue(EvAttrFileId, (__uint64)999);
        CPPUNIT_ASSERT(!state.queryLogicalFileNameHash(missing, hash));
        CPPUNIT_ASSERT(!state.queryPlaneHash(missing, hash));

        CEvent emptyPlane;
        emptyPlane.reset(MetaPlaneInformation);
        emptyPlane.setValue(EvAttrPlane, "");
        CPPUNIT_ASSERT(!state.queryPlaneHash(emptyPlane, hash));
        END_TEST
    }

    void testPlaneHashDirectAttributeMatchesMetaDerived()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
    <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
    <event type="FileInformation" FileId="1" Path="/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::1" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        CEvent metaEvent;
        metaEvent.reset(MetaFileInformation);
        metaEvent.setValue(EvAttrFileId, (__uint64)1);
        __uint64 metaHash = 0;
        CPPUNIT_ASSERT(state.queryPlaneHash(metaEvent, metaHash));

        CEvent directEvent;
        directEvent.reset(MetaPlaneInformation);
        directEvent.setValue(EvAttrPlane, "myplane");
        __uint64 directHash = 0;
        CPPUNIT_ASSERT(state.queryPlaneHash(directEvent, directHash));

        CPPUNIT_ASSERT_EQUAL(metaHash, directHash);
        END_TEST
    }

    void testLogicalFileNameHashEmptyDerivedReturnsFalse()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
    <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
    <event type="FileInformation" FileId="10" Path="/var/lib/hpccsystems/hpcc-data/myplane/" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        CEvent event;
        event.reset(MetaFileInformation);
        event.setValue(EvAttrFileId, (__uint64)10);

        // A path identical to the plane root derives to an empty logical file name.
        const char* logicalName = state.queryLogicalFileName(event);
        CPPUNIT_ASSERT(logicalName != nullptr);
        CPPUNIT_ASSERT_EQUAL(std::string(""), std::string(logicalName));

        __uint64 hash = 123;
        CPPUNIT_ASSERT(!state.queryLogicalFileNameHash(event, hash));
        CPPUNIT_ASSERT_EQUAL((__uint64)0, hash);
        END_TEST
    }

    void testHashOutputsMatchKnownFnv1aValues()
    {
        START_TEST
        CMetaInfoState state;
        Owned<IEventVisitationLink> collector = state.getCollector();

        CEvent queryStartA;
        queryStartA.reset(EventQueryStart);
        queryStartA.setValue(EvAttrEventTraceId, "trace-a");
        queryStartA.setValue(EvAttrServiceName, "a");
        CPPUNIT_ASSERT(collector->visitEvent(queryStartA));

        CEvent queryStartFoobar;
        queryStartFoobar.reset(EventQueryStart);
        queryStartFoobar.setValue(EvAttrEventTraceId, "trace-foobar");
        queryStartFoobar.setValue(EvAttrServiceName, "foobar");
        CPPUNIT_ASSERT(collector->visitEvent(queryStartFoobar));

        __uint64 hashA = 0;
        CPPUNIT_ASSERT(state.queryServiceNameHash("trace-a", hashA));
        CPPUNIT_ASSERT_EQUAL((__uint64)0xaf63dc4c8601ec8cULL, hashA);

        __uint64 hashFoobar = 0;
        CPPUNIT_ASSERT(state.queryServiceNameHash("trace-foobar", hashFoobar));
        CPPUNIT_ASSERT_EQUAL((__uint64)0x85944171f73967e8ULL, hashFoobar);
        END_TEST
    }

    void testPlaneLookup()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
  <event type="PlaneInformation" Plane="mystripedplane" Path="/var/lib/hpccsystems/hpcc-data/mystripedplane/" IsStriped="1" />
  <event type="FileInformation" FileId="1" Path="/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::1" />
  <event type="FileInformation" FileId="2" Path="/var/lib/hpccsystems/hpcc-data/mystripedplane/d123/some/logical/file::2" />
  <event type="FileInformation" FileId="3" Path="/var/lib/hpccsystems/hpcc-data/unknownplane/some/logical/file::3" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        CEvent q1;
        q1.reset(MetaFileInformation);
        q1.setValue(EvAttrFileId, (unsigned long long)1);
        const char * plane1 = state.queryPlane(q1);
        CPPUNIT_ASSERT(plane1 != nullptr && *plane1 != 0);
        CPPUNIT_ASSERT_EQUAL(std::string("myplane"), std::string(plane1));

        CEvent q2;
        q2.reset(MetaFileInformation);
        q2.setValue(EvAttrFileId, (unsigned long long)2);
        const char * plane2 = state.queryPlane(q2);
        CPPUNIT_ASSERT(plane2 != nullptr && *plane2 != 0);
        CPPUNIT_ASSERT_EQUAL(std::string("mystripedplane"), std::string(plane2));

        CEvent q3;
        q3.reset(MetaFileInformation);
        q3.setValue(EvAttrFileId, (unsigned long long)3);
        const char * plane3 = state.queryPlane(q3);
        CPPUNIT_ASSERT(plane3 == nullptr || *plane3 == 0);

        END_TEST
    }

    void testDataDrivenLogicalFileNameDerivation()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
  <event type="PlaneInformation" Plane="mystripedplane" Path="/var/lib/hpccsystems/hpcc-data/mystripedplane/" IsStriped="1" />

  <event type="FileInformation" FileId="1" Path="/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::1" />
  <!-- Striped plane matching path removing d123 prefix -->
  <event type="FileInformation" FileId="2" Path="/var/lib/hpccsystems/hpcc-data/mystripedplane/d123/some/logical/file::2" />
  <!-- Logical path with ._X_of_Y suffix to be stripped -->
  <event type="FileInformation" FileId="3" Path="/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::3._1_of_4" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        CEvent q1;
        q1.reset(MetaFileInformation);
        q1.setValue(EvAttrFileId, (__uint64)1);
        const char * ln1 = state.queryLogicalFileName(q1);
        CPPUNIT_ASSERT(ln1 != nullptr && *ln1 != '\0');
        CPPUNIT_ASSERT_EQUAL(std::string("some::logical::file::1"), std::string(ln1));

        CEvent q2;
        q2.reset(MetaFileInformation);
        q2.setValue(EvAttrFileId, (__uint64)2);
        const char * ln2 = state.queryLogicalFileName(q2);
        CPPUNIT_ASSERT(ln2 != nullptr && *ln2 != '\0');
        CPPUNIT_ASSERT_EQUAL(std::string("some::logical::file::2"), std::string(ln2));

        CEvent q3;
        q3.reset(MetaFileInformation);
        q3.setValue(EvAttrFileId, (__uint64)3);
        const char * ln3 = state.queryLogicalFileName(q3);
        CPPUNIT_ASSERT(ln3 != nullptr && *ln3 != '\0');
        CPPUNIT_ASSERT_EQUAL(std::string("some::logical::file::3"), std::string(ln3));
        END_TEST
    }

    void testConflictingPlanePath()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/OTHER_PATH/" IsStriped="0" />
</events>
)";
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(executeTestEvents(xmlEvents, state), "Conflicting plane definition");
        END_TEST
    }

    void testConflictingPlaneStriped()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="0" />
  <event type="PlaneInformation" Plane="myplane" Path="/var/lib/hpccsystems/hpcc-data/myplane/" IsStriped="1" />
</events>
)";
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(executeTestEvents(xmlEvents, state), "Conflicting plane definition");
        END_TEST
    }

    void testLongestMatchLogicalFileNameDerivation()
    {
        START_TEST
        CMetaInfoState state;
        const char* xmlEvents = R"(
<events>
  <event type="PlaneInformation" Plane="aplane" Path="/data/" IsStriped="0" />
  <event type="PlaneInformation" Plane="bplane" Path="/data/logs/" IsStriped="0" />

  <event type="FileInformation" FileId="1" Path="/data/some/logical/file::1" />
  <event type="FileInformation" FileId="2" Path="/data/logs/other/logical/file::2" />
</events>
)";
        executeTestEvents(xmlEvents, state);

        CEvent q1;
        q1.reset(MetaFileInformation);
        q1.setValue(EvAttrFileId, (__uint64)1);
        const char * ln1 = state.queryLogicalFileName(q1);
        CPPUNIT_ASSERT(ln1 != nullptr && *ln1 != '\0');
        CPPUNIT_ASSERT_EQUAL(std::string("some::logical::file::1"), std::string(ln1));

        CEvent q2;
        q2.reset(MetaFileInformation);
        q2.setValue(EvAttrFileId, (__uint64)2);
        const char * ln2 = state.queryLogicalFileName(q2);
        CPPUNIT_ASSERT(ln2 != nullptr && *ln2 != '\0');
        CPPUNIT_ASSERT_EQUAL(std::string("other::logical::file::2"), std::string(ln2));
        END_TEST
    }

    void testBulkLogicalFileNameDerivation()
    {
        START_TEST
        CMetaInfoState state;
        std::string xmlEvents = "<events>\n";
        xmlEvents += "  <event type=\"PlaneInformation\" Plane=\"myplane\" Path=\"/var/lib/hpccsystems/hpcc-data/myplane/\" IsStriped=\"0\" />\n";
        for (int i = 0; i < 20; ++i)
        {
            xmlEvents += "  <event type=\"FileInformation\" FileId=\"" + std::to_string(i) + "\" Path=\"/var/lib/hpccsystems/hpcc-data/myplane/some/logical/file::" + std::to_string(i) + "\" />\n";
        }
        xmlEvents += "</events>\n";
        executeTestEvents(xmlEvents.c_str(), state);

        for (int i = 0; i < 20; ++i)
        {
            CEvent q;
            q.reset(MetaFileInformation);
            q.setValue(EvAttrFileId, (__uint64)i);
            const char * ln = state.queryLogicalFileName(q);
            CPPUNIT_ASSERT(ln != nullptr && *ln != '\0');
            std::string expected = "some::logical::file::" + std::to_string(i);
            CPPUNIT_ASSERT_EQUAL(expected, std::string(ln));
        }
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventMetaStateTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventMetaStateTest, "eventmetastate");

#endif
