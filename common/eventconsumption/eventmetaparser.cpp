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
#include "jevent.hpp"

bool operator < (const CMetaInfoState::IndexFileProperties& left, const CMetaInfoState::IndexFileProperties& right)
{
    return left.path < right.path;
}

bool operator < (const char* left, const CMetaInfoState::IndexFileProperties& right)
{
    return left < right.path;
}

bool operator < (const CMetaInfoState::IndexFileProperties& left, const char* right)
{
    return left.path < right;
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

const char* CMetaInfoState::queryFilePath(__uint64 fileId) const
{
    auto it = fileIdToPath.find(fileId);
    return (it != fileIdToPath.end()) ? it->second : "";
}

const char* CMetaInfoState::queryPlane(const CEvent& event) const
{
    if (event.hasAttribute(EvAttrPlane))
        return event.queryTextValue(EvAttrPlane);

    if (!event.hasAttribute(EvAttrFileId))
        return "";

    auto it = fileIdToPlane.find(event.queryNumericValue(EvAttrFileId));
    if (it != fileIdToPlane.end())
    {
        return it->second;
    }
    return "";
}

const char* CMetaInfoState::queryLogicalFileName(const CEvent& event) const
{
    if (!event.hasAttribute(EvAttrFileId))
        return "";

    const unsigned fileId = event.queryNumericValue(EvAttrFileId);

    auto it = fileIdToLogicalName.find(fileId);
    if (it != fileIdToLogicalName.end())
        return it->second;

    return "";
}

bool CMetaInfoState::hasFileMapping(__uint64 fileId) const
{
    return fileIdToPath.find(fileId) != fileIdToPath.end();
}

const char* CMetaInfoState::queryServiceName(const char* traceId) const
{
    if (!traceId || !*traceId)
        return "";
    auto it = traceIdToService.find(traceId);
    return (it != traceIdToService.end()) ? it->second.c_str() : "";
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
                    sourceToProps.emplace(generateSourceFileId(event), &(*indexIt));
                    event.setValue(EvAttrFileId, targetFileId);
                }

                // In multi-source mode, file IDs are generated per unique path (so we only map if isNewPath).
                // In single-source mode, multiple unique file IDs might share the same path (so always map).
                if (isNewPath || sourceCount <= 1)
                {
                    fileIdToPath.emplace(targetFileId, indexIt->path.c_str());

                    // Derive logical file name eagerly
                    const PlaneInformation* bestPlane = findBestPlaneMatch(path);
                    if (bestPlane)
                        fileIdToPlane[targetFileId] = bestPlane->plane.c_str();
                    fileIdToLogicalName[targetFileId] = deriveLogicalFileName(path, bestPlane);
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
                traceIdToService.emplace(std::make_pair(traceId, serviceName));
        }
        break;
    default:
        if (event.hasAttribute(EvAttrFileId) && sourceCount > 1)
        {
            auto sourcePropsIt = sourceToProps.find(generateSourceFileId(event));
            if (sourcePropsIt != sourceToProps.end())
            {
                event.setValue(EvAttrFileId, sourcePropsIt->second->id);
            }
        }
        break;
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

const char* CMetaInfoState::deriveLogicalFileName(const char* path, const CMetaInfoState::PlaneInformation* plane)
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
    return pit->c_str();
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventMetaStateTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventMetaStateTest);
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
