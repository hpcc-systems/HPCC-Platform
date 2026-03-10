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

// The relationship between a full index file path and the logical file is:
//   [ <local install path> ] '/var/lib/HPCCSystems/hpcc-data/roxie/' [ 'd' <unsigned> '/' ] <logical file path> [ '/' <crc> ]
// This method splits a presumed full path into parts, identifies the leading and trailing parts
// that are not part of the logical file path, and reassembles the logical file path from the
// remaining parts.
//
// Given a full path that deviates from this format, the full path is returned as the logical
// file path.
StringBuffer& CMetaInfoState::getLogicalFile(StringBuffer& file, __uint64 fileId) const
{
    const char* fullPath = queryFilePath(fileId);
    if (isEmptyString(fullPath))
        return file;
    StringArray pathParts;
    pathParts.appendList(fullPath, "/");
    if (pathParts.ordinality() == 0)
    {
        file.append(fullPath);
        return file;
    }
    aindex_t firstPart = 0, lastPart = pathParts.ordinality() - 1;
    // skip parts until the installed root is found
    while (firstPart <= lastPart && !streq(pathParts.item(firstPart), "var"))
        firstPart++;
    // skip fixed parts (var/lib/HPCCSystems/hpcc-data/roxie/)
    if (firstPart + 5 > lastPart ||
        !streq(pathParts.item(firstPart), "var") ||
        !streq(pathParts.item(firstPart + 1), "lib") ||
        !streq(pathParts.item(firstPart + 2), "HPCCSystems") ||
        !streq(pathParts.item(firstPart + 3), "hpcc-data") ||
        !streq(pathParts.item(firstPart + 4), "roxie"))
    {
        // Not in expected format, return full path
        file.append(fullPath);
        return file;
    }
    firstPart += 5;
    // skip optional storage part
    if (firstPart <= lastPart)
    {
        const char* tmp = pathParts.item(firstPart);
        char* endptr = nullptr;
        if (*tmp == 'd' && *(tmp + 1) != '\0' && isdigit((unsigned char)*(tmp + 1)))
        {
            (void)strtoul(tmp + 1, &endptr, 10);
            if (*endptr == '\0')
                firstPart++;
        }
    }
    // skip the crc suffix (#/#) if present
    while (firstPart < lastPart)
    {
        const char* tmp = pathParts.item(lastPart);
        char* endptr = nullptr;
        (void)strtoull(tmp, &endptr, 10);
        if (*endptr != '\0')
            break;
        lastPart--;
    }
    // assemble and return the output path
    if (firstPart <= lastPart)
    {
        while (firstPart < lastPart)
        {
            file.append(pathParts.item(firstPart)).append('/');
            firstPart++;
        }
        file.append(pathParts.item(lastPart));
    }
    else
        file.append(fullPath);
    return file;
}

StringBuffer& CMetaInfoState::selectFilePath(StringBuffer& buf, __uint64 fileId, bool fullPath) const
{
    if (fullPath)
    {
        const char* path = queryFilePath(fileId);
        if (path)
            buf.append(path);
        return buf;
    }
    return getLogicalFile(buf, fileId);
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

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class MetaInfoStateTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(MetaInfoStateTest);
    CPPUNIT_TEST(testLogicalFileStandardFormat);
    CPPUNIT_TEST(testLogicalFileWithStoragePart);
    CPPUNIT_TEST(testLogicalFileWithCRCSuffix);
    CPPUNIT_TEST(testLogicalFileWithStorageAndCRC);
    CPPUNIT_TEST(testLogicalFileNonStandardFormat);
    CPPUNIT_TEST(testLogicalFileShortPath);
    CPPUNIT_TEST(testLogicalFileNoVarPrefix);
    CPPUNIT_TEST(testLogicalFileEmptyPath);
    CPPUNIT_TEST(testLogicalFileUnmappedId);
    CPPUNIT_TEST(testLogicalFileMultiplePaths);
    CPPUNIT_TEST_SUITE_END();

private:
    void injectFileInformation(CMetaInfoState& metaState, IEventVisitationLink& collector, __uint64 fileId, const char* path)
    {
        CEvent event;
        event.reset(MetaFileInformation);
        event.setValue(EvAttrFileId, fileId);
        event.setValue(EvAttrPath, path);

        collector.visitEvent(event);
    }

    Owned<IEventVisitationLink> startFileInjection(CMetaInfoState& metaState)
    {
        Owned<IEventVisitationLink> collector = metaState.getCollector();
        collector->visitFile("test.evt", 1);
        return collector;
    }

    void endFileInjection(IEventVisitationLink& collector)
    {
        collector.departFile(0);
    }

public:
    void testLogicalFileStandardFormat()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/var/lib/HPCCSystems/hpcc-data/roxie/mydata/myfile/myindex";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        CPPUNIT_ASSERT_EQUAL_STR("mydata/myfile/myindex", result.str());
        END_TEST
    }

    void testLogicalFileWithStoragePart()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/var/lib/HPCCSystems/hpcc-data/roxie/d5/mydata/myfile/myindex";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        CPPUNIT_ASSERT_EQUAL_STR("mydata/myfile/myindex", result.str());
        END_TEST
    }

    void testLogicalFileWithCRCSuffix()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/var/lib/HPCCSystems/hpcc-data/roxie/mydata/myfile/myindex/12345/67890";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        CPPUNIT_ASSERT_EQUAL_STR("mydata/myfile/myindex", result.str());
        END_TEST
    }

    void testLogicalFileWithStorageAndCRC()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/some/prefix/var/lib/HPCCSystems/hpcc-data/roxie/d3/mydata/myfile/myindex/12345/67890";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        CPPUNIT_ASSERT_EQUAL_STR("mydata/myfile/myindex", result.str());
        END_TEST
    }

    void testLogicalFileNonStandardFormat()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/opt/mydata/myindex";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        CPPUNIT_ASSERT_EQUAL_STR("/opt/mydata/myindex", result.str());
        END_TEST
    }

    void testLogicalFileShortPath()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/var/lib/HPCCSystems/hpcc-data/myindex";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        // Path too short to match expected format, returns full path
        CPPUNIT_ASSERT_EQUAL_STR("/var/lib/HPCCSystems/hpcc-data/myindex", result.str());
        END_TEST
    }

    void testLogicalFileNoVarPrefix()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath = "/home/lib/HPCCSystems/hpcc-data/roxie/mydata/myindex";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath);
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        // Doesn't start with expected path, returns full path
        CPPUNIT_ASSERT_EQUAL_STR("/home/lib/HPCCSystems/hpcc-data/roxie/mydata/myindex", result.str());
        END_TEST
    }

    void testLogicalFileEmptyPath()
    {
        START_TEST
        CMetaInfoState metaState;

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, "");
        endFileInjection(*collector);

        StringBuffer result;
        metaState.getLogicalFile(result, 1);
        // Empty path returns empty result
        CPPUNIT_ASSERT_EQUAL_STR("", result.str());
        END_TEST
    }

    void testLogicalFileUnmappedId()
    {
        START_TEST
        CMetaInfoState metaState;

        StringBuffer result;
        metaState.getLogicalFile(result, 999);
        // Unmapped file ID returns empty result
        CPPUNIT_ASSERT_EQUAL_STR("", result.str());
        END_TEST
    }

    void testLogicalFileMultiplePaths()
    {
        START_TEST
        CMetaInfoState metaState;
        const char* fullPath1 = "/var/lib/HPCCSystems/hpcc-data/roxie/dataset1/part1/12345";
        const char* fullPath2 = "/var/lib/HPCCSystems/hpcc-data/roxie/d2/dataset2/part2";
        const char* fullPath3 = "/custom/path/to/data";

        Owned<IEventVisitationLink> collector = startFileInjection(metaState);
        injectFileInformation(metaState, *collector, 1, fullPath1);
        injectFileInformation(metaState, *collector, 2, fullPath2);
        injectFileInformation(metaState, *collector, 3, fullPath3);
        endFileInjection(*collector);

        StringBuffer result1, result2, result3;
        metaState.getLogicalFile(result1, 1);
        metaState.getLogicalFile(result2, 2);
        metaState.getLogicalFile(result3, 3);

        CPPUNIT_ASSERT_EQUAL_STR("dataset1/part1", result1.str());
        CPPUNIT_ASSERT_EQUAL_STR("dataset2/part2", result2.str());
        CPPUNIT_ASSERT_EQUAL_STR("/custom/path/to/data", result3.str());
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(MetaInfoStateTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(MetaInfoStateTest, "MetaInfoStateTest");

#endif // _USE_CPPUNIT
