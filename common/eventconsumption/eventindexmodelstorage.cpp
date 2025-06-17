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

#include "eventindexmodel.hpp"
#include "jexcept.hpp"

bool operator < (const Storage::Plane& left, const Storage::Plane& right) { return strcmp(left.name.str(), right.name.str()) < 0; }
bool operator < (const Storage::Plane& left, const char* right) { return strcmp(left.name.str(), right) < 0; }
bool operator < (const char* left, const Storage::Plane& right) { return strcmp(left, right.name.str()) < 0; }

bool operator < (const Storage::File& left, const Storage::File& right) { return strcmp(left.path.str(), right.path.str()) < 0; }
bool operator < (const Storage::File& left, const char* right) { return strcmp(left.path.str(), right) < 0; }
bool operator < (const char* left, const Storage::File& right) { return strcmp(left, right.path.str()) < 0; }

Storage::File::File(const char* _path, const Plane& _plane) : path(_path)
{
    planes[0] = &_plane;
    planes[1] = &_plane;
}

const Storage::Plane& Storage::File::lookupPlane(__uint64 nodeKind) const
{
    assertex(nodeKind < 2);
    return *planes[nodeKind];
}

void Storage::PageCache::configure(const IPropertyTree& config)
{
    readTime = __uint64(config.getPropInt64("@cache-read", readTime));
    if (readTime)
        capacity = __uint64(config.getPropInt64("@cache-capacity", capacity));
}

__uint64 Storage::PageCache::find(const Key& key)
{
    if (!readTime)
        return 0;
    Hash::iterator hashIt = hash.find(key);
    if (hash.end() == hashIt)
        return 0;
    MRU::iterator mruIt = std::find(mru.begin(), mru.end(), &(*hashIt));
    if (mru.end() == mruIt)
        throw makeStringExceptionV(-1, "page cache MRU unexpectedly missing key %llu, %llu", key.fileId, key.offset);
    mru.splice(mru.begin(), mru, mruIt);
    return readTime;
}

bool Storage::PageCache::insert(const Key& key)
{
    if (!readTime)
        return false;
    if (find(key))
        return false;
    reserve(DefaultPageSize);
    Hash::iterator hashIt = hash.insert(key).first;
    mru.push_front(&(*hashIt));
    used += DefaultPageSize;
    return true;
}

void Storage::PageCache::reserve(__uint64 request)
{
    if (!capacity)
        return;
    if (capacity < request)
        throw makeStringExceptionV(-1, "page cache capacity %llu less than reserved page request %llu", capacity, request);
    while ((capacity - used) < request)
    {
        if (mru.empty())
            throw makeStringException(-1, "page cache MRU unexpectedly empty");
        used -= request;
        hash.erase(*mru.back());
        mru.pop_back();
    }
}

void Storage::configure(const IPropertyTree& config)
{
    cache.configure(config);
    configurePlanes(config);
    configureFiles(config);
}

void Storage::observeFile(__uint64 fileId, const char* path)
{
    const File& mappedFile = lookupFile(path);
    const File*& observation = observedFiles[fileId];
    if (!observation)
        observation = &mappedFile;
    else
        // Conflicting observations are always an error. Conflicting observations from a single
        // file indicate a bug in the event recording. Conflicting observations from multiple files
        // may indicate an event recording bug or the files are incompatible.
        assertex(&mappedFile == observation);
}

void Storage::describePage(__uint64 fileId, __uint64 offset, __uint64 nodeKind, ModeledPage& page) const
{
    const File& file = lookupFile(fileId);
    page.fileId = fileId;
    page.offset = offset;
    page.nodeKind = nodeKind;
    page.readTime = cache.find(fileId, offset);
    if (!page.readTime)
    {
        const Plane& plane = file.lookupPlane(nodeKind);
        page.readTime = plane.readTime;
        // MORE: if multiple pages should be loaded, add all to the cache, with the requested page
        // added last.
        (void)cache.insert(fileId, offset);
    }
}

void Storage::configurePlanes(const IPropertyTree& config)
{
    Owned<IPropertyTreeIterator> it = config.getElements("plane");
    ForEach(*it)
    {
        Plane plane;
        plane.name.set(it->query().queryProp("@name"));
        plane.readTime = it->query().getPropInt("@readTime", plane.readTime);
        if (plane.name.isEmpty() || !plane.readTime)
            throw makeStringException(-1, "invalid storage plane configuration - both @name and @readTime are required");
        auto[ planeIt, inserted ] = planes.insert(plane);
        if (!inserted)
            throw makeStringExceptionV(-1, "duplicate storage plane name '%s'", plane.name.str());
        else if (!defaultPlane)
            defaultPlane = &*planeIt;
    }
    if (planes.empty())
        throw makeStringException(-1, "missing storage plane configurations");
}

const Storage::Plane& Storage::lookupPlane(const char* name, const char* forFile) const
{
    if (!isEmptyString(name))
    {
        Planes::const_iterator planeIt = planes.find(name);
        if (planeIt != planes.end())
            return *planeIt;
        if (isEmptyString(forFile))
            throw makeStringExceptionV(-1, "unrecognized storage plane name '%s' in default storage file configuration", name);
        throw makeStringExceptionV(-1, "unrecognized storage plane name '%s' in storage file configuration '%s'", name, forFile);
    }
    assertex(defaultPlane);
    return *defaultPlane;
}

void Storage::configureFiles(const IPropertyTree& config)
{
    bool haveDefault = false;
    Owned<IPropertyTreeIterator> it = config.getElements("file");
    ForEach(*it)
    {
        File file;
        const IPropertyTree& fileConfig = it->query();
        file.path.set(fileConfig.queryProp("@path"));
        // allow the configuration to place the entire file in a single plane
        if (fileConfig.hasProp("@plane"))
        {
            const Plane& plane = lookupPlane(fileConfig.queryProp("@plane"), file.path.str());
            for (size_t i = 0; i < File::NumPlanes; ++i)
                file.planes[i] = &plane;
        }
        // branch nodes may be stored separately from leaf nodes
        if (fileConfig.hasProp("@branchPlane"))
        {
            const Plane& plane = lookupPlane(fileConfig.queryProp("@branchPlane"), file.path.str());
            file.planes[0] = &plane;
        }
        else if (!file.planes[0])
            file.planes[0] = defaultPlane;
        // leaf nodes may be stored separately from branch nodes
        if (fileConfig.hasProp("@leafPlane"))
        {
            const Plane& plane = lookupPlane(fileConfig.queryProp("@leafPlane"), file.path.str());
            file.planes[1] = &plane;
        }
        else if (!file.planes[1])
            file.planes[1] = defaultPlane;
        if (!configuredFiles.insert(file).second)
            throw makeStringExceptionV(-1, "duplicate file path '%s'", file.path.str());
        if (isEmptyString(file.path.get()))
            haveDefault = true;
    }
    if (!haveDefault)
        configuredFiles.insert(File(nullptr, lookupPlane(nullptr, nullptr)));
}

const Storage::File& Storage::lookupFile(const char* path) const
{
    Files::const_iterator fileIt;
    if (!isEmptyString(path))
    {
        fileIt = configuredFiles.find(path);
        if (fileIt != configuredFiles.end())
            return *fileIt;
    }
    fileIt = configuredFiles.find("");
    assertex(fileIt != configuredFiles.end());
    return *fileIt;
}

const Storage::File& Storage::lookupFile(__uint64 fileId) const
{
    ObservedFiles::const_iterator obsevedIt = observedFiles.find(fileId);
    if (obsevedIt != observedFiles.end())
        return *obsevedIt->second;
    // An unobserved file ID can only map to the default file. This happening indicates a bug in
    // the event recording causing MetaFileInformation to not be recorded for all files.
    Files::const_iterator configuredIt = configuredFiles.find("");
    assertex(configuredIt != configuredFiles.end());
    return *configuredIt;
}

#ifdef _USE_CPPUNIT

#include "unittests.hpp"

class IndexModelStorageTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(IndexModelStorageTest);
    CPPUNIT_TEST(testPageCache);
    CPPUNIT_TEST_SUITE_END();

public:
    void testPageCache()
    {
        constexpr const char* configText =
R"!!!(cache-capacity: 16384
cache-read: 100
)!!!";
        Storage::PageCache cache;
        Owned<IPropertyTree> configTree = createPTreeFromYAMLString(configText);
        cache.configure(*configTree);
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.readTime);
        CPPUNIT_ASSERT_EQUAL(16384ULL, cache.capacity);
        CPPUNIT_ASSERT(!cache.find(1, 0));
        CPPUNIT_ASSERT(cache.insert(1, 0));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 0));
        CPPUNIT_ASSERT(!cache.insert(1, 0));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 0));
        CPPUNIT_ASSERT(cache.insert(1, 8192));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 0));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 8192));
        CPPUNIT_ASSERT_EQUAL(0ULL, cache.find(1, 16384));
        CPPUNIT_ASSERT(cache.insert(1, 16384));
        CPPUNIT_ASSERT_EQUAL(0ULL, cache.find(1, 0));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 8192));
        CPPUNIT_ASSERT_EQUAL(100ULL, cache.find(1, 16384));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(IndexModelStorageTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(IndexModelStorageTest, "indexmodelstorage");

#endif // _USE_CPPUNIT
