/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "unittests.hpp"
#include "xref.hpp"
#include <thread>
#include <vector>
#include <mutex>
#include <string>


class XRefAllocatorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(XRefAllocatorTest);
        CPPUNIT_TEST(testBasicAllocation);
        CPPUNIT_TEST(testDeallocation);
        CPPUNIT_TEST(testMemoryLimit);
    CPPUNIT_TEST_SUITE_END();

public:
    void testBasicAllocation()
    {
        const int numThreads = 10;
        const size_t allocMBs = 1;
        const size_t allocSize = allocMBs * 1024 * 1024;
        XRefAllocator allocator(numThreads * allocMBs);
        std::vector<std::thread> threads;
        threads.reserve(numThreads);
        std::vector<void*> allocatedPtrs(numThreads);
        bool allAllocationsSucceeded = true;

        // Lambda function for each thread to allocate memory
        auto threadFunc = [&](int threadId)
        {
            try
            {
                void *ptr = allocator.alloc(allocSize);
                if (ptr != nullptr)
                    allocatedPtrs[threadId] = ptr;
                else
                    allAllocationsSucceeded = false;
            }
            catch (...)
            {
                allAllocationsSucceeded = false;
            }
        };

        // Create and start 10 threads
        for (int i = 0; i < numThreads; i++)
            threads.emplace_back(threadFunc, i);

        for (auto& thread : threads)
            thread.join();

        // Verify all allocations succeeded
        CPPUNIT_ASSERT(allAllocationsSucceeded);

        // Verify all pointers are non-null and unique
        for (int i = numThreads-1; i >= 0; i--)
        {
            CPPUNIT_ASSERT(allocatedPtrs[i] != nullptr);

            // Check that this pointer is unique among all allocated pointers
            for (int j = i - 1; j >= 0; j--)
                CPPUNIT_ASSERT(allocatedPtrs[i] != allocatedPtrs[j]);

            // Deallocate memory
            allocator.dealloc(allocatedPtrs[i], allocSize);
            allocatedPtrs.pop_back();
        }
    }

    void testDeallocation()
    {
        XRefAllocator allocator(1); // 1MB limit

        void *ptr1 = allocator.alloc(512);
        void *ptr2 = allocator.alloc(512);

        CPPUNIT_ASSERT(ptr1 != nullptr);
        CPPUNIT_ASSERT(ptr2 != nullptr);
        CPPUNIT_ASSERT(ptr1 != ptr2);

        allocator.dealloc(ptr1, 512);
        allocator.dealloc(ptr2, 512);
    }

    void testMemoryLimit()
    {
        XRefAllocator allocator(1); // 1MB limit

        // Try to allocate more than the limit
        try {
            void *ptr = allocator.alloc(2 * 1024 * 1024); // 2MB
            allocator.dealloc(ptr, 2 * 1024 * 1024); // cleanup in case test fails
            CPPUNIT_FAIL("Should have thrown an exception for memory limit exceeded");
        }
        catch (...)
        {
            // Expected behavior
        }
    }
};


class XRefcMisplacedRecTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(XRefcMisplacedRecTest);
        CPPUNIT_TEST(testCreation);
        CPPUNIT_TEST(testInitialization);
        CPPUNIT_TEST(testEquality);
        CPPUNIT_TEST(testGetters);
    CPPUNIT_TEST_SUITE_END();

public:
    void testCreation()
    {
        XRefAllocator allocator(1); // 1MB limit
        cMisplacedRec *rec = cMisplacedRec::create(&allocator);

        CPPUNIT_ASSERT(rec != nullptr);
        CPPUNIT_ASSERT(rec->allocator == &allocator);

        delete rec;
    }

    void testInitialization()
    {
        XRefAllocator allocator(1);
        cMisplacedRec *rec = cMisplacedRec::create(&allocator);

        rec->init(0, 5, 2, 4); // drv=0, part=5, node=2, totalNodes=4

        CPPUNIT_ASSERT_EQUAL((unsigned short)5, rec->pn);
        CPPUNIT_ASSERT_EQUAL((unsigned short)2, rec->nn);
        CPPUNIT_ASSERT_EQUAL(false, rec->marked);
        CPPUNIT_ASSERT(rec->next == nullptr);

        delete rec;
    }

    void testEquality()
    {
        XRefAllocator allocator(1);
        cMisplacedRec *rec = cMisplacedRec::create(&allocator);

        rec->init(0, 5, 2, 4);

        CPPUNIT_ASSERT(rec->eq(0, 5, 2, 4));
        CPPUNIT_ASSERT(!rec->eq(0, 6, 2, 4)); // different part
        CPPUNIT_ASSERT(!rec->eq(0, 5, 3, 4)); // different node

        delete rec;
    }

    void testGetters()
    {
        XRefAllocator allocator(1);
        cMisplacedRec *rec = cMisplacedRec::create(&allocator);

        rec->init(1, 5, 2, 4); // drv=1, part=5, node=2, totalNodes=4

        CPPUNIT_ASSERT_EQUAL(1U, rec->getDrv(4));
        CPPUNIT_ASSERT_EQUAL(2U, rec->getNode(4));

        delete rec;
    }
};


class XRefcFileDescTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(XRefcFileDescTest);
        CPPUNIT_TEST(testCreation);
        CPPUNIT_TEST(testPresentBits);
        CPPUNIT_TEST(testMarkedBits);
        CPPUNIT_TEST(testHPCCFile);
        CPPUNIT_TEST(testGetNameAndMask);
        CPPUNIT_TEST(testGetPartName);
    CPPUNIT_TEST_SUITE_END();

public:
    void testCreation()
    {
        XRefAllocator allocator(1);
        const char *testName = "testfile.$N$_of_$P$";
        cFileDesc *file = cFileDesc::create(testName, 400, true, 8, &allocator);

        CPPUNIT_ASSERT(file != nullptr);
        CPPUNIT_ASSERT(file->eq(testName));
        CPPUNIT_ASSERT_EQUAL((unsigned short)400, file->N);
        CPPUNIT_ASSERT_EQUAL(true, file->isDirPerPart);
        CPPUNIT_ASSERT_EQUAL((byte)8, file->filenameLen);
        CPPUNIT_ASSERT(file->eq(testName));
        CPPUNIT_ASSERT(!file->eq("differentfile.$N$_of_$P$"));

        delete file;
    }

    void testPresentBits()
    {
        XRefAllocator allocator(1);
        cFileDesc *file = cFileDesc::create("testfile.$N$_of_$P$", 400, true, 8, &allocator);

        // Test setting and getting present bits
        CPPUNIT_ASSERT_EQUAL(false, file->setpresent(0, 1));    // should return false (not previously set)
        CPPUNIT_ASSERT_EQUAL(true, file->testpresent(0, 1));    // should now be set
        CPPUNIT_ASSERT_EQUAL(true, file->setpresent(0, 1));     // should return true (previously set)
        CPPUNIT_ASSERT_EQUAL(false, file->testpresent(0, 400)); // should be false (not set)
        CPPUNIT_ASSERT_EQUAL(false, file->setpresent(0, 400));  // should return false (not previously set)
        CPPUNIT_ASSERT_EQUAL(true, file->testpresent(0, 400));  // should now be set

        delete file;
    }

    void testMarkedBits()
    {
        XRefAllocator allocator(1);
        cFileDesc *file = cFileDesc::create("testfile.$N$_of_$P$", 400, true, 8, &allocator);

        // Test setting and getting marked bits
        CPPUNIT_ASSERT_EQUAL(false, file->setmarked(0, 1));    // should return false (not previously set)
        CPPUNIT_ASSERT_EQUAL(true, file->testmarked(0, 1));    // should now be set
        CPPUNIT_ASSERT_EQUAL(true, file->setmarked(0, 1));     // should return true (previously set)
        CPPUNIT_ASSERT_EQUAL(false, file->testmarked(0, 400)); // should be false (not set)
        CPPUNIT_ASSERT_EQUAL(false, file->setmarked(0, 400));  // should return false (not previously set)
        CPPUNIT_ASSERT_EQUAL(true, file->testmarked(0, 400));  // should now be set

        delete file;
    }

    void testHPCCFile()
    {
        XRefAllocator allocator(1);

        cFileDesc *hpccFile = cFileDesc::create("testfile.$N$_of_$P$", 4, false, 8, &allocator);
        CPPUNIT_ASSERT(hpccFile->isHPCCFile());

        // For non-HPCC files, filenameLen will be 0 because deduceMask fails to find a partmask
        cFileDesc *nonHpccFile = cFileDesc::create("testfile.dat", 4, false, 0, &allocator);
        CPPUNIT_ASSERT(!nonHpccFile->isHPCCFile());

        delete hpccFile;
        delete nonHpccFile;
    }

    void testGetNameAndMask()
    {
        XRefAllocator allocator(1);

        // Test HPCC file with filename length
        cFileDesc *hpccFile = cFileDesc::create("testfile.$N$_of_$P$", 4, false, 8, &allocator);

        StringBuffer nameBuf;
        CPPUNIT_ASSERT(hpccFile->getName(nameBuf));
        CPPUNIT_ASSERT_EQUAL(std::string("testfile"), std::string(nameBuf.str()));

        StringBuffer maskBuf;
        hpccFile->getNameMask(maskBuf);
        CPPUNIT_ASSERT_EQUAL(std::string("testfile.$N$_of_$P$"), std::string(maskBuf.str()));

        delete hpccFile;

        // Test non-HPCC file (no filename length)
        cFileDesc *nonHpccFile = cFileDesc::create("regularfile.dat", 1, false, 0, &allocator);

        StringBuffer nameBuf2;
        CPPUNIT_ASSERT(!nonHpccFile->getName(nameBuf2)); // Should return false

        delete nonHpccFile;
    }

    void testGetPartName()
    {
        XRefAllocator allocator(1);
        cFileDesc *file = cFileDesc::create("testfile.$N$_of_$P$", 4, false, 8, &allocator);

        StringBuffer partName;
        file->getPartName(partName, 1);

        // Should expand the mask with part number
        // Note: The exact expansion depends on expandMask implementation
        CPPUNIT_ASSERT(partName.length() > 0);
        CPPUNIT_ASSERT(strstr(partName.str(), "testfile") != nullptr);

        delete file;
    }
};


class XRefcDirDescTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(XRefcDirDescTest);
        CPPUNIT_TEST(testCreation);
        CPPUNIT_TEST(testEmpty);
        CPPUNIT_TEST(testAddFile);
        CPPUNIT_TEST(testIsMisplaced);
    CPPUNIT_TEST_SUITE_END();

public:
    void testCreation()
    {
        XRefAllocator allocator(1);
        const char *testName = "directory";

        cDirDesc *dir = cDirDesc::create(testName, &allocator);

        CPPUNIT_ASSERT(dir != nullptr);
        CPPUNIT_ASSERT(dir->eq(testName));
        CPPUNIT_ASSERT(!dir->eq("directory2"));

        delete dir;
    }

    void testEmpty()
    {
        XRefAllocator allocator(1);
        cDirDesc *dir = cDirDesc::create("directory", &allocator);

        // Initially, directory should be empty
        CPPUNIT_ASSERT(dir->empty(0));
        CPPUNIT_ASSERT(dir->empty(1));

        // Add a file to the directory
        const char *testName = "testfile.$N$_of_$P$";
        cFileDesc *file = cFileDesc::create(testName, 400, true, 8, &allocator);
        dir->files.emplace(testName, file);
        CPPUNIT_ASSERT(!dir->empty(0));
        CPPUNIT_ASSERT(!dir->empty(1));

        delete dir;
    }

    void testAddFile()
    {
        XRefAllocator allocator(1);
        cDirDesc *dir = cDirDesc::create("directory", &allocator);

        // Add a file to the directory
        unsigned numNodes = isContainerized() ? 1 : 456;
        SocketEndpointArray epa;
        // Populate the endpoint array with the required number of endpoints
        for (unsigned i = 0; i < numNodes; i++)
        {
            SocketEndpoint ep;
            ep.set("127.0.0.1", 7070 + i); // Use localhost with different ports
            epa.append(ep);
        }
        Owned<IGroup> grp = createIGroup(epa);
        unsigned stripeNum = calcStripeNumber(122, "testfile", 20);
        unsigned nodeNum = isContainerized() ? 0 : 122;
        dir->addFile(0, "testfile._123_of_456", "/home/test/", 11, 0, epa.item(nodeNum), *grp, 1, stripeNum, 20, &allocator);
        CPPUNIT_ASSERT(!dir->empty(0));
        CPPUNIT_ASSERT(!dir->empty(1));

        // Check file was created properly
        cFileDesc *file = dir->files["testfile._$P$_of_456"].get();
        CPPUNIT_ASSERT(file != nullptr);
        CPPUNIT_ASSERT(file->misplaced == nullptr);

        delete dir;
    }

    void testIsMisplaced()
    {
        XRefAllocator allocator(1);
        cDirDesc *dir = cDirDesc::create("directory", &allocator);

        // Test external files (numParts == NotFound) - should always return true
        SocketEndpointArray emptyEpa;
        Owned<IGroup> emptyGroup = createIGroup(emptyEpa);
        CPPUNIT_ASSERT(dir->isMisplaced(0, NotFound, SocketEndpoint(), *emptyGroup, "/test/file.dat", 5, 1, 1));

        if (isContainerized())
        {
            // Test stripe number validation - invalid stripe numbers should return true
            CPPUNIT_ASSERT(dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/file._1_of_4", 5, 0, 2)); // stripeNum < 1
            CPPUNIT_ASSERT(dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/file._1_of_4", 5, 3, 2)); // stripeNum > numStripedDevices

            // Test dir-per-part detection

            // Valid dir-per-part where part matches directory number
            CPPUNIT_ASSERT(!dir->isMisplaced(41, 4, SocketEndpoint(), *emptyGroup, "/test/scope/42/file._42_of_45", 11, 1, 1));

            // Invalid dir-per-part where part doesn't match directory number
            CPPUNIT_ASSERT(dir->isMisplaced(19, 40, SocketEndpoint(), *emptyGroup, "/test/scope/21/file._20_of_40", 11, 1, 1));

            // Directory number too large (beyond numParts) - should be ignored, not considered misplaced
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/scope/999/file._1_of_4", 11, 1, 1));
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/scope/20250101/file._1_of_4", 11, 1, 1));

            // Test files in root directory (no dir-per-part to check)
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/file._1_of_4", 0, 1, 1));

            // Test striped files
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/scope/subdir/file._1_of_4", 11, calcStripeNumber(0, "subdir::file", 20), 20));
            CPPUNIT_ASSERT(!dir->isMisplaced(99, 4, SocketEndpoint(), *emptyGroup, "/test/scope/subdir/100/file._100_of_400", 11, calcStripeNumber(99, "subdir::file", 20), 20));

            // Test edge case with single digit dir-per-part
            CPPUNIT_ASSERT(!dir->isMisplaced(2, 4, SocketEndpoint(), *emptyGroup, "/test/scope/3/file._3_of_4", 11, 1, 1));

            // Test leading zeros in directory names (should still parse correctly)
            // MORE: I don't think an actual dir-per-part will have leading zeros. This may be an edge case to handle.
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, SocketEndpoint(), *emptyGroup, "/test/scope/01/file._1_of_4", 11, 1, 1));
        }
        else
        {
            // Test non-containerized paths
            SocketEndpointArray epa;
            for (unsigned i = 0; i < 4; i++)
            {
                SocketEndpoint ep;
                ep.set("127.0.0.1", 7070 + i);
                epa.append(ep);
            }
            Owned<IGroup> grp = createIGroup(epa);

            // Test mismatched group size
            CPPUNIT_ASSERT(dir->isMisplaced(0, 5, epa.item(0), *grp, "/test/file._1_of_5", 5, 1, 1)); // numParts != grp.ordinality()

            // Test part number out of range
            CPPUNIT_ASSERT(dir->isMisplaced(4, 4, epa.item(0), *grp, "/test/file._5_of_4", 5, 1, 1)); // partNum >= grp.ordinality()

            // Test endpoint mismatch
            SocketEndpoint wrongEp;
            wrongEp.set("192.168.1.1", 8080);
            CPPUNIT_ASSERT(dir->isMisplaced(0, 4, wrongEp, *grp, "/test/file._1_of_4", 5, 1, 1)); // endpoint not in group

            // Test correct placement
            CPPUNIT_ASSERT(!dir->isMisplaced(0, 4, epa.item(0), *grp, "/test/file._1_of_4", 5, 1, 1)); // correct placement
            CPPUNIT_ASSERT(!dir->isMisplaced(1, 4, epa.item(1), *grp, "/test/file._2_of_4", 5, 1, 1)); // correct placement
        }

        delete dir;
    }
};


CPPUNIT_TEST_SUITE_REGISTRATION(XRefAllocatorTest);
CPPUNIT_TEST_SUITE_REGISTRATION(XRefcMisplacedRecTest);
CPPUNIT_TEST_SUITE_REGISTRATION(XRefcFileDescTest);
CPPUNIT_TEST_SUITE_REGISTRATION(XRefcDirDescTest);
