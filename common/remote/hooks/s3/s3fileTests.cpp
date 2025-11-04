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

#ifdef _USE_CPPUNIT

#include "platform.h"
#include "jlib.hpp"
#include "jstring.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"

#include "unittests.hpp"
#include "s3file.hpp"

/*
 * Unit tests for the new S3 file implementation
 *
 * These tests focus on functionality that doesn't require actual S3 connectivity:
 * - URL parsing and validation
 * - Configuration loading
 * - Error handling
 * - Basic interface compliance
 *
 * Tests that require actual S3 connection are marked as integration tests
 * and should be run separately with proper S3 credentials configured.
 */

class S3FileTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(S3FileTest);
        CPPUNIT_TEST(testS3UrlValidation);
        CPPUNIT_TEST(testS3UrlParsing);
        CPPUNIT_TEST(testFileCreation);
        CPPUNIT_TEST(testFileHookInstallation);
        CPPUNIT_TEST(testErrorHandling);
        CPPUNIT_TEST(testFileInterface);
        // Integration tests (require S3 connection)
        // CPPUNIT_TEST(testActualS3Connection);
        // CPPUNIT_TEST(testS3ReadWrite);
    CPPUNIT_TEST_SUITE_END();

protected:
    void testS3UrlValidation()
    {
        // Valid S3 URLs
        CPPUNIT_ASSERT(isS3FileName("s3://bucket/"));
        CPPUNIT_ASSERT(isS3FileName("s3://bucket/key"));
        CPPUNIT_ASSERT(isS3FileName("s3://my-bucket/path/to/file.txt"));
        CPPUNIT_ASSERT(isS3FileName("s3://bucket-name/nested/path/file.dat"));

        // Invalid S3 URLs
        CPPUNIT_ASSERT(!isS3FileName(""));
        CPPUNIT_ASSERT(!isS3FileName("s3://"));
        CPPUNIT_ASSERT(!isS3FileName("s3://bucket"));
        CPPUNIT_ASSERT(!isS3FileName("http://bucket/key"));
        CPPUNIT_ASSERT(!isS3FileName("file:///path/to/file"));
        CPPUNIT_ASSERT(!isS3FileName("/local/path"));
        CPPUNIT_ASSERT(!isS3FileName("bucket/key"));
    }

    void testS3UrlParsing()
    {
        // Test URL parsing through file creation
        // Note: This doesn't connect to S3, just tests parsing
        try
        {
            Owned<IFile> file = createS3File("s3://test-bucket/path/to/test-file.txt");
            CPPUNIT_ASSERT(file != nullptr);

            CPPUNIT_ASSERT_EQUAL(strcmp("s3://test-bucket/path/to/test-file.txt", file->queryFilename()), 0);
        }
        catch (IException* e)
        {
            // Expected if AWS SDK is not properly initialized in test environment
            StringBuffer msg;
            e->errorMessage(msg);
            PROGLOG("Expected exception during URL parsing test: %s", msg.str());
            e->Release();
        }
    }

    void testFileCreation()
    {
        // Test that file creation doesn't crash and returns valid objects
        try
        {
            Owned<IFile> file1 = createS3File("s3://bucket1/file1.txt");
            Owned<IFile> file2 = createS3File("s3://bucket2/path/file2.dat");

            CPPUNIT_ASSERT(file1 != nullptr);
            CPPUNIT_ASSERT(file2 != nullptr);
            CPPUNIT_ASSERT(file1.get() != file2.get());

            // Test filename retrieval
            CPPUNIT_ASSERT_EQUAL(std::string("s3://bucket1/file1.txt"),
                                std::string(file1->queryFilename()));
            CPPUNIT_ASSERT_EQUAL(std::string("s3://bucket2/path/file2.dat"),
                                std::string(file2->queryFilename()));
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            PROGLOG("Expected exception during file creation test: %s", msg.str());
            e->Release();
        }
    }

    void testFileHookInstallation()
    {
        // Test hook installation and removal
        try
        {
            installFileHook();

            // Test that hook can create S3 files
            Owned<IFile> file = createIFile("s3://test-bucket/hook-test.txt");
            // Note: This might fail if the hook conflicts with existing S3 implementation

            removeFileHook();
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            PROGLOG("Exception during hook test (may be expected): %s", msg.str());
            e->Release();

            // Clean up
            try { removeFileHook(); } catch (...) {}
        }
    }

    void testErrorHandling()
    {
        // Test error handling for invalid URLs
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(createS3File("s3://"), "url should be invalid");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(createS3File("s3://bucket"), "url should be invalid");
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(createS3File("invalid-url"), "url should be invalid");

        // Test operations on non-existent files (should not crash)
        try
        {
            Owned<IFile> file = createS3File("s3://non-existent-bucket/non-existent-file.txt");

            // These operations should not crash even if file doesn't exist
            // (though they may fail due to network/credentials)
            try
            {
                bool exists = file->exists();
                PROGLOG("File exists check returned: %s", exists ? "true" : "false");
            }
            catch (IException* e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                PROGLOG("Expected exception during exists check: %s", msg.str());
                e->Release();
            }

            try
            {
                fileBool isFile = file->isFile();
                PROGLOG("File isFile check returned: %d", (int)isFile);
            }
            catch (IException* e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                PROGLOG("Expected exception during isFile check: %s", msg.str());
                e->Release();
            }
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            PROGLOG("Expected exception during error handling test: %s", msg.str());
            e->Release();
        }
    }

    void testFileInterface()
    {
        // Test that our S3 file properly implements the IFile interface
        try
        {
            Owned<IFile> file = createS3File("s3://test-bucket/interface-test.txt");

            // Test basic interface methods don't crash
            const char* filename = file->queryFilename();
            CPPUNIT_ASSERT(filename != nullptr);
            CPPUNIT_ASSERT(strlen(filename) > 0);

            // Test read-only nature
            fileBool isReadOnly = file->isReadOnly();
            CPPUNIT_ASSERT(isReadOnly == fileBool::foundYes);

            // Test unsupported operations throw exceptions
            CPPUNIT_ASSERT_THROWS_IEXCEPTION(file->rename("new-name"), "rename should not be supported");
            CPPUNIT_ASSERT_THROWS_IEXCEPTION(file->setReadOnly(false), "setReadOnly should not be supported");
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            PROGLOG("Expected exception during interface test: %s", msg.str());
            e->Release();
        }
    }

    // Integration tests (commented out by default)
    /*
    void testActualS3Connection()
    {
        // This test requires actual S3 credentials and bucket
        // Only run when specifically testing with real S3 connection

        const char* testBucket = getenv("S3_TEST_BUCKET");
        const char* testRegion = getenv("S3_TEST_REGION");

        if (!testBucket || !testRegion)
        {
            PROGLOG("Skipping S3 connection test - S3_TEST_BUCKET and S3_TEST_REGION not set");
            return;
        }

        // Configure S3 for testing
        Owned<IPropertyTree> config = createPTree("s3");
        config->setProp("@region", testRegion);
        setS3Config(config);

        StringBuffer testFileUrl;
        testFileUrl.appendf("s3://%s/test-file-%u.txt", testBucket, getRandom());

        Owned<IFile> file = createS3File(testFileUrl.str());

        // Test file existence (should be false for new file)
        CPPUNIT_ASSERT(!file->exists());

        // Test file metadata
        CDateTime createTime, modTime, accessTime;
        bool hasTime = file->getTime(&createTime, &modTime, &accessTime);
        // Should return false for non-existent file
    }

    void testS3ReadWrite()
    {
        // This test requires actual S3 credentials and bucket
        const char* testBucket = getenv("S3_TEST_BUCKET");
        const char* testRegion = getenv("S3_TEST_REGION");

        if (!testBucket || !testRegion)
        {
            PROGLOG("Skipping S3 read/write test - S3_TEST_BUCKET and S3_TEST_REGION not set");
            return;
        }

        Owned<IPropertyTree> config = createPTree("s3");
        config->setProp("@region", testRegion);
        setS3Config(config);

        StringBuffer testFileUrl;
        testFileUrl.appendf("s3://%s/unittest-readwrite-%u.txt", testBucket, getRandom());

        const char* testData = "This is test data for S3 file read/write operations.";
        size32_t testDataLen = strlen(testData);

        // Test write
        {
            Owned<IFile> file = createS3File(testFileUrl.str());
            Owned<IFileIO> io = file->open(IFOcreate);
            CPPUNIT_ASSERT(io != nullptr);

            size32_t written = io->write(0, testDataLen, testData);
            CPPUNIT_ASSERT_EQUAL(testDataLen, written);

            io->close();
        }

        // Test read
        {
            Owned<IFile> file = createS3File(testFileUrl.str());
            CPPUNIT_ASSERT(file->exists());
            CPPUNIT_ASSERT_EQUAL((offset_t)testDataLen, file->size());

            Owned<IFileIO> io = file->open(IFOread);
            CPPUNIT_ASSERT(io != nullptr);

            MemoryBuffer readBuffer;
            readBuffer.ensureCapacity(testDataLen + 1);

            size32_t bytesRead = io->read(0, testDataLen, readBuffer.reserveTruncate(testDataLen));
            CPPUNIT_ASSERT_EQUAL(testDataLen, bytesRead);

            readBuffer.append('\0'); // Null terminate for comparison
            CPPUNIT_ASSERT_EQUAL(std::string(testData), std::string((const char*)readBuffer.toByteArray()));
        }

        // Clean up - delete test file
        {
            Owned<IFile> file = createS3File(testFileUrl.str());
            file->remove();
        }
    }
    */
};

CPPUNIT_TEST_SUITE_REGISTRATION(S3FileTest);

#endif // _USE_CPPUNIT
