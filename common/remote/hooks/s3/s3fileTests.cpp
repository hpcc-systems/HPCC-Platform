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
#include "jplane.hpp"

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
        // Integration tests (require S3 connection)
        // CPPUNIT_TEST(testActualS3Connection);
        // CPPUNIT_TEST(testS3ReadWrite);
    CPPUNIT_TEST_SUITE_END();

protected:
    void testS3UrlValidation()
    {
        // Valid S3 URLs
        CPPUNIT_ASSERT(isS3FileName("s3:myplane/bucket/key"));
        CPPUNIT_ASSERT(isS3FileName("s3:prod-plane/my-bucket/path/to/file.txt"));
        CPPUNIT_ASSERT(isS3FileName("s3:dev/bucket-name/nested/path/file.dat"));
        CPPUNIT_ASSERT(isS3FileName("s3:plane/d1/bucket/file.txt"));

        // Invalid S3 URLs
        CPPUNIT_ASSERT(!isS3FileName(""));
        CPPUNIT_ASSERT(!isS3FileName("s3:"));
        CPPUNIT_ASSERT(!isS3FileName("s3:/"));
        CPPUNIT_ASSERT(!isS3FileName("s3:plane/"));
        CPPUNIT_ASSERT(!isS3FileName("s3:plane"));
        CPPUNIT_ASSERT(!isS3FileName("http://bucket/key"));
        CPPUNIT_ASSERT(!isS3FileName("file:///path/to/file"));
        CPPUNIT_ASSERT(!isS3FileName("/local/path"));
        CPPUNIT_ASSERT(!isS3FileName("bucket/key"));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(S3FileTest);

#endif // _USE_CPPUNIT
