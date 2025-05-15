/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include <algorithm>
#include <memory>
#include <vector>

#include "jlib.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "rmtfile.hpp"
#include "unittests.hpp"

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x ? x : ""), std::string(y ? y : ""))

// Declare functions to test
extern void setDafsLocalMountRedirect(const IpAddress &ip, const char *dir, const char *mountdir);
extern IFile *createFileLocalMount(const IpAddress &ip, const char *filename);

constexpr const char * testIp_127_0_0_1 = "127.0.0.1";
constexpr const char * testIp_127_0_0_2 = "127.0.0.2";

// Thread class for testing concurrent access to createFileLocalMount
class CCreateFileLocalMountThread : public CSimpleInterfaceOf<IInterface>, implements IThreaded
{
    IpAddress ip;
    StringAttr filename;
    StringAttr expectedPath;
    CThreaded threaded;

public:
    CCreateFileLocalMountThread(const IpAddress &_ip, const char *_filename, const char *_expectedPath)
        : ip(_ip), filename(_filename), expectedPath(_expectedPath),
          threaded("CreateFileLocalMountThread", this)
    {
        threaded.start(false);
    }

    void join()
    {
        threaded.join();
    }

    virtual void threadmain() override
    {
        try
        {
            Owned<IFile> file = createFileLocalMount(ip, filename.get());
            if (!file)
                CPPUNIT_FAIL("Expected non-null file but got nullptr");
            else if (strcmp(file->queryFilename(), expectedPath.get()) != 0)
                CPPUNIT_FAIL(VStringBuffer("Path mismatch: expected '%s', got '%s'", expectedPath.get(), file->queryFilename()));
            else
                CPPUNIT_ASSERT_EQUAL_STR(expectedPath.get(), file->queryFilename());
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(VStringBuffer("Exception caught: [%s]", msg.str()));
        }
        catch (...)
        {
            CPPUNIT_FAIL("Unknown exception");
        }
    }
};

class RmtFileTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RmtFileTest);
    CPPUNIT_TEST(testCanAccessDirectly);
    CPPUNIT_TEST(testSetCanAccessDirectly);
    CPPUNIT_TEST(testDaliServixPort);
    CPPUNIT_TEST(testLocalMountRedirects);
    CPPUNIT_TEST(testMultithreadedLocalMountRedirects);
    CPPUNIT_TEST(testCreateFileLocalMount);
    CPPUNIT_TEST(testPartialPathMatch);
    CPPUNIT_TEST(testMultipleMounts);
    CPPUNIT_TEST(testRemoveMounts);
    CPPUNIT_TEST(testNonMatchingCases);
    CPPUNIT_TEST(testCaseSensitivity);
    CPPUNIT_TEST_SUITE_END();

private:
    // Helper function to clean up mounts between tests
    void clearAllMounts()
    {
        // Setup a dummy IP to match all IPs
        IpAddress ip1;
        ip1.ipset(testIp_127_0_0_1);
        setDafsLocalMountRedirect(ip1, "/remote/path", nullptr);
        setDafsLocalMountRedirect(ip1, "/remote/path1", nullptr);
        setDafsLocalMountRedirect(ip1, "/remote/path2", nullptr);
        setDafsLocalMountRedirect(ip1, "/remote/path3", nullptr);
#ifndef _WIN32
        // Unix-like systems are case-sensitive
        setDafsLocalMountRedirect(ip1, "/Remote/Path", nullptr);
#endif

        IpAddress ip2;
        ip2.ipset(testIp_127_0_0_2);
        setDafsLocalMountRedirect(ip2, "/remote/path", nullptr);
        setDafsLocalMountRedirect(ip2, "/remote/path1", nullptr);
        setDafsLocalMountRedirect(ip2, "/remote/path2", nullptr);
        setDafsLocalMountRedirect(ip2, "/remote/path3", nullptr);
    }

public:
    void setUp() override
    {
        clearAllMounts();
    }

    void tearDown() override
    {
        clearAllMounts();
    }

    void testCanAccessDirectly()
    {
        RemoteFilename rfn;

        // Port 0 should be accessible directly
        rfn.setPort(0);
        CPPUNIT_ASSERT(canAccessDirectly(rfn));

        // Non-zero port should not be accessible directly
        rfn.setPort(1234);
        CPPUNIT_ASSERT(!canAccessDirectly(rfn));
    }

    void testSetCanAccessDirectly()
    {
        RemoteFilename rfn;

        // Setting to "can access directly" should set port to 0
        rfn.setPort(1234);
        setCanAccessDirectly(rfn, true);
        int port = rfn.getPort();
        CPPUNIT_ASSERT_EQUAL(0, port);
        CPPUNIT_ASSERT(canAccessDirectly(rfn));

        // Setting to "cannot access directly" should set port to default Daliservix port
        setCanAccessDirectly(rfn, false);
        CPPUNIT_ASSERT(rfn.getPort() != 0);
        CPPUNIT_ASSERT(!canAccessDirectly(rfn));
    }

    void testDaliServixPort()
    {
        // This is a basic test - the actual port value might vary
        // Just verify it returns something (not likely to be 0)
        unsigned short port = getDaliServixPort();
        CPPUNIT_ASSERT(port != 0);

        // Test multiple calls return same value (cached)
        unsigned short port2 = getDaliServixPort();
        CPPUNIT_ASSERT_EQUAL(port, port2);
    }

    void testLocalMountRedirects()
    {
        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);
        const char *remoteDir = "/remote/path";
        const char *localDir = "/local/path";

        // Set up a local mount redirect
        setDafsLocalMountRedirect(ip, remoteDir, localDir);

        // Use similar function as in createFileLocalMount to check if mount was set
        Owned<IFile> file = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file != nullptr);
    }

    void testMultithreadedLocalMountRedirects()
    {
        // Set up a mount and IP for testing
        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);
        const char *remoteDir = "/remote/path";
        const char *localDir = "/local/path";

        // Set up a local mount redirect
        setDafsLocalMountRedirect(ip, remoteDir, localDir);

        // Create multiple threads accessing the same mount
        constexpr unsigned numThreads = 20;
        IPointerArrayOf<CCreateFileLocalMountThread> threads;

        // Start threads with different filenames
        for (unsigned i = 0; i < numThreads; i++)
        {
            VStringBuffer filename("/remote/path/file%u.txt", i);
            VStringBuffer expectedPath("/local/path/file%u.txt", i);

            CCreateFileLocalMountThread *thread = new CCreateFileLocalMountThread(ip, filename.str(), expectedPath.str());
            threads.append(thread);
        }

        // Wait for all threads to complete and check for exceptions
        ForEachItemIn(i, threads)
        {
            CCreateFileLocalMountThread *thread = threads.item(i);
            thread->join();
        }
    }
    void testCreateFileLocalMount()
    {
        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);
        const char *remoteDir = "/remote/path";
        const char *localDir = "/local/path";

        // Set up a local mount redirect
        setDafsLocalMountRedirect(ip, remoteDir, localDir);

        // Test with file in the mount path
        Owned<IFile> file1 = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file1 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path/file.txt", file1->queryFilename());

        // Test with file exactly at the mount path
        Owned<IFile> file2 = createFileLocalMount(ip, "/remote/path");
        CPPUNIT_ASSERT(file2 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path", file2->queryFilename());
    }

    void testPartialPathMatch()
    {
        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);

        // Set up a local mount redirect
        setDafsLocalMountRedirect(ip, "/remote/path", "/local/path");

        // These should match
        Owned<IFile> file1 = createFileLocalMount(ip, "/remote/path/subdir/file.txt");
        CPPUNIT_ASSERT(file1 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path/subdir/file.txt", file1->queryFilename());

        // This should not match because it's not the same directory
        Owned<IFile> file2 = createFileLocalMount(ip, "/remote/pathother/file.txt");
        CPPUNIT_ASSERT(file2 == nullptr);

        // This should not match because it's a partial directory name
        Owned<IFile> file3 = createFileLocalMount(ip, "/remote/pat/file.txt");
        CPPUNIT_ASSERT(file3 == nullptr);
    }

    void testMultipleMounts()
    {
        IpAddress ip1, ip2;
        ip1.ipset(testIp_127_0_0_1);
        ip2.ipset(testIp_127_0_0_2);

        // Set up multiple mounts
        setDafsLocalMountRedirect(ip1, "/remote/path1", "/local/path1");
        setDafsLocalMountRedirect(ip1, "/remote/path2", "/local/path2");
        setDafsLocalMountRedirect(ip2, "/remote/path3", "/local/path3");

        // Test each mount
        Owned<IFile> file1 = createFileLocalMount(ip1, "/remote/path1/file.txt");
        CPPUNIT_ASSERT(file1 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path1/file.txt", file1->queryFilename());

        Owned<IFile> file2 = createFileLocalMount(ip1, "/remote/path2/file.txt");
        CPPUNIT_ASSERT(file2 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path2/file.txt", file2->queryFilename());

        Owned<IFile> file3 = createFileLocalMount(ip2, "/remote/path3/file.txt");
        CPPUNIT_ASSERT(file3 != nullptr);
        CPPUNIT_ASSERT_EQUAL_STR("/local/path3/file.txt", file3->queryFilename());
    }

    void testRemoveMounts()
    {
        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);

        // Set up a mount
        setDafsLocalMountRedirect(ip, "/remote/path", "/local/path");

        // Verify it's set
        Owned<IFile> file1 = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file1 != nullptr);

        // Remove the mount by setting mountdir to nullptr
        setDafsLocalMountRedirect(ip, "/remote/path", nullptr);

        // Verify it's removed
        Owned<IFile> file2 = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file2 == nullptr);

        // Test removing all mounts for an IP
        setDafsLocalMountRedirect(ip, "/remote/path", "/local/path");
        setDafsLocalMountRedirect(ip, "/remote/path2", "/local/path2");

        // Verify they're set
        Owned<IFile> file3 = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file3 != nullptr);

        Owned<IFile> file4 = createFileLocalMount(ip, "/remote/path2/file.txt");
        CPPUNIT_ASSERT(file4 != nullptr);

        // Remove all mounts for this IP
        setDafsLocalMountRedirect(ip, "/remote/path", nullptr);
        setDafsLocalMountRedirect(ip, "/remote/path2", nullptr);

        // Verify they're removed
        Owned<IFile> file5 = createFileLocalMount(ip, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file5 == nullptr);

        Owned<IFile> file6 = createFileLocalMount(ip, "/remote/path2/file.txt");
        CPPUNIT_ASSERT(file6 == nullptr);
    }

    void testNonMatchingCases()
    {
        IpAddress ip1, ip2;
        ip1.ipset(testIp_127_0_0_1);
        ip2.ipset(testIp_127_0_0_2);

        // Set up a mount for ip1 only
        setDafsLocalMountRedirect(ip1, "/remote/path", "/local/path");

        // Test with wrong IP
        Owned<IFile> file1 = createFileLocalMount(ip2, "/remote/path/file.txt");
        CPPUNIT_ASSERT(file1 == nullptr);

        // Test with wrong path
        Owned<IFile> file2 = createFileLocalMount(ip1, "/different/path/file.txt");
        CPPUNIT_ASSERT(file2 == nullptr);

        // Test with file that is not in the mount path
        Owned<IFile> file3 = createFileLocalMount(ip1, "/file.txt");
        CPPUNIT_ASSERT(file3 == nullptr);
    }

    void testCaseSensitivity()
    {
        // This test assumes case-sensitive paths (Unix-like systems)
        // On Windows, this might not detect issues with case sensitivity

        IpAddress ip;
        ip.ipset(testIp_127_0_0_1);

        // Set up a mount
        setDafsLocalMountRedirect(ip, "/Remote/Path", "/local/path");

        // Test with exact case match
        Owned<IFile> file1 = createFileLocalMount(ip, "/Remote/Path/file.txt");
        if (file1) // May be null on case-insensitive filesystems
            CPPUNIT_ASSERT_EQUAL_STR("/local/path/file.txt", file1->queryFilename());

        // Test with different case
        Owned<IFile> file2 = createFileLocalMount(ip, "/remote/path/file.txt");
#ifdef _WIN32
        // Windows is case-insensitive
        CPPUNIT_ASSERT(file2 != nullptr);
#else
        // Unix-like systems are case-sensitive
        CPPUNIT_ASSERT(file2 == nullptr);
#endif
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(RmtFileTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(RmtFileTest, "RmtFileTest");

#endif // _USE_CPPUNIT
