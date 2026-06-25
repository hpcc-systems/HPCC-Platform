/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

/*
 * File regression tests
 *
 */

#ifdef _USE_CPPUNIT
#include <memory>
#include <chrono>
#include <algorithm>
#include <random>
#include <thread>

#include "jsem.hpp"
#include "jfile.hpp"
#include "jplane.hpp"
#include "jdebug.hpp"
#include "jset.hpp"
#include "rmtfile.hpp"
#include "jlzw.hpp"
#include "jqueue.hpp"
#include "jregexp.hpp"
#include "jsecrets.hpp"
#include "jutil.hpp"
#include "junicode.hpp"
#include "jstream.hpp"
#include "jcrc.hpp"

#include "thorread.hpp"
#include "thorwrite.hpp"

#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/resource/resource.h"

#include "unittests.hpp"

static constexpr byte zeros[0x100000] = { 0 };

bool createTestBufferedInputStream(Shared<IBufferedSerialInputStream> & inputStream, Shared<IFileIO> & inputfileio, IFile * inputFile, const IPropertyTree * providerOptions)
{
    if (providerOptions->getPropBool("@null"))
    {
        inputfileio.setown(createNullFileIO());
        inputStream.setown(createBufferedInputStream(inputfileio, providerOptions));
        return true;
    }
    return createBufferedInputStream(inputStream, inputfileio, inputFile, providerOptions);
}

bool createTestBufferedOutputStream(Shared<IBufferedSerialOutputStream> & outputStream, Shared<IFileIO> & outputfileio, IFile * outputFile, const IPropertyTree * providerOptions)
{
    if (providerOptions->getPropBool("@null"))
    {
        outputfileio.setown(createNullFileIO());
        outputStream.setown(createBufferedOutputStream(outputfileio, providerOptions));
        return true;
    }
    return createBufferedOutputStream(outputStream, outputfileio, outputFile, providerOptions);
}

class JlibFileTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibFileTest);
        CPPUNIT_TEST(testCompressed);
        CPPUNIT_TEST(testAppendCompressed);
        CPPUNIT_TEST(testCompressWrite);
        CPPUNIT_TEST(cleanup);
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char * testFilename = "unittests_compressfile";
    void createCompressed()
    {
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io(createCompressedFileWriter(file, false, false, nullptr, COMPRESS_METHOD_LZ4, 0, -1, IFEnone));

        constexpr size_t cnt = 10000;
        constexpr size_t size = 1000;
        offset_t pos = 0;
        for (unsigned i = 0; i < cnt; i++)
        {
            byte temp[size];

            for (unsigned j = 0; j < size; j += 4)
            {
                temp[j] = (byte)j;
                temp[j+1] = (byte)j+1;
                temp[j+2] = (byte)j+2;
                temp[j+3] = (byte)random();
            }

            io->write(pos, size, temp);
            pos += size;
        }
        DBGLOG("Compressed file size = %llu -> %llu", pos, file->size());
    }
    void createCompressedAppend()
    {
        Owned<IFile> file(createIFile(testFilename));

        constexpr size_t cnt = 10000;
        constexpr size_t size = 1000;
        offset_t pos = 0;
        for (unsigned i = 0; i < cnt; i++)
        {
            bool append = (i != 0);
            Owned<IFileIO> io(createCompressedFileWriter(file, append, false, nullptr, COMPRESS_METHOD_LZ4, 0, -1, IFEnone));

            byte temp[size];

            for (unsigned j = 0; j < size; j += 4)
            {
                temp[j] = (byte)j;
                temp[j+1] = (byte)j+1;
                temp[j+2] = (byte)j+2;
                temp[j+3] = (byte)random();
            }

            io->write(pos, size, temp);
            pos += size;
        }
        DBGLOG("Compressed file size = %llu -> %llu", pos, file->size());
    }
    void readCompressed(bool errorExpected, size32_t ioBufferSize = useDefaultIoBufferSize)
    {
        bool success = false;
        CCycleTimer timer;
        try
        {
            Owned<IFile> file(createIFile(testFilename));
            Owned<IFileIO> io(createCompressedFileReader(file, nullptr, useDefaultIoBufferSize, false, IFEnone));

            constexpr size_t cnt = 10000;
            constexpr size_t size = 1000;
            offset_t pos = 0;
            for (unsigned i = 0; i < cnt; i++)
            {
                byte temp[size];

                io->read(pos, size, temp);

                for (unsigned j = 0; j < size; j += 4)
                {
                    CPPUNIT_ASSERT_EQUAL(temp[j], (byte)j);
                    CPPUNIT_ASSERT_EQUAL(temp[j+1], (byte)(j+1));
                }

                pos += size;
            }

            DBGLOG("Read compressed file (%u) in %u ms", ioBufferSize, timer.elapsedMs());
            success = true;
        }
        catch (IException *e)
        {
            if (errorExpected)
            {
                DBGLOG(e, "Expected error reading compressed file:");
            }
            else
            {
                StringBuffer msg("Unexpected error reading compressed file:");
                e->errorMessage(msg);
                CPPUNIT_FAIL(msg.str());
            }
            e->Release();
        }
        if (success && errorExpected)
            CPPUNIT_FAIL("Expected error reading compressed file");
    }
    void read(offset_t offset, size32_t size, void * data)
    {
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io(file->open(IFOread));
        io->read(offset, size, data);
    }
    void write(offset_t offset, size32_t size, void * data)
    {
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io(file->open(IFOwrite));
        io->write(offset, size, data);
    }
    void testCompressed()
    {
        createCompressed();
        readCompressed(false);
        readCompressed(false, 0x400000);

        // patch the file with zeroes in various places, retest

        write(0, sizeof(zeros), (void *)zeros);
        readCompressed(true);

        createCompressed();
        write(0x10000, sizeof(zeros), (void *)zeros);
        readCompressed(true);

        createCompressed();
        write(0x9000, sizeof(zeros), (void *)zeros);
        readCompressed(true);

        //Test the second block being corrupted with zeros
        size32_t firstBlockSize = 0;
        createCompressed();
        read(4, sizeof(firstBlockSize), &firstBlockSize);
        write(8+firstBlockSize, sizeof(zeros), (void *)zeros);
        readCompressed(true);

        //Test the data after the second block being corrupted with zeros
        createCompressed();
        read(4, sizeof(firstBlockSize), &firstBlockSize);
        write(8+4+firstBlockSize, sizeof(zeros), (void *)zeros);
        readCompressed(true);

        //Test the second block being corrupted to an invalid size
        size32_t newSize = 1;
        createCompressed();
        read(4, sizeof(firstBlockSize), &firstBlockSize);
        write(8+firstBlockSize, sizeof(newSize), &newSize);
        readCompressed(true);
    }
    void testAppendCompressed()
    {
        createCompressedAppend();
        readCompressed(false);
        readCompressed(false, 0x400000);
        readCompressed(false, 0x4000000);
    }

    void testCompressWrite(const char * srcFilename, offset_t sizeToWrite, CompressionMethod compMethod)
    {
        Owned<IFile> srcFile(createIFile(srcFilename));
        if (!srcFile->exists())
            return;

        offset_t srcSize = srcFile->size();
        Owned<IFileIO> srcIO(srcFile->open(IFOread));
        Owned<IFileIOStream> srcStream(createBufferedIOStream(srcIO, 0x100000));

        std::mt19937 randomGenerator; // Use the default seed, do not re-initialize

        CCycleTimer timer;
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io(createCompressedFileWriter(file, false, false, nullptr, compMethod, 0x10000, -1, IFEnone));

        offset_t offset = 0;
        constexpr size32_t maxReadSize = 0x20000;
        MemoryAttr buffer(maxReadSize);
        while (offset < sizeToWrite)
        {
            offset_t thisSize = randomGenerator() % 0x20000;
            offset_t thisOffset = randomGenerator() % (srcSize - thisSize);

            srcStream->seek(thisOffset, IFSbegin);
            srcStream->read(thisSize, buffer.mem());
            io->write(offset, thisSize, buffer.mem());
            offset += thisSize;
        }
        io->flush();
        io->close();

        //MORE: Also write uncompressed and compare the contents.

        DBGLOG("Compressed %s file size = %llu -> %llu took %ums", translateFromCompMethod(compMethod), sizeToWrite, file->size(), timer.elapsedMs());
    }

    void testCompressWrite()
    {
        const char * sampleSrc = "/home/gavin/dev/hpcc/testing/regress/download/pge0112.txt";
        testCompressWrite(sampleSrc, 0x10000000, COMPRESS_METHOD_LZ4);
        testCompressWrite(sampleSrc, 0x10000000, COMPRESS_METHOD_LZ4HC3);
        testCompressWrite(sampleSrc, 0x10000000, COMPRESS_METHOD_LZW);
        testCompressWrite(sampleSrc, 0x10000000, COMPRESS_METHOD_ZSTDS);
        testCompressWrite(sampleSrc, 0x10000000, COMPRESS_METHOD_ZSTD);
    }

    void cleanup()
    {
        Owned<IFile> file(createIFile(testFilename));
        file->remove();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileTest, "JlibFileTest" );

class JlibWriteSyncCacheTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibWriteSyncCacheTest);
        CPPUNIT_TEST(testSnapshotExpiryAndRepublish);
        CPPUNIT_TEST(testMultiEntrySelectiveExpiry);
        CPPUNIT_TEST(testConcurrentStress);
    CPPUNIT_TEST_SUITE_END();

public:
    // Deadlines are second-granularity wall-clock. 'short' entries are the ones we deliberately let
    // expire (large enough that the immediate "live" read cannot race a second rollover); 'long' entries
    // must stay live for the whole test. The wide gap keeps both robust on slow single-core containers,
    // and waitUntilExpired only ever waits, so it cannot report a premature expiry.
    static constexpr unsigned shortMarginSecs = 3;
    static constexpr unsigned longMarginSecs = 600;

    void setUp() { resetWriteSyncStateForTest(); }
    void tearDown() { resetWriteSyncStateForTest(); }

    // Note a single part, returning its deadline.
    time_t note(const char * path, offset_t size, bool compressed, unsigned marginSecs)
    {
        time_t deadline = time(nullptr) + marginSecs;
        noteWriteSyncFiles({ WriteSyncFileInfo{path, size, compressed} }, deadline);
        return deadline;
    }

    // Assert a part is live and reports exactly the metadata it was noted with.
    void assertLive(const char * path, offset_t size, bool compressed)
    {
        offset_t gotSize = (offset_t)-1;
        bool gotCompressed = false;
        CPPUNIT_ASSERT(getPathWriteSyncDelayRemainingMs(path, gotSize, gotCompressed) > 0);
        CPPUNIT_ASSERT_EQUAL(size, gotSize);
        CPPUNIT_ASSERT_EQUAL(compressed, gotCompressed);
    }

    // Assert a part reports no remaining delay (expired or absent).
    void assertNoDelay(const char * path)
    {
        offset_t gotSize = (offset_t)-1;
        bool gotCompressed = false;
        CPPUNIT_ASSERT_EQUAL(0U, getPathWriteSyncDelayRemainingMs(path, gotSize, gotCompressed));
    }

    void waitUntilExpired(time_t deadline)
    {
        while (time(nullptr) < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // A noted part is live and carries its metadata; once its deadline passes it reports no delay;
    // re-noting publishes a fresh snapshot carrying the new metadata.
    void testSnapshotExpiryAndRepublish()
    {
        static constexpr const char * path = "/var/lib/HPCCSystems/test/writeSync/single";
        time_t deadline = note(path, 1234, true, shortMarginSecs);
        assertLive(path, 1234, true);

        waitUntilExpired(deadline);
        assertNoDelay(path);

        note(path, 4321, false, shortMarginSecs);
        assertLive(path, 4321, false);
    }

    // With several keys in the snapshot, each is looked up independently and expires on its own per-entry
    // deadline: the short entry expires while the long entry (which also sets the snapshot-wide
    // latestDeadline, still in the future) stays live - proving neither gate short-circuits the other.
    void testMultiEntrySelectiveExpiry()
    {
        static constexpr const char * shortPath = "/var/lib/HPCCSystems/test/writeSync/multiShort";
        static constexpr const char * longPath = "/var/lib/HPCCSystems/test/writeSync/multiLong";
        time_t shortDeadline = note(shortPath, 100, false, shortMarginSecs);
        note(longPath, 200, true, longMarginSecs);

        assertLive(shortPath, 100, false);
        assertLive(longPath, 200, true);

        waitUntilExpired(shortDeadline);
        assertNoDelay(shortPath);
        assertLive(longPath, 200, true);
    }

    // Stress test. Many readers hammer the lock-free read path while several writers
    // concurrently note/update entries, expire-prune, and swap in fresh snapshots. Each path has a fixed
    // (size, compressed) identity that every writer always reproduces, so any read that reports a live entry
    // MUST observe exactly that identity - so a torn/inconsistent snapshot swap, a partially published entry,
    // or a use-after-free of a retired snapshot shows up as a mismatch (run under TSAN/UBSAN for full value).
    void testConcurrentStress()
    {
        static constexpr unsigned numPaths = 64;
        static constexpr unsigned numReaders = 8;
        static constexpr unsigned numWriters = 3;
        static constexpr unsigned runMs = 1500;

        std::vector<std::string> paths;
        paths.reserve(numPaths);
        for (unsigned i = 0; i < numPaths; i++)
            paths.push_back(std::string("/var/lib/HPCCSystems/test/writeSync/stress/") + std::to_string(i));

        auto sizeFor = [](unsigned i) -> offset_t { return (offset_t)(1000 + i); };
        auto compressedFor = [](unsigned i) -> bool { return (i & 1) != 0; };

        std::atomic<bool> stop{false};
        std::atomic<bool> failed{false};

        auto reader = [&]()
        {
            std::mt19937 rng(std::random_device{}());
            while (!stop.load(std::memory_order_relaxed))
            {
                unsigned i = rng() % numPaths;
                offset_t gotSize = (offset_t)-1;
                bool gotCompressed = false;
                if (getPathWriteSyncDelayRemainingMs(paths[i].c_str(), gotSize, gotCompressed) > 0)
                {
                    if (gotSize != sizeFor(i) || gotCompressed != compressedFor(i))
                    {
                        failed.store(true, std::memory_order_relaxed);
                        stop.store(true, std::memory_order_relaxed);
                    }
                }
            }
        };

        auto writer = [&](unsigned seed)
        {
            std::mt19937 rng(seed);
            while (!stop.load(std::memory_order_relaxed))
            {
                std::vector<WriteSyncFileInfo> batch;
                unsigned batchSize = 1 + (rng() % 8);
                for (unsigned n = 0; n < batchSize; n++)
                {
                    unsigned i = rng() % numPaths;
                    batch.push_back(WriteSyncFileInfo{paths[i], sizeFor(i), compressedFor(i)});
                }
                // Mix short deadlines (which expire and get pruned mid-run) with longer-lived ones.
                unsigned marginSecs = (0 == (rng() % 4)) ? 1 : 30;
                noteWriteSyncFiles(batch, time(nullptr) + marginSecs);
                // Brief yield so the writer does not busy-spin and inflate CPU/log churn on shared CI runners,
                // while still exercising concurrent publish/read against the (hammered) lock-free reader.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        };

        std::vector<std::thread> threads;
        for (unsigned r = 0; r < numReaders; r++)
            threads.emplace_back(reader);
        for (unsigned w = 0; w < numWriters; w++)
            threads.emplace_back(writer, 1 + w);

        std::this_thread::sleep_for(std::chrono::milliseconds(runMs));
        stop.store(true, std::memory_order_relaxed);
        for (auto &t : threads)
            t.join();

        CPPUNIT_ASSERT_MESSAGE("write-sync read observed an inconsistent snapshot entry", !failed.load());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibWriteSyncCacheTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibWriteSyncCacheTest, "JlibWriteSyncCacheTest" );


// This atomic is incremented to allow the writing thread to perform work in parallel with disk output
std::atomic<unsigned> toil{0};
class JlibStreamTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibStreamTest);
        CPPUNIT_TEST(testStreamOptions);
        CPPUNIT_TEST(cleanup);
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char * testFilename = "unittests_compressfile";

    inline void putString(IBufferedSerialOutputStream * stream, const char * str)
    {
        stream->put(strlen(str)+1, str);
    }

    void testWrite(const char * title, const IPropertyTree * options)
    {
        CCycleTimer timer;
        Owned<IFile> file(createIFile(testFilename));
        Owned<IBufferedSerialOutputStream> stream;
        Owned<IFileIO> io;
        if (!createTestBufferedOutputStream(stream, io, file, options))
             CPPUNIT_FAIL("Failed to create output stream");

        // Write sample data
        static constexpr const char * field1[30] = {
            "Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel", "India", "Juliet",
            "Kilo", "Lima", "Mike", "November", "Oscar", "Papa", "Quebec", "Romeo", "Sierra", "Tango",
            "Uniform", "Victor", "Whiskey", "X-ray", "Yankee", "Zulu", "Apple", "Banana", "Cherry", "Date"
        };
        static constexpr const char * field2[30] = {
            "Red", "Blue", "Green", "Yellow", "Purple", "Orange", "Black", "White", "Gray", "Pink",
            "Cyan", "Magenta", "Brown", "Violet", "Indigo", "Gold", "Silver", "Bronze", "Copper", "Teal",
            "Maroon", "Olive", "Navy", "Lime", "Coral", "Peach", "Mint", "Lavender", "Plum", "Azure"
        };
        static constexpr const char * field3[30] = {
            "Dog", "Cat", "Mouse", "Horse", "Cow", "Sheep", "Goat", "Pig", "Chicken", "Duck",
            "Goose", "Turkey", "Rabbit", "Deer", "Fox", "Wolf", "Bear", "Lion", "Tiger", "Leopard",
            "Cheetah", "Panther", "Jaguar", "Otter", "Beaver", "Moose", "Elk", "Bison", "Buffalo", "Camel"
        };
        static constexpr const char * field4[30] = {
            "Car", "Bike", "Bus", "Train", "Plane", "Boat", "Truck", "Scooter", "Tram", "Subway",
            "Helicopter", "Jet", "Ship", "Ferry", "Taxi", "Van", "SUV", "Pickup", "Motorcycle", "Skateboard",
            "Rollerblade", "Segway", "Rickshaw", "Cart", "Wagon", "Sled", "Snowmobile", "ATV", "RV", "Yacht"
        };
        static constexpr const char * field5[30] = {
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "Holiday", "Workday", "Birthday",
            "Anniversary", "Festival", "Meeting", "Conference", "Seminar", "Workshop", "Webinar", "Exam", "Test", "Quiz",
            "Lecture", "Class", "Session", "Break", "Lunch", "Dinner", "Breakfast", "Brunch", "Supper", "Snack"
        };

        unsigned work = options->getPropInt("@work", 0);
        constexpr unsigned numRows = 2000000;
        unsigned counter = 17; // Start with a prime number

        for (unsigned i = 0; i < numRows; ++i)
        {
            unsigned idx1 = (counter * 17) % 30;
            unsigned idx2 = (counter * 19) % 30;
            unsigned idx3 = (counter * 23) % 30;
            unsigned idx4 = (counter * 29) % 30;
            unsigned idx5 = (counter * 31) % 30;

            putString(stream, field1[idx1]);
            putString(stream, field2[idx2]);
            putString(stream, field3[idx3]);
            putString(stream, field4[idx4]);
            putString(stream, field5[idx5]);
            stream->put(4, &counter);

            for (unsigned i = 0; i < work; i++)
                toil += counter;

            counter += 7919; // Increment by a large prime
        }

        stream->flush();
        offset_t sizeWritten = stream->tell();
        stream.clear();
        io->close();

        DBGLOG("Write '%-25s' took %5ums size=%llu->%llu", title, timer.elapsedMs(), sizeWritten, file->size());

    }
    void testRead(const char * title, const IPropertyTree * options)
    {
        CCycleTimer timer;
        Owned<IFile> file(createIFile(testFilename));
        Owned<IBufferedSerialInputStream> stream;
        Owned<IFileIO> io;
        if (!createTestBufferedInputStream(stream, io, file, options))
             CPPUNIT_FAIL("Failed to create input stream");

        unsigned tempSize = options->getPropInt("@tempSize", 1024);
        MemoryAttr tempBuffer(tempSize);
        offset_t totalRead = 0;
        for (;;)
        {
            size32_t sizeRead = stream->read(tempSize, tempBuffer.mem());
            totalRead += sizeRead;
            if (sizeRead != tempSize)
                break;
        }

        io->close();
        stream.clear();

        DBGLOG("Read  '%-25s' took %5ums size=%llu<-%llu", title, timer.elapsedMs(), totalRead, file->size());
    }

    //Use XML for the options.  Json might be cleaner it cannot set attributes
    static constexpr std::initializer_list<std::pair<const char *, const char *>> testCases
    {
        { "null",                   R"!(sizeIoBuffer="1000000" null="1")!" },
        { "null thread",            R"!(sizeIoBuffer="1000000" null="1" threading="1")!" },
        { "simple",                 R"!()!" },
        { "small buffer",           R"!(sizeIoBuffer="256")!" },
        { "large buffer",           R"!(sizeIoBuffer="4000000")!" },
        { "large buffer append",    R"!(sizeIoBuffer="4000000" extend="1")!" },
        { "lz4",                    R"!(sizeIoBuffer="1000000" compression="lz4")!" },
        // Append to the previous file (in the same format) and check that the file read back is twice as long
        { "lz4 append",             R"!(sizeIoBuffer="1000000" compression="lz4" extend="1")!" },
        // Sequential is the mode that can be used for spill files that are only ready sequentially
        { "lz4 seq",                R"!(sizeIoBuffer="1000000" compression="lz4" sequentialAccess="1")!" },
        { "lz4 seq append",         R"!(sizeIoBuffer="1000000" compression="lz4" sequentialAccess="1" extend="1")!" },
        { "lz4hc3",                 R"!(sizeIoBuffer="1000000" compression="lz4hc3")!" },
        { "zstd",                   R"!(sizeIoBuffer="1000000" compression="zstd")!" },
        { "zstd thread",            R"!(sizeIoBuffer="1000000" compression="zstd" threading="1")!" },
        { "zstd small block",       R"!(sizeIoBuffer="1000000" compression="zstd" sizeCompressBlock="32768")!" },
        { "zstd work",              R"!(sizeIoBuffer="1000000" compression="zstd" work="50")!" },
        { "zstd work thread",       R"!(sizeIoBuffer="1000000" compression="zstd" work="50" threading="1")!" },
        { "zstd seq",               R"!(sizeIoBuffer="1000000" compression="zstd" sequentialAccess="1")!" },
        { "zstd work slow",         R"!(sizeIoBuffer="1000000" compression="zstd" work="50" delayNs="200000000")!" },
        // Check the buffer size is being used - this should be much faster than the line above because the delay is per write
        { "zstd work slow/4",       R"!(sizeIoBuffer="4000000" compression="zstd" work="50" delayNs="200000000")!" },
        { "zstd work thread slow",  R"!(sizeIoBuffer="1000000" compression="zstd" work="50" threading="1" delayNs="200000000")!" },
// Where should the following options be implemented:
//    overwrite
//    optional
    };

    void processTest(const char * title, const char * config)
    {
        START_TEST

        StringBuffer xml;
        xml.append("<providerOptions ").append(config).append("/>");
        Owned<IPropertyTree> options = createPTreeFromXMLString(xml.str());
        testWrite(title, options);
        testRead(title, options);

        END_TEST
    }

    void testStreamOptions()
    {
        for (const auto & test : testCases)
            processTest(test.first, test.second);
    }

    void cleanup()
    {
        Owned<IFile> file(createIFile(testFilename));
        file->remove();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibStreamTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibStreamTest, "JlibStreamTest" );


class JlibFileStressTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibFileStressTest);
        CPPUNIT_TEST(testUncompressedSeeks);
        CPPUNIT_TEST(testCompressedSeeks);
        CPPUNIT_TEST(cleanup);
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char * testFilename = "unittests_seekfile";

    void createSemiRandomFile(IFileIO * io, offset_t size)
    {
        // Create semi-random data where byte value is a function of offset
        // Using simple algorithm: byte = (offset * 31 + offset / 256) to appear random but still compress
        constexpr size32_t bufferSize = 0x10000; // 64KB chunks
        byte buffer[bufferSize];

        offset_t offset = 0;
        while (offset < size)
        {
            size32_t thisSize = (size32_t)std::min((offset_t)bufferSize, size - offset);

            // Fill buffer with semi-random data based on offset
            for (size32_t i = 0; i < thisSize; i++)
            {
                offset_t pos = offset + i;
                // Simple algorithm: multiply by prime, add high byte to create pseudo-randomness
                buffer[i] = (byte)((pos * 31 + pos / 256) & 0xFF);
            }

            io->write(offset, thisSize, buffer);
            offset += thisSize;
        }
    }

    // Verify semi-random data at any offset - used to check random reads
    bool verifySemiRandomData(const byte * data, offset_t offset, size32_t size)
    {
        for (size32_t i = 0; i < size; i++)
        {
            offset_t pos = offset + i;
            byte expected = (byte)((pos * 31 + pos / 256) & 0xFF);
            if (data[i] != expected)
                return false;
        }
        return true;
    }
    void createSemiRandomFile(CompressionMethod compression, offset_t size)
    {
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io;
        if (compression != COMPRESS_METHOD_NONE)
            io.setown(createCompressedFileWriter(file, false, false, nullptr, compression, 0, -1, IFEnone));
        else
            io.setown(file->open(IFOcreate));
        createSemiRandomFile(io, size);
    }

    void verifyRandomReads(CompressionMethod compression, offset_t size, size32_t verifySize, unsigned numReads)
    {
        Owned<IFile> file(createIFile(testFilename));
        Owned<IFileIO> io;
        if (compression != COMPRESS_METHOD_NONE)
            io.setown(createCompressedFileReader(file, nullptr, useDefaultIoBufferSize, false, IFEnone));
        else
            io.setown(file->open(IFOread));

        // Perform random reads and verify data
        std::mt19937 randomGenerator; // Use the default seeds so that the results are reproducible
        MemoryAttr buffer(verifySize);

        for (unsigned i = 0; i < numReads; i++)
        {
            offset_t offset = randomGenerator() % (size - verifySize);

            size32_t sizeRead = io->read(offset, verifySize, buffer.mem());
            CPPUNIT_ASSERT_EQUAL(verifySize, sizeRead);
            CPPUNIT_ASSERT(verifySemiRandomData(buffer.bytes(), offset, verifySize));
        }
    }
    void testSeeks(CompressionMethod compression, unsigned verifySize, unsigned numReads)
    {
        offset_t size = 0x40000000; // 1GB
        CCycleTimer timer;
        createSemiRandomFile(compression, size);
        unsigned msToCreate = timer.elapsedMs();

        timer.reset();
        verifyRandomReads(compression, size, verifySize, numReads);

        Owned<IFile> file(createIFile(testFilename));
        offset_t compressedSize = file->size();

        DBGLOG("Verified %u random reads of %u bytes in %s file of size %llu(%llu) in %ums (creation %ums)", numReads, verifySize, translateFromCompMethod(compression), size, compressedSize, timer.elapsedMs(), msToCreate);
    }
    void testSeeks(CompressionMethod compression)
    {
        unsigned verifySize = 0x1000; // 4KB
        unsigned numReads = 2000;
        testSeeks(compression, verifySize, numReads);
    }
    void testUncompressedSeeks()
    {
        testSeeks(COMPRESS_METHOD_NONE);
        testSeeks(COMPRESS_METHOD_NONE, 0x400, 1000000);
    }
    void testCompressedSeeks()
    {
        testSeeks(COMPRESS_METHOD_LZ4);
        testSeeks(COMPRESS_METHOD_LZ4HC3);
        testSeeks(COMPRESS_METHOD_ZSTD);
        testSeeks(COMPRESS_METHOD_LZW);
        testSeeks(COMPRESS_METHOD_LZ4, 0x400, 100000); // Test smaller reads and more of them to increase chance of hitting edge cases in the compressed seek logic
    }
    void cleanup()
    {
        Owned<IFile> file(createIFile(testFilename));
        file->remove();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileStressTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileStressTest, "JlibFileStressTest" );


// Fill a block with deterministic content that achieves a realistic compression ratio (~50%).
// Even bytes are pseudo-random (high entropy), odd bytes are zero (easily compressed).
// This gives compressors enough redundancy to achieve roughly 2:1 compression.
static void fillBlockSemiCompressible(byte * block, size32_t len, offset_t blockOffset)
{
    uint32_t state = static_cast<uint32_t>(blockOffset ^ (blockOffset >> 32)) ^ 0x5A5A5A5A;
    if (state == 0) state = 1;
    for (size32_t i = 0; i < len; i++)
    {
        if (i & 1)
        {
            block[i] = 0;
        }
        else
        {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            block[i] = static_cast<byte>(state);
        }
    }
}

// Read an entire IBufferedSerialInputStream, accumulate CRC32, return elapsed nanoseconds.
static __uint64 readStreamWithCrc(IBufferedSerialInputStream * in, size32_t readSize, unsigned & readCrc, offset_t limit = UnknownOffset)
{
    MemoryAttr bufMem(readSize);
    byte * buf = static_cast<byte *>(bufMem.bufferBase());
    CRC32 crc;
    CCycleTimer timer;
    offset_t totalRead = 0;
    for (;;)
    {
        size32_t toRead = readSize;
        if (limit != UnknownOffset && (limit - totalRead) < readSize)
            toRead = (size32_t)(limit - totalRead);
        if (toRead == 0)
            break;
        size32_t got = in->read(toRead, buf);
        if (got == 0)
            break;
        crc.tally(got, buf);
        totalRead += got;
    }
    __uint64 ns = timer.elapsedNs();
    readCrc = crc.get();
    return ns;
}

// Write a test file using the serial-stream compression pipeline (matching how Thor writes data).
// Returns the CRC32 of the *uncompressed* content so readers can verify decompression integrity.
//
// Pipeline: IFileIO -> createSerialOutputStream -> createBufferedOutputStream
//           -> createCompressingOutputStream -> createBufferedOutputStream (outer, decompressed-block-sized)
//
// The content is semi-compressible (~50% ratio) so the compressed file is roughly
// half the uncompressed size -- targeting a compressed file just over 4 GB on disk.
static unsigned createCompressedTestFile(const char * filename, offset_t uncompressedSize, const char * compression, size32_t ioBufferSize, bool overwrite = false)
{
    Owned<IFile> file = createIFile(filename);
    if (file->exists() && !overwrite)
    {
        DBGLOG("File %s already exists - will not create. size=%" I64F "d", filename, file->size());
        // read to calculate CRC of existing file content using crc streamer
        Owned<IFileIO> io = file->open(IFOread);
        Owned<ISerialInputStream> rawStream = createSerialInputStream(io);
        Owned<IBufferedSerialInputStream> ioBuffer = createBufferedInputStream(rawStream, ioBufferSize);
        Owned<IExpander> expander = getExpander(compression);
        Owned<ISerialInputStream> decompressed = createDecompressingInputStream(ioBuffer, expander);
        Owned<IBufferedSerialInputStream> in = createBufferedInputStream(decompressed, ioBufferSize);
        unsigned crc;
        readStreamWithCrc(in, ioBufferSize, crc, UnknownOffset);
        return crc;
    }
    DBGLOG("Creating %.2f GB test file (compression=%s)...",
        (double)uncompressedSize / 0x40000000, compression);
    Owned<IFileIO> io = file->open(IFOcreate);
    Owned<ISerialOutputStream> rawStream = createSerialOutputStream(io);
    Owned<IBufferedSerialOutputStream> ioBuffer = createBufferedOutputStream(rawStream, ioBufferSize);
    Owned<ICompressor> compressor = getCompressor(compression);
    Owned<ISerialOutputStream> compStream = createCompressingOutputStream(ioBuffer, compressor);
    Owned<ISerialOutputStream> progStream = createProgressStream(compStream, 0, uncompressedSize, "Creating compress file", 1);
    // Outer buffer collects uncompressed rows before handing blocks to the compressor
    Owned<IBufferedSerialOutputStream> out = createBufferedOutputStream(progStream, ioBufferSize);

    constexpr size32_t blockSize = 0x100000;  // 1 MB write blocks
    MemoryAttr blockMem(blockSize);
    byte * block = static_cast<byte *>(blockMem.bufferBase());
    CRC32 fileCrc;
    offset_t offset = 0;
    while (offset < uncompressedSize)
    {
        size32_t toWrite = static_cast<size32_t>(std::min<offset_t>(blockSize, uncompressedSize - offset));
        fillBlockSemiCompressible(block, toWrite, offset);
        fileCrc.tally(toWrite, block);
        out->put(toWrite, block);
        offset += toWrite;
    }
    out->flush();
    out.clear();
    progStream.clear();
    compStream.clear();
    compressor.clear();
    ioBuffer.clear();
    rawStream.clear();

    double ratio = (double)io->size() / uncompressedSize * 100;
    DBGLOG("File created: compressed=%.1f MB  ratio=%.1f%%",
        (double)io->size() / 0x100000, ratio);

    return fileCrc.get();
}

// "Timing" in the suite name ensures the test is excluded from default unittest runs.
//
// Target scenario: reading multi-GB compressed files from Azure Blob storage where
// 4 MB block size and 16-32 reader threads is the optimal configuration.
//
// Command-line options (all optional):
//   --JlibParallelReadAheadTimingTest.filename=<path>     Use an existing file instead of creating one
//   --JlibParallelReadAheadTimingTest.compression=<type>  Compression codec (default: zstd)
//   --JlibParallelReadAheadTimingTest.size=<MB>           Uncompressed size in MB (default: ~8192)
//   --JlibParallelReadAheadTimingTest.mode=[raw,decompressed,both]
class JlibParallelReadAheadTimingTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibParallelReadAheadTimingTest);
        CPPUNIT_TEST(testParallelReadAheadTiming);
    CPPUNIT_TEST_SUITE_END();
public:

    // ~8 GB uncompressed; with ~50% compression ratio the on-disk file is just over 4 GB.
    static constexpr offset_t defaultUncompressedSize = (offset_t)8 * 0x40000000 + 0x180000;
    static constexpr size32_t defaultChunkSize = 0x400000;  // 4 MB -- Azure Blob optimal block size
    static constexpr size32_t compressBlockSize = 0x10000;   // 64K -- compressed block size for test file creation
    static constexpr size32_t readSize = 0x100000;   // 1 MB consumer read size
    static constexpr size32_t overflowMaxSize = 0x400000;  // 4 MB overflow region
    static constexpr const char * defaultFilename = "parallelReadAheadTimingTest.tmp";
    static constexpr const char * defaultCompression = "zstd";

    void testParallelReadAheadTiming()
    {
        try
        {
            installDefaultFileHooks(nullptr);

            Owned<IPropertyTree> config = getComponentConfigSP();

            StringBuffer configuredFilename;
            config->getProp("JlibParallelReadAheadTimingTest/@filename", configuredFilename);

            StringBuffer compressionType;
            config->getProp("JlibParallelReadAheadTimingTest/@compression", compressionType);
            if (!compressionType.length())
                compressionType.set(defaultCompression);

            bool doRaw = true;
            bool doDecompressed = true;
            StringBuffer modeType;
            config->getProp("JlibParallelReadAheadTimingTest/@mode", modeType);
            if (modeType.length())
            {
                doRaw = strieq(modeType.str(), "raw") || strieq(modeType.str(), "both");
                doDecompressed = strieq(modeType.str(), "decompressed") || strieq(modeType.str(), "both");
            }

            offset_t uncompressedSize = defaultUncompressedSize;
            unsigned sizeMB = config->getPropInt("JlibParallelReadAheadTimingTest/@size", 0);
            if (sizeMB)
                uncompressedSize = (offset_t)sizeMB * 0x100000;

            // If the uncompressed size is smaller than the default chunk size, reduce it
            size32_t chunkSize = defaultChunkSize;
            if (uncompressedSize < chunkSize)
                chunkSize = (size32_t)uncompressedSize;

            if (configuredFilename.length())
            {
                filename.set(configuredFilename);
                DBGLOG("Using configured filename: %s  compression=%s", filename.get(), compressionType.str());
            }
            else
                filename.set(defaultFilename);

            bool overwrite = config->getPropBool("JlibParallelReadAheadTimingTest/@overwrite", false);
            unsigned expectedCrc = createCompressedTestFile(filename, uncompressedSize, compressionType, compressBlockSize, overwrite);

            Owned<IExpander> expander = getExpander(compressionType);

            // --- Serial baseline: single-threaded sequential read with decompression ---
            unsigned serialCrc = 0;
            unsigned serialHalfCrc = 0;
            unsigned serialRawCrc = 0;
            unsigned serialHalfRawCrc = 0;
            offset_t halfReadLimit = uncompressedSize / 2;
            __uint64 serialNs = 0;
            __uint64 serialRawNs = 0;

            Owned<IFile> file = createIFile(filename);
            offset_t fileLen = file->size();

            {
                Owned<IFileIO> io = file->open(IFOread);
                Owned<ISerialInputStream> raw = createSerialInputStream(io);
                Owned<ICrcSerialInputStream> rawCrc = createCrcInputStream(raw);
                Owned<IBufferedSerialInputStream> ioStream = createBufferedInputStream(rawCrc, chunkSize);

                if (doRaw)
                {
                    readStreamWithCrc(ioStream, readSize, serialHalfRawCrc, fileLen / 2);
                    ioStream->reset(0, fileLen);
                    serialRawNs = readStreamWithCrc(ioStream, readSize, serialRawCrc);
                    logResult("Serial (Raw)", fileLen, serialRawNs, serialRawNs);
                }

                if (doDecompressed)
                {
                    if (doRaw)
                        ioStream->reset(0, fileLen);

                    Owned<ISerialInputStream> decompressed = createDecompressingInputStream(ioStream, expander);
                    Owned<IBufferedSerialInputStream> in = createBufferedInputStream(decompressed, chunkSize);

                    readStreamWithCrc(in, readSize, serialHalfCrc, halfReadLimit);
                    in->reset(0, fileLen);
                    serialNs = readStreamWithCrc(in, readSize, serialCrc);
                    serialRawCrc = rawCrc->queryCrc();

                    CPPUNIT_ASSERT_EQUAL_MESSAGE("Serial CRC mismatch", expectedCrc, serialCrc);
                    logResult("Serial (Decompress)", uncompressedSize, serialNs, serialNs);
                }
            }

            // --- Test Parallel Reset ---
            {
                Owned<IFile> pFile = createIFile(filename);
                Owned<IFileIO> pIo = pFile->open(IFOread);
                Owned<IBufferedSerialInputStream> pReadAhead = createParallelReadAheadInputStream(pIo, 4, chunkSize, overflowMaxSize);

                if (doRaw)
                {
                    unsigned parallelHalfRawCrc = 0;
                    unsigned parallelRawCrc = 0;

                    readStreamWithCrc(pReadAhead, readSize, parallelHalfRawCrc, fileLen / 2);
                    CPPUNIT_ASSERT_EQUAL_MESSAGE("Parallel Raw Half CRC mismatch", serialHalfRawCrc, parallelHalfRawCrc);

                    pReadAhead->reset(0, fileLen);
                    readStreamWithCrc(pReadAhead, readSize, parallelRawCrc);
                    CPPUNIT_ASSERT_EQUAL_MESSAGE("Parallel Raw CRC mismatch", serialRawCrc, parallelRawCrc);
                }

                if (doDecompressed)
                {
                    if (doRaw)
                        pReadAhead->reset(0, fileLen);
                    unsigned parallelHalfCrc = 0;
                    unsigned parallelCrc = 0;
                    Owned<ISerialInputStream> pDecompressed = createDecompressingInputStream(pReadAhead, expander);
                    Owned<IBufferedSerialInputStream> pIn = createBufferedInputStream(pDecompressed, chunkSize);

                    readStreamWithCrc(pIn, readSize, parallelHalfCrc, halfReadLimit);
                    CPPUNIT_ASSERT_EQUAL_MESSAGE("Parallel Half CRC mismatch", serialHalfCrc, parallelHalfCrc);

                    pIn->reset(0, fileLen);
                    readStreamWithCrc(pIn, readSize, parallelCrc);
                    CPPUNIT_ASSERT_EQUAL_MESSAGE("Parallel CRC mismatch", serialCrc, parallelCrc);
                }
            }

            // --- Sweep: concurrency x chunk size x consumer read size ---
            DBGLOG("--- Sweep: threads x chunkSize x readSize ---");
            DBGLOG("%-24s | %10s | %10s | %10s", "Label", "Time (us)", "Speed (MB/s)", "Speedup");
            DBGLOG("-------------------------+------------+------------+-----------");
            static constexpr unsigned sweepThreads[] = { 4, 8, 16, 32 };
            static constexpr size32_t sweepChunkSizes[] = { 0x100000, 0x400000 };   // 1M, 4M
            static constexpr size32_t sweepReadSizes[] = { 0x10000, 0x100000, 0x400000 };    //  64K, 1M, 4M

            for (size32_t testChunkSize : sweepChunkSizes)
            {
                for (unsigned numThreads : sweepThreads)
                {
                    for (size32_t testReadSize : sweepReadSizes)
                    {
                        {
                            size32_t currentOverflowMaxSize = std::min<size32_t>(overflowMaxSize, (numThreads - 1) * testChunkSize);
                            Owned<IFile> file = createIFile(filename);
                            Owned<IFileIO> io = file->open(IFOread);
                            Owned<IBufferedSerialInputStream> readAhead = createParallelReadAheadInputStream(io, numThreads, testChunkSize, currentOverflowMaxSize);

                            if (doRaw)
                            {
                                // Measure pure read-ahead performance first
                                unsigned parallelRawCrcIter = 0;
                                __uint64 parallelRawNsIter = readStreamWithCrc(readAhead, testReadSize, parallelRawCrcIter);
                                CPPUNIT_ASSERT_EQUAL_MESSAGE("Sweep parallel raw data CRC mismatch", serialRawCrc, parallelRawCrcIter);

                                VStringBuffer labelRaw("t=%u c=%uK r=%uK (Raw)", numThreads, testChunkSize / 1024, testReadSize / 1024);
                                logResult(labelRaw.str(), fileLen, parallelRawNsIter, serialRawNs);
                            }

                            if (doDecompressed)
                            {
                                if (doRaw)
                                    readAhead->reset(0, fileLen);
                                Owned<ISerialInputStream> decompressed = createDecompressingInputStream(readAhead, expander);
                                Owned<IBufferedSerialInputStream> in = createBufferedInputStream(decompressed, testReadSize);

                                unsigned parallelCrc = 0;
                                __uint64 parallelNs = readStreamWithCrc(in, testReadSize, parallelCrc);
                                CPPUNIT_ASSERT(in->eos());

                                VStringBuffer crcMsg("Sweep t=%u chunk=%uK read=%uK CRC mismatch",
                                                    numThreads, testChunkSize / 1024, testReadSize / 1024);
                                CPPUNIT_ASSERT_EQUAL_MESSAGE(crcMsg.str(), serialCrc, parallelCrc);

                                VStringBuffer label("t=%u c=%uK r=%uK (Decompress)", numThreads, testChunkSize / 1024, testReadSize / 1024);
                                logResult(label.str(), uncompressedSize, parallelNs, serialNs);
                            }
                        }
                    }
                }
            }
        }
        catch (IException *e)
        {
            StringBuffer msg("Unexpected exception in testParallelReadAheadTiming: ");
            e->errorMessage(msg);
            EXCLOG(e);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }

private:
    void logResult(const char * label, offset_t dataSize, __uint64 elapsedNs, __uint64 serialNs)
    {
        double mbs = (double)dataSize / elapsedNs * 1000;
        double speedup = (double)serialNs / elapsedNs;
        DBGLOG("%-24s | %10llu | %10.1f | %9.2fx", label, elapsedNs / 1000, mbs, speedup);
    }

    StringAttr filename;
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibParallelReadAheadTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibParallelReadAheadTimingTest, "JlibParallelReadAheadTimingTest");

#endif
