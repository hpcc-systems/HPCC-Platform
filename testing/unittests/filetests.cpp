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

#include "jsem.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jset.hpp"
#include "rmtfile.hpp"
#include "jlzw.hpp"
#include "jqueue.hpp"
#include "jregexp.hpp"
#include "jsecrets.hpp"
#include "jutil.hpp"
#include "junicode.hpp"

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


#endif
