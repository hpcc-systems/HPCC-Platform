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

#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/resource/resource.h"

#include "unittests.hpp"

static constexpr byte zeros[0x100000] = { 0 };

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
    }

    void cleanup()
    {
        Owned<IFile> file(createIFile(testFilename));
        file->remove();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileTest, "JlibFileTest" );

#endif
