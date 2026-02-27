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

/*
 * Jlib regression tests
 *
 */

#ifdef _USE_CPPUNIT

#include <algorithm>
#include <chrono>
#include <memory>
#include <random>
#include <vector>

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

#include "unittests.hpp"

//--------------------------------------------------------------------------------------------------------------------

class JlibCompressionTestBase : public CppUnit::TestFixture
{
protected:
    static constexpr size32_t sz = 100*0x100000; // 100MB
    static constexpr const char *aesKey = "012345678901234567890123";
    enum CompressOpt {
        RowCompress,                    // Compress a row at a time into an expanding buffer
        AllRowCompress,                 // Compress all rows in one call into a expanding buffer
        BlockCompress,                  // Call the alternative api to directly compress a block of data
        CompressToBuffer,               // Compress to a buffer using the compressToBuffer function
        FixedBlockCompress,             // Compress a row at a time to fixed size 32KB blocks, allow rows to be split over blocks (like a file)
        FixedIndexCompress,             // Compress a row at a time to fixed size 32KB blocks, prevent rows being split between blocks (index payload)
        FixedIndex8Compress,            // Compress a row at a time to fixed size 8KB blocks, prevent rows being split between blocks (index payload)
        FixedIndex4Compress,            // Compress a row at a time to fixed size 4KB blocks, prevent rows being split between blocks (index payload)
        LargeBlockCompress,             // Compress all remaining rows in each write to a 32K fixed size block
        MBBlockCompress                 // Compress all remaining rows in each write to a 1MB fixed size block
    };
public:
    void disableBacktraceOnAssert() { setBacktraceOnAssert(false); }

    void initCompressionBuffer(MemoryBuffer & src, size32_t & rowSz)
    {
        src.ensureCapacity(sz);

        StringBuffer tmp;
        unsigned card = 0;
        while (true)
        {
            size32_t cLen = src.length();
            if (cLen > sz)
                break;
            src.append(cLen);
            tmp.clear().appendf("%10u", cLen);
            src.append(tmp.length(), tmp.str());
            src.append(++card % 52);
            src.append(crc32((const char *)&cLen, sizeof(cLen), 0));
            unsigned ccrc = crc32((const char *)&card, sizeof(card), 0);
            tmp.clear().appendf("%10u", ccrc);
            src.append(tmp.length(), tmp.str());
            tmp.clear().appendf("%20u", (++card % 10));
            src.append(tmp.length(), tmp.str());
            if (0 == rowSz)
                rowSz = src.length();
            else
            {
                dbgassertex(0 == (src.length() % rowSz));
            }
        }
    }
    unsigned transferTimeMs(__int64 size, __int64 bytesPerSecond)
    {
        return (unsigned)((size * 1000) / bytesPerSecond);
    }

    void testCompressor(ICompressHandler &handler, const char * options, size32_t rowSz, size32_t srcLen, const byte * src, CompressOpt opt)
    {
        Owned<ICompressor> compressor = handler.getCompressor(options);

        MemoryBuffer compressed;
        UnsignedArray sizes;
        CCycleTimer timer;
        const byte * ptr = src;
        switch (opt)
        {
            case RowCompress:
            {
                compressor->open(compressed, sz, rowSz);
                const byte *ptrEnd = ptr + srcLen;
                for (size_t offset = 0; offset < srcLen; offset += rowSz)
                {
                    compressor->write(src+offset, rowSz);
                }
                compressor->close();
                try
                {
                    //Test that calling close again doesn't cause any problems
                    compressor->close();
                }
                catch (IException * e)
                {
                    //In debug mode it is ok to throw an unexpected error, in release it should be ignored.
                    if (!isDebugBuild() || e->errorCode() != 9999)
                        CPPUNIT_FAIL("Unexpected exception from second call to compressor->close()");
                    e->Release();
                }
                break;
            }
            case AllRowCompress:
            {
                compressor->open(compressed, sz, rowSz);
                compressor->write(ptr, srcLen);
                compressor->close();
                break;
            }
            case BlockCompress:
            {
                void * target = compressed.reserve(sz);
                unsigned written = compressor->compressBlock(sz, target, srcLen, ptr);
                compressed.setLength(written);
                break;
            }
            case CompressToBuffer:
            {
                compressToBuffer(compressed, srcLen, ptr, handler.queryAliasMethod(), options);
                break;
            }
            case FixedBlockCompress:
            case FixedIndexCompress:
            case FixedIndex8Compress:
            case FixedIndex4Compress:
            {
                const size32_t blocksize = (opt == FixedIndex4Compress) ? 4096 : (opt == FixedIndex8Compress) ? 8192 : 32768;
                bool allowPartialWrites = (opt == FixedBlockCompress);
                MemoryAttr buffer(blocksize);

                compressor->open(buffer.bufferBase(), blocksize, rowSz, allowPartialWrites);
                for (size_t offset = 0; offset < srcLen; offset += rowSz)
                {
                    const byte * ptr = src + offset;
                    size32_t written = compressor->write(ptr, rowSz);
                    if (written != rowSz)
                    {
                        compressor->close();
                        if (opt != FixedBlockCompress)
                            CPPUNIT_ASSERT(written == 0);
                        size32_t size = compressor->buflen();
                        sizes.append(size);
                        compressed.append(size, buffer.bufferBase());
                        compressor->open(buffer.bufferBase(), blocksize, rowSz, allowPartialWrites);
                        size32_t next = compressor->write(ptr+written, rowSz-written);
                        CPPUNIT_ASSERT(next == rowSz - written);
                    }
                }
                compressor->close();
                size32_t size = compressor->buflen();
                sizes.append(size);
                compressed.append(size, buffer.bufferBase());
                break;
            }
            case LargeBlockCompress:
            case MBBlockCompress:
            {
                size32_t blocksize = (opt == LargeBlockCompress) ? 32768 : 0x100000;
                MemoryAttr buffer(blocksize);

                size32_t offset = 0;
                while (offset < srcLen)
                {
                    compressor->open(buffer.bufferBase(), blocksize, rowSz, true);
                    size32_t written = compressor->write(ptr + offset, srcLen - offset);
                    CPPUNIT_ASSERT(written != 0);
                    compressor->close();

                    size32_t size = compressor->buflen();
                    sizes.append(size);
                    compressed.append(size, buffer.bufferBase());

                    offset += written;
                }
                break;
            }
        }

        cycle_t compressCycles = timer.elapsedCycles();
        MemoryBuffer tgt;
        if (!strieq(handler.queryType(), "randrow"))
        {
            Owned<IExpander> expander = handler.getExpander(options);
            timer.reset();
            switch (opt)
            {
            case CompressToBuffer:
                decompressToBuffer(tgt, compressed, options);
                break;
            case FixedBlockCompress:
            case LargeBlockCompress:
            case MBBlockCompress:
            case FixedIndexCompress:
            case FixedIndex8Compress:
            case FixedIndex4Compress:
                {
                    const byte * cur = compressed.bytes();
                    ForEachItemIn(i, sizes)
                    {
                        size32_t size = sizes.item(i);
                        size32_t required = expander->init(cur);
                        void * target = tgt.reserve(required);
                        expander->expand(target);
                        cur += size;
                    }
                    break;
                }
            default:
                {
                    size32_t required = expander->init(compressed.bytes());
                    tgt.reserveTruncate(required);
                    expander->expand(tgt.bufferBase());
                    tgt.setWritePos(required);
                    break;
                }
            }
        }
        else
        {
            timer.reset();
            Owned<IRandRowExpander> expander = createRandRDiffExpander();

            if ((opt == FixedBlockCompress) || (opt == LargeBlockCompress))
            {
                const byte * cur = compressed.bytes();
                ForEachItemIn(i, sizes)
                {
                    size32_t size = sizes.item(i);
                    expander->init(cur, false);
                    unsigned numRows = expander->numRows();
                    for (unsigned i = 0; i < numRows; i++)
                    {
                        void * row = tgt.reserve(rowSz);
                        expander->expandRow(row, i);
                    }
                    cur += size;
                }
            }
            else
            {
                tgt.append(srcLen, src);
            }
        }

        cycle_t decompressCycles = timer.elapsedCycles();

        float ratio = (float)(srcLen) / compressed.length();

        StringBuffer name(handler.queryType());
        if (opt == CompressToBuffer)
            name.append("-c2b");
        else if (opt == FixedBlockCompress)
            name.append("-fb");
        else if (opt == FixedIndexCompress)
            name.append("-fi");
        else if (opt == FixedIndex8Compress)
            name.append("-fi8");
        else if (opt == FixedIndex4Compress)
            name.append("-fi4");
        else if (opt == LargeBlockCompress)
            name.append("-lb");
        else if (opt == MBBlockCompress)
            name.append("-mb");
        if (options && *options)
            name.append("-").append(options);


        if (name.length() > 19)
            name.setLength(19);

        unsigned compressTime = (unsigned)cycle_to_millisec(compressCycles);
        unsigned decompressTime = (unsigned)cycle_to_millisec(decompressCycles);
        unsigned compressedTime = compressTime + decompressTime;
        unsigned copyTime200MBs = transferTimeMs(compressed.length(), 200000000);
        unsigned copyTime1GBs = transferTimeMs(compressed.length(), 1000000000);
        unsigned copyTime5GBs = transferTimeMs(compressed.length(), 5000000000);
        unsigned time200MBs = copyTime200MBs + compressedTime;
        unsigned time1GBs = copyTime1GBs + compressedTime;
        unsigned time5GBs = copyTime5GBs + compressedTime;
        DBGLOG("%19s || %8u || %8u || %4u(%4u,%4u) || %4u(%4u,%4u) || %4u(%4u,%4u) || %5.2f [%u]", name.str(), compressTime, decompressTime,
            time200MBs, copyTime200MBs + compressTime, copyTime200MBs + decompressTime,
            time1GBs, copyTime1GBs + compressTime, copyTime1GBs + decompressTime,
            time5GBs, copyTime5GBs + compressTime, copyTime5GBs + decompressTime,
             ratio, compressed.length());

        size32_t toCompare = std::min(srcLen, tgt.length());
        for (size32_t i=0; i < toCompare; i++)
        {
            if (src[i] != tgt.bytes()[i])
            {
                VStringBuffer msg("Mismatch at %u", i);
                CPPUNIT_FAIL(msg);
                break;
            }
        }
        CPPUNIT_ASSERT(tgt.length() == srcLen);
    }

    bool compressRows(MemoryBuffer & out, unsigned numRows, size32_t sizeLimit, ICompressHandler &handler, size32_t rowSz,const byte * src)
    {
        const char * options = nullptr;
        Owned<ICompressor> compressor = handler.getCompressor(options);

        compressor->open(out.ensureCapacity(sizeLimit), sizeLimit, rowSz, true);
        bool success = true;
        for (unsigned i = 0; i < numRows; i++)
        {
            if (compressor->write(src + i * rowSz, rowSz) != rowSz)
            {
                success = false;
                break;
            }
        }
        compressor->close();
        out.setLength(compressor->buflen());
        return success;
    }

    void testRollbackKeyCompression(unsigned numRows, ICompressHandler &handler, size32_t rowSz, const byte * src)
    {
        try
        {
            //Allocate plenty of space for the data
            MemoryBuffer targetN;
            bool okN = compressRows(targetN, numRows, numRows * rowSz, handler, rowSz, src);

            // Provide just enough space for the data - ideally it should succeed, but it may not if the compression granularity changes
            // so keep compressing with increasing sizes until it succeeds
            MemoryBuffer targetNlimit;
            unsigned n1Size = targetN.length();
            bool okN1l = compressRows(targetNlimit, numRows, n1Size, handler, rowSz, src);
            while (!okN1l)
            {
                okN1l = compressRows(targetNlimit, numRows, ++n1Size, handler, rowSz, src);
            }

            // Provide too little space for the data N+1, but enough for N.
            // Very occasionally this may succeed depending on the compression algorithm
            MemoryBuffer targetN1fail;
            bool okN1f = compressRows(targetN1fail, numRows + 1, n1Size+1, handler, rowSz, src);

            bool contentsMatch = (targetNlimit.length() == targetN1fail.length()) && (memcmp(targetNlimit.bytes(), targetN1fail.bytes(), targetNlimit.length()) == 0);

            DBGLOG("Limit %s/%u: %s, sizes(%u, %u, %u) increase(%u) limit(%s), fail(%s)",
                    handler.queryType(), numRows,
                    (okN1f || contentsMatch) ? "SUCCESS" : "FAIL",
                    targetN.length(), targetNlimit.length(), targetN1fail.length(), n1Size - targetN.length(),
                    boolToStr(okN1l), boolToStr(okN1f));
        }
        catch (IException *e)
        {
            StringBuffer msg;
            msg.appendf("Limit %s/%u: ", handler.queryType(), numRows);
            e->errorMessage(msg);
            DBGLOG("%s", msg.str());
            ::Release(e);
        }
    }

    void testOverflowBug()
    {
        // Test for bug where compressor crashes on close() with buffer overflow
        // caused by large rows causing an excessive growth in the buffer size

        ICompressHandler * handler = queryCompressHandler("lz4");
        CPPUNIT_ASSERT(handler);
        Owned<ICompressor> compressor = handler->getCompressor(nullptr);

        MemoryBuffer compressed;
        size32_t initialSize = 0x100000;  // 1MB initial size

        compressor->open(compressed, initialSize, 1);

        // Write enough incompressible data to trigger buffer growth
        // Write in chunks to trigger the write() path that resizes blksz
        size32_t chunkSizes[] = { 0x8000000, 0x8001000, 0x20020000 }; // chosen to hit the pathological resizing case

        for (size32_t chunkSize : chunkSizes)
        {
            MemoryBuffer chunk(chunkSize);
            for (size32_t j = 0; j < chunkSize; j++)
                chunk.append((byte)j);
            size32_t written = compressor->write(chunk.bytes(), chunk.length());
            CPPUNIT_ASSERT_EQUAL(chunk.length(), written);
        }

        compressor->close();  // This previously crashed with a buffer overflow

        CPPUNIT_ASSERT(compressed.length() > 0);
    }

    void testStandardCompression()
    {
        try
        {
            MemoryBuffer src;
            size32_t rowSz = 0;
            initCompressionBuffer(src, rowSz);


            DBGLOG("Algorithm(options)  || Comp(ms) || Deco(ms) || 200MB/s (w,r)   || 1GB/s (w,r)     || 5GB/s (w,r)     || Ratio [cLen]");
            DBGLOG("                    ||          ||          || 2Gb/s           || 10Gb/s          || 50Gb/s          ||");

            unsigned time200MBs = transferTimeMs(sz, 200000000);
            unsigned time1GBs = transferTimeMs(sz, 1000000000);
            unsigned time5GBs = transferTimeMs(sz, 5000000000);
            DBGLOG("%19s || %8u || %8u || %4u(%4u,%4u) || %4u(%4u,%4u) || %4u(%4u,%4u) || %5.2f [%u]", "uncompressed", 0, 0,
                time200MBs, time200MBs, time200MBs, time1GBs, time1GBs, time1GBs, time5GBs, time5GBs, time5GBs, 1.0, sz);

            Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();
            ForEach(*iter)
            {
                ICompressHandler &handler = iter->query();
                const char * type = handler.queryType();
                const char * options = streq("AES", handler.queryType()) ? aesKey: "";
                bool onlyFixedSize = strieq(type, "diff") || strieq(type, "randrow");

                testCompressor(handler, options, rowSz, src.length(), src.bytes(), FixedBlockCompress);
                testCompressor(handler, options, rowSz, src.length(), src.bytes(), FixedIndexCompress);

                //The stream compressors only currently support fixed size outputs
                //They also do not support partial writes - so largeBlockCompress will fail.
                if (strieq(type, "lz4s") || strieq(type, "lz4shc"))
                    continue;

                //zstds needs to support partial writes to be able to support LargeBlockCompress/MBBlockCompress
                //(That would be good for compressing network packets so worth revisiting)
                if (startsWithIgnoreCase(type, "zstds"))
                    continue;

                testCompressor(handler, options, rowSz, src.length(), src.bytes(), LargeBlockCompress);
                if  (!onlyFixedSize)
                    testCompressor(handler, options, rowSz, src.length(), src.bytes(), MBBlockCompress);


                //randrow has a limit of 64K rows - so it fails the row compress test
                if (strieq(type, "randrow"))
                    continue;

                testCompressor(handler, options, rowSz, src.length(), src.bytes(), RowCompress);
                if (!onlyFixedSize)
                    testCompressor(handler, options, rowSz, src.length(), src.bytes(), CompressToBuffer);
           }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            EXCLOG(e, nullptr);
            ::Release(e);
            CPPUNIT_FAIL(msg.str());
        }
    }
};


class JlibCompressionStandardTest : public JlibCompressionTestBase
{
    CPPUNIT_TEST_SUITE(JlibCompressionStandardTest);
        CPPUNIT_TEST(disableBacktraceOnAssert);
        CPPUNIT_TEST(testCompressionRegistration);
        CPPUNIT_TEST(testSingle);
        CPPUNIT_TEST(testTinyCompression);
        CPPUNIT_TEST(testStandardCompression);
        CPPUNIT_TEST(testOverflowBug);
    CPPUNIT_TEST_SUITE_END();

public:
    void testTinyCompression()
    {
        // Test compression of a very small incompressible block of data
        try
        {
            MemoryBuffer src;
            size32_t rowSz = 8;
            src.append(8, "\x1D\x9C\x05\x8B\x7E\xF2\x4A\x36");  // Very small incompressible data

            Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();
            ForEach(*iter)
            {
                ICompressHandler &handler = iter->query();
                const char * type = handler.queryType();
                if (streq("AES", type))
                    continue;

                const char * options = nullptr;
                testCompressor(handler, options, rowSz, src.length(), src.bytes(), FixedBlockCompress);

                //The stream compressors only currently support fixed size outputs
                //They also do not support partial writes - so largeBlockCompress will fail.
                if (strieq(type, "lz4s") || strieq(type, "lz4shc") || startsWithIgnoreCase(type, "zstds"))
                    continue;

                testCompressor(handler, options, rowSz, src.length(), src.bytes(), CompressToBuffer);
           }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            EXCLOG(e, nullptr);
            ::Release(e);
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testSingle()
    {
        try
        {
            MemoryBuffer src;
            size32_t rowSz = 0;
            initCompressionBuffer(src, rowSz);

            const char * compression = "zstd";
            const char * options = nullptr;
            ICompressHandler * handler = queryCompressHandler(compression);
            CPPUNIT_ASSERT_MESSAGE("Unknown compression type", handler);

            testCompressor(*handler, options, rowSz, src.length(), src.bytes(), FixedIndexCompress);
            testCompressor(*handler, options, rowSz, src.length(), src.bytes(), LargeBlockCompress);
            testCompressor(*handler, options, rowSz, src.length(), src.bytes(), RowCompress);
            testCompressor(*handler, options, rowSz, src.length(), src.bytes(), FixedBlockCompress);
            testCompressor(*handler, options, rowSz, src.length(), src.bytes(), MBBlockCompress);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            EXCLOG(e, nullptr);
            ::Release(e);
            CPPUNIT_FAIL(msg.str());
        }
    }

    void testCompressionRegistration()
    {
        for (unsigned i= COMPRESS_METHOD_NONE+1; i < COMPRESS_METHOD_LAST_ALIAS; i++)
        {
            switch (i)
            {
            case COMPRESS_METHOD_LAST_PERSISTED: // Not a real method
            case COMPRESS_METHOD_ROWDIF:    // Does not have a standard compresasor/decompressor
            case COMPRESS_METHOD_LZMA:      // A strange compression only used in keydiff
                continue;
            }

            CompressionMethod method = (CompressionMethod)i;
            const char * type = translateFromCompMethod(method);
            ICompressHandler * handler1 = queryCompressHandler(method);
            ICompressHandler * handler2 = queryCompressHandler(type);
            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("No handler by enum for method %s[%u]", type, i), handler1);
            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("No handler by name for method %s[%u]", type, i), handler2);
            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Handlers do not match for method %s", type), handler1 == handler2);

            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Handlers method does not match for method %s", type), handler1->queryAliasMethod() == method);
            if (i < COMPRESS_METHOD_LAST_PERSISTED && i != COMPRESS_METHOD_LZ4HC3)
                CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Handlers persist method does not match for method %s", type), handler1->queryPersistMethod() == method);
            else
                CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Handlers persist method matches for method %s", type), handler1->queryPersistMethod() != method);

            Owned<ICompressor> compressor = handler1->getCompressor(nullptr);
            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("No compressor for method %s", type), compressor);

            CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Inconsistent persist method for method %s", type), compressor->getCompressionMethod() == handler1->queryPersistMethod());
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibCompressionStandardTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibCompressionStandardTest, "JlibCompressionStandardTest" );


class JlibCompressionTimingTest : public JlibCompressionTestBase
{
    CPPUNIT_TEST_SUITE(JlibCompressionTimingTest);
        CPPUNIT_TEST(disableBacktraceOnAssert);
        CPPUNIT_TEST(testStandardCompression);
        CPPUNIT_TEST(testIndexBlock);
        CPPUNIT_TEST(testKeyRollback);
    CPPUNIT_TEST_SUITE_END();

public:
    void testIndexBlock()
    {
        START_TEST

        MemoryBuffer src;
        size32_t rowSz = 0;
        initCompressionBuffer(src, rowSz);


        DBGLOG("Algorithm(options)  || Comp(ms) || Deco(ms) || 200MB/s (w,r)   || 1GB/s (w,r)     || 5GB/s (w,r)     || Ratio [cLen]");
        DBGLOG("                    ||          ||          || 2Gb/s           || 10Gb/s          || 50Gb/s          ||");

        Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();
        ForEach(*iter)
        {
            ICompressHandler &handler = iter->query();
            const char * type = handler.queryType();
            if (streq("AES", type))
                continue;
            if (startsWithIgnoreCase(type, "lz4s") || startsWithIgnoreCase(type, "zstds"))
            {
                testCompressor(handler, nullptr, rowSz, src.length(), src.bytes(), FixedIndexCompress);
                for (const char * options : { "", "maxRecompress(0)", "maxRecompress(1)", "maxRecompress(2)", "minSizeToCompress(40)", "minSizeToCompress(80)" })
                {
                    testCompressor(handler, options, rowSz, src.length(), src.bytes(), FixedIndex8Compress);
                }
                testCompressor(handler, nullptr, rowSz, src.length(), src.bytes(), FixedIndex4Compress);
            }
            else
                testCompressor(handler, nullptr, rowSz, src.length(), src.bytes(), FixedIndexCompress);
        }

        END_TEST
    }

    void testKeyRollback()
    {
        // Test whether writing data to a fixed length buffer fails consistently when too much data is written
        // This affects whether inplace: compression can be used - because that can lead to the payload size increasing,
        // but if attempting and failing to extend the payload changes the compressed data size it can
        // prevent the key insertion being rolled back.
        try
        {
            MemoryBuffer src;
            size32_t rowSz = 0;
            initCompressionBuffer(src, rowSz);

            Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();
            ForEach(*iter)
            {
                ICompressHandler &handler = iter->query();
                if (streq("AES", handler.queryType()))
                    continue;

                testRollbackKeyCompression(1, handler, rowSz, src.bytes());
                testRollbackKeyCompression(32768/rowSz, handler, rowSz, src.bytes());
                testRollbackKeyCompression(0x40000/rowSz, handler, rowSz, src.bytes());
           }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            EXCLOG(e, nullptr);
            ::Release(e);
            CPPUNIT_FAIL(msg.str());
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibCompressionTimingTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibCompressionTimingTest, "JlibCompressionTimingTest" );


class JlibCompressionTestsStress : public JlibCompressionTestBase
{
    CPPUNIT_TEST_SUITE(JlibCompressionTestsStress);
        CPPUNIT_TEST(disableBacktraceOnAssert);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

public:
    void test()
    {
        try
        {
            MemoryBuffer src;
            size32_t rowSz = 0;
            initCompressionBuffer(src, rowSz);

            DBGLOG("Algorithm(options)  || Comp(ms) || Deco(ms) || 200MB/s (w,r)   || 1GB/s (w,r)     || 5GB/s (w,r)     || Ratio [cLen]");
            DBGLOG("                    ||          ||          || 2Gb/s           || 10Gb/s          || 50Gb/s          ||");

            unsigned time200MBs = transferTimeMs(sz, 200000000);
            unsigned time1GBs = transferTimeMs(sz, 1000000000);
            unsigned time5GBs = transferTimeMs(sz, 5000000000);
            DBGLOG("%19s || %8u || %8u || %4u(%4u,%4u) || %4u(%4u,%4u) || %4u(%4u,%4u) || %5.2f [%u]", "uncompressed", 0, 0,
                time200MBs, time200MBs, time200MBs, time1GBs, time1GBs, time1GBs, time5GBs, time5GBs, time5GBs, 1.0, sz);

            Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();
            ForEach(*iter)
            {
                ICompressHandler &handler = iter->query();
                const char * type = handler.queryType();
                const char * options = streq("AES", handler.queryType()) ? aesKey: "";
                bool onlyFixedSize = strieq(type, "diff") || strieq(type, "randrow");

                if (streq(type, "LZ4HC"))
                {
                    testCompressor(handler, "hclevel=3", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=4", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=5", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=6", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=8", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=10", rowSz, src.length(), src.bytes(), RowCompress);
                }
                if (strieq(type, "lz4s") || strieq(type, "lz4shc") || startsWithIgnoreCase(type, "zstds"))
                {
                    testCompressor(handler, options, rowSz, src.length(), src.bytes(), FixedBlockCompress);
                    continue;
                }
                testCompressor(handler, options, rowSz, src.length(), src.bytes(), RowCompress);

                if (!onlyFixedSize)
                    testCompressor(handler, options, rowSz, src.length(), src.bytes(), CompressToBuffer);

                if (streq(type, "LZ4"))
                {
                    testCompressor(handler, "allrow", rowSz, src.length(), src.bytes(), AllRowCompress); // block doesn't affect the compressor, just tracing
                    testCompressor(handler, "block", rowSz, src.length(), src.bytes(), BlockCompress); // block doesn't affect the compressor, just tracing
                }
           }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            EXCLOG(e, nullptr);
            ::Release(e);
            CPPUNIT_FAIL(msg.str());
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibCompressionTestsStress );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibCompressionTestsStress, "JlibCompressionTestsStress" );


#endif // _USE_CPPUNIT
