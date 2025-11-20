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
#include <memory>
#include <chrono>
#include <algorithm>
#include <random>

#include "jsem.hpp"
#include "jfile.hpp"
#include "jstream.hpp"
#include "jlzw.hpp"
#include "jptree.hpp"

#include "unittests.hpp"

static const unsigned oneMinute = 60000; // msec

class CDataProvider
{
public:
    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row) = 0;
    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row) = 0;
    const char * queryName() const { return name.str(); };
protected:
    StringBuffer name;
};

//Highly compressible
class SequenceDataProvider : public CDataProvider
{
public:
    SequenceDataProvider(size32_t _len, bool _useRead = false, bool _useWrite = true)
    : len(_len), useRead(_useRead), useWrite(_useWrite)
    {
        name.append("Seq_").append(len).append(useRead ? 'R' : 'P').append(useWrite ? 'W' : 'C');
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        byte * next;
        if (useWrite)
        {
            next = (byte *)alloca(len);
        }
        else
        {
            size32_t got;
            next = (byte *)target->reserve(len, got);
        }
        for (size32_t i=0; i < len; i++)
            next[i] = (byte)(i * row);
        if (useWrite)
            target->put(len, next);
        else
            target->commit(len);
        return len;
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        byte * next;
        if (useRead)
        {
            next = (byte *)alloca(len);
            size32_t read = source->read(len, next);
            assertex(read == len);
        }
        else
        {
            size32_t available;
            next = (byte *)source->peek(len, available);
            assertex(available >= len);
        }
        for (size32_t i=0; i < len; i++)
            if (next[i] != (byte)(i * row))
                throw MakeStringException(0, "Mismatch at %u,%u", i, row);
        if (!useRead)
            source->skip(len);
        return len;
    }

protected:
    size32_t len;
    bool useRead;
    bool useWrite;
};

class SkipSequenceDataProvider : public SequenceDataProvider
{
public:
    SkipSequenceDataProvider(size32_t _len) : SequenceDataProvider(_len)
    {
        name.clear().append("Skip").append(len);
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        constexpr size32_t checkByte = 7;
        size32_t available;
        source->skip(checkByte);
        const byte * next = (const byte *)source->peek(1, available);
        assertex(available >= 1);
        if (next[0] != (byte)(checkByte * row))
            throw MakeStringException(0, "Skip mismatch at %u", row);
        source->skip(len-checkByte);
        return len;
    }

};

class ReservedDataProvider : public SequenceDataProvider
{
public:
    ReservedDataProvider(size32_t _len) : SequenceDataProvider(_len)
    {
        name.clear().append("Res").append(len);
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        size32_t got;
        byte * next = (byte *)target->reserve(len+2, got);
        for (size32_t i=0; i < len; i++)
            next[i] = (byte)(i * row);
        target->commit(len);
        return len;
    }
};

//Not very compressible
class Sequence2DataProvider : public CDataProvider
{
public:
    Sequence2DataProvider(size32_t _len)
    : len(_len)
    {
        name.append("Seq2_").append(len);
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        byte * next = (byte *)alloca(len);
        for (size32_t i=0; i < len; i++)
            next[i] = (byte)(i * row + (row >> 3));
        target->put(len, next);
        return len;
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        byte * next = (byte *)alloca(len);
        size32_t read = source->read(len, next);
        assertex(read == len);
        for (size32_t i=0; i < len; i++)
            if (next[i] != (byte)(i * row + (row >> 3)))
                throw MakeStringException(0, "Mismatch at %u,%u", i, row);
        return len;
    }

protected:
    size32_t len;
};

class RandomDataProvider : public CDataProvider
{
public:
    RandomDataProvider(size32_t _len)
    : len(_len), generator{std::random_device{}()}
    {
        name.append("Rand").append(len);
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        byte * next = (byte *)  alloca(len);
        for (size32_t i=0; i < len; i++)
            next[i] = generator();
        target->put(len, next);
        return len;
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        source->skip(len);
        return len;
    }

protected:
    size32_t len;
    std::mt19937 generator;
};

//Output (int8 , string, dataset({unsigned3 })
class VariableDataProvider : public CDataProvider
{
public:
    VariableDataProvider(bool _useCount) : useCount(_useCount)
    {
        name.append("Var_").append(useCount ? 'C' : 'S');
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        //Output (row, (string)row, (row % 7)items of (row, row*2, row*3))
        __uint64 id = row;
        StringBuffer name;
        name.append(row);

        target->put(8, &id);
        size32_t len = name.length();
        target->put(4, &len);
        target->put(len, name.str());
        size32_t childCount = (row % 7);
        size32_t childSize = 3 * childCount;
        if (useCount)
            target->put(4, &childCount);
        else
            target->suspend(sizeof(size32_t));
        for (unsigned i=0; i < childCount; i++)
        {
            size32_t value = row * (i+1);
            target->put(3, &value);
        }
        if (!useCount)
            target->resume(sizeof(childSize), &childSize);
        return 8 + 4 + len + 4 + childSize;
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        //Output (row, (string)row, (row % 7)items of (row, row*2, row*3))
        __uint64 id = row;
        source->read(8, &id);
        assertex(id == row);

        size32_t len;
        source->read(4, &len);
        StringBuffer name;
        source->read(len, name.reserve(len));
        assertex(atoi(name) == row);

        size32_t size;
        source->read(sizeof(size), &size);
        if (useCount)
            assertex(size == (row % 7));
        else
            assertex(size == (row % 7) * 3);
        for (unsigned i=0; i < (row % 7); i++)
        {
            size32_t value = 0;
            source->read(3, &value);
            size32_t expected = ((row * (i+1)) & 0xFFFFFF);
            assertex(value == expected);
        }
        return 8 + 4 + len + 4 + (row % 7) * 3;
    }

protected:
    bool useCount;
};


//A very large row (because of a large embedded dataset)
//100 bytes of data then 500K rows of 100 bytes
class LargeRowDataProvider : public CDataProvider
{
public:
    LargeRowDataProvider(bool _useCount, unsigned _numChildren) : useCount(_useCount), numChildren(_numChildren)
    {
        name.append("Large_").append(useCount ? 'C' : 'S').append(numChildren);
    }

    virtual size32_t create(IBufferedSerialOutputStream * target, unsigned row)
    {
        //Output (row, (string)row, (row % 7)items of (row, row*2, row*3))
        byte mainRow[100];
        unsigned childRow[25];

        for (size32_t i=0; i < sizeof(mainRow); i++)
            mainRow[i] = (byte)(i * row);
        target->put(sizeof(mainRow), mainRow);

        size32_t childCount = numChildren + row;
        size32_t childSize = sizeof(childRow) * childCount;
        if (useCount)
            target->put(4, &childCount);
        else
            target->suspend(sizeof(size32_t));

        unsigned next = 1234 + row * 31419264U;
        for (unsigned i=0; i < childCount; i++)
        {
            for (size32_t i=0; i < sizeof(mainRow)/sizeof(next); i++)
            {
                childRow[i] = next;
                next *= 0x13894225;
                next += row;
            }
            target->put(sizeof(childRow), &childRow);
        }
        if (!useCount)
            target->resume(sizeof(childSize), &childSize);

        return sizeof(mainRow) + 4 + childSize;
    }

    virtual size32_t check(IBufferedSerialInputStream * source, unsigned row)
    {
        byte mainRow[100];
        unsigned childRow[25];

        source->read(sizeof(mainRow), &mainRow);
        for (size32_t i=0; i < sizeof(mainRow); i++)
            assertex(mainRow[i] == (byte)(i * row));

        size32_t childCount = numChildren + row;
        size32_t childSize = sizeof(childRow) * childCount;
        size32_t size;
        source->read(sizeof(size), &size);
        if (useCount)
            assertex(size == childCount);
        else
            assertex(size == childSize);

        unsigned next = 1234 + row * 31419264U;
        for (unsigned i=0; i < childCount; i++)
        {
            source->read(sizeof(childRow), &childRow);
            for (size32_t i=0; i < sizeof(mainRow)/sizeof(next); i++)
            {
                assertex(childRow[i] == next);
                next *= 0x13894225;
                next += row;
            }
        }
        return sizeof(mainRow) + 4 + childSize;
    }

protected:
    bool useCount;
    unsigned numChildren = 10'000'000;
};

class NullOuputStream : public CInterfaceOf<IBufferedSerialOutputStream>
{
    virtual size32_t write(size32_t len, const void * ptr) { return len; }
    virtual void put(size32_t len, const void * ptr) {}
    virtual void flush() {}
    virtual byte * reserve(size32_t wanted, size32_t & got) { return nullptr; }
    virtual void commit(size32_t written) {}
    virtual void suspend(size32_t wanted) {}
    virtual void resume(size32_t len, const void * ptr) {}
    virtual offset_t tell() const override { return 0; }
    virtual void replaceOutput(ISerialOutputStream * newOutput) override {}
};

class JlibStreamStressTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibStreamStressTest);
        CPPUNIT_TEST(testGeneration);
        CPPUNIT_TEST(testBufferStream);     // Write a file and then read the results
        CPPUNIT_TEST(testVarStringHelpers);  // Test functions for reading varstrings from a stream
        CPPUNIT_TEST(testGroupingInPeekStringList);  // Test grouping functionality in peekStringList
        CPPUNIT_TEST(testSimpleStream);     // Write a file and then read the results
        CPPUNIT_TEST(testIncSequentialStream); // write a file and read results after each flush
        CPPUNIT_TEST(testEvenSequentialStream); // write a file and read results after each flush
        CPPUNIT_TEST(testParallelStream);   // write a file and read in parallel from a separate thread
        CPPUNIT_TEST(testThreadedWriteStream);   // write a file using a threaded writer
        CPPUNIT_TEST(testCrcBufferedOutputStream); // test that the CRC is calculated correctly
        CPPUNIT_TEST(testPathologicalRows); // 1M child rows, total row size 100MB
        //MORE:
        //Threaded writer
        //Threaded reader
        //Threaded reader and writer
        //Threaded reader and writer all in parallel
        //Directly create the stream around the handle to avoid virtuals
    CPPUNIT_TEST_SUITE_END();

    //The following options control which tests are run and the parameters for each test
    static constexpr const char * filename = "testfile";
    static constexpr offset_t numTestRows = 10'000'000;
    static constexpr offset_t numRowsPerBatch = 10'000;
    static constexpr bool testCore              = true;
    static constexpr bool testCompressible      = true;
    static constexpr bool testRandom            = true;
    static constexpr bool testSkip              = true;
    static constexpr bool testHighCompression   = true;

    static constexpr bool timeGeneration = false;
    static constexpr bool testBuffer = true;
    static constexpr bool testSimple = true;
    static constexpr bool testIncSequential = false;
    static constexpr bool testEvenSequential = true;
    static constexpr bool testParallel = true;
    static constexpr bool testThreadedWrite = true;

    __uint64 timeSeq = 0;
    __uint64 timeSkip = 0;
    __uint64 timeRand = 0;

    __uint64 testGeneration(CDataProvider & dataProvider, unsigned numRows)
    {
        Owned<IBufferedSerialOutputStream> out = new NullOuputStream();

        CCycleTimer timer;

        offset_t totalWritten = 0;
        for (unsigned i=0; i < numRows; i++)
            totalWritten += dataProvider.create(out, i);
        out->flush();
        __uint64 elapsedNs = timer.elapsedNs();

        DBGLOG("testGeneration(%s, %u) took %lluus", dataProvider.queryName(), numRows, elapsedNs/1000);
        return elapsedNs;
    }
    void reportResult(const char * testname, ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows, offset_t totalWritten, __uint64 elapsedNs)
    {
        Owned<IFile> file = createIFile(filename);
        offset_t compressedSize = file->size();

        const char * compressMethod = compressHandler ? compressHandler->queryType() : "none";
        double rate = (double)totalWritten * 1000 / elapsedNs;
        DBGLOG("%s(%s, %s, %u, %u, %u) took %lluus %.2fMB/s %.2f%%", testname, compressMethod, dataProvider.queryName(), bufferSize, compressedBufferSize, numRows, elapsedNs/1000, rate, (double)compressedSize * 100 / totalWritten);

    }
    void reportFailure(const char * testname, ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, IException * ownedException)
    {
        const char * compressMethod = compressHandler ? compressHandler->queryType() : "none";
        StringBuffer msg;
        msg.appendf("%s(%s, %s, %u, %u) failed: ", testname, compressMethod, dataProvider.queryName(), bufferSize, compressedBufferSize);
        ownedException->errorMessage(msg);
        ownedException->Release();
        CPPUNIT_FAIL(msg.str());
    }
    void runSimpleStream(const char * testname, ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows, bool threadedRead, bool threadedWrite)
    {
        try
        {
            Owned<IBufferedSerialOutputStream> out = createOutput(filename, bufferSize, compressHandler, compressedBufferSize, threadedWrite);
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize ? bufferSize : 32, compressHandler, compressedBufferSize, threadedRead);

            CCycleTimer timer;
            offset_t totalWritten = 0;
            for (unsigned i=0; i < numRows; i++)
                totalWritten += dataProvider.create(out, i);
            out->flush();

            offset_t totalRead = 0;
            for (unsigned i=0; i < numRows; i++)
            {
                CPPUNIT_ASSERT_EQUAL(totalRead, in->tell());
                totalRead += dataProvider.check(in, i);
            }

            size32_t got;
            CPPUNIT_ASSERT_EQUAL(totalRead, in->tell());
            CPPUNIT_ASSERT_MESSAGE("Data available after the end of stream", !in->peek(1, got));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, got);
            CPPUNIT_ASSERT_MESSAGE("Data available after the end of stream", in->eos());
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", in->tell(), totalRead);

            byte end;
            size32_t remaining = in->read(1, &end);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, remaining);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("eos is not true at end of stream", true, in->eos());

            CPPUNIT_ASSERT_EQUAL(totalWritten, totalRead);

            __uint64 elapsedNs = timer.elapsedNs();
            reportResult(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, totalWritten, elapsedNs);
        }
        catch (IException * e)
        {
            reportFailure(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, e);
        }
    }
    void runSimpleStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        runSimpleStream("testSimple", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, false, false);
    }
    void runBufferStream(const char * testname, ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows, bool threadedRead, bool threadedWrite)
    {
        try
        {
            MemoryBuffer buffer;
            Owned<IBufferedSerialOutputStream> out = createBufferedSerialOutputStream(buffer);
            Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStreamFillMemory(buffer);

            CCycleTimer timer;
            offset_t totalWritten = 0;
            for (unsigned i=0; i < numRows; i++)
                totalWritten += dataProvider.create(out, i);
            out->flush();

            offset_t totalRead = 0;
            for (unsigned i=0; i < numRows; i++)
            {
                totalRead += dataProvider.check(in, i);
                CPPUNIT_ASSERT_EQUAL(totalRead, in->tell());
            }

            size32_t got;
            CPPUNIT_ASSERT_MESSAGE("Data available after the end of stream", !in->peek(1, got));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, got);
            CPPUNIT_ASSERT_MESSAGE("Data available after the end of stream", in->eos());
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", in->tell(), totalRead);

            byte end;
            size32_t remaining = in->read(1, &end);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, remaining);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("eos is not true at end of stream", true, in->eos());

            CPPUNIT_ASSERT_EQUAL(totalWritten, totalRead);

            __uint64 elapsedNs = timer.elapsedNs();
            reportResult(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, totalWritten, elapsedNs);
        }
        catch (IException * e)
        {
            reportFailure(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, e);
        }
    }
    void runBufferStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        runBufferStream("testSimple", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, false, false);
    }
    void runThreadedWriteStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        if ((compressHandler ? compressedBufferSize : bufferSize) == 0)
            return;
        runSimpleStream("testThreadedWriteStream", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, false, true);
    }

    void runSequentialStream(const char * testname, ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows, bool evenSize)
    {
        try
        {
            Owned<IBufferedSerialOutputStream> out = createOutput(filename, bufferSize, compressHandler, compressedBufferSize, false);
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize ? bufferSize : 32, compressHandler, compressedBufferSize, false);

            CCycleTimer timer;
            offset_t totalWritten = 0;
            offset_t totalRead = 0;

            offset_t rowsRemaining = numRows;
            offset_t rowsWritten = 0;
            for (size32_t batch=1; rowsRemaining; batch++)
            {
                unsigned rowsThisTime = std::min(rowsRemaining, evenSize ? numRowsPerBatch : (offset_t)batch);

                for (unsigned i=0; i < rowsThisTime; i++)
                {
                    totalWritten += dataProvider.create(out, rowsWritten+i);
                    CPPUNIT_ASSERT_EQUAL(totalWritten, out->tell());
                }
                out->flush();

                for (unsigned i=0; i < rowsThisTime; i++)
                {
                    totalRead += dataProvider.check(in, rowsWritten+i);
                    CPPUNIT_ASSERT_EQUAL(totalRead, in->tell());
                }

                rowsRemaining -= rowsThisTime;
                rowsWritten += rowsThisTime;
            }

            byte end;
            size32_t remaining = in->read(1, &end);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, remaining);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("eos is not true at end of stream", true, in->eos());

            CPPUNIT_ASSERT_EQUAL(totalWritten, totalRead);

            __uint64 elapsedNs = timer.elapsedNs();
            reportResult(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, totalWritten, elapsedNs);
        }
        catch (IException * e)
        {
            reportFailure(testname, compressHandler, dataProvider, bufferSize, compressedBufferSize, e);
        }
    }

    void runEvenSequentialStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        runSequentialStream("testEvenSequential", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, true);
    }
    void runIncSequentialStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        runSequentialStream("testIncSequential", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, false);
    }
    class ParallelWorker : public Thread
    {
    public:
        ParallelWorker(Semaphore & _started, Semaphore & _ready, std::atomic<offset_t> & _available)
        : started(_started), ready(_ready), available(_available) {}

        Semaphore go;
        Semaphore & started;
        Semaphore & ready;
        std::atomic<offset_t> & available;
    };

    class ParallelWriter : public ParallelWorker
    {
    public:
        ParallelWriter(Semaphore & _started, Semaphore & _ready, std::atomic<offset_t> & _available, CDataProvider & _dataProvider, IBufferedSerialOutputStream * _out, unsigned _numRows)
        : ParallelWorker(_started, _ready, _available), dataProvider(_dataProvider), out(_out), numRows(_numRows)
        {
        }
        virtual int run()
        {
            started.signal();
            go.wait();

            unsigned batches = numRows / numRowsPerBatch;
            offset_t totalWritten = 0;
            for (unsigned batch=0; batch < batches; batch++)
            {
                for (unsigned i=0; i < numRowsPerBatch; i++)
                    totalWritten += dataProvider.create(out, i+batch*numRowsPerBatch);
                out->flush();
                available.fetch_add(numRowsPerBatch);
                ready.signal();
            }
            totalSent = totalWritten;
            return 0;
        }

    public:
        CDataProvider & dataProvider;
        IBufferedSerialOutputStream * out;
        offset_t totalSent = 0;
        unsigned numRows;
    };


    class ParallelReader : public ParallelWorker
    {
    public:
        ParallelReader(Semaphore & _started, Semaphore & _ready, std::atomic<offset_t> & _available, CDataProvider & _dataProvider, IBufferedSerialInputStream * _in, unsigned _numRows)
        : ParallelWorker(_started, _ready, _available), dataProvider(_dataProvider), in(_in), numRows(_numRows)
        {
        }
        virtual int run()
        {
            try
            {
                started.signal();
                go.wait();

                offset_t readSoFar = 0;
                unsigned batches = numRows / numRowsPerBatch;
                for (unsigned batch=0; batch < batches; batch++)
                {
                    ready.wait();
                    offset_t nowAvailable = available.load();

                    while (readSoFar < nowAvailable)
                        dataProvider.check(in, readSoFar++);
                }
            }
            catch (IException * _e)
            {
                e.setown(_e);
            }
            return 0;
        }

    public:
        CDataProvider & dataProvider;
        IBufferedSerialInputStream * in;
        Owned<IException> e;
        unsigned numRows;
    };

    void runParallelStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows, bool threadedRead, bool threadedWrite)
    {
        try
        {
            Semaphore ready;
            Semaphore started;
            std::atomic<offset_t> available{0};

            Owned<IBufferedSerialOutputStream> out = createOutput(filename, bufferSize, compressHandler, compressedBufferSize, threadedWrite);
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize ? bufferSize : 32, compressHandler, compressedBufferSize, threadedRead);
            Owned<ParallelWriter> writer = new ParallelWriter(started, ready, available, dataProvider, out, numRows);
            Owned<ParallelReader> reader = new ParallelReader(started, ready, available, dataProvider, in, numRows);
            reader->start(true);
            writer->start(true);

            started.wait();
            started.wait();

            CCycleTimer timer;
            reader->go.signal();
            writer->go.signal();

            reader->join();
            writer->join();

            if (reader->e)
                reportFailure("testParallel", compressHandler, dataProvider, bufferSize, compressedBufferSize, LINK(reader->e));

            byte end;
            size32_t remaining = in->read(1, &end);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Data available after the end of stream", 0U, remaining);
            CPPUNIT_ASSERT_EQUAL_MESSAGE("eos is not true at end of stream", true, in->eos());

            __uint64 elapsedNs = timer.elapsedNs();
            reportResult("testParallel", compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, writer->totalSent, elapsedNs);
        }
        catch (IException * e)
        {
            reportFailure("testParallel", compressHandler, dataProvider, bufferSize, compressedBufferSize, e);
        }
    }

    void runParallelStream(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows)
    {
        runParallelStream(compressHandler, dataProvider, bufferSize, compressedBufferSize, numRows, false, false);
    }

    using TestFunction = void (JlibStreamStressTest::*)(ICompressHandler * compressHandler, CDataProvider & dataProvider, size32_t bufferSize, size32_t compressedBufferSize, unsigned numRows);
    void applyTests(TestFunction testFunction)
    {
        SequenceDataProvider seqProvider(40, false, true);
        SequenceDataProvider seqProviderRW(40, true, true);
        SequenceDataProvider seqProviderRC(40, true, false);
        SequenceDataProvider seqProviderPC(40, false, false);
        Sequence2DataProvider seq2Provider(40);
        VariableDataProvider varProvider(false);
        SkipSequenceDataProvider skipProvider(17);
        ReservedDataProvider resProvider(40);
        RandomDataProvider randProvider(37);
        ICompressHandler * lz4 = queryCompressHandler(COMPRESS_METHOD_LZ4);
        ICompressHandler * lz4hc = queryCompressHandler(COMPRESS_METHOD_LZ4HC);

        if (testCore)
        {
            (this->*testFunction)(nullptr, seqProvider, 0x100000, 0x100000, numTestRows);
            (this->*testFunction)(lz4, seqProvider, 0x100000, 0x100000, numTestRows);
            (this->*testFunction)(lz4, varProvider, 0x100000, 0x100000, numTestRows);
            (this->*testFunction)(lz4, resProvider, 0x100000, 0x100000, numTestRows);
        }

        if (testCompressible)
        {
            (this->*testFunction)(nullptr, seqProvider, 7, 0, numTestRows/10);
            (this->*testFunction)(nullptr, seqProvider, 64, 0, numTestRows/10);
            (this->*testFunction)(nullptr, seqProvider, 0x10000, 0, numTestRows);
            (this->*testFunction)(nullptr, seqProvider, 0x100000, 0, numTestRows);

            (this->*testFunction)(lz4, seqProvider, 7, 19, numTestRows/10);
            (this->*testFunction)(lz4, seqProvider, 64, 64, numTestRows/10);
            (this->*testFunction)(lz4, seqProvider, 1024, 1024, numTestRows);
            (this->*testFunction)(lz4, seqProvider, 0x10000, 0x10000, numTestRows);
            (this->*testFunction)(lz4, seqProvider, 0x40000, 0x40000, numTestRows);
            (this->*testFunction)(lz4, seqProvider, 0x100000, 0x100000, numTestRows);
            (this->*testFunction)(lz4, seqProvider, 0x40000, 0x100000, numTestRows);
            (this->*testFunction)(lz4, seq2Provider, 7, 19, numTestRows/10);

            (this->*testFunction)(lz4, seqProvider, 43, 97, numTestRows/10);
            (this->*testFunction)(lz4, resProvider, 43, 97, numTestRows/10);
        }

        if (testSkip)
        {
            //Test skipping functionality to ensure coverage
            (this->*testFunction)(nullptr, skipProvider, 64, 64, numTestRows/10);
            (this->*testFunction)(nullptr, skipProvider, 0x10000, 0x10000, numTestRows);
            (this->*testFunction)(lz4, skipProvider, 64, 64, numTestRows/10);
            (this->*testFunction)(lz4, skipProvider, 0x10000, 0x10000, numTestRows);
        }

        if (testRandom)
        {
            (this->*testFunction)(lz4, randProvider, 64, 64, numTestRows/10);
            (this->*testFunction)(lz4, randProvider, 7, 19, numTestRows/10);
            (this->*testFunction)(lz4, randProvider, 1024, 1024, numTestRows);
            (this->*testFunction)(lz4, randProvider, 0x10000, 0x10000, numTestRows);
            (this->*testFunction)(lz4, randProvider, 0x40000, 0x40000, numTestRows);
            (this->*testFunction)(lz4, randProvider, 0x100000, 0x100000, numTestRows);
        }

        if (testHighCompression)
        {
            (this->*testFunction)(lz4hc, seq2Provider, 0x100000, 0x100000, numTestRows);
        }
    }

    void testGeneration()
    {
        if (timeGeneration)
        {
            SequenceDataProvider seqProvider(40);
            SkipSequenceDataProvider skipProvider(17);
            RandomDataProvider randProvider(37);

            timeSeq = testGeneration(seqProvider, numTestRows);
            timeRand = testGeneration(randProvider, numTestRows);
            timeSkip = testGeneration(skipProvider, numTestRows);
        }
    }

    void testSimpleStream()
    {
        if (testSimple)
        {
            DBGLOG("Simple tests: write then read");

            SequenceDataProvider seqProviderPW(40, false, true);
            SequenceDataProvider seqProviderRW(40, true, true);
            SequenceDataProvider seqProviderRC(40, true, false);
            SequenceDataProvider seqProviderPC(40, false, false);
            SkipSequenceDataProvider skipProvider(17);
            VariableDataProvider varcProvider(true);
            VariableDataProvider varsProvider(false);
            Sequence2DataProvider seq2Provider(40);
            ReservedDataProvider resProvider(40);
            RandomDataProvider randProvider(37);
            ICompressHandler * lz4 = queryCompressHandler(COMPRESS_METHOD_LZ4);

            runSimpleStream(nullptr, seqProviderPW, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, seqProviderRW, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, seqProviderRC, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, seqProviderPC, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, seq2Provider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, varsProvider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, varcProvider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(nullptr, resProvider, 0x100000, 0x100000, numTestRows);

            runSimpleStream(lz4, seqProviderPW, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, seqProviderRW, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, seqProviderRC, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, seqProviderPC, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, seq2Provider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, varsProvider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, varcProvider, 0x100000, 0x100000, numTestRows);
            runSimpleStream(lz4, resProvider, 0x100000, 0x100000, numTestRows);

            runSimpleStream(nullptr, seqProviderPW, 7, 0, numTestRows/10);
            runSimpleStream(nullptr, seqProviderRW, 7, 0, numTestRows/10);
            runSimpleStream(nullptr, seqProviderRC, 7, 0, numTestRows/10);
            runSimpleStream(nullptr, seqProviderPC, 7, 0, numTestRows/10);
            runSimpleStream(nullptr, seq2Provider, 7, 0, numTestRows/10);
            runSimpleStream(nullptr, resProvider, 7, 0, numTestRows/10);

            runSimpleStream(lz4, seqProviderPW, 43, 97, numTestRows/10);
            runSimpleStream(lz4, seqProviderRW, 43, 97, numTestRows/10);
            runSimpleStream(lz4, seqProviderRC, 43, 97, numTestRows/10);
            runSimpleStream(lz4, seqProviderPC, 43, 97, numTestRows/10);
            runSimpleStream(lz4, seq2Provider, 43, 97, numTestRows/10);
            runSimpleStream(lz4, resProvider, 43, 97, numTestRows/10);

            applyTests(&JlibStreamStressTest::runSimpleStream);
        }
    }

    void testBufferStream()
    {
        if (testBuffer)
        {
            DBGLOG("Simple tests: write then read");

            SequenceDataProvider seqProviderPW(40, false, true);
            SequenceDataProvider seqProviderRW(40, true, true);
            SequenceDataProvider seqProviderRC(40, true, false);
            SequenceDataProvider seqProviderPC(40, false, false);
            VariableDataProvider varcProvider(true);
            VariableDataProvider varsProvider(false);
            Sequence2DataProvider seq2Provider(40);
            ReservedDataProvider resProvider(40);

            runBufferStream(nullptr, seqProviderPW, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, seqProviderRW, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, seqProviderRC, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, seqProviderPC, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, seq2Provider, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, varsProvider, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, varcProvider, 0x100000, 0x100000, numTestRows);
            runBufferStream(nullptr, resProvider, 0x100000, 0x100000, numTestRows);
        }
    }

    void generateVarStrings(IBufferedSerialOutputStream & out, size32_t maxStringLength, offset_t minLength)
    {
        offset_t offset = 0;
        size32_t prevLength = 0;
        size32_t prime = 1997333137;
        unsigned delta = 0;
        while (offset < minLength)
        {
            size32_t length = (prevLength + prime) % maxStringLength;
            size32_t got;
            char * data = (char *)out.reserve(length+1, got);
            CPPUNIT_ASSERT(got > length);
            for (unsigned j=0; j< length; j++)
                data[j] = (char)(' ' + (delta + j) % 64);
            data[length] = 0;
            out.commit(length+1);
            offset += length+1;
            delta++;
        }
    }

    void generatePeekStringPairListStrings(IBufferedSerialOutputStream & out, const String pairedStrings[], size32_t numStrings)
    {
        // Generate a list of strings in pairs for testing peekStringList grouping:
        // - The first string must not be empty
        // - The second string can be empty (zero-length strings), this equates to a grouping of 2 for peekStringList
        // - The list is terminated with a null
        size32_t got = 0;
        for (size32_t i = 0; i < numStrings; i++)
        {
            size32_t length = pairedStrings[i].length();
            char * data = (char *)out.reserve(length+1, got);
            CPPUNIT_ASSERT(got > length);
            memcpy(data, pairedStrings[i].str(), length);
            data[length] = 0;
            out.commit(length+1);
        }
        char * terminator = (char *)out.reserve(1, got);
        terminator[0] = 0;
        out.commit(1);
    }

    static inline unsigned check(const char * testName, offset_t offset, const char * data, size32_t delta)
    {
        CPPUNIT_ASSERT(data != nullptr);
        unsigned j = 0;
        for (;;)
        {
            char c = data[j];
            if (!c)
                return j;
            if (unlikely(c != (char)(' ' + (j + delta) % 64)))
            {
                VStringBuffer msg("Invalid string read in %s: expected %02x got %02x at position %llu:%u", testName, (char)(' ' + (j + delta) % 64), c, offset, j);
                CPPUNIT_FAIL(msg);
            }
            j++;
        }
    }
    void testVarStringRead(IBufferedSerialInputStream & in, offset_t minLength)
    {
        offset_t offset = 0;
        unsigned delta = 0;
        StringBuffer str;
        while (offset < minLength)
        {
            CPPUNIT_ASSERT(readZeroTerminatedString(str.clear(), in));
            size32_t length = str.length();
            check("testVarStringRead", offset, str.str(), delta);
            offset += length+1;
            delta++;
        }
        CPPUNIT_ASSERT(in.eos());
    }
    void testVarStringPeek(IBufferedSerialInputStream & in, offset_t minLength)
    {
        offset_t offset = 0;
        unsigned delta = 0;
        while (offset < minLength)
        {
            size32_t length;
            const char * data = queryZeroTerminatedString(in, length);
            check("testVarStringPeek", offset, data, delta);
            in.skip(length+1);
            offset += length+1;
            delta++;
        }
        CPPUNIT_ASSERT(in.eos());
    }
    void testKVStringPeek(IBufferedSerialInputStream & in, offset_t minLength)
    {
        offset_t offset = 0;
        const char * next = nullptr;
        unsigned skipLen = 0;
        unsigned delta = 0;
        while (offset < minLength)
        {
            size32_t length;
            const char * data = next;
            next = nullptr;
            if (!data)
            {
                in.skip(skipLen);
                std::tie(data,next) = peekKeyValuePair(in, length);
                skipLen = length+1;
            }
            CPPUNIT_ASSERT(data != nullptr);
            unsigned len = check("testKVStringPeek", offset, data, delta);
            offset += len+1;
            delta++;
        }
        assertex(!next);
        in.skip(skipLen);
        CPPUNIT_ASSERT(in.eos());
    }

    void testStringListPeek(IBufferedSerialInputStream & in)
    {
        offset_t offset = 0;
        std::vector<size32_t> matches;
        unsigned skipLen = 0;
        const char * base = peekStringList(matches, in, skipLen);

        unsigned delta = 0;
        for (size32_t next : matches)
        {
            unsigned len = check("testStringListPeek", offset, base + next, delta);
            offset += len+1;
            delta++;
        }
        assertex(skipLen == offset);
        in.skip(skipLen);
        CPPUNIT_ASSERT(in.eos());
    }

    void testStringListPeekGrouping(IBufferedSerialInputStream & in, unsigned expectedNumStrings, const String pairedStrings[])
    {
        std::vector<size32_t> matches;
        unsigned skipLen = 0;
        constexpr int keyValuePairGrouping = 2;
        const char * base = peekStringList(matches, in, skipLen, keyValuePairGrouping);

        CPPUNIT_ASSERT_EQUAL(expectedNumStrings, (unsigned)matches.size());

        // Verify that we can access each string via the returned offsets
        // and that each string is null-terminated within the parsed region.
        // The vector contains alternating string pairs: The first string must not be empty string.
        bool valueExpected{false};
        for (size_t i = 0; i < matches.size(); ++i)
        {
            size32_t off = matches[i];
            const char * str = base + off;
            CPPUNIT_ASSERT_MESSAGE("String not matching expected results", memcmp(str, pairedStrings[i].str(), pairedStrings[i].length() + 1) == 0);

            if (!valueExpected)
                CPPUNIT_ASSERT_MESSAGE("Attribute name must not be empty", str[0] != '\0');

            valueExpected = !valueExpected;
        }

        in.skip(skipLen);
        CPPUNIT_ASSERT(in.eos());
    }

    void testVarStringBuffer()
    {
        size32_t maxStringLength = 0x10000;
        offset_t minFileLength = 0x4000000;
        MemoryBuffer buffer;

        {
            Owned<IBufferedSerialOutputStream> out = createBufferedSerialOutputStream(buffer);
            generateVarStrings(*out, maxStringLength, minFileLength);
        }

        {
            CCycleTimer timer;
            MemoryBuffer clone(buffer.length(), buffer.toByteArray());
            Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStreamFillMemory(clone);
            testVarStringRead(*in, minFileLength);
            DBGLOG("Buffer:testVarStringRead took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            MemoryBuffer clone(buffer.length(), buffer.toByteArray());
            Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStreamFillMemory(clone);
            testVarStringPeek(*in, minFileLength);
            DBGLOG("Buffer:testVarStringPeek took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            MemoryBuffer clone(buffer.length(), buffer.toByteArray());
            Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStreamFillMemory(clone);
            testKVStringPeek(*in, minFileLength);
            DBGLOG("Buffer:testKVStringPeek took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            MemoryBuffer clone(buffer.length(), buffer.toByteArray());
            Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStreamFillMemory(clone);
            testStringListPeek(*in);
            DBGLOG("Buffer:testStringListPeek took %lluus", timer.elapsedNs()/1000);
        }
    }

    void testGroupingInPeekStringList()
    {
        if (testBuffer)
        {
            MemoryBuffer buffer;
            unsigned numStrings{0};

            {
                // Empty stream test
                String nullString[] = {"\0"};
                MemoryBuffer emptyStreamBuffer(1, nullString);
                Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStream(emptyStreamBuffer);
                CCycleTimer timer;
                testStringListPeekGrouping(*in, numStrings, nullString);
                DBGLOG("Buffer:testStringListPeekGrouping took %lluus", timer.elapsedNs()/1000);
            }

            String pairedStrings[] = {"@Name1", "Value1", "Name2", "", "name3", "Value3", "@Name4", "Value4", "NAME5", ""};
            constexpr unsigned numPairedStrings = sizeof(pairedStrings) / sizeof(pairedStrings[0]);

            {
                // Generate attribute name/value pairs into the buffer.
                Owned<IBufferedSerialOutputStream> out = createBufferedSerialOutputStream(buffer);
                generatePeekStringPairListStrings(*out, pairedStrings, numPairedStrings);
                CPPUNIT_ASSERT(numPairedStrings > 0);
                CPPUNIT_ASSERT(numPairedStrings % 2 == 0);
            }

            {
                MemoryBuffer clone(buffer.length(), buffer.toByteArray());
                Owned<IBufferedSerialInputStream> in = createBufferedSerialInputStream(clone);
                CCycleTimer timer;
                testStringListPeekGrouping(*in, numPairedStrings, pairedStrings);
                DBGLOG("Buffer:testStringListPeekGrouping took %lluus", timer.elapsedNs()/1000);
            }
        }
    }

    void testVarStringFile(size32_t maxStringLength)
    {
        size32_t bufferSize = 0x4000;
        offset_t minFileLength = 0x400000;

        {
            Owned<IBufferedSerialOutputStream> out = createOutput(filename, bufferSize, nullptr, 0, false);
            generateVarStrings(*out, maxStringLength, minFileLength);
        }

        {
            CCycleTimer timer;
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize, nullptr, 0, false);
            testVarStringRead(*in, minFileLength);
            DBGLOG("File:testVarStringRead took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize, nullptr, 0, false);
            testVarStringPeek(*in, minFileLength);
            DBGLOG("File:testVarStringPeek took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize, nullptr, 0, false);
            testKVStringPeek(*in, minFileLength);
            DBGLOG("File:testKVStringPeek took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize, nullptr, 0, false);
            testStringListPeek(*in);
            DBGLOG("File:testStringListPeek took %lluus", timer.elapsedNs()/1000);
        }

        {
            CCycleTimer timer;
            Owned<IBufferedSerialInputStream> in = createInput(filename, bufferSize, nullptr, 0, true);
            testVarStringPeek(*in, minFileLength);
            DBGLOG("File:testVarStringPeekThreaded took %lluus", timer.elapsedNs()/1000);
        }
    }
    void testVarStringFile()
    {
        testVarStringFile(0x10000);
        testVarStringFile(200);
    }
    void testVarStringHelpers()
    {
        if (testBuffer)
        {
            testVarStringBuffer();
            testVarStringFile();
        }
    }

    IPropertyTree *createCompatibilityConfigPropertyTree()
    {
        // Creates a complex nested property tree with multiple compatibility elements for serialization testing
        Owned<IPropertyTree> root = createPTree("__array__");

        // Helper lambda to add property elements with name/value attributes
        auto addProperty = [](IPropertyTree *parent, const char *name, const char *value = nullptr)
        {
            IPropertyTree *prop = parent->addPropTree("property");
            prop->setProp("@name", name);
            if (value)
                prop->setProp("@value", value);
        };

        // Helper lambda to add operation/accepts/uses elements
        auto addNamedElement = [](IPropertyTree *parent, const char *elementName, const char *name, const char *presence)
        {
            IPropertyTree *elem = parent->addPropTree(elementName);
            elem->setProp("@name", name);
            elem->setProp("@presence", presence);
        };

        // Helper lambda to add valueType elements with maskStyle children
        auto addValueType = [](IPropertyTree *parent, const char *name, const char *presence, bool addMaskStyle = false, const char *setName = nullptr)
        {
            IPropertyTree *valueType = parent->addPropTree("valueType");
            valueType->setProp("@name", name);
            valueType->setProp("@presence", presence);

            if (addMaskStyle)
            {
                IPropertyTree *maskStyle1 = valueType->addPropTree("maskStyle");
                maskStyle1->setProp("@name", "keep-last-4-numbers");
                maskStyle1->setProp("@presence", "r");

                IPropertyTree *maskStyle2 = valueType->addPropTree("maskStyle");
                maskStyle2->setProp("@name", "mask-last-4-numbers");
                maskStyle2->setProp("@presence", "o");
            }

            if (setName)
            {
                IPropertyTree *set = valueType->addPropTree("Set");
                set->setProp("@name", setName);
                set->setProp("@presence", "r");
            }
        };

        // Helper lambda to add rule elements
        auto addRule = [](IPropertyTree *parent, const char *contentType, const char *presence)
        {
            IPropertyTree *rule = parent->addPropTree("rule");
            rule->setProp("@contentType", contentType);
            rule->setProp("@presence", presence);
        };

        // First compatibility element
        {
            IPropertyTree *compatibility = root->addPropTree("__item__");
            IPropertyTree *compat = compatibility->addPropTree("compatibility");

            // Context
            IPropertyTree *context = compat->addPropTree("context");
            context->setProp("@domain", "urn:hpcc:unittest");
            context->setProp("@version", "0");
            addProperty(context, "valuetype-set", "*");
            addProperty(context, "rule-set", "*");

            // Operations
            addNamedElement(compat, "operation", "maskValue", "r");
            addNamedElement(compat, "operation", "maskContent", "r");
            addNamedElement(compat, "operation", "maskMarkupValue", "o");

            // Accepts
            addNamedElement(compat, "accepts", "valuetype-set", "r");
            addNamedElement(compat, "accepts", "valuetype-set:value-type-set-a", "r");
            addNamedElement(compat, "accepts", "valuetype-set:value-type-set-b", "r");
            addNamedElement(compat, "accepts", "rule-set", "r");
            addNamedElement(compat, "accepts", "rule-set:rule-set-2", "r");
            addNamedElement(compat, "accepts", "required-acceptance", "r");
            addNamedElement(compat, "accepts", "optional-acceptance", "o");

            // Uses
            addNamedElement(compat, "uses", "valuetype-set", "r");
            addNamedElement(compat, "uses", "valuetype-set:value-type-set-a", "r");
            addNamedElement(compat, "uses", "valuetype-set:value-type-set-b", "r");
            addNamedElement(compat, "uses", "rule-set", "r");
            addNamedElement(compat, "uses", "rule-set:rule-set-2", "r");
            addNamedElement(compat, "uses", "required-acceptance", "p");
            addNamedElement(compat, "uses", "optional-acceptance", "p");

            // ValueTypes
            addValueType(compat, "secret", "r");
            addValueType(compat, "secret-if-a", "r", true, "value-type-set-a");
            addValueType(compat, "secret-if-b", "r", false, "value-type-set-b");
            addValueType(compat, "*", "r");

            // Rules
            addRule(compat, "", "r");
            addRule(compat, "xml", "r");
        }

        // Second compatibility element
        {
            IPropertyTree *compatibility = root->addPropTree("__item__");
            IPropertyTree *compat = compatibility->addPropTree("compatibility");

            IPropertyTree *context = compat->addPropTree("context");
            context->setProp("@domain", "urn:hpcc:unittest");
            context->setProp("@version", "0");
            addProperty(context, "valuetype-set", "value-type-set-a");
            addProperty(context, "rule-set", "");

            addValueType(compat, "secret", "r");
            addValueType(compat, "secret-if-a", "r", true, "value-type-set-a");
            addValueType(compat, "secret-if-b", "p", false, "value-type-set-b");

            addRule(compat, "", "r");
            addRule(compat, "xml", "r");
        }

        return root.getClear();
    };

    void testCrcBufferedOutputStream()
    {
        DBGLOG("Testing CRC buffered output stream");

        // Test data with known CRC values
        constexpr const char *testData1 = "Hello, World!";
        constexpr size32_t len1 = strlen(testData1);
        constexpr const char *testData2 = "This is test data for CRC calculation.";
        constexpr size32_t len2 = strlen(testData2);
        constexpr const char *testData3 = "0123456789ABCDEF";
        constexpr size32_t len3 = strlen(testData3);

        // Test: Multiple put() calls with multiple put() methods using MemoryBuffer
        {
            MemoryBuffer buffer;
            {
                Owned<IBufferedSerialOutputStream> serialOut = createBufferedSerialOutputStream(buffer);
                Owned<ICrcSerialOutputStream> crcOut = createCrcOutputStream(serialOut);

                // Write test data with flush to ensure CRC is calculated
                crcOut->put(len1, testData1);
                crcOut->put(len3, testData3);
                crcOut->flush();

                // Calculate expected CRC manually
                CRC32 expectedCrc;
                expectedCrc.tally(len1, testData1);
                expectedCrc.tally(len3, testData3);

                CPPUNIT_ASSERT_EQUAL(expectedCrc.get(), crcOut->queryCrc());
                DBGLOG("Multiple put() test using MemoryBuffer passed - CRC: 0x%08X", crcOut->queryCrc());
            }
        }

        // Test: CRC calculation using buffered Serial output stream
        {
            StringBuffer buffer;
            {
                Owned<IBufferedSerialOutputStream> serialOut = createBufferedSerialOutputStream(buffer);
                Owned<ICrcSerialOutputStream> crcOut = createCrcOutputStream(serialOut);

                // Write test data with flush to ensure CRC is calculated
                crcOut->put(len2, testData2);
                crcOut->put(len1, testData1);
                crcOut->flush();

                // Calculate expected CRC manually
                CRC32 expectedCrc;
                expectedCrc.tally(len2, testData2);
                expectedCrc.tally(len1, testData1);

                CPPUNIT_ASSERT_EQUAL(expectedCrc.get(), crcOut->queryCrc());
                DBGLOG("Sequential put() test passed - CRC: 0x%08X", crcOut->queryCrc());
            }
        }

        // Test: CRC calculation with IPropertyTree serialization (similar to dasds.cpp pattern)
        {
            MemoryBuffer buffer;
            {
                // Create a complex property tree for testing
                Owned<IPropertyTree> testTree = createCompatibilityConfigPropertyTree();

                Owned<IBufferedSerialOutputStream> serialOut = createBufferedSerialOutputStream(buffer);
                Owned<ICrcSerialOutputStream> crcSerialStream = createCrcOutputStream(serialOut);
                constexpr size32_t blockSize1mb = 1 * 1024 * 1024;
                Owned<IBufferedSerialOutputStream> crcOutStream = createBufferedOutputStream(crcSerialStream, blockSize1mb);

                // Serialize the property tree to the CRC stream with flush to ensure CRC is calculated
                testTree->serializeToStream(*crcOutStream);
                crcOutStream->flush();
                crcSerialStream->flush();

                // Calculate expected CRC by manually processing the buffer contents
                CRC32 expectedCrc;
                expectedCrc.tally(buffer.length(), buffer.toByteArray());

                CPPUNIT_ASSERT_EQUAL(expectedCrc.get(), crcSerialStream->queryCrc());
                DBGLOG("IPropertyTree serialization test passed - CRC: 0x%08X", crcSerialStream->queryCrc());
            }
        }
    }

    void testPathologicalRows()
    {
        LargeRowDataProvider largeCount50K(true, 50'000);
        LargeRowDataProvider largeCount10M(true, 10'000'000);
        LargeRowDataProvider largeSize50K(false, 50'000);
        LargeRowDataProvider largeSize10M(false, 10'000'000);

        ICompressHandler * lz4 = queryCompressHandler(COMPRESS_METHOD_LZ4);

        runSimpleStream(nullptr, largeCount50K, 0x100000, 0x100000, 2000);
        runSimpleStream(nullptr, largeCount10M, 0x100000, 0x100000, 10);
        runSimpleStream(nullptr, largeSize50K, 0x100000, 0x100000, 2000);
        runSimpleStream(nullptr, largeSize10M, 0x100000, 0x100000, 10);
    }

    void testIncSequentialStream()
    {
        if (testIncSequential)
        {
            DBGLOG("Sequential tests: write then read alternating, increasing sizes");
            applyTests(&JlibStreamStressTest::runIncSequentialStream);
        }
    }

    void testEvenSequentialStream()
    {
        if (testEvenSequential)
        {
            DBGLOG("Sequential tests: write then read alternating, even sizes");
            applyTests(&JlibStreamStressTest::runEvenSequentialStream);
        }
    }

    void testParallelStream()
    {
        if (testParallel)
        {
            DBGLOG("Parallel tests: write and read in parallel, even sizes");
            applyTests(&JlibStreamStressTest::runParallelStream);
        }
    }

    void testThreadedWriteStream()
    {
        if (testThreadedWrite)
        {
            DBGLOG("Threaded write tests: threaded write and read sequentially");
            applyTests(&JlibStreamStressTest::runThreadedWriteStream);
        }
    }

protected:
    IBufferedSerialInputStream * createInput(const char * filename, unsigned bufferSize, ICompressHandler * compressHandler, unsigned decompressedSize, bool threaded)
    {
        Owned<IFile> file = createIFile(filename);
        Owned<IFileIO> io = file->open(IFOread);
        Owned<ISerialInputStream> in = createSerialInputStream(io);
        if (compressHandler)
        {
            const char *options = nullptr;
            Owned<IExpander> decompressor = compressHandler->getExpander(options);

            Owned<IBufferedSerialInputStream> stream = createBufferedInputStream(in, bufferSize);
            Owned<ISerialInputStream> decompressed = createDecompressingInputStream(stream, decompressor);
            return createBufferedInputStream(decompressed, decompressedSize, threaded);
        }
        else
            return createBufferedInputStream(in, bufferSize, threaded);
    }

    IBufferedSerialOutputStream * createOutput(const char * filename, unsigned bufferSize, ICompressHandler * compressHandler, unsigned decompressedSize, bool threaded)
    {
        Owned<IFile> file = createIFile(filename);
        Owned<IFileIO> io = file->open(IFOcreate);
        Owned<ISerialOutputStream> out = createSerialOutputStream(io);
        if (compressHandler)
        {
            const char *options = nullptr;
            Owned<ICompressor> compressor = compressHandler->getCompressor(options);

            Owned<IBufferedSerialOutputStream> stream = createBufferedOutputStream(out, bufferSize);
            Owned<ISerialOutputStream> compressed = createCompressingOutputStream(stream, compressor);
            return createBufferedOutputStream(compressed, decompressedSize, threaded);
        }
        else
            return createBufferedOutputStream(out, bufferSize, threaded);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibStreamStressTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibStreamStressTest, "JlibStreamStressTest" );



#endif // _USE_CPPUNIT
