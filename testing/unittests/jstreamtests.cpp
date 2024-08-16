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

#include "unittests.hpp"

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x ? x : ""),std::string(y ? y : ""))

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
        CPPUNIT_TEST(testSimpleStream);     // Write a file and then read the results
        CPPUNIT_TEST(testIncSequentialStream); // write a file and read results after each flush
        CPPUNIT_TEST(testEvenSequentialStream); // write a file and read results after each flush
        CPPUNIT_TEST(testParallelStream);   // write a file and read in parallel from a separate thread
        CPPUNIT_TEST(testThreadedWriteStream);   // write a file using a threaded writer
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
                totalRead += dataProvider.check(in, i);

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
            VariableDataProvider varcProvider(true);
            VariableDataProvider varsProvider(false);
            Sequence2DataProvider seq2Provider(40);
            ReservedDataProvider resProvider(40);
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
