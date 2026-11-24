/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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
#include "azureblob.hpp"
#include "jlog.hpp"
#include "jplane.hpp"
#include "jstream.hpp"
#include "jthread.hpp"
#include "jutil.hpp"

#include <cppunit/TestCaller.h>
#include <cppunit/TestSuite.h>
#include <azure/storage/blobs.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "jstats.h"

static constexpr const char mockAccountKey[] = "dGVzdC1rZXk=";

class CTestAzureBlockOperations : public CInterfaceOf<IAzureBlockOperations>
{
public:
    virtual void initializeEmptyBlob() override
    {
        if (operationDelayMs)
            Sleep(operationDelayMs);
        emptyBlobInitialized = true;
    }

    virtual void stageBlock(unsigned sequence, const byte * data, size32_t length) override
    {
        if (operationDelayMs)
            Sleep(operationDelayMs);
        std::unique_lock<std::mutex> lock(mutex);
        startedBlocks.emplace_back(sequence);
        changed.notify_all();
        if (holdAllBlocks || sequence == heldSequence)
            changed.wait(lock, [this] { return releaseHeldBlock; });
        if ((failFirstBlock && sequence == 1) || sequence == failSequence)
            throw makeStringException(7101, "injected StageBlock failure");

        const char * bytes = reinterpret_cast<const char *>(data);
        stagedBytes[sequence] = std::string(bytes, length);
        completedBlocks.emplace_back(sequence);
        changed.notify_all();
    }

    virtual void commitBlockList(unsigned count) override
    {
        if (operationDelayMs)
            Sleep(operationDelayMs);
        std::lock_guard<std::mutex> lock(mutex);
        if (failCommit)
            throw makeStringException(7102, "injected CommitBlockList failure");
        for (unsigned sequence = 1; sequence <= count; ++sequence)
            CPPUNIT_ASSERT_MESSAGE("A committed prefix must already be staged", stagedBytes.find(sequence) != stagedBytes.end());
        successfulCommits.emplace_back(count);
    }

    bool waitForStartedBlocks(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(5), [this, count]
        {
            return startedBlocks.size() >= count;
        });
    }

    bool waitForCompletedBlocks(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(5), [this, count]
        {
            return completedBlocks.size() >= count;
        });
    }

    void releaseBlock()
    {
        std::lock_guard<std::mutex> lock(mutex);
        releaseHeldBlock = true;
        changed.notify_all();
    }

    std::vector<unsigned> completedBlockOrder() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return completedBlocks;
    }

    std::vector<unsigned> successfulCommitCounts() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return successfulCommits;
    }

    std::string readCommittedBytes(unsigned count) const
    {
        std::lock_guard<std::mutex> lock(mutex);
        std::string result;
        for (unsigned sequence = 1; sequence <= count; ++sequence)
            result += stagedBytes.at(sequence);
        return result;
    }

    unsigned heldSequence = 0;
    unsigned failSequence = 0;
    unsigned operationDelayMs = 0;
    bool holdAllBlocks = false;
    bool failFirstBlock = false;
    bool failCommit = false;
    bool emptyBlobInitialized = false;

private:
    mutable std::mutex mutex;
    std::condition_variable changed;
    bool releaseHeldBlock = false;
    std::vector<unsigned> startedBlocks;
    std::vector<unsigned> completedBlocks;
    std::map<unsigned, std::string> stagedBytes;
    std::vector<unsigned> successfulCommits;
};

template <class Action>
static bool throwsHPCCException(Action action)
{
    try
    {
        action();
    }
    catch (IException * error)
    {
        error->Release();
        return true;
    }
    return false;
}

// Returns the error code of the IException thrown by action, or 0 if none is thrown.
template <class Action>
static int thrownErrorCode(Action action)
{
    try
    {
        action();
    }
    catch (IException * error)
    {
        int code = error->errorCode();
        error->Release();
        return code;
    }
    return 0;
}

static StorageApiConfig makeStorageWriterConfig(unsigned concurrency, unsigned chunkBytes)
{
    StorageApiConfig config;
    config.parallelWriteConcurrency = concurrency;
    config.parallelWriteChunkBytes = chunkBytes;
    return config;
}

/*
 * Exercises the blob writer through IFileIO with injected, in-memory block operations.
 * Covers chunk ordering, buffering, backpressure, worker lifecycle, configuration,
 * failure propagation, sequential fallback and statistics. No Azure service is contacted.
 */
class AzureBlobWriteLifecycleTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobWriteLifecycleTest);
        CPPUNIT_TEST(testOutOfOrderStagesCommitInOrder);
        CPPUNIT_TEST(testFlushIsNoOp);
        CPPUNIT_TEST(testCommitsOnlyOnClose);
        CPPUNIT_TEST(testSequentialFallback);
        CPPUNIT_TEST(testSequentialStageFailurePreventsCommit);
        CPPUNIT_TEST(testStageFailurePropagates);
        CPPUNIT_TEST(testInvalidConfigurationRejected);
        CPPUNIT_TEST(testBufferSizeLimit);
        CPPUNIT_TEST(testSmallWritesShareChunk);
        CPPUNIT_TEST(testWorkersGrowOnDemand);
        CPPUNIT_TEST(testCloseStopsWorkers);
        CPPUNIT_TEST(testCommitFailureStopsWorkers);
        CPPUNIT_TEST(testOperationStatistics);
        CPPUNIT_TEST(testCloseWriteStatistics);
    CPPUNIT_TEST_SUITE_END();

    IFile *createFile(const StorageApiConfig &config, CTestAzureBlockOperations &operations)
    {
        static constexpr const char mockAccountName[] = "testaccount";
        static std::atomic<unsigned> counter{0};
        std::string url = "https://test.invalid/container/lifecycle-" + std::to_string(++counter);
        return createAzureBlob(url.c_str(), mockAccountName, mockAccountKey, config, &operations);
    }

    void testOutOfOrderStagesCommitInOrder()
    {
        CTestAzureBlockOperations operations;
        operations.heldSequence = 1;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        {
            COnScopeExit releaseOnExit([&] { operations.releaseBlock(); });
            CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(8), writer->write(0, 8, "abcdefgh"));
            CPPUNIT_ASSERT_MESSAGE("Both block uploads should start", operations.waitForStartedBlocks(2));
            CPPUNIT_ASSERT_MESSAGE("The second block should finish while the first is held", operations.waitForCompletedBlocks(1));
        }
        writer->close();

        const auto completed = operations.completedBlockOrder();
        CPPUNIT_ASSERT_EQUAL(size_t(2), completed.size());
        CPPUNIT_ASSERT_EQUAL(2U, completed[0]);
        CPPUNIT_ASSERT_EQUAL(1U, completed[1]);
        const auto commits = operations.successfulCommitCounts();
        CPPUNIT_ASSERT_EQUAL(size_t(1), commits.size());
        CPPUNIT_ASSERT_EQUAL(2U, commits.back());
        CPPUNIT_ASSERT_EQUAL(std::string("abcdefgh"), operations.readCommittedBytes(2));
    }

    void testFlushIsNoOp()
    {
        CTestAzureBlockOperations operations;
        operations.heldSequence = 2;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        std::future<void> action;
        {
            COnScopeExit releaseOnExit([&] { operations.releaseBlock(); });
            CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(9), writer->write(0, 9, "abcdefghi"));
            CPPUNIT_ASSERT_MESSAGE("Both full block uploads should start", operations.waitForStartedBlocks(2));

            action = std::async(std::launch::async, [&] { writer->flush(); });
            CPPUNIT_ASSERT_MESSAGE("Flush must not wait for outstanding uploads",
                action.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
            action.get();
            CPPUNIT_ASSERT_MESSAGE("Flush must not commit a staged prefix", operations.successfulCommitCounts().empty());
            CPPUNIT_ASSERT_EQUAL(size_t(1), operations.completedBlockOrder().size());

            action = std::async(std::launch::async, [&] { writer->close(); });
            CPPUNIT_ASSERT_MESSAGE("Close must wait for outstanding uploads",
                action.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout);
            CPPUNIT_ASSERT(operations.successfulCommitCounts().empty());
        }
        CPPUNIT_ASSERT_MESSAGE("Close should finish once outstanding uploads finish",
            action.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        action.get();

        const auto commits = operations.successfulCommitCounts();
        CPPUNIT_ASSERT_EQUAL(size_t(1), commits.size());
        CPPUNIT_ASSERT_EQUAL(3U, commits.front());
        CPPUNIT_ASSERT_EQUAL(std::string("abcdefghi"), operations.readCommittedBytes(3));
    }

    void testCommitsOnlyOnClose()
    {
        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        const std::string data(600 * 4 + 1, 'x');
        CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(data.size()), writer->write(0, data.size(), data.data()));
        CPPUNIT_ASSERT_MESSAGE("Uploading many blocks must not commit intermediate prefixes",
            operations.successfulCommitCounts().empty());
        writer->flush();
        CPPUNIT_ASSERT(operations.successfulCommitCounts().empty());
        writer->close();

        const auto commits = operations.successfulCommitCounts();
        CPPUNIT_ASSERT_EQUAL(size_t(1), commits.size());
        CPPUNIT_ASSERT_EQUAL(601U, commits.front());
        CPPUNIT_ASSERT_EQUAL(data, operations.readCommittedBytes(601));
        writer->close();
        CPPUNIT_ASSERT_EQUAL(size_t(1), operations.successfulCommitCounts().size());
    }

    void testSequentialFallback()
    {
        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(0, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        CPPUNIT_ASSERT(operations.emptyBlobInitialized);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Non-empty writes require a buffer",
            azureWriterError, thrownErrorCode([&] { writer->write(0, 1, nullptr); }));
        CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(5), writer->write(0, 5, "hello"));
        writer->close();
        CPPUNIT_ASSERT_EQUAL(std::string("hello"), operations.readCommittedBytes(1));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("A closed writer must reject further writes",
            azureWriterError, thrownErrorCode([&] { writer->write(5, 1, "!"); }));
    }

    void testSequentialStageFailurePreventsCommit()
    {
        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(0, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(4), writer->write(0, 4, "good"));

        operations.failSequence = 2;
        CPPUNIT_ASSERT_EQUAL_MESSAGE("The failed stage must be reported by write()",
            7101, thrownErrorCode([&] { writer->write(4, 4, "fail"); }));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Subsequent writes must rethrow the retained stage error",
            7101, thrownErrorCode([&] { writer->write(4, 1, "!"); }));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Close must report the retained stage error",
            7101, thrownErrorCode([&] { writer->close(); }));
        CPPUNIT_ASSERT_MESSAGE("A failed sequential write must not commit a staged prefix",
            operations.successfulCommitCounts().empty());
    }

    void testStageFailurePropagates()
    {
        const unsigned initialThreadCount = getThreadCount();
        for (size32_t length : {2U, 4U})
        {
            CTestAzureBlockOperations operations;
            operations.failFirstBlock = true;
            operations.operationDelayMs = 10;
            Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
            Owned<IFileIO> writer = file->open(IFOcreate);
            int writeError = thrownErrorCode([&] { CPPUNIT_ASSERT_EQUAL(length, writer->write(0, length, "abcd")); });
            CPPUNIT_ASSERT_MESSAGE("Only a full-chunk write may report the injected stage failure",
                writeError == 0 || (length == 4 && writeError == 7101));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Close must report the injected stage failure",
                7101, thrownErrorCode([&] { writer->close(); }));
            CPPUNIT_ASSERT(writer->getStatistic(StTimeBlobWriteStage) >= operations.operationDelayMs * 1000000ULL);
            CPPUNIT_ASSERT_EQUAL(stat_type(0), writer->getStatistic(StTimeBlobWriteCommit));
            CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
            CPPUNIT_ASSERT(operations.successfulCommitCounts().empty());
        }
    }

    void testInvalidConfigurationRejected()
    {
        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(513, 4), operations);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Invalid concurrency must be rejected before creating the writer",
            azureConfigError, thrownErrorCode([&] { Owned<IFileIO> writer = file->open(IFOcreate); }));
    }

    void testBufferSizeLimit()
    {
        // Concurrency 15 gives 16 chunk buffers: 15 uploading plus 1 being filled.
        constexpr unsigned chunkAtLimit = 256U * 1024 * 1024;
        for (unsigned chunkBytes : {chunkAtLimit, chunkAtLimit + 1})
        {
            CTestAzureBlockOperations operations;
            Owned<IFile> file = createFile(makeStorageWriterConfig(15, chunkBytes), operations);
            CPPUNIT_ASSERT_MESSAGE("Pools of 4 GiB or more must be rejected",
                throwsHPCCException([&] { Owned<IFileIO> writer = file->open(IFOcreate); }));
            CPPUNIT_ASSERT_MESSAGE("Invalid buffer sizes must not truncate the blob", !operations.emptyBlobInitialized);
        }

        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(15, std::numeric_limits<size32_t>::max() / 16), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        CPPUNIT_ASSERT_MESSAGE("A pool just below 4 GiB should be accepted", operations.emptyBlobInitialized);
        writer->close();
    }

    void testSmallWritesShareChunk()
    {
        const unsigned initialThreadCount = getThreadCount();
        CTestAzureBlockOperations operations;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 32), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        std::string data;
        for (unsigned i = 0; i < 83; ++i)
            data.push_back('A' + i % 26);
        for (size32_t pos = 0; pos < data.size();)
        {
            size32_t length = std::min<size32_t>(10, data.size() - pos);
            CPPUNIT_ASSERT_EQUAL(length, writer->write(pos, length, data.data() + pos));
            pos += length;
            if (pos < 32)
                CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
        }
        writer->close();
        CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
        const auto commits = operations.successfulCommitCounts();
        CPPUNIT_ASSERT_EQUAL(size_t(1), commits.size());
        CPPUNIT_ASSERT_EQUAL(3U, commits.front());
        CPPUNIT_ASSERT_EQUAL(data, operations.readCommittedBytes(3));
    }

    void testWorkersGrowOnDemand()
    {
        const unsigned initialThreadCount = getThreadCount();
        CTestAzureBlockOperations operations;
        operations.holdAllBlocks = true;
        Owned<IFile> file = createFile(makeStorageWriterConfig(3, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        std::future<void> action;
        {
            COnScopeExit releaseOnExit([&] { operations.releaseBlock(); });
            writer->write(0, 3, "abc");
            CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
            writer->write(3, 1, "d");
            CPPUNIT_ASSERT(operations.waitForStartedBlocks(1));
            CPPUNIT_ASSERT_EQUAL(initialThreadCount + 1, getThreadCount());
            writer->write(4, 4, "efgh");
            CPPUNIT_ASSERT(operations.waitForStartedBlocks(2));
            CPPUNIT_ASSERT_EQUAL(initialThreadCount + 2, getThreadCount());
            writer->write(8, 4, "ijkl");
            CPPUNIT_ASSERT(operations.waitForStartedBlocks(3));
            CPPUNIT_ASSERT_EQUAL(initialThreadCount + 3, getThreadCount());
            CPPUNIT_ASSERT_EQUAL(stat_type(3), writer->getStatistic(StNumBlobWritePeakConcurrency));

            writer->write(12, 4, "mnop");
            CPPUNIT_ASSERT_MESSAGE("The caller's extra buffer must not add a worker",
                initialThreadCount + 3 == getThreadCount());

            action = std::async(std::launch::async, [&] { writer->write(16, 4, "qrst"); });
            CPPUNIT_ASSERT_MESSAGE("A full buffer pool must apply backpressure",
                action.wait_for(std::chrono::milliseconds(50)) == std::future_status::timeout);
            CPPUNIT_ASSERT_EQUAL(initialThreadCount + 3, getThreadCount());
        }
        CPPUNIT_ASSERT_MESSAGE("Writing should resume when a slot is freed",
            action.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
        action.get();
        writer->close();
        CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
        CPPUNIT_ASSERT_EQUAL(std::string("abcdefghijklmnopqrst"), operations.readCommittedBytes(5));
        CPPUNIT_ASSERT_MESSAGE("Backpressure must record time spent waiting for a free slot",
            writer->getStatistic(StTimeBlobWriteWait) > 0);
        CPPUNIT_ASSERT_EQUAL(stat_type(3), writer->getStatistic(StNumBlobWritePeakConcurrency));
    }

    void testCloseStopsWorkers()
    {
        const unsigned initialThreadCount = getThreadCount();
        const std::string data("abcdefghij");
        for (size32_t length : {2U, 10U})
        {
            CTestAzureBlockOperations operations;
            operations.heldSequence = 1;
            Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
            Owned<IFileIO> writer = file->open(IFOcreate);
            {
                COnScopeExit releaseOnExit([&] { operations.releaseBlock(); });
                writer->write(0, length, data.data());
                CPPUNIT_ASSERT_EQUAL(initialThreadCount + (length >= 4 ? 2 : 0), getThreadCount());
            }
            writer->close();
            CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
            CPPUNIT_ASSERT_EQUAL(data.substr(0, length), operations.readCommittedBytes((length + 3) / 4));
            writer->close();
            CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
            CPPUNIT_ASSERT_MESSAGE("A closed writer must reject further writes",
                throwsHPCCException([&] { writer->write(length, 1, "k"); }));
        }
    }

    void testCommitFailureStopsWorkers()
    {
        const unsigned initialThreadCount = getThreadCount();
        CTestAzureBlockOperations operations;
        operations.failCommit = true;
        operations.operationDelayMs = 10;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        writer->write(0, 8, "abcdefgh");
        const auto writeTime = writer->getStatistic(StTimeDiskWriteIO);
        CPPUNIT_ASSERT_MESSAGE("Close must report a failed commit", throwsHPCCException([&] { writer->close(); }));
        CPPUNIT_ASSERT(writer->getStatistic(StTimeDiskWriteIO) >= writeTime + operations.operationDelayMs * 1000000ULL);
        CPPUNIT_ASSERT(writer->getStatistic(StTimeBlobWriteCommit) >= operations.operationDelayMs * 1000000ULL);
        CPPUNIT_ASSERT_EQUAL(initialThreadCount, getThreadCount());
        CPPUNIT_ASSERT_MESSAGE("A repeated close must preserve the failure", throwsHPCCException([&] { writer->close(); }));
        CPPUNIT_ASSERT_MESSAGE("A failed writer must reject further writes",
            throwsHPCCException([&] { writer->write(8, 1, "i"); }));
        CPPUNIT_ASSERT(operations.successfulCommitCounts().empty());
    }

    void testOperationStatistics()
    {
        for (unsigned concurrency : {0U, 1U, 2U})
        {
            CTestAzureBlockOperations operations;
            operations.operationDelayMs = 10;
            Owned<IFile> file = createFile(makeStorageWriterConfig(concurrency, 4), operations);
            Owned<IFileIO> writer = file->open(IFOcreate);
            CPPUNIT_ASSERT(writer->getStatistic(StTimeBlobWriteInitialize) >= operations.operationDelayMs * 1000000ULL);
            CPPUNIT_ASSERT_EQUAL(stat_type(0), writer->getStatistic(StTimeBlobWriteStage));
            CPPUNIT_ASSERT_EQUAL(stat_type(0), writer->getStatistic(StNumBlobWritePeakConcurrency));
            writer->write(0, 5, "abcde");
            CPPUNIT_ASSERT_EQUAL(stat_type(0), writer->getStatistic(StTimeBlobWriteCommit));
            writer->close();
            const stat_type expectedStages = concurrency == 0 ? 1 : 2;
            CPPUNIT_ASSERT(writer->getStatistic(StTimeBlobWriteStage) >= expectedStages * operations.operationDelayMs * 1000000ULL);
            CPPUNIT_ASSERT(writer->getStatistic(StTimeBlobWriteCommit) >= operations.operationDelayMs * 1000000ULL);
            if (concurrency != 1)
                CPPUNIT_ASSERT_EQUAL(stat_type(0), writer->getStatistic(StTimeBlobWriteWait));
            CPPUNIT_ASSERT(writer->getStatistic(StNumBlobWritePeakConcurrency) >= 1);
            CPPUNIT_ASSERT(writer->getStatistic(StNumBlobWritePeakConcurrency) <= std::max(concurrency, 1U));

            const auto stageTime = writer->getStatistic(StTimeBlobWriteStage);
            const auto commitTime = writer->getStatistic(StTimeBlobWriteCommit);
            writer->close();
            CPPUNIT_ASSERT_EQUAL(stageTime, writer->getStatistic(StTimeBlobWriteStage));
            CPPUNIT_ASSERT_EQUAL(commitTime, writer->getStatistic(StTimeBlobWriteCommit));
        }
    }

    void testCloseWriteStatistics()
    {
        CTestAzureBlockOperations operations;
        operations.operationDelayMs = 10;
        Owned<IFile> file = createFile(makeStorageWriterConfig(2, 4), operations);
        Owned<IFileIO> writer = file->open(IFOcreate);
        writer->write(0, 3, "abc");
        const auto writeTime = writer->getStatistic(StTimeDiskWriteIO);
        const auto writeCount = writer->getStatistic(StNumDiskWrites);
        const auto writeBytes = writer->getStatistic(StSizeDiskWrite);
        writer->close();
        const auto totalWriteTime = writer->getStatistic(StTimeDiskWriteIO);
        CPPUNIT_ASSERT(totalWriteTime >= writeTime + 2 * operations.operationDelayMs * 1000000ULL);
        CPPUNIT_ASSERT_EQUAL(writeCount, writer->getStatistic(StNumDiskWrites));
        CPPUNIT_ASSERT_EQUAL(writeBytes, writer->getStatistic(StSizeDiskWrite));
        writer->close();
        CPPUNIT_ASSERT_EQUAL(totalWriteTime, writer->getStatistic(StTimeDiskWriteIO));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobWriteLifecycleTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobWriteLifecycleTest, "AzureBlobWriteLifecycleTest");

/*
 * Verifies deterministic block ID generation and sequence bounds, including Azure's
 * 50,000-block limit. This is a local-only test with no Azure service dependency.
 */
class AzureBlockIdTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlockIdTest);
        CPPUNIT_TEST(testGenerationAndBounds);
    CPPUNIT_TEST_SUITE_END();

    void testGenerationAndBounds()
    {
        CPPUNIT_ASSERT_EQUAL(std::string("AAAA00000001"), makeAzureBlockId("AAAA", 1));
        CPPUNIT_ASSERT_EQUAL(std::string("AAAA00050000"), makeAzureBlockId("AAAA", maxAzureBlockCount));
        CPPUNIT_ASSERT_EQUAL(std::string("BBBB00000001"), makeAzureBlockId("BBBB", 1));
        CPPUNIT_ASSERT(throwsHPCCException([] { makeAzureBlockId("AAAA", 0); }));
        CPPUNIT_ASSERT(throwsHPCCException([] { makeAzureBlockId("AAAA", maxAzureBlockCount + 1); }));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlockIdTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlockIdTest, "AzureBlockIdTest");

/*
 * Validates direct shared-key blob URLs, including Azure and Azurite URL forms,
 * and rejects invalid connection arguments. Clients and directory iterators are
 * constructed but never used to send requests; no Azure service is contacted.
 */
class AzureBlobUrlTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobUrlTest);
        CPPUNIT_TEST(testStandaloneUrls);
        CPPUNIT_TEST(testCustomHostPathStyleUrl);
        CPPUNIT_TEST(testInvalidArguments);
    CPPUNIT_TEST_SUITE_END();

public:
    void testStandaloneUrls()
    {
        START_TEST
        struct Connection
        {
            const char *url;
            bool pathStyle;
        };
        const Connection connections[] = {
            {"https://devstoreaccount1.blob.core.windows.net/devstoreaccount1/nested/blob%20name", false},
            {"http://127.0.0.1:10000/devstoreaccount1/container/blob", true},
            {"http://localhost:10000/devstoreaccount1/container/", true}
        };
        for (const auto &connection : connections)
        {
            Owned<IFile> file = createAzureBlob(connection.url, "devstoreaccount1", mockAccountKey, connection.pathStyle);
            Azure::Core::Url expectedUrl(connection.url);
            CPPUNIT_ASSERT_EQUAL(expectedUrl.GetAbsoluteUrl(), std::string(file->queryFilename()));
            Owned<IDirectoryIterator> directory = file->directoryFiles(nullptr, false, false);
            CPPUNIT_ASSERT(directory);
        }
        END_TEST
    }

    void testCustomHostPathStyleUrl()
    {
        START_TEST
        const char *url = "http://azurite:10000/devstoreaccount1/container/blob";
        Owned<IFile> file = createAzureBlob(url, "devstoreaccount1", mockAccountKey, true);
        Azure::Core::Url expectedUrl(url);
        CPPUNIT_ASSERT_EQUAL(expectedUrl.GetAbsoluteUrl(), std::string(file->queryFilename()));
        Owned<IDirectoryIterator> directory = file->directoryFiles(nullptr, false, false);
        CPPUNIT_ASSERT(directory);
        END_TEST
    }

    void testInvalidArguments()
    {
        START_TEST
        struct Connection
        {
            const char * url;
            const char * account;
            const char * key;
            bool pathStyle;
        };
        const Connection connections[] = {
            {nullptr, "devstoreaccount1", mockAccountKey, false},
            {"https://account.blob.core.windows.net/container/blob", "", mockAccountKey, false},
            {"https://account.blob.core.windows.net/container/blob", "account", nullptr, false},
            {"ftp://account.blob.core.windows.net/container/blob", "account", mockAccountKey, false},
            {"https://account.blob.core.windows.net/", "account", mockAccountKey, false},
            {"http://127.0.0.1:10000/other/container/blob", "devstoreaccount1", mockAccountKey, true},
            {"https://account.blob.core.windows.net/container/blob?sig=not-accepted", "account", mockAccountKey, false}
        };
        for (const auto & connection : connections)
        {
            try
            {
                Owned<IFile> file = createAzureBlob(connection.url, connection.account, connection.key, connection.pathStyle);
                CPPUNIT_FAIL("Expected an invalid Azure connection to be rejected");
            }
            catch (IException * error)
            {
                Owned<IException> ownedException(error);
            }
        }
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobUrlTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobUrlTest, "AzureBlobUrlTest");

static constexpr const char azuriteBlobEndpoint[] = "http://127.0.0.1:10000/devstoreaccount1";
static constexpr const char azuriteDevStoreKey[] = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

class CTestAzureBlobConnection : public CSimpleInterface
{
public:
    CTestAzureBlobConnection(const IPropertyTree &options, const char *suiteName)
    {
        options.getProp(VStringBuffer("%s/@url", suiteName), baseUrl);
        const bool usingDefaults = !baseUrl.length();
        if (usingDefaults)
            baseUrl.append(azuriteBlobEndpoint).append("/hpccparalleltest/azure-blob-test");

        const char *authMode = options.queryProp(VStringBuffer("%s/@auth", suiteName));
        if (isEmptyString(authMode) || strieq(authMode, "sharedKey"))
            useManagedIdentity = false;
        else if (strieq(authMode, "managedIdentity"))
            useManagedIdentity = true;
        else
            CPPUNIT_FAIL(VStringBuffer("%s auth must be sharedKey or managedIdentity", suiteName).str());

        pathStyle = options.getPropBool(VStringBuffer("%s/@pathStyle", suiteName), usingDefaults);

        options.getProp(VStringBuffer("%s/@accountName", suiteName), accountName);
        if (!accountName.length())
        {
            if (usingDefaults)
                accountName.set("devstoreaccount1");
            else
            {
                Azure::Core::Url parsedUrl(baseUrl.str());
                const std::string host = parsedUrl.GetHost();
                const size_t blobHostMarker = host.find(".blob.");
                CPPUNIT_ASSERT_MESSAGE(VStringBuffer("Set --%s.accountName for a non-standard Azure host", suiteName).str(),
                                       blobHostMarker != std::string::npos && blobHostMarker > 0);
                accountName.append(blobHostMarker, host.c_str());
            }
        }

        if (!useManagedIdentity)
        {
            accountKey = usingDefaults ? azuriteDevStoreKey : std::getenv("HPCC_AZURE_TEST_ACCOUNT_KEY");
            if (isEmptyString(accountKey))
                accountKey = std::getenv("HPCC_AZURE_WRITE_STRESS_ACCOUNT_KEY");
            CPPUNIT_ASSERT_MESSAGE("Set HPCC_AZURE_TEST_ACCOUNT_KEY for sharedKey authentication "
                                   "(HPCC_AZURE_WRITE_STRESS_ACCOUNT_KEY is also accepted)",
                                   !isEmptyString(accountKey));
        }
    }

    const char *queryBaseUrl() const { return baseUrl.str(); }

    IFile *createFile(const char *url, const StorageApiConfig &config) const
    {
        if (useManagedIdentity)
            return createAzureBlob(url, accountName.str(), config, pathStyle);
        return createAzureBlob(url, accountName.str(), accountKey, config, nullptr, pathStyle);
    }

    IFile *createFile(const char *url) const
    {
        StorageApiConfig config;
        return createFile(url, config);
    }

private:
    StringBuffer baseUrl;
    StringBuffer accountName;
    const char *accountKey = nullptr;
    bool useManagedIdentity = false;
    bool pathStyle = false;
};

/*
 * Verifies blob write/readback and directory traversal against Azure or Azurite,
 * including writes spanning multiple parallel chunks. Makes real HTTP requests.
 * Defaults to local Azurite at 127.0.0.1:10000 with its built-in development key.
 * A configured base blob URL uses shared-key or managed-identity authentication.
 * The localhost alias check runs only for a loopback Azurite URL.
 *
 * Local Azurite (start azurite-blob separately; no key environment variable needed):
 *   In a separate terminal: azurite-blob --location /tmp/hpcc-azurite-tests --blobHost 127.0.0.1 --blobPort 10000
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteExternalServiceTest
 *
 * Azure with a shared key (read without putting the key in shell history):
 *   read -rsp 'Azure storage account key: ' HPCC_AZURE_TEST_ACCOUNT_KEY; echo
 *   export HPCC_AZURE_TEST_ACCOUNT_KEY
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteExternalServiceTest \
 *       --AzureBlobWriteExternalServiceTest.url=https://<account>.blob.core.windows.net/<container>/<base-blob> \
 *       --AzureBlobWriteExternalServiceTest.auth=sharedKey
 *
 * Azure with a configured managed/workload identity:
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteExternalServiceTest \
 *       --AzureBlobWriteExternalServiceTest.url=https://<account>.blob.core.windows.net/<container>/<base-blob> \
 *       --AzureBlobWriteExternalServiceTest.auth=managedIdentity
 *
 * For custom Azurite endpoints, supply url and accountName and set
 * HPCC_AZURE_TEST_ACCOUNT_KEY to the Azurite account key; auth defaults to sharedKey.
 * Add --<suite>.pathStyle=true when a custom hostname serves a path-style URL.
 * For other non-standard hosts, also set --AzureBlobWriteExternalServiceTest.accountName.
 * The container is created if needed and retained after the tests.
 * Omit -d when the runner automatically discovers the Azure test library.
 * Excluded from the default run because an external service is required.
 */
class AzureBlobWriteExternalServiceTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobWriteExternalServiceTest);
        CPPUNIT_TEST(testWriteSizes);
        CPPUNIT_TEST(testDirectoryFiles);
    CPPUNIT_TEST_SUITE_END();

public:
    virtual void setUp() override
    {
        try
        {
            Owned<IPropertyTree> options = getComponentConfig();
            connection.setown(new CTestAzureBlobConnection(*options, "AzureBlobWriteExternalServiceTest"));
            testRunName.set("azure-blob-test-").append(getTimeStampNowValue());
            Owned<IFile> container = connection->createFile(connection->queryBaseUrl());
            container->createDirectory();
        }
        catch (IException * error)
        {
            Owned<IException> ownedException(error);
            StringBuffer message;
            error->errorMessage(message);
            CPPUNIT_FAIL(message.str());
        }
    }

    void testWriteSizes()
    {
        START_TEST
        constexpr unsigned concurrency = StorageApiConfig::defaultParallelWriteConcurrency;
        constexpr size32_t chunkBytes = static_cast<size32_t>(StorageApiConfig::defaultParallelWriteChunkBytes);
        constexpr size32_t tailBytes = 123;
        CPPUNIT_ASSERT_MESSAGE("The external-service test requires parallel writes to be enabled", concurrency > 1);
        const std::vector<size32_t> sizes = {
            1, 1023, 1024, 1025, 3 * 1024 + 7,
            concurrency * chunkBytes + tailBytes
        };
        for (size32_t size : sizes)
            verifyWriteReadback(size, chunkBytes);
        END_TEST
    }

    void testDirectoryFiles()
    {
        START_TEST
        const std::string directoryUrl = uniqueBlobName();
        const std::string blobUrl = directoryUrl + "/nested%20dir/blob%20name";
        Owned<IFile> output = connection->createFile(blobUrl.c_str());
        Owned<IFileIO> writer = output->open(IFOcreate);
        CPPUNIT_ASSERT(writer);
        const byte expected = 42;
        CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(1), writer->write(0, 1, &expected));
        writer->close();

        Azure::Core::Url listingUrl(directoryUrl);
        if (listingUrl.GetHost() == "127.0.0.1")
            listingUrl.SetHost("localhost");
        Owned<IFile> directory = connection->createFile(listingUrl.GetAbsoluteUrl().c_str());
        Owned<IDirectoryIterator> dirs = directory->directoryFiles(nullptr, false, true);
        directory.clear();
        CPPUNIT_ASSERT(dirs->first());
        CPPUNIT_ASSERT(dirs->isDir());
        Owned<IDirectoryIterator> files = dirs->query().directoryFiles(nullptr, false, false);
        dirs.clear();
        CPPUNIT_ASSERT(files->first());
        StringBuffer name;
        CPPUNIT_ASSERT(streq(files->getName(name), "blob name"));
        Owned<IFileIO> reader = files->query().open(IFOread);
        CPPUNIT_ASSERT(reader);
        byte actual = 0;
        CPPUNIT_ASSERT_EQUAL(static_cast<size32_t>(1), reader->read(0, 1, &actual));
        CPPUNIT_ASSERT_EQUAL(expected, actual);
        CPPUNIT_ASSERT(files->query().remove());
        END_TEST
    }

private:
    void verifyWriteReadback(size32_t size, size32_t chunkBytes)
    {
        const std::string name = uniqueBlobName();
        Owned<IFile> output(connection->createFile(name.c_str()));
        Owned<IFileIO> writer(output->open(IFOcreate));
        CPPUNIT_ASSERT(writer);

        std::vector<byte> buffer(std::min(size, chunkBytes));
        auto expectedByte = [size, chunkBytes](size32_t offset)
        {
            return static_cast<byte>((static_cast<unsigned __int64>(offset) * 31 + size +
                                      (offset / chunkBytes) * 37) & 0xff);
        };

        size32_t writeCount = 0;
        for (size32_t offset = 0; offset < size;)
        {
            const size32_t length = std::min<size32_t>(chunkBytes, size - offset);
            for (size32_t i = 0; i < length; ++i)
                buffer[i] = expectedByte(offset + i);
            CPPUNIT_ASSERT_EQUAL(length, writer->write(offset, length, buffer.data()));
            offset += length;
            ++writeCount;
        }
        CPPUNIT_ASSERT_EQUAL(static_cast<offset_t>(size), writer->size());
        CPPUNIT_ASSERT_EQUAL(static_cast<unsigned __int64>(writeCount), writer->getStatistic(StNumDiskWrites));
        CPPUNIT_ASSERT_EQUAL(static_cast<unsigned __int64>(size), writer->getStatistic(StSizeDiskWrite));
        writer->close();

        Owned<IFile> inputFile(connection->createFile(name.c_str()));
        Owned<IFileIO> reader(inputFile->open(IFOread));
        CPPUNIT_ASSERT(reader);
        for (size32_t offset = 0; offset < size;)
        {
            const size32_t length = std::min<size32_t>(chunkBytes, size - offset);
            CPPUNIT_ASSERT_EQUAL(length, reader->read(offset, length, buffer.data()));
            for (size32_t i = 0; i < length; ++i)
                CPPUNIT_ASSERT_EQUAL(expectedByte(offset + i), buffer[i]);
            offset += length;
        }
        CPPUNIT_ASSERT(inputFile->remove());
    }

    Owned<CTestAzureBlobConnection> connection;
    StringBuffer testRunName;
    unsigned counter = 0;

    std::string uniqueBlobName()
    {
        return std::string(connection->queryBaseUrl()) + "-" + testRunName.str() + "-" + std::to_string(++counter);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobWriteExternalServiceTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobWriteExternalServiceTest, "AzureBlobWriteExternalServiceTest");

/*
 * Measures write throughput at several concurrency levels and verifies the entire
 * blob by reading it back. Makes real requests to the configured Azure or Azurite
 * endpoint. The supplied URL is a base blob URL; each concurrency run appends a
 * unique suffix and removes its blob after readback. The base blob is not overwritten.
 * Readback uses 4 MiB chunks, with HPCC parallel read-ahead at concurrency >= 2.
 * Read timing includes open, read-ahead setup, consumption and content verification.
 * Defaults to local Azurite at 127.0.0.1:10000 with its built-in development key.
 * Excluded from the default test run because this is a stress/timing suite.
 *
 * Local Azurite (start azurite-blob separately; no key environment variable needed):
 *   In a separate terminal: azurite-blob --location /tmp/hpcc-azurite-tests --blobHost 127.0.0.1 --blobPort 10000
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteStressTimingTest \
 *       --AzureBlobWriteStressTimingTest.sizeMB=16
 *
 * Azure with a shared key (read without putting the key in shell history):
 *   read -rsp 'Azure storage account key: ' HPCC_AZURE_TEST_ACCOUNT_KEY; echo
 *   export HPCC_AZURE_TEST_ACCOUNT_KEY
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteStressTimingTest \
 *       --AzureBlobWriteStressTimingTest.url=https://<account>.blob.core.windows.net/<container>/<base-blob> \
 *       --AzureBlobWriteStressTimingTest.sizeMB=64 \
 *       --AzureBlobWriteStressTimingTest.auth=sharedKey
 *
 * Azure with a configured managed/workload identity:
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteStressTimingTest \
 *       --AzureBlobWriteStressTimingTest.url=https://<account>.blob.core.windows.net/<container>/<base-blob> \
 *       --AzureBlobWriteStressTimingTest.sizeMB=1024 \
 *       --AzureBlobWriteStressTimingTest.auth=managedIdentity
 * accountName is derived from standard Azure blob hosts; set
 * --AzureBlobWriteStressTimingTest.accountName for a custom host.
 * For custom Azurite endpoints, supply url and accountName and set
 * HPCC_AZURE_TEST_ACCOUNT_KEY to the Azurite account key; auth defaults to sharedKey.
 * The container is created if needed and retained after the test.
 * Omit -d when the runner automatically discovers the Azure test library.
 * sizeMB defaults to 1024 per concurrency level.
 * The legacy HPCC_AZURE_WRITE_STRESS_ACCOUNT_KEY variable is also accepted.
 */
class AzureBlobWriteStressTimingTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobWriteStressTimingTest);
        CPPUNIT_TEST(testWriteThroughput);
    CPPUNIT_TEST_SUITE_END();

    void testWriteThroughput()
    {
        START_TEST
        Owned<IPropertyTree> options = getComponentConfig();
        Owned<CTestAzureBlobConnection> connection = new CTestAzureBlobConnection(*options, "AzureBlobWriteStressTimingTest");

        const int sizeMB = options->getPropInt("AzureBlobWriteStressTimingTest/@sizeMB", 1024);
        CPPUNIT_ASSERT_MESSAGE("Azure stress test sizeMB must be greater than zero", sizeMB > 0);

        constexpr size32_t chunkBytes = static_cast<size32_t>(StorageApiConfig::defaultParallelWriteChunkBytes);
        const offset_t fileSize = static_cast<offset_t>(sizeMB) * 1024 * 1024;
        const offset_t maxFileSize = static_cast<offset_t>(maxAzureBlockCount) * chunkBytes;
        CPPUNIT_ASSERT_MESSAGE("Azure stress test size exceeds the maximum block-blob size", fileSize <= maxFileSize);

        Owned<IFile> container = connection->createFile(connection->queryBaseUrl());
        container->createDirectory();

        const unsigned concurrencyLevels[] = {0, 1, 2, 4, 8, 16, 32};
        const std::string runId = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        std::vector<byte> buffer(chunkBytes);
        for (size32_t i = 0; i < chunkBytes; ++i)
            buffer[i] = static_cast<byte>((static_cast<unsigned __int64>(i) * 31 + 17) & 0xff);

        for (unsigned concurrency : concurrencyLevels)
        {
            StorageApiConfig config;
            config.parallelWriteConcurrency = concurrency;
            const std::string outputUrl = std::string(connection->queryBaseUrl()) + "-stress-" + runId + "-c" + std::to_string(concurrency);
            Owned<IFile> output = connection->createFile(outputUrl.c_str(), config);

            const auto start = std::chrono::steady_clock::now();
            Owned<IFileIO> writer(output->open(IFOcreate));
            CPPUNIT_ASSERT(writer);
            for (offset_t offset = 0; offset < fileSize;)
            {
                const size32_t length = static_cast<size32_t>(std::min<offset_t>(chunkBytes, fileSize - offset));
                const unsigned block = static_cast<unsigned>(offset / chunkBytes);
                for (unsigned marker = 0; marker < sizeof(block); ++marker)
                    buffer[marker] = static_cast<byte>((block >> (marker * 8)) & 0xff);
                CPPUNIT_ASSERT_EQUAL(length, writer->write(offset, length, buffer.data()));
                offset += length;
            }
            writer->close();
            const auto finish = std::chrono::steady_clock::now();

            const double seconds = std::chrono::duration<double>(finish - start).count();
            CPPUNIT_ASSERT(seconds > 0.0);
            const double throughput = (static_cast<double>(fileSize) / (1024 * 1024)) / seconds;
            std::printf("Azure blob write: concurrency=%u sizeMiB=%d seconds=%.2f throughputMiBPerSecond=%.2f\n",
                        concurrency, sizeMB, seconds, throughput);
            std::fflush(stdout);

            {
                const auto readStart = std::chrono::steady_clock::now();
                Owned<IFileIO> reader(output->open(IFOread));
                CPPUNIT_ASSERT(reader);
                CPPUNIT_ASSERT_EQUAL(fileSize, reader->size());
                Owned<ISerialInputStream> input;
                if (concurrency >= 2)
                    input.setown(createParallelReadAheadInputStream(reader, concurrency, chunkBytes, chunkBytes));
                else
                    input.setown(createSerialInputStream(reader));
                std::vector<byte> actual(chunkBytes);
                for (offset_t offset = 0; offset < fileSize;)
                {
                    const size32_t length = static_cast<size32_t>(std::min<offset_t>(chunkBytes, fileSize - offset));
                    const size32_t bytesRead = input->read(length, actual.data());
                    CPPUNIT_ASSERT_EQUAL(length, bytesRead);
                    for (size32_t i = 0; i < length; ++i)
                    {
                        const offset_t position = offset + i;
                        const unsigned block = static_cast<unsigned>(position / chunkBytes);
                        const size32_t withinBlock = static_cast<size32_t>(position % chunkBytes);
                        buffer[i] = withinBlock < sizeof(block)
                            ? static_cast<byte>((block >> (withinBlock * 8)) & 0xff)
                            : static_cast<byte>((static_cast<unsigned __int64>(withinBlock) * 31 + 17) & 0xff);
                    }
                    CPPUNIT_ASSERT(std::equal(buffer.begin(), buffer.begin() + length, actual.begin()));
                    offset += length;
                }
                const auto readFinish = std::chrono::steady_clock::now();
                const double readSeconds = std::chrono::duration<double>(readFinish - readStart).count();
                CPPUNIT_ASSERT(readSeconds > 0.0);
                const double readThroughput = (static_cast<double>(fileSize) / (1024 * 1024)) / readSeconds;
                std::printf("Azure blob read: concurrency=%u chunkMiB=%u sizeMiB=%d seconds=%.2f throughputMiBPerSecond=%.2f\n",
                            concurrency, chunkBytes / (1024 * 1024), sizeMB, readSeconds, readThroughput);
                std::fflush(stdout);
                input.clear();
                CPPUNIT_ASSERT_EQUAL(static_cast<unsigned __int64>((fileSize + chunkBytes - 1) / chunkBytes),
                                     reader->getStatistic(StNumDiskReads));
                CPPUNIT_ASSERT_EQUAL(static_cast<unsigned __int64>(fileSize), reader->getStatistic(StSizeDiskRead));
            }
            CPPUNIT_ASSERT_MESSAGE("Failed to remove Azure stress-test blob", output->remove());
        }
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobWriteStressTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobWriteStressTimingTest, "AzureBlobWriteStressTimingTest");
