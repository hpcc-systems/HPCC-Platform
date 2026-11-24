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

#include "platform.h"
#include "jlib.hpp"
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jregexp.hpp"
#include "jstring.hpp"
#include "jsecrets.hpp"
#include "jplane.hpp"
#include "jsocket.hpp"
#include "azureblob.hpp"
#include "azureapiutils.hpp"

#ifdef _USE_CPPUNIT
#include "jptree.hpp"
#include "jstatcodes.h"
#include "unittests.hpp"
#endif

#include <azure/core/base64.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/identity.hpp>
#include <algorithm>
#include <deque>
#include <exception>
#include <limits>
#include <map>
#include <optional>
#include <vector>

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;
using namespace std::chrono;

/*
 * Azure related comments
 *
 * Does it make more sense to create input and output streams directly from the IFile rather than going via
 * an IFileIO.  E.g., append blobs
 * The overhead is trivial, so leave as it is for now.
 */

//---------------------------------------------------------------------------------------------------------------------

using SharedBlobClient = std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient>;

class AzureBlob;

std::string makeAzureBlockId(const std::string &baseBlockId, unsigned sequence)
{
    if (sequence == 0 || sequence > maxAzureBlockCount)
        throw makeStringException(azureWriterError, "Too many blocks for Azure block blob");

    return VStringBuffer("%s%08u", baseBlockId.c_str(), sequence).str();
}

class AzureBlobBlockOperations final : public CInterfaceOf<IAzureBlockOperations>
{
public:
    AzureBlobBlockOperations(SharedBlobClient _client, const char *_baseBlockId)
        : client(std::move(_client)), baseBlockId(_baseBlockId) {}

    virtual void initializeEmptyBlob() override
    {
        Azure::Core::IO::MemoryBodyStream empty(nullptr, 0);
        client->Upload(empty);
    }

    virtual void stageBlock(unsigned sequence, const byte *data, size32_t length) override
    {
        std::string blockId = makeAzureBlockId(baseBlockId, sequence);
        Azure::Core::IO::MemoryBodyStream content(reinterpret_cast<const uint8_t *>(data), length);
        client->StageBlock(blockId, content);
    }

    virtual void commitBlockList(unsigned count) override
    {
        std::vector<std::string> blockIds;
        blockIds.reserve(count);
        for (unsigned sequence = 1; sequence <= count; ++sequence)
            blockIds.emplace_back(makeAzureBlockId(baseBlockId, sequence));

        client->CommitBlockList(blockIds);
    }

private:
    SharedBlobClient client;
    std::string baseBlockId;
};

//The base class for AzureBlobIO.  This class performs NO caching of the data - to avoid problems with
//copying the data too many times.  It is the responsibility of the caller to implement a cache if necessary.
class AzureBlobIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureBlobIO(AzureBlob * _file);

    virtual void close() override
    {
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;
    virtual IFile * queryFile() const;

protected:
    Linked<AzureBlob> file;
    FileIOStats stats;
    SharedBlobClient blockBlobClient;
};


class AzureBlobReadIO final : public AzureBlobIO
{
public:
    AzureBlobReadIO(AzureBlob * _file, offset_t _fileSize);

    virtual offset_t size() override;
    virtual size32_t read(offset_t pos, size32_t len, void * data) override;

    // Write methods not implemented - this is a read-only file
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        throwUnexpectedX("Writing to read only file");
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpectedX("Setting size of read only azure file");
    }
    virtual void flush() override
    {
    }

private:
    offset_t cachedFileSize = unknownFileSize;
};


class AzureBlobWriteIO : public AzureBlobIO
{
public:
    AzureBlobWriteIO(AzureBlob * _file);

    virtual void beforeDispose() override;
    virtual unsigned __int64 getStatistic(StatisticKind kind) override;
    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        throwUnexpectedX("Reading from write only file");
    }
    virtual offset_t size() override;
    virtual void setSize(offset_t size) override;
    virtual void flush() override;

protected:
    // Checks shared by every non-empty write(); also invalidates the file's cached metadata.
    void prepareWrite(offset_t pos, const void *data);
    // Records the IFileIO statistics for one completed write() call.
    void recordWrite(size32_t len, cycle_t cycles);
    // Wrap the block operations to record statistics and convert Azure exceptions to IException.
    void initializeBlob();
    void stageBlock(unsigned sequence, offset_t pos, const byte *data, size32_t length);
    void commitBlockList(unsigned count);
    StringBuffer baseBlockId;
    Owned<IAzureBlockOperations> operations;
    offset_t offset = 0;
    unsigned lastSequence = 0; // Caller-owned block sequence used to stage and commit the write.
    // The parallel writer protects this shared error state with stateCs while workers are active.
    Owned<IException> firstError;
    bool closed = false; // Set once close() has committed the block list.
    // Relaxed atomics: each statistic is accurate, but they may be read at slightly different moments.
    RelaxedAtomic<cycle_t> initializeCycles{0};
    RelaxedAtomic<cycle_t> stageCycles{0};
    RelaxedAtomic<cycle_t> commitCycles{0};
    RelaxedAtomic<unsigned> activeStages{0};
    RelaxedAtomic<unsigned> peakStages{0};
};

//---------------------------------------------------------------------------------------------------------------------
// Directory iterator for Azure Blob Storage using ListBlobsByHierarchy API.
// Uses "/" as a delimiter to simulate directory listing, returning both blobs
// (files) and blob prefixes (virtual directories) at the current level.

class AzureBlobDirectoryIterator final : public AzureDirectoryIteratorBase
{
public:
    AzureBlobDirectoryIterator(AzureBlob &_file, const char *_mask, bool _includeDirs);

protected:
    virtual IFile *createFile(const char *fullPath, const DirEntry &entry) override;
    virtual void resetPaging() override { pagedResponse.reset(); }

    virtual void fetchPage() override
    {
        if (!pagedResponse)
        {
            ListBlobsOptions options;
            if (!prefix.empty())
                options.Prefix = prefix;
            pagedResponse.emplace(containerClient->ListBlobsByHierarchy("/", options));
        }
        else
        {
            pagedResponse->MoveToNextPage();
        }
        extractItems(*pagedResponse);
        hasMorePages = pagedResponse->NextPageToken.HasValue() && !pagedResponse->NextPageToken.Value().empty();
    }

private:
    void extractItems(const ListBlobsByHierarchyPagedResponse &response)
    {
        if (includeDirs)
        {
            for (const auto &dp : response.BlobPrefixes)
            {
                std::string tail = stripPrefix(dp);
                if (!tail.empty() && tail.back() == '/')
                    tail.pop_back();
                if (!tail.empty() && matchesMask(tail.c_str()))
                    items.push_back({std::move(tail), true, -1, 0});
            }
        }
        for (const auto &blob : response.Blobs)
        {
            std::string tail = stripPrefix(blob.Name);
            if (!tail.empty() && tail.find('/') == std::string::npos && matchesMask(tail.c_str()))
                items.push_back({std::move(tail), false, blob.BlobSize, toTimeT(blob.Details.LastModified)});
        }
    }

    std::string stripPrefix(const std::string &name) const
    {
        if (!prefix.empty() && name.compare(0, prefix.size(), prefix) == 0)
            return name.substr(prefix.size());
        return name;
    }

    Linked<AzureBlob> file;
    std::shared_ptr<BlobContainerClient> containerClient;
    std::string prefix;
    std::optional<ListBlobsByHierarchyPagedResponse> pagedResponse;
};


class AzureBlob final : implements CInterfaceOf<IFile>
{
    friend class AzureBlobDirectoryIterator;
#ifdef _USE_CPPUNIT
    friend class AzureBlobReadAndMetadataTest;
    friend class AzureBlobUrlStyleTest;
    AzureBlob(SharedBlobClient _client) : cachedBlobClient(std::move(_client)) {}
#endif
public:
    AzureBlob(const char *_azureFileName);
    AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
              bool _pathStyleUrl);
    AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
              const StorageApiConfig &_config, IAzureBlockOperations *_blockOperations,
              bool _pathStyleUrl);
    AzureBlob(const char *_blobUrl, const char *_accountName, bool _pathStyleUrl);
    AzureBlob(const char *_blobUrl, const char *_accountName, const StorageApiConfig &_config,
              bool _pathStyleUrl);
    AzureBlob(const char *_azureFileName, const DirEntry &entry)
    : AzureBlob(_azureFileName) { setListedInfo(entry.isDir, entry.size, entry.modifiedTime); }
    virtual bool exists() override
    {
        ensureMetaData();
        return fileExists;
    }
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime) override;
    virtual fileBool isDirectory() override
    {
        // A blob name ending with '/' is a virtual directory prefix in Azure Blob Storage.
        // No real blob exists at this path, so GetProperties would return 404. Treat it as a directory.
        if (!blobName.isEmpty() && blobName.str()[blobName.length()-1] == '/')
            return fileBool::foundYes;
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isFile() override
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return !isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isReadOnly() override
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return fileBool::foundYes;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone) override
    {
        //Should this be derived from a common base that implements the setShareMode()?
        return openShared(mode,IFSHread,extraFlags);
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone) override
    {
        if (mode == IFOcreate)
            return createFileWriteIO();
        assertex(mode==IFOread);
        return createFileReadIO();
    }
    virtual const char * queryFilename() override
    {
        return fullName.str();
    }
    virtual offset_t size() override
    {
        ensureMetaData();
        return fileSize;
    }

// Directory functions
    virtual IDirectoryIterator *directoryFiles(const char *mask, bool sub, bool includeDirs) override;
    virtual bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime) override
    {
        ensureMetaData();
        isdir = isDir;
        size = fileSize;
        modtime.clear();
        return true;
    }

    // Not going to be implemented - this IFile interface is too big..
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) override
    {
        DBGLOG("AzureBlob::setTime ignored");
        return false;
    }
    virtual bool remove() override;
    virtual void rename(const char *newTail) override { UNIMPLEMENTED_X("AzureBlob::rename"); }
    virtual void move(const char *newName) override { UNIMPLEMENTED_X("AzureBlob::move"); }
    virtual void setReadOnly(bool ro) override { UNIMPLEMENTED_X("AzureBlob::setReadOnly"); }
    virtual void setFilePermissions(unsigned fPerms) override
    {
        DBGLOG("AzureBlob::setFilePermissions() ignored");
    }
    virtual bool setCompression(bool set) override { UNIMPLEMENTED_X("AzureBlob::setCompression"); }
    virtual offset_t compressedSize() override { UNIMPLEMENTED_X("AzureBlob::compressedSize"); }
    virtual unsigned getCRC() override { UNIMPLEMENTED_X("AzureBlob::getCRC"); }
    virtual void setCreateFlags(unsigned short cflags) override { UNIMPLEMENTED_X("AzureBlob::setCreateFlags"); }
    virtual void setShareMode(IFSHmode shmode) override { UNIMPLEMENTED_X("AzureBlob::setSharedMode"); }
    virtual bool createDirectory() override;
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) override { UNIMPLEMENTED_X("AzureBlob::monitorDirectory"); }
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureBlob::copySection"); }
    virtual void copyTo(IFile *dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress *progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureBlob::copyTo"); }
    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false) override { UNIMPLEMENTED_X("AzureBlob::openMemoryMapped"); }

public:
    SharedBlobClient getBlobClient() const;
    void invalidateMeta() { haveMeta = false; }
    const StorageApiConfig & queryConfig() const { return config; }
    IAzureBlockOperations * createBlockOperations(const char *baseBlockId) const;

protected:
    // Seed metadata from a directory listing during construction only.
    void setListedInfo(bool _isDir, offset_t _fileSize, time_t _lastModified)
    {
        haveMeta = true;
        isDir = _isDir;
        fileExists = true;
        fileSize = _fileSize;
        lastModified = _lastModified;
        createdOn = 0;
    }
    std::shared_ptr<StorageSharedKeyCredential> getSharedKeyCredentials() const;
    std::string getBlobUrl() const;
    BlobClientOptions makeClientOptions() const;
    std::shared_ptr<BlobContainerClient> getBlobContainerClient() const;

    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileReadIO();
    IFileIO * createFileWriteIO();
    void setProperties(int64_t _blobSize, Azure::DateTime _lastModified, Azure::DateTime _createdOn);

protected:
    StringBuffer fullName;
    StringBuffer accountName;
    StringBuffer containerName;
    StringBuffer secretName;
    StringAttr blobName;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    bool useManagedIdentity = false;
    time_t lastModified = 0;
    time_t createdOn = 0;
    std::string blobUrl;
    bool pathStyleUrl = false;
    mutable CriticalSection cacheCS{SYNC_LOCATION};
    std::string blobContainerUrl;
    std::shared_ptr<StorageSharedKeyCredential> explicitSharedKeyCredential;
    mutable std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> cachedBlobClient;  // Cache client for reuse per-file
    StorageApiConfig config;
    Linked<IAzureBlockOperations> blockOperations;

private:
    AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
              const char *_accountName, bool _useManagedIdentity,
              const StorageApiConfig *_configOverride, IAzureBlockOperations *_blockOperations,
              bool _pathStyleUrl);
};


//---------------------------------------------------------------------------------------------------------------------

// Sequential block-blob writer. IFileIO calls must be serialized by the caller.
// Each non-empty write must append at the current offset and is staged synchronously
// as one block directly from the caller's buffer; blocks may have different sizes,
// subject to Azure's per-block size limit. Empty writes do nothing. Staged blocks
// aren't visible in the blob until close() commits their ordered list; flush() does
// not commit. The writer is limited to 50,000 blocks, close() is idempotent, and
// writes after close() throw.
class AzureBlobBlockBlobWriteIO final : public AzureBlobWriteIO
{
public:
    explicit AzureBlobBlockBlobWriteIO(AzureBlob *_file) : AzureBlobWriteIO(_file) {}

    virtual size32_t write(offset_t pos, size32_t len, const void *data) override
    {
        if (!len)
            return 0;
        if (firstError)
            throw firstError.getLink();
        prepareWrite(pos, data);

        CCycleTimer timer;
        const unsigned sequence = lastSequence + 1;
        try
        {
            stageBlock(sequence, pos, static_cast<const byte *>(data), len);
        }
        catch (IException *e)
        {
            firstError.setown(e);
            throw firstError.getLink();
        }
        lastSequence = sequence;
        offset += len;

        recordWrite(len, timer.elapsedCycles());
        return len;
    }

    virtual void close() override
    {
        if (closed)
            return;
        if (firstError)
            throw firstError.getLink();
        COnScopeExit invalidateAfterCommit([this] { file->invalidateMeta(); });
        commitBlockList(lastSequence);
        closed = true;
    }
};

// Runs fn and returns any exception it throws as an IException (or nullptr), so that
// worker threads and close() can record failures instead of propagating them.
template <typename F>
static IException *runNoThrow(const char *what, F &&fn)
{
    try
    {
        fn();
    }
    catch (IException *e)
    {
        return e;
    }
    catch (const std::exception &e)
    {
        return makeStringExceptionV(azureRequestError, "Azure %s failed: %s", what, e.what());
    }
    catch (...)
    {
        return makeStringExceptionV(azureRequestError, "Azure %s failed with an unknown exception", what);
    }
    return nullptr;
}

// Parallel block-blob writer. IFileIO calls must be serialized by the caller.
// write() copies data into fixed-size chunk buffers ("slots") and queues each full
// chunk. Up to concurrency worker threads stage queued chunks while the caller fills
// one more slot, so there are concurrency + 1 slots. Workers and slots are created on
// demand. close() stages the final partial chunk, waits for all uploads to finish and
// then commits the complete block list. Upload errors are reported by the next write()
// that completes a chunk, or by close().
class AzureBlobParallelBlockBlobWriteIO final : public AzureBlobWriteIO
{
public:
    explicit AzureBlobParallelBlockBlobWriteIO(AzureBlob *_file)
        : AzureBlobWriteIO(_file),
          concurrency(_file->queryConfig().parallelWriteConcurrency),
          slotCount(concurrency + 1),
          chunkBytes(static_cast<size32_t>(_file->queryConfig().parallelWriteChunkBytes)),
          buffers(slotCount)
    {
        freeSlots.reserve(slotCount);
        workers.reserve(concurrency);
    }

    virtual ~AzureBlobParallelBlockBlobWriteIO()
    {
        stopWorkers();
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        switch (kind)
        {
        case StTimeBlobWriteWait:
            return cycle_to_nanosec(slotWaitCycles.load());
        case StCycleBlobWriteWaitCycles:
            return slotWaitCycles.load();
        }
        return AzureBlobWriteIO::getStatistic(kind);
    }

    virtual size32_t write(offset_t pos, size32_t len, const void *data) override
    {
        if (!len)
            return 0;
        prepareWrite(pos, data);

        CCycleTimer timer;
        const byte *input = static_cast<const byte *>(data);
        size32_t remaining = len;
        while (remaining)
        {
            if (currentSlot == noSlot)
            {
                IException *acquireError = runNoThrow("parallel writer buffer acquisition", [this] { acquireSlot(); });
                CriticalBlock block(stateCs);
                recordFailureIfPresent(acquireError);
                if (firstError)
                    throw firstError.getLink();
            }
            size32_t amount = std::min<size32_t>(remaining, chunkBytes - currentLength);
            memcpy(buffers[currentSlot].bytes() + currentLength, input, amount);
            currentLength += amount;
            offset += amount;
            input += amount;
            remaining -= amount;
            if (currentLength == chunkBytes)
            {
                IException *queueError = runNoThrow("parallel writer queue", [this] { enqueueFullChunk(); });
                CriticalBlock block(stateCs);
                recordFailureIfPresent(queueError);
                if (firstError)
                    throw firstError.getLink();
            }
        }

        recordWrite(len, timer.elapsedCycles());
        return len;
    }

    virtual void close() override
    {
        if (closed)
            return;

        CCycleTimer timer;
        COnScopeExit recordTime([&] { stats.ioWriteCycles.fastAdd(timer.elapsedCycles()); });

        if (currentLength)
        {
            IException *stageError = runNoThrow("final block stage", [this]
            {
                // Holding a spare slot reserves one of the concurrency uploads for the final block
                // (see takeSpareSlot). The slot itself is never used, since nothing more is queued.
                takeSpareSlot();
                stageSlot(assignSequence(), currentSlot, currentLength);
            });
            CriticalBlock block(stateCs);
            recordFailureIfPresent(stageError);
        }
        stopWorkers();
        // All workers have joined, so firstError no longer needs stateCs.
        if (!firstError)
        {
            COnScopeExit invalidateAfterCommit([this] { file->invalidateMeta(); });
            firstError.setown(runNoThrow("final block-list commit", [this] { commitBlockList(lastSequence); }));
        }
        buffers.clear();
        currentSlot = noSlot;
        currentLength = 0;
        if (firstError)
            throw firstError.getLink();
        closed = true;
    }

private:
    class AzureBlobParallelWorker final : public Thread
    {
    public:
        explicit AzureBlobParallelWorker(AzureBlobParallelBlockBlobWriteIO &_owner)
            : Thread("AzureBlobParallelWriter"), owner(_owner) {}

        virtual int run() override
        {
            owner.workerLoop();
            return 0;
        }
    private:
        AzureBlobParallelBlockBlobWriteIO &owner;
    };

    // Every block except the last is full, so block n starts at file offset (n - 1) * chunkBytes.
    void stageSlot(unsigned sequence, unsigned slot, size32_t length)
    {
        const offset_t pos = static_cast<offset_t>(sequence - 1) * chunkBytes;
        stageBlock(sequence, pos, buffers[slot].bytes(), length);
    }

    void workerLoop()
    {
        for (;;)
        {
            workerWake.wait();
            unsigned sequence;
            unsigned slot;
            {
                CriticalBlock block(stateCs);
                if (firstError)
                    return;
                if (readySlots.empty())
                {
                    if (stopping)
                        return;
                    // Defensive: Semaphore::wait() can return without a matching signal (e.g. sem_wait EINTR).
                    continue;
                }
                // Sequence follows FIFO dequeue order, not upload completion order.
                sequence = nextStageSequence++;
                slot = readySlots.front();
                readySlots.pop_front();
            }

            IException *stageError = runNoThrow("block stage", [this, sequence, slot] { stageSlot(sequence, slot, chunkBytes); });
            {
                CriticalBlock block(stateCs);
                recordFailureIfPresent(stageError);
                freeSlots.emplace_back(slot);
                signalProducer();
            }
        }
    }

    // Records the first failure and stops further staging; later failures are only logged.
    // Must be called with stateCs held. Ignores null, and a rethrown firstError.
    void recordFailureIfPresent(IException *error)
    {
        if (!error)
            return;
        if (error == firstError.get())
        {
            error->Release();
            return;
        }
        if (!firstError)
            firstError.setown(error);
        else
        {
            StringBuffer message;
            error->errorMessage(message);
            WARNLOG("Additional Azure parallel writer error: %s", message.str());
            error->Release();
        }
        readySlots.clear();
        signalProducer();
    }

    void signalProducer()
    {
        if (producerWaiting)
        {
            producerWaiting = false;
            producerWake.signal();
        }
    }

    void enqueueFullChunk()
    {
        dbgassertex(currentSlot != noSlot && currentLength == chunkBytes);
        bool startWorker;
        {
            CriticalBlock block(stateCs);
            if (firstError)
                return;
            assignSequence();
            readySlots.push_back(currentSlot);
            startWorker = workers.size() < concurrency && workers.size() < allocatedSlots - freeSlots.size();
        }
        // The slot now belongs to the workers.
        currentSlot = noSlot;
        currentLength = 0;
        if (startWorker)
        {
            Owned<AzureBlobParallelWorker> worker = new AzureBlobParallelWorker(*this);
            worker->start(true);
            workers.emplace_back(worker.getClear());
        }
        workerWake.signal();
    }

    unsigned assignSequence()
    {
        if (lastSequence >= maxAzureBlockCount)
        {
            const unsigned __int64 maxBlobBytes = static_cast<unsigned __int64>(maxAzureBlockCount) * chunkBytes;
            throw makeStringExceptionV(azureWriterError, "Azure block blob exceeds %llu bytes at configured chunk size %u", maxBlobBytes, chunkBytes);
        }
        return ++lastSequence;
    }

    // Waits until some slot other than the caller's is spare: free, or not yet allocated.
    // The caller's slot aside, the other concurrency slots are free, queued or uploading,
    // so a spare slot means fewer than concurrency chunks are queued or uploading.
    // Takes and returns a free slot if there is one, otherwise returns noSlot and the
    // caller may allocate a new slot (only the caller allocates, so that stays possible).
    unsigned takeSpareSlot()
    {
        for (;;)
        {
            {
                CriticalBlock block(stateCs);
                if (firstError)
                    throw firstError.getLink();
                if (!freeSlots.empty())
                {
                    unsigned slot = freeSlots.back();
                    freeSlots.pop_back();
                    return slot;
                }
                if (allocatedSlots < slotCount)
                    return noSlot; // None free, but the caller can allocate a new one.
                producerWaiting = true;
            }
            CCycleTimer timer;
            producerWake.wait();
            slotWaitCycles.fastAdd(timer.elapsedCycles());
        }
    }

    // Sets currentSlot to a free or newly allocated slot, waiting for a worker to free one if necessary.
    void acquireSlot()
    {
        dbgassertex(currentSlot == noSlot && currentLength == 0);
        currentSlot = takeSpareSlot();
        if (currentSlot == noSlot) // None free, so allocate a new one.
        {
            buffers[allocatedSlots].allocate(chunkBytes);
            currentSlot = allocatedSlots++;
        }
    }

    void stopWorkers()
    {
        {
            CriticalBlock block(stateCs);
            stopping = true;
        }
        if (!workers.empty())
            workerWake.signal(workers.size());
        for (auto &worker : workers)
            worker->join();
        workers.clear();
    }

    // Marks the absence of a slot, e.g. when the caller isn't currently filling one.
    static constexpr unsigned noSlot = std::numeric_limits<unsigned>::max();

    // Fixed at construction.
    const unsigned concurrency;
    const unsigned slotCount;
    const size32_t chunkBytes;

    // Used only by the caller's thread (write() and close()). Workers read the buffer of a
    // slot they have dequeued, but only the caller allocates buffers or starts workers.
    std::vector<MemoryAttr> buffers;
    std::vector<Owned<AzureBlobParallelWorker>> workers;
    unsigned allocatedSlots = 0;
    unsigned currentSlot = noSlot; // Slot the caller is filling, or noSlot if it holds none.
    size32_t currentLength = 0;

    // Shared between the caller and the workers, guarded by stateCs.
    CriticalSection stateCs{SYNC_LOCATION};
    std::vector<unsigned> freeSlots;
    std::deque<unsigned> readySlots;
    unsigned nextStageSequence = 1;
    bool producerWaiting = false;
    bool stopping = false;

    // Thread-safe without stateCs.
    Semaphore workerWake{SYNC_LOCATION};
    Semaphore producerWake{SYNC_LOCATION};
    RelaxedAtomic<cycle_t> slotWaitCycles{0};
};


//---------------------------------------------------------------------------------------------------------------------

AzureBlobIO::AzureBlobIO(AzureBlob * _file) : file(_file), blockBlobClient(file->getBlobClient())
{
}


unsigned __int64 AzureBlobIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

IFile * AzureBlobIO::queryFile() const
{
    return file;
}


AzureBlobReadIO::AzureBlobReadIO(AzureBlob * _file, offset_t _fileSize)
: AzureBlobIO(_file), cachedFileSize(_fileSize)
{
}

size32_t AzureBlobReadIO::read(offset_t pos, size32_t len, void * data)
{
    if (!len)
        return 0;

    CCycleTimer timer;
    if (pos >= cachedFileSize)
        return 0;

    if (pos + len > cachedFileSize)
        len = cachedFileSize - pos;

    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = pos;
    options.Range.Value().Length = len;

    // Configure parallel transfer options based on global configuration
    const StorageApiConfig &config = file->queryConfig();
    if (len >= config.parallelThresholdBytes)
    {
        // Use parallel transfers for larger requests
        options.TransferOptions.Concurrency = config.parallelConcurrency;
        options.TransferOptions.ChunkSize = config.parallelChunkBytes;
        options.TransferOptions.InitialChunkSize = config.parallelInitialChunkBytes;
    }
    else
    {
        // For smaller reads, use single-threaded approach to reduce overhead
        options.TransferOptions.Concurrency = 1;
        options.TransferOptions.ChunkSize = len;  // Download in single chunk
        options.TransferOptions.InitialChunkSize = len;
    }

    uint8_t * buffer = reinterpret_cast<uint8_t*>(data);
    long int sizeRead = 0;

    try
    {
        Azure::Response<Models::DownloadBlobToResult> result = blockBlobClient->DownloadTo(buffer, len, options);
        // DownloadTo can return a shorter range if the blob shrank since its size was cached.
        Azure::Core::Http::HttpRange range = result.Value.ContentRange;
        if (range.Length.HasValue() && range.Length.Value() != len)
            throw makeStringExceptionV(azureRequestError, "AzureBlob::read returned an unexpected length for file %s at offset %llu: requested %u, returned %llu",
                                       file->queryFilename(), pos, len, static_cast<unsigned long long>(range.Length.Value()));
        sizeRead = len;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::read", file->queryFilename(), pos, len);
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::read", file->queryFilename(), pos, len);
    }

    // IFileIO reads can run concurrently, e.g. via createParallelReadAheadInputStream, so updates must be atomic.
    stats.ioReads.add(1);
    stats.ioReadCycles.add(timer.elapsedCycles());
    stats.ioReadBytes.add(sizeRead);

    if (file->queryConfig().traceEnabled)
    {
        unsigned elapsed = timer.elapsedMs();
        DBGLOG("Azure read: pos=%llu, len=%u, read=%ld, time=%ums, throughput=%.2f MB/s",
            pos, len, sizeRead, elapsed,
            elapsed > 0 ? (sizeRead / (1024.0 * 1024.0)) / (elapsed / 1000.0) : 0.0);
    }

    return sizeRead;
}

offset_t AzureBlobReadIO::size()
{
    return cachedFileSize;
}

//---------------------------------------------------------------------------------------------------------------------

AzureBlobWriteIO::AzureBlobWriteIO(AzureBlob * _file)
: AzureBlobIO(_file)
{
    file->invalidateMeta();

    MemoryBuffer blockId;
    blockId.append(hashncz_fnv1a((const byte *)file->queryFilename(), fnvInitialHash32));
    blockId.append(getTimeStampNowValue());
    blockId.append(hashvalue_fnv1a(get_cycles_now(), fnvInitialHash32));
    blockId.append('_').append('_');
    dbgassertex(blockId.length() % 3 == 0);
    JBASE64_Encode(blockId.bytes(), blockId.length(), baseBlockId, false);

    operations.setown(file->createBlockOperations(baseBlockId.str()));
    initializeBlob();
}

unsigned __int64 AzureBlobWriteIO::getStatistic(StatisticKind kind)
{
    switch (kind)
    {
    case StTimeBlobWriteInitialize:
        return cycle_to_nanosec(initializeCycles.load());
    case StCycleBlobWriteInitializeCycles:
        return initializeCycles.load();
    case StTimeBlobWriteStage:
        return cycle_to_nanosec(stageCycles.load());
    case StCycleBlobWriteStageCycles:
        return stageCycles.load();
    case StTimeBlobWriteCommit:
        return cycle_to_nanosec(commitCycles.load());
    case StCycleBlobWriteCommitCycles:
        return commitCycles.load();
    case StNumBlobWritePeakConcurrency:
        return peakStages.load();
    }
    return AzureBlobIO::getStatistic(kind);
}

void AzureBlobWriteIO::initializeBlob()
{
    CCycleTimer timer;
    COnScopeExit recordTime([&] { initializeCycles.fastAdd(timer.elapsedCycles()); });
    try
    {
        operations->initializeEmptyBlob();
    }
    catch (const Azure::Core::RequestFailedException &e)
    {
        throwRequestException(e, "AzureBlob::create", file->queryFilename());
    }
    catch (const std::exception &e)
    {
        throwRequestException(e, "AzureBlob::create", file->queryFilename());
    }
}

void AzureBlobWriteIO::stageBlock(unsigned sequence, offset_t pos, const byte *data, size32_t length)
{
    // Workers stage concurrently, so these updates must be atomic (add, not fastAdd).
    peakStages.store_max(++activeStages);
    CCycleTimer timer;
    COnScopeExit recordTime([&]
    {
        stageCycles.add(timer.elapsedCycles());
        --activeStages;
    });
    try
    {
        operations->stageBlock(sequence, data, length);
    }
    catch (const Azure::Core::RequestFailedException &e)
    {
        throwRequestException(e, "AzureBlob::write", file->queryFilename(), pos, length);
    }
    catch (const std::exception &e)
    {
        throwRequestException(e, "AzureBlob::write", file->queryFilename(), pos, length);
    }
}

void AzureBlobWriteIO::commitBlockList(unsigned count)
{
    CCycleTimer timer;
    COnScopeExit recordTime([&] { commitCycles.fastAdd(timer.elapsedCycles()); });
    try
    {
        operations->commitBlockList(count);
    }
    catch (const Azure::Core::RequestFailedException &e)
    {
        throwRequestException(e, "AzureBlob::close", file->queryFilename());
    }
    catch (const std::exception &e)
    {
        throwRequestException(e, "AzureBlob::close", file->queryFilename());
    }
}

void AzureBlobWriteIO::beforeDispose()
{
    try
    {
        close();
    }
    catch (IException *e)
    {
        StringBuffer message;
        e->errorMessage(message);
        WARNLOG("Azure blob writer close failed during disposal: %s", message.str());
        e->Release();
    }
    catch (...)
    {
        WARNLOG("Azure blob writer close failed during disposal with an unknown exception");
    }
}

offset_t AzureBlobWriteIO::size()
{
    return offset;
}

void AzureBlobWriteIO::setSize(offset_t size)
{
    UNIMPLEMENTED;
}

void AzureBlobWriteIO::flush()
{
    // Only close() commits the complete block list and makes uploaded data readable.
    // Flushing neither publishes partial contents nor waits for background staging.
}

void AzureBlobWriteIO::prepareWrite(offset_t pos, const void *data)
{
    if (closed)
        throw makeStringException(azureWriterError, "Azure blob writer is already closed");
    if (unlikely(pos != offset))
        throw makeStringExceptionV(azureWriterError, "Azure Blobs only support appending writes, unexpected write position %llu, expected %llu", pos, offset);
    if (!data)
        throw makeStringException(azureWriterError, "Azure blob writer received null data for a non-empty write");
    file->invalidateMeta();
}

void AzureBlobWriteIO::recordWrite(size32_t len, cycle_t cycles)
{
    stats.ioWrites.fastAdd(1);
    stats.ioWriteBytes.fastAdd(len);
    stats.ioWriteCycles.fastAdd(cycles);
}

//---------------------------------------------------------------------------------------------------------------------

static std::string getContainerUrl(const char *account, const char * container)
{
    std::string url("https://");
    return url.append(account).append(".blob.core.windows.net/").append(container);
}

static std::string getBlobUrl(const char *account, const char * container, const char *blob)
{
    std::string url(getContainerUrl(account, container));
    return url.append("/").append(blob);
}

AzureBlob::AzureBlob(const char *_azureFileName) : fullName(_azureFileName)
{
    if (startsWith(fullName, azureBlobPrefix))
    {
        //format is azureblob:plane[/device]/path
        const char * filename = fullName + strlen(azureBlobPrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            throw makeStringException(azureConfigError, "Missing / in azureblob: file reference");

        StringBuffer planeName(slash-filename, filename);
        Owned<const IStoragePlane> plane = getStoragePlaneByName(planeName, true);
        Owned<IStorageApiInfo> apiInfo = plane->getStorageApiInfo();
        if (!apiInfo)
            throw makeStringExceptionV(azureConfigError, "No storage api defined for plane %s", planeName.str());
        config = apiInfo->queryAPIConfig();
        filename = slash+1; // advance past slash

        const char * api = apiInfo->getStorageType();
        if (!api)
            throw makeStringExceptionV(azureConfigError, "No storage api defined for plane %s", planeName.str());

        StringBuffer azureBlobAPI(strlen(azureBlobPrefix) - 1, azureBlobPrefix);
        if (!strieq(api, azureBlobAPI.str()))
            throw makeStringExceptionV(azureConfigError, "Storage api for plane %s is not azureblob", planeName.str());

        useManagedIdentity = apiInfo->useManagedIdentity();
        //MORE: We could allow the managed identity/secret to be supplied in the configuration
        if (useManagedIdentity && !areManagedIdentitiesEnabled())
            throw makeStringExceptionV(azureConfigError, "Managed identity is not enabled for this environment");

        unsigned numDevices = plane->numDevices();
        unsigned device = 1;
        if (numDevices != 1)
        {
            //The device from the path is used to identify which device is in use
            //but it is then stripped from the path
            if (filename[0] != 'd')
                throw makeStringExceptionV(azureConfigError, "Expected a device number in the filename %s", fullName.str());

            char * endDevice = nullptr;
            device = strtod(filename+1, &endDevice);
            if ((device == 0) || (device > numDevices))
                throw makeStringExceptionV(azureConfigError, "Device %d out of range for plane %s", device, planeName.str());

            if (!endDevice || (*endDevice != '/' && *endDevice != '\0'))
                throw makeStringExceptionV(azureConfigError, "Unexpected end of device partition %s", fullName.str());

            filename = (*endDevice == '/') ? endDevice+1 : endDevice;
        }

        secretName.set(apiInfo->queryStorageApiSecret(device, accountName, containerName));

        if (isEmptyString(containerName))
            throw makeStringExceptionV(azureConfigError, "Missing container name for plane %s", planeName.str());

        if (isEmptyString(accountName))
            throw makeStringExceptionV(azureConfigError, "Missing account name for plane %s", planeName.str());

        if (!useManagedIdentity && isEmptyString(secretName))
            throw makeStringExceptionV(azureConfigError, "Missing secret name for plane %s", planeName.str());

        blobName.set(filename);
    }
    else
        throw makeStringExceptionV(azureConfigError, "Unexpected prefix on azure filename %s", fullName.str());
    blobContainerUrl = ::getContainerUrl(accountName, containerName);
    blobUrl = ::getBlobUrl(accountName, containerName, blobName);
}

AzureBlob::AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
                     const char *_accountName, bool _useManagedIdentity,
                     const StorageApiConfig *_configOverride, IAzureBlockOperations *_blockOperations,
                     bool _pathStyleOverride)
    : explicitSharedKeyCredential(std::move(_explicitSharedKeyCredential))
{
    if (isEmptyString(_blobUrl))
        throw makeStringException(azureArgumentError, "Azure blob URL is required");
    if (strchr(_blobUrl, '?') || strchr(_blobUrl, '#'))
        throw makeStringException(azureArgumentError, "Azure blob URL must not contain a query or fragment");
    if (!_useManagedIdentity && !explicitSharedKeyCredential)
        throw makeStringException(azureArgumentError, "Azure shared-key credential is required");
    if (_useManagedIdentity && !areManagedIdentitiesEnabled())
        throw makeStringException(azureArgumentError, "Azure managed identity is not enabled for this environment");
    if (_useManagedIdentity && isEmptyString(_accountName))
        throw makeStringException(azureArgumentError, "Azure account name is required for managed-identity blob URLs");
    useManagedIdentity = _useManagedIdentity;
    blockOperations.set(_blockOperations);
    if (_configOverride)
        config = *_configOverride;

    try
    {
        Azure::Core::Url url(_blobUrl);
        if ((url.GetScheme() != "https" && url.GetScheme() != "http") || url.GetHost().empty())
            throw makeStringException(azureArgumentError, "Azure blob URL must use HTTP or HTTPS and include a host");

        accountName.set(_useManagedIdentity ? _accountName : explicitSharedKeyCredential->AccountName.c_str());
        const std::string &path = url.GetPath();
        size_t containerStart = 0;
        pathStyleUrl = _pathStyleOverride;
        if (pathStyleUrl)
        {
            std::string accountPrefix = std::string(accountName.str()) + "/";
            if (path.compare(0, accountPrefix.size(), accountPrefix) != 0)
                throw makeStringException(azureArgumentError, "Azure path-style blob URL must start with the account name");
            containerStart = accountPrefix.size();
        }

        size_t containerEnd = path.find('/', containerStart);
        const std::string container = path.substr(containerStart, containerEnd - containerStart);
        if (container.empty())
            throw makeStringException(azureArgumentError, "Azure blob URL must include a container");

        containerName.set(Azure::Core::Url::Decode(container).c_str());
        blobName.set(containerEnd == std::string::npos ? "" : Azure::Core::Url::Decode(path.substr(containerEnd + 1)).c_str());
        blobUrl = url.GetAbsoluteUrl();
        fullName.set(blobUrl.c_str());
        url.SetPath(path.substr(0, containerEnd));
        blobContainerUrl = url.GetAbsoluteUrl();
    }
    catch (const std::exception &e)
    {
        throw makeStringExceptionV(azureArgumentError, "Invalid Azure blob URL: %s", e.what());
    }
}

AzureBlob::AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
                     bool _pathStyleUrl)
    : AzureBlob(_blobUrl, std::move(_explicitSharedKeyCredential), nullptr, false, nullptr, nullptr, _pathStyleUrl)
{
}

AzureBlob::AzureBlob(const char *_blobUrl, std::shared_ptr<StorageSharedKeyCredential> _explicitSharedKeyCredential,
                     const StorageApiConfig &_config, IAzureBlockOperations *_blockOperations,
                     bool _pathStyleUrl)
    : AzureBlob(_blobUrl, std::move(_explicitSharedKeyCredential), nullptr, false, &_config, _blockOperations, _pathStyleUrl)
{
}

AzureBlob::AzureBlob(const char *_blobUrl, const char *_accountName, bool _pathStyleUrl)
    : AzureBlob(_blobUrl, nullptr, _accountName, true, nullptr, nullptr, _pathStyleUrl)
{
}

AzureBlob::AzureBlob(const char *_blobUrl, const char *_accountName, const StorageApiConfig &_config,
                     bool _pathStyleUrl)
    : AzureBlob(_blobUrl, nullptr, _accountName, true, &_config, nullptr, _pathStyleUrl)
{
}


AzureBlobDirectoryIterator::AzureBlobDirectoryIterator(AzureBlob &_file, const char *_mask, bool _includeDirs)
    : AzureDirectoryIteratorBase(_file.fullName.str(), _file.containerName.str(), _mask, _includeDirs),
      file(&_file), containerClient(_file.getBlobContainerClient()), prefix(_file.blobName.str())
{
    // The SDK prefix is container-relative; the full prefix preserves the HPCC filename or URL.
    if (!prefix.empty() && prefix.back() != '/')
        prefix += '/';

    StringBuffer fullPathPrefix(fullPrefix);
    if (fullPathPrefix.length() > 0 && fullPathPrefix.charAt(fullPathPrefix.length()-1) != '/')
        fullPathPrefix.append('/');
    fullPrefix.set(fullPathPrefix.str());
}

IFile * AzureBlobDirectoryIterator::createFile(const char *fullPath, const DirEntry &entry)
{
    if (file->explicitSharedKeyCredential || !startsWith(fullPath, azureBlobPrefix))
    {
        std::string url = containerClient->GetBlockBlobClient(prefix + entry.name).GetUrl();
        Owned<AzureBlob> child;
        if (file->explicitSharedKeyCredential)
            child.setown(new AzureBlob(url.c_str(), file->explicitSharedKeyCredential, file->queryConfig(), nullptr, file->pathStyleUrl));
        else
            child.setown(new AzureBlob(url.c_str(), file->accountName.str(), file->queryConfig(), file->pathStyleUrl));
        child->setListedInfo(entry.isDir, entry.size, entry.modifiedTime);
        return child.getClear();
    }
    return createAzureBlob(fullPath, entry);
}

std::shared_ptr<StorageSharedKeyCredential> AzureBlob::getSharedKeyCredentials() const
{
    if (explicitSharedKeyCredential)
        return explicitSharedKeyCredential;
    return getAzureSharedKeyCredential(accountName, secretName.str());
}

std::string AzureBlob::getBlobUrl() const
{
    return blobUrl;
}

// Options shared by every client for this file: configured retries, and the shared HTTP
// transport so connections are pooled across all blob operations.
BlobClientOptions AzureBlob::makeClientOptions() const
{
    BlobClientOptions clientOptions;
    setAzureRetryOptions(clientOptions.Retry, config);
    clientOptions.Transport.Transport = getHttpTransport();
    return clientOptions;
}

std::shared_ptr<BlobContainerClient> AzureBlob::getBlobContainerClient() const
{
    if (useManagedIdentity)
        return std::make_shared<BlobContainerClient>(blobContainerUrl, getAzureManagedIdentityCredential(), makeClientOptions());
    return std::make_shared<BlobContainerClient>(blobContainerUrl, getSharedKeyCredentials(), makeClientOptions());
}

SharedBlobClient AzureBlob::getBlobClient() const
{
    // Concurrent opens may request the per-file client simultaneously; serialize lazy initialization.
    CriticalBlock block(cacheCS);
    // Return cached client if available (per-file caching only)
    if (cachedBlobClient)
        return cachedBlobClient;

    if (useManagedIdentity)
        cachedBlobClient = std::make_shared<BlockBlobClient>(getBlobUrl(), getAzureManagedIdentityCredential(), makeClientOptions());
    else
        cachedBlobClient = std::make_shared<BlockBlobClient>(getBlobUrl(), getSharedKeyCredentials(), makeClientOptions());
    return cachedBlobClient;
}

IAzureBlockOperations *AzureBlob::createBlockOperations(const char *baseBlockId) const
{
    if (blockOperations)
        return LINK(blockOperations.get());
    return new AzureBlobBlockOperations(getBlobClient(), baseBlockId);
}

bool AzureBlob::createDirectory()
{
    auto blobContainerClient = getBlobContainerClient();
    try
    {
        Azure::Response<Models::CreateBlobContainerResult> result = blobContainerClient->CreateIfNotExists();
        if (result.Value.Created==false)
            DBGLOG("AzureBlob::createDirectory: container not created because it already exists %s", containerName.str());
        else
            DBGLOG("AzureBlob::createDirectory: container created %s", containerName.str());
        return true;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::createDirectory", queryFilename());
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::createDirectory", queryFilename());
    }
}


bool AzureBlob::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
{
    ensureMetaData();
    if (createTime)
    {
        createTime->clear();
        createTime->set(createdOn);
    }
    if (modifiedTime)
    {
        modifiedTime->clear();
        modifiedTime->set(lastModified);
    }
    if (accessedTime)
        accessedTime->clear();
    return false;
}


IFileIO * AzureBlob::createFileReadIO()
{
    // Ensure metadata is loaded and capture the size before creating the reader.
    if (!exists())
        return nullptr;

    return new AzureBlobReadIO(this, fileSize); // NB: exists() ensures metadata is loaded and fileSize is valid
}

static void validateAzureWriteConfig(const StorageApiConfig &config)
{
    constexpr unsigned maxConcurrency = 512;
    constexpr unsigned __int64 maxChunkBytes = 4000ULL * 1024 * 1024;
    if (config.parallelWriteConcurrency > maxConcurrency)
        throw makeStringExceptionV(azureConfigError, "Azure parallel write concurrency %u is outside [0, %u]", config.parallelWriteConcurrency, maxConcurrency);
    if (!config.parallelWriteChunkBytes || config.parallelWriteChunkBytes > maxChunkBytes)
        throw makeStringExceptionV(azureConfigError, "Azure parallel write chunk size %llu is outside (0, %llu]", config.parallelWriteChunkBytes, maxChunkBytes);
    // The parallel writer buffers one chunk per upload plus one being filled by the caller.
    const unsigned slotCount = config.parallelWriteConcurrency + 1;
    if (config.parallelWriteChunkBytes > std::numeric_limits<size32_t>::max() / slotCount)
        throw makeStringException(azureConfigError, "Azure parallel write buffer size must be less than 4 GiB");
    if (config.parallelWriteChunkBytes > std::numeric_limits<size_t>::max() / slotCount)
        throw makeStringException(azureConfigError, "Azure parallel write buffer size overflows the address space");
}

IFileIO * AzureBlob::createFileWriteIO()
{
    validateAzureWriteConfig(config);
    if (config.parallelWriteConcurrency > 0)
        return new AzureBlobParallelBlockBlobWriteIO(this);
    return new AzureBlobBlockBlobWriteIO(this);
}

IDirectoryIterator * AzureBlob::directoryFiles(const char *mask, bool sub, bool includeDirs)
{
    if (sub)
        UNIMPLEMENTED_X("AzureBlob::directoryFiles recursive");

    return new AzureBlobDirectoryIterator(*this, mask, includeDirs);
}

void AzureBlob::ensureMetaData()
{
    // Concurrent opens may trigger the same cold metadata lookup; serialize the cache check and fill.
    CriticalBlock block(cacheCS);
    if (haveMeta)
        return;

    gatherMetaData();
}

void AzureBlob::gatherMetaData()
{
    auto blobClient = getBlobClient();
    try
    {
        Azure::Response<Models::BlobProperties> properties = blobClient->GetProperties();
        Models::BlobProperties & props = properties.Value;
        setProperties(props.BlobSize, props.LastModified, props.CreatedOn);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
        {
            fileExists = false;
            fileSize = unknownFileSize;
            lastModified = 0;
            createdOn = 0;
        }
        else
            throwRequestException(e, "AzureBlob::gatherMetaData", queryFilename());
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::gatherMetaData", queryFilename());
    }
    haveMeta = true;
}

bool AzureBlob::remove()
{
    auto blobClient = getBlobClient();
    try
    {
        Azure::Response<Models::DeleteBlobResult> resp = blobClient->DeleteIfExists();
        invalidateMeta();
        return resp.Value.Deleted;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::remove", queryFilename());
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::remove", queryFilename());
    }
}

void AzureBlob::setProperties(int64_t _blobSize, Azure::DateTime _lastModified, Azure::DateTime _createdOn)
{
    fileExists = true;
    fileSize = _blobSize;
    lastModified = system_clock::to_time_t(system_clock::time_point(_lastModified));
    createdOn = system_clock::to_time_t(system_clock::time_point(_createdOn));
}

IFile *createAzureBlob(const char *azureFileName)
{
    return new AzureBlob(azureFileName);
}

IFile *createAzureBlob(const char *azureFileName, const DirEntry &entry)
{
    return new AzureBlob(azureFileName, entry);
}

#ifdef _USE_CPPUNIT
/*
 * Checks read boundaries, short-download rejection and metadata invalidation after
 * deletion or commit. Azure SDK requests use an in-memory mock HTTP transport;
 * no Azure service is contacted.
 */
class AzureBlobReadAndMetadataTest : public CppUnit::TestFixture
{
    class MockTransport final : public Azure::Core::Http::HttpTransport
    {
    public:
        std::vector<std::unique_ptr<Azure::Core::Http::RawResponse>> responses;
        size_t requests = 0;

        virtual std::unique_ptr<Azure::Core::Http::RawResponse> Send(
            Azure::Core::Http::Request &request, const Azure::Core::Context &context) override
        {
            CPPUNIT_ASSERT_MESSAGE("Unexpected Azure request", requests < responses.size());
            return std::move(responses[requests++]);
        }

        void addResponse(Azure::Core::Http::HttpStatusCode status, const char *body)
        {
            auto response = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, status, "Mock response");
            response->SetHeader("Content-Length", std::to_string(strlen(body)));
            response->SetHeader("ETag", "\"test\"");
            response->SetHeader("Last-Modified", "Mon, 02 Nov 2026 12:00:00 GMT");
            response->SetHeader("x-ms-creation-time", "Mon, 02 Nov 2026 12:00:00 GMT");
            response->SetHeader("x-ms-blob-type", "BlockBlob");
            response->SetHeader("x-ms-server-encrypted", "true");
            response->SetHeader("x-ms-request-server-encrypted", "true");
            if (status == Azure::Core::Http::HttpStatusCode::NotFound)
                response->SetHeader("x-ms-error-code", "BlobNotFound");
            response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(
                reinterpret_cast<const uint8_t *>(body), strlen(body)));
            responses.push_back(std::move(response));
        }
    };

    static AzureBlob *createFile(const std::shared_ptr<MockTransport> &transport, offset_t size)
    {
        BlobClientOptions options;
        options.Transport.Transport = transport;
        options.Retry.MaxRetries = 0;
        Owned<AzureBlob> file = new AzureBlob(std::make_shared<BlockBlobClient>(
            "https://test.invalid/container/blob", options));
        file->fullName.set("azureblob:test/blob");
        file->setListedInfo(false, size, 1);
        return file.getClear();
    }

public:
    CPPUNIT_TEST_SUITE(AzureBlobReadAndMetadataTest);
        CPPUNIT_TEST(testReadBoundaries);
        CPPUNIT_TEST(testShortDownload);
        CPPUNIT_TEST(testDeleteInvalidatesMetadata);
        CPPUNIT_TEST(testCommitInvalidatesMetadata);
    CPPUNIT_TEST_SUITE_END();

    void testReadBoundaries()
    {
        START_TEST
        auto transport = std::make_shared<MockTransport>();
        Owned<AzureBlob> file = createFile(transport, 4);
        Owned<IFileIO> io = file->open(IFOread);
        char buffer[8] = {};
        CPPUNIT_ASSERT_EQUAL(size32_t(0), io->read(0, 0, nullptr));
        CPPUNIT_ASSERT_EQUAL(size32_t(0), io->read(4, sizeof(buffer), buffer));
        CPPUNIT_ASSERT_EQUAL(size32_t(0), io->read(5, sizeof(buffer), buffer));
        transport->addResponse(Azure::Core::Http::HttpStatusCode::Ok, "data");
        CPPUNIT_ASSERT_EQUAL(size32_t(4), io->read(0, sizeof(buffer), buffer));
        CPPUNIT_ASSERT_EQUAL_STR("data", buffer);
        Owned<AzureBlob> emptyFile = createFile(transport, 0);
        Owned<IFileIO> emptyIO = emptyFile->open(IFOread);
        CPPUNIT_ASSERT_EQUAL(size32_t(0), emptyIO->read(0, sizeof(buffer), buffer));
        CPPUNIT_ASSERT_EQUAL(size_t(1), transport->requests);
        END_TEST
    }

    void testShortDownload()
    {
        START_TEST
        auto transport = std::make_shared<MockTransport>();
        Owned<AzureBlob> file = createFile(transport, 8);
        Owned<IFileIO> io = file->open(IFOread);
        transport->addResponse(Azure::Core::Http::HttpStatusCode::Ok, "data");
        char buffer[8];
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(io->read(0, sizeof(buffer), buffer),
                                        "A shorter Azure download must not be accepted");
        END_TEST
    }

    void testDeleteInvalidatesMetadata()
    {
        START_TEST
        for (auto status : {Azure::Core::Http::HttpStatusCode::Accepted, Azure::Core::Http::HttpStatusCode::NotFound})
        {
            auto transport = std::make_shared<MockTransport>();
            Owned<AzureBlob> file = createFile(transport, 4);
            file->createdOn = 1;
            transport->addResponse(status, "");
            CPPUNIT_ASSERT_EQUAL(status == Azure::Core::Http::HttpStatusCode::Accepted, file->remove());
            transport->addResponse(Azure::Core::Http::HttpStatusCode::NotFound, "");
            CPPUNIT_ASSERT(!file->exists());
            CPPUNIT_ASSERT_EQUAL(unknownFileSize, file->size());
            CPPUNIT_ASSERT_EQUAL(time_t(0), file->lastModified);
            CPPUNIT_ASSERT_EQUAL(time_t(0), file->createdOn);
            CPPUNIT_ASSERT_EQUAL(size_t(2), transport->requests);
        }
        END_TEST
    }

    void testCommitInvalidatesMetadata()
    {
        START_TEST
        for (unsigned concurrency : {0U, 1U})
        {
            auto transport = std::make_shared<MockTransport>();
            Owned<AzureBlob> file = createFile(transport, 4);
            file->config.parallelWriteConcurrency = concurrency;
            transport->addResponse(Azure::Core::Http::HttpStatusCode::Created, "");
            Owned<IFileIO> io = file->open(IFOcreate);
            if (!concurrency)
                transport->addResponse(Azure::Core::Http::HttpStatusCode::Created, "");
            CPPUNIT_ASSERT_EQUAL(size32_t(4), io->write(0, 4, "data"));
            transport->addResponse(Azure::Core::Http::HttpStatusCode::Ok, "");
            CPPUNIT_ASSERT_EQUAL(offset_t(0), file->size());
            if (concurrency)
                transport->addResponse(Azure::Core::Http::HttpStatusCode::Created, "");
            transport->addResponse(Azure::Core::Http::HttpStatusCode::Created, "");
            io->close();
            transport->addResponse(Azure::Core::Http::HttpStatusCode::Ok, "data");
            CPPUNIT_ASSERT_EQUAL(offset_t(4), file->size());
            io->close();
            CPPUNIT_ASSERT_EQUAL(size_t(5), transport->requests);
        }
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobReadAndMetadataTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobReadAndMetadataTest, "AzureBlobReadAndMetadataTest");

class AzureBlobUrlStyleTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobUrlStyleTest);
        CPPUNIT_TEST(testCustomHostPathStyleUrl);
    CPPUNIT_TEST_SUITE_END();

    void testCustomHostPathStyleUrl()
    {
        START_TEST
        const char *url = "http://azurite:10000/devstoreaccount1/container/blob";
        Owned<IFile> file = createAzureBlob(url, "devstoreaccount1", "dGVzdC1rZXk=", true);
        auto *azureBlob = static_cast<AzureBlob *>(file.get());
        CPPUNIT_ASSERT_EQUAL(std::string("container"), std::string(azureBlob->containerName.str()));
        CPPUNIT_ASSERT_EQUAL(std::string("blob"), std::string(azureBlob->blobName.str()));
        Azure::Core::Url expectedContainerUrl(url);
        expectedContainerUrl.SetPath("/devstoreaccount1/container");
        CPPUNIT_ASSERT_EQUAL(expectedContainerUrl.GetAbsoluteUrl(), azureBlob->blobContainerUrl);
        CPPUNIT_ASSERT(azureBlob->pathStyleUrl);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobUrlStyleTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobUrlStyleTest, "AzureBlobUrlStyleTest");
#endif

static std::shared_ptr<StorageSharedKeyCredential> createAzureSharedKeyCredential(const char *accountName,
                                                                                   const char *accountKey)
{
    if (isEmptyString(accountName))
        throw makeStringException(azureArgumentError, "Azure account name is required");
    if (isEmptyString(accountKey))
        throw makeStringException(azureArgumentError, "Azure account key is required");
    return std::make_shared<StorageSharedKeyCredential>(accountName, accountKey);
}

IFile *createAzureBlob(const char *blobUrl, const char *accountName, const char *accountKey,
                       bool pathStyle)
{
    return new AzureBlob(blobUrl, createAzureSharedKeyCredential(accountName, accountKey), pathStyle);
}


IFile *createAzureBlob(const char *blobUrl, const char *accountName, const char *accountKey,
                       const StorageApiConfig &config, IAzureBlockOperations *operations,
                       bool pathStyle)
{
    return new AzureBlob(blobUrl, createAzureSharedKeyCredential(accountName, accountKey), config, operations, pathStyle);
}

IFile *createAzureBlob(const char *blobUrl, const char *accountName, bool pathStyle)
{
    return new AzureBlob(blobUrl, accountName, pathStyle);
}

IFile *createAzureBlob(const char *blobUrl, const char *accountName, const StorageApiConfig &config,
                       bool pathStyle)
{
    return new AzureBlob(blobUrl, accountName, config, pathStyle);
}
