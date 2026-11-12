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
#include "azureblob.hpp"
#include "azureapiutils.hpp"

#ifdef _USE_CPPUNIT
#include "jptree.hpp"
#include "jstatcodes.h"
#include "unittests.hpp"
#include <vector>
#endif

#include <azure/core/base64.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/core/http/policies/policy.hpp>
#include <azure/identity.hpp>
#include <map>

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

static std::string makeAzureBlockId(const std::string &baseBlockId, unsigned sequence)
{
    if (sequence == 0 || sequence > maxAzureBlockCount)
        throw makeStringException(1234, "Too many blocks for Azure block blob");

    return VStringBuffer("%s%08u", baseBlockId.c_str(), sequence).str();
}

class AzureBlobBlockOperations final : public CInterfaceOf<IAzureBlockOperations>
{
public:
    AzureBlobBlockOperations(SharedBlobClient _client, const char *_baseBlockId)
        : client(std::move(_client)), baseBlockId(_baseBlockId) {}

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
    AzureBlobIO(AzureBlob * _file, const FileIOStats & _stats);
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
    AzureBlobReadIO(AzureBlob * _file, const FileIOStats & _stats);

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
};


class AzureBlobWriteIO : public AzureBlobIO
{
public:
    AzureBlobWriteIO(AzureBlob * _file);

    virtual void beforeDispose() override;

    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        throwUnexpectedX("Reading from write only file");
    }
    virtual offset_t size() override;
    virtual void setSize(offset_t size) override;
    virtual void flush() override;

protected:
    void checkAppendPosition(offset_t pos) const;
    StringBuffer baseBlockId;
    Owned<IAzureBlockOperations> operations;
    offset_t offset = 0;
};

// Sequential block-blob writer. IFileIO calls must be serialized by the caller.
// Each non-empty write must append at the current offset and is staged synchronously
// as one block directly from the caller's buffer; blocks may have different sizes,
// subject to Azure's per-block size limit. Empty writes do nothing. Staged blocks
// aren't visible in the blob until close() commits their ordered list; flush() does
// not commit. The writer is limited to 50,000 blocks, and close() is idempotent.
class AzureBlobBlockBlobWriteIO final : implements AzureBlobWriteIO
{
public:
    AzureBlobBlockBlobWriteIO(AzureBlob * _file);

    virtual void close() override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;

private:
    bool committed = false;
    unsigned lastSequence = 0;
};


//---------------------------------------------------------------------------------------------------------------------
// Directory iterator for Azure Blob Storage using ListBlobsByHierarchy API.
// Uses "/" as a delimiter to simulate directory listing, returning both blobs
// (files) and blob prefixes (virtual directories) at the current level.

class AzureBlobDirectoryIterator final : public AzureDirectoryIteratorBase
{
public:
    AzureBlobDirectoryIterator(std::shared_ptr<BlobContainerClient> _containerClient,
                               const char *_prefix, const char *_fullPrefix,
                               const char *_containerName,
                               const char *_mask, bool _includeDirs)
        : AzureDirectoryIteratorBase(_fullPrefix, _containerName, _mask, _includeDirs),
          containerClient(std::move(_containerClient)), prefix(_prefix) {}

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

    std::shared_ptr<BlobContainerClient> containerClient;
    std::string prefix;
    std::optional<ListBlobsByHierarchyPagedResponse> pagedResponse;
};


class AzureBlob final : implements CInterfaceOf<IFile>
{
public:
    AzureBlob(const char *_azureFileName);
#ifdef _USE_CPPUNIT
    AzureBlob(const char *_azureFileName, const char *_blobServiceEndpoint,
        std::shared_ptr<StorageSharedKeyCredential> _testSharedKeyCredential);
#endif
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
    void setListedInfo(bool _isDir, offset_t _fileSize, time_t _lastModified)
    {
        haveMeta = true;
        isDir = _isDir;
        fileExists = true;
        fileSize = _fileSize;
        lastModified = _lastModified;
        createdOn = 0;
    }
    const StorageApiConfig & queryConfig() const { return config; }

protected:
    std::shared_ptr<StorageSharedKeyCredential> getSharedKeyCredentials() const;
    std::string getBlobUrl() const;
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
#ifdef _USE_CPPUNIT
    // Alternate endpoint used only by the Azurite CppUnit tests.
    std::string blobServiceEndpoint;
    std::shared_ptr<StorageSharedKeyCredential> testSharedKeyCredential;
#endif
    mutable CriticalSection cs{SYNC_LOCATION};
    mutable std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient> cachedBlobClient;  // Cache client for reuse per-file
    StorageApiConfig config;
};


//---------------------------------------------------------------------------------------------------------------------

AzureBlobIO::AzureBlobIO(AzureBlob * _file, const FileIOStats & _firstStats)
: file(_file), stats(_firstStats), blockBlobClient(file->getBlobClient())
{

}

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


AzureBlobReadIO::AzureBlobReadIO(AzureBlob * _file, const FileIOStats & _firstStats)
: AzureBlobIO(_file, _firstStats)
{
}

size32_t AzureBlobReadIO::read(offset_t pos, size32_t len, void * data)
{
    CCycleTimer timer;
    offset_t fileSize = file->size();
    if (pos > fileSize)
        return 0;

    if (pos + len > fileSize)
        len = fileSize - pos;

    if (len == 0)
        return 0;

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
        // result.Value.BlobSize is the size of the blob, not the size of the data returned, use ContentRange instead
        Azure::Core::Http::HttpRange range = result.Value.ContentRange;
        if (range.Length.HasValue())
            sizeRead = range.Length.Value();
        else
            sizeRead = 0;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::read", file->queryFilename(), pos, len);
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::read", file->queryFilename(), pos, len);
    }

    //Use fastAdd because multi threaded access is not supported by this class
    stats.ioReads.fastAdd(1);
    stats.ioReadCycles.fastAdd(timer.elapsedCycles());
    stats.ioReadBytes.fastAdd(sizeRead);

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
    return file->size();
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

    try
    {
        Azure::Core::IO::MemoryBodyStream empty(nullptr, 0);
        blockBlobClient->Upload(empty);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throw makeStringExceptionV(1234, "Azure create block blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
    }

    operations.setown(new AzureBlobBlockOperations(blockBlobClient, baseBlockId.str()));
}

void AzureBlobWriteIO::beforeDispose()
{
    try
    {
        close();
    }
    catch (...)
    {
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
}

void AzureBlobWriteIO::checkAppendPosition(offset_t pos) const
{
    if (unlikely(pos != offset))
        throw makeStringExceptionV(1234, "Azure Blobs only support appending writes, unexpected write position %llu, expected %llu", pos, offset);
}

//---------------------------------------------------------------------------------------------------------------------

AzureBlobBlockBlobWriteIO::AzureBlobBlockBlobWriteIO(AzureBlob * _file) : AzureBlobWriteIO(_file)
{
}

size32_t AzureBlobBlockBlobWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len == 0)
        return 0;

    checkAppendPosition(pos);

    file->invalidateMeta();

    CCycleTimer timer;
    const unsigned sequence = lastSequence + 1;

    try
    {
        operations->stageBlock(sequence, static_cast<const byte *>(data), len);
        lastSequence = sequence;
        offset += len;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::write", file->queryFilename(), pos, len);
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::write", file->queryFilename(), pos, len);
    }

    stats.ioWrites.fastAdd(1);
    stats.ioWriteCycles.fastAdd(timer.elapsedCycles());
    stats.ioWriteBytes.fastAdd(len);
    return len;
}

void AzureBlobBlockBlobWriteIO::close()
{
    if (committed)
        return;
    file->invalidateMeta();

    try
    {
        operations->commitBlockList(lastSequence);
        committed = true;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throwRequestException(e, "AzureBlob::close", file->queryFilename(), offset, 0);
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::close", file->queryFilename(), offset, 0);
    }
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

#ifdef _USE_CPPUNIT
static std::string getContainerUrl(const char *account, const char *container, const char *blobServiceEndpoint)
{
    std::string url(blobServiceEndpoint);
    return url.append("/").append(container);
}

static std::string getBlobUrl(const char *account, const char *container, const char *blob, const char *blobServiceEndpoint)
{
    std::string url(getContainerUrl(account, container, blobServiceEndpoint));
    return url.append("/").append(blob);
}
#endif

AzureBlob::AzureBlob(const char *_azureFileName) : fullName(_azureFileName)
{
    if (startsWith(fullName, azureBlobPrefix))
    {
        //format is azureblob:plane[/device]/path
        const char * filename = fullName + strlen(azureBlobPrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            throw makeStringException(99, "Missing / in azureblob: file reference");

        StringBuffer planeName(slash-filename, filename);
        Owned<const IStoragePlane> plane = getStoragePlaneByName(planeName, true);
        Owned<IStorageApiInfo> apiInfo = plane->getStorageApiInfo();
        if (!apiInfo)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());
        config = apiInfo->queryAPIConfig();
        filename = slash+1; // advance past slash

        const char * api = apiInfo->getStorageType();
        if (!api)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        StringBuffer azureBlobAPI(strlen(azureBlobPrefix) - 1, azureBlobPrefix);
        if (!strieq(api, azureBlobAPI.str()))
            throw makeStringExceptionV(99, "Storage api for plane %s is not azureblob", planeName.str());

        useManagedIdentity = apiInfo->useManagedIdentity();
        //MORE: We could allow the managed identity/secret to be supplied in the configuration
        if (useManagedIdentity && !areManagedIdentitiesEnabled())
            throw makeStringExceptionV(99, "Managed identity is not enabled for this environment");

        unsigned numDevices = plane->numDevices();
        unsigned device = 1;
        if (numDevices != 1)
        {
            //The device from the path is used to identify which device is in use
            //but it is then stripped from the path
            if (filename[0] != 'd')
                throw makeStringExceptionV(99, "Expected a device number in the filename %s", fullName.str());

            char * endDevice = nullptr;
            device = strtod(filename+1, &endDevice);
            if ((device == 0) || (device > numDevices))
                throw makeStringExceptionV(99, "Device %d out of range for plane %s", device, planeName.str());

            if (!endDevice || (*endDevice != '/' && *endDevice != '\0'))
                throw makeStringExceptionV(99, "Unexpected end of device partition %s", fullName.str());

            filename = (*endDevice == '/') ? endDevice+1 : endDevice;
        }

        secretName.set(apiInfo->queryStorageApiSecret(device, accountName, containerName));

        if (isEmptyString(containerName))
            throw makeStringExceptionV(99, "Missing container name for plane %s", planeName.str());

        if (isEmptyString(accountName))
            throw makeStringExceptionV(99, "Missing account name for plane %s", planeName.str());

        if (!useManagedIdentity && isEmptyString(secretName))
            throw makeStringExceptionV(99, "Missing secret name for plane %s", planeName.str());

        blobName.set(filename);
    }
    else
        throw makeStringExceptionV(99, "Unexpected prefix on azure filename %s", fullName.str());
    blobUrl = ::getBlobUrl(accountName, containerName, blobName);
}

#ifdef _USE_CPPUNIT
AzureBlob::AzureBlob(const char *_azureFileName, const char *_blobServiceEndpoint,
    std::shared_ptr<StorageSharedKeyCredential> _testSharedKeyCredential)
: AzureBlob(_azureFileName)
{
    if (isEmptyString(_blobServiceEndpoint))
        throw makeStringException(-1, "Azure test blob service endpoint is required");
    if (!_testSharedKeyCredential)
        throw makeStringException(-1, "Azure test shared-key credential is required");

    blobServiceEndpoint = _blobServiceEndpoint;
    while (!blobServiceEndpoint.empty() && blobServiceEndpoint.back() == '/')
        blobServiceEndpoint.pop_back();
    blobUrl = ::getBlobUrl(accountName, containerName, blobName, blobServiceEndpoint.c_str());
    testSharedKeyCredential = std::move(_testSharedKeyCredential);
}
#endif

IFile * AzureBlobDirectoryIterator::createFile(const char *fullPath, const DirEntry &entry)
{
    return createAzureBlob(fullPath, entry);
}

std::shared_ptr<StorageSharedKeyCredential> AzureBlob::getSharedKeyCredentials() const
{
#ifdef _USE_CPPUNIT
    if (testSharedKeyCredential)
        return testSharedKeyCredential;
#endif
    return getAzureSharedKeyCredential(accountName, secretName.str());
}

std::string AzureBlob::getBlobUrl() const
{
    return blobUrl;
}

std::shared_ptr<BlobContainerClient> AzureBlob::getBlobContainerClient() const
{
#ifdef _USE_CPPUNIT
    std::string blobContainerUrl = blobServiceEndpoint.empty()
        ? getContainerUrl(accountName, containerName)
        : getContainerUrl(accountName, containerName, blobServiceEndpoint.c_str());
#else
    std::string blobContainerUrl = getContainerUrl(accountName, containerName);
#endif

    // Create optimized client options for better performance
    Azure::Storage::Blobs::BlobClientOptions clientOptions;

    setAzureRetryOptions(clientOptions.Retry, config);

    // Use shared transport instance for connection pooling across all blob operations
    clientOptions.Transport.Transport = getHttpTransport();

    if (useManagedIdentity)
    {
        return std::make_shared<BlobContainerClient>(blobContainerUrl, getAzureManagedIdentityCredential(), clientOptions);
    }
    else
    {
        return std::make_shared<BlobContainerClient>(blobContainerUrl, getSharedKeyCredentials(), clientOptions);
    }
}

SharedBlobClient AzureBlob::getBlobClient() const
{
    // Return cached client if available (per-file caching only)
    CriticalBlock block(cs);
    if (cachedBlobClient)
        return cachedBlobClient;

    // Create optimized client options for better performance
    Azure::Storage::Blobs::BlobClientOptions clientOptions;

    setAzureRetryOptions(clientOptions.Retry, config);

    // Use shared transport instance for connection pooling across all blob operations
    clientOptions.Transport.Transport = getHttpTransport();

    // Create account-specific credentials with caching
    if (useManagedIdentity)
        cachedBlobClient = std::make_shared<Azure::Storage::Blobs::BlockBlobClient>(getBlobUrl(), getAzureManagedIdentityCredential(), clientOptions);
    else
        cachedBlobClient = std::make_shared<Azure::Storage::Blobs::BlockBlobClient>(getBlobUrl(), getSharedKeyCredentials(), clientOptions);
    return cachedBlobClient;
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
        IException * error = makeStringExceptionV(1234, "Azure create container failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
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
    //Read the first chunk of the file.  If it is the full file then fill in the meta information, otherwise
    //ensure the meta information is calculated before creating the file IO object
    FileIOStats readStats;

    CriticalBlock block(cs);
    if (!exists())
        return nullptr;

    return new AzureBlobReadIO(this, readStats);
}

IFileIO * AzureBlob::createFileWriteIO()
{
    return new AzureBlobBlockBlobWriteIO(this);
}

IDirectoryIterator * AzureBlob::directoryFiles(const char *mask, bool sub, bool includeDirs)
{
    if (sub)
        UNIMPLEMENTED_X("AzureBlob::directoryFiles recursive");

    // Build the blob prefix for listing. The blobName is the path within the container.
    // For directory listing, we want to list blobs under this prefix with "/" delimiter.
    std::string listPrefix(blobName.str());
    if (!listPrefix.empty() && listPrefix.back() != '/')
        listPrefix += '/';

    // Build the full azure path prefix for constructing child IFile paths
    // fullName is e.g. "azureblob:plane/path" or "azureblob:plane/d1/path"
    StringBuffer fullPathPrefix(fullName);
    if (fullPathPrefix.length() > 0 && fullPathPrefix.charAt(fullPathPrefix.length()-1) != '/')
        fullPathPrefix.append('/');

    auto containerClient = getBlobContainerClient();
    return new AzureBlobDirectoryIterator(containerClient, listPrefix.c_str(), fullPathPrefix.str(), containerName.str(), mask, includeDirs);
}

void AzureBlob::ensureMetaData()
{
    CriticalBlock block(cs);
    if (haveMeta)
        return;

    gatherMetaData();
    haveMeta = true;
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
            return;
        }
        throwRequestException(e, "AzureBlob::gatherMetaData", queryFilename());
    }
    catch (const std::exception& e)
    {
        throwRequestException(e, "AzureBlob::gatherMetaData", queryFilename());
    }
}

bool AzureBlob::remove()
{
    auto blobClient = getBlobClient();
    try
    {
        Azure::Response<Models::DeleteBlobResult> resp = blobClient->DeleteIfExists();
        if (resp.Value.Deleted==true)
        {
            fileExists = false;
            return true;
        }
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure delete blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
    return false;
}

void AzureBlob::setProperties(int64_t _blobSize, Azure::DateTime _lastModified, Azure::DateTime _createdOn)
{
    haveMeta = true;
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
static constexpr const char azuriteBlobEndpoint[] = "http://127.0.0.1:10000/devstoreaccount1";
static constexpr const char azuriteDevStoreKey[] = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

static IFile *createAzureBlobForTest(const char *azureFileName)
{
    auto credential = std::make_shared<StorageSharedKeyCredential>("devstoreaccount1", azuriteDevStoreKey);
    return new AzureBlob(azureFileName, azuriteBlobEndpoint, std::move(credential));
}

/*
 * This test needs a local Azurite Blob service. Start it in one terminal:
 *
 *   npx --yes azurite --location /tmp/azurite-test --blobHost 127.0.0.1 --blobPort 10000
 *
 * Then run from the build package directory:
 *   bin/unittests -v -d <filehooks-directory> -e AzureBlobWriteTimingTest
 *
 * The suite name contains "Timing", so the main unittest driver excludes it
 * from the default run unless the test is selected explicitly or -a is used.
 */
class AzureBlobWriteTimingTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(AzureBlobWriteTimingTest);
        CPPUNIT_TEST(testWriteSizes);
    CPPUNIT_TEST_SUITE_END();

public:
    virtual void setUp() override
    {
        originalComponentConfig.setown(getComponentConfig());
        originalGlobalConfig.setown(getGlobalConfig());

        try
        {
            setupAzuriteEnvironment();
        }
        catch (IException *e)
        {
            Owned<IException> ownedException(e);
            StringBuffer message;
            e->errorMessage(message);
            cleanupTestEnvironment();
            CPPUNIT_FAIL(message.str());
        }
        catch (...)
        {
            cleanupTestEnvironment();
            throw;
        }
    }

    virtual void tearDown() override
    {
        cleanupTestEnvironment();
    }

    void testWriteSizes()
    {
        START_TEST
        const std::vector<size32_t> sizes = {1, 1023, 1024, 1025, 3 * 1024 + 7};
        for (size32_t size : sizes)
        {
            std::string name = uniqueBlobName();
            std::vector<byte> expected(size);
            for (size32_t i = 0; i < size; ++i)
                expected[i] = static_cast<byte>((i * 31 + size) & 0xff);

            Owned<IFile> output(createAzureBlobForTest(name.c_str()));
            Owned<IFileIO> writer(output->open(IFOcreate));
            CPPUNIT_ASSERT(writer);
            CPPUNIT_ASSERT_EQUAL(size, writer->write(0, size, expected.data()));
            CPPUNIT_ASSERT_EQUAL(static_cast<offset_t>(size), writer->size());
            CPPUNIT_ASSERT_EQUAL(1ULL, writer->getStatistic(StNumDiskWrites));
            CPPUNIT_ASSERT_EQUAL(static_cast<unsigned __int64>(size), writer->getStatistic(StSizeDiskWrite));
            writer->close();

            Owned<IFile> inputFile(createAzureBlobForTest(name.c_str()));
            Owned<IFileIO> reader(inputFile->open(IFOread));
            CPPUNIT_ASSERT(reader);
            std::vector<byte> actual(size);
            CPPUNIT_ASSERT_EQUAL(size, reader->read(0, size, actual.data()));
            CPPUNIT_ASSERT(actual == expected);
            inputFile->remove();
        }
        END_TEST
    }

private:
    void setupAzuriteEnvironment()
    {
        testPlaneName.set("azurite-test-").append(getTimeStampNowValue());

        VStringBuffer testPlaneYaml(R"!!(
planes:
- name: %s
  category: data
  prefix: /tmp/azurite-test
  storageapi:
    type: azureblob
    account: devstoreaccount1
    secret: test-credential-injected
    containers:
    - name: hpccparalleltest
)!!", testPlaneName.str());

        Owned<IPropertyTree> testPlaneConfig = createPTreeFromYAMLString(testPlaneYaml);
        Owned<IPropertyTree> testPlane = testPlaneConfig->getPropTree("planes");
        CPPUNIT_ASSERT(testPlane);

        Owned<IPropertyTree> globalConfig = createPTreeFromIPT(originalGlobalConfig);
        Owned<IPropertyTree> storageConfig = globalConfig->getPropTree("storage");
        if (!storageConfig)
            storageConfig.set(globalConfig->addPropTree("storage", createPTree()));
        storageConfig->addPropTree("planes", testPlane.getClear());

        replaceComponentConfig(originalComponentConfig, globalConfig);

        Owned<IFile> container(createAzureBlobForTest(VStringBuffer("azureblob:%s/", testPlaneName.str())));
        container->createDirectory();
    }

    void cleanupTestEnvironment()
    {
        replaceComponentConfig(originalComponentConfig, originalGlobalConfig);
    }

    StringBuffer testPlaneName;
    Owned<IPropertyTree> originalComponentConfig;
    Owned<IPropertyTree> originalGlobalConfig;
    bool configReplacementAttempted = false;

    std::string uniqueBlobName()
    {
        // The plane name is only an HPCC lookup key; Azure identifies blobs by container and blob name.
        // Include the run ID here to prevent concurrent test processes from sharing blob names.
        static unsigned counter = 0;
        return std::string("azureblob:") + testPlaneName.str() + "/write-timing-" + testPlaneName.str() + "-" + std::to_string(++counter);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AzureBlobWriteTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AzureBlobWriteTimingTest, "AzureBlobWriteTimingTest");
#endif
