/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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
#include "jlog.hpp"
#include "jplane.hpp"
#include "azurefile.hpp"

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/shares.hpp>
#include <azure/core/base64.hpp>

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;
using namespace Azure::Storage::Files;
using namespace std::chrono;

#define TRACE_AZURE

/*
 * Azure related comments
 *
 * Does it make more sense to create input and output streams directly from the IFile rather than going via
 * an IFileIO.  E.g., append blobs
 * The overhead is trivial, so leave as it is for now.
 */
constexpr const char * azureBlobPrefix = "azureblob:";  // Syntax azureblob:storageplane[/device]/<path>

static bool areManagedIdentitiesEnabled()
{
    //Use a local static to avoid re-evaluation.  Performance is not critical - so once overhead is acceptable.
    static bool enabled = std::getenv("MSI_ENDPOINT") || std::getenv("IDENTITY_ENDPOINT");
    return enabled;
}

static constexpr unsigned maxAzureBlockCount = 50000;


//---------------------------------------------------------------------------------------------------------------------

using SharedBlobClient = std::shared_ptr<Azure::Storage::Blobs::BlockBlobClient>;

class AzureBlob;
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
    CriticalSection cs;
    offset_t offset = 0;
};

class AzureBlobBlockBlobWriteIO final : implements AzureBlobWriteIO
{
public:
    AzureBlobBlockBlobWriteIO(AzureBlob * _file);

    virtual void close() override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;

protected:
    std::string generateNextUniqueBlockId();

private:
    StringBuffer baseBlockId;
    std::vector<std::string> blockIds;
    bool committed = false;
    unsigned blockIndex = 0;
};


class AzureBlob final : implements CInterfaceOf<IFile>
{
public:
    AzureBlob(const char *_azureFileName);
    virtual bool exists() override
    {
        ensureMetaData();
        return fileExists;
    }
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime) override;
    virtual fileBool isDirectory() override
    {
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
        //Should this be derived from a comman base that implements the setShareMode()?
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
    virtual IDirectoryIterator *directoryFiles(const char *mask, bool sub, bool includeDirs) override
    {
        UNIMPLEMENTED_X("AzureBlob::directoryFiles");
    }
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
    StringAttr accountName;
    StringAttr containerName;
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
    CriticalSection cs;
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


//---------------------------------------------------------------------------------------------------------------------

static void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries)
{
    OWARNLOG("%s", message);

    if (attempt >= maxRetries)
        throw makeStringException(1234, message);

    // Exponential backoff with jitter
    unsigned backoffMs = (1U << attempt) * 100 + (rand() % 100);
    Sleep(backoffMs);
}

static void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("AzureBlob::%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s (%d)",
                      op, attempt, maxRetries, filename, pos, len, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

static void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("AzureBlob::%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s",
                      op, attempt, maxRetries, filename, pos, len, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}

//---------------------------------------------------------------------------------------------------------------------


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
    uint8_t * buffer = reinterpret_cast<uint8_t*>(data);
    long int sizeRead = 0;

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Response<Models::DownloadBlobToResult> result = blockBlobClient->DownloadTo(buffer, len, options);
            Azure::Core::Http::HttpRange range = result.Value.ContentRange;
            if (range.Length.HasValue())
                sizeRead = range.Length.Value();
            else
                sizeRead = 0;
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            //Future: update stats if the read fails... - use a local object with a destructor that updates the time
            attempt++;
            handleRequestException(e, "read", attempt, maxRetries, file->queryFilename(), pos, len);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "read", attempt, maxRetries, file->queryFilename(), pos, len);
        }
    }

    //Use fastAdd because multi threaded access is not supported by this class
    stats.ioReads.fastAdd(1);
    stats.ioReadCycles.fastAdd(timer.elapsedCycles());
    stats.ioReadBytes.fastAdd(sizeRead);
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

//---------------------------------------------------------------------------------------------------------------------

AzureBlobBlockBlobWriteIO::AzureBlobBlockBlobWriteIO(AzureBlob * _file) : AzureBlobWriteIO(_file)
{
    // Each block in a block blob needs to have a unique id.  This must be base64 encoded, and less than 64 characters in length (before encoding).
    // Use the following to generate a unique id:
    // * 32bit hash of the file name
    // * 64bit timestamp
    // * 32bit hash of get_cycles_now()
    // * 2 underscores
    //
    // This is then encoded to a base64 base string.
    //
    // Each block id then has a 8 character blockid appended to this base-64 encoded base id.
    // 18bytes pre-encoding.  24 bytes post encoding.  32 characters with the blockid appended.

    file->invalidateMeta();

    MemoryBuffer blockId;
    blockId.append(hashncz_fnv1a((const byte *)file->queryFilename(), fnvInitialHash32));
    blockId.append(getTimeStampNowValue());
    blockId.append(hashvalue_fnv1a(get_cycles_now(), fnvInitialHash32));
    blockId.append('_').append('_');
    //Ensure the base block id is a multiple of 3, so that when it is base 64 encoded there are no padding characters
    dbgassertex(blockId.length() % 3 == 0);
    JBASE64_Encode(blockId.bytes(), blockId.length(), baseBlockId, false);

    try
    {
        Azure::Core::IO::MemoryBodyStream empty(nullptr, 0);
        // The Azure SDK for C++ overwrites an existing blob by default when calling Upload().
        // There is no 'Overwrite' option in UploadBlockBlobOptions; simply call Upload().
        blockBlobClient->Upload(empty);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure create block blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

std::string AzureBlobBlockBlobWriteIO::generateNextUniqueBlockId()
{
    ++blockIndex;
    if (blockIndex > maxAzureBlockCount)
        throw makeStringException(1234, "Too many blocks for Azure block blob");

    //Ensure a multiple of 4 characters are appended - so that it is a valid base64 encoding
    char blockIndexText[9];
    sprintf(blockIndexText, "%08u", blockIndex);

    return std::string(baseBlockId).append(blockIndexText);
}


size32_t AzureBlobBlockBlobWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len == 0)
        return 0;

    if (unlikely(pos != offset))
        throw makeStringExceptionV(1234, "Azure Blobs only support appending writes, unexpected write position %llu, expected %llu", pos, offset);

    file->invalidateMeta();

    CCycleTimer timer;
    std::string blockId = generateNextUniqueBlockId();
    blockIds.push_back(blockId);

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Core::IO::MemoryBodyStream content(reinterpret_cast<const uint8_t*>(data), len);
            blockBlobClient->StageBlock(blockId, content);
            offset += len;
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            attempt++;
            handleRequestException(e, "write", attempt, maxRetries, file->queryFilename(), pos, len);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "write", attempt, maxRetries, file->queryFilename(), pos, len);
        }
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

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            blockBlobClient->CommitBlockList(blockIds);
            committed = true;
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            attempt++;
            handleRequestException(e, "close", attempt, maxRetries, file->queryFilename(), offset, 0);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "close", attempt, maxRetries, file->queryFilename(), offset, 0);
        }
    }
}

//---------------------------------------------------------------------------------------------------------------------
static bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

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
            throw makeStringException(99, "Missing / in azureblob: file reference");

        StringBuffer planeName(slash-filename, filename);
        Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeName, true);
        IPropertyTree * storageapi = plane->queryPropTree("storageapi");
        if (!storageapi)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        const char * api = storageapi->queryProp("@type");
        if (!api)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        StringBuffer azureBlobAPI(strlen(azureBlobPrefix) - 1, azureBlobPrefix);
        if (!strieq(api, azureBlobAPI.str()))
            throw makeStringExceptionV(99, "Storage api for plane %s is not azureblob", planeName.str());

        useManagedIdentity = storageapi->getPropBool("@managed", false);
        //MORE: We could allow the managed identity/secret to be supplied in the configuration
        if (useManagedIdentity && !areManagedIdentitiesEnabled())
            throw makeStringExceptionV(99, "Managed identity is not enabled for this environment");

        unsigned numDevices = plane->getPropInt("@numDevices", 1);
        if (numDevices != 1)
        {
            if (slash[1] != 'd')
                throw makeStringExceptionV(99, "Expected a device number in the filename %s", fullName.str());

            char * endDevice = nullptr;
            unsigned device = strtod(slash+2, &endDevice);
            if ((device == 0) || (device > numDevices))
                throw makeStringExceptionV(99, "Device %d out of range for plane %s", device, planeName.str());

            if (!endDevice || (*endDevice != '/'))
                throw makeStringExceptionV(99, "Unexpected end of device partition %s", fullName.str());

            VStringBuffer childPath("containers[%d]", device);
            IPropertyTree * deviceInfo = storageapi->queryPropTree(childPath);
            if (deviceInfo)
            {
                accountName.set(deviceInfo->queryProp("@account"));
                secretName.set(deviceInfo->queryProp("@secret"));
            }

            //If device-specific information is not provided all defaults come from the storage plane
            if (!accountName)
                accountName.set(storageapi->queryProp("@account"));
            if (!secretName)
                secretName.set(storageapi->queryProp("@secret"));

            filename = endDevice+1;
        }
        else
        {
            accountName.set(storageapi->queryProp("@account"));
            secretName.set(storageapi->queryProp("@secret"));
            filename = slash+1;
        }

        if (isEmptyString(accountName))
            throw makeStringExceptionV(99, "Missing account name for plane %s", planeName.str());

        if (!useManagedIdentity && isEmptyString(secretName))
            throw makeStringExceptionV(99, "Missing secret name for plane %s", planeName.str());

        //I am not at all sure we need to split this apart, only to join in back together again.
        slash = strchr(filename, '/');
        if (!slash)
            throw makeStringExceptionV(99, "Missing container in azureblob: file reference '%s'", filename);

        containerName.set(filename, slash-filename);
        blobName.set(slash+1);
    }
    else
        throw makeStringExceptionV(99, "Unexpected prefix on azure filename %s", fullName.str());

    blobUrl = ::getBlobUrl(accountName, containerName, blobName);
}

std::shared_ptr<StorageSharedKeyCredential> AzureBlob::getSharedKeyCredentials() const
{
    StringBuffer key;
    getSecretValue(key, "storage", secretName, "key", true);
    //Trim trailing whitespace/newlines in case the secret has been entered by hand e.g. on bare metal
    size32_t len = key.length();
    for (;;)
    {
        if (!len)
            break;
        if (isBase64Char(key.charAt(len-1)))
            break;
        len--;
    }
    key.setLength(len);

    try
    {
        return std::make_shared<StorageSharedKeyCredential>(accountName.str(), key.str());
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(-1, "Azure access: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

std::string AzureBlob::getBlobUrl() const
{
    return blobUrl;
}

std::shared_ptr<BlobContainerClient> AzureBlob::getBlobContainerClient() const
{
    std::string blobContainerUrl = getContainerUrl(accountName, containerName);

    if (useManagedIdentity)
    {
        // For managed identity, create client without credentials
        // The Azure SDK will automatically use managed identity when no explicit credentials are provided
        // and the application is running in an Azure environment (VM, App Service, etc.)
        try
        {
            return std::make_shared<BlobContainerClient>(blobContainerUrl);
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            throw makeStringExceptionV(-1, "Azure access error: Failed to authenticate using Managed Identity. Reason: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        }
    }
    else
    {
        auto cred = getSharedKeyCredentials();
        return std::make_shared<BlobContainerClient>(blobContainerUrl, cred);
    }
}

SharedBlobClient AzureBlob::getBlobClient() const
{
    if (useManagedIdentity)
    {
        // For managed identity, create client without credentials
        // The Azure SDK will automatically use managed identity when no explicit credentials are provided
        // and the application is running in an Azure environment (VM, App Service, etc.)
        return std::make_shared<Azure::Storage::Blobs::BlockBlobClient>(getBlobUrl());
    }
    else
    {
        auto cred = getSharedKeyCredentials();
        return std::make_shared<Azure::Storage::Blobs::BlockBlobClient>(getBlobUrl(), cred);
    }
}

bool AzureBlob::createDirectory()
{
    auto blobContainerClient = getBlobContainerClient();
    try
    {
        Azure::Response<Models::CreateBlobContainerResult> result = blobContainerClient->CreateIfNotExists();
        if (result.Value.Created==false)
            DBGLOG("AzureBlob::createDirectory: container not created because it already exists");
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
    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Response<Models::BlobProperties> properties = blobClient->GetProperties();
            Models::BlobProperties & props = properties.Value;
            setProperties(props.BlobSize, props.LastModified, props.CreatedOn);
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
            {
                fileExists = false;
                fileSize = unknownFileSize;
                break;
            }
            attempt++;
            handleRequestException(e, "AzureBlob::gatherMetaData", attempt, maxRetries, queryFilename(), 0, 0);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureBlob::gatherMetaData", attempt, maxRetries, queryFilename(), 0, 0);
        }
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

//---------------------------------------------------------------------------------------------------------------------

static IFile *createAzureBlob(const char *azureFileName)
{
    return new AzureBlob(azureFileName);
}

//---------------------------------------------------------------------------------------------------------------------

class AzureAPICopyClientBase : public CInterfaceOf<IAPICopyClientOp>
{
    ApiCopyStatus status = ApiCopyStatus::NotStarted;
    virtual void doStartCopy(const char * source) = 0;
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) = 0;
    virtual void doAbortCopy() = 0;
    virtual void doDelete() = 0;

public:
    virtual void startCopy(const char * source) override
    {
        try
        {
            doStartCopy(source);
            status = ApiCopyStatus::Pending;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            IERRLOG("AzureFileClient startCopy failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
            status = ApiCopyStatus::Failed;
            throw makeStringException(MSGAUD_operator, -1, e.ReasonPhrase.c_str());
        }
    }
    virtual ApiCopyStatus getProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        dateTime.clear();
        outputLength=0;

        if (status!=ApiCopyStatus::Pending)
            return status;
        try
        {
            status = doGetProgress(dateTime, outputLength);
        }
        catch(const Azure::Core::RequestFailedException& e)
        {
            IERRLOG("Transfer using API .Poll() failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
            status = ApiCopyStatus::Failed;
        }
        return status;
    }
    virtual ApiCopyStatus abortCopy() override
    {
        if (status==ApiCopyStatus::Aborted || status==ApiCopyStatus::Failed)
            return status;
        int64_t outputLength;
        CDateTime dateTime;
        status = getProgress(dateTime, outputLength);
        switch (status)
        {
        case ApiCopyStatus::Pending:
            try
            {
                doAbortCopy();
                status = ApiCopyStatus::Aborted;
            }
            catch(const Azure::Core::RequestFailedException& e)
            {
                IERRLOG("Abort copy operation failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
                status = ApiCopyStatus::Failed;
            }
            // fallthrough to delete any file remnants
            [[fallthrough]];
        case ApiCopyStatus::Success:
            // already copied-> need to delete
            try
            {
                doDelete();
                status = ApiCopyStatus::Aborted;
            }
            catch(const Azure::Core::RequestFailedException& e)
            {
                // Ignore exceptions as file may not exist (may have been aborted already)
            }
            break;
        }
        return status;
    }
    virtual ApiCopyStatus getStatus() const override
    {
        return status;
    }
};

class AzureFileClient : public AzureAPICopyClientBase
{
    std::unique_ptr<Shares::ShareFileClient> fileClient;
    Shares::StartFileCopyOperation fileCopyOp;

    virtual void doStartCopy(const char * source) override
    {
        fileCopyOp = fileClient->StartCopy(source);
    }
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        fileCopyOp.Poll();
        Shares::Models::FileProperties props = fileCopyOp.Value();
        dateTime.setString(props.LastModified.ToString().c_str());
        outputLength = props.FileSize;
        Shares::Models::CopyStatus tstatus = props.CopyStatus.HasValue()?props.CopyStatus.Value():(Shares::Models::CopyStatus::Pending);
        if (tstatus==Shares::Models::CopyStatus::Success) // FYI. CopyStatus is an object so can't use switch statement
            return ApiCopyStatus::Success;
        else if (tstatus==Shares::Models::CopyStatus::Pending)
            return ApiCopyStatus::Pending;
        else if (tstatus==Shares::Models::CopyStatus::Aborted)
            return ApiCopyStatus::Aborted;
        return ApiCopyStatus::Failed;
    }
    virtual void doAbortCopy() override
    {
        if (fileCopyOp.HasValue() && fileCopyOp.Value().CopyId.HasValue())
            fileClient->AbortCopy(fileCopyOp.Value().CopyId.Value().c_str());
        else
            IERRLOG("AzureFileClient::AbortCopy() failed: CopyId is empty");
    }
    virtual void doDelete() override
    {
        fileClient->Delete();
    }
public:
    AzureFileClient(const char *target)
    {
        fileClient.reset(new Shares::ShareFileClient(target));
    }
};

class AzureBlobClient : public AzureAPICopyClientBase
{
    std::unique_ptr<BlobClient> blobClient;
    StartBlobCopyOperation blobCopyOp;

    virtual void doStartCopy(const char * source) override
    {
        blobCopyOp = blobClient->StartCopyFromUri(source);
    }
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        blobCopyOp.Poll();
        Models::BlobProperties props = blobCopyOp.Value();
        dateTime.setString(props.LastModified.ToString().c_str());
        outputLength = props.BlobSize;
        Blobs::Models::CopyStatus tstatus = props.CopyStatus.HasValue()?props.CopyStatus.Value():(Blobs::Models::CopyStatus::Pending);
        if (tstatus==Blobs::Models::CopyStatus::Success) // FYI. CopyStatus is an object so can't use switch statement
            return ApiCopyStatus::Success;
        else if (tstatus==Blobs::Models::CopyStatus::Pending)
            return ApiCopyStatus::Pending;
        else if (tstatus==Blobs::Models::CopyStatus::Aborted)
            return ApiCopyStatus::Aborted;
        return ApiCopyStatus::Failed;
    }
    virtual void doAbortCopy() override
    {
        if (blobCopyOp.HasValue() && blobCopyOp.Value().CopyId.HasValue())
            blobClient->AbortCopyFromUri(blobCopyOp.Value().CopyId.Value().c_str());
        else
            IERRLOG("AzureBlobClient::AbortCopy() failed: CopyId is empty");
    }
    virtual void doDelete() override
    {
        blobClient->Delete();
    }
public:
    AzureBlobClient(const char * target)
    {
        blobClient.reset(new BlobClient(target));
    }
};


class CAzureApiCopyClient : public CInterfaceOf<IAPICopyClient>
{
public:
    CAzureApiCopyClient(IStorageApiInfo *_sourceApiInfo, IStorageApiInfo *_targetApiInfo): sourceApiInfo(_sourceApiInfo), targetApiInfo(_targetApiInfo)
    {
        dbgassertex(isAzureBlob(sourceApiInfo->getStorageType())||isAzureFile(sourceApiInfo->getStorageType()));
        dbgassertex(isAzureBlob(targetApiInfo->getStorageType())||isAzureFile(targetApiInfo->getStorageType()));
    }
    virtual const char * name() const override
    {
        return "Azure API copy client";
    }
    static bool canCopy(IStorageApiInfo *_sourceApiInfo, IStorageApiInfo *_targetApiInfo)
    {
        if (_sourceApiInfo && _targetApiInfo)
        {
            const char * stSource = _sourceApiInfo->getStorageType();
            const char * stTarget = _targetApiInfo->getStorageType();
            if (stSource && stTarget)
            {
                if ((isAzureFile(stSource) || isAzureBlob(stSource))
                    && (isAzureFile(stTarget) || isAzureBlob(stTarget)))
                return true;
            }
        }
        return false;
    }
    virtual IAPICopyClientOp * startCopy(const char *srcPath, unsigned srcStripeNum,  const char *tgtPath, unsigned tgtStripeNum) const override
    {
        StringBuffer targetURI;
        getAzureURI(targetURI, tgtStripeNum,  tgtPath, targetApiInfo);
        Owned<IAPICopyClientOp> apiClient;
        if (isAzureFile(targetApiInfo->getStorageType()))
            apiClient.setown(new AzureFileClient(targetURI.str()));
        else
            apiClient.setown(new AzureBlobClient(targetURI.str()));

        StringBuffer sourceURI;
        getAzureURI(sourceURI, srcStripeNum, srcPath, sourceApiInfo);
        apiClient->startCopy(sourceURI.str());
        return apiClient.getClear();
    }
protected:
    void getAzureURI(StringBuffer & uri, unsigned stripeNum, const char *filePath, const IStorageApiInfo *apiInfo) const
    {
        const char *accountName = apiInfo->queryStorageApiAccount(stripeNum);
        uri.appendf("https://%s", accountName);

        if (isAzureFile(apiInfo->getStorageType()))
            uri.append(".file");
        else
            uri.append(".blob");
        uri.append(".core.windows.net/");

        StringBuffer tmp, token;
        const char * container = apiInfo->queryStorageContainerName(stripeNum);
        uri.appendf("%s%s%s", container, encodeURL(tmp, filePath).str(), apiInfo->getSASToken(stripeNum, token).str());
    }
    static inline bool isAzureFile(const char *storageType)
    {
        return strsame(storageType, "azurefile");
    }
    static inline bool isAzureBlob(const char *storageType)
    {
        return strsame(storageType, "azureblob");
    }

    Linked<IStorageApiInfo> sourceApiInfo, targetApiInfo;
};

//---------------------------------------------------------------------------------------------------------------------

class AzureFileHook : public CInterfaceOf<IContainedFileHook>
{
public:
    virtual IFile * createIFile(const char *fileName) override
    {
        if (isAzureBlobName(fileName))
            return createAzureBlob(fileName);
        else
            return nullptr;
    }
    virtual IAPICopyClient * getCopyApiClient(IStorageApiInfo * source, IStorageApiInfo * target) override
    {
        if (CAzureApiCopyClient::canCopy(source, target))
            return new CAzureApiCopyClient(source, target);
        return nullptr;
    }

protected:
    static bool isAzureBlobName(const char *fileName)
    {
        if (startsWith(fileName, azureBlobPrefix))
            return true;
        return false;
    }
} *azureFileHook;

extern AZURE_FILE_API void installFileHook()
{
    if (!azureFileHook)
    {
        azureFileHook = new AzureFileHook;
        addContainedFileHook(azureFileHook);
    }
}

extern AZURE_FILE_API void removeFileHook()
{
    if (azureFileHook)
    {
        removeContainedFileHook(azureFileHook);
        delete azureFileHook;
        azureFileHook = nullptr;
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    azureFileHook = nullptr;
    return true;
}

MODULE_EXIT()
{
    if (azureFileHook)
    {
        removeContainedFileHook(azureFileHook);
        ::Release(azureFileHook);
        azureFileHook = nullptr;
    }
}
