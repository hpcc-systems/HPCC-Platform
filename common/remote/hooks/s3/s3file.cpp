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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include "platform.h"
#include "jlib.hpp"
#include "jbuff.hpp"
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jstring.hpp"
#include "jlog.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jtime.hpp"
#include "jplane.hpp"
#include "jsecrets.hpp"
#include "jregexp.hpp"
#include "jutil.hpp"

#include "s3file.hpp"

#ifdef _MSC_VER
#undef GetObject
#endif


// Constants
constexpr const char* s3FilePrefix = "s3:";
constexpr size_t s3FilePrefixLen = 3;  // Length of "s3:" for pointer arithmetic
constexpr unsigned defaultMaxRetries = 3;

// Global AWS initialization with reference counting
static unsigned awsInitRefCount = 0;
static CriticalSection awsCS;
static Aws::SDKOptions awsOptions;

static void initAWS()
{
    CriticalBlock block(awsCS);
    if (awsInitRefCount == 0)
    {
        // First initialization
        awsOptions.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
        Aws::InitAPI(awsOptions);
        LOG(MCdebugProgress, "AWS SDK initialized for S3 file operations");
    }

    awsInitRefCount++;
}

static void shutdownAWS()
{
    CriticalBlock block(awsCS);
    if (awsInitRefCount == 1)
    {
        // Last reference - perform shutdown
        // IMPORTANT: All S3Clients must be destroyed before this point
        Aws::ShutdownAPI(awsOptions);
        LOG(MCdebugProgress, "AWS SDK shutdown for S3 file operations");
    }

    assertex(awsInitRefCount > 0);
    awsInitRefCount--;
}

// S3 Client cache key based on plane and device
struct S3ClientKey
{
    std::string planeName;
    unsigned device;

    bool operator==(const S3ClientKey& other) const
    {
        return (planeName == other.planeName) && (device == other.device);
    }
};

// Hash specialization for S3ClientKey
namespace std {
    template<>
    struct hash<S3ClientKey>
    {
        size_t operator()(const S3ClientKey& key) const noexcept
        {
            unsigned h = fnvInitialHash32;
            if (key.planeName.c_str())
                h = hashc((const unsigned char*)key.planeName.c_str(), key.planeName.length(), h);
            h = hashvalue(key.device, h);
            return h;
        }
    };
}

class S3ClientManager
{
private:
    mutable CriticalSection cs;
    std::unordered_map<S3ClientKey, std::unique_ptr<Aws::S3::S3Client>> clients;
    bool initialized = false;

public:
    Aws::S3::S3Client& getClient(const char* planeName, unsigned device)
    {
        CriticalBlock block(cs);
        if (!initialized)
        {
            initAWS();
            initialized = true;
        }

        S3ClientKey key;
        key.planeName = planeName;
        key.device = device;

        if (auto it = clients.find(key); it != clients.end())
        {
            return *(it->second);
        }
        else
        {
            std::unique_ptr<Aws::S3::S3Client> client = createClient(planeName, device);
            Aws::S3::S3Client& ref = *client;
            clients.emplace(key, std::move(client));
            return ref;
        }
    }

    void cleanup()
    {
        CriticalBlock block(cs);
        if (initialized)
        {
            // Destroy clients before shutting down AWS SDK
            clients.clear();
            shutdownAWS();
            initialized = false;
        }
    }

    ~S3ClientManager()
    {
        cleanup();
    }

private:
    std::unique_ptr<Aws::S3::S3Client> createClient(const char* planeName, unsigned device)
    {
        // Load plane configuration
        Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeName, true);
        const IPropertyTree * storageapi = plane->queryPropTree("storageapi");
        if (!storageapi)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName);
        const char *type = storageapi->queryProp("@type");
        if (!type)
            throw makeStringExceptionV(99, "No storage api type defined for plane %s", planeName);
        if (!strieq(type, "s3"))
            throw makeStringExceptionV(99, "Storage api type for plane %s is not s3", planeName);

        // Get bucket configuration by device index
        VStringBuffer childPath("buckets[%u]", device);
        const IPropertyTree * bucketInfo = storageapi->queryPropTree(childPath);
        if (!bucketInfo)
            throw makeStringExceptionV(99, "Missing bucket specification for device %u in plane %s", device, planeName);

        // Build AWS client configuration
        Aws::Client::ClientConfiguration clientConfig;

        const char* regionStr = storageapi->queryProp("@region");
        if (regionStr && !isEmptyString(regionStr))
            clientConfig.region = regionStr;

        const char* endpointStr = storageapi->queryProp("@endpoint");
        if (endpointStr && !isEmptyString(endpointStr))
            clientConfig.endpointOverride = endpointStr;

        clientConfig.scheme = storageapi->getPropBool("@useSSL", true) ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
        clientConfig.connectTimeoutMs = storageapi->getPropInt("@timeoutMs", 30000);
        clientConfig.requestTimeoutMs = clientConfig.connectTimeoutMs * 2;
        clientConfig.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(storageapi->getPropInt("@maxRetries", defaultMaxRetries));

        // Use secret-based credentials if provided, otherwise use default credential chain
        const char* secretName = bucketInfo->queryProp("@secret");

        if (secretName && !isEmptyString(secretName))
        {
            StringBuffer accessKey, keyId;
            getSecretValue(accessKey, "storage", secretName, "aws-access-key", true);
            getSecretValue(keyId, "storage", secretName, "aws-key-id", true);

            auto credentials = Aws::Auth::AWSCredentials(keyId.str(), accessKey.str());
            return std::make_unique<Aws::S3::S3Client>(
                credentials, 
                clientConfig, 
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent, 
                true,  // useVirtualAddressing
                Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET);
        }
        else
        {
            // Environment variables or ~/.aws/credentials
            auto credentialsProvider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
            return std::make_unique<Aws::S3::S3Client>(
                credentialsProvider, 
                clientConfig, 
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent, 
                true,  // useVirtualAddressing
                Aws::S3::US_EAST_1_REGIONAL_ENDPOINT_OPTION::NOT_SET);
        }
    }
};

static S3ClientManager& getS3ClientManager()
{
    static S3ClientManager manager = S3ClientManager();
    return manager;
}


// Utility functions
static void logAwsError(const char* operation, const Aws::Client::AWSError<Aws::S3::S3Errors>& error, const char* bucket, const char* key)
{
    ERRLOG("S3 %s failed for s3://%s/%s: %s - %s",
           operation, bucket, key,
           error.GetExceptionName().c_str(),
           error.GetMessage().c_str());
}

static void handleRequestBackoff(const char* message, unsigned attempt, unsigned maxRetries)
{
    OWARNLOG("%s", message);

    if (attempt >= maxRetries)
        throw makeStringException(-1, message);

    // Exponential backoff with jitter
    unsigned backoffMs = (1U << attempt) * 100 + (rand() % 100);
    Sleep(backoffMs);
}

static void handleRequestException(const Aws::Client::AWSError<Aws::S3::S3Errors>& e, const char* op, unsigned attempt, unsigned maxRetries, const char* filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s - %s",
                      op, attempt, maxRetries, filename,
                      e.GetExceptionName().c_str(), e.GetMessage().c_str());

    handleRequestBackoff(msg, attempt, maxRetries);
}

static void handleRequestException(const std::exception& e, const char* op, unsigned attempt, unsigned maxRetries, const char* filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s",
                      op, attempt, maxRetries, filename, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}

// Execute an S3 SDK call with retry/backoff, translating failures to IException
// (via handleRequestException) rather than std::exception. Returns the successful outcome.
template <typename Fn>
static auto retryS3Op(const char* op, const char* context, Fn&& fn) -> decltype(fn())
{
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            auto outcome = fn();
            if (outcome.IsSuccess())
                return outcome;
            attempt++;
            handleRequestException(outcome.GetError(), op, attempt, defaultMaxRetries, context);
        }
        catch (IException *)
        {
            throw;
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, op, attempt, defaultMaxRetries, context);
        }
    }
}

//---------------------------------------------------------------------------------------------------------------------
// Forward declarations
class S3File;

//---------------------------------------------------------------------------------------------------------------------
// S3FileReadIO implementation
class S3FileReadIO : implements CInterfaceOf<IFileIO>
{
private:
    Linked<S3File> file;
    FileIOStats stats;
    CriticalSection ioCS;
    offset_t cachedFileSize;

public:
    S3FileReadIO(S3File* _file);

    // IFileIO interface
    virtual size32_t read(offset_t pos, size32_t len, void* data) override;
    virtual offset_t size() override;
    virtual void close() override { /* No-op for read operations */ }
    virtual void flush() override { /* No-op for read operations */ }

    // Not implemented for read-only file
    virtual size32_t write(offset_t pos, size32_t len, const void* data) override
    {
        throwUnexpected();
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpected();
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;
    virtual IFile* queryFile() const override;

private:
    size32_t readFromS3(offset_t pos, size32_t len, void* data);
};

//---------------------------------------------------------------------------------------------------------------------
// Multipart upload helper
class S3MultipartUpload
{
private:
    StringAttr planeName;
    unsigned device;
    StringAttr bucket;
    StringAttr key;
    StringAttr uploadId;
    StringBuffer fullPath;
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    unsigned partNumber = 1;
    bool active = false;

public:
    S3MultipartUpload(const char* _planeName, unsigned _device, const char* _bucket, const char* _key)
        : planeName(_planeName), device(_device), bucket(_bucket), key(_key)
    {
        fullPath.appendf("s3:%s/%s", _planeName, _key);
    }

    ~S3MultipartUpload()
    {
        if (!active)
            return;
        // abort() can throw (via retryS3Op) after exhausting retries; a throw must
        // never escape a destructor, so swallow and log any failure here.
        try
        {
            abort();
        }
        catch (IException* e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            ERRLOG("Failed to abort S3 multipart upload during cleanup: %s", msg.str());
            e->Release();
        }
        catch (...)
        {
            ERRLOG("Unknown exception aborting S3 multipart upload during cleanup");
        }
    }

    bool initiate();
    bool uploadPart(const void* data, size32_t len);
    bool complete();
    bool abort();

private:
    Aws::S3::S3Client& getClient() { return getS3ClientManager().getClient(planeName.str(), device); }
};

//---------------------------------------------------------------------------------------------------------------------
// S3FileWriteIO implementation
class S3FileWriteIO : implements CInterfaceOf<IFileIO>
{
private:
    Linked<S3File> file;
    FileIOStats stats;
    std::unique_ptr<S3MultipartUpload> multipartUpload;
    CriticalSection ioCS;
    bool closed = false;
    offset_t currentPos = 0;
    MemoryBuffer partBuffer;
    // S3 multipart requires every non-final part to be at least 5MB.  The engine
    // issues writes well below that (typically ~1MB blocks), so we coalesce them
    // into >=minPartSize parts before uploading.  The final part, flushed on
    // close(), is allowed to be smaller.
    static constexpr size32_t minPartSize = 8 * 1024 * 1024; // 8MB

    void flushPart();

public:
    S3FileWriteIO(S3File* _file);
    virtual void beforeDispose() override;

    // IFileIO interface
    virtual size32_t write(offset_t pos, size32_t len, const void* data) override;
    virtual void close() override;
    virtual void flush() override;
    virtual void setSize(offset_t size) override { /* Not supported for S3 */ }

    // Not implemented for write-only file
    virtual size32_t read(offset_t pos, size32_t len, void* data) override
    {
        throwUnexpected();
    }
    virtual offset_t size() override
    {
        throwUnexpected();
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;
    virtual IFile* queryFile() const override;
};

//---------------------------------------------------------------------------------------------------------------------
// S3File implementation
class S3File : implements CInterfaceOf<IFile>
{
    friend class S3FileReadIO;
    friend class S3FileWriteIO;
    friend class S3DirectoryIterator;

private:
    StringBuffer fullName;
    StringBuffer planeName;
    StringBuffer bucketName;
    StringBuffer keyName;
    unsigned device = 1;

    // Cached metadata
    mutable CriticalSection metaCS;
    mutable bool haveMeta = false;
    mutable bool fileExists = false;
    mutable bool isDir = false;
    mutable offset_t fileSize = unknownFileSize;
    mutable time_t modifiedTime = 0;

public:
    S3File(const char* s3FileName);

    // IFile interface - query methods
    virtual const char* queryFilename() override { return fullName.str(); }
    virtual bool exists() override;
    virtual fileBool isDirectory() override;
    virtual fileBool isFile() override;
    virtual fileBool isReadOnly() override { return fileBool::foundYes; } // S3 files are read-only via this interface
    virtual offset_t size() override;
    virtual bool getTime(CDateTime* createTime, CDateTime* modifiedTime, CDateTime* accessedTime) override;
    virtual bool getInfo(bool& isdir, offset_t& size, CDateTime& modtime) override;

    // IFile interface - I/O operations
    virtual IFileIO* open(IFOmode mode, IFEflags extraFlags = IFEnone) override;
    virtual IFileAsyncIO* openAsync(IFOmode mode) override { UNIMPLEMENTED; }
    virtual IFileIO* openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags = IFEnone) override;

    // IFile interface - modification operations
    virtual bool remove() override;
    virtual bool createDirectory() override;

    // Not implemented operations
    virtual bool setTime(const CDateTime* createTime, const CDateTime* modifiedTime, const CDateTime* accessedTime) override { UNIMPLEMENTED; }
    virtual void rename(const char* newTail) override { UNIMPLEMENTED; }
    virtual void move(const char* newName) override { UNIMPLEMENTED; }
    virtual void setReadOnly(bool ro) override { UNIMPLEMENTED; }
    virtual void setFilePermissions(unsigned fPerms) override { UNIMPLEMENTED; }
    virtual bool setCompression(bool set) override { UNIMPLEMENTED; }
    virtual offset_t compressedSize() override { UNIMPLEMENTED; }
    virtual unsigned getCRC() override { UNIMPLEMENTED; }
    virtual void setCreateFlags(unsigned short cflags) override { UNIMPLEMENTED; }
    virtual void setShareMode(IFSHmode shmode) override { UNIMPLEMENTED; }
    virtual IDirectoryIterator* directoryFiles(const char* mask, bool sub, bool includeDirs) override;
    virtual IDirectoryDifferenceIterator* monitorDirectory(IDirectoryIterator* prev, const char* mask, bool sub, bool includedirs, unsigned checkinterval, unsigned timeout, Semaphore* abortsem) override { UNIMPLEMENTED; }
    virtual void copySection(const RemoteFilename& dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress* progress, CFflags copyFlags) override { UNIMPLEMENTED; }
    virtual void copyTo(IFile* dest, size32_t buffersize, ICopyFileProgress* progress, bool usetmp, CFflags copyFlags) override { UNIMPLEMENTED; }
    virtual IMemoryMappedFile* openMemoryMapped(offset_t ofs, memsize_t len, bool write) override { UNIMPLEMENTED; }

protected:
    void ensureMetadata() const;
    void gatherMetadata() const;
    void invalidateMeta() { CriticalBlock block(metaCS); haveMeta = false; }
    Aws::S3::S3Client& getClient() const { return getS3ClientManager().getClient(planeName.str(), device); }
};

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3FileReadIO

S3FileReadIO::S3FileReadIO(S3File* _file)
    : file(_file), cachedFileSize(_file->size())
{
}

size32_t S3FileReadIO::read(offset_t pos, size32_t len, void* data)
{
    if (pos >= cachedFileSize)
        return 0;

    if (pos + len > cachedFileSize)
        len = (size32_t)(cachedFileSize - pos);

    if (len == 0)
        return 0;

    CriticalBlock block(ioCS);

    size32_t bytesRead = readFromS3(pos, len, data);
    stats.ioReads++;
    stats.ioReadBytes += bytesRead;
    return bytesRead;
}

size32_t S3FileReadIO::readFromS3(offset_t pos, size32_t len, void* data)
{
    const char* filename = file->queryFilename();

    CCycleTimer timer;

    auto outcome = retryS3Op("S3File::read", filename, [&]()
    {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(file->bucketName.str());
        request.SetKey(file->keyName.str());

        // Only set range header for partial reads
        if (pos > 0 || len != cachedFileSize)
        {
            StringBuffer range;
            range.appendf("bytes=%llu-%llu", (unsigned long long)pos, (unsigned long long)(pos + len - 1));
            request.SetRange(range.str());
        }

        return file->getClient().GetObject(request);
    });

    auto& body = outcome.GetResult().GetBody();
    body.read((char*)data, len);
    size32_t bytesRead = (size32_t)body.gcount();

    stats.ioReadCycles += timer.elapsedCycles();
    return bytesRead;
}

offset_t S3FileReadIO::size()
{
    return file->size();
}

unsigned __int64 S3FileReadIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

IFile* S3FileReadIO::queryFile() const
{
    return file.get();
}

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3MultipartUpload

bool S3MultipartUpload::initiate()
{
    // Reserve space for estimated parts (assume average 200 parts for large files)
    completedParts.reserve(200);

    auto outcome = retryS3Op("S3File::initiate", fullPath.str(), [&]()
    {
        Aws::S3::Model::CreateMultipartUploadRequest request;
        request.SetBucket(bucket.str());
        request.SetKey(key.str());
        return getClient().CreateMultipartUpload(request);
    });

    uploadId.set(outcome.GetResult().GetUploadId().c_str());
    active = true;
    return true;
}

bool S3MultipartUpload::uploadPart(const void* data, size32_t len)
{
    if (!active)
        return false;

    auto outcome = retryS3Op("S3File::uploadPart", fullPath.str(), [&]()
    {
        Aws::S3::Model::UploadPartRequest request;
        request.SetBucket(bucket.str());
        request.SetKey(key.str());
        request.SetUploadId(uploadId.str());
        request.SetPartNumber(partNumber);

        Aws::Utils::Stream::PreallocatedStreamBuf buf((unsigned char*)data, len);
        request.SetBody(std::make_shared<Aws::IOStream>(&buf));

        return getClient().UploadPart(request);
    });

    Aws::S3::Model::CompletedPart completedPart;
    completedPart.SetPartNumber(partNumber);
    completedPart.SetETag(outcome.GetResult().GetETag());
    completedParts.push_back(completedPart);
    partNumber++;
    return true;
}

bool S3MultipartUpload::complete()
{
    if (!active)
        return false;

    retryS3Op("S3File::complete", fullPath.str(), [&]()
    {
        Aws::S3::Model::CompletedMultipartUpload completedUpload;
        completedUpload.SetParts(completedParts);

        Aws::S3::Model::CompleteMultipartUploadRequest request;
        request.SetBucket(bucket.str());
        request.SetKey(key.str());
        request.SetUploadId(uploadId.str());
        request.SetMultipartUpload(completedUpload);

        return getClient().CompleteMultipartUpload(request);
    });

    active = false;
    return true;
}

bool S3MultipartUpload::abort()
{
    if (!active)
        return true;

    active = false;
    retryS3Op("S3File::abort", fullPath.str(), [&]()
    {
        Aws::S3::Model::AbortMultipartUploadRequest request;
        request.SetBucket(bucket.str());
        request.SetKey(key.str());
        request.SetUploadId(uploadId.str());
        return getClient().AbortMultipartUpload(request);
    });
    return true;
}

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3FileWriteIO

S3FileWriteIO::S3FileWriteIO(S3File* _file)
    : file(_file)
{
}

void S3FileWriteIO::beforeDispose()
{
    try
    {
        close();
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        ERRLOG("Exception during S3 file disposal: %s", msg.str());
        e->Release();
    }
    catch (...)
    {
        ERRLOG("Unknown exception during S3 file disposal for %s", file->queryFilename());
    }
}

size32_t S3FileWriteIO::write(offset_t pos, size32_t len, const void* data)
{
    if (closed)
        throw makeStringException(-1, "Attempt to write to closed S3 file");

    if (len == 0)
        return 0;

    CriticalBlock block(ioCS);

    // For simplicity, require sequential writes
    if (pos != currentPos)
        throw makeStringException(-1, "S3 file writer only supports sequential writes");

    file->invalidateMeta();

    CCycleTimer timer;

    // Coalesce writes into >=minPartSize multipart parts.  S3 rejects any non-final
    // part smaller than 5MB and the engine writes in much smaller blocks, so buffer
    // until a full part is available before uploading.
    partBuffer.append(len, data);
    currentPos += len;
    if (partBuffer.length() >= minPartSize)
        flushPart();

    stats.ioWrites++;
    stats.ioWriteBytes += len;
    stats.ioWriteCycles += timer.elapsedCycles();
    return len;
}

void S3FileWriteIO::flushPart()
{
    if (partBuffer.length() == 0)
        return;

    // Initiate the multipart upload lazily, on the first part to be uploaded.
    if (!multipartUpload)
    {
        multipartUpload = std::make_unique<S3MultipartUpload>(file->planeName.str(), file->device, file->bucketName.str(), file->keyName.str());
        if (!multipartUpload->initiate())
            throw makeStringException(-1, "Failed to initiate multipart upload");
    }

    if (!multipartUpload->uploadPart(partBuffer.bytes(), partBuffer.length()))
        throw makeStringException(-1, "Failed to upload part to S3");

    partBuffer.clear();
}

void S3FileWriteIO::flush()
{
    CriticalBlock block(ioCS);
    // Intentionally a no-op: flushing partial data mid-stream could produce a
    // sub-5MB non-final part, which S3 multipart rejects.  Buffered data is
    // uploaded once it reaches minPartSize, and the remainder on close().
}

void S3FileWriteIO::close()
{
    if (closed)
        return;

    CriticalBlock block(ioCS);

    // Flush any remaining buffered data as the final part (may be <5MB, which
    // S3 permits for the last part of a multipart upload).
    if (partBuffer.length() > 0)
        flushPart();

    if (multipartUpload)
    {
        if (!multipartUpload->complete())
            throw makeStringException(-1, "Failed to complete multipart upload");
        multipartUpload.reset();
    }

    closed = true;
}



unsigned __int64 S3FileWriteIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

IFile* S3FileWriteIO::queryFile() const
{
    return file.get();
}

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3File

S3File::S3File(const char* s3FileName)
    : fullName(s3FileName)
{
    if (!startsWith(fullName, s3FilePrefix))
        throw makeStringExceptionV(99, "Unexpected prefix on S3 filename %s", fullName.str());

    //format is s3:plane[/device]/path
    const char * filename = fullName.str() + strlen(s3FilePrefix);
    const char * slash = strchr(filename, '/');
    if (!slash)
        throw makeStringException(99, "Missing / in s3: file reference");

    planeName.append(slash-filename, filename);
    Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeName, true);
    filename = slash+1; // advance past slash

    unsigned numDevices = plane->getPropInt("@numDevices", 1);
    if (numDevices != 1)
    {
        //The device from the path is used to identify which device is in use
        //but it is then stripped from the path
        if (filename[0] != 'd')
            throw makeStringExceptionV(99, "Expected a device number in the filename %s", fullName.str());

        char * endDevice = nullptr;
        device = strtol(filename+1, &endDevice, 10);
        if ((device == 0) || (device > numDevices))
            throw makeStringExceptionV(99, "Device %d out of range for plane %s", device, planeName.str());

        if (!endDevice || (*endDevice != '/'))
            throw makeStringExceptionV(99, "Unexpected end of device partition %s", fullName.str());

        filename = endDevice+1;
    }

    getClient(); // validate plane and device

    VStringBuffer childPath("storageapi/buckets[%u]", device);
    const char * bucket = plane->queryPropTree(childPath)->queryProp("@name");
    if (isEmptyString(bucket))
        throw makeStringExceptionV(99, "Missing bucket name for plane %s", planeName.str());

    bucketName.set(bucket);
    keyName.set(filename);
}

bool S3File::exists()
{
    ensureMetadata();
    return fileExists;
}

fileBool S3File::isDirectory()
{
    ensureMetadata();
    if (!fileExists)
        return fileBool::notFound;
    return isDir ? fileBool::foundYes : fileBool::foundNo;
}

fileBool S3File::isFile()
{
    ensureMetadata();
    if (!fileExists)
        return fileBool::notFound;
    return !isDir ? fileBool::foundYes : fileBool::foundNo;
}

offset_t S3File::size()
{
    ensureMetadata();
    return fileSize;
}

bool S3File::getTime(CDateTime* createTime, CDateTime* modifiedTime, CDateTime* accessedTime)
{
    ensureMetadata();
    if (createTime)
        createTime->set(this->modifiedTime);
    if (modifiedTime)
        modifiedTime->set(this->modifiedTime);
    if (accessedTime)
        accessedTime->clear();
    return fileExists;
}

bool S3File::getInfo(bool& isdir, offset_t& size, CDateTime& modtime)
{
    ensureMetadata();
    isdir = this->isDir;
    size = fileSize;
    modtime.set(this->modifiedTime);
    return fileExists;
}

IFileIO* S3File::open(IFOmode mode, IFEflags extraFlags)
{
    switch (mode)
    {
        case IFOread:
            if (!exists())
                return nullptr;
            return new S3FileReadIO(this);
        case IFOcreate:
        case IFOwrite:
            return new S3FileWriteIO(this);
        default:
            throw makeStringException(-1, "Unsupported file open mode for S3 file");
    }
}

IFileIO* S3File::openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags)
{
    return open(mode, extraFlags); // S3 files are inherently shared
}

bool S3File::remove()
{
    try
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucketName.str());
        request.SetKey(keyName.str());

        auto outcome = getClient().DeleteObject(request);
        if (outcome.IsSuccess())
        {
            CriticalBlock block(metaCS);
            haveMeta = true;
            fileExists = false;
            fileSize = 0;
            return true;
        }
        else
        {
            logAwsError("DeleteObject", outcome.GetError(), bucketName.str(), keyName.str());
            return false;
        }
    }
    catch (const std::exception& e)
    {
        ERRLOG("Exception in S3 file deletion: %s", e.what());
        return false;
    }
}

bool S3File::createDirectory()
{
    // For S3, we don't need to create directory markers explicitly
    // S3 is a flat namespace where directories are just key prefixes
    // Directory markers are optional and not required for file operations
    // When we write the file, S3 will automatically handle the key structure

    return true;
}

void S3File::ensureMetadata() const
{
    CriticalBlock block(metaCS);
    if (haveMeta)
        return;
    gatherMetadata();
}

void S3File::gatherMetadata() const
{
    // Not routed through retryS3Op: HeadObject treats NO_SUCH_KEY / RESOURCE_NOT_FOUND
    // as "file does not exist" (a normal result) rather than a retryable failure.
    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            Aws::S3::Model::HeadObjectRequest request;
            request.SetBucket(bucketName.str());
            request.SetKey(keyName.str());

            auto outcome = getClient().HeadObject(request);
            if (outcome.IsSuccess())
            {
                fileExists = true;
                fileSize = outcome.GetResult().GetContentLength();
                modifiedTime = outcome.GetResult().GetLastModified().Seconds();
                isDir = false; // S3 objects are not directories in the traditional sense
                break;
            }
            else
            {
                auto& error = outcome.GetError();
                if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
                    error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND)
                {
                    // Object not found - not an error to retry
                    fileExists = false;
                    fileSize = 0;
                    modifiedTime = 0;
                    isDir = false;
                    break;
                }
                attempt++;
                handleRequestException(error, "S3File::gatherMetaData", attempt, defaultMaxRetries, fullName.str());
            }
        }
        catch (const IException& e)
        {
            throw; // Re-throw IException from handleRequestException
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::gatherMetaData", attempt, defaultMaxRetries, fullName.str());
        }
    }

    haveMeta = true;
}

//---------------------------------------------------------------------------------------------------------------------
// S3DirectoryIterator - lists objects under a prefix via ListObjectsV2

class S3DirectoryIterator : implements IDirectoryIterator, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    S3DirectoryIterator(S3File & _owner, const char * _mask, bool _sub, bool _includeDirs)
        : owner(&_owner), mask(_mask), sub(_sub), includeDirs(_includeDirs) {}

    virtual bool first() override { index = 0; entries.kill(); fetchEntries(); return isValid(); }
    virtual bool next() override { index++; return isValid(); }
    virtual bool isValid() override { return index < entries.ordinality(); }

    virtual IFile & query() override
    {
        Entry & e = entries.item(index);
        if (!e.file)
        {
            StringBuffer path;
            path.append(s3FilePrefix).append(owner->planeName).append("/").append(e.key);
            e.file.setown(createS3File(path.str()));
        }
        return *e.file;
    }

    virtual StringBuffer & getName(StringBuffer & buf) override
    {
        const char * key = entries.item(index).key.str();
        size_t keyLen = strlen(key);
        if (keyLen > 0 && key[keyLen - 1] == '/')
            keyLen--;
        const char * slash = nullptr;
        for (size_t i = keyLen; i > 0; i--)
            if (key[i - 1] == '/') { slash = key + i - 1; break; }
        return slash ? buf.append(keyLen - (slash + 1 - key), slash + 1) : buf.append(keyLen, key);
    }

    virtual bool isDir() override { return entries.item(index).isDir; }
    virtual __int64 getFileSize() override { return entries.item(index).size; }
    virtual bool getModifiedTime(CDateTime & ret) override
    {
        ret.clear();
        time_t t = entries.item(index).modified;
        if (t) ret.set(t);
        return t != 0;
    }

private:
    struct Entry : public CInterface
    {
        StringAttr key;
        Owned<IFile> file;
        offset_t size = 0;
        time_t modified = 0;
        bool isDir = false;
    };

    void fetchEntries()
    {
        StringBuffer prefix(owner->keyName);
        if (prefix.length() && prefix.charAt(prefix.length() - 1) != '/')
            prefix.append('/');

        VStringBuffer listCtx("s3://%s/%s", owner->bucketName.str(), prefix.str());
        Aws::String continuationToken;
        bool hasMore = true;
        while (hasMore)
        {
            Aws::S3::Model::ListObjectsV2Request request;
            request.SetBucket(owner->bucketName.str());
            request.SetPrefix(prefix.str());
            if (!sub) request.SetDelimiter("/");
            if (!continuationToken.empty()) request.SetContinuationToken(continuationToken);

            auto outcome = retryS3Op("S3File::directoryFiles", listCtx.str(), [&]()
            {
                return owner->getClient().ListObjectsV2(request);
            });

            auto & result = outcome.GetResult();
            for (auto & obj : result.GetContents())
            {
                const Aws::String & key = obj.GetKey();
                if (key.length() == (size_t)prefix.length()) continue;
                const char * name = key.c_str() + prefix.length();
                if (mask.length() && !WildMatch(name, mask, false)) continue;
                Entry * e = new Entry;
                e->key.set(key.c_str());
                e->size = obj.GetSize();
                e->modified = obj.GetLastModified().Seconds();
                entries.append(*e);
            }
            if (includeDirs && !sub)
            {
                for (auto & cp : result.GetCommonPrefixes())
                {
                    Entry * e = new Entry;
                    e->key.set(cp.GetPrefix().c_str());
                    e->isDir = true;
                    entries.append(*e);
                }
            }
            hasMore = result.GetIsTruncated();
            continuationToken = result.GetNextContinuationToken();
        }
    }

    Linked<S3File> owner;
    StringAttr mask;
    bool sub;
    bool includeDirs;
    unsigned index = 0;
    CIArrayOf<Entry> entries;
};

IDirectoryIterator * S3File::directoryFiles(const char * mask, bool sub, bool includeDirs)
{
    return new S3DirectoryIterator(*this, mask ? mask : "", sub, includeDirs);
}

//---------------------------------------------------------------------------------------------------------------------
// File hook implementation


extern S3FILE_API IFile *createS3File(const char *s3FileName)
{
    return new S3File(s3FileName);
}

extern S3FILE_API bool isS3FileName(const char *fileName)
{
    if (!fileName || !startsWith(fileName, s3FilePrefix))
        return false;

    const char *planeName = fileName + s3FilePrefixLen;
    const char *slash = strchr(planeName, '/');
    // Require:
    // - a non-empty plane name (slash not at the start)
    // - a slash separating plane name from path
    // - content after the slash
    return (slash != nullptr && slash != planeName && *(slash + 1) != '\0');
}

//---------------------------------------------------------------------------------------------------------------------
// S3 API copy client - server-side copy via CopyObject, or multipart UploadPartCopy for large objects

static bool isS3Type(const char * type) { return type && strieq(type, "s3"); }

static constexpr offset_t maxSingleCopySize = (offset_t)5 * 1024 * 1024 * 1024;  // 5GB - S3 single CopyObject limit
static constexpr offset_t copyPartSize = (offset_t)500 * 1024 * 1024;             // 500MB per copy part

class S3APICopyClientOp : public CInterfaceOf<IAPICopyClientOp>
{
public:
    S3APICopyClientOp(const char * _srcBucket, const char * _srcKey,
                      const char * _tgtBucket, const char * _tgtKey,
                      const char * _srcPlane, unsigned _srcDevice,
                      const char * _tgtPlane, unsigned _tgtDevice)
        : srcBucket(_srcBucket), srcKey(_srcKey), tgtBucket(_tgtBucket), tgtKey(_tgtKey),
          srcPlane(_srcPlane), tgtPlane(_tgtPlane), srcDevice(_srcDevice), tgtDevice(_tgtDevice) {}

    virtual void startCopy(const char * source) override
    {
        try
        {
            auto outcome = retryS3Op("S3::HeadObject", srcKey.str(), [&]()
            {
                Aws::S3::Model::HeadObjectRequest headReq;
                headReq.SetBucket(srcBucket.str());
                headReq.SetKey(srcKey.str());
                return getSrcClient().HeadObject(headReq);
            });
            srcSize = outcome.GetResult().GetContentLength();
            if (srcSize <= maxSingleCopySize)
                doSimpleCopy();
            else
                doMultipartCopy();
            status = ApiCopyStatus::Success;
        }
        catch (...)
        {
            status = ApiCopyStatus::Failed;
            throw;
        }
    }

    virtual ApiCopyStatus getProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        dateTime.clear();
        outputLength = (status == ApiCopyStatus::Success) ? srcSize : 0;
        return status;
    }
    virtual ApiCopyStatus abortCopy() override { status = ApiCopyStatus::Aborted; return status; }
    virtual ApiCopyStatus getStatus() const override { return status; }

private:
    void doSimpleCopy()
    {
        StringBuffer rawSource, copySource;
        rawSource.appendf("%s/%s", srcBucket.str(), srcKey.str());
        encodeURL(copySource, rawSource.str());
        retryS3Op("S3::CopyObject", copySource.str(), [&]()
        {
            Aws::S3::Model::CopyObjectRequest request;
            request.SetBucket(tgtBucket.str());
            request.SetKey(tgtKey.str());
            request.SetCopySource(copySource.str());
            return getTgtClient().CopyObject(request);
        });
    }

    void doMultipartCopy()
    {
        StringBuffer rawSource, copySource;
        rawSource.appendf("%s/%s", srcBucket.str(), srcKey.str());
        encodeURL(copySource, rawSource.str());

        auto initOutcome = retryS3Op("S3::CreateMultipartUpload", tgtKey.str(), [&]()
        {
            Aws::S3::Model::CreateMultipartUploadRequest initReq;
            initReq.SetBucket(tgtBucket.str());
            initReq.SetKey(tgtKey.str());
            return getTgtClient().CreateMultipartUpload(initReq);
        });
        Aws::String uploadId = initOutcome.GetResult().GetUploadId();

        Aws::Vector<Aws::S3::Model::CompletedPart> parts;
        try
        {
            unsigned partNum = 1;
            for (offset_t pos = 0; pos < srcSize; pos += copyPartSize, partNum++)
            {
                offset_t end = std::min(pos + copyPartSize - 1, srcSize - 1);
                VStringBuffer range("bytes=%llu-%llu", (unsigned long long)pos, (unsigned long long)end);
                auto partOutcome = retryS3Op("S3::UploadPartCopy", copySource.str(), [&]()
                {
                    Aws::S3::Model::UploadPartCopyRequest partReq;
                    partReq.SetBucket(tgtBucket.str());
                    partReq.SetKey(tgtKey.str());
                    partReq.SetUploadId(uploadId);
                    partReq.SetPartNumber(partNum);
                    partReq.SetCopySource(copySource.str());
                    partReq.SetCopySourceRange(range.str());
                    return getTgtClient().UploadPartCopy(partReq);
                });
                Aws::S3::Model::CompletedPart cp;
                cp.SetPartNumber(partNum);
                cp.SetETag(partOutcome.GetResult().GetCopyPartResult().GetETag());
                parts.push_back(cp);
            }
            retryS3Op("S3::CompleteMultipartUpload", tgtKey.str(), [&]()
            {
                Aws::S3::Model::CompletedMultipartUpload completed;
                completed.SetParts(parts);
                Aws::S3::Model::CompleteMultipartUploadRequest completeReq;
                completeReq.SetBucket(tgtBucket.str());
                completeReq.SetKey(tgtKey.str());
                completeReq.SetUploadId(uploadId);
                completeReq.SetMultipartUpload(completed);
                return getTgtClient().CompleteMultipartUpload(completeReq);
            });
        }
        catch (...)
        {
            // Best-effort abort of the incomplete multipart upload
            Aws::S3::Model::AbortMultipartUploadRequest abortReq;
            abortReq.SetBucket(tgtBucket.str());
            abortReq.SetKey(tgtKey.str());
            abortReq.SetUploadId(uploadId);
            getTgtClient().AbortMultipartUpload(abortReq);
            throw;
        }
    }

    Aws::S3::S3Client & getSrcClient() { return getS3ClientManager().getClient(srcPlane.str(), srcDevice); }
    Aws::S3::S3Client & getTgtClient() { return getS3ClientManager().getClient(tgtPlane.str(), tgtDevice); }

    StringAttr srcBucket, srcKey, tgtBucket, tgtKey, srcPlane, tgtPlane;
    unsigned srcDevice, tgtDevice;
    offset_t srcSize = 0;
    ApiCopyStatus status = ApiCopyStatus::NotStarted;
};

class S3APICopyClient : public CInterfaceOf<IAPICopyClient>
{
    Linked<IStorageApiInfo> source, target;
public:
    S3APICopyClient(IStorageApiInfo * _source, IStorageApiInfo * _target)
        : source(_source), target(_target) {}
    virtual const char * name() const override { return "S3 API copy client"; }
    virtual IAPICopyClientOp * startCopy(const char * srcPath, unsigned srcStripeNum,
                                         const char * tgtPath, unsigned tgtStripeNum) const override
    {
        Owned<S3APICopyClientOp> op = new S3APICopyClientOp(
            source->queryStorageContainerName(srcStripeNum), srcPath,
            target->queryStorageContainerName(tgtStripeNum), tgtPath,
            source->queryPlaneName(), srcStripeNum,
            target->queryPlaneName(), tgtStripeNum);
        op->startCopy(srcPath);
        return op.getClear();
    }
};

class S3FileHook : public CInterfaceOf<IContainedFileHook>
{
public:
    virtual IFile* createIFile(const char* fileName) override
    {
        if (isS3FileName(fileName))
            return createS3File(fileName);
        return nullptr;
    }

    virtual IAPICopyClient* getCopyApiClient(IStorageApiInfo* source, IStorageApiInfo* target) override
    {
        if (source && target && isS3Type(source->getStorageType()) && isS3Type(target->getStorageType()))
            return new S3APICopyClient(source, target);
        return nullptr;
    }
};

static S3FileHook* s3FileHook = nullptr;
static CriticalSection hookCS;

//---------------------------------------------------------------------------------------------------------------------
// Exported functions

extern S3FILE_API void installFileHook()
{
    CriticalBlock block(hookCS);
    if (!s3FileHook)
    {
        s3FileHook = new S3FileHook;
        addContainedFileHook(s3FileHook);
    }
}

extern S3FILE_API void removeFileHook()
{
    CriticalBlock block(hookCS);
    if (s3FileHook)
    {
        removeContainedFileHook(s3FileHook);
        delete s3FileHook;
        s3FileHook = nullptr;
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    if (s3FileHook)
    {
        removeContainedFileHook(s3FileHook);
        delete s3FileHook;
        s3FileHook = nullptr;
    }
}
