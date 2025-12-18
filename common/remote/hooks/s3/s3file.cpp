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

#include "platform.h"
#include "jlib.hpp"
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

#include "s3file.hpp"

#ifdef _MSC_VER
#undef GetObject
#endif

using namespace Aws;

// Constants
constexpr const char* s3FilePrefix = "s3:";
constexpr size_t s3FilePrefixLen = 3;  // Length of "s3:" for pointer arithmetic
constexpr size32_t minMultipartSize = 5 * 1024 * 1024; // 5MB AWS minimum
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
        PROGLOG("AWS SDK initialized for S3 file operations");
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
        PROGLOG("AWS SDK shutdown for S3 file operations");
    }
    else if (awsInitRefCount == 0)
    {
        // Should never happen - more shutdowns than inits
        ERRLOG("AWS SDK shutdown called more times than init");
    }

    awsInitRefCount--;
}

// S3 Client cache key based on plane and device
struct S3ClientKey
{
    StringAttr planeName;
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
            unsigned h = 0;
            if (key.planeName.str())
                h = hashc((const unsigned char*)key.planeName.str(), key.planeName.length(), h);
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
        key.planeName.set(planeName);
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

static S3ClientManager* s3ClientManager = nullptr;

static S3ClientManager& getS3ClientManager()
{
    if (!s3ClientManager)
    {
        CriticalBlock block(awsCS);
        if (!s3ClientManager)
            s3ClientManager = new S3ClientManager();
    }
    return *s3ClientManager;
}

static void cleanupS3ClientManager()
{
    CriticalBlock block(awsCS);
    if (s3ClientManager)
    {
        delete s3ClientManager;
        s3ClientManager = nullptr;
    }
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

static void handleRequestException(const Aws::Client::AWSError<Aws::S3::S3Errors>& e, const char* op, unsigned attempt, unsigned maxRetries, const char* filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s - %s",
                      op, attempt, maxRetries, filename, pos, len,
                      e.GetExceptionName().c_str(), e.GetMessage().c_str());

    handleRequestBackoff(msg, attempt, maxRetries);
}

static void handleRequestException(const std::exception& e, const char* op, unsigned attempt, unsigned maxRetries, const char* filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s",
                      op, attempt, maxRetries, filename, pos, len, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
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

//---------------------------------------------------------------------------------------------------------------------
// Forward declarations
class S3File;

//---------------------------------------------------------------------------------------------------------------------
// ReadAhead buffer for efficient S3 reading
class S3ReadAheadBuffer
{
private:
    mutable CriticalSection cs;
    MemoryBuffer buffer;
    offset_t bufferStart = 0;
    offset_t bufferEnd = 0;
    size32_t bufferSize;

public:
    S3ReadAheadBuffer(size32_t _bufferSize) : bufferSize(_bufferSize)
    {
        buffer.ensureCapacity(bufferSize);
    }

    bool contains(offset_t pos, size32_t len) const
    {
        CriticalBlock block(cs);
        return pos >= bufferStart && (pos + len) <= bufferEnd;
    }

    size32_t read(offset_t pos, size32_t len, void* data)
    {
        CriticalBlock block(cs);
        if (pos < bufferStart || pos >= bufferEnd)
            return 0;

        offset_t maxRead = bufferEnd - pos;
        if (len > maxRead)
            len = (size32_t)maxRead;

        offset_t bufferOffset = pos - bufferStart;
        memcpy(data, buffer.toByteArray() + bufferOffset, len);
        return len;
    }

    // Atomic operation: check if data is in buffer and read if available
    size32_t tryRead(offset_t pos, size32_t len, void* data, bool& found)
    {
        CriticalBlock block(cs);
        if (pos < bufferStart || pos >= bufferEnd)
        {
            found = false;
            return 0;
        }

        offset_t maxRead = bufferEnd - pos;
        if (len > maxRead)
            len = (size32_t)maxRead;

        offset_t bufferOffset = pos - bufferStart;
        memcpy(data, buffer.toByteArray() + bufferOffset, len);
        found = true;
        return len;
    }

    void fill(offset_t startPos, const void* data, size32_t dataLen)
    {
        CriticalBlock block(cs);
        buffer.clear();
        buffer.append(dataLen, data);
        bufferStart = startPos;
        bufferEnd = startPos + dataLen;
    }

    void clear()
    {
        CriticalBlock block(cs);
        buffer.clear();
        bufferStart = bufferEnd = 0;
    }
};

//---------------------------------------------------------------------------------------------------------------------
// S3FileReadIO implementation
class S3FileReadIO : implements CInterfaceOf<IFileIO>
{
private:
    Linked<S3File> file;
    FileIOStats stats;
    std::unique_ptr<S3ReadAheadBuffer> readAheadBuffer;
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
    void fillReadAheadBuffer(offset_t pos);
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
        if (active)
            abort();
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
    MemoryBuffer writeBuffer;
    std::unique_ptr<S3MultipartUpload> multipartUpload;
    CriticalSection ioCS;
    bool closed = false;
    offset_t currentPos = 0;

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

private:
    void flushBuffer();
    void finishUpload();
};

//---------------------------------------------------------------------------------------------------------------------
// S3File implementation
class S3File : implements CInterfaceOf<IFile>
{
    friend class S3FileReadIO;
    friend class S3FileWriteIO;

private:
    StringBuffer fullName;
    StringAttr planeName;
    StringBuffer bucketName;
    StringBuffer keyName;
    unsigned device = 1;

    // Configuration from plane
    size32_t readAheadSize = 4 * 1024 * 1024; // 4MB default
    size32_t writeBufferSize = 5 * 1024 * 1024; // 5MB minimum for multipart

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
    virtual IDirectoryIterator* directoryFiles(const char* mask, bool sub, bool includeDirs) override { UNIMPLEMENTED; }
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
    readAheadBuffer = std::make_unique<S3ReadAheadBuffer>(file->readAheadSize);
}

size32_t S3FileReadIO::read(offset_t pos, size32_t len, void* data)
{
    if (!file->exists())
        return 0;

    if (pos >= cachedFileSize)
        return 0;

    if (pos + len > cachedFileSize)
        len = (size32_t)(cachedFileSize - pos);

    if (len == 0)
        return 0;

    CriticalBlock block(ioCS);

    // Try to read from cache first (atomic operation)
    bool found = false;
    size32_t bytesRead = readAheadBuffer->tryRead(pos, len, data, found);
    if (found)
    {
        stats.ioReads++;
        stats.ioReadBytes += bytesRead;
        return bytesRead;
    }

    // Read from S3 and fill cache
    fillReadAheadBuffer(pos);
    bytesRead = readAheadBuffer->read(pos, len, data);
    stats.ioReads++;
    stats.ioReadBytes += bytesRead;
    return bytesRead;
}

void S3FileReadIO::fillReadAheadBuffer(offset_t pos)
{
    size32_t readSize = file->readAheadSize;
    if (pos + readSize > cachedFileSize)
        readSize = (size32_t)(cachedFileSize - pos);

    MemoryBuffer tempBuffer;
    tempBuffer.ensureCapacity(readSize);

    size32_t bytesRead = readFromS3(pos, readSize, tempBuffer.reserveTruncate(readSize));
    if (bytesRead > 0)
    {
        readAheadBuffer->fill(pos, tempBuffer.toByteArray(), bytesRead);
    }
}

size32_t S3FileReadIO::readFromS3(offset_t pos, size32_t len, void* data)
{
    unsigned attempt = 0;
    size32_t bytesRead = 0;
    const char* filename = file->queryFilename();

    CCycleTimer timer;

    for (;;)
    {
        try
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

            auto outcome = file->getClient().GetObject(request);

            if (outcome.IsSuccess())
            {
                auto& body = outcome.GetResult().GetBody();
                body.read((char*)data, len);
                bytesRead = (size32_t)body.gcount();
                break;
            }
            else
            {
                attempt++;
                handleRequestException(outcome.GetError(), "S3File::read", attempt, defaultMaxRetries, filename, pos, len);
            }
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::read", attempt, defaultMaxRetries, filename, pos, len);
        }
    }

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
    unsigned attempt = 0;

    // Reserve space for estimated parts (assume average 200 parts for large files)
    completedParts.reserve(200);

    for (;;)
    {
        try
        {
            Aws::S3::Model::CreateMultipartUploadRequest request;
            request.SetBucket(bucket.str());
            request.SetKey(key.str());

            auto outcome = getClient().CreateMultipartUpload(request);
            if (outcome.IsSuccess())
            {
                uploadId.set(outcome.GetResult().GetUploadId().c_str());
                active = true;
                return true;
            }
            else
            {
                attempt++;
                handleRequestException(outcome.GetError(), "S3File::initiate", attempt, defaultMaxRetries, fullPath.str());
            }
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::initiate", attempt, defaultMaxRetries, fullPath.str());
        }
    }
}

bool S3MultipartUpload::uploadPart(const void* data, size32_t len)
{
    if (!active)
        return false;

    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            Aws::S3::Model::UploadPartRequest request;
            request.SetBucket(bucket.str());
            request.SetKey(key.str());
            request.SetUploadId(uploadId.str());
            request.SetPartNumber(partNumber);

            auto body = std::make_shared<std::stringstream>(std::ios::in | std::ios::out | std::ios::binary);
            body->write(reinterpret_cast<const char*>(data), len);
            if (!body->good() || body->tellp() != static_cast<std::streampos>(len))
                throw makeStringException(-1, "Failed to prepare multipart upload buffer");
            request.SetBody(body);

            auto outcome = getClient().UploadPart(request);
            if (outcome.IsSuccess())
            {
                Aws::S3::Model::CompletedPart completedPart;
                completedPart.SetPartNumber(partNumber);
                completedPart.SetETag(outcome.GetResult().GetETag());
                completedParts.push_back(completedPart);
                partNumber++;
                return true;
            }
            else
            {
                attempt++;
                handleRequestException(outcome.GetError(), "S3File::uploadPart", attempt, defaultMaxRetries, fullPath.str(), 0, len);
            }
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::uploadPart", attempt, defaultMaxRetries, fullPath.str(), 0, len);
        }
    }
}

bool S3MultipartUpload::complete()
{
    if (!active)
        return false;

    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            Aws::S3::Model::CompletedMultipartUpload completedUpload;
            completedUpload.SetParts(completedParts);

            Aws::S3::Model::CompleteMultipartUploadRequest request;
            request.SetBucket(bucket.str());
            request.SetKey(key.str());
            request.SetUploadId(uploadId.str());
            request.SetMultipartUpload(completedUpload);

            auto outcome = getClient().CompleteMultipartUpload(request);
            if (outcome.IsSuccess())
            {
                active = false;
                return true;
            }
            else
            {
                attempt++;
                handleRequestException(outcome.GetError(), "S3File::complete", attempt, defaultMaxRetries, fullPath.str());
            }
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::complete", attempt, defaultMaxRetries, fullPath.str());
        }
    }
}

bool S3MultipartUpload::abort()
{
    if (!active)
        return true;

    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            Aws::S3::Model::AbortMultipartUploadRequest request;
            request.SetBucket(bucket.str());
            request.SetKey(key.str());
            request.SetUploadId(uploadId.str());

            auto outcome = getClient().AbortMultipartUpload(request);
            active = false;

            if (outcome.IsSuccess())
            {
                return true;
            }
            else
            {
                attempt++;
                handleRequestException(outcome.GetError(), "S3File::abort", attempt, defaultMaxRetries, fullPath.str());
            }
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "S3File::abort", attempt, defaultMaxRetries, fullPath.str());
        }
    }
}

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3FileWriteIO

S3FileWriteIO::S3FileWriteIO(S3File* _file)
    : file(_file)
{
    writeBuffer.ensureCapacity(file->writeBufferSize);
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

    writeBuffer.append(len, data);
    currentPos += len;

    // If buffer is full, flush it
    if (writeBuffer.length() >= file->writeBufferSize)
        flushBuffer();

    stats.ioWrites++;
    stats.ioWriteBytes += len;
    return len;
}

void S3FileWriteIO::flush()
{
    CriticalBlock block(ioCS);
    if (writeBuffer.length() > 0)
        flushBuffer();
}

void S3FileWriteIO::close()
{
    if (closed)
        return;

    CriticalBlock block(ioCS);
    finishUpload();
    closed = true;
}

void S3FileWriteIO::flushBuffer()
{
    if (writeBuffer.length() == 0)
        return;

    if (!multipartUpload)
    {
        if (writeBuffer.length() >= minMultipartSize)
        {
            // Use multipart upload for large files
            multipartUpload = std::make_unique<S3MultipartUpload>(
                file->planeName.str(), file->device, file->bucketName.str(), file->keyName.str());
            if (!multipartUpload->initiate())
                throw makeStringException(-1, "Failed to initiate multipart upload");
        }
        else
        {
            // Buffer is not large enough to initiate multipart upload
            // Keep it buffered - will be handled in finishUpload()
            return;
        }
    }

    // If we get here, multipartUpload must be active
    if (!multipartUpload->uploadPart(writeBuffer.toByteArray(), writeBuffer.length()))
        throw makeStringException(-1, "Failed to upload part to S3");

    writeBuffer.clear();
}

void S3FileWriteIO::finishUpload()
{
    if (writeBuffer.length() > 0)
    {
        if (!multipartUpload && writeBuffer.length() < minMultipartSize)
        {
            // Small file - use simple PUT with retry
            unsigned attempt = 0;
            const char* filename = file->queryFilename();
            CCycleTimer timer;

            for (;;)
            {
                try
                {
                    Aws::S3::Model::PutObjectRequest request;
                    request.SetBucket(file->bucketName.str());
                    request.SetKey(file->keyName.str());

                    auto body = std::make_shared<std::stringstream>(std::ios::in | std::ios::out | std::ios::binary);
                    body->write(reinterpret_cast<const char*>(writeBuffer.toByteArray()), writeBuffer.length());
                    if (!body->good() || body->tellp() != static_cast<std::streampos>(writeBuffer.length()))
                        throw makeStringException(-1, "Failed to prepare upload buffer");
                    request.SetBody(body);

                    auto outcome = file->getClient().PutObject(request);

                    if (outcome.IsSuccess())
                    {
                        break;
                    }
                    else
                    {
                        attempt++;
                        handleRequestException(outcome.GetError(), "S3File::write", attempt, defaultMaxRetries, filename, 0, writeBuffer.length());
                    }
                }
                catch (const std::exception& e)
                {
                    attempt++;
                    handleRequestException(e, "S3File::write", attempt, defaultMaxRetries, filename, 0, writeBuffer.length());
                }
            }

            stats.ioWriteCycles += timer.elapsedCycles();
        }
        else
        {
            // Flush remaining data as final part
            flushBuffer();
        }
    }

    if (multipartUpload)
    {
        if (!multipartUpload->complete())
            throw makeStringException(-1, "Failed to complete multipart upload");
        multipartUpload.reset();
    }
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
    if (startsWith(fullName, s3FilePrefix))
    {
        //format is s3:plane[/device]/path
        const char * filename = fullName + strlen(s3FilePrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            throw makeStringException(99, "Missing / in s3: file reference");

        StringBuffer planeNameBuf(slash-filename, filename);
        planeName.set(planeNameBuf);
        Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeNameBuf, true);
        const IPropertyTree * storageapi = plane->queryPropTree("storageapi");
        if (!storageapi)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeNameBuf.str());
        filename = slash+1; // advance past slash

        const char * api = storageapi->queryProp("@type");
        if (!api)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeNameBuf.str());

        if (!strieq(api, "s3"))
            throw makeStringExceptionV(99, "Storage api for plane %s is not s3", planeNameBuf.str());

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
                throw makeStringExceptionV(99, "Device %d out of range for plane %s", device, planeNameBuf.str());

            if (!endDevice || (*endDevice != '/'))
                throw makeStringExceptionV(99, "Unexpected end of device partition %s", fullName.str());

            filename = endDevice+1;
        }

        VStringBuffer childPath("buckets[%u]", device);
        const IPropertyTree * deviceInfo = storageapi->queryPropTree(childPath);
        if (!deviceInfo)
            throw makeStringExceptionV(99, "Missing bucket specification for device %u in plane %s", device, planeNameBuf.str());

        const char * bucket = deviceInfo->queryProp("@name");
        if (isEmptyString(bucket))
            throw makeStringExceptionV(99, "Missing bucket name for plane %s", planeNameBuf.str());

        bucketName.set(bucket);
        keyName.set(filename);

        // Load buffer sizes from plane configuration
        readAheadSize = storageapi->getPropInt("@readAheadSize", 4 * 1024 * 1024);
        writeBufferSize = storageapi->getPropInt("@writeBufferSize", 5 * 1024 * 1024);
        if (writeBufferSize < minMultipartSize)
            writeBufferSize = minMultipartSize;
    }
    else
        throw makeStringExceptionV(99, "Unexpected prefix on S3 filename %s", fullName.str());
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
    // Proper cleanup sequence:
    // 1. Remove file hook from registry
    // 2. Delete file hook (no AWS calls here)
    // 3. Clean up S3ClientManager (destroys all clients BEFORE ShutdownAPI)
    // 4. ClientManager destructor calls shutdownAWS() when refcount reaches zero

    if (s3FileHook)
    {
        removeContainedFileHook(s3FileHook);
        delete s3FileHook;
        s3FileHook = nullptr;
    }

    // This destroys all S3Clients and calls Aws::ShutdownAPI in the correct order
    cleanupS3ClientManager();
}
