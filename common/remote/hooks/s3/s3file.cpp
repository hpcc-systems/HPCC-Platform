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

#include "s3file.hpp"

#ifdef _MSC_VER
#undef GetObject
#endif

using namespace Aws;

// Constants
constexpr const char* s3FilePrefix = "s3://";
constexpr size_t s3FilePrefixLen = 5;  // Length of "s3://" for pointer arithmetic
constexpr size32_t minMultipartSize = 5 * 1024 * 1024; // 5MB AWS minimum
constexpr unsigned defaultMaxRetries = 3;

// Global AWS initialization
static std::atomic_bool initializedAws{false};
static CriticalSection awsCS;
static S3Config globalS3Config;

static void ensureAWSInitialized()
{
    if (initializedAws)
        return;
    CriticalBlock block(awsCS);
    if (initializedAws)
        return;

    // Configure AWS SDK options - use heap allocation to avoid destructor issues
    // CRITICAL: We intentionally "leak" awsOptions and never call ShutdownAPI!
    // The AWS SDK has complex internal dependencies. Calling ShutdownAPI or
    // allowing SDKOptions to destruct causes segfaults due to destruction order
    // issues with AWS SDK internal objects (CurlHandleContainer, EC2MetadataClient, etc.)
    static Aws::SDKOptions* awsOptions = new Aws::SDKOptions();
    awsOptions->loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;

    Aws::InitAPI(*awsOptions);
    initializedAws = true;
    PROGLOG("AWS SDK initialized for S3 file operations");
}

// S3Config implementation
S3Config::S3Config(IPropertyTree* _config)
{
    loadFromConfig(_config);
}

void S3Config::loadFromConfig(IPropertyTree* _config)
{
    if (!_config)
        return;

    // Store strings using StringAttr to own the memory
    const char* regionStr = _config->queryProp("@region");
    const char* endpointStr = _config->queryProp("@endpoint");

    region.set(regionStr ? regionStr : "us-east-1");
    endpoint.set(endpointStr ? endpointStr : "");

    useSSL = _config->getPropBool("@useSSL", true);
    useVirtualHosting = _config->getPropBool("@useVirtualHosting", true);
    readAheadSize = _config->getPropInt("@readAheadSize", 4 * 1024 * 1024);
    writeBufferSize = _config->getPropInt("@writeBufferSize", 5 * 1024 * 1024);
    maxRetries = _config->getPropInt("@maxRetries", 3);
    timeoutMs = _config->getPropInt("@timeoutMs", 30000);

    // Validate settings
    if (writeBufferSize < minMultipartSize)
        writeBufferSize = minMultipartSize;
}

// S3 Client management
class S3ClientManager
{
private:
    mutable CriticalSection cs;
    std::unique_ptr<Aws::S3::S3Client> client;
    S3Config currentConfig;

public:
    Aws::S3::S3Client& getClient(const S3Config& config)
    {
        CriticalBlock block(cs);
        if (!client || configChanged(config))
        {
            client = createClient(config);
            currentConfig = config;
        }
        return *client;
    }
private:
    bool configChanged(const S3Config& config) const
    {
        return (!strsame(config.region.str(), currentConfig.region.str())) ||
               (!strsame(config.endpoint.str(), currentConfig.endpoint.str())) ||
               (config.useSSL != currentConfig.useSSL) ||
               (config.useVirtualHosting != currentConfig.useVirtualHosting);
    }

    std::unique_ptr<Aws::S3::S3Client> createClient(const S3Config& config)
    {
        Aws::Client::ClientConfiguration clientConfig;

        if (!config.region.isEmpty())
        {
            clientConfig.region = config.region.str();
        }
        // If no region specified in config, don't set a default - let AWS SDK auto-detect
        // from environment variables, AWS config file, or use the bucket's region

        if (!config.endpoint.isEmpty())
        {
            clientConfig.endpointOverride = config.endpoint.str();
        }

        clientConfig.scheme = config.useSSL ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
        clientConfig.connectTimeoutMs = config.timeoutMs;
        clientConfig.requestTimeoutMs = config.timeoutMs * 2;
        clientConfig.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.maxRetries);

        auto credentialsProvider = std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        return std::make_unique<Aws::S3::S3Client>(credentialsProvider, clientConfig, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent, false);
    }
};

static S3ClientManager& getS3ClientManager()
{
    // CRITICAL: Never delete this! Use heap allocation to avoid destructor
    // running during static destruction, which causes segfaults with AWS SDK
    static S3ClientManager* manager = new S3ClientManager();
    return *manager;
}

// Utility functions
static void logAwsError(const char* operation, const Aws::Client::AWSError<Aws::S3::S3Errors>& error, const char* bucket, const char* key)
{
    ERRLOG("S3 %s failed for s3://%s/%s: %s - %s",
           operation, bucket, key,
           error.GetExceptionName().c_str(),
           error.GetMessage().c_str());
}

static void parseS3Url(const char* s3Url, StringBuffer& bucket, StringBuffer& key)
{
    if (!s3Url)
        throw makeStringException(-1, "S3 URL cannot be null");
    const char* path = s3Url + s3FilePrefixLen;
    const char* slash = strchr(path, '/');
    if (!slash)
        throw makeStringExceptionV(-1, "Invalid S3 URL format: %s (expected s3://bucket/key)", s3Url);

    bucket.append(slash - path, path);
    key.set(slash + 1);
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
    S3Config config;
    FileIOStats stats;
    std::unique_ptr<S3ReadAheadBuffer> readAheadBuffer;
    CriticalSection ioCS;
    offset_t cachedFileSize;

public:
    S3FileReadIO(S3File* _file, const S3Config& _config);

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
    StringAttr bucket;
    StringAttr key;
    StringAttr uploadId;
    StringBuffer fullPath;
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    unsigned partNumber = 1;
    S3Config config;
    bool active = false;

public:
    S3MultipartUpload(const char* _bucket, const char* _key, const S3Config& _config)
        : bucket(_bucket), key(_key), config(_config)
    {
        fullPath.appendf("s3://%s/%s", _bucket, _key);
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
    Aws::S3::S3Client& getClient() { return getS3ClientManager().getClient(config); }
};

//---------------------------------------------------------------------------------------------------------------------
// S3FileWriteIO implementation
class S3FileWriteIO : implements CInterfaceOf<IFileIO>
{
private:
    Linked<S3File> file;
    S3Config config;
    FileIOStats stats;
    MemoryBuffer writeBuffer;
    std::unique_ptr<S3MultipartUpload> multipartUpload;
    CriticalSection ioCS;
    bool closed = false;
    offset_t currentPos = 0;

public:
    S3FileWriteIO(S3File* _file, const S3Config& _config);
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
    StringBuffer bucketName;
    StringBuffer keyName;
    S3Config config;

    // Cached metadata
    mutable CriticalSection metaCS;
    mutable bool haveMeta = false;
    mutable bool fileExists = false;
    mutable bool isDir = false;
    mutable offset_t fileSize = unknownFileSize;
    mutable time_t modifiedTime = 0;

public:
    S3File(const char* s3FileName, const S3Config& _config);

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
    Aws::S3::S3Client& getClient() const { return getS3ClientManager().getClient(config); }
};

//---------------------------------------------------------------------------------------------------------------------
// Implementation of S3FileReadIO

S3FileReadIO::S3FileReadIO(S3File* _file, const S3Config& _config)
    : file(_file), config(_config), cachedFileSize(_file->size())
{
    readAheadBuffer = std::make_unique<S3ReadAheadBuffer>(config.readAheadSize);
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
    size32_t readSize = config.readAheadSize;
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

S3FileWriteIO::S3FileWriteIO(S3File* _file, const S3Config& _config)
    : file(_file), config(_config)
{
    writeBuffer.ensureCapacity(config.writeBufferSize);
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
    if (writeBuffer.length() >= config.writeBufferSize)
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
                file->bucketName.str(), file->keyName.str(), config);
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

S3File::S3File(const char* s3FileName, const S3Config& _config)
    : fullName(s3FileName), config(_config)
{
    parseS3Url(s3FileName, bucketName, keyName);
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
    ensureAWSInitialized();

    switch (mode)
    {
        case IFOread:
            if (!exists())
                return nullptr;
            return new S3FileReadIO(this, config);
        case IFOcreate:
        case IFOwrite:
            return new S3FileWriteIO(this, config);
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
    // In S3, directories are virtual - they don't really exist as separate entities.
    // This method creates parent directory markers only, similar to Azure's implementation.
    // It does NOT create the file itself - that happens during write operations.

    try
    {
        // First, verify the bucket exists with retry logic
        unsigned attempt = 0;
        const char* filename = queryFilename();

        for (;;)
        {
            try
            {
                Aws::S3::Model::HeadBucketRequest bucketRequest;
                bucketRequest.SetBucket(bucketName.str());

                auto bucketOutcome = getClient().HeadBucket(bucketRequest);
                if (bucketOutcome.IsSuccess())
                {
                    break; // Bucket exists and is accessible
                }
                else
                {
                    // Bucket doesn't exist or we don't have access
                    const auto& error = bucketOutcome.GetError();

                    // Don't retry if bucket doesn't exist or we lack permissions
                    if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
                        error.GetErrorType() == Aws::S3::S3Errors::ACCESS_DENIED)
                    {
                        StringBuffer errorMsg;
                        errorMsg.append("S3 bucket verification failed for ").append(bucketName.str());

                        if (!error.GetExceptionName().empty())
                            errorMsg.append(": ").append(error.GetExceptionName().c_str());

                        if (!error.GetMessage().empty())
                            errorMsg.append(" - ").append(error.GetMessage().c_str());

                        errorMsg.appendf(" (HTTP Status: %d)", static_cast<int>(error.GetResponseCode()));

                        throw makeStringExceptionV(-1, "%s", errorMsg.str());
                    }

                    // Retry for transient errors
                    attempt++;
                    handleRequestException(error, "S3File::createDirectory::HeadBucket", attempt, defaultMaxRetries, filename);
                }
            }
            catch (const std::exception& e)
            {
                attempt++;
                handleRequestException(e, "S3File::createDirectory::HeadBucket", attempt, defaultMaxRetries, filename);
            }
        }

        // Create directory markers for parent directories only (not the file itself)
        const char *start = keyName.str();
        if (!isEmptyString(start))
        {
            StringBuffer currentPath;
            while (true)
            {
                const char *slash = strchr(start, '/');
                if (!slash)
                    break; // Stop before the filename

                // Append this directory component
                if (!currentPath.isEmpty())
                    currentPath.append('/');
                currentPath.append(slash - start, start);

                // Create directory marker with trailing slash
                StringBuffer dirKey(currentPath);
                dirKey.append('/');

                // Use retry logic for directory marker creation
                unsigned dirAttempt = 0;
                StringBuffer dirFilename;
                dirFilename.appendf("s3://%s/%s", bucketName.str(), dirKey.str());

                for (;;)
                {
                    try
                    {
                        Aws::S3::Model::PutObjectRequest request;
                        request.SetBucket(bucketName.str());
                        request.SetKey(dirKey.str());

                        // Set an empty body for the directory marker
                        auto body = std::make_shared<std::stringstream>();
                        request.SetBody(body);
                        request.SetContentLength(0);

                        auto outcome = getClient().PutObject(request);
                        if (outcome.IsSuccess())
                            break;
                        else
                        {
                            const auto& error = outcome.GetError();
                            // Conflict means it already exists - that's fine, don't retry
                            if (error.GetResponseCode() == Aws::Http::HttpResponseCode::CONFLICT)
                                break;

                            dirAttempt++;
                            handleRequestException(error, "S3File::createDirectory::PutObject", dirAttempt, defaultMaxRetries, dirFilename.str());
                        }
                    }
                    catch (const std::exception& e)
                    {
                        dirAttempt++;
                        handleRequestException(e, "S3File::createDirectory::PutObject", dirAttempt, defaultMaxRetries, dirFilename.str());
                    }
                }

                start = slash + 1;
                if (!*start)
                    break;
            }
        }

        return true;
    }
    catch (IException* e)
    {
        // Re-throw IException (including the ones we created above)
        throw;
    }
    catch (const std::exception& e)
    {
        // Wrap standard exceptions in IException
        throw makeStringExceptionV(-1, "Exception in S3 createDirectory: %s", e.what());
    }
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
    ensureAWSInitialized();
    return new S3File(s3FileName, globalS3Config);
}

extern S3FILE_API bool isS3FileName(const char *fileName)
{
    if (!fileName || !startsWith(fileName, s3FilePrefix))
        return false;

    const char *bucketName = fileName + s3FilePrefixLen;
    const char *slash = strchr(bucketName, '/');
    // Require a non-empty key after the slash
    return (slash != nullptr && *(slash + 1) != '\0');
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
        ensureAWSInitialized();
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
    // Cleanup file hook
    if (s3FileHook)
    {
        removeContainedFileHook(s3FileHook);
        delete s3FileHook;
        s3FileHook = nullptr;
    }
}
