/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>

#include "platform.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jregexp.hpp"
#include "jstring.hpp"
#include "jlog.hpp"

#include "s3file.hpp"

#ifdef _MSC_VER
#undef GetObject
#endif
/*
 * S3 questions:
 *
 * Where would we store access id/secrets?
 *     Current use the default default credential manager which gets them from environment variables, or passed in
 *     when running on an AWS instance.
 * What is the latency on file access?  ~200ms.
 * What is the cost of accessing the data?  $0.4/million GET requests (including requesting meta)
 * How to you efficiently page results from a S3 bucket?  You can perform a range get, but you'll need to pay for each call.
 * You can get the length of an object using HeadObject..getContentLength, but you will get charged - so for small files it is better to just get it.
 *     Probably best to request the first 10Mb, and if that returns a full 10Mb then request the size information.
 * S3 does supports partial writes in chunks of 5Mb or more.  It may be simpler to create a file locally and then submit it in a single action.
 *
 * This is currently a proof of concept implementations.  The following changes are required for a production version:
 * - Revisit credentials
 * - Support chunked writes.
 * - Implement directory iteration.
 * - Ideally switch engines to use streaming interfaces for reading and writing files.
 * - Investigate adding a read-ahead thread to read the next block of the file.
 */

//#define TRACE_S3
//#define TEST_S3_PAGING
//#define FIXED_CREDENTIALS

constexpr const char * s3FilePrefix = "s3://";

#ifdef TEST_S3_PAGING
constexpr offset_t awsReadRequestSize = 50;
#else
constexpr offset_t awsReadRequestSize = 0x400000;  // Default to requesting 4Mb each time
#endif

static std::atomic_bool initializedAws;
static CriticalSection awsCS;
static Aws::SDKOptions options;
MODULE_INIT(INIT_PRIORITY_HQLINTERNAL)
{
    return true;
}
MODULE_EXIT()
{
    if (initializedAws)
    {
        Aws::ShutdownAPI(options);
        initializedAws = false;
    }
}

static void ensureAWSInitialized()
{
    if (initializedAws)
        return;
    CriticalBlock block(awsCS);
    if (initializedAws)
        return;
    Aws::InitAPI(options);
    initializedAws = true;
}

//---------------------------------------------------------------------------------------------------------------------

class S3File;
class S3FileReadIO : implements CInterfaceOf<IFileIO>
{
public:
    S3FileReadIO(S3File * _file, Aws::S3::Model::GetObjectOutcome & firstRead, FileIOStats & _stats);

    virtual size32_t read(offset_t pos, size32_t len, void * data) override;
    virtual offset_t size() override;
    virtual void close() override
    {
        //This could set a flag here to check for reading after close(), but I don't think any file read code
        //ever calls close, and it would be harmless (and would complicate the rest of the code).
    }

    // Write methods not implemented - this is a read-only file
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        throwUnexpected();
        return 0;
    }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override
    {
        throwUnexpected();
        return 0;
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpected();
    }
    virtual void flush() override
    {
        //Could implement if we use the async version of the putObject call.
    }
    unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    size_t extractDataFromResult(size_t offset, size_t length, void * target);

protected:
    Linked<S3File> file;
    CriticalSection cs;
    offset_t startResultOffset = 0;
    offset_t endResultOffset = 0;
    Aws::S3::Model::GetObjectOutcome readResult;
    FileIOStats stats;
};

class S3FileWriteIO : implements CInterfaceOf<IFileIO>
{
public:
    S3FileWriteIO(S3File * _file);
    virtual void beforeDispose() override;

    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        throwUnexpected();
    }

    virtual offset_t size() override
    {
        throwUnexpected();
    }

    virtual void close() override;
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;
    virtual void setSize(offset_t size) override;
    virtual void flush() override;

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    Linked<S3File> file;
    CriticalSection cs;
    FileIOStats stats;
    bool blobWritten = false;
};

class S3File : implements CInterfaceOf<IFile>
{
    friend class S3FileReadIO;
    friend class S3FileWriteIO;
public:
    S3File(const char *_s3FileName);
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
        if (mode == IFOcreate)
            return createFileWriteIO();
        assertex(mode==IFOread && fileExists);
        return createFileReadIO();
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode) override
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone) override
    {
        if (mode == IFOcreate)
            return createFileWriteIO();
        assertex(mode==IFOread && fileExists);
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
        UNIMPLEMENTED;
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
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) override { UNIMPLEMENTED; }
    virtual bool remove() override;
    virtual void rename(const char *newTail) override { UNIMPLEMENTED; }
    virtual void move(const char *newName) override { UNIMPLEMENTED; }
    virtual void setReadOnly(bool ro) override { UNIMPLEMENTED; }
    virtual void setFilePermissions(unsigned fPerms) override { UNIMPLEMENTED; }
    virtual bool setCompression(bool set) override { UNIMPLEMENTED; }
    virtual offset_t compressedSize() override { UNIMPLEMENTED; }
    virtual unsigned getCRC() override { UNIMPLEMENTED; }
    virtual void setCreateFlags(unsigned short cflags) override { UNIMPLEMENTED; }
    virtual void setShareMode(IFSHmode shmode) override { UNIMPLEMENTED; }
    virtual bool createDirectory() override { UNIMPLEMENTED; }
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) override { UNIMPLEMENTED; }
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone) override { UNIMPLEMENTED; }
    virtual void copyTo(IFile *dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress *progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) override { UNIMPLEMENTED; }
    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false) override { UNIMPLEMENTED; }

protected:
    void readBlob(Aws::S3::Model::GetObjectOutcome & readResult, FileIOStats & stats, offset_t from = 0, offset_t length = unknownFileSize);
    size32_t writeBlob(size32_t len, const void * data, FileIOStats & stats);
    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileReadIO();
    IFileIO * createFileWriteIO();

protected:
    StringBuffer fullName;
    StringBuffer bucketName;
    StringBuffer objectName;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    int64_t modifiedMsTime = 0;
    CriticalSection cs;
};

//---------------------------------------------------------------------------------------------------------------------

S3FileReadIO::S3FileReadIO(S3File * _file, Aws::S3::Model::GetObjectOutcome & firstRead, FileIOStats & _firstStats)
: file(_file), readResult(std::move(firstRead)), stats(_firstStats)
{
    startResultOffset = 0;
    endResultOffset = readResult.GetResult().GetContentLength();
}

size32_t S3FileReadIO::read(offset_t pos, size32_t len, void * data)
{
    if (pos > file->fileSize)
        return 0;
    if (pos + len > file->fileSize)
        len = file->fileSize - pos;
    if (len == 0)
        return 0;

    size32_t sizeRead = 0;
    offset_t lastOffset = pos + len;

    // MORE: Do we ever read file IO from more than one thread?  I'm not convinced we do, and the critical blocks waste space and slow it down.
    //It might be worth revisiting (although I'm not sure what effect stranding has) - by revisiting the primary interface used to read files.
    CriticalBlock block(cs);
    for(;;)
    {
        //Check if part of the request can be fulfilled from the current read block
        if (pos >= startResultOffset && pos < endResultOffset)
        {
            size_t copySize = ((lastOffset > endResultOffset) ? endResultOffset : lastOffset) - pos;
            size_t extractedSize = extractDataFromResult((pos - startResultOffset), copySize, data);
            assertex(copySize == extractedSize);
            pos += copySize;
            len -= copySize;
            data = (byte *)data + copySize;
            sizeRead += copySize;
            if (len == 0)
                return sizeRead;
        }

#ifdef TEST_S3_PAGING
        offset_t readSize = awsReadRequestSize;
#else
        offset_t readSize = (len > awsReadRequestSize) ? len : awsReadRequestSize;
#endif

        file->readBlob(readResult, stats, pos, readSize);
        if (!readResult.IsSuccess())
            return sizeRead;
        offset_t contentSize = readResult.GetResult().GetContentLength();
        //If the results are inconsistent then do not loop forever
        if (contentSize == 0)
            return sizeRead;

        startResultOffset = pos;
        endResultOffset = pos + contentSize;
    }
}

offset_t S3FileReadIO::size()
{
    return file->fileSize;
}

size_t S3FileReadIO::extractDataFromResult(size_t offset, size_t length, void * target)
{
    auto & contents = readResult.GetResultWithOwnership().GetBody();
    auto buffer = contents.rdbuf();
    buffer->pubseekoff(0, std::ios_base::beg, std::ios_base::in);
    return buffer->sgetn((char *)target, length);
}

unsigned __int64 S3FileReadIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

//---------------------------------------------------------------------------------------------------------------------

S3FileWriteIO::S3FileWriteIO(S3File * _file)
: file(_file)
{
}

void S3FileWriteIO::beforeDispose()
{
    try
    {
        close();
    }
    catch (...)
    {
    }
}

void S3FileWriteIO::close()
{
    CriticalBlock block(cs);
    if (!blobWritten)
        file->writeBlob(0, nullptr, stats);
}

offset_t S3FileWriteIO::appendFile(IFile *file, offset_t pos, offset_t len)
{
    throwUnexpected();
    return 0;
}

size32_t S3FileWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len)
    {
        CriticalBlock block(cs);
        //Very strange semantics for a proof of concept - only allow a single write to the file.
        //A full implementation will need to either
        //  write to a temporary file, and then copy to the s3 file when the file is closed.
        //  use the multi-part upload functionality (has a minimum part size of 5Mb)
        assertex(!blobWritten);
        file->writeBlob(len, data, stats);
        blobWritten = true;
    }
    return len;
}

void S3FileWriteIO::setSize(offset_t size)
{
    UNIMPLEMENTED;
}

void S3FileWriteIO::flush()
{
}

unsigned __int64 S3FileWriteIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

//---------------------------------------------------------------------------------------------------------------------

static Aws::S3::S3Client createAwsClient()
{
    //There should be a default region, (and a default client), but allow the region to be overridden with s3:@region//,
    //in which a new client would be created.
    // Set up the request
    Aws::Client::ClientConfiguration configuration;
    configuration.region = "eu-west-2";

#ifdef FIXED_CREDENTIALS
    //The following code allows the access id/secret to come from a value that had been saved away in a secrets manager
    constexpr const char * myAccessKeyId = "<id>";
    constexpr const char * myAccessKeySecret = "<secret>";
    auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(Aws::String(myAccessKeyId), Aws::String(myAccessKeySecret));
    return Aws::S3::S3Client(credentials, configuration);
#else
    //Retrieve the details from environment variables/file/current environment
    return Aws::S3::S3Client(configuration);
#endif
}

static Aws::S3::S3Client & getAwsClient()
{
    static Aws::S3::S3Client client = createAwsClient();
    return client;
}


S3File::S3File(const char *_s3FileName) : fullName(_s3FileName)
{
    const char * filename = fullName.str() + strlen(s3FilePrefix);
    const char * slash = strchr(filename, '/');
    assertex(slash);

    bucketName.append(slash-filename, filename);
    objectName.set(slash+1);
}

bool S3File::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
{
    ensureMetaData();
    if (createTime)
    {
        createTime->clear();
        //Creation date does not seem to be available, so use the last modified date instead.
        createTime->set((time_t)(modifiedMsTime / 1000));
    }
    if (modifiedTime)
    {
        modifiedTime->clear();
        modifiedTime->set((time_t)(modifiedMsTime / 1000));
    }
    if (accessedTime)
        accessedTime->clear();
    return false;
}

void S3File::readBlob(Aws::S3::Model::GetObjectOutcome & readResult, FileIOStats & stats, offset_t from, offset_t length)
{
    Aws::S3::S3Client & s3_client = getAwsClient();

    Aws::S3::Model::GetObjectRequest object_request;
    object_request.SetBucket(bucketName);
    object_request.SetKey(objectName);
    if ((from != 0) || (length != unknownFileSize))
    {
        StringBuffer range;
        range.append("bytes=").append(from).append("-");
        if (length != unknownFileSize)
            range.append(from + length - 1);
        object_request.SetRange(Aws::String(range));
    }

    // Get the object

    CCycleTimer timer;
    readResult = s3_client.GetObject(object_request);
    stats.ioReads++;
    stats.ioReadCycles += timer.elapsedCycles();
    stats.ioReadBytes += readResult.GetResult().GetContentLength();

#ifdef TRACE_S3
    if (!readResult.IsSuccess())
    {
        auto error = readResult.GetError();
        DBGLOG("ERROR: %s: %s", error.GetExceptionName().c_str(), error.GetMessage().c_str());
    }
#endif
}

size32_t S3File::writeBlob(size32_t len, const void * data, FileIOStats & stats)
{
    Aws::S3::S3Client & s3_client = getAwsClient();

    Aws::S3::Model::PutObjectOutcome writeResult;
    Aws::S3::Model::PutObjectRequest writeRequest;
    writeRequest.WithBucket(bucketName).WithKey(objectName);

    auto body = std::make_shared<std::stringstream>(std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    body->write(reinterpret_cast<const char*>(data), len);

    writeRequest.SetBody(body);

    CCycleTimer timer;
    writeResult = s3_client.PutObject(writeRequest);
    stats.ioWrites++;
    stats.ioWriteCycles += timer.elapsedCycles();
    stats.ioWriteBytes += len;

#ifdef TRACE_S3
    if (!writeResult.IsSuccess())
    {
        auto error = writeResult.GetError();
        DBGLOG("ERROR: %s: %s", error.GetExceptionName().c_str(), error.GetMessage().c_str());
        return 0;
    }
#endif
    return len;
}

IFileIO * S3File::createFileReadIO()
{
    //Read the first chunk of the file.  If it is the full file then fill in the meta information, otherwise
    //ensure the meta information is calculated before creating the file IO object
    Aws::S3::Model::GetObjectOutcome readResult;
    FileIOStats readStats;

    CriticalBlock block(cs);
    readBlob(readResult, readStats, 0, awsReadRequestSize);
    if (!readResult.IsSuccess())
        return nullptr;

    if (!haveMeta)
    {
        offset_t readSize = readResult.GetResult().GetContentLength();

        //If we read the entire file then we don't need to gather the meta to discover the file size.
        if (readSize < awsReadRequestSize)
        {
            haveMeta = true;
            fileExists = true;
            fileSize = readResult.GetResult().GetContentLength();
            modifiedMsTime = readResult.GetResult().GetLastModified().Millis();
        }
        else
        {
            gatherMetaData();
            if (!fileExists)
            {
                DBGLOG("Internal consistency - read succeeded but head failed.");
                return nullptr;
            }
        }
    }

    return new S3FileReadIO(this, readResult, readStats);
}

IFileIO * S3File::createFileWriteIO()
{
    return new S3FileWriteIO(this);
}

void S3File::ensureMetaData()
{
    CriticalBlock block(cs);
    if (haveMeta)
        return;

    gatherMetaData();
}

void S3File::gatherMetaData()
{
    Aws::S3::S3Client & s3_client = getAwsClient();

    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectName);

    // Get the object
    Aws::S3::Model::HeadObjectOutcome headResult = s3_client.HeadObject(request);
    if (headResult.IsSuccess())
    {
        fileExists = true;
        fileSize = headResult.GetResult().GetContentLength();
        modifiedMsTime = headResult.GetResult().GetLastModified().Millis();
    }
    else
    {
#ifdef TRACE_S3
        auto error = headResult.GetError();
        DBGLOG("ERROR: %s: %s", error.GetExceptionName().c_str(), error.GetMessage().c_str());
#endif
    }
    haveMeta = true;
}

bool S3File::remove()
{
    Aws::S3::S3Client & s3_client = getAwsClient();

    Aws::S3::Model::DeleteObjectRequest object_request;
    object_request.SetBucket(bucketName);
    object_request.SetKey(objectName);

    // Get the object
    Aws::S3::Model::DeleteObjectOutcome result = s3_client.DeleteObject(object_request);
    if (result.IsSuccess())
    {
        CriticalBlock block(cs);
        haveMeta = true;
        fileExists = false;
        fileSize = unknownFileSize;
        return true;
    }
    else
    {
#ifdef TRACE_S3
        auto error = result.GetError();
        DBGLOG("ERROR: S3 Delete Object %s: %s", error.GetExceptionName().c_str(), error.GetMessage().c_str());
#endif
        return false;
    }
}


//---------------------------------------------------------------------------------------------------------------------

static IFile *createS3File(const char *s3FileName)
{
    ensureAWSInitialized();
    return new S3File(s3FileName);
}


//---------------------------------------------------------------------------------------------------------------------

class S3FileHook : public CInterfaceOf<IContainedFileHook>
{
public:
    virtual IFile * createIFile(const char *fileName)
    {
        if (isS3FileName(fileName))
            return createS3File(fileName);
        else
            return NULL;
    }
    virtual IAPICopyClient * getCopyApiClient(IStorageApiInfo * source, IStorageApiInfo * target) override
    {
        return nullptr;
    }

protected:
    static bool isS3FileName(const char *fileName)
    {
        if (!startsWith(fileName, s3FilePrefix))
            return false;
        const char * filename = fileName + strlen(s3FilePrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            return false;
        return true;
    }
};
static S3FileHook * s3FileHook = nullptr;

static CriticalSection hookCS;

extern S3FILE_API void installFileHook()
{
    CriticalBlock b(hookCS); // Probably overkill!
    if (!s3FileHook)
    {
        s3FileHook = new S3FileHook;
        addContainedFileHook(s3FileHook);
    }
}

extern S3FILE_API void removeFileHook()
{
    CriticalBlock b(hookCS); // Probably overkill!
    if (s3FileHook)
    {
        removeContainedFileHook(s3FileHook);
        delete s3FileHook;
        s3FileHook = NULL;
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
        s3FileHook = NULL;
    }
}
