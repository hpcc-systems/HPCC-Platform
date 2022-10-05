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
#include "azurefile.hpp"

#include <chrono>
#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/shares.hpp>

using namespace Azure::Storage;
using namespace Azure::Storage::Blobs;
using namespace std::chrono;

#define TRACE_AZURE
//#define TEST_AZURE_PAGING

/*
 * Azure related comments
 *
 * Does it make more sense to create input and output streams directly from the IFile rather than going via
 * an IFileIO.  E.g., append blobs
 */
constexpr const char * azureFilePrefix = "azure:";
#ifdef TEST_AZURE_PAGING
constexpr offset_t azureReadRequestSize = 50;
#else
constexpr offset_t azureReadRequestSize = 0x400000;  // Default to requesting 4Mb each time
#endif

//---------------------------------------------------------------------------------------------------------------------

class AzureFile;
class AzureFileReadIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureFileReadIO(AzureFile * _file, const FileIOStats & _stats);

    virtual size32_t read(offset_t pos, size32_t len, void * data) override;
    virtual offset_t size() override;
    virtual void close() override
    {
    }

    // Write methods not implemented - this is a read-only file
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override
    {
        throwUnexpectedX("Writing to read only file");
    }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override
    {
        throwUnexpectedX("Appending to read only file");
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpectedX("Setting size of read only azure file");
    }
    virtual void flush() override
    {
    }
    unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    size_t extractDataFromResult(size_t offset, size_t length, void * target);

protected:
    Linked<AzureFile> file;
    CriticalSection cs;
    offset_t startResultOffset = 0;
    offset_t endResultOffset = 0;
    MemoryBuffer contents;
    FileIOStats stats;
};


class AzureFileWriteIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureFileWriteIO(AzureFile * _file);
    virtual void beforeDispose() override;

    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        throwUnexpected();
    }

    virtual offset_t size() override;
    virtual void setSize(offset_t size) override;
    virtual void flush() override;

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    Linked<AzureFile> file;
    CriticalSection cs;
    FileIOStats stats;
    offset_t offset = 0;
};

class AzureFileAppendBlobWriteIO final : implements AzureFileWriteIO
{
public:
    AzureFileAppendBlobWriteIO(AzureFile * _file);

    virtual void close() override;
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override;
    virtual offset_t size() override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;
};

class AzureFileBlockBlobWriteIO final : implements AzureFileWriteIO
{
public:
    AzureFileBlockBlobWriteIO(AzureFile * _file);

    virtual void close() override;
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;
};


class AzureFile : implements CInterfaceOf<IFile>
{
    friend class AzureFileReadIO;
    friend class AzureFileAppendBlobWriteIO;
    friend class AzureFileBlockBlobWriteIO;
public:
    AzureFile(const char *_azureFileName);
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
        UNIMPLEMENTED_X("AzureFile::directoryFiles");
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
        DBGLOG("AzureFile::setTime ignored");
        return false;
    }
    virtual bool remove() override;
    virtual void rename(const char *newTail) override { UNIMPLEMENTED_X("AzureFile::rename"); }
    virtual void move(const char *newName) override { UNIMPLEMENTED_X("AzureFile::move"); }
    virtual void setReadOnly(bool ro) override { UNIMPLEMENTED_X("AzureFile::setReadOnly"); }
    virtual void setFilePermissions(unsigned fPerms) override
    {
        DBGLOG("AzureFile::setFilePermissions() ignored");
    }
    virtual bool setCompression(bool set) override { UNIMPLEMENTED_X("AzureFile::setCompression"); }
    virtual offset_t compressedSize() override { UNIMPLEMENTED_X("AzureFile::compressedSize"); }
    virtual unsigned getCRC() override { UNIMPLEMENTED_X("AzureFile::getCRC"); }
    virtual void setCreateFlags(unsigned short cflags) override { UNIMPLEMENTED_X("AzureFile::setCreateFlags"); }
    virtual void setShareMode(IFSHmode shmode) override { UNIMPLEMENTED_X("AzureFile::setSharedMode"); }
    virtual bool createDirectory() override;
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) override { UNIMPLEMENTED_X("AzureFile::monitorDirectory"); }
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureFile::copySection"); }
    virtual void copyTo(IFile *dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress *progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureFile::copyTo"); }
    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false) override { UNIMPLEMENTED_X("AzureFile::openMemoryMapped"); }

protected:
    std::shared_ptr<StorageSharedKeyCredential> getCredentials() const;
    std::string getBlobUrl() const;
    std::shared_ptr<BlobContainerClient> getBlobContainerClient() const;
    template<typename T> std::shared_ptr<T> getClient() const;
    void createAppendBlob();
    void appendToAppendBlob(size32_t len, const void * data);
    void createBlockBlob();
    void appendToBlockBlob(size32_t len, const void * data);

    offset_t readBlock(MemoryBuffer & contents, FileIOStats & stats, offset_t from = 0, offset_t length = unknownFileSize);
    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileReadIO();
    IFileIO * createFileWriteIO();
    void setProperties(int64_t _blobSize, Azure::DateTime _lastModified, Azure::DateTime _createdOn);
protected:
    StringBuffer fullName;
    StringAttr accountName;
    StringAttr accountKey;
    StringAttr containerName;
    StringAttr blobName;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    time_t lastModified = 0;
    time_t createdOn = 0;
    std::string blobUrl;
    CriticalSection cs;
};


//---------------------------------------------------------------------------------------------------------------------

AzureFileReadIO::AzureFileReadIO(AzureFile * _file, const FileIOStats & _firstStats)
: file(_file), stats(_firstStats)
{
    startResultOffset = 0;
    endResultOffset = 0;
}

size32_t AzureFileReadIO::read(offset_t pos, size32_t len, void * data)
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
    //It might be worth revisiting (although I'm not sure what effect stranding has)
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

#ifdef TEST_AZURE_PAGING
        offset_t readSize = azureReadRequestSize;
#else
        offset_t readSize = (len > azureReadRequestSize) ? len : azureReadRequestSize;
#endif

        offset_t contentSize = file->readBlock(contents, stats, pos, readSize);
        //If the results are inconsistent then do not loop forever
        if (contentSize == 0)
            return sizeRead;

        startResultOffset = pos;
        endResultOffset = pos + contentSize;
    }
}

offset_t AzureFileReadIO::size()
{
    return file->size();
}

size_t AzureFileReadIO::extractDataFromResult(size_t offset, size_t length, void * target)
{
    if (offset>=contents.length())
        return 0;
    const byte * base = (byte *)(contents.bufferBase())+offset;
    unsigned len = std::min(length, contents.length()-offset);
    memcpy(target, base, len);
    return len;
}

unsigned __int64 AzureFileReadIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

unsigned __int64 FileIOStats::getStatistic(StatisticKind kind)
{
    switch (kind)
    {
    case StCycleDiskReadIOCycles:
        return ioReadCycles.load();
    case StCycleDiskWriteIOCycles:
        return ioWriteCycles.load();
    case StTimeDiskReadIO:
        return cycle_to_nanosec(ioReadCycles.load());
    case StTimeDiskWriteIO:
        return cycle_to_nanosec(ioWriteCycles.load());
    case StSizeDiskRead:
        return ioReadBytes.load();
    case StSizeDiskWrite:
        return ioWriteBytes.load();
    case StNumDiskReads:
        return ioReads.load();
    case StNumDiskWrites:
        return ioWrites.load();
    }
    return 0;
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileWriteIO::AzureFileWriteIO(AzureFile * _file)
: file(_file)
{
}

void AzureFileWriteIO::beforeDispose()
{
    try
    {
        close();
    }
    catch (...)
    {
    }
}

offset_t AzureFileWriteIO::size()
{
    throwUnexpected();
}

void AzureFileWriteIO::setSize(offset_t size)
{
    UNIMPLEMENTED;
}

void AzureFileWriteIO::flush()
{
}

unsigned __int64 AzureFileWriteIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileAppendBlobWriteIO::AzureFileAppendBlobWriteIO(AzureFile * _file) : AzureFileWriteIO(_file)
{
    file->createAppendBlob();
}

void AzureFileAppendBlobWriteIO::close()
{
}

offset_t AzureFileAppendBlobWriteIO::appendFile(IFile *file, offset_t pos, offset_t len)
{
    UNIMPLEMENTED_X("AzureFileAppendBlobWriteIO::appendFile");
}

offset_t AzureFileAppendBlobWriteIO::size()
{
#ifdef TRACE_AZURE
    //The following is fairly unusual, and suggests an unnecessary operation.
    DBGLOG("Warning: Size (%" I64F "u) requested on output IO", offset);
#endif
    return offset;
}

size32_t AzureFileAppendBlobWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len)
    {
        if (offset != pos)
            throw makeStringExceptionV(100, "Azure file output only supports append.  File %s %" I64F "u v %" I64F "u", file->queryFilename(), pos, offset);

        stats.ioWrites++;
        stats.ioWriteBytes += len;
        CCycleTimer timer;
        {
            file->appendToAppendBlob(len, data);
            offset += len;
        }
        stats.ioWriteCycles += timer.elapsedCycles();
    }
    return len;
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileBlockBlobWriteIO::AzureFileBlockBlobWriteIO(AzureFile * _file) : AzureFileWriteIO(_file)
{
    file->createAppendBlob();
}

void AzureFileBlockBlobWriteIO::close()
{

}

offset_t AzureFileBlockBlobWriteIO::appendFile(IFile *file, offset_t pos, offset_t len)
{
    UNIMPLEMENTED_X("AzureFileBlockBlobWriteIO::appendFile");
    return 0;
}

size32_t AzureFileBlockBlobWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len)
    {
        assertex(offset == pos);
        file->appendToBlockBlob(len, data);
        offset += len;
    }
    return len;
}

//---------------------------------------------------------------------------------------------------------------------
static bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

static std::shared_ptr<StorageSharedKeyCredential> getCredentials(const char * accountName, const char * key)
{
    //MORE: The client should be cached and shared between different file access - implement when secret storage is added.
    StringBuffer keyTemp;
    if (!accountName)
        accountName = getenv("AZURE_ACCOUNT_NAME");
    if (!key)
    {
        key = getenv("AZURE_ACCOUNT_KEY");
        if (!key)
        {
            StringBuffer secretName;
            secretName.append("azure-").append(accountName);
            getSecretValue(keyTemp, "storage", secretName, "key", true);
            //Trim trailing whitespace/newlines in case the secret has been entered by hand e.g. on bare metal
            size32_t len = keyTemp.length();
            for (;;)
            {
                if (!len)
                    break;
                if (isBase64Char(keyTemp.charAt(len-1)))
                    break;
                len--;
            }
            keyTemp.setLength(len);
            key = keyTemp.str();
        }
    }
    try
    {
        return std::make_shared<StorageSharedKeyCredential>(accountName, key);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(-1, "Azure access: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
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

AzureFile::AzureFile(const char *_azureFileName) : fullName(_azureFileName)
{
    const char * filename = fullName + strlen(azureFilePrefix);
    if (filename[0] != '/' || filename[1] != '/')
        throw makeStringException(99, "// missing from azure: file reference");

    //Allow the access key to be provided after the // before a @  i.e. azure://<account>:<access-key>@...
    filename += 2;

    //Allow the account and key to be quoted so that it can support slashes within the access key (since they are part of base64 encoding)
    //e.g. i.e. azure://'<account>:<access-key>'@...
    StringBuffer accessExtra;
    if (filename[0] == '"' || filename[0] == '\'')
    {
        const char * endQuote = strchr(filename + 1, filename[0]);
        if (!endQuote)
            throw makeStringException(99, "access key is missing terminating quote");
        accessExtra.append(endQuote - (filename + 1), filename + 1);
        filename = endQuote+1;
        if (*filename != '@')
            throw makeStringException(99, "missing @ following quoted access key");
        filename++;
    }

    const char * at = strchr(filename, '@');
    const char * slash = strchr(filename, '/');
    assertex(slash);  // could probably relax this....

    //Possibly pedantic - only spot @s before the first leading /
    if (at && (!slash || at < slash))
    {
        accessExtra.append(at - filename, filename);
        filename = at+1;
    }

    if (accessExtra)
    {
        const char * colon = strchr(accessExtra, ':');
        if (colon)
        {
            accountName.set(accessExtra, colon-accessExtra);
            accountKey.set(colon+1);
        }
        else
            accountName.set(accessExtra); // Key is retrieved from the secrets
    }

    containerName.set(filename, slash-filename);
    blobName.set(slash+1);
    blobUrl = ::getBlobUrl(accountName, containerName, blobName);
}

std::shared_ptr<StorageSharedKeyCredential> AzureFile::getCredentials() const
{
    return ::getCredentials(accountName, accountKey);
}

std::string AzureFile::getBlobUrl() const
{
    return blobUrl;
}

std::shared_ptr<BlobContainerClient> AzureFile::getBlobContainerClient() const
{
    auto cred = getCredentials();
    std::string blobContainerUrl = getContainerUrl(accountName, containerName);
    return std::make_shared<BlobContainerClient>(blobContainerUrl, cred);
}

template<typename T>
std::shared_ptr<T> AzureFile::getClient() const
{
    auto cred = getCredentials();
    return std::make_shared<T>(getBlobUrl(), cred);
}

bool AzureFile::createDirectory()
{
    auto blobContainerClient = getBlobContainerClient();
    try
    {
        Azure::Response<Models::CreateBlobContainerResult> result = blobContainerClient->CreateIfNotExists();
        if (result.Value.Created==false)
            OERRLOG("Azure create container: container not created");
        return result.Value.Created;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure create container failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

void AzureFile::createAppendBlob()
{
    auto appendBlobClient = getClient<AppendBlobClient>();
    try
    {
        Azure::Response<Models::CreateAppendBlobResult> result = appendBlobClient->CreateIfNotExists();
        if (result.Value.Created==false)
            OERRLOG("Azure append blob (container %s blob %s): blob not created", containerName.str(), blobName.str());
        else
        {
            setProperties(0, result.Value.LastModified, result.Value.LastModified);
        }
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure create append blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}


void AzureFile::appendToAppendBlob(size32_t len, const void * data)
{
    auto appendBlobClient = getClient<AppendBlobClient>();
    try
    {
        // MemoryBodyStream clones the data.  Future: use class derived from Azure::Core::IO::BodyStream to avoid creating a copy of data
        Azure::Core::IO::MemoryBodyStream content(reinterpret_cast <const uint8_t *>(data), len);
        appendBlobClient->AppendBlock(content);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure append blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

void AzureFile::createBlockBlob()
{
    auto blockBlobClient = getClient<BlockBlobClient>();
    try
    {
        Azure::Core::IO::MemoryBodyStream empty(nullptr, 0);
        Azure::Response<Models::UploadBlockBlobResult> result = blockBlobClient->Upload(empty); // need to do this to create an empty blob
        setProperties(0, result.Value.LastModified, result.Value.LastModified);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure create block blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

void AzureFile::appendToBlockBlob(size32_t len, const void * data)
{
    auto appendBlobClient = getClient<AppendBlobClient>();
    try
    {
        Azure::Core::IO::MemoryBodyStream content(reinterpret_cast <const uint8_t *>(data), len);
        appendBlobClient->AppendBlock(content);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure append block blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

bool AzureFile::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
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

offset_t AzureFile::readBlock(MemoryBuffer & contents, FileIOStats & stats, offset_t from, offset_t length)
{
    CCycleTimer timer;
    auto blockBlobClient = getClient<BlockBlobClient>();

    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = from;
    options.Range.Value().Length = length;
    contents.ensureCapacity(length);
    uint8_t * buffer = reinterpret_cast<uint8_t*>(contents.bufferBase());
    long int sizeRead = 0;
    try
    {
        Azure::Response<Models::DownloadBlobToResult> result = blockBlobClient->DownloadTo(buffer, length, options);
        Azure::Core::Http::HttpRange range = result.Value.ContentRange;
        if (range.Length.HasValue())
            sizeRead = range.Length.Value();
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(1234, "Azure read block blob failed: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }

    stats.ioReads++;
    stats.ioReadCycles += timer.elapsedCycles();
    stats.ioReadBytes += sizeRead;
    return sizeRead;
}

IFileIO * AzureFile::createFileReadIO()
{
    //Read the first chunk of the file.  If it is the full file then fill in the meta information, otherwise
    //ensure the meta information is calculated before creating the file IO object
    FileIOStats readStats;

    CriticalBlock block(cs);
    if (!exists())
        return nullptr;

    return new AzureFileReadIO(this, readStats);
}

IFileIO * AzureFile::createFileWriteIO()
{
    return new AzureFileAppendBlobWriteIO(this);
}

void AzureFile::ensureMetaData()
{
    CriticalBlock block(cs);
    if (haveMeta)
        return;

    gatherMetaData();
    haveMeta = true;
}

void AzureFile::gatherMetaData()
{
    CCycleTimer timer;
    auto blobClient = getClient<BlobClient>();
    try
    {
        Azure::Response<Models::BlobProperties> properties = blobClient->GetProperties();
        Models::BlobProperties & props = properties.Value;
        setProperties(props.BlobSize, props.LastModified, props.CreatedOn);
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        fileExists = false;
        fileSize = 0;
    }
}

bool AzureFile::remove()
{
    auto blobClient = getClient<BlobClient>();
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

void AzureFile::setProperties(int64_t _blobSize, Azure::DateTime _lastModified, Azure::DateTime _createdOn)
{
    haveMeta = true;
    fileExists = true;
    fileSize = _blobSize;
    lastModified = system_clock::to_time_t(system_clock::time_point(_lastModified));
    createdOn = system_clock::to_time_t(system_clock::time_point(_createdOn));
};

//---------------------------------------------------------------------------------------------------------------------

static IFile *createAzureFile(const char *azureFileName)
{
    return new AzureFile(azureFileName);
}


//---------------------------------------------------------------------------------------------------------------------

class AzureFileHook : public CInterfaceOf<IContainedFileHook>
{
public:
    virtual IFile * createIFile(const char *fileName) override
    {
        if (isAzureFileName(fileName))
            return createAzureFile(fileName);
        else
            return nullptr;
    }

protected:
    static bool isAzureFileName(const char *fileName)
    {
        if (!startsWith(fileName, azureFilePrefix))
            return false;
        const char * filename = fileName + strlen(azureFilePrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            return false;
        return true;
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
