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

#include <azure/core.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/shares.hpp>
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
using namespace Azure::Storage::Files;
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
constexpr const char * azureBlobPrefix = "azureblob:";  // Syntax azureblob:storageplane[/device]/apth
#ifdef TEST_AZURE_PAGING
constexpr offset_t azureReadRequestSize = 50;
#else
constexpr offset_t azureReadRequestSize = 0x400000;  // Default to requesting 4Mb each time
#endif

//---------------------------------------------------------------------------------------------------------------------

class AzureFile;

//The base class for AzureFileIO.  This class performs NO caching of the data - to avoid problems with
//copying the data too many times.  It is the responsibility of the caller to implement a cache if necessary.
class AzureFileIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureFileIO(AzureFile * _file, const FileIOStats & _stats);
    AzureFileIO(AzureFile * _file) : file(_file) {}

    virtual size32_t read(offset_t pos, size32_t len, void * data) override;
    virtual offset_t size() override;
    virtual void close() override
    {
    }

    unsigned __int64 getStatistic(StatisticKind kind) override;

protected:
    Linked<AzureFile> file;
    FileIOStats stats;
};


class AzureFileReadIO : public AzureFileIO
{
public:
    AzureFileReadIO(AzureFile * _file, const FileIOStats & _stats);

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
};


class AzureFileWriteIO : public AzureFileIO
{
public:
    AzureFileWriteIO(AzureFile * _file);
    virtual void beforeDispose() override;

    virtual offset_t size() override;
    virtual void setSize(offset_t size) override;
    virtual void flush() override;

protected:
    CriticalSection cs;
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


class AzureFile final : implements CInterfaceOf<IFile>
{
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

//Helper functions for the azureFileIO classes
    offset_t read(offset_t pos, size32_t len, void * data, FileIOStats & stats);

    void createAppendBlob();
    void appendToAppendBlob(size32_t len, const void * data);
    void createBlockBlob();
    void appendToBlockBlob(size32_t len, const void * data);

protected:
    std::shared_ptr<StorageSharedKeyCredential> getCredentials() const;
    std::string getBlobUrl() const;
    std::shared_ptr<BlobContainerClient> getBlobContainerClient() const;
    template<typename T> std::shared_ptr<T> getClient() const;


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
    StringBuffer secretName;
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

AzureFileIO::AzureFileIO(AzureFile * _file, const FileIOStats & _firstStats)
: file(_file), stats(_firstStats)
{
}


size32_t AzureFileIO::read(offset_t pos, size32_t len, void * data)
{
    offset_t fileSize = file->size();
    if (pos > fileSize)
        return 0;
    if (pos + len > fileSize)
        len = fileSize - pos;
    if (len == 0)
        return 0;

    return file->read(pos, len, data, stats);
}

offset_t AzureFileIO::size()
{
    return file->size();
}

unsigned __int64 AzureFileIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

//---------------------------------------------------------------------------------------------------------------------


AzureFileReadIO::AzureFileReadIO(AzureFile * _file, const FileIOStats & _firstStats)
: AzureFileIO(_file, _firstStats)
{
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileWriteIO::AzureFileWriteIO(AzureFile * _file)
: AzureFileIO(_file)
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

//---------------------------------------------------------------------------------------------------------------------

AzureFileAppendBlobWriteIO::AzureFileAppendBlobWriteIO(AzureFile * _file) : AzureFileWriteIO(_file)
{
    file->createBlockBlob();
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
    if (startsWith(fullName, azureBlobPrefix))
    {
        //format is azureblob:plane[/device]/path
        const char * filename = fullName + strlen(azureBlobPrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            throw makeStringException(99, "Missing / in azureblob: file reference");

        StringBuffer planeName(slash-filename, filename);
        Owned<IPropertyTree> plane = getStoragePlane(planeName);
        const char * api = plane->queryProp("storageapi/@type");
        if (!api)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        constexpr size_t lenPrefix = strlen(azureBlobPrefix);
        if ((strncmp(api, azureBlobPrefix, lenPrefix-1) != 0) || api[lenPrefix-1] != ':')
            throw makeStringExceptionV(99, "Storage api for plane %s is not azureblob", planeName.str());

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

            VStringBuffer childPath("containers[%d]", device-1);
            IPropertyTree * deviceInfo = plane->queryPropTree(childPath);
            if (deviceInfo)
            {
                accountName.set(deviceInfo->queryProp("@account"));
                secretName.set(deviceInfo->queryProp("@secret"));
            }

            //If device-specific information is not provided all defaults come from the storage plane
            if (!accountName)
                accountName.set(plane->queryProp("@account"));
            if (!secretName)
                secretName.set(plane->queryProp("@secret"));

            filename = endDevice+1;
        }
        else
        {
            accountName.set(plane->queryProp("@account"));
            secretName.set(plane->queryProp("@secret"));
            filename = slash+1;
        }

        if (isEmptyString(accountName) || isEmptyString(secretName))
            throw makeStringExceptionV(99, "Missing container or secret name for plane %s", planeName.str());

        //I am not at all sure we need to split this apart, only to join in back together again.
        slash = strchr(filename, '/');
        assertex(slash);  // could probably relax this....
        containerName.set(filename, slash-filename);
        blobName.set(slash+1);
    }
    else if (startsWith(fullName, azureFilePrefix))
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
                secretName.set(colon+1);
            }
            else
            {
                accountName.set(accessExtra); // Key is retrieved from the secrets
                secretName.set("azure-").append(accountName);
            }
        }
        containerName.set(filename, slash-filename);
        blobName.set(slash+1);
    }
    else
        throw makeStringExceptionV(99, "Unexpected prefix on azure filename %s", fullName.str());

    blobUrl = ::getBlobUrl(accountName, containerName, blobName);
}

std::shared_ptr<StorageSharedKeyCredential> AzureFile::getCredentials() const
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

offset_t AzureFile::read(offset_t pos, size32_t len, void * data, FileIOStats & stats)
{
    CCycleTimer timer;
    auto blockBlobClient = getClient<BlockBlobClient>();

    Azure::Storage::Blobs::DownloadBlobToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = pos;
    options.Range.Value().Length = len;
    uint8_t * buffer = reinterpret_cast<uint8_t*>(data);
    long int sizeRead = 0;
    try
    {
        Azure::Response<Models::DownloadBlobToResult> result = blockBlobClient->DownloadTo(buffer, len, options);
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
        if (isAzureFileName(fileName))
            return createAzureFile(fileName);
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
    static bool isAzureFileName(const char *fileName)
    {
        if (startsWith(fileName, azureBlobPrefix))
            return true;
        if (startsWith(fileName, azureFilePrefix))
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
