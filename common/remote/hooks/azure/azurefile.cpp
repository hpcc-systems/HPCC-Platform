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
#include "azureapiutils.hpp"

#include <azure/core.hpp>
#include <azure/core/base64.hpp>

using namespace Azure::Storage;
using namespace Azure::Storage::Files::Shares;
using namespace std::chrono;

/*
 * Azure Files API implementation
 *
 * This implementation mirrors the Azure Blob API but uses Azure File Shares instead.
 * Azure Files provides SMB-compatible file shares in the cloud.
 */

//---------------------------------------------------------------------------------------------------------------------

using SharedFileClient = std::shared_ptr<Azure::Storage::Files::Shares::ShareFileClient>;

class AzureFile;
//The base class for AzureFileIO.  This class performs NO caching of the data - to avoid problems with
//copying the data too many times.  It is the responsibility of the caller to implement a cache if necessary.
class AzureFileIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureFileIO(AzureFile * _file, const FileIOStats & _stats);
    AzureFileIO(AzureFile * _file);

    virtual void close() override
    {
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind) override;
    virtual IFile * queryFile() const;

protected:
    Linked<AzureFile> file;
    FileIOStats stats;
    SharedFileClient shareFileClient;
};


class AzureFileReadIO final : public AzureFileIO
{
public:
    AzureFileReadIO(AzureFile * _file, const FileIOStats & _stats);

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


class AzureFileWriteIO : public AzureFileIO
{
public:
    AzureFileWriteIO(AzureFile * _file);

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

class AzureFileShareWriteIO final : implements AzureFileWriteIO
{
public:
    AzureFileShareWriteIO(AzureFile * _file);

    virtual void close() override;
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override;

private:
    void createFileIfNeeded();

    bool fileCreated = false;
    static const offset_t maxFileSize = 1024LL * 1024LL * 1024LL * 1024LL; // 1TB max file size
};


class AzureFile final : implements CInterfaceOf<IFile>
{
    friend class AzureFileIO;
    friend class AzureFileReadIO;
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
        return fileBool::foundNo; // Azure Files are not read-only by default
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
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) override { UNIMPLEMENTED_X("AzureFile::monitorDirectory"); }
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureFile::copySection"); }
    virtual void copyTo(IFile *dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress *progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("AzureFile::copyTo"); }
    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false) override { UNIMPLEMENTED_X("AzureFile::openMemoryMapped"); }

public:
    SharedFileClient getFileClient() const;
    void invalidateMeta() { haveMeta = false; }

protected:
    std::shared_ptr<StorageSharedKeyCredential> getSharedKeyCredentials() const;
    std::string getFileUrl() const;
    std::shared_ptr<ShareClient> getShareClient() const;

    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileReadIO();
    IFileIO * createFileWriteIO();
    void setProperties(int64_t _fileSize, Azure::Nullable<Azure::DateTime> _lastModified, Azure::Nullable<Azure::DateTime> _createdOn);

protected:
    StringBuffer fullName;
    StringAttr accountName;
    StringAttr shareName;
    StringBuffer secretName;
    StringAttr fileName;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    bool useManagedIdentity = false;
    time_t lastModified = 0;
    time_t createdOn = 0;
    std::string fileUrl;
    CriticalSection cs;
};

//---------------------------------------------------------------------------------------------------------------------

static std::string getShareUrl(const char *account, const char * share)
{
    std::string url("https://");
    return url.append(account).append(".file.core.windows.net/").append(share);
}

static std::string getFileUrl(const char *account, const char * share, const char *file)
{
    std::string url(getShareUrl(account, share));
    return url.append("/").append(file);
}

IFile * createAzureFile(const char * filename)
{
    return new AzureFile(filename);
}

//---------------------------------------------------------------------------------------------------------------------

AzureFile::AzureFile(const char *_azureFileName) : fullName(_azureFileName)
{
    if (startsWith(fullName, azureFilePrefix))
    {
        //format is azurefiles:plane[/device]/sharename/path
        const char * filename = fullName + strlen(azureFilePrefix);
        const char * slash = strchr(filename, '/');
        if (!slash)
            throw makeStringException(99, "Missing / in azurefiles: file reference");

        StringBuffer planeName(slash-filename, filename);
        Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeName, true);
        IPropertyTree * storageapi = plane->queryPropTree("storageapi");
        if (!storageapi)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        const char * api = storageapi->queryProp("@type");
        if (!api)
            throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

        StringBuffer azureFileAPI(strlen(azureFilePrefix) - 1, azureFilePrefix);
        if (!strieq(api, azureFileAPI.str()))
            throw makeStringExceptionV(99, "Storage api for plane %s is not azurefiles", planeName.str());

        useManagedIdentity = storageapi->getPropBool("@managed", false);
        //MORE: We could allow the managed identity/secret to be supplied in the configuration
        if (useManagedIdentity && !areManagedIdentitiesEnabled())
            throw makeStringException(99, "Managed identity is not enabled for this environment");

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

        //Parse the remaining path: sharename/filepath
        slash = strchr(filename, '/');
        if (!slash)
            throw makeStringExceptionV(99, "Missing share name in azurefiles: file reference '%s'", filename);

        shareName.set(filename, slash-filename);
        fileName.set(slash+1);
    }
    else
        throw makeStringExceptionV(99, "Unexpected prefix on azure filename %s", fullName.str());

    fileUrl = ::getFileUrl(accountName, shareName, fileName);
}

SharedFileClient AzureFile::getFileClient() const
{
    if (useManagedIdentity)
    {
        // For managed identity, create client without credentials
        // The Azure SDK will automatically use managed identity when no explicit credentials are provided
        // and the application is running in an Azure environment (VM, App Service, etc.)
        return std::make_shared<ShareFileClient>(getFileUrl());
    }
    else
    {
        auto cred = getSharedKeyCredentials();
        return std::make_shared<ShareFileClient>(getFileUrl(), cred);
    }
}

std::shared_ptr<StorageSharedKeyCredential> AzureFile::getSharedKeyCredentials() const
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

std::string AzureFile::getFileUrl() const
{
    return fileUrl;
}

std::shared_ptr<ShareClient> AzureFile::getShareClient() const
{
    if (useManagedIdentity)
    {
        // For managed identity, create client without credentials
        // The Azure SDK will automatically use managed identity when no explicit credentials are provided
        // and the application is running in an Azure environment (VM, App Service, etc.)
        return std::make_shared<ShareClient>(getShareUrl(accountName, shareName));
    }
    else
    {
        auto cred = getSharedKeyCredentials();
        return std::make_shared<ShareClient>(getShareUrl(accountName, shareName), cred);
    }
}

void AzureFile::ensureMetaData()
{
    if (haveMeta)
        return;

    CriticalBlock block(cs);
    if (haveMeta)
        return;

    gatherMetaData();
    haveMeta = true;
}

void AzureFile::gatherMetaData()
{
    auto fileClient = getFileClient();
    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Response<Models::FileProperties> response = fileClient->GetProperties();
            Models::FileProperties &props = response.Value;
            setProperties(props.FileSize, props.SmbProperties.LastWrittenOn, props.SmbProperties.CreatedOn);
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
            handleRequestException(e, "AzureFile::gatherMetaData", attempt, maxRetries, queryFilename());
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::gatherMetaData", attempt, maxRetries, queryFilename());
        }
    }
}

bool AzureFile::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
{
    ensureMetaData();
    if (!fileExists)
        return false;

    if (createTime)
        createTime->setTimeStamp(createdOn);
    if (modifiedTime)
        modifiedTime->setTimeStamp(lastModified);
    if (accessedTime)
        accessedTime->setTimeStamp(lastModified); // Use modified time as accessed time

    return true;
}

IFileIO * AzureFile::createFileReadIO()
{
    FileIOStats stats;
    return new AzureFileReadIO(this, stats);
}

IFileIO * AzureFile::createFileWriteIO()
{
    return new AzureFileShareWriteIO(this);
}

void AzureFile::setProperties(int64_t _blobSize, Azure::Nullable<Azure::DateTime> _lastModified, Azure::Nullable<Azure::DateTime> _createdOn)
{
    haveMeta = true;
    fileExists = true;
    fileSize = _blobSize;
    if (_lastModified.HasValue())
        lastModified = system_clock::to_time_t(system_clock::time_point(*_lastModified));
    else
        lastModified = 0;
    if (_createdOn.HasValue())
        createdOn = system_clock::to_time_t(system_clock::time_point(*_createdOn));
    else
        createdOn = 0;
}

bool AzureFile::remove()
{
    try
    {
        auto fileClient = getFileClient();
        fileClient->Delete();
        invalidateMeta();
        return true;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        DBGLOG("Azure Files delete failed: %s", e.Message.c_str());
        return false;
    }
}

bool AzureFile::createDirectory()
{
    std::shared_ptr<ShareClient> shareClient;
    try
    {
        // Try to create the File Share first
        shareClient = getShareClient();
        shareClient->CreateIfNotExists();
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        DBGLOG("Azure Files directory creation failed: %s", e.Message.c_str());
        return false;
    }

    // Try to create any child directories before creating the file
    const char *start = fileName.str();
    if (!isEmptyString(start))
    {
        auto dirClient = shareClient->GetRootDirectoryClient();
        while (true)
        {
            const char *slash = strchr(start, '/');
            if (!slash)
                break;

            std::string dirName(start, slash - start);
            try
            {
                dirClient = dirClient.GetSubdirectoryClient(dirName);
                dirClient.CreateIfNotExists();
            }
            catch (const Azure::Core::RequestFailedException& e)
            {
                if (e.StatusCode != Azure::Core::Http::HttpStatusCode::Conflict)
                    DBGLOG("AzureFile: Failed to create directory %s: %s", dirName.c_str(), e.Message.c_str());
            }

            start = slash + 1;
            if (!start)
                break;
        }
    }

    return true;
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileIO::AzureFileIO(AzureFile * _file, const FileIOStats & _firstStats)
: file(_file), stats(_firstStats), shareFileClient(file->getFileClient())
{

}

AzureFileIO::AzureFileIO(AzureFile * _file) : file(_file), shareFileClient(file->getFileClient())
{
}


unsigned __int64 AzureFileIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

IFile * AzureFileIO::queryFile() const
{
    return file;
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileReadIO::AzureFileReadIO(AzureFile * _file, const FileIOStats & _stats)
    : AzureFileIO(_file, _stats)
{
}


offset_t AzureFileReadIO::size()
{
    file->ensureMetaData();
    return file->size();
}

size32_t AzureFileReadIO::read(offset_t pos, size32_t len, void * data)
{
    CCycleTimer timer;
    offset_t fileSize = file->size();
    if (pos > fileSize)
        return 0;

    if (pos + len > fileSize)
        len = fileSize - pos;

    if (len == 0)
        return 0;

    Azure::Storage::Files::Shares::DownloadFileToOptions options;
    options.Range = Azure::Core::Http::HttpRange();
    options.Range.Value().Offset = pos;
    options.Range.Value().Length = len;
    uint8_t* buffer = reinterpret_cast<uint8_t*>(data);
    long int sizeRead = 0;

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Response<Models::DownloadFileToResult> result = shareFileClient->DownloadTo(buffer, len, options);
            // result.Value.FileSize is the size of the file share, not the size of the data returned, use ContentRange instead
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
            handleRequestException(e, "AzureFile::read", attempt, maxRetries, file->queryFilename(), pos, len);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::read", attempt, maxRetries, file->queryFilename(), pos, len);
        }
    }

    stats.ioReads.fastAdd(1);
    stats.ioReadCycles.fastAdd(timer.elapsedCycles());
    stats.ioReadBytes.fastAdd(sizeRead);
    return sizeRead;
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileWriteIO::AzureFileWriteIO(AzureFile * _file) : AzureFileIO(_file)
{
}

void AzureFileWriteIO::beforeDispose()
{
}

offset_t AzureFileWriteIO::size()
{
    CriticalBlock block(cs);
    return offset;
}

void AzureFileWriteIO::setSize(offset_t size)
{
    UNIMPLEMENTED;
}

void AzureFileWriteIO::flush()
{
}

//---------------------------------------------------------------------------------------------------------------------

AzureFileShareWriteIO::AzureFileShareWriteIO(AzureFile * _file) : AzureFileWriteIO(_file)
{
}

void AzureFileShareWriteIO::close()
{
    if (!fileCreated || offset == 0)
        return; // No file created or no data written

    file->invalidateMeta();

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            // Resize the file to the actual content size using SetProperties
            Azure::Storage::Files::Shares::Models::FileHttpHeaders httpHeaders;
            Azure::Storage::Files::Shares::Models::FileSmbProperties smbProperties;
            Azure::Storage::Files::Shares::SetFilePropertiesOptions options;
            options.Size = offset;

            shareFileClient->SetProperties(httpHeaders, smbProperties, options);
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::close", attempt, maxRetries, file->queryFilename(), 0, offset);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::close", attempt, maxRetries, file->queryFilename(), 0, offset);
        }
    }
}

size32_t AzureFileShareWriteIO::write(offset_t pos, size32_t len, const void * data)
{
    if (len == 0)
        return 0;

    if (pos != offset)
        throwUnexpected(); // Non-sequential writes not supported in this implementation

    file->invalidateMeta();
    createFileIfNeeded();

    CCycleTimer timer;

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;
    for (;;)
    {
        try
        {
            Azure::Core::IO::MemoryBodyStream stream(static_cast<const uint8_t*>(data), len);
            shareFileClient->UploadRange(pos, stream);
            offset += len;
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::write", attempt, maxRetries, file->queryFilename(), pos, len);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::write", attempt, maxRetries, file->queryFilename(), pos, len);
        }
    }

    stats.ioWrites.fastAdd(1);
    stats.ioWriteCycles.fastAdd(timer.elapsedCycles());
    stats.ioWriteBytes.fastAdd(len);
    return len;
}


void AzureFileShareWriteIO::createFileIfNeeded()
{
    if (fileCreated)
        return;

    constexpr unsigned maxRetries = 4;
    unsigned attempt = 0;

    for (;;)
    {
        try
        {
            // Create the file with maximum size
            shareFileClient->Create(maxFileSize);
            fileCreated = true;
            break;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::createFileIfNeeded", attempt, maxRetries, file->queryFilename(), 0, 0);
        }
        catch (const std::exception& e)
        {
            attempt++;
            handleRequestException(e, "AzureFile::createFileIfNeeded", attempt, maxRetries, file->queryFilename(), 0, 0);
        }
    }
}
