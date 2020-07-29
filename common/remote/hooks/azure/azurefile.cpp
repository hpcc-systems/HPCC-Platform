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

#include "storage_credential.h"
#include "storage_account.h"
#include "blob/blob_client.h"
#include "blob/create_block_blob_request.h"

using namespace azure::storage_lite;

//#undef some macros that clash with macros or function defined within the azure library
#undef GetCurrentThreadId
#undef __declspec


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

typedef std::stringstream BlobPageContents;
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
    BlobPageContents contents;
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
    virtual bool exists()
    {
        ensureMetaData();
        return fileExists;
    }
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime);
    virtual fileBool isDirectory()
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isFile()
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return !isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isReadOnly()
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return fileBool::foundYes;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone)
    {
        if (mode == IFOcreate)
            return createFileWriteIO();
        assertex(mode==IFOread);
        return createFileReadIO();
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone)
    {
        if (mode == IFOcreate)
            return createFileWriteIO();
        assertex(mode==IFOread);
        return createFileReadIO();
    }
    virtual const char * queryFilename()
    {
        return fullName.str();
    }
    virtual offset_t size()
    {
        ensureMetaData();
        return fileSize;
    }

// Directory functions
    virtual IDirectoryIterator *directoryFiles(const char *mask, bool sub, bool includeDirs)
    {
        UNIMPLEMENTED;
        return createNullDirectoryIterator();
    }
    virtual bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime)
    {
        ensureMetaData();
        isdir = isDir;
        size = fileSize;
        modtime.clear();
        return true;
    }

    // Not going to be implemented - this IFile interface is too big..
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) { UNIMPLEMENTED; }
    virtual bool remove();
    virtual void rename(const char *newTail) { UNIMPLEMENTED; }
    virtual void move(const char *newName) { UNIMPLEMENTED; }
    virtual void setReadOnly(bool ro) { UNIMPLEMENTED; }
    virtual void setFilePermissions(unsigned fPerms) { UNIMPLEMENTED; }
    virtual bool setCompression(bool set) { UNIMPLEMENTED; }
    virtual offset_t compressedSize() { UNIMPLEMENTED; }
    virtual unsigned getCRC() { UNIMPLEMENTED; }
    virtual void setCreateFlags(unsigned short cflags) { UNIMPLEMENTED; }
    virtual void setShareMode(IFSHmode shmode) { UNIMPLEMENTED; }
    virtual bool createDirectory();
    virtual IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL)  { UNIMPLEMENTED; }
    virtual void copySection(const RemoteFilename &dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress *progress=NULL, CFflags copyFlags=CFnone) { UNIMPLEMENTED; }
    virtual void copyTo(IFile *dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress *progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) { UNIMPLEMENTED; }
    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false)  { UNIMPLEMENTED; }

protected:
    void createAppendBlob();
    void appendToAppendBlob(size32_t len, const void * data);
    void createBlockBlob();
    void appendToBlockBlob(size32_t len, const void * data);
    void commitBlockBlob();

    offset_t readBlock(BlobPageContents & contents, FileIOStats & stats, offset_t from = 0, offset_t length = unknownFileSize);
    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileReadIO();
    IFileIO * createFileWriteIO();
    std::shared_ptr<azure::storage_lite::blob_client> getClient();

protected:
    StringBuffer fullName;
    StringAttr accountName;
    StringAttr accountKey;
    std::string containerName;
    std::string blobName;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    time_t blobModifiedTime = 0;
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
    //MORE: There are probably more efficient ways of extracting data - but this avoids the clone calling str()
    contents.seekg(offset);
    contents.read((char *)target, length);
    return contents.gcount();
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
    return 0;
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
    file->commitBlockBlob();
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

void throwStorageError(const azure::storage_lite::storage_error & error)
{
    unsigned errCode = atoi(error.code.c_str());
    throw makeStringExceptionV(errCode, "Azure Error: %s, %s", error.code.c_str(), error.code_name.c_str());
}

template <typename RESULT>
void checkError(const RESULT & result)
{
    if (!result.success())
        throwStorageError(result.error());
}

static bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

static std::shared_ptr<azure::storage_lite::blob_client> getClient(const char * accountName, const char * key)
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

    std::shared_ptr<azure::storage_lite::storage_credential> cred = nullptr;
    try
    {
        cred = std::make_shared<azure::storage_lite::shared_key_credential>(accountName, key);
    }
    catch (const std::exception & e)
    {
        IException * error = makeStringExceptionV(1234, "Azure access: %s", e.what());
        throw error;
    }

    std::shared_ptr<azure::storage_lite::storage_account> account = std::make_shared<azure::storage_lite::storage_account>(accountName, cred, /* use_https */ true);
    const int max_concurrency = 10;
    return std::make_shared<azure::storage_lite::blob_client>(account, max_concurrency);
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

    containerName.assign(filename, slash-filename);
    blobName.assign(slash+1);
}

bool AzureFile::createDirectory()
{
    for (;;)
    {
        //Ensure that the container exists...
        auto client = getClient();
        auto ret = client->create_container(containerName).get();
        if (ret.success())
            return true;    // No need to create any directory structure - all blobs are flat within the container

        DBGLOG("Failed to create container, Error: %s, %s", ret.error().code.c_str(), ret.error().code_name.c_str());

        //MORE: Need to update the code / code_names so the following works:
        if (streq(ret.error().code.c_str(), "ContainerAlreadyExists"))
            return true;
        if (!streq(ret.error().code.c_str(), "ContainerBeingDeleted"))
            return true;
        MilliSleep(100);
    }
}


void AzureFile::createAppendBlob()
{
    auto client = getClient();
    auto ret = client->create_append_blob(containerName, blobName).get();
    checkError(ret);
}


void AzureFile::appendToAppendBlob(size32_t len, const void * data)
{
    auto client = getClient();
    std::istringstream input(std::string((const char *)data, len));
    //implement append_block_from_buffer based on upload_block_from_buffer
    auto ret = client->append_block_from_stream(containerName, blobName, input).get();
    checkError(ret);
}

void AzureFile::createBlockBlob()
{
    auto client = getClient();
    auto http = client->client()->get_handle();

    auto request = std::make_shared<azure::storage_lite::create_block_blob_request>(containerName, blobName);

    auto ret = async_executor<void>::submit(client->account(), request, http, client->context()).get();
    checkError(ret);
}


void AzureFile::appendToBlockBlob(size32_t len, const void * data)
{
    auto client = getClient();
    std::string blockid;
    auto ret = client->upload_block_from_buffer(containerName, blobName, blockid, (const char *)data, len).get();
    checkError(ret);
}

void AzureFile::commitBlockBlob()
{
    auto client = getClient();
    std::vector<azure::storage_lite::put_block_list_request_base::block_item> blocks;
    auto ret = client->put_block_list(containerName, blobName, blocks, {}).get();
    checkError(ret);
}

std::shared_ptr<azure::storage_lite::blob_client> AzureFile::getClient()
{
    return ::getClient(accountName, accountKey);
}


bool AzureFile::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
{
    ensureMetaData();
    if (createTime)
        createTime->clear();
    if (modifiedTime)
    {
        modifiedTime->clear();
        modifiedTime->set(blobModifiedTime);
    }
    if (accessedTime)
        accessedTime->clear();
    return false;
}

offset_t AzureFile::readBlock(BlobPageContents & contents, FileIOStats & stats, offset_t from, offset_t length)
{
    CCycleTimer timer;
    {
        auto client = getClient();
        azure::storage_lite::blob_client_wrapper wrapper(client);

        //NOTE: Currently each call starts a new thread and then waits for the result.  It will be better in
        //the long term to avoid the wrapper calls and use the asynchronous calls to read ahead.
        contents.seekp(0);
        wrapper.download_blob_to_stream(containerName, blobName, from, length, contents);
    }

    offset_t sizeRead = contents.tellp();
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

    auto client = getClient();
    azure::storage_lite::blob_client_wrapper wrapper(client);

    auto properties = wrapper.get_blob_property(containerName, blobName);
    if (errno == 0)
    {
        fileExists = true;
        fileSize = properties.size;
        blobModifiedTime = properties.last_modified;    // Could be more accurate
        //MORE: extract information from properties.metadata
    }
    else
    {
        fileExists = false;
    }
}


bool AzureFile::remove()
{
    auto client = getClient();
    azure::storage_lite::blob_client_wrapper wrapper(client);

    wrapper.delete_blob(containerName, blobName);
    return (errno == 0);
}

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
        azureFileHook = NULL;
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    azureFileHook = NULL;  // Not really needed, but you have to have a modinit to match a modexit
    return true;
}

MODULE_EXIT()
{
    if (azureFileHook)
    {
        removeContainedFileHook(azureFileHook);
        ::Release(azureFileHook);
        azureFileHook = NULL;
    }
}
