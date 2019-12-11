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
#include "jlog.hpp"
#include "azurefile.hpp"

//#undef some macros that clash with macros or function defined within the azure library
#undef GetCurrentThreadId
#undef __declspec

#include <was/storage_account.h>
#include <was/blob.h>
#include <sstream>


#define TRACE_AZURE
#define TEST_AZURE_PAGING

/*
 * Azure related comments
 *
 * Does it make more sense to create input and output streams directly from the IFile rather than going via
 * an IFileIO.  E.g., append blobs
 */
constexpr const char * azureFilePrefix = "azure://";
#ifdef TEST_AZURE_PAGING
constexpr offset_t azureReadRequestSize = 50;
#else
constexpr offset_t azureReadRequestSize = 0x400000;  // Default to requesting 4Mb each time
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
}

void traceStorageError(const std::exception & e)
{
#ifdef TRACE_AZURE
    auto storageException = dynamic_cast<const azure::storage::storage_exception *>(&e);
    if (storageException)
    {
        azure::storage::request_result result = storageException->result();
        azure::storage::storage_extended_error extended_error = result.extended_error();
        if (!extended_error.message().empty())
        {
            DBGLOG("ERROR: %s", extended_error.message().c_str());
        }
    }
    else
        DBGLOG("ERROR: %s", e.what());
#endif
}

//---------------------------------------------------------------------------------------------------------------------

class FileIOStats
{
public:
    ~FileIOStats()
    {
        printf("Reads: %u  Bytes: %u  TimeMs: %u\n", (unsigned)ioReads, (unsigned)ioReadBytes, (unsigned)cycle_to_millisec(ioReadCycles));
    }
    unsigned __int64 getStatistic(StatisticKind kind);

public:
    RelaxedAtomic<cycle_t> ioReadCycles{0};
    RelaxedAtomic<cycle_t> ioWriteCycles{0};
    RelaxedAtomic<__uint64> ioReadBytes{0};
    RelaxedAtomic<__uint64> ioWriteBytes{0};
    RelaxedAtomic<__uint64> ioReads{0};
    RelaxedAtomic<__uint64> ioWrites{0};
    RelaxedAtomic<__uint64> remoteOperations{0};
};

typedef concurrency::streams::container_buffer<std::vector<uint8_t>> BlobPageContents;
class AzureFile;
class AzureFileIO : implements CInterfaceOf<IFileIO>
{
public:
    AzureFileIO(AzureFile * _file, const FileIOStats & _stats);

    virtual size32_t read(offset_t pos, size32_t len, void * data) override;
    virtual offset_t size() override;
    virtual void close() override
    {
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
    Linked<AzureFile> file;
    CriticalSection cs;
    offset_t startResultOffset = 0;
    offset_t endResultOffset = 0;
    BlobPageContents contents;
    FileIOStats stats;
};

class AzureFile : implements CInterfaceOf<IFile>
{
    friend class AzureFileIO;
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
            return notFound;
        return isDir ? foundYes : foundNo;
    }
    virtual fileBool isFile()
    {
        ensureMetaData();
        if (!fileExists)
            return notFound;
        return !isDir ? foundYes : foundNo;
    }
    virtual fileBool isReadOnly()
    {
        ensureMetaData();
        if (!fileExists)
            return notFound;
        return foundYes;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && fileExists);
        return createFileIO();
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && fileExists);
        return createFileIO();
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
    virtual bool createDirectory() { UNIMPLEMENTED; }
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
    bool openBlob(FileIOStats & stats);
    void readBlock(BlobPageContents & contents, FileIOStats & stats, offset_t from = 0, offset_t length = unknownFileSize);
    void ensureMetaData();
    void gatherMetaData();
    IFileIO * createFileIO();

protected:
    StringBuffer fullName;
    StringBuffer bucketName;
    StringBuffer objectName;
    azure::storage::cloud_block_blob blobResult;
    offset_t fileSize = unknownFileSize;
    bool haveMeta = false;
    bool isDir = false;
    bool fileExists = false;
    int64_t modifiedMsTime = 0;
    SpinLock lock;
};


//---------------------------------------------------------------------------------------------------------------------

AzureFileIO::AzureFileIO(AzureFile * _file, const FileIOStats & _firstStats)
: file(_file), stats(_firstStats)
{
    startResultOffset = 0;
    endResultOffset = 0;
}

size32_t AzureFileIO::read(offset_t pos, size32_t len, void * data)
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

        file->readBlock(contents, stats, pos, readSize);
        offset_t contentSize = contents.size();
        //If the results are inconsistent then do not loop forever
        if (contentSize == 0)
            return sizeRead;

        startResultOffset = pos;
        endResultOffset = pos + contentSize;
    }
}

offset_t AzureFileIO::size()
{
    return file->size();
}

size_t AzureFileIO::extractDataFromResult(size_t offset, size_t length, void * target)
{
    std::vector<uint8_t>& data = contents.collection();
    memcpy(target, &data[offset], length);
    return length;
}

unsigned __int64 AzureFileIO::getStatistic(StatisticKind kind)
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

AzureFile::AzureFile(const char *_azureFileName) : fullName(_azureFileName)
{
    const char * filename = fullName + strlen(azureFilePrefix);
    const char * slash = strchr(filename, '/');
    assertex(slash);

    bucketName.append(slash-filename, filename);
    objectName.set(slash+1);
}

bool AzureFile::getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
{
    ensureMetaData();
    if (createTime)
        createTime->clear();
    if (modifiedTime)
    {
        modifiedTime->clear();
        modifiedTime->set((time_t)(modifiedMsTime / 1000));
    }
    if (accessedTime)
        accessedTime->clear();
    return false;
}

void AzureFile::readBlock(BlobPageContents & contents, FileIOStats & stats, offset_t from, offset_t length)
{
    CCycleTimer timer;
    {
        contents.seekpos(0, std::ios_base::out);
        contents.collection().clear();
//        contents.collection().shrink_to_fit();
        concurrency::streams::ostream output_stream(contents);
        if (length == unknownFileSize)
            blobResult.download_to_stream(output_stream);
        else
            blobResult.download_range_to_stream(output_stream, from, length);
    }

    stats.ioReads++;
    stats.ioReadCycles += timer.elapsedCycles();
    stats.ioReadBytes += contents.size();
}

bool AzureFile::openBlob(FileIOStats & stats)
{
    try
    {
        CCycleTimer timer;
        const char * storage_connection_string = getenv("AZURE_CONNECTION_STRING");

        // Retrieve storage account from connection string.
        azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(storage_connection_string);

        // Create the blob client.
        azure::storage::cloud_blob_client blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a previously created container.
        azure::storage::cloud_blob_container container = blob_client.get_container_reference(bucketName.str());

        // Retrieve reference to a blob named "my-blob-1".
        blobResult = container.get_block_blob_reference(objectName.str());

        stats.ioReads++;
        stats.ioReadCycles += timer.elapsedCycles();
//        stats.ioReadBytes += properties.size();
        return true;
    }
    catch (const azure::storage::storage_exception& e)
    {
        fileExists = false;
        traceStorageError(e);
        return false;
    }
}

IFileIO * AzureFile::createFileIO()
{
    //Read the first chunk of the file.  If it is the full file then fill in the meta information, otherwise
    //ensure the meta information is calculated before creating the file IO object
    FileIOStats readStats;

    SpinBlock block(lock);
    if (!openBlob(readStats))
        return nullptr;

    return new AzureFileIO(this, readStats);
}

void AzureFile::ensureMetaData()
{
    SpinBlock block(lock);
    if (haveMeta)
        return;

    gatherMetaData();
    haveMeta = true;
}

void AzureFile::gatherMetaData()
{
    FileIOStats stats;
    openBlob(stats);

    //Is this needed?
    blobResult.download_attributes();
    auto properties = blobResult.properties();

    fileExists = true;
    fileSize = properties.size();
    modifiedMsTime = properties.last_modified().utc_timestamp();    // Could be more accurate

}


bool AzureFile::remove()
{
    try
    {
        FileIOStats stats;
        openBlob(stats);
        blobResult.delete_blob();
        return true;
    }
    catch (const std::exception & e)
    {
        traceStorageError(e);
        return false;
    }
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
    virtual IFile * createIFile(const char *fileName)
    {
        if (isAzureFileName(fileName))
            return createAzureFile(fileName);
        else
            return NULL;
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

static CriticalSection *cs;

extern AZURE_FILE_API void installFileHook()
{
    CriticalBlock b(*cs); // Probably overkill!
    if (!azureFileHook)
    {
        azureFileHook = new AzureFileHook;
        addContainedFileHook(azureFileHook);
    }
}

extern AZURE_FILE_API void removeFileHook()
{
    if (cs)
    {
        CriticalBlock b(*cs); // Probably overkill!
        if (azureFileHook)
        {
            removeContainedFileHook(azureFileHook);
            delete azureFileHook;
            azureFileHook = NULL;
        }
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    cs = new CriticalSection;
    azureFileHook = NULL;  // Not really needed, but you have to have a modinit to match a modexit
    return true;
}

MODULE_EXIT()
{
    if (azureFileHook)
    {
        removeContainedFileHook(azureFileHook);
        azureFileHook = NULL;
    }
    ::Release(azureFileHook);
    delete cs;
    cs = NULL;
}
