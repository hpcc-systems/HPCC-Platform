/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifdef _WIN32
#define S_ISDIR(m) (((m)&_S_IFDIR)!=0)
#endif

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jregexp.hpp"
#include "archive.hpp"

#include <sys/stat.h>
#include <archive.h>
#include <archive_entry.h>

/*
 * Direct access to files in zip archives (and other libarchive-supported formats), without needing to extract them first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/myfile.zip/{password}/path/within/archive
 */

#ifdef _WIN32
#define ARCHIVE_SIGNATURE "[.]{zip|tar|tar[.]gz|tgz}{$|/|\\\\}"
#else
#define ARCHIVE_SIGNATURE "[.]{zip|tar|tar[.]gz|tgz}{$|/}"
#endif

static RegExpr *signature;
static SpinLock *lock;

static const char *splitName(const char *fileName)
{
    if (!fileName)
        return NULL;
    SpinBlock b(*lock);
    const char *sig = signature->find(fileName);
    if (sig)
        return sig+signature->findlen();
    else
        return NULL;
}

static void splitArchivedFileName(const char *fullName, StringAttr &container, StringAttr &option, StringAttr &relPath)
{
    const char *tail = splitName(fullName);
    assertex(tail);
    size_t containerLen = tail-fullName;
    if (fullName[containerLen-1]==PATHSEPCHAR)
        containerLen--;
    container.set(fullName, containerLen);
    if (*tail=='{')
    {
        tail++;
        const char *end = strchr(tail, '}');
        if (!end)
            throw MakeStringException(0, "Invalid archive-embedded filename - no matching } found");
        option.set(tail, end - tail);
        tail = end+1;
        if (*tail==PATHSEPCHAR)
            tail++;
        else if (*tail != 0)
            throw MakeStringException(0, "Invalid archive-embedded filename - " PATHSEPSTR " expected after }");
    }
    else
        option.clear();
    if (tail && *tail)
    {
        StringBuffer s(tail);
        s.replace(PATHSEPCHAR, '/');
        relPath.set(s);
    }
    else
        relPath.clear();
}

static StringBuffer & buildArchivedFileName(StringBuffer &fullname, const char *archiveFile, const char *option, const char *relPath)
{
    fullname.append(archiveFile);
    if (option && *option)
        fullname.append(PATHSEPCHAR).append('{').append(option).append('}');
    if (relPath && *relPath)
        fullname.append(PATHSEPCHAR).append(relPath);
    return fullname;
}

IDirectoryIterator *createArchiveDirectoryIterator(const char *gitFileName, const char *mask, bool sub, bool includeDirs);

// Wrapper around libarchive's archive_entry struct to ensure we free them at right time
// Because not clear whether safe to use a struct archive_entry object after the archive has been closed,
// we copy the info we need out of them into something we CAN be sure of the lifespan of

class ArchiveEntry : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ArchiveEntry(struct archive_entry *entry)
    {
        mode = archive_entry_filetype(entry);
        filesize = archive_entry_size(entry);
        path.set(archive_entry_pathname(entry));
        accessTime = archive_entry_atime(entry);
        createTime = archive_entry_ctime(entry);
        modifiedTime = archive_entry_mtime(entry);
    }
    bool isDir() const
    {
        return S_ISDIR(mode);
    }
    inline offset_t size()
    {
        return filesize;
    }
    const char *pathname()
    {
        return path.get();
    }
    CDateTime &getAccessTime(CDateTime &t)
    {
        t.set(accessTime);
        return t;
    }
    CDateTime &getCreateTime(CDateTime &t)
    {
        t.set(createTime);
        return t;
    }
    CDateTime &getModifiedTime(CDateTime &t)
    {
        t.set(modifiedTime);
        return t;
    }
private:
    unsigned mode;
    offset_t filesize;
    StringAttr path;
    time_t accessTime;
    time_t createTime;
    time_t modifiedTime;
};

// IFileIO implementation for reading out of libarchive-supported archives
// Because of the nature of the libarchive this may not be efficient for some archive formats
// Have to read through the entire archive directory to find the bit you want, it seems
// It's possible that we could add some seek support to at least avoid having to do so twice?

class ArchiveFileIO : public CInterface, implements IFileIO
{
public:
    IMPLEMENT_IINTERFACE;
    ArchiveFileIO(const char *_fullName) : fullName(_fullName)
    {
        // Sadly it seems we can't use a saved entry to read data from an archive. We have to open a new archive
        // object and scan through until we find the matching file, in order to extract it.
        StringAttr container, option, relpath;
        splitArchivedFileName(_fullName, container, option, relpath);
        curPos = 0;
        lastPos = 0;
        curBuffSize = 0;
        curBuff = NULL;
        archive = archive_read_new();
#ifdef _WIN32
        archive_read_support_format_zip(archive);
        archive_read_support_format_tar(archive);
        archive_read_support_compression_bzip2(archive);
#else
        archive_read_support_format_all(archive);
#if (ARCHIVE_VERSION_NUMBER >= 3000000)
        archive_read_support_filter_all(archive);
#else
        archive_read_support_compression_all(archive);
#endif
#endif
        int retcode = archive_read_open_filename(archive, container, 10240);
        if (retcode == ARCHIVE_OK)
        {
            struct archive_entry *entry = archive_entry_new();
            while (archive_read_next_header2(archive, entry) == ARCHIVE_OK)
            {
                const char *filename = archive_entry_pathname(entry);
                if (strcmp(filename, relpath.get())==0)
                {
                    fileSize = archive_entry_size(entry);
                    break;
                }
            }
            archive_entry_free(entry);
        }
    }
    ~ArchiveFileIO()
    {
#if (ARCHIVE_VERSION_NUMBER >= 3000000)
        archive_read_free(archive);
#else
        archive_read_finish(archive);
#endif
    }

    virtual size32_t read(offset_t pos, size32_t len, void * _data)
    {
        // NOTE - we don't support multithreaded access (the sequential-only restriction would make that tricky anyway)
        if (pos < lastPos)
            throw MakeStringException(0, "Only sequential access to contained file %s supported", fullName.get());
        byte *data = (byte *) _data;
        size32_t lenRequested = len;
        while (len > 0 && pos < fileSize)
        {
            if (pos >= curPos+curBuffSize)
            {
                int ret = archive_read_data_block(archive, &curBuff, &curBuffSize, &curPos);
                if (ret != ARCHIVE_OK)
                {
                    if (ret == ARCHIVE_EOF)
                        break;  // This shouldn't happen if the quoted fileSize was accurate...
                    else
                        throw MakeStringException(0, "Read error reading contained file %s", fullName.get());
                }
            }
            else
            {
                // Copy as much of the current request as we can fulfil from this block
                offset_t buffOffset = (pos - curPos);
                size_t copyLen = (curBuffSize - buffOffset) > len ? len : curBuffSize - buffOffset;  // careful for overflows, we are mixing 64/32bit values
                if (curBuff)
                    memcpy(data, ((const byte *) curBuff) + buffOffset, copyLen);
                else
                    memset(data, 0, copyLen);  // Sparse areas of compressed files may be represented with NULL buffers
                data += copyLen;
                len -= copyLen;
                pos += copyLen;
            }
        }
        lastPos = pos;
        return lenRequested - len;
    }
    virtual offset_t size()
    {
        return fileSize;
    }
    virtual void close()
    {
    }

    // Write methods not implemented - this is a read-only file
    virtual size32_t write(offset_t pos, size32_t len, const void * data)
    {
        throwUnexpected();
    }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1)
    {
        throwUnexpected();
    }
    virtual void setSize(offset_t size)
    {
        throwUnexpected();
    }
    virtual void flush()
    {
        throwUnexpected();
    }
    unsigned __int64 getStatistic(StatisticKind kind)
    {
        return 0;
    }
protected:
    struct archive *archive;
    offset_t fileSize;
#if ARCHIVE_VERSION_NUMBER < 3000000
    off_t curPos;
#else
#if defined(_WIN32)
#define	int64_t	__int64
#endif
    int64_t curPos;
#endif
    offset_t lastPos;
    size_t curBuffSize;
    const void *curBuff;
    StringAttr fullName;
};

// IFile implementation for reading out of libarchive-supported archives
// These use the struct_archive_entry objects allocated in the directory iterator
// in the hope they might be useful for directly seeking to the file to be extracted
// at some point.

class ArchiveFile : public CInterface, implements IFile
{
public:
    IMPLEMENT_IINTERFACE;
    ArchiveFile(const char *_fileName, ArchiveEntry *_entry)
    : fullName(_fileName),entry(_entry)
    {
    }
    virtual bool exists()
    {
        return entry != NULL;
    }
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
    {
        if (entry)
        {
            if (accessedTime)
                entry->getAccessTime(*accessedTime);
            if (createTime)
                entry->getCreateTime(*createTime);
            if (modifiedTime)
                entry->getModifiedTime(*modifiedTime);
            return true;
        }
        else
            return false;
    }
    virtual fileBool isDirectory()
    {
        if (!entry)
            return notFound;
        return entry->isDir() ? foundYes : foundNo;
    }
    virtual fileBool isFile()
    {
        if (!entry)
            return notFound;
        return entry->isDir() ? foundNo : foundYes;
    }
    virtual fileBool isReadOnly()
    {
        if (!entry)
            return notFound;
        return foundYes;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && entry != NULL);
        return new ArchiveFileIO(fullName.str());
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && entry != NULL);
        return new ArchiveFileIO(fullName.str());
    }
    virtual const char * queryFilename()
    {
        return fullName.str();
    }
    virtual offset_t size()
    {
        if (!entry)
            return 0;
        return entry->size();
    }

// Directory functions
    virtual IDirectoryIterator *directoryFiles(const char *mask, bool sub, bool includeDirs)
    {
        if (isDirectory() != foundYes || (mask && !*mask))   // Empty mask string means matches nothing - NULL means matches everything
            return createNullDirectoryIterator();
        else
        {
            StringBuffer dirName(fullName);
            dirName.append(PATHSEPCHAR);
            return createArchiveDirectoryIterator(dirName, mask, sub, includeDirs);
        }
    }
    virtual bool getInfo(bool &_isdir,offset_t &_size,CDateTime &_modtime)
    {
        _isdir = isDirectory()==foundYes;
        _size = size();
        _modtime.clear(); // MORE could probably do better
        return true; // MORE should this be false if not existing?
    }

    // Not going to be implemented - this IFile interface is too big..
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) { UNIMPLEMENTED; }
    virtual bool remove() { UNIMPLEMENTED; }
    virtual void rename(const char *newTail) { UNIMPLEMENTED; }
    virtual void move(const char *newName) { UNIMPLEMENTED; }
    virtual void setReadOnly(bool ro) { UNIMPLEMENTED; }
    virtual bool setCompression(bool set) { UNIMPLEMENTED; }
    virtual offset_t compressedSize() { UNIMPLEMENTED; }
    virtual unsigned getCRC() { UNIMPLEMENTED; }
    virtual void setCreateFlags(unsigned cflags) { UNIMPLEMENTED; }
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
    virtual void treeCopyTo(IFile *dest,IpSubNet &subnet,IpAddress &resfrom,bool usetmp=false,CFflags copyFlags=CFnone) { UNIMPLEMENTED; }


protected:
    StringBuffer fullName;
    Linked<ArchiveEntry> entry;
};

static IFile *createIFileInArchive(const char *containedFileName)
{
    StringBuffer fname(containedFileName);
    assertex(fname.length());
    removeTrailingPathSepChar(fname);
    StringAttr container, option, relpath;
    splitArchivedFileName(fname.str(), container, option, relpath);
    if (relpath.length())
    {
        StringBuffer dirPath, dirTail;
        dirPath.append(container).append(option);
        splitFilename(relpath, &dirPath, &dirPath, &dirTail, &dirTail);
        Owned<IDirectoryIterator> dir = createArchiveDirectoryIterator(dirPath.str(), dirTail.str(), false, true);
        if (dir->first())
        {
            Linked<IFile> file = &dir->query();
            assertex(!dir->next());
            return file.getClear();
        }
        else
            return new ArchiveFile(containedFileName, NULL);
    }
    else
    {
        // Create an IFile representing the root of the archive as a directory
        struct archive_entry *rootEntry = archive_entry_new();
        archive_entry_set_pathname(rootEntry, ".");
        archive_entry_set_mode(rootEntry, S_IFDIR);
        archive_entry_set_size(rootEntry, 0);
        return new ArchiveFile(containedFileName, new ArchiveEntry(rootEntry));
    }
}

class ArchiveDirectoryIterator : public CInterface, implements IDirectoryIterator
{
public:
    IMPLEMENT_IINTERFACE;
    ArchiveDirectoryIterator(const char *_containedFileName, const char *_mask, bool _sub, bool _includeDirs)
    : mask(_mask), sub(_sub), includeDirs(_includeDirs)
    {
        splitArchivedFileName(_containedFileName, container, option, relDir);
        curIndex = 0;
    }
    virtual StringBuffer &getName(StringBuffer &buf)
    {
        assertex(curFile);
        return buf.append(curFile->queryFilename());
    }
    virtual bool isDir()
    {
        assertex(curFile);
        return curFile->isDirectory();
    }
    virtual __int64 getFileSize()
    {
        assertex(curFile);
        return curFile->size();
    }
    virtual bool getModifiedTime(CDateTime &ret)
    {
        UNIMPLEMENTED;
    }
    virtual bool first()
    {
        curFile.clear();
        entries.kill();
        curIndex = 0;
        struct archive *archive = archive_read_new();
#ifdef _WIN32
        archive_read_support_format_zip(archive);
        archive_read_support_format_tar(archive);
        archive_read_support_compression_bzip2(archive);
#else
        archive_read_support_format_all(archive);
#if (ARCHIVE_VERSION_NUMBER >= 3000000)
        archive_read_support_filter_all(archive);
#else
        archive_read_support_compression_all(archive);
#endif
#endif
        int retcode = archive_read_open_filename(archive, container, 10240);
        if (retcode == ARCHIVE_OK)
        {
            struct archive_entry *entry = archive_entry_new();
            while (archive_read_next_header2(archive, entry) == ARCHIVE_OK)
            {
                unsigned mode = archive_entry_filetype(entry);
                bool isDir = S_ISDIR(mode);
                if (includeDirs || !isDir)
                {
                    const char *filename = archive_entry_pathname(entry);
                    if (memcmp(filename, relDir.get(), relDir.length())==0)
                    {
                        StringBuffer tail(filename + relDir.length());
                        if (tail.length())
                        {
                            if (tail.charAt(tail.length()-1)=='/' || tail.charAt(tail.length()-1)==PATHSEPCHAR)
                                tail.remove(tail.length()-1, 1);
                        }
                        else
                        {
                            assert(isDir);
                            tail.append(".");
                        }
                        // Strip off a trailing /, then check that there is no / in the tail
                        if (strchr(tail, PATHSEPCHAR) == NULL &&  (!mask.length() || WildMatch(tail, mask, false)))
                        {
                            entries.append(*new ArchiveEntry(entry));
                        }
                    }
                }
            }
            archive_entry_free(entry);
        }
#if (ARCHIVE_VERSION_NUMBER >= 3000000)
        archive_read_free(archive);
#else
        archive_read_finish(archive);
#endif
        return next();
    }

    virtual bool next()
    {
        if (entries.isItem(curIndex))
        {
            ArchiveEntry &entry = entries.item(curIndex);
            curIndex++;
            const char *filename = entry.pathname();
            StringBuffer containedFileName;
            buildArchivedFileName(containedFileName, container, option, filename);
            removeTrailingPathSepChar(containedFileName);
            curFile.setown(new ArchiveFile(containedFileName, &entry));
            return true;
        }
        else
        {
            curFile.clear();
            return false;
        }
    }
    virtual bool isValid()  { return curFile != NULL; }
    virtual IFile & query() { return *curFile; }
protected:
    StringAttr container;
    StringAttr option;
    StringAttr relDir;
    StringAttr mask;
    Owned<IFile> curFile;
    unsigned curIndex;
    IArrayOf<ArchiveEntry> entries;  // The entries that matched
    bool includeDirs;
    bool sub;

};

IDirectoryIterator *createArchiveDirectoryIterator(const char *gitFileName, const char *mask, bool sub, bool includeDirs)
{
    assertex(sub==false);  // I don't know what it means!
    return new ArchiveDirectoryIterator(gitFileName, mask, sub, includeDirs);
}

class CArchiveFileHook : public CInterface, implements IContainedFileHook
{
public:
    IMPLEMENT_IINTERFACE;
    virtual IFile * createIFile(const char *fileName)
    {
        if (isArchiveFileName(fileName))
            return createIFileInArchive(fileName);
        else
            return NULL;
    }

protected:
    static bool isArchiveFileName(const char *fileName)
    {
        if (fileName)
            return splitName(fileName) != NULL;
        return false;
    }
} *archiveFileHook;

extern ARCHIVEFILE_API void installFileHook()
{
    SpinBlock b(*lock); // Probably overkill!
    if (!archiveFileHook)
    {
        archiveFileHook = new CArchiveFileHook;
        addContainedFileHook(archiveFileHook);
    }
}

extern ARCHIVEFILE_API void removeFileHook()
{
    if (lock)
    {
        SpinBlock b(*lock); // Probably overkill!
        if (archiveFileHook)
        {
            removeContainedFileHook(archiveFileHook);
            delete archiveFileHook;
            archiveFileHook = NULL;
        }
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    lock = new SpinLock;
    signature = new RegExpr(ARCHIVE_SIGNATURE);
    archiveFileHook = NULL;
    return true;
}

MODULE_EXIT()
{
    if (archiveFileHook)
    {
        removeContainedFileHook(archiveFileHook);
        archiveFileHook = NULL;
    }
    delete signature;
    delete lock;
    lock = NULL;
    signature = NULL;
    ::Release(archiveFileHook);
}
