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

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jregexp.hpp"
#include "gitfile.hpp"

/*
 * Direct access to files in git repositories, by revision, without needing to check them out first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/.git/{revision}/path/within/git
 * Bare repositories of the form  /my/directory.git/{revision}/path/within/git also supported
 */

IDirectoryIterator *createGitRepositoryDirectoryIterator(const char *gitFileName, const char *mask=NULL, bool sub=false,bool includedirs=false);

static void splitGitFileName(const char *fullName, StringAttr &gitDir, StringAttr &revision, StringAttr &relPath)
{
    assertex(fullName);
    const char *git = strstr(fullName, ".git" PATHSEPSTR "{" );
    assertex(git);
    const char *tail = git+5;
    gitDir.set(fullName, tail-fullName);
    assertex (*tail=='{');
    tail++;
    const char *end = strchr(tail, '}');
    if (!end)
        throw MakeStringException(0, "Invalid git repository filename - no matching } found");
    revision.set(tail, end - tail);
    tail = end+1;
    if (*tail==PATHSEPCHAR)
        tail++;
    else if (*tail != 0)
        throw MakeStringException(0, "Invalid git repository filename - " PATHSEPSTR " expected after }");
    if (tail && *tail)
    {
        StringBuffer s(tail);
        s.replace(PATHSEPCHAR, '/');
        relPath.set(s);
    }
    else
        relPath.clear();
    // Check it's a valid git repository
    StringBuffer configName(gitDir);
    configName.append("config");
    if (!checkFileExists(configName.str()))
        throw MakeStringException(0, "Invalid git repository - config file %s not found", configName.str());
}

static StringBuffer & buildGitFileName(StringBuffer &fullname, const char *gitDir, const char *revision, const char *relPath)
{
    fullname.append(gitDir);
    fullname.append('{').append(revision).append('}').append(PATHSEPCHAR);
    if (relPath && *relPath)
        fullname.append(relPath);
    return fullname;
}

class GitRepositoryFileIO : public CInterface, implements IFileIO
{
public:
    IMPLEMENT_IINTERFACE;
    GitRepositoryFileIO(const char * gitDirectory, const char * revision, const char * relFileName)
    {
        VStringBuffer gitcmd("git --git-dir=%s show %s:%s", gitDirectory, (revision && *revision) ? revision : "HEAD", relFileName);
        Owned<IPipeProcess> pipe = createPipeProcess();
        if (pipe->run("git", gitcmd, ".", false, true, false, 0))
        {
            Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
            const size32_t chunkSize = 8192;
            for (;;)
            {
                size32_t sizeRead = pipeReader->read(chunkSize, buf.reserve(chunkSize));
                if (sizeRead < chunkSize)
                {
                    buf.setLength(buf.length() - (chunkSize - sizeRead));
                    break;
                }
            }
            pipe->closeOutput();
        }
        int retcode = pipe->wait();
        if (retcode)
        {
            buf.clear();  // Can't rely on destructor to clean this for me
            throw MakeStringException(0, "git show returned exit status %d", retcode);
        }
    }
    virtual size32_t read(offset_t pos, size32_t len, void * data)
    {
        if (pos >= buf.length())
            return 0;
        if (pos+len > buf.length())
            len = buf.length()-pos;
        memcpy(data, buf.toByteArray()+pos, len);
        return len;
    }
    virtual offset_t size()
    {
        return buf.length();
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
        //This could be implemented, but not likely to be useful so currently return nothing.
        return 0;
    }
protected:
    MemoryBuffer buf;
};

class GitRepositoryFile : public CInterface, implements IFile
{
public:
    IMPLEMENT_IINTERFACE;
    GitRepositoryFile(const char *_gitFileName, offset_t _fileSize, bool _isDir, bool _isExisting)
    : fullName(_gitFileName),fileSize(_fileSize), isDir(_isDir), isExisting(_isExisting)
    {
        splitGitFileName(fullName, gitDirectory, revision, relFileName);
    }
    virtual bool exists()
    {
        return isExisting;
    }
    virtual bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
    {
        if (createTime)
            createTime->clear();
        if (modifiedTime)
            modifiedTime->clear();
        if (accessedTime)
            accessedTime->clear();
        return false;
    }
    virtual fileBool isDirectory()
    {
        if (!isExisting)
            return notFound;
        return isDir ? foundYes : foundNo;
    }
    virtual fileBool isFile()
    {
        if (!isExisting)
            return notFound;
        return !isDir ? foundYes : foundNo;
    }
    virtual fileBool isReadOnly()
    {
        if (!isExisting)
            return notFound;
        return foundYes;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && isExisting);
        return new GitRepositoryFileIO(gitDirectory, revision, relFileName);
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone)
    {
        assertex(mode==IFOread && isExisting);
        return new GitRepositoryFileIO(gitDirectory, revision, relFileName);
    }
    virtual const char * queryFilename()
    {
        return fullName.str();
    }
    virtual offset_t size()
    {
        return fileSize;
    }

// Directory functions
    virtual IDirectoryIterator *directoryFiles(const char *mask, bool sub, bool includeDirs)
    {
        if (!isDir || (mask && !*mask))   // Empty mask string means matches nothing - NULL means matches everything
            return createNullDirectoryIterator();
        else
        {
            StringBuffer dirName(fullName);
            dirName.append(PATHSEPCHAR);
            return createGitRepositoryDirectoryIterator(dirName, mask, sub, includeDirs);
        }
    }
    virtual bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime)
    {
        isdir = isDir;
        size = fileSize;
        modtime.clear();
        return true;
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
    StringAttr gitDirectory;
    StringAttr revision;
    StringAttr relFileName;
    StringBuffer fullName;
    offset_t fileSize;
    bool isDir;
    bool isExisting;
};

static IFile *createGitFile(const char *gitFileName)
{
    StringBuffer fname(gitFileName);
    assertex(fname.length());
    removeTrailingPathSepChar(fname);
    StringAttr gitDirectory, revision, relDir;
    splitGitFileName(fname, gitDirectory, revision, relDir);
    if (relDir.isEmpty())
        return new GitRepositoryFile(fname, 0, true, true);  // Special case the root - ugly but apparently necessary
    Owned<IDirectoryIterator> dir = createGitRepositoryDirectoryIterator(fname, NULL, false, true);
    if (dir->first())
    {
        Linked<IFile> file = &dir->query();
        assertex(!dir->next());
        return file.getClear();
    }
    else
        return new GitRepositoryFile(gitFileName, 0, false, false);
}

class GitRepositoryDirectoryIterator : public CInterface, implements IDirectoryIterator
{
public:
    IMPLEMENT_IINTERFACE;
    GitRepositoryDirectoryIterator(const char *_gitFileName, const char *_mask, bool _sub, bool _includeDirs)
    : mask(_mask), sub(_sub), includeDirs(_includeDirs)
    {
        splitGitFileName(_gitFileName, gitDirectory, revision, relDir);
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
        return curFile->isDirectory()==foundYes;
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
        files.kill();
        curFile.clear();
        curIndex = 0;
        VStringBuffer gitcmd("git --git-dir=%s ls-tree --long -z %s %s", gitDirectory.get(), revision.length() ? revision.get() : "HEAD", relDir.length() ? relDir.get() : "");
        Owned<IPipeProcess> pipe = createPipeProcess();
        if (pipe->run("git", gitcmd, ".", false, true, false, 0))
        {
            Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
            char c;
            StringBuffer thisLine;
            while (pipeReader->read(sizeof(c), &c))
            {
                if (!c)
                {
                    if (thisLine.length())
                    {
                        // info from Git looks like this:
                        // 100644 blob 6c131b5954   36323  sourcedoc.xml
                        // 040000 tree 6c131b5954   -    subdir
                        char size[32];
                        char filename[1024];
                        int ret= sscanf(thisLine, "%*s %*s %*s %31s %1023s", &size[0], &filename[0]);
                        if (ret != 2)
                            throw MakeStringException(0, "Unexpected data returned from git ls-tree: %s", thisLine.str());
                        if (includeDirs || size[0]!='-')
                        {
                            const char *tail = strrchr(filename, '/'); // Git uses / even on Windows
                            if (tail)
                                tail += 1;
                            else
                                tail = filename;
                            if (!mask.length() || WildMatch(tail, mask, false))
                            {
                                files.append(tail);
                                sizes.append(size[0]=='-' ? (offset_t) -1 : _atoi64(size));
                            }
                        }
                    }
                    thisLine.clear();
                }
                else
                    thisLine.append(c);
            }
        }
        unsigned retCode = pipe->wait();
        if (retCode)
        {
            files.kill();
            return false; // Or an exception?
        }
        open();
        return isValid();
    }
    virtual bool next()
    {
        curIndex++;
        open();
        return isValid();
    }
    virtual bool isValid()  { return curFile != NULL; }
    virtual IFile & query() { return *curFile; }
protected:
    StringAttr gitDirectory;
    StringAttr revision;
    StringAttr relDir;
    StringAttr mask;
    Owned<IFile> curFile;
    unsigned curIndex;
    StringArray files;
    UInt64Array sizes;
    bool includeDirs;
    bool sub;

    void open()
    {
        if (files.isItem(curIndex))
        {
            const char *filename = files.item(curIndex);
            offset_t size = sizes.item(curIndex);
            StringBuffer gitFileName;
            buildGitFileName(gitFileName, gitDirectory, revision, relDir);
            // Git ls-tree behaves differently according to whether you put the trailing / on the path you supply.
            // With /, it gets all files in that directory
            // Without, it will return just a single match (for the file or dir with that name)
            // So we are effectively in two different modes according to which we used.
            if (gitFileName.charAt(gitFileName.length()-1)=='/')   // NOTE: / not PATHSEPCHAR - we translated to git representation
                gitFileName.append(filename);
            if (size==(offset_t) -1)
                curFile.setown(new GitRepositoryFile(gitFileName, 0, true, true));
            else
                curFile.setown(new GitRepositoryFile(gitFileName, size, false, true));
        }
        else
            curFile.clear();
    }
};

IDirectoryIterator *createGitRepositoryDirectoryIterator(const char *gitFileName, const char *mask, bool sub, bool includeDirs)
{
    assertex(sub==false);  // I don't know what it means!
    return new GitRepositoryDirectoryIterator(gitFileName, mask, sub, includeDirs);
}

class CGitRepositoryFileHook : public CInterface, implements IContainedFileHook
{
public:
    IMPLEMENT_IINTERFACE;
    virtual IFile * createIFile(const char *fileName)
    {
        if (isGitFileName(fileName))
            return createGitFile(fileName);
        else
            return NULL;
    }

protected:
    static bool isGitFileName(const char *fileName)
    {
        if (fileName && strstr(fileName, ".git" PATHSEPSTR "{"))
            return true;
        return false;
    }
} *gitRepositoryFileHook;

static CriticalSection *cs;

extern GITFILE_API void installFileHook()
{
    CriticalBlock b(*cs); // Probably overkill!
    if (!gitRepositoryFileHook)
    {
        gitRepositoryFileHook = new CGitRepositoryFileHook;
        addContainedFileHook(gitRepositoryFileHook);
    }
}

extern GITFILE_API void removeFileHook()
{
    if (cs)
    {
        CriticalBlock b(*cs); // Probably overkill!
        if (gitRepositoryFileHook)
        {
            removeContainedFileHook(gitRepositoryFileHook);
            delete gitRepositoryFileHook;
            gitRepositoryFileHook = NULL;
        }
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    cs = new CriticalSection;
    gitRepositoryFileHook = NULL;  // Not really needed, but you have to have a modinit to match a modexit
    return true;
}

MODULE_EXIT()
{
    if (gitRepositoryFileHook)
    {
        removeContainedFileHook(gitRepositoryFileHook);
        gitRepositoryFileHook = NULL;
    }
    ::Release(gitRepositoryFileHook);
    delete cs;
    cs = NULL;
}
