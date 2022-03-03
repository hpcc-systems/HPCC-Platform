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
#include "jlog.hpp"

#include "git2.h"

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
    if (*tail==PATHSEPCHAR || *tail == '/')
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
    fullname.append('{').append(revision).append('}').append('/');
    if (relPath && *relPath)
        fullname.append(relPath);
    return fullname;
}


//--------------------------------------------------------------------------------------------------------------------
// New implementation using libgit2
//--------------------------------------------------------------------------------------------------------------------

static git_oid nullOid;

#define GIT_CHECK(x) check(x, #x)
class GitCommitTree : public CInterface
{
public:
    GitCommitTree(const char * directory, const char * version)
    {
        GIT_CHECK(git_repository_open(&gitRepo, directory));

        if (gitRepo)
        {
            //Check to see if the version is a tag/branch etc. - these take precedence if they happen to match a sha prefix
            git_reference * ref = nullptr;
            if (git_reference_dwim(&ref, gitRepo, version) == 0)
            {
                //Map the symbolic reference to the underlying object
                git_reference * resolvedRef = nullptr;
                if (git_reference_resolve(&resolvedRef, ref) == 0)
                {
                    const git_oid * oid = git_reference_target(resolvedRef);
                    GIT_CHECK(git_commit_lookup(&gitCommit, gitRepo, oid));
                    git_reference_free(resolvedRef);
                }

                git_reference_free(ref);
            }

            if (!gitCommit)
            {
                git_oid gitOid;
                if (git_oid_fromstrp(&gitOid, version) == 0)
                {
                    //User provided a SHA (possibly shorted) -> resolve it.  Error will be reported later if it does not exist.
                    GIT_CHECK(git_commit_lookup_prefix(&gitCommit, gitRepo, &gitOid, strlen(version)));
                }
            }

            if (gitCommit)
                GIT_CHECK(git_commit_tree(&gitRoot, gitCommit));
        }
    }
    ~GitCommitTree()
    {
        git_tree_free(gitRoot);
        git_commit_free(gitCommit);
        git_repository_free(gitRepo);
    }

    const git_tree * queryTree() const { return gitRoot; }

protected:
    void check(int code, const char * func)
    {
        if (code != 0)
        {
            const git_error * err = git_error_last();
            const char * errmsg = err ? err->message : "<unknown>";
            WARNLOG("libgit %s returned %u: %s", func, code, errmsg);
        }
    }
protected:
    git_repository * gitRepo = nullptr;
    git_commit * gitCommit = nullptr;
    git_tree * gitRoot = nullptr;
};


class GitRepositoryFileIO : implements CSimpleInterfaceOf<IFileIO>
{
public:
    GitRepositoryFileIO(GitCommitTree * commitTree, const git_oid * oid)
    {
        git_blob *blob = nullptr;
        int error = git_blob_lookup(&blob, git_tree_owner(commitTree->queryTree()), oid);
        if (error)
            throw MakeStringException(0, "git git_blob_lookup returned exit status %d", error);

        git_object_size_t blobsize = git_blob_rawsize(blob);
        const void * data = git_blob_rawcontent(blob);
        buf.append(blobsize, data);

        git_blob_free(blob);
    }
    virtual size32_t read(offset_t pos, size32_t len, void * data)
    {
        if (pos >= buf.length())
            return 0;
        if (pos+len > buf.length())
            len = buf.length()-pos;
        memcpy_iflen(data, buf.toByteArray()+pos, len);
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

class GitRepositoryFile : implements IFile, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    GitRepositoryFile(const char *_gitFileName, bool _isDir, bool _isExisting, GitCommitTree * _commitTree, const git_oid & _oid)
    : commitTree(_commitTree), oid(_oid), fullName(_gitFileName), isDir(_isDir), isExisting(_isExisting)
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
            return fileBool::notFound;
        return isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isFile()
    {
        if (!isExisting)
            return fileBool::notFound;
        return !isDir ? fileBool::foundYes : fileBool::foundNo;
    }
    virtual fileBool isReadOnly()
    {
        if (!isExisting)
            return fileBool::notFound;
        return fileBool::foundYes;
    }
    virtual IFileAsyncIO * openAsync(IFOmode mode)
    {
        UNIMPLEMENTED;
    }
    virtual const char * queryFilename()
    {
        return fullName.str();
    }
    virtual offset_t size()
    {
        if (!isExisting)
            return (offset_t) -1;
        if (fileSize != (offset_t) -1)
            return fileSize;
        if (isDir)
            fileSize = 0;
        else
        {
            git_blob *blob = nullptr;
            int error = git_blob_lookup(&blob, git_tree_owner(commitTree->queryTree()), &oid);
            if (error)
                throw MakeStringException(0, "git git_blob_lookup returned exit status %d", error);

            fileSize = git_blob_rawsize(blob);
            git_blob_free(blob);
        }
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
    virtual bool getInfo(bool &isdir,offset_t &_size,CDateTime &modtime)
    {
        isdir = isDir;
        _size = size();
        modtime.clear();
        return true;
    }
    virtual IFileIO * open(IFOmode mode, IFEflags extraFlags) override
    {
        assertex(mode==IFOread && isExisting && !isDir);
        return new GitRepositoryFileIO(commitTree, &oid);
    }
    virtual IFileIO * openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags) override
    {
        assertex(mode==IFOread && isExisting && !isDir);
        return new GitRepositoryFileIO(commitTree, &oid);
    }


    // Not going to be implemented - this IFile interface is too big..
    virtual bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime) { UNIMPLEMENTED; }
    virtual bool remove() { UNIMPLEMENTED; }
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
    Linked<GitCommitTree> commitTree;
    const git_oid oid;
    StringAttr gitDirectory;
    StringAttr revision;
    StringAttr relFileName;
    StringBuffer fullName;
    offset_t fileSize = (offset_t) -1;
    bool isDir;
    bool isExisting;
};

class GitRepositoryDirectoryIterator : implements IDirectoryIterator, public CInterface
{
    static int treeCallback(const char *root, const git_tree_entry *entry, void *payload)
    {
        GitRepositoryDirectoryIterator * self = reinterpret_cast<GitRepositoryDirectoryIterator *>(payload);
        return self->noteEntry(root, entry);
    }
public:
    IMPLEMENT_IINTERFACE;
    GitRepositoryDirectoryIterator(const char *_gitFileName, const char *_mask, bool _sub, bool _includeDirs)
    : mask(_mask), sub(_sub), includeDirs(_includeDirs)
    {
        splitGitFileName(_gitFileName, gitDirectory, revision, relDir);
        curIndex = 0;

        const char * version = revision.length() ? revision.get() : "HEAD";
        commitTree.setown(new GitCommitTree(gitDirectory, version));
        if (!commitTree->queryTree())
            throw makeStringExceptionV(9900 , "Cannot resolve git revision %s", _gitFileName);
    }
    virtual StringBuffer &getName(StringBuffer &buf)
    {
        assertex(curFile);
        return buf.append(curFile->queryFilename());
    }
    virtual bool isDir()
    {
        assertex(curFile);
        return curFile->isDirectory()==fileBool::foundYes;
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
        matchedPath = 0;

        git_tree_walk(commitTree->queryTree(), GIT_TREEWALK_PRE, treeCallback, this);
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
    int noteEntry(const char *root, const git_tree_entry *entry)
    {
        const char * filename = git_tree_entry_name(entry);

        // BLOB is a file revision object, TREE is a nested directory, COMMIT seems to be used for an empty directory.
        git_object_t kind = git_tree_entry_type(entry);
        bool isDirectory = kind != GIT_OBJECT_BLOB;
        if (matchedPath >= relDir.length())
        {
            if (relDir)
            {
                //Check for the root directory changing - if it does we have finished all the matches => abort recursion
                size_t lenRoot = strlen(root);
                if (lenRoot != relDir.length())
                    return -1;
                if (!strieq(root, relDir.str()))
                    return -1;
            }

            //Currently avoid de-referencing the file sizes - may need to revisit if it is required
            sizes.append(isDirectory ? (offset_t) -1 : 0);
            files.append(filename);
            oids.emplace_back(*git_tree_entry_id(entry));
            return 1;  // do not recurse - only expand a single level of the directory tree
        }

        unsigned lenFilename = strlen(filename);
        unsigned remaining = relDir.length() - matchedPath;
        if (lenFilename <= remaining)
        {
            const char * next = relDir.str() + matchedPath;
            if (strnicmp(next, filename, lenFilename) == 0)
            {
                if (lenFilename == remaining)
                {
                    sizes.append(isDirectory ? (offset_t) -1 : 0);
                    files.append(filename);
                    oids.emplace_back(*git_tree_entry_id(entry));
                    return -1;  // found the single match
                }

                unsigned nextChar = next[lenFilename];
                if (isPathSepChar(nextChar))
                {
                    matchedPath += (lenFilename + 1);
                    return 0; // recurse
                }

                // filename only matches a substring of the next directory that needs to match
            }
        }
        return 1;   // skip
    }

protected:
    StringAttr gitDirectory;
    StringAttr revision;
    StringAttr relDir;
    StringAttr mask;
    Owned<IFile> curFile;
    unsigned curIndex = 0;
    StringArray files;
    UInt64Array sizes;
    std::vector<git_oid> oids;
    Owned<GitCommitTree> commitTree;
    bool includeDirs = true;
    bool sub = false;
    unsigned matchedPath = 0;

    void open()
    {
        if (files.isItem(curIndex))
        {
            const char *filename = files.item(curIndex);
            offset_t size = sizes.item(curIndex);
            const git_oid & oid = oids[curIndex];
            StringBuffer gitFileName;
            buildGitFileName(gitFileName, gitDirectory, revision, relDir);
            // Git ls-tree behaves differently according to whether you put the trailing / on the path you supply.
            // With /, it gets all files in that directory
            // Without, it will return just a single match (for the file or dir with that name)
            // So we are effectively in two different modes according to which we used.
            char lastChar = gitFileName.charAt(gitFileName.length()-1);
            // NOTE: / or PATHSEPCHAR - we translated to git representation, but root directory is .git{x}<pathsep>
            if ((lastChar == '/') || (lastChar == PATHSEPCHAR))
                gitFileName.append(filename);
            if (size==(offset_t) -1)
                curFile.setown(new GitRepositoryFile(gitFileName, true, true, commitTree, oid));
            else
                curFile.setown(new GitRepositoryFile(gitFileName, false, true, commitTree, oid));
        }
        else
            curFile.clear();
    }
};

//--------------------------------------------------------------------------------------------------------------------

IDirectoryIterator *createGitRepositoryDirectoryIterator(const char *gitFileName, const char *mask, bool sub, bool includeDirs)
{
    assertex(sub==false);  // I don't know what it means!
    return new GitRepositoryDirectoryIterator(gitFileName, mask, sub, includeDirs);
}

static IFile *createGitFile(const char *gitFileName)
{
    StringBuffer fname(gitFileName);
    assertex(fname.length());
    removeTrailingPathSepChar(fname);
    StringAttr gitDirectory, revision, relDir;
    splitGitFileName(fname, gitDirectory, revision, relDir);
    if (relDir.isEmpty())
    {
        // Special case the root - ugly but apparently necessary
        return new GitRepositoryFile(fname, true, true, nullptr, nullOid);
    }
    Owned<IDirectoryIterator> dir = createGitRepositoryDirectoryIterator(fname, NULL, false, true);
    if (dir->first())
    {
        Linked<IFile> file = &dir->query();
        assertex(!dir->next());
        return file.getClear();
    }
    else
        return new GitRepositoryFile(gitFileName, false, false, nullptr, nullOid);
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
    git_libgit2_init();
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
    git_libgit2_shutdown();
}
