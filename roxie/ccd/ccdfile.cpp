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

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jmd5.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "jqueue.tpp"
#include "dautils.hpp"

#include "keydiff.hpp"

#include "ccd.hpp"
#include "ccdfile.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdsnmp.hpp"
#include "rmtfile.hpp"
#include "ccdqueue.ipp"
#ifdef __linux__
#include <sys/mman.h>
#endif

atomic_t numFilesOpen[2];

#define MAX_READ_RETRIES 2

#ifdef _DEBUG
//#define FAIL_20_READ
//#define FAIL_20_OPEN
#endif

// We point unopened files at a FailingIO object, which avoids having to test for NULL on every access

class NotYetOpenException : public CInterface, implements IException
{
public: 
    IMPLEMENT_IINTERFACE;
    virtual int             errorCode() const { return 0; }
    virtual StringBuffer &  errorMessage(StringBuffer &msg) const { return msg.append("not yet open"); }
    virtual MessageAudience errorAudience() const { return MSGAUD_internal; }
};

class CFailingFileIO : public CInterface, implements IFileIO
{
#define THROWNOTOPEN throw new NotYetOpenException()

public:
    IMPLEMENT_IINTERFACE;
    virtual size32_t read(offset_t pos, size32_t len, void * data) { THROWNOTOPEN; }
    virtual offset_t size() { THROWNOTOPEN; }
    virtual void flush() { THROWNOTOPEN; }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) { THROWNOTOPEN; }
    virtual void setSize(offset_t size) { UNIMPLEMENTED; }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { UNIMPLEMENTED; return 0; }
    virtual void close() { }
    virtual unsigned __int64 getStatistic(StatisticKind kind) { return 0; }
} failure;

class CLazyFileIO : public CInterface, implements ILazyFileIO, implements IDelayedFile
{
protected:
    IArrayOf<IFile> sources;
    Owned<IFile> logical;
    unsigned currentIdx;
    Owned<IFileIO> current;
    Owned<IMemoryMappedFile> mmapped;
    mutable CriticalSection crit;
    bool remote;
    offset_t fileSize;
    CDateTime fileDate;
    unsigned crc;
    unsigned lastAccess;
    bool copying;
    bool isCompressed;
    const IRoxieFileCache *cached;
    CRuntimeStatisticCollection fileStats;

#ifdef FAIL_20_READ
    unsigned readCount;
#endif

public:
    IMPLEMENT_IINTERFACE;

    CLazyFileIO(IFile *_logical, offset_t size, const CDateTime &_date, unsigned _crc, bool _isCompressed)
        : logical(_logical), fileSize(size), crc(_crc), isCompressed(_isCompressed), fileStats(diskLocalStatistics)
    {
        fileDate.set(_date);
        currentIdx = 0;
        current.set(&failure);
        remote = false;
#ifdef FAIL_20_READ
        readCount = 0;
#endif
        lastAccess = msTick();
        copying = false;
        cached = NULL;
    }
    
    ~CLazyFileIO()
    {
        setFailure(); // ensures the open file count properly maintained
    }

    virtual void beforeDispose()
    {
        if (cached)
            cached->removeCache(this);
    }

    void setCache(const IRoxieFileCache *cache)
    {
        assertex(!cached);
        cached = cache;
    }

    void removeCache(const IRoxieFileCache *cache)
    {
        assertex(cached==cache);
        cached = NULL;
    }

    inline void setRemote(bool _remote) { remote = _remote; }

    virtual void setCopying(bool _copying)
    {
        CriticalBlock b(crit);
        if (remote && currentIdx)
        {
            // The current location is not our preferred location. Recheck whether we can now access our preferred location
            setFailure();
            currentIdx = 0;
            _checkOpen();
        }
        copying = _copying; 
    }

    virtual bool isCopying() const
    {
        CriticalBlock b(crit);
        return copying; 
    }

    virtual bool isOpen() const 
    {
        CriticalBlock b(crit);
        return current.get() != &failure; 
    }

    virtual unsigned getLastAccessed() const
    {
        CriticalBlock b(crit);
        return lastAccess;
    }

    virtual void close()
    {
        CriticalBlock b(crit);
        setFailure();
    }

    virtual bool isRemote()
    {
        CriticalBlock b(crit);
        return remote;
    }

    void setFailure()
    {
        try
        {
            if (current.get()!=&failure)
            {
                atomic_dec(&numFilesOpen[remote]);
                mergeStats(fileStats, current);
            }
            current.set(&failure); 
        }
        catch (IException *E) 
        {
            if (traceLevel > 5)
            {
                StringBuffer s;
                DBGLOG("setFailure ignoring exception %s from IFileIO close", E->errorMessage(s).str());
            }
            E->Release(); 
        }
    }

    void checkOpen()
    {
        CriticalBlock b(crit);
        _checkOpen();
    }

    void _checkOpen()
    {
        if (current.get() == &failure)
        {
            StringBuffer filesTried;
            unsigned tries = 0;
            bool firstTime = true; // first time try the "fast / cache" way - if that fails - try original away - if that still fails, error
            RoxieFileStatus fileStatus = FileNotFound;

            loop
            {
                if (currentIdx >= sources.length())
                    currentIdx = 0;
                if (tries==sources.length())
                {
                    if (firstTime)  // if first time - reset and try again - non cache way
                    {
                        firstTime = false;
                        tries = 0;
                    }
                    else
                        throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Failed to open file %s at any of the following remote locations %s", logical->queryFilename(), filesTried.str()); // operations doesn't want a trap
                }
                const char *sourceName = sources.item(currentIdx).queryFilename();
                if (traceLevel > 10)
                    DBGLOG("Trying to open %s", sourceName);
                try
                {
#ifdef FAIL_20_OPEN
                    openCount++;
                    if ((openCount % 5) == 0)
                        throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Pretending to fail on an open");
#endif
                    IFile *f = &sources.item(currentIdx);
                    if (firstTime)
                        cacheFileConnect(f, dafilesrvLookupTimeout);  // set timeout to 10 seconds
                    else
                    {
                        if (traceLevel > 10)
                            DBGLOG("Looking for file using non-cached file open");
                    }

                    fileStatus = queryFileCache().fileUpToDate(f, fileSize, fileDate, crc, isCompressed);
                    if (fileStatus == FileIsValid)
                    {
                        if (isCompressed)
                            current.setown(createCompressedFileReader(f));
                        else
                            current.setown(f->open(IFOread));
                        if (current)
                        {
                            if (traceLevel > 5)
                                DBGLOG("Opening %s", sourceName);
                            disconnectRemoteIoOnExit(current);
                            break;
                        }
    //                  throwUnexpected();  - try another location if this one has the wrong version of the file
                    }
                    disconnectRemoteFile(f);
                }
                catch (IException *E)
                {
                    E->Release();
                }
                currentIdx++;
                tries++;
                if (!firstTime)  // log error on last attempt for each file name - it will have the "best" error condition
                {
                    filesTried.appendf(" %s", sourceName);  // only need to build this list once

                    switch (fileStatus)
                    {
                        case FileNotFound:
                            filesTried.append(": FileNotFound");
                            break;

                        case FileSizeMismatch:
                            filesTried.append(": FileSizeMismatch");
                            break;

                        case FileCRCMismatch:
                            filesTried.append(": FileCRCMismatch");
                            break;

                        case FileDateMismatch:
                            filesTried.append(": FileDateMismatch");
                            break;
                    }
                }
            }
            lastAccess = msTick();
            atomic_inc(&numFilesOpen[remote]);
            if ((unsigned) atomic_read(&numFilesOpen[remote]) > maxFilesOpen[remote])
                queryFileCache().closeExpired(remote); // NOTE - this does not actually do the closing of expired files (which could deadlock, or could close the just opened file if we unlocked crit)
        }
    }

    virtual void addSource(IFile *newSource)
    {
        if (newSource)
        {
            if (traceLevel > 10)
                DBGLOG("Adding information for location %s for %s", newSource->queryFilename(), logical->queryFilename());
            CriticalBlock b(crit);
            sources.append(*newSource);
        }
    }

    virtual size32_t read(offset_t pos, size32_t len, void * data) 
    {
        CriticalBlock b(crit);
        unsigned tries = 0;
        loop
        {
            try
            {
                size32_t ret = current->read(pos, len, data);
                lastAccess = msTick();
                return ret;

            }
            catch (NotYetOpenException *E)
            {
                E->Release();
            }
            catch (IException *E)
            {
                EXCLOG(MCoperatorError, E, "Read error");
                E->Release();
                DBGLOG("Failed to read length %d offset %" I64F "x file %s", len, pos, sources.item(currentIdx).queryFilename());
                currentIdx++;
                setFailure();
            }
            _checkOpen();
            tries++;
            if (tries == MAX_READ_RETRIES)
                throw MakeStringException(ROXIE_FILE_ERROR, "Failed to read length %d offset %" I64F "x file %s after %d attempts", len, pos, sources.item(currentIdx).queryFilename(), tries);
        }
    }

    virtual void flush()
    {
        CriticalBlock b(crit);
        if (current.get() != &failure)
            current->flush();
    }

    virtual offset_t size() 
    { 
        CriticalBlock b(crit);
        _checkOpen();
        lastAccess = msTick();
        return current->size();
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        CriticalBlock b(crit);
        unsigned __int64 openValue = current->getStatistic(kind);
        return openValue + fileStats.getStatisticValue(kind);
    }

    virtual size32_t write(offset_t pos, size32_t len, const void * data) { throwUnexpected(); }
    virtual void setSize(offset_t size) { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { throwUnexpected(); return 0; }

    virtual const char *queryFilename() { return logical->queryFilename(); }
    virtual bool isAlive() const { return CInterface::isAlive(); }
    virtual int getLinkCount() const { return CInterface::getLinkCount(); }

    virtual IMemoryMappedFile *queryMappedFile()
    {
        CriticalBlock b(crit);
        if (mmapped)
            return mmapped;
        if (!remote)
        {
            mmapped.setown(logical->openMemoryMapped());
            return mmapped;
        }
        return NULL;
    }

    virtual IFileIO *queryFileIO()
    {
        return this;
    }


    virtual bool createHardFileLink()
    {
        unsigned tries = 0;
        loop
        {
            StringBuffer filesTried;
            if (currentIdx >= sources.length())
                currentIdx = 0;
            if (tries==sources.length())
                return false;
            const char *sourceName = sources.item(currentIdx).queryFilename();
            filesTried.appendf(" %s", sourceName);
            try
            {
                if (queryFileCache().fileUpToDate(&sources.item(currentIdx), fileSize, fileDate, crc, isCompressed) == FileIsValid)
                {
                    StringBuffer source_drive;
                    splitFilename(sourceName, &source_drive, NULL, NULL, NULL);

                    StringBuffer query_drive;
                    splitFilename(logical->queryFilename(), &query_drive, NULL, NULL, NULL);

                    // only try to create link if on the same drive
                    if ( (stricmp(query_drive.str(), source_drive.str()) == 0))
                    {
                        try
                        {
                            DBGLOG("Trying to create Hard Link for %s", sourceName);
                            createHardLink(logical->queryFilename(), sourceName);
                            current.setown(sources.item(currentIdx).open(IFOread));
                            return true;
                        }
                        catch(IException *E)
                        {
                            StringBuffer err;
                            DBGLOG("HARD LINK ERROR %s", E->errorMessage(err).str());
                            E->Release();
                        }
                    }
                }
            }
            catch (IException *E)
            {
                E->Release();
            }
            currentIdx++;
            tries++;
        }

        DBGLOG("Could not create any hard links for %s", logical->queryFilename());
        return false;  // if we get here - no hardlink
    }

    void copyComplete()
    {
        {
            CriticalBlock b(crit);
            setFailure(); // lazyOpen will then reopen it...
            currentIdx = 0;
            remote = false;
            copying = false;
            sources.kill();
            sources.add(*logical.getLink(), 0);
            if (!lazyOpen)
                _checkOpen();
        }
    }

    virtual IFile *querySource() 
    {
        CriticalBlock b(crit);
        _checkOpen();
        return &sources.item(currentIdx); 
    };
    virtual IFile *queryTarget() { return logical; }

    virtual offset_t getSize() { return fileSize; }
    virtual CDateTime *queryDateTime() { return &fileDate; }

    static int compareAccess(IInterface * const *L, IInterface * const *R)
    {
        ILazyFileIO *LL = (ILazyFileIO *) *L;
        ILazyFileIO *RR = (ILazyFileIO *) *R;
        return LL->getLastAccessed() - RR->getLastAccessed();
    }
};

//----------------------------------------------------------------------------------------------

static IPartDescriptor *queryMatchingRemotePart(IPartDescriptor *pdesc, IFileDescriptor *remoteFDesc, unsigned int partNum)
{
    if (!remoteFDesc)
        return NULL;
    IPartDescriptor *remotePDesc = remoteFDesc->queryPart(partNum);
    if (!remotePDesc)
        return NULL;
    unsigned int crc, remoteCrc;
    if (!pdesc || !pdesc->getCrc(crc)) //local crc not available, never DFS copied?
        return remotePDesc;
    if (remotePDesc->getCrc(remoteCrc) && remoteCrc==crc)
        return remotePDesc;
    return NULL;
}

static int getClusterPriority(const char *clusterName)
{
    assertex(preferredClusters);
    int *priority = preferredClusters->getValue(clusterName);
    return priority ? *priority : 100;
}

static void appendRemoteLocations(IPartDescriptor *pdesc, StringArray &locations, const char *localFileName, const char *fromCluster, bool includeFromCluster)
{
    IFileDescriptor &fdesc = pdesc->queryOwner();
    unsigned numCopies = pdesc->numCopies();
    unsigned lastClusterNo = (unsigned) -1;
    unsigned numThisCluster = 0;
    unsigned initialSize = locations.length();
    int priority = 0;
    IntArray priorities;
    for (unsigned copy = 0; copy < numCopies; copy++)
    {
        unsigned clusterNo = pdesc->copyClusterNum(copy);
        StringBuffer clusterName;
        fdesc.getClusterGroupName(clusterNo, clusterName);
        if (fromCluster && *fromCluster)
        {
            bool matches = strieq(clusterName.str(), fromCluster);
            if (matches!=includeFromCluster)
                continue;
        }
        RemoteFilename r;
        pdesc->getFilename(copy,r);
        StringBuffer path;
        r.getRemotePath(path);
        if (localFileName && r.isLocal())
        {
            StringBuffer l;
            r.getLocalPath(l);
            if (streq(l, localFileName))
                continue; // don't add ourself
        }
        if (clusterNo == lastClusterNo)
        {
            numThisCluster++;
            if (numThisCluster > 2)  // Don't add more than 2 from one cluster
                continue;
        }
        else
        {
            numThisCluster = 1;
            lastClusterNo = clusterNo;
            if (preferredClusters)
            {
                priority = getClusterPriority(clusterName);
            }
            else
                priority = copy;
        }
        if (priority >= 0)
        {
            ForEachItemIn(idx, priorities)
            {
                if (priorities.item(idx) < priority)
                    break;
            }
            priorities.add(priority, idx);
            locations.add(path.str(), idx+initialSize);
        }
    }
}

//----------------------------------------------------------------------------------------------

typedef StringArray *StringArrayPtr;

class CRoxieFileCache : public CInterface, implements ICopyFileProgress, implements IRoxieFileCache
{
    friend class CcdFileTest;
    mutable ICopyArrayOf<ILazyFileIO> todo; // Might prefer a queue but probably doesn't really matter.
    InterruptableSemaphore toCopy;
    InterruptableSemaphore toClose;
    mutable CopyMapStringToMyClass<ILazyFileIO> files;
    mutable CriticalSection crit;
    CriticalSection cpcrit;
    bool started;
    bool aborting;
    bool closing;
    bool testMode;
    bool closePending[2];
    StringAttrMapping fileErrorList;
    Semaphore bctStarted;
    Semaphore hctStarted;

    RoxieFileStatus fileUpToDate(IFile *f, offset_t size, const CDateTime &modified, unsigned crc, bool isCompressed)
    {
        // Ensure that SockFile does not keep these sockets open (or we will run out)
        class AutoDisconnector
        {
        public:
            AutoDisconnector(IFile *_f) : f(_f) {}
            ~AutoDisconnector() { disconnectRemoteFile(f); }
        private:
            IFile *f;
        } autoDisconnector(f);

        cacheFileConnect(f, dafilesrvLookupTimeout);  // set timeout to 10 seconds
        if (f->exists())
        {
            // only check size if specified
            if ( (size != -1) && !isCompressed && f->size()!=size) // MORE - should be able to do better on compressed you'da thunk
                return FileSizeMismatch;

            if (crc > 0)
            {
                // if a crc is specified let's check it
                unsigned file_crc = f->getCRC();
                if (file_crc && crc != file_crc)  // for remote files crc_file can fail, even if the file is valid
                {
                    DBGLOG("FAILED CRC Check");
                    return FileCRCMismatch;
                }
            }
            CDateTime mt;
            return (modified.isNull() || (f->getTime(NULL, &mt, NULL) &&  mt.equals(modified, false))) ? FileIsValid : FileDateMismatch;
        }
        else
            return FileNotFound;
    }

    ILazyFileIO *openFile(const char *lfn, unsigned partNo, const char *localLocation,
                           IPartDescriptor *pdesc,
                           const StringArray &remoteLocationInfo,
                           offset_t size, const CDateTime &modified, unsigned crc)
    {
        Owned<IFile> local = createIFile(localLocation);
        bool isCompressed = testMode ? false : pdesc->queryOwner().isCompressed();
        Owned<CLazyFileIO> ret = new CLazyFileIO(local.getLink(), size, modified, crc, isCompressed);
        RoxieFileStatus fileStatus = fileUpToDate(local, size, modified, crc, isCompressed);
        if (fileStatus == FileIsValid)
        {
            ret->addSource(local.getLink());
            ret->setRemote(false);
        }
        else if (local->exists() && !ignoreOrphans)  // Implies local dali and local file out of sync
            throw MakeStringException(ROXIE_FILE_ERROR, "Local file %s does not match DFS information", localLocation);
        else
        {
            bool addedOne = false;

            // put the peerRoxieLocations next in the list
            StringArray localLocations;
            if (testMode)
                localLocations.append("test.buddy");
            else
                appendRemoteLocations(pdesc, localLocations, localLocation, roxieName, true);  // Adds all locations on the same cluster
            ForEachItemIn(roxie_idx, localLocations)
            {
                try
                {
                    const char *remoteName = localLocations.item(roxie_idx);
                    Owned<IFile> remote = createIFile(remoteName);
                    RoxieFileStatus status = fileUpToDate(remote, size, modified, crc, isCompressed);
                    if (status==FileIsValid)
                    {
                        if (miscDebugTraceLevel > 5)
                            DBGLOG("adding peer location %s", remoteName);
                        ret->addSource(remote.getClear());
                        addedOne = true;
                    }
                    else if (status==FileNotFound)
                    {
                        // Even though it's not on the buddy (yet), add it to the locations since it may well be there
                        // by the time we come to copy (and if it is, we want to copy from there)
                        if (miscDebugTraceLevel > 5)
                            DBGLOG("adding missing peer location %s", remoteName);
                        ret->addSource(remote.getClear());
                        // Don't set addedOne - we need to go to remote too
                    }
                    else if (miscDebugTraceLevel > 10)
                        DBGLOG("Checked peer roxie location %s, status=%d", remoteName, (int) status);
                }
                catch (IException *E)
                {
                    EXCLOG(MCoperatorError, E, "While creating remote file reference");
                    E->Release();
                }
            }

            if (!addedOne && (copyResources || useRemoteResources || testMode))  // If no peer locations available, go to remote
            {
                ForEachItemIn(idx, remoteLocationInfo)
                {
                    try
                    {
                        const char *remoteName = remoteLocationInfo.item(idx);
                        Owned<IFile> remote = createIFile(remoteName);
                        if (traceLevel > 5)
                            DBGLOG("checking remote location %s", remoteName);
                        RoxieFileStatus status = fileUpToDate(remote, size, modified, crc, isCompressed);
                        if (status==FileIsValid)
                        {
                            if (miscDebugTraceLevel > 5)
                                DBGLOG("adding remote location %s", remoteName);
                            ret->addSource(remote.getClear());
                            addedOne = true;
                        }
                        else if (miscDebugTraceLevel > 10)
                            DBGLOG("Checked remote file location %s, status=%d", remoteName, (int) status);
                    }
                    catch (IException *E)
                    {
                        EXCLOG(MCoperatorError, E, "While creating remote file reference");
                        E->Release();
                    }
                }
            }

            if (!addedOne)
            {
                if (local->exists())  // Implies local dali and local file out of sync
                    throw MakeStringException(ROXIE_FILE_ERROR, "Local file %s does not match DFS information", localLocation);
                else
                {
                    if (traceLevel >= 2)
                    {
                        DBGLOG("Failed to open file at any of the following %d local locations:", localLocations.length());
                        ForEachItemIn(local_idx, localLocations)
                        {
                            DBGLOG("%d: %s", local_idx+1, localLocations.item(local_idx));
                        }
                        DBGLOG("Or at any of the following %d remote locations:", remoteLocationInfo.length());
                        ForEachItemIn(remote_idx, remoteLocationInfo)
                        {
                            DBGLOG("%d: %s", remote_idx+1, remoteLocationInfo.item(remote_idx));
                        }
                    }
                    throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Could not open file %s", localLocation);
                }
            }
            ret->setRemote(true);
        }
        ret->setCache(this);
        files.setValue(localLocation, (ILazyFileIO *)ret);
        return ret.getClear();
    }

    void deleteTempFiles(const char *targetFilename)
    {
        try
        {
            StringBuffer destPath;
            StringBuffer prevTempFile;
            splitFilename(targetFilename, &destPath, &destPath, &prevTempFile, &prevTempFile);

            if (useTreeCopy)
                prevTempFile.append("*.tmp");
            else
                prevTempFile.append("*.$$$");

            Owned<IFile> dirf = createIFile(destPath.str());
            Owned<IDirectoryIterator> iter = dirf->directoryFiles(prevTempFile.str(),false,false);
            ForEach(*iter)
            {
                OwnedIFile thisFile = createIFile(iter->query().queryFilename());
                if (thisFile->isFile() == foundYes)
                    thisFile->remove();
            }
        }
        catch(IException *E)
        {
            StringBuffer err;
            DBGLOG("Could not remove tmp file %s", E->errorMessage(err).str());
            E->Release();
        }
        catch(...)
        {
        }
    }

    bool doCopyFile(ILazyFileIO *f, const char *tempFile, const char *targetFilename, const char *destPath, const char *msg, CFflags copyFlags=CFnone)
    {
        bool fileCopied = false;
        IFile *sourceFile;
        try
        {
            f->setCopying(true);
            sourceFile = f->querySource();
        }
        catch (IException *E)
        {
            f->setCopying(false);
            EXCLOG(MCoperatorError, E, "While trying to start copying file");
            throw;
        }

        unsigned __int64 freeDiskSpace = getFreeSpace(destPath);
        deleteTempFiles(targetFilename);
        offset_t fileSize = sourceFile->size();
        if ( (fileSize + minFreeDiskSpace) > freeDiskSpace)
        {
            StringBuffer err;
            err.appendf("Insufficient disk space.  File %s needs %" I64F "d bytes, but only %" I64F "d remains, and %" I64F "d is needed as a reserve", targetFilename, sourceFile->size(), freeDiskSpace, minFreeDiskSpace);
            IException *E = MakeStringException(ROXIE_DISKSPACE_ERROR, "%s", err.str());
            EXCLOG(MCoperatorError, E);
            E->Release();
            f->setCopying(false);
        }
        else
        {
            IpSubNet subnet; // preferred set but not required
            IpAddress fromip; // returned
            Owned<IFile> destFile = createIFile(useTreeCopy?targetFilename:tempFile);

            bool hardLinkCreated = false;
            unsigned start = msTick();
            try
            {
                if (useHardLink)
                    hardLinkCreated = f->createHardFileLink();

                if (hardLinkCreated)
                    msg = "Hard Link";
                else
                {
                    DBGLOG("%sing %s to %s", msg, sourceFile->queryFilename(), targetFilename);
                    if (traceLevel > 5)
                    {
                        StringBuffer str;
                        str.appendf("doCopyFile %s", sourceFile->queryFilename());
                        TimeSection timing(str.str());
                        if (useTreeCopy)
                            sourceFile->treeCopyTo(destFile, subnet, fromip, true, copyFlags);
                        else
                            sourceFile->copyTo(destFile,DEFAULT_COPY_BLKSIZE,NULL,false,copyFlags);
                    }
                    else
                    {
                        if (useTreeCopy)
                            sourceFile->treeCopyTo(destFile, subnet, fromip, true, copyFlags);
                        else
                            sourceFile->copyTo(destFile,DEFAULT_COPY_BLKSIZE,NULL,false,copyFlags);
                    }
                }
                f->setCopying(false);
                fileCopied = true;
            }
            catch(IException *E)
            {
                f->setCopying(false);
                if (!useTreeCopy)
                { // done by tree copy
                    EXCLOG(E, "Copy exception - remove templocal");
                    destFile->remove(); 
                }
                deleteTempFiles(targetFilename);
                throw;
            }
            catch(...)
            {
                f->setCopying(false);
                if (!useTreeCopy)
                { // done by tree copy
                    DBGLOG("%s exception - remove templocal", msg);
                    destFile->remove(); 
                }
                deleteTempFiles(targetFilename);
                throw;
            }
            if (!hardLinkCreated && !useTreeCopy)  // for hardlinks / treeCopy - no rename needed
            {
                try
                {
                    destFile->rename(targetFilename);
                }
                catch(IException *)
                {
                    f->setCopying(false);
                    deleteTempFiles(targetFilename);
                    throw;
                }
                unsigned elapsed = msTick() - start;
                double sizeMB = ((double) fileSize) / (1024*1024);
                double MBperSec = elapsed ? (sizeMB / elapsed) * 1000 : 0;
                DBGLOG("%s to %s complete in %d ms (%.1f MB/sec)", msg, targetFilename, elapsed, MBperSec);
            }

            f->copyComplete();
        }
        deleteTempFiles(targetFilename);
        return fileCopied;
    }

    bool doCopy(ILazyFileIO *f, bool background, bool displayFirstFileMessage, CFflags copyFlags=CFnone)
    {
        if (!f->isRemote())
            f->copyComplete();
        else
        {
            if (displayFirstFileMessage)
                DBGLOG("Received files to copy");

            const char *targetFilename = f->queryTarget()->queryFilename();
            StringBuffer tempFile(targetFilename);
            StringBuffer destPath;
            splitFilename(tempFile.str(), &destPath, &destPath, NULL, NULL);
            if (destPath.length())
                recursiveCreateDirectory(destPath.str());
            else
                destPath.append('.');
            if (!checkDirExists(destPath.str())) {
                ERRLOG("Dest directory %s does not exist", destPath.str());
                return false;
            }
            
            tempFile.append(".$$$");
            const char *msg = background ? "Background copy" : "Copy";
            return doCopyFile(f, tempFile.str(), targetFilename, destPath.str(), msg, copyFlags);
        }
        return false;  // if we get here there was no file copied
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieFileCache(bool _testMode = false) : bct(*this), hct(*this), testMode(_testMode)
    {
        aborting = false;
        closing = false;
        closePending[false] = false;
        closePending[true] = false;
        started = false;
    }

    ~CRoxieFileCache()
    {
        // NOTE - I assume that by the time I am being destroyed, system is single threaded.
        // Removing any possible race between destroying of the cache and destroying of the files in it would be complex otherwise
        HashIterator h(files);
        ForEach(h)
        {
            ILazyFileIO *f = files.mapToValue(&h.query());
            f->removeCache(this);
        }
    }

    virtual void start()
    {
        if (!started)
        {
            bct.start();
            hct.start();
            bctStarted.wait();
            hctStarted.wait();
            started = true;
        }
    }

    class BackgroundCopyThread : public Thread
    {
        CRoxieFileCache &owner;
    public:
        BackgroundCopyThread(CRoxieFileCache &_owner) : owner(_owner), Thread("CRoxieFileCacheBackgroundCopyThread") {}

        virtual int run()
        {
            return owner.runBackgroundCopy();
        }
    } bct;

    class HandleCloserThread : public Thread
    {
        CRoxieFileCache &owner;
    public:
        HandleCloserThread(CRoxieFileCache &_owner) : owner(_owner), Thread("CRoxieFileCacheHandleCloserThread") {}
        virtual int run()
        {
            return owner.runHandleCloser();
        }
    } hct;

    int runBackgroundCopy()
    {
        bctStarted.signal();
        if (traceLevel)
            DBGLOG("Background copy thread %p starting", this);
        try
        {
            int fileCopiedCount = 0;
            bool fileCopied = false;

            loop
            {
                fileCopied = false;
                Linked<ILazyFileIO> next;
                toCopy.wait();
                {
                    CriticalBlock b(crit);
                    if (closing)
                        break;
                    if (todo.ordinality())
                    {
                        ILazyFileIO *popped = &todo.popGet();
                        if (popped->isAlive())
                        {
                            next.set(popped);
                        }
                        atomic_dec(&numFilesToProcess);    // must decrement counter for SNMP accuracy
                    }
                }
                if (next)
                {
                    try
                    {
                        fileCopied = doCopy(next, true, (fileCopiedCount==0) ? true : false, CFflush_rdwr);
                        CriticalBlock b(crit);
                        if (fileCopied)
                            fileCopiedCount++;
                    }
                    catch (IException *E)
                    {
                        if (aborting)
                            throw;
                        EXCLOG(MCoperatorError, E, "Roxie background copy: ");
                        E->Release();
                    }
                    catch (...) 
                    {
                        EXCLOG(MCoperatorError, "Unknown exception in Roxie background copy");
                    }
                }

                CriticalBlock b(crit);
                if ( (todo.ordinality()== 0) && (fileCopiedCount)) // finished last copy
                {
                    DBGLOG("No more data files to copy");
                    fileCopiedCount = 0;
                }
            }
        }
        catch (IException *E)
        {
            if (!aborting)
                EXCLOG(MCoperatorError, E, "Roxie background copy: ");
            E->Release();
        }
        catch (...) 
        {
            DBGLOG("Unknown exception in background copy thread");
        }
        if (traceLevel)
            DBGLOG("Background copy thread %p exiting", this);
        return 0;
    }

    int runHandleCloser()
    {
        hctStarted.signal();
        if (traceLevel)
            DBGLOG("HandleCloser thread %p starting", this);
        try
        {
            loop
            {
                toClose.wait(10 * 60 * 1000);  // check expired file handles every 10 minutes 
                if (closing)
                    break;
                doCloseExpired(true);
                doCloseExpired(false);
            }
        }
        catch (IException *E)
        {
            if (!aborting)
                EXCLOG(MCoperatorError, E, "Roxie handle closer: ");
            E->Release();
        }
        catch (...) 
        {
            DBGLOG("Unknown exception in handle closer thread");
        }
        if (traceLevel)
            DBGLOG("Handle closer thread %p exiting", this);
        return 0;
    }

    virtual void join(unsigned timeout=INFINITE)
    {
        aborting = true;
        if (started)
        {
            toCopy.interrupt();
            toClose.interrupt();
            bct.join(timeout);
            hct.join(timeout);
        }
    }

    virtual void wait()
    {
        closing = true;
        if (started)
        {
            toCopy.signal();
            toClose.signal();
            bct.join();
            hct.join();
        }
    }

    virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize)
    {
        return aborting ? CFPcancel : CFPcontinue;
    }

    virtual void removeCache(ILazyFileIO *file) const
    {
        CriticalBlock b(crit);
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table.
        // So only remove from hash table if what we find there matches the item that is being deleted.
        const char *filename = file->queryFilename();
        ILazyFileIO *goer = files.getValue(filename);
        if (goer == file)
            files.remove(filename);
        ForEachItemInRev(idx, todo)
        {
            if (file == &todo.item(idx))
            {
                todo.remove(idx);
                atomic_dec(&numFilesToProcess);    // must decrement counter for SNMP accuracy
            }
        }
    }

    virtual ILazyFileIO *lookupFile(const char *lfn, RoxieFileType fileType,
                                     IPartDescriptor *pdesc, unsigned numParts, unsigned replicationLevel,
                                     const StringArray &deployedLocationInfo, bool startFileCopy)
    {
        IPropertyTree &partProps = pdesc->queryProperties();
        offset_t dfsSize = partProps.getPropInt64("@size", -1);
        bool local = partProps.getPropBool("@local");
        unsigned crc;
        if (!crcResources || !pdesc->getCrc(crc))
            crc = 0;
        CDateTime dfsDate;
        if (checkFileDate)
        {
            const char *dateStr = partProps.queryProp("@modified");
            dfsDate.setString(dateStr);
        }

        unsigned partNo = pdesc->queryPartIndex() + 1;
        StringBuffer localLocation;

        if (local)
        {
            assertex(partNo==1 && numParts==1);
            localLocation.append(lfn);  // any resolution done earlier
        }
        else
        {
            // MORE - not at all sure about this. Foreign files should stay foreign ?
            CDfsLogicalFileName dlfn;
            dlfn.set(lfn);
            if (dlfn.isForeign())
                dlfn.clearForeign();
            makePhysicalPartName(dlfn.get(), partNo, numParts, localLocation, replicationLevel, DFD_OSdefault);
        }
        Owned<ILazyFileIO> ret;
        try
        {
            CriticalBlock b(crit);
            Linked<ILazyFileIO> f = files.getValue(localLocation);
            if (f && f->isAlive())
            {
                if ((dfsSize != (offset_t) -1 && dfsSize != f->getSize()) ||
                    (!dfsDate.isNull() && !dfsDate.equals(*f->queryDateTime(), false)))
                {
                    StringBuffer modifiedDt;
                    if (!dfsDate.isNull())
                        dfsDate.getString(modifiedDt);
                    StringBuffer fileDt;
                    f->queryDateTime()->getString(fileDt);
                    if (fileErrorList.find(lfn) == 0)
                    {
                        switch (fileType)
                        {
                            case ROXIE_KEY:
                                fileErrorList.setValue(lfn, "Key");
                                break;

                            case ROXIE_FILE:
                                fileErrorList.setValue(lfn, "File");
                                break;
                        }
                    }
                    throw MakeStringException(ROXIE_MISMATCH, "Different version of %s already loaded: sizes = %" I64F "d %" I64F "d  Date = %s  %s", lfn, dfsSize, f->getSize(), modifiedDt.str(), fileDt.str());
                }
                else
                    return f.getClear();
            }

            ret.setown(openFile(lfn, partNo, localLocation, pdesc, deployedLocationInfo, dfsSize, dfsDate, crc));

            if (startFileCopy)
            {
                if (ret->isRemote())
                {
                    if (copyResources) // MORE - should always copy peer files
                    {
                        if (numParts==1 || (partNo==numParts && fileType==ROXIE_KEY))
                        {
                            ret->checkOpen();
                            doCopy(ret, false, false, CFflush_rdwr);
                            return ret.getLink();
                        }

                        // Copies are popped from end of the todo list
                        // By putting the replicates on the front we ensure they are done after the primaries
                        // and are therefore likely to result in local rather than remote copies.
                        if (replicationLevel)
                            todo.add(*ret, 0);
                        else
                            todo.append(*ret);
                        atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
                        toCopy.signal();

                    }
                }
            }

            if (!lazyOpen)
                ret->checkOpen();
        }
        catch(IException *e)
        {
            if (e->errorCode() == ROXIE_FILE_OPEN_FAIL)
            {
                if (fileErrorList.find(lfn) == 0)
                {
                    switch (fileType)
                    {
                        case ROXIE_KEY:
                            fileErrorList.setValue(lfn, "Key");
                            break;
                        
                        case ROXIE_FILE:
                            fileErrorList.setValue(lfn, "File");
                            break;
                    }
                }
            }
            throw;
        }
        return ret.getLink();
    }

    virtual void closeExpired(bool remote)
    {
        // This schedules a close at the next available opportunity
        CriticalBlock b(cpcrit); // paranoid...
        if (!closePending[remote])
        {
            closePending[remote] = true;
            DBGLOG("closeExpired %s scheduled - %d files open", remote ? "remote" : "local", (int) atomic_read(&numFilesOpen[remote]));
            toClose.signal();
        }
    }

    void doCloseExpired(bool remote)
    {
        {
            CriticalBlock b(cpcrit); // paranoid...
            closePending[remote] = false;
        }
        CriticalBlock b(crit);
        ICopyArrayOf<ILazyFileIO> goers;
        HashIterator h(files);
        ForEach(h)
        {
            ILazyFileIO *f = files.mapToValue(&h.query());
            if (f->isAlive() && f->isOpen() && f->isRemote()==remote && !f->isCopying())
            {
                unsigned age = msTick() - f->getLastAccessed();
                if (age > maxFileAge[remote])
                {
                    if (traceLevel > 5)
                    {
                        // NOTE - querySource will cause the file to be opened if not already open
                        // That's OK here, since we know the file is open and remote.
                        // But don't be tempted to move this line outside these if's (eg. to trace the idle case)
                        const char *fname = remote ? f->querySource()->queryFilename() : f->queryFilename();
                        DBGLOG("Closing inactive %s file %s (last accessed %u ms ago)", remote ? "remote" : "local",  fname, age);
                    }
                    f->close();
                }
                else
                    goers.append(*f);
            }
        }
        unsigned numFilesLeft = goers.ordinality(); 
        if (numFilesLeft > maxFilesOpen[remote])
        {
            goers.sort(CLazyFileIO::compareAccess);
            DBGLOG("Closing LRU %s files, %d files are open", remote ? "remote" : "local",  numFilesLeft);
            unsigned idx = minFilesOpen[remote];
            while (idx < numFilesLeft)
            {
                ILazyFileIO &f = goers.item(idx++);
                if (!f.isCopying())
                {
                    if (traceLevel > 5)
                    {
                        unsigned age = msTick() - f.getLastAccessed();
                        DBGLOG("Closing %s (last accessed %u ms ago)", f.queryFilename(), age);
                    }
                    f.close();
                }
            }
        }
    }

    virtual void flushUnusedDirectories(const char *origBaseDir, const char *directory, StringBuffer &xml)
    {
        Owned<IFile> dirf = createIFile(directory);
        if (dirf->exists() && dirf->isDirectory())
        {
            try
            {
                Owned<IDirectoryIterator> iter = dirf->directoryFiles(NULL,false,true);
                ForEach(*iter)
                {
                    const char *thisName = iter->query().queryFilename();
                    flushUnusedDirectories(origBaseDir, thisName, xml);
                }
                
                if (stricmp(origBaseDir, directory) != 0)
                {
                    try
                    {
                        dirf->remove();
                        xml.appendf("<Directory>%s</Directory>\n", directory);
                        DBGLOG("Deleted directory %s", directory);
                    }
                    catch (IException *e)
                    {
                        // don't care if we can't delete the directory
                        e->Release();
                    }
                    catch(...)
                    {
                        // don't care if we can't delete the directory
                    }
                }
            }
            catch (IException *e)
            {
                // don't care if we can't delete the directory
                e->Release();
            }
            catch(...)
            {
                // don't care if we can't delete the directory
            }
        }
    }

    int numFilesToCopy()
    {
        CriticalBlock b(crit);
        return todo.ordinality();
    }

    virtual StringAttrMapping *queryFileErrorList() { return &fileErrorList; }  // returns list of files that could not be open

    static inline bool validFNameChar(char c)
    {
        static const char *invalids = "*\"/:<>?\\|";
        return (c>=32 && c<127 && !strchr(invalids, c));
    }
};

ILazyFileIO *createPhysicalFile(const char *id, IPartDescriptor *pdesc, IPartDescriptor *remotePDesc, RoxieFileType fileType, int numParts, bool startCopy, unsigned channel)
{
    StringArray remoteLocations;
    const char *peerCluster = pdesc->queryOwner().queryProperties().queryProp("@cloneFromPeerCluster");
    if (peerCluster)
    {
        if (*peerCluster!='-') // a remote cluster was specified explicitly
            appendRemoteLocations(pdesc, remoteLocations, NULL, peerCluster, true);  // Add only from specified cluster
    }
    else
        appendRemoteLocations(pdesc, remoteLocations, NULL, roxieName, false);      // Add from any cluster on same dali, other than mine
    if (remotePDesc)
        appendRemoteLocations(remotePDesc, remoteLocations, NULL, NULL, false);    // Then any remote on remote dali

    return queryFileCache().lookupFile(id, fileType, pdesc, numParts, replicationLevel[channel], remoteLocations, startCopy);
}

//====================================================================================================

class CFilePartMap : public CInterface, implements IFilePartMap
{
    class FilePartMapElement
    {
    public:
        offset_t base;
        offset_t top;

        inline int compare(offset_t offset)
        {
            if (offset < base)
                return -1;
            else if (offset >= top)
                return 1;
            else
                return 0;
        }
    } *map;

    static int compareParts(const void *l, const void *r)
    {
        offset_t lp = * (offset_t *) l;
        FilePartMapElement *thisPart = (FilePartMapElement *) r;
        return thisPart->compare(lp);
    }

    unsigned numParts;
    offset_t recordCount;
    offset_t totalSize;
    StringAttr fileName;

public:
    IMPLEMENT_IINTERFACE;
    CFilePartMap(IPropertyTree &resource)
    {
        fileName.set(resource.queryProp("@id"));
        numParts = resource.getPropInt("@numparts");
        recordCount = resource.getPropInt64("@recordCount");
        totalSize = resource.getPropInt64("@size");
        assertex(numParts);
        map = new FilePartMapElement[numParts];
        for (unsigned i = 0; i < numParts; i++)
        {
            StringBuffer partPath;
            partPath.appendf("Part[@num='%d']", i+1);
            IPropertyTree *part = resource.queryPropTree(partPath.str());
            if (!part)
            {
                partPath.clear().appendf("Part_%d", i+1);   // legacy format support
                part = resource.queryPropTree(partPath.str());
            }
            assertex(part);
            offset_t size = part->getPropInt64("@size", (unsigned __int64) -1);
            assertex(size != (unsigned __int64) -1);
            map[i].base = i ? map[i-1].top : 0;
            map[i].top = map[i].base + size;
        }
        if (totalSize == (offset_t)-1)
            totalSize = map[numParts-1].top;
        else if (totalSize != map[numParts-1].top)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%" I64F "d vs %" I64F "d", map[numParts-1].top, totalSize);
    }

    CFilePartMap(const char *_fileName, IFileDescriptor &fdesc)
        : fileName(_fileName)
    {
        numParts = fdesc.numParts();
        IPropertyTree &props = fdesc.queryProperties();
        recordCount = props.getPropInt64("@recordCount", -1);
        totalSize = props.getPropInt64("@size", -1);
        assertex(numParts);
        map = new FilePartMapElement[numParts];
        for (unsigned i = 0; i < numParts; i++)
        {
            IPartDescriptor &part = *fdesc.queryPart(i);
            IPropertyTree &partProps = part.queryProperties();
            offset_t size = partProps.getPropInt64("@size", (unsigned __int64) -1);
            map[i].base = i ? map[i-1].top : 0;
            if (size==(unsigned __int64) -1)
            {
                if (i==numParts-1)
                    map[i].top = (unsigned __int64) -1;
                else
                    throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file sizes not known for file %s", fileName.get());
            }
            else
                map[i].top = map[i].base + size;
        }
        if (totalSize == (offset_t)-1)
            totalSize = map[numParts-1].top;
        else if (totalSize != map[numParts-1].top)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%" I64F "d vs %" I64F "d", map[numParts-1].top, totalSize);
    }

    ~CFilePartMap()
    {
        delete [] map;
    }
    virtual bool IsShared() const { return CInterface::IsShared(); };
    virtual unsigned mapOffset(offset_t pos) const
    {
        FilePartMapElement *part = (FilePartMapElement *) bsearch(&pos, map, numParts, sizeof(map[0]), compareParts);
        if (!part)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file position %" I64F "d in file %s out of range (max offset permitted is %" I64F "d)", pos, fileName.str(), totalSize);
        return (part-map)+1;
    }
    virtual unsigned getNumParts() const
    {
        return numParts;
    }
    virtual offset_t getTotalSize() const
    {
        return totalSize;
    }
    virtual offset_t getRecordCount() const
    {
        return recordCount;
    }
    virtual offset_t getBase(unsigned part) const
    {
        if (part > numParts || part == 0)
        {
            throw MakeStringException(ROXIE_FILE_ERROR, "Internal error - requesting base for non-existant file part %d (valid are 1-%d)", part, numParts);
        }
        return map[part-1].base;
    }
    virtual offset_t getFileSize() const
    {
        return map[numParts-1].top;
    }
};

extern IFilePartMap *createFilePartMap(const char *fileName, IFileDescriptor &fdesc)
{
    return new CFilePartMap(fileName, fdesc);
}

//====================================================================================================

class CFileIOArray : public CInterface, implements IFileIOArray
{
    unsigned __int64 totalSize;
    mutable CriticalSection crit;
    mutable StringAttr id;

    void _getId() const
    {
        md5_state_t md5;
        md5_byte_t digest[16];
        md5_init(&md5);

        ForEachItemIn(idx, files)
        {
            IFileIO *file = files.item(idx);
            if (file)
            {
                md5_append(&md5, (const md5_byte_t *) &file, sizeof(file));
            }
        }
        md5_finish(&md5, digest);
        char digestStr[33];
        for (int i = 0; i < 16; i++)
        {
            sprintf(&digestStr[i*2],"%02x", digest[i]);
        }
        id.set(digestStr, 32);
    }

public:
    IMPLEMENT_IINTERFACE;
    CFileIOArray()
    {
        valid = 0;
        totalSize = (unsigned __int64) -1;
    }

    virtual bool IsShared() const { return CInterface::IsShared(); };
    IPointerArrayOf<IFileIO> files;
    StringArray filenames;
    Int64Array bases;
    unsigned valid;

    virtual IFileIO *getFilePart(unsigned partNo, offset_t &base)
    {
        if (!files.isItem(partNo))
        {
            DBGLOG("getFilePart requested invalid part %d", partNo);
            throw MakeStringException(ROXIE_FILE_ERROR, "getFilePart requested invalid part %d", partNo);
        }
        IFileIO *file = files.item(partNo);
        if (!file)
        {
//          DBGLOG("getFilePart requested nonBonded part %d", partNo);
//          throw MakeStringException(ROXIE_FILE_FAIL, "getFilePart requested nonBonded part %d", partNo);
            base = 0;
            return NULL;
        }
        base = bases.item(partNo);
        return LINK(file);
    }

    virtual const char *queryLogicalFilename(unsigned partNo)
    {
        if (!filenames.isItem(partNo))
        {
            DBGLOG("queryLogicalFilename requested invalid part %d", partNo);
            throw MakeStringException(ROXIE_FILE_ERROR, "queryLogicalFilename requested invalid part %d", partNo);
        }
        return filenames.item(partNo);
    }

    void addFile(IFileIO *f, offset_t base, const char *filename)
    {
        if (f)
            valid++;
        files.append(f);
        bases.append(base);
        filenames.append(filename ? filename : "");  // Hack!
    }

    virtual unsigned length()
    {
        return files.length();
    }

    virtual unsigned numValid()
    {
        return valid;
    }

    virtual bool isValid(unsigned partNo)
    {
        if (!files.isItem(partNo))
            return false;
        IFileIO *file = files.item(partNo);
        if (!file)
            return false;
        return true;
    }

    virtual unsigned __int64 size()
    {
        CriticalBlock b(crit);
        if (totalSize == (unsigned __int64) -1)
        {
            totalSize = 0;
            ForEachItemIn(idx, files)
            {
                IFileIO *file = files.item(idx);
                if (file)
                    totalSize += file->size();
            }
        }
        return totalSize;
    }

    virtual StringBuffer &getId(StringBuffer &ret) const
    {
        CriticalBlock b(crit);
        if (!id)
            _getId();
        return ret.append(id);
    }
};

template <class X> class PerChannelCacheOf
{
    IPointerArrayOf<X> cache;
    IntArray channels;
public:
    void set(X *value, unsigned channel)
    {
        cache.append(value);
        channels.append(channel);
    }

    X *get(unsigned channel) const
    {
        ForEachItemIn(idx, channels)
        {
            if (channels.item(idx)==channel)
                return cache.item(idx);
        }
        return NULL;
    }
};

CRoxieFileCache * fileCache;

class CResolvedFile : public CInterface, implements IResolvedFileCreator, implements ISDSSubscription
{
protected:
    IResolvedFileCache *cached;
    StringAttr lfn;
    StringAttr physicalName;
    Owned<IDistributedFile> dFile; // NULL on copies serialized to slaves. Note that this implies we keep a lock on dali file for the lifetime of this object.
    CDateTime fileTimeStamp;
    offset_t fileSize;
    unsigned fileCheckSum;
    RoxieFileType fileType;
    bool isSuper;

    StringArray subNames;
    IPointerArrayOf<IFileDescriptor> subFiles; // note - on slaves, the file descriptors may have incomplete info. On originating server is always complete
    IPointerArrayOf<IFileDescriptor> remoteSubFiles; // note - on slaves, the file descriptors may have incomplete info. On originating server is always complete
    IPointerArrayOf<IDefRecordMeta> diskMeta;
    IArrayOf<IDistributedFile> subDFiles;  // To make sure subfiles get locked too
    IArrayOf<IResolvedFile> subRFiles;  // To make sure subfiles get locked too

    Owned <IPropertyTree> properties;
    Linked<IRoxieDaliHelper> daliHelper;
    Owned<IDaliPackageWatcher> notifier;

    void addFile(const char *subName, IFileDescriptor *fdesc, IFileDescriptor *remoteFDesc)
    {
        subNames.append(subName);
        subFiles.append(fdesc);
        remoteSubFiles.append(remoteFDesc);
        IPropertyTree const & props = fdesc->queryProperties();
        if(props.hasProp("_record_layout"))
        {
            MemoryBuffer mb;
            props.getPropBin("_record_layout", mb);
            diskMeta.append(deserializeRecordMeta(mb, true));
        }
        else
            diskMeta.append(NULL);

        unsigned numParts = fdesc->numParts();
        offset_t base = 0;
        for (unsigned i = 0; i < numParts; i++)
        {
            IPartDescriptor *pdesc = fdesc->queryPart(i);
            IPropertyTree &partProps = pdesc->queryProperties();
            offset_t dfsSize = partProps.getPropInt64("@size");
            partProps.setPropInt64("@offset", base);
            base += dfsSize;
        }
        fileSize += base;
    }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        if (traceLevel > 2)
            DBGLOG("Superfile %s change detected", lfn.get());

        {
            CriticalBlock b(lock);
            if (cached)
            {
                cached->removeCache(this);
                cached = NULL;
            }
        }
        globalPackageSetManager->requestReload(false, false);
    }

    // We cache all the file maps/arrays etc here. 
    mutable CriticalSection lock;
    mutable Owned<IFilePartMap> fileMap;
    mutable PerChannelCacheOf<IInMemoryIndexManager> indexMap;
    mutable PerChannelCacheOf<IFileIOArray> ioArrayMap;
    mutable PerChannelCacheOf<IKeyArray> keyArrayMap;

public:
    IMPLEMENT_IINTERFACE;
    CResolvedFile(const char *_lfn, const char *_physicalName, IDistributedFile *_dFile, RoxieFileType _fileType, IRoxieDaliHelper* _daliHelper, bool isDynamic, bool cacheIt, bool writeAccess, bool _isSuperFile)
    : daliHelper(_daliHelper), lfn(_lfn), physicalName(_physicalName), dFile(_dFile), fileType(_fileType), isSuper(_isSuperFile)
    {
        cached = NULL;
        fileSize = 0;
        fileCheckSum = 0;
        if (dFile)
        {
            if (traceLevel > 5)
                DBGLOG("Roxie server adding information for file %s", lfn.get());
            bool tsSet = dFile->getModificationTime(fileTimeStamp);
            bool csSet = dFile->getFileCheckSum(fileCheckSum);
            assertex(tsSet); // per Nigel, is always set
            IDistributedSuperFile *superFile = dFile->querySuperFile();
            if (superFile)
            {
                isSuper = true;
                Owned<IDistributedFileIterator> subs = superFile->getSubFileIterator(true);
                ForEach(*subs)
                {
                    IDistributedFile &sub = subs->query();
                    Owned<IFileDescriptor> fDesc = sub.getFileDescriptor();
                    Owned<IFileDescriptor> remoteFDesc;
                    if (daliHelper)
                        remoteFDesc.setown(daliHelper->checkClonedFromRemote(sub.queryLogicalName(), fDesc, cacheIt));
                    subDFiles.append(OLINK(sub));
                    addFile(sub.queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
                }
                // We have to clone the properties since we don't want to keep the superfile locked
                properties.setown(createPTreeFromIPT(&dFile->queryAttributes()));
                if (!isDynamic && !lockSuperFiles)
                {
                    notifier.setown(daliHelper->getSuperFileSubscription(lfn, this));
                    dFile.clear();  // We don't lock superfiles, except dynamic ones
                }
            }
            else // normal file, not superkey
            {
                isSuper = false;
                properties.set(&dFile->queryAttributes());
                Owned<IFileDescriptor> fDesc = dFile->getFileDescriptor();
                Owned<IFileDescriptor> remoteFDesc;
                if (daliHelper)
                    remoteFDesc.setown(daliHelper->checkClonedFromRemote(_lfn, fDesc, cacheIt));
                addFile(dFile->queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
            }
        }
    }
    virtual void beforeDispose()
    {
        if (notifier)
            daliHelper->releaseSubscription(notifier);
        notifier.clear();
        if (cached)
        {
            cached->removeCache(this);
        }
    }
    virtual unsigned numSubFiles() const
    {
        return subNames.length();
    }
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const
    {
        if (subNames.isItem(num))
        {
            name.append(subNames.item(num));
            return true;
        }
        else
        {
            return false;
        }
    }
    virtual unsigned findSubName(const char *subname) const
    {
        ForEachItemIn(idx, subNames)
        {
            if (stricmp(subNames.item(idx), subname))
                return idx;
        }
        return NotFound;
    }
    virtual unsigned getContents(StringArray &contents) const
    {
        ForEachItemIn(idx, subNames)
        {
            contents.append(subNames.item(idx));
        }
        return subNames.length();
    }
    virtual bool isSuperFile() const
    {
        return isSuper;
    }
    virtual bool isKey() const
    {
        return fileType==ROXIE_KEY;
    }
    virtual IFilePartMap *getFileMap() const
    {
        CriticalBlock b(lock);
        if (!fileMap)
        {
            if (subFiles.length())
            {
                if (subFiles.length()!=1)
                    throw MakeStringException(0, "Roxie does not support FETCH or KEYED JOIN to superkey with multiple parts");
                fileMap.setown(createFilePartMap(lfn, *subFiles.item(0)));
            }
        }
        return fileMap.getLink();
    }
    virtual unsigned getNumParts() const
    {
        CriticalBlock b(lock);
        unsigned numParts = 0;
        ForEachItemIn(idx, subFiles)
        {
            unsigned thisNumParts = subFiles.item(idx)->numParts();
            if (thisNumParts > numParts)
                numParts = thisNumParts;
        }
        return numParts;
    }
    virtual void serializeFDesc(MemoryBuffer &mb, IFileDescriptor *fdesc, unsigned channel, bool isLocal) const
    {
        // Find all the partno's that go to this channel
        unsigned numParts = fdesc->numParts();
        if (numParts > 1 && fileType==ROXIE_KEY && isLocal)
            numParts--; // don't want to send TLK
        UnsignedArray partNos;
        for (unsigned i = 1; i <= numParts; i++)
        {
            IPartDescriptor *pdesc = fdesc->queryPart(i-1);
            if (getBondedChannel(i)==channel || !isLocal)
            {
                partNos.append(i-1);
            }
        }
        fdesc->serializeParts(mb, partNos);
    }
    virtual void serializePartial(MemoryBuffer &mb, unsigned channel, bool isLocal) const
    {
        if (traceLevel > 6)
            DBGLOG("Serializing file information for dynamic file %s, channel %d, local %d", lfn.get(), channel, isLocal);
        byte type = (byte) fileType;
        mb.append(type);
        fileTimeStamp.serialize(mb);
        mb.append(fileCheckSum);
        mb.append(fileSize);
        unsigned numSubFiles = subFiles.length();
        mb.append(numSubFiles);
        ForEachItemIn(idx, subFiles)
        {
            mb.append(subNames.item(idx));
            IFileDescriptor *fdesc = subFiles.item(idx);
            serializeFDesc(mb, fdesc, channel, isLocal);
            IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
            if (remoteFDesc)
            {
                mb.append(true);
                serializeFDesc(mb, remoteFDesc, channel, isLocal);
            }
            else
                mb.append(false);
            if (fileType == ROXIE_KEY) // for now we only support translation on index files
            {
                IDefRecordMeta *meta = diskMeta.item(idx);
                if (meta)
                {
                    mb.append(true);
                    serializeRecordMeta(mb, meta, true);
                }
                else
                    mb.append(false);
            }
        }
        if (properties)
        {
            mb.append(true);
            properties->serialize(mb);
        }
        else
            mb.append(false);
    }
    virtual IFileIOArray *getIFileIOArray(bool isOpt, unsigned channel) const
    {
        CriticalBlock b(lock);
        IFileIOArray *ret = ioArrayMap.get(channel);
        if (!ret)
        {
            ret = createIFileIOArray(isOpt, channel);
            ioArrayMap.set(ret, channel);
        }
        return LINK(ret);
    }
    IFileIOArray *createIFileIOArray(bool isOpt, unsigned channel) const
    {
        Owned<CFileIOArray> f = new CFileIOArray();
        f->addFile(NULL, 0, NULL);
        ForEachItemIn(idx, subFiles)
        {
            IFileDescriptor *fdesc = subFiles.item(idx);
            IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
            const char *subname = subNames.item(idx);
            if (fdesc)
            {
                unsigned numParts = fdesc->numParts();
                for (unsigned i = 1; i <= numParts; i++)
                {
                    if (!channel || getBondedChannel(i)==channel)
                    {
                        try
                        {
                            IPartDescriptor *pdesc = fdesc->queryPart(i-1);
                            assertex(pdesc);
                            IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, i-1);
                            Owned<ILazyFileIO> file = createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_FILE, numParts, cached != NULL, channel);
                            IPropertyTree &partProps = pdesc->queryProperties();
                            f->addFile(file.getClear(), partProps.getPropInt64("@offset"), subname);
                        }
                        catch (IException *E)
                        {
                            StringBuffer err;
                            err.append("Could not load file ");
                            fdesc->getTraceName(err);
                            DBGLOG(E, err.str());
                            if (!isOpt)
                                throw;
                            E->Release();
                            f->addFile(NULL, 0, NULL);
                        }
                    }
                    else
                        f->addFile(NULL, 0, NULL);
                }
            }
        }
        return f.getClear();
    }
    virtual IKeyArray *getKeyArray(IDefRecordMeta *activityMeta, TranslatorArray *translators, bool isOpt, unsigned channel, bool allowFieldTranslation) const
    {
        unsigned maxParts = 0;
        ForEachItemIn(subFile, subFiles)
        {
            IFileDescriptor *fdesc = subFiles.item(subFile);
            if (fdesc)
            {
                unsigned numParts = fdesc->numParts();
                if (numParts > 1)
                    numParts--; // Don't include TLK
                if (numParts > maxParts)
                    maxParts = numParts;
            }

            IDefRecordMeta *thisDiskMeta = diskMeta.item(subFile);
            if (translators)
            {
                if (fdesc && thisDiskMeta && activityMeta && !thisDiskMeta->equals(activityMeta))
                    if (allowFieldTranslation)
                        translators->append(createRecordLayoutTranslator(lfn, thisDiskMeta, activityMeta));
                    else
                    {
                        DBGLOG("Key layout mismatch: %s", lfn.get());
                        StringBuffer q, d;
                        getRecordMetaAsString(q, activityMeta);
                        getRecordMetaAsString(d, thisDiskMeta);
                        DBGLOG("Activity: %s", q.str());
                        DBGLOG("Disk: %s", d.str());
                        throw MakeStringException(ROXIE_MISMATCH, "Key layout mismatch detected for index %s", lfn.get());
                    }
                else
                    translators->append(NULL);
            }
        }
        CriticalBlock b(lock);
        IKeyArray *ret = keyArrayMap.get(channel);
        if (!ret)
        {
            ret = createKeyArray(isOpt, channel, maxParts);
            keyArrayMap.set(ret, channel);
        }
        return LINK(ret);
    }
    IKeyArray *createKeyArray(bool isOpt, unsigned channel, unsigned maxParts) const
    {
        Owned<IKeyArray> ret = ::createKeyArray();
        if (channel)
        {
            ret->addKey(NULL);
            for (unsigned partNo = 1; partNo <= maxParts; partNo++)
            {
                if (channel == getBondedChannel(partNo))
                {
                    Owned<IKeyIndexSet> keyset = createKeyIndexSet();
                    ForEachItemIn(idx, subFiles)
                    {
                        IFileDescriptor *fdesc = subFiles.item(idx);
                        IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);

                        Owned <ILazyFileIO> part;
                        unsigned crc = 0;
                        if (fdesc) // NB there may be no parts for this channel 
                        {
                            IPartDescriptor *pdesc = fdesc->queryPart(partNo-1);
                            if (pdesc)
                            {
                                IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, partNo-1);
                                part.setown(createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, fdesc->numParts(), cached != NULL, channel));
                                pdesc->getCrc(crc);
                            }
                        }
                        if (part)
                        {
                            if (lazyOpen)
                            {
                                // We pass the IDelayedFile interface to createKeyIndex, so that it does not open the file immediately
                                keyset->addIndex(createKeyIndex(part->queryFilename(), crc, *QUERYINTERFACE(part.get(), IDelayedFile), false, false));
                            }
                            else
                                keyset->addIndex(createKeyIndex(part->queryFilename(), crc, *part.get(), false, false));
                        }
                        else
                            keyset->addIndex(NULL);
                    }
                    ret->addKey(keyset.getClear());
                }
                else
                    ret->addKey(NULL);
            }
        }
        else
        {
            // Channel 0 means return the TLK
            IArrayOf<IKeyIndexBase> subkeys;
            Owned<IKeyIndexSet> keyset = createKeyIndexSet();
            ForEachItemIn(idx, subFiles)
            {
                IFileDescriptor *fdesc = subFiles.item(idx);
                IFileDescriptor *remoteFDesc = remoteSubFiles.item(idx);
                Owned<IKeyIndexBase> key;
                if (fdesc)
                {
                    unsigned numParts = fdesc->numParts();
                    assertex(numParts > 0);
                    IPartDescriptor *pdesc = fdesc->queryPart(numParts - 1);
                    IPartDescriptor *remotePDesc = queryMatchingRemotePart(pdesc, remoteFDesc, numParts - 1);
                    Owned<ILazyFileIO> keyFile = createPhysicalFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, numParts, cached != NULL, channel);
                    unsigned crc = 0;
                    pdesc->getCrc(crc);
                    StringBuffer pname;
                    pdesc->getPath(pname);
                    if (lazyOpen)
                    {
                        // We pass the IDelayedFile interface to createKeyIndex, so that it does not open the file immediately
                        key.setown(createKeyIndex(pname.str(), crc, *QUERYINTERFACE(keyFile.get(), IDelayedFile), numParts>1, false));
                    }
                    else
                        key.setown(createKeyIndex(pname.str(), crc, *keyFile.get(), numParts>1, false));
                    keyset->addIndex(LINK(key->queryPart(0)));
                }
                else
                    keyset->addIndex(NULL);
            }
            if (keyset->numParts())
                ret->addKey(keyset.getClear());
            else if (!isOpt)
                throw MakeStringException(ROXIE_FILE_ERROR, "Key %s has no key parts", lfn.get());
            else if (traceLevel > 4)
                DBGLOG(ROXIE_OPT_REPORTING, "Key %s has no key parts", lfn.get());
        }
        return ret.getClear();
    }

    virtual IInMemoryIndexManager *getIndexManager(bool isOpt, unsigned channel, IFileIOArray *files, IRecordSize *recs, bool preload, int numKeys) const 
    {
        // MORE - I don't know that it makes sense to pass isOpt in to these calls
        // Failures to resolve will not be cached, only successes.
        // MORE - preload and numkeys are all messed up - can't be specified per query have to be per file

        CriticalBlock b(lock);
        IInMemoryIndexManager *ret = indexMap.get(channel);
        if (!ret)
        {
            ret = createInMemoryIndexManager(isOpt, lfn);
            ret->load(files, recs, preload, numKeys);   // note - files (passed in) are channel specific
            indexMap.set(ret, channel);
        }
        return LINK(ret);

    }

    virtual const CDateTime &queryTimeStamp() const
    {
        return fileTimeStamp;
    }

    virtual unsigned queryCheckSum() const
    {
        return fileCheckSum;
    }

    virtual offset_t getFileSize() const
    {
        return fileSize;
    }

    virtual hash64_t addHash64(hash64_t hashValue) const
    {
        hashValue = fileTimeStamp.getHash(hashValue);
        if (fileCheckSum)
            hashValue = rtlHash64Data(sizeof(fileCheckSum), &fileCheckSum, hashValue);
        return hashValue;
    }

    virtual void addSubFile(const IResolvedFile *_sub)
    {
        const CResolvedFile *sub = static_cast<const CResolvedFile *>(_sub);
        if (subFiles.length())
            assertex(sub->fileType==fileType);
        else
            fileType = sub->fileType;
        subRFiles.append((IResolvedFile &) *LINK(_sub));
        ForEachItemIn(idx, sub->subFiles)
        {
            addFile(sub->subNames.item(idx), LINK(sub->subFiles.item(idx)), LINK(sub->remoteSubFiles.item(idx)));
        }
    }
    virtual void addSubFile(IFileDescriptor *_sub, IFileDescriptor *_remoteSub)
    {
        addFile(lfn, _sub, _remoteSub);
    }
    virtual void addSubFile(const char *localFileName)
    {
        Owned<IFile> file = createIFile(localFileName);
        assertex(file->exists());
        offset_t size = file->size();
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        Owned<IPropertyTree> pp = createPTree("Part");
        pp->setPropInt64("@size",size);
        pp->setPropBool("@local", true);
        fdesc->setPart(0, queryMyNode(), localFileName, pp);
        addSubFile(fdesc.getClear(), NULL);
    }

    virtual void setCache(IResolvedFileCache *cache)
    {
        if (cached)
        {
            if (traceLevel > 9)
                DBGLOG("setCache removing from prior cache %s", queryFileName());
            if (cache==NULL)
                cached->removeCache(this);
            else
                throwUnexpected();
        }
        cached = cache;
    }

    virtual bool isAlive() const
    {
        return CInterface::isAlive();
    }

    virtual const char *queryFileName() const
    {
        return lfn.get();
    }
    virtual const char *queryPhysicalName() const
    {
        return physicalName.get();
    }
    virtual const IPropertyTree *queryProperties() const
    {
        return properties;
    }
    virtual void remove()
    {
        subFiles.kill();
        subDFiles.kill();
        subRFiles.kill();
        subNames.kill();
        remoteSubFiles.kill();
        diskMeta.kill();
        properties.clear();
        notifier.clear();
        if (isSuper)
        {
            // Because we don't lock superfiles, we need to behave differently
            UNIMPLEMENTED;
        }
        else if (dFile)
        {
            dFile->detach();
        }
        else if (!physicalName.isEmpty())
        {
            try
            {
                Owned<IFile> file = createIFile(physicalName.get());
                file->remove();
            }
            catch (IException *e)
            {
                ERRLOG(-1, "Error removing file %s (%s)", lfn.get(), physicalName.get());
                e->Release();
            }
        }
    }
    virtual bool exists() const
    {
        // MORE - this is a little bizarre. We sometimes create a resolvedFile for a file that we are intending to create.
        // This will make more sense if/when we start to lock earlier.
        if (dFile || isSuper)
            return true; // MORE - may need some thought - especially the isSuper case
        else if (!physicalName.isEmpty())
            return checkFileExists(physicalName.get());
        else
            return false;
    }
};


/*----------------------------------------------------------------------------------------------------------
MORE
    - on remote() calls we can't pass the expected file date but we will pass it back with the file info.
------------------------------------------------------------------------------------------------------------*/

class CSlaveDynamicFile : public CResolvedFile
{
public:
    bool isOpt; // MORE - this is not very good. Needs some thought unless you cache opt / nonOpt separately which seems wasteful
    bool isLocal;
    unsigned channel;
    unsigned serverIdx;

public:
    CSlaveDynamicFile(const IRoxieContextLogger &logctx, const char *_lfn, RoxiePacketHeader *header, bool _isOpt, bool _isLocal)
        : CResolvedFile(_lfn, NULL, NULL, ROXIE_FILE, NULL, true, false, false, false), channel(header->channel), serverIdx(header->serverIdx), isOpt(_isOpt), isLocal(_isLocal)
    {
        // call back to the server to get the info
        IPendingCallback *callback = ROQ->notePendingCallback(*header, lfn); // note that we register before the send to avoid a race.
        try
        {
            RoxiePacketHeader newHeader(*header, ROXIE_FILECALLBACK);
            bool ok = false;
            for (unsigned i = 0; i < callbackRetries; i++)
            {
                Owned<IMessagePacker> output = ROQ->createOutputStream(newHeader, true, logctx);
                unsigned len = strlen(lfn)+3; // 1 for isOpt, 1 for isLocal, 1 for null terminator
                char *buf = (char *) output->getBuffer(len, true);
                buf[0] = isOpt;
                buf[1] = isLocal;
                strcpy(buf+2, lfn.get());
                output->putBuffer(buf, len, true);
                output->flush(true);
                output.clear();
                if (callback->wait(callbackTimeout))
                {
                    ok = true;
                    break;
                }
                else
                {
                    DBGLOG("timed out waiting for server callback - retrying");
                }
            }
            if (ok)
            {
                if (traceLevel > 6)
                    { StringBuffer s; DBGLOG("Processing information from server in response to %s", newHeader.toString(s).str()); }

                MemoryBuffer &serverData = callback->queryData();
                byte type;
                serverData.read(type);
                fileType = (RoxieFileType) type;
                fileTimeStamp.deserialize(serverData);
                serverData.read(fileCheckSum);
                serverData.read(fileSize);
                unsigned numSubFiles;
                serverData.read(numSubFiles);
                for (unsigned fileNo = 0; fileNo < numSubFiles; fileNo++)
                {
                    StringBuffer subName;
                    serverData.read(subName);
                    subNames.append(subName.str());
                    deserializeFilePart(serverData, subFiles, fileNo, false);
                    bool remotePresent;
                    serverData.read(remotePresent);
                    if (remotePresent)
                        deserializeFilePart(serverData, remoteSubFiles, fileNo, true);
                    else
                        remoteSubFiles.append(NULL);
                    if (fileType==ROXIE_KEY)
                    {
                        bool diskMetaPresent;
                        serverData.read(diskMetaPresent);
                        if (diskMetaPresent)
                            diskMeta.append(deserializeRecordMeta(serverData, true));
                        else
                            diskMeta.append(NULL);
                    }
                }
                bool propertiesPresent;
                serverData.read(propertiesPresent);
                if (propertiesPresent)
                    properties.setown(createPTree(serverData));
            }
            else
                throw MakeStringException(ROXIE_CALLBACK_ERROR, "Failed to get response from server for dynamic file callback");
        }
        catch (...)
        {
            ROQ->removePendingCallback(callback);
            throw;
        }
        ROQ->removePendingCallback(callback);
    }
private:
    void deserializeFilePart(MemoryBuffer &serverData, IPointerArrayOf<IFileDescriptor> &files, unsigned fileNo, bool remote)
    {
        IArrayOf<IPartDescriptor> parts;
        deserializePartFileDescriptors(serverData, parts);
        if (parts.length())
        {
            files.append(LINK(&parts.item(0).queryOwner()));
        }
        else
        {
            if (traceLevel > 6)
                DBGLOG("No information for %s subFile %d of file %s", remote ? "remote" : "", fileNo, lfn.get());
            files.append(NULL);
        }
    }
};

extern IResolvedFileCreator *createResolvedFile(const char *lfn, const char *physical, bool isSuperFile)
{
    return new CResolvedFile(lfn, physical, NULL, ROXIE_FILE, NULL, true, false, false, isSuperFile);
}

extern IResolvedFile *createResolvedFile(const char *lfn, const char *physical, IDistributedFile *dFile, IRoxieDaliHelper *daliHelper, bool isDynamic, bool cacheIt, bool writeAccess)
{
    const char *kind = dFile ? dFile->queryAttributes().queryProp("@kind") : NULL;
    return new CResolvedFile(lfn, physical, dFile, kind && stricmp(kind, "key")==0 ? ROXIE_KEY : ROXIE_FILE, daliHelper, isDynamic, cacheIt, writeAccess, false);
}

class CSlaveDynamicFileCache : public CInterface, implements ISlaveDynamicFileCache
{
    mutable CriticalSection crit;
    CIArrayOf<CSlaveDynamicFile> files; // expect numbers to be small - probably not worth hashing
    unsigned tableSize;

public:
    IMPLEMENT_IINTERFACE;
    CSlaveDynamicFileCache(unsigned _limit) : tableSize(_limit) {}

    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, unsigned checksum, RoxiePacketHeader *header, bool isOpt, bool isLocal)
    {
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s;
            logctx.CTXLOG("lookupDynamicFile %s for packet %s", lfn, header->toString(s).str());
        }
        // we use a fixed-size array with linear lookup for ease of initial coding - but unless we start making heavy use of the feature this may be adequate.
        CriticalBlock b(crit);
        if (!cacheDate.isNull())
        {
            unsigned idx = 0;
            while (files.isItem(idx))
            {
                CSlaveDynamicFile &f = files.item(idx);
                if (f.channel==header->channel && f.serverIdx==header->serverIdx && stricmp(f.queryFileName(), lfn)==0)
                {
                    if (!cacheDate.equals(f.queryTimeStamp()) || checksum != f.queryCheckSum())
                    {
                        if (f.isKey())
                            clearKeyStoreCacheEntry(f.queryFileName());
                        files.remove(idx);
                        idx--;
                    }
                    else if ((!f.isLocal || isLocal) && f.isOpt==isOpt)
                    {
                        files.swap(idx, 0);
                        return LINK(&f);
                    }
                }
                idx++;
            }
        }
        Owned<CSlaveDynamicFile> ret;
        {
            // Don't prevent access to the cache while waiting for server to reply. Can deadlock if you do, apart from being inefficient
            CriticalUnblock b1(crit);
            ret.setown(new CSlaveDynamicFile(logctx, lfn, header, isOpt, isLocal));
        }
        while (files.length() > tableSize)
            files.remove(files.length()-1);
        files.add(*ret.getLink(), 0);
        return ret.getClear();
    }
};

static CriticalSection slaveDynamicFileCacheCrit;
static Owned<ISlaveDynamicFileCache> slaveDynamicFileCache;

extern ISlaveDynamicFileCache *querySlaveDynamicFileCache()
{
    if (!slaveDynamicFileCache)
    {
        CriticalBlock b(slaveDynamicFileCacheCrit);
        if (!slaveDynamicFileCache)
            slaveDynamicFileCache.setown(new CSlaveDynamicFileCache(20));
    }
    return slaveDynamicFileCache;
}

extern void releaseSlaveDynamicFileCache()
{
    CriticalBlock b(slaveDynamicFileCacheCrit);
    slaveDynamicFileCache.clear();
}


// Initialization/termination
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    fileCache = new CRoxieFileCache;
    return true;
}

MODULE_EXIT()
{ 
    fileCache->join();
    fileCache->Release();
}

extern IRoxieFileCache &queryFileCache()
{
    return *fileCache;
}

class CRoxieWriteHandler : public CInterface, implements IRoxieWriteHandler
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieWriteHandler(IRoxieDaliHelper *_daliHelper, ILocalOrDistributedFile *_dFile, const StringArray &_clusters)
    : daliHelper(_daliHelper), dFile(_dFile)
    {
        ForEachItemIn(idx, _clusters)
        {
            addCluster(_clusters.item(idx));
        }
        if (dFile->queryDistributedFile())
        {
            isTemporary = (localCluster.get() == NULL); // if only writing to remote clusters, write to a temporary first, then copy
            if (isTemporary)
            {
                UNIMPLEMENTED;
            }
            else
                localFile.setown(dFile->getPartFile(0, 0));
        }
        else
        {
            isTemporary = false;
            localFile.setown(dFile->getPartFile(0, 0));
        }
        if (!recursiveCreateDirectoryForFile(localFile->queryFilename()))
            throw MakeStringException(ROXIE_FILE_ERROR, "Cannot create directory for file %s", localFile->queryFilename());
    }
    virtual IFile *queryFile() const
    {
        return localFile;
    }
    void getClusters(StringArray &clusters) const
    {
        ForEachItemIn(idx, allClusters)
        {
            clusters.append(allClusters.item(idx));
        }
    }

    virtual void finish(bool success, const IRoxiePublishCallback *activity)
    {
        if (success)
        {
            copyPhysical();
            if (daliHelper && daliHelper->connected())
                publish(activity);
        }
        if (isTemporary || !success)
        {
            localFile->remove();
        }
    }
private:
    bool isTemporary;
    Linked<IRoxieDaliHelper> daliHelper;
    Owned<ILocalOrDistributedFile> dFile;
    Owned<IFile> localFile;
    Owned<IGroup> localCluster;
    StringAttr localClusterName;
    IArrayOf<IGroup> remoteNodes;
    StringArray allClusters;

    void copyPhysical() const
    {
        if (remoteNodes.length())
        {
            RemoteFilename rfn, rdn;
            dFile->getPartFilename(rfn, 0, 0);
            StringBuffer physicalName, physicalDir, physicalBase;
            rfn.getLocalPath(physicalName);
            splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
            rdn.setLocalPath(physicalDir.str());
            ForEachItemIn(idx, remoteNodes)
            {
                rdn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
                rfn.setEp(remoteNodes.item(idx).queryNode(0).endpoint());
                Owned<IFile> targetdir = createIFile(rdn);
                Owned<IFile> target = createIFile(rfn);
                targetdir->createDirectory();
                copyFile(target, localFile);
            }
        }
    }

    void publish(const IRoxiePublishCallback *activity)
    {
        if (!dFile->isExternal())
        {
            Owned<IFileDescriptor> desc = createFileDescriptor();
            desc->setNumParts(1);

            RemoteFilename rfn;
            dFile->getPartFilename(rfn, 0, 0);
            StringBuffer physicalName, physicalDir, physicalBase;
            rfn.getLocalPath(physicalName);
            splitFilename(physicalName, &physicalDir, &physicalDir, &physicalBase, &physicalBase);
            desc->setDefaultDir(physicalDir.str());
            desc->setPartMask(physicalBase.str());

            IPropertyTree &partProps = desc->queryPart(0)->queryProperties(); //properties of the first file part.
            IPropertyTree &fileProps = desc->queryProperties(); // properties of the logical file
            offset_t fileSize = localFile->size();
            fileProps.setPropInt64("@size", fileSize);
            partProps.setPropInt64("@size", fileSize);

            CDateTime createTime, modifiedTime, accessedTime;
            localFile->getTime(&createTime, &modifiedTime, &accessedTime);
            // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
            unsigned hour, min, sec, nanosec;
            modifiedTime.getTime(hour, min, sec, nanosec);
            modifiedTime.setTime(hour, min, sec, 0);
            StringBuffer timestr;
            modifiedTime.getString(timestr);
            if(timestr.length())
                partProps.setProp("@modified", timestr.str());

            ClusterPartDiskMapSpec partmap;
            if (localCluster)
            {
                desc->addCluster(localCluster, partmap);
                desc->setClusterGroupName(0, localClusterName.get());
            }
            ForEachItemIn(idx, remoteNodes)
                desc->addCluster(&remoteNodes.item(idx), partmap);

            if (activity)
                activity->setFileProperties(desc);

            Owned<IDistributedFile> publishFile = queryDistributedFileDirectory().createNew(desc); // MORE - we'll create this earlier if we change the locking paradigm
            publishFile->setAccessedTime(modifiedTime);
            publishFile->attach(dFile->queryLogicalName(), activity ? activity->queryUserDescriptor() : UNKNOWN_USER);
            // MORE should probably write to the roxielocalstate too in case Dali is down next time I look...
        }
    }

    void addCluster(char const * cluster)
    {
        Owned<IGroup> group = queryNamedGroupStore().lookup(cluster);
        if (!group)
            throw MakeStringException(0, "Unknown cluster %s while writing file %s",
                    cluster, dFile->queryLogicalName());
        if (group->isMember())
        {
            if (localCluster)
                throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                        cluster, dFile->queryLogicalName());
            localCluster.setown(group.getClear());
            localClusterName.set(cluster);
        }
        else
        {
            ForEachItemIn(idx, remoteNodes)
            {
                Owned<INode> other = remoteNodes.item(idx).getNode(0);
                if (group->isMember(other))
                    throw MakeStringException(0, "Cluster %s occupies node already specified while writing file %s",
                            cluster, dFile->queryLogicalName());
            }
            remoteNodes.append(*group.getClear());
        }
        allClusters.append(cluster);
    }
};

extern IRoxieWriteHandler *createRoxieWriteHandler(IRoxieDaliHelper *_daliHelper, ILocalOrDistributedFile *_dFile, const StringArray &_clusters)
{
    return new CRoxieWriteHandler(_daliHelper, _dFile, _clusters);
}

//================================================================================================================

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class CcdFileTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CcdFileTest);
        CPPUNIT_TEST(testCopy);
    CPPUNIT_TEST_SUITE_END();
protected:

    class DummyPartDescriptor : public CInterfaceOf<IPartDescriptor>
    {
        virtual unsigned queryPartIndex() { UNIMPLEMENTED; }

        virtual unsigned numCopies() { UNIMPLEMENTED; }
        virtual INode *getNode(unsigned copy=0) { UNIMPLEMENTED; }
        virtual INode *queryNode(unsigned copy=0) { UNIMPLEMENTED; }

        virtual IPropertyTree &queryProperties() { UNIMPLEMENTED; }
        virtual IPropertyTree *getProperties() { UNIMPLEMENTED; }

        virtual RemoteFilename &getFilename(unsigned copy, RemoteFilename &rfn) { UNIMPLEMENTED; }
        virtual StringBuffer &getTail(StringBuffer &name) { UNIMPLEMENTED; }
        virtual StringBuffer &getDirectory(StringBuffer &name,unsigned copy = 0) { UNIMPLEMENTED; }
        virtual StringBuffer &getPath(StringBuffer &name,unsigned copy = 0) { UNIMPLEMENTED; }

        virtual void serialize(MemoryBuffer &tgt) { UNIMPLEMENTED; }

        virtual bool isMulti() { UNIMPLEMENTED; }
        virtual RemoteMultiFilename &getMultiFilename(unsigned copy, RemoteMultiFilename &rfn) { UNIMPLEMENTED; }

        virtual bool getCrc(unsigned &crc) { UNIMPLEMENTED; }
        virtual IFileDescriptor &queryOwner() { UNIMPLEMENTED; }
        virtual const char *queryOverrideName() { UNIMPLEMENTED; }
        virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL) { UNIMPLEMENTED; }
        virtual IReplicatedFile *getReplicatedFile() { UNIMPLEMENTED; }
    };

    void testCopy()
    {
        CRoxieFileCache cache(true);
        StringArray remotes;
        DummyPartDescriptor pdesc;
        CDateTime dummy;
        remotes.append("test.remote");

        int f = open("test.remote", _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        int val = 1;
        int wrote = write(f, &val, sizeof(int));
        CPPUNIT_ASSERT(wrote==sizeof(int));
        close(f);

        Owned<ILazyFileIO> io = cache.openFile("test.local", 0, "test.local", NULL, remotes, sizeof(int), dummy, 0);
        CPPUNIT_ASSERT(io != NULL);

        // Reading it should read 1
        val = 0;
        io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(val==1);

        // Now create the buddy

        f = open("test.buddy", _O_WRONLY | _O_CREAT | _O_TRUNC, _S_IREAD | _S_IWRITE);
        val = 2;
        ssize_t numwritten = write(f, &val, sizeof(int));
        CPPUNIT_ASSERT(numwritten == sizeof(int));
        close(f);

        // Reading it should still read 1...
        val = 0;
        io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(val==1);

        // Now copy it - should copy the buddy
        cache.doCopy(io, false, false);

        // Reading it should read 2...
        val = 0;
        io->read(0, sizeof(int), &val);
        CPPUNIT_ASSERT(val==2);

        // And the data in the file should be 2
        f = open("test.local", _O_RDONLY);
        val = 0;
        ssize_t numread = read(f, &val, sizeof(int));
        CPPUNIT_ASSERT(numread == sizeof(int));
        close(f);
        CPPUNIT_ASSERT(val==2);

        io.clear();
        remove("test.local");
        remove("test.remote");
        remove("test.buddy");
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( CcdFileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CcdFileTest, "CcdFileTest" );

#endif
