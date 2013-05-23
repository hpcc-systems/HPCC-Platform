/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    bool memFileRequested;
    StringAttr id;
    bool remote;
    offset_t fileSize;
    CDateTime fileDate;
    unsigned crc;
    Owned<ILazyFileIO> patchFile;
    StringBuffer baseIndexFileName;
    RoxieFileType fileType;
    unsigned lastAccess;
    bool copying;
    bool isCompressed;
    const IRoxieFileCache *cached;

#ifdef FAIL_20_READ
    unsigned readCount;
#endif

public:
    IMPLEMENT_IINTERFACE;

    CLazyFileIO(const char *_id, RoxieFileType _fileType, IFile *_logical, offset_t size, const CDateTime &_date, bool _memFileRequested, unsigned _crc, bool _isCompressed)
        : id(_id), 
          fileType(_fileType), logical(_logical), fileSize(size), crc(_crc), isCompressed(_isCompressed)
    {
        fileDate.set(_date);
        currentIdx = 0;
        current.set(&failure);
        remote = false;
#ifdef FAIL_20_READ
        readCount = 0;
#endif
        memFileRequested = _memFileRequested;
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
                atomic_dec(&numFilesOpen[remote]);
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
#if 0
                    if (memFileRequested && &sources.item(currentIdx)==logical)
                        current.setown(createMemoryFile(logical));
                    else
#endif
                    {
                        IFile *f = &sources.item(currentIdx);
                        if (firstTime)
                            cacheFileConnect(f, dafilesrvLookupTimeout);  // set timeout to 10 seconds
                        else
                        {
                            if (traceLevel > 10)
                                DBGLOG("Looking for file using non-cached file open");
                        }

                        fileStatus = queryFileCache().fileUpToDate(f, fileType, fileSize, fileDate, crc, sourceName, isCompressed);
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
                DBGLOG("Failed to read length %d offset %"I64F"x file %s", len, pos, sources.item(currentIdx).queryFilename());
                currentIdx++;
                setFailure();
            }
            _checkOpen();
            tries++;
            if (tries == MAX_READ_RETRIES)
                throw MakeStringException(ROXIE_FILE_ERROR, "Failed to read length %d offset %"I64F"x file %s after %d attempts", len, pos, sources.item(currentIdx).queryFilename(), tries);
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

    virtual void setBaseIndexFileName(const char *val)
    {
        CriticalBlock b(crit);
        baseIndexFileName.append(val);
    }

    virtual const char *queryBaseIndexFileName()
    {
        CriticalBlock b(crit);
        if (baseIndexFileName.length())
            return baseIndexFileName.str();
        return NULL;
    }

    virtual void setPatchFile(ILazyFileIO *val)
    {
        CriticalBlock b(crit);
        patchFile.setown(LINK(val));
    }

    virtual ILazyFileIO *queryPatchFile()
    {
        CriticalBlock b(crit);
        return patchFile;
    }

    virtual size32_t write(offset_t pos, size32_t len, const void * data) { throwUnexpected(); }
    virtual void setSize(offset_t size) { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { throwUnexpected(); return 0; }

    virtual const char *queryFilename() { return logical->queryFilename(); }
    virtual bool isAlive() const { return CInterface::isAlive(); }
    virtual int getLinkCount() const { return CInterface::getLinkCount(); }
    virtual RoxieFileType getFileType() { return fileType; }

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
                if (queryFileCache().fileUpToDate(&sources.item(currentIdx), fileType, fileSize, fileDate, crc, sourceName, isCompressed) == FileIsValid)
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

    static int compareAccess(IInterface **L, IInterface **R)
    {
        ILazyFileIO *LL = (ILazyFileIO *) *L;
        ILazyFileIO *RR = (ILazyFileIO *) *R;
        return LL->getLastAccessed() - RR->getLastAccessed();
    }
};

//----------------------------------------------------------------------------------------------

typedef StringArray *StringArrayPtr;
typedef MapStringTo<StringArrayPtr> MapStringToDiffFileUsage;

class CRoxieFileCache : public CInterface, implements ICopyFileProgress, implements IRoxieFileCache
{
    mutable ICopyArrayOf<ILazyFileIO> todo; // Might prefer a queue but probably doesn't really matter.
    InterruptableSemaphore toCopy;
    InterruptableSemaphore toClose;
    mutable CopyMapStringToMyClass<ILazyFileIO> files;
    mutable CriticalSection crit;
    CriticalSection cpcrit;
    bool started;
    bool aborting;
    bool closing;
    bool closePending[2];
    bool needToDeleteFile;
    StringBuffer currentTodoFile;
    StringAttrMapping fileErrorList;
    Semaphore bctStarted;
    Semaphore hctStarted;


    RoxieFileStatus fileUpToDate(IFile *f, RoxieFileType fileType, offset_t size, const CDateTime &modified, unsigned crc, const char* id, bool isCompressed)
    {
        if (f->exists())
        {
            // only check size if specified
            if ( (size != -1) && !isCompressed && f->size()!=size) // MORE - should be able to do better on compressed you'da thunk
                return FileSizeMismatch;

            if (crc > 0)
            {
                // if a crc is specified lets check it
                unsigned file_crc = getFileCRC(id);
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

    ILazyFileIO *openFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &remoteLocationInfo, offset_t size, const CDateTime &modified, bool memFile, unsigned crc, bool isCompressed)
    {
        Owned<IFile> local = createIFile(localLocation);

        Owned<CLazyFileIO> ret = new CLazyFileIO(id, fileType, local.getLink(), size, modified, memFile, crc, isCompressed);
        RoxieFileStatus fileStatus = fileUpToDate(local, fileType, size, modified, crc, localLocation, isCompressed);
        if (fileStatus == FileIsValid)
        {
            ret->addSource(local.getLink());
            ret->setRemote(false);
        }
        else if (copyResources || useRemoteResources)
        {
            if (local->exists())
            {
                StringBuffer errStatus;
                switch (fileStatus)
                {
                    case FileSizeMismatch:
                        errStatus.append("FileSizeMismatch");
                        break;

                    case FileCRCMismatch:
                        errStatus.append("FileCRCMismatch");
                        break;

                    case FileDateMismatch:
                        errStatus.append("FileDateMismatch");
                        break;
                }
                DBGLOG("Removing local file - %s because %s", localLocation, errStatus.str());
                local->remove();
            }
            bool addedOne = false;

            // put the peerRoxieLocations next in the list
            ForEachItemIn(roxie_idx, peerRoxieCopiedLocationInfo)
            {
                try
                {
                    const char *remoteName = peerRoxieCopiedLocationInfo.item(roxie_idx);
                    if (miscDebugTraceLevel > 10)
                        DBGLOG("adding peer roxie location %s", remoteName);
        
                    ret->addSource(createIFile(remoteName));
                    addedOne = true;
                }
                catch (IException *E)
                {
                    EXCLOG(MCoperatorError, E, "While creating remote file reference");
                    E->Release();
                }
            }

            ForEachItemIn(idx, remoteLocationInfo)
            {
                try
                {
                    const char *remoteName = remoteLocationInfo.item(idx);
                    if (miscDebugTraceLevel > 10)
                        DBGLOG("adding remote location %s", remoteName);
        
                    ret->addSource(createIFile(remoteName));
                    addedOne = true;
                }
                catch (IException *E)
                {
                    EXCLOG(MCoperatorError, E, "While creating remote file reference");
                    E->Release();
                }
            }
            if (!addedOne)
            {
                StringBuffer remoteLocs;

//              ForEachItemIn(roxie_idx, peerRoxieCopiedLocationInfo)
//                  remoteLocs.appendf("%s ", peerRoxieCopiedLocationInfo.item(roxie_idx));  // all remote locations that were checked

                ForEachItemIn(idx2, remoteLocationInfo)
                    remoteLocs.appendf("%s ", remoteLocationInfo.item(idx2));  // all remote locations that were checked
                throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Could not open file %s at any remote location - %s", localLocation, remoteLocs.str());
            }
            ret->setRemote(true);
        }
        else
            throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Could not open file %s", localLocation);
        ret->setCache(this);
        files.setValue(localLocation, (ILazyFileIO *)ret);
        return ret.getClear();
    }

    bool doCreateFromPatch(ILazyFileIO *targetFile, const char *baseIndexFilename, ILazyFileIO *patchFile, const char *targetFilename, const char *destPath)
    {
        if (!enableKeyDiff)
            return false;  // feature disabled in roxietopology

        bool fileCopied = false;
        IFile *patch_sourceFile;
        try
        {
            // MORE - sort out when to disallow closes and of what
            patch_sourceFile = patchFile->querySource();
        }
        catch (IException *E)
        {
            EXCLOG(MCoperatorError, E, "While trying to open patch file");
            throw;
        }

        unsigned __int64 freeDiskSpace = getFreeSpace(destPath);
        if ( (targetFile->size() + minFreeDiskSpace) > freeDiskSpace)
        {
            StringBuffer err;
            err.appendf("Insufficient disk space.  File %s needs %"I64F"d bytes, but only %"I64F"d remains, and %"I64F"d is needed as a reserve", targetFilename, targetFile->size(), freeDiskSpace, minFreeDiskSpace   );
            IException *E = MakeStringException(ROXIE_DISKSPACE_ERROR, "%s", err.str());
            EXCLOG(MCoperatorError, E);
            E->Release();
        }
        else
        {
            try
            {
                MTimeSection timing(NULL, "createKeyDiff");
                Owned<IKeyDiffApplicator> patchApplicator;
                const char *patchFilename = patch_sourceFile->queryFilename();
                    
                DBGLOG("***** Using KeyDiff to create %s", targetFilename);
                patchApplicator.setown(createKeyDiffApplicator(patchFilename, baseIndexFilename, targetFilename, NULL, true, true));
                patchApplicator->run();
                patchApplicator.clear();

                // need to update time stamp in roxiestate file
                Owned<IFile> tmp_file = createIFile(targetFilename);
                IFile* remote_sourceFile = targetFile->querySource();
                CDateTime dt1, dt2, dt3;
                remote_sourceFile->getTime(&dt1, &dt2, &dt3);
                tmp_file->setTime(&dt1, &dt2, &dt3);
            }
            catch(IException *E)
            {
                EXCLOG(E, "Create PatchFile exception");
                E->Release();
                return false;
                //throw; - do not treat as fatal
            }
            if (needToDeleteFile)
            {
                DBGLOG("creating of data file %s stopped since query has been deleted", targetFilename);
            }
            else
            {
                targetFile->copyComplete();
                fileCopied = true;
            }
        }
        return fileCopied;
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

    bool doCopyFile(ILazyFileIO *f, const char *tempFile, const char *targetFilename, const char *destPath, const char *msg)
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
        if ( (sourceFile->size() + minFreeDiskSpace) > freeDiskSpace)
        {
            StringBuffer err;
            err.appendf("Insufficient disk space.  File %s needs %"I64F"d bytes, but only %"I64F"d remains, and %"I64F"d is needed as a reserve", targetFilename, sourceFile->size(), freeDiskSpace, minFreeDiskSpace);
            IException *E = MakeStringException(ROXIE_DISKSPACE_ERROR, "%s", err.str());
            EXCLOG(MCoperatorError, E);
            E->Release();
        }
        else
        {
            IpSubNet subnet; // preferred set but not required
            IpAddress fromip; // returned
            Owned<IFile> destFile = createIFile(useTreeCopy?targetFilename:tempFile);

            bool hardLinkCreated = false;
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
                        MTimeSection timing(NULL, str.str());
                        if (useTreeCopy)
                            sourceFile->treeCopyTo(destFile, subnet, fromip, true);
                        else
                            sourceFile->copyTo(destFile);
                    }
                    else
                    {
                        if (useTreeCopy)
                            sourceFile->treeCopyTo(destFile, subnet, fromip, true);
                        else
                            sourceFile->copyTo(destFile);
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
            if (needToDeleteFile)
            {
                DBGLOG("%s of data file %s stopped since query has been deleted", msg, targetFilename);
                destFile->remove();
            }
            else 
            {
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

                    DBGLOG("%s to %s complete", msg, targetFilename);
                }
                
                f->copyComplete();
            }
        }
        deleteTempFiles(targetFilename);
        return fileCopied;
    }

    bool doCopy(ILazyFileIO *f, bool background, bool displayFirstFileMessage)
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
            
            const char *baseIndexFilename = f->queryBaseIndexFileName();
            ILazyFileIO* patchFile = f->queryPatchFile();

            if (baseIndexFilename && patchFile)
                if (doCreateFromPatch(f, baseIndexFilename, patchFile, targetFilename, destPath.str()))
                    return true;

            tempFile.append(".$$$");
            const char *msg = background ? "Background copy" : "Copy";
            return doCopyFile(f, tempFile.str(), targetFilename, destPath.str(), msg);
        }
        return false;  // if we get here there was no file copied
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieFileCache(bool testmode = false) : bct(*this), hct(*this)
    {
        aborting = false;
        closing = false;
        closePending[false] = false;
        closePending[true] = false;
        needToDeleteFile = false;
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
                {
                    CriticalBlock b(crit);
                    needToDeleteFile = false;
                    currentTodoFile.clear();
                }
                fileCopied = false;
                Linked<ILazyFileIO> next;
                toCopy.wait();
                {
                    CriticalBlock b(crit);
                    if (closing)
                        break;
                    if (todo.ordinality())
                    {
                        ILazyFileIO *popped = &todo.pop();
                        if (popped->isAlive())
                        {
                            next.set(popped);
                            if (next)
                                currentTodoFile.append(next->queryFilename());
                        }
                        atomic_dec(&numFilesToProcess);    // must decrement counter for SNMP accuracy
                    }
                }
                if (next)
                {
                    try
                    {
                        fileCopied = doCopy(next, true, (fileCopiedCount==0) ? true : false);
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
            }
        }
    }

    virtual ILazyFileIO *lookupFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const char *baseIndexFileName,  ILazyFileIO *patchFile, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &deployedLocationInfo, offset_t size, const CDateTime &modified, bool memFile, bool isRemote, bool startFileCopy, bool doForegroundCopy, unsigned crc, bool isCompressed, const char *lookupDali)
    {
        Owned<ILazyFileIO> ret;
        try
        {
            CriticalBlock b(crit);
            Linked<ILazyFileIO> f = files.getValue(localLocation);
            if (f && f->isAlive())
            {
                if ((size != -1 && size != f->getSize()) ||
                    (!modified.isNull() && !modified.equals(*f->queryDateTime(), false)))
                {
                    StringBuffer modifiedDt;
                    if (!modified.isNull())
                        modified.getString(modifiedDt);
                    StringBuffer fileDt;
                    f->queryDateTime()->getString(fileDt);
                    if (fileErrorList.find(id) == 0)
                    {
                        switch (fileType)
                        {
                            case ROXIE_KEY:
                                fileErrorList.setValue(id, "Key");
                                break;

                            case ROXIE_FILE:
                                fileErrorList.setValue(id, "File");
                                break;
                        }
                    }
                    throw MakeStringException(ROXIE_MISMATCH, "Different version of %s already loaded: sizes = %"I64F"d %"I64F"d  Date = %s  %s", id, size, f->getSize(), modifiedDt.str(), fileDt.str());
                }
                else
                    return f.getClear();
            }

            ret.setown(openFile(id, partNo, fileType, localLocation, peerRoxieCopiedLocationInfo, deployedLocationInfo, size, modified, memFile, crc, isCompressed));  // for now don't check crcs

            if (baseIndexFileName)
                ret->setBaseIndexFileName(baseIndexFileName);
            if (patchFile)
                ret->setPatchFile(patchFile);

            if (startFileCopy)
            {
                if (ret->isRemote())
                {
                    if (copyResources || memFile)
                    {
                        needToDeleteFile = false;
                        if (doForegroundCopy)
                        {
                            ret->checkOpen();
                            doCopy(ret, false, false);
                            return ret.getLink();
                        }

                        todo.append(*ret);
                        atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
                        toCopy.signal();

                    }
                }
                else if (isRemote)
                {
//                  todo.append(*ret);  // don't really need to copy. But do need to send message that copy happened (async)
//                  atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
//                  toCopy.signal();
                }
            }

            if (!lazyOpen || fileType == ROXIE_PATCH)  // patch file MUST be open at this point - make sure we open it
                ret->checkOpen();
        }
        catch(IException *e)
        {
            if (e->errorCode() == ROXIE_FILE_OPEN_FAIL)
            {
                if (fileErrorList.find(id) == 0)
                {
                    switch (fileType)
                    {
                        case ROXIE_KEY:
                            fileErrorList.setValue(id, "Key");
                            break;
                        
                        case ROXIE_FILE:
                            fileErrorList.setValue(id, "File");
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

IPartDescriptor *queryMatchingRemotePart(IPartDescriptor *pdesc, IFileDescriptor *remoteFDesc, unsigned int partNum)
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

inline bool isCopyFromCluster(IPartDescriptor *pdesc, unsigned clusterNo, const char *process)
{
    StringBuffer s;
    return strieq(process, pdesc->queryOwner().getClusterGroupName(clusterNo, s));
}

inline bool checkClusterCount(UnsignedArray &counts, unsigned clusterNo, unsigned max)
{
    while (!counts.isItem(clusterNo))
        counts.append(0);
    unsigned count = counts.item(clusterNo);
    if (count>=max)
        return false;
    counts.replace(++count, clusterNo);
    return true;
}

inline void appendRemoteLocations(IPartDescriptor *pdesc, StringArray &locations, bool checkSelf)
{
    UnsignedArray clusterCounts;
    unsigned numCopies = pdesc->numCopies();
    for (unsigned copy = 0; copy < numCopies; copy++)
    {
        unsigned clusterNo = pdesc->copyClusterNum(copy);
        if (!checkClusterCount(clusterCounts, clusterNo, 2))
            continue;
        if (checkSelf && isCopyFromCluster(pdesc, clusterNo, roxieName.str())) //don't add ourself
            continue;
        RemoteFilename r;
        pdesc->getFilename(copy,r);
        StringBuffer path;
        locations.append(r.getRemotePath(path).str());
    }
}

ILazyFileIO *createDynamicFile(const char *id, IPartDescriptor *pdesc, IPartDescriptor *remotePDesc, RoxieFileType fileType, int numParts, bool startCopy)
{
    IPropertyTree &partProps = pdesc->queryProperties();
    offset_t dfsSize = partProps.getPropInt64("@size");
    unsigned crc;
    if (!pdesc->getCrc(crc))
        crc = 0;
    CDateTime fileDate;
    if (checkFileDate)
    {
        const char *dateStr = partProps.queryProp("@modified");
        fileDate.setString(dateStr);
    }

    StringArray localLocations;
    StringArray remoteLocations;

    unsigned partNo = pdesc->queryPartIndex() + 1;
    StringBuffer localFileName;

    CDfsLogicalFileName dlfn;   
    dlfn.set(id);
    if (dlfn.isForeign())
        dlfn.clearForeign();

    const char *logicalname = dlfn.get();

    makePhysicalPartName(logicalname, partNo, numParts, localFileName, false, DFD_OSdefault, baseDataDirectory);  // MORE - if we get the dataDirectory we can pass it in and possibly reuse an existing file

    appendRemoteLocations(pdesc, remoteLocations, true);
    if (remotePDesc)
        appendRemoteLocations(remotePDesc, remoteLocations, false);

    bool foregroundCopy = numParts==1 || (partNo==numParts && fileType==ROXIE_KEY);
    return queryFileCache().lookupFile(id, partNo, fileType, localFileName, NULL, NULL, localLocations, remoteLocations, dfsSize, fileDate, false, true, startCopy, foregroundCopy, crcResources ? crc : 0, pdesc->queryOwner().isCompressed(), NULL);
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
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%"I64F"d vs %"I64F"d", map[numParts-1].top, totalSize);
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
            assertex(size != (unsigned __int64) -1);
            map[i].base = i ? map[i-1].top : 0;
            map[i].top = map[i].base + size;
        }
        if (totalSize == (offset_t)-1)
            totalSize = map[numParts-1].top;
        else if (totalSize != map[numParts-1].top)
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file part sizes do not add up to expected total size (%"I64F"d vs %"I64F"d", map[numParts-1].top, totalSize);
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
            throw MakeStringException(ROXIE_DATA_ERROR, "CFilePartMap: file position %"I64F"d in file %s out of range (max offset permitted is %"I64F"d)", pos, fileName.sget(), totalSize);
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
    PointerIArrayOf<IFileIO> files;
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

    void addFile(IFileIO *f, offset_t base)
    {
        if (f)
            valid++;
        files.append(f);
        bases.append(base);
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
    PointerIArrayOf<X> cache;
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

class CResolvedFile : public CInterface, implements IResolvedFileCreator
{
protected:
    const IRoxiePackage *cached;
    StringAttr lfn;
    StringAttr physicalName;
    Owned<IDistributedFile> dFile; // NULL on copies serialized to slaves. Note that this implies we keep a lock on dali file for the lifetime of this object.
    CDateTime fileTimeStamp;
    RoxieFileType fileType;
    offset_t fileSize;
    unsigned fileCheckSum;

    StringArray subNames;
    PointerIArrayOf<IFileDescriptor> subFiles; // note - on slaves, the file descriptors may have incomplete info. On originating server is always complete
    PointerIArrayOf<IFileDescriptor> remoteSubFiles; // note - on slaves, the file descriptors may have incomplete info. On originating server is always complete
    PointerIArrayOf<IDefRecordMeta> diskMeta;
    Owned <IPropertyTree> properties;

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

    // We cache all the file maps/arrays etc here. 
    mutable CriticalSection lock;
    mutable Owned<IFilePartMap> fileMap;
    mutable PerChannelCacheOf<IInMemoryIndexManager> indexMap;
//  MORE - cache the others, using per-channel cache support. 

public:
    IMPLEMENT_IINTERFACE;
    CResolvedFile(const char *_lfn, const char *_physicalName, IDistributedFile *_dFile, RoxieFileType _fileType, IRoxieDaliHelper* daliHelper, bool cacheIt, bool writeAccess)
    : lfn(_lfn), physicalName(_physicalName), dFile(_dFile), fileType(_fileType)
    {
        cached = NULL;
        fileSize = 0;
        fileCheckSum = 0;
        if (dFile)
        {
            if (traceLevel > 5)
                DBGLOG("Roxie server adding information for file %s", lfn.get());
            IDistributedSuperFile *superFile = dFile->querySuperFile();
            if (superFile)
            {
                Owned<IDistributedFileIterator> subs = superFile->getSubFileIterator(true);
                ForEach(*subs)
                {
                    IDistributedFile &sub = subs->query();
                    Owned<IFileDescriptor> fDesc = sub.getFileDescriptor();
                    Owned<IFileDescriptor> remoteFDesc;
                    if (daliHelper)
                        remoteFDesc.setown(daliHelper->checkClonedFromRemote(sub.queryLogicalName(), fDesc, cacheIt, writeAccess));
                    addFile(sub.queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
                }
            }
            else // normal file, not superkey
            {
                Owned<IFileDescriptor> fDesc = dFile->getFileDescriptor();
                Owned<IFileDescriptor> remoteFDesc;
                if (daliHelper)
                    remoteFDesc.setown(daliHelper->checkClonedFromRemote(_lfn, fDesc, cacheIt, writeAccess));
                addFile(dFile->queryLogicalName(), fDesc.getClear(), remoteFDesc.getClear());
            }
            bool tsSet = dFile->getModificationTime(fileTimeStamp);
            bool csSet = dFile->getFileCheckSum(fileCheckSum);
            assertex(tsSet); // per Nigel, is always set
            properties.set(&dFile->queryAttributes());
        }
    }
    virtual void beforeDispose()
    {
        if (cached)
            cached->removeCache(this);
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
    inline bool isKey() const
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
                assertex(subFiles.length()==1);
                fileMap.setown(createFilePartMap(lfn, *subFiles.item(0)));
            }
        }
        return fileMap.getLink();
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
        Owned<CFileIOArray> f = new CFileIOArray();
        f->addFile(NULL, 0);
        if (subFiles.length())
        {
            IFileDescriptor *fdesc = subFiles.item(0);
            IFileDescriptor *remoteFDesc = remoteSubFiles.item(0);
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
                            Owned<ILazyFileIO> file = createDynamicFile(subNames.item(0), pdesc, remotePDesc, ROXIE_FILE, numParts, cached != NULL);
                            IPropertyTree &partProps = pdesc->queryProperties();
                            f->addFile(file.getClear(), partProps.getPropInt64("@offset"));
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
                            f->addFile(NULL, 0);
                        }
                    }
                    else
                        f->addFile(NULL, 0);
                }
            }
        }
        return f.getClear();
    }
    virtual IKeyArray *getKeyArray(IDefRecordMeta *activityMeta, TranslatorArray *translators, bool isOpt, unsigned channel, bool allowFieldTranslation) const
    {
        Owned<IKeyArray> ret = ::createKeyArray();
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
                                part.setown(createDynamicFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, fdesc->numParts(), cached != NULL));
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
                    Owned<ILazyFileIO> keyFile = createDynamicFile(subNames.item(idx), pdesc, remotePDesc, ROXIE_KEY, numParts, cached != NULL);
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
        // MORE - if we want to share this then we need to pass in channel too and cache per channel. Should combine the get() and the load().
        // MORE - I don't know that it makes sense to pass isOpt in to these calls rather than just when creating the IResolvedFile... think about it.
        // MORE - preload and numkeys are all messed up - can't be specified per query have to be per file
//      return createInMemoryIndexManager(isOpt, lfn);

        CriticalBlock b(lock);
        IInMemoryIndexManager *ret = indexMap.get(channel);
        if (!ret)
        {
            ret = createInMemoryIndexManager(isOpt, lfn);
            ret->load(files, recs, preload, numKeys);
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

    virtual void addSubFile(const IResolvedFile *_sub)
    {
        const CResolvedFile *sub = static_cast<const CResolvedFile *>(_sub);
        if (subFiles.length())
            assertex(sub->fileType==fileType);
        else
            fileType = sub->fileType;
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
        fdesc->setPart(0, queryMyNode(), localFileName, pp);
        addSubFile(fdesc.getClear(), NULL);
    }

    virtual void setCache(const IRoxiePackage *cache)
    {
        assertex (!cached);
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
        properties.clear();
        if (dFile)
        {
            dFile->detach();
        }
        else
        {
            try
            {
                Owned<IFile> file = createIFile(physicalName.get());
                file->remove();
            }
            catch (IException *e)
            {
                ERRLOG(-1, "Error removing file %s",lfn.get());
                e->Release();
            }
        }
    }
    virtual bool exists() const
    {
        // MORE - this is a little bizarre. We sometimes create a resolvedFile for a file that we are intending to create.
        // This will make more sense if/when we start to lock earlier.
        if (dFile)
            return true; // MORE - may need some thought
        else
            return checkFileExists(lfn.get());
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
        : CResolvedFile(_lfn, NULL, NULL, ROXIE_FILE, NULL, false, false), channel(header->channel), serverIdx(header->serverIdx), isOpt(_isOpt), isLocal(_isLocal)
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
    void deserializeFilePart(MemoryBuffer &serverData, PointerIArrayOf<IFileDescriptor> &files, unsigned fileNo, bool remote)
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

extern IResolvedFileCreator *createResolvedFile(const char *lfn, const char *physical)
{
    return new CResolvedFile(lfn, physical, NULL, ROXIE_FILE, NULL, false, false);
}

extern IResolvedFile *createResolvedFile(const char *lfn, const char *physical, IDistributedFile *dFile, IRoxieDaliHelper *daliHelper, bool cacheIt, bool writeAccess)
{
    const char *kind = dFile ? dFile->queryAttributes().queryProp("@kind") : NULL;
    return new CResolvedFile(lfn, physical, dFile, kind && stricmp(kind, "key")==0 ? ROXIE_KEY : ROXIE_FILE, daliHelper, cacheIt, writeAccess);
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

class CDiffFileInfoCache : public CInterface, implements IDiffFileInfoCache
{
    CriticalSection crit;
    MapStringToDiffFileUsage diffFileInfoMap;  // store all diff / patch file location info - even if not used

public:
    IMPLEMENT_IINTERFACE;

    CDiffFileInfoCache()
    {
    }

    ~CDiffFileInfoCache()
    {
        HashIterator info(diffFileInfoMap);
        for(info.first();info.isValid();info.next())
        {
            StringArray *a = *diffFileInfoMap.mapToValue(&info.query());
            delete a;
        }
    }

    virtual void saveDiffFileLocationInfo(const char *id, const StringArray &locations)
    {
        CriticalBlock b(crit);

        StringArray *diffNames = 0;
        StringArrayPtr *diffs = diffFileInfoMap.getValue(id);
        if (diffs)
            diffNames = *diffs;
        else
        {
            diffNames = new StringArray;
            diffFileInfoMap.setValue(id, diffNames);
        }

        ForEachItemIn(idx, locations)
            diffNames->append(locations.item(idx));
    }

    virtual void saveDiffFileLocationInfo(const char *id, const char *location)
    {
        CriticalBlock b(crit);

        StringArray *diffNames = 0;
        StringArrayPtr *diffs = diffFileInfoMap.getValue(id);
        if (diffs)
        {
            diffNames = *diffs;
        }
        else
        {
            diffNames = new StringArray;
            diffFileInfoMap.setValue(id, diffNames);
        }

        diffNames->append(location);
    }

    virtual const char *queryDiffFileNames(StringBuffer &names)
    {
        names.append("<DiffFileNames>");
        HashIterator diffs_iter(diffFileInfoMap);
        for(diffs_iter.first();diffs_iter.isValid();diffs_iter.next())
        {
            IMapping &cur = diffs_iter.query();
            const char *name = (const char *) cur.getKey();
            names.appendf("<name>%s</name>", name);
        }

        names.append("</DiffFileNames>");
        return names.str();
    }

    virtual void deleteDiffFiles(IPropertyTree *tree, IPropertyTree *goers)
    {
        Owned<IPropertyTreeIterator> diffFiles = tree->getElements("Patch");

        ForEach(*diffFiles)
        {
            IPropertyTree &item = diffFiles->query();
            StringBuffer id(item.queryProp("@id"));
            StringArray **a = diffFileInfoMap.getValue(id.str());

            if (!a)
            {
                if (id[0] == '~')
                    id.remove(0,1);
                else
                    id.insert(0,'~');

                a = diffFileInfoMap.getValue(id.str());
            }

            if (a)
            {
                ForEachItemIn(idx, **a)
                {
                    const char *name = (*a)->item(idx);
                    try
                    {
                        OwnedIFile unneededFile = createIFile(name);
                        unneededFile->remove();
                        DBGLOG("deleted key diff file %s", name);
                    }
                    catch (IException *E)
                    {
                        // we don't care if there was an error - the file may not exist
                        E->Release();
                    }
                }
                // add Patch name to delete delta state file info
                IPropertyTree *goer = createPTree("Patch");
                goer->setProp("@id", id);
                goer->setProp("@mode", "delete");
                goers->addPropTree("Patch", goer);

                item.setProp("@mode", "delete");
            }
        }
    }

};


static CriticalSection diffFileInfoCacheCrit;
static Owned<IDiffFileInfoCache> diffFileInfoCache;

extern IDiffFileInfoCache *queryDiffFileInfoCache()
{
    if (!diffFileInfoCache)
    {
        CriticalBlock b(diffFileInfoCacheCrit);
        if (!diffFileInfoCache)
            diffFileInfoCache.setown(new CDiffFileInfoCache());
    }
    return diffFileInfoCache;
}

extern void releaseDiffFileInfoCache()
{
    CriticalBlock b(diffFileInfoCacheCrit);
    diffFileInfoCache.clear();
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
                desc->setClusterRoxieLabel(0, roxieName); // MORE not sure what this is for!!
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

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

class LazyIOTest: public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( LazyIOTest );
        CPPUNIT_TEST(testAllCases);
    CPPUNIT_TEST_SUITE_END();

    void testIt(bool localPresent, bool localCorrect)
    {
        DBGLOG("Testing localPresent=%d localCorrect=%d", localPresent, localCorrect);
        remove("cppfile_localfile1");
        FILE *x = fopen("cppfile_localfile2", "wb");
        assertex(x);
        fputs("Test", x);
        fclose(x);
        if (localPresent)
        {
            if (localCorrect)
                copyFile("cppfile_localfile1", "cppfile_localfile2");
            else
            {
                FILE *x = fopen("cppfile_localfile1", "wb");
                assertex(x);
                fputs("Pink1", x);
                fclose(x);
            }
        }
        Owned<CRoxieFileCache> cache = new CRoxieFileCache(true);
        StringArray remoteNames;
        StringArray peerNames;
        remoteNames.append("cppfile_localfile2");
        CDateTime nullDT;
        Owned<IFileIO> l1 = cache->lookupFile("cppfile_localfile1", 0, ROXIE_FILE, "cppfile_localfile1", NULL, NULL, peerNames, remoteNames, 4, nullDT, false, false, true, false, 0, false, NULL);
        Owned<IFileIO> l2 = cache->lookupFile("cppfile_localfile1", 0, ROXIE_FILE, "cppfile_localfile1", NULL, NULL, peerNames, remoteNames, 4, nullDT, false, false, true, false, 0, false, NULL);
        CPPUNIT_ASSERT(l1 == l2);
        char buf[4];
        l1->read(0, 4, buf);
        if (memcmp(buf, "Test", 4)!=0)
            DBGLOG("huh");
        CPPUNIT_ASSERT(memcmp(buf, "Test", 4)==0);
        cache->start();
        cache->wait();
        memset(buf, 0, 4);
        l1->read(0, 4, buf);
        CPPUNIT_ASSERT(memcmp(buf, "Test", 4)==0);
        l1.clear();
        l2.clear();
        cache.clear();
        DBGLOG("Tested localPresent=%d localCorrect=%d", localPresent, localCorrect);
    }

protected:
    void testAllCases()
    {
        for (unsigned i1 = 0; i1 < 2; i1++)
            for (unsigned i2 = 0; i2 < 2; i2++)
                for (unsigned i3 = 0; i3 < 2; i3++)
                    for (unsigned i4 = 0; i4 < 2; i4++)
                        for (unsigned i5 = 0; i5 < 2; i5++)
                        {
                            useRemoteResources = i1==0;
                            copyResources = i2==0;
                            lazyOpen = i3==0;
                            bool localPresent = i4==0;
                            bool localCorrect = i5==0;
                            try
                            {
                                testIt(localPresent, localCorrect);
                            }
                            catch (IException *E)
                            {
                                E->Release();
                                CPPUNIT_ASSERT(!(localPresent && localCorrect) && !(useRemoteResources || copyResources));
                            }
                        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( LazyIOTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( LazyIOTest, "LazyIOTest" );

#endif

