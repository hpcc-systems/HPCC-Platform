/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    virtual size32_t write(offset_t pos, size32_t len, const void * data) { THROWNOTOPEN; }
    virtual void setSize(offset_t size) { UNIMPLEMENTED; }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { UNIMPLEMENTED; return 0; }
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
    bool fileIsMemFile;
    bool copyInForeground;
    unsigned crc;
    Owned<ILazyFileIO> patchFile;
    StringBuffer baseIndexFileName;
    RoxieFileType fileType;
    unsigned lastAccess;
    bool copying;
    bool isCompressed;

#ifdef FAIL_20_READ
    unsigned readCount;
#endif

public:
    IMPLEMENT_IINTERFACE;

    CLazyFileIO(const char *_id, RoxieFileType _fileType, IFile *_logical, offset_t size, const CDateTime *date, bool _memFileRequested, unsigned _crc, bool _isCompressed) 
        : id(_id), 
          fileType(_fileType), logical(_logical), fileSize(size), crc(_crc), isCompressed(_isCompressed)
    {
        if (date)
            fileDate.set(*date);
        currentIdx = 0;
        current.set(&failure);
        remote = false;
#ifdef FAIL_20_READ
        readCount = 0;
#endif
        memFileRequested = _memFileRequested;
        fileIsMemFile = false;
        copyInForeground = false;
        lastAccess = msTick();
        copying = false;
    }
    
    ~CLazyFileIO()
    {
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

                        fileStatus = queryFileCache().fileUpToDate(f, fileType, fileSize, (!fileDate.isNull()) ? &fileDate : NULL, crc, sourceName, isCompressed);
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
                throw MakeStringException(ROXIE_FILE_FAIL, "Failed to read length %d offset %"I64F"x file %s after %d attempts", len, pos, sources.item(currentIdx).queryFilename(), tries);
        }
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

    // the following calls are always made from inside of a critical block...
    virtual void setFileIsMemFile(bool val) { fileIsMemFile = val; }
    virtual bool getFileIsMemFile() { return fileIsMemFile; }
    virtual void setCopyInForeground(bool val) { copyInForeground = val; }
    virtual bool getCopyInForeground() { return copyInForeground; }


    virtual size32_t write(offset_t pos, size32_t len, const void * data) { throwUnexpected(); }
    virtual void setSize(offset_t size) { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos,offset_t len) { throwUnexpected(); return 0; }

    virtual const char *queryFilename() { return logical->queryFilename(); }
    virtual bool IsShared() const { return CInterface::IsShared(); }
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
                if (queryFileCache().fileUpToDate(&sources.item(currentIdx), fileType, fileSize, (fileDate.isNull()) ? NULL : &fileDate, crc, sourceName, isCompressed) == FileIsValid)
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
    ICopyArrayOf<ILazyFileIO> todo; // Might prefer a queue but probably doesn't really matter.
    InterruptableSemaphore toCopy;
    InterruptableSemaphore toClose;
    MapStringToMyClass<ILazyFileIO> files;
    CriticalSection crit;
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


    RoxieFileStatus fileUpToDate(IFile *f, RoxieFileType fileType, offset_t size, const CDateTime *modified, unsigned crc, const char* id, bool isCompressed)
    {
        if (f->exists())
        {
            // only check size if specified
            if ( (size != -1) && !isCompressed && f->size()!=size) // MORE - should be able to do better on compressed you'da thunk
                return FileSizeMismatch;

            if (crc > 0)
            {
                // if a crc is specified lets check it - query dll crc differs from file / key crc
                unsigned file_crc = 0;
                if (fileType == ROXIE_PLUGIN_DLL)
                    file_crc = 0;  // don't bother checking plugins - roxie not responsible for copying them
                if (fileType == ROXIE_WU_DLL)
                    file_crc = crc_file(id);
                else
                    file_crc = getFileCRC(id);

                if (file_crc && crc != file_crc)  // for remote files crc_file can fail, even if the file is valid
                {
                    DBGLOG("FAILED CRC Check");
                    return FileCRCMismatch;
                }
            }
            CDateTime mt;
            return (modified==NULL || (f->getTime(NULL, &mt, NULL) &&  mt.equals(*modified, false))) ? FileIsValid : FileDateMismatch;
        }
        else
            return FileNotFound;
    }

    ILazyFileIO *openFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &remoteLocationInfo, offset_t size, const CDateTime *modified, bool memFile, unsigned crc, bool isCompressed)
    {
        Owned<IFile> local = createIFile(localLocation);

        Owned<CLazyFileIO> ret = new CLazyFileIO(id, fileType, local.getLink(), size, modified, memFile, crc, isCompressed);
        RoxieFileStatus fileStatus = fileUpToDate(local, fileType, size, modified, crc, localLocation, isCompressed);
        if (fileStatus == FileIsValid)
        {
            ret->addSource(local.getLink());
            ret->setRemote(false);
        }
        else if (copyResources || useRemoteResources || fileType == ROXIE_WU_DLL)
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

        return ret.getClear();
    }

    ILazyFileIO *openPlugin(const char *id, const char *localLocation)
    {
        Owned<IFile> local = createIFile(localLocation);
        Owned<CLazyFileIO> ret = new CLazyFileIO(id, ROXIE_PLUGIN_DLL, local.getLink(), 0, NULL, false, 0, false);

        // MORE - should we check the version label here ?
        if (ret)
        {
            ret->addSource(local.getLink());
            ret->setRemote(false);
            return ret.getClear();
        }

        throw MakeStringException(ROXIE_FILE_OPEN_FAIL, "Could not open file %s", localLocation);
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
                DBGLOG("Received files to copy!!!!!");

            const char *targetFilename = f->queryTarget()->queryFilename();
            StringBuffer tempFile(targetFilename);
            StringBuffer destPath;
            splitFilename(tempFile.str(), &destPath, &destPath, NULL, NULL);
            if (destPath.length())
                recursiveCreateDirectory(destPath.str());
            else
                destPath.append('.');
            
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
                        next.set(&todo.pop());
                        atomic_dec(&numFilesToProcess);    // must decrement counter for SNMP accuracy
                        if (next)
                            currentTodoFile.append(next->queryFilename());
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
                    DBGLOG("No more data files to copy!!!!!");
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

    virtual ILazyFileIO *lookupFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const char *baseIndexFileName,  ILazyFileIO *patchFile, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &deployedLocationInfo, offset_t size, const CDateTime *modified, bool memFile, bool isRemote, bool startFileCopy, bool doForegroundCopy, unsigned crc, bool isCompressed, const char *lookupDali)
    {
        Owned<ILazyFileIO> ret;
        try
        {
            CriticalBlock b(crit);
            ILazyFileIO *f = files.getValue(localLocation);
            if (f)
            {
                if ((size != -1) && (size != f->getSize()) || (modified && !modified->equals(*f->queryDateTime(), false)))
                {
                    StringBuffer modifiedDt;
                    if (modified)
                        modified->getString(modifiedDt);
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

                return LINK(f);
            }

            ret.setown(openFile(id, partNo, fileType, localLocation, peerRoxieCopiedLocationInfo, deployedLocationInfo, size, modified, memFile, crc, isCompressed));  // for now don't check crcs
            files.setValue(localLocation, ret);

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
            else
            {
                ret->setFileIsMemFile(memFile);
                ret->setCopyInForeground(doForegroundCopy);
                                    
                todo.append(*ret);
                atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
                toCopy.signal();
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

    virtual IFileIO *lookupDllFile(const char* dllname, const char *localLocation, const StringArray &remoteNames, unsigned crc, bool isRemote)
    {
        // this is foreground work
        ILazyFileIO *ret;
        {
            CriticalBlock b(crit);
            ret = files.getValue(localLocation);
            if (ret)
            {
                if (crc)
                {
                    unsigned file_crc = crc_file(localLocation);
                    if (crc != file_crc)  // do not have the proper version
                    {
                        throw MakeStringException(ROXIE_MISMATCH, "Different version of %s already loaded", dllname);
                    }
                }
                return LINK(ret);
            }
            ret = openFile(dllname, 1, ROXIE_WU_DLL, localLocation, remoteNames, remoteNames, -1, NULL, false, crc, false);  // make partno = 1 (second param)
            files.setValue(localLocation, ret);
            if (ret->isRemote())
            {
                try
                {
                    needToDeleteFile = false;
                    doCopy(ret, false, false);
                    todo.append(*ret);
                    atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
                    toCopy.signal();
                }
                catch (IException *E)
                {
                    if (ret)
                        ret->Release();
                    EXCLOG(MCoperatorError, E, "Roxie background copy: ");
                    throw;
                }
                catch (...) 
                {
                    EXCLOG(MCoperatorError, "Unknown exception in Roxie background copy");
                }
            }
            else if (isRemote)
            {
//              todo.append(*ret);  // don't really need to copy, but need to update file location
//              atomic_inc(&numFilesToProcess);  // must increment counter for SNMP accuracy
//              toCopy.signal();
            }
        }
        ret->checkOpen();
        return ret;
    }

    virtual IFileIO *lookupPluginFile(const char* dllname, const char *localLocation)
    {
        // plugins should be copied via deployment tool, not by roxie, so they should already be copied to the node - error if not
        ILazyFileIO *ret;
        {
            CriticalBlock b(crit);
            ret = files.getValue(localLocation);
            if (ret) // already opened, valid and increase LINK count
            {
                // MORE - need to check version label
                return LINK(ret);
            }
            
            ret = openPlugin(dllname, localLocation);
            files.setValue(localLocation, ret);
        }
        ret->checkOpen();
        return ret;
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
            if (f->isOpen() && f->isRemote()==remote && !f->isCopying())
            {
                unsigned age = msTick() - f->getLastAccessed();
                if (age > maxFileAge[remote])
                {
                    if (traceLevel > 5)
                    {
                        const char *fname;
                        if (remote)
                            fname = f->querySource()->queryFilename();
                        else
                            fname = f->queryFilename();
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

    virtual void flushUnused(bool deleteFiles, bool cleanUpOneTimeQueries)
    {
        CriticalBlock b(crit);
        // Everything currently in the queue should be removed if it is only in the queue and the cache.
        ForEachItemInRev(idx, todo)
        {
            ILazyFileIO *f = &todo.item(idx);
            if (f)  // make sure it hasn't already been blanked out by prior call to flushUnused
            {
                if (!f->IsShared())
                    todo.remove(idx);
            }
        }

        IArrayOf<ILazyFileIO> goers;
        HashIterator h(files);
        StringAttrMapping metaFileGoers;

        ForEach(h)
        {
            // checking if file is shared, but count is 1 and names match - if so it means the only use of this file 
            // is the copy, so we can remove the file
            ILazyFileIO *f = files.mapToValue(&h.query());
            if (!f->IsShared())
                goers.append(*LINK(f));
            else if (f->getLinkCount() == 2 && (stricmp(f->queryFilename(), currentTodoFile.str()) == 0) && deleteFiles)
            {
                // cannot be in the middle of copying a dll so don't worry about cleanUpOneTimeQueries
                // this file is in the middle of being copied, get rid of it...
                needToDeleteFile = true;
                goers.append(*LINK(f));

                DBGLOG("NAME = %s", f->queryFilename());
            }
        }

        ForEachItemInRev(idx1, goers)
        {
            ILazyFileIO *item = &goers.item(idx1);
            StringBuffer goer(item->queryFilename());

            RoxieFileType type = item->getFileType();

            bool okToDelete = true;  // assume ok to delete - all DLLS are always good to delete
            if (cleanUpOneTimeQueries)  // only want to delete dlls
            {
                if (type != ROXIE_WU_DLL)
                    continue;
            }
            
            if (!files.remove(goer))
                DBGLOG("ERROR - file was not removed from cache %s", goer.str());
            else if (deleteFiles || type == ROXIE_WU_DLL)  // always want to delete WU dlls
            {
                bool isRemote = item->isRemote();

                goers.remove(idx1);   // remove so we can delete the file if needed
                if ((!isRemote) && (type != ROXIE_PLUGIN_DLL))
                {
                    DBGLOG("trying to delete - file %s", goer.str());
                    try
                    {
                        OwnedIFile unneededFile = createIFile(goer.str());
                        unneededFile->remove();
                    }
                    catch (IException *E)
                    {
                        EXCLOG(MCoperatorError, E, "While trying to delete a file");
                        E->Release();
                    }
                }
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

ILazyFileIO *createDynamicFile(const char *id, IPartDescriptor *pdesc, RoxieFileType fileType, int numParts)
{
    unsigned short daliServixPort = getDaliServixPort();
    IPropertyTree &partProps = pdesc->queryProperties();
    offset_t dfsSize = partProps.getPropInt64("@size");
    unsigned crc;
    if (!pdesc->getCrc(crc))
        crc = 0;

    StringArray localLocations;
    StringArray remoteLocations;

    unsigned partNo = pdesc->queryPartIndex() + 1;
    StringBuffer localFileName;

    CDfsLogicalFileName dlfn;   
    dlfn.set(id);
    if (dlfn.isForeign())
        dlfn.clearForeign();

    const char *logicalname = dlfn.get();

    makePhysicalPartName(logicalname, partNo, numParts, localFileName, false, DFD_OSdefault, baseDataDirectory);  // MORE - if we get the dataDirectory we can pass it in and possibly` reuse an existing file

    unsigned numCopies = pdesc->numCopies();
    if (numCopies > 2)
        numCopies = 2;   // only care about maximum of 2 locations at this time
    for (unsigned copy = 0; copy < numCopies; copy++)
    {
        RemoteFilename r;
        pdesc->getFilename(copy,r);
        StringBuffer origName;
        r.getRemotePath(origName);
        remoteLocations.append(origName.str());
    }
    return queryFileCache().lookupFile(id, partNo, fileType, localFileName, NULL, NULL, localLocations, remoteLocations, dfsSize, NULL, false, true, false, false, crcResources ? crc : 0, pdesc->queryOwner().isCompressed(), NULL);
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
            throw MakeStringException(ROXIE_FILE_FAIL, "Internal error - requesting base for non-existant file part %d (valid are 1-%d)", part, numParts);
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
            throw MakeStringException(ROXIE_FILE_FAIL, "getFilePart requested invalid part %d", partNo);
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
    PointerArrayOf<X> cache;
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
    Owned<IDistributedFile> dFile; // NULL on copies serialized to slaves. Note that this implies we keep a lock on dali file for the lifetime of this object.
    CDateTime fileTimeStamp;
    RoxieFileType fileType;
    offset_t fileSize;

    StringArray subNames;
    PointerIArrayOf<IFileDescriptor> subFiles; // note - on slaves, the file descriptors may have incomplete info. On originating server is always complete
    PointerIArrayOf<IDefRecordMeta> diskMeta;
    Owned <IPropertyTree> properties;

    void addFile(const char *subName, IFileDescriptor *fdesc)
    {
        subNames.append(subName);
        subFiles.append(fdesc);
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
    CResolvedFile(const char *_lfn, IDistributedFile *_dFile, RoxieFileType _fileType) : lfn(_lfn), dFile(_dFile), fileType(_fileType)
    {
        cached = NULL;
        fileSize = 0;
        if (dFile)
        {
            if (traceLevel > 5)
                DBGLOG("Roxie server adding information for dynamic file %s", lfn.get());
            IDistributedSuperFile *superFile = dFile->querySuperFile();
            if (superFile)
            {
                unsigned numSubFiles = superFile->numSubFiles(true);
                Owned<IDistributedFileIterator> subs = superFile->getSubFileIterator(true);
                ForEach(*subs)
                {
                    IDistributedFile &sub = subs->query();
                    addFile(sub.queryLogicalName(), sub.getFileDescriptor());
                }
            }
            else // normal file, not superkey
                addFile(dFile->queryLogicalName(), dFile->getFileDescriptor());
            bool tsSet = dFile->getModificationTime(fileTimeStamp);
            assertex(tsSet); // per Nigel, is always set
            properties.set(&dFile->queryProperties());
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
    inline const char *queryLFN() const { return lfn; }
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
    virtual void serializePartial(MemoryBuffer &mb, unsigned channel, bool isLocal) const
    {
        if (traceLevel > 6)
            DBGLOG("Serializing file information for dynamic file %s, channel %d, local %d", lfn.get(), channel, isLocal);
        byte type = (byte) fileType;
        mb.append(type);
        fileTimeStamp.serialize(mb);
        mb.append(fileSize);
        unsigned numSubFiles = subFiles.length();
        mb.append(numSubFiles);
        ForEachItemIn(idx, subFiles)
        {
            mb.append(subNames.item(idx));
            IFileDescriptor *fdesc = subFiles.item(idx);
            // Find all the partno's that go to this channel
            unsigned numParts = fdesc->numParts();
            if (numParts > 1 && fileType==ROXIE_KEY && isLocal)
                numParts--; // don't want to send TLK
            offset_t base = 0;
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
        IFileDescriptor *fdesc = subFiles.item(0);
        if (fdesc)
        {
            unsigned numParts = fdesc->numParts();
            offset_t base = 0;
            for (unsigned i = 1; i <= numParts; i++)
            {
                if (!channel || getBondedChannel(i)==channel)
                {
                    try
                    {
                        IPartDescriptor *pdesc = fdesc->queryPart(i-1);
                        assertex(pdesc);
                        Owned<ILazyFileIO> file = createDynamicFile(subNames.item(0), pdesc, ROXIE_FILE, numParts);
                        IPropertyTree &partProps = pdesc->queryProperties();
                        f->addFile(LINK(file), partProps.getPropInt64("@offset"));
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
                        Owned <ILazyFileIO> part;
                        IFileDescriptor *fdesc = subFiles.item(idx);
                        unsigned crc = 0;
                        if (fdesc) // NB there may be no parts for this channel 
                        {
                            IPartDescriptor *pdesc = fdesc->queryPart(partNo-1);
                            if (pdesc)
                            {
                                part.setown(createDynamicFile(subNames.item(idx), pdesc, ROXIE_KEY, maxParts));
                                pdesc->getCrc(crc);
                            }
                        }
                        if (part)
                            keyset->addIndex(createKeyIndex(part->queryFilename(), crc, *part, false, false)); 
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
                Owned<IKeyIndexBase> key;
                if (fdesc)
                {
                    unsigned numParts = fdesc->numParts();
                    assertex(numParts > 0);
                    IPartDescriptor *pdesc = fdesc->queryPart(numParts - 1);
                    Owned<ILazyFileIO> keyFile = createDynamicFile(subNames.item(idx), pdesc, ROXIE_KEY, numParts);
                    unsigned crc;
                    pdesc->getCrc(crc);
                    StringBuffer pname;
                    pdesc->getPath(pname);
                    key.setown(createKeyIndex(pname.str(), crc, *keyFile, numParts>1, false));
                    keyset->addIndex(LINK(key->queryPart(0)));
                }
                else
                    keyset->addIndex(NULL);
            }
            if (keyset->numParts())
                ret->addKey(keyset.getClear());
            else if (!isOpt)
                throw MakeStringException(ROXIE_FILE_FAIL, "Key %s has no key parts", lfn.get());
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
            addFile(sub->subNames.item(idx), sub->subFiles.item(idx));
        }
    }
    virtual void addSubFile(IFileDescriptor *_sub)
    {
        addFile(lfn, _sub);
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
    virtual const IPropertyTree *queryProperties() const
    {
        return properties;
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
        : CResolvedFile(_lfn, NULL, ROXIE_FILE), channel(header->channel), serverIdx(header->serverIdx), isOpt(_isOpt), isLocal(_isLocal)
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
                serverData.read(fileSize);
                unsigned numSubFiles;
                serverData.read(numSubFiles);
                for (unsigned fileNo = 0; fileNo < numSubFiles; fileNo++)
                {
                    StringBuffer subName;
                    serverData.read(subName);
                    subNames.append(subName.str());
                    IArrayOf<IPartDescriptor> parts;
                    deserializePartFileDescriptors(serverData, parts);
                    if (parts.length())
                        subFiles.append(LINK(&parts.item(0).queryOwner()));
                    else
                    { 
                        if (traceLevel > 6)
                            DBGLOG("No information for subFile %d of file %s", fileNo, lfn.get());
                        subFiles.append(NULL);
                    }
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
};

extern IResolvedFileCreator *createResolvedFile(const char *lfn)
{
    return new CResolvedFile(lfn, NULL, ROXIE_FILE);
}

extern IResolvedFile *createResolvedFile(const char *lfn, IDistributedFile *dFile)
{
    const char *kind = dFile ? dFile->queryProperties().queryProp("@kind") : NULL;
    return new CResolvedFile(lfn, dFile, kind && stricmp(kind, "key")==0 ? ROXIE_KEY : ROXIE_FILE);
}

class CSlaveDynamicFileCache : public CInterface, implements ISlaveDynamicFileCache
{
    mutable CriticalSection crit;
    CIArrayOf<CSlaveDynamicFile> files; // expect numbers to be small - probably not worth hashing
    unsigned tableSize;

public:
    IMPLEMENT_IINTERFACE;
    CSlaveDynamicFileCache(unsigned _limit) : tableSize(_limit) {}

    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, RoxiePacketHeader *header, bool isOpt, bool isLocal)
    {
        if (logctx.queryTraceLevel() > 5)
        {
            StringBuffer s;
            logctx.CTXLOG("lookupDynamicFile %s for packet %s", lfn, header->toString(s).str());
        }
        // we use a fixed-size array with linear lookup for ease of initial coding - but unless we start making heavy use of the feaure this may be adequate.
        CriticalBlock b(crit);
        if (!cacheDate.isNull())
        {
            unsigned idx = 0;
            while (files.isItem(idx))
            {
                CSlaveDynamicFile &f = files.item(idx);
                if (f.channel==header->channel && f.serverIdx==header->serverIdx && stricmp(f.queryLFN(), lfn)==0)
                {
                    if (!cacheDate.equals(f.queryTimeStamp()))
                    {
                        if (f.isKey())
                            clearKeyStoreCacheEntry(f.queryLFN());
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
        fputs("Test", x);
        fclose(x);
        if (localPresent)
        {
            if (localCorrect)
                copyFile("cppfile_localfile1", "cppfile_localfile2");
            else
            {
                FILE *x = fopen("cppfile_localfile1", "wb");
                fputs("Pink1", x);
                fclose(x);
            }
        }
        Owned<CRoxieFileCache> cache = new CRoxieFileCache(true);
        StringArray remoteNames;
        StringArray peerNames;
        remoteNames.append("cppfile_localfile2");
        Owned<IFileIO> l1 = cache->lookupFile("cppfile_localfile1", 0, ROXIE_FILE, "cppfile_localfile1", NULL, NULL, peerNames, remoteNames, 4, NULL, false, false, true, false, 0, false, NULL);
        Owned<IFileIO> l2 = cache->lookupFile("cppfile_localfile1", 0, ROXIE_FILE, "cppfile_localfile1", NULL, NULL, peerNames, remoteNames, 4, NULL, false, false, true, false, 0, false, NULL);
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

