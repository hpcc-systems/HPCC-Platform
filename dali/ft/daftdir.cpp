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

#include "jliball.hpp"

#include "rmtspawn.hpp"

#include "fterror.hpp"
#include "daft.hpp"
#include "dadfs.hpp"
#include "daftcfg.hpp"
#include "daftdir.ipp"
#include "daftmc.hpp"
#include "dalienv.hpp"

#ifdef __linux__
#include <glob.h>
#endif

#ifdef _WIN32
#define DEFAULT_DRIVE       "c:"                    // What about solaris machines.
#else
#define DEFAULT_DRIVE       ""
#endif

#define DEFAULT_ROOT_PATH   PATHSEPSTR              // Should this be the thor output directory
#define WILDCARD_ALL        "*"

//Use hash defines for properties so I can't mis-spell them....
#define ANcrc               "@crc"
#define ANtime              "@time"
#define ANrecurse           "@recurse"

//---------------------------------------------------------------------------

#ifdef _WIN32
void FILETIMEtoIDateTime(CDateTime * target, const FILETIME & ft)
{
    if (target)
    {
        SYSTEMTIME systime;
        FileTimeToSystemTime(&ft, &systime);
        target->setDate(systime.wYear, systime.wMonth, systime.wDay);
        target->setTime(systime.wHour, systime.wMinute, systime.wSecond, systime.wMilliseconds*1000000);
    }
}

void setTimestamp(IPropertyTree * entry, const char * attr, const FILETIME & ft)
{
    CDateTime time;
    StringBuffer timeText;

    FILETIMEtoIDateTime(&time, ft);
    time.getString(timeText.clear());
    entry->setProp(attr, timeText.str());
}

#endif

//---------------------------------------------------------------------------

DirectoryBuilder::DirectoryBuilder(ISocket * _masterSocket, IPropertyTree * options)
{
    masterSocket = _masterSocket;
    calcCRC = false;
    recurse = false;
    addTimes = true;
    if (options)
    {
        addTimes = options->getPropBool(ANtime, addTimes);
        calcCRC = options->getPropBool(ANcrc);
        recurse = options->getPropBool(ANrecurse);
    }
    includeEmptyDirectory = false;
}


void DirectoryBuilder::rootDirectory(const char * directory, INode * node, IPropertyTree * result)
{
    OwnedIFile dir = createIFile(directory);
    StringBuffer path;
    const char * tag = "directory";
    if (dir->isDirectory() == fileBool::foundYes)
    {
        implicitWildcard = true;
        includeEmptyDirectory = true;

        path.append(directory);
        wildcard.set(WILDCARD_ALL);
    }
    else
    {
        implicitWildcard = false;
        includeEmptyDirectory = false;

        StringBuffer wild;
        StringBuffer drive;
        splitFilename(directory, &drive, &path, &wild, &wild);
        wildcard.set(wild.str());
        if (!drive.length())
            drive.append(DEFAULT_DRIVE);
        if (!path.length())
            path.append(DEFAULT_ROOT_PATH);
        path.insert(0, drive.str());
    }

    IPropertyTree * dirTree = result->addPropTree(tag, createPTree(ipt_caseInsensitive));
    dirTree->setProp("@name", path.str());

    if (addTimes)
    {
#ifdef _WIN32
        WIN32_FILE_ATTRIBUTE_DATA info;
        if (GetFileAttributesEx(path.str(), GetFileExInfoStandard, &info))
        {
            setTimestamp(dirTree, "@created", info.ftCreationTime);
            setTimestamp(dirTree, "@modified", info.ftLastWriteTime);
            setTimestamp(dirTree, "@accessed", info.ftLastAccessTime);
        }
#else
        OwnedIFile file = createIFile(path.str());
        if(file->exists())
        {
            CDateTime ctime, mtime, atime;
            file->getTime(&ctime, &mtime, &atime);
            StringBuffer ctimestr, mtimestr, atimestr;
            ctime.getString(ctimestr);
            mtime.getString(mtimestr);
            atime.getString(atimestr);
            dirTree->setProp("@created", ctimestr.str());
            dirTree->setProp("@modified", mtimestr.str());
            dirTree->setProp("@accessed", atimestr.str());
        }
#endif
    }
    walkDirectory("", dirTree);
}


bool DirectoryBuilder::walkDirectory(const char * path, IPropertyTree * directory)
{
    StringBuffer fullname, search;
    directory->getProp("@name", fullname.append(path));
    if (fullname.length() && fullname.charAt(fullname.length()-1) != PATHSEPCHAR)
        fullname.append(PATHSEPCHAR);
    search.append(fullname).append(wildcard);

    IArray pending;
    bool empty = true;

    checkForRemoteAbort(masterSocket);

#ifdef _WIN32
    WIN32_FIND_DATA info;
    HANDLE handle = FindFirstFile(search.str(), &info);
    if (handle != INVALID_HANDLE_VALUE)
    {
        do
        {
            if (strcmp(info.cFileName, ".") == 0 || strcmp(info.cFileName, "..") == 0)
                continue;

            const char * tag = (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ? "directory" : "file";
            IPropertyTree * entry = NULL;
            if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            {
                if (implicitWildcard && !recurse)
                    entry = directory->addPropTree(tag, createPTree(ipt_caseInsensitive));
            }
            else
            {
                entry = directory->addPropTree(tag, createPTree(ipt_caseInsensitive));

                entry->setPropInt64("@size", ((unsigned __int64)info.nFileSizeHigh) << 32 | info.nFileSizeLow);
                if (calcCRC)
                {
                    StringBuffer filename;
                    filename.append(fullname).append(info.cFileName);
                    try
                    {
                        OwnedIFile file = createIFile(filename.str());
                        OwnedIFileIO io = file->open(IFOread);
                        if (io)
                        {
                            OwnedIFileIOStream stream = createIOStream(io);
                            CrcIOStream crcstream(stream, ~0);
                            char buffer[32768];
                            while (crcstream.read(sizeof(buffer), buffer))
                            { }

                            entry->setPropInt("@crc", ~crcstream.getCRC());
                        }
                    }
                    catch (IException * e)
                    {
                        FLLOG(MCexception(e)(1000), unknownJob, e, "Trying to calculate CRC");
                        e->Release();
                    }
                }
            }
            if (entry)
            {
                entry->setProp("@name", info.cFileName);
                if (addTimes)
                {
                    setTimestamp(entry, "@created", info.ftCreationTime);
                    setTimestamp(entry, "@modified", info.ftLastWriteTime);
                    setTimestamp(entry, "@accessed", info.ftLastAccessTime);
                }
                empty = false;
            }
        } while (FindNextFile(handle, &info));

        FindClose(handle);
    }

    if (recurse)
    {
        search.clear().append(fullname).append(WILDCARD_ALL);
        HANDLE handle = FindFirstFile(search.str(), &info);
        if (handle != INVALID_HANDLE_VALUE)
        {
            StringBuffer prev;
            do
            {
                if (strcmp(info.cFileName, ".") == 0 || strcmp(info.cFileName, "..") == 0)
                    continue;

                if (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
                {
                    prev.clear().append("directory[@name=\"").append(info.cFileName).append("\"]");
                    if (directory->hasProp(prev.str()))
                        continue;

                    IPropertyTree * entry = directory->addPropTree("directory", createPTree(ipt_caseInsensitive));
                    entry->setProp("@name", info.cFileName);
                    pending.append(*LINK(entry));

                    if (addTimes)
                    {
                        setTimestamp(entry, "@created", info.ftCreationTime);
                        setTimestamp(entry, "@modified", info.ftLastWriteTime);
                        setTimestamp(entry, "@accessed", info.ftLastAccessTime);
                    }
                }
            } while (FindNextFile(handle, &info));

            FindClose(handle);
        }
    }

    ForEachItemIn(idx, pending)
    {
        IPropertyTree & cur = (IPropertyTree &)pending.item(idx);
        if (walkDirectory(fullname.str(), &cur))
            empty = false;
        else if (!includeEmptyDirectory)
            directory->removeTree(&cur);
    }
#elif defined(__linux__)
    glob_t glbuf;
    int glret = glob(search.str(), 0, NULL, &glbuf);

    if(glret != 0)
    {
        switch (glret)
        {
        case GLOB_NOSPACE:
            IERRLOG("glob error for %s : running out of memory space", search.str());
            break;
        case GLOB_ABORTED:
            IERRLOG("glob error for %s : read error", search.str());
            break;
        case GLOB_NOMATCH:
            IERRLOG("no match found for %s", search.str());
            break;
        default:
            IERRLOG("glob error for %s : %s", search.str(), strerror(errno));
        }
    }
    else
    {
        for(int fno = 0; fno < glbuf.gl_pathc; fno++)
        {
            const char* curfname = glbuf.gl_pathv[fno];
            if (strcmp(curfname, ".") == 0 || strcmp(curfname, "..") == 0)
                continue;

            OwnedIFile file = createIFile(curfname);
            const char * tag = (file->isDirectory()==fileBool::foundYes) ? "directory" : "file";

            IPropertyTree * entry = NULL;
            if (file->isDirectory()==fileBool::foundYes)
            {
                if (implicitWildcard && !recurse)
                    entry = directory->addPropTree(tag, createPTree(ipt_caseInsensitive));
                else if(recurse)
                {
                    StringBuffer prev;
                    prev.append("directory[@name=\"").append(curfname).append("\"]");
                    if (directory->hasProp(prev.str()))
                        continue;

                    entry = directory->addPropTree("directory", createPTree(ipt_caseInsensitive));
                    pending.append(*LINK(entry));
                }
            }
            else
            {
                entry = directory->addPropTree(tag, createPTree(ipt_caseInsensitive));

                entry->setPropInt64("@size", file->size());
                if (calcCRC)
                {
                    try
                    {
                        OwnedIFileIO io = file->open(IFOread);
                        if (io)
                        {
                            OwnedIFileIOStream stream = createIOStream(io);
                            CrcIOStream crcstream(stream, ~0);
                            char buffer[32768];
                            while (crcstream.read(sizeof(buffer), buffer))
                            { }

                            entry->setPropInt("@crc", ~crcstream.getCRC());
                        }
                    }
                    catch (IException * e)
                    {
                        FLLOG(MCexception(e)(1000), unknownJob, e, "Trying to calculate CRC");
                        e->Release();
                    }
                }
            }
            if (entry)
            {
                entry->setProp("@name", curfname);
                if (addTimes)
                {
                    CDateTime ctime, mtime, atime;
                    file->getTime(&ctime, &mtime, &atime);
                    StringBuffer ctimestr, mtimestr, atimestr;
                    ctime.getString(ctimestr);
                    mtime.getString(mtimestr);
                    atime.getString(atimestr);
                    entry->setProp("@created", ctimestr.str());
                    entry->setProp("@modified", mtimestr.str());
                    entry->setProp("@accessed", atimestr.str());
                }
                empty = false;
            }
        }
    }

    ForEachItemIn(idx, pending)
    {
        IPropertyTree & cur = (IPropertyTree &)pending.item(idx);
        if (walkDirectory("", &cur))
            empty = false;
        else if (!includeEmptyDirectory)
            directory->removeTree(&cur);
    }
#endif
    return !empty;
}

//---------------------------------------------------------------------------

bool processDirCommand(ISocket * masterSocket, MemoryBuffer & cmd, MemoryBuffer & result)
{
    StringAttr directory; cmd.read(directory);
    Owned<IPropertyTree> options = createPTree(cmd);
    Owned<INode> node = deserializeINode(cmd);

    DirectoryBuilder builder(masterSocket, options);
    StringBuffer url;
    
    Owned<IPropertyTree> dirTree = createPTree("machine", ipt_caseInsensitive);
    node->endpoint().getIpText(url.clear());
    dirTree->setProp("@ip", url.str());

    StringAttr nextDir;
    const char * cur = directory;
    for (;;)
    {
        const char * sep = strchr(cur, ';');
        if (sep)
            nextDir.set(cur, sep-cur);
        else
            nextDir.set(cur);
        LOG(MCdebugProgress, unknownJob, "Process Directory Command: %s", nextDir.get());
        builder.rootDirectory(nextDir, node, dirTree);
    
        if (!sep)
            break;

        cur = sep+1;        
    }

    result.clear();
    dirTree->serialize(result);
    return true;
}


//---------------------------------------------------------------------------

DirectoryThread::DirectoryThread(IRunningSlaveObserver & _observer, const char * _directory, INode * _node, IPropertyTree * _options) : Thread("directoryThread"), observer(_observer)
{
    directory = _directory;
    node.set(_node);
    options.set(_options);
    sem = NULL;
    ok = false;
    job = unknownJob;
}

void DirectoryThread::go(Semaphore & _sem)
{
    sem = &_sem;
#ifdef RUN_SLAVES_ON_THREADS
    start();
#else
    commandAndSignal();
#endif
}

bool DirectoryThread::performCommand()
{
    bool ok = false;
    StringBuffer url;
    node->endpoint().getUrlStr(url);

    if (!canSpawnChildProcess(node->endpoint()))
        throwError(DFTERR_NoSolarisDir);

    LOG(MCdebugProgressDetail, job, "Starting to generate part %s [%p]", url.str(), this);
    StringBuffer tmp;
    Owned<ISocket> socket = spawnRemoteChild(SPAWNdfu, queryFtSlaveExecutable(node->endpoint(), tmp), node->endpoint(), DAFT_VERSION, queryFtSlaveLogDir(), NULL, NULL);
    if (socket)
    {
        observer.addSlave(socket);

        MemoryBuffer msg;
        msg.setEndian(__BIG_ENDIAN);

        //Send message and wait for response... 
        //MORE: they should probably all be sent on different threads....
        msg.append((byte)FTactiondirectory);
        msg.append(directory);
        options->serialize(msg);
        node->serialize(msg);

        if (!catchWriteBuffer(socket, msg))
            throwError1(RFSERR_TimeoutWaitConnect, url.str());

        bool done;
        for (;;)
        {
            msg.clear();
            if (!catchReadBuffer(socket, msg, FTTIME_DIRECTORY))
                throwError1(RFSERR_TimeoutWaitSlave, url.str());

            msg.setEndian(__BIG_ENDIAN);
            msg.read(done);
            if (done)
                break;

            assertex(!"Progress not supported yet...");

            if (isAborting())
            {
                msg.clear().append(isAborting());
                if (!catchWriteBuffer(socket, msg))
                    throwError1(RFSERR_TimeoutWaitSlave, url.str());
            }
        }
        msg.read(ok);
        error.setown(deserializeException(msg));
        if (ok)
            resultTree.setown(createPTree(msg));

        msg.clear().append(true);
        catchWriteBuffer(socket, msg);          // if it fails then can't do anything about it...
        observer.removeSlave(socket);
    }
    else
    {
        throwError1(DFTERR_FailedStartSlave, url.str());
    }
    LOG(MCdebugProgressDetail, job, "Completed generating part %s [%p]", url.str(), this);

    return ok;
}


bool DirectoryThread::commandAndSignal()
{
    ok = false;
    try
    {
        ok = performCommand();
    }
    catch (IException * e)
    {
        PrintExceptionLog(e, "Gathering directory");
        error.setown(e);
    }
    sem->signal();
    return ok;
}


int DirectoryThread::run()
{
    commandAndSignal();
    return 0;
}

//---------------------------------------------------------------------------

#if 0
void doDirectoryCommand(const char * directory, IGroup * machines, IPropertyTree * options, IPropertyTree * result)
{
    DirectoryBuilder builder(options);

    StringBuffer url;
    unsigned max = machines->ordinality();
    for (unsigned idx=0; idx < max; idx++)
    {
        INode & node = machines->queryNode(idx);
        node.endpoint().getIpText(url.clear());
        IPropertyTree * machine = createPTree("machine", ipt_caseInsensitive);
        machine->setProp("@ip", url.str());
        result->addPropTree("machine", machine);
        builder.rootDirectory(directory, &node, machine);
    }
}
#endif

class BroadcastAbortHandler : public CInterface, implements IAbortHandler, implements IRunningSlaveObserver
{
public:
    IMPLEMENT_IINTERFACE

    void addSlave(ISocket * node);
    void removeSlave(ISocket * node);

    void abort();
    virtual bool onAbort();

protected:
    IArrayOf<ISocket>   sockets;
    CriticalSection     crit;
};


void BroadcastAbortHandler::abort()
{
    CriticalBlock proceduce(crit);

    //MORE: Implement mode efficiently;
    ForEachItemIn(i, sockets)
    {
        MemoryBuffer msg;
        msg.append(true);
        catchWriteBuffer(&sockets.item(i), msg);    // async?
    }
}

bool BroadcastAbortHandler::onAbort()
{
    if (isAborting())
        abort();
    return false;
}

void BroadcastAbortHandler::addSlave(ISocket * socket)      
{ 
    CriticalBlock procedure(crit);
    sockets.append(*LINK(socket)); 
}

void BroadcastAbortHandler::removeSlave(ISocket * socket)   
{ 
    CriticalBlock procedure(crit);
    sockets.zap(*socket); 
}

void doDirectory(const char * directory, IGroup * machines, IPropertyTree * options, IPropertyTree * result)
{
    LocalAbortHandler localHandler(daftAbortHandler);
    BroadcastAbortHandler broadcaster;
    LocalIAbortHandler localHandler2(broadcaster);

    StringBuffer url;
    CIArrayOf<DirectoryThread> threads;
    unsigned max = machines->ordinality();
    unsigned idx;
    for (idx=0; idx < max; idx++)
    {
        INode & node = machines->queryNode(idx);
        DirectoryThread & cur = * new DirectoryThread(broadcaster, directory, &node, options);
        threads.append(cur);
    }

    Semaphore sem;
    for (idx=0; idx < max; idx++)
        threads.item(idx).go(sem);

    for (idx=0; idx < max; idx++)
        sem.wait();

    for (idx=0; idx < max; idx++)
    {
        DirectoryThread & cur = threads.item(idx);

        if (cur.error)
            throw cur.error.getLink();
        result->addPropTree("machine", threads.item(idx).getTree());
    }
}



//---------------------------------------------------------------------------

DirectoryCopier::DirectoryCopier(ISocket * _masterSocket, MemoryBuffer & in)
{
    masterSocket = _masterSocket;
    source.setown(createPTree(in));
    target.deserialize(in);
    options.setown(createPTree(in));

    initOptions();
}


DirectoryCopier::DirectoryCopier(ISocket * _masterSocket, IPropertyTree * _source, RemoteFilename & _target, IPropertyTree * _options)
{
    masterSocket = _masterSocket;
    source.set(_source);
    target.set(_target);
    options.set(_options);

    initOptions();
}


void DirectoryCopier::initOptions()
{
    onlyCopyMissing = options->getPropBool("@copyMissing", false);
    onlyCopyExisting = options->getPropBool("@copyExisting", false);
    preserveTimes = options->getPropBool("@preserveTimes", false);
    preserveIfNewer = options->getPropBool("@preserveIfNewer", false);
    verbose = options->getPropBool("@verbose", false);
}


void DirectoryCopier::copy()
{
    IPropertyTree * machine = source->queryPropTree("machine");
    IPropertyTree * rootDirectory = machine->queryPropTree("directory");

    RemoteFilename sourceName;
    StringBuffer sourcePath;
    StringBuffer targetPath;
    SocketEndpoint ip(machine->queryProp("@ip"));

    sourceName.setPath(ip, rootDirectory->queryProp("@name"));
    sourceName.getRemotePath(sourcePath);
    target.getLocalPath(targetPath);

    recursiveCopy(rootDirectory, sourcePath.str(), targetPath.str());
}

void DirectoryCopier::recursiveCopy(IPropertyTree * level, const char * sourcePath, const char * targetPath)
{
    if (masterSocket)
        checkForRemoteAbort(masterSocket);

    Owned<IFile> dir = createIFile(targetPath);
    dir->createDirectory();

    StringBuffer source, target;
    Owned<IPropertyTreeIterator> iter = level->getElements("file");
    ForEach(*iter)
    {
        const char * filename = iter->query().queryProp("@name");
        source.clear().append(sourcePath).append(PATHSEPCHAR).append(filename);
        target.clear().append(targetPath).append(PATHSEPCHAR).append(filename);

        bool doCopy = true;
        OwnedIFile sourceFile = createIFile(source.str());
        OwnedIFile targetFile = createIFile(target.str());
        if (onlyCopyExisting || onlyCopyMissing)
        {
            fileBool exists = targetFile->isFile();
            if (onlyCopyExisting && (exists != fileBool::foundYes))
                doCopy = false;
            if (onlyCopyMissing && (exists != fileBool::notFound))
                doCopy = false;
        }
        if (doCopy && preserveIfNewer)
        {
            if (targetFile->isFile() == fileBool::foundYes)
            {
                CDateTime modifiedSource, modifiedTarget;
                sourceFile->getTime(NULL, &modifiedSource, NULL);
                targetFile->getTime(NULL, &modifiedTarget, NULL);
                if (modifiedSource.compare(modifiedTarget) <= 0)
                    doCopy = false;
            }
        }
        if (doCopy)
        {
            if (verbose)
            {
                MemoryBuffer msg;
                msg.setEndian(__BIG_ENDIAN);
                msg.append(false);
                msg.append(source.str());
                writeBuffer(masterSocket, msg);
            }

            copyFile(targetFile, sourceFile);

            if (preserveTimes)
            {
                CDateTime created, modified, accessed;
                sourceFile->getTime(&created, &modified, &accessed);
                targetFile->setTime(&created, &modified, &accessed);
            }
        }
    }

    iter.setown(level->getElements("directory"));
    ForEach(*iter)
    {
        IPropertyTree * directory = &iter->query();
        const char * filename = directory->queryProp("@name");
        source.clear().append(sourcePath).append(PATHSEPCHAR).append(filename);
        target.clear().append(targetPath).append(PATHSEPCHAR).append(filename);
        recursiveCopy(directory, source.str(), target.str());
    }
}

//---------------------------------------------------------------------------

bool processPhysicalCopyCommand(ISocket * masterSocket, MemoryBuffer & cmd, MemoryBuffer & result)
{
    LOG(MCdebugProgress, unknownJob, "Process Physical Copy Command");
    DirectoryCopier copier(masterSocket, cmd);
    copier.copy();
    result.clear();
    return true;
}

//---------------------------------------------------------------------------


void doPhysicalCopy(IPropertyTree * source, const char * target, IPropertyTree * _options, IDaftCopyProgress * progress)
{
    LocalAbortHandler localHandler(daftAbortHandler);
    BroadcastAbortHandler broadcaster;
    LocalIAbortHandler localHandler2(broadcaster);

    Linked<IPropertyTree> options = _options;
    if (!options)
        options.setown(createPTree("options", ipt_caseInsensitive));

    if (progress)
        options->setPropBool("@verbose", true);

#if 0
    //Enable for debugging locally
    RemoteFilename xtargetName;
    xtargetName.setRemotePath(target);
    DirectoryCopier copier(NULL, source, xtargetName, options);
    copier.copy();
    return;
#endif

    SocketEndpoint sourceMachine(source->queryProp("machine/@ip"));
    RemoteFilename targetName;
    Owned<IException> error;

    targetName.setRemotePath(target);
    const IpAddress & targetIP = targetName.queryIP();
    if (!canSpawnChildProcess(targetIP))
        throwError(DFTERR_NoSolarisCopy);

    bool ok = false;
    StringBuffer url;
    targetIP.getIpText(url);

    LOG(MCdebugProgressDetail, unknownJob, "Starting to generate part %s", url.str());
    StringBuffer tmp;
    Owned<ISocket> socket = spawnRemoteChild(SPAWNdfu, queryFtSlaveExecutable(targetIP, tmp), targetName.queryEndpoint(), DAFT_VERSION, queryFtSlaveLogDir(), NULL);
    if (socket)
    {
        broadcaster.addSlave(socket);

        MemoryBuffer msg;
        msg.setEndian(__BIG_ENDIAN);

        //Send message and wait for response... 
        //MORE: they should probably all be sent on different threads....
        msg.append((byte)FTactionpcopy);
        source->serialize(msg);
        targetName.serialize(msg);
        options->serialize(msg);

        if (!catchWriteBuffer(socket, msg))
            throwError1(RFSERR_TimeoutWaitConnect, url.str());

        bool done;
        for (;;)
        {
            msg.clear();
            if (!catchReadBuffer(socket, msg, FTTIME_DIRECTORY))
                throwError1(RFSERR_TimeoutWaitSlave, url.str());

            msg.setEndian(__BIG_ENDIAN);
            msg.read(done);
            if (done)
                break;

            StringAttr displayText;
            msg.read(displayText);
            if (progress)
                progress->onProgress(displayText);
            else
                LOG(MCoperatorProgress, unknownJob, "Copy file %s", displayText.get());

            if (isAborting())
            {
                msg.clear().append(isAborting());
                if (!catchWriteBuffer(socket, msg))
                    throwError1(RFSERR_TimeoutWaitSlave, url.str());
            }
        }
        msg.read(ok);
        error.setown(deserializeException(msg));

        msg.clear().append(true);
        catchWriteBuffer(socket, msg);
        broadcaster.removeSlave(socket);
    }
    else
    {
        throwError1(DFTERR_FailedStartSlave, url.str());
    }
    LOG(MCdebugProgressDetail, unknownJob, "Completed generating part %s", url.str());

    if (error)
        throw error.getClear();
}
