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

#include "platform.h"
#include <algorithm>
#include "jlib.hpp"
#include "jio.hpp"
#include <math.h>

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jdebug.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "rmtspawn.hpp"
#include "filecopy.ipp"
#include "jptree.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "fterror.hpp"
#include "daftformat.hpp"
#include "daftmc.hpp"
#include "dasds.hpp"
#include "jlog.hpp"
#include "dalienv.hpp"
#include "ftbase.ipp"

#define DEFAULT_MAX_CONNECTIONS 25
#define PARTITION_RECOVERY_LIMIT 1000
#define EXPECTED_RESPONSE_TIME          (60 * 1000)
#define RESPONSE_TIME_TIMEOUT           (60 * 60 * 1000)
#define DEFAULT_MAX_XML_RECORD_SIZE 0x100000

//#define CLEANUP_RECOVERY

//Use hash defines for properties so I can't mis-spell them....
#define ANcomplete          "@complete"
#define ANcompress          "@compress"
#define ANcrc               "@crc"
#define ANcrcCheck          "@crcCheck"
#define ANcrcDiffers        "@crcDiffers"
#define ANdone              "@done"
#define ANhasPartition      "@hasPartition"
#define ANhasProgress       "@hasProgress"
#define ANhasRecovery       "@hasRecovery"
#define ANmaxConnections    "@maxConnections"
#define ANnocommon          "@noCommon"
#define ANnoRecover         "@noRecover"
#define ANnosplit           "@nosplit"
#define ANnosplit2          "@noSplit"
#define ANprefix            "@prefix"
#define ANpull              "@pull"
#define ANpush              "@push"
#define ANsafe              "@safe"
#define ANsizedate          "@sizedate"
#define ANsplit             "@split"
#define ANsplitPrefix       "@splitPrefix"
#define ANthrottle          "@throttle"
#define ANverify            "@verify"
#define ANtransferBufferSize "@transferBufferSize"
#define ANencryptKey        "@encryptKey"
#define ANdecryptKey        "@decryptKey"

#define PNpartition         "partition"
#define PNprogress          "progress"

//File attributes
#define FArecordSize        "@recordSize"
#define FArecordCount       "@recordCount"
#define FAformat            "@format"
#define FAcrc               "@fileCrc"
#define FAsize              "@size"
#define FAcompressedSize    "@compressedSize"


const unsigned operatorUpdateFrequency = 5000;      // time between updates in ms
const unsigned abortCheckFrequency = 20000;         // time between updates in ms
const unsigned sdsUpdateFrequency = 20000;          // time between updates in ms
const unsigned maxSlaveUpdateFrequency = 1000;      // time between updates in ms - small number of nodes.
const unsigned minSlaveUpdateFrequency = 5000;      // time between updates in ms - large number of nodes.


bool TargetLocation::canPull() 
{ 
    return queryOS(filename.queryIP()) != MachineOsSolaris;
}

//----------------------------------------------------------------------------

FilePartInfo::FilePartInfo(const RemoteFilename & _filename) 
{ 
    filename.set(_filename);
    init();
}

FilePartInfo::FilePartInfo()
{
    init();
}

bool FilePartInfo::canPush()            
{ 
    return queryOS(filename.queryIP()) != MachineOsSolaris;
}


void FilePartInfo::extractExtra(IPartDescriptor &part)
{
    unsigned _crc;
    hasCRC = part.getCrc(_crc);
    if (hasCRC)
        crc = _crc;

    properties.set(&part.queryProperties());
    if (part.queryProperties().hasProp("@modified"))
        modifiedTime.setString(part.queryProperties().queryProp("@modified"));
}

void FilePartInfo::extractExtra(IDistributedFilePart &part)
{
    unsigned _crc;
    hasCRC = part.getCrc(_crc);
    if (hasCRC)
        crc = _crc;

    properties.set(&part.queryAttributes());
    if (part.queryAttributes().hasProp("@modified"))
        modifiedTime.setString(part.queryAttributes().queryProp("@modified"));
}

void FilePartInfo::init()
{
    offset = 0; 
    size = UNKNOWN_PART_SIZE; 
    psize = UNKNOWN_PART_SIZE; 
    headerSize = 0; 
    hasCRC = false;
    xmlHeaderLength = 0;
    xmlFooterLength = 0;
}

//----------------------------------------------------------------------------

void shuffle(CIArray & array)
{
    //Use our own seeded random number generator, so that multiple dfu at the same time are less likely to clash.
    Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
    random->seed(123456789);
    unsigned i = array.ordinality();
    while (i > 1)
    {
        unsigned j = random->next() % i;
        i--;
        array.swap(i, j);
    }
}

//----------------------------------------------------------------------------

FileTransferThread::FileTransferThread(FileSprayer & _sprayer, byte _action, const SocketEndpoint & _ep, bool _calcCRC, const char *_wuid) 
: Thread("pullThread"), sprayer(_sprayer), wuid(_wuid)
{
    calcCRC = _calcCRC;
    action = _action;
    ep.set(_ep);
//  progressInfo = _progressInfo;
    sem = NULL;
    ok = false;
    job = unknownJob;
    allDone = false;
    started = false;
}

void FileTransferThread::addPartition(PartitionPoint & nextPartition, OutputProgress & nextProgress)    
{ 
    partition.append(OLINK(nextPartition)); 
    progress.append(OLINK(nextProgress));
    passwordProvider.addPasswordForIp(nextPartition.inputName.queryIP());
    passwordProvider.addPasswordForIp(nextPartition.outputName.queryIP());
}

unsigned __int64 FileTransferThread::getInputSize()
{
    unsigned __int64 inputSize = 0;
    ForEachItemIn(idx, partition)
        inputSize += partition.item(idx).inputLength;
    return inputSize;
}

void FileTransferThread::go(Semaphore & _sem)
{
    sem = &_sem;
    if (partition.empty())
        transferAndSignal();            // do nothing, but don't start a new thread
    else
    {
#ifdef RUN_SLAVES_ON_THREADS
        start();
#else
        transferAndSignal();
#endif
    }
}

bool FileTransferThread::isAborting()
{
    return sprayer.isAborting() || ::isAborting();
}

void FileTransferThread::logIfRunning(StringBuffer &list)
{
    if (started && !allDone && !error)
    {
        StringBuffer url;
        ep.getUrlStr(url);
        LOG(MCwarning, unknownJob, "Still waiting for slave %s", url.str());
        if (list.length())
            list.append(',');
        list.append(url);
    }
}

bool FileTransferThread::catchReadBuffer(ISocket * socket, MemoryBuffer & msg, unsigned timeout)
{
    unsigned nowTime = msTick();

    unsigned abortCheckTimeout = 120*1000;
    loop
    {
        try
        {
            readBuffer(socket, msg, abortCheckTimeout);
            return true;
        }
        catch (IException * e)
        {
            switch (e->errorCode())
            {
            case JSOCKERR_graceful_close:
                break;
            case JSOCKERR_timeout_expired:
                if (isAborting())
                    break;
                if (msTick() - nowTime < timeout) 
                {
                    e->Release();
                    continue;
                }
                break;
            default:
                EXCLOG(e,"FileTransferThread::catchReadBuffer");
                break;
            }
            e->Release();
            return false;
        }
    }
}
    

bool FileTransferThread::performTransfer()
{
    bool ok = false;
    StringBuffer url;
    ep.getUrlStr(url);

    LOG(MCdebugProgress, job, "Transferring part %s [%p]", url.str(), this);
    started = true;
    allDone = true;
    if (sprayer.isSafeMode || action == FTactionpush)
    {
        ForEachItemIn(i, progress)
        {
            if (progress.item(i).status != OutputProgress::StatusCopied)
                allDone = false;
        }
    }
    else
    {
        unsigned whichOutput = (unsigned)-1;
        ForEachItemIn(i, progress)
        {
            PartitionPoint & curPartition = partition.item(i);
            OutputProgress & curProgress = progress.item(i);
            //pull should rename as well as copy the files.
            if (curPartition.whichOutput != whichOutput)
            {
                if (curProgress.status != OutputProgress::StatusRenamed)
                    allDone = false;
                whichOutput = curPartition.whichOutput;
            }
        }
    }

    if (allDone)
    {
        LOG(MCdebugInfo, job, "Creation of part %s already completed", url.str());
        return true;
    }

    if (partition.empty())
    {
        LOG(MCdebugInfo, job, "No elements to transfer for this slave");
        return true;
    }

    LOG(MCdebugProgressDetail, job, "Start generate part %s [%p]", url.str(), this);
    StringBuffer tmp;
    Owned<ISocket> socket = spawnRemoteChild(SPAWNdfu, sprayer.querySlaveExecutable(ep, tmp), ep, DAFT_VERSION, queryFtSlaveLogDir(), this, wuid);
    if (socket)
    {
        MemoryBuffer msg;
        msg.setEndian(__BIG_ENDIAN);

        //MORE: Allow this to be configured by an option.
        unsigned slaveUpdateFrequency = minSlaveUpdateFrequency;
        if (sprayer.numParallelSlaves() < 5)
            slaveUpdateFrequency = maxSlaveUpdateFrequency;

        //Send message and wait for response... 
        msg.append(action);
        passwordProvider.serialize(msg);
        ep.serialize(msg);
        sprayer.srcFormat.serialize(msg);
        sprayer.tgtFormat.serialize(msg);
        msg.append(sprayer.calcInputCRC());
        msg.append(calcCRC);
        serialize(partition, msg);
        msg.append(sprayer.numParallelSlaves());
        msg.append(slaveUpdateFrequency);
        msg.append(sprayer.replicate);
        msg.append(sprayer.mirroring);
        msg.append(sprayer.isSafeMode);

        msg.append(progress.ordinality());
        ForEachItemIn(i, progress)
            progress.item(i).serializeCore(msg);

        msg.append(sprayer.throttleNicSpeed);
        msg.append(sprayer.compressedInput);
        msg.append(sprayer.compressOutput);
        msg.append(sprayer.copyCompressed);
        msg.append(sprayer.transferBufferSize);
        msg.append(sprayer.encryptKey);
        msg.append(sprayer.decryptKey);

        sprayer.srcFormat.serializeExtra(msg, 1);
        sprayer.tgtFormat.serializeExtra(msg, 1);

        ForEachItemIn(i2, progress)
            progress.item(i2).serializeExtra(msg, 1);

        //NB: Any extra data must be appended at the end...
        if (!catchWriteBuffer(socket, msg))
            throwError1(RFSERR_TimeoutWaitConnect, url.str());

        bool done;
        loop
        {
            msg.clear();
            if (!catchReadBuffer(socket, msg, FTTIME_PROGRESS))
                throwError1(RFSERR_TimeoutWaitSlave, url.str());

            msg.setEndian(__BIG_ENDIAN);
            msg.read(done);
            if (done)
                break;

            OutputProgress newProgress;
            newProgress.deserializeCore(msg);
            newProgress.deserializeExtra(msg, 1);
            sprayer.updateProgress(newProgress);

            LOG(MCdebugProgress(10000), job, "Update %s: %d %" I64F "d->%" I64F "d", url.str(), newProgress.whichPartition, newProgress.inputLength, newProgress.outputLength);

            if (isAborting())
            {
                if (!sendRemoteAbort(socket))
                    throwError1(RFSERR_TimeoutWaitSlave, url.str());
            }
        }
        msg.read(ok);
        setErrorOwn(deserializeException(msg));

        LOG(MCdebugProgressDetail, job, "Finished generating part %s [%p] ok(%d) error(%d)", url.str(), this, (int)ok, (int)(error!=NULL));

        msg.clear().append(true);
        catchWriteBuffer(socket, msg);
        if (sprayer.options->getPropInt("@fail", 0))
            throwError(DFTERR_CopyFailed);
    }
    else
    {
        throwError1(DFTERR_FailedStartSlave, url.str());
    }
    LOG(MCdebugProgressDetail, job, "Stopped generate part %s [%p]", url.str(), this);

    allDone = true;
    return ok;
}


void FileTransferThread::setErrorOwn(IException * e)
{
    error.setown(e);
    if (error)
        sprayer.setError(ep, error);
}

bool FileTransferThread::transferAndSignal()
{
    ok = false;
    if (!isAborting())
    {
        try
        {
            ok = performTransfer();
        }
        catch (IException * e)
        {
            FLLOG(MCexception(e), job, e, "Transferring files");
            setErrorOwn(e);
        }
    }
    sem->signal();
    return ok;
}


int FileTransferThread::run()
{
    transferAndSignal();
    return 0;
}

//----------------------------------------------------------------------------

FileSizeThread::FileSizeThread(FilePartInfoArray & _queue, CriticalSection & _cs, bool _isCompressed, bool _errorIfMissing) : Thread("fileSizeThread"), queue(_queue), cs(_cs)
{
    isCompressed = _isCompressed;
    errorIfMissing = _errorIfMissing;
}

bool FileSizeThread::wait(unsigned timems)
{
    while (!sem.wait(timems))   { // report every time
        if (!cur.get())
            continue;       // window here?
        cur->Link();
        RemoteFilename *rfn=NULL;
        if (copy) {
            if (!cur->mirrorFilename.isNull())
                rfn = &cur->mirrorFilename;
        }
        else {
            rfn = &cur->filename;
        }   
        if (rfn) {
            StringBuffer url;
            WARNLOG("Waiting for file: %s",rfn->getRemotePath(url).str());
            cur->Release();
            return false;
        }
        cur->Release();
    }
    sem.signal(); // if called again
    return true;

}

int FileSizeThread::run()
{
    try
    {
        RemoteFilename remoteFilename;
        loop
        {
            cur.clear();
            cs.enter();
            if (queue.ordinality())
                cur.setown(&queue.popGet());
            cs.leave();

            if (!cur.get())
                break;
            copy=0;
            for (copy = 0;copy<2;copy++) {
                if (copy) {
                    if (cur->mirrorFilename.isNull())
                        continue;  // not break
                    remoteFilename.set(cur->mirrorFilename);
                }
                else
                    remoteFilename.set(cur->filename);
                OwnedIFile thisFile = createIFile(remoteFilename);
                offset_t thisSize = thisFile->size();
                if (thisSize == -1) {
                    if (errorIfMissing) {
                        StringBuffer s;
                        throwError1(DFTERR_CouldNotOpenFile, remoteFilename.getRemotePath(s).str());
                    }
                    continue;
                }
                cur->psize = thisSize;
                if (isCompressed)
                {
                    Owned<IFileIO> io = createCompressedFileReader(thisFile); //check succeeded?
                    if (!io) {
                        StringBuffer s;
                        throwError1(DFTERR_CouldNotOpenCompressedFile, remoteFilename.getRemotePath(s).str());
                    }
                    thisSize = io->size();
                }
                cur->size = thisSize;
                break;
            }
            if (copy==1) { // need to set primary
                cur->mirrorFilename.set(cur->filename);
                cur->filename.set(remoteFilename);
            }
            cur.clear();
        }
    }
    catch (IException * e)
    {
        error.setown(e);
    }
    sem.signal();
    return 0;
}


//----------------------------------------------------------------------------

FileSprayer::FileSprayer(IPropertyTree * _options, IPropertyTree * _progress, IRemoteConnection * _recoveryConnection, const char *_wuid)
  : wuid(_wuid)
{
    totalSize = 0;
    replicate = false;
    unknownSourceFormat = true;
    unknownTargetFormat = true;
    progressTree.set(_progress);
    recoveryConnection = _recoveryConnection;
    options.set(_options);
    if (!options)
        options.setown(createPTree());
    if (!progressTree)
        progressTree.setown(createPTree("progress", ipt_caseInsensitive));

    //split prefix messes up recovery because the target filenames aren't saved in the recovery info.
    allowRecovery = !options->getPropBool(ANnoRecover) && !querySplitPrefix();
    isRecovering = allowRecovery && progressTree->hasProp(ANhasProgress);
    isSafeMode = options->getPropBool(ANsafe);
    job = unknownJob;
    progressReport = NULL;
    abortChecker = NULL;
    sizeToBeRead = 0;
    calcedPullPush = false;
    mirroring = false;
    lastAbortCheckTick = lastSDSTick = lastOperatorTick = msTick();
    calcedInputCRC = false;
    aborting = false;
    totalLengthRead = 0;
    throttleNicSpeed = 0;
    compressedInput = false;
    compressOutput = options->getPropBool(ANcompress);
    copyCompressed = false;
    transferBufferSize = options->getPropInt(ANtransferBufferSize);
    if (transferBufferSize)
        LOG(MCdebugProgressDetail, job, "Using transfer buffer size %d", transferBufferSize);
    else // zero is default
        transferBufferSize = DEFAULT_STD_BUFFER_SIZE;
    progressDone = false;
    encryptKey.set(options->queryProp(ANencryptKey));
    decryptKey.set(options->queryProp(ANdecryptKey));

}




class AsyncAfterTransfer : public CAsyncFor
{
public:
    AsyncAfterTransfer(FileSprayer & _sprayer) : sprayer(_sprayer) {}

    virtual void Do(unsigned idxTarget)
    {
        TargetLocation & cur = sprayer.targets.item(idxTarget);
        if (!sprayer.filter || sprayer.filter->includePart(idxTarget))
        {
            RemoteFilename & targetFilename = cur.filename;
            if (sprayer.isSafeMode)
            {
                OwnedIFile file = createIFile(targetFilename);
                file->remove();
            }

            renameDfuTempToFinal(targetFilename);
            if (sprayer.replicate)
            {
                OwnedIFile file = createIFile(targetFilename);
                if (!sprayer.mirroring)
                    file->setTime(NULL, &cur.modifiedTime, NULL);
            }
            else if (cur.modifiedTime.isNull())
            {
                OwnedIFile file = createIFile(targetFilename);
                file->getTime(NULL, &cur.modifiedTime, NULL);
            }
        }
    }
protected:
    FileSprayer & sprayer;
};


void FileSprayer::addEmptyFilesToPartition(unsigned from, unsigned to)
{
    for (unsigned i = from; i < to ; i++)
    {
        LOG(MCdebugProgressDetail, job, "Insert a dummy entry for target %d", i);
        PartitionPoint & next = createLiteral(0, NULL, 0);
        next.whichOutput = i;
        partition.append(next);
    }
}

void FileSprayer::addEmptyFilesToPartition()
{
    unsigned lastOutput = (unsigned)-1;;
    ForEachItemIn(idx, partition)
    {
        PartitionPoint & cur = partition.item(idx);
        if (cur.whichOutput != lastOutput)
        {
            if (cur.whichOutput != lastOutput+1)
                addEmptyFilesToPartition(lastOutput+1, cur.whichOutput);
            lastOutput = cur.whichOutput;
        }
    }
    if (lastOutput != targets.ordinality()-1)
        addEmptyFilesToPartition(lastOutput+1, targets.ordinality());
}

void FileSprayer::afterTransfer()
{
    if (calcInputCRC())
    {
        LOG(MCdebugProgressDetail, job, "Checking input CRCs");
        CRC32Merger partCRC;

        unsigned startCurSource = 0;
        ForEachItemIn(idx, partition)
        {
            PartitionPoint & curPartition = partition.item(idx);
            OutputProgress & curProgress = progress.item(idx);

            if (!curProgress.hasInputCRC)
            {
                LOG(MCdebugProgressDetail, job, "Could not calculate input CRCs - cannot check");
                break;
            }
            partCRC.addChildCRC(curProgress.inputLength, curProgress.inputCRC, false);

            StringBuffer errorText;
            bool failed = false;
            UnsignedArray failedOutputs;
            if (idx+1 == partition.ordinality() || partition.item(idx+1).whichInput != curPartition.whichInput)
            {
                FilePartInfo & curSource = sources.item(curPartition.whichInput);
                if (curSource.crc != partCRC.get())
                {
                    StringBuffer name;
                    if (!failed)
                        errorText.append("Input CRCs do not match for part ");
                    else
                        errorText.append(", ");
                    curSource.filename.getPath(errorText);
                    failed = true;

                    //Need to copy anything that involves this part of the file again.
                    //pulling it will be the whole file, if pushing we can cope with single parts
                    //in the middle of the partition.
                    for (unsigned i = startCurSource; i <= idx; i++)
                    {
                        OutputProgress & cur = progress.item(i);
                        cur.reset();
                        if (cur.tree)
                            cur.save(cur.tree);
                        unsigned out = partition.item(i).whichOutput;
                        if (failedOutputs.find(out) == NotFound)
                            failedOutputs.append(out);
                    }
                }
                partCRC.clear();
    
                startCurSource = idx+1;

                //If copying m to n, and not splitting, there may be some dummy text entries (containing nothing) on the end.
                //if so skip them, otherwise you'll get crc errors on part 1
                if (partition.isItem(startCurSource) && (partition.item(startCurSource).whichInput == 0))
                    idx = partition.ordinality()-1;
            }
            if (failed)
            {
                if (usePullOperation())
                {
                    //Need to clear progress for any partitions that copy to the same target file
                    //However, need to do it after the crc checking, otherwise it will generate more errors...
                    ForEachItemIn(idx, partition)
                    {
                        if (failedOutputs.find(partition.item(idx).whichOutput) != NotFound)
                        {
                            OutputProgress & cur = progress.item(idx);
                            cur.reset();
                            if (cur.tree)
                                cur.save(cur.tree);
                        }
                    }
                }
                if (recoveryConnection)
                    recoveryConnection->commit();
                throw MakeStringException(DFTERR_InputCrcMismatch, "%s", errorText.str());
            }
        }
    }

    if (isSafeMode || !usePullOperation())
    {
        unsigned numTargets = targets.ordinality();
        AsyncAfterTransfer async(*this);
        async.For(numTargets, (unsigned)sqrt((float)numTargets));
    }
    else
    {
        ForEachItemIn(idx, progress)
        {
            OutputProgress & curProgress = progress.item(idx);
            if (!curProgress.resultTime.isNull())
                targets.item(partition.item(idx).whichOutput).modifiedTime.set(curProgress.resultTime);
        }
    }
}

bool FileSprayer::allowSplit()
{
    return !(options->getPropBool(ANnosplit) || options->getPropBool(ANnosplit2) || options->queryProp(ANprefix));
}

void FileSprayer::assignPartitionFilenames()
{
    ForEachItemIn(idx, partition)
    {
        PartitionPoint & cur = partition.item(idx);
        if (cur.whichInput != (unsigned)-1)
        {
            cur.inputName.set(sources.item(cur.whichInput).filename);
            setCanAccessDirectly(cur.inputName);
        }
        cur.outputName.set(targets.item(cur.whichOutput).filename);
        setCanAccessDirectly(cur.outputName);
        if (replicate)
            cur.modifiedTime.set(targets.item(cur.whichOutput).modifiedTime);
    }
}

class CheckExists : public CAsyncFor
{
public:
    CheckExists(TargetLocationArray & _targets, IDFPartFilter * _filter) : targets(_targets) { filter = _filter; }

    virtual void Do(unsigned idx)
    {
        if (!filter || filter->includePart(idx))
        {
            const RemoteFilename & cur = targets.item(idx).filename;
            OwnedIFile file = createIFile(cur);
            if (file->exists())
            {
                StringBuffer s;
                throwError1(DFTERR_PhysicalExistsNoOverwrite, cur.getRemotePath(s).str());
            }
        }
    }

public:
    TargetLocationArray & targets;
    IDFPartFilter * filter;
};


void FileSprayer::beforeTransfer()
{
    if (!isRecovering && !options->getPropBool("@overwrite", true))
    {
        CheckExists checker(targets, filter);
        checker.For(targets.ordinality(), 25, true, true);
    }

    if (!isRecovering && !usePullOperation())
    {
        try {
            //Should this be on an option.  Shouldn't be too inefficient since push is seldom used.
            ForEachItemIn(idx2, targets)
            {
                if (!filter || filter->includePart(idx2))
                {
                    //MORE: This does not cope with creating directories on a solaris machine.
                    StringBuffer remoteFilename, remoteDirectory;
                    targets.item(idx2).filename.getRemotePath(remoteFilename);
                    splitUNCFilename(remoteFilename.str(), &remoteDirectory, &remoteDirectory, NULL, NULL);

                    Owned<IFile> dir = createIFile(remoteDirectory.str());
                    dir->createDirectory();
                }
            }
        }
        catch (IException *e) {
            FLLOG(MCexception(e), job, e, "Creating Directory");
            e->Release();
            LOG(MCdebugInfo, job, "Ignoring create directory error");
        }

        // If pushing files, and not recovering, then need to delete the target files, because the slaves might be writing in any order
        // for pull, the slave deletes it when creating the file.
        unsigned curPartition = 0;
        ForEachItemIn(idxTarget, targets)
        {
            if (!filter || filter->includePart(idxTarget))
            {
                if (!isSafeMode)
                {
                    OwnedIFile file = createIFile(targets.item(idxTarget).filename);
                    file->remove();
                }

                while (partition.isItem(curPartition+1) && partition.item(curPartition+1).whichOutput == idxTarget)
                    curPartition++;
                PartitionPoint & lastPartition = partition.item(curPartition);
                offset_t lastOutputOffset = lastPartition.outputOffset + lastPartition.outputLength;

                RemoteFilename remote;
                getDfuTempName(remote, targets.item(idxTarget).filename);
                OwnedIFile file = createIFile(remote);
                OwnedIFileIO io = file->open(IFOcreate);
                if (!io)
                {
                    StringBuffer name;
                    remote.getPath(name);
                    throwError1(DFTERR_CouldNotCreateOutput, name.str());
                }
                //Create the headers on the utf files.
                unsigned headerSize = getHeaderSize(tgtFormat.type);
                if (headerSize)
                    io->write(0, headerSize, getHeaderText(tgtFormat.type));

                if ((lastOutputOffset != 0)&&!compressOutput)
                {
                    char null = 0;
                    io->write(lastOutputOffset-sizeof(null), sizeof(null), &null);
                }
            }
        }
    }

    throttleNicSpeed = options->getPropInt(ANthrottle, 0);
    if (throttleNicSpeed == 0 && !usePullOperation() && targets.ordinality() == 1 && sources.ordinality() > 1)
    {
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        if (factory) {
            Owned<IConstEnvironment> env = factory->openEnvironment();
            if (env) {
                StringBuffer ipText;
                targets.item(0).filename.queryIP().getIpText(ipText);
                Owned<IConstMachineInfo> machine = env->getMachineByAddress(ipText.str());
                if (machine) 
                {
                    if (machine->getOS() == MachineOsW2K)
                    {
                        throttleNicSpeed = machine->getNicSpeedMbitSec();
                        LOG(MCdebugInfo, job, "Throttle target speed to %dMbit/sec", throttleNicSpeed);
                    }
                }
            }
        }
    }
}


bool FileSprayer::calcCRC()
{
    return options->getPropBool(ANcrc, true) && !compressOutput && !copyCompressed; 
}

bool FileSprayer::calcInputCRC()
{
    if (!calcedInputCRC)
    {
        calcedInputCRC = true;
        cachedInputCRC = false;
        if (options->getPropBool(ANcrcCheck, true) && !compressedInput)
        {
            ForEachItemIn(idx, sources)
            {
                if (!sources.item(idx).hasCRC)
                    return cachedInputCRC;
            }
            cachedInputCRC = true;

            //If keeping headers then we lose bits of the input files, so they can't be crc checked.
            bool canKeepHeader = srcFormat.equals(tgtFormat) || !needToCalcOutput();
            if (options->getPropBool("@keepHeader", canKeepHeader) && srcFormat.rowTag && sources.ordinality() > 1)
                cachedInputCRC = false;
            if (querySplitPrefix())
                cachedInputCRC = false;
        }
    }
    return cachedInputCRC;
}

void FileSprayer::calculateOne2OnePartition()
{
    LOG(MCdebugProgressDetail, job, "Setting up one2One partition");
    if (sources.ordinality() != targets.ordinality())
        throwError(DFTERR_ReplicateNumPartsDiffer);
    if (!srcFormat.equals(tgtFormat))
       throwError(DFTERR_ReplicateSameFormat);

    if (compressedInput && compressOutput && (strcmp(encryptKey.str(),decryptKey.str())==0))
        setCopyCompressedRaw();                 

    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        RemoteFilename curFilename;
        curFilename.set(cur.filename);
        setCanAccessDirectly(curFilename);
        partition.append(*new PartitionPoint(idx, idx, cur.headerSize, copyCompressed?cur.psize:cur.size, copyCompressed?cur.psize:cur.size));  // outputoffset == 0
        targets.item(idx).modifiedTime.set(cur.modifiedTime);
    }
}

class AsyncExtractBlobInfo : public CAsyncFor
{
    friend class FileSprayer;
public:
    AsyncExtractBlobInfo(const char * _splitPrefix, FileSprayer & _sprayer) : sprayer(_sprayer) 
    {
        extracted = new ExtractedBlobArray[sprayer.sources.ordinality()];
        splitPrefix = _splitPrefix;
    }
    ~AsyncExtractBlobInfo()
    {
        delete [] extracted;
    }
    virtual void Do(unsigned i)
    {
        if (!sprayer.sources.item(i).filename.isLocal()) {
            try {
                remoteExtractBlobElements(splitPrefix, sprayer.sources.item(i).filename, extracted[i]);
                return;
            }
            catch (IException *e) {
                StringBuffer path;
                StringBuffer err;
                WARNLOG("dafilesrv ExtractBlobElements(%s) failed with: %s",
                        sprayer.sources.item(i).filename.getPath(path).str(),
                        e->errorMessage(err).str());
                PROGLOG("Trying direct access (this may be slow)");
                e->Release();
            }
        }
        // try local
        extractBlobElements(splitPrefix, sprayer.sources.item(i).filename, extracted[i]);
    }
protected:
    FileSprayer & sprayer;
    const char * splitPrefix;
    ExtractedBlobArray * extracted;
};


void FileSprayer::calculateSplitPrefixPartition(const char * splitPrefix)
{
    if (targets.ordinality() != 1)
        throwError(DFTERR_SplitPrefixSingleTarget);

    if (!srcFormat.equals(tgtFormat))
       throwError(DFTERR_SplitPrefixSameFormat);

    LOG(MCdebugProgressDetail, job, "Setting up split prefix partition");

    Owned<TargetLocation> target = &targets.popGet();       // remove target, add lots of new ones
    RemoteFilename blobTarget;
    StringBuffer remoteTargetPath, remoteFilename;
    target->filename.getRemotePath(remoteTargetPath);
    char sepChar = target->filename.getPathSeparator();

    //Remove the tail name from the filename
    const char * temp = remoteTargetPath.str();
    remoteTargetPath.setLength(strrchr(temp, sepChar)-temp);

    AsyncExtractBlobInfo extractor(splitPrefix, *this);
    unsigned numSources = sources.ordinality();
    extractor.For(numSources, numParallelConnections(numSources), true, false);

    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        ExtractedBlobArray & extracted = extractor.extracted[idx];
        ForEachItemIn(i, extracted)
        {
            ExtractedBlobInfo & curBlob = extracted.item(i);

            remoteFilename.clear().append(remoteTargetPath);
            addPathSepChar(remoteFilename, sepChar);
            remoteFilename.append(curBlob.filename);
            blobTarget.clear();
            blobTarget.setRemotePath(remoteFilename);
            targets.append(* new TargetLocation(blobTarget));
            partition.append(*new PartitionPoint(idx, targets.ordinality()-1, curBlob.offset, curBlob.length, curBlob.length));
        }
    }
}

void FileSprayer::calculateMany2OnePartition()
{
    LOG(MCdebugProgressDetail, job, "Setting up many2one partition");
    const char *partSeparator = srcFormat.getPartSeparatorString();
    offset_t lastContentLength = 0;
    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        RemoteFilename curFilename;
        curFilename.set(cur.filename);
        setCanAccessDirectly(curFilename);
        if (partSeparator)
        {
            offset_t contentLength = (cur.size > cur.xmlHeaderLength + cur.xmlFooterLength ? cur.size - cur.xmlHeaderLength - cur.xmlFooterLength : 0);
            if (contentLength)
            {
                if (lastContentLength)
                {
                    PartitionPoint &part = createLiteral(1, partSeparator, (unsigned) -1);
                    part.whichOutput = 0;
                    partition.append(part);
                }
                lastContentLength = contentLength;
            }
        }
        partition.append(*new PartitionPoint(idx, 0, cur.headerSize, cur.size, cur.size));
    }
}

void FileSprayer::calculateNoSplitPartition()
{
    LOG(MCdebugProgressDetail, job, "Setting up no split partition");
    if (!usePullOperation() && !srcFormat.equals(tgtFormat))
        throwError(DFTERR_NoSplitPushChangeFormat);

#if 1
    //split by number
    unsigned numSources = sources.ordinality();
    unsigned numTargets = targets.ordinality();
    if (numSources < numTargets)
        numTargets = numSources;
    unsigned tally = 0;
    unsigned curTarget = 0;
    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        partition.append(*new PartitionPoint(idx, curTarget, cur.headerSize, copyCompressed?cur.psize:cur.size, copyCompressed?cur.psize:cur.size));    // outputoffset == 0
        tally += numTargets;
        if (tally >= numSources)
        {
            tally -= numSources;
            curTarget++;
        }
    }
#else
    //split by size
    offset_t totalSize = 0;
    ForEachItemIn(i, sources)
        totalSize += sources.item(i).size;

    unsigned numTargets = targets.ordinality();
    offset_t chunkSize = (totalSize / numTargets);
    offset_t nextBoundary = chunkSize;
    offset_t sizeSoFar = 0;
    unsigned curTarget = 0;
    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        offset_t nextSize = sizeSoFar + cur.size;

        if ((sizeSoFar >= nextBoundary) || 
             ((nextSize > nextBoundary) &&
              (nextBoundary - sizeSoFar < nextSize - nextBoundary)))
        {
            if (curTarget != numTargets-1)
            {
                curTarget++;
                nextBoundary += chunkSize;
            }
        }

        RemoteFilename curFilename;
        curFilename.set(cur.filename);
        setCanAccessDirectly(curFilename);
        partition.append(*new PartitionPoint(idx, curTarget, cur.headerSize, cur.size, cur.size));  // outputoffset == 0
        sizeSoFar = nextSize;
    }
#endif
}

void FileSprayer::calculateSprayPartition()
{
    LOG(MCdebugProgressDetail, job, "Calculating N:M partition");

    bool calcOutput = needToCalcOutput();
    FormatPartitionerArray partitioners;

    unsigned numParts = targets.ordinality();
    StringBuffer remoteFilename;
    StringBuffer slaveName;
    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        cur.filename.getRemotePath(remoteFilename.clear());

        srcFormat.quotedTerminator = options->getPropBool("@quotedTerminator", true);
        LOG(MCdebugInfoDetail, job, "Partition %d(%s)", idx, remoteFilename.str());
        const SocketEndpoint & ep = cur.filename.queryEndpoint();
        IFormatPartitioner * partitioner = createFormatPartitioner(ep, srcFormat, tgtFormat, calcOutput, queryFixedSlave(), wuid);


        // CSV record structure discovery of every source
        bool isRecordStructurePresent = options->getPropBool("@recordStructurePresent", false);
        partitioner->setRecordStructurePresent(isRecordStructurePresent);

        RemoteFilename name;
        name.set(cur.filename);
        setCanAccessDirectly(name);
        partitioner->setPartitionRange(totalSize, cur.offset, cur.size, cur.headerSize, numParts);
        partitioner->setSource(idx, name, compressedInput, decryptKey);
        partitioners.append(*partitioner);
    }

    unsigned numProcessors = partitioners.ordinality();
    unsigned maxConnections = numParallelConnections(numProcessors);

    //Throttle maximum number of concurrent transfers by starting n threads, and
    //then waiting for one to complete before going on to the next
    Semaphore sem;
    unsigned goIndex;
    for (goIndex=0; goIndex < maxConnections; goIndex++)
        partitioners.item(goIndex).calcPartitions(&sem);

    for (; goIndex<numProcessors; goIndex++)
    {
        sem.wait();
        partitioners.item(goIndex).calcPartitions(&sem);
    }

    for (unsigned waitCount=0; waitCount < maxConnections;waitCount++)
        sem.wait();

    ForEachItemIn(idx2, partitioners)
        partitioners.item(idx2).getResults(partition);

    if ((partitioners.ordinality() > 0) && !srcAttr->hasProp("ECL"))
    {
        // Store discovered CSV record structure into target logical file.
        StringBuffer recStru;
        partitioners.item(0).getRecordStructure(recStru);
        if ((recStru.length() > 0) && strstr(recStru.str(),"END;"))
        {
            if (distributedTarget)
                distributedTarget->setECL(recStru.str());
        }
    }

}

void FileSprayer::calculateOutputOffsets()
{
    unsigned headerSize = getHeaderSize(tgtFormat.type);
    offset_t outputOffset = headerSize;
    unsigned curOutput = 0;
    ForEachItemIn(idx, partition)
    {
        PartitionPoint & cur = partition.item(idx);
        if (curOutput != cur.whichOutput)
        {
            outputOffset = headerSize;
            curOutput = cur.whichOutput;
        }

        cur.outputOffset = outputOffset;
        outputOffset += cur.outputLength;
    }
}


void FileSprayer::checkFormats()
{
    if (unknownSourceFormat)
    {
        //If target format is specified, use that - not really very good, but...
        srcFormat.set(tgtFormat);

        //If format omitted, and number of parts are the same then okay to omit the format
        if (sources.ordinality() == targets.ordinality() && !disallowImplicitReplicate())
            replicate = true;

        bool noSplit = !allowSplit();
        if (!replicate && !noSplit)
        {
            //copy to a single target => assume same format concatenated.
            if (targets.ordinality() != 1)
            {
                if (!unknownTargetFormat)
                    throwError(DFTERR_TargetFormatUnknownSource);
                else
                    throwError(DFTERR_FormatNotSpecified);
            }
        }
    }

    FileFormatType srcType = srcFormat.type;
    FileFormatType tgtType = tgtFormat.type;
    if (srcType != tgtType)
    {
        switch (srcType)
        {
        case FFTfixed:
            if ((tgtType != FFTvariable)&&(tgtType != FFTvariablebigendian))        
                throwError(DFTERR_BadSrcTgtCombination);
            break;
        case FFTvariable:
            if ((tgtType != FFTfixed) && (tgtType != FFTblocked)&& (tgtType != FFTvariablebigendian))   
                throwError(DFTERR_BadSrcTgtCombination);
            break;
        case FFTvariablebigendian:
            if ((tgtType != FFTfixed) && (tgtType != FFTblocked) && (tgtType != FFTvariable))   
                throwError(DFTERR_BadSrcTgtCombination);
            break;
        case FFTblocked:
            if ((tgtType != FFTvariable)&&(tgtType != FFTvariablebigendian))        
                throwError(DFTERR_BadSrcTgtCombination);
            break;
        case FFTcsv:
            throwError(DFTERR_BadSrcTgtCombination);
        case FFTutf: case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
            switch (tgtFormat.type)
            {
            case FFTutf: case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
                break;
            default:
                throwError(DFTERR_OnlyConvertUtfUtf);
                break;
            }
            break;
        }
    }
    switch (srcType)
    {
        case FFTutf: case FFTutf8: case FFTutf8n: case FFTutf16: case FFTutf16be: case FFTutf16le: case FFTutf32: case FFTutf32be: case FFTutf32le:
            if (srcFormat.rowTag)
            {
                srcFormat.maxRecordSize = srcFormat.maxRecordSize > DEFAULT_MAX_XML_RECORD_SIZE ? srcFormat.maxRecordSize : DEFAULT_MAX_XML_RECORD_SIZE;
            }
            break;

        default:
            break;
    }
}

void FileSprayer::calibrateProgress()
{
    sizeToBeRead = 0;
    ForEachItemIn(idx, transferSlaves)
        sizeToBeRead += transferSlaves.item(idx).getInputSize();

    totalLengthRead = calcSizeReadAlready();
}

void FileSprayer::checkForOverlap()
{
    unsigned num = std::min(sources.ordinality(), targets.ordinality());

    for (unsigned idx = 0; idx < num; idx++)
    {
        RemoteFilename & srcName = sources.item(idx).filename;
        RemoteFilename & tgtName = targets.item(idx).filename;

        if (srcName.equals(tgtName))
        {
            StringBuffer x;
            srcName.getPath(x);
            throwError1(DFTERR_CopyFileOntoSelf, x.str());
        }
    }
}

void FileSprayer::cleanupRecovery()
{
    progressTree->setPropBool(ANcomplete, true);
#ifdef CLEANUP_RECOVERY
    progressTree->removeProp(ANhasPartition);
    progressTree->removeProp(ANhasProgress);
    progressTree->removeProp(ANhasRecovery);
    progressTree->removeProp(PNpartition);
    progressTree->removeProp(PNprogress);
    progressTree->removeProp(ANpull);
#endif
}


//Several files being pulled to the same machine - only run ftslave once...
void FileSprayer::commonUpSlaves()
{
    unsigned max = partition.ordinality();
    bool pull = usePullOperation();
    for (unsigned idx = 0; idx < max; idx++)
    {
        PartitionPoint & cur = partition.item(idx);
        cur.whichSlave = pull ? cur.whichOutput : cur.whichInput;
        if (cur.whichSlave == -1)
            cur.whichSlave = 0;
    }

    if (options->getPropBool(ANnocommon, false))
        return;

    //First work out which are the same slaves, and then map the partition.  
    //Previously it was n^2 in partition, which is fine until you spray 100K files.
    unsigned numSlaves = pull ? targets.ordinality() : sources.ordinality();
    unsigned * slaveMapping = new unsigned [numSlaves];
    for (unsigned i = 0; i < numSlaves; i++)
        slaveMapping[i] = i;

    if (pull)
    {
        for (unsigned i1 = 1; i1 < numSlaves; i1++)
        {
            TargetLocation & cur = targets.item(i1);
            for (unsigned i2 = 0; i2 < i1; i2++)
            {
                if (targets.item(i2).filename.queryIP().ipequals(cur.filename.queryIP()))
                {
                    slaveMapping[i1] = i2;
                    break;
                }
            }
        }
    }
    else
    {
        for (unsigned i1 = 1; i1 < numSlaves; i1++)
        {
            FilePartInfo & cur = sources.item(i1);
            for (unsigned i2 = 0; i2 < i1; i2++)
            {
                if (sources.item(i2).filename.queryIP().ipequals(cur.filename.queryIP()))
                {
                    slaveMapping[i1] = i2;
                    break;
                }
            }
        }
    }


    for (unsigned i3 = 0; i3 < max; i3++)
    {
        PartitionPoint & cur = partition.item(i3);
        cur.whichSlave = slaveMapping[cur.whichSlave];
    }

    delete [] slaveMapping;
}


void FileSprayer::analyseFileHeaders(bool setcurheadersize)
{
    FileFormatType defaultFormat = FFTunknown;
    switch (srcFormat.type)
    {
    case FFTutf:
    case FFTutf8:
        defaultFormat = FFTutf8n;
        break;
    case FFTutf16:
        defaultFormat = FFTutf16be;
        break;
    case FFTutf32:
        defaultFormat = FFTutf32be;
        break;
    default:
        if (!srcFormat.rowTag)
            return;
        break;
    }

    FileFormatType actualType = FFTunknown;
    unsigned numEmptyXml = 0;
    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        StringBuffer s;
        cur.filename.getPath(s);
        LOG(MCdebugInfo, job, "Examine header of file %s", s.str());

        Owned<IFile> file = createIFile(cur.filename);
        Owned<IFileIO> io = file->open(IFOread);
        if (!io)
        {
            StringBuffer s;
            cur.filename.getRemotePath(s);
            throwError1(DFTERR_CouldNotOpenFilePart, s.str());
        }

        if (compressedInput) {
            Owned<IExpander> expander;
            if (!decryptKey.isEmpty()) {
                StringBuffer key;
                decrypt(key,decryptKey);
                expander.setown(createAESExpander256(key.length(),key.str()));
            }
            io.setown(createCompressedFileReader(io,expander)); 
        }

        if (defaultFormat != FFTunknown)
        {
            FileFormatType thisType;
            unsigned char header[4];
            memset(header, 255, sizeof(header));            // fill so don't get clashes if file is very small!
            unsigned numRead = io->read(0, 4, header);
            unsigned headerSize = 0;
            if ((memcmp(header, "\xEF\xBB\xBF", 3) == 0) && (srcFormat.type == FFTutf || srcFormat.type == FFTutf8))
            {
                thisType = FFTutf8n;
                headerSize = 3;
            }
            else if ((memcmp(header, "\xFF\xFE\x00\x00", 4) == 0) && (srcFormat.type == FFTutf || srcFormat.type == FFTutf32))
            {
                thisType = FFTutf32le;
                headerSize = 4;
            }
            else if ((memcmp(header, "\x00\x00\xFE\xFF", 4) == 0) && (srcFormat.type == FFTutf || srcFormat.type == FFTutf32))
            {
                thisType = FFTutf32be;
                headerSize = 4;
            }
            else if ((memcmp(header, "\xFF\xFE", 2) == 0) && (srcFormat.type == FFTutf || srcFormat.type == FFTutf16))
            {
                thisType = FFTutf16le;
                headerSize = 2;
            }
            else if ((memcmp(header, "\xFE\xFF", 2) == 0) && (srcFormat.type == FFTutf || srcFormat.type == FFTutf16))
            {
                thisType = FFTutf16be;
                headerSize = 2;
            }
            else
            {
                thisType = defaultFormat;
                headerSize = 0;
            }
            if (actualType == FFTunknown)
                actualType = thisType;
            else if (actualType != thisType)
                throwError(DFTERR_PartsDoNotHaveSameUtfFormat);

            if (setcurheadersize) {
                cur.headerSize = headerSize;
                cur.size -= headerSize;
            }
        }
        if (srcFormat.rowTag&&setcurheadersize)
        {
            try
            {
                if (distributedSource)
                {
                    // Despray from distributed file

                    // Check XMLheader/footer in file level
                    DistributedFilePropertyLock lock(distributedSource);
                    IPropertyTree &curProps = lock.queryAttributes();
                    if (curProps.hasProp(FPheaderLength) && curProps.hasProp(FPfooterLength))
                    {
                        cur.xmlHeaderLength = curProps.getPropInt(FPheaderLength, 0);
                        cur.xmlFooterLength = curProps.getPropInt(FPfooterLength, 0);
                    }
                    else
                    {
                        // Try it in file part level
                        Owned<IDistributedFilePart> curPart = distributedSource->getPart(idx);
                        IPropertyTree& curPartProps = curPart->queryAttributes();
                        cur.xmlHeaderLength = curPartProps.getPropInt(FPheaderLength, 0);
                        cur.xmlFooterLength = curPartProps.getPropInt(FPfooterLength, 0);
                    }
                }
                else
                {
                    // Spray from file
                    if (srcFormat.headerLength == (unsigned)-1 || srcFormat.footerLength == (unsigned)-1)
                        locateContentHeader(io, cur.headerSize, cur.xmlHeaderLength, cur.xmlFooterLength);
                    else
                    {
                        cur.xmlHeaderLength = srcFormat.headerLength;
                        cur.xmlFooterLength = srcFormat.footerLength;
                    }
                }
                cur.headerSize += (unsigned)cur.xmlHeaderLength;
                if (cur.size >= cur.xmlHeaderLength + cur.xmlFooterLength)
                    cur.size -= (cur.xmlHeaderLength + cur.xmlFooterLength);
                else
                    throwError3(DFTERR_InvalidXmlPartSize, cur.size, cur.xmlHeaderLength, cur.xmlFooterLength);
            }
            catch (IException * e)
            {
                if (e->errorCode() != DFTERR_CannotFindFirstXmlRecord)
                    throw;
                e->Release();
                if (!replicate)
                {
                    cur.headerSize = 0;
                    cur.size = 0;
                }
                numEmptyXml++;
            }
        }
    }
    if (numEmptyXml == sources.ordinality())
    {
        if (numEmptyXml == 1)
            throwError(DFTERR_CannotFindFirstXmlRecord);
//      else
//          throwError(DFTERR_CannotFindAnyXmlRecord);
    }

    if (defaultFormat != FFTunknown)
        srcFormat.type = actualType;
    if (unknownTargetFormat)
    {
        tgtFormat.set(srcFormat);
        if (distributedTarget)
        {
            DistributedFilePropertyLock lock(distributedTarget);
            IPropertyTree &curProps = lock.queryAttributes();
            tgtFormat.save(&curProps);
        }
    }
}


void FileSprayer::locateXmlHeader(IFileIO * io, unsigned headerSize, offset_t & xmlHeaderLength, offset_t & xmlFooterLength)
{
    Owned<IFileIOStream> in = createIOStream(io);

    XmlSplitter splitter(srcFormat);
    BufferedDirectReader reader;

    reader.set(in);
    reader.seek(headerSize);
    if (xmlHeaderLength == 0)
    {
        try
        {
            xmlHeaderLength = splitter.getHeaderLength(reader);
        }
        catch (IException * e)
        {
            if (e->errorCode() != DFTERR_CannotFindFirstXmlRecord)
                throw;
            e->Release();
            xmlHeaderLength = 0;
        }
    }

    offset_t size = io->size();
    offset_t endOffset = (size > srcFormat.maxRecordSize*2 + headerSize) ? size - srcFormat.maxRecordSize*2 : headerSize;
    reader.seek(endOffset);
    if (xmlFooterLength == 0)
    {
        try
        {
            xmlFooterLength = splitter.getFooterLength(reader, size);
        }
        catch (IException * e)
        {
            if (e->errorCode() != DFTERR_CannotFindLastXmlRecord)
                throw;
            e->Release();
            xmlFooterLength= 0;
        }
    }
}

void FileSprayer::locateJsonHeader(IFileIO * io, unsigned headerSize, offset_t & headerLength, offset_t & footerLength)
{
    Owned<IFileIOStream> in = createIOStream(io);

    JsonSplitter jsplitter(srcFormat, *in);
    headerLength = jsplitter.getHeaderLength();
    footerLength = jsplitter.getFooterLength();
}

void FileSprayer::locateContentHeader(IFileIO * io, unsigned headerSize, offset_t & headerLength, offset_t & footerLength)
{
    if (srcFormat.markup == FMTjson)
        locateJsonHeader(io, headerSize, headerLength, footerLength);
    else
        locateXmlHeader(io, headerSize, headerLength, footerLength);
}

void FileSprayer::derivePartitionExtra()
{
    calculateOutputOffsets();
    assignPartitionFilenames();
    commonUpSlaves();

    IPropertyTreeIterator * iter = NULL;
    if (isRecovering)
    {
        Owned<IPropertyTreeIterator> iter = progressTree->getElements(PNprogress);
        ForEach(*iter)
        {
            OutputProgress & next = * new OutputProgress;
            next.restore(&iter->query());
            next.tree.set(&iter->query());
            progress.append(next);
        }
        assertex(progress.ordinality() == partition.ordinality());
    }
    else
    {
        if (allowRecovery)
            progressTree->setPropBool(ANhasProgress, true);
        ForEachItemIn(idx, partition)
        {
            OutputProgress & next = * new OutputProgress;
            next.whichPartition=idx;
            if (allowRecovery)
            {
                IPropertyTree * progressInfo = progressTree->addPropTree(PNprogress, createPTree(PNprogress, ipt_caseInsensitive));
                next.tree.set(progressInfo);
                next.save(progressInfo);
            }
            progress.append(next);
        }
    }
}


void FileSprayer::displayPartition()
{
    ForEachItemIn(idx, partition)
        partition.item(idx).display();
}


void FileSprayer::extractSourceFormat(IPropertyTree * props)
{
    if (srcFormat.restore(props))
        unknownSourceFormat = false;
    else
        srcFormat.set(FFTfixed, 1);
    bool blockcompressed;
    if (isCompressed(*props, &blockcompressed))
    {
        if (!blockcompressed)
            throwError(DFTERR_RowCompressedNotSupported);
        compressedInput = true;
    }
    else if (!decryptKey.isEmpty())
        compressedInput = true;
}


void FileSprayer::gatherFileSizes(bool errorIfMissing)
{
    FilePartInfoArray fileSizeQueue;

    LOG(MCdebugProgress, job, "Start gathering file sizes...");

    ForEachItemIn(idx, sources)
    {
        FilePartInfo & cur = sources.item(idx);
        if (cur.size == UNKNOWN_PART_SIZE)
            fileSizeQueue.append(OLINK(cur));
    }

    gatherFileSizes(fileSizeQueue, errorIfMissing);
    LOG(MCdebugProgress, job, "Finished gathering file sizes...");
}

void FileSprayer::afterGatherFileSizes()
{

    if (!copyCompressed)
    {
        ForEachItemIn(idx2, sources)
        {
            FilePartInfo & cur = sources.item(idx2);
            cur.offset = totalSize;
            totalSize += cur.size;
            if (cur.size % srcFormat.getUnitSize())
            {
                StringBuffer s;
                if (srcFormat.isUtf())
                    throwError2(DFTERR_InputIsInvalidMultipleUtf, cur.filename.getRemotePath(s).str(), srcFormat.getUnitSize());
                else
                    throwError2(DFTERR_InputIsInvalidMultiple, cur.filename.getRemotePath(s).str(), srcFormat.getUnitSize());
            }
        }
    }
}


void FileSprayer::gatherFileSizes(FilePartInfoArray & fileSizeQueue, bool errorIfMissing)
{
    if (fileSizeQueue.ordinality())
    {
        CIArrayOf<FileSizeThread> threads;
        CriticalSection fileSizeCS;

        //Is this a good guess?  start square root of number of files threads??
        unsigned numThreads = (unsigned)sqrt((float)fileSizeQueue.ordinality());
        if (numThreads>20)
            numThreads = 20;
        LOG(MCdebugProgress, job, "Gathering %d file sizes on %d threads", fileSizeQueue.ordinality(), numThreads);
        unsigned idx;
        for (idx = 0; idx < numThreads; idx++)
            threads.append(*new FileSizeThread(fileSizeQueue, fileSizeCS, compressedInput&&!copyCompressed, errorIfMissing));
        for (idx = 0; idx < numThreads; idx++)
            threads.item(idx).start();
        loop {
            bool alldone = true;
            StringBuffer err;
            for (idx = 0; idx < numThreads; idx++) {
                bool ok = threads.item(idx).wait(10*1000);
                if (!ok) 
                    alldone = false;
            }
            if (alldone)
                break;
        }
        for (idx = 0; idx < numThreads; idx++)
            threads.item(idx).queryThrowError();
    }
}

void FileSprayer::gatherMissingSourceTarget(IFileDescriptor * source)
{
    //First gather all the file sizes...
    RemoteFilename filename;
    FilePartInfoArray primparts;  
    FilePartInfoArray secparts;
    UnsignedArray secstart;
    FilePartInfoArray queue;
    unsigned numParts = source->numParts();
    for (unsigned idx1=0; idx1 < numParts; idx1++)
    {
        if (!filter.get() || filter->includePart(idx1)) 
        {
            unsigned numCopies = source->numCopies(idx1);
            if (numCopies>=1) // only add if there is one or more replicates
            {  
                for (unsigned copy=0; copy < numCopies; copy++)             
                {
                    FilePartInfo & next = * new FilePartInfo;
                    source->getFilename(idx1, copy, next.filename);
                    if (copy==0)
                        primparts.append(next);
                    else 
                    {   
                        if (copy==1) 
                            secstart.append(secparts.ordinality());
                        secparts.append(next);
                    }
                    queue.append(OLINK(next));
                }
            }
        }
    }
    secstart.append(secparts.ordinality());

    gatherFileSizes(queue, false);

    //Now process the information...
    StringBuffer primaryPath, secondaryPath;
    for (unsigned idx=0; idx < primparts.ordinality(); idx++)
    {
        FilePartInfo & primary = primparts.item(idx);
        offset_t primarySize = primary.size;
        primary.filename.getRemotePath(primaryPath.clear());

        for (unsigned idx2=secstart.item(idx);idx2<secstart.item(idx+1);idx2++) 
        {
            FilePartInfo & secondary = secparts.item(idx2);
            offset_t secondarySize = secondary.size;
            secondary.filename.getRemotePath(secondaryPath.clear());

            unsigned sourceCopy = 0;
            if (primarySize != secondarySize)
            {
                if (primarySize == -1)
                {
                    sourceCopy = 1;
                }
                else if (secondarySize != -1) 
                {
                    LOG(MCwarning, unknownJob, "Replicate - primary and secondary copies have different sizes (%" I64F "d v %" I64F "d) for part %u", primarySize, secondarySize, idx);
                    continue; // ignore copy
                }
            }
            else
            {
                if (primarySize == -1)
                {
                    LOG(MCwarning, unknownJob, "Replicate - neither primary or secondary copies exist for part %u", idx);
                    primarySize = 0;        // to stop later failure to gather the file size
                }
                continue; // ignore copy
            }
            
            RemoteFilename *dst= (sourceCopy == 0) ? &secondary.filename : &primary.filename;
            // check nothing else to same destination
            bool done = false;
            ForEachItemIn(dsti,targets) {
                TargetLocation &tgt = targets.item(dsti);
                if (tgt.filename.equals(*dst)) {
                    done = true;
                    break;
                }
            }
            if (!done) {
                sources.append(* new FilePartInfo(*((sourceCopy == 0)? &primary.filename : &secondary.filename)));
                targets.append(* new TargetLocation(*dst));
                sources.tos().size = (sourceCopy == 0) ? primarySize : secondarySize;
            }
        }
    }

    filter.clear(); // we have already filtered
}


unsigned __int64 FileSprayer::calcSizeReadAlready()
{
    unsigned __int64 sizeRead = 0;
    ForEachItemIn(idx, progress)
    {
        OutputProgress & cur = progress.item(idx);
        sizeRead += cur.inputLength;
    }
    return sizeRead;
}

unsigned __int64 FileSprayer::getSizeReadAlready()
{
    return totalLengthRead;
}


PartitionPoint & FileSprayer::createLiteral(size32_t len, const void * data, unsigned idx)
{
    PartitionPoint & next = * new PartitionPoint;
    next.inputOffset = 0;
    next.inputLength = len;
    next.outputLength = len;
    next.fixedText.set(len, data);
    if (partition.isItem(idx))
    {
        PartitionPoint & cur = partition.item(idx);
        next.whichInput = cur.whichInput;
        next.whichOutput = cur.whichOutput;
    }
    else
    {
        next.whichInput = (unsigned)-1;
        next.whichOutput = (unsigned)-1;
    }
    return next;
}

void FileSprayer::addHeaderFooter(size32_t len, const void * data, unsigned idx, bool before)
{
    PartitionPoint & next = createLiteral(len, data, idx);
    unsigned insertPos = before ? idx : idx+1;
    partition.add(next, insertPos);
}


//MORE: This should be moved to jlib....
//MORE: I should really be doing this on unicode characters and supporting \u \U
void replaceEscapeSequence(StringBuffer & out, const char * in, bool errorIfInvalid)
{
    out.ensureCapacity(strlen(in)+1);
    while (*in)
    {
        char c = *in++;
        if (c == '\\')
        {
            char next = *in;
            if (next)
            {
                in++;
                switch (next)
                {
                case 'a': c = '\a'; break;
                case 'b': c = '\b'; break;
                case 'f': c = '\f'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                case 'v': c = '\v'; break;
                case '\\':
                case '\'':
                case '?':
                case '\"': break;
                case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7':
                    {
                        c = next - '0';
                        if (*in >= '0' && *in <= '7')
                        {
                            c = c << 3 | (*in++-'0');
                            if (*in >= '0' && *in <= '7')
                                c = c << 3 | (*in++-'0');
                        }
                        break;
                    }
                case 'x':
                    c = 0;
                    while (isxdigit(*in))
                    {
                        next = *in++;
                        c = c << 4;
                        if (next >= '0' && next <= '9') c |= (next - '0');
                        else if (next >= 'A' && next <= 'F') c |= (next - 'A' + 10);
                        else if (next >= 'a' && next <= 'f') c |= (next - 'a' + 10);
                    }
                    break;
                default:
                    if (errorIfInvalid)
                        throw MakeStringException(1, "unrecognised character escape sequence '\\%c'", next);
                    in--;   // keep it as is.
                    break;
                }
            }
        }
        out.append(c);
    }
}



void FileSprayer::addHeaderFooter(const char * data, unsigned idx, bool before)
{
    StringBuffer expanded;
    //MORE: Should really expand as unicode, so can have unicode control characters.
    decodeCppEscapeSequence(expanded, data, true);

    MemoryBuffer translated;
    convertUtf(translated, getUtfFormatType(tgtFormat.type), expanded.length(), expanded.str(), UtfReader::Utf8);
    //MORE: Convert from utf-8 to target format.
    addHeaderFooter(translated.length(), translated.toByteArray(), idx, before);
}

void FileSprayer::cloneHeaderFooter(unsigned idx, bool isHeader)
{
    PartitionPoint & cur = partition.item(idx);
    FilePartInfo & curSrc = sources.item(cur.whichInput);
    PartitionPoint & next = * new PartitionPoint;
    //NB: headerSize include the size of the xmlHeader; size includes neither header or footers.
    if (isHeader)
        // Set offset to the XML header
        next.inputOffset = curSrc.headerSize - curSrc.xmlHeaderLength;
    else
        //Set offset to the XML footer
        next.inputOffset = curSrc.headerSize + curSrc.size;
    next.inputLength = isHeader ? curSrc.xmlHeaderLength : curSrc.xmlFooterLength;
    next.outputLength = needToCalcOutput() ? next.inputLength : 0;
    next.whichInput = cur.whichInput;
    next.whichOutput = cur.whichOutput;
    if (isHeader)
        partition.add(next, idx);
    else
        partition.add(next, idx+1);
}

void FileSprayer::addPrefix(size32_t len, const void * data, unsigned idx, PartitionPointArray & partitionWork)
{
    //Merge header and original partition item into partitionWork array
    PartitionPoint & header = createLiteral(len, data, idx);
    partitionWork.append(header);

    PartitionPoint &  partData = partition.item(idx);
    partitionWork.append(OLINK(partData));
}

void FileSprayer::insertHeaders()
{
    const char * header = options->queryProp("@header");
    const char * footer = options->queryProp("@footer");
    const char * glue = options->queryProp("@glue");
    const char * prefix = options->queryProp(ANprefix);
    bool canKeepHeader = srcFormat.equals(tgtFormat) || !needToCalcOutput();
    bool keepHeader = options->getPropBool("@keepHeader", canKeepHeader) && srcFormat.rowTag;
    if (header || footer || prefix || glue)
        keepHeader = false;

    if (keepHeader && !canKeepHeader)
        throwError(DFTERR_CannotKeepHeaderChangeFormat);

    if (header || footer || keepHeader)
    {
        unsigned idx;
        unsigned curOutput = (unsigned)-1;
        bool footerPending = false;
        for (idx = 0; idx < partition.ordinality(); idx++)
        {
            PartitionPoint & cur = partition.item(idx);
            if (curOutput != cur.whichOutput)
            {
                if (keepHeader)
                {
                    if (footerPending && (idx != 0))
                    {
                        footerPending = false;
                        cloneHeaderFooter(idx-1, false);
                        idx++;
                    }

                    //Don't add a header if there are no records in this part, and coming from more than one source file
                    //If coming from one then we'll be guaranteed to have a correct header in that part.  
                    //If more than one, (and not replicating) then we will have failed to know where the header/footers are for this part.
                    if ((cur.inputLength == 0) && (sources.ordinality() > 1))
                        continue;

                    cloneHeaderFooter(idx, true);
                    footerPending = true;
                    idx++;
                }
                if (footer && (idx != 0))
                {
                    addHeaderFooter(footer, idx-1, false);
                    idx++;
                }
                if (header)
                {
                    addHeaderFooter(header, idx, true);
                    idx++;
                }
                curOutput = cur.whichOutput;
            }
        }
        if (keepHeader && footerPending)
        {
            while (idx && partition.item(idx-1).inputLength == 0)
                idx--;
            if (idx)
            {
                cloneHeaderFooter(idx-1, false);
                idx++;
            }
        }
        if (footer)
        {
            addHeaderFooter(footer, idx-1, false);
            idx++;
        }
    }

    if (glue)
    {
        unsigned idx;
        unsigned curInput = 0;
        unsigned curOutput = 0;
        for (idx = 0; idx < partition.ordinality(); idx++)
        {
            PartitionPoint & cur = partition.item(idx);
            if ((curInput != cur.whichInput) && (curOutput == cur.whichOutput))
            {
                addHeaderFooter(glue, idx, true);
                idx++;
            }
            curInput = cur.whichInput;
            curOutput = cur.whichOutput;
        }
    }

    if (prefix)
    {
        if (!srcFormat.equals(tgtFormat))
            throwError(DFTERR_PrefixCannotTransform);
        if (glue || header || footer)
            throwError(DFTERR_PrefixCannotAlsoAddHeader);

        PartitionPointArray partitionWork;
        MemoryBuffer filePrefix;
        filePrefix.setEndian(__LITTLE_ENDIAN);
        for (unsigned idx = 0; idx < partition.ordinality(); idx++)
        {
            PartitionPoint & cur = partition.item(idx);
            filePrefix.clear();

            const char * finger = prefix;
            while (finger)
            {
                StringAttr command;
                const char * comma = strchr(finger, ',');
                if (comma)
                {
                    command.set(finger, comma-finger);
                    finger = comma+1;
                }
                else
                {
                    command.set(finger);
                    finger = NULL;
                }

                command.toUpperCase();
                if (memcmp(command, "FILENAME", 8) == 0)
                {
                    StringBuffer filename;
                    cur.inputName.split(NULL, NULL, &filename, &filename);
                    if (command[8] == ':')
                    {
                        unsigned maxLen = atoi(command+9);
                        filename.padTo(maxLen);
                        filePrefix.append(maxLen, filename.str());
                    }
                    else
                    {
                        filePrefix.append((unsigned)filename.length());
                        filePrefix.append(filename.length(), filename.str());
                    }
                }
                else if ((memcmp(command, "FILESIZE", 8) == 0) || (command.length() == 2))
                {
                    const char * format = command;
                    if (memcmp(format, "FILESIZE", 8) == 0)
                    {
                        if (format[8] == ':')
                            format = format+9;
                        else
                            format = "L4";
                    }

                    bool bigEndian;
                    char c = format[0];
                    if (c == 'B')
                        bigEndian = true;
                    else if (c == 'L')
                        bigEndian = false;
                    else
                        throwError1(DFTERR_InvalidPrefixFormat, format);
                    c = format[1];
                    if ((c <= '0') || (c > '8'))
                        throwError1(DFTERR_InvalidPrefixFormat, format);

                    unsigned length = (c - '0');
                    unsigned __int64 value = cur.inputLength;
                    byte temp[8];
                    for (unsigned i=0; i<length; i++)
                    {
                        temp[i] = (byte)value;
                        value >>= 8;
                    }
                    if (value)
                        throwError(DFTERR_PrefixTooSmall);
                    if (bigEndian)
                    {
                        byte temp2[8];
                        _cpyrevn(&temp2, &temp, length);
                        filePrefix.append(length, &temp2);
                    }
                    else
                        filePrefix.append(length, &temp);
                }
                else
                    throwError1(DFTERR_InvalidPrefixFormat, command.get());
            }
            addPrefix(filePrefix.length(), filePrefix.toByteArray(), idx, partitionWork);
        }
        LOG(MCdebugProgress, job, "Publish headers");
        partition.swapWith(partitionWork);
    }
}


bool FileSprayer::needToCalcOutput()
{
    return !usePullOperation() || options->getPropBool(ANverify);
}

unsigned FileSprayer::numParallelConnections(unsigned limit)
{
    unsigned maxConnections = options->getPropInt(ANmaxConnections, limit);
    if ((maxConnections == 0) || (maxConnections > limit)) maxConnections = limit;
    return maxConnections;
}

unsigned FileSprayer::numParallelSlaves()
{
    unsigned numPullers = transferSlaves.ordinality();
    unsigned maxConnections = DEFAULT_MAX_CONNECTIONS;
    unsigned connectOption = options->getPropInt(ANmaxConnections, 0);
    if (connectOption)
        maxConnections = connectOption;
    else if (mirroring && (maxConnections * 3 < numPullers))
        maxConnections = numPullers/3;
    if (maxConnections > numPullers) maxConnections = numPullers;
    return maxConnections;
}

void FileSprayer::performTransfer()
{
    unsigned numSlaves = transferSlaves.ordinality();
    unsigned maxConnections = numParallelSlaves();
    unsigned failure = options->getPropInt("@fail", 0);
    if (failure) maxConnections = 1;

    calibrateProgress();
    numSlavesCompleted = 0;
    if (maxConnections > 1)
        shuffle(transferSlaves);

    if (progressReport)
        progressReport->setRange(getSizeReadAlready(), sizeToBeRead, transferSlaves.ordinality());


    LOG(MCdebugInfo, job, "Begin to transfer parts (%d threads)\n", maxConnections);

    //Throttle maximum number of concurrent transfers by starting n threads, and
    //then waiting for one to complete before going on to the next
    lastProgressTick = msTick();
    Semaphore sem;
    unsigned goIndex;
    for (goIndex=0; goIndex<maxConnections; goIndex++)
        transferSlaves.item(goIndex).go(sem);

    //MORE: Should abort early if we get an error on one of the transfers...
    //      to do that we will need a queue of completed pullers.
    for (; !error && goIndex<numSlaves;goIndex++)
    {
        waitForTransferSem(sem);
        numSlavesCompleted++;
        transferSlaves.item(goIndex).go(sem);
    }

    for (unsigned waitCount=0; waitCount<maxConnections;waitCount++)
    {
        waitForTransferSem(sem);
        numSlavesCompleted++;
    }

    if (error)
        throw LINK(error);

    bool ok = true;
    ForEachItemIn(idx3, transferSlaves)
    {
        FileTransferThread & cur = transferSlaves.item(idx3);
        if (!cur.ok)
            ok = false;
    }

    if (!ok) {
        if (isAborting())
            throwError(DFTERR_CopyAborted);
        else
            throwError(DFTERR_CopyFailed);
    }
}

void FileSprayer::pullParts()
{
    bool needCalcCRC = calcCRC();
    LOG(MCdebugInfoDetail, job, "Calculate CRC = %d", needCalcCRC);
    ForEachItemIn(idx, targets)
    {
        FileTransferThread & next = * new FileTransferThread(*this, FTactionpull, targets.item(idx).filename.queryEndpoint(), needCalcCRC, wuid);
        transferSlaves.append(next);
    }

    ForEachItemIn(idx3, partition)
    {
        PartitionPoint & cur = partition.item(idx3);
        if (!filter || filter->includePart(cur.whichOutput))
            transferSlaves.item(cur.whichSlave).addPartition(cur, progress.item(idx3));
    }

    performTransfer();
}


void FileSprayer::pushParts()
{
    bool needCalcCRC = calcCRC();
    ForEachItemIn(idx, sources)
    {
        FileTransferThread & next = * new FileTransferThread(*this, FTactionpush, sources.item(idx).filename.queryEndpoint(), needCalcCRC, wuid);
        transferSlaves.append(next);
    }

    ForEachItemIn(idx3, partition)
    {
        PartitionPoint & cur = partition.item(idx3);
        if (!filter || filter->includePart(cur.whichOutput))
            transferSlaves.item(cur.whichSlave).addPartition(cur, progress.item(idx3));
    }

    performTransfer();
}


void FileSprayer::removeSource()
{
    LOG(MCwarning, job, "Source file removal not yet implemented");
}

bool FileSprayer::restorePartition()
{
    if (allowRecovery && progressTree->getPropBool(ANhasPartition))
    {
        IPropertyTreeIterator * iter = progressTree->getElements(PNpartition);
        ForEach(*iter)
        {
            PartitionPoint & next = * new PartitionPoint;
            next.restore(&iter->query());
            partition.append(next);
        }
        iter->Release();
        return (partition.ordinality() != 0);
    }
    return false;
}

void FileSprayer::savePartition()
{
    if (allowRecovery)
    {
        ForEachItemIn(idx, partition)
        {
            IPropertyTree * child = createPTree(PNpartition, ipt_caseInsensitive);
            partition.item(idx).save(child);
            progressTree->addPropTree(PNpartition, child);
        }
        progressTree->setPropBool(ANhasPartition, true);
    }
}


void FileSprayer::setCopyCompressedRaw()
{
    assertex(compressedInput && compressOutput);
    // encrypt/decrypt keys should be same
    compressedInput = false;
    compressOutput = false;
    calcedInputCRC = true;
    cachedInputCRC = false;
    copyCompressed = true;
}


void FileSprayer::setError(const SocketEndpoint & ep, IException * e)
{
    CriticalBlock lock(errorCS);
    if (!error)
    {
        StringBuffer url;
        ep.getUrlStr(url);
        error.setown(MakeStringException(e->errorCode(), "%s", e->errorMessage(url.append(": ")).str()));
    }
}

void FileSprayer::setPartFilter(IDFPartFilter * _filter)
{
    filter.set(_filter);
}


void FileSprayer::setProgress(IDaftProgress * _progress)
{
    progressReport = _progress;
}


void FileSprayer::setAbort(IAbortRequestCallback * _abort)
{
    abortChecker = _abort;
}

void FileSprayer::setReplicate(bool _replicate)
{
    replicate = _replicate;
}

void FileSprayer::setSource(IDistributedFile * source)
{
    distributedSource.set(source);
    srcAttr.setown(createPTreeFromIPT(&source->queryAttributes()));
    extractSourceFormat(srcAttr);
    unsigned numParts = source->numParts();
    for (unsigned idx=0; idx < numParts; idx++)
    {
        Owned<IDistributedFilePart> curPart = source->getPart(idx);
        RemoteFilename rfn;
        FilePartInfo & next = * new FilePartInfo(curPart->getFilename(rfn));
        next.extractExtra(*curPart);
        if (curPart->numCopies()>1)
            next.mirrorFilename.set(curPart->getFilename(rfn,1));
        // don't set the following here - force to check disk
        //next.size = curPart->getFileSize(true,false);
        //next.psize = curPart->getDiskSize(true,false);
        sources.append(next);
    }

    gatherFileSizes(false);

}


void FileSprayer::setSource(IFileDescriptor * source)
{
    setSource(source, 0, 1);

    //Now get the size of the files directly (to check they exist).  If they don't exist then switch to the backup instead.
    gatherFileSizes(false);
}



void FileSprayer::setSource(IFileDescriptor * source, unsigned copy, unsigned mirrorCopy)
{
    IPropertyTree *attr = &source->queryProperties();
    extractSourceFormat(attr);
    srcAttr.setown(createPTreeFromIPT(&source->queryProperties()));
    extractSourceFormat(srcAttr);

    RemoteFilename filename;
    unsigned numParts = source->numParts();
    for (unsigned idx=0; idx < numParts; idx++)
    {
        if (source->isMulti(idx))
        {
            RemoteMultiFilename multi;
            source->getMultiFilename(idx, copy, multi);
            multi.expandWild();

            ForEachItemIn(i, multi)
            {
                const RemoteFilename &rfn = multi.item(i);
                FilePartInfo & next = * new FilePartInfo(rfn);
                Owned<IPartDescriptor> part = source->getPart(idx);
                next.extractExtra(*part);
                next.size = multi.getSize(i);
                sources.append(next);
            }

            //MORE: Need to extract the backup filenames for mirror files.
        }
        else
        {
            source->getFilename(idx, copy, filename);
            FilePartInfo & next = * new FilePartInfo(filename);
            Owned<IPartDescriptor> part = source->getPart(idx);
            next.extractExtra(*part);
            if (mirrorCopy != (unsigned)-1)
                source->getFilename(idx, mirrorCopy, next.mirrorFilename);

            sources.append(next);
        }
    }

    if (sources.ordinality() == 0)
        LOG(MCuserWarning, unknownJob, "The wildcarded source did not match any filenames");
//      throwError(DFTERR_NoFilesMatchWildcard);
}



void FileSprayer::setSource(IDistributedFilePart * part)
{
    tgtFormat.set(FFTfixed, 1);

    unsigned copy = 0;  
    RemoteFilename rfn;
    sources.append(* new FilePartInfo(part->getFilename(rfn,copy)));
    if (compressedInput)  
    {
        calcedInputCRC = true;
        cachedInputCRC = false;
    }
}



void FileSprayer::setSourceTarget(IFileDescriptor * fd, DaftReplicateMode mode)
{
    extractSourceFormat(&fd->queryProperties());
    tgtFormat.set(srcFormat);

    if (options->getPropBool(ANcrcDiffers, false))
        throwError1(DFTERR_ReplicateOptionNoSupported, "crcDiffers");
    if (options->getPropBool(ANsizedate, false))
        throwError1(DFTERR_ReplicateOptionNoSupported, "sizedate");

    switch (mode)
    {
    case DRMreplicatePrimary:       // doesn't work for multi copies
        setSource(fd, 0);
        setTarget(fd, 1);
        break;
    case DRMreplicateSecondary:     // doesn't work for multi copies
        setSource(fd, 1);
        setTarget(fd, 0);
        break;
    case DRMcreateMissing:          // this does though (but I am not sure works with mult-files)
        gatherMissingSourceTarget(fd);
        break;
    }
    isSafeMode = false;
    mirroring = true;
    replicate = true;

    //Optimize replicating compressed - copy it raw, but it means we can't check the input crc
    assertex(compressOutput == compressedInput);  
    if (compressedInput)
        setCopyCompressedRaw();
}

void FileSprayer::setTargetCompression(bool compress)
{
    compressOutput = compress;
}

void FileSprayer::setTarget(IDistributedFile * target)
{
    distributedTarget.set(target);
    compressOutput = !encryptKey.isEmpty()||target->isCompressed();

    if (tgtFormat.restore(&target->queryAttributes()))
        unknownTargetFormat = false;
    else
    {
        tgtFormat.set(srcFormat);
        if (!unknownSourceFormat)
        {
            DistributedFilePropertyLock lock(target);
            IPropertyTree &curProps = lock.queryAttributes();
            tgtFormat.save(&curProps);
        }
    }

    unsigned copy = 0;
    unsigned numParts = target->numParts();
    if (numParts == 0)
        throwError(DFTERR_NoPartsInDestination);
    for (unsigned idx=0; idx < numParts; idx++)
    {
        Owned<IDistributedFilePart> curPart(target->getPart(idx));
        RemoteFilename rfn;
        TargetLocation & next = * new TargetLocation(curPart->getFilename(rfn,copy));
        targets.append(next);
    }
}


void FileSprayer::setTarget(IFileDescriptor * target, unsigned copy)
{
    if (tgtFormat.restore(&target->queryProperties()))
        unknownTargetFormat = false;
    else
        tgtFormat.set(srcFormat);
    compressOutput = !encryptKey.isEmpty()||target->isCompressed();

    unsigned numParts = target->numParts();
    if (numParts == 0)
        throwError(DFTERR_NoPartsInDestination);
    RemoteFilename filename;
    for (unsigned idx=0; idx < numParts; idx++)
    {
        target->getFilename(idx, copy, filename);
        targets.append(*new TargetLocation(filename));
    }
}

void FileSprayer::updateProgress(const OutputProgress & newProgress)
{
    CriticalBlock block(soFarCrit); 
    lastProgressTick = msTick();
    OutputProgress & curProgress = progress.item(newProgress.whichPartition);

    totalLengthRead += (newProgress.inputLength - curProgress.inputLength);
    curProgress.set(newProgress);
    if (curProgress.tree)
        curProgress.save(curProgress.tree);

    if (newProgress.status != OutputProgress::StatusRenamed)
        updateSizeRead();
}


void FileSprayer::updateSizeRead()
{
    if (progressDone)
        return;
    unsigned nowTick = msTick();
    //MORE: This call shouldn't need to be done so often...
    unsigned __int64 sizeReadSoFar = getSizeReadAlready();
    bool done = sizeReadSoFar == sizeToBeRead;
    if (progressReport)
    {
        // A cheat to get 100% saying all the slaves have completed - should really
        // pass completed information in the progress info, or return the last progress
        // info when a slave is done.
        unsigned numCompleted = (sizeReadSoFar == sizeToBeRead) ? transferSlaves.ordinality() : numSlavesCompleted;
        if (done || (nowTick - lastOperatorTick >= operatorUpdateFrequency))
        {
            progressReport->onProgress(sizeReadSoFar, sizeToBeRead, numCompleted);
            lastOperatorTick = nowTick;
            progressDone = done;
        }
    }
    if (allowRecovery && recoveryConnection)
    {
        if (done || (nowTick - lastSDSTick >= sdsUpdateFrequency))
        {
            recoveryConnection->commit();
            lastSDSTick = nowTick;
            progressDone = done;
        }
    }
}

void FileSprayer::waitForTransferSem(Semaphore & sem)
{
    while (!sem.wait(EXPECTED_RESPONSE_TIME))
    {
        unsigned timeSinceProgress = msTick() - lastProgressTick;
        if (timeSinceProgress > EXPECTED_RESPONSE_TIME)
        {
            LOG(MCwarning, unknownJob, "No response from any slaves in last %d seconds.", timeSinceProgress/1000);

            CriticalBlock block(soFarCrit); 
            StringBuffer list;
            ForEachItemIn(i, transferSlaves)
                transferSlaves.item(i).logIfRunning(list);

            if (timeSinceProgress>RESPONSE_TIME_TIMEOUT)
            {
                //Set an error - the transfer threads will check it after a couple of minutes, and then terminate gracefully
                CriticalBlock lock(errorCS);
                if (!error)
                    error.setown(MakeStringException(RFSERR_TimeoutWaitSlave, RFSERR_TimeoutWaitSlave_Text, list.str()));
            }
        }
    }
}

void FileSprayer::addTarget(unsigned idx, INode * node)
{
    RemoteFilename filename;
    filename.set(sources.item(idx).filename);
    filename.setEp(node->endpoint());

    targets.append(* new TargetLocation(filename));
}

bool FileSprayer::isAborting()
{
    if (aborting || error)
        return true;

    unsigned nowTick = msTick();
    if (abortChecker && (nowTick - lastAbortCheckTick >= abortCheckFrequency))
    {
        if (abortChecker->abortRequested())
        {
            LOG(MCdebugInfo, unknownJob, "Abort requested via callback");
            aborting = true;
        }
        lastAbortCheckTick = nowTick;
    }
    return aborting || error; 
}

const char * FileSprayer::querySplitPrefix()
{
    const char * ret = options->queryProp(ANsplitPrefix);
    if (ret && *ret)
        return ret;
    return NULL;
}

const char * FileSprayer::querySlaveExecutable(const IpAddress &ip, StringBuffer &ret)
{
    const char * slave = queryFixedSlave();
    try {
        queryFtSlaveExecutable(ip, ret);
        if (ret.length())
            return ret.str();
    }
    catch (IException * e) {
        if (!slave||!*slave)
            throw;
        e->Release();
    }
    if (slave)
        ret.append(slave);
    return ret.str();
}


const char * FileSprayer::queryFixedSlave()
{
    return options->queryProp("@slave");
}


void FileSprayer::setTarget(IGroup * target)
{
    tgtFormat.set(srcFormat);

    if (sources.ordinality() != target->ordinality())
        throwError(DFTERR_ReplicateNumPartsDiffer);

    ForEachItemIn(idx, sources)
        addTarget(idx, &target->queryNode(idx));
}



void FileSprayer::setTarget(INode * target)
{
    tgtFormat.set(srcFormat);

    if (sources.ordinality() != 1)
        throwError(DFTERR_ReplicateNumPartsDiffer);

    addTarget(0, target);
}

inline bool nonempty(IPropertyTree *pt, const char *p) { const char *s = pt->queryProp(p); return s&&*s; }

bool FileSprayer::disallowImplicitReplicate()
{
    return options->getPropBool(ANsplit) || 
           querySplitPrefix() ||
           nonempty(options,"@header") || 
           nonempty(options,"@footer") || 
           nonempty(options,"@glue") || 
           nonempty(options,ANprefix) ||
           nonempty(options,ANencryptKey) ||
           nonempty(options,ANdecryptKey);
          
}

void FileSprayer::spray()
{
    if (!allowSplit() && querySplitPrefix())
        throwError(DFTERR_SplitNoSplitClash);

    aindex_t sourceSize = sources.ordinality();
    bool failIfNoSourceFile = options->getPropBool("@failIfNoSourceFile");

    if ((sourceSize == 0) && failIfNoSourceFile)
        throwError(DFTERR_NoFilesMatchWildcard);

    LOG(MCdebugInfo, job, "compressedInput:%d, compressOutput:%d", compressedInput, compressOutput);

    LocalAbortHandler localHandler(daftAbortHandler);

    if (allowRecovery && progressTree->getPropBool(ANcomplete))
    {
        LOG(MCdebugInfo, job, "Command completed successfully in previous invocation");
        return;
    }

    checkFormats();
    checkForOverlap();

    progressTree->setPropBool(ANpull, usePullOperation());

    const char * splitPrefix = querySplitPrefix();
    bool pretendreplicate = false;
    if (!replicate && (sources.ordinality() == targets.ordinality())) 
    {   
        if (srcFormat.equals(tgtFormat) && !disallowImplicitReplicate()) {
            pretendreplicate = true;
            replicate = true;
        }
    }

    if (compressOutput&&!replicate) { 
        PROGLOG("Compress output forcing pull");
        options->setPropBool(ANpull, true);
        allowRecovery = false;
    }


    gatherFileSizes(true);
    if (!replicate||pretendreplicate)
        analyseFileHeaders(!pretendreplicate); // if pretending replicate don't want to remove headers
    afterGatherFileSizes();

    if (compressOutput && !usePullOperation() && !replicate)
        throwError(DFTERR_CannotPushAndCompress);

    if (restorePartition())
    {
        LOG(MCdebugProgress, job, "Partition restored from recovery information");
    }
    else
    {
        LOG(MCdebugProgress, job, "Calculate partition information");
        if (replicate)
            calculateOne2OnePartition();
        else if (!allowSplit())
            calculateNoSplitPartition();
        else if (splitPrefix && *splitPrefix)
            calculateSplitPrefixPartition(splitPrefix);
        else if ((targets.ordinality() == 1) && srcFormat.equals(tgtFormat))
            calculateMany2OnePartition();
        else
            calculateSprayPartition();
        if (partition.ordinality() > PARTITION_RECOVERY_LIMIT)
            allowRecovery = false;
        savePartition();
    }
    assignPartitionFilenames();     // assign source filenames - used in insertHeaders..
    if (!replicate)
    {
        LOG(MCdebugProgress, job, "Insert headers");
        insertHeaders();
    }
    addEmptyFilesToPartition();

    derivePartitionExtra();
    if (partition.ordinality() < 1000)
        displayPartition();
    if (isRecovering)
        displayProgress(progress);

    throwExceptionIfAborting();

    beforeTransfer();
    if (usePullOperation())
        pullParts();
    else
        pushParts();
    afterTransfer();

    //If got here then we have succeeded
    updateTargetProperties();
    StringBuffer copyEventText;     // [logical-source] > [logical-target]
    if (distributedSource)
        copyEventText.append(distributedSource->queryLogicalName());
    copyEventText.append(">");
    if (distributedTarget && distributedTarget->queryLogicalName())
        copyEventText.append(distributedTarget->queryLogicalName());

    //MORE: use new interface to send 'file copied' event
    //LOG(MCevent, unknownJob, EVENT_FILECOPIED, copyEventText.str());
        
    cleanupRecovery();
}

bool FileSprayer::isSameSizeHeaderFooter()
{
    bool retVal = true;
    unsigned whichHeaderInput = 0;
    bool isEmpty = true;
    headerSize = 0;
    footerSize = 0;

    if (sources.ordinality() == 0)
        return retVal;

    ForEachItemIn(idx, partition)
    {
        PartitionPoint & cur = partition.item(idx);
        if (cur.inputLength && (idx+1 == partition.ordinality() || partition.item(idx+1).whichOutput != cur.whichOutput))
        {
            if (isEmpty)
            {
                headerSize = sources.item(whichHeaderInput).xmlHeaderLength;
                footerSize = sources.item(cur.whichInput).xmlFooterLength;
                isEmpty = false;
                continue;
            }

            if (headerSize != sources.item(whichHeaderInput).xmlHeaderLength)
            {
                retVal = false;
                break;
            }

            if (footerSize != sources.item(cur.whichInput).xmlFooterLength)
            {
                retVal = false;
                break;
            }

            if ( idx+1 != partition.ordinality() )
                whichHeaderInput = partition.item(idx+1).whichInput;
        }

    }
    return retVal;
}

void FileSprayer::updateTargetProperties()
{
    Owned<IException> error;
    if (distributedTarget)
    {
        StringBuffer failedParts;
        CRC32Merger partCRC;
        offset_t partLength = 0;
        CRC32Merger totalCRC;
        offset_t totalLength = 0;
        offset_t totalCompressedSize = 0;
        unsigned whichHeaderInput = 0;
        bool sameSizeHeaderFooter = isSameSizeHeaderFooter();
        ForEachItemIn(idx, partition)
        {
            PartitionPoint & cur = partition.item(idx);
            OutputProgress & curProgress = progress.item(idx);

            partCRC.addChildCRC(curProgress.outputLength, curProgress.outputCRC, false);
            totalCRC.addChildCRC(curProgress.outputLength, curProgress.outputCRC, false);
            offset_t physPartLength = curProgress.outputLength;
            if (copyCompressed) {
                FilePartInfo & curSource = sources.item(cur.whichInput);
                partLength = curSource.size;
                totalLength += partLength;
            }
            else {
                partLength += curProgress.outputLength;  // AFAICS this might as well be =
                totalLength += curProgress.outputLength;
            }

            if (idx+1 == partition.ordinality() || partition.item(idx+1).whichOutput != cur.whichOutput)
            {
                Owned<IDistributedFilePart> curPart = distributedTarget->getPart(cur.whichOutput);
                // TODO: Create DistributedFilePropertyLock for parts
                curPart->lockProperties();
                IPropertyTree& curProps = curPart->queryAttributes();

                if (!sameSizeHeaderFooter)
                {
                    FilePartInfo & curHeaderSource = sources.item(whichHeaderInput);
                    curProps.setPropInt(FPheaderLength, curHeaderSource.xmlHeaderLength);

                    FilePartInfo & curFooterSource = sources.item(cur.whichInput);
                    curProps.setPropInt(FPfooterLength, curFooterSource.xmlFooterLength);

                    if ( idx+1 != partition.ordinality() )
                        whichHeaderInput = partition.item(idx+1).whichInput;
                }


                if (calcCRC())
                {
                    curProps.setPropInt(FAcrc, partCRC.get());
                    if (cur.whichInput != (unsigned)-1)
                    {
                        FilePartInfo & curSource = sources.item(cur.whichInput);
                        if (replicate && curSource.hasCRC)
                        {
                            if ((partCRC.get() != curSource.crc)&&(compressedInput==compressOutput))    // if expanding will be different!
                            {
                                if (failedParts.length())
                                    failedParts.append(", ");
                                else
                                    failedParts.append("Output CRC failed to match expected: ");
                                curSource.filename.getPath(failedParts);
                                failedParts.appendf("(%x,%x)",partCRC.get(),curSource.crc);

                            }
                        }
                    }
                }
                else if (compressOutput || copyCompressed)
                    curProps.setPropInt(FAcrc, (int)COMPRESSEDFILECRC);
                if (copyCompressed) // don't know if just compress
                {
                    curProps.setPropInt64(FAcompressedSize, physPartLength);
                    totalCompressedSize += physPartLength;
                }


                curProps.setPropInt64(FAsize, partLength);

                if (compressOutput)
                {
                    curProps.setPropInt64(FAcompressedSize, curProgress.compressedPartSize);

                    totalCompressedSize += curProgress.compressedPartSize;
                }

                TargetLocation & curTarget = targets.item(cur.whichOutput);
                if (!curTarget.modifiedTime.isNull())
                {
                    CDateTime temp;
                    StringBuffer timestr;

                    temp.set(curTarget.modifiedTime);
                    unsigned hour, min, sec, nanosec;
                    temp.getTime(hour, min, sec, nanosec);
                    temp.setTime(hour, min, sec, 0);

                    curProps.setProp("@modified", temp.getString(timestr).str());
                }
                if (replicate && (distributedSource != distributedTarget) )
                {
                    assertex(cur.whichInput != (unsigned)-1);
                    FilePartInfo & curSource = sources.item(cur.whichInput);
                    if (curSource.properties)
                    {
                        Owned<IAttributeIterator> aiter = curSource.properties->getAttributes();
                        //At the moment only clone the topLevelKey indicator (stored in kind), but make it easy to add others.
                        ForEach(*aiter) {
                            const char *aname = aiter->queryName();
                            if (strieq(aname,"@kind")
                                ) {
                                if (!curProps.hasProp(aname))
                                    curProps.setProp(aname,aiter->queryValue());
                            }
                        }
                    }
                }
                curPart->unlockProperties();
                partCRC.clear();
                partLength = 0;
            }
        }

        if (failedParts.length())
            error.setown(MakeStringException(DFTERR_InputOutputCrcMismatch, "%s", failedParts.str()));

        DistributedFilePropertyLock lock(distributedTarget);
        IPropertyTree &curProps = lock.queryAttributes();
        if (calcCRC())
            curProps.setPropInt(FAcrc, totalCRC.get());
        curProps.setPropInt64(FAsize, totalLength);

        if (totalCompressedSize != 0)
            curProps.setPropInt64(FAcompressedSize, totalCompressedSize);

        unsigned rs = curProps.getPropInt(FArecordSize); // set by user
        bool gotrc = false;
        if (rs && (totalLength%rs == 0)) {
            curProps.setPropInt64(FArecordCount,totalLength/(offset_t)rs);
            gotrc = true;
        }

        if (sameSizeHeaderFooter)
        {
            curProps.setPropInt(FPheaderLength, headerSize);
            curProps.setPropInt(FPfooterLength, footerSize);
        }

        if (srcAttr.get() && !mirroring) {
            StringBuffer s;
            // copy some attributes (do as iterator in case we want to change to *exclude* some
            Owned<IAttributeIterator> aiter = srcAttr->getAttributes();
            ForEach(*aiter) {
                const char *aname = aiter->queryName();
                if (!curProps.hasProp(aname)&&
                    ((stricmp(aname,"@job")==0)||
                     (stricmp(aname,"@workunit")==0)||
                     (stricmp(aname,"@description")==0)||
                     (stricmp(aname,"@eclCRC")==0)||
                     (stricmp(aname,"@formatCrc")==0)||
                     (stricmp(aname,"@owner")==0)||
                     ((stricmp(aname,FArecordCount)==0)&&!gotrc) ||
                     ((stricmp(aname,"@blockCompressed")==0)&&copyCompressed) ||
                     ((stricmp(aname,"@rowCompressed")==0)&&copyCompressed)
                     )
                    )
                    curProps.setProp(aname,aiter->queryValue());
            }

            // and simple (top level) elements
            Owned<IPropertyTreeIterator> iter = srcAttr->getElements("*");
            ForEach(*iter) {
                curProps.addPropTree(iter->query().queryName(),createPTreeFromIPT(&iter->query()));
            }
        }
    }
    if (error)
        throw error.getClear();
}



bool FileSprayer::usePullOperation()
{
    if (!calcedPullPush)
    {
        calcedPullPush = true;
        cachedUsePull = calcUsePull();
    }
    return cachedUsePull;
}


bool FileSprayer::canLocateSlaveForNode(const IpAddress &ip)
{
    try
    {
        StringBuffer ret;
        querySlaveExecutable(ip, ret);
        return true;
    }
    catch (IException * e)
    {
        e->Release();
    }
    return false;
}


bool FileSprayer::calcUsePull()
{
    if (allowRecovery && progressTree->hasProp(ANpull))
    {
        bool usePull = progressTree->getPropBool(ANpull);
        LOG(MCdebugInfo, job, "Pull = %d from recovery", (int)usePull);
        return usePull;
    }

    if (sources.ordinality() == 0)
        return true;

    if (options->getPropBool(ANpull, false))
    {
        LOG(MCdebugInfo, job, "Use pull since explicitly specified");
        return true;
    }
    if (options->getPropBool(ANpush, false))
    {
        LOG(MCdebugInfo, job, "Use push since explicitly specified");
        return false;
    }

    ForEachItemIn(idx2, sources)
    {
        if (!sources.item(idx2).canPush())
        {
            StringBuffer s;
            sources.item(idx2).filename.queryIP().getIpText(s);
            LOG(MCdebugInfo, job, "Use pull operation because %s cannot push", s.str());
            return true;
        }
    }
    if (!canLocateSlaveForNode(sources.item(0).filename.queryIP()))
    {
        StringBuffer s;
        sources.item(0).filename.queryIP().getIpText(s);
        LOG(MCdebugInfo, job, "Use pull operation because %s doesn't appear to have an ftslave", s.str());
        return true;
    }

    ForEachItemIn(idx, targets)
    {
        if (!targets.item(idx).canPull())
        {
            StringBuffer s;
            targets.item(idx).queryIP().getIpText(s);
            LOG(MCdebugInfo, job, "Use push operation because %s cannot pull", s.str());
            return false;
        }
    }

    if (!canLocateSlaveForNode(targets.item(0).queryIP()))
    {
        StringBuffer s;
        targets.item(0).queryIP().getIpText(s);
        LOG(MCdebugInfo, job, "Use push operation because %s doesn't appear to have an ftslave", s.str());
        return false;
    }

    //Use push if going to a single node.
    if ((targets.ordinality() == 1) && (sources.ordinality() > 1))
    {
        LOG(MCdebugInfo, job, "Use push operation because going to a single node from many");
        return false;
    }

    LOG(MCdebugInfo, job, "Use pull operation as default");
    return true;
}


extern DALIFT_API IFileSprayer * createFileSprayer(IPropertyTree * _options, IPropertyTree * _progress, IRemoteConnection * recoveryConnection, const char *wuid)
{
    return new FileSprayer(_options, _progress, recoveryConnection, wuid);
}


/*
Parameters:
1. A list of target locations (machine+drive?) (and possibly a number for each)
2. A list of source locations [derived from logical file]
3. Information on the source and target formats
3. A mask for the parts that need to be copied. [recovery is special case of this]

Need to
a) Start servers on machines that cannot be accessed directly [have to be running anyway]
b) Work out how the file is going to be partioned
  1. Find out the sizes of all the files.
  2. Calculation partion points -
     For each source file pass [thisoffset, totalsize, thissize, startPoint?], and returns a list of
     numbered partion points.
     Two calls: calcPartion() and retreivePartion() to allow for multithreading on variable length.
     A. If variable length
        Start servers on each of the source machines
        Query each server for partion information (which walks file).
     * If N->N copy don't need to calculate the partion, can do it one a 1:1 mapping.
       E.g. copy variable to blocked format with one block per variable.
c) Save partion information for quick/consistent recovery
d) Start servers on each of the targets or source for push to non-accessible
e) Start pulling/pushing
   Each saves flag when complete for recovery

*/

//----------------------------------------------------------------------------


void testPartitions()
{
    unsigned sizes[] = { 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 
                         100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                         100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                         100, 100, 100, 100, 100, 100, 100, 100, 100, 100,
                         10,
    };
    unsigned parts = _elements_in(sizes);
    unsigned offsets[_elements_in(sizes)];
    unsigned targetParts = 20;
    unsigned recordSize = 20;

    unsigned totalSize =0;
    unsigned idx;
    for (idx = 0; idx < parts; idx++)
    {
        offsets[idx] = totalSize;
        totalSize += sizes[idx];
    }

    PartitionPointArray results;
    for (idx = 0; idx < parts; idx++)
    {
        CFixedPartitioner partitioner(recordSize);
        partitioner.setPartitionRange(totalSize, offsets[idx], sizes[idx], 0, targetParts);
        partitioner.calcPartitions(NULL);
        partitioner.getResults(results);
    }

    ForEachItemIn(idx2, results)
        results.item(idx2).display();
}




/*

  MORE:
  * What about multiple parts for a source file - what should we do with them?
    Ignore?  Try if 
  * Pushing - how do we manage it?
    A. Copy all at once.
        1. For simple non-translation easy to copy all at once.
        2. For others, could hook up a translator so it only calculates the target size.
           Problem is it is a reasonably complex interaction with the partitioner.
           Easier to implement, but not as efficient, as a separate pass.
           - Optimize for variable to VBX.
    B. Copy a chunk at a time
        1. The first source for each chunk write in parallel, followed by the next.
        -  okay if not all writing to a single large file.
  * Unreachable machines
    1. Can I use message passing?
    2. Mock up + test code [ need multi threading access ].
    3. Implement an exists primitive.
    4. How do I distinguish machines?
  * Main program needs to survive if slave nodes die.
  * Asynchronus calls + avoiding the thread switching for notifications?

  * Code for replicating parts
    - set up as a copy from fixed1 to fixed1, which partition matching sources.

*/

