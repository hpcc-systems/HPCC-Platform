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

#include "jfile.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsocket.hpp"
#include "jsort.hpp"
#include "jtime.hpp"

#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "rtlkey.hpp"
#include "eclhelper.hpp" // tmp for IHThor..Arg interfaces.

#include "thbufdef.hpp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thsortu.hpp"
#include "thexception.hpp"
#include "thactivityutil.ipp"
#include "commonext.hpp"

#include "thdiskbaseslave.ipp"

void getPartsMetaInfo(ThorDataLinkMetaInfo &metaInfo, unsigned nparts, IPartDescriptor **partDescs, CDiskPartHandlerBase *partHandler)
{
    ThorDataLinkMetaInfo *metaInfos = new ThorDataLinkMetaInfo[nparts];
    struct ownedMetaInfos
    {
        ThorDataLinkMetaInfo *&metaInfos;
        ownedMetaInfos(ThorDataLinkMetaInfo *&_metaInfos) : metaInfos(_metaInfos) { }
        ~ownedMetaInfos() { delete [] metaInfos; }
    } omi(metaInfos);
    unsigned __int64 sizeTotal = 0;
    unsigned p=0;
    for (; p<nparts; p++)
    {
        initMetaInfo(metaInfos[p]);
        partHandler->getMetaInfo(metaInfos[p], partDescs[p]);
        sizeTotal += partDescs[p]->queryProperties().getPropInt64("@size");
    }
    calcMetaInfoSize(metaInfo, metaInfos, nparts);
    if (!metaInfo.unknownRowsOutput && !metaInfo.canReduceNumRows && !metaInfo.canIncreaseNumRows)
        metaInfo.byteTotal = sizeTotal;
}

//////////////////////////////////////////////

CDiskPartHandlerBase::CDiskPartHandlerBase(CDiskReadSlaveActivityBase &_activity) 
    : activity(_activity), closedPartFileStats(diskReadPartStatistics)
{
    which = 0;
    eoi = false;
    kindStr = activityKindStr(activity.queryContainer().getKind());
    compressed = blockCompressed = firstInGroup = false;
}

void CDiskPartHandlerBase::setPart(IPartDescriptor *_partDesc)
{
    stop(); // close previous if open
    partDesc.set(_partDesc);
    compressed = partDesc->queryOwner().isCompressed(&blockCompressed);
    if (NULL != activity.eexp.get())
        compressed = true;
    fileBaseOffset = partDesc->queryProperties().getPropInt64("@offset");

    which = partDesc->queryPartIndex();
    if (0 != (activity.helper->getFlags() & TDRfilenamecallback)) // only get/serialize if using virtual file name fields
    {
        IFileDescriptor &fileDesc = partDesc->queryOwner();
        ISuperFileDescriptor *superFDesc = fileDesc.querySuperFileDescriptor();
        if (superFDesc)
        {
            unsigned subfile;
            unsigned lnum;
            if (superFDesc->mapSubPart(which, subfile, lnum))
                logicalFilename.set(activity.queryLogicalFilename(subfile));
            else
                logicalFilename.set("UNKNOWN"); // shouldn't happen, but will prevent query fault if did.
        }
        else if (1 == activity.subfileLogicalFilenames.ordinality())
            logicalFilename.set(activity.queryLogicalFilename(0));
        else
            logicalFilename.set(activity.logicalFilename);
    }
    else
        logicalFilename.set(activity.logicalFilename);
    eoi = false;
    firstInGroup = true;

    activity.helper->setCallback(this); // NB, if we were to have >1 of these objects, would prob. need separate helper instances also
    open();
}

void CDiskPartHandlerBase::open() 
{
    unsigned location;
    StringBuffer filePath;
    if (!(globals->getPropBool("@autoCopyBackup", !isContainerized())?ensurePrimary(&activity, *partDesc, iFile, location, filePath):getBestFilePart(&activity, *partDesc, iFile, location, filePath, &activity)))
    {
        StringBuffer locations;
        IException *e = MakeActivityException(&activity, TE_FileNotFound, "No physical file part for logical file %s, found at given locations: %s (Error = %d)", activity.logicalFilename.get(), getFilePartLocations(*partDesc, locations).str(), GetLastError());
        IERRLOG(e, "CDiskPartHandlerBase::open()");
        throw e;
    }
    filename.set(iFile->queryFilename());
    ActPrintLog(&activity, "%s[part=%d]: reading physical file '%s' (logical file = %s)", kindStr, which, filePath.str(), activity.logicalFilename.get());
    if (activity.checkFileDates)
    {
        CDateTime createTime, modifiedTime, accessedTime;
        iFile->getTime(&createTime, &modifiedTime, &accessedTime);
        const char *descModTimeStr = partDesc->queryProperties().queryProp("@modified");
        CDateTime descModTime;
        descModTime.setString(descModTimeStr);
        if (!descModTime.equals(modifiedTime, false))
        {
            StringBuffer diskTimeStr;
            ActPrintLog(&activity, "WARNING: file (%s); modified date stamps on disk (%s) are not equal to published modified data (%s)", filePath.str(), modifiedTime.getString(diskTimeStr).str(), descModTimeStr);
        }
    }

    ActPrintLog(&activity, thorDetailedLogLevel, "%s[part=%d]: Base offset to %" I64F "d", kindStr, which, fileBaseOffset);

    if (compressed)
    {
        ActPrintLog(&activity, "Reading %s compressed file: %s", (NULL != activity.eexp.get())?"encrypted":blockCompressed?"block":"row", filename.get());
    }

    // The following call will likely read the size of the file.  That adds some latency when reading remote
    // (especially when using the api?).  Worth thinking about whether there is a better alternative.
    offset_t expectedSize, actualSize;
    if (!doesPhysicalMatchMeta(*partDesc, *iFile, expectedSize, actualSize))
        throw MakeActivityException(&activity, 0, "File size mismatch: file %s was supposed to be %" I64F "d bytes but appears to be %" I64F "d bytes", iFile->queryFilename(), expectedSize, actualSize);
}

void CDiskPartHandlerBase::stop()
{
    if (!iFile)
        return;
    CRC32 fileCRC;
    close(fileCRC);
    iFile.clear();
}



// IThorDiskCallback impl.
unsigned __int64 CDiskPartHandlerBase::getFilePosition(const void * row)
{
    return getLocalOffset() + fileBaseOffset;
}

unsigned __int64 CDiskPartHandlerBase::getLocalFilePosition(const void * row)
{
    return makeLocalFposOffset(partDesc->queryPartIndex(), getLocalOffset());
}

const char * CDiskPartHandlerBase::queryLogicalFilename(const void * row)
{
    return logicalFilename;
}

//////////////////////////////////////////////

CDiskReadSlaveActivityBase::CDiskReadSlaveActivityBase(CGraphElementBase *_container, IHThorArg *_helper) : CSlaveActivity(_container, diskReadActivityStatistics)
{
    if (_helper)
        baseHelper.set(_helper);
    helper = (IHThorDiskReadBaseArg *)queryHelper();
    reInit = 0 != (helper->getFlags() & (TDXvarfilename|TDXdynamicfilename));
    markStart = gotMeta = false;
    checkFileDates = getOptBool(THOROPT_CHECK_FILE_DATES, checkFileDates);
}

// IThorSlaveActivity
void CDiskReadSlaveActivityBase::init(MemoryBuffer &data, MemoryBuffer &slaveData)
{
    subfileLogicalFilenames.kill();
    partDescs.kill();
    data.read(logicalFilename);
    unsigned subfiles;
    data.read(subfiles);
    unsigned s=0;
    for (; s<subfiles; s++)
    {
        StringAttr subfile;
        data.read(subfile);
        subfileLogicalFilenames.append(subfile);
    }
    data.read(fileTableStart);
    unsigned parts;
    data.read(parts);
    if (parts)
    {
        deserializePartFileDescriptors(data, partDescs);

        if (helper->getFlags() & TDXtemporary)
        {
            // put temp files in temp dir
            if (!container.queryJob().queryUseCheckpoints())
                partDescs.item(0).queryOwner().setDefaultDir(queryTempDir());
        }
        else
        {
            if ((TDXjobtemp & helper->getFlags())==0)
            {
                ISuperFileDescriptor *super = partDescs.item(0).queryOwner().querySuperFileDescriptor();
                setupSpace4FileStats(fileTableStart, reInit, super!=nullptr, super?super->querySubFiles():0, diskReadRemoteStatistics);
            }
        }
    }
    gotMeta = false; // if variable filename and inside loop, need to invalidate cached meta
}
void CDiskReadSlaveActivityBase::mergeFileStats(IPartDescriptor *partDesc, IExtRowStream *partStream)
{
    if (fileStats.size()>0)
    {
        ISuperFileDescriptor * superFDesc = partDesc->queryOwner().querySuperFileDescriptor();
        if (superFDesc)
        {
            unsigned subfile, lnum;
            if(superFDesc->mapSubPart(partDesc->queryPartIndex(), subfile, lnum))
                mergeStats(*fileStats[subfile+fileTableStart], partStream);
        }
        else
            mergeStats(*fileStats[fileTableStart], partStream);
    }
}

const char *CDiskReadSlaveActivityBase::queryLogicalFilename(unsigned index)
{
    return subfileLogicalFilenames.item(index);
}

void CDiskReadSlaveActivityBase::start()
{
    PARENT::start();
    markStart = true;
    unsigned encryptedKeyLen;
    void *encryptedKey;
    helper->getEncryptKey(encryptedKeyLen, encryptedKey);
    if (0 != encryptedKeyLen)
    {
        bool dfsEncrypted = partDescs.ordinality() && partDescs.item(0).queryOwner().queryProperties().getPropBool("@encrypted");
        if (dfsEncrypted) // otherwise ignore (warning issued by master)
            eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
        memset(encryptedKey, 0, encryptedKeyLen);
        free(encryptedKey);
    }
}

void CDiskReadSlaveActivityBase::kill()
{
    if (markStart && !abortSoon && 0 != (helper->getFlags() & TDXtemporary) && !container.queryJob().queryUseCheckpoints())
    {
        if (1 == partDescs.ordinality() && !partDescs.item(0).queryOwner().queryProperties().getPropBool("@pausefile"))
        {
            IPartDescriptor &partDesc = partDescs.item(0);
            RemoteFilename rfn;
            partDesc.getFilename(0, rfn);
            StringBuffer locationName;
            rfn.getLocalPath(locationName);
            container.queryTempHandler()->deregisterFile(locationName.str());
        }
    }
    markStart = false;
    CSlaveActivity::kill();
}

IThorRowInterfaces * CDiskReadSlaveActivityBase::queryProjectedDiskRowInterfaces()
{
    if (!projectedDiskRowIf)
        projectedDiskRowIf.setown(createRowInterfaces(helper->queryProjectedDiskRecordSize()));
    return projectedDiskRowIf;
}


void CDiskReadSlaveActivityBase::gatherActiveStats(CRuntimeStatisticCollection &activeStats) const
{
    PARENT::gatherActiveStats(activeStats);
    if (partHandler)
        partHandler->gatherStats(activeStats);
}

void CDiskReadSlaveActivityBase::serializeStats(MemoryBuffer &mb)
{
    PARENT::serializeStats(mb);
    if (partDescs.ordinality())
    {
        mb.append((unsigned)fileStats.size());
        for (auto &stats: fileStats)
            stats->serialize(mb);
    }
}


/////////////////

void CDiskWriteSlaveActivityBase::open()
{
    start();
    if (dlfn.isExternal() && !firstNode())
    {
        if (hasLookAhead(0))
            startLookAhead(0);
        else
            setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), PROCESS_SMART_BUFFER_SIZE, ::canStall(input), grouped, RCUNBOUND, NULL), false);
        if (!rfsQueryParallel)
        {
            ActPrintLog("Blocked, waiting for previous part to complete write");
            CMessageBuffer msg;
            if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag))
                return;
            rowcount_t prevRows;
            msg.read(prevRows);
            msg.read(tempExternalName); // reuse temp filename, last node will rename
            ActPrintLog("Previous write row count = %" RCPF "d", prevRows);
        }
    }
    processed = THORDATALINK_STARTED;

    unsigned helperFlags = diskHelperBase->getFlags();
    bool extend = 0 != (helperFlags & TDWextend);
    if (extend)
        ActPrintLog("Extending file %s", fName.get());

    bool external = dlfn.isExternal();
    bool query = dlfn.isQuery();
    if (query && compress)
        UNIMPLEMENTED;

    unsigned twFlags = external ? TW_External : 0;
    if (query || (external && !firstNode()))
        twFlags |= TW_Direct;
    if (!external || (!query && lastNode()))
        twFlags |= TW_RenameToPrimary;
    if (extend||(external&&!query))
        twFlags |= TW_Extend;
    if (helperFlags & TDXtemporary)
        twFlags |= TW_Temporary;
    if (helperFlags & TDXjobtemp)
        twFlags |= TW_JobTemp;
    if (helperFlags & TDWpersist)
        twFlags |= TW_Persist;

    Owned<IFileIO> partOutputIO = createMultipleWrite(this, *partDesc, twFlags, compress, ecomp, this, &abortSoon, (external&&!query) ? &tempExternalName : NULL);

    {
        CriticalBlock block(statsCs);
        outputIO.setown(partOutputIO.getClear());
    }

    if (compress)
    {
        ActPrintLog("Performing compression on output file: %s", fName.get());
    }

    Owned<IFileIOStream> stream;
    if (wantRaw())
    {
        //GH->JCS: Why isn't this done inside createMultipleWrite?  Why is it not done for the row writer?
        //I suspect that after switching to a write stream interface this code could be cleaned up further.
        StringBuffer planeName;
        partDesc->queryOwner().queryClusterNum(0)->getGroupName(planeName);
        size32_t blockedIoSize = getBlockedFileIOSize(planeName, (size32_t)-1);

        outraw.setown(createBufferedIOStream(outputIO, blockedIoSize));
        stream.set(outraw);
    }
    else
    {
        //GH->JCS this will not take the blocked io size into consideration, but that can be fixed when reafactored
        stream.setown(createIOStream(outputIO));
        unsigned rwFlags = 0;
        if (grouped)
            rwFlags |= rw_grouped;
        out.setown(createRowWriter(stream, ::queryRowInterfaces(input), rwFlags));
    }

    //If writing to an external file, each of the slaves appends to th file in turn.
    if (extend || (external && !query))
        stream->seek(0,IFSend);
    ActPrintLog("Created output stream for %s", fName.get());
}

void CDiskWriteSlaveActivityBase::removeFiles()
{
    if (!fName.length())
        return;
    Owned<IFile> primary = createIFile(fName);
    try { primary->remove(); }
    catch (IException *e) { ActPrintLogEx(&queryContainer(), e, thorlog_null, MCwarning, "Failed to remove file: %s", fName.get()); }
    catch (CATCHALL) { ActPrintLogEx(&queryContainer(), thorlog_null, MCwarning, "Failed to remove: %s", fName.get()); }
}

void CDiskWriteSlaveActivityBase::close()
{
    try
    {
        if (outputIO)
        {
            if (out)
            {
                uncompressedBytesWritten = out->getPosition();
                if (!abortSoon)
                    out->flush();
                out.clear();
            }
            else if (outraw)
            {
                outraw->flush();
                uncompressedBytesWritten = outraw->tell();
                outraw.clear();
            }

            Owned<IFileIO> tmpFileIO;
            {
                CriticalBlock block(statsCs);
                // ensure it is released/destroyed after releasing crit, since the IFileIO might involve a final copy and take considerable time.
                tmpFileIO.setown(outputIO.getClear());
            }
            tmpFileIO->close(); // NB: close now, do not rely on close in dtor
            mergeStats(inactiveStats, tmpFileIO, diskWriteRemoteStatistics); // needs to be after close() for complete stats
            partDiskSize = tmpFileIO->getStatistic(StSizeDiskWrite);
            if (tmpUsage)
                tmpUsage->setSize(partDiskSize);
        }

        if (!rfsQueryParallel && dlfn.isExternal() && !lastNode())
        {
            rowcount_t rows = processed & THORDATALINK_COUNT_MASK;
            ActPrintLog("External write done, signalling next (row count = %" RCPF "d)", rows);
            CMessageBuffer msg;
            msg.append(rows);
            msg.append(tempExternalName);
            queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag);
        }
    }
    catch (IException *e)
    { 
        ActPrintLogEx(&queryContainer(), e, thorlog_null, MCwarning, "Error closing file: %s", fName.get());
        abortSoon = true;
        removeFiles();
        throw e;
    }
    if (abortSoon)
        removeFiles();
}

CDiskWriteSlaveActivityBase::CDiskWriteSlaveActivityBase(CGraphElementBase *container)
    : ProcessSlaveActivity(container, diskWriteActivityStatistics)
{
    diskHelperBase = static_cast <IHThorDiskWriteArg *> (queryHelper());
    reInit = 0 != (diskHelperBase->getFlags() & (TDXvarfilename|TDXdynamicfilename));
    grouped = false;
    compress = false;
    uncompressedBytesWritten = 0;
    partDiskSize = UINT_MAX; // unset
    replicateDone = 0;
    usageCount = 0;
    rfsQueryParallel = false;
}

void CDiskWriteSlaveActivityBase::init(MemoryBuffer &data, MemoryBuffer &slaveData)
{
    StringAttr logicalFilename;
    data.read(logicalFilename);
    dlfn.set(logicalFilename);
    if (diskHelperBase->getFlags() & TDXtemporary)
        data.read(usageCount);
    partDesc.setown(deserializePartFileDescriptor(data));

    // put temp files in temp dir
    if ((diskHelperBase->getFlags() & TDXtemporary) && (!container.queryJob().queryUseCheckpoints()))
        partDesc->queryOwner().setDefaultDir(queryTempDir());

    if (dlfn.isExternal())
    {
        mpTag = container.queryJobChannel().deserializeMPTag(data);
        if (dlfn.isQuery() && (0 != container.queryJob().getWorkUnitValueInt("rfsqueryparallel", 0)))
            rfsQueryParallel = true;
    }
    if (0 != (diskHelperBase->getFlags() & TDXgrouped))
        grouped = true;
}

void CDiskWriteSlaveActivityBase::abort()
{
    ProcessSlaveActivity::abort();
    if (!rfsQueryParallel && dlfn.isExternal() && !firstNode())
        cancelReceiveMsg(queryJobChannel().queryMyRank()-1, mpTag);
}

// NB: always called within statsCs crit
void CDiskWriteSlaveActivityBase::gatherActiveStats(CRuntimeStatisticCollection &activeStats) const
{
    PARENT::gatherActiveStats(activeStats);
    if (outputIO)
    {
        mergeStats(activeStats, outputIO, diskWriteRemoteStatistics);
        if (tmpUsage) // Update tmpUsage file size (Needed to calc inter-graph spill stats)
            tmpUsage->setSize(outputIO->getStatistic(StSizeDiskWrite));
    }
    activeStats.setStatistic(StPerReplicated, replicateDone);
}


// ICopyFileProgress
CFPmode CDiskWriteSlaveActivityBase::onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize)
{
    replicateDone = sizeDone ? ((unsigned)(sizeDone*100/totalSize)) : 0;
    return abortSoon?CFPstop:CFPcontinue;
}

// IThorSlaveProcess overloaded methods
void CDiskWriteSlaveActivityBase::kill()
{
    ProcessSlaveActivity::kill();
    if (abortSoon)
        removeFiles();
}

void CDiskWriteSlaveActivityBase::process()
{
    compress = partDesc->queryOwner().isCompressed();
    void *ekey;
    size32_t ekeylen;
    diskHelperBase->getEncryptKey(ekeylen,ekey);
    if (ekeylen!=0)
    {
        ecomp.setown(createAESCompressor256(ekeylen,ekey));
        memset(ekey,0,ekeylen);
        free(ekey);
        compress = true;
    }
    uncompressedBytesWritten = 0;
    partDiskSize = UINT_MAX; // unset
    replicateDone = 0;
    StringBuffer tmpStr;
    fName.set(getPartFilename(*partDesc, 0, tmpStr).str());
    if (diskHelperBase->getFlags() & TDXtemporary && !container.queryJob().queryUseCheckpoints())
        tmpUsage = container.queryTempHandler()->registerFile(fName, container.queryOwner().queryGraphId(), usageCount, true);

    ActPrintLog("handling fname : %s", fName.get());

    try
    {
        open();
        assertex(out||outraw);
        write();
    }
    catch (IException *)
    {
        abortSoon = true;
        try { close(); }
        catch (IException *e)
        {
            IWARNLOG(e, "close()"); // NB: primary exception will be rethrown
            e->Release();
        }
        throw;
    }
    catch (CATCHALL)
    {
        abortSoon = true;
        try { close(); }
        catch (IException *e)
        {
            IWARNLOG(e, "close()");
            e->Release();
        }
        throw;
    }
    close();
    ActPrintLog("Wrote %" RCPF "d records", processed & THORDATALINK_COUNT_MASK);
}

void CDiskWriteSlaveActivityBase::endProcess()
{
    if (processed & THORDATALINK_STARTED)
    {
        stop();
        processed |= THORDATALINK_STOPPED;
    }
}

void CDiskWriteSlaveActivityBase::processDone(MemoryBuffer &mb)
{
    if (abortSoon)
        return;
    rowcount_t _processed = processed & THORDATALINK_COUNT_MASK;
    Owned<IFile> ifile = createIFile(fName);
    if (tempExternalName.isEmpty() || firstNode()) // else, previous workers have written to same file, cannot verify stat. against IFile::size
        partDiskSize = verifyFileSize(ifile, partDiskSize);
    mb.append(_processed).append(compress?uncompressedBytesWritten:partDiskSize).append(partDiskSize);

    CDateTime createTime, modifiedTime, accessedTime;
    ifile->getTime(&createTime, &modifiedTime, &accessedTime);
    // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
    unsigned hour, min, sec, nanosec;
    modifiedTime.getTime(hour, min, sec, nanosec);
    modifiedTime.setTime(hour, min, sec, 0);
    modifiedTime.serialize(mb);
}

/////////////

rowcount_t getFinalCount(CSlaveActivity &activity)
{
    rowcount_t totalCount = 0;
    CMessageBuffer mb;
    if (activity.receiveMsg(mb, 0, activity.queryMpTag()))
        mb.read(totalCount);
    return totalCount;
}

void sendPartialCount(CSlaveActivity &activity, rowcount_t partialCount)
{
    CMessageBuffer msg;
    msg.append(partialCount);
    if (!activity.queryJobChannel().queryJobComm().send(msg, 0, activity.queryMpTag(), 5000))
        throw MakeThorException(0, "Failed to give partial result to master");
}


const void *CPartialResultAggregator::getResult()
{
    CMessageBuffer mb;
    if (activity.receiveMsg(mb, 0, activity.queryMpTag()))
    {
        if (mb.length())
        {
            CThorStreamDeserializerSource ds(mb.length(), mb.readDirect(mb.length()));
            RtlDynamicRowBuilder rowBuilder(activity.queryRowAllocator());
            size32_t sz = activity.queryRowDeserializer()->deserialize(rowBuilder,ds);
            return rowBuilder.finalizeRowClear(sz);
        }
    }
    return NULL;
}

void CPartialResultAggregator::sendResult(const void *row)
{
    CMessageBuffer mb;
    if (row)
    {
        CMemoryRowSerializer mbs(mb);
        activity.queryRowSerializer()->serialize(mbs,(const byte *)row);
    }
    if (!activity.queryJobChannel().queryJobComm().send(mb, 0, activity.queryMpTag(), 5000))
        throw MakeThorException(0, "Failed to give partial result to master");
}

void CPartialResultAggregator::cancelGetResult()
{
    activity.cancelReceiveMsg(0, activity.queryMpTag());
}

