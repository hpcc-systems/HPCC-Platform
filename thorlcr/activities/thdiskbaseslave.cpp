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

#include "platform.h"

#include "jfile.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsocket.hpp"
#include "jsort.hpp"
#include "jtime.hpp"

#include "dafdesc.hpp"
#include "rtlkey.hpp"
#include "eclhelper.hpp" // tmp for IHThor..Arg interfaces.

#include "thbufdef.hpp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thcrc.hpp"
#include "thsortu.hpp"
#include "thexception.hpp"
#include "thactivityutil.ipp"
#include "commonext.hpp"

#include "thdiskbaseslave.ipp"

void getPartsMetaInfo(ThorDataLinkMetaInfo &metaInfo, CThorDataLink &link, unsigned nparts, IPartDescriptor **partDescs, CDiskPartHandlerBase *partHandler)
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
        link.initMetaInfo(metaInfos[p]);
        partHandler->getMetaInfo(metaInfos[p], partDescs[p]);
        sizeTotal += partDescs[p]->queryProperties().getPropInt64("@size");
    }
    link.calcMetaInfoSize(metaInfo, metaInfos, nparts);
    if (!metaInfo.unknownRowsOutput && !metaInfo.canReduceNumRows && !metaInfo.canIncreaseNumRows)
        metaInfo.byteTotal = sizeTotal;
}

//////////////////////////////////////////////

CDiskPartHandlerBase::CDiskPartHandlerBase(CDiskReadSlaveActivityBase &_activity) 
    : activity(_activity)
{
    checkFileCrc = activity.checkFileCrc;
    which = 0;
    eoi = false;
    kindStr = activityKindStr(activity.queryContainer().getKind());
    progress = 0;
}

void CDiskPartHandlerBase::setPart(IPartDescriptor *_partDesc, unsigned partNoSerialized)
{
    partDesc.set(_partDesc);
    compressed = partDesc->queryOwner().isCompressed(&blockCompressed);
    if (NULL != activity.eexp.get())
        compressed = true;
    checkFileCrc = activity.checkFileCrc?partDesc->getCrc(storedCrc):false;
    fileBaseOffset = partDesc->queryProperties().getPropInt64("@offset");

    if (0 != (activity.helper->getFlags() & TDRfilenamecallback)) // only get/serialize if using virtual file name fields
    {
        IFileDescriptor &fileDesc = partDesc->queryOwner();
        ISuperFileDescriptor *superFDesc = fileDesc.querySuperFileDescriptor();
        if (superFDesc)
        {
            unsigned subfile;
            unsigned lnum;
            if (superFDesc->mapSubPart(partNoSerialized, subfile, lnum))
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
    which = partDesc->queryPartIndex();

    activity.helper->setCallback(this); // NB, if we were to have >1 of these objects, would prob. need separate helper instances also
    open();
}

void CDiskPartHandlerBase::open() 
{
    unsigned location;
    StringBuffer filePath;
    if (!(globals->getPropBool("@autoCopyBackup", true)?ensurePrimary(&activity, *partDesc, iFile, location, filePath):getBestFilePart(&activity, *partDesc, iFile, location, filePath, &activity)))
    {
        StringBuffer locations;
        IException *e = MakeActivityException(&activity, TE_FileNotFound, "No physical file part for logical file %s, found at given locations: %s (Error = %d)", activity.logicalFilename.get(), getFilePartLocations(*partDesc, locations).str(), GetLastError());
        EXCLOG(e, NULL);
        throw e;
    }
    filename.set(iFile->queryFilename());
    ActPrintLog(&activity, "%s[part=%d]: reading physical file '%s' (logical file = %s)", kindStr, which, filePath.str(), activity.logicalFilename.get());
    if (checkFileCrc)
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

    ActPrintLog(&activity, "%s[part=%d]: Base offset to %"I64F"d", kindStr, which, fileBaseOffset);

    if (compressed)
    {
        ActPrintLog(&activity, "Reading %s compressed file: %s", (NULL != activity.eexp.get())?"encrypted":blockCompressed?"block":"row", filename.get());
        if (checkFileCrc)
        {
            checkFileCrc = false;
            if (activity.crcCheckCompressed) // applies to encrypted too, (optional, default off)
            {
                ActPrintLog(&activity, "Calculating crc for file: %s", filename.get());
                unsigned calcCrc = iFile->getCRC();
                // NB: for compressed files should always be ~0
                ActPrintLog(&activity, "Calculated crc = %x, storedCrc = %x", calcCrc, storedCrc);
                if (calcCrc != storedCrc)
                {
                    IThorException *e = MakeActivityException(&activity, TE_FileCrc, "CRC Failure validating compressed file: %s", iFile->queryFilename());
                    e->setAudience(MSGAUD_operator);
                    throw e;
                }
            }
        }
    }
}

void CDiskPartHandlerBase::stop()
{
    if (!eoi)
        checkFileCrc = false; // cannot perform file CRC if diskread has not read whole file.
    CRC32 fileCRC;
    close(fileCRC);
    if (!activity.abortSoon && checkFileCrc)
    {
        ActPrintLog(&activity, "%s[part=%d]: CRC Stored=%x, calculated=%x file(%s)", kindStr, which, storedCrc, fileCRC.get(), filename.get());
        if (fileCRC.get() != storedCrc)
            throw MakeThorOperatorException(TE_FileCrc, "CRC Failure having read file: %s", filename.get());
        checkFileCrc = false;
    }
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

CDiskReadSlaveActivityBase::CDiskReadSlaveActivityBase(CGraphElementBase *_container) : CSlaveActivity(_container)
{
    helper = (IHThorDiskReadBaseArg *)queryHelper();
    crcCheckCompressed = 0 != container.queryJob().getWorkUnitValueInt("crcCheckCompressed", 0);
    gotMeta = false;
    checkFileCrc = !globals->getPropBool("Debug/@fileCrcDisabled");
}

// IThorSlaveActivity
void CDiskReadSlaveActivityBase::init(MemoryBuffer &data, MemoryBuffer &slaveData)
{
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
    unsigned parts;
    data.read(parts);
    if (parts)
    {
        deserializePartFileDescriptors(data, partDescs);
        unsigned encryptedKeyLen;
        void *encryptedKey;
        helper->getEncryptKey(encryptedKeyLen, encryptedKey);
        if (0 != encryptedKeyLen) 
        {
            bool dfsEncrypted = partDescs.item(0).queryOwner().queryProperties().getPropBool("@encrypted");
            if (dfsEncrypted) // otherwise ignore (warning issued by master)
                eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
            memset(encryptedKey, 0, encryptedKeyLen);
            free(encryptedKey);
        }
    }
}

const char *CDiskReadSlaveActivityBase::queryLogicalFilename(unsigned index)
{
    return subfileLogicalFilenames.item(index);
}

void CDiskReadSlaveActivityBase::kill()
{
    if (!abortSoon && 0 != (helper->getFlags() & TDXtemporary) && !container.queryJob().queryUseCheckpoints())
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
    CSlaveActivity::kill();
}

IRowInterfaces * CDiskReadSlaveActivityBase::queryDiskRowInterfaces()
{
    if (!diskRowIf) 
        diskRowIf.setown(createRowInterfaces(helper->queryDiskRecordSize(),queryActivityId(),queryCodeContext()));
    return diskRowIf;
}

void CDiskReadSlaveActivityBase::serializeStats(MemoryBuffer &mb)
{
    CSlaveActivity::serializeStats(mb);
    mb.append(getHandlerProgress());
}


/////////////////

void CDiskWriteSlaveActivityBase::open()
{
    if (dlfn.isExternal() && !firstNode())
    {
        input.setown(createDataLinkSmartBuffer(this, inputs.item(0), PROCESS_SMART_BUFFER_SIZE, isSmartBufferSpillNeeded(this), grouped, RCUNBOUND, NULL, false, &container.queryJob().queryIDiskUsage()));
        startInput(input);
        if (!rfsQueryParallel)
        {
            ActPrintLog("Blocked, waiting for previous part to complete write");
            CMessageBuffer msg;
            if (!receiveMsg(msg, container.queryJob().queryMyRank()-1, mpTag))
                return;
            rowcount_t prevRows;
            msg.read(prevRows);
            msg.read(tempExternalName); // reuse temp filename, last node will rename
            ActPrintLog("Previous write row count = %"RCPF"d", prevRows);
        }
    }
    else
    {
        input.set(inputs.item(0));
        startInput(input);
    }
    processed = THORDATALINK_STARTED;

    bool extend = 0 != (diskHelperBase->getFlags() & TDWextend);
    if (extend)
        ActPrintLog("Extending file %s", fName.get());
    size32_t exclsz = 0;
    calcFileCrc = true;

    bool external = dlfn.isExternal();
    bool query = dlfn.isQuery();
    if (query && compress)
        UNIMPLEMENTED;

    bool direct = query || (external && !firstNode());
    bool rename = !external || (!query && lastNode());
    Owned<IFileIO> iFileIO = createMultipleWrite(this, *partDesc, exclsz, compress, extend||(external&&!query), ecomp, this, direct, rename, &abortSoon, (external&&!query) ? &tempExternalName : NULL);

    if (compress)
    {
        ActPrintLog("Performing row compression on output file: %s", fName.get());
        calcFileCrc = false;
    }
    Owned<IFileIOStream> stream;
    if (wantRaw())
    {
        outraw.setown(createBufferedIOStream(iFileIO));
        stream.set(outraw);
    }
    else
    {
        stream.setown(createIOStream(iFileIO));
        out.setown(createRowWriter(stream,::queryRowSerializer(input),::queryRowAllocator(input),grouped,calcFileCrc,false)); // flushed by close
    }
    CDfsLogicalFileName dlfn;
    dlfn.set(logicalFilename);
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
        if (out) {
            uncompressedBytesWritten = out->getPosition();
            if (calcFileCrc) {
                if (diskHelperBase->getFlags() & TDWextend) {
                    assertex(!"TBD need to merge CRC");
                }   
                else
                    out->flush(&fileCRC);
            }
            else
                out->flush();
            out.clear();
        }
        else if (outraw) {
            outraw->flush();
            outraw.clear();
        }
        if (!rfsQueryParallel && dlfn.isExternal() && !lastNode())
        {
            rowcount_t rows = processed & THORDATALINK_COUNT_MASK;
            ActPrintLog("External write done, signalling next (row count = %"RCPF"d)", rows);
            CMessageBuffer msg;
            msg.append(rows);
            msg.append(tempExternalName);
            container.queryJob().queryJobComm().send(msg, container.queryJob().queryMyRank()+1, mpTag);
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

CDiskWriteSlaveActivityBase::CDiskWriteSlaveActivityBase(CGraphElementBase *container) : ProcessSlaveActivity(container)
{
    grouped = false;
    compress = calcFileCrc = false;
    uncompressedBytesWritten = 0;
    replicateDone = 0;
    usageCount = 0;
    rfsQueryParallel = false;
}

void CDiskWriteSlaveActivityBase::init(MemoryBuffer &data, MemoryBuffer &slaveData)
{
    diskHelperBase = static_cast <IHThorDiskWriteArg *> (queryHelper());

    data.read(logicalFilename);
    dlfn.set(logicalFilename);
    if (diskHelperBase->getFlags() & TDXtemporary)
        data.read(usageCount);
    if (diskHelperBase->getFlags() & TDWextend)
    {
        assertex(!"TBD extended CRC broken");
        unsigned crc;
        if (partDesc->getCrc(crc))
            fileCRC.reset(~crc);
    }
    partDesc.setown(deserializePartFileDescriptor(data));
    if (dlfn.isExternal())
    {
        mpTag = container.queryJob().deserializeMPTag(data);
        if (dlfn.isQuery() && (0 != container.queryJob().getWorkUnitValueInt("rfsqueryparallel", 0)))
            rfsQueryParallel = true;
    }
    if (0 != (diskHelperBase->getFlags() & TDXgrouped))
        grouped = true;
    compress = partDesc->queryOwner().isCompressed();
    void *ekey;
    size32_t ekeylen;
    diskHelperBase->getEncryptKey(ekeylen,ekey);
    if (ekeylen!=0) {
        ecomp.setown(createAESCompressor256(ekeylen,ekey));
        memset(ekey,0,ekeylen);
        free(ekey);
        compress = true;
    }
}

void CDiskWriteSlaveActivityBase::abort()
{
    ProcessSlaveActivity::abort();
    if (!rfsQueryParallel && dlfn.isExternal() && !firstNode())
        cancelReceiveMsg(container.queryJob().queryMyRank()-1, mpTag);
}

void CDiskWriteSlaveActivityBase::serializeStats(MemoryBuffer &mb)
{
    ProcessSlaveActivity::serializeStats(mb);
    mb.append(replicateDone);
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
    calcFileCrc = false;
    uncompressedBytesWritten = 0;
    replicateDone = 0;
    StringBuffer tmpStr;
    fName.set(getPartFilename(*partDesc, 0, tmpStr).str());
    if (diskHelperBase->getFlags() & TDXtemporary && !container.queryJob().queryUseCheckpoints())
        container.queryTempHandler()->registerFile(fName, container.queryOwner().queryGraphId(), usageCount, true);
    try
    {
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
            close();
            throw;
        }
        catch (CATCHALL)
        {
            abortSoon = true;
            close();
            throw;
        }
        close();
    }
    catch (IException *)
    {
        calcFileCrc = false;
        throw;
    }
    catch(CATCHALL)
    {
        calcFileCrc = false;
        throw;
    }
    unsigned crc = compress?~0:fileCRC.get();
    ActPrintLog("Wrote %"RCPF"d records%s", processed & THORDATALINK_COUNT_MASK, calcFileCrc?StringBuffer(", crc=0x").appendf("%X", crc).str() : "");
}

void CDiskWriteSlaveActivityBase::endProcess()
{
    if (processed & THORDATALINK_STARTED)
    {
        stopInput(input);
        processed |= THORDATALINK_STOPPED;
    }
}

void CDiskWriteSlaveActivityBase::processDone(MemoryBuffer &mb)
{
    if (abortSoon)
        return;
    rowcount_t _processed = processed & THORDATALINK_COUNT_MASK;
    Owned<IFile> ifile = createIFile(fName);
    offset_t sz = ifile->size();
    if (-1 != sz)
        container.queryJob().queryIDiskUsage().increase(sz);
    mb.append(_processed).append(compress?uncompressedBytesWritten:sz).append(sz);
    unsigned crc = compress?~0:fileCRC.get();
    mb.append(calcFileCrc?crc:0);

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
    if (!activity.queryContainer().queryJob().queryJobComm().send(msg, 0, activity.queryMpTag(), 5000))
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
    if (!activity.queryContainer().queryJob().queryJobComm().send(mb, 0, activity.queryMpTag(), 5000))
        throw MakeThorException(0, "Failed to give partial result to master");
}

void CPartialResultAggregator::cancelGetResult()
{
    activity.cancelReceiveMsg(0, activity.queryMpTag());
}

