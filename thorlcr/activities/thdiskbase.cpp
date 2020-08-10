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

#include "rmtfile.hpp"
#include "dadfs.hpp"

#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "thmem.hpp"

#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "thexception.hpp"

#include "eclhelper.hpp" // tmp for IHThorArg interface
#include "thdiskbase.ipp"


CDiskReadMasterBase::CDiskReadMasterBase(CMasterGraphElement *info) :
    CMasterActivity(info, diskReadActivityStatistics)
{
    hash = NULL;
}

void CDiskReadMasterBase::init()
{
    CMasterActivity::init();
    IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *) queryHelper();
    bool mangle = 0 != (helper->getFlags() & (TDXtemporary|TDXjobtemp));
    OwnedRoxieString helperFileName = helper->getFileName();
    StringBuffer expandedFileName;
    queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName, mangle);
    fileName.set(expandedFileName);
    reInit = 0 != (helper->getFlags() & (TDXvarfilename|TDXdynamicfilename));

    Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helperFileName, 0 != ((TDXtemporary|TDXjobtemp) & helper->getFlags()), 0 != (TDRoptional & helper->getFlags()), true, container.activityIsCodeSigned());
    if (file)
    {
        if (file->isExternal() && (helper->getFlags() & TDXcompress))
            file->queryAttributes().setPropBool("@blockCompressed", true);
        if (file->numParts() > 1)
            fileDesc.setown(getConfiguredFileDescriptor(*file));
        else
            fileDesc.setown(file->getFileDescriptor());
        validateFile(file);
        if (container.queryLocal() || helper->canMatchAny()) // if local, assume may match
        {
            bool temp = 0 != (TDXtemporary & helper->getFlags());
            bool local;
            if (temp)
                local = false;
            else
                local = container.queryLocal();
            mapping.setown(getFileSlaveMaps(file->queryLogicalName(), *fileDesc, container.queryJob().queryUserDescriptor(), container.queryJob().querySlaveGroup(), local, false, hash, file->querySuperFile()));
            addReadFile(file, temp);
        }
        if (0 != (helper->getFlags() & TDRfilenamecallback)) // only get/serialize if using virtual file name fields
        {
            IDistributedSuperFile *super = file->querySuperFile();
            if (super)
            {
                unsigned numsubs = super->numSubFiles(true);
                unsigned s=0;
                for (; s<numsubs; s++)
                {
                    IDistributedFile &subfile = super->querySubFile(s, true);
                    subfileLogicalFilenames.append(subfile.queryLogicalName());
                }
            }
        }
        void *ekey;
        size32_t ekeylen;
        helper->getEncryptKey(ekeylen,ekey);
        bool encrypted = fileDesc->queryProperties().getPropBool("@encrypted");
        if (0 != ekeylen)
        {
            memset(ekey,0,ekeylen);
            free(ekey);
            if (!encrypted)
            {
                Owned<IException> e = MakeActivityWarning(&container, TE_EncryptionMismatch, "Ignoring encryption key provided as file '%s' was not published as encrypted", fileName.get());
                queryJobChannel().fireException(e);
            }
        }
        else if (encrypted)
            throw MakeActivityException(this, 0, "File '%s' was published as encrypted but no encryption key provided", fileName.get());
    }
}

void CDiskReadMasterBase::serializeSlaveData(MemoryBuffer &dst, unsigned slave)
{
    dst.append(fileName);
    dst.append(subfileLogicalFilenames.ordinality());
    if (subfileLogicalFilenames.ordinality())
    {
        ForEachItemIn(s, subfileLogicalFilenames)
            dst.append(subfileLogicalFilenames.item(s));
    }
    if (mapping)
        mapping->serializeMap(slave, dst);
    else
        CSlavePartMapping::serializeNullMap(dst);
}

/////////////////

void CWriteMasterBase::init()
{
    published = false;
    recordsProcessed = 0;
    bool mangle = 0 != (diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp));
    OwnedRoxieString helperFileName = diskHelperBase->getFileName();
    StringBuffer expandedFileName;
    queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName, mangle);
    fileName.set(expandedFileName);
    dlfn.set(fileName);
    if (diskHelperBase->getFlags() & TDWextend)
    {
        assertex(0 == (diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp)));
        Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helperFileName, false, true, false, container.activityIsCodeSigned());
        if (file.get())
        {
            fileDesc.setown(file->getFileDescriptor());
            queryThorFileManager().noteFileRead(container.queryJob(), file, true);
        }
    }
    if (dlfn.isExternal())
        mpTag = container.queryJob().allocateMPTag(); // used
    if (NULL == fileDesc.get())
    {
        bool overwriteok = 0!=(TDWoverwrite & diskHelperBase->getFlags());

        unsigned idx=0;
        while (true)
        {
            OwnedRoxieString cluster(diskHelperBase->getCluster(idx));
            if(!cluster)
                break;
            clusters.append(cluster);
            idx++;
        }

        if (idx == 0)
        {
            const char * defaultCluster = queryDefaultStoragePlane();
            if (defaultCluster)
                clusters.append(defaultCluster);
        }

        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), fileName, clusters, groups);
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), fileName, clusters, groups, overwriteok, diskHelperBase->getFlags()));
        if (1 == groups.ordinality())
            targetOffset = getGroupOffset(groups.item(0), container.queryJob().querySlaveGroup());
        IPropertyTree &props = fileDesc->queryProperties();
        if (diskHelperBase->getFlags() & (TDWowned|TDXjobtemp|TDXtemporary))
            props.setPropBool("@owned", true);
        if (diskHelperBase->getFlags() & TDWresult)
            props.setPropBool("@result", true);
        const char *rececl= diskHelperBase->queryRecordECL();
        if (rececl&&*rececl)
            props.setProp("ECL", rececl);
        setRtlFormat(props, diskHelperBase->queryDiskRecordSize());

        bool blockCompressed=false;
        void *ekey;
        size32_t ekeylen;
        diskHelperBase->getEncryptKey(ekeylen,ekey);
        if (ekeylen)
        {
            memset(ekey,0,ekeylen);
            free(ekey);
            props.setPropBool("@encrypted", true);
            blockCompressed = true;
        }
        else if (0 != (diskHelperBase->getFlags() & TDWnewcompress) || 0 != (diskHelperBase->getFlags() & TDXcompress))
            blockCompressed = true;
        if (blockCompressed)
            props.setPropBool("@blockCompressed", true);
        props.setProp("@kind", "flat");
        if (((TAKdiskwrite == container.getKind()) || (TAKspillwrite == container.getKind())) &&
                (0 != (diskHelperBase->getFlags() & TDXtemporary)) && container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
        { // do early, because this will be local act. and will not come back to master until end of owning graph.
            publish();
        }
    }
}

void CWriteMasterBase::publish()
{
    if (published) return;
    published = true;
    if (!(diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp)))
        updateActivityResult(container.queryJob().queryWorkUnit(), diskHelperBase->getFlags(), diskHelperBase->getSequence(), fileName, recordsProcessed);

    IPropertyTree &props = fileDesc->queryProperties();
    props.setPropInt64("@recordCount", recordsProcessed);
    if (0 == (diskHelperBase->getFlags() & TDXtemporary) || container.queryJob().queryUseCheckpoints())
    {
        if (0 != (diskHelperBase->getFlags() & TDWexpires))
            setExpiryTime(props, diskHelperBase->getExpiryDays());
        if (TDWupdate & diskHelperBase->getFlags())
        {
            unsigned eclCRC;
            unsigned __int64 totalCRC;
            diskHelperBase->getUpdateCRCs(eclCRC, totalCRC);
            props.setPropInt("@eclCRC", eclCRC);
            props.setPropInt64("@totalCRC", totalCRC);
        }
    }
    if (TDWrestricted & diskHelperBase->getFlags())
        props.setPropBool("restricted", true );
    container.queryTempHandler()->registerFile(fileName, container.queryOwner().queryGraphId(), diskHelperBase->getTempUsageCount(), TDXtemporary & diskHelperBase->getFlags(), getDiskOutputKind(diskHelperBase->getFlags()), &clusters);
    if (!dlfn.isExternal())
    {
        bool temporary = 0 != (diskHelperBase->getFlags()&TDXtemporary);
        if (!temporary && (queryJob().querySlaves() < fileDesc->numParts()))
        {
            // create empty parts for a fileDesc being published that is larger than this clusters
            size32_t recordSize = 0;
            IOutputMetaData *diskRowMeta = diskHelperBase->queryDiskRecordSize()->querySerializedDiskMeta();
            if (diskRowMeta->isFixedSize() && ((TAKdiskwrite == container.getKind()) || (TAKspillwrite == container.getKind())))
            {
                recordSize = diskRowMeta->getMinRecordSize();
                if (0 != (diskHelperBase->getFlags() & TDXgrouped))
                    recordSize += 1;
            }
            unsigned compMethod = COMPRESS_METHOD_LZ4;
            // rowdiff used if recordSize > 0, else fallback to compMethod
            if (getOptBool(THOROPT_COMP_FORCELZW, false))
            {
                recordSize = 0; // by default if fixed length (recordSize set), row diff compression is used. This forces compMethod.
                compMethod = COMPRESS_METHOD_LZW;
            }
            else if (getOptBool(THOROPT_COMP_FORCEFLZ, false))
                compMethod = COMPRESS_METHOD_FASTLZ;
            else if (getOptBool(THOROPT_COMP_FORCELZ4, false))
                compMethod = COMPRESS_METHOD_LZ4;
            else if (getOptBool(THOROPT_COMP_FORCELZ4HC, false))
                compMethod = COMPRESS_METHOD_LZ4HC;
            bool blockCompressed;
            bool compressed = fileDesc->isCompressed(&blockCompressed);
            for (unsigned clusterIdx=0; clusterIdx<fileDesc->numClusters(); clusterIdx++)
            {
                StringBuffer clusterName;
                fileDesc->getClusterGroupName(clusterIdx, clusterName, &queryNamedGroupStore());
                LOG(MCthorDetailedDebugInfo, thorJob, "Creating blank parts for file '%s', cluster '%s'", fileName.get(), clusterName.str());
                unsigned p=0;
                while (p<fileDesc->numParts())
                {
                    if (p == targetOffset)
                        p += queryJob().querySlaves();
                    IPartDescriptor *partDesc = fileDesc->queryPart(p);
                    CDateTime createTime, modifiedTime;
                    for (unsigned c=0; c<partDesc->numCopies(); c++)
                    {
                        RemoteFilename rfn;
                        partDesc->getFilename(c, rfn);
                        StringBuffer path;
                        rfn.getPath(path);
                        try
                        {
                            ensureDirectoryForFile(path.str());
                            OwnedIFile iFile = createIFile(path.str());
                            Owned<IFileIO> iFileIO;
                            if (compressed)
                                iFileIO.setown(createCompressedFileWriter(iFile, recordSize, false, true, NULL, compMethod));
                            else
                                iFileIO.setown(iFile->open(IFOcreate));
                            dbgassertex(iFileIO.get());
                            iFileIO.clear();
                            // ensure copies have matching datestamps, as they would do normally (backupnode expects it)
                            if (partDesc->numCopies() > 1)
                            {
                                if (0 == c)
                                    iFile->getTime(&createTime, &modifiedTime, NULL);
                                else
                                    iFile->setTime(&createTime, &modifiedTime, NULL);
                            }
                        }
                        catch (IException *e)
                        {
                            if (0 == c)
                                throw;
                            Owned<IThorException> e2 = MakeThorException(e);
                            e->Release();
                            e2->setAction(tea_warning);
                            queryJob().fireException(e2);
                        }
                    }
                    partDesc->queryProperties().setPropInt64("@size", 0);
                    p++;
                }
            }
        }
        queryThorFileManager().publish(container.queryJob(), fileName, *fileDesc, NULL);
    }
}

CWriteMasterBase::CWriteMasterBase(CMasterGraphElement *info)
     : CMasterActivity(info, diskWriteActivityStatistics)
{
    diskHelperBase = (IHThorDiskWriteArg *)queryHelper();
    targetOffset = 0;
}

void CWriteMasterBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    CMasterActivity::preStart(parentExtractSz, parentExtract);
    if (TAKdiskwrite == container.getKind())
    {
        if (0 == ((TDXvarfilename|TDXtemporary|TDXjobtemp) & diskHelperBase->getFlags()))
        {
            OwnedRoxieString fname(diskHelperBase->getFileName());
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), fname, false, true, false, container.activityIsCodeSigned());
            if (file)
            {
                if (0 == ((TDWextend+TDWoverwrite) & diskHelperBase->getFlags()))
                    throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
                checkSuperFileOwnership(*file);
            }
        }
    }
}

void CWriteMasterBase::serializeSlaveData(MemoryBuffer &dst, unsigned slave)
{
    OwnedRoxieString fname(diskHelperBase->getFileName());
    dst.append(fileName);
    if (diskHelperBase->getFlags() & TDXtemporary)
    {
        unsigned usageCount = container.queryJob().queryWorkUnit().queryFileUsage(fname);
        if (0 == usageCount) usageCount = diskHelperBase->getTempUsageCount();
        dst.append(usageCount);
    }
    if (dlfn.isExternal())
    {
        fileDesc->queryPart(0)->serialize(dst);
        dst.append(mpTag);
    }
    else
        fileDesc->queryPart(targetOffset+slave)->serialize(dst);
}

void CWriteMasterBase::done()
{
    CMasterActivity::done();
    publish();
    if (((TAKdiskwrite == container.getKind()) || (TAKspillwrite == container.getKind())) && (0 != (diskHelperBase->getFlags() & TDXtemporary)) && container.queryOwner().queryOwner()) // I am in a child query
    {
        published = false;
        recordsProcessed = 0;
    }
}

void CWriteMasterBase::slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
{
    if (mb.length()) // if 0 implies aborted out from this slave.
    {
        rowcount_t slaveProcessed;
        mb.read(slaveProcessed);
        recordsProcessed += slaveProcessed;
        if (dlfn.isExternal())
            return;

        offset_t size, physicalSize;
        mb.read(size);
        mb.read(physicalSize);

        unsigned fileCrc;
        mb.read(fileCrc);
        CDateTime modifiedTime(mb);

        IPartDescriptor *partDesc = fileDesc->queryPart(targetOffset+slaveIdx);
        IPropertyTree &props = partDesc->queryProperties();
        props.setPropInt64("@size", size);
        if (fileDesc->isCompressed())
            props.setPropInt64("@compressedSize", physicalSize);
        props.setPropInt64("@fileCrc", fileCrc);
        StringBuffer timeStr;
        modifiedTime.getString(timeStr);
        props.setProp("@modified", timeStr.str());
        props.setPropInt64("@recordCount", slaveProcessed);
    }
}



/////////////////
rowcount_t getCount(CActivityBase &activity, unsigned partialResults, rowcount_t limit, mptag_t mpTag)
{
    rowcount_t totalCount = 0;
    CMessageBuffer msg;
    while (partialResults--)
    {
        rank_t sender;
        msg.clear();
        if (!activity.receiveMsg(msg, RANK_ALL, mpTag, &sender)) return 0;
        if (activity.queryAbortSoon()) return 0;
        rowcount_t partialCount;
        msg.read(partialCount);
        totalCount += (rowcount_t)partialCount;
        if (totalCount > limit)
            break;
    }
    return totalCount;
}

const void *getAggregate(CActivityBase &activity, unsigned partialResults, IThorRowInterfaces &rowIf, IHThorCompoundAggregateExtra &aggHelper, mptag_t mpTag)
{
    // JCSMORE - pity this isn't common routine with similar one in aggregate, but helper is not common
    CThorExpandingRowArray slaveResults(activity, &activity, ers_allow, stableSort_none, true, partialResults);
    unsigned _partialResults = partialResults;
    while (_partialResults--)
    {
        CMessageBuffer mb;
        rank_t sender;
        if (!activity.receiveMsg(mb, RANK_ALL, mpTag, &sender)) return NULL;
        if (activity.queryAbortSoon()) return 0;
        if (mb.length())
        {
            CThorStreamDeserializerSource ds(mb.length(), mb.readDirect(mb.length()));
            RtlDynamicRowBuilder rowBuilder(rowIf.queryRowAllocator());
            size32_t sz = rowIf.queryRowDeserializer()->deserialize(rowBuilder, ds);
            slaveResults.setRow(sender-1, rowBuilder.finalizeRowClear(sz));
        }
    }
    RtlDynamicRowBuilder result(rowIf.queryRowAllocator(), false);
    size32_t sz = 0;
    bool first = true;
    _partialResults = 0;
    for (;_partialResults<partialResults; _partialResults++)
    {
        const void *partialResult = slaveResults.query(_partialResults);
        if (partialResult)
        {
            if (first)
            {
                first = false;
                sz = cloneRow(result, partialResult, rowIf.queryRowMetaData());
            }
            else
                sz = aggHelper.mergeAggregate(result, partialResult);
        }
    }
    if (first)
        sz = aggHelper.clearAggregate(result);
    return result.finalizeRowClear(sz);
}
