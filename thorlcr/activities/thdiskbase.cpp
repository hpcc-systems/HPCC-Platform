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

#include "rmtfile.hpp"
#include "dadfs.hpp"

#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"

#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "thexception.hpp"

#include "eclhelper.hpp" // tmp for IHThorArg interface
#include "thdiskbase.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/thdiskbase.cpp $ $Id: thdiskbase.cpp 65337 2011-06-10 18:02:00Z ghalliday $");

CDiskReadMasterBase::CDiskReadMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
{
    hash = NULL;
    inputProgress.setown(new ProgressInfo);
}

void CDiskReadMasterBase::init()
{
    IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *) queryHelper();
    Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helper->getFileName(), 0 != ((TDXtemporary|TDXjobtemp) & helper->getFlags()), 0 != (TDRoptional & helper->getFlags()), true);

    if (file)
    {
        if (file->numParts() > 1)
            fileDesc.setown(getConfiguredFileDescriptor(*file));
        else
            fileDesc.setown(file->getFileDescriptor());
        if (container.queryLocal() || helper->canMatchAny()) // if local, assume may match
        {
            bool local;
            if (0 == (TDXtemporary & helper->getFlags())) // don't add temp files
            {
                queryThorFileManager().noteFileRead(container.queryJob(), file);
                local = container.queryLocal();
            }
            else
                local = false;
            mapping.setown(getFileSlaveMaps(file->queryLogicalName(), *fileDesc, container.queryJob().queryUserDescriptor(), container.queryJob().querySlaveGroup(), local, false, hash, file->querySuperFile()));
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
        validateFile(file);
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
                Owned<IException> e = MakeActivityWarning(&container, TE_EncryptionMismatch, "Ignoring encryption key provided as file '%s' was not published as encrypted", helper->getFileName());
                container.queryJob().fireException(e);
            }
        }
        else if (encrypted)
            throw MakeActivityException(this, 0, "File '%s' was published as encrypted but no encryption key provided", helper->getFileName());
    }
}

void CDiskReadMasterBase::serializeSlaveData(MemoryBuffer &dst, unsigned slave)
{
    IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *) queryHelper();
    dst.append(helper->getFileName());
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

void CDiskReadMasterBase::done()
{
    IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *) queryHelper();
    fileDesc.clear();
    if (!abortSoon) // in case query has relinquished control of file usage to another query (e.g. perists) 
    {
        if (0 != (helper->getFlags() & TDXupdateaccessed))
            queryThorFileManager().updateAccessTime(container.queryJob(), helper->getFileName());
    }
}

void CDiskReadMasterBase::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CMasterActivity::deserializeStats(node, mb);
    rowcount_t progress;
    mb.read(progress);
    inputProgress->set(node, progress);
}

void CDiskReadMasterBase::getXGMML(unsigned idx, IPropertyTree *edge)
{
    CMasterActivity::getXGMML(idx, edge);
    inputProgress->processInfo();
    StringBuffer label;
    label.append("@inputProgress");
    edge->setPropInt64(label.str(), inputProgress->queryTotal());
}

/////////////////


void CWriteMasterBase::publish()
{
    if (published) return;
    published = true;
    if (!(diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp)))
    {
        StringBuffer scopedName;
        queryThorFileManager().addScope(container.queryJob(), diskHelperBase->getFileName(), scopedName);
        updateActivityResult(container.queryJob().queryWorkUnit(), diskHelperBase->getFlags(), diskHelperBase->getSequence(), scopedName.str(), recordsProcessed);
    }

    IPropertyTree &props = fileDesc->queryProperties();
    props.setPropInt64("@recordCount", recordsProcessed);
    if (0 == (diskHelperBase->getFlags() & TDXtemporary) || container.queryJob().queryUseCheckpoints())
    {
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
    container.queryTempHandler()->registerFile(diskHelperBase->getFileName(), container.queryOwner().queryGraphId(), diskHelperBase->getTempUsageCount(), TDXtemporary & diskHelperBase->getFlags(), getDiskOutputKind(diskHelperBase->getFlags()), &clusters);
    if (!dlfn.isExternal())
    {
        bool mangle = 0 != (diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp));
        queryThorFileManager().publish(container.queryJob(), diskHelperBase->getFileName(), mangle, *fileDesc, NULL, targetOffset);
    }
}

CWriteMasterBase::CWriteMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
{
    publishReplicatedDone = !globals->getPropBool("@replicateAsync", true);
    replicateProgress.setown(new ProgressInfo);
    diskHelperBase = (IHThorDiskWriteArg *)queryHelper();
    targetOffset = 0;
}

void CWriteMasterBase::deserializeStats(unsigned node, MemoryBuffer &mb)
{
    CMasterActivity::deserializeStats(node, mb);
    unsigned repPerc;
    mb.read(repPerc);
    replicateProgress->set(node, repPerc);
}

void CWriteMasterBase::getXGMML(IWUGraphProgress *progress, IPropertyTree *node)
{
    CMasterActivity::getXGMML(progress, node);
    if (publishReplicatedDone)
    {
        replicateProgress->processInfo();
        replicateProgress->addAttribute(node, "replicatedPercentage", replicateProgress->queryAverage());
    }
}

void CWriteMasterBase::preStart(size32_t parentExtractSz, const byte *parentExtract)
{
    CMasterActivity::preStart(parentExtractSz, parentExtract);
    if (TAKdiskwrite == container.getKind())
    {
        if (0 == ((TDXvarfilename|TDXtemporary|TDXjobtemp) & diskHelperBase->getFlags()))
        {
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), diskHelperBase->getFileName(), false, true);
            if (file)
            {
                if (0 == (TDWextend+TDWoverwrite & diskHelperBase->getFlags()))
                    throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
                checkSuperFileOwnership(*file);
            }
        }
    }
}

void CWriteMasterBase::init()
{
    published = false;
    recordsProcessed = 0;
    dlfn.set(diskHelperBase->getFileName());
    if (diskHelperBase->getFlags() & TDWextend)
    {
        assertex(0 == (diskHelperBase->getFlags() & (TDXtemporary|TDXjobtemp)));
        Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), diskHelperBase->getFileName(), false, true);
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
        while (diskHelperBase->queryCluster(idx))
            clusters.append(diskHelperBase->queryCluster(idx++));
        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), diskHelperBase->getFileName(), clusters, groups);
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), diskHelperBase->getFileName(), clusters, groups, overwriteok, diskHelperBase->getFlags()));
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
        if (TAKdiskwrite == container.getKind() && (0 != (diskHelperBase->getFlags() & TDXtemporary)) && container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
        { // do early, becasue this will be local act. and will not come back to master until end of owning graph.
            publish();
        }
    }
}

void CWriteMasterBase::serializeSlaveData(MemoryBuffer &dst, unsigned slave)
{
    dst.append(diskHelperBase->getFileName());
    if (diskHelperBase->getFlags() & TDXtemporary)
    {
        unsigned usageCount = container.queryJob().queryWorkUnit().queryFileUsage(diskHelperBase->getFileName());
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
    publish();
    if (TAKdiskwrite == container.getKind() && (0 != (diskHelperBase->getFlags() & TDXtemporary)) && container.queryOwner().queryOwner()) // I am in a child query
    {
        published = false;
        recordsProcessed = 0;
    }
}

void CWriteMasterBase::slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
{
    if (dlfn.isExternal())
        return;
    if (mb.length()) // if 0 implies aborted out from this slave.
    {
        rowcount_t slaveProcessed;
        mb.read(slaveProcessed);
        recordsProcessed += slaveProcessed;

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

const void *getAggregate(CActivityBase &activity, unsigned partialResults, IRowInterfaces &rowIf, IHThorCompoundAggregateExtra &aggHelper, mptag_t mpTag)
{
    // JCSMORE - pity this isn't common routine with similar one in aggregate, but helper is not common
    CThorRowArray slaveResults;
    slaveResults.ensure(partialResults);
    unsigned _partialResults = partialResults;
    while (_partialResults--)
    {
        CMessageBuffer mb;
        rank_t sender;
        if (!activity.receiveMsg(mb, RANK_ALL, mpTag, &sender)) return false;
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
    size32_t sz;
    bool first = true;
    _partialResults = 0;
    for (;_partialResults<partialResults; _partialResults++)
    {
        const void *partialResult = slaveResults.item(_partialResults);
        if (partialResult)
        {
            if (first)
            {
                first = false;
                sz = cloneRow(result, slaveResults.item(_partialResults), rowIf.queryRowMetaData());
            }
            else
                sz = aggHelper.mergeAggregate(result, partialResult);
        }
    }
    if (first)
        sz = aggHelper.clearAggregate(result);
    return result.finalizeRowClear(sz);
}
