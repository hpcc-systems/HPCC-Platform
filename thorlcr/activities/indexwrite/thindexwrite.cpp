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

#include "thindexwrite.ipp"
#include "thexception.hpp"

#include "dasess.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"

#include "eclrtl.hpp"
#include "thorfile.hpp"

class IndexWriteActivityMaster : public CMasterActivity
{
    rowcount_t recordsProcessed;
    offset_t fileSize;
    Owned<IFileDescriptor> fileDesc;
    bool buildTlk, isLocal, singlePartKey;
    StringArray clusters;
    mptag_t mpTag2;
    bool refactor;
    Owned<ProgressInfo> replicateProgress;
    bool publishReplicatedDone;
    CDfsLogicalFileName dlfn;

public:
    IndexWriteActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        replicateProgress.setown(new ProgressInfo);
        publishReplicatedDone = !globals->getPropBool("@replicateAsync", true);
        recordsProcessed = 0;
        refactor = singlePartKey = isLocal = false;
        mpTag2 = TAG_NULL;
    }
    ~IndexWriteActivityMaster()
    {
        if (TAG_NULL != mpTag2)
            container.queryJob().freeMPTag(mpTag2);
    }
    void init()
    {
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper();
        dlfn.set(helper->getFileName());
        isLocal = 0 != (TIWlocal & helper->getFlags());
        unsigned maxSize = helper->queryDiskRecordSize()->getMinRecordSize();
        if (maxSize >= 0x8000)
            throw MakeActivityException(this, 0, "Index minimum record length (%d) exceeds 32767 internal limit", maxSize);

        singlePartKey = 0 != (helper->getFlags() & TIWsmall) || dlfn.isExternal();
        unsigned idx=0;
        while (helper->queryCluster(idx))
            clusters.append(helper->queryCluster(idx++));
        IArrayOf<IGroup> groups;
        if (singlePartKey)
        {
            isLocal = true;
            buildTlk = false;
        }
        else if (!isLocal || globals->getPropBool("@buildLocalTlks", true))
            buildTlk = true;

        fillClusterArray(container.queryJob(), helper->getFileName(), clusters, groups);
        unsigned restrictedWidth = 0;
        if (TIWhaswidth & helper->getFlags())
        {
            restrictedWidth = helper->getWidth();
            if (restrictedWidth > container.queryJob().querySlaves())
                throw MakeActivityException(this, 0, "Unsupported, can't refactor to width(%d) larger than host cluster(%d)", restrictedWidth, container.queryJob().querySlaves());
            else if (restrictedWidth < container.queryJob().querySlaves())
            {
                if (!isLocal)
                    throw MakeActivityException(this, 0, "Unsupported, refactoring to few parts only supported for local indexes.");
                assertex(!singlePartKey);
                unsigned gwidth = groups.item(0).ordinality();
                if (0 != container.queryJob().querySlaves() % gwidth)
                    throw MakeActivityException(this, 0, "Unsupported, refactored target size (%d) must be factor of thor cluster width (%d)", groups.item(0).ordinality(), container.queryJob().querySlaves());
                if (0 == restrictedWidth)
                    restrictedWidth = gwidth;
                ForEachItemIn(g, groups)
                {
                    IGroup &group = groups.item(g);
                    if (gwidth != groups.item(g).ordinality())
                        throw MakeActivityException(this, 0, "Unsupported, cannot output multiple refactored widths, targeting cluster '%s' and '%s'", clusters.item(0), clusters.item(g));
                    if (gwidth != restrictedWidth)
                        groups.replace(*group.subset((unsigned)0, restrictedWidth), g);
                }
                refactor = true;
            }
        }
        else
            restrictedWidth = singlePartKey?1:container.queryJob().querySlaves();
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), helper->getFileName(), clusters, groups, 0 != (TIWoverwrite & helper->getFlags()), 0, buildTlk, restrictedWidth));
        if (buildTlk)
            fileDesc->queryPart(fileDesc->numParts()-1)->queryProperties().setProp("@kind", "topLevelKey");

        if (helper->getDistributeIndexName())
        {
            assertex(!isLocal);
            buildTlk = false;
            Owned<IDistributedFile> _f = queryThorFileManager().lookup(container.queryJob(), helper->getDistributeIndexName());
            checkFormatCrc(this, _f, helper->getFormatCrc(), true);
            IDistributedFile *f = _f->querySuperFile();
            if (!f) f = _f;
            Owned<IDistributedFilePart> existingTlk = f->getPart(f->numParts()-1);
            if (!existingTlk->queryProperties().hasProp("@kind") || 0 != stricmp("topLevelKey", existingTlk->queryProperties().queryProp("@kind")))
                throw MakeActivityException(this, 0, "Cannot build new key '%s' based on non-distributed key '%s'", helper->getFileName(), helper->getDistributeIndexName());
            IPartDescriptor *tlkDesc = fileDesc->queryPart(fileDesc->numParts()-1);
            IPropertyTree &props = tlkDesc->queryProperties();
            if (existingTlk->queryProperties().hasProp("@size"))
                props.setPropInt64("@size", existingTlk->queryProperties().getPropInt64("@size"));
            if (existingTlk->queryProperties().hasProp("@fileCrc"))
                props.setPropInt64("@fileCrc", existingTlk->queryProperties().getPropInt64("@fileCrc"));
            if (existingTlk->queryProperties().hasProp("@modified"))
                props.setProp("@modified", existingTlk->queryProperties().queryProp("@modified"));
        }
        
        StringBuffer datasetName;
        fileSize = 0;
        const char *dname = helper->getDatasetName();
        if (dname)
        {
            if (dname[0] == '~')
                datasetName.append(dname+1);
            else
            {               
                datasetName.append(container.queryJob().queryScope());
                if (datasetName.length())
                    datasetName.append("::");
                datasetName.append(dname);
            }

            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(datasetName.str(), container.queryJob().queryUserDescriptor());
            if (df)
                fileSize = df->queryProperties().getPropInt64("@size", 0);
        }

        // Fill in some logical file properties here
        IPropertyTree &props = fileDesc->queryProperties();
#if 0   // not sure correct record size to put in yet
        IRecordSize *irecsize =((IHThorIndexWriteArg *) baseHelper)->queryDiskRecordSize();
        if (irecsize && (irecsize->isFixedSize()))
            props.setPropInt("@recordSize", irecsize->getRecordSize(NULL));
#endif
        const char *rececl= helper->queryRecordECL();
        if (rececl&&*rececl)
            props.setProp("ECL", rececl);
        void * layoutMetaBuff;
        size32_t layoutMetaSize;
        if(helper->getIndexLayout(layoutMetaSize, layoutMetaBuff))
        {
            props.setPropBin("_record_layout", layoutMetaSize, layoutMetaBuff);
            rtlFree(layoutMetaBuff);
        }
        mpTag = container.queryJob().allocateMPTag();
        mpTag2 = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper(); 
        dst.append(mpTag);  // used to build TLK on node1
        dst.append(mpTag2); // counts for moxie OR used to send all recs to node 1 in singlePartKey
        if (slave < fileDesc->numParts()-(buildTlk?1:0)) // if false - due to mismatch width fitting - fill in with a blank entry
        {
            dst.append(true);
            IPartDescriptor *partDesc = fileDesc->queryPart(slave);
            dst.append(helper->getFileName());
            partDesc->serialize(dst);
        }
        else
            dst.append(false);

        dst.append(fileSize);
        dst.append(singlePartKey);
        dst.append(refactor);
        if (!singlePartKey)
        {
            dst.append(buildTlk);
            if (0==slave)
            {
                if (buildTlk)
                {
                    IPartDescriptor *tlkDesc = fileDesc->queryPart(fileDesc->numParts()-1);
                    tlkDesc->serialize(dst);
                }
                else if (!isLocal)
                {
                    assertex(helper->getDistributeIndexName());
                    IPartDescriptor *tlkDesc = fileDesc->queryPart(fileDesc->numParts()-1);
                    tlkDesc->serialize(dst);
                    Owned<IDistributedFile> _f = queryThorFileManager().lookup(container.queryJob(), helper->getDistributeIndexName());
                    IDistributedFile *f = _f->querySuperFile();
                    if (!f) f = _f;
                    Owned<IDistributedFilePart> existingTlk = f->getPart(f->numParts()-1);

                    dst.append(existingTlk->numCopies());
                    unsigned c;
                    for (c=0; c<existingTlk->numCopies(); c++)
                    {
                        RemoteFilename rf;
                        existingTlk->getFilename(rf, c);
                        rf.serialize(dst);
                    }
                }
            }
        }
    }
    void done()
    {
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper();
        StringBuffer scopedName;
        queryThorFileManager().addScope(container.queryJob(), helper->getFileName(), scopedName);
        updateActivityResult(container.queryJob().queryWorkUnit(), helper->getFlags(), helper->getSequence(), scopedName.str(), recordsProcessed);

        // MORE - add in the extra entry somehow
        if (helper->getFileName())
        {
            IPropertyTree &props = fileDesc->queryProperties();
            props.setPropInt64("@recordCount", recordsProcessed);
            props.setProp("@kind", "key");
            setExpiryTime(props, helper->getExpiryDays());
            if (TIWupdate & helper->getFlags())
            {
                unsigned eclCRC;
                unsigned __int64 totalCRC;
                helper->getUpdateCRCs(eclCRC, totalCRC);
                props.setPropInt("@eclCRC", eclCRC);
                props.setPropInt64("@totalCRC", totalCRC);
            }
            props.setPropInt("@formatCrc", helper->getFormatCrc());
            if (isLocal)
                props.setPropBool("@local", true);
            container.queryTempHandler()->registerFile(helper->getFileName(), container.queryOwner().queryGraphId(), 0, false, WUFileStandard, &clusters);
            if (!dlfn.isExternal())
                queryThorFileManager().publish(container.queryJob(), helper->getFileName(), false, *fileDesc);
        }
    }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        if (mb.length()) // if 0 implies aborted out from this slave.
        {
            rowcount_t r;
            mb.read(r);
            recordsProcessed += r;
            if (!singlePartKey || 0 == slaveIdx)
            {
                IPartDescriptor *partDesc = fileDesc->queryPart(slaveIdx);
                offset_t size;
                mb.read(size);
                CDateTime modifiedTime(mb);
                IPropertyTree &props = partDesc->queryProperties();
                props.setPropInt64("@size", size);
                StringBuffer dateStr;
                props.setProp("@modified", modifiedTime.getString(dateStr).str());
                unsigned crc;
                mb.read(crc);
                props.setPropInt64("@fileCrc", crc);
                if (!singlePartKey && 0 == slaveIdx && buildTlk)
                {
                    IPartDescriptor *partDesc = fileDesc->queryPart(fileDesc->numParts()-1);
                    IPropertyTree &props = partDesc->queryProperties();
                    mb.read(crc);
                    props.setPropInt64("@fileCrc", crc);
                    if (buildTlk)
                    {
                        mb.read(size);
                        CDateTime modifiedTime(mb);
                        props.setPropInt64("@size", size);
                        props.setProp("@modified", modifiedTime.getString(dateStr.clear()).str());
                    }
                }
            }
        }
    }
    void process()
    {
        CMasterActivity::process();
    }
    void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag2);
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *) queryHelper();
        if (0 == (TIWvarfilename & helper->getFlags()))
        {
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helper->getFileName(), false, true);
            if (file)
            {
                if (0 == (TIWoverwrite & helper->getFlags()))
                    throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
                checkSuperFileOwnership(*file);
            }
        }
    }
    void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        unsigned repPerc;
        mb.read(repPerc);
        replicateProgress->set(node, repPerc);
    }
    void getXGMML(IWUGraphProgress *progress, IPropertyTree *node)
    {
        CMasterActivity::getXGMML(progress, node);
        if (publishReplicatedDone)
        {
            replicateProgress->processInfo();
            replicateProgress->addAttribute(node, "replicatedPercentage", replicateProgress->queryAverage());
        }
    }
};

CActivityBase *createIndexWriteActivityMaster(CMasterGraphElement *container)
{
    return new IndexWriteActivityMaster(container);
}
