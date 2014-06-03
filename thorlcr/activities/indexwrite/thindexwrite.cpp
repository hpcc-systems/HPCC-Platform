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

#include "thindexwrite.ipp"
#include "thexception.hpp"

#include "dasess.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"

#include "ctfile.hpp"
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
    IHThorIndexWriteArg *helper;

public:
    IndexWriteActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = (IHThorIndexWriteArg *)queryHelper();
        replicateProgress.setown(new ProgressInfo);
        publishReplicatedDone = !globals->getPropBool("@replicateAsync", true);
        recordsProcessed = 0;
        refactor = singlePartKey = isLocal = false;
        mpTag2 = TAG_NULL;
        reInit = (0 != (TIWvarfilename & helper->getFlags()));
    }
    ~IndexWriteActivityMaster()
    {
        if (TAG_NULL != mpTag2)
            container.queryJob().freeMPTag(mpTag2);
    }
    virtual void init()
    {
        CMasterActivity::init();
        OwnedRoxieString fname(helper->getFileName());
        dlfn.set(fname);
        isLocal = 0 != (TIWlocal & helper->getFlags());
        IOutputMetaData * diskSize = helper->queryDiskRecordSize();
        unsigned minSize = diskSize->getMinRecordSize();
        if (minSize > KEYBUILD_MAXLENGTH)
            throw MakeActivityException(this, 0, "Index minimum record length (%d) exceeds %d internal limit", minSize, KEYBUILD_MAXLENGTH);
        unsigned maxSize;
        if (diskSize->isVariableSize())
        {
            if (TIWmaxlength & helper->getFlags())
                maxSize = helper->getMaxKeySize();
            else
                maxSize = KEYBUILD_MAXLENGTH; //Current default behaviour, could be improved in the future
        }
        else
            maxSize = diskSize->getFixedSize();
        if (maxSize > KEYBUILD_MAXLENGTH)
            throw MakeActivityException(this, 0, "Index maximum record length (%d) exceeds %d internal limit. Maximum size = %d, try setting index MAXLENGTH", maxSize, KEYBUILD_MAXLENGTH, minSize);

        singlePartKey = 0 != (helper->getFlags() & TIWsmall) || dlfn.isExternal();
        clusters.kill();
        unsigned idx=0;
        while (true)
        {
            OwnedRoxieString cluster(helper->getCluster(idx));
            if(!cluster)
                break;
            clusters.append(cluster);
            idx++;
        }
        IArrayOf<IGroup> groups;
        if (singlePartKey)
        {
            isLocal = true;
            buildTlk = false;
        }
        else if (!isLocal || globals->getPropBool("@buildLocalTlks", true))
            buildTlk = true;

        fillClusterArray(container.queryJob(), fname, clusters, groups);
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
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), fname, clusters, groups, 0 != (TIWoverwrite & helper->getFlags()), 0, buildTlk, restrictedWidth));
        if (buildTlk)
            fileDesc->queryPart(fileDesc->numParts()-1)->queryProperties().setProp("@kind", "topLevelKey");

        OwnedRoxieString diName(helper->getDistributeIndexName());
        if (diName.get())
        {
            assertex(!isLocal);
            buildTlk = false;
            Owned<IDistributedFile> _f = queryThorFileManager().lookup(container.queryJob(), diName);
            checkFormatCrc(this, _f, helper->getFormatCrc(), true);
            IDistributedFile *f = _f->querySuperFile();
            if (!f) f = _f;
            Owned<IDistributedFilePart> existingTlk = f->getPart(f->numParts()-1);
            if (!existingTlk->queryAttributes().hasProp("@kind") || 0 != stricmp("topLevelKey", existingTlk->queryAttributes().queryProp("@kind")))
                throw MakeActivityException(this, 0, "Cannot build new key '%s' based on non-distributed key '%s'", fname.get(), diName.get());
            IPartDescriptor *tlkDesc = fileDesc->queryPart(fileDesc->numParts()-1);
            IPropertyTree &props = tlkDesc->queryProperties();
            if (existingTlk->queryAttributes().hasProp("@size"))
                props.setPropInt64("@size", existingTlk->queryAttributes().getPropInt64("@size"));
            if (existingTlk->queryAttributes().hasProp("@fileCrc"))
                props.setPropInt64("@fileCrc", existingTlk->queryAttributes().getPropInt64("@fileCrc"));
            if (existingTlk->queryAttributes().hasProp("@modified"))
                props.setProp("@modified", existingTlk->queryAttributes().queryProp("@modified"));
        }
        
        StringBuffer datasetName;
        fileSize = 0;
        OwnedRoxieString dname(helper->getDatasetName());
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
                fileSize = df->queryAttributes().getPropInt64("@size", 0);
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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper(); 
        dst.append(mpTag);  // used to build TLK on node1
        dst.append(mpTag2); // counts for moxie OR used to send all recs to node 1 in singlePartKey
        if (slave < fileDesc->numParts()-(buildTlk?1:0)) // if false - due to mismatch width fitting - fill in with a blank entry
        {
            dst.append(true);
            IPartDescriptor *partDesc = fileDesc->queryPart(slave);
            OwnedRoxieString fname(helper->getFileName());
            dst.append(fname.get());
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
                    OwnedRoxieString diName(helper->getDistributeIndexName());
                    assertex(diName.get());
                    IPartDescriptor *tlkDesc = fileDesc->queryPart(fileDesc->numParts()-1);
                    tlkDesc->serialize(dst);
                    Owned<IDistributedFile> _f = queryThorFileManager().lookup(container.queryJob(), diName);
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
    virtual void done()
    {
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *)queryHelper();
        StringBuffer scopedName;
        OwnedRoxieString fname(helper->getFileName());
        queryThorFileManager().addScope(container.queryJob(), fname, scopedName);
        updateActivityResult(container.queryJob().queryWorkUnit(), helper->getFlags(), helper->getSequence(), scopedName.str(), recordsProcessed);

        // MORE - add in the extra entry somehow
        if (fname.get())
        {
            IPropertyTree &props = fileDesc->queryProperties();
            props.setPropInt64("@recordCount", recordsProcessed);
            props.setProp("@kind", "key");
            if (0 != (helper->getFlags() & TIWexpires))
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
            container.queryTempHandler()->registerFile(fname, container.queryOwner().queryGraphId(), 0, false, WUFileStandard, &clusters);
            if (!dlfn.isExternal())
                queryThorFileManager().publish(container.queryJob(), fname, false, *fileDesc);
        }
        CMasterActivity::done();
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
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag2);
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        IHThorIndexWriteArg *helper = (IHThorIndexWriteArg *) queryHelper();
        if (0 == (TIWvarfilename & helper->getFlags()))
        {
            OwnedRoxieString fname(helper->getFileName());
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), fname, false, true);
            if (file)
            {
                if (0 == (TIWoverwrite & helper->getFlags()))
                    throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
                checkSuperFileOwnership(*file);
            }
        }
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        unsigned repPerc;
        mb.read(repPerc);
        replicateProgress->set(node, repPerc);
    }
    virtual void getXGMML(IWUGraphProgress *progress, IPropertyTree *node)
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
