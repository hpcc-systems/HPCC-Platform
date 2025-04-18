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

#include "jiface.hpp"

#include "dadfs.hpp"
#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thkeypatch.ipp"

class CKeyPatchMaster : public CMasterActivity
{
    IHThorKeyPatchArg *helper;
    Owned<IFileDescriptor> newIndexDesc, originalDesc, patchDesc;
    bool local;
    unsigned width;
    StringArray clusters;
    Owned<IDistributedFile> originalIndexFile, patchFile;
    StringAttr originalName, patchName, outputName;

public:
    CKeyPatchMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = (IHThorKeyPatchArg *)queryHelper();
        local = false;
        width = 0;
    }
    virtual void init()
    {
        CMasterActivity::init();
        OwnedRoxieString originalHelperName(helper->getOriginalName());
        OwnedRoxieString patchHelperName(helper->getPatchName());
        OwnedRoxieString outputHelperName(helper->getOutputName());
        StringBuffer expandedFileName;
        queryThorFileManager().addScope(container.queryJob(), originalHelperName, expandedFileName, false);
        originalName.set(expandedFileName);
        queryThorFileManager().addScope(container.queryJob(), patchHelperName, expandedFileName.clear(), false);
        patchName.set(expandedFileName);
        queryThorFileManager().addScope(container.queryJob(), outputHelperName, expandedFileName.clear(), false);
        outputName.set(expandedFileName);

        originalIndexFile.setown(lookupReadFile(originalHelperName, AccessMode::readRandom, false, false, false));
        if (!isFileKey(originalIndexFile))
            throw MakeActivityException(this, ENGINEERR_FILE_TYPE_MISMATCH, "Attempting to read flat file as an index: %s", originalHelperName.get());
        patchFile.setown(lookupReadFile(patchHelperName, AccessMode::readRandom, false, false, false));
        if (isFileKey(patchFile))
            throw MakeActivityException(this, ENGINEERR_FILE_TYPE_MISMATCH, "Attempting to read index as a patch file: %s", patchHelperName.get());
        
        if (originalIndexFile->numParts() != patchFile->numParts())
            throw MakeActivityException(this, TE_KeyPatchIndexSizeMismatch, "Index %s and patch %s differ in width", originalName.get(), patchName.get());
        if (originalIndexFile->querySuperFile() || patchFile->querySuperFile())
            throw MakeActivityException(this, 0, "Patching super files not supported");
        
        width = originalIndexFile->numParts();

        originalDesc.setown(originalIndexFile->getFileDescriptor());
        patchDesc.setown(patchFile->getFileDescriptor());
        local = !hasTLK(*originalIndexFile, this);
        if (!local && width > 1)
            width--; // 1 part == No n distributed / Monolithic key
        if (width > container.queryJob().querySlaves())
            throw MakeActivityException(this, 0, "Unsupported: keypatch(%s, %s) - Cannot patch a key that's wider(%d) than the target cluster size(%d)", originalIndexFile->queryLogicalName(), patchFile->queryLogicalName(), width, container.queryJob().querySlaves());

        StringBuffer defaultCluster;
        if (getDefaultIndexBuildStoragePlane(defaultCluster))
            clusters.append(defaultCluster);
        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), outputName, clusters, groups);
        newIndexDesc.setown(queryThorFileManager().create(container.queryJob(), outputName, clusters, groups, 0 != (KDPoverwrite & helper->getFlags()), 0, !local, width));
        if (!local)
            newIndexDesc->queryPart(newIndexDesc->numParts()-1)->queryProperties().setProp("@kind", "topLevelKey");
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (slave < width) // if false - due to mismatch width fitting - fill in with a blank entry
        {
            dst.append(true);
            Owned<IPartDescriptor> originalPartDesc = originalDesc->getPart(slave);
            originalPartDesc->serialize(dst);
            Owned<IPartDescriptor> patchPartDesc = patchDesc->getPart(slave);
            patchPartDesc->serialize(dst);
            newIndexDesc->queryPart(slave)->serialize(dst);

            if (0 == slave)
            {
                if (!local)
                {
                    dst.append(true);
                    Owned<IPartDescriptor> originalTlkPartDesc = originalDesc->getPart(originalDesc->numParts()-1);
                    originalTlkPartDesc->serialize(dst);                                        
                    Owned<IPartDescriptor> patchTlkPartDesc = patchDesc->getPart(patchDesc->numParts()-1);
                    patchTlkPartDesc->serialize(dst);
                    newIndexDesc->queryPart(newIndexDesc->numParts()-1)->serialize(dst);
                }
                else
                    dst.append(false);
            }
        }
        else
            dst.append(false); // no part
    }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        if (mb.length()) // if 0 implies aborted out from this slave.
        {
            offset_t size;
            mb.read(size);
            CDateTime modifiedTime(mb);

            IPartDescriptor *partDesc = newIndexDesc->queryPart(slaveIdx);
            IPropertyTree &props = partDesc->queryProperties();
            props.setPropInt64("@size", size);
            StringBuffer timeStr;
            modifiedTime.getString(timeStr);
            props.setProp("@modified", timeStr.str());
            if (!local && 0 == slaveIdx)
            {
                mb.read(size);
                CDateTime modifiedTime(mb);
                IPartDescriptor *partDesc = newIndexDesc->queryPart(newIndexDesc->numParts()-1);
                IPropertyTree &props = partDesc->queryProperties();
                props.setPropInt64("@size", size);
                StringBuffer timeStr;
                modifiedTime.getString(timeStr);
                props.setProp("@modified", timeStr.str());
            }
        }
    }
    virtual void done()
    {
        Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
        Owned<IWUResult> r = wu->updateResultBySequence(helper->getSequence());
        r->setResultStatus(ResultStatusCalculated);
        r->setResultLogicalName(outputName);
        r.clear();
        wu.clear();

        IPropertyTree &props = newIndexDesc->queryProperties();
        props.setProp("@kind", "key");
        if (0 != (helper->getFlags() & KDPexpires))
            setExpiryTime(props, helper->getExpiryDays());

        // Fill in some logical file properties here
        IPropertyTree &originalProps = originalDesc->queryProperties();;
        if (originalProps.queryProp("ECL"))
            props.setProp("ECL", originalProps.queryProp("ECL"));
        MemoryBuffer rLMB;
        // Legacy record layout info
        if (originalProps.getPropBin("_record_layout", rLMB))
            props.setPropBin("_record_layout", rLMB.length(), rLMB.toByteArray());
        // New record layout info
        if (originalProps.getPropBin("_rtlType", rLMB.clear()))
            props.setPropBin("_rtlType", rLMB.length(), rLMB.toByteArray());
        props.setPropInt("@formatCrc", originalProps.getPropInt("@formatCrc"));
        if (originalProps.getPropBool("@local"))
            props.setPropBool("@local", true);

        container.queryTempHandler()->registerFile(outputName, container.queryOwner().queryGraphId(), 0, false, WUFileStandard, &clusters);
        queryThorFileManager().publish(container.queryJob(), outputName, *newIndexDesc);
        CMasterActivity::done();
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        IHThorKeyPatchArg *helper = (IHThorKeyPatchArg *) queryHelper();
        if (0==(KDPoverwrite & helper->getFlags()))
        {
            if (KDPvaroutputname & helper->getFlags()) return;
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), outputName, AccessMode::readMeta, false, true, false, container.activityIsCodeSigned());
            if (file)
                throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
        }
    }
};

// CMasterGraphElement
CActivityBase *createKeyPatchActivityMaster(CMasterGraphElement *container)
{
    return new CKeyPatchMaster(container);
}
