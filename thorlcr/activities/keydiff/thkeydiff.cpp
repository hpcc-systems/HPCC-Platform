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

#include "jiface.hpp"
#include "dadfs.hpp"

#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thkeydiff.ipp"

class CKeyDiffMaster : public CMasterActivity
{
    IHThorKeyDiffArg *helper;
    Owned<IDistributedFile> originalIndexFile, newIndexFile;
    bool local, copyTlk;
    unsigned width;
    Owned<IFileDescriptor> patchDesc, originalDesc, newIndexDesc;
    StringArray clusters;

public:
    CKeyDiffMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = NULL;
        local = false;
        width = 0;
        copyTlk = globals->getPropBool("@diffCopyTlk", true); // because tlk can have meta data and diff/patch does not support
    }
    ~CKeyDiffMaster()
    {
    }
    void init()
    {
        helper = (IHThorKeyDiffArg *)queryHelper();

        originalIndexFile.setown(queryThorFileManager().lookup(container.queryJob(), helper->queryOriginalName()));
        newIndexFile.setown(queryThorFileManager().lookup(container.queryJob(), helper->queryUpdatedName()));
        if (originalIndexFile->numParts() != newIndexFile->numParts())
            throw MakeActivityException(this, TE_KeyDiffIndexSizeMismatch, "Index %s and %s differ in width", helper->queryOriginalName(), helper->queryUpdatedName());
        if (originalIndexFile->querySuperFile() || newIndexFile->querySuperFile())
            throw MakeActivityException(this, 0, "Diffing super files not supported");  

        width = originalIndexFile->numParts();

        originalDesc.setown(originalIndexFile->getFileDescriptor());
        newIndexDesc.setown(newIndexFile->getFileDescriptor());
        Owned<IPartDescriptor> tlkDesc = originalDesc->getPart(originalDesc->numParts()-1);
        const char *kind = tlkDesc->queryProperties().queryProp("@kind");
        local = NULL == kind || 0 != stricmp("topLevelKey", kind);

        if (!local)
            width--; // 1 part == No n distributed / Monolithic key
        if (width > container.queryJob().querySlaves())
            throw MakeActivityException(this, 0, "Unsupported: keydiff(%s, %s) - Cannot diff a key that's wider(%d) than the target cluster size(%d)", originalIndexFile->queryLogicalName(), newIndexFile->queryLogicalName(), width, container.queryJob().querySlaves());

        queryThorFileManager().noteFileRead(container.queryJob(), originalIndexFile);
        queryThorFileManager().noteFileRead(container.queryJob(), newIndexFile);

        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), helper->queryOutputName(), clusters, groups);
        patchDesc.setown(queryThorFileManager().create(container.queryJob(), helper->queryOutputName(), clusters, groups, 0 != (KDPoverwrite & helper->getFlags()), 0, !local, width));
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (slave < width) // if false - due to mismatch width fitting - fill in with a blank entry
        {
            dst.append(true);
            Owned<IPartDescriptor> originalPartDesc = originalDesc->getPart(slave);
            originalPartDesc->serialize(dst);
            Owned<IPartDescriptor> newIndexPartDesc = newIndexDesc->getPart(slave);
            newIndexPartDesc->serialize(dst);
            patchDesc->queryPart(slave)->serialize(dst);
            
            if (0 == slave)
            {
                if (!local)
                {
                    dst.append(true);
                    Owned<IPartDescriptor> originalTlkPartDesc = originalDesc->getPart(originalDesc->numParts()-1);
                    originalTlkPartDesc->serialize(dst);                                        
                    Owned<IPartDescriptor> newIndexTlkPartDesc = newIndexDesc->getPart(newIndexDesc->numParts()-1);
                    newIndexTlkPartDesc->serialize(dst);
                    patchDesc->queryPart(patchDesc->numParts()-1)->serialize(dst);
                }
                else
                    dst.append(false);
            }
        }
        else
            dst.append(false); // no part
    }
    void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        if (mb.length()) // if 0 implies aborted out from this slave.
        {
            offset_t size;
            mb.read(size);
            CDateTime modifiedTime(mb);

            IPartDescriptor *partDesc = patchDesc->queryPart(slaveIdx);
            IPropertyTree &props = partDesc->queryProperties();         
            StringBuffer timeStr;
            modifiedTime.getString(timeStr);
            props.setProp("@modified", timeStr.str());
            unsigned crc;
            mb.read(crc);
            props.setPropInt64("@fileCrc", crc);
            if (!local && 0 == slaveIdx)
            {
                IPartDescriptor *partDesc = patchDesc->queryPart(patchDesc->numParts()-1);
                IPropertyTree &props = partDesc->queryProperties();
                mb.read(size);
                props.setPropInt64("@size", size);
                CDateTime modifiedTime(mb);
                StringBuffer timeStr;
                modifiedTime.getString(timeStr);
                props.setProp("@modified", timeStr.str());
                if (copyTlk)
                {
                    Owned<IPartDescriptor> tlkDesc = newIndexDesc->getPart(newIndexDesc->numParts()-1);
                    if (partDesc->getCrc(crc))
                        props.setPropInt64("@fileCrc", crc);
                    props.setProp("@diffFormat", "copy");
                }
                else
                {
                    mb.read(crc);
                    props.setPropInt64("@fileCrc", crc);
                    props.setProp("@diffFormat", "diffV1");
                }
            }
        }
    }
    void done()
    {
        StringBuffer scopedName;
        queryThorFileManager().addScope(container.queryJob(), helper->queryOutputName(), scopedName);
        Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
        Owned<IWUResult> r = wu->updateResultBySequence(helper->getSequence());
        r->setResultStatus(ResultStatusCalculated);
        r->setResultLogicalName(scopedName.str());
        r.clear();
        wu.clear();

        IPropertyTree &patchProps = patchDesc->queryProperties();
        setExpiryTime(patchProps, helper->getExpiryDays());
        IPropertyTree &originalProps = originalDesc->queryProperties();;
        if (originalProps.queryProp("ECL"))
            patchProps.setProp("ECL", originalProps.queryProp("ECL"));
        if (originalProps.getPropBool("@local"))
            patchProps.setPropBool("@local", true);
        container.queryTempHandler()->registerFile(helper->queryOutputName(), container.queryOwner().queryGraphId(), 0, false, WUFileStandard, &clusters);
        Owned<IDistributedFile> patchFile;
        // set part sizes etc
        queryThorFileManager().publish(container.queryJob(), helper->queryOutputName(), false, *patchDesc, &patchFile, 0, false);
        try { // set file size
            if (patchFile) {
                __int64 fs = patchFile->getFileSize(true,false);
                if (fs!=-1)
                    patchFile->queryProperties().setPropInt64("@size",fs);
            }
        }
        catch (IException *e) {
            EXCLOG(e,"keydiff setting file size");
            e->Release();
        }
        // Add a new 'Patch' description to the secondary key.
        IPropertyTree &fileProps = newIndexFile->lockProperties();
        StringBuffer path("Patch[@name=\"");
        path.append(scopedName.str()).append("\"]");
        IPropertyTree *patch = fileProps.queryPropTree(path.str());
        if (!patch) patch = fileProps.addPropTree("Patch", createPTree());
        patch->setProp("@name", scopedName.str());
        unsigned checkSum;
        if (patchFile->getFileCheckSum(checkSum))
            patch->setPropInt64("@checkSum", checkSum);

        IPropertyTree *index = patch->setPropTree("Index", createPTree());
        index->setProp("@name", originalIndexFile->queryLogicalName());
        if (originalIndexFile->getFileCheckSum(checkSum))
            index->setPropInt64("@checkSum", checkSum);

        newIndexFile->unlockProperties();
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        IHThorKeyDiffArg *helper = (IHThorKeyDiffArg *) queryHelper();
        if (0==(KDPoverwrite & helper->getFlags()))
        {
            if (KDPvaroutputname & helper->getFlags()) return;
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helper->queryOutputName(), false, true);
            if (file)
                throw MakeActivityException(this, TE_OverwriteNotSpecified, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", file->queryLogicalName());
        }
    }
    void kill()
    {
        CMasterActivity::kill();
        originalIndexFile.clear();
        newIndexFile.clear();
    }
};

CActivityBase *createKeyDiffActivityMaster(CMasterGraphElement *container)
{
    return new CKeyDiffMaster(container);
}
