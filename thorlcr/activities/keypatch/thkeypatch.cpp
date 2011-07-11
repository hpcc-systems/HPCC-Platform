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
#include "thkeypatch.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/keypatch/thkeypatch.cpp $ $Id: thkeypatch.cpp 65578 2011-06-20 11:49:27Z jsmith $");


class CKeyPatchMaster : public CMasterActivity
{
    IHThorKeyPatchArg *helper;
    Owned<IFileDescriptor> newIndexDesc, originalDesc, patchDesc;
    bool local;
    unsigned width;
    StringArray clusters;

public:
    CKeyPatchMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = NULL;
        local = false;
        width = 0;
    }
    ~CKeyPatchMaster()
    {
    }
    void init()
    {
        helper = (IHThorKeyPatchArg *)queryHelper();

        Owned<IDistributedFile> originalIndexFile = queryThorFileManager().lookup(container.queryJob(), helper->queryOriginalName());
        Owned<IDistributedFile> patchFile = queryThorFileManager().lookup(container.queryJob(), helper->queryPatchName());
        
        if (originalIndexFile->numParts() != patchFile->numParts())
            throw MakeActivityException(this, TE_KeyPatchIndexSizeMismatch, "Index %s and patch %s differ in width", helper->queryOriginalName(), helper->queryPatchName());
        if (originalIndexFile->querySuperFile() || patchFile->querySuperFile())
            throw MakeActivityException(this, 0, "Patching super files not supported");
        
        width = originalIndexFile->numParts();

        originalDesc.setown(originalIndexFile->getFileDescriptor());
        patchDesc.setown(patchFile->getFileDescriptor());

        Owned<IPartDescriptor> tlkDesc = originalDesc->getPart(originalDesc->numParts()-1);
        const char *kind = tlkDesc->queryProperties().queryProp("@kind");
        local = NULL == kind || 0 != stricmp("topLevelKey", kind);

        if (!local && width > 1)
            width--; // 1 part == No n distributed / Monolithic key
        if (width > container.queryJob().querySlaves())
            throw MakeActivityException(this, 0, "Unsupported: keypatch(%s, %s) - Cannot patch a key that's wider(%d) than the target cluster size(%d)", originalIndexFile->queryLogicalName(), patchFile->queryLogicalName(), width, container.queryJob().querySlaves());

        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), helper->queryOutputName(), clusters, groups);
        newIndexDesc.setown(queryThorFileManager().create(container.queryJob(), helper->queryOutputName(), clusters, groups, 0 != (KDPoverwrite & helper->getFlags()), 0, !local, width));
        if (!local)
            newIndexDesc->queryPart(newIndexDesc->numParts()-1)->queryProperties().setProp("@kind", "topLevelKey");
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
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
    void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
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

        IPropertyTree &props = newIndexDesc->queryProperties();
        props.setProp("@kind", "key");
        setExpiryTime(props, helper->getExpiryDays());

        // Fill in some logical file properties here
        IPropertyTree &originalProps = originalDesc->queryProperties();;
        if (originalProps.queryProp("ECL"))
            props.setProp("ECL", originalProps.queryProp("ECL"));
        MemoryBuffer rLMB;
        if (originalProps.getPropBin("_record_layout", rLMB))
            props.setPropBin("_record_layout", rLMB.length(), rLMB.toByteArray());
        props.setPropInt("@formatCrc", originalProps.getPropInt("@formatCrc"));
        if (originalProps.getPropBool("@local"))
            props.setPropBool("@local", true);

        container.queryTempHandler()->registerFile(helper->queryOutputName(), container.queryOwner().queryGraphId(), 0, false, WUFileStandard, &clusters);
        queryThorFileManager().publish(container.queryJob(), helper->queryOutputName(), false, *newIndexDesc);
    }
    void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        IHThorKeyPatchArg *helper = (IHThorKeyPatchArg *) queryHelper();
        if (0==(KDPoverwrite & helper->getFlags()))
        {
            if (KDPvaroutputname & helper->getFlags()) return;
            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helper->queryOutputName(), false, true);
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
