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
#include "eclhelper.hpp"
#include "thspill.ipp"

class SpillActivityMaster : public CMasterActivity
{
    Owned<IFileDescriptor> fileDesc;
    StringArray clusters;
    __int64 recordsProcessed;
    Owned<IDistributedFile> file;

public:
    SpillActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        recordsProcessed = 0;
    }
    void init()
    {
        IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
        IArrayOf<IGroup> groups;
        fillClusterArray(container.queryJob(), helper->getFileName(), clusters, groups);
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), helper->getFileName(), clusters, groups, true, TDWnoreplicate+TDXtemporary));
        IPropertyTree &props = fileDesc->queryProperties();
        bool blockCompressed=false;
        void *ekey;
        size32_t ekeylen;
        helper->getEncryptKey(ekeylen,ekey);
        if (ekeylen)
        {
            memset(ekey,0,ekeylen);
            free(ekey);
            props.setPropBool("@encrypted", true);      
            blockCompressed = true;
        }
        else if (0 != (helper->getFlags() & TDWnewcompress) || 0 != (helper->getFlags() & TDXcompress))
            blockCompressed = true;
        if (blockCompressed)
            props.setPropBool("@blockCompressed", true);        
        if (0 != (helper->getFlags() & TDXgrouped))
            fileDesc->queryProperties().setPropBool("@grouped", true);

        if (container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
        { // do early, because this will be local act. and will not come back to master until end of owning graph.
            container.queryTempHandler()->registerFile(helper->getFileName(), container.queryOwner().queryGraphId(), helper->getTempUsageCount(), TDXtemporary & helper->getFlags(), getDiskOutputKind(helper->getFlags()), &clusters);
            queryThorFileManager().publish(container.queryJob(), helper->getFileName(), true, *fileDesc);
        }
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
        IPartDescriptor *partDesc = fileDesc->queryPart(slave);
        partDesc->serialize(dst);
        dst.append(helper->getTempUsageCount());
    }
    void done()
    {
        if (!container.queryOwner().queryOwner() || container.queryOwner().isGlobal()) // I am in a child query
        {
            IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
            container.queryTempHandler()->registerFile(helper->getFileName(), container.queryOwner().queryGraphId(), helper->getTempUsageCount(), TDXtemporary & helper->getFlags(), getDiskOutputKind(helper->getFlags()), &clusters);
            queryThorFileManager().publish(container.queryJob(), helper->getFileName(), true, *fileDesc);
        }
    }
    void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
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
            StringBuffer timeStr;
            modifiedTime.getString(timeStr);

            IPartDescriptor *partDesc = fileDesc->queryPart(slaveIdx);
            IPropertyTree &props = partDesc->queryProperties();
            props.setPropInt64("@size", size);
            if (fileDesc->isCompressed())
                props.setPropInt64("@compressedSize", physicalSize);
            props.setPropInt("@fileCrc", fileCrc);
            props.setProp("@modified", timeStr.str());
        }
    }
};

CActivityBase *createSpillActivityMaster(CMasterGraphElement *info)
{
    return new SpillActivityMaster(info);
}
