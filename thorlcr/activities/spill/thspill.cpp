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
#include "eclhelper.hpp"
#include "thspill.ipp"

class SpillActivityMaster : public CMasterActivity
{
    Owned<IFileDescriptor> fileDesc;
    StringArray clusters;
    __int64 recordsProcessed;
    Owned<IDistributedFile> file;
    StringAttr fileName;

public:
    SpillActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        recordsProcessed = 0;
    }
    virtual void init()
    {
        CMasterActivity::init();
        IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
        IArrayOf<IGroup> groups;
        OwnedRoxieString helperFileName = helper->getFileName();
        StringBuffer expandedFileName;
        queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName, true);
        fileName.set(expandedFileName);

        if (TDXtemporary & helper->getFlags())
        {
            // NB: these temp IFileDescriptors are not published
            // but we want to ensure they don't have the default data plane
            // which would cause the paths to be manipulated if numDevices>1
            StringBuffer planeName;
            if (getDefaultSpillPlane(planeName))
            {
                clusters.append(planeName);
                groups.append(*LINK(&queryLocalGroup()));
            }
        }
        if (0 == groups.ordinality()) // may be filled if temp (see above)
            fillClusterArray(container.queryJob(), fileName, clusters, groups);
        fileDesc.setown(queryThorFileManager().create(container.queryJob(), fileName, clusters, groups, true, TDWnoreplicate+TDXtemporary));
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
        props.setProp("@kind", "flat");
        if (container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
        { // do early, because this will be local act. and will not come back to master until end of owning graph.
            container.queryTempHandler()->registerFile(fileName, container.queryOwner().queryGraphId(), helper->getTempUsageCount(), TDXtemporary & helper->getFlags(), getDiskOutputKind(helper->getFlags()), &clusters);
            queryThorFileManager().publish(container.queryJob(), fileName, *fileDesc);
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
        IPartDescriptor *partDesc = fileDesc->queryPart(slave);
        partDesc->serialize(dst);
        dst.append(helper->getTempUsageCount());
    }
    virtual void done()
    {
        CMasterActivity::done();
        if (!container.queryOwner().queryOwner() || container.queryOwner().isGlobal()) // I am in a child query
        {
            IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
            container.queryTempHandler()->registerFile(fileName, container.queryOwner().queryGraphId(), helper->getTempUsageCount(), TDXtemporary & helper->getFlags(), getDiskOutputKind(helper->getFlags()), &clusters);
            IPropertyTree &props = fileDesc->queryProperties();
            props.setPropInt64("@recordCount", recordsProcessed);
            queryThorFileManager().publish(container.queryJob(), fileName, *fileDesc);
        }
    }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        if (mb.length()) // if 0 implies aborted out from this slave.
        {
            rowcount_t slaveProcessed;
            mb.read(slaveProcessed);
            recordsProcessed += slaveProcessed;

            offset_t size, physicalSize;
            mb.read(size);
            mb.read(physicalSize);

            CDateTime modifiedTime(mb);
            StringBuffer timeStr;
            modifiedTime.getString(timeStr);

            IPartDescriptor *partDesc = fileDesc->queryPart(slaveIdx);
            IPropertyTree &props = partDesc->queryProperties();
            props.setPropInt64("@size", size);
            props.setPropInt64("@recordCount", slaveProcessed);
            if (fileDesc->isCompressed())
                props.setPropInt64("@compressedSize", physicalSize);
            props.setProp("@modified", timeStr.str());
        }
    }
};

CActivityBase *createSpillActivityMaster(CMasterGraphElement *info)
{
    return new SpillActivityMaster(info);
}
