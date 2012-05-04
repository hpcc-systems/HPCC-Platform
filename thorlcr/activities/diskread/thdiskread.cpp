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

#include "dadfs.hpp"

#include "thorport.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "jlzw.hpp"

#include "thmem.hpp"
#include "thdiskread.ipp"

class CDiskReadMasterVF : public CDiskReadMasterBase
{
public:
    CDiskReadMasterVF(CMasterGraphElement *info) : CDiskReadMasterBase(info) { }
    virtual void validateFile(IDistributedFile *file)
    {
        IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *)queryHelper();
        bool codeGenGrouped = 0 != (TDXgrouped & helper->getFlags());
        bool isGrouped = fileDesc->isGrouped();
        if (isGrouped != codeGenGrouped)
        {
            Owned<IException> e = MakeActivityWarning(&container, TE_GroupMismatch, "DFS and code generated group info. differs: DFS(%s), CodeGen(%s), using DFS info", isGrouped?"grouped":"ungrouped", codeGenGrouped?"grouped":"ungrouped");
            container.queryJob().fireException(e);
        }
        IOutputMetaData *recordSize = helper->queryDiskRecordSize()->querySerializedMeta();
        if (recordSize->isFixedSize()) // fixed size
        {
            if (0 != fileDesc->queryProperties().getPropInt("@recordSize"))
            {
                size32_t rSz = fileDesc->queryProperties().getPropInt("@recordSize");
                if (isGrouped)
                    rSz--; // eog byte not to be included in this test.
                if (rSz != recordSize->getMinRecordSize())
                    throw MakeThorException(TE_RecordSizeMismatch, "Published record size %d for file %s, does not match coded record size %d", rSz, helper->getFileName(), recordSize->getMinRecordSize());
            }
            if (!fileDesc->isCompressed() && (TDXcompress & helper->getFlags()))
            {
                size32_t rSz = recordSize->getMinRecordSize();
                if (isGrouped) rSz++;
                if (rSz >= MIN_ROWCOMPRESS_RECSIZE)
                {
                    Owned<IException> e = MakeActivityWarning(&container, TE_CompressionMismatch, "Ignoring compression attribute on file '%s', which is not published as compressed in DFS", helper->getFileName());
                    container.queryJob().fireException(e);
                }
            }
        }
        if (0 == (TDRnocrccheck & helper->getFlags()))
            checkFormatCrc(this, file, helper->getFormatCrc(), false);
    }
};

class CDiskReadActivityMaster : public CDiskReadMasterVF
{
public:
    CDiskReadActivityMaster(CMasterGraphElement *info, IHThorArg *_helper) : CDiskReadMasterVF(info)
    {
        if (_helper)
            baseHelper.set(_helper);
    }
    virtual void done()
    {
        IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *)queryHelper();
        if (0 != (helper->getFlags() & TDXtemporary) && !container.queryJob().queryUseCheckpoints())
            container.queryTempHandler()->deregisterFile(helper->getFileName(), fileDesc->queryProperties().getPropBool("@pausefile"));
    }
    virtual void init()
    {
        CDiskReadMasterVF::init();
        IHThorDiskReadArg *helper = (IHThorDiskReadArg *)queryHelper();
#if 0 // JCSMORE
        hash = helper->queryRehash();
#endif
    }
};

CActivityBase *createDiskReadActivityMaster(CMasterGraphElement *info, IHThorArg *_helper)
{
    return new CDiskReadActivityMaster(info, _helper);
}

class CDiskAggregateActivityMaster : public CDiskReadMasterVF
{
    IHThorDiskAggregateArg *helper;
public:
    CDiskAggregateActivityMaster(CMasterGraphElement *info) : CDiskReadMasterVF(info)
    {
        helper = (IHThorDiskAggregateArg *)queryHelper();
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        CDiskReadMasterVF::serializeSlaveData(dst, slave);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
    }
    virtual void process()
    {
        IRecordSize *recordSize = helper->queryOutputMeta();

        Owned<IRowInterfaces> rowIf = createRowInterfaces(helper->queryOutputMeta(), queryActivityId(), queryCodeContext());                
        OwnedConstThorRow result = getAggregate(*this, container.queryJob().querySlaves(), *rowIf, *helper, mpTag);
        if (!result)
            return;
        CMessageBuffer msg;
        CMemoryRowSerializer mbs(msg);
        rowIf->queryRowSerializer()->serialize(mbs, (const byte *)result.get());
        if (!container.queryJob().queryJobComm().send(msg, 1, mpTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
    virtual void abort()
    {
        CDiskReadMasterVF::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

CActivityBase *createDiskAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CDiskAggregateActivityMaster(info);
}

class CDiskCountActivityMaster : public CDiskReadMasterVF
{
    rowcount_t totalCount;
    bool totalCountKnown;
    IHThorDiskCountArg *helper;
    rowcount_t stopAfter;
    
public:
    CDiskCountActivityMaster(CMasterGraphElement *info) : CDiskReadMasterVF(info)
    {
        totalCount = 0;
        totalCountKnown = false;
        helper = (IHThorDiskCountArg *)queryHelper();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
    }
    virtual void init()
    {
        CDiskReadMasterVF::init();
        bool canMatch = container.queryLocalOrGrouped() || helper->canMatchAny(); // if local, assume may match
        if (!canMatch)
            totalCountKnown = true; // totalCount = 0;
        else if (!container.queryLocalOrGrouped())
        {
            if (!helper->hasSegmentMonitors() && !helper->hasFilter() && !(helper->getFlags() & TDXtemporary))
            {
                Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), helper->getFileName(), 0 != ((TDXtemporary|TDXjobtemp) & helper->getFlags()), 0 != (TDRoptional & helper->getFlags()));
                if (file.get() && canMatch)
                {
                    if (0 != (TDRunfilteredcount & helper->getFlags()) && file->queryAttributes().hasProp("@recordCount"))
                    {
                        totalCount = (rowcount_t)file->queryAttributes().getPropInt64("@recordCount");
                        if (totalCount > stopAfter)
                            totalCount = stopAfter;
                        totalCountKnown = true;
                    }
                }
            }
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        CDiskReadMasterVF::serializeSlaveData(dst, slave);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
        dst.append(totalCountKnown);
        dst.append(totalCount);
    }
    virtual void process()
    {
        if (totalCountKnown) return;
        if (container.queryLocalOrGrouped())
            return;
        totalCount = ::getCount(*this, container.queryJob().querySlaves(), stopAfter, mpTag);
        if (totalCount > stopAfter)
            totalCount = stopAfter;
        CMessageBuffer msg;
        msg.append(totalCount);
        if (!container.queryJob().queryJobComm().send(msg, 1, mpTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
    virtual void abort()
    {
        CDiskReadMasterVF::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

CActivityBase *createDiskCountActivityMaster(CMasterGraphElement *info)
{
    return new CDiskCountActivityMaster(info);
}


class CDiskGroupAggregateActivityMaster : public CDiskReadMasterVF
{
public:
    CDiskGroupAggregateActivityMaster(CMasterGraphElement *info) : CDiskReadMasterVF(info)
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        CDiskReadMasterVF::serializeSlaveData(dst, slave);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
    }
};

CActivityBase *createDiskGroupAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CDiskGroupAggregateActivityMaster(info);
}




