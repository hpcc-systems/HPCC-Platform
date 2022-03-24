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

#include "dadfs.hpp"

#include "thorport.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "eclhelper.hpp"
#include "rtldynfield.hpp"
#include "jlzw.hpp"

#include "thmem.hpp"
#include "thdiskread.ipp"

class CDiskReadMasterVF : public CDiskReadMasterBase
{
public:
    CDiskReadMasterVF(CMasterGraphElement *info) : CDiskReadMasterBase(info) { }
    virtual void validateFile(IDistributedFile *file) override
    {
        IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *)queryHelper();
        IOutputMetaData *recordSize = helper->queryDiskRecordSize()->querySerializedDiskMeta();
        if (!recordSize->isFixedSize())
            checkFileType(this, file, "flat", true); // throws an exception if variable size and type mismatch
        bool codeGenGrouped = 0 != (TDXgrouped & helper->getFlags());
        bool isGrouped = fileDesc->isGrouped();
        if (isGrouped != codeGenGrouped)
        {
            Owned<IException> e = MakeActivityWarning(&container, TE_GroupMismatch, "DFS and code generated group info. differs: DFS(%s), CodeGen(%s), using DFS info", isGrouped?"grouped":"ungrouped", codeGenGrouped?"grouped":"ungrouped");
            queryJobChannel().fireException(e);
        }
        RecordTranslationMode translationMode = getTranslationMode(*this);
        if (RecordTranslationMode::None != translationMode)
        {
            /* Turn off translation if not "flat" or not RecordTranslationMode::AlwaysDisk
             * NB: translation is turned off in slaves in this case too,
             * turning off here, ensures format crc's are checked and any mismatch spotted
             * with sensible error.
             */

            const char *kind = queryFileKind(file);
            if (!strisame(kind, "flat") && (RecordTranslationMode::AlwaysDisk != translationMode))
                translationMode = RecordTranslationMode::None;
        }
        if (RecordTranslationMode::None == translationMode)
        {
            unsigned dfsCrc;
            if (file->getFormatCrc(dfsCrc) && helper->getDiskFormatCrc() != dfsCrc)
            {
                VStringBuffer fileStr("File: %s", file->queryLogicalName());
                Owned<IThorException> e = MakeActivityException(this, TE_FormatCrcMismatch, "Automatic translation not available, and layout does not match published layout. %s", fileStr.str());
                if (!queryContainer().queryJob().getWorkUnitValueInt("skipFileFormatCrcCheck", 0))
                    throw e.getClear();
                e->setAction(tea_warning);
                fireException(e);
            }
            if (recordSize->isFixedSize()) // fixed size
            {
                if (0 != fileDesc->queryProperties().getPropInt("@recordSize"))
                {
                    size32_t rSz = fileDesc->queryProperties().getPropInt("@recordSize");
                    if (isGrouped)
                        rSz--; // eog byte not to be included in this test.
                    if (rSz != recordSize->getMinRecordSize())
                        throw MakeThorException(TE_RecordSizeMismatch, "Published record size %d for file %s, does not match coded record size %d", rSz, fileName.get(), recordSize->getMinRecordSize());
                }
                if (!fileDesc->isCompressed() && (TDXcompress & helper->getFlags()))
                {
                    size32_t rSz = recordSize->getMinRecordSize();
                    if (isGrouped) rSz++;
                    if (rSz >= MIN_ROWCOMPRESS_RECSIZE)
                    {
                        Owned<IException> e = MakeActivityWarning(&container, TE_CompressionMismatch, "Ignoring compression attribute on file '%s', which is not published as compressed in DFS", fileName.get());
                        queryJobChannel().fireException(e);
                    }
                }
            }
        }
        checkFormatCrc(this, file, helper->getDiskFormatCrc(), helper->queryDiskRecordSize(), helper->getProjectedFormatCrc(), helper->queryProjectedDiskRecordSize(), false);
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
    virtual void done() override
    {
        CDiskReadMasterVF::done();
        if (file)
        {
            IHThorDiskReadBaseArg *helper = (IHThorDiskReadBaseArg *)queryHelper();
            if (0 != (helper->getFlags() & TDXtemporary) && !container.queryJob().queryUseCheckpoints())
                container.queryTempHandler()->deregisterFile(fileName, fileDesc->queryProperties().getPropBool("@pausefile"));
        }
    }
    virtual void init() override
    {
        CDiskReadMasterVF::init();
#if 0 // JCSMORE
        IHThorDiskReadArg *helper = (IHThorDiskReadArg *)queryHelper();
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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        CDiskReadMasterVF::serializeSlaveData(dst, slave);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
    }
    virtual void process() override
    {
        Owned<IThorRowInterfaces> rowIf = createRowInterfaces(helper->queryOutputMeta());
        OwnedConstThorRow result = getAggregate(*this, container.queryJob().querySlaves(), *rowIf, *helper, mpTag);
        if (!result)
            return;
        CMessageBuffer msg;
        CMemoryRowSerializer mbs(msg);
        rowIf->queryRowSerializer()->serialize(mbs, (const byte *)result.get());
        if (!queryJobChannel().queryJobComm().send(msg, 1, mpTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
    virtual void abort() override
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
    virtual void init() override
    {
        CDiskReadMasterVF::init();
        bool canMatch = container.queryLocalOrGrouped() || helper->canMatchAny(); // if local, assume may match
        if (!canMatch)
            totalCountKnown = true; // totalCount = 0;
        else if (!container.queryLocalOrGrouped())
        {
            if (!helper->hasSegmentMonitors() && !helper->hasFilter() && !(helper->getFlags() & TDXtemporary))
            {
                if (file && canMatch)
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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        CDiskReadMasterVF::serializeSlaveData(dst, slave);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
        dst.append(totalCountKnown);
        dst.append(totalCount);
    }
    virtual void process() override
    {
        if (totalCountKnown) return;
        if (container.queryLocalOrGrouped())
            return;
        totalCount = ::getCount(*this, container.queryJob().querySlaves(), stopAfter, mpTag);
        if (totalCount > stopAfter)
            totalCount = stopAfter;
        CMessageBuffer msg;
        msg.append(totalCount);
        if (!queryJobChannel().queryJobComm().send(msg, 1, mpTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
    virtual void abort() override
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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
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




