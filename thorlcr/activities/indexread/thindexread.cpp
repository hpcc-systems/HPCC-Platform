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

#include "thexception.hpp"
#include "jhtree.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"

#include "thmem.hpp"
#include "thdiskbase.ipp"
#include "thindexread.ipp"

class CIndexReadBase : public CMasterActivity
{
protected:
    BoolArray performPartLookup;
    Owned<IFileDescriptor> fileDesc;
    rowcount_t keyedLimit = RCMAX;
    IHThorIndexReadBaseArg *indexBaseHelper;
    Owned<CSlavePartMapping> mapping;
    bool nofilter = false;
    bool localKey = false;
    bool partitionKey = false;
    StringBuffer fileName;

    rowcount_t aggregateToLimit()
    {
        rowcount_t total = 0;
        unsigned slaves = container.queryJob().querySlaves();
        unsigned s;
        for (s=0; s<slaves; s++)
        {
            CMessageBuffer msg;
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender))
                return 0;
            if (abortSoon)
                return 0;
            rowcount_t count;
            msg.read(count);
            total += count;
        }
        return total;
    }
    bool processKeyedLimit()
    {
        rowcount_t total = aggregateToLimit();
        CMessageBuffer msg;
        msg.append(total);
        ICommunicator &comm = queryJobChannel().queryJobComm();
        unsigned slaves = container.queryJob().querySlaves();
        unsigned s=0;
        for (; s<slaves; s++)
        {
            verifyex(comm.send(msg, s+1, mpTag));
        }
        return total > keyedLimit;
    }
    void prepareKey(IDistributedFile *index)
    {
        IDistributedFile *f = index;
        IDistributedSuperFile *super = f->querySuperFile();

        unsigned nparts = f->numParts(); // includes tlks if any, but unused in array
        performPartLookup.ensure(nparts);

        bool checkTLKConsistency = (nullptr != super) && !localKey && (0 != (TIRsorted & indexBaseHelper->getFlags()));
        if (nofilter)
        {
            while (nparts--) performPartLookup.append(true);
            if (!checkTLKConsistency) return;
        }
        else
        {
            while (nparts--) performPartLookup.append(false); // parts to perform lookup set later
        }

        Owned<IDistributedFileIterator> iter;
        if (super)
        {
            iter.setown(super->getSubFileIterator(true));
            verifyex(iter->first());
            f = &iter->query();
        }
        unsigned width = f->numParts();
        if (!localKey)
            --width;
        assertex(width);
        unsigned tlkCrc = 0;
        bool first = true;
        unsigned superSubIndex=0;
        bool fileCrc = false, rowCrc = false;
        for (;;)
        {
            Owned<IDistributedFilePart> part = f->getPart(width);
            if (checkTLKConsistency)
            {
                unsigned _tlkCrc;
                if (part->getCrc(_tlkCrc))
                    fileCrc = true;
                else if (part->queryAttributes().hasProp("@crc")) // NB: key "@crc" is not a crc on the file, but data within.
                {
                    _tlkCrc = part->queryAttributes().getPropInt("@crc");
                    rowCrc = true;
                }
                else if (part->queryAttributes().hasProp("@tlkCrc")) // backward compat.
                {
                    _tlkCrc = part->queryAttributes().getPropInt("@tlkCrc");
                    rowCrc = true;
                }
                else
                {
                    if (rowCrc || fileCrc)
                    {
                        checkTLKConsistency = false;
                        Owned<IException> e = MakeActivityWarning(&container, 0, "Cannot validate that tlks in superfile %s match, some crc attributes are missing", super->queryLogicalName());
                        queryJobChannel().fireException(e);
                    }
                }
                if (rowCrc && fileCrc)
                {
                    checkTLKConsistency = false;
                    Owned<IException> e = MakeActivityWarning(&container, 0, "Cannot validate that tlks in superfile %s match, due to mixed crc types.", super->queryLogicalName());
                    queryJobChannel().fireException(e);
                }
                if (checkTLKConsistency)
                {
                    if (first)
                    {
                        tlkCrc = _tlkCrc;
                        first = false;
                    }
                    else if (tlkCrc != _tlkCrc)
                        throw MakeActivityException(this, 0, "Sorted output on super files comprising of non copartitioned sub keys is not supported (TLK's do not match)");
                }
            }
            if (!nofilter)
            {
                Owned<IKeyIndex> keyIndex;
                unsigned copy;
                for (copy=0; copy<part->numCopies(); copy++)
                {
                    RemoteFilename rfn;
                    OwnedIFile ifile = createIFile(part->getFilename(rfn,copy));
                    if (ifile->exists())
                    {
                        StringBuffer remotePath;
                        rfn.getPath(remotePath);
                        unsigned crc = 0;
                        part->getCrc(crc);
                        keyIndex.setown(createKeyIndex(remotePath.str(), crc, false, false));
                        break;
                    }
                }
                if (!keyIndex)
                    throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", index->queryLogicalName());

                Owned <IKeyManager> tlk = createLocalKeyManager(indexBaseHelper->queryDiskRecordSize()->queryRecordAccessor(true), keyIndex, nullptr, indexBaseHelper->hasNewSegmentMonitors(), false);
                indexBaseHelper->createSegmentMonitors(tlk);
                tlk->finishSegmentMonitors();
                tlk->reset();
                if (partitionKey)
                {
                    unsigned slavePart = tlk->getPartition();  // Returns 0 if no partition info, or filter cannot be partitioned
                    if (slavePart)
                        performPartLookup.replace(true, slavePart-1);
                    else
                    {
                        // if not partitionable filter, all parts need to be used
                        for (unsigned p=0; p<f->numParts(); p++)
                            performPartLookup.replace(true, p);
                    }
                }
                else
                {
                    while (tlk->lookup(false))
                    {
                        offset_t node = extractFpos(tlk);
                        if (node)
                            performPartLookup.replace(true, (aindex_t)(super?super->numSubFiles(true)*(node-1)+superSubIndex:node-1));
                    }
                }
            }
            if (!super||!iter->next())
                break;
            superSubIndex++;
            f = &iter->query();
            if (width != f->numParts()-1)
                throw MakeActivityException(this, 0, "Unsupported: Super key '%s' contains subfiles with differing numbers of file parts.", f->queryLogicalName());
        }
    }

public:
    CIndexReadBase(CMasterGraphElement *info) : CMasterActivity(info, indexReadActivityStatistics)
    {
        indexBaseHelper = (IHThorIndexReadBaseArg *)queryHelper();
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
        reInit = 0 != (indexBaseHelper->getFlags() & (TIRvarfilename|TIRdynamicfilename));
    }
    virtual void init() override
    {
        CMasterActivity::init();
        OwnedRoxieString helperFileName = indexBaseHelper->getFileName();
        StringBuffer expandedFileName;
        queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName);
        fileName.set(expandedFileName);
        Owned<IDistributedFile> index = queryThorFileManager().lookup(container.queryJob(), helperFileName, false, 0 != (TIRoptional & indexBaseHelper->getFlags()), true, container.activityIsCodeSigned());
        if (index)
        {
            checkFileType(this, index, "key", true);

            partitionKey = index->queryAttributes().hasProp("@partitionFieldMask");
            localKey = index->queryAttributes().getPropBool("@local") && !partitionKey;

            if (container.queryLocalData() && !localKey)
                throw MakeActivityException(this, 0, "Index Read cannot be LOCAL unless supplied index is local");

            nofilter = 0 != (TIRnofilter & indexBaseHelper->getFlags());
            if (localKey)
                nofilter = true;
            else
            {
                IDistributedSuperFile *super = index->querySuperFile();
                IDistributedFile *sub = super ? &super->querySubFile(0,true) : index.get();
                if (sub && 1 == sub->numParts())
                    nofilter = true;
            }
            //MORE: Change index getFormatCrc once we support projected rows for indexes.
            checkFormatCrc(this, index, indexBaseHelper->getDiskFormatCrc(), indexBaseHelper->queryDiskRecordSize(), indexBaseHelper->getProjectedFormatCrc(), indexBaseHelper->queryProjectedDiskRecordSize(), true);
            if ((container.queryLocalOrGrouped() || indexBaseHelper->canMatchAny()) && index->numParts())
            {
                fileDesc.setown(getConfiguredFileDescriptor(*index));
                if (container.queryLocalOrGrouped())
                    nofilter = true;
                prepareKey(index);
                addReadFile(index);
                mapping.setown(getFileSlaveMaps(index->queryLogicalName(), *fileDesc, container.queryJob().queryUserDescriptor(), container.queryJob().querySlaveGroup(), container.queryLocalOrGrouped(), true, NULL, index->querySuperFile()));
            }
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        dst.append(fileName);
        if (!container.queryLocalOrGrouped())
            dst.append(mpTag);
        IArrayOf<IPartDescriptor> parts;
        if (fileDesc.get())
        {
            mapping->getParts(slave, parts);
            if (!nofilter)
            {
                ForEachItemInRev(p, parts)
                {
                    IPartDescriptor &part = parts.item(p);
                    if (!performPartLookup.item(part.queryPartIndex()))
                        parts.zap(part);
                }
            }
        }
        dst.append((unsigned)parts.ordinality());
        UnsignedArray partNumbers;
        ForEachItemIn(p2, parts)
            partNumbers.append(parts.item(p2).queryPartIndex());
        if (partNumbers.ordinality())
            fileDesc->serializeParts(dst, partNumbers);
    }
    virtual void abort() override
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

class CIndexReadActivityMaster : public CIndexReadBase
{
    typedef CIndexReadBase PARENT;

    IHThorIndexReadArg *helper;

    void processKeyedLimit()
    {
        if (PARENT::processKeyedLimit())
        {
            if (0 == (TIRkeyedlimitskips & helper->getFlags()))
            {
                if (0 == (TIRkeyedlimitcreates & helper->getFlags()))
                    helper->onKeyedLimitExceeded(); // should throw exception
            }
        }
    }
public:
    CIndexReadActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexReadArg *)queryHelper();
    }
    virtual void process() override
    {
        if (container.queryLocalOrGrouped())
            return;
        if (!helper->canMatchAny())
            return;
        keyedLimit = (rowcount_t)helper->getKeyedLimit();
        if (keyedLimit != RCMAX)
            processKeyedLimit();
    }
};


CActivityBase *createIndexReadActivityMaster(CMasterGraphElement *info)
{
    return new CIndexReadActivityMaster(info);
}


class CIndexCountActivityMaster : public CIndexReadBase
{
    typedef CIndexReadBase PARENT;

    IHThorIndexCountArg *helper;

    void processKeyedLimit()
    {
        if (PARENT::processKeyedLimit())
        {
            if (0 == (TIRkeyedlimitskips & helper->getFlags()))
            {
                if (0 == (TIRkeyedlimitcreates & helper->getFlags()))
                    helper->onKeyedLimitExceeded(); // should throw exception
            }
        }
    }
public:
    CIndexCountActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexCountArg *)queryHelper();
    }
    virtual void process() override
    {
        if (container.queryLocalOrGrouped())
            return;
        if (!helper->canMatchAny())
            return; // implies count will be 0
        keyedLimit = (rowcount_t)helper->getKeyedLimit();
        if (keyedLimit != RCMAX)
            processKeyedLimit();
        rowcount_t total = aggregateToLimit();
        CMessageBuffer msg;
        msg.append(total);
        ICommunicator &comm = queryJobChannel().queryJobComm();
        verifyex(comm.send(msg, 1, mpTag)); // send to 1st slave only
    }
};

CActivityBase *createIndexCountActivityMaster(CMasterGraphElement *info)
{
    return new CIndexCountActivityMaster(info);
}

class CIndexNormalizeActivityMaster : public CIndexReadBase
{
    typedef CIndexReadBase PARENT;

    IHThorIndexNormalizeArg *helper;

    void processKeyedLimit()
    {
        if (PARENT::processKeyedLimit())
        {
            if (0 == (TIRkeyedlimitskips & helper->getFlags()))
            {
                if (0 == (TIRkeyedlimitcreates & helper->getFlags()))
                    helper->onKeyedLimitExceeded(); // should throw exception
            }
        }
    }
public:
    CIndexNormalizeActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexNormalizeArg *)queryHelper();
    }
    virtual void process() override
    {
        if (container.queryLocalOrGrouped())
            return;
        if (!helper->canMatchAny())
            return;
        keyedLimit = (rowcount_t)helper->getKeyedLimit();
        if (keyedLimit != RCMAX)
            processKeyedLimit();
    }
};

CActivityBase *createIndexNormalizeActivityMaster(CMasterGraphElement *info)
{
    return new CIndexNormalizeActivityMaster(info);
}


class CIndexAggregateActivityMaster : public CIndexReadBase
{
    IHThorIndexAggregateArg *helper;
public:
    CIndexAggregateActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexAggregateArg *)queryHelper();
    }
    virtual void process() override
    {
        if (container.queryLocalOrGrouped())
            return;
        Owned<IThorRowInterfaces> rowIf = createRowInterfaces(helper->queryOutputMeta());
        OwnedConstThorRow result = getAggregate(*this, container.queryJob().querySlaves(), *rowIf, *helper, mpTag);
        if (!result)
            return;
        CMessageBuffer msg;
        CMemoryRowSerializer mbs(msg);
        rowIf->queryRowSerializer()->serialize(mbs,(const byte *)result.get());
        if (!queryJobChannel().queryJobComm().send(msg, 1, mpTag, 5000))
            throw MakeThorException(0, "Failed to give result to slave");
    }
};


CActivityBase *createIndexAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CIndexAggregateActivityMaster(info);
}

class CIndexGroupAggregateActivityMaster : public CIndexReadBase
{
public:
    CIndexGroupAggregateActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
    }
};

CActivityBase *createIndexGroupAggregateActivityMaster(CMasterGraphElement *info)
{
    return new CIndexGroupAggregateActivityMaster(info);
}
