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
    unsigned fileStatsTableStart = NotFound;

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
        IDistributedSuperFile *super = index->querySuperFile();

        unsigned numParts = index->numParts();

        if (!nofilter)
        {
            performPartLookup.ensureCapacity(numParts); // includes tlks if any, but will never be marked used
            while (numParts--)
                performPartLookup.append(false);
        }

        bool checkTLKConsistency = (nullptr != super) && !localKey && (0 != (TIRsorted & indexBaseHelper->getFlags()));
        unsigned width = 0;
        bool hasTLK = false;
        unsigned tlkCrc = 0;
        bool first = true;
        unsigned superSubIndex=0;
        bool fileCrc = false, rowCrc = false;
        Owned<IDistributedFileIterator> iter;
        IDistributedFile *f = index;
        if (super)
        {
            iter.setown(super->getSubFileIterator(true));
            verifyex(iter->first());
            f = &iter->query();
        }
        unsigned numSubFiles = super ? super->numSubFiles(true) : 1;
        for (;;)
        {
            unsigned thisWidth = f->numParts();
            Owned<IDistributedFilePart> lastPart = f->getPart(thisWidth-1);

            // there are assumptions litered through the code, that indexes always have a TLK

            bool thisHasTLK = false;
            if (thisWidth > 1)
            {
                thisHasTLK = isPartTLK(lastPart->queryAttributes());
                if (!thisHasTLK)
                    verifyex(localKey); // only very old distributed local keys have no TLK
            }
            if (first)
            {
                hasTLK = thisHasTLK;
                width = thisWidth;
                if (!hasTLK)
                    checkTLKConsistency = false;
            }
            else if (width != thisWidth)
                throw MakeActivityException(this, 0, "Unsupported: Super key '%s' contains subfiles with differing numbers of file parts.", f->queryLogicalName());
            else if (hasTLK != thisHasTLK)
                throw MakeActivityException(this, 0, "Unsupported: Super key '%s' contains a mixture of subfiles, some with TLKs, some without", f->queryLogicalName());
            if (checkTLKConsistency)
            {
                unsigned _tlkCrc;
                if (lastPart->getCrc(_tlkCrc))
                    fileCrc = true;
                else if (lastPart->queryAttributes().hasProp("@crc")) // NB: key "@crc" is not a crc on the file, but data within.
                {
                    _tlkCrc = lastPart->queryAttributes().getPropInt("@crc");
                    rowCrc = true;
                }
                else if (lastPart->queryAttributes().hasProp("@tlkCrc")) // backward compat.
                {
                    _tlkCrc = lastPart->queryAttributes().getPropInt("@tlkCrc");
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
                        tlkCrc = _tlkCrc;
                    else if (tlkCrc != _tlkCrc)
                        throw MakeActivityException(this, 0, "Sorted output on super files comprising of non copartitioned sub keys is not supported (TLK's do not match)");
                }
            }
            if (hasTLK)
                thisWidth--;
            if (!nofilter)
            {
                Owned<IKeyIndex> keyIndex;
                unsigned copy;
                for (copy=0; copy<lastPart->numCopies(); copy++)
                {
                    RemoteFilename rfn;
                    OwnedIFile ifile = createIFile(lastPart->getFilename(rfn,copy));
                    if (ifile->exists())
                    {
                        StringBuffer remotePath;
                        rfn.getPath(remotePath);
                        unsigned crc = 0;
                        lastPart->getCrc(crc);
                        keyIndex.setown(createKeyIndex(remotePath.str(), crc, false, 0));
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
                    unsigned part = tlk->getPartition(); // Returns 0 if no partition info, or filter cannot be partitioned
                    if (part)
                        performPartLookup.replace(true, (aindex_t)(numSubFiles*(part-1)+superSubIndex));
                    else
                    {
                        // if not partitionable filter, all parts need to be used
                        for (unsigned p=0; p<thisWidth; p++)
                            performPartLookup.replace(true, numSubFiles*p+superSubIndex);
                    }
                }
                else
                {
                    while (tlk->lookup(false))
                    {
                        offset_t part = extractFpos(tlk);
                        if (part)
                            performPartLookup.replace(true, (aindex_t)(numSubFiles*(part-1)+superSubIndex));
                    }
                }
            }
            if (!super||!iter->next())
                break;
            first = false;
            superSubIndex++;
            f = &iter->query();
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
        if ((container.queryLocalOrGrouped() || indexBaseHelper->canMatchAny()))
        {
            OwnedRoxieString helperFileName = indexBaseHelper->getFileName();
            StringBuffer expandedFileName;
            queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName);
            fileName.set(expandedFileName);
            Owned<IDistributedFile> index = lookupReadFile(helperFileName, AccessMode::readRandom, false, false, 0 != (TIRoptional & indexBaseHelper->getFlags()), reInit, indexReadActivityStatistics, &fileStatsTableStart);
            if (index && (0 == index->numParts())) // possible if superfile
                index.clear();
            if (index)
            {
                checkFileType(this, index, "key", true);

                partitionKey = index->queryAttributes().hasProp("@partitionFieldMask");
                localKey = index->queryAttributes().getPropBool("@local") && !partitionKey;

                if (container.queryLocalData() && !localKey)
                    throw MakeActivityException(this, 0, "Index Read cannot be LOCAL unless supplied index is local");

                IDistributedSuperFile *super = index->querySuperFile();
                nofilter = 0 != (TIRnofilter & indexBaseHelper->getFlags());
                if (localKey)
                    nofilter = true;
                else
                {
                    IDistributedFile *sub = super ? &super->querySubFile(0,true) : index.get();
                    if (sub && 1 == sub->numParts())
                        nofilter = true;
                }
                //MORE: Change index getFormatCrc once we support projected rows for indexes.
                checkFormatCrc(this, index, indexBaseHelper->getDiskFormatCrc(), indexBaseHelper->queryDiskRecordSize(), indexBaseHelper->getProjectedFormatCrc(), indexBaseHelper->queryProjectedDiskRecordSize(), true);
                fileDesc.setown(getConfiguredFileDescriptor(*index));
                if (container.queryLocalOrGrouped())
                    nofilter = true;
                prepareKey(index);
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
        {
            fileDesc->serializeParts(dst, partNumbers);
            dst.append(fileStatsTableStart);
        }
    }
    virtual void abort() override
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb) override
    {
        CMasterActivity::deserializeStats(node, mb);
        unsigned numFilesToRead;
        mb.read(numFilesToRead);
        assertex(numFilesToRead<=fileStats.size());
        for (unsigned i=0; i<numFilesToRead; i++)
            fileStats[i]->deserialize(node, mb);
    }
    virtual void getActivityStats(IStatisticGatherer & stats) override
    {
        CMasterActivity::getActivityStats(stats);
        diskAccessCost = calcFileReadCostStats(false);
        if (diskAccessCost)
            stats.addStatistic(StCostFileAccess, diskAccessCost);
    }
    virtual void done() override
    {
        diskAccessCost = calcFileReadCostStats(true);
        CMasterActivity::done();
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
