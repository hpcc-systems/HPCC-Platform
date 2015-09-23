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
    rowcount_t limit;
    IHThorIndexReadBaseArg *indexBaseHelper;
    Owned<CSlavePartMapping> mapping;
    bool nofilter;
    ProgressInfoArray progressInfoArr;
    UnsignedArray progressKinds;
    Owned<ProgressInfo> inputProgress;
    StringBuffer fileName;

    rowcount_t aggregateToLimit()
    {
        rowcount_t total = 0;
        ICommunicator &comm = queryJobChannel().queryJobComm();
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
            if (total > limit)
                break;
        }
        return total;
    }
    void prepareKey(IDistributedFile *index)
    {
        IDistributedFile *f = index;
        IDistributedSuperFile *super = f->querySuperFile();

        unsigned nparts = f->numParts(); // includes tlks if any, but unused in array
        performPartLookup.ensure(nparts);

        bool checkTLKConsistency = NULL != super && 0 != (TIRsorted & indexBaseHelper->getFlags());
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
        unsigned width = f->numParts()-1;
        assertex(width);
        unsigned tlkCrc = 0;
        bool first = true;
        unsigned superSubIndex=0;
        bool fileCrc = false, rowCrc = false;
        loop
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
                        throw MakeActivityException(this, 0, "Sorted output on super files comprising of non coparitioned sub keys is not supported (TLK's do not match)");
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
                        rfn.getRemotePath(remotePath);
                        unsigned crc = 0;
                        part->getCrc(crc);
                        keyIndex.setown(createKeyIndex(remotePath.str(), crc, false, false));
                        break;
                    }
                }
                if (!keyIndex)
                    throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", index->queryLogicalName());
            
                unsigned fixedSize = indexBaseHelper->queryDiskRecordSize()->querySerializedDiskMeta()->getFixedSize(); // used only if fixed
                Owned <IKeyManager> tlk = createKeyManager(keyIndex, fixedSize, NULL);
                indexBaseHelper->createSegmentMonitors(tlk);
                tlk->finishSegmentMonitors();
                tlk->reset();
                while (tlk->lookup(false))
                {
                    if (tlk->queryFpos())
                        performPartLookup.replace(true, (aindex_t)(super?super->numSubFiles(true)*(tlk->queryFpos()-1)+superSubIndex:tlk->queryFpos()-1));
                }
            }
            if (!super||!iter->next())
                break;
            superSubIndex++;
            f = &iter->query();
            if (width != f->numParts()-1)
                throw MakeActivityException(this, 0, "Super key %s, with mixture of sub key width are not supported.", f->queryLogicalName());
        }
    }

public:
    CIndexReadBase(CMasterGraphElement *info) : CMasterActivity(info)
    {
        indexBaseHelper = (IHThorIndexReadBaseArg *)queryHelper();
        limit = RCMAX;
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
        progressKinds.append(StNumIndexSeeks);
        progressKinds.append(StNumIndexScans);
        ForEachItemIn(l, progressKinds)
            progressInfoArr.append(*new ProgressInfo);
        inputProgress.setown(new ProgressInfo);
        reInit = 0 != (indexBaseHelper->getFlags() & (TIRvarfilename|TIRdynamicfilename));
    }
    virtual void init()
    {
        CMasterActivity::init();
        nofilter = false;
        OwnedRoxieString helperFileName = indexBaseHelper->getFileName();
        StringBuffer expandedFileName;
        queryThorFileManager().addScope(container.queryJob(), helperFileName, expandedFileName);
        fileName.set(expandedFileName);
        Owned<IDistributedFile> index = queryThorFileManager().lookup(container.queryJob(), helperFileName, false, 0 != (TIRoptional & indexBaseHelper->getFlags()), true);
        if (index)
        {
            bool localKey = index->queryAttributes().getPropBool("@local");

            if (container.queryLocalData() && !localKey)
                throw MakeActivityException(this, 0, "Index Read cannot be LOCAL unless supplied index is local");

            nofilter = 0 != (TIRnofilter & indexBaseHelper->getFlags());
            if (index->queryAttributes().getPropBool("@local"))
                nofilter = true;
            else
            {
                IDistributedSuperFile *super = index->querySuperFile();
                IDistributedFile *sub = super ? &super->querySubFile(0,true) : index.get();
                if (sub && 1 == sub->numParts())
                    nofilter = true;
            }   
            checkFormatCrc(this, index, indexBaseHelper->getFormatCrc(), true);
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
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
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
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        rowcount_t progress;
        mb.read(progress);
        inputProgress->set(node, progress);
        ForEachItemIn(p, progressKinds)
        {
            unsigned __int64 st;
            mb.read(st);
            progressInfoArr.item(p).set(node, st);
        }
    }
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx)
    {
        //This should be an activity stats
        CMasterActivity::getEdgeStats(stats, idx);

        stats.addStatistic(StNumIndexRowsRead, inputProgress->queryTotal());
        ForEachItemIn(p, progressInfoArr)
        {
            ProgressInfo &progress = progressInfoArr.item(p);
            progress.processInfo();
            stats.addStatistic((StatisticKind)progressKinds.item(p), progress.queryTotal());
        }
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

class CIndexReadActivityMaster : public CIndexReadBase
{
    IHThorIndexReadArg *helper;

    void processKeyedLimit()
    {
        rowcount_t total = aggregateToLimit();
        if (total > limit)
        {
            if (0 == (TIRkeyedlimitskips & helper->getFlags()))
            {
                if (0 == (TIRkeyedlimitcreates & helper->getFlags()))
                    helper->onKeyedLimitExceeded(); // should throw exception
            }
        }
        CMessageBuffer msg;
        msg.append(total);
        ICommunicator &comm = queryJobChannel().queryJobComm();
        unsigned slaves = container.queryJob().querySlaves();
        unsigned s=0;
        for (; s<slaves; s++)
        {
            verifyex(comm.send(msg, s+1, mpTag));
        }
    }
public:
    CIndexReadActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexReadArg *)queryHelper();
    }
    virtual void init()
    {
        CIndexReadBase::init();
        if (!container.queryLocalOrGrouped())
        {
            if (helper->canMatchAny())
                limit = (rowcount_t)helper->getKeyedLimit();
        }
    }
    virtual void process()
    {
        if (limit != RCMAX)
            processKeyedLimit();
    }
};


CActivityBase *createIndexReadActivityMaster(CMasterGraphElement *info)
{
    return new CIndexReadActivityMaster(info);
}


class CIndexCountActivityMaster : public CIndexReadBase
{
    IHThorIndexCountArg *helper;
    bool totalCountKnown;
    rowcount_t totalCount;

public:
    CIndexCountActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexCountArg *)queryHelper();
        totalCount = 0;
        totalCountKnown = false;
        if (!container.queryLocalOrGrouped())
        {
            if (helper->canMatchAny())
                limit = (rowcount_t)helper->getChooseNLimit();
            else
                totalCountKnown = true; // totalCount = 0
        }
    }
    virtual void process()
    {
        if (container.queryLocalOrGrouped())
            return;
        if (totalCountKnown) return;
        rowcount_t total = aggregateToLimit();
        if (limit != RCMAX && total > limit)
            total = limit;
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
    IHThorIndexNormalizeArg *helper;

    void processKeyedLimit()
    {
        rowcount_t total = aggregateToLimit();
        if (total > limit)
        {
            if (0 == (TIRkeyedlimitskips & helper->getFlags()))
            {
                if (0 == (TIRkeyedlimitcreates & helper->getFlags()))
                    helper->onKeyedLimitExceeded(); // should throw exception
            }
        }
        CMessageBuffer msg;
        msg.append(total);
        ICommunicator &comm = queryJobChannel().queryJobComm();
        unsigned slaves = container.queryJob().querySlaves();
        unsigned s=0;
        for (; s<slaves; s++)
        {
            verifyex(comm.send(msg, s+1, mpTag));
        }
    }
public:
    CIndexNormalizeActivityMaster(CMasterGraphElement *info) : CIndexReadBase(info)
    {
        helper = (IHThorIndexNormalizeArg *)queryHelper();
    }
    virtual void init()
    {
        CIndexReadBase::init();
        if (!container.queryLocalOrGrouped())
        {
            if (helper->canMatchAny())
                limit = (rowcount_t)helper->getKeyedLimit();
        }
    }
    virtual void process()
    {
        if (limit != RCMAX)
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
    virtual void process()
    {
        if (container.queryLocalOrGrouped())
            return;
        Owned<IRowInterfaces> rowIf = createRowInterfaces(helper->queryOutputMeta(), queryActivityId(), queryCodeContext());                
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
