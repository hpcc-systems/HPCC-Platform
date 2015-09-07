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

#include "thhashdistrib.ipp"

#include "eclhelper.hpp"
#include "mptag.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "jhtree.hpp"

#include "thorport.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"

#define NUMINPARALLEL 16

enum DistributeMode { DM_distrib, DM_dedup, DM_join , DM_groupaggregate, DM_index, DM_redistribute, DM_distribmerge };


class HashDistributeMasterBase : public CMasterActivity
{
    DistributeMode mode;
    mptag_t mptag;
    mptag_t mptag2; // for tag 2
    bool redistribute;
    double skew;
    double targetskew;
public:
    HashDistributeMasterBase(DistributeMode _mode, CMasterGraphElement *info) 
        : CMasterActivity(info), mode(_mode) 
    {
        mptag = TAG_NULL;
        mptag2 = TAG_NULL;
    }

    ~HashDistributeMasterBase()
    {
        if (mptag!=TAG_NULL)
            container.queryJob().freeMPTag(mptag);
        if (mptag2!=TAG_NULL)
            container.queryJob().freeMPTag(mptag2);
    }

protected:
    virtual void init()
    {
        CMasterActivity::init();
        mptag = container.queryJob().allocateMPTag();
        if (mode==DM_join)
            mptag2 = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mptag);
        if (mode==DM_join) 
            dst.append((int)mptag2);
    }
};

class HashDistributeActivityMaster : public HashDistributeMasterBase
{
public:
    HashDistributeActivityMaster(DistributeMode mode, CMasterGraphElement *info) : HashDistributeMasterBase(mode, info) { }
};

class HashJoinDistributeActivityMaster : public HashDistributeActivityMaster
{
    Owned<ProgressInfo> lhsProgress, rhsProgress;

public:
    HashJoinDistributeActivityMaster(DistributeMode mode, CMasterGraphElement *info) : HashDistributeActivityMaster(mode, info)
    {
        lhsProgress.setown(new ProgressInfo);
        rhsProgress.setown(new ProgressInfo);
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        HashDistributeActivityMaster::deserializeStats(node, mb);
        rowcount_t lhsProgressCount, rhsProgressCount;
        mb.read(lhsProgressCount);
        mb.read(rhsProgressCount);
        lhsProgress->set(node, lhsProgressCount);
        rhsProgress->set(node, rhsProgressCount);
    }
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx)
    {
        //This should be an activity stats
        HashDistributeActivityMaster::getEdgeStats(stats, idx);
        assertex(0 == idx);
        lhsProgress->processInfo();
        rhsProgress->processInfo();

        stats.addStatistic(StNumLeftRows, lhsProgress->queryTotal());
        stats.addStatistic(StNumRightRows, rhsProgress->queryTotal());
    }
};

class IndexDistributeActivityMaster : public HashDistributeMasterBase
{
    MemoryBuffer tlkMb;

public:
    IndexDistributeActivityMaster(CMasterGraphElement *info) : HashDistributeMasterBase(DM_index, info) { }
    virtual void init()
    {
        HashDistributeMasterBase::init();

        // JCSMORE should common up some with indexread
        IHThorKeyedDistributeArg *helper = (IHThorKeyedDistributeArg *)queryHelper();

        StringBuffer scoped;
        OwnedRoxieString indexFileName(helper->getIndexFileName());
        queryThorFileManager().addScope(container.queryJob(), indexFileName, scoped);
        Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), indexFileName);
        if (!file)
            throw MakeActivityException(this, 0, "KeyedDistribute: Failed to find key: %s", scoped.str());
        if (0 == file->numParts())
            throw MakeActivityException(this, 0, "KeyedDistribute: Can't distribute based on an empty key: %s", scoped.str());

        checkFormatCrc(this, file, helper->getFormatCrc(), true);
        Owned<IFileDescriptor> fileDesc = file->getFileDescriptor();
        Owned<IPartDescriptor> tlkDesc = fileDesc->getPart(fileDesc->numParts()-1);
        if (!tlkDesc->queryProperties().hasProp("@kind") || 0 != stricmp("topLevelKey", tlkDesc->queryProperties().queryProp("@kind")))
            throw MakeActivityException(this, 0, "Cannot distribute using a non-distributed key: '%s'", scoped.str());
        unsigned location;
        OwnedIFile iFile;
        StringBuffer filePath;
        if (!getBestFilePart(this, *tlkDesc, iFile, location, filePath))
            throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", file->queryLogicalName());
        OwnedIFileIO iFileIO = iFile->open(IFOread);
        assertex(iFileIO);

        tlkMb.append(iFileIO->size());
        ::read(iFileIO, 0, (size32_t)iFileIO->size(), tlkMb);

        addReadFile(file);
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        HashDistributeMasterBase::serializeSlaveData(dst, slave); // have to chain for standard activity data..
        dst.append(tlkMb);
    }
};



class ReDistributeActivityMaster : public HashDistributeMasterBase
{
    mptag_t statstag;

public:
    ReDistributeActivityMaster(CMasterGraphElement *info) : HashDistributeMasterBase(DM_redistribute, info) 
    { 
        statstag = container.queryJob().allocateMPTag();
    }
    ~ReDistributeActivityMaster()
    {
        container.queryJob().freeMPTag(statstag);
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        HashDistributeMasterBase::serializeSlaveData(dst, slave); // have to chain for standard activity data..
        dst.append((int)statstag);
    }
    void process()
    {
        ActPrintLog("ReDistributeActivityMaster::process");
        HashDistributeMasterBase::process();        
        IHThorHashDistributeArg *helper = (IHThorHashDistributeArg *)queryHelper(); 
        unsigned n = container.queryJob().querySlaves();
        MemoryAttr ma;
        offset_t *sizes = (offset_t *)ma.allocate(sizeof(offset_t)*n);
        unsigned i;
        try {
            for (i=0;i<n;i++) {
                if (abortSoon)
                    return;
                CMessageBuffer mb;
#ifdef _TRACE
                ActPrintLog("ReDistribute process, Receiving on tag %d",statstag);
#endif
                rank_t sender;
                if (!receiveMsg(mb, RANK_ALL, statstag, &sender)||abortSoon) 
                    return;
#ifdef _TRACE
                ActPrintLog("ReDistribute process, Received size from %d",sender);
#endif
                sender--;
                assertex((unsigned)sender<n);
                mb.read(sizes[sender]);
            }
            ActPrintLog("ReDistributeActivityMaster::process sizes got");
            for (i=0;i<n;i++) {
                CMessageBuffer mb;
                mb.append(n*sizeof(offset_t),sizes);
#ifdef _TRACE
                ActPrintLog("ReDistribute process, Replying to node %d tag %d",i+1,statstag);
#endif
                if (!queryJobChannel().queryJobComm().send(mb, (rank_t)i+1, statstag))
                    return;
            }
            // check if any max skew broken
            double maxskew = helper->getTargetSkew();
            if (maxskew>helper->getSkew()) {
                offset_t tot = 0;
                for (i=0;i<n;i++) 
                    tot += sizes[i];
                offset_t avg = tot/n;
                for (i=0;i<n;i++) {
                    double r = ((double)sizes[i]-(double)avg)/(double)avg;
                    if ((r>=maxskew)||(-r>maxskew)) {
                        throw MakeActivityException(this, TE_DistributeFailedSkewExceeded, "DISTRIBUTE maximum skew exceeded (node %d has %" I64F "d, average is %" I64F "d)",i+1,sizes[i],avg);
                    }               
                }
            }

        }
        catch (IException *e) {
            ActPrintLog(e,"ReDistribute");
            throw;
        }
        ActPrintLog("ReDistributeActivityMaster::process exit");
    }
    void abort()
    {
        HashDistributeMasterBase::abort();
        cancelReceiveMsg(RANK_ALL, statstag);
    }
};


CActivityBase *createHashDistributeActivityMaster(CMasterGraphElement *container)
{
    if (container&&(((IHThorHashDistributeArg *)container->queryHelper())->queryHash()==NULL))
        return new ReDistributeActivityMaster(container);
    else
        return new HashDistributeActivityMaster(DM_distrib, container);
}

CActivityBase *createDistributeMergeActivityMaster(CMasterGraphElement *container)
{
    return new HashDistributeActivityMaster(DM_distribmerge, container);
}

CActivityBase *createHashDedupMergeActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new HashDistributeActivityMaster(DM_dedup, container);
}

CActivityBase *createHashJoinActivityMaster(CMasterGraphElement *container)
{
        return new HashJoinDistributeActivityMaster(DM_join, container);        
}

CActivityBase *createHashAggregateActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new HashDistributeActivityMaster(DM_groupaggregate, container);      
}

CActivityBase *createKeyedDistributeActivityMaster(CMasterGraphElement *container)
{
    return new IndexDistributeActivityMaster(container);        
}
