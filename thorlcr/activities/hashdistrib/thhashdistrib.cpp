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

#include "thhashdistrib.ipp"

#include "eclhelper.hpp"
#include "mptag.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "jhtree.hpp"

#include "thorport.hpp"
#include "thbufdef.hpp"
#include "thmem.hpp"
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
        mptag = container.queryJob().allocateMPTag();
        if (mode==DM_join)
            mptag2 = container.queryJob().allocateMPTag();
    }
    virtual void process()
    {
        ActPrintLog("HashDistributeActivityMaster::process");

        CMasterActivity::process();
        ActPrintLog("HashDistributeActivityMaster::process exit");
    }
    virtual void done()
    {
        ActPrintLog("HashDistributeActivityMaster::done");
        CMasterActivity::done();
        ActPrintLog("HashDistributeActivityMaster::done exit");
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
    virtual void getXGMML(unsigned idx, IPropertyTree *edge)
    {
        HashDistributeActivityMaster::getXGMML(idx, edge);
        assertex(0 == idx);
        lhsProgress->processInfo();
        rhsProgress->processInfo();

        StringBuffer label;
        label.append("@progressInput-").append(container.queryInput(0)->queryId());
        edge->setPropInt64(label.str(), lhsProgress->queryTotal());
        label.clear().append("@progressInput-").append(container.queryInput(1)->queryId());
        edge->setPropInt64(label.str(), rhsProgress->queryTotal());
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
        queryThorFileManager().addScope(container.queryJob(), helper->getIndexFileName(), scoped);
        Owned<IDistributedFile> f = queryThorFileManager().lookup(container.queryJob(), helper->getIndexFileName());
        if (!f)
            throw MakeActivityException(this, 0, "KeyedDistribute: Failed to find key: %s", scoped.str());
        if (0 == f->numParts())
            throw MakeActivityException(this, 0, "KeyedDistribute: Can't distribute based on an empty key: %s", scoped.str());

        checkFormatCrc(this, f, helper->getFormatCrc(), true);
        Owned<IFileDescriptor> fileDesc = f->getFileDescriptor();
        Owned<IPartDescriptor> tlkDesc = fileDesc->getPart(fileDesc->numParts()-1);
        if (!tlkDesc->queryProperties().hasProp("@kind") || 0 != stricmp("topLevelKey", tlkDesc->queryProperties().queryProp("@kind")))
            throw MakeActivityException(this, 0, "Cannot distribute using a non-distributed key: '%s'", scoped.str());
        unsigned location;
        OwnedIFile iFile;
        StringBuffer filePath;
        if (!getBestFilePart(this, *tlkDesc, iFile, location, filePath))
            throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", f->queryLogicalName());
        OwnedIFileIO iFileIO = iFile->open(IFOread);
        assertex(iFileIO);

        tlkMb.append(iFileIO->size());
        ::read(iFileIO, 0, (size32_t)iFileIO->size(), tlkMb);

        queryThorFileManager().noteFileRead(container.queryJob(), f);
    }

    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
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
                if (!container.queryJob().queryJobComm().send(mb, (rank_t)i+1, statstag))
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
                        throw MakeActivityException(this, TE_DistributeFailedSkewExceeded, "DISTRIBUTE maximum skew exceeded (node %d has %"I64F"d, average is %"I64F"d)",i+1,sizes[i],avg);
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
    return new HashDistributeActivityMaster(DM_groupaggregate, container);      
}

CActivityBase *createKeyedDistributeActivityMaster(CMasterGraphElement *container)
{
    return new IndexDistributeActivityMaster(container);        
}
