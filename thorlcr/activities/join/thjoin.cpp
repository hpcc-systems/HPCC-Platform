/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#include "jlib.hpp"
#include "eclhelper.hpp"

#include "thjoin.ipp"

#include "thor.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thorport.hpp"

#include "tsortm.hpp"

#define JOIN_SOCKETS 2

class JoinActivityMaster : public CMasterActivity
{
    IThorSorterMaster *imaster;
    IHThorJoinArg *helper;
    bool islocal;
    bool rightpartition;
    unsigned selfJoinWarnLevel, lastMsgTime;
    mptag_t mpTagRPC, barrierMpTag;
    Owned<IBarrier> barrier;
    Owned<ProgressInfo> lhsProgress, rhsProgress;

    bool nosortPrimary()
    {
        if (ALWAYS_SORT_PRIMARY)
            return false;
        return (rightpartition?helper->isRightAlreadySorted():helper->isLeftAlreadySorted());
    }

    bool nosortSecondary()
    {
        return (rightpartition?helper->isLeftAlreadySorted():helper->isRightAlreadySorted());
    }
    class cLimitedCmp: implements ICompare  // special to improve limited match partitioning
    {
        ICompare *base;
        ICompare *pcmp;
    public:
        cLimitedCmp(ICompare *_base, ICompare *_pcmp)
        {
            base = _base;
            pcmp = _pcmp;
        }

        int docompare(const void *l,const void *r) const
        {
            int ret = base->docompare(l,r);
            if (ret!=0)
                return ret;
            ret = pcmp->docompare(l,r);
            if (ret==1)         // this effectively checks if 1st char different
                return 1;
            if (ret==-1)
                return -1;
            return 0;
        }
    } *climitedcmp;
public:
    JoinActivityMaster(CMasterGraphElement * info, bool local) : CMasterActivity(info)
    {
        ActPrintLog("JoinActivityMaster");
        lhsProgress.setown(new ProgressInfo);
        rhsProgress.setown(new ProgressInfo);
        helper = NULL;
        islocal = local;
        imaster = NULL;
        selfJoinWarnLevel = INITIAL_SELFJOIN_MATCH_WARNING_LEVEL;
        lastMsgTime = 0;
        mpTagRPC = container.queryJob().allocateMPTag();
        barrierMpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJob().createBarrier(barrierMpTag));
        climitedcmp = NULL;
    }
    ~JoinActivityMaster()
    {
        container.queryJob().freeMPTag(mpTagRPC);
        container.queryJob().freeMPTag(barrierMpTag);
        delete climitedcmp;
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (!islocal)
        {
            serializeMPtag(dst, mpTagRPC);
            serializeMPtag(dst, barrierMpTag);
        }
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        ActPrintLog("preStart");
        if (!islocal) {
            imaster = CreateThorSorterMaster(this);
            unsigned s=0;
            for (; s<container.queryJob().querySlaves(); s++)
            {
                SocketEndpoint ep;
                ep.deserialize(queryInitializationData(s));
                
                imaster->AddSlave(&container.queryJob().queryJobComm(), s+1,ep,mpTagRPC);
            }
        }
    }
    virtual void process()
    {
        ActPrintLog("process");
        CMasterActivity::process();
        if (!islocal)
        {
            helper = (IHThorJoinArg *)queryHelper();
            StringBuffer skewV;
            double skewError;
            container.queryJob().getWorkUnitValue("overrideSkewError", skewV);
            if (skewV.length())
                skewError = atof(skewV.str());
            else
            {
                skewError = helper->getSkew();
                if (!skewError)
                {
                    container.queryJob().getWorkUnitValue("defaultSkewError", skewV.clear());
                    if (skewV.length())
                        skewError = atof(skewV.str());
                }
            }
            container.queryJob().getWorkUnitValue("defaultSkewWarning", skewV.clear());
            double defaultSkewWarning = skewV.length() ? atof(skewV.str()) : 0;
            double skewWarning = defaultSkewWarning;
            unsigned __int64 skewThreshold = container.queryJob().getWorkUnitValueInt("overrideSkewThreshold", 0);
            if (!skewThreshold)
            {
                skewThreshold = helper->getThreshold();
                if (!skewThreshold)
                    skewThreshold = container.queryJob().getWorkUnitValueInt("defaultSkewThreshold", 0);
            }
            try
            {
                size32_t maxdeviance = getOptUInt(THOROPT_SORT_MAX_DEVIANCE, 10*1024*1024);
                rightpartition = (container.getKind() == TAKjoin) && ((helper->getJoinFlags()&JFpartitionright)!=0);

                CGraphElementBase *primaryInput = NULL;
                CGraphElementBase *secondaryInput = NULL;
                ICompare *primaryCompare = NULL, *secondaryCompare = NULL;
                ISortKeySerializer *primaryKeySerializer = NULL;
                if (!rightpartition)
                {
                    primaryInput = container.queryInput(0);
                    primaryCompare = helper->queryCompareLeft();
                    primaryKeySerializer = helper->querySerializeLeft();
                    if (container.getKind() != TAKselfjoin)
                    {
                        secondaryInput = container.queryInput(1);
                        secondaryCompare = helper->queryCompareRight();
                    }
                }
                else
                {
                    primaryInput = container.queryInput(1);
                    secondaryInput = container.queryInput(0);
                    primaryCompare = helper->queryCompareRight();
                    secondaryCompare = helper->queryCompareLeft();
                    primaryKeySerializer = helper->querySerializeRight();
                }
                if (helper->getJoinFlags()&JFslidingmatch) // JCSMORE shouldn't be necessary
                    primaryKeySerializer = NULL;
                Owned<IRowInterfaces> primaryRowIf = createRowInterfaces(primaryInput->queryHelper()->queryOutputMeta(), queryActivityId(), queryCodeContext());
                Owned<IRowInterfaces> secondaryRowIf;
                if (secondaryInput)
                    secondaryRowIf.setown(createRowInterfaces(secondaryInput->queryHelper()->queryOutputMeta(), queryActivityId(), queryCodeContext()));

                bool betweenjoin = (helper->getJoinFlags()&JFslidingmatch)!=0;
                if (container.getKind() == TAKselfjoin)
                {
                    if (betweenjoin)
                        throw MakeActivityException(this, -1, "SELF BETWEEN JOIN not supported"); // Gavin shouldn't generate
                    ICompare *cmpleft = primaryCompare;
                    if ((helper->getJoinFlags()&JFlimitedprefixjoin)&&(helper->getJoinLimit()))
                    {
                        delete climitedcmp;
                        climitedcmp = new cLimitedCmp(helper->queryCompareLeftRight(),helper->queryPrefixCompare());
                        cmpleft = climitedcmp;
                        // partition by L/R
                    }
                    imaster->SortSetup(primaryRowIf, cmpleft, primaryKeySerializer, false, true, NULL, NULL);
                    if (barrier->wait(false)) // local sort complete
                    {
                        try
                        {
                            imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,false,false,false,0);
                        }
                        catch (IThorException *e)
                        {
                            if (TE_SkewError == e->errorCode())
                            {
                                StringBuffer s;
                                Owned<IThorException> e2 = MakeActivityException(this, TE_JoinFailedSkewExceeded, "SELFJOIN failed. %s", e->errorMessage(s).str());
                                e->Release();
                                fireException(e2);
                            }
                            else
                                throw;
                        }
                        ActPrintLog("JOIN waiting for barrier.1");
                        barrier->wait(false);
                        ActPrintLog("JOIN barrier.1 raised");
                    }
                    imaster->SortDone();
                }
                else if (!nosortPrimary()||betweenjoin)
                {
                    Owned<IRowInterfaces> secondaryRowIf = createRowInterfaces(secondaryInput->queryHelper()->queryOutputMeta(), queryActivityId(), queryCodeContext());

                    imaster->SortSetup(primaryRowIf, primaryCompare, primaryKeySerializer, false, true, NULL, NULL);
                    ActPrintLog("JOIN waiting for barrier.1");
                    if (barrier->wait(false))
                    {
                        ActPrintLog("JOIN barrier.1 raised");
                        try
                        {
                            imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,false,false,false,0);
                        }
                        catch (IThorException *e)
                        {
                            if (TE_SkewError == e->errorCode())
                            {
                                StringBuffer s;
                                Owned<IThorException> e2 = MakeActivityException(this, TE_JoinFailedSkewExceeded, "JOIN failed, skewed %s. %s", rightpartition?"RHS":"LHS", e->errorMessage(s).str());
                                e->Release();
                                fireException(e2);
                            }
                            else
                                throw;
                        }
                        ActPrintLog("JOIN waiting for barrier.2");
                        if (barrier->wait(false)) // merge complete
                        {
                            ActPrintLog("JOIN barrier.2 raised");
                            imaster->SortDone();
                            // NB on the cosort should use same serializer as sort (but in fact it only gets used when 0 rows on primary side)
                            imaster->SortSetup(secondaryRowIf, secondaryCompare, primaryKeySerializer, true, false, NULL, NULL); //serializers OK
                            ActPrintLog("JOIN waiting for barrier.3");
                            if (barrier->wait(false)) // local sort complete
                            {
                                ActPrintLog("JOIN barrier.3 raised");
                                try
                                {
                                    imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,false,nosortPrimary(),betweenjoin,0);
                                }
                                catch (IThorException *e)
                                {
                                    if (TE_SkewError == e->errorCode())
                                    {
                                        VStringBuffer s("JOIN failed, %s skewed, based on distribution of %s partition points. ", rightpartition?"LHS":"RHS", rightpartition?"RHS":"LHS");
                                        e->errorMessage(s);
                                        Owned<IThorException> e2 = MakeActivityException(this, TE_JoinFailedSkewExceeded, "%s", s.str());
                                        e->Release();
                                        fireException(e2);
                                    }
                                    else
                                        throw;
                                }
                                ActPrintLog("JOIN waiting for barrier.4");
                                barrier->wait(false); // merge complete
                                ActPrintLog("JOIN barrier.4 raised");
                            }
                        }
                        imaster->SortDone();
                    }
                }
                else // only sort non-partition side
                {
                    imaster->SortSetup(secondaryRowIf, secondaryCompare, primaryKeySerializer, true, true, NULL, primaryRowIf);
                    ActPrintLog("JOIN waiting for barrier.1");
                    if (barrier->wait(false)) // local sort complete
                    {
                        ActPrintLog("JOIN barrier.1 raised");
                        try
                        {
                            imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,false,nosortPrimary(),false,0);
                        }
                        catch (IThorException *e)
                        {
                            if (TE_SkewError == e->errorCode())
                            {
                                VStringBuffer s("JOIN failed. %s skewed, based on distribution of presorted %s partition points. ", rightpartition?"LHS":"RHS", rightpartition?"RHS":"LHS");
                                e->errorMessage(s);
                                Owned<IThorException> e2 = MakeActivityException(this, TE_JoinFailedSkewExceeded, "%s", s.str());
                                e->Release();
                                fireException(e2);
                            }
                            else
                                throw;
                        }
                        ActPrintLog("JOIN waiting for barrier.2");
                        barrier->wait(false); // merge complete
                        ActPrintLog("JOIN barrier.2 raised");
                    }
                    imaster->SortDone();
                }
            }
            catch (IMP_Exception *e)
            {
                if (e->errorCode()!=MPERR_link_closed) 
                    throw;
                ActPrintLogEx(&queryContainer(), thorlog_null, MCwarning, "WARNING: MPERR_link_closed in SortDone");
                e->Release();
            }
            ::Release(imaster);
        }
        ActPrintLog("process exit");
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        rowcount_t lhsProgressCount, rhsProgressCount;
        mb.read(lhsProgressCount);
        lhsProgress->set(node, lhsProgressCount);
        if (TAKselfjoin != container.getKind() && TAKselfjoinlight != container.getKind())
        {
            mb.read(rhsProgressCount);
            rhsProgress->set(node, rhsProgressCount);
        }
    }
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx)
    {
        //This should be an activity stats
        CMasterActivity::getEdgeStats(stats, idx);
        assertex(0 == idx);

        lhsProgress->processInfo();
        stats.addStatistic(StNumLeftRows, lhsProgress->queryTotal());
        if (TAKselfjoin != container.getKind() && TAKselfjoinlight != container.getKind())
        {
            rhsProgress->processInfo();
            stats.addStatistic(StNumRightRows, rhsProgress->queryTotal());
        }
    }
#define MSGTIME (5*60*1000)
    virtual bool fireException(IException *_e)
    {
        IThorException *e = QUERYINTERFACE(_e, IThorException);
        if (NULL != e && (TAKselfjoin == container.getKind() || TAKselfjoinlight == container.getKind()) && TE_SelfJoinMatchWarning == e->errorCode())
        {
            // Output these warning ever if not reported since MSGTIME
            // OR if count is > than scaled threshold.
            unsigned count;
            e->queryData().read(count);
            if (count >= selfJoinWarnLevel)
            {
                selfJoinWarnLevel *= 2;
                Owned<IException> e = MakeActivityWarning(this, -1, "SELFJOIN: Warning %d preliminary matches, join will take some time", count);
                CMasterActivity::fireException(e);
                lastMsgTime = msTick();
            }
            else if (msTick() > lastMsgTime + MSGTIME)
            {
                Owned<IException> e = MakeActivityWarning(this, -1, "SELFJOIN: Warning %d preliminary matches, join will take some time", count);
                CMasterActivity::fireException(e);
                lastMsgTime = msTick();
            }
            return true;
        }
        return CMasterActivity::fireException(_e);
    }
};


CActivityBase *createJoinActivityMaster(CMasterGraphElement *container)
{
    return new JoinActivityMaster(container, container->queryLocalOrGrouped());
}


