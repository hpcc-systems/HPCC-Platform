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

#include "thmsort.ipp"
#include "tsortm.hpp"

#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thorport.hpp"

#define SORT_SOCKETS 2

//
// CMSortActivityMaster
//


class CGroupSortActivityMaster : public CMasterActivity
{
public:
    CGroupSortActivityMaster(CMasterGraphElement * info) : CMasterActivity(info) { }

    virtual void init()
    {
        CMasterActivity::init();
        IHThorSortArg *helper = (IHThorSortArg *)queryHelper();
        IHThorAlgorithm *algo = static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1));
        OwnedRoxieString algoname = algo->getAlgorithm();
        unsigned flags = algo->getAlgorithmFlags();
        if (algoname && (0 != stricmp(algoname, "quicksort")))
        {
            Owned<IException> e = MakeActivityException(this, 0, "Ignoring, unsupported sort order algorithm '%s'", algoname.get());
            reportExceptionToWorkunit(container.queryJob().queryWorkUnit(), e);
        }
    }
};

class CMSortActivityMaster : public CMasterActivity
{
    IThorSorterMaster *imaster;
    mptag_t mpTagRPC, barrierMpTag;
    Owned<IBarrier> barrier;
    StringBuffer cosortfilenames;

public:
    CMSortActivityMaster(CMasterGraphElement *info)
      : CMasterActivity(info)
    {
        mpTagRPC = container.queryJob().allocateMPTag();
        barrierMpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJobChannel().createBarrier(barrierMpTag));
    }
    ~CMSortActivityMaster()
    {
        container.queryJob().freeMPTag(mpTagRPC);
        container.queryJob().freeMPTag(barrierMpTag);
    }

protected:
    virtual void init()
    {
        CMasterActivity::init();
        IHThorSortArg *helper = (IHThorSortArg *)queryHelper();
        IHThorAlgorithm *algo = static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1));
        OwnedRoxieString algoname(algo->getAlgorithm());
        unsigned flags = algo->getAlgorithmFlags();
        if (algoname && (0 != stricmp(algoname, "quicksort")))
        {
            Owned<IException> e = MakeActivityException(this, 0, "Ignoring, unsupported sort order algorithm '%s'", algoname.get());
            reportExceptionToWorkunit(container.queryJob().queryWorkUnit(), e);
        }
        OwnedRoxieString cosortlogname(helper->getSortedFilename());
        if (cosortlogname&&*cosortlogname)
        {
            Owned<IDistributedFile> coSortFile = queryThorFileManager().lookup(container.queryJob(), cosortlogname);
            addReadFile(coSortFile);
            Owned<IFileDescriptor> fileDesc = coSortFile->getFileDescriptor();
            unsigned o;
            for (o=0; o<fileDesc->numParts(); o++)
            {
                Owned<IPartDescriptor> partDesc = fileDesc->getPart(o);
                if (cosortfilenames.length())
                    cosortfilenames.append("|");

                // JCSMORE - picking the primary here, means no automatic use of backup copy, could use RMF's possibly.
                getPartFilename(*partDesc, 0, cosortfilenames);
            }
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        serializeMPtag(dst, mpTagRPC);
        serializeMPtag(dst, barrierMpTag);
    }   
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        ActPrintLog("preStart");
        imaster = CreateThorSorterMaster(this);
        unsigned s=0;
        for (; s<container.queryJob().querySlaves(); s++)
        {
            SocketEndpoint ep;
            ep.deserialize(queryInitializationData(s)); // this is a bit of a Kludge until we get proper MP Thor
            imaster->AddSlave(&queryJobChannel().queryJobComm(), s+1, ep,mpTagRPC);
        }
    }
    virtual void process()
    {
        ActPrintLog("process");

        CMasterActivity::process();

        IHThorSortArg *helper = (IHThorSortArg *)queryHelper();
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

        Owned<IRowInterfaces> rowif = createRowInterfaces(container.queryInput(0)->queryHelper()->queryOutputMeta(),queryActivityId(),queryCodeContext());
        Owned<IRowInterfaces> auxrowif = createRowInterfaces(helper->querySortedRecordSize(),queryActivityId(),queryCodeContext());
        try
        {
            imaster->SortSetup(rowif,helper->queryCompare(),helper->querySerialize(),cosortfilenames.length()!=0,true,cosortfilenames.str(),auxrowif);
            if (barrier->wait(false)) // local sort complete
            {
                size32_t maxdeviance = getOptUInt(THOROPT_SORT_MAX_DEVIANCE, 10*1024*1024);
                try
                {
                    imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,true,false,false,getOptUInt(THOROPT_SMALLSORT));
                }
                catch (IThorException *e)
                {
                    if (TE_SkewError == e->errorCode())
                    {
                        StringBuffer s;
                        Owned<IThorException> e2 = MakeActivityException(this, TE_SortFailedSkewExceeded, "SORT failed. %s", e->errorMessage(s).str());
                        e->Release();
                        fireException(e2);
                    }
                    else
                        throw;
                }
                barrier->wait(false); // merge complete
            }
            imaster->SortDone();
        }
        catch (IException *e)
        {
            ActPrintLog(e, "WARNING: exception during sort");
            throw;
        }
        ::Release(imaster);
        ActPrintLog("process exit");
    }
};

CActivityBase *createSortActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CGroupSortActivityMaster(container);
    else
        return new CMSortActivityMaster(container);
}
