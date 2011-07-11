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

#include "thmsort.ipp"
#include "tsortm.hpp"

#include "eclhelper.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thorport.hpp"

#define SORT_SOCKETS 2

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/msort/thmsort.cpp $ $Id: thmsort.cpp 65756 2011-06-24 15:48:52Z jsmith $");

//
// CMSortActivityMaster
//


class CGroupSortActivityMaster : public CMasterActivity
{
public:
    CGroupSortActivityMaster(CMasterGraphElement * info) : CMasterActivity(info) { }

    virtual void init()
    {
        IHThorSortArg *helper = (IHThorSortArg *)queryHelper();
        IHThorAlgorithm *algo = static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1));
        char const *algoname = algo->queryAlgorithm();
        unsigned flags = algo->getAlgorithmFlags();
        if (algoname && (0 != stricmp(algoname, "quicksort")))
        {
            Owned<IException> e = MakeActivityException(this, 0, "Ignoring, unsupported sort order algorithm '%s'", algoname);
            reportExceptionToWorkunit(container.queryJob().queryWorkUnit(), e);
        }
    }
};

class CMSortActivityMaster : public CMasterActivity
{
    IThorSorterMaster *imaster;
    mptag_t mpTagRPC;
    Owned<IBarrier> barrier;
public:
    CMSortActivityMaster(CMasterGraphElement *info)
      : CMasterActivity(info)
    {
        mpTagRPC = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJob().createBarrier(container.queryJob().allocateMPTag()));
    }

    ~CMSortActivityMaster()
    {
        container.queryJob().freeMPTag(mpTagRPC);
    }

protected:
    virtual void init()
    {
        IHThorSortArg *helper = (IHThorSortArg *)queryHelper();
        IHThorAlgorithm *algo = static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1));
        char const *algoname = algo->queryAlgorithm();
        unsigned flags = algo->getAlgorithmFlags();
        if (algoname && (0 != stricmp(algoname, "quicksort")))
        {
            Owned<IException> e = MakeActivityException(this, 0, "Ignoring, unsupported sort order algorithm '%s'", algoname);
            reportExceptionToWorkunit(container.queryJob().queryWorkUnit(), e);
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTagRPC);
        dst.append((int)barrier->queryTag());
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
            ep.deserialize(queryInitializationData(s)); // this is a bit of a Kludge untill we get proper MP Thor
            imaster->AddSlave(&container.queryJob().queryJobComm(), s+1, ep,mpTagRPC);
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
        StringBuffer cosortfilenames;
        const char *cosortlogname = helper->getSortedFilename();
        if (cosortlogname&&*cosortlogname) {

            Owned<IDistributedFile> file = queryThorFileManager().lookup(container.queryJob(), cosortlogname);
            Owned<IFileDescriptor> fileDesc = file->getFileDescriptor();
            queryThorFileManager().noteFileRead(container.queryJob(), file);
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

        Owned<IRowInterfaces> rowif = createRowInterfaces(container.queryInput(0)->queryHelper()->queryOutputMeta(),queryActivityId(),queryCodeContext());
        Owned<IRowInterfaces> auxrowif = createRowInterfaces(helper->querySortedRecordSize(),queryActivityId(),queryCodeContext());
        try {   
            imaster->SortSetup(rowif,helper->queryCompare(),helper->querySerialize(),cosortfilenames.length()!=0,true,cosortfilenames.toCharArray(),auxrowif);
            if (barrier->wait(false)) { // local sort complete
                size32_t maxdeviance=globals->getPropInt("@sort_max_deviance", 10*1024*1024);
                if (!imaster->Sort(skewThreshold,skewWarning,skewError,maxdeviance,true,false,false,(unsigned)globals->getPropInt("@smallSortThreshold"))) {
                    Owned<IThorException> e = MakeActivityException(this, TE_SortFailedSkewExceeded,"SORT failed, skew exceeded");
                    fireException(e);
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
    virtual void done()
    {
        ActPrintLog("done");
        CMasterActivity::done();
        ActPrintLog("done exit");
    }
};

CActivityBase *createSortActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CGroupSortActivityMaster(container);
    else
        return new CMSortActivityMaster(container);
}
