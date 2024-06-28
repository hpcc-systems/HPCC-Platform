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

#ifndef THSortU_HPP
#define THSortU_HPP

#include "slave.hpp"
#include "jio.hpp"


interface IHThorJoinArg;
interface IOutputMetaData;
class CThorExpandingRowArray;

interface ILimitedCompareHelper: public IInterface
{
    virtual void init(
            unsigned atmost,
            IRowStream *strm,
            ICompare *compare,
            ICompare *limcompare
        )=0;

    virtual bool getGroup(CThorExpandingRowArray &group,const void *left) = 0;
};

interface IMulticoreIntercept
{
    virtual void addWork(CThorExpandingRowArray *lgroup, CThorExpandingRowArray *rgroup)=0;
    virtual void addRow(const void *row)=0;
};

class CActivityBase;
interface IJoinHelper: public IRowStream
{
    virtual bool init(
            IRowStream *strmL,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *allocatorL,
            IEngineRowAllocator *allocatorR,
            IOutputMetaData * outputmetaL,   // for XML output 
            IMulticoreIntercept *mcoreintercept=NULL
        )=0;

    virtual rowcount_t getLhsProgress() const = 0;
    virtual rowcount_t getRhsProgress() const = 0;
    virtual const void *nextRow() = 0;
    virtual void stop() = 0;
    virtual void gatherStats(CRuntimeStatisticCollection & stats) const = 0;
};

IJoinHelper *createJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IThorRowInterfaces *rowIf, bool parallelmatch, bool unsortedoutput);
IJoinHelper *createSelfJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IThorRowInterfaces *rowIf, bool parallelmatch, bool unsortedoutput);
IJoinHelper *createDenormalizeHelper(CActivityBase &activity, IHThorDenormalizeArg *helper, IThorRowInterfaces *rowIf);

ILimitedCompareHelper *createLimitedCompareHelper();

//Included here so this can be shared between join and lookup join.
class JoinMatchStats
{
public:
    void gatherStats(CRuntimeStatisticCollection & stats) const
    {
        //Left and right progress could be added here.
        if (maxLeftGroupSize)
            stats.addStatistic(StNumMatchLeftRowsMax, maxLeftGroupSize);
        if (maxRightGroupSize)
            stats.addStatistic(StNumMatchRightRowsMax, maxRightGroupSize);
        if (numMatchCandidates)
            stats.addStatistic(StNumMatchCandidates, numMatchCandidates);
        if (maxMatchCandidates)
            stats.addStatistic(StNumMatchCandidatesMax, maxMatchCandidates);
    }

    void noteGroup(rowcount_t numLeft, rowcount_t numRight)
    {
        rowcount_t numCandidates = numLeft * numRight;
        if (numLeft > maxLeftGroupSize)
            maxLeftGroupSize = numLeft;
        if (numRight > maxRightGroupSize)
            maxRightGroupSize = numRight;
        numMatchCandidates += numCandidates;
        if (numCandidates > maxMatchCandidates)
            maxMatchCandidates = numCandidates;
    }

public:
    stat_type maxLeftGroupSize = 0;
    stat_type maxRightGroupSize = 0;
    stat_type numMatchCandidates = 0;
    stat_type maxMatchCandidates = 0;
};

#endif
