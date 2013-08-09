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
            bool *_abort,
            IMulticoreIntercept *mcoreintercept=NULL
        )=0;

    virtual rowcount_t getLhsProgress() const = 0;
    virtual rowcount_t getRhsProgress() const = 0;
    virtual const void *nextRow() = 0;
    virtual void stop() = 0;
};

IJoinHelper *createJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IRowInterfaces *rowIf, bool parallelmatch, bool unsortedoutput);
IJoinHelper *createSelfJoinHelper(CActivityBase &activity, IHThorJoinArg *helper, IRowInterfaces *rowIf, bool parallelmatch, bool unsortedoutput);
IJoinHelper *createDenormalizeHelper(CActivityBase &activity, IHThorDenormalizeArg *helper, IRowInterfaces *rowIf);



ILimitedCompareHelper *createLimitedCompareHelper();





#endif
