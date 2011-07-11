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

#ifndef THSortU_HPP
#define THSortU_HPP

#include "slave.hpp"
#include "jio.hpp"
#include "thcrc.hpp"
#include "thmem.hpp"


interface IHThorJoinArg;
interface IOutputMetaData;
class CThorRowArray;

interface ILimitedCompareHelper: public IInterface
{
    virtual void init(
            unsigned atmost,
            IRowStream *strm,
            ICompare *compare,
            ICompare *limcompare
        )=0;

    virtual bool getGroup(CThorRowArray &group,const void *left) = 0;

};

class CThorRowArray;
interface IMulticoreIntercept
{
    virtual void addWork(CThorRowArray *lgroup,CThorRowArray *rgroup)=0;
    virtual void addRow(const void *row)=0;
};

class CActivityBase;
interface IJoinHelper: public IInterface
{
    virtual bool init(
            IRowStream *strmL,
            IRowStream *strmR,      // not used for self join - must be NULL
            IEngineRowAllocator *allocatorL,
            IEngineRowAllocator *allocatorR,
            IOutputMetaData * outputmetaL,   // for XML output 
            bool *_abort,
            CActivityBase *activity=NULL,
            IMulticoreIntercept *mcoreintercept=NULL
        )=0;

    virtual const void *nextRow() = 0;
    virtual rowcount_t getLhsProgress() const = 0;
    virtual rowcount_t getRhsProgress() const = 0;
};

IJoinHelper *createJoinHelper(IHThorJoinArg *helper, const char *activityName, activity_id activityId, IEngineRowAllocator *allocator,bool parallelmatch,bool unsortedoutput);
IJoinHelper *createSelfJoinHelper(IHThorJoinArg *helper, const char *activityName, activity_id activityId, IEngineRowAllocator *allocator,bool parallelmatch,bool unsortedoutput);
IJoinHelper *createDenormalizeHelper(IHThorDenormalizeArg *helper, const char *activityName, ThorActivityKind kind, activity_id activityId, IEngineRowAllocator *allocator);



ILimitedCompareHelper *createLimitedCompareHelper();





#endif
