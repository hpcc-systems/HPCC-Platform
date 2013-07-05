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

#ifndef _THHASHDISTRIBSLAVE_IPP
#define _THHASHDISTRIBSLAVE_IPP

#include "platform.h"
#include "jsocket.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"


interface IHashDistributor : extends IInterface
{
    virtual IRowStream *connect(IRowInterfaces *rowIf, IRowStream *in, IHash *ihash, ICompare *icompare)=0;
    virtual void disconnect(bool stop)=0;
    virtual void join()=0;
    virtual void setBufferSizes(unsigned sendBufferSize, unsigned outputBufferSize, unsigned pullBufferSize) = 0;
    virtual void abort()=0;
};

interface IStopInput;
IHashDistributor *createHashDistributor(
    CActivityBase *activity,
    ICommunicator &comm, 
    mptag_t tag, 
    bool dedup,
    IStopInput *istop);

CThorRowAggregator *mergeLocalAggs(Owned<IHashDistributor> &distributor, CActivityBase &activity, IHThorRowAggregator &helper, IHThorHashAggregateExtra &helperExtra, CThorRowAggregator *localAggTable, mptag_t mptag, bool ordered);

activityslaves_decl CActivityBase *createHashDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashDistributeMergeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashLocalDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashJoinSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashAggregateSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createIndexDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createReDistributeSlave(CGraphElementBase *container);

#endif
