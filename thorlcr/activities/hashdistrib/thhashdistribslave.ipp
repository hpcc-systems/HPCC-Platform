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

#ifndef _THHASHDISTRIBSLAVE_IPP
#define _THHASHDISTRIBSLAVE_IPP

#include "platform.h"
#include "jsocket.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"


interface IHashDistributor : extends IInterface
{
    virtual IRowStream *connect(IThorRowInterfaces *rowIf, IRowStream *in, IHash *ihash, ICompare *icompare, ICompare *compareBest)=0;
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
    bool isAll,
    IStopInput *istop, const char *id=NULL); // id optional, used for tracing to identify which distributor if >1 in activity

// IAggregateTable allows rows to be added and aggregated and retrieved via a IRowStream
interface IAggregateTable : extends IInterface
{
    virtual void init(IEngineRowAllocator *_rowAllocator) = 0;
    virtual void reset() = 0;
    virtual void addRow(const void *row) = 0;
    virtual unsigned elementCount() const = 0;
    virtual IRowStream *getRowStream(bool sorted) = 0;
};
IAggregateTable *createRowAggregator(CActivityBase &activity, IHThorHashAggregateExtra &extra, IHThorRowAggregator &helper);
IRowStream *mergeLocalAggs(Owned<IHashDistributor> &distributor, CSlaveActivity &activity, IHThorRowAggregator &helper, IHThorHashAggregateExtra &helperExtra, IRowStream *localAggTable, mptag_t mptag);

activityslaves_decl CActivityBase *createHashDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWayDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashDistributeMergeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashLocalDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashJoinSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashAggregateSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createIndexDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createFetchSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createReDistributeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createHashDistributedSlave(CGraphElementBase *container);

#endif
