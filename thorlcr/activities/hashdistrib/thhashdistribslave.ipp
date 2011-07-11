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

#ifndef _THHASHDISTRIBSLAVE_IPP
#define _THHASHDISTRIBSLAVE_IPP

#include "platform.h"
#include "jsocket.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"

interface IRowStreamWithMetaData: extends IRowStream
{   // currently fixed size data only
    virtual bool nextRow(const void *&row,void *meta)=0;
};

interface IHashDistributor: extends IInterface
{
    virtual IRowStream *connect(IRowStream *in, IHash *_ihash, ICompare *_icompare)=0;
    virtual IRowStreamWithMetaData *connect(IRowStreamWithMetaData *in, size32_t sizemeta, IHash *_ihash, ICompare *_icompare)=0;
    virtual void disconnect(bool stop)=0;
    virtual void removetemp()=0;
    virtual void join()=0;
    virtual void setBufferSizes(unsigned _sendBufferSize, unsigned _outputBufferSize, unsigned _pullBufferSize) = 0;
};

interface IStopInput;
IHashDistributor *createHashDistributor(
    CActivityBase *activity,
    ICommunicator &comm, 
    mptag_t tag, 
    IRowInterfaces *_rowif, 
    const bool &abort,
    bool dedup,
    IStopInput *istop);

CThorRowAggregator *mergeLocalAggs(CActivityBase &activity, IHThorRowAggregator &helper, IHThorHashAggregateExtra &helperExtra, CThorRowAggregator *localAggTable, mptag_t mptag, memsize_t maxMem, bool grow, bool ordered);

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
