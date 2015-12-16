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

#ifndef TSortS_HPP
#define TSortS_HPP

//#define TRACE_PARTITION

#include "thor.hpp"
#include "jsocket.hpp"
#include "jmisc.hpp"
#include "jsort.hpp"
#include "mptag.hpp"
#include "mpbase.hpp"

interface ISortKeySerializer;
interface IRowInterfaces;
interface IThorDataLink;


class IThorSorter: public IInterface
{
public:
    virtual void Gather(
        IRowInterfaces *_rowif,
        IRowStream *in,
        ICompare *icompare,
        ICompare *icollate,
        ICompare *icollateupper,
        ISortKeySerializer *keyserializer, 
        const void *partitionrow, 
        bool nosort, 
        bool unstable, 
        bool &abort,
        IRowInterfaces *_auxrowif
        )=0;
    virtual IRowStream * startMerge(rowcount_t &totalrows)=0;
    virtual void stopMerge()=0;
    virtual unsigned __int64 getStatistic(StatisticKind kind) = 0;
};

interface IDiskUsage;
interface ICommunicator;
interface IOutputRowSerializer;

interface ISocketRowWriter: extends IRowWriter
{
    virtual bool bufferSent()=0;
    virtual void stop()=0;
};

class CActivityBase;
IThorSorter *CreateThorSorter(CActivityBase *activity, SocketEndpoint &ep,IDiskUsage *iDiskUsage,ICommunicator *clusterComm, mptag_t _mpTagRPC);
IRowStream *ConnectMergeRead(unsigned id,IRowInterfaces *rowif,SocketEndpoint &nodeaddr,rowcount_t startrec,rowcount_t numrecs);
ISocketRowWriter *ConnectMergeWrite(IRowInterfaces *rowif,ISocket *socket,size32_t bufsize,rowcount_t &startrec,rowcount_t &numrecs);
#define SOCKETSERVERINC                    1
#define NUMSLAVESOCKETS                    2


interface ISortedInput: extends IInterface // reads rows from sorted local data - returns whole rows
{
    virtual void setNode(unsigned node,unsigned start,unsigned len)=0;
    // assumes input split N way
    virtual size32_t readbuf(unsigned node,offset_t pos,MemoryBuffer &mb, size32_t bufsize)=0; // buffsize is just suggested value

};


interface ISortSlaveBase  // for global merging 
{
    virtual IRowStream *createMergeInputStream(rowcount_t sstart, rowcount_t _snum) = 0;
    virtual size32_t getTransferBlockSize() = 0;
    virtual unsigned getTransferPort() = 0;
    virtual void startMerging(IArrayOf<IRowStream> &readers, rowcount_t _totalrows) = 0;
};



interface IMergeTransferServer: extends IInterface
{
    virtual void start() = 0;
    virtual rowcount_t merge(unsigned mapsize,rowcount_t *map,rowcount_t *mapupper,
                            unsigned num,SocketEndpoint* endpoints,
                            unsigned partno
                           ) = 0;
    virtual void setRowIF(IRowInterfaces *rowif)=0;
    virtual void stop() = 0;
    virtual void subjoin() = 0;
};

IMergeTransferServer *createMergeTransferServer(ISortSlaveBase *parent);

#endif
