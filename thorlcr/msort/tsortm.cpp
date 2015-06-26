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

// MSort Master           

#include "platform.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdarg.h>
#ifdef _WIN32
#include <process.h>
#endif
#include <limits.h>

#include "jlib.hpp"
#include "jflz.hpp"
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include "thorport.hpp"

#include "jsocket.hpp"
#include "jthread.hpp"
#include "jsort.hpp"

#include "thexception.hpp"
#include "thgraph.hpp"

#include "tsorts.hpp"
#include "tsortmp.hpp"

#include "tsortm.hpp"


#ifdef _TESTING
#define _TRACE
#endif

#ifdef _DEBUG
//#define _MEMTRACE
#endif
#include "tsorta.hpp"

#ifdef _DEBUG
//#define TRACE_PARTITION
//#define TRACE_PARTITION2

//#define _TRACE_SKEW_SPLIT
#endif

#define CONNECT_IN_PARALLEL 16
#define INIT_IN_PARALLEL 16
#define CLOSE_IN_PARALLEL 16
#define ASYNC_PARTIONING
//#define USE_SAMPLE_PARTITIONING   // set to try sample partitioning first


enum NODESTATE { NS_null, NS_gather, NS_subsort, NS_merge, NS_done, NS_failed };

class CSortMaster;


class CSortNode: public SortSlaveMP
{
public:

    CSortMaster     &sorter;
    NODESTATE       state; 
    SocketEndpoint  endpoint;
    unsigned short  mpport;
    mptag_t         mpTagRPC;
    unsigned        beat;
    rowcount_t      numrecs;
    offset_t        slavesize;
    bool            overflow;
    unsigned        scale;     // num times overflowed
    
    CActivityBase *activity;
    
    
    CSortNode(CActivityBase *_activity, ICommunicator *comm,rank_t _rank, SocketEndpoint &sep,mptag_t _mpTagRPC, CSortMaster &_sorter) 
        : sorter(_sorter)
    { 
        endpoint = sep; 
        mpport = getFixedPort(comm->queryGroup().queryNode(_rank).endpoint().port,TPORT_mp); // this is a bit odd 
        mpTagRPC = _mpTagRPC;
        state = NS_null;
        beat=0; 
        activity = _activity;
        overflow = false;
        scale = 1;
        numrecs = 0;
        slavesize = 0;
        assertex(_rank!=RANK_NULL);
        SortSlaveMP::init(comm,_rank,mpTagRPC);
    }

    ~CSortNode()
    {
        try {
            Disconnect();
        }
        catch(IException *e) {
            PrintExceptionLog(e,"Disconnecting sort node");
            e->Release();
        }
    }
    


    bool doConnect(unsigned part,unsigned numnodes)
    {
        try {

            StringBuffer epstr;
            ActPrintLog(activity, "Connect to %s:%d",endpoint.getIpText(epstr).str(),(unsigned)mpport);
            SocketEndpoint ep = endpoint;
            ep.port = mpport;
            Owned<INode> node = createINode(ep);
//          SortSlaveMP::setNode(node);
            if (!SortSlaveMP::Connect(part,numnodes)) {
                state = NS_failed;
                return false;
            }
        }
        catch (IException *e)
        {
            pexception("Connecting",e);
            PrintExceptionLog(e,"Connecting");
            e->Release();
            state = NS_failed;
            return false;
        }
        return true;
    }

    void init()
    {
        state = NS_gather;
        numrecs = 0;
        overflow = false;
        scale = 1;
    }

    void AdjustNumRecs(rowcount_t n);
};

class CTimer
{
public:
    void start()
    {
        cstart = msTick();  
    }
    void stop(const char *title)
    {
        PrintLog("%8.2f : %s",(double)((double)(msTick()-cstart))/1000,title);
        PrintLog("--------------------------------------");
    }
    unsigned cstart;
};


inline byte *dupb(byte *b,size32_t l)
{
    byte *ret = (byte *)malloc(l);
    memcpy(ret,b,l);
    return ret;
}



struct PartitionInfo
{
    size32_t guard;
    Linked<IRowInterfaces> prowif;
    PartitionInfo(CActivityBase *_activity, IRowInterfaces *rowif)
        : splitkeys(*_activity, rowif, true), prowif(rowif)
    {
        nodes = NULL;
        mpports = NULL;
        guard = rowif?rowif->queryRowMetaData()->getMinRecordSize():(size32_t)-1;
    }

    ~PartitionInfo()
    {
        free(nodes);
        free(mpports);
    }

    unsigned        numnodes;
    SocketEndpoint  *nodes;
    unsigned short  *mpports;
    mptag_t mpTagRPC;
    CThorExpandingRowArray splitkeys;
    void init() 
    {
        nodes = NULL;
        mpports = NULL;
        splitkeys.kill();
        numnodes = 0;
    }
    void kill()
    {
        free(nodes);
        free(mpports);
        init();
    }
    bool IsOK()
    {
        // should be more defensive here
        return (numnodes!=0)&&(splitkeys.ordinality()!=0);
    }

    void serialize(MemoryBuffer &mb)
    {
        mb.append(numnodes);
        unsigned i;
        for (i=0;i<numnodes;i++)
            nodes[i].serialize(mb);
        for (i=0;i<numnodes;i++)
            mb.append((unsigned short)mpports[i]);
        mb.append((unsigned)mpTagRPC);
        mb.append(guard);
        splitkeys.serialize(mb);
    }   
    void deserialize(size32_t len,void *src)
    {
        kill();
        MemoryBuffer mb(len,src);
        mb.read(numnodes);
        nodes = (SocketEndpoint *)malloc(numnodes*sizeof(SocketEndpoint));
        unsigned i;
        for (i=0;i<numnodes;i++)
            nodes[i].deserialize(mb);
        mpports = (unsigned short *)malloc(numnodes*sizeof(unsigned short));
        for (i=0;i<numnodes;i++) 
            mb.read(mpports[i]);
        unsigned t;
        mb.read(t);
        mpTagRPC = (mptag_t)t;
        size32_t left = mb.remaining();
        size32_t dsguard;
        mb.read(dsguard);
        if (guard!=dsguard)
            throw MakeStringException(-1,"SORT: PartitionInfo meta info mismatch(%d,%d)",guard,dsguard);
        splitkeys.kill();
        splitkeys.deserialize(left, mb.readDirect(left));
    }
};



typedef CopyReferenceArrayOf<CSortNode> NodeArray;

class CSortMaster : public IThorSorterMaster, public CSimpleInterface
{ 
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    CActivityBase *activity;
    NodeArray slaves;
    bool sorted;
    rowcount_t total;
    rowcount_t stotal;  // scaled total (~= to real spilled total)
    offset_t totalmem;
    unsigned numnodes;
    rowcount_t minrecsonnode;
    rowcount_t maxrecsonnode;
    ICompare *icompare;
    ISortKeySerializer *keyserializer;  // used on partition calculation
    PartitionInfo *partitioninfo;
    bool resultorderset;
    Mutex slavemutex;
    char *cosortfilenames;
    size32_t estrecsize;            // serialized
    size32_t maxdeviance;
    Linked<IRowInterfaces> rowif;
    Linked<IRowInterfaces> auxrowif;
    Linked<IRowInterfaces> keyIf;

    int AddSlave(ICommunicator *comm,rank_t rank,SocketEndpoint &endpoint,mptag_t mpTagRPC)
    {
        CSortNode *slave=new CSortNode(activity,comm,rank, endpoint,mpTagRPC,*this);
        slaves.append(*slave);
        return (int)slaves.ordinality()-1;
    }

    void ConnectSlaves()
    {
        ActPrintLog(activity, "CSortMaster::ConnectSlaves");
        class casyncfor: public CAsyncFor
        {
        public:
            casyncfor(CSortMaster &_owner, NodeArray &_slaves) : owner(_owner), slaves(_slaves) { }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);
                if (!slave.doConnect(i,slaves.ordinality())) {
                    char url[100];
                    slave.endpoint.getUrlStr(url,sizeof(url));
                    throw MakeActivityException(owner.activity,TE_CannotConnectToSlave,"CSortMaster::ConnectSlaves: Could not connect to %s",url);
                }
            }
        private:
            NodeArray &slaves;
            CSortMaster &owner;
        } afor(*this,slaves);
        afor.For(slaves.ordinality(), CONNECT_IN_PARALLEL);
    }

    void InitSlaves()
    {
        class casyncfor: public CAsyncFor
        {
        public:
            casyncfor(NodeArray &_slaves) : slaves(_slaves) { }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);          
                slave.init();
            }
        private:
            NodeArray &slaves;
        } afor(slaves);
        afor.For(slaves.ordinality(), INIT_IN_PARALLEL);
    }


    CSortMaster(CActivityBase *_activity) : activity(_activity)
    {
        resultorderset=false;
        numnodes = 0;
        minrecsonnode = 0;
        maxrecsonnode = 0;
        total = 0;
        stotal = 0;
        totalmem = 0;
        icompare = NULL;
        sorted = false;
        cosortfilenames = NULL;
        maxdeviance = 0;
        partitioninfo = NULL;
    }


    void TraceLeft(NODESTATE state)
    {
        unsigned left=slaves.ordinality();
        ForEachItemIn(i,slaves) {
            CSortNode &slave = slaves.item(i);          
            if (slave.state !=state)
                left--;
        }
        ActPrintLog(activity, " Left=%d",left);
        if (left<5) {
            StringBuffer s;
            ForEachItemIn(j,slaves) {
                CSortNode &slave = slaves.item(j);          
                if (slave.state==state) {
                    s.append(' ');
                    slave.endpoint.getIpText(s);
                }
            }
            ActPrintLog(activity, "%s",s.str());
        }
    }


    void SortSetup(IRowInterfaces *_rowif,ICompare *_icompare,ISortKeySerializer *_keyserializer,bool cosort,bool needconnect,const char *_cosortfilenames,IRowInterfaces *_auxrowif)
    {
        ActPrintLog(activity, "Sort setup cosort=%s, needconnect=%s %s",cosort?"true":"false",needconnect?"true":"false",_keyserializer?"has key serializer":"");
        assertex(_icompare);
        rowif.set(_rowif);
        if (_auxrowif&&_auxrowif->queryRowMetaData())
            auxrowif.set(_auxrowif);
        else
            auxrowif.set(_rowif);
        synchronized proc(slavemutex);
        keyserializer = _keyserializer;
        if (keyserializer)
        {
            keyIf.setown(createRowInterfaces(keyserializer->queryRecordSize(), activity->queryContainer().queryId(), activity->queryCodeContext()));
            icompare = keyserializer->queryCompareKey();
        }
        else
        {
            keyIf.set(auxrowif);
            icompare = _icompare;
        }
        sorted = false;
        total = 0;
        stotal = 0;
        totalmem = 0;
        minrecsonnode = RCMAX;
        maxrecsonnode = 0;
        numnodes = slaves.ordinality();
        estrecsize = 100;
        if (!partitioninfo) // if cosort use aux
        {
            if (cosort)
                ActPrintLog(activity, "Cosort with no prior partition");
            partitioninfo = new PartitionInfo(activity, keyIf);
        }
        free(partitioninfo->nodes);
        free(partitioninfo->mpports);
        partitioninfo->numnodes=numnodes;
        partitioninfo->nodes=(SocketEndpoint *)malloc(numnodes*sizeof(SocketEndpoint));
        partitioninfo->mpports=(unsigned short *)malloc(numnodes*sizeof(unsigned short));
        partitioninfo->mpTagRPC = slaves.item(0).mpTagRPC;  // NB all same

        SocketEndpoint *nep=partitioninfo->nodes;
        unsigned short *mpp =partitioninfo->mpports;
        ForEachItemIn(j,slaves) {
            CSortNode &slave = slaves.item(j);          
            *nep = slave.endpoint;
            nep++;
            *mpp = slave.mpport;
            mpp++;
        }
        if (needconnect) // if cosort set, already done!
            ConnectSlaves();
        InitSlaves();
        if (_cosortfilenames&&*_cosortfilenames) {
            cosortfilenames = strdup(_cosortfilenames);
        }
        ForEachItemIn(k,slaves) {
            CSortNode &slave = slaves.item(k);          
            slave.StartGather();
        }
        ActPrintLog(activity, "Sort Setup Complete");
    }

    ~CSortMaster()
    {
        free(cosortfilenames);
        delete partitioninfo;
        synchronized proc(slavemutex);
        ForEachItemInRev(i,slaves) {
            CSortNode *slave = &slaves.item(i);         
            slaves.remove(i);
            delete slave;
        }
    }


    virtual void SortDone()
    {
        ActPrintLog(activity, "Sort Done in");
        synchronized proc(slavemutex);
        if (activity->queryAbortSoon())
            return;
        class casyncfor: public CAsyncFor
        {
            CActivityBase &activity;
        public:
            casyncfor(CActivityBase &_activity, NodeArray &_slaves) : activity(_activity), slaves(_slaves) { wait = false; }
            void Do(unsigned i)
            {
                if (activity.queryAbortSoon())
                    return;
                if (wait)
                    slaves.item(i).CloseWait();
                else
                    slaves.item(i).Close();

            }
            bool wait;
        private:
            NodeArray &slaves;
        } afor(*activity, slaves);
        afor.For(slaves.ordinality(), CLOSE_IN_PARALLEL);
        if (activity->queryAbortSoon())
            return;
        afor.wait = true;
        afor.For(slaves.ordinality(), CLOSE_IN_PARALLEL);
        ActPrintLog(activity, "Sort Done");
    }


    bool GetNode(unsigned num,SocketEndpoint &endpoint)
    {
        ActPrintLog(activity, "GetNode %u",num);
        if (num<slaves.ordinality()) {
            CSortNode &slave = slaves.item(num);            
            endpoint = slave.endpoint;
            endpoint.port++;    // for socket server
            return true;
        }
        return false;
    }

    unsigned NumActiveNodes()
    {
        return slaves.ordinality();
    }


    unsigned __int64 CalcMinMax(OwnedConstThorRow &min, OwnedConstThorRow &max)
    {
        // initialize min/max keys
        rowcount_t tot=0;
        unsigned i;
        size32_t ers = 0;
        unsigned ersn=0;
        for (i=0;i<numnodes;i++)
        {
            CSortNode &slave = slaves.item(i);
            if (slave.numrecs==0)
                continue;
            void *p = NULL;
            size32_t retlen = 0;
            size32_t avrecsize=0;
            rowcount_t num=slave.GetMinMax(retlen,p,avrecsize);
            if (avrecsize)
            {
                ers += avrecsize;       // should probably do mode but this is OK
                ersn++;
            }
            tot += num;
            if (num>0)
            {
                OwnedConstThorRow slaveMin, slaveMax;
                RtlDynamicRowBuilder rowBuilder(keyIf->queryRowAllocator());
                CThorStreamDeserializerSource dsz(retlen, p);
                size32_t sz = keyIf->queryRowDeserializer()->deserialize(rowBuilder, dsz);
                slaveMin.setown(rowBuilder.finalizeRowClear(sz));
                sz = keyIf->queryRowDeserializer()->deserialize(rowBuilder, dsz);
                slaveMax.setown(rowBuilder.finalizeRowClear(sz));
                free(p);
                if (!min.get()||(icompare->docompare(min, slaveMin)>0))
                    min.setown(slaveMin.getClear());
                if (!max.get()||(icompare->docompare(max, slaveMax)<0))
                    max.setown(slaveMax.getClear());
            }
        }
        if (ersn)
            estrecsize = ers/ersn;
        else
            estrecsize = 100;
#ifdef _TRACE
        if (min)
            traceKey(keyIf->queryRowSerializer(),"Min",min);
        if (max)
            traceKey(keyIf->queryRowSerializer(),"Max",max);
        if (min&&max)
        {
            int cmp=icompare->docompare(min,max);
            if (cmp==0) 
                ActPrintLog(activity, "Min == Max : All keys equal!");
            else if (cmp>0)
                ActPrintLog(activity, "ERROR: Min > Max!");
        }
        ActPrintLog(activity, "Tot = %" I64F "d", tot);
#endif
        return tot;
    }



    static CriticalSection ECFcrit;
    static CThorExpandingRowArray *ECFarray;
    static ICompare *ECFcompare;
    static int elemCompareFunc(unsigned const *p1, unsigned const *p2)
    {
        return ECFcompare->docompare(ECFarray->query(*p1), ECFarray->query(*p2));
    }


    rowcount_t *CalcPartitionUsingSampling()
    {   // doesn't support between
#define OVERSAMPLE 16
        OwnedMalloc<rowcount_t> splitMap(numnodes*numnodes, true);
        unsigned numsplits=numnodes-1;
        if (total==0) {
            // no partition info!
            partitioninfo->kill();
            return splitMap.getClear();
        }
        unsigned averagesamples = OVERSAMPLE*numnodes;  
        rowcount_t averagerecspernode = (rowcount_t)(total/numnodes);
        CriticalSection asect;
        CThorExpandingRowArray sample(*activity, keyIf, true);
        class casyncfor1: public CAsyncFor
        {
            CSortMaster &owner;
            NodeArray &slaves;
            CThorExpandingRowArray &sample;
            CriticalSection &asect;
            unsigned averagesamples;
            rowcount_t averagerecspernode;
        public:
            casyncfor1(CSortMaster &_owner, NodeArray &_slaves, CThorExpandingRowArray &_sample, unsigned _averagesamples, rowcount_t _averagerecspernode, CriticalSection &_asect)
                : owner(_owner), slaves(_slaves), sample(_sample), asect(_asect)
            { 
                averagesamples = _averagesamples;
                averagerecspernode = _averagerecspernode;
            }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);
                unsigned slavesamples = averagerecspernode?((unsigned)((averagerecspernode/2+averagesamples*slave.numrecs)/averagerecspernode)):1;
                //PrintLog("%d samples for %d",slavesamples,i);
                if (slavesamples)
                {
                    size32_t samplebufsize;
                    void *samplebuf=NULL;
                    slave.GetMultiNthRow(slavesamples, samplebufsize, samplebuf);
                    MemoryBuffer mb;
                    fastLZDecompressToBuffer(mb, samplebuf);
                    free(samplebuf);
                    CriticalBlock block(asect);
                    CThorStreamDeserializerSource d(mb.length(), mb.toByteArray());
                    while (!d.eos())
                    {
                        RtlDynamicRowBuilder rowBuilder(owner.keyIf->queryRowAllocator());
                        size32_t sz = owner.keyIf->queryRowDeserializer()->deserialize(rowBuilder, d);
                        sample.append(rowBuilder.finalizeRowClear(sz));
                    }
                }
            }
        } afor1(*this, slaves,sample,averagesamples,averagerecspernode,asect);
        afor1.For(numnodes, 20, true);
#ifdef TRACE_PARTITION2
        {
            ActPrintLog(activity, "partition points");
            for (unsigned i=0;i<sample.ordinality();i++) {
                const byte *k = sample.query(i);
                StringBuffer str;
                str.appendf("%d: ",i);
                traceKey(rowif->queryRowSerializer(),str.str(),k);
            }
        }
#endif
        unsigned numsamples = sample.ordinality();
        offset_t ts=sample.serializedSize();
        estrecsize = numsamples?((size32_t)(ts/numsamples)):100;
        sample.sort(*icompare, activity->queryMaxCores());
        CThorExpandingRowArray mid(*activity, keyIf, true);
        if (numsamples) // could shuffle up empty nodes here
        {
            for (unsigned i=0;i<numsplits;i++)
            {
                unsigned pos = (unsigned)(((count_t)numsamples*(i+1))/((count_t)numsplits+1));
                const void *r = sample.get(pos);
                mid.append(r);
            }
        }
#ifdef TRACE_PARTITION2
        {
            ActPrintLog(activity, "merged partitions");
            for (unsigned i=0;i<mid.ordinality();i++)
            {
                const void *k = mid.query(i);
                StringBuffer str;
                str.appendf("%d: ",i);
                traceKey(keyIf->queryRowSerializer(),str.str(),(const byte *)k);
            }
        }
#endif
        // calculate split map
        size32_t mdl=0;
        MemoryBuffer mdmb;
        mid.serializeCompress(mdmb);
        mdl = mdmb.length();
        const byte *mdp=(const byte *)mdmb.bufferBase();
        unsigned i = 0;
        class casyncfor2: public CAsyncFor
        {
            NodeArray &slaves;
            size32_t mdl;
            const byte *mdp;
        public:
            casyncfor2(NodeArray &_slaves,size32_t _mdl,const byte *_mdp)
                : slaves(_slaves)
            { 
                mdl = _mdl;
                mdp = _mdp;
            }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);
                if (slave.numrecs!=0)
                    slave.MultiBinChopStart(mdl,mdp,CMPFN_NORMAL);
            }
        } afor2(slaves,mdl,mdp);
        afor2.For(numnodes, 20, true);
        class casyncfor3: public CAsyncFor
        {
            NodeArray &slaves;
            rowcount_t *splitmap;
            unsigned numnodes;
            unsigned numsplits;
        public:
            casyncfor3(NodeArray &_slaves,rowcount_t *_splitmap,unsigned _numnodes,unsigned _numsplits)
                : slaves(_slaves)
            { 
                splitmap = _splitmap;
                numnodes = _numnodes;
                numsplits = _numsplits;
            }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);
                if (slave.numrecs!=0) {
                    rowcount_t *res=splitmap+(i*numnodes);
                    slave.MultiBinChopStop(numsplits,res);
                    res[numnodes-1] = slave.numrecs;
                }
            }
        } afor3(slaves, splitMap, numnodes, numsplits);
        afor3.For(numnodes, 20, true);
#ifdef _TRACE
#ifdef TRACE_PARTITION
        for (i=0;i<numnodes;i++) {
            StringBuffer str;
            str.appendf("%d: ",i);
            for (unsigned j=0;j<numnodes;j++) {
                str.appendf("%" RCPF "d, ",splitMap[j+i*numnodes]);
            }
            PrintLog("%s",str.str());
        }
#endif
#endif
        return splitMap.getClear();
    }


    rowcount_t *CalcPartition(bool logging)
    {
        CriticalBlock block(ECFcrit);
        // this is a bit long winded

        OwnedConstThorRow mink;
        OwnedConstThorRow maxk;
        // so as won't overflow
        OwnedMalloc<rowcount_t> splitmap(numnodes*numnodes, true);
        if (CalcMinMax(mink, maxk)==0)
        {
            // no partition info!
            partitioninfo->kill();
            return splitmap.getClear();
        }
        unsigned numsplits=numnodes-1;
        CThorExpandingRowArray emin(*activity, keyIf, true);
        CThorExpandingRowArray emax(*activity, keyIf, true);
        CThorExpandingRowArray totmid(*activity, keyIf, true);
        ECFarray = &totmid;
        ECFcompare = icompare;
        CThorExpandingRowArray mid(*activity, keyIf, true);
        unsigned i;
        unsigned j;
        for(i=0;i<numsplits;i++)
        {
            emin.append(mink.getLink());
            emax.append(maxk.getLink());
        }
        UnsignedArray amid;
        unsigned iter=0;
        try
        {
            MemoryBuffer mbmn;
            MemoryBuffer mbmx;
            MemoryBuffer mbmd;
            loop
            {
#ifdef _TRACE
                iter++;
                ActPrintLog(activity, "Split: %d",iter);
#endif
                emin.serializeCompress(mbmn.clear());
                emax.serializeCompress(mbmx.clear());
                class casyncfor: public CAsyncFor
                {
                    NodeArray &slaves;
                    const MemoryBuffer &mbmn;
                    const MemoryBuffer &mbmx;
                public:
                    casyncfor(NodeArray &_slaves,const MemoryBuffer &_mbmn,const MemoryBuffer &_mbmx) 
                        : slaves(_slaves),mbmn(_mbmn), mbmx(_mbmx)
                    { 
                    }
                    void Do(unsigned i)
                    {
                        CSortNode &slave = slaves.item(i);
                        if (slave.numrecs!=0)
                            slave.GetMultiMidPointStart(mbmn.length(),mbmn.bufferBase(),mbmx.length(),mbmx.bufferBase());
                    }
                } afor(slaves,mbmn,mbmx);
                afor.For(numnodes, 20, true);
                Semaphore *nextsem = new Semaphore[numnodes];
                CriticalSection nextsect;

                totmid.kill();
                class casyncfor2: public CAsyncFor
                {
                    NodeArray &slaves;
                    CThorExpandingRowArray &totmid;
                    Semaphore *nextsem;
                    unsigned numsplits;
                    IRowInterfaces *keyIf;
                public:
                    casyncfor2(NodeArray &_slaves, CThorExpandingRowArray &_totmid, unsigned _numsplits, Semaphore *_nextsem, IRowInterfaces *_keyIf)
                        : slaves(_slaves), totmid(_totmid), keyIf(_keyIf)
                    { 
                        nextsem = _nextsem;
                        numsplits = _numsplits;
                    }
                    void Do(unsigned i)
                    {
                        CSortNode &slave = slaves.item(i);
                        void *p = NULL;
                        size32_t retlen=0;
                        if (slave.numrecs!=0) 
                            slave.GetMultiMidPointStop(retlen,p);
                        if (i)
                            nextsem[i-1].wait();
                        try
                        {
                            unsigned base = totmid.ordinality();
                            if (p)
                            {
                                MemoryBuffer mb;
                                fastLZDecompressToBuffer(mb, p);
                                free(p);
                                CThorStreamDeserializerSource d(mb.length(), mb.toByteArray());
                                while (!d.eos())
                                {
                                    RtlDynamicRowBuilder rowBuilder(keyIf->queryRowAllocator());
                                    bool nullRow;
                                    d.read(sizeof(bool),&nullRow);
                                    if (nullRow)
                                        totmid.append(NULL);
                                    else
                                    {
                                        size32_t sz = keyIf->queryRowDeserializer()->deserialize(rowBuilder, d);
                                        totmid.append(rowBuilder.finalizeRowClear(sz));
                                    }
                                }
                            }
                            while (totmid.ordinality()-base<numsplits)
                                totmid.append(NULL);
                        }
                        catch (IException *)
                        {
                            // must ensure signal to avoid other threads in this asyncfor deadlocking
                            nextsem[i].signal();
                            throw;
                        }
                        nextsem[i].signal();
                    }
                } afor2(slaves, totmid, numsplits, nextsem, keyIf);
                afor2.For(numnodes, 20);

                delete [] nextsem;
                mid.kill();
                mbmn.clear();
                mbmx.clear();
                for (i=0;i<numsplits;i++)
                {
                    amid.kill();
                    unsigned k;
                    unsigned t = i;
                    for (k=0;k<numsplits;k++)
                    {
                        const void *row = totmid.query(t);
                        if (row)
                            amid.append(t);
                        t += numsplits;
                    }
                    amid.sort(elemCompareFunc);
                    while (amid.ordinality() && (icompare->docompare(emin.query(i), totmid.query(amid.item(0)))>=0))
                        amid.remove(0);
                    while (amid.ordinality()&&(icompare->docompare(emax.query(i),totmid.query(amid.item(amid.ordinality()-1)))<=0))
                        amid.remove(amid.ordinality()-1);
                    if (amid.ordinality()) {
                        unsigned mi = amid.item(amid.ordinality()/2);
#ifdef _DEBUG
                        if (logging)
                        {
                            MemoryBuffer buf;
                            const void *b = totmid.query(mi);
                            ActPrintLog(activity, "%d: %d %d",i,mi,amid.ordinality()/2);
                            traceKey(keyIf->queryRowSerializer(),"mid",b);
                        }
#endif
                        mid.append(totmid.get(mi));
                    }
                    else
                        mid.append(emin.get(i));
                }

                // calculate split map
                mid.serializeCompress(mbmd.clear());
                for (i=0;i<numnodes;i++)
                {
                    CSortNode &slave = slaves.item(i);
                    if (slave.numrecs!=0)
                        slave.MultiBinChopStart(mbmd.length(),(const byte *)mbmd.bufferBase(),CMPFN_NORMAL);
                }
                mbmd.clear();
                for (i=0;i<numnodes;i++)
                {
                    CSortNode &slave = slaves.item(i);
                    if (slave.numrecs!=0)
                    {
                        rowcount_t *res=splitmap+(i*numnodes);
                        slave.MultiBinChopStop(numsplits,res);
                        res[numnodes-1] = slave.numrecs;
                    }
                }

                CThorExpandingRowArray newmin(*activity, keyIf, true);
                CThorExpandingRowArray newmax(*activity, keyIf, true);
                unsigned __int64 maxerror=0;
                unsigned __int64 nodewanted = (stotal/numnodes); // Note scaled total
                unsigned __int64 variancelimit = estrecsize?maxdeviance/estrecsize:0;
                if (variancelimit>nodewanted/50)
                    variancelimit=nodewanted/50; // 2%
                for (i=0;i<numsplits;i++)
                {
                    unsigned __int64 tot = 0;
                    unsigned __int64 loc = 0;
                    for (j=0;j<numnodes;j++) {
                        unsigned scale = slaves.item(j).scale; 
                        unsigned midx = i+j*numnodes;
                        tot += scale*(__int64)splitmap[midx];
                        unsigned __int64 tloc = splitmap[midx];
                        if (i) 
                            tloc -= splitmap[midx-1];
                        loc += tloc*scale;
                    }
                    unsigned __int64 wanted = nodewanted*(i+1); // scaled total assumed >> numnodes
#ifdef _DEBUG
                    if (logging) 
                        ActPrintLog(activity, "  wanted = %" CF "d, %stotal = %" CF "d, loc = %" CF "d, locwanted = %" CF "d\n",wanted,(total!=stotal)?"scaled ":"",tot,loc,nodewanted);
#endif
                    bool isdone=false;
                    unsigned __int64 error = (loc>nodewanted)?(loc-nodewanted):(nodewanted-loc);
                    if (error>maxerror)
                        maxerror = error;
                    if (wanted<tot)
                    {
                        newmin.append(emin.get(i));
                        newmax.append(mid.get(i));
                    }
                    else if (wanted>tot)
                    {
                        newmin.append(mid.get(i));
                        newmax.append(emax.get(i));
                    }
                    else
                    {
                        newmin.append(emin.get(i));
                        newmax.append(emax.get(i));
                    }
                }
                if (emin.equal(icompare,newmin)&&emax.equal(icompare,newmax))
                    break; // reached steady state 
                if ((maxerror*10000<nodewanted)||((iter>3)&&(maxerror<variancelimit))) // within .01% or within variancelimit
                {
                    ActPrintLog(activity, "maxerror = %" CF "d, nodewanted = %" CF "d, variancelimit=%" CF "d, estrecsize=%u, maxdeviance=%u",
                             maxerror,nodewanted,variancelimit,estrecsize,maxdeviance);
                    break;
                }
                emin.transfer(newmin);
                emax.transfer(newmax);
            } 
        }
        catch (IOutOfMemException *e)
        {
            StringBuffer str;
            e->errorMessage(str);
            str.append("\nKey size too large for distributed sort?");
            e->Release();
            throw MakeActivityException(activity,-1,"%s",str.str());
        }
        partitioninfo->splitkeys.transfer(mid);
        partitioninfo->numnodes = numnodes;
#ifdef _DEBUG
        if (logging)
        {
            for (unsigned i=0;i<numnodes;i++)
            {
                StringBuffer str;
                str.appendf("%d: ",i);
                for (j=0;j<numnodes;j++)
                {
                    str.appendf("%" RCPF "d, ",splitmap[j+i*numnodes]);
                }
                ActPrintLog(activity, "%s",str.str());
            }
        }
#endif
        return splitmap.getClear();
    }


    rowcount_t *UsePartitionInfo(PartitionInfo &pi, bool uppercmp)
    {
        unsigned i;
#ifdef _TRACE
#ifdef TRACE_PARTITION
        ActPrintLog(activity, "UsePartitionInfo %s",uppercmp?"upper":"");
        for (i=0;i<pi.splitkeys.ordinality();i++)
        {
            StringBuffer s;
            s.appendf("%d: ",i);
            traceKey(pi.prowif->queryRowSerializer(), s.str(), pi.splitkeys.query(i));
        }
#endif
#endif
        // first find split points
        unsigned numnodes = pi.numnodes;
        unsigned numsplits = numnodes-1;
        OwnedMalloc<rowcount_t> splitMap(numnodes*numnodes, true);
        OwnedMalloc<rowcount_t> res(numsplits);
        unsigned j;
        rowcount_t *mapp=splitMap;
        for (i=0;i<numnodes;i++)
        {
            CSortNode &slave = slaves.item(i);
            if (numsplits>0)
            {
                MemoryBuffer mb;
                pi.splitkeys.serialize(mb);
                assertex(pi.splitkeys.ordinality()==numsplits);
                slave.MultiBinChop(mb.length(),(const byte *)mb.bufferBase(),numsplits,res,uppercmp?CMPFN_UPPER:CMPFN_COLLATE);
                rowcount_t *resp = res;
                rowcount_t p=*resp;
                *mapp = p;
                resp++;
                mapp++;
                for (j=1;j<numsplits;j++)
                {
                    rowcount_t n = *resp;
                    *mapp = n;
                    if (p>n)
                    {
                        ActPrintLog(activity, "ERROR: Split positions out of order!");
                        throw MakeActivityException(activity, TE_SplitPostionsOutOfOrder,"CSortMaster::UsePartitionInfo: Split positions out of order!");
                    }
                    resp++;
                    mapp++;
                    p = n;
                }
            }
            *mapp = slave.numrecs; // final entry is number in node
            mapp++;
        }
#ifdef _TRACE
#ifdef TRACE_PARTITION
        ActPrintLog(activity, "UsePartitionInfo result");
        rowcount_t *p = splitMap;
        for (i=0;i<numnodes;i++)
        {
            StringBuffer s;
            s.appendf("%d: ",i);
            for (j=0;j<numnodes;j++)
            {
                s.appendf(" %" RCPF "d,",*p);
                p++;
            }
            ActPrintLog(activity, "%s",s.str());
        }
#endif
#endif
        return splitMap.getClear();
    }

    void CalcExtPartition()
    {
        // I think this dependent on row being same format as meta

        unsigned numsplits=numnodes-1;
        CThorExpandingRowArray splits(*activity, keyIf, true);
        char *s=cosortfilenames;
        unsigned i;
        for(i=0;i<numnodes;i++)
        {
            char *e=strchr(s,'|');
            if (e) 
                *e = 0;
            else if (i!=numnodes-1)
                return;
            if (i)
            {
                CSortNode &slave = slaves.item(i);
                byte *rowmem;
                size32_t rowsize;
                if (!slave.FirstRowOfFile(s,rowsize,rowmem))
                    return;
                OwnedConstThorRow row;
                row.deserialize(keyIf,rowsize,rowmem);
                splits.append(row.getClear());
                free(rowmem);
            }
            s = s+strlen(s)+1;
        }
        partitioninfo->splitkeys.transfer(splits);
        partitioninfo->numnodes = numnodes;
        free(cosortfilenames);
        cosortfilenames = NULL;
    }

    void CalcPreviousPartition()
    {
        ActPrintLog(activity, "Previous partition");
        unsigned numsplits=numnodes-1;
        CThorExpandingRowArray splits(*activity, keyIf, true);
        unsigned i;
        for(i=1;i<numnodes;i++)
        {
            CSortNode &slave = slaves.item(i);
            byte *rowmem;
            size32_t rowsize;
            if (!slave.FirstRowOfFile("",rowsize,rowmem))
                return;
            OwnedConstThorRow row;
            row.deserialize(keyIf, rowsize, rowmem);
            if (row&&rowsize)
            {
                StringBuffer n;
                n.append(i).append(": ");
                traceKey(keyIf->queryRowSerializer(),n,row);
            }
            splits.append(row.getClear());
            free(rowmem);
        }
        partitioninfo->splitkeys.transfer(splits);
        partitioninfo->numnodes = numnodes;
    }

    IThorException *CheckSkewed(unsigned __int64 threshold, double skewWarning, double skewError, unsigned n, rowcount_t total, rowcount_t max)
    {
        if (n<=0)
            return NULL;
        if (total==0)
            return NULL;
        if (skewError<0)
            return NULL;
        if (threshold==0)
            threshold = 1000000000;
        if (skewError<0.000000001)
            skewError = 1.0/(double) n;
        double cSkew = ((double)n*(double)max/(double)total - 1.0) / ((double)n-1.0);
        ActPrintLog(activity, "Skew check: Threshold %" I64F "d/%" I64F "d  Skew: %f/[warning=%f, error=%f]",
                  (unsigned __int64)max*(unsigned __int64)estrecsize,threshold,cSkew,skewWarning,skewError);
        if ((unsigned __int64)max*(unsigned __int64)estrecsize>threshold)
        {
            if (cSkew > skewError)
            {
                Owned<IThorException> e = MakeActivityException(activity, TE_SkewError, "Exceeded skew limit: %f, estimated skew: %f", skewError, cSkew);
                return e.getClear();
            }
            else if (skewWarning && cSkew > skewWarning)
            {
                Owned<IThorException> e = MakeActivityWarning(activity, TE_SkewWarning, "Exceeded skew warning limit: %f, estimated skew: %f", skewWarning, cSkew);
                activity->fireException(e);
            }
        }
        return NULL;
    }

    void Sort(unsigned __int64 threshold, double skewWarning, double skewError, size32_t _maxdeviance,bool canoptimizenullcolumns, bool usepartitionrow, bool betweensort, unsigned minisortthresholdmb)
    {
        memsize_t minisortthreshold = 1024*1024*(memsize_t)minisortthresholdmb;
        // JCSMORE - size a bit arbitary
        if ((minisortthreshold>0)&&(roxiemem::getTotalMemoryLimit()/2<minisortthreshold))
            minisortthreshold = roxiemem::getTotalMemoryLimit()/2;
        if (skewError>0.0 && skewWarning > skewError)
        {
            ActPrintLog(activity, "WARNING: Skew warning %f > skew error %f", skewWarning, skewError);
            skewWarning = 0.0;
        }
        ActPrintLog(activity, "Sort: canoptimizenullcolumns=%s, usepartitionrow=%s, betweensort=%s skewWarning=%f skewError=%f minisortthreshold=%" I64F "d",canoptimizenullcolumns?"true":"false",usepartitionrow?"true":"false",betweensort?"true":"false",skewWarning,skewError,(__int64)minisortthreshold);
        assertex(partitioninfo);
        maxdeviance = _maxdeviance;
        unsigned i;
        bool overflowed = false;
        unsigned numnodes = slaves.ordinality();
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            slave.GetGatherInfo(slave.numrecs,slave.slavesize,slave.scale,keyserializer!=NULL);
            assertex(slave.scale);
            slave.overflow = slave.scale>1;
            if (slave.overflow)
                overflowed = true;
            total += slave.numrecs;
            stotal += slave.numrecs*slave.scale;
            totalmem += slave.slavesize;
            if (slave.numrecs>maxrecsonnode)
                maxrecsonnode = slave.numrecs;
            if (slave.numrecs<minrecsonnode)
                minrecsonnode = slave.numrecs;
        }
        ActPrintLog(activity,"Total recs in mem = %" RCPF "d scaled recs= %" RCPF "d size = %" CF "d bytes, minrecsonnode = %" RCPF "d, maxrecsonnode = %" RCPF "d",total,stotal,totalmem,minrecsonnode,maxrecsonnode);
        if (!usepartitionrow&&!betweensort&&(totalmem<minisortthreshold)&&!overflowed) {
            ActPrintLog(activity, "Performing minisort of %" RCPF "d records", total);
            sorted = MiniSort(total);
            return;
        }
#ifdef USE_SAMPLE_PARTITIONING
        bool usesampling = true;        
#endif
        bool useAux = false; // JCSMORE using existing partioning and auxillary rowIf (only used if overflow)
        loop
        {
            OwnedMalloc<rowcount_t> splitMap, splitMapUpper;
            CTimer timer;
            if (numnodes>1)
            {
                timer.start();
                if (cosortfilenames)
                {
                    useAux = true;
                    CalcExtPartition();
                    canoptimizenullcolumns = false;
                }
                if (usepartitionrow)
                {
                    useAux = true;
                    CalcPreviousPartition();
                    canoptimizenullcolumns = false;
                }
                if (partitioninfo->IsOK())
                {
                    useAux = true;
                    splitMap.setown(UsePartitionInfo(*partitioninfo, betweensort));
                    if (betweensort)
                    {
                        splitMapUpper.setown(UsePartitionInfo(*partitioninfo, false));
                        canoptimizenullcolumns = false;
                    }
                }
                else
                {
                    // check for small sort here
                    if ((skewError<0.0)&&!betweensort)
                    {
                        splitMap.setown(CalcPartitionUsingSampling());
                        skewError = -skewError;
#ifdef USE_SAMPLE_PARTITIONING
                        usesampling = false;
#endif
                    }
                    else
                    {
                        if (skewError<0.0)
                            skewError = -skewError;

#ifdef USE_SAMPLE_PARTITIONING
                        if (usesampling)
                            splitMap.setown(CalcPartitionUsingSampling());
                        else
#endif
#ifdef TRACE_PARTITION
                            splitMap.setown(CalcPartition(true));
#else
                            splitMap.setown(CalcPartition(false));
#endif
                    }
                    if (!partitioninfo->splitkeys.checkSorted(icompare))
                    {
                        ActPrintLog(activity, "ERROR: Split keys out of order!");
                        partitioninfo->splitkeys.sort(*icompare, activity->queryMaxCores());
                    }
                }
                timer.stop("Calculating split map");
            }
            OwnedMalloc<SocketEndpoint> endpoints(numnodes);
            SocketEndpoint *epp = endpoints;
            for (i=0;i<numnodes;i++)
            {
                CSortNode &slave = slaves.item(i);
                *epp = slave.endpoint;
                epp++;
            }
            if (numnodes>1)
            {
                // minimize logging
                unsigned numspilt = 0;
                UnsignedArray spilln;
                for (i=0;i<numnodes;i++)
                {
                    CSortNode &slave = slaves.item(i);
                    if (slave.overflow)
                    {
                        while (spilln.ordinality()<slave.scale)
                            spilln.append(0);
                        spilln.replace(spilln.item(slave.scale-1)+1,slave.scale-1);
                        numspilt++;
                    }
                }
                if (numspilt==0)
                    ActPrintLog(activity, "Gather - no nodes spilt to disk");
                else {
                    unsigned mostspilt = 0;
                    unsigned spiltmax = 0;
                    ForEachItemIn(smi,spilln)
                    {
                        if (spilln.item(smi)>spiltmax)
                        {
                            spiltmax = spilln.item(smi);
                            mostspilt = smi;
                        }
                    }
                    ActPrintLog(activity, "Gather - %d nodes spilt to disk, most %d times",numspilt,mostspilt);
                    for (i=0;i<numnodes;i++)
                    {
                        CSortNode &slave = slaves.item(i);
                        if (slave.scale!=mostspilt+1)
                        {
                            char url[100];
                            slave.endpoint.getUrlStr(url,sizeof(url));
                            ActPrintLog(activity, "Gather - node %s spilled %d times to disk",url,slave.scale-1);
                        }
                    }
                    MemoryBuffer mbsk;
                    partitioninfo->splitkeys.serialize(mbsk);
                    for (i=0;i<numnodes;i++)
                    {
                        CSortNode &slave = slaves.item(i);
                        if (slave.overflow) 
                            slave.OverflowAdjustMapStart(numnodes,splitMap+i*numnodes,mbsk.length(),(const byte *)mbsk.bufferBase(),CMPFN_COLLATE,useAux);
                    }
                    for (i=0;i<numnodes;i++)
                    {
                        CSortNode &slave = slaves.item(i);
                        if (slave.overflow) 
                            slave.AdjustNumRecs(slave.OverflowAdjustMapStop(numnodes,splitMap+i*numnodes));
                    }
                    if (splitMapUpper.get())
                    {
                        for (i=0;i<numnodes;i++)
                        {
                            CSortNode &slave = slaves.item(i);
                            if (slave.overflow) 
                                slave.OverflowAdjustMapStart(numnodes,splitMapUpper+i*numnodes,mbsk.length(),(const byte *)mbsk.bufferBase(),CMPFN_UPPER,useAux);
                        }
                        for (i=0;i<numnodes;i++)
                        {
                            CSortNode &slave = slaves.item(i);
                            if (slave.overflow) 
                                slave.OverflowAdjustMapStop(numnodes,splitMapUpper+i*numnodes);
                        }
                    }
                }

                OwnedMalloc<rowcount_t> tot(numnodes, true);
                rowcount_t max=0;
                unsigned imax=numnodes;
                for (i=0;i<imax;i++){
                    unsigned j;
                    for (j=0;j<numnodes;j++)
                    {
                        if (splitMapUpper)
                            tot[i]+=splitMapUpper[i+j*numnodes];
                        else
                            tot[i]+=splitMap[i+j*numnodes];
                        if (i)
                            tot[i]-=splitMap[i+j*numnodes-1];
                    }
                    if (tot[i]>max)
                        max = tot[i];
                    if (!betweensort&&canoptimizenullcolumns&&(tot[i]==0))
                    {
                        for (j=0;j<numnodes;j++)
                        {
                            for (unsigned k=i+1;k<numnodes;k++)
                            {
                                splitMap[k+j*numnodes-1] = splitMap[k+j*numnodes];
                            }
                        }
                        imax--;
                        i--;
                    }
                }
                for (i=0;i<numnodes;i++)
                {
                    CSortNode &slave = slaves.item(i);
                    char url[100];
                    slave.endpoint.getUrlStr(url,sizeof(url));
                    ActPrintLog(activity, "Split point %d: %" RCPF "d rows on %s", i, tot[i], url);
                }
                Owned<IThorException> e = CheckSkewed(threshold,skewWarning,skewError,numnodes,total,max);
                if (e)
                {
#ifdef _TRACE_SKEW_SPLIT
                    splitMap.clear();
                    splitMap.setown(CalcPartition(true));
#endif
#ifdef USE_SAMPLE_PARTITIONING
                    if (usesampling)
                    {
                        ActPrintLog(activity, "Partioning using sampling failed, trying iterative partitioning"); 
                        usesampling = false;
                        continue;
                    }
#endif
                    throw e.getClear();
                }
                ActPrintLog(activity, "Starting Merge of %" RCPF "d records",total);
                for (i=0;i<numnodes;i++)
                {
                    CSortNode &slave = slaves.item(i);
                    char url[100];
                    slave.endpoint.getUrlStr(url,sizeof(url));
                    if (splitMapUpper)
                        slave.MultiMergeBetween(numnodes*numnodes,splitMap,splitMapUpper,numnodes,endpoints);
                    else
                        slave.MultiMerge(numnodes*numnodes,splitMap,numnodes,endpoints);
    //              ActPrintLog(activity, "Merge %d started: %d rows on %s",i,tot[i],url);
                }
            }
            else
            {
                CSortNode &slave = slaves.item(0);
                slave.SingleMerge();
                ActPrintLog(activity, "Merge started");
            }
            sorted = true;
            break;
        }
    }

    bool MiniSort(rowcount_t _totalrows)
    {
        class casyncfor1: public CAsyncFor
        {
            rowcount_t totalrows;
        public:
            casyncfor1(NodeArray &_slaves,rowcount_t _totalrows) 
                : slaves(_slaves) 
            { 
                totalrows = _totalrows;
            }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);      
                slave.StartMiniSort(totalrows);
            }
        private:
            NodeArray &slaves;
        } afor1(slaves,_totalrows);
        afor1.For(slaves.ordinality(), CONNECT_IN_PARALLEL);
        return true;
    }
};

CThorExpandingRowArray *CSortMaster::ECFarray;
ICompare *CSortMaster::ECFcompare;
CriticalSection CSortMaster::ECFcrit; 


IThorSorterMaster *CreateThorSorterMaster(CActivityBase *activity)
{
    return new CSortMaster(activity);
}



void CSortNode::AdjustNumRecs(rowcount_t num)
{
    rowcount_t old = numrecs;
    numrecs = num;
    sorter.total += num-old;
    if (num>sorter.maxrecsonnode)
        sorter.maxrecsonnode = num;
}











        

