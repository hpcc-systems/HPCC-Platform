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

#include <mpbase.hpp>
#include <mpcomm.hpp>
#include "thorport.hpp"

#include "jsocket.hpp"
#include "jthread.hpp"
#include "jlib.hpp"
#include "jsort.hpp"
#include "thexception.hpp"
#include "thgraph.hpp"

#define PROGNAME "tsort"

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
    rowmap_t        numrecs;
    memsize_t       memsize;
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
        memsize = 0;
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

    void AdjustNumRecs(rowmap_t n);

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
    PartitionInfo(IRowInterfaces *rowif)
        : splitkeys(rowif,NULL), prowif(rowif)
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
    VarElemArray    splitkeys;
    void init() 
    {
        nodes = NULL;
        mpports = NULL;
        splitkeys.clear();
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
        splitkeys.deserialize(mb.readDirect(left),left,false);
    }   
};
    


MAKEPointerArray(CSortNode,NodeArray);

class CSortMaster: public IThorSorterMaster, public CSimpleInterface
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

    int AddSlave(ICommunicator *comm,rank_t rank,SocketEndpoint &endpoint,mptag_t mpTagRPC)
    {
        CSortNode *slave=new CSortNode(activity,comm,rank, endpoint,mpTagRPC,*this);
        slaves.append(*slave);
        return (int)slaves.ordinality()-1;
    }

    void ConnectSlaves()
    {
        ActPrintLog(activity, "CSortMaster::ConnectSlaves");
#ifdef CONNECT_IN_PARALLEL
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
#else
        ForEachItemIn(i,slaves) {
            CSortNode &slave = slaves.item(i);          
            if (!slave.doConnect(i,slaves.ordinality())) {
                char url[100];
                slave.endpoint.getUrlStr(url,sizeof(url));
                throw MakeActivityException(activity,TE_CannotConnectToSlave,"CSortMaster::ConnectSlaves: Could not connect to %s",url);
            }
        }
#endif
    }

    void InitSlaves()
    {
#ifdef INIT_IN_PARALLEL
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
#else
        ForEachItemIn(i,slaves) {
            CSortNode &slave = slaves.item(i);          
            slave.init();
        }
#endif
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
        rowif.set(_rowif);
        if (_auxrowif&&_auxrowif->queryRowMetaData())
            auxrowif.set(_auxrowif);
        else
            auxrowif.set(_rowif);
        synchronized proc(slavemutex);
        icompare = _icompare;
        keyserializer = _keyserializer;
        sorted = false;
        total = 0;
        stotal = 0;
        totalmem = 0;
        minrecsonnode = UINT_MAX;
        maxrecsonnode = 0;
        numnodes = slaves.ordinality();
        estrecsize = 100;
        if (!partitioninfo) { // if cosort use aux
            if (cosort) {
                ActPrintLog(activity, "Cosort with no prior partition");
                partitioninfo = new PartitionInfo(auxrowif);
            }
            else
                partitioninfo = new PartitionInfo(rowif);
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
        if (keyserializer&&cosort&&!partitioninfo->IsOK()) {
            keyserializer = NULL; // when joining to 0 rows can't use (LHS) serializer getMinMax will tell slave
            ActPrintLog(activity, "Suppressing key serializer on master");
        }
        assertex(icompare);
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
#ifdef CLOSE_IN_PARALLEL
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
#else
        ForEachItemInRev(i,slaves) {
            slaves.item(i).Close();
        }
#endif
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
        unsigned __int64 tot=0;
        unsigned i;
        size32_t ers = 0;
        unsigned ersn=0;
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            if (slave.numrecs==0)
                continue;
            VarElemArray minmax(rowif,keyserializer) ;
            void *p = NULL;
            size32_t retlen = 0;
            size32_t avrecsize=0;
            rowmap_t num=slave.GetMinMax(retlen,p,avrecsize);
            if (avrecsize) {
                ers += avrecsize;       // should probably do mode but this is OK
                ersn++;
            }
            tot += num;
            if (num>0) {
                minmax.deserialize(p,retlen,false);
                free(p);
                const void *p = minmax.item(0);
                if (!min.get()||(icompare->docompare(min,p)>0)) 
                    min.set(p);
                p = minmax.item(1);
                if (!max.get()||(icompare->docompare(max,p)<0)) 
                    max.set(p);
            }
        }
        if (ersn)
            estrecsize = ers/ersn;
        else
            estrecsize = 100;
#ifdef _TRACE
        if (min)
            traceKey(rowif->queryRowSerializer(),"Min",min);
        if (max)
            traceKey(rowif->queryRowSerializer(),"Max",max);
        if (min&&max) {
            int cmp=icompare->docompare(min,max);
            if (cmp==0) 
                ActPrintLog(activity, "Min == Max : All keys equal!");
            else if (cmp>0)
                ActPrintLog(activity, "ERROR: Min > Max!");
        }
        ActPrintLog(activity, "Tot = %"I64F"d", tot);
#endif
        return tot;
    }



    static CriticalSection ECFcrit;
    static VarElemArray *ECFarray;
    static ICompare *ECFcompare;
    static int elemCompareFunc(unsigned *p1, unsigned *p2)
    {
        return ECFarray->compare(ECFcompare,*p1,*p2);
    }


    rowmap_t *CalcPartitionUsingSampling()
    {   // doesn't support between
#define OVERSAMPLE 16
        OwnedMalloc<rowmap_t> splitMap(numnodes*numnodes, true);
        if (sizeof(rowmap_t)<=4) 
            assertex(total/numnodes<INT_MAX); // keep record numbers on individual nodes in 31 bits
        unsigned numsplits=numnodes-1;
        if (total==0) {
            // no partition info!
            partitioninfo->kill();
            return splitMap.getClear();
        }
        unsigned averagesamples = OVERSAMPLE*numnodes;  
        rowmap_t averagerecspernode = (rowmap_t)(total/numnodes);
        CriticalSection asect;
        VarElemArray sample(rowif,keyserializer);
#ifdef ASYNC_PARTIONING
        class casyncfor1: public CAsyncFor
        {
            NodeArray &slaves;
            VarElemArray &sample;
            CriticalSection &asect;
            unsigned averagesamples;
            rowmap_t averagerecspernode;
        public:
            casyncfor1(NodeArray &_slaves,VarElemArray &_sample,unsigned _averagesamples,rowmap_t _averagerecspernode,CriticalSection &_asect)
                : slaves(_slaves), sample(_sample), asect(_asect)
            { 
                averagesamples = _averagesamples;
                averagerecspernode = _averagerecspernode;
            }
            void Do(unsigned i)
            {
                CSortNode &slave = slaves.item(i);
                unsigned slavesamples = averagerecspernode?((unsigned)((averagerecspernode/2+averagesamples*(count_t)slave.numrecs)/averagerecspernode)):1;  
                //PrintLog("%d samples for %d",slavesamples,i);
                if (slavesamples) {
                    size32_t samplebufsize;
                    void *samplebuf=NULL;
                    slave.GetMultiNthRow(slavesamples,samplebufsize,samplebuf);
                    CriticalBlock block(asect);
                    sample.deserializeExpand(samplebuf,samplebufsize,true);
                    free(samplebuf);
                }
            }
        } afor1(slaves,sample,averagesamples,averagerecspernode,asect);
        afor1.For(numnodes, 20, true);
#else
        unsigned i;
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            unsigned slavesamples = (unsigned)((count_t)averagesamples*(count_t)slave.numrecs/(count_t)averagerecspernode);
            PrintLog("%d samples for %d",slavesamples,i);
            if (!slavesamples)
                continue;
            size32_t samplebufsize;
            void *samplebuf=NULL;
            slave.GetMultiNthRow(slavesamples,samplebufsize,samplebuf);
            sample.deserializeExpand(samplebuf,true);
            free(samplebuf);
        }   
#endif
#ifdef TRACE_PARTITION2
        {
            ActPrintLog(activity, "partition points");
            for (unsigned i=0;i<sample.ordinality();i++) {
                const byte *k = sample.item(i);
                StringBuffer str;
                str.appendf("%d: ",i);
                traceKey(rowif->queryRowSerializer(),str.str(),k);
            }
        }
#endif
        unsigned numsamples = sample.ordinality();
        size32_t ts=sample.totalSize();
        estrecsize = numsamples?(ts/numsamples):100;
        sample.sort(icompare);
        VarElemArray mid(rowif,keyserializer);
        if (numsamples) { // could shuffle up empty nodes here
            for (unsigned i=0;i<numsplits;i++) {
                unsigned pos = (unsigned)(((count_t)numsamples*(i+1))/((count_t)numsplits+1));
                const byte *r = sample.item(pos);
                mid.appendLink(r);
            }
        }
#ifdef TRACE_PARTITION2
        {
            ActPrintLog(activity, "merged partitions");
            for (unsigned i=0;i<mid.ordinality();i++) {
                const byte *k = mid.item(i);
                StringBuffer str;
                str.appendf("%d: ",i);
                traceKey(rowif->queryRowSerializer(),str.str(),k);
            }
        }
#endif
        // calculate split map
        size32_t mdl=0;
        MemoryBuffer mdmb;
        mid.serializeCompress(mdmb);
        mdl = mdmb.length();
        const byte *mdp=(const byte *)mdmb.bufferBase();
        unsigned i;
#ifdef ASYNC_PARTIONING
        i = 0;
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
#else
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            if (slave.numrecs!=0)
                slave.MultiBinChopStart(mdl,mdp,CMPFN_NORMAL);
        }
#endif
#ifdef ASYNC_PARTIONING
        class casyncfor3: public CAsyncFor
        {
            NodeArray &slaves;
            rowmap_t *splitmap;
            unsigned numnodes;
            unsigned numsplits;
        public:
            casyncfor3(NodeArray &_slaves,rowmap_t *_splitmap,unsigned _numnodes,unsigned _numsplits)
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
                    rowmap_t *res=splitmap+(i*numnodes);
                    slave.MultiBinChopStop(numsplits,res);
                    res[numnodes-1] = slave.numrecs;
                }
            }
        } afor3(slaves, splitMap, numnodes, numsplits);
        afor3.For(numnodes, 20, true);
#else
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            if (slave.numrecs!=0) {
                rowmap_t *res=splitMap+(i*numnodes);
                slave.MultiBinChopStop(numsplits,res);
                res[numnodes-1] = slave.numrecs;
            }
        }
#endif
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


    rowmap_t *CalcPartition(bool logging)
    {
        CriticalBlock block(ECFcrit);       
        // this is a bit long winded
        if (sizeof(rowmap_t)<=4) 
            assertex(stotal/numnodes<INT_MAX); // keep record numbers on individual nodes in 31 bits

        OwnedConstThorRow mink;
        OwnedConstThorRow maxk;
        // so as won't overflow
        OwnedMalloc<rowmap_t> splitmap(numnodes*numnodes, true);
        if (CalcMinMax(mink,maxk)==0) {
            // no partition info!
            partitioninfo->kill();
            return splitmap.getClear();
        }
        unsigned numsplits=numnodes-1;
        VarElemArray emin(rowif,keyserializer);
        VarElemArray emax(rowif,keyserializer);
        VarElemArray totmid(rowif,keyserializer);
        ECFarray = &totmid;
        ECFcompare = icompare;
        VarElemArray mid(rowif,keyserializer);
        unsigned i;
        unsigned j;
        for(i=0;i<numsplits;i++) {
            emin.appendLink(mink);
            emax.appendLink(maxk);
        }
        UnsignedArray amid;
        unsigned iter=0;
        try {
            MemoryBuffer mbmn;
            MemoryBuffer mbmx;
            MemoryBuffer mbmd;
            loop {
#ifdef _TRACE
                iter++;
                ActPrintLog(activity, "Split: %d",iter);
#endif
                emin.serializeCompress(mbmn.clear());       
                emax.serializeCompress(mbmx.clear());       
#ifdef ASYNC_PARTIONING
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
#else
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    if (slave.numrecs!=0)
                        slave.GetMultiMidPointStart(mbmn.length(),mbmn.bufferBase(),mbmx.length(),mbmx.bufferBase());               
                }
#endif
#ifdef ASYNC_PARTIONING
                Semaphore *nextsem = new Semaphore[numnodes];
                CriticalSection nextsect;

                totmid.clear();
                class casyncfor2: public CAsyncFor
                {
                    NodeArray &slaves;
                    VarElemArray &totmid;
                    Semaphore *nextsem;
                    unsigned numsplits;
                public:
                    casyncfor2(NodeArray &_slaves,VarElemArray &_totmid,unsigned _numsplits,Semaphore *_nextsem) 
                        : slaves(_slaves),totmid(_totmid)
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
                        unsigned base = totmid.ordinality();
                        if (p) {
                            totmid.deserializeExpand(p,retlen,true);
                            free(p);
                        }
                        while (totmid.ordinality()-base<numsplits)
                            totmid.appendNull();
                        nextsem[i].signal();
                    }
                } afor2(slaves,totmid,numsplits,nextsem);
                afor2.For(numnodes, 20);
                delete [] nextsem;
#else
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    unsigned base = totmid.ordinality();
                    if (slave.numrecs!=0) {
                        void *p = NULL;
                        size32_t retlen = 0;
                        slave.GetMultiMidPointStop(retlen,p);               
                        totmid.deserializeExpand(p,retlen,true);
                        free(p);
#ifdef _DEBUG
                        if (logging) {
                            MemoryBuffer buf;
                            for (j=0;j<numsplits;j++) {
                                ActPrintLog(activity, "Min(%d): ",j); traceKey(rowif->queryRowSerializer(),"    ",emin.item(j));
                                ActPrintLog(activity, "Mid(%d): ",j); traceKey(rowif->queryRowSerializer(),"    ",totmid.item(j+base));
                                ActPrintLog(activity, "Max(%d): ",j); traceKey(rowif->queryRowSerializer(),"    ",emax.item(j));
                            }
                        }
#endif
                    }
                    while (totmid.ordinality()-base<numsplits)
                        totmid.appendNull();
                }
#endif
                mid.clear();        
                mbmn.clear();
                mbmx.clear();
                for (i=0;i<numsplits;i++) {
                    amid.kill();
                    unsigned k;
                    unsigned t = i;
                    for (k=0;k<numsplits;k++) {
                        if (!totmid.isNull(t))
                            amid.append(t);
                        t += numsplits;
                    }
                    amid.sort(elemCompareFunc);
                    while (amid.ordinality()&&(emin.compare(icompare,i,totmid,amid.item(0))>=0))
                        amid.remove(0);
                    while (amid.ordinality()&&(emax.compare(icompare,i,totmid,amid.item(amid.ordinality()-1))<=0))
                        amid.remove(amid.ordinality()-1);
                    if (amid.ordinality()) {
                        unsigned mi = amid.item(amid.ordinality()/2);
#ifdef _DEBUG
                        if (logging) {
                            MemoryBuffer buf;
                            const void *b =totmid.item(mi);
                            ActPrintLog(activity, "%d: %d %d",i,mi,amid.ordinality()/2);
                            traceKey(rowif->queryRowSerializer(),"mid",b);
                        }
#endif
                        mid.appendLink(totmid,mi);
                    }
                    else
                        mid.appendLink(emin,i);
                }

                // calculate split map
                mid.serializeCompress(mbmd.clear());
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    if (slave.numrecs!=0)
                        slave.MultiBinChopStart(mbmd.length(),(const byte *)mbmd.bufferBase(),CMPFN_NORMAL);
                }
                mbmd.clear();
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    if (slave.numrecs!=0) {
                        rowmap_t *res=splitmap+(i*numnodes);
                        slave.MultiBinChopStop(numsplits,res);
                        res[numnodes-1] = slave.numrecs;
                    }
                }

                VarElemArray newmin(rowif,keyserializer);
                VarElemArray newmax(rowif,keyserializer);
                unsigned __int64 maxerror=0;
                unsigned __int64 nodewanted = (stotal/numnodes); // Note scaled total
                unsigned __int64 variancelimit = estrecsize?maxdeviance/estrecsize:0;
                if (variancelimit>nodewanted/50)
                    variancelimit=nodewanted/50; // 2%
                for (i=0;i<numsplits;i++) {
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
                        ActPrintLog(activity, "  wanted = %"CF"d, %stotal = %"CF"d, loc = %"CF"d, locwanted = %"CF"d\n",wanted,(total!=stotal)?"scaled ":"",tot,loc,nodewanted);
#endif
                    bool isdone=false;
                    unsigned __int64 error = (loc>nodewanted)?(loc-nodewanted):(nodewanted-loc);
                    if (error>maxerror)
                        maxerror = error;
                    if (wanted<tot) {
                        newmin.appendLink(emin,i);
                        newmax.appendLink(mid,i);
                    }
                    else if (wanted>tot) {
                        newmin.appendLink(mid,i);
                        newmax.appendLink(emax,i);
                    }
                    else {
                        newmin.appendLink(emin,i);
                        newmax.appendLink(emax,i);
                    }
                }
                if (emin.equal(icompare,newmin)&&emax.equal(icompare,newmax)) {
                    break; // reached steady state 
                }
                if ((maxerror*10000<nodewanted)||((iter>3)&&(maxerror<variancelimit))) { // within .01% or within variancelimit 
                    ActPrintLog(activity, "maxerror = %"CF"d, nodewanted = %"CF"d, variancelimit=%"CF"d, estrecsize=%u, maxdeviance=%u",
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
        if (logging) {
            for (i=0;i<numnodes;i++) {
                StringBuffer str;
                str.appendf("%d: ",i);
                for (j=0;j<numnodes;j++) {
                    str.appendf("%"RCPF"d, ",splitmap[j+i*numnodes]);
                }
                ActPrintLog(activity, "%s",str.str());
            }
        }
#endif
        return splitmap.getClear();
    }


    rowmap_t *UsePartitionInfo(PartitionInfo &pi, bool uppercmp)
    {
        unsigned i;
#ifdef _TRACE
#ifdef TRACE_PARTITION
        ActPrintLog(activity, "UsePartitionInfo %s",uppercmp?"upper":"");
        for (i=0;i<pi.splitkeys.ordinality();i++) {
            StringBuffer s;
            s.appendf("%d: ",i);
            traceKey(pi.prowif->queryRowSerializer(),s.str(),pi.splitkeys.item(i));
        }
#endif
#endif
        // first find split points
        unsigned numnodes = pi.numnodes;
        unsigned numsplits = numnodes-1;
        OwnedMalloc<rowmap_t> splitMap(numnodes*numnodes, true);
        OwnedMalloc<rowmap_t> res(numsplits);
        unsigned j;
        rowmap_t *mapp=splitMap;
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            if (numsplits>0) {
                MemoryBuffer mb;
                pi.splitkeys.serialize(mb);
                assertex(pi.splitkeys.ordinality()==numsplits);
                slave.MultiBinChop(mb.length(),(const byte *)mb.bufferBase(),numsplits,res,uppercmp?CMPFN_UPPER:CMPFN_COLLATE,true);
                rowmap_t *resp = res;
                rowmap_t p=*resp;
                *mapp = p;
                resp++;
                mapp++;
                for (j=1;j<numsplits;j++) {
                    rowmap_t n = *resp;
                    *mapp = n;
                    if (p>n) {
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
        rowmap_t *p = splitMap;
        for (i=0;i<numnodes;i++) {
            StringBuffer s;
            s.appendf("%d: ",i);
            for (j=0;j<numnodes;j++) {
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
        // I think this dependant on row being same format as meta

        unsigned numsplits=numnodes-1;
        VarElemArray splits(rowif,NULL);
        char *s=cosortfilenames;
        unsigned i;
        for(i=0;i<numnodes;i++) {
            char *e=strchr(s,'|');
            if (e) 
                *e = 0;
            else if (i!=numnodes-1)
                return;
            if (i) {
                CSortNode &slave = slaves.item(i);
                byte *rowmem;
                size32_t rowsize;
                if (!slave.FirstRowOfFile(s,rowsize,rowmem))
                    return;
                OwnedConstThorRow row;
                row.deserialize(auxrowif,rowsize,rowmem);
                splits.appendLink(row);
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
        VarElemArray splits(rowif,NULL);
        unsigned i;
        for(i=1;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            byte *rowmem;
            size32_t rowsize;
            if (!slave.FirstRowOfFile("",rowsize,rowmem))
                return;
            OwnedConstThorRow row;
            row.deserialize(auxrowif,rowsize,rowmem);
            if (row&&rowsize) {
                StringBuffer n;
                n.append(i).append(": ");
                traceKey(auxrowif->queryRowSerializer(),n,row);
            }
            splits.appendLink(row);
            free(rowmem);
        }
        partitioninfo->splitkeys.transfer(splits);
        partitioninfo->numnodes = numnodes;
    }

    IThorException *CheckSkewed(unsigned __int64 threshold, double skewWarning, double skewError, rowmap_t n, unsigned __int64 total, rowcount_t max)
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
        ActPrintLog(activity, "Skew check: Threshold %"I64F"d/%"I64F"d  Skew: %f/[warning=%f, error=%f]",
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
        if ((minisortthreshold>0)&&(ThorRowMemoryAvailable()/2<minisortthreshold))
            minisortthreshold = ThorRowMemoryAvailable()/2;
        if (skewError>0.0 && skewWarning > skewError)
        {
            ActPrintLog(activity, "WARNING: Skew warning %f > skew error %f", skewWarning, skewError);
            skewWarning = 0.0;
        }
        ActPrintLog(activity, "Sort: canoptimizenullcolumns=%s, usepartitionrow=%s, betweensort=%s skewWarning=%f skewError=%f minisortthreshold=%"I64F"d",canoptimizenullcolumns?"true":"false",usepartitionrow?"true":"false",betweensort?"true":"false",skewWarning,skewError,(__int64)minisortthreshold);
        assertex(partitioninfo);
        maxdeviance = _maxdeviance;
        unsigned i;
        bool overflowed = false;
        for (i=0;i<numnodes;i++) {
            CSortNode &slave = slaves.item(i);
            slave.GetGatherInfo(slave.numrecs,slave.memsize,slave.scale,keyserializer!=NULL);
            assertex(slave.scale);
            slave.overflow = slave.scale>1;
            if (slave.overflow)
                overflowed = true;
            total += slave.numrecs;
            stotal += slave.numrecs*slave.scale;
            totalmem += slave.memsize;
            if (slave.numrecs>maxrecsonnode)
                maxrecsonnode = slave.numrecs;
            if (slave.numrecs<minrecsonnode)
                minrecsonnode = slave.numrecs;
        }
        ActPrintLog(activity,"Total recs in mem = %"RCPF"d scaled recs= %"RCPF"d size = %"CF"d bytes, minrecsonnode = %"RCPF"d, maxrecsonnode = %"RCPF"d",total,stotal,totalmem,minrecsonnode,maxrecsonnode);
        unsigned numnodes = slaves.ordinality();
        if (!usepartitionrow&&!betweensort&&(totalmem<minisortthreshold)&&!overflowed) {
            sorted = MiniSort(total);
            return;
        }
#ifdef USE_SAMPLE_PARTITIONING
        bool usesampling = true;        
#endif
        loop {
            OwnedMalloc<rowmap_t> splitMap, splitMapUpper;
            CTimer timer;
            if (numnodes>1) {
                timer.start();
                if (cosortfilenames) {
                    CalcExtPartition();
                    canoptimizenullcolumns = false;
                }
                if (usepartitionrow) {
                    CalcPreviousPartition();
                    canoptimizenullcolumns = false;
                }
                if (partitioninfo->IsOK()) {
                    splitMap.setown(UsePartitionInfo(*partitioninfo, betweensort));
                    if (betweensort) {
                        splitMapUpper.setown(UsePartitionInfo(*partitioninfo, false));
                        canoptimizenullcolumns = false;
                    }
                }
                else {
                    // check for small sort here
                    if ((skewError<0.0)&&!betweensort) {
                        splitMap.setown(CalcPartitionUsingSampling());
                        skewError = -skewError;
#ifdef USE_SAMPLE_PARTITIONING
                        usesampling = false;
#endif
                    }
                    else {
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
                    if (!partitioninfo->splitkeys.checksorted(icompare)) {
                        ActPrintLog(activity, "ERROR: Split keys out of order!");
                        partitioninfo->splitkeys.sort(icompare);
                    }
                }
                timer.stop("Calculating split map");
            }
            OwnedMalloc<SocketEndpoint> endpoints(numnodes);
            SocketEndpoint *epp = endpoints;
            for (i=0;i<numnodes;i++) {
                CSortNode &slave = slaves.item(i);
                *epp = slave.endpoint;
                epp++;
            }
            if (numnodes>1) {
                // minimize logging
                unsigned numspilt = 0;
                UnsignedArray spilln;
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    if (slave.overflow) {
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
                        if (spilln.item(smi)>spiltmax) {
                            spiltmax = spilln.item(smi);
                            mostspilt = smi;
                        }
                    ActPrintLog(activity, "Gather - %d nodes spilt to disk, most %d times",numspilt,mostspilt);
                    for (i=0;i<numnodes;i++) {
                        CSortNode &slave = slaves.item(i);
                        if (slave.scale!=mostspilt+1) {
                            char url[100];
                            slave.endpoint.getUrlStr(url,sizeof(url));
                            ActPrintLog(activity, "Gather - node %s spilled %d times to disk",url,slave.scale-1);
                        }
                    }
                    MemoryBuffer mbsk;
                    partitioninfo->splitkeys.serialize(mbsk);
                    for (i=0;i<numnodes;i++) {
                        CSortNode &slave = slaves.item(i);
                        if (slave.overflow) 
                            slave.OverflowAdjustMapStart(numnodes,splitMap+i*numnodes,mbsk.length(),(const byte *)mbsk.bufferBase(),CMPFN_COLLATE);
                    }
                    for (i=0;i<numnodes;i++) {
                        CSortNode &slave = slaves.item(i);
                        if (slave.overflow) 
                            slave.AdjustNumRecs(slave.OverflowAdjustMapStop(numnodes,splitMap+i*numnodes));
                    }
                    if (splitMapUpper.get()) {
                        for (i=0;i<numnodes;i++) {
                            CSortNode &slave = slaves.item(i);
                            if (slave.overflow) 
                                slave.OverflowAdjustMapStart(numnodes,splitMapUpper+i*numnodes,mbsk.length(),(const byte *)mbsk.bufferBase(),CMPFN_UPPER);
                        }
                        for (i=0;i<numnodes;i++) {
                            CSortNode &slave = slaves.item(i);
                            if (slave.overflow) 
                                slave.OverflowAdjustMapStop(numnodes,splitMapUpper+i*numnodes);
                        }
                    }
                }

                OwnedMalloc<rowmap_t> tot(numnodes, true);
                rowcount_t max=0;
                unsigned imax=numnodes;
                for (i=0;i<imax;i++) {
                    unsigned j;
                    for (j=0;j<numnodes;j++) {
                        if (splitMapUpper)
                            tot[i]+=splitMapUpper[i+j*numnodes];
                        else
                            tot[i]+=splitMap[i+j*numnodes];
                        if (i)
                            tot[i]-=splitMap[i+j*numnodes-1];
                    }
                    if (tot[i]>max)
                        max = tot[i];
                    if (!betweensort&&canoptimizenullcolumns&&(tot[i]==0)) {
                        for (j=0;j<numnodes;j++) {
                            for (unsigned k=i+1;k<numnodes;k++) {
                                splitMap[k+j*numnodes-1] = splitMap[k+j*numnodes];
                            }
                        }
                        imax--;
                        i--;
                    }
                }
                for (i=0;i<numnodes;i++) {
                    CSortNode &slave = slaves.item(i);
                    char url[100];
                    slave.endpoint.getUrlStr(url,sizeof(url));
                    ActPrintLog(activity, "Split point %d: %"RCPF"d rows on %s", i, tot[i], url);
                }
                Owned<IThorException> e = CheckSkewed(threshold,skewWarning,skewError,numnodes,total,max);
                if (e)
                {
#ifdef _TRACE_SKEW_SPLIT
                    splitMap.clear();
                    splitMap.setown(CalcPartition(true));
#endif
#ifdef USE_SAMPLE_PARTITIONING
                    if (usesampling) {
                        ActPrintLog(activity, "Partioning using sampling failed, trying iterative partitioning"); 
                        usesampling = false;
                        continue;
                    }
#endif
                    throw e.getClear();
                }
                ActPrintLog(activity, "Starting Merge of %"RCPF"d records",total);
                for (i=0;i<numnodes;i++) {
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
            else {
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

VarElemArray *CSortMaster::ECFarray;
ICompare *CSortMaster::ECFcompare;
CriticalSection CSortMaster::ECFcrit; 


IThorSorterMaster *CreateThorSorterMaster(CActivityBase *activity)
{
    return new CSortMaster(activity);
}



void CSortNode::AdjustNumRecs(rowmap_t num)
{
    rowmap_t old = numrecs;
    numrecs = num;
    sorter.total += num-old;
    if (num>sorter.maxrecsonnode)
        sorter.maxrecsonnode = num;
}











        

