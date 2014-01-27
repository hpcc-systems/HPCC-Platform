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

#if 0
TODO add extra param for program url
remove SEND timings from grid
calculate top 10
#endif

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jsuperhash.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"

//#define TRACE
#define TRACECONN
#define TIMEOUT (1000*60*15)        

#define SERVER_PORT 7077

#define MAX_BLOCK_SIZE          0x40000     // 256K
#define AVG_BLOCK_SIZE          0x10000     // 64K

#define LOOPBACK_INTRO  100
#define LOOPBACK_ITER   100
#define LOOPBACK_OUTRO  100

#define BF_PROTOCOL BF_SYNC_TRANSFER_PUSH

#define NL "\r\n"

void usage()
{
    printf("floodtest usage:\n");
    printf("  floodtest <dali-ip> <group-name> <gb> [ <full url of floodtest.exe> ]\n");
    printf("or:\n");
    printf("  floodtest <dali-ip> <group-name> loopback [ <full url of floodtest.exe> ]\n");
    printf("e.g.\n");
    printf("  floodtest 10.150.12.34 thor_data400 1 \\\\10.150.12.12\\c$\\floodtest\\floodtest.exe\n");

}

const unsigned FLOODTEST_STARTED    = 0;
const unsigned FLOODTEST_ERROR      = 1;
const unsigned FLOODTEST_CONNECTED  = 2;
const unsigned FLOODTEST_RESULTS    = 3;

class CSendConnection;
class CRecvThread;

static byte RandomBuffer[MAX_BLOCK_SIZE+256];
static IRandomNumberGenerator *IRand;

struct nodeInfo: public CInterface
{
    unsigned size;
    __int64 *sendbytes;
    __int64 *recvbytes;
    unsigned *recvtime;
    CRecvThread **recvthreads;
    CSendConnection **sendconns;
    unsigned *recvseq;
    unsigned *sendseq;
    rank_t myrank;
    IQueueChannel *schannel;
    IGroup *grp;

    nodeInfo(IGroup *grp,rank_t _myrank,IQueueChannel *_schannel);
    ~nodeInfo();
    void serialize(MemoryBuffer &mb);
    void serializeResults(MemoryBuffer &mb);
    void serializeLoopbackResults(MemoryBuffer &mb);
    void deserialize(MemoryBuffer &mb);
    void sendError(const char *errstr1,const char *errstr2=NULL);
};

StringBuffer &getShortNodeName(IGroup* grp,unsigned i,StringBuffer &buf)
{
    // not that quick
    IpAddress ip = grp->queryNode(i).endpoint().ip;
    unsigned ipbaselen = 2;
    unsigned j=grp->ordinality();
    while (j--) {
        if (i!=j) {
            IpAddress ip2 = grp->queryNode(j).endpoint().ip;
            if (ip.ip[0]!=ip2.ip[0]) {
                ipbaselen = 0;
                break;
            }
            if ((ipbaselen==2)&&(ip.ip[1]!=ip2.ip[1])) 
                ipbaselen = 1;
        }
    }
    if (ipbaselen==0)
        buf.append((unsigned)ip.ip[0]).append('.');
    if (ipbaselen<=1)
        buf.append((unsigned)ip.ip[1]).append('.');
    buf.append((unsigned)ip.ip[2]).append('.').append((unsigned)ip.ip[3]);
    return buf;
}

static CriticalSection recvsect;

class CRecvThread: public Thread
{
    Owned<ISocket> sock;
    nodeInfo *ninfo;
    rank_t src;
public:
    CRecvThread(ISocket *_sock,nodeInfo *_ninfo,rank_t r)
        : sock(_sock)
    {
        src = r;
        ninfo = _ninfo;
        sock->set_block_mode(BF_PROTOCOL,0,TIMEOUT);
    }
    virtual int run()
    {
        unsigned start=0;
        try {
            MemoryBuffer mb;
            bool first = true;
            loop
            {
                size32_t sz = sock->receive_block_size();
                if (sz==0)
                    break;
                {
                    //CriticalBlock block(recvsect);        // TEST
                    unsigned start=msTick();
                    if (!sock->receive_block(mb.clear().reserve(sz),sz)) {
                        StringBuffer err("Timeout receiving from ");
                        err.append(src);
                        ninfo->sendError(err.str());
                        break;
                    }
                }
#ifdef TRACE
                DBGLOG("received %d from %d",sz,src+1);
#endif
                unsigned seq;
                mb.read(seq);
                if (seq!=ninfo->recvseq[src]++) {
                    StringBuffer msg;
                    msg.append("Block out of sequence from ").append(src+1).append(", got ").append(seq).append(" expected ").append(ninfo->recvseq[src]-1);
                    ninfo->sendError(msg.str());
                    break; // errors fatal otherwise overload q
                }
                ninfo->recvtime[src] += (msTick()-start);
                ninfo->recvbytes[src] += sz;
            }
        }
        catch (IException *e) {
            StringBuffer err;
            e->errorMessage(err);
            ninfo->sendError("Receive exception",err.str());
        }
#ifdef TRACE
        DBGLOG("receive exit from %d",src+1);
#endif
        return 1;
    };
};



class CAcceptThread: public Thread
{
    nodeInfo *ninfo;
public:
    CAcceptThread(nodeInfo *_ninfo)
    {
        ninfo = _ninfo;
    }
    virtual int run()
    {
        Owned<ISocket> servsock;
        try {
            servsock.setown(ISocket::create(SERVER_PORT,20));
            unsigned i;
            for (i=1;i<ninfo->size;i++) {               // don't include self
                ISocket *sock = servsock->accept();
                rank_t rank;
                size32_t sizeread;
                sock->readtms(&rank,sizeof(rank),sizeof(rank),sizeread,TIMEOUT);
#ifdef TRACECONN
                DBGLOG("accepted from %d",rank+1);
#endif
                assertex(rank<ninfo->size);
                assertex(!ninfo->recvthreads[rank]);
                ninfo->recvthreads[rank] = new CRecvThread(sock,ninfo,rank);
                ninfo->recvthreads[rank]->start();
            }

        }
        catch (IException *e) {
            StringBuffer err;
            e->errorMessage(err);
            ninfo->sendError("Accept exception",err.str());
        }
        return 1;
    };
};

class CSendConnection: public CInterface
{
    Owned<ISocket> sock;
    nodeInfo *ninfo;
    rank_t dest;
    __int64 todo;
public:
    CSendConnection(IGroup *group,rank_t r,nodeInfo *_ninfo,unsigned ngb)
    {
        dest = r;
        ninfo = _ninfo;
        SocketEndpoint ep = group->queryNode(r).endpoint();
        ep.port = SERVER_PORT;
        sock.setown(ISocket::connect(ep));
        sock->write(&ninfo->myrank,sizeof(ninfo->myrank));
        sock->set_block_mode(BF_PROTOCOL,0,TIMEOUT);
        todo = ((__int64)0x40000000)*ngb/(ninfo->size-1);
    }
    bool send()
    {
        bool supresserr = false;
        try {
            size32_t sz = (IRand->next()%(AVG_BLOCK_SIZE/2))+AVG_BLOCK_SIZE-(AVG_BLOCK_SIZE/4);
            __int64 done = ninfo->sendbytes[dest] + sz;
            if (done<=todo) {   // don't send to self
                MemoryBuffer mb;
                unsigned seq = ninfo->sendseq[dest]++;
                mb.append(seq);
                mb.append(sz-sizeof(seq),RandomBuffer+(seq%256));
                if (!sock->send_block(mb.readDirect(sz),sz)) {
                    StringBuffer err("Timeout sending to ");
                    err.append(dest);
                    ninfo->sendError(err.str());
                    return false;
                }   
                ninfo->sendbytes[dest] = done;
#ifdef TRACE
                DBGLOG("sent %d to %d  done = %"I64F"d",sz,dest+1,done);
#endif
                return true;
            }
        }
        catch (IException *e) {
            StringBuffer err;
            e->errorMessage(err);
            ninfo->sendError("Send exception",err.str());
            supresserr = true;
        }
        try {
            stop();
        }
        catch (IException *e) {
            if (!supresserr) {
                StringBuffer err;
                e->errorMessage(err);
                ninfo->sendError("Send stop exception",err.str());
            }
        }
        return false;
    }
    void stop()
    {
#ifdef TRACE
        DBGLOG("send done to %d",dest+1);
#endif
        if (sock)
            sock->send_block(NULL,0);
        sock.clear();
    }
};

nodeInfo::nodeInfo(IGroup *_grp,rank_t _myrank,IQueueChannel *_schannel)
{
    grp = _grp;
    size = grp->ordinality();
    assertex(size>1);
    StringBuffer base;
    myrank = _myrank;
    schannel = _schannel;
    sendbytes = (__int64 *)calloc(size,sizeof(__int64));
    recvbytes = (__int64 *)calloc(size,sizeof(__int64));
    recvtime = (unsigned *)calloc(size,sizeof(unsigned));
    sendseq = (unsigned *)calloc(size,sizeof(unsigned));
    recvseq = (unsigned *)calloc(size,sizeof(unsigned));
    recvthreads = (CRecvThread **)calloc(size,sizeof(CRecvThread *));
    sendconns = (CSendConnection **)calloc(size,sizeof(CSendConnection *));
}
nodeInfo::~nodeInfo()
{
    free(sendbytes);
    free(recvbytes);
    free(recvtime);
    free(sendseq);
    free(recvseq);
    unsigned i;
    for (i=0;i<size;i++) {
        if (recvthreads[i])
            recvthreads[i]->Release();
        if (sendconns[i])
            sendconns[i]->Release();
    }
    free(recvthreads);
    free(sendconns);
}
void nodeInfo::serialize(MemoryBuffer &mb)
{
    mb.append(size);
    mb.append(size*sizeof(__int64),recvbytes);
    mb.append(size*sizeof(unsigned),recvtime);
}
void nodeInfo::deserialize(MemoryBuffer &mb)
{
    size32_t _size;
    mb.read(_size);
    assertex(size==_size);
    mb.read(size*sizeof(__int64),recvbytes);
    mb.read(size*sizeof(unsigned),recvtime);
}

void nodeInfo::serializeResults(MemoryBuffer &mb)
{
    mb.append(size);
    unsigned bps;
    unsigned i;
    __int64 total=0;
    for (i=0;i<size;i++) {
        bps = recvtime[i]?((recvbytes[i]*1000)/recvtime[i]):0;
        mb.append(bps);
        total += recvbytes[i];
    }
    mb.append(total);
}





void nodeInfo::sendError(const char *errstr1,const char *errstr2)
{
    StringBuffer err(errstr1);
    if (errstr2)
        err.append(" ").append(errstr2);
    ERRLOG("%s",err.str());
    MemoryBuffer mb;
    mb.append(FLOODTEST_ERROR).append(myrank).append(err.str());
    schannel->put(mb);
}

static void deserializeResults(MemoryBuffer &mb,unsigned size,unsigned *recvres, __int64 &total)
{
    size32_t _size;
    mb.read(_size);
    assertex(size==_size);
    for (unsigned i=0;i<size;i++) 
        mb.read(*(recvres++));
    mb.read(total);
}


static void deserializeLoopbackResults(MemoryBuffer &mb,unsigned *res,unsigned *totalres)
{
    for (unsigned i=0;i<3;i++) 
        mb.read(*(res++));
    if (LOOPBACK_ITER<=100)
        for (unsigned j=0;j<LOOPBACK_ITER;j++) 
            mb.read(*(totalres++));

}



void floodtestClient(const char *grpname,unsigned ngb)
{
    bool loopback = (ngb==0);
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0);
    Owned<IQueueChannel> cchannel = conn->open("floodqclient");
    rank_t myrank = RANK_NULL;
    Owned<IGroup> group = queryNamedGroupStore().lookup(grpname);
    bool ok = false;
    unsigned i;
    MemoryBuffer mb;
    if (group) {
        myrank = group->rank();
        if (myrank!=RANK_NULL) {
            ok = true;
            mb.append(FLOODTEST_STARTED).append(myrank);
            queryMyNode()->serialize(mb);
            PROGLOG("Started");
        }
        else {
            ERRLOG("Failed to find node in group");
            mb.append(FLOODTEST_ERROR).append(myrank).append("Failed to find node in group");
        }
    }
    else {
        ERRLOG("Failed to find node in group");
        mb.append(FLOODTEST_ERROR).append(myrank).append("Failed to find group");
    }
    IRand->seed(myrank);
    Owned<IQueueChannel> schannel = conn->open("floodqserver");
    schannel->put(mb);
    mb.clear();
    if (!ok)
        return;
    unsigned n = group->ordinality();
    Owned<nodeInfo> ninfo = new nodeInfo(group,myrank,schannel);
    if (loopback) {

        mb.append(FLOODTEST_CONNECTED).append(myrank);
    }
    else {
        Owned<CAcceptThread> acceptthread=new CAcceptThread(ninfo);
        acceptthread->start();
        for (i=0;i<n;i++) {
            if (i!=myrank) {
    #ifdef TRACECONN
                DBGLOG("connecting to %d",i);
    #endif
                ninfo->sendconns[i] = new CSendConnection(group,i,ninfo,ngb);
            }
        }
        if (acceptthread->join(TIMEOUT)) {
            mb.append(FLOODTEST_CONNECTED).append(myrank);
            PROGLOG("Connected");
        }
        else {
            ok = false;
            mb.append(FLOODTEST_ERROR).append(myrank).append("Failed to connect to all nodes");
            PROGLOG("Failed to connect to all nodes");
        }
    }
    schannel->put(mb);
    if (!ok)
        return;
    mb.clear();
    if (!cchannel->get(mb,0,TIMEOUT)) {
        DBGLOG("Failed to get initial Q item");
        return;                                 // auto close if not started in 15 mins
    }
    mb.read(ok);
    if (!ok) {
        PROGLOG("Aborted");
        return;
    }
    Owned<INode> master = deserializeINode(mb);
    if (loopback) {
        unsigned minloopback = (unsigned)-1;
        unsigned maxloopback = 0;
        __int64 totalloopback = 0;
        unsigned nloopbacks = 0;
        unsigned *res=(unsigned *)malloc(sizeof(unsigned)*LOOPBACK_ITER);
        INode *g[1];
        g[0] = master.get();
        Owned<IGroup> mgrp = createIGroup(1,g);
        Owned<ICommunicator> comm = createCommunicator(mgrp,false);
        for (i=0;i<LOOPBACK_INTRO+LOOPBACK_ITER+LOOPBACK_OUTRO;i++) {
            char buf[32];
            CMessageBuffer mb;
            mb.append(sizeof(buf),buf);
            unsigned start = usTick();
            if (!comm->sendRecv(mb,0,MPTAG_TEST,1000*60*15)) {
                DBGLOG("loop back timed out");
                break;
            }
            if ((i>=LOOPBACK_INTRO)&&(i<LOOPBACK_INTRO+LOOPBACK_ITER)) {
                unsigned t = usTick()-start;
                res[i-LOOPBACK_INTRO] = t;
                if (t>30*1000*1000)
                    PROGLOG("1 took %dms",t/1000);
                if (t<minloopback)
                    minloopback = t;
                if (t>maxloopback)
                    maxloopback = t;
                totalloopback += t;
                nloopbacks++;
            }
        }
        DBGLOG("creating results");
        mb.clear().append(FLOODTEST_RESULTS).append(myrank);
        mb.append(minloopback);
        mb.append((unsigned)(nloopbacks?(totalloopback/nloopbacks):0));
        mb.append(maxloopback);
        if (LOOPBACK_ITER<=100)
            mb.append(sizeof(unsigned)*LOOPBACK_ITER,res);
        free(res);
    }
    else {
        bool *done = (bool *)calloc(n,sizeof(bool));
        done[myrank] = true;
        unsigned ndone;
        unsigned *shuffler = (unsigned *)malloc(n*sizeof(unsigned));
        for (i=0;i<n;i++)
            shuffler[i] = i;
        i = 0;
        for (ndone=1;ndone<n;) {
            unsigned next = shuffler[i++];
            if (!done[next]) {
                if (!ninfo->sendconns[next]->send()) {
                    done[next] = true;
                    ndone++;
                }
            }
            if (i>=n) {
                unsigned j=n;
                while (j>1) {
                    i = IRand->next()%j;  // NB n is correct here
                    j--;
                    unsigned t = shuffler[i];
                    shuffler[i] = shuffler[j];
                    shuffler[j] = t;
                }
                i = 0;
            }
        }
        DBGLOG("joining receivers");
        for (i=0;i<n;i++) {
            if (i!=myrank) {
                if (ninfo->recvthreads[i]) {
                    if(!ninfo->recvthreads[i]->join()) // total job time
                        return;
                }
            }
        }
        DBGLOG("creating results");
        mb.clear().append(FLOODTEST_RESULTS).append(myrank);
        ninfo->serializeResults(mb);
    }
    DBGLOG("sending results");
    schannel->put(mb);
    
}

bool setState(IGroup *grp,const char *str,unsigned i,bool *b)
{
    bool ret = false;
    StringBuffer msg(str);
    msg.append(' ');
    getShortNodeName(grp,i,msg);
    if (i>=grp->ordinality()) 
        msg.append(": out of range - ERROR");
    else if (b[i])
        msg.append(": already done - ERROR");
    else {
        b[i] = true;
        unsigned remaining=0;
        unsigned j;
        for (j=0;j<grp->ordinality();j++) {
            if (!b[j]) {
                remaining++;
            }
        }
        if (remaining==0) {
            msg.append(" all done ");
            ret = true;
        }
        else { 
            msg.appendf(" remaining = %d",remaining);
            if (remaining<=5) {
                msg.append(" (");
                bool first=true;
                for (j=0;j<grp->ordinality();j++) {
                    if (!b[j]) {
                        if (first)
                            first = false;
                        else
                            msg.append(' ');
                        getShortNodeName(grp,j,msg);
                    }
                }
                msg.append(")");
            }
        }
    }
    PROGLOG("%s",msg.str());
    return ret;
}

bool runSlaves(IGroup *grp,const char *progname,const char *daliserver,const char *grpname,unsigned ngb,const char *fullexe)
{
    RemoteFilename rfn;
    StringBuffer remoteexe;
    SocketEndpoint ep;
    if (!fullexe) {
        ep.setLocalHost(0);
        char exename[256];
#ifdef _WIN32
        if (!strchr(progname,'\\')) {
            _getcwd(exename, 255);
            strcat(exename,"\\");
        }
        else
            exename[0] = 0;
        strcat(exename,progname);
#else
        if (!strchr(progname,'/')) {
            getcwd(exename, 255);
            strcat(exename,"/");
        }
        else
            exename[0] = 0;
        strcat(exename,progname);
#endif
        rfn.setPath(ep,exename);
        rfn.getRemotePath(remoteexe);
    }
    else {
//      rfn.setRemotePath(fullexe);
//      ep.ip = rfn.queryIP();
//      ep.port = 0;
        remoteexe.append(fullexe);
    }
    StringBuffer params;
    params.append(daliserver).append(' ').append(grpname).append(' ').append(ngb).append(" client");
    PROGLOG("Command line: %s %s",remoteexe.str(),params.str());
    class casyncfor: public CAsyncFor
    {
        StringAttr remoteexe;
        StringAttr params;
    public:
        IGroup *grp;
        bool error;
        CriticalSection statesect;
        casyncfor(IGroup *_grp,const char *_remoteexe,const char *_params)
            : remoteexe(_remoteexe), params(_params)
        {
            grp = _grp;
            error = false;
        }
        void Do(unsigned idx)
        {
            try {
                IpAddress ip=grp->queryNode(idx).endpoint().ip;
                if (runRemoteProgram(remoteexe, params, ip)) {
                    CriticalBlock block(statesect);
                }
                else {
                    StringBuffer ips;
                    ip.getText(ips);
                    ERRLOG("Failed to run slave on %d (%s)",idx+1,ips.str());
                    error = true;
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, "connect(array)");
                error = true;
            }
        }
        bool failed() { return error; }
    } afor(grp,remoteexe.str(),params.str());
    afor.For(grp->ordinality(), 10);
    return !afor.failed();
}

unsigned *cmp_srt;
unsigned *cmp_srtp;
int srtcmp(const void *_v1, const void *_v2)
{
    const unsigned &v1 = *(const unsigned *) _v1;
    const unsigned &v2 = *(const unsigned *) _v2;
    if (cmp_srt[v1]<cmp_srt[v2])
        return -1;
    if (cmp_srt[v1]>cmp_srt[v2])
        return 1;
    return 0;
}

static void addTopTen(StringBuffer &out,IGroup *grp,unsigned *srt,const char *titletop,const char *titlebottom)
{
    unsigned n =grp->ordinality();
    cmp_srtp = new unsigned[n];
    unsigned i;
    for(i=0;i<n;i++)
        cmp_srtp[i] = i;
    cmp_srt = srt;
    qsort(cmp_srtp,n,sizeof(unsigned),srtcmp);
    unsigned isbottom;
    for (isbottom=0;isbottom<2;isbottom++) {
        out.append(isbottom?titlebottom:titletop).append(NL);
        i = strlen(isbottom?titlebottom:titletop);
        while (i--)
            out.append('-');
        out.append(NL);
        unsigned num=(n<10)?n:10;
        unsigned j=isbottom?num:n; 
        for (i=0;i<num;i++) {
            j--;
            size32_t start=out.length();
            getShortNodeName(grp,cmp_srtp[j],out);
            do {
                out.append(' ');
            } while (out.length()<start+20);
            double v=((double)(srt[cmp_srtp[j]]))/(1024*1024);
            out.appendf("%.2fMB/s"NL,v);  
        }
        out.append(NL);
    }
    delete [] cmp_srtp;
}

#define LOOPBACK_TIMEOUT 120000 

class CLoopbackServer: public Thread
{
    bool stopping;
    Owned<ICommunicator> comm;
    Semaphore sem;
    IGroup *grp;
    __int64 &total;

public:
    CLoopbackServer(SocketEndpointArray &epa,__int64 &_total)
        : Thread("CLoopbackServer"), total(_total)
    {
        Owned<IGroup> grp = createIGroup(epa);
        stopping = false;
        comm.setown(createCommunicator(grp,false));
        DBGLOG("verifying connections");
        comm->verifyAll();
        DBGLOG("verifying connections done");
    }
    int run()
    {
        CMessageHandler<CLoopbackServer> handler("CLoopbackServer",this,&CLoopbackServer::processMessage, NULL, 500, LOOPBACK_TIMEOUT);
        sem.signal();
        while (!stopping) {
//          unsigned waiting = comm->probe(RANK_ALL,MPTAG_TEST,NULL);
//          if ((waiting!=0)&&(waiting%10==0))
//              DBGLOG("QPROBE: MPTAG_TEST has %d waiting",waiting);
            unsigned start=usTick();
            CMessageBuffer mb;
            if (comm->recv(mb, RANK_ALL, MPTAG_TEST, NULL)) {
                total++;
                handler.handleMessage(mb);
            }
            else
                stopping = true;
        }
        return 0;
    }

    void stop()
    {
        if (!stopping) {
            stopping = true;
            queryDefaultDali()->queryCoven().queryComm().cancel(RANK_ALL, MPTAG_TEST);
        }
        join();
    }

    void processMessage(CMessageBuffer &mb)
    {
        sem.wait();
        char retbuf[1024];
        mb.clear().append(sizeof(retbuf),retbuf);
        comm->reply(mb);
        sem.signal();
    }
            

};


void floodtestServer(const char *exename,const char *daliserver,const char *grpname,unsigned ngb,const char *fullexe)
{
    bool loopback = (ngb==0);
    Owned<IGroup> group = queryNamedGroupStore().lookup(grpname);
    if (!group) {
        ERRLOG("Cannot find group %s",grpname);
        return;
    }
    Owned<CLoopbackServer> lbserver;
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0);
    Owned<IQueueChannel> cchannel = conn->open("floodqclient");
    Owned<IQueueChannel> schannel = conn->open("floodqserver");
    MemoryBuffer mb;
    while (cchannel->probe()) {
        PROGLOG("Deleting floodqclient item from previous run"); 
        cchannel->get(mb.clear(),0,60*1000);
    }
    while (schannel->probe()) {
        PROGLOG("Deleting floodqserver item from previous run"); 
        schannel->get(mb.clear(),0,60*1000);
    }
    if (!runSlaves(group,exename,daliserver,grpname,ngb,fullexe))
        return;
    unsigned n = group->ordinality();
    unsigned i;
    bool *connected = (bool *)calloc(n,(sizeof(bool)));
    bool *finished = (bool *)calloc(n,(sizeof(bool)));
    bool *started = (bool *)calloc(n,(sizeof(bool)));
    unsigned *recvres = (unsigned *)calloc(n*n,sizeof(unsigned));
    unsigned *loopbackres = (unsigned *)calloc(n*3,sizeof(unsigned));
    unsigned *totalres = (unsigned *)malloc(n*sizeof(unsigned)*LOOPBACK_ITER);
    unsigned numerrs = 0;
    unsigned starttime;
    __int64 total=0;
    SocketEndpointArray epa;
    loop {
        schannel->get(mb.clear());
        unsigned fn;
        mb.read(fn);
        rank_t r;
        mb.read(r);
        if (fn==FLOODTEST_STARTED) {
            Owned<INode> slave = deserializeINode(mb);
            while (epa.ordinality()<=r) {
                SocketEndpoint null;
                epa.append(null);
            }
            epa.item(r) = slave->endpoint();
            if (setState(group,"Started  ",r,started)) {
                starttime = msTick();
            }
        }
        else if (fn==FLOODTEST_ERROR) {
            numerrs++;
            StringAttr msg;
            mb.read(msg);
            StringBuffer url;
            ERRLOG("%3d (%s): %s\n",r+1,group->queryNode(r).endpoint().getUrlStr(url).str(),msg.get());
        }
        else if (fn==FLOODTEST_CONNECTED) {
            if (setState(group,"Connected",r,connected)) {
                // All connected so start
                if (loopback) {
                    lbserver.setown(new CLoopbackServer(epa,total));
                    lbserver->start();
                }
                for (i=0;i<n;i++) {
                    bool b = true;
                    mb.clear().append(b);
                    queryMyNode()->serialize(mb);
                    cchannel->put(mb);
                }

            }
        }
        else if (fn==FLOODTEST_RESULTS) {
            __int64 t;
            if (r<n) {
                if (loopback) 
                    deserializeLoopbackResults(mb,loopbackres+(r*3),totalres+(r*LOOPBACK_ITER));
                else {
                    deserializeResults(mb,n,recvres+(n*r),t);
                    total += t;
                }
            }
            if (setState(group,"Finished ",r,finished)) {
                if (lbserver)
                    lbserver->stop();
                break;
            }
        }
    }
    unsigned endtime=msTick();
    StringBuffer out;
    out.appendf("Floodtest: %d error%s reported"NL,numerrs,(numerrs==1)?"":"s");
    unsigned *r;
    if (loopback) {
        r = loopbackres;
        for (i=0;i<n;i++) {
            getShortNodeName(group,i,out);
            for (unsigned j=0;j<3;j++) 
                out.appendf(",%d",*(r++));  
            out.append(NL);
        }
        out.append(NL);
        if (LOOPBACK_ITER<=100) {
            out.append(NL);
            r = totalres;
            for (i=0;i<n;i++) {
                getShortNodeName(group,i,out);
                for (unsigned j=0;j<LOOPBACK_ITER;j++) 
                    out.appendf(",%d",*(r++));  
                out.append(NL);
            }
            out.append(NL);
        }
        Owned<IFile> resf=createIFile("floodlbres.txt");
        Owned<IFileIO> res = resf->open(IFOcreate);
        res->write(0,out.length(),out.str());
        PROGLOG("Results written to floodlbres.txt");
        PROGLOG("Floodtest loopback %"I64F"d messages received",total);
        PROGLOG("Floodtest loopback finished, %d error%s reported",numerrs,(numerrs==1)?"":"s");
    }
    else {
        out.appendf("Total job per-node rate: %.2fMB/s"NL NL,(((double)total/n)*1000.0/(endtime-starttime))/(1024*1024));
        unsigned j;
        unsigned *srt= (unsigned *)calloc(n,sizeof(unsigned));
        r = recvres;
        for (i=0;i<n;i++) {
            for (j=0;j<n;j++) 
                srt[i]+=*(r++);
            srt[i]/=n;
        }
        addTopTen(out,group,srt,"Fastest Receive","Slowest Receive");
        memset(srt,0,sizeof(unsigned)*n);
        r = recvres;
        for (i=0;i<n;i++) {
            for (j=0;j<n;j++) 
                srt[j]+=*(r++);
        }
        for (i=0;i<n;i++) 
            srt[i]/=(n-1);
        addTopTen(out,group,srt,"Fastest Send","Slowest Send");
        out.append(NL"-");
        for (i=0;i<n;i++) {
            out.append(',');
            getShortNodeName(group,i,out);
        }
        out.append(NL);
        r = recvres;
        for (i=0;i<n;i++) {
            getShortNodeName(group,i,out);
            for (j=0;j<n;j++) {
                out.append(',');
                if (*r) 
                    out.appendf("%.2f",((double)(*r))/(1024*1024));  
                else
                    out.append('-');
                r++;
            }
            out.append(NL);
        }
        out.append(NL);
        Owned<IFile> resf=createIFile("floodres.txt");
        Owned<IFileIO> res = resf->open(IFOcreate);
        res->write(0,out.length(),out.str());
        PROGLOG("Results written to floodres.txt");
        PROGLOG("Floodtest finished, %d error%s reported",numerrs,(numerrs==1)?"":"s");
        PROGLOG("Average node rate %.2fMB/s",(((double)total/n)*1000.0/(endtime-starttime))/(1024*1024));
    }
    free(loopbackres);
    free(started);
    free(connected);
    free(finished);
    free(recvres);
}


    


struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };
int main(int argc, char* argv[])
{   
    ReleaseAtomBlock rABlock;
    InitModuleObjects();
    if (argc<4) {
        usage();
        return 0;
    }

    EnableSEHtoExceptionMapping();
    bool isserver = (argc!=5)||(stricmp(argv[4],"client")!=0);
    attachStandardFileLogMsgMonitor(isserver?"floodtest.log":
#ifdef _WIN32
    "c:\\floodtest.log", 
#else
    "/c$/floodtest.log", 
#endif
    NULL, MSGFIELD_STANDARD, MSGAUD_all, MSGCLS_all, TopDetail, false);
    queryStderrLogMsgHandler()->setMessageFields(0);
    try {
        IRand = createRandomNumberGenerator();
        IRand->seed(1234);
        unsigned i;
        for (i=0;i<MAX_BLOCK_SIZE+256;i++)
            RandomBuffer[i] = (byte)(IRand->next()%255);    // reserve 0xFF
        SocketEndpoint ep;
        SocketEndpointArray epa;
        ep.set(argv[1],DALI_SERVER_PORT);
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 
        initClientProcess(group);
        if (isserver) {
            PROGLOG("FLOODTEST server Starting");

            floodtestServer(argv[0],argv[1],argv[2],(stricmp(argv[3],"loopback")==0)?0:atoi(argv[3]),(argc>4)?argv[4]:NULL);
        }
        else {
            floodtestClient(argv[2],atoi(argv[3]));
        }
        closedownClientProcess();
        IRand->Release();
    }
    catch (IException *e) {
        EXCLOG(e, "Floodtest");
        e->Release();
    }
    return 0;
}

