

// CSocketSelectThread error 10038  

#include <platform.h>
#include <jlib.hpp>
#include <jthread.hpp>
#include <jmisc.hpp>
#include <jcrc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include <mpicomm.hpp>
#include <string>

#include "mplog.hpp"

using namespace std;

#define MPPORT 8888

//#define MULTITEST
//#define STREAMTEST
//#define MPRING
//#define MPALLTOALL
//#define MPTEST2
//#define GPF
#define DYNAMIC_TEST

#define TEST_AlltoAll "AlltoAll"
#define TEST_STREAM "Stream"
#define TEST_MULTI "Multi"
#define TEST_RING "Ring"
#define TEST_RANK "PrintRank"
#define TEST_SELFSEND "SelfSend"
#define TEST_SINGLE_SEND "SingleSend"
#define TEST_RIGHT_SHIFT "RightShift"
#define TEST_RECV_FROM_ANY "RecvFromAny"
#define TEST_SEND_TO_ALL "SendToAll"
#define TEST_MULTI_MT "MTMultiSendRecv"

#ifdef MULTITEST
//#define MYMACHINES "10.150.10.16,10.150.10.17,10.150.10.18,10.150.10.19,10.150.10.20,10.150.10.21,10.150.10.22,10.150.10.23,10.150.10.47,10.150.10.48,10.150.10.49,10.150.10.50,10.150.10.51,10.150.10.52,10.150.10.53,10.150.10.54,10.150.10.55,10.150.10.73,10.150.10.75,10.150.10.79"
//#define MYMACHINES "192.168.16.124,10.150.10.17,10.150.10.18,10.150.10.19,10.150.10.20,10.150.10.21,10.150.10.22,10.150.10.23,10.150.10.47,10.150.10.48,10.150.10.49,10.150.10.50,10.150.10.51,10.150.10.52,10.150.10.53,10.150.10.54,10.150.10.55,10.150.10.73,10.150.10.75,10.150.10.79"
#endif

// #define aWhile 100000
#define aWhile 10



class CSectionTimer
{
    HiresTimer hrt[1000];
    unsigned tids[1000];

    const char *name;
    static CriticalSection findsect;
    double total;
    double max;
    unsigned count;

    unsigned idx()
    {
        CriticalBlock block(findsect);
        unsigned tid = (unsigned)(memsize_t)GetCurrentThreadId();
        unsigned i;
        for (i=0;i<999;i++) {
            if (tids[i]==tid)
                break;
            if (tids[i]==0) {
                tids[i] = tid;
                break;
            }
        }
        return i;
    }
public:
    CSectionTimer(const char *_name)
    {
        name = (const char *)strdup(_name);
        total = 0;
        max = 0;
        memset(tids,0,sizeof(tids));
        count = 0;
    }
    ~CSectionTimer()
    {
        free((void *)name);
    }


    void begin()
    {
        hrt[idx()].reset();
    }

    void end()
    {
        double v = hrt[idx()].get();
        total += v;
        if (max<v)
            max = v;
        count++;
    }
    void print()
    {
        if (count)
            PrintLog("TIME: %s(%u): max=%.6f, avg=%.6f, tot=%.6f",name,count,max,(double)total/count,total);
    }
};

CriticalSection CSectionTimer::findsect;

class TimedBlock
{
    CSectionTimer &stim;
public:
    TimedBlock(CSectionTimer &_stim) : stim(_stim)      { stim.begin(); }
    ~TimedBlock()                       { stim.end(); }
};


class TimedCriticalBlock
{
    CriticalSection &crit;
public:
    TimedCriticalBlock(CriticalSection &c,CSectionTimer &stim) 
        : crit(c)       
    { 
        TimedBlock block(stim);  crit.enter();
    }
    ~TimedCriticalBlock()                       { crit.leave(); }
};

static CSectionTimer STsend("send");
static CSectionTimer STrecv("recv");

//#define NITER 100
#define NITER 40
#define BLOCKSIZE (0x100000*10)
//#define BLOCKSIZE (0x1000*10)

#define WRITEDELAY 100
#define READDELAY   5000

void StreamTest(IGroup *group,ICommunicator *comm)
{

    CMessageBuffer mb;
    for (unsigned i=0;i<NITER;i++) {
        if (group->rank() == 1) {
            mb.clear();
            StringBuffer header;
            header.append("Test Block #").append(i);
            mb.append(header.str()).reserve(BLOCKSIZE-mb.length());
            PrintLog("MPTEST: StreamTest sending '%s' length %u",header.str(),mb.length());
            {
                TimedBlock block(STsend);
                comm->send(mb,0,MPTAG_TEST,MP_ASYNC_SEND);
            }
            PrintLog("MPTEST: StreamTest sent");
            //Sleep(WRITEDELAY);
        }
        else if (group->rank() == 0) {
            rank_t r;
            comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
            StringAttr str;
            PrintLog("MPTEST: StreamTest receiving");
            {
                TimedBlock block(STrecv);
                mb.read(str);
            }
            PrintLog("MPTEST: StreamTest received(%u) '%s' length %u",r,str.get(),mb.length());
        }
        else
        {
            PrintLog("MPTEST: StreamTest skipping extra rank %u", group->rank());
            break;
        }
    }

    comm->barrier();

    STsend.print();
    STrecv.print();
}


void Test1(IGroup *group,ICommunicator *comm)
{ 
    PrintLog("test1");
    CMessageBuffer mb;
    if (group->rank()==0) {
        mb.append("Hello - Test1");
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1) {
        rank_t r;
        comm->recv(mb,0,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("(1) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test2(IGroup *group,ICommunicator *comm)
{
    PrintLog("test2");
    CMessageBuffer mb;
    if (group->rank()==0) {
        mb.append("Hello - Test2");
        comm->send(mb,RANK_ALL,MPTAG_TEST);
    }
    else if (group->rank()==1) {
#ifdef GPF
        PrintLog("GPFING");
        Sleep(aWhile);
        byte *p = NULL; *p = 1;
#endif
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("(2) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test3(IGroup *group,ICommunicator *comm)
{
    PrintLog("test3");
    CMessageBuffer mb;
    if (group->rank()==0) {
        mb.append("Hello - Test3");
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1) {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("(3) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test4(IGroup *group,ICommunicator *comm)
{
    PrintLog("test4");
    CMessageBuffer mb;
    if (group->rank()==0) {
        INode *singlenode=&group->queryNode(1);
        IGroup *singlegroup = createIGroup(1,&singlenode);
        ICommunicator * singlecomm = createCommunicator(singlegroup);
        mb.append("Hello - Test4");
        singlecomm->send(mb,0,MPTAG_TEST);
        singlecomm->Release();
        singlegroup->Release();
    }
    else if (group->rank()==1) {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("(4) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test5(IGroup *group,ICommunicator *comm)
{
    PrintLog("test5");
    rank_t rank = group->rank();
    INode *singlenode=&group->queryNode(1);
    IGroup *singlegroup = createIGroup(1,&singlenode);
    ICommunicator * singlecomm = createCommunicator(singlegroup);
    CMessageBuffer mb;
    if (rank==0) {
        mb.append("Hello - Test5");
        singlecomm->send(mb,0,MPTAG_TEST);
    }
    else if (rank==1) {
        rank_t r;
        singlecomm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("(5) Received '%s' from rank %u (unknown)",str.get(),r);
    }
    comm->barrier();
    singlecomm->Release();
    singlegroup->Release();
}


void Test6(IGroup *group,ICommunicator *comm)
{
    PrintLog("test6");
    //DebugBreak();
    CMessageBuffer mb;
    StringAttr str;
    if (group->rank()==1) {
        mb.append("Test");
        bool cancelled = comm->sendRecv(mb,0,MPTAG_TEST);
        StringAttr str;
        mb.read(str);
        StringBuffer url;
        PrintLog("(6) Received '%s' from %s",str.get(),mb.getSender().getUrlStr(url).str());
    }
    else if (group->rank()==0) {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        mb.read(str);
        PrintLog("(6) - str = <%s>", str.get());
        assertex(strcmp(str.get(),"Test")==0);
        mb.clear();
        mb.append("Hello - Test6");

        printf("crash now!");
        Sleep(1);

        comm->reply(mb);
    }
    comm->barrier();
}

void Test7(IGroup *group,ICommunicator *comm)
{ 
    PrintLog("test7");
    CMessageBuffer mb;
    if (group->rank()==0) {
        mb.append("Hello - Test7");
        mb.reserve(150*1024);
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1) {
        rank_t r;
        comm->recv(mb,(mptag_t) TAG_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PrintLog("Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


// #define MAXBUFFERSIZE 0x100000
#define MAXBUFFERSIZE 0x10000
struct CRandomBuffer
{   
    size32_t size;
    char buffer[MAXBUFFERSIZE];
    unsigned crc;
    void fill() {
        size = getRandom()%MAXBUFFERSIZE;
        // size = 100000;
        if (size) {
            char c = (char)getRandom();
#if 0
            for (unsigned i=0;i<size;i++) {
                buffer[i] = c;
                c += (c*16);
                c += 113;
            }
#endif
            for (unsigned i=0;i<size;i++) {
                buffer[i] = 'a' + i%26;
            }
        }
        crc = crc32(&buffer[0],size,0);
    }
    bool check() 
    {
        int errs = 50;
        if (crc!=crc32(buffer,size,0)) {
            PrintLog("**** Error: CRC check failed");
            PrintLog("size = %u",size);
            char c = buffer[0];
            for (unsigned i=1;i<size;i++) {
                c += (c*16);
                c += 113;
                if (buffer[i] != c) {
                    PrintLog("Failed at %u, expected %02x found %02x %02x %02x %02x %02x %02x %02x %02x",i,(int)(byte)c,(int)(byte)buffer[i],(int)(byte)buffer[i+1],(int)(byte)buffer[i+2],(int)(byte)buffer[i+3],(int)(byte)buffer[i+4],(int)(byte)buffer[i+5],(int)(byte)buffer[i+6],(int)(byte)buffer[i+7]);
                    if (errs--==0)
                        break;
                }
            }
            return false;
        }
        return true;
    }
    void serialize(MemoryBuffer &mb)
    {
        // PROGLOG("1serialize: size = %u, length = %u", size, mb.length());
        mb.append(size).append(size,buffer).append(crc);
        // PROGLOG("2serialize: size = %u, length = %u", size, mb.length());
    }
    void deserialize(MemoryBuffer &mb)
    {
        // PROGLOG("1de-serialize: size = %u, length = %u", size, mb.length());
        mb.read(size);
        // PROGLOG("2de-serialize: size = %u, length = %u", size, mb.length());
        mb.read(size,buffer).read(crc);
    }
};

void printtrc(char c)
{
    static CriticalSection crit;
    CriticalBlock block(crit);
    printf("%c",c);
}

// #define N 100
#define N 20

void MultiTest(ICommunicator *_comm)
{

    class Server: public Thread
    {
    public:
        Owned<ICommunicator> comm;
        Server(ICommunicator *_comm) { comm.set(_comm); }
        int run()
        {
            unsigned n=(comm->queryGroup().ordinality()-1)*N;
            CMessageBuffer mb;
            CRandomBuffer *buff = new CRandomBuffer();
            PrintLog("MPTEST: MultiTest server started, myrank = %u", comm->queryGroup().rank());
            try {
                while(n--) {
                    mb.clear();
                    rank_t rr;
                    if (!comm->recv(mb,RANK_ALL,MPTAG_TEST,&rr)) 
                        break;
                    PrintLog("MPTEST: MultiTest server Received from %u, len = %u",rr, mb.length());
                    StringBuffer str;
                    comm->queryGroup().queryNode(rr).endpoint().getUrlStr(str);
                    // PrintLog("MPTEST: MultiTest server Received from %s",str.str());

                    buff->deserialize(mb);

#ifdef DO_CRC_CHECK
                    if (!buff->check())
                        PrintLog("MPTEST: MultiTest server Received from %s",str.str());
#endif

                    mb.clear().append(buff->crc);

                    int delay = getRandom() % 20;
                    Sleep(delay);

                    comm->reply(mb);
                }
            }
            catch (IException *e) {
                pexception("Server Exception",e);
            }

            comm->barrier();

            PrintLog("MPTEST: MultiTest server stopped");
            delete buff;
            return 0;
        }

    } server(_comm);

    Owned<ICommunicator> comm;
    comm.set(_comm); 

    server.start();

    CMessageBuffer mb;
    CRandomBuffer *buff = new CRandomBuffer();
    unsigned nr = comm->queryGroup().ordinality();
    unsigned n=(nr-1)*N;
    rank_t r = comm->queryGroup().rank();
    rank_t *targets = new rank_t[n];
    rank_t *t = targets;
    rank_t i;

    for (i=0;i<nr;i++) 
        if (i!=r)
            for (unsigned j=0;j<N;j++) 
                *(t++) = i;

    unsigned k=n;
    while (k>1) {
        i = getRandom()%k;  // NB n is correct here 
        k--;
        unsigned t = targets[i];
        targets[i] = targets[k];
        targets[k] = t;
    }

    PrintLog("MPTEST: Multitest client started, myrank = %u", comm->queryGroup().rank());

    try {
        while (n--) {
            buff->fill();
            buff->serialize(mb.clear());

#if 0
            StringBuffer str;
            comm->queryGroup().queryNode(targets[n]).endpoint().getUrlStr(str);
            PrintLog("MPTEST: Multitest client Sending to %s, length=%u",str.str(), mb.length());
#endif

            PrintLog("MPTEST: Multitest client Sending to %u, length=%u", targets[n], mb.length());

            if (!comm->sendRecv(mb,targets[n],MPTAG_TEST)) 
                break;

            // Sleep((n+1)*2000);
            // PrintLog("MPTEST: Multitest client Sent to %s",str.str());
            unsigned crc;
            mb.read(crc);
            assertex(crc==buff->crc);
        }
    }
    catch (IException *e) {
        pexception("Client Exception",e);
    }

    PrintLog("MPTEST: MultiTest client finished");

    server.join();

    delete [] targets;
    delete buff;
}

void MPRing(IGroup *group, ICommunicator *mpicomm, unsigned iters=0)
{
    CMessageBuffer smb;
    CMessageBuffer rmb;
    rank_t myrank = group->rank();
    rank_t numranks = group->ordinality();

    if (numranks < 2)
        throw MakeStringException(-1, "MPTEST: MPRing Error, numranks (%u) must be > 1", numranks);

    if (iters == 0)
        iters = 1000;

    unsigned pintvl = iters/10;
    if (pintvl < 1)
        pintvl = 1;

    PrintLog("MPTEST: MPRing myrank=%u numranks=%u iters=%u", myrank, numranks, iters);

    unsigned next = myrank;
    unsigned prev = myrank;
    unsigned k = 0;
    do
    {
        next = (next+1) % numranks;
        prev = prev > 0 ? prev-1 : numranks-1;

        // skip self
        if ( (next == prev) && (next == myrank) )
            continue;

        smb.clear();
        smb.append(k);
        if ((k%pintvl) == 0)
            PrintLog("MPTEST: MPRing %u send to rank %u", myrank, next);
        bool oksend = mpicomm->send(smb, next, MPTAG_TEST);
        if (!oksend)
            throw MakeStringException(-1, "MPTEST: MPRing %u send() to rank %u failed", myrank, next);

        rmb.clear();
        if ((k%pintvl) == 0)
            PrintLog("MPTEST: MPRing %u recv from rank %u", myrank, prev);
        bool okrecv = mpicomm->recv(rmb, prev, MPTAG_TEST);
        if (!okrecv)
            throw MakeStringException(-1, "MPTEST: MPRing %u recv() from rank %u failed", myrank, prev);
        rmb.read(k);

        k++;

        if ((k%pintvl) == 0)
            PrintLog("MPTEST: MPRing %u iteration %u complete", myrank, k);

        if (k == iters)
            break;
    }
    while (true);

    PrintLog("MPTEST: MPRing complete");

    mpicomm->barrier();

    return;
}

#define MSGLEN 1048576

void MPAlltoAll(IGroup *group, ICommunicator *mpicomm, size32_t buffsize=0, unsigned iters=0)
{
    rank_t myrank = group->rank();
    rank_t numranks = group->ordinality();

    if (numranks < 2)
        throw MakeStringException(-1, "MPAlltoAll: MPRing Error, numranks (%u) must be > 1", numranks);

    if (buffsize == 0)
        buffsize = MSGLEN;
    if (iters == 0)
        iters = 1000;
    if (iters < 1)
        iters = 1;

    PrintLog("MPTEST: MPAlltoAll myrank=%u numranks=%u buffsize=%u iters=%u", myrank, numranks, buffsize, iters);

    // ---------

    class Sender : public Thread
    {
    public:
        Linked<ICommunicator> mpicomm;
        rank_t numranks;
        rank_t myrank;
        size32_t buffsize;
        unsigned iters;
        Sender(ICommunicator *_mpicomm, rank_t _numranks, rank_t _myrank, size32_t _buffsize, unsigned _iters) : mpicomm(_mpicomm), numranks(_numranks), myrank(_myrank), buffsize(_buffsize), iters(_iters)
        {
        }

        int run()
        {
            PrintLog("MPTEST: MPAlltoAll sender started, myrank = %u", myrank);

            int pintvl = iters/10;
            if (pintvl < 1)
                pintvl = 1;

            CMessageBuffer smb;
            smb.appendBytes('a', buffsize);

            for (unsigned k=1;k<=iters;k++)
            {
                bool oksend = mpicomm->send(smb, RANK_ALL_OTHER, MPTAG_TEST);
                if (!oksend)
                    throw MakeStringException(-1, "MPTEST: MPAlltoAll %u send() failed", myrank);
                if ((k%pintvl) == 0)
                    PrintLog("MPTEST: MPAlltoAll sender %u iteration %u complete", myrank, k);
            }

            mpicomm->barrier();
            PrintLog("MPTEST: MPAlltoAll sender stopped");
            return 0;
        }
    } sender(mpicomm, numranks, myrank, buffsize, iters);

    unsigned startTime = msTick();

    sender.start();

    // ---------

    PrintLog("MPTEST: MPAlltoAll receiver started, myrank = %u", myrank);

    int pintvl = iters/10;
    if (pintvl < 1)
        pintvl = 1;

    CMessageBuffer rmb(buffsize);

    for (unsigned k=1;k<=iters;k++)
    {
        for (rank_t i=1;i<numranks;i++)
        {
            // rmb.clear();
            bool okrecv = mpicomm->recv(rmb, RANK_ALL, MPTAG_TEST);
            if (!okrecv)
                throw MakeStringException(-1, "MPTEST: MPAlltoAll %u recv() failed", myrank);
            if (i==1 && (k%pintvl) == 0)
                PrintLog("MPTEST: MPAlltoAll receiver rank %u iteration %u complete", myrank, k);
        }
    }

    mpicomm->barrier();

    PrintLog("MPTEST: MPAlltoAll receiver finished");

    // ---------

    sender.join();

    unsigned endTime = msTick();

    double msgRateMB = (2.0*(double)buffsize*(double)iters*(double)(numranks-1)) / ((endTime-startTime)*1000.0);

    PrintLog("MPTEST: MPAlltoAll complete %g MB/s", msgRateMB);

    return;
}

void MPTest2(IGroup *group, ICommunicator *mpicomm)
{
    rank_t myrank = group->rank();
    rank_t numranks = group->ordinality();

    PrintLog("MPTEST: MPTest2: myrank=%u numranks=%u", myrank, numranks);

    mpicomm->barrier();

    return;
}

void MPSelfSend(ICommunicator *mpcomm)
{
    CMessageBuffer mb;
    int sendMessage = 1234;
    int receivedMessage;

    rank_t myrank = mpcomm->getGroup()->rank();
    mb.append(sendMessage);
    mpcomm->send(mb, myrank, MPTAG_TEST);

    mb.clear();
    mpcomm->recv(mb, myrank, MPTAG_TEST);
    mb.read(receivedMessage);

    assertex(sendMessage == receivedMessage);
    PrintLog("MPTEST: %s: Message sent from %d to %d", TEST_SELFSEND, myrank, myrank);
}

void testIPnodeHash()
{
    setNodeCaching(true);
    class casyncfor: public CAsyncFor
    {
    public:
        casyncfor()
        {
        }
        void Do(unsigned i)
        {
            StringBuffer ips;
            ips.appendf("%d.%d.%d.%d",i/256,1,2,getRandom()%10);
            SocketEndpoint ep(ips.str());
            try {
                Owned<INode> node = createINode(ep);
            }
            catch (IException *e)
            {
                EXCLOG(e,"failed");
            }
        }
    } afor;
    afor.For(100000,10);
}

//-----------Utility classes and global variables---------------//
CriticalSection sendCriticalSec;
CriticalSection recvCriticalSec;
CriticalSection validateCriticalSec;
bool* validate;

int getNextCount(CriticalSection &sect, int &count)
{
    CriticalBlock block(sect);
    if (count)
        return count--;
    else
        return 0;
}

//validate that numbers from 1 to maxCounter are received only once
void setValidate(int i, int maxCounter)
{
    CriticalBlock block(validateCriticalSec);
    assertex(i>0);
    assertex(i<=maxCounter);
    assertex(validate[i-1] == false);
    validate[i-1] = true;
}

//-------------------------------------------------------------//

void MPRightShift(ICommunicator* comm)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t source_rank = (rank - 1 + p) % p;
    rank_t destination_rank = (rank + 1) % p;

    CMessageBuffer sendMsg;
    sendMsg.append(rank);
    comm->send(sendMsg, destination_rank, MPTAG_TEST);

    CMessageBuffer recvMsg;
    int received_msg;
    comm->recv(recvMsg, source_rank, MPTAG_TEST);
    recvMsg.read(received_msg);
    assertex(source_rank == received_msg);
    PrintLog("Message received from node %d to node %d.", source_rank, rank);
}

void MPReceiveFromAny(ICommunicator* comm, rank_t nodeRank)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t destinationRank = (p-1);
    double expectedValue = 1234.0;
    _T("nodeRank="<<nodeRank);
    if (rank == nodeRank)
    {
        CMessageBuffer sendMsg;
        sendMsg.append(expectedValue);
        comm->send(sendMsg, destinationRank, MPTAG_TEST);
        PrintLog("Message sent by node %d to node %d.", rank, destinationRank);
    }
    if (rank == destinationRank)
    {
        CMessageBuffer recvMsg;
        bool success = comm->recv(recvMsg, RANK_ALL, MPTAG_TEST);
        assertex(success);
        double receivedValue;
        recvMsg.read(receivedValue);
        _T("rank="<<comm->getGroup()->rank(recvMsg.getSender())<<" nodeRank="<<nodeRank);
        assertex(nodeRank == comm->getGroup()->rank(recvMsg.getSender()));
        assertex(expectedValue == receivedValue);
        PrintLog("Message successfully received from node %d to node %d.", comm->getGroup()->rank(recvMsg.getSender()), rank);
    }
}

void MPSendToAll(ICommunicator* comm, rank_t nodeRank)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    double expectedValue = 1234.0;
    double receivedValue;
    if (rank == nodeRank)
    {
        CMessageBuffer sendMsg;
        sendMsg.append(expectedValue);
        comm->send(sendMsg, RANK_ALL, MPTAG_TEST);
    }
    CMessageBuffer recvMsg;
    comm->recv(recvMsg, nodeRank, MPTAG_TEST, NULL);
    recvMsg.read(receivedValue);
    assertex(expectedValue == receivedValue);
    PrintLog("Message received from node %d to node %d.", nodeRank, rank);
}

void MPMultiMTSendRecv(ICommunicator* comm, int counter)
{
    assertex(comm->getGroup()->ordinality()>1);
    counter = (counter? counter: 100);
    int SEND_THREADS, RECV_THREADS;
    SEND_THREADS = RECV_THREADS = 8;
    rank_t rank = comm->getGroup()->rank();

    // nodes ranked 0 and 1 will be conducting this test
    if (rank<2)
    {
        validate = new bool[counter];
        for(int i=0; i<counter; i++) validate[i] = false;
        class SWorker: public Thread
        {
        private:
            ICommunicator* comm;
            int* counter;
        public:
            SWorker(ICommunicator* _comm, int* _counter):comm(_comm), counter(_counter){}
            int run()
            {
                IGroup *group = comm->getGroup();
                rank_t p = group->ordinality();
                rank_t rank = group->rank();
                rank_t destination_rank = 1 - rank;

                CMessageBuffer sendMsg;
                int served = 0;
                while(true)
                {
                    sendMsg.clear();
                    int v = getNextCount(sendCriticalSec, *counter);
                    if (v > 0)
                    {
                        sendMsg.append(v);
                        comm->send(sendMsg, destination_rank, MPTAG_TEST);
                        served++;
                    } else
                    {
                        break;
                    }
                }
                _T("This thread sent "<<served);
                return 0;
            }
        };
        class RWorker: public Thread
        {
        private:
            ICommunicator* comm;
            int* counter;
            int maxCounter;

        public:
            RWorker(ICommunicator* _comm, int* _counter):comm(_comm), counter(_counter), maxCounter(*_counter){}
            int run()
            {
                IGroup *group = comm->getGroup();
                rank_t p = group->ordinality();
                rank_t rank = group->rank();
                rank_t source_rank = 1 - rank;
                int served = 0;
                CMessageBuffer recvMsg;
                int received_msg;
                while (*counter)
                {
                    recvMsg.clear();
                    if (comm->recv(recvMsg, source_rank, MPTAG_TEST, NULL, 100))
                    {
                        recvMsg.read(received_msg);
                        setValidate(received_msg, maxCounter);
                        getNextCount(recvCriticalSec, *counter);
                        served++;
                    }
                }
                _T("This thread received "<<served);
                return 0;
            }
        };
        std::vector<Thread*> workers;
        int s_counter, r_counter;
        s_counter = r_counter = counter;
        _T("counter="<<counter);
        for(int i=0;i<SEND_THREADS; i++)
        {
            workers.push_back(new SWorker(comm, &s_counter));
        }
        for(int i=0;i<RECV_THREADS; i++)
        {
            workers.push_back(new RWorker(comm, &r_counter));
        }
        for(int i=0;i<workers.size(); i++)
        {
            workers[i]->start();
        }
        for(int i=0;i<workers.size(); i++)
        {
            workers[i]->join();
        }
        for(int i=0;i<workers.size(); i++)
        {
            delete workers[i];
        }
        assertex(s_counter == 0);
        assertex(r_counter == 0);
        PrintLog("Rank %d sent %d messages", rank, (counter-s_counter));
        PrintLog("Rank %d received %d messages", rank, (counter-r_counter));
        delete validate;
    }
    comm->barrier();
}

void runTest(const char *caption, char testname[256], const Owned<IGroup>& group,
        ICommunicator* comm, unsigned numiters,
        size32_t buffsize, rank_t inputRank)
{
    if (group.get()->rank()==0)
    {
        printf("\n\n");
        PrintLog("%s", caption);
        PrintLog("========================");
    }
    comm->barrier();
#ifdef STREAMTEST
        StreamTest(group,comm);
#else
# ifdef MULTITEST
        MultiTest(comm);
# else
#  ifdef MPRING
        MPRing(group, comm, numiters);
#  else
#   ifdef MPALLTOALL
        MPAlltoAll(group, comm, buffsize, numiters);
#   else
#    ifdef MPTEST2
        MPTest2(group, comm);
#    else
#     ifdef DYNAMIC_TEST
        if (strnicmp(testname, TEST_STREAM, 6) == 0)
            StreamTest(group, comm);
        else if (strnicmp(testname, TEST_MULTI, 5) == 0)
            MultiTest(comm);
        else if (strieq(testname, "MPRing") || strieq(testname, TEST_RING))
            MPRing(group, comm, numiters);
        else if (strieq(testname, "MPAlltoAll") || strieq(testname, TEST_AlltoAll))
            MPAlltoAll(group, comm, buffsize, numiters);
        else if (strieq(testname, "MPTest2") || strieq(testname, TEST_RANK))
            MPTest2(group, comm);
        else if (strieq(testname, TEST_SELFSEND))
            MPSelfSend(comm);
        else if (strieq(testname, TEST_SINGLE_SEND) )
            Test1(group, comm);
        else if ( strieq(testname, TEST_RIGHT_SHIFT) )
            MPRightShift(comm);
        else if ( strieq(testname, TEST_RECV_FROM_ANY) )
            MPReceiveFromAny(comm, inputRank);
        else if ( strieq(testname, TEST_SEND_TO_ALL) )
            MPSendToAll(comm, inputRank);
        else if ( strieq(testname, TEST_MULTI_MT) )
            MPMultiMTSendRecv(comm, numiters);
        else if ((int) (strlen(testname)) > 0)
            PrintLog("MPTEST: Error, invalid testname specified (-t %s)", testname);
        else
            // default is MPRing ...
            MPRing(group, comm, numiters);
#     else
        for (unsigned i = 0;i<1;i++)
        {
            Test1(group,comm);
            PrintLog("MPTEST: test1 done, waiting"); Sleep(aWhile);
            Test2(group,comm);
            PrintLog("MPTEST: test2 done, waiting"); Sleep(aWhile);
            Test3(group,comm);
            PrintLog("MPTEST: test3 done, waiting"); Sleep(aWhile);
            Test4(group,comm);
            PrintLog("MPTEST: test4 done, waiting"); Sleep(aWhile);
            Test5(group,comm);
            PrintLog("MPTEST: test5 done, waiting"); Sleep(aWhile);
            Test6(group,comm);
            PrintLog("MPTEST: test6 done, waiting"); Sleep(aWhile);
            Test7(group,comm);
            PrintLog("MPTEST: test7 done, waiting"); Sleep(aWhile);
        }
#     endif
#    endif
#   endif
#  endif
# endif
#endif
    comm->barrier();
}

bool createNodeList(IArrayOf<INode> &nodes, const char* hostfile, int my_port, rank_t max_ranks) {
    unsigned i = 1;
    char hoststr[256] = { "" };
    FILE* fp = fopen(hostfile, "r");
    if (fp == NULL) {
        PrintLog("MPTest: Error, cannot open hostfile <%s>", hostfile);
        return false;
    }
    char line[256] = { "" };
    while (fgets(line, 255, fp) != NULL) {
        if ((max_ranks > 0) && ((i - 1) >= max_ranks))
            break;

        int srtn = sscanf(line, "%s", hoststr);
        if (srtn == 1 && line[0] != '#') {
            INode* newNode = createINode(hoststr, my_port);
            nodes.append(*newNode);
            i++;
        }
    }
    fclose(fp);
    return true;
}

int getServerPort(int basePort) {
    int my_port = basePort;
    int rank = getMPIGlobalRank();
    if (rank >= 0)            // mpi launch
    {
        /* when launched via MPI, my_port is same
         * Ideally MPI would pass in rank and the rest can be done internally.
         * For now manipulate my_port in useMPI case to make different per process based on rank.
         */
        my_port += rank;
    }
    return my_port;
}

void printHelp(char* executableName) {
    printf("\nMPTEST: Usage: %s <myport> [-f <hostfile> [-t <testname> -b <buffsize> -i <iters> -r <rank> -n <numprocs> -d] | <ip:port> <ip:port>] [-mpi] [-mp]\n\n",executableName);
    std::vector<std::string> tests = { TEST_RANK, TEST_SELFSEND, TEST_MULTI,
            TEST_STREAM, TEST_RING, TEST_AlltoAll, TEST_SINGLE_SEND,
            TEST_RIGHT_SHIFT, TEST_RECV_FROM_ANY, TEST_SEND_TO_ALL,
            TEST_MULTI_MT };
    std::vector<std::string>::iterator it = tests.begin();
    printf("\t <testname>\t%s\n", (*it).c_str());
    it++;
    for (; it != tests.end(); ++it)
        printf("\t\t\t%s\n", (*it).c_str());
    printf("\n");
}

int main(int argc, char* argv[])
{
    int mpi_debug = 0;
    char testname[256] = { "" };
    size32_t buffsize = 0;
    unsigned numiters = 0;
    rank_t max_ranks = 0;
    rank_t inputRank = 0;
    bool useMPI = false;
    bool useMP = false;

    InitModuleObjects();
    EnableSEHtoExceptionMapping();

//  startMPServer(9123);
//  testIPnodeHash();
//  stopMPServer();
//  return 0;

/*  mp hostfile format:
 *  -------------------
 *  <IP0>:port0
 *  <IP1>:port1
 *  <IP2>:port2
 *  ...
 *
 *  run script:
 *  -----------
 *  # NOTE: because mptest will stop if its cmdline port and native IP do not match
 *  # corresponding entry in hostfile - the same cmdline can be repeated on all hosts ...
 *  mptest port0 -f hostfile [-t testname] [-b buffsize] [-i iters] [-n numprocs] [-d] &
 *  mptest port1 -f hostfile ... &
 *  ...
 *  [wait]
 *
 *  Test names (-t):
 *  -----------
 *  MPRing (default)
 *  StreamTest
 *  MultiTest
 *  MPAlltoAll
 *  MPTest2
 *
 *  Options: (available with -f hostfile arg)
 *  --------
 *  -b buffsize (bytes) for MPAlltoAll test
 *  -i iterations for MPRing and MPAlltoAll tests
 *  -n numprocs for when wanting to test a subset of ranks from hostfile/script
 *  -d for some additional debug output
 */

    int argSize = argc;
    char** argL = argv;

#ifndef MYMACHINES
    if (argSize<3) {
        printHelp(argv[0]);
        return 0;
    }
#endif

    try {
        EnableSEHtoExceptionMapping();
        StringBuffer lf;

#ifndef MYMACHINES
        rank_t tot_ranks = 0;
        int basePort = atoi(argL[1]);


        int my_port = getServerPort(basePort);
        char logfile[256] = { "" };
        sprintf(logfile,"mptest-%d.log",my_port);

        IArrayOf<INode> nodes;

        const char * hostfile = nullptr;
        if (argSize > 3)
        {
            if (strcmp(argL[2], "-f") == 0)
                hostfile = argL[3];
        }
        if (hostfile)
        {

            int j = 4;
            while (j < argSize)
            {
                if (streq(argL[j], "-t"))
                {
                    if ((j+1) < argSize)
                    {
                        strcpy(testname, argL[j+1]);
                        j++;
                    }
                }
                else if (streq(argL[j], "-d"))
                {
                    mpi_debug++;
                }
                else if (streq(argL[j], "-b"))
                {
                    if ((j+1) < argSize)
                    {
                        buffsize = atoi(argL[j+1]);
                        j++;
                    }
                }
                else if (streq(argL[j], "-i"))
                {
                    if ((j+1) < argSize)
                    {
                        numiters = atoi(argL[j+1]);
                        j++;
                    }
                }
                else if ( streq(argL[j], "-n") || streq(argL[j], "-np") )
                {
                    if ((j+1) < argSize)
                    {
                        max_ranks = atoi(argL[j+1]);
                        j++;
                    }
                }
                else if (streq(argL[j], "-r"))
                {
                    if ((j+1) < argSize)
                    {
                        inputRank = atoi(argL[j+1]);
                        j++;
                    }
                }
                else if (streq(argL[j], "-mpi"))
                {
                    useMPI = true;
                }
                else if (streq(argL[j], "-mp"))
                {
                    useMP = true;
                }
                j++;
            }
            if (!createNodeList(nodes, hostfile, my_port, max_ranks))
                return 1;
        }
        else
        {
            unsigned i = 1;
            while (i+1 < argSize)
            {
                PrintLog("MPTEST: adding node %u, port = <%s>", i-1, argL[i+1]);
                INode *newNode = createINode(argL[i+1], my_port);
                nodes.append(*newNode);
                i++;
            }
        }
        tot_ranks = nodes.length();

        Owned<IGroup> group = createIGroup(tot_ranks, nodes.getArray());

        // stop if not meant for this host ...

        IpAddress myIp;
        GetHostIp(myIp);
        SocketEndpoint myEp(my_port, myIp);

        bool die = true;
        for (rank_t k=0;k<tot_ranks;k++)
        {
            if (nodes.item(k).endpoint().equals(myEp))
                die = false;
        }

        if (die)
            return 0;
        PrintLog("MPTEST: Starting, port = %d tot ranks = %u", my_port, tot_ranks);
        startMPServer(my_port);

        if (mpi_debug)
        {
            for (rank_t k=0;k<tot_ranks;k++)
            {
                StringBuffer urlStr;
                nodes.item(k).endpoint().getUrlStr(urlStr);
                PrintLog("MPTEST: adding node %u, %s", k, urlStr.str());
            }
        }
#else
        openLogFile(lf, "mptest.log");
        startMPServer(MPPORT);
        Owned<IGroup> group = createIGroup(MYMACHINES,MPPORT);
#endif

        Owned<ICommunicator> comm;
        if (useMP || !(useMP || useMPI))
        {
            comm.setown(createCommunicator(group));
            runTest("MPTEST: Running MP Test:", testname, group, comm, numiters, buffsize, inputRank);
        }
        if (useMPI || !(useMP || useMPI))
        {
            comm.setown(createMPICommunicator(group));
            runTest("MPTEST: Running MPI Test:", testname, group, comm, numiters, buffsize, inputRank);
        }

        stopMPServer();
    }
    catch (IException *e) {
        pexception("Exception",e);
    }

    PrintLog("MPTEST: bye");
    return 0;
}
