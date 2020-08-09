

// CSocketSelectThread error 10038  

#include <platform.h>
#include <jlib.hpp>
#include <jthread.hpp>
#include <jmisc.hpp>
#include <jcrc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>

#include <algorithm>
#include <queue>
#include <string>

using namespace std;

#define MPPORT 8888

//#define GPF

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
#define TEST_NXN "NxN"

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
        for (i=0;i<999;i++)
        {
            if (tids[i]==tid)
                break;
            if (tids[i]==0)
            {
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
            PROGLOG("TIME: %s(%u): max=%.6f, avg=%.6f, tot=%.6f",name,count,max,(double)total/count,total);
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
    for (unsigned i=0;i<NITER;i++)
    {
        if (group->rank() == 1)
        {
            mb.clear();
            StringBuffer header;
            header.append("Test Block #").append(i);
            mb.append(header.str()).reserve(BLOCKSIZE-mb.length());
            PROGLOG("MPTEST: StreamTest sending '%s' length %u",header.str(),mb.length());
            {
                TimedBlock block(STsend);
                comm->send(mb,0,MPTAG_TEST,MP_ASYNC_SEND);
            }
            PROGLOG("MPTEST: StreamTest sent");
            //Sleep(WRITEDELAY);
        }
        else if (group->rank() == 0)
        {
            rank_t r;
            comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
            StringAttr str;
            PROGLOG("MPTEST: StreamTest receiving");
            {
                TimedBlock block(STrecv);
                mb.read(str);
            }
            PROGLOG("MPTEST: StreamTest received(%u) '%s' length %u",r,str.get(),mb.length());
        }
        else
        {
            PROGLOG("MPTEST: StreamTest skipping extra rank %u", group->rank());
            break;
        }
    }

    comm->barrier();

    STsend.print();
    STrecv.print();
}


void Test1(IGroup *group,ICommunicator *comm)
{ 
    PROGLOG("test1");
    CMessageBuffer mb;
    if (group->rank()==0)
    {
        mb.append("Hello - Test1");
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1)
    {
        rank_t r;
        comm->recv(mb,0,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("(1) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test2(IGroup *group,ICommunicator *comm)
{
    PROGLOG("test2");
    CMessageBuffer mb;
    if (group->rank()==0)
    {
        mb.append("Hello - Test2");
        comm->send(mb,RANK_ALL,MPTAG_TEST);
    }
    else if (group->rank()==1)
    {
#ifdef GPF
        PROGLOG("GPFING");
        Sleep(aWhile);
        byte *p = NULL; *p = 1;
#endif
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("(2) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test3(IGroup *group,ICommunicator *comm)
{
    PROGLOG("test3");
    CMessageBuffer mb;
    if (group->rank()==0)
{
        mb.append("Hello - Test3");
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1)
    {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("(3) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test4(IGroup *group,ICommunicator *comm)
{
    PROGLOG("test4");
    CMessageBuffer mb;
    if (group->rank()==0)
    {
        INode *singlenode=&group->queryNode(1);
        IGroup *singlegroup = createIGroup(1,&singlenode);
        ICommunicator * singlecomm = createCommunicator(singlegroup);
        mb.append("Hello - Test4");
        singlecomm->send(mb,0,MPTAG_TEST);
        singlecomm->Release();
        singlegroup->Release();
    }
    else if (group->rank()==1)
    {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("(4) Received '%s' from rank %u",str.get(),r);
    }
    comm->barrier();
}


void Test5(IGroup *group,ICommunicator *comm)
{
    PROGLOG("test5");
    rank_t rank = group->rank();
    INode *singlenode=&group->queryNode(1);
    IGroup *singlegroup = createIGroup(1,&singlenode);
    ICommunicator * singlecomm = createCommunicator(singlegroup);
    CMessageBuffer mb;
    if (rank==0) {
        mb.append("Hello - Test5");
        singlecomm->send(mb,0,MPTAG_TEST);
    }
    else if (rank==1)
    {
        rank_t r;
        singlecomm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("(5) Received '%s' from rank %u (unknown)",str.get(),r);
    }
    comm->barrier();
    singlecomm->Release();
    singlegroup->Release();
}


void Test6(IGroup *group,ICommunicator *comm)
{
    PROGLOG("test6");
    //DebugBreak();
    CMessageBuffer mb;
    StringAttr str;
    if (group->rank()==1)
    {
        mb.append("Test");
        bool cancelled = comm->sendRecv(mb,0,MPTAG_TEST);
        StringAttr str;
        mb.read(str);
        StringBuffer url;
        PROGLOG("(6) Received '%s' from %s",str.get(),mb.getSender().getUrlStr(url).str());
    }
    else if (group->rank()==0)
    {
        rank_t r;
        comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
        mb.read(str);
        PROGLOG("(6) - str = <%s>", str.get());
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
    PROGLOG("test7");
    CMessageBuffer mb;
    if (group->rank()==0)
    {
        mb.append("Hello - Test7");
        mb.reserve(150*1024);
        comm->send(mb,1,MPTAG_TEST);
    }
    else if (group->rank()==1)
    {
        rank_t r;
        comm->recv(mb,(mptag_t) TAG_ALL,MPTAG_TEST,&r);
        StringAttr str;
        mb.read(str);
        PROGLOG("Received '%s' from rank %u",str.get(),r);
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
    void fill()
    {
        size = getRandom()%MAXBUFFERSIZE;
        // size = 100000;
        if (size)
        {
            char c = (char)getRandom();
#if 0
            for (unsigned i=0;i<size;i++)
            {
                buffer[i] = c;
                c += (c*16);
                c += 113;
            }
#endif
            for (unsigned i=0;i<size;i++)
            {
                buffer[i] = 'a' + i%26;
            }
        }
        crc = crc32(&buffer[0],size,0);
    }
    bool check() 
    {
        int errs = 50;
        if (crc!=crc32(buffer,size,0))
        {
            PROGLOG("**** Error: CRC check failed");
            PROGLOG("size = %u",size);
            char c = buffer[0];
            for (unsigned i=1;i<size;i++)
            {
                c += (c*16);
                c += 113;
                if (buffer[i] != c)
                {
                    PROGLOG("Failed at %u, expected %02x found %02x %02x %02x %02x %02x %02x %02x %02x",i,(int)(byte)c,(int)(byte)buffer[i],(int)(byte)buffer[i+1],(int)(byte)buffer[i+2],(int)(byte)buffer[i+3],(int)(byte)buffer[i+4],(int)(byte)buffer[i+5],(int)(byte)buffer[i+6],(int)(byte)buffer[i+7]);
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
            PROGLOG("MPTEST: MultiTest server started, myrank = %u", comm->queryGroup().rank());
            try
            {
                while(n--)
                {
                    mb.clear();
                    rank_t rr;
                    if (!comm->recv(mb,RANK_ALL,MPTAG_TEST,&rr)) 
                        break;
                    PROGLOG("MPTEST: MultiTest server Received from %u, len = %u",rr, mb.length());
                    StringBuffer str;
                    comm->queryGroup().queryNode(rr).endpoint().getUrlStr(str);
                    // PROGLOG("MPTEST: MultiTest server Received from %s",str.str());

                    buff->deserialize(mb);

#ifdef DO_CRC_CHECK
                    if (!buff->check())
                        PROGLOG("MPTEST: MultiTest server Received from %s",str.str());
#endif

                    mb.clear().append(buff->crc);

                    int delay = getRandom() % 20;
                    Sleep(delay);

                    comm->reply(mb);
                }
            }
            catch (IException *e)
            {
                pexception("Server Exception",e);
                e->Release();
            }

            comm->barrier();

            PROGLOG("MPTEST: MultiTest server stopped");
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
    while (k>1)
    {
        i = getRandom()%k;  // NB n is correct here 
        k--;
        unsigned t = targets[i];
        targets[i] = targets[k];
        targets[k] = t;
    }

    PROGLOG("MPTEST: Multitest client started, myrank = %u", comm->queryGroup().rank());

    try {
        while (n--)
        {
            buff->fill();
            buff->serialize(mb.clear());

#if 0
            StringBuffer str;
            comm->queryGroup().queryNode(targets[n]).endpoint().getUrlStr(str);
            PROGLOG("MPTEST: Multitest client Sending to %s, length=%u",str.str(), mb.length());
#endif

            PROGLOG("MPTEST: Multitest client Sending to %u, length=%u", targets[n], mb.length());

            if (!comm->sendRecv(mb,targets[n],MPTAG_TEST)) 
                break;

            // Sleep((n+1)*2000);
            // PROGLOG("MPTEST: Multitest client Sent to %s",str.str());
            unsigned crc;
            mb.read(crc);
            assertex(crc==buff->crc);
        }
    }
    catch (IException *e)
    {
        pexception("Client Exception",e);
        e->Release();
    }

    PROGLOG("MPTEST: MultiTest client finished");

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

    PROGLOG("MPTEST: MPRing myrank=%u numranks=%u iters=%u", myrank, numranks, iters);

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
            PROGLOG("MPTEST: MPRing %u send to rank %u", myrank, next);
        bool oksend = mpicomm->send(smb, next, MPTAG_TEST);
        if (!oksend)
            throw MakeStringException(-1, "MPTEST: MPRing %u send() to rank %u failed", myrank, next);

        rmb.clear();
        if ((k%pintvl) == 0)
            PROGLOG("MPTEST: MPRing %u recv from rank %u", myrank, prev);
        bool okrecv = mpicomm->recv(rmb, prev, MPTAG_TEST);
        if (!okrecv)
            throw MakeStringException(-1, "MPTEST: MPRing %u recv() from rank %u failed", myrank, prev);
        rmb.read(k);

        k++;

        if ((k%pintvl) == 0)
            PROGLOG("MPTEST: MPRing %u iteration %u complete", myrank, k);

        if (k == iters)
            break;
    }
    while (true);

    PROGLOG("MPTEST: MPRing complete");

    mpicomm->barrier();
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

    PROGLOG("MPTEST: MPAlltoAll myrank=%u numranks=%u buffsize=%u iters=%u", myrank, numranks, buffsize, iters);

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
            PROGLOG("MPTEST: MPAlltoAll sender started, myrank = %u", myrank);

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
                    PROGLOG("MPTEST: MPAlltoAll sender %u iteration %u complete", myrank, k);
            }

            mpicomm->barrier();
            PROGLOG("MPTEST: MPAlltoAll sender stopped");
            return 0;
        }
    } sender(mpicomm, numranks, myrank, buffsize, iters);

    unsigned startTime = msTick();

    sender.start();

    // ---------

    PROGLOG("MPTEST: MPAlltoAll receiver started, myrank = %u", myrank);

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
                PROGLOG("MPTEST: MPAlltoAll receiver rank %u iteration %u complete", myrank, k);
        }
    }

    mpicomm->barrier();

    PROGLOG("MPTEST: MPAlltoAll receiver finished");

    // ---------

    sender.join();

    unsigned endTime = msTick();

    double msgRateMB = (2.0*(double)buffsize*(double)iters*(double)(numranks-1)) / ((endTime-startTime)*1000.0);

    PROGLOG("MPTEST: MPAlltoAll complete %g MB/s", msgRateMB);

    return;
}

void MPTest2(IGroup *group, ICommunicator *mpicomm)
{
    rank_t myrank = group->rank();
    rank_t numranks = group->ordinality();

    PROGLOG("MPTEST: MPTest2: myrank=%u numranks=%u", myrank, numranks);

    mpicomm->barrier();

    return;
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


/**
 * Test sending a message to its self
 */
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
    PROGLOG("MPTEST: %s: Message sent from %d to %d", TEST_SELFSEND, myrank, myrank);
}

/**
 * Test sending message to next (wrap-around) processor
 */
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
    PROGLOG("Message received from node %d to node %d.", source_rank, rank);
}

/**
 * Test receiving message from an unknown node
 */
void MPReceiveFromAny(ICommunicator* comm)
{
    IGroup* group = comm->getGroup();
    rank_t nodeRank = group->rank();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t destinationRank = (p-1);
    double expectedValue = 1234.0;
    PROGLOG("nodeRank=%u", nodeRank);
    if (rank == nodeRank)
    {
        CMessageBuffer sendMsg;
        sendMsg.append(expectedValue);
        comm->send(sendMsg, destinationRank, MPTAG_TEST);
        PROGLOG("Message sent by node %d to node %d.", rank, destinationRank);
    }
    if (rank == destinationRank)
    {
        CMessageBuffer recvMsg;
        bool success = comm->recv(recvMsg, RANK_ALL, MPTAG_TEST);
        assertex(success);
        double receivedValue;
        recvMsg.read(receivedValue);
        PROGLOG("rank=%u, nodeRank=%u", comm->getGroup()->rank(recvMsg.getSender()), nodeRank);
        assertex(nodeRank == comm->getGroup()->rank(recvMsg.getSender()));
        assertex(expectedValue == receivedValue);
        PROGLOG("Message successfully received from node %d to node %d.", comm->getGroup()->rank(recvMsg.getSender()), rank);
    }
}

/**
 * Test one node sending a message to all nodes
 */
void MPSendToAll(ICommunicator* comm)
{
    IGroup* group = comm->getGroup();
    rank_t nodeRank = group->rank();
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
    PROGLOG("Message received from node %d to node %d.", nodeRank, rank);
}

/**
 * Test multiple threads calling send and recv functions
 */
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
                    }
                    else
                    {
                        break;
                    }
                }
                PROGLOG("This thread sent %d", served);
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
                PROGLOG("This thread received %d", served);
                return 0;
            }
        };
        std::vector<Thread*> workers;
        int s_counter, r_counter;
        s_counter = r_counter = counter;
        PROGLOG("counter=%d", counter);
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
        PROGLOG("Rank %d sent %d messages", rank, (counter-s_counter));
        PROGLOG("Rank %d received %d messages", rank, (counter-r_counter));
        delete [] validate;
    }
    comm->barrier();
}

void MPNxN(ICommunicator *comm, unsigned numStreams, size32_t perStreamMBSize, size32_t msgSize, bool async)
{
    // defaults
    if (0 == numStreams)
        numStreams = 8;
    if (0 == perStreamMBSize)
        perStreamMBSize = 100;
    if (0 == msgSize)
        msgSize = 1024*1024;

    unsigned grpSize = comm->queryGroup().ordinality();
    rank_t myRank = comm->queryGroup().rank();

    PROGLOG("MPNxN: myrank=%u, numStreams=%u, perStreamMBSize=%u, msgSize(bytes)=%u", myRank=(unsigned)myRank, numStreams, perStreamMBSize, msgSize);

    class CSendStream : public CInterfaceOf<IInterface>, implements IThreaded
    {
        CThreaded threaded;
        ICommunicator *comm = nullptr;
        rank_t myRank;
        unsigned grpSize;
        mptag_t mpTag, replyTag;
        unsigned __int64 totalSendSize;
        size32_t msgSize;
        StringBuffer resultMsg;
        bool passed = false;
        std::vector<rank_t> tgtRanks;
        StringBuffer tgtRanksStr;
        bool async = false;

        // used if async
        class CAckThread : implements IThreaded
        {
            CThreaded threaded;
            CSendStream &owner;
            mptag_t mpTag;
            CriticalSection cs;
            Owned<IException> exception;
            std::vector<std::queue<unsigned>> expectedHashes;

            unsigned getNextExpectedHash(unsigned sender)
            {
                std::queue<unsigned> &queue = expectedHashes[sender];

                CriticalBlock b(cs);
                assertex(queue.size());
                unsigned hash = queue.front();
                expectedHashes[sender].pop();
                return hash;
            }
        public:
            CAckThread(CSendStream &_owner, mptag_t _mpTag) : owner(_owner), mpTag(_mpTag), threaded("CAckThread", this)
            {
                expectedHashes.resize(owner.grpSize);
            }
            void addHash(unsigned tgt, unsigned hash)
            {
                CriticalBlock b(cs);
                expectedHashes[tgt].push(hash);
            }
            void start() { threaded.start(); }
            void join()
            {
                threaded.join();
                if (exception)
                    throw exception.getClear();
            }
            // IThredaed
            virtual void threadmain() override
            {
                try
                {
                    CMessageBuffer msg;
                    rank_t sender;
                    unsigned finalAcks = 0;
                    while (true)
                    {
                        unsigned receivedHash;
                        if (!owner.receiveAck(msg, sender, receivedHash)) // empty message indicates end by client
                        {
                            finalAcks++;
                            if (finalAcks == owner.grpSize)
                                break; // all done
                        }
                        else
                        {
                            unsigned expectedHash = getNextExpectedHash(sender);
                            owner.verifyAck(receivedHash, expectedHash);
                        }
                    }
                }
                catch (IException *e)
                {
                    exception.setown(e);
                }
            }
        } ackThread;

        unsigned fillData(void *data, size32_t sz, unsigned hash)
        {
            byte *pData = (byte *)data;
            do
            {
                if (sz<sizeof(hash))
                {
                    memset(pData, 0, sz);
                    break;
                }
                hash = hashc((unsigned char *)&hash, sizeof(hash), hash);
                memcpy(pData, &hash, sizeof(hash));
                sz -= sizeof(hash);
                pData += sizeof(hash);
            }
            while (true);
            return hash;
        }
        bool receiveAck(CMessageBuffer &msg, rank_t &sender, unsigned &ack)
        {
            while (!comm->recv(msg, RANK_ALL, replyTag, &sender, 60000))
                WARNLOG("Waiting for receive ack");
            if (0 == msg.length())
                return false; // final empty message indicated complete
            if (std::find(tgtRanks.begin(), tgtRanks.end(), sender) == tgtRanks.end())
                throwStringExceptionV(0, "Received reply from rank(%u) this stream did not send to", (unsigned)sender);
            msg.read(ack);
            msg.clear();
            return true;
        }
        void verifyAck(unsigned receivedHash, unsigned expectedHash)
        {
            if (receivedHash != expectedHash)
                throwStringExceptionV(0, "Checksums mismatch: %u sent vs %u received", expectedHash, receivedHash);
        }

    public:
        CSendStream(ICommunicator *_comm, rank_t _myRank, unsigned _grpSize, mptag_t _mpTag, unsigned __int64 _totalSendSize, size32_t _msgSize, bool _async)
            : threaded("CSendStream", this), comm(_comm), myRank(_myRank), grpSize(_grpSize), mpTag(_mpTag), totalSendSize(_totalSendSize), msgSize(_msgSize), ackThread(*this, _mpTag), async(_async)
        {
            if (1 == grpSize) // group only contains self, so target self
                tgtRanks.push_back(0);
            else
            {
                unsigned pc=25; // %

                // add 'pc'% targets, starting from myRank+1
                unsigned num = grpSize*pc/100;
                if (0 == num)
                    num = 1;
                unsigned step = grpSize / num;
                unsigned t = myRank+1;
                if (t == grpSize)
                    t = 0;
                while (true)
                {
                    tgtRanks.push_back(t);
                    --num;
                    if (0 == num)
                        break;
                    t += step;
                    if (t >= grpSize)
                        t -= grpSize;
                }
            }

            auto iter = tgtRanks.begin();
            while (true)
            {
                tgtRanksStr.append(*iter);
                iter++;
                if (iter == tgtRanks.end())
                    break;
                tgtRanksStr.append(",");
            }
            replyTag = createReplyTag();
            if (async)
                ackThread.start();
            threaded.start();
        }
        ~CSendStream()
        {
            threaded.join();
        }
        bool waitResult(StringBuffer &resultMessage)
        {
            threaded.join();
            return passed;
        }
        const char *queryTgtRanks() { return tgtRanksStr; }
        virtual void threadmain() override
        {
            passed = false;
            try
            {
                CMessageBuffer msg, recvMsg;
                msg.setReplyTag(replyTag);
                void *data = msg.reserveTruncate(msgSize);
                unsigned hash = (unsigned)mpTag;

                VStringBuffer logMsg("NxN: mpTag=%u, dstRank(s) [%s]", (unsigned)mpTag, tgtRanksStr.str());
                PROGLOG("%s", logMsg.str());

                unsigned __int64 remaining = totalSendSize;
                CCycleTimer timer;
                while (true)
                {
                    size32_t sz;
                    if (remaining >= msgSize)
                    {
                        sz = msgSize;
                        remaining -= msgSize;
                    }
                    else
                    {
                        sz = remaining;
                        msg.setLength(sz);
                        remaining = 0;
                    }
                    hash = fillData(data, sz, hash);
                    for (rank_t t: tgtRanks)
                    {
                        if (async)
                            ackThread.addHash(t, hash);
                        if (!comm->send(msg, t, mpTag))
                            throwUnexpected();
                    }
                    if (!async)
                    {
                        for (int t: tgtRanks)
                        {
                            rank_t sender;
                            unsigned receivedHash;
                            assertex(receiveAck(recvMsg, sender, receivedHash));
                            verifyAck(hash, receivedHash);
                        }
                    }
                    if (!remaining)
                        break;
                }
                msg.clear();
                // send blank msg to all to signal end to receivers.
                if (!comm->send(msg, RANK_ALL, mpTag))
                    throwUnexpected();
                if (async)
                    ackThread.join();
                else
                {
                    rank_t sender;
                    for (unsigned r=0; r<grpSize; r++)
                    {
                        while (!comm->recv(msg, r, replyTag, &sender, 60000))
                            WARNLOG("Waiting for final ack");
                        assertex(sender == r);
                        assertex(0 == msg.length());
                    }
                }
                float ms = timer.elapsedMs();
                float mbPerSec = (totalSendSize/ms*1000)/0x100000;
                PROGLOG("Stream stats: time taken = %.2f seconds, total sent=%u MB, throughput = %.2f MB/s", ms/1000, (unsigned)(totalSendSize/0x100000), mbPerSec);
            }
            catch (IException *e)
            {
                e->errorMessage(resultMsg);
                EXCLOG(e, "FAIL");
                e->Release();
                return;
            }
            passed = true;
        }
    };
    class CRecvServer : public CInterfaceOf<IInterface>, implements IThreaded
    {
        CThreaded threaded;
        ICommunicator *comm;
        rank_t myRank;
        unsigned grpSize;
        mptag_t mpTag;
        unsigned numStreams;

        unsigned checkData(MemoryBuffer &mb, unsigned hash)
        {
            size32_t len = mb.remaining();
            const byte *p = (const byte *)mb.readDirect(len);
            while (len >= sizeof(hash))
            {
                hash = hashc((unsigned char *)&hash, sizeof(hash), hash);
                if (0 != memcmp(p, &hash, sizeof(hash)))
                    return 0;
                p += sizeof(hash);
                len -= sizeof(hash);
            }
            return hash;
        }
    public:
        CRecvServer(ICommunicator *_comm, rank_t _myRank, unsigned _grpSize, mptag_t _mpTag, unsigned _numStreams)
            : threaded("CSendStream", this), comm(_comm), myRank(_myRank), grpSize(_grpSize), mpTag(_mpTag), numStreams(_numStreams)
        {
            threaded.start();
        }
        ~CRecvServer()
        {
            threaded.join();
        }
        void join()
        {
            threaded.join();
        }
        void stop()
        {
            comm->cancel(RANK_ALL, mpTag);
            join();
        }
        virtual void threadmain() override
        {
            unsigned __int64 szRecvd = 0;
            std::vector<rank_t> endReplyTags;
            try
            {
                unsigned hash = (unsigned)mpTag;
                unsigned clients = grpSize;
                endReplyTags.resize(clients);
                CMessageBuffer msg;
                do
                {
                    rank_t sender;
                    while (!comm->recv(msg, RANK_ALL, mpTag, &sender, 60000))
                        PROGLOG("Waiting for data on %u", (unsigned)mpTag);
                    if (!msg.length())
                    {
                        /* each client sends a zero length buffer when done.
                         * When all received, receiver can stop, and replies to indicate finished.
                         *
                         */
                        endReplyTags[(unsigned)sender] = msg.getReplyTag();
                        --clients;
                        if (0 == clients)
                        {
                            for (unsigned r=0; r<endReplyTags.size(); r++)
                            {
                                if (!comm->send(msg, r, (mptag_t)endReplyTags[r]))
                                    throwUnexpected();
                            }
                            break;
                        }
                    }
                    else
                    {
                        szRecvd += msg.length();

                        // read 1st hash, then use to calculate and check incoming data.
                        msg.read(hash);
                        hash = checkData(msg, hash);

                        msg.clear();
                        msg.append(hash); // this should match what client calculated presend.
                        if (!comm->reply(msg))
                            throwUnexpected();
                    }
                }
                while (true);
            }
            catch (IException *e)
            {
                EXCLOG(e, "CRecvServer");
                e->Release();
            }
            PROGLOG("NxN:Receiver[tag=%u] szRecvd=%" I64F "u finished", (unsigned)mpTag, szRecvd);
        }
    };

    mptag_t mpTag = (mptag_t)0x20000;

    std::vector<Owned<CRecvServer>> receivers;
    std::vector<Owned<CSendStream>> senders;
    for (unsigned s=0; s<numStreams; s++)
    {
        receivers.push_back(new CRecvServer(comm, myRank, grpSize, mpTag, numStreams));
        senders.push_back(new CSendStream(comm, myRank, grpSize, mpTag, ((unsigned __int64)perStreamMBSize)*0x100000, msgSize, async));
        mpTag = (mptag_t)((unsigned)mpTag+1);
    }
    bool allSuccess = true;
    for (unsigned senderN=0; senderN<senders.size(); senderN++)
    {
        const auto &sender = senders[senderN];
        StringBuffer resultMsg;
        bool res = sender->waitResult(resultMsg);
        VStringBuffer logMsg("Stream[%u] from rank %u -> rank(s) [%s] result: ", senderN, (unsigned)myRank, sender->queryTgtRanks());
        if (res)
            logMsg.append("PASSED");
        else
        {
            logMsg.append("FAILED - ").append(resultMsg.str());
            allSuccess = false;
        }
        PROGLOG("%s", logMsg.str());
    }
    for (const auto &receiver: receivers)
    {
        if (allSuccess)
            receiver->join();
        else
            receiver->stop();
    }
}


// various test use some of these configurable parameters
static size32_t buffsize = 0;
static unsigned numiters = 0;
static unsigned numStreams = 0;
static unsigned perStreamMBSize = 0;
static bool async = false;

void runTest(const char *caption, const char *testname, IGroup* group, ICommunicator* comm)
{
    if (group->rank()==0)
    {
        printf("\n\n");
        PROGLOG("%s %s", caption, testname);
        PROGLOG("========================");
    }
    comm->barrier();
    if (strieq(testname, TEST_STREAM))
        StreamTest(group, comm);
    else if (strieq(testname, TEST_MULTI))
        MultiTest(comm);
    else if (strieq(testname, TEST_RING))
        MPRing(group, comm, numiters);
    else if (strieq(testname, TEST_AlltoAll))
        MPAlltoAll(group, comm, buffsize, numiters);
    else if (strieq(testname, TEST_RANK))
        MPTest2(group, comm);
    else if (strieq(testname, TEST_SELFSEND))
        MPSelfSend(comm);
    else if (strieq(testname, TEST_SINGLE_SEND))
        Test1(group, comm);
    else if (strieq(testname, TEST_RIGHT_SHIFT))
        MPRightShift(comm);
    else if (strieq(testname, TEST_RECV_FROM_ANY))
        MPReceiveFromAny(comm);
    else if (strieq(testname, TEST_SEND_TO_ALL))
        MPSendToAll(comm);
    else if (strieq(testname, TEST_MULTI_MT))
        MPMultiMTSendRecv(comm, numiters);
    else if (strieq(testname, TEST_NXN))
        MPNxN(comm, numStreams, perStreamMBSize, buffsize, async);
    else
        PROGLOG("MPTEST: Error, invalid testname specified (-t %s)", testname);
    comm->barrier();
}

bool createNodeList(IArrayOf<INode> &nodes, const char* hostfile, int my_port, rank_t max_ranks) {
    unsigned i = 1;
    char hoststr[256] = { "" };
    FILE* fp = fopen(hostfile, "r");
    if (fp == NULL)
    {
        PROGLOG("MPTest: Error, cannot open hostfile <%s>", hostfile);
        return false;
    }
    char line[256] = { "" };
    while (fgets(line, 255, fp) != NULL)
    {
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

void printHelp(char* executableName)
{
    printf("\nMPTEST: Usage: %s <myport> [-f <hostfile> [-t <testname> -b <buffsize> -i <iters> -r <rank> -n <numprocs> -d] | <ip:port> <ip:port>] [-mpi] [-mp]\n\n",executableName);
    std::vector<std::string> tests = { TEST_RANK, TEST_SELFSEND, TEST_MULTI,
            TEST_STREAM, TEST_RING, TEST_AlltoAll, TEST_SINGLE_SEND,
            TEST_RIGHT_SHIFT, TEST_RECV_FROM_ANY, TEST_SEND_TO_ALL,
            TEST_MULTI_MT, TEST_NXN };
    printf("\t <testname>");
    for (auto &testName: tests)
        printf("\t%s\n\t\t", testName.c_str());
    printf("\n");
}

int main(int argc, char* argv[])
{
    int mpi_debug = 0;
    const char * testname = "";
    rank_t max_ranks = 0;
    unsigned startupDelay = 0;

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
 *  MPNxN [-b <msgSiz>] [-s <numStreams>] [-m <perStreamMBSize>] [-a]
 *
 *  Options: (available with -f hostfile arg)
 *  --------
 *  -b buffsize (bytes) for MPAlltoAll and MPNxN tests
 *  -i iterations for MPRing and MPAlltoAll tests
 *  -n numprocs for when wanting to test a subset of ranks from hostfile/script
 *  -d for some additional debug output
 *  -s number of streams for MPNxN test
 *  -m total MB's to send per stream for MPNxN test
 *  -a async for NxN test
 */

    int argSize = argc;
    char** argL = argv;

    if (argSize<3)
    {
        printHelp(argv[0]);
        return 0;
    }

    try
    {
        EnableSEHtoExceptionMapping();
        StringBuffer lf;

        rank_t tot_ranks = 0;
        int my_port = atoi(argv[1]);
        char logfile[256] = { "" };
        sprintf(logfile,"mptest-%d.log",my_port);
        openLogFile(lf, logfile);

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
                        testname = argL[j+1];
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
                else if (streq(argv[j], "-s"))
                {
                    if ((j+1) < argc)
                    {
                        numStreams = atoi(argv[j+1]);
                        j++;
                    }
                }
                else if (streq(argv[j], "-m"))
                {
                    if ((j+1) < argc)
                    {
                        perStreamMBSize = atoi(argv[j+1]);
                        j++;
                    }
                }
                else if (streq(argv[j], "-a"))
                    async = true;
                else if (streq(argv[j], "-delay"))
                {
                    if ((j+1) < argc)
                    {
                        startupDelay = atoi(argv[j+1]);
                        j++;
                    }
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
                PROGLOG("MPTEST: adding node %u, port = <%s>", i-1, argL[i+1]);
                INode *newNode = createINode(argL[i+1], my_port);
                nodes.append(*newNode);
                i++;
            }
        }
        tot_ranks = nodes.length();

        if (startupDelay)
        {
            PROGLOG("Pausing for startupDelay = %u second(s)", startupDelay);
            MilliSleep(startupDelay * 1000);
            PROGLOG("Resuming");
        }

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
        PROGLOG("MPTEST: Starting, port = %d tot ranks = %u", my_port, tot_ranks);
        startMPServer(my_port);

        if (mpi_debug)
        {
            for (rank_t k=0;k<tot_ranks;k++)
            {
                StringBuffer urlStr;
                nodes.item(k).endpoint().getUrlStr(urlStr);
                PROGLOG("MPTEST: adding node %u, %s", k, urlStr.str());
            }
        }

        Owned<ICommunicator> comm = createCommunicator(group);
        runTest("MPTEST: Running MP Test:", testname, group, comm);

        stopMPServer();
    }
    catch (IException *e)
    {
        pexception("Exception",e);
        e->Release();
    }

    PROGLOG("MPTEST: bye");
    return 0;
}
