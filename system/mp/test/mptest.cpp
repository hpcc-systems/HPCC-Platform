

// CSocketSelectThread error 10038  

#include <platform.h>
#include <jlib.hpp>
#include <jthread.hpp>
#include <jmisc.hpp>
#include <jcrc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>

using namespace std;

#define MPPORT 8888

#define MULTITEST
//#define STREAMTEST
//#define MPITEST
//#define MPITEST2
//#define GPF

#ifdef MULTITEST
//#define MYMACHINES "10.150.10.16,10.150.10.17,10.150.10.18,10.150.10.19,10.150.10.20,10.150.10.21,10.150.10.22,10.150.10.23,10.150.10.47,10.150.10.48,10.150.10.49,10.150.10.50,10.150.10.51,10.150.10.52,10.150.10.53,10.150.10.54,10.150.10.55,10.150.10.73,10.150.10.75,10.150.10.79"
//#define MYMACHINES "192.168.16.124,10.150.10.17,10.150.10.18,10.150.10.19,10.150.10.20,10.150.10.21,10.150.10.22,10.150.10.23,10.150.10.47,10.150.10.48,10.150.10.49,10.150.10.50,10.150.10.51,10.150.10.52,10.150.10.53,10.150.10.54,10.150.10.55,10.150.10.73,10.150.10.75,10.150.10.79"
#endif

// #define aWhile 100000
#define aWhile 10



class CSectionTimer
{
    HiresTimer hrt[100];
    unsigned tids[100];

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
        for (i=0;i<99;i++) {
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
            PrintLog("TIME: %s(%d): max=%.6f, avg=%.6f, tot=%.6f",name,count,max,(double)total/count,total);
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
    void *bufs[18]; 
    unsigned bi;

    for (bi=0;bi<16;bi++) {
        bufs[bi] = malloc(1024*1024*100);
        assertex(bufs[bi]);
        memset(bufs[bi],bi,1024*1024*100);
    }

    CMessageBuffer mb;
    for (unsigned i=0;i<NITER;i++) {
        if (group->rank() == 1) {
            mb.clear();
            StringBuffer header;
            header.append("Test Block #").append(i);
            mb.append(header.str()).reserve(BLOCKSIZE-mb.length());
            PrintLog("Sending '%s' length %d",header.str(),mb.length());
            {
                TimedBlock block(STsend);
                comm->send(mb,0,MPTAG_TEST,MP_ASYNC_SEND);
            }
            PrintLog("Sent");
            //Sleep(WRITEDELAY);
        }
        else if (group->rank() == 0) {
            rank_t r;
            comm->recv(mb,RANK_ALL,MPTAG_TEST,&r);
            StringAttr str;
            PrintLog("Receiving");
            {
                TimedBlock block(STrecv);
                mb.read(str);
            }
            PrintLog("Received(%d) '%s' length %d",r,str.get(),mb.length());
            //if (i==0)
            //  Sleep(1000*1000); // 15 mins or so
            //Sleep(READDELAY);
        }
        else
            PrintLog("Skipping extra rank %d", group->rank());
    }

    comm->barrier();

    for (bi=0;bi<16;bi++) 
        free(bufs[bi]);

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
        PrintLog("(1) Received '%s' from rank %d",str.get(),r);
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
        PrintLog("(2) Received '%s' from rank %d",str.get(),r);
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
        PrintLog("(3) Received '%s' from rank %d",str.get(),r);
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
        PrintLog("(4) Received '%s' from rank %d",str.get(),r);
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
        PrintLog("(5) Received '%s' from rank %d (unknown)",str.get(),r);
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
        PrintLog("Received '%s' from rank %d",str.get(),r);
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
            PrintLog("size = %d",size);
            char c = buffer[0];
            for (unsigned i=1;i<size;i++) {
                c += (c*16);
                c += 113;
                if (buffer[i] != c) {
                    PrintLog("Failed at %d, expected %02x found %02x %02x %02x %02x %02x %02x %02x %02x",i,(int)(byte)c,(int)(byte)buffer[i],(int)(byte)buffer[i+1],(int)(byte)buffer[i+2],(int)(byte)buffer[i+3],(int)(byte)buffer[i+4],(int)(byte)buffer[i+5],(int)(byte)buffer[i+6],(int)(byte)buffer[i+7]);
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
            PrintLog("MPTEST: started server");
            try {
                while(n--) {
                    mb.clear();
                    rank_t rr;
#if 1
                    if (!comm->recv(mb,RANK_ALL,MPTAG_TEST,&rr)) 
                        break;
#else
                    comm->recv(mb,RANK_ALL,MPTAG_TEST,&rr);
#endif
                    PrintLog("MPTEST: Received from %d, len = %d",rr, mb.length());
                    StringBuffer str;
                    comm->queryGroup().queryNode(rr).endpoint().getUrlStr(str);
                    // PrintLog("MPTEST: Received from %s",str.str());

                    buff->deserialize(mb);

#if 0
                    if (!buff->check())
                        PrintLog("MPTEST: Received from %s",str.str());
#endif

                    mb.clear().append(buff->crc);
                    comm->reply(mb);
                }
            }
            catch (IException *e) {
                pexception("Server Exception",e);
            }

            comm->barrier();  // MCK

            PrintLog("MPTEST: stopped server");
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

    PrintLog("MPTEST: client started");

    try {
        while (n--) {
            buff->fill();
            buff->serialize(mb.clear());

#if 0
            StringBuffer str;
            comm->queryGroup().queryNode(targets[n]).endpoint().getUrlStr(str);
            PrintLog("MPTEST: Sending to %s, length=%u",str.str(), mb.length());
#endif

            PrintLog("MPTEST: Sending to %d, length=%u", targets[n], mb.length());

#if 1
            if (!comm->sendRecv(mb,targets[n],MPTAG_TEST)) 
                break;
#else
            comm->sendRecv(mb,targets[n],MPTAG_TEST);
#endif
            // Sleep((n+1)*2000);
            // PrintLog("MPTEST: Sent to %s",str.str());
            unsigned crc;
            mb.read(crc);
            assertex(crc==buff->crc);
        }
    }
    catch (IException *e) {
        pexception("Client Exception",e);
    }

    PrintLog("MPTEST: client finished");

    server.join();

    delete [] targets;
    delete buff;
}

void MPITest(IGroup *group, ICommunicator *mpicomm)
{
    CMessageBuffer mb;
    CMessageBuffer mb2;
    int myrank = group->rank();
    int numranks = group->ordinality();

    int rnksumtotal = 0;
    for(int i=0;i<numranks;i++)
        rnksumtotal += (i+1);

    PrintLog("MPTEST: MPITest myrank=%d numranks=%d rnksumtotal=%d", myrank, numranks, rnksumtotal);

    // send and recv to/from all others without a send/recv deadlock ...

    mb.clear();
    mb.append(myrank+1);

    rank_t r;
    int rankval;
    int ranksum = myrank+1;

    int left, right;

    if (numranks == 2)
    {
        if (myrank == 0) {
            left = 1;
            right = 1;
            PrintLog("MPTEST: MPITest: %d send to rank %d", myrank, right);
            mpicomm->send(mb,right,MPTAG_TEST);

            mb2.clear();
            PrintLog("MPTEST: MPITest: %d recv from rank %d", myrank, left);
            mpicomm->recv(mb2,left,MPTAG_TEST,&r);
            mb2.read(rankval);
            ranksum += rankval;
        }else{
            left = 0;
            right = 0;
            mb2.clear();
            PrintLog("MPTEST: MPITest: %d recv from rank %d", myrank, left);
            mpicomm->recv(mb2,left,MPTAG_TEST,&r);
            mb2.read(rankval);
            ranksum += rankval;

            PrintLog("MPTEST: MPITest: %d send to rank %d", myrank, right);
            mpicomm->send(mb,right,MPTAG_TEST);
        }
    }
    else if (numranks > 2)
    {
        int m = 0;
        while (m < (numranks - 1)) {

            int rankid = 0;
            while (rankid < numranks) {

                left = rankid - 1 - m;
                if (left < 0)
                    left = numranks + left;
                right = rankid + 1 + m;
                if (right >= numranks)
                    right = right % numranks;

                if (rankid == myrank) {

                    if (rankid == 0) {
                        PrintLog("MPTEST: MPITest: %d send to rank %d", myrank, right);
                        mpicomm->send(mb,right,MPTAG_TEST);
                        mb2.clear();
                        PrintLog("MPTEST: MPITest: %d recv from rank %d", myrank, left);
                        mpicomm->recv(mb2,left,MPTAG_TEST,&r);
                        mb2.read(rankval);
                        ranksum += rankval;
                    } else {
                        mb2.clear();
                        PrintLog("MPTEST: MPITest: %d recv from rank %d", myrank, left);
                        mpicomm->recv(mb2,left,MPTAG_TEST,&r);
                        mb2.read(rankval);
                        ranksum += rankval;
                        PrintLog("MPTEST: MPITest: %d send to rank %d", myrank, right);
                        mpicomm->send(mb,right,MPTAG_TEST);
                    }

                }

                rankid++;
            }

            m++;
        }
    }

    PrintLog("MPTEST: MPITest: ranksum = %d", ranksum);

    assertex(rnksumtotal==ranksum);

    mpicomm->barrier();

    return;
}

void MPITest2(IGroup *group, ICommunicator *mpicomm)
{
    int myrank = group->rank();
    int numranks = group->ordinality();

    PrintLog("MPTEST: MPITest2: myrank=%d numranks=%d", myrank, numranks);

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

int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

//  startMPServer(9123);
//  testIPnodeHash();
//  stopMPServer();
//  return 0;

#if 1

#ifndef MYMACHINES
    if (argc<3) {
        printf("\nMPTEST: Usage: %s <myport> <ip:port> <ip:port> ...\n\n", argv[0]);
        return 0;
    }
#endif

    try {
        EnableSEHtoExceptionMapping();
        StringBuffer lf;
        openLogFile(lf, "mptest.log");
        // PrintLog("MPTEST Starting");

#ifndef MYMACHINES
#if 1
        int num_nodes = 0;
        int my_port = atoi(argv[1]);

        PrintLog("MPTEST: Starting %d", my_port);

        startMPServer(my_port);

        INode *nodes[1000];

        int i = 1;
        while (i+1 < argc && i-1 < 1000) {
            PrintLog("MPTEST: adding node %d, port = <%s>", i-1, argv[i+1]);
            nodes[i-1] = createINode(argv[i+1], my_port);
            i++;
        }

        PrintLog("MPTEST: num_nodes = %d", i-1);

        IGroup *group = createIGroup(i-1,nodes);
#else
        printf("argc = %d\n", argc);
        // int my_port = atoi(argv[3]);
        int my_port = (argc==3)?MPPORT:atoi(argv[3]);
        printf("my_port = %d\n", my_port);
        startMPServer(my_port);
        // startMPServer((argc==3)?MPPORT:atoi(argv[3]));
        INode *nodes[2];
        nodes[0] = createINode(argv[1], my_port);
        printf("adding node %d, port = <%s> %d\n", 0, argv[1], my_port);
        nodes[1] = createINode(argv[2], my_port);
        printf("adding node %d, port = <%s> %d\n", 1, argv[2], my_port);

        IGroup *group = createIGroup(2,nodes);
#endif
#else
        startMPServer(MPPORT);
        IGroup *group = createIGroup(MYMACHINES,MPPORT); 
#endif


#if 1 // --------

#ifdef STREAMTEST

        ICommunicator * mpicomm = createCommunicator(group);
        StreamTest(group,mpicomm);
        mpicomm->Release();

#else
# ifdef MULTITEST

        ICommunicator * mpicomm = createCommunicator(group);
        MultiTest(mpicomm);
        mpicomm->Release();

# else
#  ifdef MPITEST

        ICommunicator * mpicomm = createCommunicator(group);
        MPITest(group, mpicomm);
        mpicomm->Release();

#  else
#   ifdef MPITEST2

        ICommunicator * mpicomm = createCommunicator(group);
        MPITest2(group, mpicomm);
        mpicomm->Release();

#   else

        ICommunicator * comm = createCommunicator(group);
        for (unsigned i = 0;i<1;i++) {
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
        comm->Release();

#   endif
#  endif
# endif
#endif

#endif // --------------

        group->Release();

#ifndef MYMACHINES
        for (int i=0;i<num_nodes;i++)
            nodes[i]->Release();
#endif

        stopMPServer();
    }
    catch (IException *e) {
        pexception("Exception",e);
    }
#endif

    return 0;
}
