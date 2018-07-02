

// CSocketSelectThread error 10038  

#include <platform.h>
#include <jlib.hpp>
#include <jthread.hpp>
#include <jmisc.hpp>
#include <jcrc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>
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
            //if (i==0)
            //  Sleep(1000*1000); // 15 mins or so
            //Sleep(READDELAY);
        }
        else
        {
            PrintLog("MPTEST: StreamTest skipping extra rank %u", group->rank());
            break;
        }
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
    int mpi_debug = 0;
    char testname[256] = { "" };
    size32_t buffsize = 0;
    unsigned numiters = 0;
    rank_t max_ranks = 0;
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
//    bool withMPI = false;

    if ((argSize>1) && (strcmp(argL[1], "--with-mpi")==0)){
        argSize--;
        argL++;
//        withMPI = true;
    }

#ifndef MYMACHINES
    if (argSize<3) {
        printf("\nMPTEST: Usage: %s [--with-mpi] <myport> [-f <hostfile> [-t <testname> -b <buffsize> -i <iters> -n <numprocs> -d] | <ip:port> <ip:port>]\n\n", argv[0]);
        return 0;
    }
#endif

    try {
        EnableSEHtoExceptionMapping();
        StringBuffer lf;
        // PrintLog("MPTEST Starting");

#ifndef MYMACHINES
        rank_t tot_ranks = 0;
        int my_port = atoi(argL[1]);
        char logfile[256] = { "" };
        sprintf(logfile,"mptest-%d.log",my_port);
        // openLogFile(lf, logfile);

        IArrayOf<INode> nodes;

        const char * hostfile = nullptr;
        if (argSize > 3)
        {
            if (strcmp(argL[2], "-f") == 0)
                hostfile = argL[3];
        }
        unsigned i = 1;
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
                j++;
            }
            char hoststr[256] = { "" };
            FILE *fp = fopen(hostfile, "r");
            if (fp == NULL)
            {
                PrintLog("MPTest: Error, cannot open hostfile <%s>", hostfile);
                return 1;
            }
            char line[256] = { "" };
            while(fgets(line, 255, fp) != NULL)
            {
                if ( (max_ranks > 0) && ((i-1) >= max_ranks) )
                    break;
                int srtn = sscanf(line,"%s",hoststr);
                if (srtn == 1 && line[0] != '#')
                {
                    INode *newNode = createINode(hoststr, my_port);
                    nodes.append(*newNode);
                    i++;
                }
            }
            fclose(fp);
        }
        else
        {
            while (i+1 < argSize)
            {
                PrintLog("MPTEST: adding node %u, port = <%s>", i-1, argL[i+1]);
                INode *newNode = createINode(argL[i+1], my_port);
                nodes.append(*newNode);
                i++;
            }
        }
        tot_ranks = i-1;

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

        Owned<ICommunicator> mpicomm = createCommunicator(group);

#ifdef STREAMTEST
        StreamTest(group,mpicomm);
#else
# ifdef MULTITEST
        MultiTest(mpicomm);
# else
#  ifdef MPRING
        MPRing(group, mpicomm, numiters);
#  else
#   ifdef MPALLTOALL
        MPAlltoAll(group, mpicomm, buffsize, numiters);
#   else
#    ifdef MPTEST2
        MPTest2(group, mpicomm);
#    else
#     ifdef DYNAMIC_TEST
        _T("hostfile="<<hostfile<<" port="<<my_port<<" testname="<<testname<<" tot_ranks="<<tot_ranks);
        if (strnicmp(testname, "Stream", 6)==0)
            StreamTest(group, mpicomm);
        else if (strnicmp(testname, "Multi", 5)==0)
            MultiTest(mpicomm);
        else if ( strieq(testname, "MPRing") || strieq(testname, "Ring") )
            MPRing(group, mpicomm, numiters);
        else if ( strieq(testname, "MPAlltoAll") || strieq(testname, "AlltoAll") )
            MPAlltoAll(group, mpicomm, buffsize, numiters);
        else if ( strieq(testname, "MPTest2") || strieq(testname, "Test2") )
            MPTest2(group, mpicomm);
        else if ((int)strlen(testname) > 0)
            PrintLog("MPTEST: Error, invalid testname specified (-t %s)", testname);
        else  // default is MPRing ...
            MPRing(group, mpicomm, numiters);
#     else
        for (unsigned i = 0;i<1;i++)
        {
            Test1(group,mpicomm);
            PrintLog("MPTEST: test1 done, waiting"); Sleep(aWhile);
            Test2(group,mpicomm);
            PrintLog("MPTEST: test2 done, waiting"); Sleep(aWhile);
            Test3(group,mpicomm);
            PrintLog("MPTEST: test3 done, waiting"); Sleep(aWhile);
            Test4(group,mpicomm);
            PrintLog("MPTEST: test4 done, waiting"); Sleep(aWhile);
            Test5(group,mpicomm);
            PrintLog("MPTEST: test5 done, waiting"); Sleep(aWhile);
            Test6(group,mpicomm);
            PrintLog("MPTEST: test6 done, waiting"); Sleep(aWhile);
            Test7(group,mpicomm);
            PrintLog("MPTEST: test7 done, waiting"); Sleep(aWhile);
        }
#     endif
#    endif
#   endif
#  endif
# endif
#endif

        stopMPServer();
    }
    catch (IException *e) {
        pexception("Exception",e);
    }

    PrintLog("MPTEST: bye");
    return 0;
}
