

// CSocketSelectThread error 10038  

#include <jmisc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include <jthread.hpp>
#include <vector>
#include <jexcept.hpp>
#include "mplog.hpp"

using namespace std;

#define RANK_TEST 0
#define SINGLE_SEND_TEST 1
#define RIGHT_SHIFT_TEST 2
#define CUSTOM_SEND_TEST 3
#define RECEIVE_FROM_ANY_TEST 4
#define SEND_ONE_TO_ALL_TEST 5
#define RECEIVE_ONE_FROM_ALL_TEST 6
#define MT_SIMPLE_SEND_RECV 7
#define MT_SEND_RECV 8
#define RING_TEST 9
#define MT_ALLTOALL_TEST 10

rank_t myrank;

void printHelp(int argc, char** argv)
{
    if (myrank == 0)
    {
        printf("USAGE \n\t$ mpirun -np <# of procs> %s -t <test-type> <para_1...para_n>\n", argv[0]);
        printf("\n");
        printf("-t <test-type>\t Select the type of test to run. Avaialable tests are the following:\n");
        printf("\t\t %d - Print rank of each node (# of procs > 1).\n",RANK_TEST);
        printf("\t\t %d - Send message from node 0 to node 1 (# of procs >= 2).\n",SINGLE_SEND_TEST);
        printf("\t\t %d - Send data to the node represented by next rank.\n",RIGHT_SHIFT_TEST);
        printf("\t\t %d - Send/Receive data based on custom routing.\n",CUSTOM_SEND_TEST);
        printf("\t\t\t\t    parameters: <routing_file> \n");
        printf("\t\t %d - Last node/processpor receive data from any node_rank (default=0).\n",RECEIVE_FROM_ANY_TEST);
        printf("\t\t\t\t    parameters: [node_rank]\n");
        printf("\t\t %d - Node node_rank (default=0) send to all nodes.\n",SEND_ONE_TO_ALL_TEST);
        printf("\t\t\t\t    parameters: [node_rank]\n");
        printf("\t\t %d - Node node_rank (default=0) receive from all nodes.\n",RECEIVE_ONE_FROM_ALL_TEST);
        printf("\t\t\t\t    parameters: [node_rank]\n");
        printf("\t\t %d - Multi-threaded simple send and receive\n",MT_SIMPLE_SEND_RECV);
        printf("\t\t %d - Multi-threaded competing send and receive\n",MT_SEND_RECV);
        printf("\t\t %d - Ring skip test\n",RING_TEST);
        printf("\t\t %d - Multi-threaded All to all test\n",MT_ALLTOALL_TEST);
        printf("\n<para_i>\t Test parameters for individual test\n");
        printf("\n\n");
    }
}

//--- Rank Test --//
void TEST_rank(ICommunicator* comm)
{
    IGroup *group = comm->getGroup();
    assertex(group->rank() >= 0);
    assertex(group->ordinality() > 1); //at least 2 processors
    assertex(group->rank() < group->ordinality());
    PrintLog("Hello from %d. Total of %d nodes.",group->rank(), group->ordinality());
}

void TEST_single_send(ICommunicator* comm)
{
    _TF("TEST_single_send");
    IGroup* group = comm->getGroup();
    int expected_msg = 42;
    int received_msg;
    CMessageBuffer testMsg;
    assertex(group->ordinality() > 1);
    
    if (group->rank() == 0)
    {
        rank_t target = 1;
        testMsg.append(expected_msg);
        comm->send(testMsg, target, MPTAG_TEST);
    }
    if (group->rank() == 1)
    {
        rank_t source = 0;
        bool success = comm->recv(testMsg, source, MPTAG_TEST, NULL);
        assertex(success == true);
        testMsg.read(received_msg);
        assertex(expected_msg == received_msg);
        PrintLog("Message sent from node 0 to 1.");
    }
}

void TEST_right_shift(ICommunicator* comm)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t source_rank = (rank - 1 + p) % p;
    rank_t destination_rank = (rank + 1) % p;
    
    CMessageBuffer sendMsg;
    sendMsg.append(rank);
    comm->send(sendMsg, destination_rank, MPTAG_TEST, MP_WAIT_FOREVER);
    
    CMessageBuffer recvMsg;
    int received_msg;
    comm->recv(recvMsg, source_rank, MPTAG_TEST, NULL, MP_WAIT_FOREVER);
    recvMsg.read(received_msg);
    assertex(source_rank == received_msg);
    PrintLog("Message received from node %d to node %d.", source_rank, rank);
}

void TEST_receive_from_any(ICommunicator* comm, rank_t nodeRank)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t destinationRank = (p-1);
    double expectedValue = 1234.0;
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
        comm->recv(recvMsg, RANK_ALL, MPTAG_TEST);
        double receivedValue;
        recvMsg.read(receivedValue);
        assertex(nodeRank == comm->getGroup()->rank(recvMsg.getSender()));
        assertex(expectedValue == receivedValue);
        PrintLog("Message successfully received from node %d to node %d.", comm->getGroup()->rank(recvMsg.getSender()), rank);
    }
}

void TEST_one_to_all(ICommunicator* comm, rank_t nodeRank)
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

void TEST_one_from_all(ICommunicator* comm, rank_t nodeRank)
{
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    
    double baseValue = 1234.0;
    CMessageBuffer sendMsg;
    double expectedValue = baseValue * rank;
    sendMsg.append(expectedValue); 
    comm->send(sendMsg, nodeRank, MPTAG_TEST);
    
    if (rank == nodeRank)
    {
        for(int i=0; i<p; i++)
        {
            CMessageBuffer recvMsg;
            comm->recv(recvMsg, i, MPTAG_TEST, NULL);
            double receivedValue;
            recvMsg.read(receivedValue);
            expectedValue = baseValue * i;
            assertex(expectedValue == receivedValue);
            PrintLog("Message received from node %d to node %d.", i, rank);
        }
    }
}


CriticalSection sendCriticalSec;
CriticalSection recvCriticalSec;
CriticalSection validateCriticalSec;
int getNextCount(CriticalSection &sect, int &count)
{
    CriticalBlock block(sect);
    if (count)
        return count--;
    else
        return 0;
}

void TEST_MT_simple_send_recv(ICommunicator* comm)
{
    assertex(comm->getGroup()->ordinality()>1);
    rank_t rank = comm->getGroup()->rank();
    // nodes ranked 0 and 1 will be conducting this test
    if (rank<2)
    {
        class SWorker: public Thread
        {
        private:
            ICommunicator* comm;
        public:
            SWorker(ICommunicator* _comm):comm(_comm){}
            int run()
            {
                IGroup *group = comm->getGroup();
                rank_t p = group->ordinality();
                rank_t rank = group->rank();
                rank_t destination_rank = 1 - rank;

                CMessageBuffer sendMsg;
                int msg = (rank + 1)*10;
                sendMsg.append(msg);
                comm->send(sendMsg, destination_rank, MPTAG_TEST);
                PrintLog("Message sent from %d", rank);
                return 1;
            }
        } s(comm);
        class RWorker: public Thread
        {
        private:
            ICommunicator* comm;
        public:
            RWorker(ICommunicator* _comm):comm(_comm){}
            int run()
            {
                IGroup *group = comm->getGroup();
                rank_t p = group->ordinality();
                rank_t rank = group->rank();
                rank_t source_rank = 1 - rank;

                CMessageBuffer recvMsg;
                if (comm->recv(recvMsg, source_rank, MPTAG_TEST, NULL, 100))
                {
                    int received_msg;
                    recvMsg.read(received_msg);
                    //TODO validate received messge
                    PrintLog("Message %d received to %d", received_msg, rank);
                }
                return 1;
            }
        }r(comm);
        s.start(); r.start();
        s.join(); r.join();
    }
    comm->barrier();
}
bool* validate;

//validate that numbers from 1 to maxCounter are received only once
void setValidate(int i, int maxCounter)
{
    CriticalBlock block(validateCriticalSec);
    assertex(i>0);
    assertex(i<=maxCounter);
    assertex(validate[i-1] == false);
    validate[i-1] = true;
}

void TEST_MT_send_recv(ICommunicator* comm, int counter)
{
    assertex(comm->getGroup()->ordinality()>1);
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


void run_tests(ICommunicator* comm, int type, int paramCount, char* parameter[])
{
    _T("Selected test "<<type);
    switch (type)
    {
        case RANK_TEST:
        {
            TEST_rank(comm);
            break;
        }
        case SINGLE_SEND_TEST:
        {
            TEST_single_send(comm);
            break;
        }
        case RIGHT_SHIFT_TEST:
        {
            TEST_right_shift(comm);
            break;
        }
        case CUSTOM_SEND_TEST:
        {
            break;
        }
        case RECEIVE_FROM_ANY_TEST:
        {
            int rank = (paramCount == 1) ? atoi(parameter[0]) : 0;
            TEST_receive_from_any(comm, rank);
            break;
        }
        case SEND_ONE_TO_ALL_TEST:
        {
            int rank = (paramCount == 1) ? atoi(parameter[0]) : 0;
            TEST_one_to_all(comm, rank);
            break;
        }
        case RECEIVE_ONE_FROM_ALL_TEST:
        {
            int rank = (paramCount == 1) ? atoi(parameter[0]) : 0;
            TEST_one_from_all(comm, rank);
            break;
        }
        case MT_SEND_RECV:
        {
            int counter = (paramCount == 1) ? atoi(parameter[0]) : 10;
            TEST_MT_send_recv(comm, counter);
            break;
        }
        case MT_SIMPLE_SEND_RECV:
        {
            TEST_MT_simple_send_recv(comm);
            break;
        }
        case RING_TEST:
        {
            MPRing(comm->getGroup(), comm);
            break;
        }
        case MT_ALLTOALL_TEST:
        {
            MPAlltoAll(comm->getGroup(), comm);
            break;
        }
        default:
        {
            //TODO run all tests?
        }
    }
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    try
    {
        EnableSEHtoExceptionMapping();
        startMPServer(0);
        ICommunicator* comm = createCommunicator(NULL);
        myrank = comm->getGroup()->rank();
        int type;
        if ((argc > 2) && strcmp(argv[1], "-t")==0)
        {
            type = strcmp(argv[2], "all")==0? -1:atoi(argv[2]);
        } else
        {
            throw makeStringException(0, "Invalid commandline parameters.");
        }
        _T("Starting tests");
        run_tests(comm, type, argc-3, &argv[3]);
        stopMPServer();
    } catch (IException *e)
    {
        stopMPServer();
        if ((argc != 2) || strcmp(argv[1], "--help"))
            pexception("Exception", e);
        printHelp(argc, argv);
    }
    return 0;
}
