#include <mpcomm.hpp>
#include <mpbase.hpp>
#include <mplog.hpp>
#include <jmisc.hpp>
#include <vector>
#include <string>

#define TEST_SINGLE_SEND "SingleSend"
#define TEST_RIGHT_SHIFT "RightShift"
#define TEST_RECV_FROM_ANY "RecvFromAny"
#define TEST_SEND_TO_ALL "SendToAll"
#define TEST_SINGLE_MT "MTSingleSendRecv"
#define TEST_MULTI_MT "MTMultiSendRecv"

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

void MPSingleSend(ICommunicator* comm)
{
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

void MPSingleMTSendRecv(ICommunicator* comm)
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
                    //TODO validate received message
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

void MPMultiMTSendRecv(ICommunicator* comm, int counter)
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

bool runAdditionalTests(char *testname, ICommunicator* comm, int iter, int bufferSize, rank_t rank)
{
    iter = (iter == 0? 100: iter);
    if ( strieq(testname, TEST_SINGLE_SEND) )
        MPSingleSend(comm);
    else if ( strieq(testname, TEST_RIGHT_SHIFT) )
        MPRightShift(comm);
    else if ( strieq(testname, TEST_RECV_FROM_ANY) )
        MPReceiveFromAny(comm, rank);
    else if ( strieq(testname, TEST_SEND_TO_ALL) )
        MPSendToAll(comm, rank);
    else if ( strieq(testname, TEST_SINGLE_MT) )
        MPSingleMTSendRecv(comm);
    else if ( strieq(testname, TEST_MULTI_MT) )
        MPMultiMTSendRecv(comm, iter);
    else
        return false;
    return true;
}

void appendAdditionalTests(std::vector<std::string> &testnames)
{
    std::vector<std::string> additionaltest={TEST_SINGLE_SEND, TEST_RIGHT_SHIFT, TEST_RECV_FROM_ANY, TEST_SEND_TO_ALL , TEST_SINGLE_MT, TEST_MULTI_MT};
    testnames.insert(testnames.end(),additionaltest.begin(), additionaltest.end());
}

