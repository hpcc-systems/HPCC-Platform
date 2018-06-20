

// CSocketSelectThread error 10038  

#include <jmisc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>
#include <jlib.hpp>

using namespace std;

#define MULTITEST

rank_t myrank;

//class ThreadTest: public implements IThreaded{
//protected:
//	ICommunicator* comm;
//public:
//	void setComm(ICommunicator* _comm){
//		comm = _comm;
//	}
//};

void printHelp(int argc, char** argv){
    if (myrank == 0){
        printf("USAGE \n\t$ mpirun -np <# of procs> %s", argv[0]);
        std::string desc="\n\nDESCRIPTION\n\t";
    #ifdef RANK_TEST
        printf("%sPrint rank of each node (# of procs > 1).", desc.c_str()); 
    #elif SINGLE_SEND_TEST
        printf("%sSend message from node 0 to node 1 (# of procs >= 2).", desc.c_str()); 
    #elif RIGHT_SHIFT_TEST
        printf("%sSend data to the node represented by next rank.", desc.c_str()); 
    #elif RECEIVE_FROM_ANY_TEST    
        printf(" [node_rank]%sLast node/processpor receive data from any node_rank (default=0).", desc.c_str()); 
    #elif CUSTOM_SEND_TEST
        printf(" <routing_file>%sSend/Receive data based on custom routing.", desc.c_str()); 
    #elif SEND_ONE_TO_ALL_TEST
        printf(" [node_rank]%sNode node_rank (default=0) send to all nodes.", desc.c_str()); 
    #elif RECEIVE_ONE_FROM_ALL_TEST
        printf(" [node_rank]%sNode node_rank (default=0) receive from all nodes.", desc.c_str()); 
    #endif
        printf("\n\n");
    }
}

//--- Rank Test --//
void TEST_rank(ICommunicator* comm){
    IGroup *group = comm->getGroup();
    assertex(group->rank() >= 0);
    assertex(group->ordinality() > 1); //at least 2 processors
    assertex(group->rank() < group->ordinality());
    PrintLog("Hello from %d. Total of %d nodes.",group->rank(), group->ordinality());
}

void TEST_single_send(ICommunicator* comm){
    IGroup* group = comm->getGroup();
    int expected_msg = 42;
    int received_msg;
    CMessageBuffer testMsg;
    assertex(group->ordinality() > 1);
    
    if (group->rank() == 0){
        rank_t target = 1;
        testMsg.append(expected_msg);
        comm->send(testMsg, target, MPTAG_TEST, MP_WAIT_FOREVER);
    }
    if (group->rank() == 1){
        rank_t source = 0;
        comm->recv(testMsg, source, MPTAG_TEST, NULL, MP_WAIT_FOREVER);
        testMsg.read(received_msg);
        assertex(expected_msg == received_msg);
        PrintLog("Message sent from node 0 to 1.");
    }
}

void TEST_right_shift(ICommunicator* comm){
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

void TEST_receive_from_any(ICommunicator* comm, rank_t nodeRank){
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    rank_t destinationRank = (p-1);
    double expectedValue = 1234.0;
    if (rank == nodeRank){
        CMessageBuffer sendMsg;
        sendMsg.append(expectedValue); 
        comm->send(sendMsg, destinationRank, MPTAG_TEST);
        PrintLog("Message sent by node %d to node %d.", rank, destinationRank);
    }
    if (rank == destinationRank){
        CMessageBuffer recvMsg;
        comm->recv(recvMsg, RANK_ALL, MPTAG_TEST);
        double receivedValue;
        recvMsg.read(receivedValue);
        assertex(nodeRank == comm->getGroup()->rank(recvMsg.getSender()));
        assertex(expectedValue == receivedValue);
        PrintLog("Message successfully received from node %d to node %d.", comm->getGroup()->rank(recvMsg.getSender()), rank);
    }
}

void TEST_one_to_all(ICommunicator* comm, rank_t nodeRank){
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    double expectedValue = 1234.0;
    double receivedValue;
    if (rank == nodeRank){
        CMessageBuffer sendMsg;
        sendMsg.append(expectedValue);        
        comm->send(sendMsg, RANK_ALL, MPTAG_TEST, MP_WAIT_FOREVER);
    }
    
    CMessageBuffer recvMsg;
    comm->recv(recvMsg, nodeRank, MPTAG_TEST, NULL, MP_WAIT_FOREVER);
    recvMsg.read(receivedValue);
    assertex(expectedValue == receivedValue);
    PrintLog("Message received from node %d to node %d.", nodeRank, rank);
}

void TEST_one_from_all(ICommunicator* comm, rank_t nodeRank){
    IGroup* group = comm->getGroup();
    rank_t p = group->ordinality();
    rank_t rank = group->rank();
    
    double baseValue = 1234.0;
    CMessageBuffer sendMsg;
    double expectedValue = baseValue * rank;
    sendMsg.append(expectedValue); 
    comm->send(sendMsg, nodeRank, MPTAG_TEST, MP_WAIT_FOREVER);    
    
    if (rank == nodeRank){
        for(int i=0; i<p; i++){
            CMessageBuffer recvMsg;
            comm->recv(recvMsg, i, MPTAG_TEST, NULL, MP_WAIT_FOREVER);
            double receivedValue;
            recvMsg.read(receivedValue);
            expectedValue = baseValue * i;
            assertex(expectedValue == receivedValue);
            PrintLog("Message received from node %d to node %d.", i, rank);
        }
    }
}

void TEST_MT_right_shift(ICommunicator* comm){
//    class: public ThreadTest{
//    public:
//    	void main(){
//    		IGroup *group = comm->getGroup();
//    		rank_t p = group->ordinality();
//			rank_t rank = group->rank();
//			rank_t destination_rank = (rank + 1) % p;
//
//			CMessageBuffer sendMsg;
//			sendMsg.append(rank);
//			comm->send(sendMsg, destination_rank, MPTAG_TEST, MP_WAIT_FOREVER);
//    	}
//    } s;
//    class: public ThreadTest{
//    public:
//    	void main(){
//    		IGroup *group = comm->getGroup();
//    		rank_t p = group->ordinality();
//			rank_t rank = group->rank();
//			rank_t source_rank = (rank - 1 + p) % p;
//
//			CMessageBuffer recvMsg;
//			int received_msg;
//			comm->recv(recvMsg, source_rank, MPTAG_TEST, NULL, MP_WAIT_FOREVER);
//			recvMsg.read(received_msg);
//			assertex(source_rank == received_msg);
//			PrintLog("Message received from node %d to node %d.", source_rank, rank);
//    	}
//    } r;
//    s.setComm(comm);
//    r.setComm(comm);
//
//    CThreaded sendThread = new CThreaded("Send Thread", s);
//    CThreaded recvThread = new CThreaded("Receive Thread", r);
//    sendThread.start();
//    recvThread.start();
//    recvThread.join();
}

int main(int argc, char* argv[]){
    InitModuleObjects();
    try {
        EnableSEHtoExceptionMapping();
        startMPServer(0);
        IGroup* group = createIGroup(0, (INode **) NULL);
        ICommunicator* comm = createCommunicator(group);
        myrank = group->rank();
        if ((argc == 2) && (strcmp(argv[1], "--help") == 0)){
            printHelp(argc, argv);
            stopMPServer();
            return 0;
        }        
#ifdef RANK_TEST
        if (argc < 2)
            TEST_rank(comm);
        else
            printHelp(argc, argv);
#elif SINGLE_SEND_TEST        
        if (argc < 2)
            TEST_single_send(comm);
        else
            printHelp(argc, argv);    
#elif RIGHT_SHIFT_TEST
        if (argc < 2)
            TEST_right_shift(comm);
        else
            printHelp(argc, argv);            
#elif CUSTOM_SEND_TEST
#elif RECEIVE_FROM_ANY_TEST
        if (argc < 3){
            int rank = (argc == 2)? atoi(argv[1]) : 0;
            TEST_receive_from_any(comm, rank);
        }else
            printHelp(argc, argv);  
#elif SEND_ONE_TO_ALL_TEST
        if (argc < 3){
            int rank = (argc == 2)? atoi(argv[1]) : 0;
            TEST_one_to_all(comm, rank);
        }else
            printHelp(argc, argv);     
#elif RECEIVE_ONE_FROM_ALL_TEST
        if (argc < 3){
            int rank = (argc == 2)? atoi(argv[1]) : 0;
            TEST_one_from_all(comm, rank);
        }else
            printHelp(argc, argv);      
#endif    
        stopMPServer();
    } catch (IException *e){
        pexception("Exception", e);
        printHelp(argc, argv);
        stopMPServer();
    }
    return 0;
}
