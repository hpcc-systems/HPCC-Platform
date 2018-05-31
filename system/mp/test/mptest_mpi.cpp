

// CSocketSelectThread error 10038  

#include <jmisc.hpp>
#include <mpbase.hpp>
#include <mpcomm.hpp>

using namespace std;

#define MULTITEST

void printHelp(int argc, char** argv){
    printf("\nMPTEST: Usage: mpirun -np <# of procs> %s", argv[0]);
#ifdef RANK_TEST
    printf("\nPrint rank of each node (# of procs > 1)."); 
#elif SINGLE_SEND_TEST
    printf("\nSend message from node 0 to node 1 (# of procs >= 2)."); 
#elif RIGHT_SHIFT_TEST
    printf("\nSend data to the node represented by next rank."); 
#elif CUSTOM_SEND_TEST
    printf("<routing_file> \nSend/Receive data based on custom routing."); 
#elif SEND_ONE_TO_ALL_TEST
    printf("[node_rank] \nNode node_rank (default=0) send to all nodes."); 
#elif RECEIVE_ONE_FROM_ALL_TEST
    printf("[node_rank] \nNode node_rank (default=0) receive from all nodes."); 
#endif
    printf("\n");
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

int main(int argc, char* argv[]){
    if ((argc == 2) && (strcmp(argv[1], "-help") == 0)){
        printHelp(argc, argv);
        return 0;
    }
    InitModuleObjects();
    try {
        EnableSEHtoExceptionMapping();

        startMPServer(0);
        IGroup* group = createIGroup(0, (INode **) NULL);
        ICommunicator* comm = createCommunicator(group);
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
  
#elif CUSTOM_SEND_TEST
    
#elif SEND_ONE_TO_ALL_TEST
    
#elif RECEIVE_ONE_FROM_ALL_TEST
    
#endif    
        comm->barrier();
        stopMPServer();
    } catch (IException *e){
        pexception("Exception", e);
    }
    return 0;
}
