/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"
#include <cstdlib>
#include <queue>
#include <mutex>

#define MPTAG_SIZE 1000

//-------------------send/receive asynchronous communication----------------------//

// Data structure to keep the data relating to send/receive communications
class CommData{
public:    
    int requiresProbing;            // Required for asynchronous receive
    void* data;                     // Data structure which keeps the sent/recv data
    int size;                       // size of 'data'
    CMessageBuffer *mbuf;           // buffer only valid for recv comm.
    NodeGroup* group;               // To which communicator this belongs to
    int rank;                       // source/destination rank of the processor
    int tag;                        // MPI tag infomation
    hpcc_mpi::CommStatus status;    // Status of the communication
    MPI_Comm comm;                  // MPI communicator
    MPI_Request request;            // request object to keep track of ongoing MPI call
    CriticalSection *dataLock;
};

std::vector<CommData> requests;     // CommData Pool
std::deque<int> freeRequests;       // List of index unused in ComData Pool
CriticalSection requestListLock;         // A mutex lock for the index list above

// Returns index of a ComData object in CommData Pool
int get_free_req(){
    int req;
    CriticalBlock block(requestListLock);
    if(freeRequests.empty()) {      //if no free objects, create a new one
        requests.push_back(CommData());
        requests[requests.size()-1].dataLock = new CriticalSection();
        freeRequests.push_back(requests.size()-1);
    }
    // pop the index at the front of the queue
    req = freeRequests.front();
    freeRequests.pop_back();
    // requestListLock.unlock();
    // all reset CommData
    requests[req].requiresProbing = 0;
    requests[req].mbuf = NULL;
    requests[req].status = hpcc_mpi::CommStatus::INCOMPLETE;
    return req;
}

// Performs asynchronous MPI send and return the index of CommData
int send(void* data, int size, int rank, int tag, MPI_Comm comm){
    int nextFree = get_free_req();
    int err = MPI_Isend(data, size, MPI_BYTE, rank, tag, comm, &(requests[nextFree].request));
    requests[nextFree].status = (err != MPI_SUCCESS)? hpcc_mpi::CommStatus::ERROR : hpcc_mpi::CommStatus::INCOMPLETE;
    return nextFree;
}

// Check to see if a message is awaiting for retrieval based on the data in 
// CommData object (obtained by the index of CommData pool) and if so start an 
// asynchronous MPI recv. Return the status of the communication.
hpcc_mpi::CommStatus startReceiveProcess(int i){
    //TODO revisit to check for the correctness of this function
    MPI_Status stat;
    int flag = 0;
    MPI_Iprobe(requests[i].rank, requests[i].tag, requests[i].comm, &flag, &stat);
    if (flag){
        requests[i].requiresProbing = 0;
        MPI_Get_count(&stat, MPI_BYTE, &(requests[i].size));
        requests[i].data = malloc(requests[i].size);
        MPI_Irecv(requests[i].data, requests[i].size, MPI_BYTE, requests[i].rank, requests[i].tag, requests[i].comm, &(requests[i].request));
        requests[i].status = hpcc_mpi::getCommStatus(i);
    }
    return requests[i].status;
}

// Performs asynchronous MPI recv and return the index of CommData
int recv(int rank, int tag, CMessageBuffer &mbuf, MPI_Comm comm){
    int nextFree = get_free_req();
    CriticalBlock block(*(requests[nextFree].dataLock));
    requests[nextFree].requiresProbing = 1;
    requests[nextFree].rank = rank;
    requests[nextFree].tag = tag;
    requests[nextFree].comm = comm;
    requests[nextFree].mbuf = &mbuf;
    startReceiveProcess(nextFree);
    return nextFree;
}

int getRank(rank_t sourceRank){
    if (sourceRank == RANK_ALL)
        return MPI_ANY_SOURCE;
    else
        return sourceRank;
}

int getTag(mptag_t mptag){
    if (mptag == TAG_ALL)
        return MPI_ANY_TAG;
    else
        return mptag;
}

//----------------------------------------------------------------------------//

/** See mpi_wrapper.hpp header file for function descriptions of the following **/

bool hpcc_mpi::hasIncomingMessage(rank_t &sourceRank, mptag_t &mptag, NodeGroup &group){
    MPI_Status stat;
    int flag;
    int rank = getRank(sourceRank);
    int tag = getTag(mptag);
    MPI_Iprobe(rank, tag, group(), &flag, &stat);
    if (flag){ // update the sender and tag based on status object
        sourceRank = stat.MPI_SOURCE;
        mptag = mptag_t(stat.MPI_TAG);
    }
    return flag > 0;
}

void hpcc_mpi::releaseComm(CommRequest commReq){
    CriticalBlock block(requestListLock);
    (requests[commReq].group)->removeCommRequest
            (requests[commReq].rank, (mptag_t)requests[commReq].tag);
    //TODO Free CommData object if there's too many in the Pool (don't forget to delete mutex)   
    freeRequests.push_back(commReq);
    // requestListLock.unlock();
}

bool hpcc_mpi::cancelComm(hpcc_mpi::CommRequest commReq){
    if (requests[commReq].status == hpcc_mpi::CommStatus::INCOMPLETE){
        if (!requests[commReq].requiresProbing){ 
            // we need to perform cancellation only if there's no probing step
            int cancelled = MPI_Cancel(&(requests[commReq].request));
            if (cancelled == MPI_SUCCESS){ // MPI framework managed to successfully cancel
                MPI_Request_free(&(requests[commReq].request));
                requests[commReq].status = hpcc_mpi::CommStatus::CANCELED;
            }
            return (cancelled == MPI_SUCCESS);
        }else{
            //Nothing else to do
            requests[commReq].status = hpcc_mpi::CommStatus::CANCELED;
        }
    }
    return true;
}

hpcc_mpi::CommStatus hpcc_mpi::getCommStatus(CommRequest commReq){
    hpcc_mpi::CommStatus ret;
    if (commReq < 0){                           // for synchronous calls
        ret = hpcc_mpi::CommStatus::SUCCESS;
    } else {
        CriticalBlock block(*(requests[commReq].dataLock));
        if (requests[commReq].status == hpcc_mpi::CommStatus::INCOMPLETE){                 // if requests are not completed/canceled/error
            if (requests[commReq].requiresProbing){     // recv communication still waiting for a message to arrive
                requests[commReq].status = startReceiveProcess(commReq);
            } else {
                MPI_Status stat;
                int flag;
                MPI_Test(&(requests[commReq].request), &flag, &stat);
                if (flag){                              // if request completed
                    if (requests[commReq].mbuf != NULL){//if it was a receive call
                        (requests[commReq].mbuf)->reset();
                        (requests[commReq].mbuf)->append(requests[commReq].size,requests[commReq].data);
                        //TODO revisit on how to create the replytag in the CMessageBuffer
                        SocketEndpoint ep;
                        ep.port = stat.MPI_SOURCE;
                        (requests[commReq].mbuf)->init(ep, (mptag_t)(stat.MPI_TAG), TAG_REPLY_BASE);
                    }
                    free(requests[commReq].data);       
                    //TODO test for canceled and update status for cancelled
                    requests[commReq].status = hpcc_mpi::CommStatus::SUCCESS;
                }
            }
        }
        ret = requests[commReq].status;
    }
    return ret;
}

rank_t hpcc_mpi::rank(NodeGroup &group){
    int rank;
    MPI_Comm_rank(group(), &rank);
    return rank;
}

rank_t hpcc_mpi::size(NodeGroup &group){
    int size;
    MPI_Comm_size(group(), &size);
    return size;
}

void hpcc_mpi::initialize(){
    int required = MPI_THREAD_FUNNELED;
    int provided;
    MPI_Init_thread(NULL,NULL, required, &provided);
    // printf("expected=%d, provided=%d, %d, %d, %d, %d\n",required, provided, MPI_THREAD_SINGLE, MPI_THREAD_FUNNELED, MPI_THREAD_SERIALIZED, MPI_THREAD_MULTIPLE);
    assertex(provided == required);
    // MPI_Init(NULL,NULL);
}

void hpcc_mpi::finalize(){
    MPI_Finalize();
}

int hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, CMessageBuffer &mbuf, NodeGroup &group, bool async){
    int size = mbuf.length();
    char* data = (char *) malloc(size);
    mbuf.reset();
    mbuf.read(size, data);
    int target = getRank(dstRank);
    int tag = getTag(mptag);
    
    int req = -1;
    if (async){
        req = send(data, size, target, tag, group());
        requests[req].group = &group;
        group.addCommRequest(dstRank, mptag, req);      // keep track of current requests incase of need to cancel
    }else{
        int sizeTag = MPTAG_SIZE + tag;
        MPI_Send(&size, 1, MPI_INT, target, sizeTag, group());
        MPI_Send(data, size, MPI_BYTE, target, tag, group());
        free(data);
    }
    return req;
}

int hpcc_mpi::readData(rank_t sourceRank, mptag_t mptag, CMessageBuffer &mbuf, NodeGroup &group, bool async){
    int source = getRank(sourceRank);
    int tag = getTag(mptag);
    MPI_Status stat;
    int req = -1;
    if (async){
        req = recv(source, tag, mbuf, group());
        requests[req].group = &group;
        group.addCommRequest(sourceRank, mptag, req);   // keep track of current requests incase of need to cancel
    }else {
        int size;
        int sizeTag = MPTAG_SIZE + tag;
        MPI_Recv(&size, 1, MPI_INT, source, sizeTag, group(), &stat);
        void* data = malloc(size);
        MPI_Recv(data, size, MPI_BYTE, source, tag, group(), &stat);
        mbuf.reset();
        mbuf.append(size, data);
        free(data);
    }
    return req;
}

void hpcc_mpi::barrier(NodeGroup &group){
    MPI_Barrier(group());
}
