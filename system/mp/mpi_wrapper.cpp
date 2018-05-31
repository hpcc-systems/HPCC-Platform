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
struct CommData{
    int requiresProbing;
    void* data;
    int size;
    CMessageBuffer *mbuf;
    int rank;
    int tag;
    hpcc_mpi::CommStatus status;
    MPI_Comm comm;
    MPI_Request request;
};

std::vector<CommData> requests;
std::deque<int> freeRequests;
std::mutex requestListLock;

int get_free_req(){
    int req;
    requestListLock.lock();
    if(freeRequests.empty()) {
        requests.push_back(CommData());
        freeRequests.push_back(requests.size()-1);
    }
    req = freeRequests.front();
    freeRequests.pop_back();
    requestListLock.unlock();
    requests[req].requiresProbing = 0;
    requests[req].mbuf = NULL;
    requests[req].status = hpcc_mpi::CommStatus::INCOMPLETE;
    return req;
}

int send(void* data, int size, int rank, int tag, MPI_Comm comm){
    int nextFree = get_free_req();
    MPI_Isend(data, size, MPI_BYTE, rank, tag, comm, &(requests[nextFree].request));
    return nextFree;
}

hpcc_mpi::CommStatus startReceiveProcess(int i){
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

int recv(int rank, int tag, CMessageBuffer &mbuf, MPI_Comm comm){
    int nextFree = get_free_req();
    requests[nextFree].requiresProbing = 1;
    requests[nextFree].rank = rank;
    requests[nextFree].tag = tag;
    requests[nextFree].comm = comm;
    requests[nextFree].mbuf = &mbuf;
    startReceiveProcess(nextFree);
    return nextFree;
}

//----------------------------------------------------------------------------//

void hpcc_mpi::releaseComm(CommRequest commReq){
    requestListLock.lock();
    freeRequests.push_back(commReq);
    requestListLock.unlock();
}

hpcc_mpi::CommStatus hpcc_mpi::getCommStatus(CommRequest commReq){
    if (commReq < 0){
        return hpcc_mpi::CommStatus::SUCCESS;
    }
    if (requests[commReq].status){
        return requests[commReq].status;
    }
    if (requests[commReq].requiresProbing){
        requests[commReq].status = startReceiveProcess(commReq);
    }else{
        MPI_Status stat;
        int flag;
        MPI_Test(&(requests[commReq].request), &flag, &stat);
        if (flag){
            if (requests[commReq].mbuf != NULL){ //if it was a receive call
                (requests[commReq].mbuf)->reset();
                (requests[commReq].mbuf)->append(requests[commReq].size,requests[commReq].data);
            }
            free(requests[commReq].data);
            //TODO test for canceled and update status for cancelled
            requests[commReq].status = hpcc_mpi::CommStatus::SUCCESS;
        }
    }
    return requests[commReq].status;
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
    MPI_Init(NULL,NULL);
}

void hpcc_mpi::finalize(){
    MPI_Finalize();
}

int hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, CMessageBuffer &mbuf, NodeGroup &group, bool async){
    int size = mbuf.length();
    char* data = (char *) malloc(size);
    mbuf.reset();
    mbuf.read(size, data);
    
    int target = dstRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    
    int req = -1;
    if (async){
        req = send(data, size, target, tag, group());
    }else{
        MPI_Send(&size, 1, MPI_INT, target, sizeTag, group());
        MPI_Send(data, size, MPI_BYTE, target, tag, group());
        free(data);
    }
    return req;
}

int hpcc_mpi::readData(rank_t sourceRank, mptag_t mptag, CMessageBuffer &mbuf, NodeGroup &group, bool async){
//    int size;
    int source = sourceRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    MPI_Status stat;
    int req = -1;
    if (async){
        req = recv(source, tag, mbuf, group());
    }else {
        int size;
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