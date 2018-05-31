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
    void** data;
    int* size;
    int rank;
    int tag;
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
    return req;
}

void release_request(int req){
    requestListLock.lock();
    freeRequests.push_back(req);
    requestListLock.unlock();
}

int send(void* data, int size, int rank, int tag, MPI_Comm comm){
    int nextFree = get_free_req();
    MPI_Isend(data, size, MPI_BYTE, rank, tag, comm, &(requests[nextFree].request));
    return nextFree;
}

int startReceiveProcess(int i){
    int flag;
    MPI_Status stat;
    MPI_Iprobe(requests[i].rank, requests[i].tag, requests[i].comm, &flag, &stat);
    if (flag){
        requests[i].requiresProbing = 0;
        MPI_Get_count(&stat, MPI_BYTE, requests[i].size);
        *(requests[i].data) = malloc(requests[i].size[0]);
        MPI_Irecv(*(requests[i].data), requests[i].size[0], MPI_BYTE, requests[i].rank, requests[i].tag, requests[i].comm, &(requests[i].request));
        flag = hpcc_mpi::isCommComplete(i);
    }
    return flag;
}

int recv(void* &data, int &size, int rank, int tag, MPI_Comm comm){
    int nextFree = get_free_req();
    requests[nextFree].requiresProbing = 1;
    requests[nextFree].data = &data;
    requests[nextFree].size = &size;
    requests[nextFree].rank = rank;
    requests[nextFree].tag = tag;
    requests[nextFree].comm = comm;
    startReceiveProcess(nextFree);
    return nextFree;
}

//----------------------------------------------------------------------------//
int hpcc_mpi::isCommComplete(int i){
    if (i < 0){
        return 1;
    }
    int flag = 0;
    if (requests[i].requiresProbing){
        flag = startReceiveProcess(i);
    }else{
        MPI_Status stat;
        MPI_Test(&(requests[i].request), &flag, &stat);
    }
    return flag;
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

int hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, void* data, int size, NodeGroup &group, bool async){
//    int size = buffer.length();
//    char* data = (char *) malloc(size);
//    buffer.reset();
//    buffer.read(size, data);
    
    int target = dstRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    
    int req = -1;
    if (async){
        req = send(data, size, target, tag, group());
    }else{
        MPI_Send(&size, 1, MPI_INT, target, sizeTag, group());
        MPI_Send(data, size, MPI_BYTE, target, tag, group());
    }
    return req;
}

int hpcc_mpi::readData(rank_t sourceRank, mptag_t mptag, void* &data, int &size, NodeGroup &group, bool async){
//    int size;
    int source = sourceRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    MPI_Status stat;
    int req = -1;
    if (async){
        req = recv(data, size, source, tag, group());
    }else {
        MPI_Recv(&size, 1, MPI_INT, source, sizeTag, group(), &stat);
        data = malloc(size);
        MPI_Recv(data, size, MPI_BYTE, source, tag, group(), &stat);
    }
    return req;
}

void hpcc_mpi::barrier(NodeGroup &group){
    MPI_Barrier(group());
}