/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"

#define MPTAG_SIZE 1000

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

int hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, CMessageBuffer &buffer, NodeGroup &group, bool async){
    int size = buffer.length();
    char* data = (char *) malloc(size);
    buffer.reset();
    buffer.read(size, data);
    
    int target = dstRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    
    if (async){
        
    }else{
        MPI_Send(&size, 1, MPI_INT, target, sizeTag, group());
        MPI_Send(data, size, MPI_BYTE, target, tag, group());
    }
    return 0;
}

int hpcc_mpi::readData(rank_t sourceRank, mptag_t mptag, CMessageBuffer &buffer, NodeGroup &group, bool async){
    int size;
    int source = sourceRank;
    int tag = mptag;
    int sizeTag = MPTAG_SIZE + tag;
    MPI_Status stat;
    if (async){
        
    }else {
        MPI_Recv(&size, 1, MPI_INT, source, sizeTag, group(), &stat);
    
        char* data = (char *) malloc(size);
        MPI_Recv(data, size, MPI_BYTE, source, tag, group(), &stat);

        buffer.reset();
        buffer.append(size, data);
        free(data);
    }
    return 0;
}