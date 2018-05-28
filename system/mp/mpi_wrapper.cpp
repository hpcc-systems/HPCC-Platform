/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"

rank_t hpcc_mpi::rank(){
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank;
}

rank_t hpcc_mpi::size(){
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return size;
}

void hpcc_mpi::initialize(){
    MPI_Init(NULL,NULL);
}

void hpcc_mpi::finalize(){
    MPI_Finalize();
}