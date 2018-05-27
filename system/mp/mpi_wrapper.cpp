/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>

#include "mpi_wrapper.hpp"

rank_t hpcc_mpi::rank(IGroup &group){
    return 0;
}

void hpcc_mpi::initialize(){
    MPI_Init(NULL,NULL);
}

void hpcc_mpi::finalize(){
    MPI_Finalize();
}