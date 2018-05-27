/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   mpi_wrapper.hpp
 * Author: samindaw
 *
 * Created on May 26, 2018, 4:14 PM
 */

#ifndef MPI_WRAPPER_HPP
#define MPI_WRAPPER_HPP

#include "mpi.h"
#include "mpbase.hpp"

namespace hpcc_mpi{
    rank_t rank(IGroup &group);
    void initialize();
    void finalize();
}

#endif /* MPI_WRAPPER_HPP */

