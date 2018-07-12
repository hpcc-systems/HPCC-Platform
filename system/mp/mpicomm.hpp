/*
 * mpicomm.hpp
 *
 *  Created on: Jul 5, 2018
 *      Author: samindaw
 */

#ifndef SYSTEM_MP_MPICOMM_HPP_
#define SYSTEM_MP_MPICOMM_HPP_
#undef BOOL

#include <mpi.h>
#include "mpcomm.hpp"

/**
 * Create an instance of an ICommunicator that uses MPI framework for communication
 *
 * @param group     IGroup containing the nodes which the communicator would initiate
 *                  communications on
 */
extern mp_decl ICommunicator *createMPICommunicator(IGroup *group);

/**
 * Initialize the MPI framework with the provided MPI communicator
 */
extern mp_decl void initializeMPI(MPI::Comm& comm);

/**
 * Finalize MPI framework
 */
extern mp_decl void terminateMPI();

/**
 * Get the global rank among all the MPI hosts
 */
extern mp_decl int getMPIGlobalRank();

#endif /* SYSTEM_MP_MPICOMM_HPP_ */
