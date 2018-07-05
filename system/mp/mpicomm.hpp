/*
 * mpicomm.hpp
 *
 *  Created on: Jul 5, 2018
 *      Author: samindaw
 */

#ifndef SYSTEM_MP_MPICOMM_HPP_
#define SYSTEM_MP_MPICOMM_HPP_

#include "mpcomm.hpp"

#define MPI_ENV "hpcc-withmpi"

extern mp_decl ICommunicator *createMPICommunicator(IGroup *group);

#endif /* SYSTEM_MP_MPICOMM_HPP_ */
