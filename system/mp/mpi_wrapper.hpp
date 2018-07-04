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
#undef BOOL

#include <map>
#include <vector>
#include <algorithm>
#include "mpi.h"
#include "mpbase.hpp"
#include "mptag.hpp"
#include "mpbuff.hpp"
#include "mplog.hpp"

class NodeGroup;

namespace hpcc_mpi
{
    // Variable type to keep track of send/receive requests
    typedef int CommRequest;
    
//    class mp_decl SharedMessageBuffer: public CMessageBuffer
//    {
//    private:
//        int count = 0;
//        CriticalSection sect;
//    public:
//        int addShare()
//        {
//            sect.enter();
//            count++;
//            sect.leave();
//        }
//        inline void Release() const
//        {
//            sect.enter();
//            count--;
//            sect.leave();
//            if (count == 0)
//                delete this;
//        }
//
//    };

    // Status of a send/receive request
    enum CommStatus
    {
        INCOMPLETE = 0,
        SUCCESS = 1,
        CANCELED = 2,
        ERROR = 3,
        TIMEDOUT = 4
    };
    
    /**
     * Initialize MPI framework
     * @param withMultithreading     (optional, default=false) initialize with multi-threaded support
     */
    void initialize(bool withMultithreading = false);

    /**
     * tear-down the MPI framework
     */
    void finalize();

    /**
    * Get the rank of the processor within the MPI communicator in the NodeGroup
    * @param group      NodeGroup which the processor rank we want to get
    * @return           rank of the calling node/processor
    */
    rank_t rank(MPI_Comm comm);

    /**
    * Get the no of the processors within the MPI communicator in the NodeGroup
    * @param group      NodeGroup which the number of processors we want to get
    * @return           number of nodes/processors in the NodeGroup
    */
    rank_t size(MPI_Comm comm);
    
    /**
    * Send data to a destination node/processor
    * @param dstRank    Rank of the node which we want to send data to
    * @param tag        Message tag
    * @param mbuf       The message
    * @param group      In which nodegroup the destination rank belongs to
    * @param timeout    Time to complete the communication
    * @return           Return a CommRequest object which you can use to keep
    *                   track of the status of this communication call. Use
    *                   releaseComm(...) function to release this object once
    *                   done using it.
    */
    CommStatus sendData(rank_t dstRank, mptag_t tag, CMessageBuffer &mbuf, MPI_Comm comm, unsigned timeout);

    /**
    * Receive data from a node/processor
    * @param sourceRank Rank of the node which to receive data from
    * @param tag        Message tag
    * @param mbuf       The CMessageBuffer to save the incoming message to
    * @param group      In which nodegroup the destination rank belongs to
    * @param timeout    Time to complete the communication
    * @return           Return a CommRequest object which you can use to keep
    *                   track of the status of this communication call. Use
    *                   releaseComm(...) function to release this object once
    *                   done using it.
    */
    CommStatus readData(rank_t sourceRank, mptag_t tag, CMessageBuffer &mbuf, MPI_Comm comm, unsigned timeout);
    
    /**
    * Check to see if there's a incoming message
    * @param sourceRank Rank of the node (or RANK_ALL) which to receive data from
    * @param tag        Message tag (or TAG_ALL)
    * @param group      In which nodegroup the destination rank belongs to
    * @return           Returns true if there is a incoming message and both 
    *                   sourceRank and tag variables updated.
    */    
    bool hasIncomingMessage(rank_t &sourceRank, mptag_t &tag, MPI_Comm comm);
    
    /**
    * Cancel a send/receive communication request
    * @param send       true=>Send communication, false=>Recv communication
    * @param rank       Rank of the processor
    * @param tag        Message tag
    * @param comm       MPI communicator
    * @return           True if successfully canceled
    */    
    bool cancelComm(bool send, rank_t rank, mptag_t mptag, MPI_Comm comm);
    
    /**
    * Communication barrier 
    * @param group      NodeGroup to put barrier on
    */    
    void barrier(MPI_Comm comm);

}

#endif /* MPI_WRAPPER_HPP */

