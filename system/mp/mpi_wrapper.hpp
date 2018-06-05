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
#include "mpi.h"
#include "mpbase.hpp"
#include "mptag.hpp"
#include "mpbuff.hpp"

class NodeGroup;


namespace hpcc_mpi{
    // Variable type to keep track of send/receive requests
    typedef int CommRequest;
    
    // Status of a send/receive request
    enum CommStatus{
        INCOMPLETE = 0,
        SUCCESS,
        CANCELED
    };
    
    /**
     */
    void initialize();
    //tear-down the MPI framework
    void finalize();

    /**
    * Get the rank of the processor within the MPI communicator in the NodeGroup
    * @param group      NodeGroup which the processor rank we want to get
    * @return           rank of the calling node/processor
    */
    rank_t rank(NodeGroup &group);

    /**
    * Get the no of the processors within the MPI communicator in the NodeGroup
    * @param group      NodeGroup which the number of processors we want to get
    * @return           number of nodes/processors in the NodeGroup
    */
    rank_t size(NodeGroup &group);

    /**
    * Send data to a destination node/processor
    * @param dstRank    Rank of the node which we want to send data to
    * @param tag        Message tag 
    * @param mbuf       The message
    * @param group      In which nodegroup the destination rank belongs to
    * @param async      (optional; default=true) synchronous/asynchronous call
    * @return           Return a CommRequest object which you can use to keep 
    *                   track of the status of this communication call. Use 
    *                   releaseComm(...) function to release this object once 
    *                   done using it. 
    */
    CommRequest sendData(rank_t dstRank, mptag_t tag, CMessageBuffer &mbuf, NodeGroup &group, bool async = true);
    
    /**
    * Receive data from a node/processor
    * @param sourceRank Rank of the node which to receive data from
    * @param tag        Message tag 
    * @param mbuf       The CMessageBuffer to save the incoming message to
    * @param group      In which nodegroup the destination rank belongs to
    * @param async      (optional; default=true) synchronous/asynchronous call
    * @return           Return a CommRequest object which you can use to keep 
    *                   track of the status of this communication call. Use 
    *                   releaseComm(...) function to release this object once 
    *                   done using it. 
    */    
    CommRequest readData(rank_t sourceRank, mptag_t tag, CMessageBuffer &mbuf, NodeGroup &group, bool async = true);    
    
    /**
    * Check to see if there's a incoming message
    * @param sourceRank Rank of the node (or RANK_ALL) which to receive data from
    * @param tag        Message tag (or TAG_ALL)
    * @param group      In which nodegroup the destination rank belongs to
    * @return           Returns true if there is a incoming message and both 
    *                   sourceRank and tag variables updated.
    */    
    bool hasIncomingMessage(rank_t &sourceRank, mptag_t &tag, NodeGroup &group);
    
    /**
    * Cancel a send/receive communication request
    * @param commReq    CommRequest object 
    * @return           True if successfully canceled
    */    
    bool cancelComm(hpcc_mpi::CommRequest commReq);
    
    /**
    * Get the status of a send/receive communication request
    * @param commReq    CommRequest object 
    * @return           Communication Status
    */    
    CommStatus getCommStatus(hpcc_mpi::CommRequest commReq);
    
    /**
    * Free a send/receive communication request
    * @param commReq    CommRequest object 
    */    
    void releaseComm(CommRequest commReq);

    /**
    * Communication barrier 
    * @param group      NodeGroup to put barrier on
    */    
    void barrier(NodeGroup &group);

}

/**
 * MPI aware IGroup implementation
 */
class NodeGroup: implements IGroup, public CInterface{
private:
    MPI_Comm mpi_comm;
    std::vector<std::map<mptag_t, hpcc_mpi::CommRequest> > commRequests;
    std::map<int, std::map<mptag_t, hpcc_mpi::CommRequest> > commRequestsNonRank;
    
    NodeGroup(const MPI_Comm &_mpi_comm): mpi_comm(_mpi_comm){
        count = hpcc_mpi::size(*this);
        self_rank = hpcc_mpi::rank(*this);
        commRequests.resize(count);
    }
    
protected: friend class CNodeIterator;
    rank_t count = RANK_NULL;
    mutable rank_t self_rank = RANK_NULL;
    NodeGroup *parent = NULL;
    
public:
    IMPLEMENT_IINTERFACE;

    NodeGroup(): NodeGroup(MPI_COMM_WORLD){
    }

    rank_t ordinality()  const{ 
        return count; 
    }
    rank_t rank() const { 
        return self_rank;
    }

    const MPI_Comm operator()(){
        return mpi_comm;
    };
    
    void addCommRequest(rank_t rank, mptag_t tag, hpcc_mpi::CommRequest req){
        int r = rank;
        if (r < 0){
            if (commRequestsNonRank.find(r)==commRequestsNonRank.end()){
                commRequestsNonRank[r]=std::map<mptag_t, hpcc_mpi::CommRequest>();
            }
            commRequestsNonRank[r][tag] = req;
        }else{
            commRequests[r][tag] = req;
        }
    }
    
    hpcc_mpi::CommRequest getCommRequest(rank_t rank, mptag_t tag){
        int r = rank;
        if (r < 0){
            if (commRequestsNonRank.find(r)!=commRequestsNonRank.end()){
                return commRequests[r][tag];
            }
        }else if (commRequests[r].find(tag)!=commRequests[r].end()){
            return commRequests[r][tag];
        }
        return -1;
    }
    
    void removeCommRequest(rank_t rank, mptag_t tag){
        int r = rank;
        if (getCommRequest(rank, tag)>=0){
            if (r < 0){
                commRequestsNonRank[r].erase(tag);
            }else{ 
                commRequests[r].erase(tag);
            }
        }
        
    }
    
    rank_t rank(const SocketEndpoint &ep) const {
        return (rank_t)(ep.port);
    }
    
    ~NodeGroup(){}
    
    rank_t rank(INode *node) const  {UNIMPLEMENTED;}
    GroupRelation compare(const IGroup *grp) const {UNIMPLEMENTED;}
    bool equals(const IGroup *grp) const{UNIMPLEMENTED;}
    void translate(const IGroup *othergroup, rank_t nranks, const rank_t *otherranks, rank_t *resranks ) const {UNIMPLEMENTED;}
    IGroup *subset(rank_t start,rank_t num) const{UNIMPLEMENTED;}
    virtual IGroup *subset(const rank_t *order,rank_t num) const{UNIMPLEMENTED;}
    virtual IGroup *combine(const IGroup *grp) const{UNIMPLEMENTED;}
    bool isMember(INode *node) const{UNIMPLEMENTED;}
    bool isMember() const{UNIMPLEMENTED;}
    unsigned distance(const IpAddress &ip) const{UNIMPLEMENTED;}
    unsigned distance(const IGroup *grp) const{UNIMPLEMENTED;}
    IGroup *diff(const IGroup *g) const{UNIMPLEMENTED;}
    bool overlaps(const IGroup *grp) const{UNIMPLEMENTED;}
    IGroup *intersect(const IGroup *g) const{UNIMPLEMENTED;}
    IGroup *swap(rank_t r1,rank_t r2) const{UNIMPLEMENTED;}
    virtual IGroup *add(INode *node) const {UNIMPLEMENTED;}
    virtual IGroup *add(INode *node,unsigned pos) const {UNIMPLEMENTED;}
    virtual IGroup *remove(unsigned pos) const{UNIMPLEMENTED;}
    virtual IGroup *rotate(int num) const{UNIMPLEMENTED;}
    INode &queryNode(rank_t r) const {UNIMPLEMENTED;}
    INode *getNode(rank_t r) const {UNIMPLEMENTED;}
    StringBuffer &getText(StringBuffer &text) const {UNIMPLEMENTED;}
    static IGroup *fromText(const char *s,unsigned defport) {UNIMPLEMENTED;}
    void serialize(MemoryBuffer &tgt) const{UNIMPLEMENTED;}
    static IGroup *deserialize(MemoryBuffer &src) {UNIMPLEMENTED;}
    void getSocketEndpoints(SocketEndpointArray &sea) const{UNIMPLEMENTED;}
    INodeIterator *getIterator(rank_t start=0,rank_t num=RANK_NULL) const {UNIMPLEMENTED; }
};

#endif /* MPI_WRAPPER_HPP */

