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

class NodeGroup;

namespace hpcc_mpi{
    void initialize();
    void finalize();

    rank_t rank(NodeGroup &);
    rank_t size(NodeGroup &);
    
    int sendData(int dstRank, int tag, void *buffer, int bufferSize, NodeGroup &group, bool async = true);
    int receiveData(int sourceRank, int tag, void **buffer, int bufferSize, NodeGroup &group, bool async = true);

}

class NodeGroup: implements IGroup, public CInterface{
private:
    MPI_Comm mpi_comm;
    
    NodeGroup(const MPI_Comm &_mpi_comm): mpi_comm(_mpi_comm){
        count = hpcc_mpi::size(*this);
        self_rank = hpcc_mpi::rank(*this);
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
    ~NodeGroup(){}
    rank_t rank(const SocketEndpoint &ep) const {UNIMPLEMENTED;}
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

