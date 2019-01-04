/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#define mp_decl DECL_EXPORT
#include "platform.h"
#include "jlib.hpp"
#include "jlog.hpp"

#include "mpbase.hpp"

static INode *MyNode=NULL;
static INode *NullNode=NULL;


class MPNode: implements INode, public CInterface
{
protected: friend class MPNodeCache;
    SocketEndpoint ep;
public:
    IMPLEMENT_IINTERFACE;
    MPNode(const SocketEndpoint &_ep)
        : ep(_ep)
    {
#ifdef _DEBUG
//      assertex(!_ep.LoopBack()); // localhost not supported for nodes
#endif
    }
    bool equals(const INode *n) const { return endpoint().equals(n->endpoint()); }
    void serialize(MemoryBuffer &tgt)
    {
        ep.serialize(tgt);
    }
    static MPNode *deserialize(MemoryBuffer &src);
    unsigned getHash() const { return ep.hash(0); }
    virtual bool isHost() const
    {
        return ep.isHost();
    }
    virtual bool isLocalTo(INode *to) const
    {
        return ep.ipequals(to->endpoint());
    }
    const SocketEndpoint &endpoint() const { return ep; }

};

class MPNodeCache: public SuperHashTableOf<MPNode,SocketEndpoint>
{
    CriticalSection sect;
public:
    ~MPNodeCache()
    {
        _releaseAll();
    }

    void onAdd(void *)
    {
        // not used
    }

    void onRemove(void *e)
    {
        MPNode &elem=*(MPNode *)e;      
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const MPNode &elem=*(const MPNode *)e;      
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const MPNode &elem=*(const MPNode *)p;      
        return &elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((MPNode *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(MPNode,SocketEndpoint);

    MPNode *lookup(const SocketEndpoint &ep)
    {
        CriticalBlock block(sect);
        MPNode *item=SuperHashTableOf<MPNode,SocketEndpoint>::find(&ep);
        if (!item) {
            item = new MPNode(ep);
            add(*item);
        }
        return LINK(item);
    }


} *NodeCache = NULL;

MPNode *MPNode::deserialize(MemoryBuffer &src)
{
    SocketEndpoint ep;
    ep.deserialize(src);
    if (NodeCache)
        return NodeCache->lookup(ep);
    return new MPNode(ep);
}




class CGroup: implements IGroup, public CInterface
{
protected: friend class CNodeIterator;
    rank_t count;
    mutable rank_t myrank;
    INode **nodes;
public:
    IMPLEMENT_IINTERFACE;

    CGroup(rank_t _count,INode **_nodes=NULL) 
    {
        count = _count;
        myrank = RANK_NULL;
        nodes = count?(INode **)malloc(count*sizeof(INode *)):NULL;
        if (_nodes) {
            for (rank_t i=0; i<count; i++)
                nodes[i] = LINK(_nodes[i]);
        }
    }

    CGroup(rank_t _count,const SocketEndpoint *ep) 
    {
        count = _count;
        myrank = RANK_NULL;
        nodes = count?(INode **)malloc(count*sizeof(INode *)):NULL;
        for (rank_t i=0; i<count; i++) {
            if (NodeCache)
                nodes[i] = NodeCache->lookup(*ep);
            else
                nodes[i] = new MPNode(*ep);
            ep++;
        }
    }

    CGroup(SocketEndpointArray &epa) 
    {
        count = epa.ordinality();
        myrank = RANK_NULL;
        nodes = count?(INode **)malloc(count*sizeof(INode *)):NULL;
        for (rank_t i=0; i<count; i++) {
            if (NodeCache)
                nodes[i] = NodeCache->lookup(epa.item(i));
            else
                nodes[i] = new MPNode(epa.item(i));
        }

    }

    ~CGroup()
    {
        for (rank_t i=0; i<count; i++)
            nodes[i]->Release();
        free(nodes);
    }

    rank_t ordinality()  const { return count; }
    rank_t rank(const SocketEndpoint &ep) const 
    {
        rank_t i=count;
        while (i) {
            i--;
            // a group is a list of IpAddresses or a list of full endpoints
            SocketEndpoint nep=nodes[i]->endpoint();
            if (nep.port) {
                if (ep.equals(nep))
                    return i;
            }
            else if (ep.ipequals(nep))  // ip list so just check IP
                return i;
        }
        return RANK_NULL;
    }
    rank_t rank(INode *node) const  { return node?rank(node->endpoint()):RANK_NULL; }
    rank_t rank()  const { if (myrank==RANK_NULL) myrank = rank(MyNode); return myrank; }

    GroupRelation compare(const IGroup *grp) const 
    {
        if (grp) {
            rank_t r1=ordinality();
            rank_t r2=grp->ordinality();
            rank_t r;
            if (r1==0) {
                if (r2==0)
                    return GRidentical;
                return GRdisjoint;
            }
            if (r2==0)
                return GRdisjoint;
            bool somematch=false;
            if (r1==r2) { // check for identical
                r=r1;
                for (;;) {
                    r--;
                    if (!nodes[r]->equals(&grp->queryNode(r)))
                        break;
                    somematch = true;
                    if (r==0)
                        return GRidentical;
                }
            }
            else if (r2>r1) {   
                for (r=0;r<r1;r++) 
                    if (!nodes[r]->equals(&grp->queryNode(r)))
                        break;
                if (r==r1)
                    return GRbasesubset;
            }
            else {
                for (r=0;r<r1;r++) 
                    if (!nodes[r]->equals(&grp->queryNode(r%r2)))
                        break;
                if (r==r1)
                    return GRwrappedsuperset;
            }                   
            // the following could be improved
            bool *matched2=(bool *)calloc(r2,sizeof(bool));
            bool anymatched = false;
            bool allmatched1 = true;
            do {
                r1--;
                r=r2;
                for (;;) {
                    r--;
                    if (!matched2[r]) {
                        if (nodes[r1]->equals(&grp->queryNode(r))) {
                            matched2[r] = true;
                            anymatched = true;
                            break;
                        }
                    }
                    if (r==0) {
                        allmatched1 = false;
                        break;
                    }
                }
            } while (r1);
            bool allmatched2 = true;
            do {
                r2--;
                if (!matched2[r2])
                    allmatched2 = false;
            } while (r2);
            free(matched2);
            if (allmatched1) {
                if (allmatched2)
                    return GRdifferentorder;
                return GRsubset;
            }
            if (allmatched2)
                return GRsuperset;
            if (anymatched)
                return GRintersection;
        }
        return GRdisjoint;
    }

    bool equals(const IGroup *grp) const 
    {
        if (!grp)
            return false;
        rank_t r1=ordinality();
        if (r1!=grp->ordinality())
            return false;
        for (rank_t r=0;r<r1;r++)
            if (!nodes[r]->equals(&grp->queryNode(r)))
                return false;
        return true;
    }

    void translate(const IGroup *othergroup, rank_t nranks, const rank_t *otherranks, rank_t *resranks ) const 
    {
        while (nranks) {
            *resranks = rank(&othergroup->queryNode(*otherranks));
            nranks--;
            resranks++;
            otherranks++;
        }
    }

    IGroup *subset(rank_t start,rank_t num) const 
    {
        CGroup *newgrp = new CGroup(num);
        while (num) {
            num--;
            newgrp->nodes[num] = LINK(nodes[start+num]);
        }
        return newgrp;
    }

    virtual IGroup *subset(const rank_t *order,rank_t num) const 
    {
        CGroup *newgrp = new CGroup(num);
        for( rank_t i=0; i<num; i++ ) {
            newgrp->nodes[i] = LINK(nodes[*order]);
            order++;
        }
        return newgrp;
    }

    virtual IGroup *combine(const IGroup *grp) const 
    {
        rank_t g2ord = grp->ordinality();
        rank_t j = 0;
        INode **tmp = (INode **)malloc(g2ord*sizeof(INode *));
        rank_t i;
        for (i=0; i<g2ord; i++) {
            if (rank(&grp->queryNode(i))==RANK_NULL) {
                tmp[j] = grp->getNode(i);
                j++;
            }
        }
        CGroup *newgrp = new CGroup(count+j);
        for (i=0; i<count; i++)
            newgrp->nodes[i] = LINK(nodes[i]);
        for (rank_t k=0;k<j; k++)
            newgrp->nodes[i++] = tmp[k];
        free(tmp);
        return newgrp;
    }

    bool isMember(INode *node) const 
    {
        if (!node)
            return false;
        return rank(node->endpoint())!=RANK_NULL;
    }

    bool isMember() const 
    {
        assertex(MyNode);
        return rank()!=RANK_NULL;
    }

    unsigned distance(const IpAddress &ip) const
    {
        unsigned bestdist = (unsigned)-1;
        for (unsigned i=0;i<count;i++) {
            unsigned d = ip.ipdistance(nodes[i]->endpoint());
            if (d<bestdist)
                bestdist = d;
            }
        return bestdist;
    }

    unsigned distance(const IGroup *grp) const
    {
        if (!grp) 
            return (unsigned)-1;
        unsigned c2 = grp->ordinality();
        if (c2<count)
            return grp->distance(this);
        unsigned ret = c2;
        unsigned bestdist = (unsigned)-1;
        for (unsigned i=0;i<count;i++) {
            INode &node1 = *nodes[i];
            if (node1.equals(&grp->queryNode(i)))
                ret--;
            else {
                unsigned d = grp->distance(node1.endpoint());
                if (d<bestdist)
                    bestdist = d;
            }
        }
        if (bestdist!=(unsigned)-1)
            ret += bestdist;
        return ret;
    }




    IGroup *diff(const IGroup *g) const 
    {
        PointerArray toadd;
        ForEachNodeInGroup(i,*this) {
            INode *node = &queryNode(i);
            if (node&&!g->isMember(node))
                toadd.append(node);
        }
        ForEachNodeInGroup(j,*g) {
            INode *node = &g->queryNode(j);
            if (node&&!isMember(node))
                toadd.append(node);
        }
        unsigned num = toadd.ordinality();
        CGroup *newgrp = new CGroup(num);
        while (num) {
            num--;
            newgrp->nodes[num] = LINK(((INode *)toadd.item(num)));
        }
        return newgrp;
    }

    bool overlaps(const IGroup *grp) const
    {
        if (grp) {
            ForEachNodeInGroup(i,*grp) {
                if (isMember(&grp->queryNode(i)))
                    return true;
            }
        }
        return false;
    }


    IGroup *intersect(const IGroup *g) const 
    {
        PointerArray toadd;
        ForEachNodeInGroup(i,*this) {
            INode *node = &queryNode(i);
            if (node&&g->isMember(node))
                toadd.append(node);
        }
        unsigned num = toadd.ordinality();
        CGroup *newgrp = new CGroup(num);
        while (num) {
            num--;
            newgrp->nodes[num] = LINK(((INode *)toadd.item(num)));
        }
        return newgrp;
    }

    IGroup *swap(rank_t r1,rank_t r2) const 
    {
        CGroup *newgrp = new CGroup(count);
        rank_t i;
        for(i=0; i<count; i++ ) {
            if ((i==r1)&&(r2<count))
                newgrp->nodes[i] = LINK(nodes[r2]);
            if ((i==r2)&&(r1<count))
                newgrp->nodes[i] = LINK(nodes[r1]);
            else
                newgrp->nodes[i] = LINK(nodes[i]);
        }
        return newgrp;
    }

    virtual IGroup *add(INode *node) const 
    {
        CGroup *newgrp = new CGroup(count+1);
        rank_t i;
        for(i=0; i<count; i++ ) {
            newgrp->nodes[i] = LINK(nodes[i]);
        }
        newgrp->nodes[i] = LINK(node);
        return newgrp;
    }

    virtual IGroup *add(INode *node,unsigned pos) const 
    {
        CGroup *newgrp = new CGroup(count+1);
        unsigned j = 0;
        for( rank_t i=0; i<count; i++ ) {
            if (j==pos) {
                newgrp->nodes[j++] = LINK(node);
            }
            newgrp->nodes[j++] = LINK(nodes[i]);
        }
        return newgrp;
    }

    virtual IGroup *remove(unsigned pos) const 
    {
        assertex(pos<count);
        CGroup *newgrp = new CGroup(count-1);
        unsigned j=0;
        for( rank_t i=0; i<count; i++ ) {
            if (i!=pos)
                newgrp->nodes[j++] = LINK(nodes[i]);
        }
        return newgrp;
    }

    virtual IGroup *rotate(int num) const 
    {
        CGroup *newgrp = new CGroup(count);
        bool neg = false;
        if (num<0) {
            num=-num;
            neg = true;
        }
        unsigned j=(num%count);
        for( rank_t i=0; i<count; i++ ) {
            if (neg)
                newgrp->nodes[i] = LINK(nodes[j]);
            else
                newgrp->nodes[j] = LINK(nodes[i]);
            j++;
            if (j==count)
                j = 0;
        }
        return newgrp;
    }


    INodeIterator *getIterator(rank_t start=0,rank_t num=RANK_NULL) const ;

    INode &queryNode(rank_t r) const 
    {
        assertex(r<count);
        return *nodes[r];
    }
    INode *getNode(rank_t r) const 
    {
        assertex(r<count);
        return LINK(nodes[r]);
    }

    StringBuffer &getText(StringBuffer &text) const 
    {
        if (!count)
            return text;
        if (count==1)
            return nodes[0]->endpoint().getUrlStr(text);
        // following is rather slow maybe could do with more direct method with pointers TBD
        SocketEndpointArray epa;
        for(unsigned i=0;i<count;i++) {
            SocketEndpoint ep(nodes[i]->endpoint()); // pretty horrible
            epa.append(ep);
        }
        return epa.getText(text);
    }

    static IGroup *fromText(const char *s,unsigned defport) 
    {
        SocketEndpointArray epa;
        epa.fromText(s,defport);
        ForEachItemIn(idx, epa)
        {
            if (!epa.item(idx).isNull())
                return createIGroup(epa);
        }
        throw MakeStringException(0, "Invalid group %s (all nodes null)", s);
    }

    void serialize(MemoryBuffer &tgt) const 
    {
        tgt.append(count);
        for (rank_t i=0; i<count; i++)
            nodes[i]->serialize(tgt);
    }
    static CGroup *deserialize(MemoryBuffer &src) 
    {
        CGroup *ret = new CGroup(0);
        src.read(ret->count);
        if (ret->count) {
            ret->nodes = ret->count?(INode **)malloc(ret->count*sizeof(INode *)):NULL;
            for (rank_t i=0; i<ret->count; i++) 
                ret->nodes[i] = MPNode::deserialize(src);
        }
        return ret;
    }

    void getSocketEndpoints(SocketEndpointArray &sea) const
    {
        for (rank_t i=0; i<count; i++) 
            sea.append((SocketEndpoint &)nodes[i]->endpoint());
    }

};



class CNodeIterator : implements INodeIterator, public CInterface
{
    Linked<IGroup> parent;
    rank_t start;
    rank_t num;
    rank_t pos;
public:
    IMPLEMENT_IINTERFACE;
    CNodeIterator(IGroup *_parent,rank_t _start,rank_t _num)
        :   parent(_parent)
    {
        start = _start;
        num = _num;
        if ((num==RANK_NULL)||(num+start>parent->ordinality()))
            num = (start<parent->ordinality())?parent->ordinality()-start:0;
    }
    bool first()
    {
        pos = 0;
        return (pos<num);
    }
    bool next()
    {
        pos++;
        return (pos<num);
    }
    bool isValid()
    {
        return (pos<num);
    }
    INode &query()
    {
        return parent->queryNode(start+pos);
    }
    INode &get()
    {
        return *parent->getNode(start+pos);
    }
};

INodeIterator *CGroup::getIterator(rank_t start,rank_t num) const
{
    return new CNodeIterator((IGroup *)this,start,num);
}



INode *deserializeINode(MemoryBuffer &src)
{
    return MPNode::deserialize(src);
}

INode *createINode(const SocketEndpoint &ep)
{
    if (NodeCache)
        return NodeCache->lookup(ep);
    return new MPNode(ep);
}

INode *createINodeIP(const IpAddress &ip,unsigned short port=0)
{
    SocketEndpoint ep(port,ip);
    return createINode(ep);
}

INode *createINode(const char *name,unsigned short port)
{
    SocketEndpoint ep(name,port);
    return createINode(ep);
}


IGroup *createIGroup(rank_t num,INode **nodes)
{
    return new CGroup(num,nodes);
}

IGroup *createIGroup(rank_t num,const SocketEndpoint *ep)
{
    return new CGroup(num,ep);
}

IGroup *createIGroup(SocketEndpointArray &epa)
{
    return new CGroup(epa);
}

IGroup *createIGroup(const char *endpointlist,unsigned short defport)
{
    const char *s = endpointlist;
    bool oldform=false;
    if (s)
        while (*s) {
            if (*s=='|') {
                oldform = true;
                break;
            }
            if (*s==',') {
                while (isspace(*s)) 
                    s++;
                if ((*s=='=')||(*s=='*')) {
                    oldform = true;
                    break;
                }
            }
            s++;
        }
    if (oldform) {
        SocketListParser list(endpointlist);
        SocketEndpointArray eparray;
        list.getSockets(eparray,defport);
        return createIGroup(eparray);
    }
    return CGroup::fromText(endpointlist,defport);
}

IGroup *deserializeIGroup(MemoryBuffer &src)
{
    return CGroup::deserialize(src);
}



void initMyNode(unsigned short port)
{
    setNodeCaching(port != 0);
    ::Release(MyNode);
    MyNode = NULL;
    if (port) {
        SocketEndpoint ep(port);
        MyNode = new MPNode(ep);
        if (ep.isLoopBack()) {
            OWARNLOG("MP Warning - localhost used for MP host address, NIC adaptor not identified");
        }
        queryNullNode();
    }
    else
    {
        ::Release(NullNode);
        NullNode = NULL;
    }
}

INode *queryMyNode()
{
    return MyNode;
}

INode *queryNullNode()
{
    if (!NullNode) {
        SocketEndpoint ep;
        NullNode = new MPNode(ep);
    }
    return NullNode;
}

void setNodeCaching(bool on)
{
    if (on) {
        if (!NodeCache)
            NodeCache = new MPNodeCache();
    }
    else { 
        MPNodeCache *nc = NodeCache;
        NodeCache = NULL;
        delete nc;
    }
}

