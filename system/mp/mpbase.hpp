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

// Documentation for this file can be found at http://mgweb.mg.seisint.com/WebHelp/mp/html/mpbase_hpp.html


#ifndef MPBASE_HPP
#define MPBASE_HPP

#ifndef mp_decl
#define mp_decl DECL_IMPORT
#endif

#include "jutil.hpp"
#include "jsocket.hpp"


interface INode;
interface INodeIterator;

interface INodeCompare
{
    virtual int docompare(const INode *left, const INode *right) const;
};


typedef unsigned rank_t;

#define RANK_NULL       ((rank_t)-1)
#define RANK_ALL        ((rank_t)-2)
#define RANK_RANDOM     ((rank_t)-3)
#define RANK_ALL_OTHER  ((rank_t)-4)

enum GroupRelation { GRidentical, GRdifferentorder, GRbasesubset, GRwrappedsuperset, GRsubset, GRsuperset, GRintersection, GRdisjoint };

interface IGroup: extends IInterface
{
    virtual rank_t ordinality() const =0;
    virtual rank_t rank() const =0;                             // rank of calling process (RANK_NULL if not in group)
    virtual rank_t rank(INode *node) const =0;                  // rank of node (RANK_NULL if not in group)
    virtual rank_t rank(const SocketEndpoint &ep) const =0;     // rank of node (RANK_NULL if not in group)

    virtual bool isMember() const =0;                           // true if calling member is in group
    virtual bool isMember(INode *node) const =0;            // true if node member of group

    virtual GroupRelation compare(const IGroup *grp) const =0;  // compares two groups
    virtual bool equals(const IGroup *grp) const =0;            // faster than compare
    virtual bool overlaps(const IGroup *grp) const =0;          // true if intersection non-empty


    virtual void translate(const IGroup *othergroup, rank_t nranks, const rank_t *otherranks, 
                           rank_t *resranks )  const =0;        // translate ranks in othergroup to this group ranks

    // NB *** all the below do not change the group but return a new group as result ***
    virtual IGroup *subset(rank_t start,rank_t num) const =0;       // selection    
    virtual IGroup *subset(const rank_t *order,rank_t num) const =0;// re-ordered selection 
    virtual IGroup *combine(const IGroup *) const =0;               // union
    virtual IGroup *diff(const IGroup *) const =0;                  // difference
    virtual IGroup *intersect(const IGroup *) const =0;             // intersection
    virtual IGroup *add(INode *node) const =0;              // appends a node        
    virtual IGroup *add(INode *node,unsigned pos) const =0; // inserts a node at pos
    virtual IGroup *remove(unsigned pos) const =0;              // removes a node        
    virtual IGroup *swap(rank_t,rank_t) const =0;               // reorder
    virtual IGroup *rotate(int num) const =0;                   // rotate (+ve or -ve)

    virtual INodeIterator *getIterator(rank_t start=0,rank_t num=RANK_NULL) const = 0;
    virtual INode &queryNode(rank_t) const = 0;
    virtual INode *getNode(rank_t) const  = 0;
    virtual void serialize(MemoryBuffer &tgt) const = 0;
    virtual StringBuffer &getText(StringBuffer &text) const = 0;
    virtual void getSocketEndpoints(SocketEndpointArray &sea) const = 0;

    virtual unsigned distance(const IpAddress &ip) const = 0;
    virtual unsigned distance(const IGroup *grp) const = 0;

};

interface IGroupResolver: extends IInterface
{
    virtual IGroup *lookup(const char *logicalgroupname) = 0;
    virtual bool find(IGroup *grp,StringBuffer &name, bool add=false) = 0;
};

interface INode: extends IInterface
{
    virtual const SocketEndpoint &endpoint() const = 0;
    virtual bool equals(const INode *node) const = 0;
    virtual unsigned getHash() const = 0;
    virtual bool isLocalTo(INode *node) const = 0; // is same machine as node 
    virtual bool isHost() const = 0;                // is same machine as running this process
    virtual void serialize(MemoryBuffer &tgt) = 0;

};

interface INodeIterator : extends IInterface
{
public:
  virtual bool first() = 0;
  virtual bool next() = 0;
  virtual bool isValid() = 0;
  virtual INode &query() = 0;
  virtual INode &get() = 0;
};

#define ForEachNodeInGroup(x,y)  rank_t numItems##x = (y).ordinality();         \
                                 for(rank_t x = 0; x < numItems##x; ++x)

#define ForEachOtherNodeInGroup(x,y)  rank_t numItems##x = (y).ordinality();    \
                                      rank_t myrank##x = (y).rank();            \
                                      for(rank_t x = 0; x < numItems##x; ++x)   \
                                        if (x!=myrank##x)


extern mp_decl INode *createINode(const SocketEndpoint &ep);
extern mp_decl INode *createINodeIP(const IpAddress &ip,unsigned short port);
extern mp_decl INode *createINode(const char *name,unsigned short port=0);
extern mp_decl INode *deserializeINode(MemoryBuffer &src);

extern mp_decl IGroup *createIGroup(rank_t num,INode **);
extern mp_decl IGroup *createIGroup(rank_t num,const SocketEndpoint *); 
extern mp_decl IGroup *createIGroup(SocketEndpointArray &);
extern mp_decl IGroup *createIGroup(const char *endpointlist,unsigned short defport=0); // takes socketendpointlist or result of toText
constexpr unsigned defaultGroupResolveTimeout = 10*1000*60;
extern mp_decl IGroup *createIGroupRetry(const char *endpointlist,unsigned short defport, unsigned timeout = defaultGroupResolveTimeout);
extern mp_decl IGroup *deserializeIGroup(MemoryBuffer &src); 

extern mp_decl INode *queryNullNode(); 
extern mp_decl INode * queryMyNode();
extern mp_decl void initMyNode(unsigned short port);

// Exceptions

interface mp_decl IMP_Exception: extends IException
{
    virtual const SocketEndpoint &queryEndpoint() const = 0;
};

enum MessagePassingError
{
    MPERR_ok,
    MPERR_connection_failed,            // connection dropped (or could not be made)
    MPERR_process_not_in_group,         // using an 'inner' communicator when not part of it's group 
    MPERR_protocol_version_mismatch,    // incompatible version of MP being used
    MPERR_link_closed                   // raised if other end closed (e.g. aborted) during a specific recv or probe
};


extern mp_decl void setNodeCaching(bool on);

#endif
