/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef DACOVEN_HPP
#define DACOVEN_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "jlog.hpp"
#include "jptree.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"

typedef __int64 DALI_UID;

interface ICoven: extends ICommunicator // ICoven can be used to communicate with the coven (or intra-coven)
{
    virtual unsigned        size()=0;                                           // number of processes in Coven
    virtual DALI_UID        getServerId(rank_t server)=0;
    virtual DALI_UID        getServerId()=0;                                    // my ID, 0 if not in coven
    virtual rank_t          getServerRank(DALI_UID id)=0;
    virtual rank_t          getServerRank()=0;                                  // my rank in coven (RANK_NULL if not in)
    virtual DALI_UID        getCovenId()=0;                                     // unique ID for coven

    virtual unsigned        getInitSDSNodes()=0;
    virtual void            setInitSDSNodes(unsigned e)=0;
    virtual bool            inCoven()=0;
    virtual bool            inCoven(INode *node)=0;

    // UID routines
    virtual DALI_UID        getUniqueId(SocketEndpoint *foreigndali=NULL)=0;                            // create unique ID
    virtual DALI_UID        getUniqueIds(unsigned num,SocketEndpoint *foreigndali=NULL)=0;              // create unique ID range
    virtual rank_t          chooseServer(DALI_UID uid, int tag=0)=0;    // choose a server deterministically based on UID and tag

    inline ICommunicator &queryComm() { return *this; }
};

class da_decl CDaliVersion
{
    unsigned major, minor;
public:
    CDaliVersion() { major = minor = 0; }
    CDaliVersion(const char *s) { set(s); }
    void set(const char *s);
    unsigned queryMinor() const { return minor; }
    unsigned queryMajor() const { return major; }
    int compare(const CDaliVersion &other) const;
    int compare(const char *other) const;
    StringBuffer &toString(StringBuffer &str) const;
};

extern da_decl ICoven &queryCoven();
extern da_decl bool isCovenActive();
extern da_decl const CDaliVersion &queryDaliServerVersion();
extern da_decl bool verifyCovenConnection(unsigned timeout=5*60*1000);
extern da_decl DALI_UID getGlobalUniqueIds(unsigned num,SocketEndpoint *_foreignnode);


#endif

