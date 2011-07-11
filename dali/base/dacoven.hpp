/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

