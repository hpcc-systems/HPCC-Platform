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

// DFDXREF environment support
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "daft.hpp"
#include "rmtfile.hpp"
#include "jptree.hpp"

#define SDS_LOCK_TIMEOUT 300000

struct CMachineEntry: public CInterface
{
    CMachineEntry(const char *_mname,SocketEndpoint _ep,bool _avail)
        : mname(_mname),ep(_ep)
    {
        avail = _avail;
    }
    StringAttr mname;
    SocketEndpoint ep;
    bool avail;
};

typedef CMachineEntry *CMachineEntryPtr;

MAKESTRINGMAPPING(CMachineEntryPtr, CMachineEntryPtr, CMachineEntryMap);
static  CMachineEntryMap machinemap;
static  CIArrayOf<CMachineEntry> machinelist;


void loadMachineMap()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Hardware", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> machines= root->getElements("Computer");
    if (machines->first()) {
        do {
            IPropertyTree &machine = machines->query();
            SocketEndpoint ep(machine.queryProp("@netAddress"));
            const char *name = machine.queryProp("@name");
            const char *state=machine.queryProp("@state");
            CMachineEntry *entry = new CMachineEntry(name,ep,!state||stricmp(machine.queryProp("@state"),"Available")==0);
            machinemap.setValue(name, entry);
            machinelist.append(*entry);
        } while (machines->next());
    }
}

void removeGroup(const char *groupname)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Groups", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    IPropertyTree* root = conn->queryRoot();
    bool deleted=false;
    do {
        deleted=false;
        Owned<IPropertyTreeIterator> grps= root->getElements("Group");
        if (grps->first()) {
            do {
                IPropertyTree &grp = grps->query();
                if (strcmp(grp.queryProp("@name"),groupname)==0) {
                    root->removeTree(&grp);
                    deleted = true;
                    break;
                }
            } while (grps->next());
        }
    } while (deleted);
}


void appendEndpoints(IPropertyTree& cluster,SocketEndpointArray &eps)
{
    SocketEndpoint ep;
    unsigned n=cluster.getPropInt("@slaves");
    unsigned i;
    unsigned base = eps.ordinality();
    for (i=0;i<n;i++)
        eps.append(ep);
    Owned<IPropertyTreeIterator> nodes= cluster.getElements("ThorSlaveProcess");
    if (nodes->first()) {
        i = 0;
        do {
            IPropertyTree &node = nodes->query();
            const char *computer = node.queryProp("@computer");
            CMachineEntryPtr *m = machinemap.getValue(computer);
            if (!m) {
                printf("Cannot construct %s, computer name %s not found\n",cluster.queryProp("@name"),computer);
                return;
            }
            if ((*m)->avail)
                printf("WARNING: In group %s, slave %s computer %s is marked available\n",cluster.queryProp("@name"),node.queryProp("@name"),computer);
            if (i<n) 
                eps.item(base+i) = (*m)->ep;
            else {
                ERRLOG("Cannot construct %s, Too many slaves defined [@slaves = %d, found slave %d]",cluster.queryProp("@name"),n,i+1);
                return;
            }
            i++;
        } while (nodes->next());
    }
}



void constructGroup(IPropertyTree& cluster)
{
    const char *groupname = cluster.queryProp("@name");
    SocketEndpointArray eps;
    appendEndpoints(cluster,eps);
    if (eps.ordinality()) {
        Owned<IGroup> grp = createIGroup(eps);
        Owned<IGroup> oldgrp = queryNamedGroupStore().lookup(groupname);
        if (grp->equals(oldgrp.get()))
            return;
        oldgrp.clear();
        removeGroup(groupname);
        queryNamedGroupStore().add(groupname,grp,true);
        printf("GROUP: %s updated\n",groupname);
    }
}

void constructGroups()
{
    loadMachineMap();
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> clusters= root->getElements("ThorCluster");
    if (clusters->first()) {
        do {
            IPropertyTree &cluster = clusters->query();
            constructGroup(cluster);
        } while (clusters->next());
    }
}

IGroup *getCompleteGroup()
{
    SocketEndpointArray eps;
    loadMachineMap();
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> clusters= root->getElements("ThorCluster");
    if (clusters->first()) {
        do {
            IPropertyTree &cluster = clusters->query();
            appendEndpoints(cluster,eps);
        } while (clusters->next());
    }
    return createIGroup(eps);
}


IGroup *getCompleteGroupFull()
{
    SocketEndpointArray eps;
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Hardware", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    IPropertyTree* root = conn->queryRoot();
    Owned<IPropertyTreeIterator> machines= root->getElements("Computer");
    if (machines->first()) {
        do {
            IPropertyTree &machine = machines->query();
            SocketEndpoint ep(machine.queryProp("@netAddress"));
            if (stricmp(machine.queryProp("@state"),"Available")!=0)
                eps.append(ep);
        } while (machines->next());
    }
    return createIGroup(eps);
}
