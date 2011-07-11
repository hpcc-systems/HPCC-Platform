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

#ifndef DANQS_HPP
#define DANQS_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "mpbuff.hpp"
#include "dasess.hpp"
#include "dasubs.hpp"


interface INamedQueueSubscription: extends IInterface
{
    virtual void notify(const char *name,MemoryBuffer &buf) = 0; 
        // buffer contains queue item
    virtual void abort() = 0; // called if Dali Server closes down
};


interface IQueueChannel: extends IInterface
{
    virtual void put(MemoryBuffer &buf, int priority=0) = 0;    
                                                        // puts message on queue, buf is clear on return
    virtual bool get(MemoryBuffer &buf, int priority=0, unsigned timeout=WAIT_FOREVER) = 0; 
                                                        // gets message from queue
    virtual void cancelGet() = 0;                       // called from other thread to cancel ongoing get 
    virtual SubscriptionId subscribe(INamedQueueSubscription *subs, int priority, bool oneshot=false) = 0;
                                                        // cause notify to be called when item added to queue
    virtual void cancelSubscription(SubscriptionId id) = 0; // called from other thread to cancel subscription 
    virtual int changePriority(int newpriority,SubscriptionId id=0) = 0;        
                                                        // called from other thread to change priority of ongoing get (id=0)or subscribe
    virtual unsigned probe() = 0;                       // enquires number of queue items available
    virtual unsigned probe(MemoryBuffer &buf,PointerArray &ptrs) = 0; // probes contents of queue
};


interface INamedQueueConnection: extends IInterface
{
    virtual IQueueChannel *open(const char *qname) = 0;
    virtual void startTransaction() = 0;
    virtual bool commit() = 0;    // returns success, false indicates not in frame, a failed connection, or existence of pending GETs
    virtual bool rollback() = 0;  // ditto
};


extern da_decl INamedQueueConnection *createNamedQueueConnection(SecurityToken tok);

// for server use
interface IDaliServer;
extern da_decl IDaliServer *createDaliNamedQueueServer(); // called for coven members

extern da_decl unsigned queryDaliNamedQueueCount(); // function will fail client-side
extern da_decl unsigned queryDaliNamedQueueMeanLength(); // function will fail client-side
extern da_decl unsigned queryDaliNamedQueueMaxLength(); // function will fail client-side

#endif
