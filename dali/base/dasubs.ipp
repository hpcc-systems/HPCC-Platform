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

#ifndef DASUBS_IPP
#define DASUBS_IPP

#include "dasubs.hpp"

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

interface ISubscription: extends IInterface             
{
    virtual const MemoryAttr &queryData() = 0;
    virtual void notify(MemoryBuffer &returndata) = 0; 
    virtual void abort()=0; // called when daliserver closes
    virtual bool aborted()=0; // returns true if in an aborted state (prior to being removed)
};

interface ISubscriptionManager: extends IInterface       
{
    virtual void add(ISubscription *subs,SubscriptionId id) = 0;  // takes ownership
    virtual void remove(SubscriptionId id) = 0;
};


// Coven side

extern da_decl void registerSubscriptionManager(unsigned tag, ISubscriptionManager *manager); // takes ownership of manager


// Client side

extern da_decl ISubscriptionManager *querySubscriptionManager(unsigned tag);
extern da_decl void closeSubscriptionManager();



#define SESSION_PUBLISHER   1
#define SDS_PUBLISHER       2
#define NQS_PUBLISHER       3
#define SDSCONN_PUBLISHER   4
#define SDSEXACT_PUBLISHER  5
#define SDSNODE_PUBLISHER   6


#if 0
//----------------------------------------------------------------------

SDS Example:

On initialization coven side:
   registerSubscriptionManager(SDS_PUBLISHER,SdsSubscriptionManager);

On usage client side:
   scsconn->subscribe(xpath,mynotify) 
causes:
   ISubscription *sub = new SDSsub(xpath,notify,...) // wrapper
   querySubscriptionManager(SDS_PUBLISHER)->add(sub);

Which results in (on coven side on every server)
    SdsSubscriptionManager.add(newsubscription)


then when something changes (coven side on 1 server)
    newsubscription.notify(returndata);     // called

which results on client side
    sub.notify(returndata);     // called on new thread

and sub then calls mynotify with correct method/parameters

#endif


// for server use
interface IDaliServer;

extern da_decl IDaliServer *createDaliPublisherServer(); // called for coven members

extern da_decl StringBuffer &getSubscriptionList(StringBuffer &buf);

#endif



