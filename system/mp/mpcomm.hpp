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

#ifndef MPCOMM_HPP
#define MPCOMM_HPP

#ifndef mp_decl
#define mp_decl __declspec(dllimport)
#endif

#include "mpbase.hpp"
#include "mpbuff.hpp"
#include "mptag.hpp"

// timeout values
#define MP_WAIT_FOREVER ((unsigned)-1)
#define MP_ASYNC_SEND   ((unsigned)-2)


interface ICommunicator: extends IInterface
{
    virtual bool send (CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER) = 0;  
                            // blocking send (unless MP_ASYNC_SEND used for timeout), NB, mbuf clear on exit
                            // returns false if timedout

    virtual unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=0) = 0; 
                            //  default non-blocking, check message returns sender if message waiting
                            //  returns 0 if no message available in time given (0 for poll) otherwise number waiting
    
    virtual bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER) = 0;    
                            // receive, returns senders rank or false if no message available in time given or cancel called

    virtual IGroup &queryGroup() = 0;       //  query group for communicator
    virtual IGroup *getGroup() = 0;     //  link and return group for communicator

    virtual bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER) = 0;
    virtual bool reply   (CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER) = 0;
    virtual void cancel  (rank_t srcrank, mptag_t tag) = 0;   // cancels in-progress recvs

    virtual void flush  (mptag_t tag) = 0;    // flushes pending buffers

    virtual bool verifyConnection(rank_t rank, unsigned timeout=1000*60*5) = 0; // verifies connected to rank
    virtual bool verifyAll(bool duplex=false, unsigned timeout=1000*60*30) = 0;
    virtual void disconnect(INode *node) = 0;
    virtual void barrier() = 0;
};

interface IInterCommunicator: extends IInterface
// Non-grouped communication
{
    virtual bool send (CMessageBuffer &mbuf, INode *dst, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER) = 0;  
                            // blocking send (unless MP_ASYNC_SEND used for timeout), NB, mbuf clear on exit
                            // returns false if timedout

    virtual unsigned probe(INode *src, mptag_t tag, INode **sender=NULL, unsigned timeout=0) = 0; 
                            //  default non-blocking, check message returns sender if message waiting
                            //  returns 0 if no message available or recv cancelled, or number waiting
    
    virtual bool recv(CMessageBuffer &mbuf, INode *src, mptag_t tag, INode **sender=NULL, unsigned timeout=MP_WAIT_FOREVER) = 0;    
                            // receive, returns false if no message available in time given

    virtual bool sendRecv(CMessageBuffer &mbuff, INode *dst, mptag_t dsttag, unsigned timeout=MP_WAIT_FOREVER) = 0;
    virtual bool reply   (CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER) = 0;
    virtual void cancel  (INode *src, mptag_t tag) = 0;   // cancels in-progress recvs
    virtual void flush  (mptag_t tag) = 0;    // flushes pending buffers
    virtual bool verifyConnection  (INode *node, unsigned timeout=1000*60*5) = 0; // verifies connected to node
    virtual bool verifyAll(IGroup *group,bool duplex=false, unsigned timeout=1000*60*30) = 0;
    virtual void verifyAll(StringBuffer &log) = 0;
    virtual void disconnect(INode *node) = 0;
};


extern mp_decl mptag_t createReplyTag(); // creates (short-lived) reply-tag;


extern mp_decl ICommunicator *createCommunicator(IGroup *group,bool outer=false); // outer allows nodes outside group to send
extern mp_decl IInterCommunicator &queryWorldCommunicator();

interface IMPServer : extends IInterface
{
    virtual mptag_t createReplyTag() = 0;
    virtual ICommunicator *createCommunicator(IGroup *group, bool outer=false) = 0;
    virtual void stop() = 0;
    virtual INode *queryMyNode() = 0;
};

extern mp_decl void startMPServer(unsigned port,bool paused=false);
extern mp_decl void stopMPServer();
extern mp_decl IMPServer *getMPServer();
extern mp_decl IMPServer *startNewMPServer(unsigned port);

interface IConnectionMonitor: extends IInterface
{
    virtual void onClose(SocketEndpoint &ep)=0;
};

extern mp_decl void addMPConnectionMonitor(IConnectionMonitor *monitor);
extern mp_decl void removeMPConnectionMonitor(IConnectionMonitor *monitor);

extern mp_decl StringBuffer &getReceiveQueueDetails(StringBuffer &buf);

interface IMPProtocol: extends IInterface
{   
    virtual void send (CMessageBuffer &mbuf, rank_t dstrank)=0;  
    virtual void sendStop (rank_t dstrank)=0;
    virtual bool recv (CMessageBuffer &mbuf, rank_t &sender)=0; // returns false if stopped
    virtual void confirm (CMessageBuffer &mbuf)=0;              // confirms recv
    virtual unsigned remaining()=0; // number not stopped
};

extern mp_decl void registerSelfDestructChildProcess(HANDLE handle);
extern mp_decl void unregisterSelfDestructChildProcess(HANDLE handle);


#endif
