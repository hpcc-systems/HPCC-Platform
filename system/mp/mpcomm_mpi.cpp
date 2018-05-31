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

/* TBD
    lost packet disposal
    synchronous send
    connection protocol (HRPC);
    look at all timeouts
*/

#include "platform.h"
#include "portlist.h"
//#include "jlib.hpp"
#include <limits.h>

//#include "jsocket.hpp"
//#include "jmutex.hpp"
//#include "jutil.hpp"
//#include "jthread.hpp"
//#include "jqueue.tpp"
//#include "jsuperhash.hpp"
//#include "jmisc.hpp"
#include <unistd.h>

#include "mpcomm.hpp"
#include "mpbuff.hpp"
#include "mputil.hpp"
#include "mplog.hpp"

#include "mpi_wrapper.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

//#define _TRACE
//#define _FULLTRACE

//#define _TRACEMPSERVERNOTIFYCLOSED
#define _TRACEORPHANS


#define REFUSE_STALE_CONNECTION


#define MP_PROTOCOL_VERSION    0x102                   
#define MP_PROTOCOL_VERSIONV6   0x202                   // extended for IPV6

// These should really be configurable
#define CANCELTIMEOUT       1000             // 1 sec
#define CONNECT_TIMEOUT          (5*60*1000) // 5 mins
#define CONNECT_READ_TIMEOUT     (10*1000)   // 10 seconds. NB: used by connect readtms loop (see loopCnt)
#define CONNECT_TIMEOUT_INTERVAL 1000        // 1 sec
#define CONNECT_RETRYCOUNT       180         // Overall max connect time is = CONNECT_RETRYCOUNT * CONNECT_READ_TIMEOUT
#define CONNECT_TIMEOUT_MINSLEEP 2000        // random range: CONNECT_TIMEOUT_MINSLEEP to CONNECT_TIMEOUT_MAXSLEEP milliseconds
#define CONNECT_TIMEOUT_MAXSLEEP 5000

#define CONFIRM_TIMEOUT          (90*1000) // 1.5 mins
#define CONFIRM_TIMEOUT_INTERVAL 5000 // 5 secs
#define TRACESLOW_THRESHOLD      1000 // 1 sec

#define VERIFY_DELAY            (1*60*1000)  // 1 Minute
#define VERIFY_TIMEOUT          (1*60*1000)  // 1 Minute

#define DIGIT1 U64C(0x10000000000) // (256ULL*256ULL*256ULL*65536ULL)
#define DIGIT2 U64C(0x100000000)   // (256ULL*256ULL*65536ULL)
#define DIGIT3 U64C(0x1000000)     // (256ULL*65536ULL)
#define DIGIT4 U64C(0x10000)       // (65536ULL)

#define _TRACING

class NodeCommunicator: public ICommunicator, public CInterface
{
    NodeGroup *group;
    bool outer;
    rank_t myrank;

public:
    IMPLEMENT_IINTERFACE;

    bool send(CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout){ 
        /***         
         * if dstrank == self 
         *      1.1 duplicate the message and initialize sender as self
         *      1.2 Queue the message in receiver queue
         * else
         *      2.1 dstrank={RANK_ALL, RANK_ALL_OTHER, RANK_RANDOM, NODE_RANK}
         *              => start_rank=?, end_rank=?, 
         *      2.2 Send message to all ranks from start_rank to end_rank until timeout
         * return (invalid dstrank) or (timeout expired)
         */
        hpcc_mpi::CommRequest req = hpcc_mpi::sendData(dstrank, tag, mbuf, *group, true);
        while (hpcc_mpi::getCommStatus(req) == hpcc_mpi::CommStatus::INCOMPLETE){
            usleep(100);
        }
        hpcc_mpi::releaseComm(req);
        
        return true;
    }

    bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=MP_WAIT_FOREVER){
        /*
         * 1. Wait for the message to be received, canceled or timeout
         * 2. Update the sender if sender is valid
         */
        hpcc_mpi::CommRequest req = hpcc_mpi::readData(srcrank, tag, mbuf, *group, true);
        while (hpcc_mpi::getCommStatus(req) == hpcc_mpi::CommStatus::INCOMPLETE){
            usleep(100);
        }
        hpcc_mpi::releaseComm(req);
        
        return true;
    }
    
    void barrier(void){
        hpcc_mpi::barrier(*group);
    }

    bool verifyConnection(rank_t rank,  unsigned timeout){
        UNIMPLEMENTED;
    }

    bool verifyAll(bool duplex, unsigned timeout){
        UNIMPLEMENTED;
    }

    unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=0){
        /*
         * if there is a message waiting from a srcrank (within timeout)
         *      set the sender to the rank
         *      return true
         * else
         *      return false
         */    
        if (hpcc_mpi::hasIncomingMessage(srcrank, tag, *group)){
            if (sender)
                *sender = srcrank;
        }
        return false;
    }
    
    void flush(mptag_t tag){
        // Handled by MPI
    }

    IGroup &queryGroup() { 
        UNIMPLEMENTED;
    }
    
    IGroup *getGroup()  { 
        return group;
    }

    bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER){
        /*
         * 1. Determine valid sendrank (assign if its RANK_RANDOM)
         * 2. Perform send within timeout
         * 3. clear mbuff to be used as receive buffer
         * 4. Perform receive within timeout
         * 5. Return true if all operations are success
         */
        UNIMPLEMENTED;
    }

    bool reply(CMessageBuffer &mbuf, unsigned timeout=MP_WAIT_FOREVER){
        /*
         * If mbuf has a sender
         *      send mbuf to to the sender with the replytag
         * else
         *      WHAT SHOULD WE DO???
         */
        UNIMPLEMENTED;
    }

    void cancel(rank_t srcrank, mptag_t tag){
        /*
         * Add a cancel message from a srcrank to receive queue
         */        
        UNIMPLEMENTED;
    }

    void disconnect(INode *node){
        UNIMPLEMENTED;
    }

    NodeCommunicator(IGroup *_group){
        //TODO throw meaningful exception if _group is not NodeGroup type
        this->group = (NodeGroup*) _group;
    }
    
    ~NodeCommunicator(){
    }

};


///////////////////////////////////

IMPServer *startNewMPServer(unsigned port){
    UNIMPLEMENTED;
}


MODULE_INIT(INIT_PRIORITY_STANDARD){
    return true;
}

MODULE_EXIT(){
  
}

void startMPServer(unsigned port, bool paused){
    hpcc_mpi::initialize();
}

void stopMPServer(){
    hpcc_mpi::finalize();
}

bool hasMPServerStarted(){
    UNIMPLEMENTED;
}


mptag_t createReplyTag(){
    UNIMPLEMENTED;
}

ICommunicator *createCommunicator(IGroup *group, bool outer){
    return new NodeCommunicator(group);
}

IInterCommunicator &queryWorldCommunicator(){
    UNIMPLEMENTED;
}

StringBuffer &getReceiveQueueDetails(StringBuffer &buf){
    UNIMPLEMENTED;
}

void addMPConnectionMonitor(IConnectionMonitor *monitor){
    UNIMPLEMENTED;
}

void removeMPConnectionMonitor(IConnectionMonitor *monitor){
    UNIMPLEMENTED;
}

IMPServer *getMPServer(){
    UNIMPLEMENTED;
}


void registerSelfDestructChildProcess(HANDLE handle){
    UNIMPLEMENTED;
}

void unregisterSelfDestructChildProcess(HANDLE handle){
    UNIMPLEMENTED;
}
