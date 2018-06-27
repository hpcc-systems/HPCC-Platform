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
#include <vector>
#include <limits.h>

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
private:
    class SelfMessage
    {
        mptag_t mptag;
        CMessageBuffer* message = NULL;
    public:
        SelfMessage(mptag_t _tag, CMessageBuffer* _message): mptag(_tag), message(_message){};
        bool isTag(mptag_t _tag)
        {
            return mptag == _tag;
        }
        CMessageBuffer *getMessage()
        {
            return message;
        }
        mptag_t tag()
        {
            return mptag;
        }

        ~SelfMessage()
        {
            if (message){
                delete message;
            }
        }
    };
    IGroup *group;
    rank_t myrank;
    MPI_Comm comm;
    std::vector<SelfMessage*> selfMessages;
    CriticalSection selfMessagesLock;
private:
    void addSelfMessage(SelfMessage *selfMessage)
    {
        _TF("addSelfMessage", selfMessage->tag());
        CriticalBlock block(selfMessagesLock);
        selfMessages.push_back(selfMessage);
    }

    SelfMessage *popSelfMessage(mptag_t tag)
    {
        _TF("popSelfMessage", tag);
        CriticalBlock block(selfMessagesLock);
        int index = -1;
        for(int i=0; i< selfMessages.size(); i++)
        {
            if (selfMessages[i]->isTag(tag))
            {
                index = i;
                break;
            }
        }
        SelfMessage *ret = NULL;
        if (index != -1)
        {
            ret = selfMessages[index];
            selfMessages.erase(selfMessages.begin()+index);
        }
        return ret;
    }

public:
    IMPLEMENT_IINTERFACE;

    bool send(CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout)
    {
        _TF("send", dstrank, tag, timeout);
        assertex(dstrank!=RANK_NULL);
        CTimeMon tm(timeout);
        rank_t myrank = group->rank();
        rank_t startrank = dstrank;
        rank_t endrank;
        if (dstrank==RANK_ALL || dstrank==RANK_ALL_OTHER)
        {
            startrank = 0;
            endrank = group->ordinality()-1;
        } else if (dstrank==RANK_RANDOM)
        {
            if (group->ordinality()>1)
            {
                do
                {
                    startrank = getRandom()%group->ordinality();
                } while (startrank==myrank);
            } else
            {
                assertex(myrank!=0);
                startrank = 0;
            }
            endrank = startrank;
        } else {
            endrank = startrank;
        }
        for (;startrank<=endrank;startrank++)
        {
            if (startrank==myrank)
            {
                if (dstrank !=RANK_ALL_OTHER)
                {
                    CMessageBuffer *ret = mbuf.clone();
                    addSelfMessage(new SelfMessage(tag, ret));
                }
            }else
            {
                unsigned remaining;
                if (tm.timedout(&remaining))
                    return false;
                hpcc_mpi::CommStatus status = hpcc_mpi::sendData(startrank, tag, mbuf, comm, remaining);
                if (status != hpcc_mpi::CommStatus::SUCCESS)
                    return false;
            }
        }
       
        return true;
    }

    bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=MP_WAIT_FOREVER)
    {
        _TF("recv", srcrank, tag, timeout);
        CTimeMon tm(timeout);
        unsigned remaining;
        bool messageFromSelf = (srcrank == group->rank());
        bool completed = false;

        if (messageFromSelf || srcrank == RANK_ALL)
        {
            SelfMessage *msg = NULL;
            while (msg == NULL && !tm.timedout(&remaining))
            {
                msg = popSelfMessage(tag);
                if (!messageFromSelf)
                    break; //if its not specifically from self, we should continue checking from other nodes
            }
            if (msg)
            {
                _T("Message found in self message list");
                mbuf.clear();
                _T("Buffer cleared");
                assertex((msg->getMessage()) != NULL);
                _T("Assert passed");
                mptag_t reply = msg->getMessage()->getReplyTag();
                _T("Reply tag from message = "<<reply);
                mbuf.transferFrom(*(msg->getMessage()));
                _T("Data copied to buffer");
                mbuf.init(msg->getMessage()->getSender(),tag,reply);
                _T("Initialized buffer");
                delete msg;
                completed = true;
            }
        }
        if (!messageFromSelf)
        {
            tm.timedout(&remaining);
            hpcc_mpi::CommStatus status = hpcc_mpi::readData(srcrank, tag, mbuf, comm, remaining);
            _T("recv status="<<status);
            completed = (status == hpcc_mpi::CommStatus::SUCCESS);
            //TODO what if no message received and selfMsg bcomes available now?
        }
        if (completed)
        {
            mbuf.reset();
        }
        return completed;
    }
    
    void barrier(void)
    {
        hpcc_mpi::barrier(comm);
    }

    unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender, unsigned timeout=0)
    {
        _TF("probe", srcrank, tag, timeout);
        if (hpcc_mpi::hasIncomingMessage(srcrank, tag, comm))
        {
            if (sender)
                *sender = srcrank;
        }
        return false;
    }
    
    void flush(mptag_t tag)
    {
        // Handled by MPI
    }

    bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER)
    {
        _TF("sendRecv", sendrank, sendtag, timeout);
        //TODO share timeout between send/recv?
        mptag_t replytag = createReplyTag();
        CTimeMon tm(timeout);
        mbuff.setReplyTag(replytag);
        unsigned remaining;
        if (tm.timedout(&remaining))
            return false;
        if (!send(mbuff,sendrank,sendtag,remaining)||tm.timedout(&remaining))
            return false;
        mbuff.clear();
        return recv(mbuff,sendrank,replytag,NULL,remaining);
    }

    bool reply(CMessageBuffer &mbuf, unsigned timeout=MP_WAIT_FOREVER)
    {
        _TF("reply", mbuf.getReplyTag(), timeout);
        mptag_t replytag = mbuf.getReplyTag();
        rank_t dstrank = group->rank(mbuf.getSender());
        if (dstrank!=RANK_NULL)
        {
            if (send (mbuf, dstrank, replytag,timeout))
            {
                mbuf.setReplyTag(TAG_NULL);
                return true;
            }
            return false;
        }
        return false;
    }

    void cancel(rank_t srcrank, mptag_t tag)
    {
        _TF("cancel", srcrank, tag);
        assertex(srcrank!=RANK_NULL);
        //cancel only recv calls?
        hpcc_mpi::cancelComm(false, srcrank, tag, comm);
    }

    bool verifyConnection(rank_t rank,  unsigned timeout)
    {
        UNIMPLEMENTED;
    }

    bool verifyAll(bool duplex, unsigned timeout)
    {
        UNIMPLEMENTED;
    }
    
    void disconnect(INode *node)
    {
        UNIMPLEMENTED;
    }

    IGroup &queryGroup()
    {
        return *group;
    }
    
    IGroup *getGroup()
    {
        return group;
    }

    NodeCommunicator(IGroup *_group, MPI_Comm _comm)
    {
        this->group = _group;
        this->comm = _comm;
        myrank = this->group->rank();
    }
    
    ~NodeCommunicator()
    {
    }

};


///////////////////////////////////

IMPServer *startNewMPServer(unsigned port)
{
    UNIMPLEMENTED;
}


MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
  
}

void startMPServer(unsigned port, bool paused)
{
    hpcc_mpi::initialize(true);
}

void stopMPServer()
{
    hpcc_mpi::finalize();
}

bool hasMPServerStarted()
{
    UNIMPLEMENTED;
}
CriticalSection replyTagSect;
byte RTsalt = 0xff;
int rettag;
mptag_t createReplyTag()
{
    UNIMPLEMENTED;
    mptag_t ret;
    {
        CriticalBlock block(replyTagSect);
        if (RTsalt==0xff)
        {
            RTsalt = (byte)(getRandom()%16);
            rettag = (int)TAG_REPLY_BASE-RTsalt;
        }
        if (rettag>(int)TAG_REPLY_BASE)
        {           // wrapped
            rettag = (int)TAG_REPLY_BASE-RTsalt;
        }
        ret = (mptag_t)rettag;
        rettag -= 16;
    }
    //TODO flush
    //flush(ret);
    return ret;
}

ICommunicator *createCommunicator(IGroup *group, bool outer)
{
    int size = hpcc_mpi::size(MPI_COMM_WORLD);
    int rank = hpcc_mpi::rank(MPI_COMM_WORLD);
    INode* nodes[size];
    for(int i=0; i<size; i++)
    {
        SocketEndpoint ep(i);
        nodes[i] = createINode(ep);
    }
    initMyNode(rank);
    IGroup* _group = createIGroup(size, nodes);
    return new NodeCommunicator(_group, MPI_COMM_WORLD);
}

IInterCommunicator &queryWorldCommunicator()
{
    UNIMPLEMENTED;
}

StringBuffer &getReceiveQueueDetails(StringBuffer &buf)
{
    UNIMPLEMENTED;
}

void addMPConnectionMonitor(IConnectionMonitor *monitor)
{
    UNIMPLEMENTED;
}

void removeMPConnectionMonitor(IConnectionMonitor *monitor)
{
    UNIMPLEMENTED;
}

IMPServer *getMPServer()
{
    UNIMPLEMENTED;
}

void registerSelfDestructChildProcess(HANDLE handle)
{
    UNIMPLEMENTED;
}

void unregisterSelfDestructChildProcess(HANDLE handle)
{
    UNIMPLEMENTED;
}
