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

#include "mpicomm.hpp"
#include "mpbuff.hpp"
#include "mputil.hpp"
#include "mplog.hpp"

#include "mpi_wrapper.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

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
    Owned<IGroup> group;
    rank_t myrank;
    rank_t commSize;
    MPI::Comm& comm;
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
        rank_t startrank = dstrank;
        rank_t endrank;
        if (dstrank==RANK_ALL || dstrank==RANK_ALL_OTHER)
        {
            startrank = 0;
            endrank = commSize-1;
        } else if (dstrank==RANK_RANDOM)
        {
            if (commSize>1)
            {
                do
                {
                    startrank = getRandom()%commSize;
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
        _T("commSize="<<commSize);
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
        bool messageFromSelf = (srcrank == myrank);
        bool success = false;

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
                mbuf.clear();
                assertex((msg->getMessage()) != NULL);
                mptag_t reply = msg->getMessage()->getReplyTag();
                mbuf.transferFrom(*(msg->getMessage()));
                mbuf.init(msg->getMessage()->getSender(),tag,reply);
                delete msg;
                success = true;
            }
        }
        if (!messageFromSelf)
        {
            tm.timedout(&remaining);
            hpcc_mpi::CommStatus status = hpcc_mpi::readData(srcrank, tag, mbuf, comm, remaining);
            _T("recv status="<<status);
            success = (status == hpcc_mpi::CommStatus::SUCCESS);
            //TODO what if no message received and selfMsg bcomes available now?
        }
        if (success)
        {
            const SocketEndpoint &ep = getGroup()->queryNode(srcrank).endpoint();
            mbuf.init(ep, tag, TAG_REPLY_BASE);
            if (sender)
                *sender = srcrank;
            mbuf.reset();
        }
        return success;
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
        _T("replytag="<<replytag);
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
        rank_t dstrank = getGroup()->rank(mbuf.getSender());
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
        return *(getGroup());
    }
    
    IGroup *getGroup()
    {
        return group.getLink();
    }

    NodeCommunicator(IGroup *_group, MPI::Comm& _comm): comm(_comm),  group(_group)
    {
        initializeMPI(comm);

        commSize = hpcc_mpi::size(comm);
        myrank = hpcc_mpi::rank(comm);

        assertex(getGroup()->ordinality()==commSize);
    }
    
    ~NodeCommunicator()
    {
        terminateMPI();
    }
};

ICommunicator *createMPICommunicator(IGroup *group)
{
    MPI::Comm& comm = MPI::COMM_WORLD;
    if (group)
        group->Link();
    else
    {
        initializeMPI(comm);
        int size = hpcc_mpi::size(comm);
        terminateMPI();
        INode* nodes[size];
        for(int i=0; i<size; i++)
        {
            SocketEndpoint ep(i);
            nodes[i] = createINode(ep);
        }
        group = LINK(createIGroup(size, nodes));
    }
    return new NodeCommunicator(group, comm);
}

int mpiInitCounter = 0;
CriticalSection initCounterBlock;

void initializeMPI(MPI::Comm& comm)
{
    //Only initialize the framework once
    CriticalBlock block(initCounterBlock);
    if (!mpiInitCounter)
        hpcc_mpi::initialize(true);
    hpcc_mpi::setErrorHandler(comm, MPI::ERRORS_THROW_EXCEPTIONS);

    mpiInitCounter++;
}

void terminateMPI()
{
    //Only finalize the framework once when everyone had requested to finalize it.
    CriticalBlock block(initCounterBlock);
    mpiInitCounter--;
    if (mpiInitCounter == 0)
        hpcc_mpi::finalize();
    assertex(mpiInitCounter>=0);
}
