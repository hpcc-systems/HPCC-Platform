/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"
#include <cstdlib>
#include <queue>
#include "mputil.hpp"

#define WAIT_DELAY 100
#define PROBING_DELAY 100

//----------Functions and Data structures managing communication data related to Send/Recv Communications in orogress-----------//

// Data structure to keep the data relating to send/receive communications
class CommData
{
private:
    bool send;                      // TRUE => relates to a send communication | FALSE => relates to receive communication
public:    
    bool isSend(){return send;}
    bool isReceive(){return !isSend();}
    bool isEqual(bool _send, int _rank, int _tag, MPI_Comm _comm) {return (send==_send) && (_rank<0 || rank==_rank) && (_tag<0 || tag==_tag) && (comm == _comm);}

    void* data;                     // Data structure which points to the sent/recv buffer
    int size;                       // size of 'data'
    int rank;                       // source/destination rank of the processor
    int tag;                        // MPI tag infomation
    MPI_Request *request;           // persistent request object to keep track of ongoing MPI call
    MPI_Comm comm;                  // MPI communicator
    bool probingProgress = false;

    CommData(bool _send, int _rank, int _tag, int _size, MPI_Comm _comm):
        send(_send), size(_size), rank(_rank), tag(_tag), comm(_comm)
    {
        data = malloc(size);
        request = new MPI_Request();
    }

    ~CommData()
    {
        if (data) free(data);
        if (request) delete request;
    }
};

std::vector<CommData*> asyncCommData; // CommData list to manage while send/recv communication in progress
CriticalSection commDataLock;         // A mutex lock for the index list above

CommData* _popCommData(int index)
{
    _TF("_popCommData", index);
    CommData* ret = NULL;
    if (index != -1) {
        ret = asyncCommData[index];
        asyncCommData.erase(asyncCommData.begin() + index);
    }
    return ret;
}

void addCommData(CommData *commData)
{
    _TF("addCommData", commData->rank, commData->tag);
    CriticalBlock block(commDataLock);

    //TODO Do a cleanup while we are at it
//    int size = asyncCommData.size(); int completed; MPI_Status stat;
//    for(int i=(size-1); i>=0 ; i--)
//    {
//        if (!(asyncCommData[i]->probingProgress))
//        {
//            completed = 0;
//            assertex(asyncCommData[i]->request != NULL);
//            _T("asyncCommData[i]->request="<<*(asyncCommData[i]->request)<<" mem_address="<<asyncCommData[i]->request);
//            _T("send="<<asyncCommData[i]->isSend()<<" rank="<<asyncCommData[i]->rank<<" tag="<<asyncCommData[i]->tag);
//            bool error = (MPI_Test(asyncCommData[i]->request, &completed, &stat)!= MPI_SUCCESS);
//            if (completed || error) //unlikely an error would occur
//            {
//                delete _popCommData(i);
//            }
//        }
//    }

    asyncCommData.push_back(commData);
}

CommData* popCommData(int index)
{
    _TF("popCommData", index);
    CriticalBlock block(commDataLock);
    return _popCommData(index);
}

CommData *popCommData(bool send, int rank, int tag, MPI_Comm comm)
{
   _TF("popCommData", rank, tag);
   CriticalBlock block(commDataLock);
   int index = -1;
   for(int i=0; i< asyncCommData.size(); i++)
   {
       if (asyncCommData[i]->isEqual(send, rank, tag, comm))
       {
           index = i;
           break;
       }
   }
   return _popCommData(index);
}

CommData *popCommData(CommData * commData)
{
   _TF("popCommData(CommData * commData)", commData->rank, commData->tag, commData->comm);
   CriticalBlock block(commDataLock);
   int index = -1;
   for(int i=0; i< asyncCommData.size(); i++)
   {
       if (asyncCommData[i]==commData)
       {
           index = i;
           break;
       }
   }
   return _popCommData(index);
}


int getRank(rank_t sourceRank)
{
    if (sourceRank == RANK_ALL)
        return MPI_ANY_SOURCE;
    else
        return sourceRank;
}

int getTag(mptag_t mptag)
{
    if (mptag == TAG_ALL)
        return MPI_ANY_TAG;
    else
        return mptag;
}

MPI_Status waitToComplete(MPI_Request* req, bool& completed, bool& error, bool& canceled, bool& timedout, unsigned timeout, bool &keepProbing)
{
    _TF("mpi_wrapper:waitToComplete", completed, error, canceled, timeout);
    CTimeMon tm(timeout);
    MPI_Status stat;
    int flag;
    unsigned remaining;
    while (keepProbing && !(completed || error || (timedout = tm.timedout(&remaining))))
    {
        usleep(WAIT_DELAY);
        error = (MPI_Test(req, &flag, &stat) != MPI_SUCCESS);
        completed = (flag > 0);
    }
    if (completed)
    {
        MPI_Test_cancelled(&stat, &flag);
        canceled = (flag > 0);
    }else
    {
        canceled = !keepProbing;
    }

    return stat;
}

MPI_Status hasIncomingData(rank_t sourceRank, mptag_t mptag, MPI_Comm comm,
        bool& incomingMessage, bool& error, unsigned timeout = 0)
{
    _TF("mpi_wrapper:hasIncomingData", sourceRank, mptag, incomingMessage, error, timeout);
    CTimeMon tm(timeout);
    MPI_Status stat;
    int source = getRank(sourceRank);
    int tag = getTag(mptag);
    int flag;
    unsigned remaining;
    while (!(incomingMessage || error || (timeout !=0 && tm.timedout(&remaining))))
    {
        usleep(PROBING_DELAY);
        error = (MPI_Iprobe(source, tag, comm, &flag, &stat) != MPI_SUCCESS);
        incomingMessage = (flag > 0);
        if (timeout == 0) break;
    }
    return stat;
}

//----------------------------------------------------------------------------//

/** See mpi_wrapper.hpp header file for function descriptions of the following **/

bool hpcc_mpi::hasIncomingMessage(rank_t &sourceRank, mptag_t &mptag, MPI_Comm comm)
{
    _TF("mpi_wrapper:hasIncomingMessage", sourceRank, mptag);
    bool incomingMessage = false; bool error = false;
    MPI_Status stat = hasIncomingData(sourceRank, mptag, comm, incomingMessage, error);
    if (incomingMessage)
    {
        sourceRank = stat.MPI_SOURCE;
        mptag = mptag_t(stat.MPI_TAG);
    }

    return incomingMessage;
}

int cancelComm(CommData* commData) {
    int ret = true;
    if (commData) {
        commData->probingProgress = false; //Incase the main send/recv methods are waiting in loop for the comm. to complete, this will tell them to stop
        MPI_Status stat;
        int completed;
        bool error = (MPI_Test(commData->request, &completed, &stat)
                != MPI_SUCCESS);
        if (!error && !completed) {
            error = (MPI_Cancel(commData->request) != MPI_SUCCESS);
            if (!error) {
                // MPI framework managed to successfully cancel
                MPI_Request_free(commData->request);
            }
        }
        ret = !error;
    }

    return ret;
}

bool hpcc_mpi::cancelComm(bool send, int rank, int tag, MPI_Comm comm)
{
    _TF("mpi_wrapper:cancelComm", send, rank, tag, comm);
    CommData* commData = popCommData(send, rank, tag, comm);
    bool success = cancelComm(commData);
    delete commData;
    return success;
}

rank_t hpcc_mpi::rank(MPI_Comm comm)
{
    _TF("mpi_wrapper:rank");
    int rank;
    MPI_Comm_rank(comm, &rank);
    return rank;
}

rank_t hpcc_mpi::size(MPI_Comm comm)
{
    _TF("mpi_wrapper:size");
    int size;
    MPI_Comm_size(comm, &size);
    return size;
}

void hpcc_mpi::initialize(bool withMultithreading)
{
    _TF("mpi_wrapper:initialize", withMultithreading);
    if (withMultithreading)
    {
        int required = MPI_THREAD_MULTIPLE;
        int provided;
        MPI_Init_thread(NULL,NULL, required, &provided);
        assertex(provided == required);
    }else
    {
        MPI_Init(NULL,NULL);
    }
#ifdef DEBUG
    MPI_Comm_rank(MPI_COMM_WORLD, &global_proc_rank);
#endif
}

void hpcc_mpi::finalize()
{
    _TF("mpi_wrapper:finalize");
    MPI_Finalize();
}

hpcc_mpi::CommStatus hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, CMessageBuffer &mbuf, MPI_Comm comm, unsigned timeout)
{
    _TF("mpi_wrapper:sendData", dstRank, mptag, timeout);
    CTimeMon tm(timeout);
    unsigned remaining;
    int target = getRank(dstRank); int tag = getTag(mptag);

    CommData* commData = new CommData(true, target, tag,mbuf.length(), comm);

    mbuf.reset();
    mbuf.read(mbuf.length(), commData->data);

    bool completed = false; bool error = false; bool canceled = false; bool timedout = false;

    error  = (MPI_Isend(commData->data, mbuf.length(), MPI_BYTE, target, tag, comm, commData->request) != MPI_SUCCESS);
    tm.timedout(&remaining);

    addCommData(commData); //So that it can be cancelled from outside

    if (timeout != MP_ASYNC_SEND)
    {
        commData->probingProgress = true;
        MPI_Status stat = waitToComplete(commData->request, completed, error, canceled, timedout, remaining, commData->probingProgress);
        if (!canceled)
        { //if it was canceled by another thread commData would have cleanedup after itself so nothing to do here.
            popCommData(commData);
            if (timedout)
            {
                cancelComm(commData);
            }
            delete commData;

        }

    }

    hpcc_mpi::CommStatus status =
            error ? hpcc_mpi::CommStatus::ERROR
                    : (completed? (canceled? hpcc_mpi::CommStatus::CANCELED
                                             : hpcc_mpi::CommStatus::SUCCESS)
                                  : hpcc_mpi::CommStatus::TIMEDOUT);
    return status;
}

hpcc_mpi::CommStatus hpcc_mpi::readData(rank_t sourceRank, mptag_t mptag, CMessageBuffer &mbuf, MPI_Comm comm, unsigned timeout)
{
    _TF("mpi_wrapper:readData", sourceRank, mptag, timeout);
    CTimeMon tm(timeout);
    unsigned remaining;
    bool incomingMessage = false; bool error = false; bool completed = false; bool canceled = false;; bool timedout = false;

    tm.timedout(&remaining);
    MPI_Status stat = hasIncomingData(sourceRank, mptag, comm, incomingMessage, error, remaining);
    _T("Incoming message = "<<incomingMessage<<" error="<<error);
    if (incomingMessage)
    {
        _T("Incoming message from rank="<<sourceRank<<" with tag="<<mptag);
        int size;
        MPI_Get_count(&stat, MPI_BYTE, &size);
        assertex(size>0);
        _T("Incoming message from rank="<<sourceRank<<" with tag="<<mptag<<" Message size="<<size);

        int source = getRank(sourceRank);
        int tag = getTag(mptag);
        CommData* commData = new CommData(false, source, tag, size, comm);
        error  = (MPI_Irecv(commData->data, size, MPI_BYTE, source, tag, comm, commData->request) != MPI_SUCCESS);
        tm.timedout(&remaining);

        addCommData(commData); //So that it can be cancelled from outside

        if (timeout != MP_ASYNC_SEND)
        {
            commData->probingProgress = true;
            MPI_Status stat = waitToComplete(commData->request, completed, error, canceled, timedout, remaining, commData->probingProgress);
            if (!canceled)
            {   //if it was canceled by another thread commData would have cleanedup after itself so nothing to do here.
                _T("Irecv completed="<<completed<<" error="<<error<<" canceled="<<canceled);
                popCommData(commData);
                if (!error && completed)
                {
                    mbuf.reset();
                    mbuf.append(size,commData->data);
                    SocketEndpoint ep(stat.MPI_SOURCE);
                    mbuf.init(ep, (mptag_t)(stat.MPI_TAG), TAG_REPLY_BASE);
                } else if (timedout)
                {
                    cancelComm(commData);
                }
                delete commData;
            }
        }
    }

    hpcc_mpi::CommStatus status =
            error ? hpcc_mpi::CommStatus::ERROR
                    : (completed? (canceled? hpcc_mpi::CommStatus::CANCELED
                                             : hpcc_mpi::CommStatus::SUCCESS)
                                  : hpcc_mpi::CommStatus::TIMEDOUT);
    return status;

}

void hpcc_mpi::barrier(MPI_Comm comm)
{
    _TF("mpi_wrapper:barrier");
    MPI_Barrier(comm);
}
