/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"
#include <cstdlib>
#include <queue>
#include <mutex>
#include "mputil.hpp"

#define MPTAG_SIZE 1000

//-------------------send/receive asynchronous communication----------------------//

// Data structure to keep the data relating to send/receive communications
class CommData
{
private:
    bool send;
public:    
    bool isSend(){return send;}
    bool isReceive(){return !send;}
    bool isEqual(bool _send, int _rank, int _tag, MPI_Comm _comm) {return (send==_send) && (_rank<0 || rank==_rank) && (_tag<0 || tag==_tag) && (comm == _comm);}

    void* data;                     // Data structure which keeps the sent/recv data
    int size;                       // size of 'data'
    int rank;                       // source/destination rank of the processor
    int tag;                        // MPI tag infomation
    MPI_Request *request;            // request object to keep track of ongoing MPI call
    MPI_Comm comm;

    CommData(bool _send, void* _data, int _size, int _rank, int _tag, MPI_Request* _request, MPI_Comm _comm):
        send(_send), data(_data), size(_size), rank(_rank), tag(_tag), request(_request), comm(_comm){}
    ~CommData()
    {
        if (data) free(data);
        if (request) delete request;
    }
};

std::vector<CommData*> asyncCommData;     // CommData Pool
CriticalSection commDataLock;         // A mutex lock for the index list above

void addCommData(CommData *commData)
{
   _TF("addCommData", commData->rank, commData->size);
   CriticalBlock block(commDataLock);
   asyncCommData.push_back(commData);
}

CommData* _popCommData(int index)
{
    _TF("popCommData", index);
    CommData* ret = NULL;
    if (index != -1) {
        ret = asyncCommData[index];
        asyncCommData.erase(asyncCommData.begin() + index);
    }
    return ret;
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

CommData *popCommData(CommData * _commData)
{
   _TF("popCommData");
   CriticalBlock block(commDataLock);
   int index = -1;
   for(int i=0; i< asyncCommData.size(); i++)
   {
       if (asyncCommData[i]==_commData)
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

MPI_Status waitToComplete(MPI_Request &req, bool& completed, bool& error, bool& canceled, bool& timedout, unsigned timeout)
{
    _TF("mpi_wrapper:waitToComplete", completed, error, canceled, timeout);
    CTimeMon tm(timeout);
    MPI_Status stat;
    int flag;
    unsigned remaining;
    while (!(completed || error || (timedout = tm.timedout(&remaining))))
    {
        usleep(100);
        error = (MPI_Test(&req, &flag, &stat) != MPI_SUCCESS);
        completed = (flag > 0);
    }
    if (completed)
    {
        MPI_Test_cancelled(&stat, &flag);
        canceled = (flag > 0);
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
        usleep(100);
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
    //TODO async support
    CTimeMon tm(timeout);
    unsigned remaining;

    char* data = (char *) malloc(mbuf.length());
    mbuf.reset();
    mbuf.read(mbuf.length(), data);
    int target = getRank(dstRank); int tag = getTag(mptag);
    bool completed = false; bool error = false; bool canceled = false; bool timedout = false;

    MPI_Request* req = new MPI_Request();
    error  = (MPI_Isend(data, mbuf.length(), MPI_BYTE, target, tag, comm, req) != MPI_SUCCESS);
    tm.timedout(&remaining);

    CommData* commData = new CommData(true, data, mbuf.length(), target, tag,req, comm);
    addCommData(commData); //So that it can be cancelled from outside

    if (timeout != MP_ASYNC_SEND)
    {
        MPI_Status stat = waitToComplete(*req, completed, error, canceled, timedout, remaining);
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
    //TODO async support
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

        char* data = (char *) malloc(size);

        MPI_Request* req = new MPI_Request();
        int source = getRank(sourceRank);
        int tag = getTag(mptag);
        error  = (MPI_Irecv(data, size, MPI_BYTE, source, tag, comm, req) != MPI_SUCCESS);
        tm.timedout(&remaining);

        CommData* commData = new CommData(false, data, size, source, tag,req, comm);
        addCommData(commData); //So that it can be cancelled from outside

        if (timeout != MP_ASYNC_SEND)
        {
            MPI_Status stat = waitToComplete(*req, completed, error, canceled, timedout, remaining);
            if (!canceled)
            {   //if it was canceled by another thread commData would have cleanedup after itself so nothing to do here.
                _T("Irecv completed="<<completed<<" error="<<error<<" canceled="<<canceled);
                popCommData(commData);
                if (!error && completed)
                {
                    mbuf.reset();
                    mbuf.append(size,data);
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
