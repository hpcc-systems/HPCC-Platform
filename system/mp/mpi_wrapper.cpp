/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

#include <mpi/mpi.h>
#include "mpi_wrapper.hpp"
#include <cstdlib>
#include <queue>
#include <typeinfo>

#include "mputil.hpp"

#define WAIT_DELAY 100
#define PROBING_DELAY 100

//----------Functions and Data structures managing communication data related to Send/Recv Communications in orogress-----------//

// Data structure to keep the data relating to send/receive communications
class CommData
{
private:
    bool send;                      // TRUE => relates to a send communication | FALSE => relates to receive communication
    bool locked = false;
    bool cancellationLock = false;
    bool cancellationInProgress = false;
//    CMessageBuffer* data = NULL;              // Data structure which points to the sent/recv buffer
    CriticalSection dataChangeLock;
    void lock(){if (!locked) dataChangeLock.enter(); locked=true;}
    void unlock(){if (locked) dataChangeLock.leave(); locked=false;}
public:    
    bool isSend(){return send;}
    bool isReceive(){return !isSend();}
    bool isEqual(bool _send, int _rank, int _tag, MPI::Comm& _comm)
    {
        return (send==_send) && (_rank==MPI_ANY_SOURCE || rank==_rank) && (_tag==MPI_ANY_TAG || tag==_tag) && (comm == _comm);
    }

    int rank;                       // source/destination rank of the processor
    int tag;                        // MPI tag information
    MPI::Request request;           // persistent request object to keep track of ongoing MPI call
    MPI::Comm& comm;                // MPI communicator


    CommData(bool _send, int _rank, int _tag, MPI::Comm& _comm):
        send(_send), rank(_rank), tag(_tag), comm(_comm){}

    CommData(int _rank, int _tag, MPI::Comm& _comm): rank(_rank), tag(_tag), comm(_comm), send(false){}

    void notifyCancellation()
    {
        lock();
        cancellationInProgress = true;
        unlock();
    }

    bool lockFromCancellation()         //returns true only if currently some thread is not attempting to cancel
    {
        lock();
        if (cancellationInProgress)
        {
            unlock();
        } else
        {
            cancellationLock = true;
        }
        return !cancellationInProgress;
    }

    void releaseCancellationLock()
    {
        if (cancellationLock)
        {
            cancellationLock = false;
            unlock();
        }//TODO else clause: throw a meaningful error for invalid call for this function
    }

    ~CommData()
    {

    }
};

std::vector<CommData*> asyncCommData; // CommData list to manage while send/recv communication in progress
CriticalSection commDataLock;         // A mutex lock for the index list above

CommData* _popCommData(int index)
{
    _TF("_popCommData", index);
    CommData* ret = NULL;
    assertex(index>=0);
    assertex(index < asyncCommData.size());
    ret = asyncCommData[index];
    asyncCommData.erase(asyncCommData.begin() + index);
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

std::vector<CommData*> popCommData(bool send, int rank, int tag, MPI::Comm& comm)
{
   _TF("popCommData", rank, tag);
   CriticalBlock block(commDataLock);
   std::vector<CommData*> matchedResults = std::vector<CommData*>();
   int size = asyncCommData.size();
   for(int i=(size-1); i>=0; i--)
   {
       if (asyncCommData[i]->isEqual(send, rank, tag, comm))
           matchedResults.push_back(_popCommData(i));
   }
   return matchedResults;
}

CommData* popCommData(CommData * commData)
{
    _TF("popCommData(CommData * commData)", commData->rank, commData->tag);
    CriticalBlock block(commDataLock);
    int size = asyncCommData.size();
    for(int i=(size-1); i>=0; i--)
    {
        if (asyncCommData[i]==commData)
        {
            _popCommData(i);
            break;
        }
    }
    return commData;
}

//----------------------------------------------------------------------------------------------------------------------------//

#define TAG_UB 100000 //INT_MAX //MPI_TAG_UB
int getRank(rank_t sourceRank)
{
    _TF("getRank", sourceRank);
    if (sourceRank == RANK_ALL)
        return MPI::ANY_SOURCE;
    else
        return sourceRank;
}

int getTag(mptag_t mptag)
{
    _TF("getTag", mptag);
    int returnVal;
    unsigned tag = mptag;
    if (mptag == TAG_ALL)
        returnVal = MPI::ANY_TAG;
    else
    {
        if (tag > TAG_UB)
        {
            tag= TAG_UB-((unsigned)-tag);                         //MPI doesn't allow custom tags with negative values or beyond MPI_TAG_UB. Thus we shift it to the MPI_TAG_UB range
        }
        returnVal = static_cast<int>(tag);

    }
    return returnVal;
}

mptag_t getTag(int tag)
{
    _TF("getTag", tag);
    unsigned returnVal;
    if (tag == MPI::ANY_TAG)
        returnVal = TAG_ALL;
    else
    {
        if (tag > (TAG_UB/2))
        {
            returnVal = (unsigned) - (TAG_UB-tag);                         //MPI doesn't allow custom tags with negative values
        } else
            returnVal = tag;

    }
    return (mptag_t)returnVal;
}

MPI::Status waitToComplete( bool& completed, bool& error, bool& canceled, bool& timedout, unsigned timeout, CommData *commData)
{
    _TF("waitToComplete", completed, error, canceled, timeout);
    CTimeMon tm(timeout);
    MPI::Status stat;
    unsigned remaining;
    bool noCancellation;
    while ((noCancellation = commData->lockFromCancellation()) && !(completed || error || (timedout = tm.timedout(&remaining))))
    {
        completed = commData->request.Test(stat);
        commData->releaseCancellationLock();
        usleep(WAIT_DELAY);
    }
    if (completed)
        canceled = stat.Is_cancelled();
    else
        canceled = !noCancellation;

    return stat;
}

MPI::Status hasIncomingData(int sourceRank, int mptag, MPI::Comm& comm,
        bool& incomingMessage, bool& error, bool& canceled, unsigned timeout, CommData *commData)
{
    _TF("hasIncomingData", sourceRank, mptag, incomingMessage, error, timeout);

    CTimeMon tm(timeout);
    MPI::Status stat;

    int flag;
    unsigned remaining;
    bool noCancellation;

    while ((noCancellation = commData->lockFromCancellation()) && !(incomingMessage || error || (timeout !=0 && tm.timedout(&remaining))))
    {
        incomingMessage = comm.Iprobe(sourceRank, mptag, stat);
        commData->releaseCancellationLock();
        if (timeout == 0) break;
        usleep(PROBING_DELAY);
    }
    canceled = !noCancellation;
    return stat;
}

//----------------------------------------------------------------------------//

/** See mpi_wrapper.hpp header file for function descriptions of the following **/

bool hpcc_mpi::hasIncomingMessage(rank_t &sourceRank, mptag_t &mptag, MPI::Comm& comm)
{
    _TF("hasIncomingMessage", sourceRank, mptag);
    bool incomingMessage = false; bool error = false; bool canceled = false;
    int source = getRank(sourceRank);
    int tag = getTag(mptag);
    CommData* tmpCommData = new CommData(source, tag, comm);
    MPI::Status stat = hasIncomingData(source, tag, comm, incomingMessage, error, canceled, 0, tmpCommData);
    if (incomingMessage)
    {
        sourceRank = stat.Get_source();
        mptag = getTag(stat.Get_tag());
    }

    return incomingMessage;
}

bool cancelComm(CommData* commData)
{
    bool ret = true;
    if (commData)
    {
        MPI::Status stat;
        //TODO: try catch for error
        bool completed;
        completed = commData->request.Test(stat);
        if (!completed) {
            commData->request.Cancel();
            commData->request.Free();
        }
    }

    return ret;
}

bool hpcc_mpi::cancelComm(bool send, rank_t rank, mptag_t mptag, MPI::Comm& comm)
{
    _TF("cancelComm", send, rank, mptag);
    int r = getRank(rank);
    int tag = getTag(mptag);
    std::vector<CommData*> commDataList = popCommData(send, r, tag, comm);
    bool success = true;
    for(int i=0; i<commDataList.size(); i++)
    {
        commDataList[i]->notifyCancellation();              // In case sendData and readData functions still in progress we want to let
                                                            // them know that we are about to screw up their plans
        success = success && cancelComm(commDataList[i]);   // If we managed to cancel everything then the cancellation was successful
        usleep(WAIT_DELAY);                                 // Wait for a short while for active threads to exit send/recv function
        delete commDataList[i];
    }
    return success;
}

rank_t hpcc_mpi::rank(MPI::Comm& comm)
{
    _TF("rank");
    return comm.Get_rank();
}

rank_t hpcc_mpi::size(MPI::Comm& comm)
{
    _TF("size");
    return comm.Get_size();
}

#define MPI_BUFFER_SIZE 16777216        // 16MB
void* mpi_buffer_data;

void hpcc_mpi::initialize(bool withMultithreading)
{
    _TF("initialize", withMultithreading);
    if (withMultithreading)
    {
        int required = MPI_THREAD_MULTIPLE;
        int provided = MPI::Init_thread(required);
        assertex(provided == required);
    }else
    {
        MPI_Init(NULL,NULL);
    }
    mpi_buffer_data = malloc(MPI_BUFFER_SIZE);
    MPI::Attach_buffer(mpi_buffer_data, MPI_BUFFER_SIZE);
#ifdef DEBUG
    global_proc_rank = MPI::COMM_WORLD.Get_rank();
#endif
}

void hpcc_mpi::setErrorHandler(MPI::Comm& comm, MPI::Errhandler handler)
{
    comm.Set_errhandler(handler);
}

void hpcc_mpi::finalize()
{
    _TF("finalize");
    int size;
    void *ptr;

    size = MPI::Detach_buffer(ptr);
    free(mpi_buffer_data);
    MPI::Finalize();
}

hpcc_mpi::CommStatus hpcc_mpi::sendData(rank_t dstRank, mptag_t mptag, CMessageBuffer &mbuf, MPI::Comm& comm, unsigned timeout)
{
    _TF("sendData", dstRank, mptag, mbuf.getReplyTag(), timeout);
    CTimeMon tm(timeout);
    unsigned remaining;
    int target = getRank(dstRank); int tag = getTag(mptag);
//    mptag_t t = getTag(tag);
//    assertex(t==tag);
//    _T("Rank="<<target<<" Tag="<<tag);
    CommData* commData = new CommData(true, target, tag, comm);
    bool timedout = false; bool error = false; bool canceled = false; bool completed;
    bool bufferedSendComplete = false;
    addCommData(commData); //So that it can be cancelled from outside

    //TODO: find a better way to send the reply tag
    size_t orginalLength = mbuf.length();
    unsigned replyTag = mbuf.getReplyTag();
    mbuf.append(replyTag);
    _T("Sending replyTag="<<replyTag);
    while(!bufferedSendComplete)
    {
        try
        {
            bool notCanceled = commData->lockFromCancellation();
            if (notCanceled)
            {
                commData->request  = comm.Ibsend(mbuf.bufferBase(), mbuf.length(), MPI_BYTE, target, tag);
                commData->releaseCancellationLock();
            }
            bufferedSendComplete = true;
            //remove reply tag from the buffer
            mbuf.setLength(orginalLength);
            canceled = !notCanceled;
        } catch (MPI::Exception &e) {
            commData->releaseCancellationLock();
            if (e.Get_error_class() == MPI::ERR_BUFFER)
                usleep(WAIT_DELAY); //retry after giving some time for the buffers to clear up
            else
            {
                _T("Error occured while trying to do Ibsend code="<<e.Get_error_code()<<" string="<<e.Get_error_string());
                throw e;
            }
        }
    }

    timedout = tm.timedout(&remaining);

    if (!error && timedout)
    {
        popCommData(commData);
        cancelComm(commData);
    }

    hpcc_mpi::CommStatus status =
            error ? hpcc_mpi::CommStatus::ERROR
                    : (timedout?    hpcc_mpi::CommStatus::TIMEDOUT
                                    :canceled?  hpcc_mpi::CommStatus::CANCELED
                                            :hpcc_mpi::CommStatus::SUCCESS);
    return status;
}

hpcc_mpi::CommStatus hpcc_mpi::readData(rank_t &sourceRank, mptag_t &mptag, CMessageBuffer &mbuf, MPI::Comm& comm, unsigned timeout)
{
    _TF("readData", sourceRank, mptag, timeout);
    CTimeMon tm(timeout);
    unsigned remaining;
    bool incomingMessage = false; bool error = false; bool completed = false; bool canceled = false; bool timedout = false;

    tm.timedout(&remaining);
    int source = getRank(sourceRank);
    int tag = getTag(mptag);

    CommData* commData = new CommData(source, tag, comm);
    addCommData(commData); //So that it can be cancelled from outside

    MPI::Status stat = hasIncomingData(source, tag, comm, incomingMessage, error, canceled, remaining, commData);

    timedout = tm.timedout(&remaining);
    if (incomingMessage && !canceled)
    {
        int size = stat.Get_count(MPI_BYTE);
        assertex(size>0);
        mbuf.setLength(size);
        bool notCanceled = commData->lockFromCancellation();
        if (notCanceled)
        {
            commData->request  = comm.Irecv(mbuf.bufferBase(), mbuf.length(), MPI_BYTE, source, tag);
            commData->releaseCancellationLock();
        }
        canceled = !notCanceled;
        tm.timedout(&remaining);

        MPI::Status stat = waitToComplete(completed, error, canceled, timedout, remaining, commData);
        if (!canceled)
        {   //if it was canceled by another thread commData would have cleanedup after itself so nothing to do here.
            if (!error && completed)
            {
                bool noCancellation = commData->lockFromCancellation();
                if (noCancellation)
                {
                    sourceRank = stat.Get_source();
                    mptag = getTag(stat.Get_tag());
//                    int t = getTag(mptag);
//                    assertex(t==mptag);
                    //pop the reply tag and resetting the length of the message without reply tag
                    size_t orginalLength = mbuf.length();
                    size_t newLength = orginalLength - sizeof(mptag_t);
                    mbuf.reset(newLength);
                    unsigned replyTag;
                    mbuf.read(replyTag);
                    _T("Sender="<<sourceRank<<", Tag="<<mptag<<", replyTag="<<replyTag);
                    mbuf.setReplyTag((mptag_t)replyTag);
                    mbuf.setLength(newLength);

                    commData->releaseCancellationLock();
                }
                canceled = !noCancellation;
            } else if (timedout)
            {
                cancelComm(commData);
                canceled = true;
            }
        }

    }
    if (!canceled)
    {
        popCommData(commData);
        delete commData;
    }
    hpcc_mpi::CommStatus status =
            error ? hpcc_mpi::CommStatus::ERROR
                    : (completed? (canceled? hpcc_mpi::CommStatus::CANCELED
                                             : hpcc_mpi::CommStatus::SUCCESS)
                                  : hpcc_mpi::CommStatus::TIMEDOUT);
    return status;
}

void hpcc_mpi::barrier(MPI::Comm& comm)
{
    _TF("barrier");
    comm.Barrier();
}
