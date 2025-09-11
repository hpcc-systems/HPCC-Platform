/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "platform.h"
#include <liburing.h>

#include "jiouring.hpp"
#include "jiface.hpp"
#include "jptree.hpp"
#include "jmutex.hpp"


#if defined(__linux__) || defined (__FreeBSD__)

//------------------------------------------------------------------------------

struct CompletionResponse
{
    IAsyncCallback * callback;
    int result;
};

class URingProcessor : public CInterfaceOf<IAsyncProcessor>
{
public:
    URingProcessor(const IPropertyTree * config);

    virtual void beforeDispose() override
    {
        terminate();
    }

    virtual void enqueueCallbackCommand(IAsyncCallback & callback) override;
    virtual void enqueueCallbackCommands(std::vector<IAsyncCallback *> callbacks);
    virtual void terminate() override;

    bool dequeueCompletion(CompletionResponse & response);
    bool isAborting() const { return aborting; }

protected:
    virtual void submitRequests() = 0;

protected:
    CriticalSection crit;
    bool isMultiThreaded{true};             // If false, the critical section is not used
    bool alive{false};
    std::atomic<bool> aborting{false};
    io_uring ring;
};


URingProcessor::URingProcessor(const IPropertyTree * config)
{
    unsigned queueDepth = config->getPropInt("@queueDepth", 128);
    bool polling = config->getPropBool("@poll", false);

    io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.sq_entries = queueDepth;
    params.cq_entries = queueDepth*2;
    params.flags = IORING_SETUP_CQSIZE;
    if (polling)
        params.flags |= IORING_SETUP_SQPOLL;

    int sharedIoRing = 0;
    if (sharedIoRing)
    {
        params.wq_fd = sharedIoRing;
        params.flags |= IORING_SETUP_ATTACH_WQ;
    }

    //The following causes an invalid parameter error - revisit later
    if (false)
    {
        bool interruptOnCompletion = config->getPropBool("@interruptOnCompletion", false);
        if (!interruptOnCompletion)
            params.flags |= IORING_SETUP_COOP_TASKRUN;
    }

    int ret = io_uring_queue_init_params(queueDepth, &ring, &params);
    if (ret != 0)
        throw MakeStringException(0, "Failed to initialize io_uring queue: error code %d", ret);

    io_uring_ring_dontfork(&ring);
    alive = true;
}

bool URingProcessor::dequeueCompletion(CompletionResponse & response)
{
    if (aborting)
        return false;

    struct io_uring_cqe *cqe;
    if (io_uring_wait_cqe(&ring, &cqe) != 0)
        return false;

    response.callback = static_cast<IAsyncCallback *>(io_uring_cqe_get_data(cqe));
    response.result = cqe->res;
    io_uring_cqe_seen(&ring, cqe);
    return true;
}

void URingProcessor::enqueueCallbackCommand(IAsyncCallback & callback)
{
    CLeavableCriticalBlock block(crit, isMultiThreaded);

    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    /* Setup a nop operation - should be called back immediately */
    io_uring_prep_nop(sqe);
    /* Set user data */
    io_uring_sqe_set_data(sqe, &callback);

    submitRequests();
}

void URingProcessor::enqueueCallbackCommands(std::vector<IAsyncCallback *> callbacks)
{
    CLeavableCriticalBlock block(crit, isMultiThreaded);

    for (IAsyncCallback * callback : callbacks)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        /* Setup a nop operation - should be called back immediately */
        io_uring_prep_nop(sqe);
        /* Set user data */
        io_uring_sqe_set_data(sqe, callback);
    }

    submitRequests();
}

void URingProcessor::terminate()
{
    if (!alive)
        return;

    alive = false;
    io_uring_queue_exit(&ring);
}

//------------------------------------------------------------------------------

// URingThreadedProcessor is a URingProcessor with a thread for processing completion events
class TerminateCompletionThreadAction final : public IAsyncCallback
{
public:
    TerminateCompletionThreadAction(std::atomic<bool> & _aborting) : aborting(_aborting) {}
    virtual void onAsyncComplete(int result) override
    {
        aborting = true;
    };
private:
    std::atomic<bool> & aborting;
};


class URingCompletionThread : public Thread
{
public:
    URingCompletionThread(URingProcessor & _processor) : Thread("URingCompletionThread"), processor(_processor)
    {
    }

    int run() override
    {
        while (!processor.isAborting())
        {
            CompletionResponse response;
            if (processor.dequeueCompletion(response))
                response.callback->onAsyncComplete(response.result);
        }
        return 0;
    }

private:
    URingProcessor & processor;
};

// This class has a thread for processing completion events
class URingThreadedProcessor final : public URingProcessor
{
public:
    URingThreadedProcessor(const IPropertyTree * config) : URingProcessor(config), completionThread(*this)
    {
        completionThread.start(false);
    }

    virtual void checkForCompletions() override
    {
    }

    virtual void submitRequests() override
    {
        /* Finally, submit all the requests */
        io_uring_submit(&ring);
    }

    virtual void terminate() override
    {
        if (!alive)
            return;

        TerminateCompletionThreadAction action(aborting);
        enqueueCallbackCommand(action);
        completionThread.join();
        URingProcessor::terminate();
    }

protected:
    URingCompletionThread completionThread;
};


//------------------------------------------------------------------------------

// URingUnthreadedProcessor is a URingProcessor which checks for completion events each time a new event is completed.
class URingUnthreadedProcessor final : public URingProcessor
{
public:
    URingUnthreadedProcessor(const IPropertyTree * config) : URingProcessor(config), completionThread(*this)
    {
    }

    virtual void checkForCompletions() override
    {
        processCompletions();
    }

    virtual void submitRequests() override
    {
        io_uring_submit(&ring);

        //Then check for all existing completions
        processCompletions();
    }

protected:
    void processCompletions()
    {
        while (io_uring_cq_ready(&ring) != 0)
        {
            CompletionResponse response;
            if (dequeueCompletion(response))
                response.callback->onAsyncComplete(response.result);
        }
    }

protected:
    URingCompletionThread completionThread;
};


//------------------------------------------------------------------------------

IAsyncProcessor * createUring(const IPropertyTree * config, bool threaded)
{
    if (threaded)
        return new URingThreadedProcessor(config);
    else
        return new URingUnthreadedProcessor(config);
}

#else

// Lib uring is only supported on Linux and FreeBSD
IAsyncProcessor * createUring(const IPropertyTree * config)
{
    return nullptr;
}

#endif
