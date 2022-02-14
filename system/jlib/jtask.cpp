/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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
#include <string.h>
#include <limits.h>
#include <algorithm>
#include "jtask.hpp"
#include "jlog.hpp"
#include "jqueue.hpp"

constexpr bool traceTasks = false;

static std::atomic<ITaskScheduler *> taskScheduler{nullptr};
static std::atomic<ITaskScheduler *> iotaskScheduler{nullptr};
static CriticalSection singletonCs;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    ::Release(taskScheduler.load());
    ::Release(iotaskScheduler.load());
}

class CTaskProcessor;
class TaskScheduler;
static thread_local CTaskProcessor * activeTaskProcessor = nullptr;

//---------------------------------------------------------------------------------------------------------------------

static inline unsigned __int64 nextPowerOf2(unsigned __int64 v)
{
    assert(sizeof(v)==8);
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}


/*
This class aims to implement a lock free stack - with operations to push and pop items from the stack, but also
allow other threads to steal items from top of the stack.

The aim is for push and pop to be as efficient as possible (so that recursive tasks that are using all threads)
progress as efficiently as possible.

There is a nasty ABA race condition
- one thread pushes an item
- another thread starts to steal it.
- the first thread pops that item, and then pushes another one.
- the stealing thread then completes the steal, but with the old task pointer

To avoid this, the pop code increments both start and end if the last item is removed, so that the steal will be
forced to retry.  (branch savedTaskManager contains a more complex solution adding a sequence number)
*/

class CasTaskStack
{
public:
    using seq_type = unsigned int;
    using pair_type = unsigned __int64;

protected:
    static constexpr unsigned shift = sizeof(seq_type) * 8;

    //Pack (start,end) into a single value.  End is in the top so it can be incremented (and decremented) without unpacking
    static seq_type getStart(pair_type value) { return (seq_type)(value); }
    static seq_type getEnd(pair_type value) { return (seq_type)(value >> shift); }
    static constexpr pair_type makePair(seq_type start, seq_type end) { return (((pair_type)end) << shift) | start; }
public:
    CasTaskStack(seq_type _numElements) : elementMask(nextPowerOf2(_numElements)-1)
    {
        tasks = new std::atomic<CTask *>[numElements()];
    }
    ~CasTaskStack()
    {
        try
        {
            for (;;)
            {
                CTask * task = popNextTask();
                if (!task)
                    break;
                task->Release();
            }
            delete [] tasks;
        }
        catch (...)
        {
            printf("Error popping task\n");
        }
    }

    void pushTask(CTask * ownedTask)
    {
        pair_type cur = startEnd.load(std::memory_order_acquire);
        seq_type start = getStart(cur);
        seq_type end = getEnd(cur);
        if (end - start > elementMask)
            outOfMemory();

        set(end, ownedTask);
        //increment end atomically - wrapping causes no problem, nothing else can modify end at the same time
        pair_type incEnd = makePair(0, 1);
        startEnd.fetch_add(incEnd, std::memory_order_acq_rel);
    }

    CTask * popNextTask()
    {
        pair_type cur = startEnd.load(std::memory_order_acquire);
        //Optimize: Can calculate end here and load task, and avoid reloading later, since nothing else can modify it
        seq_type end = getEnd(cur);

        //Fast path should be popping an item from a non-empty list => best to load even if the list proves to be empty
        CTask * task = get(end-1);
        for (;;)
        {
            seq_type start = getStart(cur);
            if (start == end)
                return nullptr;

            pair_type next = makePair(start, end-1);
            //Avoid ABA problem mentioned above by incrementing start and end if this is the last element
            if (start == end -1)
                next = makePair(end, end);

            //or...  next = cur - makePair(0, 1);
            if (startEnd.compare_exchange_weak(cur, next, std::memory_order_acq_rel))
                return task;
        }
    }

    //Tasks are stolen from the start of the list
    CTask * stealTask()
    {
        pair_type cur = startEnd.load(std::memory_order_acquire);
        for (;;)
        {
            seq_type start = getStart(cur);
            seq_type end = getEnd(cur);
            if (start == end)
                return nullptr;
            CTask * task = get(start);
            pair_type next = makePair(start+1, end);
            if (startEnd.compare_exchange_weak(cur, next, std::memory_order_acq_rel))
                return task;
        }
    }

    void outOfMemory() __attribute__((noinline))
    {
        //Fail if no room to add the item - could expand at this point, by cloning it into another array, and replacing tasks
        //create a new array double the size
        //copy current array into both sides of the new array
        //atomically update tasks
        //atomically update elementMask
        //or store number of elements inside the tasks array object and only have a single atomic
        printf("TaskStack::outOfMemory\n");
        UNIMPLEMENTED_X("TaskStack::outOfMemory");

    }
    size_t numElements() const { return elementMask + 1; }

    CTask * get(seq_type index) const
    {
        return tasks[index & elementMask].load(std::memory_order_acquire);
    }
    void set(seq_type index, CTask * task) const
    {
        tasks[index & elementMask].store(task, std::memory_order_release);
    }

protected:
    seq_type elementMask;
    std::atomic<pair_type> startEnd{0};
    std::atomic<CTask *> * tasks;
};

static_assert(sizeof(CasTaskStack::pair_type) == 2 * sizeof(CasTaskStack::seq_type), "pair_type and seq_type are inconsistent");

//---------------------------------------------------------------------------------------------------------------------

class CTaskProcessor final : public Thread
{
    using TaskStack = CasTaskStack;
public:
    CTaskProcessor(TaskScheduler * _scheduler, unsigned _id);

// Thread
    virtual int run();

    void enqueueOwnedChildTask(CTask & ownedTask);
    bool isAborting() const { return abort; }
    void stopProcessing() { abort = true; }
    CTask * stealTask() { return tasks.stealTask(); }

protected:
    void doRun();

private:
    TaskScheduler * scheduler;
    TaskStack tasks;
    unsigned id;
    std::atomic_bool abort{false};
};

class TaskScheduler final : public CInterfaceOf<ITaskScheduler>
{
    friend class CTaskProcessor;
public:
    TaskScheduler(unsigned _numThreads);
    ~TaskScheduler();

    virtual void enqueueOwnedTask(CTask & ownedTask) override final
    {
        assertex(ownedTask.isReady());
        {
            CriticalBlock block(cs);
            queue.enqueue(&ownedTask);
        }
        if (processorsWaiting)
            avail.signal();
    }

    virtual unsigned numProcessors() const override { return numThreads; }


protected:
    // Return a new task for a thread processor which has run out of .
    CTask * getNextTask(unsigned id)
    {
        //Implement in 3 phases
        //a) Check if anything is on the global queue or could be stolen from another processor
        //b) Indicate there is a thread waiting and then repeat (a)
        //c) wait for a semaphore to indicate something has been added and then repeat (a)
        //
        // The aim is to:
        // * Avoid semaphore signals and waits if the system is busy
        // * Ensure the processors sleep if there is no work to do
        //
        // It is possible/likely that the semaphore will be signalled too many times - if a thread is waiting and
        // several work items are added, but the only negative side-effect of that is that processors will spin around the check loop
        // when there is no more work to be done.

        CTask * task = nullptr;
        bool waiting = false;
        for (;;)
        {
            if (aborting)
                break;

            //First check to see if there is a global queue
            if (!queue.isEmpty())
            {
                CriticalBlock block(cs);
                task = queue.dequeue();
                if (task)
                    break;
            }

            //Nothing there - now see if we can steal a child from all processors except ourself.
            unsigned nextTarget = id;
            for (unsigned i=1; i < numThreads; i++)
            {
                nextTarget++;
                if (nextTarget == numThreads)
                    nextTarget = 0;
                task = processors[nextTarget]->stealTask();
                if (task)
                {
                    if (traceTasks)
                        printf("Stolen for %u on %u\n", id, sched_getcpu());
                    if (waiting)
                        processorsWaiting--;
                    return task;
                }
            }

            //Nothing was found - probably another processor added a child but then processed it before
            //anyone stole it.
            if (waiting)
            {
                if (traceTasks)
                    printf("Pause %u on %u\n", id, sched_getcpu());
                avail.wait();
                if (traceTasks)
                    printf("Restart %u on %u\n", id, sched_getcpu());
            }
            else
            {
                waiting = true;
                processorsWaiting++;
            }
        }

        if (traceTasks)
            printf("Task for %u on %u\n", id, sched_getcpu());

        if (waiting)
            processorsWaiting--;
        return task;

    }

    void noteChildEqueued()
    {
        if (processorsWaiting != 0)
            avail.signal();
    }

protected:
    unsigned numThreads = 0;
    std::atomic<unsigned> processorsWaiting{0};
    CTaskProcessor * * processors = nullptr;
    Semaphore avail;
    CriticalSection cs;
    DListOf<CTask> queue;
    std::atomic_bool aborting{false};
};


//---------------------------------------------------------------------------------------------------------------------

static constexpr unsigned maxChildTasks = 1024;

CTaskProcessor::CTaskProcessor(TaskScheduler * _scheduler, unsigned _id)
: scheduler(_scheduler), tasks(maxChildTasks), id(_id)
{
}

void CTaskProcessor::enqueueOwnedChildTask(CTask & ownedTask)
{
    tasks.pushTask(&ownedTask);
    scheduler->noteChildEqueued();
}

void CTaskProcessor::doRun()
{
    for (;;)
    {
        CTask * next = scheduler->getNextTask(id);

        for (;;)
        {
            if (abort)
            {
                ::Release(next);
                return;
            }

            try
            {
                CTask * follow = next->execute();
                //Not sure this should even be special cased - more sensible would be a loop within execute if you want to rerun
                if (likely(follow != next))
                {
                    next->Release();
                    if (!follow)
                    {
                        // Pop the next child task from the processors' private list.
                        follow = tasks.popNextTask();
                        if (!follow)
                            break;  // i.e. check for an item on the global task list
                    }
                }
                next = follow;
            }
            catch (IException * e)
            {
                EXCLOG(e);
                e->Release();
            }
        }
    }
}

int CTaskProcessor::run()
{
    activeTaskProcessor = this;
    doRun();
    activeTaskProcessor = nullptr;
    return 0;
}

//---------------------------------------------------------------------------------------------------------------------

void notifyPredDone(Owned<CTask> && successor)
{
    if (successor->notePredDone())
        activeTaskProcessor->enqueueOwnedChildTask(*successor.getClear());
}

void notifyPredDone(CTask & successor)
{
    if (successor.notePredDone())
        activeTaskProcessor->enqueueOwnedChildTask(OLINK(successor));
}

void enqueueOwnedTask(ITaskScheduler & scheduler, CTask & ownedTask)
{
    if (activeTaskProcessor)
        activeTaskProcessor->enqueueOwnedChildTask(ownedTask);
    else
        scheduler.enqueueOwnedTask(ownedTask);
}

//---------------------------------------------------------------------------------------------------------------------

void CTask::spawnOwnedChildTask(CTask & ownedTask)
{
    assertex(activeTaskProcessor);
    activeTaskProcessor->enqueueOwnedChildTask(ownedTask);
}

void CTask::setException(IException * e)
{
    IException * expected = nullptr;
    if (exception.compare_exchange_strong(expected, e))
        e->Link();
}

//---------------------------------------------------------------------------------------------------------------------

void CCompletionTask::decAndWait()
{
    if (notePredDone())
    {
        //This is the last predecessor - skip signalling the semaphore and then waiting for it
        //common if no child tasks have been created...
        if (!tasksLinkedOnSchedule)
            Release();
    }
    else
        sem.wait();

    if (exception)
        throw LINK(exception.load());
}

void CCompletionTask::setMinimalLinking()
{
    Link();     // This will be release either when the task is scheduled, or when waiting for completion.
    tasksLinkedOnSchedule = false;
}

void CCompletionTask::spawn(std::function<void ()> func, bool ignoreParallelExceptions)
{
    // Avoid spawning a new child task if a different child task has failed
    if (ignoreParallelExceptions || !hasException())
    {
        CTask * task = new CFunctionTask(func, this, ignoreParallelExceptions);
        enqueueOwnedTask(scheduler, *task);
    }
}


//---------------------------------------------------------------------------------------------------------------------

CFunctionTask::CFunctionTask(std::function<void ()> _func, CTask * _successor, bool _ignoreParallelExceptions)
: CPredecessorTask(0, _successor), func(_func), ignoreParallelExceptions(_ignoreParallelExceptions)
{
}

CTask * CFunctionTask::execute()
{
    //Avoid starting a new subtask if one of the subtasks has already failed
    if (ignoreParallelExceptions || !successor->hasException())
    {
        try
        {
            func();
        }
        catch (IException * e)
        {
            successor->setException(e);
            e->Release();
        }
    }
    return checkNextTask();
}

//---------------------------------------------------------------------------------------------------------------------
static unsigned maxTasks = (unsigned)-1;
static unsigned maxIOTasks = (unsigned)-1;

void setTaskLimits(unsigned _maxCpuTasks, unsigned _maxIOTasks)
{
    maxTasks = _maxCpuTasks;
    maxIOTasks = _maxIOTasks;
}

TaskScheduler::TaskScheduler(unsigned _numThreads) : numThreads(_numThreads)
{
    processors = new CTaskProcessor * [numThreads];
    for (unsigned i = 0; i < numThreads; i++)
        processors[i] = new CTaskProcessor(this, i);
    for (unsigned i2 = 0; i2 < numThreads; i2++)
        processors[i2]->start();
}

TaskScheduler::~TaskScheduler()
{
    aborting = true;

    //Indicate to all schedulers they should terminate
    for (unsigned i1 = 0; i1 < numThreads; i1++)
        processors[i1]->stopProcessing();

    //Start all the processors
    avail.signal(numThreads);

    //Join all processors before deleting - just in case something tries to steal from a deleted processor
    //(only possible if this is terminating while tasks are currently being processed)
    for (unsigned i2 = 0; i2 < numThreads; i2++)
        processors[i2]->join();

    for (unsigned i3 = 0; i3 < numThreads; i3++)
        delete processors[i3];
    delete [] processors;
}

extern jlib_decl ITaskScheduler & queryTaskScheduler()
{
    unsigned numTasks = std::min(maxTasks, getAffinityCpus());
    return *querySingleton(taskScheduler, singletonCs, [ numTasks ] { return new TaskScheduler(numTasks); });
}

extern jlib_decl ITaskScheduler & queryIOTaskScheduler()
{
    unsigned numTasks = std::min(maxIOTasks, getAffinityCpus() * 2);
    return *querySingleton(iotaskScheduler, singletonCs, [ numTasks ] { return new TaskScheduler(numTasks); });
}

//---------------------------------------------------------------------------------------------------------------------
