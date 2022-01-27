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

#ifndef JTASK_HPP
#define JTASK_HPP

#include <atomic>
#include "jiface.hpp"
#include "jthread.hpp"
#include "jqueue.hpp"

interface ITaskScheduler;

class jlib_decl CTask : public CInterface
{
    friend class TaskQueue;
    friend class DListOf<CTask>;

public:
    CTask(unsigned _numPred) : numPredecessors(_numPred) {}

    //Return the next task to execute
    virtual CTask * execute() = 0;

    bool isReady() const { return numPredecessors == 0; }
    void addPred()
    {
        numPredecessors.fetch_add(1);
    }
    // Return true if this is now available to execute.
    bool notePredDone()
    {
        return numPredecessors.fetch_add(-1) == 1;
    }

    CTask * checkNextTask()
    {
        return nullptr;
    }

    //Set an exception (if one has not already been set), which will be thrown after waiting is complete
    void setException(IException * e);
    bool hasException() const { return exception != nullptr; }

protected:
    CTask * next = nullptr;
    CTask * prev = nullptr;
    std::atomic<unsigned> numPredecessors;
    std::atomic<IException *> exception{nullptr};
};


//---------------------------------------------------------------------------------------------------------------------

interface ITaskScheduler : public IInterface
{
public:
    virtual void enqueueOwnedTask(CTask * ownedTask) = 0;
    virtual unsigned numProcessors() const = 0;
};

// Functions to provide schedulers for tasks with different characteristics.
//    queryTaskScheduler()
//          - for tasks that should be non-blocking and reasonably fine-grained.  Number of active tasks never exceeds the number of cores.
//    queryIOTaskScheduler()
//          - for tasks that could be blocked by io, but not for long periods.  Number of active tasks may be higher than number of cores.
extern jlib_decl ITaskScheduler & queryTaskScheduler();
extern jlib_decl ITaskScheduler & queryIOTaskScheduler();

// Future - a scheduler for periodic tasks might be useful

//---------------------------------------------------------------------------------------------------------------------

//MORE: This can probably be private within the cpp file (and enqueue can become non-virtual).
class jlib_decl ATaskProcessor  : public Thread
{
public:
    virtual void enqueueOwnedChildTask(CTask * ownedTask) = 0;
};

extern jlib_decl ATaskProcessor * queryCurrentTaskProcessor();

//---------------------------------------------------------------------------------------------------------------------

extern jlib_decl void notifyPredDone(CTask * successor);
extern jlib_decl void notifyPredDone(Owned<CTask> && successor);
extern jlib_decl void enqueueOwnedTask(ITaskScheduler & scheduler, CTask * ownedTask);

//---------------------------------------------------------------------------------------------------------------------
// Helper task implementations
//---------------------------------------------------------------------------------------------------------------------

// A task with a successor, which automatically manages the predecessor count for the successor task
// return checkNextTask() from the execute method of the task when it is complete.
class jlib_decl CPredecessorTask : public CTask
{
public:
    CPredecessorTask(unsigned _numPred, CTask * _successor) : CTask(_numPred), successor(_successor)
    {
        if (successor)
            successor->addPred();
    }

    CTask * checkNextTask()
    {
        if (successor)
        {
            if (successor->notePredDone())
                return successor.getClear();
        }
        return nullptr;
    }

protected:
    Linked<CTask> successor; // may be cleared once this task is complete
};

//---------------------------------------------------------------------------------------------------------------------

// A helpful utility class which can be used as a successor for other tasks, and will signal a semaphore once all
// the preceeding tasks have completed.  Allows a sort or similar with nested tasks to wait until all work is complete.
// NB: Always allocate this on the heap, otherwise it can go out of scope before execute() returns causing chaos!
class jlib_decl CCompletionTask final : public CTask
{
public:
    CCompletionTask(unsigned _numPred, ITaskScheduler & _scheduler) : CTask(_numPred), scheduler(_scheduler) {}
    ~CCompletionTask() { ::Release(exception.load()); }

    virtual CTask * execute() override
    {
        sem.signal();
        return nullptr;
    }

    // Execute a function as a child task - decAndWait() will wait for completion
    void spawn(std::function<void ()> func);

    //Called when main thread has completed - decrements the predecessor count, and waits for completion
    void decAndWait();

protected:
    ITaskScheduler & scheduler;
    Semaphore sem;
};

// A class used by CCompletionTask to implement spawn
class jlib_decl CFunctionTask final : public CPredecessorTask
{
public:
    CFunctionTask(std::function<void ()> _func, CTask * _successor);

    virtual CTask * execute() override;

protected:
    std::function<void ()> func;
};


#endif
