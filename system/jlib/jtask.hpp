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


/*

This file defines multiple taskSchedulers than be used to execute code in parallel without the cost of starting up
new threads, and avoiding over-commiting the number of cores to process tasks.

It is currently aimed at non-blocking tasks, but the hope is to also use it for IO based tasks (which may over-commit to a certain degree).

There are two common ways of using it.

1) Lambda style functions.

Create a completion task.  When you want to execute some code in parallel use the spawn() function, and at the end wait for all tasks to complete.

Owned<CCompletionTask> completed = new CCompletionTask[(queryTaskScheduler())];
...
completed->spawn([n]() { printf("%u\n", n); });
...
completed->decAndWait();


2) Create task classes, and schedule them when they are ready to run


Owned<CCompletionTask> completed = new CCompletionTask[(queryTaskScheduler())];
...
CTask * task = new XTask(completed);
queryTaskScheduler().enqueueOwnedTask(*task);
...
processor->decAndWait();


Link counting
-------------

- Simple successor linking:
  The simplest approach is for all tasks to LINK their successor tasks.  When a successor task is ready to be scheduled
  it is LINKed and scheduled.  The link counts are reduced when the predecessor task is destroyed, and then released
  again when the task completes.
  However, this means two effective link counts are held for a task, one for the number of predecessors and another
  for the lifetime.

  The classes used to implmement the lambda tasks use this approach.

- Avoiding successor linking:
  A task is only ever scheduled by a single predecessor. If all created tasks will eventually be
  executed there is no need to link/release the successor tasks.  The link count will be 1 from when it was created.
  When it is executed it will be decremented and cleaned up.

  If this approach is used, you must call setMinimalLinking() on the CCompletionTask - this increments the link count
  ready for decrementing when the task is scheduled, and also avoids starting a task if there are no child tasks
  waiting to complete.

  The parallel merge sort uses this approach.

Exceptions
----------
Any code using tasks needs to be careful that it will complete if exceptions are reported.  Task base class has
helper functions for thread-safely recording exceptions for later throwing

The lambda style functions automatically forward any exception to the completion task, which will rethrow the first
exception once all functions have completed.  By default new functions will not be executed once another function
has thrown an exception, but this can be overriden.

*/

interface ITaskScheduler;

class jlib_decl CTask : public CInterface
{
    friend class TaskQueue;
    friend class DListOf<CTask>;

public:
    CTask(unsigned _numPred) : numPredecessors(_numPred) {}
    ~CTask() { ::Release(exception.load()); }

    //Return the next task to execute
    virtual CTask * execute() = 0;

    bool isReady() const { return numPredecessors == 0; }
    void addPred()
    {
        numPredecessors.fetch_add(1);
    }
    void incPred(unsigned numExtra)
    {
        numPredecessors.fetch_add(numExtra);
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

    void setNumPredecessors(unsigned _numPred)
    {
        numPredecessors.store(_numPred);
    }

    // Called within an executing task to start a child task - will be pushed onto the local task queue
    void spawnOwnedChildTask(CTask & ownedTask);

// Exception management helper functions
    bool hasException() const { return exception != nullptr; }
    IException * queryException() const { return exception.load(); }
    void setException(IException * e);      //Set an exception (if one has not already been set)

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
    virtual void enqueueOwnedTask(CTask & ownedTask) = 0;
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

extern jlib_decl void notifyPredDone(CTask & successor);
extern jlib_decl void notifyPredDone(Owned<CTask> && successor);
extern jlib_decl void enqueueOwnedTask(ITaskScheduler & scheduler, CTask & ownedTask);

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
    CCompletionTask(ITaskScheduler & _scheduler, unsigned _numPred=1) : CTask(_numPred), scheduler(_scheduler) {}
    CCompletionTask() : CTask(1), scheduler(queryTaskScheduler()) {}

    virtual CTask * execute() override
    {
        sem.signal();
        return nullptr;
    }

    // Execute a function as a child task - decAndWait() will wait for completion
    void spawn(std::function<void ()> func, bool ignoreParallelExceptions = false);

    //Called when main thread has completed - decrements the predecessor count, and waits for completion
    void decAndWait();

    //Called when tasks are not linked before scheduling
    void setMinimalLinking();

protected:
    ITaskScheduler & scheduler;
    Semaphore sem;
    bool tasksLinkedOnSchedule{true};
};

// A class used by CCompletionTask to implement spawn
class jlib_decl CFunctionTask final : public CPredecessorTask
{
public:
    CFunctionTask(std::function<void ()> _func, CTask * _successor, bool _ignoreParallelExceptions);

    virtual CTask * execute() override;

protected:
    std::function<void ()> func;
    bool ignoreParallelExceptions;
};

//Implementation of asyncFor that uses the task library
template <typename AsyncFunc>
inline void taskAsyncFor(unsigned num, ITaskScheduler & scheduler, AsyncFunc func)
{
    if (num != 1)
    {
        Owned<CCompletionTask> completed = new CCompletionTask(scheduler);
        for (unsigned i=0; i < num; i++)
            completed->spawn([i, func]() { func(i); });

        completed->decAndWait();
    }
    else
        func(0);
}

static constexpr unsigned UnlimitedTasks = (unsigned)-1;
// Allow the number of parallel tasks to be restricted.  by default it will be set to (#cores, 2* #cores)
void jlib_decl setTaskLimits(unsigned _maxCpuTasks, unsigned _maxIOTasks);

#endif
