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

#ifndef workflow_incl
#define workflow_incl

#include "thorhelper.hpp"
#include "workunit.hpp"
#include "jlog.hpp"
#include "eclhelper.hpp"
#include <queue>
#include <thread>
#include <atomic>

#ifdef _DEBUG
//    #define TRACE_WORKFLOW
#endif

#define WFERR_ExecutingInWaitState      5100
#define WFERR_ExecutingInBlockedState   5101
#define WFERR_ExecutingItemMoreThanOnce 5102
#define WFERR_NoParentItemFound         5103
#define WFERR_NoReqdItemFound           5104

class WORKUNIT_API WorkflowException : public IException, public CInterface
{
public:
    typedef enum { SYSTEM, USER, ABORT } Type;
    WorkflowException(int _code, char const * _msg, unsigned _wfid, Type _type, MessageAudience _audience) : code(_code), msg(_msg), wfid(_wfid), type(_type), audience(_audience) {}
    virtual ~WorkflowException() {}
    IMPLEMENT_IINTERFACE;
    virtual int errorCode() const { return code; }
    virtual StringBuffer & errorMessage(StringBuffer & buff) const { return buff.append(msg.get()); }
    virtual MessageAudience errorAudience() const { return audience; }
    unsigned queryWfid() const { return wfid; }
    Type queryType() const { return type; }
private:
    int code;
    StringAttr msg;
    unsigned wfid;
    Type type;
    MessageAudience audience;
};

/** This is the main work-flow interface. The dependency tree is kept in the
  * IWorkflowItemArray object and each item is executed in order, or recursed
  * in case of dependencies.
  *
  * The workflow object is created with a global function createWorkflowItemArray
  * and populated via the createWorkflowItem. Shouldn't be static member? Or better,
  * using a builder or factory pattern?
  *
  * Calling the method perform will then execute the whole dependency graph recursively,
  * depth-first, and account for workunits' scheduling and machine epilogue/prologue.
  *
  * The main features are:
  *  - Allow a graph of dependent workflow items
  *  - Allow actions to be performed on success/failure
  *  - Allow recovery actions before retrying, with limits on number of retries.
  *  - Ensure that workflow items inside SEQUENTIAL actions are executed correctly.
  *  - Allow workflow items to be triggered on events.
  *  - Support once, stored, persist workflow items.
  *
  */
class CCloneWorkflowItem;
class WORKUNIT_API WorkflowMachine : public CInterface
{
public:
    WorkflowMachine();
    WorkflowMachine(const IContextLogger &logctx);
    void perform(IGlobalCodeContext *_ctx, IEclProcess *_process);
    int queryLastFailCode() const;
    char const * queryLastFailMessage() const;
    const char * queryEventName() const;
    const char * queryEventExtra() const;
    bool hasItemsWaiting() const { return (itemsWaiting > 0); }
    void setCondition(bool value) { condition = value; }
    bool isItemOlderThanInputPersists(IRuntimeWorkflowItem & item);

protected:
    // Machine specific prologue/epilogue
    virtual void begin() = 0;
    virtual void end() = 0;
    // Workflow specific scheduling
    virtual void schedulingStart() = 0;
    virtual bool schedulingPull() = 0;
    virtual bool schedulingPullStop() = 0;
    // Error handling
    virtual void reportContingencyFailure(char const * type, IException * e) = 0;
    virtual void checkForAbort(unsigned wfid, IException * handling) = 0;
    // Persistence styles varies from machine to machine
    virtual void doExecutePersistItem(IRuntimeWorkflowItem & item) = 0;
    virtual void doExecuteCriticalItem(IRuntimeWorkflowItem & item) = 0;
    virtual bool getPersistTime(time_t & when, IRuntimeWorkflowItem & item) = 0;
    virtual void noteTiming(unsigned wfid, timestamp_type startTime, stat_type elapsedNs) = 0;

    // Check conditions, item type and call operations below based on type
    bool executeItem(unsigned wfid, unsigned scheduledWfid);

    // Iterate through dependencies and execute them
    bool doExecuteItemDependencies(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    bool doExecuteItemDependency(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid, bool alwaysEvaluate);
    // Execute an item (wrapper to deal with exceptions)
    void doExecuteItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    // Actually executes item: calls process->perform()
    void performItem(unsigned wfid, unsigned scheduledWfid);
    // Conditional dependency execution
    bool doExecuteConditionItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    // Block execution of the currently executing scheduled item
    void doExecuteBeginWaitItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    // Unblock the scheduled workflow item, which should mean execution continues.
    void doExecuteEndWaitItem(IRuntimeWorkflowItem & item);

    //Used for checking if a persist is older than its inputs
    bool isOlderThanPersist(time_t when, IRuntimeWorkflowItem & item);
    bool isOlderThanInputPersists(time_t when, IRuntimeWorkflowItem & item);

    bool attemptRetry(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid);
    void handleFailure(IRuntimeWorkflowItem & item, WorkflowException const * e, bool isDep);

    //returns false if the workflow is valid, but there is nothing to be done
    bool addSuccessors();
    //This function defines the implicit dependencies of the workflow, by creating logical successorships.
    //It traverses through all the items, by recursing through their dependencies.
    //It also reverses dependencies, so that items point to their successors.
    void defineLogicalRelationships(unsigned wfid, CCloneWorkflowItem * logicalPredecessor, bool prevOrdered);
    CCloneWorkflowItem & queryWorkflowItem(unsigned wfid);
    //This creates a new node which is inserted as a predecessor to the successor item.
    //This new runtime item is a logical predecessor - one that may activate the successor.
    //The logical predecessor can also activate any of the successor's children.
    //The pointer to the runtime item is returned.
    unsigned insertLogicalPredecessor(unsigned successorWfid);

    void performParallel(IGlobalCodeContext *_ctx, IEclProcess *_process);
    void processWfItems();
    void executeItemParallel(unsigned wfid);
    void doExecuteItemParallel(IRuntimeWorkflowItem & item);
    void doExecuteConditionExpression(CCloneWorkflowItem & item);
    void performItemParallel(unsigned wfid);
    //Returns true if a failure contingency has been queued
    bool handleFailureParallel(CCloneWorkflowItem & item, WorkflowException * e);
    //Returns true if a failure contingency has been queued
    bool activateFailureContingency(CCloneWorkflowItem & item);
    void checkAbort(CCloneWorkflowItem & item, bool depFailed);
    void startContingency();
    void endContingency();

    void processDependentSuccessors(CCloneWorkflowItem &item);
    void processLogicalSuccessors(CCloneWorkflowItem &item);
    //when an item fails, this marks dependentSuccessors with the exception belonging to their predecessor
    void failDependentSuccessors(CCloneWorkflowItem &item);

    void addToItemQueue(unsigned wfid);
    bool checkIfDone();

    virtual bool getParallelFlag() const = 0;
    virtual unsigned getThreadNumFlag() const = 0;
    bool isParallelViable();


protected:
    const IContextLogger &logctx;
    Owned<IWorkflowItemArray> workflow;
    //contains extra workflow items that are created at runtime. These support logical successorships
    std::vector<Shared<IRuntimeWorkflowItem>> logicalWorkflow;
    std::queue<unsigned> wfItemQueue;
    Semaphore wfItemQueueSem;
    //used to pop/add items to the queue
    CriticalSection queueCritSec;
    //optional debug value "parallelThreads" to select number of threads
    unsigned numThreads = 1U;
    //the wfid of the parent item. It has no successors, only dependents.
    unsigned parentWfid = 0U;
    //If startItem has an item as its logical successor, then that item will be active before the start.
    //Any items that are active from the start don't need to perform the defineLogicalRelationships algorithm more than once.
    CCloneWorkflowItem *startItem = nullptr;
     //flag is set when the "parent" item is reached. There may still be pending contingencies
    std::atomic<bool> parentReached{false};
    //flag is set once the workflow is completed
    std::atomic<bool> done{false};
    //flag is set when a workflowItem fails and is not successfully recovered
    std::atomic<bool> abort{false};
    //This protects against a race condition between activate() and deactivate()
    CriticalSection activationCritSec;
    //This protects each item from having its exception set twice, in a race condition
    CriticalSection exceptionCritSec;
    //This counts the active contingency clauses (that haven't finished being executed)
    //This ensures that the query doesn't finish without completing the contingencies.
    std::atomic<unsigned> activeContingencies{0U};
    //The number of branches is the number of dependent successors to the failed workflow item.
    //Each successor then fails its own successors, so the branch count increases.
    //In order to verify that all possible contingency clauses have been reached, the number of open-ended "branches" that haven't yet reached the parent item must be tracked.
    //The query is finished when there are zero open-ended branches.
    std::atomic<unsigned> branchCount{0U};
    //optional debug value "parallelWorkflow" to select parallel algorithm
    bool parallel = false;
    Owned<WorkflowException> runtimeError;

    IGlobalCodeContext *ctx;
    IEclProcess *process;
    IntArray wfidStack;
    unsigned currentWfid;
    unsigned currentScheduledWfid;
    unsigned itemsWaiting;
    unsigned itemsUnblocked;

    //allows the condition result to be returned from a process in a thread-safe way
    CriticalSection conditionCritSec;
    unsigned condition;
};

extern WORKUNIT_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree *p);
extern WORKUNIT_API IWorkflowItemArray *createWorkflowItemArray(unsigned size);
extern WORKUNIT_API IWorkflowItem *createWorkflowItem(IPropertyTree * ptree, unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor);
extern WORKUNIT_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree * ptree);

extern WORKUNIT_API const char * queryWorkflowTypeText(WFType type);
extern WORKUNIT_API const char * queryWorkflowModeText(WFMode mode);
extern WORKUNIT_API const char * queryWorkflowStateText(WFState state);

#ifdef TRACE_WORKFLOW
constexpr LogMsgCategory MCworkflow = MCprogress(50); // Category used to inform enqueue/start/finish of workflow item
#endif

#endif
