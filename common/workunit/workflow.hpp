/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

class WORKUNIT_API WorkflowException : public CInterface, public IException
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
class WORKUNIT_API WorkflowMachine : public CInterface
{
public:
    WorkflowMachine();
    WorkflowMachine(const IContextLogger &logctx);
    void perform(IGlobalCodeContext *_ctx, IEclProcess *_process);
    unsigned queryCurrentWfid() const { return currentWfid; }
    int queryLastFailCode() const;
    char const * queryLastFailMessage() const;
    const char * queryEventName() const;
    const char * queryEventExtra() const;
    bool hasItemsWaiting() const { return (itemsWaiting > 0); }
    void setCondition(bool value) { condition = value; }

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

    bool attemptRetry(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid);
    void handleFailure(IRuntimeWorkflowItem & item, WorkflowException const * e, bool isDep);

protected:
    const IContextLogger &logctx;
    Owned<IWorkflowItemArray> workflow;
    IGlobalCodeContext *ctx;
    IEclProcess *process;
    IntArray wfidStack;
    unsigned currentWfid;
    unsigned currentScheduledWfid;
    unsigned itemsWaiting;
    unsigned itemsUnblocked;
    unsigned condition;
};

extern WORKUNIT_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree *p);
extern WORKUNIT_API IWorkflowItemArray *createWorkflowItemArray(unsigned size);
extern WORKUNIT_API IWorkflowItem *createWorkflowItem(IPropertyTree * ptree, unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor);
extern WORKUNIT_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree * ptree);

#ifdef TRACE_WORKFLOW
extern const WORKUNIT_API LogMsgCategory MCworkflow;       // Category used to inform enqueue/start/finish of workflow item
#endif

#endif
