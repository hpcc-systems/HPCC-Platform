/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef workflow_incl
#define workflow_incl

#include "eclrtl.hpp"
#include "workunit.hpp"
#include "jlog.hpp"
#include "eclhelper.hpp"

class ECLRTL_API WorkflowException : public CInterface, public IException
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
  */
class ECLRTL_API WorkflowMachine : public CInterface
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
    bool doExecuteItemDependency(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid);
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

extern ECLRTL_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree *p);
extern ECLRTL_API IWorkflowItemArray *createWorkflowItemArray(unsigned size);
extern ECLRTL_API IWorkflowItem *createWorkflowItem(IPropertyTree * ptree, unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor);
extern ECLRTL_API IWorkflowItemIterator *createWorkflowItemIterator(IPropertyTree * ptree);

#ifdef TRACE_WORKFLOW
extern const ECLRTL_API LogMsgCategory MCworkflow;       // Category used to inform enqueue/start/finish of workflow item
#endif

#endif
