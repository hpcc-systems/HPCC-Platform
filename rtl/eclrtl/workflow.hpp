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
    virtual void begin() = 0;
    virtual void end() = 0;
    virtual void schedulingStart() = 0;
    virtual bool schedulingPull() = 0;
    virtual bool schedulingPullStop() = 0;
    virtual void reportContingencyFailure(char const * type, IException * e) = 0;
    virtual void checkForAbort(unsigned wfid, IException * handling) = 0;
    virtual void doExecutePersistItem(IRuntimeWorkflowItem & item) = 0;

    bool executeItem(unsigned wfid, unsigned scheduledWfid);
    bool doExecuteItemDependencies(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    bool doExecuteItemDependency(IRuntimeWorkflowItem & item, unsigned dep, unsigned scheduledWfid);
    void doExecuteItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    bool doExecuteConditionItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    void doExecuteBeginWaitItem(IRuntimeWorkflowItem & item, unsigned scheduledWfid);
    void doExecuteEndWaitItem(IRuntimeWorkflowItem & item);
    void performItem(unsigned wfid, unsigned scheduledWfid);
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
